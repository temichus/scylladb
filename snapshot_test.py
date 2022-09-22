import glob
import logging
import os
import random
import re
from functools import lru_cache
from pathlib import Path

import pytest
import requests
import shutil
import time
import uuid

from cassandra.cluster import Session
from cassandra.concurrent import execute_concurrent_with_args
from concurrent.futures import ThreadPoolExecutor
from pkg_resources import parse_version
from threading import Thread, Event
from typing import List, Tuple, Dict, Any

from ccmlib.node import NodetoolError
from ccmlib.scylla_node import ScyllaNode
from dtest_class import Tester, create_ks, create_cf
from tools.data import create_index, create_local_index
from tools.files import safe_mkdtemp, replace_in_file
from tools.misc import require
from tools.snapshots import make_snapshot, get_cf_snapshot_saved_dir, restore_snapshot_with_refresh, \
    restore_snapshot_with_sstableloader, get_table_description
from dtest_setup_overrides import DTestSetupOverrides
from tools.misc import ImmutableMapping
from tools.stress import format_cs_output, assert_cs_success

logger = logging.getLogger(__name__)


class SnapshotOperations:
    """Base snapshot operations for parallel executing

    Have operations for creating keyspaces, tables,
    populating the tables, run operations in parallel
    """

    def verify_stderr_empty(self, results):
        """check that snapshot commands don't have stderr

        Arguments:
            results {list} -- list of list with future results
        """
        for future_result in results:
            for result in future_result:
                stdout, stderr = result
                assert not stderr

    def init_cluster(self):
        self.cluster.populate(1).start(wait_for_binary_proto=True)
        node = self.cluster.nodelist()[0]
        session = self.patient_cql_connection(node)

        return node, session

    def prepare_schemas_and_data(self, session, num_ks=1, num_cf=1, num_rows=1, column_length=10):

        for j in range(num_ks):
            create_ks(session, name="ks{}".format(j), rf=1)

            for j in range(num_cf):
                create_cf(session, "table_cf{}".format(j),
                          key_type="varchar")
                st = session.prepare("INSERT INTO table_cf{} (key, c, v) VALUES (?, ?, ?)".format(j))
                execute_concurrent_with_args(session,
                                             st,
                                             map(lambda x, y, z: [str(x), str(y), str(z)],
                                                 list(range(num_rows)),
                                                 list(range(num_rows)),
                                                 ["{}".format(i) * column_length for i in range(num_rows)]))

    def create_snapshot_for_all_keyspaces(self, node, start_process=None) -> List[Tuple[str, str]]:
        if start_process:
            start_process.wait()

        stdout, stderr = node.nodetool(f'snapshot -t {uuid.uuid4()}')
        return [(stdout, stderr)]

    def create_snapshots_per_keyspace_table(self, node, start_process=None, num_ks=1, num_cf=1):
        if start_process:
            start_process.wait()
        results = []
        for i in range(num_ks):
            for j in range(num_cf):
                stdout, stderr = node.nodetool(f'snapshot ks{i}.table_cf{j} -t {uuid.uuid4()}')

            results.append((stdout, stderr))
        return results

    def clear_all_snapshots(self, node, start_process=None):
        """Clear all snapshots on nde

        :param node: Node were run clear snapshots command
        :type node: ScyllaNode
        :param start_process: Flag to start threads at same time, default None
        :type start_process: Barrier, optional
        """
        if start_process:
            start_process.wait()
        stdout, stderr = node.nodetool('clearsnapshot')
        return [(stdout, stderr)]

    def clear_snapshots_per_keyspace(self, node, start_process=None, num_ks=1):
        if start_process:
            start_process.wait()
        results = []
        for i in range(num_ks):
            stdout, stderr = node.nodetool(f'clearsnapshot ks{i}')
            results.append((stdout, stderr))
        return results

    def list_snapshots(self, node):
        stdout, stderr = node.nodetool('listsnapshots')
        return [(stdout, stderr)]


class SnapshotTester(Tester):
    """
    Object with utility functions to perform snapshot operations.
    """

    def init_cluster(self) -> Tuple[ScyllaNode, Session]:
        self.cluster.populate(1).start(wait_for_binary_proto=True)
        node = self.cluster.nodelist()[0]
        session = self.patient_cql_connection(node)

        return node, session

    def create_tables(self, session, tables_number):
        tables = []
        for i in range(tables_number):
            name = f"cf{i}"
            create_cf(session=session, name=name, key_type='int', columns={'val': 'text'})
            tables.append(name)
        return tables

    def insert_rows(self, session, start, end, cf=None):
        cfs = ['cf'] if cf == None else [cf] if isinstance(cf, str) else cf
        for cf_name in cfs:
            insert_statement = session.prepare(f"INSERT INTO ks.{cf_name} (key, val) VALUES (?, 'asdf')")
            args = [(r,) for r in range(start, end)]
            execute_concurrent_with_args(session, insert_statement, args, concurrency=20)

    def validate_rows_count_in_all_tables(self, session, tables, expected_rows_count):
        for table in tables:
            rows = session.execute(f'SELECT count(*) from ks.{table}')
            assert rows[0][0] == expected_rows_count

    def clear_snapshot_per_keyspace_per_table(self, ip, tag, ks, cf):
        requests.delete("http://{}:10000/storage_service/snapshots?tag={}&kn={}&cf={}".format(ip, tag, ks, cf))


@pytest.mark.dtest_full
@pytest.mark.single_node
class TestSnapshot(SnapshotTester):
    """
    Test snapshot operations.
    """

    @pytest.mark.next_gating
    @pytest.mark.dtest_debug
    def test_basic_snapshot_and_restore_with_sstableloader(self):
        """
        Test basic snapshot and restore using an sstable loader.
        """
        self.basic_snapshot_and_restore(use_sstableloader=True, tables_number=1, cf_param_name='')

    @pytest.mark.next_gating
    @pytest.mark.dtest_debug
    def test_basic_snapshot_and_restore_with_refresh(self):
        """
        Test basic snapshot and restore without an sstable loader.
        """
        self.basic_snapshot_and_restore(use_sstableloader=False, tables_number=1, cf_param_name='')

    @pytest.mark.next_gating
    @pytest.mark.dtest_debug
    def test_basic_mulitple_tables_snapshot_and_restore_with_refresh(self):
        """
        Test basic snapshot and restore without an sstable loader.
        """
        self.basic_snapshot_and_restore(use_sstableloader=False, tables_number=5, cf_param_name='-cf')

    @pytest.mark.next_gating
    @pytest.mark.dtest_debug
    def test_basic_mulitple_tables_snapshot_and_restore_with_sstableloader(self):
        """
        Test basic snapshot and restore using an sstable loader.
        """
        self.basic_snapshot_and_restore(use_sstableloader=True, tables_number=5, cf_param_name='-cf')

    @pytest.mark.next_gating
    @pytest.mark.dtest_debug
    def test_basic_mulitple_tables_snapshot_using_column_family(self):
        """
        Test basic snapshot and restore without an sstable loader.
        """
        self.basic_snapshot_and_restore(use_sstableloader=False, tables_number=5, cf_param_name='--column-family')

    @pytest.mark.next_gating
    @pytest.mark.dtest_debug
    def test_basic_mulitple_tables_snapshot_using_table(self):
        """
        Test basic snapshot and restore without an sstable loader.
        """
        self.basic_snapshot_and_restore(use_sstableloader=False, tables_number=5, cf_param_name='--table')

    def basic_snapshot_and_restore(self, use_sstableloader, tables_number, cf_param_name):
        """
        Base testing method:

        1. Create a keyspace
        2. Create a column family
        3. Insert 100 rows into the column family
        4. Take a snapshot
        5. Insert more rows after the snapshot
        6. Drop the keyspace, assure we have no data after the deletion
        7. Restore the snapshot
        8. Verify we have the same num of rows inserted prior to the snapshot.

        @param use_sstableloader: Whether to use sstable loader to
            restore the snapshot.
        """
        cluster = self.cluster
        cluster.populate(1).start()
        (node1,) = cluster.nodelist()
        session = self.patient_cql_connection(node1)
        create_ks(session, 'ks', 1)
        tables = self.create_tables(session=session, tables_number=tables_number)

        self.insert_rows(session, 0, 100, cf=tables)

        if cf_param_name:
            tables_for_snapshot = ','.join(table for table in tables)
            snapshot_dir = make_snapshot(node1, ks='ks', cf=tables_for_snapshot, cf_param_name=cf_param_name,
                                         name='basic')
        else:
            snapshot_dir = make_snapshot(node1, ks='ks', name='basic')

        # Write more data after the snapshot, this will get thrown
        # away when we restore:
        self.insert_rows(session, 100, 200, cf=tables)
        self.validate_rows_count_in_all_tables(session=session, tables=tables, expected_rows_count=200)

        # Drop the keyspace, make sure we have no data:
        session.execute('DROP KEYSPACE ks')
        shutil.rmtree(os.path.join(node1.get_path(), 'data', 'ks'))
        create_ks(session, 'ks', 1)
        self.create_tables(session=session, tables_number=tables_number)
        self.validate_rows_count_in_all_tables(session=session, tables=tables, expected_rows_count=0)

        # Restore data from snapshot:
        for table in tables:
            if use_sstableloader:
                restore_snapshot_with_sstableloader(snapshot_dir, node1, 'ks', table)
            else:
                restore_snapshot_with_refresh(snapshot_dir, node1, 'ks', table)

            node1.nodetool(f'refresh ks {table}')

        # clean up
        logger.info("removing snapshot_dir: " + snapshot_dir)
        shutil.rmtree(snapshot_dir)

        self.validate_rows_count_in_all_tables(session=session, tables=tables, expected_rows_count=100)

    def test_snapshot_for_2kc_and_cf_failure(self):
        """
        Base testing method:

        1. Create a 2 keyspaces
        2. Create a column family in every ks
        3. Insert 100 rows into the column family
        4. Try take a snapshot
        5. Verify error message: Only one keyspace allowed when specifying a column family
        """
        cluster = self.cluster
        cluster.populate(1).start()
        (node1,) = cluster.nodelist()
        session = self.patient_cql_connection(node1)
        session1 = self.patient_cql_connection(node1)

        create_ks(session, 'ks', 1)
        create_ks(session1, 'ks1', 1)
        tables = self.create_tables(session=session, tables_number=1)
        tables1 = self.create_tables(session=session1, tables_number=1)

        self.insert_rows(session, 0, 100, cf=tables)
        self.insert_rows(session1, 0, 100, cf=tables1)

        self.validate_rows_count_in_all_tables(session=session, tables=tables, expected_rows_count=100)
        self.validate_rows_count_in_all_tables(session=session1, tables=tables1, expected_rows_count=100)

        tables_for_snapshot = ','.join(table for table in tables)

        expected_error = 'Only one keyspace allowed when specifying a column family'
        self.ignore_log_patterns += [expected_error]

        with pytest.raises(NodetoolError) as ne:
            make_snapshot(node1, ks='ks,ks1', cf=tables_for_snapshot, name='basic')

        assert expected_error in ne.value.stdout, ne.tb

    def restore_snapshot_with_alter_table(self, drop=False):
        """
        1. create table (A INT,B INT ,C INT)
        2. insert and flush data
        3. alter table: rename C to D / drop B
        4. insert and flush data
        5. create snapshot
        6. stop node and clear data
        7. try to load snapshot using sstableloader
        """
        cluster = self.cluster
        cluster.populate(1).start()
        (node1,) = cluster.nodelist()
        session = self.patient_cql_connection(node1)
        create_ks(session, 'ks', 1)
        session.execute('CREATE TABLE ks.cf (key varchar, A int, B int, C int, PRIMARY KEY(key, c));')

        query = "INSERT INTO ks.cf (key, A, B, C) VALUES ('{}', 11, 22, 33);".format(str(uuid.uuid1()))
        session.execute(query)
        node1.nodetool("flush -- ks")

        logger.info('Alter table...')
        session.execute("ALTER TABLE ks.cf RENAME C TO D;")
        if drop:
            session.execute("ALTER TABLE ks.cf DROP B;")
        query = "INSERT INTO ks.cf (key, A, D) VALUES ('{}', 44, 55);".format(str(uuid.uuid1()))
        session.execute(query)
        node1.nodetool("flush -- ks")

        snapshot_dir = make_snapshot(node1, ks='ks', cf='cf', name='basic')

        # clear data
        cluster.stop(gently=True)
        logger.info('Clear data...')
        data_path = '{}/data/ks/cf-*'.format(node1.get_path())
        data_files = glob.glob(os.path.join(data_path, '*'))
        for f in data_files:
            if os.path.isfile(f):
                os.unlink(f)

        cluster.start()
        restore_snapshot_with_sstableloader(snapshot_dir, node1, 'ks', 'cf')

    def restore_snapshot_with_alter_table_test(self):
        self.restore_snapshot_with_alter_table()

    def restore_snapshot_with_alter_table_drop_column_test(self):
        self.restore_snapshot_with_alter_table(drop=True)

    def test_nodetool_snapshot_race_condition_with_compaction_under_stress(self):
        # Cover Issue #4051 https://github.com/scylladb/scylla/issues/4051
        def run_stress(node):
            logger.info('Start stress command')
            results = node.stress(['write', 'duration=5m', '-mode', 'cql3', 'native', '-rate',
                                  'threads=100', '-pop', 'seq=1..100000000', '-log', 'interval=5'], capture_output=True)
            logger.info('Stress results:\n' + format_cs_output(results))
            assert_cs_success(results)

        cluster = self.cluster
        cluster.populate(1).start()
        node1 = cluster.nodelist()[0]

        # Start stress in thread
        stress_run_th = Thread(target=run_stress, args=(node1, ))
        stress_run_th.start()

        # wait for 2,5min (150s) while db populated with data
        time.sleep(150)

        # run several snapshot commands
        for i in range(2):
            time.sleep(60)
            results, errors = node1.nodetool(f'snapshot -t {uuid.uuid4()}')
            logger.info(results + errors)
            assert 'failed: filesystem error: link failed: No such file or directory' not in ' '.join(results + errors)
            # Check that no other errors occured during snapshot command
            assert not errors, "Some errors in creating snapshot: %s" % errors

        # wait stress command completion.
        stress_run_th.join()

    def test_nodetool_snapshot_race_condition_with_compaction_after_node_start(self):

        cluster = self.cluster
        cluster.populate(1).start()
        node1 = cluster.nodelist()[0]

        logger.info('Run stress command')
        results = node1.stress(['write', 'n=10000', '-rate', 'threads=10'], capture_output=True)
        logger.info('Stress results:\n' + format_cs_output(results))
        assert_cs_success(results)

        logger.info('Stoping node..')
        node1.stop()
        logger.info('Node has been stopped')

        logger.info('Starting node...')
        node1.start(wait_for_binary_proto=True)
        logger.info('Node has been started')

        logger.info('Create snapshot right after start')
        result, errors = node1.nodetool(f'snapshot -t {uuid.uuid4()}')
        logger.info(result + errors)
        assert 'failed: filesystem error: link failed: No such file or directory' not in ' '.join(result + errors)
        # Check that no other errors occured during snapshot command
        assert not errors, "Some errors in creating snapshot: %s" % errors

    def test_nodetool_snapshot_during_major_compaction(self):

        def run_compaction(node):
            logger.info('Start compaction by command')
            node.compact()
            logger.info('Compaction done')

        cluster = self.cluster
        cluster.populate(1).start()
        node1 = cluster.nodelist()[0]

        logger.info('Run stress command')
        results = node1.stress(['write', 'n=1000000', '-rate', 'threads=10'], capture_output=True)
        logger.info('Stress results:\n' + format_cs_output(results))
        assert_cs_success(results)
        assert node1.is_live()

        compaction_thread = Thread(target=run_compaction, args=(node1, ))
        compaction_thread.start()
        time.sleep(0.5)
        logger.info('Create snapshot right after start')
        result, errors = node1.nodetool('snapshot')
        logger.info(result + errors)
        assert 'failed: filesystem error: link failed: No such file or directory' not in ' '.join(result + errors)
        # Check that no other errors occured during snapshot command
        assert not errors, "Some errors in creating snapshot: %s" % errors

        compaction_thread.join()

    def test_cleaning_snapshot_created_by_ks(self):
        self.cleaning_snapshot_by_cf(snapshot_by_multiple_cf=False)

    def test_cleaning_snapshot_created_by_multiple_cf(self):
        self.cleaning_snapshot_by_cf(snapshot_by_multiple_cf=True)

    def cleaning_snapshot_by_cf(self, snapshot_by_multiple_cf):
        """Test deleting specific table from snapshot
           The test create a keyspace and two tables
           it take a snapshot, make sure that both tables are part of the backup
           It then delete on table and make sure that it is deleted but the other one is not.
        """

        def search_cf_in_snapshot(node, cf, tag):
            snapshot_dir = os.path.join(node.get_path(), 'data', 'ks')
            cf_id = [s for s in os.listdir(snapshot_dir) if s.startswith(cf + "-")][0]

            if not os.path.exists(os.path.join(snapshot_dir, cf_id)):
                return False
            if not os.path.exists(os.path.join(snapshot_dir, cf_id, 'snapshots', tag)):
                return False
            return True

        cluster = self.cluster
        cluster.populate(1).start()
        node = cluster.nodelist()[0]
        session = self.patient_cql_connection(node)
        create_ks(session, 'ks', 1)
        session.execute('CREATE TABLE ks.cf ( key int PRIMARY KEY, val text);')
        session.execute('CREATE TABLE ks.cf1 ( key int PRIMARY KEY, val text);')

        self.insert_rows(session, 0, 100)
        self.insert_rows(session, 0, 100, "cf1")

        logger.info("all KSes and CFes are created")
        node.flush()
        if snapshot_by_multiple_cf:
            # Take snapshot by multiple tables
            node.nodetool('snapshot ks -cf cf,cf1 -t per_cf')
        else:
            # Take snapshot for all tables in keyspace
            node.nodetool('snapshot ks -t per_cf')

        assert search_cf_in_snapshot(node, "cf", "per_cf"), "cf {} is not found in snapshot".format("cf")
        assert search_cf_in_snapshot(node, "cf1", "per_cf"), "cf {} is not found in snapshot".format("cf1")
        logger.info("all KSes and CFes are part of the snapshot")

        self.clear_snapshot_per_keyspace_per_table(self.cluster.get_node_ip(1), 'per_cf', "ks", "cf")

        assert not search_cf_in_snapshot(node, "cf", "per_cf"), \
            "cf {} is found in snapshot but should be deleted".format("cf")
        assert search_cf_in_snapshot(node, "cf1", "per_cf"), \
            "cf {} is not found in snapshot but should be remain".format("cf1")


@pytest.mark.dtest_full
@pytest.mark.single_node
class TestArchiveCommitlog(SnapshotTester):
    """
    Test operations with the archive commit log.
    """

    @pytest.fixture(scope='function', autouse=True)
    def fixture_dtest_setup_overrides(self, dtest_config):
        dtest_setup_overrides = DTestSetupOverrides()
        dtest_setup_overrides.cluster_options = ImmutableMapping({"commitlog_segment_size_in_mb": 1})
        return dtest_setup_overrides

    @pytest.mark.skip('Feature commitlog-archiving is not supported')
    def test_archive_commitlog(self):
        self.run_archive_commitlog(restore_point_in_time=False)

    @pytest.mark.skip('Feature commitlog-archiving is not supported')
    def test_archive_commitlog_with_active_commitlog(self):
        """
        Copy the active commitlogs to the archive directory before restoration
        """
        self.run_archive_commitlog(restore_point_in_time=False, archive_active_commitlogs=True)

    @pytest.mark.skip('Feature commitlog-archiving is not supported')
    def dont_test_archive_commitlog(self):
        """
        Run the archive commitlog test, but forget to add the restore commands
        """
        self.run_archive_commitlog(restore_point_in_time=False, restore_archived_commitlog=False)

    @pytest.mark.skip('Feature commitlog-archiving is not supported')
    def test_archive_commitlog_point_in_time(self):
        """
        Test archive commit log with restore_point_in_time setting
        """
        self.run_archive_commitlog(restore_point_in_time=True)

    @pytest.mark.skip('Feature commitlog-archiving is not supported')
    def test_archive_commitlog_point_in_time_with_active_commitlog(self):
        """
        Test archive commit log with restore_point_in_time setting
        """
        self.run_archive_commitlog(restore_point_in_time=True, archive_active_commitlogs=True)

    def run_archive_commitlog(self, restore_point_in_time=False, restore_archived_commitlog=True,
                              archive_active_commitlogs=False):
        """
        Run archive commit log restoration test
        """

        cluster = self.cluster
        cluster.populate(1)
        (node1,) = cluster.nodelist()

        # Create a temp directory for storing commitlog archives:
        tmp_commitlog = safe_mkdtemp()
        logger.info("tmp_commitlog: " + tmp_commitlog)

        # Edit commitlog_archiving.properties and set an archive
        # command:
        replace_in_file(os.path.join(node1.get_path(), 'conf', 'commitlog_archiving.properties'),
                        [(r'^archive_command=.*$', 'archive_command=cp %path {tmp_commitlog}/%name'.format(
                            tmp_commitlog=tmp_commitlog))])

        cluster.start()

        session = self.patient_cql_connection(node1)
        create_ks(session, 'ks', 1)
        session.execute('CREATE TABLE ks.cf ( key bigint PRIMARY KEY, val text);')
        logger.info("Writing first 30,000 rows...")
        self.insert_rows(session, 0, 30000)
        # Record when this first set of inserts finished:
        insert_cutoff_times = [time.gmtime()]

        # Delete all commitlog backups so far:
        for f in glob.glob(tmp_commitlog + "/*"):
            os.remove(f)

        snapshot_dir = make_snapshot(node1, ks='ks', cf='cf', name='basic')

        if parse_version(self.cluster.version()) >= parse_version('3.0'):
            system_ks_snapshot_dir = make_snapshot(node1, ks='system_schema', cf='keyspaces', name='keyspaces')
        else:
            system_ks_snapshot_dir = make_snapshot(node1, ks='system', cf='schema_keyspaces', name='keyspaces')

        if parse_version(self.cluster.version()) >= parse_version('3.0'):
            system_col_snapshot_dir = make_snapshot(node1, ks='system_schema', cf='columns', name='columns')
        else:
            system_col_snapshot_dir = make_snapshot(node1, ks='system', cf='schema_columns', name='columns')

        if parse_version(self.cluster.version()) >= parse_version('3.0'):
            system_ut_snapshot_dir = make_snapshot(node1, ks='system_schema', cf='types', name='usertypes')
        else:
            system_ut_snapshot_dir = make_snapshot(node1, ks='system', cf='schema_usertypes', name='usertypes')

        if parse_version(self.cluster.version()) >= parse_version('3.0'):
            system_cfs_snapshot_dir = make_snapshot(node1, ks='system_schema', cf='tables', name='cfs')
        else:
            system_cfs_snapshot_dir = make_snapshot(node1, ks='system', cf='schema_columnfamilies', name='cfs')

        try:
            # Write more data:
            logger.info("Writing second 30,000 rows...")
            self.insert_rows(session, 30000, 60000)
            node1.flush()
            time.sleep(10)
            # Record when this second set of inserts finished:
            insert_cutoff_times.append(time.gmtime())

            logger.info("Writing final 5,000 rows...")
            self.insert_rows(session, 60000, 65000)
            # Record when the third set of inserts finished:
            insert_cutoff_times.append(time.gmtime())

            rows = session.execute('SELECT count(*) from ks.cf')
            # Make sure we have the same amount of rows as when we snapshotted:
            assert rows[0][0] == 65000

            # Check that there are at least one commit log backed up that
            # is not one of the active commit logs:
            commitlog_dir = os.path.join(node1.get_path(), 'commitlogs')
            logger.info("node1 commitlog dir: " + commitlog_dir)

            assert len(set(os.listdir(tmp_commitlog)) - set(os.listdir(commitlog_dir))) > 0

            cluster.flush()
            cluster.compact()
            node1.drain()
            if archive_active_commitlogs:
                # restart the node which causes the active commitlogs to be archived
                node1.stop()
                node1.start(wait_for_binary_proto=True)

            # Destroy the cluster
            cluster.stop()
            self.copy_logs(name=self.id().split(".")[0] + "_pre-restore")
            self._cleanup_cluster()
            cluster = self.cluster = self.get_cluster()
            cluster.populate(1)
            node1, = cluster.nodelist()

            # Restore schema from snapshots:
            if parse_version(self.cluster.version()) >= parse_version('3.0'):
                self.restore_snapshot(system_ks_snapshot_dir, node1, 'system_schema', 'keyspaces', 'keyspaces')
            else:
                self.restore_snapshot(system_ks_snapshot_dir, node1, 'system', 'schema_keyspaces', 'keyspaces')

            if parse_version(self.cluster.version()) >= parse_version('3.0'):
                self.restore_snapshot(system_col_snapshot_dir, node1, 'system_schema', 'columns', 'columns')
            else:
                self.restore_snapshot(system_col_snapshot_dir, node1, 'system', 'schema_columns', 'columns')

            if parse_version(self.cluster.version()) >= parse_version('3.0'):
                self.restore_snapshot(system_ut_snapshot_dir, node1, 'system_schema', 'types', 'usertypes')
            else:
                self.restore_snapshot(system_ut_snapshot_dir, node1, 'system', 'schema_usertypes', 'usertypes')

            if parse_version(self.cluster.version()) >= parse_version('3.0'):
                self.restore_snapshot(system_cfs_snapshot_dir, node1, 'system_schema', 'tables', 'cfs')
            else:
                self.restore_snapshot(system_cfs_snapshot_dir, node1, 'system', 'schema_columnfamilies', 'cfs')

            self.restore_snapshot(snapshot_dir, node1, 'ks', 'cf', 'basic')

            cluster.start(wait_for_binary_proto=True)

            session = self.patient_cql_connection(node1)
            node1.nodetool('refresh ks cf')

            rows = session.execute('SELECT count(*) from ks.cf')
            # Make sure we have the same amount of rows as when we snapshotted:
            assert rows[0][0] == 30000

            # Edit commitlog_archiving.properties. Remove the archive
            # command  and set a restore command and restore_directories:
            if restore_archived_commitlog:
                replace_in_file(os.path.join(node1.get_path(), 'conf', 'commitlog_archiving.properties'),
                                [(r'^archive_command=.*$', 'archive_command='),
                                 (r'^restore_command=.*$', 'restore_command=cp -f %from %to'),
                                 (r'^restore_directories=.*$', 'restore_directories={tmp_commitlog}'.format(
                                     tmp_commitlog=tmp_commitlog))])

                if restore_point_in_time:
                    restore_time = time.strftime("%Y:%m:%d %H:%M:%S", insert_cutoff_times[1])
                    replace_in_file(os.path.join(node1.get_path(), 'conf', 'commitlog_archiving.properties'),
                                    [(r'^restore_point_in_time=.*$', 'restore_point_in_time={restore_time}'.format(**locals()))])

            logger.info("Restarting node1..")
            node1.stop()
            node1.start(wait_for_binary_proto=True)

            node1.nodetool('flush')
            node1.nodetool('compact')

            session = self.patient_cql_connection(node1)
            rows = session.execute('SELECT count(*) from ks.cf')
            # Now we should have 30000 rows from the snapshot + 30000 rows
            # from the commitlog backups:
            if not restore_archived_commitlog:
                assert rows[0][0] == 30000
            elif restore_point_in_time:
                assert rows[0][0] == 60000
            else:
                assert rows[0][0] == 65000

        finally:
            # clean up
            logger.info("removing snapshot_dir: " + snapshot_dir)
            shutil.rmtree(snapshot_dir)
            logger.info("removing snapshot_dir: " + system_ks_snapshot_dir)
            shutil.rmtree(system_ks_snapshot_dir)
            logger.info("removing snapshot_dir: " + system_cfs_snapshot_dir)
            shutil.rmtree(system_cfs_snapshot_dir)
            logger.info("removing snapshot_dir: " + system_ut_snapshot_dir)
            shutil.rmtree(system_ut_snapshot_dir)
            logger.info("removing snapshot_dir: " + system_col_snapshot_dir)
            shutil.rmtree(system_col_snapshot_dir)
            logger.info("removing tmp_commitlog: " + tmp_commitlog)
            shutil.rmtree(tmp_commitlog)


@pytest.mark.dtest_full
@pytest.mark.single_node
class TestParallelSnapshotOperations(Tester, SnapshotOperations):
    log = logging.getLogger()

    def test_parallel_creating_cleaning_one_ks(self):
        node, session = self.init_cluster()
        self.prepare_schemas_and_data(session, num_ks=1, num_cf=1, num_rows=1, column_length=10)
        logger.info("Keyspaces and columns are created and populated")
        starter = Event()
        futures = []
        results = []
        self.create_snapshots_per_keyspace_table(node, num_ks=1, num_cf=1)
        with ThreadPoolExecutor(max_workers=2) as pool:
            futures.append(pool.submit(self.create_snapshots_per_keyspace_table, node, starter, num_ks=1, num_cf=1))
            futures.append(pool.submit(self.clear_snapshots_per_keyspace, node, starter, num_ks=1))
            logger.info("Start processes")
            starter.set()
            for f in futures:
                results.append(f.result())

        # assert that result of each command has not stderr message
        self.verify_stderr_empty(results)

    def test_parallel_operations_for_10_ks_1_table_per_ks(self):
        node, session = self.init_cluster()
        self.prepare_schemas_and_data(session, num_ks=10, num_cf=1, num_rows=1, column_length=10)
        logger.info("Keyspaces and columns are created and populated")
        starter = Event()
        futures = []
        results = []
        self.create_snapshots_per_keyspace_table(node, num_ks=10, num_cf=1)
        with ThreadPoolExecutor(max_workers=2) as pool:
            futures.append(pool.submit(self.create_snapshots_per_keyspace_table, node, starter, num_ks=10, num_cf=1))
            futures.append(pool.submit(self.clear_snapshots_per_keyspace, node, starter, num_ks=10))
            logger.info("Start processes")
            starter.set()

            for f in futures:
                results.append(f.result())

        # assert that result of each command has not stderr message
        self.verify_stderr_empty(results)

    def test_parallel_operations_for_10ks_and_10tables_and_clearallsnapshots(self):
        node, session = self.init_cluster()
        self.prepare_schemas_and_data(session, num_ks=10, num_cf=10)
        logger.info("Keyspaces and columns are created and populated")
        starter = Event()
        futures = []
        results = []
        self.create_snapshots_per_keyspace_table(node, num_ks=10, num_cf=10)
        with ThreadPoolExecutor(max_workers=3) as pool:
            futures.append(pool.submit(self.create_snapshots_per_keyspace_table, node, starter, num_ks=10, num_cf=10))
            futures.append(pool.submit(self.clear_snapshots_per_keyspace, node, starter, num_ks=10))
            futures.append(pool.submit(self.clear_all_snapshots, node, starter))
            logger.info("Start processes")
            starter.set()

            for f in futures:
                results.append(f.result())
        # assert that result of each command has not stderr message
        self.verify_stderr_empty(results)

    def test_parallel_operation_create_clear_for_all_ks(self):
        node, session = self.init_cluster()
        self.prepare_schemas_and_data(session, num_ks=10, num_cf=10, num_rows=1, column_length=10)
        logger.info("Keyspaces and columns are created and populated")
        starter = Event()
        futures = []
        results = []
        self.create_snapshot_for_all_keyspaces(node)
        with ThreadPoolExecutor(max_workers=4) as pool:
            futures.append(pool.submit(self.create_snapshot_for_all_keyspaces, node, starter))
            futures.append(pool.submit(self.clear_all_snapshots, node, starter))
            logger.info("Start processes")
            starter.set()

            # run operations in parallel without syncinc start operations
            futures.append(pool.submit(self.create_snapshot_for_all_keyspaces, node))
            futures.append(pool.submit(self.clear_all_snapshots, node))

            for f in futures:
                results.append(f.result())

        # assert that result of each command has not stderr message
        self.verify_stderr_empty(results)

    def test_parallel_operations_create_clear_per_ks_and_all(self):
        """Test create snapshots per keyspae and clear all

        Run create snpahosts for each keyspaces and run clearing all snapshots
        in parallel
        """
        node, session = self.init_cluster()
        self.prepare_schemas_and_data(session, num_ks=10, num_cf=10, num_rows=1, column_length=10)
        logger.info("Keyspaces and columns are created and populated")
        starter = Event()
        futures = []
        results = []
        self.create_snapshot_for_all_keyspaces(node)
        with ThreadPoolExecutor(max_workers=6) as pool:
            futures.append(pool.submit(self.create_snapshot_for_all_keyspaces, node, starter))
            futures.append(pool.submit(self.clear_all_snapshots, node, starter))
            futures.append(pool.submit(self.create_snapshots_per_keyspace_table, node, starter, num_ks=10, num_cf=10))
            futures.append(pool.submit(self.clear_snapshots_per_keyspace, node, starter, num_ks=10))
            starter.set()
            # run operations in parallel without syncinc start operations
            futures.append(pool.submit(self.create_snapshot_for_all_keyspaces, node))
            futures.append(pool.submit(self.clear_all_snapshots, node))

            for f in futures:
                results.append(f.result())

        # assert that result of each command has not stderr message
        self.verify_stderr_empty(results)

    def test_parallel_operations_create_list_clear_for_all_ks(self):
        """Test create/list/clear for all keyspaces

        Verify that parallel operations for snapshots for all
        keyspaces run without errors
        """
        node, session = self.init_cluster()
        self.prepare_schemas_and_data(session, num_ks=30, num_cf=10, num_rows=10, column_length=10)
        logger.info("Keyspaces and columns are created and populated")
        starter = Event()
        futures = []
        results = []
        self.create_snapshot_for_all_keyspaces(node)
        with ThreadPoolExecutor(max_workers=6) as pool:
            futures.append(pool.submit(self.create_snapshot_for_all_keyspaces, node, starter))
            futures.append(pool.submit(self.clear_all_snapshots, node, starter))
            futures.append(pool.submit(self.list_snapshots, node))
            starter.set()
            # run operations in parallel without syncinc start operations
            futures.append(pool.submit(self.create_snapshot_for_all_keyspaces, node))
            futures.append(pool.submit(self.clear_all_snapshots, node))
            futures.append(pool.submit(self.list_snapshots, node))

            for f in futures:
                results.append(f.result())
        # assert that result of each command has not stderr message
        self.verify_stderr_empty(results)

    def test_parallel_operations_with_large_data_size(self):
        """Test create/list/clear in parallel, which start not at same time

        Validate that if operations started at same time and
        another operations started in parallel, doesn't cause
        any crtitical issues.
        """
        node, session = self.init_cluster()
        self.prepare_schemas_and_data(session, num_ks=15, num_cf=15, num_rows=1000, column_length=1000)
        logger.info("Keyspaces and columns are created and populated")
        starter = Event()
        futures = []
        results = []
        self.create_snapshot_for_all_keyspaces(node)
        with ThreadPoolExecutor(max_workers=6) as pool:
            futures.append(pool.submit(self.create_snapshots_per_keyspace_table, node, starter, num_ks=15, num_cf=15))
            futures.append(pool.submit(self.clear_snapshots_per_keyspace, node, starter, num_ks=15))
            futures.append(pool.submit(self.create_snapshot_for_all_keyspaces, node))
            futures.append(pool.submit(self.list_snapshots, node))
            futures.append(pool.submit(self.clear_all_snapshots, node))
            starter.set()

            for f in futures:
                results.append(f.result())
        self.verify_stderr_empty(results)

    def test_snapshot_parallel_in_complex_mode_creating_listing_clearing(self):
        """Test varios snapshot operations in parallel

        Verify that snapshot operations (create, list, clear)
        which running in parallel at same time, are not crashed
        and not return stderr
        Additionally run periodically the listsnapsots and clearsnapshot
        operations

        this test has very long time to run
        """

        def monitor_lists_snapshots(node, kill):
            while not kill.is_set():
                result = self.list_snapshots(node)
                self.verify_stderr_empty([result])
                kill.wait(1)

        def clear_snapshots_periodically(node, kill):
            while not kill.is_set():
                result = self.clear_all_snapshots(node)
                self.verify_stderr_empty([result])
                kill.wait(2)

        node, session = self.init_cluster()
        self.prepare_schemas_and_data(session, num_ks=15, num_cf=15, num_rows=1000, column_length=1000)
        logger.info("all KSes and CFes are created")

        kill = Event()
        futures = []
        results = []
        starter = Event()
        self.create_snapshot_for_all_keyspaces(node)
        with ThreadPoolExecutor(max_workers=6) as pool:
            futures.append(pool.submit(self.create_snapshot_for_all_keyspaces, node, starter))
            futures.append(pool.submit(self.create_snapshots_per_keyspace_table, node, starter, num_ks=15, num_cf=15))
            futures.append(pool.submit(self.clear_all_snapshots, node, starter))
            futures.append(pool.submit(self.create_snapshot_for_all_keyspaces, node, starter))
            starter.set()
            monitor = pool.submit(monitor_lists_snapshots, node, kill)
            clearing_snapshots = pool.submit(clear_snapshots_periodically, node, kill)
            for f in futures:
                results.append(f.result())
            kill.set()
            monitor.result()
            clearing_snapshots.result()

        self.verify_stderr_empty(results)


@pytest.mark.skip('Failing on scylla due to the schema.cql issue, https://github.com/scylladb/scylla/issues/7980')
@pytest.mark.dtest_full
@pytest.mark.single_node
class TestSchemaFileInSnapshot(SnapshotTester):
    native_column_types_and_values = {
        "bigint": ('10000', '1', '2'),
        "boolean": ('true', 'true', 'false'),
        "blob": ("textAsBlob('1234567890qwertyuiop')", "textAsBlob('a')", "bigintAsBlob(1)"),
        "date": ('currentDate()', 'currentDate()', 'currentDate()'),
        "decimal": ('10.1', '11.1', '12.2'),
        "double": ('10.1000001', '11.111111', '22.22222'),
        "duration": ('89h1m48s', '11h11m11s', '22h22m22s'),
        "float": ('10.10001', '33.33', '44.44'),
        "inet": ("'1.1.1.1'", "'1.1.1.1'", "'2.2.2.2'"),
        "int": ('100001', '1', '2'),
        "smallint": ('1', '1', '2'),
        "time": ('currentTime()', 'currentTime()', 'currentTime()'),
        "timestamp": ('currentTimestamp()', 'currentTimestamp()', 'currentTimestamp()'),
        "timeuuid": ('currentTimeUUID()', 'currentTimeUUID()', 'currentTimeUUID()'),
        "tinyint": ('1', '2', '5'),
        "uuid": ('uuid()', 'uuid()', 'uuid()'),
        "varint": ('1', '4', '5'),
        "text": ("'a'", "'A'", "'b'"),
        "varchar": ("'c'", "'d'", "'E'"),
        "ascii": ("'1'", "'c'", "'$'")
    }

    def test_schema_file_created(self):
        """Check that schema.cql file is in snapshot

        """
        node1, session = self.init_cluster()
        create_ks(session, "ks", 1)
        create_cf(session, name="cf", key_type="int", columns={"val": "text"})
        self.insert_rows(session, 0, 100)

        base_snapshot_dir = make_snapshot(node1, ks="ks", cf="cf", name="basic")
        schema_file = self.get_schema_file_from_snapshot(base_snapshot_dir, "ks", "cf", "basic")

        table_desc = get_table_description(node1, "ks", "cf")

        self.drop_keyspaces_and_clear_files(session, "ks", node1)
        create_ks(session, "ks", 1)
        self.restore_table_by_schema_file(session, schema_file)
        restored_table_desc = get_table_description(node1, "ks", "cf")

        assert table_desc == restored_table_desc

    def test_schema_file_created_by_multiple_tables(self):
        """Check that schema.cql file is in snapshot

        """
        node1, session = self.init_cluster()
        create_ks(session, "ks", 1)
        tables = self.create_tables(session=session, tables_number=5)
        self.insert_rows(session, 0, 100, cf=tables)

        tables_for_snapshot = ','.join(t for t in tables)
        base_snapshot_dir = make_snapshot(node1, ks="ks", cf=tables_for_snapshot, name="basic")
        schema_files = []
        tables_desc = []
        for table in tables:
            schema_files.append(self.get_schema_file_from_snapshot(base_snapshot_dir, "ks", table, "basic"))
            tables_desc.append(get_table_description(node1, "ks", table))

        self.drop_keyspaces_and_clear_files(session, "ks", node1)
        create_ks(session, "ks", 1)

        for table, schema_file, desc in zip(tables, schema_files, tables_desc):
            self.restore_table_by_schema_file(session, schema_file)
            restored_table_desc = get_table_description(node1, "ks", table)

            assert desc == restored_table_desc

    def test_restore_snapshot_by_table_schema_file_with_sstableloader(self):
        self.create_restore_data_with_snapshot(use_sstableloader=True)

    def test_restoring_by_schema_file_with_refresh(self):
        self.create_restore_data_with_snapshot(use_sstableloader=False)

    def test_restoring_by_schema_with_mv_use_sstableloader(self):
        self.create_restore_data_with_snapshot_with_mv(use_sstableloader=True, multiple_tables=False)

    def test_restoring_by_schema_with_mv_use_refresh(self):
        self.create_restore_data_with_snapshot_with_mv(use_sstableloader=False, multiple_tables=False)

    def test_restoring_by_schema_with_mv_use_sstableloader_multiple_tables(self):
        self.create_restore_data_with_snapshot_with_mv(use_sstableloader=True, multiple_tables=True)

    def test_restoring_by_schema_with_mv_use_refresh_multiple_tables(self):
        self.create_restore_data_with_snapshot_with_mv(use_sstableloader=False, multiple_tables=True)

    def test_restoring_snapshot_with_indexes_use_sstableloader(self):
        self.create_and_restore_data_with_snapshot_and_si(use_sstableloader=True, multiple_tables=False)

    def test_restoring_snapshot_with_indexes_use_refresh(self):
        self.create_and_restore_data_with_snapshot_and_si(use_sstableloader=False, multiple_tables=False)

    def test_restoring_snapshot_with_indexes_use_sstableloader_multiple_tables(self):
        self.create_and_restore_data_with_snapshot_and_si(use_sstableloader=True, multiple_tables=True)

    def test_restoring_snapshot_with_indexes_use_refresh_multiple_tables(self):
        self.create_and_restore_data_with_snapshot_and_si(use_sstableloader=False, multiple_tables=True)

    def test_restoring_snapshot_for_lsi_use_sstablesloader(self):
        self.create_restore_data_with_lsi_from_snapshot(use_sstableloader=True, multiple_tables=False)

    def test_restoring_snapshot_for_lsi_use_refresh(self):
        self.create_restore_data_with_lsi_from_snapshot(use_sstableloader=False, multiple_tables=False)

    def test_restoring_snapshot_for_lsi_use_sstablesloader_multiple_tables(self):
        self.create_restore_data_with_lsi_from_snapshot(use_sstableloader=True, multiple_tables=True)

    def test_restoring_snapshot_for_lsi_use_refresh_multiple_tables(self):
        self.create_restore_data_with_lsi_from_snapshot(use_sstableloader=False, multiple_tables=True)

    def test_schema_file_contains_altering_table_changes(self):
        node1, session = self.init_cluster_and_create_schema('ks', 'cf')

        self.insert_rows(session, 0, 100)
        base_snapshot_dir = make_snapshot(node1, ks='ks', cf='cf', name='basic')
        schema_file = self.get_schema_file_from_snapshot(base_snapshot_dir, 'ks', 'cf', 'basic')
        table_desc = get_table_description(node1, 'ks', 'cf')

        session.execute('ALTER TABLE ks.cf ADD val1 text')
        base_snapshot_dir = make_snapshot(node1, ks='ks', cf='cf', name='basic1')
        new_schema_file = self.get_schema_file_from_snapshot(base_snapshot_dir, 'ks', 'cf', 'basic1')
        altered_table_desc = get_table_description(node1, 'ks', 'cf')

        self.drop_keyspaces_and_clear_files(session, 'ks', node1)
        create_ks(session, 'ks', rf=1)

        self.restore_table_by_schema_file(session, new_schema_file)
        restored_altered_table_desc = get_table_description(node1, 'ks', 'cf')

        assert altered_table_desc == restored_altered_table_desc
        assert restored_altered_table_desc != table_desc
        assert self.read_schema_from_file(schema_file) != self.read_schema_from_file(new_schema_file)

    def test_restore_data_for_all_native_data_types_from_snapshot_with_sstablesloader(self):
        self.create_and_restore_data_all_native_datatypes(use_sstableloader=True)

    def test_restore_data_for_all_native_data_types_from_snapshot_with_refresh(self):
        self.create_and_restore_data_all_native_datatypes(use_sstableloader=False)

    @require("#5762")
    def test_restore_data_from_snapshot_with_udt_with_sstablesloader(self):
        self.create_and_restore_udt_from_snapshot(use_sstableloader=True)

    def test_restore_data_from_snapshot_with_udt_with_refresh(self):
        self.create_and_restore_udt_from_snapshot(use_sstableloader=False)

    def test_restore_data_from_snapshot_with_frozen_udt_with_sstablesloader(self):
        self.create_and_restore_udt_from_snapshot(use_sstableloader=True, use_frozen=True)

    def test_restore_data_from_snapshot_with_frozen_udt_with_refresh(self):
        self.create_and_restore_udt_from_snapshot(use_sstableloader=False, use_frozen=True)

    def test_upper_case_of_table_name_is_saved(self):
        node1, session = self.init_cluster()
        create_ks(session, 'ks', 1)
        session.execute('CREATE TABLE "UPPER_CASE_CF" ( "KEY" int PRIMARY KEY, "VAL" text);')
        session.execute("INSERT INTO \"UPPER_CASE_CF\" (\"KEY\", \"VAL\") VALUES (1, 'ASDFG');")

        base_snapshot_dir = make_snapshot(node1, ks='ks', cf='UPPER_CASE_CF', name='basic')
        schema_file = self.get_schema_file_from_snapshot(base_snapshot_dir, 'ks', 'UPPER_CASE_CF', 'basic')
        table_desc = get_table_description(node1, 'ks', '\"UPPER_CASE_CF\"')
        self.drop_keyspaces_and_clear_files(session, 'ks', node1)
        create_ks(session, 'ks', 1)
        self.restore_table_by_schema_file(session, schema_file)
        restored_table_desc = get_table_description(node1, 'ks', '\"UPPER_CASE_CF\"')

        assert table_desc == restored_table_desc

    def test_upper_case_of_table_name_is_saved_mixed_case(self):
        node1, session = self.init_cluster()
        create_ks(session, 'ks', 1)
        session.execute('CREATE TABLE "UPPER_CASE_CF" ( "KEY" int PRIMARY KEY, "VAL" text);')
        session.execute("INSERT INTO \"UPPER_CASE_CF\" (\"KEY\", \"VAL\") VALUES (1, 'ASDFG');")
        create_cf(session=session, name='cf', key_type='int', columns={'val': 'text'})
        self.insert_rows(session, 0, 10)

        base_snapshot_dir = make_snapshot(node1, ks='ks', cf='UPPER_CASE_CF,cf', name='basic')

        upper_schema_file = self.get_schema_file_from_snapshot(base_snapshot_dir, 'ks', 'UPPER_CASE_CF', 'basic')
        upper_table_desc = get_table_description(node1, 'ks', '\"UPPER_CASE_CF\"')

        lower_schema_file = self.get_schema_file_from_snapshot(base_snapshot_dir, 'ks', 'cf', 'basic')
        lower_table_desc = get_table_description(node1, 'ks', 'cf')

        self.drop_keyspaces_and_clear_files(session, 'ks', node1)
        create_ks(session, 'ks', 1)
        self.restore_table_by_schema_file(session, upper_schema_file)
        upper_restored_table_desc = get_table_description(node1, 'ks', '\"UPPER_CASE_CF\"')
        self.restore_table_by_schema_file(session, lower_schema_file)
        lower_restored_table_desc = get_table_description(node1, 'ks', 'cf')

        assert upper_table_desc == upper_restored_table_desc
        assert lower_table_desc == lower_restored_table_desc

    def create_restore_data_with_snapshot(self, use_sstableloader=True):
        node1, session = self.init_cluster_and_create_schema("ks", "cf")
        self.insert_rows(session, 0, 100)
        self.check_rows_number_in_table(session, "ks", "cf", 100)

        snapshot_dir = make_snapshot(node1, ks='ks', cf='cf', name='basic')

        # get table schema from schema file saved in snapshot
        schema_cql_file = self.get_schema_file_from_snapshot(snapshot_dir, 'ks', 'cf', 'basic')
        table_schema = get_table_description(node1, "ks", "cf")

        # Write more data after the snapshot, this will get thrown
        # away when we restore:
        self.insert_rows(session, 100, 200)
        self.check_rows_number_in_table(session, "ks", "cf", 200)

        # Drop the keyspace, make sure we have no data:
        self.drop_keyspaces_and_clear_files(session, "ks", node1)

        # Restore keyspace
        create_ks(session, 'ks', 1)

        self.restore_table_by_schema_file(session, schema_cql_file)

        restored_table_schema = get_table_description(node1, "ks", "cf")

        assert table_schema == restored_table_schema

        # check that data is not restored yet
        self.check_rows_number_in_table(session, "ks", "cf", 0)

        if use_sstableloader:
            restore_snapshot_with_sstableloader(snapshot_dir, node1, 'ks', 'cf')
        else:
            restore_snapshot_with_refresh(snapshot_dir, node1, 'ks', 'cf', 'basic')
            node1.nodetool('refresh ks cf')

        # check data correctly restored and updated
        self.check_rows_number_in_table(session, "ks", "cf", 100)

    def create_restore_data_with_snapshot_with_mv(self, use_sstableloader, multiple_tables):
        node1, session = self.init_cluster_and_create_schema("ks", "cf", mv=True)

        self.insert_rows(session, 0, 100)

        if multiple_tables:
            create_cf(session, name="cf1", key_type="int", columns={"val": "text"})
            self.insert_rows(session, 0, 100, cf="cf1")

        # create snapshot for keyspace
        if not multiple_tables:
            snapshot_dir_base_table = make_snapshot(node1, ks='ks')
        else:
            snapshot_dir_base_table = make_snapshot(node1, ks='ks', cf='cf,cf1')

        # expect explcit snapshot of view to fail
        logger.debug("Taking snapshot of mv. Expected to fail...")
        expected_error = 'take_snapshot failed'
        self.ignore_log_patterns.append(expected_error)
        with pytest.raises(NodetoolError) as ne:
            make_snapshot(node1, ks='ks', cf='cf_mv', name='expected_to_fail')

        # get schema.cql files for base table and mv
        schema_cql_file_basic_table = self.get_schema_file_from_snapshot(snapshot_dir_base_table, 'ks', 'cf')
        schema_table_desc = get_table_description(node1, "ks", "cf")
        schema_cql_file_mv = self.get_schema_file_from_snapshot(snapshot_dir_base_table, 'ks', 'cf_mv')
        schema_mv_desc = self.get_mv_description(node1, "ks", "cf_mv")

        # Write more data after the snapshot, this will get thrown
        # away when we restore:
        self.insert_rows(session, 100, 200)
        self.check_rows_number_in_table(session, "ks", "cf", 200)

        # check that MV was updated too
        self.check_rows_number_in_table(session, "ks", "cf_mv", 200)

        # Drop the keyspace, make sure we have no data:
        self.drop_keyspaces_and_clear_files(session, "ks", node1)

        # restore keyspace
        create_ks(session, 'ks', 1)

        self.restore_table_by_schema_file(session, schema_cql_file_basic_table)
        self.restore_table_by_schema_file(session, schema_cql_file_mv)

        # validate that base table and mv are empty
        self.check_rows_number_in_table(session, "ks", "cf", 0)
        self.check_rows_number_in_table(session, "ks", "cf_mv", 0)

        # checkt that restored table and mv are same as before
        restored_base_table_desc = get_table_description(node1, "ks", "cf")
        restored_mv_table_desc = self.get_mv_description(node1, "ks", "cf_mv")

        assert schema_table_desc == restored_base_table_desc
        assert schema_mv_desc == restored_mv_table_desc

        if use_sstableloader:
            restore_snapshot_with_sstableloader(snapshot_dir_base_table, node1, 'ks', 'cf')
            restore_snapshot_with_sstableloader(snapshot_dir_base_table, node1, 'ks', 'cf_mv')
        else:
            restore_snapshot_with_refresh(snapshot_dir_base_table, node1, 'ks', 'cf', wait_for_mv=True)

        # check data have been restored
        self.check_rows_number_in_table(session, "ks", "cf", 100)
        self.check_rows_number_in_table(session, "ks", "cf_mv", 100)

    def create_and_restore_data_with_snapshot_and_si(self, use_sstableloader, multiple_tables):
        node1, session = self.init_cluster_and_create_schema('ks', 'cf', si=True)

        self.insert_rows(session, 0, 100)

        if multiple_tables:
            create_cf(session, name="cf1", key_type="int", columns={"val": "text"})
            self.insert_rows(session, 0, 100, cf="cf1")

        self.check_rows_number_in_table(session, 'ks', 'cf', 100)
        # check secondary index
        self.check_rows_number_in_index(session, 'ks', 'cf', 100, "val", "'asdf'")

        if not multiple_tables:
            snapshot_dir = make_snapshot(node1, ks='ks')
        else:
            snapshot_dir = make_snapshot(node1, ks='ks', cf='cf,cf1')

        # expect explcit snapshot of view to fail
        logger.debug("Taking snapshot of index. Expected to fail...")
        expected_error = 'take_snapshot failed'
        self.ignore_log_patterns.append(expected_error)
        with pytest.raises(NodetoolError) as ne:
            make_snapshot(node1, ks='ks', cf='cf_ind', name='expected_to_fail')
        with pytest.raises(NodetoolError) as ne:
            make_snapshot(node1, ks='ks', cf='cf_ind_index', name='expected_to_fail')

        schema_cql_file_basic_table = self.get_schema_file_from_snapshot(snapshot_dir, 'ks', 'cf')
        base_table_desc = get_table_description(node1, 'ks', 'cf')
        schema_cql_file_index_table = self.get_schema_file_from_snapshot(snapshot_dir, 'ks', 'cf_ind_index')
        si_table_desc = self.get_index_description(node1, 'ks', 'cf_ind')

        # Write more data after the snapshot, this will get thrown
        # away when we restore:
        self.insert_rows(session, 100, 200)
        self.check_rows_number_in_table(session, "ks", "cf", 200)
        # check secondary index
        self.check_rows_number_in_index(session, "ks", "cf", 200, "val", "'asdf'")

        # Drop the keyspace, make sure we have no data:
        self.drop_keyspaces_and_clear_files(session, 'ks', node1)

        # restore data
        create_ks(session, 'ks', 1)

        # load schema and restore data
        self.restore_table_by_schema_file(session, schema_cql_file_basic_table)
        self.restore_table_by_schema_file(session, schema_cql_file_index_table)

        # checkt that restored table and mv are same as before
        restored_base_table_desc = get_table_description(node1, "ks", "cf")
        restored_index_table_desc = self.get_index_description(node1, "ks", "cf_ind")

        assert base_table_desc == restored_base_table_desc
        assert si_table_desc == restored_index_table_desc

        self.check_rows_number_in_table(session, "ks", "cf", 0)
        self.check_rows_number_in_index(session, "ks", "cf", 0, "val", "'asdf'")

        if use_sstableloader:
            restore_snapshot_with_sstableloader(snapshot_dir, node1, 'ks', 'cf')
            restore_snapshot_with_sstableloader(snapshot_dir, node1, 'ks', 'cf_ind_index')
        else:
            restore_snapshot_with_refresh(snapshot_dir, node1, 'ks', 'cf', wait_for_mv=True)

        self.check_rows_number_in_table(session, "ks", "cf", 100)
        self.check_rows_number_in_index(session, "ks", "cf", 100, index_column="val", value="'asdf'")
        self.insert_rows(session, 100, 200)
        self.check_rows_number_in_table(session, "ks", "cf", 200)
        self.check_rows_number_in_index(session, "ks", "cf", 200, index_column="val", value="'asdf'")

    def create_restore_data_with_lsi_from_snapshot(self, use_sstableloader, multiple_tables):
        node1, session = self.init_cluster_and_create_schema("ks", "cf", lsi=True)

        self.insert_rows(session, 0, 100)
        self.check_rows_number_in_table(session, "ks", "cf", 100)

        if multiple_tables:
            create_cf(session, name="cf1", key_type="int", columns={"val": "text"})
            self.insert_rows(session, 0, 100, cf="cf1")

        if not multiple_tables:
            snapshot_dir_base_table = make_snapshot(node1, ks='ks')
        else:
            snapshot_dir_base_table = make_snapshot(node1, ks='ks', cf='cf,cf1')

        # expect explcit snapshot of view to fail
        logger.debug("Taking snapshot of index. Expected to fail...")
        expected_error = 'take_snapshot failed'
        self.ignore_log_patterns.append(expected_error)
        with pytest.raises(NodetoolError) as ne:
            make_snapshot(node1, ks='ks', cf='cf_val', name='expected_to_fail')
        with pytest.raises(NodetoolError) as ne:
            make_snapshot(node1, ks='ks', cf='cf_val_index', name='expected_to_fail')

        schema_cql_file_basic_table = self.get_schema_file_from_snapshot(snapshot_dir_base_table, 'ks', 'cf')
        table_desc = get_table_description(node1, "ks", "cf")
        schema_cql_file_lsi = self.get_schema_file_from_snapshot(snapshot_dir_base_table, 'ks', 'cf_val_index')
        lsi_desc = self.get_index_description(node1, "ks", "cf_val")

        # Write more data after the snapshot, this will get thrown
        # away when we restore:
        self.insert_rows(session, 100, 200)
        self.check_rows_number_in_table(session, "ks", "cf", 200)

        # Drop the keyspace, make sure we have no data:
        self.drop_keyspaces_and_clear_files(session, 'ks', node1)

        # restore schema and data
        create_ks(session, 'ks', 1)

        # restore schema from schema.cql file
        self.restore_table_by_schema_file(session, schema_cql_file_basic_table)
        self.restore_table_by_schema_file(session, schema_cql_file_lsi)

        self.check_rows_number_in_table(session, 'ks', 'cf', 0)

        restored_table_desc = get_table_description(node1, 'ks', 'cf')
        restored_lsi_desc = self.get_index_description(node1, 'ks', 'cf_val')

        assert table_desc == restored_table_desc
        assert lsi_desc == restored_lsi_desc

        if use_sstableloader:
            restore_snapshot_with_sstableloader(snapshot_dir_base_table, node1, 'ks', 'cf')
            restore_snapshot_with_sstableloader(snapshot_dir_base_table, node1, 'ks', 'cf_val_index')
        else:
            restore_snapshot_with_refresh(snapshot_dir_base_table, node1, 'ks', 'cf', wait_for_mv=True)

        self.insert_rows(session, 0, 100)
        self.check_rows_number_in_table(session, 'ks', 'cf', 100)

    def create_and_restore_data_all_native_datatypes(self, use_sstableloader=True):
        node1, session = self.init_cluster()
        create_ks(session, 'ks', rf=1)
        cl_types = list(self.native_column_types_and_values.keys())
        columns = ""
        for cl_type in cl_types:
            columns += f"cl_{cl_type} {cl_type}, "
        session.execute(f"CREATE TABLE native_types_table ({columns} PRIMARY KEY (cl_{cl_types[0]}))")

        for i in range(3):
            columns = [f"cl_{cl_type}" for cl_type in cl_types]
            values = [f"{values[i]}" for _, values in self.native_column_types_and_values.items()]

            session.execute(f"INSERT INTO native_types_table ({', '.join(columns)}) VALUES ({', '.join(values)})")

        self.check_rows_number_in_table(session, 'ks', 'native_types_table', 3)

        snapshots_dir = make_snapshot(node1, ks='ks', name='basic')
        table_desc = get_table_description(node1, 'ks', 'native_types_table')
        schema_file = self.get_schema_file_from_snapshot(snapshots_dir, 'ks', 'native_types_table', 'basic')

        self.drop_keyspaces_and_clear_files(session, 'ks', node1)

        create_ks(session, 'ks', rf=1)

        self.restore_table_by_schema_file(session, schema_file)

        self.check_rows_number_in_table(session, 'ks', 'native_types_table', 0)

        restored_table_desc = get_table_description(node1, 'ks', 'native_types_table')
        if use_sstableloader:
            restore_snapshot_with_sstableloader(snapshots_dir, node1, 'ks', 'native_types_table', 'basic')
        else:
            restore_snapshot_with_refresh(snapshots_dir, node1, 'ks', 'native_types_table', 'basic')

        self.check_rows_number_in_table(session, 'ks', 'native_types_table', 3)

        assert table_desc == restored_table_desc

    def create_and_restore_udt_from_snapshot(self, use_sstableloader, use_frozen=False):
        node1, session = self.init_cluster()

        create_ks(session, 'ks', rf=1)

        cl_types = list(self.native_column_types_and_values.keys())
        columns = [f"cl_{cl_type} {cl_type}" for cl_type in cl_types]
        session.execute(f"CREATE TYPE all_native_types ({', '.join(columns)})")
        udt_type = "frozen<all_native_types>" if use_frozen else "all_native_types"
        session.execute(
            f"CREATE TABLE table_with_udt (cl_{cl_types[0]} {cl_types[0]}, data {udt_type}, PRIMARY KEY (cl_{cl_types[0]}))")

        for i in range(3):
            columns = [f"cl_{cl_type}" for cl_type in cl_types]
            udt_values = [f"cl_{cl_type}: {values[i]}" for cl_type,
                          values in self.native_column_types_and_values.items()]
            session.execute(f"INSERT INTO table_with_udt (cl_{cl_types[0]}, data) VALUES ({self.native_column_types_and_values[cl_types[0]][i]}, \
                            {{{', '.join(udt_values)}}})")

        self.check_rows_number_in_table(session, 'ks', 'table_with_udt', 3)

        snapshots_dir = make_snapshot(node1, ks='ks', name='basic')
        table_desc = get_table_description(node1, 'ks', 'table_with_udt')
        schema_file = self.get_schema_file_from_snapshot(snapshots_dir, 'ks', 'table_with_udt', 'basic')

        self.drop_keyspaces_and_clear_files(session, 'ks', node1)

        create_ks(session, 'ks', rf=1)
        columns = [f"cl_{cl_type} {cl_type}" for cl_type in cl_types]
        session.execute(f"CREATE TYPE all_native_types ({', '.join(columns)})")

        self.restore_table_by_schema_file(session, schema_file)

        self.check_rows_number_in_table(session, 'ks', 'table_with_udt', 0)

        restored_table_desc = get_table_description(node1, 'ks', 'table_with_udt')
        if use_sstableloader:
            restore_snapshot_with_sstableloader(snapshots_dir, node1, 'ks', 'table_with_udt', 'basic')
        else:
            restore_snapshot_with_refresh(snapshots_dir, node1, 'ks', 'table_with_udt', 'basic')

        self.check_rows_number_in_table(session, 'ks', 'table_with_udt', 3)

        assert table_desc == restored_table_desc

    def drop_keyspaces_and_clear_files(self, session, ks, node):
        session.execute(f'DROP KEYSPACE {ks}')
        node.rmtree(os.path.join(node.get_path(), 'data', ks))

    def restore_table_by_schema_file(self, session, schema_file):
        schema = self.read_schema_from_file(schema_file)
        session.execute(schema)

    def read_schema_from_file(self, schema_file):
        with open(schema_file, "r") as fp:
            content = fp.read()
        return content

    def get_mv_description(self, node, ks, mv):
        mv_desc = node.run_cqlsh(f"DESCRIBE MATERIALIZED VIEW {ks}.{mv}", return_output=True)
        return mv_desc[0]

    def get_index_description(self, node, ks, index):
        index_desc = node.run_cqlsh(f"describe index {ks}.{index}", return_output=True)
        return index_desc[0]

    def get_schema_file_from_snapshot(self, base_snapshot_dir: str, ks: str, cf: str, name: str = None) -> str:
        snapshot_dir = get_cf_snapshot_saved_dir(base_snapshot_dir, ks, cf, name)
        schema_file = os.path.join(snapshot_dir, "schema.cql")
        assert os.path.exists(schema_file)
        return schema_file

    def check_schema_file(self, schema_file: str, *attributes: List[str]) -> None:
        """Check that schema file is exists and contains provided attributes

        Validate that schema file was created and is in snapshot dir.
        Check that all attributes provided in attributes list are present
        in schema.cql file
        :param schema_file: path to schema.cql file
        :type schema_file: str
        :param *attributes: list of attributes to check in schema
        :type *attributes: List[str]
        """
        assert "schema.cql" in schema_file
        with open(schema_file, "r") as fp:
            content = fp.read()

        assert content
        for attribute in attributes:
            assert attribute in content

    def check_rows_number_in_table(self, session, ks, cf, number):
        rows = session.execute(f'SELECT count(*) from {ks}.{cf}')
        assert rows[0][0] == number

    def check_rows_number_in_index(self, session, ks, cf, number, index_column, value):
        rows = session.execute(f'SELECT count(*) from {ks}.{cf} WHERE {index_column} = {value}')
        assert rows[0][0], number

    def init_cluster_and_create_schema(self, ks, cf, mv=False, si=False, lsi=False):
        node, session = self.init_cluster()
        create_ks(session, ks, 1)
        create_cf(session, name=cf, key_type="int", columns={"val": "text"})
        if mv:
            session.execute(f'CREATE MATERIALIZED VIEW {cf}_mv AS SELECT val, key FROM ks.cf \
                              WHERE val IS NOT NULL PRIMARY KEY (val, key)')
        if si:
            create_index(session, cf, "val", f"{cf}_ind")
        if lsi:
            create_local_index(session, cf, "key", "val", index_name="cf_val")

        return node, session


@pytest.mark.dtest_full
@pytest.mark.single_node
class TestSnapshotOptions(SnapshotTester):
    SNAP_OPS = SnapshotOperations
    NODE_COUNT = 1
    KEYSPACE_COUNT = 3
    TABLE_COUNT = 2
    ROWS_PER_TABLE_COUNT = 1
    COLUMN_LENGTH = 10

    @lru_cache(maxsize=None)
    def prepare(self):
        self.node, self.session = self.init_cluster()
        self.system_keyspaces = self._get_system_keyspace_names()
        self.keyspaces = [f"ks{i}" for i in range(self.KEYSPACE_COUNT)]
        self.table_names = [f"table_cf{j}" for j in range(self.TABLE_COUNT)]
        self.SNAP_OPS.prepare_schemas_and_data(
            self,
            session=self.session,
            num_ks=self.KEYSPACE_COUNT,
            num_cf=self.TABLE_COUNT,
            num_rows=self.ROWS_PER_TABLE_COUNT,
            column_length=self.COLUMN_LENGTH)

    def test_snapshot_defaults_to_all_keyspaces(self):
        """
        Assert that using the nodetool snapshot command without
        specifying any keyspaces or tables defaults to a making
        a snapshot of all the keyspaces.
        1. Create a snapshot using 'nodetool snapshot'.
        2. Extract keyspace names from snapshot directory paths.
        3. Assert that extracted names contain all of the system keyspaces
        plus created keyspaces and that the number of names equals the sum
        of system_keyspace_count + created_keyspaces_count.
        """
        self.prepare()
        keyspaces = self.keyspaces + self.system_keyspaces
        keyspace_dir_names = self.base_case_make_snapshot_and_extract_ks_cf_names(
            make_snapshot_kwargs={"node": self.node}
        )[0]

        assert set(keyspace_dir_names) == set(keyspaces)

    def test_snapshot_of_specified_keyspaces_only(self):
        """
        Assert that using nodetool snapshot command with specifying
        keyspaces creates a snapshot of the specified keyspaces only,
        i.e. no other keyspaces are present in the snapshot
        directory.
        1. Create a snapshot using 'nodetool snapshot <keyspaces>'.
        Use one keyspace less than the full created keyspaces list.
        2. Extract keyspace names from snapshot directory paths.
        3. Assert that extracted names contain only the keyspace
        names provided to the 'nodetool snapshot <keyspcaes>'
        command.
        """
        self.prepare()
        keyspaces_to_snap = self._get_random_keyspaces_to_snap()
        keyspace_dir_names = self.base_case_make_snapshot_and_extract_ks_cf_names(
            make_snapshot_kwargs={"node": self.node, "ks": ','.join(keyspaces_to_snap)}
        )[0]

        assert set(keyspace_dir_names) == set(keyspaces_to_snap)

    def test_snapshot_of_specified_keyspaces_only_with_kc_list_option(self):
        """
        Assert that using nodetool snapshot command with specifying
        keyspaces  with the '-kc' option creates a snapshot of the
        specified keyspaces only, i.e. no other keyspaces are present
        in the snapshot directory.
        1. Create a snapshot using 'nodetool
        snapshot -kc <keyspaces>'.
        Use one keyspace less than the full created keyspaces list.
        2. Extract keyspace names from snapshot directory paths.
        3. Assert that extracted names contain only the keyspace
        names provided to the 'nodetool snapshot <keyspcaes>'
        command.
        """
        self.prepare()
        keyspaces_to_snap = self._get_random_keyspaces_to_snap()
        keyspace_dir_names = self.base_case_make_snapshot_and_extract_ks_cf_names(
            make_snapshot_kwargs={"node": self.node, "additional_options": [f"-kc {','.join(keyspaces_to_snap)}"]}
        )[0]

        assert set(keyspace_dir_names) == set(keyspaces_to_snap)

    def test_snapshot_tagging(self):
        """
        Assert that when using the '-t' option for specifying a
        snapshot tag, the snapshot is named according to the
        parameter provided for that option.
        1. Execute the nodetool snapshot command with the '-t' option.
        2. Check the stdout for the name of the snapshot.
        """
        self.prepare()
        tag = "charybdis"

        assert f"Snapshot directory: {tag}" in self.node.nodetool(cmd=f"snapshot -t {tag}")[0]

    def test_snapshot_skip_flush(self):
        """
        Assert that using the '-sf'/'--skip-flush' forces nodetool
        to make a snapshot of the data without flushing memtables.
        1. Insert a few rows of data to populate the memtable.
        2. Query 'nodetool tablestats' for memtable data size and
        number of memtable switches.
        3. Use 'nodetool snapshot --skip-flush' to trigger the
        snapshot without flushing the memtable.
        4. Query 'nodetool tablestats' again for memtable data size
        and number of memtable switches.
        5. Assert that memtable data size is greater or equal to
        before the snapshot.
        6. Assert that the number of memtable switches is equal to
        the number before the snapshot.
        """
        self.prepare()
        table = self.table_names[0]
        ks = self.keyspaces[0]
        self._insert_rows_into_ks_cf(insert_row_count=2, ks=ks, cf=table)
        stdout_pre, stderr_pre = self._get_tabestats_for_table(
            keyspace=ks,
            table=table
        )

        memtable_data_size_pre, memtable_switch_count_pre = self._get_memtable_stats_from_tablestats(stdout_pre)
        logger.info("memtable_data_size_pre=%s memtable_switch_count_pre=%s",
                    memtable_data_size_pre, memtable_switch_count_pre)
        self.base_case_make_snapshot_and_extract_ks_cf_names(
            make_snapshot_kwargs={"node": self.node,
                                  "ks": ks,
                                  "cf": table,
                                  "additional_options": [
                                      "--skip-flush"]}
        )

        stdout_post, stderr_post = self._get_tabestats_for_table(
            keyspace=ks,
            table=table
        )
        memtable_data_size_post, memtable_switch_count_post = self._get_memtable_stats_from_tablestats(stdout_post)
        logger.info("memtable_data_size_post=%s memtable_switch_count_post=%s",
                    memtable_data_size_post, memtable_switch_count_post)

        assert memtable_data_size_post >= memtable_data_size_pre, \
            f"Expected memtable data size after the skip-flush snapshot to be greater or equal " \
            f"to size before snapshot, but was not.\nSize pre: {memtable_data_size_pre}\n" \
            f"Size post: {memtable_data_size_post}"
        assert memtable_switch_count_pre == memtable_switch_count_post, \
            "Expected memtable switch count after the skip-flush snapshot to be equal to the switch " \
            f"count before the snapshot, but was not.\nSwitch count pre: {memtable_switch_count_pre}" \
            f"\nSwitch count post: {memtable_switch_count_post}"

    def base_case_make_snapshot_and_extract_ks_cf_names(
            self,
            make_snapshot_kwargs: Dict[str, Any]) -> Tuple[List[str], List[str], str]:
        """
        Base case collecting common steps for other TestSnapshotOptions
        test cases.
        1. Make a snapshot with the given keyword args.
        2. Extract keyspace and table dir names from the temporary
        dir housing the snapshot dir copy.
        3. Return a tuple of keyspace dir names list, table subdir
        names list and the snapshot root dir name.
        """
        snapshot_root_dir = make_snapshot(**make_snapshot_kwargs)
        snapshot_dir_paths = self._get_snapshot_dir_paths(snapshot_root_dir)
        keyspace_dir_names = self._parse_names_from_paths(snapshot_dir_paths["keyspace_dirs"])
        table_subdirs_names = self._parse_names_from_paths(snapshot_dir_paths["table_subdirs"])

        return keyspace_dir_names, table_subdirs_names, snapshot_root_dir

    def _insert_rows_into_ks_cf(self, insert_row_count: int = 1, ks: str = "ks0", cf: str = "table_cf0"):
        insert_statement = self.session.prepare(f"INSERT INTO {ks}.{cf} (key, c, v) "
                                                f"VALUES (?, 'sometext', 'someothertext')")
        logger.info("Inserting %d rows into keyspace %s, column family: %s", insert_row_count, ks, cf)
        for i in range(self.ROWS_PER_TABLE_COUNT, self.ROWS_PER_TABLE_COUNT + insert_row_count):
            args = [(str(i),)]
            execute_concurrent_with_args(self.session, insert_statement, args, concurrency=20)

    def _get_random_keyspaces_to_snap(self) -> List[str]:
        keyspace_to_omit = random.choice(self.keyspaces)
        keyspaces = self.keyspaces.copy()
        keyspaces.remove(keyspace_to_omit)

        return keyspaces

    def _get_tabestats_for_table(self, keyspace: str, table: str) -> Tuple[str, str]:
        nodetool_cmd = f"tablestats {keyspace}.{table}"
        stdout, stderr = self.node.nodetool(nodetool_cmd)

        return stdout, stderr

    def _get_system_keyspace_names(self):
        query = "select keyspace_name from system_schema.keyspaces"
        keyspace_list = [item.keyspace_name for item in self.session.execute(query=query).all()]

        return keyspace_list

    @staticmethod
    def _get_memtable_stats_from_tablestats(tablestats_stdout: str) -> Tuple[int, int]:
        memtable_data_size_pattern = re.compile(r'(?:Memtable data size:\s*)(\d+)')
        memtable_switch_count_pattern = re.compile(r'(?:Memtable switch count:\s*)(\d+)')
        memtable_data_szie = int(memtable_data_size_pattern.search(tablestats_stdout)
                                 .group(1))
        memtable_switch_count = int(memtable_switch_count_pattern.search(tablestats_stdout)
                                    .group(1))

        return memtable_data_szie, memtable_switch_count

    @staticmethod
    def _get_snapshot_dir_paths(snapshot_root_path: str) -> Dict[str, List[Path]]:
        root = Path(snapshot_root_path)
        keyspace_dirs = [path for path in root.glob("*") if path.is_dir]
        table_subdirs = [path for subdir in keyspace_dirs for path in subdir.glob("*") if path.is_dir]

        return {"keyspace_dirs": keyspace_dirs, "table_subdirs": table_subdirs}

    @staticmethod
    def _parse_names_from_paths(path_list: List[Path]) -> List[str]:
        return [path.stem for path in path_list]
