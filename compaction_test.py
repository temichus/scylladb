import datetime
import logging
import os
import random
import re
import tempfile
import time
from typing import Dict

import pytest
from cassandra import ConsistencyLevel
from pkg_resources import parse_version

from ccmlib.node import NodetoolError
from dtest_class import Tester, create_ks, is_autocompaction_enabled, retry_till_success
from dtest_setup_overrides import DTestSetupOverrides
from tools.assertions import assert_none, assert_one
from tools.data import create_c1c2_table, insert_c1c2, chunks_list
from tools.files import get_node_cf_dir, copy_files_to
from tools.marks import enterprise_only_param
from tools.misc import ImmutableMapping
from tools.rest_clients import StorageServiceClient
from tools.stress import fill_data_by_cs
from tools.cluster import parallel_nodetool, run_rest_api

logger = logging.getLogger(__file__)


@pytest.mark.dtest_full
@pytest.mark.single_node
@pytest.mark.parametrize('strategy', [
    'LeveledCompactionStrategy',
    'SizeTieredCompactionStrategy',
    'TimeWindowCompactionStrategy',
    enterprise_only_param('IncrementalCompactionStrategy')
])
class TestCompaction(Tester):
    strategy = None
    PROPAGATION_DELAY_IN_SECONDS = 5
    KEYSPACE_NAME = 'ks'
    FULL_TABLE_NAME = f"{KEYSPACE_NAME}.cf"

    @pytest.fixture(scope='function', autouse=True)
    def fixture_compaction_strategy(self, strategy):
        self.strategy = strategy
        dtest_setup_overrides = DTestSetupOverrides()
        dtest_setup_overrides.cluster_options = ImmutableMapping({'start_rpc': 'true'})
        return dtest_setup_overrides

    def _test_compaction_delete(self):
        cluster = self.cluster
        cluster.populate(1).start(wait_for_binary_proto=True)
        [node1] = cluster.nodelist()

        session = self.patient_cql_connection(node1)
        create_ks(session, 'ks', 1)

        gc_grace_seconds = 60
        session.execute("create table ks.cf (key int PRIMARY KEY, val int) "
                        "with compaction = {{'class':'{}'}} and gc_grace_seconds = {};".format(self.strategy, gc_grace_seconds))

        for x in range(0, 100):
            session.execute('insert into cf (key, val) values (' + str(x) + ',1)')

        node1.flush()
        self.tombstone_expiry_time = time.time() + gc_grace_seconds
        for x in range(0, 10):
            session.execute('delete from cf where key = ' + str(x))

        node1.flush()
        for x in range(0, 10):
            assert_none(session, 'select * from cf where key = ' + str(x))

        json_path = tempfile.mkstemp(suffix='.json')
        jname = json_path[1]
        with open(jname, 'w') as f:
            node1.run_sstable2json(f)

        with open(jname, 'r') as g:
            jsoninfo = g.read()

        numfound = jsoninfo.count("marked_deleted")

        assert numfound == 10, "Error: expected {} deleted partitions but found {}:\n{}".format(
            10, numfound, jsoninfo)

    def test_compaction_delete(self):
        """
        Test that executing a delete properly tombstones a row.
        Insert data, delete a partition of data and check that the requisite rows are tombstoned.
        """
        self._test_compaction_delete()

    def test_compaction_delete_2(self):
        """
        Test that executing a delete properly tombstones a row.
        Insert data, delete a partition of data, compact and test
        Wait past gc_period, compact and test.
        """

        self._test_compaction_delete()
        [node1] = self.cluster.nodelist()
        session = self.patient_cql_connection(node1)

        # check that after compaction the tombstones remain
        # force an update so that compact will have something to do
        session.execute('insert into ks.cf (key, val) values (99,1);')
        node1.flush()
        node1.compact()

        json_path = tempfile.mkstemp(suffix='.json')
        jname = json_path[1]
        with open(jname, 'w') as f:
            node1.run_sstable2json(f)

        with open(jname, 'r') as g:
            jsoninfo = g.read()

        numfound = jsoninfo.count("marked_deleted")

        time_to_expire = self.tombstone_expiry_time - time.time()
        logger.debug("Time left to expire: {}".format(time_to_expire))
        assert time_to_expire > 0, "Error: missed tombstone expiration time: {} >= {}".format(
            time.time(), self.tombstone_expiry_time)

        assert numfound == 10, "Error: expected {} deleted partitions but found {}:\n{}".format(
            10, numfound, jsoninfo)

        time.sleep(time_to_expire + 1)

        # check that after gc_period compaction removes tombstones
        # force an update so that compact will have something to do
        session.execute('insert into ks.cf (key, val) values (99,1);')
        node1.flush()
        node1.compact()

        json_path = tempfile.mkstemp(suffix='.json')
        jname = json_path[1]
        with open(jname, 'w') as f:
            node1.run_sstable2json(f)

        with open(jname, 'r') as g:
            jsoninfo = g.read()

        numfound = jsoninfo.count("marked_deleted")

        assert numfound == 0, "Error: expected {} deleted partitions but found {}:\n{}".format(0, numfound, jsoninfo)

    def verify_deleted(self, session, node, num_deleted_rows):
        """
            This method verifies there are <num_deleted_rows> deleted rows (tombstones) on node.
        """
        count, jsoninfo = self.count_deleted(session=session, node=node)
        assert count == num_deleted_rows, "Error: expected {} deleted partitions but found {}:\n{}".format(
            num_deleted_rows, count, jsoninfo)

    def count_deleted(self, session, node):
        """
            Count number of tombstones on node.
            Return count nuber and json-info of node sstables.
        """
        session.execute(f'insert into {self.FULL_TABLE_NAME} (key, val) values (99,1);')
        logger.debug(f"Run nodetool flush and compact on node: {node.name}")
        node.flush()
        node.compact()

        json_path = tempfile.mkstemp(suffix='.json')
        jname = json_path[1]
        with open(jname, 'w') as f:
            node.run_sstable2json(f, keyspace=self.KEYSPACE_NAME)

        with open(jname, 'r') as g:
            jsoninfo = g.read()

        numfound = jsoninfo.count("marked_deleted")

        logger.debug(f'Number of tombstones found on node {node.name}: {numfound}')
        return numfound, jsoninfo

    def _test_compaction_delete_tombstone_gc(self, tombstone_gc_mode='repair', node_num: int = 2,
                                             r_factor: int = None, delete_keys: bool = True, partition_num: int = 100):
        """
        Start all cluster nodes.
        Create table with RF and tombstone_gc_mode option
        Insert partition_num (100) rows
        Delete 10 rows by default
        """
        cluster = self.cluster
        cluster.populate(node_num).start(wait_for_binary_proto=True)
        node1 = cluster.nodelist()[0]

        session = self.patient_cql_connection(node1)
        r_factor = r_factor or node_num
        create_ks(session, self.KEYSPACE_NAME, rf=r_factor)

        if tombstone_gc_mode == 'timeout':
            gc_grace_seconds = 60
        else:
            gc_grace_seconds = 5

        logger.debug(f'Create table with tombstone_gc = mode ={tombstone_gc_mode}')
        session.execute(f"create table {self.FULL_TABLE_NAME} (key int PRIMARY KEY, val int) "
                        f"with tombstone_gc = {{'mode':'{tombstone_gc_mode}', 'propagation_delay_in_seconds':'{self.PROPAGATION_DELAY_IN_SECONDS}'}} "
                        f"and compaction = {{'class':'{self.strategy}'}} and gc_grace_seconds = {gc_grace_seconds};")

        for x in range(0, partition_num):
            session.execute(f'insert into {self.FULL_TABLE_NAME} (key, val) values ({x},1)')

        for node in cluster.nodelist():
            node.flush()
        self.tombstone_expiry_time = time.time() + gc_grace_seconds
        if delete_keys:
            self._delete_keys()

    def _delete_keys(self, verify_deleted: bool = True, num: int = 10):
        session = self.patient_cql_connection(self.cluster.nodelist()[0])

        for idx in range(0, num):
            session.execute(f'delete from {self.FULL_TABLE_NAME} where key = {idx}')

        if verify_deleted:
            for node in self.cluster.nodelist():
                node.flush()
            for idx in range(0, num):
                assert_none(session, f'select * from {self.FULL_TABLE_NAME} where key = {idx}')

    @pytest.mark.parametrize("tombstone_gc_mode", ['repair', 'timeout', 'disabled', 'immediate'])
    def test_compaction_delete_tombstone_gc(self, tombstone_gc_mode):
        """
        Test compaction drop tombstones correctly in different tombstone_gc_mode mode
        """
        assert tombstone_gc_mode in ['repair', 'timeout', 'disabled',
                                     'immediate'], f"tombstone_gc_mode {tombstone_gc_mode} is not supported"

        self._test_compaction_delete_tombstone_gc(tombstone_gc_mode)

        node1, node2 = self.cluster.nodelist()
        session = self.patient_cql_connection(node1)

        if tombstone_gc_mode == 'immediate':
            logger.debug(f"Check with tombstone_gc_mode = {tombstone_gc_mode}, before timeout there are no tombstones")
            self.verify_deleted(session, node1, 0)
            self.verify_deleted(session, node2, 0)
        else:
            logger.debug(f"Check with tombstone_gc_mode = {tombstone_gc_mode}, before timeout there are 10 tombstones")
            self.verify_deleted(session, node1, 10)
            self.verify_deleted(session, node2, 10)

        time_to_expire = max(0, self.tombstone_expiry_time - time.time())
        logger.debug("Time left to expire: {}".format(time_to_expire))

        logger.debug("Sleep time_to_expire")
        time.sleep(time_to_expire + 1)

        if tombstone_gc_mode in ('immediate', 'timeout'):
            logger.debug(f"Check with tombstone_gc_mode = {tombstone_gc_mode}, before repair there are no tombstones")
            self.verify_deleted(session, node1, 0)
            self.verify_deleted(session, node2, 0)
        elif tombstone_gc_mode in ('repair', 'disabled'):
            logger.debug(f"Check with tombstone_gc_mode = {tombstone_gc_mode}, before repair there are 10 tombstones")
            self.verify_deleted(session, node1, 10)
            self.verify_deleted(session, node2, 10)

        logger.debug("Run repair on node1")
        node1.repair(['ks cf'])

        if tombstone_gc_mode in ('repair', 'immediate', 'timeout'):
            logger.debug(f"Check with tombstone_gc_mode = {tombstone_gc_mode}, after repair there are no tombstones")
            self.verify_deleted(session, node1, 0)
            self.verify_deleted(session, node2, 0)
        elif tombstone_gc_mode == 'disabled':
            logger.debug(
                f"Check with tombstone_gc_mode = {tombstone_gc_mode}, after repair there are still 10 tombstones")
            self.verify_deleted(session, node1, 10)
            self.verify_deleted(session, node2, 10)

    @staticmethod
    def repair_and_wait_for_off_strategy(node, table: str = FULL_TABLE_NAME):
        log_mark = node.mark_log()
        node.repair()
        ks, cf = table.split('.')
        run_rest_api(node, f"/storage_service/keyspace_offstrategy_compaction/{ks}?cf={cf}")
        node.watch_log_for(f"Done with off-strategy compaction for {table}", timeout=300, from_mark=log_mark,
                           verbose=True)

    def test_delete_tombstone_gc_node_down(self):
        """
        Test compaction drop tombstones correctly in 'repair' tombstone_gc_mode mode
        And a node that is temporarily down.
        1. Create 4 nodes cluster + RF = 3.
        2. Write some keys.
        3. Stop node4
        4. Delete data on other nodes and flush.
        5. Run a repair on node1 where some repaired rows succeed (the repair eventually fails).
        6. Run major compaction as well.
        7. Verify node1 now has same tombstones as before the repair.
        8. Start node4.
        9. Count node4 tombstones and verify no tombstones after repairing the nodes + compact.
        """
        partition_num = 10
        self._test_compaction_delete_tombstone_gc('repair', node_num=4, r_factor=3, delete_keys=False,
                                                  partition_num=partition_num)
        node1, node2, node3, node4 = self.cluster.nodelist()

        logger.debug("Stopping node4")
        node4.stop(wait_other_notice=True, gently=False)

        self._delete_keys(verify_deleted=False, num=partition_num // 2)
        node1.flush()
        node2.flush()
        node3.flush()

        with self.patient_exclusive_cql_connection(node1) as session:
            total_tombstones_num_before_repair = 0
            for node in [node1, node2, node3]:
                numfound, _ = self.count_deleted(session=session, node=node)
                total_tombstones_num_before_repair += numfound
            logger.debug("Run a partially-failing repair on node1")
            with pytest.raises(NodetoolError):
                node1.repair(['ks cf'])

            logger.debug(
                f"Check, when node4 is down, the number of tombstones on nodes 1,2,3 is unchanged following a repair")
            total_tombstones_num_after_repair = 0
            for node in [node1, node2, node3]:
                numfound, _ = self.count_deleted(session=session, node=node)
                total_tombstones_num_after_repair += numfound
            assert total_tombstones_num_after_repair == total_tombstones_num_before_repair

        logger.debug("Starting node4")
        log_mark = node4.mark_log()
        node4.start(wait_other_notice=True, wait_for_binary_proto=True)
        node4.watch_log_for(f"Done with off-strategy compaction for {self.FULL_TABLE_NAME}", timeout=300,
                            from_mark=log_mark, verbose=True)
        with self.patient_cql_connection(node4, consistency_level=ConsistencyLevel.QUORUM) as session:
            logger.debug("Running a repair on all nodes")
            self.repair_and_wait_for_off_strategy(node=node4)
            repair_history = list(session.execute("SELECT table_name from system.repair_history"))
            assert any("cf" in repair for repair in repair_history)
            parallel_nodetool([node1, node2, node3], 'repair -pr ks cf')
            logger.debug(
                f"Check with tombstone_gc_mode = repair, after a full successful repair there are no tombstones")
            self.verify_deleted(session=session, node=node1, num_deleted_rows=0)
            self.verify_deleted(session=session, node=node2, num_deleted_rows=0)
            self.verify_deleted(session=session, node=node3, num_deleted_rows=0)
            self.verify_deleted(session=session, node=node4, num_deleted_rows=0)

    def test_data_size(self):
        """
        Ensure that data size does not have unwarranted increases after compaction.
        Insert data and check data size before and after a compaction.
        """
        cluster = self.cluster
        cluster.populate(1).start(wait_for_binary_proto=True)
        [node1] = cluster.nodelist()

        stress_write(node1)

        node1.flush()

        table_name = 'standard1'
        output = node1.nodetool('cfstats', True)[0]
        if output.find(table_name) != -1:
            output = output[output.find(table_name):]
            output = output[output.find("Space used (live)"):]
            initial_value = int(output[output.find(":") + 1:output.find("\n")].strip())
        else:
            logger.debug("datasize not found")
            logger.debug(output)

        block_on_compaction_log(node1)

        output = node1.nodetool('cfstats', True)[0]
        if output.find(table_name) != -1:
            output = output[output.find(table_name):]
            output = output[output.find("Space used (live)"):]
            final_value = int(output[output.find(":") + 1:output.find("\n")].strip())
        else:
            logger.debug("datasize not found")

        assert final_value <= initial_value

    @pytest.mark.next_gating
    @pytest.mark.dtest_debug
    def test_sstable_deletion(self):
        """
        Test that sstables are deleted properly when able after compaction.
        Insert data setting gc_grace_seconds to 0, and determine sstable
        is deleted upon data deletion.
        """
        self.skip_if_no_major_compaction()
        cluster = self.cluster
        cluster.populate(1).start(wait_for_binary_proto=True)
        [node1] = cluster.nodelist()
        session = self.patient_cql_connection(node1)
        create_ks(session, 'ks', 1)
        session.execute("create table cf (key int PRIMARY KEY, val int) with gc_grace_seconds = 0 and "
                        "compaction= {'class':'" + self.strategy + "'}")

        for x in range(0, 100):
            session.execute(f'insert into cf (key, val) values ({x},{x})')
        node1.flush()
        for x in range(0, 100):
            session.execute('delete from cf where key = ' + str(x))
        time.sleep(1)   # to make sure the tombstones will gc-expire

        block_on_compaction_log(node1, ks='ks', table='cf')
        time.sleep(1)

        try:
            path = os.path.join(node1.get_path(), "data", "ks")
            cfs = os.listdir(path)
            path = os.path.join(path, cfs[0])
            ssdir = os.listdir(path)
            found = [afile for afile in ssdir if "Data" in afile]
            if found:
                msg = f"Expected no SSTables in {path}, but found: {found}"
                logger.error(msg)
                json_path = tempfile.mkstemp(suffix='.json')
                jname = json_path[1]
                with open(jname, 'w') as f:
                    node1.run_sstable2json(out_file=f, keyspace='ks', column_families=['cf'])
                with open(jname, 'r') as g:
                    jsoninfo = g.read()
                    logger.debug(f"{jsoninfo}")
                pytest.fail(msg)

        except OSError:
            pytest.fail("Path to sstables not valid.")

    @pytest.mark.skip('sstable is already removed when expecting it to be marked as expired only')
    def test_dtcs_deletion(self):
        """
        Test that sstables are deleted properly when able after compaction with
        DateTieredCompactionStrategy.
        Insert data setting max_sstable_age_days low, and determine sstable
        is deleted upon data deletion past max_sstable_age_days.
        """
        if not hasattr(self, 'strategy'):
            self.strategy = 'DateTieredCompactionStrategy'
        elif self.strategy != 'DateTieredCompactionStrategy':
            pytest.skip('Not implemented unless DateTieredCompactionStrategy is used')

        cluster = self.cluster
        cluster.populate(1).start(wait_for_binary_proto=True)
        [node1] = cluster.nodelist()
        session = self.patient_cql_connection(node1)
        create_ks(session, 'ks', 1)
        # max sstable age is 0.5 minute:
        session.execute("create table cf (key int PRIMARY KEY, val int) with gc_grace_seconds = 0 and "
                        "compaction= {'class':'DateTieredCompactionStrategy', 'max_sstable_age_days':0.00035, "
                        "'min_threshold':2}")

        # insert data
        for x in range(0, 300):
            session.execute('insert into cf (key, val) values (' + str(x) + ',1) USING TTL 35')
        node1.flush()
        time.sleep(40)
        expired_sstables = node1.get_sstables('ks', 'cf')
        assert len(expired_sstables) == 1
        expired_sstable = expired_sstables[0]
        # write a new sstable to make DTCS check for expired sstables:
        for x in range(0, 100):
            session.execute('insert into cf (key, val) values (%d, %d)' % (x, x))
        node1.flush()
        time.sleep(5)
        # we only check every 10 minutes - sstable should still be there:
        assert expired_sstable in node1.get_sstables('ks', 'cf')

        session.execute("alter table cf with "
                        "compaction =  {'class':'DateTieredCompactionStrategy', 'max_sstable_age_days':0.00035, "
                        "'min_threshold':2, 'expired_sstable_check_frequency_seconds':0}")
        time.sleep(1)
        for x in range(0, 100):
            session.execute('insert into cf (key, val) values (%d, %d)' % (x, x))
        node1.flush()
        time.sleep(5)
        assert expired_sstable not in node1.get_sstables('ks', 'cf')

    @pytest.mark.skip('nodetool command setcompactionthroughput not supported in scylla')
    def test_compaction_throughput(self):
        """
        Test setting compaction throughput.
        Set throughput, insert data and ensure compaction performance corresponds.
        """
        cluster = self.cluster
        cluster.populate(1).start(wait_for_binary_proto=True)
        [node1] = cluster.nodelist()

        # disableautocompaction only disables compaction for existing tables,
        # so initialize stress tables with stress first
        stress_write(node1, keycount=1)
        node1.nodetool('disableautocompaction')

        stress_write(node1, keycount=200000)

        threshold = "5"
        node1.nodetool('setcompactionthroughput -- ' + threshold)

        matches = block_on_compaction_log(node1)
        stringline = matches[0]
        throughput_pattern = re.compile(r'''.*          # it doesn't matter what the line starts with
                                           =           # wait for an equals sign
                                           ([\s\d\.]*) # capture a decimal number, possibly surrounded by whitespace
                                           MB/s.*      # followed by 'MB/s'
                                        ''', re.X)

        avgthroughput = re.match(throughput_pattern, stringline).group(1).strip()
        logger.debug(avgthroughput)

        assert float(threshold) >= float(avgthroughput)

    @pytest.mark.parametrize("strategies", argvalues=(
        ['LeveledCompactionStrategy', 'SizeTieredCompactionStrategy', 'TimeWindowCompactionStrategy'],
        enterprise_only_param(['IncrementalCompactionStrategy'])), ids=("oss", "enterprise"))
    def test_compaction_strategy_switching(self, strategies):
        """Ensure that switching strategies does not result in problems.
        Insert data, switch strategies, then check against data loss.
        """
        # clone the list, so we can remove self.strategy out of it
        strategies = strategies[:]

        if self.strategy in strategies:
            strategies.remove(self.strategy)

        cluster = self.cluster
        cluster.populate(1).start(wait_for_binary_proto=True)
        [node1] = cluster.nodelist()

        for strat in strategies:
            session = self.patient_cql_connection(node1)
            create_ks(session, 'ks', 1)

            session.execute("create table ks.cf (key int PRIMARY KEY, val int) with gc_grace_seconds = 0 "
                            "and compaction= {'class':'" + self.strategy + "'};")

            for x in range(0, 100):
                session.execute('insert into ks.cf (key, val) values (' + str(x) + ',1)')

            node1.flush()

            for x in range(0, 10):
                session.execute('delete from cf where key = ' + str(x))

            session.execute("alter table ks.cf with compaction = {'class':'" + strat + "'};")

            for x in range(11, 100):
                assert_one(session, "select * from ks.cf where key =" + str(x), [x, 1])

            for x in range(0, 10):
                assert_none(session, 'select * from cf where key = ' + str(x))

            node1.flush()
            cluster.clear()
            time.sleep(5)
            cluster.start(wait_for_binary_proto=True)

    def test_large_compaction_warning(self):
        """
        @jira_ticket CASSANDRA-9643
        Check that we log a warning when the partition size is bigger than
        compaction_large_partition_warning_threshold_mb
        """
        cluster = self.cluster
        cluster.set_configuration_options({'compaction_large_partition_warning_threshold_mb': 10})
        cluster.populate(1).start(wait_for_binary_proto=True)
        [node] = cluster.nodelist()

        session = self.patient_cql_connection(node)
        create_ks(session, 'ks', 1)

        strlen = (10 * 1024 * 1024) // 100
        session.execute("CREATE TABLE large(userid text PRIMARY KEY, properties map<int, text>) with compression = {}")
        for i in range(200):  # ensures partition size larger than compaction_large_partition_warning_threshold_mb
            session.execute("UPDATE ks.large SET properties[%i] = '%s' "
                            "WHERE userid = 'user'" % (i, get_random_word(strlen)))

        ret = list(session.execute("SELECT properties from ks.large where userid = 'user'"))
        assert len(ret) == 1
        assert 200 == len(ret[0][0].keys())

        node.flush()
        mark = node.mark_log()
        node.nodetool('compact ks large')
        node.watch_log_for(r'Writing large row ks/large:.* \(\d+ bytes\)', from_mark=mark, timeout=180)

        ret = list(session.execute("SELECT properties from ks.large where userid = 'user'"))

        assert len(ret) == 1
        assert 200 == len(ret[0][0].keys())

        # Check that system.large_partitions contains this large entry
        large_partition_ret = list(session.execute("SELECT * from system.large_partitions"))
        assert len(large_partition_ret) == 1
        row = large_partition_ret[0]
        assert row.partition_size == pytest.approx(20974000, 50)
        assert row.partition_key == 'user'

    def test_disable_autocompaction_nodetool(self):
        """
        Make sure we can enable/disable compaction using nodetool
        """
        node = self.prepate_testbed()
        session = self.patient_cql_connection(node)

        self.disable_autocompaction(node=node, ks=self.primary_ks, table=self.primary_table)
        self.fill_table_with_data(node=node, ks=self.primary_ks, table=self.primary_table, keys=1000)
        self.fill_table_with_data(node=node, ks=self.secondary_ks, table=self.secondary_table, keys=1000)
        self.assert_table_did_not_compact(session, self.primary_table)
        timestamp = self.assert_table_compacted(session, self.secondary_table)

        self.enable_autocompaction(node=node, ks=self.primary_ks, table=self.primary_table)
        self.fill_table_with_data(node=node, ks=self.primary_ks, table=self.primary_table, keys=1000)
        self.fill_table_with_data(node=node, ks=self.secondary_ks, table=self.secondary_table, keys=1000)
        self.assert_table_compacted(session, self.primary_table, since_timestamp=timestamp)
        self.assert_table_compacted(session, self.secondary_table, since_timestamp=timestamp)

    def test_disable_autocompaction_schema(self):
        """
        Make sure we can disable compaction via the schema compaction parameter 'enabled' = false
        """
        node = self.prepate_testbed(with_compaction=f'{{\'class\':\'{self.strategy}\', '
                                    f'\'enabled\':\'false\'}}')
        session = self.patient_cql_connection(node)
        self.disable_autocompaction(node=node, ks=self.primary_ks, table=self.primary_table)
        self.fill_table_with_data(node=node, ks=self.primary_ks, table=self.primary_table, keys=1000)
        self.fill_table_with_data(node=node, ks=self.secondary_ks, table=self.secondary_table, keys=1000)
        self.assert_table_did_not_compact(session, self.primary_table)
        timestamp = self.assert_table_compacted(session, self.secondary_table)
        # should still be disabled after restart:
        node.stop()
        node.start(wait_for_binary_proto=True)
        session = self.patient_cql_connection(node)
        session.execute('use ks')
        self.assert_table_did_not_compact(session, self.primary_table)
        timestamp = self.assert_table_compacted(session, self.secondary_table)
        # TODO: in Scylla it doesn't work, so i shall run here alter table and update with the `enabled: true`
        # in Scylla 'nodetool enableautocompaction' doesn't work if compaction disabled in schema,
        # so here alter table and update with the `enabled: true`
        session.execute(f"ALTER TABLE {self.primary_table} "
                        f"with compaction = {{'class': '{self.strategy}', 'enabled': 'true'}}")
        self.disable_autocompaction(node=node, ks=self.primary_ks, table=self.primary_table)

        self.enable_autocompaction(node=node, ks=self.primary_ks, table=self.primary_table)
        self.fill_table_with_data(node=node, ks=self.primary_ks, table=self.primary_table, keys=1000)
        self.fill_table_with_data(node=node, ks=self.secondary_ks, table=self.secondary_table, keys=1000)
        self.assert_table_compacted(session, self.primary_table, since_timestamp=timestamp)
        self.assert_table_compacted(session, self.secondary_table, since_timestamp=timestamp)

    def test_disable_autocompaction_alter(self):
        """
        Make sure we can enable compaction using an alter-statement
        """
        node = self.prepate_testbed()
        session = self.patient_cql_connection(node)
        session.execute('use ks')
        session.execute('ALTER TABLE to_disable '
                        f'WITH compaction = {{\'class\':\'{self.strategy}\', \'enabled\':\'false\'}}')
        # the API used on is_autocompaction_enabled doesn't return correct values if they are set in the table schema
        self.fill_table_with_data(node=node, ks=self.primary_ks, table=self.primary_table, keys=1000)
        self.fill_table_with_data(node=node, ks=self.secondary_ks, table=self.secondary_table, keys=1000)
        self.assert_table_did_not_compact(session, self.primary_table)
        timestamp = self.assert_table_compacted(session, self.secondary_table)
        session.execute(f'ALTER TABLE {self.primary_table} '
                        f'WITH compaction = {{\'class\':\'{self.strategy}\', \'enabled\':\'true\'}}')
        # the API used on is_autocompaction_enabled doesn't return correct values if they are set in the table schema
        self.fill_table_with_data(node=node, ks=self.primary_ks, table=self.primary_table, keys=1000)
        self.fill_table_with_data(node=node, ks=self.secondary_ks, table=self.secondary_table, keys=1000)
        self.assert_table_compacted(session, self.primary_table, since_timestamp=timestamp)
        self.assert_table_compacted(session, self.secondary_table, since_timestamp=timestamp)

    def test_disable_autocompaction_alter_and_nodetool(self):
        """
        Make sure compaction stays disabled after an alter statement where we have disabled using nodetool first
        """
        node = self.prepate_testbed()
        session = self.patient_cql_connection(node)
        session.execute('use ks')

        self.disable_autocompaction(node=node, ks=self.primary_ks, table=self.primary_table)
        self.fill_table_with_data(node=node, ks=self.primary_ks, table=self.primary_table, keys=1000)
        self.fill_table_with_data(node=node, ks=self.secondary_ks, table=self.secondary_table, keys=1000)
        self.assert_table_did_not_compact(session, self.primary_table)
        timestamp = self.assert_table_compacted(session, self.secondary_table)

        session.execute(f'ALTER TABLE {self.primary_table} '
                        f'WITH compaction = {{\'class\':\'{self.strategy}\', \'tombstone_threshold\':0.9}}')
        session.execute(f'insert into {self.primary_table} (key, c1, c2) values (\'99\', \'hello\', \'hello\')')
        self.fill_table_with_data(node=node, ks=self.primary_ks, table=self.primary_table, keys=1000)
        self.fill_table_with_data(node=node, ks=self.secondary_ks, table=self.secondary_table, keys=1000)
        self.assert_table_did_not_compact(session, self.primary_table, since_timestamp=timestamp)
        timestamp = self.assert_table_compacted(session, self.secondary_table, since_timestamp=timestamp)

        self.enable_autocompaction(node=node, ks=self.primary_ks, table=self.primary_table)
        self.fill_table_with_data(node=node, ks=self.primary_ks, table=self.primary_table, keys=1000)
        self.fill_table_with_data(node=node, ks=self.secondary_ks, table=self.secondary_table, keys=1000)
        self.assert_table_compacted(session, self.primary_table, since_timestamp=timestamp)
        self.assert_table_compacted(session, self.secondary_table, since_timestamp=timestamp)

    def test_disable_autocompaction_without_params(self):
        """
        Make sure compaction is disabled even if no ks and table name params are passed to the nodetool command
        """
        node = self.prepate_testbed()
        session = self.patient_cql_connection(node)
        node.nodetool('disableautocompaction')
        disable_mark = node.mark_log()
        assert not is_autocompaction_enabled(node, self.primary_ks, self.primary_table), \
            'Expected to have autocompaction disabled but got it is enabled'
        assert not is_autocompaction_enabled(node, self.secondary_ks, self.secondary_table), \
            'All keyspaces and tables are expected to be affected by disableautocompaction'
        self.fill_table_with_data(node=node, ks=self.primary_ks, table=self.primary_table, keys=1000)
        self.fill_table_with_data(node=node, ks=self.secondary_ks, table=self.secondary_table, keys=1000)
        self.assert_table_did_not_compact(session, self.primary_table)
        timestamp = self.assert_table_did_not_compact(session, self.secondary_table)

        node.nodetool('enableautocompaction')
        assert is_autocompaction_enabled(node, self.primary_ks, self.primary_table), \
            'Expected to have autocompaction enabled but got it is disabled'
        assert is_autocompaction_enabled(node, self.secondary_ks, self.secondary_table), \
            'All keyspaces and tables are expected to be affected by enableautocompaction'
        self.fill_table_with_data(node=node, ks=self.primary_ks, table=self.primary_table, keys=1000)
        self.fill_table_with_data(node=node, ks=self.secondary_ks, table=self.secondary_table, keys=1000)
        self.assert_table_compacted(session, self.primary_table, since_timestamp=timestamp)
        self.assert_table_compacted(session, self.secondary_table, since_timestamp=timestamp)

    def test_disable_autocompaction_twice(self):
        """
        Make sure disabling compaction command executed twice in a row doesn't fail
        """
        node = self.prepate_testbed()
        session = self.patient_cql_connection(node)
        self.disable_autocompaction(node=node, ks=self.primary_ks, table=self.primary_table)
        self.fill_table_with_data(node=node, ks=self.primary_ks, table=self.primary_table, keys=1000)
        self.fill_table_with_data(node=node, ks=self.secondary_ks, table=self.secondary_table, keys=1000)
        self.assert_table_did_not_compact(session, self.primary_table)
        timestamp = self.assert_table_compacted(session, self.secondary_table)
        self.disable_autocompaction(node=node, ks=self.primary_ks, table=self.primary_table)
        self.disable_autocompaction(node=node, ks=self.primary_ks, table=self.primary_table)
        self.fill_table_with_data(node=node, ks=self.primary_ks, table=self.primary_table, keys=1000)
        self.fill_table_with_data(node=node, ks=self.secondary_ks, table=self.secondary_table, keys=1000)
        self.assert_table_did_not_compact(session, self.primary_table, since_timestamp=timestamp)
        self.assert_table_compacted(session, self.secondary_table, since_timestamp=timestamp)

    @pytest.mark.parametrize(argnames=["compaction_type"],
                             argvalues=[
                                 ["CLEANUP"],
                                 ["VALIDATE"],
                                 ["SCRUB"],
    ])
    def test_disable_autocompaction_doesnt_block_user_initiated_compactions(self, compaction_type: str):
        """
        Test that disabling autocompaction does not affect the
        user's ability to trigger maintenance compactions such
        as VALIDATE, CLEANUP etc.

        Test steps:
        1. Disable autocompaction on the test table.
        2. Fill the test table with some data.
        3. Wait for remaining compactions to stop.
        4. Trigger a maintenance compaction using the Rest API.
        5. Assert that the compaction ran and was completed.
        """
        node = self.prepate_testbed()
        storage_service_client = StorageServiceClient(node)

        self.disable_autocompaction(node=node, ks=self.primary_ks, table=self.primary_table)
        self.fill_table_with_data(node=node, ks=self.primary_ks, table=self.primary_table, keys=1000)

        compactions = {
            "VALIDATE": {
                "func": storage_service_client.scrub_ks_cf,
                "kwargs": {
                    "keyspace": self.primary_ks,
                    "cf": self.primary_table,
                    "scrub_mode": compaction_type
                },
                "log_expression": "Finished scrubbing in validate mode"
            },
            "SCRUB": {
                "func": storage_service_client.scrub_ks_cf,
                "kwargs": {
                    "keyspace": self.primary_ks,
                    "cf": self.primary_table
                },
                "log_expression": "Finished scrubbing in abort mode"
            },
            "CLEANUP": {
                "func": storage_service_client.cleanup_ks_cf,
                "kwargs": {
                    "keyspace": self.primary_ks,
                    "cf": self.primary_table,
                },
                "log_expression": r"Cleaned \d* sstables to"
            }
        }

        func = compactions[compaction_type]["func"]
        func_kwargs = compactions[compaction_type]["kwargs"]
        func(**func_kwargs)

        assert node.watch_log_for(compactions[compaction_type]["log_expression"], timeout=180)

    def test_disable_autocompaction_doesnt_block_user_initiated_upgrade_compaction(self):
        """
        Test that disabling autocompaction does not affect the
        user's ability to trigger upgrade compaction.

        Test steps:
        1. Create a cluster using the old md sstable format.
        2. Fill the table with some data.
        3. Disable autocompaction on the test table.
        4. Stop the cluster.
        5. Change the sstable format to mc and restart the cluster.
        6. Trigger the upgrade compaction using the Rest API.
        7. Assert that the compaction ran and was completed.
        """
        log_expression = f"Upgrade {self.primary_ks}.{self.primary_table}"
        node = self.prepate_testbed(configuration_options={"enable_sstables_mc_format": True,
                                                           "enable_sstables_md_format": False})
        storage_service_client = StorageServiceClient(node)

        self.fill_table_with_data(node=node, ks=self.primary_ks, table=self.primary_table, keys=1000)
        self.disable_autocompaction(node=node, ks=self.primary_ks, table=self.primary_table)
        self.cluster.stop()
        self.cluster.set_configuration_options(values={"enable_sstables_mc_format": False,
                                                       "enable_sstables_md_format": True})
        self.cluster.start()
        mark = node.mark_log()

        storage_service_client.upgrade_sstables(keyspace=self.primary_ks, cf=self.primary_table)

        assert node.watch_log_for(exprs=log_expression, from_mark=mark)

    def test_disable_autocompaction_doesnt_block_user_initiated_reshape_compaction(self):
        """
        Test that disabling autocompaction does not affect the
        user's ability to trigger reshape compaction.

        Test steps:
        1. Initialize cluster with 1 node.
        2. Populate using STCS as the compaction mode.
        3. Disable autocompaction on the test table.
        4. Populate the cluster with data using c-s and flush to sstables.
        5. Copy the sstables to the upload directory.
        6. Truncate the test table.
        7. Alter the compaction strategy for the table to TWCS.
        8. Run nodetool refresh to on the test table.
        9. Assert that the Reshape compaction ran.
        """
        if self.strategy != "LeveledCompactionStrategy":
            pytest.skip("Skipping redundant runs as this test does not depend on preset compaction strategy.")

        TWCS = {'class': 'TimeWindowCompactionStrategy', 'compaction_window_size': 1,
                'compaction_window_unit': 'MINUTES', 'max_threshold': 1, 'min_threshold': 1}
        STCS = {'class': 'SizeTieredCompactionStrategy', 'bucket_high': 1.5, 'bucket_low': 0.5,
                'min_sstable_size': 1, 'max_threshold': 1, 'min_threshold': 1}
        ks = "keyspace1"
        cf = "standard1"
        node, session = self._prepare_reshape_testbed()
        session.execute(f"ALTER TABLE {ks}.{cf} WITH compaction={STCS}")
        self.disable_autocompaction(node=node, ks=ks, table=cf, verify=False)
        fill_data_by_cs(node, n_range=[], duration_range=[70],
                        other_opt=['-rate', 'threads=1', '-col', 'size=FIXED(1024)'])
        self._copy_files_for_population_after_restart(ks=ks, cf=cf)
        mark = node.mark_log()
        session.execute(f"TRUNCATE {ks}.{cf}")
        session.execute(f"ALTER TABLE {ks}.{cf} WITH compaction={TWCS}")
        node.nodetool(f"refresh -- {ks} {cf}")

        assert node.watch_log_for(exprs="Reshaped", from_mark=mark)

    primary_ks = 'ks'
    primary_table = 'to_disable'
    secondary_ks = 'ks2'
    secondary_table = 'std1'
    empty_error_message = "Query didn't return any rows"

    def _prepare_reshape_testbed(self):
        cluster = self.cluster
        cluster.populate(1).start(wait_for_binary_proto=True)
        [node] = cluster.nodelist()
        session = self.patient_cql_connection(node)
        session.execute("DROP KEYSPACE IF EXISTS keyspace1")
        node.stress(['write', 'n=0', 'no-warmup', '-schema', 'replication(factor=1)', '-rate', 'threads=1'])

        return node, session

    def _copy_files_for_population_after_restart(self, ks: str, cf: str) -> None:
        [node] = self.cluster.nodelist()
        cf_dir = get_node_cf_dir(node, f'{ks}', f'{cf}', latest=True)
        copy_files_to(cf_dir, os.path.join(cf_dir, './upload/'), files_only=True)

    @staticmethod
    def wait_for_compactions(node, timeout: int):
        pattern = re.compile("pending tasks: 0")
        start = time.time()
        while True and time.time() < start + timeout:
            output, err = node.nodetool("compactionstats", capture_output=True)
            if pattern.search(output):
                break

    def disable_autocompaction(self, node, ks, table, verify=True):
        node.nodetool(f'disableautocompaction {ks} {table}')
        mark = node.mark_log()
        if verify:
            assert not is_autocompaction_enabled(node, ks, table), \
                'Expected to have autocompaction disabled but got it is enabled'
            assert is_autocompaction_enabled(node, self.secondary_ks, self.secondary_table), \
                'Other keyspaces and tables are not supposed to be affected by disableautocompaction'
        return mark

    def enable_autocompaction(self, node, ks, table, verify=True):
        node.nodetool(f'enableautocompaction -- {ks} {table}')
        mark = node.mark_log()
        if verify:
            assert is_autocompaction_enabled(node, ks, table), \
                'Expected to have autocompaction enabled but got it is disabled'
            assert is_autocompaction_enabled(node, self.secondary_ks, self.secondary_table), \
                'Expected to have autocompaction enabled but got it is disabled'
        return mark

    def create_ks_and_table(self, node, ks, table, with_compaction=None):
        session = self.patient_cql_connection(node)
        create_ks(session, ks, 1)
        if not with_compaction:
            with_compaction = f'{{\'class\':\'{self.strategy}\'}}'
        create_c1c2_table(session=session, cf=table, compaction=with_compaction)

    def fill_table_with_data(self, node, ks, table, keys, flush=True):
        session = self.patient_cql_connection(node)
        session.execute(f'use {ks}')
        for chunk in chunks_list(list(range(keys)), 100):
            insert_c1c2(session=session, keys=chunk, consistency=0, ks=ks, cf=table)
            if flush:
                node.flush()

    def prepate_testbed(self, with_compaction=None, configuration_options: Dict = None):
        cluster = self.cluster
        cluster.populate(1)
        if configuration_options:
            self.cluster.set_configuration_options(values=configuration_options)
        cluster.start(wait_for_binary_proto=True)
        [node] = cluster.nodelist()
        self.create_ks_and_table(node=node, ks=self.primary_ks, table=self.primary_table,
                                 with_compaction=with_compaction)
        self.create_ks_and_table(node=node, ks=self.secondary_ks, table=self.secondary_table)
        return node

    def skip_if_no_major_compaction(self):
        if parse_version(self.cluster.version()) < parse_version('2.2') and self.strategy == 'LeveledCompactionStrategy':
            pytest.skip('major compaction not implemented for LCS in this version of Cassandra')

    def last_compaction_timestamp(self, session, cf_name, since_timestamp=None):
        timestamp_filter = f"AND compacted_at > {since_timestamp} " if since_timestamp else ""
        query = f"SELECT MAX(compacted_at) FROM system.compaction_history WHERE columnfamily_name = '{cf_name}' {timestamp_filter}ALLOW FILTERING"
        result = list(session.execute(query))[0][0]
        # Python converts datetime to timestamp by returning float number of seconds
        # The value in the table is an int representing milliseconds, so multiply
        # by 1000 and convert from float to int to match types.
        timestamp = int(1000 * datetime.datetime.timestamp(result)) if result else None
        if not timestamp:
            raise RuntimeError(self.empty_error_message)
        return timestamp

    def assert_table_did_not_compact(self, session, cf_name, timeout=2, since_timestamp=None):
        with pytest.raises(RuntimeError, match=self.empty_error_message) as ex_info:
            return retry_till_success(self.last_compaction_timestamp, session, cf_name, timeout=timeout, since_timestamp=since_timestamp)

    def assert_table_compacted(self, session, cf_name, timeout=2, since_timestamp=None):
        return retry_till_success(self.last_compaction_timestamp, session, cf_name, timeout=timeout, since_timestamp=since_timestamp)


def get_random_word(word_len):
    word = ''
    for i in range(word_len):
        word += random.choice('ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789')
    return word


def block_on_compaction_log(node, ks=None, table=None):
    """
    @param node the node on which to trigger and block on compaction
    @param ks the keyspace to compact
    @param table the table to compact

    Helper method for testing compaction. This triggers compactions by
    calling flush and compact on node. In situations where major
    compaction won't apply to a table, such as in pre-2.2 LCS tables, the
    flush will trigger minor compactions.

    This method uses log-watching to block until compaction is completed.

    By default, this method uses the keyspace and table names generated by
    cassandra-stress. These will not be used if ks and table names parameters
    are passed in.

    Calling flush before calling this method may cause it to hang; if
    compaction completes before the method starts, it may not occur again
    during this method.
    """
    if node.is_scylla() or node.get_cassandra_version() < '2.2':
        log_file = 'system.log'
    else:
        log_file = 'debug.log'
    mark = node.mark_log(filename=log_file)
    node.flush()

    # on newer C* versions, default stress names are titlecased
    stress_keyspace, stress_table = ('keyspace1', 'standard1')

    ks = ks or stress_keyspace
    table = table or stress_table

    logger.debug(f"Running major compaction on {node.name} {ks}.{table}")
    node.nodetool('compact {ks} {table}'.format(ks=ks, table=table))

    return node.watch_log_for('Compacted', from_mark=mark, filename=log_file)


def stress_write(node, keycount=100000):
    node.stress(['write', f'n={keycount}'])
