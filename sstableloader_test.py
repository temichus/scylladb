import os
import shutil
import subprocess
from typing import Optional

from cassandra.cluster import Session

from ccmlib.scylla_node import ScyllaNode
from migration_test import MigrationTestBase
from dtest import Tester, debug
from tools import safe_mkdtemp, require
from scylla_tools import insert_c1c2, get_cf_dir, copy_files_to

# from nose import tools
from nose.plugins.attrib import attr
from assertions import assert_none, assert_one, assert_all


@attr('dtest-full', 'single_node')
class TestSSTableLoader(MigrationTestBase):

    __test__ = False

    def __init__(self, *args, **kwargs):
        kwargs['cluster_options'] = {'start_rpc': 'true'}
        Tester.__init__(self, *args, **kwargs)

    def load_migrated_tables(self, node, migrated_files_dir, extra_args=None,
                             partitioner='org.apache.cassandra.dht.Murmur3Partitioner'):
        cassandra_sstable_dir = self.get_cassandra_sstable_dir(self.version, migrated_files_dir)
        debug("cassandra sstable dir is {}".format(cassandra_sstable_dir))

        ks = "ks"
        cf = "cf"
        tmpdir = safe_mkdtemp()
        dir = os.path.join(tmpdir, ks, cf)

        os.mkdir(os.path.join(tmpdir, ks))
        os.mkdir(dir)

        debug("Copying sstables created by Cassandra...")
        self.copy_files_to(cassandra_sstable_dir, dir)

        ip = node.address()
        args = [node.get_tool('sstableloader'), '-v', '-pt', partitioner, '-d', ip, dir]
        if self.prepared:
            args.append(self.prepared)
        if extra_args:
            args += extra_args

        p = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = p.communicate()
        exit_status = p.wait()
        if stderr:
            debug("sstableloader command: %s" % args)
            debug("=== sstableloader stderr ===")
            debug(stderr)
            debug("====")

        shutil.rmtree(tmpdir)

        if exit_status != 0:
            raise Exception("sstableloader command '%s' failed; exit status: %d'; stdout: %s; stderr: %s" %
                            (" ".join(args), exit_status, stdout, stderr))

    def load_migrated_tables_expect_fail(self, node, migrated_files_dir, message=None, ks='ks', cf='cf'):
        cassandra_sstable_dir = self.get_cassandra_sstable_dir(self.version, migrated_files_dir)
        debug("cassandra sstable dir is {}".format(cassandra_sstable_dir))

        tmpdir = safe_mkdtemp()
        dir = os.path.join(tmpdir, ks, cf)

        os.mkdir(os.path.join(tmpdir, ks))
        os.mkdir(dir)

        debug("Copying sstables created by Cassandra...")
        self.copy_files_to(cassandra_sstable_dir, dir)

        ip = node.address()
        args = [node.get_tool('sstableloader'), '-v', '-d', ip, dir]
        if self.prepared:
            args.append(self.prepared)
        p = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = p.communicate()
        debug('STDOUT: {}'.format(stdout))
        exit_status = p.wait()
        if stderr:
            debug("=== sstableloader stderr ===")
            debug(stderr)
            debug("====")

        shutil.rmtree(tmpdir)

        if exit_status != 0:
            if message:
                assert message in str(stderr), stderr
            else:
                raise Exception("sstableloader command '%s' failed; exit status: %d'; stdout: %s; stderr: %s" %
                                (" ".join(args), exit_status, stdout, stderr))

    def get_wrong_partitioner_error_message(self):
        return "partitioner org.apache.cassandra.dht.RandomPartitioner" + \
               " does not match system partitioner" + \
               " org.apache.cassandra.dht.Murmur3Partitioner"

    @attr('next-gating')
    @attr('dtest-debug')
    def migrate_sstable_with_wrong_partitioner_test(self):
        """
        https://github.com/scylladb/scylla/issues/4331
        Partitioner: org.apache.cassandra.dht.RandomPartitioner
        CREATE KEYSPACE ks
            WITH replication={
                'class':'SimpleStrategy', 'replication_factor':1
            };
        CREATE TABLE ks.cf ( pk INT, ck INT, v INT, PRIMARY KEY(pk, ck))
            WITH compression = { 'sstable_compression' : '' };
        INSERT INTO ks.cf(pk, ck, s, val) VALUES(1, 10, 100);
        INSERT INTO ks.cf(pk, ck, s, val) VALUES(2, 20, 200);
        INSERT INTO ks.cf(pk, ck, s, val) VALUES(3, 30, 300);
        flush
        """
        cluster = self.cluster
        self.populate_cluster(cluster)
        node1 = self.cluster.nodelist()[0]
        node1.set_configuration_options()
        self.start_cluster(cluster)

        query = "CREATE TABLE ks.cf (pk INT, ck INT, v INT, PRIMARY KEY(pk, ck))" + \
                " WITH compression = { 'sstable_compression' : '' }"
        self.create_ks_and_cf(node1, None, None, False, query=query)
        self.load_migrated_tables(node1,
                                  'with_wrong_partitioner',
                                  partitioner='org.apache.cassandra.dht.RandomPartitioner')

    @attr('next-gating')
    @attr('dtest-debug')
    def load_migrated_table_with_old_counter_test(self):
        """
        Test migration of old data with counter, using the default (--ignore-dropped-counter-data isn't passed)
        """

        if not self.version == '2_1_x':
            self.skipTest('Test only relevant to old counter')

        cluster = self.cluster
        self.populate_cluster(cluster)
        node1 = self.cluster.nodelist()[0]
        node1.set_configuration_options()
        self.start_cluster(cluster)

        query = "CREATE TABLE ks.cf (pk int PRIMARY KEY, cnt COUNTER);"
        self.create_ks_and_cf(node1, None, None, False, query=query)

        # cause of issue scylladb/scylla-tools-java#84, the exit code is 0
        expected_message = "Local counter shard found. Data loss may occur"
        self.load_migrated_tables_expect_fail(node1, 'with_old_format_counter', message=expected_message)

        # Expect success with sstableloader --ignore-dropped-counter-data
        self.load_migrated_tables(node1, 'with_old_format_counter', extra_args=['--ignore-dropped-counter-data'])

    @attr('next-gating')
    @attr('dtest-debug')
    def load_migrated_table_with_counter_test(self):
        """
        Test migration of old data with counter, using the default (--ignore-dropped-counter-data isn't passed)
        """

        if self.version == '2_1_x':
            self.skipTest('Test only relevant to new counter')

        cluster = self.cluster
        self.populate_cluster(cluster)
        node1 = self.cluster.nodelist()[0]
        node1.set_configuration_options()
        self.start_cluster(cluster)

        query = "CREATE TABLE ks.cf (pk int PRIMARY KEY, cnt COUNTER);"
        self.create_ks_and_cf(node1, None, None, False, query=query)

        self.load_migrated_tables(node1, 'with_counter')


versions = ['2_1_x', '2_2_x', '3_0_x', '3_0_mc', '3_0_md']
for version in versions:
    for prepared in ['-nx', '']:
        cls_name = ('TestMigration_with_{0}{1}'.format(version, '' if prepared else '_prepared'))
        vars()[cls_name] = type(cls_name, (TestSSTableLoader,), {
            'version': version, 'prepared': prepared, '__test__': True})


@attr('dtest-full', 'single_node')
class AdditionalTestSSTableLoader(Tester):
    __test__ = True

    def prepare_cluster(self, nodes: int = 1) -> (Session, ScyllaNode):
        cluster = self.cluster
        cluster.populate(nodes).start(wait_for_binary_proto=True)
        node1 = cluster.nodelist()[0]

        # Prepare test table and test data
        session = self.patient_cql_connection(node1)
        return session, node1

    def create_cf_sstable_copy(self, ks: str, cf: str) -> (str, str):
        orig_cf_dir = get_cf_dir(os.path.join(self.test_path, 'test', 'node1', 'data', ks), cf_name=cf)
        assert orig_cf_dir is not None, 'table folder is not found'

        tmpdir = safe_mkdtemp()
        copy_cf_dir = os.path.join(tmpdir, ks, cf)
        os.mkdir(os.path.join(tmpdir, ks))
        os.mkdir(copy_cf_dir)

        debug("Copying sstables ")
        copy_files_to(orig_cf_dir, copy_cf_dir, files_only=True)
        return copy_cf_dir, tmpdir

    def prepare_cf(self, session: Session, cf_name: str, data: list):
        session.execute(f"CREATE COLUMNFAMILY {cf_name} (key int, c1 text, c2 text, c3 text, "
                        f"PRIMARY KEY(key, c1))")

        for insert in ["insert into cf (key, c1, c2, c3) values ({}, '{}', '{}', '{}')".format(*one_set)
                       for one_set in data]:
            session.execute(insert)

        # for i in range(rows):
        #     session.execute(f"insert into cf (key, c1, c2, c3) values ({i}, 'a', 'b', 'c')")

    @staticmethod
    def get_rows_data(rows: int) -> list:
        return [[i, f'a{i}', f'b{i}', f'c{i}'] for i in range(rows)]

    @staticmethod
    def remove_column_from_data(data: list, element_to_remove_index: list) -> list:
        """
        element_to_remove_index: index of the removed column in the data
        Example:
                     key c1    c2    c3
                     ====================
            data = [[0, 'a0', 'b0', 'c0'],
                    [1, 'a1', 'b1', 'c1']
                  ]
            Column "c2" was dropped. So to remove its values, we need to remove element with "c2" index in the list = 2
        """
        for i, one in enumerate(data):
            for ind in element_to_remove_index:
                del data[i][ind]

        return data

    def run_sstableloader(self, node: ScyllaNode, copy_cf_dir: str, ignore_columns: str = '', err: str = None,
                          completed: Optional[None or bool] = None):
        debug('Run sstableloader on node1')
        ip = node.address()

        cmd = [node.get_tool('sstableloader'), '-d', ip]
        if ignore_columns:
            cmd.extend(['--ignore-missing-columns', ignore_columns])
        cmd.extend([copy_cf_dir, '-v'])

        debug(cmd)
        result = subprocess.run(cmd, stderr=subprocess.PIPE, stdout=subprocess.PIPE, check=True)

        if completed is not None:
            if completed:
                assert '100% done.' in str(result.stdout)
            else:
                assert ' 0% done.' in str(result.stdout)

        if err is not None:
            assert err in str(result.stderr)

        if not err and result.stderr:
            raise Exception(
                f"sstableloader command failed, exit status: {result.returncode},"
                f"\n\nSTDERR: {result.stderr}\n,\nSTDOUT: {result.stdout}")

    @require('scylla-tools-java/#216')
    def ignore_missing_columns_by_drop_column_from_snapshot_test(self):
        """
        Verify `--ignore-missing-columns` option of sstableloader works
        Drop one column and load the data from snapshot with `--ignore-missing-columns` parameter

        - create table
        - insert 10 rows and flush
        - take snapshot
        - drop one column
        - load from snapshot
        - validate that all 10 rows in the table

        Related issues: https://github.com/scylladb/scylla/issues/6990 and
                        https://github.com/scylladb/scylla-tools-java/issues/216
        """
        ks = 'ks'
        cf = 'cf'
        rows = 10
        session, node1 = self.prepare_cluster()
        self.create_ks(session, ks, 1)
        data = self.get_rows_data(rows=rows)
        self.prepare_cf(session=session, cf_name=cf, data=data)
        node1.flush()
        assert_one(session, f'SELECT count(*) FROM {ks}.{cf}', expected=[rows])

        # Create snapshot
        snapshot_name = 'test_snapshot'
        node1.nodetool(f"snapshot -t {snapshot_name} -cf {cf} -- {ks}")

        snapshot_dir = None
        for root, dirs, files in os.walk(os.path.join(self.test_path, 'test', 'node1', 'data', ks)):
            for name in dirs:
                if name == snapshot_name:
                    snapshot_dir = os.path.join(root, name)
        assert snapshot_dir is not None, 'snapshot_dir is not found'

        # Clean data and drop one column
        session.execute(f"TRUNCATE {ks}.{cf}")
        session.execute(f"ALTER TABLE {ks}.{cf} DROP c2")

        # Load snapshot by sstableloader
        self.run_sstableloader(node=node1, copy_cf_dir=snapshot_dir, ignore_columns='c2')

        # Remove data of dropped column
        data = self.remove_column_from_data(data=data, element_to_remove_index=[2])
        assert_all(session, f'SELECT key, c1, c3 FROM {ks}.{cf}', expected=data, ignore_order=True)

    @require('scylla-tools-java/#216')
    def ignore_missing_columns_by_drop_table_from_snapshot_test(self):
        """
        Verify `--ignore-missing-columns` option of sstableloader works
        Drop one column and load the data from snapshot with `--ignore-missing-columns` parameter

        - create table
        - insert 10 rows and flush
        - take snapshot
        - drop one column
        - load from snapshot
        - validate that all 10 rows in the table

        Related issues: https://github.com/scylladb/scylla/issues/6990 and
                        https://github.com/scylladb/scylla-tools-java/issues/216
        """
        ks = 'ks'
        cf = 'cf'
        rows = 10
        session, node1 = self.prepare_cluster()
        self.create_ks(session, ks, 1)
        data = self.get_rows_data(rows=rows)
        self.prepare_cf(session=session, cf_name=cf, data=data)
        node1.flush()
        assert_one(session, f'SELECT count(*) FROM {ks}.{cf}', expected=[rows])

        # Create snapshot
        snapshot_name = 'test_snapshot'
        node1.nodetool(f"snapshot -t {snapshot_name} -cf {cf} -- {ks}")

        snapshot_dir = None
        for root, dirs, files in os.walk(os.path.join(self.test_path, 'test', 'node1', 'data', ks)):
            for name in dirs:
                if name == snapshot_name:
                    snapshot_dir = os.path.join(root, name)
        assert snapshot_dir is not None, 'snapshot_dir is not found'

        # Clean data and drop one column
        session.execute(f"DROP TABLE {ks}.{cf}")
        session.execute(f"CREATE COLUMNFAMILY {cf} (key int, c1 text, c3 text, PRIMARY KEY(key, c1))")

        # Load snapshot by sstableloader
        self.run_sstableloader(node=node1, copy_cf_dir=snapshot_dir, ignore_columns='c2')

        # Remove data of dropped column
        data = self.remove_column_from_data(data=data, element_to_remove_index=[2])
        assert_all(session, f'SELECT key, c1, c3 FROM {ks}.{cf}', expected=data, ignore_order=True)

    def ignore_missing_one_column_by_drop_table_from_backup_test(self):
        """
        Verify `--ignore-missing-columns` option of sstableloader works
        Recreate table with one column less and load the data from backup folder with `--ignore-missing-columns`
        parameter

        - create table
        - insert 10 rows and flush
        - copy sstables to tmp backup folder
        - drop table
        - create table with one column less
        - load from backup folder
        - validate that all 10 rows in the table

        Related issue: https://github.com/scylladb/scylla/issues/6990
        """
        ks = 'ks'
        cf = 'cf'
        rows = 10
        session, node1 = self.prepare_cluster()
        self.create_ks(session=session, name=ks, rf=1)
        data = self.get_rows_data(rows=rows)
        self.prepare_cf(session=session, cf_name=cf, data=data)
        node1.flush()
        assert_one(session, f'SELECT count(*) FROM {ks}.{cf}', expected=[rows])

        copy_cf_dir, tmpdir = self.create_cf_sstable_copy(ks, cf)

        # Drop table and recreate with miss 1 column
        session.execute(f"DROP TABLE {cf}")
        session.execute(f"CREATE COLUMNFAMILY {cf} (key int, c1 text, c3 text, PRIMARY KEY(key, c1))")

        # Load snapshot by sstableloader
        self.run_sstableloader(node=node1, copy_cf_dir=copy_cf_dir, ignore_columns='c2')

        # Drop temp directory
        shutil.rmtree(tmpdir)

        # Remove data of dropped column
        data = self.remove_column_from_data(data=data, element_to_remove_index=[2])
        assert_all(session, f'SELECT key, c1, c3 FROM {ks}.{cf}', expected=data, ignore_order=True)

    def ignore_missing_one_column_by_drop_column_from_backup_test(self):
        """
        Verify `--ignore-missing-columns` option of sstableloader works
        Drop one column and load the data from backup folder with `--ignore-missing-columns` parameter

        - create table
        - insert 10 rows and flush
        - copy sstables to tmp backup folder
        - drop one column
        - load from backup folder
        - validate that all 10 rows in the table

        Related issue: https://github.com/scylladb/scylla/issues/6990
        """
        ks = 'ks'
        cf = 'cf'
        rows = 10
        session, node1 = self.prepare_cluster()
        self.create_ks(session, ks, 1)
        data = self.get_rows_data(rows=rows)
        self.prepare_cf(session=session, cf_name=cf, data=data)
        node1.flush()
        assert_one(session, f'SELECT count(*) FROM {ks}.{cf}', expected=[rows])

        copy_cf_dir, tmpdir = self.create_cf_sstable_copy(ks, cf)

        # Truncate table and drop 1 column
        session.execute(f"TRUNCATE TABLE {cf}")
        session.execute(f"ALTER TABLE {cf} DROP c2")

        # Load snapshot by sstableloader
        self.run_sstableloader(node=node1, copy_cf_dir=copy_cf_dir, ignore_columns='c2')

        # Drop temp directory
        shutil.rmtree(tmpdir, ignore_errors=True)

        # Remove data of dropped column
        data = self.remove_column_from_data(data=data, element_to_remove_index=[2])
        assert_all(session, f'SELECT key, c1, c3 FROM {ks}.{cf}', expected=data, ignore_order=True)

    def ignore_missing_two_column_by_drop_column_from_backup_test(self):
        """
        Verify `--ignore-missing-columns` option of sstableloader works
        Drop two columns and load the data from backup folder with `--ignore-missing-columns` parameter

        - create table
        - insert 10 rows and flush
        - copy sstables to tmp backup folder
        - drop two columns
        - load from backup folder
        - validate that all 10 rows in the table

        Related issue: https://github.com/scylladb/scylla/issues/6990
        """
        ks = 'ks'
        cf = 'cf'
        rows = 10
        session, node1 = self.prepare_cluster()
        self.create_ks(session, ks, 1)
        data = self.get_rows_data(rows=rows)
        self.prepare_cf(session=session, cf_name=cf, data=data)
        node1.flush()
        assert_one(session, f'SELECT count(*) FROM {ks}.{cf}', expected=[rows])

        copy_cf_dir, tmpdir = self.create_cf_sstable_copy(ks, cf)

        # Truncate table and drop 2 column
        session.execute(f"TRUNCATE TABLE {cf}")
        session.execute(f"ALTER TABLE {cf} DROP (c2, c3)")

        # Load snapshot by sstableloader
        self.run_sstableloader(node=node1, copy_cf_dir=copy_cf_dir, ignore_columns='c2,c3')

        # Drop temp directory
        shutil.rmtree(tmpdir, ignore_errors=True)

        # Remove data of dropped columns
        data = self.remove_column_from_data(data=data, element_to_remove_index=[2, -1])
        assert_all(session, f'SELECT key, c1 FROM {ks}.{cf}', expected=data, ignore_order=True)

    @require('scylla-tools-java/214')
    def ignore_missing_two_column_by_drop_table_from_backup_test(self):
        """
        Verify `--ignore-missing-columns` option of sstableloader works
        Recreate table with two columns less and load the data from backup folder with `--ignore-missing-columns`
        parameter

        - create table
        - insert 10 rows and flush
        - copy sstables to tmp backup folder
        - drop table
        - create table with two columns less
        - load from backup folder
        - validate that all 10 rows in the table

        Related issue: https://github.com/scylladb/scylla/issues/6990
        """
        ks = 'ks'
        cf = 'cf'
        rows = 10
        session, node1 = self.prepare_cluster()
        self.create_ks(session, ks, 1)
        data = self.get_rows_data(rows=rows)
        self.prepare_cf(session=session, cf_name=cf, data=data)
        node1.flush()
        assert_one(session, f'SELECT count(*) FROM {ks}.{cf}', expected=[rows])

        copy_cf_dir, tmpdir = self.create_cf_sstable_copy(ks, cf)

        # Drop table and recreate with miss 2 column
        session.execute(f"DROP TABLE {cf}")
        session.execute(f"CREATE COLUMNFAMILY {cf} (key int, c1 text, PRIMARY KEY(key, c1))")

        # Load snapshot by sstableloader
        self.run_sstableloader(node=node1, copy_cf_dir=copy_cf_dir, ignore_columns='c2,c3')

        # Drop temp directory
        shutil.rmtree(tmpdir, ignore_errors=True)

        # Remove data of dropped columns
        data = self.remove_column_from_data(data=data, element_to_remove_index=[2, -1])
        assert_all(session, f'SELECT key, c1 FROM {ks}.{cf}', expected=data, ignore_order=True)

    def invalid_sstable_test(self):
        """Test with unsupported version and index missing, sstable won't success"""
        self.cluster.populate(1).start(wait_for_binary_proto=True)
        node1 = self.cluster.nodelist()[0]

        # Prepare test table and test data
        session = self.patient_cql_connection(node1)
        self.create_ks(session, 'ks', 1)
        self.create_cf(session, 'cf', columns={'c1': 'text', 'c2': 'text'})

        # Missing index
        # INFO: "Skipping file md-2-big-Data.db - missing index"
        self.run_sstableloader(node=node1, copy_cf_dir='test-sstables/missing_index/ks/cf-test', completed=False)
        assert_none(session, "SELECT * FROM ks.cf")

        # Unknown format / unsupported version
        self.run_sstableloader(node=node1, copy_cf_dir='test-sstables/unknown_format/ks/cf-test',
                               err='Skipping file md---big-Data.db - unsupported SSTable format or version',
                               completed=False)
        assert_none(session, "SELECT * FROM ks.cf")

        # Test with right sstable
        self.run_sstableloader(node=node1, copy_cf_dir='test-sstables/original/ks/cf-test', completed=True)
        assert_all(session, "SELECT * FROM ks.cf", [['k0', 'c1', 'c2']])
