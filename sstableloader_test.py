import subprocess
import shutil
import os


from migration_test import MigrationTestBase
from dtest import Tester, debug
from tools import safe_mkdtemp
from scylla_tools import insert_c1c2
# from nose import tools
from nose.plugins.attrib import attr
from assertions import assert_none, assert_one, assert_all

# @tools.istest


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

    def option_ignore_missing_columns_test(self):
        """
        Verify `--ignore-missing-columns` option of sstableloader works
        Related issue: https://github.com/scylladb/scylla/issues/6990
        """
        cluster = self.cluster
        cluster.populate(1).start(wait_for_binary_proto=True)
        node1 = cluster.nodelist()[0]

        # Prepare test table and test data
        session = self.patient_cql_connection(node1)
        self.create_ks(session, 'ks', 1)
        self.create_cf(session, 'cf', columns={'c1': 'text', 'c2': 'text'})
        insert_c1c2(session, n=10)
        node1.flush()

        # Create snapshot
        snapshot_name = 'test_snapshot'
        node1.nodetool(f"snapshot -t {snapshot_name} -cf cf -- ks")

        snapshot_dir = None
        for root, dirs, files in os.walk(os.path.join(self.test_path, 'test', 'node1', 'data', 'ks')):
            for name in dirs:
                if name == snapshot_name:
                    snapshot_dir = os.path.join(root, name)
        assert snapshot_dir is not None, 'snapshot_dir is not found'
        cf_dir = os.path.join(snapshot_dir, '..')

        # Clean data and drop one column
        session.execute("TRUNCATE ks.cf")
        session.execute("ALTER TABLE ks.cf DROP c2")

        # Load snapshot by sstableloader
        ip = node1.address()
        debug('Run sstableloader on node1')
        cmd = [node1.get_tool('sstableloader'), '-d', ip, '--ignore-missing-columns', 'c2', f'{snapshot_dir}']
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = p.communicate()
        exit_status = p.wait()
        if exit_status != 0:
            raise Exception(
                f"sstableloader command failed, exit status: {exit_status}, stdout: {stdout}, stderr: {stderr}")
        assert_none(session, 'SELECT * FROM ks.cf')

    def invalid_sstable_test(self):
        """Test with unsupported version and index missing, sstable won't success"""
        self.cluster.populate(1).start(wait_for_binary_proto=True)
        node1 = self.cluster.nodelist()[0]

        # Prepare test table and test data
        session = self.patient_cql_connection(node1)
        self.create_ks(session, 'ks', 1)
        self.create_cf(session, 'cf', columns={'c1': 'text', 'c2': 'text'})

        def run_sstableloader(sstable_dir, err, completed=True):
            """Execute sstableloader to load the sstables"""
            ip = node1.address()
            debug(f'Run sstableloader on node1, sstable_dir: {sstable_dir}')
            cmd = [node1.get_tool('sstableloader'), '-d', ip, sstable_dir, '-v']
            p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            stdout, stderr = p.communicate()
            exit_status = p.wait()

            debug(f"sstableloader stderr: {stderr}")
            if completed:
                assert '100% done.' in str(stdout)
            else:
                assert ' 0% done.' in str(stdout)
            if err is not None:
                assert err in str(stderr)
            if exit_status != 0:
                raise Exception("sstableloader command failed, "
                                f"exit status: {exit_status}, stdout: {stdout}, stderr: {stderr}")

        # Missing index
        # INFO: "Skipping file md-2-big-Data.db - missing index"
        run_sstableloader('test-sstables/missing_index/ks/cf-test',
                          None, completed=False)
        assert_none(session, "SELECT * FROM ks.cf")

        # Unknown format / unsupported version
        run_sstableloader('test-sstables/unknown_format/ks/cf-test',
                          'Skipping file md---big-Data.db - unsupported SSTable format or version',
                          completed=False)
        assert_none(session, "SELECT * FROM ks.cf")

        # Test with right sstable
        run_sstableloader('test-sstables/original/ks/cf-test', None, completed=True)
        assert_all(session, "SELECT * FROM ks.cf", [['k0', 'c1', 'c2']])
