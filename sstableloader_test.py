import subprocess
import shutil
import os


from migration_test import MigrationTestBase
from dtest import Tester, debug
from tools import safe_mkdtemp
# from nose import tools


# @tools.istest
class TestSSTableLoader(MigrationTestBase):

    __test__ = False

    def __init__(self, *args, **kwargs):
        kwargs['cluster_options'] = {'start_rpc': 'true'}
        Tester.__init__(self, *args, **kwargs)

    def load_migrated_tables(self, node, migrated_files_dir, extra_args=None):
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
        args = [node.get_tool('sstableloader'), '-v', '-d', ip, dir]
        if self.prepared:
            args.append(self.prepared)
        if extra_args:
            args += extra_args

        p = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = p.communicate()
        exit_status = p.wait()
        if stderr:
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

    def load_migrated_tables_with_old_counter_test(self):
        """
        Test migration of old data with counter, while using --ignore-dropped-counter-data
        """

        if self.version == '2_1_x':
            self.migrate_sstable_with_old_format_counter_helper()
        else:
            self.skipTest('Test only relevant to old counter')

    def load_migrated_table_with_old_counter_default(self):
        """
        Test migration of old data with counter, using the default (--ignore-dropped-counter-data isn't passed)
        """

        if not self.version == '2_1_x':
            self.skipTest('Test only relevant to old counter')

        self.allow_log_errors = True
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


versions = ['2_1_x', '2_2_x', '3_0_x', '3_0_mc']
for version in versions:
    for prepared in ['-nx', '']:
        cls_name = ('TestMigration_with_{0}{1}'.format(version, '' if prepared else '_prepared'))
        vars()[cls_name] = type(cls_name, (TestSSTableLoader,), {'version': version, 'prepared':prepared, '__test__': True})
