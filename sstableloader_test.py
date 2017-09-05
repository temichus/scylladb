import subprocess
import shutil
import os


from migration_test import MigrationTestBase
from dtest import Tester, debug
from tools import safe_mkdtemp
#from nose import tools


#@tools.istest
class TestSSTableLoader(MigrationTestBase):

    __test__ = False

    def __init__(self, *args, **kwargs):
        kwargs['cluster_options'] = {'start_rpc': 'true'}
        Tester.__init__(self, *args, **kwargs)

    def load_migrated_tables(self, node, migrated_files_dir):
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
    def load_migrated_tables_with_old_counter_test(self):
        if self.__class__.__name__ == 'TestMigration_with_2_1_x':
            self.migrate_sstable_with_old_format_counter_helper()


versions = ['2_1_x', '2_2_x', '3_0_x']
for version in versions:
    cls_name = ('TestMigration_with_' + version)
    vars()[cls_name] = type(cls_name, (TestSSTableLoader,), {'version': version, '__test__': True})
