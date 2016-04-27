import subprocess
import shutil
import os


from migration_test import MigrationTestBase
from dtest import debug
from tools import safe_mkdtemp
from nose import tools

@tools.istest
class TestSSTableLoader(MigrationTestBase):
    def load_migrated_tables(self, node, migrated_files_dir):
        cassandra_sstable_dir = self.get_cassandra_sstable_dir(node, migrated_files_dir)
        debug("cassandra sstable dir is {}".format(cassandra_sstable_dir))

        ks = "ks"
        cf = "cf"
        tmpdir = safe_mkdtemp()
        dir = os.path.join(tmpdir, ks, cf);

        os.mkdir(os.path.join(tmpdir, ks));
        os.mkdir(dir);

        debug("Copying sstables created by Cassandra...")
        self.copy_files_to(cassandra_sstable_dir, dir)

        ip = node.address()
        args = [node.get_tool('sstableloader'), '-v', '-d', ip, dir]
        p = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = p.communicate()
        exit_status = p.wait()

        shutil.rmtree(tmpdir)

        if exit_status != 0:
            raise Exception("sstableloader command '%s' failed; exit status: %d'; stdout: %s; stderr: %s" %
                            (" ".join(args), exit_status, stdout, stderr))

