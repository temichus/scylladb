import glob
import logging
import os
import subprocess

import pytest
from ccmlib import common
from ccmlib.node import NodetoolError

# These must match the stress schema names
from dtest_class import Tester
from dtest_setup_overrides import DTestSetupOverrides
from tools.intervention import InterruptCompaction

KEYSPACE_NAME = 'keyspace1'
TABLE_NAME = 'standard1'

logger = logging.getLogger(__name__)


def _normcase_all(files):
    """
    Return a list of the elements in xs, each with its casing normalized for
    use as a filename.
    """
    return [os.path.normcase(file) for file in files]


@pytest.mark.require("scylladb/scylla-tools-java#309")
@pytest.mark.dtest_full
@pytest.mark.single_node
class TestSSTableUtil(Tester):

    @staticmethod
    @pytest.fixture(scope='function', autouse=True)
    def fixture_dtest_setup_overrides():
        dtest_setup_overrides = DTestSetupOverrides()
        dtest_setup_overrides.cluster_options = {'start_rpc': 'true'}
        return dtest_setup_overrides

    def test_compaction(self):
        """
        @jira_ticket CASSANDRA-7066
        Check we can list the sstable files after successfull compaction (no temporary sstable files)
        """
        cluster = self.cluster
        cluster.populate(1).start(wait_for_binary_proto=True)
        node = cluster.nodelist()[0]

        self._create_data(node, KEYSPACE_NAME, TABLE_NAME, 100000)
        tmpfiles = self._check_files(node, KEYSPACE_NAME, TABLE_NAME)[1]
        assert len(tmpfiles) == 0

        node.compact()
        tmpfiles = self._check_files(node, KEYSPACE_NAME, TABLE_NAME)[1]
        assert len(tmpfiles) == 0

    def test_abortedcompaction(self):
        """
        @jira_ticket CASSANDRA-7066
        Check we can list the sstable files after aborted compaction (temporary sstable files)
        Then perform a cleanup and verify the temporary files are gone
        """
        log_file_name = 'system.log'
        cluster = self.cluster
        cluster.populate(1).start(wait_for_binary_proto=True)
        node = cluster.nodelist()[0]

        numrecords = 400000

        self._create_data(node, KEYSPACE_NAME, TABLE_NAME, numrecords)
        finalfiles, tmpfiles = self._check_files(node, KEYSPACE_NAME, TABLE_NAME)
        assert len(tmpfiles) == 0

        interrupt_compaction = InterruptCompaction(node, TABLE_NAME, filename=log_file_name)
        interrupt_compaction.start()

        with pytest.raises(expected_exception=NodetoolError):
            node.compact()

        interrupt_compaction.join()

        # should compaction finish before the node is killed, this test would fail,
        # in which case try increasing numrecords
        finalfiles, tmpfiles = self._check_files(node, KEYSPACE_NAME, TABLE_NAME, finalfiles)
        assert len(tmpfiles) > 0

        self._invoke_sstableutil(KEYSPACE_NAME, TABLE_NAME, cleanup=True)

        self._check_files(node, KEYSPACE_NAME, TABLE_NAME, finalfiles, [])

        # restart to make sure not data is lost
        node.start(wait_for_binary_proto=True)
        node.watch_log_for("Compacted(.*)%s" % (TABLE_NAME,), filename=log_file_name)

        finalfiles, tmpfiles = self._check_files(node, KEYSPACE_NAME, TABLE_NAME)
        assert len(tmpfiles) == 0

        logger.info("Run stress to ensure data is readable")
        self._read_data(node, numrecords)

    @staticmethod
    def _create_data(node, ks, table, numrecords):
        """
         This is just to create the schema so we can disable compaction
        """
        node.stress(['write', 'n=1', '-rate', 'threads=1'])
        node.nodetool('disableautocompaction %s %s' % (ks, table))

        node.stress(['write', f'n={numrecords}', '-rate', 'threads=50'])
        node.flush()

    @staticmethod
    def _read_data(node, numrecords):
        node.stress(['read', f'n={numrecords}', '-rate', 'threads=25'])

    def _check_files(self, node, ks, table, expected_finalfiles=None,  # pylint:disable=too-many-arguments
                     expected_tmpfiles=None):
        sstablefiles = _normcase_all(self._get_sstable_files(node, ks, table))
        allfiles = _normcase_all(self._invoke_sstableutil(ks, table, sstable_type='all'))
        finalfiles = _normcase_all(self._invoke_sstableutil(ks, table, sstable_type='final'))
        tmpfiles = _normcase_all(self._invoke_sstableutil(ks, table, sstable_type='tmp'))
        expected_oplogs = _normcase_all(self._get_sstable_transaction_logs(node, ks, table))
        tmpfiles_with_oplogs = _normcase_all(self._invoke_sstableutil(ks, table, sstable_type='tmp', oplogs=True))
        oplogs = list(set(tmpfiles_with_oplogs) - set(tmpfiles))

        if expected_finalfiles is None:
            expected_finalfiles = allfiles
        else:
            expected_finalfiles = _normcase_all(expected_finalfiles)

        if expected_tmpfiles is None:
            expected_tmpfiles = sorted(set(allfiles) - set(finalfiles))
        else:
            expected_tmpfiles = _normcase_all(expected_tmpfiles)

        logger.info("Comparing all files...")
        assert sstablefiles == allfiles

        logger.info("Comparing final files...")
        assert expected_finalfiles == finalfiles

        logger.info("Comparing tmp files...")
        assert expected_tmpfiles == tmpfiles

        logger.info("Comparing op logs...")
        assert expected_oplogs == oplogs

        return finalfiles, tmpfiles

    def _invoke_sstableutil(self, ks, table, sstable_type='all', oplogs=False,  # pylint:disable=too-many-arguments
                            cleanup=False):
        """
        Invoke sstableutil and return the list of files, if any
        """
        logger.info("About to invoke sstableutil...")
        node1 = self.cluster.nodelist()[0]
        env = common.make_cassandra_env(node1.get_install_cassandra_root(), node1.get_node_cassandra_root())
        tool_bin = node1.get_tool('sstableutil')

        args = [tool_bin, '--type', sstable_type]

        if oplogs:
            args.append('--oplog')
        if cleanup:
            args.append('--cleanup')

        args.extend([ks, table])
        logger.info("All parameters %s", args)

        p_open_result = subprocess.Popen(args, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        (stdout, stderr) = p_open_result.communicate()

        if p_open_result.returncode != 0:
            logger.error(stderr)
            assert False, "Error invoking sstableutil; returned {code}".format(code=p_open_result.returncode)

        logger.info(stdout)
        match = ks + os.sep + table + '-'
        ret = sorted(filter(lambda line: match in line, stdout.decode().splitlines()))
        logger.info("Got {} files".format(len(ret)))
        return ret

    @staticmethod
    def _get_sstable_files(node, ks, table):
        """
        Read sstable files directly from disk
        """
        keyspace_dir = os.path.join(node.get_path(), 'data', ks)

        ret = []
        for ext in ('*.db', '*.txt', '*.adler32', '*.crc32'):
            ret.extend(glob.glob(os.path.join(keyspace_dir, table + '-*', ext)))

        return sorted(ret)

    @staticmethod
    def _get_sstable_transaction_logs(node, ks, table):
        keyspace_dir = os.path.join(node.get_path(), 'data', ks)
        ret = glob.glob(os.path.join(keyspace_dir, table + '-*', "*.log"))

        return sorted(ret)
