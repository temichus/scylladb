from __future__ import division

import time
from math import floor
from os.path import getsize

import pytest

from dtest_class import Tester
import logging

from dtest_setup_overrides import DTestSetupOverrides
from tools.misc import ImmutableMapping

logger = logging.getLogger(__name__)


@pytest.mark.dtest_full
class TestSSTableSplit(Tester):

    @pytest.fixture(scope='function', autouse=True)
    def fixture_dtest_setup_overrides(self, dtest_config):
        dtest_setup_overrides = DTestSetupOverrides()
        dtest_setup_overrides.cluster_options = ImmutableMapping({'start_rpc': 'true'})
        return dtest_setup_overrides

    def test_split(self):
        """
        Check that after running compaction, sstablessplit can succesfully split
        The resultant sstable.  Check that split is reversable and that data is readable
        after carrying out these operations.
        """
        cluster = self.cluster
        cluster.populate(1).start(wait_for_binary_proto=True)
        node = cluster.nodelist()[0]
        version = cluster.version()

        logger.debug("Run stress to insert data")
        node.stress(['write', 'n=10000', '-rate', 'threads=10'])

        self._do_compaction(node)
        self._do_split(node, version)
        self._do_compaction(node)
        self._do_split(node, version)

        logger.debug("Run stress to ensure data is readable")
        node.stress(['read', 'n=10000', '-rate', 'threads=25'])

    def _do_compaction(self, node):
        logger.debug("Compact sstables.")
        node.flush()
        node.compact()
        node.flush()
        keyspace = 'keyspace1'
        sstables = node.get_sstables(keyspace, '')
        logger.debug("Number of sstables after compaction: %s" % len(sstables))

    def _do_split(self, node, version):
        logger.debug("Run sstablesplit")
        time.sleep(5.0)
        node.stop()

        # default split size is 50MB
        splitmaxsize = 10
        expected_sstable_size = (10 * 1024 * 1024)
        keyspace = 'keyspace1'

        # get the initial sstables and their total size
        origsstables = node.get_sstables(keyspace, '')
        origsstable_size = sum([getsize(sstable) for sstable in origsstables])
        logger.debug(
            "Original sstable and sizes before split: {}".format([(name, getsize(name)) for name in origsstables]))

        # calculate the expected number of sstables post-split
        expected_num_sstables = floor(origsstable_size / expected_sstable_size)

        # split the sstables
        result = node.run_sstablesplit(keyspace=keyspace, size=splitmaxsize,
                                       no_snapshot=True, debug=True)

        for (out, error, rc) in result:
            logger.debug("stdout: {}".format(out))
            logger.debug("stderr: {}".format(error))
            logger.debug("rc: {}".format(rc))

        # get the sstables post-split and their total size
        sstables = node.get_sstables(keyspace, '')
        logger.debug("Number of sstables after split: %s. expected %s" % (len(sstables), expected_num_sstables))
        assert expected_num_sstables <= len(sstables) + 1
        assert 1 <= len(sstables)

        # make sure none of the tables are bigger than the max expected size
        sstable_sizes = [getsize(sstable) for sstable in sstables]
        # add a bit extra for overhead
        assert max(sstable_sizes) <= expected_sstable_size + 512
        # make sure node can start with changed sstables
        node.start(wait_for_binary_proto=True)

    def test_single_file_split(self):
        """
        Covers CASSANDRA-8623

        Check that sstablesplit doesn't crash when splitting a single sstable at the time.
        """
        cluster = self.cluster
        cluster.populate(1).start(wait_for_binary_proto=True)
        node = cluster.nodelist()[0]

        logger.debug("Run stress to insert data")
        node.stress(['write', 'n=20000', '-rate', 'threads=10',
                     '-schema', 'compaction(strategy=LeveledCompactionStrategy, sstable_size_in_mb=10)'])
        self._do_compaction(node)
        node.stop()
        result = node.run_sstablesplit(keyspace='keyspace1', size=2, no_snapshot=True)

        for (stdout, stderr, rc) in result:
            logger.debug(stderr)
            failure = stderr.find("java.lang.AssertionError: Data component is missing")
            assert failure == -1, "Error during sstablesplit"
