import time
from concurrent.futures import ThreadPoolExecutor

import pytest

from dtest_class import Tester
from dtest_setup_overrides import DTestSetupOverrides
from tools.misc import ImmutableMapping


class TestMetadata(Tester):
    @pytest.fixture(scope='function', autouse=True)
    def fixture_dtest_setup_overrides(self, dtest_config):
        dtest_setup_overrides = DTestSetupOverrides()
        dtest_setup_overrides.cluster_options = ImmutableMapping({'start_rpc': 'true'})
        return dtest_setup_overrides

    def force_compact(self):
        cluster = self.cluster
        (node1, node2) = cluster.nodelist()
        node1.nodetool("compact keyspace1 standard1")

    def force_repair(self):
        cluster = self.cluster
        (node1, node2) = cluster.nodelist()
        node1.nodetool('repair keyspace1 standard1')

    def do_read(self):
        cluster = self.cluster
        (node1, node2) = cluster.nodelist()

        node1.stress(['read', 'no-warmup', 'n=30000', '-schema', 'replication(factor=2)', 'compression=LZ4Compressor',
                      '-rate', 'threads=1'])

    @pytest.mark.skip('limit compaction not supported by scylla https://github.com/scylladb/scylla/issues/72')
    def test_metadata_reset_while_compact(self):
        """
        Resets the schema while a compact, read and repair happens.
        All kinds of glorious things can fail.
        """

        # while the schema is being reset, there will inevitably be some
        # queries that will error with this message
        self.fixture_dtest_setup.ignore_log_patterns = ['.*Unknown keyspace/cf pair.*']

        cluster = self.cluster
        cluster.populate(2).start(wait_other_notice=True)
        (node1, node2) = cluster.nodelist()

        node1.nodetool("disableautocompaction")

        for i in range(3):
            node1.stress(['write', 'no-warmup', 'n=30000', '-schema', 'replication(factor=2)',
                          'compression=LZ4Compressor', '-rate', 'threads=5', '-pop', 'seq=1..30000'])
            node1.flush()

        executor = ThreadPoolExecutor(max_workers=3)
        thread = executor.submit(self.force_compact)
        time.sleep(1)

        thread2 = executor.submit(self.force_repair)
        time.sleep(5)

        thread3 = executor.submit(self.do_read)
        time.sleep(5)

        node1.nodetool("resetlocalschema")

        thread.result()
        thread2.result()
        thread3.result()
