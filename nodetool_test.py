import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor

import pytest
from ccmlib.node import NodetoolError

from dtest_class import Tester

logger = logging.getLogger(__name__)


@pytest.mark.dtest_full
class TestNodetool(Tester):

    def test_decommission_after_drain_is_invalid(self):
        """
        @jira_ticket CASSANDRA-8741

        Running a decommission after a drain should generate
        an unsupported operation message and exit with an error
        code (which we receive as a NodetoolError exception).
        """
        cluster = self.cluster
        cluster.populate([3]).start()

        node = cluster.nodelist()[0]
        node.drain(block_on_log=True)

        with pytest.raises(expected_exception=(NodetoolError,)) as err:
            node.decommission()
        assert not err.value.stderr
        assert 'Node in DRAINED state' in err.value.stdout

    def test_correct_dc_rack_in_nodetool_info(self):
        """
        @jira_ticket CASSANDRA-10382

        Test that nodetool info returns the correct rack and dc
        """

        cluster = self.cluster
        cluster.populate([2, 2])
        cluster.set_configuration_options(
            values={'endpoint_snitch': 'org.apache.cassandra.locator.GossipingPropertyFileSnitch'})

        for idx, node in enumerate(cluster.nodelist()):
            with open(os.path.join(node.get_conf_dir(), 'cassandra-rackdc.properties'), 'w') as snitch_file:
                for line in ["dc={}".format(node.data_center), "rack=rack{}".format(idx % 2)]:
                    snitch_file.write(line + os.linesep)

        cluster.start(wait_for_binary_proto=True)

        for idx, node in enumerate(cluster.nodelist()):
            out, err = node.nodetool('info')
            assert not err, err
            logger.info(out)
            for line in out.split(os.linesep):
                if line.startswith('Data Center'):
                    assert line.endswith(node.data_center), f'Expected dc {node.data_center} for {node.address()} ' \
                                                            f'but got {line.rsplit(None, 1)[-1]}'
                elif line.startswith('Rack'):
                    rack = f'rack{idx % 2}'
                    assert line.endswith(rack), f'Expected rack {rack} for {node.address()} but got ' \
                                                f'{line.rsplit(None, 1)[-1]}'

    @staticmethod
    def _background_workload(node):
        logger.info('start write workload in background...')
        cs_result, cs_err = node.stress(
            ['write', 'duration=180s', 'no-warmup', '-schema', 'replication(factor=3)', '-rate', 'threads=10', '-log',
             'interval=10'],
            capture_output=True)
        logger.info('background workload finished')
        logger.info(cs_result)
        logger.info(cs_err)
        return cs_result

    def _remove_seed(self, method='kill'):
        """
        We have a old issue (scylla/issues/2090), cassandra-stress will exit if
        seed node is decomission or killed. This new subtest is used to reproduce it.
        """
        self.cluster.populate(4).start(wait_for_binary_proto=True, wait_other_notice=True)
        node1, node2 = self.cluster.nodelist()[0:2]

        executor = ThreadPoolExecutor(max_workers=1)
        thread = executor.submit(self._background_workload, node1)

        time.sleep(60)
        if method == 'kill':
            logger.info('start to kill node1 ...')
            node1.stop(gently=False)
            logger.info('node1 has been killed')
        elif method == 'decommission':
            logger.info('start to decommission node1 ...')
            node1.decommission()
            logger.info('decommission node1 finished')
        else:
            raise Exception('Unknown method: %s' % method)

        out = node2.nodetool('status', capture_output=True)[0]
        logger.info(out)
        cs_result = thread.result()

        assert 'END' in cs_result.split()[-1], "Stress doesn't complete successfully"

    @pytest.mark.parametrize('method', ['decommission', 'kill'])
    def test_seed(self, method):
        """
        Test if cassandra-stress works well when seed node is  "decommission" or "killed".
        """
        self._remove_seed(method=method)
