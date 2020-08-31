from ccmlib.node import NodetoolError
from dtest import Tester, debug

from concurrent.futures import ThreadPoolExecutor
import os
import time


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

        try:
            node.decommission()
            self.assertFalse("Expected nodetool error")
        except NodetoolError as e:
            self.assertEqual('', e.stderr)
            self.assertTrue('Node in DRAINED state' in e.stdout)

    def test_correct_dc_rack_in_nodetool_info(self):
        """
        @jira_ticket CASSANDRA-10382

        Test that nodetool info returns the correct rack and dc
        """

        cluster = self.cluster
        cluster.populate([2, 2])
        cluster.set_configuration_options(
            values={'endpoint_snitch': 'org.apache.cassandra.locator.GossipingPropertyFileSnitch'})

        for i, node in enumerate(cluster.nodelist()):
            with open(os.path.join(node.get_conf_dir(), 'cassandra-rackdc.properties'), 'w') as snitch_file:
                for line in ["dc={}".format(node.data_center), "rack=rack{}".format(i % 2)]:
                    snitch_file.write(line + os.linesep)

        cluster.start(wait_for_binary_proto=True)

        for i, node in enumerate(cluster.nodelist()):
            out, err = node.nodetool('info')
            self.assertEqual(0, len(err), err)
            debug(out)
            for line in out.split(os.linesep):
                if line.startswith('Data Center'):
                    self.assertTrue(line.endswith(node.data_center),
                                    "Expected dc {} for {} but got {}".format(node.data_center, node.address(), line.rsplit(None, 1)[-1]))
                elif line.startswith('Rack'):
                    rack = "rack{}".format(i % 2)
                    self.assertTrue(line.endswith(rack),
                                    "Expected rack {} for {} but got {}".format(rack, node.address(), line.rsplit(None, 1)[-1]))

    def _background_workload(self, node):
        debug('start write workload in background...')
        cs_result, cs_err = node.stress(['write', 'duration=180s', 'no-warmup', '-schema', 'replication(factor=3)', '-rate', 'threads=10', '-log', 'interval=10'],
                                        capture_output=True)
        debug('background workload finished')
        debug(cs_result)
        debug(cs_err)
        return cs_result

    def _remove_seed(self, method='kill'):
        """
        We have a old issue (scylla/issues/2090), cassandra-stress will exit if
        seed node is decomission or killed. This new subtest is used to reproduce it.
        """
        self.cluster.populate(4).start(wait_for_binary_proto=True, wait_other_notice=True)
        node1, node2 = self.cluster.nodelist()[0:2]

        executor = ThreadPoolExecutor(max_workers=1)
        t = executor.submit(self._background_workload, node1)

        time.sleep(60)
        if method == 'kill':
            debug('start to kill node1 ...')
            node1.stop(gently=False)
            debug('node1 has been killed')
        elif method == 'decommission':
            debug('start to decommission node1 ...')
            node1.decommission()
            debug('decommission node1 finished')
        else:
            raise Exception('Unknown method: %s' % method)

        out, err = node2.nodetool('status', capture_output=True)
        debug(out)
        cs_result = t.result()

        self.assertTrue('END' in cs_result.split()[-1], "Stress doesn't complete successfully")

    def test_decommission_seed(self):
        """
        Test if cassandra-stress works well when seed node is decommission.
        """
        self._remove_seed(method='decommission')

    def test_kill_seed(self):
        """
        Test if cassandra-stress works well when seed node is killed.
        """
        self._remove_seed(method='kill')
