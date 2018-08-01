import threading
import time
from dtest import Tester, debug


class HeatWeightedLB(Tester):
    _multiprocess_can_split_ = False

    METRICS = ['scylla_storage_proxy_coordinator_reads_local_node',
               'scylla_storage_proxy_replica_reads',
               'scylla_column_family_cache_hit_rate.*cf=.*standard1']

    def __init__(self, *argv, **kwargs):
        super(HeatWeightedLB, self).__init__(*argv, **kwargs)
        self._op_cnt = 10000

    def _pretty_print(self, metrics):
        for key in metrics:
            for node_ind in (1, 2, 3):
                if not metrics[key][node_ind]:
                    debug('WARNING: no metrics found for {}'.format(key))
                    continue
            print key
            print '{:10s}   {:10s}   {:10s}'.format('node1', 'node2', 'node3')
            for i in range(100):
                value = 'delta' if 'cache_hit_rate' not in key else 'val'
                print '{:15s}  {:15s}  {:15s}'.format(str(metrics[key][1][i][value]),
                                                      str(metrics[key][2][i][value]),
                                                      str(metrics[key][3][i][value]))

    def get_metrics_from_nodes(self):
        debug('Get metrics from all nodes')
        node_metrics = {k: {1: [], 2: [], 3: []} for k in self.METRICS}
        for i in range(100):
            for node_ind in (1, 2, 3):
                metrics = self.get_node_metrics(node_ip=self.cluster.get_node_ip(node_ind), metrics=self.METRICS)
                for k, v in metrics.iteritems():
                    delta = v - node_metrics[k][node_ind][-1]['val'] if node_metrics[k][node_ind] else 0
                    node_metrics[k][node_ind].append(dict(val=v, delta=delta))
            time.sleep(1)
        self._pretty_print(node_metrics)
        return node_metrics

    def verify_metrics(self, metrics, cached=True):
        """
        On regular read all the parameters have an equal values for all the nodes,
        but after restart of one of the nodes(node2), the values for this node expected to be
        much less then on other nodes, and grow with cache filling.
        """
        debug('Verify metrics')
        for key in ('scylla_storage_proxy_coordinator_reads_local_node', 'scylla_storage_proxy_replica_reads'):
            debug('Verify {}'.format(key))
            for i in range(10, 50):
                for node_ind in (1, 3):
                    if cached:
                        # parameter's delta is almost equal for all the nodes
                        self.assertLessEqual(metrics[key][node_ind][i]['delta']/metrics[key][2][i]['delta'], 1)
                    else:
                        # parameter's delta on the restarted node is less from 4 to 13 times
                        mean_window = 5
                        mean_avg = sum([metrics[key][node_ind][j]['delta'] for j in range(i, i + mean_window)]) / mean_window
                        node_mean_avg = sum([metrics[key][2][j]['delta'] for j in range(i, i + mean_window)]) / mean_window
                        self.assertIn(mean_avg / node_mean_avg, range(4, 13),
                                      'Cache difference between nodes is less then expected: {}/{}, metric {}'.format(
                                          mean_avg, node_mean_avg, key))
        key = 'scylla_column_family_cache_hit_rate.*cf=.*standard1'
        for i in range(20, 50):
            for node_ind in (1, 3):
                if cached:
                    # parameter's value is equal for all the nodes
                    self.assertEqual(metrics[key][node_ind][i]['val'], metrics[key][2][i]['val'])
                else:
                    # parameter's delta on the restarted node is less but growing
                    self.assertGreater(metrics[key][node_ind][i]['val'], metrics[key][2][i]['val'])
                    self.assertGreaterEqual(metrics[key][2][i]['val'], metrics[key][2][i - 1]['val'])

    def run_read_thread(self):
        def run_read():
            debug('Run stress read')
            resp = self.node1.stress_object(
                ['read', 'cl=QUORUM', '-schema', 'replication(factor=3)', '-rate', 'threads>=4', 'threads<=64',
                 '-pop', 'seq=1..{}'.format(self._op_cnt)])
            if not resp or 'total partitions:read' not in resp:
                raise Exception('Error running stress test: {}'.format(resp))

        thr = threading.Thread(target=run_read)
        thr.start()
        return thr

    def run_heat_weighted_load_balancing(self, cl):
        """
        Create 3-node cluster, run write, then read all the data(heat cache),
        restart one node, check that it starts to serve gradually due to a cold cache.
        """
        cluster = self.cluster
        cluster.set_configuration_options(values={'enable_keyspace_column_family_metrics': True})
        cluster.populate(3).start()
        self.node1, self.node2, self.node3 = cluster.nodelist()

        debug('Run stress write')
        resp = self.node1.stress_object(
            ['write', 'cl={}'.format(cl), '-schema', 'replication(factor=3)', '-rate', 'threads=4',
             '-pop', 'seq=1..{}'.format(self._op_cnt)])
        if not resp or 'total partitions:write' not in resp:
            raise Exception('Error running stress test: {}'.format(resp))

        debug('Flush system tables')
        self.node1.flush()
        self.node2.flush()
        self.node3.flush()

        thr = self.run_read_thread()
        time.sleep(30)
        metrics = self.get_metrics_from_nodes()
        self.verify_metrics(metrics)

        debug('Wait for stress read finish')
        thr.join()
        debug('Restart node {}'.format(self.node2.name))
        self.node2.stop(wait_other_notice=True)
        self.node2.start(wait_other_notice=True, wait_for_binary_proto=True)

        thr = self.run_read_thread()
        metrics = self.get_metrics_from_nodes()
        self.verify_metrics(metrics, cached=False)

        debug('Wait for stress read finish')
        thr.join()

    def heat_weighted_load_balancing_cl_ONE_test(self):
        self.run_heat_weighted_load_balancing('ONE')

    def heat_weighted_load_balancing_cl_TWO_test(self):
        self.run_heat_weighted_load_balancing('TWO')

    def heat_weighted_load_balancing_cl_ANY_test(self):
        self.run_heat_weighted_load_balancing('ANY')

    def heat_weighted_load_balancing_cl_QUORUM_test(self):
        self.run_heat_weighted_load_balancing('QUORUM')
