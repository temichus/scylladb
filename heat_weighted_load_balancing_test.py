from __future__ import print_function
import time
from dtest import Tester, debug
from concurrent.futures import ThreadPoolExecutor
from nose.plugins.attrib import attr
from tools import create_stress_compatible_table


@attr('dtest-full')
class HeatWeightedLB(Tester):

    METRICS = ['scylla_storage_proxy_coordinator_reads_local_node',
               'scylla_storage_proxy_replica_reads',
               'scylla_column_family_cache_hit_rate.*cf=.*standard1']

    def __init__(self, *argv, **kwargs):
        super(HeatWeightedLB, self).__init__(*argv, **kwargs)
        self._op_cnt = 5000
        self._metrics_count = 60

    def _pretty_print(self, metrics):
        for key in metrics:
            for node_ind in (1, 2, 3):
                if not metrics[key][node_ind]:
                    debug('WARNING: no metrics found for {}'.format(key))
                    continue
            debug(key)
            debug('{:10s}   {:10s}   {:10s}'.format('node1', 'node2', 'node3'))
            for i in range(self._metrics_count):
                value = 'delta' if 'cache_hit_rate' not in key else 'val'
                debug('{:15s}  {:15s}  {:15s}'.format(str(metrics[key][1][i][value]),
                                                      str(metrics[key][2][i][value]),
                                                      str(metrics[key][3][i][value])))

    def get_metrics_from_nodes(self):
        debug('Get metrics from all nodes')
        node_metrics = {k: {1: [], 2: [], 3: []} for k in self.METRICS}
        for _ in range(self._metrics_count):
            t = time.time()
            for node_ind in (1, 2, 3):
                metrics = self.get_node_metrics(node_ip=self.cluster.get_node_ip(node_ind), metrics=self.METRICS)
                for k, v in metrics.items():
                    delta = v - node_metrics[k][node_ind][-1]['val'] if node_metrics[k][node_ind] else 0
                    node_metrics[k][node_ind].append(dict(val=v, delta=delta))
            delta = time.time() - t
            if delta < 1:
                time.sleep(1 - delta)
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
                        # parameter's delta is within 0.25x - 4x for all the nodes
                        delta_ratio = metrics[key][node_ind][i]['delta'] / metrics[key][2][i]['delta']
                        self.assertGreaterEqual(delta_ratio, 0.25)
                        self.assertLessEqual(delta_ratio, 4)
                    else:
                        # parameter's delta on the restarted node is less from 3 to 13 times
                        mean_window = 5
                        mean_avg = sum([metrics[key][node_ind][j]['delta']
                                        for j in range(i, i + mean_window)]) / mean_window
                        node_mean_avg = sum([metrics[key][2][j]['delta']
                                             for j in range(i, i + mean_window)]) / mean_window
                        ratio = mean_avg / node_mean_avg
                        lower_bound = 1 + 2 * (50 - i) / 40
                        upper_bound = 11 + 2 * (50 - i) / 40
                        err_msg = 'Cache difference between node{} and node2 is out of range: {}/{}={} expected to be {} < ratio <= {}. index={} metric {}'.format(
                            node_ind, mean_avg, node_mean_avg, ratio,
                            lower_bound, upper_bound,
                            i, key)
                        assert ratio > lower_bound and ratio <= upper_bound, err_msg
        key = 'scylla_column_family_cache_hit_rate.*cf=.*standard1'
        last_drop = None
        for i in range(20, 50):
            for node_ind in (1, 3):
                if cached:
                    # parameter's value is equal for all the nodes
                    self.assertEqual(metrics[key][node_ind][i]['val'], metrics[key][2][i]['val'])
                else:
                    # parameter's value on the restarted node is less than others
                    self.assertGreaterEqual(metrics[key][node_ind][i]['val'], metrics[key][2][i]['val'])
            if not cached:
                # parameter's value on the restarted node may drop, but just a bit
                ratio = metrics[key][2][i]['val'] / metrics[key][2][i-1]['val']
                if ratio < 1.0:
                    # allow one slight drop and then plateau at most
                    self.assertGreaterEqual(ratio, 0.98)
                    self.assertEqual(last_drop, None)
                    last_drop = i
                elif ratio > 1.0:
                    last_drop = None
        # parameter's value on the restarted node is on a growing trend
        if not cached:
            v = metrics[key][2][19]['val']
            val_min = v
            val_min_pos = 19
            val_max = v
            val_max_pos = 19
            for i in range(20, 50):
                v = metrics[key][2][i]['val']
                if v < val_min:
                    val_min = v
                    val_min_pos = i
                if v > val_max:
                    val_max = v
                    val_max_pos = i
            self.assertGreater(val_max_pos, val_min_pos)
            self.assertGreater((20+50)/2, val_min_pos)
            self.assertGreaterEqual(val_max_pos, (20+50)/2)

    def run_read_thread(self):
        executor = ThreadPoolExecutor(max_workers=1)

        def run_read():
            debug('Run stress read')
            resp = self.node1.stress_object(
                ['read', 'cl=QUORUM', 'duration=1m', '-schema', 'replication(factor=3)', '-rate', 'threads>=4', 'threads<=64',
                 '-pop', 'seq=1..{}'.format(self._op_cnt)])
            if not resp or 'total partitions:read' not in resp:
                raise Exception('Error running stress test: {}'.format(resp))

        return executor.submit(run_read)

    def run_heat_weighted_load_balancing(self, cl):
        """
        Create 3-node cluster, run write, then read all the data(heat cache),
        restart one node, check that it starts to serve gradually due to a cold cache.
        """
        cluster = self.cluster
        cluster.set_configuration_options(values={'enable_keyspace_column_family_metrics': True})
        cluster.populate(3).start(wait_for_binary_proto=True, wait_other_notice=True)
        self.node1, self.node2, self.node3 = cluster.nodelist()
        self.ignore_log_patterns = [r'sstable read queue overloaded']

        debug('Run stress write')
        create_stress_compatible_table(self, node=self.node1, rf=3, dclocal_read_repair_chance=0.1)
        out, err = self.node1.run_cqlsh("DESCRIBE SCHEMA; // post-line comment",
                                        return_output=True)
        debug(out)
        resp = self.node1.stress_object(
            ['write', 'cl={}'.format(cl), 'n={}'.format(self._op_cnt), '-schema', 'replication(factor=3)', '-rate',
             'threads=4', '-pop', 'seq=1..{}'.format(self._op_cnt)])
        if not resp or 'total partitions:write' not in resp:
            raise Exception('Error running stress test: {}'.format(resp))

        debug('Flush system tables')
        self.node1.flush()
        self.node2.flush()
        self.node3.flush()

        thr = self.run_read_thread()
        time.sleep(10)
        metrics = self.get_metrics_from_nodes()
        self.verify_metrics(metrics)

        if not thr.done():
            debug('Cancel stress read')
            thr.cancel()

        debug('Restart node {}'.format(self.node2.name))
        self.node2.stop(wait_other_notice=True)
        self.node2.start(wait_other_notice=True, wait_for_binary_proto=True)

        thr = self.run_read_thread()
        time.sleep(10)
        metrics = self.get_metrics_from_nodes()
        self.verify_metrics(metrics, cached=False)

        if not thr.done():
            debug('Cancel stress read')
            thr.cancel()

    def heat_weighted_load_balancing_cl_ONE_test(self):
        self.run_heat_weighted_load_balancing('ONE')

    def heat_weighted_load_balancing_cl_TWO_test(self):
        self.run_heat_weighted_load_balancing('TWO')

    def heat_weighted_load_balancing_cl_ANY_test(self):
        self.run_heat_weighted_load_balancing('ANY')

    @attr('next-gating')
    def heat_weighted_load_balancing_cl_QUORUM_test(self):
        self.run_heat_weighted_load_balancing('QUORUM')
