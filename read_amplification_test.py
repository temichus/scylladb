import threading
import math
import time
from dtest import Tester, debug


class ReadAmplificationTest(Tester):

    def get_metrics(self, metric_names, node_ips=[]):
        metrics = {n: 0 for n in metric_names}
        for node_ip in node_ips:
            node_metrics = self.get_node_metrics(node_ip=node_ip, metrics=metrics.keys())
            for key in metrics:
                self.assertIn(key, node_metrics, 'Metrics not found: {}'.format(key))
            metrics = {k: metrics[k] + node_metrics[k] for k in metrics}
        debug(metrics)
        return metrics

    def no_read_amplification_on_repair_test(self):
        """
        Check total bytes read during streaming on repair corresponds to data size
        """
        cluster = self.cluster
        debug("Starting cluster..")
        cluster.populate(4).start(wait_for_binary_proto=True)
        nodes = cluster.nodelist()

        debug("Stop node2")
        nodes[1].stop(wait_other_notice=True)

        debug("Run stress write")
        cnt = 1000000
        size = 1024
        resp = nodes[0].stress_object(stress_options=['write', 'n={}'.format(cnt), 'cl=QUORUM',
                                                      '-schema', 'replication(factor=3)',
                                                      '-col', 'size=FIXED({}) n=FIXED(1)'.format(size),
                                                      '-pop', 'seq=1..{}'.format(cnt)])
        self.assertIsInstance(resp, dict, 'Stress error: {}'.format(resp))
        self.assertAlmostEqual(int(resp['Total partitions:write']), cnt, delta=500)

        debug("Start node2")
        nodes[1].start(wait_other_notice=True)

        debug("Start node2 repair")
        thr = threading.Thread(target=lambda: nodes[1].nodetool("repair -local keyspace1 standard1"))
        thr.start()

        debug("Verify there is no read amplification in repair streaming")
        node_ips = [cluster.get_node_ip(node_ind) for node_ind in xrange(1, len(nodes) + 1)]
        amplification_rate = 3
        max_val = {}
        metric_names = ['scylla_streaming_total_incoming_bytes', 'scylla_streaming_total_outgoing_bytes']
        while thr.is_alive():
            bytes_total = self.get_metrics(metric_names, node_ips)
            for param in bytes_total:
                self.assertLess(bytes_total[param], size * cnt * amplification_rate)
                max_val[param] = bytes_total[param] if param not in max_val else max(max_val[param], bytes_total[param])
            thr.join(3)
        for key in max_val:
            debug('{}: {}(+{}%)'.format(
                key, max_val[key], int(math.fabs(max_val[key] - (cnt * size)) * 100 / (cnt * size))))

    def read_amplification(self, read_size, wait_interval=1, threads=100, max_ratio_expected=10):
        """
        Check total bytes read corresponds to data size
        """
        cluster = self.cluster
        debug("Starting cluster..")
        cluster.populate(1).start(wait_for_binary_proto=True)
        node = cluster.nodelist()[0]

        size = 1024
        cnt = read_size / size
        debug("Write {} bytes({} writes of {} bytes) of data".format(read_size, cnt, size))
        resp = node.stress_object(stress_options=['write', 'n={}'.format(cnt),
                                                  '-col', 'size=FIXED({}) n=FIXED(1)'.format(size),
                                                  '-rate', 'threads=16', '-pop', 'seq=1..{}'.format(cnt)])
        self.assertIsInstance(resp, dict, 'Stress error: {}'.format(resp))
        self.assertAlmostEqual(int(resp['Total partitions:write']), cnt, delta=int(cnt / 100))

        node.flush()
        debug('Run compaction to prevent running this during read')
        node.compact()
        time.sleep(10)
        debug('Restart node - for cache cleanup')
        node.stop(wait_other_notice=True)
        node.start(wait_other_notice=True)
        time.sleep(10)

        debug('Metrics before read')
        metric_names = ['scylla_reactor_aio_bytes_read', 'scylla_reactor_aio_reads']
        node_ip = cluster.get_node_ip(1)
        io_bytes_before = self.get_metrics(metric_names, [node_ip])
        debug(io_bytes_before)

        def run_read(threads=100):
            resp = node.stress_object(stress_options=['read', 'n={}'.format(cnt),
                                                      '-col', 'size=FIXED({}) n=FIXED(1)'.format(size),
                                                      '-rate', 'threads={}'.format(threads),
                                                      '-pop', 'seq=1..{}'.format(cnt)])
            self.assertIsInstance(resp, dict, 'Stress error: {}'.format(resp))
            self.assertAlmostEqual(int(resp['Total partitions:read']), cnt, delta=int(cnt / 100))

        debug('Start reading')
        thr = threading.Thread(target=run_read, args=(threads, ))
        thr.start()

        debug('Metrics during read')
        while thr.is_alive():
            io_bytes_read = self.get_metrics(metric_names, [node_ip])
            debug(io_bytes_read)
            thr.join(wait_interval)

        debug("Verify there is no read amplification")
        total_read_bytes = io_bytes_read['scylla_reactor_aio_bytes_read'] -\
                           io_bytes_before['scylla_reactor_aio_bytes_read']
        total_written_bytes = cnt * size
        ampl = total_read_bytes / total_written_bytes
        ampl_percent = total_read_bytes * 100 / total_written_bytes
        size_formatted = read_size / 1024
        size_formatted = '{}kb'.format(size_formatted) if size_formatted < 1024 else\
            '{}mb'.format(size_formatted / 1024)
        debug('Read amplification for {} data size: {} times or {}%'.format(size_formatted, ampl, ampl_percent))
        self.assertLessEqual(ampl, max_ratio_expected, 'Read amplification is too large: {} times'.format(ampl))

    def no_amplification_on_read_20kb_test(self):
        self.read_amplification(1024 * 20, 1, 1, 20)

    def no_amplification_on_read_20mb_test(self):
        self.read_amplification(1024 * 1024 * 20)

    def no_amplification_on_read_1gb_test(self):
        self.read_amplification(1024 * 1024 * 1000, 15, 1000)
