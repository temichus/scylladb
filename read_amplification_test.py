import threading
import math
import time
from cassandra.query import SimpleStatement
from cassandra import ConsistencyLevel
from dtest import Tester, debug
from upgrade_tests.paging_test import PageFetcher
import scylla_tools

PARTITION_READ = 'partition'
SCAN_READ = 'scan'
KBYTE = 1024


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
        cluster.set_configuration_options(values={'hinted_handoff_enabled': False, 'compaction_enforce_min_threshold': True})
        debug("Starting cluster..")
        cluster.populate(4).start(wait_for_binary_proto=True,wait_other_notice=True)
        nodes = cluster.nodelist()

        session = self.patient_cql_connection(nodes[0])
        self.create_ks(session, 'ks', 3)
        self.create_cf(session, 'cf', read_repair=0.0, columns={'c1': 'text', 'c2': 'text'})
        scylla_tools.insert_c1c2(session, keys=xrange(1,100), consistency=ConsistencyLevel.ALL)

        debug("Stop node2")
        nodes[1].stop(wait_other_notice=True)

        cnt = 500000
        size = 2 * KBYTE
        c = 'a' * 1024  * 1 # 1KB
        cs = [c] * cnt
        debug("Insert data")
        scylla_tools.insert_c1c2(session, keys=xrange(1,cnt+1), consistency=ConsistencyLevel.QUORUM, c1_values=cs,
                              c2_values=cs)

        debug("Start node2")
        nodes[1].start(wait_other_notice=True,wait_for_binary_proto=True)

        debug("Start node2 repair")
        thr = threading.Thread(target=lambda: nodes[1].nodetool("repair -local ks cf"))
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

    def read_amplification(self, read_type, read_size, wait_interval=1, max_ratio_expected=10):
        """
        Check total bytes read corresponds to data size
        """
        cluster = self.cluster
        debug("Starting cluster..")
        cluster.populate(1).start(wait_for_binary_proto=True)
        node = cluster.nodelist()[0]

        session = self.patient_cql_connection(node)
        self.create_ks(session, 'ks', 1)
        self.create_cf(session, 'cf', read_repair=0.0, columns={'c1': 'text', 'c2': 'text'})

        c1 = 'a' * KBYTE
        c2 = 'b' * KBYTE
        size = KBYTE * 2
        cnt = read_size / size
        debug('count: %s' % cnt)
        c1s = [c1] * cnt
        c2s = [c2] * cnt
        debug("Insert data")
        scylla_tools.insert_c1c2(session, keys=range(cnt), consistency=ConsistencyLevel.ONE,
                                 c1_values=c1s, c2_values=c2s)

        node.flush()
        debug('Run compaction to prevent running this during read')
        node.compact()
        time.sleep(10)
        debug('Restart node - for cache cleanup')
        node.stop(wait_other_notice=True)
        node.start(wait_other_notice=True, wait_for_binary_proto=True)
        time.sleep(10)

        debug('Metrics before read')
        session = self.patient_cql_connection(node)
        metric_names = ['scylla_reactor_aio_bytes_read', 'scylla_reactor_aio_reads']
        node_ip = cluster.get_node_ip(1)
        io_bytes_before = self.get_metrics(metric_names, [node_ip])
        debug(io_bytes_before)

        def run_read():
            for i in range(cnt):
                result = list(session.execute("SELECT c1,c2 FROM ks.cf where key = \'k{}\'".format(i)))
                self.assertEqual(len(result), 1, len(result))

        def run_scan_read():
            future = session.execute_async(
                SimpleStatement("select * from ks.cf", fetch_size=25)
            )
            pf = PageFetcher(future).request_all(timeout=30)
            all_pages = pf.num_results_all()
            self.assertEqual(sum(all_pages), cnt)

        def get_total_read_bytes(bytes_read, bytes_before):
            return bytes_read['scylla_reactor_aio_bytes_read'] - bytes_before['scylla_reactor_aio_bytes_read']

        debug('Start reading')
        thr_target = run_read if read_type == PARTITION_READ else run_scan_read
        thr = threading.Thread(target=thr_target)
        thr.start()

        debug('Metrics during read')
        total_read_bytes = 0
        while thr.is_alive() or total_read_bytes == 0:
            io_bytes_read = self.get_metrics(metric_names, [node_ip])
            total_read_bytes = get_total_read_bytes(io_bytes_read, io_bytes_before)
            debug(io_bytes_read)
            thr.join(wait_interval)

        debug('Metrics after read')
        io_bytes_after = self.get_metrics(metric_names, [node_ip])
        debug(io_bytes_after)

        debug("Verify there is no read amplification")
        total_read_bytes = get_total_read_bytes(io_bytes_after, io_bytes_before)
        total_written_bytes = cnt * size
        ampl = total_read_bytes / total_written_bytes
        ampl_percent = total_read_bytes * 100 / total_written_bytes
        size_formatted = read_size / KBYTE
        size_formatted = '{}kb'.format(size_formatted) if size_formatted < KBYTE else\
            '{}mb'.format(size_formatted / KBYTE)
        debug('Read amplification for {} data size: {} times or {}%'.format(size_formatted, ampl, ampl_percent))
        self.assertLessEqual(ampl, max_ratio_expected, 'Read amplification is too large: {} times'.format(ampl))

    def no_amplification_on_read_20kb_test(self):
        self.read_amplification(PARTITION_READ, KBYTE * 20, 1, 20)

    def no_amplification_on_read_400kb_test(self):
        self.read_amplification(PARTITION_READ, KBYTE * 400)

    def no_amplification_on_read_20mb_test(self):
        self.read_amplification(PARTITION_READ, KBYTE * KBYTE * 20)

    def no_amplification_on_scanning_read_20kb_test(self):
        self.read_amplification(SCAN_READ, KBYTE * 20)

    def no_amplification_on_scanning_read_2mb_test(self):
        self.read_amplification(SCAN_READ, KBYTE * KBYTE * 2)

    def no_amplification_on_scanning_read_20mb_test(self):
        self.read_amplification(SCAN_READ, KBYTE * KBYTE * 20)
