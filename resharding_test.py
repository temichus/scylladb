import os
import glob
import re
import time
from dtest import Tester, debug
import tools


class ReshardingTest(Tester):

    def __init__(self, *argv, **kwargs):
        super(ReshardingTest, self).__init__(*argv, **kwargs)
        self._compaction_strategy = kwargs.get('compaction_strategy', 'LeveledCompactionStrategy')
        self._smp = kwargs.get('smp', '2')
        self._mem = '{}M'.format(512 * int(self._smp))

    def setUp(self):
        super(ReshardingTest, self).setUp()
        cluster = self.cluster
        conf_values = dict(murmur3_partitioner_ignore_msb_bits=0)
        if 'counter' in self._testMethodName:
            conf_values.update(dict(experimental=True))
        cluster.set_configuration_options(values=conf_values)
        cluster = cluster.populate(1)
        cluster.start(jvm_args=['--smp', self._smp, '--memory', self._mem])
        self.node = cluster.nodelist()[0]

    def _reload_with_resharding(self):
        debug('Reload node with resharding')
        self.node.stop(wait_other_notice=True)

        data_files_num_before = self._get_number_of_data_files()

        self.node.set_configuration_options(values={'murmur3_partitioner_ignore_msb_bits': 12})
        self.node.start(jvm_args=['--smp', self._smp, '--memory', self._mem],
                        wait_other_notice=True, wait_for_binary_proto=True)

        return data_files_num_before

    def _get_number_of_data_files(self):
        data_dir = os.path.join(self.node.get_path(), 'data/keyspace1/standard1-*')
        data_files = glob.glob(os.path.join(data_dir, '*.*'))
        return len(data_files)

    def _verify_number_of_data_files(self, expected_num):
        debug('Verify number of data files')
        data_files_num = self._get_number_of_data_files()
        self.assertEquals(data_files_num, expected_num)

    def _wait_for_resharding(self, timeout=60):
        """
        wait until there's no RESHARD listed in compactionstats
        sleep for more 5 seconds
        break if there's no RESHARD in compactionstats
        """
        debug('Wait for re-sharding to be finished')
        patt = re.compile('RESHARD')
        to = 0
        sleep_time = 5
        while to <= timeout:
            out, err = self.node.nodetool("compactionstats", capture_output=True)
            m = patt.search(out)
            if not to or to == timeout:
                debug(out)
            if not m:
                if not timeout:
                    return True
                time.sleep(sleep_time)
                return self._wait_for_resharding(timeout=0)
            time.sleep(sleep_time)
            to += sleep_time
        return False

    def _run_stress(self, op_cnt, stress_cmd):
        res = self.node.stress_object(stress_cmd)
        self.assertIsInstance(res, dict, 'failed to run stress test')
        self.assertEquals(res['Total errors'], 0)
        self.assertGreaterEqual(res['Total partitions'], op_cnt)

    def _verify_row_number(self, cf, expected_row_num):
        session = self.patient_cql_connection(self.node)
        resp = session.execute('SELECT count(*) FROM keyspace1.{};'.format(cf), timeout=120)
        row_number = tools.rows_to_list(resp)[0][0]
        debug('number of rows: {}'.format(row_number))
        self.assertEquals(row_number, expected_row_num)

    def _verify_data(self, op_cnt, stress_cmd):
        debug('Read data')
        res = self.node.stress_object(stress_cmd)
        self.assertIsInstance(res, dict, 'failed to run stress test')
        self.assertEquals(res['Total errors'], 0)
        self.assertGreaterEqual(res['Total partitions'], op_cnt)

    def resharding_basic_test(self):
        """
        Resharding with small data set(c-s 1M objects) after changing the parameter
        and restarting the cluster
        """
        debug('Run stress test on node1')
        op_cnt = 1000000
        stress_cmd = ['write', 'n={}'.format(op_cnt), 'no-warmup', '-rate', 'threads=16',
                      '-schema', 'compaction(strategy={})'.format(self._compaction_strategy)]
        self._run_stress(op_cnt, stress_cmd)

        self._verify_row_number('standard1', op_cnt)

        data_files_num_before = self._reload_with_resharding()

        data_files_num_during = self._get_number_of_data_files()
        self.assertLessEqual(data_files_num_during, data_files_num_before * 3)

        res = self._wait_for_resharding()
        self.assertEquals(res, True, 'Failed to recognize re-sharding finish')

        self._verify_number_of_data_files(data_files_num_before)

        stress_cmd = ['read', 'n={}'.format(op_cnt), 'no-warmup', '-rate', 'threads=16']
        self._verify_data(op_cnt, stress_cmd)
        self._verify_row_number('standard1', op_cnt)

    def resharding_counter_test(self):
        """
        Resharding with small counter data set(c-s 1M counter objects) after changing the parameter
        and restarting the cluster
        """
        session = self.patient_cql_connection(self.node)
        session.execute("""
            CREATE KEYSPACE keyspace1
            WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
        """)
        session.execute("""
            CREATE TABLE keyspace1.counter1 (
                key blob PRIMARY KEY,
                "C0" counter,
                "C1" counter,
                "C2" counter,
                "C3" counter,
                "C4" counter
            ) WITH COMPACT STORAGE
                AND bloom_filter_fp_chance = 0.01
                AND caching = '{"keys":"ALL","rows_per_partition":"ALL"}'
                AND comment = ''
                AND compression = {}
                AND dclocal_read_repair_chance = 0.1
                AND default_time_to_live = 0
                AND gc_grace_seconds = 864000
                AND max_index_interval = 2048
                AND memtable_flush_period_in_ms = 0
                AND min_index_interval = 128
                AND read_repair_chance = 0.0
                AND speculative_retry = '99.0PERCENTILE';
        """)

        debug('Run counter_write stress test on node1')
        op_cnt = 1000000
        stress_cmd = ['counter_write', 'n={}'.format(op_cnt), 'no-warmup', '-rate', 'threads=16',
                      '-schema', 'compaction(strategy={})'.format(self._compaction_strategy)]

        self._run_stress(op_cnt, stress_cmd)

        self._verify_row_number('counter1', op_cnt)

        data_files_num_before = self._reload_with_resharding()

        data_files_num_during = self._get_number_of_data_files()
        self.assertLessEqual(data_files_num_during, data_files_num_before * 3)

        res = self._wait_for_resharding()
        self.assertEquals(res, True, 'Failed to recognize re-sharding finish')

        self._verify_number_of_data_files(data_files_num_before)

        stress_cmd = ['counter_read', 'n={}'.format(op_cnt), 'no-warmup', '-rate', 'threads=16']
        self._verify_data(op_cnt, stress_cmd)
        self._verify_row_number('counter1', op_cnt)


strategies = ['LeveledCompactionStrategy', 'SizeTieredCompactionStrategy', 'DateTieredCompactionStrategy']
for strategy in strategies:
    cls_name = ('ReshardingTest_with_' + strategy)
    vars()[cls_name] = type(cls_name, (ReshardingTest,), {'compaction_strategy': strategy, '__test__': True})
