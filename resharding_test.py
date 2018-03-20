import os
import glob
import re
import time
from dtest import Tester, debug
from tools import rows_to_list, require
import datetime

class ReshardingTest(Tester):
    DEFAUL_MUMUR3 = 12
    DEFAULT_SMP = '2'
    DEFAULT_NODES = 1
    __test__ = False
    def __init__(self, *args, **kwargs):
        super(ReshardingTest, self).__init__(*args, **kwargs)
        self.compaction_strategy = self.compaction_strategy if hasattr(self, 'compaction_strategy') else 'LeveledCompactionStrategy'
        self.smp = self.smp if hasattr(self, 'smp') else self.DEFAULT_SMP
        self.murmur3 = self.murmur3 if hasattr(self, 'murmur3') else self.DEFAUL_MUMUR3
        self.nodes = self.nodes if hasattr(self, 'nodes') else self.DEFAULT_NODES
        # smp_for_increase and smp_for_decrease values should be according to the monster environment
        self.smp_for_increase = '47'
        self.smp_for_decrease = '9'
        self.murmur3_for_decrease = 10
        self.murmur3_for_increase = 17
        self.rf = 1 if self.nodes < 3 else 3
        self.mem =  self.set_memory_param(self.smp)

    def setUp(self):
        super(ReshardingTest, self).setUp()
        cluster = self.cluster
        cluster = cluster.populate(self.nodes)
        cluster.set_configuration_options(values={'experimental': True})
        cluster.start(jvm_args=['--smp', self.smp, '--memory', self.mem])
        self.node = cluster.nodelist()[0]

    @staticmethod
    def set_memory_param(smp):
        return '{}M'.format(512 * int(smp))

    def _reload_with_resharding(self, murmur3=DEFAUL_MUMUR3, smp=None):
        debug('{0} Reload node with resharding:\n CPU: from {1} to {2}\n murmur3 parameter: from {3} to {4}'.format(
                                    datetime.datetime.now(), self.smp, smp, self.murmur3, murmur3))
        smp = self.smp if not smp else smp
        self.node.stop(wait_other_notice=True)

        data_files_num_before = self._get_number_of_data_files()
        self.node.set_configuration_options(values={'murmur3_partitioner_ignore_msb_bits': murmur3})
        self.node.start(jvm_args=['--smp', smp, '--memory', self.set_memory_param(smp)],
                        wait_other_notice=True, wait_for_binary_proto=True)
        debug('{0} Node has been started'.format(datetime.datetime.now()))
        return data_files_num_before

    def _get_number_of_data_files(self, data_dir='data/keyspace1/standard1-*'):
        data_files = []
        for node in self.cluster.nodelist():
            data_dir = os.path.join(node.get_path(), data_dir)
            data_files.extend(glob.glob(os.path.join(data_dir, '*.*')))
            # TODO: remove the debug lines below when the issue #3302 is fixed
            # debug('\n\n\n\n\n\n')
            # debug(len(data_files))
            # debug('\n'.join(['{0} - size {1} bytes'.format(f, os.stat(f).st_size) for f in data_files]))
        return len(data_files)

    def _verify_number_of_data_files(self, expected_num):
        debug('Verify number of data files')
        data_files_num = self._get_number_of_data_files()
        self.assertLessEqual(data_files_num, expected_num,
                             msg='{0} not less than or equal to {1}. Data files amount after resharding '
                                 'should be not more then data files amount before resharding multiplying by 3.'.format
                             (data_files_num, expected_num))

    def _wait_for_resharding(self, timeout=60, reshard_found=False):
        """
        wait until there's no RESHARD listed in compactionstats
        sleep for more 5 seconds
        break if there's no RESHARD in compactionstats
        """
        debug('Wait for re-sharding to be finished')
        patt = re.compile('RESHARD')
        to = 0
        sleep_time = 5
        m = False
        prev_out = []
        while to <= timeout:
            # Commented because of "compactionstats" does not recognize resharding if there are few rows
            # because of resharding is going fast and finishs before this function calls.
            # out, err = self.node.nodetool("compactionstats", capture_output=True)
            # m = patt.search(out)
            # Temporary solution while the "compactionstats" problem will be resolved
            out = self.node.grep_log('Reshard')
            m = False if not out else True
            if m and prev_out == [o[0] for o in out]:
                break
            prev_out = [o[0] for o in out]
            # END - Temporary solution while the "compactionstats" problem will be resolved
            # debug('{0} {1}'.format(datetime.datetime.now(), out))
            # if not to or to == timeout:
            #     debug('{0} {1}'.format(datetime.datetime.now(), out))
            if not m:
                if not timeout:
                    return reshard_found
                time.sleep(sleep_time)
                return self._wait_for_resharding(timeout=0, reshard_found=reshard_found)
            reshard_found = True
            time.sleep(sleep_time)
            to += sleep_time
        return reshard_found

    def _run_stress(self, op_cnt, stress_cmd):
        res = self.node.stress_object(stress_cmd)
        self.assertIsInstance(res, dict, 'failed to run stress test')
        self.assertEquals(res['Total errors'], 0)
        self.assertGreaterEqual(res['Total partitions'], op_cnt)

    def _verify_row_number(self, cf, expected_row_num, keyspace='keyspace1'):
        session = self.patient_cql_connection(self.node)
        resp = session.execute('SELECT count(*) FROM {0}.{1};'.format(keyspace, cf), timeout=120)
        row_number = rows_to_list(resp)[0][0]
        debug('number of rows: {}'.format(row_number))
        self.assertEquals(row_number, expected_row_num)

    def _verify_data(self, op_cnt, stress_cmd):
        debug('Read data')
        res = self.node.stress_object(stress_cmd)
        self.assertIsInstance(res, dict, 'failed to run stress test')
        self.assertEquals(res['Total errors'], 0)
        self.assertGreaterEqual(res['Total partitions'], op_cnt)

    def _check_logs_for_errors(self):
        debug('Verify there are no errors in the logs')
        for node in self.cluster.nodelist():
            self.assertFalse(node.grep_log_for_errors(distinct_errors=True))

    def _resharding_basic(self, reshard_to, rows, murmur3):
        debug('Run stress test on node1')
        op_cnt = rows
        stress_cmd = ['write', 'n={}'.format(op_cnt), 'no-warmup', '-rate', 'threads=16',
                      '-schema', 'replication(factor={})'.format(self.rf), 'compaction(strategy={})'.format(self.compaction_strategy)]
        self._run_stress(op_cnt, stress_cmd)

        self._verify_row_number('standard1', op_cnt)

        data_files_num_before = self._reload_with_resharding(smp=reshard_to, murmur3=murmur3)

        data_files_num_during = self._get_number_of_data_files()

        res = self._wait_for_resharding()
        exp_res, msg = (False, 'Unexpected re-sharding recognized') \
                       if reshard_to == self.smp and murmur3 == self.murmur3 \
                       else (True, 'Failed to recognize re-sharding finish')
        self.assertEquals(res, exp_res, msg)
        self._check_logs_for_errors()

        # Verify data files number during resharding
        # data_files_num_before * 3: multiply by 3 because of we expect that files amount could
        #                            be increased not more than *3 (by Avi)
        self.assertLessEqual(data_files_num_during, data_files_num_before * 3,
                             msg='{0} not less than or equal to {1}. Data files amount during resharding '
                                 'should be not more then data files amount before resharding multiplying by 3.'.format
                             (data_files_num_during, data_files_num_before * 3))

        # Verify data files number after resharding and compaction
        # data_files_num_before * 3: multiply by 3 because of could be increased not more than *3 (by Avi)
        self._verify_number_of_data_files(data_files_num_before*3)

        stress_cmd = ['read', 'n={}'.format(op_cnt), 'no-warmup', '-rate', 'threads=16']
        self._verify_data(op_cnt, stress_cmd)
        self._verify_row_number('standard1', op_cnt)

        # Verify data files number after resharding and compaction
        # data_files_num_before * 3: multiply by 3 because of could be increased not more than *3 (by Avi)
        self._verify_number_of_data_files(data_files_num_before*3)

    @require('#3273')
    def resharding_by_murmur3_increase_test(self):
        """
        Resharding with 10M objects after increasing the MURMUR3 parameter
        and restarting the cluster
        """
        if self.compaction_strategy in ['SizeTieredCompactionStrategy', 'DateTieredCompactionStrategy']:
            self.skipTest('issue #3302 - High data files amount during resharding')
        self._resharding_basic(self.smp, rows=1000, murmur3=self.murmur3_for_increase)


    @require('#3273')
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
                      '-schema', 'compaction(strategy={})'.format(self.compaction_strategy)]

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
# SMP value should be according to the monster environment
smp = '24'
murmur3 = 15
for node in [1, 4]:
    for strategy in strategies:
        cls_name = ('ReshardingTest_nodes' + str(node) + '_with_' + strategy)
        vars()[cls_name] = type(cls_name, (ReshardingTest,), {'nodes': node, 'compaction_strategy': strategy, 'smp': smp,
                                                              'murmur3': murmur3, '__test__': True})
