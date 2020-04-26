import os
import glob
import re
import time

from nose.plugins.attrib import attr
from dtest import Tester, debug, flaky
from tools import rows_to_list, require
from scylla_tools import TableManager, MaterializedViewManager
from assertions import assert_one, assert_two_queries_equal
from cassandra import ConsistencyLevel


@attr('dtest-full', 'dtest-heavy')
class ReshardingTest(Tester):
    DEFAULT_MURMUR3_PARTITIONER = 12
    DEFAULT_SMP = 2
    DEFAULT_NODES = 1
    SMP_FOR_INCREASE = 9
    SMP_FOR_DECREASE = int(DEFAULT_SMP)
    MURMUR3_PARTITIONER_FOR_DECREASE = 10
    MURMUR3_PARTITIONER_FOR_INCREASE = 17
    __test__ = False
    def __init__(self, *args, **kwargs):
        super(ReshardingTest, self).__init__(*args, **kwargs)
        self.compaction_strategy = self.compaction_strategy if hasattr(self, 'compaction_strategy') else 'LeveledCompactionStrategy'
        self.smp = self.smp if hasattr(self, 'smp') else self.DEFAULT_SMP
        self.murmur3 = self.murmur3 if hasattr(self, 'murmur3') else self.DEFAULT_MURMUR3_PARTITIONER
        self.nodes = self.nodes if hasattr(self, 'nodes') else self.DEFAULT_NODES
        # smp_for_increase and smp_for_decrease values should be according to the monster environment
        self.rf = 1 if self.nodes < 3 else 3
        self.mem =  self.set_memory_param(self.smp)

    def setUp(self):
        super(ReshardingTest, self).setUp()
        cluster = self.cluster
        cluster = cluster.populate(self.nodes)
        cluster.set_configuration_options(values={'murmur3_partitioner_ignore_msb_bits': self.murmur3})
        cluster.start(wait_for_binary_proto=True, wait_other_notice=True, jvm_args=['--smp', str(self.smp), '--memory', self.mem])
        self.node = cluster.nodelist()[0]

    @staticmethod
    def set_memory_param(smp):
        return '{}M'.format(512 * int(smp))

    def _reload_with_resharding(self, murmur3=DEFAULT_MURMUR3_PARTITIONER, smp=None, data_dir='data/keyspace1/standard1-*'):
        debug('Reload node with resharding:\n CPU: from {0} to {1}\n murmur3 parameter: from {2} to {3}'.format(
                                    self.smp, smp, self.murmur3, murmur3))
        smp = self.smp if not smp else smp
        self.node.stop(wait_other_notice=True)

        data_files_num_before = self._get_number_of_data_files(data_dir=data_dir)
        self.node.set_configuration_options(values={'murmur3_partitioner_ignore_msb_bits': murmur3})
        self.node.start(jvm_args=['--smp', str(smp), '--memory', self.set_memory_param(smp)],
                        wait_other_notice=True, wait_for_binary_proto=True)
        debug('Node has been started')
        return data_files_num_before

    def _get_number_of_data_files(self, data_dir='data/keyspace1/standard1-*'):
        data_files = []
        data_dir = os.path.join(self.node.get_path(), data_dir)
        data_files.extend(glob.glob(os.path.join(data_dir, '*.*')))
        return len(data_files)

    def _verify_number_of_data_files(self, data_files_num_before, reshard_to, actual_data_files_num=None,
                                     data_dir='data/keyspace1/standard1-*'):
        debug('Verify number of data files')
        expected_num = data_files_num_before * reshard_to
        if not actual_data_files_num:
            actual_data_files_num = self._get_number_of_data_files(data_dir=data_dir)
        self.assertLessEqual(actual_data_files_num, expected_num,
                             msg='{0} not less than or equal to {1}. Data files amount after resharding '
                                 'should be not more then data files amount before resharding multiplying by {2}.'.format
                             (actual_data_files_num, expected_num, reshard_to))

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
            # debug(out)
            # if not to or to == timeout:
            #     debug(out)
            if not m:
                if not timeout:
                    return reshard_found
                time.sleep(sleep_time)
                return self._wait_for_resharding(timeout=0, reshard_found=reshard_found)
            reshard_found = True
            time.sleep(sleep_time)
            to += sleep_time
        return reshard_found

    def _remove_existent_ks(self, session, keyspace_name):
        session.execute("DROP KEYSPACE IF EXISTS {}".format(keyspace_name))

    def _run_stress(self, op_cnt, stress_cmd):
        res = self.node.stress_object(stress_cmd)
        if not isinstance(res,dict):
             raise Exception('Error running cassandra-stress: {}'.format(res))
        self.assertEquals(res['total errors'], 0)
        self.assertGreaterEqual(res['total partitions'], op_cnt)

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
        self.assertEquals(res['total errors'], 0)

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
        self.check_errors_all_nodes()

        # Verify data files number during resharding
        self._verify_number_of_data_files(data_files_num_before=data_files_num_before, reshard_to=reshard_to,
                                          actual_data_files_num=data_files_num_during)

        # Verify data files number after resharding and compaction
        self._verify_number_of_data_files(data_files_num_before=data_files_num_before, reshard_to=reshard_to)

        stress_cmd = ['read', 'n={}'.format(op_cnt), 'no-warmup', '-rate', 'threads=4', '-errors ignore']
        self._verify_data(op_cnt, stress_cmd)
        self._verify_row_number('standard1', op_cnt)

        # Verify data files number after resharding and compaction
        self._verify_number_of_data_files(data_files_num_before=data_files_num_before, reshard_to=reshard_to)

    def resharding_by_murmur3_increase_test(self):
        """
        Resharding with 10M objects after increasing the MURMUR3 parameter
        and restarting the cluster
        """
        self._resharding_basic(self.smp, rows=1000, murmur3=self.MURMUR3_PARTITIONER_FOR_INCREASE)

    def resharding_by_murmur3_decrease_test(self):
        """
        Resharding with 10M objects after decreasing the MURMUR3 parameter
        and restarting the cluster
        """
        self._resharding_basic(self.smp, rows=1000, murmur3=self.MURMUR3_PARTITIONER_FOR_DECREASE)

    @flaky
    def resharding_by_smp_increase_test(self):
        """
        Resharding with 10M objects after increasing the SMP parameter
        and restarting the cluster
        """
        self._resharding_basic(self.SMP_FOR_INCREASE, rows=10000, murmur3=self.murmur3)

    @flaky
    def resharding_by_smp_decrease_test(self):
        """
        Resharding with 10M objects after decreasing the SMP parameter
        and restarting the cluster
        """
        self._resharding_basic(self.SMP_FOR_DECREASE, rows=100000, murmur3=self.murmur3)

    def resharding_by_same_smp_test(self):
        """
        Cluster with 10M objects. Both SMP and MURMUR3 parameter are not changed.
        No resharding expected
        """
        self._resharding_basic(self.smp, rows=1000, murmur3=self.murmur3)

    def resharding_by_murmur3_smp_test(self):
        """
        Cluster with 10M objects. Both SMP and MURMUR3 parameter are changed
        and restarting the cluster
        """
        self._resharding_basic(self.SMP_FOR_INCREASE, rows=1000, murmur3=self.MURMUR3_PARTITIONER_FOR_INCREASE)

    @flaky
    def resharding_counter_test(self):
        """
        Resharding with small counter data set(c-s 1M counter objects) after changing the parameter
        and restarting the cluster
        """
        keyspace_name = 'keyspace1'
        session = self.patient_cql_connection(self.node)
        # If test failed and re-run by @flaky decorator, the existent keyspace should be re-created
        self._remove_existent_ks(session=session, keyspace_name=keyspace_name)
        session.execute("""
            CREATE KEYSPACE %s
            WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
        """ % keyspace_name)
        session.execute("""
            CREATE TABLE %s.counter1 (
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
        """ % keyspace_name)

        debug('Run counter_write stress test on node1')
        op_cnt = 10000
        stress_cmd = ['counter_write', 'n={}'.format(op_cnt), 'no-warmup', '-rate', 'threads=16',
                      '-schema', 'replication(factor={})'.format(self.rf),
                      'compaction(strategy={})'.format(self.compaction_strategy)]

        self._run_stress(op_cnt, stress_cmd)

        self._verify_row_number('counter1', op_cnt)

        data_files_num_before = self._reload_with_resharding(smp=self.SMP_FOR_INCREASE)

        self._verify_number_of_data_files(data_files_num_before=data_files_num_before, reshard_to=self.SMP_FOR_INCREASE)

        res = self._wait_for_resharding()
        self.assertEquals(res, True, 'Failed to recognize re-sharding finish')

        self._verify_number_of_data_files(data_files_num_before=data_files_num_before, reshard_to=self.SMP_FOR_INCREASE)
        self.check_errors_all_nodes()

        stress_cmd = ['counter_read', 'n={}'.format(op_cnt), 'no-warmup', '-rate', 'threads=16']
        self._verify_data(op_cnt, stress_cmd)
        self._verify_row_number('counter1', op_cnt)

    def resharding_mv_test(self):
        """
        Resharding with small counter data set(c-s 1M counter objects) after changing the parameter
        and restarting the cluster
        """
        session = self.patient_cql_connection(self.node)
        self.create_ks(session, 'ks', self.rf)
        compaction = {'compaction': {'class': self.compaction_strategy}}
        op_cnt = 10000
        tm = TableManager(session, self.cluster,
                          columns={'int': {'amount': 1, 'frozen': False, 'value length': {'min': 1, 'max': 100}}
                                   }, pk_columns={}, cl_columns={}, table_options=compaction)
        tm.create_table()

        mv = MaterializedViewManager(tm)
        mv_pk_name = tm.column_names_list[-1]
        mv.create_materialized_view(mv_columns={'int': {'names': [mv_pk_name]}},
                                    mv_pk_column={'type': 'int'}, options=compaction)

        tm.prefill_table(op_cnt)

        self._verify_row_number(tm.table_name, op_cnt, keyspace=tm.keyspace)
        self._verify_row_number(mv.mv_name, op_cnt, keyspace=tm.keyspace)

        data_dir = 'data/{0}/{1}-*'.format(tm.keyspace, tm.table_name)
        data_files_num_before = self._reload_with_resharding(smp=self.SMP_FOR_INCREASE, data_dir=data_dir)

        self._verify_number_of_data_files(data_files_num_before=data_files_num_before, reshard_to=self.SMP_FOR_INCREASE)

        res = self._wait_for_resharding()
        self.assertEquals(res, True, 'Failed to recognize re-sharding finish')

        self._verify_number_of_data_files(data_files_num_before=data_files_num_before, reshard_to=self.SMP_FOR_INCREASE,
                                          data_dir=data_dir)
        self.check_errors_all_nodes()

        # Read data
        session = self.patient_cql_connection(self.node)
        query = 'select count(*) from {0}.{1} where id={2}'
        for i in range(op_cnt):
            assert_one(session, query.format(tm.keyspace, tm.table_name, i), [1])
            assert_one(session, query.format(tm.keyspace, mv.mv_name, i), [1])

        # Validate data
        self._verify_row_number(tm.table_name, op_cnt, keyspace=tm.keyspace)
        self._verify_row_number(mv.mv_name, op_cnt, keyspace=tm.keyspace)
        query = 'select * from {0}.{1}'
        assert_two_queries_equal(session, query.format(tm.keyspace, tm.table_name),
                                 session, query.format(tm.keyspace, tm.table_name),
                                 consistency_level=ConsistencyLevel.ALL, session_timeout=120,
                                 group=True, groupby_column1=mv_pk_name, groupby_column2=mv_pk_name)

strategies = ['LeveledCompactionStrategy', 'SizeTieredCompactionStrategy', 'DateTieredCompactionStrategy',
              'TimeWindowCompactionStrategy']
# SMP value should be according to the monster environment
smp = 5
murmur3 = 15
for node_count in [1, 4]:
    for strategy in strategies:
        cls_name = ('ReshardingTest_nodes' + str(node_count) + '_with_' + strategy)
        vars()[cls_name] = type(cls_name, (ReshardingTest,), {'nodes': node_count, 'compaction_strategy': strategy,
                                                              'smp': smp, 'murmur3': murmur3, '__test__': True})
