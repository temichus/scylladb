from dtest import Tester
from ccmlib.node import NodetoolError
from scylla_tools import insert_c1c2_no_prepared, query_c1c2
from tools import create_c1c2_table, require

import time
import re
import logging

from concurrent.futures import ThreadPoolExecutor
from collections import OrderedDict

logger = logging.getLogger(__file__)


class TestTopPartitions(Tester):
    """Class to test new functioanality of nodetool command toppartitions

    Return the most usable(writen/read) partitions in column family for appropriate period
    Doc Links:
    - https://docs.datastax.com/en/cassandra/3.0/cassandra/tools/toolsToppartitions.html

    Extends:
        Tester
    """
    toppartitions_cmd_template = "toppartitions {optional_params} {ks} {cf} {duration}"

    @staticmethod
    def _parse_toppartitions_output(output):
        """parsing output of toppartitions

        input format stored in output parameter:
        WRITES Sampler:
          Cardinality: ~10 (15 capacity)
          Top 10 partitions:
            Partition     Count       +/-
            9        11         0
            0         1         0
            1         1         0

        READS Sampler:
          Cardinality: ~10 (256 capacity)
          Top 3 partitions:
            Partition     Count       +/-
            0         3         0
            1         3         0
            2         3         0
            3         2         0

        return Dict:
        {
            'READS': {
                'toppartitions': '10',
                'partitions': OrderedDict('0': {'count': '1', 'margin': '0'},
                                          '1': {'count': '1', 'margin': '0'},
                                          '2': {'count': '1', 'margin': '0'}),
                'cardinality': '10',
                'capacity': '256',
            },
            'WRITES': {
                'toppartitions': '10',
                'partitions': OrderedDict('10': {'count': '1', 'margin': '0'},
                                          '11': {'count': '1', 'margin': '0'},
                                          '21': {'count': '1', 'margin': '0'}),
                'cardinality': '10',
                'capacity': '256',
                'sampler': 'WRITES'
            }
        }


        Arguments:
            output {str} -- stdout of nodetool topparitions command

        Returns:
            dict -- result of parsing
        """

        pattern1 = "(?P<sampler>[A-Z]+)\sSampler:\W+Cardinality:\s~(?P<cardinality>[0-9]+)\s\((?P<capacity>[0-9]+)\scapacity\)\W+Top\s(?P<toppartitions>[0-9]+)\spartitions:"
        pattern2 = "(?P<partition>[\w:]+)\s+(?P<count>[\d]+)\s+(?P<margin>[\d]+)"
        toppartitions = {}
        for out in output.split('\n\n'):
            partition = OrderedDict()
            sampler_data = re.match(pattern1, out, re.MULTILINE)
            sampler_data = sampler_data.groupdict()
            partitions = re.findall(pattern2, out, re.MULTILINE)
            for v in partitions:
                partition.update({v[0]: {'count': v[1], 'margin': v[2]}})
            sampler_data.update({'partitions': partition})
            toppartitions[sampler_data.pop('sampler')] = sampler_data
        return toppartitions

    def prepare_cluster_with_ks_cf_c1c2(self, ks, cf):
        self.cluster.populate([1]).start()
        node = self.cluster.nodelist()[0]
        session = self.patient_cql_connection(node)
        self.create_ks(session, ks, 1)
        create_c1c2_table(self, session, cf)
        return node, session

    def prepare_cluster_with_ks_cf_complex_primary_key(self, ks, cf):
        self.cluster.populate([1]).start()
        node = self.cluster.nodelist()[0]

        query = 'CREATE TABLE IF NOT EXISTS {ks}.{cf} (key1 int, key2 int, ckey int, val text, PRIMARY KEY ((key1, key2), ckey));'.format(ks=ks, cf=cf)
        session = self.patient_cql_connection(node)
        self.create_ks(session, ks, 1)
        session.execute(query)

        return node, session

    def run_toppartitions_with_wrong_parameters(self, node, ks='', cf='', duration='', optional_params=''):
        cmd = self.toppartitions_cmd_template.format(**locals())
        with self.assertRaises(NodetoolError) as cm:
            node.nodetool(cmd)
        return cm.exception

    def run_toppartition_for(self, node, ks, cf, duration=5000, optional_params=''):
        cmd = self.toppartitions_cmd_template.format(**locals())

        try:
            out, err = node.nodetool(cmd)
            if err:
                self.fail(err)
            return self._parse_toppartitions_output(out)

        except NodetoolError as details:
            self.fail(details)

    def run_operations_c1c2(self, session, mode="write", key=None, w_keys=1, r_keys=1, w_num=1, r_num=1,
                            ks='ks', cf='cf', delay=1, read_only=False):
        """Execute operations on cluster for table with c1c2 columns

        Execute operations on cluster in mode

        Arguments:
            session {cassandra.cluster.Session} -- [description]

        Keyword Arguments:
            mode {str} -- Define which operations should be run: Write, Read, Write and Read (default: {"write"})
            w_keys {number} -- number of keys to write (default: {1})
            r_keys {number} -- number of keys to read (default: {1})
            w_num {number} -- repeat write operations for keys w_num times (default: {1})
            r_num {number} -- repeat read operatiions for keys r_num times (default: {1})
            ks {str} -- name of keyspace (default: {'ks'})
            cf {str} -- name of columnfamily (default: {'cf'})
            delay {number} -- delay in seconds before start operation (default: {1})
        """
        if mode == "write":
            time.sleep(delay)
            for _ in range(w_num):
                if key:
                    insert_c1c2_no_prepared(session, keys=key, ks=ks, cf=cf)
                else:
                    insert_c1c2_no_prepared(session, n=w_keys, ks=ks, cf=cf)
        if mode == "read":
            if not read_only:
                insert_c1c2_no_prepared(session, key, n=w_keys, ks=ks, cf=cf)
            time.sleep(delay)
            for _ in range(r_num):
                if key:
                    query_c1c2(session, key, ks=ks, cf=cf)
                    continue
                for key in range(r_keys):
                    query_c1c2(session, key, ks=ks, cf=cf)

    def verify_thread_execution(self, th):
        exc = th.exception()
        if exc:
            raise exc

    def verify_error_message(self, details):
            error_msg = "nodetool: toppartitions requires keyspace, column family name, and duration"
            self.assertNotEqual(details.exit_status, 0)
            self.assertIn(error_msg, details.stdout)

    def verify_empty_result(self, out):
        self.assertFalse(out['WRITES']['partitions'])
        self.assertFalse(out['READS']['partitions'])

    def verifySamplesPresentInResult(self, samplers, result):
        self.assertListEqual(sorted(samplers), sorted(result.keys()))

    def verifyTopPartitionCounterForSample(self, actual_results, expected_results):
        self.assertEqual(len(actual_results["partitions"]), len(expected_results))
        for partition, counter in expected_results:
            self.assertIn(partition, actual_results['partitions'].keys())
            self.assertEqual(actual_results["partitions"][partition]['count'], counter)

    def verfityPartitionKeyInTopPartitionList(self, actual_partition_keys, expected_toppartition_keys):
        for actual_key in actual_partition_keys:
            self.assertIn(actual_key, expected_toppartition_keys)

    def test_help_description(self):
        """Check help subcommand output

        Check that nodetool help toppartitions is not empty
        """
        def get_help_toppartitions_description(node):
            try:
                return node.nodetool('help toppartitions')
            except NodetoolError as details:
                self.assertNotEqual(details.exit_status, 0)
                self.fail()

        def verify_help_output(out, err):
            if err:
                self.fail(err)
            msg = "nodetool toppartitions - Sample and print the most active partitions for"
            self.assertIn(msg, out)

        node = self.cluster.populate([1]).nodelist()[0]
        stdout, stderr = get_help_toppartitions_description(node)
        verify_help_output(stdout, stderr)

    def test_any_of_required_parameters_is_missing(self):
        """test required parameters

        If not keyspace, column family, duration provided,
        command terminated

        """
        node, session = self.prepare_cluster_with_ks_cf_c1c2(ks='keyspace1', cf='columnfamily1')
        # no requied parameters
        details = self.run_toppartitions_with_wrong_parameters(node)
        self.verify_error_message(details)
        # only ks required parameter is passed
        details = self.run_toppartitions_with_wrong_parameters(node, ks='keyspace1')
        self.verify_error_message(details)
        # duration required parameter is not passed
        details = self.run_toppartitions_with_wrong_parameters(node, ks='keyspace1', cf='columnfamily1')
        self.verify_error_message(details)
        # ks is not passed
        details = self.run_toppartitions_with_wrong_parameters(node, cf='columnfamily1', duration=500)
        self.verify_error_message(details)

    def test_for_empty_ks_cf(self):
        """Validate empty results for just created
        keyspace and columnfamily

        """
        node, session = self.prepare_cluster_with_ks_cf_c1c2(ks='keyspace1', cf='columnfamily1')

        stdout = self.run_toppartition_for(node, ks='keyspace1', cf='columnfamily1', duration=500)
        self.verify_empty_result(stdout)

    @require('scylladb/scylla-dtest#1090')
    def test_writes_reads_samples_for_1_partition_with_1_op(self):
        """Validate that only one 1 writen/read

        1. Create KS and CF
        2. start toppartions for 2 seconds
        3. write/read in thread to ks.cf one partitions
        3. assert results
        """
        node, session = self.prepare_cluster_with_ks_cf_c1c2(ks='ks', cf='cf')

        with ThreadPoolExecutor(max_workers=2) as executor:
            ft = executor.submit(self.run_toppartition_for, node, ks='ks', cf='cf')
            self.run_operations_c1c2(session, mode="write")
            self.run_operations_c1c2(session, mode="read", read_only=True)
            self.verify_thread_execution(ft)
            toppartion_results = ft.result()

        expected_toppartition_key_count = [("k0", "1")]

        self.verifySamplesPresentInResult(["WRITES", "READS"], toppartion_results)
        self.verifyTopPartitionCounterForSample(actual_results=toppartion_results['WRITES'],
                                                expected_results=expected_toppartition_key_count)
        self.verifyTopPartitionCounterForSample(actual_results=toppartion_results['READS'],
                                                expected_results=expected_toppartition_key_count)

    @require('scylladb/scylla-dtest#1090')
    def test_writes_sample_for_1_partition_with_1_op_and_empty_reads_sample(self):
        """ validate that for upon write operations,
        toppartitions results for read sampler is empty

        Flow:
        1. Create KS, CF
        2. Run toppartition with duration 2 seconds
        3. Execute one write operation
        4. Assert write sampler, empty read sampler
        """

        node, session = self.prepare_cluster_with_ks_cf_c1c2(ks='ks', cf='cf')

        with ThreadPoolExecutor(max_workers=1) as executor:
            ft = executor.submit(self.run_toppartition_for, node, ks='ks', cf='cf')
            self.run_operations_c1c2(session, mode="write")
            self.verify_thread_execution(ft)
            toppartion_results = ft.result()

        expected_write_toppartition_key_count = [("k0", "1")]
        self.verifySamplesPresentInResult(["WRITES", "READS"], toppartion_results)
        self.verifyTopPartitionCounterForSample(actual_results=toppartion_results['WRITES'],
                                                expected_results=expected_write_toppartition_key_count)
        self.verifyTopPartitionCounterForSample(actual_results=toppartion_results['READS'],
                                                expected_results=[])

    @require('scylladb/scylla-dtest#1090')
    def test_reads_sample_for_1_partition_with_1_op_and_empty_writes_sample(self):
        """ validate that for upon read operations,
        toppartitions results for read sampler is empty

        Flow:
        1. Create KS, CF
        2. Run toppartition with duration 2 seconds
        3. Execute one read operation
        4. Assert read sampler, empty write sampler
        """

        node, session = self.prepare_cluster_with_ks_cf_c1c2(ks='ks', cf='cf')
        with ThreadPoolExecutor(max_workers=1) as executor:
            ft = executor.submit(self.run_toppartition_for, node, ks='ks', cf='cf')
            self.run_operations_c1c2(session, mode="read")
            self.verify_thread_execution(ft)
            toppartion_results = ft.result()

        expected_read_toppartition_key_count = [("k0", "1")]
        self.verifySamplesPresentInResult(["WRITES", "READS"], toppartion_results)
        self.verifyTopPartitionCounterForSample(actual_results=toppartion_results['READS'],
                                                expected_results=expected_read_toppartition_key_count)
        self.verifyTopPartitionCounterForSample(actual_results=toppartion_results['WRITES'],
                                                expected_results=[])

    @require('scylladb/scylla-dtest#1090')
    def test_writes_sample_for_10_partitions_with_1_op_and_empty_reads_sample(self):
        """ validate that write operations is correctly counted
        for 10 partitions

        Flow:
        1. Create KS, CF
        2. Run toppartition with duration 3 seconds
        3. Execute 1 write operation for 10 partitions
        4. Assert write sampler, empty read sampler
        """
        node, session = self.prepare_cluster_with_ks_cf_c1c2(ks='ks', cf='cf')
        with ThreadPoolExecutor(max_workers=1) as executor:
            ft = executor.submit(self.run_toppartition_for, node, ks='ks', cf='cf')
            self.run_operations_c1c2(session, mode="write", w_keys=10)
            self.verify_thread_execution(ft)
            toppartion_results = ft.result()

        expected_write_toppartition_key_count = [("k{}".format(i), "1") for i in range(10)]
        self.verifySamplesPresentInResult(["WRITES", "READS"], toppartion_results)
        self.verifyTopPartitionCounterForSample(actual_results=toppartion_results["WRITES"],
                                                expected_results=expected_write_toppartition_key_count)
        self.verifyTopPartitionCounterForSample(actual_results=toppartion_results["READS"],
                                                expected_results=[])

    @require('scylladb/scylla-dtest#1090')
    def test_reads_sample_for_10_partitions_with_1_op_and_empty_writes_sample(self):
        """ validate that read operations is correctly counted
        for 10 partitions

        Flow:
        1. Create KS, CF
        2. Run toppartition with duration 3 seconds
        3. Execute 1 read operation for 10 partitions
        4. Assert read sampler, empty write sampler
        """
        node, session = self.prepare_cluster_with_ks_cf_c1c2(ks='ks', cf='cf')
        with ThreadPoolExecutor(max_workers=1) as executor:
            ft = executor.submit(self.run_toppartition_for, node, ks='ks', cf='cf')
            self.run_operations_c1c2(session, mode="read", r_keys=10, w_keys=10)

            self.verify_thread_execution(ft)
            toppartion_results = ft.result()

        expected_read_toppartitions_keys_count = [("k{}".format(i), "1") for i in range(10)]
        self.verifySamplesPresentInResult(["WRITES", "READS"], toppartion_results)
        self.verifyTopPartitionCounterForSample(actual_results=toppartion_results["READS"],
                                                expected_results=expected_read_toppartitions_keys_count)
        self.verifyTopPartitionCounterForSample(actual_results=toppartion_results["WRITES"],
                                                expected_results=[])

    @require('scylladb/scylla-dtest#1090')
    def test_writes_sample_for_1_partition_with_10_ops_and_empty_read_sample(self):
        """Validate count of operations correctly counted
        for 1 partition

        Flow:
        1. Create KS and CF
        2. run toppartitions for 3 seconds
        3. run in thread 10 write operation for 1 partition
        4. assert the write samplers, empty read samplers
        """
        node, session = self.prepare_cluster_with_ks_cf_c1c2(ks='ks', cf='cf')
        with ThreadPoolExecutor(max_workers=1) as executor:
            ft = executor.submit(self.run_toppartition_for, node, ks='ks', cf='cf')
            self.run_operations_c1c2(session, mode="write", w_num=10)

            self.verify_thread_execution(ft)
            toppartion_results = ft.result()

        expected_write_toppartition_key_count = [("k0", "10")]
        self.verifySamplesPresentInResult(["WRITES", "READS"], toppartion_results)
        self.verifyTopPartitionCounterForSample(actual_results=toppartion_results["WRITES"],
                                                expected_results=expected_write_toppartition_key_count)
        self.verifyTopPartitionCounterForSample(actual_results=toppartion_results["READS"],
                                                expected_results=[])

    @require('scylladb/scylla-dtest#1090')
    def test_reads_sample_for_1_partition_with_10_ops_and_empty_write_sample(self):
        """Validate count of operations correctly counted
        for 1 partition

        Flow:
        1. Create KS and CF
        2. run toppartitions for 3 seconds
        3. run in thread 10 read operation for 1 partition
        4. assert the read samplers, empty write samplers
        """
        node, session = self.prepare_cluster_with_ks_cf_c1c2(ks='ks', cf='cf')
        with ThreadPoolExecutor(max_workers=1) as executor:
            ft = executor.submit(self.run_toppartition_for, node, ks='ks', cf='cf')
            self.run_operations_c1c2(session, mode="read", r_num=10)

            self.verify_thread_execution(ft)
            toppartion_results = ft.result()

        expected_read_toppartition_key_count = [("k0", "10")]
        self.verifySamplesPresentInResult(["WRITES", "READS"], toppartion_results)
        self.verifyTopPartitionCounterForSample(actual_results=toppartion_results["READS"],
                                                expected_results=expected_read_toppartition_key_count)
        self.verifyTopPartitionCounterForSample(actual_results=toppartion_results["WRITES"],
                                                expected_results=[])

    @require('scylladb/scylla-dtest#1090')
    def test_top_5_paritions_write_with_1_op_and_empty_read_sample(self):
        """validate only top 5 partions displayed

        Flow
        1. Create KS and column family
        2. run toppartitions command for 3 seconds
        3. run in thread 1 write operations for 10 partitions
        4. assert that only latest 5 are displayed.

        #4529
        """
        node, session = self.prepare_cluster_with_ks_cf_c1c2(ks='ks', cf='cf')
        with ThreadPoolExecutor(max_workers=1) as executor:
            ft = executor.submit(self.run_toppartition_for, node, ks='ks', cf='cf', optional_params='-k 5')
            self.run_operations_c1c2(session, mode="write", w_keys=10)

            self.verify_thread_execution(ft)
            toppartion_results = ft.result()

        expected_write_toppartition_key_count = [("k{}".format(i), "1") for i in range(5)]

        self.verifySamplesPresentInResult(["WRITES", "READS"], toppartion_results)
        self.verifyTopPartitionCounterForSample(actual_results=toppartion_results["WRITES"],
                                                expected_results=expected_write_toppartition_key_count)
        self.verifyTopPartitionCounterForSample(actual_results=toppartion_results["READS"],
                                                expected_results=[])

    @require('scylladb/scylla-dtest#1090')
    def test_top_5_paritions_read_with_1_op_and_empty_write_sample(self):
        """validate only top 5 partions displayed

        Flow
        1. Create KS and column family
        2. run toppartitions command for 3 seconds
        3. run in thread 1 read operations for 10 partitions
        4. assert that only latest 5 are displayed.

        #4529
        """
        node, session = self.prepare_cluster_with_ks_cf_c1c2(ks='ks', cf='cf')
        with ThreadPoolExecutor(max_workers=1) as executor:
            ft = executor.submit(self.run_toppartition_for, node, ks='ks', cf='cf', optional_params='-k 5')
            self.run_operations_c1c2(session, mode="read", r_keys=10, w_keys=10)

            self.verify_thread_execution(ft)
            toppartion_results = ft.result()

        expected_read_toppartition_key_count = [("k{}".format(i), "1") for i in range(5)]
        self.verifySamplesPresentInResult(["WRITES", "READS"], toppartion_results)
        self.verifyTopPartitionCounterForSample(actual_results=toppartion_results["READS"],
                                                expected_results=expected_read_toppartition_key_count)
        self.verifyTopPartitionCounterForSample(actual_results=toppartion_results["WRITES"],
                                                expected_results=[])

    def test_top_3_paritions_for_write_samplers_only(self):
        """validate only top 3 partions are displayed

        Flow
        1. Create KS and column family
        2. run toppartitions command for 3 seconds
        3. run in thread with write operations for 10 partitions
        4. assert that only latest 3 are displayed.

        """
        node, session = self.prepare_cluster_with_ks_cf_c1c2(ks='ks', cf='cf')
        futures = []
        range_of_write_keys = [15, 5, 3]  # write to key ranges k0-k14, k0-k4, k0-k2
        with ThreadPoolExecutor(max_workers=4) as executor:
            ft_top = executor.submit(self.run_toppartition_for, node, ks='ks', cf='cf', optional_params='-k 3 -a writes')
            futures.append(ft_top)
            time.sleep(1)
            for keys in range_of_write_keys:
                ft = executor.submit(self.run_operations_c1c2, session, mode="write", w_keys=keys)
                futures.append(ft)

            for ft in futures:
                self.verify_thread_execution(ft)
            toppartion_results = ft_top.result()

        expected_write_toppartition_key_count = [("k{}".format(i), "3") for i in range(3)]
        self.verifySamplesPresentInResult(["WRITES"], toppartion_results)
        self.verifyTopPartitionCounterForSample(actual_results=toppartion_results["WRITES"],
                                                expected_results=expected_write_toppartition_key_count)

    def test_top_3_paritions_for_read_samplers_only(self):
        """validate only top 3 partions displayed

        Flow
        1. Create KS and column family
        2. run toppartitions command for 3 seconds
        3. run in thread with read operations for 10 partitions
        4. assert that only latest 3 are displayed.

        #4529
        """
        node, session = self.prepare_cluster_with_ks_cf_c1c2(ks='ks', cf='cf')
        futures = []
        range_of_read_keys = [5, 3]
        with ThreadPoolExecutor(max_workers=4) as executor:
            ft_top = executor.submit(self.run_toppartition_for, node, ks='ks', cf='cf', optional_params='-k 3 -a reads')
            futures.append(ft_top)
            time.sleep(1)
            ft = executor.submit(self.run_operations_c1c2, session, mode="read", w_keys=15, r_keys=15)
            futures.append(ft)
            for keys in range_of_read_keys:
                ft = executor.submit(self.run_operations_c1c2, session, mode="read", r_keys=keys, read_only=True)
                futures.append(ft)

            for ft in futures:
                self.verify_thread_execution(ft)

            toppartion_results = ft_top.result()

        expected_read_toppartition_key_count = [("k{}".format(i), "3") for i in range(3)]
        self.verifySamplesPresentInResult(["READS"], toppartion_results)
        self.verifyTopPartitionCounterForSample(actual_results=toppartion_results["READS"],
                                                expected_results=expected_read_toppartition_key_count)

    @require('scylladb/scylla-dtest#1090')
    def test_param_sampler_writes_and_capacity_size(self):
        """validate result include only write samples with
        capacity size equal to parameter

        Flow
        1. Create KS and column family
        2. run toppartitions command for 3 seconds
        3. run in thread 1 write operations for 10 partitions
        4. assert only write samplers in output
        """
        node, session = self.prepare_cluster_with_ks_cf_c1c2(ks='ks', cf='cf')
        futures = []
        top_write_keys = [{"key": 9, "write_num": 10}, {"key": 15, "write_num": 15}]
        with ThreadPoolExecutor(max_workers=4) as executor:
            ft_top = executor.submit(self.run_toppartition_for, node, ks='ks', cf='cf', optional_params='-a writes -s 15')
            futures.append(ft_top)
            time.sleep(1)
            ft = executor.submit(self.run_operations_c1c2, session, mode="write", w_keys=20)
            futures.append(ft)
            for top_write in top_write_keys:
                ft = executor.submit(self.run_operations_c1c2, session,
                                     mode="write",
                                     key=[top_write["key"]],
                                     w_num=top_write["write_num"])
                futures.append(ft)

            for ft in futures:
                self.verify_thread_execution(ft)
            toppartion_results = ft_top.result()

        expected_write_toppartition_key_count = [("k15", '16'), ("k9", "11")]
        for i in range(8):
            expected_write_toppartition_key_count.append(('k{}'.format(i), "1"))

        self.verifySamplesPresentInResult(["WRITES"], toppartion_results)
        self.verifyTopPartitionCounterForSample(actual_results=toppartion_results["WRITES"],
                                                expected_results=expected_write_toppartition_key_count)

    @require('scylladb/scylla-dtest#1090')
    def test_param_sampler_read_and_capacity_size(self):
        """validate result include only read samples with
        capacity size equal to parameter

        Flow
        1. Create KS and column family
        2. run toppartitions command for 3 seconds
        3. run in thread read operations for 10 partitions
        4. assert only read samplers in output
        """
        node, session = self.prepare_cluster_with_ks_cf_c1c2(ks='ks', cf='cf')
        futures = []
        top_read_keys = [{"key": 19, "read_num": 20}, {"key": 9, "read_num": 10}]
        with ThreadPoolExecutor(max_workers=4) as executor:
            ft_top = executor.submit(self.run_toppartition_for, node, ks='ks', cf='cf', optional_params='-a reads -s 12')
            futures.append(ft_top)
            time.sleep(1)
            ft = executor.submit(self.run_operations_c1c2, session, mode="read", w_keys=20, r_keys=20)
            futures.append(ft)
            for top_read in top_read_keys:
                ft = executor.submit(self.run_operations_c1c2, session,
                                     mode="read",
                                     key=top_read["key"],
                                     r_num=top_read["read_num"],
                                     read_only=True)
                futures.append(ft)
            for ft in futures:
                self.verify_thread_execution(ft)
            toppartion_results = ft_top.result()

        expected_write_toppartition_key_count = [("k19", '21'), ("k9", "11")]
        for i in range(8):
            expected_write_toppartition_key_count.append(('k{}'.format(i), "1"))

        self.verifySamplesPresentInResult(["READS"], toppartion_results)
        self.verifyTopPartitionCounterForSample(toppartion_results["READS"],
                                                expected_write_toppartition_key_count)

    def test_write_by_gause_key_distribution(self):
        """Validate that top write partitions are correlate to gaus distribution

        Stress command will populate table on write operations for keys distributed by gaus
        The custom profile file is used test_data/c-s-profiles/cs_normal_distribution.yaml
        The partition key will be populated with guas distribution : gaussian(1..3000,1500).
        This means that toppartitions for write operation should be: 1500, 1501, 1499, 1498, 1502

        the stress command will be running  for 30 seconds. toppartition command will be executed several
        times, each time result for top partition should be the same.
        """
        self.cluster.populate([1]).start()
        node = self.cluster.nodelist()[0]

        top_5_write_partitions_keys_results = []
        with ThreadPoolExecutor(max_workers=1) as excutor:
            stress_cmd = "user profile=test_data/c-s-profiles/cs_normal_distribution.yaml duration=30s ops(insert=1) no-warmup -port jmx=6868 -mode cql3 native -rate threads=1"
            future = excutor.submit(self.cluster.stress, stress_cmd.split(" "))
            time.sleep(5)
            # result 1 for compare
            for _ in range(3):
                toppartition_result = self.run_toppartition_for(node, ks='keyspace1', cf='standard1',
                                                                duration=2000, optional_params='-k 5')
                top_5_write_partitions_keys_results.append(toppartition_result['WRITES']['partitions'].keys())
                time.sleep(10)
            self.verify_thread_execution(future)

        expected_average_top_partition_keys = ['1500', '1501', '1499', '1502', '1498']

        for actual_results in top_5_write_partitions_keys_results:
            self.verfityPartitionKeyInTopPartitionList(actual_partition_keys=actual_results,
                                                       expected_toppartition_keys=expected_average_top_partition_keys)

    def test_read_by_gause_key_distribution(self):
        """Validate that top read partitions are correlate to gaus distribution

        Stress command will populate table on read operations for keys distributed by gaus
        The custom profile file is used test_data/c-s-profiles/cs_normal_distribution.yaml
        The partition key will be populated with guas distribution : gaussian(1..3000,1500).
        This means that toppartitions for read operation should be: 1500, 1501, 1499, 1498, 1502

        the stress command will be running  for 30 seconds. toppartition command will be executed several
        times, each time result for top partition should be the same.
        """
        self.cluster.populate([1]).start()
        node = self.cluster.nodelist()[0]

        top_5_read_partitions_keys_results = []
        with ThreadPoolExecutor(max_workers=1) as excutor:
            # prepare the keyspace and tables, and write date to db
            stress_cmd = "user profile=test_data/c-s-profiles/cs_normal_distribution.yaml duration=10s ops(insert=1) no-warmup -port jmx=6868 -mode cql3 native -rate threads=1"
            future = excutor.submit(self.cluster.stress, stress_cmd.split(" "))
            self.verify_thread_execution(future)
            # start read queries
            stress_cmd = "user profile=test_data/c-s-profiles/cs_normal_distribution.yaml duration=30s ops(single=1) no-warmup -port jmx=6868 -mode cql3 native -rate threads=1"
            future = excutor.submit(self.cluster.stress, stress_cmd.split(" "))
            time.sleep(5)
            # result 1 for compare
            for _ in range(3):
                toppartition_result = self.run_toppartition_for(node, ks='keyspace1', cf='standard1',
                                                                duration=2000, optional_params='-k 5')
                top_5_read_partitions_keys_results.append(toppartition_result['READS']['partitions'].keys())
                time.sleep(10)
            self.verify_thread_execution(future)

        expected_average_top_partition_keys = ['1500', '1501', '1499', '1502', '1498']

        for actual_results in top_5_read_partitions_keys_results:
            self.verfityPartitionKeyInTopPartitionList(actual_partition_keys=actual_results,
                                                       expected_toppartition_keys=expected_average_top_partition_keys)

    def test_topCount_shouldbe_smaller_than_capacity(self):
        node, session = self.prepare_cluster_with_ks_cf_c1c2(ks='keyspace1', cf='columnfamily1')

        result = self.run_toppartitions_with_wrong_parameters(node, ks='keyspace1', cf='columnfamily1', duration=3000, optional_params='-k 15 -s 12')

        err_msg = "TopK count (-k) option must be smaller then the summary capacity (-s)"
        self.assertIn(err_msg, result.stdout)

    def test_write_into_one_paritions_to_different_rows(self):
        """
        """
        def write_1_op_for_10_partitions(session):
            time.sleep(1)
            for i in range(10):
                session.execute('INSERT INTO keyspace1.columnfamily1 (key1, key2, ckey, val) VALUES (1, {0}, {0}, \'asdf\');'.format(i))

        def write_into_one_partition_to_different_rows(session):
            time.sleep(1)
            for i in range(10):
                session.execute('INSERT INTO keyspace1.columnfamily1 (key1, key2, ckey, val) VALUES (1, 1, {0}, \'asdf{0}\');'.format(i))

        node, session = self.prepare_cluster_with_ks_cf_complex_primary_key(ks='keyspace1', cf='columnfamily1')
        futures = []
        with ThreadPoolExecutor(max_workers=3) as executor:
            ft_top = executor.submit(self.run_toppartition_for, node, ks='keyspace1', cf='columnfamily1')
            futures.append(ft_top)
            time.sleep(1)
            ft1 = executor.submit(write_1_op_for_10_partitions, session)
            futures.append(ft1)
            ft2 = executor.submit(write_into_one_partition_to_different_rows, session)
            futures.append(ft2)
            for ft in futures:
                self.verify_thread_execution(ft)
            toppartition_result = ft_top.result()

        expected_write_toppartition_results = [("1:1", "11"), ("1:0", "1")]
        for i in range(2, 10):
            expected_write_toppartition_results.append(("1:{}".format(i), "1"))

        self.verifySamplesPresentInResult(["WRITES", "READS"], toppartition_result)
        self.verifyTopPartitionCounterForSample(actual_results=toppartition_result["WRITES"],
                                                expected_results=expected_write_toppartition_results)
        self.verifyTopPartitionCounterForSample(actual_results=toppartition_result["READS"],
                                                expected_results=[])

    def test_write_to_parition_with_1000_rows_during_writing_partitions_with_10_rows(self):
        """Verify the most host partition with larger number of written rows

        validate, that writing large number of rows(100) for on partition during writing to
        paritions with less number of rows(10), the top (hotest) partition is partition with
        large number of rows.

        Validation happened on long period = 10sec
        """

        def write_into_1000_partitions_to_10_rows(session):
            time.sleep(1)
            for i in range(1000):
                for j in range(10):
                    session.execute('INSERT INTO keyspace1.columnfamily1 (key1, key2, ckey, val) VALUES (1, {0}, {1}, \'test\');'.format(i, j))

        def write_into_one_partition_to_1000_rows(session):
            time.sleep(1)
            for i in range(1000):
                session.execute('INSERT INTO keyspace1.columnfamily1 (key1, key2, ckey, val) VALUES (1, 50, {0}, \'test{0}\');'.format(i))

        node, session = self.prepare_cluster_with_ks_cf_complex_primary_key(ks='keyspace1', cf='columnfamily1')
        futures = []
        with ThreadPoolExecutor(max_workers=3) as executor:
            ft_top = executor.submit(self.run_toppartition_for, node, ks='keyspace1', cf='columnfamily1', duration=10000, optional_params='-k 1')
            futures.append(ft_top)
            time.sleep(1)
            ft1 = executor.submit(write_into_1000_partitions_to_10_rows, session)
            futures.append(ft1)
            ft2 = executor.submit(write_into_one_partition_to_1000_rows, session)
            futures.append(ft2)
            for ft in futures:
                self.verify_thread_execution(ft)
            toppartition_result = ft_top.result()

        expected_write_toppartition = [("1:50", "1010")]

        self.verifySamplesPresentInResult(["WRITES", "READS"], toppartition_result)
        self.verifyTopPartitionCounterForSample(actual_results=toppartition_result["WRITES"],
                                                expected_results=expected_write_toppartition)
        self.verifyTopPartitionCounterForSample(actual_results=toppartition_result["READS"],
                                                expected_results=[])

    @require('scylladb/scylla-dtest#1090')  # Note: could be flaky for other reasons as well, please update if needed
    def test_write_by_gause_key_distribution_for_compound_primary_key_and_large_rows_number(self):
        """validate that top write partitions with compound partition key (p_key, p_key1) and large number of rows
        per partition are correlate to guas key disribution

        The column family has next configuration:
            CREATE TABLE standard1 (
                p_key bigint,
                p_key1 bigint,
                cl_key bigint,
                c1 text,
                PRIMARY KEY((p_key,p_key1),cl_key)
            );

        The write operation has gaus key distribution for each field of partition key:
              - name: p_key
                population: gaussian(1..3000,1500)
              - name: p_key1
                population: gaussian(1..3000,1500)

        this assumes that top write parition should be around next keys:
        1500:1500, 1500:1501, 1500:1499, 1499:1500 and simalar.

        Using profile file for c-s tool run write operation and validate the nodetool toppartition result
        """

        self.cluster.populate([1]).start()
        node = self.cluster.nodelist()[0]

        top_5_write_partitions_keys_results = []
        with ThreadPoolExecutor(max_workers=1) as excutor:
            stress_cmd = "user profile=test_data/c-s-profiles/cs_large_numbers_rows_per_partition.yaml duration=60s ops(insert=1) no-warmup -port jmx=6868 -mode cql3 native -rate threads=1"
            future = excutor.submit(self.cluster.stress, stress_cmd.split(" "))
            time.sleep(5)

            for _ in range(3):
                time.sleep(15)
                toppartition_result = self.run_toppartition_for(node, ks='keyspace1', cf='standard1',
                                                                duration=2000, optional_params='-k 5')

                top_5_write_partitions_keys_results.append(toppartition_result['WRITES']['partitions'].keys())

            self.verify_thread_execution(future)

        expected_average_top_partition_keys = ['1500:1501', '1500:1500', '1500:1499', '1500:1502', '1500:1498',
                                               '1501:1500', '1501:1499', '1501:1501', '1501:1498', '1501:1502',
                                               '1499:1500', '1499:1499', '1499:1501', '1499:1498', '1499:1502',
                                               '1498:1500', '1498:1499', '1498:1501', '1498:1498', '1498:1502']

        for actual_results in top_5_write_partitions_keys_results:
            self.verfityPartitionKeyInTopPartitionList(actual_partition_keys=actual_results,
                                                       expected_toppartition_keys=expected_average_top_partition_keys)

    def test_read_by_gause_key_distribution_for_compound_primary_key_and_large_rows_number(self):
        """validate that top read partitions with compound partition key (p_key, p_key1) and large number of rows
        per partition are correlate to guas key disribution

        The column family has next configuration:
            CREATE TABLE standard1 (
                p_key bigint,
                p_key1 bigint,
                cl_key bigint,
                c1 text,
                PRIMARY KEY((p_key,p_key1),cl_key)
            );

        The read operation has gaus key distribution for each field of partition key:
              - name: p_key
                population: gaussian(1..3000,1500)
              - name: p_key1
                population: gaussian(1..3000,1500)
            and next read queries:


        this assumes that top read parition should be around next keys:
        1500:1500, 1500:1501, 1500:1499, 1499:1500 and simalar.

        Using profile file for c-s tool run read operation and validate the nodetool toppartition result
        """
        self.cluster.populate([1]).start()
        node = self.cluster.nodelist()[0]

        top_5_read_partitions_keys_results = []
        with ThreadPoolExecutor(max_workers=1) as excutor:
            # prepare keyspace and columnfamily
            stress_cmd = "user profile=test_data/c-s-profiles/cs_large_numbers_rows_per_partition.yaml duration=15s ops(insert=1) no-warmup -port jmx=6868 -mode cql3 native -rate threads=1"
            future = excutor.submit(self.cluster.stress, stress_cmd.split(" "))
            self.verify_thread_execution(future)

            stress_cmd = "user profile=test_data/c-s-profiles/cs_large_numbers_rows_per_partition.yaml duration=60s ops(multi_row=2) no-warmup -port jmx=6868 -mode cql3 native -rate threads=2"
            future = excutor.submit(self.cluster.stress, stress_cmd.split(" "))
            time.sleep(5)

            for _ in range(3):
                time.sleep(15)
                toppartition_result = self.run_toppartition_for(node, ks='keyspace1', cf='standard1',
                                                                duration=2000, optional_params='-k 5')

                top_5_read_partitions_keys_results.append(toppartition_result['READS']['partitions'].keys())

            self.verify_thread_execution(future)

        expected_average_top_partition_keys = ['1500:1501', '1500:1500', '1500:1499', '1500:1502', '1500:1498',
                                               '1501:1500', '1501:1499', '1501:1501', '1501:1498', '1501:1502',
                                               '1499:1500', '1499:1499', '1499:1501', '1499:1498', '1499:1502',
                                               '1498:1500', '1498:1499', '1498:1501', '1498:1498', '1498:1502']

        for actual_results in top_5_read_partitions_keys_results:
            self.verfityPartitionKeyInTopPartitionList(actual_partition_keys=actual_results,
                                                       expected_toppartition_keys=expected_average_top_partition_keys)
