import logging
import random
from pprint import pformat

import pytest
from deepdiff import DeepDiff

from alternator.utils import enums
from alternator_utils import BaseAlternatorStream, NUM_OF_ITEMS, TABLE_NAME, StreamsTable
from tools.retrying import retrying
from tools.cluster import new_node


logger = logging.getLogger(__name__)


# @pytest.mark.dtest_full
class TestAlternatorStreams(BaseAlternatorStream):

    @pytest.mark.next_gating
    def test_verify_all_nodes_have_same_stream(self):
        num_of_items = NUM_OF_ITEMS
        self.prepare_dynamodb_cluster(num_of_nodes=3)
        node1 = self.cluster.nodelist()[0]
        stream_arn = self.prefill_dynamodb_table(
            node=node1, stream_specification=enums.StreamSpecification.KEYS_ONLY.value, num_of_items=num_of_items)[0]
        expected_items = [{self._table_primary_key: item[self._table_primary_key]} for item in self.create_items()]

        for node in self.cluster.nodelist():
            responses = self.get_responses(node=node, stream_arn=stream_arn, num_of_requests=2 * num_of_items)
            records = self.extract_data_from_responses(responses)
            diff = self.compare_table_keys_only_data(expected_table_data=expected_items, table_data=records)
            assert not diff, f"The following keys are missing '{pformat(diff)}'"

    def test_verify_stream_records_after_topology_changed(self):
        """
        The tests verify the data after topology changes - Stream during maintenance operations that alter topology
         (ex. Decommission/stop/delete/add node).
        Create a test that checks there are no new events after reading multiple records from different nodes.
        """
        def _add_new_nodes(nodes_size):
            new_nodes = []
            for node_idx in range(cluster_size + 1, cluster_size + 1 + nodes_size):
                logger.info(f'Adding new node{node_idx} to cluster')
                node = new_node(self.cluster, bootstrap=True)
                node.start(wait_for_binary_proto=True, wait_other_notice=True)
                self.wait_for_alternator(node=node)
                logger.info(f'The node{node_idx} was successfully added')
                new_nodes.append(node)
            return new_nodes

        def _verify_items(_node, _expected_table_data, _num_of_requests):
            logger.info(f'Verifying the new items exists in "{_node.name}" node')
            responses = self.get_responses(node=_node, stream_arn=stream_arn, num_of_requests=_num_of_requests)
            records = self.extract_data_from_responses(responses)
            diff = self.compare_table_keys_only_data(expected_table_data=_expected_table_data, table_data=records)
            assert not diff, f"The following keys are missing '{pformat(diff)}'"

        stream_specification = enums.StreamSpecification.KEYS_ONLY.value
        table_name = TABLE_NAME
        cluster_size = 3
        self.prepare_dynamodb_cluster(num_of_nodes=cluster_size)
        node1, node2, node3 = self.cluster.nodelist()

        logger.info(f'Pre setup - Creating "{table_name}" table via "{node1.name}" node with "{stream_specification}"'
                    f' stream key')
        self.create_table(node=node1, table_name=table_name, stream_specification=stream_specification)
        items = self.create_items(num_of_items=400)
        logger.info(f'Waiting until stream of "{table_name}" table be active')
        stream_arn = self.wait_for_active_stream(node=node1, table_name=table_name)[0]

        node4, node5 = _add_new_nodes(nodes_size=2)
        selected_node = node1
        expected_table_data = new_items = items[:100]
        logger.info(f'Step 1 - Adding "{len(new_items)}" new items from "{node1.name}" node')
        self.batch_write_actions(table_name=table_name, node=node1, new_items=new_items)
        _verify_items(_node=selected_node, _expected_table_data=expected_table_data,
                      _num_of_requests=2 * len(new_items))

        new_items = items[100:200]
        expected_table_data.extend(new_items)
        logger.info(f'Step 2 - Adding "{len(new_items)}" new items from "{node2.name}" node')
        self.batch_write_actions(table_name=table_name, node=node2, new_items=new_items)
        logger.info(f'Decommission the "{selected_node.name}" node')
        selected_node.decommission()
        _verify_items(_node=node4, _expected_table_data=expected_table_data, _num_of_requests=2 * len(new_items))

        new_items = items[200:300]
        expected_table_data.extend(new_items)
        logger.info(f'Step 3 - Adding "{len(new_items)}" new items from "{node3.name}" node')
        self.batch_write_actions(table_name=table_name, node=node3, new_items=new_items)
        logger.info(f'Stopping the "{node4.name}" node')
        node4.stop(wait_other_notice=True)
        logger.info(f'Starting the "{node4.name}" node')
        node4.start(wait_for_binary_proto=True, wait_other_notice=True)
        self.wait_for_alternator(node=node4)
        _verify_items(_node=node3, _expected_table_data=expected_table_data, _num_of_requests=2 * len(new_items))

        new_items = items[300:400]
        expected_table_data.extend(new_items)
        logger.info(f'Step 4 - Adding 100 new items from "{node4.name}" node')
        self.batch_write_actions(table_name=table_name, node=node4, new_items=new_items)
        logger.info(f'Deleting the "{node4.name}" node')
        self.cluster.remove(node4, wait_other_notice=True)
        _verify_items(_node=node5, _expected_table_data=expected_table_data, _num_of_requests=2 * len(new_items))

    @pytest.mark.next_gating
    def test_list_streams_limit_parameter(self):
        """
        Test the list_streams command limit parameter.
        See that when the response is large enough, it can be paged
        correctly according the 'limit' value.
        Test steps:
        1. create and enable streams for 20 tables.
        2. read variable size chunks of these tables streams in list_streams command via random node.
        3. where using and verifying different 'limit' values + its total output.
        """
        stream_specification = enums.StreamSpecification.KEYS_ONLY.value
        self.prepare_dynamodb_cluster(num_of_nodes=3)
        node1 = self.cluster.nodelist()[0]
        table_names = [TABLE_NAME+str(idx) for idx in range(20)]
        for table_name in table_names:
            self.create_table(node=node1, table_name=table_name, stream_specification=stream_specification)
        tables_arns = [self.wait_for_active_stream(node=node1, table_name=table_name)[0] for table_name in table_names]
        total_limited_stream_responses = []
        last_evaluated_stream_arn = None
        for limit in [5, 7, 3, 1, 4]:  # slice the created 20 tables to various size chunks
            params = {'Limit': limit}
            if last_evaluated_stream_arn:
                params['ExclusiveStartStreamArn'] = last_evaluated_stream_arn
            dynamodb_api = self.get_dynamodb_api(node=random.choice(self.cluster.nodelist()))
            result = dynamodb_api.stream.list_streams(**params)
            assert len(result['Streams']) == limit, \
                f"Got unexpected number of streams [{len(result['Streams'])}] for [{limit}] requested!"
            total_limited_stream_responses += result['Streams']
            last_evaluated_stream_arn = result['LastEvaluatedStreamArn']
        assert sorted(tables_arns) == sorted([stream['StreamArn'] for stream in total_limited_stream_responses]), \
            f"Got unexpected ARN values by list streams paged responses: {total_limited_stream_responses}"
        empty_streams_list = dynamodb_api.stream.list_streams(ExclusiveStartStreamArn=last_evaluated_stream_arn)[
            'Streams']
        assert len(empty_streams_list) == 0, \
            f"Got unexpected list of Streams after the last evaluated Stream: {empty_streams_list}"

    @pytest.mark.next_gating
    def test_updated_shards_during_add_decommission_node(self):
        """
        The tests verify Streams handles topology changes of decommission a node and adding new node.
        verifies that open-shards always exist. and verifies the open-shards are changed during topology changes.
        """

        stream_specification = enums.StreamSpecification.KEYS_ONLY.value
        self.prepare_dynamodb_cluster(num_of_nodes=4)
        node1 = self.cluster.nodelist()[0]

        logger.info(f'Pre setup - Creating "{TABLE_NAME}" table via "{node1.name}" node with "{stream_specification}"'
                    f' stream key')
        stream_arn = self.prefill_dynamodb_table(node=node1, stream_specification=stream_specification)[0]
        self.run_write_stress(table_name=TABLE_NAME, node=node1, num_of_item=1000, ignore_errors=True)
        decommission_thread = self.run_decommission_add_node_thread()
        wait_for_running_decommission(node=node1)
        streams_table = StreamsTable(stream_arn=stream_arn, dynamodb_api=self.get_dynamodb_api(node=node1))

        @retrying(num_attempts=7, sleep_time=1, allowed_exceptions=NoOpenShardsDiffError)
        def wait_for_open_shards_diff():
            current_open_shards = [shard for shard in streams_table.shards if streams_table.is_shard_open(shard)]
            wait_for_running_add_node(node=node1)
            wait_for_running_decommission(node=node1)
            streams_table.update_shards()
            assert streams_table.count_open_shards(), "No open shards found"
            new_open_shards = [shard for shard in streams_table.shards if streams_table.is_shard_open(shard)]
            diff = DeepDiff(current_open_shards, new_open_shards, ignore_order=True,
                            ignore_numeric_type_changes=True)
            if not diff:
                raise NoOpenShardsDiffError

        wait_for_open_shards_diff()
        decommission_thread.join()

    @pytest.mark.next_gating
    def test_sequence_numbers_during_add_decommission_node(self):
        """
        Verify shards sequence numbers on topology changes.
        1) calculate monotonic increasing sequence numbers comparing the StartingSequenceNumber of old and new shards.
        2) calculate monotonic increasing sequence numbers comparing the EndingSequenceNumber of old shards
           to EndingSequenceNumber of new shards.
        """

        stream_specification = enums.StreamSpecification.KEYS_ONLY.value
        self.prepare_dynamodb_cluster(num_of_nodes=4)
        node1 = self.cluster.nodelist()[0]

        logger.info(f'Pre setup - Creating "{TABLE_NAME}" table via "{node1.name}" node with "{stream_specification}"'
                    f' stream key')
        stream_arn = self.prefill_dynamodb_table(node=node1, stream_specification=stream_specification)[0]
        self.run_write_stress(table_name=TABLE_NAME, node=node1, num_of_item=1000, ignore_errors=True)
        decommission_thread = self.run_decommission_add_node_thread()
        streams_table = StreamsTable(stream_arn=stream_arn, dynamodb_api=self.get_dynamodb_api(node=node1))

        for cycle in range(3):
            logger.info(f'Starting cycle #{cycle+1}..')
            # Get original shards metadata
            original_start_sequence_numbers_set = streams_table.start_sequence_numbers_set
            original_start_sequence_numbers = streams_table.start_sequence_numbers_list
            max_original_start_sequence_number = max(
                original_start_sequence_numbers) if original_start_sequence_numbers else -1

            # Update table shards after topology change
            wait_for_running_add_node(node=node1)
            wait_for_running_decommission(node=node1)
            streams_table.update_shards()

            # Get updated shards metadata
            new_start_sequence_numbers = [seq_num for seq_num in streams_table.start_sequence_numbers_list if
                                          seq_num not in original_start_sequence_numbers]
            assert (streams_table.start_sequence_numbers_set - original_start_sequence_numbers_set), \
                "Start-sequence-numbers are not changed after topology changes!"

            # Verify new CDC/Streams Generation attribute of monotonic increasing sequence numbers.
            if new_start_sequence_numbers:
                min_new_start_sequence_numbers = min(new_start_sequence_numbers)
                assert min_new_start_sequence_numbers > max_original_start_sequence_number, \
                    "New Start sequence number is not greater than previous level one"

            # Verify EndingSequenceNumber is greater than StartingSequenceNumber
            closed_shards = [shard for shard in streams_table.shards if not streams_table.is_shard_open(shard)]
            for shard in closed_shards:
                assert int(shard['SequenceNumberRange']['EndingSequenceNumber']) > int(shard['SequenceNumberRange'][
                    'StartingSequenceNumber']),\
                    "EndingSequenceNumber is not greater than StartingSequenceNumber"

        decommission_thread.join()

    @pytest.mark.next_gating
    def test_added_node_gets_closed_shards(self):
        """
        test scenario:
            1. create a table with Streams.
            2. run alternator stress. (or multiple stresses to multiple nodes needed?)
            3. add new node to cluster.
            4. wait for new node bootstrap.
            5. run streams APIs queries, connecting to the new node.
            6. see that it returns some 'closed' shards with EndingSequenceNumber.
            https://github.com/scylladb/scylla/pull/8209#issuecomment-790625323
        """
        stream_specification = enums.StreamSpecification.KEYS_ONLY.value
        self.prepare_dynamodb_cluster(num_of_nodes=3)
        node1 = self.cluster.nodelist()[0]

        logger.info(f'Pre setup - Creating "{TABLE_NAME}" table via "{node1.name}" node with "{stream_specification}"'
                    f' stream key')
        stream_arn = self.prefill_dynamodb_table(node=node1, stream_specification=stream_specification)[0]
        stress_thread = self.run_write_stress(table_name=TABLE_NAME, node=node1, num_of_item=1000, ignore_errors=True)
        logger.info(f'Adding new node to cluster')
        node4 = new_node(self.cluster, bootstrap=True)
        node4.start(wait_for_binary_proto=True, wait_other_notice=True)
        self.wait_for_alternator(node=node4)
        logger.info(f'{node4.name} was successfully added')
        streams_table = StreamsTable(stream_arn=stream_arn, dynamodb_api=self.get_dynamodb_api(node=node4))
        closed_shards = [shard for shard in streams_table.shards if not streams_table.is_shard_open(shard)]
        assert closed_shards, f"New node {node4.name} has no closed shards"
        stress_thread.join()


@retrying(num_attempts=60, sleep_time=10, allowed_exceptions=AssertionError)
def wait_for_running_decommission(node):
    out, err = node.nodetool('status', capture_output=True)
    logger.debug(f"nodetool status is: {out}")
    assert 'UL' in out, \
        f"No Leaving node found, decommission is not running"


@retrying(num_attempts=60, sleep_time=10, allowed_exceptions=AssertionError)
def wait_for_running_add_node(node):
    out, err = node.nodetool('status', capture_output=True)
    logger.debug(f"nodetool status is: {out}")
    assert 'UJ' in out, \
        f"No Joining node found, add-node is not running"


class NoOpenShardsDiffError(Exception):
    pass
