import os
import random
import shutil
import tempfile
from decimal import Decimal
from botocore.exceptions import ClientError, EndpointConnectionError
from deepdiff import DeepDiff
from nose.plugins.attrib import attr
from pprint import pformat

from alternator.utils.data_generator import AlternatorDataGenerator, TypeMode
from alternator_utils import TesterAlternator, ALTERNATOR_SNAPSHOT_FOLDER, TABLE_NAME, NUM_OF_ITEMS, random_string, \
    CONDITION_EXPRESSION_SCHEMA, DEFAULT_STRING_LENGTH, NUM_OF_NODES
from alternator_utils import generate_put_request_items, Gsi, full_query
from dtest import debug
from tools import new_node


@attr('dtest-full')
class AlternatorTest(TesterAlternator):

    def test_load_older_snapshot_and_refresh(self):
        """
        The test loading older snapshot files and checking the refresh command works
        Test will:
           - Create a cluster of 3 nodes.
           - Load older snapshot files
           - Execute nodetool `refresh` command
           - Verify after `refresh` commands are equal to snapshot data
        """
        table_name, num_of_items, node_idx = TABLE_NAME, NUM_OF_ITEMS, 0
        snapshot_folder = os.path.join(ALTERNATOR_SNAPSHOT_FOLDER, "scylla4.0.rc1")

        self.prepare_dynamodb_cluster(num_of_nodes=3)
        node1 = self.cluster.nodelist()[node_idx]
        self.create_table(table_name=table_name, node=node1)
        table_data = self.create_items()
        self.load_snapshot_and_refresh(table_name=table_name, node=node1, snapshot_folder=snapshot_folder)
        diff = self.compare_table_data(table_name=table_name, table_data=table_data, node=node1)
        self.assertTrue(expr=not diff, msg=f"The following items are missing:\n{pformat(diff)}")

    def test_create_snapshot_and_refresh(self):
        """
        The test checks the behavior of the `snapshot` and `refresh` commands for Alternator
        Test will:
          - Create a cluster of 3 nodes.
          - Create a new table with 100 items.
          - Create a snapshot of the table we created earlier.
          - Copy the files from the `snapshot` command and moved it to the table folder and used the `refresh`
            command to load the data.
          - Verify the data before `snapshot` and after `refresh` commands are equal.
        """
        table_name, num_of_items = TABLE_NAME, NUM_OF_ITEMS

        self.prepare_dynamodb_cluster(num_of_nodes=1)
        node1 = self.cluster.nodelist()[0]
        self.create_table(table_name=table_name, node=node1)
        new_items = self.create_items(num_of_items=num_of_items)
        self.batch_write_actions(table_name=table_name, node=node1, new_items=new_items)
        data_before_refresh = self.scan_table(table_name=table_name, node=node1)

        snapshot_folder = tempfile.mkdtemp()
        self.addCleanup(lambda: shutil.rmtree(snapshot_folder))
        self.create_snapshot(table_name=TABLE_NAME, node=node1, snapshot_folder=snapshot_folder)
        self.delete_table(table_name=table_name, node=node1)
        self.create_table(table_name=table_name, node=node1)
        self.load_snapshot_and_refresh(table_name=table_name, node=node1, snapshot_folder=snapshot_folder)
        diff = self.compare_table_data(table_name=table_name, table_data=data_before_refresh, node=node1)
        self.assertTrue(expr=not diff, msg=f"The following items are missing:\n{pformat(diff)}")

    def test_dynamo_gsi(self):
        self.prepare_dynamodb_cluster(num_of_nodes=4)
        node1 = self.cluster.nodelist()[0]
        self.create_table(node=node1, create_gsi=True)
        debug(f"Writing Alternator data on a table with GSI")
        items = generate_put_request_items(num_of_items=NUM_OF_ITEMS, add_gsi=True)
        node_resource_table = self.batch_write_actions(table_name=TABLE_NAME, node=node1, new_items=items)

        node = self.cluster.nodelist()[1]
        debug(f"Stopping {node.name} before testing GSI query")
        node.stop()

        debug("Testing and validating a query using GSI")
        gsi_filtered_val = items[random.randint(0, NUM_OF_ITEMS - 1)][Gsi.ATTRIBUTE_NAME]
        expected_items = [item for item in items if item[Gsi.ATTRIBUTE_NAME] == gsi_filtered_val]
        key_condition = {Gsi.ATTRIBUTE_NAME: {'AttributeValueList': [gsi_filtered_val], 'ComparisonOperator': 'EQ'}}
        result_items = full_query(node_resource_table, IndexName=Gsi.NAME,
                                  KeyConditions=key_condition)
        diff_result = DeepDiff(t1=result_items, t2=expected_items, ignore_order=True)
        self.assertTrue(expr=not diff_result, msg=f"The following items differs:\n{pformat(diff_result)}")

    def test_drain_during_dynamo_load(self):
        self.prepare_dynamodb_cluster(num_of_nodes=3)
        node1, node2, node3 = self.cluster.nodelist()
        self.create_table(table_name=TABLE_NAME, node=node1)
        self.wait_table_exists(TABLE_NAME, self.cluster.nodelist())

        items = self.create_items(num_of_items=NUM_OF_ITEMS)
        self.batch_write_actions(table_name=TABLE_NAME, node=node1, new_items=items)
        get_items_thread = self.run_stress(table_name=TABLE_NAME, node=node1)
        debug(f'Start drain for: {node3.name}')
        node3.drain()
        debug('Drain finished')
        get_items_thread.join()

    def test_decommission_during_dynamo_load(self):
        self.prepare_dynamodb_cluster(num_of_nodes=3)
        node1, node2, node3 = self.cluster.nodelist()
        self.create_table(table_name=TABLE_NAME, node=node1)
        self.wait_table_exists(TABLE_NAME, self.cluster.nodelist())

        items = self.create_items(num_of_items=NUM_OF_ITEMS)
        self.batch_write_actions(table_name=TABLE_NAME, node=node1, new_items=items)
        alternator_consistent_stress = self.run_stress(table_name=TABLE_NAME, node=node1)
        debug(f'Start first decommission during consistent Alternator-load for: {node2.name}')
        node2.decommission()
        debug('Decommission finished')
        alternator_consistent_stress.join()
        alternator_non_consistent_stress = self.run_stress(table_name=TABLE_NAME, node=node1, consistent_read=False)
        debug(f'Start a second decommission during non-consistent alternator-load for: {node3.name}')
        node3.decommission()
        debug('Decommission finished')
        alternator_non_consistent_stress.join()

        debug("Check that the correct error is returned for a consistent read where cluster has no quorum")
        try:
            self.get_table_items(table_name=TABLE_NAME, node=node1, num_of_items=10, consistent_read=True)
            self.fail(msg="Expected ClientError for Alternator query.")
        except ClientError as query_exp:
            self.assertIn('Cannot achieve consistency level for cl LOCAL_QUORUM',
                          query_exp.response['Error']['Message'], msg=query_exp)
            self.assertIn('Internal Server Error', query_exp.response['Error']['Code'], msg=query_exp)

        debug("Check that the correct error is returned for a resource of a decommissioned node")
        dynamodb_api_node2 = self.get_dynamodb_api(node=node2)
        self.node2_resource_table = dynamodb_api_node2.resource.Table(TABLE_NAME)
        with self.assertRaisesRegexp(EndpointConnectionError, "Could not connect to the endpoint URL"):
            self.get_table_items(table_name=TABLE_NAME, node=node2, num_of_items=10, consistent_read=True)

    def test_dynamo_reads_after_repair(self):
        self.prepare_dynamodb_cluster(num_of_nodes=3)
        node1, node2, node3 = self.cluster.nodelist()
        debug(f"Adding data for all nodes except {node2.name}...")
        node2.flush()
        debug(f"Stopping {node2.name}")
        node2.stop(wait_other_notice=True)
        self.create_table(table_name=TABLE_NAME, node=node1)

        items = self.create_items(num_of_items=NUM_OF_ITEMS)
        self.batch_write_actions(table_name=TABLE_NAME, node=node1, new_items=items)

        debug(f"Starting {node2.name}")
        node2.start(wait_other_notice=True, wait_for_binary_proto=True)
        debug(f"starting repair on {node2.name}...")
        info = node2.repair()
        debug(f"{info[0]}\n{info[1]}")
        debug(f"Reading Alternator queries from node {node2.name}")
        self.get_table_items(table_name=TABLE_NAME, node=node2)

    def test_dynamo_queries_on_multi_dc(self):
        self.prepare_dynamodb_cluster(num_of_nodes=3, is_multi_dc=True)
        dc1_node = self.cluster.nodelist()[0]
        self.create_table(table_name=TABLE_NAME, node=dc1_node)
        self.wait_table_exists(TABLE_NAME, self.cluster.nodelist())

        debug(f"Writing Alternator queries to node {dc1_node.name} on data-center {dc1_node.data_center}")
        items = self.create_items(num_of_items=NUM_OF_ITEMS)
        self.batch_write_actions(table_name=TABLE_NAME, node=dc1_node, new_items=items)

        dc2_node = next(node for node in self.cluster.nodelist() if node.data_center != dc1_node.data_center)
        debug(f"Reading Alternator queries from node {dc2_node.name} on data-center {dc2_node.data_center}")
        self.get_table_items(table_name=TABLE_NAME, node=dc2_node, consistent_read=False)

    def test_dynamo_reads_after_new_node_repair(self):
        self.prepare_dynamodb_cluster(num_of_nodes=3)
        node1, node2, node3 = self.cluster.nodelist()
        debug("Adding data for all nodes")
        self.prefill_dynamodb_table(node=node1)
        debug(f"Decommissioning {node3.name}")
        node3.decommission()
        debug("Add node4..")
        node4 = new_node(self.cluster, bootstrap=True)
        debug("Start node4..")
        node4.start(wait_for_binary_proto=True, wait_other_notice=True)
        debug(f"starting repair on {node4.name}...")
        stdout, stderr = node4.repair()
        debug(f'nodetool repair : stdout={stdout}, stderr={stderr}')
        debug(f"Stopping {node1.name}")
        node1.stop(wait_other_notice=True)
        debug(f"Stopping {node2.name}")
        node2.stop(wait_other_notice=True)
        tested_node = node4
        debug(f"Reading Alternator queries from node {tested_node.name}")
        self.get_table_items(table_name=TABLE_NAME, node=tested_node, consistent_read=False)

    def test_read_key_condition_expression(self):
        self.prepare_dynamodb_cluster(num_of_nodes=3)
        node1 = self.cluster.nodelist()[0]
        self.create_table(node=node1, base_schema=CONDITION_EXPRESSION_SCHEMA)
        debug("Writing Alternator items of the same partition key")
        pk_condition_value = random_string(length=DEFAULT_STRING_LENGTH)
        items = [{'pk': pk_condition_value, 'c': Decimal(i), 'a': random_string(length=DEFAULT_STRING_LENGTH)} for i in range(12)]
        table = self.batch_write_actions(table_name=TABLE_NAME, node=node1, new_items=items)
        debug("Writing an extra different partition key")
        with table.batch_writer() as batch:
            batch.put_item({'pk': random_string(length=DEFAULT_STRING_LENGTH), 'c': 123, 'a': random_string(length=DEFAULT_STRING_LENGTH)})
        node = self.cluster.nodelist()[1]
        debug(f"Stopping {node.name} before testing key condition expression query")
        node.stop()
        debug("Testing and validating a query using key condition expression")
        got_condition_items = full_query(table, KeyConditionExpression='pk=:pk',
                                         ExpressionAttributeValues={':pk': pk_condition_value})
        diff_result = DeepDiff(t1=items, t2=got_condition_items, ignore_order=True)
        self.assertTrue(expr=not diff_result, msg=f"The following items differs:\n{pformat(diff_result)}")

    def test_update_condition_expression(self):
        self.prepare_dynamodb_cluster(num_of_nodes=3, is_multi_dc=True)
        node1 = self.cluster.nodelist()[0]
        node2 = next(node for node in self.cluster.nodelist() if
                     node.data_center == node1.data_center and node.name != node1.name)
        debug("Adding data for all nodes from DC1")
        table = self.prefill_dynamodb_table(node=node1)

        debug(f"Stopping {node2.name} (before testing update query with key condition expression)")
        node2.stop()

        dc2_node = next(node for node in self.cluster.nodelist() if node.data_center != node1.data_center)
        dynamodb_api = self.get_dynamodb_api(node=dc2_node)
        dc2_table = dynamodb_api.resource.Table(name=TABLE_NAME)

        debug("Testing and validating an update query using key condition expression")
        new_pk_val = random_string(length=DEFAULT_STRING_LENGTH)
        debug("simple update from dc1")
        table.update_item(Key={'pk': new_pk_val},
                          AttributeUpdates={'a': {'Value': 1, 'Action': 'PUT'}})
        debug("ConditionExpression update from dc2")
        dc2_table.update_item(Key={'pk': new_pk_val},
                              UpdateExpression='SET c = :val',
                              ConditionExpression='attribute_exists (a)',
                              ExpressionAttributeValues={':val': 2})
        debug("ConditionExpression update from dc1")
        table.update_item(Key={'pk': new_pk_val},
                          UpdateExpression='SET c = :val',
                          ConditionExpression='attribute_not_exists (b)',
                          ExpressionAttributeValues={':val': 3})
        assert table.get_item(Key={'pk': new_pk_val}, ConsistentRead=True)['Item']['c'] == 3
        with self.assertRaisesRegexp(ClientError, "ConditionalCheckFailedException"):
            table.update_item(Key={'pk': new_pk_val},
                              UpdateExpression='SET c = :val',
                              ConditionExpression='attribute_not_exists (a)',
                              ExpressionAttributeValues={':val': 4})

    def _reboot_nodes_while_running_stress(self, node):
        for _node in self.cluster.nodelist():
            if _node.name != node.name:
                debug(f"Stopping node '{_node.name}'")
                _node.stop()
                debug(f"Starting node '{_node.name}'")
                _node.start(wait_other_notice=True, wait_for_binary_proto=True)

    def test_full_scan_table_while_restart_each_nodes(self):
        """
        Checks scan logic while each node is stopping and after that starting
        """
        table_name, num_of_items, node_idx = TABLE_NAME, NUM_OF_ITEMS, 0
        self.prepare_dynamodb_cluster(num_of_nodes=NUM_OF_NODES)
        node1 = self.cluster.nodelist()[node_idx]
        self.prefill_dynamodb_table(node=node1, table_name=table_name, num_of_items=num_of_items)
        alternator_scan_thread = self.run_scan_stress(table_name=table_name, node=node1)

        debug("Starting Alternator scan stress..")
        alternator_scan_thread.start()
        self._reboot_nodes_while_running_stress(node=node1)

    def test_full_parallel_scan_table_while_restart_each_nodes(self):
        """
        Checks parallel scan logic while each node is stopping and after that starting
        """
        table_name, num_of_items, node_idx, threads_num = TABLE_NAME, NUM_OF_ITEMS, 0, 4
        self.prepare_dynamodb_cluster(num_of_nodes=NUM_OF_NODES)
        node1 = self.cluster.nodelist()[node_idx]
        self.prefill_dynamodb_table(node=node1, table_name=table_name, num_of_items=num_of_items)
        alternator_scan_thread = self.run_scan_stress(table_name=table_name, node=node1, threads_num=threads_num)

        debug("Starting Alternator scan stress..")
        alternator_scan_thread.start()
        self._reboot_nodes_while_running_stress(node=node1)

    def test_full_parallel_scan_table_while_insert_update_delete_items(self):
        """
        Checks parallel scan logic while each node is stopping and after that starting and in the background there is
         a thread that creates and updates items.
        """
        table_name, num_of_items, node_idx, threads_num = TABLE_NAME, NUM_OF_ITEMS, 0, 4
        self.prepare_dynamodb_cluster(num_of_nodes=NUM_OF_NODES)
        node1 = self.cluster.nodelist()[node_idx]
        self.prefill_dynamodb_table(node=node1, table_name=table_name, num_of_items=num_of_items)
        self.update_items(table_name=table_name, node=node1)
        alternator_scan_thread = \
            self.run_scan_stress(table_name=table_name, node=node1, threads_num=threads_num,
                                 is_compare_scan_result=False)

        insert_update_thread = self.run_delete_insert_update_item_stress(table_name=table_name, node=node1)
        debug("Starting Alternator create and update items stress..")
        insert_update_thread.start()

        debug("Starting Alternator scan stress..")
        alternator_scan_thread.start()
        self._reboot_nodes_while_running_stress(node=node1)

    def test_dynamo_types(self):
        """
        Create items with each of DynamoDB supported types and verify:
            * No errors while inserting items to DB
            * Get all the values from DB and compare to what we know we inserted
        """
        all_items = []
        table_name, items_count = TABLE_NAME, 0
        data_generator = AlternatorDataGenerator(
            primary_key=self._table_primary_key, primary_key_format=self._table_primary_key_format)
        data_generator.create_random_number_item()
        self.prepare_dynamodb_cluster(num_of_nodes=NUM_OF_NODES)
        node1, node2, node3 = self.cluster.nodelist()
        self.create_table(table_name=table_name, node=node1)
        self.wait_table_exists(table_name, self.cluster.nodelist())

        for mode in TypeMode:
            items = data_generator.create_multiple_items(num_of_items=random.randint(1, 10), mode=mode)
            all_items += items
            debug(f"Adding '{len(items)}' {data_generator.get_mode_name} items to table '{table_name}'..")
            self.batch_write_actions(table_name=table_name, node=node1, new_items=items)
            diff = self.compare_table_data(table_name=table_name, table_data=all_items, node=node1)
            self.assertTrue(expr=not diff, msg=f"The following items are missing:\n{pformat(diff)}")
