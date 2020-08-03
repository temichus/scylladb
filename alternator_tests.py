import operator
import os
import random
import shutil
import string
import tempfile
import time
from copy import deepcopy
from boto3.dynamodb.conditions import Attr
from decimal import Decimal
from pprint import pformat

import boto3.dynamodb.types
from botocore.exceptions import ClientError, EndpointConnectionError
from deepdiff import DeepDiff
from nose.plugins.attrib import attr

from alternator.utils import schemas
from alternator.utils.data_generator import AlternatorDataGenerator, TypeMode
from alternator_utils import TesterAlternator, ALTERNATOR_SNAPSHOT_FOLDER, TABLE_NAME, NUM_OF_ITEMS, random_string, \
    DEFAULT_STRING_LENGTH, NUM_OF_NODES, set_write_isolation, WriteIsolation, LONGEST_TABLE_SIZE, SHORTEST_TABLE_SIZE
from alternator_utils import generate_put_request_items, Gsi, full_query
from dtest import debug, wait_for, info
from tools import new_node, require


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

        items = self.create_items(num_of_items=NUM_OF_ITEMS)
        self.batch_write_actions(table_name=TABLE_NAME, node=node1, new_items=items)
        get_items_thread = self.run_read_stress(table_name=TABLE_NAME, node=node1)
        debug(f'Start drain for: {node3.name}')
        node3.drain()
        debug('Drain finished')
        get_items_thread.join()

    def test_decommission_during_dynamo_load(self):
        self.prepare_dynamodb_cluster(num_of_nodes=3)
        node1, node2, node3 = self.cluster.nodelist()
        self.create_table(table_name=TABLE_NAME, node=node1)

        items = self.create_items(num_of_items=NUM_OF_ITEMS)
        self.batch_write_actions(table_name=TABLE_NAME, node=node1, new_items=items)
        alternator_consistent_stress = self.run_read_stress(table_name=TABLE_NAME, node=node1)
        debug(f'Start first decommission during consistent Alternator-load for: {node2.name}')
        node2.decommission()
        debug('Decommission finished')
        alternator_consistent_stress.join()
        alternator_non_consistent_stress = self.run_read_stress(table_name=TABLE_NAME, node=node1, consistent_read=False)
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
        self.create_table(node=node1, schema=schemas.CONDITION_EXPRESSION_SCHEMA)
        debug("Writing Alternator items of the same partition key")
        pk_condition_value = random_string(length=DEFAULT_STRING_LENGTH)
        items = [{'pk': pk_condition_value, 'c': Decimal(i), 'a': random_string(length=DEFAULT_STRING_LENGTH)} for i in range(12)]
        table = self.batch_write_actions(table_name=TABLE_NAME, node=node1, new_items=items)
        debug("Writing an extra different partition key")
        with table.batch_writer() as batch:
            batch.put_item({'pk': random_string(length=DEFAULT_STRING_LENGTH), 'c': 123,
                            'a': random_string(length=DEFAULT_STRING_LENGTH)})
        node = self.cluster.nodelist()[1]
        debug(f"Stopping {node.name} before testing key condition expression query")
        node.stop()
        debug("Testing and validating a query using key condition expression")
        got_condition_items = full_query(table, KeyConditionExpression='pk=:pk',
                                         ExpressionAttributeValues={':pk': pk_condition_value})
        diff_result = DeepDiff(t1=items, t2=got_condition_items, ignore_order=True)
        self.assertTrue(expr=not diff_result, msg=f"The following items differs:\n{pformat(diff_result)}")

    def test_write_isolation_during_stress(self):
        """
        Modify tables write-isolation during stress
        """
        self.prepare_dynamodb_cluster(num_of_nodes=3)
        node1 = self.cluster.nodelist()[0]
        debug("Adding data for tables of all write-isolation types")
        conf_workloads = []
        for isolation in WriteIsolation:
            table_name=f'{TABLE_NAME}_{isolation.value}'
            table = self.prefill_dynamodb_table(node=node1, table_name=table_name)
            set_write_isolation(table=table, isolation=isolation)
            conf_workloads.append(
                {'table': table, 'write_stress': self.run_write_stress(table_name=table_name, node=node1),
                 'read_stress': self.run_read_stress(table_name=table_name, node=node1)})

        cycles = 3
        for cycle in range(1, cycles+1):
            for conf in conf_workloads:
                debug(f"cycle {cycle}/{cycles}: modifying {conf['table']}")
                set_write_isolation(table=conf['table'], isolation=random.choice(list(WriteIsolation)))
            time.sleep(5)

        for conf in conf_workloads:
            conf['write_stress'].join()
            conf['read_stress'].join()

    def test_update_condition_unused_entries_short_circuit(self):
        """
        A test for https://github.com/scylladb/scylla/issues/6572 plus a multi DC configuration
        """
        self.prepare_dynamodb_cluster(num_of_nodes=3, is_multi_dc=True)
        node1 = self.cluster.nodelist()[0]
        debug("Creating a table..")
        table = self.create_table(table_name=TABLE_NAME, node=node1)
        new_pk_val = random_string(length=DEFAULT_STRING_LENGTH)
        debug("simple update item")
        table.update_item(Key={self._table_primary_key: new_pk_val},
                          AttributeUpdates={'a': {'Value': 1, 'Action': 'PUT'}})
        conditional_update_short_circuit = dict(Key={self._table_primary_key: new_pk_val},
                                                ConditionExpression='#name1 = :val1 OR #name2 = :val2 OR :val3 = :val2',
                                                UpdateExpression="SET #name3 = :val3",
                                                ExpressionAttributeNames={'#name1': 'a', '#name2': 'b', '#name3': 'c'},
                                                ExpressionAttributeValues={':val1': 1, ':val2': 2, ':val3': 3})
        dc2_node = next(node for node in self.cluster.nodelist() if node.data_center != node1.data_center)
        dc2_table = self.get_table(table_name=TABLE_NAME, node=dc2_node)
        wait_for(self.is_table_schema_synced, timeout=30, text='Waiting until table schema is updated',
                 table_name=TABLE_NAME, nodes=[node1, dc2_node])
        node1.stop()
        debug("Testing and validating an update query using key condition expression")
        debug(f"ConditionExpression update of short circuit is: {conditional_update_short_circuit}")
        dc2_table.update_item(**conditional_update_short_circuit)
        dc2_node.stop()
        node1.start()
        debug(f"Reading Alternator queries from node {node1.name} on data-center {node1.data_center}")
        item = table.get_item(Key={self._table_primary_key: new_pk_val}, ConsistentRead=True)['Item']
        assert item == {self._table_primary_key: new_pk_val, 'a': 1, 'c': 3}

    def test_filter_expression(self):
        self.prepare_dynamodb_cluster(is_multi_dc=True)
        node1 = self.cluster.nodelist()[0]
        schema = schemas.HASH_AND_NUM_RANGE_SCHEMA
        self.create_table(node=node1, schema=schema)
        hash_key_name, range_key_name = schemas.HASH_KEY_NAME, schemas.RANGE_KEY_NAME
        items = [{hash_key_name: f"{hash_value}", range_key_name: range_value}
                 for hash_value in range(10)
                 for range_value in range(10)]
        self.batch_write_actions(table_name=TABLE_NAME, node=node1, new_items=items, schema=schema)
        selected_range_value = random.choice(items)[range_key_name]
        dc2_node = next(node for node in self.cluster.nodelist() if node.data_center != node1.data_center)
        wait_for(self.is_table_schema_synced, timeout=30, text='Waiting until table schema is updated',
                 table_name=TABLE_NAME, nodes=[node1, dc2_node])
        node1.stop()
        debug("Testing a query using filter expression")
        expected_items = [item for item in items if item[range_key_name] >= selected_range_value]
        diff = self.compare_table_data(table_name=TABLE_NAME, table_data=expected_items, node=dc2_node,
                                FilterExpression=Attr(range_key_name).gte(selected_range_value))
        self.assertTrue(expr=not diff, msg=f"The following items differs:\n{pformat(diff)}")

    def test_update_condition_expression_and_write_isolation(self):
        """
        See that using conditional update queries run correctly when LWT is enabled.
        Check that they can't run when LWT is disabled for table, when using "forbid_lwt" write-isolation.
        """
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
        table.update_item(Key={self._table_primary_key: new_pk_val},
                          AttributeUpdates={'a': {'Value': 1, 'Action': 'PUT'}})
        debug("ConditionExpression update from dc2:")
        conditional_update_c_2 = dict(Key={self._table_primary_key: new_pk_val},
                                      UpdateExpression='SET c = :val',
                                      ConditionExpression='attribute_exists (a)',
                                      ExpressionAttributeValues={':val': 2})
        debug(conditional_update_c_2)
        debug("Check that conditional update fails on write-isolation 'forbid' mode (dc2)")
        set_write_isolation(table, WriteIsolation.FORBID_RMW)
        wait_for(self.is_table_schema_synced, timeout=30, text='Waiting until table schema is updated',
                 table_name=TABLE_NAME, nodes=[node1, dc2_node])
        msg_rmw_is_disabled = 'Read-modify-write operations are disabled'
        with self.assertRaisesRegexp(ClientError, msg_rmw_is_disabled):
            res= dc2_table.update_item(**conditional_update_c_2)
            debug(res)
        set_write_isolation(table, WriteIsolation.ALWAYS_USE_LWT)
        wait_for(self.is_table_schema_synced, timeout=30, text='Waiting until table schema is updated',
                 table_name=TABLE_NAME, nodes=[node1, dc2_node])
        dc2_table.update_item(**conditional_update_c_2)
        debug("ConditionExpression update from dc1:")
        conditional_update_c_3 = dict(Key={self._table_primary_key: new_pk_val},
                                      UpdateExpression='SET c = :val',
                                      ConditionExpression='attribute_not_exists (b)',
                                      ExpressionAttributeValues={':val': 3})
        debug(conditional_update_c_3)

        with self.assertRaisesRegexp(ClientError, msg_rmw_is_disabled):
            set_write_isolation(table, WriteIsolation.FORBID_RMW)
            table.update_item(**conditional_update_c_3)
        set_write_isolation(table, WriteIsolation.ONLY_RMW_USES_LWT)
        table.update_item(**conditional_update_c_3)
        assert table.get_item(Key={self._table_primary_key: new_pk_val}, ConsistentRead=True)['Item']['c'] == 3
        with self.assertRaisesRegexp(ClientError, "ConditionalCheckFailedException"):
            table.update_item(Key={self._table_primary_key: new_pk_val},
                              UpdateExpression='SET c = :val',
                              ConditionExpression='attribute_not_exists (a)',
                              ExpressionAttributeValues={':val': 4})

    def test_modified_tag_is_propagated_to_other_dc(self):
        self.prepare_dynamodb_cluster(num_of_nodes=1, is_multi_dc=True)
        node1 = self.cluster.nodelist()[0]
        table = self.prefill_dynamodb_table(node=node1)
        dc2_node = next(node for node in self.cluster.nodelist() if node.data_center != node1.data_center)
        debug("Check that updating write-isolation tag on one DC is propagated to a node of the other DC (dc2)")
        set_write_isolation(table, WriteIsolation.FORBID_RMW)
        res = wait_for(self.is_table_schema_synced, timeout=30, step=3, text='Waiting until table schema is updated',
                       table_name=TABLE_NAME, nodes=[node1, dc2_node])

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

        for mode in TypeMode:
            items = data_generator.create_multiple_items(num_of_items=random.randint(1, 10), mode=mode)
            all_items += items
            debug(f"Adding {len(items)} {data_generator.get_mode_name(mode)} items to table '{table_name}'..")
            self.batch_write_actions(table_name=table_name, node=node1, new_items=items)
            diff = self.compare_table_data(table_name=table_name, table_data=all_items, node=node1)
            self.assertTrue(expr=not diff, msg=f"The following items are missing:\n{pformat(diff)}")

    def test_read_system_tables_via_dynamodb_api(self):
        """
        make sure we could only read system tables via dynamodb api

        https://github.com/scylladb/scylla/issues/6122
        """
        self.prepare_dynamodb_cluster(num_of_nodes=3)
        all_nodes = self.cluster.nodelist()

        # check each node peer are the node we expect
        for node in all_nodes:
            results = self.scan_table('.scylla.alternator.system.peers', node=node)
            peers = set(item['peer'] for item in results)

            # get all the other nodes ip addresses
            other_nodes_ips = set(self.get_ip_from_node(n) for n in set(all_nodes).difference({node}))

            assert peers == other_nodes_ips, f"peers in {node.name} are not as expected {other_nodes_ips}"

            debug("trying to write into system table, which should be readonly")
            self.assertRaisesRegex(expected_exception=ClientError, expected_regex=r"ResourceNotFoundException",
                                   callable=self.batch_write_actions,
                                   table_name='.scylla.alternator.system.peers', new_items=[dict(pk=1)], node=node)

    def _check_comparison_query_key_conditions_options(self, scan_index_forward=True):
        schema = schemas.HASH_AND_NUM_RANGE_SCHEMA
        python_compare_op_dict = {'LE': operator.le, "LT": operator.lt, "GE": operator.ge, "GT": operator.gt}
        all_selected_hash_items = []
        hash_key_name, range_key_name = schemas.HASH_KEY_NAME, schemas.RANGE_KEY_NAME
        table_name, node_idx = TABLE_NAME, 0

        self.prepare_dynamodb_cluster(num_of_nodes=NUM_OF_NODES)
        node = self.cluster.nodelist()[node_idx]
        if self.is_table_exists(table_name=table_name, node=node):
            self.delete_table(table_name=table_name, node=node)
        self.create_table(node=node, table_name=table_name, schema=schema)

        items = [{hash_key_name: f"{hash_value}", range_key_name: range_value}
                 for hash_value in range(random.randint(1, 10))
                 for range_value in range(random.randint(1, 10))]
        node_resource_table = self.batch_write_actions(table_name=table_name, node=node, new_items=items, schema=schema)
        selected_item = random.choice(items)
        selected_hash_value = selected_item[hash_key_name]
        selected_range_value = selected_item[range_key_name]
        all_selected_hash_items = [item for item in items if selected_hash_value == item[hash_key_name]]

        for compare_op in ["EQ", "LE", "LT", "GE", "GT"]:
            key_condition = {
                hash_key_name: {'AttributeValueList': [selected_hash_value], 'ComparisonOperator': 'EQ'},
                range_key_name: {'AttributeValueList': [selected_range_value], 'ComparisonOperator': compare_op},
            }
            query_result = full_query(node_resource_table, KeyConditions=key_condition,
                                      ScanIndexForward=scan_index_forward)
            if compare_op == "EQ":
                expected_items = [selected_item]
            else:
                py_compare_op = python_compare_op_dict[compare_op]
                expected_items = [item for item in all_selected_hash_items[::-1 if not scan_index_forward else 1]
                                  if py_compare_op(item[range_key_name], selected_range_value)]

            debug(f"Running query with key condition '{key_condition}'")
            diff = DeepDiff(t1=expected_items, t2=query_result, ignore_numeric_type_changes=True)
            self.assertTrue(expr=not diff, msg=f"The following items differs:\n{pformat(diff)}")

    def test_check_comparison_query_key_condition_options(self):
        """
        Check the result of Query for each "EQ", "LE", "LT", "GE" and "GT" comparison operation when ScanIndexForward is
        True (The resulting order is ascending)
        """
        self._check_comparison_query_key_conditions_options()

    def test_check_comparison_query_key_condition_options_without_scan_index_forward(self):
        """
        Check the result of Query for each "EQ", "LE", "LT", "GE" and "GT" comparison operation when ScanIndexForward is
        True (The resulting order is descending)
        """
        self._check_comparison_query_key_conditions_options(scan_index_forward=False)

    def _check_string_query_key_conditions_options(self, scan_index_forward=True):
        secondary_key_values = [chr(char_value) for char_value in range(256)]
        binary_items = []
        hash_key_name, range_key_name = schemas.HASH_KEY_NAME, schemas.RANGE_KEY_NAME
        table_name, node_idx, selected_item_idx = TABLE_NAME, 0, 0
        regular_items = [{hash_key_name: f"{hash_value}", range_key_name: range_value}
                         for hash_value in range(random.randint(1, 10))
                         for range_value in secondary_key_values]
        for item in deepcopy(regular_items):
            item[range_key_name] = boto3.dynamodb.types.Binary(item[range_key_name].encode())
            binary_items.append(item)

        self.prepare_dynamodb_cluster(num_of_nodes=NUM_OF_NODES)
        node = self.cluster.nodelist()[node_idx]

        def test_logic(schema):
            is_binary_mode = bool(schema == schemas.HASH_AND_BINARY_RANGE_SCHEMA)
            debug(f"Check Query string comparison for item with {'binary' if is_binary_mode else 'string'}")
            if self.is_table_exists(table_name=table_name, node=node):
                self.delete_table(table_name=table_name, node=node)
            self.create_table(node=node, table_name=table_name, schema=schema)
            items = binary_items if is_binary_mode else regular_items

            selected_hash_value = random.choice(items)[hash_key_name]
            all_selected_hash_items = [_item for _item in items if _item[hash_key_name] == selected_hash_value]
            node_resource_table = self.batch_write_actions(table_name=table_name, node=node, new_items=items)

            for compare_op in ["BEGINS_WITH", "BETWEEN"]:
                expected_items = []
                if compare_op == "BETWEEN":
                    selected_range_values = random.choices(population=secondary_key_values, k=2)
                    low, high = selected_range_values[0], selected_range_values[1]
                    if high < low:
                        # Swap between 2 variables
                        low, high = high, low
                        selected_range_values = [low, high]

                    for _item in all_selected_hash_items:
                        range_value = _item[range_key_name].value.decode() if is_binary_mode else _item[range_key_name]
                        if low <= range_value <= high:
                            expected_items.append(_item)
                elif compare_op == "BEGINS_WITH":
                    selected_range_values = [random.choice(secondary_key_values)]
                    for _item in all_selected_hash_items:
                        range_value = _item[range_key_name].value.decode() if is_binary_mode else _item[range_key_name]
                        if range_value.startswith(selected_range_values[0]):
                            expected_items.append(_item)
                else:
                    raise ValueError(f"The following value '{compare_op}' not supported")

                if not scan_index_forward:
                    expected_items = expected_items[::-1]
                if is_binary_mode:
                    selected_range_values = [boto3.dynamodb.types.Binary(val.encode()) for val in selected_range_values]

                key_condition = {
                    hash_key_name: {'AttributeValueList': [selected_hash_value], 'ComparisonOperator': 'EQ'},
                    range_key_name: {'AttributeValueList': selected_range_values, 'ComparisonOperator': compare_op},
                }
                query_result = full_query(
                    node_resource_table, KeyConditions=key_condition, ScanIndexForward=scan_index_forward)
                debug(f"Running query with key condition '{key_condition}'")
                diff = DeepDiff(t1=expected_items, t2=query_result)
                self.assertTrue(expr=not diff, msg=f"The following items differs:\n{pformat(diff)}")

        test_logic(schema=schemas.HASH_AND_STR_RANGE_SCHEMA)
        test_logic(schema=schemas.HASH_AND_BINARY_RANGE_SCHEMA)

    def test_check_string_and_binary_query_key_condition_options(self):
        """
        Check the result of Query for each "BEGINS_WITH" and "BETWEEN" comparison operation when ScanIndexForward is
        True (The resulting order is ascending).

        For each HASH key generator all combinations of all digits (0-9) and chars (a-z and A-Z).
        """
        self._check_string_query_key_conditions_options()

    def test_check_string_and_binary_query_key_condition_options_without_scan_index_forward(self):
        """
        Check the result of Query for each "BEGINS_WITH" and "BETWEEN" comparison operation when ScanIndexForward is
        True (The resulting order is descending).

        For each HASH key generator all combinations of all digits (0-9) and chars (a-z and A-Z).
        """
        self._check_string_query_key_conditions_options(scan_index_forward=False)

    def test_table_name_length(self):
        # TODO: After the bug "#6521" is resolved, need to add "." char to variable valid_dynamodb_chars
        valid_dynamodb_chars = (list(string.digits) + list(string.ascii_uppercase) + ["_", "-"])
        self.prepare_dynamodb_cluster(num_of_nodes=3)
        node1 = self.cluster.nodelist()[0]

        shortest_table_name = "".join(random.choices(valid_dynamodb_chars, k=SHORTEST_TABLE_SIZE))
        info(f"Creating new table with following name '{shortest_table_name}' (The shortest table name - "
             f"'{SHORTEST_TABLE_SIZE}' chars)")
        self.create_table(node=node1, table_name=shortest_table_name)

        middle_table_name = "".join(random.choices(valid_dynamodb_chars, k=(
                SHORTEST_TABLE_SIZE + LONGEST_TABLE_SIZE) // 2))
        info(f"Creating new table with following name '{middle_table_name}' ('{len(middle_table_name)}' chars)")
        self.create_table(node=node1, table_name=middle_table_name)

        longest_table_name = "".join(random.choices(valid_dynamodb_chars, k=LONGEST_TABLE_SIZE))
        info(f"Creating new table with following name '{longest_table_name}' (The shortest table name - "
             f"'{LONGEST_TABLE_SIZE}' chars)")
        self.create_table(node=node1, table_name=longest_table_name)
        cmd = f"tablestats alternator_{longest_table_name}"
        info(f"Executing the following command '{cmd}'")
        node1.nodetool(cmd)

    @require("#6521")
    def test_table_name_with_dot_prefix(self):
        valid_dynamodb_chars = (list(string.digits) + list(string.ascii_uppercase) + ["_", "-", "."])
        self.prepare_dynamodb_cluster(num_of_nodes=3)
        node1 = self.cluster.nodelist()[0]

        table_name_with_dot_prefix = "." + "".join(random.choices(valid_dynamodb_chars, k=min(random.choice(range(
            SHORTEST_TABLE_SIZE, LONGEST_TABLE_SIZE + 1)), 100)))
        info("Creating new table with dot ('.') char prefix")
        self.create_table(node=node1, table_name=table_name_with_dot_prefix)
        cmd = f"tablestats alternator_{table_name_with_dot_prefix}"
        info(f"Executing the following command '{cmd}'")
        node1.nodetool(cmd)
