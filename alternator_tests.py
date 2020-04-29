import os
import shutil
import tempfile
from pprint import pformat

from botocore import exceptions as boto3_exceptions

from alternator_utils import TesterAlternator, ALTERNATOR_SNAPSHOT_FOLDER, TABLE_NAME, NUM_OF_ITEMS
from dtest import debug


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

        self.prepare_cluster(num_of_nodes=3)
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

        self.prepare_cluster(num_of_nodes=1)
        node1 = self.cluster.nodelist()[0]
        self.create_table(table_name=table_name, node=node1)
        self.generate_request_items(table_name=table_name, num_of_items=num_of_items, node=node1)
        data_before_refresh = self.scan_table(table_name=table_name, node=node1)

        snapshot_folder = tempfile.mkdtemp()
        self.addCleanup(lambda: shutil.rmtree(snapshot_folder))
        self.create_snapshot(table_name=TABLE_NAME, node=node1, snapshot_folder=snapshot_folder)
        self.delete_table(table_name=table_name, node=node1)
        self.create_table(table_name=table_name, node=node1)
        self.load_snapshot_and_refresh(table_name=table_name, node=node1, snapshot_folder=snapshot_folder)
        diff = self.compare_table_data(table_name=table_name, table_data=data_before_refresh, node=node1)
        self.assertTrue(expr=not diff, msg=f"The following items are missing:\n{pformat(diff)}")

    def test_drain_during_dynamo_load(self):
        self.prepare_cluster()
        node1, node2, node3 = self.cluster.nodelist()
        self.create_table(table_name=TABLE_NAME, node=node1)
        self.generate_request_items(table_name=TABLE_NAME, node=node1)
        get_items_thread = self.run_stress(table_name=TABLE_NAME, node=node1)
        debug(f'Start drain for: {node3.name}')
        node3.drain()
        debug('Drain finished')
        get_items_thread.join()

    def test_decommission_during_dynamo_load(self):
        self.prepare_cluster()
        node1, node2, node3 = self.cluster.nodelist()
        self.create_table(table_name=TABLE_NAME, node=node1)
        self.generate_request_items(table_name=TABLE_NAME, node=node1)
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
        except boto3_exceptions.ClientError as query_exp:
            self.assertIn('Cannot achieve consistency level for cl LOCAL_QUORUM',
                          query_exp.response['Error']['Message'], msg=query_exp)
            self.assertIn('Internal Server Error', query_exp.response['Error']['Code'], msg=query_exp)

        debug("Check that the correct error is returned for a resource of a decommissioned node")
        dynamodb_api_node2 = self.get_dynamodb_api(node=node2)
        self.node2_resource_table = dynamodb_api_node2.resource.Table(TABLE_NAME)
        with self.assertRaisesRegexp(boto3_exceptions.EndpointConnectionError, "Could not connect to the endpoint URL"):
            self.get_table_items(table_name=TABLE_NAME, node=node2, num_of_items=10, consistent_read=True)

    def test_dynamo_reads_after_repair(self):
        self.prepare_cluster(num_of_nodes=3)
        node1, node2, node3 = self.cluster.nodelist()
        debug(f"Adding data for all nodes except {node2.name}...")
        node2.flush()
        debug(f"Stopping {node2.name}")
        node2.stop(wait_other_notice=True)
        self.create_table(table_name=TABLE_NAME, node=node1)
        self.generate_request_items(table_name=TABLE_NAME, node=node1)
        debug(f"Starting {node2.name}")
        node2.start(wait_other_notice=True, wait_for_binary_proto=True)
        debug(f"starting repair on {node2.name}...")
        info = node2.repair()
        debug(f"{info[0]}\n{info[1]}")
        debug(f"Reading Alternator queries from node {node2.name}")
        self.get_table_items(table_name=TABLE_NAME, node=node2)

    def test_dynamo_queries_on_multi_dc(self):
        self.prepare_cluster(num_of_nodes=3, is_multi_dc=True)
        dc1_node = self.cluster.nodelist()[0]
        self.create_table(table_name=TABLE_NAME, node=dc1_node)
        debug(f"Writing Alternator queries to node {dc1_node.name} on data-center {dc1_node.data_center}")
        self.generate_request_items(table_name=TABLE_NAME, node=dc1_node)
        dc2_node = next(node for node in self.cluster.nodelist() if node.data_center != dc1_node.data_center)
        debug(f"Reading Alternator queries from node {dc2_node.name} on data-center {dc2_node.data_center}")
        self.get_table_items(table_name=TABLE_NAME, node=dc2_node, consistent_read=False)
