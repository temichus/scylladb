import boto3
from botocore import exceptions as boto3_exceptions
from nose.plugins.attrib import attr

from alternator_utils import create_dynamodb_table, generate_put_request_items, StoppableThread, get_table_items
from dtest import Tester, debug

TABLE_NAME = 'user_table'
NUM_OF_NODES = 3
NUM_OF_ITEMS = 100

@attr('dtest-full')
class AlternatorTest(Tester):

    # #############################  Utils #########################################

    def _prepare_alternator_cluster(self, num_of_nodes: int = NUM_OF_NODES, is_multi_dc: bool = False) -> None:
        cluster_type = 'single DC' if not is_multi_dc else 'multi DC'
        debug(f'Populate a cluster with: {num_of_nodes} nodes for {cluster_type}')
        cluster = self.cluster
        cluster.set_configuration_options({'start_native_transport': True})
        cluster.set_configuration_options({'experimental': True, 'alternator_port': 8080})
        if not is_multi_dc:
            cluster.populate([num_of_nodes])
        else:
            cluster.populate([num_of_nodes, num_of_nodes])
        debug('Starting cluster..')
        cluster.start(wait_for_binary_proto=True, wait_other_notice=True)

    def _run_alternator_stress(self, consistent_read: bool = True) -> StoppableThread:
        debug('Start Alternator stress..')
        resource_table = self._get_dynamodb_client().Table(TABLE_NAME)
        get_items_thread = StoppableThread(target=get_table_items,
                                           kwargs={'table': resource_table,
                                                   'consistent_read': consistent_read})

        self.addCleanup(get_items_thread.join)
        get_items_thread.start()
        return get_items_thread

    def _get_dynamodb_client(self, node=None):
        node = node or self.cluster.nodelist()[0]
        node_ip = self.get_ip_from_node(node)
        debug(f"Getting a resource for node {node.name} {node_ip}")
        return boto3.resource('dynamodb', endpoint_url=f'http://{node_ip}:8080',
                              region_name='None', aws_access_key_id='None', aws_secret_access_key='None')

    def _create_dynamodb_table(self, node=None):
        node_resource = self._get_dynamodb_client(node=node)
        debug(f"Creating a table, using {node_resource.meta.client.meta.endpoint_url} resource..")
        create_dynamodb_table(dynamodb_resource=node_resource)
        return node_resource

    def _batch_writer_item_list(self, dynamo_client=None):
        dynamo_client = dynamo_client or self._get_dynamodb_client()
        debug(f"Executing batch_write_item, using {dynamo_client.meta.client.meta.endpoint_url} resource..")
        table = dynamo_client.Table(TABLE_NAME)
        with table.batch_writer() as batch:
            for item in generate_put_request_items(num_of_items=NUM_OF_ITEMS):
                debug(f"put item: {item}")
                batch.put_item(item)
        return table

    def _prefill_alternator_table(self, node=None):
        node_resource = self._create_dynamodb_table(node=node)
        node_resource_table = self._batch_writer_item_list(dynamo_client=node_resource)
        return node_resource_table

    def _generate_alternator_cluster_with_stress(self, num_of_nodes: int = NUM_OF_NODES) -> StoppableThread:
        self._prepare_alternator_cluster(num_of_nodes=num_of_nodes)
        self._prefill_alternator_table()
        return self._run_alternator_stress()

    # #############################  Utils #########################################

    def test_drain_during_dynamo_load(self):
        get_items_thread = self._generate_alternator_cluster_with_stress()
        node = self.cluster.nodelist()[-1]
        debug('Start drain for: {}'.format(node.name))
        node.drain()
        debug('Drain finished')
        get_items_thread.join()

    def test_decommission_during_dynamo_load(self):
        alternator_consistent_stress = self._generate_alternator_cluster_with_stress()
        node1, node2, node3 = self.cluster.nodelist()
        debug('Start first decommission during consistent Alternator-load for: {}'.format(node2.name))
        node2.decommission()
        debug('Decommission finished')
        alternator_consistent_stress.join()
        alternator_non_consistent_stress = self._run_alternator_stress(consistent_read=False)
        debug('Start a second decommission during non-consistent alternator-load for: {}'.format(node3.name))
        node3.decommission()
        debug('Decommission finished')
        alternator_non_consistent_stress.join()

        debug("Check that the correct error is returned for a consistent read where cluster has no quorum")
        try:
            get_table_items(table=self._get_dynamodb_client(node=node1).Table(TABLE_NAME), num_of_items=10,
                            consistent_read=True)
            self.fail(msg="Expected ClientError for Alternator query.")
        except boto3_exceptions.ClientError as query_exp:
            self.assertIn('Cannot achieve consistency level for cl LOCAL_QUORUM',
                          query_exp.response['Error']['Message'], msg=query_exp)
            self.assertIn('Internal Server Error', query_exp.response['Error']['Code'], msg=query_exp)

        debug("Check that the correct error is returned for a resource of a decommissioned node")
        dynamodb_node2 = self._get_dynamodb_client(node=node2)
        self.node2_resource_table = dynamodb_node2.Table(TABLE_NAME)
        with self.assertRaisesRegexp(boto3_exceptions.EndpointConnectionError, "Could not connect to the endpoint URL"):
            get_table_items(table=self.node2_resource_table, num_of_items=10, consistent_read=True)

    def test_dynamo_reads_after_repair(self):
        self._prepare_alternator_cluster(num_of_nodes=3)
        node1, node2, node3 = self.cluster.nodelist()
        debug(f"Adding data for all nodes except {node2.name}...")
        node2.flush()
        debug(f"Stopping {node2.name}")
        node2.stop(wait_other_notice=True)
        self._prefill_alternator_table(node=node1)
        debug(f"Starting {node2.name}")
        node2.start(wait_other_notice=True, wait_for_binary_proto=True)
        debug(f"starting repair on {node2.name}...")
        info = node2.repair()
        debug(info[0])
        debug(info[1])
        node2_resource = self._get_dynamodb_client(node=node2)
        node2_table = node2_resource.Table(TABLE_NAME)
        debug(f"Reading Alternator queries from node {node2.name}")
        get_table_items(table=node2_table, num_of_items=NUM_OF_ITEMS, consistent_read=False)

    def test_dynamo_queries_on_multi_dc(self):
        self._prepare_alternator_cluster(num_of_nodes=3, is_multi_dc=True)
        dc1_node = self.cluster.nodelist()[0]
        debug(f"Writing Alternator queries to node {dc1_node.name} on data-center {dc1_node.data_center}")
        self._prefill_alternator_table(node=dc1_node)
        dc2_node = [node for node in self.cluster.nodelist() if node.data_center != dc1_node.data_center][0]
        dc2_node_resource = self._get_dynamodb_client(node=dc2_node)
        dc2_node_table = dc2_node_resource.Table(TABLE_NAME)
        debug(f"Reading Alternator queries from node {dc2_node.name} on data-center {dc2_node.data_center}")
        get_table_items(table=dc2_node_table, num_of_items=NUM_OF_ITEMS, consistent_read=False)
