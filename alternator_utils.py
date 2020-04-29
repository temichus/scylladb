import os
import shutil
import threading
from collections import namedtuple
from concurrent.futures import ThreadPoolExecutor
from typing import List, Dict

import boto3
from ccmlib.scylla_node import ScyllaNode
from deepdiff import DeepDiff
from nose.plugins.attrib import attr

from dtest import debug, Tester, info

AlternatorApi = namedtuple("AlternatorApi", ["resource", "client"])
ALTERNATOR_SNAPSHOT_FOLDER = os.path.join(os.getcwd(), "alternator", "snapshot")
TABLE_NAME = 'user_table'
NUM_OF_NODES = 3
NUM_OF_ITEMS = 100
ALTERNATOR_PORT = 8080


class StoppableThread:
    """Thread class with a stop() method. it runs the given "target" function in a loop
        until the 'stop-event' is set."""

    def __init__(self, target, kwargs=None):
        self._stop_event = threading.Event()
        self.target = target
        self.target_name = target.__name__
        self.kwargs = kwargs or {}
        self.pool = ThreadPoolExecutor(max_workers=1)
        self.future = None

    def stop(self):
        self._stop_event.set()

    def join(self):
        self.stop()
        return self.future.result()

    def start(self):
        self.future = self.pool.submit(self.run)

    def run(self):
        while not self._stop_event.is_set():
            debug(f"Running {self.target_name}...")
            self.target(**self.kwargs)
            debug(f"{self.target_name} is completed!")
        debug(f"{self.target_name} is stopped!")


@attr('dtest-full')
class TesterAlternator(Tester):

    def __init__(self, *argv, **kwargs):
        super().__init__(*argv, **kwargs)
        self._nodes_url_list = None
        self.keyspace_name_template = "alternator_{}"
        self._table_pk = "key"
        self._dynamo_params = dict(service_name="dynamodb", aws_access_key_id="None", aws_secret_access_key="None",
                                   region_name="None")
        self.alternator_apis = {}

    def get_dynamodb_api(self, node: ScyllaNode) -> AlternatorApi:
        return self.alternator_apis[node.name]

    def prepare_cluster(self, num_of_nodes: int = NUM_OF_NODES, is_multi_dc: bool = False) -> None:
        cluster_type = "single DC" if not is_multi_dc else "multi DC"
        debug(f"Populating a cluster with {num_of_nodes} nodes for {cluster_type}..")
        cluster = self.cluster
        cluster.set_configuration_options(
            {"start_native_transport": True, "experimental": True, "alternator_port": ALTERNATOR_PORT})
        cluster.populate([num_of_nodes, num_of_nodes] if is_multi_dc else num_of_nodes)
        debug("Starting cluster..")
        cluster.start(wait_for_binary_proto=True, wait_other_notice=True)
        for node_idx, node in enumerate(self.cluster.nodelist()):
            node_alternator_address = f"http://{self.get_ip_from_node(node=node)}:{ALTERNATOR_PORT}"
            self.alternator_apis[node_idx] = self.alternator_apis[node.name] = AlternatorApi(
                resource=boto3.resource(endpoint_url=node_alternator_address, **self._dynamo_params),
                client=boto3.client(endpoint_url=node_alternator_address, **self._dynamo_params)
            )

    # pylint:disable=too-many-arguments
    def create_table(self, table_name: str, node: ScyllaNode, key_schema: List[Dict[str, str]] = None,
                     attribute_definitions: List[Dict[str, str]] = None, wait_until_table_exists: bool = True):
        dynamodb_api = self.get_dynamodb_api(node=node)
        debug(f"Creating a new table '{table_name}' for node '{node.name}'..")
        dynamodb_api.resource.create_table(
            TableName=table_name,
            KeySchema=key_schema or [{"AttributeName": self._table_pk, "KeyType": "HASH"}],
            AttributeDefinitions=attribute_definitions or [
                {"AttributeName": self._table_pk, "AttributeType": "S"}, ],
            BillingMode="PAY_PER_REQUEST",
        )
        if wait_until_table_exists:
            waiter = dynamodb_api.client.get_waiter('table_exists')
            waiter.wait(TableName=table_name)
        info(f"The table '{table_name}' successfully created..")

    def delete_table_items(self, table_name: str, node: ScyllaNode, items: List[Dict[str, str]], primary_key: str = None
                           ) -> None:
        dynamodb_api = self.get_dynamodb_api(node=node)
        table = dynamodb_api.resource.Table(name=table_name)
        primary_key = primary_key or self._table_pk
        with table.batch_writer() as batch:
            for item in items:
                batch.delete_item(Key={primary_key: item[primary_key]})
        debug(f"Executing flush on node '{node.name}'")
        node.flush()
        info(f"All items of table '{table_name}' successfully removed..")

    def delete_table(self, table_name: str, node: ScyllaNode) -> None:
        node_ks_path = self.get_table_folder(table_name=table_name, node=node)
        dynamodb_api = self.get_dynamodb_api(node=node)
        table = dynamodb_api.resource.Table(name=table_name)
        debug(f"Removing table '{table_name}'")
        table.delete()
        waiter = dynamodb_api.client.get_waiter('table_not_exists')
        waiter.wait(TableName=table_name)
        debug(f"Removing table keyspace folder '{node_ks_path}' from node '{node.name}'")
        shutil.rmtree(path=node_ks_path)

    def create_items(self, primary_key: str = None, items: List[Dict[str, str]] = None,
                     num_of_items: int = NUM_OF_ITEMS) -> List[Dict[str, str]]:
        items = items or num_of_items
        primary_key = primary_key or self._table_pk
        if isinstance(items, int):
            if items < 1:
                raise ValueError("The number of items should be greater from 1")
            items = [{primary_key: f"test{item_idx}", "x": {"hello": f"world{item_idx}"}, }
                     for item_idx in range(0, items)]
        return items

    # pylint:disable=too-many-arguments
    def generate_request_items(self, table_name: str, node: ScyllaNode, items: List[Dict[str, str]] = None,
                               num_of_items: int = NUM_OF_ITEMS, primary_key: str = None) -> None:
        dynamodb_api = self.get_dynamodb_api(node=node)
        items = self.create_items(primary_key=primary_key, items=items, num_of_items=num_of_items)
        debug(f"Generating '{len(items)}' items for table '{table_name}'..")
        with dynamodb_api.resource.Table(name=table_name).batch_writer() as batch:
            for item in items:
                batch.put_item(item)

    def scan_table(self, table_name: str, node: ScyllaNode) -> list:
        dynamodb_api = self.get_dynamodb_api(node=node)
        result, still_running_while = [], True
        table = dynamodb_api.resource.Table(name=table_name)
        while still_running_while:
            response = table.scan()
            result.extend(response["Items"])
            still_running_while = 'LastEvaluatedKey' in response

        return result

    def is_table_exists(self, table_name: str, node: ScyllaNode) -> bool:
        dynamodb_api = self.get_dynamodb_api(node=node)
        is_table_exists = table_name in dynamodb_api.client.list_tables()["TableNames"]
        debug(f"The table '{table_name}'{'' if is_table_exists else 'not'} exists in node {node.name}..")
        return is_table_exists

    def get_table_folder(self, table_name: str, node: ScyllaNode) -> str:
        node_data_folder_path = os.path.join(node.get_path(), "data")
        table_folder_name = next((name for name in os.listdir(node_data_folder_path) if name.endswith(table_name)),
                                 None)
        if table_folder_name is None:
            raise FileNotFoundError(f"The folder of table '{table_name}' not found in following path "
                                    f"'{node_data_folder_path}'")
        table_folder_path = os.path.join(node_data_folder_path, table_folder_name)
        scylla_table_files = next((name for name in os.listdir(table_folder_path) if name.startswith(table_name)), None)
        if scylla_table_files is None:
            raise FileNotFoundError(f"The folder that contain Scylla files for table '{table_name}' not found in"
                                    f" following path '{scylla_table_files}'")
        return os.path.join(table_folder_path, scylla_table_files)

    def create_snapshot(self, table_name: str, snapshot_folder: str, node: ScyllaNode) -> None:
        keyspace = self.keyspace_name_template.format(table_name)
        debug(f"Making Alternator snapshot for node '{node.name}'..")
        debug(node.nodetool(f"snapshot {keyspace} -t {table_name} "))
        node_table_folder_path = self.get_table_folder(table_name=table_name, node=node)
        node_snapshot_folder_path = os.path.join(node_table_folder_path, "snapshots", table_name)

        debug(f"Creating local snapshot folder in following path '{snapshot_folder}' and moving all snapshot files to"
              f" this folder..")
        for file_name in os.listdir(node_snapshot_folder_path):
            shutil.copyfile(src=os.path.join(node_snapshot_folder_path, file_name),
                            dst=os.path.join(snapshot_folder, file_name))

    def load_snapshot_and_refresh(self, table_name: str, node: ScyllaNode, snapshot_folder: str = ""):
        keyspace_folder_path = self.get_table_folder(table_name=table_name, node=node)
        snapshot_folder = snapshot_folder or os.path.join(keyspace_folder_path, "snapshots", table_name)
        upload_folder = os.path.join(keyspace_folder_path, "upload")
        if not os.path.exists(path=snapshot_folder):
            raise NotADirectoryError(f"The snapshot folder '{snapshot_folder}' not exists")
        if not len(os.listdir(snapshot_folder)):
            raise IsADirectoryError(f"The snapshot folder '{snapshot_folder}' not contain any files")

        debug(f"Loading snapshot files from folder '{snapshot_folder}' to '{upload_folder}'..")
        for file_name in os.listdir(snapshot_folder):
            shutil.copyfile(src=os.path.join(snapshot_folder, file_name),
                            dst=os.path.join(upload_folder, file_name))

        refresh_cmd = f"refresh -- {self.keyspace_name_template.format(table_name)} {table_name}"
        debug(f"Running following refresh cmd '{refresh_cmd}'..")
        node.nodetool(refresh_cmd)

    def compare_table_data(self, table_name: str, table_data: List[Dict[str, str]], node: ScyllaNode) -> DeepDiff:
        data = self.scan_table(table_name=table_name, node=node)
        return DeepDiff(t1=table_data, t2=data, ignore_order=True)

    def run_stress(self, table_name: str, node: ScyllaNode, num_of_item: int = NUM_OF_ITEMS,
                   verbose: bool = True, consistent_read: bool = True) -> StoppableThread:
        debug("Start Alternator stress..")
        get_items_thread = StoppableThread(target=self.get_table_items, kwargs=dict(
            table_name=table_name, node=node, num_of_items=num_of_item, verbose=verbose,
            consistent_read=consistent_read))

        self.addCleanup(get_items_thread.join)
        get_items_thread.start()
        return get_items_thread

    def get_table_items(self, table_name: str, node: ScyllaNode, num_of_items: int = NUM_OF_ITEMS,
                        verbose: bool = True, consistent_read: bool = True):
        dynamodb_api = self.get_dynamodb_api(node=node)
        table = dynamodb_api.resource.Table(name=table_name)
        debug(f"Starting queries of: {num_of_items} items with ConsistentRead = {consistent_read}")
        if verbose:
            debug("First Item in range: {}".format(
                table.get_item(ConsistentRead=consistent_read, Key={'key': 'test0'})['Item']))
            debug("Last Item in range: {}".format(
                table.get_item(ConsistentRead=consistent_read, Key={'key': f'test{num_of_items - 1}'})['Item']))

        for idx in range(num_of_items):
            table.get_item(ConsistentRead=consistent_read, Key={'key': f'test{idx}'})
