import os
import random
import shutil
import string
from enum import Enum

import threading
from concurrent.futures import ThreadPoolExecutor
from itertools import chain
from pprint import pformat
from typing import List, Dict, Union, NamedTuple, Optional

import boto3
from mypy_boto3_dynamodb import DynamoDBClient, DynamoDBServiceResource
from mypy_boto3_dynamodb.type_defs import AttributeValueTypeDef
from mypy_boto3_dynamodb.service_resource import Table
from deepdiff import DeepDiff
from nose.plugins.attrib import attr
from alternator.utils import schemas
from ccmlib.scylla_node import ScyllaNode
from dtest import debug, Tester, info, retrying

ALTERNATOR_SNAPSHOT_FOLDER = os.path.join(os.getcwd(), "alternator", "snapshot")
TABLE_NAME = 'user_table'
NUM_OF_NODES = 3
NUM_OF_ITEMS = 100
ALTERNATOR_PORT = 8080
ALTERNATOR_SECURE_PORT = 8043
DEFAULT_STRING_LENGTH = 5
# https://github.com/scylladb/scylla/issues/4480 - according Nadav the table name contains dash char and
# 32-byte UUID string -> 222 + 1 + 32 = 255 (The longest dynanodb's table name)
LONGEST_TABLE_SIZE = 222
SHORTEST_TABLE_SIZE = 3


class WriteIsolation(Enum):
    ALWAYS_USE_LWT = "always_use_lwt"
    FORBID_RMW = "forbid_rmw"
    ONLY_RMW_USES_LWT = "only_rmw_uses_lwt"
    UNSAFE_RMW = "unsafe_rmw"


class TableConf:
    """
    The dynamodb table meta data of schema and tags as seen by a table of a specific node resource
    """

    def __init__(self, table: DynamoDBServiceResource.Table):
        self.table = table
        self.describe = table.meta.client.describe_table(TableName=table.name)['Table']
        self.arn = self.describe['TableArn']
        self.tags = table.meta.client.list_tags_of_resource(ResourceArn=self.arn)['Tags']

    def update(self):
        self.describe = self.table.meta.client.describe_table(TableName=self.table.name)['Table']
        self.tags = self.table.meta.client.list_tags_of_resource(ResourceArn=self.arn)['Tags']
        debug(f'{self.table.name} {self.table.meta.client.meta.endpoint_url} tags: {self.tags}')
        debug(f'{self.table.name} {self.table.meta.client.meta.endpoint_url} describe: {self.describe}')

    def __eq__(self, other_table):
        self.update()
        other_table.update()
        if isinstance(other_table, self.__class__):
            return self.__dict__ == other_table.__dict__
        else:
            return False


def set_write_isolation(table: DynamoDBServiceResource.Table, isolation: Union[WriteIsolation, str]):
    isolation = isolation if not isinstance(isolation, WriteIsolation) else isolation.value
    table_conf = TableConf(table=table)
    tags = [
        {
            'Key': 'system:write_isolation',
            'Value': isolation
        }
    ]
    table.meta.client.tag_resource(ResourceArn=table_conf.arn, Tags=tags)
    table_conf.update()


class AlternatorApi(NamedTuple):
    resource: DynamoDBServiceResource
    client: DynamoDBClient


class Gsi:
    ATTRIBUTE_NAME = 'g_s_i'
    ATTRIBUTE_DEFINITION = {'AttributeName': ATTRIBUTE_NAME, 'AttributeType': 'S'}
    NAME = f'hello_{ATTRIBUTE_NAME}'
    CONFIG = dict(
        GlobalSecondaryIndexes=[
            {'IndexName': NAME,
             'KeySchema': [
                 {'AttributeName': ATTRIBUTE_NAME, 'KeyType': 'HASH'},
             ],
             'Projection': {'ProjectionType': 'ALL'}
             }
        ]
    )


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
        self._table_primary_key = schemas.HASH_KEY_NAME
        self._table_primary_key_format = "test{}"
        self._dynamo_params = dict(service_name="dynamodb", aws_access_key_id="None", aws_secret_access_key="None",
                                   region_name="None", verify=False)
        self.alternator_apis = {}

    def _add_api_for_node(self, node: ScyllaNode, is_encrypted: bool = False) -> None:
        if is_encrypted:
            node_alternator_address = f"https://{self.get_ip_from_node(node=node)}:{ALTERNATOR_SECURE_PORT}"
        else:
            node_alternator_address = f"http://{self.get_ip_from_node(node=node)}:{ALTERNATOR_PORT}"
        self.alternator_apis[node.name] = AlternatorApi(
            resource=boto3.resource(endpoint_url=node_alternator_address, **self._dynamo_params),
            client=boto3.client(endpoint_url=node_alternator_address, **self._dynamo_params)
        )

    def get_dynamodb_api(self, node: ScyllaNode) -> AlternatorApi:
        if node.name not in self.alternator_apis:
            self._add_api_for_node(node=node)
        return self.alternator_apis[node.name]

    def prepare_dynamodb_cluster(self, num_of_nodes: int = NUM_OF_NODES, is_multi_dc: bool = False,
                                 is_encrypted: bool = False, extra_config: Optional[dict] = None) -> None:
        cluster_config = {
            "start_native_transport": True,
            "alternator_port": ALTERNATOR_PORT,
            "alternator_write_isolation": "always"
        }
        cluster_type = "single DC" if not is_multi_dc else "multi DC"
        debug(f"Populating a cluster with {num_of_nodes} nodes for {cluster_type}..")
        if is_encrypted:
            cert_file, key_file = self.create_self_signed_x509_certificate()
            cluster_config['alternator_encryption_options'] = {
                'certificate': cert_file,
                'keyfile': key_file,
            }
            cluster_config.pop('alternator_port')
            cluster_config['alternator_https_port'] = ALTERNATOR_SECURE_PORT
        if extra_config is not None:
            cluster_config.update(extra_config)
        cluster = self.cluster
        cluster.set_configuration_options(cluster_config)
        cluster.populate([num_of_nodes, num_of_nodes] if is_multi_dc else num_of_nodes)
        debug("Starting cluster..")
        cluster.start(wait_for_binary_proto=True, wait_other_notice=True)
        for node in self.cluster.nodelist():
            self._add_api_for_node(node=node, is_encrypted=is_encrypted)

    # pylint:disable=too-many-arguments
    def create_table(self, node: ScyllaNode, table_name: str = TABLE_NAME,
                     schema: Union[tuple, Dict] = schemas.HASH_SCHEMA,
                     wait_until_table_exists: bool = True,
                     create_gsi: bool = False, **kwargs) -> Table:
        if isinstance(schema, tuple):
            schema = dict(schema)
        if create_gsi:
            schema['AttributeDefinitions'].append(Gsi.ATTRIBUTE_DEFINITION)
            schema.update(Gsi.CONFIG)
        dynamodb_api = self.get_dynamodb_api(node=node)
        debug(f"Creating a new table '{table_name}' using node '{node.name}'..")
        table = dynamodb_api.resource.create_table(
            TableName=table_name,
            BillingMode="PAY_PER_REQUEST",
            **schema,
            **kwargs
        )
        if wait_until_table_exists:
            waiter = dynamodb_api.client.get_waiter('table_exists')
            waiter.wait(TableName=table_name)
        info(f"The table '{table_name}' successfully created..")
        response = dynamodb_api.client.describe_table(TableName=table_name)
        debug(f"Table's schema and configuration are: {response}")
        return table

    def delete_table_items(self, table_name: str, node: ScyllaNode, items: List[Dict[str, str]],
                           schema: Union[tuple, Dict] = schemas.HASH_SCHEMA) -> None:
        dynamodb_api = self.get_dynamodb_api(node=node)
        table = dynamodb_api.resource.Table(name=table_name)
        table_keys = [key["AttributeName"] for key in schema[0][1]]
        with table.batch_writer() as batch:
            for item in items:
                batch.delete_item(Key={key: item[key] for key in table_keys})
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
        primary_key = primary_key or self._table_primary_key
        if isinstance(items, int):
            if items < 1:
                raise ValueError("The number of items should be greater from 1")
            items = [{primary_key: self._table_primary_key_format.format(item_idx), "x": {"hello": f"world{item_idx}"}, }
                     for item_idx in range(0, items)]
        return items

    # pylint:disable=too-many-arguments
    def batch_write_actions(self, table_name: str, node: ScyllaNode, new_items: List[Dict[str, str]] = None,
                            delete_items: List[Dict[str, str]] = None,
                            schema: Union[tuple, Dict] = schemas.HASH_SCHEMA):
        dynamodb_api = self.get_dynamodb_api(node=node)
        table_keys = [key["AttributeName"] for key in schema[0][1]]
        assert new_items or delete_items, "should pass new_items or delete_items, other it's a no-op"
        new_items, delete_items = new_items or [], delete_items or []
        if new_items:
            debug(f"Adding new {len(new_items)} items to table '{table_name}'..")
        if delete_items:
            debug(f"Deleting {len(delete_items)} items from table '{table_name}'..")

        table = dynamodb_api.resource.Table(name=table_name)
        with table.batch_writer() as batch:
            for item in new_items:
                batch.put_item(item)
            for item in delete_items:
                batch.delete_item({key: item[key] for key in table_keys})
        return table

    def update_items(self, table_name: str, node: ScyllaNode, items: List[Dict] = None,
                     primary_key: str = None) -> None:
        items = items or self.create_items(num_of_items=NUM_OF_ITEMS)
        dynamodb_api = self.get_dynamodb_api(node=node)
        primary_key = primary_key or self._table_primary_key

        debug(f"Updating '{len(items)}' items from table '{table_name}'..")
        table = dynamodb_api.resource.Table(name=table_name)
        for update_item in items:
            if "AttributeUpdates" in update_item:
                table.update_item(**update_item)
            else:
                table.update_item(**dict(
                    Key={primary_key: update_item[primary_key]}, AttributeUpdates={
                        key: dict(Value=value, Action="PUT") for key, value in update_item.items()
                        if key != primary_key}))

    def scan_table(self, table_name: str, node: ScyllaNode, threads_num: int = None,
                   consistent_read: bool = True, **kwargs) -> List[Dict[str, AttributeValueTypeDef]]:
        scan_result, is_parallel_scan = [], threads_num and threads_num > 0
        dynamodb_api = self.get_dynamodb_api(node=node)
        table = dynamodb_api.resource.Table(name=table_name)
        kwargs["ConsistentRead"] = consistent_read

        def _scan_table(part_scan_idx=None) -> List[Dict[str, AttributeValueTypeDef]]:
            parallel_params = {}

            if is_parallel_scan:
                parallel_params = {"TotalSegments": threads_num, "Segment": part_scan_idx}
                debug(f"Starting parallel scan part '{part_scan_idx + 1}' on table '{table_name}'")
            else:
                debug(f"Starting full scan on table '{table_name}'")

            response = table.scan(**parallel_params, **kwargs)
            result = response["Items"]
            while 'LastEvaluatedKey' in response:
                response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'], **parallel_params, **kwargs)
                result.extend(response["Items"])

            return result

        if is_parallel_scan:
            with ThreadPoolExecutor(max_workers=threads_num) as executor:
                threads = [executor.submit(_scan_table, part_idx) for part_idx in range(threads_num)]
                scan_result = [thread.result() for thread in threads]
            return list(chain(*scan_result)) if len(scan_result) > 1 else scan_result
        return _scan_table()

    def is_table_schema_synced(self, table_name: str, nodes: List[ScyllaNode]) -> bool:
        debug(f"Checking table {table_name} schema sync on nodes:")
        for node in nodes:
            debug(node.name)
        assert len(nodes) > 1, "A minimum of 2 nodes is required for checking schema sync."
        nodes_table_conf = [TableConf(self.get_table(table_name=table_name, node=node)) for node in nodes]
        for idx, table_conf in enumerate(nodes_table_conf[:-1]):
            if not table_conf == nodes_table_conf[idx+1]:
                return False
        return True

    def is_table_exists(self, table_name: str, node: ScyllaNode) -> bool:
        dynamodb_api = self.get_dynamodb_api(node=node)
        is_table_exists = True

        try:
            dynamodb_api.client.describe_table(TableName=table_name)
        except dynamodb_api.client.exceptions.ResourceNotFoundException:
            is_table_exists = False
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
        if not os.listdir(snapshot_folder):
            raise IsADirectoryError(f"The snapshot folder '{snapshot_folder}' not contain any files")

        debug(f"Loading snapshot files from folder '{snapshot_folder}' to '{upload_folder}'..")
        for file_name in os.listdir(snapshot_folder):
            shutil.copyfile(src=os.path.join(snapshot_folder, file_name),
                            dst=os.path.join(upload_folder, file_name))

        refresh_cmd = f"refresh -- {self.keyspace_name_template.format(table_name)} {table_name}"
        debug(f"Running following refresh cmd '{refresh_cmd}'..")
        node.nodetool(refresh_cmd)
        node.repair()

    def compare_table_data(self, expected_table_data: List[Dict[str, str]], table_name: str = None,
                           node: ScyllaNode = None, ignore_order: bool = True, consistent_read: bool = True,
                           table_data: List[Dict[str, str]] = None, **kwargs) -> DeepDiff:
        if not table_data:
            table_data = self.scan_table(table_name=table_name, node=node, ConsistentRead=consistent_read, **kwargs)
        return DeepDiff(t1=expected_table_data, t2=table_data, ignore_order=ignore_order,
                        ignore_numeric_type_changes=True)

    def _run_stress(self, table_name: str, node: ScyllaNode, target, num_of_item: int = NUM_OF_ITEMS,
                    **kwargs) -> StoppableThread:
        params = dict(table_name=table_name, node=node, num_of_items=num_of_item)
        for key, val in kwargs.items():
            params.update({key: val})
        stress_thread = StoppableThread(target=target, kwargs=params)

        self.addCleanup(stress_thread.join)
        debug(f"Start Alternator stress of {stress_thread.target_name}..\n Using parameters of: {stress_thread.kwargs}")
        stress_thread.start()
        return stress_thread

    def run_read_stress(self, table_name: str, node: ScyllaNode, num_of_item: int = NUM_OF_ITEMS,
                        verbose: bool = True, consistent_read: bool = True) -> StoppableThread:
        return self._run_stress(table_name=table_name, node=node, target=self.get_table_items, num_of_item=num_of_item,
                                verbose=verbose, consistent_read=consistent_read)

    def run_write_stress(self, table_name: str, node: ScyllaNode, num_of_item: int = NUM_OF_ITEMS) -> StoppableThread:
        return self._run_stress(table_name=table_name, node=node, target=self.put_table_items, num_of_item=num_of_item)

    def get_table(self, table_name: str, node: ScyllaNode):
        return self.get_dynamodb_api(node=node).resource.Table(name=table_name)

    def put_table_items(self, table_name: str, node: ScyllaNode, num_of_items: int = NUM_OF_ITEMS):
        items = self.create_items(num_of_items=num_of_items)
        self.batch_write_actions(table_name=table_name, node=node, new_items=items)

    def get_table_items(self, table_name: str, node: ScyllaNode, num_of_items: int = NUM_OF_ITEMS,
                        verbose: bool = True, consistent_read: bool = True):
        dynamodb_api = self.get_dynamodb_api(node=node)
        table: Table = dynamodb_api.resource.Table(name=table_name)
        debug(f"Starting queries of: {num_of_items} items with ConsistentRead = {consistent_read}")
        if verbose:
            debug("First Item in range: {}".format(
                table.get_item(ConsistentRead=consistent_read, Key={self._table_primary_key: 'test0'})['Item']))
            debug("Last Item in range: {}".format(
                table.get_item(ConsistentRead=consistent_read, Key={self._table_primary_key: f'test{num_of_items - 1}'})[
                    'Item']))

        for idx in range(num_of_items):
            table.get_item(ConsistentRead=consistent_read, Key={self._table_primary_key: f'test{idx}'})

    def prefill_dynamodb_table(self, node: ScyllaNode, table_name: str = TABLE_NAME, num_of_items: int = NUM_OF_ITEMS):
        self.create_table(table_name=table_name, node=node)
        new_items = self.create_items(num_of_items=num_of_items)
        return self.batch_write_actions(table_name=table_name, node=node, new_items=new_items)

    def run_scan_stress(self, table_name: str, node: ScyllaNode, items: List[Dict[str, str]] = None,
                        threads_num: int = None, is_compare_scan_result: bool = True) -> StoppableThread:
        items = items or self.create_items(num_of_items=NUM_OF_ITEMS)

        def full_scan():
            self.scan_table(table_name=table_name, node=node, threads_num=threads_num)
            debug("Verifying the scan result..")
            if not is_compare_scan_result:
                return
            self.compare_table_items_data(table_name=table_name, expected_items=items, node=node)

        debug("Creating Alternator scan stress..")
        scan_thread = StoppableThread(target=full_scan)
        self.addCleanup(scan_thread.stop)
        return scan_thread

    def run_delete_insert_update_item_stress(self, table_name: str, node: ScyllaNode):
        primary_key, total_items = "insert_stress_{}", 0

        def insert_item():
            nonlocal total_items
            sub_items_size = total_items // 3
            items = self.create_items(primary_key=primary_key, num_of_items=total_items)
            update_items = items[sub_items_size: sub_items_size * 2]
            if total_items % 2 == 0:
                delete_items = items[:sub_items_size]
                new_items = items[2 * sub_items_size:]
            else:
                delete_items = items[2 * sub_items_size:]
                new_items = items[:sub_items_size]
            if total_items % 25 == 0:
                debug(f"Updating '{len(update_items)}' existing items, creating '{len(new_items)}' new items and "
                      f"removing '{len(delete_items)}' items from table '{table_name}'..")

            with ThreadPoolExecutor(max_workers=3) as executor:
                executor.submit(fn=self.batch_write_actions, **dict(
                    table_name=table_name, node=node, primary_key=primary_key, new_items=new_items))
                executor.submit(fn=self.batch_write_actions, **dict(
                    table_name=table_name, node=node, primary_key=primary_key, delete_items=delete_items))
                executor.submit(fn=self.update_items, **dict(
                    table_name=table_name, node=node, items=update_items, primary_key=primary_key))
            total_items += 1

        debug("Creating Alternator scan stress..")
        insert_update_thread = StoppableThread(target=insert_item)
        self.addCleanup(insert_update_thread.stop)
        return insert_update_thread

    def get_item(self, node: ScyllaNode, item_key: Dict[str, AttributeValueTypeDef], table_name: str = TABLE_NAME,
                 consistent_read: bool = False):
        dynamodb_api = self.get_dynamodb_api(node=node)
        table: Table = dynamodb_api.resource.Table(name=table_name)
        debug(f'Getting item "{pformat(item_key)}" with ConsistentRead = "{consistent_read}"')
        response = table.get_item(Key=item_key, ConsistentRead=consistent_read)
        if response['ResponseMetadata']['HTTPStatusCode'] != 200:
            raise RuntimeError(f'The "get_item" of "{pformat(item_key)} is failed (full response is '
                               f'"{pformat(response)}")"')
        return response['Item']

    def put_item(self, node: ScyllaNode, item: Dict[str, AttributeValueTypeDef], table_name: str = TABLE_NAME):
        dynamodb_api = self.get_dynamodb_api(node=node)
        table: Table = dynamodb_api.resource.Table(name=table_name)
        debug(f'Adding new item "{pformat(item)}" ')
        response = table.put_item(Item=item)
        if response['ResponseMetadata']['HTTPStatusCode'] != 200:
            raise RuntimeError(f'The "put_item" of "{pformat(item)} is failed (full response is '
                               f'"{pformat(response)}")"')

    def update_item(self, node: ScyllaNode, item_key: Dict[str, AttributeValueTypeDef], table_name: str = TABLE_NAME):
        dynamodb_api = self.get_dynamodb_api(node=node)
        table: Table = dynamodb_api.resource.Table(name=table_name)
        debug(f'Updating item "{pformat(item_key)}"')
        response = table.update_item(Key=item_key)
        if response['ResponseMetadata']['HTTPStatusCode'] != 200:
            raise RuntimeError(f'The "update_item" of "{pformat(item_key)} is failed (full response is '
                               f'"{pformat(response)}")"')

    def delete_item(self, node: ScyllaNode, item_key: Dict[str, AttributeValueTypeDef], table_name: str = TABLE_NAME):
        dynamodb_api = self.get_dynamodb_api(node=node)
        table: Table = dynamodb_api.resource.Table(name=table_name)
        debug(f'Deleting item "{pformat(item_key)}"')
        response = table.delete_item(Key=item_key)
        if response['ResponseMetadata']['HTTPStatusCode'] != 200:
            raise RuntimeError(f'The "delete_item" of "{pformat(item_key)} is failed (full response is '
                               f'"{pformat(response)}")"')

    @retrying(num_attempts=10, sleep_time=1, allowed_exceptions=(AssertionError, ))
    def get_all_traces_events(self, expected_traces_size):
        result = []
        table_name_prefix = '.scylla.alternator.system_traces.'
        node = self.cluster.nodelist()[0]
        table: Table = self.get_dynamodb_api(node=node).resource.Table(name=f'{table_name_prefix}events')

        traces = self.scan_table(table_name=f'{table_name_prefix}sessions', node=node)
        if expected_traces_size > len(traces):
            raise AssertionError(f'"Expected at least "{expected_traces_size}" traces!')
        for trace in sorted(traces, key=lambda _trace: _trace['started_at']):
            session_id = trace['session_id']
            result.append(full_query(
                table=table, consistent_read=True, KeyConditionExpression='session_id = :s',
                ExpressionAttributeValues={':s': session_id}))
        return result


def random_string(length: int, chars=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for x in range(length))


def generate_put_request_items(num_of_items: int = NUM_OF_ITEMS, add_gsi: bool = False) -> List[
        Dict[str, Union[str, Dict[str, str]]]]:
    debug(f"Generating {num_of_items} put request items..")
    put_request_items = list()  # type: List[Dict[str, Union[str, Dict[str, str]]]]
    for idx in range(num_of_items):
        item = {
            schemas.HASH_KEY_NAME: f'test{idx}', 'other': random_string(length=DEFAULT_STRING_LENGTH),
            'x': {'hello': f'world{idx}'}
        }
        if add_gsi:
            item[Gsi.ATTRIBUTE_NAME] = random_string(length=1)
        put_request_items.append(item)
    return put_request_items


def full_query(table, consistent_read=True, **kwargs):
    """
    A dynamodb table query that can also be extended with parameters like 'KeyConditions'
    :param table:  the dynamodb table object to run query on
    :param consistent_read: Strongly consistent reads
    :param kwargs: for adding any other optional dynamodb params
    :return: A list of query result items.
    """
    response = table.query(**kwargs)
    items = response['Items']
    kwargs["ConsistentRead"] = consistent_read

    while 'LastEvaluatedKey' in response:
        response = table.query(ExclusiveStartKey=response['LastEvaluatedKey'], **kwargs)
        items.extend(response['Items'])
    return items
