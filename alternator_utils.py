from concurrent.futures import ThreadPoolExecutor
import threading
from dtest import debug

TABLE_NAME = 'user_table'
NUM_OF_ITEMS = 100


class StoppableThread():
    """Thread class with a stop() method. it runs the given "target" function in a loop
        until the 'stop-event' is set."""

    def __init__(self, target, kwargs=None):
        self._stop_event = threading.Event()
        self.target = target
        self.target_name = target.__name__
        self.kwargs = kwargs
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


def create_dynamodb_table(dynamodb_resource, table_name=TABLE_NAME):
    debug(f"Creating a table using a resource with node URL: {dynamodb_resource.meta.client.meta.endpoint_url}..")
    dynamodb_resource.create_table(
        AttributeDefinitions=[
            {
                'AttributeName': 'key',
                'AttributeType': 'S'
            },
        ],
        BillingMode='PAY_PER_REQUEST',
        TableName=table_name,
        KeySchema=[
            {
                'AttributeName': 'key',
                'KeyType': 'HASH'
            },
        ])


def generate_put_request_items(num_of_items: int = NUM_OF_ITEMS) -> list:
    debug(f"Generating {num_of_items} put request items..")
    put_request_items = []
    for idx in range(num_of_items):
        item = {
                    'key': f'test{idx}', 'x': {'hello': f'world{idx}'}
                }
        put_request_items.append(item)
    return put_request_items


def get_table_items(table, num_of_items: int = NUM_OF_ITEMS, verbose: bool = True, consistent_read: bool = True):
    debug(f"Starting queries of: {num_of_items} items with ConsistentRead = {consistent_read}")
    if verbose:
        debug("First Item in range: {}".format(
            table.get_item(ConsistentRead=consistent_read, Key={'key': 'test0'})['Item']))
        debug("Last Item in range: {}".format(
            table.get_item(ConsistentRead=consistent_read, Key={'key': f'test{num_of_items - 1}'})['Item']))

    for idx in range(num_of_items):
        table.get_item(ConsistentRead=consistent_read, Key={'key': f'test{idx}'})
