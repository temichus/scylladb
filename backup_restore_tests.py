import threading
import time
import random
from Queue import Queue

from cassandra import Unavailable,ConsistencyLevel
from cassandra.query import SimpleStatement
from cassandra.cluster import NoHostAvailable
from ccmlib.node import NodeError
from cassandra.concurrent import execute_concurrent_with_args

from dtest import Tester, debug
from tools import new_node
from scylla_tools import insert_c1c2, query_c1c2_concurrent

class TestBackupRestore(Tester):
    def failure_durring_snapshot_no_corrupt_data(self):
        """
        Check that we can recover from a failure durring snapshot:

        1. Use a single node and create a keyspace + table
        2. Insert data
        3. Start create snapshot
        4. Kill node
        5. Start node
        6. Check that all data exists

        """
        cluster = self.cluster

        # Disable hinted handoff and set batch commit log so this doesn't
        # interfer with the test (this must be after the populate)
        cluster.set_configuration_options(values={'hinted_handoff_enabled': False}, batch_commitlog=True)
        debug("Starting a cluster of one node...")
        cluster.populate(1).start()
        node1 = cluster.nodelist()[0]

        debug("Creating a CQL connection...")
        session = self.patient_cql_connection(node1)

        debug("Creating a keyspace 'ks'...")
        self.create_ks(session, 'ks', 1)

        debug("Creating a column family 'cf'...")
        self.create_cf(session, 'cf', read_repair=0.0, columns={'c1': 'text', 'c2': 'text'})

        num_keys  = 1000
        c1_values = map(lambda x: '{}'.format(x), range(num_keys))
        c2_values = map(lambda x: '{}'.format(x), range(num_keys, 2 * num_keys))
        keys      = range(num_keys)

        debug("Inserting concurrently {} keys...".format(num_keys))
        insert_c1c2(session, keys=keys, consistency=ConsistencyLevel.ONE,
                    c1_values=c1_values, c2_values=c2_values)

        def run(name, q):
            global snapshot_failed

            debug("Starting a snapshot...")
            try:
                q.put(True)
                node1.nodetool('snapshot -t testsnapshot')
                debug("Snapshot done")
            except:
                debug("Snapshot has failed")

        queue = Queue()
        snapshot_thread = threading.Thread(target=run, args=("Thread-1", queue))
        snapshot_thread.start()
        random.seed()
        wait_time = random.random()

        queue.get(block=True)

        debug("Wait for {} seconds".format(wait_time))
        time.sleep(wait_time)

        debug("Killing node1...")
        node1.stop(gently=False)

        snapshot_thread.join()

        debug("Restarting node1...")
        node1.start(wait_for_binary_proto=True)

        debug("Checking rows on node1...")
        self.check_rows_on_node(node1, num_keys, found=keys, c1_values=c1_values, c2_values=c2_values)

    def check_rows_on_node(self, node_to_check, rows, found=None, missings=None, c1_values=None, c2_values=None):
        s = self.patient_cql_connection(node_to_check, 'ks')
        query="SELECT COUNT(*) FROM cf"
        statement = SimpleStatement(query)
        result = list(s.execute(statement))
        self.assertEqual(result[0].count, rows, len(result))

        if found is not None:
            if c1_values is None:
                c1_values = ['value1'] * len(found)

            if c2_values is None:
                c2_values = ['value2'] * len(found)

            query_c1c2_concurrent(session=s,
                                  keys=found,
                                  c1_values=c1_values,
                                  c2_values=c2_values,
                                  consistency=ConsistencyLevel.ONE)

        if missings is not None:
            query_c1c2_concurrent(session=s,
                                  keys=missings,
                                  consistency=ConsistencyLevel.ONE,
                                  must_be_missing=True)




