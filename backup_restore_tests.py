import os
import random
import re
import shutil
import threading
import time
from Queue import Queue

from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement

from dtest import Tester, debug
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

        debug("Taking a snapshot...")
        self.start_nodetool_and_kill_node(node1, "snapshot -t testsnapshot")

        debug("Restarting node1...")
        node1.start(wait_for_binary_proto=True)

        debug("Checking rows on node1...")
        self.check_rows_on_node(node1, num_keys, found=keys, c1_values=c1_values, c2_values=c2_values)

    def failure_durring_restore_no_corrupt_data(self):
        """
        Check that we can recover from a failure during restore

        1. Use a single node and create a keyspace + table
        2. Insert data
        3. Create snapshot and save files
        4. Drop keyspace
        5. Create keyspace + table + populate new data + drain
        6. Start restore data
        7. Kill node
        8. Start node
        9. Check that all data exists
        """
        cluster = self.cluster
        snapshot_name = 'testsnapshot'

        # Disable hinted handoff and set batch commit log so this doesn't
        # interfere with the test (this must be after the populate)
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

        debug("Creating a snapshot...")
        node1.nodetool('snapshot -t {} -cf cf -- ks'.format(snapshot_name))

        debug("Dropping a keyspace...")
        session.execute(SimpleStatement("DROP KEYSPACE ks"))

        debug("Creating the same keyspace.table with different content...")
        self.create_ks(session, 'ks', 1)
        self.create_cf(session, 'cf', read_repair=0.0, columns={'c1': 'text', 'c2': 'text'})
        insert_c1c2(session, keys=keys, consistency=ConsistencyLevel.ONE,
                    c1_values=c2_values, c2_values=c1_values)

        # sanity check
        self.check_rows_on_node(node1, num_keys, found=keys, c1_values=c2_values, c2_values=c1_values)

        debug("Draining the cluster...")
        node1.nodetool('drain')

        snapshot_dir = self.get_snapshot_dir(snapshot_name)
        self.assertTrue(snapshot_dir is not None, "Can't find a snapshot directory for {}".format(snapshot_name))
        debug("Snapshot dir is {}".format(snapshot_dir))

        ks_dir = os.path.join(self.test_path, 'test', 'node1', 'data', 'ks')

        #
        # As a result of 'DROP KEYSPACE' and the following 'CF CREATE' there
        # will be two directories for the 'cf' CF: one with the old UUID and one
        # with the new one.
        #
        # Since we can't get a UUID of the cf we will just look for the
        # CF directory without a snapshot we've created.
        #
        cf_dir = self.get_non_snapshot_cf_dir(ks_dir, snapshot_name)
        debug("Column family directory is {}".format(cf_dir))

        debug("Removing sstables...")
        for f in os.listdir(cf_dir):
            full_name = os.path.join(cf_dir, f)
            if os.path.isfile(full_name):
                os.remove(full_name)

        debug("Copy sstables from the snapshot...")
        for f in os.listdir(snapshot_dir):
            shutil.copy2(os.path.join(snapshot_dir, f), os.path.join(cf_dir, f))

        debug("Running 'nodetool refresh'...")
        self.start_nodetool_and_kill_node(node1, 'refresh -- ks cf')

        debug("Delete commitlogs...")
        commitlog_dir = os.path.join(self.test_path, 'test', 'node1', 'commitlogs')
        for f in os.listdir(commitlog_dir):
            os.remove(os.path.join(commitlog_dir, f))

        debug("Restart the node...")
        node1.start(wait_for_binary_proto=True)

        debug("Checking rows on node1...")
        self.check_rows_on_node(node1, num_keys, found=keys, c1_values=c1_values, c2_values=c2_values)

    def replay_restore_no_additional_data(self):
        """
        Check that we can restore snapshot files that use old schema

        1. Use a single node and create a keyspace + table
        2. Insert data
        3. Create snapshot and save files
        4. Drop keyspace
        5. Create keyspace + table
        6. Run 'nodetool refresh'
        7. Check that all data exists
        8. Run 'nodetool refresh'
        9. Check that all data exists
        """
        cluster = self.cluster
        snapshot_name = 'testsnapshot'

        # Disable hinted handoff and set batch commit log so this doesn't
        # interfere with the test (this must be after the populate)
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

        debug("Creating a snapshot...")
        node1.nodetool('snapshot -t {} -cf cf -- ks'.format(snapshot_name))

        snapshot_dir = self.get_snapshot_dir(snapshot_name)
        self.assertTrue(snapshot_dir is not None, "Can't find a snapshot directory for {}".format(snapshot_name))
        debug("Snapshot dir is {}".format(snapshot_dir))

        debug("Dropping a keyspace...")
        session.execute(SimpleStatement("DROP KEYSPACE ks"))

        debug("Creating the same keyspace.table...")
        self.create_ks(session, 'ks', 1)
        self.create_cf(session, 'cf', read_repair=0.0, columns={'c1': 'text', 'c2': 'text'})

        debug("Flushing a keyspace...")
        node1.nodetool("flush -- ks")

        ks_dir = os.path.join(self.test_path, 'test', 'node1', 'data', 'ks')
        cf_dir = self.get_non_snapshot_cf_dir(ks_dir, snapshot_name)
        debug("Column family directory is {}".format(cf_dir))

        debug("Removing sstables...")
        for f in os.listdir(cf_dir):
            full_name = os.path.join(cf_dir, f)
            if os.path.isfile(full_name):
                os.remove(full_name)

        debug("Copy sstables from the snapshot...")
        for f in os.listdir(snapshot_dir):
            shutil.copy2(os.path.join(snapshot_dir, f), os.path.join(cf_dir, f))

        debug("Running 'nodetool refresh -- ks cf' - first take...")
        node1.nodetool("refresh -- ks cf")

        debug("Checking rows on node1...")
        self.check_rows_on_node(node1, num_keys, found=keys, c1_values=c1_values, c2_values=c2_values)

        debug("Running 'nodetool refresh -- ks cf' - second take...")
        node1.nodetool("refresh -- ks cf")

        debug("Checking rows on node1...")
        self.check_rows_on_node(node1, num_keys, found=keys, c1_values=c1_values, c2_values=c2_values)

    def get_snapshot_dir(self, snapshotname):
        for root, dirs, files in os.walk(self.test_path):
            for name in dirs:
                if name == snapshotname:
                    return os.path.join(root, name)

        return None

    # Return the first CF directory that doesn't have a snapshot with a given tag
    def get_non_snapshot_cf_dir(self, ks_dir, snapshotname):
        for root, dirs, files in os.walk(ks_dir):
            for d in dirs:
                if not os.path.isdir(os.path.join(root, d, 'snapshots', snapshotname)):
                    return os.path.join(root, d)
            break

        return None

    def start_nodetool_and_kill_node(self, node, cmd):
        def run(name, q):
            global snapshot_failed

            debug("Starting nodetool {}...".format(cmd))
            try:
                q.put(True)
                node.nodetool(cmd)
                debug("nodetool {} done".format(cmd))
            except:
                debug("nodetool {} killed".format(cmd))

        queue = Queue()
        nodetool_thread = threading.Thread(target=run, args=("nodetool-thread", queue))
        nodetool_thread.start()
        random.seed()
        wait_time = random.random()

        queue.get(block=True)

        debug("Wait for {} seconds".format(wait_time))
        time.sleep(wait_time)

        debug("Killing a node...")
        node.stop(gently=False)

        nodetool_thread.join()

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




