import os
import random
import re
import shutil
import threading
import time
from Queue import Queue
from tools import new_node

from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement

from dtest import Tester, debug
from scylla_tools import insert_c1c2, query_c1c2_concurrent

class TestBackupRestore(Tester):
    def failure_durring_snapshot_no_corrupt_data_test(self):
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

        num_keys = 1000
        c1_values = map(lambda x: '{}'.format(x), range(num_keys))
        c2_values = map(lambda x: '{}'.format(x), range(num_keys, 2 * num_keys))
        keys = range(num_keys)

        debug("Inserting concurrently {} keys...".format(num_keys))
        insert_c1c2(session, keys=keys, consistency=ConsistencyLevel.ONE,
                    c1_values=c1_values, c2_values=c2_values)

        debug("Taking a snapshot...")
        self.start_nodetool_and_kill_node(node1, "snapshot -t testsnapshot")

        debug("Restarting node1...")
        node1.start(wait_for_binary_proto=True)

        debug("Checking rows on node1...")
        self.check_rows_on_node(node1, num_keys, found=keys, c1_values=c1_values, c2_values=c2_values)

    def failure_durring_restore_no_corrupt_data_test(self):
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

        num_keys = 1000
        c1_values = map(lambda x: '{}'.format(x), range(num_keys))
        c2_values = map(lambda x: '{}'.format(x), range(num_keys, 2 * num_keys))
        keys = range(num_keys)

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
        self.delete_cf_sstables(cf_dir)

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

    def replay_restore_no_additional_data_test(self):
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

        num_keys = 1000
        c1_values = map(lambda x: '{}'.format(x), range(num_keys))
        c2_values = map(lambda x: '{}'.format(x), range(num_keys, 2 * num_keys))
        keys = range(num_keys)

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
        self.delete_cf_sstables(cf_dir)

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

    def restore_snapshot_using_different_smp_setting_test(self):
        """
        Check that we can restore snapshot files that used a different smp setting

        1. Use a single node with smp=1 and create a keyspace + table
        2. Insert data
        3. Create snapshot and save files
        4. Drop keyspace
        5. Stop node, start it with smp=2
        6. Create keyspace + table
        7. Restore data
        8. Check that all data exists
        """
        cluster = self.cluster
        snapshot_name = 'testsnapshot'
        num_keys = 1000
        c1_values = map(lambda x: '{}'.format(x), range(num_keys))
        c2_values = map(lambda x: '{}'.format(x), range(num_keys, 2 * num_keys))
        keys = range(num_keys)

        # Disable hinted handoff and set batch commit log so this doesn't
        # interfere with the test (this must be after the populate)
        cluster.set_configuration_options(values={'hinted_handoff_enabled': False}, batch_commitlog=True)
        debug("Starting a cluster of one node on a single core...")
        cluster.populate(1).start(jvm_args=['--smp', '1'])
        node1 = cluster.nodelist()[0]

        debug("Creating a CQL connection...")
        session = self.patient_cql_connection(node1)

        debug("Creating a keyspace 'ks'...")
        self.create_ks(session, 'ks', 1)

        debug("Creating a column family 'cf'...")
        self.create_cf(session, 'cf', read_repair=0.0, columns={'c1': 'text', 'c2': 'text'})

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

        debug("Stopping the node...")
        node1.stop(gently=True)

        debug("Starting a node on two cores...")
        node1.start(wait_for_binary_proto=True, jvm_args=['--smp', '2'])

        debug("Creating a CQL connection...")
        session = self.patient_cql_connection(node1)

        debug("Creating the same keyspace.table...")
        self.create_ks(session, 'ks', 1)
        self.create_cf(session, 'cf', read_repair=0.0, columns={'c1': 'text', 'c2': 'text'})

        debug("Flushing a keyspace...")
        node1.nodetool("flush -- ks")

        ks_dir = os.path.join(self.test_path, 'test', 'node1', 'data', 'ks')
        cf_dir = self.get_non_snapshot_cf_dir(ks_dir, snapshot_name)
        debug("Column family directory is {}".format(cf_dir))

        debug("Removing sstables...")
        self.delete_cf_sstables(cf_dir)

        debug("Copy sstables from the snapshot...")
        for f in os.listdir(snapshot_dir):
            shutil.copy2(os.path.join(snapshot_dir, f), os.path.join(cf_dir, f))

        debug("Running 'nodetool refresh -- ks cf'")
        node1.nodetool("refresh -- ks cf")

        debug("Checking rows on node1...")
        self.check_rows_on_node(node1, num_keys, found=keys, c1_values=c1_values, c2_values=c2_values)

    def restore_snapshot_using_old_token_ownership_test(self):
        """
        Check that we can restore snapshot files that use a non updated token ownership

        1. Use a single node and create a keyspace + table
        2. Insert data
        3. Create snapshot and save files
        4. Add an additional node
        5. Drop keyspace
        6. Create keyspace + table
        7. Restore data
        8. Check that all data exists
        """
        cluster = self.cluster
        snapshot_name = 'testsnapshot'
        num_keys = 1000
        c1_values = map(lambda x: '{}'.format(x), range(num_keys))
        c2_values = map(lambda x: '{}'.format(x), range(num_keys, 2 * num_keys))
        keys = range(num_keys)

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

        debug("Inserting concurrently {} keys...".format(num_keys))
        insert_c1c2(session, keys=keys, consistency=ConsistencyLevel.ONE,
                    c1_values=c1_values, c2_values=c2_values)

        debug("Creating a snapshot...")
        node1.nodetool('snapshot -t {} -cf cf -- ks'.format(snapshot_name))

        snapshot_dir = self.get_snapshot_dir(snapshot_name)
        self.assertTrue(snapshot_dir is not None, "Can't find a snapshot directory for {}".format(snapshot_name))
        debug("Snapshot dir is {}".format(snapshot_dir))

        debug("Staring a new node (node2)...")
        node2 = new_node(cluster)
        node2.start(wait_for_binary_proto=True, wait_other_notice=True)

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
        self.delete_cf_sstables(cf_dir)

        debug("Copy sstables from the snapshot...")
        for f in os.listdir(snapshot_dir):
            shutil.copy2(os.path.join(snapshot_dir, f), os.path.join(cf_dir, f))

        debug("Running 'nodetool refresh -- ks cf'")
        node1.nodetool("refresh -- ks cf")

        debug("Check that we may query ks.cf on node1...")
        session.execute(SimpleStatement("SELECT COUNT(*) FROM ks.cf"))

    def incremental_backup_test(self):
        """
        Check that incremetal backup works as expected

        1. Use a single node
        2. Enable incremental_backup
        3. Create a keyspace + table
        4. Insert data
        5. Check that while sstables are flushed - incremental backups are created
        6. Run compact - forcing all sstables to be merged
        7. Check that backups holds all the old files and the new compacted file

        """
        cluster = self.cluster
        num_keys = 1000
        c1_values = map(lambda x: '{}'.format(x), range(num_keys))
        c2_values = map(lambda x: '{}'.format(x), range(num_keys, 2 * num_keys))
        keys = range(num_keys)

        # Disable hinted handoff and set batch commit log so this doesn't
        # interfere with the test (this must be after the populate)
        cluster.set_configuration_options(values={'hinted_handoff_enabled': False}, batch_commitlog=True)
        debug("Starting a cluster of one node...")
        cluster.populate(1).start()
        node1 = cluster.nodelist()[0]

        debug("Enabling incremental backups...")
        node1.nodetool("enablebackup")

        debug("Creating a CQL connection...")
        session = self.patient_cql_connection(node1)

        debug("Creating a keyspace 'ks'...")
        self.create_ks(session, 'ks', 1)

        debug("Creating a column family 'cf'...")
        self.create_cf(session, 'cf', read_repair=0.0, columns={'c1': 'text', 'c2': 'text'})

        debug("Inserting concurrently {} keys...".format(num_keys))
        insert_c1c2(session, keys=keys, consistency=ConsistencyLevel.ONE,
                    c1_values=c1_values, c2_values=c2_values)

        debug("Flushing...")
        node1.nodetool("flush -- ks cf")

        ks_dir = os.path.join(self.test_path, 'test', 'node1', 'data', 'ks')
        cf_dir = self.get_cf_dir(ks_dir, 'cf')
        debug("'cf' directory is {}".format(cf_dir))

        # Save the names of the current sstable files
        sstables_files1 = self.get_sstables_files(cf_dir, 'ks', 'cf')
        debug("sstables before compaction: {}".format(sstables_files1))

        # get the names of files in the 'backups' subdir
        backups1_files = self.get_sstables_files("{}/backups".format(cf_dir), 'ks', 'cf')
        debug("backups before compaction: {}".format(backups1_files))

        self.assertEqual(sstables_files1, backups1_files, "backup doesn't contain all sstable files")

        debug("Run a compaction...")
        node1.compact()

        sstables_files2 = self.get_sstables_files(cf_dir, 'ks', 'cf')
        debug("sstables after compaction: {}".format(sstables_files2))

        backups2_files = self.get_sstables_files("{}/backups".format(cf_dir), 'ks', 'cf')
        debug("backups after compaction: {}".format(backups2_files))

        self.assertEqual(sstables_files1 | sstables_files2, backups2_files, "backup after compaction doesn't contain all sstable files")

    def restore_snapshot_from_cassandra_test(self):
        """
        Check that we can restore snapshot files that have been created by cassandra

        1. Use a single node and create a keyspace + table
        2. Restore data from a cassandra snapshot
        3. Check that all data exists

        """
        cluster = self.cluster
        num_keys = 1000
        c1_values = map(lambda x: '{}'.format(x), range(num_keys))
        c2_values = map(lambda x: '{}'.format(x), range(num_keys, 2 * num_keys))
        keys = range(num_keys)

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

        debug("Flushing a keyspace...")
        node1.nodetool("flush -- ks")

        cassandra_snapshot_dir = "{}/cassandra-sstables/restore-snapshot-from-cassandra".format(os.path.dirname(os.path.realpath(__file__)))
        debug("cassandra snapshot dir is {}".format(cassandra_snapshot_dir))

        ks_dir = os.path.join(self.test_path, 'test', 'node1', 'data', 'ks')
        cf_dir = self.get_cf_dir(ks_dir, 'cf')
        debug("Column family directory is {}".format(cf_dir))

        debug("Removing sstables...")
        self.delete_cf_sstables(cf_dir)

        debug("Copy sstables from the snapshot...")
        for f in os.listdir(cassandra_snapshot_dir):
            shutil.copy2(os.path.join(cassandra_snapshot_dir, f), os.path.join(cf_dir, f))

        debug("Running 'nodetool refresh -- ks cf'")
        node1.nodetool("refresh -- ks cf")

        debug("Checking rows on node1...")
        self.check_rows_on_node(node1, num_keys, found=keys, c1_values=c1_values, c2_values=c2_values)

    def clearsnapshot_options_test(self):
        """
        Check different 'nodetool clearsnapshot' options

        1. Use a single node and create a keyspace ks0 + table cf.
        2. Create a keyspace ks1 + table cf.
        3. Insert data into both tables above.
        4. Create a snapshot snapshot0.
        5. Add more data to both keyspaces and create a snapshot snapshot1.
        6. Add more data to both keyspaces and create a snapshot snapshot2.
        7. Call 'nodetool clearsnapshot -t snapshot0'.
        8. Check that
            1. snapshot0 has been deleted in both keyspaces.
            2. snapshot1 and snapshot2 are still present and haven't been touched.
        9. Call 'nodetool clearsnapshot -t snapshot1 -- ks1' and check that
            1. snapshot1 has been removed from ks1 and not from ks0.
            2. snapshot2 is still present and hasn't been touched.
        10. Call 'nodetool clearsnapshot' and check that all snapshots have been removed.
        """
        cluster = self.cluster

        # Disable hinted handoff and set batch commit log so this doesn't
        # interfere with the test (this must be after the populate)
        cluster.set_configuration_options(values={'hinted_handoff_enabled': False}, batch_commitlog=True)
        debug("Starting a cluster of one node...")
        cluster.populate(1).start()
        node1 = cluster.nodelist()[0]

        debug("Creating a CQL connection...")
        session = self.patient_cql_connection(node1)

        for i in range(2):
            keyspace_name = 'ks{}'.format(i)
            debug("Creating a keyspace '{}'...".format(keyspace_name))
            self.create_ks(session, keyspace_name, 1)

            debug("Creating a column family 'cf'...")
            self.create_cf(session, 'cf', read_repair=0.0, columns={'c1': 'text', 'c2': 'text'})

        num_keys = 1000
        start_key = 0

        for i in range(3):
            snapshot_name = 'snapshot{}'.format(i)
            c1_values = map(lambda x: '{}'.format(x), range(start_key, start_key + num_keys))
            c2_values = map(lambda x: '{}'.format(x), range(start_key + num_keys, start_key + 2 * num_keys))
            keys = range(start_key, start_key + num_keys)

            debug("Inserting concurrently {} keys into 'ks0.cf' and 'ks1.cf'...".format(num_keys))
            insert_c1c2(session, ks='ks0', keys=keys, consistency=ConsistencyLevel.ONE,
                        c1_values=c1_values, c2_values=c2_values)
            insert_c1c2(session, ks='ks1', keys=keys, consistency=ConsistencyLevel.ONE,
                        c1_values=c1_values, c2_values=c2_values)

            debug("Creating a snapshot for 'ks0' and 'ks1'...")
            node1.nodetool('snapshot -t {} ks0 ks1'.format(snapshot_name))

            start_key = start_key + num_keys

        ks_dir = [None, None]
        for i in range(2):
            ks_dir[i] = os.path.join(self.test_path, 'test', 'node1', 'data', 'ks{}'.format(i))

        ks_snapshot_dir = [[None, None, None], [None, None, None]]

        for i in range(2):
            for j in [1, 2]:
                ks_snapshot_dir[i][j] = self.get_snapshot_dir('snapshot{}'.format(j), ks_dir=ks_dir[i])
                self.assertTrue(ks_snapshot_dir[i][j] is not None, "Can't find a snapshot directory for 'ks{}.snapshot{}'".format(i, j))

        ks_snapshot_files = [[None, None, None], [None, None, None]]
        for i in range(2):
            for j in [1, 2]:
                ks_snapshot_files[i][j] = self.get_all_files_in_dir(ks_snapshot_dir[i][j])

        debug("Call 'nodetool clearsnapshot -t snapshot0'...")
        node1.nodetool('clearsnapshot -t snapshot0')

        # First check that 'snapshot1' has been deleted...
        for i in range(2):
            debug("Check that snapshot0 for ks{} was deleted...".format(i))
            test_dir = self.get_snapshot_dir('snapshot0', ks_dir=ks_dir[i])
            self.assertTrue(test_dir is None, "'ks{}' snapshot 'snapshot0' has not been deleted!".format(i))

        # ...then check that other snapshots are untouched
        for i in range(2):
            for j in [1, 2]:
                debug("Check that snapshot{} for ks{} was not deleted...".format(j, i))
                test_dir = self.get_snapshot_dir('snapshot{}'.format(j), ks_dir=ks_dir[i])
                self.assertTrue(test_dir is not None, "'ks{}' snapshot 'snapshot{}' has not been deleted!".format(i, j))
                test_files = self.get_all_files_in_dir(ks_snapshot_dir[i][j])
                self.assertEqual(test_files, ks_snapshot_files[i][j], "'ks{}' snapshot 'snapshot{}' direcotry contents has changed!".format(i, j))

        # Call 'nodetool clearsnapshot -t snapshot1 -- ks1'
        debug("Call 'nodetool clearsnapshot -t snapshot1 -- ks1'")
        node1.nodetool('clearsnapshot -t snapshot1 -- ks1')

        # Check that snapshot1 for ks1 has been deleted...
        debug("Check that snapshot1 for ks1 was deleted...")
        test_dir = self.get_snapshot_dir('snapshot1', ks_dir=ks_dir[1])
        self.assertTrue(test_dir is None, "'ks1' snapshot 'snapshot1' has not been deleted!")

        # ...but not for ks0!
        debug("Check that snapshot1 for ks0 was not deleted...")
        test_dir = self.get_snapshot_dir('snapshot1', ks_dir=ks_dir[0])
        self.assertTrue(test_dir is not None, "'ks0' snapshot 'snapshot1' has been deleted!")
        test_files = self.get_all_files_in_dir(ks_snapshot_dir[0][1])
        self.assertEqual(test_files, ks_snapshot_files[0][1], "'ks0' snapshot 'snapshot1' direcotry contents has changed!")

        # ...then check that snapshot2 is intact
        for i in range(2):
            debug("Check that snapshot2 for ks{} was not deleted...".format(i))
            test_dir = self.get_snapshot_dir('snapshot2', ks_dir=ks_dir[i])
            self.assertTrue(test_dir is not None, "'ks{}' snapshot 'snapshot2' has not been deleted!".format(i))
            test_files = self.get_all_files_in_dir(ks_snapshot_dir[i][2])
            self.assertEqual(test_files, ks_snapshot_files[i][2], "'ks{}' snapshot 'snapshot2' direcotry contents has changed!".format(i))

        # Call 'nodetool clearsnapshot' and check that all snapshots has been cleared
        debug("Call 'nodetool clearsnapshot'")
        node1.nodetool('clearsnapshot')
        for i in range(3):
            debug("Check that snapshot{} doesn't exist any more...".format(i))
            test_dir = self.get_snapshot_dir('snapshot{}'.format(i))
            self.assertTrue(test_dir is None, "'snapshot{}' has not been deleted!".format(i))

# ######################## Helper functions ####################################
    def get_sstables_files(self, cf_dir, ks_name, cf_name):
        """
        Returns a set of sstable(s) files for a given KS and CF
        """
        sstable_pattern = re.compile("{}-{}-".format(ks_name, cf_name))
        sstables_files = set()
        for f in os.listdir(cf_dir):
            if sstable_pattern.match(f):
                sstables_files.add(f)

        return sstables_files

    def get_all_files_in_dir(self, dir_path):
        """
        Returs a set of all files in the given directory
        """
        dir_files = set()
        for f in os.listdir(dir_path):
            full_name = os.path.join(dir_path, f)
            if os.path.isfile(full_name):
                dir_files.add(f)

        return dir_files

    def delete_cf_sstables(self, cf_dir):
        for f in os.listdir(cf_dir):
            full_name = os.path.join(cf_dir, f)
            if os.path.isfile(full_name):
                os.remove(full_name)

    def get_snapshot_dir(self, snapshotname, ks_dir=None):
        search_base_dir = None
        if ks_dir is None:
            search_base_dir = self.test_path
        else:
            search_base_dir = ks_dir

        for root, dirs, files in os.walk(search_base_dir):
            for name in dirs:
                if name == snapshotname:
                    return os.path.join(root, name)

        return None

    def get_cf_dir(self, ks_dir, cf_name):
        """
        Return the first CF directory for a CF with a given name
        """
        cf_pattern = re.compile("{}-".format(cf_name))
        for root, dirs, files in os.walk(ks_dir):
            for d in dirs:
                if cf_pattern.match(d):
                    return os.path.join(root, d)

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




