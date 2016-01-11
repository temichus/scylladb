# coding: utf-8

from dtest import Tester, debug
from unittest import skip

from tools import insert_c1c2, query_c1c2
from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement
import time


class RepairAdditionalTest(Tester):

    def check_rows_on_node(self, node_to_check, rows, found=None, missings=None, restart=True):
        if found is None:
            found = []
        if missings is None:
            missings = []
        stopped_nodes = []

        for node in self.cluster.nodes.values():
            if node.is_running() and node is not node_to_check:
                stopped_nodes.append(node)
                node.stop(wait_other_notice=True)

        session = self.patient_cql_connection(node_to_check, 'ks')
        result = list(session.execute("SELECT * FROM cf LIMIT %d" % (rows * 2)))
        self.assertEqual(len(result), rows, len(result))

        for k in found:
            query_c1c2(session, k, ConsistencyLevel.ONE)

        for k in missings:
            query = SimpleStatement("SELECT c1, c2 FROM cf WHERE key='k%d'" % k, consistency_level=ConsistencyLevel.ONE)
            res = list(session.execute(query))
            self.assertEqual(len(filter(lambda x: len(x) != 0, res)), 0, res)

        if restart:
            for node in stopped_nodes:
                node.start(wait_other_notice=True)

    def repair_disjoint_data_test(self):
        """
        On each of three replicas, insert completely different data.
        Confirm that repairing a single of these nodes brings all the data 
        to all three replicas.
        """
        debug("Starting cluster...");
        # Disable hinted handoff so it doesn't do what we expect repair to do
        self.cluster.set_configuration_options(values={'hinted_handoff_enabled': False})
        # Create a cluster of 3 nodes, and a keyspace with RF=3 on all nodes
        # (disable read repair, as we want to test the full repair).
        self.cluster.populate(3).start()
        node1, node2, node3 = self.cluster.nodelist()
        session = self.patient_cql_connection(node1);
        self.create_ks(session, 'ks', 3);
        self.create_cf(session, 'cf', read_repair=0.0, columns={'c1': 'text', 'c2': 'text'});

        # Insert 1000 keys *only* on node 1, another 1000 keys *only* on node 2,
        # another 1000 *only on node 3:
        debug("Adding data only on node 1...");
        node2.flush()
        node2.stop(wait_other_notice=True)
        node3.flush()
        node3.stop(wait_other_notice=True)
        session = self.patient_cql_connection(node1)
        session.set_keyspace('ks')
        insert_c1c2(session, keys=range(1000, 2000), consistency=ConsistencyLevel.ONE)
        self.cluster.flush()
        debug("Adding data only on node 2...")
        node2.start(wait_other_notice=True, wait_for_binary_proto=True)
        node1.flush()
        node1.stop(wait_other_notice=True)
        session = self.patient_cql_connection(node2)
        session.set_keyspace('ks')
        insert_c1c2(session, keys=range(2000, 3000), consistency=ConsistencyLevel.ONE)
        debug("Adding data only on node 3...")
        node3.start(wait_other_notice=True, wait_for_binary_proto=True)
        node2.flush()
        node2.stop(wait_other_notice=True)
        session = self.patient_cql_connection(node3)
        session.set_keyspace('ks')
        insert_c1c2(session, keys=range(3000, 4000), consistency=ConsistencyLevel.ONE)

        # Bring up all 3 nodes, each should have different data
        node1.start(wait_other_notice=True, wait_for_binary_proto=True)
        node2.start(wait_other_notice=True, wait_for_binary_proto=True)

        # Run repair on (arbitrarily), node 3
        time.sleep(10) # see CASSANDRA-4373
        debug("starting repair...")
        info=node3.repair(['ks'])
        debug(info[0])
        debug(info[1])

        # Check that all nodes have all data
        self.check_rows_on_node(node1, 3000)
        self.check_rows_on_node(node2, 3000)
        self.check_rows_on_node(node3, 3000)

    def repair_schema_test(self):
        """
        In a keyspace with two replicas, insert a new column family on one
        replica only (while the other node is down), and initiate repair from
        the node with the data. Verify that the data (and its schema) have been
        correctly replicated to the second node.
        """
        debug("Starting cluster...");
        # Start a cluster of two nodes, and create a keyspace with RF=2.
        # Do *not* create a table yet - we'll do that with one node down
        self.cluster.set_configuration_options(values={'hinted_handoff_enabled': False})
        self.cluster.populate(2).start()
        node1, node2 = self.cluster.nodelist()
        session = self.patient_cql_connection(node1);
        self.create_ks(session, 'ks', 2);

        # Take node2 down, and create a new table and data on node1 only.
        debug("Creating table and data only on node 1...");
        node2.flush()
        node2.stop(wait_other_notice=True)
        self.create_cf(session, 'cf', read_repair=0.0, columns={'c1': 'text', 'c2': 'text'});
        insert_c1c2(session, keys=range(1000, 2000), consistency=ConsistencyLevel.ONE)

        # At this point node2 is not only missing some data, it is actually
        # missing an entire table. Let's bring node2 back up, start repair on
        # node1, and see if node2 gets the new table, and all its data.
        node2.start(wait_other_notice=True, wait_for_binary_proto=True)
        time.sleep(10) # see CASSANDRA-4373
        debug("starting repair on node1...")
        info=node1.repair(['ks'])
        debug(info[0])
        debug(info[1])

        # Check that all nodes have all data
        debug("checking data on node1...")
        self.check_rows_on_node(node1, 1000)
        debug("checking data on node2...")
        self.check_rows_on_node(node2, 1000)

    def repair_schema_2_test(self):
        """
        In a keyspace with two replicas, insert a new column family on one
        replica only (while the other node is down), and initiate repair from
        the node *without* the data. Verify that the data (and its schema) have been
        correctly replicated to this node.
        The difference between this test and repair_schema_test is that this one
        starts the repair from the node *without* the table. This is a slightly
        harder test, because there is a risk our code will not try to repair the
        cf it doesn't know about.
        """
        debug("Starting cluster...");
        # Start a cluster of two nodes, and create a keyspace with RF=2.
        # Do *not* create a table yet - we'll do that with one node down
        self.cluster.set_configuration_options(values={'hinted_handoff_enabled': False})
        self.cluster.populate(2).start()
        node1, node2 = self.cluster.nodelist()
        session = self.patient_cql_connection(node1);
        self.create_ks(session, 'ks', 2);

        # Take node2 down, and create a new table and data on node1 only.
        debug("Creating table and data only on node 1...");
        node2.flush()
        node2.stop(wait_other_notice=True)
        self.create_cf(session, 'cf', read_repair=0.0, columns={'c1': 'text', 'c2': 'text'});
        insert_c1c2(session, keys=range(1000, 2000), consistency=ConsistencyLevel.ONE)

        # At this point node2 is not only missing some data, it is actually
        # missing an entire table. Let's bring node2 back up, start repair on
        # node2, and see if node2 gets the new table, and all its data.
        node2.start(wait_other_notice=True, wait_for_binary_proto=True)
        time.sleep(10) # see CASSANDRA-4373
        debug("starting repair on node1...")
        info=node2.repair(['ks'])
        debug(info[0])
        debug(info[1])

        # Check that all nodes have all data
        debug("checking data on node1...")
        self.check_rows_on_node(node1, 1000)
        debug("checking data on node2...")
        self.check_rows_on_node(node2, 1000)

    def repair_cell_update_test(self):
        """
        With data replicated on two nodes, update an existing partition on only
        one of these nodes (with the other node down). Then confirm that repair can
        fix this on the second node as well.
        """
        debug("Starting cluster and inserting data...");
        # Start a cluster of two nodes, and create a keyspace with RF=2, and
        # a table with one partition. Hinted handoff and read repair are disabled
        # so they don't fix the problems which repair is supposed to fix
        self.cluster.set_configuration_options(values={'hinted_handoff_enabled': False})
        self.cluster.populate(2).start()
        node1, node2 = self.cluster.nodelist()
        session = self.patient_cql_connection(node1);
        self.create_ks(session, 'ks', 2);
        self.create_cf(session, 'cf', read_repair=0.0, columns={'c1': 'text', 'c2': 'text'});
        query = SimpleStatement("INSERT INTO cf (key, c1, c2) VALUES ('key', 'hello', 'hi')", consistency_level=ConsistencyLevel.ALL)
        session.execute(query)

        # Bring down node2, and change the existing data on node 1
        debug("Bringing down node2 and updating data on node 1...");
        node2.flush()
        node2.stop(wait_other_notice=True)
        query = SimpleStatement("INSERT INTO cf (key, c1, c2) VALUES ('key', 'new', 'yo')", consistency_level=ConsistencyLevel.ONE)
        session.execute(query)

        # Confirm that node1 has new data, and (by bringing only node 2 up) that
        # node2 still has old data
        result = session.execute("SELECT * from cf")
        self.assertEqual(len(result), 1, len(result))
        self.assertEqual(result[0].key, 'key', result[0].key)
        self.assertEqual(result[0].c1, 'new', result[0].c1)
        self.assertEqual(result[0].c2, 'yo', result[0].c2)
        node2.start(wait_other_notice=True, wait_for_binary_proto=True)
        node1.flush()
        node1.stop(wait_other_notice=True)
        session = self.patient_cql_connection(node2, 'ks')
        result = session.execute("SELECT * from cf")
        self.assertEqual(len(result), 1, len(result))
        self.assertEqual(result[0].key, 'key', result[0].key)
        self.assertEqual(result[0].c1, 'hello', result[0].c1)
        self.assertEqual(result[0].c2, 'hi', result[0].c2)

        # Finally bring both nodes up, repair, and confirm (by bringing up only
        # node 2) that the data on node2 is now up to date.
        node1.start(wait_other_notice=True, wait_for_binary_proto=True)
        info=node2.repair(['ks'])
        debug(info[0])
        debug(info[1])
        node1.flush()
        node1.stop(wait_other_notice=True)
        session = self.patient_cql_connection(node2, 'ks')
        result = session.execute("SELECT * from cf")
        self.assertEqual(len(result), 1, len(result))
        self.assertEqual(result[0].key, 'key', result[0].key)
        self.assertEqual(result[0].c1, 'new', result[0].c1)
        self.assertEqual(result[0].c2, 'yo', result[0].c2)

    @skip ('unimplemented')
    def repair_of_cluster_all_nodes_are_out_of_sync(self):
        """
        Check that repair fixes all inconsistencies in data
        1. Create a cluster of 3 nodes with rf=3, disable read_repair, hinted_handoff
        2. Shutdown node 2,3
        3. Insert data set A
        4. Start node 2 Shutdown node 1
        5. Insert data set B (A != B)
        6. Start node 3 Shutdown node 2
        7. Insert data set C (A != B, B != C, A != C)
        8. Start node 1,2
        9. Run repair on node 3
        10. Shutdown node 1,2 check all data on node 3
        11. Shutdown node 1,3 check all data on node 2
        12. Shutdown node 2,3 check all data on node 1
        """
        fail

    @skip ('unimplemented')
    def full_repair_of_node_initiated_on_node_with_latest_data_test(self):
        """
        Check that repair transfers all the data in case non exists
        1. Create a cluster of 2 nodes with rf=2, disable read_repair, hinted_handoff
        2. Shutdown node 2 (prior to schema creation)
        3. Insert data
        4. Start node 2
        5. Run repair on node 1
        6. Shutdown node 1 - check that all data exists on node 2
        """
        fail

    @skip ('unimplemented')
    def full_repair_of_node_initiated_on_node_without_data(self):
        """
        Check that repair transfers all the data in case non exists
        1. Create a cluster of 2 nodes with rf=2, disable read_repair, hinted_handoff
        2. Shutdown node 2 (prior to schema creation)
        3. Insert data
        4. Start node 2
        5. Run repair on node 2
        6. Shutdown node 1 - check that all data exists on node 2
        """
        fail

    @skip ('unimplemented')
    def repair_fixes_updates_to_cells_test(self):
        """
        Check that repair fixes a few update to cells contents
        1. Create a cluster of 2 nodes with rf=2, disable read_repair, hinted_handoff
        2. Insert data
        3. Shutdown node 2
        4. Update some cells
        5. Start node 2
        6. Run repair on node 2
        7. Shutdown node 1 - check that all data exists on node 2
        """
        fail

    @skip ('unimplemented')
    def repair_fixes_remove_of_keys_test(self):
        """
        Check that repair fixes a few removed keys
        1. Create a cluster of 2 nodes with rf=2, disable read_repair, hinted_handoff
        2. Insert data
        3. Shutdown node 2
        4. Remove some keys
        5. Start node 2
        6. Run repair on node 2
        7. Shutdown node 1 - check that all data exists on node 2
        """
        fail

    @skip ('unimplemented')
    def repair_fixes_deletion_of_cells_test(self):
        """
        Check that repair fixes a deleteion of cells
        1. Create a cluster of 2 nodes with rf=2, disable read_repair, hinted_handoff
        2. Insert data
        3. Shutdown node 2
        4. Delete some cells
        5. Start node 2
        6. Run repair on node 2
        7. Shutdown node 1 - check that all data exists on node 2
        """
        fail

    @skip ('unimplemented')
    def repair_fixes_deletion_of_range_of_cells_test(self):
        """
        Check that repair fixes a deletion of cell range
        1. Create a cluster of 2 nodes with rf=2, disable read_repair, hinted_handoff
        2. Insert data
        3. Shutdown node 2
        4. Delete a range of some cells
        5. Start node 2
        6. Run repair on node 2
        7. Shutdown node 1 - check that all data exists on node 2
        """
        fail

    @skip ('unimplemented')
    def repair_fixes_update_of_ttl_test(self):
        """
        Check that repair fixes updates to ttl
        CQL: UPDATE table USING TTL <ttl value> where key=X
        1. Create a cluster of 2 nodes with rf=2, disable read_repair, hinted_handoff
        2. Insert data
        3. Shutdown node 2
        4. Update ttl of some cells
        5. Start node 2
        6. Run repair on node 2
        7. Shutdown node 1 - check that all data exists on node 2
        """
        fail

    @skip ('unimplemented')
    def fail_node_initiating_repair_test(self):
        """
        Check that killing a repaired node does not cause additional failures
        1. Create a cluster of 2 nodes with rf=2
        2. Stop node 2
        3. Insert data
        4. In a loop
           a. Start node 2
           b. Start repair
           c. Kill node 2
           d. Check that cluster is avilable (read/writes)
        """
        fail

    @skip ('unimplemented')
    def fail_node_responding_to_repair_test(self):
        """
        Check that killing a repairing node does not cause additional failures
        1. Create a cluster of 2 nodes with rf=2
        2. Stop node 2
        3. Insert data
        4. Start node 2
        5. In a loop
           a. Start node 1 if its down
           b. Start repair
           c. Kill node 1
           d. Check that cluster is avilable (read/writes)
        """
        fail

    @skip ('unimplemented')
    def repair_while_data_is_updated_test(self):
        """
        Check that data can be updated while repair is running
        1. Create a cluster of 2 nodes with rf=2
        2. Stop node 2
        3. Insert data
        4. Start node 2
        5. In a loop update part of data with CL=2
        6. Start repair
        7. Stop node 1
        8. Check that all the data is up to date
        """
        fail

    @skip ('unimplemented')
    def repair_while_nodes_are_down_1_test(self):
        """
        Check that repair is not able to complete if no replicas for data exist
        1. Create a cluster of 3 nodes with rf=2
        2. Stop node 2
        3. Insert data
        4. Stop node 3
        5. Start node 2
        6. Start repair on node 2
        7. Check if repair is succesfull
        """
        fail

    @skip ('unimplemented')
    def repair_while_nodes_are_down_2_test(self):
        """
        Check that repair is able to complete if one replica of data exists
        1. Create a cluster of 4 nodes with rf=3
        2. Stop node 2
        3. Insert data
        4. Stop node 3
        5. Start node 2
        6. Start repair on node 2
        7. Check if repair is succesfull
        """
        fail

    @skip ('unimplemented')
    def repair_while_new_node_is_added_test(self):
        """
        Check that repair is accompileshed while new node is added
        1. Create a cluster of 2 nodes with rf=2
        2. Stop node 2
        3. Insert data
        4. Start node 2
        6. Start repair
        7. Create a new node and start it
        """
        fail

    @skip ('unimplemented')
    def repair_while_node_is_decomissioned_test(self):
        """
        Check that repair is accompileshed while node is decomissioned
        1. Create a cluster of 3 nodes with rf=2
        2. Stop node 2
        3. Insert data
        4. Start node 2
        6. Start repair
        7. Decomission node 3
        8. Stop node 1 - does node 2 hold all the data
        """
        fail

    @skip ('unimplemented')
    def test_multiple_repair_test(self):
        """
        Check that repair is accompilshed when multiple repairs are initiated in parallel
        1. Create a cluster of 3 nodes with rf=3
        2. Insert data
        3. Stop node 2
        4. Insert data
        5. Stop node 3
        6. Insert data
        7. Start node 2, Start node 3
        8. Start repair on node 2, node 3
        9. Stop node 1,node 3 - does node 2 hold all the data
        10. Stop node 1,node 2 - does node 3 hold all the data
        """
        fail

