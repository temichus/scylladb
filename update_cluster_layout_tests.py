import time
from collections import namedtuple

from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement

from dtest import Tester
from tools import insert_c1c2, query_c1c2, new_node
from ccmlib.node import NodeError
import threading



class TestUpdateClusterLayout(Tester):

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

        cursor = self.patient_cql_connection(node_to_check, 'ks')
        result = cursor.execute("SELECT * FROM cf LIMIT %d" % (rows * 2))
        self.assertEqual(len(result), rows, len(result))

        for k in found:
            query_c1c2(cursor, k, ConsistencyLevel.ONE)

        for k in missings:
            query = SimpleStatement("SELECT c1, c2 FROM cf WHERE key='k%d'" % k, consistency_level=ConsistencyLevel.ONE)
            res = cursor.execute(query)
            self.assertEqual(len(filter(lambda x: len(x) != 0, res)), 0, res)

        if restart:
           self.start_all_nodes()

    def start_all_nodes(self):
        nodes_marks = []
        for node in self.cluster.nodes.values():
            if node.is_running():
                nodes_marks.append((node,node.mark_log()))
            else:
                nodes_marks.append((node,None))

        for node in self.cluster.nodes.values():
            if not node.is_running():
                node.start(wait_other_notice=True,wait_for_binary_proto=True)

        for node,mark in nodes_marks:
            for other_node, _ in nodes_marks:
                if other_node is not node:
                    if mark:
                        node.watch_log_for_alive(other_node, from_mark=mark)
                    else:
                        node.watch_log_for_alive(other_node)

    def simple_add_node_1_test(self):
        """
        Test bootstrapped node streams all data
        1. Create a cluster with a single node with rf=2, insert data
        2. Add a new node 
        3. Check that each node has all the data
        """
        cluster = self.cluster

        # Disable hinted handoff and set batch commit log so this doesn't
        # interfer with the test (this must be after the populate)
        cluster.set_configuration_options(values={'hinted_handoff_enabled': False}, batch_commitlog=True)
        cluster.populate(1).start()
        node1 = cluster.nodelist()[0]

        cursor = self.patient_cql_connection(node1)
        self.create_ks(cursor, 'ks', 2)
        self.create_cf(cursor, 'cf', read_repair=0.0, columns={'c1': 'text', 'c2': 'text'})

        for i in xrange(0, 1000):
            insert_c1c2(cursor, i, ConsistencyLevel.ONE)

        node2 = new_node(cluster)
        node2.start(wait_for_binary_proto=True)

        session = self.patient_exclusive_cql_connection(node2)
        node1.watch_log_for_alive(node2)
        node2.watch_log_for_alive(node1)

        for i in xrange(1000, 2000):
            insert_c1c2(cursor, i, ConsistencyLevel.TWO)

        self.check_rows_on_node(node2, 2000)
        self.check_rows_on_node(node1, 2000)


    def simple_add_node_2_test(self):
        """
        We are using the row_cache to rvalue the number of entries in each node
        We do not yet support the nodetool cleanup operation that removes old data - yet the cache sould be cleared
        If the cache is not cleared then if a range is returned the data will be wroung

        Test bootstrapped node streams part of its data
        1. Create a cluster with a single node with rf=1,insert data
        2. Check the row cahe can be used as an estimator
        3. Add a new node 
        4. Check that all data can be read
        5. Check that the sum of cache entires on both nodes is logical
        """

        cluster = self.cluster

        # Disable hinted handoff and set batch commit log so this doesn't
        # interfer with the test (this must be after the populate)
        cluster.set_configuration_options(values={'hinted_handoff_enabled': False}, batch_commitlog=True)
        cluster.populate(1).start()
        node1 = cluster.nodelist()[0]

        cursor_node1 = self.patient_cql_connection(node1)
        self.create_ks(cursor_node1, 'ks', 1)
        cursor_node1.execute("""
            CREATE TABLE ks.cf (
                key varchar,
                c1 text,
                c2 text,
                PRIMARY KEY(key)
            ) WITH read_repair_chance = 0.0
            AND caching = { 'keys' : 'NONE', 'rows_per_partition' : '2000' };
        """)

        node1.flush()
        pre_insert = node1.row_cache_entries()

        for i in xrange(0, 1000):
            insert_c1c2(cursor_node1, i, ConsistencyLevel.ONE)

        node1.flush()
        node1_cache_entries = node1.row_cache_entries() - pre_insert
        self.assertEqual(node1_cache_entries,1000,"node1 cache %d expected 1000" % node1_cache_entries)
 
        # We booted the new node and it got part of the items
        node2 = new_node(cluster)
        node2.start(wait_for_binary_proto=True)

        cursor_node2 = self.patient_exclusive_cql_connection(node2)
        node1.watch_log_for_alive(node2)
        node2.watch_log_for_alive(node1)

        result = cursor_node1.execute("SELECT * FROM ks.cf")
        self.assertEqual(len(result),1000,"expected 1000 lines got %d" % len(result))

        # We are flushing on all nodes - to update the cache (we know all fits into the cache)
        self.cluster.flush()

        # We are checking the number of elemnts in the cache - we know some should have been removed as we moved some elements
        node1_cache_entries = node1.row_cache_entries() - pre_insert
        node2_cache_entries = node2.row_cache_entries()
        self.assertEqual(node1_cache_entries + node2_cache_entries,1000, "node1 cache %d node2 cache %d expected total of 1000" % (node1_cache_entries,node2_cache_entries))

    def simple_add_two_nodes_in_parallel_test(self):
        """
        Test bootstrapped node streams all data
        1. Create a cluster with a single node with rf=3, insert data
        2. Add two nodes
        3. Check that first added node succeeds to join the cluster and completes bootstrap
        4. Check that the second fails with correct cause
        """
        cluster = self.cluster
        self.allow_log_errors = True

        # Disable hinted handoff and set batch commit log so this doesn't
        # interfer with the test (this must be after the populate)
        cluster.set_configuration_options(values={'hinted_handoff_enabled': False}, batch_commitlog=True)
        cluster.populate(1).start()
        node1 = cluster.nodelist()[0]

        cursor = self.patient_cql_connection(node1)
        self.create_ks(cursor, 'ks', 3)
        self.create_cf(cursor, 'cf', read_repair=0.0, columns={'c1': 'text', 'c2': 'text'})

        for i in xrange(0, 1000):
            insert_c1c2(cursor, i, ConsistencyLevel.ONE)

        node2 = new_node(cluster)
        # creating an additional node without actually adding it to the cluster
        i = len(cluster.nodes) + 1
        node3 = cluster.create_node('node%s' % i,
                True,
                ('127.0.0.%s' % i, 9160),
                ('127.0.0.%s' % i, 7000),
                str(7000 + i * 100),
                None,
                None,
                binary_interface=('127.0.0.%s' % i, 9042))

        node2.start()
        time.sleep(0.1)
        try:
            node3.start()
        except NodeError:
            pass

        node2.watch_log_for("Starting listening for CQL clients")
        session = self.patient_exclusive_cql_connection(node2)
        node1.watch_log_for_alive(node2)
        node2.watch_log_for_alive(node1)

        for i in xrange(1000, 2000):
            insert_c1c2(cursor, i, ConsistencyLevel.TWO)

        self.check_rows_on_node(node2, 2000)
        self.check_rows_on_node(node1, 2000)
        # check that node3 existed with the correct message
        node3.watch_log_for("Other bootstrapping/leaving/moving nodes detected, cannot bootstrap while cassandra.consistent.rangemovement is true")

    def simple_kill_new_node_while_bootstrapping(self):
        """
        Test bootstrapped node streams all data
        1. Create a cluster with a three nodes with rf=1, insert data
        2. Add node, wait for each to start bootstrapping and kill it
        3. Add node, wait for each to start bootstrapping and kill it
        4. Check that the cluster returns all 
        """
        cluster = self.cluster
        self.allow_log_errors = True

        # Disable hinted handoff and set batch commit log so this doesn't
        # interfer with the test (this must be after the populate)
        cluster.set_configuration_options(values={'hinted_handoff_enabled': False}, batch_commitlog=True)
        cluster.populate(3).start()
        node1 = cluster.nodelist()[0]

        cursor = self.patient_cql_connection(node1)
        self.create_ks(cursor, 'ks', 1)
        self.create_cf(cursor, 'cf', read_repair=0.0, columns={'c1': 'text', 'c2': 'text'})

        for i in xrange(0, 1000):
            insert_c1c2(cursor, i, ConsistencyLevel.ONE)

        for i in xrange(4,6):
            # creating an additional node without actually adding it to the cluster
            new_node = cluster.create_node('node%s' % i,
                    True,
                    ('127.0.0.%s' % i, 9160),
                    ('127.0.0.%s' % i, 7000),
                    str(7000 + i * 100),
                    None,
                    None,
                    binary_interface=('127.0.0.%s' % i, 9042))
            new_node.start()
            new_node.watch_log_for("Beginning stream session")
            new_node.stop(gently=False)
            time.sleep(10)

        result = cursor.execute("SELECT * FROM cf")
        self.assertEqual(len(result), 1000, len(result))

    def simple_add_new_node_while_adding_info(self,rf):
        """
        Test bootstrapped node streams all data
        1. Create a cluster with a three nodes with rf, insert data
        3. Add node, while node is bootstrapping insert data
        4. Check that the cluster returns all
        """
        cluster = self.cluster
        self.allow_log_errors = True
        consistency = {1 : ConsistencyLevel.ONE, 2: ConsistencyLevel.TWO}[rf]

        # Disable hinted handoff and set batch commit log so this doesn't
        # interfer with the test (this must be after the populate)
        cluster.set_configuration_options(values={'hinted_handoff_enabled': False}, batch_commitlog=True)
        cluster.populate(3).start()
        node1 = cluster.nodelist()[0]

        cursor = self.patient_cql_connection(node1)
        self.create_ks(cursor, 'ks', rf)
        self.create_cf(cursor, 'cf', read_repair=0.0, columns={'c1': 'text', 'c2': 'text'})

        for i in xrange(0, 2000):
            insert_c1c2(cursor, i, consistency)

        event = threading.Event()
        def run():
             for i in xrange(2000, 4000):
                 insert_c1c2(cursor, i, consistency)
             event.set()
             pass

        t = threading.Thread(target=run)
        t.setDaemon(True)

        node4 = new_node(cluster)
        node4.start()
        node4.watch_log_for("Beginning stream session")
        t.start()

        event.wait()
        query = SimpleStatement("SELECT * FROM cf", consistency_level=consistency)
        result = cursor.execute(query)
        self.assertEqual(len(result), 4000, len(result))

        for k in xrange(0,4000):
            query_c1c2(cursor, k, consistency)

    def simple_add_new_node_while_adding_info_1(self):
        self.simple_add_new_node_while_adding_info(1)

    def simple_add_new_node_while_adding_info_2(self):
        self.simple_add_new_node_while_adding_info(2)

    def simple_add_new_node_while_query_info(self,rf):
        """
        Test bootstrapped node streams all data
        1. Create a cluster with a three nodes with rf, insert data
        3. Add node, while node is bootstrapping query data
        4. Check that the cluster returns all
        """
        cluster = self.cluster
        self.allow_log_errors = True
        consistency = {1 : ConsistencyLevel.ONE, 2: ConsistencyLevel.TWO}[rf]

        # Disable hinted handoff and set batch commit log so this doesn't
        # interfer with the test (this must be after the populate)
        cluster.set_configuration_options(values={'hinted_handoff_enabled': False}, batch_commitlog=True)
        cluster.populate(3).start()
        node1 = cluster.nodelist()[0]

        cursor = self.patient_cql_connection(node1)
        self.create_ks(cursor, 'ks', rf)
        self.create_cf(cursor, 'cf', read_repair=0.0, columns={'c1': 'text', 'c2': 'text'})

        for i in xrange(0, 2000):
            insert_c1c2(cursor, i, consistency)

        event = threading.Event()
        def run():
             while i in xrange(1,100):
                query = SimpleStatement("SELECT * FROM cf", consistency_level=consistency)
                result = cursor.execute(query)
                self.assertEqual(len(result), 2000, len(result))
                time.sleep(0.01)
             event.set()
             pass

        t = threading.Thread(target=run)
        t.setDaemon(True)

        node4 = new_node(cluster)
        node4.start()
        node4.watch_log_for("Beginning stream session")
        t.start()

        node4.watch_log_for("Starting listening for CQL clients")

        query = SimpleStatement("SELECT * FROM cf", consistency_level=consistency)

        result = cursor.execute(query)
        self.assertEqual(len(result), 2000, len(result))
        for k in xrange(0,2000):
            query_c1c2(cursor, k, consistency)

        event.wait()

    def simple_add_new_node_while_query_info_1(self):
        self.simple_add_new_node_while_query_info(1)

    def simple_add_new_node_while_query_info_2(self):
        self.simple_add_new_node_while_query_info(2)

    def simple_decomission_node_1_test(self):
        """
        Test bootstrapped node streams all data
        1. Create a cluster with a single node with rf=1, insert data
        2. Decomission one node
        3. Check that the last node has all the data
        """
        cluster = self.cluster

        # Disable hinted handoff and set batch commit log so this doesn't
        # interfer with the test (this must be after the populate)
        cluster.set_configuration_options(values={'hinted_handoff_enabled': False}, batch_commitlog=True)
        cluster.populate(2).start(wait_for_binary_proto=True,wait_other_notice=True)
        node1,node2 = cluster.nodelist()

        cursor = self.patient_cql_connection(node1)
        self.create_ks(cursor, 'ks', 1)
        self.create_cf(cursor, 'cf', read_repair=0.0, columns={'c1': 'text', 'c2': 'text'})

        for i in xrange(0, 1000):
            insert_c1c2(cursor, i, ConsistencyLevel.ONE)

        node2.decommission()
        node2.stop()

        self.check_rows_on_node(node1, 1000, restart=False)

    def simple_kill_new_node_while_decommissioning(self):
        """
        Test a cedecomissioning node killed is able to rejoin the cluster with data
        1. Create a cluster with a three nodes with rf=1, insert data
        2. Decomission a node 
        3. While node is decomissioning kill it
        4. Boot the node back up
        5. Check that the node rejoins the cluster and works correctly
        """
        cluster = self.cluster
        self.allow_log_errors = True

        # Disable hinted handoff and set batch commit log so this doesn't
        # interfer with the test (this must be after the populate)
        cluster.set_configuration_options(values={'hinted_handoff_enabled': False}, batch_commitlog=True)
        cluster.populate(3).start(wait_for_binary_proto=True,wait_other_notice=True)
        node1,node2,node3 = cluster.nodelist()

        cursor = self.patient_cql_connection(node1)
        self.create_ks(cursor, 'ks', 1)
        self.create_cf(cursor, 'cf', read_repair=0.0, columns={'c1': 'text', 'c2': 'text'})

        for i in xrange(0, 1000):
            insert_c1c2(cursor, i, ConsistencyLevel.ONE)

        def run():
             try:
                node2.decommission()
             except Exception:
                pass

        t = threading.Thread(target=run)
        t.setDaemon(True)
        t.start()
   
        # check node2 has started decomission
        node2.watch_log_for("Beginning stream session")
        node2.stop(gently=False)

        # starting node2 - it should reconnect and run as is
        node2.start(wait_other_notice=True,wait_for_binary_proto=True)
        result = cursor.execute("SELECT * FROM cf")
        self.assertEqual(len(result), 1000, len(result))

    def simple_decommission_node_while_adding_info(self,rf):
        """
        Test bootstrapped node streams all data
        1. Create a cluster with a three nodes with rf, insert data
        2. Decomission node, while node is decomissioning insert data
        3. Check that the cluster returns all
        """
        cluster = self.cluster
        self.allow_log_errors = True
        consistency = {1 : ConsistencyLevel.ONE, 2: ConsistencyLevel.TWO}[rf]

        # Disable hinted handoff and set batch commit log so this doesn't
        # interfer with the test (this must be after the populate)
        cluster.set_configuration_options(values={'hinted_handoff_enabled': False}, batch_commitlog=True)
        cluster.populate(3).start()
        node1,node2,node3 = cluster.nodelist()

        cursor = self.patient_cql_connection(node1)
        self.create_ks(cursor, 'ks', rf)
        self.create_cf(cursor, 'cf', read_repair=0.0, columns={'c1': 'text', 'c2': 'text'})

        for i in xrange(0, 2000):
            insert_c1c2(cursor, i, consistency)

        event = threading.Event()
        def run():
             for i in xrange(2000, 4000):
                 insert_c1c2(cursor, i, consistency)

             query = SimpleStatement("SELECT * FROM cf", consistency_level=consistency)
             result = cursor.execute(query)
             self.assertEqual(len(result), 4000, len(result))

             event.set()
             pass

        t = threading.Thread(target=run)
        t.setDaemon(True)
        t.start()

        node2.decommission()

        event.wait()
        node2.stop()
        query = SimpleStatement("SELECT * FROM cf", consistency_level=consistency)
        result = cursor.execute(query)
        self.assertEqual(len(result), 4000, len(result))
        for k in xrange(0,4000):
            query_c1c2(cursor, k, consistency)

    def simple_decommission_node_while_adding_info_1(self):
        self.simple_decommission_node_while_adding_info(1)

    def simple_decommission_node_while_adding_info_2(self):
        self.simple_decommission_node_while_adding_info(2)

    def simple_decommission_node_while_query_info(self,rf):
        """
        Test bootstrapped node streams all data
        1. Create a cluster with a three nodes with rf, insert data
        2. Decomission node, while node is decomissioning query data
        3. Check that the cluster returns all
        """
        cluster = self.cluster
        self.allow_log_errors = True
        consistency = {1 : ConsistencyLevel.ONE, 2: ConsistencyLevel.TWO}[rf]

        # Disable hinted handoff and set batch commit log so this doesn't
        # interfer with the test (this must be after the populate)
        cluster.set_configuration_options(values={'hinted_handoff_enabled': False}, batch_commitlog=True)
        cluster.populate(3).start()
        node1,node2,node3 = cluster.nodelist()

        cursor = self.patient_cql_connection(node1)
        self.create_ks(cursor, 'ks', rf)
        self.create_cf(cursor, 'cf', read_repair=0.0, columns={'c1': 'text', 'c2': 'text'})

        for i in xrange(0, 2000):
            insert_c1c2(cursor, i, consistency)

        event = threading.Event()
        def run():
             while i in xrange(1,100):
                query = SimpleStatement("SELECT * FROM cf", consistency_level=consistency)
                result = cursor.execute(query)
                self.assertEqual(len(result), 2000, len(result))
                time.sleep(0.01)
             event.set()
             pass

        t = threading.Thread(target=run)
        t.setDaemon(True)
        t.start()

        node2.decommission()

        query = SimpleStatement("SELECT * FROM cf", consistency_level=consistency)
        result = cursor.execute(query)
        self.assertEqual(len(result), 2000, len(result))

        node2.stop()

        query = SimpleStatement("SELECT * FROM cf", consistency_level=consistency)
        result = cursor.execute(query)
        self.assertEqual(len(result), 2000, len(result))
        for k in xrange(0,2000):
            query_c1c2(cursor, k, consistency)

        event.wait()

    def simple_decommission_node_while_query_info_1(self):
        self.simple_decommission_node_while_query_info(1)

    def simple_decommission_node_while_query_info_2(self):
        self.simple_decommission_node_while_query_info(2)
