import time
from collections import namedtuple

from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement

from dtest import Tester
from tools import insert_c1c2, query_c1c2, new_node
from ccmlib.node import NodeError



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

        # Insert 1000 keys, kill node 3, insert 1 key, restart node 3, insert 1000 more keys
        for i in xrange(0, 1000):
            insert_c1c2(cursor, i, ConsistencyLevel.ONE)

        node2 = new_node(cluster)
        node2.start(wait_for_binary_proto=True)

        session = self.patient_exclusive_cql_connection(node2)
        node1.watch_log_for_alive(node2)
        node2.watch_log_for_alive(node1)

        for i in xrange(1000, 2000):
            insert_c1c2(cursor, i, ConsistencyLevel.TWO)

        # check nodes have all the data
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
        # Insert 1000 keys, kill node 3, insert 1 key, restart node 3, insert 1000 more keys
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

        # Insert 1000 keys, kill node 3, insert 1 key, restart node 3, insert 1000 more keys
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

        # check nodes have all the data
        self.check_rows_on_node(node2, 2000)
        self.check_rows_on_node(node1, 2000)
        # check that node3 existed with the correct message
        node3.watch_log_for("Other bootstrapping/leaving/moving nodes detected, cannot bootstrap while cassandra.consistent.rangemovement is true")
