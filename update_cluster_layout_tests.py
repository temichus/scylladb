import time
from collections import namedtuple

from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement

from dtest import Tester
from tools import insert_c1c2, query_c1c2, new_node


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
                node.start(wait_other_notice=True)

        for node,mark in nodes_marks:
            for other_node, _ in nodes_marks:
                if other_node is not node:
                    if mark:
                        node.watch_log_for_alive(other_node, from_mark=mark)
                    else:
                        node.watch_log_for_alive(other_node)

    def simple_add_node_1_test(self):
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
        cluster = self.cluster

        # Disable hinted handoff and set batch commit log so this doesn't
        # interfer with the test (this must be after the populate)
        cluster.set_configuration_options(values={'hinted_handoff_enabled': False}, batch_commitlog=True)
        cluster.populate(1).start()
        node1 = cluster.nodelist()[0]

        cursor = self.patient_cql_connection(node1)
        self.create_ks(cursor, 'ks', 1)
        self.create_cf(cursor, 'cf', read_repair=0.0, columns={'c1': 'text', 'c2': 'text'})

        # Insert 1000 keys, kill node 3, insert 1 key, restart node 3, insert 1000 more keys
        for i in xrange(0, 1000):
            insert_c1c2(cursor, i, ConsistencyLevel.ONE)

        node2 = new_node(cluster)
        node2.start(wait_for_binary_proto=True)

        session = self.patient_exclusive_cql_connection(node2)
        node1.watch_log_for_alive(node2)
        node2.watch_log_for_alive(node1)

        node1.stop();
        node2.watch_log_for_death(node1)
        cursor_node2 = self.patient_cql_connection(node2, 'ks')
        result_node2 = cursor_node2.execute("SELECT * FROM cf LIMIT %d" % 4000)
        
        self.start_all_nodes()
 
        node2.stop();
        node1.watch_log_for_death(node2)
        cursor_node1 = self.patient_cql_connection(node1, 'ks')
        result_node1 = cursor_node1.execute("SELECT * FROM cf LIMIT %d" % 4000)

        merged_result = []
        merged_result.update(result_node1)
        merged_result.update(result_node2)
        assert(len(merged_result) == 1000)
 
        tmp1 = []
        tmp1.update(result_node1).intersection_update(result_node2)
        assert(len(tmp1) == 0)
 
        tmp2 = []
        tmp2.update(result_node2).intersection_update(result_node1)
        assert(len(tmp2) == 0)

        

