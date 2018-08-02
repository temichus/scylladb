# coding: utf-8

from dtest import Tester, debug
from unittest import skip

from tools import insert_c1c2, query_c1c2
from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement
from ccmlib.node import NodetoolError
import time
import tempfile
import os
import threading
import random
import datetime
import commands



class RepairAdditionalBase(Tester):
    __test__ = False

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

    def _repair(self, node, options=[]):
        return node.repair(options)

    def _repair_disjoint_data_test(self, more_options=[]):
        """
        On each of three replicas, insert completely different data.
        Confirm that repairing a single of these nodes brings all the data
        to all three replicas.
        """
        debug("Starting cluster...")
        # Disable hinted handoff so it doesn't do what we expect repair to do
        self.cluster.set_configuration_options(values={'hinted_handoff_enabled': False})
        # Create a cluster of 3 nodes, and a keyspace with RF=3 on all nodes
        # (disable read repair, as we want to test the full repair).
        self.cluster.populate(3).start(wait_for_binary_proto=True, wait_other_notice=True)
        node1, node2, node3 = self.cluster.nodelist()
        session = self.patient_cql_connection(node1)
        self.create_ks(session, 'ks', 3)
        self.create_cf(session, 'cf', read_repair=0.0, columns={'c1': 'text', 'c2': 'text'})

        # Insert 1000 keys *only* on node 1, another 1000 keys *only* on node 2,
        # another 1000 *only on node 3:
        debug("Adding data only on node 1...")
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
        time.sleep(10)  # see CASSANDRA-4373
        debug("starting repair...")
        info = self._repair(node3,more_options + ['ks'])
        debug(info[0])
        debug(info[1])

        # Check that all nodes have all data
        self.check_rows_on_node(node1, 3000)
        self.check_rows_on_node(node2, 3000)
        self.check_rows_on_node(node3, 3000)

    def _repair_schema_test(self):
        """
        In a keyspace with two replicas, insert a new column family on one
        replica only (while the other node is down), and initiate repair from
        the node with the data. Verify that the data (and its schema) have been
        correctly replicated to the second node.
        """
        debug("Starting cluster...")
        # Start a cluster of two nodes, and create a keyspace with RF=2.
        # Do *not* create a table yet - we'll do that with one node down
        self.cluster.set_configuration_options(values={'hinted_handoff_enabled': False})
        self.cluster.populate(2).start(wait_for_binary_proto=True, wait_other_notice=True)
        node1, node2 = self.cluster.nodelist()
        session = self.patient_cql_connection(node1)
        self.create_ks(session, 'ks', 2)

        # Take node2 down, and create a new table and data on node1 only.
        debug("Creating table and data only on node 1...")
        node2.flush()
        node2.stop(wait_other_notice=True)
        self.create_cf(session, 'cf', read_repair=0.0, columns={'c1': 'text', 'c2': 'text'})
        insert_c1c2(session, keys=range(1000, 2000), consistency=ConsistencyLevel.ONE)

        # At this point node2 is not only missing some data, it is actually
        # missing an entire table. Let's bring node2 back up, start repair on
        # node1, and see if node2 gets the new table, and all its data.
        node2.start(wait_other_notice=True, wait_for_binary_proto=True)
        time.sleep(10)  # see CASSANDRA-4373
        debug("starting repair on node1...")
        info = self._repair(node1,['ks'])
        debug(info[0])
        debug(info[1])

        # Check that all nodes have all data
        debug("checking data on node1...")
        self.check_rows_on_node(node1, 1000)
        debug("checking data on node2...")
        self.check_rows_on_node(node2, 1000)

        if not hasattr(self, 'ignore_log_patterns'):
            self.ignore_log_patterns = []
        self.ignore_log_patterns.append(r'.*migration_task - Can\'t send migration request.*')


    def _repair_schema_2_test(self):
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
        debug("Starting cluster...")
        # Start a cluster of two nodes, and create a keyspace with RF=2.
        # Do *not* create a table yet - we'll do that with one node down
        self.cluster.set_configuration_options(values={'hinted_handoff_enabled': False})
        self.cluster.populate(2).start(wait_for_binary_proto=True, wait_other_notice=True)
        node1, node2 = self.cluster.nodelist()
        session = self.patient_cql_connection(node1)
        self.create_ks(session, 'ks', 2)

        # Take node2 down, and create a new table and data on node1 only.
        debug("Creating table and data only on node 1...")
        node2.flush()
        node2.stop(wait_other_notice=True)
        self.create_cf(session, 'cf', read_repair=0.0, columns={'c1': 'text', 'c2': 'text'})
        insert_c1c2(session, keys=range(1000, 2000), consistency=ConsistencyLevel.ONE)

        # At this point node2 is not only missing some data, it is actually
        # missing an entire table. Let's bring node2 back up, start repair on
        # node2, and see if node2 gets the new table, and all its data.
        node2.start(wait_other_notice=True, wait_for_binary_proto=True)
        time.sleep(10)  # see CASSANDRA-4373
        debug("starting repair on node1...")
        info = self._repair(node2,['ks'])
        debug(info[0])
        debug(info[1])

        # Check that all nodes have all data
        debug("checking data on node1...")
        self.check_rows_on_node(node1, 1000)
        debug("checking data on node2...")
        self.check_rows_on_node(node2, 1000)

        if not hasattr(self, 'ignore_log_patterns'):
            self.ignore_log_patterns = []
        self.ignore_log_patterns.append(r'.*migration_task - Can\'t send migration request.*')

    def _repair_cell_update_test(self):
        """
        With data replicated on two nodes, update an existing partition on only
        one of these nodes (with the other node down). Then confirm that repair can
        fix this on the second node as well.
        """
        debug("Starting cluster and inserting data...")
        # Start a cluster of two nodes, and create a keyspace with RF=2, and
        # a table with one partition. Hinted handoff and read repair are disabled
        # so they don't fix the problems which repair is supposed to fix
        self.cluster.set_configuration_options(values={'hinted_handoff_enabled': False})
        self.cluster.populate(2).start(wait_for_binary_proto=True, wait_other_notice=True)
        node1, node2 = self.cluster.nodelist()
        session = self.patient_cql_connection(node1)
        self.create_ks(session, 'ks', 2)
        self.create_cf(session, 'cf', read_repair=0.0, columns={'c1': 'text', 'c2': 'text'})
        query = SimpleStatement("INSERT INTO cf (key, c1, c2) VALUES ('key', 'hello', 'hi')", consistency_level=ConsistencyLevel.ALL)
        session.execute(query)

        # Bring down node2, and change the existing data on node 1
        debug("Bringing down node2 and updating data on node 1...")
        node2.flush()
        node2.stop(wait_other_notice=True)
        query = SimpleStatement("INSERT INTO cf (key, c1, c2) VALUES ('key', 'new', 'yo')", consistency_level=ConsistencyLevel.ONE)
        session.execute(query)

        # Confirm that node1 has new data, and (by bringing only node 2 up) that
        # node2 still has old data
        result = list(session.execute("SELECT * from cf"))
        self.assertEqual(len(result), 1, len(result))
        self.assertEqual(result[0].key, 'key', result[0].key)
        self.assertEqual(result[0].c1, 'new', result[0].c1)
        self.assertEqual(result[0].c2, 'yo', result[0].c2)
        node2.start(wait_other_notice=True, wait_for_binary_proto=True)
        node1.flush()
        node1.stop(wait_other_notice=True)
        session = self.patient_cql_connection(node2, 'ks')
        result = list(session.execute("SELECT * from cf"))
        self.assertEqual(len(result), 1, len(result))
        self.assertEqual(result[0].key, 'key', result[0].key)
        self.assertEqual(result[0].c1, 'hello', result[0].c1)
        self.assertEqual(result[0].c2, 'hi', result[0].c2)

        # Finally bring both nodes up, repair, and confirm (by bringing up only
        # node 2) that the data on node2 is now up to date.
        node1.start(wait_other_notice=True, wait_for_binary_proto=True)
        info = self._repair(node2,['ks'])
        debug(info[0])
        debug(info[1])
        node1.flush()
        node1.stop(wait_other_notice=True)
        session = self.patient_cql_connection(node2, 'ks')
        result = list(session.execute("SELECT * from cf"))
        self.assertEqual(len(result), 1, len(result))
        self.assertEqual(result[0].key, 'key', result[0].key)
        self.assertEqual(result[0].c1, 'new', result[0].c1)
        self.assertEqual(result[0].c2, 'yo', result[0].c2)

    def _repair_cell_delete_test(self):
        """
        With data replicated on two nodes, update an existing partition on only
        one of these nodes (with the other node down) to delete an existing cell.
        Then confirm that repair can fix this on the second node as well.
        """
        debug("Starting cluster and inserting data...")
        # Start a cluster of two nodes, and create a keyspace with RF=2, and
        # a table with one partition. Hinted handoff and read repair are disabled
        # so they don't fix the problems which repair is supposed to fix
        self.cluster.set_configuration_options(values={'hinted_handoff_enabled': False})
        self.cluster.populate(2).start(wait_for_binary_proto=True, wait_other_notice=True)
        node1, node2 = self.cluster.nodelist()
        session = self.patient_cql_connection(node1)
        self.create_ks(session, 'ks', 2)
        self.create_cf(session, 'cf', read_repair=0.0, columns={'c1': 'text', 'c2': 'text'})
        query = SimpleStatement("INSERT INTO cf (key, c1, c2) VALUES ('key', 'hello', 'hi')", consistency_level=ConsistencyLevel.ALL)
        session.execute(query)

        # Bring down node2, and change the existing data on node 1
        debug("Bringing down node2 and updating data on node 1...")
        node2.flush()
        node2.stop(wait_other_notice=True)
        query = SimpleStatement("DELETE c1 FROM cf WHERE key ='key'", consistency_level=ConsistencyLevel.ONE)
        session.execute(query)

        # Confirm that node1 has new data, and (by bringing only node 2 up) that
        # node2 still has old data
        result = list(session.execute("SELECT * from cf"))
        self.assertEqual(len(result), 1, len(result))
        self.assertEqual(result[0].key, 'key', result[0].key)
        self.assertEqual(result[0].c1, None, result[0].c1)
        self.assertEqual(result[0].c2, 'hi', result[0].c2)
        node2.start(wait_other_notice=True, wait_for_binary_proto=True)
        node1.flush()
        node1.stop(wait_other_notice=True)
        session = self.patient_cql_connection(node2, 'ks')
        result = list(session.execute("SELECT * from cf"))
        self.assertEqual(len(result), 1, len(result))
        self.assertEqual(result[0].key, 'key', result[0].key)
        self.assertEqual(result[0].c1, 'hello', result[0].c1)
        self.assertEqual(result[0].c2, 'hi', result[0].c2)

        # Finally bring both nodes up, repair, and confirm (by bringing up only
        # node 2) that the data on node2 is now up to date.
        node1.start(wait_other_notice=True, wait_for_binary_proto=True)
        info = self._repair(node2,['ks'])
        debug(info[0])
        debug(info[1])
        node1.flush()
        node1.stop(wait_other_notice=True)
        session = self.patient_cql_connection(node2, 'ks')
        result = list(session.execute("SELECT * from cf"))
        self.assertEqual(len(result), 1, len(result))
        self.assertEqual(result[0].key, 'key', result[0].key)
        self.assertEqual(result[0].c1, None, result[0].c1)
        self.assertEqual(result[0].c2, 'hi', result[0].c2)

    def _repair_row_delete_test(self):
        """
        With data replicated on two nodes, update an existing partition on only
        one of these nodes (with the other node down) to delete an existing CQL row.
        Such a delete will result in a range tombstone.
        Then confirm that repair can fix this on the second node as well.
        """
        debug("Starting cluster and inserting data...")
        # Start a cluster of two nodes, and create a keyspace with RF=2, and
        # a table with one partition. Hinted handoff and read repair are disabled
        # so they don't fix the problems which repair is supposed to fix
        self.cluster.set_configuration_options(values={'hinted_handoff_enabled': False})
        self.cluster.populate(2).start(wait_for_binary_proto=True, wait_other_notice=True)
        node1, node2 = self.cluster.nodelist()
        session = self.patient_cql_connection(node1)
        self.create_ks(session, 'ks', 2)
        session.execute("CREATE TABLE cf (name text, pet text, age int, PRIMARY KEY ((name), pet)) WITH compression = {} AND read_repair_chance = 0.0;")

        query = SimpleStatement("INSERT INTO cf (name, pet, age) VALUES ('nadav', 'kitty', 5)", consistency_level=ConsistencyLevel.ALL)
        session.execute(query)
        query = SimpleStatement("INSERT INTO cf (name, pet, age) VALUES ('nadav', 'adamdami', 1)", consistency_level=ConsistencyLevel.ALL)
        session.execute(query)

        # Bring down node2, and change the existing data on node 1
        debug("Bringing down node2 and updating data on node 1...")
        node2.flush()
        node2.stop(wait_other_notice=True)
        query = SimpleStatement("DELETE FROM cf WHERE name = 'nadav' AND pet = 'kitty'", consistency_level=ConsistencyLevel.ONE)
        session.execute(query)

        # Confirm that node1 has new data, and (by bringing only node 2 up) that
        # node2 still has old data
        result = list(session.execute("SELECT * from cf"))
        self.assertEqual(len(result), 1, len(result))
        self.assertEqual(result[0].name, 'nadav', result[0].name)
        self.assertEqual(result[0].pet, 'adamdami', result[0].pet)
        self.assertEqual(result[0].age, 1, result[0].age)
        node2.start(wait_other_notice=True, wait_for_binary_proto=True)
        node1.flush()
        node1.stop(wait_other_notice=True)
        session = self.patient_cql_connection(node2, 'ks')
        result = list(session.execute("SELECT * from cf"))
        self.assertEqual(len(result), 2, len(result))

        # Finally bring both nodes up, repair, and confirm (by bringing up only
        # node 2) that the data on node2 is now up to date.
        node1.start(wait_other_notice=True, wait_for_binary_proto=True)
        info = self._repair(node2,['ks'])
        debug(info[0])
        debug(info[1])
        node1.flush()
        node1.stop(wait_other_notice=True)
        session = self.patient_cql_connection(node2, 'ks')
        result = list(session.execute("SELECT * from cf"))
        self.assertEqual(len(result), 1, len(result))
        self.assertEqual(result[0].name, 'nadav', result[0].name)
        self.assertEqual(result[0].pet, 'adamdami', result[0].pet)
        self.assertEqual(result[0].age, 1, result[0].age)

    def _repair_partition_delete_test(self):
        """
        With data replicated on two nodes, delete partition on only one of these
        nodes (with the other node down). Then confirm that repair can fix this on
        the second node as well.
        """
        debug("Starting cluster and inserting data...")
        # Start a cluster of two nodes, and create a keyspace with RF=2, and
        # a table with three partitions. Hinted handoff and read repair are disabled
        # so they don't fix the problems which repair is supposed to fix
        self.cluster.set_configuration_options(values={'hinted_handoff_enabled': False})
        self.cluster.populate(2).start(wait_for_binary_proto=True, wait_other_notice=True)
        node1, node2 = self.cluster.nodelist()
        session = self.patient_cql_connection(node1)
        self.create_ks(session, 'ks', 2)
        self.create_cf(session, 'cf', read_repair=0.0, columns={'c1': 'text', 'c2': 'text'})
        query = SimpleStatement("INSERT INTO cf (key, c1, c2) VALUES ('k1', 'v11', 'v12')", consistency_level=ConsistencyLevel.ALL)
        session.execute(query)
        query = SimpleStatement("INSERT INTO cf (key, c1, c2) VALUES ('k2', 'v21', 'v22')", consistency_level=ConsistencyLevel.ALL)
        session.execute(query)
        query = SimpleStatement("INSERT INTO cf (key, c1, c2) VALUES ('k3', 'v31', 'v32')", consistency_level=ConsistencyLevel.ALL)
        session.execute(query)

        # Bring down node2, and change the existing data on node 1
        debug("Bringing down node2 and updating data on node 1...")
        node2.flush()
        node2.stop(wait_other_notice=True)
        query = SimpleStatement("DELETE FROM cf WHERE key = 'k1';", consistency_level=ConsistencyLevel.ONE)
        session.execute(query)
        query = SimpleStatement("DELETE FROM cf WHERE key = 'k3';", consistency_level=ConsistencyLevel.ONE)
        session.execute(query)

        # Confirm that node1 has new data, and (by bringing only node 2 up) that
        # node2 still has old data
        result = list(session.execute("SELECT * from cf"))
        self.assertEqual(len(result), 1, len(result))
        self.assertEqual(result[0].key, 'k2', result[0].key)
        self.assertEqual(result[0].c1, 'v21', result[0].c1)
        self.assertEqual(result[0].c2, 'v22', result[0].c2)
        node2.start(wait_other_notice=True, wait_for_binary_proto=True)
        node1.flush()
        node1.stop(wait_other_notice=True)
        session = self.patient_cql_connection(node2, 'ks')
        result = list(session.execute("SELECT * from cf"))
        self.assertEqual(len(result), 3, len(result))

        # Finally bring both nodes up, repair, and confirm (by bringing up only
        # node 2) that the data on node2 is now up to date.
        node1.start(wait_other_notice=True, wait_for_binary_proto=True)
        info = self._repair(node2,['ks'])
        debug(info[0])
        debug(info[1])
        node1.flush()
        node1.stop(wait_other_notice=True)
        session = self.patient_cql_connection(node2, 'ks')
        result = list(session.execute("SELECT * from cf"))
        self.assertEqual(len(result), 1, len(result))
        self.assertEqual(result[0].key, 'k2', result[0].key)
        self.assertEqual(result[0].c1, 'v21', result[0].c1)
        self.assertEqual(result[0].c2, 'v22', result[0].c2)

    def read_sstable(self, node):
        tmp = tempfile.TemporaryFile()
        node.run_sstable2json(tmp)
        tmp.seek(0)
        return tmp.read()

    def _repair_ttl_update_test(self):
        """
        With data replicated on two nodes, update an existing partition on only
        one of these nodes (with the other node down). Then confirm that repair can
        fix this on the second node as well.
        This test is identical to repair_cell_update_test, except the update also
        involves setting a TTL (and we confirm the repaired value also gets this ttl)
        """
        # Start a cluster of two nodes, and create a keyspace with RF=2, and
        # a table with one partition. Hinted handoff and read repair are disabled
        # so they don't fix the problems which repair is supposed to fix
        self.cluster.set_configuration_options(values={'hinted_handoff_enabled': False})
        self.cluster.populate(2).start(wait_for_binary_proto=True, wait_other_notice=True)
        node1, node2 = self.cluster.nodelist()
        session = self.patient_cql_connection(node1)
        self.create_ks(session, 'ks', 2)
        self.create_cf(session, 'cf', read_repair=0.0, columns={'c1': 'text', 'c2': 'text'})
        query = SimpleStatement("INSERT INTO cf (key, c1, c2) VALUES ('key', 'hello', 'hi')", consistency_level=ConsistencyLevel.ALL)
        session.execute(query)

        # Bring down node2, and change the existing data on node 1
        node2.flush()
        node2.stop(wait_other_notice=True)
        query = SimpleStatement("UPDATE cf using TTL 1234 SET c1='new' WHERE key = 'key'", consistency_level=ConsistencyLevel.ONE)
        session.execute(query)

        # Confirm that node1 has the new data, with the TTL. Unfortunately, to
        # verify the TTL we cannot simply use "SELECT TTL(c1) from cf",
        # because the TTL we get from that is not the original TTL we had set,
        # but rather the *remaining* TTL at this time. To verify the original
        # TTL set, we need to resort to reading the sstable.
        result = list(session.execute("SELECT * from cf"))
        self.assertEqual(len(result), 1, len(result))
        self.assertEqual(result[0].key, 'key', result[0].key)
        self.assertEqual(result[0].c1, 'new', result[0].c1)
        self.assertEqual(result[0].c2, 'hi', result[0].c2)
        node1.flush()
        sstable = self.read_sstable(node1)
        # The "c1" cell should have an expiration time and will look something
        # like this:   ["c1","6e6577",1452615051661760,"e",1234,1452616285],
        # We need to verify the number "1234" is the same as we set, and save
        # the entire line to verify it is identical on the repaired machine.
        save_line = None
        for line in sstable.split('\n'):
            if '"name" : "c1",' in line:
                self.assertTrue('"ttl" : 1234,' in line, "TTL set to 1234")
                save_line = line
        self.assertTrue(save_line is not None, "TTL set in sstable")

        # Confirm (by bringing only node 2 up) that node2 still has old data
        node2.start(wait_other_notice=True, wait_for_binary_proto=True)
        node1.flush()
        node1.stop(wait_other_notice=True)
        session = self.patient_cql_connection(node2, 'ks')
        result = list(session.execute("SELECT * from cf"))
        self.assertEqual(len(result), 1, len(result))
        self.assertEqual(result[0].key, 'key', result[0].key)
        self.assertEqual(result[0].c1, 'hello', result[0].c1)
        self.assertEqual(result[0].c2, 'hi', result[0].c2)
        sstable = self.read_sstable(node2)
        for line in sstable.split('\n'):
            if '["c1",' in line:
                self.assertFalse('"e",' in line, "TTL should not be set")

        # sstable2json has a bug (see CASSANDRA-8616) where it writes commit
        # log files. Since Scylla can't read those (they are in Cassandra
        # format) we need to remove them before we can restart node 1.
        # This may also end up deleting Scylla commit logs, but those should
        # not exist anyway (as we used node1.flush()).
        commitlog_dir = node1.get_path() + "/commitlogs/"
        for f in os.listdir(commitlog_dir):
            os.remove(commitlog_dir + f)

        # Finally bring both nodes up, repair, and confirm (by bringing up only
        # node 2) that the data on node2 is now up to date.
        node1.start(wait_other_notice=True, wait_for_binary_proto=True)
        info = self._repair(node2,['ks'])
        debug(info[0])
        debug(info[1])
        node1.flush()
        node1.stop(wait_other_notice=True)
        session = self.patient_cql_connection(node2, 'ks')
        result = list(session.execute("SELECT * from cf"))
        self.assertEqual(len(result), 1, len(result))
        self.assertEqual(result[0].key, 'key', result[0].key)
        self.assertEqual(result[0].c1, 'new', result[0].c1)
        self.assertEqual(result[0].c2, 'hi', result[0].c2)
        node2.flush()
        sstable = self.read_sstable(node2)
        # Confirm that one of the sstables contains the expected value and
        # expiration time (because we didn't do compaction, we'll see both
        # the old and new values in different sstables)
        for line in sstable.split('\n'):
            if '"name" : "c1",' in line:
                if save_line == line:
                    save_line = None
        self.assertTrue(save_line is None, "expected c1 value and timeout in sstable")

    def _repair_option_pr_test(self):
        """
        Test the "partitioner range" (-pr) option. We start two nodes and a
        keyspace with RF=2, and put 1000 different rows on each of the nodes
        (as in repair_disjoint_data_set). Each node has in "partioner ranges"
        only half the key space, so that starting a repair with "-pr" on one
        node will bring in around 500 missing partitions, but the other 500
        will continue to be missing until we start a repair with "-pr" on the
        second node as well.
        """
        # Start a cluster of two nodes, and create a keyspace ks with RF=2,
        # and a table cf. Hinted handoff and read repair are disabled so
        # they don't fix the problems which repair is supposed to fix.
        self.cluster.set_configuration_options(values={'hinted_handoff_enabled': False})
        self.cluster.populate(2).start(wait_for_binary_proto=True, wait_other_notice=True)
        node1, node2 = self.cluster.nodelist()
        session = self.patient_cql_connection(node1)
        self.create_ks(session, 'ks', 2)
        self.create_cf(session, 'cf', read_repair=0.0, columns={'c1': 'text', 'c2': 'text'})

        # Insert 1000 keys *only* on node 1, another 1000 keys *only* on node 2:
        debug("Adding data only on node 1...")
        node2.flush()
        node2.stop(wait_other_notice=True)
        session = self.patient_cql_connection(node1, 'ks')
        insert_c1c2(session, keys=range(1000, 2000), consistency=ConsistencyLevel.ONE)
        self.cluster.flush()
        debug("Adding data only on node 2...")
        node2.start(wait_other_notice=True, wait_for_binary_proto=True)
        node1.flush()
        node1.stop(wait_other_notice=True)
        session = self.patient_cql_connection(node2, 'ks')
        insert_c1c2(session, keys=range(2000, 3000), consistency=ConsistencyLevel.ONE)

        # Bring up both nodes, each should have different data
        node1.start(wait_other_notice=True, wait_for_binary_proto=True)

        # Run partioner-range repair on node 1
        info = node1.repair(['-pr', 'ks'])
        debug(info[0])
        debug(info[1])

        # We expect "-pr" repair to have repared only half of the ranges
        # (those for which node 1 is their primary replica), so both nodes
        # should now have around 1500 partitions. We don't know the exact
        # number, but given the assumed random distribution of tokens and keys,
        # it is unlikely to be far from 1500 - let's assert it is between
        # 1200 and 1800
        node1.flush()
        node1.stop(wait_other_notice=True)
        session = self.patient_cql_connection(node2, 'ks')
        count = len(list(session.execute("SELECT * FROM cf LIMIT 2000")))
        self.assertTrue(count > 1200 and count < 1800, "expected pr repair to repair part, but not everything")
        node1.start(wait_other_notice=True, wait_for_binary_proto=True)
        node2.flush()
        node2.stop(wait_other_notice=True)
        session = self.patient_cql_connection(node1, 'ks')
        count = len(list(session.execute("SELECT * FROM cf LIMIT 2000")))
        self.assertTrue(count > 1200 and count < 1800, "expected pr repair to repair part, but not everything")
        node2.start(wait_other_notice=True, wait_for_binary_proto=True)

        # Run a second "-pr" repair, this time on node 2. This should repair
        # all the ranges not previously repared (i.e., this times the ranges
        # whose primary is node 2), and at the end, all data, 2000 partitions,
        # should be on both nodes.
        info = node2.repair(['-pr', 'ks'])
        debug(info[0])
        debug(info[1])
        self.check_rows_on_node(node1, 2000)
        self.check_rows_on_node(node2, 2000)

    def _repair_option_pr_dc_host_test(self):
        """
        Test how the "partitioner range" (-pr) option interacts with the
        options which restrict the nodes participating in the repair -
        -dc, -local and -hosts.
        Since -pr usually assigns each token to just one node in the entire
        cluster, it is generally forbidden to restrict the repair to only
        part of the cluster otherwise some ranges will never be repaired.
        Nevertheless, combining -pr with restriction to the local dc
        ("-local") *is* allowed, and changes the meaning of -pr to not
        pick just one node in the cluster as the primary for every token -
        but rather one node in every dc.
        In this test we verify that forbidden option combinations are
        indeed forbidden, and the supported combination "-pr -local" is
        supported correctly - so if we loop on all nodes of just one dc
        and repair them with "-pr -local", it will repair the data center
        completely, over the entire token range.
        """
        # Start a cluster of three data centers with two nodes each, and
        # create a keyspace ks with RF=2, and a table cf.
        # Hinted handoff and read repair are disabled so they don't fix the
        # problems which repair is supposed to fix.
        self.cluster.set_configuration_options(values={'hinted_handoff_enabled': False})
        self.cluster.populate([2,2,2]).start(wait_for_binary_proto=True, wait_other_notice=True)
        node1_1, node1_2, node2_1, node2_2, node3_1, node3_2 = self.cluster.nodelist()
        session = self.patient_cql_connection(node1_1)
        self.create_ks(session, 'ks', {'dc1': 2, 'dc2': 2, 'dc3': 2})
        self.create_cf(session, 'cf', read_repair=0.0, columns={'c1': 'text', 'c2': 'text'})

        # Repair with "-pr" that restricts the repair to a subset of
        # data centers or a subset of hosts is forbidden, and should
        # cause a failure. Since generally repair not including the
        # current dc or host is forbidden, we check a case which does
        # include the current dc and the current host.
        # Interestingly, both nodetool (Repair.java) and Scylla have
        # code to fail this case, so we only test the outer layer
        # (Repair.java).
        # Note that combining -pr with -local is a special case,
        # which is supported, and we'll test below.
        with self.assertRaises(NodetoolError):
            node1_1.repair(['-pr', '-dc', 'dc1,dc3', 'ks'])

        # Same issue with combination of -pr with -hosts
        with self.assertRaises(NodetoolError):
            node1_1.repair(['-pr', '-hosts', node1_1.address(), 'ks'])

        # Although combining -pr with -local is allowed (see below),
        # the supposedly equivalent "-pr -dc dc1" (when dc1 is the local
        # dc) is NOT allowed, caught by nodetool (Repair.java).
        # Let's test that this is indeed the case.
        # I think this is deliberate, and the thinking is that we want to
        # allow only a command which, if run on every node, will work.
        # So while "-pr -local" will work (repair using the local cluster
        # on every node), "-pr -dc dc1" will not (for nodes in dc2, dc2
        # would need to be used instead).
        # Note that as far as Scylla is concerned, there is no difference
        # between "-local" or "-dc dc1" when dc1 is the local dc1. So this
        # case *could* have worked if nodetool didn't forbid it.
        with self.assertRaises(NodetoolError):
            node1_1.repair(['-pr', '-dc', 'dc1', 'ks'])

        # However, "-pr" combined with restriction to the *local* datacenter
        # is supported, and should be supported correctly (see issue #3557).
        # In that case, if we run repair with "-pr -local" on all the nodes
        # of this datacenter only, all token ranges will be repaired and not
        # parts. Let's start with the trivial test that -pr -local doesn't
        # cause an error. Then we'll check a more elaborate example with
        # actual data, repair again and verify it actually repairs data.
        node1_1.repair(['-pr', '-local', 'ks'])

        # Insert 1000 keys *only* on node 1, another 1000 keys *only* on node 2
        # both in the first data center. The other data centers will be
        # completely missing this data:
        debug("Adding data only on node 1...")
        for node in self.cluster.nodelist():
            if node != node1_1:
                node.stop(wait_other_notice=True)
        session = self.patient_cql_connection(node1_1, 'ks')
        insert_c1c2(session, keys=range(1000, 2000), consistency=ConsistencyLevel.LOCAL_ONE)
        self.cluster.flush()
        debug("Adding data only on node 2...")
        node1_2.start(wait_other_notice=True, wait_for_binary_proto=True)
        node1_1.stop(wait_other_notice=True)
        session = self.patient_cql_connection(node1_2, 'ks')
        insert_c1c2(session, keys=range(2000, 3000), consistency=ConsistencyLevel.LOCAL_ONE)

        # Bring up all nodes, each node on dc 1 should have different data
        # and all the nodes of the two other clusters are empty (but that's
        # not important in this case).
        for node in self.cluster.nodelist():
            if node != node1_2:
                node.start(wait_other_notice=True, wait_for_binary_proto=True)

        # Run dc-local partioner-range repair on node 1
        info = node1_1.repair(['-pr', '-local', 'ks'])
        debug(info[0])
        debug(info[1])
        # We expect "-pr" repair to have repaired only half of the ranges
        # (those for which node 1 is their primary replica), so both nodes
        # should now have around 1500 partitions. We don't know the exact
        # number, but given the assumed random distribution of tokens and keys,
        # it is unlikely to be far from 1500 - let's assert it is between
        # 1200 and 1800
        # Note that if the "-local" *was* not obeyed, we would see a
        # failure here because without "-local", "-pr" repair of just one
        # node in a cluster of 6 would just repair 1/6th of the range,
        # not 1/2.
        node1_1.flush()
        node1_1.stop(wait_other_notice=True)
        session = self.patient_cql_connection(node1_2, 'ks')
        count_query = SimpleStatement("SELECT count(*) from cf", consistency_level=ConsistencyLevel.ONE)
        count = session.execute(count_query)[0][0]
        self.assertTrue(count > 1200 and count < 1800, "expected pr repair to repair part, but not everything")
        node1_1.start(wait_other_notice=True, wait_for_binary_proto=True)
        node1_2.flush()
        node1_2.stop(wait_other_notice=True)
        session = self.patient_cql_connection(node1_1, 'ks')
        count = session.execute(count_query)[0][0]
        self.assertTrue(count > 1200 and count < 1800, "expected pr repair to repair part, but not everything")
        node1_2.start(wait_other_notice=True, wait_for_binary_proto=True)

        # Run a second dc-local "-pr" repair, this time on node 2. This
        # should repair all the ranges not previously repared (i.e., this
        # times the ranges whose primary is node 2), and at the end, all
        # data, 2000 partitions, should be on both nodes.
        # Note that if the "-local" *was* not obeyed, we would see a
        # failure here because without "-local", one would need to do
        # a "-pr" repair on all six nodes of the cluster to cover the
        # entire token range.
        info = node1_2.repair(['-pr', '-local', 'ks'])
        debug(info[0])
        debug(info[1])
        self.check_rows_on_node(node1_1, 2000)
        self.check_rows_on_node(node1_2, 2000)

    def _repair_option_pr_multi_dc_test(self):
        """
        Test how the "partitioner range" (-pr) option interacts with the
        a multi-dc setup (but without a "-local" parameter tested above).
        A user needs to do a -pr repair on each and every one of the nodes -
        on all data centers - to achieve a full repair.
        """
        # Start a cluster of three data centers with two nodes each, and
        # create a keyspace ks with RF=2, and a table cf.
        # Hinted handoff and read repair are disabled so they don't fix the
        # problems which repair is supposed to fix.
        debug("Starting 6 nodes...")
        self.cluster.set_configuration_options(values={'hinted_handoff_enabled': False})
        self.cluster.populate([2,2,2]).start(wait_for_binary_proto=True, wait_other_notice=True)
        node1_1, node1_2, node2_1, node2_2, node3_1, node3_2 = self.cluster.nodelist()
        session = self.patient_cql_connection(node1_1)
        self.create_ks(session, 'ks', {'dc1': 2, 'dc2': 2, 'dc3': 2})
        self.create_cf(session, 'cf', read_repair=0.0, columns={'c1': 'text', 'c2': 'text'})

        # Insert 1000 keys *only* on node 1, another 1000 keys *only* on node 2
        # both in the first data center. The other data centers will be
        # completely missing this data:
        debug("Adding data only on node 1...")
        for node in self.cluster.nodelist():
            if node != node1_1:
                node.stop(wait_other_notice=True)
        session = self.patient_cql_connection(node1_1, 'ks')
        insert_c1c2(session, keys=range(1000, 2000), consistency=ConsistencyLevel.LOCAL_ONE)
        self.cluster.flush()
        debug("Adding data only on node 2...")
        node1_2.start(wait_other_notice=True, wait_for_binary_proto=True)
        node1_1.stop(wait_other_notice=True)
        session = self.patient_cql_connection(node1_2, 'ks')
        insert_c1c2(session, keys=range(2000, 3000), consistency=ConsistencyLevel.LOCAL_ONE)

        # Bring up all nodes, each should have different data
        # (all the nodes of the two other clusters are empty, but that's
        # not important in this case).
        debug("Bring back all nodes...")
        for node in self.cluster.nodelist():
            if node != node1_2:
                node.start(wait_other_notice=True, wait_for_binary_proto=True)

        # Run dc-local partioner-range repair on node 1
        debug("Repair with -pr on node 1...")
        info = node1_1.repair(['-pr', 'ks'])
        debug(info[0])
        debug(info[1])
        # We expect "-pr" repair to have repaired only 1/6th of the ranges
        # (those for which node 1 is their primary replica), so node 1
        # should now have around 1166 partitions. We don't know the exact
        # number, but given the assumed random distribution of tokens and keys,
        # it is unlikely to be far from 1166 - let's assert it is between
        # 1050 and 1300
        node1_2.flush()
        node1_2.stop(wait_other_notice=True)
        session = self.patient_cql_connection(node1_1, 'ks')
        count = len(list(session.execute("SELECT * FROM cf LIMIT 2000")))
        debug(count)
        self.assertTrue(count > 1050 and count < 1300, "expected pr repair to repair part, but not everything")
        node1_2.start(wait_other_notice=True, wait_for_binary_proto=True)

        # Run dc-local "-pr" repair on all other nodes. This should repair
        # all the ranges not previously repared and at the end, all
        # data, 2000 partitions, should be on all nodes.
        for node in self.cluster.nodelist():
            if node != node1_1:
                debug("Repair with -pr on " + node.name)
                info = node.repair(['-pr', 'ks'])
                debug(info[0])
                debug(info[1])
        for node in self.cluster.nodelist():
            debug("Checking data on " + node.name)
            self.check_rows_on_node(node, 2000)

    def _repair_option_cf_test(self):
        """
        Test that we can specify the list of column families to repair. We
        create 3 column families in need of repair, and ask to repair only 2
        of them, and confirm that 2 were repaired (so a list of cfs is
        supported correctly) and the third was not. Finally, confirm that a
        repair without a column family list repairs all of them.
        """
        # Start a cluster of two nodes, and create a keyspace ks with RF=2,
        # and 3 tables. Hinted handoff and read repair are disabled so
        # they don't fix the problems which repair is supposed to fix.
        self.cluster.set_configuration_options(values={'hinted_handoff_enabled': False})
        self.cluster.populate(2).start(wait_for_binary_proto=True, wait_other_notice=True)
        node1, node2 = self.cluster.nodelist()
        session = self.patient_cql_connection(node1)
        self.create_ks(session, 'ks', 2)
        self.create_cf(session, 'cf1', read_repair=0.0, columns={'c1': 'text'})
        self.create_cf(session, 'cf2', read_repair=0.0, columns={'c1': 'text'})
        self.create_cf(session, 'cf3', read_repair=0.0, columns={'c1': 'text'})

        # Insert one key in each cf *only* on node 1, another key *only* on node 2:
        node2.flush()
        node2.stop(wait_other_notice=True)
        session = self.patient_cql_connection(node1, 'ks')
        query = SimpleStatement("INSERT INTO cf1 (key, c1) VALUES ('k11', 'v11')", consistency_level=ConsistencyLevel.ONE)
        session.execute(query)
        query = SimpleStatement("INSERT INTO cf2 (key, c1) VALUES ('k21', 'v21')", consistency_level=ConsistencyLevel.ONE)
        session.execute(query)
        query = SimpleStatement("INSERT INTO cf3 (key, c1) VALUES ('k31', 'v31')", consistency_level=ConsistencyLevel.ONE)
        session.execute(query)
        self.cluster.flush()
        node2.start(wait_other_notice=True, wait_for_binary_proto=True)
        node1.flush()
        node1.stop(wait_other_notice=True)
        session = self.patient_cql_connection(node2, 'ks')
        query = SimpleStatement("INSERT INTO cf1 (key, c1) VALUES ('k11a', 'v11a')", consistency_level=ConsistencyLevel.ONE)
        session.execute(query)
        query = SimpleStatement("INSERT INTO cf2 (key, c1) VALUES ('k21a', 'v21a')", consistency_level=ConsistencyLevel.ONE)
        session.execute(query)
        query = SimpleStatement("INSERT INTO cf3 (key, c1) VALUES ('k31a', 'v31a')", consistency_level=ConsistencyLevel.ONE)
        session.execute(query)

        # Bring up both nodes, each should have different data
        node1.start(wait_other_notice=True, wait_for_binary_proto=True)

        # Run partioner-range repair on node 1
        info = node1.repair(['ks', 'cf1', 'cf3'])
        debug(info[0])
        debug(info[1])

        # We expect each node to now have 2 partitions in each of cf1 and cf3
        # because those have been repaired - but only 1 in cf2.
        node1.flush()
        node1.stop(wait_other_notice=True)
        session = self.patient_cql_connection(node2, 'ks')
        self.assertEqual(len(list(session.execute("SELECT * from cf1"))), 2, "cf1 on node2")
        self.assertEqual(len(list(session.execute("SELECT * from cf2"))), 1, "cf2 on node2")
        self.assertEqual(len(list(session.execute("SELECT * from cf3"))), 2, "cf2 on node2")
        node1.start(wait_other_notice=True, wait_for_binary_proto=True)
        node2.flush()
        node2.stop(wait_other_notice=True)
        session = self.patient_cql_connection(node1, 'ks')
        self.assertEqual(len(list(session.execute("SELECT * from cf1"))), 2, "cf1 on node1")
        self.assertEqual(len(list(session.execute("SELECT * from cf2"))), 1, "cf2 on node1")
        self.assertEqual(len(list(session.execute("SELECT * from cf3"))), 2, "cf2 on node1")

        # repair again without a cf option, and see that all cfs, and in
        # particular cf2 (which we haven't repaired so far), get repaired.
        node2.start(wait_other_notice=True, wait_for_binary_proto=True)
        info = node1.repair(['ks'])
        debug(info[0])
        debug(info[1])
        node1.flush()
        node1.stop(wait_other_notice=True)
        session = self.patient_cql_connection(node2, 'ks')
        self.assertEqual(len(list(session.execute("SELECT * from cf2"))), 2, "cf2 on node2")

    def _repair_option_invalid_ks_cf_test(self):
        """
        Test that specifying a non-existant keyspace or column family to
        repair results in failure.
        """
        self.cluster.set_configuration_options(values={'hinted_handoff_enabled': False})
        self.cluster.populate(2).start(wait_for_binary_proto=True, wait_other_notice=True)
        node1, node2 = self.cluster.nodelist()
        session = self.patient_cql_connection(node1)
        self.create_ks(session, 'ks', 2)
        self.create_cf(session, 'cf', read_repair=0.0, columns={'c1': 'text'})

        # Repairing an invalid column family in a valid keyspace
        with self.assertRaises(NodetoolError):
            node1.repair(['ks', 'badcf'])
        # Repairing an invalid keyspace
        with self.assertRaises(NodetoolError):
            node1.repair(['badks'])
        # Repair with one of the cfs being invalid
        with self.assertRaises(NodetoolError):
            node1.repair(['badks', 'cf', 'badcf'])
        # Finally, sanity check that a valid repair succeeds:
        node1.repair(['ks', 'cf'])

    def _repair_option_dc_test(self):
        """
        Test the "-dc" and "-local" repair options: Create 3 data centers, the
        first with 2 nodes, second with 1 node, and third with 1 node. We then
        update data on one of the nodes in the first data center, and check
        that repairing with "-dc" and "-local" repairs the nodes of the
        requested datacenters, and not more.
        Finally, check that error conditions (like non-existant data center name,
        or not listing the current data center) are caught.
        """
        # Create 3 data centers, dc1 with 2 nodes, dc2 with 1 node, and dc3
        # with 1 node. Then create a keyspace ks replicated on all nodes,
        # and one cf. Hinted handoff and read repair are disabled so they
        # don't fix the problems which repair is supposed to fix.
        self.cluster.set_configuration_options(values={'hinted_handoff_enabled': False})
        self.cluster.populate([2, 1, 1]).start(wait_for_binary_proto=True, wait_other_notice=True)
        node1, node2, node3, node4 = self.cluster.nodelist()
        session = self.patient_cql_connection(node1)
        session.execute("CREATE KEYSPACE ks WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 2, 'dc2' : 1, 'dc3': 1};")
        session.set_keyspace('ks')
        self.create_cf(session, 'cf', read_repair=0.0, columns={'c1': 'text'})

        # Insert one key *only* on node 1 (of dc1). All the other nodes will
        # be missing this data.
        # Insert one key in each cf *only* on node 1, another key *only* on node 2:
        node2.flush()
        node2.stop(wait_other_notice=True)
        node3.flush()
        node3.stop(wait_other_notice=True)
        node4.flush()
        node4.stop(wait_other_notice=True)
        query = SimpleStatement("INSERT INTO cf (key, c1) VALUES ('k11', 'v11')", consistency_level=ConsistencyLevel.ONE)
        session.execute(query)

        # Start all nodes, do a repair limited to dc1 and dc3, and confirm the
        # data was correctly copied to node2 (in dc1) and node4 (in dc3) but
        # not to node3 (in dc2):
        node2.start(wait_other_notice=True, wait_for_binary_proto=True)
        node3.start(wait_other_notice=True, wait_for_binary_proto=True)
        node4.start(wait_other_notice=True, wait_for_binary_proto=True)
        info = node1.repair(['-dc', 'dc1,dc3', 'ks'])
        debug(info[0])
        debug(info[1])
        node1.flush()
        node1.stop(wait_other_notice=True)
        node3.flush()
        node3.stop(wait_other_notice=True)
        node4.flush()
        node4.stop(wait_other_notice=True)
        session = self.patient_cql_connection(node2, 'ks')
        self.assertEqual(len(list(session.execute("SELECT * from cf"))), 1, "cf on node2")
        node3.start(wait_other_notice=True, wait_for_binary_proto=True)
        node2.flush()
        node2.stop(wait_other_notice=True)
        session = self.patient_cql_connection(node3, 'ks')
        self.assertEqual(len(list(session.execute("SELECT * from cf"))), 0, "cf on node3")
        node4.start(wait_other_notice=True, wait_for_binary_proto=True)
        node3.flush()
        node3.stop(wait_other_notice=True)
        session = self.patient_cql_connection(node4, 'ks')
        self.assertEqual(len(list(session.execute("SELECT * from cf"))), 1, "cf on node4")

        node1.start(wait_other_notice=True, wait_for_binary_proto=True)
        node2.start(wait_other_notice=True, wait_for_binary_proto=True)
        node3.start(wait_other_notice=True, wait_for_binary_proto=True)

        # Repair with one of the data centers specified being invalid should
        # cause a failure
        with self.assertRaises(NodetoolError):
            node1.repair(['-dc', 'dc1,baddc', 'ks'])

        # Repair with data centers specified *without* the current data center
        # is an error too.
        with self.assertRaises(NodetoolError):
            node1.repair(['-dc', 'dc2,dc3', 'ks'])

        # Repair again without a "-dc" option - should repair all nodes in all
        # data centers, and in particular node3 (in dc2) .
        info = node1.repair(['ks'])
        debug(info[0])
        debug(info[1])
        node1.flush()
        node1.stop(wait_other_notice=True)
        node2.flush()
        node2.stop(wait_other_notice=True)
        node4.flush()
        node4.stop(wait_other_notice=True)
        session = self.patient_cql_connection(node3, 'ks')
        self.assertEqual(len(list(session.execute("SELECT * from cf"))), 1, "cf on node3")

        # Similiarly test the "-local" option: Add one more partition to node1
        # (in dc1), repair node1 with "-local" and confirm that only node2 (the
        # other node in dc1) gets another partition, but node3 (dc2) and node4
        # (dc3) don't.
        node1.start(wait_other_notice=True, wait_for_binary_proto=True)
        node3.flush()
        node3.stop(wait_other_notice=True)
        session = self.patient_cql_connection(node1, 'ks')
        query = SimpleStatement("INSERT INTO cf (key, c1) VALUES ('k12', 'v12')", consistency_level=ConsistencyLevel.ONE)
        session.execute(query)
        node2.start(wait_other_notice=True, wait_for_binary_proto=True)
        node3.start(wait_other_notice=True, wait_for_binary_proto=True)
        node4.start(wait_other_notice=True, wait_for_binary_proto=True)
        info = node1.repair(['-local', 'ks'])
        debug(info[0])
        debug(info[1])
        node1.flush()
        node1.stop(wait_other_notice=True)
        node3.flush()
        node3.stop(wait_other_notice=True)
        node4.flush()
        node4.stop(wait_other_notice=True)
        session = self.patient_cql_connection(node2, 'ks')
        self.assertEqual(len(list(session.execute("SELECT * from cf"))), 2, "cf on node2")
        node3.start(wait_other_notice=True, wait_for_binary_proto=True)
        node2.flush()
        node2.stop(wait_other_notice=True)
        session = self.patient_cql_connection(node3, 'ks')
        self.assertEqual(len(list(session.execute("SELECT * from cf"))), 1, "cf on node3")
        node4.start(wait_other_notice=True, wait_for_binary_proto=True)
        node3.flush()
        node3.stop(wait_other_notice=True)
        session = self.patient_cql_connection(node4, 'ks')
        self.assertEqual(len(list(session.execute("SELECT * from cf"))), 1, "cf on node4")

    def _repair_multiple_test(self, more_options=[]):
        """
        Starting multiple repairs in parallel from multiple nodes (without
        "-pr") is a waste, but besides being wasteful, should not cause any
        harm, and should produce correct results.
        """
        # Disable hinted handoff so it doesn't do what we expect repair to do
        self.cluster.set_configuration_options(values={'hinted_handoff_enabled': False})
        # Create a cluster of 3 nodes, and a keyspace with RF=3 on all nodes
        # (disable read repair, as we want to test the full repair).
        self.cluster.populate(3).start(wait_for_binary_proto=True, wait_other_notice=True)
        node1, node2, node3 = self.cluster.nodelist()
        session = self.patient_cql_connection(node1)
        self.create_ks(session, 'ks', 3)
        self.create_cf(session, 'cf', read_repair=0.0, columns={'c1': 'text', 'c2': 'text'})

        # Insert 1000 keys *only* on node 1, another 1000 keys *only* on node 2,
        # another 1000 *only on node 3:
        debug("Adding data only on node 1...")
        node2.flush()
        node2.stop(wait_other_notice=True)
        node3.flush()
        node3.stop(wait_other_notice=True)
        session = self.patient_cql_connection(node1, 'ks')
        insert_c1c2(session, keys=range(1000, 2000), consistency=ConsistencyLevel.ONE)
        self.cluster.flush()
        debug("Adding data only on node 2...")
        node2.start(wait_other_notice=True, wait_for_binary_proto=True)
        node1.flush()
        node1.stop(wait_other_notice=True)
        session = self.patient_cql_connection(node2, 'ks')
        insert_c1c2(session, keys=range(2000, 3000), consistency=ConsistencyLevel.ONE)
        debug("Adding data only on node 3...")
        node3.start(wait_other_notice=True, wait_for_binary_proto=True)
        node2.flush()
        node2.stop(wait_other_notice=True)
        session = self.patient_cql_connection(node3, 'ks')
        insert_c1c2(session, keys=range(3000, 4000), consistency=ConsistencyLevel.ONE)

        # Bring up all 3 nodes, each should have different data
        node1.start(wait_other_notice=True, wait_for_binary_proto=True)
        node2.start(wait_other_notice=True, wait_for_binary_proto=True)

        # Run repair on all three nods in parallel
        thread1 = threading.Thread(target=lambda: node1.repair(more_options + ['ks']))
        thread2 = threading.Thread(target=lambda: node2.repair(more_options + ['ks']))
        thread3 = threading.Thread(target=lambda: node3.repair(more_options + ['ks']))
        thread1.start()
        thread2.start()
        thread3.start()
        thread1.join()
        thread2.join()
        thread3.join()

        # Check that all nodes have all data
        self.check_rows_on_node(node1, 3000)
        self.check_rows_on_node(node2, 3000)
        self.check_rows_on_node(node3, 3000)

    def _repair_multiple_pr_test(self):
        """
        If a user plans to start repair from multiple nodes in parallel, he
        should at least use the "-pr" (partitioner range) option to avoid
        the waste of repairing the same data multiple times. Let's check that
        this actually works.
        """
        self.repair_multiple_test(['-pr'])

    def _repair_option_seq_test(self):
        """
        Test that the "-seq" repair options works. In Scylla, it doesn't
        actually change anything, but we need to test it doesn't do anything
        bad.
        """
        self.repair_disjoint_data_test(['-seq'])

    def repair_n_gt_rf(self, more_options=[]):
        """
        Another basic test for repair, this time we have more nodes than
        replication factor, so different ranges of tokens have a different
        set of replicas - so the repair is forced to retrieve different
        sections of the data from different replicas.
        """
        debug("Starting cluster...")
        # Disable hinted handoff so it doesn't do what we expect repair to do
        self.cluster.set_configuration_options(values={'hinted_handoff_enabled': False})
        # Create a cluster of 3 nodes, and a keyspace with RF=2 on all nodes
        # (disable read repair, as we want to test the full repair).
        self.cluster.populate(3).start(wait_for_binary_proto=True, wait_other_notice=True)
        node1, node2, node3 = self.cluster.nodelist()
        session = self.patient_cql_connection(node1)
        self.create_ks(session, 'ks', 2)
        self.create_cf(session, 'cf', read_repair=0.0, columns={'c1': 'text', 'c2': 'text'})

        # Insert 1000 keys while node 3 is down. Because RF=2, all the data
        # will have a replica in one of the two available nodes
        debug("Adding data with node 3 down...")
        node3.flush()
        node3.stop(wait_other_notice=True)
        session = self.patient_cql_connection(node1)
        session.set_keyspace('ks')
        insert_c1c2(session, keys=range(1000, 2000), consistency=ConsistencyLevel.ONE)
        self.cluster.flush()

        # Bring node 3 back up, it will not yet have any data
        node3.start(wait_other_notice=True, wait_for_binary_proto=True)

        # Run repair on node 3. It should copy to node 3 all data that node 3 should
        # hold
        time.sleep(10)  # see CASSANDRA-4373
        debug("starting repair...")
        info = node3.repair(more_options + ['ks'])
        debug(info[0])
        debug(info[1])

        # check that node 3 can read all 1000 partitions, even if node 1 or
        # or node 2 is down. Note that if both were down, it can't, because
        # about a third of the data is only replicated on node1 and node2.
        session = self.patient_cql_connection(node3, 'ks')
        debug("Checking read with no node down...")
        result = list(session.execute("SELECT * FROM cf LIMIT 2000"))
        self.assertEqual(len(result), 1000, len(result))
        debug("Checking read with node 1 down...")
        node1.flush()
        node1.stop(wait_other_notice=True)
        result = list(session.execute("SELECT * FROM cf LIMIT 2000"))
        self.assertEqual(len(result), 1000, len(result))
        node1.start(wait_other_notice=True, wait_for_binary_proto=True)
        debug("Checking read with node 2 down...")
        node2.flush()
        node2.stop(wait_other_notice=True)
        result = list(session.execute("SELECT * FROM cf LIMIT 2000"))
        self.assertEqual(len(result), 1000, len(result))
        node2.start(wait_other_notice=True, wait_for_binary_proto=True)

    def _repair_kill_1_test(self, kill_master=True):
        """
        Killing the master node of a repair stops the repair (obviously), but
        does not otherwise cause problems on the other nodes.
        """
        # Start a cluster of two nodes, and create a keyspace with RF=2, and
        # a table with one partition. Hinted handoff and read repair are disabled
        # so they don't fix the problems which repair is supposed to fix.
        self.cluster.set_configuration_options(values={'hinted_handoff_enabled': False})
        self.cluster.populate(2).start(wait_for_binary_proto=True, wait_other_notice=True)
        node1, node2 = self.cluster.nodelist()
        session = self.patient_cql_connection(node1)
        self.create_ks(session, 'ks', 2)
        self.create_cf(session, 'cf', read_repair=0.0, columns={'c1': 'text', 'c2': 'text'})
        # Insert 1000 keys *only* on node 1, another 1000 keys *only* on node 2
        node2.flush()
        node2.stop(wait_other_notice=True)
        session = self.patient_cql_connection(node1, 'ks')
        insert_c1c2(session, keys=range(1000, 2000), consistency=ConsistencyLevel.ONE)
        node2.start(wait_other_notice=True, wait_for_binary_proto=True)
        node1.flush()
        node1.stop(wait_other_notice=True)
        session = self.patient_cql_connection(node2, 'ks')
        insert_c1c2(session, keys=range(2000, 3000), consistency=ConsistencyLevel.ONE)
        node1.start(wait_other_notice=True, wait_for_binary_proto=True)

        # Run repair on node 1, and kill this node quickly after repair started
        def do_repair():
            try:
                info = node1.repair(['ks'])
                debug(info[0])
                debug(info[1])
            except (NodetoolError):
                pass
        thread1 = threading.Thread(target=do_repair)
        thread1.start()
        node1.watch_log_for("starting user-requested repair")
        time.sleep(random.uniform(0.0, 0.5))
        if kill_master:
            node1.stop(wait_other_notice=True)
        else:
            node2.stop(wait_other_notice=True)
        thread1.join()

        # Check that we can still read from the unkilled node normally.
        # We expect to see at least 1000 partitions - potentially up to
        # 2000 depending on how far the repair progressed.
        if kill_master:
            session = self.patient_cql_connection(node2, 'ks')
        else:
            session = self.patient_cql_connection(node1, 'ks')
        count = len(list(session.execute("SELECT * FROM cf LIMIT 3000")))
        debug("count is %d" % count)
        self.assertTrue(count >= 1000 and count <= 2000)

        if kill_master:
            node1.start(wait_other_notice=True, wait_for_binary_proto=True)
        else:
            node2.start(wait_other_notice=True, wait_for_binary_proto=True)

        # dtest.py, when the test is over, checks if there have been any
        # "ERROR" messages in the log, and if there have, it fails the test.
        # But in this test, "ERROR" messages are expected (e.g., errors about
        # failing parts of the repair), and we just want to ignore them.
        self.allow_log_errors = True

    def _repair_kill_2_test(self):
        """
        Killing a participant (non-master) of a repair stops the repair with
        an error. Note that this doesn't work on Cassandra - see
        https://support.datastax.com/hc/en-us/articles/204226119-Troubleshooting-hanging-repairs
        Moreover, the other nodes continue to work correctly.
        """
        self._repair_kill_1_test(False)

    def _repair_kill_3_test(self):
        """
        When a node busy in being a repair master is killed, check that it
        shuts down normally and doesn't crash because of shut down bugs.
        Scylla issue #699 caused this test to fail - the repair continues
        through the shutdown, and then crashes (with an assertion failure)
        when it suddenly noticed the data structures it uses are gone.
        """
        # Start a cluster of two nodes, and create a keyspace with RF=2, and
        # a table with one partition. Hinted handoff and read repair are disabled
        # so they don't fix the problems which repair is supposed to fix.
        self.cluster.set_configuration_options(values={'hinted_handoff_enabled': False})
        self.cluster.populate(2).start(wait_for_binary_proto=True, wait_other_notice=True)
        node1, node2 = self.cluster.nodelist()
        session = self.patient_cql_connection(node1)
        self.create_ks(session, 'ks', 2)
        self.create_cf(session, 'cf', read_repair=0.0, columns={'c1': 'text', 'c2': 'text'})
        # Insert 1000 keys *only* on node 1, another 1000 keys *only* on node 2
        node2.flush()
        node2.stop(wait_other_notice=True)
        session = self.patient_cql_connection(node1, 'ks')
        insert_c1c2(session, keys=range(1000, 2000), consistency=ConsistencyLevel.ONE)
        node2.start(wait_other_notice=True, wait_for_binary_proto=True)
        node1.flush()
        node1.stop(wait_other_notice=True)
        session = self.patient_cql_connection(node2, 'ks')
        insert_c1c2(session, keys=range(2000, 3000), consistency=ConsistencyLevel.ONE)
        node1.start(wait_other_notice=True, wait_for_binary_proto=True)

        # Run repair on node 1, and kill this node as soon as repair started
        def do_repair():
            try:
                info = node1.repair(['ks'])
                debug(info[0])
                debug(info[1])
            except (NodetoolError):
                pass
        thread1 = threading.Thread(target=do_repair)
        thread1.start()
        node1.watch_log_for("starting user-requested repair")
        node1.stop(wait_other_notice=True)
        thread1.join()

        # We don't want to see any assertion failures like in isue #699 :-(
        match = node1.grep_log("Assertion .* failed.")
        debug(match)
        self.assertEqual(len(match), 0)

    def _repair_during_update_test(self, more_options=[]):
        """
        Test that a repair works correctly in parallel with data being
        updated: We set up a cluster of two replicas with different data,
        and run a repair on it in parallel with adding more data to both
        nodes - and verify that at the end both nodes have all the data.
        """
        # Start a cluster of two nodes, and create a keyspace with RF=2, and
        # a table with one partition. Hinted handoff and read repair are disabled
        # so they don't fix the problems which repair is supposed to fix.
        self.cluster.set_configuration_options(values={'hinted_handoff_enabled': False})
        self.cluster.populate(2).start(wait_for_binary_proto=True, wait_other_notice=True)
        node1, node2 = self.cluster.nodelist()
        session = self.patient_cql_connection(node1)
        self.create_ks(session, 'ks', 2)
        self.create_cf(session, 'cf', read_repair=0.0, columns={'c1': 'text', 'c2': 'text'})

        # Insert 10000 keys *only* on node 1, another 10000 keys *only* on node 2:
        node2.flush()
        node2.stop(wait_other_notice=True)
        session = self.patient_cql_connection(node1, 'ks')
        insert_c1c2(session, keys=range(0, 10000), consistency=ConsistencyLevel.ONE)
        node2.start(wait_other_notice=True, wait_for_binary_proto=True)
        node1.flush()
        node1.stop(wait_other_notice=True)
        session = self.patient_cql_connection(node2, 'ks')
        insert_c1c2(session, keys=range(10000, 20000), consistency=ConsistencyLevel.ONE)
        node1.start(wait_other_notice=True, wait_for_binary_proto=True)

        # Run repair on node 1 in the background
        def do_repair():
            try:
                info = node1.repair(['ks'])
                debug(info[0])
                debug(info[1])
            except (NodetoolError):
                pass
        thread1 = threading.Thread(target=do_repair)
        thread1.start()

        # In parallel with the repair, for as long as it doesn't finish,
        # we write more data to both nodes
        original_count = 20000
        count = original_count
        session = self.patient_cql_connection(node1, 'ks')
        while thread1.is_alive():
            prev_count = count
            count = count + 1000
            insert_c1c2(session, keys=range(prev_count, count), consistency=ConsistencyLevel.TWO)
        debug("wrote %d partitions in parallel with repair" % (count - original_count))
        thread1.join()

        # Check that all nodes have all data
        self.check_rows_on_node(node1, count)
        self.check_rows_on_node(node2, count)

    def _repair_with_down_nodes_1_test(self, more_options=[]):
        """
        Test that a repair fails when no replica can be found for one of the
        ranges being repaired, because of a down node.
        """
        # Start a cluster of three nodes, and create a keyspace with RF=2, and
        # an empty table. We don't need any data in the table to check whether
        # repair complains about the missing neighbors.
        self.cluster.populate(3).start(wait_for_binary_proto=True, wait_other_notice=True)
        node1, node2, node3 = self.cluster.nodelist()
        session = self.patient_cql_connection(node1)
        self.create_ks(session, 'ks', 2)
        self.create_cf(session, 'cf', columns={'c1': 'text', 'c2': 'text'})

        # Bring down node 3, and start repair on node 2. Note that because we
        # have 3 nodes and RF=2, half of the vnodes in node 2 will have as
        # their only other replica the dead node 3.
        node3.stop(wait_other_notice=True)
        with self.assertRaises(NodetoolError):
            node2.repair(['ks'])

    def _repair_with_down_nodes_1a_test(self, more_options=[]):
        """
        When we have 3 nodes with RF=2, bring down one of the nodes and
        start repair on another. This repair will fail because for some of
        the vnodes, its second replica is the dead node. However, the
        purpose of this test is to verify that beyond the repair failing,
        it actually repairs what we could repair - i.e., data in vnodes
        replicated only in the live nodes. When the dead node later is
        brought back up, and repair is started on it, the entire cluster
        will become repaired and have all the partitions.
        Note that had the first repair done nothing, the second repair would
        not have been enough, because the second repair only repairs the
        token ranges held by one node - and these are not all the ranges.
        """
        # Start a cluster of 3 nodes, and a keyspace with RF=2, and a table.
        self.cluster.set_configuration_options(values={'hinted_handoff_enabled': False})
        self.cluster.populate(3).start(wait_for_binary_proto=True, wait_other_notice=True)
        node1, node2, node3 = self.cluster.nodelist()
        session = self.patient_cql_connection(node1)
        self.create_ks(session, 'ks', 2)
        self.create_cf(session, 'cf', read_repair=0.0, columns={'c1': 'text', 'c2': 'text'})
        # We want to put different data on node 1 and on node 2 so repair
        # of these nodes has something to do. We can't write specific
        # partitions specifically to node 1 directly because on 3 nodes with
        # RF=2, node 1 only carries part of the token ranges! So we need to
        # write to pairs of nodes - the pair 1&3 and the pair 2&3.
        node2.flush()
        node2.stop(wait_other_notice=True)
        session = self.patient_cql_connection(node1, 'ks')
        insert_c1c2(session, keys=range(0, 1000), consistency=ConsistencyLevel.ONE)
        # let ConsistencyLevel.ONE delayed replication succeed (to node 3) or
        # timeout (to node 2)
        time.sleep(10)

        node2.start(wait_other_notice=True, wait_for_binary_proto=True)
        node1.flush()
        node1.stop(wait_other_notice=True)
        session = self.patient_cql_connection(node2, 'ks')
        insert_c1c2(session, keys=range(1000, 2000), consistency=ConsistencyLevel.ONE)
        time.sleep(10)

        node1.start(wait_other_notice=True, wait_for_binary_proto=True)

        # Shut down node 3, and start repair on node 2. Note that because we
        # have 3 nodes and RF=2, half of the vnodes in node 2 will have as
        # their only other replica the dead node 3, so the repair is supposed
        # to fail (this was also tested by the previous test function).
        # But the other half of the vnodes to have their other replica alive
        # (node 1), and may be repaired by the repair command.
        node3.flush()
        node3.stop(wait_other_notice=True)

        # Before the repair, doing SELECT * will return *around* (but not
        # exactly!) 1000 partitions. We can't check it because it's not
        # exactly 1000.
        session = self.patient_cql_connection(node2, 'ks')
        result = list(session.execute("SELECT * from cf"))
        debug(len(result))

        with self.assertRaises(NodetoolError):
            node2.repair(['ks'])

        # Check whether despite node 3 being down and the repair failing,
        # the vnodes whose replicas are node 1 and 2 (these are one half of
        # the data on node 2) could have been repaired. Unfortunately, we
        # cannot do a "SELECT *" on just node 2 because it is missing some
        # of the ranges. So we need to run the query with both node 1 and 2
        # alive (we can't be sure which of these will be queried, but we
        # assume that after a repair they will have the same data for
        # ranges they both own).
        # Despite the above repair failing, it did something. We will now
        # see about 1300 partitions in the following query. But we can't
        # check this number because it is not exact.
        session = self.patient_cql_connection(node2, 'ks')
        result = list(session.execute("SELECT * from cf"))
        debug(len(result))

        # Bring up also node 3. Trying "SELECT *" again will still not show
        # all 2000 partitions, because we still have different data in node 3
        # and node 2 because node 3 was down during the above repair.
        node3.start(wait_other_notice=True, wait_for_binary_proto=True)
        session = self.patient_cql_connection(node2, 'ks')
        result = list(session.execute("SELECT * from cf"))
        debug(len(result))

        # Repair node 3's ranges. This will not repair the ranges held only
        # by node 1 and 2, but we were hoping that the failed repair above
        # already did this. So after this additional repair, so should finally
        # have the full 2000 partitions.
        node3.repair(['ks'])
        session = self.patient_cql_connection(node2, 'ks')
        result = list(session.execute("SELECT * from cf"))
        self.assertEqual(len(result), 2000)

    def _repair_with_down_nodes_2_test(self, more_options=[]):
        """
        Test that a repair fails when one of the replicas of one of the ranges
        being repaired is missing. The fact that another replica does exist is
        not enough.
        """
        # Start a cluster of 4 nodes, and create a keyspace with RF=3, and
        # an empty table. We don't need any data in the table to check whether
        # repair complains about the missing neighbors.
        self.cluster.populate(4).start(wait_for_binary_proto=True, wait_other_notice=True)
        node1, node2, node3, node4 = self.cluster.nodelist()
        session = self.patient_cql_connection(node1)
        self.create_ks(session, 'ks', 3)
        self.create_cf(session, 'cf', columns={'c1': 'text', 'c2': 'text'})

        # Bring down node 3, and start repair on node 2. Note that because we
        # have 4 nodes and RF=3, 2/3rds of the vnodes in node 2 will have the
        # dead node 3 as one of their replicas.
        node3.stop(wait_other_notice=True)
        with self.assertRaises(NodetoolError):
            node2.repair(['ks'])

    def _repair_with_down_nodes_2a_test(self, more_options=[]):
        """
        This test is similar to repair_with_down_nodes_1a_test, except we
        have 4 nodes with RF=3, and shut down node 4.
        Because RF=3, repair's failure mechanism is now slightly differently
        the one the 1a test: In this test, all token ranges (vnodes) have at
        least one other replica alive (as opposed to 1a where some of them
        had no living replica to repair with); Some ranges have all replicas
        alive (in nodes 1,2,3) and can be fully repaired as in test 1a. Yet
        other token ranges have one replica alive and one dead (in node 4),
        and we want to check whether we make an effort to repair between these
        living replicas, or not.

        For the same test we did in 1a - of whether a second repair of dead node
        once it comes up completes the repair of everything - it is enough
        that the partial repair only repairs ranges for which all replicas
        is alive. This test does NOT test what the partial repair did with the
        ranges for which one of the replicas was dead. Test 2b below does that.
        """
        # Start a cluster of 4 nodes, and a keyspace with RF=3, and a table.
        self.cluster.set_configuration_options(values={'hinted_handoff_enabled': False})
        self.cluster.populate(4).start(wait_for_binary_proto=True, wait_other_notice=True)
        node1, node2, node3, node4 = self.cluster.nodelist()
        session = self.patient_cql_connection(node1)
        self.create_ks(session, 'ks', 3)
        self.create_cf(session, 'cf', read_repair=0.0, columns={'c1': 'text', 'c2': 'text'})
        # We want to put different data on node 1 and on node 2 so repair
        # of these nodes has something to do. We can't write specific
        # partitions specifically to node 1 directly because on 4 nodes with
        # RF=3, node 1 only carries part of the token ranges. So we need to
        # write to triplets of nodes - the pair 1,3,4 and the pair 2,3,4.
        node2.flush()
        node2.stop(wait_other_notice=True)
        session = self.patient_cql_connection(node1, 'ks')
        insert_c1c2(session, keys=range(0, 1000), consistency=ConsistencyLevel.ONE)
        # let ConsistencyLevel.ONE delayed replication succeed (to node 3,4) or
        # timeout (to node 2)
        time.sleep(10)

        node2.start(wait_other_notice=True, wait_for_binary_proto=True)
        node1.flush()
        node1.stop(wait_other_notice=True)
        session = self.patient_cql_connection(node2, 'ks')
        insert_c1c2(session, keys=range(1000, 2000), consistency=ConsistencyLevel.ONE)
        time.sleep(10)

        node1.start(wait_other_notice=True, wait_for_binary_proto=True)

        # Shut down node 4, and start repair on node 2.
        node4.flush()
        node4.stop(wait_other_notice=True)

        session = self.patient_cql_connection(node2, 'ks')
        result = list(session.execute("SELECT * from cf"))
        debug(len(result))

        with self.assertRaises(NodetoolError):
            node2.repair(['ks'])

        # NOTE: In test 1a, at this point we needed to bring back the down
        # node and repair it, before we have all the data available for query.
        # But in this test, because of the RF=3, if repair tried hard enough
        # to use the living replicas and not give up prematurely, at this
        # point we could have already seen the full data. We don't test this
        # in this test (we will below, in test 2b), and merely test that like
        # in test 1a, another repair of the revived node will make all the
        # data available.
        session = self.patient_cql_connection(node2, 'ks')
        result = list(session.execute("SELECT * from cf"))
        debug(len(result))

        node4.start(wait_other_notice=True, wait_for_binary_proto=True)
        session = self.patient_cql_connection(node2, 'ks')
        result = list(session.execute("SELECT * from cf"))
        debug(len(result))

        # Repair node 4's ranges. This will not repair the ranges held only
        # by node 1,2,3, but we were hoping that the failed repair above
        # already did this. So after this additional repair, so should finally
        # have the full 2000 partitions.
        node4.repair(['ks'])
        session = self.patient_cql_connection(node2, 'ks')
        result = list(session.execute("SELECT * from cf"))
        debug(len(result))
        self.assertEqual(len(result), 2000)

    def _repair_with_down_nodes_2b_test(self, more_options=[]):
        """
        This is a stricter version of test 2a above. We keep it as a separate
        test because it fails miserably on Apache Cassandra (and older
        versions of Scylla). In this test we confirm that when repair sees
        some replicas are dead and some are alive, it does its best to
        repair the data between the live nodes, instead of giving up early.
        Apparently neither Apache Cassandra nor old versions of Scylla tried
        hard enough.
        """
        # Start a cluster of 4 nodes, and a keyspace with RF=3, and a table.
        self.cluster.set_configuration_options(values={'hinted_handoff_enabled': False})
        self.cluster.populate(4).start(wait_for_binary_proto=True, wait_other_notice=True)
        node1, node2, node3, node4 = self.cluster.nodelist()
        session = self.patient_cql_connection(node1)
        self.create_ks(session, 'ks', 3)
        self.create_cf(session, 'cf', read_repair=0.0, columns={'c1': 'text', 'c2': 'text'})
        # We want to put different data on node 1 and on node 2 so repair
        # of these nodes has something to do. We can't write specific
        # partitions specifically to node 1 directly because on 4 nodes with
        # RF=3, node 1 only carries part of the token ranges. So we need to
        # write to triplets of nodes - the pair 1,3,4 and the pair 2,3,4.
        node2.flush()
        node2.stop(wait_other_notice=True)
        session = self.patient_cql_connection(node1, 'ks')
        insert_c1c2(session, keys=range(0, 1000), consistency=ConsistencyLevel.TWO)
        # let ConsistencyLevel.TWO delayed replication succeed (to node 3,4) or
        # timeout (to node 2)
        time.sleep(10)

        node2.start(wait_other_notice=True, wait_for_binary_proto=True)
        node1.flush()
        node1.stop(wait_other_notice=True)
        session = self.patient_cql_connection(node2, 'ks')
        insert_c1c2(session, keys=range(1000, 2000), consistency=ConsistencyLevel.TWO)
        time.sleep(10)

        node1.start(wait_other_notice=True, wait_for_binary_proto=True)

        # Shut down node 4, and start repair on nodes 1,2,3. To repair all
        # the ranges held by these three nodes, we unfortunately need to
        # start a full repair on two of them - "-pr" repair would not be
        # enough because the ranges whose primary is the dead node 4 will
        # not be repaired.
        # The purpose of this test is to confirm whether the repair done on
        # the 3 living nodes will try hard enough to reconcile their data despite
        # the fact that some of the replicas - on node 4 - are not available.
        node4.flush()
        node4.stop(wait_other_notice=True)
        session = self.patient_cql_connection(node2, 'ks')
        result = list(session.execute("SELECT * from cf"))
        debug(len(result))

        with self.assertRaises(NodetoolError):
            node2.repair(['ks'])
        with self.assertRaises(NodetoolError):
            node3.repair(['ks'])

        # Try "SELECT *" again, with 4 still down. This should already return
        # the full list of 2000 partitions, even without repairing node 4 (or
        # bringing it up), because we have RF=3 so none of the data lives only
        # on node 4, and if repair was diligent enough, it could repair the
        # 3 living nodes.
        session = self.patient_cql_connection(node2, 'ks')
        result = list(session.execute("SELECT * from cf"))
        debug(len(result))
        self.assertEqual(len(result), 2000)

    def _repair_abort_test(self):
        """
        Add different data to each node, then start repair in background,
        try to abort repair before complete, verify the repair streaming stops,
        and some keys aren't synced.
        """
        # Disable hinted handoff so it doesn't do what we expect repair to do
        self.cluster.set_configuration_options(values={'hinted_handoff_enabled': False})
        # Create a cluster of 3 nodes, and a keyspace with RF=3 on all nodes
        # (disable read repair, as we want to test the full repair).
        self.cluster.populate(3).start(wait_for_binary_proto=True, wait_other_notice=True)
        node1, node2, node3 = self.cluster.nodelist()
        session = self.patient_exclusive_cql_connection(node1)
        self.create_ks(session, 'ks', 3)
        self.create_cf(session, 'cf', read_repair=0.0, columns={'c1': 'text', 'c2': 'text'})

        keys_unit = 3000
        # Insert 3000 keys *only* on node 1, another 3000 keys *only* on node 2,
        # another 3000 *only on node 3:
        debug("Adding data only on node 1...")
        node2.flush()
        node2.stop(wait_other_notice=True)
        node3.flush()
        node3.stop(wait_other_notice=True)
        session = self.patient_cql_connection(node1)
        session.set_keyspace('ks')
        insert_c1c2(session, keys=range(0, keys_unit), consistency=ConsistencyLevel.ONE)
        self.cluster.flush()

        debug("Adding data only on node 2...")
        node2.start(wait_other_notice=True, wait_for_binary_proto=True)
        node1.flush()
        node1.stop(wait_other_notice=True)
        session = self.patient_cql_connection(node2)
        session.set_keyspace('ks')
        insert_c1c2(session, keys=range(keys_unit, 2 * keys_unit), consistency=ConsistencyLevel.ONE)

        debug("Adding data only on node 3...")
        node3.start(wait_other_notice=True, wait_for_binary_proto=True)
        node2.flush()
        node2.stop(wait_other_notice=True)
        session = self.patient_cql_connection(node3)
        session.set_keyspace('ks')
        insert_c1c2(session, keys=range(2 * keys_unit, 3 * keys_unit), consistency=ConsistencyLevel.ONE)

        # Bring up all 3 nodes, each should have different data
        node1.start(wait_other_notice=True, wait_for_binary_proto=True)
        node2.start(wait_other_notice=True, wait_for_binary_proto=True)

        # Run repair on (arbitrarily), node 3
        time.sleep(10)  # see CASSANDRA-4373
        debug("starting repair...")

        def checking_keys_num(prefix='', less_than_num=None):
            rows = 3 * keys_unit
            for node_to_check in self.cluster.nodes.values():
                stopped_nodes = []
                for node in self.cluster.nodes.values():
                    if node.is_running() and node is not node_to_check:
                        stopped_nodes.append(node)
                        node.stop(wait_other_notice=True)

                session = self.patient_cql_connection(node_to_check, 'ks')
                result = list(session.execute("SELECT * FROM cf LIMIT %d" % (rows * 2)))
                debug('%s - %s, keys num: %s' % (prefix, node_to_check.name, len(result)))
                if less_than_num:
                    assert len(result) <= less_than_num

                for node in stopped_nodes:
                    node.start(wait_other_notice=True)

        def repair_thread(more_options):
            try:
                debug('Start repair')
                info = self._repair(node3, more_options)
                debug(info[0])
                debug(info[1])
            except Exception as ex:
                debug(ex)
                output = commands.getoutput('curl http://%s:10000/stream_manager/' % self.get_ip_from_node(node3))
                assert 'repair-' not in output
                checking_keys_num('After Repair Exception')

        checking_keys_num('Before Repair')
        thread1 = threading.Thread(target=repair_thread, args=(['ks'], ))
        thread1.start()

        found_repair_sessions = False
        for i in range(600):
            output = commands.getoutput('curl http://%s:10000/stream_manager/' % self.get_ip_from_node(node3))
            if 'repair-' in output:
                debug('Found repair stream sessions')
                found_repair_sessions = True
                break
            time.sleep(0.01)
        assert found_repair_sessions, 'repair stream sessions must exist before abort'

        debug('Abort repair sessions')
        url = "http://%s:10000/storage_service/force_terminate_repair" % self.get_ip_from_node(node3)
        commands.getoutput('curl -X POST  --header "Accept: application/json" %s' % url)
        thread1.join(timeout=120)

        debug('Sleep 10 seconds')
        time.sleep(10)
        self.cluster.flush()
        checking_keys_num('After Abort', less_than_num=keys_unit * 3)

    @skip('unimplemented')
    def _repair_of_cluster_all_nodes_are_out_of_sync(self):
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

    @skip('unimplemented')
    def _full_repair_of_node_initiated_on_node_with_latest_data_test(self):
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

    @skip('unimplemented')
    def _full_repair_of_node_initiated_on_node_without_data(self):
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

    @skip('unimplemented')
    def _repair_fixes_updates_to_cells_test(self):
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

    @skip('unimplemented')
    def _repair_fixes_remove_of_keys_test(self):
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

    @skip('unimplemented')
    def _repair_fixes_deletion_of_cells_test(self):
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

    @skip('unimplemented')
    def _repair_fixes_deletion_of_range_of_cells_test(self):
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

    @skip('unimplemented')
    def _repair_fixes_update_of_ttl_test(self):
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

    @skip('unimplemented')
    def _fail_node_initiating_repair_test(self):
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

    @skip('unimplemented')
    def _fail_node_responding_to_repair_test(self):
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

    @skip('unimplemented')
    def _repair_while_data_is_updated_test(self):
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

    @skip('unimplemented')
    def _repair_while_nodes_are_down_1_test(self):
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

    @skip('unimplemented')
    def _repair_while_nodes_are_down_2_test(self):
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

    @skip('unimplemented')
    def _repair_while_new_node_is_added_test(self):
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

    @skip('unimplemented')
    def _repair_while_node_is_decomissioned_test(self):
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

class RepairAdditionalTest(RepairAdditionalBase):
    __test__ = True

    def repair_disjoint_data_test(self, more_options=[]):
        return RepairAdditionalBase._repair_disjoint_data_test(self,more_options)

    def repair_schema_test(self):
        return RepairAdditionalBase._repair_schema_test(self)

    def repair_schema_2_test(self):
        return RepairAdditionalBase._repair_schema_2_test(self)

    def repair_cell_update_test(self):
       return RepairAdditionalBase._repair_cell_update_test(self)

    def repair_cell_delete_test(self):
       return RepairAdditionalBase._repair_cell_delete_test(self)

    def repair_row_delete_test(self):
       return RepairAdditionalBase._repair_row_delete_test(self)

    def repair_partition_delete_test(self):
       return RepairAdditionalBase._repair_partition_delete_test(self)

    def repair_ttl_update_test(self):
       return RepairAdditionalBase._repair_ttl_update_test(self)

    def repair_option_pr_test(self):
       return RepairAdditionalBase._repair_option_pr_test(self)

    def repair_option_pr_dc_host_test(self):
       return RepairAdditionalBase._repair_option_pr_dc_host_test(self)

    def repair_option_pr_multi_dc_test(self):
       return RepairAdditionalBase._repair_option_pr_multi_dc_test(self)

    def repair_option_cf_test(self):
       return RepairAdditionalBase._repair_option_cf_test(self)

    def repair_option_invalid_ks_cf_test(self):
       return RepairAdditionalBase._repair_option_invalid_ks_cf_test(self)

    def repair_option_dc_test(self):
       return RepairAdditionalBase._repair_option_dc_test(self)

    def repair_multiple_test(self, more_options=[]):
       return RepairAdditionalBase._repair_multiple_test(self)

    def repair_multiple_pr_test(self):
       return RepairAdditionalBase._repair_multiple_pr_test(self)

    def repair_option_seq_test(self):
       return RepairAdditionalBase._repair_option_seq_test(self)

    def repair_kill_1_test(self, kill_master=True):
       return RepairAdditionalBase._repair_kill_1_test(self)

    def repair_kill_2_test(self):
       return RepairAdditionalBase._repair_kill_2_test(self)

    def repair_kill_3_test(self):
       return RepairAdditionalBase._repair_kill_3_test(self)

    def repair_during_update_test(self, more_options=[]):
       return RepairAdditionalBase._repair_during_update_test(self,more_options)

    def repair_with_down_nodes_1_test(self, more_options=[]):
       return RepairAdditionalBase._repair_with_down_nodes_1_test(self,more_options)

    def repair_with_down_nodes_1a_test(self, more_options=[]):
       return RepairAdditionalBase._repair_with_down_nodes_1a_test(self,more_options)

    def repair_with_down_nodes_2_test(self, more_options=[]):
       return RepairAdditionalBase._repair_with_down_nodes_2_test(self,more_options)

    def repair_with_down_nodes_2a_test(self, more_options=[]):
       return RepairAdditionalBase._repair_with_down_nodes_2a_test(self,more_options)

    def repair_with_down_nodes_2b_test(self, more_options=[]):
       return RepairAdditionalBase._repair_with_down_nodes_2b_test(self,more_options)

    def repair_abort_test(self):
       return RepairAdditionalBase._repair_abort_test(self)
