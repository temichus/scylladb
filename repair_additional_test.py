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

    def repair_disjoint_data_test(self, more_options=[]):
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
        self.cluster.populate(3).start()
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
        info = node3.repair(more_options + ['ks'])
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
        debug("Starting cluster...")
        # Start a cluster of two nodes, and create a keyspace with RF=2.
        # Do *not* create a table yet - we'll do that with one node down
        self.cluster.set_configuration_options(values={'hinted_handoff_enabled': False})
        self.cluster.populate(2).start()
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
        info = node1.repair(['ks'])
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
        debug("Starting cluster...")
        # Start a cluster of two nodes, and create a keyspace with RF=2.
        # Do *not* create a table yet - we'll do that with one node down
        self.cluster.set_configuration_options(values={'hinted_handoff_enabled': False})
        self.cluster.populate(2).start()
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
        info = node2.repair(['ks'])
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
        debug("Starting cluster and inserting data...")
        # Start a cluster of two nodes, and create a keyspace with RF=2, and
        # a table with one partition. Hinted handoff and read repair are disabled
        # so they don't fix the problems which repair is supposed to fix
        self.cluster.set_configuration_options(values={'hinted_handoff_enabled': False})
        self.cluster.populate(2).start()
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
        info = node2.repair(['ks'])
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

    def repair_cell_delete_test(self):
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
        self.cluster.populate(2).start()
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
        info = node2.repair(['ks'])
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

    def repair_row_delete_test(self):
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
        self.cluster.populate(2).start()
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
        info = node2.repair(['ks'])
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

    def repair_partition_delete_test(self):
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
        self.cluster.populate(2).start()
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
        info = node2.repair(['ks'])
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

    def repair_ttl_update_test(self):
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
        self.cluster.populate(2).start()
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
            if '["c1",' in line:
                self.assertTrue('"e",1234,' in line, "TTL set to 1234")
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
        info = node2.repair(['ks'])
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
            if '["c1",' in line:
                if save_line == line:
                    save_line = None
        self.assertTrue(save_line is None, "expected c1 value and timeout in sstable")

    def repair_option_pr_test(self):
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
        self.cluster.populate(2).start()
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

    def repair_option_cf_test(self):
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
        self.cluster.populate(2).start()
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
        self.assertEqual(len(session.execute("SELECT * from cf2")), 2, "cf2 on node2")

    def repair_option_invalid_ks_cf_test(self):
        """
        Test that specifying a non-existant keyspace or column family to
        repair results in failure.
        """
        self.cluster.set_configuration_options(values={'hinted_handoff_enabled': False})
        self.cluster.populate(2).start()
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

    def repair_option_dc_test(self):
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
        self.cluster.populate([2, 1, 1]).start()
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
        self.assertEqual(len(session.execute("SELECT * from cf")), 1, "cf on node3")

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
        self.assertEqual(len(session.execute("SELECT * from cf")), 2, "cf on node2")
        node3.start(wait_other_notice=True, wait_for_binary_proto=True)
        node2.flush()
        node2.stop(wait_other_notice=True)
        session = self.patient_cql_connection(node3, 'ks')
        self.assertEqual(len(session.execute("SELECT * from cf")), 1, "cf on node3")
        node4.start(wait_other_notice=True, wait_for_binary_proto=True)
        node3.flush()
        node3.stop(wait_other_notice=True)
        session = self.patient_cql_connection(node4, 'ks')
        self.assertEqual(len(session.execute("SELECT * from cf")), 1, "cf on node4")

    def repair_multiple_test(self, more_options=[]):
        """
        Starting multiple repairs in parallel from multiple nodes (without
        "-pr") is a waste, but besides being wasteful, should not cause any
        harm, and should produce correct results.
        """
        # Disable hinted handoff so it doesn't do what we expect repair to do
        self.cluster.set_configuration_options(values={'hinted_handoff_enabled': False})
        # Create a cluster of 3 nodes, and a keyspace with RF=3 on all nodes
        # (disable read repair, as we want to test the full repair).
        self.cluster.populate(3).start()
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

    def repair_multiple_pr_test(self):
        """
        If a user plans to start repair from multiple nodes in parallel, he
        should at least use the "-pr" (partitioner range) option to avoid
        the waste of repairing the same data multiple times. Let's check that
        this actually works.
        """
        self.repair_multiple_test(['-pr'])

    def repair_option_par_test(self):
        """
        Test that the "-par" repair options works. In Scylla, it doesn't
        actually change anything, but we need to test it doesn't do anything
        bad.
        """
        self.repair_disjoint_data_test(['-par'])

    def repair_kill_1_test(self, kill_master=True):
        """
        Killing the master node of a repair stops the repair (obviously), but
        does not otherwise cause problems on the other nodes.
        """
        # Start a cluster of two nodes, and create a keyspace with RF=2, and
        # a table with one partition. Hinted handoff and read repair are disabled
        # so they don't fix the problems which repair is supposed to fix.
        self.cluster.set_configuration_options(values={'hinted_handoff_enabled': False})
        self.cluster.populate(2).start()
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

    def repair_kill_2_test(self):
        """
        Killing a participant (non-master) of a repair stops the repair with
        an error. Note that this doesn't work on Cassandra - see
        https://support.datastax.com/hc/en-us/articles/204226119-Troubleshooting-hanging-repairs
        Moreover, the other nodes continue to work correctly.
        """
        self.repair_kill_1_test(False)

    def repair_kill_3_test(self):
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
        self.cluster.populate(2).start()
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

    def repair_during_update_test(self, more_options=[]):
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
        self.cluster.populate(2).start()
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

    def repair_with_down_nodes_1_test(self, more_options=[]):
        """
        Test that a repair fails when no replica can be found for one of the
        ranges being repaired, because of a down node.
        """
        # Start a cluster of three nodes, and create a keyspace with RF=2, and
        # an empty table. We don't need any data in the table to check whether
        # repair complains about the missing neighbors.
        self.cluster.populate(3).start()
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

    def repair_with_down_nodes_1a_test(self, more_options=[]):
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
        self.cluster.populate(3).start()
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

    def repair_with_down_nodes_2_test(self, more_options=[]):
        """
        Test that a repair fails when one of the replicas of one of the ranges
        being repaired is missing. The fact that another replica does exist is
        not enough.
        """
        # Start a cluster of 4 nodes, and create a keyspace with RF=3, and
        # an empty table. We don't need any data in the table to check whether
        # repair complains about the missing neighbors.
        self.cluster.populate(4).start()
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

    def repair_with_down_nodes_2a_test(self, more_options=[]):
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
        self.cluster.populate(4).start()
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

    def repair_with_down_nodes_2b_test(self, more_options=[]):
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
        self.cluster.populate(4).start()
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

    @skip('unimplemented')
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

    @skip('unimplemented')
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

    @skip('unimplemented')
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

    @skip('unimplemented')
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

    @skip('unimplemented')
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

    @skip('unimplemented')
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

    @skip('unimplemented')
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

    @skip('unimplemented')
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

    @skip('unimplemented')
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

    @skip('unimplemented')
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

    @skip('unimplemented')
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

    @skip('unimplemented')
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

    @skip('unimplemented')
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

    @skip('unimplemented')
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

    @skip('unimplemented')
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
