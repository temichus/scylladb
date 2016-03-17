import threading
import time
import logging
from datetime import datetime

from cassandra import Unavailable,ConsistencyLevel,WriteTimeout,OperationTimedOut
from cassandra.policies import FallthroughRetryPolicy
from cassandra.query import SimpleStatement
from cassandra.cluster import NoHostAvailable
from ccmlib.node import NodeError

from dtest import Tester, debug
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

        session = self.patient_cql_connection(node_to_check, 'ks')
        result = session.execute("SELECT * FROM cf LIMIT %d" % (rows * 2))
        self.assertEqual(len(result), rows, len(result))

        for k in found:
            query_c1c2(session, k, ConsistencyLevel.ONE)

        for k in missings:
            query_c1c2(session, k, ConsistencyLevel.ONE, must_be_missing=True)

        if restart:
            self.start_all_nodes()

    def start_all_nodes(self):
        nodes_marks = []
        for node in self.cluster.nodes.values():
            if node.is_running():
                nodes_marks.append((node, node.mark_log()))
            else:
                nodes_marks.append((node, None))

        for node in self.cluster.nodes.values():
            if not node.is_running():
                node.start(wait_other_notice=True, wait_for_binary_proto=True)

        for node, mark in nodes_marks:
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

        session = self.patient_cql_connection(node1)
        self.create_ks(session, 'ks', 2)
        self.create_cf(session, 'cf', read_repair=0.0, columns={'c1': 'text', 'c2': 'text'})

        insert_c1c2(session, keys=range(1000), consistency=ConsistencyLevel.ONE)

        node2 = new_node(cluster)
        node2.start(wait_for_binary_proto=True)

        session = self.patient_exclusive_cql_connection(node2)
        session.execute("use ks;");
        node1.watch_log_for_alive(node2)
        node2.watch_log_for_alive(node1)

        insert_c1c2(session, keys=range(1000, 2000), consistency=ConsistencyLevel.TWO)

        self.check_rows_on_node(node2, 2000)
        self.check_rows_on_node(node1, 2000)

    def _iterative_add_decommission(self,iterations=2,node_count=2,rf=1):
        """
        Test gorwing and shrinking a cluster
        1. Create a cluster with a single node with rf=2, insert data
        2. In a loop add new nodes
        3. Check that all data exists
        4. In a loop remove all nodes but the last added one
        5. Check that all data exists
        """
        cluster = self.cluster

        self.allow_log_errors = True

        # Disable hinted handoff and set batch commit log so this doesn't
        # interfer with the test (this must be after the populate)
        cluster.set_configuration_options(values={'hinted_handoff_enabled': False}, batch_commitlog=True)
        cluster.populate(1).start()
        node1 = cluster.nodelist()[0]

        session = self.patient_cql_connection(node1)
        self.create_ks(session, 'ks', rf)
        self.create_cf(session, 'cf', read_repair=0.0, columns={'c1': 'text', 'c2': 'text'})

        consistency=ConsistencyLevel.ALL
        insert_c1c2(session, keys=range(1000), consistency=ConsistencyLevel.ONE)

        query = SimpleStatement("SELECT key FROM ks.cf limit 3000", consistency_level=consistency)
        for iteration in range(1,iterations+1):
            for i in range(1,node_count+1):
                node_i = new_node(cluster)
                node_i.start(wait_for_binary_proto=True,wait_other_notice=True)
                session_i = self.patient_exclusive_cql_connection(node_i)
                session_i.execute("use ks;");
                insert_c1c2(session_i, keys=range(iteration*100000+i*2000,iteration*100000+i*2000+100), consistency=consistency)
                debug("added %s" % node_i.name)


            result = session.execute(query)
            self.assertEqual(len(result), iteration*node_count*100+1000, "data loss after increasing size to %d expecting %d rows %d" % (len(cluster.nodelist()),iteration*node_count*100+1000,len(result)))

            for node_i in cluster.nodelist()[0:-1]:
                if node1.name != node_i.name and node_i.is_live():
                    node_i.decommission()
                    node_i.stop()
                    debug("decommissioned %s" % node_i.name)

            last_node = cluster.nodelist()[-1]
            session = self.patient_cql_connection(last_node)
            result = session.execute("SELECT key FROM ks.cf limit 3000")
            self.assertEqual(len(result), iteration*node_count*100+1000, "data loss after shrinking to 2 node execpeting %d rows %d" % (iteration*node_count*100+1000,len(result)))

        node1.decommission()
        node1.stop()
        debug("decommissioned %s" % node1.name)
        last_node = cluster.nodelist()[-1]
        session = self.patient_cql_connection(last_node)
        result = session.execute("SELECT key FROM ks.cf limit 3000")
        self.assertEqual(len(result), iterations*node_count*100+1000, "data loss after shrinking to 1 node %s expecting %d rows %d" % (last_node.name,iterations*node_count*100+1000,len(result)))

    def iterative_add_1_node_decommission_1_node_rf_1_test(self):
        """
        Test gorwing and shrinking a cluster 1 node in each iteration with rf=1
        """
        self._iterative_add_decommission(node_count=1,iterations=3,rf=1)

    def iterative_add_3_node_decommission_3_node_rf_1_test(self):
        """
        Test gorwing and shrinking a cluster 3 node in each iteration with rf=1
        """
        self._iterative_add_decommission(node_count=3,iterations=2,rf=1)

    def iterative_add_1_node_decommission_1_node_rf_2_test(self):
        """
        Test gorwing and shrinking a cluster 1 node in each iteration with rf=2
        """
        self._iterative_add_decommission(node_count=1,iterations=3,rf=2)

    def iterative_add_3_node_decommission_3_node_rf_2_test(self):
        """
        Test gorwing and shrinking a cluster 3 node in each iteration with rf=2
        """
        self._iterative_add_decommission(node_count=3,iterations=2,rf=2)

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

        session = self.patient_cql_connection(node1)
        self.create_ks(session, 'ks', 3)
        self.create_cf(session, 'cf', read_repair=0.0, columns={'c1': 'text', 'c2': 'text'})

        insert_c1c2(session, keys=range(1000), consistency=ConsistencyLevel.ONE)

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
            node3.start(wait_other_notice=True,wait_for_binary_proto=True)
            # lets check that it detected there was another bootstrapping in progress
	    node3.watch_log_for("Checking bootstrapping/leaving/moving nodes: .* sleep 1 second and check again .*")
	    node3.watch_log_for("Checking bootstrapping/leaving/moving nodes: ok");
        except NodeError:
            # if the node was not allowed to boot check reason
            node3.watch_log_for("Other bootstrapping/leaving/moving nodes detected, cannot bootstrap while cassandra.consistent.rangemovement is true")
            pass

        node2.watch_log_for("Starting listening for CQL clients")
        session = self.patient_exclusive_cql_connection(node2)
        session.execute("use ks;")
        node1.watch_log_for_alive(node2)
        node2.watch_log_for_alive(node1)

        insert_c1c2(session, keys=range(1000, 2000), consistency=ConsistencyLevel.TWO)

        self.check_rows_on_node(node2, 2000)
        self.check_rows_on_node(node1, 2000)

    def simple_kill_streaming_node_while_bootstrapping_test(self):
        """
        Test bootstrapped node streams all data
        1. Create a cluster with a three nodes with rf=3, insert data
        2. Add node, wait for node to start bootstrappig
        3. Kill original cluster node while it is streaming info to the new node
        4. Check that the new node has all data
        """
        cluster = self.cluster
        self.allow_log_errors = True

        # Disable hinted handoff and set batch commit log so this doesn't
        # interfer with the test (this must be after the populate)
        cluster.set_configuration_options(values={'hinted_handoff_enabled': False}, batch_commitlog=True)
        cluster.populate(3).start()
        node1, node2, node3 = cluster.nodelist()

        session = self.patient_cql_connection(node1)
        self.create_ks(session, 'ks', 4)
        self.create_cf(session, 'cf', read_repair=0.0, columns={'c1': 'text', 'c2': 'text'})

        insert_c1c2(session, keys=range(1000), consistency=ConsistencyLevel.THREE)

        debug("Inserting more data to make streaming process longer...")
        node1.stress(['write', 'n=20000', 'no-warmup', '-schema', 'replication(factor=3) keyspace=ks1'])
        node2.stress(['write', 'n=20000', 'no-warmup', '-schema', 'replication(factor=3) keyspace=ks2'])
        node3.stress(['write', 'n=20000', 'no-warmup', '-schema', 'replication(factor=3) keyspace=ks3'])

        debug("Flush cluster...")
        self.cluster.flush()

        debug("Start node 4...")
        node4 = new_node(cluster)
        node4.start()
        node4.watch_log_for("Beginning stream session")

        debug("Stop node 2...")
        node2.stop()

        debug("Look for Stream failed in node 4...")
	# The keep alive timer expires in 10 minutes.
	# Wait 2 minutes more in the test to wait for the stream to fail
        node4.watch_log_for("Stream failed", timeout=720)

    def simple_kill_new_node_while_bootstrapping_test(self):
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
        node2 = cluster.nodelist()[1]
        node3 = cluster.nodelist()[2]

        session = self.patient_cql_connection(node1)
        self.create_ks(session, 'ks', 1)
        self.create_cf(session, 'cf', read_repair=0.0, columns={'c1': 'text', 'c2': 'text'})

        insert_c1c2(session, keys=range(1000), consistency=ConsistencyLevel.ONE)

        debug("Inserting more data to make streaming process longer...")
        node1.stress(['write', 'n=5000', 'no-warmup', '-schema', 'replication(factor=3) keyspace=ks1'])
        node2.stress(['write', 'n=5000', 'no-warmup', '-schema', 'replication(factor=3) keyspace=ks2'])
        node3.stress(['write', 'n=5000', 'no-warmup', '-schema', 'replication(factor=3) keyspace=ks3'])

        for i in xrange(4, 5):
            # creating an additional node without actually adding it to the cluster
            new_node = cluster.create_node('node%s' % i,
                                           True,
                                           ('127.0.0.%s' % i, 9160),
                                           ('127.0.0.%s' % i, 7000),
                                           str(7000 + i * 100),
                                           None,
                                           None,
                                           binary_interface=('127.0.0.%s' % i, 9042))
            debug("Start Node %d" % i);
            new_node.start()
            new_node.watch_log_for("JOINING: Starting to bootstrap")
            new_node.watch_log_for("Beginning stream session")
            debug("Stop Node %d" % i);
            new_node.stop(gently=False)

            # Sleep 1 second to make sure other nodes knows this node is joining through gossip
            time.sleep(1)

            # Check the status:
            # We expect the new node will not be added to the cluster
            # status looks like below, new_node should not be in UN state but in UJ state.
            # UN  127.0.0.1  99823      256     ?       a7498138-1878-421d-8f11-cc98b204090a  rack1
            # UN  127.0.0.2  37278      256     ?       f118383c-c569-49d1-9aa6-223d3b224caa  rack1
            # UN  127.0.0.3  24834      256     ?       78b7e6ba-3039-4fc6-a875-a71661f8cd04  rack1
            # UJ  127.0.0.4  ?          256     ?       637edd3f-8888-48ab-b0ea-3ea81f8e9865  rack1
            status, err = node1.nodetool('status')
            assert status.find("UJ  127.0.0.4 ") > -1, status

            # Slep 30 seconds to make sure other nodes removed the new node
            time.sleep(30)
            node1.watch_log_for("FatClient .* has been silent for 30000ms, removing from gossip")
            node2.watch_log_for("FatClient .* has been silent for 30000ms, removing from gossip")
            node3.watch_log_for("FatClient .* has been silent for 30000ms, removing from gossip")

            # Check status again:
            # status looks like below, new_node should not be in UN state
            # UN  127.0.0.1  99823      256     ?       a7498138-1878-421d-8f11-cc98b204090a  rack1
            # UN  127.0.0.2  37278      256     ?       f118383c-c569-49d1-9aa6-223d3b224caa  rack1
            # UN  127.0.0.3  24834      256     ?       78b7e6ba-3039-4fc6-a875-a71661f8cd04  rack1
            status, err = node1.nodetool('status')
            assert status.find("127.0.0.4") == -1, status

        result = session.execute("SELECT * FROM cf")
        self.assertEqual(len(result), 1000, len(result))

    def simple_kill_new_node_while_bootstrapping_with_parallel_writes_test(self):
        """
        Test bootstrapped node streams all data
        1. Create a cluster with a three nodes with rf=1, insert data
        2. Add node, wait for each to start bootstrapping and write additional data
        3. kill it while writting data
        4. Check that the operation exists with an expected exception
        """

        cluster = self.cluster
        self.allow_log_errors = True

        # Disable hinted handoff and set batch commit log so this doesn't
        # interfer with the test (this must be after the populate)
        cluster.set_configuration_options(values={'hinted_handoff_enabled': False}, batch_commitlog=True)
        cluster.populate(3).start(wait_for_binary_proto=True, wait_other_notice=True)
        node1 = cluster.nodelist()[0]
        node2 = cluster.nodelist()[1]
        node3 = cluster.nodelist()[2]

        session = self.cql_connection(node1)
        session.default_timeout = 60.0
        self.create_ks(session, 'ks', 1)
        self.create_cf(session, 'cf', read_repair=0.0, columns={'c1': 'text', 'c2': 'text'})
        statement = session.prepare("INSERT INTO cf (key, c1, c2) VALUES (?, 'value1', 'value2')")
        session.execute(statement,('k1',))

        insert_c1c2(session, keys=range(1000), consistency=ConsistencyLevel.ONE)

        debug("Inserting more data to make streaming process longer...")
        node1.stress(['write', 'n=5000', 'no-warmup', '-schema', 'replication(factor=3) keyspace=ks1'])
        node2.stress(['write', 'n=5000', 'no-warmup', '-schema', 'replication(factor=3) keyspace=ks2'])
        node3.stress(['write', 'n=5000', 'no-warmup', '-schema', 'replication(factor=3) keyspace=ks3'])

        for i in xrange(4, 5):
            # creating an additional node without actually adding it to the cluster
            new_node = cluster.create_node('node%s' % i,
                                           True,
                                           ('127.0.0.%s' % i, 9160),
                                           ('127.0.0.%s' % i, 7000),
                                           str(7000 + i * 100),
                                           None,
                                           None,
                                           binary_interface=('127.0.0.%s' % i, 9042))
            event = threading.Event()
            failed = None
            def run():
                try:
                   debug("start write")
                   for key in range(2000,4000):
                       # working around the default retry_policy that attempts 5 times
                       statement = SimpleStatement("INSERT INTO cf (key, c1, c2) VALUES ('k%d', 'value1', 'value2')" % key ,consistency_level=ConsistencyLevel.ONE, retry_policy=FallthroughRetryPolicy())
                       tbefore=str(datetime.now())
                       session.execute(statement)
                   debug("end write")
                   failed='insert should have failed'
                except (Unavailable) as e:
                   tfailed=str(datetime.now())
                   debug("exception thrown Unavailable %s" % e);
                   pass
                except (WriteTimeout) as e:
                   tfailed=str(datetime.now())
                   debug("exception thrown WriteTimeout %s" % e);
                   pass
                except (OperationTimedOut) as e:
                   tfailed=str(datetime.now())
                   failed="Server side escrption not thrown  driver side exception thrown OperationTimeout %s %s %s" % (e,tbefore,tfailed)
                event.set()
            t = threading.Thread(target=run)
            t.setDaemon(True)

            debug("Start Node %d" % i);
            new_node.start()
            new_node.watch_log_for("JOINING: Starting to bootstrap")
            t.start()
            new_node.watch_log_for("Beginning stream session")
            debug("Stop Node %d" % i);
            new_node.stop(gently=False)
            event.wait()
            self.assertTrue(failed==None,failed)

            # Sleep 1 second to make sure other nodes knows this node is joining through gossip
            time.sleep(1)

        session.execute("SELECT * FROM cf")

    def simple_kill_new_node_while_bootstrapping_with_parallel_writes_in_multidc_test(self):
        """
        Test bootstrapped node streams all data
        1. Create a cluster with two nodes in multidc with rf=1, insert data
        2. Add node, wait for each to start bootstrapping and write additional data
        3. kill it
        4. Check that the cluster returns all
        """
        cluster = self.cluster
        self.allow_log_errors = True

        # Disable hinted handoff and set batch commit log so this doesn't
        # interfer with the test (this must be after the populate)
        cluster.set_configuration_options(values={'hinted_handoff_enabled': False}, batch_commitlog=True)
        cluster.populate([1,1]).start(wait_for_binary_proto=True, wait_other_notice=True)
        node1 = cluster.nodelist()[0]
        node2 = cluster.nodelist()[1]

        session = self.cql_connection(node1)
        session.default_timeout = 60.0
        self.create_ks(session, 'ks', {'dc1': 1, 'dc2': 1})
        self.create_cf(session, 'cf', read_repair=0.0, columns={'c1': 'text', 'c2': 'text'})

        insert_c1c2(session, keys=range(1000), consistency=ConsistencyLevel.EACH_QUORUM)

        debug("Inserting more data to make streaming process longer...")
        node1.stress(['write', 'n=5000', 'no-warmup', '-schema', 'replication(factor=3) keyspace=ks1'])
        node2.stress(['write', 'n=5000', 'no-warmup', '-schema', 'replication(factor=3) keyspace=ks2'])

        # create a new node and adding it - we cannot do this more then once
        a_new_node = new_node(cluster,data_center='dc1')
        event = threading.Event()
        failed = None
        def run():
            try:
               debug("start write")
               for key in range(2000,4000):
                   # working around the default retry_policy that attempts 5 times
                   statement = SimpleStatement("INSERT INTO cf (key, c1, c2) VALUES ('k%d', 'value1', 'value2')" % key ,consistency_level=ConsistencyLevel.EACH_QUORUM, retry_policy=FallthroughRetryPolicy())
                   tbefore=str(datetime.now())
                   session.execute(statement)
               debug("end write")
               failed='insert should have failed'
            except (Unavailable) as e:
               tfailed=str(datetime.now())
               debug("exception thrown Unavailable %s" % e);
               pass
            except (WriteTimeout) as e:
               tfailed=str(datetime.now())
               debug("exception thrown WriteTimeout %s" % e);
               pass
            except (OperationTimedOut) as e:
               tfailed=str(datetime.now())
               failed="Server side escrption not thrown  driver side exception thrown OperationTimeout %s %s %s" % (e,before,failed)
            event.set()

        t = threading.Thread(target=run)
        t.setDaemon(True)

        debug("Start Node");
        a_new_node.start()
        a_new_node.watch_log_for("JOINING: Starting to bootstrap")
        time.sleep(1)
        t.start()
        time.sleep(1)
        a_new_node.watch_log_for("Beginning stream session")
        debug("Stop Node");
        a_new_node.stop(gently=False)
        event.wait()
        self.assertTrue(failed==None,failed)

        # Sleep 1 second to make sure other nodes knows this node is joining through gossip
        time.sleep(1)

        session.execute("SELECT * FROM cf")

    def _simple_add_new_node_while_adding_info(self, rf):
        """
        Test bootstrapped node streams all data
        1. Create a cluster with a three nodes with rf, insert data
        3. Add node, while node is bootstrapping insert data
        4. Check that the cluster returns all
        """
        cluster = self.cluster
        self.allow_log_errors = True
        consistency = {1: ConsistencyLevel.ONE, 2: ConsistencyLevel.TWO}[rf]

        # Disable hinted handoff and set batch commit log so this doesn't
        # interfer with the test (this must be after the populate)
        cluster.set_configuration_options(values={'hinted_handoff_enabled': False}, batch_commitlog=True)
        cluster.populate(3).start()
        node1 = cluster.nodelist()[0]

        session = self.patient_cql_connection(node1)
        self.create_ks(session, 'ks', rf)
        self.create_cf(session, 'cf', read_repair=0.0, columns={'c1': 'text', 'c2': 'text'})

        insert_c1c2(session, keys=range(2000), consistency=consistency)

        event = threading.Event()

        def run():
            insert_c1c2(session, keys=range(2000, 4000), consistency=consistency)
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
        result = session.execute(query)
        self.assertEqual(len(result), 4000, len(result))

        for k in xrange(0, 4000):
            query_c1c2(session, k, consistency)

    def simple_add_new_node_while_adding_info_1_test(self):
        self._simple_add_new_node_while_adding_info(1)

    def simple_add_new_node_while_adding_info_2_test(self):
        self._simple_add_new_node_while_adding_info(2)

    def simple_add_new_node_while_schema_changes_test(self):
        """
        Test bootstrapped node streams all data
        1. Create a cluster with a three nodes with rf=1, insert data
        2. Add node, while node is bootstrapping remove keyspace
        3. Still while bootstrapping add a keyspace and insert data
        4. Check that node was connected and the cluster returns all
        """
        cluster = self.cluster
        self.allow_log_errors = True
        rf = 1
        consistency = {1: ConsistencyLevel.ONE, 2: ConsistencyLevel.TWO}[rf]

        # Disable hinted handoff and set batch commit log so this doesn't
        # interfer with the test (this must be after the populate)
        cluster.set_configuration_options(values={'hinted_handoff_enabled': False}, batch_commitlog=True)
        cluster.populate(3).start()
        node1 = cluster.nodelist()[0]

        session = self.patient_cql_connection(node1)
        self.create_ks(session, 'ks', rf)
        self.create_cf(session, 'cf', read_repair=0.0, columns={'c1': 'text', 'c2': 'text'})

        insert_c1c2(session, keys=range(4000), consistency=consistency)

        event = threading.Event()

        def run():
            query = SimpleStatement("DROP KEYSPACE ks")
            result = session.execute(query)

            self.create_ks(session, 'ks1', rf)
            self.create_cf(session, 'cf1', read_repair=0.0, columns={'c1': 'text', 'c2': 'text'})
            for i in xrange(0, 100):
                insert = SimpleStatement("insert into ks1.cf1 (key,c1,c2) values ('%d','%d','%d')" % (i, i, i), consistency_level=consistency)
                session.execute(insert)
            event.set()
            pass

        t = threading.Thread(target=run)
        t.setDaemon(True)

        node4 = new_node(cluster)
        node4.start()
        node4.watch_log_for("Beginning stream session")
        t.start()

        node4.watch_log_for("Starting listening for CQL clients")
        session = self.patient_cql_connection(node4)

        event.wait()
        query = SimpleStatement("SELECT * FROM ks1.cf1", consistency_level=consistency)
        result = session.execute(query)
        self.assertEqual(len(result), 100, len(result))

    def _simple_add_new_node_while_query_info(self, rf):
        """
        Test bootstrapped node streams all data
        1. Create a cluster with a three nodes with rf, insert data
        3. Add node, while node is bootstrapping query data
        4. Check that the cluster returns all
        """
        cluster = self.cluster
        self.allow_log_errors = True
        consistency = {1: ConsistencyLevel.ONE, 2: ConsistencyLevel.TWO}[rf]

        # Disable hinted handoff and set batch commit log so this doesn't
        # interfer with the test (this must be after the populate)
        cluster.set_configuration_options(values={'hinted_handoff_enabled': False}, batch_commitlog=True)
        cluster.populate(3).start()
        node1 = cluster.nodelist()[0]

        session = self.patient_cql_connection(node1)
        self.create_ks(session, 'ks', rf)
        self.create_cf(session, 'cf', read_repair=0.0, columns={'c1': 'text', 'c2': 'text'})

        insert_c1c2(session, keys=range(2000), consistency=consistency)

        event = threading.Event()

        def run():
            for i in xrange(1, 100):
                query = SimpleStatement("SELECT * FROM cf", consistency_level=consistency)
                result = session.execute(query)
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

        result = session.execute(query)
        self.assertEqual(len(result), 2000, len(result))
        for k in xrange(0, 2000):
            query_c1c2(session, k, consistency)

        event.wait()

    def simple_add_new_node_while_query_info_1_test(self):
        self._simple_add_new_node_while_query_info(1)

    def simple_add_new_node_while_query_info_2_test(self):
        self._simple_add_new_node_while_query_info(2)

    def simple_decommission_node_1_test(self):
        """
        Test decommissioned node streams all data
        1. Create a cluster with a single node with rf=1, insert data
        2. Decommission one node
        3. Check that the last node has all the data
        """
        cluster = self.cluster

        # Disable hinted handoff and set batch commit log so this doesn't
        # interfer with the test (this must be after the populate)
        cluster.set_configuration_options(values={'hinted_handoff_enabled': False}, batch_commitlog=True)
        cluster.populate(2).start(wait_for_binary_proto=True, wait_other_notice=True)
        node1, node2 = cluster.nodelist()

        session = self.patient_cql_connection(node1)
        self.create_ks(session, 'ks', 1)
        self.create_cf(session, 'cf', read_repair=0.0, columns={'c1': 'text', 'c2': 'text'})

        insert_c1c2(session, keys=range(1000), consistency=ConsistencyLevel.ONE)

        node2.decommission()
        # lets verify new connection can not be openned to a decomissioned node
        try:
            session2 = self.patient_cql_connection(node2)
            fail
        except NoHostAvailable:
            pass
        node2.stop()

        self.check_rows_on_node(node1, 1000, restart=False)

    def simple_decommission_node_2_test(self):
        """
        Test that on decommission row cache entries of non owned transfeered range are invalidated

        1. Create a cluster with a single node with rf=1,insert data
        2. Check the row cahe can be used as an estimator
        3. Add a new node
        4. Check that all data can be read
        5. Delete all the data
        6. Compact data on new node
        7. Restart new node (it should not have any data including tombstones)
        8. Decommission the new node
        9. Test if any data exists in the cluster
        """

        cluster = self.cluster

        # Disable hinted handoff and set batch commit log so this doesn't
        # interfer with the test (this must be after the populate)
        cluster.set_configuration_options(values={'hinted_handoff_enabled': False}, batch_commitlog=True)
        cluster.populate(1).start()
        node1 = cluster.nodelist()[0]

        session_node1 = self.patient_cql_connection(node1)
        self.create_ks(session_node1, 'ks', 1)
        self.create_cf(session_node1, 'cf', gc_grace=0, read_repair=0.0, columns={'c1': 'text', 'c2': 'text'})

        node1.flush()

        insert_c1c2(session_node1, keys=range(1000), consistency=ConsistencyLevel.ONE)

        node1.flush()

        # We booted the new node and it got part of the items
        node2 = new_node(cluster)
        node2.start(wait_for_binary_proto=True,wait_other_notice=True)

        session_node2 = self.patient_exclusive_cql_connection(node2)
        session_node2.execute("use ks;")
        node1.watch_log_for_alive(node2)
        node2.watch_log_for_alive(node1)

        result = session_node1.execute("SELECT * FROM ks.cf limit 2000;")
        self.assertEqual(len(result), 1000, "expected 1000 lines got %d" % len(result))

        session_node1.execute("DELETE from ks.cf where key in (\'k%s\');" % "\',\'k".join(str(x) for x in range(1000)))

        result = session_node1.execute("SELECT * FROM ks.cf limit 2000;")
        self.assertEqual(len(result), 0, "expected 0 lines got %d %s" % (len(result),result))

        cluster.flush()
        # lets make sure all the data in ssstables is removed
        node2.compact()
        # restart the node to make sure no data is left
        node2.stop()
        node2.start(wait_for_binary_proto=True,wait_other_notice=True)

        node2.decommission()
        node1.flush()

        result = session_node1.execute("SELECT * FROM ks.cf")
        self.assertEqual(len(result), 0, "expected 0 lines got %d" % len(result))

    def simple_kill_node_while_decommissioning_test(self):
        """
        Test a decommissioning node killed is able to rejoin the cluster with data
        1. Create a cluster with a three nodes with rf=1, insert data
        2. Decommission a node
        3. While node is decommissioning kill it
        4. Boot the node back up
        5. Check that the node rejoins the cluster and works correctly
        """
        cluster = self.cluster
        self.allow_log_errors = True

        # Disable hinted handoff and set batch commit log so this doesn't
        # interfer with the test (this must be after the populate)
        cluster.set_configuration_options(values={'hinted_handoff_enabled': False}, batch_commitlog=True)
        cluster.populate(3).start(wait_for_binary_proto=True, wait_other_notice=True)
        node1, node2, node3 = cluster.nodelist()

        session = self.patient_cql_connection(node1)
        self.create_ks(session, 'ks', 1)
        self.create_cf(session, 'cf', read_repair=0.0, columns={'c1': 'text', 'c2': 'text'})

        insert_c1c2(session, keys=range(1000), consistency=ConsistencyLevel.ONE)

        def run():
            try:
                node2.decommission()
            except Exception:
                pass

        t = threading.Thread(target=run)
        t.setDaemon(True)
        t.start()

        # check node2 has started decommission
        node2.watch_log_for("Beginning stream session")
        node2.stop(gently=False)

        # starting node2 - it should reconnect and run as is
        node2.start(wait_other_notice=True, wait_for_binary_proto=True)
        result = session.execute("SELECT * FROM cf")
        self.assertEqual(len(result), 1000, len(result))

    def _simple_decommission_node_while_adding_info(self, rf):
        """
        Test bootstrapped node streams all data
        1. Create a cluster with a three nodes with rf, insert data
        2. Decommission node, while node is decommissioning insert data
        3. Check that the cluster returns all
        """
        cluster = self.cluster
        self.allow_log_errors = True
        consistency = {1: ConsistencyLevel.ONE, 2: ConsistencyLevel.TWO}[rf]

        # Disable hinted handoff and set batch commit log so this doesn't
        # interfer with the test (this must be after the populate)
        cluster.set_configuration_options(values={'hinted_handoff_enabled': False}, batch_commitlog=True)
        cluster.populate(3).start()
        node1, node2, node3 = cluster.nodelist()

        session = self.patient_cql_connection(node1)
        self.create_ks(session, 'ks', rf)
        self.create_cf(session, 'cf', read_repair=0.0, columns={'c1': 'text', 'c2': 'text'})

        insert_c1c2(session, keys=range(2000), consistency=consistency)

        event = threading.Event()

        def run():
            insert_c1c2(session, keys=range(2000, 4000), consistency=consistency)

            query = SimpleStatement("SELECT * FROM cf", consistency_level=consistency)
            result = session.execute(query)
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
        result = session.execute(query)
        self.assertEqual(len(result), 4000, len(result))
        for k in xrange(0, 4000):
            query_c1c2(session, k, consistency)

    def simple_decommission_node_while_adding_info_1_test(self):
        self._simple_decommission_node_while_adding_info(1)

    def simple_decommission_node_while_adding_info_2_test(self):
        self._simple_decommission_node_while_adding_info(2)

    def _simple_decommission_node_while_query_info(self, rf):
        """
        Test decommissioning node streams all data
        1. Create a cluster with a three nodes with rf, insert data
        2. Decommission node, while node is decommissioning query data
        3. Check that the cluster returns all
        """
        cluster = self.cluster
        self.allow_log_errors = True
        consistency = {1: ConsistencyLevel.ONE, 2: ConsistencyLevel.TWO}[rf]

        # Disable hinted handoff and set batch commit log so this doesn't
        # interfer with the test (this must be after the populate)
        cluster.set_configuration_options(values={'hinted_handoff_enabled': False}, batch_commitlog=True)
        cluster.populate(3).start()
        node1, node2, node3 = cluster.nodelist()

        session = self.patient_cql_connection(node1)
        self.create_ks(session, 'ks', rf)
        self.create_cf(session, 'cf', read_repair=0.0, columns={'c1': 'text', 'c2': 'text'})

        insert_c1c2(session, keys=range(2000), consistency=consistency)

        event = threading.Event()

        def run():
            for i in xrange(1, 100):
                query = SimpleStatement("SELECT * FROM cf", consistency_level=consistency)
                result = session.execute(query)
                self.assertEqual(len(result), 2000, len(result))
                time.sleep(0.01)
            event.set()
            pass

        t = threading.Thread(target=run)
        t.setDaemon(True)
        t.start()

        node2.decommission()

        query = SimpleStatement("SELECT * FROM cf", consistency_level=consistency)
        result = session.execute(query)
        self.assertEqual(len(result), 2000, len(result))

        node2.stop()

        query = SimpleStatement("SELECT * FROM cf", consistency_level=consistency)
        result = session.execute(query)
        self.assertEqual(len(result), 2000, len(result))
        for k in xrange(0, 2000):
            query_c1c2(session, k, consistency)

        event.wait()

    def simple_decommission_node_while_query_info_1_test(self):
        self._simple_decommission_node_while_query_info(1)

    def simple_decommission_node_while_query_info_2_test(self):
        self._simple_decommission_node_while_query_info(2)

    def simple_removenode_1_test(self):
        """
        Test removenode with rf>1 (no data should be lost)
        1. Create a cluster with a two node with rf=2, insert data
        2. stop and remove a node
        3. Check that the data is accesible
        """
        cluster = self.cluster

        # Disable hinted handoff and set batch commit log so this doesn't
        # interfer with the test (this must be after the populate)
        cluster.set_configuration_options(values={'hinted_handoff_enabled': False}, batch_commitlog=True)
        cluster.populate(3).start()
        node1,node2,node3 = cluster.nodelist()

        session = self.patient_cql_connection(node1)
        self.create_ks(session, 'ks', 2)
        self.create_cf(session, 'cf', read_repair=0.0, columns={'c1': 'text', 'c2': 'text'})

        insert_c1c2(session, keys=range(100), consistency=ConsistencyLevel.ALL)

        node2_hostid = node2.hostid()
        node2.stop(wait_other_notice=True)
        query = SimpleStatement("SELECT * FROM cf", consistency_level=ConsistencyLevel.ONE)
        result = session.execute(query)
        self.assertEqual(len(result), 100, len(result))

        node1.nodetool("removenode %s" % node2_hostid)
        time.sleep(2)
        query = SimpleStatement("SELECT * FROM cf", consistency_level=ConsistencyLevel.TWO)
        result = session.execute(query)
        self.assertEqual(len(result), 100, len(result))
        insert_c1c2(session, keys=range(120), consistency=ConsistencyLevel.TWO)

    def simple_removenode_2_test(self):
        """
        Test removenode when rf=1 (data will be lost)
        1. Create a cluster with a two node with rf=1, insert data
        2. stop and remove a node
        3. Check that the data is accesible
        """
        cluster = self.cluster

        # Disable hinted handoff and set batch commit log so this doesn't
        # interfer with the test (this must be after the populate)
        cluster.set_configuration_options(values={'hinted_handoff_enabled': False}, batch_commitlog=True)
        cluster.populate(2).start()
        node1,node2 = cluster.nodelist()

        session = self.patient_cql_connection(node1)
        self.create_ks(session, 'ks', 1)
        self.create_cf(session, 'cf', read_repair=0.0, columns={'c1': 'text', 'c2': 'text'})

        insert_c1c2(session, keys=range(10), consistency=ConsistencyLevel.ALL)

        node2_hostid = node2.hostid()
        node2.stop(wait_other_notice=True)
        try:
            query = SimpleStatement("SELECT * FROM cf", consistency_level=ConsistencyLevel.ONE)
            result = session.execute(query)
            fail
        except Unavailable:
            pass

        node1.nodetool("removenode %s" % node2_hostid)
        insert_c1c2(session, keys=range(10), consistency=ConsistencyLevel.ALL)
        query = SimpleStatement("SELECT * FROM cf", consistency_level=ConsistencyLevel.ONE)
        result = session.execute(query)
        self.assertEqual(len(result), 10, len(result))

    def _add_new_node_while_add_new_table(self, when):
        """
        Test bootstrapped node get data in the new table
        1. Create a cluster with a three nodes with rf=1, insert data
        2. Add node, while node is bootstrapping add new table and insert data
        4. Check that node was connected and the cluster returns all data inserted
        """
        cluster = self.cluster
        self.allow_log_errors = True
        rf = 1
        consistency = {1: ConsistencyLevel.ONE, 2: ConsistencyLevel.TWO}[rf]

        # Disable hinted handoff and set batch commit log so this doesn't
        # interfer with the test (this must be after the populate)
        cluster.set_configuration_options(values={'hinted_handoff_enabled': False}, batch_commitlog=True)
        cluster.populate(3).start()
        node1 = cluster.nodelist()[0]

        session = self.patient_cql_connection(node1)
        self.create_ks(session, 'ks', rf)
        self.create_cf(session, 'cf', read_repair=0.0, columns={'c1': 'text', 'c2': 'text'})

        insert_c1c2(session, keys=range(4000), consistency=consistency)

        event = threading.Event()

        def run():
            self.create_ks(session, 'ks1', rf)
            self.create_cf(session, 'cf1', read_repair=0.0, columns={'c1': 'text', 'c2': 'text'})
            for i in xrange(0, 1000):
                insert = SimpleStatement("insert into ks1.cf1 (key,c1,c2) values ('%d','%d','%d')" % (i, i, i), consistency_level=consistency)
                session.execute(insert)

            event.set()
            pass

        t = threading.Thread(target=run)
        t.setDaemon(True)

	# Create table and insert data before bootstrapping of the new node
	if when == "before":
            t.start()

        node4 = new_node(cluster)
        node4.start()
        node4.watch_log_for("Beginning stream session")
	# Create table and insert data during bootstrapping of the new node
	if when == "during":
            t.start()

        node4.watch_log_for("Starting listening for CQL clients")
        session = self.patient_cql_connection(node4)

	# Create table and insert data after bootstrapping of the new node
	if when == "after":
            t.start()

        event.wait()
        query = SimpleStatement("SELECT * FROM ks1.cf1", consistency_level=consistency)
        result = session.execute(query)
        self.assertEqual(len(result), 1000, len(result))

    def add_new_node_while_add_new_table_before_bootstrapping_test(self):
        self._add_new_node_while_add_new_table("before");

    def add_new_node_while_add_new_table_during_bootstrapping_test(self):
        self._add_new_node_while_add_new_table("during");

    def add_new_node_while_add_new_table_after_bootstrapping_test(self):
        self._add_new_node_while_add_new_table("after");

    def _get_gossipinfo(self, output):
        """
        Parse gossipinfo output and put it into a python dict.

        Trailing slash on node ips is removed.

        :param output: 'nodetool gossip' stdout
        :returns: Dict with nodetool info. Example follows.
        {'127.0.0.1': {'DC': 'datacenter1',
                       'HOST_ID': 'bb821819-9049-4929-b7cc-7b2edf1eec10',
                       'LOAD': '128982',
                       'NET_VERSION': '0',
                       'RACK': 'rack1',
                       'RELEASE_VERSION': '2.1.8',
                       'RPC_ADDRESS': '127.0.0.1',
                       'SCHEMA': '2576e940-0936-3ff6-a12c-9c4ed9571175',
                       'STATUS': 'NORMAL,996695790724469087',
                       'generation': '1457611493',
                       'heartbeat': '118'}}
        """
        gossipinfo = {}
        current_node = None
        for line in output.splitlines():
            line = line.strip()
            try:
                if current_node and current_node not in gossipinfo:
                    gossipinfo[current_node] = {}
                key, value = line.split(':')
                gossipinfo[current_node].update({key: value})
            except:
                current_node = line[1:]
        return gossipinfo


    def remove_node_from_gossip_test(self):
        """
        Test a node can be removed from gossip
        1. Create a cluster with 3 nodes
        2. Add node, wait for node to start bootstrappig
        3. Kill the new node
        4. Check that the new node will be removed from
        4. Check gossip on_remove callback in storage_service will not cause deadlock
        """
        cluster = self.cluster
        self.allow_log_errors = True

        # Disable hinted handoff and set batch commit log so this doesn't
        # interfere with the test (this must be after the populate)
        cluster.set_configuration_options(values={'hinted_handoff_enabled': False}, batch_commitlog=True)
        cluster.populate(3).start()
        node1, node2, node3 = cluster.nodelist()

        session = self.patient_cql_connection(node1)
        self.create_ks(session, 'ks', 2)
        self.create_cf(session, 'cf', read_repair=0.0, columns={'c1': 'text', 'c2': 'text'})

        insert_c1c2(session, keys=range(2000), consistency=ConsistencyLevel.TWO)

        debug("Start node 4...")
        node4 = new_node(cluster)
        node4.start()
        node4.watch_log_for("Beginning stream session")

        debug("Stop node 4 ...")
        node4.stop()

        debug("Check node 1 removed node4  ...")
        node1.watch_log_for("FatClient .* has been silent for .*ms, removing from gossip")

        debug("Check the hearbeat of node 1 ...")
        status1, err1 = node1.nodetool('gossipinfo')
        gossipinfo_1 = self._get_gossipinfo(status1)
        heartbeat_1 = int(gossipinfo_1['127.0.0.1']['heartbeat'])

        time.sleep(3)

        debug("Check the hearbeat of node 1 updated ...")
        status2, err2 = node1.nodetool('gossipinfo')
        gossipinfo_2 = self._get_gossipinfo(status2)
        heartbeat_2 = int(gossipinfo_2['127.0.0.1']['heartbeat'])
        e_msg = ("Heartbeat for status 2 '%s' is not greater than for status 1 '%s', something is wrong" % (heartbeat_2, heartbeat_1))
        debug("heartbeat_2 = %d, heartbeat_1 = %d" % (heartbeat_2, heartbeat_1))
        self.assertGreater(heartbeat_2, heartbeat_1, e_msg)
