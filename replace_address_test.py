import datetime
import threading

from time import sleep
from unittest import skip
from nose.plugins.attrib import attr

from cassandra import ConsistencyLevel, ReadTimeout, Unavailable, ReadFailure
from cassandra.query import SimpleStatement

from assertions import assert_row_count, assert_all
from ccmlib.node import NodeError
from dtest import DISABLE_VNODES, Tester, debug
from tools import InterruptBootstrap, since, new_node, require, rows_to_list, insert_c1c2
import scylla_tools

from concurrent.futures import ThreadPoolExecutor


class NodeUnavailable(Exception):
    pass


@attr('dtest-full')
class TestReplaceAddress(Tester):

    rbo_enabled = False
    __test__ = False

    def __init__(self, *args, **kwargs):
        kwargs['cluster_options'] = {'start_rpc': 'true'}
        # Ignore these log patterns:
        self.ignore_log_patterns = [
            # This one occurs when trying to send the migration to a
            # node that hasn't started yet, and when it does, it gets
            # replayed and everything is fine.
            r'Can\'t send migration request: node.*is down',
            # This is caused by starting a node improperly (replacing active/nonexistent)
            r'Exception encountered during startup',
            # This is caused by trying to replace a nonexistent node
            r'Exception in thread Thread',
            # ignore streaming error during bootstrap
            r'Streaming error occurred'
        ]
        Tester.__init__(self, *args, **kwargs)

    def init_cluster(self, num_nodes=3, configuration_options={}):
        rbo_status = "true" if self.rbo_enabled else "false"
        configuration_options.update({"enable_repair_based_node_ops": rbo_status})
        debug(f"Setting cluster configuration options: {configuration_options}")
        self.cluster.populate(num_nodes)
        self.cluster.set_configuration_options(values=configuration_options)
        self.cluster.start(no_wait=False, wait_for_binary_proto=True, wait_other_notice=True)

    def replace_stopped_node_test(self):
        """
        Test that we can replace a node that is not shutdown gracefully.
        """
        self._replace_node_test(gently=False)

    def get_sorted_tokens(self, node):
        # sorted([line.split()[-1] for line in node3.nodetool('ring')[0].splitlines()
        #         if node3.address() in line])

        ring_lines = [line for line in node.nodetool('ring')[0].splitlines() if node.address() in line]
        tokens_list = [token.split()[-1] for token in ring_lines]
        return sorted(tokens_list)

    def replace_shutdown_node_test(self):
        """
        @jira_ticket CASSANDRA-9871
        Test that we can replace a node that is shutdown gracefully.
        """
        self._replace_node_test(gently=True)

    def _replace_node_test(self, gently):
        """
        Check that the replace address function correctly replaces a node that has failed in a cluster.
        Create a cluster, cause a node to fail, and bring up a new node with the replace_address parameter.
        Check that tokens are migrated and that data is replicated properly.
        """
        debug("Starting cluster with 3 nodes.")
        self.init_cluster(num_nodes=3)
        node1, node2, node3 = self.cluster.nodelist()

        tokens = self.get_sorted_tokens(node3)

        debug(len(tokens))

        debug("Inserting Data...")
        node1.stress(['write', 'n=10000', '-schema', 'replication(factor=3)'])

        session = self.patient_cql_connection(node1)
        stress_table = 'keyspace1.standard1'
        query = SimpleStatement('select * from %s LIMIT 1' % stress_table, consistency_level=ConsistencyLevel.THREE)
        initialData = list(session.execute(query))

        # stop node, query should not work with consistency 3
        debug("Stopping node 3.")
        node3.stop(gently=gently, wait_other_notice=True)

        debug("Testing node stoppage (query should fail).")
        with self.assertRaises(NodeUnavailable):
            try:
                query = SimpleStatement('select * from %s LIMIT 1' % stress_table,
                                        consistency_level=ConsistencyLevel.THREE)
                session.execute(query)
            except (Unavailable, ReadTimeout, ReadFailure):
                raise NodeUnavailable("Node could not be queried.")

        # replace node 3 with node 4
        debug("Starting node 4 to replace node 3")

        node4 = new_node(self.cluster, bootstrap=True, token=None, remote_debug_port='0', data_center=None)
        node4.start(replace_address=self.cluster.get_node_ip(3), wait_for_binary_proto=True)

        # query should work again
        debug("Verifying querying works again.")
        query = SimpleStatement('select * from %s LIMIT 1' % stress_table, consistency_level=ConsistencyLevel.THREE)
        finalData = list(session.execute(query))
        self.assertCountEqual(initialData, finalData)

        debug("Verifying tokens migrated successfully")
        moved_tokens_list = self.get_sorted_tokens(node4)
        debug(len(moved_tokens_list))
        self.assertEqual(moved_tokens_list, tokens)

        # check that restarting node 3 doesn't work
        # FIXME: https://github.com/scylladb/scylla/issues/5523 is fixed
        # need to verify that the node doesn't start listening
        debug("Try to restart node 3 (should fail)")
        node3.start(no_wait=True)
        checkCollision = node1.grep_log("between .*"+self.cluster.get_node_ip(3)+" and .*" +
                                        self.cluster.get_node_ip(4)+"; .*"+self.cluster.get_node_ip(4)+" is the new owner")
        debug(checkCollision)
        self.assertEqual(len(checkCollision), 1)

    def serve_writes_during_bootstrap_test(self):
        """
        When replacing a node, the new node should serve writes while data is streamed into it, ensuring that when
        the operation completes it will have up-to-date data.
        """
        debug("Starting cluster with 3 nodes.")
        self.init_cluster(num_nodes=3)
        node1, node2, node3 = self.cluster.nodelist()
        session = self.patient_cql_connection(node1)

        keyspace_name = 'ks'
        table_name = 'cf'

        self.create_ks(session, keyspace_name, rf=3)
        session.execute(f"USE {keyspace_name}")

        session.execute(f"CREATE TABLE {table_name} (pk int, ck int, v int, primary key (pk, ck))")

        debug("Insert 100000 rows.")
        insert_stmt = session.prepare(f"INSERT INTO {table_name} (pk, ck, v) VALUES (?, ?, ?)")
        data = []
        for i in range(300):
            for k in range(500):
                data.append([i, k, k])
                session.execute(insert_stmt, (i, k, k))

        debug("Flush cluster")
        self.cluster.flush()

        tokens = self.get_sorted_tokens(node3)

        assert_row_count(session, table_name, 150000)

        # stop node
        debug("Stopping node 3.")
        node3.stop(gently=True, wait_other_notice=True)

        # replace node 3 with node 4
        debug("Starting node 4 to replace node 3")
        node4 = new_node(self.cluster, bootstrap=True, token=None, remote_debug_port='0', data_center=None)
        node4.start(replace_address=self.cluster.get_node_ip(3), no_wait=True,
                    jvm_args=['--logger-log-level', 'stream_session=debug'])

        node4.watch_log_for("JOINING: Starting to bootstrap")
        node4.watch_log_for("Beginning stream session|sync data for keyspace=ks, status=started")

        debug("Insert 1000 rows more.")
        for i in range(300, 310):
            for k in range(500, 600):
                data.append([i, k, k])
                session.execute(insert_stmt, (i, k, k))

        mark_log = node4.mark_log()

        assert_row_count(session, table_name, 151000, consistency_level=ConsistencyLevel.QUORUM)

        debug("Waiting for node4 is up")
        node4.watch_log_for("initialization completed", from_mark=mark_log)

        debug("Verifying tokens migrated successfully")
        moved_tokens_list = self.get_sorted_tokens(node4)
        self.assertEqual(moved_tokens_list, tokens)

        # stop all nodes except new one
        debug("Stopping nodes 1 and 2")
        for node in [node1, node2]:
            node.stop(gently=True, wait_other_notice=True)

        # validate data
        node4.flush()
        session = self.patient_cql_connection(node4)
        session.execute(f"USE {keyspace_name}")
        assert_row_count(session, table_name, 151000)
        assert_all(session, f"select * from {table_name}", data, ignore_order=True)

    @require('#4325')
    def shutdown_all_and_replace_node_test(self):
        debug("Starting cluster with 3 nodes.")
        self.init_cluster(num_nodes=3)
        node1, node2, node3 = self.cluster.nodelist()

        self.cluster.stop_nodes([node1, node2, node3])
        self.cluster.start_nodes([node1, node2], wait_for_binary_proto=True)

        debug("Starting node 4 to replace node 3")
        node4 = new_node(self.cluster, bootstrap=True, token=None, remote_debug_port='0', data_center=None)

        node4.start(wait_for_binary_proto=True, replace_address=self.cluster.get_node_ip(3))

    @attr('next-gating')
    @attr('dtest-debug')
    def replace_active_node_test(self):

        debug("Starting cluster with 3 nodes.")
        self.init_cluster(num_nodes=3)
        node1, node2, node3 = self.cluster.nodelist()

        # replace active node 3 with node 4
        debug("Starting node 4 to replace active node 3")
        node4 = new_node(self.cluster, bootstrap=True, token=None, remote_debug_port='0', data_center=None)

        expected_message = "Cannot replace a live node"
        self.ignore_log_patterns += [expected_message]

        mark = node4.mark_log()
        node4.start(replace_address=self.cluster.get_node_ip(3), no_wait=True)
        node4.watch_log_for(expected_message, from_mark=mark)
        self.check_not_running(node4)

    def replace_nonexistent_node_test(self):
        debug("Starting cluster with 3 nodes.")
        self.init_cluster(num_nodes=3)
        node1, node2, node3 = self.cluster.nodelist()

        debug('Start node 4 and replace an address with no node')
        node4 = new_node(self.cluster, bootstrap=True, token=None, remote_debug_port='0', data_center=None)

        expected_message = "Cannot replace_address .*"+self.cluster.get_node_ip(5)+" because it doesn't exist in gossip"
        self.ignore_log_patterns += [expected_message]

        # try to replace an unassigned ip address
        mark = node4.mark_log()
        try:
            node4.start(replace_address=self.cluster.get_node_ip(5), no_wait=True)
        except NodeError:
            pass  # node doesn't start as expected
        node4.watch_log_for(expected_message, from_mark=mark)
        self.check_not_running(node4)

    def check_not_running(self, node):
        attempts = 0
        while node.is_running() and attempts < 10:
            sleep(1)
            attempts = attempts + 1

        self.assertFalse(node.is_running())

    def replace_first_boot_test(self):
        debug("Starting cluster with 3 nodes.")
        self.init_cluster(num_nodes=3, configuration_options={'range_request_timeout_in_ms': 10000})
        node1, node2, node3 = self.cluster.nodelist()

        tokens = self.get_sorted_tokens(node3)

        debug(len(tokens))

        debug("Inserting Data...")
        node1.stress(['write', 'n=10000', '-schema', 'replication(factor=3)'])

        session = self.patient_cql_connection(node1)
        stress_table = 'keyspace1.standard1'
        query = SimpleStatement('select * from %s LIMIT 1' % stress_table, consistency_level=ConsistencyLevel.THREE)
        initialData = list(session.execute(query))

        # stop node, query should not work with consistency 3
        debug("Stopping node 3.")
        node3.stop(gently=False)

        debug("Testing node stoppage (query should fail).")
        with self.assertRaises(NodeUnavailable):
            try:
                session.execute(query, timeout=30)
            except (Unavailable, ReadTimeout, ReadFailure):
                raise NodeUnavailable("Node could not be queried.")

        # replace node 3 with node 4
        debug("Starting node 4 to replace node 3")
        node4 = new_node(self.cluster, bootstrap=True, token=None, remote_debug_port='0', data_center=None)
        node4.start(jvm_args=["-Dcassandra.replace_address_first_boot=" +
                              self.cluster.get_node_ip(3)], wait_for_binary_proto=True)

        # query should work again
        debug("Verifying querying works again.")
        finalData = list(session.execute(query))
        self.assertCountEqual(initialData, finalData)

        debug("Verifying tokens migrated successfully")
        moved_tokens_list = self.get_sorted_tokens(node4)
        debug(len(moved_tokens_list))
        self.assertEqual(moved_tokens_list, tokens)

        checkCollision = node1.grep_log("between .*"+self.cluster.get_node_ip(3)+" and .*" +
                                        self.cluster.get_node_ip(4)+"; .*"+self.cluster.get_node_ip(4)+" is the new owner")
        debug(checkCollision)
        self.assertEqual(len(checkCollision), 1)

        # FIXME: Do not restart the replaced node until
        # https://github.com/scylladb/scylla/issues/5523 is fixed
        # With #5523 fixed, we can verify that n3 could not start and rejoin
        # the cluster.
        # debug("Try to restart node 3 (should fail)")
        # node3.start(no_wait=True)

        # restart node4 (if error's might have to change tokens)
        node4.stop(gently=False)
        node4.start(wait_for_binary_proto=True, wait_other_notice=False)

        debug("Verifying querying works again.")
        finalData = list(session.execute(query))
        self.assertCountEqual(initialData, finalData)

        # we redo this check because restarting node should not result in tokens being moved again.
        # ie tokens should be same
        debug("Verifying tokens migrated successfully")
        moved_tokens_list = self.get_sorted_tokens(node4)
        debug(len(moved_tokens_list))
        self.assertEqual(moved_tokens_list, tokens)

    @since('2.2')
    @skip('test hangs: see CASSANDRA-9831')
    def resumable_replace_test(self):
        """Test resumable bootstrap while replacing node"""

        self.init_cluster(num_nodes=3)
        node1, node2, node3 = self.cluster.nodelist()

        node1.stress(['write', 'n=100000', '-schema', 'replication(factor=3)'])

        session = self.patient_cql_connection(node1)
        stress_table = 'keyspace1.standard1'
        query = SimpleStatement('select * from %s LIMIT 1' % stress_table, consistency_level=ConsistencyLevel.THREE)
        initialData = list(session.execute(query))

        node3.stop(gently=False)

        # kill node1 in the middle of streaming to let it fail
        t = InterruptBootstrap(node1)
        t.start()
        # replace node 3 with node 4
        debug("Starting node 4 to replace node 3")
        node4 = new_node(self.cluster, bootstrap=True, token=None, remote_debug_port='0', data_center=None)
        try:
            node4.start(jvm_args=["-Dcassandra.replace_address_first_boot="+self.cluster.get_node_ip(3)])
        except NodeError:
            pass  # node doesn't start as expected
        t.join()

        # bring back node1 and invoke nodetool bootstrap to resume bootstrapping
        node1.start()
        node4.nodetool('bootstrap resume')
        # check if we skipped already retrieved ranges
        node4.watch_log_for("already available. Skipping streaming.")
        # wait for node3 ready to query
        node4.watch_log_for("Listening for thrift clients...")

        # check if 2nd bootstrap succeeded
        session = self.exclusive_cql_connection(node4)
        rows = list(session.execute("SELECT bootstrapped FROM system.local WHERE key='local'"))
        assert len(rows) == 1
        assert rows[0][0] == 'COMPLETED', rows[0][0]

        # query should work again
        debug("Verifying querying works again.")
        finalData = list(session.execute(query))
        self.assertCountEqual(initialData, finalData)

    @since('2.2')
    @skip('Scylla does not support the cassandra.reset_bootstrap_progress option.')
    def replace_with_reset_resume_state_test(self):
        """Test replace with resetting bootstrap progress"""

        self.init_cluster(num_nodes=3)
        node1, node2, node3 = self.cluster.nodelist()

        node1.stress(['write', 'n=100000', '-schema', 'replication(factor=3)'])

        session = self.patient_cql_connection(node1)
        stress_table = 'keyspace1.standard1'
        query = SimpleStatement('select * from %s LIMIT 1' % stress_table, consistency_level=ConsistencyLevel.THREE)
        initialData = list(session.execute(query))

        node3.stop(gently=False)

        # kill node1 in the middle of streaming to let it fail
        t = InterruptBootstrap(node1)
        t.start()
        # replace node 3 with node 4
        debug("Starting node 4 to replace node 3")
        node4 = new_node(self.cluster, bootstrap=True, token=None, remote_debug_port='0', data_center=None)
        try:
            node4.start(jvm_args=["-Dcassandra.replace_address_first_boot="+self.cluster.get_node_ip(3)])
        except NodeError:
            pass  # node doesn't start as expected
        t.join()
        node1.start()

        # restart node4 bootstrap with resetting bootstrap state
        node4.stop()
        mark = node4.mark_log()
        node4.start(jvm_args=[
                    "-Dcassandra.replace_address_first_boot="+self.cluster.get_node_ip(3),
                    "-Dcassandra.reset_bootstrap_progress=true"
                    ])
        # check if we reset bootstrap state
        node4.watch_log_for("Resetting bootstrap progress to start fresh", from_mark=mark)
        # wait for node3 ready to query
        node4.watch_log_for("Listening for thrift clients...", from_mark=mark)

        # check if 2nd bootstrap succeeded
        session = self.exclusive_cql_connection(node4)
        rows = list(session.execute("SELECT bootstrapped FROM system.local WHERE key='local'"))
        assert len(rows) == 1
        assert rows[0][0] == 'COMPLETED', rows[0][0]

        # query should work again
        debug("Verifying querying works again.")
        finalData = list(session.execute(query))
        self.assertCountEqual(initialData, finalData)

    def replace_node_no_hibernate_state_test(self):
        """Test that there is no HIBERNATE status for a replacing node.

        See https://github.com/scylladb/scylla/issues/5449 for details.
        """
        debug("Starting cluster with 2 nodes.")
        self.init_cluster(2)
        node1, node2 = self.cluster.nodelist()
        debug(f"Node 1 address is {self.cluster.get_node_ip(1)}")

        node2_address = self.cluster.get_node_ip(2)
        debug(f"Node 2 address is {node2_address}")

        tokens = self.get_sorted_tokens(node2)
        debug(f"Detected number of tokens: {len(tokens)}")

        debug("Inserting Data...")
        node1.stress(["write", "n=10000", "-schema", "replication(factor=2)"])

        session = self.patient_cql_connection(node1)
        stress_table = "keyspace1.standard1"
        query = SimpleStatement(f"SELECT * FROM {stress_table} LIMIT 1", consistency_level=ConsistencyLevel.TWO)
        initial_data = list(session.execute(query))

        debug("Stopping node 2.")
        node2.stop()

        debug("Starting node 3 to replace node 2, but stop it in the middle of the replace.")
        node3 = new_node(self.cluster, bootstrap=True, token=None, remote_debug_port="0", data_center=None)
        node3.start(replace_address=node2_address, no_wait=True)

        node3_address = self.cluster.get_node_ip(3)
        debug(f"Node 3 address is {node3_address}")

        self.ignore_log_patterns += ['Startup failed']
        node3.stop()

        status1, err1 = node1.nodetool("gossipinfo")
        debug(f"gossipinfo:\n{status1}")
        self.assertNotIn("STATUS:hibernate,true", status1, "There is a node in HIBERNATE status.")

        debug("Starting node 4 to replace node 2.")
        node4 = new_node(self.cluster, bootstrap=True, token=None, remote_debug_port='0', data_center=None)
        node4.start(replace_address=node2_address, wait_for_binary_proto=True, wait_other_notice=True)

        node4_address = self.cluster.get_node_ip(4)
        debug(f"Node 4 address is {node4_address}")

        status2, err2 = node1.nodetool("gossipinfo")
        debug(f"gossipinfo:\n{status2}")
        self.assertNotIn("STATUS:hibernate,true", status2, "There is a node in HIBERNATE status.")
        self.assertNotIn(f"/{node3_address}\n", status2, "Node 3 stays in gossip.")

        debug("Verifying querying works.")
        final_data = list(session.execute(query))
        self.assertCountEqual(initial_data, final_data)

        debug("Verifying tokens migrated successfully.")
        moved_tokens_list = self.get_sorted_tokens(node4)
        debug(len(moved_tokens_list))
        self.assertEqual(moved_tokens_list, tokens)

        debug("Verifying system.peers table.")
        peers = rows_to_list(session.execute("SELECT * FROM system.peers"))
        self.assertEqual(len(peers), 1, "There are more peers than expected.")

    def replace_with_background_workload_test(self):
        """
        The subtest is used to reproduce https://github.com/scylladb/scylla/issues/4705
        the background write workload continue running more than 30 seconds,
        the gossiper reached a timeout, and nodes raise 'unknown endpoint' error.
        """
        self.init_cluster(num_nodes=3)

        node1 = self.cluster.nodelist()[0]
        debug(node1.nodetool('status')[0])

        enable_nodetool_debug = False

        def nodetool_thread():
            debug('nodetool thread')
            for key in range(20):
                debug('enable_nodetool_debug: {}'.format(n))
                debug(node1.nodetool('status')[0])
                debug(node1.nodetool("gossipinfo", True)[0])
            debug('nodetool thread: completed')

        def workload_thread():
            debug('workload thread: start')
            # The added scylla 3.1 node can be up quicker than latest master, 140s workload is enough for scylla 3.1
            node1.stress(['write', 'duration=320s', 'no-warmup', 'cl=QUORUM', '-rate',
                          'threads=1', '-schema', 'replication(factor=3)', '-pop', 'seq=1..1000'])
            debug('workload thread: completed')
            expect_msg = 'Expect workload continue running more than 30 seconds after new node is added'
            assert self.replace_done_time, expect_msg
            rest_time = (datetime.datetime.now() - self.replace_done_time).total_seconds()
            debug('Workload still executes {} seconds after new node is added. {}'.format(rest_time, expect_msg))
            assert rest_time > 30, expect_msg

        cs_thread = threading.Thread(target=workload_thread)
        cs_thread.start()

        if enable_nodetool_debug:
            nodetool_thread = threading.Thread(target=nodetool_thread)
            nodetool_thread.start()

        debug('Sleep 5 seconds to wait the workload starts')
        sleep(5)

        debug('Start to kill node3 ...')
        node3 = self.cluster.nodelist()[2]
        node3.stop(gently=False)
        debug('node3 has been killed')

        debug('Add a new node to replace the dead node')
        self.replace_done_time = None
        added_node = new_node(self.cluster, data_center='dc1')
        added_node.start(replace_address=self.cluster.get_node_ip(3), wait_for_binary_proto=True)
        debug('Successfully add a new node to replace node3')
        self.replace_done_time = datetime.datetime.now()

        cs_thread.join(timeout=300)
        if enable_nodetool_debug:
            nodetool_thread.join(timeout=300)

        for node in self.cluster.nodelist():
            err_log = node.grep_log('unknown endpoint')[0:3]
            debug('{}: {}'.format(node.name, err_log))
            self.assertEqual(0, len(err_log))

    def replace_stopped_node_with_schema_rf_1_test(self):
        """
        Test that we can replace a node that is not shutdown gracefully
        and schema have replication factor equal 1

        """
        self.replace_node_with_schema_rf_1(gently=False)

    def replace_shutdown_node_with_schema_rf_1_test(self):
        """
        Test that we can replace a node that is shutdown gracefully
        and schema have replication factor equal 1
        """
        self.replace_node_with_schema_rf_1(gently=True)

    def replace_node_with_schema_rf_1(self, gently):
        self.init_cluster(num_nodes=3)
        node1, node2, node3 = self.cluster.nodelist()

        node3_tokens = self.get_sorted_tokens(node3)

        debug("Inserting Data...")
        node1.stress(['write', 'n=10000', '-schema', 'replication(factor=1)'])

        debug("Stopping node 3.")
        node3.stop(gently=gently, wait_other_notice=True)

        # replace node 3 with node 4
        debug("Starting node 4 to replace node 3")
        node4 = new_node(self.cluster, bootstrap=True)
        node4.start(replace_address=self.cluster.get_node_ip(3), wait_for_binary_proto=True)
        if not self.rbo_enabled:
            node4.watch_log_for(
                [r"WARN .* Unable to find sufficient sources to stream range .* for keyspace .* with RF = 1 for replace operation"])
        else:
            debug("Issue #6351 is not related to scylla with repair based operations enabled")

        debug("Verifying tokens migrated successfully")
        moved_tokens_list = self.get_sorted_tokens(node4)

        self.assertEqual(moved_tokens_list, node3_tokens, "Tokens were not moved correctly to node4")
        self.assertTrue(node4.is_live(), "Node4 is not alive after node4 has replaced node3")

    def replace_node_diff_ip_test(self):
        debug("Starting cluster with 5 nodes.")
        cluster = self.cluster
        cluster.populate(5).start(wait_for_binary_proto=True)
        node1, node2, node3, node4, node5 = cluster.nodelist()

        session = self.patient_cql_connection(node5)
        self.create_ks(session, 'ks', 3)
        self.create_cf(session, 'cf', read_repair=0.0, columns={'c1': 'text', 'c2': 'text'})
        insert_c1c2(session, keys=range(1000), consistency=ConsistencyLevel.ALL)

        ip5 = node5.address()
        node5.stop()

        debug("Starting node 6 to replace node 5")
        node6 = new_node(cluster, bootstrap=True, token=None, remote_debug_port='0', data_center=None)
        node6.start(wait_for_binary_proto=True, replace_address=ip5)
        for node in [node1, node2, node3, node4, node6]:
            node.watch_log_for(f"FatClient {ip5} has been silent for .*ms, removing from gossip")

    def replace_node_same_ip_test(self):
        debug("Starting cluster with 5 nodes.")
        cluster = self.cluster
        cluster.populate(5).start(wait_for_binary_proto=True)
        node1, node2, node3, node4, node5 = cluster.nodelist()

        session = self.patient_cql_connection(node5)
        self.create_ks(session, 'ks', 3)
        self.create_cf(session, 'cf', read_repair=0.0, columns={'c1': 'text', 'c2': 'text'})
        insert_c1c2(session, keys=range(1000), consistency=ConsistencyLevel.ALL)

        ip5 = node5.address()
        node5.stop()

        debug("Starting node 5 to replace node 5")
        node5.clear()
        jvm_args = ['--auto-bootstrap', 'true', '--seed-provider-parameters', 'seeds={}'.format(node1.address())]
        node5.start(wait_for_binary_proto=True, replace_address=ip5, jvm_args=jvm_args)

    @attr('dtest-heavy')
    def replace_node_diff_ip_take_write_test(self):
        debug("Starting cluster with 5 nodes.")
        cluster = self.cluster
        cluster.populate(5).start(wait_for_binary_proto=True)
        node1, node2, node3, node4, node5 = cluster.nodelist()

        session = self.patient_cql_connection(node5)
        self.create_ks(session, 'ks', 3)
        self.create_cf(session, 'cf', read_repair=0.0, columns={'c1': 'text', 'c2': 'text'})
        insert_c1c2(session, keys=range(1000), consistency=ConsistencyLevel.ALL)

        ip5 = node5.address()
        node5.stop()

        rounds_cnt = 100 if not hasattr(cluster, 'scylla_mode') or cluster.scylla_mode != 'debug' else 10
        keys_per_round = 1000
        writes_diff_cnt = rounds_cnt * keys_per_round / 100

        stop = threading.Event()

        def insert_data(session, rounds, keys_per_round, stop):
            session.execute("use ks;")
            debug("Started to write rounds={}".format(rounds))
            for i in range(rounds):
                if stop.is_set():
                    debug("Write thread is stopped")
                    break
                start = i * keys_per_round
                end = start + keys_per_round
                if (start % 100000 == 0):
                    debug("Writing keys start={} , end={}".format(start, end))
                insert_c1c2(session, range(start, end), consistency=ConsistencyLevel.QUORUM)
            debug("Finished to write rounds={}".format(rounds))
            return rounds

        debug("Starting node 6 to replace node 5")
        node6 = new_node(cluster, bootstrap=True, token=None, remote_debug_port='0', data_center=None)
        node6.start(wait_for_binary_proto=False, replace_address=ip5)

        with_replacing_take_write_patch = True
        if with_replacing_take_write_patch:
            node6.watch_log_for("Wait until peer nodes know the bootstrap tokens of local node")
        else:
            node6.watch_log_for("Starting up server gossip")

        executor = ThreadPoolExecutor(max_workers=1)
        session = self.patient_cql_connection(node1)
        write_thread = executor.submit(insert_data, session, rounds_cnt, keys_per_round, stop)

        debug("Get metrics when other knows replacing node = HIBERNATE")
        metrics = ['scylla_database_total_writes', 'scylla_database_total_reads']
        writes_when_replace_ops_started = 0
        for node in [1, 2, 3, 4, 6]:
            node_metrics = self.get_node_metrics(node_ip=self.cluster.get_node_ip(node), metrics=metrics)
            debug("scylla_database_total_writes: node{}={}".format(node, node_metrics))
            if node == 6:
                writes_when_replace_ops_started = node_metrics['scylla_database_total_writes']
                debug(f"writes_when_replace_ops_started={writes_when_replace_ops_started}")

        node6.watch_log_for("Bootstrap completed!")

        debug("Get metrics when other knows replacing node = NORMAL")
        metrics = ['scylla_database_total_writes', 'scylla_database_total_reads']
        writes_when_replace_ops_done = 0
        for node in [1, 2, 3, 4, 6]:
            node_metrics = self.get_node_metrics(node_ip=self.cluster.get_node_ip(node), metrics=metrics)
            debug("scylla_database_total_writes: node{}={}".format(node, node_metrics))
            if node == 6:
                writes_when_replace_ops_done = node_metrics['scylla_database_total_writes']
                debug(f"writes_when_replace_ops_done={writes_when_replace_ops_done}")

        assert writes_when_replace_ops_done - writes_when_replace_ops_started > writes_diff_cnt

        stop.set()

        # Wait for node6 to finish the replace ops
        session = self.patient_cql_connection(node6)

        for node in [node1, node2, node3, node4, node6]:
            node.watch_log_for(f"FatClient {ip5} has been silent for .*ms, removing from gossip")

        write_thread.result()

    @attr('dtest-heavy')
    def replace_node_same_ip_take_write_test(self):
        debug("Starting cluster with 5 nodes.")
        cluster = self.cluster
        cluster.populate(5).start(wait_for_binary_proto=True)
        node1, node2, node3, node4, node5 = cluster.nodelist()

        session = self.patient_cql_connection(node5)
        self.create_ks(session, 'ks', 3)
        self.create_cf(session, 'cf', read_repair=0.0, columns={'c1': 'text', 'c2': 'text'})
        insert_c1c2(session, keys=range(1000), consistency=ConsistencyLevel.ALL)

        ip5 = node5.address()
        node5.stop()
        mark = node5.mark_log()

        rounds_cnt = 100 if not hasattr(cluster, 'scylla_mode') or cluster.scylla_mode != 'debug' else 10
        keys_per_round = 1000
        writes_diff_cnt = rounds_cnt * keys_per_round / 100

        stop = threading.Event()

        def insert_data(session, rounds, keys_per_round, stop):
            session.execute("use ks;")
            debug("Started to write and read rounds={}".format(rounds))
            for i in range(1, rounds + 1):
                if stop.is_set():
                    debug("Write thread is stopped")
                    break
                start = i * keys_per_round
                end = start + keys_per_round
                if (start % 100000 == 0):
                    debug("Writing keys start={} , end={}".format(start, end))
                insert_c1c2(session, range(start, end), consistency=ConsistencyLevel.QUORUM)
            debug("Finished to write and read rounds={}".format(rounds))
            return rounds

        debug("Starting node 5 to replace node 5")
        node5.clear()
        jvm_args = ['--auto-bootstrap', 'true', '--seed-provider-parameters', 'seeds={}'.format(node1.address())]
        node5.start(wait_for_binary_proto=False, replace_address=ip5, jvm_args=jvm_args)

        with_replacing_take_write_patch = True
        if with_replacing_take_write_patch:
            node5.watch_log_for("Wait until peer nodes know the bootstrap tokens of local node", from_mark=mark)
        else:
            node5.watch_log_for("Starting up server gossip", from_mark=mark)

        executor = ThreadPoolExecutor(max_workers=1)
        session = self.patient_cql_connection(node1)
        write_thread = executor.submit(insert_data, session, rounds_cnt, keys_per_round, stop)

        metrics = ['scylla_database_total_writes', 'scylla_database_total_reads']
        debug("Get metrics when other knows replacing node = HIBERNATE")
        writes_when_replace_ops_started = 0
        for node in [1, 2, 3, 4, 5]:
            node_metrics = self.get_node_metrics(node_ip=self.cluster.get_node_ip(node), metrics=metrics)
            debug("metrics: node{}={}".format(node, node_metrics))
            if node == 5:
                writes_when_replace_ops_started = node_metrics['scylla_database_total_writes']
                debug(f"writes_when_replace_ops_started={writes_when_replace_ops_started}")

        node5.watch_log_for("Bootstrap completed!", from_mark=mark)
        debug("Get metrics when other knows replacing node = NORMAL")
        metrics = ['scylla_database_total_writes', 'scylla_database_total_reads']
        writes_when_replace_ops_done = 0
        for node in [1, 2, 3, 4, 5]:
            node_metrics = self.get_node_metrics(node_ip=self.cluster.get_node_ip(node), metrics=metrics)
            debug("metrics: node{}={}".format(node, node_metrics))
            if node == 5:
                writes_when_replace_ops_done = node_metrics['scylla_database_total_writes']
                debug(f"writes_when_replace_ops_done={writes_when_replace_ops_done}")

        assert writes_when_replace_ops_done - writes_when_replace_ops_started > writes_diff_cnt

        stop.set()
        # Wait for node5 to finish the replace ops
        session = self.patient_cql_connection(node5)

        write_thread.result()


for rbo_status in [True, False]:
    cls_name = "TestReplaceAddress_rbo_enabled" if rbo_status else "TestReplaceAddress_rbo_disabled"
    vars()[cls_name] = type(cls_name, (TestReplaceAddress,), {"rbo_enabled": rbo_status, "__test__": True})
