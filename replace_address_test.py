import datetime
import threading

from time import sleep
from unittest import skip
from nose.plugins.attrib import attr

from cassandra import ConsistencyLevel, ReadTimeout, Unavailable, ReadFailure
from cassandra.query import SimpleStatement

from ccmlib.node import NodeError
from dtest import DISABLE_VNODES, Tester, debug
from tools import InterruptBootstrap, since, new_node, require, rows_to_list


class NodeUnavailable(Exception):
    pass


@attr('dtest-full')
class TestReplaceAddress(Tester):

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

    def replace_stopped_node_test(self):
        """
        Test that we can replace a node that is not shutdown gracefully.
        """
        self._replace_node_test(gently=False)

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
        cluster = self.cluster
        cluster.populate(3).start()
        node1, node2, node3 = cluster.nodelist()

        if DISABLE_VNODES:
            numNodes = 1
        else:
            # a little hacky but grep_log returns the whole line...
            numNodes = int(node3.get_conf_option('num_tokens'))

        debug(numNodes)

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
                query = SimpleStatement('select * from %s LIMIT 1' % stress_table, consistency_level=ConsistencyLevel.THREE)
                session.execute(query)
            except (Unavailable, ReadTimeout, ReadFailure):
                raise NodeUnavailable("Node could not be queried.")

        # replace node 3 with node 4
        debug("Starting node 4 to replace node 3")

        node4 = new_node(cluster, bootstrap=True, token=None, remote_debug_port='0', data_center=None)
        node4.start(replace_address=self.cluster.get_node_ip(3), wait_for_binary_proto=True)

        # query should work again
        debug("Verifying querying works again.")
        query = SimpleStatement('select * from %s LIMIT 1' % stress_table, consistency_level=ConsistencyLevel.THREE)
        finalData = list(session.execute(query))
        self.assertCountEqual(initialData, finalData)

        debug("Verifying tokens migrated sucessfully")
        movedTokensList = node4.grep_log("Token .* changing ownership from .*"+self.cluster.get_node_ip(3)+" to .*"+self.cluster.get_node_ip(4))
        debug(movedTokensList[0])
        self.assertEqual(len(movedTokensList), numNodes)

        # check that restarting node 3 doesn't work
        # FIXME: https://github.com/scylladb/scylla/issues/5523 is fixed
        # need to verify that the node doesn't start listening
        debug("Try to restart node 3 (should fail)")
        node3.start(no_wait=True)
        checkCollision = node1.grep_log("between .*"+self.cluster.get_node_ip(3)+" and .*"+self.cluster.get_node_ip(4)+"; .*"+self.cluster.get_node_ip(4)+" is the new owner")
        debug(checkCollision)
        self.assertEqual(len(checkCollision), 1)

    @require('#4325')
    def shutdown_all_and_replace_node_test(self):
        debug("Starting cluster with 3 nodes.")
        cluster = self.cluster
        cluster.populate(3).start()
        node1, node2, node3 = cluster.nodelist()

        cluster.stop_nodes([node1, node2, node3])
        cluster.start_nodes([node1, node2], wait_for_binary_proto=True)

        debug("Starting node 4 to replace node 3")
        node4 = new_node(cluster, bootstrap=True, token=None, remote_debug_port='0', data_center=None)

        node4.start(wait_for_binary_proto=True, replace_address=self.cluster.get_node_ip(3))

    @attr('next-gating')
    @attr('dtest-debug')
    def replace_active_node_test(self):

        debug("Starting cluster with 3 nodes.")
        cluster = self.cluster
        cluster.populate(3).start()
        node1, node2, node3 = cluster.nodelist()

        # replace active node 3 with node 4
        debug("Starting node 4 to replace active node 3")
        node4 = new_node(cluster, bootstrap=True, token=None, remote_debug_port='0', data_center=None)

        expected_message = "Cannot replace a live node"
        self.ignore_log_patterns += [expected_message]

        mark = node4.mark_log()
        node4.start(replace_address=self.cluster.get_node_ip(3), no_wait=True)
        node4.watch_log_for(expected_message, from_mark=mark)
        self.check_not_running(node4)

    def replace_nonexistent_node_test(self):
        debug("Starting cluster with 3 nodes.")
        cluster = self.cluster
        cluster.populate(3).start()
        node1, node2, node3 = cluster.nodelist()

        debug('Start node 4 and replace an address with no node')
        node4 = new_node(cluster, bootstrap=True, token=None, remote_debug_port='0', data_center=None)

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
        cluster = self.cluster
        cluster.populate(3).start()
        node1, node2, node3 = cluster.nodelist()

        if DISABLE_VNODES:
            numNodes = 1
        else:
            # a little hacky but grep_log returns the whole line...
            numNodes = int(node3.get_conf_option('num_tokens'))

        debug(numNodes)

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
        node4 = new_node(cluster, bootstrap=True, token=None, remote_debug_port='0', data_center=None)
        node4.start(jvm_args=["-Dcassandra.replace_address_first_boot="+self.cluster.get_node_ip(3)], wait_for_binary_proto=True)

        # query should work again
        debug("Verifying querying works again.")
        finalData = list(session.execute(query))
        self.assertCountEqual(initialData, finalData)

        debug("Verifying tokens migrated sucessfully")
        movedTokensList = node4.grep_log("Token .* changing ownership from .*"+self.cluster.get_node_ip(3)+" to .*"+self.cluster.get_node_ip(4))
        debug(movedTokensList[0])
        self.assertEqual(len(movedTokensList), numNodes)

        checkCollision = node1.grep_log("between .*"+self.cluster.get_node_ip(3)+" and .*"+self.cluster.get_node_ip(4)+"; .*"+self.cluster.get_node_ip(4)+" is the new owner")
        debug(checkCollision)
        self.assertEqual(len(checkCollision), 1)

        # FIXME: Do not restart the replaced node until
        # https://github.com/scylladb/scylla/issues/5523 is fixed
        # With #5523 fixed, we can verify that n3 could not start and rejoin
        # the cluster.
        # debug("Try to restart node 3 (should fail)")
        # node3.start(no_wait=True)

        # restart node4 (if error's might have to change num_tokens)
        node4.stop(gently=False)
        node4.start(wait_for_binary_proto=True, wait_other_notice=False)

        debug("Verifying querying works again.")
        finalData = list(session.execute(query))
        self.assertCountEqual(initialData, finalData)

        # we redo this check because restarting node should not result in tokens being moved again, ie number should be same
        debug("Verifying tokens migrated sucessfully")
        movedTokensList = node4.grep_log("Token .* changing ownership from .*"+self.cluster.get_node_ip(3)+" to .*"+self.cluster.get_node_ip(4))
        debug(movedTokensList[0])
        self.assertEqual(len(movedTokensList), numNodes)

    @since('2.2')
    @skip('test hangs: see CASSANDRA-9831')
    def resumable_replace_test(self):
        """Test resumable bootstrap while replacing node"""

        cluster = self.cluster
        cluster.populate(3).start()
        node1, node2, node3 = cluster.nodelist()

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
        node4 = new_node(cluster, bootstrap=True, token=None, remote_debug_port='0', data_center=None)
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

        cluster = self.cluster
        cluster.populate(3).start()
        node1, node2, node3 = cluster.nodelist()

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
        node4 = new_node(cluster, bootstrap=True, token=None, remote_debug_port='0', data_center=None)
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
        cluster = self.cluster
        cluster.populate(2).start()
        node1, node2 = cluster.nodelist()
        debug(f"Node 1 address is {cluster.get_node_ip(1)}")

        node2_address = cluster.get_node_ip(2)
        debug(f"Node 2 address is {node2_address}")

        if DISABLE_VNODES:
            num_tokens = 1
        else:
            # A little hacky but grep_log returns the whole line.
            num_tokens = int(node2.get_conf_option("num_tokens"))
        debug(f"Detected number of tokens: {num_tokens}")

        debug("Inserting Data...")
        node1.stress(["write", "n=10000", "-schema", "replication(factor=2)"])

        session = self.patient_cql_connection(node1)
        stress_table = "keyspace1.standard1"
        query = SimpleStatement(f"SELECT * FROM {stress_table} LIMIT 1", consistency_level=ConsistencyLevel.TWO)
        initial_data = list(session.execute(query))

        debug("Stopping node 2.")
        node2.stop()

        debug("Starting node 3 to replace node 2, but stop it in the middle of the replace.")
        node3 = new_node(cluster, bootstrap=True, token=None, remote_debug_port="0", data_center=None)
        node3.start(replace_address=node2_address, no_wait=True)

        node3_address = cluster.get_node_ip(3)
        debug(f"Node 3 address is {node3_address}")

        self.ignore_log_patterns += ['Startup failed']
        node3.stop()

        status1, err1 = node1.nodetool("gossipinfo")
        debug(f"gossipinfo:\n{status1}")
        self.assertNotIn("STATUS:hibernate,true", status1, "There is a node in HIBERNATE status.")

        debug("Starting node 4 to replace node 2.")
        node4 = new_node(cluster, bootstrap=True, token=None, remote_debug_port='0', data_center=None)
        node4.start(replace_address=node2_address, wait_for_binary_proto=True, wait_other_notice=True)

        node4_address = cluster.get_node_ip(4)
        debug(f"Node 4 address is {node4_address}")

        status2, err2 = node1.nodetool("gossipinfo")
        debug(f"gossipinfo:\n{status2}")
        self.assertNotIn("STATUS:hibernate,true", status2, "There is a node in HIBERNATE status.")
        self.assertNotIn(f"/{node3_address}\n", status2, "Node 3 stays in gossip.")

        debug("Verifying querying works.")
        final_data = list(session.execute(query))
        self.assertCountEqual(initial_data, final_data)

        debug("Verifying tokens migrated sucessfully.")
        moved_tokens_list = node4.grep_log(f"Token .* changing ownership from .*{node2_address} to .*{node4_address}")
        debug(moved_tokens_list[0])
        self.assertEqual(len(moved_tokens_list), num_tokens)

        debug("Verifying logs for connection refuse messages.")
        connection_refuse_message = f"rpc - client {node3_address}:7000: fail to connect: Connection refused"
        self.assertEqual(node1.grep_log(connection_refuse_message), [])
        self.assertEqual(node4.grep_log(connection_refuse_message), [])

        debug("Verifying system.peers table.")
        peers = rows_to_list(session.execute("SELECT * FROM system.peers"))
        self.assertEqual(len(peers), 1, "There are more peers than expected.")

    def replace_with_background_workload_test(self):
        """
        The subtest is used to reproduce https://github.com/scylladb/scylla/issues/4705
        the background write workload continue running more than 30 seconds,
        the gossiper reached a timeout, and nodes raise 'unknown endpoint' error.
        """
        cluster = self.cluster
        cluster.populate(3).start(no_wait=False, wait_for_binary_proto=True, wait_other_notice=True)

        node1 = cluster.nodelist()[0]
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
            node1.stress(['write', 'duration=320s', 'no-warmup', 'cl=QUORUM', '-rate', 'threads=1', '-schema', 'replication(factor=3)', '-pop', 'seq=1..1000'])
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
        node3 = cluster.nodelist()[2]
        node3.stop(gently=False)
        debug('node3 has been killed')

        debug('Add a new node to replace the dead node')
        self.replace_done_time = None
        added_node = new_node(cluster, data_center='dc1')
        added_node.start(replace_address=self.cluster.get_node_ip(3), wait_for_binary_proto=True)
        debug('Successfully add a new node to replace node3')
        self.replace_done_time = datetime.datetime.now()

        cs_thread.join(timeout=300)
        if enable_nodetool_debug:
            nodetool_thread.join(timeout=300)

        for node in cluster.nodelist():
            err_log = node.grep_log('unknown endpoint')[0:3]
            debug('{}: {}'.format(node.name, err_log))
            self.assertEqual(0, len(err_log))
