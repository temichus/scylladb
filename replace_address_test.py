import datetime
import threading
import logging
from time import sleep
from concurrent.futures import ThreadPoolExecutor

import pytest
from cassandra import ConsistencyLevel, ReadTimeout, Unavailable, ReadFailure, OperationTimedOut
from cassandra.query import SimpleStatement
from ccmlib.node import NodeError
from ccmlib.scylla_cluster import ScyllaCluster

from dtest_class import Tester, create_ks, create_cf
from dtest_setup_overrides import DTestSetupOverrides
from dtest_setup import DTestSetup
from tools.assertions import assert_row_count, assert_all, assert_lists_equal_ignoring_order
from tools.data import rows_to_list, insert_c1c2
from tools.intervention import InterruptBootstrap
from tools.misc import ImmutableMapping
from tools.metrics import get_node_metrics


class NodeUnavailable(Exception):
    pass


logger = logging.getLogger(__name__)


@pytest.mark.dtest_full
@pytest.mark.parametrize("rbo_status", [True, False], ids=["rbo_enabled", "rbo_disabled"])
class TestReplaceAddress(Tester):
    rbo_enabled: bool

    @pytest.fixture(scope='function', autouse=True)
    def fixture_dtest_setup_overrides(self, dtest_config, rbo_status):
        dtest_setup_overrides = DTestSetupOverrides()
        dtest_setup_overrides.cluster_options = ImmutableMapping({'start_rpc': 'true'})
        self.rbo_enabled = rbo_status
        return dtest_setup_overrides

    @pytest.fixture(autouse=True)
    def fixture_add_additional_log_patterns(self, fixture_dtest_setup: DTestSetup):
        fixture_dtest_setup.ignore_log_patterns += [
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

    def init_cluster(self, num_nodes=3, configuration_options=None):
        configuration_options = configuration_options or {}
        rbo_status = "true" if self.rbo_enabled else "false"
        configuration_options.update({"enable_repair_based_node_ops": rbo_status})
        logger.debug(f"Setting cluster configuration options: {configuration_options}")
        self.debug_mode = isinstance(self.cluster, ScyllaCluster) and self.cluster.scylla_mode == 'debug'
        self.cluster.populate(num_nodes)
        self.cluster.set_configuration_options(values=configuration_options)
        self.cluster.start(no_wait=False, wait_for_binary_proto=True, wait_other_notice=True)

    def test_replace_stopped_node(self):
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

    def test_replace_shutdown_node(self):
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
        logger.info("Starting cluster with 3 nodes.")
        self.init_cluster(num_nodes=3)
        node1, node2, node3 = self.cluster.nodelist()

        tokens = self.get_sorted_tokens(node3)

        logger.info(len(tokens))

        logger.info("Inserting Data...")
        node1.stress(['write', 'n=10000', '-schema', 'replication(factor=3)'])

        session = self.patient_cql_connection(node1)
        stress_table = 'keyspace1.standard1'
        query = SimpleStatement('select * from %s LIMIT 1' % stress_table, consistency_level=ConsistencyLevel.THREE)
        initialData = list(session.execute(query))

        # stop node, query should not work with consistency 3
        logger.info("Stopping node 3.")
        node3.stop(gently=gently, wait_other_notice=True)

        logger.info("Testing node stoppage (query should fail).")
        with pytest.raises(expected_exception=(Unavailable, ReadTimeout, ReadFailure, OperationTimedOut)):
            query = SimpleStatement('select * from %s LIMIT 1' % stress_table,
                                    consistency_level=ConsistencyLevel.THREE)
            session.execute(query)

        # replace node 3 with node 4
        logger.info("Starting node 4 to replace node 3")

        node4 = self.cluster.new_node(4, auto_bootstrap=True, is_seed=False)
        node4.start(replace_address=self.cluster.get_node_ip(3), wait_for_binary_proto=True)

        # query should work again
        logger.info("Verifying querying works again.")
        query = SimpleStatement('select * from %s LIMIT 1' % stress_table, consistency_level=ConsistencyLevel.THREE)
        finalData = list(session.execute(query))
        assert_lists_equal_ignoring_order(initialData, finalData)

        logger.info("Verifying tokens migrated successfully")
        moved_tokens_list = self.get_sorted_tokens(node4)
        logger.info(len(moved_tokens_list))
        assert moved_tokens_list == tokens

        # check that restarting node 3 doesn't work
        # FIXME: https://github.com/scylladb/scylla/issues/5523 is fixed
        # need to verify that the node doesn't start listening
        logger.info("Try to restart node 3 (should fail)")
        node3.start(no_wait=True)
        checkCollision = node1.grep_log("between .*"+self.cluster.get_node_ip(3)+" and .*" +
                                        self.cluster.get_node_ip(4)+"; .*"+self.cluster.get_node_ip(4)+" is the new owner")
        logger.info(checkCollision)
        assert len(checkCollision) == 1

    def test_serve_writes_during_bootstrap(self):
        """
        When replacing a node, the new node should serve writes while data is streamed into it, ensuring that when
        the operation completes it will have up-to-date data.
        """
        logger.info("Starting cluster with 3 nodes.")
        self.init_cluster(num_nodes=3)
        node1, node2, node3 = self.cluster.nodelist()
        session = self.patient_cql_connection(node1)

        keyspace_name = 'ks'
        table_name = 'cf'

        create_ks(session, keyspace_name, rf=3)
        session.execute(f"USE {keyspace_name}")

        session.execute(f"CREATE TABLE {table_name} (pk int, ck int, v int, primary key (pk, ck))")

        keys = 30 if self.debug_mode else 300
        rows = 500
        total_rows = keys * rows
        logger.debug(f"Insert {keys} partitions of {rows} rows (total {total_rows} rows).")
        insert_stmt = session.prepare(f"INSERT INTO {table_name} (pk, ck, v) VALUES (?, ?, ?)")
        data = []
        for i in range(keys):
            for k in range(rows):
                data.append([i, k, k])
                session.execute(insert_stmt, (i, k, k))

        logger.info("Flush cluster")
        self.cluster.flush()

        tokens = self.get_sorted_tokens(node3)

        assert_row_count(session, table_name, total_rows)

        # stop node
        logger.info("Stopping node 3.")
        node3.stop(gently=True, wait_other_notice=True)

        # replace node 3 with node 4
        logger.info("Starting node 4 to replace node 3")
        node4 = self.cluster.new_node(4, auto_bootstrap=True, is_seed=False)
        node4.start(replace_address=self.cluster.get_node_ip(3), no_wait=True,
                    jvm_args=['--logger-log-level', 'stream_session=debug'])

        node4.watch_log_for("Starting to bootstrap")
        node4.watch_log_for("Beginning stream session|sync data for keyspace=ks, status=started")

        logger.debug("Insert 1000 rows more.")
        for i in range(keys, keys + 10):
            for k in range(rows, rows + 100):
                data.append([i, k, k])
                session.execute(insert_stmt, (i, k, k))

        mark_log = node4.mark_log()

        assert_row_count(session, table_name, total_rows + 1000, consistency_level=ConsistencyLevel.QUORUM)

        logger.info("Waiting for node4 is up")
        node4.watch_log_for("initialization completed", from_mark=mark_log)

        logger.info("Verifying tokens migrated successfully")
        moved_tokens_list = self.get_sorted_tokens(node4)
        assert moved_tokens_list == tokens

        # stop all nodes except new one
        logger.info("Stopping nodes 1 and 2")
        for node in [node1, node2]:
            node.stop(gently=True, wait_other_notice=True)

        # validate data
        node4.flush()
        session = self.patient_cql_connection(node4)
        session.execute(f"USE {keyspace_name}")
        assert_row_count(session, table_name, total_rows + 1000)
        assert_all(session, f"select * from {table_name}", data, ignore_order=True)

    def test_shutdown_all_and_replace_node(self):
        logger.info("Starting cluster with 3 nodes.")
        self.init_cluster(num_nodes=3)
        node1, node2, node3 = self.cluster.nodelist()

        self.cluster.stop_nodes([node1, node2, node3])
        self.cluster.start_nodes([node1, node2], wait_for_binary_proto=True)

        logger.info("Starting node 4 to replace node 3")
        node4 = self.cluster.new_node(4, auto_bootstrap=True, is_seed=False)

        node4.start(wait_for_binary_proto=True, replace_address=self.cluster.get_node_ip(3))

    @pytest.mark.next_gating
    @pytest.mark.dtest_debug
    def test_replace_active_node(self):

        logger.info("Starting cluster with 3 nodes.")
        self.init_cluster(num_nodes=3)
        node1, node2, node3 = self.cluster.nodelist()

        # replace active node 3 with node 4
        logger.info("Starting node 4 to replace active node 3")
        node4 = self.cluster.new_node(4, auto_bootstrap=True, is_seed=False)

        expected_message = "Cannot replace a live node"
        self.ignore_log_patterns += [expected_message]

        mark = node4.mark_log()
        node4.start(replace_address=self.cluster.get_node_ip(3), no_wait=True)
        node4.watch_log_for(expected_message, from_mark=mark)
        self.check_not_running(node4)

    def test_replace_nonexistent_node(self):
        logger.info("Starting cluster with 3 nodes.")
        self.init_cluster(num_nodes=3)
        node1, node2, node3 = self.cluster.nodelist()

        logger.info('Start node 4 and replace an address with no node')
        node4 = self.cluster.new_node(4, auto_bootstrap=True, is_seed=False)

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

        assert not node.is_running()

    def test_replace_first_boot(self):
        logger.info("Starting cluster with 3 nodes.")
        self.init_cluster(num_nodes=3, configuration_options={'range_request_timeout_in_ms': 10000})
        node1, node2, node3 = self.cluster.nodelist()

        tokens = self.get_sorted_tokens(node3)

        logger.info(len(tokens))

        logger.info("Inserting Data...")
        node1.stress(['write', 'n=10000', '-schema', 'replication(factor=3)'])

        session = self.patient_cql_connection(node1)
        stress_table = 'keyspace1.standard1'
        query = SimpleStatement('select * from %s LIMIT 1' % stress_table, consistency_level=ConsistencyLevel.THREE)
        initialData = list(session.execute(query))

        # stop node, query should not work with consistency 3
        logger.info("Stopping node 3.")
        node3.stop(gently=False)

        logger.info("Testing node stoppage (query should fail).")
        with pytest.raises(expected_exception=(Unavailable, ReadTimeout, ReadFailure, OperationTimedOut)):
            session.execute(query, timeout=30)

        # replace node 3 with node 4
        logger.info("Starting node 4 to replace node 3")
        node4 = self.cluster.new_node(4, auto_bootstrap=True, is_seed=False)
        node4.start(jvm_args=["-Dcassandra.replace_address_first_boot=" +
                              self.cluster.get_node_ip(3)], wait_for_binary_proto=True)

        # query should work again
        logger.info("Verifying querying works again.")
        finalData = list(session.execute(query))
        assert_lists_equal_ignoring_order(initialData, finalData)

        logger.info("Verifying tokens migrated successfully")
        moved_tokens_list = self.get_sorted_tokens(node4)
        logger.info(len(moved_tokens_list))
        assert moved_tokens_list == tokens

        checkCollision = node1.grep_log("between .*"+self.cluster.get_node_ip(3)+" and .*" +
                                        self.cluster.get_node_ip(4)+"; .*"+self.cluster.get_node_ip(4)+" is the new owner")
        logger.info(checkCollision)
        assert len(checkCollision) == 1

        # FIXME: Do not restart the replaced node until
        # https://github.com/scylladb/scylla/issues/5523 is fixed
        # With #5523 fixed, we can verify that n3 could not start and rejoin
        # the cluster.
        # logger.info("Try to restart node 3 (should fail)")
        # node3.start(no_wait=True)

        # restart node4 (if error's might have to change tokens)
        node4.stop(gently=False)
        node4.start(wait_for_binary_proto=True, wait_other_notice=False)

        logger.info("Verifying querying works again.")
        finalData = list(session.execute(query))
        assert_lists_equal_ignoring_order(initialData, finalData)

        # we redo this check because restarting node should not result in tokens being moved again.
        # ie tokens should be same
        logger.info("Verifying tokens migrated successfully")
        moved_tokens_list = self.get_sorted_tokens(node4)
        logger.info(len(moved_tokens_list))
        assert moved_tokens_list == tokens

    @pytest.mark.skip('test hangs: see CASSANDRA-9831')
    def test_resumable_replace(self):
        """Test resumable bootstrap while replacing node"""

        self.init_cluster(num_nodes=3)
        node1, node2, node3 = self.cluster.nodelist()

        keys = 10000 if self.debug_mode else 100000
        node1.stress(['write', f'n={keys}', '-schema', 'replication(factor=3)'])

        session = self.patient_cql_connection(node1)
        stress_table = 'keyspace1.standard1'
        query = SimpleStatement('select * from %s LIMIT 1' % stress_table, consistency_level=ConsistencyLevel.THREE)
        initialData = list(session.execute(query))

        node3.stop(gently=False)

        # kill node1 in the middle of streaming to let it fail
        t = InterruptBootstrap(node1)
        t.start()
        # replace node 3 with node 4
        logger.info("Starting node 4 to replace node 3")
        node4 = self.cluster.new_node(4, auto_bootstrap=True, is_seed=False)
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
        logger.info("Verifying querying works again.")
        finalData = list(session.execute(query))
        assert_lists_equal_ignoring_order(initialData, finalData)

    @pytest.mark.skip('Scylla does not support the cassandra.reset_bootstrap_progress option.')
    def test_replace_with_reset_resume_state(self):
        """Test replace with resetting bootstrap progress"""

        self.init_cluster(num_nodes=3)
        node1, node2, node3 = self.cluster.nodelist()

        keys = 10000 if self.debug_mode else 100000
        node1.stress(['write', f'n={keys}', '-schema', 'replication(factor=3)'])

        session = self.patient_cql_connection(node1)
        stress_table = 'keyspace1.standard1'
        query = SimpleStatement('select * from %s LIMIT 1' % stress_table, consistency_level=ConsistencyLevel.THREE)
        initialData = list(session.execute(query))

        node3.stop(gently=False)

        # kill node1 in the middle of streaming to let it fail
        t = InterruptBootstrap(node1)
        t.start()
        # replace node 3 with node 4
        logger.info("Starting node 4 to replace node 3")
        node4 = self.cluster.new_node(4, auto_bootstrap=True, is_seed=False)
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
        logger.info("Verifying querying works again.")
        finalData = list(session.execute(query))
        assert_lists_equal_ignoring_order(initialData, finalData)

    def test_replace_node_no_hibernate_state(self):
        """Test that there is no HIBERNATE status for a replacing node.

        See https://github.com/scylladb/scylla/issues/5449 for details.
        """
        logger.info("Starting cluster with 2 nodes.")
        self.init_cluster(2)
        node1, node2 = self.cluster.nodelist()
        logger.info(f"Node 1 address is {self.cluster.get_node_ip(1)}")

        node2_address = self.cluster.get_node_ip(2)
        logger.info(f"Node 2 address is {node2_address}")

        tokens = self.get_sorted_tokens(node2)
        logger.info(f"Detected number of tokens: {len(tokens)}")

        logger.info("Inserting Data...")
        node1.stress(["write", "n=10000", "-schema", "replication(factor=2)"])

        session = self.patient_cql_connection(node1)
        stress_table = "keyspace1.standard1"
        query = SimpleStatement(f"SELECT * FROM {stress_table} LIMIT 1", consistency_level=ConsistencyLevel.TWO)
        initial_data = list(session.execute(query))

        logger.info("Stopping node 2.")
        node2.stop()

        logger.info("Starting node 3 to replace node 2, but stop it in the middle of the replace.")
        node3 = self.cluster.new_node(3, auto_bootstrap=True, is_seed=False)
        node3.start(replace_address=node2_address, no_wait=True)

        node3_address = self.cluster.get_node_ip(3)
        logger.info(f"Node 3 address is {node3_address}")

        self.ignore_log_patterns += ['Startup failed']
        node3.stop()

        status1, err1 = node1.nodetool("gossipinfo")
        logger.info(f"gossipinfo:\n{status1}")
        assert "STATUS:hibernate,true" not in status1, "There is a node in HIBERNATE status."

        logger.info("Starting node 4 to replace node 2.")
        node4 = self.cluster.new_node(4, auto_bootstrap=True, is_seed=False)
        node4.start(replace_address=node2_address, wait_for_binary_proto=True, wait_other_notice=True)

        node4_address = self.cluster.get_node_ip(4)
        logger.info(f"Node 4 address is {node4_address}")

        status2, err2 = node1.nodetool("gossipinfo")
        logger.info(f"gossipinfo:\n{status2}")
        assert "STATUS:hibernate,true" not in status2, "There is a node in HIBERNATE status."
        assert f"/{node3_address}\n" not in status2, "Node 3 stays in gossip."

        logger.info("Verifying querying works.")
        final_data = list(session.execute(query))
        assert_lists_equal_ignoring_order(initial_data, final_data)

        logger.info("Verifying tokens migrated successfully.")
        moved_tokens_list = self.get_sorted_tokens(node4)
        logger.info(len(moved_tokens_list))
        assert moved_tokens_list == tokens

        logger.info("Verifying system.peers table.")
        peers = rows_to_list(session.execute("SELECT * FROM system.peers"))
        assert len(peers) == 1, "There are more peers than expected."

    def test_replace_with_background_workload(self):
        """
        The subtest is used to reproduce https://github.com/scylladb/scylla/issues/4705
        the background write workload continue running more than 30 seconds,
        the gossiper reached a timeout, and nodes raise 'unknown endpoint' error.
        """
        self.init_cluster(num_nodes=3)

        node1 = self.cluster.nodelist()[0]
        logger.info(node1.nodetool('status')[0])

        enable_nodetool_debug = False

        def nodetool_thread():
            logger.info('nodetool thread')
            for key in range(20):
                logger.info('enable_nodetool_debug: {}'.format(enable_nodetool_debug))
                logger.info(node1.nodetool('status')[0])
                logger.info(node1.nodetool("gossipinfo", True)[0])
            logger.info('nodetool thread: completed')

        def workload_thread():
            logger.info('workload thread: start')
            # The added scylla 3.1 node can be up quicker than latest master, 140s workload is enough for scylla 3.1
            node1.stress(['write', 'duration=320s', 'no-warmup', 'cl=QUORUM', '-rate',
                          'threads=1', '-schema', 'replication(factor=3)', '-pop', 'seq=1..1000'])
            logger.info('workload thread: completed')
            expect_msg = 'Expect workload continue running more than 30 seconds after new node is added'
            assert self.replace_done_time, expect_msg
            rest_time = (datetime.datetime.now() - self.replace_done_time).total_seconds()
            logger.info('Workload still executes {} seconds after new node is added. {}'.format(rest_time, expect_msg))
            assert rest_time > 30, expect_msg

        cs_thread = threading.Thread(target=workload_thread)
        cs_thread.start()

        if enable_nodetool_debug:
            nodetool_thread = threading.Thread(target=nodetool_thread)
            nodetool_thread.start()

        logger.info('Sleep 5 seconds to wait the workload starts')
        sleep(5)

        logger.info('Start to kill node3 ...')
        node3 = self.cluster.nodelist()[2]
        node3.stop(gently=False)
        logger.info('node3 has been killed')

        logger.info('Add a new node to replace the dead node')
        self.replace_done_time = None
        added_node = self.cluster.new_node(4, data_center='dc1', is_seed=False)
        added_node.start(replace_address=self.cluster.get_node_ip(3), wait_for_binary_proto=True)
        logger.info('Successfully add a new node to replace node3')
        self.replace_done_time = datetime.datetime.now()

        cs_thread.join(timeout=300)
        if enable_nodetool_debug:
            nodetool_thread.join(timeout=300)

        for node in self.cluster.nodelist():
            err_log = node.grep_log('unknown endpoint')[0:3]
            logger.info('{}: {}'.format(node.name, err_log))
            assert not err_log

    def test_replace_stopped_node_with_schema_rf_1(self):
        """
        Test that we can replace a node that is not shutdown gracefully
        and schema have replication factor equal 1

        """
        self.replace_node_with_schema_rf_1(gently=False)

    def test_replace_shutdown_node_with_schema_rf_1(self):
        """
        Test that we can replace a node that is shutdown gracefully
        and schema have replication factor equal 1
        """
        self.replace_node_with_schema_rf_1(gently=True)

    def replace_node_with_schema_rf_1(self, gently):
        self.init_cluster(num_nodes=3)
        node1, node2, node3 = self.cluster.nodelist()

        node3_tokens = self.get_sorted_tokens(node3)

        logger.info("Inserting Data...")
        node1.stress(['write', 'n=10000', '-schema', 'replication(factor=1)'])

        logger.info("Stopping node 3.")
        node3.stop(gently=gently, wait_other_notice=True)

        # replace node 3 with node 4
        logger.info("Starting node 4 to replace node 3")
        node4 = self.cluster.new_node(4, auto_bootstrap=True, is_seed=False)
        node4.start(replace_address=self.cluster.get_node_ip(3), wait_for_binary_proto=True)
        if not self.rbo_enabled:
            node4.watch_log_for(
                [r"WARN .* Unable to find sufficient sources to stream range .* for keyspace .* with RF = 1 for replace operation"])
        else:
            logger.info("Issue #6351 is not related to scylla with repair based operations enabled")

        logger.info("Verifying tokens migrated successfully")
        moved_tokens_list = self.get_sorted_tokens(node4)

        assert moved_tokens_list == node3_tokens, "Tokens were not moved correctly to node4"
        assert node4.is_live(), "Node4 is not alive after node4 has replaced node3"

    def test_replace_node_diff_ip(self):
        logger.info("Starting cluster with 5 nodes.")
        cluster = self.cluster
        cluster.populate(5).start(wait_for_binary_proto=True)
        node1, node2, node3, node4, node5 = cluster.nodelist()

        session = self.patient_cql_connection(node5)
        create_ks(session, 'ks', 3)
        create_cf(session, 'cf', read_repair=0.0, columns={'c1': 'text', 'c2': 'text'})
        insert_c1c2(session, keys=range(1000), consistency=ConsistencyLevel.ALL)

        ip5 = node5.address()
        node5.stop()

        logger.info("Starting node 6 to replace node 5")
        node6 = cluster.new_node(6, auto_bootstrap=True, is_seed=False)
        node6.start(wait_for_binary_proto=True, replace_address=ip5)
        for node in [node1, node2, node3, node4, node6]:
            node.watch_log_for(f"FatClient {ip5} has been silent for .*ms, removing from gossip")

    def test_replace_node_same_ip(self):
        logger.info("Starting cluster with 5 nodes.")
        cluster = self.cluster
        cluster.populate(5).start(wait_for_binary_proto=True)
        node1, node2, node3, node4, node5 = cluster.nodelist()

        session = self.patient_cql_connection(node5)
        create_ks(session, 'ks', 3)
        create_cf(session, 'cf', read_repair=0.0, columns={'c1': 'text', 'c2': 'text'})
        insert_c1c2(session, keys=range(1000), consistency=ConsistencyLevel.ALL)

        ip5 = node5.address()
        node5.stop()

        logger.info("Starting node 5 to replace node 5")
        node5.clear()
        jvm_args = ['--auto-bootstrap', 'true', '--seed-provider-parameters', 'seeds={}'.format(node1.address())]
        node5.start(wait_for_binary_proto=True, replace_address=ip5, jvm_args=jvm_args)

    @pytest.mark.dtest_heavy
    def test_replace_node_diff_ip_take_write(self):
        logger.info("Starting cluster with 5 nodes.")
        cluster = self.cluster
        cluster.populate(5).start(wait_for_binary_proto=True)
        node1, node2, node3, node4, node5 = cluster.nodelist()

        session = self.patient_cql_connection(node5)
        create_ks(session, 'ks', 3)
        create_cf(session, 'cf', read_repair=0.0, columns={'c1': 'text', 'c2': 'text'})
        insert_c1c2(session, keys=range(1000), consistency=ConsistencyLevel.ALL)

        ip5 = node5.address()
        node5.stop()

        rounds_cnt = 100 if not hasattr(cluster, 'scylla_mode') or cluster.scylla_mode != 'debug' else 10
        keys_per_round = 1000
        writes_diff_cnt = rounds_cnt * keys_per_round / 400

        stop = threading.Event()

        def insert_data(session, rounds, keys_per_round, stop):
            session.execute("use ks;")
            logger.info("Started to write rounds={}".format(rounds))
            for i in range(rounds):
                if stop.is_set():
                    logger.info("Write thread is stopped")
                    break
                start = i * keys_per_round
                end = start + keys_per_round
                if (start % 100000 == 0):
                    logger.info("Writing keys start={} , end={}".format(start, end))
                insert_c1c2(session, range(start, end), consistency=ConsistencyLevel.QUORUM)
            logger.info("Finished to write rounds={}".format(rounds))
            return rounds

        logger.info("Starting node 6 to replace node 5")
        node6 = cluster.new_node(6, auto_bootstrap=True, is_seed=False)
        node6.start(wait_for_binary_proto=False, replace_address=ip5)

        with_replacing_take_write_patch = True
        if with_replacing_take_write_patch:
            node6.watch_log_for(
                "Wait until peer nodes know the bootstrap tokens of local node|Started replace operation")
        else:
            node6.watch_log_for("Starting up server gossip")

        executor = ThreadPoolExecutor(max_workers=1)
        session = self.patient_cql_connection(node1)
        write_thread = executor.submit(insert_data, session, rounds_cnt, keys_per_round, stop)

        logger.info("Get metrics when other knows replacing node = HIBERNATE")
        metrics = ['scylla_database_total_writes', 'scylla_database_total_reads']
        writes_when_replace_ops_started = 0
        for node in [6, 1, 2, 3, 4]:
            node_metrics = get_node_metrics(node_ip=self.cluster.get_node_ip(node), metrics=metrics)
            logger.debug("scylla_database_total_writes: node{}={}".format(node, node_metrics))
            if node == 6:
                writes_when_replace_ops_started = node_metrics['scylla_database_total_writes']
                logger.info(f"writes_when_replace_ops_started={writes_when_replace_ops_started}")

        node6.watch_log_for("Bootstrap completed!")

        logger.info("Get metrics when other knows replacing node = NORMAL")
        metrics = ['scylla_database_total_writes', 'scylla_database_total_reads']
        writes_when_replace_ops_done = 0
        for node in [6, 1, 2, 3, 4]:
            node_metrics = get_node_metrics(node_ip=self.cluster.get_node_ip(node), metrics=metrics)
            logger.debug("scylla_database_total_writes: node{}={}".format(node, node_metrics))
            if node == 6:
                writes_when_replace_ops_done = node_metrics['scylla_database_total_writes']
                logger.info(f"writes_when_replace_ops_done={writes_when_replace_ops_done}")

        assert writes_when_replace_ops_done - writes_when_replace_ops_started > writes_diff_cnt

        stop.set()

        # Wait for node6 to finish the replace ops
        session = self.patient_cql_connection(node6)

        for node in [node1, node2, node3, node4, node6]:
            node.watch_log_for(f"FatClient {ip5} has been silent for .*ms, removing from gossip")

        write_thread.result()

    @pytest.mark.dtest_heavy
    def test_replace_node_same_ip_take_write(self):
        logger.info("Starting cluster with 5 nodes.")
        cluster = self.cluster
        cluster.populate(5).start(wait_for_binary_proto=True)
        node1, node2, node3, node4, node5 = cluster.nodelist()

        session = self.patient_cql_connection(node5)
        create_ks(session, 'ks', 3)
        create_cf(session, 'cf', read_repair=0.0, columns={'c1': 'text', 'c2': 'text'})
        insert_c1c2(session, keys=range(1000), consistency=ConsistencyLevel.ALL)

        ip5 = node5.address()
        node5.stop()
        mark = node5.mark_log()

        rounds_cnt = 100 if not hasattr(cluster, 'scylla_mode') or cluster.scylla_mode != 'debug' else 10
        keys_per_round = 1000
        writes_diff_cnt = rounds_cnt * keys_per_round / 400

        stop = threading.Event()

        def insert_data(session, rounds, keys_per_round, stop):
            session.execute("use ks;")
            logger.info("Started to write and read rounds={}".format(rounds))
            for i in range(1, rounds + 1):
                if stop.is_set():
                    logger.info("Write thread is stopped")
                    break
                start = i * keys_per_round
                end = start + keys_per_round
                if (start % 100000 == 0):
                    logger.info("Writing keys start={} , end={}".format(start, end))
                insert_c1c2(session, range(start, end), consistency=ConsistencyLevel.QUORUM)
            logger.info("Finished to write and read rounds={}".format(rounds))
            return rounds

        logger.info("Starting node 5 to replace node 5")
        node5.clear()
        jvm_args = ['--auto-bootstrap', 'true', '--seed-provider-parameters', 'seeds={}'.format(node1.address())]
        node5.start(wait_for_binary_proto=False, replace_address=ip5, jvm_args=jvm_args)

        with_replacing_take_write_patch = True
        if with_replacing_take_write_patch:
            node5.watch_log_for(
                "Wait until peer nodes know the bootstrap tokens of local node|Started replace operation",
                from_mark=mark)
        else:
            node5.watch_log_for("Starting up server gossip", from_mark=mark)

        executor = ThreadPoolExecutor(max_workers=1)
        session = self.patient_cql_connection(node1)
        write_thread = executor.submit(insert_data, session, rounds_cnt, keys_per_round, stop)

        metrics = ['scylla_database_total_writes', 'scylla_database_total_reads']
        logger.info("Get metrics when other knows replacing node = HIBERNATE")
        writes_when_replace_ops_started = 0
        for node in [5, 4, 3, 2, 1]:
            node_metrics = get_node_metrics(node_ip=self.cluster.get_node_ip(node), metrics=metrics)
            logger.info("metrics: node{}={}".format(node, node_metrics))
            if node == 5:
                writes_when_replace_ops_started = node_metrics['scylla_database_total_writes']
                logger.info(f"writes_when_replace_ops_started={writes_when_replace_ops_started}")

        node5.watch_log_for("Bootstrap completed!", from_mark=mark)
        logger.info("Get metrics when other knows replacing node = NORMAL")
        metrics = ['scylla_database_total_writes', 'scylla_database_total_reads']
        writes_when_replace_ops_done = 0
        for node in [5, 4, 3, 2, 1]:
            node_metrics = get_node_metrics(node_ip=self.cluster.get_node_ip(node), metrics=metrics)
            logger.info("metrics: node{}={}".format(node, node_metrics))
            if node == 5:
                writes_when_replace_ops_done = node_metrics['scylla_database_total_writes']
                logger.info(f"writes_when_replace_ops_done={writes_when_replace_ops_done}")

        assert writes_when_replace_ops_done - writes_when_replace_ops_started > writes_diff_cnt

        stop.set()
        # Wait for node5 to finish the replace ops
        session = self.patient_cql_connection(node5)

        write_thread.result()
