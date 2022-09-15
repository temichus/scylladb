import logging
import os
import random
import re
import subprocess
import tempfile
import time
from concurrent.futures.thread import ThreadPoolExecutor

import pytest
from cassandra import ConsistencyLevel
from cassandra.concurrent import execute_concurrent_with_args
from ccmlib.node import NodeError
from psutil import Process

from ccmlib.scylla_node import ScyllaNode
from dtest_class import create_cf, create_ks, Tester, get_ip_from_node
from dtest_setup import DTestSetup
from dtest_setup_overrides import DTestSetupOverrides
from tools.assertions import (assert_almost_equal,
                              assert_one, assert_all)
from tools.cluster import new_node
from tools.data import query_c1c2, insert_c1c2, create_c1c2_table
from tools.intervention import InterruptBootstrap, KillOnBootstrap
from tools.misc import ImmutableMapping, require
from tools.stress import format_cs_output, assert_cs_success

logger = logging.getLogger(__name__)


@pytest.mark.dtest_full
class TestBootstrap(Tester):  # pylint: disable=too-many-public-methods
    @pytest.fixture(scope='function', autouse=True)
    def fixture_dtest_setup_overrides(self, dtest_config):
        dtest_setup_overrides = DTestSetupOverrides()
        dtest_setup_overrides.cluster_options = ImmutableMapping({'start_rpc': 'true'})
        return dtest_setup_overrides

    @pytest.fixture(autouse=True)
    def fixture_add_additional_log_patterns(self, fixture_dtest_setup: DTestSetup):
        fixture_dtest_setup.ignore_log_patterns += [
            # This one occurs when trying to send the migration to a
            # node that hasn't started yet, and when it does, it gets
            # replayed and everything is fine.
            r'Can\'t send migration request: node.*is down',
            # ignore streaming error during bootstrap
            r'Exception encountered during startup',
            r'Streaming error occurred'
        ]

    def get_space_used(self, node, table_name='cf'):
        output, *_ = node.nodetool('cfstats')
        if output.find(table_name) != -1:
            output = output[output.find(table_name):]
            output = output[output.find("Space used (total)"):]
            initial_value = int(output[output.find(":") + 1:output.find("\n")].strip())
            return initial_value
        return -1

    @pytest.mark.next_gating
    @pytest.mark.dtest_debug
    @pytest.mark.dtest_smoke
    @pytest.mark.single_node
    def test_start_stop(self):
        logger.info("populating cluster with one node")
        cluster = self.cluster
        cluster.populate(1)
        logger.info("starting cluster")
        cluster.start(wait_for_binary_proto=True, wait_other_notice=True)
        logger.info("stopping cluster")
        cluster.stop()
        logger.info("done")

    @pytest.mark.next_gating
    @pytest.mark.dtest_debug
    @pytest.mark.dtest_smoke
    def test_start_stop_node(self):
        logger.info("populating cluster with three nodes")
        cluster = self.cluster
        cluster.populate(3)
        logger.info("starting cluster")
        cluster.start(wait_for_binary_proto=True, wait_other_notice=True)
        logger.info("stopping node")
        node1 = cluster.nodelist()[0]
        node1.stop(wait_other_notice=True, wait_seconds=10)
        logger.info("stopping cluster")
        cluster.stop()
        logger.info("done")

    @pytest.mark.next_gating
    @pytest.mark.dtest_debug
    def test_add_node(self):
        logger.info("populating cluster with three nodes")
        cluster = self.cluster
        cluster.populate(2)
        logger.info("starting cluster")
        cluster.start(wait_other_notice=True)
        logger.info("adding node3")
        node3 = cluster.new_node(3)
        logger.info("starting node3")
        node3.start(wait_other_notice=True)
        logger.info("stopping cluster")
        cluster.stop()
        logger.info("done")

    @pytest.mark.next_gating
    @pytest.mark.dtest_debug
    def test_add_detached_node(self, request: pytest.FixtureRequest):
        logger.info("populating cluster with three nodes")
        cluster = self.cluster
        cluster.populate(2)
        logger.info("starting cluster")
        cluster.start(wait_other_notice=True)
        logger.info("adding node3")
        node3 = cluster.new_node(3, add_node=False)

        def stop_node3():
            logger.info("stopping node3")
            node3.stop()

        request.addfinalizer(stop_node3)

        logger.info("starting node3")
        node3.start(wait_other_notice=True)
        logger.info("stopping cluster")
        cluster.stop()
        logger.info("done")

    def test_off_strategy_during_bootstrap(self):
        """
        Compaction will be disabled during repair-based bootstrap and replace.
        """
        cluster = self.cluster
        cluster.set_configuration_options(values={'enable_repair_based_node_ops': True})
        keys = 10000
        keyspace_name = 'ks'
        table_name = 'cf'

        # Create a single node cluster
        cluster.populate(1)
        node1 = cluster.nodelist()[0]
        cluster.start()

        with self.patient_cql_connection(node1) as session:
            create_ks(session, keyspace_name, 1)
            create_cf(session, table_name, columns={'c1': 'text', 'c2': 'text'})

            insert_statement = session.prepare(f"INSERT INTO {keyspace_name}.{table_name} (key, c1, c2) "
                                               f"VALUES (?, 'value1', 'value2')")
            execute_concurrent_with_args(session, insert_statement, [['k%d' % k] for k in range(keys)])

        node1.flush()

        # Bootstrapping a new node
        node2 = new_node(cluster)
        node2.start(wait_for_binary_proto=True)

        self._validate_off_strategy_started(node=node2, keyspace=keyspace_name, table=table_name, from_mark=0)

        matched_logs = node2.grep_log("Compacted .* sstables to |Starting to bootstrap|Bootstrap completed!")
        bootstrap_status = None
        for item in matched_logs:
            line = item[0]
            if 'Starting to bootstrap' in line:
                bootstrap_status = 'START'
            elif 'Bootstrap completed!' in line:
                bootstrap_status = 'END'
                break
            if bootstrap_status == 'START' and f'Compact {keyspace_name}.{table_name} ' in line \
                    and 'Compacted ' in line:
                raise Exception("Unexpected compaction of test table occurred during bootstrap, off-strategy doesn't"
                                " work")
        assert bootstrap_status == 'END'

        session = self.patient_cql_connection(node2)
        assert_one(session, "SELECT count(*) from ks.cf", [keys], cl=ConsistencyLevel.ONE)

    def test_simple_bootstrap(self):
        cluster = self.cluster
        tokens = cluster.balanced_tokens(2)
        cluster.set_configuration_options(values={'num_tokens': 1})

        logger.info("[node1, node2] tokens: %r", tokens)

        keys = 10000

        # Create a single node cluster
        cluster.populate(1)
        node1 = cluster.nodelist()[0]
        node1.set_configuration_options(values={'initial_token': tokens[0]})
        cluster.start(wait_other_notice=True)

        session = self.patient_cql_connection(node1)
        create_ks(session, 'ks', 1)
        create_cf(session, 'cf', columns={'c1': 'text', 'c2': 'text'})

        insert_statement = session.prepare("INSERT INTO ks.cf (key, c1, c2) VALUES (?, 'value1', 'value2')")
        execute_concurrent_with_args(session, insert_statement, [['k%d' % k] for k in range(keys)])

        node1.flush()
        node1.compact()

        data_total_size_node1 = self.get_space_used(node1)
        logger.info("before={}".format(data_total_size_node1))

        # Reads inserted data all during the bootstrap process. We shouldn't
        # get any error
        reader = self.go(lambda _: query_c1c2(session, random.randint(0, keys - 1), ConsistencyLevel.ONE))

        # Bootstraping a new node
        node2 = cluster.new_node(2)
        node2.set_configuration_options(values={'initial_token': tokens[1]})
        node2.start(wait_for_binary_proto=True)
        node2.flush()
        node2.compact()

        reader.check()
        node1.cleanup()
        node1.compact()
        time.sleep(.5)
        reader.check()

        data_total_size_node1_after = self.get_space_used(node1)
        data_total_size_node2_after = self.get_space_used(node2)

        logger.info("before={}, after={} + {}={}".format(data_total_size_node1, data_total_size_node1_after,
                                                         data_total_size_node2_after, data_total_size_node1_after+data_total_size_node2_after))
        assert_almost_equal(data_total_size_node1, data_total_size_node1_after + data_total_size_node2_after, error=0.3)
        assert_almost_equal(data_total_size_node1_after, data_total_size_node2_after, error=0.3)

    def test_schema_is_pulled_before_schema_is_declared_complete(self):
        """Test that bootstrapping node does a schema pull, before claiming to have a complete schema."""

        cluster = self.cluster
        cluster.populate(1)
        cluster.start(wait_for_binary_proto=True)

        node1 = cluster.nodelist()[0]

        node2 = cluster.new_node(2)
        node2.start(wait_for_binary_proto=True)

        messages = [
            "Pulling schema from {}(:[0-9]+)?".format(node1.address()),
            "Schema merge with {}(:[0-9]+)? completed".format(node1.address()),
            "Checking bootstrapping/leaving nodes: ok",
        ]

        matches = node2.watch_log_for(messages)

        # watch_log_for() should already ensure this, but just to be sure...
        assert len(messages) == len(matches)

        # Make sure the order of the matching log lines is exactly that of in `messages`.
        for msg_re, match in zip(messages, matches):
            log_line = match[0]
            assert re.search(msg_re, log_line) is not None

    def test_read_from_bootstrapped_node(self):
        """Test bootstrapped node sees existing data, eg. CASSANDRA-6648"""
        cluster = self.cluster
        cluster.populate(3)
        cluster.start()

        node1 = cluster.nodes['node1']
        node1.stress(['write', 'n=10000', '-rate', 'threads=8'])

        session = self.patient_cql_connection(node1)
        stress_table = 'keyspace1.standard1'
        original_rows = list(session.execute("SELECT * FROM %s" % (stress_table,)))

        node4 = cluster.new_node(4)
        node4.start(wait_for_binary_proto=True)

        session = self.patient_exclusive_cql_connection(node4)
        new_rows = list(session.execute("SELECT * FROM %s" % (stress_table,)))
        assert original_rows == new_rows

    # new node failed with error:
    #     Startup failed: exceptions::unavailable_exception (Cannot achieve consistency level for cl QUORUM. Requires 2,
    #     alive 1)
    #
    # because of node1 was stopped before the new node was added. And as result log watching failed
    # (because Scylla process failed) - ccmlib.node.Node.watch_log_for, l. 431
    #
    # Asias comment:
    #
    # it is expected to fail the bootstrap operation if one of the node is down. We now favor safety when adding nodes.
    #
    # The procedure to fix it is that before we add new node, we should fix existing nodes in the cluster
    # either fix the network or replace the dead node
    #
    # Also the test start the new node with 2 parameters that are not supported by Scylla:
    # - 'stream_throughput_outbound_megabits_per_sec'
    # - 'streaming_socket_timeout_in_ms are not supported in scylla
    @pytest.mark.skip('fail the bootstrap operation if one of the node is down.')
    def test_resumable_bootstrap(self):
        """Test resuming bootstrap after data streaming failure"""

        cluster = self.cluster
        cluster.populate(2).start(wait_other_notice=True)

        node1 = cluster.nodes['node1']
        node1.stress(['write', 'n=100000', '-schema', 'replication(factor=2)'])
        node1.flush()

        # kill node1 in the middle of streaming to let it fail
        thread = InterruptBootstrap(node1)
        thread.start()

        # start bootstrapping node3 and wait for streaming
        node3 = cluster.new_node(3)
        node3.set_configuration_options(values={'stream_throughput_outbound_megabits_per_sec': 1})
        # keep timeout low so that test won't hang
        node3.set_configuration_options(values={'streaming_socket_timeout_in_ms': 1000})
        with pytest.raises(NodeError):
            node3.start()
        thread.join()

        # wait for node3 ready to query
        node3.watch_log_for("Starting listening for CQL clients")
        mark = node3.mark_log()
        # check if node3 is still in bootstrap mode
        session = self.exclusive_cql_connection(node3)
        rows = list(session.execute("SELECT bootstrapped FROM system.local WHERE key='local'"))
        assert len(rows) == 1
        assert rows[0][0] == 'IN_PROGRESS', rows[0][0]
        # bring back node1 and invoke nodetool bootstrap to resume bootstrapping
        node1.start(wait_other_notice=True)
        node3.nodetool('bootstrap resume')
        # check if we skipped already retrieved ranges
        node3.watch_log_for("already available. Skipping streaming.")
        node3.watch_log_for("Resume complete", from_mark=mark)
        rows = list(session.execute("SELECT bootstrapped FROM system.local WHERE key='local'"))
        assert rows[0][0] == 'COMPLETED', rows[0][0]

    @pytest.mark.skip('Scylla does not support the cassandra.reset_bootstrap_progress option and has no alternative'
                      ' parameter ')
    def test_bootstrap_with_reset_bootstrap_state(self):
        """Test bootstrap with resetting bootstrap progress"""

        cluster = self.cluster
        cluster.set_configuration_options(values={'stream_throughput_outbound_megabits_per_sec': 1})
        cluster.populate(2).start(wait_other_notice=True)

        node1 = cluster.nodes['node1']
        node1.stress(['write', 'n=100000', '-schema', 'replication(factor=2)'])
        node1.flush()

        # kill node1 in the middle of streaming to let it fail
        thread = InterruptBootstrap(node1)
        thread.start()

        # start bootstrapping node3 and wait for streaming
        node3 = cluster.new_node(3)
        with pytest.raises(NodeError):
            node3.start()
        thread.join()
        node1.start()

        # restart node3 bootstrap with resetting bootstrap progress
        node3.stop()
        mark = node3.mark_log()
        node3.start(jvm_args=["-Dcassandra.reset_bootstrap_progress=true"])
        # check if we reset bootstrap state
        node3.watch_log_for("Resetting bootstrap progress to start fresh", from_mark=mark)
        # wait for node3 ready to query
        node3.watch_log_for("Listening for thrift clients...", from_mark=mark)

        # check if 2nd bootstrap succeeded
        session = self.exclusive_cql_connection(node3)
        rows = list(session.execute("SELECT bootstrapped FROM system.local WHERE key='local'"))
        assert len(rows) == 1
        assert rows[0][0] == 'COMPLETED', rows[0][0]

    @pytest.mark.next_gating
    @pytest.mark.dtest_debug
    def test_manual_bootstrap(self):
        """Test adding a new node and bootstrappig it manually. No auto_bootstrap.
           This test also verify that all data are OK after the addition of the new node.
           eg. CASSANDRA-9022
        """
        cluster = self.cluster
        cluster.populate(2).start(wait_other_notice=True)
        (node1, node2) = cluster.nodelist()

        node1.stress(['write', 'n=1000', '-schema', 'replication(factor=2)',
                      '-rate', 'threads=1', '-pop', 'dist=UNIFORM(1..1000)'])

        session = self.patient_exclusive_cql_connection(node2)
        stress_table = 'keyspace1.standard1'

        original_rows = list(session.execute("SELECT * FROM %s" % stress_table))

        # Add a new node
        node3 = cluster.new_node(3, auto_bootstrap=False)
        node3.start(wait_for_binary_proto=True)
        node3.repair()
        node1.cleanup()

        current_rows = list(session.execute("SELECT * FROM %s" % stress_table))
        assert original_rows == current_rows

    @pytest.mark.scylla_mode('!debug')
    def test_local_quorum_bootstrap(self):
        """Test that CL local_quorum works while a node is bootstrapping. CASSANDRA-8058"""

        cluster = self.cluster
        cluster.populate([1, 1])
        cluster.start()

        node1 = cluster.nodes['node1']
        yaml_config = """
        # Create the keyspace and table
        keyspace: keyspace1
        keyspace_definition: |
          CREATE KEYSPACE keyspace1 WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 1, 'dc2': 1};
        table: users
        table_definition:
          CREATE TABLE users (
            username text,
            first_name text,
            last_name text,
            email text,
            PRIMARY KEY(username)
          ) WITH compaction = {'class':'SizeTieredCompactionStrategy'};
        insert:
          partitions: fixed(1)
          batchtype: UNLOGGED
        queries:
          read:
            cql: select * from users where username = ?
            fields: samerow
        """
        stress_config = tempfile.NamedTemporaryFile(mode='w+', delete=False)
        stress_config.write(yaml_config)
        stress_config.close()
        node1.stress(['user', 'profile=' + stress_config.name, 'n=2000000',
                      'ops(insert=1)', '-rate', 'threads=50'])

        node3 = cluster.new_node(3, data_center='dc2')
        node3.start(no_wait=True)
        time.sleep(3)

        with tempfile.TemporaryFile(mode='w+') as tmpfile:
            node1.stress(['user', 'profile=' + stress_config.name, 'ops(insert=1)',
                          'n=500000', 'cl=LOCAL_QUORUM',
                          '-rate', 'threads=5',
                          '-errors', 'retries=2'],
                         stdout=tmpfile, stderr=subprocess.STDOUT)
            os.unlink(stress_config.name)

            tmpfile.seek(0)
            output = tmpfile.read()

        logger.info(output)
        regex = re.compile("Operation.+error inserting key.+Exception")
        failure = regex.search(str(output))
        assert failure is None, "Error during stress while bootstrapping"

    def test_shutdown_wiped_node_cannot_join(self):
        self._wiped_node_cannot_join_test(gently=True)

    def test_killed_wiped_node_cannot_join(self):
        self._wiped_node_cannot_join_test(gently=False)

    def _wiped_node_cannot_join_test(self, gently):
        """
        @jira_ticket CASSANDRA-9765
        Test that if we stop a node and wipe its data then the node cannot join
        when it is not a seed. Test both a nice shutdown or a forced shutdown, via
        the gently parameter.
        """
        cluster = self.cluster
        cluster.populate(3)
        cluster.start(wait_for_binary_proto=True)

        stress_table = 'keyspace1.standard1'

        # write some data
        node1 = cluster.nodelist()[0]
        node1.stress(['write', 'n=10000', '-rate', 'threads=8'])

        session = self.patient_cql_connection(node1)
        original_rows = list(session.execute("SELECT * FROM {}".format(stress_table,)))

        # Add a new node, bootstrap=True ensures that it is not a seed
        node4 = cluster.new_node(4, auto_bootstrap=True)
        node4.start(wait_for_binary_proto=True)

        session = self.patient_cql_connection(node4)
        assert original_rows == list(session.execute("SELECT * FROM {}".format(stress_table,)))

        # Stop the new node and wipe its data
        node4.stop(gently=gently)
        self._cleanup(node4)

        # Now start it, it should not be allowed to join.
        expected_error = "A node with address {} already exists, cancelling join".format(self.cluster.get_node_ip(4))
        self.ignore_log_patterns += [expected_error]
        mark = node4.mark_log()
        try:
            node4.start(no_wait=True)
        except NodeError:
            # It is expected that the node will not boot
            pass
        node4.watch_log_for(expected_error, from_mark=mark)

    @staticmethod
    def _validate_off_strategy_started(node: ScyllaNode, keyspace: str, table: str, from_mark: int):
        logger.debug(f"Validate off-strategy start on the {node.name} node")
        off_strategy_message = f"Starting off-strategy compaction for {keyspace}.{table}"
        matched_logs = node.grep_log(f"{off_strategy_message}|Starting to bootstrap",
                                     from_mark=from_mark)
        bootstrap_status = None
        off_strategy_run = False
        for item in matched_logs:
            line = item[0]
            if 'Starting to bootstrap' in line:
                bootstrap_status = 'START'

            if bootstrap_status == 'START' and off_strategy_message in line:
                off_strategy_run = True
                break

        assert off_strategy_run, "off-strategy was not started during bootstrap"

    def test_decommissioned_wiped_node_can_join(self):
        """
        @jira_ticket CASSANDRA-9765
        Test that if we decommission a node and then wipe its data, it can join the cluster.
        """
        cluster = self.cluster
        cluster.populate(3)
        cluster.start(wait_for_binary_proto=True)

        keyspace_name = 'keyspace1'
        table_name = 'standard1'
        query = f"SELECT * FROM {keyspace_name}.{table_name}"

        # write some data
        node1 = cluster.nodelist()[0]
        node1.stress(['write', 'n=10K', '-rate', 'threads=8'])

        with self.patient_cql_connection(node1) as session:
            original_rows = list(session.execute(query))

        # Add a new node, bootstrap=True ensures that it is not a seed
        logger.info("Starting node4")
        node4 = cluster.new_node(4, auto_bootstrap=True)
        node4.start(wait_for_binary_proto=True, wait_other_notice=True)

        with self.patient_cql_connection(node4) as session:
            assert_all(session=session, query=query, expected=original_rows, cl=ConsistencyLevel.QUORUM,
                       ignore_order=True)

        # Decommision the new node and wipe its data
        logger.info("Decommissioning node4")
        mark = node1.mark_log()
        node4.decommission()
        logger.debug("Stopping node4")
        node4.stop(wait_other_notice=False)
        node1.watch_log_for("{} is now (dead|DOWN)".format(node4.address()), from_mark=mark)
        data_dir = os.path.join(node4.get_path(), 'data')
        commitlog_dir = os.path.join(node4.get_path(), 'commitlogs')
        logger.debug("Deleting {}".format(data_dir))
        node4.rmtree(data_dir)
        node4.rmtree(commitlog_dir)

        # Now start it, it should be allowed to join
        ip4 = get_ip_from_node(node=node4)
        node1.watch_log_for(f"{ip4} gossip quarantine over")
        logger.debug("Restarting node4")
        mark = node4.mark_log()
        node4.start(wait_for_binary_proto=True, wait_other_notice=True)
        logger.debug("Waiting for node4 to join")
        node4.watch_log_for("Starting to bootstrap", from_mark=mark, timeout=0)

        self._validate_off_strategy_started(node=node4, keyspace=keyspace_name, table=table_name, from_mark=mark)

    def test_failed_bootstap_wiped_node_can_join(self):
        """
        @jira_ticket CASSANDRA-9765
        Test that if a node fails to bootstrap, it can join the cluster even if the data is wiped.
        """
        cluster = self.cluster
        cluster.populate(1)
        cluster.start(wait_for_binary_proto=True)

        stress_table = 'keyspace1.standard1'

        # write some data, enough for the bootstrap to fail later on
        node1 = cluster.nodelist()[0]
        node1.stress(['write', 'n=100000', '-rate', 'threads=8'])
        node1.flush()

        session = self.patient_cql_connection(node1)
        original_rows = list(session.execute("SELECT * FROM {}".format(stress_table)))

        # Add a new node, bootstrap=True ensures that it is not a seed
        node2 = cluster.new_node(2, auto_bootstrap=True)
        node2.set_configuration_options(values={'stream_throughput_outbound_megabits_per_sec': 1})

        # kill node2 in the middle of bootstrap
        thread = KillOnBootstrap(node2)
        thread.start()

        mark = node1.mark_log()
        logger.info("Starting node2")
        node2.start(wait_for_binary_proto=False, wait_other_notice=False)
        thread.join()
        assert not node2.is_running()
        logger.info("node2 killed during bootstrap. Waiting for other nodes to notice...")
        node1.watch_log_for("{} has been silent .* removing from gossip".format(node2.address()), from_mark=mark)

        # wipe any data for node2
        self._cleanup(node2)

        # Now start it again, it should be allowed to join
        ip2 = get_ip_from_node(node=node2)
        node1.watch_log_for(f"{ip2} gossip quarantine over")
        mark = node2.mark_log()
        logger.debug("Restarting node2")
        node2.start(wait_for_binary_proto=True, wait_other_notice=True)
        logger.debug("Waiting for node2 to join")
        node2.watch_log_for("Starting to bootstrap", from_mark=mark, timeout=0)

    # In Scylla when one node bootstraps, it will check if there is any node in bootstrap status in gossip.
    # If it finds one, it will stop bootstrap (Asias)
    @pytest.mark.skip('not relevant for Scylla')
    def test_simultaneous_bootstrap(self):
        """
        Attempt to bootstrap two nodes at once, to assert the second bootstrapped node fails, and does not interfere.

        Start a one node cluster and run a stress write workload.
        Start up a second node, and wait for the first node to detect it has joined the cluster.
        While the second node is bootstrapping, start a third node. This should fail.

        @jira_ticket CASSANDRA-7069
        @jira_ticket CASSANDRA-9484
        """

        bootstrap_error = ["Other bootstrapping/leaving/moving nodes detected,"
                           " cannot bootstrap while cassandra.consistent.rangemovement is true"]

        self.ignore_log_patterns.append(bootstrap_error)

        cluster = self.cluster
        cluster.populate(1)
        cluster.start(wait_for_binary_proto=True)

        node1, = cluster.nodelist()

        node1.stress(['write', 'n=500K', '-schema', 'replication(factor=1)',
                      '-rate', 'threads=10'])

        node2 = cluster.new_node(2)
        node2.start(wait_other_notice=True)

        node3 = cluster.new_node(3, remote_debug_port='2003')
        process = node3.start()
        stderr = process.communicate()[1]
        assert bootstrap_error in stderr, stderr
        time.sleep(.5)
        node2.watch_log_for("Starting listening for CQL clients")

        session = self.patient_exclusive_cql_connection(node2)

        # Repeat the select count(*) query, to help catch
        # bugs like 9484, where count(*) fails at higher
        # data loads.
        for _ in range(5):
            assert_one(session, "SELECT count(*) from keyspace1.standard1", [500000], cl=ConsistencyLevel.ONE)

    def _full_cluster_recovery_after_stop(self, gently, num_of_nodes, rf):
        """
        steps:
        - Create N node cluster (RF=rf)
        - Insert some data with cassandra-stress (wait all data is inserted, no flush manually)
        - Flush (only needed on D-test otherwise the test will fail - no data will be written w/o the flush)
        - Stop cluster (gently/forcibly)
        - Start the cluster
        - read data make sure all data is alive
        """
        # Create/Start cluster
        cluster = self.cluster
        cluster.populate(num_of_nodes).start(wait_for_binary_proto=True)
        node1 = cluster.nodelist()[0]
        session = self.patient_cql_connection(node1)

        logger.info("Preparing a KS and a CF...")
        create_ks(session, name='ks', rf=rf)
        create_c1c2_table(session)

        logger.info("Populating the data...")
        insert_c1c2(session, n=10000, consistency=ConsistencyLevel.QUORUM)
        # This flush will only be needed on d-test otherwise the test will fail (no data will be written)
        cluster.flush()

        logger.info("Saving nodes process list")
        pid_ls = [node.pid for node in cluster.nodelist()]
        process_ls = []
        for pid in pid_ls:
            process = Process(pid)
            process_ls.append(process)

        logger.info("Killing all nodes")
        cluster.stop_nodes(gently=gently, wait_seconds=20)

        logger.info("Making sure all node processes are down")
        for process in process_ls:
            assert not process.is_running(), f"Node with the following pid {process.pid} didn't stop/exit correctly"

        logger.info("Starting all nodes")
        cluster.start_nodes(no_wait=False)

        logger.info("Checking that no data was lost")
        for key in range(10000):
            query_c1c2(session, key, ConsistencyLevel.QUORUM)

    def test_full_cluster_recovery_after_forcibly_stop_3_nodes_rf_3(self):
        self._full_cluster_recovery_after_stop(gently=False, num_of_nodes=3, rf=3)

    def test_full_cluster_recovery_after_gentle_stop_5_nodes_rf_2(self):
        self._full_cluster_recovery_after_stop(gently=True, num_of_nodes=5, rf=2)

    def test_full_cluster_recovery_after_forcibly_stop_4_nodes_rf_1(self):
        self._full_cluster_recovery_after_stop(gently=False, num_of_nodes=4, rf=1)

    def _cluster_become_unavailable_when_kill_node_during_bootstrap(self,  # pylint: disable=too-many-locals
                                                                    is_gracefully=True):
        """
        Add n1,n2
        Create ks with RF =2
        Insert data with CL = 2
        Bootstrap n3
        Kill n3 before n3 finishes bootstrap
        Check n1 and n2 will notice n3 is gone
        Check writes with CL = 2 will recover

        https://github.com/scylladb/scylla/issues/4488
        """
        executor = ThreadPoolExecutor(max_workers=2)
        bootstrap_msg = "Starting to bootstrap"
        kill_node_err_msg = "The process is dead, returncode={}"
        ks_name, consistency_level_key = "keyspace", "TWO"
        beginning_stream_session_msg = f"Beginning stream session|sync data for keyspace={ks_name}, status=started"
        removing_from_gossip_msg = r"FatClient {} has been silent for (\d+)ms, removing from gossip"
        stress_duration_minutes = 3
        replication_factor = 2
        cluster_size = 2

        cluster = self.cluster
        logger.info("Creating new cluster with '%s' nodes", cluster_size)
        cluster.populate(nodes=cluster_size).start(wait_for_binary_proto=True, wait_other_notice=True)
        node1, node2 = cluster.nodelist()

        write_stress_cmd = ["write", f"cl={consistency_level_key}", f"duration={stress_duration_minutes}m",
                            "-rate", "threads=10", "-log", "interval=5", "-schema",
                            f"replication(factor={replication_factor}) keyspace={ks_name}"]

        logger.info("Executing the following write stress command '%s'", write_stress_cmd)
        stress_thread = executor.submit(lambda: node1.stress(stress_options=write_stress_cmd, capture_output=True))

        logger.info("Adding new node")
        node3 = cluster.new_node(i=cluster_size + 1, debug=True, auto_bootstrap=True, is_seed=False)
        start_new_node_thread = executor.submit(lambda: node3.start(
            wait_for_binary_proto=True, jvm_args=['--logger-log-level', 'stream_session=debug'], no_wait=True))
        mark_log = node3.mark_log()

        logger.info("Trying to find the following '%s' message in logs of node '%s'", bootstrap_msg, node3.name)
        node3.watch_log_for(exprs=bootstrap_msg, from_mark=mark_log)
        logger.info(
            "Trying to find the following '%s' message in logs of node '%s'", beginning_stream_session_msg, node3.name)
        node3.watch_log_for(exprs=beginning_stream_session_msg, from_mark=mark_log)

        nodes = [node1, node2]
        mark_log_list = [node.mark_log() for node in nodes]
        logger.info("%s killing node'%s' (PID is '%s')", {'Gracefully' if is_gracefully else 'Force'}, node3.name,
                    node3.pid)
        node3.stop(wait=True, gently=is_gracefully)
        removing_from_gossip_msg = removing_from_gossip_msg.format(get_ip_from_node(node=node3))
        for node, mark_log in zip(nodes, mark_log_list):
            logger.info("Checking the following message '%s' exits in node '%s'", removing_from_gossip_msg, node.name)
            node.watch_log_for(exprs=removing_from_gossip_msg, from_mark=mark_log)

        assert kill_node_err_msg.format(1 if is_gracefully else -9) == str(start_new_node_thread.exception()), \
            f"The node '{node3.name}' should be killed by SIGKILL signal"
        logger.info("Waiting until stress thread will finish running")
        results = stress_thread.result()
        logger.debug(format_cs_output(results))
        assert_cs_success(results)

    @require("#4488")
    def test_cluster_become_unavailable_when_force_kill_node_during_bootstrap(self):
        self._cluster_become_unavailable_when_kill_node_during_bootstrap(is_gracefully=False)

    @require("#4488")
    def test_cluster_become_unavailable_when_gracefully_kill_node_during_bootstrap(self):
        self._cluster_become_unavailable_when_kill_node_during_bootstrap(is_gracefully=True)

    def test_ignore_auto_bootstrap_option(self):
        """
        After get rid of seed concept by Asias, auto_bootstrap option will be ignored.
        Bootstrap of first node which had smallest ip will be skipped, and bootstrap
        of other nodes will be enabled all the time.

        related PR: https://github.com/scylladb/scylla/pull/6848
        """
        cluster = self.cluster
        cluster.populate(3)
        (node1, node2, node3) = self.cluster.nodelist()

        # node1: auto_bootstrap option will be ignored even it's set to True
        node1.set_configuration_options(values={'auto_bootstrap': True})
        # node2: normal test
        node2.set_configuration_options(values={'auto_bootstrap': True})
        # node3: auto_bootstrap option will be ignored even it's set to False
        node3.set_configuration_options(values={'auto_bootstrap': False})
        cluster.start(wait_for_binary_proto=True)

        skip_bootstrap_msg = "I am the first node in the cluster. Skip bootstrap"
        start_bootstrap_msg = 'Starting to bootstrap'

        node1.watch_log_for(exprs=skip_bootstrap_msg)
        logger.info("Verified bootstrap didn't start on node1")
        node2.watch_log_for(exprs=start_bootstrap_msg)
        logger.info("Verified bootstrap started on node2")
        node3.watch_log_for(exprs=start_bootstrap_msg)
        logger.info("Verified bootstrap started on node3")

        session = self.patient_exclusive_cql_connection(node1)
        create_ks(session, 'ks', 3)
        create_cf(session, 'cf', read_repair=0.0, columns={'c1': 'text', 'c2': 'text'})
        insert_c1c2(session, n=1000)

        # node4: auto_bootstrap option will be ignored for new node
        node4 = cluster.new_node(4, auto_bootstrap=False)
        node4.start(wait_for_binary_proto=True)
        node4.watch_log_for(exprs=start_bootstrap_msg)
        logger.info("Verified bootstrap started on node4")
        for k in range(1000):
            query_c1c2(session, k)

    # The test is disabled in Raft mode since it's checking what happens
    # if one of the nodes listed in seed list is not available at boot time.
    # With Raft on, the entire cluster simply doesn't boot,
    # so it's impossible to observe the effects observed in the test.
    @pytest.mark.gossip_only
    def test_smallest_ip_join_late(self):
        """
        The first node has smallest ip in seeds list, it always skips the bootstrap.
        In this test we start the other nodes first, before the first node is started,
        and we expect them to fail their bootstrap. See scylladb/scylla#7726
        """
        cluster = self.cluster
        cluster.populate(3)
        (node1, node2, node3) = self.cluster.nodelist()

        # node1 has the smallest ip, but it's started later, expect bootstrap
        # even the option isn't enabled
        node1.set_configuration_options(values={'auto_bootstrap': False})
        # node2: normal test
        node2.set_configuration_options(values={'auto_bootstrap': True})
        # node3: auto_bootstrap option will be ignored even it's set to False
        node3.set_configuration_options(values={'auto_bootstrap': False})

        skip_shadow_round_msg = "All nodes.* are down.* Skip ShadowRound"
        start_failure_msg = "Startup failed: .*Failed to learn about other nodes' tokens during bootstrap"

        self.ignore_log_patterns.append(start_failure_msg)

        # only start node2 and node3, node2 will be the real `first node`
        node2.start(wait_for_binary_proto=False, wait_other_notice=False)

        node2.watch_log_for(exprs=skip_shadow_round_msg)
        logger.info("Verified shadow round doesn't start on node2")

        node3.start(wait_for_binary_proto=False, wait_other_notice=False)

        node2.watch_log_for(exprs=start_failure_msg)
        node3.watch_log_for(exprs=start_failure_msg)

    def test_seeds_on_duty(self):
        """
        This test try to stop original seeds after added new node, then try to add more node.
        Expect the seeds duty will be transferred to other nodes.
        """
        logger.info("populating cluster with 2 nodes")
        cluster = self.cluster
        cluster.populate(2)
        (node1, node2) = cluster.nodelist()
        logger.info("starting init cluster")
        cluster.start(wait_for_binary_proto=True, wait_other_notice=True)

        def add_and_start_a_node(node_idx):
            """add a new node to cluster, and start it"""
            logger.info("adding node%d", node_idx)
            node = cluster.new_node(node_idx)
            logger.info("starting node%d", node_idx)
            node.start(wait_other_notice=True)
            return node

        add_and_start_a_node(3)

        logger.info("stopping node1")
        node1.stop(wait_other_notice=True, gently=True)

        self.ignore_log_patterns += ['Startup failed']
        # can't add a new node to cluster if a node stop
        with pytest.raises(RuntimeError, match='The process is dead'):
            node4 = cluster.new_node(4)
            node4.start(wait_other_notice=True)

        logger.info("starting node1 again")
        node1.start(wait_other_notice=True)

        ip4 = get_ip_from_node(node=node4)
        node2.watch_log_for(f"{ip4} gossip quarantine over")

        logger.debug("starting node4")
        node4.start(wait_other_notice=True)
        add_and_start_a_node(5)
        logger.info('removing node1 and node2, `node3, node4, node5` will be on duty')
        node1.decommission()
        node2.decommission()

        add_and_start_a_node(6)
        logger.info('removing node3, `node4, node5, node6` will be on duty')
        node4.decommission()
        add_and_start_a_node(7)

    @staticmethod
    def _cleanup(node):
        commitlog_dir = os.path.join(node.get_path(), 'commitlogs')
        data_dir = os.path.join(node.get_path(), 'data')
        logger.debug("Deleting {}".format(data_dir))
        node.rmtree(data_dir)
        node.rmtree(commitlog_dir)
