import os
import random
import re
import shutil
import subprocess
import tempfile
import time
from concurrent.futures.thread import ThreadPoolExecutor
from psutil import Process

from tools import require
from assertions import assert_almost_equal, assert_one
from cassandra import ConsistencyLevel
from cassandra.concurrent import execute_concurrent_with_args
from ccmlib.node import NodeError
from dtest import Tester, debug
from unittest import skip
from tools import (InterruptBootstrap, KillOnBootstrap, new_node, query_c1c2,
                   since, create_c1c2_table, insert_c1c2)
from scylla_tools import scylla_mode
from nose.plugins.attrib import attr


@attr('dtest-full')
class TestBootstrap(Tester):

    def __init__(self, *args, **kwargs):
        kwargs['cluster_options'] = {'start_rpc': 'true'}
        # Ignore these log patterns:
        self.ignore_log_patterns = [
            # This one occurs when trying to send the migration to a
            # node that hasn't started yet, and when it does, it gets
            # replayed and everything is fine.
            r'Can\'t send migration request: node.*is down',
            # ignore streaming error during bootstrap
            r'Exception encountered during startup',
            r'Streaming error occurred'
        ]
        Tester.__init__(self, *args, **kwargs)

    def get_space_used(self, node, table_name='cf'):
        output = node.nodetool('cfstats', True)[0]
        if output.find(table_name) != -1:
            output = output[output.find(table_name):]
            output = output[output.find("Space used (total)"):]
            initial_value = int(output[output.find(":") + 1:output.find("\n")].strip())
            return initial_value
        return -1

    @attr('next-gating', 'dtest-debug', 'dtest-smoke', 'single_node')
    def start_stop_test(self):
        debug("populating cluster with one node")
        cluster = self.cluster
        cluster.populate(1)
        debug("starting cluster")
        cluster.start(wait_for_binary_proto=True, wait_other_notice=True)
        debug("stopping cluster")
        cluster.stop()
        debug("done")

    @attr('next-gating')
    @attr('dtest-debug')
    @attr('dtest-smoke')
    def start_stop_test_node(self):
        debug("populating cluster with three nodes")
        cluster = self.cluster
        cluster.populate(3)
        debug("starting cluster")
        cluster.start(wait_for_binary_proto=True, wait_other_notice=True)
        debug("stopping node")
        node1 = cluster.nodelist()[0]
        node1.stop(wait_other_notice=True, wait_seconds=10)
        debug("stopping cluster")
        cluster.stop()
        debug("done")

    @attr('next-gating')
    @attr('dtest-debug')
    def add_node_test(self):
        debug("populating cluster with three nodes")
        cluster = self.cluster
        cluster.populate(2)
        debug("starting cluster")
        cluster.start(wait_other_notice=True)
        debug("adding node3")
        node3 = cluster.new_node(3)
        debug("starting node3")
        node3.start(wait_other_notice=True)
        debug("stopping cluster")
        cluster.stop()
        debug("done")

    @attr('next-gating')
    @attr('dtest-debug')
    def add_detached_node_test(self):
        debug("populating cluster with three nodes")
        cluster = self.cluster
        cluster.populate(2)
        debug("starting cluster")
        cluster.start(wait_other_notice=True)
        debug("adding node3")
        node3 = cluster.new_node(3, add_node=False)

        def stop_node3():
            debug("stopping node3")
            node3.stop()

        self.addCleanup(stop_node3)

        debug("starting node3")
        node3.start(wait_other_notice=True)
        debug("stopping cluster")
        cluster.stop()
        debug("done")

    def simple_bootstrap_test(self):
        cluster = self.cluster
        tokens = cluster.balanced_tokens(2)
        cluster.set_configuration_options(values={'num_tokens': 1})

        debug("[node1, node2] tokens: %r" % (tokens,))

        keys = 10000

        # Create a single node cluster
        cluster.populate(1)
        node1 = cluster.nodelist()[0]
        node1.set_configuration_options(values={'initial_token': tokens[0]})
        cluster.start(wait_other_notice=True)

        session = self.patient_cql_connection(node1)
        self.create_ks(session, 'ks', 1)
        self.create_cf(session, 'cf', columns={'c1': 'text', 'c2': 'text'})


        insert_statement = session.prepare("INSERT INTO ks.cf (key, c1, c2) VALUES (?, 'value1', 'value2')")
        execute_concurrent_with_args(session, insert_statement, [['k%d' % k] for k in range(keys)])

        node1.flush()
        node1.compact()

        data_total_size_node1 = self.get_space_used(node1)
        debug("before={}".format(data_total_size_node1))

        # Reads inserted data all during the bootstrap process. We shouldn't
        # get any error
        reader = self.go(lambda _: query_c1c2(session, random.randint(0, keys - 1), ConsistencyLevel.ONE))

        # Bootstraping a new node
        node2 = new_node(cluster)
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

        debug("before={}, after={} + {}={}".format(data_total_size_node1, data_total_size_node1_after, data_total_size_node2_after, data_total_size_node1_after+data_total_size_node2_after));
        assert_almost_equal(data_total_size_node1, data_total_size_node1_after + data_total_size_node2_after, error=0.3)
        assert_almost_equal(data_total_size_node1_after, data_total_size_node2_after, error=0.3)

    def schema_is_pulled_before_schema_is_declared_complete_test(self):
        """Test that bootstrapping node does a schema pull, before claiming to have a complete schema."""

        cluster = self.cluster
        cluster.populate(1)
        cluster.start(wait_for_binary_proto=True)

        node1 = cluster.nodelist()[0]

        node2 = new_node(cluster)
        node2.start(wait_for_binary_proto=True)

        messages = [
            "Pulling schema from {}(:[0-9]+)?".format(node1.address()),
            "Schema merge with {}(:[0-9]+)? completed".format(node1.address()),
            "JOINING: schema complete, ready to bootstrap",
        ]

        matches = node2.watch_log_for(messages)

        # watch_log_for() should already ensure this, but just to be sure...
        self.assertEquals(len(messages), len(matches))

        # Make sure the order of the matching log lines is exactly that of in `messages`.
        for msg_re, match in zip(messages, matches):
            log_line, match_obj = match
            self.assertTrue(re.search(msg_re, log_line) is not None)

    def read_from_bootstrapped_node_test(self):
        """Test bootstrapped node sees existing data, eg. CASSANDRA-6648"""
        cluster = self.cluster
        cluster.populate(3)
        cluster.start()

        node1 = cluster.nodes['node1']
        node1.stress(['write', 'n=10000', '-rate', 'threads=8'])

        session = self.patient_cql_connection(node1)
        stress_table = 'keyspace1.standard1'
        original_rows = list(session.execute("SELECT * FROM %s" % (stress_table,)))

        node4 = new_node(cluster)
        node4.start(wait_for_binary_proto=True)

        session = self.patient_exclusive_cql_connection(node4)
        new_rows = list(session.execute("SELECT * FROM %s" % (stress_table,)))
        self.assertEquals(original_rows, new_rows)

    @since('2.2')
    @skip('failing on code: "node3.watch_log_for("Starting listening for CQL clients")"')
    def resumable_bootstrap_test(self):
        """Test resuming bootstrap after data streaming failure"""

        cluster = self.cluster
        cluster.populate(2).start(wait_other_notice=True)

        node1 = cluster.nodes['node1']
        node1.stress(['write', 'n=100000', '-schema', 'replication(factor=2)'])
        node1.flush()

        # kill node1 in the middle of streaming to let it fail
        t = InterruptBootstrap(node1)
        t.start()

        # start bootstrapping node3 and wait for streaming
        node3 = new_node(cluster)
        node3.set_configuration_options(values={'stream_throughput_outbound_megabits_per_sec': 1})
        # keep timeout low so that test won't hang
        node3.set_configuration_options(values={'streaming_socket_timeout_in_ms': 1000})
        try:
            node3.start()
        except NodeError:
            pass  # node doesn't start as expected
        t.join()

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

    @skip('Scylla does not support the cassandra.reset_bootstrap_progress option.')
    def bootstrap_with_reset_bootstrap_state_test(self):
        """Test bootstrap with resetting bootstrap progress"""

        cluster = self.cluster
        cluster.set_configuration_options(values={'stream_throughput_outbound_megabits_per_sec': 1})
        cluster.populate(2).start(wait_other_notice=True)

        node1 = cluster.nodes['node1']
        node1.stress(['write', 'n=100000', '-schema', 'replication(factor=2)'])
        node1.flush()

        # kill node1 in the middle of streaming to let it fail
        t = InterruptBootstrap(node1)
        t.start()

        # start bootstrapping node3 and wait for streaming
        node3 = new_node(cluster)
        try:
            node3.start()
        except NodeError:
            pass  # node doesn't start as expected
        t.join()
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

    @attr('next-gating')
    @attr('dtest-debug')
    def manual_bootstrap_test(self):
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
        node3 = new_node(cluster, bootstrap=False)
        node3.start(wait_for_binary_proto=True)
        node3.repair()
        node1.cleanup()

        current_rows = list(session.execute("SELECT * FROM %s" % stress_table))
        self.assertEquals(original_rows, current_rows)

    @scylla_mode('!debug')
    def local_quorum_bootstrap_test(self):
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

        node3 = new_node(cluster, data_center='dc2')
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

        debug(output)
        regex = re.compile("Operation.+error inserting key.+Exception")
        failure = regex.search(output)
        self.assertIsNone(failure, "Error during stress while bootstrapping")

    def shutdown_wiped_node_cannot_join_test(self):
        self._wiped_node_cannot_join_test(gently=True)

    def killed_wiped_node_cannot_join_test(self):
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
        node2 = new_node(cluster, bootstrap=True)
        node2.start(wait_for_binary_proto=True)

        session = self.patient_cql_connection(node2)
        self.assertEquals(original_rows, list(session.execute("SELECT * FROM {}".format(stress_table,))))

        # Stop the new node and wipe its data
        node2.stop(gently=gently)
        data_dir = os.path.join(node2.get_path(), 'data')
        commitlog_dir = os.path.join(node2.get_path(), 'commitlogs')
        debug("Deleting {}".format(data_dir))
        shutil.rmtree(data_dir)
        shutil.rmtree(commitlog_dir)

        # Now start it, it should not be allowed to join.
        expected_error = "A node with address {} already exists, cancelling join".format(self.cluster.get_node_ip(4))
        self.ignore_log_patterns += [expected_error]
        mark = node2.mark_log()
        try:
            node2.start(no_wait=True)
        except NodeError:
            # It is expected that the node will not boot
            pass
        node2.watch_log_for(expected_error, from_mark=mark)

    def decommissioned_wiped_node_can_join_test(self):
        """
        @jira_ticket CASSANDRA-9765
        Test that if we decommission a node and then wipe its data, it can join the cluster.
        """
        cluster = self.cluster
        cluster.populate(3)
        cluster.start(wait_for_binary_proto=True)

        stress_table = 'keyspace1.standard1'

        # write some data
        node1 = cluster.nodelist()[0]
        node1.stress(['write', 'n=10K', '-rate', 'threads=8'])

        session = self.patient_cql_connection(node1)
        original_rows = list(session.execute("SELECT * FROM {}".format(stress_table,)))

        # Add a new node, bootstrap=True ensures that it is not a seed
        debug("Starting node4")
        node4 = new_node(cluster, bootstrap=True)
        node4.start(wait_for_binary_proto=True, wait_other_notice=True)

        session = self.patient_cql_connection(node4)
        self.assertEquals(original_rows, list(session.execute("SELECT * FROM {}".format(stress_table,))))

        # Decommision the new node and wipe its data
        debug("Decommissioning node4")
        node4.decommission()
        debug("Stopping node4")
        node4.stop(wait_other_notice=True)
        data_dir = os.path.join(node4.get_path(), 'data')
        commitlog_dir = os.path.join(node4.get_path(), 'commitlogs')
        debug("Deleting {}".format(data_dir))
        shutil.rmtree(data_dir)
        shutil.rmtree(commitlog_dir)

        # Now start it, it should be allowed to join
        debug("Restarting node4")
        mark = node4.mark_log()
        node4.start(wait_other_notice=True)
        debug("Waiting for node4 to join")
        node4.watch_log_for("JOINING:", from_mark=mark)

    def failed_bootstap_wiped_node_can_join_test(self):
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
        original_rows = list(session.execute("SELECT * FROM {}".format(stress_table,)))

        # Add a new node, bootstrap=True ensures that it is not a seed
        node2 = new_node(cluster, bootstrap=True)
        node2.set_configuration_options(values={'stream_throughput_outbound_megabits_per_sec': 1})

        # kill node2 in the middle of bootstrap
        t = KillOnBootstrap(node2)
        t.start()

        mark = node1.mark_log()
        debug("Starting node2")
        node2.start(wait_for_binary_proto=False, wait_other_notice=False)
        t.join()
        self.assertFalse(node2.is_running())
        debug("node2 killed during bootstrap. Waiting for other nodes to notice...")
        node1.watch_log_for("{} has been silent .* removing from gossip".format(node2.address()), from_mark=mark)

        # wipe any data for node2
        data_dir = os.path.join(node2.get_path(), 'data')
        commitlog_dir = os.path.join(node2.get_path(), 'commitlogs')
        debug("Deleting {}".format(data_dir))
        shutil.rmtree(data_dir)
        shutil.rmtree(commitlog_dir)

        # Now start it again, it should be allowed to join
        mark = node2.mark_log()
        debug("Restarting node2")
        node2.start(wait_other_notice=True)
        node2.watch_log_for("JOINING:", from_mark=mark)

    @since('2.1.1')
    @skip('Failing on code: "stdout, stderr = process.communicate()"')
    def simultaneous_bootstrap_test(self):
        """
        Attempt to bootstrap two nodes at once, to assert the second bootstrapped node fails, and does not interfere.

        Start a one node cluster and run a stress write workload.
        Start up a second node, and wait for the first node to detect it has joined the cluster.
        While the second node is bootstrapping, start a third node. This should fail.

        @jira_ticket CASSANDRA-7069
        @jira_ticket CASSANDRA-9484
        """

        bootstrap_error = ("Other bootstrapping/leaving/moving nodes detected,"
                           " cannot bootstrap while cassandra.consistent.rangemovement is true")

        self.ignore_log_patterns.append(bootstrap_error)

        cluster = self.cluster
        cluster.populate(1)
        cluster.start(wait_for_binary_proto=True)

        node1, = cluster.nodelist()

        node1.stress(['write', 'n=500K', '-schema', 'replication(factor=1)',
                      '-rate', 'threads=10'])

        node2 = new_node(cluster)
        node2.start(wait_other_notice=True)

        node3 = new_node(cluster, remote_debug_port='2003')
        process = node3.start()
        stdout, stderr = process.communicate()
        self.assertIn(bootstrap_error, stderr, msg=stderr)
        time.sleep(.5)
        self.assertFalse(node3.is_running(), msg="Two nodes bootstrapped simultaneously")

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

        debug("Preparing a KS and a CF...")
        self.create_ks(session, name='ks', rf=rf)
        create_c1c2_table(self, session)

        debug("Populating the data...")
        insert_c1c2(session, n=10000, consistency=ConsistencyLevel.QUORUM)
        # This flush will only be needed on d-test otherwise the test will fail (no data will be written)
        cluster.flush()

        debug("Saving nodes process list")
        pid_ls = [node.all_pids[0] for node in cluster.nodelist()]
        process_ls = []
        for pid in pid_ls:
            process = Process(pid)
            process_ls.append(process)

        debug("Killing all nodes")
        cluster.stop_nodes(gently=gently, wait_seconds=20)

        debug("Making sure all node processes are down")
        for process in process_ls:
            self.assertEqual(False, process.is_running(), "Node with the following pid {} didn't stop/exit correctly"
                             .format(process.pid))

        debug("Starting all nodes")
        cluster.start_nodes(no_wait=False)

        debug("Checking that no data was lost")
        for n in range(10000):
            query_c1c2(session, n, ConsistencyLevel.QUORUM)

    def test_full_cluster_recovery_after_forcibly_stop_3_nodes_rf_3(self):
        self._full_cluster_recovery_after_stop(gently=False, num_of_nodes=3, rf=3)

    def test_full_cluster_recovery_after_gentle_stop_5_nodes_rf_2(self):
        self._full_cluster_recovery_after_stop(gently=True, num_of_nodes=5, rf=2)

    def test_full_cluster_recovery_after_forcibly_stop_4_nodes_rf_1(self):
        self._full_cluster_recovery_after_stop(gently=False, num_of_nodes=4, rf=1)

    def _cluster_become_unavailable_when_kill_node_during_bootstrap(self, is_gracefully=True):
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
        bootstrap_msg = "JOINING: Starting to bootstrap"
        kill_node_err_msg = "The process is dead, returncode={}"
        ks_name, consistency_level_key = "keyspace", "TWO"
        beginning_stream_session_msg = f"Beginning stream session|sync data for keyspace={ks_name}, status=started"
        removing_from_gossip_msg = r"FatClient {} has been silent for (\d+)ms, removing from gossip"
        stress_duration_minutes = 3
        replication_factor, consistency_level = 2, 2
        cluster_size = 2
        cassandra_err_msg = f"com.datastax.driver.core.exceptions.WriteTimeoutException: Cassandra timeout during" \
                            f" SIMPLE write query at consistency {consistency_level_key} ({replication_factor + 1}" \
                            f" replica were required but only {replication_factor} acknowledged the write)"

        cluster = self.cluster
        debug(f"Creating new cluster with '{cluster_size}' nodes")
        cluster.populate(nodes=cluster_size).start(wait_for_binary_proto=True, wait_other_notice=True)
        node1, node2 = cluster.nodelist()

        write_stress_cmd = ["write", f"cl={consistency_level_key}", f"duration={stress_duration_minutes}m",
                            "-rate", "threads=10", "-log", "interval=5", "-schema",
                            f"replication(factor={replication_factor}) keyspace={ks_name}"]

        debug(f"Executing the following write stress command '{write_stress_cmd}'")
        stress_thread = executor.submit(lambda: node1.stress(stress_options=write_stress_cmd, capture_output=True))

        debug("Adding new node")
        node3 = cluster.new_node(i=cluster_size + 1, debug=True, auto_bootstrap=True, is_seed=False)
        start_new_node_thread = executor.submit(lambda: node3.start(
            wait_for_binary_proto=True, jvm_args=['--logger-log-level', 'stream_session=debug'], no_wait=True))
        mark_log = node3.mark_log()

        debug(f"Trying to find the following '{bootstrap_msg}' message in logs of node '{node3.name}'")
        node3.watch_log_for(exprs=bootstrap_msg, from_mark=mark_log)
        debug(f"Trying to find the following '{beginning_stream_session_msg}' message in logs of node '{node3.name}'")
        node3.watch_log_for(exprs=beginning_stream_session_msg, from_mark=mark_log)

        nodes = [node1, node2]
        mark_log_list = [node.mark_log() for node in nodes]
        debug(f"{'Gracefully' if is_gracefully else 'Force'} killing node'{node3.name}' (PID is '{node3.pid}')")
        node3.stop(wait=True, gently=is_gracefully)
        removing_from_gossip_msg = removing_from_gossip_msg.format(self.get_ip_from_node(node=node3))
        for node, mark_log in zip(nodes, mark_log_list):
            debug(f"Checking the following message '{removing_from_gossip_msg}' exits in node '{node.name}'")
            node.watch_log_for(exprs=removing_from_gossip_msg, from_mark=mark_log)

        self.assertEquals(first=kill_node_err_msg.format(1 if is_gracefully else -9),
                          second=str(start_new_node_thread.exception()),
                          msg=f"The node '{node3.name}' should be killed by SIGKILL signal")
        debug("Waiting until stress thread will finish running")
        stdout, stderr = stress_thread.result()
        if stderr:
            debug(f"The output from stdout is:\n{stdout}")
            debug(f"The following errors occurred during the run:\n{stderr}")
            self.assertNotIn(member=cassandra_err_msg, container=stderr,
                             msg=f"The following message '{cassandra_err_msg}' found in stderr")

    @require("#4488")
    def cluster_become_unavailable_when_force_kill_node_during_bootstrap_test(self):
        self._cluster_become_unavailable_when_kill_node_during_bootstrap(is_gracefully=False)

    @require("#4488")
    def cluster_become_unavailable_when_gracefully_kill_node_during_bootstrap_test(self):
        self._cluster_become_unavailable_when_kill_node_during_bootstrap(is_gracefully=True)
