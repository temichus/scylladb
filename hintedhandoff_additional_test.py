from unittest import skip
from cassandra import ConsistencyLevel

from dtest import Tester, debug, wait_for
import glob
from tools import create_c1c2_table, insert_c1c2, query_c1c2, delete_c1c2, new_node
import time
from nose.plugins.attrib import attr
from ccmlib.scylla_cluster import ScyllaCluster


@attr('dtest-full')
class TestHintedHandoff(Tester):
    def hintedhandoff_rebalance_test(self):
        """
        Test that hints segments rebalancing code works.

        Things to verify:
           - Hints segments are evenly rebalanced.
           - Hints are properly sent and accepted on the destination site after rebalancing.

        Test will:
           - Create a cluster of 3 nodes with SMP=1.
           - Stop node3.
           - Populate the data using the cassandra-stress tool: RF=3.
           - Restart the nodes of the cluster with SMP=3 (setting the "hintable DC" to non-existing DC in order to
             prevent hints from being sent) and check that hints segments are balanced among shard.
           - Repeat the previous step for SMP=2.
           - Restart the nodes and don't prevent hints sending this time.
           - Wait till all hints are sent.
           - Stop nodes node1 and node2.
           - Verify that all data may be read from node3.

        """
        self.__start_cluster_with_hints(num=3, custom_args=['--smp', '1'])

        [node1, node2, node3] = self.cluster.nodelist()

        debug("Stopping node3...")
        node3.stop(wait_other_notice=True)

        debug("Populating the data...")
        op_cnt = 1000000
        if isinstance(self.cluster, ScyllaCluster) and self.cluster.scylla_mode == 'debug':
            op_cnt = 10000
        stress_cmd = ['write', 'n={}'.format(op_cnt), 'no-warmup', 'cl=QUORUM',
                      '-rate', 'threads=300', '-schema', 'replication(factor=3)']
        resp = node1.stress_object(stress_cmd, ignore_errors=True)

        if not resp or 'total partitions:write' not in resp:
            raise Exception('Error running stress test: {}'.format(resp))

        assert resp['total partitions:write'] == op_cnt

        debug("Check SMP=3")
        self.__stop_all([node1, node2])
        self.__start_all([node1, node2, node3], hh_enabled_value='dont_send_hints', extra_jvm_args=['--smp', '3'])
        self.__check_rebalanced_dirs([node1, node2], node3, 3)

        debug("Check SMP=2")
        self.__stop_all([node2, node3, node1])
        self.__start_all([node1, node2, node3], hh_enabled_value='dont_send_hints', extra_jvm_args=['--smp', '2'])
        self.__check_rebalanced_dirs([node1, node2], node3, 2)

        debug("Check that shard 2 directories are gone")
        assert self.__check_hints_dir_present(node_from=node1, node_to=node3, must_be_present=False, shard=2) and \
            self.__check_hints_dir_present(node_from=node2, node_to=node3, must_be_present=False, shard=2)

        debug("Check data consistency")
        self.__stop_all([node2, node3, node1])
        self.__start_all([node1, node2, node3], extra_jvm_args=['--smp', '2'])

        # Wait fill all hints are sent
        for node in [node1, node2]:
            for shard in range(0, 2):
                while self.__get_hint_segs_count(node, node3, shard) > 1:
                    debug("Still sending hints")
                    time.sleep(1)

        self.__stop_all([node2, node1])
        debug("Reading data")
        stress_cmd = ['read', 'n={}'.format(op_cnt), 'no-warmup', 'cl=ONE', '-rate',
                      'threads=300', '-schema', 'replication(factor=3)']
        resp = node3.stress_object(stress_cmd, ignore_errors=True)
        if not resp or 'total partitions:read' not in resp:
            raise Exception('Error running stress test: {}'.format(resp))

        assert resp['total partitions:read'] == op_cnt

    @attr('dtest-debug')
    def hintedhandoff_removenode_test(self):
        """
        Test hints draining when node is removed (nodetool removenode) from the cluster.

        Create a 3 nodes cluster with hinted handoff enabled
        Create a KS wit RF=1
        Create a table
        Shut down node3
        Insert 100 rows with CL=ANY. The data for node3 will only be written to hints.
        Remove node3 from the cluster: nodetool removenode <node3 host ID>
        Wait till hints are flushed
        Read all 100 rows with CL=ONE - it should succeed.

        If hints don't work the rows that were intended for node3 will be missing.
        """
        self.__start_cluster_with_hints(num=3)

        [node1, node2, node3] = self.cluster.nodelist()
        session = self.patient_cql_connection(node1)

        debug("Preparing a KS and a CF...")
        self.create_ks(session, 'ks', 1)
        create_c1c2_table(self, session)

        node3_addr = node3.address()
        node3_hid = node3.hostid()

        debug("Stopping node3...")
        node3.stop(wait_other_notice=True)

        debug("Populating the data...")
        insert_c1c2(session, n=100, consistency=ConsistencyLevel.ANY)

        marks = []
        for node in [node1, node2]:
            marks.append((node, node.mark_log()))

        debug("Removing node3...")
        node1.removenode(node3_hid)

        timeout = self.__hint_flush_threshold * 2
        wait_until = time.time() + timeout
        debug("Waiting {}s for hints to be sent...".format(timeout))

        for (node, from_mark) in marks:
            # We wait twice on each shard because there are two hints managers: one for regular writes and one for views
            msgs = ['hints_manager - drain_for: finished draining {}'.format(node3_addr)] * (2 * node._smp)

            node_timeout = max(wait_until - time.time(), 1)
            node.watch_log_for(msgs, from_mark=from_mark, timeout=node_timeout)

        debug("Reading the data...")
        for x in range(0, 100):
            query_c1c2(session, x, ConsistencyLevel.ONE)

        debug("Check that the directories have been cleaned up...")
        assert self.__check_hints_dir_present(node_from=node1, node_to=node3, must_be_present=False) and \
            self.__check_hints_dir_present(node_from=node2, node_to=node3, must_be_present=False)

    @attr('next-gating')
    @attr('dtest-debug')
    def hintedhandoff_basic_check_test(self):
        """
        A basic test:
        Create a 3 nodes cluster with hinted handoff enabled
        Create a KS wit RF=3
        Create a table
        Shut down node1
        Write a single row
        Start node1
        Wait till the hint is flushed
        Stop node2 and node3
        Read the row with CL=ONE - it should succeed.

        If hint is not written node1 is not going to have the row.
        """
        self.__start_cluster_with_hints(num=3)

        [node1, node2, node3] = self.cluster.nodelist()
        session = self.patient_cql_connection(node1)
        debug("Creating a keyspace...")
        self.create_ks(session, 'ks', 3)

        debug("Creating a table..")
        create_c1c2_table(self, session)

        debug("Stopping node1...")
        node1.stop(wait_other_notice=True)

        debug("Inserting a key...")
        insert_c1c2(session, n=1, consistency=ConsistencyLevel.ONE)

        # Wait for the write to timeout if needed
        time.sleep(5)

        assert self.__check_hints_dir_present(node_from=node3, node_to=node1) or \
            self.__check_hints_dir_present(node_from=node2, node_to=node1)

        debug("Starting node1...")
        node1.start(wait_for_binary_proto=True, jvm_args=self.__jvm_args())

        time.sleep(self.__hint_flush_threshold)

        debug("Stopping node2 and node3...")
        node2.stop(wait_other_notice=True)
        node3.stop(wait_other_notice=True)

        debug("Checking the key...")
        query_c1c2(session, 0, ConsistencyLevel.ONE)

    def hintedhandoff_counter_test(self):
        """
        Tests that counter updates are sent correctly as hints.

        Create a 2 node cluster with hinted handoff enabled
        Create a KS with RF=2
        Create a table with counters
        Shut down node2
        Insert 100 rows with CL=ONE. Counters need at least consistency ONE, we can't use ANY.
        Restart node1 with hinted handoff disabled
        Bring the node2 up
        Add node3 to the cluster
        Restart node1 with hinted handoff enabled
        Wait till hints are sent out from node1
        Stop node1
        Read all 100 rows with CL=ONE - it should succeed and have values that were written by us.
        """
        self.__start_cluster_with_hints(num=2)

        [node1, node2] = self.cluster.nodelist()
        session = self.patient_cql_connection(node1)

        debug("Preparing a KS and a CF...")
        self.create_ks(session, 'ks', 2)
        session.execute("CREATE TABLE ks.tbl (pk int PRIMARY KEY, c counter) " +
                        "WITH speculative_retry = 'NONE' " +
                        "AND read_repair_chance = 0 " +
                        "AND dclocal_read_repair_chance = 0")

        debug("Stopping node2...")
        node2.stop(wait_other_notice=True)

        debug("Populating the data...")
        stmt = session.prepare("UPDATE ks.tbl SET c = c + ? WHERE pk = ?")
        stmt.consistency_level = ConsistencyLevel.ONE

        expected_values = [100 * i for i in range(100)]
        for i, v in enumerate(expected_values):
            session.execute(stmt.bind((v, i)))

        debug("Restarting node1 with hinted handoff disabled...")
        node1.stop()
        node1.start(wait_for_binary_proto=True, jvm_args=self.__jvm_args(hh_enabled_value='false'))

        debug("Starting node2...")
        node2.start(wait_for_binary_proto=True, jvm_args=self.__jvm_args())

        debug("Adding node3...")
        node3 = new_node(self.cluster, bootstrap=True)
        node3.start(wait_for_binary_proto=True, jvm_args=self.__jvm_args())

        debug("Restarting node1 with hinted handoff enabled...")
        node1.stop()
        node1.start(wait_for_binary_proto=True, jvm_args=self.__jvm_args(hh_enabled_value='true'))

        debug("Waiting for hints to be sent...".format(self.__hint_flush_threshold))
        self.__wait_until_hints_are_sent_from(node_from=node1, count=len(expected_values))

        # We want to check that hints for counters are sent correctly.
        # At this point, there should be 100 hints sent from node1 and node2.
        # There was a topology change between hints storing and sending, so
        # node2 might no longer be a replica for some of them (those hints are
        # orphaned in a sense). Non-orphaned and orphaned hints use a slightly
        # different code path for sending and we want to test them both.
        #
        # We can check if values written by both paths have sensible values
        # by stopping node1 and reading with CL=ONE.
        # - Rows with nodes 1 & 2 as replicas were written to node2
        #   by non-orphaned hints. We will read those rows from node2 only,
        #   because node1 is down.
        # - Rows with nodes 1 & 3 as replicas were written to node3
        #   by orphaned hints. If they were sent incorrectly, they
        #   will overwrite what node3 previously had. We will read
        #   those rows from node3 only, because node1 is down.
        # - Rows with nodes 2 & 3 as replicas will be read from either node2
        #   or node3. They should have correct value, but it won't be obvious
        #   from which node the value came if it is incorrect.
        debug("Stopping node1...")
        node1.stop(wait_other_notice=True)

        debug("Reading the data from nodes 2 and 3...")
        session = self.patient_cql_connection(node3)
        stmt = session.prepare("SELECT c FROM ks.tbl WHERE pk = ?")
        stmt.consistency_level = ConsistencyLevel.ONE

        # Collect all rows into a list and then compare with data that we expect
        actual_values = []
        for i, v in enumerate(expected_values):
            row = list(session.execute(stmt.bind((i,))))[0]
            if row.c != v:
                debug("pk={}; actual c={}, expected c={}".format(i, row.c, v))
            actual_values.append(row.c)

        self.assertListEqual(actual_values, expected_values)

    def hintedhandoff_decom_test(self):
        """
        Test hints draining when node is decommissioned (nodetool decommission).

        Create a 3 nodes cluster with hinted handoff enabled
        Create a KS wit RF=1
        Create a table
        Shut down node3
        Insert 100 rows with CL=ANY. The data for node3 will only be written to hints.
        Bring the node3 up
        Decommission node3
        Wait till hints are flushed
        Read all 100 rows with CL=ONE - it should succeed.
        """
        self.__start_cluster_with_hints(num=3)

        [node1, node2, node3] = self.cluster.nodelist()
        session = self.patient_cql_connection(node1)

        debug("Preparing a KS and a CF...")
        self.create_ks(session, 'ks', 1)
        create_c1c2_table(self, session)

        debug("Stopping node3...")
        node3.stop(wait_other_notice=True)

        debug("Populating the data...")
        insert_c1c2(session, n=100, consistency=ConsistencyLevel.ANY)

        debug("Staring node3...")
        node3.start(wait_for_binary_proto=True, jvm_args=self.__jvm_args())

        debug("Decommissioning node3...")
        node3.decommission()

        debug("Waiting {}s for hints to be sent...".format(self.__hint_flush_threshold))
        time.sleep(self.__hint_flush_threshold)

        debug("Check that the directories have been cleaned up...")
        assert self.__check_hints_dir_present(node_from=node1, node_to=node3, must_be_present=False) and \
            self.__check_hints_dir_present(node_from=node2, node_to=node3, must_be_present=False)

        debug("Reading the data...")
        for x in range(0, 100):
            query_c1c2(session, x, ConsistencyLevel.ONE)

    def hintedhandoff_dont_revive_test(self):
        """
        Test that hints don't revive the data.

        Create a cluster of 3 nodes
        Create a KS wit RF=3
        Create a table
        Stop node1
        Insert a single row with CL=TWO - this should create a hint towards node1
        Stop the node that has that hint
        Start node1
        Delete the row created above
        Start the node with the hint
        Wait till the hint is flushed
        Read the row with CL=ALL - the row should be missing
        """
        self.__start_cluster_with_hints(num=3)

        [node1, node2, node3] = self.cluster.nodelist()
        session = self.patient_cql_connection(node1)
        debug("Creating a keyspace...")
        self.create_ks(session, 'ks', 3)

        debug("Creating a table..")
        create_c1c2_table(self, session)

        debug("Stopping node1...")
        node1.stop(wait_other_notice=True)

        debug("Inserting a key...")
        insert_c1c2(session, n=1, consistency=ConsistencyLevel.TWO)

        # Wait for the write to timeout if needed
        time.sleep(5)

        assert self.__check_hints_dir_present(node_from=node3, node_to=node1) or \
            self.__check_hints_dir_present(node_from=node2, node_to=node1)

        debug("Stopping the node that has the hint...")
        hinting_node = None
        if self.__check_hints_dir_present(node_from=node3, node_to=node1):
            node3.stop(wait_other_notice=True)
            hinting_node = node3
        else:
            node2.stop(wait_other_notice=True)
            hinting_node = node2

        debug("Stopped {}".format(hinting_node.name))

        debug("Starting node1...")
        node1.start(wait_for_binary_proto=True, jvm_args=self.__jvm_args())

        debug("Deleting the row...")
        delete_c1c2(session, n=1, consistency=ConsistencyLevel.TWO)

        debug("Starting {}".format(hinting_node.name))
        hinting_node.start(wait_for_binary_proto=True, jvm_args=self.__jvm_args(hinting_node))

        debug("Waiting {}s for hints to be sent...".format(self.__hint_flush_threshold))
        time.sleep(self.__hint_flush_threshold)

        debug("Checking the key (must be missing)...")
        debug("Checking on node1...")
        node3.stop(wait_other_notice=True)
        node2.stop(wait_other_notice=True)
        query_c1c2(session, 0, ConsistencyLevel.ONE, must_be_missing=True)

        debug("Checking on node2...")
        node2.start(wait_for_binary_proto=True, jvm_args=self.__jvm_args())
        node1.stop(wait_other_notice=True)
        session = self.patient_cql_connection(node2)
        session.execute('USE ks')
        query_c1c2(session, 0, ConsistencyLevel.ONE, must_be_missing=True)

        debug("Checking on node3...")
        node3.start(wait_for_binary_proto=True, jvm_args=self.__jvm_args())
        node2.stop(wait_other_notice=True)
        session = self.patient_cql_connection(node3)
        session.execute('USE ks')
        query_c1c2(session, 0, ConsistencyLevel.ONE, must_be_missing=True)

    def hintedhandoff_retransmit_test(self):
        """
        Test sending consistency. There should be no discarded hints.
        Validates the fix of scylladb/scylla#4122.
        """
        self.__start_cluster_with_hints(num=3)

        node1, node2, node3 = self.cluster.nodelist()

        debug("Stopping node2...")
        node2.stop(wait_other_notice=True)

        # Make node2 slower than others in order to trigger hints generation
        debug("Starting node2 with \"trace\" log level...")
        node2.start(wait_for_binary_proto=True, jvm_args=self.__jvm_args() +
                ["--logger-log-level", "hints_manager=trace"])

        debug("starting a stress...")
        stress_cmd = ['write', 'duration=4m', 'no-warmup', 'cl=ONE',
                      '-rate', 'threads=300', '-schema', 'replication(factor=3)']
        node1.stress_object(stress_cmd, ignore_errors=True)
        debug("stress finished")

        for node in [node1, node2, node3]:
            debug("checking {}".format(node.name))
            res = self.get_node_metrics(self.get_ip_from_node(node), metrics=["scylla_hints_manager_discarded"])
            debug("checking that scylla_hints_manager_discarded is present")
            assert "scylla_hints_manager_discarded" in res
            debug("checking that scylla_hints_manager_discarded is zero")
            if res["scylla_hints_manager_discarded"] != 0:
                debug("{}: scylla_hints_manager_discarded = {}".format(
                    node.name, res["scylla_hints_manager_discarded"]))
                self.assertEqual(res["scylla_hints_manager_discarded"], 0, "There were discarded hints")


########################################################################################################################


    @property
    def __hint_flush_threshold(self):
        """
        Hints are flushed every 10s, so wait for 15s to make sure the hints are sent.
        """
        return 15

    def __jvm_args(self, hh_enabled_value=None):
        hh_enabled = 'true'
        if not hh_enabled_value is None:
            hh_enabled = hh_enabled_value

        return ['--hinted-handoff-enabled', hh_enabled, '--logger-log-level', 'hints_manager=trace']

    def __start_cluster_with_hints(self, num, custom_args=[], hh_enabled_value=None):
        cluster = self.cluster
        cluster.populate(num)
        nodes = self.cluster.nodelist()

        for node in nodes:
            node.start(wait_for_binary_proto=True, jvm_args=self.__jvm_args(hh_enabled_value) + custom_args)

    def __wait_until_hints_are_sent_from(self, node_from, count):
        def check():
            res = self.get_node_metrics(self.get_ip_from_node(node_from), metrics=["scylla_hints_manager_sent"])
            sent_count = res["scylla_hints_manager_sent"]
            debug("There were {} hints sent".format(sent_count))
            return sent_count >= count
        wait_for(check, timeout=60, text="Waiting until there are {} hints sent from {}...".format(count, node_from.name))

    def __check_hints_dir_present(self, node_from, node_to, must_be_present=True, shard=None):
        dir_name = ""
        if shard is None:
            dir_name = "{}/hints/*/{}".format(node_from.get_path(), node_to.address())
        else:
            dir_name = "{}/hints/{}/{}".format(node_from.get_path(), shard, node_to.address())

        debug("Check that the directory {} is {}...".format(dir_name, "present" if must_be_present else "not present"))
        if must_be_present:
            return len(glob.glob(dir_name)) != 0
        else:
            return len(glob.glob(dir_name)) == 0

    def __get_hint_segs_count(self, node_from, node_to, shard=0):
        files_mask = "{}/hints/{}/{}/*".format(node_from.get_path(), shard, node_to.address())
        return len(glob.glob(files_mask))

    def __stop_all(self, nodes):
        for node in nodes:
            debug("Stopping {}...".format(node.name))
            node.stop(wait_other_notice=True)

    def __start_all(self, nodes, hh_enabled_value=None, extra_jvm_args=[]):
        for node in nodes:
            debug("Starting {}...".format(node.name))
            node.start(wait_for_binary_proto=True,
                       jvm_args=self.__jvm_args(hh_enabled_value=hh_enabled_value) + extra_jvm_args)

    def __check_rebalanced_dirs(self, nodes, down_node, num_shards):
        """
        Check that number of hint files on each shard differs by not more than 1 from the number of hint files on other
        shards.
        """
        hints_on_nodes = []

        for node in nodes:
            hints_on_node = []

            for shard in range(0, num_shards):
                hints_on_node.append(self.__get_hint_segs_count(node_from=node, node_to=down_node, shard=shard))

            hints_on_nodes.append(hints_on_node)

        for i in range(0, num_shards):
            for k in range(i + 1, num_shards):
                for j in range(0, len(nodes)):
                    debug("{}: comparing number of files on shards {} and {}".format(nodes[j].name, i, k))
                    assert abs(hints_on_nodes[j][i] - hints_on_nodes[j][k]) <= 1
