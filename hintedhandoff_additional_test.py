import glob
import logging
import pytest
import requests
import time

from concurrent.futures import ThreadPoolExecutor

from distutils.util import strtobool
from cassandra import ConsistencyLevel

from ccmlib.scylla_cluster import ScyllaCluster
from ccmlib.node import NodeError

from dtest_class import Tester, wait_for, create_ks, get_ip_from_node
from tools.data import create_c1c2_table, insert_c1c2, query_c1c2, delete_c1c2
from tools.metrics import get_node_metrics


logger = logging.getLogger(__file__)


@pytest.mark.dtest_full
class TestHintedHandoff(Tester):

    @pytest.mark.dtest_debug
    def test_hintedhandoff_rebalance(self):
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

        logger.info("Stopping node3...")
        node3.stop(wait_other_notice=True)

        op_cnt = 100000
        if isinstance(self.cluster, ScyllaCluster) and self.cluster.scylla_mode == 'debug':
            op_cnt = 25000
        logger.info(f"Populating the data with {op_cnt} keys...")
        stress_cmd = ['write', 'n={}'.format(op_cnt), 'no-warmup', 'cl=QUORUM',
                      '-rate', 'threads=300', '-schema', 'replication(factor=3)']
        resp = node1.stress_object(stress_cmd, ignore_errors=True)

        if not resp or 'total partitions:write' not in resp:
            raise Exception('Error running stress test: {}'.format(resp))

        assert resp['total partitions:write'] == op_cnt

        logger.info("Check SMP=3")
        self.__stop_all([node1, node2])
        self.__start_all([node1, node2, node3], hh_enabled_value='dont_send_hints', extra_jvm_args=['--smp', '3'])
        self.__check_rebalanced_dirs([node1, node2], node3, 3)

        logger.info("Check SMP=2")
        self.__stop_all([node2, node3, node1])
        self.__start_all([node1, node2, node3], hh_enabled_value='dont_send_hints', extra_jvm_args=['--smp', '2'])
        self.__check_rebalanced_dirs([node1, node2], node3, 2)

        logger.info("Check that shard 2 directories are gone")
        assert self.__check_hints_dir_present(node_from=node1, node_to=node3, must_be_present=False, shard=2) and \
            self.__check_hints_dir_present(node_from=node2, node_to=node3, must_be_present=False, shard=2)

        logger.info("Check data consistency")
        self.__stop_all([node2, node3, node1])
        self.__start_all([node1, node2, node3], extra_jvm_args=['--smp', '2'])

        # Wait fill all hints are sent
        for node in [node1, node2]:
            for shard in range(0, 2):
                while self.__get_hint_segs_count(node, node3, shard) > 1:
                    logger.info("Still sending hints")
                    time.sleep(1)

        self.__stop_all([node2, node1])
        logger.info("Reading data")
        stress_cmd = ['read', 'n={}'.format(op_cnt), 'no-warmup', 'cl=ONE', '-rate',
                      'threads=300', '-schema', 'replication(factor=3)']
        resp = node3.stress_object(stress_cmd, ignore_errors=True)
        if not resp or 'total partitions:read' not in resp:
            raise Exception('Error running stress test: {}'.format(resp))

        assert resp['total partitions:read'] == op_cnt

    @pytest.mark.dtest_debug
    def test_hintedhandoff_removenode(self, fixture_dtest_setup):
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
        self.__start_cluster_with_hints(num=3, custom_args=['--logger-log-level', 'hints_manager=trace'])

        [node1, node2, node3] = self.cluster.nodelist()
        session = fixture_dtest_setup.patient_cql_connection(node1)

        logger.info("Preparing a KS and a CF...")
        create_ks(session, 'ks', 1)
        create_c1c2_table(session)

        node3_addr = node3.address()
        node3_hid = node3.hostid()

        logger.info("Stopping node3...")
        node3.stop(wait_other_notice=True)

        logger.info("Populating the data...")
        insert_c1c2(session, n=100, consistency=ConsistencyLevel.ANY)

        marks = []
        for node in [node1, node2]:
            marks.append((node, node.mark_log()))

        logger.info("Removing node3...")
        node1.removenode(node3_hid)

        timeout = self.__hint_flush_threshold * 2
        wait_until = time.time() + timeout
        logger.info("Waiting {}s for hints to be sent...".format(timeout))

        for (node, from_mark) in marks:
            # We wait twice on each shard because there are two hints managers: one for regular writes and one for views
            msgs = ['hints_manager - drain_for: finished draining {}'.format(node3_addr)] * (2 * node._smp)

            node_timeout = max(wait_until - time.time(), 1)
            node.watch_log_for(msgs, from_mark=from_mark, timeout=node_timeout)

        logger.info("Reading the data...")
        for x in range(0, 100):
            query_c1c2(session, x, ConsistencyLevel.ONE)

        logger.info("Check that the directories have been cleaned up...")
        assert self.__check_hints_dir_present(node_from=node1, node_to=node3, must_be_present=False) and \
            self.__check_hints_dir_present(node_from=node2, node_to=node3, must_be_present=False)

    @pytest.mark.next_gating
    @pytest.mark.dtest_debug
    def test_hintedhandoff_basic_check(self, fixture_dtest_setup):
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
        session = fixture_dtest_setup.patient_cql_connection(node1)
        logger.info("Creating a keyspace...")
        create_ks(session, 'ks', 3)

        logger.info("Creating a table..")
        create_c1c2_table(session)

        logger.info("Stopping node1...")
        node1.stop(wait_other_notice=True)

        logger.info("Inserting a key...")
        insert_c1c2(session, n=1, consistency=ConsistencyLevel.ONE)

        # Wait for the write to timeout if needed
        time.sleep(5)

        assert self.__check_hints_dir_present(node_from=node3, node_to=node1) or \
            self.__check_hints_dir_present(node_from=node2, node_to=node1)

        logger.info("Starting node1...")
        node1.start(wait_for_binary_proto=True, jvm_args=self.__jvm_args())

        time.sleep(self.__hint_flush_threshold)

        logger.info("Stopping node2 and node3...")
        node2.stop(wait_other_notice=True)
        node3.stop(wait_other_notice=True)

        logger.info("Checking the key...")
        query_c1c2(session, 0, ConsistencyLevel.ONE)

    def test_hintedhandoff_counter(self, fixture_dtest_setup):
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
        session = fixture_dtest_setup.patient_cql_connection(node1)

        logger.info("Preparing a KS and a CF...")
        create_ks(session, 'ks', 2)
        session.execute("CREATE TABLE ks.tbl (pk int PRIMARY KEY, c counter) " +
                        "WITH speculative_retry = 'NONE' " +
                        "AND read_repair_chance = 0 " +
                        "AND dclocal_read_repair_chance = 0")

        logger.info("Stopping node2...")
        node2.stop(wait_other_notice=True)

        logger.info("Populating the data...")
        stmt = session.prepare("UPDATE ks.tbl SET c = c + ? WHERE pk = ?")
        stmt.consistency_level = ConsistencyLevel.ONE

        expected_values = [100 * i for i in range(100)]
        for i, v in enumerate(expected_values):
            session.execute(stmt.bind((v, i)))

        logger.info("Restarting node1 with hinted handoff disabled...")
        node1.stop()
        node1.start(wait_for_binary_proto=True, jvm_args=self.__jvm_args(hh_enabled_value='false'))

        logger.info("Starting node2...")
        node2.start(wait_for_binary_proto=True, jvm_args=self.__jvm_args())

        logger.info("Adding node3...")
        node3 = self.cluster.new_node(3, auto_bootstrap=True)
        node3.start(wait_for_binary_proto=True, jvm_args=self.__jvm_args())

        logger.info("Restarting node1 with hinted handoff enabled...")
        node1.stop()
        node1.start(wait_for_binary_proto=True, jvm_args=self.__jvm_args(hh_enabled_value='true'))

        logger.info("Waiting for hints to be sent...")
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
        logger.info("Stopping node1...")
        node1.stop(wait_other_notice=True)

        logger.info("Reading the data from nodes 2 and 3...")
        session = fixture_dtest_setup.patient_cql_connection(node3)
        stmt = session.prepare("SELECT c FROM ks.tbl WHERE pk = ?")
        stmt.consistency_level = ConsistencyLevel.ONE

        # Collect all rows into a list and then compare with data that we expect
        actual_values = []
        for i, v in enumerate(expected_values):
            row = list(session.execute(stmt.bind((i,))))[0]
            if row.c != v:
                logger.info("pk={}; actual c={}, expected c={}".format(i, row.c, v))
            actual_values.append(row.c)

        assert actual_values == expected_values

    def test_hintedhandoff_decom(self, fixture_dtest_setup):
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
        session = fixture_dtest_setup.patient_cql_connection(node1)

        logger.info("Preparing a KS and a CF...")
        create_ks(session, 'ks', 1)
        create_c1c2_table(session)

        logger.info("Stopping node3...")
        node3.stop(wait_other_notice=True)

        logger.info("Populating the data...")
        insert_c1c2(session, n=100, consistency=ConsistencyLevel.ANY)

        logger.info("Staring node3...")
        node3.start(wait_for_binary_proto=True, jvm_args=self.__jvm_args())

        logger.info("Decommissioning node3...")
        node3.decommission()

        logger.info("Waiting {}s for hints to be sent...".format(self.__hint_flush_threshold))
        time.sleep(self.__hint_flush_threshold)

        logger.info("Check that the directories have been cleaned up...")
        assert self.__check_hints_dir_present(node_from=node1, node_to=node3, must_be_present=False) and \
            self.__check_hints_dir_present(node_from=node2, node_to=node3, must_be_present=False)

        logger.info("Reading the data...")
        for x in range(0, 100):
            query_c1c2(session, x, ConsistencyLevel.ONE)

    def test_hintedhandoff_dont_revive(self, fixture_dtest_setup):
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
        session = fixture_dtest_setup.patient_cql_connection(node1)
        logger.info("Creating a keyspace...")
        create_ks(session, 'ks', 3)

        logger.info("Creating a table..")
        create_c1c2_table(session)

        logger.info("Stopping node1...")
        node1.stop(wait_other_notice=True)

        logger.info("Inserting a key...")
        insert_c1c2(session, n=1, consistency=ConsistencyLevel.TWO)

        # Wait for the write to timeout if needed
        time.sleep(5)

        assert self.__check_hints_dir_present(node_from=node3, node_to=node1) or \
            self.__check_hints_dir_present(node_from=node2, node_to=node1)

        logger.info("Stopping the node that has the hint...")
        hinting_node = None
        if self.__check_hints_dir_present(node_from=node3, node_to=node1):
            node3.stop(wait_other_notice=True)
            hinting_node = node3
        else:
            node2.stop(wait_other_notice=True)
            hinting_node = node2

        logger.info("Stopped {}".format(hinting_node.name))

        logger.info("Starting node1...")
        node1.start(wait_for_binary_proto=True, jvm_args=self.__jvm_args())

        logger.info("Deleting the row...")
        delete_c1c2(session, n=1, consistency=ConsistencyLevel.TWO)

        logger.info("Starting {}".format(hinting_node.name))
        hinting_node.start(wait_for_binary_proto=True, jvm_args=self.__jvm_args())

        logger.info("Waiting {}s for hints to be sent...".format(self.__hint_flush_threshold))
        time.sleep(self.__hint_flush_threshold)

        logger.info("Checking the key (must be missing)...")
        logger.info("Checking on node1...")
        node3.stop(wait_other_notice=True)
        node2.stop(wait_other_notice=True)
        query_c1c2(session, 0, ConsistencyLevel.ONE, must_be_missing=True)

        logger.info("Checking on node2...")
        node2.start(wait_for_binary_proto=True, jvm_args=self.__jvm_args())
        node1.stop(wait_other_notice=True)
        session = fixture_dtest_setup.patient_cql_connection(node2)
        session.execute('USE ks')
        query_c1c2(session, 0, ConsistencyLevel.ONE, must_be_missing=True)

        logger.info("Checking on node3...")
        node3.start(wait_for_binary_proto=True, jvm_args=self.__jvm_args())
        node2.stop(wait_other_notice=True)
        session = fixture_dtest_setup.patient_cql_connection(node3)
        session.execute('USE ks')
        query_c1c2(session, 0, ConsistencyLevel.ONE, must_be_missing=True)

    def test_hintedhandoff_retransmit(self):
        """
        Test sending consistency. There should be no discarded hints.
        Validates the fix of scylladb/scylla#4122.
        """
        self.__start_cluster_with_hints(num=3)

        node1, node2, node3 = self.cluster.nodelist()

        logger.info("Stopping node2...")
        node2.stop(wait_other_notice=True)

        # Make node2 slower than others in order to trigger hints generation
        logger.info("Starting node2 with \"trace\" log level...")
        node2.start(wait_for_binary_proto=True, jvm_args=self.__jvm_args() +
                    ["--logger-log-level", "hints_manager=trace"])

        logger.info("starting a stress...")
        stress_cmd = ['write', 'duration=4m', 'no-warmup', 'cl=ONE',
                      '-rate', 'threads=300', '-schema', 'replication(factor=3)']
        node1.stress_object(stress_cmd, ignore_errors=True)
        logger.info("stress finished")

        for node in [node1, node2, node3]:
            logger.info("checking {}".format(node.name))
            res = get_node_metrics(get_ip_from_node(node), metrics=["scylla_hints_manager_discarded"])
            logger.info("checking that scylla_hints_manager_discarded is present")
            assert "scylla_hints_manager_discarded" in res
            logger.info("checking that scylla_hints_manager_discarded is zero")
            if res["scylla_hints_manager_discarded"] != 0:
                logger.info("{}: scylla_hints_manager_discarded = {}".format(
                    node.name, res["scylla_hints_manager_discarded"]))
                assert 0 == res["scylla_hints_manager_discarded"], "There were discarded hints"

    @staticmethod
    def validate_max_hinted_handoff_concurrency_value(node, expected_value):
        response = requests.get(f'http://{get_ip_from_node(node)}:10000/v2/config/max_hinted_handoff_concurrency')
        assert "No such config entry" not in response.text, f"No 'max_hinted_handoff_concurrency' config entry"
        assert int(response.text) == expected_value, \
            f"Expected 'max_hinted_handoff_concurrency' value is {expected_value}, got {response.text}"

    @pytest.mark.parametrize(argnames=("max_hinted_handoff_concurrency", "jvm_args"),
                             argvalues=[(0, None), (64, None), (128, ['--max-hinted-handoff-concurrency', '128'])])
    def test_support_max_hh_concurrency_param(self, max_hinted_handoff_concurrency: int, jvm_args: list):
        logger.info(f"Creating a cluster with hints initially enabled and max_hinted_handoff_concurrency "
                    f"is {max_hinted_handoff_concurrency}")
        if not jvm_args:
            if max_hinted_handoff_concurrency == 0:
                self.cluster.set_configuration_options(values={"hinted_handoff_enabled": "true"})
            else:
                self.cluster.set_configuration_options(values={"hinted_handoff_enabled": "true",
                                                               "max_hinted_handoff_concurrency":
                                                               max_hinted_handoff_concurrency})

        self.cluster.populate(2).start(wait_other_notice=True, wait_for_binary_proto=True, jvm_args=jvm_args)
        node1, node2 = self.cluster.nodelist()

        self.validate_max_hinted_handoff_concurrency_value(node=node1, expected_value=max_hinted_handoff_concurrency)
        self.validate_max_hinted_handoff_concurrency_value(node=node2, expected_value=max_hinted_handoff_concurrency)

        logger.info("Stop node1 to create a hints on node2")
        node1.stop(wait_other_notice=True)
        rows = 100
        node2.stress(['write', f'n={rows}', '-schema', 'replication(factor=2)'])
        node1.start(wait_other_notice=True, wait_for_binary_proto=True)

        logger.info("Waiting for hints to be sent...")
        self.__wait_until_hints_are_sent_from(node_from=node2, count=rows)

        logger.info("Stop node2 and validate that all data was sent to node1")
        node2.stop(wait_other_notice=True)

        session = self.patient_cql_connection(node1)
        stress_table = 'keyspace1.standard1'
        assert list(session.execute(f"SELECT count(*) FROM {stress_table}"))[0].count == rows

    @pytest.mark.require('#scylladb/scylla#10111')
    def test_support_max_hh_concurrency_param_negative(self, fixture_dtest_setup):
        """
        https://github.com/scylladb/scylla/commit/de1679b1b99435bea9d2e801d0e7f61785aed8ff
        Maximum concurrency allowed for sending hints. The concurrency is divided across shards and rounded up if not
        divisible by the number of shards. By default, (or when set to 0), concurrency of 8*shard_count will be used.

        This test scenario:
         - run cluster with negative max_hinted_handoff_concurrency value, set in scylla.yaml
         - validate error message
        """
        expected_error_message = 'bad conversion : max_hinted_handoff_concurrency'
        self.ignore_log_patterns += [expected_error_message]
        self.cluster.set_configuration_options(values={"hinted_handoff_enabled": "true",
                                                       "max_hinted_handoff_concurrency": '-1'})
        self.cluster.populate(1).start(wait_other_notice=True, wait_for_binary_proto=True)

        # TODO: validate that it's correct message
        assert self.cluster.nodelist()[0].grep_log(expr=expected_error_message), \
            f"Expected error message '{expected_error_message}' is not found for node {self.cluster.nodelist()[0].name}"

    @pytest.mark.require('#scylladb/scylla#10111')
    def test_support_max_hh_concurrency_param_negative_via_args(self, fixture_dtest_setup):
        """
        https://github.com/scylladb/scylla/commit/de1679b1b99435bea9d2e801d0e7f61785aed8ff
        Maximum concurrency allowed for sending hints. The concurrency is divided across shards and rounded up if not
        divisible by the number of shards. By default, (or when set to 0), concurrency of 8*shard_count will be used.

        This test scenario:
         - run cluster with negative max_hinted_handoff_concurrency value, set in arguments
         - validate that node has not been started
        """
        max_hinted_handoff_concurrency = '-1'
        # TODO: validate that it's correct message
        expected_error_message = (rf"the argument \('{max_hinted_handoff_concurrency}'\) for option "
                                  r"'--max-hinted-handoff-concurrency' is invalid\)")
        self.ignore_log_patterns += [expected_error_message]
        with pytest.raises(expected_exception=(Exception,)):
            self.cluster.populate(1).start(wait_other_notice=True, wait_for_binary_proto=True,
                                           jvm_args=['--max-hinted-handoff-concurrency',
                                                     max_hinted_handoff_concurrency])
            raise NodeError("Node was started unexpectedly")

        assert self.cluster.nodelist()[0].grep_log(expr=expected_error_message), \
            f"Expected error message '{expected_error_message}' is not found for node {self.cluster.nodelist()[0].name}"

    def hintedhandoff_switch_config_in_runtime_template(self, fixture_dtest_setup, hh_enabled_updater):
        """
        A template for testing that switching hinted handoff configuration works in runtime.
        Ref: https://github.com/scylladb/scylla/issues/5634

        Create a cluster of 3 nodes with hinted handoff disabled, each node is put into separate DC
        Create a KS with RF=3
        Create a table
        Stop node2 and node3

        Case A:
        Enable hinting on node1 using the provided configuration update function (hh_enabled_updater)
        Insert some rows with CL=ONE - each write should create one hint towards node2 and one for node3

        Case B:
        Disable hinting on node1
        Insert some rows with CL=ONE - no hints should be created during this step

        Case C:
        Enable hinting on node1, but towards node3's DC only
        Insert some rows with CL=ONE - each write should create one hint towards node3 only

        Verification:
        Start node2 and node3
        Wait until hints are sent to node2 and node3
        Stop node1 and node3
        Verify that node2 has rows caused by hints from case A, but not B or C
        Start node3
        Stop node2
        Verify that node3 has rows caused by hints from case A and C, but not B
        """

        logger.info("Creating a cluster with hints initially disabled")
        cluster = self.cluster
        # If we want to test changing hint generation options through config
        # reload, we cannot specify --hinted-handoff-parameter in commandline.
        # A commandline option always overrides configuration options, and
        # prevents such option to be reloaded from config.
        cluster.set_configuration_options(values={"endpoint_snitch": "GossipingPropertyFileSnitch",
                                                  "hinted_handoff_enabled": "false"})
        cluster.populate(nodes=[1, 1, 1])  # Put each node in a separate DC
        all_nodes = self.cluster.nodelist()
        node1, node2, node3 = all_nodes

        for node in all_nodes:
            # Use multiple shards so that we check that filtering is updated
            # on all shards
            jvm_args = ['--logger-log-level', 'hints_manager=trace', '--smp', '3']
            node.start(wait_for_binary_proto=True, jvm_args=jvm_args)

        keys1 = list(range(0, 100))
        keys2 = list(range(100, 200))
        keys3 = list(range(200, 300))
        expected_hints_count = 0

        session = fixture_dtest_setup.patient_cql_connection(node1)
        logger.info("Creating a keyspace...")
        create_ks(session, 'ks', 3)

        logger.info("Creating a table...")
        create_c1c2_table(session)

        logger.info("Stopping node2 and node3...")
        node2.stop(wait_other_notice=True)
        node3.stop(wait_other_notice=True)

        logger.info("Enable hints on node1")
        hh_enabled_updater(node1, "true")

        session = fixture_dtest_setup.patient_cql_connection(node1)
        session.execute('USE ks')

        logger.info("Inserting keys...")
        insert_c1c2(session, keys=keys1, consistency=ConsistencyLevel.ONE)
        # Each write should generate two hints
        expected_hints_count += 2 * len(keys1)

        logger.info("Disable hints on node1")
        hh_enabled_updater(node1, "false")

        logger.info("Inserting keys...")
        insert_c1c2(session, keys=keys2, consistency=ConsistencyLevel.ONE)
        # No hints should be generated

        logger.info("Enable hints on node1, but only towards node3")
        # Add more dummy DCs so that we test parsing commas
        dcs = ",".join(set([node3.data_center, 'some-dc', 'some-other-dc']))
        hh_enabled_updater(node1, dcs)

        logger.info("Inserting keys...")
        insert_c1c2(session, keys=keys3, consistency=ConsistencyLevel.ONE)
        # Only hints towards node3 should be generated
        expected_hints_count += len(keys3)

        logger.info("Starting node2 and node3...")
        node2.start(wait_other_notice=True)
        node3.start(wait_other_notice=True)

        logger.info("Enable hints on node1")
        hh_enabled_updater(node1, "true")

        logger.info("Waiting for hints to be sent...")
        self.__wait_until_hints_are_sent_from(node_from=node1, count=expected_hints_count)

        # Check rows on node2, should only have keys from keys1

        logger.info("Stopping node1 and node3...")
        node1.stop(wait_other_notice=True)
        node3.stop(wait_other_notice=True)

        session = fixture_dtest_setup.patient_cql_connection(node2)
        session.execute('USE ks')

        logger.info("Checking that data inserted when hinted handoff was ENABLED IS present on node2...")
        for k in keys1:
            query_c1c2(session, k, ConsistencyLevel.ONE, must_be_missing=False)

        logger.info("Checking that data inserted when hinted handoff was DISABLED IS NOT present on node2...")
        for k in keys2:
            query_c1c2(session, k, ConsistencyLevel.ONE, must_be_missing=True)

        logger.info("Checking that data inserted when hinted handoff was DISABLED towards node2's DC, IS NOT present on node2...")
        for k in keys3:
            query_c1c2(session, k, ConsistencyLevel.ONE, must_be_missing=True)

        # Check rows on node3, should only have keys from keys1 and keys3

        logger.info("Starting node3...")
        node3.start(wait_other_notice=True)
        logger.info("Stopping node2...")
        node2.stop(wait_other_notice=True)

        session = fixture_dtest_setup.patient_cql_connection(node3)
        session.execute('USE ks')

        logger.info("Checking that data inserted when hinted handoff was ENABLED IS present on node3...")
        for k in keys1:
            query_c1c2(session, k, ConsistencyLevel.ONE, must_be_missing=False)

        logger.info("Checking that data inserted when hinted handoff was DISABLED IS NOT present on node3...")
        for k in keys2:
            query_c1c2(session, k, ConsistencyLevel.ONE, must_be_missing=True)

        logger.info("Checking that data inserted when hinted handoff was ENABLED towards node3's DC IS present on node3...")
        for k in keys3:
            query_c1c2(session, k, ConsistencyLevel.ONE, must_be_missing=False)

    def hintedhandoff_switch_config_in_runtime_via_http_api(self, fixture_dtest_setup):
        self.hintedhandoff_switch_config_in_runtime_template(fixture_dtest_setup, self.__update_hh_enabled_via_http_api)

    @pytest.mark.dtest_debug
    @pytest.mark.scylla_mode('!release')
    def hintedhandoff_sync_point_api_test(self):
        """
        Tests the HTTP API for hint sync points.
        Hint sync points allow to wait until all current hints are sent
        between two specified sets of nodes: sources and destinations.

        There are two subtests:
        - Check that waiting for a point finishes after all waited on hints are replayed
        - Check that waiting for a point aborts when the waiting node shuts down
        - Check that waiting for a point works after the node is restarted with a different number of shards
        - Check that waiting for a point created on another node is forbidden
        - Check that waiting for a point works when the waiting node is being decommissioned
        """

        self.__start_cluster_with_hints(num=2, custom_args=['--smp', '3'])

        node1, node2 = self.cluster.nodelist()

        hint_sync_point_url = 'http://{}:10000/hinted_handoff/sync_point'

        def create_sync_point(node=node1):
            url = hint_sync_point_url.format(self.get_ip_from_node(node))
            sync_point_id = requests.post(url, params={
                "target_hosts": self.get_ip_from_node(node2),
            }).json()
            logger.debug("Created hint sync point with ID {}".format(sync_point_id))
            self.assertTrue(isinstance(sync_point_id, str))
            return sync_point_id

        def wait_for_sync_point(sync_point_id, timeout, expect, node=node1):
            url = hint_sync_point_url.format(self.get_ip_from_node(node))
            status = requests.get(url, params={
                "id": sync_point_id,
                "timeout": str(timeout),
            })
            if expect == 'FAILED':
                self.assertFalse(status.ok)
            else:
                self.assertEqual(status.json(), expect)
            logger.debug("Got status {}, which was expected".format(status.json()))

        # An executor which will be used to asynchronously wait for sync points
        waiter_executor = ThreadPoolExecutor(max_workers=1)

        # We are using RF=1, so roughly half of the writes will be written as hints
        keys1 = list(range(0, 100))
        keys2 = list(range(100, 200))
        keys3 = list(range(200, 300))

        # Nothing is written for subtest 4
        keys5 = list(range(400, 500))

        session = self.patient_cql_connection(node1)
        logger.debug("Creating a keyspace...")
        self.create_ks(session, 'ks', 1)

        logger.debug("Creating a table...")
        create_c1c2_table(self, session)

        logger.debug("SUBTEST 1: Create a hint sync point, unpause hint replay and wait until they are replayed to the end")

        logger.debug("Stopping node2...")
        node2.stop(wait_other_notice=True)

        logger.debug("Pause hint replay on node1")
        self.enable_error("hinted_handoff_pause_hint_replay", node1)

        logger.debug("Inserting {} keys...".format(len(keys1)))
        insert_c1c2(session, keys=keys1, consistency=ConsistencyLevel.ANY)

        logger.debug("Starting node2...")
        node2.start(wait_other_notice=True)

        logger.debug("Create hint sync point")
        sync_point_id = create_sync_point()

        logger.debug("Check that the sync point is not immediately resolved (because hint replay is paused)")
        wait_for_sync_point(sync_point_id, 0, expect="IN_PROGRESS")

        logger.debug("Start waiting for the point, asynchonously, with infinite timeout")
        fut = waiter_executor.submit(wait_for_sync_point, sync_point_id, -1, expect="DONE")

        logger.debug("Unpause hint replay on node1")
        self.disable_error("hinted_handoff_pause_hint_replay", node1)

        # Waiting should resolve soon - successfully
        logger.debug("Join with the future waiting for the point")
        fut.result(timeout=60)

        logger.debug("Check that the sync point still returns success")
        wait_for_sync_point(sync_point_id, 0, expect="DONE")

        logger.debug("Verifying that hints replayed all of the data...")
        for x in keys1:
            query_c1c2(session, x, ConsistencyLevel.ONE)

        logger.debug("SUBTEST 2: Create a hint sync point, shutdown the waiting node and observe the failure")

        logger.debug("Stopping node2...")
        node2.stop(wait_other_notice=True)

        logger.debug("Pause hint replay on node1")
        self.enable_error("hinted_handoff_pause_hint_replay", node1)

        logger.debug("Inserting {} keys...".format(len(keys2)))
        insert_c1c2(session, keys=keys2, consistency=ConsistencyLevel.ANY)

        logger.debug("Starting node2...")
        node2.start(wait_other_notice=True)

        logger.debug("Create hint sync point...")
        sync_point_id = create_sync_point()

        # Asynchronously wait, indefinitely
        logger.debug("Start waiting for the point, asynchonously, with infinite timeout")
        fut = waiter_executor.submit(wait_for_sync_point, sync_point_id, -1, expect="FAILED")

        logger.debug("Stopping node1...")
        node1.stop(wait_other_notice=True)

        logger.debug("Join with the future waiting for the point")
        fut.result(timeout=60)

        logger.debug("Starting node1...")
        node1.start(wait_other_notice=True, jvm_args=['--smp', '3'])

        # Error injections are reset on restart, so hint replay will be unpaused at this point

        logger.debug("SUBTEST 3: Create a hint sync point, restart with different shard count and wait until hints are replayed")

        logger.debug("Stopping node2...")
        node2.stop(wait_other_notice=True)

        logger.debug("Pause hint replay on node1")
        self.enable_error("hinted_handoff_pause_hint_replay", node1)

        logger.debug("Inserting {} keys...".format(len(keys3)))
        insert_c1c2(session, keys=keys3, consistency=ConsistencyLevel.ANY)

        logger.debug("Starting node2...")
        node2.start(wait_other_notice=True)

        logger.debug("Create hint sync point")
        sync_point_id = create_sync_point()

        logger.debug("Stopping node1...")
        node1.stop(wait_other_notice=True)

        logger.debug("Starting node1 with SMP=2...")
        node1.start(wait_for_binary_proto=True, jvm_args=['--smp', '2'])

        # Hint replay is unpaused because of the restart

        logger.debug("Wait until all hints are successfully replayed...")
        wait_for_sync_point(sync_point_id, 60, expect="DONE")

        logger.debug("Verifying that hints replayed all of the data...")
        for x in keys3:
            query_c1c2(session, x, ConsistencyLevel.ONE)

        logger.debug("SUBTEST 4: Create a hint sync point and try to use it on another node - should fail")

        logger.debug("Create hint sync point on node1")
        sync_point_id = create_sync_point(node=node1)

        logger.debug("Try waiting for the point on node2 - should fail")
        wait_for_sync_point(sync_point_id, 0, expect='FAILED', node=node2)

        #####
        logger.debug("SUBTEST 5: Create a hint sync point and decommission the target node - waiting should succeed")

        logger.debug("Stopping node2...")
        node2.stop(wait_other_notice=True)

        logger.debug("Pause hint replay on node1")
        self.enable_error("hinted_handoff_pause_hint_replay", node1)

        logger.debug("Inserting {} keys...".format(len(keys5)))
        insert_c1c2(session, keys=keys5, consistency=ConsistencyLevel.ANY)

        logger.debug("Starting node2...")
        node2.start(wait_other_notice=True)

        logger.debug("Create hint sync point on node1")
        sync_point_id = create_sync_point(node=node1)

        logger.debug("Decommissioning node2...")
        node2.decommission()

        logger.debug("Check that the sync point is not yet resolved (because hint replay is paused)")
        wait_for_sync_point(sync_point_id, 0, expect="IN_PROGRESS")

        logger.debug("Start waiting for the point, asynchonously, with infinite timeout")
        fut = waiter_executor.submit(wait_for_sync_point, sync_point_id, -1, expect="DONE")

        logger.debug("Unpause hint replay on node1")
        self.disable_error("hinted_handoff_pause_hint_replay", node1)

        # Waiting should resolve soon - successfully
        logger.debug("Join with the future waiting for the point")
        fut.result(timeout=60)

        logger.debug("Verifying that hints replayed all of the data...")
        for x in keys5:
            query_c1c2(session, x, ConsistencyLevel.ONE)

        waiter_executor.shutdown()

    @property
    def __hint_flush_threshold(self):
        """
        Hints are flushed every 10s, so wait for 15s to make sure the hints are sent.
        """
        return 15

    @staticmethod
    def __sanitize_hh_enabled_value(v):
        """
        hh_enabled_value should be set either to a boolean value (true|false|1|0)
        or to one or more DCs for which hintedhandoffs are to be enabled.
        If None, this function returns 'true', otherwise it verifies the value's
        syntax and normalizes it.
        """
        if v is None:
            return 'true'
        if isinstance(v, bool):
            return str(v).lower()
        if isinstance(v, int):
            assert v in [0, 1], f"'{v}' must be either '0' or '1'"
            return str(v)
        assert isinstance(v, str), f"'{v}' is not a string"
        assert v != '', "hh_enabled_value must not be empty"
        if v.lower() in ['true', 'false']:
            return v.lower()
        return v

    def __jvm_args(self, hh_enabled_value=None):
        hh_enabled = self.__sanitize_hh_enabled_value(hh_enabled_value)
        return ['--hinted-handoff-enabled', hh_enabled]

    def __start_cluster_with_hints(self, num, custom_args=[], hh_enabled_value=None):
        cluster = self.cluster
        cluster.populate(num)
        nodes = self.cluster.nodelist()

        self.__start_all(nodes, hh_enabled_value=hh_enabled_value, extra_jvm_args=custom_args)

    def __wait_until_hints_are_sent_from(self, node_from, count):
        def check():
            res = get_node_metrics(get_ip_from_node(node_from), metrics=["scylla_hints_manager_sent"])
            sent_count = res["scylla_hints_manager_sent"]
            logger.info("There were {} hints sent".format(sent_count))
            return sent_count >= count
        wait_for(check, timeout=60, text="Waiting until there are {} hints sent from {}...".format(count, node_from.name))

    def __check_hints_dir_present(self, node_from, node_to, must_be_present=True, shard=None):
        dir_name = ""
        if shard is None:
            dir_name = "{}/hints/*/{}".format(node_from.get_path(), node_to.address())
        else:
            dir_name = "{}/hints/{}/{}".format(node_from.get_path(), shard, node_to.address())

        logger.info("Check that the directory {} is {}...".format(
            dir_name, "present" if must_be_present else "not present"))
        if must_be_present:
            return len(glob.glob(dir_name)) != 0
        else:
            return len(glob.glob(dir_name)) == 0

    def __get_hint_segs_count(self, node_from, node_to, shard=0):
        files_mask = "{}/hints/{}/{}/HintsLog-*.log".format(node_from.get_path(), shard, node_to.address())
        return len(glob.glob(files_mask))

    def __stop_all(self, nodes):
        logger.info("Stopping {}...".format([n.name for n in nodes]))
        self.cluster.stop_nodes(nodes)

    def __start_all(self, nodes, hh_enabled_value=None, extra_jvm_args=[]):
        hh_enabled = self.__sanitize_hh_enabled_value(hh_enabled_value)
        hh_description = ''
        try:
            hh_description = "enabled" if strtobool(hh_enabled) else "disabled"
        except ValueError:
            hh_description = f'"{hh_enabled}"'
        logger.info("Starting {} with hintedhandoff {}".format([n.name for n in nodes],
                                                               hh_description))
        self.cluster.start_nodes(wait_for_binary_proto=True,
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
                    logger.info("{}: comparing number of files on shards {}: {} and {}: {}".format(nodes[j].name,
                                                                                                   i, hints_on_nodes[j][i],
                                                                                                   k, hints_on_nodes[j][k]))
                    assert abs(hints_on_nodes[j][i] - hints_on_nodes[j][k]) <= 1, \
                        f"Unexpected number of hint files per shard on node{j+1}: " + \
                        f"abs({hints_on_nodes[j][i]} - {hints_on_nodes[j][k]}) > 1"

    def __update_hh_enabled_via_http_api(self, node, new_value):
        if new_value in ("true", "false"):
            ep = 'storage_proxy/hinted_handoff_enabled'
            url_params = {'enable': new_value}
            expected = (new_value == "true")
        else:
            ep = 'storage_proxy/hinted_handoff_enabled_by_dc'
            url_params = {'dcs': new_value}
            expected = sorted(new_value.split(","))

        url = 'http://{}:10000/{}'.format(get_ip_from_node(node), ep)

        logger.info("Changing hint sending options on {} to {}, through HTTP API: {}".format(node.name, new_value, ep))
        requests.post(url, params=url_params)

        # Sanity check: see if we can fetch the configuration we requested back
        response = requests.get(url).json()
        logger.info("Got response: {}".format(response))

        # List of DCs might be in different order than we specified - that is expected
        if isinstance(response, list):
            response.sort()

        assert expected == response
