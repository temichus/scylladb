from cassandra import ConsistencyLevel

from dtest import Tester, debug
import glob
from tools import create_c1c2_table, insert_c1c2, query_c1c2, delete_c1c2
import time

class TestHintedHandoff(Tester):
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

        node3_hid = node3.hostid()

        debug("Stopping node3...")
        node3.stop(wait_other_notice=True)

        debug("Populating the data...")
        insert_c1c2(session, n=100, consistency=ConsistencyLevel.ANY)

        debug("Removing node3...")
        node1.removenode(node3_hid)

        debug("Waiting {}s for hints to be sent...".format(self.__hint_flush_threshold))
        time.sleep(self.__hint_flush_threshold)

        debug("Reading the data...")
        for x in xrange(0, 100):
            query_c1c2(session, x, ConsistencyLevel.ONE)

        debug("Check that the directories have been cleaned up...")
        assert self.__check_hints_dir_present(node_from=node1, node_to=node3, must_be_present=False) and \
               self.__check_hints_dir_present(node_from=node2, node_to=node3, must_be_present=False)

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
        node1.start(wait_for_binary_proto=True, jvm_args=self.__jvm_args(node1))

        time.sleep(self.__hint_flush_threshold)

        debug("Stopping node2 and node3...")
        node2.stop(wait_other_notice=True)
        node3.stop(wait_other_notice=True)

        debug("Checking the key...")
        query_c1c2(session, 0, ConsistencyLevel.ONE)


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
        node3.start(wait_for_binary_proto=True, jvm_args=self.__jvm_args(node3))

        debug("Decommissioning node3...")
        node3.decommission()

        debug("Waiting {}s for hints to be sent...".format(self.__hint_flush_threshold))
        time.sleep(self.__hint_flush_threshold)

        debug("Check that the directories have been cleaned up...")
        assert self.__check_hints_dir_present(node_from=node1, node_to=node3, must_be_present=False) and \
               self.__check_hints_dir_present(node_from=node2, node_to=node3, must_be_present=False)

        debug("Reading the data...")
        for x in xrange(0, 100):
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
        node1.start(wait_for_binary_proto=True, jvm_args=self.__jvm_args(node1))

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
        node2.start(wait_for_binary_proto=True, jvm_args=self.__jvm_args(node2))
        node1.stop(wait_other_notice=True)
        session = self.patient_cql_connection(node2)
        session.execute('USE ks')
        query_c1c2(session, 0, ConsistencyLevel.ONE, must_be_missing=True)

        debug("Checking on node3...")
        node3.start(wait_for_binary_proto=True, jvm_args=self.__jvm_args(node3))
        node2.stop(wait_other_notice=True)
        session = self.patient_cql_connection(node3)
        session.execute('USE ks')
        query_c1c2(session, 0, ConsistencyLevel.ONE, must_be_missing=True)


########################################################################################################################
    @property
    def __hint_flush_threshold(self):
        """
        Hints are flushed every 10s, so wait for 15s to make sure the hints are sent.
        """
        return 15

    def __jvm_args(self, node):
        return ['--hinted-handoff-enabled', 'true', '--experimental', 'true']
        # return []

    def __start_cluster_with_hints(self, num):
        cluster = self.cluster
        cluster.populate(num)
        nodes = self.cluster.nodelist()

        for node in nodes:
            node.start(wait_for_binary_proto=True, jvm_args=self.__jvm_args(node))

    def __check_hints_dir_present(self, node_from, node_to, must_be_present=True):
        dir_name = "{}/hints/*/{}".format(node_from.get_path(), node_to.address())

        debug("Check that the directory {} is {}...".format(dir_name, "present" if must_be_present else "not present"))
        if must_be_present:
            return len(glob.glob(dir_name)) != 0
        else:
            return len(glob.glob(dir_name)) == 0
