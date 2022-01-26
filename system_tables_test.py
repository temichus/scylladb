import logging
from time import sleep
from typing import List, Union, Dict, Any

import pytest
from ccmlib.cluster import Cluster
from ccmlib.node import Node
from cassandra.cluster import Session
from dtest_class import Tester, create_ks


logger = logging.getLogger(__name__)


class SystemTableBase(Tester):
    KEYSPACE_NAME = "system"

    def prepare_cluster(self, nodes: Union[List[int], int]) -> Cluster:
        logger.debug("Preparing the cluster...")
        cluster = self.cluster
        cluster.populate(nodes).start()
        logger.debug("Cluster has been prepared...")
        return cluster

    @staticmethod
    def run_query_on_node(session: Session, query: str) -> List:
        logger.debug("Running query \"%s\"...", query)
        return session.execute(query).current_rows


@pytest.mark.dtest_full
class TestClusterStatusTable(SystemTableBase):
    TABLE_NAME = "cluster_status"
    SELECT_QUERY = f"select * from {SystemTableBase.KEYSPACE_NAME}.{TABLE_NAME};"

    @staticmethod
    def parse_query_output(query_output) -> Dict[str, Dict[str, Any]]:
        node_data = {}
        for row in query_output:
            node_data[row.peer] = {
                "dc": row.dc,
                "host_id": row.host_id,
                "owns": row.owns,
                "status": row.status,
                "up": row.up,
            }
        return node_data

    def check_running_node_status(self, node_status: Dict[str, Any], node_to_check: Node):
        node_ip_address = node_to_check.address()
        node_dc = node_to_check.get_datacenter_name()
        node_id = node_to_check.hostid()
        logger.info("Checking if the node %s has correct status in %s.%s...",
                    node_ip_address, self.KEYSPACE_NAME, self.TABLE_NAME)
        assert node_status["dc"] == node_dc, f"Expected to get 'dc={node_dc}' for node {node_ip_address}, " \
                                             f"but didn't get it!"
        assert str(node_status["host_id"]) == node_id, f"Expected to get 'id={node_id}' for node {node_ip_address}, " \
                                                       f"but didn't get it!"
        assert node_status["status"].upper() == "NORMAL", f"Expected to get 'status=NORMAL' for node " \
                                                          f"{node_ip_address}, but didn't get it!"
        assert node_status["up"], f"Expected to see node {node_ip_address} in running state, but it seems to be down!"

    def test_content_all_nodes_are_up(self):
        """
        The cluster_status table content when all nodes are up

         peer       | dc          | host_id                              | load        | owns     | status | tokens | up
        ------------+-------------+--------------------------------------+-------------+----------+--------+--------+------
         172.17.0.3 | datacenter1 | ff5361b6-d190-4582-a424-64dc4e2f768d |      576966 | 0.468747 | NORMAL |    256 | True
         172.17.0.2 | datacenter1 | d2191d83-1158-4215-999b-363500703235 | 1.05503e+06 | 0.531253 | NORMAL |    256 | True

        The test scenario
        1. Create the cluster of 3 nodes
        2. Check the table content for all 3 nodes.
        """

        cluster = self.prepare_cluster(nodes=3)
        node1 = cluster.nodelist()[0]

        with self.patient_cql_connection(node1) as session:
            query_output = self.run_query_on_node(session=session, query=self.SELECT_QUERY)
            parsed_query_result = self.parse_query_output(query_output=query_output)

        for node in cluster.nodelist():
            node_ip_address = node.address()
            assert parsed_query_result.get(node_ip_address), f"There should the row for the node {node_ip_address} " \
                                                             f"in {self.KEYSPACE_NAME}.{self.TABLE_NAME}, but it " \
                                                             f"wasn't found!"
            self.check_running_node_status(node_status=parsed_query_result[node_ip_address], node_to_check=node)

    def test_content_add_new_node(self):
        """
        The test scenario
        1. Create the cluster of 1 node
        2. Add a new node to the cluster
        3. Check table content: the new row with the information about the new node should be inserted into the table
        4. Check the sum of "owns" values is equal to 1 (since there are no special RF settings).
        5. Check the table content for the new node.
        """
        cluster = self.prepare_cluster(nodes=1)
        node1 = cluster.nodelist()[0]

        logger.debug("Adding a new node to the cluster...")
        node2 = cluster.new_node(2, auto_bootstrap=True, add_node=True)
        node2.start()
        node2_ip_address = node2.address()
        logger.info("The new node %s has been started.", node2_ip_address)

        with self.patient_cql_connection(node1) as session:
            query_output = self.run_query_on_node(session=session, query=self.SELECT_QUERY)
            parsed_query_result = self.parse_query_output(query_output=query_output)

        assert len(parsed_query_result) == len(cluster.nodes), "The table has wrong number of rows!"

        assert parsed_query_result.get(node2_ip_address), f"There should the row for the node {node2_ip_address} " \
                                                          f"in {self.KEYSPACE_NAME}.{self.TABLE_NAME}, but it " \
                                                          f"wasn't found!"

        owns = round(sum([row["owns"] for row in parsed_query_result.values()]), 4)

        assert owns == 1, "The sum of values in the 'owns' column should equal 1"

        self.check_running_node_status(node_status=parsed_query_result[node2_ip_address], node_to_check=node2)

    def test_content_stop_node(self):
        """
        The cluster_status table content when one node is down
         peer       | dc          | host_id                              | load        | owns     | status   | tokens | up
        ------------+-------------+--------------------------------------+-------------+----------+----------+--------+-------
         172.17.0.3 | datacenter1 | ff5361b6-d190-4582-a424-64dc4e2f768d |      588499 | 0.309867 | shutdown |    256 | False
         172.17.0.2 | datacenter1 | d2191d83-1158-4215-999b-363500703235 | 1.06626e+06 | 0.368409 |   NORMAL |    256 |  True
         172.17.0.4 | datacenter1 | 9c51fae6-8a9b-41a3-910f-4afb5e3489db |      934981 | 0.321724 |   NORMAL |    256 |  True

        The test scenario
        1. Create the cluster of 2 nodes
        2. Stop the 2nd node
        3. Check state and status of the stopped node in the table.
        """

        cluster = self.prepare_cluster(nodes=2)

        node1, node2 = cluster.nodelist()
        node2_ip_address = node2.address()

        logger.debug("Stopping the node %s...", node2_ip_address)
        node2.stop()
        logger.info("The node %s has been stopped...", node2_ip_address)

        with self.patient_cql_connection(node1) as session:
            query_output = self.run_query_on_node(session=session, query=self.SELECT_QUERY)
            parsed_query_result = self.parse_query_output(query_output=query_output)

        assert not parsed_query_result[node2_ip_address]["up"], \
            f"The node {node2_ip_address} is down, but in {self.KEYSPACE_NAME}.{self.TABLE_NAME} it has 'up = True'."

        assert parsed_query_result[node2_ip_address]["status"].upper() == "SHUTDOWN", \
            f"Wrong status of node {node2_ip_address} in {self.KEYSPACE_NAME}.{self.TABLE_NAME} table!"

    def test_content_remove_node(self):
        """
        The cluster_status table content when one node is removed
         peer       | dc          | host_id                              | load        | owns     | status  | tokens | up
        ------------+-------------+--------------------------------------+-------------+----------+---------+--------+-------
         172.17.0.3 |        null |                                 null |      588499 |     null | removed |      0 | False
         172.17.0.2 | datacenter1 | d2191d83-1158-4215-999b-363500703235 | 1.06626e+06 | 0.529747 |  NORMAL |    256 |  True
         172.17.0.4 | datacenter1 | 9c51fae6-8a9b-41a3-910f-4afb5e3489db |      934981 | 0.470253 |  NORMAL |    256 |  True

        The test scenario
        1. Create the cluster of 2 nodes
        2. Remove the 2nd node
        3. Check state and status of the removed node in the table.
        """

        cluster = self.prepare_cluster(nodes=2)

        node1, node2 = cluster.nodelist()
        node2_ip_address = node2.address()

        logger.debug("Removing the node %s from the cluster...", node2_ip_address)
        node1.removenode(hid=node2.hostid())
        logger.info("The node %s has been removed from the cluster...", node2_ip_address)

        sleep(10)

        with self.patient_cql_connection(node1) as session:
            query_output = self.run_query_on_node(session=session, query=self.SELECT_QUERY)
            parsed_query_result = self.parse_query_output(query_output=query_output)

        assert not parsed_query_result[node2_ip_address]["up"], f"The node {node2_ip_address} was removed, but in " \
                                                                f"{self.KEYSPACE_NAME}.{self.TABLE_NAME} it has " \
                                                                f"'up = True'."

        assert parsed_query_result[node2_ip_address]["status"].upper() == "REMOVED", \
            f"Wrong status of node {node2_ip_address} in {self.KEYSPACE_NAME}.{self.TABLE_NAME} table!"

    def test_content_multi_dc_all_nodes_are_up(self):
        """
        The cluster_status table content when all nodes are up in multi-dc cluster
         peer       | dc  | host_id                              | load   | owns     | status | tokens | up
        ------------+-----+--------------------------------------+--------+----------+--------+--------+------
         127.0.75.1 | dc1 | 7b13cb68-68ae-4dde-bfdb-96a637616cda | 111808 | 0.339353 | NORMAL |    256 | True
         127.0.75.2 | dc2 | 877f2731-ed8a-49c8-a377-32a790c0dd6b | 149536 | 0.309136 | NORMAL |    256 | True
         127.0.75.3 | dc2 | c6106ead-b8a1-40b4-b20d-9f5c5cdd1e54 | 215766 | 0.351511 | NORMAL |    256 | True

        The test scenario
        1. Create the multi-dc cluster of 2 nodes (one node in each dc)
        2. Check table content for both nodes.
        3. Add a new node to the cluster in the datacenter dc2
        4. Check table content: the new row should be inserted into the table
        5. Check the sum of "owns" values is equal to 1.
        6. Check the table content for the new node.
        """

        cluster = self.prepare_cluster(nodes=[1, 1])
        node1 = cluster.nodelist()[0]

        with self.patient_cql_connection(node1) as session:
            query_output = self.run_query_on_node(session=session, query=self.SELECT_QUERY)
            parsed_query_result = self.parse_query_output(query_output=query_output)

            for node in cluster.nodelist():
                node_ip_address = node.address()
                assert parsed_query_result.get(node_ip_address), \
                    f"There should the row for the node {node_ip_address} in " \
                    f"{self.KEYSPACE_NAME}.{self.TABLE_NAME}, but it wasn't found!"
                self.check_running_node_status(node_status=parsed_query_result[node_ip_address], node_to_check=node)

            logger.debug("Adding a new node to the cluster...")
            node3 = cluster.new_node(3, auto_bootstrap=True, add_node=True, data_center="dc2")
            node3.start()
            node3_ip_address = node3.address()
            logger.info("The new node %s has been started.", node3_ip_address)

            query_output = self.run_query_on_node(session=session, query=self.SELECT_QUERY)
            parsed_query_result = self.parse_query_output(query_output=query_output)

        assert parsed_query_result.get(node3_ip_address), f"There should the row for the node {node3_ip_address} " \
                                                          f"in {self.KEYSPACE_NAME}.{self.TABLE_NAME}, but it " \
                                                          f"wasn't found!"

        owns = round(sum([row["owns"] for row in parsed_query_result.values()]), 4)

        assert owns == 1, "The sum of values in the 'owns' column should equal 1"

        self.check_running_node_status(node_status=parsed_query_result[node3_ip_address], node_to_check=node3)


@pytest.mark.dtest_full
class TestTokenRingTable(SystemTableBase):
    """
    Example of the table content
    -----------------------------
     keyspace_name | start_token          | endpoint   | dc          | end_token            | rack
    ---------------+----------------------+------------+-------------+----------------------+-------
     test_keyspace | -1028636990904053927 | 172.17.0.2 | datacenter1 |  -840980620915277404 | rack1
     test_keyspace | -1059882504016989347 | 172.17.0.2 | datacenter1 | -1028636990904053927 | rack1
     test_keyspace | -1083305362326820612 | 172.17.0.2 | datacenter1 | -1059882504016989347 | rack1
     test_keyspace | -1083467855096097310 | 172.17.0.2 | datacenter1 | -1083305362326820612 | rack1

    """
    TABLE_NAME = "token_ring"
    TEST_KEYSPACE = "test_keyspace"

    def create_test_keyspace(self, session: Session, replication_factor: Union[Dict[str, int], int]):
        logger.debug("Creating a new keyspace '%s'...", self.TEST_KEYSPACE)
        create_ks(session=session, name=self.TEST_KEYSPACE, rf=replication_factor)
        logger.info("New keyspace '%s' has been created", self.TEST_KEYSPACE)

    @pytest.mark.single_node
    def test_content_one_node_create_and_drop_keyspace(self):
        """
        The test scenario
        1. Create 1 node
        2. Create a keyspace.
        3. Check there are rows inserted into the table for this keyspace.
        4. Drop the keyspace
        5. Check there are no rows for dropped keyspace in the table.
        """
        cluster = self.prepare_cluster(nodes=1)
        node1 = cluster.nodelist()[0]

        with self.patient_cql_connection(node1) as session:
            self.create_test_keyspace(session=session, replication_factor=1)

            select_query = f"select distinct keyspace_name from {self.KEYSPACE_NAME}.{self.TABLE_NAME};"
            query_result = self.run_query_on_node(session=session, query=select_query)

            found_keyspaces = [row.keyspace_name for row in query_result]
            logger.info("Checking the records for keyspace '%s' in table %s.%s...", self.TEST_KEYSPACE,
                        self.KEYSPACE_NAME, self.TABLE_NAME)
            assert self.TEST_KEYSPACE in found_keyspaces, \
                f"Token ranges for the keyspace '{self.TEST_KEYSPACE}' weren't found " \
                f"in the table {self.KEYSPACE_NAME}.{self.TABLE_NAME}!"

            logger.debug("Dropping keyspace '%s'...", self.TEST_KEYSPACE)
            query_to_run = f"drop keyspace {self.TEST_KEYSPACE};"
            self.run_query_on_node(session=session, query=query_to_run)
            logger.debug("Keyspace '%s' has been dropped", self.TEST_KEYSPACE)

            query_result = self.run_query_on_node(session=session, query=select_query)

        found_keyspaces = [row.keyspace_name for row in query_result]
        logger.info("Checking the records for keyspace '%s' in table %s.%s...", self.TEST_KEYSPACE, self.KEYSPACE_NAME,
                    self.TABLE_NAME)
        assert self.TEST_KEYSPACE not in found_keyspaces, f"Found token ranges for the non-existing keyspace " \
                                                          f"'{self.TEST_KEYSPACE}' in the table " \
                                                          f"{self.KEYSPACE_NAME}.{self.TABLE_NAME}!"

    def test_content_increasing_cluster(self):
        """
        When a new node is added to the cluster it is assigned a range of tokens for the keyspace, if the total number
        of nodes in the cluster is equal (or lower) the replication factor of that keyspace.
        If it happens the IP address of the node should appear in 'endpoint' column of the 'system.token_ring' table.

        The test scenario:
        1. Create a cluster of 2 nodes
        2. Create a keyspace with RF=3
        3. Select one token range and check that it is assigned to 2 nodes
        4. Add the 3rd node and check that selected token range is assigned to 3 nodes.
        5. Add the 4th node and check that selected token range is still assigned to 3 nodes.
        """
        cluster = self.prepare_cluster(nodes=2)
        node1 = cluster.nodelist()[0]
        replication_factor = 3

        with self.patient_cql_connection(node1) as session:
            self.create_test_keyspace(session=session, replication_factor=replication_factor)

            logger.debug("Getting one token range to check...")
            query_to_run = f"select start_token from {self.KEYSPACE_NAME}.{self.TABLE_NAME} " \
                           f"where keyspace_name = '{self.TEST_KEYSPACE}' limit 1;"
            query_result = self.run_query_on_node(session=session, query=query_to_run)
            start_token = query_result[0].start_token

            query_to_run = f"select * from {self.KEYSPACE_NAME}.{self.TABLE_NAME} where " \
                           f"keyspace_name='{self.TEST_KEYSPACE}' and start_token='{start_token}';"

            logger.info("Checking the number assigned nodes for selected token range...")
            assert len(self.run_query_on_node(session=session, query=query_to_run)) == len(cluster.nodes), \
                f"The token range starting from {start_token} should be assigned to {len(cluster.nodes)} nodes."

            for node_index in [3, 4]:
                logger.debug("Adding the %drd node to the cluster...", node_index)
                cluster.new_node(node_index, auto_bootstrap=True, add_node=True).start()
                logger.info("The %drd node has been added to the cluster.", node_index)

                logger.info("Checking the number assigned nodes for selected token range...")
                assert len(self.run_query_on_node(session=session, query=query_to_run)) == replication_factor, \
                    f"The token range starting from {start_token} should be assigned to {replication_factor} nodes."

    def test_content_multi_dc_cluster(self):
        """
        Test scenario
        1. Create a multi DC cluster of 4 nodes (2 DCs with 2 nodes each)
        2. Create a keyspace with RF={DC1: 2, DC2: 1}
        3. Select one token range and check it is assigned to 3 nodes in different DCs
        """
        cluster = self.prepare_cluster(nodes=[2, 2])
        node1 = cluster.nodelist()[0]
        dc_name_1 = "dc1"
        dc_name_2 = "dc2"

        with self.patient_cql_connection(node1) as session:
            self.create_test_keyspace(session=session, replication_factor={dc_name_1: 2, dc_name_2: 1})

            logger.debug("Getting one token range to check...")
            query_to_run = f"select start_token from {self.KEYSPACE_NAME}.{self.TABLE_NAME} " \
                           f"where keyspace_name = '{self.TEST_KEYSPACE}' limit 1;"
            query_result = self.run_query_on_node(session=session, query=query_to_run)
            start_token = query_result[0].start_token

            query_to_run = f"select dc from {self.KEYSPACE_NAME}.{self.TABLE_NAME} " \
                           f"where keyspace_name = '{self.TEST_KEYSPACE}' and start_token = '{start_token}';"
            query_result = self.run_query_on_node(session=session, query=query_to_run)

        dc_list = [row.dc for row in query_result]
        logger.info("Checking the content of %s.%s...", self.KEYSPACE_NAME, self.TABLE_NAME)
        assert len(dc_list) == 3, f"The token range starting from {start_token} should be assigned to 3 nodes."
        assert dc_name_1 in dc_list and dc_name_2 in dc_list, f"The token range starting from {start_token} should " \
                                                              f"be assigned to nodes from '{dc_name_1}' " \
                                                              f"and '{dc_name_2}'."

    def test_content_match_on_all_nodes(self):
        """
        Test scenario:
        1. Create cluster of 3 nodes
        2. Create keyspace with RF=2
        3. Get the content of system.token_ring for created keyspace on the node1
        4. Compare it with the table content from all the other nodes.
        """
        cluster = self.prepare_cluster(nodes=3)
        node1 = cluster.nodelist()[0]
        node1_ip_address = node1.address()

        query_to_run = f"select * from {self.KEYSPACE_NAME}.{self.TABLE_NAME} " \
                       f"where keyspace_name = '{self.TEST_KEYSPACE}'"
        with self.patient_exclusive_cql_connection(node1) as session:
            self.create_test_keyspace(session=session, replication_factor=2)

            logger.debug("Getting table content of %s.%s on node %s...", self.KEYSPACE_NAME, self.TABLE_NAME,
                         node1_ip_address)
            test_ks_token_set = self.run_query_on_node(session=session, query=query_to_run)
        for node in cluster.nodelist()[1:]:
            node_ip_address = node.address()
            with self.patient_exclusive_cql_connection(node) as session:
                logger.debug("Getting table content of %s.%s on node %s...", self.KEYSPACE_NAME, self.TABLE_NAME,
                             node_ip_address)
                assert test_ks_token_set == self.run_query_on_node(session=session, query=query_to_run), \
                    f"The set of token ranges does not match on the nodes {node1_ip_address} and {node_ip_address}!"

    def test_content_stop_and_replace_node(self):
        """
        1. Create cluster of 3 nodes
        2. Create keyspace with RF=2
        3. Get the content of system.token_ring for created keyspace
        4. Select the token ranges only for node3
        5. Stop node3 and replace it with node4
        6. Get the token ranges from system.token_ring for created keyspace only for node4
        7. Compare token ranges for node 3 and 4
        """
        cluster = self.prepare_cluster(nodes=3)
        node3 = cluster.nodelist()[2]
        node3_ip_address = node3.address()

        query_to_run = f"select * from {self.KEYSPACE_NAME}.{self.TABLE_NAME} " \
                       f"where keyspace_name = '{self.TEST_KEYSPACE}'"
        with self.patient_cql_connection(node3) as session:
            self.create_test_keyspace(session=session, replication_factor=2)

            logger.debug("Getting table content of %s.%s...", self.KEYSPACE_NAME, self.TABLE_NAME)
            test_ks_token_set = self.run_query_on_node(session=session, query=query_to_run)

        node3_token_set = [row.start_token for row in test_ks_token_set if row.endpoint == node3_ip_address]

        logger.info("Replacing node...")
        logger.debug("Stopping node %s...", node3_ip_address)
        node3.stop(wait_other_notice=True)
        logger.debug("Starting new node as replacement of %s...", node3_ip_address)
        node4 = cluster.new_node(4, auto_bootstrap=True, is_seed=False, add_node=True)
        node4.start(replace_address=node3_ip_address)
        node4_ip_address = node4.address()

        with self.patient_exclusive_cql_connection(node4) as session:
            logger.debug("Getting table content of %s.%s on node %s...", self.KEYSPACE_NAME, self.TABLE_NAME,
                         node4_ip_address)
            test_ks_token_set = self.run_query_on_node(session=session, query=query_to_run)

        node4_token_set = [row.start_token for row in test_ks_token_set if row.endpoint == node4_ip_address]
        assert node3_token_set == node4_token_set, "The token ranges before and after node replacement do not match!"
