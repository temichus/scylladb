import logging

import pytest
from cassandra.query import SimpleStatement
from cassandra.cluster import ConsistencyLevel


from ccmlib.scylla_cluster import ScyllaNode

from upgrade_test import UpgradeTester, upgrade_matrix_from_last_release_version
from tools.assertions import assert_all, assert_row_count, assert_one

logger = logging.getLogger(__name__)


@pytest.mark.dtest_full
class TestSchemaChanges(UpgradeTester):

    __test__ = True

    upgrade_path = upgrade_matrix_from_last_release_version
    init_version = upgrade_path[0]
    ks = "test_upgrades"
    cf = "cf"

    def test_schema_and_data_on_mixed_versions_cluster(self, dtest_config):
        """
        After upgrading one of two nodes, create a new table (which will
        be propagated to the old node) and check that queries against
        that table return correct results.
        """

        self.clone_upgrade_path(dtest_config)
        self.init_cluster(nodes=2)

        node_for_upgrade = self.cluster.nodelist()[0]
        version = self.current_upgrade_path[0]
        logger.info(f"****** START UPGRADE TEST FROM {node_for_upgrade.node_scylla_version} TO {version} ******")
        logger.info(
            f"Upgrade {node_for_upgrade.name} node to from '{node_for_upgrade.node_scylla_version}' to '{version}' version")
        node_for_upgrade.upgrade(upgrade_to_version=version)
        logger.info(f"****** FINISHED UPGRADE TO {version}******")

        node1: ScyllaNode = self.cluster.nodelist()[0]
        node2: ScyllaNode = self.cluster.nodelist()[1]
        assert node1.get_node_scylla_version() != node2.get_node_scylla_version(
        ), f"Nodes have same version {node2.get_node_scylla_version()}"

        with self.patient_exclusive_cql_connection(node1) as session:
            logger.debug("Creating keyspace and table on upgraded node")
            session.execute(
                f"CREATE KEYSPACE {self.ks} WITH replication={{'class': 'SimpleStrategy', 'replication_factor': '2'}}")
            session.execute(f"CREATE TABLE {self.ks}.{self.cf} (a int primary key, b int)")
            logger.debug("Insert 200 rows on upgraded node")
            expected = []
            for i in range(200):
                session.execute(SimpleStatement(
                    f"INSERT INTO {self.ks}.{self.cf} (a, b) VALUES ({i}, {i+1})", consistency_level=ConsistencyLevel.ALL))
                expected.append([i, i + 1])

        logger.info("Check data on not upgraded node")
        with self.patient_exclusive_cql_connection(node2) as session:
            assert_row_count(session, f"{self.ks}.{self.cf}", len(expected), consistency_level=ConsistencyLevel.ALL)
            assert_all(session, f"SELECT * FROM {self.ks}.{self.cf}",
                       expected, cl=ConsistencyLevel.ALL, ignore_order=True)

            for i in range(200):
                assert_one(session, f"SELECT * FROM {self.ks}.{self.cf} WHERE a = {i}",
                           [i, i + 1], cl=ConsistencyLevel.ALL)
