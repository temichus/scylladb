import logging
import re
import pytest
from ccmlib.scylla_cluster import ScyllaCluster
from ccmlib.scylla_node import ScyllaNode

from dtest_class import Tester
from tools.cluster import new_node

logger = logging.getLogger(__name__)


class TestRaftRecoverProcedure(Tester):

    @pytest.mark.dtest_full
    @pytest.mark.parametrize("num_of_nodes", (3, 4, 5))
    def test_raft_recovery_procedure(self, num_of_nodes):
        """Manual Recover Raft procedure

        Doc: https://docs.scylladb.com/master/architecture/raft.html#recover-raft-procedure
        """
        logger.debug(f"populating cluster with {num_of_nodes} nodes and start")
        cluster: ScyllaCluster = self.cluster
        cluster.set_configuration_options({"consistent_cluster_management": True})
        cluster.populate(num_of_nodes).start(wait_for_binary_proto=True, wait_other_notice=True)
        median = num_of_nodes // 2
        alive_nodes = cluster.nodelist()[:median]
        failing_nodes = cluster.nodelist()[median:]
        failing_host_ids = [node.hostid() for node in failing_nodes]

        logger.debug("Simulate nodes failing and cluster lost majority")
        for node in failing_nodes:
            node.stop(gently=False)

        logger.debug("Move Raft to recovery")
        for node in alive_nodes:
            node.run_cqlsh("UPDATE system.scylla_local SET value = 'recovery' WHERE key = 'group0_upgrade_state';",
                           show_output=True, return_output=True)

        logger.debug("Rolling restart alive nodes to recover mode")
        marks = [node.mark_log() for node in alive_nodes]
        self.run_rolling_restart(alive_nodes)
        for node, mark in zip(alive_nodes, marks):
            assert node.watch_log_for("RECOVERY mode", from_mark=mark, timeout=300), "'RECOVERY' mode was not enabled"
            logger.debug(f"Node {node.name} restarted in raft recovery mode")

        logger.debug("Remove dead nodes from cluster")
        alive_node = alive_nodes[0]
        while failing_host_ids:
            host_id = failing_host_ids.pop(0)
            if not failing_host_ids:
                alive_node.nodetool(f"removenode {host_id}")
                break
            ignore_dead_nodes = ','.join(failing_host_ids)
            alive_node.nodetool(f"removenode {host_id} --ignore-dead-nodes {ignore_dead_nodes}")

        logger.debug("Check schema version on alive nodes")
        self.assert_schema_version(alive_nodes)

        logger.debug("Truncate raft data")
        cql_cmds = [
            "TRUNCATE TABLE system.discovery;",
            "TRUNCATE TABLE system.group0_history;",
            "DELETE value FROM system.scylla_local WHERE key = 'raft_group0_id';"
        ]
        for node in alive_nodes:
            for cmd in cql_cmds:
                node.run_cqlsh(cmd, show_output=True, return_output=True)

        logger.debug("Check schema version according to procedure again after system table modification")
        self.assert_schema_version(alive_nodes)

        for node in alive_nodes:
            node.run_cqlsh("DELETE FROM system.scylla_local WHERE key = 'group0_upgrade_state';",
                           show_output=True, return_output=True)

        logger.debug("Run rolling restart")
        marks = [node.mark_log() for node in alive_nodes]
        self.run_rolling_restart(alive_nodes)
        for node, mark in zip(alive_nodes, marks):
            assert node.watch_log_for("Raft upgrade finished", from_mark=mark,
                                      timeout=300), "'Raft upgrade was not finished"
            logger.debug(f"Node {node.name} raft upgrade finished")

        logger.debug("Add new node to cluster")
        newnode = new_node(cluster)
        newnode.start(wait_other_notice=True)
        alive_nodes.append(newnode)
        logger.debug("Check schema version")
        self.assert_schema_version(alive_nodes)

    def assert_schema_version(self, nodes: list[ScyllaNode]):
        """Verify all nodes have same schema version.

        Example of output for nodetool describecluster
            Cluster Information:
                Name: Test Cluster
                Snitch: org.apache.cassandra.locator.SimpleSnitch
                DynamicEndPointSnitch: disabled
                Partitioner: org.apache.cassandra.dht.Murmur3Partitioner
                Schema versions:
                    1f32ef89-a6ad-308d-9c69-5110917f1052: [172.18.0.2, 172.18.0.3]
        """
        schemas = set()
        for node in nodes:
            response = node.nodetool('describecluster', capture_output=True)[0]
            logger.debug(f"Describecluster: {response}")
            schemas = response.split('Schema versions:')[1].strip()
            num_schemas = len(re.findall(r'\[.*?\]', schemas))
            assert num_schemas == 1, f"There were multiple schema versions: {schemas} for node {node.name}"

    def run_rolling_restart(self, nodes: list[ScyllaNode]):
        for node in nodes:
            node.nodetool("drain")
            node.stop()
            node.start(wait_other_notice=True)
