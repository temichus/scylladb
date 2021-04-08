import pytest
import logging
from time import sleep
from datetime import datetime, timedelta

from dtest_scylla_manager import ScyllaManagerTool, ScyllaManagerError, TaskStatus, ScyllaManagerMixin, NodeStatus, \
    CqlStatus, HostRestStatus, HostHealth
from dtest_class import Tester, wait_for

logger = logging.getLogger(__name__)


@pytest.mark.scylla_manager
class TestScyllaManagerClusterMgmt(Tester, ScyllaManagerMixin):
    def test_adding_cluster_while_its_down(self):
        self.cluster.populate(3).start(wait_for_binary_proto=True, wait_other_notice=True)
        node1, node2, node3 = self.cluster.nodelist()

        logger.debug("Create Manager Tool instance to run scylla-manager operations")
        manager_tool = ScyllaManagerTool(scylla_manager=self.cluster._scylla_manager)
        cluster_name = "cluster1"
        logger.debug("trying to add an offline cluster to scylla-manager, named: {}".format(cluster_name))
        self.cluster.stop()

        try:
            manager_tool.add_cluster(node=node1, name=cluster_name)
        except ScyllaManagerError as err:
            assert "connection refused" in err.args[0].lower(
            ), "Received an irrelevant ScyllaManagerError when trying to add an offline cluster"
            return
        assert False, "Expected to fail when adding an offline cluster to the manager, but didn't"

    def test_add_more_than_one_scylla_cluster(self):
        self.cluster.populate(3).start(wait_for_binary_proto=True, wait_other_notice=True)
        node1, node2, node3 = self.cluster.nodelist()

        manager_tool = ScyllaManagerTool(scylla_manager=self.cluster._scylla_manager)

        cluster_name1 = "cluster1"
        cluster_name2 = "cluster2"
        logger.debug("Add a cluster to scylla-manager, named: {}".format(cluster_name1))
        manager_tool.add_cluster(node=node1, name=cluster_name1)

        logger.debug("Add a cluster to scylla-manager, named: {}".format(cluster_name2))
        manager_tool.add_cluster(node=node2, name=cluster_name2)

        logger.debug(manager_tool.cluster_list)
        expected_list = [cluster_name1, cluster_name2]
        assert sorted(manager_tool.parsed_cluster_list) == sorted(expected_list), \
            """The list of clusters managed by the manager differ from the expected list:
            Expected:{}
            In actuality:{}""".format(expected_list, manager_tool.parsed_cluster_list)

    def _wait_until_task_has_started(self, repair_task, num_of_retries=100, interval=5):
        for i in range(num_of_retries):
            if repair_task.status == "RUNNING":
                return True
            sleep(interval)
        assert False, "Timeout: The task {} did not start".format(repair_task.id)

    def _node_inwhich_repair_started(self, node_list, timeout=100, step=10):
        start = datetime.now()
        while datetime.now() - start < timedelta(seconds=timeout):
            for node in node_list:
                repair_beginning_message_results = node.grep_log(expr='starting user-requested repair')
                if repair_beginning_message_results:
                    return node
            sleep(step)
        return False

    @pytest.mark.require("#2155")
    def test_removing_managed_driver_during_repair(self):
        self.cluster.populate(3).start(wait_for_binary_proto=True, wait_other_notice=True)
        node1, node2, node3 = self.cluster.nodelist()
        self.cluster.stress(['write', 'n=1000K', '-rate', 'threads=50', '-pop', 'seq=10000001..20000000',
                             '-schema', 'replication(replication_factor=3)'])

        logger.debug("Create Manager Tool instance to run scylla-manager operations")
        manager_tool = ScyllaManagerTool(scylla_manager=self.cluster._scylla_manager)
        cluster_name = "cluster1"
        mgr_cluster = manager_tool.add_cluster(node=node1, name=cluster_name)
        repair_task = mgr_cluster.repair_api.repair(cluster_name=mgr_cluster.id)
        is_status_reached = repair_task.wait_for_status([TaskStatus.RUNNING])
        assert is_status_reached, "Timeout: The task {} did not start".format(repair_task.id)

        repaired_node = self._node_inwhich_repair_started([node1, node2, node3])
        logger.debug(f"Chosen node: {repaired_node.name}")
        assert repaired_node, \
            "The manager started the repair task, yet could not find evidence of that in the cluster nodes"
        repair_task.stop()
        sleep(10)  # The manager waits 5 seconds for lingering repair threads before aborting the repair
        # Making sure that the repair that started in the cluster has stopped
        repair_ending_message_results = repaired_node.grep_log(expr="Aborted [0-9] repair job")
        assert repair_ending_message_results, "Stopping the repair through the manager did not stop the repair " \
                                              "in the cluster"

    def test_removing_node_from_managed_cluster(self):
        self.cluster.populate(3).start(wait_for_binary_proto=True, wait_other_notice=True)
        node1, node2, node3 = self.cluster.nodelist()
        step = 3
        timeout = 10 * step

        manager_tool = ScyllaManagerTool(scylla_manager=self.cluster._scylla_manager)
        cluster_name = "cluster1"
        mgr_cluster = manager_tool.add_cluster(node=node1, name=cluster_name)

        self.cluster.remove(node3)
        cluster_status = mgr_cluster.get_hosts_health()
        node_details: HostHealth = cluster_status[node3.address()]
        logger.info(f"Checking the status of CQL and REST after node '{node3.name}' is removed")
        assert node_details.cql.status == CqlStatus.DOWN, \
            f"The CQL status of node '{node3.name}' should be '{CqlStatus.DOWN}'"
        assert node_details.rest.status == HostRestStatus.DOWN, \
            f"The CQL status of node '{node3.name}' should be '{HostRestStatus.DOWN}'"
        logger.info(f"Waiting until the status node '{node3.name}' changing to '{NodeStatus.DOWN}'")

        err_msg = f"The status of node '{node3.name}' should be '{NodeStatus.DOWN}'"
        wait_for(func=lambda: mgr_cluster.get_hosts_health()[node3.address()].node_status == NodeStatus.DOWN,
                 text=err_msg, step=step, timeout=timeout)
