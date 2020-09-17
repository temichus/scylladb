from time import sleep
from datetime import datetime, timedelta

from nose.plugins.attrib import attr

from tools import require
from dtest_scylla_manager import ScyllaManagerTool, ScyllaManagerError, TaskStatus, ScyllaManagerMixin, NodeStatus, \
    CqlStatus, HostRestStatus, HostHealth
from dtest import Tester, debug, wait_for, info


class TestScyllaManagerClusterMgmt(Tester, ScyllaManagerMixin):

    @attr('scylla-manager')
    def adding_cluster_while_its_down_test(self):
        self.cluster.populate(3).start(wait_for_binary_proto=True, wait_other_notice=True)
        node1, node2, node3 = self.cluster.nodelist()

        debug("Create Manager Tool instance to run scylla-manager operations")
        manager_tool = ScyllaManagerTool(scylla_manager=self.cluster._scylla_manager)
        cluster_name = "cluster1"
        debug("trying to add an offline cluster to scylla-manager, named: {}".format(cluster_name))
        self.cluster.stop()

        try:
            manager_tool.add_cluster(node=node1, name=cluster_name)
        except ScyllaManagerError as err:
            assert "connection refused" in err.args[0].lower(
            ), "Received an irrelevant ScyllaManagerError when trying to add an offline cluster"
            return
        assert False, "Expected to fail when adding an offline cluster to the manager, but didn't"

    @attr('scylla-manager')
    def add_more_than_one_scylla_cluster_test(self):
        self.cluster.populate(3).start(wait_for_binary_proto=True, wait_other_notice=True)
        node1, node2, node3 = self.cluster.nodelist()

        manager_tool = ScyllaManagerTool(scylla_manager=self.cluster._scylla_manager)

        cluster_name1 = "cluster1"
        cluster_name2 = "cluster2"
        debug("Add a cluster to scylla-manager, named: {}".format(cluster_name1))
        manager_tool.add_cluster(node=node1, name=cluster_name1)

        debug("Add a cluster to scylla-manager, named: {}".format(cluster_name2))
        manager_tool.add_cluster(node=node2, name=cluster_name2)

        debug(manager_tool.cluster_list)
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
        assert False, "Timeout: The task {} did not start".format(repair_task.task_id)

    def _node_inwhich_repair_started(self, node_list, timeout=100, step=10):
        start = datetime.now()
        while datetime.now() - start < timedelta(seconds=timeout):
            for node in node_list:
                repair_beginning_message_results = node.grep_log(expr='starting user-requested repair')
                if repair_beginning_message_results:
                    return node
            sleep(step)
        return False

    @require("#2155")
    @attr('scylla-manager')
    def removing_managed_driver_during_repair_test(self):
        self.cluster.populate(3).start(wait_for_binary_proto=True, wait_other_notice=True)
        node1, node2, node3 = self.cluster.nodelist()
        self.cluster.stress(['write', 'n=1000K', '-rate', 'threads=50', '-pop', 'seq=10000001..20000000',
                             '-schema', 'replication(replication_factor=3)'])

        debug("Create Manager Tool instance to run scylla-manager operations")
        manager_tool = ScyllaManagerTool(scylla_manager=self.cluster._scylla_manager)
        cluster_name = "cluster1"
        mgr_cluster = manager_tool.add_cluster(node=node1, name=cluster_name)
        repair_task = mgr_cluster.create_repair_task()
        is_status_reached = repair_task.wait_for_status([TaskStatus.RUNNING])
        assert is_status_reached, "Timeout: The task {} did not start".format(repair_task.task_id)

        repaired_node = self._node_inwhich_repair_started([node1, node2, node3])
        debug(f"Chosen node: {repaired_node.name}")
        assert repaired_node, \
            "The manager started the repair task, yet could not find evidence of that in the cluster nodes"
        repair_task.stop()
        sleep(10)  # The manager waits 5 seconds for lingering repair threads before aborting the repair
        # Making sure that the repair that started in the cluster has stopped
        repair_ending_message_results = repaired_node.grep_log(expr="Aborted [0-9] repair job")
        assert repair_ending_message_results, "Stopping the repair through the manager did not stop the repair " \
                                              "in the cluster"

    @attr('scylla-manager')
    def removing_node_from_managed_cluster_test(self):
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
        info(f"Checking the status of CQL and REST after node '{node3.name}' is removed")
        assert node_details.cql.status == CqlStatus.DOWN, \
            f"The CQL status of node '{node3.name}' should be '{CqlStatus.DOWN}'"
        assert node_details.rest.status == HostRestStatus.DOWN, \
            f"The CQL status of node '{node3.name}' should be '{HostRestStatus.DOWN}'"
        info(f"Waiting until the status node '{node3.name}' changing to '{NodeStatus.DOWN}'")

        err_msg = f"The status of node '{node3.name}' should be '{NodeStatus.DOWN}'"
        wait_for(func=lambda: mgr_cluster.get_hosts_health()[node3.address()].node_status == NodeStatus.DOWN,
                 text=err_msg, step=step, timeout=timeout)

    def cluster_list(self):
        pass
