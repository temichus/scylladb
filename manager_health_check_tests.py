# coding: utf-8
from dtest_scylla_manager import TaskStatus, ScyllaManagerTool
from dtest import Tester, debug


class ManagerHealthCheckTest(Tester):

    def create_2_nodes_cluster(self):
        self.cluster.populate(2).start(wait_for_binary_proto=False, wait_other_notice=False)

    def get_manager_cluster(self):
        debug("Create Manager Tool instance to run scylla-manager operations")
        manager_tool = ScyllaManagerTool(scylla_manager=self.cluster._scylla_manager)
        cluster_name = "cluster1"
        debug("Add a cluster to scylla-manager, named: {}".format(cluster_name))
        manager_cluster = manager_tool.add_cluster(node=self.cluster.nodelist()[0], name=cluster_name)
        return manager_cluster

    def auto_gen_health_check_task_test(self):
        """
            ver: 1.4
            verify that auto generated health check task is created and verify default interval
        """
        default_interval = "+15s"  # seconds
        self.create_2_nodes_cluster()
        manager_cluster = self.get_manager_cluster()
        healthcheck_task = manager_cluster.get_healthcheck_task()
        assert default_interval in healthcheck_task.next_run
        assert TaskStatus.ERROR.value not in healthcheck_task.status

    def update_health_check_task_test(self):
        """
            ver: 1.4
            verify that auto generated health check task can be updated
        """
        self.create_2_nodes_cluster()
        manager_cluster = self.get_manager_cluster()
        healthcheck_task = manager_cluster.get_healthcheck_task()

        healthcheck_task.update(interval='60m')
        assert TaskStatus.ERROR.value not in healthcheck_task.status

        healthcheck_task.update(num_retries='2')
        assert TaskStatus.ERROR.value not in healthcheck_task.status
        assert '+1h' in healthcheck_task.next_run

        healthcheck_task.update(start_time='now+35s')
        assert TaskStatus.ERROR.value not in healthcheck_task.status

        # TODO: wait for yaron to fix the wait_for_status
        # healthcheck_task.wait_for_status(list_status=[TaskStatus.DONE], timeout=60, step=3)

        healthcheck_task.update(enabled='false')
        assert TaskStatus.ERROR.value not in healthcheck_task.status



