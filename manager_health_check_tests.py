# coding: utf-8
from dtest_scylla_manager import TaskStatus, ScyllaManagerTool
from dtest import Tester, debug
from datetime import datetime, timedelta


class ManagerHealthCheckTest(Tester):

    def create_x_nodes_cluster(self, node_amount=2):
        self.cluster.populate(node_amount).start(wait_for_binary_proto=False, wait_other_notice=False)

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
        self.create_x_nodes_cluster()
        manager_cluster = self.get_manager_cluster()
        healthcheck_task = manager_cluster.get_healthcheck_task()
        assert default_interval in healthcheck_task.next_run
        assert TaskStatus.ERROR.value not in healthcheck_task.status

    def update_health_check_task_test(self):
        """
            ver: 1.4
            verify that auto generated health check task can be updated
        """
        self.create_x_nodes_cluster()
        manager_cluster = self.get_manager_cluster()
        healthcheck_task = manager_cluster.get_healthcheck_task()

        healthcheck_task.update(interval='60m')
        assert TaskStatus.ERROR.value not in healthcheck_task.status
        assert '+1h' in healthcheck_task.next_run, "The interval of the task did not change to the requested interval"

        healthcheck_task.update(num_retries='2')
        assert TaskStatus.ERROR.value not in healthcheck_task.status

        time_to_start_task = 35
        healthcheck_task.update(start_time='now+{}s'.format(time_to_start_task))
        assert TaskStatus.ERROR.value not in healthcheck_task.status

        now = datetime.now()
        list_next_run = healthcheck_task.next_run.split()
        next_run_time = datetime.strptime(" ".join(list_next_run[:4]), "%d %b %y %H:%M:%S")
        assert next_run_time - now > timedelta(seconds=time_to_start_task-1),\
            "Healthcheck was not set to the proper time: {}   {}".format(next_run_time, now)

        healthcheck_task.wait_for_status(list_status=[TaskStatus.DONE, TaskStatus.RUNNING, TaskStatus.STARTING],
                                         timeout=time_to_start_task*2, step=time_to_start_task/2)

        healthcheck_task.update(enabled='false')
        assert TaskStatus.ERROR.value not in healthcheck_task.status

    def healthcheck_while_one_node_down_test(self):
        self.create_x_nodes_cluster(node_amount=3)
        manager_cluster = self.get_manager_cluster()

        node = self.cluster.nodelist()[-1]
        node.stop()
        healthcheck_task = manager_cluster.get_healthcheck_task()
        healthcheck_task.start()
        a=1
