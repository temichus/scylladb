# coding: utf-8
from datetime import datetime, timedelta

from nose.plugins.attrib import attr

from dtest_scylla_manager import TaskStatus, ScyllaManagerTool, ScyllaManagerMixin, CqlStatus, HostRestStatus, Memory, \
    Status, AlternatorStatus, NodeStatus, HostHealth
from dtest import Tester, debug, info
from alternator_utils import ALTERNATOR_PORT, WriteIsolation
from manager_backup_tests import CLUSTER_NAME


@attr('scylla-manager')
class ManagerHealthCheckTest(Tester, ScyllaManagerMixin):

    def get_manager_cluster(self):
        debug("Create Manager Tool instance to run scylla-manager operations")
        manager_tool = ScyllaManagerTool(scylla_manager=self.cluster._scylla_manager)
        cluster_name = "cluster1"
        debug("Add a cluster to scylla-manager, named: {}".format(cluster_name))
        manager_cluster = manager_tool.add_cluster(node=self.cluster.nodelist()[0], name=cluster_name)
        return manager_cluster

    @staticmethod
    def _check_healthcheck_updates(healthcheck_task):
        healthcheck_task.update(interval='60m')
        assert healthcheck_task.status != TaskStatus.ERROR, "Task interval update failed"
        assert '+1h' in healthcheck_task.next_run, "The interval of the task did not change to the requested interval"

        healthcheck_task.update(num_retries='2')
        assert healthcheck_task.status != TaskStatus.ERROR, "Task num-retries update failed"

        time_to_start_task = 35
        healthcheck_task.update(start_time='now+{}s'.format(time_to_start_task))
        assert healthcheck_task.status != TaskStatus.ERROR, "Task start-time update failed"

        now = datetime.now()
        list_next_run = healthcheck_task.next_run.split()
        next_run_time = datetime.strptime(" ".join(list_next_run[:4]), "%d %b %y %H:%M:%S")
        assert next_run_time - now > timedelta(seconds=time_to_start_task - 2), \
            "Healthcheck was not set to the proper time: {}   {}".format(next_run_time, now)

        healthcheck_task.wait_for_status(list_status=[TaskStatus.DONE, TaskStatus.RUNNING, TaskStatus.STARTING],
                                         timeout=time_to_start_task * 2, step=time_to_start_task / 2)

        healthcheck_task.update(enabled='false')
        assert healthcheck_task.status != TaskStatus.ERROR, "Task enabled update failed"
        assert healthcheck_task.is_task_disabled(), "The healthcheck test was not disabled"

    def auto_gen_health_check_task_test(self):
        """
            ver: 1.4
            verify that auto generated health check task is created and verify default interval
        """
        default_interval = "+15s"  # seconds
        self.config_and_create_cluster(nodes=2)
        manager_cluster = self.get_manager_cluster()
        healthcheck_task = manager_cluster.get_healthcheck_task()
        assert default_interval in healthcheck_task.next_run
        assert TaskStatus.ERROR.value not in healthcheck_task.status.value

    def update_health_check_task_test(self):
        """
            ver: 1.4
            verify that auto generated health check task can be updated
        """
        self.config_and_create_cluster(nodes=2)
        manager_cluster = self.get_manager_cluster()
        healthcheck_task = manager_cluster.get_healthcheck_task()

        self._check_healthcheck_updates(healthcheck_task)

    def test_down_node_isnt_pinged(self):
        """
        When a node is DN, the manager should not ping the agent to check the node's CQL and REST statuses,
        but instead it should just skip them, and in the output sctool cluster status it should just mark them as '-'
        """
        node1, node2, node3 = self.config_and_create_cluster(nodes=3)
        mgr_cluster = self._create_mgr_cluster(node=node1, name=CLUSTER_NAME)
        node3.stop(wait_other_notice=True)
        cluster_status = mgr_cluster.get_hosts_health()
        downed_node_data: HostHealth = cluster_status[node3.address()]
        regular_node_data: HostHealth = cluster_status[node1.address()]
        assert downed_node_data.cql == downed_node_data.rest == Status(),\
            "The manager pinged a node while it was DN, while it should skip any DN nodes"
        assert regular_node_data.cql.status == CqlStatus.UP and regular_node_data.rest.status == HostRestStatus.UP, \
            "The status of an UN node is not UP"

    def auto_gen_health_check_alternator_task_test(self):
        """
            ver: 2.2
            verify that auto generated alternator health check task is created and verify default interval
        """
        default_interval = "+15s"  # seconds

        self.config_and_create_cluster(nodes=2,
                                       extra_config_options=dict(alternator_port=ALTERNATOR_PORT,
                                                                 alternator_write_isolation=WriteIsolation.ALWAYS_USE_LWT.value))
        manager_cluster = self.get_manager_cluster()

        healthcheck_alternator_task = manager_cluster.get_healthcheck_alternator_task()
        assert default_interval in healthcheck_alternator_task.next_run
        assert TaskStatus.ERROR.value not in healthcheck_alternator_task.status.value

    def update_health_check_alternator_task_test(self):
        """
            ver: 2.2
            verify that auto generated alternator health check task can be updated
        """
        self.config_and_create_cluster(nodes=2,
                                       extra_config_options=dict(alternator_port=ALTERNATOR_PORT,
                                                                 alternator_write_isolation=WriteIsolation.ALWAYS_USE_LWT.value))
        manager_cluster = self.get_manager_cluster()
        healthcheck_alternator_task = manager_cluster.get_healthcheck_alternator_task()

        self._check_healthcheck_updates(healthcheck_alternator_task)

    def test_down_alternator_node_isnt_pinged(self):
        """
        ver: 2.2
        When a node is DN, the manager should not ping the agent to check the node's CQL, REST and Alternator statuses,
        but instead it should just skip them, and in the output sctool cluster status it should just mark them as '-'
        """
        node1, _, node3 = self.config_and_create_cluster(nodes=3, extra_config_options=dict(
            alternator_port=ALTERNATOR_PORT, alternator_write_isolation=WriteIsolation.ALWAYS_USE_LWT.value))
        mgr_cluster = self._create_mgr_cluster(node=node1, name=CLUSTER_NAME)
        node3.stop(wait_other_notice=True)
        cluster_status = mgr_cluster.get_hosts_health()
        downed_node_data: HostHealth = cluster_status[node3.address()]
        regular_node_data: HostHealth = cluster_status[node1.address()]
        assert downed_node_data.alternator == downed_node_data.cql == downed_node_data.rest == Status(), \
            "The manager pinged a node while it was DN, while it should skip any DN nodes"
        assert regular_node_data.alternator.status == AlternatorStatus.UP and \
            regular_node_data.cql.status == CqlStatus.UP and regular_node_data.rest.status == HostRestStatus.UP, \
            "The status of an UN node is not UP"

    def test_health_check_metrics(self):
        """
        Test healthcheck new metrics (OS CPU, total OS MEM, uptime and version of Scylla and Agent)
        """
        cluster_size = 3
        nodes = self.config_and_create_cluster(nodes=cluster_size)
        node1 = nodes[0]
        mgr_cluster = self._create_mgr_cluster(node=node1, name=CLUSTER_NAME)
        agent_version = mgr_cluster.scylla_manager.version.vstring
        scylla_version = self.cluster.version()

        info(f"Stopping the node '{node1.name}'")
        node1.stop()
        info("Extracting all cluster status details")
        cluster_status = mgr_cluster.get_hosts_health()

        for node in nodes:
            node_details: HostHealth = cluster_status[node.address()]
            if node is not node1:
                node_status = NodeStatus.UP
                assert node_details.node_status == node_status, \
                    f"The status of node '{node1.name}' is not in '{node_status}'"
                assert node_details.cql.status == CqlStatus.UP, f"The CQL is not in '{CqlStatus.UP}' status"
                assert node_details.rest.status == HostRestStatus.UP, f"The REST is not in '{HostRestStatus.UP}' status"
                assert str(node_details.scylla_version).startswith(scylla_version), \
                    f"The Scylla version does not contain the '{scylla_version}' prefix"
                assert node_details.agent_version == agent_version, \
                    f"The expected agent version should be '{agent_version}' and not '{node_details.agent_version}'"
            else:
                node_status = NodeStatus.DOWN
                empty_state = Status()
                assert node_details.node_status == node_status, \
                    f"The state of node '{node1.name}' is not in '{node_status}'"
                assert node_details.cql == empty_state, "The 'CQL' value is not empty"
                assert node_details.rest == empty_state, "The 'REST' value is not empty"
                assert node_details.cql == empty_state, "The 'Uptime' value is not empty"
                assert node_details.memory == Memory(), "The 'Memory' value is not empty"
                assert node_details.scylla_version is None, "The Scylla version should be empty"
                assert node_details.agent_version is None, "The agent version should be empty"
