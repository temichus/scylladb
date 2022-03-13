import pytest
import logging
import datetime
from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement

from dtest_class import Tester, create_ks
from dtest_scylla_manager import ScyllaManagerTool, ScyllaManagerMixin, TaskStatus, ScyllaManagerError

logger = logging.getLogger(__name__)


@pytest.mark.scylla_manager
class TestScyllaManagerTask(Tester, ScyllaManagerMixin):
    def _initiate_cluster(self):
        logger.debug("Starting cluster...")
        # Start a cluster of three nodes, and create a keyspace with RF=3, and
        self.cluster.set_configuration_options(values={'hinted_handoff_enabled': False})
        self.cluster.populate(3).start(wait_for_binary_proto=True, wait_other_notice=True)
        node1 = self.cluster.nodelist()[0]
        return node1

    def _initiate_cluster_with_data(self):
        logger.debug("Inserting data to cluster...")
        node1 = self._initiate_cluster()
        session = self.patient_cql_connection(node1)
        create_ks(session, 'ks', 3)

        session.execute("""CREATE TABLE cf (
        name text,
        pet text,
        age int,
        PRIMARY KEY ((name), pet)
        ) WITH compression = {} AND read_repair_chance = 0.0;""")

        query = SimpleStatement("INSERT INTO cf (name, pet, age) VALUES ('nadav', 'kitty', 5)",
                                consistency_level=ConsistencyLevel.ALL)
        session.execute(query)
        query = SimpleStatement("INSERT INTO cf (name, pet, age) VALUES ('nadav', 'adamdami', 1)",
                                consistency_level=ConsistencyLevel.ALL)
        session.execute(query)

    @pytest.mark.skip("Should be rewritten with cron time")
    def test_task_next_run(self):
        self._initiate_cluster()
        node1, node2, node3 = self.cluster.nodelist()

        logger.debug("Create Manager Tool instance to run scylla-manager operations")
        manager_tool = ScyllaManagerTool(scylla_manager=self.cluster._scylla_manager)
        cluster_name = "cluster1"
        logger.debug("Add a cluster to scylla-manager, named: {}".format(cluster_name))
        mgr_cluster = manager_tool.add_cluster(node=node1, name=cluster_name)

        # Test health-check task values
        logger.debug("Test cluster Health-Check task")
        healthcheck_task = mgr_cluster.get_healthcheck_task()
        next_run = healthcheck_task.next_run
        list_next_run = next_run.split()

        logger.debug("Health-check task next run is: {}".format(next_run))
        now = datetime.datetime.now()
        assert len(list_next_run) == 6
        assert int(list_next_run[0]) in [now.day, now.day+1, 1]
        assert list_next_run[5] == '(+15s)'

        # Test repair task values
        logger.debug("Test repair task")
        repair_task = mgr_cluster.repair_task_list[0]
        mgr_cluster.get_healthcheck_task()
        logger.debug("repair task status is: {}".format(repair_task.status))
        next_run = repair_task.next_run
        list_next_run = next_run.split()

        logger.debug("Repair task next run is: {}".format(next_run))
        now = datetime.datetime.now()
        assert len(list_next_run) == 6
        assert int(list_next_run[0]) in [now.day+1, 1]  # repair starts the next day of the month
        assert list_next_run[5] == '(+7d)'

    def test_aborted_task_has_end_time(self):
        node1, _ = self.config_and_create_cluster(nodes=2)
        manager_tool = ScyllaManagerTool(scylla_manager=self.cluster._scylla_manager)
        mgr_cluster = manager_tool.add_cluster(node=node1, name="cluster1")

        repair_task = mgr_cluster.repair_api.repair(cluster_name=mgr_cluster.id)
        repair_task.wait_for_status(list_status=[TaskStatus.RUNNING], step=1)
        manager_tool.restart_manager_server(gently=False)

        task_history = repair_task.history_list
        assert TaskStatus.from_str(task_history[0]["Status"]) == TaskStatus.ABORTED, "Task was not aborted as expected"
        assert task_history[0]["End time"], "And aborted task did not have an end time"

    def test_basic_task_naming(self):
        """
        New in manager 3.0.
        The test creates a task with a specified name, waits for it to end and check
        its progress using the unique name, expecting success.
        """
        task_name = "totally_original_name"
        node1, _ = self.config_and_create_cluster(nodes=2)
        manager_tool = ScyllaManagerTool(scylla_manager=self.cluster._scylla_manager)
        mgr_cluster = manager_tool.add_cluster(node=node1, name="cluster1")

        repair_task = mgr_cluster.repair_api.repair(cluster_name=mgr_cluster.id, name=task_name)
        final_status = repair_task.wait_and_get_final_status()
        assert final_status == TaskStatus.DONE
        task_start_time_by_id = repair_task.start_time
        stdout, _ = mgr_cluster.sctool.run(f"-c {mgr_cluster.id} progress repair/{task_name}",
                                           is_verify_errorless_result=True)
        start_time_checked_by_task_name = stdout[2][0]
        assert task_start_time_by_id in start_time_checked_by_task_name, \
            f"The start time printed when checking the task status by ID is {task_start_time_by_id}, which did not " \
            f"appear in the output when checking the progress by task name:\n{stdout}"

    def test_same_task_name_two_different_clusters(self, secondary_cluster):
        """
        New in manager 3.0.
        The test creates two managed clusters and creates a task by the same name
        in each of the two clusters, expecting success.
        """
        task_name = "totally_original_name"
        cluster1_nodes = self.config_and_create_cluster(nodes=2)
        manager_tool = ScyllaManagerTool(scylla_manager=self.cluster._scylla_manager)
        mgr_cluster = manager_tool.add_cluster(node=cluster1_nodes[0], name="cluster1")

        cluster2_nodes = self.config_and_create_cluster(nodes=2, cluster=secondary_cluster)
        secondary_mgr_cluster = self._create_mgr_cluster(node=cluster2_nodes[0], name="second_cluster")

        repair_task_cluster1 = mgr_cluster.repair_api.repair(cluster_name=mgr_cluster.id, name=task_name)
        repair_task_cluster2 = secondary_mgr_cluster.repair_api.repair(cluster_name=secondary_mgr_cluster.id,
                                                                       name=task_name)
        assert repair_task_cluster2.id, "The task in the second cluster does not have an ID"

    def test_check_status_by_task_type_single_task(self):
        """
        New in manager 3.0.
        The test starts a managed cluster (at which point the manager automatically creates a repair)
        and attempt to check the status of the repair by stating "repair" instead of
        the task's name/ID, expecting success.
        """
        node1, _ = self.config_and_create_cluster(nodes=2)
        manager_tool = ScyllaManagerTool(scylla_manager=self.cluster._scylla_manager)
        mgr_cluster = manager_tool.add_cluster(node=node1, name="cluster1")

        stdout, _ = mgr_cluster.sctool.run(f"-c {mgr_cluster.id} progress repair", is_verify_errorless_result=True)
        assert stdout[1][0] == "Status: NEW", f"The status of the automatic repair should be {TaskStatus.NEW}, but " \
                                              f"instead it's {stdout[1][0]}"
