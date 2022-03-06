import pytest
import logging
import datetime
from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement

from dtest_class import Tester, create_ks
from dtest_scylla_manager import ScyllaManagerTool, ScyllaManagerMixin, TaskStatus

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
