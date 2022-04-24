import time
import pytest
import logging

from dtest_scylla_manager import TaskStatus, ScyllaManagerError
from dtest_class import Tester
from dtest_scylla_manager import ScyllaManagerMixin

CLUSTER_NAME = 'cluster1'
logger = logging.getLogger(__name__)


@pytest.mark.scylla_manager
class TestScyllaManagerSuspension(Tester, ScyllaManagerMixin):

    def test_create_task_while_suspended(self):
        self.config_and_create_cluster(3)
        mgr_cluster = self._create_mgr_cluster(self.cluster.nodelist()[0], name=CLUSTER_NAME)

        mgr_cluster.suspend()
        try:
            mgr_cluster.repair_api.repair(cluster_name=mgr_cluster.id)
        except ScyllaManagerError as err:
            assert "suspended" in err.args[0].lower() and "failed to create task" in err.args[0].lower(), \
                f"Task creation failed, as expected, but not with proper error message: {err.args[0]}"
        else:
            raise AssertionError("Test creation while the manager is suspended did not fail")

    def test_suspend_on_resume_start_tasks_without_duration(self):
        """
        New in manager 3.0

        The on-resume-start-tasks flag in the sctool suspend command is meant to be used in conjunction
        with the duration flag.
        If on-resume-start-tasks is used without duration, the command should fail. The test makes sure
        of that.
        """
        self.config_and_create_cluster(nodes=2)
        mgr_cluster = self._create_mgr_cluster(self.cluster.nodelist()[0], name=CLUSTER_NAME)

        try:
            mgr_cluster.suspend(on_resume_start_tasks=True)
        except ScyllaManagerError as err:
            assert "duration" in err.args[0].lower(), \
                f"Suspending the cluster with the 'on-resume-start-tasks' flag but without  duration failed, but " \
                f"without a proper error message: {err.args[0]}"
        else:
            raise ScyllaManagerError("Suspending the cluster with 'on-resume-start-tasks' but without 'duration' "
                                     "did not fail")

    def test_create_scheduled_task_while_suspended(self):
        """
        When suspended, the manager lets the user scheduled task only at least 8 hours in the future.
        Otherwise, creation will fail.
        """
        self.config_and_create_cluster(3)
        mgr_cluster = self._create_mgr_cluster(self.cluster.nodelist()[0], name=CLUSTER_NAME)

        mgr_cluster.suspend()
        try:
            repair_task_too_soon = mgr_cluster.repair_api.repair(cluster_name=mgr_cluster.id, start_date="now+2m")
        except ScyllaManagerError as err:
            assert "suspended" in err.args[0].lower() and "failed to create task" in err.args[0].lower() \
                   and "8h" in err.args[0].lower(), \
                f"Task creation withing the next 8 hours failed, as expected, but not with proper error message: " \
                f"{err.args[0]}"
        else:
            raise AssertionError("Test creation withing the next 8 hours while the manager is suspended did not fail")
        try:
            repair_task_proper_time = mgr_cluster.repair_api.repair(cluster_name=mgr_cluster.id, start_date="now+8h2m")
        except ScyllaManagerError as err:
            raise AssertionError(f"Creating a task more than 8 hours in the future failed: {err.args}")
        except Exception:
            raise

    @pytest.mark.require("scylla-manager/#2496")
    def test_schedule_task_to_run_while_suspended(self):
        self.config_and_create_cluster(3)
        mgr_cluster = self._create_mgr_cluster(self.cluster.nodelist()[0], name=CLUSTER_NAME)
        repair_task = mgr_cluster.repair_api.repair(cluster_name=mgr_cluster.id, start_date="now+2m")

        mgr_cluster.suspend()
        time.sleep(200)
        repair_task_next_run = repair_task.next_run
        assert repair_task_next_run == "[SUSPENDED]", \
            f'Task that was set to run while the manager was suspended has a Next Run value of ' \
            f'"{repair_task_next_run}" instead of the expected [SUSPENDED]'
        mgr_cluster.resume()

        assert not len(repair_task.history), f"The task has ran while the manager was suspended"
        repair_task_next_run = repair_task.next_run
        repair_task_status = repair_task.status
        assert not repair_task_next_run, \
            f"The task Has a destined run time of {repair_task_next_run} even though it's not suppose to run at all"
        assert repair_task_status == TaskStatus.SKIPPED, \
            f"The task was expected to reach SKIPPED status, instead it reached {str(repair_task_status)}"
