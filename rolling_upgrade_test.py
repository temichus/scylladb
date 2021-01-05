from concurrent.futures._base import Future
from concurrent.futures.thread import ThreadPoolExecutor

from ccmlib.scylla_node import ScyllaNode

from dtest import debug
from upgrade_test import UpgradeTester, upgrade_matrix_2


class RollingUpgradeTest(UpgradeTester):
    __test__ = True
    _multiprocess_can_split_ = False

    upgrade_path = upgrade_matrix_2
    init_version = upgrade_path[0]

    def test_rolling_upgrade(self):
        self.set_ignore_log_patterns()
        # Remove first version from the path as it'a already used
        self.current_upgrade_path.pop(0)

        session = self.init_cluster(nodes=3)
        self.prepare_schema(session)
        row_end_index = 100
        self.validate_data(session=session, row_start_index=1, row_end_index=row_end_index)

        base_node__version = self.init_version
        executor = ThreadPoolExecutor(max_workers=2)

        for version in self.current_upgrade_path:
            debug(f"****** START ROLLBACK TEST FROM {base_node__version} TO {version} ******")
            # Run write load in parallel with first node upgrade
            write_thread = self.run_stress(node=self.cluster.nodelist()[1],
                                           stress_command=self.write_stress_command(stress_duration_minutes=2, rf=3),
                                           executor=executor)

            # First node upgrade
            self.run_upgrade(node_index=0, upgrade_to_version=version, upgrade_type='upgrade')

            # Validate write stress
            self.validate_stress(stress_thread=write_thread, stress_type='write',
                                 ignore_msgs="Timed out waiting for server response Connection refused")

            # Start read stress load
            read_thread = self.run_stress(node=self.cluster.nodelist()[0],
                                          stress_command=self.read_stress_command(stress_duration_minutes=1),
                                          executor=executor)

            # Insert data and validate existent data
            row_end_index += 100
            self.insert_data_and_validate(session=session, row_end_index=row_end_index, flush=True)

            # Validate read stress
            self.validate_stress(stress_thread=read_thread, stress_type='read')

            # Start read stress load
            read_thread = self.run_stress(node=self.cluster.nodelist()[0],
                                          stress_command=self.read_stress_command(stress_duration_minutes=2),
                                          executor=executor)

            # Second node upgrade
            self.run_upgrade(node_index=1, upgrade_to_version=version, upgrade_type='upgrade')

            # Insert data and validate existent data
            row_end_index += 100
            self.insert_data_and_validate(session=session, row_end_index=row_end_index, flush=True)

            # Validate read stress
            self.validate_stress(stress_thread=read_thread, stress_type='read',
                                 ignore_msgs=" Timed out waiting for server response Connection refused")

            # Start read stress load
            read_thread = self.run_stress(node=self.cluster.nodelist()[0],
                                          stress_command=self.read_stress_command(stress_duration_minutes=2),
                                          executor=executor)

            # Second node rollback
            self.run_upgrade(node_index=1, upgrade_to_version=base_node__version, upgrade_type='rollback')

            # Insert data and validate existent data
            row_end_index += 100
            self.insert_data_and_validate(session=session, row_end_index=row_end_index, flush=True)

            # Validate read stress
            self.validate_stress(stress_thread=read_thread, stress_type='read',
                                 ignore_msgs=" Timed out waiting for server response Connection refused")

            # Upgrade 2d and 3th nodes
            self.run_upgrade(node_index=1, upgrade_to_version=version, upgrade_type='upgrade')
            self.run_upgrade(node_index=2, upgrade_to_version=version, upgrade_type='upgrade')

            debug(f"****** FINISHED ROLLBACK TEST FROM {base_node__version} "
                  f"TO {self.cluster.nodelist()[0].node_scylla_version} ******")

            base_node__version = version

        executor.shutdown()
        session.cluster.shutdown()

    @staticmethod
    def write_stress_command(stress_duration_minutes: int, rf: int = 3) -> list:
        return ["write", f"cl=QUORUM", f"duration={stress_duration_minutes}m",
                "-rate", "threads=10", "-log", "interval=5", "-schema",
                f"replication(factor={rf})"]

    @staticmethod
    def read_stress_command(stress_duration_minutes: int) -> list:
        return ['read', f'duration={stress_duration_minutes}m', "no-warmup", '-rate', 'threads=2',
                "-pop", "seq=1...10000"]

    @staticmethod
    def run_stress(node: ScyllaNode, stress_command: list, executor: ThreadPoolExecutor) -> Future:
        debug(f"Executing the following {stress_command[0]} stress command '{stress_command}'")
        return executor.submit(lambda: node.stress(stress_options=stress_command, capture_output=True))

    def run_upgrade(self, node_index: int, upgrade_to_version: str, upgrade_type: str):
        node_for_upgrade = self.cluster.nodelist()[node_index]
        debug(f"{upgrade_type.capitalize()} {node_for_upgrade.name} node to from  "
              f"'{node_for_upgrade.node_scylla_version}' to '{upgrade_to_version}' version")
        node_for_upgrade.upgrade(upgrade_to_version=upgrade_to_version)

    def validate_stress(self, stress_thread: Future, stress_type: str, ignore_msgs: str = '') -> None:
        ignore_err_msg = "com.datastax.driver.core.exceptions.WriteTimeoutException: Cassandra timeout during" \
                         " SIMPLE write query at consistency" \
                         f" replica were required but only{f' {ignore_msgs}' if ignore_msgs else ''}"

        debug(f"Waiting until {stress_type} stress thread will finish running")
        stdout, stderr = stress_thread.result()
        self.assertNotIn(member=ignore_err_msg, container=stderr,
                         msg=f"The following message '{ignore_err_msg}' found in stderr")
