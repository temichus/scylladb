import logging
import tempfile
import time
from concurrent.futures._base import Future
from concurrent.futures.thread import ThreadPoolExecutor

import pytest
from ccmlib.scylla_node import ScyllaNode

from tools.stress import format_cs_output, assert_cs_success
from upgrade_test import UpgradeTester, upgrade_matrix_from_last_release_version

logger = logging.getLogger(__name__)


class RollingUpgradeBase(UpgradeTester):
    __test__ = False

    def test_rolling_upgrade(self, dtest_config):
        self.clone_upgrade_path(dtest_config)

        session = self.init_cluster(nodes=3)
        self.prepare_schema(session)
        row_end_index = 100
        self.validate_data(session=session, row_start_index=1, row_end_index=row_end_index)

        base_node__version = self.init_version
        executor = ThreadPoolExecutor(max_workers=2)

        for version in self.current_upgrade_path:
            logger.debug(f"****** START ROLLBACK TEST FROM {base_node__version} TO {version} ******")
            # Run write load in parallel with first node upgrade
            write_thread = self.run_stress(node=self.cluster.nodelist()[1],
                                           stress_command=self.write_stress_command(stress_duration_minutes=2, rf=3),
                                           executor=executor)

            # First node upgrade
            self.run_upgrade(node_index=0, upgrade_to_version=version, upgrade_type='upgrade')

            # Validate write stress
            self.validate_stress(stress_thread=write_thread, stress_type='write')

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
            self.validate_stress(stress_thread=read_thread, stress_type='read')

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
            self.validate_stress(stress_thread=read_thread, stress_type='read')

            # Upgrade 2d and 3th nodes
            self.run_upgrade(node_index=1, upgrade_to_version=version, upgrade_type='upgrade')
            self.run_upgrade(node_index=2, upgrade_to_version=version, upgrade_type='upgrade')

            # Upgrade sstables (if available)
            self.upgrade_and_verify_sstable()

            logger.debug(f"****** FINISHED ROLLBACK TEST FROM {base_node__version} "
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
        logger.debug(f"Executing the following {stress_command[0]} stress command '{stress_command}'")
        return executor.submit(lambda: node.stress(stress_options=stress_command, capture_output=True))

    def run_upgrade(self, node_index: int, upgrade_to_version: str, upgrade_type: str):
        node_for_upgrade = self.cluster.nodelist()[node_index]
        logger.debug(f"{upgrade_type.capitalize()} {node_for_upgrade.name} node to from  "
                     f"'{node_for_upgrade.node_scylla_version}' to '{upgrade_to_version}' version")

        if upgrade_type == "rollback":
            node_for_upgrade.rollback(upgrade_to_version=upgrade_to_version)
        elif upgrade_type == "upgrade":
            node_for_upgrade.upgrade(upgrade_to_version=upgrade_to_version)
        else:
            raise ValueError(f"Unsupported upgrade type value '{upgrade_type}'")

    def validate_stress(self, stress_thread: Future, stress_type: str) -> None:
        logger.debug(f"Waiting until {stress_type} stress thread will finish running")
        results = stress_thread.result()
        logger.debug(format_cs_output(results))
        assert_cs_success(results)

    def get_highest_supported_sstable_version(self):
        """
        find the highest sstable format version supported in the cluster

        :return:
        """
        output = []
        for node in self.cluster.nodelist():
            output.extend(node.get_node_supported_sstable_versions())
        return max(set(output))

    def upgradesstables_if_command_available(self):
        upgradesstables_available = []
        for node in self.cluster.nodelist():
            upgradesstables_available.append(node.upgradesstables_if_command_available())

        return all(upgradesstables_available)

    def upgradesstables(self):
        for node in self.cluster.nodelist():
            node.nodetool(cmd="upgradesstables -a")

    def wait_for_sstables_upgrade(self, expected_sstable_format_version, timeout=60):
        all_tables_upgraded = True

        logger.debug(("Start waiting for upgardesstables to finish"))
        start_time = time.time()
        finished = False
        while not finished:
            for node in self.cluster.nodelist():
                try:
                    sstable_versions = node.check_node_sstables_format()
                    assert len(sstable_versions) == 1, "expected all table format to be the same found {}".format(
                        sstable_versions)
                    assert list(sstable_versions)[0] == expected_sstable_format_version, \
                        "expected to format version to be '{}', found '{}'".format(
                            expected_sstable_format_version, list(sstable_versions)[0])
                except Exception:
                    if time.time() - start_time > timeout:
                        raise
                    all_tables_upgraded = False

            if all_tables_upgraded:
                finished = True

    def upgrade_and_verify_sstable(self):
        supported_sstable_version = self.get_highest_supported_sstable_version()
        upgradesstables_available = self.upgradesstables_if_command_available()
        if upgradesstables_available:
            logger.debug('Upgrading sstables if new version is available')
            self.upgradesstables()
            self.wait_for_sstables_upgrade(supported_sstable_version)

            # Verify sstabledump
            logger.debug('Starting sstabledump to verify correctness of sstables')
            json_path = tempfile.mktemp(suffix='.schema.json')
            with open(json_path, 'w') as fdw:
                data_json = self.cluster.nodelist()[0].run_sstable2json(out_file=fdw, keyspace='ks')
            with open(json_path, 'r') as fdr:
                data = fdr.read()

            assert data, "Failed to create sstable dump"


@pytest.mark.dtest_full
class TestRollingUpgrade(RollingUpgradeBase):
    __test__ = True
    _multiprocess_can_split_ = False

    upgrade_path = upgrade_matrix_from_last_release_version
    init_version = upgrade_path[0]
