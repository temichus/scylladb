import copy
from concurrent.futures.thread import ThreadPoolExecutor
from time import sleep
import logging

import pytest
from filelock import FileLock
from cassandra import ConsistencyLevel
from cassandra.cluster import Session
from cassandra.concurrent import execute_concurrent_with_args
from ccmlib import scylla_repository

from tools.assertions import assert_all
from dtest_class import Tester, create_ks, create_cf
from dtest_setup import DTestSetup
from dtest_config import DTestConfig

logger = logging.getLogger(__name__)

upgrade_matrix_full_path = ['release:4.0', 'release:4.1', 'release:4.2', 'release:4.3', 'release:4.4', 'release:4.5']
upgrade_matrix_from_last_release_version = ['release:4.5']


class UpgradeTester(Tester):
    __test__ = False

    init_version: str
    upgrade_path: list
    _version_under_test = ''
    current_upgrade_path: list

    @pytest.fixture(autouse=True)
    def fixture_add_additional_log_patterns(self, fixture_dtest_setup: DTestSetup):
        fixture_dtest_setup.allow_log_errors = True
        fixture_dtest_setup.ignore_log_patterns += [
            # from sdcm.sct_events.group_common_events.ignore_upgrade_schema_errors
            "Failed to load schema",
            "Failed to pull schema",
            # https://github.com/scylladb/scylla/issues/7817
            r'Could not retrieve CDC streams with timestamp',
        ]

    @pytest.fixture(scope='session')
    def dtest_config(self, request, tmp_path_factory, worker_id):
        """
        override dtest_config fixture, so we should start the initial cluster the the correct release version (i.e. not the version under test)
        also in charge of making sure we downloaded and cached all the needed versions
        """
        dtest_config = DTestConfig()
        dtest_config.setup(request)

        self.add_current_version_to_upgrade_path(dtest_config)
        # put the current_upgrade_path on the dtest_config related to this class,
        # so it can be clone later in the test
        # using `self.clone_upgrade_path()`
        dtest_config.current_upgrade_path = self.create_upgrade_path()

        if worker_id == "master":
            # not executing in with multiple workers, just produce the data and let
            # pytest's fixture caching do its job
            self.download_all_relocatables(dtest_config.current_upgrade_path)
        else:
            # get the temp directory shared by all workers
            root_tmp_dir = tmp_path_factory.getbasetemp().parent

            fn = root_tmp_dir / f"download_version_for_{str(self.__class__.__name__)}"
            with FileLock(str(fn) + ".lock", timeout=20 * 60.0):  # lock for 20min max
                if not fn.is_file():
                    self.download_all_relocatables(dtest_config.current_upgrade_path)
                    fn.touch()

        dtest_config.scylla_version = self.init_version
        yield dtest_config

    def clone_upgrade_path(self, dtest_config):
        self.current_upgrade_path = copy.deepcopy(dtest_config.current_upgrade_path)
        logger.debug(f"current_upgrade_path: {self.current_upgrade_path}")

        # Remove first version from the path as it'a already used
        self.current_upgrade_path.pop(0)
        logger.debug(f"current_upgrade_path after pop: {self.current_upgrade_path}")

    def init_cluster(self, nodes: int) -> Session:
        self.cluster.populate(nodes).start(wait_for_binary_proto=True)
        session = self.patient_cql_connection(self.cluster.nodelist()[0])
        return session

    @staticmethod
    def download_all_relocatables(current_upgrade_path):
        logger.info(f"Prepare (download) all versions start: {current_upgrade_path}")

        with ThreadPoolExecutor(max_workers=len(current_upgrade_path),
                                thread_name_prefix='RelocatableDownload') as tp:
            threads = []
            for version in current_upgrade_path:
                logger.info(f"Download relocatables for version: {version}")
                threads.append(tp.submit(scylla_repository.setup, version))
                # Allow to start download before call the next
                sleep(3)

            for thread in threads:
                thread.result(timeout=3600)

        logger.info("Prepare (download) all versions finished")

    def validate_data(self, session: Session, row_start_index: int, row_end_index: int, flush: bool = True) -> None:
        if flush:
            self.cluster.flush()

        assert_all(session=session,
                   query="select key, val1, val2 from ks.cf",
                   expected=self.data(start=row_start_index, end=row_end_index),
                   cl=ConsistencyLevel.QUORUM,
                   ignore_order=True)

    def prepare_schema(self, session: Session, keyspace_name: str = 'ks', table_name: str = 'cf', rf: int = 3,
                       row_start_index: int = 1, row_end_index: int = 100):
        create_ks(session=session, name=keyspace_name, rf=rf)
        create_cf(session=session, name=table_name, key_type='int', columns={'val1': 'int', 'val2': 'int'})
        self.insert_rows(session=session, start=row_start_index, end=row_end_index)

    def insert_rows(self, session: Session, start: int, end: int, keyspace_name: str = 'ks', cf: str = 'cf') -> None:
        logger.info(f"Insert rows from {start} to {end}")
        insert_statement = session.prepare(f"INSERT INTO {keyspace_name}.{cf} (key, val1, val2) VALUES (?, ?, ?)")
        args = self.data(start, end)
        execute_concurrent_with_args(session, insert_statement, args, concurrency=20)

    def insert_data_and_validate(self, session: Session, row_end_index: int, flush: bool = True) -> None:
        logger.info("Add more 100 rows")
        self.insert_rows(session=session, start=row_end_index - 100, end=row_end_index)
        self.validate_data(session=session, row_start_index=1, row_end_index=row_end_index, flush=flush)

    def data(self, start: int, end: int) -> list:
        return [[r, r + 1, r + 2] for r in range(start, end)]

    def current_version(self, dtest_config) -> str:
        version_under_test = dtest_config.scylla_version
        assert version_under_test, "Expected SCYLLA_VERSION parameter but it isn't supplied. The test can't be run"
        return version_under_test

    def add_current_version_to_upgrade_path(self, dtest_config) -> None:
        current_version = self.current_version(dtest_config)
        if not current_version in self.upgrade_path:
            self.upgrade_path.append(current_version)

    def create_upgrade_path(self) -> list:
        current_upgrade_path = copy.deepcopy(self.upgrade_path)
        return current_upgrade_path


class BaseTests(UpgradeTester):
    __test__ = False

    def test_cluster_upgrade(self, dtest_config):
        """
        Test upgrade all nodes in the cluster sequentially.
        Prefill the table before upgrade and validate the data is not corrupted
        """
        self.clone_upgrade_path(dtest_config)

        session = self.init_cluster(nodes=3)
        self.prepare_schema(session)
        row_end_index = 100
        self.validate_data(session=session, row_start_index=1, row_end_index=row_end_index)

        node1 = self.cluster.nodelist()[0]

        for version in self.current_upgrade_path:
            logger.info(f"****** START UPGRADE TEST FROM {node1.node_scylla_version} TO {version} ******")

            logger.info(f"Upgrade all nodes to from '{node1.node_scylla_version}' to '{version}' version")
            self.cluster.upgrade_cluster(version)

            # Validate existent data
            self.validate_data(session=session, row_start_index=1, row_end_index=row_end_index, flush=False)

            logger.info("Add more 100 rows")
            row_end_index += 100
            self.insert_data_and_validate(session=session, row_end_index=row_end_index, flush=True)

            logger.info(f"Cluster has been upgraded. "
                        f"Current cluster version is {node1.node_scylla_version}")

            logger.info(f"****** FINISHED UPGRADE TO {version} ******")

        session.cluster.shutdown()

    def test_one_node_upgrade(self, dtest_config):
        """
        Test upgrade one node.
        1. Prefill the table before upgrade
        2. Upgrade one node
        3. Validate the data is not corrupted
        4. Add new data and validate
        """
        self.clone_upgrade_path(dtest_config)

        session = self.init_cluster(nodes=3)
        self.prepare_schema(session)
        row_end_index = 100
        self.validate_data(session=session, row_start_index=1, row_end_index=row_end_index)

        node_for_upgrade = self.cluster.nodelist()[1]

        for version in self.current_upgrade_path:
            logger.info(f"****** START UPGRADE TEST FROM {node_for_upgrade.node_scylla_version} TO {version} ******")
            logger.info(
                f"Upgrade {node_for_upgrade.name} node to from '{node_for_upgrade.node_scylla_version}' to '{version}' version")
            node_for_upgrade.upgrade(upgrade_to_version=version)

            # Validate existent data
            self.validate_data(session=session, row_start_index=1, row_end_index=row_end_index, flush=False)

            logger.info("Add more 100 rows")
            row_end_index += 100
            self.insert_data_and_validate(session=session, row_end_index=row_end_index, flush=True)

            logger.info(f"Node {node_for_upgrade.name} has been upgraded. "
                        f"Current node version is {node_for_upgrade.node_scylla_version}")

            logger.info(f"****** FINISHED UPGRADE TO {version}******")

        session.cluster.shutdown()


@pytest.mark.dtest_full
class TestUpgradeFrom40ToLast(BaseTests):
    __test__ = True

    upgrade_path = upgrade_matrix_full_path
    init_version = upgrade_path[0]

    @pytest.mark.skip("skip the test for this matrix")
    def test_one_node_upgrade(self):
        pass


@pytest.mark.dtest_full
class TestUpgradeOneNode(BaseTests):
    __test__ = True

    upgrade_path = upgrade_matrix_from_last_release_version
    init_version = upgrade_path[0]

    @pytest.mark.skip("skip the test for this matrix")
    def test_cluster_upgrade(self):
        pass
