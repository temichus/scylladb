import copy
from concurrent.futures.thread import ThreadPoolExecutor
from random import randint
from time import sleep
import logging
import yaml
import os.path

import pytest
from filelock import FileLock
from cassandra import ConsistencyLevel
from cassandra.cluster import Session
from cassandra.concurrent import execute_concurrent_with_args
from ccmlib import scylla_repository
from ccmlib.scylla_cluster import ScyllaCluster, ScyllaNode

from tools.assertions import assert_all
from tools.cluster import new_node
from dtest_class import DtestTimeoutError, Tester, create_ks, create_cf
from dtest_setup import DTestSetup
from dtest_config import DTestConfig
from tools.misc import seconds_to_micros
from tools.data import simulate_write_process_in_minutes

logger = logging.getLogger(__name__)

upgrade_matrix_full_path = ['release:4.0', 'release:4.1', 'release:4.2', 'release:4.3', 'release:4.4', 'release:4.5']
upgrade_matrix_from_last_release_version = ['release:4.6']
upgrade_matrix_from_last_enterprise_release_version = ['release:2021.1']
upgrade_matrix_enterprise_full_path = ['release:2020.1', 'release:2021.1']
upgrade_matrix_for_raft_experimental = ['release:4.6']


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

    def get_timewindow_compaction_settings(self, optimize_enabled: bool = True):
        optimized_settings = ""
        if not optimize_enabled:
            optimized_settings = ",'enable_optimized_twcs_queries': false"

        return f"{{'class': 'TimeWindowCompactionStrategy', \
                'compaction_window_unit': 'MINUTES', \
                'compaction_window_size': 5 {optimized_settings}}}"

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

    def validate_twcs_data(self, session: Session, expected_results):
        current_results = self.get_twcs_data(session)
        assert current_results == expected_results

    def prepare_schema(self, session: Session, keyspace_name: str = 'ks', table_name: str = 'cf', rf: int = 3,
                       row_start_index: int = 1, row_end_index: int = 100):
        create_ks(session=session, name=keyspace_name, rf=rf)
        create_cf(session=session, name=table_name, key_type='int', columns={'val1': 'int', 'val2': 'int'})

        self.insert_rows(session=session, start=row_start_index, end=row_end_index)

    def prepare_twcs_schema(self, session: Session, keyspace_name: str = "ks", table_name: str = "cf_twcs", rf: int = 3):
        create_ks(session=session, name=keyspace_name, rf=rf)
        create_cf(session=session, name=f"{table_name}", key_name='pk', key_type='int',
                  compaction=self.get_timewindow_compaction_settings(),
                  columns={'ck': 'int', 'v': 'blob'}, primary_key='pk, ck')
        self.tw_pks, _ = simulate_write_process_in_minutes(self.cluster, session, keyspace_name, table_name)
        self.tw_data = self.get_twcs_data(session)

    def get_twcs_data(self, session: Session, max_time_minute: int = 20):
        queries = [
            f"SELECT * FROM ks.cf_twcs WHERE pk = {self.tw_pks[0]} and ck > {(max_time_minute - 5) * 60}",
            f"SELECT * FROM ks.cf_twcs WHERE pk = {self.tw_pks[-1]} and ck < {(max_time_minute - 15) * 60}",
            f"SELECT * FROM ks.cf_twcs WHERE pk = {self.tw_pks[len(self.tw_pks) // 2]} and ck > {(max_time_minute -1) * 60}",
        ]
        pk_set = ",".join([str(pk) for pk in self.tw_pks[3:7]])
        queries.append(f"SELECT * FROM ks.cf_twcs WHERE pk in ({pk_set}) and ck > {2 * 60} and ck < {4 * 60}")
        pk_set = ",".join([str(pk) for pk in self.tw_pks[len(self.tw_pks)-2: len(self.tw_pks)]])
        queries.append(f"SELECT * FROM ks.cf_twcs WHERE pk in ({pk_set}) and \
                       ck > {(max_time_minute - 6) * 60} and ck < {(max_time_minute - 5) * 60}")

        result = []
        for query in queries:
            res = list(session.execute(query))
            result.append(res)
        return result

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

    def test_upgrade_cluster_nodes_with_twcs(self, dtest_config):
        """
        Test upgrade all nodes in the cluster sequentially.
        Create schema with table with twcs
        Prefill the table before upgrade and validate the data is not corrupted
        during upgarde enable/disable optimized queries for twcs
        and validate that data no corrupted and returned same results
        """
        self.clone_upgrade_path(dtest_config)

        session = self.init_cluster(nodes=3)
        self.prepare_twcs_schema(session)
        expected_data = self.get_twcs_data(session)
        [node1, node2, node3] = self.cluster.nodelist()

        for version in self.current_upgrade_path:
            logger.info(f"****** START UPGRADE TEST FROM {node1.node_scylla_version} TO {version} ******")

            logger.info(f"Upgrade 1st node to from '{node1.node_scylla_version}' to '{version}' version")
            node1.upgrade(version)

            logger.info("Disable optimized queries")
            session.execute(
                f"ALTER TABLE ks.cf_twcs with compaction = {self.get_timewindow_compaction_settings(optimize_enabled=False)}")

            # Validate existent data
            self.validate_twcs_data(session, expected_data)

            logger.info(f"Upgrade 2nd node to from '{node2.node_scylla_version}' to '{version}' version")
            node2.upgrade(version)

            logger.info("Enable optimized queries")
            session.execute(
                f"ALTER TABLE ks.cf_twcs with compaction = {self.get_timewindow_compaction_settings(optimize_enabled=True)}")

            # Validate existent data
            self.validate_twcs_data(session, expected_data)

            logger.info(f"Upgrade 3rd node to from '{node3.node_scylla_version}' to '{version}' version")
            node3.upgrade(version)

            logger.info("Enable optimized queries")
            session.execute(
                f"ALTER TABLE ks.cf_twcs with compaction = {self.get_timewindow_compaction_settings(optimize_enabled=False)}")

            # Validate existent data
            self.validate_twcs_data(session, expected_data)

            logger.info("Enable optimized queries")
            session.execute(
                f"ALTER TABLE ks.cf_twcs with compaction = {self.get_timewindow_compaction_settings(optimize_enabled=True)}")

            # Validate existent data
            self.validate_twcs_data(session, expected_data)

            logger.info(f"****** FINISHED UPGRADE TO {version} ******")

        session.cluster.shutdown()


@pytest.mark.dtest_full
class TestUpgradeFrom40ToLast(BaseTests):
    __test__ = True

    upgrade_path = upgrade_matrix_full_path
    init_version = upgrade_path[0]

    @pytest.mark.skip("skip the test for this matrix")
    def test_one_node_upgrade(self):
        pass

    @pytest.mark.skip("skip the test for this matrix")
    def test_upgrade_cluster_nodes_with_twcs(self):
        pass


@pytest.mark.dtest_full
class TestUpgradeOneNode(BaseTests):
    __test__ = True

    upgrade_path = upgrade_matrix_from_last_release_version
    init_version = upgrade_path[0]

    @pytest.mark.skip("skip the test for this matrix")
    def test_cluster_upgrade(self):
        pass

    @pytest.mark.skip("skip the test for this matrix")
    def test_upgrade_cluster_nodes_with_twcs(self):
        pass


@pytest.mark.dtest_full
class TestUpgradeClusterWithEnableDisableTWCSQueries(BaseTests):
    __test__ = True

    upgrade_path = upgrade_matrix_from_last_release_version
    init_version = upgrade_path[0]

    @pytest.mark.skip("skip the test for this matrix")
    def test_cluster_upgrade(self):
        pass

    @pytest.mark.skip("skip the test for this matrix")
    def test_one_node_upgrade(self):
        pass


class TestUpgradeWithExperimentalRaft(BaseTests):
    __test__ = True

    upgrade_path = upgrade_matrix_for_raft_experimental
    init_version = upgrade_path[0]

    @pytest.mark.skip("skip the test for this matrix")
    def test_cluster_upgrade(self, dtest_config):
        pass

    @pytest.mark.skip("skip the test for this matrix")
    def test_one_node_upgrade(self, dtest_config):
        pass

    def test_upgrade_cluster_with_node_different_versions(self, dtest_config: DTestConfig):
        """
        Test scenario:
         1. create cluster with version without raft
         2. add new node with experimental feature - raft: disabled
         3. upgrade other nodes to new version
         4. enable raft on all nodes and restart one by one
         5. check data
        """
        self.clone_upgrade_path(dtest_config)

        cluster: ScyllaCluster = self.cluster
        logger.info(f"Init cluster with version {dtest_config.scylla_version}")
        session = self.init_cluster(nodes=3)
        logger.info("Create schema and insert data")
        self.prepare_schema(session)
        # add node with new version
        logger.info(f"Add node with version {self.upgrade_path[0]} to cluster")
        self.add_new_node(version=self.upgrade_path[0], dtest_config=dtest_config)
        logger.info("Validate data on cluster")
        self.validate_data(session=session, row_start_index=1, row_end_index=100, flush=True)

        logger.info(f"Upgrade nodes with version {dtest_config.scylla_version} to {self.upgrade_path[0]}")
        for node in cluster.nodelist()[:3]:
            node.upgrade(self.upgrade_path[0])

        logger.info("Validate data on cluster")
        self.validate_data(session=session, row_start_index=1, row_end_index=100, flush=True)

        self.enable_raft_experimental_per_node()

        logger.info("Insert and validate new data")
        self.insert_rows(session, start=100, end=200)
        self.validate_data(session, row_start_index=1, row_end_index=200, flush=True)

    def test_enable_raft_after_upgrade(self, dtest_config: DTestConfig):
        """
        Test scenario:
         1. create cluster with version without raft
         2. upgrade nodes to version with experimental raft
         3. enable raft on all nodes
         4. restart node one by one
         5. check data
        """
        self.clone_upgrade_path(dtest_config)

        self.init_cluster_upgrade_enable_raft(dtest_config)

        logger.info("Insert and verify data")
        session = self.get_session()
        self.insert_rows(session, start=100, end=200)
        self.validate_data(session, row_start_index=1, row_end_index=200, flush=True)

    def test_add_node_with_next_raft_enabling_to_upgraded_cluster_with_raft(self, dtest_config: DTestConfig):
        """
        Test scenario:
         1. create cluster with version without raft
         2. upgrade nodes to version with experimental raft
         3. enable raft on all nodes
         4. restart node one by one
         5. Add new node with disabled raft and enable raft
         5. check data
        """
        self.clone_upgrade_path(dtest_config)
        self.init_cluster_upgrade_enable_raft(dtest_config)

        logger.info(f"Add node with version {self.upgrade_path[0]} to cluster ")
        new_node = self.add_new_node(version=self.upgrade_path[0], dtest_config=dtest_config)
        logger.info("Enable raft experimental on new node")
        self.enable_raft_on_node(new_node)
        session = self.get_session(new_node)
        self.validate_data(session=session, row_start_index=1, row_end_index=100, flush=True)

        logger.info("Insert and verify data")
        session = self.get_session()
        self.insert_rows(session, start=100, end=200)
        self.validate_data(session, row_start_index=1, row_end_index=200, flush=True)

    def init_cluster_upgrade_enable_raft(self, dtest_config: DTestConfig):
        """Base case for cluster upgrade and enable raft

        Create cluster with base version,
        Populate with data
        Upgrade cluster per each node
        Enable raft per node.
        Verify dataset

        """
        cluster: ScyllaCluster = self.cluster
        logger.info(f"Init cluster with version {dtest_config.scylla_version}")
        session = self.init_cluster(nodes=3)
        self.prepare_schema(session)
        self.validate_data(session=session, row_start_index=1, row_end_index=100, flush=True)

        logger.info(f"Upgrade cluster version {dtest_config.scylla_version} to {self.upgrade_path[0]}")
        for node in cluster.nodelist():
            node.upgrade(self.upgrade_path[0])
        logger.info("Verify data after upgrade")
        self.validate_data(session=session, row_start_index=1, row_end_index=100, flush=True)
        self.enable_raft_experimental_per_node()
        self.validate_data(session=session, row_start_index=1, row_end_index=100, flush=True)

    def enable_raft_experimental_per_node(self):
        """ Enable experimental raft feature on cluster nodes
        """
        logger.info("Enable raft and restart node")
        for node in self.cluster.nodelist():
            self.enable_raft_on_node(node)
            session = self.patient_cql_connection(node)
            logger.info("Validate data after upgrade")

    @staticmethod
    def enable_raft_on_node(node):
        """ Enable raft on node
        Stop node
        Add config option to yaml
        Start node
        """
        node.stop(wait_other_notice=True)
        node.set_configuration_options({"experimental_features": ["raft"]})
        node.start(wait_other_notice=True)

    @staticmethod
    def _change_cluster_version(cluster: ScyllaCluster, version: str):
        logger.debug(f"Change cluster version to {version}")
        cdir, _ = scylla_repository.setup(version)
        cluster.set_install_dir(cdir)

    def add_new_node(self, version: str, dtest_config: DTestConfig) -> ScyllaNode:

        self._change_cluster_version(self.cluster, version)
        logger.info(f"Add new node to cluster with version {version}")
        node = new_node(self.cluster)

        self._change_cluster_version(self.cluster, dtest_config.scylla_version)
        logger.debug(f"Node scylla version: {node.node_scylla_version}")
        return node

    def _update_yaml_with_raft(self, node: ScyllaNode):
        scylla_yaml = os.path.join(node.get_conf_dir(), "scylla.yaml")
        with open(scylla_yaml, "r") as fp:
            data = yaml.safe_load(fp)

        data["experimental_features"] = ["raft"]

        with open(scylla_yaml, "w") as fp:
            yaml.safe_dump(data, fp)

        with open(scylla_yaml, "r") as fp:
            data = yaml.safe_load(fp)

    def get_session(self, node=None) -> Session:
        if not node:
            node = self.cluster.nodelist()[0]
        return self.patient_cql_connection(node)
