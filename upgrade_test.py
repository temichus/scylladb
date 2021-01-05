import copy
import os
import tempfile
from concurrent.futures.thread import ThreadPoolExecutor
from time import sleep

from cassandra import ConsistencyLevel
from cassandra.cluster import Session
from cassandra.concurrent import execute_concurrent_with_args

from ccmlib import scylla_repository
from ccmlib.scylla_cluster import ScyllaCluster

from assertions import assert_all
from dtest import Tester, debug


upgrade_matrix_1 = ['release:4.0', 'release:4.1', 'release:4.2', 'release:4.3']
upgrade_matrix_2 = ['release:4.3']


class UpgradeTester(Tester):
    __test__ = False

    init_version = None
    upgrade_path = None
    _version_under_test = ''
    current_upgrade_path = None

    def set_ignore_log_patterns(self):
        self.ignore_log_patterns = [
            # from sdcm.sct_events.group_common_events.ignore_upgrade_schema_errors
            "Failed to load schema",
            "Failed to pull schema",
            # https://github.com/scylladb/scylla/issues/7817
            r'Could not retrieve CDC streams with timestamp',
        ]

    def get_cluster(self, name: str = 'test', version: str = None) -> ScyllaCluster:
        self.add_current_version_to_upgrade_path()
        self.current_upgrade_path = self.create_upgrade_path()
        self.download_all_relocatables()

        dtest_root = os.path.join(os.path.expanduser("~"), '.dtest')
        if not os.path.exists(dtest_root):
            os.makedirs(dtest_root)
        self.test_path = tempfile.mkdtemp(dir=dtest_root, prefix='dtest-')

        debug(f"Starting Scylla cluster version {self.init_version}")
        cluster = ScyllaCluster(self.test_path, name, cassandra_version=self.init_version,
                                force_wait_for_cluster_start=True)
        id = self.cluster_id_allocator.alloc(self.test_path)
        cluster.set_id(id)
        cluster.set_ipprefix("127.0.%d." % id)
        return cluster

    def init_cluster(self, nodes: int) -> Session:
        self.cluster.populate(nodes).start(wait_for_binary_proto=True)
        session = self.patient_cql_connection(self.cluster.nodelist()[0])
        return session

    def download_all_relocatables(self):
        debug(f"Prepare (download) all versions start: {self.current_upgrade_path}")

        with ThreadPoolExecutor(max_workers=len(self.current_upgrade_path),
                                thread_name_prefix='RelocatableDownload') as tp:
            threads = []
            for version in self.current_upgrade_path:
                debug(f"Download relocatables for version: {version}")
                threads.append(tp.submit(scylla_repository.setup, version))
                # Allow to start download before call the next
                sleep(3)

            for thread in threads:
                thread.result(timeout=1800)

        debug("Prepare (download) all versions finished")

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
        self.create_ks(session=session, name=keyspace_name, rf=rf)
        self.create_cf(session=session, name=table_name, key_type='int', columns={'val1': 'int', 'val2': 'int'})
        self.insert_rows(session=session, start=row_start_index, end=row_end_index)

    def insert_rows(self, session: Session, start: int, end: int, keyspace_name: str = 'ks', cf: str = 'cf') -> None:
        debug(f"Insert rows from {start} to {end}")
        insert_statement = session.prepare(f"INSERT INTO {keyspace_name}.{cf} (key, val1, val2) VALUES (?, ?, ?)")
        args = self.data(start, end)
        execute_concurrent_with_args(session, insert_statement, args, concurrency=20)

    def insert_data_and_validate(self, session: Session, row_end_index: int, flush: bool = True) -> None:
        debug("Add more 100 rows")
        self.insert_rows(session=session, start=row_end_index - 100, end=row_end_index)
        self.validate_data(session=session, row_start_index=1, row_end_index=row_end_index, flush=flush)

    def data(self, start: int, end: int) -> list:
        return [[r, r + 1, r + 2] for r in range(start, end)]

    def current_version(self) -> str:
        version_under_test = os.environ.get('SCYLLA_VERSION', None)
        self.assertTrue(version_under_test, "Expected SCYLLA_VERSION parameter but it isn't supplied. "
                                            "The test can't be run")
        return version_under_test

    def add_current_version_to_upgrade_path(self) -> None:
        current_version = self.current_version()
        if not current_version in self.upgrade_path:
            self.upgrade_path.append(current_version)

    def create_upgrade_path(self) -> list:
        current_upgrade_path = copy.deepcopy(self.upgrade_path)
        return current_upgrade_path


class BaseTests(UpgradeTester):
    __test__ = False

    def test_cluster_upgrade(self):
        """
        Test upgrade all nodes in the cluster sequentially.
        Prefill the table before upgrade and validate the data is not corrupted
        """
        self.set_ignore_log_patterns()
        debug(f"current_upgrade_path: {self.current_upgrade_path}")
        # Remove first version from the path as it'a already used
        self.current_upgrade_path.pop(0)
        debug(f"current_upgrade_path after pop: {self.current_upgrade_path}")

        session = self.init_cluster(nodes=3)
        self.prepare_schema(session)
        row_end_index = 100
        self.validate_data(session=session, row_start_index=1, row_end_index=row_end_index)

        node1 = self.cluster.nodelist()[0]

        for version in self.current_upgrade_path:
            debug(f"****** START UPGRADE TEST FROM {node1.node_scylla_version} TO {version} ******")

            debug(f"Upgrade all nodes to from '{node1.node_scylla_version}' to '{version}' version")
            self.cluster.upgrade_cluster(version)

            # Validate existent data
            self.validate_data(session=session, row_start_index=1, row_end_index=row_end_index, flush=False)

            debug("Add more 100 rows")
            row_end_index += 100
            self.insert_data_and_validate(session=session, row_end_index=row_end_index, flush=True)

            debug(f"Cluster has been upgraded. "
                  f"Current cluster version is {node1.node_scylla_version}")

            debug(f"****** FINISHED UPGRADE TO {version} ******")

        session.cluster.shutdown()

    def test_one_node_upgrade(self):
        """
        Test upgrade one node.
        1. Prefill the table before upgrade
        2. Upgrade one node
        3. Validate the data is not corrupted
        4. Add new data and validate
        """
        self.set_ignore_log_patterns()
        # Remove first version from the path as it'a already used
        self.current_upgrade_path.pop(0)

        session = self.init_cluster(nodes=3)
        self.prepare_schema(session)
        row_end_index = 100
        self.validate_data(session=session, row_start_index=1, row_end_index=row_end_index)

        node_for_upgrade = self.cluster.nodelist()[1]

        for version in self.current_upgrade_path:
            debug(f"****** START UPGRADE TEST FROM {node_for_upgrade.node_scylla_version} TO {version} ******")
            debug(
                f"Upgrade {node_for_upgrade.name} node to from '{node_for_upgrade.node_scylla_version}' to '{version}' version")
            node_for_upgrade.upgrade(upgrade_to_version=version)

            # Validate existent data
            self.validate_data(session=session, row_start_index=1, row_end_index=row_end_index, flush=False)

            debug("Add more 100 rows")
            row_end_index += 100
            self.insert_data_and_validate(session=session, row_end_index=row_end_index, flush=True)

            debug(f"Node {node_for_upgrade.name} has been upgraded. "
                  f"Current node version is {node_for_upgrade.node_scylla_version}")

            debug(f"****** FINISHED UPGRADE TO {version}******")

        session.cluster.shutdown()


class UpgradeTestFrom40ToLast(BaseTests):
    __test__ = True
    _multiprocess_can_split_ = False

    upgrade_path = upgrade_matrix_1
    init_version = upgrade_path[0]
