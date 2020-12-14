import copy
import os
import tempfile
from concurrent.futures._base import Future
from concurrent.futures.thread import ThreadPoolExecutor

from cassandra import ConsistencyLevel
from cassandra.cluster import Session
from cassandra.concurrent import execute_concurrent_with_args
from ccmlib.scylla_cluster import ScyllaCluster
from ccmlib.scylla_node import ScyllaNode

from assertions import assert_all
from dtest import Tester, debug


class UpgradeTester(Tester):
    __test__ = False
    init_version = None
    upgrade_path = []
    _version_under_test = ''

    def set_ignore_log_patterns(self):
        self.ignore_log_patterns = [
            # from sdcm.sct_events.group_common_events.ignore_upgrade_schema_errors
            "Failed to load schema",
            "Failed to pull schema",
            # https://github.com/scylladb/scylla/issues/7817
            r'Could not retrieve CDC streams with timestamp',
        ]

    def get_cluster(self, name: str = 'test', version: str = None) -> ScyllaCluster:
        dtest_root = os.path.join(os.path.expanduser("~"), '.dtest')
        if not os.path.exists(dtest_root):
            os.makedirs(dtest_root)
        self.test_path = tempfile.mkdtemp(dir=dtest_root, prefix='dtest-')

        debug("Starting Scylla cluster version {}".format(self.init_version))
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
        self.upgrade_path.append(current_version)

    def create_upgrade_path(self) -> list:
        ...


class BaseTests(UpgradeTester):
    __test__ = False

    def test_cluster_upgrade(self):
        """
        Test upgrade all nodes in the cluster sequentially.
        Prefill the table before upgrade and validate the data is not corrupted
        """
        self.set_ignore_log_patterns()
        self.add_current_version_to_upgrade_path()
        current_upgrade_path = self.create_upgrade_path()

        session = self.init_cluster(nodes=3)
        self.prepare_schema(session)
        row_end_index = 100
        self.validate_data(session=session, row_start_index=1, row_end_index=row_end_index)

        node1 = self.cluster.nodelist()[0]

        for version in current_upgrade_path:
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
        self.add_current_version_to_upgrade_path()
        current_upgrade_path = self.create_upgrade_path()

        session = self.init_cluster(nodes=3)
        self.prepare_schema(session)
        row_end_index = 100
        self.validate_data(session=session, row_start_index=1, row_end_index=row_end_index)

        node_for_upgrade = self.cluster.nodelist()[1]

        for version in current_upgrade_path:
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
    upgrade_path = ['release:4.0', 'release:4.1', 'release:4.2', 'release:4.3']
    init_version = upgrade_path[0]

    def create_upgrade_path(self) -> list:
        current_upgrade_path = copy.deepcopy(self.upgrade_path)
        current_upgrade_path.pop(0)
        return current_upgrade_path


class UpgradeTestFrom41ToLast(BaseTests):
    __test__ = True
    upgrade_path = ['release:4.1', 'release:4.2', 'release:4.3']
    init_version = upgrade_path[0]

    def create_upgrade_path(self) -> list:
        current_upgrade_path = copy.deepcopy(self.upgrade_path)
        current_upgrade_path.pop(0)
        return current_upgrade_path


class RollingUpgradeTest(UpgradeTester):
    __test__ = True
    upgrade_path = ['release:4.3']
    init_version = upgrade_path[0]

    def create_upgrade_path(self) -> list:
        current_upgrade_path = copy.deepcopy(self.upgrade_path)
        current_upgrade_path.pop(0)
        return current_upgrade_path

    def test_rolling_upgrade(self):
        self.set_ignore_log_patterns()
        self.add_current_version_to_upgrade_path()
        current_upgrade_path = self.create_upgrade_path()

        session = self.init_cluster(nodes=3)
        self.prepare_schema(session)
        row_end_index = 100
        self.validate_data(session=session, row_start_index=1, row_end_index=row_end_index)

        base_node__version = self.init_version
        executor = ThreadPoolExecutor(max_workers=2)

        for version in current_upgrade_path:
            debug(f"****** START ROLLBACK TEST FROM {base_node__version} TO {version} ******")
            # Run write load in parallel with first node upgrade
            # Letto c-s run 5 minutes - first time running will download relocatable packages
            write_thread = self.run_stress(node=self.cluster.nodelist()[1],
                                           stress_command=self.write_stress_command(stress_duration_minutes=5, rf=3),
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

            # Run repair on all nodes
            self.cluster.repair()

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
