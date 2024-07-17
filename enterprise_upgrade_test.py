import logging
import time

import pytest
import requests
from cassandra import InvalidRequest
from ccmlib.node import Status

from dtest_class import create_cf, create_ks, get_ip_from_node, read_barrier, wait_for
from rolling_upgrade_test import RollingUpgradeBase
from upgrade_test import upgrade_matrix_from_last_enterprise_release_version, BaseTests, \
    upgrade_matrix_enterprise_full_path, upgrade_matrix_from_last_release_version

logger = logging.getLogger(__name__)


@pytest.mark.dtest_full
@pytest.mark.dtest_enterprise
class TestEnterpriseRollingUpgrade(RollingUpgradeBase):
    __test__ = True
    _multiprocess_can_split_ = False
    upgrade_path = upgrade_matrix_from_last_enterprise_release_version
    init_version = upgrade_path[0]


@pytest.mark.dtest_full
@pytest.mark.dtest_enterprise
class TestEnterpriseUpgradeBetweenReleases(BaseTests):
    __test__ = True
    _multiprocess_can_split_ = False
    upgrade_path = upgrade_matrix_enterprise_full_path
    init_version = upgrade_path[0]

    @pytest.mark.skip("skip the test for this matrix")
    def test_one_node_upgrade(self):
        pass


@pytest.mark.dtest_full
@pytest.mark.dtest_enterprise
class TestEnterpriseUpgradeFromOSS(BaseTests):
    __test__ = True
    _multiprocess_can_split_ = False
    upgrade_path = upgrade_matrix_from_last_release_version
    init_version = upgrade_path[0]

    roles = ["role1", "role2", "role3"]

    def connect_sessions(self):
        return [self.patient_exclusive_cql_connection(node, user="cassandra", password="cassandra") for node in self.cluster.nodelist()]

    def is_raft_service_levels(self, session):
        row = session.execute("SELECT value FROM system.scylla_local WHERE key = 'service_level_version'").one()
        if row:
            return row.value == "2"
        else:
            return False

    def create_roles(self, session):
        for role in self.roles:
            session.execute(f"CREATE ROLE {role} WITH login=true AND password='{role}' AND SUPERUSER=true")

    def connect_clients(self):
        clients = {}
        for role in self.roles:
            clients[role] = self.patient_cql_connection(self.cluster.nodelist()[0], user=role, password=role)
        return clients

    @staticmethod
    def role_to_sl_name(role_name):
        return f"sl{role_name[4]}"

    # Checks if shares column is present in service levels v1 (system_distributed) table
    def check_shares_column_added(self, cql, table):
        desc = cql.execute(f"DESCRIBE TABLE {table}").one()
        return "shares int" in desc.create_statement

    def validate_connections_scheduling_groups(self, nodes):
        for node in nodes:
            # Sample result of /service_levels/count_connections:
            # {'sl:test_sl': {'test_role': 3}, 'sl:default': {'cassandra': 3}}
            response = requests.get(f"http://{get_ip_from_node(node=node)}:10000/service_levels/count_connections")
            sg_connections_map = response.json()
            for role in self.roles:
                sl_name = self.role_to_sl_name(role)
                assert role in sg_connections_map[f"sl:{sl_name}"], f"Role {role} doesn't have connections with sl:{sl_name} scheduling group on node {node.name}"

    def validate_connections_semaphore(self, clients):
        for role, client in clients.items():
            result = client.execute("SELECT * FROM ks.cf", trace=True)
            trace = result.get_query_trace()

            sg = f"sl:{self.role_to_sl_name(role)}"
            for e in trace.events:
                # Verify scheduling group of a trace event
                assert sg in e.thread_name, f"Query on {e.source} was not executed under {sg} scheduling group"

                # Verify reader concurrency semaphore name
                if "[reader concurrency semaphore" in e.description:
                    assert f"[reader concurrency semaphore {sg}]" in e.description, f"Query on {e.source} was not executed with semaphore for {sg} scheduling group"

    def test_workload_prioritization_after_upgrade(self, dtest_config):
        self.clone_upgrade_path(dtest_config)
        config = {
            "authenticator": "org.apache.cassandra.auth.PasswordAuthenticator",
            "authorizer": "org.apache.cassandra.auth.CassandraAuthorizer",
            "role_manager": "org.apache.cassandra.auth.CassandraRoleManager",
            "service_levels_interval_ms": 500,
        }
        self.init_cluster(nodes=3, additional_config=config, skip_session=True)
        sessions = self.connect_sessions()
        is_raft_sl = self.is_raft_service_levels(sessions[0])

        # Create table to use it later for tracing. Data is not needed
        create_ks(session=sessions[0], name="ks", rf=1)
        create_cf(session=sessions[0], name="cf")

        # Create user roles and connect them to the cluster
        self.create_roles(sessions[0])
        sessions[0].execute("CREATE SERVICE LEVEL sl1")
        sessions[0].execute("CREATE SERVICE LEVEL sl2")
        sessions[0].execute("ATTACH SERVICE LEVEL sl1 TO role1")
        sessions[0].execute("ATTACH SERVICE LEVEL sl2 TO role2")

        # Upgrade the cluster
        nodes = self.cluster.nodelist()
        for version in self.current_upgrade_path:
            for node in nodes:
                node.upgrade(upgrade_to_version=version)
                assert node.status == Status.UP
                logger.info(f"Node '{node.name}' was upgraded.")

        # Reconnect main sessions
        sessions = self.connect_sessions()
        wait_for(lambda: self.check_shares_column_added(
            sessions[0], "system.service_levels_v2" if is_raft_sl else "system_distributed.service_levels"), timeout=60)
        marks = [node.mark_log() for node in nodes]

        sessions[0].execute("ALTER SERVICE LEVEL sl2 WITH shares = 400")
        sessions[0].execute("CREATE SERVICE LEVEL sl3 WITH shares = 500")
        sessions[0].execute("ATTACH SERVICE LEVEL sl3 TO role3")

        for node, session, mark in zip(nodes, sessions, marks):
            if is_raft_sl:
                read_barrier(session)
            node.watch_log_for('service level "sl3" was added.', from_mark=mark, timeout=30)

        clients = self.connect_clients()
        for _, client in clients.items():
            client.shutdown()
        for session in sessions:
            session.shutdown()

        sessions[0].cluster.shutdown()
