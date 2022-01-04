import hashlib
import logging
import os
import pytest
import time

from cassandra.query import dict_factory

from dtest_class import Tester, wait_for
from dtest_setup import DTestSetup
from tools.misc import generate_ssl_stores
from typing import List, Dict
from ccmlib import common
from cassandra import ConsistencyLevel


logger = logging.getLogger(__file__)


def get_system_clients_records(session, protocol_version=None, user=None, ssl_opts=None):
    # Read content of system.clients:
    #  address    | port  | client_type | connection_stage | driver_name | driver_version | hostname | protocol_version | shard_id | ssl_cipher_suite | ssl_enabled | ssl_protocol | username
    #  ------------+-------+-------------+------------------+-------------+----------------+----------+------------------+----------+------------------+-------------+--------------+-----------
    #  172.17.0.2 | 37392 |         cql |             null |        null |           null |     null |                0 |        0 |             null |        null |         null | anonymous
    #  172.17.0.2 | 37394 |         cql |             null |        null |           null |     null |                0 |        1 |             null |        null |         null | anonymous
    filters = []
    if ssl_opts is None:
        filters.append('ssl_enabled=null')
    elif ssl_opts != '*':
        filters.append('ssl_enabled=true')

    if user is None:
        filters.append("username='anonymous'")
    elif user != '*':
        filters.append(f"username='{user}'")

    if protocol_version is None:
        filters.append('protocol_version=null')
    elif protocol_version != '*':
        filters.append(f"protocol_version='{protocol_version}'")

    if filters:
        filters = 'WHERE ' + ' AND '.join(filters)
    else:
        filters = ''
    query = f'SELECT client_type, protocol_version, ssl_enabled, username FROM system.clients {filters} ALLOW FILTERING'
    return list(session.execute(query))


class SessionStore:
    def __init__(self):
        self._opened_sessions: Dict[int: List[CQLSession]] = {}
        self._closed_sessions: List[CQLSession] = []

    def remember_session(self, session: 'CQLSession'):
        session_hash = hash(session)
        if session_hash not in self._opened_sessions:
            self._opened_sessions[hash(session)] = [session]
            return
        self._opened_sessions[hash(session)].append(session)

    def forget_session(self, session: 'CQLSession'):
        opened_session_bucket = self._opened_sessions.get(hash(session), None)
        if opened_session_bucket:
            if session in opened_session_bucket:
                self._opened_sessions[hash(session)].remove(session)
        self._closed_sessions.append(session)

    def clear_sessions(self):
        for session in self._opened_sessions:
            try:
                session.shutdown()
            except Exception:  # pylint: disable=broad-except
                pass
        self._opened_sessions = []

    def expect_system_clients(self, tester):
        for cql_sessions in self._opened_sessions.values():
            if not cql_sessions:
                continue
            cql_sessions[0].check_if_in_system_clients()
        with tester.patient_cql_connection(
                tester.cluster.nodelist()[0], user='cassandra', password='cassandra') as session:
            for cql_session in self._closed_sessions:
                if hash(cql_session) in self._opened_sessions:
                    continue
                cql_session.check_if_not_in_system_clients(session)


class CQLSession:
    _session = None

    def __init__(self,
                 user=None,
                 password=None,
                 port=None,
                 ssl_opts=None,
                 protocol_version=None,
                 session_store: SessionStore = None,
                 session=None
                 ):
        self.user = user
        self.password = password
        self.port = port
        self.ssl_opts = ssl_opts
        self.protocol_version = protocol_version
        self.session_store = session_store
        self._session = session
        self.session_store.remember_session(self)

    def __str__(self):
        body = ','.join([n + '=' + str(getattr(self, n)) for n in ['user', 'port', 'ssl_opts', 'protocol_version']])
        return f"CQLSession<{body}>"

    def check_if_in_system_clients(self):
        assert len(get_system_clients_records(
            self._session,
            protocol_version=self.protocol_version,
            user=self.user,
            ssl_opts=self.ssl_opts
        )) > 0, f"Can't find record in system.clients for cql session {str(self)}"

    def check_if_not_in_system_clients(self, session):
        result = get_system_clients_records(
            session,
            protocol_version=self.protocol_version,
            user=self.user,
            ssl_opts=self.ssl_opts
        )
        assert len(result) == 0, f"Expected to find 0 record in system.clients for cql session {str(self)}, " \
                                 f"but see {len(result)}:\n{str(result)}"

    def __enter__(self):
        return self

    def __exit__(self, exc_type=None, exc_val=None, exc_tb=None):
        if self._session:
            self._session.shutdown()
        self.session_store.forget_session(self)
        return self

    def __hash__(self):
        return int.from_bytes(hashlib.md5(str(self).encode('utf8')).digest(), 'little')


@pytest.mark.dtest_full
class TestSystemClients(Tester):
    _test_users = [
        {'user': 'user1', 'password': 'password1'},
        {'user': 'user2', 'password': 'password2'},
        {'user': 'user3', 'password': 'password3'},
        {'user': 'user4', 'password': 'password4'},
        {'user': 'user5', 'password': 'password5'},
    ]

    @pytest.fixture(autouse=True)
    def fixture_add_additional_log_patterns(self, fixture_dtest_setup: DTestSetup):
        fixture_dtest_setup.allow_log_errors = True

    def node_session(self, node, user=None, password=None, port=None, ssl_opts=None, session_store=None,
                     row_factory=None):
        logger.debug(f"node_session: node={node} user={user} port={port} ssl_opts={ssl_opts}")
        session = self.patient_cql_connection(
            self.cluster.nodelist()[node],
            user=user,
            password=password,
            port=port,
            ssl_opts=ssl_opts,
            row_factory=row_factory,
        )
        return CQLSession(
            user=user,
            password=password,
            session_store=session_store,
            port=port,
            ssl_opts=ssl_opts,
            session=session,
        )

    @staticmethod
    def get_total_records_in_system_clients(session):
        return len(get_system_clients_records(
            session,
            protocol_version='*',
            user='*',
            ssl_opts='*'
        ))

    def wait_total_records_in_system_clients(self, session, expected, timeout=5):
        end_time = time.time() + timeout
        while True:
            last_value = self.get_total_records_in_system_clients(session)
            if last_value == expected:
                return
            if time.time() > end_time:
                raise RuntimeError(
                    "Timed out waiting for number of records in system_clients "
                    f"to get to {expected}, last value was {last_value}")
            time.sleep(0.2)

    @staticmethod
    def wait_anonymous_connections_purged(session):
        wait_for(lambda: not get_system_clients_records(session,
                                                        user=None,
                                                        ssl_opts='*',
                                                        protocol_version='*'), step=0.2, timeout=30)

    def prepare(self, ssl_optional=False, require_ssl_auth=False, nodes=1, system_auth_rf=1, superuser=False,
                ssl_enabled=True):
        cluster = self.cluster
        if ssl_enabled:
            generate_ssl_stores(self.test_path)
        # C* versions before 3.0 (CASSANDRA-10559) do not know about
        # 'client_encryption_options.optional' - so we must not add that parameter
        # Note: does of course not work with scylla, we dont support "optional" (3.x feature)
        ssl_options = {
            'enabled': ssl_enabled,
        }
        if ssl_optional:
            ssl_options['optional'] = ssl_optional

        if common.isScylla(cluster.get_install_dir()):
            ssl_options.update({
                'certificate': os.path.join(self.test_path, 'ccm_node.pem'),
                'keyfile': os.path.join(self.test_path, 'ccm_node.key')
            })
            if require_ssl_auth:
                ssl_options.update({
                    'truststore': os.path.join(self.test_path, 'ccm_node.cer'),
                    'require_client_auth': True
                })
        else:
            ssl_options.update({
                'keystore': os.path.join(self.test_path, 'keystore.jks'),
                'keystore_password': 'cassandra',
            })
            if require_ssl_auth:
                ssl_options.update({
                    'truststore': os.path.join(self.test_path, 'truststore.jks'),
                    'truststore_password': 'cassandra',
                    'require_client_auth': True
                })
        cluster.set_configuration_options({
            'client_encryption_options': ssl_options,
            'authenticator': 'org.apache.cassandra.auth.PasswordAuthenticator',
            'authorizer': 'org.apache.cassandra.auth.CassandraAuthorizer',
            'role_manager': 'org.apache.cassandra.auth.CassandraRoleManager',
            'permissions_validity_in_ms': 0,
            'roles_validity_in_ms': 0,
            'native_transport_port': 9042,
            'native_transport_port_ssl': 9142,
        })
        cluster.populate(nodes).start(wait_for_binary_proto=True, wait_other_notice=True)
        with self.patient_cql_connection(
                self.cluster.nodelist()[0],
                user='cassandra',
                password='cassandra',
                consistency_level=ConsistencyLevel.ALL) as session:
            if system_auth_rf > 1:
                session.execute(
                    "ALTER KEYSPACE system_auth WITH REPLICATION = {'class': "
                    f"'org.apache.cassandra.locator.SimpleStrategy', 'replication_factor':{system_auth_rf}}};")
                self.cluster.nodelist()[0].nodetool('repair -- system_auth')
            for user_record in self._test_users:
                user = user_record['user']
                password = user_record['password']
                session.execute(
                    f"CREATE ROLE '{user}' WITH PASSWORD = '{password}' AND LOGIN = true AND  SUPERUSER = {superuser}")

    def expect_system_clients(self):
        for cql_sessions in self._opened_sessions.values():
            if not cql_sessions:
                continue
            cql_sessions[0].check_if_in_system_clients()

        error = None
        session = None

        for node in self.cluster.nodelist():
            try:
                session = self.patient_cql_connection(node, user='cassandra', password='cassandra')
                break
            except Exception as exc:
                error = exc

        if not session:
            raise RuntimeError(f"Can't find working node, last error: {error}")

        for cql_session in self._closed_sessions:
            if hash(cql_session) in self._opened_sessions:
                continue
            cql_session.check_if_not_in_system_clients(session)

    def test_system_clients(self):
        self.prepare(
            nodes=len(self._test_users) + 1,
            ssl_optional=True,
            require_ssl_auth=False,
            system_auth_rf=len(self._test_users) + 1
        )
        session_store = SessionStore()
        # Success SSL connection test
        with self.node_session(
                1,
                **self._test_users[0],
                port=9142,
                session_store=session_store,
                ssl_opts={'ca_certs': os.path.join(self.test_path, 'ccm_node.cer')}):
            session_store.expect_system_clients(tester=self)
        session_store.expect_system_clients(tester=self)

        # Failed SSL connection test to non-SSL port
        with self.patient_cql_connection(self.cluster.nodelist()[0], user='cassandra', password='cassandra') as session:
            self.wait_anonymous_connections_purged(session)
            original_sessions_count = self.get_total_records_in_system_clients(session)
            with pytest.raises(Exception):
                self.node_session(
                    1,
                    **self._test_users[0],
                    port=9042,
                    session_store=session_store,
                    ssl_opts={'ca_certs': os.path.join(self.test_path, 'ccm_node.cer')})
            self.wait_total_records_in_system_clients(session, original_sessions_count)

        # Failed non-SSL connection test to SSL port
        with self.patient_cql_connection(self.cluster.nodelist()[0], user='cassandra', password='cassandra') as session:
            self.wait_anonymous_connections_purged(session)
            original_sessions_count = self.get_total_records_in_system_clients(session)
            with pytest.raises(Exception):
                self.node_session(
                    1,
                    **self._test_users[0],
                    session_store=session_store,
                    port=9142)
            self.wait_total_records_in_system_clients(session, original_sessions_count)

        # Failed authentication
        with self.patient_cql_connection(self.cluster.nodelist()[0], user='cassandra', password='cassandra') as session:
            self.wait_anonymous_connections_purged(session)
            original_sessions_count = self.get_total_records_in_system_clients(session)
            with pytest.raises(Exception):
                self.node_session(
                    1,
                    user='user-to-fail',
                    password='password-to-fail',
                    session_store=session_store,
                    port=9042)
            self.wait_total_records_in_system_clients(session, original_sessions_count)

        # Ledger test
        with self.node_session(1, session_store=session_store, **self._test_users[0]):
            session_store.expect_system_clients(tester=self)
            with self.node_session(2, session_store=session_store, **self._test_users[1]):
                session_store.expect_system_clients(tester=self)
                with self.node_session(3, session_store=session_store, **self._test_users[2]):
                    session_store.expect_system_clients(tester=self)
                    with self.node_session(4, session_store=session_store, **self._test_users[3]):
                        session_store.expect_system_clients(tester=self)
                        with self.node_session(5, session_store=session_store, **self._test_users[4]):
                            logger.info("Number of nodes: %s: %s", len(self.cluster.nodelist()),
                                        str(self.cluster.nodelist()))
                            with self.node_session(5, session_store=session_store, **self._test_users[0]):
                                logger.info("Number of nodes: %s: %s", len(self.cluster.nodelist()),
                                            str(self.cluster.nodelist()))
                                session_store.expect_system_clients(tester=self)
                            session_store.expect_system_clients(tester=self)
                        session_store.expect_system_clients(tester=self)
                    session_store.expect_system_clients(tester=self)
                session_store.expect_system_clients(tester=self)
            session_store.expect_system_clients(tester=self)
        session_store.expect_system_clients(tester=self)

        # Shutdown while session is alive test
        target_node = self.cluster.nodelist()[5]
        logger.info("Number of nodes: %s: %s", len(self.cluster.nodelist()), str(self.cluster.nodelist()))
        with self.node_session(5, session_store=session_store, **self._test_users[4]) as session:
            session_store.expect_system_clients(tester=self)
            # If gently is True it won't kill node with live session on it
            target_node.stop(gently=False, wait_other_notice=True)
            # Inform expect_system_clients that session should gone
            session_store.forget_session(session)
            session_store.expect_system_clients(tester=self)

        # Decomission while session is alive test
        with self.node_session(4, session_store=session_store, **self._test_users[3]) as session:
            session_store.expect_system_clients(tester=self)
            self.cluster.nodelist()[4].decomission()
            # Inform expect_system_clients that session should gone
            session_store.forget_session(session)
            session_store.expect_system_clients(tester=self)

        session_store.expect_system_clients(tester=self)
        self.cluster.nodelist()[4].stop(wait_other_notice=True)
        session_store.expect_system_clients(tester=self)

        # Drain while session is alive test
        with self.node_session(3, session_store=session_store, **self._test_users[2]):
            session_store.expect_system_clients(tester=self)
            self.cluster.nodelist()[3].node.nodetool('drain')
            # Inform expect_system_clients that session should gone
            session_store.forget_session(session)
            session_store.expect_system_clients(tester=self)
        self.cluster.nodelist()[3].stop(wait_other_notice=True)
        session_store.expect_system_clients(tester=self)

    def test_system_clients_ssl_authentication(self):
        self.prepare(nodes=1, ssl_optional=False, require_ssl_auth=True)
        session_store = SessionStore()
        # Successful SSL authentication test
        with self.node_session(
                0,
                port=9142,
                session_store=session_store,
                ssl_opts={
                    'ca_certs': os.path.join(self.test_path, 'ccm_node.cer'),
                    'keyfile': os.path.join(self.test_path, 'ccm_node.key'),
                    'certfile': os.path.join(self.test_path, 'ccm_node.pem')
                },
                **self._test_users[0],
        ):
            session_store.expect_system_clients(tester=self)
        session_store.expect_system_clients(tester=self)

        # Failed SSL authentication test
        with self.patient_cql_connection(self.cluster.nodelist()[0], user='cassandra', password='cassandra') as session:
            self.wait_anonymous_connections_purged(session)
            original_sessions_count = self.get_total_records_in_system_clients(session)
            with pytest.raises(Exception):
                self.node_session(
                    0,
                    **self._test_users[0],
                    port=9142,
                    session_store=session_store,
                    ssl_opts={'ca_certs': os.path.join(self.test_path, 'ccm_node.cer')})
                pytest.fail("Should not be able to connect when ssl auth is enabled")
            self.wait_total_records_in_system_clients(session, original_sessions_count)

    def _system_client_content(self, fields_with_value, fields_with_none_value=None, ssl_optional=True,
                               ssl_enabled=True):
        port = 9142
        ssl_opts = {'ca_certs': os.path.join(self.test_path, 'ccm_node.cer')}
        if not ssl_optional:
            port = 9042
            ssl_opts = {}
        if not fields_with_none_value:
            fields_with_none_value = []
        self.prepare(
            nodes=1,
            ssl_optional=ssl_optional,
            require_ssl_auth=False,
            system_auth_rf=1,
            superuser=True,
            ssl_enabled=ssl_enabled,
        )
        empty_value_fields_map = {}
        not_none_fields_map = {}
        session_store = SessionStore()
        with self.node_session(
                0,
                **self._test_users[0],
                row_factory=dict_factory,
                port=port,
                session_store=session_store,
                ssl_opts=ssl_opts) as session_container:
            session = session_container._session
            query = 'select * from system.clients'
            current_rows = session.execute(query).current_rows
            logger.debug(f"system.clients: {current_rows}")
            row = current_rows[0]
            for field in fields_with_none_value:
                field_value = row.get(field, None)
                if field_value is not None:
                    not_none_fields_map[field] = field_value
            for field in fields_with_value:
                field_value = row.get(field, None)
                if field_value is None:
                    empty_value_fields_map[field] = field_value

        assert not empty_value_fields_map, f"expect fields Value with content, got {empty_value_fields_map}"
        assert not not_none_fields_map, f"expect fields value without content, got {not_none_fields_map}"

    @pytest.mark.single_node
    def test_system_client_not_none(self):
        fields = ['address', 'port', 'client_type', 'connection_stage',
                  'protocol_version', 'shard_id', 'username', 'driver_name', 'driver_version']
        self._system_client_content(fields)

    @pytest.mark.require("#9216")
    @pytest.mark.single_node
    def test_system_client_hostname(self):
        fields = ['hostname']
        self._system_client_content(fields)

    @pytest.mark.require("#9216")
    @pytest.mark.single_node
    def test_system_client_ssl(self):
        fields = ['ssl_cipher_suite', 'ssl_enabled', 'ssl_protocol']
        self._system_client_content(fields)

    @pytest.mark.single_node
    def test_system_client_not_none_non_ssl(self):
        fields = ['address', 'port', 'client_type', 'connection_stage',
                  'protocol_version', 'shard_id', 'username', 'driver_name', 'driver_version']
        self._system_client_content(fields, ssl_optional=False, ssl_enabled=False)

    @pytest.mark.require("#9216")
    @pytest.mark.single_node
    def test_system_client_hostname_non_ssl(self):
        fields = ['hostname']
        self._system_client_content(fields, ssl_optional=False, ssl_enabled=False)

    @pytest.mark.single_node
    def test_system_client_ssl_non_ssl(self):
        fields = []
        fields_with_none_value = ['ssl_cipher_suite', 'ssl_enabled', 'ssl_protocol']
        self._system_client_content(fields, fields_with_none_value, ssl_optional=False)
