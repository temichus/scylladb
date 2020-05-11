import hashlib
import os

from dtest import Tester, debug
from tools import since, generate_ssl_stores
from typing import List, Dict
from nose.plugins.attrib import attr
from cassandra.cluster import Session
from ccmlib import common


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
        filters = 'WHERE' + ' AND '.join(filters)
    else:
        filters = ''

    return list(session.execute(
        f'SELECT client_type, protocol_version, ssl_enabled, username FROM system.clients {filters} ALLOW FILTERING'))


class CQLSession:
    _session = None
    def __init__(self,
                 node=None,
                 user=None,
                 password=None,
                 test_instance: Tester = None,
                 port=None,
                 ssl_opts=None,
                 protocol_version=None):
        self.user = user
        self.password = password
        self.port = port
        self.ssl_opts = ssl_opts
        self.protocol_version = protocol_version
        if node is not None:
            self._session: Session = test_instance.patient_cql_connection(
                node,
                user=user,
                password=password,
                port=port,
                ssl_opts=ssl_opts,
                protocol_version=protocol_version)
        self._test_instance = test_instance
        test_instance._remember_session(self)

    def __str__(self):
        body = ','.join([ n + '=' + str(getattr(self, n)) for n in ['user', 'port', 'ssl_opts', 'protocol_version']])
        return f"CQLSession<{body}>"

    def check_if_in_system_clients(self):
        self._test_instance.assertTrue(
            len(get_system_clients_records(
                self._session,
                protocol_version=self.protocol_version,
                user=self.user,
                ssl_opts=self.ssl_opts
            )) > 0,
            f"Can't find record in system.clients for cql session {str(self)}"
        )

    def check_if_not_in_system_clients(self, session):
        result = get_system_clients_records(
            session,
            protocol_version=self.protocol_version,
            user=self.user,
            ssl_opts=self.ssl_opts
        )
        self._test_instance.assertTrue(
            len(result) == 0,
            f"Expected to find 0 record in system.clients for "
            f"cql session {str(self)}, but see {len(result)}:\n{str(result)}"
        )

    def __enter__(self):
        return self

    def __exit__(self, exc_type=None, exc_val=None, exc_tb=None):
        if self._session:
            self._session.shutdown()
        self._test_instance._forget_session(self)
        return self

    def __hash__(self):
        return int.from_bytes(hashlib.md5(str(self).encode('utf8')).digest(), 'little')


@since("3.3")
@attr('dtest-full')
class SystemClientsTest(Tester):
    _test_users = [
        {'user': 'user1', 'password': 'password1'},
        {'user': 'user2', 'password': 'password2'},
        {'user': 'user3', 'password': 'password3'},
        {'user': 'user4', 'password': 'password4'},
        {'user': 'user5', 'password': 'password5'},
    ]

    def __init__(self, *args, **kwargs):
        self._opened_sessions: Dict[int: List[CQLSession]] = {}
        self._closed_sessions: List[CQLSession] = []
        Tester.__init__(self, *args, **kwargs)

    def _remember_session(self, session: CQLSession):
        session_hash = hash(session)
        if session_hash not in self._opened_sessions:
            self._opened_sessions[hash(session)] = [session]
            return
        self._opened_sessions[hash(session)].append(session)

    def _forget_session(self, session: CQLSession):
        opened_session_bucket = self._opened_sessions.get(hash(session), None)
        if opened_session_bucket:
            if session in opened_session_bucket:
                self._opened_sessions[hash(session)].remove(session)
        self._closed_sessions.append(session)

    def _clear_sessions(self):
        for session in self._opened_sessions:
            try:
                session.shutdown()
            except Exception:  #pylint: disable=broad-except
                pass
        self._opened_sessions = []

    def node_session(self, node, user=None, password=None, port=None, ssl_opts=None):
        return CQLSession(
            node=self.cluster.nodelist()[node],
            user=user,
            password=password,
            test_instance=self,
            port=port,
            ssl_opts=ssl_opts)

    def get_total_records_in_system_clients(self, session):
        return len(get_system_clients_records(
            session,
            protocol_version='*',
            user='*',
            ssl_opts='*'
        ))

    def prepare(self, sslOptional=False, requireSslAuth=False, nodes=1, system_auth_rf=1):
        cluster = self.cluster
        generate_ssl_stores(self.test_path)
        # C* versions before 3.0 (CASSANDRA-10559) do not know about
        # 'client_encryption_options.optional' - so we must not add that parameter
        # Note: does of course not work with scylla, we dont support "optional" (3.x feature)
        ssl_options = {
            'enabled': True,
        }
        if sslOptional:
            ssl_options['optional'] = sslOptional

        if common.isScylla(cluster.get_install_dir()):
            ssl_options.update({
                'certificate': os.path.join(self.test_path, 'ccm_node.pem'),
                'keyfile': os.path.join(self.test_path, 'ccm_node.key')
            })
            if requireSslAuth:
                ssl_options.update({
                    'truststore': os.path.join(self.test_path, 'ccm_node.cer'),
                    'require_client_auth': True
                })
        else:
            ssl_options.update({
                'keystore': os.path.join(self.test_path, 'keystore.jks'),
                'keystore_password': 'cassandra',
            })
            if requireSslAuth:
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
        with self.patient_cql_connection(self.cluster.nodelist()[0], user='cassandra', password='cassandra') as session:
            session.execute("ALTER KEYSPACE system_auth WITH REPLICATION = {'class': "
                            f"'org.apache.cassandra.locator.SimpleStrategy', 'replication_factor':{system_auth_rf}}};")
            for user_record in self._test_users:
                user = user_record['user']
                password = user_record['password']
                session.execute(f"CREATE ROLE '{user}' WITH PASSWORD = '{password}' AND LOGIN = true")

    def expect_system_clients(self):
        for cql_sessions in self._opened_sessions.values():
            if not cql_sessions:
                continue
            cql_sessions[0].check_if_in_system_clients()
        with self.patient_cql_connection(self.cluster.nodelist()[0], user='cassandra', password='cassandra') as session:
            for cql_session in self._closed_sessions:
                if hash(cql_session) in self._opened_sessions:
                    continue
                cql_session.check_if_not_in_system_clients(session)

    def system_clients_test(self):
        self.prepare(nodes=len(self._test_users) + 1, sslOptional=True, requireSslAuth=False)

        # Success SSL connection test
        with self.node_session(
                1,
                **self._test_users[0],
                port=9142,
                ssl_opts={'ca_certs': os.path.join(self.test_path, 'ccm_node.cer')}):
            self.expect_system_clients()
        self.expect_system_clients()

        # Failed SSL connection test to non-SSL port
        with self.patient_cql_connection(self.cluster.nodelist()[0], user='cassandra', password='cassandra') as session:
            original_sessions_count = self.get_total_records_in_system_clients(session)
            try:
                self.node_session(
                    1,
                    **self._test_users[0],
                    port=9042,
                    ssl_opts={'ca_certs': os.path.join(self.test_path, 'ccm_node.cer')})
                self.fail("Should not be able to connect with ssl client to non-ssl port")
            except:  # pylint: disable=bare-exception
                self.assertEqual(original_sessions_count, self.get_total_records_in_system_clients(session))

        # Failed non-SSL connection test to SSL port
        with self.patient_cql_connection(self.cluster.nodelist()[0], user='cassandra', password='cassandra') as session:
            original_sessions_count = self.get_total_records_in_system_clients(session)
            try:
                self.node_session(
                    1,
                    **self._test_users[0],
                    port=9142)
                self.fail("Should not be able to connect with non-ssl client to ssl port")
            except:  # pylint: disable=bare-exception
                self.assertEqual(original_sessions_count, self.get_total_records_in_system_clients(session))

        # Failed authentication
        with self.patient_cql_connection(self.cluster.nodelist()[0], user='cassandra', password='cassandra') as session:
            original_sessions_count = self.get_total_records_in_system_clients(session)
            try:
                self.node_session(
                    1,
                    user='user-to-fail',
                    password='password-to-fail',
                    port=9042)
                self.fail("Should not be able to connect with wrong credentials")
            except:  # pylint: disable=bare-exception
                self.assertEqual(original_sessions_count, self.get_total_records_in_system_clients(session))

        # Ledger test
        with self.node_session(1, **self._test_users[0]):
            self.expect_system_clients()
            with self.node_session(2, **self._test_users[1]):
                self.expect_system_clients()
                with self.node_session(3, **self._test_users[2]):
                    self.expect_system_clients()
                    with self.node_session(4, **self._test_users[3]):
                        self.expect_system_clients()
                        with self.node_session(5, **self._test_users[4]):
                            debug(f"Number of nodes: {len(self.cluster.nodelist())}: {str(self.cluster.nodelist())}")
                            with self.node_session(5, **self._test_users[0]):
                                debug(
                                    f"Number of nodes: {len(self.cluster.nodelist())}: {str(self.cluster.nodelist())}")
                                self.expect_system_clients()
                            self.expect_system_clients()
                        self.expect_system_clients()
                    self.expect_system_clients()
                self.expect_system_clients()
            self.expect_system_clients()
        self.expect_system_clients()

        # Shutdown while session is alive test
        target_node = self.cluster.nodelist()[5]
        debug(f"Number of nodes: {len(self.cluster.nodelist())}: {str(self.cluster.nodelist())}")
        with self.node_session(5, **self._test_users[4]) as session:
            self.expect_system_clients()
            # If gently is True it won't kill node with live session on it
            target_node.stop(gently=False, wait_other_notice=True)
            # Inform expect_system_clients that session should gone
            self._forget_session(session)
            self.expect_system_clients()

        # Decomission while session is alive test
        with self.node_session(4, **self._test_users[3]) as session:
            self.expect_system_clients()
            self.cluster.nodelist()[4].decomission()
            # Inform expect_system_clients that session should gone
            self._forget_session(session)
            self.expect_system_clients()

        self.expect_system_clients()
        self.cluster.nodelist()[4].stop(wait_other_notice=True)
        self.expect_system_clients()

        # Drain while session is alive test
        with self.node_session(3, **self._test_users[2]):
            self.expect_system_clients()
            self.cluster.nodelist()[3].node.nodetool('drain')
            # Inform expect_system_clients that session should gone
            self._forget_session(session)
            self.expect_system_clients()
        self.cluster.nodelist()[3].stop(wait_other_notice=True)
        self.expect_system_clients()

    def system_clients_ssl_authentication_test(self):
        self.prepare(nodes=1, sslOptional=False, requireSslAuth=True)
        # Successful SSL authentication test
        with self.node_session(
                0,
                port=9142,
                ssl_opts={
                    'ca_certs': os.path.join(self.test_path, 'ccm_node.cer'),
                    'keyfile': os.path.join(self.test_path, 'ccm_node.key'),
                    'certfile': os.path.join(self.test_path, 'ccm_node.pem')
                },
                **self._test_users[0],
        ):
            self.expect_system_clients()
        self.expect_system_clients()

        # Failed SSL authentication test
        with self.patient_cql_connection(self.cluster.nodelist()[0], user='cassandra', password='cassandra') as session:
            original_sessions_count = self.get_total_records_in_system_clients(session)
            try:
                self.node_session(
                    0,
                    **self._test_users[0],
                    port=9142,
                    ssl_opts={'ca_certs': os.path.join(self.test_path, 'ccm_node.cer')})
                self.fail("Should not be able to connect when ssl auth is enabled")
            except:  # pylint: disable=bare-exception
                self.assertEqual(original_sessions_count, self.get_total_records_in_system_clients(session))

