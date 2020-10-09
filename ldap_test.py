import uuid
import ldap_docker
import os
import random
import shutil
import subprocess

from dtest import Tester, info
from cassandra import Unauthorized


class TestLdap(Tester):
    LDAP_USER = 'scylla-qa'
    LDAP_PASSWORD = 'cassandra'

    def tearDown(self):
        if self.saslauthd_proc is not None:
            self.saslauthd_proc.kill() # Using terminate() here somehow terminates nosetests itself. o_O
            self.saslauthd_proc.wait()
        shutil.rmtree(self.saslauthd_dir) # Next line requires self.test_path directory to be empty.
        Tester.tearDown(self)
        self.test_ldap_docker.remove_container(force=True)

    def setUp(self):
        Tester.setUp(self)
        self.create_ldap_container()
        self.saslauthd_dir = os.path.join(self.test_path, 'saslauthd')
        os.mkdir(self.saslauthd_dir)
        self.saslauthd_proc = None

    def get_default_scylla_yaml_ldap_config(self):
        return {'role_manager': 'com.scylladb.auth.LDAPRoleManager',
                'ldap_url_template': f'{self.test_ldap_docker.ldap_server.name}/'
                                     f'{self.test_ldap_docker.ldap_base_object}?cn?sub?(uniqueMember='
                                     f'uid={{USER}},ou=Person,{self.test_ldap_docker.ldap_base_object})',
                'ldap_attr_role': 'cn',
                'ldap_bind_dn': f'cn=admin,{self.test_ldap_docker.ldap_base_object}',
                'ldap_bind_passwd': 'scylla'}

    def prepare(self, nodes=1, user='cassandra', password='cassandra', configure_ldap=True, create_role=True,
                create_ks_and_table=True, **kwargs):
        self.nodes = []
        config = dict()
        options = kwargs.get('options', None)
        if options:
            config.update(options)
        if nodes < 3:
            config.update({'commitlog_sync': 'batch'})
        cluster = self.cluster
        if configure_ldap:
            ldap_options = kwargs.get('ldap_options', None)
            self.test_ldap_docker.create_ldap_connection()
            saslauthd_conf_path = os.path.join(self.saslauthd_dir, 'saslauthd.conf')
            with open(saslauthd_conf_path, 'w') as f:
                f.write(f'ldap_servers: ldap://{self.test_ldap_docker.ldap_server.name}\n'
                        f'ldap_search_base: {self.test_ldap_docker.ldap_base_object}')
            self.saslauthd_proc = subprocess.Popen(
                ['saslauthd', '-d', '-n', '1', '-a', 'ldap', '-O', saslauthd_conf_path, '-m', self.saslauthd_dir],
                stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)
            if ldap_options:
                config.update(ldap_options)
            else:
                config.update(self.get_default_scylla_yaml_ldap_config())
        if kwargs.get('start_rpc', False):
            config.update(values={'start_rpc': True})
        config.update({'authenticator': 'org.apache.cassandra.auth.PasswordAuthenticator',
                       'authorizer': 'org.apache.cassandra.auth.CassandraAuthorizer',
                       'permissions_validity_in_ms': 0})
        cluster.set_configuration_options(values=config)

        if not cluster.nodelist():
            # --logger-log-level ldap_role_manager=debug
            cluster.populate(nodes).start(wait_for_binary_proto=True)
        self.nodes = cluster.nodelist()[:]
        session = self.patient_cql_connection(self.nodes[0], user=user, password=password)
        if create_role:
            session.execute(f'CREATE ROLE \'{self.LDAP_USER}\' WITH login=true AND password=\'{self.LDAP_PASSWORD}\'')
        if create_ks_and_table:
            self.create_ks(session, name='ks', rf=1)
            self.create_cf(session, name='cf')
            session.execute('INSERT INTO ks.cf (key, c, v) VALUES (\'key1\', \'c1\', \'v1\')')

    def create_ldap_container(self):
        docker_name = '-'.join(['openldap', f'{uuid.uuid4()}'[:8]])
        self.test_ldap_docker = ldap_docker.LdapDocker()
        self.test_ldap_docker.create_ldap_container(name=docker_name)

    def add_role_to_ldap(self, ldap_role='cassandra', ldap_password=LDAP_PASSWORD, unique_members=None):
        unique_members_list = []
        if not unique_members:
            unique_members = [self.LDAP_USER, 'qa-user']
        for member in unique_members:
            unique_members_list.append(f'uid={member},ou=Person,{self.test_ldap_docker.ldap_base_object}')
        ldap_user_group = [f'cn={ldap_role},{self.test_ldap_docker.ldap_base_object}',
                           ['groupOfUniqueNames', 'simpleSecurityObject', 'top'],
                           {'uniqueMember': unique_members_list, 'userPassword': ldap_password}]
        self.test_ldap_docker.add_ldap_object(*ldap_user_group)

    @staticmethod
    def create_role_grant_permission(session, permission_dict):
        session.execute(f'CREATE ROLE \'{permission_dict["role"]}\'')
        for permission in permission_dict['permissions']:
            session.execute(f'GRANT {permission} ON {permission_dict["resource"]} TO \'{permission_dict["role"]}\'')

    def check_user_permissions(self, permission_dict):
        session = self.patient_cql_connection(self.nodes[0], user=permission_dict['user'],
                                              password=permission_dict['password'])
        random_name = uuid.uuid4().__repr__()[6:14]
        # trying to create objects if there is create permission
        if 'create' not in permission_dict['permissions']:
            with self.assertRaisesRegexp(Unauthorized, f"User {permission_dict['user']} has no CREATE permission on "):
                self.create_ks(session=session, name=f'ks_{random_name}', rf=1)
            with self.assertRaisesRegexp(Unauthorized, f"User {permission_dict['user']} has no CREATE permission on "):
                self.create_cf(session=session, name=f'ks_{random_name}.table_{random_name}')
        else:
            self.create_ks(session=session, name=f'ks_{random_name}', rf=1)
            self.create_cf(session=session, name=f'table_{random_name}')
        if 'modify' not in permission_dict['permissions']:
            with self.assertRaisesRegexp(Unauthorized, f"User {permission_dict['user']} has no MODIFY permission on "):
                session.execute('INSERT INTO ks.cf (key, c, v) VALUES (\'key\', \'c\', \'v\')')
            with self.assertRaisesRegexp(Unauthorized, f"User {permission_dict['user']} has no MODIFY permission on "):
                session.execute('UPDATE ks.cf SET v = \'vv\' WHERE key = \'key\' and c = \'c\'')
            with self.assertRaisesRegexp(Unauthorized, f"User {permission_dict['user']} has no MODIFY permission on "):
                session.execute('DELETE from ks.cf WHERE key = \'key\' and c = \'c\'')
        else:
            session.execute('INSERT INTO ks.cf (key, c, v) VALUES (\'key\', \'c\', \'v\')')
            session.execute('UPDATE ks.cf SET v = \'vv\' WHERE key = \'key\' and c = \'c\'')
            session.execute('DELETE from ks.cf WHERE key = \'key\' and c = \'c\'')
        # trying to read data from a table
        if 'select' not in permission_dict['permissions']:
            with self.assertRaisesRegexp(Unauthorized, f"User {permission_dict['user']} has no SELECT permission on "):
                session.execute('SELECT * from ks.cf LIMIT 1')
        else:
            session.execute('SELECT * from ks.cf LIMIT 1')

    def test_simple_ldap_connection(self):
        self.prepare()
        self.add_role_to_ldap()
        permission = {'user': self.LDAP_USER,
                      'password': self.LDAP_PASSWORD,
                      'role': 'cassandra',
                      'permissions': ['create', 'modify', 'select'],
                      'resource': 'all'}
        self.check_user_permissions(permission_dict=permission)

    def test_user_login_only(self):
        self.prepare()
        self.add_role_to_ldap()
        session = self.patient_cql_connection(self.nodes[0], user=self.LDAP_USER, password=self.LDAP_PASSWORD)
        session.execute('CREATE ROLE \'login_user\' with login=true and password=\'test\'')
        permission = {'user': 'login_user',
                      'password': 'test',
                      'role': 'empty_role',
                      'permissions': [],
                      'resource': None}
        self.check_user_permissions(permission_dict=permission)

    def test_wrong_user(self):
        self.prepare()
        self.add_role_to_ldap()
        failed = False
        try:
            self.cql_connection(self.nodes[0], user='abcd', password=self.LDAP_PASSWORD)
        except Exception:
            info(f'Failed to get a session for an user that does\'t exist - Success')
            failed = True
        if not failed:
            raise Exception('User succeeded to create a session, instead of failing')

    def test_partial_permissions(self):
        self.prepare()
        create_permission = {'user': 'create_user',
                             'password': 'create_user',
                             'role': 'create_role',
                             'permissions': ['create'],
                             'resource': 'all keyspaces'}
        select_permission = {'user': 'select_user',
                             'password': 'select_user',
                             'role': 'select_role',
                             'permissions': ['select'],
                             'resource': 'all keyspaces'}
        modify_permission = {'user': 'modify_user',
                             'password': 'modify_user',
                             'role': 'modify_role',
                             'permissions': ['modify'],
                             'resource': 'all keyspaces'}
        create_select_permission = {'user': 'create_select_user',
                                    'password': 'create_select_user',
                                    'role': 'create_select_role',
                                    'permissions': ['create', 'select'],
                                    'resource': 'all keyspaces'}
        modify_select_permission = {'user': 'modify_select_user',
                                    'password': 'modify_select_user',
                                    'role': 'modify_select_role',
                                    'permissions': ['modify', 'select'],
                                    'resource': 'all keyspaces'}
        create_modify_select_permission = {'user': 'create_modify_select_user',
                                           'password': 'create_modify_select_user',
                                           'role': 'create_modify_select_role',
                                           'permissions': ['create', 'modify', 'select'],
                                           'resource': 'all keyspaces'}
        all_permissions = {'create': create_permission, 'select': select_permission, 'modify': modify_permission,
                           'create_select': create_select_permission, 'modify_select': modify_select_permission,
                           'create_modify_select': create_modify_select_permission}
        for k, permission_dict in all_permissions.items():
            info(f'Starting with {k}')
            session = self.patient_cql_connection(self.nodes[0], user='cassandra', password='cassandra')
            self.create_role_grant_permission(session=session, permission_dict=permission_dict)
            session.execute(f"CREATE ROLE \'{permission_dict['user']}\' WITH login=true AND "
                            f"password=\'{permission_dict['password']}\'")
            self.add_role_to_ldap(ldap_role=permission_dict['role'], unique_members=[permission_dict['user']])
            self.check_user_permissions(permission_dict=permission_dict)
            info(f'Finished with {k}')

    def test_hard_restart_scylla(self):
        self.prepare()
        self.add_role_to_ldap()
        permission = {'user': self.LDAP_USER,
                      'password': self.LDAP_PASSWORD,
                      'role': 'empty_role',
                      'permissions': ['create', 'modify', 'select'],
                      'resource': None}
        self.check_user_permissions(permission_dict=permission)
        self.nodes[0].stop(gently=False)
        self.nodes[0].start(wait_other_notice=True, wait_for_binary_proto=True)
        self.check_user_permissions(permission_dict=permission)

    def test_soft_restart_scylla(self):
        self.prepare()
        self.add_role_to_ldap()
        permission = {'user': self.LDAP_USER,
                      'password': self.LDAP_PASSWORD,
                      'role': 'empty_role',
                      'permissions': ['create', 'modify', 'select'],
                      'resource': None}
        self.check_user_permissions(permission_dict=permission)
        self.nodes[0].stop()
        self.nodes[0].start(wait_other_notice=True, wait_for_binary_proto=True)
        self.check_user_permissions(permission_dict=permission)

    def test_multiple_roles_superuser(self):
        self.prepare()
        list_of_roles = ['r1', 'r2', 'r3', 'r4', 'r5', 'cassandra']
        cassandra_session = self.patient_cql_connection(node=self.nodes[0], user='cassandra', password='cassandra')
        for role in list_of_roles:
            permission = {'user': self.LDAP_USER,
                          'password': self.LDAP_PASSWORD,
                          'role': role,
                          'permissions': ['create', 'modify', 'select'],
                          'resource': 'ALL KEYSPACES'}
            if not role == 'cassandra':
                self.create_role_grant_permission(session=cassandra_session, permission_dict=permission)
            self.add_role_to_ldap(ldap_role=role)
            self.check_user_permissions(permission_dict=permission)

    def test_multiple_roles_single_permission(self):
        self.prepare(create_role=False)
        actions_list = ['create', 'modify', 'select']
        permission = random.choice(actions_list)
        info(f'permission={permission}')
        list_of_roles = ['r1', 'r2', 'r3', 'r4', 'r5']
        cassandra_session = self.patient_cql_connection(node=self.nodes[0], user='cassandra', password='cassandra')
        cassandra_session.execute(f'create role \'{self.LDAP_USER}\' with login=true and '
                                  f'password=\'{self.LDAP_PASSWORD}\'')
        permission_dict = {'user': f'{self.LDAP_USER}',
                           'password': self.LDAP_PASSWORD,
                           'permissions': [permission],
                           'resource': 'ALL KEYSPACES'}
        for role in list_of_roles:
            permission_dict['role'] = role
            self.create_role_grant_permission(session=cassandra_session, permission_dict=permission_dict)
            self.add_role_to_ldap(ldap_role=role)
            self.check_user_permissions(permission_dict=permission_dict)

    def test_multiple_roles_permissions_combination(self):
        self.prepare(create_role=False)
        actions_list = ['create', 'modify', 'select']
        list_of_roles = ['r1', 'r2', 'r3', 'r4', 'r5', 'r6']
        cassandra_session = self.patient_cql_connection(node=self.nodes[0], user='cassandra', password='cassandra')
        cassandra_session.execute(f'create role \'{self.LDAP_USER}\' with login=true and '
                                  f'password=\'{self.LDAP_PASSWORD}\'')
        permission = {'user': f'{self.LDAP_USER}',
                      'password': self.LDAP_PASSWORD,
                      'resource': 'ALL KEYSPACES'}
        for role, action in zip(list_of_roles, actions_list*2):
            permission['role'] = role
            permission['permissions'] = [action]
            self.create_role_grant_permission(session=cassandra_session, permission_dict=permission)
            self.add_role_to_ldap(ldap_role=f'{role}')
        permission['permissions'] = actions_list[:]
        self.check_user_permissions(permission_dict=permission)

    def test_add_ldap_after_regular_work(self):
        self.prepare(create_role=False, configure_ldap=False)
        cassandra_session = self.patient_cql_connection(node=self.nodes[0], user='cassandra', password='cassandra')
        cassandra_session.execute(f'create role \'{self.LDAP_USER}\' with login=true and '
                                  f'password=\'{self.LDAP_PASSWORD}\'')
        self.create_role_grant_permission(session=cassandra_session, permission_dict={'role': 'select_modify',
                                                                                      'permissions': ['select',
                                                                                                      'modify'],
                                                                                      'resource': 'ALL KEYSPACES'})
        cassandra_session.execute(f'grant \'select_modify\' to \'{self.LDAP_USER}\'')
        for i in range(100):
            cassandra_session.execute(f'INSERT INTO ks.cf (key, c, v) VALUES (\'key{i}\', \'c{i}\', \'v{i}\')')
        self.test_ldap_docker.create_ldap_connection()
        self.cluster.set_configuration_options(values=self.get_default_scylla_yaml_ldap_config())
        self.nodes[0].stop()
        self.nodes[0].start(wait_other_notice=True, wait_for_binary_proto=True)
        self.add_role_to_ldap()
        permission = {'user': self.LDAP_USER,
                      'password': self.LDAP_PASSWORD,
                      'role': 'cassandra',
                      'permissions': ['create', 'modify', 'select'],
                      'resource': 'ALL KEYSPACES'}
        self.check_user_permissions(permission_dict=permission)

    def test_multiple_users_superuser_role(self):
        self.prepare()
        list_of_unique_members = [f'user_{i}' for i in range(10)]
        self.add_role_to_ldap(unique_members=list_of_unique_members)
        session = self.patient_cql_connection(self.nodes[0], user='cassandra', password='cassandra')
        for user in list_of_unique_members:
            session.execute(f'create role \'{user}\' with login=true and password=\'{self.LDAP_PASSWORD}\'')
            permission = {'user': user,
                          'password': self.LDAP_PASSWORD,
                          'role': 'cassandra',
                          'permissions': ['create', 'modify', 'select'],
                          'resource': 'ALL KEYSPACES'}
            self.check_user_permissions(permission_dict=permission)

    def test_multiple_users_with_modify_role(self):
        self.prepare()
        session = self.patient_cql_connection(self.nodes[0], user='cassandra', password='cassandra')
        permission = {'password': self.LDAP_PASSWORD,
                      'role': 'modify_role',
                      'permissions': ['modify'],
                      'resource': 'ALL KEYSPACES'}
        self.create_role_grant_permission(session=session, permission_dict=permission)
        list_of_unique_members = [f'user_{i}' for i in range(10)]
        self.add_role_to_ldap(ldap_role='modify_role', unique_members=list_of_unique_members)
        for user in list_of_unique_members:
            session.execute(f'create role \'{user}\' with login=true and password=\'{self.LDAP_PASSWORD}\'')
            permission['user'] = user
            self.check_user_permissions(permission_dict=permission)

    def test_grant_role_permissions(self):
        self.prepare()
        permission = {'user': self.LDAP_USER,
                      'password': self.LDAP_PASSWORD,
                      'role': 'test_role',
                      'permissions': ['create'],
                      'resource': 'all keyspaces'}

        session = self.patient_cql_connection(self.nodes[0], user='cassandra', password='cassandra')
        self.create_role_grant_permission(session=session, permission_dict=permission)
        self.add_role_to_ldap(ldap_role=permission['role'], unique_members=[permission['user']])
        self.check_user_permissions(permission_dict=permission)
        session.execute(f'GRANT modify ON {permission["resource"]} TO \'{permission["role"]}\'')
        permission['permissions'].append('modify')
        self.check_user_permissions(permission_dict=permission)
        session.execute(f'GRANT select ON {permission["resource"]} TO \'{permission["role"]}\'')
        permission['permissions'].append('select')
        self.check_user_permissions(permission_dict=permission)

    def test_revoke_role_permissions(self):
        self.prepare()
        permission = {'user': self.LDAP_USER,
                      'password': self.LDAP_PASSWORD,
                      'role': 'test_role',
                      'permissions': ['create', 'modify', 'select'],
                      'resource': 'all keyspaces'}
        session = self.patient_cql_connection(self.nodes[0], user='cassandra', password='cassandra')
        self.create_role_grant_permission(session=session, permission_dict=permission)
        self.add_role_to_ldap(ldap_role=permission['role'], unique_members=[permission['user']])
        self.check_user_permissions(permission_dict=permission)
        revoke_permission = permission['permissions'].pop(-1)
        session.execute(f'REVOKE {revoke_permission} ON {permission["resource"]} FROM \'{permission["role"]}\'')
        self.check_user_permissions(permission_dict=permission)
        revoke_permission = permission['permissions'].pop(-1)
        session.execute(f'REVOKE {revoke_permission} ON {permission["resource"]} FROM \'{permission["role"]}\'')
        self.check_user_permissions(permission_dict=permission)

    def test_remove_user_from_ldap(self):
        self.prepare()
        self.add_role_to_ldap()
        permission = {'user': self.LDAP_USER,
                      'password': self.LDAP_PASSWORD,
                      'role': 'cassandra',
                      'permissions': ['create', 'modify', 'select'],
                      'resource': 'all'}
        self.check_user_permissions(permission_dict=permission)
        dn = str(self.test_ldap_docker.search_ldap_object(self.test_ldap_docker.ldap_base_object,
                                                          f'(cn={permission["role"]})')).split()[1]
        res = self.test_ldap_docker.modify_ldap_object(dn, {'uniqueMember': [('MODIFY_DELETE',
                                                                              [f'uid={permission["user"]},ou=Person,'
                                                                               f'dc=scylladb,dc=com'])]})
        if not res:
            raise Exception('Failed to delete user from LDAP')
        permission['permissions'] = []
        self.check_user_permissions(permission_dict=permission)

    def test_modify_username_on_ldap(self):
        self.prepare()
        self.add_role_to_ldap()
        permission = {'user': self.LDAP_USER,
                      'password': self.LDAP_PASSWORD,
                      'role': 'cassandra',
                      'permissions': ['create', 'modify', 'select'],
                      'resource': 'all'}
        self.check_user_permissions(permission_dict=permission)
        new_permission = permission.copy()
        permission['permissions'] = []
        new_user = 'qa-user'
        new_permission['user'] = new_user
        dn = str(self.test_ldap_docker.search_ldap_object(self.test_ldap_docker.ldap_base_object,
                                                          f'(cn={permission["role"]})')).split()[1]
        res = self.test_ldap_docker.modify_ldap_object(dn, {'uniqueMember': [('MODIFY_REPLACE',
                                                                              [f'uid={new_user},ou=Person,'
                                                                               f'dc=scylladb,dc=com'])]})
        if not res:
            raise Exception('Failed to modify user on LDAP')
        try:
            self.check_user_permissions(permission_dict=permission)
        except Unauthorized as ex:
            info(f'User {permission["user"]} was removed, and it was supposed to fail to connect to scylla')
        session = self.patient_cql_connection(self.nodes[0], user='cassandra', password='cassandra')
        session.execute(f'CREATE ROLE \'{new_user}\' WITH login=true and password=\'{permission["password"]}\'')
        self.check_user_permissions(permission_dict=new_permission)

    def test_add_user_to_ldap(self):
        self.prepare()
        self.add_role_to_ldap()
        permission = {'user': self.LDAP_USER,
                      'password': self.LDAP_PASSWORD,
                      'role': 'cassandra',
                      'permissions': ['create', 'modify', 'select'],
                      'resource': 'all'}
        self.check_user_permissions(permission_dict=permission)
        new_user = 'qa-superuser'
        dn = str(self.test_ldap_docker.search_ldap_object(self.test_ldap_docker.ldap_base_object,
                                                          f'(cn={permission["role"]})')).split()[1]
        res = self.test_ldap_docker.modify_ldap_object(dn, {'uniqueMember': [('MODIFY_ADD',
                                                                              [f'uid={new_user},ou=Person,'
                                                                               f'dc=scylladb,dc=com'])]})
        if not res:
            raise Exception('Failed to modify user on LDAP')
        self.check_user_permissions(permission_dict=permission)
        session = self.patient_cql_connection(self.nodes[0], user='cassandra', password='cassandra')
        session.execute(f'CREATE ROLE \'{new_user}\' WITH login=true and password=\'{permission["password"]}\'')
        permission['user'] = new_user
        self.check_user_permissions(permission_dict=permission)
