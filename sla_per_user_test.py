#!/usr/bin/env python
import logging

import pytest
from cassandra import InvalidRequest, ReadTimeout
from cassandra.cluster import Session
from cassandra.protocol import SyntaxException

from dtest_class import Tester, create_ks
from tools.data import create_c1c2_table, insert_c1c2
from tools.sla import ServiceLevel, Role, User
from tools.units import ScyllaDuration

logger = logging.getLogger(__name__)


class SLATester(Tester):
    def prepare(self, nodes: int = 1) -> Session:
        config = {'authenticator': 'org.apache.cassandra.auth.PasswordAuthenticator',
                  'authorizer': 'org.apache.cassandra.auth.CassandraAuthorizer',
                  'role_manager': 'org.apache.cassandra.auth.CassandraRoleManager'}

        self.cluster.set_configuration_options(values=config)
        self.cluster.populate(nodes)
        self.cluster.start(wait_other_notice=True, wait_for_binary_proto=True)
        session = self.patient_cql_connection(self.cluster.nodelist()[0], user='cassandra', password='cassandra')
        return session

    @staticmethod
    def populate_data(session: Session, number_of_keys: int, replication_factor: int = 1):
        create_ks(session=session, name="ks", rf=replication_factor)
        create_c1c2_table(session=session)
        insert_c1c2(session=session, n=number_of_keys)

    @staticmethod
    def create_entity_with_service_level(entity, service_level: ServiceLevel):
        service_level.create()
        entity.create()
        entity.attach_service_level(service_level=service_level)
        return entity

    @staticmethod
    def create_user(session: Session, name: str, password: str = None, superuser: bool = None) -> User:
        user = User(session=session, name=name, password=password, superuser=superuser)
        user.create()
        return user


@pytest.mark.dtest_enterprise
class TestSLA(SLATester):

    DEFAULT_SHARES = 1000

    def validate_sla(self, service_level=None, expected_slas_list=None, expected_attached_slas_list=None,
                     expected_attached_all_slas_list=None, expected_effective_slas_list=None, entity=None,
                     session=None):
        # Validate per SLA
        def validate_sla_list(sl_list, expected_sla_list, msg):
            if sl_list:
                sl_list = sorted(sl_list, key=lambda x: x[1])
                expected_sla_list = sorted(expected_sla_list, key=lambda x: x[1])
            assert expected_sla_list == sl_list, msg.format(**locals())

        def case_sensitive(name):
            if name:
                return name.replace('"', '')
            return name

        def default_shares(shares):
            if not shares:
                return self.DEFAULT_SHARES
            return shares

        if expected_slas_list is not None:
            expected_slas = [[case_sensitive(sl.name), default_shares(sl.service_shares)] for sl in expected_slas_list]
            if service_level:
                sl_list = service_level.list_service_level()
            else:
                if not expected_slas_list:
                    sl_list = ServiceLevel(session=session, name='dummy').list_all_service_levels()
                else:
                    sl_list = expected_slas_list[0].list_all_service_levels()
            validate_sla_list(sl_list, expected_slas, 'Expected SLA list: {expected_sla_list}, actual: {sl_list}')

        # Validate attached services of role
        if expected_attached_slas_list is not None and entity:
            expected_attached_slas = [[entity.name, case_sensitive(entity.attached_service_level_name)]
                                      for entity in expected_attached_slas_list]
            sl_list = entity.list_user_role_attached_service_levels()
            validate_sla_list(sl_list, expected_attached_slas,
                              'Expected attached SLA list: {expected_sla_list}, actual: {sl_list}')

        # TODO: fix commented when will work
        # Fails with "syntax error". Issue #744
        # # Validate attached ALL services
        # if expected_attached_all_slas_list is not None:
        #     expected_attached_all_slas = [[role.name, sla.name] for role, sla in expected_attached_all_slas_list]
        #     sla = entity.list_attached_service_levels(session=session)
        #     assert expected_attached_all_sla_list == sla, \
        #                 'Expected all attached SLA list: {expected_attached_all_sla_list}, actual: {sla}'
        # .format(**locals())
        # Not developed yet
        # # Validate effective services
        # if expected_effective_sla_list is not None:
        #   sla = self.list_effective_service_levels(session=session, role_name=role_name)
        #   assert expected_effective_sla_list == sla, \
        #                 'Expected effective SLA: {expected_effective_sla_list}, actual: {sla}'.format(**locals()))

    def test_sla(self):
        """
        Create SLA with 100 shares
        """
        session = self.prepare()

        sl = self.create_service_level(session=session, name='sla1', service_shares=100)

        self.validate_sla(service_level=sl, expected_slas_list=[sl], expected_attached_slas_list=[],
                          expected_attached_all_slas_list=[], expected_effective_slas_list=[])

    def test_sla_role(self):
        """
        Create SLA with 100 shares and create a role that attach to SLA
        """
        session = self.prepare()

        sl = ServiceLevel(session=session, name='sla1', shares=100)
        role = Role(session=session, name='role1')
        self.create_entity_with_service_level(entity=role, service_level=sl)

        self.validate_sla(service_level=sl,
                          expected_slas_list=[sl],
                          expected_attached_slas_list=[role],
                          expected_attached_all_slas_list=[[role, sl]],
                          expected_effective_slas_list=[],
                          entity=role)

    def test_sla_named_empty(self):
        """
        Create SLA with 100 shares
        """
        session = self.prepare()

        sl = self.create_service_level(session=session, name='empty', service_shares=100)

        self.validate_sla(service_level=sl, expected_slas_list=[sl], expected_attached_slas_list=[],
                          expected_attached_all_slas_list=[], expected_effective_slas_list=[])

    def test_user_named_empty(self):
        """
        Create SLA with 100 shares
        """
        session = self.prepare()
        sl = self.create_user(session=session, name='empty')

    def test_sla_role_named_empty(self):
        """
        Create SLA with 100 shares and create a role that attach to SLA
        """
        session = self.prepare()

        sl = ServiceLevel(session=session, name='empty', shares=100)
        role = Role(session=session, name='empty')
        self.create_entity_with_service_level(entity=role, service_level=sl)
        self.validate_sla(service_level=sl,
                          expected_slas_list=[sl],
                          expected_attached_slas_list=[role],
                          expected_attached_all_slas_list=[[role, sl]],
                          expected_effective_slas_list=[],
                          entity=role)

    def test_sla_no_shares(self):
        """
        Create SLA with default shares (not define SHARES parameter), create a role that attach to SLA and grant this
        role to the user
        """
        session = self.prepare()

        sl = ServiceLevel(session=session, name='sla1')
        role = Role(session=session, name='role1')
        self.create_entity_with_service_level(entity=role, service_level=sl)

        user = self.create_user(session=session, name='user1')

        self.validate_sla(service_level=sl,
                          expected_slas_list=[sl],
                          expected_attached_slas_list=[role],
                          expected_attached_all_slas_list=[[role, sl]],
                          expected_effective_slas_list=[],
                          entity=role)

        self.validate_sla(service_level=sl,
                          expected_slas_list=[sl],
                          expected_attached_slas_list=[],
                          expected_attached_all_slas_list=[[role, sl]],
                          expected_effective_slas_list=[[user, sl]],
                          entity=user)

    def test_user_and_role_with_sla(self):
        """
        Create SLA with shares=100, create a role that attach to SLA and grant to the user
        Create SLA with shares=500 and attach to user
        """
        session = self.prepare()
        service_levels = [ServiceLevel(session=session, name='sla%d' % s, shares=s) for s in [100, 500]]

        role = Role(session=session, name='role100')
        self.create_entity_with_service_level(entity=role, service_level=service_levels[0])

        user = User(session=session, name='user100')
        self.create_entity_with_service_level(entity=user, service_level=service_levels[1])
        role.grant_me_to(grant_to=user)

        self.validate_sla(service_level=service_levels[0],
                          expected_slas_list=[service_levels[0]],
                          expected_attached_slas_list=[role],
                          expected_attached_all_slas_list=[[role, service_levels[0]], [user, service_levels[1]]],
                          expected_effective_slas_list=[[role, service_levels[0]]],
                          entity=role)

        self.validate_sla(service_level=service_levels[1],
                          expected_slas_list=[service_levels[1]],
                          expected_attached_slas_list=[user],
                          expected_attached_all_slas_list=[[role, service_levels[0]], [user, service_levels[1]]],
                          expected_effective_slas_list=[[user, service_levels[1]]],
                          entity=user)

    def test_user_with_2_roles_and_slas(self):
        """
        Create 2 SLAs where shares are 50 and 300, attach to 2 roles and grant both to the user
        Create one more SLA with shares=100 and attach to user
        """
        session = self.prepare()
        service_levels = [ServiceLevel(session=session, name='sla%d' % s, shares=s) for s in [50, 300, 100]]
        role_sl = [[Role(session=session, name='role%d' % i), service_levels[i]] for i in range(1, 3)]
        user_sl = [[User(session=session, name='user100'), service_levels[2]]]

        for user, sl in user_sl:
            self.create_entity_with_service_level(entity=user, service_level=sl)

        for role, sl in role_sl:
            self.create_entity_with_service_level(entity=role, service_level=sl)
            role.grant_me_to(grant_to=user_sl[0][0])

        expected_attached_all_slas_list = [role_sl] + [user_sl]

        # Validate role1 and role2
        for entity, sl in role_sl+user_sl:
            self.validate_sla(service_level=sl,
                              expected_slas_list=[sl],
                              expected_attached_slas_list=[entity],
                              expected_attached_all_slas_list=expected_attached_all_slas_list,
                              expected_effective_slas_list=[],
                              entity=entity)

    def test_role_with_authentication(self):
        """
        Create SLA with default shares (not define shares parameter), create a role with password and login,
        attach to SLA and grant this role to the use
        """
        session = self.prepare()

        sl = ServiceLevel(session=session, name='sla1')
        role = Role(session=session, name='role1', password='test', login=True)
        self.create_entity_with_service_level(entity=role, service_level=sl)

        user = self.create_user(session=session, name='user1')
        role.grant_me_to(grant_to=user)

        self.validate_sla(service_level=sl,
                          expected_slas_list=[sl],
                          expected_attached_slas_list=[role],
                          expected_attached_all_slas_list=[[role, sl]],
                          expected_effective_slas_list=[],
                          entity=role)

        self.validate_sla(service_level=sl,
                          expected_slas_list=[sl],
                          expected_attached_all_slas_list=[[role, sl]],
                          expected_effective_slas_list=[[user, sl]],
                          entity=user)

    def test_case_sensitive_sla(self):
        """
        Create SLA with case sensitive name and 100 shares, create a role that attach to SLA and grant role to the user
        """
        session = self.prepare()

        sl = ServiceLevel(session=session, name='"Sla1"', shares=100)
        role = Role(session=session, name='role1')
        self.create_entity_with_service_level(entity=role, service_level=sl)

        user = self.create_user(session=session, name='user1')
        role.grant_me_to(grant_to=user)

        self.validate_sla(service_level=sl,
                          expected_slas_list=[sl],
                          expected_attached_slas_list=[role],
                          expected_attached_all_slas_list=[role, sl],
                          expected_effective_slas_list=[],
                          entity=role)

        self.validate_sla(service_level=sl,
                          expected_slas_list=[sl],
                          expected_attached_slas_list=[],
                          expected_attached_all_slas_list=[role, sl],
                          expected_effective_slas_list=[[user, sl]],
                          entity=user)

    def test_user_with_same_slas(self):
        """
        Create 2 SLAs with same shares amount (300), attach to 2 roles and grant both to the user
        """
        session = self.prepare()
        service_levels = [ServiceLevel(session=session, name='sla%d' % s, shares=300) for s in range(2)]
        role_sl = [[Role(session=session, name='role%d' % i), service_levels[i]] for i in range(2)]

        user = self.create_user(session=session, name='user1')

        for role, sl in role_sl:
            self.create_entity_with_service_level(entity=role, service_level=sl)
            role.grant_me_to(grant_to=user)

        # Validate role1 and role 2
        for role, sl in role_sl:
            self.validate_sla(service_level=sl,
                              expected_slas_list=[sl],
                              expected_attached_slas_list=[role],
                              expected_attached_all_slas_list=role_sl,
                              expected_effective_slas_list=[[role, sl]],
                              entity=role)

        # Validate user
        # TODO: not clear, what is the effective SLA here, because of both SLAs have same SHARES amount?
        self.validate_sla(expected_slas_list=service_levels,
                          expected_attached_slas_list=[],
                          expected_attached_all_slas_list=role_sl,
                          expected_effective_slas_list=[[user, service_levels[0]]],
                          entity=user)

    def test_user_without_role(self):
        """
        Create user with no role. No SLA with "default" name
        """
        session = self.prepare()

        sl = ServiceLevel(session=session, name='', shares=self.DEFAULT_SHARES)
        user = self.create_user(session=session, name='user1')

        self.validate_sla(expected_slas_list=[],
                          expected_attached_slas_list=[],
                          expected_attached_all_slas_list=[],
                          expected_effective_slas_list=[[user, sl]],
                          entity=user,
                          session=session)

    def test_change_default_sla_and_attach_user(self):
        """
        Create user with no role. Create SLA with "DEFAULT" name and shares=50 and attach to user
        """
        session = self.prepare()

        sl = ServiceLevel(session=session, name='"DEFAULT"', shares=50)
        user = User(session=session, name='user1')
        self.create_entity_with_service_level(entity=user, service_level=sl)

        self.validate_sla(service_level=sl,
                          expected_slas_list=[sl],
                          expected_attached_slas_list=[user],
                          expected_attached_all_slas_list=[user, sl],
                          expected_effective_slas_list=[[user, sl]],
                          entity=user)

    def test_change_default_sla_and_attach_role(self):
        """
        Create SLA with "DEFAULT" name and 100 shares, create a role and attach SLA
        """
        session = self.prepare()

        sl = ServiceLevel(session=session, name='"DEFAULT"', shares=50)
        role = Role(session=session, name='role1')
        self.create_entity_with_service_level(entity=role, service_level=sl)

        self.validate_sla(service_level=sl,
                          expected_slas_list=[sl],
                          expected_attached_slas_list=[role],
                          expected_attached_all_slas_list=[role, sl],
                          expected_effective_slas_list=[[role, sl]],
                          entity=role)

    def test_detach_one_of_two_slas(self):
        """
        -Create 2 SLAs where shares are 50 and 300, attach to 2 roles and grant both to the user.
        -De-attach 300 shares SLA
        """
        session = self.prepare()
        service_levels = [ServiceLevel(session=session, name='sla%d' % s, shares=s) for s in [50, 300]]
        role_sl = [[Role(session=session, name='role%d' % s), service_levels[s]] for s in range(2)]
        user = self.create_user(session=session, name='user1')

        for role, sl in role_sl:
            self.create_entity_with_service_level(entity=role, service_level=sl)
            role.grant_me_to(grant_to=user)

        # Validate role1 and role2
        for role, sl in role_sl:
            self.validate_sla(service_level=sl,
                              expected_slas_list=[sl],
                              expected_attached_slas_list=[role],
                              expected_attached_all_slas_list=role_sl,
                              expected_effective_slas_list=[[role, sl]],
                              entity=role)

        # Validate user
        self.validate_sla(expected_slas_list=service_levels,
                          expected_attached_slas_list=[],
                          expected_attached_all_slas_list=role_sl,
                          expected_effective_slas_list=[[role_sl[1], service_levels[1]]],
                          entity=user)

        # Detach SERVICE_LEVEL 300 and validate
        for i, r_s in enumerate(role_sl):
            if r_s[1].service_shares == 300:
                r_s[0].detach_service_level()
                detached_sl = r_s[1]
                role_sl[i][1] = ServiceLevel(session=session, name='', shares=self.DEFAULT_SHARES)

        # Validate role1
        for role, sl in role_sl:
            if sl.service_shares == self.DEFAULT_SHARES:
                service_level = detached_sl
                expected_slas_list = [detached_sl]
                expected_attached_slas_list = []
            else:
                service_level = sl
                expected_slas_list = [sl]
                expected_attached_slas_list = [role]

            self.validate_sla(service_level=service_level,
                              expected_slas_list=expected_slas_list,
                              expected_attached_slas_list=expected_attached_slas_list,
                              expected_attached_all_slas_list=role_sl,
                              expected_effective_slas_list=[[role, sl]],
                              entity=role)
        # Validate user
        # TODO: what is expected_effective_slas_list
        self.validate_sla(expected_slas_list=service_levels,
                          expected_attached_slas_list=[],
                          expected_effective_slas_list=[[user, role_sl[1][0]]],
                          entity=user)

    def test_detach_sla(self):
        """
        -Create SLA with shares=100 and attach to the user
        -De-attach the SLA
        """
        session = self.prepare()
        sla_name = 'sla1'
        sla_shares = 100
        user_name = 'user1'

        sl = ServiceLevel(session=session, name='sla100', shares=100)
        user = User(session=session, name='user100')
        self.create_entity_with_service_level(entity=user, service_level=sl)

        # Validate user
        self.validate_sla(expected_slas_list=[sl],
                          expected_attached_slas_list=[user],
                          expected_effective_slas_list=[[user, sl]],
                          entity=user)

        # Detach SERVICE_LEVEL and validate
        user.detach_service_level()

        # Validate user
        self.validate_sla(expected_slas_list=[sl],
                          expected_attached_slas_list=[],
                          expected_effective_slas_list=[[user, ServiceLevel(session=session,
                                                                            name='',
                                                                            shares=self.DEFAULT_SHARES)
                                                         ]
                                                        ],
                          entity=user)

    def test_two_roles_one_slas_to_user(self):
        """
         - Create few SLAs, roles and users
         - grant one SLA to every roles
         - grant 2 roles to each user
         - attache 1 SLA to each user
        """
        session = self.prepare()
        sla_shares = [50, 100, 250, 350, 550, 750]
        slas = [ServiceLevel(session=session, name='sla%d' % shares, shares=shares) for shares in sla_shares]
        roles_slas = [(Role(name="role%d" % sla.shares, session=session), sla) for sla in slas]

        user_sla_roles = [[User(name='user%d' % idx, session=session), slas[idx],
                           [roles_slas[idx + 2][0], roles_slas[idx + 3][0]]] for idx in range(0, 3)]

        for role, sl in roles_slas:
            self.create_entity_with_service_level(entity=role, service_level=sl)
            self.validate_sla(service_level=sl,
                              expected_slas_list=[sl],
                              expected_attached_slas_list=[role],
                              expected_effective_slas_list=[[role, sl]],
                              entity=role)

        for user, sl, roles in user_sla_roles:
            self.create_entity_with_service_level(entity=user, service_level=sl)
            for role in roles:
                role.grant_me_to(grant_to=user)

            self.validate_sla(expected_slas_list=slas,
                              expected_attached_slas_list=[user],
                              expected_effective_slas_list=[[user, sl]],
                              entity=user)

    def test_inherit_2_slas(self):
        """
        -Create 2 SLAs: 50 and 200.
        -Create 2 role
        -Assign SLAs to the roles
        -Grant role with "200" to role "50".
        -Grant role with "50" to the user
        """
        session = self.prepare()
        sla_shares = [50, 200]
        slas = [ServiceLevel(session=session, name='sla%d' % shares, shares=shares) for shares in sla_shares]
        roles_slas = [(Role(name="role%d" % sla.shares, session=session), sla) for sla in slas]
        user = self.create_user(session=session, name='user1')

        for role, sl in roles_slas:
            self.create_entity_with_service_level(entity=role, service_level=sl)
            if sl.shares == 50:
                role50 = role
            else:
                role.grant_me_to(grant_to=role50)

            self.validate_sla(service_level=sl,
                              expected_slas_list=[sl],
                              expected_attached_slas_list=[role],
                              expected_attached_all_slas_list=[],
                              expected_effective_slas_list=[roles_slas],
                              entity=role)

        # Validate user
        self.validate_sla(expected_slas_list=slas,
                          expected_attached_slas_list=[],
                          expected_effective_slas_list=[user, slas[1]],
                          entity=user)

    def find_role_by_attached_share(self, roles_slas_list, find_shares):
        for role, _ in roles_slas_list:
            if role.attached_service_level_shares == find_shares:
                return role

        return None

    def test_inherit_3_slas(self):
        """
        -Create 3 SLAs: 50, 200, 600.
        -Create 3 role
        -Assign 200 and 600 SLAs to the roles
        -Grant role with "200" to role "600".
        -Grant role with "200" to the user
        -Attach SLA 50 to user
        """
        session = self.prepare()
        sla_shares = [200, 600, 50]
        slas = [ServiceLevel(session=session, name='sla%d' % shares, shares=shares) for shares in sla_shares]
        roles_slas = {s.shares: {'role': Role(name="role%d" % s.shares, session=session),
                                 'service_level': s} for s in slas[:2]}
        user = User(session=session, name='user1')

        for _, role_sl in roles_slas.items():
            self.create_entity_with_service_level(entity=role_sl['role'], service_level=role_sl['service_level'])

        roles_slas[200]['role'].grant_me_to(grant_to=roles_slas[600]['role'])

        self.create_entity_with_service_level(entity=user, service_level=slas[-1])
        roles_slas[200]['role'].grant_me_to(grant_to=user)

        expected_attached_all_sla_list = [role_sl['role'] for _, role_sl in roles_slas.items()] + [user]

        for _, role_sl in roles_slas.items():
            self.validate_sla(service_level=role_sl['service_level'],
                              expected_slas_list=[role_sl['service_level']],
                              expected_attached_slas_list=[role_sl['role']],
                              expected_attached_all_slas_list=expected_attached_all_sla_list,
                              expected_effective_slas_list=[role_sl['role'], slas[1]],
                              entity=role_sl['role'])

        # Validate user
        self.validate_sla(expected_slas_list=slas,
                          expected_attached_slas_list=[user],
                          expected_attached_all_slas_list=expected_attached_all_sla_list,
                          expected_effective_slas_list=[user, slas[1]],
                          entity=user)

    def test_inherit_4_slas(self):
        """
        -Create 4 SLAs: 50, 200, 500, 1000.
        -Create 4 role
        -Assign SLAs to the roles
        -Grant role with "200" to role "50".
        -Grant role with "1000" to role "200".
        -Grant role with "500" to role "200".
        -Grant role with "50" to the user
        """
        session = self.prepare()
        sla_shares = [50, 200, 500, 1000]
        slas = [ServiceLevel(session=session, name='sla%d' % shares, shares=shares) for shares in sla_shares]
        roles_slas = {s.shares: {'role': Role(name="role%d" % s.shares, session=session),
                                 'service_level': s} for s in slas}
        user = self.create_user(session=session, name='user1')
        for _, role_sl in roles_slas.items():
            self.create_entity_with_service_level(entity=role_sl['role'], service_level=role_sl['service_level'])

        roles_slas[200]['role'].grant_me_to(grant_to=roles_slas[50]['role'])
        roles_slas[1000]['role'].grant_me_to(grant_to=roles_slas[200]['role'])
        roles_slas[500]['role'].grant_me_to(grant_to=roles_slas[200]['role'])
        roles_slas[50]['role'].grant_me_to(grant_to=user)

        # Validate roles after grant
        expected_attached_all_sla_list = [role_sl['role'] for _, role_sl in roles_slas.items()]
        for _, role_sl in roles_slas.items():
            self.validate_sla(service_level=role_sl['service_level'],
                              expected_slas_list=[role_sl['service_level']],
                              expected_attached_slas_list=[role_sl['role']],
                              expected_attached_all_slas_list=expected_attached_all_sla_list,
                              expected_effective_slas_list=[role_sl['role'], slas[1]],
                              entity=role_sl['role'])

        # Validate user
        self.validate_sla(expected_slas_list=slas,
                          expected_attached_slas_list=[],
                          expected_attached_all_slas_list=expected_attached_all_sla_list,
                          expected_effective_slas_list=[user, slas[3]],
                          entity=user)

    def test_drop_not_assigned_sla(self):
        """
        Drop not assigned and not granted SLA
        -Create non-default SLA
        -Drop the SLA
        """
        session = self.prepare()
        sl = self.create_service_level(session=session, name='sla1', service_shares=100)

        self.validate_sla(service_level=sl,
                          expected_slas_list=[sl],
                          expected_attached_slas_list=[])

        sl.drop()
        self.validate_sla(service_level=sl,
                          expected_slas_list=[],
                          expected_attached_slas_list=[])

    def test_drop_default_not_assigned_sla(self):
        """
        Drop default SLA
        -Create SLA with "default" the name
        -Create role without attach the SLA. The role's effective shares should be os "DEFAULT" service level
        -Drop the SLA
        -Create role without SLA and validate, that role's effective shares is DEFAULT_SERVICE_LEVEL_SHARES
        """
        session = self.prepare()

        sl = self.create_service_level(session=session, name='"DEFAULT"', service_shares=100)
        role = self.create_role(session=session, name='role1')

        self.validate_sla(service_level=sl,
                          expected_slas_list=[sl],
                          expected_attached_slas_list=[])

        self.validate_sla(service_level=sl,
                          expected_slas_list=[sl],
                          expected_attached_slas_list=[],
                          expected_attached_all_slas_list=[],
                          expected_effective_slas_list=[[role, sl]],
                          entity=role)

        sl.drop()

        self.validate_sla(service_level=sl,
                          expected_slas_list=[],
                          expected_attached_slas_list=[],
                          expected_attached_all_slas_list=[],
                          expected_effective_slas_list=[[role, ServiceLevel(session=session,
                                                                            name='',
                                                                            shares=self.DEFAULT_SHARES)
                                                         ]
                                                        ],
                          entity=role)

    def test_drop_sla_assigned_to_role(self):
        """
        Drop assigned SLA
        -Create non-default SLA
        -Assign to the role
        -Drop the SLA
        """
        session = self.prepare()

        sl = self.create_service_level(session=session, name='sla1', service_shares=100)
        role = Role(session=session, name='role1')
        self.create_entity_with_service_level(entity=role, service_level=sl)

        self.validate_sla(service_level=sl,
                          expected_slas_list=[sl],
                          expected_attached_slas_list=[role],
                          expected_effective_slas_list=[[role, sl]],
                          entity=role)

        sl.drop()
        self.validate_sla(service_level=sl,
                          expected_slas_list=[],
                          expected_attached_slas_list=[],
                          expected_effective_slas_list=[[role, ServiceLevel(session=session,
                                                                            name='',
                                                                            shares=self.DEFAULT_SHARES)
                                                         ]
                                                        ],
                          entity=role)

    def test_drop_granted_sla(self):
        """
        Drop granted SLA
        -Create non-default SLA
        -Assign to the role
        -Grant to the user
        -Drop the SLA
        """
        session = self.prepare()

        sl = self.create_service_level(session=session, name='sla1', service_shares=100)
        role = Role(session=session, name='role1')
        self.create_entity_with_service_level(entity=role, service_level=sl)

        user = self.create_user(session=session, name='user1')
        role.grant_me_to(grant_to=user)

        self.validate_sla(service_level=sl,
                          expected_slas_list=[sl],
                          expected_attached_slas_list=[role],
                          expected_effective_slas_list=[[role, sl]],
                          entity=role)

        self.validate_sla(expected_slas_list=[sl],
                          expected_attached_slas_list=[],
                          expected_effective_slas_list=[[user, sl]],
                          entity=user)

        sl.drop()
        dummy_sl = ServiceLevel(session=session, name='', shares=self.DEFAULT_SHARES)
        self.validate_sla(expected_slas_list=[],
                          expected_attached_slas_list=[],
                          expected_effective_slas_list=[[role, dummy_sl]],
                          entity=role,
                          session=session)

        self.validate_sla(expected_slas_list=[],
                          expected_attached_slas_list=[],
                          expected_effective_slas_list=[[user, dummy_sl]],
                          entity=user,
                          session=session)

    def test_drop_sla_assigned_to_user(self):
        """
        -Create non-default SLA
        -Assign to the user
        -Drop the SLA
        """
        session = self.prepare()
        sl = self.create_service_level(session=session, name='sla1', service_shares=100)
        user = User(session=session, name='user1')
        self.create_entity_with_service_level(entity=user, service_level=sl)

        self.validate_sla(service_level=sl,
                          expected_slas_list=[sl],
                          expected_attached_slas_list=[user],
                          expected_effective_slas_list=[[user, sl]],
                          entity=user)

        sl.drop()
        self.validate_sla(service_level=sl, expected_slas_list=[],
                          expected_attached_slas_list=[],
                          expected_effective_slas_list=[[user, ServiceLevel(session=session,
                                                                            name='',
                                                                            shares=self.DEFAULT_SHARES)
                                                         ]
                                                        ],
                          entity=user)

    def test_drop_role_with_sla(self):
        """
        -Create non-default SLA
        -Attach to the role
        -Drop the role
        """
        session = self.prepare()

        sl = self.create_service_level(session=session, name='sla1', service_shares=100)
        role = Role(session=session, name='role1')
        self.create_entity_with_service_level(entity=role, service_level=sl)

        self.validate_sla(service_level=sl,
                          expected_slas_list=[sl],
                          expected_attached_slas_list=[role],
                          expected_effective_slas_list=[[role, sl]],
                          entity=role)

        role.drop()
        self.validate_sla(service_level=sl,
                          expected_slas_list=[sl],
                          expected_attached_slas_list=[],
                          expected_effective_slas_list=[],
                          entity=role)

    def test_drop_user_with_role_sla(self):
        """
        -Create non-default SLA
        -Assign to the role
        -Grant to the user
        -Drop the user
        """
        session = self.prepare()

        sl = self.create_service_level(session=session, name='sla1', service_shares=100)
        role = Role(session=session, name='role1')
        self.create_entity_with_service_level(entity=role, service_level=sl)

        user = self.create_user(session=session, name='user1')
        role.grant_me_to(grant_to=user)

        self.validate_sla(service_level=sl,
                          expected_slas_list=[sl],
                          expected_attached_slas_list=[role],
                          expected_effective_slas_list=[[role, sl]],
                          entity=role)

        self.validate_sla(expected_slas_list=[sl],
                          expected_attached_slas_list=[],
                          expected_effective_slas_list=[[user, sl]],
                          entity=user)

        user.drop()
        self.validate_sla(service_level=sl,
                          expected_slas_list=[sl],
                          expected_attached_slas_list=[role],
                          expected_effective_slas_list=[[role, sl]],
                          entity=role)

    def update_not_assigned_sla(self):
        """
        Update not assigned and not granted SLA
        -Create non-default SLA
        -Update the SLA with different shares number
        """
        session = self.prepare()

        sl = self.create_service_level(session=session, name='sla1', service_shares=100)
        self.validate_sla(service_level=sl,
                          expected_slas_list=[sl])

        new_shares = 500
        sl.alter(new_shares=new_shares)
        self.validate_sla(service_level=sl,
                          expected_slas_list=[sl])

    def update_default_assigned_sla(self):
        """
        -Create SLA with "default" in the name
        -Create user with no SLA
        -Update the SLA with different shares number
        """
        session = self.prepare()

        user = self.create_user(session=session, name='user1')

        self.validate_sla(expected_slas_list=[],
                          expected_attached_slas_list=[],
                          expected_effective_slas_list=[[user, ServiceLevel(session=session,
                                                                            name='',
                                                                            shares=self.DEFAULT_SHARES)
                                                         ]
                                                        ],
                          entity=user)

        sl = self.create_service_level(session=session, name='"DEFAULT"', service_shares=100)
        self.validate_sla(service_level=sl,
                          expected_slas_list=[sl],
                          expected_attached_slas_list=[],
                          expected_effective_slas_list=[[user, sl]],
                          entity=user)

        new_shares = 500
        sl.alter(new_shares=new_shares)
        self.validate_sla(service_level=sl,
                          expected_slas_list=[sl],
                          expected_attached_slas_list=[],
                          expected_effective_slas_list=[[user, sl]],
                          entity=user)

    def update_assigned_sla(self):
        """
        -Create non-default SLA
        -Assign to the role
        -Grant to the user
        -Update the SLA
        """
        session = self.prepare()

        sl = ServiceLevel(session=session, name='sla1', shares=100)
        role = Role(session=session, name='role1')
        self.create_entity_with_service_level(entity=role, service_level=sl)

        self.validate_sla(service_level=sl,
                          expected_slas_list=[sl],
                          expected_attached_slas_list=[role],
                          expected_effective_slas_list=[[role, sl]],
                          entity=role)

        user = self.create_user(session=session, name='user1')
        role.grant_me_to(grant_to=user)
        self.validate_sla(service_level=sl,
                          expected_slas_list=[sl],
                          expected_attached_slas_list=[],
                          expected_effective_slas_list=[[user, sl]],
                          entity=user)

        new_shares = 500
        sl.alter(new_shares=new_shares)
        self.validate_sla(service_level=sl,
                          expected_slas_list=[sl],
                          expected_attached_slas_list=[role],
                          expected_effective_slas_list=[[role, sl]],
                          entity=role)

        self.validate_sla(service_level=sl,
                          expected_slas_list=[sl],
                          expected_attached_slas_list=[],
                          expected_effective_slas_list=[[user, sl]],
                          entity=user)

    def test_attach_2_slas_to_role(self):
        """
        Create 2 SLAs, attach to 1 role
        """
        session = self.prepare()

        sl100 = ServiceLevel(session=session, name='sla1', shares=100)
        role = Role(session=session, name='role1')
        self.create_entity_with_service_level(entity=role, service_level=sl100)

        self.validate_sla(service_level=sl100,
                          expected_slas_list=[sl100],
                          expected_attached_slas_list=[role],
                          expected_effective_slas_list=[[role, sl100]],
                          entity=role)

        sl200 = self.create_service_level(session=session, name='sla2', service_shares=200)
        role.attach_service_level(service_level=sl200)

        self.validate_sla(expected_slas_list=[sl100, sl200],
                          expected_attached_slas_list=[role],
                          expected_effective_slas_list=[[role, sl200]],
                          entity=role)

    #####
    # Negative tests
    #####

    def test_attach_not_exists_sla_to_role(self):
        """
        Create role and attach not existing service level
        """
        session = self.prepare()

        sl = ServiceLevel(session=session, name='tmp')
        role = self.create_role(session=session, name='role1')

        expected_error = 'Service Level {} doesn\'t exists.'.format(sl.name)
        with pytest.raises(InvalidRequest, match=expected_error):
            role.attach_service_level(service_level=sl)

    def test_attach_sla_to_not_exists_role(self):
        """
        Create SLA and attach to not existing role
        """
        session = self.prepare()

        sl = self.create_service_level(session=session, name='sla1')
        role = Role(session=session, name='role1')

        expected_error = 'Role {} doesn\'t exist.'.format(role.name)

        with pytest.raises(InvalidRequest, match=expected_error):
            role.attach_service_level(service_level=sl)

    def test_drop_not_existing_sla(self):
        """
        Drop not-created service level
        """
        session = self.prepare()
        sl = ServiceLevel(session=session, name='sla1')
        expected_error = 'Service Level {} doesn\'t exists.'.format(sl.name)

        with pytest.raises(InvalidRequest, match=expected_error):
            sl.drop(if_exists=False)

    @pytest.mark.require('scylladb/scylla-enterprise#776')
    def test_update_not_existing_sla(self):
        """
        Update not-created service level
        """
        session = self.prepare()
        sl = self.create_service_level(session=session, name='sla1')
        expected_error = 'The service Level \'{}\' doesn\'t exists.'.format(sl.name)

        with pytest.raises(SyntaxException, match=expected_error):
            sl.alter(new_shares=100)

    def test_create_sla_with_more_1000_shares(self):
        """
        Create SLA with 1001 SHARES
        """
        self._wrong_shares(shares=1001)

    def test_create_sla_with_0_shares(self):
        """
        Create SLA with 0 SHARES
        """
        self._wrong_shares(shares=0)

    def test_create_sla_with_negative_shares(self):
        """
        Create 2 SLAs, attach to 1 role
        """
        self._wrong_shares(shares=-1)

    def _wrong_shares(self, shares):
        session = self.prepare()
        expected_error = r"'SHARES' can only take values of 1-1000 \(given %d\)" % shares

        with pytest.raises(SyntaxException, match=expected_error):
            ServiceLevel(session=session, name='sla1', shares=shares).create()


class TestSLATimeouts(SLATester):
    KEY_NUM = 1000

    @pytest.mark.parametrize(argnames=("duration"),
                             argvalues=[
                                 ScyllaDuration(milliseconds=1),
                                 ScyllaDuration(hours=5),
                                 ScyllaDuration(hours=23, minutes=2, seconds=2, milliseconds=2),
    ],
        ids=("1ms", "5h", "23h2m2s2ms"))
    def test_timeout_valid_values(self, duration: ScyllaDuration):
        """
        Create s Service Level with a valid timeout value.
        """
        read_query = "SELECT * FROM ks.cf"
        session = self.prepare()
        node = self.cluster.nodelist()[0]
        self.populate_data(session=session, number_of_keys=self.KEY_NUM)
        role = Role(session=session, name="test_role", password="test_role", login=True).create()
        sl = ServiceLevel(session=session, name="sl1", timeout=duration, shares=None).create()
        role.attach_service_level(sl)
        grant_select_query = f"GRANT SELECT ON KEYSPACE ks TO {role.name};"
        session.execute(grant_select_query)
        listed_sl = sl.list_service_level()

        assert sl == listed_sl

        new_session = self.patient_cql_connection(
            node=node, user=role.name, password=role.password
        )
        query_result = new_session.execute(read_query).all()
        logger.debug("Query result: %s", query_result)

        assert query_result

    @pytest.mark.require('#scylladb/scylla#10285')
    @pytest.mark.parametrize(argnames=("scylla_yaml_timeout", "sl_timeout", "query_timeout"),
                             argvalues=[
                                 (100, ScyllaDuration(milliseconds=0), None),
                                 (None, ScyllaDuration(milliseconds=100), "0ms"),
    ],
        ids=[
                                 "service_level_wins_over_scylla_yaml",
                                 "query_timeout_wins_over_service_level_timeout",
    ])
    def test_sla_timeout_priority(self, scylla_yaml_timeout, sl_timeout: ScyllaDuration, query_timeout):
        """
        Request timeouts can be set using 3 different methods:
        1. scylla.yaml config file entry
        2. Service Level definition
        3. Per-query timeout defined in the CQL statement

        In terms of prioritization:
        3 > 2 > 1

        This test checks if 2 scenarios are true:
        - 3 > 2
        - 2 > 1

        Test steps:
        1) Populate db with some data.
        2) Create a test Role.
        3) Open a new session for the test Role.
        and query the data.
        4) Change the timeout value using the given method.
        5) Open a new CQL session and attempt the same query as
        in (2).
        6) Assert that a RequestTimeout error was raised.
        """
        if query_timeout:
            read_query = f"SELECT * FROM ks.cf USING TIMEOUT {query_timeout}"
        else:
            read_query = "SELECT * FROM ks.cf"

        session = self.prepare()
        node = self.cluster.nodelist()[0]
        self.populate_data(session=session, number_of_keys=self.KEY_NUM)
        sl = ServiceLevel(session=session, name="sl1", timeout=ScyllaDuration(milliseconds=100), shares=None).create()
        role1 = Role(session=session, name="role1", password="role1", login=True).create()
        role1.attach_service_level(sl)

        grant_select_query = f"GRANT SELECT ON KEYSPACE ks TO {role1.name};"
        session.execute(grant_select_query)
        user_session = self.patient_cql_connection(
            self.cluster.nodelist()[0], user=role1.name, password=role1.password)

        pre_read_result = user_session.execute("SELECT * FROM ks.cf").all()
        assert len(pre_read_result) == self.KEY_NUM

        # update scylla yaml
        if scylla_yaml_timeout:
            mark = node.mark_log()
            self.cluster.stop()
            self.cluster.set_configuration_options(values={"read_request_timeout_in_ms": scylla_yaml_timeout})
            self.cluster.start(wait_other_notice=True, wait_for_binary_proto=True)
            node.watch_log_for(exprs=["cql_server_controller - Starting listening for CQL clients"],
                               from_mark=mark)

        if sl_timeout:
            new_session = self.patient_cql_connection(
                node=node, user="cassandra", password="cassandra"
            )
            sl.session = new_session
            sl.alter(new_timeout=sl_timeout)

        with pytest.raises(ReadTimeout):
            new_user_session = self.patient_cql_connection(
                node=node, user=role1.name, password=role1.password)
            read_result = new_user_session.execute(read_query)
            logger.debug("Read result: %s", len(read_result.all()))


class TestSLTimeoutsNegative(SLATester):
    @pytest.mark.require('#scylladb/scylla#10286')
    @pytest.mark.parametrize(argnames=["timeout", "expected_exception_msg"],
                             argvalues=[
                                 [ScyllaDuration(days=1), "Timeout values cannot be expressed in days/months"],
                                 [ScyllaDuration(months=2), "Timeout values cannot be expressed in days/months"],
                                 [ScyllaDuration(nanoseconds=1000),
                                  "Timeout values must be expressed in millisecond granularity"],
                                 [ScyllaDuration(milliseconds=-1200), "Timeout values must be nonnegative"]
    ],
        ids=[
                                 "using_days_as_values",
                                 "using_months_as_values",
                                 "nanosecond_value",
                                 "negative_timeout_value",
    ])
    def test_invalid_timeout_values(self, timeout: ScyllaDuration, expected_exception_msg: str):
        session = self.prepare()

        with pytest.raises(InvalidRequest) as exc:
            ServiceLevel(session=session, name="sl1", timeout=timeout, shares=None).create()

        assert exc.match(f".*{expected_exception_msg}.*")
