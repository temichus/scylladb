#!/usr/bin/env python

from unittest import skip
from dtest import Tester, debug

@skip('the feature is not ready yet')
class SLATests(Tester):

    def prepare(self, rf=1, options={}, nodes=1, jvm_args=[], roles_expiry=0):
        config = {'authenticator': 'org.apache.cassandra.auth.PasswordAuthenticator',
                  'authorizer': 'org.apache.cassandra.auth.CassandraAuthorizer',
                  'role_manager': 'org.apache.cassandra.auth.CassandraRoleManager',
                  'permissions_validity_in_ms': 0,
                  'roles_validity_in_ms': roles_expiry}

        if options:
            config.update(options)

        self.rf = rf
        self.cluster.set_configuration_options(values=config)
        self.cluster.populate(nodes)
        self.cluster.start(jvm_args=jvm_args,wait_other_notice=True,wait_for_binary_proto=True)
        session = self.patient_cql_connection(self.cluster.nodelist()[0], user='cassandra', password='cassandra')

        return session

    def validate_sla(self, session, sla_name_list=None, expected_sla_list=None, expected_attached_sla_list=None,
                     expected_attached_all_sla_list=None, expected_effective_sla_list=None, role_name=None, user_name=None):
        for sla_name in sla_name_list:
            # Validate per SLA
            sla = self.list_service_levels(session=session, sla_name=sla_name)
            self.assertTrue(expected_sla_list == sla,
                            msg='Expected SLA list: {expected_sla_list}, actual: {sla}'.format(**locals()))

            # Validate attached services
            if expected_attached_sla_list is not None:
                sla = self.list_attached_service_levels(session=session, role_name=role_name)
                self.assertTrue(expected_attached_sla_list == sla,
                            msg='Expected SLA list: {expected_sla_list}, actual: {sla}'.format(**locals()))

            # Validate attached ALL services
            if expected_attached_all_sla_list is not None:
                sla = self.list_attached_service_levels(session=session)
                self.assertTrue(expected_attached_all_sla_list == sla,
                            msg='Expected SLA list: {expected_sla_list}, actual: {sla}'.format(**locals()))

            # Not developed yet
            # # Validate effective services
            # if expected_effective_sla_list is not None:
            #   sla = self.list_effective_service_levels(session=session, role_name=role_name)
            #   self.assertTrue(expected_effective_sla_list == sla,
            #                 msg='Expected SLA list: {expected_sla_list}, actual: {sla}'.format(**locals()))

    def sla_test(self):
        """
        Create SLA with 100 shares
        """
        session = self.prepare()
        sla_name = 'sla1'
        shares = 100
        self.create_service_level(session=session, sla_name=sla_name, shares=shares)
        self.validate_sla(session=session, sla_name_list=[sla_name],
                          expected_sla_list=[[sla_name, shares]],
                          expected_attached_sla_list=[],
                          expected_attached_all_sla_list=[],
                          expected_effective_sla_list=[])

    def sla_role_test(self):
        """
        Create SLA with 100 shares and create a role that attach to SLA
        """
        session = self.prepare()
        sla_name = 'sla1'
        role_name = 'role1'
        shares = 100

        self.create_service_level(session=session, sla_name=sla_name, shares=shares)
        self.create_role(session=session, role_name=role_name)
        self.attach_service_level(session=session, sla_name=sla_name, role_name=role_name)

        self.validate_sla(session=session, sla_name_list=[sla_name],
                          expected_sla_list=[[sla_name, shares]],
                          expected_attached_sla_list=[[role_name, sla_name]],
                          expected_attached_all_sla_list=[[role_name, sla_name]],
                          expected_effective_sla_list=[],
                          role_name=role_name)

    def sla_no_shares_test(self):
        """
        Create SLA with default shares (not define SHARES parameter), create a role that attach to SLA and grant this role to the user
        """
        session = self.prepare()
        sla_name = 'sla1'
        role_name = 'role1'
        user_name = 'user1'

        self.create_service_level(session=session, sla_name=sla_name)
        self.create_role(session=session, role_name=role_name)
        self.attach_service_level(session=session, sla_name=sla_name, role_name=role_name)
        self.create_user(session=session, user_name=user_name, roles_list_to_grant=[role_name])

        self.validate_sla(session=session, sla_name_list=[sla_name],
                          expected_sla_list=[[sla_name, 1]],
                          expected_attached_sla_list=[[role_name, sla_name]],
                          expected_attached_all_sla_list=[[role_name, sla_name]],
                          expected_effective_sla_list=[],
                          role_name=role_name, user_name=user_name)

    def sla_100_shares_test(self):
        """
        Create SLA with shares=100, create a role that attach to SLA and grant to the user
        """
        session = self.prepare()
        sla_name = 'sla1'
        shares = 100
        role_name = 'role1'
        user_name = 'user1'

        self.create_service_level(session=session, sla_name=sla_name, shares=shares)
        self.create_role(session=session, role_name=role_name)
        self.attach_service_level(session=session, sla_name=sla_name, role_name=role_name)
        self.create_user(session=session, user_name=user_name, roles_list_to_grant=[role_name])

        self.validate_sla(session=session, sla_name_list=[sla_name],
                          expected_sla_list=[[sla_name, shares]],
                          expected_attached_sla_list=[[role_name, sla_name]],
                          expected_attached_all_sla_list=[[role_name, sla_name]],
                          expected_effective_sla_list=[],
                          role_name=role_name, user_name=user_name)

    def user_with_2_slas_test(self):
        """
        Create 2 SLAs where shares are 50 and 300, attach to 2 roles and grant both to the user
        """
        session = self.prepare()
        sla1_name = 'sla1'
        sla2_name = 'sla2'
        sla1_shares = 50
        sla2_shares = 300
        role1_name = 'role1'
        role2_name = 'role2'
        user_name = 'user1'

        self.create_service_level(session=session, sla_name=sla1_name,shares=sla1_shares)
        self.create_role(session=session, role_name=role1_name)
        self.attach_service_level(session=session, sla_name=sla1_name, role_name=role1_name)


        self.create_service_level(session=session, sla_name=sla2_name,shares=sla2_shares)
        self.create_role(session=session, role_name=role2_name)
        self.attach_service_level(session=session, sla_name=sla2_name, role_name=role2_name)

        self.create_user(session=session, user_name=user_name, roles_list_to_grant=[role1_name, role2_name])

        # Validate role1
        self.validate_sla(session=session, sla_name_list=[sla1_name],
                          expected_sla_list=[[sla1_name, sla1_shares]],
                          expected_attached_sla_list=[[role1_name, sla1_name]],
                          expected_attached_all_sla_list=[[role1_name, sla1_name], [role2_name, sla2_name]],
                          expected_effective_sla_list=[],
                          role_name=role1_name, user_name=user_name)
        # Validate role2
        self.validate_sla(session=session, sla_name_list=[sla2_name],
                          expected_sla_list=[[sla2_name, sla2_shares]],
                          expected_attached_sla_list=[[role2_name, sla2_name]],
                          expected_attached_all_sla_list=[[role1_name, sla1_name], [role2_name, sla2_name]],
                          expected_effective_sla_list=[],
                          role_name=role2_name, user_name=user_name)

