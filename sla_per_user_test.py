#!/usr/bin/env python
import logging
from typing import List, Union

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


@pytest.mark.dtest_enterprise
class TestSLA(SLATester):
    @staticmethod
    def _validate_sla(service_level: ServiceLevel):
        listed_sl = service_level.list_service_level()
        assert service_level == listed_sl, f"Expected created service level {service_level.name} to be equal " \
                                           f"to {listed_sl.name}, but it was not. \nExpected: {service_level}" \
                                           f"\nActual: {listed_sl}"

    def validate_sl_list(self, session: Session, expected_service_levels: List[ServiceLevel]):
        """
        Validates if the provided SL list is the same as the list of
        all the Service Levels in the db.
        If an empty list is provided or the expected_service_levels
        is None, it will query the db using a 'dummy' SL and check
        if no other SLs exist.
        If the provided list is not empty, it will query the db
        using the first SL in the list and check the SLs listed
        in the db against those provided as expected_service_levels.
        """
        if not expected_service_levels:
            dummy_sl = ServiceLevel(session=session, name="dummy").create()
            all_service_levels = [sl for sl in dummy_sl.list_all_service_levels() if sl.name != '"dummy"']
            assert not all_service_levels, f"Expected to find no service levels, but found some: {all_service_levels}"
        else:
            first_sl = expected_service_levels[0]
            full_service_levels_list = first_sl.list_all_service_levels()
            assert len(full_service_levels_list) == len(expected_service_levels)

            for sl in expected_service_levels:
                self._validate_sla(service_level=sl)

    @staticmethod
    def validate_attached_slas_list(session: Session, entity: Union[Role, User],
                                    expected_service_levels: List[ServiceLevel]):
        """
        Checks whether a given entity's attached SL list is equal to
        the provided expected_service_levels list.
        """
        rows = entity.list_user_role_attached_service_levels()

        assert len(rows) == len(expected_service_levels), "Actual number of attached service levels " \
                                                          "is different than expected. Expected list: %s\n" \
                                                          "Actual list: %s" % (expected_service_levels, rows)
        for row in rows:
            sl = ServiceLevel(session=session, name=row.service_level)
            expected_service_level = [item for item in expected_service_levels if item.name == sl.name]
            assert len(expected_service_level) == 1, "Did not find the expected service level: %s in the attached " \
                                                     "service level list: %s" % (sl, expected_service_levels)
            assert sl.list_service_level() == expected_service_level[0], "Listed attached service level did not " \
                                                                         "match expected service level."

    @pytest.mark.parametrize(argnames=["sla_name"],
                             argvalues=[["sla1"], ["Sla1"]])
    def test_sla(self, sla_name: str):
        """
        Create an SL with 100 shares using different strings as names.
        Validate that the SL created in the db is the same as
        the test model (i.e. same name and attributes).
        """
        session = self.prepare()
        sl = ServiceLevel(session=session, name=sla_name, shares=100).create()

        self.validate_sl_list(session=session, expected_service_levels=[sl])

    @pytest.mark.parametrize(argnames=["entity_class", "entity_name", "entity_pass", "entity_login"],
                             argvalues=[
                                 [Role, "role1", None, False],
                                 [User, "user1", None, False],
                                 [Role, "auth_role", "auth", True],
                                 [User, "auth_user", None, False]
    ],
        ids=[
                                 "attach_to_role",
                                 "attach_to_user",
                                 "attach_to_auth_role",
                                 "attach_to_auth_user"
    ])
    def test_sla_attached_to_entity(self, entity_class, entity_name: str, entity_pass: str, entity_login: bool):
        """
        1. Create SL with 100 shares.
        2. Create an entity (Role / User) with authentication
        settings.
        3. Attach SL to entity.
        Assert that SL attached to the entity is the same as the
        expected by the test model (i.e. same name and attributes).
        """
        session = self.prepare()

        sl = ServiceLevel(session=session, name='sla1', shares=100)
        entity_kwargs = {
            "session": session,
            "name": entity_name,
            "password": entity_pass
        }

        if entity_login:
            entity_kwargs["login"] = entity_login

        entity = entity_class(**entity_kwargs)
        self.create_entity_with_service_level(entity=entity, service_level=sl)

        self.validate_sl_list(session=session, expected_service_levels=[sl])
        self.validate_attached_slas_list(session=session, entity=entity, expected_service_levels=[sl])

    @pytest.mark.require('#scylladb/scylla-enterprise#2163')
    def test_sla_no_shares(self):
        """
        1. Create SL without specifying the number of shares.
        2. Create a Role.
        3. Attach the SL to the Role.
        4. Validate that the SL attached to the Role has the default
        value for service shares (i.e. 1000).
        """
        session = self.prepare()

        expected_sl = ServiceLevel(session=session, name='sla1')
        actual_sl = ServiceLevel(session=session, name='sla1', shares=None)
        role = Role(session=session, name='role1')
        self.create_entity_with_service_level(entity=role, service_level=actual_sl)

        self.validate_sl_list(session=session, expected_service_levels=[expected_sl])
        self.validate_attached_slas_list(session=session, entity=role, expected_service_levels=[expected_sl])

    @pytest.mark.parametrize(argnames=["entity_class", "entity_name"],
                             argvalues=[[Role, "test_role"], [User, "test_user"]],
                             ids=["with_role", "with_user"])
    def test_replace_sla(self, entity_class, entity_name: str):
        """
        1. Create 2 SLs with different number of service shares.
        2. Create a test entity (Role / User).
        2. Attach first SL to the test Role.
        3. Validate that both SLs exist and only the first is
        attached to the test entity.
        4. Replace the attached SL by:
        - detaching the attached SL from the entity
        - attaching the second SL to the entity
        5.Validate that both SLs exist and only the second one is
        attached to the test entity.
        """
        session = self.prepare()
        sl_50 = ServiceLevel(session=session, name="sla50", shares=50).create()
        sl_300 = ServiceLevel(session=session, name="sla300", shares=300).create()
        sls = [sl_50, sl_300]

        entity = entity_class(session=session, name=entity_name).create()

        entity.attach_service_level(service_level=sl_50)

        self.validate_sl_list(session=session, expected_service_levels=sls)
        self.validate_attached_slas_list(session=session, entity=entity, expected_service_levels=[sl_50])

        entity.attach_another_sla_to_role(service_level=sl_300)

        self.validate_sl_list(session=session, expected_service_levels=sls)
        self.validate_attached_slas_list(session=session, entity=entity, expected_service_levels=[sl_300])

    @pytest.mark.require('#scylladb/scylla-enterprise#2163')
    @pytest.mark.parametrize(argnames=["entity_class", "entity_name"],
                             argvalues=[[Role, "test_role"], [User, "test_user"]],
                             ids=["with_role", "with_user"])
    def test_update_assigned_sla_service_shares(self, entity_class, entity_name: str):
        """
        1. Create entity.
        2. Create SL with default service shares value.
        3. Validate that the attached SL has the default service shares value.
        3. Update the attached SL with a different shares value.
        4. Validate that the attached SL has the updated service shares value.
        """
        session = self.prepare()
        default_sl = ServiceLevel(session=session, name="test_sla")
        sl = ServiceLevel(session=session, name="test_sla", shares=None)
        entity = entity_class(session=session, name=entity_name)
        self.create_entity_with_service_level(entity=entity_class(session=session, name=entity_name),
                                              service_level=sl)

        self.validate_sl_list(session=session, expected_service_levels=[default_sl])
        self.validate_attached_slas_list(session=session, entity=entity, expected_service_levels=[default_sl])

        sl.alter(new_shares=500)

        self.validate_sl_list(session=session, expected_service_levels=[sl])
        self.validate_attached_slas_list(session=session, entity=entity, expected_service_levels=[sl])

    @pytest.mark.parametrize(argnames=["entity_class", "entity_name"],
                             argvalues=[[Role, "test_role"], [User, "test_user"]],
                             ids=["with_role", "with_user"])
    def test_attach_2_slas_to_role(self, entity_class, entity_name: str):
        """
        1. Create SL with 100 shares.
        2. Create entity (User / Role) with the SL created in (1).
        3. Validate that the SL created in (1) exists and is attached
        to the entity.
        4. Create another SL with 200 shares.
        5. Attach the SL created in (4) to the entity.
        6. Validate that the SL created in (4) exists and is attached
        to the entity and that the SL created in (1) is no longer
        attached to the entity.
        """
        session = self.prepare()

        sl100 = ServiceLevel(session=session, name='sla1', shares=100)
        entity = entity_class(session=session, name=entity_name)
        self.create_entity_with_service_level(entity=entity, service_level=sl100)

        self.validate_sl_list(session=session, expected_service_levels=[sl100])
        self.validate_attached_slas_list(session=session, entity=entity, expected_service_levels=[sl100])

        sl200 = ServiceLevel(session=session, name='sla2', shares=200).create()
        entity.attach_service_level(service_level=sl200)

        self.validate_sl_list(session=session, expected_service_levels=[sl100, sl200])
        self.validate_attached_slas_list(session=session, entity=entity, expected_service_levels=[sl200])


@pytest.mark.dtest_enterprise
class TestSLANegativeTests(SLATester):
    def test_update_not_existing_sla(self):
        """
        1. Create a ServiceLevel instance (but without creating the
        SL in the db).
        2. Attempt to alter the SL.
        3. Validate that an InvalidRequest error is request with the
        expected error message.
        """
        session = self.prepare()
        sl = ServiceLevel(session=session, name='sla1')
        expected_error = fr"""The service level '{sl.name.replace('"', '')}' doesn't exist."""

        with pytest.raises(InvalidRequest, match=expected_error):
            sl.alter(new_shares=100)

    def test_create_sla_with_more_1000_shares(self):
        """
        Create SL with an invalid value of shares: 1001.
        """
        self._wrong_shares(shares=1001)

    def test_create_sla_with_0_shares(self):
        """
        Create SL with an invalid value of shares: 0.
        """
        self._wrong_shares(shares=0)

    def test_create_sla_with_negative_shares(self):
        """
        Create SL with an invalid value of shares: -1.
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
