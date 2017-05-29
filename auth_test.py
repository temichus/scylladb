"""
All dtest functional test for authentication and authorization tests.

STATE: NOT FULLY IMPLEMENTED
"""

import os
import re
import socket
import subprocess
import time

from cassandra import AuthenticationFailed, Unauthorized, InvalidRequest, AlreadyExists
from cassandra.cluster import NoHostAvailable
from cassandra import Unavailable

from assertions import assert_invalid
from dtest import Tester, debug
from tools import since, new_node, require

from unittest import skip


class TestAuth(Tester):
    """
    Original Class of dtest
    """

    def __init__(self, *args, **kwargs):
        self.ignore_log_patterns = [
            # This one occurs if we do a non-rolling upgrade, the node
            # it's trying to send the migration to hasn't started yet,
            # and when it does, it gets replayed and everything is fine.
            r'Can\'t send migration request: node.*is down',
        ]
        Tester.__init__(self, *args, **kwargs)

    def system_auth_ks_is_alterable_test(self):
        """
        Originally from dtest.
        **Description:**

        **Expected Result:**
        """
        self.prepare(nodes=3)
        debug("nodes started")

        session = self.get_session(user='cassandra', password='cassandra')
        self.assertEquals(1, session.cluster.metadata.keyspaces['system_auth'].replication_strategy.replication_factor)

        session.execute("""
            ALTER KEYSPACE system_auth
                WITH replication = {'class':'SimpleStrategy', 'replication_factor':3};
        """)

        self.assertEquals(3, session.cluster.metadata.keyspaces['system_auth'].replication_strategy.replication_factor)

        # Run repair to workaround read repair issues caused by CASSANDRA-10655
        debug("Repairing before altering RF")
        self.cluster.repair()

        # make sure schema change is persistent
        debug("Stopping cluster..")
        self.cluster.stop()
        debug("Restarting cluster..")
        self.cluster.start(wait_other_notice=True)

        # check each node directly
        for i in range(3):
            debug('Checking node: {i}'.format(i=i))
            node = self.cluster.nodelist()[i]
            session = self.patient_exclusive_cql_connection(node, user='cassandra', password='cassandra')
            self.assertEquals(3, session.cluster.metadata.keyspaces['system_auth'].replication_strategy.replication_factor)

    def login_test(self):
        """
        Originally from dtest.
        **Description:**

        **Expected Result:**
        """
        # also tests default user creation (cassandra/cassandra)
        self.prepare()
        self.get_session(user='cassandra', password='cassandra')
        try:
            self.get_session(user='cassandra', password='badpassword')
        except NoHostAvailable as e:
            assert isinstance(e.errors.values()[0], AuthenticationFailed)
        try:
            self.get_session(user='doesntexist', password='doesntmatter')
        except NoHostAvailable as e:
            assert isinstance(e.errors.values()[0], AuthenticationFailed)
        # Authentication ID must not be null
        try:
            self.get_session(user='', password='')
        except NoHostAvailable as e:
            assert isinstance(e.errors.values()[0], AuthenticationFailed)
            assert 'Authentication ID must not be null' in e.errors.values()[0].message
        # Password must not be null
        try:
            self.get_session(user='cassandra', password='')
        except NoHostAvailable as e:
            assert isinstance(e.errors.values()[0], AuthenticationFailed)
            # Currently the null password can't be identified, comment the assert
            # https://github.com/scylladb/scylla/issues/2274
            # assert 'Password must not be null' in e.errors.values()[0].message

    # from 2.2 role creation is granted by CREATE_ROLE permissions, not superuser status
    # @since('1.2', max_version='2.1.x')
    # we still support NOSUPERUSER!!!
    def only_superuser_can_create_users_test(self):
        """
        Originally from dtest.
        **Description:**

        **Expected Result:**
        """
        self.prepare()

        cassandra = self.get_session(user='cassandra', password='cassandra')
        cassandra.execute("CREATE USER jackob WITH PASSWORD '12345' NOSUPERUSER")

        jackob = self.get_session(user='jackob', password='12345')
        self.assertUnauthorized('Only superusers are allowed to perform CREATE (\[ROLE\|USER\]|USER) queries', jackob, "CREATE USER james WITH PASSWORD '54321' NOSUPERUSER")

    # @since('1.2', max_version='2.1.x')
    # we still support NOSUPERUSER!!!
    def password_authenticator_create_user_requires_password_test(self):
        """
        Originally from dtest.
        **Description:**

        **Expected Result:**
        """
        self.prepare()

        session = self.get_session(user='cassandra', password='cassandra')
        assert_invalid(session, "CREATE USER jackob NOSUPERUSER", 'PasswordAuthenticator requires PASSWORD option')

    def cant_create_existing_user_test(self):
        """
        Originally from dtest.
        **Description:**

        **Expected Result:**
        """
        self.prepare()

        session = self.get_session(user='cassandra', password='cassandra')
        session.execute("CREATE USER 'james@example.com' WITH PASSWORD '12345' NOSUPERUSER")
        assert_invalid(session, "CREATE USER 'james@example.com' WITH PASSWORD '12345' NOSUPERUSER", 'james@example.com already exists')

    def list_users_test(self):
        """
        Originally from dtest.
        **Description:**

        **Expected Result:**
        """
        self.prepare()

        session = self.get_session(user='cassandra', password='cassandra')
        session.execute("CREATE USER alex WITH PASSWORD '12345' NOSUPERUSER")
        session.execute("CREATE USER bob WITH PASSWORD '12345' SUPERUSER")
        session.execute("CREATE USER cathy WITH PASSWORD '12345' NOSUPERUSER")
        session.execute("CREATE USER dave WITH PASSWORD '12345' SUPERUSER")

        rows = list(session.execute("LIST USERS"))
        self.assertEqual(5, len(rows))
        # {username: isSuperuser} dict.
        users = dict([(r[0], r[1]) for r in rows])

        self.assertTrue(users['cassandra'])
        self.assertFalse(users['alex'])
        self.assertTrue(users['bob'])
        self.assertFalse(users['cathy'])
        self.assertTrue(users['dave'])

    def user_cant_drop_themselves_test(self):
        """
        Originally from dtest.
        **Description:**

        **Expected Result:**
        """
        self.prepare()

        session = self.get_session(user='cassandra', password='cassandra')
        # handle different error messages between versions pre and post 2.2.0
        assert_invalid(session, "DROP USER cassandra", "(Users aren't allowed to DROP themselves|Cannot DROP primary role for current login)")

    # from 2.2 role deletion is granted by DROP_ROLE permissions, not superuser status
    # @since('1.2', max_version='2.1.x')
    # we still support NOSUPERUSER!!!
    def only_superusers_can_drop_users_test(self):
        """
        Originally from dtest.
        **Description:**

        **Expected Result:**
        """
        self.prepare()

        cassandra = self.get_session(user='cassandra', password='cassandra')
        cassandra.execute("CREATE USER cathy WITH PASSWORD '12345' NOSUPERUSER")
        cassandra.execute("CREATE USER dave WITH PASSWORD '12345' NOSUPERUSER")
        rows = list(cassandra.execute("LIST USERS"))
        self.assertEqual(3, len(rows))

        cathy = self.get_session(user='cathy', password='12345')
        self.assertUnauthorized('Only superusers are allowed to perform DROP (\[ROLE\|USER\]|USER) queries',
                                cathy, 'DROP USER dave')

        rows = list(cassandra.execute("LIST USERS"))
        self.assertEqual(3, len(rows))

        cassandra.execute('DROP USER dave')
        rows = list(cassandra.execute("LIST USERS"))
        self.assertEqual(2, len(rows))

    def dropping_nonexistent_user_throws_exception_test(self):
        """
        Originally from dtest.
        **Description:**

        **Expected Result:**
        """
        self.prepare()

        session = self.get_session(user='cassandra', password='cassandra')
        assert_invalid(session, 'DROP USER nonexistent', "nonexistent doesn't exist")

    def drop_user_case_sensitive_test(self):
        """
        * Launch a one node cluster
        * Connect as the default superuser
        * Create a user, 'Test'
        * Verify that the drop user statement is case sensitive
        """
        self.prepare()
        cassandra = self.get_session(user='cassandra', password='cassandra')
        cassandra.execute("CREATE USER Test WITH PASSWORD '12345'")

        # Should be invalid, as 'test/TEST' does not exist
        assert_invalid(cassandra, "DROP USER test")
        assert_invalid(cassandra, "DROP USER TEST")

        cassandra.execute("DROP USER Test")
        rows = [x[0] for x in list(cassandra.execute("LIST USERS"))]
        self.assertItemsEqual(rows, ['cassandra'])

        # Should be invalid, as 'Test' does not exist anymore
        assert_invalid(cassandra, "DROP USER test")
        assert_invalid(cassandra, "DROP USER TEST")
        assert_invalid(cassandra, "DROP USER Test")

        cassandra.execute("CREATE USER test WITH PASSWORD '12345'")

        # Should be invalid, as 'TEST/Test' does not exist
        assert_invalid(cassandra, "DROP USER TEST")
        assert_invalid(cassandra, "DROP USER Test")

        cassandra.execute("DROP USER test")
        rows = [x[0] for x in list(cassandra.execute("LIST USERS"))]
        self.assertItemsEqual(rows, ['cassandra'])

        # Should be invalid, as 'test' does not exist anymore
        assert_invalid(cassandra, "DROP USER test")
        assert_invalid(cassandra, "DROP USER TEST")
        assert_invalid(cassandra, "DROP USER Test")

    def alter_user_case_sensitive_test(self):
        """
        * Launch a one node cluster
        * Connect as the default superuser
        * Create a user, 'Test'
        * Verify that ALTER statements on the user are case sensitive
        """
        self.prepare()
        cassandra = self.get_session(user='cassandra', password='cassandra')
        cassandra.execute("CREATE USER Test WITH PASSWORD '12345'")
        cassandra.execute("ALTER USER Test WITH PASSWORD '54321'")
        assert_invalid(cassandra, "ALTER USER test WITH PASSWORD '12345'")
        assert_invalid(cassandra, "ALTER USER TEST WITH PASSWORD '12345'")

        cassandra.execute('DROP USER Test')
        cassandra.execute("CREATE USER test WITH PASSWORD '12345'")
        assert_invalid(cassandra, "ALTER USER Test WITH PASSWORD '12345'")
        assert_invalid(cassandra, "ALTER USER TEST WITH PASSWORD '12345'")
        cassandra.execute("ALTER USER test WITH PASSWORD '54321'")

    def regular_users_can_alter_their_passwords_only_test(self):
        """
        Originally from dtest.
        **Description:**

        **Expected Result:**
        """
        self.prepare()

        cassandra = self.get_session(user='cassandra', password='cassandra')
        cassandra.execute("CREATE USER cathy WITH PASSWORD '12345'")
        cassandra.execute("CREATE USER bob WITH PASSWORD '12345'")

        cathy = self.get_session(user='cathy', password='12345')
        cathy.execute("ALTER USER cathy WITH PASSWORD '54321'")
        cathy = self.get_session(user='cathy', password='54321')
        self.assertUnauthorized("You aren't allowed to alter this user|User cathy does not have sufficient privileges to perform the requested operation",
                                cathy, "ALTER USER bob WITH PASSWORD 'cantchangeit'")

    def users_cant_alter_their_superuser_status_test(self):
        """
        Originally from dtest.
        **Description:**

        **Expected Result:**
        """
        self.prepare()

        session = self.get_session(user='cassandra', password='cassandra')
        self.assertUnauthorized("You aren't allowed to alter your own superuser status",
                                session, "ALTER USER cassandra NOSUPERUSER")

    def only_superuser_alters_superuser_status_test(self):
        """
        Originally from dtest.
        **Description:**

        **Expected Result:**
        """
        self.prepare()

        cassandra = self.get_session(user='cassandra', password='cassandra')
        cassandra.execute("CREATE USER cathy WITH PASSWORD '12345'")

        cathy = self.get_session(user='cathy', password='12345')
        self.assertUnauthorized("Only superusers are allowed to alter superuser status",
                                cathy, "ALTER USER cassandra NOSUPERUSER")

        cassandra.execute("ALTER USER cathy SUPERUSER")

    def altering_nonexistent_user_throws_exception_test(self):
        """
        Originally from dtest.
        **Description:**

        **Expected Result:**
        """
        self.prepare()

        session = self.get_session(user='cassandra', password='cassandra')
        assert_invalid(session, "ALTER USER nonexistent WITH PASSWORD 'doesn''tmatter'", "nonexistent doesn't exist")

    def conditional_create_drop_user_test(self):
        """
        Originally from dtest.
        **Description:**

        **Expected Result:**
        """
        self.prepare()
        session = self.get_session(user='cassandra', password='cassandra')

        users = list(session.execute("LIST USERS"))
        self.assertEqual(1, len(users))  # cassandra

        session.execute("CREATE USER IF NOT EXISTS aleksey WITH PASSWORD 'sup'")
        session.execute("CREATE USER IF NOT EXISTS aleksey WITH PASSWORD 'ignored'")

        users = list(session.execute("LIST USERS"))
        self.assertEqual(2, len(users))  # cassandra + aleksey

        session.execute("DROP USER IF EXISTS aleksey")
        session.execute("DROP USER IF EXISTS aleksey")

        users = list(session.execute("LIST USERS"))
        self.assertEqual(1, len(users))  # cassandra

    def create_ks_auth_test(self):
        """
        Originally from dtest.
        **Description:**

        **Expected Result:**
        """
        self.prepare()

        cassandra = self.get_session(user='cassandra', password='cassandra')
        cassandra.execute("CREATE USER cathy WITH PASSWORD '12345'")

        cathy = self.get_session(user='cathy', password='12345')
        self.assertUnauthorized("User cathy has no CREATE permission on <all keyspaces> or any of its parents",
                                cathy,
                                "CREATE KEYSPACE ks WITH replication = {'class':'SimpleStrategy', 'replication_factor':1}")

        cassandra.execute("GRANT CREATE ON ALL KEYSPACES TO cathy")
        cathy.execute("""CREATE KEYSPACE ks WITH replication = {'class':'SimpleStrategy', 'replication_factor':1}""")

    def create_cf_auth_test(self):
        """
        Originally from dtest.
        **Description:**

        **Expected Result:**
        """
        self.prepare()

        cassandra = self.get_session(user='cassandra', password='cassandra')
        cassandra.execute("CREATE USER cathy WITH PASSWORD '12345'")
        cassandra.execute("CREATE KEYSPACE ks WITH replication = {'class':'SimpleStrategy', 'replication_factor':1}")

        cathy = self.get_session(user='cathy', password='12345')
        self.assertUnauthorized("User cathy has no CREATE permission on <keyspace ks> or any of its parents",
                                cathy, "CREATE TABLE ks.cf (id int primary key)")

        cassandra.execute("GRANT CREATE ON KEYSPACE ks TO cathy")
        cathy.execute("CREATE TABLE ks.cf (id int primary key)")

    def alter_ks_auth_test(self):
        """
        Originally from dtest.
        **Description:**

        **Expected Result:**
        """
        self.prepare()

        cassandra = self.get_session(user='cassandra', password='cassandra')
        cassandra.execute("CREATE USER cathy WITH PASSWORD '12345'")
        cassandra.execute("CREATE KEYSPACE ks WITH replication = {'class':'SimpleStrategy', 'replication_factor':1}")

        cathy = self.get_session(user='cathy', password='12345')
        self.assertUnauthorized("User cathy has no ALTER permission on <keyspace ks> or any of its parents",
                                cathy,
                                "ALTER KEYSPACE ks WITH replication = {'class':'SimpleStrategy', 'replication_factor':2}")

        cassandra.execute("GRANT ALTER ON KEYSPACE ks TO cathy")
        cathy.execute("ALTER KEYSPACE ks WITH replication = {'class':'SimpleStrategy', 'replication_factor':2}")

    @skip('index')
    def alter_cf_auth_test(self):
        """
        * Launch a one node cluster
        * Connect as the default superuser
        * Create a new user, 'cathy', with no permissions
        * Connect as 'cathy'
        * Assert that trying to alter a ks as 'cathy' throws Unauthorized
        * Grant 'cathy' alter permissions
        * Assert that 'cathy' can alter a ks
        """
        self.prepare()

        cassandra = self.get_session(user='cassandra', password='cassandra')
        cassandra.execute("CREATE USER cathy WITH PASSWORD '12345'")
        cassandra.execute("CREATE KEYSPACE ks WITH replication = {'class':'SimpleStrategy', 'replication_factor':1}")
        cassandra.execute("CREATE TABLE ks.cf (id int primary key)")

        cathy = self.get_session(user='cathy', password='12345')
        self.assertUnauthorized("User cathy has no ALTER permission on <table ks.cf> or any of its parents",
                                cathy, "ALTER TABLE ks.cf ADD val int")

        cassandra.execute("GRANT ALTER ON ks.cf TO cathy")
        cathy.execute("ALTER TABLE ks.cf ADD val int")

        cassandra.execute("REVOKE ALTER ON ks.cf FROM cathy")
        self.assertUnauthorized("User cathy has no ALTER permission on <table ks.cf> or any of its parents",
                                cathy, "CREATE INDEX ON ks.cf(val)")

        cassandra.execute("GRANT ALTER ON ks.cf TO cathy")
        cathy.execute("CREATE INDEX ON ks.cf(val)")

        cassandra.execute("REVOKE ALTER ON ks.cf FROM cathy")

        cathy.execute("USE ks")
        self.assertUnauthorized("User cathy has no ALTER permission on <table ks.cf> or any of its parents",
                                cathy, "DROP INDEX cf_val_idx")

        cassandra.execute("GRANT ALTER ON ks.cf TO cathy")
        cathy.execute("DROP INDEX cf_val_idx")

    def alter_cf_auth_test_without_indexes(self):
        """
        * Launch a one node cluster
        * Connect as the default superuser
        * Create a new user, 'cathy', with no permissions
        * Connect as 'cathy'
        * Assert that trying to alter a ks as 'cathy' throws Unauthorized
        * Grant 'cathy' alter permissions
        * Assert that 'cathy' can alter a ks
        """
        self.prepare()

        cassandra = self.get_session(user='cassandra', password='cassandra')
        cassandra.execute("CREATE USER cathy WITH PASSWORD '12345'")
        cassandra.execute("CREATE KEYSPACE ks WITH replication = {'class':'SimpleStrategy', 'replication_factor':1}")
        cassandra.execute("CREATE TABLE ks.cf (id int primary key)")

        cathy = self.get_session(user='cathy', password='12345')
        self.assertUnauthorized("User cathy has no ALTER permission on <table ks.cf> or any of its parents",
                                cathy, "ALTER TABLE ks.cf ADD val int")

        cassandra.execute("GRANT ALTER ON ks.cf TO cathy")
        cathy.execute("ALTER TABLE ks.cf ADD val int")

        cassandra.execute("REVOKE ALTER ON ks.cf FROM cathy")
        self.assertUnauthorized("User cathy has no ALTER permission on <table ks.cf> or any of its parents",
                                cathy, "CREATE INDEX ON ks.cf(val)")

        cassandra.execute("GRANT ALTER ON ks.cf TO cathy")
        cathy.execute("ALTER TABLE ks.cf ADD val2 int")

        cassandra.execute("REVOKE ALTER ON ks.cf FROM cathy")

        cathy.execute("USE ks")
        self.assertUnauthorized("User cathy has no ALTER permission on <table ks.cf> or any of its parents",
                                cathy, "ALTER TABLE ks.cf DROP val2")

        cassandra.execute("GRANT ALTER ON ks.cf TO cathy")
        cathy.execute("ALTER TABLE ks.cf DROP val2")

    @since('3.0')
    def materialized_views_auth_test(self):
        """
        Originally from dtest.
        **Description:**

        **Expected Result:** SKIPPED
        """
        self.prepare(experimental=True)

        cassandra = self.get_session(user='cassandra', password='cassandra')
        cassandra.execute("CREATE USER cathy WITH PASSWORD '12345'")
        cassandra.execute("CREATE KEYSPACE ks WITH replication = {'class':'SimpleStrategy', 'replication_factor':1}")
        cassandra.execute("CREATE TABLE ks.cf (id int primary key, value text)")

        # Try CREATE MV without ALTER permission on base table
        create_mv = "CREATE MATERIALIZED VIEW ks.mv1 AS SELECT * FROM ks.cf WHERE id IS NOT NULL " \
                    "AND value IS NOT NULL PRIMARY KEY (value, id)"
        cathy = self.get_session(user='cathy', password='12345')
        self.assertUnauthorized("User cathy has no ALTER permission on <table ks.cf> or any of its parents",
                                cathy, create_mv)

        # Grant ALTER permission and CREATE MV
        cassandra.execute("GRANT ALTER ON ks.cf TO cathy")
        cathy.execute(create_mv)

        # TRY SELECT MV without SELECT permission on base table
        self.assertUnauthorized("User cathy has no SELECT permission on <table ks.cf> or any of its parents",
                                cathy, "SELECT * FROM ks.mv1")

        # Grant SELECT permission and CREATE MV
        cassandra.execute("GRANT SELECT ON ks.cf TO cathy")
        cathy.execute("SELECT * FROM ks.mv1")

        # Revoke ALTER permission and try DROP MV
        cassandra.execute("REVOKE ALTER ON ks.cf FROM cathy")
        cathy.execute("USE ks")
        self.assertUnauthorized("User cathy has no ALTER permission on <table ks.cf> or any of its parents",
                                cathy, "DROP MATERIALIZED VIEW mv1")

        # GRANT ALTER permission and DROP MV
        cassandra.execute("GRANT ALTER ON ks.cf TO cathy")
        cathy.execute("DROP MATERIALIZED VIEW mv1")

    def drop_ks_auth_test(self):
        """
        Originally from dtest.
        **Description:**

        **Expected Result:**
        """
        self.prepare()

        cassandra = self.get_session(user='cassandra', password='cassandra')
        cassandra.execute("CREATE USER cathy WITH PASSWORD '12345'")
        cassandra.execute("CREATE KEYSPACE ks WITH replication = {'class':'SimpleStrategy', 'replication_factor':1}")

        cathy = self.get_session(user='cathy', password='12345')
        self.assertUnauthorized("User cathy has no DROP permission on <keyspace ks> or any of its parents",
                                cathy, "DROP KEYSPACE ks")

        cassandra.execute("GRANT DROP ON KEYSPACE ks TO cathy")
        cathy.execute("DROP KEYSPACE ks")

    def drop_cf_auth_test(self):
        """
        Originally from dtest.
        **Description:**

        **Expected Result:**
        """
        self.prepare()

        cassandra = self.get_session(user='cassandra', password='cassandra')
        cassandra.execute("CREATE USER cathy WITH PASSWORD '12345'")
        cassandra.execute("CREATE KEYSPACE ks WITH replication = {'class':'SimpleStrategy', 'replication_factor':1}")
        cassandra.execute("CREATE TABLE ks.cf (id int primary key)")

        cathy = self.get_session(user='cathy', password='12345')
        self.assertUnauthorized("User cathy has no DROP permission on <table ks.cf> or any of its parents",
                                cathy, "DROP TABLE ks.cf")

        cassandra.execute("GRANT DROP ON ks.cf TO cathy")
        cathy.execute("DROP TABLE ks.cf")

    def modify_and_select_auth_test(self):
        """
        Originally from dtest.
        **Description:**

        **Expected Result:**
        """
        self.prepare()

        cassandra = self.get_session(user='cassandra', password='cassandra')
        cassandra.execute("CREATE USER cathy WITH PASSWORD '12345'")
        cassandra.execute("CREATE KEYSPACE ks WITH replication = {'class':'SimpleStrategy', 'replication_factor':1}")
        cassandra.execute("CREATE TABLE ks.cf (id int primary key, val int)")

        cathy = self.get_session(user='cathy', password='12345')
        self.assertUnauthorized("User cathy has no SELECT permission on <table ks.cf> or any of its parents",
                                cathy, "SELECT * FROM ks.cf")

        cassandra.execute("GRANT SELECT ON ks.cf TO cathy")
        rows = list(cathy.execute("SELECT * FROM ks.cf"))
        self.assertEquals(0, len(rows))

        self.assertUnauthorized("User cathy has no MODIFY permission on <table ks.cf> or any of its parents",
                                cathy, "INSERT INTO ks.cf (id, val) VALUES (0, 0)")

        self.assertUnauthorized("User cathy has no MODIFY permission on <table ks.cf> or any of its parents",
                                cathy, "UPDATE ks.cf SET val = 1 WHERE id = 1")

        self.assertUnauthorized("User cathy has no MODIFY permission on <table ks.cf> or any of its parents",
                                cathy, "DELETE FROM ks.cf WHERE id = 1")

        self.assertUnauthorized("User cathy has no MODIFY permission on <table ks.cf> or any of its parents",
                                cathy, "TRUNCATE ks.cf")

        cassandra.execute("GRANT MODIFY ON ks.cf TO cathy")
        cathy.execute("INSERT INTO ks.cf (id, val) VALUES (0, 0)")
        cathy.execute("UPDATE ks.cf SET val = 1 WHERE id = 1")
        rows = list(cathy.execute("SELECT * FROM ks.cf"))
        self.assertEquals(2, len(rows))

        cathy.execute("DELETE FROM ks.cf WHERE id = 1")
        rows = list(cathy.execute("SELECT * FROM ks.cf"))
        self.assertEquals(1, len(rows))

        cathy.execute("TRUNCATE ks.cf")
        rows = list(cathy.execute("SELECT * FROM ks.cf"))

        assert len(rows) == 0

    @since('2.2')
    def grant_revoke_without_ks_specified_test(self):
        """
        * Launch a one node cluster
        * Connect as the default superuser
        * Create table ks.cf
        * Create a new users, 'cathy' and 'bob', with no permissions
        * Grant ALL on ks.cf to cathy
        * As cathy, try granting SELECT on cf to bob, without specifying the ks; verify it fails
        * As cathy, USE ks, try again, verify it works this time
        """
        self.prepare()

        cassandra = self.get_session(user='cassandra', password='cassandra')

        cassandra.execute("CREATE KEYSPACE ks WITH replication = {'class':'SimpleStrategy', 'replication_factor':1}")
        cassandra.execute("CREATE TABLE ks.cf (id int primary key, val int)")

        cassandra.execute("CREATE USER cathy WITH PASSWORD '12345'")
        cassandra.execute("CREATE USER bob WITH PASSWORD '12345'")

        cassandra.execute("GRANT ALL ON ks.cf TO cathy")

        cathy = self.get_session(user='cathy', password='12345')
        bob = self.get_session(user='bob', password='12345')

        assert_invalid(cathy, "GRANT SELECT ON cf TO bob", "No keyspace has been specified. USE a keyspace, or explicitly specify keyspace.tablename")
        self.assertUnauthorized("User bob has no SELECT permission on <table ks.cf> or any of its parents",
                                bob, "SELECT * FROM ks.cf")

        cathy.execute("USE ks")
        cathy.execute("GRANT SELECT ON cf TO bob")
        bob.execute("SELECT * FROM ks.cf")

    def grant_revoke_auth_test(self):
        """
        Originally from dtest.
        **Description:**

        **Expected Result:**
        """
        self.prepare()

        cassandra = self.get_session(user='cassandra', password='cassandra')
        cassandra.execute("CREATE USER cathy WITH PASSWORD '12345'")
        cassandra.execute("CREATE USER bob WITH PASSWORD '12345'")

        cathy = self.get_session(user='cathy', password='12345')
        # missing both SELECT and AUTHORIZE
        self.assertUnauthorized("User cathy has no AUTHORIZE permission on <all keyspaces> or any of its parents",
                                cathy, "GRANT SELECT ON ALL KEYSPACES TO bob")

        cassandra.execute("GRANT AUTHORIZE ON ALL KEYSPACES TO cathy")

        # still missing SELECT
        self.assertUnauthorized("User cathy has no SELECT permission on <all keyspaces> or any of its parents",
                                cathy, "GRANT SELECT ON ALL KEYSPACES TO bob")

        cassandra.execute("GRANT SELECT ON ALL KEYSPACES TO cathy")

        # should succeed now with both SELECT and AUTHORIZE
        cathy.execute("GRANT SELECT ON ALL KEYSPACES TO bob")

    def grant_revoke_validation_test(self):
        """
        Originally from dtest.
        **Description:**

        **Expected Result:**
        """
        self.prepare()

        cassandra = self.get_session(user='cassandra', password='cassandra')
        cassandra.execute("CREATE USER cathy WITH PASSWORD '12345'")
        cassandra.execute("CREATE KEYSPACE ks WITH replication = {'class':'SimpleStrategy', 'replication_factor':1}")

        assert_invalid(cassandra, "GRANT ALL ON KEYSPACE nonexistent TO cathy", "<keyspace nonexistent> doesn't exist")

        assert_invalid(cassandra, "GRANT ALL ON KEYSPACE ks TO nonexistent", "(User|Role) nonexistent doesn't exist")

        assert_invalid(cassandra, "REVOKE ALL ON KEYSPACE nonexistent FROM cathy", "<keyspace nonexistent> doesn't exist")

        assert_invalid(cassandra, "REVOKE ALL ON KEYSPACE ks FROM nonexistent", "(User|Role) nonexistent doesn't exist")

    def grant_revoke_cleanup_test(self):
        """
        Originally from dtest.
        **Description:**

        **Expected Result:**
        """
        self.prepare()

        cassandra = self.get_session(user='cassandra', password='cassandra')
        cassandra.execute("CREATE USER cathy WITH PASSWORD '12345'")
        cassandra.execute("CREATE KEYSPACE ks WITH replication = {'class':'SimpleStrategy', 'replication_factor':1}")
        cassandra.execute("CREATE TABLE ks.cf (id int primary key, val int)")
        cassandra.execute("GRANT ALL ON ks.cf TO cathy")

        cathy = self.get_session(user='cathy', password='12345')
        cathy.execute("INSERT INTO ks.cf (id, val) VALUES (0, 0)")
        rows = list(cathy.execute("SELECT * FROM ks.cf"))
        self.assertEquals(1, len(rows))

        # drop and recreate the user, make sure permissions are gone
        cassandra.execute("DROP USER cathy")
        cassandra.execute("CREATE USER cathy WITH PASSWORD '12345'")

        self.assertUnauthorized("User cathy has no MODIFY permission on <table ks.cf> or any of its parents",
                                cathy, "INSERT INTO ks.cf (id, val) VALUES (0, 0)")

        self.assertUnauthorized("User cathy has no SELECT permission on <table ks.cf> or any of its parents",
                                cathy, "SELECT * FROM ks.cf")

        # grant all the permissions back
        cassandra.execute("GRANT ALL ON ks.cf TO cathy")
        cathy.execute("INSERT INTO ks.cf (id, val) VALUES (0, 0)")
        rows = list(cathy.execute("SELECT * FROM ks.cf"))
        self.assertEqual(1, len(rows))

        # drop and recreate the keyspace, make sure permissions are gone
        cassandra.execute("DROP KEYSPACE ks")
        cassandra.execute("CREATE KEYSPACE ks WITH replication = {'class':'SimpleStrategy', 'replication_factor':1}")
        cassandra.execute("CREATE TABLE ks.cf (id int primary key, val int)")

        self.assertUnauthorized("User cathy has no MODIFY permission on <table ks.cf> or any of its parents",
                                cathy, "INSERT INTO ks.cf (id, val) VALUES (0, 0)")

        self.assertUnauthorized("User cathy has no SELECT permission on <table ks.cf> or any of its parents",
                                cathy, "SELECT * FROM ks.cf")

    def permissions_caching_test(self):
        """
        Originally from dtest.
        **Description:**

        **Expected Result:**
        """
        self.prepare(permissions_validity=2000)

        cassandra = self.get_session(user='cassandra', password='cassandra')
        cassandra.execute("CREATE USER cathy WITH PASSWORD '12345'")
        cassandra.execute("CREATE KEYSPACE ks WITH replication = {'class':'SimpleStrategy', 'replication_factor':1}")
        cassandra.execute("CREATE TABLE ks.cf (id int primary key, val int)")

        cathy = self.get_session(user='cathy', password='12345')
        # another user to make sure the cache is at user level
        cathy2 = self.get_session(user='cathy', password='12345')
        cathys = [cathy, cathy2]

        self.assertUnauthorized("User cathy has no SELECT permission on <table ks.cf> or any of its parents",
                                cathy, "SELECT * FROM ks.cf")

        # grant SELECT to cathy
        cassandra.execute("GRANT SELECT ON ks.cf TO cathy")
        # should still fail after 1 second.
        time.sleep(1.0)
        for c in cathys:
            self.assertUnauthorized("User cathy has no SELECT permission on <table ks.cf> or any of its parents",
                                    c, "SELECT * FROM ks.cf")

        # wait until the cache definitely expires and retry - should succeed now
        time.sleep(1.5)
        # refresh of user permissions is done asynchronously, the first request
        # will trigger the refresh, but we'll continue to use the cached set until
        # that completes (CASSANDRA-8194).
        # make a request to trigger the refresh
        try:
            cathy.execute("SELECT * FROM ks.cf")
        except Unauthorized:
            pass

        # once the async refresh completes, both clients should have the granted permissions
        success = False
        cnt = 0
        while not success and cnt < 10:
            try:
                for c in cathys:
                    rows = list(c.execute("SELECT * FROM ks.cf"))
                    self.assertEqual(0, len(rows))
                success = True
            except Unauthorized:
                pass
            cnt += 1
            time.sleep(0.1)

        assert success

    @require("2344")
    def list_permissions_test(self):
        """
        Originally from dtest.
        **Description:**

        **Expected Result:**
        """
        self.prepare()

        cassandra = self.get_session(user='cassandra', password='cassandra')
        cassandra.execute("CREATE USER cathy WITH PASSWORD '12345'")
        cassandra.execute("CREATE USER bob WITH PASSWORD '12345'")
        cassandra.execute("CREATE KEYSPACE ks WITH replication = {'class':'SimpleStrategy', 'replication_factor':1}")
        cassandra.execute("CREATE TABLE ks.cf (id int primary key, val int)")
        cassandra.execute("CREATE TABLE ks.cf2 (id int primary key, val int)")

        cassandra.execute("GRANT CREATE ON ALL KEYSPACES TO cathy")
        cassandra.execute("GRANT ALTER ON KEYSPACE ks TO bob")
        cassandra.execute("GRANT MODIFY ON ks.cf TO cathy")
        cassandra.execute("GRANT DROP ON ks.cf TO bob")
        cassandra.execute("GRANT MODIFY ON ks.cf2 TO bob")
        cassandra.execute("GRANT SELECT ON ks.cf2 TO cathy")

        all_permissions = [('cathy', '<all keyspaces>', 'CREATE'),
                           ('cathy', '<table ks.cf>', 'MODIFY'),
                           ('cathy', '<table ks.cf2>', 'SELECT'),
                           ('bob', '<keyspace ks>', 'ALTER'),
                           ('bob', '<table ks.cf>', 'DROP'),
                           ('bob', '<table ks.cf2>', 'MODIFY')]

        # CASSANDRA-7216 automatically grants permissions on a role to its creator
        if self.cluster.cassandra_version() >= '2.2':
            all_permissions.extend(data_resource_creator_permissions('cassandra', '<keyspace ks>'))
            all_permissions.extend(data_resource_creator_permissions('cassandra', '<table ks.cf>'))
            all_permissions.extend(data_resource_creator_permissions('cassandra', '<table ks.cf2>'))
            all_permissions.extend(role_creator_permissions('cassandra', '<role bob>'))
            all_permissions.extend(role_creator_permissions('cassandra', '<role cathy>'))

        self.assertPermissionsListed(all_permissions, cassandra, "LIST ALL PERMISSIONS")

        self.assertPermissionsListed([('cathy', '<all keyspaces>', 'CREATE'),
                                      ('cathy', '<table ks.cf>', 'MODIFY'),
                                      ('cathy', '<table ks.cf2>', 'SELECT')],
                                     cassandra, "LIST ALL PERMISSIONS OF cathy")

        expected_permissions = [('cathy', '<table ks.cf>', 'MODIFY'), ('bob', '<table ks.cf>', 'DROP')]
        if self.cluster.cassandra_version() >= '2.2':
            expected_permissions.extend(data_resource_creator_permissions('cassandra', '<table ks.cf>'))
        self.assertPermissionsListed(expected_permissions, cassandra, "LIST ALL PERMISSIONS ON ks.cf NORECURSIVE")

        expected_permissions = [('cathy', '<table ks.cf2>', 'SELECT')]
        # CASSANDRA-7216 automatically grants permissions on a role to its creator
        if self.cluster.cassandra_version() >= '2.2':
            expected_permissions.append(('cassandra', '<table ks.cf2>', 'SELECT'))
            expected_permissions.append(('cassandra', '<keyspace ks>', 'SELECT'))
        self.assertPermissionsListed(expected_permissions, cassandra, "LIST SELECT ON ks.cf2")

        self.assertPermissionsListed([('cathy', '<all keyspaces>', 'CREATE'),
                                      ('cathy', '<table ks.cf>', 'MODIFY')],
                                     cassandra, "LIST ALL ON ks.cf OF cathy")

        bob = self.get_session(user='bob', password='12345')
        self.assertPermissionsListed([('bob', '<keyspace ks>', 'ALTER'),
                                      ('bob', '<table ks.cf>', 'DROP'),
                                      ('bob', '<table ks.cf2>', 'MODIFY')],
                                     bob, "LIST ALL PERMISSIONS OF bob")

        self.assertUnauthorized("You are not authorized to view everyone's permissions",
                                bob, "LIST ALL PERMISSIONS")

        self.assertUnauthorized("You are not authorized to view cathy's permissions",
                                bob, "LIST ALL PERMISSIONS OF cathy")

    def type_auth_test(self):
        """
        Originally from dtest..
        **Description:**

        **Expected Result:**
        """
        self.prepare()

        cassandra = self.get_session(user='cassandra', password='cassandra')
        cassandra.execute("CREATE USER cathy WITH PASSWORD '12345'")
        cassandra.execute("CREATE KEYSPACE ks WITH replication = {'class':'SimpleStrategy', 'replication_factor':1}")

        cathy = self.get_session(user='cathy', password='12345')
        self.assertUnauthorized("User cathy has no CREATE permission on <keyspace ks> or any of its parents",
                                cathy, "CREATE TYPE ks.address (street text, city text)")
        self.assertUnauthorized("User cathy has no ALTER permission on <keyspace ks> or any of its parents",
                                cathy, "ALTER TYPE ks.address ADD zip_code int")
        self.assertUnauthorized("User cathy has no DROP permission on <keyspace ks> or any of its parents",
                                cathy, "DROP TYPE ks.address")

        cassandra.execute("GRANT CREATE ON KEYSPACE ks TO cathy")
        cathy.execute("CREATE TYPE ks.address (street text, city text)")
        cassandra.execute("GRANT ALTER ON KEYSPACE ks TO cathy")
        cathy.execute("ALTER TYPE ks.address ADD zip_code int")
        cassandra.execute("GRANT DROP ON KEYSPACE ks TO cathy")
        cathy.execute("DROP TYPE ks.address")

    def _check_session_available(self, session, expect_rf_err=False,
                                 expect_auth_err=False, expect_invalid_req=False):
        try:
            rows = list(session.execute('LIST USERS'))
            debug('Debug users list: %s' % rows)
            assert len(rows) > 0, "Failed to get user list from session"
        except Unavailable as e:
            debug('Debug: _check_session_available: Unavailable Exception')
            if expect_rf_err:
                assert e.alive_replicas != e.required_replicas, e.message
                debug("Good: session isn't available (rf error) as expected")
            else:
                debug("Fail: session isn't available, but not expected error")
                raise
        except NoHostAvailable as e:
            debug(e.errors)
            if expect_auth_err:
                assert isinstance(e.errors.values()[0], AuthenticationFailed)
                debug("Good: session isn't available (auth err) as expected")
            else:
                debug("Fail: session isn't available, but not expected error")
                raise
        except InvalidRequest as e:
            debug(e)
            if expect_invalid_req:
                debug("Good: session isn't available (invalid request) as expected")
            else:
                debug("Fail: session isn't available, but not expected error")
                raise

        if not (expect_rf_err or expect_auth_err or expect_invalid_req):
            debug("Good: session is available as expected")

    def kill_the_node_with_the_auth_info_test(self):
        """
        **Description:** Killing the node (`killall scylla`) that has authentication info (when RF=1).
        **Expected Result:** Cluster is unavailable - connection failed.
        """
        self.prepare(nodes=2)
        debug('Cluster with 2 nodes started')

        [node1, node2] = self.cluster.nodelist()
        session = self.get_session(node_idx=0, user='cassandra',
                                   password='cassandra')
        debug('Successfully get the session from node1')
        # make sure session works
        self._check_session_available(session)

        # verify the replication_factor of system_auth keyspace is 1
        rf = session.cluster.metadata.keyspaces['system_auth'].replication_strategy.replication_factor
        debug('system_auth rf: %s' % rf)
        self.assertEquals(1, rf, "RF of system_auth isn't 1")

        # check the replicas endpoint of system_auth.user:cassandra
        out, err = node1.nodetool("getendpoints system_auth users cassandra")
        debug('Endpoints of system_auth.users:cassandra : %s' % out.strip().split('\n'))
        rf_address = out.strip().split('\n')[0]

        src_node = node1
        rf_node = node2
        if node1.address() == rf_address:
            src_node = node2
            rf_node = node1

        assert rf_node.name.startswith('node')
        rf_node_idx = int(rf_node.name[4:]) - 1

        # re-get session from rf node before killing node
        session = self.get_session(node_idx=rf_node_idx, user='cassandra',
                                   password='cassandra')

        debug('Kill src node(%s: %s) to break Auth info' % (src_node.name, src_node.address()))
        src_node.stop(gently=False)

        debug('Try to re-get session from first rf endpoint(%s: %s)' % (rf_node.name, rf_address))
        try:
            new_session = self.get_session(node_idx=rf_node_idx,
                                           user='cassandra',
                                           password='cassandra')
        except NoHostAvailable as e:
            debug(e.errors)
            assert isinstance(e.errors.values()[0], AuthenticationFailed)

        debug('Check if the new session works')
        self._check_session_available(new_session, expect_rf_err=True)

        debug('Check if the first session still works')
        self._check_session_available(session, expect_rf_err=True)

    def kill_one_of_the_nodes_with_the_auth_info_test(self):
        """
        **Description:** Killing the node that has authentication info (when RF>=2).
        **Expected Result:** Cluster is available - successful connection.
        """
        self.prepare(nodes=3)
        debug('Cluster with 3 nodes started')

        [node1, node2, node3] = self.cluster.nodelist()
        session = self.get_session(node_idx=0, user='cassandra', password='cassandra')
        debug('Successfully get the session from node1')
        # make sure session works
        self._check_session_available(session)

        # change rf RF of system_auth to 2
        session.execute("alter keyspace system_auth with replication = {'class': 'org.apache.cassandra.locator.SimpleStrategy', 'replication_factor':2};")
        rf = session.cluster.metadata.keyspaces['system_auth'].replication_strategy.replication_factor
        debug('Current RF of system_auth is %s' % rf)
        self.assertEquals(2, rf)

        # check the replicas endpoint of system_auth.user:cassandra
        out, err = node1.nodetool("getendpoints system_auth users cassandra")
        debug('Endpoints of system_auth.users:cassandra : %s' % out.strip().split('\n'))
        rf_addresses = out.strip().split('\n')

        for i in self.cluster.nodelist():
            if i.address() not in rf_addresses:
                src_node = i
            if i.address() == rf_addresses[0]:
                rf_node = i

        assert rf_node.name.startswith('node')
        rf_node_idx = int(rf_node.name[4:]) - 1

        # re-get session from rf node before killing node
        session = self.get_session(node_idx=rf_node_idx, user='cassandra',
                                   password='cassandra')

        debug('Kill src node(%s: %s) to break Auth info' % (src_node.name, src_node.address()))
        src_node.stop(gently=False)

        debug('Try to re-get session from first rf endpoint(%s: %s)' % (rf_node.name, rf_addresses[0]))
        try:
            new_session = self.get_session(node_idx=rf_node_idx,
                                           user='cassandra',
                                           password='cassandra')
        except NoHostAvailable as e:
            debug(e.errors)
            assert isinstance(e.errors.values()[0], AuthenticationFailed)

        debug('Check if the new session works')
        self._check_session_available(new_session)

        debug('Check if the first session still works')
        self._check_session_available(session)

    def dropping_keyspace_system_auth_1_node_test(self):
        """
        **Description:** try to drop system_auth table
        **Expected Result:** we should not be able to drop system_auth
        """
        self.prepare()
        debug('Cluster with 1 nodes started')

        # node = self.cluster.nodelist()[0]
        session = self.get_session(node_idx=0, user='cassandra',
                                   password='cassandra')
        debug('Successfully get the session from node1')
        # make sure session works
        self._check_session_available(session)

        # expected message like "Cannot DROP <keyspace system_auth>"
        try:
            session.execute("DROP KEYSPACE system_auth")
        except Unauthorized as e:
            self.assertEquals(e.message,
                              'Error from server: code=2100 [Unauthorized] message="Cannot DROP <keyspace system_auth>"')

        debug('Try to re-get session from first endpoint')
        new_session = self.get_session(node_idx=0,
                                           user='cassandra',
                                           password='cassandra')
        self._check_session_available(new_session)

        debug('Check if the first session still works')
        self._check_session_available(session)

    def dropping_keyspace_system_auth_2_nodes_test(self):
        """
        **Description:** Dropping keyspace system_auth with 2 nodes (when RF=1).
        **Expected Result:** we should not be able to drop system_auth
        """
        self.prepare(nodes=2)
        debug('Cluster with 2 nodes started')

        [node1, node2] = self.cluster.nodelist()
        session = self.get_session(node_idx=0, user='cassandra',
                                   password='cassandra')
        debug('Successfully get the session from node1')
        # make sure session works
        self._check_session_available(session)

        # verify the replication_factor of system_auth keyspace is 1
        rf = session.cluster.metadata.keyspaces['system_auth'].replication_strategy.replication_factor
        debug('system_auth rf: %s' % rf)
        self.assertEquals(1, rf, "RF of system_auth isn't 1")

        # check the replicas endpoint of system_auth.user:cassandra
        out, err = node1.nodetool("getendpoints system_auth users cassandra")
        debug('Endpoints of system_auth.users:cassandra : %s' % out.strip().split('\n'))
        self.assertEqual(1, len(out.strip().split('\n')), "1 node expected")
        rf_address = out.strip().split('\n')[0]

        rf_node = node2
        if node1.address() == rf_address:
            rf_node = node1

        assert rf_node.name.startswith('node')
        rf_node_idx = int(rf_node.name[4:]) - 1

        # re-get session from rf node before dropping keyspace system_auth
        session = self.get_session(node_idx=rf_node_idx, user='cassandra',
                                   password='cassandra')

        debug('drop keyspace system_auth')
        try:
            session.execute("DROP KEYSPACE system_auth")
        except Unauthorized as e:
            self.assertEquals(e.message,
                              'Error from server: code=2100 [Unauthorized] message="Cannot DROP <keyspace system_auth>"')

        debug('Try to re-get session from first rf endpoint(%s: %s)' % (rf_node.name, rf_address))
        new_session = self.get_session(node_idx=rf_node_idx,
                                           user='cassandra',
                                           password='cassandra')
        self._check_session_available(new_session)

        debug('Check if the first session still works')
        self._check_session_available(session)

    def dropping_one_replica_of_keyspace_system_auth(self):
        """
        **Description:** Dropping keyspace system_auth (when RF>=2).
        **Expected Result:** Cluster is unavailable - connection failed.
        """
        self.prepare(nodes=2)
        debug('Cluster with 2 nodes started')

        [node1, node2] = self.cluster.nodelist()
        session = self.get_session(node_idx=0, user='cassandra',
                                   password='cassandra')
        debug('Successfully get the session from node1')
        # make sure session works
        self._check_session_available(session)

        # verify the replication_factor of system_auth keyspace is 1
        rf = session.cluster.metadata.keyspaces['system_auth'].replication_strategy.replication_factor
        debug('system_auth rf: %s' % rf)
        self.assertEquals(1, rf, "RF of system_auth isn't 1")

        # change rf RF of system_auth to 2
        session.execute("alter keyspace system_auth with replication = {'class': 'org.apache.cassandra.locator.SimpleStrategy', 'replication_factor':2};")
        # verify the replication_factor of system_auth keyspace is 2 now
        rf = session.cluster.metadata.keyspaces['system_auth'].replication_strategy.replication_factor
        debug('Current RF of system_auth is %s' % rf)
        self.assertEquals(2, rf)

        # check the replicas endpoint of system_auth.user:cassandra
        out, err = node1.nodetool("getendpoints system_auth users cassandra")
        debug('Endpoints of system_auth.users:cassandra : %s' % out.strip().split('\n'))
        self.assertEqual(2, len(out.strip().split('\n')), "2 nodes expected")

        debug('drop keyspace system_auth')
        session.execute("DROP KEYSPACE system_auth")

        # verify connection to all nodes
        for n in xrange(2):
            debug('Try to re-get session from first rf endpoint(node%s)' % n)
            try:
                new_session = self.get_session(node_idx=n,
                                           user='cassandra',
                                           password='cassandra')
            except NoHostAvailable as e:
                debug(e.errors)
                assert isinstance(e.errors.values()[0], AuthenticationFailed)
            else:
                debug('Check if the new session works')
                self._check_session_available(new_session, expect_auth_err=True, expect_invalid_req=True)

        debug('Check if the first session still works')
        self._check_session_available(session, expect_auth_err=True, expect_invalid_req=True)

    def kill_all_nodes_with_the_auth_info_except_one_test(self):
        """
        **Description:** Set RF of system_auth to 3, kill two nodes.
        **Expected Result:** Cluster is unavailable - connection failed.
        **Re-start 1 node. Connection to 2 successful.
        **Re-start 2 node. Connection to 3 nodes successful.
        """

        self.prepare(nodes=3)
        debug('Cluster with 3 nodes started')

        nodes = self.cluster.nodelist()
        session = self.get_session(node_idx=0, user='cassandra', password='cassandra')
        debug('Successfully get the session from node1')
        # make sure session works
        self._check_session_available(session)

        # change rf RF of system_auth to 3
        session.execute("alter keyspace system_auth with replication = {'class': 'org.apache.cassandra.locator.SimpleStrategy', 'replication_factor':3};")
        self.assertEquals(3, self.get_session(node_idx=0, user='cassandra', password='cassandra').
                          cluster.metadata.keyspaces['system_auth'].replication_strategy.replication_factor)

        # check the replicas endpoint of system_auth.user:cassandra
        out, err = nodes[0].nodetool("getendpoints system_auth users cassandra")
        debug('Endpoints of system_auth.users:cassandra : %s' % out.strip().split('\n'))
        self.assertEqual(3, len(out.strip().split('\n')), "3 nodes expected")

        # re-get session from rf node before killing node
        sessions = []
        for i in xrange(3):
            sessions.append(self.get_session(node_idx=i, user='cassandra',
                                   password='cassandra'))

        nodes[1].stop(wait_other_notice=True, gently=True)
        nodes[2].stop(wait_other_notice=True, gently=True)
        self._check_session_available(session, expect_auth_err=True, expect_invalid_req=True)

        for i in xrange(3):
            debug('Try to re-get session from %s: %s)' % (nodes[i].name, nodes[i].address()))
            try:
                self.get_session(node_idx=i,
                                              user='cassandra',
                                              password='cassandra')
            except NoHostAvailable as e:
                debug(e.errors)
                if i in [0, 3]:
                    assert isinstance(e.errors.values()[0], AuthenticationFailed)
                else:
                    assert isinstance(e.errors.values()[0], socket.error)
            else:
                if j == 1:
                    self.fail("Connection should not be created")
        nodes[1].start(wait_other_notice=True)
        # connection to 2 nodes should be ok
        for i in xrange(2):
            debug('Try to re-get session from %s: %s)' % (nodes[i].name, nodes[i].address()))
            self._check_session_available(
                self.get_session(node_idx=i, user='cassandra', password='cassandra'))

        try:
            self.get_session(node_idx=i, user='cassandra', password='cassandra')
        except NoHostAvailable as e:
            debug(e.errors)

        nodes[2].start(wait_other_notice=True)
        # connection to all nodes should be ok
        for i in xrange(3):
            debug('Try to re-get session from %s: %s)' % (nodes[i].name, nodes[i].address()))
            self._check_session_available(
                self.get_session(node_idx=i, user='cassandra', password='cassandra'))

        debug('Check if the first session still works')
        self._check_session_available(session, expect_auth_err=True, expect_invalid_req=True)

    def drop_keyspace_system_auth_1_node_test(self):
        """
        **Description:** try to drop system_auth table
        **Expected Result:** we should not be able to drop system_auth
        """
        self.prepare()
        debug('Cluster with 1 nodes started')

        # node = self.cluster.nodelist()[0]
        session = self.get_session(node_idx=0, user='cassandra',
                                   password='cassandra')
        debug('Successfully get the session from node1')
        # make sure session works
        self._check_session_available(session)

        # expected message like "Cannot DROP <keyspace system_auth>"
        try:
            session.execute("DROP KEYSPACE system_auth")
        except Unauthorized as e:
            self.assertEquals(e.message,
                              'Error from server: code=2100 [Unauthorized] message="Cannot DROP <keyspace system_auth>"')

    def change_setting_to_noauth_after_system_auth_was_lost_test(self):
        """
        **Description:** after the auth info is lost, change the setting of a node
        to no auth (while the node is down), force a client to connect to that node.
        **Expected Result:** Cluster is available but connection failed.
        """
        self.prepare()
        session = self.get_session(user='cassandra', password='cassandra')
        self._check_session_available(session)
        self.cluster.stop()
        config = {'authenticator': 'org.apache.cassandra.auth.AllowAllAuthenticator',
                  'authorizer': 'org.apache.cassandra.auth.AllowAllAuthorizer'}
        self.cluster.set_configuration_options(values=config)
        self.cluster.start(wait_for_binary_proto=True)

        for session in [self.get_session(),
                        self.get_session(user='cassandra', password='cassandra')]:
            try:
                session.execute("LIST USERS")
                self.fail('You have to be logged in and not anonymous to perform this request!')
            except Unauthorized as e:
                self.assertEquals(e.message,
                                  'Error from server: code=2100 [Unauthorized] message="You have to be logged in and not anonymous to perform this request"')

    def restart_node_doesnt_lose_auth_data_test(self):
        """
        * Launch a one node cluster
        * Connect as the default superuser
        * Create some new users, grant them permissions
        * Stop the cluster, switch to AllowAll auth, restart the cluster
        * Stop the cluster, switch back to auth, restart the cluster
        * Check all user auth data was preserved
        """
        self.prepare()
        cassandra = self.get_session(user='cassandra', password='cassandra')
        cassandra.execute("CREATE USER cathy WITH PASSWORD '12345'")
        cassandra.execute("CREATE USER philip WITH PASSWORD 'strongpass'")
        cassandra.execute("CREATE KEYSPACE ks WITH replication = {'class':'SimpleStrategy', 'replication_factor':1}")
        cassandra.execute("CREATE TABLE ks.cf (id int PRIMARY KEY)")
        cassandra.execute("GRANT ALL ON ks.cf to philip")

        self.cluster.stop()
        config = {'authenticator': 'org.apache.cassandra.auth.AllowAllAuthenticator',
                  'authorizer': 'org.apache.cassandra.auth.AllowAllAuthorizer'}
        self.cluster.set_configuration_options(values=config)
        self.cluster.start(wait_for_binary_proto=True)

        self.cluster.stop()
        config = {'authenticator': 'org.apache.cassandra.auth.PasswordAuthenticator',
                  'authorizer': 'org.apache.cassandra.auth.CassandraAuthorizer'}
        self.cluster.set_configuration_options(values=config)
        self.cluster.start(wait_for_binary_proto=True)

        philip = self.get_session(user='philip', password='strongpass')
        cathy = self.get_session(user='cathy', password='12345')
        self.assertUnauthorized("User cathy has no SELECT permission on <table ks.cf> or any of its parents",
                                cathy, "SELECT * FROM ks.cf")

        philip.execute("SELECT * FROM ks.cf")

    def system_keyspace_sensitive_test(self):
        """
        * Launch a one node cluster
        * Try to create KEYSPACEs like: 'SYSTEM_tRaCeS', 'SYSTEM_aUtH'
        * Creation should be failed because system keyspaces is not user-modifiable.
        * Then drop system keyspaces - failed too.
        """
        self.prepare()
        session = self.get_session(user='cassandra', password='cassandra')
        try:
            session.execute("create KEYSPACE SyStEM WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}")
            self.fail("system keyspace is not user-modifiable")
        except InvalidRequest as e:
            self.assertEquals(e.message, 'Error from server: code=2200 [Invalid query] message="system keyspace is not user-modifiable"')

        for name in ['SYSTEM_tRaCeS', 'SYSTEM_aUtH']:
            try:
                session.execute(
                    "create KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}" % name)
                self.fail("Keyspace %s shouldn't be created")
            except AlreadyExists as e:
                self.assertEquals(e.message, "Keyspace '%s' already exists" % name.lower())

        try:
            session.execute("drop KEYSPACE system")
            self.fail("system keyspace is not user-modifiable")
        except Unauthorized as e:
            self.assertEquals(e.message, 'Error from server: code=2100 [Unauthorized] message="system keyspace is not user-modifiable."')
        # https://github.com/scylladb/scylla/issues/2338
        """for name in ['SYSTEM_tRaCeS', 'SYSTEM_aUtH']:
            try:
                session.execute(
                    "drop KEYSPACE %s" % name)
                self.fail("Keyspace %s shouldn't be deleted")
            except InvalidRequest as e:
                self.assertEquals(e.message, 'Cannot DROP <keyspace %s>' % name.lower())"""

    def remove_dead_node_test(self):
        """
        **Description:** Run "nodetool removenode"' on the dead node (when RF=2).
        **Expected Result:** Cluster is available - successful connection.
        """
        self.prepare(nodes=3)
        debug('Cluster with 3 nodes started')

        [node1, node2, node3] = self.cluster.nodelist()
        session = self.get_session(node_idx=0, user='cassandra', password='cassandra')
        debug('Successfully get the session from node1')
        # make sure session works
        self._check_session_available(session)

        # change rf RF of system_auth to 2
        session.execute("alter keyspace system_auth with replication = {'class': 'org.apache.cassandra.locator.SimpleStrategy', 'replication_factor':2};")

        node2_hostid = node2.hostid()
        node2.stop(wait_other_notice=True, gently=False)
        node1.nodetool("removenode %s" % node2_hostid)
        session = self.get_session(node_idx=0, user='cassandra', password='cassandra')
        self._check_session_available(session)

    @require("2339")
    def remove_dead_node_consistency_failed_test(self):
        """
        **Description:** Run "nodetool removenode"' on the dead node (when RF=2).
        **Expected Result:** Cluster is available - successful connection.
        """
        self.prepare(nodes=2)
        debug('Cluster with 2 nodes started')

        [node1, node2] = self.cluster.nodelist()
        session = self.get_session(node_idx=0, user='cassandra', password='cassandra')
        debug('Successfully get the session from node1')
        # make sure session works
        self._check_session_available(session)

        # change rf RF of system_auth to 2
        session.execute("alter keyspace system_auth with replication = {'class': 'org.apache.cassandra.locator.SimpleStrategy', 'replication_factor':2};")

        node2_hostid = node2.hostid()
        node2.stop(wait_other_notice=True, gently=False)
        node1.nodetool("removenode %s" % node2_hostid)
        try:
            self.get_session(node_idx=0, user='cassandra', password='cassandra')
        except NoHostAvailable as e:
            debug(e.errors)
            assert isinstance(e.errors.values()[0], AuthenticationFailed)
            self.asserTrue('Cannot achieve consistency level QUORUM' in e.errors.values()[0].message)

    @skip('not-implemented')
    def manually_copy_system_auth_files_after_system_auth_was_lost_test(self):
        """
        **Description:** after the auth info is lost (dropping system_auth when RF=1), Upload system_auth keyspace
        with scp to other nodes, do "nodetool refresh".
        **Expected Result:** Cluster is unavailable - connection failed.
        """

    @skip('not-implemented')
    def manually_backup_and_restore_system_auth_test(self):
        """
        **Description:** Backup `system_auth` directory from the data directory to localhost, drop system_auth,
        turn off the auth, upload the auto info and turn on auth.
        **Expected Result:** Auth is recovered, Cluster is available - successful connection.
        """
        raise NotImplementedError

    @skip('not-implemented')
    def snapshot_back_and_restore_system_auth_test(self):
        """
        **Description:** Backup data by creating snapshot, try to recover auth info from snapshot.
        **Expected Result:** Auth is recovered, Cluster is available - successful connection.
        """
        raise NotImplementedError

    def all_authorization_operations_test(self):
        """
        **Description:** Test all authorization operations, actions and applied objects.
        **Expected Result:** All commands run successfully, no crash is triggered.
        """
        self.prepare()

        cassandra = self.get_session(user='cassandra', password='cassandra')
        cassandra.execute("CREATE USER cathy WITH PASSWORD '12345'")
        cassandra.execute("CREATE USER bob WITH PASSWORD '12345'")
        cassandra.execute("CREATE USER dave WITH PASSWORD '12345'")
        cassandra.execute("CREATE USER anna WITH PASSWORD '12345'")
        cassandra.execute("CREATE USER chuk WITH PASSWORD '12345'")
        cassandra.execute("CREATE KEYSPACE ks WITH replication = {'class':'SimpleStrategy', 'replication_factor':1}")
        cassandra.execute("CREATE TABLE ks.cf (id int primary key, val int)")
        cassandra.execute("CREATE TABLE ks.cf2 (id int primary key, val int)")

        cassandra.execute("GRANT CREATE ON ALL KEYSPACES TO cathy")
        cassandra.execute("GRANT ALTER ON KEYSPACE ks TO bob")
        cassandra.execute("GRANT SELECT ON ALL KEYSPACES TO dave")
        cassandra.execute("GRANT ALL ON ks.cf TO dave")
        cassandra.execute("GRANT MODIFY ON KEYSPACE ks TO anna")
        cassandra.execute("GRANT MODIFY ON ks.cf TO cathy")
        cassandra.execute("GRANT DROP ON ks.cf TO bob")
        cassandra.execute("GRANT MODIFY ON ks.cf2 TO bob")
        cassandra.execute("GRANT SELECT ON ks.cf2 TO cathy")
        cassandra.execute("GRANT ALL PERMISSIONS ON ks.cf2 TO chuk")

        all_permissions = [('anna', '<keyspace ks>', 'MODIFY'),
                           ('bob', '<keyspace ks>', 'ALTER'),
                           ('bob', '<table ks.cf>', 'DROP'),
                           ('bob', '<table ks.cf2>', 'MODIFY'),
                           ('cathy', '<all keyspaces>', 'CREATE'),
                           ('cathy', '<table ks.cf>', 'MODIFY'),
                           ('cathy', '<table ks.cf2>', 'SELECT'),
                           ('chuk', '<table ks.cf2>', 'ALTER'),
                           ('chuk', '<table ks.cf2>', 'AUTHORIZE'),
                           ('chuk', '<table ks.cf2>', 'CREATE'),
                           ('chuk', '<table ks.cf2>', 'DROP'),
                           ('chuk', '<table ks.cf2>', 'MODIFY'),
                           ('chuk', '<table ks.cf2>', 'SELECT'),
                           ('dave', '<all keyspaces>', 'SELECT'),
                           ('dave', '<table ks.cf>', 'ALTER'),
                           ('dave', '<table ks.cf>', 'AUTHORIZE'),
                           ('dave', '<table ks.cf>', 'CREATE'),
                           ('dave', '<table ks.cf>', 'DROP'),
                           ('dave', '<table ks.cf>', 'MODIFY'),
                           ('dave', '<table ks.cf>', 'SELECT')]

        self.assertPermissionsListed(all_permissions, cassandra, "LIST ALL PERMISSIONS")

        self.assertPermissionsListed([('cathy', '<all keyspaces>', 'CREATE'),
                                      ('cathy', '<table ks.cf>', 'MODIFY'),
                                      ('cathy', '<table ks.cf2>', 'SELECT')],
                                     cassandra, "LIST ALL PERMISSIONS OF cathy")

        expected_permissions = [('bob', '<table ks.cf>', 'DROP'),
                                ('cathy', '<table ks.cf>', 'MODIFY'),
                                ('dave', '<table ks.cf>', 'ALTER'),
                                ('dave', '<table ks.cf>', 'AUTHORIZE'),
                                ('dave', '<table ks.cf>', 'CREATE'),
                                ('dave', '<table ks.cf>', 'DROP'),
                                ('dave', '<table ks.cf>', 'MODIFY'),
                                ('dave', '<table ks.cf>', 'SELECT')]
        self.assertPermissionsListed(expected_permissions, cassandra, "LIST ALL PERMISSIONS ON ks.cf NORECURSIVE")

        expected_permissions = [('cathy', '<table ks.cf2>', 'SELECT'),
                                ('chuk', '<table ks.cf2>', 'SELECT'),
                                ('dave', '<all keyspaces>', 'SELECT')]
        self.assertPermissionsListed(expected_permissions, cassandra, "LIST SELECT ON ks.cf2")

        self.assertPermissionsListed([('cathy', '<all keyspaces>', 'CREATE'),
                                      ('cathy', '<table ks.cf>', 'MODIFY')],
                                     cassandra, "LIST ALL ON ks.cf OF cathy")

        bob = self.get_session(user='bob', password='12345')
        self.assertPermissionsListed([('bob', '<keyspace ks>', 'ALTER'),
                                      ('bob', '<table ks.cf>', 'DROP'),
                                      ('bob', '<table ks.cf2>', 'MODIFY')],
                                     bob, "LIST ALL PERMISSIONS OF bob")

        self.assertUnauthorized("You are not authorized to view everyone's permissions",
                                bob, "LIST ALL PERMISSIONS")

        self.assertUnauthorized("You are not authorized to view cathy's permissions",
                                bob, "LIST ALL PERMISSIONS OF cathy")

    def authentication_enabled_only_in_one_node_test(self):
        """
        **Description:** Authentication is enabled only in one node while disabled in others -
                         try to connect all node one by one.
        **Expected Result:** AuthenticationFailed failed.
        """
        self.prepare()

        node = new_node(self.cluster, bootstrap=False)

        # remove authenticator/authorizer from second node
        data_dir = os.path.join(node.get_path(), 'conf/scylla.yaml')
        cmd = 'sed -i.bak /authorizer/d "%s"' % data_dir
        subprocess.Popen(cmd.split(), stdout=subprocess.PIPE)
        cmd = 'sed -i.bak /authenticator/d "%s"' % data_dir
        subprocess.Popen(cmd.split(), stdout=subprocess.PIPE)

        node.start(wait_for_binary_proto=True)
        time.sleep(10)
        try:
            session = self.get_session(node_idx=0, user='cassandra', password='cassandra')
            self._check_session_available(session, expect_auth_err=True, expect_invalid_req=True)
        except Unauthorized as e:
            self.assertEqual(e.message, 'Error from server: code=2100 [Unauthorized] message='
                             '"You have to be logged in and not anonymous to perform this request"')
        except Exception as e:
            assert isinstance(e.errors.values()[0], AuthenticationFailed)

        session = self.get_session(node_idx=1, user='cassandra', password='cassandra')
        try:
            self._check_session_available(session, expect_auth_err=True, expect_invalid_req=True)
            self.fail("Unauthorized expected")
        except Unauthorized as e:
            self.assertEqual(e.message, 'Error from server: code=2100 [Unauthorized] message='
                                        '"You have to be logged in and not anonymous to perform this request"')

        try:
            self.get_session(node_idx=0)
            self.fail("AuthenticationFailed expected")
        except Exception as e:
            assert isinstance(e.errors.values()[0], AuthenticationFailed)

        session = self.get_session(node_idx=1)
        try:
            self._check_session_available(session, expect_auth_err=True, expect_invalid_req=True)
            self.fail("Unauthorized expected")
        except Exception as e:
            self.assertEqual(e.message,
                             'Error from server: code=2100 [Unauthorized] message='
                             '"You have to be logged in and not anonymous to perform this request"')


    def adding_new_node_not_overwrite_global_schema_test(self):
        """
        **Description:** Add new node(RF=1) to cluster with keyspace RF=2
        **Expected Result:** the keyspace RF was not changed
        """
        self.prepare(nodes=2)

        session = self.get_session(user='cassandra', password='cassandra')
        self.assertEquals(1, session.cluster.metadata.keyspaces['system_auth'].replication_strategy.replication_factor)

        session.execute("ALTER KEYSPACE system_auth "
                        "WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2};")

        session = self.get_session(user='cassandra', password='cassandra')
        self.assertEquals(2, session.cluster.metadata.keyspaces['system_auth'].replication_strategy.replication_factor)

        node3 = new_node(self.cluster, bootstrap=False)
        node3.start()

        session = self.get_session(user='cassandra', password='cassandra')
        self.assertEquals(2, session.cluster.metadata.keyspaces['system_auth'].replication_strategy.replication_factor)

        session = self.get_session(node_idx=2, user='cassandra', password='cassandra')
        self.assertEquals(2, session.cluster.metadata.keyspaces['system_auth'].replication_strategy.replication_factor)

        # wait for schema sync and verify rf
        time.sleep(5)
        node1 = self.cluster.nodelist()[0]
        resp = node1.nodetool('describecluster')
        lines = resp[0].split('\n')
        schemas = [lines[i+1] for i, line in enumerate(lines) if line.find('Schema versions:') != -1]
        self.assertEquals(1, len(schemas))
        session = self.get_session(user='cassandra', password='cassandra')
        self.assertEquals(2, session.cluster.metadata.keyspaces['system_auth'].replication_strategy.replication_factor)

    def prepare(self, nodes=1, permissions_validity=0, experimental=False):
        config = {'authenticator': 'org.apache.cassandra.auth.PasswordAuthenticator',
                  'authorizer': 'org.apache.cassandra.auth.CassandraAuthorizer',
                  'permissions_validity_in_ms': permissions_validity,
                  'permissions_update_interval_in_ms' : int(permissions_validity / 2)}
        if experimental:
            config.update({'experimental': True})
        self.cluster.set_configuration_options(values=config)
        self.cluster.populate(nodes).start()

        n = self.wait_for_any_log(self.cluster.nodelist(), 'Created default superuser', 10)
        debug("Default role created by " + n.name)

    def get_session(self, node_idx=0, user=None, password=None):
        node = self.cluster.nodelist()[node_idx]
        conn = self.patient_cql_connection(node, user=user, password=password)
        return conn

    def assertPermissionsListed(self, expected, session, query):
        # from cassandra.query import named_tuple_factory
        # session.row_factory = named_tuple_factory
        rows = session.execute(query)
        perms = [(str(r.username), str(r.resource), str(r.permission)) for r in rows]
        self.assertEqual(sorted(expected), sorted(perms))

    def assertUnauthorized(self, message, session, query):
        with self.assertRaises(Unauthorized) as cm:
            session.execute(query)
        assert re.search(message, cm.exception.message), "Expected '%s', but got '%s'" % (message, cm.exception.message)


def data_resource_creator_permissions(creator, resource):
    permissions = []
    for perm in 'SELECT', 'MODIFY', 'ALTER', 'DROP', 'AUTHORIZE':
        permissions.append((creator, resource, perm))
    if resource.startswith("<keyspace "):
        permissions.append((creator, resource, 'CREATE'))
        keyspace = resource[10:-1]
        # also grant the creator of a ks perms on functions in that ks
        for perm in 'CREATE', 'ALTER', 'DROP', 'AUTHORIZE', 'EXECUTE':
            permissions.append((creator, '<all functions in %s>' % keyspace, perm))
    return permissions


def role_creator_permissions(creator, role):
    permissions = []
    for perm in 'ALTER', 'DROP', 'AUTHORIZE':
        permissions.append((creator, role, perm))
    return permissions


def function_resource_creator_permissions(creator, resource):
    permissions = []
    for perm in 'ALTER', 'DROP', 'AUTHORIZE', 'EXECUTE':
        permissions.append((creator, resource, perm))
    return permissions
