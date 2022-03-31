import math
import time

import pytest
from cassandra import FunctionFailure

from dtest_class import Tester, create_ks
from tools.misc import ImmutableMapping
from tools.assertions import assert_invalid, assert_one
from dtest_setup_overrides import DTestSetupOverrides


@pytest.mark.dtest_full
class TestUserFunctions(Tester):

    @pytest.fixture(scope="function", autouse=True)
    def fixture_dtest_setup_overrides(self, dtest_config):
        dtest_setup_overrides = DTestSetupOverrides()
        if dtest_config.is_scylla:
            dtest_setup_overrides.cluster_options = ImmutableMapping({
                "experimental_features": ["udf"],
                "enable_user_defined_functions": "true",
            })
        elif dtest_config.cassandra_version_from_build >= "3.0":
            dtest_setup_overrides.cluster_options = ImmutableMapping({
                "enable_user_defined_functions": "true",
                "enable_scripted_user_defined_functions": "true",
            })
        else:
            dtest_setup_overrides.cluster_options = ImmutableMapping({
                "enable_user_defined_functions": "true",
            })
        return dtest_setup_overrides

    def prepare(self, create_keyspace=True, nodes=1, rf=1):
        cluster = self.cluster

        cluster.populate(nodes).start()
        node1 = cluster.nodelist()[0]
        time.sleep(0.2)

        session = self.patient_cql_connection(node1)
        if create_keyspace:
            create_ks(session, 'ks', rf)
        return session

    def test_migration(self):
        """ Test migration of user functions """
        cluster = self.cluster

        # Uses 3 nodes just to make sure function mutations are correctly serialized
        cluster.populate(3).start()
        node1 = cluster.nodelist()[0]
        node2 = cluster.nodelist()[1]
        node3 = cluster.nodelist()[2]
        time.sleep(0.2)

        session1 = self.patient_exclusive_cql_connection(node1)
        session2 = self.patient_exclusive_cql_connection(node2)
        session3 = self.patient_exclusive_cql_connection(node3)
        create_ks(session1, 'ks', 1)
        session2.execute("use ks")
        session3.execute("use ks")

        session1.execute("""
            CREATE TABLE udf_kv (
                key    int primary key,
                value  double
            );
        """)
        time.sleep(1)

        session1.execute("INSERT INTO udf_kv (key, value) VALUES (%d, %d)" % (1, 1))
        session1.execute("INSERT INTO udf_kv (key, value) VALUES (%d, %d)" % (2, 2))
        session1.execute("INSERT INTO udf_kv (key, value) VALUES (%d, %d)" % (3, 3))

        session1.execute("""
            create or replace function x_2 ( input double ) called on null input
            returns double language lua as 'return (input ~= nil) and input * 2.0 or nil'
            """)
        session2.execute("""
            create or replace function x_3 ( input double ) called on null input
            returns double language lua as 'return (input ~= nil) and input * 3.0 or nil'
            """)
        session3.execute("""
            create or replace function x_4 ( input double ) called on null input
            returns double language lua as 'return (input ~= nil) and input * 4.0 or nil'
            """)

        time.sleep(1)

        assert_one(session1,
                   "SELECT key, value, x_2(value), x_3(value), x_4(value) FROM udf_kv where key = 1",
                   [1, 1.0, 2.0, 3.0, 4.0])

        assert_one(session2,
                   "SELECT key, value, x_2(value), x_3(value), x_4(value) FROM udf_kv where key = 2",
                   [2, 2.0, 4.0, 6.0, 8.0])

        assert_one(session3,
                   "SELECT key, value, x_2(value), x_3(value), x_4(value) FROM udf_kv where key = 3",
                   [3, 3.0, 6.0, 9.0, 12.0])

        # try giving existing function bad input, should error
        assert_invalid(session1,
                       "SELECT key, value, x_2(key) FROM udf_kv where key = 1",
                       "Type error: key cannot be passed as argument 0 of function ks.x_2 of type double")

        session2.execute("drop function x_2")
        session3.execute("drop function x_3")
        session1.execute("drop function x_4")

        assert_invalid(session1, "SELECT key, value, x_2(value), x_3(value), x_4(value) FROM udf_kv where key = 1")
        assert_invalid(session2, "SELECT key, value, x_2(value), x_3(value), x_4(value) FROM udf_kv where key = 1")
        assert_invalid(session3, "SELECT key, value, x_2(value), x_3(value), x_4(value) FROM udf_kv where key = 1")

    @pytest.mark.single_node
    def test_udf_overload_test(self):
        session = self.prepare()

        session.execute("CREATE TABLE tab (v varchar PRIMARY KEY, i int, t text, a ascii)")
        session.execute("INSERT INTO tab (v, i, t, a) VALUES ('foo', 1, 'foo', 'foo');")

        # create overloaded udfs
        session.execute(
            "CREATE FUNCTION overloaded(v varchar) called on null input RETURNS text LANGUAGE lua AS 'return \"f1\"'")
        session.execute(
            "CREATE OR REPLACE FUNCTION overloaded(i int) called on null input RETURNS text LANGUAGE lua AS 'return \"f2\"'")
        session.execute(
            "CREATE OR REPLACE FUNCTION overloaded(t text) called on null input RETURNS text LANGUAGE lua AS 'return \"f3\"'")
        session.execute(
            "CREATE OR REPLACE FUNCTION overloaded(a ascii) called on null input RETURNS text LANGUAGE lua AS 'return \"f4\"'")

        # ensure that works with correct specificity
        assert_invalid(session, "SELECT v FROM tab WHERE k = overloaded('foo')",
                       "Ambiguous call to function overloaded")
        assert_one(session, "SELECT v, overloaded(v) FROM tab", ["foo", "f3"])  # varchar is the same as text
        assert_one(session, "SELECT i, overloaded(i) FROM tab", [1, "f2"])
        assert_one(session, "SELECT t, overloaded(t) FROM tab", ["foo", "f3"])
        assert_one(session, "SELECT a, overloaded(a) FROM tab", ["foo", "f4"])

        # try non-existent functions
        assert_invalid(session, "DROP FUNCTION overloaded(boolean)")
        assert_invalid(session, "DROP FUNCTION overloaded(bigint)")

        # try dropping overloaded - should fail because ambiguous
        assert_invalid(session, "DROP FUNCTION overloaded")

        # varchar is the same as text here too.
        session.execute("DROP FUNCTION overloaded(varchar)")
        assert_invalid(session, "DROP FUNCTION overloaded(text)", "User function ks.overloaded(text) doesn't exist")

        session.execute("DROP FUNCTION overloaded(ascii)")

        # should now work - unambiguous
        session.execute("DROP FUNCTION overloaded")

    @pytest.mark.xfail(reason="Language 'javascript' is not supported")
    @pytest.mark.single_node
    def test_udf_scripting(self):
        session = self.prepare()
        session.execute("create table nums (key int primary key, val double);")

        for x in range(1, 4):
            session.execute("INSERT INTO nums (key, val) VALUES (%d, %d)" % (x, float(x)))

        session.execute(
            "CREATE FUNCTION x_sin(val double) called on null input returns double language javascript as 'Math.sin(val)'")

        assert_one(session, "SELECT key, val, x_sin(val) FROM nums where key = %d" % 1, [1, 1.0, math.sin(1.0)])
        assert_one(session, "SELECT key, val, x_sin(val) FROM nums where key = %d" % 2, [2, 2.0, math.sin(2.0)])
        assert_one(session, "SELECT key, val, x_sin(val) FROM nums where key = %d" % 3, [3, 3.0, math.sin(3.0)])

        session.execute(
            "create function y_sin(val double) called on null input returns double language javascript as 'Math.sin(val).toString()'")

        assert_invalid(session, "select y_sin(val) from nums where key = 1", expected=FunctionFailure)

        assert_invalid(
            session, "create function compilefail(key int) called on null input returns double language javascript as 'foo bar';")

        session.execute("create function plustwo(key int) called on null input returns double language javascript as 'key+2'")

        assert_one(session, "select plustwo(key) from nums where key = 3", [5])

    @pytest.mark.single_node
    def test_default_aggregate(self):
        session = self.prepare()
        session.execute("create table nums (key int primary key, val double);")

        for x in range(1, 10):
            session.execute("INSERT INTO nums (key, val) VALUES (%d, %d)" % (x, float(x)))

        assert_one(session, "SELECT min(key) FROM nums", [1])
        assert_one(session, "SELECT max(val) FROM nums", [9.0])
        assert_one(session, "SELECT sum(key) FROM nums", [45])
        assert_one(session, "SELECT avg(val) FROM nums", [5.0])
        assert_one(session, "SELECT count(*) FROM nums", [9])

    @pytest.mark.single_node
    def test_aggregate_udf(self):
        session = self.prepare()
        session.execute("create table nums (key int primary key, val int);")

        for x in range(1, 4):
            session.execute("INSERT INTO nums (key, val) VALUES (%d, %d)" % (x, x))
        session.execute(
            "create function plus(key int, val int) called on null input returns int language lua as 'return key + val'")
        session.execute(
            "create function stri(key int) called on null input returns text language lua as 'return tostring(key)'")
        session.execute("create aggregate suma (int) sfunc plus stype int finalfunc stri initcond 10")

        assert_one(session, "select suma(val) from nums", ["16"])

        session.execute(
            "create function test(a int, b double) called on null input returns int language lua as 'return a + b'")
        session.execute("create aggregate aggy(double) sfunc test stype int")

        assert_invalid(session, "create aggregate aggtwo(int) sfunc aggy stype int")

        assert_invalid(session, "create aggregate aggthree(int) sfunc test stype int finalfunc aggtwo")

    @pytest.mark.single_node
    def test_udf_with_udt(self):
        session = self.prepare()

        session.execute("create type test (a text, b int);")

        # assert_invalid(session, "create table tab (key int primary key, udt test);")

        session.execute("create table tab (key int primary key, udt frozen<test>);")

        session.execute("insert into tab (key, udt) values (1, {a: 'un', b:1});")
        session.execute("insert into tab (key, udt) values (2, {a: 'deux', b:2});")
        session.execute("insert into tab (key, udt) values (3, {a: 'trois', b:3});")

        session.execute(
            "create function funk(udt test) called on null input returns int language lua as 'return udt.b';")

        assert_one(session, "select sum(funk(udt)) from tab", [6])

        assert_invalid(session, "drop type test;")

    @pytest.mark.single_node
    def test_udf_with_udt_keyspace_isolation(self):
        """
        Ensure functions dont allow a UDT from another keyspace
        @jira_ticket CASSANDRA-9409
        @since 2.2
        """
        session = self.prepare()

        session.execute("create type udt (a text, b int);")
        create_ks(session, 'user_ks', 1)

        # ensure we cannot use a udt from another keyspace as function argument
        assert_invalid(
            session,
            "CREATE FUNCTION overloaded(v ks.udt) called on null input RETURNS text LANGUAGE java AS 'return \"f1\";'",
            "Statement on keyspace user_ks cannot refer to a user type in keyspace ks"
        )

        # ensure we cannot use a udt from another keyspace as return value
        assert_invalid(
            session,
            ("CREATE FUNCTION test(v text) called on null input RETURNS ks.udt "
             "LANGUAGE java AS 'return null;';"),
            "Statement on keyspace user_ks cannot refer to a user type in keyspace ks"
        )

    @pytest.mark.single_node
    def test_aggregate_with_udt_keyspace_isolation(self):
        """
        Ensure aggregates dont allow a UDT from another keyspace
        @jira_ticket CASSANDRA-9409
        """
        session = self.prepare()

        session.execute("create type udt (a int);")
        create_ks(session, 'user_ks', 1)
        assert_invalid(
            session,
            "create aggregate suma (ks.udt) sfunc plus stype int finalfunc stri initcond 10",
            "Statement on keyspace user_ks cannot refer to a user type in keyspace ks"
        )
