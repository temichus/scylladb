# coding: utf-8
import string

import struct
import time
from datetime import datetime, timedelta
from unittest import skip
from random import randint

from cassandra import ConsistencyLevel, InvalidRequest
from cassandra.policies import FallthroughRetryPolicy
from cassandra.protocol import ProtocolException
from cassandra.query import SimpleStatement

from assertions import assert_invalid, assert_one, assert_unavailable, assert_all
from dtest import Tester

from thrift_bindings.thrift010.ttypes import \
    ConsistencyLevel as ThriftConsistencyLevel
from thrift_bindings.thrift010.ttypes import (CfDef, Column, ColumnOrSuperColumn,
                                              Mutation)
from thrift.Thrift import TApplicationException
from thrift_tests import get_thrift_client
from tools import debug, require, rows_to_list, since, new_node
from scylla_tools import get_entity_id, get_truncated_time_from_system_local, get_truncated_time_from_system_truncated
from nose.plugins.attrib import attr


@attr('dtest-full')
class CQLTester(Tester):

    def prepare(self, create_keyspace=True, use_cache=False, nodes=1, rf=1, protocol_version=None, user=None, password=None, configuration_options=None, **kwargs):
        cluster = self.cluster

        if (use_cache):
            cluster.set_configuration_options(values={'row_cache_size_in_mb': 100})

        start_rpc = kwargs.pop('start_rpc', False)
        if start_rpc:
            cluster.set_configuration_options(values={'start_rpc': True})

        if user:
            config = {'authenticator': 'org.apache.cassandra.auth.PasswordAuthenticator',
                      'authorizer': 'org.apache.cassandra.auth.CassandraAuthorizer',
                      'permissions_validity_in_ms': 0}
            cluster.set_configuration_options(values=config)

        if configuration_options:
            cluster.set_configuration_options(values=configuration_options)

        if not cluster.nodelist():
            cluster.populate(nodes).start(wait_for_binary_proto=True)
        node1 = cluster.nodelist()[0]

        session = self.patient_cql_connection(node1, protocol_version=protocol_version, user=user, password=password)
        if create_keyspace:
            if self._preserve_cluster:
                session.execute("DROP KEYSPACE IF EXISTS ks")
            self.create_ks(session, 'ks', rf)
        return session


@attr('dtest-full', 'single_node')
class StorageProxyCQLTester(CQLTester):
    """
    Each CQL statement is exercised at least once in order to
    ensure we execute the code path in StorageProxy.
    Note that in depth CQL validation is done in Java unit tests,
    see CASSANDRA-9160.
    """

    def keyspace_test(self):
        """
        CREATE KEYSPACE, USE KEYSPACE, ALTER KEYSPACE, DROP KEYSPACE statements
        """
        session = self.prepare(create_keyspace=False)

        session.execute(
            "CREATE KEYSPACE ks WITH replication = { 'class':'SimpleStrategy', 'replication_factor':1} AND DURABLE_WRITES = true")

        session.execute("USE ks")

        session.execute(
            "ALTER KEYSPACE ks WITH replication = { 'class' : 'NetworkTopologyStrategy', 'dc1' : 1 } AND DURABLE_WRITES = false")

        session.execute("DROP KEYSPACE ks")
        assert_invalid(session, "USE ks", expected=InvalidRequest)

    def table_test(self):
        """
        CREATE TABLE, ALTER TABLE, TRUNCATE TABLE, DROP TABLE statements
        """
        session = self.prepare()

        session.execute("CREATE TABLE test1 (k int PRIMARY KEY, v1 int)")
        session.execute("CREATE TABLE test2 (k int, c1 int, v1 int, PRIMARY KEY (k, c1)) WITH COMPACT STORAGE")

        session.execute("ALTER TABLE test1 ADD v2 int")

        for i in range(0, 10):
            session.execute("INSERT INTO test1 (k, v1, v2) VALUES (%d, %d, %d)" % (i, i, i))
            session.execute("INSERT INTO test2 (k, c1, v1) VALUES (%d, %d, %d)" % (i, i, i))

        res = sorted(session.execute("SELECT * FROM test1"))
        assert rows_to_list(res) == [[i, i, i] for i in range(0, 10)], res

        res = sorted(session.execute("SELECT * FROM test2"))
        assert rows_to_list(res) == [[i, i, i] for i in range(0, 10)], res

        session.execute("TRUNCATE test1")
        session.execute("TRUNCATE test2")

        res = session.execute("SELECT * FROM test1")
        assert rows_to_list(res) == [], res

        res = session.execute("SELECT * FROM test2")
        assert rows_to_list(res) == [], res

        session.execute("DROP TABLE test1")
        session.execute("DROP TABLE test2")

        assert_invalid(session, "SELECT * FROM test1", expected=InvalidRequest)
        assert_invalid(session, "SELECT * FROM test2", expected=InvalidRequest)

    @skip('Scylla does not support CREATE INDEX')
    def index_test(self):
        """
        CREATE INDEX, DROP INDEX statements
        """
        session = self.prepare()

        session.execute("CREATE TABLE test3 (k int PRIMARY KEY, v1 int, v2 int)")
        session.execute("CREATE INDEX testidx ON test3 (v1)")

        for i in range(0, 10):
            session.execute("INSERT INTO test3 (k, v1, v2) VALUES (%d, %d, %d)" % (i, i, i))

        res = session.execute("SELECT * FROM test3 WHERE v1 = 0")
        assert rows_to_list(res) == [[0, 0, 0]], res

        session.execute("DROP INDEX testidx")

        assert_invalid(session, "SELECT * FROM test3 where v1 = 0", expected=InvalidRequest)

    def type_test(self):
        """
        CREATE TYPE, ALTER TYPE, DROP TYPE statements
        """
        session = self.prepare()

        session.execute("CREATE TYPE address_t (street text, city text, zip_code int)")
        session.execute("CREATE TABLE test4 (id int PRIMARY KEY, address frozen<address_t>)")

        session.execute("ALTER TYPE address_t ADD phones set<text>")
        session.execute("CREATE TABLE test5 (id int PRIMARY KEY, address frozen<address_t>)")

        session.execute("DROP TABLE test4")
        session.execute("DROP TABLE test5")
        session.execute("DROP TYPE address_t")
        assert_invalid(session, "CREATE TABLE test6 (id int PRIMARY KEY, address frozen<address_t>)",
                       expected=InvalidRequest)

    def user_test(self):
        """
        CREATE USER, ALTER USER, DROP USER statements
        """
        session = self.prepare(user='cassandra', password='cassandra')

        session.execute("CREATE USER user1 WITH PASSWORD 'secret'")

        session.execute("ALTER USER user1 WITH PASSWORD 'secret^2'")

        session.execute("DROP USER user1")

    @attr('next-gating')
    @attr('dtest-debug')
    def statements_test(self):
        """
        INSERT, UPDATE, SELECT, SELECT COUNT, DELETE statements
        """
        session = self.prepare()

        session.execute("CREATE TABLE test7 (kind text, time int, v1 int, v2 int, PRIMARY KEY(kind, time) )")

        for i in range(0, 10):
            session.execute("INSERT INTO test7 (kind, time, v1, v2) VALUES ('ev1', %d, %d, %d)" % (i, i, i))
            session.execute("INSERT INTO test7 (kind, time, v1, v2) VALUES ('ev2', %d, %d, %d)" % (i, i, i))

        res = session.execute("SELECT COUNT(*) FROM test7 WHERE kind = 'ev1'")
        assert rows_to_list(res) == [[10]], res

        res = session.execute("SELECT COUNT(*) FROM test7 WHERE kind IN ('ev1', 'ev2')")
        assert rows_to_list(res) == [[20]], res

        res = session.execute("SELECT COUNT(*) FROM test7 WHERE kind IN ('ev1', 'ev2') AND time=0")
        assert rows_to_list(res) == [[2]], res

        res = session.execute("SELECT * FROM test7 WHERE kind = 'ev1'")
        assert rows_to_list(res) == [['ev1', i, i, i] for i in range(0, 10)], res

        res = session.execute("SELECT * FROM test7 WHERE kind = 'ev2'")
        assert rows_to_list(res) == [['ev2', i, i, i] for i in range(0, 10)], res

        for i in range(0, 10):
            session.execute("UPDATE test7 SET v1 = 0, v2 = 0 where kind = 'ev1' AND time=%d" % (i,))

        res = session.execute("SELECT * FROM test7 WHERE kind = 'ev1'")
        assert rows_to_list(res) == [['ev1', i, 0, 0] for i in range(0, 10)], res

        res = session.execute("DELETE FROM test7 WHERE kind = 'ev1'")
        res = session.execute("SELECT * FROM test7 WHERE kind = 'ev1'")
        assert rows_to_list(res) == [], res

        res = session.execute("SELECT COUNT(*) FROM test7 WHERE kind = 'ev1'")
        assert rows_to_list(res) == [[0]], res

    def batch_test(self):
        """
        BATCH statement
        """
        session = self.prepare()

        session.execute("""
            CREATE TABLE test8 (
                userid text PRIMARY KEY,
                name text,
                password text
            )
        """)

        query = SimpleStatement("""
            BEGIN BATCH
                INSERT INTO test8 (userid, password, name) VALUES ('user2', 'ch@ngem3b', 'second user');
                UPDATE test8 SET password = 'ps22dhds' WHERE userid = 'user3';
                INSERT INTO test8 (userid, password) VALUES ('user4', 'ch@ngem3c');
                DELETE name FROM test8 WHERE userid = 'user1';
            APPLY BATCH;
        """, consistency_level=ConsistencyLevel.QUORUM)
        session.execute(query)


@attr('dtest-full')
class MiscellaneousCQLTester(CQLTester):
    """
    CQL tests that cannot be performed as Java unit tests, see CASSANDRA-9160. Please consider
    writing java unit tests for CQL validation, add a new test here only if there is a reason for it,
    e.g. something related to the client protocol or thrift, or examing the log files, or multiple nodes
    required.
    """

    def large_collection_errors(self):
        """
        For large collections, make sure that we are printing warnings.
        """

        # We only warn with protocol 2
        session = self.prepare(protocol_version=2)

        cluster = self.cluster
        node1 = cluster.nodelist()[0]
        self.ignore_log_patterns = ["Detected collection for table"]

        session.execute("""
            CREATE TABLE maps (
                userid text PRIMARY KEY,
                properties map<int, text>
            );
        """)

        # Insert more than the max, which is 65535
        for i in range(70000):
            session.execute("UPDATE maps SET properties[%i] = 'x' WHERE userid = 'user'" % i)

        # Query for the data and throw exception
        session.execute("SELECT properties FROM maps WHERE userid = 'user'")
        node1.watch_log_for("Detected collection for table ks.maps with 70000 "
                            "elements, more than the 65535 limit. Only the "
                            "first 65535 elements will be returned to the "
                            "client. Please see http://cassandra.apache.org/doc/cql3/CQL.html#collections for more details.")

    @skip('scylla does not support manipulation of regular tables (See scylladb/scylla#7568)')
    def cql3_insert_thrift_test(self):
        """ Check that we can insert from thrift into a CQL3 table (#4377) """
        session = self.prepare(start_rpc=True)

        session.execute("""
            CREATE TABLE test (
                k int,
                c int,
                v int,
                PRIMARY KEY (k, c)
            )
        """)

        node = self.cluster.nodelist()[0]
        host, port = node.network_interfaces['thrift']
        client = get_thrift_client(host, port)
        client.transport.open()
        client.set_keyspace('ks')
        key = struct.pack('>i', 2)
        column_name_component = struct.pack('>i', 4)
        # component length + component + EOC
        column_name = b'\x00\x04' + column_name_component + b'\x00'
        value = struct.pack('>i', 8)
        client.batch_mutate(
            {key: {'test': [Mutation(ColumnOrSuperColumn(
                column=Column(name=column_name, value=value, timestamp=100)))]}},
            ThriftConsistencyLevel.ONE)
        node.flush()

        res = session.execute("SELECT * FROM test")
        assert rows_to_list(res) == [[2, 4, 8]], res

    @attr('single_node')
    def cql3_insert_thrift_test_expect_error(self):
        """
        Originally, the test checked that we can insert from thrift into a CQL3 table (#4377).
        However, Scylla does not support manipulation of regular tables (See scylladb/scylla#7568)
        so expect thrift insert to fail.
        """
        session = self.prepare(start_rpc=True)

        session.execute("""
            CREATE TABLE test (
                k int,
                c int,
                v int,
                PRIMARY KEY (k, c)
            )
        """)

        node = self.cluster.nodelist()[0]
        host, port = node.network_interfaces['thrift']
        client = get_thrift_client(host, port)
        client.transport.open()
        client.set_keyspace('ks')
        key = struct.pack('>i', 2)
        column_name_component = struct.pack('>i', 4)
        # component length + component + EOC + component length + component + EOC
        column_name = b'\x00\x04' + column_name_component + b'\x00' + b'\x00\x01' + 'v'.encode() + b'\x00'
        value = struct.pack('>i', 8)
        rejected = False
        try:
            client.batch_mutate(
                {key: {'test': [Mutation(ColumnOrSuperColumn(
                    column=Column(name=column_name, value=value, timestamp=100)))]}},
                ThriftConsistencyLevel.ONE)
        except TApplicationException:
            rejected = True
            pass
        assert rejected, "mutation expected to be rejected due to bad clustering key"

    @attr('single_node')
    def rename_test(self):
        session = self.prepare(start_rpc=True)

        node = self.cluster.nodelist()[0]
        host, port = node.network_interfaces['thrift']
        client = get_thrift_client(host, port)
        client.transport.open()

        cfdef = CfDef()
        cfdef.keyspace = 'ks'
        cfdef.name = 'test'
        cfdef.column_type = 'Standard'
        cfdef.comparator_type = 'CompositeType(Int32Type, Int32Type, Int32Type)'
        cfdef.key_validation_class = 'UTF8Type'
        cfdef.default_validation_class = 'UTF8Type'

        client.set_keyspace('ks')
        client.system_add_column_family(cfdef)

        session.execute("INSERT INTO ks.test (key, column1, column2, column3, value) VALUES ('foo', 4, 3, 2, 'bar')")

        time.sleep(1)

        session.execute("ALTER TABLE test RENAME column1 TO foo1 AND column2 TO foo2 AND column3 TO foo3")
        assert_one(session, "SELECT foo1, foo2, foo3 FROM test", [4, 3, 2])

    @attr('single_node')
    def prepared_statement_invalidation_test(self):
        """
        @jira_ticket CASSANDRA-7910
        """
        session = self.prepare()

        session.execute("CREATE TABLE test (k int PRIMARY KEY, a int, b int, c int)")
        session.execute("INSERT INTO test (k, a, b, c) VALUES (0, 0, 0, 0)")

        wildcard_prepared = session.prepare("SELECT * FROM test")
        explicit_prepared = session.prepare("SELECT k, a, b, c FROM test")
        result = list(session.execute(wildcard_prepared.bind(None)))
        self.assertEqual(result, [(0, 0, 0, 0)])

        session.execute("ALTER TABLE test DROP c")
        result = list(session.execute(wildcard_prepared.bind(None)))
        # wildcard select can be automatically re-prepared by the driver
        self.assertEqual(result, [(0, 0, 0)])
        # but re-preparing the statement with explicit columns should fail
        # (see PYTHON-207 for why we expect InvalidRequestException instead of the normal exc)
        assert_invalid(session, explicit_prepared.bind(None), expected=InvalidRequest)

        session.execute("ALTER TABLE test ADD d int")
        result = list(session.execute(wildcard_prepared.bind(None)))
        self.assertEqual(result, [(0, 0, 0, None)])

        explicit_prepared = session.prepare("SELECT k, a, b, d FROM test")

        # when the type is altered, both statements will need to be re-prepared
        # by the driver, but the re-preparation should succeed
        session.execute("ALTER TABLE test ALTER d TYPE blob")
        result = list(session.execute(wildcard_prepared.bind(None)))
        self.assertEqual(result, [(0, 0, 0, None)])

        result = list(session.execute(explicit_prepared.bind(None)))
        self.assertEqual(result, [(0, 0, 0, None)])

    def reverse_query_test(self):
        """
         Issue: https://github.com/scylladb/scylla/issues/6171
         Commit: https://github.com/scylladb/scylla/commit/791acc7f3858e5541ee216034f4c7111818510c5
         Create table with 2 clustering keys
         Read with filter using "in" restriction on clustering keys and ordered by clustering keys DESC with and without
         BYPASS CACHE
        """
        session = self.prepare(nodes=4, rf=3)

        session.execute("CREATE TABLE cf (pk int, ck int, ck1 int, v text, PRIMARY KEY (pk, ck, ck1))")

        test_value = string.ascii_lowercase * 40
        for i in range(10000):
            session.execute(f"INSERT INTO cf(pk, ck, ck1, v) VALUES (0, {i}, {i}, '{test_value}')")

        for node in self.cluster.nodelist():
            node.flush()

        assert_one(session, "select count(*) from cf", [10000], cl=ConsistencyLevel.QUORUM)

        in_list = [i for i in range(0, 10000, 1000)]
        in_str = ', '.join(str(i) for i in in_list)
        expected_results = [[test_value] for _ in reversed(in_list)]

        read_stmt = f"SELECT v FROM cf WHERE pk = 0 and ck in ({in_str}) and ck1 in ({in_str}) ORDER BY ck DESC, ck1 DESC"

        with self.subTest('Read without BYPASS CACHE'):
            debug(f'Read without BYPASS CACHE with query: {read_stmt}')
            assert_all(session, read_stmt, expected_results, cl=ConsistencyLevel.QUORUM)

        with self.subTest('Read with BYPASS CACHE'):
            debug(f'Read with BYPASS CACHE with query: {read_stmt} BYPASS CACHE')
            assert_all(session, f"{read_stmt} BYPASS CACHE", expected_results, cl=ConsistencyLevel.QUORUM)

    def normal_query_test(self):
        """
         Issue: https://github.com/scylladb/scylla/issues/6171
         Commit: https://github.com/scylladb/scylla/commit/791acc7f3858e5541ee216034f4c7111818510c5
         Create table with 2 clustering keys
         Read with filter using "in" restriction on clustering keys and ordered by clustering keys ASC with and without
         BYPASS CACHE
        """
        session = self.prepare(nodes=4, rf=3)

        session.execute("CREATE TABLE cf (pk int, ck int, ck1 int, v text, PRIMARY KEY (pk, ck, ck1))")

        test_value = string.ascii_lowercase * 40
        for i in range(10000):
            session.execute(f"INSERT INTO cf(pk, ck, ck1, v) VALUES (0, {i}, {i}, '{test_value}')")

        for node in self.cluster.nodelist():
            node.flush()

        assert_one(session, "select count(*) from cf", [10000], cl=ConsistencyLevel.QUORUM)

        in_list = [i for i in range(0, 10000, 1000)]
        in_str = ', '.join(str(i) for i in in_list)
        expected_results = [[test_value] for _ in in_list]

        read_stmt = f"SELECT v FROM cf WHERE pk = 0 and ck in ({in_str}) and ck1 in ({in_str})"

        with self.subTest('Read without BYPASS CACHE'):
            debug(f'Read without BYPASS CACHE with query: {read_stmt}')
            assert_all(session, read_stmt, expected_results, cl=ConsistencyLevel.QUORUM)

        with self.subTest('Read with BYPASS CACHE'):
            debug(f'Read with BYPASS CACHE with query: {read_stmt} BYPASS CACHE')
            assert_all(session, f"{read_stmt} BYPASS CACHE", expected_results, cl=ConsistencyLevel.QUORUM)

    def reverse_query_ck_collect_test(self):
        """
         Issue: https://github.com/scylladb/scylla/issues/6171
         Commit: https://github.com/scylladb/scylla/commit/791acc7f3858e5541ee216034f4c7111818510c5
         Create table where clustering key is frozen collection
         Read with filter using "in" restriction on clustering key and ordered by clustering key DESC with and without
         BYPASS CACHE
        """
        session = self.prepare(nodes=4, rf=3)

        session.execute("CREATE TABLE cf (pk int, ck frozen<list<text>>, v text, PRIMARY KEY (pk, ck))")

        all_ascii = list(string.ascii_lowercase)
        text_value = string.ascii_lowercase * 40
        for i in all_ascii:
            session.execute(f"INSERT INTO cf(pk, ck, v) VALUES (0, ['{i}'], '{text_value}')")

        for node in self.cluster.nodelist():
            node.flush()

        assert_one(session, "select count(*) from cf", [len(all_ascii)], cl=ConsistencyLevel.QUORUM)

        in_list = [all_ascii[i] for i in range(0, 26, 10)]
        in_str = ', '.join(f"['{i}']" for i in in_list)
        expected_results = [[f'{text_value}'] for _ in reversed(in_list)]

        read_stmt = f"SELECT v FROM cf WHERE pk = 0 and ck in ({in_str}) ORDER BY ck DESC"

        with self.subTest('Read without BYPASS CACHE'):
            debug(f'Read without BYPASS CACHE with query: {read_stmt}')
            assert_all(session, read_stmt, expected_results, cl=ConsistencyLevel.QUORUM)

        with self.subTest('Read with BYPASS CACHE'):
            debug(f'Read with BYPASS CACHE with query: {read_stmt} BYPASS CACHE')
            assert_all(session, f"{read_stmt} BYPASS CACHE", expected_results, cl=ConsistencyLevel.QUORUM)

    def reverse_query_table_desc_test(self):
        """
         Issue: https://github.com/scylladb/scylla/issues/6171
         Commit: https://github.com/scylladb/scylla/commit/791acc7f3858e5541ee216034f4c7111818510c5
         Create table with 2 clustering keys and ordered by both clustering keys DESC
         Read with filter using "in" restriction on clustering keys and ordered by clustering keys DESC with and without
         BYPASS CACHE
        """
        session = self.prepare(nodes=4, rf=3)

        session.execute("CREATE TABLE cf (pk int, ck int, ck1 int, v text, PRIMARY KEY (pk, ck, ck1)) "
                        "WITH CLUSTERING ORDER BY (ck DESC, ck1 DESC)")

        text_value = string.ascii_lowercase * 40
        for i in range(10000):
            session.execute(f"INSERT INTO cf(pk, ck, ck1, v) VALUES (0, {i}, {i}, '{text_value}')")

        for node in self.cluster.nodelist():
            node.flush()

        assert_one(session, "select count(*) from cf", [10000], cl=ConsistencyLevel.QUORUM)

        in_list = [i for i in range(10000, 0, 1000)]
        in_str = ', '.join(str(i) for i in in_list)
        expected_results = [[text_value] for _ in reversed(in_list)]

        read_stmt = f"SELECT v FROM cf WHERE pk = 0 and ck in ({in_str}) and ck1 in ({in_str}) ORDER BY ck DESC, ck1 DESC"

        with self.subTest('Read without BYPASS CACHE'):
            debug(f'Read without BYPASS CACHE with query: {read_stmt}')
            assert_all(session, read_stmt, expected_results, cl=ConsistencyLevel.QUORUM)

        with self.subTest('Read with BYPASS CACHE'):
            debug(f'Read with BYPASS CACHE with query: {read_stmt} BYPASS CACHE')
            assert_all(session, f"{read_stmt} BYPASS CACHE", expected_results, cl=ConsistencyLevel.QUORUM)

    def range_slice_test(self):
        """ Test a regression from #1337 """

        cluster = self.cluster

        cluster.populate(2).start()
        node1 = cluster.nodelist()[0]
        time.sleep(0.2)

        session = self.patient_cql_connection(node1)
        self.create_ks(session, 'ks', 1)

        session.execute("""
            CREATE TABLE test (
                k text PRIMARY KEY,
                v int
            );
        """)
        time.sleep(1)

        session.execute("INSERT INTO test (k, v) VALUES ('foo', 0)")
        session.execute("INSERT INTO test (k, v) VALUES ('bar', 1)")

        res = list(session.execute("SELECT * FROM test"))
        assert len(res) == 2, res

    def _test_query_failed_when_node_is_down(self, stop_gently: bool):
        """
        Test that a select query with consistency_level=QUOROM
        returns an Unavailable error properly when one out of two nodes is DOWN.
        """
        cluster = self.cluster
        # reduce range_request_timeout to make the first try
        # timeout faster when node2 is killed.
        session = self.prepare(nodes=2, rf=2,
                               configuration_options={'range_request_timeout_in_ms': '5000'})
        node1, node2 = self.cluster.nodelist()

        ks = 'ks'
        cf = 'cf'
        session.execute(
            f"CREATE TABLE {cf} (pk int, ck int, v text, PRIMARY KEY (pk, ck))")

        debug("Inserting data...")
        for pk in range(10):
            for ck in range(100):
                q = SimpleStatement(f"INSERT INTO {ks}.{cf} (pk, ck, v) VALUES ({pk}, {ck}, 'foo')",
                                    consistency_level=ConsistencyLevel.ALL)
                session.execute(q)
        cluster.flush()

        debug(f"Stopping node (gently={stop_gently})")
        node2.stop(gently=stop_gently)

        with self.patient_cql_cluster_session(node1, ks, exclusive=True) as session:
            debug("Selecting with CL=ONE")
            assert_one(session,
                       f"SELECT count(*) from {ks}.{cf} BYPASS CACHE",
                       [1000], cl=ConsistencyLevel.ONE)

            debug("Selecting with CL=QUORUM (expected to fail)")
            q = SimpleStatement(f"SELECT count(*) from {ks}.{cf} BYPASS CACHE",
                                consistency_level=ConsistencyLevel.QUORUM)
            assert_unavailable(lambda t: session.execute(q, timeout=t),
                               self.cql_timeout(60))

    def test_query_failed_when_node_is_stopped(self):
        self._test_query_failed_when_node_is_down(stop_gently=True)

    def test_query_failed_when_node_is_killed(self):
        self._test_query_failed_when_node_is_down(stop_gently=False)


@attr('dtest-full')
class TruncateTester(CQLTester):

    @staticmethod
    def create_schema(session, rf=1):
        session.execute("CREATE KEYSPACE ks WITH replication = { 'class':'SimpleStrategy', 'replication_factor':%d} "
                        "AND DURABLE_WRITES = true" % rf)
        session.execute("CREATE TABLE ks.test1 (k int PRIMARY KEY, v1 int)")

    @staticmethod
    def insert_data(conn, data=None):
        if not data:
            data = list([i, i] for i in range(0, 30))

        for (x, y) in data:
            conn.execute("INSERT INTO ks.test1 (k, v1) VALUES (%d, %d)" % (x, y))
        return data

    def validate_truncated_entries_for_table(self, keyspace_name, table_name, prev_truncated_time=None):
        truncated_time_per_node = []
        for node in self.cluster.nodelist():
            if node.status == 'DOWN':
                continue
            session = self.patient_exclusive_cql_connection(node=node)
            id = get_entity_id(session=session, table_or_view='table', keyspace_name=keyspace_name,
                               entity_name=table_name)

            # validate truncation entries in the system.truncated table - expected entry
            truncated_time = get_truncated_time_from_system_truncated(session=session, table_id=id)
            self.assertTrue(truncated_time, msg='Expected truncated entry in the system.truncated table, '
                                                'but it\'s not found')
            truncated_time_per_node.append({node.name: truncated_time})

            # validate truncation entries in the system.local table - not expected entry
            truncated_time = get_truncated_time_from_system_local(session=session)
            self.assertTrue(truncated_time == [[None]],
                            msg='Not expected truncated entry in the system.local table, '
                                'but it\'s found')

        if prev_truncated_time:
            self.assertTrue(prev_truncated_time == truncated_time_per_node)

        return truncated_time_per_node

    def truncate_before_restart_test(self):
        """
        Truncate table and then restart the node. Validate that truncated en
        """
        session = self.prepare(nodes=3, create_keyspace=False)

        self.create_schema(session=session, rf=3)

        data = self.insert_data(conn=session)

        select_query = "SELECT * FROM ks.test1"
        assert_all(session=session, query=select_query, expected=data, cl=ConsistencyLevel.QUORUM, ignore_order=True)

        session.execute("TRUNCATE ks.test1")
        assert_all(session=session, query=select_query, expected=[], cl=ConsistencyLevel.ALL)

        truncated_time_per_node = self.validate_truncated_entries_for_table(keyspace_name='ks', table_name='test1')

        node2 = self.cluster.nodelist()[1]
        node2.stop(wait_other_notice=True)
        node2.start(wait_for_binary_proto=True, wait_other_notice=True)

        session = self.patient_exclusive_cql_connection(node2)
        assert_all(session=session, query=select_query, expected=[], cl=ConsistencyLevel.ALL)

        self.validate_truncated_entries_for_table(keyspace_name='ks', table_name='test1',
                                                  prev_truncated_time=truncated_time_per_node)

    def truncate_twice_test(self):
        """
        Truncate table and then restart the node. Validate that truncated en
        """
        session = self.prepare(nodes=3, create_keyspace=False)

        self.create_schema(session=session, rf=3)

        data = self.insert_data(conn=session)

        select_query = "SELECT * FROM ks.test1"
        assert_all(session=session, query=select_query, expected=data, cl=ConsistencyLevel.QUORUM, ignore_order=True)

        debug('Truncate first time')
        session.execute("TRUNCATE ks.test1")
        assert_all(session=session, query=select_query, expected=[], cl=ConsistencyLevel.ALL)

        truncated_time_per_node = self.validate_truncated_entries_for_table(keyspace_name='ks', table_name='test1')

        time.sleep(60)
        debug('Truncate second time')
        session.execute("TRUNCATE ks.test1")
        assert_all(session=session, query=select_query, expected=[], cl=ConsistencyLevel.ALL)

        sec_truncated_time_per_node = self.validate_truncated_entries_for_table(keyspace_name='ks', table_name='test1')

        self.assertLessEqual(len(truncated_time_per_node), len(sec_truncated_time_per_node))

    @attr('next-gating', 'dtest-debug', 'single_node')
    def truncate_after_restart_test(self):
        session = self.prepare(nodes=1, create_keyspace=False)

        self.create_schema(session=session, rf=1)

        node2 = new_node(self.cluster, bootstrap=True)
        node2.start(wait_for_binary_proto=True)

        data = self.insert_data(conn=session)

        select_query = "SELECT * FROM ks.test1"
        assert_all(session=session, query=select_query, expected=data, cl=ConsistencyLevel.QUORUM, ignore_order=True)

        node2.stop(wait_other_notice=True)
        node2.start(wait_for_binary_proto=True)

        # Many connections to exercise many shards
        conns = [self.patient_exclusive_cql_connection(node2) for i in range(0, 3)]
        for conn in conns:
            self.insert_data(conn=conn, data=data)
            conn.execute("TRUNCATE ks.test1")
            assert_all(session=conn, query=select_query, expected=[], cl=ConsistencyLevel.ALL)

    def cql_query_filtering_without_indexes_test(self):
        """
        https://github.com/scylladb/scylla/issues/2025
        Testing cql query filtering without the use of indexes
        use cases:
        # general use-case
        # CQL statement with relational operations( =, !=, >, < ).
        # CQL statement with IN
        # CQL statement with Limit
        """
        loop_size = 500
        cluster = self.cluster
        cluster.populate(2).start(wait_other_notice=True, wait_for_binary_proto=True)
        node1 = cluster.nodelist()[0]

        session = self.patient_exclusive_cql_connection(node1)
        self.create_ks(session, 'ks', 1)
        session.execute("""
                    CREATE TABLE t1 (
                        p int,
                        c int,
                        v int,
                        PRIMARY KEY (p, c)
                    );
                """)

        for i in range(loop_size):
            session.execute("INSERT INTO t1 (p, c, v) VALUES ({},{},{}) ".format(i, i, i+1))

        rand_num = randint(0, loop_size-1)
        q1_ls = ["select * from ks.t1 where c = {} and v = {} allow filtering;".format(rand_num, rand_num + 1),
                 "select * from ks.t1 where p = {} and v = {} allow filtering;".format(rand_num, rand_num + 1),
                 "select * from ks.t1 where p = {0} and c = {0} and v = {1} allow filtering;"
                 .format(rand_num, rand_num + 1)]

        for query in q1_ls:
            result = rows_to_list(session.execute(query))
            debug(f"Query: {query} Result: {result}")
            assert result == [[rand_num, rand_num, rand_num+1]], f"Query {query}: failed on assertion," \
                f" Result: {result}"

        session.execute("""
                    CREATE TABLE t2 (
                        item_id int,
                        item_name text,
                        insert_time time,
                        PRIMARY KEY (item_id,item_name)
                    );
                """)

        count_above_selected_time = 0
        selected_time_str = "{}:{}:{}".format(randint(0, 23), randint(0, 59), randint(0, 59))
        selected_time = time.strptime(selected_time_str, "%H:%M:%S")

        selected_items_q3 = None
        for i in range(loop_size):
            rand_time_str = "{}:{}:{}".format(randint(0, 23), randint(0, 59), randint(0, 59))
            rand_time = time.strptime(rand_time_str, "%H:%M:%S")
            count_above_selected_time += 1 if rand_time > selected_time else 0
            item_name = "name_" + str(i)
            if randint(1, 10) == 1:
                selected_items_q3 = "'{}'".format(item_name) if selected_items_q3 is None else selected_items_q3 + ", "\
                                    + "'{}'".format(item_name)
            session.execute("INSERT INTO t2 (item_id, item_name, insert_time) VALUES ({},'{}','{}')"
                            .format(i, item_name, rand_time_str))

        # CQL statement with relational operations( =, !=, >, < ).
        q2 = "Select item_id from t2 where insert_time > '{}' allow filtering;"\
            .format(selected_time_str)
        q2_result = rows_to_list(session.execute(q2))
        debug(f"Query: {q2}, Len_Result: {len(q2_result)}, Result: {q2_result}")
        assert len(q2_result) == count_above_selected_time, f"The returned list count doesnt match the calculated count"

        # CQL statement with IN
        q3 = "Select * from t2 where item_name IN ({})  allow filtering;".format(selected_items_q3)
        q3_result = rows_to_list(session.execute(q3))
        debug(f"Query: {q3}, Len_Result: {len(q3_result)}, Result: {q3_result}")
        assert len(q3_result) == len(selected_items_q3.split(",")), f"The returned list count does not match " \
            f"the calculated count"

        # CQL statement with Limit
        rand_limit = randint(1, count_above_selected_time)
        q4 = "Select item_id from t2 where insert_time >'{}' limit {} allow filtering;"\
            .format(selected_time_str, rand_limit)
        q4_result = rows_to_list(session.execute(q4))
        debug(f"Query: {q4}, Len_Result: {len(q4_result)} Result: {q4_result}")
        assert len(q4_result) == min(count_above_selected_time, rand_limit), f"The returned rows count doesnt match " \
            f"min(count_above_selected_time,rand_limit)" \
            f" [{min(count_above_selected_time,rand_limit)}]"


@since('3.0')
@require("7392")
@attr('dtest-full')
class AbortedQueriesTester(CQLTester):
    """
    @jira_ticket CASSANDRA-7392
    Test that read-queries that take longer than read_request_timeout_in_ms time out
    """
    @attr('single_node')
    def local_query_test(self):
        """
        Check that a query running on the local coordinator node times out
        """
        cluster = self.cluster
        cluster.set_configuration_options(values={'read_request_timeout_in_ms': 1000})

        # cassandra.test.read_iteration_delay_ms causes the state tracking read iterators
        # introduced by CASSANDRA-7392 to pause by the specified amount of milliseconds during each
        # iteration of non system queries, so that these queries take much longer to complete,
        # see ReadCommand.withStateTracking()
        cluster.populate(1).start(wait_for_binary_proto=True, jvm_args=["-Dcassandra.monitoring_check_interval_ms=50",
                                                                        "-Dcassandra.test.read_iteration_delay_ms=1500"])
        node = cluster.nodelist()[0]
        session = self.patient_cql_connection(node)

        self.create_ks(session, 'ks', 1)
        session.execute("""
            CREATE TABLE test1 (
                id int PRIMARY KEY,
                val text
            );
        """)

        for i in range(500):
            session.execute("INSERT INTO test1 (id, val) VALUES ({}, 'foo')".format(i))

        mark = node.mark_log()
        statement = SimpleStatement("SELECT * from test1", consistency_level=ConsistencyLevel.ONE,
                                    retry_policy=FallthroughRetryPolicy())
        assert_unavailable(lambda c: debug(c.execute(statement)), session)
        node.watch_log_for("Some operations timed out", from_mark=mark, timeout=60)

    def remote_query_test(self):
        """
        Check that a query running on a node other than the coordinator times out
        """
        cluster = self.cluster
        cluster.set_configuration_options(values={'read_request_timeout_in_ms': 1000})

        cluster.populate(2)
        node1, node2 = cluster.nodelist()

        node1.start(wait_for_binary_proto=True, join_ring=False)  # ensure other node executes queries
        node2.start(wait_for_binary_proto=True, jvm_args=["-Dcassandra.monitoring_check_interval_ms=50",
                                                          "-Dcassandra.test.read_iteration_delay_ms=1500"])  # see above for explanation

        session = self.patient_exclusive_cql_connection(node1)

        self.create_ks(session, 'ks', 1)
        session.execute("""
            CREATE TABLE test2 (
                id int,
                col int,
                val text,
                PRIMARY KEY(id, col)
            );
        """)

        for i in range(500):
            for j in range(10):
                session.execute("INSERT INTO test2 (id, col, val) VALUES ({}, {}, 'foo')".format(i, j))

        mark = node2.mark_log()

        statement = SimpleStatement("SELECT * from test2", consistency_level=ConsistencyLevel.ONE,
                                    retry_policy=FallthroughRetryPolicy())
        assert_unavailable(lambda c: debug(c.execute(statement)), session)

        statement = SimpleStatement("SELECT * from test2 where id = 1",
                                    consistency_level=ConsistencyLevel.ONE, retry_policy=FallthroughRetryPolicy())
        assert_unavailable(lambda c: debug(c.execute(statement)), session)

        statement = SimpleStatement("SELECT * from test2 where id IN (1, 10,  20) AND col < 10",
                                    consistency_level=ConsistencyLevel.ONE, retry_policy=FallthroughRetryPolicy())
        assert_unavailable(lambda c: debug(c.execute(statement)), session)

        statement = SimpleStatement("SELECT * from test2 where col > 5 ALLOW FILTERING",
                                    consistency_level=ConsistencyLevel.ONE, retry_policy=FallthroughRetryPolicy())
        assert_unavailable(lambda c: debug(c.execute(statement)), session)

        node2.watch_log_for("Some operations timed out", from_mark=mark, timeout=60)

    @attr('single_node')
    def index_query_test(self):
        """
        Check that a secondary index query times out
        """
        cluster = self.cluster
        cluster.set_configuration_options(values={'read_request_timeout_in_ms': 1000})

        cluster.populate(1).start(wait_for_binary_proto=True, jvm_args=["-Dcassandra.monitoring_check_interval_ms=50",
                                                                        "-Dcassandra.test.read_iteration_delay_ms=1500"])  # see above for explanation
        node = cluster.nodelist()[0]
        session = self.patient_cql_connection(node)

        self.create_ks(session, 'ks', 1)
        session.execute("""
            CREATE TABLE test3 (
                id int PRIMARY KEY,
                col int,
                val text
            );
        """)

        session.execute("CREATE INDEX ON test3 (col)")

        for i in range(500):
            session.execute("INSERT INTO test3 (id, col, val) VALUES ({}, {}, 'foo')".format(i, i // 10))

        mark = node.mark_log()
        statement = session.prepare("SELECT * from test3 WHERE col < ? ALLOW FILTERING")
        statement.consistency_level = ConsistencyLevel.ONE
        statement.retry_policy = FallthroughRetryPolicy()
        assert_unavailable(lambda c: debug(c.execute(statement, [50])), session)
        node.watch_log_for("Some operations timed out", from_mark=mark, timeout=60)

    def materialized_view_test(self):
        """
        Check that a materialized view query times out
        """
        cluster = self.cluster
        cluster.set_configuration_options(values={'read_request_timeout_in_ms': 1000})

        cluster.populate(2)
        node1, node2 = cluster.nodelist()

        node1.start(wait_for_binary_proto=True, join_ring=False)  # ensure other node executes queries
        node2.start(wait_for_binary_proto=True, jvm_args=["-Dcassandra.monitoring_check_interval_ms=50",
                                                          "-Dcassandra.test.read_iteration_delay_ms=1500"])  # see above for explanation

        session = self.patient_exclusive_cql_connection(node1)

        self.create_ks(session, 'ks', 1)
        session.execute("""
            CREATE TABLE test4 (
                id int PRIMARY KEY,
                col int,
                val text
            );
        """)

        session.execute(("CREATE MATERIALIZED VIEW mv AS SELECT * FROM test4 "
                         "WHERE col IS NOT NULL AND id IS NOT NULL PRIMARY KEY (col, id)"))

        for i in range(50):
            session.execute("INSERT INTO test4 (id, col, val) VALUES ({}, {}, 'foo')".format(i, i // 10))

        mark = node2.mark_log()
        statement = SimpleStatement("SELECT * FROM mv WHERE col = 50",
                                    consistency_level=ConsistencyLevel.ONE, retry_policy=FallthroughRetryPolicy())
        assert_unavailable(lambda c: debug(c.execute(statement)), session)
        node2.watch_log_for("Some operations timed out", from_mark=mark, timeout=60)
