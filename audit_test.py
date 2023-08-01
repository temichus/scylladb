from contextlib import contextmanager
from dataclasses import dataclass
import logging
import os.path
from typing import Optional, Dict, List, Any

import pytest
from cassandra import ConsistencyLevel, InvalidRequest, Unauthorized
from cassandra.query import named_tuple_factory
from cassandra.query import SimpleStatement
from ccmlib.node import NodeError
from cassandra.cluster import Session

from tools.assertions import assert_invalid
from dtest_class import Tester, create_ks
from tools.data import rows_to_list

logger = logging.getLogger(__name__)


class AuditRowMustNotExist(Exception):
    pass


class AuditTester(Tester):
    audit_default_settings = {'audit': 'table',
                              'audit_categories': 'ADMIN,AUTH,QUERY,DML,DDL,DCL',
                              'audit_keyspaces': 'ks'}
    def prepare(self, ordered=False, create_keyspace=True, use_cache=False,
                nodes=1, rf=1, protocol_version=None, user=None, password=None,
                audit_settings=audit_default_settings, reload_config=False, **kwargs):
        logger.debug(f"Preparing cluster with {nodes} node(s): rf={rf} ordered={ordered} use_cache={use_cache} "
                     f"audit_settings={audit_settings}")

        cluster = self.cluster

        if ordered:
            cluster.set_partitioner("org.apache.cassandra.dht.ByteOrderedPartitioner")

        if use_cache:
            cluster.set_configuration_options(values={'row_cache_size_in_mb': 100})

        start_rpc = kwargs.pop('start_rpc', False)
        if start_rpc:
            cluster.set_configuration_options(values={'start_rpc': True})

        cluster.set_configuration_options(values=audit_settings)

        if user:
            config = {'authenticator': 'org.apache.cassandra.auth.PasswordAuthenticator',
                      'authorizer': 'org.apache.cassandra.auth.CassandraAuthorizer',
                      'permissions_validity_in_ms': 0}
            cluster.set_configuration_options(values=config)

        if reload_config:
            # The cluster is restarted to reload the config file.
            cluster.stop()
            cluster.start(wait_for_binary_proto=True)

        if not cluster.nodelist():
            cluster.populate([nodes]).start(wait_for_binary_proto=True)
        node1 = cluster.nodelist()[0]

        session = self.patient_cql_connection(node1, protocol_version=protocol_version, user=user, password=password)
        if create_keyspace:
            session.execute("DROP KEYSPACE IF EXISTS ks")
            create_ks(session, 'ks', rf)
        return session


@dataclass
class AuditEntry:
    category: str
    statement: str
    table: str
    ks: str
    user: str
    cl: str
    error: bool


@pytest.mark.dtest_full
@pytest.mark.dtest_enterprise
@pytest.mark.single_node
class TestCQLAudit(AuditTester):
    """
    Make sure CQL statements are audited
    """
    AUDIT_LOG_QUERY = "SELECT * FROM audit.audit_log"

    def deduplicate_audit_entries(self, entries):
        """
        Returns a list of audit entries with duplicate entries removed.
        """
        unique = set()
        deduplicated_entries = []

        for entry in entries:
            fields_subset = (entry.node, entry.category, entry.consistency, entry.error,
                             entry.keyspace_name, entry.operation, entry.source,
                             entry.table_name, entry.username)

            if fields_subset in unique:
                continue

            unique.add(fields_subset)
            deduplicated_entries.append(entry)

        return deduplicated_entries

    def getAuditLogList(self, session):
        """_summary_
            returns a sorted list of audit log, the logs are sorted by the event times (time-uuid)
            with the node as tie breaker.
        """
        # We would like to have named tuples as results so we can verify the
        # order in which the fields are returned as the tests make assumptions about this.
        assert session.row_factory == named_tuple_factory
        res = session.execute(self.AUDIT_LOG_QUERY)
        res_list = list(res)
        res_list.sort(key=lambda row: (row.event_time, row.node))
        return res_list

    # This assert is added just in order to still fail the test if the order of columns is changed, this is an implied assumption
    def assertAuditRowFields(self, row):
        expected_fields = ['date', 'node', 'event_time', 'category', 'consistency',
                           'error', 'keyspace_name', 'operation', 'source', 'table_name', 'username']
        assert list(row._fields) == expected_fields

    def assertAuditRow(self,    row, category, statement, table="", ks="ks", user="anonymous", cl="ONE", error=False):
        self.assertAuditRowFields(row)
        assert row.node == self.cluster.get_node_ip(1)
        assert row.category == category
        assert row.consistency == cl
        assert row.error == error
        assert row.keyspace_name == ks
        assert row.operation == statement
        assert row.source == "127.0.0.1"
        assert row.table_name == table
        assert row.username == user

    def assertLastAuditRow(self, session, category, statement, table="", ks="ks", user="anonymous", cl="ONE",
                           error=False, match=True):
        res_list = self.getAuditLogList(session)

        assert len(res_list) > 0
        try:
            logger.debug("last audit row: %s", res_list[-1])
            self.assertAuditRow(res_list[-1], category, statement, table, ks, user, cl, error)
            if not match:
                raise AuditRowMustNotExist(f'row: {res_list[-1]} shouldn\'t match')
        except AssertionError:
            if match:
                raise

    def getAuditEntriesCount(self, session):
        res_list = self.getAuditLogList(session)
        logger.debug('Printing audit table content:')
        for row in res_list:
            logger.debug('  %s', row)
        return len(res_list)

    @contextmanager
    def assert_no_audit_entries_were_added(self, session):
        count_before = self.getAuditEntriesCount(session)
        yield
        count_after = self.getAuditEntriesCount(session)
        assert count_before == count_after, \
            "audit entries count changed (before: {} after: {})".format(count_before, count_after)

    def execute_and_validate_audit_entry(self, session: Session,
                                         query: Any,
                                         category: str,
                                         audit_settings: Dict[str, str] = AuditTester.audit_default_settings,
                                         table: str = "",
                                         ks: str = "ks",
                                         cl: str = "ONE",
                                         user: str = "anonymous",
                                         expected_error: Any = None,
                                         bound_values: Optional[List[Any]] = None,
                                         expect_new_audit_entry: bool = True,
                                         expected_operation: str = None,
                                         session_for_audit_entry_validation: Optional[Session] = None):
        """
        Execute a query and validate that an audit entry was added to the audit
        log table. Use the audit_settings parameter in combination with category
        to determine if the audit entry should be added or not. If the audit
        entry is expected, validate that the audit entry's content is as
        expected.
        """

        # In some cases, provided session does not have access to the audit
        # table. In that case, session_for_audit_entry_validation should be
        # provided.
        if session_for_audit_entry_validation is None:
            session_for_audit_entry_validation = session

        if category in audit_settings['audit_categories'].split(',') and expect_new_audit_entry:
            operation = query if expected_operation is None else expected_operation
            error = expected_error is not None

            expected_entries = [AuditEntry(category, operation, table, ks, user, cl, error)]
        else:
            expected_entries = []

        with self.assert_entries_were_added(session_for_audit_entry_validation,
                                            expected_entries):
            if expected_error is None:
                res = session.execute(query, bound_values)
            else:
                assert_invalid(session, query, expected=expected_error)
                res = None

        return res

    @contextmanager
    def assert_entries_were_added(self, session: Session, expected_entries: List[AuditEntry],
                                  merge_duplicate_rows: bool=True):
        # Get audit entries before executing the query, to later compare with
        # audit entries after executing the query.
        rows_before = self.getAuditLogList(session)
        set_of_rows_before = set(rows_before)
        assert len(set_of_rows_before) == len(rows_before), \
            f"audit table contains duplicate rows: {rows_before}"

        yield

        # Remember audit entries after executing the query.
        rows_after = self.getAuditLogList(session)
        set_of_rows_after = set(rows_after)
        assert len(set_of_rows_after) == len(rows_after), \
            f"audit table contains duplicate rows: {rows_after}"

        new_rows = rows_after[len(rows_before):]
        assert set(new_rows) == set_of_rows_after - set_of_rows_before, \
            f"new rows are not the last rows in the audit table: {rows_after}"

        if merge_duplicate_rows:
            new_rows = self.deduplicate_audit_entries(new_rows)

        assert len(new_rows) == len(expected_entries), \
            f"Expected {len(expected_entries)} new audit entries, but got {len(new_rows)} new entries: {new_rows}"

        for row, entry in zip(new_rows, expected_entries):
            self.assertAuditRow(row, entry.category, entry.statement,
                                entry.table, entry.ks, entry.user, entry.cl, entry.error)

    def verify_keyspace(self, audit_settings=None):
        """
        CREATE KEYSPACE, USE KEYSPACE, ALTER KEYSPACE, DROP KEYSPACE statements
        """
        session = self.prepare(create_keyspace=False, audit_settings=audit_settings)

        def execute_and_validate_audit_entry(query, category, **kwargs):
            return self.execute_and_validate_audit_entry(session, query, category, audit_settings, **kwargs)

        execute_and_validate_audit_entry(
            "CREATE KEYSPACE ks WITH replication = { 'class':'SimpleStrategy', 'replication_factor':1} AND DURABLE_WRITES = true",
            category="DDL",
        )
        execute_and_validate_audit_entry(
            'USE "ks"',
            category="DML",
        )
        execute_and_validate_audit_entry(
            "ALTER KEYSPACE ks WITH replication = { 'class' : 'NetworkTopologyStrategy', 'dc1' : 1 } AND DURABLE_WRITES = false",
            category="DDL",
        )
        execute_and_validate_audit_entry(
            "DROP KEYSPACE ks",
            category="DDL",
        )

        # Test that the audit entries are not added if the keyspace is not
        # specified in the audit_keyspaces setting.
        keyspaces = audit_settings['audit_keyspaces'].split(',') if 'audit_keyspaces' in audit_settings else []
        assert "ks2" not in keyspaces
        query_sequence = [
            "CREATE KEYSPACE ks2 WITH replication = { 'class':'SimpleStrategy', 'replication_factor':1} AND DURABLE_WRITES = true",
            'USE "ks2"',
            "ALTER KEYSPACE ks2 WITH replication = { 'class' : 'NetworkTopologyStrategy', 'dc1' : 1 } AND DURABLE_WRITES = false",
            "DROP KEYSPACE ks2",
        ]

        with self.assert_no_audit_entries_were_added(session):
            for query in query_sequence:
                session.execute(query)

    @pytest.mark.require('scylladb/scylla-enterprise#3236')
    def test_using_non_existent_keyspace(self):
        """
        Test tha using a non-existent keyspace generates an audit entry with an
        error field set to True.
        """
        session = self.prepare()

        self.execute_and_validate_audit_entry(
            session,
            'USE "non_existing_ks"',
            category="DML",
            expected_error=InvalidRequest,
        )

    def verify_table(self, audit_settings=AuditTester.audit_default_settings):
        """
        CREATE TABLE, ALTER TABLE, TRUNCATE TABLE, DROP TABLE statements
        """
        session = self.prepare(audit_settings=audit_settings)

        session.execute("CREATE TABLE test1 (k int PRIMARY KEY, v1 int)")
        self.assertLastAuditRow(session, "DDL", "CREATE TABLE test1 (k int PRIMARY KEY, v1 int)", "test1",
                                match='DDL' in audit_settings['audit_categories'])
        session.execute("CREATE TABLE test2 (k int, c1 int, v1 int, PRIMARY KEY (k, c1)) WITH COMPACT STORAGE")
        self.assertLastAuditRow(session, "DDL",
                                "CREATE TABLE test2 (k int, c1 int, v1 int, PRIMARY KEY (k, c1)) WITH COMPACT STORAGE",
                                "test2", match='DDL' in audit_settings['audit_categories'])

        session.execute("ALTER TABLE test1 ADD v2 int")
        self.assertLastAuditRow(session, "DDL", "ALTER TABLE test1 ADD v2 int", "test1",
                                match='DDL' in audit_settings['audit_categories'])

        for i in range(0, 10):
            session.execute("INSERT INTO test1 (k, v1, v2) VALUES (%d, %d, %d)" % (i, i, i))
            self.assertLastAuditRow(session, "DML", "INSERT INTO test1 (k, v1, v2) VALUES (%d, %d, %d)" % (i, i, i),
                                    "test1", match='DML' in audit_settings['audit_categories'])
            session.execute("INSERT INTO test2 (k, c1, v1) VALUES (%d, %d, %d)" % (i, i, i))
            self.assertLastAuditRow(session, "DML", "INSERT INTO test2 (k, c1, v1) VALUES (%d, %d, %d)" % (i, i, i),
                                    "test2", match='DML' in audit_settings['audit_categories'])

        res = sorted(session.execute("SELECT * FROM test1"))
        assert rows_to_list(res) == [[i, i, i] for i in range(0, 10)], res
        self.assertLastAuditRow(session, "QUERY", "SELECT * FROM test1", "test1",
                                match='QUERY' in audit_settings['audit_categories'])

        res = sorted(session.execute("SELECT * FROM test2"))
        assert rows_to_list(res) == [[i, i, i] for i in range(0, 10)], res
        self.assertLastAuditRow(session, "QUERY", "SELECT * FROM test2", "test2",
                                match='QUERY' in audit_settings['audit_categories'])

        session.execute("TRUNCATE test1")
        self.assertLastAuditRow(session, "DML", "TRUNCATE test1", "test1",
                                match='DML' in audit_settings['audit_categories'])
        session.execute("TRUNCATE test2")
        self.assertLastAuditRow(session, "DML", "TRUNCATE test2", "test2",
                                match='DML' in audit_settings['audit_categories'])

        res = session.execute("SELECT * FROM test1")
        assert rows_to_list(res) == [], res
        self.assertLastAuditRow(session, "QUERY", "SELECT * FROM test1", "test1",
                                match='QUERY' in audit_settings['audit_categories'])

        res = session.execute("SELECT * FROM test2")
        assert rows_to_list(res) == [], res
        self.assertLastAuditRow(session, "QUERY", "SELECT * FROM test2", "test2",
                                match='QUERY' in audit_settings['audit_categories'])

        session.execute("DROP TABLE test1")
        self.assertLastAuditRow(session, "DDL", "DROP TABLE test1", "test1",
                                match='DDL' in audit_settings['audit_categories'])
        session.execute("DROP TABLE test2")
        self.assertLastAuditRow(session, "DDL", "DROP TABLE test2", "test2",
                                match='DDL' in audit_settings['audit_categories'])

        assert_invalid(session, "SELECT * FROM test1", expected=InvalidRequest)
        assert_invalid(session, "SELECT * FROM test2", expected=InvalidRequest)

        count_before = self.getAuditEntriesCount(session)
        session.execute(
            "CREATE KEYSPACE ks2 WITH replication = { 'class':'SimpleStrategy', 'replication_factor':1} AND DURABLE_WRITES = true")
        session.execute("CREATE TABLE ks2.test1 (k int PRIMARY KEY, v1 int)")
        session.execute("ALTER TABLE ks2.test1 ADD v2 int")
        for i in range(0, 10):
            session.execute("INSERT INTO ks2.test1 (k, v1, v2) VALUES (%d, %d, %d)" % (i, i, i))
        res = sorted(session.execute("SELECT * FROM ks2.test1"))
        assert rows_to_list(res) == [[i, i, i] for i in range(0, 10)], res
        session.execute("TRUNCATE ks2.test1")
        res = session.execute("SELECT * FROM ks2.test1")
        assert rows_to_list(res) == [], res
        session.execute("DROP TABLE ks2.test1")
        assert_invalid(session, "SELECT * FROM ks2.test1", expected=InvalidRequest)
        count_after = self.getAuditEntriesCount(session)
        assert (count_before == count_after), "count_before is {} and count_after is {}".format(count_before, count_after)

    def test_audit_keyspace(self):
        self.verify_keyspace(audit_settings=AuditTester.audit_default_settings)

    def test_audit_keyspace_extra_parameter(self):
        self.verify_keyspace(audit_settings={'audit': 'table',
                                             'audit_categories': 'ADMIN,AUTH,DML,DDL,DCL',
                                             'audit_keyspaces': 'ks',
                                             'extra_parameter': 'new'})

    def test_audit_keyspace_many_ks(self):
        self.verify_keyspace(audit_settings={'audit': 'table',
                                             'audit_categories': 'ADMIN,AUTH,QUERY,DML,DDL,DCL',
                                             'audit_keyspaces': 'a,b,c,ks'})

    def test_audit_keyspace_table_not_exists(self):
        self.verify_keyspace(audit_settings={'audit': 'table',
                                             'audit_categories': 'DML,DDL',
                                             'audit_keyspaces': 'ks',
                                             'audit_tables': 'ks.fake'})

    def test_audit_type_none(self):
        """
        'audit': None
         CREATE KEYSPACE, USE KEYSPACE, ALTER KEYSPACE, DROP KEYSPACE statements
         check audit KS not created
        """

        audit_settings = {'audit': None, 'audit_categories': 'ADMIN,AUTH,QUERY,DML,DDL,DCL',
                          'audit_keyspaces': 'ks'}

        session = self.prepare(create_keyspace=False, audit_settings=audit_settings)

        session.execute(
            "CREATE KEYSPACE ks WITH replication = { 'class':'SimpleStrategy', 'replication_factor':1} AND DURABLE_WRITES = true")

        session.execute("USE ks")

        session.execute(
            "ALTER KEYSPACE ks WITH replication = { 'class' : 'NetworkTopologyStrategy', 'dc1' : 1 } AND DURABLE_WRITES = false")

        session.execute("DROP KEYSPACE ks")
        assert_invalid(session, "use audit;", expected=InvalidRequest)

    def test_audit_type_syslog(self):
        """
        'audit': syslog
         CREATE KEYSPACE, USE KEYSPACE, ALTER KEYSPACE, DROP KEYSPACE statements
         check audit KS not created
         check /var/log/scylla-audit.log not created by default
        """
        audit_log = '/var/log/scylla-audit.log'
        if os.path.exists(audit_log):
            os.remove(audit_log)
        audit_settings = {'audit': 'syslog', 'audit_categories': 'ADMIN,AUTH,QUERY,DML,DDL,DCL',
                          'audit_keyspaces': 'ks'}

        session = self.prepare(create_keyspace=False, audit_settings=audit_settings)

        session.execute(
            "CREATE KEYSPACE ks WITH replication = { 'class':'SimpleStrategy', 'replication_factor':1} AND DURABLE_WRITES = true")

        session.execute("USE ks")

        session.execute(
            "ALTER KEYSPACE ks WITH replication = { 'class' : 'NetworkTopologyStrategy', 'dc1' : 1 } AND DURABLE_WRITES = false")

        session.execute("DROP KEYSPACE ks")
        assert_invalid(session, "use audit;", expected=InvalidRequest)
        # to see audit logs in /var/log/scylla-audit.log we need to set in /etc/rsyslog.conf
        # if $programname contains 'scylla-audit' then /var/log/scylla-audit.log
        assert not os.path.exists(audit_log)

    def test_audit_type_invalid(self):
        """
        'audit': invalid
         check node not started
        """
        self.fixture_dtest_setup.allow_log_errors = True

        audit_settings = {'audit': 'invalid', 'audit_categories': 'ADMIN,AUTH,QUERY,DML,DDL,DCL',
                          'audit_keyspaces': 'ks'}

        cluster = self.cluster

        cluster.set_configuration_options(values=audit_settings)

        try:
            cluster.populate(1).start(no_wait=True)
        except (NodeError, RuntimeError):
            pass

        expected_error = r"Startup failed: audit::audit_exception \(Bad configuration: invalid 'audit': invalid\)"
        self.ignore_log_patterns.append(expected_error)
        self.cluster.nodes['node1'].watch_log_for(expected_error)

    def test_audit_empty_settings(self):
        """
        'audit': none
         check node started, ks audit not created
        """
        session = self.prepare(create_keyspace=False, audit_settings={'audit': 'none'})
        assert_invalid(session, "use audit;", expected=InvalidRequest)

    def test_audit_audit_ks(self):
        """
        'audit_keyspaces': 'audit'
        check node started, ks audit created
        """
        audit_settings = {'audit': 'table', 'audit_categories': 'ADMIN,AUTH,QUERY,DML,DDL,DCL',
                          'audit_keyspaces': 'audit'}
        session = self.prepare(create_keyspace=False, audit_settings=audit_settings)

        self.execute_and_validate_audit_entry(
            session,

            query=self.AUDIT_LOG_QUERY,
            category="QUERY",

            ks="audit",
            table="audit_log",
            audit_settings=audit_settings
        )

    @pytest.mark.single_node
    def test_audit_categories_invalid(self):
        """
        'audit_categories': invalid
        check node not started
        """
        self.fixture_dtest_setup.allow_log_errors = True

        audit_settings = {'audit': 'table', 'audit_categories': 'INVALID',
                          'audit_keyspaces': 'ks'}

        cluster = self.cluster

        cluster.set_configuration_options(values=audit_settings)
        cluster.force_wait_for_cluster_start = False

        try:
            cluster.populate(1).start(no_wait=True)
        except NodeError:
            pass
        expected_error = r"Startup failed: audit::audit_exception \(Bad configuration: invalid 'audit_categories': INVALID\)"
        self.ignore_log_patterns.append(expected_error)
        self.cluster.nodes['node1'].watch_log_for(expected_error)

    def test_audit_table(self):
        self.verify_table(audit_settings=AuditTester.audit_default_settings)

    def test_audit_table_extra_parameter(self):
        self.verify_table(audit_settings={'audit': 'table',
                                          'audit_categories': 'ADMIN,AUTH,QUERY,DML,DDL,DCL',
                                          'audit_keyspaces': 'ks',
                                          'extra_parameter': 'new'})

    def test_audit_table_audit_keyspaces_empty(self):
        self.verify_table(audit_settings={'audit': 'table',
                                          'audit_categories': 'ADMIN,AUTH,QUERY,DML,DDL,DCL',
                                          'audit_keyspaces': '',
                                          'audit_tables': 'ks.test1, ks.test2'})

    def test_audit_table_no_ks(self):
        self.verify_table(audit_settings={'audit': 'table',
                                          'audit_categories': 'ADMIN,AUTH,QUERY,DML,DDL,DCL',
                                          'audit_tables': 'ks.test2, ks.test1'})

    def test_audit_categories_part1(self):
        self.verify_table(audit_settings={'audit': 'table',
                                          'audit_categories': 'AUTH,QUERY,DDL',
                                          'audit_tables': 'ks.test2, ks.test1'})

    def test_audit_categories_part2(self):
        self.verify_table(audit_settings={'audit': 'table',
                                          'audit_categories': 'DDL, ADMIN,AUTH,DCL',
                                          'audit_keyspaces': 'ks'})

    def test_audit_categories_part3(self):
        self.verify_table(audit_settings={'audit': 'table',
                                          'audit_categories': 'DDL, ADMIN,AUTH',
                                          'audit_keyspaces': 'ks'})

    def test_user_password_masking(self):
        """
        CREATE USER, ALTER USER, DROP USER statements
        """
        session = self.prepare(user='cassandra', password='cassandra')

        session.execute("CREATE USER user1 WITH PASSWORD 'secret'")
        self.assertLastAuditRow(session, "DCL", "CREATE USER user1 WITH PASSWORD '***'", ks="", user="cassandra")

        session.execute("ALTER USER user1 WITH PASSWORD 'Secret^%$#@!'")
        self.assertLastAuditRow(session, "DCL", "ALTER USER user1 WITH PASSWORD '***'", ks="", user="cassandra")

        session.execute("DROP USER user1")
        self.assertLastAuditRow(session, "DCL", "DROP USER user1", ks="", user="cassandra")

    def test_role_password_masking(self):
        """
        CREATE ROLE, ALTER ROLE, DROP ROLE statements
        """
        session = self.prepare(user='cassandra', password='cassandra')

        session.execute("CREATE ROLE role1 WITH PASSWORD = 'Secret!@#$'")
        self.assertLastAuditRow(session, "DCL", "CREATE ROLE role1 WITH PASSWORD = '***'", ks="", user="cassandra")

        session.execute("ALTER ROLE role1 WITH PASSWORD = 'Secret^%$#@!'")
        self.assertLastAuditRow(session, "DCL", "ALTER ROLE role1 WITH PASSWORD = '***'", ks="", user="cassandra")

        session.execute("DROP ROLE role1")
        self.assertLastAuditRow(session, "DCL", "DROP ROLE role1", ks="", user="cassandra")

    def test_login(self):
        """
        USER LOGIN
        """
        session = self.prepare(user='cassandra', password='cassandra', create_keyspace=False)
        self.assertLastAuditRow(session, "AUTH", "LOGIN", ks="", user="cassandra", cl="")

    def test_categories(self):
        """
        Test filtering audit categories
        """
        session = self.prepare(audit_settings={'audit': 'table', 'audit_categories': 'DML',
                                               'audit_keyspaces': 'ks'})
        count_before = self.getAuditEntriesCount(session)
        session.execute("CREATE TABLE test1 (k int PRIMARY KEY, v1 int)")
        count_after = self.getAuditEntriesCount(session)
        assert (count_before == count_after), "count_before is {} and count_after is {}".format(count_before, count_after)

        session.execute("ALTER TABLE test1 ADD v2 int")
        count_after = self.getAuditEntriesCount(session)
        assert (count_before == count_after), "count_before is {} and count_after is {}".format(count_before, count_after)

        for i in range(0, 10):
            session.execute("INSERT INTO test1 (k, v1, v2) VALUES (%d, %d, %d)" % (i, i, i))
            self.assertLastAuditRow(session, "DML", "INSERT INTO test1 (k, v1, v2) VALUES (%d, %d, %d)" % (i, i, i),
                                    "test1")

        count_before = self.getAuditEntriesCount(session)
        res = sorted(session.execute("SELECT * FROM test1"))
        assert rows_to_list(res) == [[i, i, i] for i in range(0, 10)], res
        count_after = self.getAuditEntriesCount(session)
        assert (count_before == count_after), "count_before is {} and count_after is {}".format(count_before, count_after)

        session.execute("TRUNCATE test1")
        self.assertLastAuditRow(session, "DML", "TRUNCATE test1", "test1")

        count_before = self.getAuditEntriesCount(session)
        res = session.execute("SELECT * FROM test1")
        assert rows_to_list(res) == [], res
        count_after = self.getAuditEntriesCount(session)
        assert (count_before == count_after), "count_before is {} and count_after is {}".format(count_before, count_after)

        session.execute("DROP TABLE test1")
        count_after = self.getAuditEntriesCount(session)
        assert (count_before == count_after), "count_before is {} and count_after is {}".format(count_before, count_after)

    def test_prepare(self):
        """ Test prepare statement """
        session = self.prepare()

        session.execute("""
            CREATE TABLE cf (
                k varchar PRIMARY KEY,
                c int,
            )
        """)

        count_before = self.getAuditEntriesCount(session)
        query = "INSERT INTO cf (k, c) VALUES (?, ?);"
        pq = session.prepare(query)
        count_after = self.getAuditEntriesCount(session)
        assert (count_before == count_after), "count_before is {} and count_after is {}".format(count_before, count_after)

        session.execute(pq, ['foo', 4])
        self.assertLastAuditRow(session, "DML", "INSERT INTO cf (k, c) VALUES (?, ?);", "cf")

    def test_permissions(self):
        """ Test user permissions """
        session = self.prepare(user='cassandra', password='cassandra')
        session.execute("CREATE TABLE test1 (k int PRIMARY KEY, v1 int)")
        session.execute("CREATE USER test WITH PASSWORD 'test'")
        session.execute("GRANT SELECT ON ks.test1 TO test")
        session.execute("INSERT INTO test1 (k, v1) VALUES (1, 1)")

        test_session = self.patient_cql_connection(self.cluster.nodelist()[0], user="test", password="test")

        test_session.execute("SELECT * FROM ks.test1")
        self.assertLastAuditRow(session, "QUERY", "SELECT * FROM ks.test1", table="test1", user="test")
        try:
            test_session.execute("INSERT INTO ks.test1 (k, v1) VALUES (2, 2)")
            assert False, "user `test` query should have failed"
        except Unauthorized as e:
            logger.debug(e)

        self.assertLastAuditRow(session, "DML", "INSERT INTO ks.test1 (k, v1) VALUES (2, 2)", table="test1",
                                user="test", error=True, match=True)

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
        res_list = self.getAuditLogList(session)

        assert len(res_list) > 3

        self.assertAuditRow(res_list[-4], "DML",
                            "INSERT INTO test8 (userid, password, name) VALUES (user2, ch@ngem3b, second user)",
                            "test8", cl="QUORUM")
        self.assertAuditRow(res_list[-3], "DML", "UPDATE test8 SET password = ps22dhds WHERE userid = user3", "test8",
                            cl="QUORUM")
        self.assertAuditRow(res_list[-2], "DML", "INSERT INTO test8 (userid, password) VALUES (user4, ch@ngem3c)",
                            "test8", cl="QUORUM")
        self.assertAuditRow(res_list[-1], "DML", "DELETE name FROM test8 WHERE userid = user1", "test8", cl="QUORUM")

    def test_service_level_statements(self):
        """
        Test auditing service level statements - ones that use the ADMIN audit category.
        """
        audit_settings = {'audit': 'table', 'audit_categories': 'ADMIN'}
        session = self.prepare(user='cassandra', password='cassandra',
                               audit_settings=audit_settings)

        # Create role to which a service level can be attached.
        session.execute("CREATE ROLE test_role")

        query_sequence = [
            "CREATE SERVICE_LEVEL test_service_level WITH SHARES = 1",

            "ATTACH SERVICE_LEVEL test_service_level TO test_role",
            "DETACH SERVICE_LEVEL FROM test_role",

            "LIST SERVICE_LEVEL test_service_level",
            "LIST ALL SERVICE_LEVELS",
            "LIST ATTACHED SERVICE_LEVEL OF test_role",
            "LIST ALL ATTACHED SERVICE_LEVELS",

            "ALTER SERVICE_LEVEL test_service_level WITH SHARES = 2",
            "DROP SERVICE_LEVEL test_service_level",
        ]

        # Execute previously defined service level statements.
        # Validate that the audit log contains the expected entries.
        for query in query_sequence:
            self.execute_and_validate_audit_entry(
                session,
                query,
                category="ADMIN",
                audit_settings=audit_settings,
                ks="",
                user="cassandra"
            )
