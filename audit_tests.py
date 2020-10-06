import os.path

from cassandra import ConsistencyLevel, InvalidRequest
from cassandra.query import SimpleStatement
from ccmlib.node import NodeError

from assertions import assert_invalid
from dtest import Tester, debug
from tools import rows_to_list


class AuditTester(Tester):
    audit_default_settings = {'audit': 'table',
                              'audit_categories': 'ADMIN,AUTH,QUERY,DML,DDL,DCL',
                              'audit_keyspaces': 'ks'}

    def prepare(self, ordered=False, create_keyspace=True, use_cache=False, nodes=1, rf=1, protocol_version=None,
                user=None, password=None, experimental=False, audit_settings=audit_default_settings, **kwargs):
        cluster = self.cluster

        if ordered:
            cluster.set_partitioner("org.apache.cassandra.dht.ByteOrderedPartitioner")

        if use_cache:
            cluster.set_configuration_options(values={'row_cache_size_in_mb': 100})

        start_rpc = kwargs.pop('start_rpc', False)
        if start_rpc:
            cluster.set_configuration_options(values={'start_rpc': True})

        if experimental:
            cluster.set_configuration_options(values={'experimental': True})

        cluster.set_configuration_options(values=audit_settings)

        if user:
            config = {'authenticator': 'org.apache.cassandra.auth.PasswordAuthenticator',
                      'authorizer': 'org.apache.cassandra.auth.CassandraAuthorizer',
                      'permissions_validity_in_ms': 0}
            cluster.set_configuration_options(values=config)

        if not cluster.nodelist():
            cluster.populate(nodes).start(wait_for_binary_proto=True)
        node1 = cluster.nodelist()[0]

        session = self.patient_cql_connection(node1, protocol_version=protocol_version, user=user, password=password)
        if create_keyspace:
            if self._preserve_cluster:
                session.execute("DROP KEYSPACE IF EXISTS ks")
            self.create_ks(session, 'ks', rf)
        return session


class CQLAuditTester(AuditTester):
    """
    Make sure CQL statements are audited
    """

    def assertAuditRow(self, row, category, statement, table="", ks="ks", user="anonymous", cl="ONE", error=False):
        self.assertEqual(row[1], self.cluster.get_node_ip(1))
        self.assertEqual(row[3], category)
        self.assertEqual(row[4], cl)
        self.assertEqual(row[5], error)
        self.assertEqual(row[6], ks)
        self.assertEqual(row[7], statement)
        self.assertEqual(row[8], "127.0.0.1")
        self.assertEqual(row[9], table)
        self.assertEqual(row[10], user)

    def assertLastAuditRow(self, session, category, statement, table="", ks="ks", user="anonymous", cl="ONE",
                           error=False, match=True):
        res = session.execute("SELECT * FROM audit.audit_log")
        res_list = rows_to_list(res)

        assert len(res_list) > 0
        try:
            self.assertAuditRow(res_list[len(res_list) - 1], category, statement, table, ks, user, cl, error)
            self.assertTrue(match)
        except:
            self.assertFalse(match)

    def getAuditEntriesCount(self, session):
        res = session.execute("SELECT * FROM audit.audit_log")
        res_list = rows_to_list(res)
        debug('Printing audit table content: {}'.format(res_list))
        return len(res_list)

    def verify_keyspace(self, audit_settings=None):
        """
        CREATE KEYSPACE, USE KEYSPACE, ALTER KEYSPACE, DROP KEYSPACE statements
        """
        session = self.prepare(create_keyspace=False, audit_settings=audit_settings)

        session.execute(
            "CREATE KEYSPACE ks WITH replication = { 'class':'SimpleStrategy', 'replication_factor':1} AND DURABLE_WRITES = true")
        self.assertLastAuditRow(session, "DDL",
                                "CREATE KEYSPACE ks WITH replication = { 'class':'SimpleStrategy', 'replication_factor':1} AND DURABLE_WRITES = true",
                                match='DDL' in audit_settings['audit_categories'])

        session.execute("USE ks")
        self.assertLastAuditRow(session, "DML", 'USE "ks"', match='DML' in audit_settings['audit_categories'])

        session.execute(
            "ALTER KEYSPACE ks WITH replication = { 'class' : 'NetworkTopologyStrategy', 'dc1' : 1 } AND DURABLE_WRITES = false")
        self.assertLastAuditRow(session, "DDL",
                                "ALTER KEYSPACE ks WITH replication = { 'class' : 'NetworkTopologyStrategy', 'dc1' : 1 } AND DURABLE_WRITES = false",
                                match='DDL' in audit_settings['audit_categories'])

        session.execute("DROP KEYSPACE ks")
        self.assertLastAuditRow(session, "DDL", "DROP KEYSPACE ks", match='DDL' in audit_settings['audit_categories'])
        assert_invalid(session, "USE ks", expected=InvalidRequest)
        self.assertLastAuditRow(session, "DML", "USE ks", match='DML' in audit_settings['audit_categories'])

        count_before = self.getAuditEntriesCount(session)
        session.execute(
            "CREATE KEYSPACE ks2 WITH replication = { 'class':'SimpleStrategy', 'replication_factor':1} AND DURABLE_WRITES = true")
        session.execute("USE ks2")
        session.execute(
            "ALTER KEYSPACE ks2 WITH replication = { 'class' : 'NetworkTopologyStrategy', 'dc1' : 1 } AND DURABLE_WRITES = false")
        session.execute("DROP KEYSPACE ks2")
        assert_invalid(session, "USE ks2", expected=InvalidRequest)
        count_after = self.getAuditEntriesCount(session)
        assert (count_before == count_after), "count_before is {} and count_after is {}".format(count_before, count_after)

    def verify_table(self, audit_settings=AuditTester.audit_default_settings):
        """
        CREATE TABLE, ALTER TABLE, TRUNCATE TABLE, DROP TABLE statements
        """
        session = self.prepare(experimental=True, audit_settings=audit_settings)

        session.execute("CREATE TABLE test1 (k int PRIMARY KEY, v1 int)")
        self.assertLastAuditRow(session, "DDL", "CREATE TABLE test1 (k int PRIMARY KEY, v1 int)", "test1",
                                match='DDL' in audit_settings['audit_categories'])
        session.execute("CREATE TABLE test2 (k int, c1 int, v1 int, PRIMARY KEY (k, c1)) WITH COMPACT STORAGE")
        self.assertLastAuditRow(session, "DDL",
                                "CREATE TABLE test2 (k int, c1 int, v1 int, PRIMARY KEY (k, c1)) WITH COMPACT STORAGE",
                                "test2", match='DDL' in audit_settings['audit_categories'])

        session.execute("ALTER TABLE test1 ADD v2 int")
        self.assertLastAuditRow(session, "DDL", "ALTER TABLE test1 ADD v2 int", "test1",
                                match='DML' in audit_settings['audit_categories'])

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

    def audit_keyspace_test(self):
        self.verify_keyspace(audit_settings=AuditTester.audit_default_settings)

    def audit_keyspace_extra_parameter_test(self):
        self.verify_keyspace(audit_settings={'audit': 'table',
                                             'audit_categories': 'ADMIN,AUTH,DML,DDL,DCL',
                                             'audit_keyspaces': 'ks',
                                             'extra_parameter': 'new'})

    def audit_keyspace_many_ks_test(self):
        self.verify_keyspace(audit_settings={'audit': 'table',
                                             'audit_categories': 'ADMIN,AUTH,QUERY,DML,DDL,DCL',
                                             'audit_keyspaces': 'a,b,c,ks'})

    def audit_keyspace_table_not_exists_test(self):
        self.verify_keyspace(audit_settings={'audit': 'table',
                                             'audit_categories': 'DML,DDL',
                                             'audit_keyspaces': 'ks',
                                             'audit_tables': 'ks.fake'})

    def audit_type_none_test(self):
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

    def audit_type_syslog_test(self):
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
        self.assertFalse(os.path.exists(audit_log))

    def audit_type_invalid_test(self):
        """
        'audit': invalid
         check node not started
        """
        self.allow_log_errors = True

        audit_settings = {'audit': 'invalid', 'audit_categories': 'ADMIN,AUTH,QUERY,DML,DDL,DCL',
                          'audit_keyspaces': 'ks'}

        cluster = self.cluster

        cluster.set_configuration_options(values=audit_settings)

        try:
            cluster.populate(1).start(no_wait=True)
        except NodeError:
            pass

        expected_error = "Startup failed: audit::audit_exception \(Bad configuration: invalid 'audit': invalid\)"
        self.ignore_log_patterns.append(expected_error)
        self.cluster.nodes['node1'].watch_log_for(expected_error)

    def audit_empty_settings_test(self):
        """
        'audit': {}
         check node started, ks audit not created
        """
        session = self.prepare(create_keyspace=False, audit_settings={})
        assert_invalid(session, "use audit;", expected=InvalidRequest)

    def audit_audit_ks_test(self):
        """
        'audit_keyspaces': 'audit'
        check node started, ks audit created
        """
        session = self.prepare(create_keyspace=False,
                               audit_settings={'audit': 'table', 'audit_categories': 'ADMIN,AUTH,QUERY,DML,DDL,DCL',
                                               'audit_keyspaces': 'audit'})

        self.assertLastAuditRow(session, "QUERY", "SELECT * FROM audit.audit_log", ks="audit", table="audit_log")

    def audit_categories_invalid_test(self):
        """
        'audit_categories': invalid
        check node not started
        """
        self.allow_log_errors = True

        audit_settings = {'audit': 'table', 'audit_categories': 'INVALID',
                          'audit_keyspaces': 'ks'}

        cluster = self.cluster

        cluster.set_configuration_options(values=audit_settings)

        try:
            cluster.populate(1).start(no_wait=True)
        except NodeError:
            pass
        expected_error = "Startup failed: audit::audit_exception \(Bad configuration: invalid 'audit_categories': INVALID\)"
        self.ignore_log_patterns.append(expected_error)
        self.cluster.nodes['node1'].watch_log_for(expected_error)

    def audit_table_test(self):
        self.verify_table(audit_settings=AuditTester.audit_default_settings)

    def audit_table_extra_parameter_test(self):
        self.verify_table(audit_settings={'audit': 'table',
                                          'audit_categories': 'ADMIN,AUTH,QUERY,DML,DDL,DCL',
                                          'audit_keyspaces': 'ks',
                                          'extra_parameter': 'new'})

    def audit_table_audit_keyspaces_empty_test(self):
        self.verify_table(audit_settings={'audit': 'table',
                                          'audit_categories': 'ADMIN,AUTH,QUERY,DML,DDL,DCL',
                                          'audit_keyspaces': '',
                                          'audit_tables': 'ks.test1, ks.test2'})

    def audit_table_no_ks_test(self):
        self.verify_table(audit_settings={'audit': 'table',
                                          'audit_categories': 'ADMIN,AUTH,QUERY,DML,DDL,DCL',
                                          'audit_tables': 'ks.test2, ks.test1'})

    def audit_categories_part1_test(self):
        self.verify_table(audit_settings={'audit': 'table',
                                          'audit_categories': 'AUTH,QUERY,DDL',
                                          'audit_tables': 'ks.test2, ks.test1'})

    def audit_categories_part2_test(self):
        self.verify_table(audit_settings={'audit': 'table',
                                          'audit_categories': 'DDL, ADMIN,AUTH,DCL',
                                          'audit_keyspaces': 'ks'})

    def audit_categories_part3_test(self):
        self.verify_table(audit_settings={'audit': 'table',
                                          'audit_categories': 'DDL, ADMIN,AUTH',
                                          'audit_keyspaces': 'ks'})

    def user_password_masking_test(self):
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

    def role_password_masking_test(self):
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

    def login_test(self):
        """
        USER LOGIN
        """
        session = self.prepare(user='cassandra', password='cassandra', create_keyspace=False)
        self.assertLastAuditRow(session, "AUTH", "LOGIN", ks="", user="cassandra", cl="")

    def categories_test(self):
        """
        Test filtering audit categories
        """
        session = self.prepare(experimental=True, audit_settings={'audit': 'table', 'audit_categories': 'DML',
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

    def prepare_test(self):
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

    def permissions_test(self):
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
            assert False
        except Exception as e:
            print(e)
        self.assertLastAuditRow(session, "DML", "INSERT INTO ks.test1 (k, v1) VALUES (2, 2)", table="test1",
                                user="test", error=True, match=False)

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
        res = session.execute("SELECT * FROM audit.audit_log")
        res_list = rows_to_list(res)

        assert len(res_list) > 3

        self.assertAuditRow(res_list[-4], "DML",
                            "INSERT INTO test8 (userid, password, name) VALUES (user2, ch@ngem3b, second user)",
                            "test8", cl="QUORUM")
        self.assertAuditRow(res_list[-3], "DML", "UPDATE test8 SET password = ps22dhds WHERE userid = user3", "test8",
                            cl="QUORUM")
        self.assertAuditRow(res_list[-2], "DML", "INSERT INTO test8 (userid, password) VALUES (user4, ch@ngem3c)",
                            "test8", cl="QUORUM")
        self.assertAuditRow(res_list[-1], "DML", "DELETE name FROM test8 WHERE userid = user1", "test8", cl="QUORUM")
