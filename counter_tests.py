import random
import time
import uuid
import os
import sys
import shutil
import re
from concurrent.futures import ThreadPoolExecutor

from dtest import Tester, debug
from cassandra import ConsistencyLevel, InvalidRequest, Unauthorized
from cassandra.query import SimpleStatement
from cassandra.query import UNSET_VALUE
from cassandra.protocol import ConfigurationException

from assertions import assert_invalid, assert_one
from tools import rows_to_list, since, require, new_node
from nose.plugins.attrib import attr


@attr('dtest-full')
class TestCounters(Tester):

    def simple_increment_test(self):
        """ Simple incrementation test (Created for #3465, that wasn't a bug) """
        cluster = self.cluster
        cluster.set_configuration_options(values={'cache_hit_rate_read_balancing': False})

        cluster.populate(3).start()
        nodes = cluster.nodelist()

        session = self.patient_cql_connection(nodes[0])
        self.create_ks(session, 'ks', 3)
        self.create_cf(session, 'cf', validation="CounterColumnType", columns={'c': 'counter'})

        sessions = [self.patient_cql_connection(node, 'ks') for node in nodes]
        nb_increment = 50
        nb_counter = 10

        for i in range(0, nb_increment):
            for c in range(0, nb_counter):
                session = sessions[(i + c) % len(nodes)]
                query = SimpleStatement("UPDATE cf SET c = c + 1 WHERE key = 'counter%i'" % c,
                                        consistency_level=ConsistencyLevel.QUORUM)
                session.execute(query)

            session = sessions[i % len(nodes)]
            keys = ",".join(["'counter%i'" % c for c in range(0, nb_counter)])
            query = SimpleStatement("SELECT key, c FROM cf WHERE key IN (%s)" % keys,
                                    consistency_level=ConsistencyLevel.QUORUM)
            res = list(session.execute(query))

            assert len(res) == nb_counter
            for c in range(0, nb_counter):
                assert len(res[c]) == 2, "Expecting key and counter for counter%i, got %s" % (c, str(res[c]))
                assert res[c][1] == i + 1, "Expecting counter%i = %i, got %i" % (c, i + 1, res[c][1])

    def upgrade_test(self):
        """ Test for bug of #4436 """

        cluster = self.cluster

        cluster.set_configuration_options(values={'cache_hit_rate_read_balancing': False})

        cluster.populate(2).start()
        nodes = cluster.nodelist()

        session = self.patient_cql_connection(nodes[0])
        self.create_ks(session, 'ks', 2)

        query = """
            CREATE TABLE counterTable (
                k int PRIMARY KEY,
                c counter
            )
        """
        query = query + "WITH compression = { 'sstable_compression' : 'SnappyCompressor' }"

        session.execute(query)
        time.sleep(2)

        keys = range(0, 4)
        updates = 50

        def make_updates():
            session = self.patient_cql_connection(nodes[0], keyspace='ks')
            upd = "UPDATE counterTable SET c = c + 1 WHERE k = %d;"
            batch = " ".join(["BEGIN COUNTER BATCH"] + [upd % x for x in keys] + ["APPLY BATCH;"])

            for i in range(0, updates):
                query = SimpleStatement(batch, consistency_level=ConsistencyLevel.QUORUM)
                session.execute(query)

        def check(i):
            session = self.patient_cql_connection(nodes[0], keyspace='ks')
            query = SimpleStatement("SELECT * FROM counterTable", consistency_level=ConsistencyLevel.QUORUM)
            rows = list(session.execute(query))

            assert len(rows) == len(keys), "Expected %d rows, got %d: %s" % (len(keys), len(rows), str(rows))
            for row in rows:
                assert row[1] == i * updates, "Unexpected value %s" % str(row)

        def rolling_restart():
            # Rolling restart
            for i in range(0, 2):
                time.sleep(.2)
                nodes[i].nodetool("drain")
                nodes[i].stop(wait_other_notice=False)
                nodes[i].start(wait_other_notice=True, wait_for_binary_proto=True)
                time.sleep(.2)

        make_updates()
        check(1)
        rolling_restart()

        make_updates()
        check(2)
        rolling_restart()

        make_updates()
        check(3)
        rolling_restart()

        check(3)

    def counter_consistency_test(self):
        """
        Do a bunch of writes with ONE, read back with ALL and check results.
        """
        cluster = self.cluster
        cluster.set_configuration_options(values={'cache_hit_rate_read_balancing': False})

        cluster.populate(3).start()
        node1, node2, node3 = cluster.nodelist()
        session = self.patient_cql_connection(node1)
        self.create_ks(session, 'counter_tests', 3)

        stmt = """
              CREATE TABLE counter_table (
              id uuid PRIMARY KEY,
              counter_one COUNTER,
              counter_two COUNTER,
              )
           """
        session.execute(stmt)

        counters = []
        # establish 50 counters (2x25 rows)
        for i in range(25):
            _id = str(uuid.uuid4())
            counters.append(
                {_id: {'counter_one': 1, 'counter_two': 1}}
            )

            query = SimpleStatement("""
                UPDATE counter_table
                SET counter_one = counter_one + 1, counter_two = counter_two + 1
                where id = {uuid}""".format(uuid=_id), consistency_level=ConsistencyLevel.ONE)
            session.execute(query)

        # increment a bunch of counters with CL.ONE
        for i in range(10000):
            counter = counters[random.randint(0, len(counters) - 1)]
            counter_id = list(counter.keys())[0]

            query = SimpleStatement("""
                UPDATE counter_table
                SET counter_one = counter_one + 2
                where id = {uuid}""".format(uuid=counter_id), consistency_level=ConsistencyLevel.ONE)
            session.execute(query)

            query = SimpleStatement("""
                UPDATE counter_table
                SET counter_two = counter_two + 10
                where id = {uuid}""".format(uuid=counter_id), consistency_level=ConsistencyLevel.ONE)
            session.execute(query)

            query = SimpleStatement("""
                UPDATE counter_table
                SET counter_one = counter_one - 1
                where id = {uuid}""".format(uuid=counter_id), consistency_level=ConsistencyLevel.ONE)
            session.execute(query)

            query = SimpleStatement("""
                UPDATE counter_table
                SET counter_two = counter_two - 5
                where id = {uuid}""".format(uuid=counter_id), consistency_level=ConsistencyLevel.ONE)
            session.execute(query)

            # update expectations to match (assumed) db state
            counter[counter_id]['counter_one'] += 1
            counter[counter_id]['counter_two'] += 5

        # let's verify the counts are correct, using CL.ALL
        for counter_dict in counters:
            counter_id = list(counter_dict.keys())[0]

            query = SimpleStatement("""
                SELECT counter_one, counter_two
                FROM counter_table WHERE id = {uuid}
                """.format(uuid=counter_id), consistency_level=ConsistencyLevel.ALL)
            rows = list(session.execute(query))

            counter_one_actual, counter_two_actual = rows[0]

            self.assertEqual(counter_one_actual, counter_dict[counter_id]['counter_one'])
            self.assertEqual(counter_two_actual, counter_dict[counter_id]['counter_two'])

    @attr('next-gating')
    @attr('dtest-debug')
    def multi_counter_update_test(self):
        """
        Test for singlular update statements that will affect multiple counters.
        """
        cluster = self.cluster

        cluster.set_configuration_options(values={'cache_hit_rate_read_balancing': False})

        cluster.populate(3).start()
        node1, node2, node3 = cluster.nodelist()
        session = self.patient_cql_connection(node1)
        self.create_ks(session, 'counter_tests', 3)

        session.execute("""
            CREATE TABLE counter_table (
            id text,
            myuuid uuid,
            counter_one COUNTER,
            PRIMARY KEY (id, myuuid))
            """)

        expected_counts = {}

        # set up expectations
        for i in range(1, 6):
            _id = uuid.uuid4()

            expected_counts[_id] = i

        for k, v in expected_counts.items():
            session.execute("""
                UPDATE counter_table set counter_one = counter_one + {v}
                WHERE id='foo' and myuuid = {k}
                """.format(k=k, v=v))

        for k, v in expected_counts.items():
            count = list(session.execute("""
                SELECT counter_one FROM counter_table
                WHERE id = 'foo' and myuuid = {k}
                """.format(k=k)))

            assert len(count) and len(count[0]), "Expected counter_one={} for myuuid={}, got: {}".format(v, k, count)
            self.assertEqual(v, count[0][0])

    @attr('single_node')
    def validate_empty_column_name_test(self):
        cluster = self.cluster
        cluster.set_configuration_options(values={'cache_hit_rate_read_balancing': False})

        cluster.populate(1).start()
        node1 = cluster.nodelist()[0]
        session = self.patient_cql_connection(node1)
        self.create_ks(session, 'counter_tests', 1)

        session.execute("""
            CREATE TABLE compact_counter_table (
                pk int,
                ck text,
                value counter,
                PRIMARY KEY (pk, ck))
            WITH COMPACT STORAGE
            """)

        assert_invalid(session, "UPDATE compact_counter_table SET value = value + 1 WHERE pk = 0 AND ck = ''")
        assert_invalid(session, "UPDATE compact_counter_table SET value = value - 1 WHERE pk = 0 AND ck = ''")

        session.execute("UPDATE compact_counter_table SET value = value + 5 WHERE pk = 0 AND ck = 'ck'")
        session.execute("UPDATE compact_counter_table SET value = value - 2 WHERE pk = 0 AND ck = 'ck'")

        assert_one(session, "SELECT pk, ck, value FROM compact_counter_table", [0, 'ck', 3])

    @since('2.0')
    @attr('single_node')
    def drop_counter_column_test(self):
        """Test for CASSANDRA-7831"""
        cluster = self.cluster

        cluster.set_configuration_options(values={'cache_hit_rate_read_balancing': False})

        cluster.populate(1).start()
        node1, = cluster.nodelist()
        session = self.patient_cql_connection(node1)
        self.create_ks(session, 'counter_tests', 1)

        session.execute("CREATE TABLE counter_bug (t int, c counter, primary key(t))")

        session.execute("UPDATE counter_bug SET c = c + 1 where t = 1")
        row = list(session.execute("SELECT * from counter_bug"))

        self.assertEqual(rows_to_list(row)[0], [1, 1])
        self.assertEqual(len(row), 1)

        session.execute("ALTER TABLE counter_bug drop c")

        assert_invalid(session, "ALTER TABLE counter_bug add c counter",
                       "Cannot re-add previously dropped counter column c")

    def increment_counters_in_threads_test(self):
        """
        3 nodes in test
        increment 2 counters * 200 threads * 500 times
        expected result: counters equal 100000(500*200)
        """
        cluster = self.cluster
        cluster.set_configuration_options(values={'cache_hit_rate_read_balancing': False})

        cluster.populate(3).start()
        nodes = cluster.nodelist()

        session = self.patient_cql_connection(nodes[0])
        self.create_ks(session, 'ks', 3)
        self.create_cf(session, 'cf', validation="CounterColumnType", columns={'c': 'counter'})

        sessions = [self.patient_cql_connection(node, 'ks') for node in nodes]
        nb_increment = 500
        nb_counter = 2

        def run(connection):
            for i in range(0, nb_increment):
                for c in range(0, nb_counter):
                    query = SimpleStatement("UPDATE cf SET c = c + 1 WHERE key = 'counter%i'" % c,
                                            consistency_level=ConsistencyLevel.QUORUM)
                    connection.execute(query)

        threads = []
        num_threads = 200
        executor = ThreadPoolExecutor(max_workers=num_threads)
        for x in range(num_threads):
            conn = sessions[x % len(nodes)]
            threads.append(executor.submit(run, conn))
        for t in threads:
            t.result()

        conn = sessions[1 % len(nodes)]
        keys = ",".join(["'counter%i'" % c for c in range(0, nb_counter)])
        query = SimpleStatement("SELECT key, c FROM cf WHERE key IN (%s)" % keys,
                                consistency_level=ConsistencyLevel.QUORUM)
        res = list(conn.execute(query))
        expected_counters = nb_increment * num_threads

        assert len(res) == nb_counter
        for c in range(0, nb_counter):
            assert len(res[c]) == 2, "Expecting key and counter for counter%i, got %s" % (
                c, str(res[c]))
            assert res[c][1] == expected_counters, "Expecting counter%i = %i, got %i" % (
                c, expected_counters, res[c][1])

    def increment_decrement_counters_in_threads_test(self):
        """
        3 nodes in test
        2 counters:
        increment 500 times * 400 threads and
        decrement 500 times * 200 threads in parallel
        expected result: counters equal 100000(500*200)
        """
        cluster = self.cluster

        cluster.set_configuration_options(values={'cache_hit_rate_read_balancing': False})

        cluster.populate(3).start()
        nodes = cluster.nodelist()

        session = self.patient_cql_connection(nodes[0])
        self.create_ks(session, 'ks', 3)
        self.create_cf(session, 'cf', validation="CounterColumnType", columns={'c': 'counter'})

        sessions = [self.patient_cql_connection(node, 'ks') for node in nodes]
        nb_increment = 500
        nb_counter = 2

        def run(connection, decrement):
            for i in range(0, nb_increment):
                for c in range(0, nb_counter):
                    if decrement:
                        query = SimpleStatement("UPDATE cf SET c = c - 1 WHERE key = 'counter%i'" % c,
                                                consistency_level=ConsistencyLevel.QUORUM)
                    else:
                        query = SimpleStatement("UPDATE cf SET c = c + 1 WHERE key = 'counter%i'" % c,
                                                consistency_level=ConsistencyLevel.QUORUM)
                    connection.execute(query)

        threads = []
        num_threads = 600
        executor = ThreadPoolExecutor(max_workers=num_threads)
        for x in range(num_threads):
            conn = sessions[x % len(nodes)]
            decrement = (x % len(nodes)) == 0
            threads.append(executor.submit(run, conn, decrement))
        for t in threads:
            t.result()

        conn = sessions[1 % len(nodes)]
        keys = ",".join(["'counter%i'" % c for c in range(0, nb_counter)])
        query = SimpleStatement("SELECT key, c FROM cf WHERE key IN (%s)" % keys,
                                consistency_level=ConsistencyLevel.QUORUM)
        res = list(conn.execute(query))
        expected_counters = nb_increment * num_threads // 3

        assert len(res) == nb_counter
        for c in range(0, nb_counter):
            assert len(res[c]) == 2, "Expecting key and counter for counter%i, got %s" % (
                c, str(res[c]))
            assert res[c][1] == expected_counters, "Expecting counter%i = %i, got %i" % (
                c, expected_counters, res[c][1])

    @attr('single_node')
    def update_counter_with_ttl_and_timestamp_negative_test(self):
        """
        Try to update counter column using TTL/TIMESTAMP option
        Result: should be rejected
        """
        cluster = self.cluster
        cluster.set_configuration_options(values={'cache_hit_rate_read_balancing': False})
        cluster.populate(1).start()
        session = self.patient_cql_connection(cluster.nodelist()[0])
        self.create_ks(session, 'Test', 1)
        session.execute("CREATE TABLE counters (t int PRIMARY KEY, c counter)")

        for option in ('TTL 5', 'TIMESTAMP 11223344'):
            try:
                session.execute("UPDATE counters USING {} SET c = c + 1 where t = 1".format(option))
            except InvalidRequest as ex:
                debug('Got an expected error trying to use {}: {}'.format(option.split()[0], ex))
            else:
                raise Exception('USING {} was not rejected!'.format(option.split()[0]))

    @attr('single_node')
    def prepare_statement_test(self):
        """
        update counters with prepare statement, and verify the data
        """
        cluster = self.cluster

        cluster.set_configuration_options(values={'cache_hit_rate_read_balancing': False})

        cluster.populate(1).start()
        node1, = cluster.nodelist()
        session = self.patient_cql_connection(node1)
        self.create_ks(session, 'counter_tests', 1)

        session.execute("CREATE TABLE counter_bug (t int, c counter, primary key(t))")

        debug('Created counter table, try to update one counter')
        session.execute("UPDATE counter_bug SET c = c + 1 where t = 0")
        res = session.execute("SELECT * from counter_bug")
        rows = rows_to_list(res)
        assert len(rows) == 1
        assert rows == [[0, 1]]
        # reset the counter (key=0) to 0
        session.execute("UPDATE counter_bug SET c = c - 1 where t = 0")

        keys_num = 1000

        counter_list = []
        debug('Update %s counters with random int by prepare statement' % keys_num)
        for key in range(keys_num):
            statement = session.prepare("update counter_tests.counter_bug set c = c + ? where t = ?")
            # int is from `-sys.maxsize - 1` to `sys.maxsize`, we will reupdate
            # counters with random int, so sys.maxsize // 2 is safe to avoid rollover
            rand_c = random.randint(-sys.maxsize // 2, sys.maxsize // 2)
            counter_list.append([key, rand_c])
            session.execute(statement.bind((rand_c, key)))
        res = session.execute("SELECT * from counter_bug")
        rows = rows_to_list(res)
        for row in rows:
            assert row in counter_list, "Counter isn't updated correctly"
        assert len(rows) == keys_num
        debug('Verified that all counters are updated correctly')

        debug('Reupdate all counters')
        for key in range(keys_num):
            statement = session.prepare("update counter_tests.counter_bug set c = c + ? where t = ?")
            rand_c = random.randint(-sys.maxsize // 2, sys.maxsize // 2)
            session.execute(statement.bind((rand_c, key)))
        res = session.execute("SELECT * from counter_bug")
        rows = rows_to_list(res)
        assert len(rows) == keys_num
        debug('Verified that counters number is correct: %s' % keys_num)

        debug('drop all counters')
        for key in range(keys_num):
            session.execute("DELETE c FROM counter_tests.counter_bug where t = %s" % key)
        res = session.execute("SELECT * from counter_bug")
        rows = rows_to_list(res)
        assert len(rows) == 0

    @attr('single_node')
    def int_rollover_test(self):
        """
        currently the counter will rollover when it reaches to MAX_INT.
        https://github.com/scylladb/scylla/issues/2225 (WONTFIX)
        Expected result: rollover
        """
        cluster = self.cluster
        cluster.set_configuration_options(values={'cache_hit_rate_read_balancing': False})

        cluster.populate(1).start()
        node1, = cluster.nodelist()
        session = self.patient_cql_connection(node1)
        self.create_ks(session, 'counter_tests', 1)

        session.execute("CREATE TABLE counter_bug (t int, c counter, primary key(t))")

        debug('Created counter table, try to update one counter to MAX_INT')
        session.execute("UPDATE counter_bug SET c = c + %s where t = 0" % sys.maxsize)
        res = session.execute("SELECT * from counter_bug")
        rows = rows_to_list(res)
        assert len(rows) == 1
        debug(rows)
        assert rows == [[0, sys.maxsize]], 'Failed to update counter to MAX_INT'

        debug('Update the counter to make it rollover')
        session.execute("UPDATE counter_bug SET c = c + 1 where t = 0")
        res = session.execute("SELECT * from counter_bug")
        rows = rows_to_list(res)
        assert len(rows) == 1
        debug(rows)
        assert rows == [[0, -sys.maxsize - 1]], "Int counter isn't rollover"

        debug('Update the counter to make it recover')
        session.execute("UPDATE counter_bug SET c = c - 1 where t = 0")
        res = session.execute("SELECT * from counter_bug")
        rows = rows_to_list(res)
        assert len(rows) == 1
        debug(rows)
        assert rows == [[0, sys.maxsize]], "Int counter isn't recovered"

    @attr('single_node')
    def prepare_unset_value_test(self):
        """
        Try to update counter with UNSET_VALUE
        Expected result: nothing is changed
        """
        cluster = self.cluster

        cluster.set_configuration_options(values={'cache_hit_rate_read_balancing': False})

        cluster.populate(1).start()
        node1, = cluster.nodelist()
        # protocol version >= 4
        session = self.patient_cql_connection(node1, protocol_version=4)
        self.create_ks(session, 'counter_tests', 1)

        session.execute("CREATE TABLE counter_bug (t int, c counter, primary key(t))")

        debug('Created counter table, try to update one counter')
        session.execute("UPDATE counter_bug SET c = c + 1 where t = 0")
        res = session.execute("SELECT * from counter_bug")
        rows = rows_to_list(res)
        assert len(rows) == 1
        assert rows == [[0, 1]]

        keys_num = 1000
        debug('Update %s counters with UNSET_VALUE by prepare statement' % keys_num)

        for key in range(keys_num):
            statement = session.prepare("update counter_tests.counter_bug set c = c + ? where t = ?")
            session.execute(statement.bind((UNSET_VALUE, key)))

        res = session.execute("SELECT * from counter_bug")
        rows = rows_to_list(res)
        debug(rows)
        assert len(rows) == 1, 'Update with UNSET_VALUE unexpectedly changed number of counters'
        assert rows == [[0, 1]], 'Update with UNSET_VALUE unexpectedly changed value of first counter'
        debug("Verified that all counters aren't updated by UNSET_VALUE")

    def assertUnauthorized(self, message, session, query):
        with self.assertRaises(Unauthorized) as cm:
            session.execute(query)
        assert re.search(message, str(cm.exception)), "Expected '%s', but got '%s'" % (message, cm.exception.message)

    @attr('single_node')
    def static_counter_column_test(self):
        """
        Test of static counter column
        """
        cluster = self.cluster
        cluster.set_configuration_options(values={'cache_hit_rate_read_balancing': False})
        cluster.populate(1).start()
        node1 = cluster.nodelist()[0]
        session = self.patient_cql_connection(node1)
        self.create_ks(session, 'Test', 1)
        session.execute("CREATE TABLE Test.cf (pk int, ck int, s counter static, v counter, primary key (pk, ck))")

        debug("Update counters")
        pk = 10
        incr = 1
        for i in range(1, 101):
            session.execute("UPDATE Test.cf SET s=s+{}, v=v+{} where pk = {} and ck = {}".format(incr, i + 1, pk, i))

        debug('Verify counter data')
        res = session.execute("SELECT * FROM Test.cf;")
        self.assertEquals(len(rows_to_list(res)), 100)
        res = session.execute("SELECT s,v FROM Test.cf;")
        rows = sorted(rows_to_list(res), key=lambda x: (x and x[0] is not None, x))
        for i in range(1, 101):
            self.assertEquals(rows[i - 1][0], 100)
            self.assertEquals(rows[i - 1][1], i + 1)

        debug("Update static counter column")
        incr = 10
        for i in range(1, 11):
            # update static counter with two methods, they have same effect
            if i % 2 == 0:
                # update all items of same pk
                session.execute("UPDATE Test.cf SET s=s+{} where pk = {}".format(incr, pk))
            else:
                # only update one item that is assigned by pk + ck
                session.execute("UPDATE Test.cf SET s=s+{}, v=v+{} where pk = {} and ck = {}".format(incr, 0, pk, i))

        debug('Verify counter data')
        res = session.execute("SELECT s,v FROM Test.cf;")
        rows = sorted(rows_to_list(res), key=lambda x: (x and x[0] is not None, x))
        self.assertEquals(len(rows), 100)
        for i in range(1, 101):
            self.assertEquals(rows[i - 1][0], 200)
            self.assertEquals(rows[i - 1][1], i + 1)

    def compact_counter_cluster_test(self):
        """
        @jira_ticket CASSANDRA-12219
        """
        cluster = self.cluster

        cluster.set_configuration_options(values={'cache_hit_rate_read_balancing': False})
        cluster.populate(3).start()
        node1 = cluster.nodelist()[0]
        session = self.patient_cql_connection(node1)
        self.create_ks(session, 'counter_tests', 1)

        session.execute("""
            CREATE TABLE IF NOT EXISTS counter_cs (
                key bigint PRIMARY KEY,
                data counter
            ) WITH COMPACT STORAGE
            """)

        for outer in range(0, 5):
            for idx in range(0, 5):
                session.execute("UPDATE counter_cs SET data = data + 1 WHERE key = {k}".format(k=idx))

        for idx in range(0, 5):
            row = list(session.execute("SELECT data from counter_cs where key = {k}".format(k=idx)))
            self.assertEqual(rows_to_list(row)[0][0], 5)

    @attr('single_node')
    def alter_non_counter_with_counter_test(self):
        """
        ALTER table with counter, should fail with configuration error
        and shouldn't crash

        Reproducer for:
        https://github.com/scylladb/scylla/issues/7065

        Fix:
        https://github.com/scylladb/scylla/commit/1c29f0a43d00028d728068e9e194e00ca5ec7b67
        """
        cluster = self.cluster

        cluster.populate(1).start()
        node1 = cluster.nodelist()[0]
        session = self.patient_cql_connection(node1)
        self.create_ks(session, 'counter_tests', 1)

        session.execute("""
            CREATE TABLE non_counter (
                a text,
                b text,
                PRIMARY KEY (a, b))
                WITH CLUSTERING ORDER BY (b ASC);
            """)
        try:
            session.execute("""
                ALTER TABLE non_counter ADD "c" counter;
            """)
        except ConfigurationException as exc:
            self.assertIn("Cannot add a counter column (c) in a non counter column family", str(exc))


@attr('dtest-full')
class TestCountersOnMultipleNodes(Tester):

    def __init__(self, *argv, **kwargs):
        kwargs['cluster_options'] = {'start_rpc': 'true'}
        super(TestCountersOnMultipleNodes, self).__init__(*argv, **kwargs)
        self._start_row = 2
        self._row_cnt = 1000
        self._extra_row_cnt = 0

    def setUp(self):
        super(TestCountersOnMultipleNodes, self).setUp()
        debug("Starting cluster with 3 nodes.")
        cluster = self.cluster
        cluster.set_configuration_options(values={'hinted_handoff_enabled': False})
        cluster.set_configuration_options(values={'cache_hit_rate_read_balancing': False})
        cluster.populate(3).start(wait_other_notice=True, wait_for_binary_proto=True)
        self.node1, self.node2, self.node3 = cluster.nodelist()

    def tearDown(self):
        row_cnt = self._row_cnt - self._start_row + self._extra_row_cnt
        self._verify_data(row_cnt)
        debug("Update counter data")
        session = self.patient_cql_connection(self.node1)
        for i in range(self._start_row, self._row_cnt):
            session.execute("UPDATE Test.cf SET cnt = cnt - 1 WHERE pk = {};".format(i))
            session.execute("UPDATE Test.cf SET cnt = cnt + 1 WHERE pk = {};".format(i))

        self._verify_data(row_cnt)
        super(TestCountersOnMultipleNodes, self).tearDown()

    def _populate_data(self, rf=2):
        session = self.patient_cql_connection(self.node1)
        self.create_ks(session, 'Test', rf)

        session.execute("""
                    CREATE TABLE cf (
                        pk INT PRIMARY KEY,
                        cnt COUNTER
                    ) WITH read_repair_chance=0.0;
                """)

        debug("Update counter data")
        for i in range(self._start_row, self._row_cnt):
            session.execute(SimpleStatement("UPDATE Test.cf SET cnt = cnt + {} WHERE pk = {};".format(i,
                                                                                                      i), consistency_level=ConsistencyLevel.ALL))
            session.execute("UPDATE Test.cf SET cnt = cnt - 1 WHERE pk = {};".format(i))
            session.execute("UPDATE Test.cf SET cnt = cnt + 1 WHERE pk = {};".format(i))

        self._verify_data(self._row_cnt - self._start_row)

    def _verify_data(self, expected_row_count):
        debug('Verify counter data')
        session = self.patient_cql_connection(self.node1)
        res = session.execute("SELECT * FROM Test.cf;")
        rows = rows_to_list(res)
        self.assertEquals(len(rows), expected_row_count)
        for row in rows:
            self.assertEquals(row[0], row[1])

    def _verify_data_repair(self, expected_row_count):
        for node in (self.node1, self.node2):
            node.stop(wait_other_notice=True)

        session = self.patient_cql_connection(self.node3)
        pk_list = ','.join([str(i) for i in range(self._row_cnt, self._row_cnt + self._extra_row_cnt)])
        query = SimpleStatement("SELECT * FROM Test.cf WHERE pk IN ({});".format(pk_list),
                                consistency_level=ConsistencyLevel.ONE)
        res = session.execute(query)
        rows = rows_to_list(res)
        self.assertEquals(len(rows), expected_row_count)

        for node in (self.node1, self.node2):
            node.start(wait_other_notice=True, wait_for_binary_proto=True)

    def _verify_data_rebuild(self):
        for node in (self.node1, self.node2):
            node.stop(wait_other_notice=True)

        debug('Verify counter data on node3')
        session = self.patient_cql_connection(self.node3)
        query = SimpleStatement("SELECT * FROM Test.cf;", consistency_level=ConsistencyLevel.ONE)
        res = session.execute(query)
        rows = rows_to_list(res)
        self.assertEquals(len(rows), self._row_cnt - self._start_row)
        for row in rows:
            self.assertEquals(row[0], row[1])

        for node in (self.node1, self.node2):
            node.start(wait_other_notice=True)

    def counter_consistency_node_replace_test(self):
        """
        Cluster: 3 nodes, keyspace RF=2
        Populate counters data, replace one of the nodes by a new one
        Result: counters data stays consistent
        """
        self._populate_data()
        debug('Stop node3 and create new node to replace it')
        self.node3.stop(gently=True, wait_other_notice=True)
        node4 = new_node(self.cluster, bootstrap=True, token=None, remote_debug_port='0')
        debug('Start the new node')
        node4.start(replace_address=self.cluster.get_node_ip(3), wait_for_binary_proto=True)

    def counter_consistency_node_remove_test(self):
        """
        Cluster: 3 nodes, keyspace RF=2
        Populate counters data, remove one of the nodes
        Result: counters data stays consistent
        """
        self._populate_data()
        debug('Stop and remove node2')
        node2_hostid = self.node2.hostid()
        self.node2.stop(wait_other_notice=True)
        self.node1.nodetool("removenode %s" % node2_hostid)

    @attr('dtest-debug')
    def counter_consistency_node_add_test(self):
        """
        Cluster: 3 nodes, keyspace RF=2
        Populate counters data, add a new node
        Result: counters data stays consistent
        """
        self._populate_data()
        debug('Add a new node')
        node4 = new_node(self.cluster, bootstrap=True, token=None, remote_debug_port='0')
        node4.start(wait_for_binary_proto=True)

    def counter_consistency_node_decommission_test(self):
        """
        Cluster: 3 nodes, keyspace RF=1
        Populate counters data, decommission one of the nodes
        Result: counters data stays consistent
        """
        self._populate_data(rf=1)
        debug('Decommission node2')
        self.node2.decommission()
        self.node2.stop()

    @attr('next-gating')
    def counter_consistency_node_repair_test(self):
        """
        Cluster: 3 nodes, keyspace RF=3
        Populate counters data, stop one of the nodes, change counter data
        Then start and repair the stopped node
        Result: counters data stays consistent
        """
        self._extra_row_cnt = 10
        self._populate_data(rf=3)
        debug('Stop node3')
        self.node3.flush()
        self.node3.stop(wait_other_notice=True)

        debug("Update counter data")
        session = self.patient_cql_connection(self.node1)
        for i in range(self._row_cnt, self._row_cnt + self._extra_row_cnt):
            query = SimpleStatement("UPDATE Test.cf SET cnt = cnt + {} WHERE pk = {};".format(i, i),
                                    consistency_level=ConsistencyLevel.TWO)
            session.execute(query)

        debug('Start node3 and verify new data is not present')
        self.node3.start(wait_other_notice=True, wait_for_binary_proto=True)
        self._verify_data_repair(0)
        debug('Repair node3')
        self.node3.repair()
        debug('Verify new data is present on node3')
        self._verify_data_repair(self._extra_row_cnt)

    def counter_consistency_node_rebuild_test(self):
        """
        Cluster: 3 nodes, keyspace RF=3
        Populate counters data, stop one of the nodes and remove sstables and commit log for it
        Then start the node and rebuild it
        Result: counters data stays consistent
        """
        self._populate_data(rf=3)
        debug('Stop node3')
        self.node3.flush()
        self.node3.stop(wait_other_notice=True)

        debug('Remove sstables and commit log for node3')
        # We should keep the system tables and delete user tables
        for dir_name in ('commitlogs', 'data/test'):
            data_dir = os.path.join(self.node3.get_path(), dir_name)
            debug("Removing {}".format(data_dir))
            shutil.rmtree(data_dir)

        debug('Start node3 and rebuild it')
        self.node3.start(wait_other_notice=True, wait_for_binary_proto=True)
        self.node3.nodetool('rebuild')
        self._verify_data_rebuild()


@attr('dtest-full')
class TestCountersStress(Tester):

    def __init__(self, *argv, **kwargs):
        super(TestCountersStress, self).__init__(*argv, **kwargs)

    def setUp(self):
        super(TestCountersStress, self).setUp()
        cluster = self.cluster

        cluster.set_configuration_options(values={'cache_hit_rate_read_balancing': False})
        cluster.populate(3).start(wait_other_notice=True, wait_for_binary_proto=True)
        self.node = cluster.nodelist()[0]
        self._op_cnt = 100000
        if hasattr(self.cluster, 'scylla_mode') and self.cluster.scylla_mode == 'debug':
            self._op_cnt //= 10

    def counter_stress_test(self):
        """
        Run cassandra stress test with multiple concurrent updates/reads of counters
        Result: written/read count is as expected
        """
        session = self.patient_cql_connection(self.node)
        session.execute("""
            CREATE KEYSPACE keyspace1
            WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '2'} AND durable_writes = true;
        """)
        session.execute("""
            CREATE TABLE keyspace1.counter1 (
                key blob PRIMARY KEY,
                "C0" counter,
                "C1" counter,
                "C2" counter,
                "C3" counter,
                "C4" counter
            ) WITH COMPACT STORAGE
                AND bloom_filter_fp_chance = 0.01
                AND caching = '{"keys":"ALL","rows_per_partition":"ALL"}'
                AND comment = ''
                AND compaction = {'class': 'SizeTieredCompactionStrategy'}
                AND compression = {}
                AND dclocal_read_repair_chance = 0.1
                AND default_time_to_live = 0
                AND gc_grace_seconds = 864000
                AND max_index_interval = 2048
                AND memtable_flush_period_in_ms = 0
                AND min_index_interval = 128
                AND read_repair_chance = 0.0
                AND speculative_retry = '99.0PERCENTILE';
        """)

        debug('Run stress counter_write')
        resp = self.node.stress_object(['counter_write', 'n={}'.format(self._op_cnt), '-rate', 'threads=4'])
        if not resp or 'total partitions:write' not in resp:
            raise Exception('Error running stress test: {}'.format(resp))
        self.assertGreaterEqual(resp['total partitions:write'], self._op_cnt)
        debug('Verifying data count')
        rows = rows_to_list(session.execute('SELECT count(*) FROM keyspace1.counter1;'))
        self.assertEqual(rows[0][0], self._op_cnt)

        debug('Run stress counter_read')
        resp = self.node.stress_object(['counter_read', 'n={}'.format(self._op_cnt), '-rate', 'threads=4'])
        if not resp or 'total partitions:read' not in resp:
            raise Exception('Error running stress test: {}'.format(resp))
        self.assertGreaterEqual(resp['total partitions:read'], self._op_cnt)

    def counter_stress_user_profile_test(self):
        """
        Run cassandra stress test updates/reads of counters with user profile
        Result: able to work with custom columns table
        """
        profile_path = os.path.join(os.path.dirname(__file__),
                                    'test_data/c-s-profiles/cassandra-stress-custom-counters-1.yaml')

        debug('Run stress update counters with user profile')
        resp = self.node.stress_object(['user', 'profile={}'.format(profile_path),
                                        'ops(insert=1)', 'n={}'.format(self._op_cnt), '-rate', 'threads=4'])
        if not resp or 'total partitions' not in resp:
            raise Exception('Error running stress test: {}'.format(resp))
        self.assertGreaterEqual(resp['total partitions'], self._op_cnt)
        session = self.patient_cql_connection(self.node)
        debug('Verifying data count')
        rows = rows_to_list(session.execute('SELECT count(*) FROM ks.counter_cf;'))
        self.assertEqual(rows[0][0], self._op_cnt)

        debug('Run stress read counters with user profile')
        resp = self.node.stress_object(['user', 'profile={}'.format(profile_path), 'ops(read1=1)',
                                        'n={}'.format(self._op_cnt), '-rate', 'threads=4'])
        if not resp or 'total partitions' not in resp:
            raise Exception('Error running stress test: {}'.format(resp))
        self.assertGreaterEqual(resp['total partitions'], self._op_cnt)
