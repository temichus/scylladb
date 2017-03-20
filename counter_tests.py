from dtest import Tester, debug
from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement
from cassandra.cluster import NoHostAvailable

import random
import time
import uuid
import os
import threading
import shutil

from assertions import assert_invalid, assert_one
from tools import rows_to_list, since, require


class TestCounters(Tester):

    def simple_increment_test(self):
        """ Simple incrementation test (Created for #3465, that wasn't a bug) """
        cluster = self.cluster
        cluster.set_configuration_options(values={'experimental': True})

        cluster.populate(3).start()
        nodes = cluster.nodelist()

        session = self.patient_cql_connection(nodes[0])
        self.create_ks(session, 'ks', 3)
        self.create_cf(session, 'cf', validation="CounterColumnType", columns={'c': 'counter'})

        sessions = [self.patient_cql_connection(node, 'ks') for node in nodes]
        nb_increment = 50
        nb_counter = 10

        for i in xrange(0, nb_increment):
            for c in xrange(0, nb_counter):
                session = sessions[(i + c) % len(nodes)]
                query = SimpleStatement("UPDATE cf SET c = c + 1 WHERE key = 'counter%i'" % c, consistency_level=ConsistencyLevel.QUORUM)
                session.execute(query)

            session = sessions[i % len(nodes)]
            keys = ",".join(["'counter%i'" % c for c in xrange(0, nb_counter)])
            query = SimpleStatement("SELECT key, c FROM cf WHERE key IN (%s)" % keys, consistency_level=ConsistencyLevel.QUORUM)
            res = list(session.execute(query))

            assert len(res) == nb_counter
            for c in xrange(0, nb_counter):
                assert len(res[c]) == 2, "Expecting key and counter for counter%i, got %s" % (c, str(res[c]))
                assert res[c][1] == i + 1, "Expecting counter%i = %i, got %i" % (c, i + 1, res[c][1])

    def upgrade_test(self):
        """ Test for bug of #4436 """

        cluster = self.cluster
        cluster.set_configuration_options(values={'experimental': True})

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
        cluster.set_configuration_options(values={'experimental': True})

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
        for i in xrange(25):
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
        for i in xrange(10000):
            counter = counters[random.randint(0, len(counters) - 1)]
            counter_id = counter.keys()[0]

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
            counter_id = counter_dict.keys()[0]

            query = SimpleStatement("""
                SELECT counter_one, counter_two
                FROM counter_table WHERE id = {uuid}
                """.format(uuid=counter_id), consistency_level=ConsistencyLevel.ALL)
            rows = list(session.execute(query))

            counter_one_actual, counter_two_actual = rows[0]

            self.assertEqual(counter_one_actual, counter_dict[counter_id]['counter_one'])
            self.assertEqual(counter_two_actual, counter_dict[counter_id]['counter_two'])

    def multi_counter_update_test(self):
        """
        Test for singlular update statements that will affect multiple counters.
        """
        cluster = self.cluster
        cluster.set_configuration_options(values={'experimental': True})

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

            self.assertEqual(v, count[0][0])

    def validate_empty_column_name_test(self):
        cluster = self.cluster
        cluster.set_configuration_options(values={'experimental': True})

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
    def drop_counter_column_test(self):
        """Test for CASSANDRA-7831"""
        cluster = self.cluster
        cluster.set_configuration_options(values={'experimental': True})

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

        assert_invalid(session, "ALTER TABLE counter_bug add c counter", "Cannot re-add previously dropped counter column c")

    def increment_counters_in_threads_test(self):
        """
        3 nodes in test
        increment 2 counters * 200 threads * 500 times
        expected result: counters equal 100000(500*200)
        """
        cluster = self.cluster
        cluster.set_configuration_options(values={'experimental': True})

        cluster.populate(3).start()
        nodes = cluster.nodelist()

        session = self.patient_cql_connection(nodes[0])
        self.create_ks(session, 'ks', 3)
        self.create_cf(session, 'cf', validation="CounterColumnType", columns={'c': 'counter'})

        sessions = [self.patient_cql_connection(node, 'ks') for node in nodes]
        nb_increment = 500
        nb_counter = 2

        class ThreadedQuery(threading.Thread):

            def __init__(self, connection):
                threading.Thread.__init__(self)
                self.connection = connection

            def run(self):
                nb_increment = 500
                nb_counter = 2
                for i in xrange(0, nb_increment):
                    for c in xrange(0, nb_counter):
                        query = SimpleStatement("UPDATE cf SET c = c + 1 WHERE key = 'counter%i'" % c,
                                                consistency_level=ConsistencyLevel.QUORUM)
                        self.connection.execute(query)

        threads = []
        num_threads = 200
        for x in range(num_threads):
            conn = sessions[x % len(nodes)]
            threads.append(ThreadedQuery(conn))
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        conn = sessions[1 % len(nodes)]
        keys = ",".join(["'counter%i'" % c for c in xrange(0, nb_counter)])
        query = SimpleStatement("SELECT key, c FROM cf WHERE key IN (%s)" % keys,
                                consistency_level=ConsistencyLevel.QUORUM)
        res = list(conn.execute(query))
        expected_counters = nb_increment * num_threads

        assert len(res) == nb_counter
        for c in xrange(0, nb_counter):
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
        cluster.set_configuration_options(values={'experimental': True})

        cluster.populate(3).start()
        nodes = cluster.nodelist()

        session = self.patient_cql_connection(nodes[0])
        self.create_ks(session, 'ks', 3)
        self.create_cf(session, 'cf', validation="CounterColumnType", columns={'c': 'counter'})

        sessions = [self.patient_cql_connection(node, 'ks') for node in nodes]
        nb_increment = 500
        nb_counter = 2

        class ThreadedQuery(threading.Thread):

            def __init__(self, connection, decrement):
                threading.Thread.__init__(self)
                self.connection = connection
                self.decrement = decrement

            def run(self):
                nb_increment = 500
                nb_counter = 2
                for i in xrange(0, nb_increment):
                    for c in xrange(0, nb_counter):
                        if self.decrement:
                            query = SimpleStatement("UPDATE cf SET c = c - 1 WHERE key = 'counter%i'" % c,
                                                    consistency_level=ConsistencyLevel.QUORUM)
                        else:
                            query = SimpleStatement("UPDATE cf SET c = c + 1 WHERE key = 'counter%i'" % c,
                                                    consistency_level=ConsistencyLevel.QUORUM)
                        self.connection.execute(query)

        threads = []
        num_threads = 600
        for x in range(num_threads):
            conn = sessions[x % len(nodes)]
            decrement = (x % len(nodes)) == 0
            threads.append(ThreadedQuery(conn, decrement))
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        conn = sessions[1 % len(nodes)]
        keys = ",".join(["'counter%i'" % c for c in xrange(0, nb_counter)])
        query = SimpleStatement("SELECT key, c FROM cf WHERE key IN (%s)" % keys,
                                consistency_level=ConsistencyLevel.QUORUM)
        res = list(conn.execute(query))
        expected_counters = nb_increment * num_threads / 3

        assert len(res) == nb_counter
        for c in xrange(0, nb_counter):
            assert len(res[c]) == 2, "Expecting key and counter for counter%i, got %s" % (
                c, str(res[c]))
            assert res[c][1] == expected_counters, "Expecting counter%i = %i, got %i" % (
                c, expected_counters, res[c][1])


class TestCountersOnMultipleNodes(Tester):

    def __init__(self, *argv, **kwargs):
        kwargs['cluster_options'] = {'start_rpc': 'true'}
        super(TestCountersOnMultipleNodes, self).__init__(*argv, **kwargs)
        self.allow_log_errors = True
        self._start_row = 2
        self._row_cnt = 1000
        self._extra_row_cnt = 0

    def setUp(self):
        super(TestCountersOnMultipleNodes, self).setUp()
        debug("Starting cluster with 3 nodes.")
        cluster = self.cluster
        cluster.set_configuration_options(values={'experimental': True, 'hinted_handoff_enabled': False})
        cluster.populate(3).start()
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
            session.execute("UPDATE Test.cf SET cnt = cnt + {} WHERE pk = {};".format(i, i))
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
            node.start(wait_other_notice=True)

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
        for dir_name in ('commitlogs', 'data'):
            data_dir = os.path.join(self.node3.get_path(), dir_name)
            shutil.rmtree(data_dir)

        debug('Start node3 and rebuild it')
        self.node3.start(wait_other_notice=True, wait_for_binary_proto=True)
        self.node3.nodetool('rebuild')
        self._verify_data_rebuild()

