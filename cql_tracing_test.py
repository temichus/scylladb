# coding: utf-8
#
# This test is based on a Cassandra's test with the same name.
#
from dtest import Tester, debug
from scylla_tools import insert_c1c2_no_prepared
from cassandra.query import SimpleStatement
from cassandra import ConsistencyLevel
import functools
from Queue import Queue
import random
import threading
import time
import re


class TestCqlTracing(Tester):
    """
    Test that the default implementation for tracing works.
    """

    def prepare(self, create_keyspace=True, nodes=3, rf=3, protocol_version=3, jvm_args=None, **kwargs):
        if jvm_args is None:
            jvm_args = []

        cluster = self.cluster
        cluster.populate(nodes).start(wait_for_binary_proto=True, jvm_args=jvm_args)

        node1 = cluster.nodelist()[0]

        session = self.patient_cql_connection(node1, protocol_version=protocol_version)
        if create_keyspace:
            if self._preserve_cluster:
                session.execute("DROP KEYSPACE IF EXISTS ks")
            self.create_ks(session, 'ks', rf)
        return session

    def trace(self, session):
        """
        * CREATE a table
        * enable TRACING
        * SELECT on a known system table and assert it ran with tracing by checking the output
        * INSERT a row into the created system table and assert it ran with tracing
        * SELECT from the table and assert it ran with tracing

        @param session The Session object to use to create a table.
        """

        node1 = self.cluster.nodelist()[0]

        # Create
        session.execute("""
            CREATE TABLE ks.users (
                userid uuid PRIMARY KEY,
                firstname text,
                lastname text,
                age int
            );
        """)

        out, err = node1.run_cqlsh('TRACING ON', return_output=True)
        self.assertIn('Tracing is enabled', out)

        out, err = node1.run_cqlsh('TRACING ON; SELECT * from system.peers', return_output=True)
        self.assertIn('Tracing session: ', out)
        self.assertIn('Request complete ', out)

        # Inserts
        out, err = node1.run_cqlsh(
            "CONSISTENCY ALL; TRACING ON; "
            "INSERT INTO ks.users (userid, firstname, lastname, age) "
            "VALUES (550e8400-e29b-41d4-a716-446655440000, 'Frodo', 'Baggins', 32)",
            return_output=True)
        debug(out)
        self.assertIn('Tracing session: ', out)
        self.assertIn('Request complete ', out)

        # Queries
        out, err = node1.run_cqlsh('CONSISTENCY ALL; TRACING ON; '
                                   'SELECT firstname, lastname '
                                   'FROM ks.users WHERE userid = 550e8400-e29b-41d4-a716-446655440000',
                                   return_output=True)
        debug(out)
        self.assertIn('Tracing session: ', out)
        self.assertIn(' 127.0.0.1 ', out)
        self.assertIn(' 127.0.0.2 ', out)
        self.assertIn(' 127.0.0.3 ', out)
        self.assertIn('Request complete ', out)
        self.assertIn(" Frodo |  Baggins", out)

    def tracing_simple_test(self):
        """
        Test tracing using the default tracing class. See trace().
        """
        session = self.prepare()
        self.trace(session)

    def tracing_shutdown_test(self):
        """
        Check tracing functionality when Node is being shut down:
           - Check that CQL handling is stopped prior to tracing being stopped
             (otherwise there will be an assert coming from a
             cql_server::connection::process_request().
           - Check that nothing bad is going on when node is being shut done
             while a remote node requests tracing via RPC.
           - Check that tracing for all CQL requests complete prior to Node's
             shutdown are being pushed to the backend.
        """
        # Start a cluster of two nodes, and create a keyspace with RF=2.
        self.cluster.populate(2).start()
        node1, node2 = self.cluster.nodelist()

        debug("Enable tracing for all CQL requests on node1 and node2...")
        node1.nodetool('settraceprobability 1.0')
        node2.nodetool('settraceprobability 1.0')

        session = self.patient_cql_connection(node1)
        self.create_ks(session, 'ks', 2)
        self.create_cf(session, 'cf', read_repair=0.0, columns={'c1': 'text', 'c2': 'text'})

        num_keys = 500
        debug("Populating a table with {} keys...".format(num_keys))
        insert_c1c2_no_prepared(session, keys=range(num_keys), consistency=ConsistencyLevel.ONE)

        debug("Stopping node1...")
        node1.stop(wait_other_notice=True)

        debug("Checking log of node1 for assertions...")
        match = node1.grep_log("Assertion .* failed.")
        self.assertEqual(len(match), 0)

        debug("Check that all tracing session have been flushed...")
        pattern = re.compile("INSERT INTO")
        all_tracing_sessions_query = SimpleStatement('SELECT parameters FROM system_traces.sessions')
        rows = list(session.execute(all_tracing_sessions_query))
        count = functools.reduce(lambda x, y: x + y, map(lambda row: self.grep_one_line(row[0]['query'], pattern), rows))
        self.assertEqual(count, num_keys)

        debug("Start node1...")
        node1.start(wait_for_binary_proto=True)

        debug("Enable tracing for all CQL requests on node1...")
        node1.nodetool('settraceprobability 1.0')

        session = self.patient_cql_connection(node1)

        def run(name, q):
            try:
                q.put(True)
                debug("Populating a table with {} more keys...".format(30 * num_keys))
                insert_c1c2_no_prepared(session, keys=range(num_keys, num_keys + 30 * num_keys), consistency=ConsistencyLevel.ONE)
                debug("insertion of {} keys is done".format(30 * num_keys))
            except:
                debug("insertions was killed")

        queue = Queue()
        insert_thread = threading.Thread(target=run, args=("insert-thread", queue))
        insert_thread.start()
        queue.get(block=True)

        random.seed()
        wait_time = random.random()
        debug("Wait for {} seconds".format(wait_time))
        time.sleep(wait_time)

        debug("Stopping node2...")
        node2.stop(wait_other_notice=True)

        insert_thread.join()

        debug("Checking log of node2 for assertions...")
        match = node2.grep_log("Assertion .* failed.")
        self.assertEqual(len(match), 0)

# ----------------------------------------------------------------------------------------------------------------------
    def grep_one_line(self, line, pattern):
        """
        A helper function that returns 1 if a pattern is found in a given string
        and 0 otherwise. 'pattern' is expected to be a compiled re(gular expression)
        object.
        """
        line = line.strip()
        if pattern.search(line):
            return 1
        return 0

# ----------------------------------------------------------------------------------------------------------------------
#    @known_failure(failure_source='test',
#                   jira_url='https://issues.apache.org/jira/browse/CASSANDRA-11465',
#                   flaky=True)
#    @since('3.4')
#    def tracing_unknown_impl_test(self):
#        """
#        Test that Cassandra logs an error, but keeps its default tracing
#        behavior, when a nonexistent tracing class is specified.
#
#        * set a nonexistent custom tracing class
#        * run trace()
#        * if running the test on a version with custom tracing classes
#          implemented, check that an error about the nonexistent class was
#          logged.
#
#        @jira_ticket CASSANDRA-10392
#        """
#        expected_error = 'Cannot use class junk for tracing'
#        self.ignore_log_patterns = [expected_error]
#        session = self.prepare(jvm_args=['-Dcassandra.custom_tracing_class=junk'])
#        self.trace(session)
#
#        errs = self.cluster.nodelist()[0].grep_log_for_errors()
#        debug('Errors after attempted trace with unknown tracing class: {errs}'.format(errs=errs))
#        self.assertEqual(len(errs), 1)
#        self.assertEqual(len(errs[0]), 1)
#        err = errs[0][0]
#        self.assertIn(expected_error, err)
#
#    @known_failure(failure_source='test',
#                   jira_url='https://issues.apache.org/jira/browse/CASSANDRA-11465',
#                   flaky=True)
#    @since('3.4')
#    def tracing_default_impl_test(self):
#        """
#        Test that Cassandra logs an error, but keeps its default tracing
#        behavior, when the default tracing class is specified.
#
#        This doesn't work because the constructor for the default
#        implementation isn't accessible.
#
#        * set the default tracing class as a custom tracing class
#        * run trace()
#        * if running the test on a version with custom tracing classes
#          implemented, check that an error about the class was
#          logged.
#
#        @jira_ticket CASSANDRA-10392
#        """
#        expected_error = 'Cannot use class org.apache.cassandra.tracing.TracingImpl'
#        self.ignore_log_patterns = [expected_error]
#        session = self.prepare(jvm_args=['-Dcassandra.custom_tracing_class=org.apache.cassandra.tracing.TracingImpl'])
#        self.trace(session)
#
#        errs = self.cluster.nodelist()[0].grep_log_for_errors()
#        debug('Errors after attempted trace with default tracing class: {errs}'.format(errs=errs))
#        self.assertEqual(len(errs), 1)
#        self.assertEqual(len(errs[0]), 1)
#        err = errs[0][0]
#        self.assertIn(expected_error, err)
#        # make sure it logged the error for the correct reason. this isn't
#        # part of the expected error to avoid having to escape parens and
#        # periods for regexes.
#        self.assertIn("Default constructor for Tracing class "
#                      "'org.apache.cassandra.tracing.TracingImpl' is inaccessible.",
#                      err)
#----------------------------------------------------------------------------------------------------------------------
