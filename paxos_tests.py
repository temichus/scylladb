# coding: utf-8

import time
import random
import itertools

from threading import Thread, Event

from nose.plugins.attrib import attr
from cassandra import ConsistencyLevel, WriteTimeout, WriteFailure
from cassandra.query import SimpleStatement

from assertions import assert_unavailable, assert_invalid, assert_one
from dtest import Tester, debug
from tools import no_vnodes, since
from nose.plugins.attrib import attr
from scylla_tools import scylla_mode
from tools import require


class LoadThread(Thread):
    def __init__(self, tester, node, shift):
        Thread.__init__(self)
        self.tester = tester
        self.target_node = node
        self._to_stop = Event()
        self._to_stop.clear()
        self._results = []
        self._step = 1000
        self._base_value = -2147483648 + shift
        self._max_value = 2147483647
        self._current = 0

    def run(self):
        if not self.target_node.is_running():
            return

        while True:
            with self.tester.patient_cql_connection(self.target_node) as session:
                insert_stmt = session.prepare("INSERT INTO ks.test(k,v) VALUES (?, ?)")
                update_stmt = session.prepare("UPDATE ks.test SET v = v + 1 WHERE k=? IF EXISTS")
                for n in range(self._base_value, self._max_value, self._step):
                    if self._to_stop.is_set():
                        return
                    try:
                        self._results[(n - self._base_value) // self._step]
                    except IndexError:
                        try:
                            session.execute(insert_stmt.bind((n, 0)))
                            self._results[(n - self._base_value)//self._step] = 0
                            continue
                        except:  # pylint: disable=bare-except
                            self._results[(n - self._base_value)//self._step] = None
                            continue
                    try:
                        session.execute(update_stmt.bind((n,)))
                        self._results[(n - self._base_value)//self._step] += 1
                    except:  # pylint: disable=bare-except
                        pass

    def stop(self, timeout=None):
        self._to_stop.set()
        try:
            self.join(timeout)
        except:  # pylint: disable=bare-except
            pass


@since('2.0.6')
@attr('dtest-full')
class TestPaxos(Tester):

    def prepare(self, create_keyspace=True, use_cache=False, nodes=1, rf=1):
        cluster = self.cluster

        if (use_cache):
            cluster.set_configuration_options(values={'row_cache_size_in_mb': 100})

        cluster.populate(nodes).start(wait_for_binary_proto=True, wait_other_notice=True)
        node1 = cluster.nodelist()[0]
        time.sleep(0.2)

        session = self.patient_cql_connection(node1)
        if create_keyspace:
            self.create_ks(session, 'ks', rf)
        self._node_num = 6
        return session

    def add_nodes(self, num=1):
        if num == 0:
            return
        added_nodes = []
        for n in range(num):
            self._node_num += 1
            new_node = self.cluster.new_node(self._node_num)
            new_node.start(wait_for_binary_proto=True, wait_other_notice=True)
            added_nodes.append(new_node)
        self._wait_till_nodes_are_up(added_nodes)
        return added_nodes

    def _wait_till_nodes_are_up(self, nodes):
        for node in nodes:
            for _ in range(10):
                try:
                    self.patient_cql_connection(node).execute('USE system')
                except Exception:  # pylint: disable=broad-except
                    pass
                time.sleep(0.5)

    def node_session(self, node):
        return self.patient_cql_connection(self.nodelist()[node])

    def replica_availability_test(self):
        """
        @jira_ticket CASSANDRA-8640

        Regression test for a bug (CASSANDRA-8640) that required all nodes to
        be available in order to run LWT queries, even if the query could
        complete correctly with quorum nodes available.
        """
        session = self.prepare(nodes=3, rf=3)
        session.execute("CREATE TABLE test (k int PRIMARY KEY, v int)")
        session.execute("INSERT INTO test (k, v) VALUES (0, 0) IF NOT EXISTS")

        self.cluster.nodelist()[2].stop()
        session.execute("INSERT INTO test (k, v) VALUES (1, 1) IF NOT EXISTS")

        self.cluster.nodelist()[1].stop()
        assert_unavailable(session.execute, "INSERT INTO test (k, v) VALUES (2, 2) IF NOT EXISTS")

        self.cluster.nodelist()[1].start(wait_for_binary_proto=True, wait_other_notice=True)
        session.execute("INSERT INTO test (k, v) VALUES (3, 3) IF NOT EXISTS")

        self.cluster.nodelist()[2].start(wait_for_binary_proto=True)
        session.execute("INSERT INTO test (k, v) VALUES (4, 4) IF NOT EXISTS")

    @no_vnodes()
    def cluster_availability_test(self):
        # Warning, a change in partitioner or a change in CCM token allocation
        # may require the partition keys of these inserts to be changed.
        # This must not use vnodes as it relies on assumed token values.

        session = self.prepare(nodes=3)
        session.execute("CREATE TABLE test (k int PRIMARY KEY, v int)")
        session.execute("INSERT INTO test (k, v) VALUES (0, 0) IF NOT EXISTS")

        self.cluster.nodelist()[2].stop()
        session.execute("INSERT INTO test (k, v) VALUES (1, 1) IF NOT EXISTS")

        self.cluster.nodelist()[1].stop()
        session.execute("INSERT INTO test (k, v) VALUES (3, 2) IF NOT EXISTS")

        self.cluster.nodelist()[1].start(wait_for_binary_proto=True)
        session.execute("INSERT INTO test (k, v) VALUES (5, 5) IF NOT EXISTS")

        self.cluster.nodelist()[2].start(wait_for_binary_proto=True)
        session.execute("INSERT INTO test (k, v) VALUES (6, 6) IF NOT EXISTS")

    def contention_test_multi_iterations(self):
        self._contention_test(8, 100)

    # Warning, this test will require you to raise the open
    # file limit on OSX. Use 'ulimit -n 1000'
    def contention_test_many_threads(self):
        self._contention_test(300, 1)

    def _contention_test(self, threads, iterations):
        """
        Test threads repeatedly contending on the same row.
        """

        verbose = False

        session = self.prepare(nodes=3)
        session.execute("CREATE TABLE test (k int, v int static, id int, PRIMARY KEY (k, id))")
        session.execute("INSERT INTO test(k, v) VALUES (0, 0)")

        class Worker(Thread):

            def __init__(self, wid, session, iterations, query):
                Thread.__init__(self)
                self.wid = wid
                self.iterations = iterations
                self.query = query
                self.session = session
                self.errors = 0
                self.retries = 0

            def run(self):
                global worker_done
                i = 0
                prev = 0
                while i < self.iterations:
                    done = False
                    while not done:
                        try:
                            res = self.session.execute(self.query, (prev + 1, prev, self.wid))
                            if verbose:
                                print("[%3d] CAS %3d -> %3d (res: %s)" % (self.wid, prev, prev + 1, str(res)))
                            if res[0][0] is True:
                                done = True
                                prev = prev + 1
                            else:
                                self.retries = self.retries + 1
                                # There is 2 conditions, so 2 reasons to fail: if we failed because the row with our
                                # worker ID already exists, it means we timeout earlier but our update did went in,
                                # so do consider this as a success
                                prev = res[0][3]
                                if res[0][2] is not None:
                                    if verbose:
                                        print("[%3d] Update was inserted on previous try (res = %s)" %
                                              (self.wid, str(res)))
                                    done = True
                        except WriteTimeout as e:
                            if verbose:
                                print("[%3d] TIMEOUT (%s)" % (self.wid, str(e)))
                            # This means a timeout: just retry, if it happens that our update was indeed persisted,
                            # we'll figure it out on the next run.
                            self.retries = self.retries + 1
                        except Exception as e:
                            if verbose:
                                print("[%3d] ERROR: %s" % (self.wid, str(e)))
                            self.errors = self.errors + 1
                            done = True
                    i = i + 1
                    # Clean up for next iteration
                    while True:
                        try:
                            self.session.execute("DELETE FROM test WHERE k = 0 AND id = %d IF EXISTS" % self.wid)
                            break
                        except WriteTimeout as e:
                            pass

        nodes = self.cluster.nodelist()
        workers = []

        c = self.patient_cql_connection(nodes[0], keyspace='ks')
        q = c.prepare("""
                BEGIN BATCH
                   UPDATE test SET v = ? WHERE k = 0 IF v = ?;
                   INSERT INTO test (k, id) VALUES (0, ?) IF NOT EXISTS;
                APPLY BATCH
            """)

        for n in range(0, threads):
            workers.append(Worker(n, c, iterations, q))

        start = time.time()

        for w in workers:
            w.start()

        for w in workers:
            w.join()

        if verbose:
            runtime = time.time() - start
            print("runtime:", runtime)

        query = SimpleStatement("SELECT v FROM test WHERE k = 0", consistency_level=ConsistencyLevel.ALL)
        rows = session.execute(query)
        value = rows[0][0]

        errors = 0
        retries = 0
        for w in workers:
            errors = errors + w.errors
            retries = retries + w.retries

        assert (value == threads * iterations) and (errors ==
                                                    0), "value=%d, errors=%d, retries=%d" % (value, errors, retries)

    def _add_random_nodes(self, max_limit, upper_node_limit, loaders):
        debug(f"_add_random_nodes(self, max_limit={max_limit}")
        nodes_to_add = random.randint(0, min(max_limit, upper_node_limit - len(self.cluster.nodelist())))
        if nodes_to_add == 0:
            return
        debug(f"_remove_random_nodes number_to_add={nodes_to_add}")
        nodes_before = len(self.cluster.nodelist())
        added_nodes = self.add_nodes(nodes_to_add)
        for node_n in range(len(added_nodes)):
            node = added_nodes[node_n]
            loaders[node] = LoadThread(self, node, nodes_before + node_n)

    def _remove_random_nodes(self, max_limit, lower_node_limit, loaders):
        nodes = self.cluster.nodelist()
        to_stop = []
        number_to_remove = random.randint(0, min(max_limit, len(self.cluster.nodelist()) - lower_node_limit))
        if not number_to_remove:
            return
        debug(f"_remove_random_nodes number_to_remove={number_to_remove}")
        for n in range(number_to_remove):
            to_stop.append(nodes[n + lower_node_limit])
        for node in to_stop:
            node.stop(wait=True, wait_other_notice=True, gently=True)
            if node.is_running():
                node.stop(wait=True, wait_other_notice=True, gently=False)
            self.cluster.remove(node)
            loaders[node].stop()
            del loaders[node]

    @since('3.3')
    @require('#7087')
    def test_topology_change_in_presence_of_down_node(self):
        session = self.prepare(nodes=6, rf=4)
        lower_node_limit = 3
        upper_node_limit = 8
        stop_start_limit = 3
        session.execute("CREATE TABLE test (k int PRIMARY KEY, v int)")
        loaders = {}
        n = 0
        for node in self.cluster.nodelist():
            loaders[node] = LoadThread(self, node, n)
            n += 1
        time.sleep(10)
        for n in range(3):
            self._remove_random_nodes(1, lower_node_limit, loaders)
            time.sleep(random.uniform(5, 15))
        for n in range(10):
            self._remove_random_nodes(stop_start_limit, lower_node_limit, loaders)
            time.sleep(random.uniform(0, 3))
            self._add_random_nodes(stop_start_limit, upper_node_limit, loaders)
            time.sleep(random.uniform(5, 15))

    @attr('dtest-debug')
    @scylla_mode('!release')
    def cas_statement_timeout_test(self):
        '''
        Tests for adequate handling timeouts from replicas in each stage of paxos algorithm.
        I.e. there should be a retry of the paxos round if a timeout is encountered.

        This test is meant to be run only on 'debug' and 'dev' builds of Scylla since in
        release mode error injections do nothing.
        '''

        # Reduce write request timeout to 1000ms in order to speed the testing a little bit
        self.cluster.set_configuration_options(values={'write_request_timeout_in_ms': 1000})
        session = self.prepare(nodes=3, rf=3)

        session.execute("CREATE TABLE test (k int PRIMARY KEY, v int)")

        nodes = self.cluster.nodelist()

        # Try different combinations of timeouts in each paxos stage
        paxos_stages_injections = {
            'prepare': 'paxos_prepare_timeout',
            'accept': 'paxos_accept_proposal_timeout',
            'learn': 'paxos_state_learn_timeout'}

        # Execute the LWT query on the first node, which acts as a coordinator in this case
        session_node1 = self.patient_exclusive_cql_connection(nodes[0], protocol_version=4)
        session_node1.set_keyspace("ks")
        stmt = session_node1.prepare("INSERT INTO test (k, v) VALUES (?, 0) IF NOT EXISTS")
        key = 0

        for combination_len in range(0, len(paxos_stages_injections) + 1):
            for combination in itertools.combinations(paxos_stages_injections.keys(), combination_len):
                # We need to clear leftover enabled injections from a previous
                # iteration of the test because each injection is enabled at
                # each shard on a given node.
                #
                # Though, it's not guaranteed that the injection is triggered on
                # each shard actually, so we can end up with some injections still
                # enabled for some shards.

                debug("Reset enabled injections on each node in the test cluster")
                for node in nodes:
                    self.disable_errors(node)

                debug(f"Testing combination {combination}")
                for stage in combination:
                    injection_name = paxos_stages_injections[stage]
                    for node in nodes:
                        self.enable_error(injection_name, node, one_shot=True)

                res = session_node1.execute(stmt, [key])
                # verify the number of retries of the query is equal to combination_len
                assert res.response_future._query_retries == combination_len

                key += 1

    # Schema mismatch tests

    def _base_schema_mismatch_test_tpl(self, clear_schema_cache, setup_test_env_action,
                                       insert_action, ddl_action, second_insert_action, verify_results_action):
        # Increase time period for which cached schema definitions will live before
        # being evicted from the node cache.
        # The tests depend on this cache being not empty, so set to some sufficiently large value, e.g. 1000 seconds.
        self.cluster.set_configuration_options(values={'schema_registry_grace_period': 1000})
        # set TRACE log level for the test node to be able
        # to catch schema_mismatch_error exceptions
        self.cluster.set_log_level('TRACE')
        session = self.prepare(nodes=1, rf=1)
        # Create test tables and configure other necessary stuff
        setup_test_env_action(session)

        node1 = self.cluster.nodelist()[0]

        # Fail at the end of "accept" stage so that we have commited a proposal but not yet completed the round
        errinj_name = "paxos_error_after_save_proposal"
        debug(f"Enable {errinj_name} injection on the test node")
        self.enable_error(errinj_name, node1, one_shot=True)

        # Execute the LWT query leaving an unfinished paxos round behind
        key = 0
        with self.assertRaises(WriteFailure):
            debug(f"Execute the first INSERT query on key {key}")
            insert_action(session, key)

        # perform DDL action to change test table schema version
        ddl_action(session)

        debug("Disable remaining injections on the node (if any)")
        self.disable_errors(node1)

        if clear_schema_cache:
            debug(f"Restart the node to clear up schema_registry cache")
            node1.stop(wait=True)
            node1.start()
            # re-open the session to the node
            session = self.patient_cql_connection(node1)
            session.execute("USE ks")

        # Initiate a subsequent round on the same key so that it performs
        # repair of the previous round and it is supposed to fail
        debug(f"Execute the second INSERT on key {key} (supposed to trigger repair of the previous round)")
        second_insert_action(session, key)
        # Execute additional actions to verify that the test executed successfully (check logs and data)
        verify_results_action(session, node1)

    def _schema_mismatch_tpl(self, clear_schema_cache):
        '''
        Tests for the following scenario:

        1. Execute an LWT query against a key. Suppose the transaction failed for
        some reason but did manage to save its paxos proposal (along with the associated mutation)
        before failing.

        2. Change the table schema so that the mutation from the saved paxos proposal holds the
        reference to an invalid schema version.

        3. Start a new LWT query against the same key so that the coordinator node tries to
        repair the previous unfinished paxos round.

        It will try to apply the stored mutation (which has an obsolete schema version) and should
        try to look up the old schema in a history table.

        Refs: #6502
        '''

        def create_test_table(session):
            session.execute("CREATE TABLE test (k int PRIMARY KEY, v int)")

        def insert_action(session, key):
            stmt = session.prepare("INSERT INTO test (k, v) VALUES (?, ?) IF NOT EXISTS")
            session.execute(stmt, [key, 0])

        def add_dummy_column(session):
            session.execute("ALTER TABLE test ADD dummy int")

        def check_schema_mismatch_exc(session, node):
            exc_msg = node.grep_log("<schema_mismatch_error>")
            if exc_msg:
                raise Exception(f"Unexpected \"schema_mismatch_error\" exception: {exc_msg}")

        self._base_schema_mismatch_test_tpl(clear_schema_cache=clear_schema_cache,
                                            setup_test_env_action=create_test_table,
                                            insert_action=insert_action,
                                            ddl_action=add_dummy_column,
                                            second_insert_action=insert_action,
                                            verify_results_action=check_schema_mismatch_exc
                                            )

    @attr('dtest-debug', 'single_node')
    @scylla_mode('!release')
    def schema_mismatch_cache_test(self):
        self._schema_mismatch_tpl(clear_schema_cache=False)

    @attr('dtest-debug', 'single_node')
    @scylla_mode('!release')
    def schema_mismatch_no_cache_test(self):
        self._schema_mismatch_tpl(clear_schema_cache=True)

    def _schema_mismatch_mv_tpl(self, clear_schema_cache):
        '''
        Tests for the following scenario:

        1. Create a table and an associated materialized view

        2. Execute an LWT query against a key. Suppose the transaction failed for
        some reason but did manage to save its paxos proposal (along with the associated mutation)
        before failing.

        3. Change the table schema so that the mutation from the saved paxos proposal holds the
        reference to an invalid schema version.

        4. Start a new LWT query against the same key so that the coordinator node tries to
        repair the previous unfinished paxos round.

        It will try to apply the stored mutation (which has an obsolete schema version) and should
        try to look up the old schema in a history table.

        Refs: scylladb/scylla#6074
        '''
        def create_test_table_and_mv(session):
            session.execute("CREATE TABLE test (k int PRIMARY KEY, v int)")
            session.execute("CREATE MATERIALIZED VIEW test_view AS SELECT * from test where k > 0 PRIMARY KEY(k)")

        def insert_action(session, key):
            stmt = session.prepare("INSERT INTO test (k, v) VALUES (?, ?) IF NOT EXISTS")
            session.execute(stmt, [key, 0])

        def add_dummy_column(session):
            session.execute("ALTER TABLE test ADD dummy int")

        def check_schema_mismatch_exc(session, node):
            exc_msg = node.grep_log("<schema_mismatch_error>")
            if exc_msg:
                raise Exception(f"Unexpected \"schema_mismatch_error\" exception: {exc_msg}")

        self._base_schema_mismatch_test_tpl(clear_schema_cache=clear_schema_cache,
                                            setup_test_env_action=create_test_table_and_mv,
                                            insert_action=insert_action,
                                            ddl_action=add_dummy_column,
                                            second_insert_action=insert_action,
                                            verify_results_action=check_schema_mismatch_exc
                                            )

    @attr('dtest-debug', 'single_node')
    @scylla_mode('!release')
    def schema_mismatch_mv_cache_test(self):
        self._schema_mismatch_mv_tpl(clear_schema_cache=False)

    @attr('dtest-debug', 'single_node')
    @scylla_mode('!release')
    def schema_mismatch_mv_no_cache_test(self):
        self._schema_mismatch_mv_tpl(clear_schema_cache=True)

    def _schema_mismatch_drop_regular_column_tpl(self, clear_schema_cache):
        '''
        Tests for the following scenario:

        1. Execute an LWT query against a key. Suppose the transaction failed for
        some reason but did manage to save its paxos proposal (along with the associated mutation)
        before failing.

        2. Change the table schema so that the mutation from the saved paxos proposal holds the
        reference to an invalid schema version (drop one of the columns participating in the query).

        3. Start a new LWT query against the same key so that the coordinator node tries to
        repair the previous unfinished paxos round.

        It will try to apply the stored mutation (which has an obsolete schema version) and should
        try to look up the old schema in a history table.

        Refs: #6467
        '''

        def create_test_table(session):
            session.execute("CREATE TABLE test (k int PRIMARY KEY, v int)")

        def insert_action(session, key):
            stmt = session.prepare("INSERT INTO test (k, v) VALUES (?, ?) IF NOT EXISTS")
            session.execute(stmt, [key, 0])

        def drop_column(session):
            session.execute("ALTER TABLE test DROP v")

        def second_insert_action(session, key):
            stmt = session.prepare("INSERT INTO test (k) VALUES (?) IF NOT EXISTS")
            session.execute(stmt, [key])

        def check_exc_and_table_data(session, node):
            exc_msg = r"exception during mutation write to ([0-9.]+): std::out_of_range \(regular column id 0 >= 0\)"
            exc_msg = node.grep_log(exc_msg)
            if exc_msg:
                raise Exception(f"Unexpected exception during mutation write: {exc_msg}")
            debug("Selecting table contents to verify that insert was applied successfully")
            assert_one(session, "SELECT * from test", [0])

        self._base_schema_mismatch_test_tpl(clear_schema_cache=clear_schema_cache,
                                            setup_test_env_action=create_test_table,
                                            insert_action=insert_action,
                                            ddl_action=drop_column,
                                            second_insert_action=second_insert_action,
                                            verify_results_action=check_exc_and_table_data
                                            )

    @attr('dtest-debug', 'single_node')
    @scylla_mode('!release')
    def schema_mismatch_drop_regular_column_cache_test(self):
        self._schema_mismatch_drop_regular_column_tpl(clear_schema_cache=False)

    @attr('dtest-debug', 'single_node')
    @scylla_mode('!release')
    def schema_mismatch_drop_regular_column_no_cache_test(self):
        self._schema_mismatch_drop_regular_column_tpl(clear_schema_cache=True)

    def _schema_mismatch_drop_regular_column_in_the_middle_tpl(self, clear_schema_cache):
        '''
        Tests for the following scenario:

        1. Execute an LWT query against a key. Suppose the transaction failed for
        some reason but did manage to save its paxos proposal (along with the associated mutation)
        before failing.

        2. Change the table schema so that the mutation from the saved paxos proposal holds the
        reference to an invalid schema version (drop one of the columns participating in the query).

        3. Start a new LWT query against the same key so that the coordinator node tries to
        repair the previous unfinished paxos round.

        It will try to apply the stored mutation (which has an obsolete schema version) and should
        try to look up the old schema in a history table.

        Refs: #6467
        '''

        def create_test_table(session):
            session.execute("CREATE TABLE test (k int PRIMARY KEY, v int, v2 int)")

        def insert_action(session, key):
            stmt = session.prepare("INSERT INTO test (k, v2) VALUES (?, ?) IF NOT EXISTS")
            session.execute(stmt, [key, 0])

        def drop_column(session):
            session.execute("ALTER TABLE test DROP v")

        def second_insert_action(session, key):
            stmt = session.prepare("INSERT INTO test (k) VALUES (?) IF NOT EXISTS")
            session.execute(stmt, [key])

        def check_exc_and_table_data(session, node):
            exc_msg = r"exception during mutation write to ([0-9.]+): std::out_of_range \(regular column id 0 >= 0\)"
            exc_msg = node.grep_log(exc_msg)
            if exc_msg:
                raise Exception(f"Unexpected exception during mutation write: {exc_msg}")
            debug("Selecting table contents to verify that insert was applied successfully")
            assert_one(session, "SELECT * from test", [0, 0])

        self._base_schema_mismatch_test_tpl(clear_schema_cache=clear_schema_cache,
                                            setup_test_env_action=create_test_table,
                                            insert_action=insert_action,
                                            ddl_action=drop_column,
                                            second_insert_action=second_insert_action,
                                            verify_results_action=check_exc_and_table_data
                                            )

    @attr('dtest-debug', 'single_node')
    @scylla_mode('!release')
    def schema_mismatch_drop_regular_column_in_the_middle_cache_test(self):
        self._schema_mismatch_drop_regular_column_in_the_middle_tpl(clear_schema_cache=False)

    @attr('dtest-debug', 'single_node')
    @scylla_mode('!release')
    def schema_mismatch_drop_regular_column_in_the_middle_no_cache_test(self):
        self._schema_mismatch_drop_regular_column_in_the_middle_tpl(clear_schema_cache=True)
