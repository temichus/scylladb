# coding: utf-8

import time
import random
import itertools
import requests

from threading import Thread, Event

from nose.plugins.attrib import attr
from cassandra import ConsistencyLevel, WriteTimeout
from cassandra.query import SimpleStatement

from assertions import assert_unavailable
from dtest import Tester, debug
from tools import no_vnodes, since
from nose.plugins.attrib import attr
from scylla_tools import scylla_mode


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
                                        print("[%3d] Update was inserted on previous try (res = %s)" % (self.wid, str(res)))
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

        assert (value == threads * iterations) and (errors == 0), "value=%d, errors=%d, retries=%d" % (value, errors, retries)

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

    def enable_error_injection(self, node, injection_name, once=False):
        ip = self.get_ip_from_node(node)
        port = 10000 # default REST API port value

        url = f"http://{ip}:{port}/v2/error_injection/injection/{injection_name}"
        resp = requests.post(url, params={"one_shot": once})
        if not resp.ok:
            raise Exception(f"Failed to enable error injection on a node. Error message: {resp.text}")

    def disable_all_error_injections(self, node):
        ip = self.get_ip_from_node(node)
        port = 10000 # default REST API port value

        url = f"http://{ip}:{port}/v2/error_injection/injection"
        resp = requests.delete(url)
        if not resp.ok:
            raise Exception(f"Failed to disable error injections on a node. Error message: {resp.text}")

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
        paxos_stages = ['prepare', 'accept', 'learn']

        # Execute the LWT query on the first node, which acts as a coordinator in this case
        session_node1 = self.patient_exclusive_cql_connection(nodes[0], protocol_version=4)
        session_node1.set_keyspace("ks")
        stmt = session_node1.prepare("INSERT INTO test (k, v) VALUES (?, 0) IF NOT EXISTS")
        key = 0

        for combination_len in range(0, len(paxos_stages) + 1):
            for combination in itertools.combinations(paxos_stages, combination_len):
                # We need to clear leftover enabled injections from a previous
                # iteration of the test because each injection is enabled at
                # each shard on a given node.
                #
                # Though, it's not guaranteed that the injection is triggered on
                # each shard actually, so we can end up with some injections still
                # enabled for some shards.

                debug("Reset enabled injections on each node in the test cluster")
                for node in nodes:
                    self.disable_all_error_injections(node)

                debug(f"Testing combination {combination}")
                for stage in combination:
                    injection_name = f"paxos_state_{stage}_timeout"
                    for node in nodes:
                        self.enable_error_injection(node, injection_name, once=True)

                res = session_node1.execute(stmt, [key])
                # verify the number of retries of the query is equal to combination_len
                assert res.response_future._query_retries == combination_len

                key += 1

