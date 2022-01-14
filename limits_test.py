import logging
import os
import pathlib
import resource
import sys
from subprocess import Popen, PIPE, check_output

import requests
from cassandra.cluster import Cluster

from dtest_class import Tester, create_ks
import math
import pytest

logger = logging.getLogger(__name__)
# Those are ideal values according to c* specifications
# they should pass

LIMIT_64_K = (64 * 1024)
LIMIT_32K = (32 * 1024)
LIMIT_2GB = (2 * 1024 * 1024 * 1024)

MAX_KEY_SIZE = LIMIT_64_K
MAX_BLOB_SIZE = 8388608  # theoretical limit LIMIT_2GB
MAX_COLUMNS = LIMIT_64_K
MAX_TUPLES = LIMIT_32K
MAX_BATCH_SIZE = 50 * 1024
MAX_CELLS_COLUMNS = LIMIT_32K
MAX_CELLS_BATCH_SIZE = 1000
MAX_CELLS = 16777216

# Those are values used to validate the tests code
#MAX_KEY_SIZE = 1000
#MAX_BLOB_SIZE = 1000
#MAX_COLUMNS = 1000
#MAX_TUPLES = 1000
#MAX_BATCH_SIZE = 1000
#MAX_CELLS_COLUMNS = 100
#MAX_CELLS_BATCH_SIZE = 100
#MAX_CELLS = 1000


@pytest.mark.dtest_full
@pytest.mark.single_node
class TestLimits(Tester):

    def prepare(self):
        """
        Sets up node to test against.
        """
        cluster = self.cluster
        return cluster

    def _do_test_max_key_length(self, session, node, size, expect_failure=False):
        print("Testing max key length for {}.{}".format(size, " Expected failure..." if expect_failure else ""))
        key_name = "k" * size

        c = "CREATE TABLE test1 ({} int PRIMARY KEY)".format(key_name)
        if expect_failure:
            expected_error = r"Key size too large: \d+ > 65535"
            self.ignore_log_patterns += [expected_error]
            with pytest.raises(Exception,
                               match=expected_error):
                session.execute(c)
            return

        session.execute(c)

        session.execute("insert into ks.test1  (%s) values (1);" % key_name)
        session.execute("insert into ks.test1  (%s) values (2);" % key_name)

        node.flush()
        # Select
        res = session.execute("""
                SELECT * FROM ks.test1
                WHERE %s=1
        """ % key_name)

        assert len(res.current_rows) == 1

        res = session.execute("""
                SELECT * FROM ks.test1
                WHERE %s=2
        """ % key_name)

        assert len(res.current_rows) == 1
        session.execute("""DROP TABLE test1""")

    def test_max_key_length(self):
        cluster = self.prepare()
        cluster.populate(1).start()
        node = cluster.nodelist()[0]

        session = self.patient_cql_connection(node)
        create_ks(session, 'ks', 1)

        # biggest that will currently work in scylla
        # key_name = "k" * 65526
        self._do_test_max_key_length(session, node, MAX_KEY_SIZE, expect_failure=True)
        self._do_test_max_key_length(session, node, MAX_KEY_SIZE - 9, expect_failure=True)

        self._do_test_max_key_length(session, node, MAX_KEY_SIZE - 10)

        size = MAX_KEY_SIZE // 2
        while size >= 1:
            self._do_test_max_key_length(session, node, size)
            size >>= 3

    def _do_test_blob_size(self, session, node, size):
        print("Testing blob size %i" % size)

        blob_a = ("a" * size)
        blob_b = ("b" * size)

        session.execute("""
            CREATE TABLE test1 (
                user ascii PRIMARY KEY,
                payload blob,
            )
        """)

        session.execute("insert into ks.test1  (user, payload) values ('tintin', textAsBlob('%s'));" % blob_a)
        session.execute("insert into ks.test1  (user, payload) values ('milou', textAsBlob('%s'));" % blob_b)

        node.flush()
        # Select
        res = session.execute("""
                SELECT * FROM ks.test1
                WHERE user='tintin'
        """)

        assert len(list(res)) == 1

        res = session.execute("""
                SELECT * FROM ks.test1
                WHERE user='milou'
        """)

        assert len(list(res)) == 1
        session.execute("""DROP TABLE test1""")

    def test_max_column_value_size(self):
        cluster = self.prepare()
        cluster.populate(1).start()
        node = cluster.nodelist()[0]

        session = self.patient_cql_connection(node)
        create_ks(session, 'ks', 1)

        size = 1
        for i in range(int(math.log(MAX_BLOB_SIZE, 2))):
            size <<= 1
            self._do_test_blob_size(session, node, size - 1)

    def _do_test_max_columns(self, session, count, expect_failure=False):
        print("Testing maximum numbers of columns with count {}.{}".format(
            count, " Expected failure..." if expect_failure else ""))

        # we must count the primary key
        count -= 1
        if count < 0:
            count = 0

        keys = ""
        keys_create = ""
        for i in range(count):
            keys += "key" + str(i) + ", "
            keys_create += "key" + str(i) + " int, "
        values = "1, " * count
        keys = keys

        c = """CREATE TABLE test1 (%s blub int PRIMARY KEY,)""" % keys_create
        if expect_failure:
            expected_error = r"Mutation of \d+ bytes is too large for the maximum size of 16777216"
            self.ignore_log_patterns += [expected_error]
            with pytest.raises(Exception,
                               match=expected_error):
                session.execute(c)
            return

        session.execute(c)

        c = "insert into ks.test1  (%s blub) values (%s 1);" % (keys, values)
        session.execute(c)

        session.execute("""DROP TABLE test1""")

    @pytest.mark.scylla_mode('!debug')  # client times out in debug mode
    def test_max_columns_and_query_parameters(self):
        cluster = self.prepare()
        cluster.populate(1).start()
        node = cluster.nodelist()[0]

        session = self.patient_cql_connection(node)
        create_ks(session, 'ks', 1)

        count = 1
        for i in range(int(math.log(MAX_COLUMNS, 2))):
            count <<= 1
            self._do_test_max_columns(session, count - 1, expect_failure=(count == MAX_COLUMNS))

    def _do_test_max_tuples(self, session, node, count):
        print("Testing max tuples for %i" % count)
        t = ""
        v = ""
        for i in range(count):
            t += "int, "
            v += "1, "
        t = t[:-2]
        v = v[:-2]

        c = """
            CREATE TABLE stuff (
              k int PRIMARY KEY,
              v frozen<tuple<%s>>
            );
            """ % t
        session.execute(c)

        c = "INSERT INTO stuff (k, v) VALUES(0, (%s));" % v
        session.execute(c)

        c = "SELECT * FROM STUFF;"
        res = session.execute(c)
        assert len(res.current_rows) == 1

        session.execute("""DROP TABLE stuff""")

    def test_max_tuple(self):
        cluster = self.prepare()
        cluster.populate(1).start()
        node = cluster.nodelist()[0]

        session = self.patient_cql_connection(node)
        create_ks(session, 'ks', 1)

        count = 1
        for i in range(int(math.log(MAX_TUPLES, 2))):
            count <<= 1
            self._do_test_max_tuples(session, node, count - 1)

    def _do_test_max_batch_size(self, session, node, size):
        print("Testing max batch size for size=%i" % size)
        c = """
            CREATE TABLE stuff (
              k int PRIMARY KEY,
              v text
            );
            """
        session.execute(c)

        c = "BEGIN UNLOGGED  BATCH\n"
        row_size = 1000
        overhead = 100
        blob = (row_size - overhead) * 'x'
        rows = size // row_size
        for i in range(rows):
            c += "INSERT INTO stuff (k, v) VALUES(%i, '%s')\n" % (i, blob)
        c += "APPLY BATCH;\n"

        session.execute(c)

        c = "SELECT * FROM STUFF;"
        res = session.execute(c)

        assert len(list(res)) == rows
        session.execute("""DROP TABLE STUFF""")

    @pytest.mark.next_gating
    @pytest.mark.dtest_debug
    def test_max_batch_size(self):
        cluster = self.prepare()
        cluster.populate(1).start()
        node = cluster.nodelist()[0]

        session = self.patient_cql_connection(node)
        create_ks(session, 'ks', 1)

        size = 1
        for i in range(int(math.log(MAX_BATCH_SIZE, 2))):
            size <<= 1
            self._do_test_max_batch_size(session, node, size - 1)

    def _do_test_max_cell_count(self, session, node, cells):
        print("Testing max cells count for %i" % cells)
        keys = ""
        keys_create = ""
        columns = MAX_CELLS_COLUMNS
        for i in range(columns):
            keys += "key" + str(i) + ", "
            keys_create += "key" + str(i) + " int, "
        values = "1, " * columns

        c = """CREATE TABLE test1 (%s blub int PRIMARY KEY,)""" % keys_create
        session.execute(c)

        batch_size = MAX_CELLS_BATCH_SIZE
        rows = cells // columns
        c = "BEGIN UNLOGGED  BATCH\n"
        for i in range(rows):
            c += "insert into ks.test1  (%s blub) values (%s %i);\n" % (keys, values, i)
            if i % batch_size == 0:
                c += "APPLY BATCH;\n"
                session.execute(c)
                c = "BEGIN UNLOGGED  BATCH\n"

        session.execute("""DROP TABLE test1""")

    @pytest.mark.scylla_mode('!debug')  # client times out in debug mode
    def test_max_cells(self):
        cluster = self.prepare()
        cluster.populate(1).start()
        node = cluster.nodelist()[0]

        session = self.patient_cql_connection(node)
        create_ks(session, 'ks', 1)

        cells = 1
        for i in range(int(math.log(MAX_CELLS, 2))):
            cells <<= 1
            self._do_test_max_cell_count(session, node, cells - 1)


class TestMaxCQLConnections(Tester):

    def test_max_cql_connections(self):
        """
        Verifies fix https://github.com/scylladb/scylla/pull/9052 which aimed issue for crashing scylla when
        there was more than 10000 connections per shard. Fix adds possibility to set max no of connections and
        increased default value. But still db crashes when reaching this limit (tracked by #9056).

        Test verifies also if connection pool is properly released after connection shutdown.
        """
        workers = 5  # opening many connections in python gets slower and slower. Spreading to workers helps.
        connections_per_worker = 3000
        total_connections = workers * connections_per_worker
        self._tune_max_open_files_limit(total_connections)
        self.cluster.populate(1).start(jvm_args=['--smp', '1', "--max-networking-io-control-blocks",
                                                 str(total_connections)])
        address = self.cluster.nodelist()[0].address()
        processes = self._create_cql_connections(address, connections_per_worker=connections_per_worker,
                                                 workers=workers)

        connections_created = self._get_cql_connections_from_metrics(address)
        assert connections_created >= total_connections, \
            f"only {connections_created} connections created from {total_connections} required"
        self._close_connections(processes)

        # repeat to verify scylla closed connections correctly and can create new ones
        processes = self._create_cql_connections(address, connections_per_worker=connections_per_worker,
                                                 workers=workers)
        self._close_connections(processes)
        connections_created = self._get_cql_connections_from_metrics(address)
        assert connections_created >= total_connections, \
            f"only {connections_created} connections created from {total_connections} required"

    def _create_cql_connections(self, address, connections_per_worker, workers):
        logger.info("starting creating connections in parallel")
        script_path = pathlib.Path(__file__).parent.absolute() / "scripts" / "create_dummy_cql_connections.py"
        processes = []
        for _ in range(workers):
            processes.append(Popen([sys.executable, script_path, address, str(connections_per_worker)],
                                   stdin=PIPE, stdout=PIPE, universal_newlines=True))
        # wait for finish connection creation
        for process in processes:
            line = process.stdout.readline()
            assert line.startswith(f"{connections_per_worker} cql connections created."), \
                "Dummy connections creation script failed."
        logger.info("All connections created successfully")
        return processes

    def _close_connections(self, processes):
        """dummy cql connections scripts end after pressing any key."""
        for process in processes:
            process.communicate("a", timeout=10)

    def _get_cql_connections_from_metrics(self, address):
        resp = requests.get(f"http://{address}:9180/metrics")
        for line in resp.text.splitlines():
            if line.startswith("scylla_transport_cql_connections"):
                return int(line.split('scylla_transport_cql_connections{shard="0"}')[1])

    def _tune_max_open_files_limit(self, total_connections):
        """each connection creates 1 open file per shard.
        Creating many connections requires tuning max open files in system."""
        pid = os.getpid()
        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        soft_new_limit = max(soft, 2 * total_connections)
        hard_new_limit = max(hard, 2 * total_connections)
        logger.debug(f"current limits: {soft}, {hard}")
        if soft < soft_new_limit:
            check_output(f"sudo prlimit --pid {pid} --nofile={soft_new_limit}:{hard_new_limit}", shell=True,
                         universal_newlines=True)
        ulimit = int(check_output("ulimit -n", shell=True, universal_newlines=True))
        assert ulimit == soft_new_limit
        logger.info(f"Updated max open files limit to: {ulimit}")
