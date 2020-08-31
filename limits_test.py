from dtest import Tester
from scylla_tools import scylla_mode

import math

from unittest import skip
from nose.plugins.attrib import attr


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


@attr('dtest-full', 'single_node')
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
            expected_error = "Key size too large: \d+ > 65535"
            self.ignore_log_patterns += [expected_error]
            with self.assertRaisesRegex(Exception, expected_error):
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

        self.assertEqual(len(res.current_rows), 1)

        res = session.execute("""
                SELECT * FROM ks.test1
                WHERE %s=2
        """ % key_name)

        self.assertEqual(len(res.current_rows), 1)
        session.execute("""DROP TABLE test1""")

    def max_key_length_test(self):
        cluster = self.prepare()
        cluster.populate(1).start()
        node = cluster.nodelist()[0]

        session = self.patient_cql_connection(node)
        self.create_ks(session, 'ks', 1)

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

        self.assertEqual(len(list(res)), 1)

        res = session.execute("""
                SELECT * FROM ks.test1
                WHERE user='milou'
        """)

        self.assertEqual(len(list(res)), 1)
        session.execute("""DROP TABLE test1""")

    def max_column_value_size_test(self):
        cluster = self.prepare()
        cluster.populate(1).start()
        node = cluster.nodelist()[0]

        session = self.patient_cql_connection(node)
        self.create_ks(session, 'ks', 1)

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
            expected_error = "Mutation of \d+ bytes is too large for the maximum size of 16777216"
            self.ignore_log_patterns += [expected_error]
            with self.assertRaisesRegex(Exception, expected_error):
                session.execute(c)
            return

        session.execute(c)

        c = "insert into ks.test1  (%s blub) values (%s 1);" % (keys, values)
        session.execute(c)

        session.execute("""DROP TABLE test1""")

    @scylla_mode('!debug')  # client times out in debug mode
    def max_columns_and_query_parameters_test(self):
        cluster = self.prepare()
        cluster.populate(1).start()
        node = cluster.nodelist()[0]

        session = self.patient_cql_connection(node)
        self.create_ks(session, 'ks', 1)

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
        self.assertEqual(len(res.current_rows), 1)

        session.execute("""DROP TABLE stuff""")

    def max_tuple_test(self):
        cluster = self.prepare()
        cluster.populate(1).start()
        node = cluster.nodelist()[0]

        session = self.patient_cql_connection(node)
        self.create_ks(session, 'ks', 1)

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
        self.assertEqual(len(list(res)), rows)

        session.execute("""DROP TABLE STUFF""")

    @attr('next-gating')
    @attr('dtest-debug')
    def max_batch_size_test(self):
        cluster = self.prepare()
        cluster.populate(1).start()
        node = cluster.nodelist()[0]

        session = self.patient_cql_connection(node)
        self.create_ks(session, 'ks', 1)

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

    @scylla_mode('!debug')  # client times out in debug mode
    def max_cells_test(self):
        cluster = self.prepare()
        cluster.populate(1).start()
        node = cluster.nodelist()[0]

        session = self.patient_cql_connection(node)
        self.create_ks(session, 'ks', 1)

        cells = 1
        for i in range(int(math.log(MAX_CELLS, 2))):
            cells <<= 1
            self._do_test_max_cell_count(session, node, cells - 1)
