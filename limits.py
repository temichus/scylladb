from dtest import Tester

import math

# Those are ideal values according to c* specifications
# they should pass

LIMIT_64_K = (64 * 1024)
LIMIT_32K = (32 * 1024)
LIMIT_2GB = (2 * 1024 * 1024 * 1024)

MAX_KEY_SIZE = LIMIT_64_K
MAX_BLOB_SIZE = LIMIT_2GB
MAX_COLUMNS = LIMIT_64_K
MAX_TUPLES = LIMIT_32K
MAX_BATCH_SIZE = LIMIT_64_K
MAX_CELLS_COLUMNS = LIMIT_32K
MAX_CELLS_BATCH_SIZE = 1000
MAX_CELLS = LIMIT_2GB

# Those are value use to validate the tests code
#MAX_KEY_SIZE = 1000
#MAX_BLOB_SIZE = 1000
#MAX_COLUMNS = 1000
#MAX_TUPLES = 1000
#MAX_BATCH_SIZE = 1000
#MAX_CELLS_COLUMNS = 100
#MAX_CELLS_BATCH_SIZE = 100
#MAX_CELLS = 1000

class TestLimits(Tester):

    def prepare(self):
        """
        Sets up node to test against.
        """
        cluster = self.cluster
        return cluster


    def _do_test_max_key_length(self, session, node, size):
        print("Testing max key length for %i" % size)
        key_name = "k" * size

        session.execute("""
            CREATE TABLE test1 (
                %s int PRIMARY KEY,
            )
        """ % (key_name))

        session.execute("insert into ks.test1  (%s) values (1);" % (key_name))
        session.execute("insert into ks.test1  (%s) values (2);" % (key_name))

        node.flush()
        # Select
        res = session.execute("""
                SELECT * FROM ks.test1
                WHERE %s=1
        """ % (key_name))

        self.assertEqual(len(res), 1)

        res = session.execute("""
                SELECT * FROM ks.test1
                WHERE %s=2
        """ % (key_name))

        self.assertEqual(len(res), 1)
        res = session.execute("""DROP TABLE test1""")

    def max_key_length_test(self):
        cluster = self.prepare()
        cluster.populate(1).start()
        node = cluster.nodelist()[0]

        session = self.patient_cql_connection(node)
        self.create_ks(session, 'ks', 1)

        # biggest that will currently works in scylla
        #key_name = "k" * 32766

        size = 1
        for i in range(int(math.log(MAX_KEY_SIZE, 2))):
            size = size << 1
            self._do_test_max_key_length(session, node, size - 1)



    def _do_test_blob_size(self, session, node, size):
        print("Testing blob size %i" % size);

        blob_a = ("a" * size)
        blob_b = ("b" * size)

        session.execute("""
            CREATE TABLE test1 (
                user ascii PRIMARY KEY,
                payload blob,
            )
        """)

        session.execute("insert into ks.test1  (user, payload) values ('tintin', textAsBlob('%s'));" % (blob_a))
        session.execute("insert into ks.test1  (user, payload) values ('milou', textAsBlob('%s'));" % (blob_b))

        node.flush()
        # Select
        res = session.execute("""
                SELECT * FROM ks.test1
                WHERE user='tintin'
        """)

        self.assertEqual(len(res), 1)

        res = session.execute("""
                SELECT * FROM ks.test1
                WHERE user='milou'
        """)

        self.assertEqual(len(res), 1)
        session.execute("""DROP TABLE test1""")

    def max_column_value_size_test(self):
        cluster = self.prepare()
        cluster.populate(1).start()
        node = cluster.nodelist()[0]

        session = self.patient_cql_connection(node)
        self.create_ks(session, 'ks', 1)

        size = 1
        for i in range(int(math.log(MAX_BLOB_SIZE, 2))):
            size = size << 1
            self._do_test_blob_size(session, node, size - 1);

    def _do_test_max_columns(self, session, node, count):
        print("Testing maximum numbers of columns with count %i" % count)
        keys = ""
        keys_create = ""
        for i in range(count):
            keys += "key" + str(i) + ", "
            keys_create += "key" + str(i) + " int, "
        values = "1, " * count
        keys = keys

        c = """CREATE TABLE test1 (%s blub int PRIMARY KEY,)""" % (keys_create)
        session.execute(c)

        c= "insert into ks.test1  (%s blub) values (%s 1);" % (keys, values)
        session.execute(c)

        res = session.execute("""DROP TABLE test1""")

    # this test colude issue #173 and issue #176
    # since we do an insert statement
    def max_columns_and_query_parameters_test(self):
        cluster = self.prepare()
        cluster.populate(1).start()
        node = cluster.nodelist()[0]

        session = self.patient_cql_connection(node)
        self.create_ks(session, 'ks', 1)

        count = 1
        for i in range(int(math.log(MAX_COLUMNS, 2))):
            count = count << 1
            self._do_test_max_columns(session, node, i - 1)

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
            """ % (t)
        session.execute(c)

        c= "INSERT INTO stuff (k, v) VALUES(0, (%s));" % (v)
        session.execute(c)

        c= "SELECT * FROM STUFF;"
        res = session.execute(c)
        self.assertEqual(len(res), 1)

        session.execute("""DROP TABLE stuff""")

    def max_tuple_test(self):
        cluster = self.prepare()
        cluster.populate(1).start()
        node = cluster.nodelist()[0]

        session = self.patient_cql_connection(node)
        self.create_ks(session, 'ks', 1)

        count = 1
        for i in range(int(math.log(MAX_TUPLES, 2))):
            count = count << 1
            self._do_test_max_tuples(session, node, count - 1)


    def _do_test_max_batch_size(self, session, node, count):
        print("Testing max batch size for size=%i" % count)
        # in the future embed a blob in this
        # so the batch will be 64K
        c = """
            CREATE TABLE stuff (
              k int PRIMARY KEY,
            );
            """
        session.execute(c)

        c = "BEGIN UNLOGGED  BATCH\n"
        for i in range(count):
            c += "INSERT INTO stuff (k) VALUES(%i)\n" % (i)
        c += "APPLY BATCH;\n"

        session.execute(c)

        c= "SELECT * FROM STUFF;"
        res = session.execute(c)
        self.assertEqual(len(res), count)

        session.execute("""DROP TABLE STUFF""")

    def max_batch_size_test(self):
        cluster = self.prepare()
        cluster.populate(1).start()
        node = cluster.nodelist()[0]

        session = self.patient_cql_connection(node)
        self.create_ks(session, 'ks', 1)

        size = 1
        for i in range(int(math.log(MAX_BATCH_SIZE, 2))):
            size = size << 1
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

        c = """CREATE TABLE test1 (%s blub int PRIMARY KEY,)""" % (keys_create)
        session.execute(c)

        batch_size = MAX_CELLS_BATCH_SIZE
        rows = cells / columns
        c = "BEGIN UNLOGGED  BATCH\n"
        for i in range(rows):
            c += "insert into ks.test1  (%s blub) values (%s %i);\n" % (keys, values, i)
            if i % batch_size == 0:
                c += "APPLY BATCH;\n"
                session.execute(c)
                c = "BEGIN UNLOGGED  BATCH\n"

        session.execute("""DROP TABLE test1""")

    def max_cells_test(self):
        cluster = self.prepare()
        cluster.populate(1).start()
        node = cluster.nodelist()[0]

        session = self.patient_cql_connection(node)
        self.create_ks(session, 'ks', 1)

        cells = 1
        for i in range(int(math.log(MAX_CELLS, 2))):
            cells = cells << 1
            self._do_test_max_cell_count(session, node, cells - 1)
