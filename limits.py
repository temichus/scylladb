from dtest import Tester

# Those are ideal values according to c* specifications
# MAX_KEY_SIZE = 65506
# MAX_BLOB_SIZE = (1024*1024*1024*1-1)
# MAX_COLUMNS = 65535
# MAX_TUPLES = 32768
# MAX_BATCH_SIZE = 65535
# MAX_CELLS_COLUMNS = 32768
# MAX_CELLS_BATCH_SIZE = 1000
# MAX_CELLS = (2*1024*1024*1024-1)

# Those are value use to validate the tests code
MAX_KEY_SIZE = 1000
MAX_BLOB_SIZE = 1000
MAX_COLUMNS = 1000
MAX_TUPLES = 1000
MAX_BATCH_SIZE = 1000
MAX_CELLS_COLUMNS = 100
MAX_CELLS_BATCH_SIZE = 100
MAX_CELLS = 1000

class TestLimits(Tester):

    def prepare(self):
        """
        Sets up node to test against.
        """
        cluster = self.cluster
        return cluster

    def max_key_length_test(self):
        cluster = self.prepare()
        cluster.populate(1).start()
        node1 = cluster.nodelist()[0]

        session = self.patient_cql_connection(node1)
        self.create_ks(session, 'ks', 1)

        # biggest that will currently works in scylla
        #key_name = "k" * 32766

        # origin max key size
        key_name = "k" * MAX_KEY_SIZE

        session.execute("""
            CREATE TABLE test1 (
                %s int PRIMARY KEY,
            )
        """ % (key_name))

        session.execute("insert into ks.test1  (%s) values (1);" % (key_name))
        session.execute("insert into ks.test1  (%s) values (2);" % (key_name))

        node1.flush()
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

    def max_column_value_size_test(self):
        cluster = self.prepare()
        cluster.populate(1).start()
        node1 = cluster.nodelist()[0]

        session = self.patient_cql_connection(node1)
        self.create_ks(session, 'ks', 1)

        blob_a = ("a" * MAX_BLOB_SIZE)
        blob_b = ("b" * MAX_BLOB_SIZE)

        session.execute("""
            CREATE TABLE test1 (
                user ascii PRIMARY KEY,
                payload blob,
            )
        """)

        session.execute("insert into ks.test1  (user, payload) values ('tintin', textAsBlob('%s'));" % (blob_a))
        session.execute("insert into ks.test1  (user, payload) values ('milou', textAsBlob('%s'));" % (blob_b))

        node1.flush()
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

    # this test colude issue #173 and issue #176
    # since we do an insert statement
    def max_columns_and_query_parameters_test(self):
        cluster = self.prepare()
        cluster.populate(1).start()
        node1 = cluster.nodelist()[0]

        session = self.patient_cql_connection(node1)
        self.create_ks(session, 'ks', 1)

        # scylla beat cassandra on this one
        count = MAX_COLUMNS

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

    def max_tuple_test(self):
        cluster = self.prepare()
        cluster.populate(1).start()
        node1 = cluster.nodelist()[0]

        session = self.patient_cql_connection(node1)
        self.create_ks(session, 'ks', 1)

        count = MAX_TUPLES

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

    def max_batch_size_test(self):
        cluster = self.prepare()
        cluster.populate(1).start()
        node1 = cluster.nodelist()[0]

        session = self.patient_cql_connection(node1)
        self.create_ks(session, 'ks', 1)

        # in the future embed a blob in this
        # so the batch will be 2GB
        c = """
            CREATE TABLE stuff (
              k int PRIMARY KEY,
            );
            """
        session.execute(c)

        count = MAX_BATCH_SIZE

        c = "BEGIN UNLOGGED  BATCH\n"
        for i in range(count):
            c += "INSERT INTO stuff (k) VALUES(%i)\n" % (i)
        c += "APPLY BATCH;\n"

        session.execute(c)

        c= "SELECT * FROM STUFF;"
        res = session.execute(c)
        self.assertEqual(len(res), count)

    def max_cells_test(self):
        cluster = self.prepare()
        cluster.populate(1).start()
        node1 = cluster.nodelist()[0]

        session = self.patient_cql_connection(node1)
        self.create_ks(session, 'ks', 1)

        columns = MAX_CELLS_COLUMNS

        keys = ""
        keys_create = ""
        for i in range(columns):
            keys += "key" + str(i) + ", "
            keys_create += "key" + str(i) + " int, "
        values = "1, " * columns

        c = """CREATE TABLE test1 (%s blub int PRIMARY KEY,)""" % (keys_create)
        session.execute(c)

        batch_size = MAX_CELLS_BATCH_SIZE
        rows = MAX_CELLS / columns
        c = "BEGIN UNLOGGED  BATCH\n"
        for i in range(rows):
            c += "insert into ks.test1  (%s blub) values (%s %i);\n" % (keys, values, i)
            if i % batch_size == 0:
                c += "APPLY BATCH;\n"
                session.execute(c)
                c = "BEGIN UNLOGGED  BATCH\n"
