from dtest import Tester


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
        key_name = "k" * 65506

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

        # What is origin single column value size
        # https://wiki.apache.org/cassandra/CassandraLimitations
        blob_a = ("a" * (1024*1024*1024*1-1))
        blob_b = ("b" * (1024*1024*1024*1-1))

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

    def max_columns_test(self):
        cluster = self.prepare()
        cluster.populate(1).start()
        node1 = cluster.nodelist()[0]

        session = self.patient_cql_connection(node1)
        self.create_ks(session, 'ks', 1)

        # scylla beat cassandra on this one
        count = 32768

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

        count = 32768

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
