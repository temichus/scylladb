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
