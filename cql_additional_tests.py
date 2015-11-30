from dtest import Tester


class CQLAdditionalTests(Tester):

    def prepare(self):
        """
        Sets up cluster to test against.
        """
        cluster = self.cluster
        cluster.populate(1).start()
        return cluster


    def simple_null_value_tests(self):
        cluster = self.prepare()
        node1 = cluster.nodelist()[0]
        session = self.patient_cql_connection(node1)
        self.create_ks(session, 'ks', 1)

        session.execute("""
             CREATE TABLE foobar ( key text PRIMARY KEY , val1 text , val2 float );
        """)

        update = session.prepare("UPDATE foobar SET val1 = ?, val2 = ? WHERE key = ?;")
        session.execute(update.bind(("ccc", 1.0, "java1")))
        session.execute(update.bind((None, 1.0, "java2")))
        session.execute(update.bind((None, None, "java3")))
        session.execute(update.bind(("ddd", None, "java4")))

        res = session.execute("""
                SELECT * FROM foobar
        """)
        assert len(res) == 3, res
