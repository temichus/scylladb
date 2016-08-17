import time

from dtest import Tester


class PersistenceTest(Tester):
    """
    Insert data into clusters, then restart them and verify if data persisted.
    """
    def restart_cluster(self):
        self.cluster.stop()
        time.sleep(0.5)
        self.cluster.start(wait_for_binary_proto=True)
        time.sleep(0.5)

    def prepare(self, nodes=1):
        cluster = self.cluster

        if not cluster.nodelist():
            cluster.populate(nodes).start(wait_for_binary_proto=True)
        node1 = cluster.nodelist()[0]

        session = self.patient_cql_connection(node1)
        return session

    def stress_with_col_size(self, size):
        self.cluster.populate(1).start(wait_for_binary_proto=True)
        node = self.cluster.nodelist()[0]
        node.stress(['write', 'n=5', "no-warmup", "cl=ALL", "-pop",
                     "seq=1...5", "-schema", "replication(factor=1)",
                     "-col", "n=fixed(1)", "size=fixed(%s)" % size, "-rate",
                     "threads=1"])
        self.restart_cluster()
        node.stress(['read', 'n=5', "no-warmup", "cl=ALL", "-pop", "seq=1...5",
                     "-schema", "replication(factor=1)", "-col", "n=fixed(1)",
                     "size=fixed(%s)" % size, "-rate", "threads=1"])

    def test_persist_simple(self):
        """
        1) Create a 1 node cluster.
        2) Create a keyspace and a table with simple schema.
        3) Add data to the table.
        4) Query table and save the result.
        5) Restart the node.
        6) Query table, compare with previous result.
        """
        session = self.prepare()
        session.execute("CREATE KEYSPACE ks WITH replication={'class':'SimpleStrategy', 'replication_factor':1}")
        session.execute("CREATE COLUMNFAMILY ks.cf (p1 text, c1 text, r1 int, PRIMARY KEY (p1, c1)) WITH compaction={'class': 'SizeTieredCompactionStrategy'}")
        session.execute("INSERT INTO ks.cf (p1, c1, r1) VALUES ('key1', 'a', 1)")
        session.execute("INSERT INTO ks.cf (p1, c1, r1) VALUES ('key2', 'b', 1)")
        res_before_restart = list(session.execute("SELECT * FROM ks.cf"))
        self.restart_cluster()
        session2 = self.prepare()
        res_after_restart = list(session2.execute("SELECT * FROM ks.cf"))
        self.assertEqual(res_before_restart, res_after_restart)

    def test_persist_small_columns(self):
        """
        1) Create a 1 node cluster.
        2) Run cassandra-stress with column size=1 (read workload) to
           populate the DB.
        3) Restart the node.
        4) Run a read workload, to see if data was persisted.
        """
        self.stress_with_col_size(1)
