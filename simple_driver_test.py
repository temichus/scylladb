import time

from ccmlib.scylla_cluster import ScyllaCluster

from dtest import Tester


class TestSimple(Tester):

    __test__ = False
    __scylla_args__ = []

    def __init__(self, *args, **kwargs):
        Tester.__init__(self, *args, **kwargs)

    def prepare(self):
        """
        Sets up cluster to test against.
        """
        cluster = self.cluster
        return cluster

    def simple_create_insert_select_test(self):
        cluster = self.prepare()
        jvm_args = []
        if type(cluster) is ScyllaCluster:
            jvm_args = self.__scylla_args__
        cluster.populate(1).start(jvm_args=jvm_args)
        node1 = cluster.nodelist()[0]
        session = self.patient_cql_connection(node1)
        self.create_ks(session, 'ks', 1)

        session.execute("""
            CREATE TABLE test1 (
                k int PRIMARY KEY,
                c int
            )
        """)

        session.execute("insert into test1  (k,c) values (1,2);")

        # Select
        res = session.execute("""
                SELECT * FROM test1
                WHERE k=1
        """)

        assert len(list(res)) == 1, list(res)

        # Select
        res = session.execute("""
                SELECT * FROM test1
                WHERE k=2
        """)

        assert len(list(res)) == 0, list(res)
        time.sleep(10)

    def simple_composite_partition_key_create_insert_select_test(self):
        cluster = self.prepare()
        jvm_args = []
        if type(cluster) is ScyllaCluster:
            jvm_args = self.__scylla_args__
        cluster.populate(1).start(jvm_args=jvm_args)
        node1 = cluster.nodelist()[0]
        session = self.patient_cql_connection(node1)
        self.create_ks(session, 'ks', 1)

        session.execute("""
            CREATE TABLE test1 (
                k1 int,
                k2 int,
                c int,
                PRIMARY KEY ((k1,k2))
            )
        """)

        session.execute("insert into test1 (k1,k2,c) values (1,1,3);")
        session.execute("insert into test1 (k1,k2,c) values (1,2,4);")

        # Select
        res = session.execute("""
                SELECT * FROM test1
                WHERE k1=1 and k2=1
        """)
        assert len(list(res)) == 1, list(res)

        res = session.execute("""
                SELECT * FROM test1
                WHERE k1=1 and k2=2
        """)
        assert len(list(res)) == 1, list(res)

        # Select
        res = session.execute("""
                SELECT * FROM test1
                WHERE k1=2 and k2=1
        """)
        assert len(list(res)) == 0, list(res)

        res = session.execute("""
                SELECT * FROM test1
                WHERE k1=1 and k2=3
        """)
        assert len(list(res)) == 0, list(res)

        time.sleep(1)

    def simple_compound_primary_key_create_insert_select_test(self):
        cluster = self.prepare()
        jvm_args = []
        if type(cluster) is ScyllaCluster:
            jvm_args = self.__scylla_args__
        cluster.populate(1).start(jvm_args=jvm_args)
        node1 = cluster.nodelist()[0]
        session = self.patient_cql_connection(node1)
        self.create_ks(session, 'ks', 1)

        session.execute("""
            CREATE TABLE test1 (
                k1 int,
                c1 int,
                c2 int,
                c3 int,
                PRIMARY KEY (k1,c1,c2)
            )
        """)

        session.execute("insert into test1 (k1,c1,c2,c3) values (1,1,1,1);")
        session.execute("insert into test1 (k1,c1,c2,c3) values (1,1,2,2);")

        # Select
        res = session.execute("""
                SELECT * FROM test1
                WHERE k1=1
        """)
        assert len(list(res)) == 2, list(res)

        res = session.execute("""
                SELECT * FROM test1
                WHERE k1=1 and c1=1 and c2=1
        """)
        assert len(list(res)) == 1, list(res)

        # Select
        res = session.execute("""
                SELECT * FROM test1
                WHERE k1=1 and c1=2
        """)

        res = session.execute("""
                SELECT * FROM test1
                WHERE k1=1 and c1=1 and c2=3
        """)
        assert len(list(res)) == 0, list(res)

        time.sleep(1)


options = {'Single': ['--smp', '1'], 'SMP': ['--smp', '2']}

for option in options.keys():
    cls_name = ('SimpleDriverTest_with_' + option)
    vars()[cls_name] = type(cls_name, (TestSimple,), {'__scylla_args__': options[option], '__test__': True})
