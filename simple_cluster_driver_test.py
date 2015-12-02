import time

from cassandra import Unavailable, ConsistencyLevel
from cassandra.query import SimpleStatement
from ccmlib.scylla_cluster import ScyllaCluster

from dtest import Tester, debug, freshCluster


class TestSimpleCluster(Tester):

    __scylla_args__ = []

    def __init__(self, *args, **kwargs):
        Tester.__init__(self, *args, **kwargs)

    def prepare(self):
        """
        Sets up cluster to test against.
        """
        cluster = self.cluster
        return cluster

    @freshCluster()
    def simple_create_insert_select_test(self):
        cluster = self.prepare()
        jvm_args = []
        if type(cluster) is ScyllaCluster:
            jvm_args = self.__scylla_args__
        cluster.populate(3).start(jvm_args=jvm_args)
        time.sleep(3)
        node1, node2, node3 = cluster.nodelist()
        session1 = self.patient_cql_connection(node1)
        session2 = self.patient_cql_connection(node2)
        session3 = self.patient_cql_connection(node3)
        self.create_ks(session1, 'ks', 3)

        session2.execute("""
            CREATE TABLE ks.test1 (
                k int PRIMARY KEY,
                c int
            )
        """)

        insert = SimpleStatement("insert into ks.test1  (k,c) values (1,2);", consistency_level=ConsistencyLevel.QUORUM)
        session3.execute(insert)

        # Select
        query1 = SimpleStatement("SELECT * FROM ks.test1 WHERE k=1", consistency_level=ConsistencyLevel.QUORUM)
        res = session1.execute(query1)
        assert len(res) == 1, res
        res = session2.execute(query1)
        assert len(res) == 1, res
        res = session3.execute(query1)
        assert len(res) == 1, res

        # Select
        query2 = SimpleStatement("SELECT * FROM ks.test1 WHERE k=2", consistency_level=ConsistencyLevel.QUORUM)
        res = session1.execute(query2)
        assert len(res) == 0, res
        res = session2.execute(query2)
        assert len(res) == 0, res
        res = session3.execute(query2)
        assert len(res) == 0, res

        time.sleep(10)

    def clname(self, cl):
        map = {
            ConsistencyLevel.ANY: 'ANY',
            ConsistencyLevel.ONE: 'ONE',
            ConsistencyLevel.TWO: 'TWO',
            ConsistencyLevel.THREE: 'THREE',
            ConsistencyLevel.QUORUM: 'QUORUM',
            ConsistencyLevel.ALL: 'ALL'
        }
        return map[cl]

    def simple_consistency_level_validate(self, session, node, read_keys, read_cls_pass, read_cls_fail, read_cls_pass_all, write_keys, write_cls_pass, write_cls_fail, write_cls_pass_all):
        for cl in read_cls_pass:
            debug("read %s cl %s" % (node, self.clname(cl)))
            read_pass = 0
            read_fail = 0
            for key in read_keys:
                try:
                    query1 = SimpleStatement("SELECT * FROM ks.test1 WHERE k=%s" % key, consistency_level=cl)
                    res = session.execute(query1)
                    assert len(res) == 1, res
                    read_pass = read_pass + 1
                except Exception as ex:
                    read_fail = read_fail + 1
            assert (read_fail == 0 or not read_cls_pass_all), "Expected all reads to pass, pass %s, fail %s" % (read_pass, read_fail)
            assert (read_fail > 0 or read_cls_pass_all), "Expected some reads to fail, pass %s, fail %s" % (read_pass, read_fail)

        for cl in read_cls_fail:
            query1 = SimpleStatement("SELECT * FROM ks.test1 WHERE k=1", consistency_level=cl)
            try:
                res = sessionexecute(query1)
                assert("Consistency level %s is not possible" % self.clname(cl))
            except Unavailable as ex:
                assert(True)
            except Exception as ex:
                assert("Expected cassandra.Unavilable exception received %s" % ex)

        for cl in write_cls_pass:
            debug("write %s cl %s" % (node, self.clname(cl)))
            write_pass = 0
            write_fail = 0
            for key in write_keys:
                try:
                    insert = SimpleStatement("insert into ks.test1  (k,c) values (%s,%s)" % (key, key), consistency_level=cl)
                    res = session.execute(insert)
                    write_pass = write_pass + 1
                except Exception as ex:
                    write_fail = write_fail + 1
            assert (write_fail == 0 or not write_cls_pass_all), "Expected all writes to pass, pass %s, fail %s" % (write_pass, write_fail)
            assert (write_fail > 0 or write_cls_pass_all), "Expected some writes to fail, pass %s, fail %s" % (write_pass, write_fail)

        for cl in write_cls_fail:
            insert = SimpleStatement("insert into ks.test1 (k,c) values (101,101)", consistency_level=cl)
            try:
                res = sessionexecute(insert)
                assert("Consistency level %s is not possible" % self.clname(cl))
            except Unavailable as ex:
                assert(True)
            except Exception as ex:
                assert("Expected cassandra.Unavilable exception received %s" % ex)

    def prepare_cluster(self, ks_rf):
        cluster = self.prepare()
        jvm_args = []
        if type(cluster) is ScyllaCluster:
            jvm_args = self.__scylla_args__
        cluster.populate(3).start(jvm_args=jvm_args)
        time.sleep(3)
        node1, node2, node3 = cluster.nodelist()
        session1 = self.patient_cql_connection(node1)
        session2 = self.patient_cql_connection(node2)
        session3 = self.patient_cql_connection(node3)

        self.create_ks(session1, 'ks', ks_rf)
        session1.execute("""
            CREATE TABLE ks.test1 (
                k int PRIMARY KEY,
                c int
            )
        """)
        return cluster

    @freshCluster()
    def simple_rf_3_consistency_level_tests(self):
        cluster = self.prepare_cluster(3)
        node1, node2, node3 = cluster.nodelist()
        session1 = self.patient_cql_connection(node1)
        session2 = self.patient_cql_connection(node2)
        session3 = self.patient_cql_connection(node3)

        keys = range(1, 100)
        for val in keys:
            insert = SimpleStatement("insert into ks.test1  (k,c) values (%s,%s)" % (val, val), consistency_level=ConsistencyLevel.ALL)
            session1.execute(insert)

        debug("3 nodes, node1,node2,node3 are running")
        read_cls_pass = [ConsistencyLevel.ONE, ConsistencyLevel.TWO, ConsistencyLevel.THREE, ConsistencyLevel.QUORUM, ConsistencyLevel.ALL]
        write_cls_pass = [ConsistencyLevel.ANY, ConsistencyLevel.ONE, ConsistencyLevel.TWO, ConsistencyLevel.THREE, ConsistencyLevel.QUORUM, ConsistencyLevel.ALL]
        self.simple_consistency_level_validate(session1, "node 1", keys, read_cls_pass, [], True, range(101, 200), write_cls_pass, [], True)
        self.simple_consistency_level_validate(session2, "node 2", keys, read_cls_pass, [], True, range(201, 300), write_cls_pass, [], True)
        self.simple_consistency_level_validate(session3, "node 3", keys, read_cls_pass, [], True, range(301, 400), write_cls_pass, [], True)

        node1.stop()
        # FIXME - currently used to make sure that the gossiper has concluded the node is dead - on origin it works without this
        time.sleep(60)
        debug("node 1 stopped, node2,node3 are running")
        read_cls_pass = [ConsistencyLevel.ONE, ConsistencyLevel.TWO, ConsistencyLevel.QUORUM]
        read_cls_fail = [ConsistencyLevel.THREE, ConsistencyLevel.ALL]
        write_cls_pass = [ConsistencyLevel.ANY, ConsistencyLevel.ONE, ConsistencyLevel.TWO, ConsistencyLevel.QUORUM]
        write_cls_fail = [ConsistencyLevel.THREE, ConsistencyLevel.ALL]
        self.simple_consistency_level_validate(session2, "node 2", keys, read_cls_pass, read_cls_fail, True, range(401, 500), write_cls_pass, write_cls_fail, True)
        self.simple_consistency_level_validate(session3, "node 3", keys, read_cls_pass, read_cls_fail, True, range(501, 600), write_cls_pass, write_cls_fail, True)

        node2.stop()
        # FIXME - currently used to make sure that the gossiper has concluded the node is dead - on origin it works without this
        time.sleep(60)
        debug("node 2 stopped, node3 is running")
        read_cls_pass = [ConsistencyLevel.ONE]
        read_cls_fail = [ConsistencyLevel.TWO, ConsistencyLevel.THREE, ConsistencyLevel.QUORUM, ConsistencyLevel.ALL]
        write_cls_pass = [ConsistencyLevel.ANY, ConsistencyLevel.ONE]
        write_cls_fail = [ConsistencyLevel.TWO, ConsistencyLevel.THREE, ConsistencyLevel.QUORUM, ConsistencyLevel.ALL]
        self.simple_consistency_level_validate(session3, "node 3", keys, read_cls_pass, read_cls_fail, True, range(601, 700), write_cls_pass, write_cls_fail, True)

        # should add additional tests once a node can be entered back into a cluster

    @freshCluster()
    def simple_rf_1_consistency_level_tests(self):
        cluster = self.prepare_cluster(1)
        node1, node2, node3 = cluster.nodelist()
        session1 = self.patient_cql_connection(node1)
        session2 = self.patient_cql_connection(node2)
        session3 = self.patient_cql_connection(node3)

        keys = range(1, 100)
        for val in keys:
            insert = SimpleStatement("insert into ks.test1  (k,c) values (%s,%s)" % (val, val), consistency_level=ConsistencyLevel.ALL)
            session1.execute(insert)

        debug("3 nodes, node1,node2,node3 are running")
        read_cls_pass = [ConsistencyLevel.ONE, ConsistencyLevel.QUORUM, ConsistencyLevel.ALL]
        read_cls_fail = [ConsistencyLevel.TWO, ConsistencyLevel.THREE]
        write_cls_pass = [ConsistencyLevel.ANY, ConsistencyLevel.ONE, ConsistencyLevel.QUORUM, ConsistencyLevel.ALL]
        write_cls_fail = [ConsistencyLevel.TWO, ConsistencyLevel.THREE]
        self.simple_consistency_level_validate(session1, "node 1", keys, read_cls_pass, read_cls_fail, True, range(101, 200), write_cls_pass, write_cls_fail, True)
        self.simple_consistency_level_validate(session2, "node 2", keys, read_cls_pass, read_cls_fail, True, range(201, 300), write_cls_pass, write_cls_fail, True)
        self.simple_consistency_level_validate(session3, "node 3", keys, read_cls_pass, read_cls_fail, True, range(301, 400), write_cls_pass, write_cls_fail, True)

        node1.stop()
        debug("node 1 stopped, node2,node3 are running")
        read_cls_pass = [ConsistencyLevel.ONE, ConsistencyLevel.QUORUM, ConsistencyLevel.ALL]
        read_cls_fail = [ConsistencyLevel.TWO, ConsistencyLevel.THREE]
        write_cls_pass = [ConsistencyLevel.ONE, ConsistencyLevel.QUORUM, ConsistencyLevel.ALL]
        write_cls_fail = [ConsistencyLevel.TWO, ConsistencyLevel.THREE]
        self.simple_consistency_level_validate(session2, "node 2", keys, read_cls_pass, read_cls_fail, False, range(401, 500), write_cls_pass, write_cls_fail, False)
        self.simple_consistency_level_validate(session3, "node 3", keys, read_cls_pass, read_cls_fail, False, range(501, 600), write_cls_pass, write_cls_fail, False)

        node2.stop()
        debug("node 2 stopped, node3 is running")
        read_cls_pass = [ConsistencyLevel.ONE, ConsistencyLevel.QUORUM, ConsistencyLevel.ALL]
        read_cls_fail = [ConsistencyLevel.TWO, ConsistencyLevel.THREE]
        write_cls_pass = [ConsistencyLevel.ONE, ConsistencyLevel.QUORUM, ConsistencyLevel.ALL]
        write_cls_fail = [ConsistencyLevel.TWO, ConsistencyLevel.THREE]
        self.simple_consistency_level_validate(session3, "node 3", keys, read_cls_pass, read_cls_fail, False, range(601, 700), write_cls_pass, write_cls_fail, False)

        # should add additional tests once a node can be entered back into a cluster

    def simple_query_validate(self, session, node, read_keys, query, read_cls_pass, read_cls_fail, read_cls_pass_all):
        for cl in read_cls_pass:
            debug("read %s cl %s" % (node, self.clname(cl)))
            read_pass = 0
            read_fail = 0
            errors = []
            try:
                query1 = SimpleStatement(query, consistency_level=cl)
                res = session.execute(query1)
                assert len(res) == read_keys, "got %s expected %s " % (len(res), read_keys) + " : " + str(res)
                read_pass = read_pass + 1
            except Exception as ex:
                read_fail = read_fail + 1
                errors = errors + [ex]
            assert (read_fail == 0 or not read_cls_pass_all), "Expected all reads to pass, pass %s, fail %s" % (read_pass, read_fail) + str(errors)
            assert (read_fail > 0 or read_cls_pass_all), "Expected some reads to fail, pass %s, fail %s" % (read_pass, read_fail)

        for cl in read_cls_fail:
            query1 = SimpleStatement(query, consistency_level=cl)
            try:
                res = sessionexecute(query1)
                assert("Consistency level %s is not possible" % self.clname(cl))
            except Unavailable as ex:
                assert(True)
            except Exception as ex:
                assert("Expected cassandra.Unavilable exception received %s" % ex)

    @freshCluster()
    def simple_rf_1_query_tests(self):
        cluster = self.prepare_cluster(1)

        node1, node2, node3 = cluster.nodelist()
        session1 = self.patient_cql_connection(node1)
        session2 = self.patient_cql_connection(node2)
        session3 = self.patient_cql_connection(node3)

        keys = range(1, 100)
        for val in keys:
            insert = SimpleStatement("insert into ks.test1  (k,c) values (%s,%s)" % (val, val), consistency_level=ConsistencyLevel.ALL)
            session1.execute(insert)

        debug("3 nodes, node1,node2,node3 are running")
        read_cls_pass = [ConsistencyLevel.ONE, ConsistencyLevel.QUORUM, ConsistencyLevel.ALL]
        read_cls_fail = [ConsistencyLevel.TWO, ConsistencyLevel.THREE]
        self.simple_query_validate(session1, "node 1", len(keys), "SELECT * FROM ks.test1", read_cls_pass, read_cls_fail, True)
        self.simple_query_validate(session1, "node 1", 10, "SELECT * FROM ks.test1 where k in (1,10,20,30,40,50,60,70,80,90) ", read_cls_pass, read_cls_fail, True)
        self.simple_query_validate(session2, "node 2", len(keys), "SELECT * FROM ks.test1", read_cls_pass, read_cls_fail, True)
        self.simple_query_validate(session2, "node 2", 10, "SELECT * FROM ks.test1 where k in (1,10,20,30,40,50,60,70,80,90) ", read_cls_pass, read_cls_fail, True)
        self.simple_query_validate(session3, "node 3", len(keys), "SELECT * FROM ks.test1", read_cls_pass, read_cls_fail, True)
        self.simple_query_validate(session3, "node 3", 10, "SELECT * FROM ks.test1 where k in (1,10,20,30,40,50,60,70,80,90) ", read_cls_pass, read_cls_fail, True)

    @freshCluster()
    def simple_rf_3_query_tests(self):
        cluster = self.prepare_cluster(3)

        node1, node2, node3 = cluster.nodelist()
        session1 = self.patient_cql_connection(node1)
        session2 = self.patient_cql_connection(node2)
        session3 = self.patient_cql_connection(node3)

        keys = range(1, 100)
        for val in keys:
            insert = SimpleStatement("insert into ks.test1  (k,c) values (%s,%s)" % (val, val), consistency_level=ConsistencyLevel.ALL)
            session1.execute(insert)

        debug("3 nodes, node1,node2,node3 are running")
        read_cls_pass = [ConsistencyLevel.ONE, ConsistencyLevel.TWO, ConsistencyLevel.THREE, ConsistencyLevel.QUORUM, ConsistencyLevel.ALL]
        self.simple_query_validate(session1, "node 1", len(keys), "SELECT * FROM ks.test1", read_cls_pass, [], True)
        self.simple_query_validate(session1, "node 1", 10, "SELECT * FROM ks.test1 where k in (1,10,20,30,40,50,60,70,80,90) ", read_cls_pass, [], True)
        self.simple_query_validate(session2, "node 2", len(keys), "SELECT * FROM ks.test1", read_cls_pass, [], True)
        self.simple_query_validate(session2, "node 2", 10, "SELECT * FROM ks.test1 where k in (1,10,20,30,40,50,60,70,80,90) ", read_cls_pass, [], True)
        self.simple_query_validate(session3, "node 3", len(keys), "SELECT * FROM ks.test1", read_cls_pass, [], True)
        self.simple_query_validate(session3, "node 3", 10, "SELECT * FROM ks.test1 where k in (1,10,20,30,40,50,60,70,80,90) ", read_cls_pass, [], True)
