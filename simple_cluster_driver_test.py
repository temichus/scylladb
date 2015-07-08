from dtest import Tester, debug
from tools import since
import subprocess, tempfile, os, shutil
import time
from ccmlib.urchin_cluster import UrchinCluster
from cassandra.query import SimpleStatement
from cassandra import ConsistencyLevel

@since('3.0')
class TestSimpleCluster(Tester):

    __urchin_args__=[]

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
        jvm_args=[]
        if type(cluster) is UrchinCluster:
           jvm_args=self.__urchin_args__
        cluster.populate(3).start(jvm_args=jvm_args)
        node1,node2,node3 = cluster.nodelist()
        session1 = self.patient_cql_connection(node1)
        session2 = self.patient_cql_connection(node2)
        session3 = self.patient_cql_connection(node3)
        self.create_ks(session1, 'ks', 3)
        time.sleep(1)

        session2.execute("""
            CREATE TABLE ks.test1 (
                k int PRIMARY KEY,
                c int
            )
        """)
        time.sleep(1)

        insert = SimpleStatement("insert into ks.test1  (k,c) values (1,2);", consistency_level=ConsistencyLevel.QUORUM)
        session3.execute(insert)
        time.sleep(1)

        # Select
        query1 = SimpleStatement("SELECT * FROM ks.test1 WHERE k=1",consistency_level=ConsistencyLevel.QUORUM)
        res = session1.execute(query1) 
        assert len(res) == 1, res
        res = session2.execute(query1) 
        assert len(res) == 1, res
        res = session3.execute(query1) 
        assert len(res) == 1, res

        # Select
        query2 = SimpleStatement("SELECT * FROM ks.test1 WHERE k=2",consistency_level=ConsistencyLevel.QUORUM)
        res = session1.execute(query2)
        assert len(res) == 0, res
        res = session2.execute(query2)
        assert len(res) == 0, res
        res = session3.execute(query2)
        assert len(res) == 0, res

        time.sleep(10)
