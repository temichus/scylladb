from dtest import Tester, debug
from tools import since
import subprocess, tempfile, os, shutil
import time
from ccmlib.urchin_cluster import UrchinCluster

@since('3.0')
class TestSimple(Tester):

    __test__= False
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

        assert len(res) == 1, res

        # Select
        res = session.execute("""
                SELECT * FROM test1
                WHERE k=2
        """)

        assert len(res) == 0, res
        time.sleep(10)


options = {'Single' : ['--smp','1'], 'SMP' : ['--smp','2']}

for option in options.keys():
    cls_name = ('SimpleDriverTest_with_' + option)
    vars()[cls_name] = type(cls_name, (TestSimple,), {'__urchin_args__': options[option], '__test__':True})
