from dtest import Tester, debug
from tools import since
import subprocess, tempfile, os, shutil
import time

@since('3.0')
class TestSimple(Tester):

#    __test__= False

    def prepare(self):
        """
        Sets up cluster to test against. Currently 3 CCM Nodes
        """
        cluster = self.cluster
        cluster.populate(1).start()
        node1 = cluster.nodelist()[0]
        session = self.patient_cql_connection(node1)
        self.create_ks(session, 'ks', 1)
        return session


    def simple_create_insert_select_test(self):
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE test1 (
                k int PRIMARY KEY,
                c int
            )
        """)

        cursor.execute("insert into test1  (k,c) values (1,2);")

        # Select
        res = cursor.execute("""
                SELECT * FROM test1
                WHERE k=1
        """)

        assert len(res) == 1, res

        # Select
        res = cursor.execute("""
                SELECT * FROM test1
                WHERE k=2
        """)

        assert len(res) == 0, res
