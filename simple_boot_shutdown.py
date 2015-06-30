from dtest import Tester, debug
from tools import since
import subprocess, tempfile, os, shutil
import time

@since('3.0')
class TestSimple(Tester):

#    __test__= False

    def prepare(self):
        """
        Sets up node to test against.
        """
        cluster = self.cluster
        return cluster


    def simple_create_insert_select_test(self):
        cluster = self.prepare()
        cluster.populate(1).start()
        node1 = cluster.nodelist()[0]

        cursor = self.patient_cql_connection(node1)
        self.create_ks(cursor, 'ks', 1)

        cursor.execute("""
            CREATE TABLE test1 (
                k int PRIMARY KEY,
                c int
            )
        """)

        node1.stop()
        node1.start(update_pid=False)       

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

