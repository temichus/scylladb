import time

from dtest import Tester


class TestSimpleBootShutdown(Tester):

    def prepare(self):
        """
        Sets up node to test against.
        """
        cluster = self.cluster
        return cluster

    def boot_create_keyspace_table_shutdown_boot_insert_select_test(self):
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

        # FIXME adding a delay after create to verify we are flushing complete information
        time.sleep(2)
        node1.stop()

        node1.start(update_pid=True)
        cursor = self.patient_cql_connection(node1, 'ks')

        cursor.execute("insert into ks.test1  (k,c) values (1,2);")

        # Select
        res = cursor.execute("""
                SELECT * FROM ks.test1
                WHERE k=1
        """)

        assert len(res) == 1, res

        # Select
        res = cursor.execute("""
                SELECT * FROM ks.test1
                WHERE k=2
        """)

        assert len(res) == 0, res

    def boot_create_keyspace_table_insert_shutdown_select_test(self):
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

        cursor.execute("insert into ks.test1  (k,c) values (1,2);")

        node1.flush()
        node1.stop()

        node1.start(update_pid=True)
        cursor = self.patient_cql_connection(node1, 'ks')
        # Select
        res = cursor.execute("""
                SELECT * FROM ks.test1
                WHERE k=1
        """)

        assert len(res) == 1, res

        # Select
        res = cursor.execute("""
                SELECT * FROM ks.test1
                WHERE k=2
        """)

        assert len(res) == 0, res

    def boot_create_keyspace_table_insert_shutdown_commitlog_replay_select_test(self):
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

        cursor.execute("insert into ks.test1  (k,c) values (1,2);")

        # wait for the commitlog to be fsynched
        time.sleep(10)
        node1.stop(gently=False)

        node1.start(update_pid=True)
        cursor = self.patient_cql_connection(node1, 'ks')
        # Select
        res = cursor.execute("""
                SELECT * FROM ks.test1
                WHERE k=1
        """)

        assert len(res) == 1, res

        # Select
        res = cursor.execute("""
                SELECT * FROM ks.test1
                WHERE k=2
        """)

        assert len(res) == 0, res
