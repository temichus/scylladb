import time

import pytest
from dtest_class import Tester, create_ks
from tools import since, rows_to_list
from tools.assertions import assert_invalid, assert_all, assert_one
from cassandra.concurrent import execute_concurrent


@pytest.mark.dtest_full
@pytest.mark.single_node
class TestSchema(Tester):

    def drop_column_compact_test(self):
        session = self.prepare()

        session.execute("USE ks")
        session.execute("CREATE TABLE cf (key int PRIMARY KEY, c1 int, c2 int) WITH COMPACT STORAGE")

        assert_invalid(session, "ALTER TABLE cf DROP c1", "Cannot drop columns from a")

    @pytest.mark.next_gating
    @pytest.mark.dtest_debug
    def test_drop_column_compaction(self):
        session = self.prepare()
        session.execute("USE ks")
        session.execute("CREATE TABLE cf (key int PRIMARY KEY, c1 int, c2 int)")

        # insert some data.
        session.execute("INSERT INTO cf (key, c1, c2) VALUES (0, 1, 2)")
        session.execute("INSERT INTO cf (key, c1, c2) VALUES (1, 2, 3)")
        session.execute("INSERT INTO cf (key, c1, c2) VALUES (2, 3, 4)")

        # drop and readd c1.
        session.execute("ALTER TABLE cf DROP c1")
        session.execute("ALTER TABLE cf ADD c1 int")

        # add another row.
        session.execute("INSERT INTO cf (key, c1, c2) VALUES (3, 4, 5)")

        node = self.cluster.nodelist()[0]
        node.flush()
        node.compact()

        # test that c1 values have been compacted away.
        session = self.patient_cql_connection(node)
        assert_all(session, "SELECT c1 FROM ks.cf", [[None], [None], [None], [4]], ignore_order=True)

    @pytest.mark.next_gating
    @pytest.mark.dtest_debug
    def drop_column_queries_test(self):
        session = self.prepare()

        session.execute("USE ks")
        session.execute("CREATE TABLE cf (key int PRIMARY KEY, c1 int, c2 int)")
        # FIXME: ScyllaDB: Indexes not supported yet
        # session.execute("CREATE INDEX ON cf(c2)")

        # insert some data.
        session.execute("INSERT INTO cf (key, c1, c2) VALUES (0, 1, 2)")
        session.execute("INSERT INTO cf (key, c1, c2) VALUES (1, 2, 3)")
        session.execute("INSERT INTO cf (key, c1, c2) VALUES (2, 3, 4)")

        # drop and readd c1.
        session.execute("ALTER TABLE cf DROP c1")
        session.execute("ALTER TABLE cf ADD c1 int")

        # add another row.
        session.execute("INSERT INTO cf (key, c1, c2) VALUES (3, 4, 5)")

        # test that old (pre-drop) c1 values aren't returned and new ones are.
        assert_all(session, "SELECT c1 FROM cf", [[None], [None], [None], [4]], ignore_order=True)
        assert_all(session, "SELECT * FROM cf", [[0, None, 2], [1, None, 3], [2, None, 4], [3, 4, 5]],
                   ignore_order=True)
        assert_one(session, "SELECT c1 FROM cf WHERE key = 0", [None])

        assert_one(session, "SELECT c1 FROM cf WHERE key = 3", [4])

        # FIXME: ScyllaDB: Indexes not supported yet
        # rows = session.execute("SELECT * FROM cf WHERE c2 = 2")
        # self.assertEqual([[0, None, 2]], rows_to_list(rows))

        # FIXME: ScyllaDB: Indexes not supported yet
        # rows = session.execute("SELECT * FROM cf WHERE c2 = 5")
        # self.assertEqual([[3, 4, 5]], rows_to_list(rows))

    def prepare(self):
        cluster = self.cluster
        cluster.populate(1).start()
        time.sleep(.5)
        nodes = cluster.nodelist()
        session = self.patient_cql_connection(nodes[0])
        # It is forbidden to re-add a column with client-side timestamps
        session.use_client_timestamp = False
        create_ks(session, 'ks', 1)
        return session

    # Reproducer for https://github.com/scylladb/scylla/issues/2623
    def test_restart_with_large_tables(self):
        cluster = self.cluster
        cluster.set_configuration_options(values={'max_cached_partition_size_in_kb': 1})
        session = self.prepare()

        n_tables = 100
        col_name = 'a' * 1024
        session.execute("USE ks")
        cmds = [("CREATE TABLE cf_{} (key int PRIMARY KEY, {} int)".format(i, col_name), ()) for i in range(n_tables)]

        nodes = cluster.nodelist()
        execute_concurrent(session, cmds, raise_on_first_error=True, concurrency=10)

        nodes[0].stop()
        nodes[0].start()

        session = self.patient_cql_connection(nodes[0])
        session.execute("select * from ks.cf_0")
        session.execute("select * from ks.cf_{}".format(n_tables - 1))
