# coding: utf-8

import time

from cassandra import InvalidRequest

from assertions import assert_invalid, assert_all, assert_none
from dtest import Tester, debug
from dtest import canReuseCluster


@canReuseCluster
class TestCQL(Tester):

    def prepare(self, ordered=False, create_keyspace=True, use_cache=False, nodes=1, rf=1, protocol_version=None,
                experimental=False, **kwargs):
        cluster = self.cluster

        if (ordered):
            cluster.set_configuration_options(values={'enable_deprecated_partitioners': True})
            cluster.set_partitioner("org.apache.cassandra.dht.ByteOrderedPartitioner")

        if (use_cache):
            cluster.set_configuration_options(values={'row_cache_size_in_mb': 100})

        if experimental:
            cluster.set_configuration_options(values={'experimental': True})

        start_rpc = kwargs.pop('start_rpc', False)
        if start_rpc:
            cluster.set_configuration_options(values={'start_rpc': True})

        enable_sstables_mc_format = kwargs.pop('enable_sstables_mc_format', False)
        cluster.set_configuration_options(values={'enable_sstables_mc_format': enable_sstables_mc_format})

        if not cluster.nodelist():
            cluster.populate(nodes).start()
        node1 = cluster.nodelist()[0]
        time.sleep(0.2)

        session = self.patient_cql_connection(node1, protocol_version=protocol_version)
        if create_keyspace:
            if self._preserve_cluster:
                session.execute("DROP KEYSPACE IF EXISTS ks")
            self.create_ks(session, 'ks', rf)
        return session

    def allow_filtering_test(self):
        session = self.prepare()

        session.execute("""
            CREATE TABLE test (
                k int,
                c int,
                v int,
                PRIMARY KEY (k, c)
            )
        """)

        for i in range(0, 3):
            for j in range(0, 3):
                session.execute("INSERT INTO test(k, c, v) VALUES(%d, %d, %d)" % (i, j, j))

        # Don't require filtering, always allowed
        queries = ["SELECT * FROM test WHERE k = 1",
                   "SELECT * FROM test WHERE k = 1 AND c > 2",
                   "SELECT * FROM test WHERE k = 1 AND c = 2"]
        for q in queries:
            session.execute(q)
            session.execute(q + " ALLOW FILTERING")

        # Require filtering, allowed only with ALLOW FILTERING
        queries = ["SELECT * FROM test WHERE c = 2",
                   "SELECT * FROM test WHERE c > 2 AND c <= 4"]
        for q in queries:
            assert_invalid(session, q)
            session.execute(q + " ALLOW FILTERING")

        # res = list(session.execute("SELECT * FROM test WHERE c = 2 ALLOW FILTERING"))
        # debug("Query C = 2 output length: {}".format(len(res)))
        # assert len(res) == 888, res
        # return

        session.execute("""
            CREATE TABLE indexed (
                k int PRIMARY KEY,
                a int,
                b int,
            )
        """)

        session.execute("CREATE INDEX ON indexed(a)")

        for i in range(0, 5):
            session.execute("INSERT INTO indexed(k, a, b) VALUES(%d, %d, %d)" % (i, i * 10, i * 100))

        # Don't require filtering, always allowed
        queries = ["SELECT * FROM indexed WHERE k = 1",
                   "SELECT * FROM indexed WHERE a = 20"]
        for q in queries:
            session.execute(q)
            session.execute(q + " ALLOW FILTERING")

        # Require filtering, allowed only with ALLOW FILTERING
        queries = ["SELECT * FROM indexed WHERE a = 20 AND b = 200"]
        for q in queries:
            assert_invalid(session, q)
            session.execute(q + " ALLOW FILTERING")

    def _insert_data(self, session):
        # insert data
        insert_stmt = "INSERT INTO users (username, password, gender, state, birth_year) VALUES "
        session.execute(insert_stmt + "('user1', 'ch@ngem3a', 'f', 'TX', 1968);")
        session.execute(insert_stmt + "('user2', 'ch@ngem3b', 'm', 'CA', 1971);")
        session.execute(insert_stmt + "('user3', 'ch@ngem3c', 'f', 'FL', 1978);")
        session.execute(insert_stmt + "('user4', 'ch@ngem3d', 'm', 'TX', 1974);")

    def allow_filtering_with_mv_test(self):
        session = self.prepare()

        session.execute(
            ("CREATE TABLE users (username varchar, password varchar, gender varchar, "
             "session_token varchar, state varchar, birth_year bigint, "
             "PRIMARY KEY (username));")
        )

        # create a materialized view
        session.execute(("CREATE MATERIALIZED VIEW users_by_state AS "
                         "SELECT * FROM users WHERE STATE IS NOT NULL AND username IS NOT NULL "
                         "PRIMARY KEY (state, username)"))

        self._insert_data(session)

        assert_all(session,
                   "SELECT count(*) FROM users WHERE username = 'user1'",
                   [[1]])

        assert_all(session,
                   "SELECT count(*) FROM users_by_state WHERE username = 'user1' ALLOW FILTERING",
                   [[1]])

        assert_all(session,
                   "SELECT count(*) FROM users_by_state WHERE state = 'TX' AND username = 'user1' ALLOW FILTERING",
                   [[1]])

        with self.assertRaises(InvalidRequest):
            session.execute(("SELECT * FROM users_by_state where username = 'user1'"))


    def partition_key_allow_filtering_test(self):
        """
        Filtering with unrestricted parts of partition keys
        @jira_ticket CASSANDRA-11031
        """
        session = self.prepare()

        session.execute("""
            CREATE TABLE IF NOT EXISTS test_filter (
                k1 int,
                k2 int,
                ck1 int,
                v int,
                PRIMARY KEY ((k1, k2), ck1)
            )
        """)

        session.execute("INSERT INTO test_filter (k1, k2, ck1, v) VALUES (0, 0, 0, 0)")
        session.execute("INSERT INTO test_filter (k1, k2, ck1, v) VALUES (0, 0, 1, 0)")
        session.execute("INSERT INTO test_filter (k1, k2, ck1, v) VALUES (0, 0, 2, 0)")
        session.execute("INSERT INTO test_filter (k1, k2, ck1, v) VALUES (0, 0, 3, 0)")
        session.execute("INSERT INTO test_filter (k1, k2, ck1, v) VALUES (0, 1, 0, 0)")
        session.execute("INSERT INTO test_filter (k1, k2, ck1, v) VALUES (0, 1, 1, 0)")
        session.execute("INSERT INTO test_filter (k1, k2, ck1, v) VALUES (0, 1, 2, 0)")
        session.execute("INSERT INTO test_filter (k1, k2, ck1, v) VALUES (0, 1, 3, 0)")
        session.execute("INSERT INTO test_filter (k1, k2, ck1, v) VALUES (1, 0, 0, 0)")
        session.execute("INSERT INTO test_filter (k1, k2, ck1, v) VALUES (1, 0, 1, 0)")
        session.execute("INSERT INTO test_filter (k1, k2, ck1, v) VALUES (1, 0, 2, 0)")
        session.execute("INSERT INTO test_filter (k1, k2, ck1, v) VALUES (1, 0, 3, 0)")
        session.execute("INSERT INTO test_filter (k1, k2, ck1, v) VALUES (1, 1, 0, 0)")
        session.execute("INSERT INTO test_filter (k1, k2, ck1, v) VALUES (1, 1, 1, 0)")
        session.execute("INSERT INTO test_filter (k1, k2, ck1, v) VALUES (1, 1, 2, 0)")
        session.execute("INSERT INTO test_filter (k1, k2, ck1, v) VALUES (1, 1, 3, 0)")

        # select test
        assert_all(session,
                   "SELECT * FROM test_filter WHERE k1 = 0 ALLOW FILTERING",
                   [[0, 0, 0, 0],
                    [0, 0, 1, 0],
                    [0, 0, 2, 0],
                    [0, 0, 3, 0],
                    [0, 1, 0, 0],
                    [0, 1, 1, 0],
                    [0, 1, 2, 0],
                    [0, 1, 3, 0]],
                   ignore_order=True)

        assert_all(session,
                   "SELECT * FROM test_filter WHERE k1 <= 1 AND k2 >= 1 ALLOW FILTERING",
                   [[0, 1, 0, 0],
                    [0, 1, 1, 0],
                    [0, 1, 2, 0],
                    [0, 1, 3, 0],
                    [1, 1, 0, 0],
                    [1, 1, 1, 0],
                    [1, 1, 2, 0],
                    [1, 1, 3, 0]],
                   ignore_order=True)

        assert_none(session, "SELECT * FROM test_filter WHERE k1 = 2 ALLOW FILTERING")
        assert_none(session, "SELECT * FROM test_filter WHERE k1 <=0 AND k2 > 1 ALLOW FILTERING")

        assert_all(session,
                   "SELECT * FROM test_filter WHERE k2 <= 0 ALLOW FILTERING",
                   [[0, 0, 0, 0],
                    [0, 0, 1, 0],
                    [0, 0, 2, 0],
                    [0, 0, 3, 0],
                    [1, 0, 0, 0],
                    [1, 0, 1, 0],
                    [1, 0, 2, 0],
                    [1, 0, 3, 0]],
                   ignore_order=True)

        assert_all(session,
                   "SELECT * FROM test_filter WHERE k1 <= 0 AND k2 = 0 ALLOW FILTERING",
                   [[0, 0, 0, 0],
                    [0, 0, 1, 0],
                    [0, 0, 2, 0],
                    [0, 0, 3, 0]])

        assert_all(session,
                   "SELECT * FROM test_filter WHERE k2 = 1 ALLOW FILTERING",
                   [[0, 1, 0, 0],
                    [0, 1, 1, 0],
                    [0, 1, 2, 0],
                    [0, 1, 3, 0],
                    [1, 1, 0, 0],
                    [1, 1, 1, 0],
                    [1, 1, 2, 0],
                    [1, 1, 3, 0]],
                   ignore_order=True)

        assert_none(session, "SELECT * FROM test_filter WHERE k2 = 2 ALLOW FILTERING")

        # filtering on both Partition Key and Clustering key
        assert_all(session,
                   "SELECT * FROM test_filter WHERE k1 = 0 AND ck1=0 ALLOW FILTERING",
                   [[0, 0, 0, 0],
                    [0, 1, 0, 0]],
                   ignore_order=True)

        assert_all(session,
                   "SELECT * FROM test_filter WHERE k1 = 0 AND k2=1 AND ck1=0 ALLOW FILTERING",
                   [[0, 1, 0, 0]])

        # count(*) test
        assert_all(session,
                   "SELECT count(*) FROM test_filter WHERE k2 = 0 ALLOW FILTERING",
                   [[8]])

        assert_all(session,
                   "SELECT count(*) FROM test_filter WHERE k2 = 1 ALLOW FILTERING",
                   [[8]])

        assert_all(session,
                   "SELECT count(*) FROM test_filter WHERE k2 = 2 ALLOW FILTERING",
                   [[0]])

        # test invalid query
        with self.assertRaises(InvalidRequest):
            session.execute("SELECT * FROM test_filter WHERE k1 = 0")

        with self.assertRaises(InvalidRequest):
            session.execute("SELECT * FROM test_filter WHERE k1 = 0 AND k2 > 0")

        with self.assertRaises(InvalidRequest):
            session.execute("SELECT * FROM test_filter WHERE k1 >= 0 AND k2 in (0,1,2)")

        with self.assertRaises(InvalidRequest):
            session.execute("SELECT * FROM test_filter WHERE k2 > 0")