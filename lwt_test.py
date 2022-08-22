import logging
from time import sleep

import pytest
from cassandra import ConsistencyLevel, Unavailable, WriteFailure
from cassandra.protocol import ConfigurationException
from cassandra.query import SimpleStatement

from dtest_class import Tester, create_ks, get_ip_from_node, create_cf
from dtest_setup import DTestSetup
from tools.assertions import assert_one, assert_none, assert_all, assert_row_count
from tools.data import rows_to_list, prepare_statement
from tools.metrics import get_node_metrics

logger = logging.getLogger(__name__)


@pytest.mark.dtest_full
class TestLwt(Tester):

    @pytest.fixture(autouse=True)
    def fixture_add_additional_log_patterns(self, fixture_dtest_setup: DTestSetup):  # pylint: disable=no-self-use
        fixture_dtest_setup.allow_log_errors = True

    def case_prologue(self, jvm_args=None, table_cql="CREATE TABLE IF NOT EXISTS t (a INT PRIMARY KEY, b INT)"):
        """ Assorted actions in preparation for a test case"""
        cluster = self.cluster
        cluster.set_configuration_options(values={"hinted_handoff_enabled": False})
        cluster.populate(1).start(wait_for_binary_proto=True, jvm_args=jvm_args)
        node = cluster.nodelist()[0]

        session = self.patient_cql_connection(node, protocol_version=4)
        create_ks(session=session, name="lwt", rf=1)
        session.execute(table_cql)
        return node, session

    @pytest.mark.single_node
    def test_no_cross_shard_ops(self):
        """Test that lightweight transaction operations, when performed
        using a shard-aware driver, do not incur cross-shard calls. This
        includes cross shard calls to write and read from paxos table. In
        particular, paxos table is partitioned in the same way as base
        table.

        The test itself is quite simple: insert rows with primary
        key from 1 to 16 using lightweight transaction. With the default
        partitioner, they will be evenly partitioned across multiple shards.
        Then check that no cross shard metric has changed.
        """
        node, session = self.case_prologue(jvm_args=["--smp", "4"])

        # leave only the scheduling groups we're interested in to prevent
        # flaky impact from background compaction
        name = "scylla_storage_proxy_replica_cross_shard_ops.*(statement|main)"

        cql = "insert into t (a, b) values (?, ?) if not exists"
        before = get_node_metrics(get_ip_from_node(node), metrics=[name])
        stmt = session.prepare(cql)
        for i in range(16):
            session.execute(stmt, (i, i))
        after = get_node_metrics(get_ip_from_node(node), metrics=[name])
        # Shard awareness doesn't work in python driver, so at least
        # there will be some bounce-to-shard messages.
        # XXX: when python driver supports shard-aware calls, this should be
        # 0.
        assert after.get(name, 0) - before.get(name, 0) <= 16, "{}:{}".format(before, after)
        before = after
        # Test direct execution as well as failing condition
        cql = "INSERT INTO t (a, b) VALUES ({}, {}) IF NOT EXISTS"
        for i in range(16):
            session.execute(cql.format(i, i))
        # Shard awareness won't work in case of direct execution,
        # but metrics won't change much thanks to bounce-to-shard
        # optimization switching to the right shard before starting
        # Paxos
        after = get_node_metrics(get_ip_from_node(node), metrics=[name])
        assert after.get(name, 0) - before.get(name, 0) <= 16, "{}:{}".format(before, after)
        cql = "DROP TABLE IF EXISTS t"
        session.execute(cql)

    @pytest.mark.single_node
    def test_metrics(self):
        # Because of
        # https://github.com/scylladb/scylla/issues/5860
        # lwt: CQL metrics are incremented twice if message was bounced
        # some of the metrics can double unless we set the number of cores
        # to 1
        node, session = self.case_prologue(jvm_args=["--smp", "1"])
        before = None
        after = None

        def check1(name, cql, expect):
            nonlocal node, session, before, after
            before = get_node_metrics(get_ip_from_node(node), metrics=[name])
            session.execute(cql)
            after = get_node_metrics(get_ip_from_node(node), metrics=[name])
            assert after.get(name, 0) - before.get(name, 0) == expect, "{} {}".format(before, after)

        name = "scylla_storage_proxy_coordinator_cas_write_condition_not_met"
        cql = "INSERT INTO t (a, b) VALUES (1, 1) IF NOT EXISTS"
        check1(name, cql, 0)
        check1(name, cql, 1)
        name = "scylla_cql_batches"
        check1(name, cql, 0)
        cql = "BEGIN BATCH " + cql + " APPLY BATCH"
        check1(name, cql, 1)
        name = "scylla_cql_statements_in_batches"
        check1(name, cql, 1)
        cql = "BEGIN BATCH APPLY BATCH"
        check1(name, cql, 0)
        # Don't confuse with scylla_cql_deletes_per_ks
        name = "scylla_cql_deletes{.*conditional=\"yes\""
        check1(name, cql, 0)
        cql = "DELETE FROM t WHERE a = 1 IF EXISTS"
        check1(name, cql, 1)
        name = "scylla_cql_inserts{.*conditional=\"yes\""
        cql = "INSERT INTO t (a, b) VALUES (1, 1) IF NOT EXISTS"
        check1(name, cql, 1)
        name = "scylla_cql_updates{.*conditional=\"yes\""
        cql = "UPDATE t SET b = 1 WHERE a = 1 if b = 1"
        check1(name, cql, 1)
        cql = "DROP TABLE IF EXISTS t"
        session.execute(cql)

        # Not testing {inserts|updates|deletes}_per_ks since their
        # machinery is not LWT specific

        # TODO: down a node and test:
        # scylla_storage_proxy_coordinator_cas_read_unavailable
        # scylla_storage_proxy_coordinator_cas_write_unavailable
        #
        # TODO: add injections and test:
        # scylla_storage_proxy_coordinator_cas_read_timeouts
        # scylla_storage_proxy_coordinator_cas_write_timeouts
        # scylla_storage_proxy_coordinator_cas_read_unfinished_commit
        # scylla_storage_proxy_coordinator_cas_write_unfinished_commit

    def test_read_round_optimization(self):
        """
         3.5 Ensure read-round-optimization works: update the record using
           transaction. Display server metrics for network round trips.
           Update the same record 10 more times: check that network
           roundtrips is roughly at 10*3 = 30. (Or add a counter for the
           optimization, and check the counter was incremented ~30 times).
         3.6 Ensure read-round-optimization is skipped when data does not
           match: update the record using lwt. Update the record without lwt
           with CL=1. Try to read the data using paxos.  Make sure
           read-round-optimization was skipped.
        """
        #
        # 3.5
        #
        cluster = self.cluster
        cluster.set_configuration_options(values={"hinted_handoff_enabled": False})
        cluster.populate(3).start(wait_for_binary_proto=True)
        node = cluster.nodelist()[0]
        session = self.patient_exclusive_cql_connection(node)
        create_ks(session=session, name="lwt", rf=3)
        cql = "DROP TABLE IF EXISTS t"
        session.execute(cql)
        cql = "CREATE TABLE IF NOT EXISTS t (a INT PRIMARY KEY, b INT)"
        session.execute(cql)
        cql = "INSERT INTO t (a,b) VALUES (1,0) IF NOT EXISTS"
        stmt = SimpleStatement(cql, consistency_level=ConsistencyLevel.QUORUM)
        session.execute(cql)
        cql = "UPDATE t SET b = ? WHERE a = 1 IF b = ?"
        stmt = session.prepare(cql)
        stmt.consistency_level = ConsistencyLevel.QUORUM
        name = "scylla_storage_proxy_coordinator_cas_failed_read_round_optimization"
        before = get_node_metrics(get_ip_from_node(node), metrics=[name])
        for i in range(10):
            session.execute(stmt, (i + 1, i))
        after = get_node_metrics(get_ip_from_node(node), metrics=[name])
        assert after.get(name, 0) - before.get(name, 0) == 0, "{} {}".format(before, after)
        cql = "DROP TABLE t"
        session.execute(cql)
        #
        # 3.6
        # use a range of statements to avoid any flakiness.
        #
        key_count = 100
        cql = "CREATE TABLE IF NOT EXISTS t (a INT PRIMARY KEY, b INT)"
        session.execute(cql)
        cql = "INSERT INTO t (a, b) VALUES (?, ?)"
        stmt = session.prepare(cql)
        stmt.consistency_level = ConsistencyLevel.ALL
        for i in range(key_count):
            session.execute(stmt, (i, i))

        non_paxos_stmt = session.prepare("UPDATE t SET b = 2 WHERE a = ?")
        non_paxos_stmt.consistency_level = ConsistencyLevel.QUORUM
        paxos_stmt = session.prepare("UPDATE t SET b = 3 WHERE a = ? IF b = 2")
        node1 = cluster.nodelist()[1]
        node2 = cluster.nodelist()[2]

        before = get_node_metrics(get_ip_from_node(node), metrics=[name])
        node1.stop()
        for i in range(key_count):
            session.execute(non_paxos_stmt, (i,))
        node2.stop()
        node1.start(wait_for_binary_proto=True)
        for i in range(key_count):
            session.execute(paxos_stmt, (i,))
        after = get_node_metrics(get_ip_from_node(node), metrics=[name])
        assert after.get(name, 0) - before.get(name, 0) == key_count, "{} {}".format(before, after)

    def test_basic_distributed(self):  # pylint: disable=too-many-statements
        """Basic distributed tests (3.1 - 3.4 from the test plan). """

        cluster = self.cluster
        cluster.set_configuration_options(values={"hinted_handoff_enabled": False})
        cluster.populate(3).start(wait_for_binary_proto=True)
        node1 = cluster.nodelist()[0]
        node2 = cluster.nodelist()[1]
        node3 = cluster.nodelist()[2]
        session1 = self.patient_cql_connection(node1)
        create_ks(session=session1, name="lwt", rf=3)
        cql = "CREATE TABLE IF NOT EXISTS t (a INT PRIMARY KEY, b INT)"
        session1.execute(cql)
        cql = "INSERT INTO t (a,b) VALUES (1,1) IF NOT EXISTS"
        session1.execute(cql)

        # 3.1 Insert data using a transaction. Select data from a different
        # node, get the expected result back.
        cql = "SELECT * FROM t WHERE a = 1"
        session2 = self.patient_exclusive_cql_connection(node2, keyspace="lwt")
        # Note: consistency level ONE
        assert_one(session2, cql, [1, 1], cl=ConsistencyLevel.ONE)

        # 3.2 Power off a node. Insert data using a transaction. Power up
        # the down node. Read the data with ONE read from the restarted
        # node, check the data is stale. Select data from the restarted
        # node with Paxos, get an up to date (most recent) result back.
        # Read the data with ONE read again. Discover automatic Paxos
        # round repair took place during the previous read with Paxos, and
        # the data has been applied to the base table on the restarted
        # node by the previous Paxos read, so non-Paxos read is also
        # returning a correct result.
        node2.stop()
        cql = "INSERT INTO t (a,b) VALUES (2,2) IF NOT EXISTS"
        session1.execute(cql)
        node2.start(wait_for_binary_proto=True)
        node1.stop()
        node3.stop()
        session2 = self.patient_exclusive_cql_connection(node2, keyspace="lwt")
        cql = "SELECT * FROM t WHERE a = 2"
        assert_none(session2, cql, cl=ConsistencyLevel.ONE)
        node1.start(wait_for_binary_proto=True)
        node3.start(wait_for_binary_proto=True)
        assert_one(session2, cql, [2, 2], cl=ConsistencyLevel.SERIAL)
        assert_one(session2, cql, [2, 2], cl=ConsistencyLevel.ONE)

        # 3.3. Power off two nodes. Try to insert data. Get the correct
        # error (lack of quorum). Power up one of the nodes. Try to
        # insert data. Succeed. Power up the third node, power down the
        # first. Read the data from the third node using quorum reads.
        # Read the data using Paxos read, get the correct result back.
        node1.stop()
        node2.stop()
        node3 = cluster.nodelist()[2]
        session3 = self.patient_exclusive_cql_connection(node3, keyspace="lwt")
        cql = "INSERT INTO t (a,b) VALUES (3,3) IF NOT EXISTS"
        with pytest.raises(Unavailable):
            session3.execute(cql)
        cql = "SELECT * FROM t WHERE a=3"
        assert_none(session3, cql, cl=ConsistencyLevel.ONE)
        # Have to specify custom seeds not because we need new vnodes,
        # but because the old seed node is down and we need to discocver
        # the rest of the cluster
        node2.start(wait_for_binary_proto=True, jvm_args=[
            "--seed-provider-parameters", "seeds={}".format(get_ip_from_node(node3))])
        cql = "SELECT * FROM t WHERE a=3"
        assert_none(session3, cql, cl=ConsistencyLevel.SERIAL)
        cql = "INSERT INTO t (a,b) VALUES (3,3) IF NOT EXISTS"
        assert_one(session3, cql, [True, None, None])
        node3.stop()
        node1.start(wait_for_binary_proto=True)
        session1 = self.patient_exclusive_cql_connection(node1, keyspace="lwt")
        cql = "SELECT * FROM t WHERE a=3"
        assert_one(session1, cql, [3, 3], cl=ConsistencyLevel.SERIAL)

        # 3.4 Power off one node. Insert records using Paxos. Let paxos table
        # expire (truncate system.paxos for the sake of the test). Bring the
        # powered down node back. Issue an LWT on the restored node. Ensure it
        # has the latest view and performs Paxos repair before applying the
        # new mutation.
        cql = "INSERT INTO t (a,b) VALUES (4,4) IF NOT EXISTS"
        assert_one(session1, cql, [True, None, None])
        cql = "SELECT row_key, cf_id FROM system.paxos"
        rows = session1.execute(cql)
        cql = "DELETE FROM system.paxos WHERE row_key = ? AND cf_id = ?"
        stmt = session1.prepare(cql)
        for row in rows:
            session1.execute(stmt, (row[0], row[1]))

        self.cluster.compact()
        node3.start(wait_for_binary_proto=True)
        session3 = self.patient_exclusive_cql_connection(node3, keyspace="lwt")
        cql = "SELECT * FROM t WHERE a=4"
        assert_one(session3, cql, [4, 4], cl=ConsistencyLevel.SERIAL)
        cql = "DROP TABLE IF EXISTS t"
        session3.execute(cql)

    def test_multi_dc(self):
        # 4.1 Check LOCAL_QUORUM works as expected if the other DC is not
        # available.
        cluster = self.cluster
        cluster.set_configuration_options(values={"hinted_handoff_enabled": False})
        cluster.populate([3, 3]).start(wait_for_binary_proto=True)
        node1 = cluster.nodelist()[0]
        session1 = self.patient_cql_connection(node1)
        create_ks(session=session1, name="lwt", rf={"dc1": 3, "dc2": 3})
        cql = "CREATE TABLE IF NOT EXISTS t (a INT PRIMARY KEY)"
        session1.execute(cql)
        node2 = cluster.nodelist()[5]
        session2 = self.patient_exclusive_cql_connection(node2, keyspace="lwt")
        cql = "INSERT INTO t (a) VALUES (?) IF NOT EXISTS"
        stmt1 = session1.prepare(cql)
        stmt1.serial_consistency_level = ConsistencyLevel.LOCAL_SERIAL
        session1.execute(stmt1, (1,))
        stmt2 = session2.prepare(cql)
        stmt2.serial_consistency_level = ConsistencyLevel.LOCAL_SERIAL
        session2.execute(stmt2, (2,))
        for i in range(2, 6):
            cluster.nodelist()[i].stop()
        session1.execute(stmt1, (3,))
        with pytest.raises(Unavailable):
            stmt1.serial_consistency_level = ConsistencyLevel.SERIAL
            session1.execute(stmt1, (4,))
        # 4.2 Issue two Paxos LOCAL_QUORUM writes in parallel at different DC.
        # Follow up by PAXOS QUORUM read, to ensure the latest write wins.
        for i in range(4, 6):
            cluster.nodelist()[i].start(wait_for_binary_proto=True)
        for i in range(0, 2):
            cluster.nodelist()[i].stop()
        session2 = self.patient_exclusive_cql_connection(node2, keyspace="lwt")
        session2.execute(stmt2, (4,))
        stmt2.serial_consistency_level = ConsistencyLevel.SERIAL
        with pytest.raises(Unavailable):
            session2.execute(stmt2, (5,))
        cluster.nodelist()[0].start(wait_for_binary_proto=True)
        cluster.nodelist()[3].start(wait_for_binary_proto=True)
        session2.execute(SimpleStatement("SELECT * FROM t WHERE a = 4",
                                         consistency_level=ConsistencyLevel.SERIAL))
        # 4.3 Power off two nodes in a single DC. Ensure LOCAL QUORUM Paxos
        # queries don’t work, while QUORUM LWT writes do.
        session1 = self.patient_exclusive_cql_connection(node1, keyspace="lwt")
        stmt1.serial_consistency_level = ConsistencyLevel.LOCAL_SERIAL
        with pytest.raises(Unavailable):
            session1.execute(stmt1, (6,))
        stmt1.serial_consistency_level = ConsistencyLevel.SERIAL
        session1.execute(stmt1, (7,))

    def create_exclusive_sessions_for_every_node(self, cluster):
        return (self.exclusive_cql_connection(node) for node in cluster.nodelist())

    @staticmethod
    def shutdown_all_sessions(sessions: list):
        for session in sessions:
            session.shutdown()

    @staticmethod
    def execute_insert_data_query(session, table, cql, start, end):
        stmt1 = session.prepare(cql)
        stmt1.serial_consistency_level = ConsistencyLevel.SERIAL
        logger.info("%s %d rows in table '%s'", cql.split()[0], end - start, table)
        for i in range(start, end):
            session.execute(stmt1, (i, i))

    def test_paxos_grace_seconds_basic(self):
        """
            Basic paxos_grace_seconds test:
            - create table with default paxos_grace_seconds
            - create another table with short (10 sec) paxos_grace_seconds
            - insert 10 rows (LWT request) in every table
            - wait 10 sec
            - check paxos: only records of table with default paxos_grace_seconds are found
         """

        cluster = self.cluster
        cluster.set_configuration_options(values={"hinted_handoff_enabled": False})
        cluster.populate(3).start(wait_for_binary_proto=True)

        # Create session as exclusive connection to the node in goal to select from paxos table on every node
        session1, session2, session3 = self.create_exclusive_sessions_for_every_node(cluster)

        create_ks(session=session1, name="lwt", rf=3)

        logger.info("Create table with paxos_grace_seconds is 10 sec.")
        create_cf(session=session1, name='ttl_10_sec', key_type='int', columns={'v1': 'int'},
                  paxos_grace_seconds=10)

        logger.info("Create table with default paxos_grace_seconds.")
        create_cf(session=session1, name='default_ttl', key_type='int', columns={'v1': 'int'})

        for table in ['default_ttl', 'ttl_10_sec']:
            self.execute_insert_data_query(session=session1, table=table,
                                           cql=f"INSERT INTO {table} (key, v1) VALUES (?, ?) IF NOT EXISTS",
                                           start=0, end=10)

            if table == 'default_ttl':
                default_ttl_paxos_rows = rows_to_list(
                    session1.execute("SELECT row_key, cf_id FROM system.paxos").current_rows)

        for session in [session1, session2, session3]:
            assert_row_count(session=session, table_name='system.paxos', expected=20,
                             consistency_level=ConsistencyLevel.LOCAL_ONE)

        logger.info("Wait for paxos rows for table 'ttl_10_sec' will be expired")
        sleep(11)

        for session in [session1, session2, session3]:
            assert_row_count(session=session, table_name='system.paxos', expected=10,
                             consistency_level=ConsistencyLevel.LOCAL_ONE)
            assert_all(session=session, query='SELECT row_key, cf_id FROM system.paxos',
                       expected=default_ttl_paxos_rows, ignore_order=True,
                       cl=ConsistencyLevel.LOCAL_ONE)

        for table in ['default_ttl', 'ttl_10_sec']:
            assert_row_count(session=session1, table_name=table, expected=10,
                             consistency_level=ConsistencyLevel.QUORUM)

        self.shutdown_all_sessions([session1, session2, session3])

    def test_paxos_grace_seconds_alter(self):
        """
            Alter paxos_grace_seconds test:
            - create table with default paxos_grace_seconds
            - insert 10 rows (LWT request)
            - set short paxos_grace_seconds and send LWT requests
            - check paxos:
         """

        cluster = self.cluster
        cluster.set_configuration_options(values={"hinted_handoff_enabled": False})
        cluster.populate(3).start(wait_for_binary_proto=True)

        # Create session as exclusive connection to the node in goal to select from paxos table on every node
        session1, session2, session3 = self.create_exclusive_sessions_for_every_node(cluster)

        create_ks(session=session1, name="lwt", rf=3)

        logger.info("Create table with default paxos_grace_seconds.")
        table_name = 'default_ttl'
        create_cf(session=session1, name=table_name, key_type='int', columns={'v1': 'int'})

        self.execute_insert_data_query(session=session1, table=table_name,
                                       cql=f"INSERT INTO {table_name} (key, v1) VALUES (?, ?) IF NOT EXISTS",
                                       start=0, end=10)

        default_ttl_paxos_rows = rows_to_list(
            session1.execute("SELECT row_key, cf_id FROM system.paxos").current_rows)

        for session in [session1, session2, session3]:
            assert_row_count(session=session, table_name='system.paxos', expected=10,
                             consistency_level=ConsistencyLevel.LOCAL_ONE)

        for session in [session1, session2, session3]:
            assert_row_count(session=session, table_name='system.paxos', expected=10,
                             consistency_level=ConsistencyLevel.LOCAL_ONE)
            assert_all(session=session, query='SELECT row_key, cf_id FROM system.paxos',
                       expected=default_ttl_paxos_rows, ignore_order=True,
                       cl=ConsistencyLevel.LOCAL_ONE)

        assert_row_count(session=session1, table_name=table_name, expected=10,
                         consistency_level=ConsistencyLevel.QUORUM)

        query = f"ALTER TABLE {table_name} WITH paxos_grace_seconds=10"
        logger.info(query)
        session1.execute(query)

        self.execute_insert_data_query(session=session1, table=table_name,
                                       cql=f"UPDATE {table_name} SET v1 = 101 WHERE key = ? IF v1 = ?",
                                       start=0, end=5)

        for session in [session1, session2, session3]:
            assert_row_count(session=session, table_name='system.paxos', expected=10,
                             consistency_level=ConsistencyLevel.LOCAL_ONE)

            assert_all(session=session, query='SELECT row_key, cf_id FROM system.paxos',
                       expected=default_ttl_paxos_rows, ignore_order=True,
                       cl=ConsistencyLevel.LOCAL_ONE)

        logger.info("Wait for paxos rows for table '%s' will be expired", table_name)
        sleep(11)

        for session in [session1, session2, session3]:
            assert_row_count(session=session, table_name='system.paxos', expected=5,
                             consistency_level=ConsistencyLevel.LOCAL_ONE)

        assert_row_count(session=session1, table_name=table_name, expected=10,
                         consistency_level=ConsistencyLevel.QUORUM)

        self.shutdown_all_sessions([session1, session2, session3])

    def test_paxos_grace_seconds_interrupt(self):
        """
            Basic paxos_grace_seconds test:
            - create another table with short (10 sec) paxos_grace_seconds
            - insert 10 rows (LWT request) in every table
            - stop one node (it should keep records in the paxos)
            - wait 10 sec
            - check paxos: no records on every node
         """

        cluster = self.cluster
        cluster.set_configuration_options(values={"hinted_handoff_enabled": False})
        cluster.populate(3).start(wait_for_binary_proto=True)

        # Create session as exclusive connection to the node in goal to select from paxos table on every node
        session1, session2, session3 = self.create_exclusive_sessions_for_every_node(cluster)

        create_ks(session=session1, name="lwt", rf=3)

        logger.info("Create table with paxos_grace_seconds is 10 sec.")
        table_name = 'ttl_10_sec'
        create_cf(session=session1, name=table_name, key_type='int', columns={'v1': 'int'},
                  paxos_grace_seconds=10)

        self.execute_insert_data_query(session=session1, table=table_name,
                                       cql=f"INSERT INTO {table_name} (key, v1) VALUES (?, ?) IF NOT EXISTS",
                                       start=0, end=10)

        for session in [session1, session2, session3]:
            assert_row_count(session=session, table_name='system.paxos', expected=10,
                             consistency_level=ConsistencyLevel.LOCAL_ONE)

        logger.info("Stop node 2")
        node2 = self.cluster.nodelist()[1]
        node2.stop(gently=False, wait_other_notice=True)
        session2.shutdown()

        logger.info("Wait for paxos rows for table '%s' will be expired", table_name)
        sleep(10)

        for session in [session1, session3]:
            assert_row_count(session=session, table_name='system.paxos', expected=0,
                             consistency_level=ConsistencyLevel.LOCAL_ONE)

        assert_row_count(session=session1, table_name=table_name, expected=10,
                         consistency_level=ConsistencyLevel.QUORUM)

        logger.info("Start node 2")
        node2.start(wait_other_notice=True)
        session2 = self.exclusive_cql_connection(node2)

        for session in [session1, session2, session3]:
            assert_row_count(session=session, table_name='system.paxos', expected=0,
                             consistency_level=ConsistencyLevel.LOCAL_ONE)

        assert_row_count(session=session1, table_name=table_name, expected=10,
                         consistency_level=ConsistencyLevel.QUORUM)

        self.shutdown_all_sessions([session1, session2, session3])

    def test_paxos_grace_seconds_negative(self):
        """
            Try to create table with paxos_grace_seconds = -1
         """

        cluster = self.cluster
        cluster.set_configuration_options(values={"hinted_handoff_enabled": False})
        cluster.populate(3).start(wait_for_binary_proto=True)

        # Create session as exclusive connection to the node in goal to select from paxos table on every node
        session1, *_ = self.create_exclusive_sessions_for_every_node(cluster)

        create_ks(session=session1, name="lwt", rf=3)

        logger.info("Try to create table with negative paxos_grace_seconds.")
        table_name = 'zero_ttl'

        with pytest.raises(ConfigurationException,
                           match="paxos_grace_seconds cannot be smaller than 0, \\(default 864000\\)"):
            create_cf(session=session1, name=table_name, key_type='int', columns={'v1': 'int'}, paxos_grace_seconds=-1)

    # This test is for covering of issue https://github.com/scylladb/scylla/issues/6284
    # Can't reproduce the issue.
    # Hold my attempt to reproduce for the future
    @pytest.mark.skip("the test is not ready. ")
    def test_conflict_transactions(self):  # pylint: disable=too-many-statements,too-many-locals
        cluster = self.cluster
        cluster.set_configuration_options(values={"hinted_handoff_enabled": False})
        cluster.populate(5).start(wait_for_binary_proto=True)

        # Create session as exclusive connection to the node in goal to select from paxos table on every node
        session1, session2, session3, *_ = self.create_exclusive_sessions_for_every_node(cluster)
        node1, node2, node3 = self.cluster.nodelist()[:3]

        create_ks(session=session1, name="lwt", rf=3)

        logger.info("Create table with paxos_grace_seconds = 15")
        table_name = 'test'
        create_cf(session=session1, name=table_name, key_type='int', columns={'v1': 'list<int>'},
                  paxos_grace_seconds=15)

        node1.stop(gently=False)
        session1.shutdown()

        self.enable_error("paxos_error_before_save_proposal", node2, one_shot=True)
        _ = session3.execute(f"INSERT INTO lwt.{table_name} (key, v1) VALUES (0, [0]) IF NOT EXISTS").current_rows

        default_ttl_paxos_rows2 = session2.execute("SELECT * FROM system.paxos").current_rows
        assert not default_ttl_paxos_rows2, "Found record in the paxos on the node2 unexpectedly"
        default_ttl_paxos_rows3 = session3.execute("SELECT * FROM system.paxos").current_rows
        assert default_ttl_paxos_rows3, "Found record in the paxos on the node2 uxpectedly"

        sleep(15)
        node3.stop(gently=False)
        session3.shutdown()

        node1.start()
        session1 = self.exclusive_cql_connection(node1)

        default_ttl_paxos_rows1 = session1.execute("SELECT * FROM system.paxos").current_rows
        logger.info("record in the paxos on the node1: %s", default_ttl_paxos_rows1)
        # assert not default_ttl_paxos_rows1, "Found record in the paxos on the node1 unexpectedly"
        rows1 = session1.execute("SELECT * FROM lwt.{table_name}").current_rows
        logger.info("row on the node1: %s", rows1)
        rows2 = session2.execute("SELECT * FROM lwt.{table_name}").current_rows
        logger.info("row on the node2: %s", rows1)

        _ = session1.execute(f"UPDATE lwt.{table_name} SET v1 = v1 + [1] WHERE key = 0 IF v1 = [0]").current_rows

        sleep(15)

        node3.start()
        session3 = self.exclusive_cql_connection(node3)

        rows1 = session1.execute("SELECT * FROM lwt.{table_name}").current_rows
        logger.info("row on the node1: %s", rows1)
        rows2 = session2.execute("SELECT * FROM lwt.{table_name}").current_rows
        logger.info("row on the node2: %s", rows2)
        rows3 = session3.execute("SELECT * FROM lwt.{table_name}").current_rows
        logger.info("row on the node3: %s", rows3)

        # default_ttl_paxos_rows3 = session1.execute("SELECT * FROM system.paxos").current_rows
        # default_ttl_paxos_rows4 = session2.execute("SELECT * FROM system.paxos").current_rows
        # default_ttl_paxos_rows5 = session3.execute("SELECT * FROM system.paxos").current_rows

        _ = session1.execute(f"UPDATE lwt.{table_name} SET v1 = v1 + [2] WHERE key = 0 IF v1 = [0]").current_rows

        default_ttl_paxos_rows6 = session1.execute("SELECT * FROM system.paxos").current_rows
        logger.info("record in the paxos on the node1: %s", default_ttl_paxos_rows6)
        default_ttl_paxos_rows7 = session2.execute("SELECT * FROM system.paxos").current_rows
        logger.info("record in the paxos on the node2: %s", default_ttl_paxos_rows7)
        default_ttl_paxos_rows8 = session3.execute("SELECT * FROM system.paxos").current_rows
        logger.info("record in the paxos on the node3: %s", default_ttl_paxos_rows8)

        rows1 = session1.execute("SELECT * FROM lwt.{table_name}").current_rows
        logger.info("row on the node1: %s", rows1)
        rows2 = session2.execute("SELECT * FROM lwt.{table_name}").current_rows
        logger.info("row on the node2: %s", rows2)
        rows3 = session3.execute("SELECT * FROM lwt.{table_name}").current_rows
        logger.info("row on the node3: %s", rows3)

        # assert_all(session1, f"select key, v1 from lwt.{table_name}", expected=[[0, 202]])
        # assert_all(session2, f"select key, v1 from lwt.{table_name}", expected=[[0, 202]])
        # assert_all(session3, f"select key, v1 from lwt.{table_name}", expected=[[0, 202]])

    def test_not_deterministic_function_in_pk(self):
        table_name = "test"
        node, session = self.case_prologue(
            table_cql=f"CREATE TABLE IF NOT EXISTS {table_name} (pk1 uuid, pk2 timestamp, ck date, "
                      "PRIMARY KEY((pk1, pk2), ck))")

        insert_cql = f"INSERT INTO {table_name}(pk1, pk2, ck) " \
                     f"VALUES (uuid(), currentTimestamp(), currentDate()) IF NOT EXISTS"

        rows = 1000
        logger.debug(f"Insert {rows} rows using not prepared query")
        for _ in range(rows):
            session.execute(insert_cql)
        assert_row_count(session=session, table_name=table_name, expected=rows)

        logger.debug(f"Insert {rows} rows using prepared query")
        prepared_cql = prepare_statement(session, insert_cql)
        for _ in range(rows):
            session.execute(prepared_cql, [])
        rows += 1000
        assert_row_count(session=session, table_name=table_name, expected=rows)


@pytest.mark.dtest_full
class PaxosBugTest(Tester):

    def test_synced_most_recent_commit_in_cas_should_not_cause_timeouts(self):
        """Test for https://issues.apache.org/jira/browse/CASSANDRA-12043:
        Syncing most recent commit in CAS across replicas can cause all CAS queries in the CQL partition to fail
        """
        self.cluster.populate(3).start(wait_for_binary_proto=True)
        session_a = self.exclusive_cql_connection(self.cluster.nodelist()[0])
        self.prepare_a_table_with_paxos_grace_seconds_set_to_120(session_a)
        self.insert_data_on_nodes_a_and_b(session_a)
        self.wait_seconds(60)
        self.query_data_from_nodes_a_and_c_with_consistency_serial(session_a)
        self.wait_seconds(60)
        self.query_data_from_nodes_a_and_c_with_consistency_serial(session_a)  # should not timeout

    @staticmethod
    def wait_seconds(seconds):
        logger.info("Waiting %d seconds", seconds)
        sleep(seconds)

    @staticmethod
    def prepare_a_table_with_paxos_grace_seconds_set_to_120(session):
        create_ks(session=session, name="paxos_bug", rf=3)
        logger.info("Create table with paxos_grace_seconds set to 120 sec.")
        create_cf(session=session, name="cassandra_12043", key_type='int', columns={'v1': 'int'},
                  paxos_grace_seconds=120)

    def insert_data_on_nodes_a_and_b(self, session):
        logger.info("stoping node C")
        node_a, node_b, node_c = self.cluster.nodelist()
        node_c.stop()
        logger.info("inserting data")
        query = SimpleStatement(
            "INSERT INTO cassandra_12043 (key, v1) VALUES (%s, %s) IF NOT EXISTS",
            consistency_level=ConsistencyLevel.QUORUM)
        session.execute(query, (1, 1))
        logger.info("flush data to disk")
        node_a.flush()
        node_b.flush()

    def query_data_from_nodes_a_and_c_with_consistency_serial(self, session):
        node_b, node_c = self.cluster.nodelist()[1:]
        if node_b.is_running():
            logger.info("Stopping node B")
            node_b.stop()
        if not node_c.is_running():
            logger.info("Starting node C")
            node_c.start(wait_for_binary_proto=True, wait_other_notice=True)
        logger.info("query nodes A and C with consistency SERIAL")
        query = SimpleStatement(
            "select * from cassandra_12043 where key = 1;",
            consistency_level=ConsistencyLevel.SERIAL)
        session.execute(query)


# Read Linearizability Test
#
NODES = 3
NODE_A = 0
NODE_B = 1
NODE_C = 2

error_injections = [
    "paxos_prepare_timeout",
    "paxos_error_before_save_promise",
    "paxos_error_after_save_promise",
    "paxos_accept_proposal_timeout",
    "paxos_error_before_save_proposal",
    "paxos_error_after_save_proposal",
    "paxos_error_before_learn",
    "paxos_state_learn_timeout",
    "paxos_timeout_after_save_decision"]


@pytest.mark.dtest_full
class LwtReadLinearizabilityTest(Tester):  # pylint: disable=too-few-public-methods

    @pytest.mark.dtest_debug
    @pytest.mark.scylla_mode('!release')
    def test_read_linearizability(self):
        """Consider 3 nodes A, B and C and a LWT failed write operation that managed to get V
           accepted on A. The value is read twice without writes in the middle. First read access
           B and C and returns nothing. Next one access A and B, notices failed round and
           completes it. Returns value V. Since two consequent writes without any reads in the
           middle return different value this breaks linearisability."""

        self.ignore_log_patterns.extend(["utils::injected_error"])

        # Runs three nodes A, B, C
        if not self.cluster.nodelist():
            self.cluster.populate(NODES)
            self.cluster.start(wait_other_notice=True)

        session_a = self.patient_cql_connection(self.cluster.nodelist()[NODE_A])
        create_ks(session_a, "ks", rf=NODES)
        session_a.execute("CREATE TABLE t (id int PRIMARY KEY, v int)")

        # 1. Inject error at accept stage in B and C
        for node in [NODE_B, NODE_C]:
            self.enable_error("paxos_error_before_save_proposal", node)

        # 2. run a write(V) that suppose to succeed
        # 3. write will fail because B and C will fail
        with self.assertRaises(WriteFailure):
            ret = session_a.execute("INSERT INTO ks.t (id, v) VALUES (1, 1) IF NOT EXISTS").current_rows
            assert ret[0].applied is False

        # 4. enable all error injections on A
        for error in error_injections:
            self.enable_error(error, NODE_A)

        # 5. remove error injection from B and C
        for node in [NODE_B, NODE_C]:
            self.disable_error("paxos_error_before_save_proposal", node)

        # 6. run a read of the same key
        # 7. verify that read does not return V

        # Verify value is not set in B and C
        for node in [NODE_B, NODE_C]:
            session = self.patient_exclusive_cql_connection(self.cluster.nodelist()[node])
            query = SimpleStatement(
                "SELECT v FROM ks.t WHERE id = 1",
                consistency_level=ConsistencyLevel.ONE
            )
            ret = session.execute(query).current_rows
            if ret:
                logger.debug("Got invalid value for node %s: %s", node, ret)
            assert ret == []

        # Verify value is not set in A  (complete round?)
        query = SimpleStatement(
            "SELECT v FROM ks.t WHERE id = 1",
            consistency_level=ConsistencyLevel.SERIAL
        )
        ret = session_a.execute(query).current_rows
        assert ret == []

        # 8. remove injected errors from A and to C
        for error in error_injections:
            self.disable_error(error, NODE_A)
            self.enable_error(error, NODE_C)

        # 9. run the read again and check that the value is still not V
        query = SimpleStatement(
            "SELECT v FROM ks.t WHERE id = 1",
            consistency_level=ConsistencyLevel.SERIAL
        )
        ret = session.execute(query).current_rows

        # 7. verify that read does not return V
        assert ret == []
