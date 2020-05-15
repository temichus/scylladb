from dtest import Tester, debug
from nose.plugins.attrib import attr
from assertions import assert_one, assert_none, assert_unavailable
from cassandra import ConsistencyLevel, Unavailable, WriteFailure
from cassandra.query import SimpleStatement
from tools import rows_to_list

class LwtTest(Tester):

    def case_prologue(self, jvm_args = None):
        """ Assorted actions in preparation for a test case"""
        cluster = self.cluster
        cluster.set_configuration_options(values={"hinted_handoff_enabled": False})
        cluster.populate(1).start(wait_for_binary_proto=True, jvm_args=jvm_args)
        node = cluster.nodelist()[0]

        session = self.patient_cql_connection(node, protocol_version=4)
        self.create_ks(session=session, name="lwt", rf=1)
        cql = "CREATE TABLE IF NOT EXISTS t (a INT PRIMARY KEY, b INT)"
        session.execute(cql)
        return node, session

    @attr("single_node")
    def no_cross_shard_ops_test(self):
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

        name = "scylla_storage_proxy_replica_cross_shard_ops"

        cql = "insert into t (a, b) values (?, ?) if not exists"
        before = self.get_node_metrics(self.get_ip_from_node(node), metrics=[name])
        stmt = session.prepare(cql)
        for i in range(16):
            session.execute(stmt, (i, i))
        after = self.get_node_metrics(self.get_ip_from_node(node), metrics=[name])
        # Shard awareness doesn't work in python driver, so at least
        # there will be some bounce-to-shard messages.
        # XXX: when python driver supports shard-aware calls, this should be
        # 0.
        assert after[name] - before[name] < 16, "{}:{}".format(before, after)
        before = after
        # Test direct execution as well as failing condition
        cql = "INSERT INTO t (a, b) VALUES ({}, {}) IF NOT EXISTS"
        for i in range(16):
            session.execute(cql.format(i, i))
        # Shard awareness won't work in case of direct execution,
        # but metrics won't change much thanks to bounce-to-shard
        # optimization switching to the right shard before starting
        # Paxos
        after = self.get_node_metrics(self.get_ip_from_node(node), metrics=[name])
        assert after[name] - before[name] < 16, "{}:{}".format(before, after)
        cql = "DROP TABLE IF EXISTS t"
        session.execute(cql)

    @attr("single_node")
    def metrics_test(self):
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
            before = self.get_node_metrics(self.get_ip_from_node(node), metrics=[name])
            session.execute(cql)
            after = self.get_node_metrics(self.get_ip_from_node(node), metrics=[name])
            assert after[name] - before[name] == expect, "{} {}".format(before, after)

        name = "scylla_storage_proxy_coordinator_cas_write_condition_not_met"
        cql = "INSERT INTO t (a, b) VALUES (1, 1) IF NOT EXISTS"
        check1(name, cql, 0)
        check1(name, cql, 1)
        name = "scylla_cql_batches"
        check1(name, cql, 0)
        cql = "BEGIN BATCH " + cql  + " APPLY BATCH"
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

    def read_round_optimization_test(self):
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
        self.create_ks(session=session, name="lwt", rf=3)
        cql = "DROP TABLE IF EXISTS t"
        session.execute(cql)
        cql = "CREATE TABLE IF NOT EXISTS t (a INT PRIMARY KEY, b INT)"
        session.execute(cql)
        cql = "INSERT INTO t (a,b) VALUES (1,0) IF NOT EXISTS"
        stmt = SimpleStatement(cql, consistency_level =
                               ConsistencyLevel.QUORUM)
        session.execute(cql)
        cql = "UPDATE t SET b = ? WHERE a = 1 IF b = ?"
        stmt = session.prepare(cql)
        stmt.consistency_level = ConsistencyLevel.QUORUM
        name = "scylla_storage_proxy_coordinator_cas_failed_read_round_optimization"
        before = self.get_node_metrics(self.get_ip_from_node(node), metrics=[name])
        for i in range(10):
            session.execute(stmt, (i+1, i))
        after = self.get_node_metrics(self.get_ip_from_node(node), metrics=[name])
        assert after[name] - before[name] ==  0, "{} {}".format(before, after)
        cql = "DROP TABLE t"
        session.execute(cql)
        #
        # 3.6
        # use a range of statements to avoid any flakiness.
        #
        KEY_COUNT = 100
        cql = "CREATE TABLE IF NOT EXISTS t (a INT PRIMARY KEY, b INT)"
        session.execute(cql)
        cql = "INSERT INTO t (a, b) VALUES (?, ?)"
        stmt = session.prepare(cql)
        stmt.consistency_level = ConsistencyLevel.ALL
        for i in range(KEY_COUNT):
            session.execute(stmt, (i,i))

        non_paxos_stmt = session.prepare("UPDATE t SET b = 2 WHERE a = ?")
        non_paxos_stmt.consistency_level = ConsistencyLevel.QUORUM
        paxos_stmt = session.prepare("UPDATE t SET b = 3 WHERE a = ? IF b = 2")
        node1 = cluster.nodelist()[1]
        node2 = cluster.nodelist()[2]

        before = self.get_node_metrics(self.get_ip_from_node(node), metrics=[name])
        node1.stop()
        for i in range(KEY_COUNT):
            session.execute(non_paxos_stmt, (i,))
        node2.stop()
        node1.start(wait_for_binary_proto=True)
        for i in range(KEY_COUNT):
            session.execute(paxos_stmt, (i,))
        after = self.get_node_metrics(self.get_ip_from_node(node), metrics=[name])
        assert after[name] - before[name] ==  KEY_COUNT, "{} {}".format(before, after)


    def basic_distributed_test(self):
        """Basic distributed tests (3.1 - 3.4 from the test plan). """

        cluster = self.cluster
        cluster.set_configuration_options(values={"hinted_handoff_enabled": False})
        cluster.populate(3).start(wait_for_binary_proto=True)
        node1 = cluster.nodelist()[0]
        node2 = cluster.nodelist()[1]
        node3 = cluster.nodelist()[2]
        session1 = self.patient_cql_connection(node1)
        self.create_ks(session=session1, name="lwt", rf=3)
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
        assert_one(session2, cql, [2,2], cl=ConsistencyLevel.SERIAL)
        assert_one(session2, cql, [2,2], cl=ConsistencyLevel.ONE)

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
        try:
            session3.execute(cql)
            assert False, "Paxos pass in lack of quorum"
        except Unavailable as e:
            pass
        cql = "SELECT * FROM t WHERE a=3"
        assert_none(session3, cql, cl=ConsistencyLevel.ONE)
        # Have to specify custom seeds not because we need new vnodes,
        # but because the old seed node is down and we need to discocver
        # the rest of the cluster
        node2.start(wait_for_binary_proto=True, jvm_args = [
            "--seed-provider-parameters", "seeds={}".format(self.get_ip_from_node(node3))])
        cql = "SELECT * FROM t WHERE a=3"
        assert_none(session3, cql, cl=ConsistencyLevel.SERIAL)
        cql = "INSERT INTO t (a,b) VALUES (3,3) IF NOT EXISTS"
        assert_one(session3, cql, [True, None, None])
        node3.stop()
        node1.start(wait_for_binary_proto=True)
        session1 = self.patient_exclusive_cql_connection(node1, keyspace="lwt")
        cql = "SELECT * FROM t WHERE a=3"
        assert_one(session1, cql, [3,3], cl=ConsistencyLevel.SERIAL)

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

    def multi_dc_test(self):
        # 4.1 Check LOCAL_QUORUM works as expected if the other DC is not
        # available.
        cluster = self.cluster
        cluster.set_configuration_options(values={"hinted_handoff_enabled": False})
        cluster.populate([3,3]).start(wait_for_binary_proto=True)
        node1 = cluster.nodelist()[0]
        session1 = self.patient_cql_connection(node1)
        self.create_ks(session=session1, name="lwt", rf={"dc1": 3, "dc2": 3})
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
        for i in range(2,6):
            cluster.nodelist()[i].stop()
        session1.execute(stmt1, (3,))
        try:
            stmt1.serial_consistency_level = ConsistencyLevel.SERIAL
            session1.execute(stmt1, (4,))
            assert  False, "Successfully executed a Paxos query in absence of quorum"
        except Unavailable as e:
            pass
        # 4.2 Issue two Paxos LOCAL_QUORUM writes in parallel at different DC.
        # Follow up by PAXOS QUORUM read, to ensure the latest write wins.
        for i in range(4,6):
            cluster.nodelist()[i].start(wait_for_binary_proto=True)
        for i in range(0,2):
            cluster.nodelist()[i].stop()
        session2 = self.patient_exclusive_cql_connection(node2, keyspace="lwt")
        session2.execute(stmt2, (4,))
        stmt2.serial_consistency_level = ConsistencyLevel.SERIAL
        try:
            session2.execute(stmt2, (5,))
            assert  False, "Successfully executed a Paxos query in absence of quorum"
        except Unavailable as e:
            pass
        cluster.nodelist()[0].start(wait_for_binary_proto=True)
        cluster.nodelist()[3].start(wait_for_binary_proto=True)
        session2.execute(SimpleStatement("SELECT * FROM t WHERE a = 4",
                                         consistency_level=ConsistencyLevel.SERIAL))
        # 4.3 Power off two nodes in a single DC. Ensure LOCAL QUORUM Paxos
        # queries don’t work, while QUORUM LWT writes do.
        session1 = self.patient_exclusive_cql_connection(node1, keyspace="lwt")
        stmt1.serial_consistency_level = ConsistencyLevel.LOCAL_SERIAL
        try:
            session1.execute(stmt1, (6,))
            assert  False, "Successfully executed a Paxos query in absence of quorum"
        except Unavailable as e:
            pass
        stmt1.serial_consistency_level = ConsistencyLevel.SERIAL
        session1.execute(stmt1, (7,))

#
# Read Linearizability Test
#
NODES  = 3
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
    "paxos_timeout_after_save_decision" ]

class LwtReadLinearizabilityTest(Tester):

    @attr("!dtest-release")
    def read_linearizability_test(self):
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
        self.create_ks(session_a, "ks", rf=NODES)
        session_a.execute("CREATE TABLE t (id int PRIMARY KEY, v int)")

        # 1. Inject error at accept stage in B and C
        for node in [NODE_B, NODE_C]:
            self.enable_error("paxos_error_before_save_proposal", node)

        # 2. run a write(V) that suppose to succeed
        # 3. write will fail because B and C will fail
        with self.assertRaises(WriteFailure):
            ret = session_a.execute("INSERT INTO ks.t (id, v) VALUES (1, 1) IF NOT EXISTS").current_rows
            assert ret[0].applied == False

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
                debug(f"Got invalid value for node {node}: {ret}")
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

