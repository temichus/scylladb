import logging

import pytest
from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement
from ccmlib.common import is_win

from dtest_class import Tester
from jmxutils import JolokiaAgent, make_mbean, remove_perf_disable_shared_mem
from tools.data import query_c1c2, insert_c1c2

logger = logging.getLogger(__name__)


@pytest.mark.skip('The following commands do not exist: "forceRepairAsync" and "forceRepairRangeAsync"')
class TestDeprecatedRepairAPI(Tester):
    """
    @jira_ticket CASSANDRA-9570

    Test if deprecated repair JMX API runs with expected parameters
    """

    def check_rows_on_node(self, node_to_check, rows, found=None, missings=None, restart=True):
        found = found or []
        missings = missings or []
        stopped_nodes = []

        for node in self.cluster.nodes.values():
            if node.is_running() and node is not node_to_check:
                stopped_nodes.append(node)
                node.stop(wait_other_notice=True)

        session = self.patient_cql_connection(node_to_check, 'ks')
        result = list(session.execute("SELECT * FROM cf LIMIT %d" % (rows * 2)))
        assert len(result) == rows, len(result)

        for key in found:
            query_c1c2(session=session, key=key, consistency=ConsistencyLevel.ONE)

        for key in missings:
            query = SimpleStatement("SELECT c1, c2 FROM cf WHERE key='k%d'" % key,
                                    consistency_level=ConsistencyLevel.ONE)
            res = session.execute(query)
            assert len(filter(lambda x: len(x) != 0, res)) == 0, res

        if restart:
            for node in stopped_nodes:
                node.start(wait_other_notice=True)

    def test_force_repair_async_1(self, ):
        """
        test forceRepairAsync(String keyspace, boolean isSequential,
                              Collection<String> dataCenters,
                              Collection<String> hosts,
                              boolean primaryRange, boolean fullRepair, String... columnFamilies)
        """
        opt = self._deprecated_repair_jmx("forceRepairAsync(java.lang.String,boolean,java.util.Collection,"
                                          "java.util.Collection,boolean,boolean,[Ljava.lang.String;)",
                                          ['ks', True, [], [], False, False, ["cf"]])
        assert opt["parallelism"] == "parallel" if is_win() else "sequential", opt
        assert opt["primary_range"] == "false", opt
        assert opt["incremental"] == "true", opt
        assert opt["job_threads"] == "1", opt
        assert opt["data_centers"] == "[]", opt
        assert opt["hosts"] == "[]", opt
        assert opt["column_families"] == "[cf]", opt

    def test_force_repair_async_2(self):
        """
        test forceRepairAsync(String keyspace, int parallelismDegree,
                              Collection<String> dataCenters,
                              Collection<String> hosts,
                              boolean primaryRange, boolean fullRepair, String... columnFamilies)
        """
        opt = self._deprecated_repair_jmx("forceRepairAsync(java.lang.String,int,java.util.Collection,j"
                                          "ava.util.Collection,boolean,boolean,[Ljava.lang.String;)",
                                          ['ks', 1, [], [], True, True, []])
        assert opt["parallelism"] == "parallel", opt
        assert opt["primary_range"] == "true", opt
        assert opt["incremental"] == "false", opt
        assert opt["job_threads"] == "1", opt
        assert opt["data_centers"] == "[]", opt
        assert opt["hosts"] == "[]", opt
        assert opt["column_families"] == "[]", opt

    def test_force_repair_async_3(self, ):
        """
        test forceRepairAsync(String keyspace, boolean isSequential,
                              boolean isLocal, boolean primaryRange,
                              boolean fullRepair, String... columnFamilies)
        """
        opt = self._deprecated_repair_jmx("forceRepairAsync(java.lang.String,boolean,boolean,boolean,boolean,"
                                          "[Ljava.lang.String;)", ['ks', False, False, False, False, ["cf"]])
        assert opt["parallelism"] == "parallel", opt
        assert opt["primary_range"] == "false", opt
        assert opt["incremental"] == "true", opt
        assert opt["job_threads"] == "1", opt
        assert opt["data_centers"] == "[]", opt
        assert opt["hosts"] == "[]", opt
        assert opt["column_families"] == "[cf]", opt

    def test_force_repair_range_async_1(self, ):
        """
        test forceRepairRangeAsync(String beginToken, String endToken,
                                   String keyspaceName, boolean isSequential,
                                   Collection<String> dataCenters,
                                   Collection<String> hosts, boolean fullRepair,
                                   String... columnFamilies)
        """
        opt = self._deprecated_repair_jmx("forceRepairRangeAsync(java.lang.String,java.lang.String,java.lang.String,"
                                          "boolean,java.util.Collection,java.util.Collection,boolean,[Ljava.lang."
                                          "String;)", ["0", "1000", "ks", True, ["dc1"], [], False, ["cf"]])
        assert opt["parallelism"] == "parallel" if is_win() else "sequential", opt
        assert opt["primary_range"] == "false", opt
        assert opt["incremental"] == "true", opt
        assert opt["job_threads"] == "1", opt
        assert opt["data_centers"] == "[dc1]", opt
        assert opt["hosts"] == "[]", opt
        assert opt["ranges"] == "1", opt
        assert opt["column_families"] == "[cf]", opt

    def test_force_repair_range_async_2(self, ):
        """
        test forceRepairRangeAsync(String beginToken, String endToken,
                                   String keyspaceName, int parallelismDegree,
                                   Collection<String> dataCenters,
                                   Collection<String> hosts,
                                   boolean fullRepair, String... columnFamilies)
        """
        opt = self._deprecated_repair_jmx("forceRepairRangeAsync(java.lang.String,java.lang.String,java.lang.String,"
                                          "int,java.util.Collection,java.util.Collection,boolean,[Ljava.lang.String;)",
                                          ["0", "1000", "ks", 2, [], [], True, ["cf"]])
        assert opt["parallelism"] == "parallel" if is_win() else "dc_parallel", opt
        assert opt["primary_range"] == "false", opt
        assert opt["incremental"] == "false", opt
        assert opt["job_threads"] == "1", opt
        assert opt["data_centers"] == "[]", opt
        assert opt["hosts"] == "[]", opt
        assert opt["ranges"] == "1", opt
        assert opt["column_families"] == "[cf]", opt

    def force_repair_range_async_3_test(self):
        """
        test forceRepairRangeAsync(String beginToken, String endToken,
                                   String keyspaceName, boolean isSequential,
                                   boolean isLocal, boolean fullRepair,
                                   String... columnFamilies)
        """
        opt = self._deprecated_repair_jmx(
            "forceRepairRangeAsync(java.lang.String,java.lang.String,java.lang.String,boolean,boolean,boolean,"
            "[Ljava.lang.String;)", ["0", "1000", "ks", True, True, True, ["cf"]])
        assert opt["parallelism"] == "parallel" if is_win() else "sequential", opt
        assert opt["primary_range"] == "false", opt
        assert opt["incremental"] == "false", opt
        assert opt["job_threads"] == "1", opt
        assert opt["data_centers"] == "[dc1]", opt
        assert opt["hosts"] == "[]", opt
        assert opt["ranges"] == "1", opt
        assert opt["column_families"] == "[cf]", opt

    def _deprecated_repair_jmx(self, method, arguments):
        cluster = self.cluster

        logger.info("Starting cluster..")
        cluster.populate([1, 1])
        node1 = cluster.nodelist()[0]
        remove_perf_disable_shared_mem(node1)
        cluster.start()

        session = self.patient_cql_connection(node1)
        self.create_ks(session, 'ks', 2)
        self.create_cf(session, 'cf', read_repair=0.0, columns={'c1': 'text', 'c2': 'text'})

        insert_c1c2(session, n=1000, consistency=ConsistencyLevel.ALL)

        # Run repair
        mbean = make_mbean('db', 'StorageService')
        with JolokiaAgent(node1) as jmx:
            # assert repair runs and returns valid cmd number
            self.assertEqual(jmx.execute_method(mbean, method, arguments), 1)
        # wait for log to start
        node1.watch_log_for("Starting repair command")
        # get repair parameters from the log
        lines = node1.grep_log(
            r"Starting repair command #1, repairing keyspace ks with repair options \(parallelism: (?P<parallelism>\w+)"
            r", primary range: (?P<pr>\w+), incremental: (?P<incremental>\w+), job threads: (?P<jobs>\d+),"
            r" ColumnFamilies: (?P<cfs>.+), dataCenters: (?P<dc>.+), hosts: (?P<hosts>.+), # of ranges: "
            r"(?P<ranges>\d+)\)")
        self.assertEqual(len(lines), 1)
        _, match = lines[0]
        return {"parallelism": match.group("parallelism"),
                "primary_range": match.group("pr"),
                "incremental": match.group("incremental"),
                "job_threads": match.group("jobs"),
                "column_families": match.group("cfs"),
                "data_centers": match.group("dc"),
                "hosts": match.group("hosts"),
                "ranges": match.group("ranges")}
