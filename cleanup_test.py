import os
import logging
from concurrent.futures import ThreadPoolExecutor

import pytest
from cassandra import ConsistencyLevel

from dtest_class import Tester, create_ks, create_cf
from tools.data import insert_c1c2
from tools.files import get_list_of_sstables
from ccmlib.scylla_cluster import ScyllaCluster


logger = logging.getLogger(__name__)


@pytest.mark.dtest_full
class TestCleanup(Tester):
    def prepare(self, nodes, num_keys, timeout=None, consistency=ConsistencyLevel.ALL, amount_of_tables=1):
        cluster = self.cluster
        if timeout:
            values = {
                "range_request_timeout_in_ms": timeout * 1000,
            }
            logger.info("Setting cluster configuration options: %s", values)
            cluster.set_configuration_options(values=values)
        cluster.populate(nodes).start()
        node1 = self.cluster.nodelist()[0]
        with self.patient_cql_cluster_session(node1) as session:
            create_ks(session=session, name="ks", rf=nodes)
            for i in range(amount_of_tables):
                create_cf(session=session, name=f"cf{i}", columns={"c1": "text", "c2": "text"})
            if num_keys:
                logger.info("Inserting %s keys", num_keys)
                for i in range(amount_of_tables):
                    insert_c1c2(session=session, keys=range(num_keys), consistency=consistency, cf=f"cf{i}")

    @pytest.mark.next_gating
    @pytest.mark.single_node
    def test_cleanup(self):
        num_keys = 100000 if isinstance(self.cluster, ScyllaCluster) and self.cluster.scylla_mode != "debug" else 10000
        timeout = self.cql_timeout(300)
        self.prepare(1, num_keys, timeout)

        logger.info("Restarting node")
        node1 = self.cluster.nodelist()[0]
        node1.stop()
        node1.start(wait_for_binary_proto=True)

        logger.info("Running cleanup")
        node1.cleanup()

        logger.info("Verifying number of rows")
        session = self.patient_cql_connection(node1)
        rows = session.execute("select count(*) from ks.cf0;", timeout=timeout)

        assert rows.one()[0] == num_keys

    def test_cluster_cleanup(self):
        num_keys = 100000 if isinstance(self.cluster, ScyllaCluster) and self.cluster.scylla_mode != "debug" else 10000
        timeout = self.cql_timeout(300)
        self.prepare(3, num_keys, timeout)

        cluster = self.cluster
        node1 = cluster.nodelist()[0]

        logger.info("Adding a new node")
        node4 = cluster.new_node(4)
        node4.start(wait_for_binary_proto=True, wait_other_notice=True)

        logger.info("Running cleanup")
        cluster.cleanup()

        logger.info("Verifying number of rows")
        session = self.patient_cql_connection(node1)
        rows = session.execute("select count(*) from ks.cf0;", timeout=timeout)
        assert rows.one()[0] == num_keys

    @pytest.mark.timeout(3000)
    def test_cleanup_space_amplification(self):
        num_keys = 100000 if isinstance(self.cluster, ScyllaCluster) and self.cluster.scylla_mode != "debug" else 10000
        timeout = self.cql_timeout(300)
        amount_of_tables = 100
        self.prepare(1, num_keys, timeout, amount_of_tables=amount_of_tables)

        def _get_list_of_sstables(node):
            full_size = 0
            for i in range(amount_of_tables):
                for file in get_list_of_sstables(node, "ks", f"cf{i}"):
                    try:
                        full_size += os.stat(file).st_size
                    except FileNotFoundError as ex:
                        logger.info("File %s was not found: %s", file, ex)
            return full_size

        cluster = self.cluster
        node1 = cluster.nodelist()[0]

        logger.info("Adding a new node")
        node2 = cluster.new_node(2)
        node2.start(wait_for_binary_proto=True, wait_other_notice=True)
        cluster.flush()
        cluster.stop()
        cluster.start(wait_for_binary_proto=True, wait_other_notice=True)
        size_before = _get_list_of_sstables(node1)

        def do_run_cleanup(node):
            node.cleanup()

        logger.info("Running cleanup")
        executor = ThreadPoolExecutor(max_workers=1)
        thread1 = executor.submit(do_run_cleanup, node1)

        while not thread1.done():
            size_during = _get_list_of_sstables(node1)
            assert size_before*1.02 >= size_during, f"Cleanup is not supposed to increase disk utilisation, " \
                                                    f"before={size_before} and during={size_during}"
        thread1.result()
        size_after = _get_list_of_sstables(node1)
        assert size_before > size_after, f"Cleanup is supposed to decrease disk utilisation, " \
                                         f"before={size_before} and after={size_after}"
