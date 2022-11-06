import os
import logging
import time
from concurrent.futures import ThreadPoolExecutor

import pytest
from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement

from dtest_class import Tester, create_ks, create_cf
from tools.data import insert_c1c2, delete_c1c2
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

    # Reproducer for https://github.com/scylladb/scylladb/issues/1239
    def test_cluster_cleanup_no_resurrection(self):
        """
        - Write data to 2-node cluster
        - Add node
        - Run cleanup
        - Delete data
        - Wait for tombstones to expire
        - Run major compaction (this will get rid of both data and tombstones)
        - Remove the added node
        - Verify there are no readable keys.
        - Any original data that wasn't cleaned up properly would get resurrected at this stage.
        """
        num_keys = 1000
        self.prepare(nodes=2, num_keys=num_keys)
        cluster = self.cluster
        node1, node2 = cluster.nodelist()
        session = self.patient_cql_connection(node1)
        gc_grace_seconds = 0
        session.execute(f"ALTER TABLE ks.cf0 WITH gc_grace_seconds={gc_grace_seconds}")

        logger.info("Adding a new node")
        new_node = cluster.new_node(len(cluster.nodelist()) + 1)
        new_node.start(wait_for_binary_proto=True, wait_other_notice=True)

        logger.info("Running cleanup")
        cluster.nodetool('cleanup ks')

        logger.info("Deleting data")
        delete_c1c2(session, n=num_keys, cf='cf0')

        logger.info("Verifying data")
        query = SimpleStatement("SELECT count(*) FROM ks.cf0", consistency_level=ConsistencyLevel.QUORUM)
        rows = session.execute(query)
        assert rows.one()[0] == 0

        logger.info(f"Sleeping until gc_grace_seconds={gc_grace_seconds} pass")
        time.sleep(gc_grace_seconds + 1)
        logger.info("Running compaction")
        cluster.compact()
        cluster.wait_for_compactions()

        new_node_hostid = new_node.hostid()
        logger.debug(f"Remove node {new_node.name} (host id {new_node_hostid})")
        new_node.stop(wait_other_notice=True)
        node1.removenode(new_node_hostid)

        node1.nodetool('snapshot ks -t after_removenode')

        logger.info("Reverifying data")
        rows = session.execute(query)
        assert rows.one()[0] == 0
