import logging

import pytest
from cassandra import ConsistencyLevel

from dtest_class import Tester, create_ks, create_cf
from tools.data import insert_c1c2
from ccmlib.scylla_cluster import ScyllaCluster


logger = logging.getLogger(__name__)


@pytest.mark.dtest_full
class TestCleanup(Tester):
    def prepare(self, nodes, num_keys, timeout=None, consistency=ConsistencyLevel.ALL):
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
            create_cf(session=session, name="cf", columns={"c1": "text", "c2": "text"})
            if num_keys:
                logger.info("Inserting %s keys", num_keys)
                insert_c1c2(session, keys=range(num_keys), consistency=consistency)

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
        rows = session.execute("select count(*) from ks.cf;", timeout=timeout)

        assert rows[0][0] == num_keys

    def test_cluster_cleanup(self):
        num_keys = 100000 if isinstance(self.cluster, ScyllaCluster) and self.cluster.scylla_mode != "debug" else 10000
        timeout = self.cql_timeout(300)
        self.prepare(3, num_keys, timeout)

        cluster = self.cluster
        node1 = cluster.nodelist()[0]

        logger.info("Adding a new node")
        node4 = cluster.new_node(4)
        node4.start(wait_for_binary_proto=True, wait_other_notice=True)
        cluster.flush()
        cluster.stop()
        cluster.start(wait_for_binary_proto=True, wait_other_notice=True)

        logger.info("Running cleanup")
        cluster.cleanup()

        logger.info("Verifying number of rows")
        session = self.patient_cql_connection(node1)
        rows = session.execute("select count(*) from ks.cf;", timeout=timeout)

        assert rows[0][0] == num_keys
