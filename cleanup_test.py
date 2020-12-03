from dtest import Tester, debug
from cassandra import ConsistencyLevel
from tools import insert_c1c2, new_node
from nose.plugins.attrib import attr
from ccmlib.scylla_cluster import ScyllaCluster


@attr('dtest-full')
class TestCleanup(Tester):

    def prepare(self, nodes, num_keys, timeout=None, consistency=ConsistencyLevel.ALL):
        cluster = self.cluster
        if timeout:
            values = {
                'range_request_timeout_in_ms': timeout * 1000,
            }
            debug(f"Setting cluster configuration options: {values}")
            cluster.set_configuration_options(values=values)
        cluster.populate(nodes).start()
        node1 = self.cluster.nodelist()[0]
        with self.patient_cql_cluster_session(node1) as session:
            self.create_ks(session, 'ks', nodes)
            self.create_cf(session, 'cf', columns={'c1': 'text', 'c2': 'text'})
            if num_keys:
                debug(f"Inserting {num_keys} keys")
                insert_c1c2(session, keys=range(num_keys), consistency=consistency)

    @attr('next-gating', 'single_node')
    def cleanup_test(self):
        num_keys = 100000 if isinstance(self.cluster, ScyllaCluster) and self.cluster.scylla_mode != 'debug' else 10000
        timeout = self.cql_timeout(300)
        self.prepare(1, num_keys, timeout)

        debug('Restarting node')
        node1 = self.cluster.nodelist()[0]
        node1.stop()
        node1.start(wait_for_binary_proto=True)
        debug('Running cleanup')
        node1.cleanup()
        debug('Verifying number of rows')
        session = self.patient_cql_connection(node1)
        rows = session.execute("select count(*) from ks.cf;", timeout=timeout)
        self.assertEqual(rows[0][0], num_keys)

    def cluster_cleanup_test(self):
        num_keys = 100000 if isinstance(self.cluster, ScyllaCluster) and self.cluster.scylla_mode != 'debug' else 10000
        timeout = self.cql_timeout(300)
        self.prepare(3, num_keys, timeout)

        cluster = self.cluster
        node1 = cluster.nodelist()[0]
        debug('Adding a new node')
        node4 = new_node(cluster)
        node4.start(wait_for_binary_proto=True, wait_other_notice=True)
        cluster.flush()
        cluster.stop()
        cluster.start(wait_for_binary_proto=True, wait_other_notice=True)
        debug('Running cleanup')
        cluster.cleanup()
        debug('Verifying number of rows')
        session = self.patient_cql_connection(node1)
        rows = session.execute("select count(*) from ks.cf;", timeout=timeout)
        self.assertEqual(rows[0][0], num_keys)
