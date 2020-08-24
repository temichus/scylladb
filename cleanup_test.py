from dtest import Tester, debug
from cassandra import ConsistencyLevel
from tools import insert_c1c2, new_node
from nose.plugins.attrib import attr
from ccmlib.scylla_cluster import ScyllaCluster


@attr('dtest-full')
class TestCleanup(Tester):

    @attr('next-gating', 'single_node')
    def cleanup_test(self):
        cluster = self.cluster
        cluster.populate(1).start(wait_for_binary_proto=True)
        node1 = cluster.nodelist()[0]
        debug('Inserting data')
        num_keys = 100000 if isinstance(self.cluster, ScyllaCluster) and self.cluster.scylla_mode != 'debug' else 10000
        with self.patient_cql_cluster_session(node1) as session:
            self.create_ks(session, 'ks', 1)
            self.create_cf(session, 'cf', columns={'c1': 'text', 'c2': 'text'})
            insert_c1c2(session, keys=range(num_keys), consistency=ConsistencyLevel.ALL)

        debug('Restarting node')
        node1.stop()
        node1.start(wait_for_binary_proto=True)
        debug('Running cleanup')
        node1.cleanup()
        debug('Verifying number of rows')
        session = self.patient_cql_connection(node1)
        rows = session.execute("select count(*) from ks.cf;");
        self.assertEqual(rows[0][0], num_keys)

    def cluster_cleanup_test(self):
        cluster = self.cluster
        cluster.populate(3).start(wait_for_binary_proto=True,wait_other_notice=True)
        node1 = cluster.nodelist()[0]
        session = self.patient_cql_connection(node1)
        self.create_ks(session, 'ks', 3)
        self.create_cf(session, 'cf', columns={'c1': 'text', 'c2': 'text'})
        insert_c1c2(session, keys=range(100000), consistency=ConsistencyLevel.ALL)
        session.shutdown()

        node4 = new_node(cluster)
        node4.start(wait_for_binary_proto=True, wait_other_notice=True)
        cluster.flush()
        cluster.stop()
        cluster.start(wait_for_binary_proto=True,wait_other_notice=True)
        for node in cluster.nodelist():
            node.cleanup()
        session = self.patient_cql_connection(node1)
        rows = session.execute("select count(*) from ks.cf;");
        self.assertEqual(rows[0][0],100000)

