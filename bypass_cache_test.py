from nose.tools import assert_true, assert_false
from dtest import Tester

import time
import tools


class TestBypassCache(Tester):
    '''
    Test that will verify if the select statement will skip cache during its read
    Introduced by commit 2a371c2689d327a10f5888f39414ec66efedb093
    '''
    def prepare(self, nodes=1, keyspace_name='bypass_cache', rf=1, options_dict=None, table_name='user_events'):
        self.keyspace_name = keyspace_name
        self.table_name = table_name
        cluster = self.cluster
        if options_dict:
            cluster.set_configuration_options(values=options_dict)
        cluster.populate(nodes).start()
        node1 = cluster.nodelist()[0]
        session = self.patient_cql_connection(node1)
        self.create_ks(session=session, name=keyspace_name, rf=rf)

        tools.create_c1c2_table(self, session)
        tools.insert_c1c2(session, n=100)

        return session

    def get_scylla_cache_reads_metrics(self, node, metrics=None):
        if metrics is None:
            metrics = ['scylla_cache_reads']
        return self.get_node_metrics(self.get_ip_from_node(node), metrics=metrics)

    def is_read_from_disk(self, node, query, session):
        cache_read_before_bypass_read = self.get_scylla_cache_reads_metrics(node=node)
        session.execute(query)
        cache_read_after_bypass_read = self.get_scylla_cache_reads_metrics(node=node)
        return cache_read_before_bypass_read['scylla_cache_reads'] == cache_read_after_bypass_read['scylla_cache_reads']

    def verify_read_was_from_disk(self, node, query, session):
        assert_true(self.is_read_from_disk(node, query, session), 'Read was made from cache instead of bypass it')

    def verify_read_was_from_cache(self, node, query, session):
        assert_false(self.is_read_from_disk(node, query, session), 'Read was made from disk instead of from cache')

    def test_simple_bypass_cache(self):
        session = self.prepare()
        node = self.cluster.nodelist()[0]
        query = 'SELECT * FROM cf BYPASS CACHE'

        self.verify_read_was_from_disk(node=node, query=query, session=session)

    def test_multiple_bypass_cache(self):
        session = self.prepare()
        node = self.cluster.nodelist()[0]
        time.sleep(3)
        for _ in xrange(20):
            query = 'SELECT * FROM cf BYPASS CACHE'
            self.verify_read_was_from_disk(node=node, query=query, session=session)

    def test_read_from_cache_and_then_bypass_cache(self):
        session = self.prepare()
        node = self.cluster.nodelist()[0]

        no_bypass_query = 'SELECT * FROM cf'
        self.verify_read_was_from_cache(node=node, query=no_bypass_query, session=session)

        bypass_query = 'SELECT * FROM cf BYPASS CACHE'
        self.verify_read_was_from_disk(node=node, query=bypass_query, session=session)
