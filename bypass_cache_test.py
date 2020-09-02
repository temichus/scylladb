from nose.tools import assert_true, assert_false
from nose.plugins.attrib import attr
from dtest import Tester
from unittest import skip
import time
import tools


@attr('dtest-full', 'single_node')
class TestBypassCache(Tester):
    '''
    Test that will verify if the select statement will skip cache during its read
    Introduced by commit 2a371c2689d327a10f5888f39414ec66efedb093
    '''
    NUM_OF_QUERY_EXECUTIONS = 100
    def prepare(self, nodes=1, keyspace_name='bypass_cache', rf=1, options_dict=None, table_name='user_events',
                insert_data=True):
        self.keyspace_name = keyspace_name
        self.table_name = table_name
        cluster = self.cluster
        if options_dict:
            cluster.set_configuration_options(values=options_dict)
        cluster.populate(nodes).start()
        node1 = cluster.nodelist()[0]
        session = self.patient_cql_connection(node1)
        self.create_ks(session=session, name=keyspace_name, rf=rf)

        if insert_data:
            tools.create_c1c2_table(self, session)
            tools.insert_c1c2(session, n=100)

        return session

    def get_scylla_cache_reads_metrics(self, node, metrics=None):
        if metrics is None:
            metrics = ['scylla_cache_reads']
        return self.get_node_metrics(self.get_ip_from_node(node), metrics=metrics)

    def is_read_from_disk(self, node, query, session, metric=None):
        if metric is None:
            metric = ['scylla_cache_reads']
        if not isinstance(metric, list):
            metric = [metric]
        cache_read_before_bypass_read = self.get_scylla_cache_reads_metrics(node=node, metrics=metric)[metric[0]]
        for _ in range(self.NUM_OF_QUERY_EXECUTIONS):
            session.execute(query)
        cache_read_after_bypass_read = self.get_scylla_cache_reads_metrics(node=node, metrics=metric)[metric[0]]
        return cache_read_after_bypass_read * 0.95 <= cache_read_before_bypass_read <= \
            cache_read_after_bypass_read * 1.05

    def verify_read_was_from_disk(self, node, query, session, metric=None):
        assert_true(self.is_read_from_disk(node, query, session, metric=metric),
                    'Read was made from cache instead of bypass it')

    def verify_read_was_from_cache(self, node, query, session, metric=None):
        assert_false(self.is_read_from_disk(node, query, session, metric=metric),
                     'Read was made from disk instead of from cache')

    def test_simple_bypass_cache(self):
        session = self.prepare()
        node = self.cluster.nodelist()[0]
        query = 'SELECT * FROM cf BYPASS CACHE'

        self.verify_read_was_from_disk(node=node, query=query, session=session)

    def test_multiple_bypass_cache(self):
        session = self.prepare()
        node = self.cluster.nodelist()[0]
        time.sleep(3)
        for _ in range(20):
            query = 'SELECT * FROM cf BYPASS CACHE'
            self.verify_read_was_from_disk(node=node, query=query, session=session)

    def test_read_from_cache_and_then_bypass_cache(self):
        session = self.prepare()
        node = self.cluster.nodelist()[0]

        no_bypass_query = 'SELECT * FROM cf'
        self.verify_read_was_from_cache(node=node, query=no_bypass_query, session=session)

        bypass_query = 'SELECT * FROM cf BYPASS CACHE'
        self.verify_read_was_from_disk(node=node, query=bypass_query, session=session)

    def insert_data_for_scan_range(self):
        import random, string
        session = self.prepare(insert_data=False)
        # create a table
        self.create_cf(session=session, name='cf', key_type='int')
        # populate table with k (int) and values (anything)
        query = 'INSERT INTO cf (key, c, v) VALUES ({}, \'{}\', \'{}\')'
        for idx in range(0, 100):
            varchar_c = ''.join(random.choice(string.ascii_lowercase) for x in range(20))
            varchar_v = ''.join(random.choice(string.ascii_lowercase) for x in range(20))
            session.execute(query.format(idx, varchar_c, varchar_v))
        return session

    def run_query_compare_select_metrics(self, session, node, query, bypass_cache, metrics=None):
        before_query = self.get_scylla_cache_reads_metrics(node=node,
                                                           metrics=metrics)
        session.execute(query)
        after_query = self.get_scylla_cache_reads_metrics(node=node,
                                                          metrics=metrics)
        assert_true(before_query[metrics[0]] + 1 == before_query[metrics[0]],
                    f'{metrics[0]} metric was supposed to be incremented by 1 and was\'t.'
                    f'Before={before_query[metrics[0]]} After={after_query[metrics[0]]}')
        if bypass_cache:
            assert_true(before_query[metrics[1]] ==
                        after_query[metrics[1]],
                        f'{metrics[1]} metric wasn\'t supposed to be incremented '
                        f'and was. Before={before_query[metrics[1]]} After={after_query[metrics[1]]}')
        else:
            assert_true(before_query[metrics[1]] + 1 ==
                        after_query[metrics[1]],
                        f'{metrics[1]} metric was supposed to be incremented by 1 '
                        f'and was\'t. Before={before_query[metrics[1]]} After={after_query[metrics[1]]}')

    @skip('skipping until #6045 is fixed')
    def test_range_scan_bypass_cache(self):
        session = self.insert_data_for_scan_range()
        node = self.cluster.nodelist()[0]
        partition_range_scan_metric = 'select_partition_range_scan'
        partition_range_scan_no_bypass_cache_metric = 'select_partition_range_scan_no_bypass_cache'
        range_scan_no_bypass_cache = 'SELECT * FROM cf WHERE key > 10 and key < 20 ALLOW FILTERING'
        range_scan_bypass_cache = 'SELECT * FROM cf WHERE key > 10 and key < 20 ALLOW FILTERING BYPASS CACHE'
        self.run_query_compare_select_metrics(session=session, node=node, query=range_scan_no_bypass_cache,
                                              bypass_cache=False, metrics=[partition_range_scan_metric,
                                                                           partition_range_scan_no_bypass_cache_metric])
        self.run_query_compare_select_metrics(session=session, node=node, query=range_scan_bypass_cache,
                                              bypass_cache=True, metrics=[partition_range_scan_metric,
                                                                          partition_range_scan_no_bypass_cache_metric])

    @skip('skipping until #6045 is fixed')
    def test_full_scan_bypass_cache(self):
        session = self.prepare()
        node = self.cluster.nodelist()[0]
        partition_range_scan_metric = 'select_partition_range_scan'
        partition_range_scan_no_bypass_cache_metric = 'select_partition_range_scan_no_bypass_cache'
        full_scan_no_bypass_cache = 'SELECT * FROM cf'
        full_scan_bypass_cache = 'SELECT * FROM cf BYPASS CACHE'
        self.run_query_compare_select_metrics(session=session, node=node, query=full_scan_no_bypass_cache,
                                              bypass_cache=False, metrics=[partition_range_scan_metric,
                                                                           partition_range_scan_no_bypass_cache_metric])
        self.run_query_compare_select_metrics(session=session, node=node, query=full_scan_bypass_cache,
                                              bypass_cache=True, metrics=[partition_range_scan_metric,
                                                                          partition_range_scan_no_bypass_cache_metric])

    def test_create_table_caching_disabled(self):
        session = self.prepare(insert_data=False)
        node = self.cluster.nodelist()[0]
        tools.create_c1c2_table(self, session, cf=self.table_name, caching=False)
        tools.insert_c1c2(session, n=100, cf=self.table_name)
        node.flush()
        query = f'select * from {self.table_name}'
        self.verify_read_was_from_disk(node=node, query=query, session=session)

    def test_alter_table_caching_disable(self):
        session = self.prepare(insert_data=False)
        node = self.cluster.nodelist()[0]
        tools.create_c1c2_table(self, session, cf=self.table_name)
        tools.insert_c1c2(session, n=100, cf=self.table_name)
        node.flush()
        query = f'select * from {self.table_name}'
        self.verify_read_was_from_cache(node=node, query=query, session=session)
        # disabling caching for table and checking read comes from disk
        session.execute(f"ALTER TABLE {self.table_name} WITH caching = {{'enabled':false}}")
        self.verify_read_was_from_disk(node=node, query=query, session=session)

    def test_alter_table_caching_enable(self):
        session = self.prepare(insert_data=False)
        node = self.cluster.nodelist()[0]
        tools.create_c1c2_table(self, session, cf=self.table_name, caching=False)
        tools.insert_c1c2(session, n=100, cf=self.table_name)
        node.flush()
        query = f'select * from {self.table_name}'
        self.verify_read_was_from_disk(node=node, query=query, session=session)
        # enabling caching for table and checking read comes from cache
        session.execute(f"ALTER TABLE {self.table_name} WITH caching = {{'enabled':true}}")
        self.verify_read_was_from_cache(node=node, query=query, session=session)
