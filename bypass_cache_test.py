import time
import tools
import pytest
import logging


from dtest_class import Tester, create_ks, create_cf, get_ip_from_node
from tools.data import create_c1c2_table, insert_c1c2
from tools.metrics import get_node_metrics


logger = logging.getLogger(__file__)


@pytest.mark.dtest_full
@pytest.mark.single_node
class TestBypassCache(Tester):
    '''
    Test that will verify if the select statement will skip cache during its read
    Introduced by commit 2a371c2689d327a10f5888f39414ec66efedb093
    '''
    NUM_OF_QUERY_EXECUTIONS = 100

    def prepare(self, nodes=1, keyspace_name='bypass_cache', rf=1, options_dict=None, table_name="user_events",
                insert_data=True):
        self.keyspace_name = keyspace_name
        self.table_name = table_name
        cluster = self.cluster
        if options_dict:
            cluster.set_configuration_options(values=options_dict)
        cluster.populate(nodes).start()
        node1 = cluster.nodelist()[0]
        session = self.patient_cql_connection(node1)
        create_ks(session=session, name=keyspace_name, rf=rf)

        if insert_data:
            create_c1c2_table(session)
            insert_c1c2(session, n=100)

        return session

    @staticmethod
    def get_scylla_cache_reads_metrics(node, metrics=None):
        if metrics is None:
            metrics = ['scylla_cache_reads']
        return get_node_metrics(get_ip_from_node(node), metrics=metrics)

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
        assert self.is_read_from_disk(node, query, session, metric=metric), \
            'Read was made from cache instead of bypass it'

    def verify_read_was_from_cache(self, node, query, session, metric=None):
        assert not self.is_read_from_disk(node, query, session, metric=metric), \
            'Read was made from disk instead of from cache'

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
        import random
        import string
        session = self.prepare(insert_data=False)
        # create a table
        create_cf(session=session, name='cf', key_type='int')
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
        assert before_query[metrics[0]] + 1 == before_query[metrics[0]], \
            f'{metrics[0]} metric was supposed to be incremented by 1 and was\'t. ' \
            f'Before={before_query[metrics[0]]} After={after_query[metrics[0]]}'
        if bypass_cache:
            assert before_query[metrics[1]] == after_query[metrics[1]], \
                f'{metrics[1]} metric wasn\'t supposed to be incremented and was. ' \
                f'Before={before_query[metrics[1]]} After={after_query[metrics[1]]}'
        else:
            assert before_query[metrics[1]] + 1 == after_query[metrics[1]], \
                f'{metrics[1]} metric was supposed to be incremented by 1 and was\'t. ' \
                f'Before={before_query[metrics[1]]} After={after_query[metrics[1]]}'

    @pytest.mark.skip('skipping until #6045 is fixed')
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

    @pytest.mark.skip('skipping until #6045 is fixed')
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
        create_c1c2_table(session, cf=self.table_name, caching=False)
        insert_c1c2(session, n=100, cf=self.table_name)
        node.flush()
        query = f'select * from {self.table_name}'
        self.verify_read_was_from_disk(node=node, query=query, session=session)

    def test_alter_table_caching_disable(self):
        session = self.prepare(insert_data=False)
        node = self.cluster.nodelist()[0]
        create_c1c2_table(session, cf=self.table_name)
        insert_c1c2(session, n=100, cf=self.table_name)
        node.flush()
        query = f'select * from {self.table_name}'
        self.verify_read_was_from_cache(node=node, query=query, session=session)
        # disabling caching for table and checking read comes from disk
        session.execute(f"ALTER TABLE {self.table_name} WITH caching = {{'enabled':false}}")
        self.verify_read_was_from_disk(node=node, query=query, session=session)

    def test_alter_table_caching_enable(self):
        session = self.prepare(insert_data=False)
        node = self.cluster.nodelist()[0]
        create_c1c2_table(session, cf=self.table_name, caching=False)
        insert_c1c2(session, n=100, cf=self.table_name)
        node.flush()
        query = f'select * from {self.table_name}'
        self.verify_read_was_from_disk(node=node, query=query, session=session)
        # enabling caching for table and checking read comes from cache
        session.execute(f"ALTER TABLE {self.table_name} WITH caching = {{'enabled':true}}")
        self.verify_read_was_from_cache(node=node, query=query, session=session)

    def verify_used_memory_grow(self, node, session):
        grew = 0
        metric = ['scylla_cache_bytes_used']
        for _ in range(self.NUM_OF_QUERY_EXECUTIONS):
            cache_bytes_used_before_write = self.get_scylla_cache_reads_metrics(node=node, metrics=metric)[metric[0]]
            insert_c1c2(session, keys=list(range(self.first_key + 10)), cf=self.table_name)
            self.cluster.nodetool(f'flush -- {self.keyspace_name} {self.table_name}')
            cache_bytes_used_after_write = self.get_scylla_cache_reads_metrics(node=node, metrics=metric)[metric[0]]
            if cache_bytes_used_before_write < cache_bytes_used_after_write:
                grew += 1
            self.first_key += 10
        return grew > self.NUM_OF_QUERY_EXECUTIONS / 2

    def test_writes_caching_disabled(self):
        session = self.prepare(insert_data=False)
        node = self.cluster.nodelist()[0]
        create_c1c2_table(session, cf=self.table_name, caching=False)
        insert_c1c2(session, n=100, cf=self.table_name)
        self.first_key = 0
        assert not self.verify_used_memory_grow(node=node, session=session), 'expected to have writes without cache'
        alter_cmd = f"ALTER TABLE {self.keyspace_name}.{self.table_name} WITH CACHING = {{'enabled': 'true'}}"
        session.execute(alter_cmd)
        assert self.verify_used_memory_grow(node=node, session=session), 'expected to have writes through cache'

    def test_writes_caching_enabled(self):
        session = self.prepare(insert_data=False)
        node = self.cluster.nodelist()[0]
        create_c1c2_table(session, cf=self.table_name)
        insert_c1c2(session, n=100, cf=self.table_name)
        self.first_key = 0
        assert self.verify_used_memory_grow(node=node, session=session), 'expected to have writes through cache'
        alter_cmd = f"ALTER TABLE {self.keyspace_name}.{self.table_name} WITH CACHING = {{'enabled': 'false'}}"
        session.execute(alter_cmd)
        assert not self.verify_used_memory_grow(node=node, session=session), 'expected to have writes without cache'
