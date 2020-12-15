import uuid

from cassandra import ConsistencyLevel as CL
from cassandra.query import SimpleStatement

from datahelp import create_rows
from paging_test import PageFetcher, BasePagingTester, PageAssertionMixin
from scylla_tools import scylla_mode
from dtest import debug
from nose.plugins.attrib import attr


@attr('dtest-full')
class TestAggregatePaging(BasePagingTester, PageAssertionMixin):
    """
    Basic aggregation tests using paging
    """

    def _test_paged_count_with_limit(self, sizes):
        session = self.prepare()
        self.create_ks(session, 'test_aggregate_paging', 2)
        session.execute("CREATE TABLE paging_test ( id uuid PRIMARY KEY, value text )")

        def random_txt(text):
            return uuid.uuid4()

        data = """
               | id     |value   |
               +--------+--------+
          *5001| [uuid] |testing |
            """
        create_rows(data, session, 'paging_test', cl=CL.ALL, format_funcs={'id': random_txt, 'value': str})

        # Note that both page size and limit is less than actual num of rows.
        # Thus we should always get a count == limit
        # They main thing we are testing is that page size > limit does not break the
        # count result. (#650)
        for page_size in sizes:
            future = session.execute_async(
                SimpleStatement("select count(*) from paging_test limit {}".format(1),
                                fetch_size=page_size, consistency_level=CL.ALL)
            )
            pf = PageFetcher(future).request_all()
            self.assertEqual(pf.num_results_all(), [1])
            self.assertEqual(pf.all_data(), [{u'count': 5001}])

    @scylla_mode('!debug')
    def test_paged_count_with_limit(self):
        self._test_paged_count_with_limit([10, 100, 1000, 3000, 5000])

    @scylla_mode('debug')
    def test_paged_count_with_limit_debug(self):
        self._test_paged_count_with_limit([10, 100, 250])

    def _test_paged_count_with_clustering_key(self, order):
        session = self.prepare()
        self.create_ks(session, 'test_aggregate_paging', 2)
        session.execute("CREATE TABLE paging_test (pk int, ck1 int, ck2 text, v int, PRIMARY KEY(pk, ck1, ck2))")

        def random_txt(text):
            return str(uuid.uuid4())

        data = """
               | pk | ck1 | ck2          | v |
               +----+-----+--------------+---+
          *1234| 0  | 1   | [random_txt] | 0 |
               """

        create_rows(data, session, 'paging_test', cl=CL.ALL, format_funcs={
                    'pk': int, 'ck1': int, 'ck2': random_txt, 'v': int})

        future = session.execute_async(
            SimpleStatement("select count(*) from paging_test where pk = 0 and ck1 = 1 order by ck1 {}, ck2 {}".format(
                order, order), fetch_size=100, consistency_level=CL.ALL)
        )
        pf = PageFetcher(future).request_all()
        self.assertEqual(pf.num_results_all(), [1])
        self.assertEqual(pf.all_data(), [{u'count': 1234}])

    @attr('next-gating')
    @attr('dtest-debug')
    def test_paged_count_with_clustering_key(self):
        self._test_paged_count_with_clustering_key('asc')

    def test_paged_count_with_clustering_key_reversed(self):
        self._test_paged_count_with_clustering_key('desc')


@attr('dtest-full')
class TestPagingSavedQueryStateBase(BasePagingTester):
    LOOKUPS = 'querier_cache_lookups'
    MISSES = 'querier_cache_misses'
    DROPS = 'querier_cache_drops'
    TIME_BASED_EVICTIONS = 'querier_cache_time_based_evictions'
    RESOURCE_BASED_EVICTIONS = 'querier_cache_resource_based_evictions'
    MEMORY_BASED_EVICTIONS = 'querier_cache_memory_based_evictions'
    POPULATION = 'querier_cache_population'

    ALL_METRICS = [LOOKUPS, MISSES, DROPS, TIME_BASED_EVICTIONS, RESOURCE_BASED_EVICTIONS, MEMORY_BASED_EVICTIONS]

    def metrics_equal(self, node_metrics, expected_metrics):
        for metric in self.ALL_METRICS:
            expected_metric = expected_metrics.get(metric.replace("querier_cache_", ""), 0)

            if expected_metric == -1:
                continue

            if node_metrics[metric] != expected_metric:
                debug(
                    f"metrics_equal: node_metrics[{metric}] {node_metrics[metric]} != {expected_metric} expected_metric")
                return False

        return True

    def match_node_metrics(self, node, expected_metrics, matched):
        if expected_metrics is None:
            return

        node_metrics = self.get_node_metrics(self.get_ip_from_node(node), metrics=self.ALL_METRICS)
        debug('{} metrics: {}'.format(node.name, node_metrics))

        matched_any = False

        for i, metrics_variant in enumerate(expected_metrics):
            if self.metrics_equal(node_metrics, metrics_variant):
                matched.add(i)
                matched_any = True

        if not matched_any:
            debug("Node metrics doesn't match any of the expected metrics:"
                  "\nnode_metrics: {}\nexpected_metrics: {}".format(node_metrics, expected_metrics))

        # The node's metrics must match at least one expected metrics
        self.assertEqual(matched_any, True)

    def assert_nodes_metrics(self, expected_metrics):
        nodes = self.cluster.nodelist()

        matched = set()

        for node in nodes:
            self.match_node_metrics(node, expected_metrics, matched)

        # All expected metrics have to match at least node's metrics
        self.assertEqual(len(matched), len(expected_metrics))


@attr('dtest-full')
class TestLargePaging(TestPagingSavedQueryStateBase, PageAssertionMixin):
    """
    Tests for queries attempting to fetch large pages
    """
    KS_NAME = 'test_large_paging'
    CF_NAME = 'paging_test'

    def setUp(self, *args, **kwargs):
        super(TestLargePaging, self).setUp(*args, **kwargs)
        self.session = self.prepare()
        self.create_ks(self.session, self.KS_NAME, 2)

    def fill_data(self, data, data_size, keys, vals, format_funcs={}):
        def get_key(text):
            return str(uuid.uuid4())

        def get_data(text):
            return ' ' * data_size

        format_funcs.update({key: get_key for key in keys})
        format_funcs.update({val: get_data for val in vals})
        create_rows(data, self.session, self.CF_NAME, cl=CL.ALL, format_funcs=format_funcs)

    def validate_data(self, query, fetch_size, row_cnt, validate_metrics=False):
        future = self.session.execute_async(
            SimpleStatement(query, fetch_size=fetch_size, consistency_level=CL.ALL)
        )
        pf = PageFetcher(future).request_all()
        all_pages = pf.num_results_all()

        self.assertEqual(sum(all_pages), row_cnt)
        for page in all_pages:
            self.assertLessEqual(page, fetch_size)

        if validate_metrics:
            self.assert_nodes_metrics(
                ({'lookups': pf.requested_pages - 1,
                  'misses': -1,
                  'resource_based_evictions': -1},
                 {}))

    @attr('next-gating')
    @attr('dtest-debug')
    def test_large_page_range_queries(self):
        self.session.execute("CREATE TABLE %s (pk text, ck text, v text, PRIMARY KEY(pk, ck))" % self.CF_NAME)

        data = """
               | pk        | ck        | v          |
               +-----------+-----------+------------+
          *1000| [get_key] | [get_key] | [get_data] |
               """

        self.fill_data(data=data, data_size=64 * 1024, keys=['pk', 'ck'], vals=['v'])
        self.validate_data(query="select * from %s" % self.CF_NAME, fetch_size=1000, row_cnt=1000)

    @attr('next-gating')
    @attr('dtest-debug')
    def test_large_page_range_queries_static_columns(self):
        self.session.execute("CREATE TABLE %s (pk text, ck text, s text static, v text, PRIMARY KEY(pk, ck))" %
                             self.CF_NAME)

        data = """
               | pk        | s          |
               +-----------+------------+
          *1000| [get_key] | [get_data] |
               """

        self.fill_data(data=data, data_size=64 * 1024, keys=['pk'], vals=['s'])
        self.validate_data(query="select * from %s" % self.CF_NAME, fetch_size=1000, row_cnt=1000)

    def test_large_page_single_partition(self):
        self.session.execute("CREATE TABLE %s (pk int, ck text, v text, PRIMARY KEY(pk, ck))" % self.CF_NAME)

        data = """
               | pk        | ck        | v          |
               +-----------+-----------+------------+
          *1000| 0         | [get_key] | [get_data] |
               """

        self.fill_data(data=data, data_size=32 * 1024, keys=['ck'], vals=['v'], format_funcs={'pk': int})
        self.validate_data(query="select * from %s where pk = 0" % self.CF_NAME, fetch_size=400, row_cnt=1000,
                           validate_metrics=True)

    def test_small_page_single_partition(self):
        self.session.execute("CREATE TABLE %s (pk int, ck text, v text, PRIMARY KEY(pk, ck))" % self.CF_NAME)

        data = """
               | pk        | ck        | v          |
               +-----------+-----------+------------+
          *1000| 0         | [get_key] | [get_data] |
               """

        self.fill_data(data=data, data_size=32 * 1024, keys=['ck'], vals=['v'], format_funcs={'pk': int})
        self.validate_data(query="select * from %s where pk = 0" % self.CF_NAME, fetch_size=15, row_cnt=1000,
                           validate_metrics=True)


@attr('dtest-full')
class TestPagingSavedQueryStateSingularRanges(TestPagingSavedQueryStateBase):
    """
    Tests concerned with querier-reuse during paging.
    """

    def setup_simple_table(self, **kwargs):
        self.create_ks(self.session, 'paging_additional_test_querier_reuse', 2)
        query = "CREATE TABLE test_singular (pk int, ck int, val text, PRIMARY KEY (pk, ck))"

        if len(kwargs) > 0:
            options = []
            for key, val in kwargs.items():
                if type(val) is float or type(val) is int:
                    options.append("{}={}".format(key, val))
                else:
                    options.append("{}='{}'".format(key, val))

            query += " WITH " + " AND ".join(options)

        self.session.execute(query)

        data = """
             | pk | ck | val    |
             +----+----+--------+
             | 1  | 1  | val1_1 |
             | 1  | 2  | val1_2 |
             | 1  | 3  | val1_3 |
             | 1  | 4  | val1_4 |
             | 2  | 1  | val2_1 |
             | 2  | 2  | val2_2 |
             | 2  | 3  | val2_3 |
             | 2  | 4  | val2_4 |
             | 2  | 5  | val2_5 |
             | 2  | 6  | val2_6 |
        """

        create_rows(data, self.session, 'test_singular', cl=CL.ALL,
                    format_funcs={
                        'pk': int,
                        'ck': int,
                        'val': str,
                    })

        return [
            {'pk': 1, 'ck': 1, 'val': 'val1_1'},
            {'pk': 1, 'ck': 2, 'val': 'val1_2'},
            {'pk': 1, 'ck': 3, 'val': 'val1_3'},
            {'pk': 1, 'ck': 4, 'val': 'val1_4'},
            {'pk': 2, 'ck': 1, 'val': 'val2_1'},
            {'pk': 2, 'ck': 2, 'val': 'val2_2'},
            {'pk': 2, 'ck': 3, 'val': 'val2_3'},
            {'pk': 2, 'ck': 4, 'val': 'val2_4'},
            {'pk': 2, 'ck': 5, 'val': 'val2_5'},
            {'pk': 2, 'ck': 6, 'val': 'val2_6'},
        ]

    def test_single_partition(self):
        """
        Test that the querier is saved and reused.
        """
        self.session = self.prepare()

        data = self.setup_simple_table()

        future = self.session.execute_async(
            SimpleStatement("select * from test_singular where pk = 1", fetch_size=3, consistency_level=CL.ALL)
        )
        pf = PageFetcher(future)

        all_pages = pf.request_all()
        self.assertEqual(pf.all_data(), [p for p in data if p['pk'] == 1])

        self.assertEqual(pf.requested_pages, 2)
        self.assert_nodes_metrics(({'lookups': pf.requested_pages - 1}, {}))

    @attr('next-gating')
    @attr('dtest-debug')
    def test_two_partitions(self):
        """
        Test that when the coordinator throws away parts of the results
        the replica recognizes the position mismatch and drops the
        cached querier.
        """
        self.session = self.prepare()

        data = self.setup_simple_table()

        future = self.session.execute_async(
            SimpleStatement("select * from test_singular where pk in (1, 2)", fetch_size=5, consistency_level=CL.ALL)
        )
        pf = PageFetcher(future)

        all_pages = pf.request_all()

        self.assertEqual(pf.requested_pages, 3)
        self.assertEqual(pf.all_data(), data)
        self.assert_nodes_metrics(({'lookups': pf.requested_pages - 1, 'drops': 1}, {}))

    def test_replica_usage(self):
        """
        Test that the coordinator sends all page-requests consistently to the
        same replica.
        """
        self.session = self.prepare()

        data = self.setup_simple_table(speculative_retry="NONE", dclocal_read_repair_chance=0.0)

        future = self.session.execute_async(
            SimpleStatement("select * from test_singular where pk = 2", fetch_size=1, consistency_level=CL.ONE)
        )
        pf = PageFetcher(future)

        def get_coordinator_reads_metric(node_ip):
            return self.get_node_metrics(node_ip, metrics=["storage_proxy_coordinator_reads"])["storage_proxy_coordinator_reads"]

        node_ips = [self.get_ip_from_node(node) for node in self.cluster.nodelist()]
        coordinator_reads_baseline = {node_ip: get_coordinator_reads_metric(node_ip) for node_ip in node_ips}

        all_pages = pf.request_all()

        for node_ip in node_ips:
            new_reads = get_coordinator_reads_metric(node_ip) - coordinator_reads_baseline[node_ip]
            # Verify that each node was used as a coordinator at least once
            # and therefore that the test is meaningful.
            # Currently the driver will round-robin through the nodes as all
            # them will have the read partitions. If this assumption will not
            # hold in the future this test will become obsolete.
            self.assertGreater(new_reads, 0)

        self.assertEqual(pf.requested_pages, 7)
        self.assertEqual(pf.all_data(), [p for p in data if p['pk'] == 2])
        self.assert_nodes_metrics(({'lookups': pf.requested_pages - 1}, {}))

    def test_per_query_read_repair_decision(self):
        """
        Test that that the read-repair decision made on the first page
        of the query is sticky to all pages of the query.
        """
        self.cluster.set_configuration_options(
            values={'tombstone_failure_threshold': 500}
        )
        self.session = self.prepare()

        data = self.setup_simple_table(speculative_retry="NONE", dclocal_read_repair_chance=0.5)

        future = self.session.execute_async(
            SimpleStatement("select * from test_singular where pk = 2", fetch_size=1, consistency_level=CL.ONE)
        )
        pf = PageFetcher(future)

        all_pages = pf.request_all()

        self.assertEqual(pf.requested_pages, 7)
        self.assertEqual(pf.all_data(), [p for p in data if p['pk'] == 2])
        self.assert_nodes_metrics(({'lookups': pf.requested_pages - 1}, {}))
