import uuid

from cassandra import ConsistencyLevel as CL
from cassandra.query import SimpleStatement

from datahelp import create_rows
from paging_test import PageFetcher, BasePagingTester, PageAssertionMixin
from scylla_tools import scylla_mode


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
        create_rows(data, session, 'paging_test', cl=CL.ALL, format_funcs={'id': random_txt, 'value': unicode})

        # Note that both page size and limit is less than actual num of rows.
        # Thus we should always get a count == limit
        # They main thing we are testing is that page size > limit does not break the
        # count result. (#650)
        for page_size in sizes:
            for limit in sizes:
                future = session.execute_async(
                    SimpleStatement("select count(*) from paging_test limit {}".format(limit), fetch_size=page_size, consistency_level=CL.ALL)
                )
                pf = PageFetcher(future).request_all()
                self.assertEqual(pf.num_results_all(), [1])
                self.assertEqual(pf.all_data(), [{u'count': limit}])

    @scylla_mode('release')
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
            return unicode(uuid.uuid4())

        data = """
               | pk | ck1 | ck2          | v |
               +----+-----+--------------+---+
          *1234| 0  | 1   | [random_txt] | 0 |
               """

        create_rows(data, session, 'paging_test', cl=CL.ALL, format_funcs={'pk': int, 'ck1': int, 'ck2': random_txt, 'v': int})

        future = session.execute_async(
            SimpleStatement("select count(*) from paging_test where pk = 0 and ck1 = 1 order by ck1 {}, ck2 {}".format(order, order), fetch_size=100, consistency_level=CL.ALL)
        )
        pf = PageFetcher(future).request_all()
        self.assertEqual(pf.num_results_all(), [1])
        self.assertEqual(pf.all_data(), [{u'count': 1234}])

    def test_paged_count_with_clustering_key(self):
        self._test_paged_count_with_clustering_key('asc')

    def test_paged_count_with_clustering_key_reversed(self):
        self._test_paged_count_with_clustering_key('desc')
