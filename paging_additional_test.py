import time
import uuid
from unittest import skip

from cassandra import ConsistencyLevel as CL
from cassandra import InvalidRequest, ReadTimeout, ReadFailure
from cassandra.query import SimpleStatement, dict_factory, named_tuple_factory

from assertions import assert_invalid
from datahelp import create_rows, flatten_into_set, parse_data_into_dicts
from tools import require, since

from paging_test import PageFetcher, BasePagingTester, PageAssertionMixin

class TestAggregatePaging(BasePagingTester, PageAssertionMixin):
    """
    Basic aggregation tests using paging
    """

    def test_paged_count_with_limit(self):
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
        sizes = [10, 100, 1000, 3000, 5000]

        for page_size in sizes:
            for limit in sizes:                
                future = session.execute_async(
                    SimpleStatement("select count(*) from paging_test limit {}".format(limit), fetch_size=page_size, consistency_level=CL.ALL)
                )
                pf = PageFetcher(future).request_all()
                self.assertEqual(pf.num_results_all(), [1])
                self.assertEqual(pf.all_data(), [{u'count': limit}])
