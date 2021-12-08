import itertools
import time
import uuid
from pprint import pprint
from unittest import SkipTest
import logging

import pytest
from cassandra import ConsistencyLevel as CL
from cassandra import InvalidRequest, ReadFailure, ReadTimeout
from cassandra.query import SimpleStatement, dict_factory, named_tuple_factory

from datahelp import create_rows, flatten_into_set, parse_data_into_dicts
from dtest_setup import DTestSetup
from tools.assertions import assert_count_equal
from tools.paging import run_scenarios
from tools.data import rows_to_list
from upgrade_test import UpgradeTester

logger = logging.getLogger(__name__)


def assert_read_timeout_or_failure(session, query):
    with pytest.raises(expected_exception=(ReadTimeout, ReadFailure)):
        session.execute(query)


class Page:
    data = None

    def __init__(self):
        self.data = []

    def add_row(self, row):
        self.data.append(row)


class PageFetcher:
    """
    Requests pages, handles their receipt,
    and provides paged data for testing.

    The first page is automatically retrieved, so an initial
    call to request_one is actually getting the *second* page!
    """
    pages = None
    error = None
    future = None
    requested_pages = None
    retrieved_pages = None
    retrieved_empty_pages = None

    def __init__(self, future):
        self.pages = []

        # the first page is automagically returned (eventually)
        # so we'll count this as a request, but the retrieved count
        # won't be incremented until it actually arrives
        self.requested_pages = 1
        self.retrieved_pages = 0
        self.retrieved_empty_pages = 0

        self.future = future
        self.future.add_callbacks(
            callback=self.handle_page,
            errback=self.handle_error
        )

        # wait for the first page to arrive, otherwise we may call
        # future.has_more_pages too early, since it should only be
        # called after the first page is returned
        self.wait(seconds=30)

    def handle_page(self, rows):
        # occasionally get a final blank page that is useless
        if not rows:
            self.retrieved_empty_pages += 1
            return

        page = Page()
        self.pages.append(page)

        for row in rows:
            page.add_row(row)

        self.retrieved_pages += 1

    def handle_error(self, exc):
        self.error = exc
        raise exc

    def request_one(self, timeout=None):
        """
        Requests the next page if there is one.

        If the future is exhausted, this is a no-op.
        @param timeout Time, in seconds, to wait for all pages.
        """
        if self.future.has_more_pages:
            self.future.start_fetching_next_page()
            self.requested_pages += 1
            self.wait(seconds=timeout)

        return self

    def request_all(self, timeout=None):
        """
        Requests any remaining pages.

        If the future is exhausted, this is a no-op.
        @param timeout Time, in seconds, to wait for all pages.
        """
        while self.future.has_more_pages:
            self.future.start_fetching_next_page()
            self.requested_pages += 1
            self.wait(seconds=timeout)

        return self

    def wait(self, seconds=None):
        """
        Blocks until all *requested* pages have been returned.

        Requests are made by calling request_one and/or request_all.

        Raises RuntimeError if seconds is exceeded.
        """
        seconds = 5 if seconds is None else seconds
        expiry = time.time() + seconds

        while time.time() < expiry:
            if self.requested_pages == (self.retrieved_pages + self.retrieved_empty_pages):
                return self

        raise RuntimeError("Requested pages were not delivered before timeout.")

    def pagecount(self):
        """
        Returns count of *retrieved* pages which were not empty.

        Pages are retrieved by requesting them with request_one and/or request_all.
        """
        return len(self.pages)

    def num_results(self, page_num):
        """
        Returns the number of results found at page_num
        """
        return len(self.pages[page_num - 1].data)

    def num_results_all(self):
        return [len(page.data) for page in self.pages]

    def page_data(self, page_num):
        """
        Returns retreived data found at pagenum.

        The page should have already been requested with request_one and/or request_all.
        """
        return self.pages[page_num - 1].data

    def all_data(self):
        """
        Returns all retrieved data flattened into a single list (instead of separated into Page objects).

        The page(s) should have already been requested with request_one and/or request_all.
        """
        all_pages_combined = []
        for page in self.pages:
            all_pages_combined.extend(page.data[:])

        return all_pages_combined

    @property  # make property to match python driver api
    def has_more_pages(self):
        """
        Returns bool indicating if there are any pages not retrieved.
        """
        return self.future.has_more_pages


class PageAssertionMixin:
    """Can be added to subclasses of unittest.Tester"""

    @staticmethod
    def assert_equal_ignore_order(actual, expected):
        if isinstance(actual, list) and len(actual) == 1 and isinstance(actual[0], list):
            actual = actual[0]
            expected = expected[0]
        return assert_count_equal(actual=actual, expected=expected)

    @staticmethod
    def assert_is_subset_of(subset, superset):
        assert flatten_into_set(subset).issubset(flatten_into_set(superset))


class BasePagingTester(UpgradeTester):

    def prepare(self, *args, **kwargs):
        cursor = UpgradeTester.prepare(self, *args, **kwargs)
        cursor.row_factory = dict_factory
        return cursor


def random_txt(_):
    return str(uuid.uuid4())


class TestPagingSize(BasePagingTester, PageAssertionMixin):
    """
    Basic tests relating to page size (relative to results set)
    and validation of page size setting.
    """

    def test_with_no_results(self):
        """
        No errors when a page is requested and query has no results.
        """
        cursor = self.prepare()
        cursor.execute("CREATE TABLE paging_test ( id int PRIMARY KEY, value text )")

        for is_upgraded, cursor in self.do_upgrade(cursor):
            cursor.row_factory = dict_factory
            logger.debug("Querying %s node" % "upgraded" if is_upgraded else "old")

            # run a query that has no results and make sure it's exhausted
            future = cursor.execute_async(
                SimpleStatement("select * from paging_test", fetch_size=100, consistency_level=CL.ALL)
            )

            page_fetcher = PageFetcher(future)
            page_fetcher.request_all()
            assert page_fetcher.all_data() == []
            assert not page_fetcher.has_more_pages

    def test_with_less_results_than_page_size(self):
        cursor = self.prepare()
        cursor.execute("CREATE TABLE paging_test ( id int PRIMARY KEY, value text )")

        for is_upgraded, cursor in self.do_upgrade(cursor):
            cursor.row_factory = dict_factory
            logger.debug("Querying %s node" % "upgraded" if is_upgraded else "old")
            cursor.execute("TRUNCATE paging_test")

            data = """
                |id| value          |
                |1 |testing         |
                |2 |and more testing|
                |3 |and more testing|
                |4 |and more testing|
                |5 |and more testing|
                """
            expected_data = create_rows(data, cursor, 'paging_test', cl=CL.ALL, format_funcs={'id': int, 'value': str})

            future = cursor.execute_async(
                SimpleStatement("select * from paging_test", fetch_size=100, consistency_level=CL.ALL)
            )
            page_fetcher = PageFetcher(future)
            page_fetcher.request_all()

            assert not page_fetcher.has_more_pages
            assert len(page_fetcher.all_data()) == len(expected_data)

    def test_with_more_results_than_page_size(self):
        cursor = self.prepare()
        cursor.execute("CREATE TABLE paging_test ( id int PRIMARY KEY, value text )")

        for is_upgraded, cursor in self.do_upgrade(cursor):
            cursor.row_factory = dict_factory
            logger.debug("Querying %s node" % "upgraded" if is_upgraded else "old")
            cursor.execute("TRUNCATE paging_test")

            data = """
                |id| value          |
                |1 |testing         |
                |2 |and more testing|
                |3 |and more testing|
                |4 |and more testing|
                |5 |and more testing|
                |6 |testing         |
                |7 |and more testing|
                |8 |and more testing|
                |9 |and more testing|
                """
            expected_data = create_rows(data, cursor, 'paging_test', cl=CL.ALL, format_funcs={'id': int, 'value': str})

            future = cursor.execute_async(
                SimpleStatement("select * from paging_test", fetch_size=5, consistency_level=CL.ALL)
            )

            page_fetcher = PageFetcher(future).request_all()

            assert page_fetcher.pagecount() == 2
            assert page_fetcher.num_results_all() == [5, 4]

            # make sure expected and actual have same data elements (ignoring order)
            self.assert_equal_ignore_order(page_fetcher.all_data(), expected_data)

    def test_with_equal_results_to_page_size(self):
        cursor = self.prepare()
        cursor.execute("CREATE TABLE paging_test ( id int PRIMARY KEY, value text )")

        for is_upgraded, cursor in self.do_upgrade(cursor):
            cursor.row_factory = dict_factory
            logger.debug("Querying %s node" % "upgraded" if is_upgraded else "old")
            cursor.execute("TRUNCATE paging_test")

            data = """
                |id| value          |
                |1 |testing         |
                |2 |and more testing|
                |3 |and more testing|
                |4 |and more testing|
                |5 |and more testing|
                """
            expected_data = create_rows(data, cursor, 'paging_test', cl=CL.ALL, format_funcs={'id': int, 'value': str})

            future = cursor.execute_async(
                SimpleStatement("select * from paging_test", fetch_size=5, consistency_level=CL.ALL)
            )

            page_fetcher = PageFetcher(future).request_all()

            assert page_fetcher.num_results_all() == [5]
            assert page_fetcher.pagecount() == 1

            # make sure expected and actual have same data elements (ignoring order)
            self.assert_equal_ignore_order(page_fetcher.all_data(), expected_data)

    def test_undefined_page_size_default(self):
        """
        If the page size isn't sent then the default fetch size is used.
        """
        cursor = self.prepare()
        cursor.execute("CREATE TABLE paging_test ( id uuid PRIMARY KEY, value text )")

        for is_upgraded, cursor in self.do_upgrade(cursor):
            cursor.row_factory = dict_factory
            logger.debug("Querying %s node" % "upgraded" if is_upgraded else "old")
            cursor.execute("TRUNCATE paging_test")

            data = """
                   | id     |value   |
              *5001| [uuid] |testing |
                """
            expected_data = create_rows(data, cursor, 'paging_test', cl=CL.ALL,
                                        format_funcs={'id': random_txt, 'value': str})

            future = cursor.execute_async(
                SimpleStatement("select * from paging_test", consistency_level=CL.ALL)
            )

            page_fetcher = PageFetcher(future).request_all()

            assert page_fetcher.num_results_all() == [5000, 1]

            self.max_diff = None
            # make sure expected and actual have same data elements (ignoring order)
            self.assert_equal_ignore_order(page_fetcher.all_data(), expected_data)


class TestPagingSizeNodes3RF3(TestPagingSize):
    NODES = 3
    RF = 3
    CL = CL.ALL


class TestPagingSizeNodes2RF1(TestPagingSize):
    NODES = 2
    RF = 1


class TestPagingWithModifiers(BasePagingTester, PageAssertionMixin):
    """
    Tests concerned with paging when CQL modifiers (such as order, limit, allow filtering) are used.
    """

    def test_with_order_by(self):
        """"
        Paging over a single partition with ordering should work.
        (Spanning multiple partitions won't though, by design. See CASSANDRA-6722).
        """
        cursor = self.prepare()
        cursor.execute(
            """
            CREATE TABLE paging_test (
                id int,
                value text,
                PRIMARY KEY (id, value)
            ) WITH CLUSTERING ORDER BY (value ASC)
            """)

        for is_upgraded, cursor in self.do_upgrade(cursor):
            cursor.row_factory = dict_factory
            logger.debug("Querying %s node" % "upgraded" if is_upgraded else "old")
            cursor.execute("TRUNCATE paging_test")

            data = """
                |id|value|
                |1 |a    |
                |1 |b    |
                |1 |c    |
                |1 |d    |
                |1 |e    |
                |1 |f    |
                |1 |g    |
                |1 |h    |
                |1 |i    |
                |1 |j    |
                """

            expected_data = create_rows(data, cursor, 'paging_test', cl=CL.ALL, format_funcs={'id': int, 'value': str})

            future = cursor.execute_async(
                SimpleStatement("select * from paging_test where id = 1 order by value asc",
                                fetch_size=5, consistency_level=CL.ALL)
            )

            page_fetcher = PageFetcher(future).request_all()

            assert page_fetcher.pagecount() == 2
            assert page_fetcher.num_results_all() == [5, 5]

            # these should be equal (in the same order)
            assert page_fetcher.all_data() == expected_data

            # make sure we don't allow paging over multiple partitions with order because that's weird
            with self.assertRaisesRegexp(InvalidRequest, 'Cannot page queries with both ORDER BY and a IN '
                                                         'restriction on the partition key'):
                stmt = SimpleStatement(
                    "select * from paging_test where id in (1,2) order by value asc", consistency_level=CL.ALL)
                cursor.execute(stmt)

    def test_with_order_by_reversed(self):
        """"
        Paging over a single partition with ordering and a reversed clustering order.
        """
        cursor = self.prepare()
        cursor.execute(
            """
            CREATE TABLE paging_test (
                id int,
                value text,
                value2 text,
                value3 text,
                PRIMARY KEY (id, value)
            ) WITH CLUSTERING ORDER BY (value DESC)
            """)

        for is_upgraded, cursor in self.do_upgrade(cursor):
            cursor.row_factory = dict_factory
            logger.debug("Querying %s node" % "upgraded" if is_upgraded else "old")
            cursor.execute("TRUNCATE paging_test")

            data = """
                |id|value|value2|value3|
                |1 |a    |a     |a     |
                |1 |b    |b     |b     |
                |1 |c    |c     |c     |
                |1 |d    |d     |d     |
                |1 |e    |e     |e     |
                |1 |f    |f     |f     |
                |1 |g    |g     |g     |
                |1 |h    |h     |h     |
                |1 |i    |i     |i     |
                |1 |j    |j     |j     |
                """

            expected_data = create_rows(data, cursor, 'paging_test', cl=CL.ALL, format_funcs={
                                        'id': int, 'value': str, 'value2': str})

            future = cursor.execute_async(
                SimpleStatement("select * from paging_test where id = 1 order by value asc",
                                fetch_size=3, consistency_level=CL.ALL)
            )

            page_fetcher = PageFetcher(future).request_all()

            logger.info("pages: %s", page_fetcher.num_results_all())
            assert page_fetcher.pagecount() == 4
            assert page_fetcher.num_results_all() == [3, 3, 3, 1]

            # these should be equal (in the same order)
            assert page_fetcher.all_data() == expected_data

            # drop the ORDER BY
            future = cursor.execute_async(
                SimpleStatement("select * from paging_test where id = 1", fetch_size=3, consistency_level=CL.ALL)
            )

            page_fetcher = PageFetcher(future).request_all()

            assert page_fetcher.pagecount() == 4
            assert page_fetcher.num_results_all() == [3, 3, 3, 1]

            # these should be equal (in the same order)
            assert page_fetcher.all_data() == list(reversed(expected_data))

    def test_with_limit(self):
        cursor = self.prepare()
        cursor.execute("CREATE TABLE paging_test ( id int, value text, PRIMARY KEY (id, value) )")

        for is_upgraded, cursor in self.do_upgrade(cursor):
            cursor.row_factory = dict_factory
            logger.debug("Querying %s node" % "upgraded" if is_upgraded else "old")
            cursor.execute("TRUNCATE paging_test")

            data = """
                   | id | value         |
                 *5| 1  | [random text] |
                 *5| 2  | [random text] |
                *10| 3  | [random text] |
                *10| 4  | [random text] |
                *20| 5  | [random text] |
                *30| 6  | [random text] |
                """
            expected_data = create_rows(data, cursor, 'paging_test', cl=CL.ALL,
                                        format_funcs={'id': int, 'value': random_txt})

            scenarios = [
                # using equals clause w/single partition
                {'limit': 10, 'fetch': 20, 'data_size': 30, 'whereclause': 'WHERE id = 6',
                    'expect_pgcount': 1, 'expect_pgsizes': [10]},      # limit < fetch < data
                {'limit': 10, 'fetch': 30, 'data_size': 20, 'whereclause': 'WHERE id = 5',
                    'expect_pgcount': 1, 'expect_pgsizes': [10]},      # limit < data < fetch
                {'limit': 20, 'fetch': 10, 'data_size': 30, 'whereclause': 'WHERE id = 6',
                    'expect_pgcount': 2, 'expect_pgsizes': [10, 10]},  # fetch < limit < data
                {'limit': 30, 'fetch': 10, 'data_size': 20, 'whereclause': 'WHERE id = 5',
                    'expect_pgcount': 2, 'expect_pgsizes': [10, 10]},  # fetch < data < limit
                {'limit': 20, 'fetch': 30, 'data_size': 10, 'whereclause': 'WHERE id = 3',
                    'expect_pgcount': 1, 'expect_pgsizes': [10]},      # data < limit < fetch
                {'limit': 30, 'fetch': 20, 'data_size': 10, 'whereclause': 'WHERE id = 3',
                    'expect_pgcount': 1, 'expect_pgsizes': [10]},      # data < fetch < limit

                # using 'in' clause w/multi partitions
                {'limit': 9, 'fetch': 20, 'data_size': 80, 'whereclause': 'WHERE id in (1,2,3,4,5,6)',
                 'expect_pgcount': 1, 'expect_pgsizes': [9]},  # limit < fetch < data
                {'limit': 10, 'fetch': 30, 'data_size': 20, 'whereclause': 'WHERE id in (3,4)', 'expect_pgcount': 1,
                 'expect_pgsizes': [10]},      # limit < data < fetch
                {'limit': 20, 'fetch': 10, 'data_size': 30, 'whereclause': 'WHERE id in (4,5)', 'expect_pgcount': 2,
                 'expect_pgsizes': [10, 10]},  # fetch < limit < data
                {'limit': 30, 'fetch': 10, 'data_size': 20, 'whereclause': 'WHERE id in (3,4)', 'expect_pgcount': 2,
                 'expect_pgsizes': [10, 10]},  # fetch < data < limit
                {'limit': 20, 'fetch': 30, 'data_size': 10, 'whereclause': 'WHERE id in (1,2)', 'expect_pgcount': 1,
                 'expect_pgsizes': [10]},      # data < limit < fetch
                {'limit': 30, 'fetch': 20, 'data_size': 10, 'whereclause': 'WHERE id in (1,2)', 'expect_pgcount': 1,
                 'expect_pgsizes': [10]},      # data < fetch < limit

                # no limit but with a defined pagesize. Scenarios added for CASSANDRA-8408.
                {'limit': None, 'fetch': 20, 'data_size': 80, 'whereclause': 'WHERE id in (1,2,3,4,5,6)',
                 'expect_pgcount': 4, 'expect_pgsizes': [20, 20, 20, 20]},  # fetch < data
                {'limit': None, 'fetch': 30, 'data_size': 20, 'whereclause': 'WHERE id in (3,4)', 'expect_pgcount': 1,
                 'expect_pgsizes': [20]},          # data < fetch
                {'limit': None, 'fetch': 10, 'data_size': 30, 'whereclause': 'WHERE id in (4,5)', 'expect_pgcount': 3,
                 'expect_pgsizes': [10, 10, 10]},  # fetch < data
                {'limit': None, 'fetch': 30, 'data_size': 10, 'whereclause': 'WHERE id in (1,2)', 'expect_pgcount': 1,
                 'expect_pgsizes': [10]},          # data < fetch

                # not setting fetch_size (unpaged) but using limit. Scenarios added for CASSANDRA-8408.
                {'limit': 9, 'fetch': None, 'data_size': 80, 'whereclause': 'WHERE id in (1,2,3,4,5,6)',
                 'expect_pgcount': 1, 'expect_pgsizes': [9]},  # limit < data
                {'limit': 30, 'fetch': None, 'data_size': 10, 'whereclause': 'WHERE id in (1,2)', 'expect_pgcount': 1,
                 'expect_pgsizes': [10]},        # data < limit
            ]

            def handle_scenario(scenario):
                # using a limit and a fetch
                if scenario['limit'] and scenario['fetch']:
                    future = cursor.execute_async(
                        SimpleStatement(
                            "select * from paging_test {} limit {}".format(scenario['whereclause'], scenario['limit']),
                            fetch_size=scenario['fetch'], consistency_level=CL.ALL)
                    )
                # using a limit but not specifying a fetch_size
                elif scenario['limit'] and scenario['fetch'] is None:
                    future = cursor.execute_async(
                        SimpleStatement(
                            "select * from paging_test {} limit {}".format(scenario['whereclause'], scenario['limit']),
                            consistency_level=CL.ALL)
                    )
                # no limit but a fetch_size specified
                elif scenario['limit'] is None and scenario['fetch']:
                    future = cursor.execute_async(
                        SimpleStatement(
                            "select * from paging_test {}".format(scenario['whereclause']),
                            fetch_size=scenario['fetch'], consistency_level=CL.ALL)
                    )
                else:
                    # this should not happen
                    assert False

                page_fetcher = PageFetcher(future).request_all()
                self.assertEqual(page_fetcher.num_results_all(), scenario['expect_pgsizes'])
                self.assertEqual(page_fetcher.pagecount(), scenario['expect_pgcount'])

                # make sure all the data retrieved is a subset of input data
                self.assert_is_subset_of(page_fetcher.all_data(), expected_data)

            run_scenarios(scenarios, handle_scenario)

    def test_with_allow_filtering(self):
        cursor = self.prepare()
        cursor.execute("CREATE TABLE paging_test ( id int, value text, PRIMARY KEY (id, value) )")

        for is_upgraded, cursor in self.do_upgrade(cursor):
            cursor.row_factory = dict_factory
            logger.debug("Querying %s node" % "upgraded" if is_upgraded else "old")
            cursor.execute("TRUNCATE paging_test")

            data = """
                |id|value           |
                |1 |testing         |
                |2 |and more testing|
                |3 |and more testing|
                |4 |and more testing|
                |5 |and more testing|
                |6 |testing         |
                |7 |and more testing|
                |8 |and more testing|
                |9 |and more testing|
                """
            create_rows(data, cursor, 'paging_test', cl=CL.ALL, format_funcs={'id': int, 'value': str})

            future = cursor.execute_async(
                SimpleStatement("select * from paging_test where value = 'and more testing' ALLOW FILTERING",
                                fetch_size=4, consistency_level=CL.ALL)
            )

            page_fetcher = PageFetcher(future).request_all()

            assert page_fetcher.pagecount() == 2
            assert page_fetcher.num_results_all() == [4, 3]

            # make sure the allow filtering query matches the expected results (ignoring order)
            self.assert_equal_ignore_order(
                page_fetcher.all_data(),
                parse_data_into_dicts(
                    """
                    |id|value           |
                    |2 |and more testing|
                    |3 |and more testing|
                    |4 |and more testing|
                    |5 |and more testing|
                    |7 |and more testing|
                    |8 |and more testing|
                    |9 |and more testing|
                    """, format_funcs={'id': int, 'value': str}
                )
            )


class TestPagingWithModifiersNodes3RF3(TestPagingWithModifiers):
    NODES = 3
    RF = 3
    CL = CL.ALL


class TestPagingWithModifiersNodes2RF1(TestPagingWithModifiers):
    NODES = 2
    RF = 1


class TestPagingData(BasePagingTester, PageAssertionMixin):

    def test_basic_paging(self):
        """
        A simple paging test that is easy to debug.
        """

        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE test (
                k int,
                c int,
                v text,
                PRIMARY KEY (k, c)
            );
        """)

        cursor.execute("""
            CREATE TABLE test2 (
                k int,
                c int,
                v text,
                PRIMARY KEY (k, c)
            ) WITH COMPACT STORAGE;
        """)

        for is_upgraded, cursor in self.do_upgrade(cursor):
            logger.debug("Querying %s node" % "upgraded" if is_upgraded else "old")
            cursor.execute("TRUNCATE test")
            cursor.execute("TRUNCATE test2")

            for table in ("test", "test2"):
                logger.debug("Querying table %s" % table)
                expected = []
                # match the key ordering for murmur3
                for key_value in (1, 0, 2):
                    for c_value in range(5):
                        value = "%d.%d" % (key_value, c_value)
                        cursor.execute("INSERT INTO " + table + " (k, c, v) VALUES (%s, %s, %s)",
                                       (key_value, c_value, value))
                        expected.append([key_value, c_value, value])

                for fetch_size in (2, 3, 5, 10, 100):
                    logger.debug("Using fetch size %d" % fetch_size)
                    cursor.default_fetch_size = fetch_size
                    results = rows_to_list(cursor.execute("SELECT * FROM %s" % (table,)))
                    pprint(results)
                    assert len(results) == len(expected)
                    assert results == expected

    def test_basic_compound_paging(self):
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE test (
                k int,
                c1 int,
                c2 int,
                v text,
                PRIMARY KEY (k, c1, c2)
            );
        """)

        cursor.execute("""
            CREATE TABLE test2 (
                k int,
                c1 int,
                c2 int,
                v text,
                PRIMARY KEY (k, c1, c2)
            ) WITH COMPACT STORAGE;
        """)

        for is_upgraded, cursor in self.do_upgrade(cursor):
            logger.debug("Querying %s node" % "upgraded" if is_upgraded else "old")
            cursor.execute("TRUNCATE test")
            cursor.execute("TRUNCATE test2")

            for table in ("test", "test2"):
                logger.debug("Querying table %s" % table)
                expected = []
                # match the key ordering for murmur3
                for k_value in (1, 0, 2):
                    for c_value in range(5):
                        value = "%d.%d" % (k_value, c_value)
                        cursor.execute("INSERT INTO " + table + " (k, c1, c2, v) VALUES (%s, %s, 0, %s)",
                                       (k_value, c_value, value))
                        expected.append([k_value, c_value, 0, value])

                for fetch_size in (2, 3, 5, 10, 100):
                    logger.debug("Using fetch size %d" % fetch_size)
                    cursor.default_fetch_size = fetch_size
                    results = rows_to_list(cursor.execute("SELECT * FROM %s" % (table,)))
                    pprint(results)
                    assert len(results) == len(expected)
                    assert results == expected

    def test_paging_a_single_wide_row(self):
        cursor = self.prepare()
        cursor.execute("CREATE TABLE paging_test ( id int, value text, PRIMARY KEY (id, value) )")

        for is_upgraded, cursor in self.do_upgrade(cursor):
            cursor.row_factory = dict_factory
            logger.debug("Querying %s node" % "upgraded" if is_upgraded else "old")
            cursor.execute("TRUNCATE paging_test")

            data = """
                  | id | value                  |
            *10000| 1  | [replaced with random] |
                """
            expected_data = create_rows(data, cursor, 'paging_test', cl=CL.ALL,
                                        format_funcs={'id': int, 'value': random_txt})

            future = cursor.execute_async(
                SimpleStatement("select * from paging_test where id = 1", fetch_size=3000, consistency_level=CL.ALL)
            )

            page_fetcher = PageFetcher(future).request_all()

            assert page_fetcher.pagecount() == 4
            assert page_fetcher.num_results_all() == [3000, 3000, 3000, 1000]

            all_results = page_fetcher.all_data()
            assert len(all_results) == len(expected_data)
            self.max_diff = None
            self.assert_equal_ignore_order(expected_data, all_results)

    def test_paging_across_multi_wide_rows(self):
        cursor = self.prepare()
        cursor.execute("CREATE TABLE paging_test ( id int, value text, PRIMARY KEY (id, value) )")

        for is_upgraded, cursor in self.do_upgrade(cursor):
            cursor.row_factory = dict_factory
            logger.debug("Querying %s node" % "upgraded" if is_upgraded else "old")
            cursor.execute("TRUNCATE paging_test")

            data = """
                  | id | value                  |
             *5000| 1  | [replaced with random] |
             *5000| 2  | [replaced with random] |
                """
            expected_data = create_rows(data, cursor, 'paging_test', cl=CL.ALL,
                                        format_funcs={'id': int, 'value': random_txt})

            future = cursor.execute_async(
                SimpleStatement("select * from paging_test where id in (1,2)",
                                fetch_size=3000, consistency_level=CL.ALL)
            )

            page_fetcher = PageFetcher(future).request_all()

            assert page_fetcher.pagecount() == 4
            assert page_fetcher.num_results_all() == [3000, 3000, 3000, 1000]

            self.assert_equal_ignore_order(page_fetcher.all_data(), expected_data)

    def test_paging_using_secondary_indexes(self):
        cursor = self.prepare()
        cursor.execute("CREATE TABLE paging_test ( id int, mybool boolean, sometext text, PRIMARY KEY (id, sometext) )")
        cursor.execute("CREATE INDEX ON paging_test(mybool)")

        def bool_from_str_int(text):
            return bool(int(text))

        for is_upgraded, cursor in self.do_upgrade(cursor):
            cursor.row_factory = dict_factory
            logger.debug("Querying %s node" % "upgraded" if is_upgraded else "old")
            cursor.execute("TRUNCATE paging_test")

            data = """
                 | id | mybool| sometext |
             *100| 1  | 1     | [random] |
             *300| 2  | 0     | [random] |
             *500| 3  | 1     | [random] |
             *400| 4  | 0     | [random] |
                """
            all_data = create_rows(
                data, cursor, 'paging_test', cl=CL.ALL,
                format_funcs={'id': int, 'mybool': bool_from_str_int, 'sometext': random_txt}
            )

            future = cursor.execute_async(
                SimpleStatement("select * from paging_test where mybool = true",
                                fetch_size=400, consistency_level=CL.ALL)
            )

            page_fetcher = PageFetcher(future).request_all()

            # the query only searched for True rows, so let's pare down the expectations for comparison
            expected_data = list(filter(lambda x: x.get('mybool') is True, all_data))

            assert page_fetcher.pagecount() == 2
            assert page_fetcher.num_results_all() == [400, 200]
            self.assert_equal_ignore_order(expected_data, page_fetcher.all_data())

    @pytest.mark.since('2.0.6')
    def test_static_columns_paging(self):
        """
        Exercises paging with static columns to detect bugs
        @jira_ticket CASSANDRA-8502.
        """

        cursor = self.prepare()
        cursor.execute("CREATE TABLE test (a int, b int, c int, s1 int static, s2 int static, PRIMARY KEY (a, b))")

        for is_upgraded, cursor in self.do_upgrade(cursor):
            min_version = min(self.get_node_versions())
            latest_version_with_bug = '2.2.3'
            if min_version <= latest_version_with_bug:
                raise SkipTest('known bug released in {latest_ver} and earlier (current min version {min_ver}); '
                               'skipping'.format(latest_ver=latest_version_with_bug, min_ver=min_version))

            cursor.row_factory = dict_factory
            logger.debug("Querying %s node" % "upgraded" if is_upgraded else "old")
            cursor.execute("TRUNCATE test")
            cursor.row_factory = named_tuple_factory

            for a_value in range(4):
                for b_and_c_value in range(4):
                    cursor.execute("INSERT INTO test (a, b, c, s1, s2) VALUES (%d, %d, %d, %d, %d)" %
                                   (a_value, b_and_c_value, b_and_c_value, 17, 42))

            selectors = (
                "*",
                "a, b, c, s1, s2",
                "a, b, c, s1",
                "a, b, c, s2",
                "a, b, c")

            for page_size in (2, 3, 4, 5, 15, 16, 17, 100):
                logger.debug("Using page size of %d", page_size)
                cursor.default_fetch_size = page_size
                for selector in selectors:
                    logger.debug("Using selector '%s'", selector)
                    results = list(cursor.execute("SELECT %s FROM test" % selector))
                    pprint(results)
                    # self.assertEqual(16, len(results))
                    assert sorted([res.a for res in results]) == [0] * 4 + [1] * 4 + [2] * 4 + [3] * 4
                    assert [res.b for res in results] == [0, 1, 2, 3] * 4
                    assert [res.c for res in results] == [0, 1, 2, 3] * 4
                    if "s1" in selector:
                        assert [res.s1 for res in results] == [17] * 16
                    if "s2" in selector:
                        assert [res.s2 for res in results] == [42] * 16

            # IN over the partitions
            for page_size in (2, 3, 4, 5, 15, 16, 17, 100):
                logger.debug("Using page size of %d", page_size)
                cursor.default_fetch_size = page_size
                for selector in selectors:
                    logger.debug("Using selector '%s'", selector)
                    results = list(cursor.execute("SELECT %s FROM test WHERE a IN (0, 1, 2, 3)" % selector))
                    assert len(results) == 16
                    assert sorted([res.a for res in results]) == [0] * 4 + [1] * 4 + [2] * 4 + [3] * 4
                    assert [res.b for res in results] == [0, 1, 2, 3] * 4
                    assert [res.c for res in results] == [0, 1, 2, 3] * 4
                    if "s1" in selector:
                        assert [res.s1 for res in results] == [17] * 16
                    if "s2" in selector:
                        assert [res.s2 for res in results] == [42] * 16

            # single partition
            for a_value in range(16):
                cursor.execute("INSERT INTO test (a, b, c, s1, s2) VALUES (%d, %d, %d, %d, %d)" % (
                    99, a_value, a_value, 17, 42))

            for page_size in (2, 3, 4, 5, 15, 16, 17, 100):
                logger.debug("Using page size of %d", page_size)
                cursor.default_fetch_size = page_size
                for selector in selectors:
                    logger.debug("Using selector '%s'", selector)
                    results = list(cursor.execute("SELECT %s FROM test WHERE a = 99" % selector))
                    assert len(results) == 16
                    assert [res.a for res in results] == [99] * 16
                    assert [res.b for res in results] == list(range(16))
                    assert [res.c for res in results] == list(range(16))
                    if "s1" in selector:
                        assert [res.s1 for res in results] == [17] * 16
                    if "s2" in selector:
                        assert [res.s2 for res in results] == [42] * 16

            # reversed
            for page_size in (2, 3, 4, 5, 15, 16, 17, 100):
                logger.debug("Using page size of %d", page_size)
                cursor.default_fetch_size = page_size
                for selector in selectors:
                    logger.debug("Using selector '%s'", selector)
                    results = list(cursor.execute("SELECT %s FROM test WHERE a = 99 ORDER BY b DESC" % selector))
                    assert len(results) == 16
                    assert [res.a for res in results] == [99] * 16
                    assert [res.b for res in results] == list(reversed(range(16)))
                    self.assertEqual(list(reversed(range(16))), [res.c for res in results])
                    if "s1" in selector:
                        assert [res.s1 for res in results] == [17] * 16
                    if "s2" in selector:
                        assert [res.s2 for res in results] == [42] * 16

            # IN on clustering column
            for page_size in (2, 3, 4, 5, 15, 16, 17, 100):
                logger.debug("Using page size of %d", page_size)
                cursor.default_fetch_size = page_size
                for selector in selectors:
                    logger.debug("Using selector '%s'", selector)
                    results = list(cursor.execute(
                        "SELECT %s FROM test WHERE a = 99 AND b IN (3, 4, 8, 14, 15)" % selector))
                    assert len(results) == 5
                    assert [res.a for res in results] == [99] * 5
                    assert [res.b for res in results] == [3, 4, 8, 14, 15]
                    assert [res.c for res in results] == [3, 4, 8, 14, 15]
                    if "s1" in selector:
                        assert [res.s1 for res in results] == [17] * 16
                    if "s2" in selector:
                        assert [res.s2 for res in results] == [42] * 16

            # reversed IN on clustering column
            for page_size in (2, 3, 4, 5, 15, 16, 17, 100):
                logger.debug("Using page size of %d", page_size)
                cursor.default_fetch_size = page_size
                for selector in selectors:
                    logger.debug("Using selector '%s'", selector)
                    results = list(cursor.execute(
                        "SELECT %s FROM test WHERE a = 99 AND b IN (3, 4, 8, 14, 15) ORDER BY b DESC" % selector))
                    assert len(results) == 5
                    assert [res.a for res in results] == [99] * 5
                    assert [res.b for res in results] == list(reversed([3, 4, 8, 14, 15]))
                    assert [res.c for res in results] == list(reversed([3, 4, 8, 14, 15]))
                    if "s1" in selector:
                        assert [res.s1 for res in results] == [17] * 16
                    if "s2" in selector:
                        assert [res.s2 for res in results] == [42] * 16

            # slice on clustering column with set start
            for page_size in (2, 3, 4, 5, 15, 16, 17, 100):
                logger.debug("Using page size of %d", page_size)
                cursor.default_fetch_size = page_size
                for selector in selectors:
                    logger.debug("Using selector '%s'", selector)
                    results = list(cursor.execute("SELECT %s FROM test WHERE a = 99 AND b > 3" % selector))
                    assert len(results) == 12
                    assert [res.a for res in results] == [99] * 12
                    assert [res.b for res in results] == list(range(4, 16))
                    assert [res.c for res in results] == list(range(4, 16))
                    if "s1" in selector:
                        assert [res.s1 for res in results] == [17] * 12
                    if "s2" in selector:
                        assert [res.s2 for res in results] == [42] * 12

            # reversed slice on clustering column with set finish
            for page_size in (2, 3, 4, 5, 15, 16, 17, 100):
                logger.debug("Using page size of %d", page_size)
                cursor.default_fetch_size = page_size
                for selector in selectors:
                    logger.debug("Using selector '%s'", selector)
                    results = list(cursor.execute(
                        "SELECT %s FROM test WHERE a = 99 AND b > 3 ORDER BY b DESC" % selector))
                    assert len(results) == 12
                    assert [res.a for res in results] == [99] * 12
                    assert [res.b for res in results] == list(reversed(range(4, 16)))
                    assert [res.c for res in results] == list(reversed(range(4, 16)))
                    if "s1" in selector:
                        assert [res.s1 for res in results] == [17] * 12
                    if "s2" in selector:
                        assert [res.s2 for res in results] == [42] * 12

            # slice on clustering column with set finish
            for page_size in (2, 3, 4, 5, 15, 16, 17, 100):
                logger.debug("Using page size of %d", page_size)
                cursor.default_fetch_size = page_size
                for selector in selectors:
                    logger.debug("Using selector '%s'", selector)
                    results = list(cursor.execute("SELECT %s FROM test WHERE a = 99 AND b < 14" % selector))
                    assert len(results) == 14
                    assert [res.a for res in results] == [99] * 14
                    assert [res.b for res in results] == list(range(14))
                    assert [res.c for res in results] == list(range(14))
                    if "s1" in selector:
                        assert [res.s1 for res in results] == [17] * 14
                    if "s2" in selector:
                        assert [res.s2 for res in results] == [42] * 14

            # reversed slice on clustering column with set start
            for page_size in (2, 3, 4, 5, 15, 16, 17, 100):
                logger.debug("Using page size of %d", page_size)
                cursor.default_fetch_size = page_size
                for selector in selectors:
                    logger.debug("Using selector '%s'", selector)
                    results = list(cursor.execute(
                        "SELECT %s FROM test WHERE a = 99 AND b < 14 ORDER BY b DESC" % selector))
                    assert len(results) == 14
                    assert [res.a for res in results] == [99] * 14
                    assert [res.b for res in results] == list(reversed(range(14)))
                    assert list(reversed(range(14))) == [res.c for res in results]
                    if "s1" in selector:
                        assert [res.s1 for res in results] == [17] * 14
                    if "s2" in selector:
                        assert [res.s2 for res in results] == [42] * 14

            # slice on clustering column with start and finish
            for page_size in (2, 3, 4, 5, 15, 16, 17, 100):
                logger.debug("Using page size of %d", page_size)
                cursor.default_fetch_size = page_size
                for selector in selectors:
                    logger.debug("Using selector '%s'", selector)
                    results = list(cursor.execute("SELECT %s FROM test WHERE a = 99 AND b > 3 AND b < 14" % selector))
                    assert len(results) == 10
                    assert [res.a for res in results] == [99] * 10
                    assert [res.b for res in results] == list(range(4, 14))
                    assert [res.c for res in results] == list(range(4, 14))
                    if "s1" in selector:
                        assert [res.s1 for res in results] == [17] * 10
                    if "s2" in selector:
                        assert [res.s2 for res in results] == [42] * 10

            # reversed slice on clustering column with start and finish
            for page_size in (2, 3, 4, 5, 15, 16, 17, 100):
                logger.debug("Using page size of %d", page_size)
                cursor.default_fetch_size = page_size
                for selector in selectors:
                    logger.debug("Using selector '%s'", selector)
                    results = list(cursor.execute(
                        "SELECT %s FROM test WHERE a = 99 AND b > 3 AND b < 14 ORDER BY b DESC" % selector))
                    assert len(results) == 10
                    assert [res.a for res in results] == [99] * 10
                    self.assertEqual(list(reversed(range(4, 14))), [res.b for res in results])
                    self.assertEqual(list(reversed(range(4, 14))), [res.c for res in results])
                    if "s1" in selector:
                        assert [res.s1 for res in results] == [17] * 10
                    if "s2" in selector:
                        assert [res.s2 for res in results] == [42] * 10

    @pytest.mark.since('2.0')
    def test_paging_using_secondary_indexes_with_static_cols(self):
        cursor = self.prepare()
        cursor.execute(
            "CREATE TABLE paging_test ( id int, s1 int static, s2 int static, mybool boolean, sometext text,"
            " PRIMARY KEY (id, sometext) )")
        cursor.execute("CREATE INDEX ON paging_test(mybool)")

        def bool_from_str_int(text):
            return bool(int(text))

        for is_upgraded, cursor in self.do_upgrade(cursor):
            cursor.row_factory = dict_factory
            logger.debug("Querying %s node" % "upgraded" if is_upgraded else "old")
            cursor.execute("TRUNCATE paging_test")

            data = """
                 | id | s1 | s2 | mybool| sometext |
             *100| 1  | 1  | 4  | 1     | [random] |
             *300| 2  | 2  | 3  | 0     | [random] |
             *500| 3  | 3  | 2  | 1     | [random] |
             *400| 4  | 4  | 1  | 0     | [random] |
                """
            all_data = create_rows(
                data, cursor, 'paging_test', cl=CL.ALL,
                format_funcs={'id': int, 'mybool': bool_from_str_int, 'sometext': random_txt, 's1': int, 's2': int}
            )

            future = cursor.execute_async(
                SimpleStatement("select * from paging_test where mybool = true",
                                fetch_size=400, consistency_level=CL.ALL)
            )

            page_fetcher = PageFetcher(future).request_all()

            # the query only searched for True rows, so let's pare down the expectations for comparison
            expected_data = list(filter(lambda x: x.get('mybool') is True, all_data))

            assert page_fetcher.pagecount() == 2
            assert page_fetcher.num_results_all() == [400, 200]
            assert expected_data == page_fetcher.all_data()


class TestPagingDataNodes3RF3(TestPagingData):
    NODES = 3
    RF = 3
    CL = CL.ALL


class TestPagingDataNodes2RF1(TestPagingData):
    NODES = 2
    RF = 1


class TestPagingDatasetChanges(BasePagingTester, PageAssertionMixin):
    """
    Tests concerned with paging when the queried dataset changes while pages are being retrieved.
    """

    def test_data_change_impacting_earlier_page(self):
        cursor = self.prepare()
        cursor.execute("CREATE TABLE paging_test ( id int, mytext text, PRIMARY KEY (id, mytext) )")

        for is_upgraded, cursor in self.do_upgrade(cursor):
            cursor.row_factory = dict_factory
            logger.debug("Querying %s node" % "upgraded" if is_upgraded else "old")
            cursor.execute("TRUNCATE paging_test")

            data = """
                  | id | mytext   |
              *500| 1  | [random] |
              *500| 2  | [random] |
                """
            expected_data = create_rows(data, cursor, 'paging_test', cl=CL.ALL,
                                        format_funcs={'id': int, 'mytext': random_txt})

            # get 501 rows so we have definitely got the 1st row of the second partition
            future = cursor.execute_async(
                SimpleStatement("select * from paging_test where id in (1,2)", fetch_size=501, consistency_level=CL.ALL)
            )

            page_fetcher = PageFetcher(future)
            # no need to request page here, because the first page is automatically retrieved

            # we got one page and should be done with the first partition (for id=1)
            # let's add another row for that first partition (id=1) and make sure it won't sneak into results
            cursor.execute(SimpleStatement(
                "insert into paging_test (id, mytext) values (1, 'foo')", consistency_level=CL.ALL))

            page_fetcher.request_all()
            assert page_fetcher.pagecount() == 2
            assert page_fetcher.num_results_all() == [501, 499]

            self.assert_equal_ignore_order(page_fetcher.all_data(), expected_data)

    def test_data_change_impacting_later_page(self):
        cursor = self.prepare()
        cursor.execute("CREATE TABLE paging_test ( id int, mytext text, PRIMARY KEY (id, mytext) )")

        for is_upgraded, cursor in self.do_upgrade(cursor):
            cursor.row_factory = dict_factory
            logger.debug("Querying %s node" % "upgraded" if is_upgraded else "old")
            cursor.execute("TRUNCATE paging_test")

            data = """
                  | id | mytext   |
              *500| 1  | [random] |
              *499| 2  | [random] |
                """
            expected_data = create_rows(data, cursor, 'paging_test', cl=CL.ALL,
                                        format_funcs={'id': int, 'mytext': random_txt})

            future = cursor.execute_async(
                SimpleStatement("select * from paging_test where id in (1,2)", fetch_size=500, consistency_level=CL.ALL)
            )

            page_fetcher = PageFetcher(future)
            # no need to request page here, because the first page is automatically retrieved

            # we've already paged the first partition, but adding a row for the second (id=2)
            # should still result in the row being seen on the subsequent pages
            cursor.execute(SimpleStatement(
                "insert into paging_test (id, mytext) values (2, 'foo')", consistency_level=CL.ALL))

            page_fetcher.request_all()
            assert page_fetcher.pagecount() == 2
            assert page_fetcher.num_results_all() == [500, 500]

            # add the new row to the expected data and then do a compare
            expected_data.append({u'id': 2, u'mytext': u'foo'})
            self.assert_equal_ignore_order(page_fetcher.all_data(), expected_data)

    def test_row_ttl_expiry_during_paging(self):
        cursor = self.prepare()
        cursor.execute("CREATE TABLE paging_test ( id int, mytext text, PRIMARY KEY (id, mytext) )")

        for is_upgraded, cursor in self.do_upgrade(cursor):
            cursor.row_factory = dict_factory
            logger.debug("Querying %s node" % "upgraded" if is_upgraded else "old")
            cursor.execute("TRUNCATE paging_test")

            # create rows with TTL (some of which we'll try to get after expiry)
            create_rows(
                """
                    | id | mytext   |
                *300| 1  | [random] |
                *400| 2  | [random] |
                """,
                cursor, 'paging_test', cl=CL.ALL, format_funcs={'id': int, 'mytext': random_txt}, postfix='USING TTL 10'
            )

            # create rows without TTL
            create_rows(
                """
                    | id | mytext   |
                *500| 3  | [random] |
                """,
                cursor, 'paging_test', cl=CL.ALL, format_funcs={'id': int, 'mytext': random_txt}
            )

            future = cursor.execute_async(
                SimpleStatement("select * from paging_test where id in (1,2,3)",
                                fetch_size=300, consistency_level=CL.ALL)
            )

            oage_fetcher = PageFetcher(future)
            # no need to request page here, because the first page is automatically retrieved
            # this page will be partition id=1, it has TTL rows but they are not expired yet

            # sleep so that the remaining TTL rows from partition id=2 expire
            time.sleep(15)

            oage_fetcher.request_all()
            assert oage_fetcher.pagecount() == 3
            assert oage_fetcher.num_results_all() == [300, 300, 200]

    def test_cell_ttl_expiry_during_paging(self):
        cursor = self.prepare()
        cursor.execute("""
            CREATE TABLE paging_test (
                id int,
                mytext text,
                somevalue text,
                anothervalue text,
                PRIMARY KEY (id, mytext) )
            """)

        for is_upgraded, cursor in self.do_upgrade(cursor):
            cursor.row_factory = dict_factory
            logger.debug("Querying %s node" % "upgraded" if is_upgraded else "old")
            cursor.execute("TRUNCATE paging_test")

            data = create_rows(
                """
                    | id | mytext   | somevalue | anothervalue |
                *500| 1  | [random] | foo       |  bar         |
                *500| 2  | [random] | foo       |  bar         |
                *500| 3  | [random] | foo       |  bar         |
                """,
                cursor, 'paging_test', cl=CL.ALL, format_funcs={'id': int, 'mytext': random_txt}
            )

            future = cursor.execute_async(
                SimpleStatement("select * from paging_test where id in (1,2,3)",
                                fetch_size=500, consistency_level=CL.ALL)
            )

            page_fetcher = PageFetcher(future)

            # no need to request page here, because the first page is automatically retrieved
            page1 = page_fetcher.page_data(1)
            self.assert_equal_ignore_order(page1, data[:500])

            # set some TTLs for data on page 3
            for row in data[1000:1500]:
                _id, mytext = row['id'], row['mytext']
                stmt = SimpleStatement("""
                    update paging_test using TTL 10
                    set somevalue='one', anothervalue='two' where id = {id} and mytext = '{mytext}'
                    """.format(id=_id, mytext=mytext),
                    consistency_level=CL.ALL
                )
                cursor.execute(stmt)

            # check page two
            page_fetcher.request_one()
            page2 = page_fetcher.page_data(2)
            self.assert_equal_ignore_order(page2, data[500:1000])

            page3expected = []
            for row in data[1000:1500]:
                _id, mytext = row['id'], row['mytext']
                page3expected.append(
                    {u'id': _id, u'mytext': mytext, u'somevalue': None, u'anothervalue': None}
                )

            time.sleep(15)

            page_fetcher.request_one()
            page3 = page_fetcher.page_data(3)
            self.assert_equal_ignore_order(page3, page3expected)


class TestPagingDatasetChangesNodes3RF3(TestPagingDatasetChanges):
    NODES = 3
    RF = 3
    CL = CL.ALL


class TestPagingDatasetChangesNodes2RF1(TestPagingDatasetChanges):
    NODES = 2
    RF = 1


class TestPagingQueryIsolation(BasePagingTester, PageAssertionMixin):
    """
    Tests concerned with isolation of paged queries (queries can't affect each other).
    """

    def test_query_isolation(self):
        """
        Interleave some paged queries and make sure nothing bad happens.
        """
        cursor = self.prepare()
        cursor.execute("CREATE TABLE paging_test ( id int, mytext text, PRIMARY KEY (id, mytext) )")

        for is_upgraded, cursor in self.do_upgrade(cursor):
            cursor.row_factory = dict_factory
            logger.debug("Querying %s node" % "upgraded" if is_upgraded else "old")
            cursor.execute("TRUNCATE paging_test")

            data = """
                   | id | mytext   |
              *5000| 1  | [random] |
              *5000| 2  | [random] |
              *5000| 3  | [random] |
              *5000| 4  | [random] |
              *5000| 5  | [random] |
              *5000| 6  | [random] |
              *5000| 7  | [random] |
              *5000| 8  | [random] |
              *5000| 9  | [random] |
              *5000| 10 | [random] |
                """
            expected_data = create_rows(data, cursor, 'paging_test', cl=CL.ALL,
                                        format_funcs={'id': int, 'mytext': random_txt})

            stmts = [
                SimpleStatement("select * from paging_test where id in (1)", fetch_size=500, consistency_level=CL.ALL),
                SimpleStatement("select * from paging_test where id in (2)", fetch_size=600, consistency_level=CL.ALL),
                SimpleStatement("select * from paging_test where id in (3)", fetch_size=700, consistency_level=CL.ALL),
                SimpleStatement("select * from paging_test where id in (4)", fetch_size=800, consistency_level=CL.ALL),
                SimpleStatement("select * from paging_test where id in (5)", fetch_size=900, consistency_level=CL.ALL),
                SimpleStatement("select * from paging_test where id in (1)", fetch_size=1000, consistency_level=CL.ALL),
                SimpleStatement("select * from paging_test where id in (2)", fetch_size=1100, consistency_level=CL.ALL),
                SimpleStatement("select * from paging_test where id in (3)", fetch_size=1200, consistency_level=CL.ALL),
                SimpleStatement("select * from paging_test where id in (4)", fetch_size=1300, consistency_level=CL.ALL),
                SimpleStatement("select * from paging_test where id in (5)", fetch_size=1400, consistency_level=CL.ALL),
                SimpleStatement("select * from paging_test where id in (1,2,3,4,5,6,7,8,9,10)",
                                fetch_size=1500, consistency_level=CL.ALL)
            ]

            page_fetchers = []

            for stmt in stmts:
                future = cursor.execute_async(stmt)
                page_fetchers.append(PageFetcher(future))
                # first page is auto-retrieved, so no need to request it

            for page_fetcher in page_fetchers:
                page_fetcher.request_one(timeout=10)

            for page_fetcher in page_fetchers:
                page_fetcher.request_one(timeout=10)

            for page_fetcher in page_fetchers:
                page_fetcher.request_all(timeout=10)

            assert page_fetchers[0].pagecount() == 10
            assert page_fetchers[1].pagecount() == 9
            assert page_fetchers[2].pagecount() == 8
            assert page_fetchers[3].pagecount() == 7
            assert page_fetchers[4].pagecount() == 6
            assert page_fetchers[5].pagecount() == 5
            assert page_fetchers[6].pagecount() == 5
            assert page_fetchers[7].pagecount() == 5
            assert page_fetchers[8].pagecount() == 4
            assert page_fetchers[9].pagecount() == 4
            assert page_fetchers[10].pagecount() == 34

            self.assert_equal_ignore_order(flatten_into_set(
                page_fetchers[0].all_data()), flatten_into_set(expected_data[:5000]))
            self.assert_equal_ignore_order(flatten_into_set(
                page_fetchers[1].all_data()), flatten_into_set(expected_data[5000:10000]))
            self.assert_equal_ignore_order(flatten_into_set(
                page_fetchers[2].all_data()), flatten_into_set(expected_data[10000:15000]))
            self.assert_equal_ignore_order(flatten_into_set(
                page_fetchers[3].all_data()), flatten_into_set(expected_data[15000:20000]))
            self.assert_equal_ignore_order(flatten_into_set(
                page_fetchers[4].all_data()), flatten_into_set(expected_data[20000:25000]))
            self.assert_equal_ignore_order(flatten_into_set(
                page_fetchers[5].all_data()), flatten_into_set(expected_data[:5000]))
            self.assert_equal_ignore_order(flatten_into_set(
                page_fetchers[6].all_data()), flatten_into_set(expected_data[5000:10000]))
            self.assert_equal_ignore_order(flatten_into_set(
                page_fetchers[7].all_data()), flatten_into_set(expected_data[10000:15000]))
            self.assert_equal_ignore_order(flatten_into_set(
                page_fetchers[8].all_data()), flatten_into_set(expected_data[15000:20000]))
            self.assert_equal_ignore_order(flatten_into_set(
                page_fetchers[9].all_data()), flatten_into_set(expected_data[20000:25000]))
            self.assert_equal_ignore_order(flatten_into_set(
                page_fetchers[10].all_data()), flatten_into_set(expected_data[:50000]))


class TestPagingQueryIsolationNodes3RF3(TestPagingQueryIsolation):
    NODES = 3
    RF = 3
    CL = CL.ALL


class TestPagingQueryIsolationNodes2RF1(TestPagingQueryIsolation):
    NODES = 2
    RF = 1


class TestPagingWithDeletions(BasePagingTester, PageAssertionMixin):
    """
    Tests concerned with paging when deletions occur.
    """

    @staticmethod
    def setup_schema(cursor):
        cursor.execute("CREATE TABLE paging_test ( "
                       "id int, mytext text, col1 int, col2 int, col3 int, "
                       "PRIMARY KEY (id, mytext) )")

    def setup_data(self, cursor):

        data = """
             | id | mytext   | col1 | col2 | col3 |
          *40| 1  | [random] | 1    | 1    | 1    |
          *40| 2  | [random] | 2    | 2    | 2    |
          *40| 3  | [random] | 4    | 3    | 3    |
          *40| 4  | [random] | 4    | 4    | 4    |
          *40| 5  | [random] | 5    | 5    | 5    |
        """

        create_rows(data, cursor, 'paging_test', cl=CL.ALL,
                    format_funcs={
                        'id': int,
                        'mytext': random_txt,
                        'col1': int,
                        'col2': int,
                        'col3': int
                    })

        page_fetcher = self.get_page_fetcher(cursor)
        page_fetcher.request_all()
        return page_fetcher.all_data()

    @staticmethod
    def get_page_fetcher(cursor):
        future = cursor.execute_async(
            SimpleStatement("select * from paging_test where id in (1,2,3,4,5)", fetch_size=25,
                            consistency_level=CL.ALL)
        )

        return PageFetcher(future)

    def check_all_paging_results(self, cursor, expected_data, pagecount, num_page_results, timeout=None):
        """Check all paging results: pagecount, num_results per page, data."""

        page_size = 25
        expected_pages_data = [expected_data[x:x + page_size] for x in
                               range(0, len(expected_data), page_size)]

        page_fetcher = self.get_page_fetcher(cursor)
        page_fetcher.request_all(timeout=timeout)
        assert page_fetcher.pagecount() == pagecount
        assert num_page_results == page_fetcher.num_results_all()

        for page_fetcher_idx in range(page_fetcher.pagecount()):
            page_data = page_fetcher.page_data(page_fetcher_idx + 1)
            assert page_data == expected_pages_data[page_fetcher_idx]

    def test_single_partition_deletions(self):
        """Test single partition deletions """
        cursor = self.prepare()
        self.setup_schema(cursor)

        for is_upgraded, cursor in self.do_upgrade(cursor):
            cursor.row_factory = dict_factory
            logger.debug("Querying %s node" % "upgraded" if is_upgraded else "old")
            cursor.execute("TRUNCATE paging_test")

            expected_data = self.setup_data(cursor)

            # Delete the a single partition at the beginning
            cursor.execute(
                SimpleStatement("delete from paging_test where id = 1",
                                consistency_level=CL.ALL)
            )
            expected_data = [row for row in expected_data if row['id'] != 1]
            self.check_all_paging_results(cursor, expected_data, 7,
                                          [25, 25, 25, 25, 25, 25, 10],
                                          timeout=10)

            # Delete the a single partition in the middle
            cursor.execute(
                SimpleStatement("delete from paging_test where id = 3",
                                consistency_level=CL.ALL)
            )
            expected_data = [row for row in expected_data if row['id'] != 3]
            self.check_all_paging_results(cursor, expected_data, 5, [25, 25, 25, 25, 20],
                                          timeout=10)

            # Delete the a single partition at the end
            cursor.execute(
                SimpleStatement("delete from paging_test where id = 5",
                                consistency_level=CL.ALL))
            expected_data = [row for row in expected_data if row['id'] != 5]
            self.check_all_paging_results(cursor, expected_data, 4, [25, 25, 25, 5],
                                          timeout=10)

            # Keep only the partition '2'
            cursor.execute(
                SimpleStatement("delete from paging_test where id = 4",
                                consistency_level=CL.ALL))
            expected_data = [row for row in expected_data if row['id'] != 4]
            self.check_all_paging_results(cursor, expected_data, 2, [25, 15],
                                          timeout=10)

    def test_multiple_partition_deletions(self):
        """Test multiple partition deletions """
        cursor = self.prepare()
        self.setup_schema(cursor)

        for is_upgraded, cursor in self.do_upgrade(cursor):
            cursor.row_factory = dict_factory
            logger.debug("Querying %s node" % "upgraded" if is_upgraded else "old")
            cursor.execute("TRUNCATE paging_test")
            expected_data = self.setup_data(cursor)

            # Keep only the partition '1'
            cursor.execute(
                SimpleStatement("delete from paging_test where id in (2,3,4,5)",
                                consistency_level=CL.ALL)
            )
            expected_data = [row for row in expected_data if row['id'] == 1]
            self.check_all_paging_results(cursor, expected_data, 2, [25, 15],
                                          timeout=10)

    def test_single_row_deletions(self):
        """Test single row deletions """
        cursor = self.prepare()
        self.setup_schema(cursor)

        for is_upgraded, cursor in self.do_upgrade(cursor):
            cursor.row_factory = dict_factory
            logger.debug("Querying %s node" % "upgraded" if is_upgraded else "old")
            cursor.execute("TRUNCATE paging_test")
            expected_data = self.setup_data(cursor)

            # Delete the first row
            row = expected_data.pop(0)
            cursor.execute(SimpleStatement(
                ("delete from paging_test where "
                 "id = {} and mytext = '{}'".format(row['id'], row['mytext'])),
                consistency_level=CL.ALL))
            self.check_all_paging_results(cursor, expected_data, 8,
                                          [25, 25, 25, 25, 25, 25, 25, 24])

            # Delete a row in the middle
            row = expected_data.pop(100)
            cursor.execute(SimpleStatement(
                ("delete from paging_test where "
                 "id = {} and mytext = '{}'".format(row['id'], row['mytext'])),
                consistency_level=CL.ALL)
            )
            self.check_all_paging_results(cursor, expected_data, 8,
                                          [25, 25, 25, 25, 25, 25, 25, 23])

            # Delete the last row
            row = expected_data.pop()
            cursor.execute(SimpleStatement(
                ("delete from paging_test where "
                 "id = {} and mytext = '{}'".format(row['id'], row['mytext'])),
                consistency_level=CL.ALL)
            )
            self.check_all_paging_results(cursor, expected_data, 8,
                                          [25, 25, 25, 25, 25, 25, 25, 22])

            # Delete all the last page row by row
            rows = expected_data[-22:]
            for row in rows:
                cursor.execute(SimpleStatement(
                    ("delete from paging_test where "
                     "id = {} and mytext = '{}'".format(row['id'], row['mytext'])),
                    consistency_level=CL.ALL)
                )
            self.check_all_paging_results(cursor, expected_data, 7,
                                          [25, 25, 25, 25, 25, 25, 25])

    def test_single_cell_deletions(self):
        """Test single cell deletions """
        cursor = self.prepare()
        self.setup_schema(cursor)

        for is_upgraded, cursor in self.do_upgrade(cursor):
            cursor.row_factory = dict_factory
            logger.debug("Querying %s node" % "upgraded" if is_upgraded else "old")
            cursor.execute("TRUNCATE paging_test")
            expected_data = self.setup_data(cursor)

            # Delete the first cell of some rows of the last partition
            pkeys = [r['mytext'] for r in expected_data if r['id'] == 5][:20]
            for row in expected_data:
                if row['id'] == 5 and row['mytext'] in pkeys:
                    row['col1'] = None

            for pkey in pkeys:
                cursor.execute(SimpleStatement(
                    ("delete col1 from paging_test where id = 5 "
                     "and mytext = '{}'".format(pkey)),
                    consistency_level=CL.ALL))
            self.check_all_paging_results(cursor, expected_data, 8,
                                          [25, 25, 25, 25, 25, 25, 25, 25])

            # Delete the mid cell of some rows of the first partition
            pkeys = [r['mytext'] for r in expected_data if r['id'] == 1][20:]
            for row in expected_data:
                if row['id'] == 1 and row['mytext'] in pkeys:
                    row['col2'] = None

            for pkey in pkeys:
                cursor.execute(SimpleStatement(
                    ("delete col2 from paging_test where id = 1 "
                     "and mytext = '{}'".format(pkey)),
                    consistency_level=CL.ALL))
            self.check_all_paging_results(cursor, expected_data, 8,
                                          [25, 25, 25, 25, 25, 25, 25, 25])

            # Delete the last cell of all rows of the mid partition
            pkeys = [r['mytext'] for r in expected_data if r['id'] == 3]
            for row in expected_data:
                if row['id'] == 3 and row['mytext'] in pkeys:
                    row['col3'] = None

            for pkey in pkeys:
                cursor.execute(SimpleStatement(
                    ("delete col3 from paging_test where id = 3 "
                     "and mytext = '{}'".format(pkey)),
                    consistency_level=CL.ALL))
            self.check_all_paging_results(cursor, expected_data, 8,
                                          [25, 25, 25, 25, 25, 25, 25, 25])

    def test_multiple_cell_deletions(self):
        """Test multiple cell deletions """
        cursor = self.prepare()
        self.setup_schema(cursor)

        for is_upgraded, cursor in self.do_upgrade(cursor):
            cursor.row_factory = dict_factory
            logger.debug("Querying %s node" % "upgraded" if is_upgraded else "old")
            cursor.execute("TRUNCATE paging_test")
            expected_data = self.setup_data(cursor)

            # Delete the multiple cells of some rows of the second partition
            pkeys = [r['mytext'] for r in expected_data if r['id'] == 2][20:]
            for row in expected_data:
                if row['id'] == 2 and row['mytext'] in pkeys:
                    row['col1'] = None
                    row['col2'] = None

            for pkey in pkeys:
                cursor.execute(SimpleStatement(
                    ("delete col1, col2 from paging_test where id = 2 "
                     "and mytext = '{}'".format(pkey)),
                    consistency_level=CL.ALL))
            self.check_all_paging_results(cursor, expected_data, 8,
                                          [25, 25, 25, 25, 25, 25, 25, 25])

            # Delete the multiple cells of all rows of the fourth partition
            pkeys = [r['mytext'] for r in expected_data if r['id'] == 4]
            for row in expected_data:
                if row['id'] == 4 and row['mytext'] in pkeys:
                    row['col2'] = None
                    row['col3'] = None

            for pkey in pkeys:
                cursor.execute(SimpleStatement(
                    ("delete col2, col3 from paging_test where id = 4 "
                     "and mytext = '{}'".format(pkey)),
                    consistency_level=CL.ALL))
            self.check_all_paging_results(cursor, expected_data, 8,
                                          [25, 25, 25, 25, 25, 25, 25, 25])

    def test_ttl_deletions(self):
        """Test ttl deletions. Paging over a query that has only tombstones """
        cursor = self.prepare()
        self.setup_schema(cursor)

        for is_upgraded, cursor in self.do_upgrade(cursor):
            cursor.row_factory = dict_factory
            logger.debug("Querying %s node" % "upgraded" if is_upgraded else "old")
            cursor.execute("TRUNCATE paging_test")
            data = self.setup_data(cursor)

            # Set TTL to all row
            for row in data:
                query_string = ("insert into paging_test (id, mytext, col1, col2, col3) "
                                "values ({}, '{}', {}, {}, {}) using ttl 3;").format(
                                    row['id'], row['mytext'], row['col1'], row['col2'], row['col3'])
                cursor.execute(SimpleStatement(query_string, consistency_level=CL.ALL))
            time.sleep(5)
            self.check_all_paging_results(cursor, [], 0, [])

    def test_failure_threshold_deletions(self, fixture_dtest_setup: DTestSetup):
        """Test that paging throws a failure in case of tombstone threshold """
        fixture_dtest_setup.allow_log_errors = True
        self.cluster.set_configuration_options(
            values={'tombstone_failure_threshold': 500,
                    'read_request_timeout_in_ms': 1000,
                    'request_timeout_in_ms': 1000,
                    'range_request_timeout_in_ms': 1000}
        )
        cursor = self.prepare()
        nodes = self.cluster.nodelist()
        self.setup_schema(cursor)

        for is_upgraded, cursor in self.do_upgrade(cursor):
            cursor.row_factory = dict_factory
            logger.debug("Querying %s node" % "upgraded" if is_upgraded else "old")
            cursor.execute("TRUNCATE paging_test")
            self.setup_data(cursor)

            # Add more data
            for value in map(lambda i: uuid.uuid4(), range(3000)):
                cursor.execute(SimpleStatement(
                    "insert into paging_test (id, mytext, col1) values (1, '{}', null) ".format(
                        value
                    ),
                    consistency_level=CL.ALL
                ))

            stmt = SimpleStatement("select * from paging_test", fetch_size=1000, consistency_level=CL.ALL)
            assert_read_timeout_or_failure(cursor, stmt)

            # new pattern
            patterns = [r"Scanned over.* tombstones during query 'SELECT \* FROM ks.paging_test.* query aborted",
                        "Scanned over.* tombstones in ks.paging_test.* query aborted"]  # old pattern

            failed = any([n.grep_log(m) for n, m in itertools.product(nodes, patterns)])

            assert failed == "Cannot find tombstone failure threshold error in log for {} node".format(
                "upgraded" if is_upgraded else "old")


class TestPagingWithDeletionsNodes3RF3(TestPagingWithDeletions):
    NODES = 3
    RF = 3
    CL = CL.ALL


class TestPagingWithDeletionsNodes2RF1(TestPagingWithDeletions):
    NODES = 2
    RF = 1
