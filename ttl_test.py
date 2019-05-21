import time
import logging
import pytest
import math
from collections import OrderedDict
from datetime import datetime

from tools.data import rows_to_list

from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement
from cassandra.util import sortedset

from tools.assertions import (
    assert_all,
    assert_none,
    assert_row_count,
    assert_almost_equal,
    assert_unavailable,
    assert_invalid
)
from dtest_class import Tester, create_ks
from tools.data import drop_table
from tools.marks import enterprise_only_param

logger = logging.getLogger(__name__)


@pytest.mark.dtest_full
class TestTTL(Tester):
    """ Test Time To Live Feature """

    def prepare(self, default_time_to_live=None, create_table_statement=None, nodes=1, rf=1, configuration_options=None):
        if configuration_options:
            self.cluster.set_configuration_options(values=configuration_options)
        self.cluster.populate(nodes).start()
        node1 = self.cluster.nodelist()[0]
        self.session1 = self.patient_cql_connection(node1)
        create_ks(self.session1, 'ks', rf=rf)

        drop_table(session=self.session1, table_name='ttl_table', if_exists=True)

        if create_table_statement is None:
            query = """
                CREATE TABLE ttl_table (
                    key int primary key,
                    col1 int,
                    col2 int,
                    col3 int,
                )
            """
        else:
            query = create_table_statement
        if default_time_to_live:
            query += " WITH default_time_to_live = {};".format(default_time_to_live)

        self.session1.execute(query)

    @staticmethod
    def format_float_time_to_readable(float_time=None):
        return datetime.fromtimestamp(float_time if float_time else time.time()).strftime('%Y-%m-%d %H:%M:%S.%f')

    def smart_sleep(self, start_time, time_to_wait):
        """ Function that sleep smartly based on the start_time.
            Useful when tests are slower than expected.

            start_time: The start time of the timed operations
            time_to_wait: The time to wait in seconds from the start_time
        """

        now = time.time()
        real_time_to_wait = time_to_wait - (now - start_time)

        if real_time_to_wait > 0:
            time.sleep(real_time_to_wait)

    # Temporary function - to debug the github.com/scylladb/scylla-dtest/issues/824 issue
    def smart_sleep_with_print(self, start_time, time_to_wait):
        """ Function that sleep smartly based on the start_time.
            Useful when tests are slower than expected.

            start_time: The start time of the timed operations
            time_to_wait: The time to wait in seconds from the start_time
        """

        now = time.time()
        logger.debug('Start action time is {}'.format(self.format_float_time_to_readable(start_time)))
        logger.debug('Start to wait at: {}'.format(self.format_float_time_to_readable(now)))
        real_time_to_wait = time_to_wait - (now - start_time)

        if real_time_to_wait > 0:
            time.sleep(real_time_to_wait)
        stop_time = time.time()
        logger.debug('Stop to wait at: {}'.format(self.format_float_time_to_readable(stop_time)))
        logger.debug('   Waiting time is {}'.format(stop_time-start_time))

    @pytest.mark.single_node
    def test_default_ttl(self):
        """ Test default_time_to_live specified on a table """

        self.prepare(default_time_to_live=1)
        start = time.time()
        self.session1.execute("INSERT INTO ttl_table (key, col1) VALUES (%d, %d)" % (1, 1))
        self.session1.execute("INSERT INTO ttl_table (key, col1) VALUES (%d, %d)" % (2, 2))
        self.session1.execute("INSERT INTO ttl_table (key, col1) VALUES (%d, %d)" % (3, 3))
        self.smart_sleep(start, 3)
        assert_row_count(self.session1, 'ttl_table', 0)

    def eventually_expires(self, session, expected: list, before: float, after: float, ttl: int, table: str = 'ttl_table', cl: ConsistencyLevel = ConsistencyLevel.ONE):
        """ verify that TTLed data eventually expires within +/- 1 second of the TTL """
        self.smart_sleep(before, ttl - 2)
        logger.debug("Verifying that data is still valid")
        assert_row_count(session, table, 1)  # should still exist
        query = f"SELECT * FROM {table}"
        simple_query = SimpleStatement(query, consistency_level=cl)
        list_res = rows_to_list(session.execute(simple_query))
        error = f"Expected {expected} from {query}, but got {list_res}"
        assert list_res == expected, error

        while time.time() - math.ceil(after) < ttl + 1:
            # Non-key column value has its original value and shouldn't became None
            list_res = rows_to_list(session.execute(simple_query))
            if not list_res:
                assert time.time() - math.floor(before) >= ttl - 1, "Data has expired prematurely"
                break
            # Issue #5290: Data expired via TTL is returned with null values for a short period of time
            assert list_res == expected, error
            time.sleep(0.1)

        logger.debug("Verifying that data has expired")
        if list_res:
            list_res = rows_to_list(session.execute(simple_query))
            assert not list_res, f"Expected [] from {query}, but got {list_res}"
        assert_row_count(session, table, 0)

    @pytest.mark.single_node
    def test_insert_ttl_has_priority_on_defaut_ttl(self):
        """ Test that a ttl specified during an insert has priority on the default table ttl """

        logger.debug("Preparing table with default_time_to_live=1")
        self.prepare(default_time_to_live=1)

        ttl = 5
        logger.debug(f"Inserting data USING TTL {ttl}")
        before = time.time()
        self.session1.execute(f"""
            INSERT INTO ttl_table (key, col1, col2, col3) VALUES (1, 1, 1, 1) USING TTL {ttl};
        """)
        after = time.time()

        self.eventually_expires(self.session1, [[1, 1, 1, 1]], before, after, ttl)

    @pytest.mark.single_node
    def test_insert_ttl_works_without_default_ttl(self):
        """ Test that a ttl specified during an insert works even if a table has no default ttl """

        self.prepare()

        ttl = 6
        logger.debug(f"Inserting data USING TTL {ttl}")
        before = time.time()
        self.session1.execute(f"""
            INSERT INTO ttl_table (key, col1, col2, col3) VALUES (1, 1, 1, 1) USING TTL {ttl};
        """)
        after = time.time()

        self.eventually_expires(self.session1, [[1, 1, 1, 1]], before, after, ttl)

    @pytest.mark.single_node
    def test_default_ttl_can_be_removed(self):
        """ Test that default_time_to_live can be removed """

        self.prepare(default_time_to_live=1)

        start = time.time()
        self.session1.execute("ALTER TABLE ttl_table WITH default_time_to_live = 0;")
        self.session1.execute("""
            INSERT INTO ttl_table (key, col1) VALUES (%d, %d);
        """ % (1, 1))
        self.smart_sleep(start, 1.5)
        assert_row_count(self.session1, 'ttl_table', 1)

    @pytest.mark.next_gating
    @pytest.mark.dtest_debug
    @pytest.mark.single_node
    def test_removing_default_ttl_does_not_affect_existing_rows(self):
        """ Test that removing a default_time_to_live doesn't affect the existings rows """

        self.prepare(default_time_to_live=1)

        self.session1.execute("ALTER TABLE ttl_table WITH default_time_to_live = 10;")
        start = time.time()
        self.session1.execute("""
            INSERT INTO ttl_table (key, col1) VALUES (%d, %d);
        """ % (1, 1))
        self.session1.execute("""
            INSERT INTO ttl_table (key, col1) VALUES (%d, %d) USING TTL 15;
        """ % (2, 1))
        self.session1.execute("ALTER TABLE ttl_table WITH default_time_to_live = 0;")
        self.session1.execute("INSERT INTO ttl_table (key, col1) VALUES (%d, %d);" % (3, 1))
        self.smart_sleep(start, 5)
        assert_row_count(self.session1, 'ttl_table', 3)
        self.smart_sleep(start, 12)
        assert_row_count(self.session1, 'ttl_table', 2)
        self.smart_sleep(start, 20)
        assert_row_count(self.session1, 'ttl_table', 1)

    @pytest.mark.next_gating
    @pytest.mark.dtest_debug
    def test_row_marker_for_ttl(self):
        """ Test that rows are removed correctly with a default_time_to_live and TTL
            Test the table with PK and CK
        """

        table_create_statement = "CREATE TABLE ttl_table (key int, col1 int, col2 int, col3 int, " \
                                 "primary key(key, col1))"
        self.prepare(default_time_to_live=1, create_table_statement=table_create_statement,
                     nodes=4)

        default_ttl = 10
        explicit_ttl = 15
        self.session1.execute("ALTER TABLE ttl_table WITH default_time_to_live = {};".format(default_ttl))
        self.session1.execute("""
            INSERT INTO ttl_table (key, col1, col2, col3) VALUES (%d, %d, %d, %d);
        """ % (1, 1, 1, 1))
        start_default = time.time()
        logger.debug("Wrote [1, 1, 1, 1] with default ttl {}".format(default_ttl))
        self.session1.execute("""
            INSERT INTO ttl_table (key, col1, col2, col3) VALUES (%d, %d, %d, %d) USING TTL %d;
        """ % (1, 2, 2, 2, explicit_ttl))
        start_explicit = time.time()
        logger.debug("Wrote [1, 2, 2, 2] with explicit ttl {}".format(explicit_ttl))

        def get_rows(session):
            res = session.execute("SELECT * FROM ttl_table;")
            return [list(row) for row in res]

        def assert_rows(rows, expected):
            assert rows == expected, "Expected the following rows: {}, but got: {}".format(expected, rows)

        rows = get_rows(self.session1)
        expected = [[1, 1, 1, 1], [1, 2, 2, 2]]
        assert_rows(rows, expected)

        def wait_for_rows_to_change(expected_cur, expected_next, start, ttl):
            delta = time.time() - start
            rows = get_rows(self.session1)
            while rows == expected_cur and delta < ttl + 2:
                time.sleep(1)
                delta = time.time() - start
                rows = get_rows(self.session1)
            logger.debug("Got {} after {} seconds".format(rows, delta))
            assert_rows(rows, expected_next)
            assert ttl - \
                1 <= delta, "Expected delta time to be greater than {} seconds, but got {}".format(ttl - 1, delta)
            return rows

        expected_next = [[1, 2, 2, 2]]
        wait_for_rows_to_change(expected, expected_next, start_default, default_ttl)

        expected = expected_next
        expected_next = []
        wait_for_rows_to_change(expected, expected_next, start_explicit, explicit_ttl)

        assert_row_count(self.session1, 'ttl_table', 0)

    @pytest.mark.single_node
    def test_update_single_column_ttl(self):
        """ Test that specifying a TTL on a single column works """

        self.prepare()

        self.session1.execute("""
            INSERT INTO ttl_table (key, col1, col2, col3) VALUES (%d, %d, %d, %d);
        """ % (1, 1, 1, 1))
        start = time.time()
        self.session1.execute("UPDATE ttl_table USING TTL 3 set col1=42 where key=%s;" % (1,))
        assert_all(self.session1, "SELECT * FROM ttl_table;", [[1, 42, 1, 1]])
        self.smart_sleep(start, 5)
        assert_all(self.session1, "SELECT * FROM ttl_table;", [[1, None, 1, 1]])

    @pytest.mark.single_node
    def test_update_multiple_columns_ttl(self):
        """ Test that specifying a TTL on multiple columns works """

        self.prepare()

        self.session1.execute("""
            INSERT INTO ttl_table (key, col1, col2, col3) VALUES (%d, %d, %d, %d);
        """ % (1, 1, 1, 1))
        start = time.time()
        self.session1.execute("""
            UPDATE ttl_table USING TTL 2 set col1=42, col2=42, col3=42 where key=%s;
        """ % (1,))
        assert_all(self.session1, "SELECT * FROM ttl_table;", [[1, 42, 42, 42]])
        self.smart_sleep(start, 4)
        assert_all(self.session1, "SELECT * FROM ttl_table;", [[1, None, None, None]])

    @pytest.mark.single_node
    def test_update_column_ttl_with_default_ttl(self):
        """
        Test that specifying a column ttl works when a default ttl is set.
        This test specify a lower ttl for the column than the default ttl.
        """

        self.prepare(default_time_to_live=8)

        start = time.time()
        self.session1.execute("""
            INSERT INTO ttl_table (key, col1, col2, col3) VALUES (%d, %d, %d, %d);
        """ % (1, 1, 1, 1))
        self.session1.execute("UPDATE ttl_table USING TTL 3 set col1=42 where key=%s;" % (1,))
        assert_all(self.session1, "SELECT * FROM ttl_table;", [[1, 42, 1, 1]])
        self.smart_sleep(start, 5)
        assert_all(self.session1, "SELECT * FROM ttl_table;", [[1, None, 1, 1]])
        self.smart_sleep(start, 10)
        assert_row_count(self.session1, 'ttl_table', 0)

    @pytest.mark.single_node
    def test_update_column_ttl_with_default_ttl_2(self):
        """
        Test that specifying a column ttl works when a default ttl is set.
        This test specify a higher column ttl than the default ttl.
        """

        self.prepare(default_time_to_live=2)

        start = time.time()
        self.session1.execute("""
            INSERT INTO ttl_table (key, col1, col2, col3) VALUES (%d, %d, %d, %d);
        """ % (1, 1, 1, 1))
        self.session1.execute("UPDATE ttl_table USING TTL 6 set col1=42 where key=%s;" % (1,))
        self.smart_sleep(start, 4)
        assert_all(self.session1, "SELECT * FROM ttl_table;", [[1, 42, None, None]])
        self.smart_sleep(start, 8)
        assert_row_count(self.session1, 'ttl_table', 0)

    @pytest.mark.single_node
    def test_remove_column_ttl(self):
        """
        Test that removing a column ttl works.
        """

        self.prepare()

        start = time.time()
        self.session1.execute("""
            INSERT INTO ttl_table (key, col1, col2, col3) VALUES (%d, %d, %d, %d) USING TTL 2;
        """ % (1, 1, 1, 1))
        self.session1.execute("UPDATE ttl_table set col1=42 where key=%s;" % (1,))
        self.smart_sleep(start, 4)
        assert_all(self.session1, "SELECT * FROM ttl_table;", [[1, 42, None, None]])

    @pytest.mark.next_gating
    @pytest.mark.dtest_debug
    @pytest.mark.single_node
    def test_remove_column_ttl_with_default_ttl(self):
        """
        Test that we cannot remove a column ttl when a default ttl is set.
        """

        self.prepare(default_time_to_live=2)

        start = time.time()
        self.session1.execute("""
            INSERT INTO ttl_table (key, col1, col2, col3) VALUES (%d, %d, %d, %d);
        """ % (1, 1, 1, 1))
        self.session1.execute("""
            INSERT INTO ttl_table (key, col1, col2, col3) VALUES (%d, %d, %d, %d);
        """ % (2, 1, 1, 1))
        self.session1.execute("UPDATE ttl_table using ttl 0 set col1=42 where key=%s;" % (1,))
        self.session1.execute("UPDATE ttl_table using ttl 8 set col1=42 where key=%s;" % (2,))
        self.smart_sleep(start, 5)
        # The first row should be deleted, using ttl 0 should fallback to default_time_to_live
        assert_all(self.session1, "SELECT * FROM ttl_table;", [[2, 42, None, None]])
        self.smart_sleep(start, 10)
        assert_row_count(self.session1, 'ttl_table', 0)

    @pytest.mark.single_node
    def test_collection_list_ttl(self):
        """
        Test that ttl has a granularity of elements using a list collection.
        """

        cts = """
            CREATE TABLE ttl_table (
                key int primary key,
                col1 int,
                col2 int,
                col3 int,
                mylist list<int>
            )
        """

        self.prepare(default_time_to_live=10, create_table_statement=cts)

        start = time.time()
        self.session1.execute("""
            INSERT INTO ttl_table (key, col1, mylist) VALUES (%d, %d, %s);
        """ % (1, 1, [1, 2, 3, 4, 5]))
        self.session1.execute("""
            UPDATE ttl_table USING TTL 5 SET mylist[0] = 42, mylist[4] = 42 WHERE key=1;
        """)
        assert_all(self.session1, "SELECT * FROM ttl_table;", [[1, 1, None, None, [42, 2, 3, 4, 42]]])
        self.smart_sleep(start, 7)
        assert_all(self.session1, "SELECT * FROM ttl_table;", [[1, 1, None, None, [2, 3, 4]]])
        self.smart_sleep(start, 12)
        assert_row_count(self.session1, 'ttl_table', 0)

    @pytest.mark.single_node
    def test_collection_set_ttl(self):
        """
        Test that ttl has a granularity of elements using a set collection.
        """

        cts = """
            CREATE TABLE ttl_table (
                key int primary key,
                col1 int,
                col2 int,
                col3 int,
                myset set<int>
            )
        """

        self.prepare(default_time_to_live=10, create_table_statement=cts)

        start = time.time()
        self.session1.execute("""
            INSERT INTO ttl_table (key, col1, myset) VALUES (%d, %d, %s);
        """ % (1, 1, '{1,2,3,4,5}'))
        self.session1.execute("""
            UPDATE ttl_table USING TTL 3 SET myset = myset + {42} WHERE key=1;
        """)
        assert_all(
            self.session1,
            "SELECT * FROM ttl_table;",
            [[1, 1, None, None, sortedset([1, 2, 3, 4, 5, 42])]]
        )
        self.smart_sleep(start, 5)
        assert_all(
            self.session1,
            "SELECT * FROM ttl_table;",
            [[1, 1, None, None, sortedset([1, 2, 3, 4, 5])]]
        )
        self.smart_sleep(start, 12)
        assert_row_count(self.session1, 'ttl_table', 0)

    @pytest.mark.single_node
    def test_collection_map_ttl(self):
        """
        Test that ttl has a granularity of elements using a map collection.
        """

        cts = """
            CREATE TABLE ttl_table (
                key int primary key,
                col1 int,
                col2 int,
                col3 int,
                mymap map<int, int>
            )
        """

        self.prepare(default_time_to_live=6, create_table_statement=cts)

        start = time.time()
        self.session1.execute("""
            INSERT INTO ttl_table (key, col1, mymap) VALUES (%d, %d, %s);
        """ % (1, 1, '{1:1,2:2,3:3,4:4,5:5}'))
        self.session1.execute("""
            UPDATE ttl_table USING TTL 2 SET mymap[1] = 42, mymap[5] = 42 WHERE key=1;
        """)
        assert_all(
            self.session1,
            "SELECT * FROM ttl_table;",
            [[1, 1, None, None, OrderedDict([(1, 42), (2, 2), (3, 3), (4, 4), (5, 42)])]]
        )
        self.smart_sleep(start, 4)
        assert_all(
            self.session1,
            "SELECT * FROM ttl_table;",
            [[1, 1, None, None, OrderedDict([(2, 2), (3, 3), (4, 4)])]]
        )
        self.smart_sleep(start, 8)
        assert_row_count(self.session1, 'ttl_table', 0)

    @pytest.mark.single_node
    def test_delete_with_ttl_expired(self):
        """
        Updating a row with a ttl does not prevent deletion, test for CASSANDRA-6363
        """
        self.prepare()

        self.session1.execute("CREATE TABLE session (id text, usr text, valid int, PRIMARY KEY (id))")

        self.session1.execute("insert into session (id, usr) values ('abc', 'abc')")
        self.session1.execute("update session using ttl 1 set valid = 1 where id = 'abc'")
        self.smart_sleep(time.time(), 2)

        # Scylla does not support lightweight transactions, let's adapt
        # the statement
        # self.session1.execute("delete from session where id = 'abc' if usr ='abc'")
        self.session1.execute("delete from session where id = 'abc'")
        assert_row_count(self.session1, 'session', 0)

    @pytest.mark.single_node
    def test_boundary_ttl(self):
        """
        Test with boundary invalid and valid TTL.

        (2 ** 31) : 2147483648            # invalid signed int
        (2 ** 31) - 1 : 2147483647        # max signed int
        20 * 365 * 24 * 3600 : 630720000  # 20 years in seconds
        boundary_ttl = MAX_DELETE_TIME - int(time.time())  # max valid ttl which will reach to max signed
                                                           # int after adding current time
        boundary_ttl + 1 # invalid
        """
        self.prepare()

        self.session1.execute("CREATE TABLE session (id text, usr text, valid int, PRIMARY KEY (id))")

        # InvalidRequest: Error from server: code=2200 [Invalid query] message="marshaling error: Value out of range for type org.apache.cassandra.db.marshal.Int32Type: '2147483648'"
        assert_invalid(self.session1, "insert into session (id, usr) values ('abc', 'abc') USING TTL 2147483648")

        # InvalidRequest: Error from server: code=2200 [Invalid query] message="ttl is too large. requested (2147483647) maximum (630720000)"
        assert_invalid(self.session1, "insert into session (id, usr) values ('abc', 'abc') USING TTL 2147483647")

        # 20 years in seconds is the maximum ttl
        max_ttl = 630720000
        assert_invalid(self.session1, f"insert into session (id, usr) values ('abc', 'abc') USING TTL {max_ttl + 1}",
                       matching='ttl is too large')
        assert_row_count(self.session1, 'session', 0)
        self.session1.execute(f"insert into session (id, usr) values ('abc', 'abc') USING TTL {max_ttl}")
        assert_row_count(self.session1, 'session', 1)

        MAX_DELETE_TIME = 2 ** 31 - 1
        start_time = time.time()
        boundary_ttl = MAX_DELETE_TIME - int(start_time)

        self.session1.execute("insert into session (id, usr) values ('abc', 'abc') USING TTL %s" % boundary_ttl)
        assert_row_count(self.session1, 'session', 1)
        self.smart_sleep(start_time, 10)
        assert_row_count(self.session1, 'session', 1)

        start_time = time.time()
        boundary_ttl = MAX_DELETE_TIME - int(start_time)
        self.session1.execute(f"insert into session (id, usr) values ('def', 'def') USING TTL {boundary_ttl + 1}")
        assert_row_count(self.session1, 'session', 2)

        start_time = time.time()
        self.session1.execute("insert into session (id, usr) values ('abc', 'abc') USING TTL %s" % 5)
        self.smart_sleep(start_time, 10)
        assert_row_count(self.session1, 'session', 1)

    def insert_few_rows(self, start, end, table_name, ttl=None):
        for i in range(start, end + 1):
            statement = 'INSERT INTO %s (key, col1, col2, col3) VALUES (%d, %d, %d, %d)' % (table_name, i, i, i, i)
            if ttl:
                statement = '{} USING TTL {}'.format(statement, ttl)
            self.session1.execute(statement)

    def execute_statement(self, action, ttl, start_key_value, end_key_value, table_name):
        # logger.debug('{action} rows {start_key_value}-{end_key_value} using TTL {ttl}'.format(**locals()))
        readble_start_time = self.format_float_time_to_readable()
        logger.debug('{action} rows with keys from {start_key_value} to {end_key_value} with TTL {ttl} started at '
                     '{readble_start_time}'.format(**locals()))
        # TODO: add UPDATE action
        if action == 'INSERT':
            self.insert_few_rows(start=start_key_value, end=end_key_value, ttl=ttl, table_name=table_name)
        execute_time = time.time()
        readble_execute_time = self.format_float_time_to_readable(execute_time)
        logger.debug('{} has been finished at {}'.format(action, readble_execute_time))
        return execute_time

    @pytest.mark.parametrize("strategies", argvalues=(
        ['LeveledCompactionStrategy', 'SizeTieredCompactionStrategy', 'TimeWindowCompactionStrategy'],
        enterprise_only_param('IncrementalCompactionStrategy')), ids=("oss", "enterprise"))
    def test_overlaped_rows_ttls(self, strategies):
        """ Test when different ttls are applyed  to the same rows
            Perform the test for different compaction strategies
        """

        self.prepare(nodes=4, rf=3)
        table_name = 'ttl_table'
        for strategy in strategies:
            logger.debug('================  Run with {} ==============='.format(strategy))
            drop_table(session=self.session1, table_name=table_name, if_exists=True)

            self.session1.execute('CREATE TABLE %s (key int, col1 int, col2 int, col3 int, PRIMARY KEY (key, col1)) '
                                  'WITH compaction = {\'class\': \'%s\'}' % (table_name, strategy))

            # logger.debug('Insert 20 rows with default TTL')
            rows = 20
            self.insert_few_rows(start=1, end=rows, table_name=table_name)
            assert_row_count(self.session1, 'ttl_table', rows)

            ttls = [13, 20, 25, 30]
            # steps: dictionary with test steps. Keys -it's TTL value
            # Update rows with key 5-10 with TTL 20
            ttl = ttls[1]
            steps = {
                ttl: {'expected_result': [[i] for i in range(1, 21) if i not in [5, 6, 7, 8, 10]],
                      'execute_time': self.execute_statement(action='INSERT', ttl=ttl, start_key_value=5, end_key_value=10, table_name=table_name)
                      }
            }

            # Update rows with key 9-13 with TTL 25
            ttl = ttls[2]
            steps[ttl] = {'expected_result': [[i] for i in range(1, 21) if i < 5 or i > 10],
                          'execute_time': self.execute_statement(action='INSERT', ttl=ttl, start_key_value=9, end_key_value=13, table_name=table_name)
                          }

            # Update rows with key 10-11 with TTL 13
            ttl = ttls[0]
            steps[ttl] = {'expected_result': [[i] for i in range(1, 21) if i != 10],
                          'execute_time': self.execute_statement(action='INSERT', ttl=ttl, start_key_value=10, end_key_value=11, table_name=table_name)
                          }

            # Update rows with key 11-15 with TTL 30
            ttl = ttls[3]
            steps[ttl] = {'expected_result': [[i] for i in range(1, 21) if i < 5 or i > 15],
                          'execute_time': self.execute_statement(action='INSERT', ttl=ttl, start_key_value=11, end_key_value=15, table_name=table_name)
                          }

            for ttl in ttls:
                logger.debug('*******Assert records with TTL {}'.format(ttl))
                self.smart_sleep_with_print(steps[ttl]['execute_time'], ttl+2)
                assert_all(session=self.session1, query='select key from {}'.format(table_name),
                           expected=steps[ttl]['expected_result'], cl=ConsistencyLevel.QUORUM, ignore_order=True)


@pytest.mark.dtest_full
class TestDistributedTTL(Tester):

    """ Test Time To Live Feature in a distributed environment """

    def prepare(self, default_time_to_live=None, options=None):
        if options:
            self.cluster.set_configuration_options(values=options)
        self.cluster.populate(2).start()
        [self.node1, self.node2] = self.cluster.nodelist()
        self.session1 = self.patient_cql_connection(self.node1)
        create_ks(self.session1, 'ks', 2)

        drop_table(session=self.session1, table_name='ttl_table', if_exists=True)
        query = """
            CREATE TABLE ttl_table (
                key int primary key,
                col1 int,
                col2 int,
                col3 int,
            )
        """
        if default_time_to_live:
            query += " WITH default_time_to_live = {};".format(default_time_to_live)

        self.session1.execute(query)

    def test_ttl_is_replicated(self):
        """
        Test that the ttl setting is replicated properly on all nodes
        """

        self.prepare(default_time_to_live=5)
        session1 = self.patient_exclusive_cql_connection(self.node1)
        session2 = self.patient_exclusive_cql_connection(self.node2)
        session1.execute("USE ks;")
        session2.execute("USE ks;")
        query = SimpleStatement(
            "INSERT INTO ttl_table (key, col1) VALUES (1, 1);",
            consistency_level=ConsistencyLevel.ALL
        )
        session1.execute(query)
        assert_all(
            session1,
            "SELECT * FROM ttl_table;",
            [[1, 1, None, None]],
            cl=ConsistencyLevel.ALL
        )
        ttl_session1 = session1.execute('SELECT ttl(col1) FROM ttl_table;')
        ttl_session2 = session2.execute('SELECT ttl(col1) FROM ttl_table;')

        # since the two queries are not executed simultaneously, the remaining
        # TTLs can differ by one second
        assert abs(ttl_session1[0][0] - ttl_session2[0][0]) <= 1

        time.sleep(7)

        assert_none(session1, "SELECT * FROM ttl_table;", cl=ConsistencyLevel.ALL)

    def test_ttl_is_respected_on_delayed_replication(self):
        """ Test that ttl is respected on delayed replication """

        self.prepare(options={'shadow_round_ms': 1000})
        logger.debug("Stopping node2")
        self.node2.stop()
        self.session1.execute("""
            INSERT INTO ttl_table (key, col1) VALUES (1, 1) USING TTL 5;
        """)
        self.session1.execute("""
            INSERT INTO ttl_table (key, col1) VALUES (2, 2) USING TTL 1000;
        """)
        assert_all(
            self.session1,
            "SELECT * FROM ttl_table;",
            [[1, 1, None, None], [2, 2, None, None]]
        )
        time.sleep(7)
        logger.debug("Stopping node1")
        self.node1.stop()
        logger.debug("Restarting node2")
        self.node2.start(wait_for_binary_proto=True)
        session2 = self.patient_exclusive_cql_connection(self.node2)
        session2.execute("USE ks;")
        logger.debug("Expecting empty ttl_table")
        assert_row_count(session2, 'ttl_table', 0)  # should be 0 since node1 is down, no replica yet
        logger.debug("Restarting node1")
        self.node1.start(wait_for_binary_proto=True)
        self.session1 = self.patient_exclusive_cql_connection(self.node1)
        self.session1.execute("USE ks;")
        self.node1.cleanup()

        logger.debug("Expecting row in ttl_table")
        assert_all(session2, "SELECT count(*) FROM ttl_table", [[1]], cl=ConsistencyLevel.ALL)
        assert_all(
            session2,
            "SELECT * FROM ttl_table;",
            [[2, 2, None, None]],
            cl=ConsistencyLevel.ALL
        )

        # Check that the TTL on both server are the same
        ttl_session1 = self.session1.execute('SELECT ttl(col1) FROM ttl_table;')
        ttl_session2 = session2.execute('SELECT ttl(col1) FROM ttl_table;')
        logger.debug("ttl_session1={} ttl_session2={}".format(ttl_session1, ttl_session2))
        assert abs(ttl_session1[0][0] - ttl_session2[0][0]) <= 1

    @pytest.mark.next_gating
    @pytest.mark.dtest_debug
    def test_ttl_is_respected_on_repair(self):
        """ Test that ttl is respected on repair """

        self.prepare()
        self.session1.execute("""
            ALTER KEYSPACE ks WITH REPLICATION =
            {'class' : 'SimpleStrategy', 'replication_factor' : 1};
        """)
        self.session1.execute("""
            INSERT INTO ttl_table (key, col1) VALUES (1, 1) USING TTL 5;
        """)
        self.session1.execute("""
            INSERT INTO ttl_table (key, col1) VALUES (2, 2) USING TTL 1000;
        """)

        assert_all(
            self.session1,
            "SELECT * FROM ttl_table;",
            [[1, 1, None, None], [2, 2, None, None]]
        )
        time.sleep(7)
        self.node1.stop()
        session2 = self.patient_exclusive_cql_connection(self.node2)
        session2.execute("USE ks;")
        assert_unavailable(session2.execute, "SELECT * FROM ttl_table;")
        self.node1.start(wait_for_binary_proto=True)
        self.session1 = self.patient_exclusive_cql_connection(self.node1)
        self.session1.execute("USE ks;")
        self.session1.execute("""
            ALTER KEYSPACE ks WITH REPLICATION =
            {'class' : 'SimpleStrategy', 'replication_factor' : 2};
        """)
        self.node1.repair(['ks'])
        ttl_start = time.time()
        ttl_session1 = self.session1.execute('SELECT ttl(col1) FROM ttl_table;')
        self.node1.stop()

        assert_row_count(session2, 'ttl_table', 1)
        assert_all(
            session2,
            "SELECT * FROM ttl_table;",
            [[2, 2, None, None]]
        )

        # Check that the TTL on both server are the same
        ttl_session2 = session2.execute('SELECT ttl(col1) FROM ttl_table;')
        ttl_session1 = ttl_session1[0][0] - (time.time() - ttl_start)
        assert_almost_equal(ttl_session1, ttl_session2[0][0], error=0.005)
