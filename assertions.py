import re
from cassandra import InvalidRequest, Unavailable, ConsistencyLevel, WriteFailure, WriteTimeout, ReadFailure, ReadTimeout
from cassandra.query import SimpleStatement
from tools import rows_to_list, run_query_with_data_processing
import time

def assert_unavailable(fun, *args):
    try:
        if len(args) == 0:
            fun(None)
        else:
            fun(*args)
    except (Unavailable, WriteTimeout, WriteFailure, ReadTimeout, ReadFailure) as e:
        pass
    except Exception as e:
        assert False, "Expecting unavailable exception, got: " + str(e)
    else:
        assert False, "Expecting unavailable exception but no exception was raised"


def assert_invalid(session, query, matching=None, expected=InvalidRequest):
    try:
        res = session.execute(query)
        assert False, "Expecting query to be invalid: got %s" % res
    except AssertionError as e:
        raise e
    except expected as e:
        msg = str(e)
        if matching is not None:
            assert re.search(matching, msg), "Error message does not contain " + matching + " (error = " + msg + ")"


def assert_one(session, query, expected, cl=ConsistencyLevel.ONE):
    simple_query = SimpleStatement(query, consistency_level=cl)
    res = session.execute(simple_query)
    list_res = rows_to_list(res)
    assert list_res == [expected], "Expected %s from %s, but got %s" % ([expected], query, list_res)


def assert_none(session, query, cl=ConsistencyLevel.ONE):
    simple_query = SimpleStatement(query, consistency_level=cl)
    res = session.execute(simple_query)
    list_res = rows_to_list(res)
    assert list_res == [], "Expected nothing from %s, but got %s" % (query, list_res)


def assert_all(session, query, expected, cl=ConsistencyLevel.ONE, ignore_order=False, attempts=1):
    # Parameter attempts is added because of materialized views insertions performs asynchronously.
    # We sleep in proportion to the attempt, but always retry until it succeeds or we exceed the maximum number of attempts.
    simple_query = SimpleStatement(query, consistency_level=cl)
    for _ in xrange(attempts):
        res = session.execute(simple_query)
        list_res = rows_to_list(res)
        if ignore_order:
            expected = sorted(expected)
            list_res = sorted(list_res)
        if list_res == expected:
            break
        time.sleep(1)
    assert list_res == expected, "Expected %s from %s, but got %s" % (expected, query, list_res)


def assert_almost_equal(*args, **kwargs):
    try:
        error = kwargs['error']
    except KeyError:
        error = 0.16

    vmax = max(args)
    vmin = min(args)
    assert vmin > vmax * (1.0 - error) or vmin == vmax, "values not within %.2f%% of the max: %s" % (error * 100, args)


def assert_row_count(session, table_name, expected, consistency_level=ConsistencyLevel.ONE, attempt=1):
    """ Function to validate the row count expected in table_name """

    count = None
    query = "SELECT count(*) FROM {}".format(table_name)
    for _ in xrange(attempt):
        count = run_query_with_data_processing(session, query, consistency_level=consistency_level)
        if isinstance(count, list):
            count = count[0][0]
        if count == expected:
            break
        time.sleep(10)

    assert count == expected, "Expected a row count of {} in table '{}', but got {}".format(
            expected, table_name, count)

def assert_row_count_in_select(session, query, expected, consistency_level=ConsistencyLevel.ONE, attempt=1):
    """ Function to validate the row count are returned by select """

    count = None
    simple_query = SimpleStatement(query, consistency_level=consistency_level)
    for _ in xrange(attempt):
        res = session.execute(simple_query)
        count = len(rows_to_list(res))
        if count == expected:
            break
        time.sleep(10)

    assert count == expected, "Expected a row count of {} in query \"{}\", but got {}".format(
            expected, query, count)

def assert_row_count_from_every_node(session, table_name, expected, nodes_list, attempt=1):
    """ Function to validate the row count expected in table_name running from every node"""

    failed_nodes = []
    query = "SELECT count(*) FROM {0}.{1};".format(session.keyspace, table_name)
    for _ in xrange(attempt):
        failed_nodes = []
        for node in nodes_list:
            if node.status != 'UP':
                continue
            res = node.run_cqlsh(query, return_output=True)
            count = 0
            try:
                count = res[0].split('\n')[3].lstrip()
                count = int(count)
            except TypeError:
                failed_nodes.append('Query "{2}" run failed. Node: {0}, error message: {1}'.format
                                    (node.name, count, query))
            except Exception as e:
                failed_nodes.append('Query "{2}" run failed. Node: {0}, error message: {1}'.format
                                    (node.name, e.message, query))

            if count != expected:
                failed_nodes.append('Node: {0}, actual count: {1}'.format(node.name, count))
        if not failed_nodes:
            break
        time.sleep(10)

    if failed_nodes:
        assert not failed_nodes, 'Expected a row count of {0} in table "{1}", but got:\n {2}'.format \
                            (expected, table_name,'\n '.join(msg for msg in failed_nodes))

def assert_crc_check_chance_equal(session, table, expected, ks="ks", view=False):
    """
    driver still doesn't support top-level crc_check_chance property,
    so let's fetch directly from system_schema
    """
    if view:
        assert_one(session,
                   "SELECT crc_check_chance from system_schema.views WHERE keyspace_name = '{ks}' AND "
                   "view_name = '{table}';".format(table=table, ks=ks),
                   [expected])
    else:
        assert_one(session,
                   "SELECT crc_check_chance from system_schema.tables WHERE keyspace_name = '{ks}' AND "
                   "table_name = '{table}';".format(table=table, ks=ks),
                   [expected])

def assert_two_queries_equal(session1, query1, session2, query2, consistency_level=ConsistencyLevel.ONE, session_timeout=120,
                             group=False, groupby_column1=None, groupby_column2=None, restrict_column1=None,
                             restrict_column2=None, restrict_value1=None, restrict_value2=None):
    exp_res = run_query_with_data_processing(session1, query1, group=group, consistency_level=consistency_level, session_timeout=session_timeout,
                                             groupby_column=groupby_column1, restrict_column=restrict_column1, restrict_value=restrict_value1)
    act_res = run_query_with_data_processing(session2, query2, group=group, consistency_level=consistency_level, session_timeout=session_timeout,
                                             groupby_column=groupby_column2, restrict_column=restrict_column2, restrict_value=restrict_value2)
    assert exp_res == act_res, "Expected %s, but got %s. Query1: %s; Query2: %s" % (exp_res, act_res, query1, query2)

def assert_two_queries_equal_ignore_order(session1, query1, session2, query2, consistency_level=ConsistencyLevel.ONE, session_timeout=120):
    expected = rows_to_list(session1.execute(query1))
    assert_all(session2, query2, expected, consistency_level, ignore_order=True)

def assert_expected_error(func, expected_error, args, kwargs):
    try:
        func(*args, **kwargs)
        assert False, 'Expected failure, but function was succeeded'
    except AssertionError:
        raise
    except Exception as e:
        if expected_error in e.message:
            assert True
        else:
            raise