import re
from cassandra import InvalidRequest, Unavailable, ConsistencyLevel, WriteFailure, WriteTimeout, ReadFailure, ReadTimeout
from cassandra.query import SimpleStatement
from tools import rows_to_list, run_query_with_data_processing


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


def assert_all(session, query, expected, cl=ConsistencyLevel.ONE, ignore_order=False):
    simple_query = SimpleStatement(query, consistency_level=cl)
    res = session.execute(simple_query)
    list_res = rows_to_list(res)
    if ignore_order:
        expected = sorted(expected)
        list_res = sorted(list_res)
    assert list_res == expected, "Expected %s from %s, but got %s" % (expected, query, list_res)


def assert_almost_equal(*args, **kwargs):
    try:
        error = kwargs['error']
    except KeyError:
        error = 0.16

    vmax = max(args)
    vmin = min(args)
    assert vmin > vmax * (1.0 - error) or vmin == vmax, "values not within %.2f%% of the max: %s" % (error * 100, args)


def assert_row_count(session, table_name, expected):
    """ Function to validate the row count expected in table_name """

    query = "SELECT count(*) FROM {};".format(table_name)
    res = list(session.execute(query))
    count = res[0][0]
    assert count == expected, "Expected a row count of {} in table '{}', but got {}".format(
        expected, table_name, count
    )


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
    assert exp_res == act_res, "Expected %s, but got %s" % (exp_res, act_res)
