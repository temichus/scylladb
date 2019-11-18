import re
from cassandra import InvalidRequest, Unavailable, ConsistencyLevel, WriteFailure, WriteTimeout, ReadFailure, ReadTimeout
from cassandra.query import SimpleStatement
from tools import rows_to_list, run_query_with_data_processing
from dtest import retry_with_func_attempts


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


def assert_invalid_case_insensitive_matching(session, query, matching=None, expected=InvalidRequest):
    try:
        res = session.execute(query)
        assert False, "Expecting query to be invalid: got %s" % res
    except AssertionError as e:
        raise e
    except expected as e:
        msg = str(e).upper()
        if matching is not None:
            assert re.search(matching.upper(), msg), "Error message does not contain " + \
                matching + " (error = " + msg + ")"


def assert_invalid(session, query, matching=None, expected=InvalidRequest):
    try:
        res = session.execute(query)
        assert False, "Expecting query to be invalid: got an ok response %s" % str(res)
    except AssertionError as e:
        raise e
    except expected as e:
        msg = str(e)
        if matching is not None:
            assert re.search(matching, msg), "Error message does not contain " + matching + " (error = " + msg + ")"


def _get_list_res(session, query, cl, ignore_order=False, result_as_string=False, timeout=None):
    simple_query = SimpleStatement(query, consistency_level=cl)
    if timeout is not None:
        res = session.execute(simple_query, timeout=timeout)
    else:
        res = session.execute(simple_query)
    list_res = rows_to_list(res)
    if ignore_order:
        list_res = sorted(list_res)
    if result_as_string:
        list_res = str(list_res)
    return list_res


@retry_with_func_attempts
def assert_one(session, query, expected, cl=ConsistencyLevel.ONE, timeout=60, num_attempts=1):
    """
    :param num_attempts: defines how many time try to assert data in case failure. Used in retry_with_func_attempts decorator
    """
    list_res = _get_list_res(session, query, cl, timeout=timeout)
    assert list_res == [expected], "Expected %s from %s, but got %s" % ([expected], query, list_res)


@retry_with_func_attempts
def assert_one_prepared(session, stmt, expected, parameters, cl=ConsistencyLevel.ONE, timeout=60, num_attempts=1):
    res = session.execute(stmt, parameters=parameters, timeout=timeout)
    list_res = rows_to_list(res)
    assert list_res == [expected], 'Expected %s from "%s", but got %s' % ([expected], stmt.query_string, list_res)


@retry_with_func_attempts
def assert_none(session, query, cl=ConsistencyLevel.ONE, num_attempts=1):
    """
    :param num_attempts: defines how many time try to assert data in case failure. Used in retry_with_func_attempts decorator
    """
    list_res = _get_list_res(session, query, cl)
    assert list_res == [], "Expected nothing from %s, but got %s" % (query, list_res)


@retry_with_func_attempts
def assert_all(session, query, expected, cl=ConsistencyLevel.ONE, ignore_order=False, num_attempts=1,
               result_as_string=False, print_result_on_failure=True, timeout=None):
    """
    :param num_attempts: defines how many time try to assert data in case failure. Used in retry_with_func_attempts decorator
    """
    list_res = _get_list_res(session, query, cl, ignore_order, result_as_string, timeout=timeout)
    if ignore_order:
        expected = sorted(expected)
    error = f"Expected {expected} from {query}, but got {list_res}" if print_result_on_failure \
        else f'Actual result ({len(list_res)} rows) is not as expected ({len(expected)} rows). Query: {query}'
    assert list_res == expected, error


def assert_all_or_none(session, query, expected, cl=ConsistencyLevel.ONE, ignore_order=False, num_attempts=1, result_as_string=False, timeout=None):
    """
    :param num_attempts: defines how many time try to assert data in case failure. Used in retry_with_func_attempts decorator
    """
    list_res = _get_list_res(session, query, cl, ignore_order, result_as_string, timeout=timeout)
    if ignore_order:
        expected = sorted(expected)
    assert (list_res == expected or list_res == []), "Expected %s or [] from %s, but got %s" % (expected, query, list_res)


def assert_almost_equal(*args, **kwargs):
    try:
        error = kwargs['error']
    except KeyError:
        error = 0.16

    vmax = max(args)
    vmin = min(args)
    assert vmin > vmax * (1.0 - error) or vmin == vmax, "values not within %.2f%% of the max: %s" % (error * 100, args)


@retry_with_func_attempts
def assert_row_count(session, table_name, expected, consistency_level=ConsistencyLevel.ONE, num_attempts=1, timeout=None):
    """
    Function to validate the row count expected in table_name
    :param num_attempts: defines how many time try to assert data in case failure. Used in retry_with_func_attempts decorator
    """

    query = "SELECT count(*) FROM {}".format(table_name)
    count = run_query_with_data_processing(session, query, consistency_level=consistency_level, session_timeout=timeout)
    if isinstance(count, list):
        count = count[0][0]
    assert count == expected, "Expected a row count of {} in table '{}', but got {}".format(
        expected, table_name, count)


@retry_with_func_attempts
def assert_row_count_in_select(session, query, num_rows_expected, consistency_level=ConsistencyLevel.ONE, num_attempts=1, timeout=None):
    """
    Function to validate the row count are returned by select
    :param num_attempts: defines how many time try to assert data in case failure. Used in retry_with_func_attempts decorator
    """
    count = len(_get_list_res(session, query, consistency_level, timeout=timeout))
    assert count == num_rows_expected, "Expected a row count of {} in query \"{}\", but got {}".format(
        num_rows_expected, query, count)


@retry_with_func_attempts
def assert_row_count_in_select_less(session, query, max_rows_expected, consistency_level=ConsistencyLevel.ONE,
                                    num_attempts=1, timeout=None):
    """
    Function to validate the row count are returned by select
    :param num_attempts: defines how many time try to assert data in case failure. Used in retry_with_func_attempts decorator
    """
    count = len(_get_list_res(session, query, consistency_level, timeout=timeout))
    assert count < max_rows_expected, "Expected a row count < of {} in query \"{}\", but got {}".format(
        max_rows_expected, query, count)


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


@retry_with_func_attempts
def assert_two_queries_equal(session1, query1, session2, query2, consistency_level=ConsistencyLevel.ONE, session_timeout=120,
                             group=False, groupby_column1=None, groupby_column2=None, restrict_column1=None,
                             restrict_column2=None, restrict_value1=None, restrict_value2=None, num_attempts=1):
    exp_res = run_query_with_data_processing(session1, query1, group=group, consistency_level=consistency_level, session_timeout=session_timeout,
                                             groupby_column=groupby_column1, restrict_column=restrict_column1, restrict_value=restrict_value1)
    act_res = run_query_with_data_processing(session2, query2, group=group, consistency_level=consistency_level, session_timeout=session_timeout,
                                             groupby_column=groupby_column2, restrict_column=restrict_column2, restrict_value=restrict_value2)
    assert exp_res == act_res, "Expected %s, but got %s. Query1: %s; Query2: %s" % (exp_res, act_res, query1, query2)


@retry_with_func_attempts
def assert_two_queries_equal_ignore_order(session1, query1, session2, query2, consistency_level=ConsistencyLevel.ONE, session_timeout=120, num_attempts=1):
    expected = rows_to_list(session1.execute(query1))
    assert_all(session2, query2, expected, consistency_level, ignore_order=True)


def assert_expected_error(func, expected_error, args, kwargs):
    try:
        func(*args, **kwargs)
        assert False, 'Expected failure, but function was succeeded'
    except AssertionError:
        raise
    except Exception as e:
        if expected_error in str(e):
            assert True
        else:
            raise


def assert_equal_more_with_deviation(actual, expect, deviation_perc):
    deviation_high = (expect * (100 + deviation_perc))/100
    assert expect <= actual < deviation_high, 'Expect that result will be between %d and %d, but received ' \
        '%d' % (expect, deviation_high, actual)


def assert_less_equal_lists(actual_list, expected_list, msg=None):
    standardMsg = msg or '{actual_list} not less than or equal to {expected_list}'.format(**locals())
    assert set(actual_list) <= set(expected_list), standardMsg
