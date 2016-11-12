import os
import unittest
from cassandra import ConsistencyLevel
from cassandra.concurrent import execute_concurrent_with_args, execute_concurrent
from cassandra.query import SimpleStatement
from ccmlib import common


def build_insert_params(keys, n, c1_values, c2_values):
    if (len(keys) == 0 and n is None) or (len(keys) != 0 and n is not None):
        raise ValueError("Expected exactly one of 'keys' or 'n' arguments to not be None; "
                         "got keys={keys}, n={n}".format(keys=keys, n=n))

    if n:
        keys.extend(list(range(n)))

    if len(c1_values) == 0:
        c1_values.extend(['value1'] * len(keys))

    if len(c2_values) == 0:
        c2_values.extend(['value2'] * len(keys))

    if len(c1_values) != len(c2_values) or len(c1_values) != len(keys):
        raise ValueError("Inconsistent 'c1/c2_values' contents. 'c1/c2_values' should be either a '[]' value or a list of the same length as a requested number of keys.")


def insert_c1c2(session, keys=None, n=None, consistency=ConsistencyLevel.QUORUM, c1_values=None, c2_values=None, ks='ks', cf='cf'):
    if keys is None:
        keys = []

    if c1_values is None:
        c1_values = []

    if c2_values is None:
        c2_values = []

    build_insert_params(keys, n, c1_values, c2_values)

    statement = session.prepare("INSERT INTO {}.{} (key, c1, c2) VALUES (?, ?, ?)".format(ks, cf))
    statement.consistency_level = consistency

    execute_concurrent_with_args(session, statement,
                                 map(lambda x, y, z: ['k{}'.format(x), y, z], keys, c1_values, c2_values))


def insert_c1c2_no_prepared(session, keys=None, n=None, consistency=ConsistencyLevel.QUORUM, c1_values=None, c2_values=None, ks='ks', cf='cf'):
    if keys is None:
        keys = []

    if c1_values is None:
        c1_values = []

    if c2_values is None:
        c2_values = []

    build_insert_params(keys, n, c1_values, c2_values)

    execute_concurrent(session, map(lambda x, y, z: (SimpleStatement('INSERT INTO {}.{} (key, c1, c2) VALUES (\'{}\', \'{}\', \'{}\')'.format(ks, cf, 'k{}'.format(x), y, z),
                                                                     consistency_level=consistency), None), keys, c1_values, c2_values))


def insert_c1cn(session, keys=None, consistency=ConsistencyLevel.QUORUM, nr_columns=5, column_size=None, ks='ks', cf='cf'):
    if keys is None:
        keys = []

    cql_str = "INSERT INTO {}.{} (key, ".format(ks, cf)
    for nr in xrange(1, nr_columns + 1):
        if nr != nr_columns:
            cql_str += 'c{}, '.format(nr)
        else:
            cql_str += 'c{}) VALUES (?, '.format(nr)
    for nr in xrange(1, nr_columns + 1):
        if nr != nr_columns:
            cql_str += '?, '
        else:
            cql_str += '?) '

    statement = session.prepare(cql_str)
    statement.consistency_level = consistency

    # build column values for c1 to cn
    col_data = []
    for nr in xrange(1, nr_columns + 1):
        'x' * column_size
        if column_size:
            col_data.append('x' * column_size)
        else:
            col_data.append('column_data_{}'.format(nr))

    # build data for each row, including key and columns
    kv = []
    for key in keys:
        data = ['k{}'.format(key)]
        data.extend(col_data)
        kv.append(data)

    execute_concurrent_with_args(session, statement, kv)


def check_c1c2_result_one(success, rows, tolerate_missing, must_be_missing, c1_value, c2_value):
    rows = list(rows)
    if not success:
        assert False, "Query failed {}".format(rows)

    if not tolerate_missing:
        assert len(rows) == 1
        res = rows[0]
        assert len(res) == 2 and res[0] == c1_value and res[1] == c2_value, res

    if must_be_missing:
        assert len(rows) == 0


def query_c1c2(session, key, consistency=ConsistencyLevel.QUORUM, tolerate_missing=False, must_be_missing=False, c1_value='value1', c2_value='value2'):
    query = SimpleStatement('SELECT c1, c2 FROM cf WHERE key=\'k%d\'' % key, consistency_level=consistency)
    rows = list(session.execute(query))
    check_c1c2_result_one(True, rows, tolerate_missing, must_be_missing, c1_value, c2_value)


def query_c1c2_concurrent(session, keys, consistency=ConsistencyLevel.QUORUM, tolerate_missing=False, must_be_missing=False, c1_values=None, c2_values=None):
    if c1_values is None:
        c1_values = ['value1'] * len(keys)

    if c2_values is None:
        c2_values = ['value2'] * len(keys)

    if len(c1_values) != len(c2_values) or len(c1_values) != len(keys):
        raise ValueError("Inconsistent 'c1/c2_values' contents. 'c1/c2_values' should be either a 'None' value or a list of the same length as a requested number of keys.")

    # prepare a query statement
    query = 'SELECT c1, c2 FROM cf WHERE key=?'
    pquery = session.prepare(query)
    pquery.consistency_level = consistency

    results = execute_concurrent_with_args(session, pquery, map(lambda x: ['k{}'.format(x)], keys))

    map(lambda (success, result), c1, c2:
        check_c1c2_result_one(success, result, tolerate_missing, must_be_missing, c1, c2),
        results, c1_values, c2_values)


def scylla_mode(modes):
    """
        Run the decorated tests if they are executed on correct mode
        @scylla_mode('release') - will run tests only if mode is release
        @scylla_mode('debug') - will run tests only if mode is debug
   ."""
    NO_SKIP = os.environ.get('SKIP', '').lower() in ('no', 'false')
    cdir = os.environ.get('CASSANDRA_DIR')
    idir, mode = common.scylla_extract_install_dir_and_mode(cdir)
    return unittest.skipIf(common.isScylla(cdir) and not NO_SKIP and modes.find(mode) == -1, 'Test disabled for scylla %s' % mode)
