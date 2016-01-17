from cassandra import ConsistencyLevel
from cassandra.concurrent import execute_concurrent_with_args
from cassandra.query import SimpleStatement
from dtest import debug

def insert_c1c2(session, keys=None, n=None, consistency=ConsistencyLevel.QUORUM, c1_values=None, c2_values=None):
    if (keys is None and n is None) or (keys is not None and n is not None):
        raise ValueError("Expected exactly one of 'keys' or 'n' arguments to not be None; "
                         "got keys={keys}, n={n}".format(keys=keys, n=n))

    if n:
        keys = list(range(n))

    if c1_values is None:
        c1_values = ['value1'] * len(keys)

    if c2_values is None:
        c2_values = ['value2'] * len(keys)

    if len(c1_values) != len(c2_values) or len(c1_values) != len(keys):
        raise ValueError("Inconsistent 'c1/c2_values' contents. 'c1/c2_values' should be either a 'None' value or a list of the same length as a requested number of keys.")

    statement = session.prepare("INSERT INTO cf (key, c1, c2) VALUES (?, ?, ?)")
    statement.consistency_level = consistency

    execute_concurrent_with_args(session, statement,
        map(lambda x,y,z: ['k{}'.format(x),y,z], keys, c1_values, c2_values))

def check_c1c2_result_one(success, rows, tolerate_missing, must_be_missing, c1_value, c2_value):
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
    query  = 'SELECT c1, c2 FROM cf WHERE key=?'
    pquery = session.prepare(query)
    pquery.consistency_level = consistency

    results = execute_concurrent_with_args(session, pquery, map(lambda x: ['k{}'.format(x)], keys))

    map(lambda (success, result), c1, c2:
            check_c1c2_result_one(success, result, tolerate_missing, must_be_missing, c1, c2),
        results, c1_values, c2_values)



