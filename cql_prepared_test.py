from dtest import Tester, retry_with_func_attempts, debug
from nose.plugins.attrib import attr
from cassandra import ConsistencyLevel
from cassandra.util import Time, Date, uuid_from_time, SortedSet
from tools import rows_to_list
from assertions import assert_one
from unittest import skip

from decimal import Decimal
from datetime import datetime, date

import time
import uuid

@retry_with_func_attempts
def assert_one_prepared(session, stmt, expected, parameters, cl=ConsistencyLevel.ONE, timeout=60, num_attempts=1):
    res = session.execute(stmt, parameters=parameters, timeout=timeout)
    list_res = rows_to_list(res)
    assert list_res == [expected], 'Expected %s from "%s", but got %s' % ([expected], stmt.query_string, list_res)

def prepare_statement(session, query, cl=ConsistencyLevel.ONE):
    debug('Prepairing statement: {}'.format(query))
    res = session.prepare(query)
    res.consistency_level = cl
    return res

@attr('single_node')
class TestCQL(Tester):

    def prepare(self, options={}):
        cluster = self.cluster

        if options:
            cluster.set_configuration_options(values=options)

        cluster.populate(1).start()
        node1 = cluster.nodelist()[0]
        time.sleep(0.2)

        session = self.patient_cql_connection(node1)
        self.create_ks(session, 'ks', 1)
        return session

    def batch_preparation_test(self):
        """ Test preparation of batch statement (#4202) """
        session = self.prepare()

        session.execute("""
            CREATE TABLE cf (
                k varchar PRIMARY KEY,
                c int,
            )
        """)

        query = "BEGIN BATCH INSERT INTO cf (k, c) VALUES (?, ?); APPLY BATCH"
        pq = session.prepare(query)

        session.execute(pq, ['foo', 4])

    def null_value_tuple_boolean_test(self):
        session = self.prepare(options={'experimental_features': ['lwt']})

        table_name = 'null_value_tuple_boolean_test'

        session.execute('''
            CREATE TABLE IF NOT EXISTS {table_name} (
                k int PRIMARY KEY,
                test boolean
            )
        '''.format(table_name=table_name))

        insert_stmt = prepare_statement(session, '''
            INSERT INTO {table_name} (k, test) VALUES(?, ?)
        '''.format(table_name=table_name))
        session.execute(insert_stmt, [0, None])

        update_stmt = prepare_statement(session, '''
            UPDATE {table_name} SET test=:new_value WHERE k=0 IF test in :v
        '''.format(table_name=table_name))

        assert_one_prepared(session, update_stmt, [True, None], {'new_value': False, 'v': (None,)})
        assert_one(session, "SELECT * FROM {table_name}".format(table_name=table_name), [0, False])

    @skip('Failing for scylla, skip for now until investigated and fixed')
    def null_value_tuple_double_test(self):
        session = self.prepare(options={'experimental_features': ['lwt']})

        table_name = 'null_value_tuple_double_test'

        session.execute('''
            CREATE TABLE IF NOT EXISTS {table_name} (
                k int PRIMARY KEY,
                test double
            )
        '''.format(table_name=table_name))

        insert_stmt = prepare_statement(session, '''
            INSERT INTO {table_name} (k, test) VALUES(?, ?)
        '''.format(table_name=table_name))
        session.execute(insert_stmt, [0, None])

        update_stmt = prepare_statement(session, '''
            UPDATE {table_name} SET test=:new_value WHERE k=0 IF test in :v
        '''.format(table_name=table_name))

        assert_one_prepared(session, update_stmt, [True, None], {'new_value': 1.0, 'v': (None,)})
        assert_one(session, "SELECT * FROM {table_name}".format(table_name=table_name), [0, 1.0])

    @skip('fails for Scylla, need to investigate')
    def null_value_tuple_uuid_test(self):
        session = self.prepare(options={'experimental_features': ['lwt']})

        table_name = 'null_value_tuple_uuid_test'

        session.execute('''
            CREATE TABLE IF NOT EXISTS {table_name} (
                k int PRIMARY KEY,
                test uuid
            )
        '''.format(table_name=table_name))

        insert_stmt = prepare_statement(session, '''
            INSERT INTO {table_name} (k, test) VALUES(?, ?)
        '''.format(table_name=table_name))
        session.execute(insert_stmt, [0, None])

        update_stmt = prepare_statement(session, '''
            UPDATE {table_name} SET test=:new_value WHERE k=0 IF test in :v
        '''.format(table_name=table_name))

        new_value = uuid.uuid4()
        assert_one_prepared(session, update_stmt, [True, None], {'new_value': new_value, 'v': (None,)})
        assert_one(session, "SELECT * FROM {table_name}".format(table_name=table_name), [0, new_value])

    def _lwt_create_update_test_table(self, session, column_type, test_data={}, table_name=None):
        '''Prepare table for test: create table and populate with test data'''

        if not table_name:
            # check if we are given a collection type
            sanitized_column_type = column_type if '<' not in column_type else \
                column_type.replace('<', '_').replace('>', '_')
            table_name = sanitized_column_type + '_update_test_table'
        column_name = 'value'

        session.execute('''
            CREATE TABLE IF NOT EXISTS {table_name} (
                k int PRIMARY KEY,
                {column_name} {column_type}
            )
        '''.format(table_name=table_name, column_name=column_name, column_type=column_type))

        insert_stmt = session.prepare('''
            INSERT INTO {table_name} (k, {column_name}) VALUES (?, ?)
        '''.format(table_name=table_name, column_name=column_name))

        for k, v in test_data:
            session.execute(insert_stmt, [k, v])

        return (table_name, column_name)

    def _lwt_execute_single_type_update_test(self, session, column_type, test_params):
        debug('Executing a single LWT Update test for type {}'.format(column_type))

        test_cases = test_params['test_cases']
        default_upd_v = test_params['default_update_value']
        raw_init_values = []
        args_per_test_case = []

        is_tuple = 'collection_type' in test_params and test_params['collection_type'] == 'tuple'

        for entry in test_cases:
            init_val = entry['init_val']
            upd_v = default_upd_v if 'update_value' not in entry else entry['update_value']

            if is_tuple:
                if isinstance(init_val, list) or isinstance(init_val, SortedSet):
                    init_val = tuple(init_val)
                if isinstance(upd_v, list):
                    upd_v = tuple(upd_v)

            for pattern in entry['update_patterns']:
                if isinstance(pattern, dict) and 'coll_type_filter' in pattern \
                    and 'collection_type' in test_params and \
                        test_params['collection_type'] not in pattern['coll_type_filter']:
                    continue
                raw_init_values.append(init_val)
                args_per_test_case.append((init_val, upd_v, pattern))

        table_name, column_name = self._lwt_create_update_test_table(session,
            column_type,
            zip(range(0, len(raw_init_values)), raw_init_values))

        for key, entry in zip(range(0, len(args_per_test_case)), args_per_test_case):
            init_val, upd_v, pattern_entry = entry

            query_args = {'upd_v': upd_v}

            if not isinstance(pattern_entry, dict):
                update_pattern = pattern_entry
                query_args.update({'v': init_val})
            else:
                update_pattern = pattern_entry['p']
                # filter out pattern string and supply the remaining keys as arguments to the query
                pattern_entry = {k: v for k, v in pattern_entry.items() if k != 'p'}
                query_args.update(pattern_entry)

            update_query = 'UPDATE {table_name} SET {column_name}=:upd_v WHERE k={id} IF {update_pattern}'.format(
                table_name=table_name, column_name=column_name, id=key, update_pattern=update_pattern
            )
            stmt = prepare_statement(session, update_query)

            debug('Asserting results from two consecutive identical CAS statements with the following query args: {}'.format(query_args))
            assert_one_prepared(session, stmt, [True, init_val], query_args)
            assert_one_prepared(session, stmt, [False, upd_v], query_args)

    def lwt_update_prepared_test(self):

        def standard_test_case(init_val):
            return {'init_val': init_val, 'update_patterns': [
                    'value=:v',
                    'value in (:v)',
                    {'p': 'value in :v', 'v': (init_val,)}
                ]
            }

        # TODO: tests fail: 'value in :v' (null) for float, double, uuid, timeuuid
        PRIMITIVE_TYPES_MAP = {
            'boolean': {
                'test_cases': [
                    standard_test_case(None),
                    standard_test_case(False)
                ],
                'default_update_value': True
            },
            'blob': {
                'test_cases': [
                    standard_test_case(None),
                    standard_test_case(b''),
                    standard_test_case(b'\x00\x00\x00\x00')
                ],
                'default_update_value': b'\x00\x00\x00\x01'
            },
            'ascii': {
                'test_cases': [
                    standard_test_case(None),
                    standard_test_case(''),
                    standard_test_case('abc')
                ],
                'default_update_value': 'def'
            },
            'decimal': {
                'test_cases': [
                    standard_test_case(None),
                    standard_test_case(Decimal('1.2349823094823948209384209348'))
                ],
                'default_update_value': Decimal('2.3495083459083095483409534534')
            },
            'double': {
                'test_cases': [
                    {'init_val': None, 'update_patterns': ['value=:v', 'value in (:v)']},
                    standard_test_case(0.0),
                    standard_test_case(2.2250738585072014e-308),
                    standard_test_case(1.7976931348623157e+308),
                    standard_test_case(-1.7976931348623157e+308)
                ],
                'default_update_value': 1.0
            },
            'float': {
                'test_cases': [
                    {'init_val': None, 'update_patterns': ['value=:v', 'value in (:v)']},
                    standard_test_case(0.0),
                    standard_test_case(1.1754943508222875e-38),
                    standard_test_case(3.4028234663852886e+38),
                    standard_test_case(-3.4028234663852886e+38)
                ],
                'default_update_value': 1.0
            },
            'text': {
                'test_cases': [
                    standard_test_case(None),
                    standard_test_case(''),
                    standard_test_case('abc')
                ],
                'default_update_value': 'def'
            },
            'varchar': {
                'test_cases': [
                    standard_test_case(None),
                    standard_test_case(''),
                    standard_test_case('abc')
                ],
                'default_update_value': 'def'
            },
            'bigint': {
                'test_cases': [
                    standard_test_case(None),
                    standard_test_case(0),
                    standard_test_case(2**63 - 1),
                    standard_test_case(-2**63)
                ],
                'default_update_value': 1
            },
            'int': {
                'test_cases': [
                    standard_test_case(None),
                    standard_test_case(0),
                    standard_test_case(2**31 - 1),
                    standard_test_case(-2**31)
                ],
                'default_update_value': 1
            },
            'smallint': {
                'test_cases': [
                    standard_test_case(None),
                    standard_test_case(0),
                    standard_test_case(2**15 - 1),
                    standard_test_case(-2**15)
                ],
                'default_update_value': 1
            },
            'tinyint': {
                'test_cases': [
                    standard_test_case(None),
                    standard_test_case(0),
                    standard_test_case(2**7 - 1),
                    standard_test_case(-2**7)
                ],
                'default_update_value': 1
            },
            'varint': {
                'test_cases': [
                    standard_test_case(None),
                    standard_test_case(0),
                    standard_test_case(2**128),
                    standard_test_case(-2**128)
                ],
                'default_update_value': 1
            },
            'timestamp': {
                'test_cases': [
                    standard_test_case(None),
                    standard_test_case(datetime(1970, 1, 1, 0, 0)),
                    standard_test_case(datetime.strptime('2020-01-02 14:13:12.001', "%Y-%m-%d %H:%M:%S.%f"))
                ],
                'default_update_value': datetime.strptime('2021-02-03 15:14:13.002', "%Y-%m-%d %H:%M:%S.%f")
            },
            'date': {
                'test_cases': [
                    standard_test_case(None),
                    standard_test_case(Date(0)),
                    standard_test_case(Date('2020-1-2'))
                ],
                'default_update_value': Date('2021-2-3')
            },
            'time': {
                'test_cases': [
                    standard_test_case(None),
                    standard_test_case(Time(0)),
                    standard_test_case(Time('12:13:14.001'))
                ],
                'default_update_value': Time('13:14:15.002')
            },
            'timeuuid': {
                'test_cases': [
                    {'init_val': None, 'update_patterns': ['value=:v', 'value in (:v)']},
                    standard_test_case(uuid_from_time(datetime(2020, 1, 2, 3, 4, 5, 0)))
                ],
                'default_update_value': uuid_from_time(datetime(2021, 2, 3, 4, 5, 6, 1))
            },
            'uuid': {
                'test_cases': [
                    {'init_val': None, 'update_patterns': ['value=:v', 'value in (:v)']},
                    standard_test_case(uuid.UUID(bytes=b'\x00' * 16)),
                    standard_test_case(uuid.uuid4())
                ],
                'default_update_value': uuid.uuid4()
            }
        }

        session = self.prepare(options={'experimental_features': ['lwt']})

        for column_type, test_data in PRIMITIVE_TYPES_MAP.items():
            self._lwt_execute_single_type_update_test(session, column_type, test_data)

    @staticmethod
    def _build_collection_typename(column_type, is_frozen, collection_type):
        ret = "{collection_type}<{column_type}>".format(collection_type=collection_type, column_type=column_type)
        if is_frozen:
            ret = 'frozen<' + ret + '>'
        return ret

    def lwt_update_prepared_listlike_and_tuples_test(self):

        def standard_test_case(init_val):
            return {'init_val': [init_val], 'update_patterns': [
                    'value=:v',
                    'value in (:v)',
                    {'p': 'value in :v', 'v': [[init_val]]},
                    # Set and tuple columns don't support indexing by key in C* and Scylla, limit these tests to lists only
                    {'p': 'value[:i]=:v', 'i': 0, 'v': init_val, 'coll_type_filter': ['list']},
                    {'p': 'value[:i] in (:v)', 'i': 0, 'v': init_val, 'coll_type_filter': ['list']},
                    {'p': 'value[:i] in :v', 'i': 0, 'v': [init_val], 'coll_type_filter': ['list']}
                ]
            }

        # TODO: null values testing
        PRIMITIVE_TYPES_MAP = {
            'boolean': {
                'test_cases': [
                    standard_test_case(False)
                ],
                'default_update_value': [True]
            },
            'blob': {
                'test_cases': [
                    standard_test_case(b''),
                    standard_test_case(b'\x00\x00\x00\x00')
                ],
                'default_update_value': [b'\x00\x00\x00\x01']
            },
            'ascii': {
                'test_cases': [
                    standard_test_case(''),
                    standard_test_case('abc')
                ],
                'default_update_value': ['def']
            },
            'decimal': {
                'test_cases': [
                    standard_test_case(Decimal('1.2349823094823948209384209348'))
                ],
                'default_update_value': [Decimal('2.3495083459083095483409534534')]
            },
            'double': {
                'test_cases': [
                    standard_test_case(0.0),
                    standard_test_case(2.2250738585072014e-308),
                    standard_test_case(1.7976931348623157e+308),
                    standard_test_case(-1.7976931348623157e+308)
                ],
                'default_update_value': [1.0]
            },
            'float': {
                'test_cases': [
                    standard_test_case(0.0),
                    standard_test_case(1.1754943508222875e-38),
                    standard_test_case(3.4028234663852886e+38),
                    standard_test_case(-3.4028234663852886e+38)
                ],
                'default_update_value': [1.0]
            },
            'text': {
                'test_cases': [
                    standard_test_case(''),
                    standard_test_case('abc')
                ],
                'default_update_value': ['def']
            },
            'varchar': {
                'test_cases': [
                    standard_test_case(''),
                    standard_test_case('abc')
                ],
                'default_update_value': ['def']
            },
            'bigint': {
                'test_cases': [
                    standard_test_case(0),
                    standard_test_case(2**63 - 1),
                    standard_test_case(-2**63)
                ],
                'default_update_value': [1]
            },
            'int': {
                'test_cases': [
                    standard_test_case(0),
                    standard_test_case(2**31 - 1),
                    standard_test_case(-2**31)
                ],
                'default_update_value': [1]
            },
            'smallint': {
                'test_cases': [
                    standard_test_case(0),
                    standard_test_case(2**15 - 1),
                    standard_test_case(-2**15)
                ],
                'default_update_value': [1]
            },
            'tinyint': {
                'test_cases': [
                    standard_test_case(0),
                    standard_test_case(2**7 - 1),
                    standard_test_case(-2**7)
                ],
                'default_update_value': [1]
            },
            'varint': {
                'test_cases': [
                    standard_test_case(0),
                    standard_test_case(2**128),
                    standard_test_case(-2**128)
                ],
                'default_update_value': [1]
            },
            'timestamp': {
                'test_cases': [
                    standard_test_case(datetime(1970, 1, 1, 0, 0)),
                    standard_test_case(datetime.strptime('2020-01-02 14:13:12.001', "%Y-%m-%d %H:%M:%S.%f"))
                ],
                'default_update_value': [datetime.strptime('2021-02-03 15:14:13.002', "%Y-%m-%d %H:%M:%S.%f")]
            },
            'date': {
                'test_cases': [
                    standard_test_case(Date(0)),
                    standard_test_case(Date('2020-1-2'))
                ],
                'default_update_value': [Date('2021-2-3')]
            },
            'time': {
                'test_cases': [
                    standard_test_case(Time(0)),
                    standard_test_case(Time('12:13:14.001'))
                ],
                'default_update_value': [Time('13:14:15.002')]
            },
            'timeuuid': {
                'test_cases': [
                    standard_test_case(uuid_from_time(datetime(2020, 1, 2, 3, 4, 5, 0)))
                ],
                'default_update_value': [uuid_from_time(datetime(2021, 2, 3, 4, 5, 6, 1))]
            },
            'uuid': {
                'test_cases': [
                    standard_test_case(uuid.UUID(bytes=b'\x00' * 16)),
                    standard_test_case(uuid.uuid4())
                ],
                'default_update_value': [uuid.uuid4()]
            }
        }

        session = self.prepare(options={'experimental_features': ['lwt']})

        for is_frozen in (False, True):
            for collection_type in ('list', 'set', 'tuple'):
                for column_type, test_data in PRIMITIVE_TYPES_MAP.items():
                    additional_test_data = {'collection_type': collection_type}
                    self._lwt_execute_single_type_update_test(session,
                        self._build_collection_typename(column_type, is_frozen, collection_type),
                        {**test_data, **additional_test_data})

    @skip('Failing for scylla, skip for now until investigated and fixed')
    def null_value_boolean_list_index_access_test(self):
        session = self.prepare(options={'experimental_features': ['lwt']})

        table_name = 'null_value_boolean_list_index_access_test'

        session.execute('''
            CREATE TABLE IF NOT EXISTS {table_name} (
                k int PRIMARY KEY,
                test list<boolean>
            )
        '''.format(table_name=table_name))

        insert_stmt = prepare_statement(session, '''
            INSERT INTO {table_name} (k, test) VALUES(?, ?)
        '''.format(table_name=table_name))
        session.execute(insert_stmt, [0, [None]])

        update_stmt = prepare_statement(session, '''
            UPDATE {table_name} SET test=:new_value WHERE k=0 IF test[:i]=:v
        '''.format(table_name=table_name))

        assert_one(session, "SELECT * FROM {table_name}".format(table_name=table_name), [0, [None]])
        assert_one_prepared(session, update_stmt, [True, [None]], {'new_value': [False], 'i': 0, 'v': None})
        assert_one(session, "SELECT * FROM {table_name}".format(table_name=table_name), [0, [False]])
