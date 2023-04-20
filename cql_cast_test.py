# coding: utf-8

import pytest
import logging

from tools.misc import require
from cqlsh_tests.cqlsh_copy_tests import CqlshPrepare


logger = logging.getLogger(__name__)


@pytest.mark.dtest_full
@pytest.mark.single_node
class TestCQLCast(CqlshPrepare):
    """ Class provides interface for CAST scalar function """

    COLUMN_NAME_TEMPLATE = '{}_clmn'
    KEYSPACE_NAME = 'ks'

    @require('#3108')
    def test_cast_negative(self):
        """Function performs positive tests CAST scalar function for user-defined type"""
        test_from = ['text', 'date']
        self._test_run(test_from, TestData.NEGATIVE_VALUES, compare_error=True)

    def test_cast_udt(self):
        """Function performs positive tests CAST scalar function for user-defined type"""
        test_from = ['udt']
        self._udt_test_run(test_from, TestData.POSITIVE_VALUES)

    def test_cast_varint(self):
        """Function performs positive tests CAST scalar function for varint type"""
        test_from = ['varint']
        self._test_run(test_from, TestData.POSITIVE_VALUES)

    def test_cast_tinyint(self):
        """Function performs positive tests CAST scalar function for tinyint type"""
        test_from = ['tinyint']
        self._test_run(test_from, TestData.POSITIVE_VALUES)

    def test_cast_smallint(self):
        """Function performs positive tests CAST scalar function for smallint type"""
        test_from = ['smallint']
        self._test_run(test_from, TestData.POSITIVE_VALUES)

    def test_cast_int(self):
        """Function performs positive tests CAST scalar function for all int type"""
        test_from = ['int']
        self._test_run(test_from, TestData.POSITIVE_VALUES)

    def test_cast_bigint(self):
        """Function performs positive tests CAST scalar function for bigint type"""
        test_from = ['bigint']
        self._test_run(test_from, TestData.POSITIVE_VALUES)

    def test_cast_float(self):
        """Function performs positive tests CAST scalar function for float type"""
        test_from = ['float']
        self._test_run(test_from, TestData.POSITIVE_VALUES)

    def test_cast_double(self):
        """Function performs positive tests CAST scalar function for double type"""
        test_from = ['double']
        self._test_run(test_from, TestData.POSITIVE_VALUES)

    def test_cast_decimal(self):
        """Function performs positive tests CAST scalar function for decimal type"""
        test_from = ['decimal']
        self._test_run(test_from, TestData.POSITIVE_VALUES)

    @pytest.mark.next_gating
    @pytest.mark.dtest_debug
    def test_cast_date(self):
        """Function performs positive tests CAST scalar function for date type"""
        test_from = ['date']
        self._test_run(test_from, TestData.POSITIVE_VALUES)

    def test_cast_time(self):
        """Function performs positive tests CAST scalar function for time type"""
        test_from = ['time']
        self._test_run(test_from, TestData.POSITIVE_VALUES)

    def test_cast_timestamp(self):
        """Function performs positive tests CAST scalar function for timestamp type"""
        test_from = ['timestamp']
        self._test_run(test_from, TestData.POSITIVE_VALUES)

    def test_cast_uuid(self):
        """Function performs positive tests CAST scalar function for uuid type"""
        test_from = ['uuid']
        self._test_run(test_from, TestData.POSITIVE_VALUES)

    def test_cast_boolean(self):
        """Function performs positive tests CAST scalar function for boolean"""
        test_from = ['boolean']
        self._test_run(test_from, TestData.POSITIVE_VALUES)

    def test_cast_ascii(self):
        """Function performs positive tests CAST scalar function for ascii type"""
        test_from = ['ascii']
        self._test_run(test_from, TestData.POSITIVE_VALUES)

    def test_cast_inet(self):
        """Function performs positive tests CAST scalar function for inet type"""
        test_from = ['inet']
        self._test_run(test_from, TestData.POSITIVE_VALUES)

    def test_cast_timeuuid(self):
        """Function performs positive tests CAST scalar function for timeuuid type"""
        test_from = ['timeuuid']
        self._test_run(test_from, TestData.POSITIVE_VALUES)

    @pytest.mark.skip('Skipped due to scylla#3109')
    def test_cast_issue_3109(self):
        """Function performs test for issue #3109"""
        # To remove the case from issue test, go to TestData.POSITIVE_VALUES and remove comment simbol "#" from relavant type
        test_from = ['float', 'double', 'timestamp']
        self._test_run(test_from, TestData.POSITIVE_VALUES, test_to=['text'], test_types=['cast'], exclude=False)

    @pytest.mark.skip('Skipped due to scylla#3109')
    def test_cast_udt_issue_3109(self):
        """Function performs test for issue #3109"""
        # To remove the case from issue test, go to TestData.POSITIVE_VALUES and remove comment simbol "#" from relavant type
        test_from = ['udt']
        self._udt_test_run(test_from, TestData.POSITIVE_VALUES, test_to=['text', 'varchar'],
                           test_types=['cast', 'min', 'max'], exclude=False)

    def test_cast_issue_3104(self):
        """Function performs or issue #3104"""
        # To remove the case from issue test, go to TestData.POSITIVE_VALUES and remove comment simbol "#" from relavant type
        test_from = ['date', 'timeuuid', 'timestamp']
        self._test_run(test_from, TestData.POSITIVE_VALUES, test_to=['date', 'timeuuid', 'timestamp'], test_types=['min', 'max'],
                       exclude=False)

    @require('#3110')
    def test_cast_issue_3110(self):
        """Function performs or issue #3110"""
        # To remove the case from issue test, go to TestData.POSITIVE_VALUES and remove comment simbol "#" from relavant type
        test_from = ['varint']
        self._test_run(test_from, TestData.POSITIVE_VALUES, test_to=['decimal'],
                       test_types=['cast', 'avg', 'sum', 'min', 'max', 'cast_min', 'cast_max', 'cast_avg'], exclude=False)

    @pytest.mark.skip('Skipped due to scylla#3111')
    def cast_issue_3111_test(self):
        """Function performs or issue #3111"""
        # To remove the case from issue test, go to TestData.POSITIVE_VALUES and remove comment simbol "#" from relavant type
        test_from = ['float', 'tinyint', 'smallint', 'int', 'bigint', 'varint']
        self._test_run(test_from, TestData.POSITIVE_VALUES, test_to=['decimal'],
                       test_types=['cast', 'sum', 'min', 'max', 'min', 'max', 'avg'], exclude=False)

    def test_cast_udt_issue_3111(self):
        """Function performs or issue #3111"""
        # To remove the case from issue test, go to TestData.POSITIVE_VALUES and remove comment simbol "#" from relavant type
        test_from = ['udt']
        self._udt_test_run(test_from, TestData.POSITIVE_VALUES, test_to=['decimal'],
                           test_types=['cast', 'sum', 'min', 'max', 'min', 'max', 'avg'], exclude=False)

    def test_cast_issue_3112(self):
        """Function performs or issue #3112"""
        # To remove the case from issue test, go to TestData.POSITIVE_VALUES and remove comment simbol "#" from relavant type
        test_from = ['decimal']
        # test_from = ['float', 'double', 'int', 'decimal', 'tinyint', 'varint']
        self._test_run(test_from, TestData.POSITIVE_VALUES, test_to=[
                       'float'], test_types=['cast_sum', 'cast_avg'], exclude=False)
        # test_types=['cast', 'sum', 'min', 'max', 'avg', 'cast_min', 'cast_max', 'cast_avg'], exclude=False)

    def _udt_test_run(self, test_from, data_dict, test_to=None, test_types=None, exclude=True, compare_error=False):
        """
        Function runs UDT test
        :param test_from: CAST FROM this column types. List of types, like: ['float', 'double', 'timestamp']
        :type test_from: list
        :param data_dict: test data dict from TestData class
        :type data_dict: dict
        :param test_to: CAST TO this column types. List of types, like: ['date', 'text', 'varchar']
        :type test_to: list
        :param test_types: which kind of test perform taken from self.TESTS.keys(). List of test types, like: ['cast', 'min']
        :type test_types: list
        :param exclude: if exclude tests types/column types are started from "#" in TestData.zzz_VALUES
        :type exclude: bool
        :return: None
        """
        if not [from_type for from_type in test_from if from_type in data_dict or (exclude and from_type.startswith('#'))]:
            return
        self.prepare()
        success = fail = 0
        for fromt in test_from:
            type_name = '{}_for_cast_test'.format(fromt)
            table_name = '{}_table'.format(fromt)
            column_name = self.COLUMN_NAME_TEMPLATE.format(fromt)
            self._prepare_udf_table(data_dict, fromt, type_name, table_name, column_name)

            for from_type in data_dict[fromt]:
                logger.debug(
                    '\n\n\n============================ CAST FROM {} ======================================'.format(from_type))
                udt_column_name = '{0}.{1}'.format(column_name, self.COLUMN_NAME_TEMPLATE.format(from_type))
                self._test_one_type(from_type, data_dict[fromt], exclude, udt_column_name, table_name, success, fail,
                                    test_types=test_types, test_to=test_to, compare_error=compare_error)

    def _test_run(self, test_from,  data_dict, test_to=None, test_types=None, exclude=True, compare_error=False):
        """
        Function runs selected tests
        :param test_from: CAST FROM this column types. List of types, like: ['float', 'double', 'timestamp']
        :type test_from: list
        :param data_dict: test data dict from TestData class
        :type data_dict: dict
        :param test_to: CAST TO this column types. List of types, like: ['date', 'text', 'varchar']
        :type test_to: list
        :param test_types: which kind of test perform taken from self.TESTS.keys(). List of test types, like: ['cast', 'min']
        :type test_types: list
        :param exclude: if exclude tests types/column types are started from "#" in TestData.zzz_VALUES
        :type exclude: bool
        :return: None
        """
        if not [from_type for from_type in test_from if from_type in data_dict or (exclude and from_type.startswith('#'))]:
            return
        self.prepare()
        success = fail = 0
        for from_type in test_from:
            if from_type in data_dict:
                logger.debug(
                    '\n\n\n============================ CAST FROM {} ======================================'.format(from_type))
                table_name = 'cast_{0}_test'.format(from_type)
                column_name = self.COLUMN_NAME_TEMPLATE.format(from_type)
                if not self._create_table_for_cast(table_name, from_type, column_name, data_dict[from_type]):
                    logger.debug('FAILURE: table {0} was not created. See error above'.format(table_name))
                    continue
                self._test_one_type(from_type, data_dict, exclude, column_name, table_name, success, fail,
                                    test_types=test_types, test_to=test_to, compare_error=compare_error)

    def _prepare_udf_table(self, data_dict, fromt, type_name, table_name, column_name):
        assert self._create_type(data_dict[fromt], type_name), \
            'FAILURE: type {0} was not created. See error above'.format(type_name)

        assert self._create_table_for_cast(table_name, 'frozen <{}>'.format(type_name), column_name, prefill=False), \
            'FAILURE: table {0} was not created. See error above'.format(table_name)

        for id in range(max([len(h) for h in data_dict[fromt].values()])):
            data = '{'
            for from_type, values in data_dict[fromt].items():
                if len(values) > id:
                    data = '{0}{1}: {2}, '.format(data, self.COLUMN_NAME_TEMPLATE.format(from_type), values[id][0])
            if data != '{':
                self._prefill_table(table_name, column_name, '{0}{1}'.format(data[:-2], '}'), id)

    def _test_one_type(self, from_type, data_dict, exclude, column_name, table_name, success, fail, test_types=None,
                       test_to=None, compare_error=None):
        for row_id, exp_results in enumerate(data_dict[from_type]):
            for test, test_exp_result in exp_results[1].items():
                # If asked exclude tests with name started with "#"
                # test_commented = False
                if exclude and test.startswith('#'):
                    continue
                test_commented = self.is_commented(exclude, test)
                test = test.replace('#', '')

                if (test_types and test in test_types) or not test_types:
                    for to_type, exp_result in test_exp_result.items():
                        # If asked exclude tests with name started with "#"
                        # type_commented = False
                        if exclude and to_type.startswith('#'):
                            continue
                        type_commented = self.is_commented(exclude, to_type)
                        to_type = to_type.replace('#', '')

                        if exp_result and (not test_to or (test_to and to_type in test_to)) \
                                and (exclude or (not exclude and (type_commented or test_commented))):
                            actual_result, err = self._test_execute(self.TESTS[test][0], column_name, to_type, row_id,
                                                                    table_name, from_type, test, exp_results=exp_results[0])

                            actual_result = actual_result.split('\n')[3].strip() \
                                if not compare_error and actual_result else err if compare_error else ''
                            assert actual_result == str(exp_result), "casting from type {} to type {}: expected {} but got {}".format(
                                from_type, to_type, exp_result, actual_result)

    def is_commented(self, exclude, ttype):
        return True if not exclude and ttype.startswith('#') else False

    def _create_type(self, data_dict, type_name):
        query = 'use {ks}; CREATE TYPE {type_name} ('.format(ks=self.KEYSPACE_NAME, type_name=type_name)
        for from_type in data_dict:
            query = '{query} {clmn_name} {clmn_type},'.format(query=query, clmn_name=self.COLUMN_NAME_TEMPLATE.format(from_type),
                                                              clmn_type=from_type)

        query = '{query})'.format(query=query[:-1])
        out, err = self.node1.run_cqlsh('{0}'.format(query), return_output=True)
        logger.debug(query)
        return False if err else True

    def _create_table_for_cast(self, table_name, from_type, column_name, values=None, prefill=True):
        """Prepare table for test: create table and fill with test data"""
        query_create = 'use {ks}; CREATE TABLE IF NOT EXISTS {table_name} (k int PRIMARY KEY, {tmp} {type_name}'.format(
            table_name=table_name, type_name=from_type, tmp=column_name, ks=self.KEYSPACE_NAME)

        out, err = self.node1.run_cqlsh('{0})'.format(query_create), return_output=True)
        logger.debug(query_create)
        if err:
            return False
        if prefill:
            for id, value in enumerate(values):
                self._prefill_table(table_name, column_name, value[0], id)
        return True

    def _prefill_table(self, table_name, column_name, value, id):
        query_insert = 'use {4}; INSERT INTO {0} (k, {1}) VALUES ({2}, {3})'.format(table_name, column_name,
                                                                                    id, value, self.KEYSPACE_NAME)
        self.node1.run_cqlsh(query_insert)
        logger.debug(query_insert)

    def _test_execute(self, func, column_name, to_type, row_id, table_name, from_type, test, exp_results):
        query = func(self, column_name, to_type, row_id, table_name, from_type, test, exp_results)
        logger.debug('Test case: CAST({2} as {3}). Run: casted value "{0}" and query {1}'
                     .format(exp_results, query, from_type, to_type))
        result, err = '', None
        try:
            result, err = self.node1.run_cqlsh(query, return_output=True)
        except Exception as e:
            logger.debug('FAILURE: test case failed. Error: {}'.format(str(e)))
        return result, err

    def _cast_single_value(self, column_name, to_type, row_id, table_name, from_type, test, exp_results):
        """Run cast select"""
        return 'use {ks}; SELECT cast({fromt} as {tot}) FROM {table_name} WHERE k={id}'.format(ks=self.KEYSPACE_NAME,
                                                                                               fromt=column_name, tot=to_type, id=row_id, table_name=table_name)

    def _agg_cast_function(self, column_name, to_type, row_id, table_name, from_type, test, exp_results=None):
        """Run DB aggregation select"""
        return 'use {ks}; SELECT {func}(cast({fromt} as {tot})) FROM {table_name}'.format(ks=self.KEYSPACE_NAME,
                                                                                          fromt=column_name, tot=to_type, table_name=table_name, func=test)

    def _cast_agg_function(self, column_name, to_type, row_id, table_name, from_type, test, exp_results=None):
        """Run DB cast of aggregation select"""
        func = test.replace('cast_', '')
        return 'use {ks}; SELECT cast({func}({fromt}) as {tot}) FROM {table_name}'.format(ks=self.KEYSPACE_NAME,
                                                                                          fromt=column_name, tot=to_type, table_name=table_name, func=func)

    def _cast_cast_function(self, column_name, to_type, row_id, table_name, from_type, test, exp_results=None):
        """Run DB cast of other cast select"""
        to_types = to_type.split('/')
        return 'use {ks}; SELECT cast(cast({fromt} as {tot1}) as {tot2}) FROM {table_name} where k={id}'.format(
            fromt=column_name, tot1=to_types[0], tot2=to_types[1], id=row_id, table_name=table_name, ks=self.KEYSPACE_NAME)

    # TEST structure: {<test type>: [<function reference>, <test name>]}
    TESTS = {'cast': [_cast_single_value, 'CAST()'],
             'avg': [_agg_cast_function, 'AVG(CAST())'],
             'sum': [_agg_cast_function, 'SUM(CAST())'],
             'min': [_agg_cast_function, 'MIN(CAST())'],
             'max': [_agg_cast_function, 'MAX(CAST())'],
             'cast_avg': [_cast_agg_function, 'CAST(AVG())'],
             'cast_sum': [_cast_agg_function, 'CAST(SUM())'],
             'cast_min': [_cast_agg_function, 'CAST(MIN())'],
             'cast_max': [_cast_agg_function, 'CAST(MAX())'],
             'cast_cast': [_cast_cast_function, 'CAST(CAST())']}


class TestData(object):
    # class CAST_DATA(object):
    # TODO: counters are not supported. should be added later
    # CAST_MAPPING structure:
    #  {<column type>: [<cast to type 1>, <cast to type 2>, <cast to type 3>...]}
    CAST_MAPPING = {
        'bigint': ['tinyint', 'smallint', 'int', 'float', 'double', 'decimal', 'varint', 'text', 'varchar'],
        'ascii': ['text', 'varchar'],
        'uuid': ['text', 'varchar'],
        'boolean': ['text', 'varchar'],
        'inet': ['text', 'varchar'],
        'time': ['text', 'varchar'],
        'timestamp': ['date', 'text', 'varchar'],
        'timeuuid': ['timestamp', 'date', 'text', 'varchar'],
        'date': ['timestamp'],
        'decimal': ['tinyint', 'smallint', 'int', 'float', 'double', 'bigint', 'varint', 'text', 'varchar'],
        'double': ['tinyint', 'smallint', 'int', 'float', 'decimal', 'bigint', 'varint', 'text', 'varchar'],
        'float': ['tinyint', 'smallint', 'int', 'double', 'decimal', 'bigint', 'varint', 'text', 'varchar'],
        'int': ['tinyint', 'smallint', 'float', 'double', 'decimal', 'bigint', 'varint', 'text', 'varchar'],
        'smallint': ['tinyint', 'int', 'float', 'double', 'decimal', 'bigint', 'varint', 'text', 'varchar'],
        'tinyint': ['smallint', 'int', 'float', 'double', 'decimal', 'bigint', 'varint', 'text', 'varchar'],
        'varint': ['tinyint', 'smallint', 'int', 'float', 'double', 'decimal', 'bigint', 'text', 'varchar']
    }

    # <ANY>_VALUES structure:
    # { <from type name>: [ [<input value>, {<test type>: {<to type>: <CAST_MAPPING[<from type name>][0]>, <to type>: <CAST_MAPPING[<from type name>][1]>, .. },
    #                                       <test type>: {<to type>: <CAST_MAPPING[<from type name>][0]>, <to type>: <CAST_MAPPING[<from type name>][1]>, .. }
    #                                       }],
    #                       [<input value>, {<test type>: {<to type>: <CAST_MAPPING[<from type name>][0]>, <to type>: <CAST_MAPPING[<from type name>][1]>, .. },
    #                                       <test type>: {<to type>: <CAST_MAPPING[<from type name>][0]>, <to type>: <CAST_MAPPING[<from type name>][1]>, .. }
    #                                       }]
    #                     ]
    # }
    # TODO: avg(cast) to decimal - Cassandra returns error
    POSITIVE_VALUES = {
        'bigint': [[7657139128428828644,
                    {'cast': {'tinyint': -28, 'smallint': -26652, 'int': -776038428, 'float': '7.6571e+18', 'double': '7.6571e+18',
                              '#decimal': '7.6571391284288287E+18', 'varint': 7657139128428828644, 'text': '7657139128428828644', 'varchar': '7657139128428828644'},
                     'avg': {'tinyint': 10, 'smallint': -10741, 'int': -258681333, 'float': '2.5524e+18', 'double': '2.5524e+18',
                             '#decimal': '2552379709476274375.7', 'varint': 2552379709476274357, 'text': None, 'varchar': None},
                     'sum': {'tinyint': 31, 'smallint': -32225, 'int': -776044001, 'float': '7.6571e+18', 'double': '7.6571e+18',
                             '#decimal': '7657139128428823127.0', 'varint': 7657139128428823071, 'text': None, 'varchar': None},
                     'min': {'tinyint': -66, 'smallint': -26652, 'int': -776038428, 'float': -5698, 'double': -5698,
                             '#decimal': -5698.0, 'varint': -5698, 'text': '-5698', 'varchar': '-5698'},
                     'max': {'tinyint': 125, 'smallint': 125, 'int': 125, 'float': '7.6571e+18', 'double': '7.6571e+18',
                             '#decimal': '7.6571391284288287E+18', 'varint': 7657139128428828644, 'text': '7657139128428828644',
                             'varchar': '7657139128428828644'}}],
                   [125, {'cast': {'tinyint': 125, 'smallint': 125, 'int': 125, 'float': 125, 'double': 125, 'decimal': 125.0,
                                   'varint': 125, 'text': '125', 'varchar': '125'}}],
                   [-5698, {'cast': {'tinyint': -66, 'smallint': -5698, 'int': -5698, 'double': -5698, 'decimal': -5698.0,
                                     'varint': -5698, 'text': '-5698', 'varchar': '-5698'}}]
                   ],
        'boolean': [[True, {'cast': {'text': 'true', 'varchar': 'true'}}],
                    [False, {'cast': {'text': 'false', 'varchar': 'false'}}]
                    ],
        'decimal': [[76571.345, {'cast': {'tinyint': 27, 'smallint': 11035, 'int': 76571, 'float': 76571.34375,
                                          'double': 76571.345, 'bigint': 76571, 'varint': 76571,
                                          'text': '76571.345', 'varchar': '76571.345'},
                                 'avg': {'tinyint': 14, 'smallint': 5390, 'int': 38158, 'float': 38158.54297,
                                         'double': 38158.54401, 'bigint': 38158, 'varint': 38158, 'text': None,
                                         'varchar': None},
                                 'sum': {'tinyint': 29, 'smallint': 10781, 'int': 76317, 'float': 76317.08594,
                                         'double': 76317.08801, 'bigint': 76317, 'varint': 76317, 'text': None,
                                         'varchar': None},
                                 'min': {'tinyint': 2, 'smallint': -254, 'int': -254, 'float': -254.25699,
                                         'double': -254.25699, 'bigint': -254, 'varint': -254,
                                         'text': '-254.256987',
                                         'varchar': '-254.256987'},
                                 'max': {'tinyint': 27, 'smallint': 11035, 'int': 76571, 'float': 76571.34375,
                                         'double': 76571.345, 'bigint': 76571, 'varint': 76571, 'text': '76571.345',
                                         'varchar': '76571.345'},
                                 'cast_avg': {'tinyint': 14, 'smallint': -27378, 'int': 38158,
                                              '#float': '38158.54297',
                                              'double': '38158.54401', 'bigint': 38158, 'varint': 38158,
                                              'text': None, 'varchar': None},
                                 'cast_sum': {'tinyint': 29, 'smallint': 10781, 'int': 76317,
                                              '#float': '76317.08594',
                                              'double': '76317.08801', 'bigint': 76317, 'varint': 76317,
                                              'text': None, 'varchar': None},
                                 'cast_min': {'tinyint': 2, 'smallint': -254, 'int': -254,
                                              'float': '-254.25699',
                                              'double': '-254.25699', 'bigint': -254, 'varint': -254,
                                              'text': '-254.256987', 'varchar': '-254.256987'},
                                 'cast_max': {'tinyint': 27, 'smallint': 11035, 'int': 76571, 'float': 76571.34375,
                                              'double': '76571.345', 'bigint': 76571, 'varint': 76571,
                                              'text': '76571.345', 'varchar': '76571.345'},
                                 'cast_cast': {'double/bigint': '76571'}}],
                    [-254.256987,
                     {'cast': {'tinyint': 2, 'smallint': -254, 'int': -254, 'float': -254.25699,
                               'double': -254.25699, 'bigint': -254, 'varint': -254, 'text': '-254.256987',
                               'varchar': '-254.256987'}}]
                    ],
        'double': [[76571.345, {'cast': {'tinyint': 27, 'smallint': 11035, 'int': 76571, 'float': 76571.34375,
                                         'decimal': '76571.345000000001', 'bigint': 76571, 'varint': 76571,
                                         '#text': '76571.345', '#varchar': '76571.345'},
                                'avg': {'tinyint': 14, 'smallint': 5390, 'int': 38158, 'float': 38158.54297,
                                        'decimal': '38158.54400650000050', 'bigint': 38158, 'varint': 38158,
                                        'text': None, 'varchar': None},
                                'sum': {'tinyint': 29, 'smallint': 10781, 'int': 76317, 'float': 76317.08594,
                                        'decimal': '76317.08801300000099', 'bigint': 76317, 'varint': 76317,
                                        'text': None, 'varchar': None},
                                'min': {'tinyint': 2, 'smallint': -254, 'int': -254, 'float': -254.25699,
                                        'decimal': '-254.25698700000001', 'bigint': -254, 'varint': -254,
                                        '#text': '-254.256987', '#varchar': '-254.256987'},
                                'max': {'tinyint': 27, 'smallint': 11035, 'int': 76571, 'float': 76571.34375,
                                        'decimal': '76571.345000000001', 'bigint': 76571, 'varint': 76571,
                                        '#text': '76571.345', '#varchar': '76571.345'}}],
                   [-254.256987, {'cast': {'tinyint': 2, 'smallint': -254, 'int': -254, 'float': -254.25699,
                                           'decimal': '-254.25698700000001', 'bigint': -254, 'varint': -254,
                                           '#text': '-254.256987', '#varchar': '-254.256987'}}]
                   ],
        'float': [[-564, {'cast': {'tinyint': -52, 'smallint': -564, 'int': -564, 'double': -564, '#decimal': '-564.0',
                                   'bigint': -564, 'varint': -564, '#text': '-564.0', '#varchar': '-564.0'},
                          'avg': {'tinyint': 10, 'smallint': 1034, 'int': 1034, 'double': '1034.23883',
                                  'decimal': '1034.238824', 'bigint': 1034, 'varint': 1034,
                                  'text': None, 'varchar': None},
                          'sum': {'tinyint': 30, 'smallint': 3102, 'int': 3102, 'double': '3102.71648',
                                  'decimal': '3102.716473', 'bigint': 3102, 'varint': 3102,
                                  'text': None, 'varchar': None},
                          'min': {'tinyint': -52, 'smallint': -564, 'int': -564, 'double': -564, '#decimal': '-564.0',
                                  'bigint': -564, 'varint': -564, '#text': '-564.0', '#varchar': '-564.0'},
                          'max': {'tinyint': 123, 'smallint': 3543, 'int': 3543, 'double': '3543.45654',
                                  'decimal': '3543.45654', 'bigint': 3543, 'varint': 3543,
                                  '#text': '3543.4565', '#varchar': '3543.4565'}}],
                  [3543.4565429688, {'cast': {'tinyint': -41, 'smallint': 3543, 'int': 3543, 'double': 3543.45654,
                                              'decimal': '3543.45654', 'bigint': 3543, 'varint': 3543,
                                              '#text': '3543.4565', '#varchar': '3543.4565'}}],
                  ['123.25993624886543132316548678', {'cast': {'tinyint': 123, 'smallint': 123, 'int': 123, 'double': '123.25993',
                                                               'decimal': '123.259933', 'bigint': 123, 'varint': 123,
                                                               '#text': '123.25993', '#varchar': '123.25993'}}]
                  ],
        'int': [[0, {'cast': {'tinyint': 0, 'smallint': 0, 'float': 0, 'double': 0, 'decimal': '0.0', 'bigint': 0, 'varint': 0,
                              'text': '0', 'varchar': '0'},
                     'avg': {'tinyint': 3, 'smallint': -2044, 'float': -2044.40002, 'double': -2044.4,
                             'decimal': '-2044.4', 'bigint': -2044, 'varint': -2044, 'text': None, 'varchar': None},
                     'sum': {'tinyint': 18, 'smallint': -10222, 'float': -10222, 'double': -10222,
                             'decimal': '-10222.0', 'bigint': -10222, 'varint': -10222, 'text': None, 'varchar': None},
                     'min': {'tinyint': -76, 'smallint': -32767, 'float': -32767, 'double': -32767,
                             'decimal': '-32767.0', 'bigint': -32767, 'varint': -32767, 'text': '-12876',
                             'varchar': '-12876'},
                     'max': {'tinyint': 94, 'smallint': 32767, 'float': 32767, 'double': 32767,
                             'decimal': '32767.0', 'bigint': 32767, 'varint': 32767, 'text': '32767', 'varchar': '32767'}}],
                [32767, {'cast': {'tinyint': -1, 'smallint': 32767, 'float': 32767, 'double': 32767,
                                  'decimal': '32767.0', 'bigint': 32767, 'varint': 32767, 'text': '32767', 'varchar': '32767'}}],
                [-32767, {'cast': {'tinyint': 1, 'smallint': -32767, 'float': -32767, 'double': -32767,
                                   'decimal': '-32767.0', 'bigint': -32767, 'varint': -32767, 'text': '-32767',
                                   'varchar': '-32767'}}],
                [-12876, {}],
                [2654, {}]
                ],
        'smallint': [[32767, {'cast': {'tinyint': -1, 'int': 32767, 'float': 32767, 'double': 32767, 'decimal': '32767.0',
                                       'bigint': 32767, 'varint': 32767, 'text': '32767', 'varchar': '32767'},
                              'avg': {'tinyint': 4, 'int': -2555, 'float': -2555.5, 'double': -2555.5, 'decimal': '-2555.5',
                                      'bigint': -2555, 'varint': -2555, 'text': None, 'varchar': None},
                              'sum': {'tinyint': 18, 'int': -10222, 'float': -10222, 'double': -10222, 'decimal': '-10222.0',
                                      'bigint': -10222, 'varint': -10222, 'text': None, 'varchar': None},
                              'min': {'tinyint': -76, 'int': -32767, 'float': -32767, 'double': -32767, 'decimal': '-32767.0',
                                      'bigint': -32767, 'varint': -32767, 'text': '-12876', 'varchar': '-12876'},
                              'max': {'tinyint': 94, 'int': 32767, 'float': 32767, 'double': 32767, 'decimal': '32767.0',
                                      'bigint': 32767, 'varint': 32767, 'text': '32767', 'varchar': '32767'}}],
                     [-32767, {'cast': {'tinyint': 1, 'int': -32767, 'float': -32767, 'double': -32767, 'decimal': '-32767.0',
                                        'bigint': -32767, 'varint': -32767, 'text': '-32767', 'varchar': '-32767'}}],
                     [-12876, {'cast': {'tinyint': -76, 'int': -12876, 'float': -12876, 'double': -12876, 'decimal': '-12876.0',
                                        'bigint': -12876, 'varint': -12876, 'text': '-12876', 'varchar': '-12876'}}],
                     [2654, {'cast': {'tinyint': 94, 'int': 2654, 'float': 2654, 'double': 2654, 'decimal': '2654.0', 'bigint': 2654,
                                      'varint': 2654, 'text': '2654', 'varchar': '2654'}}]
                     ],
        'tinyint': [
            [127, {'cast': {'smallint': 127, 'int': 127, 'float': 127, 'double': 127, 'decimal': '127.0',
                            'bigint': 127, 'varint': 127, 'text': '127', 'varchar': '127'},
                   'avg': {'smallint': 11, 'int': 11, 'float': 11.33333, 'double': 11.33333,
                           'decimal': '11.3', 'bigint': 11, 'varint': 11, 'text': None, 'varchar': None},
                   'sum': {'smallint': 34, 'int': 34, 'float': 34, 'double': 34, 'decimal': '34.0',
                           'bigint': 34, 'varint': 34, 'text': None, 'varchar': None},
                   'min': {'smallint': -128, 'int': -128, 'float': -128, 'double': -128,
                           'decimal': '-128.0', 'bigint': -128, 'varint': -128, 'text': '-128', 'varchar': '-128'},
                   'max': {'smallint': 127, 'int': 127, 'float': 127, 'double': 127, 'decimal': '127.0',
                           'bigint': 127, 'varint': 127, 'text': '35', 'varchar': '35'}}],
            [-128,
             {'cast': {'smallint': -128, 'int': -128, 'float': -128, 'double': -128, 'decimal': '-128.0',
                       'bigint': -128, 'varint': -128, 'text': '-128', 'varchar': '-128'}}],
            [35,
             {'cast': {'smallint': 35, 'int': 35, 'float': 35, 'double': 35, 'decimal': '35.0',
                       'bigint': 35, 'varint': 35, 'text': '35', 'varchar': '35'}}]
        ],
        # TODO: Cassandra problem:
        # TODO: SELECT avg(cast(varint_clmn as decimal)) FROM cast_varint_test;
        # TODO: ServerError: java.lang.ArithmeticException: Non-terminating decimal expansion; no exact representable decimal result.
        # TODO: So I do not really know which value should be here
        'varint': [[32767456456456456456545678943512357658768763546575675,
                    {'cast': {'tinyint': 59, 'smallint': 9019, 'int': -1446698181, 'float': 'Infinity',
                              'double': 3.2767e+52, '#decimal': '3.2767456456456457E+52',
                              'bigint': -4832518953173179589,
                              'text': '32767456456456456456545678943512357658768763546575675',
                              'varchar': '32767456456456456456545678943512357658768763546575675'},
                     'avg': {'tinyint': -9, 'smallint': 8011, 'int': -678398815, 'float': 'NaN',
                             'double': '1419.33333',
                             '#decimal': 1419.3,  # -- this Cassandra result seems wrong (CASSANDRA-14170)
                             'bigint': -2110054580, 'text': None, 'varchar': None},
                     'sum': {'tinyint': -29, 'smallint': 24035, 'int': -2035196445, 'float': 'NaN', 'double': 4258,
                             '#decimal': '4258.0',  # -- this Cassandra result seems wrong (CASSANDRA-14170)
                             'bigint': -6330163741, 'text': None, 'varchar': None},
                     'min': {'tinyint': -94, 'smallint': 4258, 'int': -1446698181, 'float': '-Infinity',
                             'double': -3.2767e+52, '#decimal': '-3.2767456456456457E+52',
                             'bigint': -4832518953173179589,
                             'text': '-32767456456456456456545678943512357658768769876743674',
                             'varchar': '-32767456456456456456545678943512357658768769876743674'},
                     'max': {'tinyint': 59, 'smallint': 10758, 'int': 4258, 'float': 'Infinity',
                             'double': 3.2767e+52, '#decimal': '3.2767456456456457E+52',
                             'bigint': 4832518946843011590, 'text': '4258', 'varchar': '4258'},
                     'cast_avg': {'tinyint': 76, 'smallint': 8012, 'int': -2110054580, 'float': '-2.1101e+09',
                                  'double': '-2.1101e+09', '#decimal': '-2.11005458E+9', 'bigint': -2110054580,
                                  'text': None, 'varchar': None},
                     'cast_sum': {'tinyint': -29, 'smallint': 24035, 'int': -2035196445, 'float': '-6.3302e+09',
                                  'double': '-6.3302e+09', '#decimal': '-6330163741', 'bigint': -6330163741,
                                  'text': None, 'varchar': None},
                     'cast_min': {'tinyint': 6, 'smallint': 10758, 'int': -588502522, 'float': '-Infinity',
                                  'double': '-3.2767e+52', '#decimal': '-3.2767456456456457E+52',
                                  'bigint': 4832518946843011590,
                                  'text': '-32767456456456456456545678943512357658768769876743674',
                                  'varchar': '-32767456456456456456545678943512357658768769876743674'},
                     'cast_max': {'tinyint': 59, 'smallint': 9019, 'int': -1446698181, 'float': 'Infinity',
                                  'double': '3.2767e+52', '#decimal': '3.2767456456456457E+52',
                                  'bigint': -4832518953173179589,
                                  'text': '32767456456456456456545678943512357658768763546575675',
                                  'varchar': '32767456456456456456545678943512357658768763546575675'}
                     }],
                   [-32767456456456456456545678943512357658768769876743674,
                    {'cast': {'tinyint': 6, 'smallint': 10758, 'int': -588502522, 'float': '-Infinity',
                              'double': -3.2767e+52, '#decimal': '-3.2767456456456457E+52',
                              'bigint': 4832518946843011590,
                              'text': '-32767456456456456456545678943512357658768769876743674',
                              'varchar': '-32767456456456456456545678943512357658768769876743674'}}],
                   [4258, {'cast': {'tinyint': -94, 'smallint': 4258, 'int': 4258, 'float': 4258, 'double': 4258,
                                    'decimal': '4258.0', 'bigint': 4258, 'text': '4258', 'varchar': '4258'}}]
                   ],
        'ascii': [["\'045asciitext\'", {'cast': {'text': '045asciitext', 'varchar': '045asciitext'},
                                        'min': {'text': '045asciitext', 'varchar': '045asciitext'},
                                        'max': {'text': 'abcdefj', 'varchar': 'abcdefj'}
                                        }],
                  ["\'abcdefj\'", {'cast': {'text': 'abcdefj', 'varchar': 'abcdefj'}}]
                  ],
        'date': [["\'2017-11-25\'", {'cast': {'timestamp': '2017-11-25 00:00:00.000000+0000'},
                                     'min':  {'timestamp': '1900-01-01 00:00:00.000000+0000'},
                                     'max':  {'timestamp': '2458-12-13 00:00:00.000000+0000'}
                                     }],
                 ["\'1900-01-01\'", {'cast':  {'timestamp': '1900-01-01 00:00:00.000000+0000'}}],
                 ["\'2458-12-13\'", {'cast':  {'timestamp': '2458-12-13 00:00:00.000000+0000'}}]
                 ],
        'time': [["\'13:07:45.089\'", {'cast': {'text': '13:07:45.089000000', 'varchar': '13:07:45.089000000'},
                                       'min': {'text': '00:45:25.123000000', 'varchar': '00:45:25.123000000'},
                                       'max': {'text': '13:07:45.089000000', 'varchar': '13:07:45.089000000'}
                                       }],
                 ["\'00:45:25.123\'", {'cast': {'text': '00:45:25.123000000', 'varchar': '00:45:25.123000000'}}],
                 ["\'10:14:36.0002312\'", {'cast': {'text': '10:14:36.000231200', 'varchar': '10:14:36.000231200'}}]
                 ],
        'timestamp': [["\'2017-12-27 11:57:42+0000\'",
                       {'cast': {'date': '2017-12-27', '#text': '2017-12-27T11:57:42.370Z', '#varchar': '2017-12-27T11:57:42.370Z'},
                        'min': {'date': '1900-01-01', '#text': '1900-01-01T00:00:00.000Z', '#varchar': '1900-01-01T00:00:00.000Z'},
                        'max': {'date': '2018-02-01', '#text': '2018-02-01T01:00:42.000Z', '#varchar': '2018-02-01T01:00:42.000Z'}
                        }],
                      ["\'2018-02-01 01:00:42+0000\'",
                       {'cast': {'date': '2018-02-01', '#text': '2018-02-01T01:00:42.000Z', '#varchar': '2018-02-01T01:00:42.000Z'}}],
                      ["\'1900-01-01 00:00:00+0000\'",
                       {'cast': {'date': '1900-01-01', '#text': '1900-01-01T00:00:00.000Z', '#varchar': '1900-01-01T00:00:00.000Z'}}]
                      ],
        'timeuuid': [['minTimeuuid(\'2018-01-01 17:01:07+0000\')',
                      {'cast': {'timestamp': '2018-01-01 17:01:07.000000+0000', 'date': '2018-01-01',
                                'text': '5bb48b80-ef15-11e7-8080-808080808080', 'varchar': '5bb48b80-ef15-11e7-8080-808080808080'},
                       'min': {'timestamp': '1900-01-01 00:00:00.000000+0000', 'date': '1900-01-01',
                               'text': '3c230000-a32f-1163-8080-808080808080', 'varchar': '3c230000-a32f-1163-8080-808080808080'},
                       'max': {'timestamp': '2018-01-01 17:01:07.000000+0000', 'date': '2018-01-01',
                               'text': '7a73e600-eb01-11e7-8080-808080808080', 'varchar': '7a73e600-eb01-11e7-8080-808080808080'}
                       }],
                     ['minTimeuuid(\'2017-12-27 12:28:44+0000\')',
                      {'cast': {'timestamp': '2017-12-27 12:28:44.000000+0000', 'date': '2017-12-27',
                                'text': '7a73e600-eb01-11e7-8080-808080808080', 'varchar': '7a73e600-eb01-11e7-8080-808080808080'}}],
                     ['minTimeuuid(\'1900-01-01 00:00:00+0000\')',
                      {'cast': {'timestamp': '1900-01-01 00:00:00.000000+0000', 'date': '1900-01-01',
                                'text': '3c230000-a32f-1163-8080-808080808080', 'varchar': '3c230000-a32f-1163-8080-808080808080'}}]
                     ],
        'uuid': [['de5cba0d-41a2-4f39-8834-35130d8b5d86',
                  {'cast': {'text': 'de5cba0d-41a2-4f39-8834-35130d8b5d86', 'varchar': 'de5cba0d-41a2-4f39-8834-35130d8b5d86'},
                   'min': {'text': 'de5cba0d-41a2-4f39-8834-35130d8b5d86', 'varchar': 'de5cba0d-41a2-4f39-8834-35130d8b5d86'},
                   'max': {'text': 'fa80080c-a4c5-46d6-afe4-5e184fec35ae', 'varchar': 'fa80080c-a4c5-46d6-afe4-5e184fec35ae'}
                   }],
                 ['f5f576e7-efb6-41b5-804e-fa3205548137',
                  {'cast': {'text': 'f5f576e7-efb6-41b5-804e-fa3205548137', 'varchar': 'f5f576e7-efb6-41b5-804e-fa3205548137'}}],
                 ['fa80080c-a4c5-46d6-afe4-5e184fec35ae',
                  {'cast': {'text': 'fa80080c-a4c5-46d6-afe4-5e184fec35ae', 'varchar': 'fa80080c-a4c5-46d6-afe4-5e184fec35ae'}}]
                 ],
        'inet': [["\'205.118.5.3\'", {'cast': {'text': '205.118.5.3', 'varchar': '205.118.5.3'},
                                      'min': {'text': '192.168.122.255', 'varchar': '192.168.122.255'},
                                      'max': {'text': '205.118.5.3', 'varchar': '205.118.5.3'}
                                      }],
                 ["\'192.168.2.1\'", {'cast': {'text': '192.168.2.1', 'varchar': '192.168.2.1'}}],
                 ["\'192.168.122.255\'", {'cast': {'text': '192.168.122.255', 'varchar': '192.168.122.255'}}]
                 ],
        'udt':  {'bigint': [[4561313648948, {'cast': {'decimal': '4561313648948.0', 'text': '4561313648948'},
                                             'avg': {'decimal': '2280656551235.0', 'text': None},
                                             'sum': {'decimal': '4561313102470.0', 'text': None},
                                             'min': {'decimal': '-546478.0', 'text': '-546478'},
                                             'max': {'decimal': '4561313648948.0', 'text': '4561313648948'},
                                             'cast_avg': {'decimal': '2280656551235.0', 'text': '2280656551235'},
                                             'cast_sum': {'#decimal': '4.56131310247E+12', 'text': '4561313102470'},
                                             'cast_min': {'decimal': '-546478.0', 'text': '-546478'},
                                             'cast_max': {'decimal': '4561313648948.0', 'text': '4561313648948'},
                                             'cast_cast': {'#decimal/float': '4561313464320', 'smallint/text': '-11980'}
                                             }],
                            [-546478, {'cast': {'decimal': '-546478.0', 'text': '-546478'}}]
                            ],
                 'float': [[55646.234556984, {'cast': {'int': 55646, 'double': '55646.23438'}}]],
                 'timestamp': [["\'2017-12-27 11:57:42+0000\'", {'cast': {'date': '2017-12-27', '#text': '2017-12-27T11:57:42.370Z'}}]]
                 }
    }

    NEGATIVE_VALUES = {
        'text': [['jhkhkjh',
                  {'cast': {'int': 'InvalidRequest: Error from server: code=2200 [Invalid query] message="Invalid call to function system.castAsInt, none of its type signatures match (known type signatures: system.castAsInt : (smallint) -> int, system.castAsInt : (decimal) -> int, system.castAsInt : (varint) -> int, system.castAsInt : (float) -> int, system.castAsInt : (tinyint) -> int, system.castAsInt : (counter) -> int, system.castAsInt : (bigint) -> int, system.castAsInt : (double) -> int)"',
                            'uuid': 'InvalidRequest: Error from server: code=2200 [Invalid query] message="c cannot be cast to uuid"',
                            'ascii': 'InvalidRequest: Error from server: code=2200 [Invalid query] message="Invalid call to function system.castAsAscii, none of its type signatures match (known type signatures: system.castAsAscii : (boolean) -> ascii, system.castAsAscii : (smallint) -> ascii, system.castAsAscii : (decimal) -> ascii, system.castAsAscii : (varint) -> ascii, system.castAsAscii : (timestamp) -> ascii, system.castAsAscii : (date) -> ascii, system.castAsAscii : (int) -> ascii, system.castAsAscii : (time) -> ascii, system.castAsAscii : (float) -> ascii, system.castAsAscii : (timeuuid) -> ascii, system.castAsAscii : (uuid) -> ascii, system.castAsAscii : (tinyint) -> ascii, system.castAsAscii : (inet) -> ascii, system.castAsAscii : (counter) -> ascii, system.castAsAscii : (bigint) -> ascii, system.castAsAscii : (double) -> ascii)"'},
                   'avg': {'int': 'InvalidRequest: Error from server: code=2200 [Invalid query] message="Invalid call to function system.castAsInt, none of its type signatures match (known type signatures: system.castAsInt : (smallint) -> int, system.castAsInt : (decimal) -> int, system.castAsInt : (varint) -> int, system.castAsInt : (float) -> int, system.castAsInt : (tinyint) -> int, system.castAsInt : (counter) -> int, system.castAsInt : (bigint) -> int, system.castAsInt : (double) -> int)"'},
                   'sum': {'float': 'InvalidRequest: Error from server: code=2200 [Invalid query] message="Invalid call to function system.castAsFloat, none of its type signatures match (known type signatures: system.castAsFloat : (varint) -> float, system.castAsFloat : (int) -> float, system.castAsFloat : (tinyint) -> float, system.castAsFloat : (counter) -> float, system.castAsFloat : (bigint) -> float, system.castAsFloat : (double) -> float, system.castAsFloat : (smallint) -> float, system.castAsFloat : (decimal) -> float)"'},
                   'min': {'inet': 'InvalidRequest: Error from server: code=2200 [Invalid query] message="c cannot be cast to inet"'},
                   'max': {'uuid': 'InvalidRequest: Error from server: code=2200 [Invalid query] message="c cannot be cast to uuid"'}
                   }]
                 ],
        'date':  ['2015-12-23', {'cast': {'int': 'InvalidRequest: Error from server: code=2200 [Invalid query] message="Invalid call to function system.castAsInt, none of its type signatures match (known type signatures: system.castAsInt : (smallint) -> int, system.castAsInt : (decimal) -> int, system.castAsInt : (varint) -> int, system.castAsInt : (float) -> int, system.castAsInt : (tinyint) -> int, system.castAsInt : (counter) -> int, system.castAsInt : (bigint) -> int, system.castAsInt : (double) -> int)"'}}]
    }
