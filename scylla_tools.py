import os
import shutil
import time
import unittest
import subprocess
import logging

import pytest
import tabulate
from cassandra import ConsistencyLevel
from cassandra.concurrent import execute_concurrent_with_args, execute_concurrent
from cassandra.query import SimpleStatement
from ccmlib import common
from ccmlib.node import NodetoolError
import re
from dtest_setup import DTestSetup
from dtest_config import DTestConfig
from dtest_setup_overrides import DTestSetupOverrides
import random
import string
import itertools
from copy import deepcopy
import datetime
from tools.data import rows_to_list
from uuid import UUID
from concurrent.futures import ThreadPoolExecutor
from typing import Optional
import glob
from tools.files import get_cf_dir


logger = logging.getLogger(__name__)


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
        raise ValueError(
            "Inconsistent 'c1/c2_values' contents. 'c1/c2_values' should be either a '[]' value or a list of the same length as a requested number of keys.")


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


def insert_c1c2_with_clustering(session, clustering_key_values=None, n=None, consistency=ConsistencyLevel.QUORUM,
                                c1_values=None, c2_values=None, ks='ks', cf='cf', partition_key_set_value=1,
                                output_20_lines=True):
    if clustering_key_values is None:
        clustering_key_values = []

    if c1_values is None:
        c1_values = []

    if c2_values is None:
        c2_values = []

    build_insert_params(clustering_key_values, n, c1_values, c2_values)

    partition_key_values = [partition_key_set_value]*len(clustering_key_values)

    statement = session.prepare("INSERT INTO {}.{} (pkey, ckey, c1, c2) VALUES (?, ?, ?, ?)".format(ks, cf))
    statement.consistency_level = consistency

    execute_concurrent_with_args(
        session, statement, map(lambda w, x, y, z: [w, x, y, z], partition_key_values,
                                clustering_key_values, c1_values, c2_values))

    if output_20_lines:
        logger.debug("output of 20 lines after insertion:")
        query = SimpleStatement('SELECT * FROM %s.%s limit 20' % (ks, cf), consistency_level=consistency)
        rows = list(session.execute(query))
        logger.debug("\n".join(str(row) for row in rows))


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
    for nr in range(1, nr_columns + 1):
        if nr != nr_columns:
            cql_str += 'c{}, '.format(nr)
        else:
            cql_str += 'c{}) VALUES (?, '.format(nr)
    for nr in range(1, nr_columns + 1):
        if nr != nr_columns:
            cql_str += '?, '
        else:
            cql_str += '?) '

    statement = session.prepare(cql_str)
    statement.consistency_level = consistency

    # build column values for c1 to cn
    col_data = []
    for nr in range(1, nr_columns + 1):
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
        assert len(rows) == 1, 'Wrong length, %s' % len(rows)
        res = rows[0]
        assert len(res) == 2, "Expected 2 columns in result, but got: {}".format(res)
        assert res[0] == c1_value and res[1] == c2_value, "Expected Row(c1='{}', c2='{}'), but got: {}".format(
            c1_value, c2_value, res)

    if must_be_missing:
        assert len(rows) == 0


def query_c1c2(session, key, consistency=ConsistencyLevel.QUORUM, tolerate_missing=False, must_be_missing=False, c1_value='value1', c2_value='value2', ks="ks", cf="cf"):
    query = SimpleStatement('SELECT c1, c2 FROM %s.%s WHERE key=\'k%d\'' % (ks, cf, key), consistency_level=consistency)
    rows = list(session.execute(query))
    check_c1c2_result_one(True, rows, tolerate_missing, must_be_missing, c1_value, c2_value)


def query_c1c2_concurrent(session, keys, consistency=ConsistencyLevel.QUORUM, tolerate_missing=False, must_be_missing=False, c1_values=None, c2_values=None):
    if c1_values is None:
        c1_values = ['value1'] * len(keys)

    if c2_values is None:
        c2_values = ['value2'] * len(keys)

    if len(c1_values) != len(c2_values) or len(c1_values) != len(keys):
        raise ValueError(
            "Inconsistent 'c1/c2_values' contents. 'c1/c2_values' should be either a 'None' value or a list of the same length as a requested number of keys.")

    # prepare a query statement
    query = 'SELECT c1, c2 FROM cf WHERE key=?'
    pquery = session.prepare(query)
    pquery.consistency_level = consistency

    results = execute_concurrent_with_args(session, pquery, map(lambda x: ['k{}'.format(x)], keys))
    for result, c1, c2 in zip(results, c1_values, c2_values):
        check_c1c2_result_one(result[0], list(result[1]), tolerate_missing, must_be_missing, c1, c2)


def generate_random_text(length=10):
    return ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(length))


def scylla_mode(modes):
    """
        Run the decorated tests if they are executed on correct mode
        @scylla_mode('release') - will run tests only if mode is release
        @scylla_mode('debug') - will run tests only if mode is debug
        @scylla_mode('!debug') - will run tests only if mode is not debug
   ."""
    NO_SKIP = os.environ.get('SKIP', '').lower() in ('no', 'false')
    is_scylla = False
    cdir = os.environ.get('CASSANDRA_DIR')
    if cdir:
        is_scylla = common.isScylla(cdir)
    else:
        cdir = os.environ.get('SCYLLA_CORE_PACKAGE')
        if cdir:
            is_scylla = True
        else:
            version = os.environ.get('SCYLLA_VERSION')
            if version:
                is_scylla = True
                mode = 'reloc'
    if not is_scylla:
        return unittest.skipIf(True, 'Test disabled for non-scylla installation')
    if NO_SKIP:
        return unittest.skipIf(False, 'NO_SKIP')
    if cdir:
        idir, mode = common.scylla_extract_install_dir_and_mode(cdir)
    found = (modes.find(mode) != -1)
    if modes[0] != '!':
        do_skip = not found
    else:
        do_skip = found
    return unittest.skipIf(do_skip, 'Test disabled for scylla %s' % mode)


def get_sstables_files(cf_dir, f_type=''):
    """
    Returns a set of sstable(s) files for a given KS and CF
    """
    tocs = glob.glob(os.path.join(cf_dir, '*-TOC.txt'))
    files = []
    if not f_type:
        for t in tocs:
            files += glob.glob(t[:-7] + '*')
    elif f_type == 'TOC':
        files = tocs
    else:
        for t in tocs:
            files += glob.glob(t[:-7] + f"*{f_type}*")
    return set([os.path.basename(fname) for fname in files])


def get_all_files_in_dir(dir_path):
    """
    Returs a set of all files in the given directory
    """
    dir_files = set()
    for f in os.listdir(dir_path):
        full_name = os.path.join(dir_path, f)
        if os.path.isfile(full_name):
            dir_files.add(f)

    return dir_files


def get_latest_dir(srcdir: str, pattern: Optional[str] = '') -> Optional[str]:
    """
    Get latest created directory path, which matches with the pattern
    """
    sorted_list = sorted(os.listdir(srcdir), reverse=True,
                         key=lambda x: os.path.getctime(os.path.join(srcdir, x)))
    for item in sorted_list:
        item_path = os.path.join(srcdir, item)
        if os.path.isdir(item_path) and re.compile(pattern).match(item):
            return item_path


def flush_by_node(cluster):
    for node in cluster.nodelist():
        node.flush()


class TableManager(object):
    """Class provides interface to create and prefill tables and materialized views by user demand"""
    CLMN_PREFIX = 'clmn'
    DEFAULT_MIN_LENGTH = 1
    DEFAULT_MAX_LENGTH = 10

    # TODO: add possibility for PK and CL order
    def __init__(self, session, cluster, keyspace='ks', table_name='tm_table', columns=None, pk_columns=None,
                 cl_columns=None, table_options=None, all_types_table=False):
        """
        Function initializes TableManager class
        :param columns: {<column type>: {  'amount': <how many columns with this type>,
                                           'names': [columns names, comma separated],
                                           'prefix': <prefix will be used for column names>,
                                           'frozen': <if frozen: True-frozen/False>},
                                           'value length': value length of this <column type> definition. Dictionary:
                                                        'min': dict defined minimum value length
                                                        'max': dict defined maximum value length
                                        }
                        If columns name is provided, expected:
                        - name for every column of this type(by <how many columns with this type> parameter). If not all names are proveded
                            (according to amount), default names will be used
                        OR
                        If prefix is provided, names of columns will be started from this string
                        OR
                        if 'names' AND 'prefix' are provided, 'names' will be used
                        OR
                        if 'names' or 'prefix' aren't provided, default prefix will be used
                EXAMPLES:
                    1) 'list<text>': {'amount': 1, 'frozen': False}
                    2) {'int': {'amount': 2, 'prefix': 'mypref', 'frozen': False, 'value length': {'min': 1, 'max': 10000}}
                    3) 'text': {'amount': 2, 'names': ['c1', 'c2'], 'frozen': False, 'value length': {'min': 1, 'max': 10000}}
                DEFAULT:
                    {'int': {'amount': 2, 'frozen': False, 'value length': {'min': 1, 'max': 10000}},
                    'text': {'amount': 1, 'frozen': False, 'value length': {'min': 1, 'max': 10000}}
                ***NOTE***: id column with type 'varint' will be aded automatically. Its value wil be unique.
        :type columns: dict
        :param pk_columns: columns for PRIMARY KEYS. Could by defined by:
                            - 'by name': list of explicitly defined names of this type
                            - 'by count': amount of columns of this type
                           {<column type>: {'by count': <how many columns with this type set to PK>}}
                           {<column type>: {'by name': [columns names, comma separated]}}
                      EXAMPLE:
                        {'int': {'by count': 1}, 'text': {'by name': ['c1']}}
                      DEFAULT:
                        {'int': {'by count': 1}}
        :type pk_columns: dict
        :param cl_columns: columns for CLUSTERING. Could by defined by:
                            - 'by name': list of explicitly defined names of this type
                            - 'by amount': amount of columns of this type
                           {<column type>: {'by count': <how many columns with this type set to CLUSTERING>}}
                           {<column type>: {'by name': [columns names, comma separated]}}
                      EXAMPLE:
                        {'int': {'by count': 1}, 'text': {'by name': ['c1']}}
                      DEFAULT:
                        {'text': {'by count': 1}}
                      ***NOTE***: if it shouldn't be clustering keys, send som definitions with 0 counts. For example: {'text': {'by count': 0}}
        :type cl_columns: dict
        :param table_options: WILL BE DEFINED
        :type table_options: dict
        :param all_types_table: create table with all knows column types: True/False.
                                In case True, columns/pk_columns/cl_columns values will be ignored
                                DEFAULT: False
        :type all_types_table: bool
        """
        self.session = session
        self.cluster = cluster
        self.columns_dict = columns or {'int': {'amount': 1, 'frozen': False,
                                                'value length': {'min': self.DEFAULT_MIN_LENGTH, 'max': self.DEFAULT_MAX_LENGTH}},
                                        'text': {'amount': 1, 'frozen': False,
                                                 'value length': {'min': self.DEFAULT_MIN_LENGTH, 'max': self.DEFAULT_MAX_LENGTH}}}

        self.cl_columns_dict = cl_columns if cl_columns is not None else {'text': {'by count': 1}}
        self.pk_columns_dict = pk_columns if pk_columns is not None else {'int': {'by count': 1}}
        self.keyspace = keyspace
        self.table_name = table_name
        self.columns_list = ['id varint']
        self.column_names_list = []
        self.pk_list = ['id']
        self.cl_list = []
        self.table_options = table_options
        self.all_types_table = all_types_table
        self.materialized_views = {}

    def _convert_type_to_preffix(self, ctype, c_def):
        """
        Function defines prefix for the column name (by user request)
        :param ctype: current column type
        :type ctype: str
        :param c_def: dictionary with column definition (from self.columns)
        :type c_def: dict
        :return: column prefix
        :rtype: str
        """
        if ('names' in c_def and len(c_def['names']) == c_def['amount']):
            return ''
        elif 'names' in c_def and len(c_def['names']) != c_def['amount']:
            logger.debug('Names amount does not coincides with coulmns amount. Asked create {0} columns, supplied {1} names'
                         .format(c_def['amount'], len(c_def['names'])))

        return c_def['prefix'] if 'prefix' in c_def else \
            '{clmn_prefix}_{clmn_suffix}'.format(clmn_prefix=self.CLMN_PREFIX,
                                                 clmn_suffix=''.join([s[0] for s in ctype.split('<')]) if '<' in ctype else ctype)

    def _set_column_as_key(self, definition_dict, set_list, c_type, clmn_name, exclude_list=None):
        """
        Function creates list of PRIMARY KEYS and CLUSTERING KEYS
        :param definition_dict: dictionary with columns for PRIMARY KEYS (as it described in the __init__ function)
        :type definition_dict: dict
        :param set_list: dictionary with columns for CLUSTERING KEYS (as it described in the __init__ function)
        :type set_list: list
        :param c_type: current column type
        :type c_type: str
        :param clmn_name: current column name
        :type clmn_name: str
        :param exclude_list: list with column names. If current column is already in this list, it won't be added to the set_list
        :type exclude_list: list
        :return: None
        """
        if not exclude_list or (exclude_list and clmn_name not in exclude_list):
            if c_type in definition_dict:
                if 'by name' in definition_dict[c_type] and definition_dict[c_type]['by name'] and not set(definition_dict[c_type]['by name']).issubset(set_list):
                    set_list.extend(definition_dict[c_type]['by name'])
                elif 'by count' in definition_dict[c_type] and definition_dict[c_type]['by count'] > 0:
                    set_list.append(clmn_name)
                    definition_dict[c_type]['by count'] -= 1

    def create_table(self):
        # TODO: setup if all_types_table is True
        self._create_columns_list()
        # TODO: add options (like WITH CLUSTERING ORDER BY and others) to the table statement
        # TODO: decide if columns order is important for PK and CK
        statement = 'CREATE TABLE IF NOT EXISTS {keyspace_name}.{table_name} ' \
                    '({columns_definition}, PRIMARY KEY(({pks}){cls}))'\
            .format(keyspace_name=self.keyspace,
                    table_name=self.table_name,
                    columns_definition=', '.join([s for s in self.columns_list]),
                    pks=', '.join([s for s in self.pk_list]),
                    cls=', {}'.format(', '.join([s for s in self.cl_list])) if self.cl_list else '')
        if self.table_options:
            statement = statement + ' WITH'
            for op, value in self.table_options.items():
                statement = '{} {} = {}'.format(statement, op, value)
        logger.debug(statement)
        self.session.execute(statement)

    def _create_columns_list(self):
        # TODO: add UDT
        for c_type, c_def in self.columns_dict.items():
            preffix = self._convert_type_to_preffix(c_type, c_def)
            for i in range(int(c_def['amount'])):
                clmn_name = '{clmn_prefix}{num}' .format(clmn_prefix=preffix, num=i) if preffix else c_def['names'][i]

                self.columns_list.append('{clmn_name} {frozen}{clmn_type}'
                                         .format(clmn_name=clmn_name, clmn_type=c_type,
                                                 frozen='frozen ' if c_def['frozen'] else ''))
                # Select column as PRIMARY KEY
                self._set_column_as_key(self.pk_columns_dict, self.pk_list, c_type, clmn_name)
                # Select column as CLUSTERING
                self._set_column_as_key(self.cl_columns_dict, self.cl_list, c_type, clmn_name,
                                        exclude_list=self.pk_list)
        self.column_names_list = [c.split(' ')[0] for c in self.columns_list]

    def prefill_table(self, rows, data=None, start_id_from=0, consistency=ConsistencyLevel.QUORUM, using=None, flush=True,
                      delay=0):
        """
        Function pre-fill the table(named as value of self.table_name) with requested rows
        :param rows: how many rows should be in the table
        :type rows: varint
        :return:
        """
        time.sleep(delay)
        data_arr = self._create_data_array(rows, ready_data=data)
        using_str = ' USING {0} {1}'.format(using.keys()[0], using[using.keys()[0]]) if using else ''
        st = 'INSERT INTO {ks}.{table_name} ({columns}) VALUES ({values}){using}'\
            .format(ks=self.keyspace, table_name=self.table_name,
                    columns=', '.join([c.split(' ')[0] for c in self.columns_list]),
                    values=('?,'*len(self.columns_list))[:-1], using=using_str)
        logger.debug(st)
        statement = self.session.prepare(st)
        statement.consistency_level = consistency

        execute_concurrent_with_args(self.session, statement,
                                     map(lambda k: [k+start_id_from]+[data_arr[t][k] for t in range(0, len(data_arr))],
                                         [k for k in range(0, rows)]))
        if flush:
            flush_by_node(self.cluster)

        logger.debug('Finish prefill')

    def multiple_deletes(self, filters, delay=0):
        """
        :param filter: {<column_name1>: [<value1>,<value2>,..] <column_name2>: [<value1>,<value2>,..], ..}
        """
        time.sleep(delay)
        for i in range(0, len(next(iter(filters.values())))):
            filter = {}
            for column, values in filters.items():
                filter.update({column: values[i]})
            self.delete_row(filter)

    def delete_row(self, filter):
        """
        :param filter: {<column_name1>: <value>, <column_name2>: <value>, ..}
        """
        where_statement = ['{0}={1}'.format(column, self.prepare_value(str(value))) for column, value in filter.items()]
        query = 'delete from {tbl} where {where}'.format(
            tbl=self.table_name, where=' and '.join(s for s in where_statement))
        logger.debug(query)
        self.session.execute(query)

    def truncate_table(self):
        query = 'truncate table {tbl}'.format(tbl=self.table_name)
        logger.debug(query)
        self.session.execute(query)

    def _create_data_array(self, rows, ready_data=None):
        def _get_random(dupl):
            if ready_data and c_type in ready_data and dupl:
                return ready_data[c_type]
            data = []
            value = None
            # TODO: add UDT and collection types
            for i in range(0, dupl):
                if 'int' in c_type:
                    value = random.randint(c_def['value length']['min'], c_def['value length']['max'])
                elif c_type in ['text', 'ascii', 'varchar']:
                    value = ''.join(random.choice(string.ascii_lowercase)
                                    for _ in range(c_def['value length']['min'], c_def['value length']['max']))
                elif c_type in ['float', 'decimal', 'double']:
                    value = random.uniform(c_def['value length']['min'], c_def['value length']['max'])
                elif c_type == 'decimal':
                    value = random.uniform(c_def['value length']['min'], c_def['value length']['max'])
                elif c_type == 'boolean':
                    value = 'true'
                elif c_type == 'blob':
                    pass
                elif c_type == 'timestamp':
                    pass
                elif c_type == 'timeuuid':
                    pass
                elif c_type == 'uuid':
                    pass
                elif c_type == 'time':
                    pass
                elif c_type == 'date':
                    pass
                elif c_type == 'inet':
                    pass
                data.append(value)
            return data

        data_array = {}
        dupl = 10 if not ready_data else len(ready_data)
        for c_type, c_def in self.columns_dict.items():
            if 'value length' not in c_def:
                c_def['value length'] = {'min': self.DEFAULT_MIN_LENGTH, 'max': self.DEFAULT_MAX_LENGTH}
            data_array[c_type] = _get_random(dupl)*(rows//dupl) + _get_random(rows % dupl)
        data_array = [data_array[i] for i in [c.split(' ')[1] for c in self.columns_list[1:]]]
        return data_array

    def update_table(self, set_clause, where_filter, using_clause=None, consistency_level=None,
                     delay=0, update_columns_exclude=None):
        """
        :param set_clause: which columns should by updated with wich value
                            {'by type': {'int': <new value>, 'text': '<new value>'},
                             'by name': {name1: value, name2: value}}
        :type set_clause: dict
        :param where_filter: dictionary. Will used to build WHERE clause fo the UPDATE statement.
                        [{'by type': {'int': {'value': <value>, 'operator': < =/!=/</>/in >}}},
                       {'by name': {name1: {'value': <value> or [<value>,<value> - in cas when operator in/between], 'operator': < =/!=/</>/in/between >}}}]
        :type where_filter: dict
        :param using_clause: {<option>: <value>}
        :type using_clause: dict
        :param consistency_level:
        :type consistency_level:
        :param delay: delay before start, in seconds. Default: 0
        :type delay: int
        :param update_columns_exclude: list with column names that should by excluded from set clause: [name1, name2]
        :type update_columns_exclude: list
        :return: (set_list, filter_list)
        :rtype: tuple of lists
        """
        # TODO: add - select columns from materialized views
        if delay:
            time.sleep(delay)
        set_dict = self._bulid_set_clause(set_clause, exclude_columns=update_columns_exclude or [])
        filter_dict = self._build_filter(where_filter)

        if filter_dict:
            set_str = ' and '.join('{0} = {1}'.format(name, self.prepare_value(value))
                                   for name, value in set_dict.items())
            filter_str = ' and '.join('{0} {1} {2}'.format(name, value['operator'],
                                                           '({})'.format(
                                                               ', '.join([self.prepare_value(str(i)) for i in value['value']]))
                                                           if isinstance(value['value'], list) and value['operator'] == 'in' else
                                                           self.prepare_value(value['value']))for name, value in filter_dict.items())
            using_str = ' USING {0} {1}'.format(
                list(using_clause.keys())[0], using_clause[list(using_clause.keys())[0]]) if using_clause else ''

            statement = 'UPDATE {ks}.{table_name}{using} SET {set_clause} WHERE {filter}' \
                .format(ks=self.keyspace, table_name=self.table_name,
                        using='' if not using_clause else using_str,
                        set_clause=set_str, filter=filter_str)
            self.session.execute(statement)
            return (set_dict, filter_str)
        return (None, None)

    def multiple_int_updates_by_id(self, update_to_boundaries, filter_values=[], ids=[],
                                   updated_columns=None, updates=100, same_id=True, delay=0):
        time.sleep(delay)
        query = 'select * from {}'.format(self.table_name)
        updated_columns = updated_columns or [c for c in self.column_names_list
                                              if '{} int'.format(c) in self.columns_list
                                              and c not in self.pk_list+self.cl_list]

        res = list(self.session.execute(query + ' LIMIT 1'))
        updated_column = updated_columns[random.randint(0, len(updated_columns) - 1)]
        updated_column_index = [i for i, clmn in enumerate(res[0]._fields) if clmn == updated_column][0]
        id = None
        id_condition = False if not same_id else None
        res = list(self.session.execute(query))
        for _ in range(updates):
            # Select column for update
            if not ids:
                k = 0
                if not same_id:
                    res = list(self.session.execute(query))
                while not id_condition:
                    i = random.randint(0, len(res)-1)
                    if (filter_values and res[i][updated_column_index] in filter_values) or not filter_values:
                        id = res[i].id
                        id_condition = False if not same_id else id
                        break
                    k += 1
                    if k > len(res):
                        break
            else:
                id = ids[random.randint(0, len(ids) - 1)]

            if id is not None:
                self.update_table(set_clause={'by name': {updated_column: random.randint(update_to_boundaries[0],
                                                                                         update_to_boundaries[1])}},
                                  where_filter={'by name': {'id': {'operator': '=', 'value': id}}})

        logger.debug('Updates finished')

    def select_all_mvs(self, reads=100, by_id=False):
        logger.debug('Start reads from MVs')
        statement_template = 'select * from {0}'
        if by_id:
            max_id = self.get_max_id()
            statement_template = statement_template + ' where id={1}'
        else:
            statement_template = statement_template + ' LIMIT 10'

        for _ in range(0, reads):
            i = random.randint(0, len(self.materialized_views)-1)
            mv_name = [name for j, name in enumerate(self.materialized_views.keys()) if j == i][0]
            statement = statement_template.format(mv_name, random.randint(0, max_id)) if by_id else \
                statement_template.format(mv_name)
            logger.debug(statement)
            self.session.execute(statement)
        logger.debug('Finish reads from MVs')

    def prepare_value(self, value):
        try:
            _ = int(value)
            return value
        except ValueError:
            return '\'{}\''.format(value)

    def _build_filter(self, where_filter):
        clause = {}
        f, by = (where_filter['by name'], 'name') if 'by name' in where_filter else (where_filter['by type'], 'type')
        for clmn, value in f.items():
            name = ''
            if by == 'name':
                if clmn in self.pk_list+self.cl_list:
                    name = clmn
            elif by == 'type':
                name = self._get_column_by_type(clmn, include_list=self.pk_list+self.cl_list)

            if name:
                clause.update({name: value})

        return clause

    def _bulid_set_clause(self, set_clause, exclude_columns=None):
        clause = {}
        for utype, udef in set_clause.items():
            s, by = udef, utype.replace('by ', '')
            # s, by = (clause['by name'], 'name') if 'by name' in clause else (clause['by type'], 'type')
            for name, new_value in s.items():
                update_item = ''
                if by == 'name':
                    if [n for n in self.columns_list if '{} '.format(name) in n] \
                            and name not in self.pk_list + self.cl_list + list(clause.keys()):
                        update_item = name
                elif by == 'type':
                    update_item = self._get_column_by_type(
                        name, exclude_list=self.pk_list + self.cl_list + list(clause.keys()) + exclude_columns)
                if update_item:
                    clause.update({update_item: new_value})
        return clause

    def _get_column_by_type(self, type, include_list=None, exclude_list=None):
        exclude_list = exclude_list or []
        include_list = include_list or []
        for clmn in self.columns_list:
            name = clmn.split(' ')[0]
            if ' {}'.format(type) in clmn and name not in exclude_list and (not include_list or (include_list and name in include_list)):
                return name
        return None

    def get_value_for_filter(self, row_index=0):
        statement = 'SELECT {pks} FROM {ks}.{table_name}'.format(ks=self.keyspace,
                                                                 table_name=self.table_name, pks=', '.join(name for name in self.pk_list+self.cl_list))

        res = self.session.execute(statement)
        result = {}
        for i, name in enumerate(list(res.current_rows[row_index]._fields)):
            result.update({name: {'value': res.current_rows[row_index][i], 'operator': '='}})
        return result

    def get_max_id(self):
        id = self.session.execute('select max(id) as id from {}'.format(self.table_name)).current_rows[0].id
        return 0 if not id else id

    def set_mv(self, mv_name, mv_self_arr):
        self.materialized_views[mv_name] = mv_self_arr

    def remove_mv(self, mv_name):
        del self.materialized_views[mv_name]


class MaterializedViewManager(object):
    """Class provides interface to create and manage materialized views"""
    TEMPLATE_MV_NAME = '{0}_mv_{1}'

    def __init__(self, parent_table, mv_name=None):
        self.parent_table = parent_table
        self.mv_name = mv_name or self.TEMPLATE_MV_NAME.format(self.parent_table.table_name, 0)
        self.mv_columns_list = None
        self.mv_pk_list = None
        self.mv_cl_list = None
        self.mv_where_clause = None
        self.mv_options = None
        if self.mv_name in self.parent_table.materialized_views:
            if mv_name:
                self.mv_columns_list = self.parent_table.materialized_views.mv_columns_list
                self.mv_pk_list = self.parent_table.materialized_views.mv_pk_list
                self.mv_cl_list = self.parent_table.materialized_views.mv_cl_list
                self.mv_where_clause = self.parent_table.materialized_views.mv_where_clause
                self.mv_options = self.parent_table.materialized_views.mv_options
            else:
                mv_index = max([int(mv.split('_')[-1]) for mv in self.parent_table.materialized_views.keys()])+1
                self.mv_name = self.TEMPLATE_MV_NAME.format(self.parent_table.table_name, mv_index)

    # TODO: add possibility for PK and CL order
    def create_materialized_view(self, mv_columns=None, mv_pk_column=None, mv_cl_column=None, mv_where_restriction=None,
                                 options=None, wait_for_view_built=True):
        """
        :param mv_columns: {<column type>: {  'amount': <how many columns with this type>,
                                            'names': [columns names, comma separated]
                                           }
                                    If columns name is provided, expected:
                                    - name for existent column of this type(by <how many columns with this type> parameter).
                                    OR
                                    if 'names' isn't provided, random existent column of this type will be selected
                            EXAMPLES:
                                1) {'int': {'amount': 2}, 'float': {'amount': 1}}
                                2) {'text': {'names': ['c1', 'c2']}}
        :param mv_columns: dict
        :param mv_pk_column: define which column should be added to the table primary key
                            Expected dict structure - by type or by name:
                            {'type': <random column with this type will be selected from table columns>}
                            OR
                            {'names': <existent column name>}
                            EXAMPLE:
                            {'type': 'text'} OR {'name': 'c1'}
        :type mv_pk_column: dict
        :param mv_where_restriction: define restriction in the WHERE clause if it's NOT NULL condition.
                                    Expected dict structure:
                                    {'names': {<column name>: {'operator': 'operator: =/in',
                                                               'value': <value according to column type OR LIST of values in case "in" operator>}}}
                                    OR
                                    {'position': {<position in the PK+CL list>: {'operator': 'operator: =/in',
                                                               'value': <value according to column type OR LIST of values in case "in" operator>}}}
                                    EXAMPLE:
                                    {'names': {'clmn_int0: {'operator': '>', 'value': 1}}}
                                    OR
                                    {'position': {-1: {'operator': 'in', 'value': [1, 2, 3]}}}
        :type mv_where_restriction: dict
        :return:
        """
        if self.mv_name not in self.parent_table.materialized_views:
            self.mv_columns_list = self._create_mv_columns_list(mv_columns) or ['*']
            self.mv_pk_list = self.create_mv_pk_list(mv_pk_column)
            self.mv_cl_list = self.parent_table.cl_list or []
            self.mv_where_restriction = self._restriction_list(mv_where_restriction)
            self.mv_where_clause = ' and '.join(['{} IS NOT NULL'.format(c) for c in self.mv_pk_list+self.mv_cl_list
                                                 if c not in self.mv_where_restriction])
            if self.mv_where_restriction:
                where_str = ' and '.join(['{0} {1} {2}'.format(name, value['operator'],
                                                               '({})'.format(', '.join([self.parent_table.prepare_value(str(i))
                                                                                        for i in value['value']])
                                                                             if isinstance(value['value'], list) else value['value'])
                                                               if value['operator'] == 'in' else value['value'])
                                          for name, value in self.mv_where_restriction.items()])
                self.mv_where_clause = '{0} and {1}'.format(self.mv_where_clause, where_str)
            # TODO: add option to filter
            # TODO: ADD CLUSTERING OPTION
            # self.mv_options =
            statement = "CREATE MATERIALIZED VIEW {ks}.{mv_name} AS SELECT {mv_columns} FROM {ks}.{table_name} " \
                "WHERE {where_clause} PRIMARY KEY ({pk}{cl})".format(
                    mv_name=self.mv_name,
                    ks=self.parent_table.keyspace,
                    mv_columns=', '.join([k for k in self.mv_columns_list]),
                    table_name=self.parent_table.table_name,
                    where_clause=self.mv_where_clause,
                    pk=', '.join([k for k in self.mv_pk_list]),
                    cl='' if not self.parent_table.cl_list or set(self.parent_table.cl_list).issubset(self.mv_pk_list)
                    else ', {}'.format(', '.join([k for k in self.mv_cl_list])))
            logger.debug(statement+';')
            self.parent_table.session.execute(statement)

            if wait_for_view_built:
                wait_for_view(cluster=self.parent_table.cluster, session=self.parent_table.session,
                              ks=self.parent_table.keyspace, view=self.mv_name)

            if options:
                for op, value in options.items():
                    self.parent_table.session.execute('ALTER MATERIALIZED VIEW {ks}.{mv_name} WITH {op} = {value}'.format
                                                      (ks=self.parent_table.keyspace, mv_name=self.mv_name,
                                                       op=op, value=value))
            logger.debug('Materialized view {} has been created'.format(self.mv_name))
            self.parent_table.set_mv(self.mv_name, self)

    def drop_mv(self):
        logger.debug('Start drop materialized view {}'.format(self.mv_name))
        self.parent_table.session.execute('drop materialized view {}'.format(self.mv_name))
        logger.debug('Finish drop materialized view {}'.format(self.mv_name))
        self.parent_table.remove_mv(mv_name=self.mv_name)
        self.mv_name = ''
        self.mv_columns_list = None
        self.mv_pk_list = None
        self.mv_cl_list = None
        self.mv_where_clause = None
        self.mv_options = None

    def _restriction_list(self, mv_where_restriction):
        restriction_dict = {}
        if mv_where_restriction:
            pk_list = self.mv_pk_list+self.mv_cl_list
            if 'names' in mv_where_restriction:
                restriction_dict.update(mv_where_restriction['names'])
            elif 'position' in mv_where_restriction:
                for position, r_def in mv_where_restriction['position'].items():
                    if len(pk_list) > position:
                        restriction_dict.update({pk_list[position]: r_def})

        return restriction_dict

    def _create_mv_columns_list(self, mv_columns, exclude_list=None):
        """
        :param mv_columns: as described in create_materialized_view.mv_columns
        :param mv_columns: dict
        :param exclude_list: list of column that already selected and should be excluded from new list
        :param exclude_list: list
        :return:
        :rtype: list
        """
        # TODO: add UDT
        if not mv_columns:
            return mv_columns

        exclude_list = exclude_list or []
        mv_columns_list = []
        mv_columns_list.extend(list(itertools.chain.from_iterable([self._build_columns_list(c_type, c_def, mv_columns_list+exclude_list)
                                                                   for c_type, c_def in mv_columns.items()])))
        return mv_columns_list

    def _build_columns_list(self, c_type, c_def, exclude_list):
        column_names = []
        if 'names' in c_def:
            column_names = [clmn for clmn in c_def['names'] if '{0} {1}'.format(clmn, c_type) in self.parent_table.columns_list
                            and clmn not in column_names + exclude_list]
        else:
            for i in range(c_def['amount']):
                column_names.append([clmn.split(' ')[0] for clmn in self.parent_table.columns_list if ' {}'.format(c_type) in clmn
                                     and clmn.split(' ')[0] not in column_names + exclude_list][0])
        return column_names

    def create_mv_pk_list(self, mv_pk_column):
        """
        Just one non-primary key column can be added to the PK in the materialized view
        :param mv_pk_column: define which column should be added to the table primary key
                            Expected dict structure - by type or by name:
                            {'type': <random column with this type will be selected from table columns>}
                            OR
                            {'names': <existent column name>}
                            EXAMPLE:
                            {'type': 'text'} OR {'name': 'c1'}
        :param mv_pk_column: dict
        :return:
        :rtype: list
        """
        mv_pk_column_list = deepcopy(self.parent_table.pk_list)
        if 'names' in mv_pk_column:
            for name in mv_pk_column['names']:
                if name in self.parent_table.column_names_list and name not in mv_pk_column_list:
                    mv_pk_column_list.append(name)
        elif 'type' in mv_pk_column:
            clmns = [clmn.split(' ')[0] for clmn in self.parent_table.columns_list if ' {}'.format(mv_pk_column['type']) in clmn
                     and clmn.split(' ')[0] not in mv_pk_column_list+(self.parent_table.cl_list or [])]
            if not clmns:
                logger.debug('ERROR: new column for Materialized View PK is not found. Received parameters: {}'.format(
                    mv_pk_column['type']))
            else:
                mv_pk_column_list.append(clmns[0])
        return mv_pk_column_list

    def my_count_query(self, filter=None):
        # TODO: handle filter
        return 'SELECT COUNT(*) FROM {my_name}{where_clause}'.format(my_name=self.mv_name, where_clause=filter or '')


def run_in_parallel(functions_list):
    """
        Runs the functions that are passed in proc_functions in parallel using threads.
        :param functions_list: variable holds list of dictionaries with threads definitions. Expected structure:
                               [{'func': <function pointer - the function will be runs from the thread>,
                                 'args': (arg1, arg2, arg3), - explicit function arguments by order in the function
                                 'kwargs': {<arg name1>: value, <arg name2>: value} - function arguments by name
                                }, - first thread definition
                                {{'func': <function pointer, 'args': (), 'kwargs': {}} - second thread, no arguments
                               ]
        :param functions_list: list
        :return: list of functions' return values
        :rtype: list
    """
    logger.debug('Threads start at {}'.format(datetime.datetime.now()))
    pool = ThreadPoolExecutor(max_workers=len(functions_list))
    tasks = []
    for func in functions_list:
        args = func['args'] if 'args' in func else []
        kwargs = func['kwargs'] if 'kwargs' in func else {}
        tasks.append(pool.submit(func['func'], *args, **kwargs))
    results = [task.result() for task in tasks]
    logger.debug("'{}' threads finished at {}".format(len(results), datetime.datetime.now()))
    return results


def view_built_status_query(ks='', view='', select_column='status'):
    query = "SELECT {} FROM system_distributed.view_build_status".format(select_column)
    if ks or view:
        query = "{} WHERE ".format(query)
        where = ' AND '.join(['{0} = \'{1}\''.format(k, v)
                              for k, v in {'keyspace_name': ks, 'view_name': view}.items() if v])
        if ks:
            query = "{0} {1}".format(query, where)
    return query


def get_index_view_name(index_name):
    return '{}_index'.format(index_name)


def get_view_id(session, keyspace_name, view_name):
    res = session.execute('select id from system_schema.views where keyspace_name=\'{0}\' and view_name=\'{1}\''
                          .format(keyspace_name, view_name))
    assert res, 'Secondary index view named {} has not built'.format(view_name)
    return rows_to_list(res)[0][0]


def index_is_built(cluster, session, ks_name, table_name, index_name, raise_exception=True):
    wait_for_view(cluster, session, ks_name, get_index_view_name(index_name), raise_exception=raise_exception)
    return len(list(session.execute(
        "SELECT * FROM system_schema.indexes WHERE keyspace_name = '{0}' and table_name ='{1}' AND index_name='{2}'".format(ks_name, table_name, index_name)))) == 1

# wait_for_view waits for the given materialized view to have been built on
# all *living* nodes.
# In this implementation, nodes which are not alive may or may not have
# finished building the view when wait_for_view returns. This was a deliberate
# implementation choice - we also know the state of the build for dead nodes,
# but waiting only for live nodes makes it easier to write tests which check
# how view building and dead nodes interact.


def wait_for_view(cluster, session, ks, view, raise_exception=True):
    logger.debug("Waiting for view {}.{} to finish building...".format(ks, view))

    def _view_build_finished_on_live_nodes():
        done = set()
        for entry in rows_to_list(session.execute(view_built_status_query(ks, view, 'host_id,status'))):
            if entry[1] == 'SUCCESS':
                done.add(entry[0])
        for node in cluster.nodelist():
            try:
                if node.is_live() and not (UUID(node.hostid()) in done):
                    return False
            except NodetoolError:
                # If we decomissioned a node with "nodetool decommission"
                # the code above may temporarily think that node.is_alive()
                # is still true, but node.hostid(), which calls nodetool,
                # can fail with an exception. In this case we just need to
                # consider this node non-live.
                pass
        return True

    # wait for up to 5 minutes for view building to finish
    for trial in range(60):
        if _view_build_finished_on_live_nodes():
            return
        time.sleep(5)

    error_msg = "View {}.{} not built".format(ks, view)
    if raise_exception:
        raise Exception(error_msg)
    else:
        logger.debug(error_msg)


def wait_for_view_build_start(session, ks, view, seconds_to_wait=20):

    def _check_build_started():
        result = rows_to_list(session.execute("SELECT last_token FROM system.views_builds_in_progress "
                                              "WHERE keyspace_name='{0}' AND view_name='{1}'".format(ks, view)))
        return result != [[None]]

    logger.debug("Ensure view building started.")
    start = time.time()
    while not _check_build_started():
        if time.time() - start > seconds_to_wait:
            raise Exception("View building didn't start in {} seconds".format(seconds_to_wait))


def wait_for_schema_agreement(session):
    rows = list(session.execute("SELECT schema_version FROM system.local"))
    local_version = rows[0]

    all_match = True
    rows = list(session.execute("SELECT schema_version FROM system.peers"))
    for peer_version in rows:
        if peer_version != local_version:
            all_match = False
            break

    if all_match:
        return
    else:
        time.sleep(1)
        wait_for_schema_agreement(session)


def remove_node(cluster, node, wait_other_notice=True, other_nodes=None):
    hostid = node.hostid()
    cluster.remove(node, wait_other_notice=wait_other_notice, other_nodes=other_nodes)
    remove_using_node = cluster.nodelist()[0]
    remove_using_node.nodetool("removenode {}".format(hostid))


def copy_files_to(from_dir, to_dir, files_only=False, create_to_dir=False):
    """
    Copy files from `from_dir` to `to_dir`, optionally create `to_dir`

    :param files_only: if true, only copy files and ignore sub directories
    :param create_to_dir: if true, create `to_dir` if it doesn't exist
    """
    if create_to_dir and not os.path.exists(to_dir):
        os.makedirs(to_dir)
    for f in os.listdir(from_dir):
        if os.path.isfile(os.path.join(from_dir, f)):
            shutil.copy2(os.path.join(from_dir, f), os.path.join(to_dir, f))
        elif not files_only:
            shutil.copytree(os.path.join(from_dir, f), os.path.join(to_dir, f))


def get_entity_id(session, table_or_view, keyspace_name, entity_name):
    system_table = table_or_view + 's'
    query = "SELECT id FROM system_schema.{system_table} WHERE keyspace_name='{keyspace_name}' " \
            "and {table_or_view}_name='{entity_name}'".format(**locals())
    entity_id = rows_to_list(session.execute(query))
    return entity_id[0][0]


def get_truncated_time_from_system_local(session):
    query = "SELECT truncated_at FROM system.local"
    truncated_time = rows_to_list(session.execute(query))
    return truncated_time


def get_truncated_time_from_system_truncated(session, table_id):
    query = "SELECT truncated_at FROM system.truncated WHERE table_uuid={}".format(table_id)
    truncated_time = rows_to_list(session.execute(query))
    return truncated_time[0]


def get_rows_set_from_res(res):
    return set([tuple(res_list) for res_list in rows_to_list(res)])


class CassandraCluster(object):
    """Class provides interface to create Cassandra cluster and migrate the data from Scylla"""

    def __init__(self, cassandra_version, request):
        self.cassandra_version = cassandra_version
        self.request: pytest.FixtureRequest = request
        self.dtest_config = DTestConfig()
        self.dtest_config.setup(self.request)
        self.dtest_config.cassandra_version = cassandra_version
        self.dtest_setup = DTestSetup(dtest_config=self.dtest_config,
                                      setup_overrides=DTestSetupOverrides(),
                                      cluster_name="test")
        self.test_path = None
        self.cluster = None
        self.scylla_data_tmp_folder = None
        self.scylla_schema_ddl = None
        self.ddl_obj = None
        self.folders_tree = None
        self.scylla_cluster = None
        logger.debug('\n=============== Create Cassandra cluster ====================\n')

    def create_and_start_cluster(self, nodes=1, config_options=None):
        # Stop Scylla cluster before create new Cassandra cluster because of it's impossible to run two clusters simultaneously
        if self.scylla_cluster:
            self.scylla_cluster.stop(wait_other_notice=True)
        # Set up Cassandra cluster
        self.dtest_setup.initialize_cluster(DTestSetup.create_ccm_cluster)
        self.request.addfinalizer(self.tear_down)
        self.cluster = self.dtest_setup.cluster
        self.cluster.set_configuration_options(values=config_options)
        logger.debug("Starting a Cassandra cluster of {} node(s) with options {}...".format(nodes, config_options))
        self.cluster.populate(nodes)
        self.cluster.start(wait_for_binary_proto=True, wait_other_notice=True)
        self.test_path = self.dtest_setup.test_path
        return self.cluster.nodelist()[0]

    def get_scylla_test_schema_ddl(self, keyspace_names_list=None, table_names_list=None, get_system_keyspaces=None):
        self.ddl_obj = SchemaDDL(node=self.scylla_cluster.nodelist()[0],
                                 keyspace_names_list=keyspace_names_list,
                                 table_names_list=table_names_list,
                                 get_system_keyspaces=get_system_keyspaces)
        self.scylla_schema_ddl = self.ddl_obj.get_schemas_ddl()

    def create_entites_list(self, obj, func_for_empty):
        """
        :param obj: the object should be converted to list if not empty
        :type obj: any
        :param func_for_empty: if obj is empty, run this function to receive the value. Expected list, where
                            first element is function, and second is parameter for the function
        :type  func_for_empty: list
        :return:
        """
        out = obj
        if obj and not isinstance(obj, list):
            out = [obj]
        if not obj:
            if func_for_empty[1]:
                out = func_for_empty[0](func_for_empty[1])
            else:
                out = func_for_empty[0]()
        return out

    def create_data_folders_tree(self, keyspace_names_list=None, table_names_list=None):
        """
        create dictionary with keyspace(s) and their table(s) that its data will be migrated
        """
        self.folders_tree = {}
        keyspace_names_list = self.create_entites_list(obj=keyspace_names_list, func_for_empty=[
                                                       self.ddl_obj.get_keyspaces, None])

        for keyspace_name in keyspace_names_list:
            self.folders_tree[keyspace_name] = []
            table_names_list = self.create_entites_list(obj=table_names_list, func_for_empty=[self.ddl_obj.get_entity_list,
                                                                                              "select table_name from system_schema.tables where keyspace_name='{}'".format(keyspace_name.replace('"', ''))])
            for table_name in table_names_list:
                self.folders_tree[keyspace_name].append(table_name)

    def get_table_folder(self, base_path, node, keyspace_name, table_name, create=False):
        keyspace_name = keyspace_name.replace('"', '')
        table_name = table_name.replace('"', '')
        ks_dir = os.path.join(base_path, 'test', node.name, 'data', keyspace_name)
        if create:
            the_folder = os.path.join(ks_dir, '{}-tmp'.format(table_name))
            os.makedirs(the_folder)
        else:
            the_folder = get_cf_dir(ks_dir, table_name)
        return the_folder

    def copy_scylla_test_data_to_tmp(self, scylla_test_path, keyspace_names_list=None, table_names_list=None, nodes=None):
        self.scylla_cluster.flush()
        self.create_data_folders_tree(keyspace_names_list, table_names_list)
        self.scylla_data_tmp_folder = os.path.join('/tmp', scylla_test_path.split('/')[-1])
        os.makedirs(self.scylla_data_tmp_folder)
        logger.debug("Create {} test folder".format(self.scylla_data_tmp_folder))
        self.copy_table_data_all_nodes(from_base_path=scylla_test_path, to_base_path=self.scylla_data_tmp_folder,
                                       create_to_folder=True, nodes=nodes)

    def copy_table_data_all_nodes(self, from_base_path, to_base_path, nodes=None, create_to_folder=False):
        logger.debug('Copy Scylla test data files')
        for node in nodes:
            for ks, tables in self.folders_tree.items():
                for table in tables:
                    copy_from = self.get_table_folder(base_path=from_base_path, node=node,
                                                      keyspace_name=ks, table_name=table)
                    copy_to = self.get_table_folder(base_path=to_base_path, node=node,
                                                    keyspace_name=ks, table_name=table, create=create_to_folder)
                    logger.debug(
                        'Copy data files for {ks}.{table} table: from {copy_from} to {copy_to}'.format(**locals()))
                    copy_files_to(from_dir=copy_from, to_dir=copy_to, files_only=True)

    def copy_scylla_data_to_cassandra(self, nodes=None):
        self.copy_table_data_all_nodes(from_base_path=self.scylla_data_tmp_folder,
                                       to_base_path=self.test_path, nodes=nodes)

    def create_test_schema(self, node):
        for ks, cmds in self.scylla_schema_ddl.items():
            logger.debug('Create keyspace {} with all entities'.format(ks))
            out = node.run_cqlsh(cmds=';'.join(cmd for cmd in cmds), return_output=True)
            if out[1]:
                raise Exception('Create test schema failure: {}'.format(out[1]))

    def migrate_data_to_cassandra(self, nodes):
        for node in nodes:
            for ks, tables in self.folders_tree.items():
                for table in tables:
                    logger.debug('Start data migration from Scylla to Cassandra for {}.{} table'.format(ks, table))
                    # If the keyspace/table names are case sensitive, we have to use double quotes. And nodetool refresh
                    # can't recognize it. So we need to remove double quotes to be able to run the refresh
                    node.nodetool("refresh -- {} {}".format(ks.replace('"', ''), table.replace('"', '')))

        for node in nodes:
            node.flush()

    def run_migration(self, scylla_cluster, scylla_test_path, keyspace_names_list=None, table_names=None, nodes='ALL'):
        self.scylla_cluster = scylla_cluster
        if not self.scylla_cluster:
            logger.debug('Missed Scylla cluster. Migration can''t be run')
            return
        self.get_scylla_test_schema_ddl(keyspace_names_list=keyspace_names_list, table_names_list=table_names)

        # Node(s) for Scylla cluster
        nodes_list = self.scylla_cluster.nodes.values() if nodes == 'ALL' else [self.scylla_cluster.nodes.values()[0]]

        self.copy_scylla_test_data_to_tmp(scylla_test_path=scylla_test_path, keyspace_names_list=keyspace_names_list,
                                          table_names_list=table_names, nodes=nodes_list)

        node1 = self.create_and_start_cluster(nodes=len(self.scylla_cluster.nodes.values()),
                                              config_options={'hinted_handoff_enabled': False})

        # Node(s) for Cassandra cluster
        nodes_list = self.cluster.nodelist() if nodes == 'ALL' else [node1]

        self.create_test_schema(node=nodes_list[0])
        self.copy_scylla_data_to_cassandra(nodes=nodes_list)
        self.migrate_data_to_cassandra(nodes=nodes_list)
        return node1

    def tear_down(self):
        logger.debug('Remove temporary folder with Scylla data')
        if self.scylla_data_tmp_folder and os.path.exists(self.scylla_data_tmp_folder):
            shutil.rmtree(self.scylla_data_tmp_folder)
        logger.debug('Stopping Cassandra cluster')
        if self.cluster:
            self.cluster.stop(wait_other_notice=True)

        dtest_setup = self.dtest_setup
        for con in dtest_setup.connections:
            con.cluster.shutdown()
        dtest_setup.connections = []

        rep_setup = getattr(self.request.node, "rep_setup", None)
        rep_call = getattr(self.request.node, "rep_call", None)
        failed = getattr(rep_setup, 'failed', False) or getattr(rep_call, 'failed', False)
        try:
            if not dtest_setup.allow_log_errors:
                try:
                    dtest_setup.check_errors_all_nodes()
                except AssertionError:
                    failed = True
                    raise
        finally:
            try:
                # save the logs for inspection
                if (failed and self.dtest_config.delete_logs == 'passed') or self.dtest_config.delete_logs == 'none':
                    from dtest_setup import copy_logs
                    copy_logs(self.request, dtest_setup)
            except Exception as e:
                logger.error("Error saving log: %s", str(e))
            finally:
                dtest_setup.cleanup_cluster()


class SchemaDDL(object):
    """Class provides interface to fetch schema DDL"""

    def __init__(self, node, keyspace_names_list='', table_names_list='', get_system_keyspaces=False):
        """
        :param node: node object to run the statements on
        :param keyspace_names_list: keyspace name to receive the DDL schema for
        :param table_name: table name or list of table names, for those (this) table/s the DDLs will be received.
                           In case DDl of all keyspace entites need to be created - remain it None
        :param get_system_keyspaces:
        """
        self.node = node
        self.get_system_keyspaces = get_system_keyspaces
        self.keyspace_names_list = keyspace_names_list
        self.table_names_list = table_names_list

    @property
    def keyspace_names_list(self):
        return self._keyspace_names_list

    @keyspace_names_list.setter
    def keyspace_names_list(self, value):
        self._keyspace_names_list = self.get_keyspaces() if not value else \
            [self.wrap_case_sensitive_string(ks) for ks in value]

    def get_schemas_ddl(self):
        test_ddl = {}
        keyspace = ''
        for keyspace_name in self._keyspace_names_list:
            ks_ddl = self.get_ddl(entities=keyspace_name, type='KEYSPACE', keyspace=keyspace)
            # The view of the secondary indexes shouldn't be created explicitly.
            # It'll be created automatically during secondary indexes creation
            test_ddl[keyspace_name] = self.remove_view_of_indexes(keyspace=keyspace_name, ks_ddl=ks_ddl)

            # If self.table_names_list is not None, just the tables in the list should be remain in the DDL
            if self.table_names_list:
                test_ddl[keyspace_name] = self.remain_expected_tables_only(keyspace=keyspace_name, ks_ddl=ks_ddl)
        return test_ddl

    def remain_expected_tables_only(self, keyspace, ks_ddl):
        if self.table_names_list:
            for table_name in self.table_names_list:
                for cmd in ks_ddl:
                    if ' {}.{} '.format(keyspace, table_name) not in cmd and 'KEYSPACE {} '.format(keyspace) not in cmd:
                        ks_ddl.remove(cmd)
        return ks_ddl

    def remove_view_of_indexes(self, keyspace, ks_ddl):
        indexes = self.get_entity_list(
            cmd="select index_name from system_schema.indexes where keyspace_name='{}'".format(keyspace))
        if indexes:
            for index in indexes:
                for cmd in ks_ddl:
                    if '{}.{}_index'.format(keyspace, index) in cmd:
                        ks_ddl.remove(cmd)
        return ks_ddl

    def get_ddl(self, entities, type, keyspace=''):
        ddl = []
        keyspace = '{}.'.format(keyspace) if keyspace else keyspace
        entities = [entities] if not isinstance(entities, list) else entities
        for entity in entities:
            entity_ddl = self.node.run_cqlsh(cmds="DESC {} {}{}".format(type, keyspace, entity), return_output=True)
            splitted = ['CREATE {}'.format(e) for e in entity_ddl[0].split('\nCREATE') if e]
            ddl.extend(splitted)
        return ddl

    def get_entity_list(self, cmd):
        out = self.node.run_cqlsh(cmds=cmd, return_output=True)
        if [i for i in out if 'error' in i]:
            assert False, 'Failed to run command "{cmd}". Error: {out}'.format(cmd=cmd, out=out)
        return [self.wrap_case_sensitive_string(entity.strip()) for entity in out[0].split('\n')[3:-3]]

    def get_keyspaces(self):
        if hasattr(self, '_keyspace_names_list') and self._keyspace_names_list:
            return self._keyspace_names_list
        out = self.node.run_cqlsh(cmds='select keyspace_name from system_schema.keyspaces', return_output=True)
        keyspaces = []
        for ks in out[0].split('\n')[3:-3]:
            if not self.get_system_keyspaces and 'system' in ks:
                continue
            keyspaces.append(self.wrap_case_sensitive_string(ks.strip()))
        return keyspaces

    @staticmethod
    def wrap_case_sensitive_string(string):
        if '"' in string:
            return string

        for l in string:
            if l.isupper():
                return '"{}"'.format(string)
        return string


def prepare_statement(session, query, cl=ConsistencyLevel.ONE):
    '''
    Prepare CQL query into statement and assign given consistency level to it.
    '''
    logger.debug('Prepairing statement: {}'.format(query))
    res = session.prepare(query)
    res.consistency_level = cl
    return res


def print_table(table):
    logger.debug(tabulate.tabulate(tabular_data=[
        [str(getattr(row, column_name)) for column_name in table.column_names]
        for row in table.current_rows], headers=table.column_names))


def set_trace_probability(nodes, probability_value):
    def _set_trace_probability_for_node(_node):
        logger.debug(f'{"Enable" if probability_value else "disable"} trace for node "{_node.name}" with '
                     f'"{probability_value}" probability value')
        errors = _node.nodetool(f'settraceprobability {probability_value}')[1]
        if errors:
            raise RuntimeError(f'Failed to {"enable" if probability_value else "disable"} trace for node '
                               f'"{_node.name}"')

    with ThreadPoolExecutor(max_workers=len(nodes)) as executor:
        threads = [executor.submit(_set_trace_probability_for_node, node) for node in nodes]
        [thread.result() for thread in threads]


def copy_directory(srcdir, destdir, ignore_subdir=True):
    """
    Copy file from srcdir to destdir, it supports to optionally ignore sub directories.
    """
    if not ignore_subdir:
        shutil.copytree(srcdir, destdir)
    for item in os.listdir(srcdir):
        srcfile = os.path.join(srcdir, item)
        if not os.path.exists(destdir):
            os.mkdir(destdir)
        if os.path.isfile(srcfile):
            shutil.copy2(srcfile, destdir)


def fill_data_by_cs(node, n_range=[500, 550, 600, 650], start=0, duration_range=[],
                    other_opt=['-rate', 'threads=10', '-col', 'size=FIXED(1024)'],
                    overlap_rate=0, flush=True):
    """
    fill data by multiple cassandra-stress workloads
    """
    opts = []
    for num in n_range:
        opts.append([f'n={num}', '-pop', f'seq={start}..{start+num}'])
        start += int(num * (1 - overlap_rate))
    for t in duration_range:
        opts.append([f'duration={t}s'])
    for opt in opts:
        cs_cmdline = ['write', 'no-warmup'] + opt + other_opt
        node.stress(cs_cmdline)
        if flush:
            logger.debug("Flush after writing data .....")
            node.flush()


def get_free_memory_size_in_mb():
    """
    Get current free memory from /proc/meminfo
    """
    proc = subprocess.Popen(['cat', '/proc/meminfo'], stdout=subprocess.PIPE)
    out, err = proc.communicate()
    out = out.decode()
    assert proc.returncode == 0 and 'MemFree:' in out, err
    pattern = re.compile('MemFree: (.*) ')
    for line in out.split('\n'):
        if pattern.match(line):
            return int(pattern.match(line)[1]) / 1024  # unit: mb
    raise Exception('Failed to get the valid free memory size')
