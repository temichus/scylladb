import os
import time
import unittest
from cassandra import ConsistencyLevel
from cassandra.concurrent import execute_concurrent_with_args, execute_concurrent
from cassandra.query import SimpleStatement
from ccmlib import common
import re
from dtest import debug
import random, string
import itertools
from copy import deepcopy
from threading import Thread
import datetime

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
        check_c1c2_result_one(success, list(result), tolerate_missing, must_be_missing, c1, c2),
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


def get_sstables_files(cf_dir, ks_name, cf_name, f_type=''):
    """
    Returns a set of sstable(s) files for a given KS and CF
    """
    sstable_pattern = re.compile("{}-{}-.*{}".format(ks_name, cf_name, f_type))
    sstables_files = set()
    for f in os.listdir(cf_dir):
        if sstable_pattern.match(f):
            sstables_files.add(f)

    return sstables_files


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


def get_cf_dir(ks_dir, cf_name):
    """
    Return the first CF directory for a CF with a given name
    """
    cf_pattern = re.compile("{}-".format(cf_name))
    for root, dirs, files in os.walk(ks_dir):
        for d in dirs:
            if cf_pattern.match(d):
                return os.path.join(root, d)

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
            debug('Names amount does not coincides with coulmns amount. Asked create {0} columns, supplied {1} names'
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

        debug(statement)
        self.session.execute(statement)

    def _create_columns_list(self):
        # TODO: add UDT
        for c_type, c_def in self.columns_dict.iteritems():
            preffix = self._convert_type_to_preffix(c_type, c_def)
            for i in xrange(c_def['amount']):
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

    def prefill_table(self, rows, data=None, start_id_from=0, consistency=ConsistencyLevel.QUORUM, using=None, flush=True):
        """
        Function pre-fill the table(named as value of self.table_name) with requested rows
        :param rows: how many rows should be in the table
        :type rows: varint
        :return:
        """
        data_arr = self._create_data_array(rows, ready_data=data)
        using_str = ' USING {0} {1}'.format(using.keys()[0], using[using.keys()[0]]) if using else ''
        st = 'INSERT INTO {ks}.{table_name} ({columns}) VALUES ({values}){using}'\
                                         .format(ks=self.keyspace, table_name=self.table_name,
                                                 columns=', '.join([c.split(' ')[0] for c in self.columns_list]),
                                                 values=('?,'*len(self.columns_list))[:-1], using=using_str)
        debug(st)
        statement = self.session.prepare(st)
        statement.consistency_level = consistency

        execute_concurrent_with_args(self.session, statement,
                                     map(lambda k: [k+start_id_from]+[data_arr[t][k] for t in xrange(0, len(data_arr))],
                                         [ k for k in xrange(0,rows)]))
        if flush:
            flush_by_node(self.cluster)

        debug('Finish prefill')

    def multiple_deletes(self, filters):
        """
        :param filter: {<column_name1>: [<value1>,<value2>,..] <column_name2>: [<value1>,<value2>,..], ..}
        """
        for i in xrange(0, len(next(filters.itervalues()))):
            filter = {}
            for column, values in filters.iteritems():
                filter.update({column: values[i]})
            self.delete_row(filter)

    def delete_row(self, filter):
        """
        :param filter: {<column_name1>: <value>, <column_name2>: <value>, ..}
        """
        where_statement = ['{0}={1}'.format(column, self.prepare_value(str(value))) for column, value in filter.iteritems()]
        query = 'delete from {tbl} where {where}'.format(tbl=self.table_name, where=' and '.join(s for s in where_statement))
        debug(query)
        self.session.execute(query)

    def truncate_table(self):
        query = 'truncate table {tbl}'.format(tbl=self.table_name)
        debug(query)
        self.session.execute(query)

    def _create_data_array(self, rows, ready_data=None):
        def _get_random(dupl):
            if ready_data and c_type in ready_data and dupl:
                return ready_data[c_type]
            data = []
            value = None
            #TODO: add UDT and collection types
            for i in xrange(0, dupl):
                if 'int' in c_type:
                    value = random.randint(c_def['value length']['min'], c_def['value length']['max'])
                elif c_type in ['text', 'ascii', 'varchar']:
                    value = ''.join(random.choice(string.ascii_lowercase)
                                     for _ in xrange(c_def['value length']['min'], c_def['value length']['max']))
                elif c_type in ['float','decimal', 'double']:
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
        for c_type, c_def in self.columns_dict.iteritems():
            if 'value length' not in c_def:
                c_def['value length'] = {'min': self.DEFAULT_MIN_LENGTH, 'max': self.DEFAULT_MAX_LENGTH}
            data_array[c_type] = _get_random(dupl)*(rows/dupl) + _get_random(rows%dupl)
        data_array = [data_array[i] for i in [c.split(' ')[1] for c in self.columns_list[1:]]]
        return data_array

    def update_table(self, set_clause, where_filter, using_clause=None, consistency_level=None, queue=None,
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
        :param queue: If the function is called from thread and want to save the output. Queue object
        :type queue: Queue.Queue
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
                                      for name, value in set_dict.iteritems())
            filter_str = ' and '.join('{0} {1} {2}'.format(name, value['operator'],
                                '({})'.format(', '.join([self.prepare_value(str(i)) for i in value['value']]))
                                if isinstance(value['value'], list) and value['operator'] == 'in' else
                                self.prepare_value(value['value']) )for name, value in filter_dict.iteritems())
            using_str = ' USING {0} {1}'.format(using_clause.keys()[0], using_clause[using_clause.keys()[0]]) if using_clause else ''

            statement = 'UPDATE {ks}.{table_name}{using} SET {set_clause} WHERE {filter}' \
                            .format(ks=self.keyspace, table_name=self.table_name,
                                   using='' if not using_clause else using_str,
                                   set_clause=set_str, filter=filter_str)
            self.session.execute(statement)
            if queue:
                queue.put_nowait((set_dict, filter_str))
            return (set_dict, filter_str)
        return (None, None)

    def multiple_int_updates_by_id(self, update_to_boundaries, filter_values=[], ids=[],
                                   updated_columns=None, updates=100, same_id=True):
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
        for _ in xrange(updates):
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

        debug('Updates finished')

    def select_all_mvs(self, reads=100, by_id=False):
        debug('Start reads from MVs')
        statement_template = 'select * from {0}'
        if by_id:
            max_id = self.get_max_id()
            statement_template = statement_template + ' where id={1}'
        else:
            statement_template = statement_template + ' LIMIT 10'

        for _ in xrange(0, reads):
            i = random.randint(0, len(self.materialized_views)-1)
            mv_name = [name for j, name in enumerate(self.materialized_views.keys()) if j == i][0]
            statement = statement_template.format(mv_name, random.randint(0, max_id)) if by_id else \
                                 statement_template.format(mv_name)
            debug(statement)
            self.session.execute(statement)
        debug('Finish reads from MVs')

    def prepare_value(self, value):
        try:
            _ = int(value)
            return value
        except ValueError:
            return '\'{}\''.format(value)

    def _build_filter(self, where_filter):
        clause = {}
        f, by = (where_filter['by name'], 'name') if 'by name' in where_filter else (where_filter['by type'], 'type')
        for clmn, value in f.iteritems():
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
        for utype, udef in set_clause.iteritems():
            s, by = udef, utype.replace('by ', '')
            # s, by = (clause['by name'], 'name') if 'by name' in clause else (clause['by type'], 'type')
            for name, new_value in s.iteritems():
                update_item = ''
                if by == 'name':
                    if [n for n in self.columns_list if '{} '.format(name) in n] \
                            and name not in self.pk_list+self.cl_list+clause.keys():
                        update_item = name
                elif by == 'type':
                    update_item = self._get_column_by_type(name, exclude_list=self.pk_list+self.cl_list+clause.keys()+exclude_columns)
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
    def create_materialized_view(self, mv_columns=None, mv_pk_column=None, mv_cl_column=None, mv_where_restriction=None):
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
                                                                       if isinstance(value['value'], list) else value['value'] )
                                                         if value['operator'] == 'in' else value['value'])
                                    for name, value in self.mv_where_restriction.iteritems()])
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
            debug(statement+';')
            self.parent_table.session.execute(statement)
            debug('Materialized view {} has been created'.format(self.mv_name))
            self.parent_table.set_mv(self.mv_name, self)

    def drop_mv(self):
        debug('Start drop materialized view {}'.format(self.mv_name))
        self.parent_table.session.execute('drop materialized view {}'.format(self.mv_name))
        debug('Finish drop materialized view {}'.format(self.mv_name))
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
                for position, r_def in mv_where_restriction['position'].iteritems():
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
                                for c_type, c_def in mv_columns.iteritems()])))
        return mv_columns_list


    def _build_columns_list(self, c_type, c_def, exclude_list):
        column_names = []
        if 'names' in c_def:
            column_names = [clmn for clmn in c_def['names'] if '{0} {1}'.format(clmn, c_type) in self.parent_table.columns_list
                            and clmn not in column_names + exclude_list]
        else:
            for i in xrange(c_def['amount']):
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
                debug('ERROR: new column for Materialized View PK is not found. Received parameters: {}'.format(mv_pk_column['type']))
            else:
                mv_pk_column_list.append(clmns[0])
        return mv_pk_column_list

    def my_count_query(self, filter=None):
        #TODO: handle filter
        return 'SELECT COUNT(*) FROM {my_name}{where_clause}'.format(my_name=self.mv_name, where_clause=filter or '')

def managed_thread(proc_functions, queue=None):
    """
    Function starts threads and run functions defined in the proc_functions variable. Save results of the functions if asked
    :param proc_functions: variable holds list of dictionaries with threads definitions. Expected structure:
                           [{'func': <function pointer - the function will be runs from the thread>,
                             'args': (arg1, arg2, arg3), - explicit function arguments by order in the function
                             'kwargs': {<arg name1>: value, <arg name2>: value} - function arguments by name
                            }, - first thread definition
                            {{'func': <function pointer, 'args': (), 'kwargs': {}} - second thread, no arguments
                           ]
    :param proc_functions: list
    :param queue: queue pointer
    :param queue: Queue.Queue
    :return: results of all treads if queue is not None
    :rtype: list | None
    """
    debug('Threads start at {}'.format(datetime.datetime.now()))
    threads = [Thread(target=func['func'], args=func['args'] if 'args' in func else [],
                      kwargs=func['kwargs'] if 'kwargs' in func else {})
               for func in proc_functions]
    _ = [t.start() for t in threads]
    _ = [t.join() for t in threads]
    debug('Threads finished at {}'.format(datetime.datetime.now()))
    if queue:
        results = [queue.get() for _ in threads]
        return results
