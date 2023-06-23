import logging

import pytest
from cassandra import ConsistencyLevel

from dtest_class import Tester, create_ks
from tools.assertions import assert_one
from tools.data import rows_to_list
from tools.datahelp import ColumnType
from tools.timeuuid import TimeUUID

logger = logging.getLogger(__name__)


@pytest.mark.dtest_full
class TestGroupFunctions(Tester):
    types_to_skip = ['ascii', 'blob', 'inet', 'list', 'map', 'set', 'time', 'tuple', 'udt']

    text_types_list = ['ascii', 'blob', 'inet', 'uuid', 'text', 'varchar']
    numeric_type_list = ['bigint', 'boolean', 'decimal', 'double', 'float', 'int', 'smallint', 'tinyint', 'varint']
    lists_types_list = ['list', 'map', 'set', 'tuple', 'udt']
    dates_type_list = ['date', 'time', 'timestamp', 'timeuuid']
    table_name = 'all_types'
    loop_number = 20000

    def prepare(self, nodes=3, keyspace_name='group_functions', rf=3, options_dict=None):
        self.keyspace_name = keyspace_name
        cluster = self.cluster

        if options_dict:
            cluster.set_configuration_options(values=options_dict)
        cluster.populate(nodes).start()
        node1 = cluster.nodelist()[0]
        session = self.patient_cql_connection(node1)
        create_ks(session=session, name=keyspace_name, rf=rf)

        return session

    def create_udt(self, session):
        query = 'create type my_udt (a text, b text)'
        session.execute(query)

    def create_table(self, session, table_name, single_column):
        query = 'CREATE TABLE {} ({});'.format(table_name, single_column)
        logger.debug(query)
        session.execute(query)

    def check_results(self, session, table_name, single_type):
        # In case of timeuuid the values will be of type `UUID`, but they should
        # be sorted as `TimeUUID`. `TimeUUID` sorts by timestamp, not raw uuid bytes.
        if single_type == 'timeuuid':
            def sort_key(row_with_uuid): return TimeUUID(row_with_uuid[0])
        else:
            sort_key = None

        full_res = sorted(
            rows_to_list(session.execute('select {} from {};'.format('my_{}'.format(single_type), table_name))), key=sort_key)
        assert_one(session, 'select count({}) from {}'.format('my_{}'.format(single_type), table_name), [len(full_res)],
                   cl=ConsistencyLevel.QUORUM)
        assert_one(session, 'select min({}) from {}'.format('my_{}'.format(single_type), table_name), full_res[0],
                   cl=ConsistencyLevel.QUORUM)
        assert_one(session, 'select max({}) from {}'.format('my_{}'.format(single_type), table_name), full_res[-1],
                   cl=ConsistencyLevel.QUORUM)

    def create_tables_and_run_group_functions(self, session, table_name, single_type):
        if single_type in self.types_to_skip:
            return
        column = 'my_{0} {0}'
        if single_type in self.lists_types_list:
            if single_type == 'tuple':
                type_format = 'frozen<{}<int>>'.format(single_type)
            elif single_type == 'udt':
                self.create_udt(session=session)
                type_format = 'frozen<my_{}>'.format(single_type)
            elif single_type == 'map':
                type_format = '{}<text, int>'.format(single_type)
            # here will be: 'uuid', 'bigint', 'boolean', 'date', 'decimal', 'double', 'float', 'int', 'smallint',
            # 'text', 'timestamp', 'timeuuid', 'tinyint', 'varchar', 'varint'
            else:
                type_format = '{}<int>'.format(single_type)
            single_column = 'id int PRIMARY KEY, {}'.format('my_{} {}'.format(single_type, type_format))
        else:
            single_column = 'id int PRIMARY KEY, {}'.format(column.format(single_type))
        self.create_table(session=session, table_name=table_name, single_column=single_column)
        for i in range(self.loop_number):
            self.populate_all_types_table(session=session, table_name=table_name, column_name=single_type, index=i)
        self.check_results(session=session, table_name=table_name, single_type=single_type)
        session.execute('DROP TABLE {}'.format(table_name))

    def populate_all_types_table(self, session, table_name, column_name, index):
        value = self.generate_all_types(column_name=column_name)
        query = 'INSERT INTO {} (id, {}) VALUES ({}, {});'.format(table_name, 'my_{}'.format(column_name), index, value)
        session.execute(query)

    def generate_all_types(self, column_name):
        return ColumnType(column_name).get_value()

    def test_numeric_type_group(self):
        session = self.prepare()
        for single_type in self.numeric_type_list:
            self.create_tables_and_run_group_functions(session=session, table_name=self.table_name,
                                                       single_type=single_type)

    def test_text_type_group(self):
        session = self.prepare()
        for single_type in self.text_types_list:
            self.create_tables_and_run_group_functions(session=session, table_name=self.table_name,
                                                       single_type=single_type)

    def test_lists_type_group(self):
        session = self.prepare()
        for single_type in self.lists_types_list:
            self.create_tables_and_run_group_functions(session=session, table_name=self.table_name,
                                                       single_type=single_type)

    def test_dates_type_group(self):
        session = self.prepare()
        for single_type in self.dates_type_list:
            self.create_tables_and_run_group_functions(session=session, table_name=self.table_name,
                                                       single_type=single_type)
