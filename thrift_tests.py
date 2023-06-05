import logging
import pytest
import struct
import time
import uuid
from threading import Thread
from pkg_resources import parse_version

from ccmlib.scylla_cluster import ScyllaCluster

from dtest_class import Tester
from thrift_bindings.thrift010 import Cassandra
from thrift_bindings.thrift010.Cassandra import (CfDef, Column, ColumnDef,
                                                 ColumnOrSuperColumn, ColumnParent,
                                                 ColumnPath, ColumnSlice,
                                                 ConsistencyLevel, CounterColumn,
                                                 Deletion, IndexExpression,
                                                 IndexOperator, IndexType,
                                                 InvalidRequestException, KeyRange,
                                                 KsDef, MultiSliceRequest,
                                                 Mutation, NotFoundException,
                                                 SlicePredicate, SliceRange,
                                                 SuperColumn)
from tools.assertions import assert_one, assert_none
from tools.thrift import get_thrift_client

logger = logging.getLogger(__file__)
pid_fname = "system_test.pid"


def pid():
    return int(open(pid_fname).read())


@pytest.mark.single_node
class BaseTester(Tester):
    extra_args = []

    @pytest.fixture(scope='function', autouse=True)
    def fixture_thrift_client(self, fixture_cluster):
        fixture_cluster.patient_cql_connection(fixture_cluster.cluster.nodelist()[0])
        client = get_thrift_client(host=fixture_cluster.cluster.get_node_ip(1))
        client.transport.open()
        self.define_schema(client)
        yield client
        client.transport.close()

    @pytest.fixture(scope='function', autouse=True)
    def fixture_cluster(self, fixture_dtest_setup):
        cluster = fixture_dtest_setup.cluster
        cluster.populate(1)
        node1, = cluster.nodelist()
        # If vnodes are not used, we must set our own initial_token
        # It does not matter what token we set as we only
        # ever use one node.
        if not fixture_dtest_setup.dtest_config.use_vnodes:
            node1.set_configuration_options(values={'initial_token': 1})
        node1.set_configuration_options(
            values={'start_rpc': 'true', 'partitioner': 'org.apache.cassandra.dht.Murmur3Partitioner'})

        cluster.start()
        yield fixture_dtest_setup

    def define_schema(self, client):
        raise NotImplementedError()


@pytest.mark.dtest_full
class ThriftTester(BaseTester):
    def define_schema(self, client):
        keyspace1 = Cassandra.KsDef('Keyspace1', 'org.apache.cassandra.locator.SimpleStrategy',
                                    {'replication_factor': '1'},
                                    cf_defs=[
                                        Cassandra.CfDef('Keyspace1', 'Standard1'),
                                        Cassandra.CfDef('Keyspace1', 'Standard2'),
                                        Cassandra.CfDef('Keyspace1', 'Standard3', column_metadata=[Cassandra.ColumnDef(
                                            'c1', 'AsciiType'), Cassandra.ColumnDef('c2', 'AsciiType')]),
                                        Cassandra.CfDef('Keyspace1', 'Standard4',
                                                        column_metadata=[Cassandra.ColumnDef('c1', 'AsciiType')]),
                                        Cassandra.CfDef('Keyspace1', 'StandardLong1', comparator_type='LongType'),
                                        Cassandra.CfDef('Keyspace1', 'StandardLong2', comparator_type='LongType'),
                                        Cassandra.CfDef('Keyspace1', 'StandardInteger1', comparator_type='IntegerType'),
                                        Cassandra.CfDef('Keyspace1', 'StandardComposite',
                                                        comparator_type='CompositeType(AsciiType, AsciiType)'),
                                        # Cassandra.CfDef('Keyspace1', 'Super1', column_type='Super', subcomparator_type='LongType'),
                                        # Cassandra.CfDef('Keyspace1', 'Super2', column_type='Super', subcomparator_type='LongType'),
                                        # Cassandra.CfDef('Keyspace1', 'Super3', column_type='Super', subcomparator_type='LongType'),
                                        # Cassandra.CfDef('Keyspace1', 'Super4', column_type='Super', subcomparator_type='UTF8Type'),
                                        # Cassandra.CfDef('Keyspace1', 'Super5', column_type='Super', comparator_type='LongType', subcomparator_type='UTF8Type'),
                                        Cassandra.CfDef('Keyspace1', 'Counter1',
                                                        default_validation_class='CounterColumnType'),
                                        # Cassandra.CfDef('Keyspace1', 'SuperCounter1', column_type='Super', default_validation_class='CounterColumnType'),
                                        # Cassandra.CfDef('Keyspace1', 'Indexed1', column_metadata=[Cassandra.ColumnDef('birthdate', 'LongType', Cassandra.IndexType.KEYS, 'birthdate_index')]),
                                        # Cassandra.CfDef('Keyspace1', 'Indexed2', comparator_type='TimeUUIDType', column_metadata=[Cassandra.ColumnDef(uuid.UUID('00000000-0000-1000-0000-000000000000').bytes, 'LongType', Cassandra.IndexType.KEYS)]),
                                        # Cassandra.CfDef('Keyspace1', 'Indexed3', comparator_type='TimeUUIDType', column_metadata=[Cassandra.ColumnDef(uuid.UUID('00000000-0000-1000-0000-000000000000').bytes, 'UTF8Type', Cassandra.IndexType.KEYS)]),

                                    ])

        keyspace2 = Cassandra.KsDef('Keyspace2', 'org.apache.cassandra.locator.SimpleStrategy',
                                    {'replication_factor': '1'},
                                    cf_defs=[
                                        Cassandra.CfDef('Keyspace2', 'Standard1'),
                                        Cassandra.CfDef('Keyspace2', 'Standard3'),
                                        # Cassandra.CfDef('Keyspace2', 'Super3', column_type='Super', subcomparator_type='BytesType'),
                                        # Cassandra.CfDef('Keyspace2', 'Super4', column_type='Super', subcomparator_type='TimeUUIDType'),
                                    ])

        for ks in [keyspace1, keyspace2]:
            client.system_add_keyspace(ks)


def _i64(n):
    return struct.pack('>q', n)  # big endian = network order


def _i32(n):
    return struct.pack('>i', n)  # big endian = network order


def _i16(n):
    return struct.pack('>h', n)  # big endian = network order


_SIMPLE_COLUMNS = [Column('c1', 'value1', 0),
                   Column('c2', 'value2', 0)]
_SUPER_COLUMNS = [SuperColumn(name='sc1', columns=[Column(_i64(4), 'value4', 0)]),
                  SuperColumn(name='sc2', columns=[Column(_i64(5), 'value5', 0),
                                                   Column(_i64(6), 'value6', 0)])]


def _assert_column(client, column_family, key, column, value, ts=0):
    try:
        assert client.get(key, ColumnPath(column_family, column=column),
                          ConsistencyLevel.ONE).column == Column(column, value, ts)
    except NotFoundException:
        raise Exception('expected %s:%s:%s:%s, but was not present' % (column_family, key, column, value))


def _assert_columnpath_exists(client, key, column_path):
    try:
        assert client.get(key, column_path, ConsistencyLevel.ONE)
    except NotFoundException:
        raise Exception('expected %s with %s but was not present.' % (key, column_path))


def _assert_no_columnpath(client, key, column_path):
    try:
        client.get(key, column_path, ConsistencyLevel.ONE)
        assert False, ('columnpath %s existed in %s when it should not' % (column_path, key))
    except NotFoundException:
        assert True, 'column did not exist'


def _insert_simple(client, block=True):
    return _insert_multi(client, ['key1'])


def _insert_batch(client, block):
    return _insert_multi_batch(client, ['key1'], block)


def _insert_multi(client, keys):
    CL = ConsistencyLevel.ONE
    for key in keys:
        client.insert(key, ColumnParent('Standard1'), Column('c1', 'value1', 0), CL)
        client.insert(key, ColumnParent('Standard1'), Column('c2', 'value2', 0), CL)


def _insert_multi_batch(client, keys, block):
    cfmap = {'Standard1': [Mutation(ColumnOrSuperColumn(c)) for c in _SIMPLE_COLUMNS],
             'Standard2': [Mutation(ColumnOrSuperColumn(c)) for c in _SIMPLE_COLUMNS]}
    for key in keys:
        client.batch_mutate({key: cfmap}, ConsistencyLevel.ONE)


def _big_slice(client, key, column_parent):
    p = SlicePredicate(slice_range=SliceRange('', '', False, 1000))
    return client.get_slice(key, column_parent, p, ConsistencyLevel.ONE)


def _big_multislice(client, keys, column_parent, count=1000):
    p = SlicePredicate(slice_range=SliceRange('', '', False, count))
    return client.multiget_slice(keys, column_parent, p, ConsistencyLevel.ONE)


def _verify_batch(client):
    _verify_simple(client)
    L = [result.column
         for result in _big_slice(client, 'key1', ColumnParent('Standard2'))]
    assert L == _SIMPLE_COLUMNS, L


def _verify_simple(client):
    assert client.get('key1', ColumnPath('Standard1', column='c1'),
                      ConsistencyLevel.ONE).column == Column('c1', 'value1', 0)
    L = [result.column
         for result in _big_slice(client, 'key1', ColumnParent('Standard1'))]
    assert L == _SIMPLE_COLUMNS, L


def _insert_super(client, key='key1'):
    client.insert(key, ColumnParent('Super1', 'sc1'), Column(_i64(4), 'value4', 0), ConsistencyLevel.ONE)
    client.insert(key, ColumnParent('Super1', 'sc2'), Column(_i64(5), 'value5', 0), ConsistencyLevel.ONE)
    client.insert(key, ColumnParent('Super1', 'sc2'), Column(_i64(6), 'value6', 0), ConsistencyLevel.ONE)
    time.sleep(0.1)


def _insert_range(client):
    client.insert('key1', ColumnParent('Standard1'), Column('c1', 'value1', 0), ConsistencyLevel.ONE)
    client.insert('key1', ColumnParent('Standard1'), Column('c2', 'value2', 0), ConsistencyLevel.ONE)
    client.insert('key1', ColumnParent('Standard1'), Column('c3', 'value3', 0), ConsistencyLevel.ONE)
    time.sleep(0.1)


def _insert_counter_range(client):
    client.add('key1', ColumnParent('Counter1'), CounterColumn('c1', 1), ConsistencyLevel.ONE)
    client.add('key1', ColumnParent('Counter1'), CounterColumn('c2', 2), ConsistencyLevel.ONE)
    client.add('key1', ColumnParent('Counter1'), CounterColumn('c3', 3), ConsistencyLevel.ONE)
    time.sleep(0.1)


def _verify_range(client):
    p = SlicePredicate(slice_range=SliceRange('c1', 'c2', False, 1000))
    result = client.get_slice('key1', ColumnParent('Standard1'), p, ConsistencyLevel.ONE)
    assert len(result) == 2
    assert result[0].column.name == b'c1'
    assert result[1].column.name == b'c2'

    p = SlicePredicate(slice_range=SliceRange('c3', 'c2', True, 1000))
    result = client.get_slice('key1', ColumnParent('Standard1'), p, ConsistencyLevel.ONE)
    assert len(result) == 2
    assert result[0].column.name == b'c3'
    assert result[1].column.name == b'c2'

    p = SlicePredicate(slice_range=SliceRange('a', 'z', False, 1000))
    result = client.get_slice('key1', ColumnParent('Standard1'), p, ConsistencyLevel.ONE)
    assert len(result) == 3, result

    p = SlicePredicate(slice_range=SliceRange('a', 'z', False, 2))
    result = client.get_slice('key1', ColumnParent('Standard1'), p, ConsistencyLevel.ONE)
    assert len(result) == 2, result


def _verify_counter_range(client):
    p = SlicePredicate(slice_range=SliceRange('c1', 'c2', False, 1000))
    result = client.get_slice('key1', ColumnParent('Counter1'), p, ConsistencyLevel.ONE)
    assert len(result) == 2
    assert result[0].counter_column.name == b'c1'
    assert result[1].counter_column.name == b'c2'

    p = SlicePredicate(slice_range=SliceRange('c3', 'c2', True, 1000))
    result = client.get_slice('key1', ColumnParent('Counter1'), p, ConsistencyLevel.ONE)
    assert len(result) == 2
    assert result[0].counter_column.name == b'c3'
    assert result[1].counter_column.name == b'c2'

    p = SlicePredicate(slice_range=SliceRange('a', 'z', False, 1000))
    result = client.get_slice('key1', ColumnParent('Counter1'), p, ConsistencyLevel.ONE)
    assert len(result) == 3, result

    p = SlicePredicate(slice_range=SliceRange('a', 'z', False, 2))
    result = client.get_slice('key1', ColumnParent('Counter1'), p, ConsistencyLevel.ONE)
    assert len(result) == 2, result


def _set_keyspace(client, keyspace):
    client.set_keyspace(keyspace)


def _insert_super_range(client):
    client.insert('key1', ColumnParent('Super1', 'sc1'), Column(_i64(4), 'value4', 0), ConsistencyLevel.ONE)
    client.insert('key1', ColumnParent('Super1', 'sc2'), Column(_i64(5), 'value5', 0), ConsistencyLevel.ONE)
    client.insert('key1', ColumnParent('Super1', 'sc2'), Column(_i64(6), 'value6', 0), ConsistencyLevel.ONE)
    client.insert('key1', ColumnParent('Super1', 'sc3'), Column(_i64(7), 'value7', 0), ConsistencyLevel.ONE)
    time.sleep(0.1)


def _insert_counter_super_range(client):
    client.add('key1', ColumnParent('SuperCounter1', 'sc1'), CounterColumn(_i64(4), 4), ConsistencyLevel.ONE)
    client.add('key1', ColumnParent('SuperCounter1', 'sc2'), CounterColumn(_i64(5), 5), ConsistencyLevel.ONE)
    client.add('key1', ColumnParent('SuperCounter1', 'sc2'), CounterColumn(_i64(6), 6), ConsistencyLevel.ONE)
    client.add('key1', ColumnParent('SuperCounter1', 'sc3'), CounterColumn(_i64(7), 7), ConsistencyLevel.ONE)
    time.sleep(0.1)


def _verify_super_range(client):
    p = SlicePredicate(slice_range=SliceRange('sc2', 'sc3', False, 2))
    result = client.get_slice('key1', ColumnParent('Super1'), p, ConsistencyLevel.ONE)
    assert len(result) == 2
    assert result[0].super_column.name == 'sc2'
    assert result[1].super_column.name == 'sc3'

    p = SlicePredicate(slice_range=SliceRange('sc3', 'sc2', True, 2))
    result = client.get_slice('key1', ColumnParent('Super1'), p, ConsistencyLevel.ONE)
    assert len(result) == 2
    assert result[0].super_column.name == 'sc3'
    assert result[1].super_column.name == 'sc2'


def _verify_counter_super_range(client):
    p = SlicePredicate(slice_range=SliceRange('sc2', 'sc3', False, 2))
    result = client.get_slice('key1', ColumnParent('SuperCounter1'), p, ConsistencyLevel.ONE)
    assert len(result) == 2
    assert result[0].counter_super_column.name == 'sc2'
    assert result[1].counter_super_column.name == 'sc3'

    p = SlicePredicate(slice_range=SliceRange('sc3', 'sc2', True, 2))
    result = client.get_slice('key1', ColumnParent('SuperCounter1'), p, ConsistencyLevel.ONE)
    assert len(result) == 2
    assert result[0].counter_super_column.name == 'sc3'
    assert result[1].counter_super_column.name == 'sc2'


def _verify_super(client, supercf='Super1', key='key1'):
    assert client.get(key, ColumnPath(supercf, 'sc1', _i64(4)),
                      ConsistencyLevel.ONE).column == Column(_i64(4), 'value4', 0)
    slice = [result.super_column
             for result in _big_slice(client, key, ColumnParent('Super1'))]
    assert slice == _SUPER_COLUMNS, slice


def _expect_exception(fn, type_):
    try:
        r = fn()
    except type_ as t:
        return t
    else:
        raise Exception('expected %s; got %s' % (type_.__name__, r))


def _expect_missing(fn):
    _expect_exception(fn, NotFoundException)


def get_range_slice(client, parent, predicate, start, end, count, cl, row_filter=None):
    kr = KeyRange(start, end, count=count, row_filter=row_filter)
    return client.get_range_slices(parent, predicate, kr, cl)


def _insert_six_columns(client, key='abc'):
    CL = ConsistencyLevel.ONE
    client.insert(key, ColumnParent('Standard1'), Column('a', '1', 0), CL)
    client.insert(key, ColumnParent('Standard1'), Column('b', '2', 0), CL)
    client.insert(key, ColumnParent('Standard1'), Column('c', '3', 0), CL)
    client.insert(key, ColumnParent('Standard1'), Column('d', '4', 0), CL)
    client.insert(key, ColumnParent('Standard1'), Column('e', '5', 0), CL)
    client.insert(key, ColumnParent('Standard1'), Column('f', '6', 0), CL)


def _big_multi_slice(client, key='abc'):
    c1 = ColumnSlice()
    c1.start = 'a'
    c1.finish = 'c'
    c2 = ColumnSlice()
    c2.start = 'e'
    c2.finish = 'f'
    m = MultiSliceRequest()
    m.key = key
    m.column_parent = ColumnParent('Standard1')
    m.column_slices = [c1, c2]
    m.reversed = False
    m.count = 10
    m.consistency_level = ConsistencyLevel.ONE
    return client.get_multi_slice(m)


_MULTI_SLICE_COLUMNS = [Column('a', '1', 0), Column('b', '2', 0), Column(
    'c', '3', 0), Column('e', '5', 0), Column('f', '6', 0)]


@pytest.mark.dtest_full
class TestMutations(ThriftTester):

    def test_insert(self, fixture_thrift_client):
        _set_keyspace(fixture_thrift_client, 'Keyspace1')
        _insert_simple(fixture_thrift_client, False)
        time.sleep(0.1)
        _verify_simple(fixture_thrift_client)

    def test_prepared_simple(self, fixture_thrift_client):
        """
        Insert a row and then read it back using prepared statements.
        """
        _set_keyspace(fixture_thrift_client, 'Keyspace1')
        cd = ColumnDef('v', 'AsciiType', None, None)
        newcf = CfDef('Keyspace1', 'cf', default_validation_class='AsciiType', column_metadata=[cd])
        fixture_thrift_client.system_add_column_family(newcf)

        prepared_ins = fixture_thrift_client.prepare_cql3_query(b"INSERT INTO cf (key, v) VALUES (?, ?)",
                                                                Cassandra.Compression.NONE)
        prepared_sel = fixture_thrift_client.prepare_cql3_query(b"SELECT v FROM cf WHERE key=?",
                                                                Cassandra.Compression.NONE)
        res = fixture_thrift_client.execute_prepared_cql3_query(prepared_ins.itemId, ['0', 'my_value'],
                                                                ConsistencyLevel.ONE)
        rows = fixture_thrift_client.execute_prepared_cql3_query(prepared_sel.itemId, ['0'], ConsistencyLevel.ONE)
        assert rows.rows[0].columns[0].value == b'my_value'

    def test_empty_slice(self, fixture_thrift_client):
        _set_keyspace(fixture_thrift_client, 'Keyspace1')
        assert _big_slice(fixture_thrift_client, 'key1', ColumnParent('Standard2')) == []
        # assert _big_slice(fixture_thrift_client, 'key1', ColumnParent('Super1')) == []

    @pytest.mark.skip("LWT not implemented")
    def test_cas(self, fixture_thrift_client):
        _set_keyspace(fixture_thrift_client, 'Keyspace1')

        def cas(expected, updates, column_family):
            return fixture_thrift_client.cas('key1', column_family, expected, updates, ConsistencyLevel.SERIAL,
                                             ConsistencyLevel.QUORUM)

        def test_cas_operations(first_columns, second_columns, column_family):
            # partition should be empty, so cas expecting any existing values should fail
            cas_result = cas(first_columns, first_columns, column_family)
            assert not cas_result.success
            assert len(cas_result.current_values) == 0, cas_result

            # cas of empty columns -> first_columns should succeed
            # and the reading back from the table should match first_columns
            assert cas([], first_columns, column_family).success
            result = [cosc.column for cosc in _big_slice(fixture_thrift_client, 'key1', ColumnParent(column_family))]
            # CAS will use its own timestamp, so we can't just compare result == _SIMPLE_COLUMNS
            assert dict((c.name, c.value) for c in result) == dict((ex.name, ex.value) for ex in first_columns)

            # now that the partition has been updated, repeating the
            # operation which expects it to be empty should not succeed
            cas_result = cas([], first_columns, column_family)
            assert not cas_result.success
            # When we CAS for non-existence, current_values is the first live column of the row
            assert dict((c.name, c.value) for c in cas_result.current_values) == {
                first_columns[0].name: first_columns[0].value}, cas_result

            # CL.SERIAL for reads
            assert fixture_thrift_client.get('key1', ColumnPath(
                column_family, column=first_columns[0].name), ConsistencyLevel.SERIAL).column.value == first_columns[
                0].value

            # cas first_columns -> second_columns should succeed
            assert cas(first_columns, second_columns, column_family).success

            # as before, an operation with an incorrect expectation should fail
            cas_result = cas(first_columns, second_columns, column_family)
            assert not cas_result.success

        updated_columns = [Column('c1', 'value101', 1),
                           Column('c2', 'value102', 1)]

        logger.info("Testing CAS operations on dynamic cf")
        test_cas_operations(_SIMPLE_COLUMNS, updated_columns, 'Standard1')
        logger.info("Testing CAS operations on static cf")
        test_cas_operations(_SIMPLE_COLUMNS, updated_columns, 'Standard3')
        logger.info("Testing CAS on mixed static/dynamic cf")
        test_cas_operations(_SIMPLE_COLUMNS, updated_columns, 'Standard4')

    @pytest.mark.skip("Super columns not implemented")
    def test_missing_super(self, fixture_thrift_client):
        _set_keyspace(fixture_thrift_client, 'Keyspace1')
        _expect_missing(
            lambda: fixture_thrift_client.get('key1', ColumnPath('Super1', 'sc1', _i64(1)), ConsistencyLevel.ONE))
        _insert_super(fixture_thrift_client)
        _expect_missing(
            lambda: fixture_thrift_client.get('key1', ColumnPath('Super1', 'sc1', _i64(1)), ConsistencyLevel.ONE))

    def test_count(self, fixture_thrift_client):
        _set_keyspace(fixture_thrift_client, 'Keyspace1')
        _insert_simple(fixture_thrift_client, )
        # _insert_super(fixture_thrift_client)
        p = SlicePredicate(slice_range=SliceRange('', '', False, 1000))
        assert fixture_thrift_client.get_count('key1', ColumnParent('Standard2'), p, ConsistencyLevel.ONE) == 0
        assert fixture_thrift_client.get_count('key1', ColumnParent('Standard1'), p, ConsistencyLevel.ONE) == 2
        # assert fixture_thrift_client.get_count('key1', ColumnParent('Super1', 'sc2'), p, ConsistencyLevel.ONE) == 2
        # assert fixture_thrift_client.get_count('key1', ColumnParent('Super1'), p, ConsistencyLevel.ONE) == 2

        # Let's make that a little more interesting
        fixture_thrift_client.insert('key1', ColumnParent('Standard1'), Column('c3', 'value3', 0), ConsistencyLevel.ONE)
        fixture_thrift_client.insert('key1', ColumnParent('Standard1'), Column('c4', 'value4', 0), ConsistencyLevel.ONE)
        fixture_thrift_client.insert('key1', ColumnParent('Standard1'), Column('c5', 'value5', 0), ConsistencyLevel.ONE)

        p = SlicePredicate(slice_range=SliceRange('c2', 'c4', False, 1000))
        assert fixture_thrift_client.get_count('key1', ColumnParent('Standard1'), p, ConsistencyLevel.ONE) == 3

    def test_count_paging(self, fixture_thrift_client):
        _set_keyspace(fixture_thrift_client, 'Keyspace1')
        _insert_simple(fixture_thrift_client, )

        # Exercise paging
        column_parent = ColumnParent('Standard1')
        # Paging for small columns starts at 1024 columns
        columns_to_insert = [Column('c%d' % (i,), 'value%d' % (i,), 0) for i in range(3, 1026)]
        cfmap = {'Standard1': [Mutation(ColumnOrSuperColumn(c)) for c in columns_to_insert]}
        fixture_thrift_client.batch_mutate({'key1': cfmap}, ConsistencyLevel.ONE)

        p = SlicePredicate(slice_range=SliceRange('', '', False, 2000))
        assert fixture_thrift_client.get_count('key1', column_parent, p, ConsistencyLevel.ONE) == 1025

        # Ensure that the count limit isn't clobbered
        p = SlicePredicate(slice_range=SliceRange('', '', False, 10))
        assert fixture_thrift_client.get_count('key1', ColumnParent('Standard1'), p, ConsistencyLevel.ONE) == 10

    # test get_count() to work correctly with 'count' settings around page size (CASSANDRA-4833)
    def test_count_around_page_size(self, fixture_thrift_client):
        def slice_predicate(count):
            return SlicePredicate(slice_range=SliceRange('', '', False, count))

        _set_keyspace(fixture_thrift_client, 'Keyspace1')

        key = 'key1'
        parent = ColumnParent('Standard1')
        cl = ConsistencyLevel.ONE

        for i in range(0, 3050):
            fixture_thrift_client.insert(key, parent, Column(str(i), '', 0), cl)

        # same as page size
        assert fixture_thrift_client.get_count(key, parent, slice_predicate(1024), cl) == 1024

        # 1 above page size
        assert fixture_thrift_client.get_count(key, parent, slice_predicate(1025), cl) == 1025

        # above number or columns
        assert fixture_thrift_client.get_count(key, parent, slice_predicate(4000), cl) == 3050

        # same as number of columns
        assert fixture_thrift_client.get_count(key, parent, slice_predicate(3050), cl) == 3050

        # 1 above number of columns
        assert fixture_thrift_client.get_count(key, parent, slice_predicate(3051), cl) == 3050

    def test_insert_blocking(self, fixture_thrift_client):
        _set_keyspace(fixture_thrift_client, 'Keyspace1')
        _insert_simple(fixture_thrift_client, )
        _verify_simple(fixture_thrift_client)

    @pytest.mark.skip("Super columns not implemented")
    def test_super_insert(self, fixture_thrift_client):
        _set_keyspace(fixture_thrift_client, 'Keyspace1')
        _insert_super(fixture_thrift_client)
        _verify_super(fixture_thrift_client)

    @pytest.mark.skip("Super columns not implemented")
    def test_super_get(self, fixture_thrift_client):
        _set_keyspace(fixture_thrift_client, 'Keyspace1')
        _insert_super(fixture_thrift_client)
        result = fixture_thrift_client.get('key1', ColumnPath('Super1', 'sc2'), ConsistencyLevel.ONE).super_column
        assert result == _SUPER_COLUMNS[1], result

    @pytest.mark.skip("Super columns not implemented")
    def test_super_subcolumn_limit(self, fixture_thrift_client):
        _set_keyspace(fixture_thrift_client, 'Keyspace1')
        _insert_super(fixture_thrift_client)
        p = SlicePredicate(slice_range=SliceRange('', '', False, 1))
        column_parent = ColumnParent('Super1', 'sc2')
        slice = [result.column
                 for result in fixture_thrift_client.get_slice('key1', column_parent, p, ConsistencyLevel.ONE)]
        assert slice == [Column(_i64(5), 'value5', 0)], slice
        p = SlicePredicate(slice_range=SliceRange('', '', True, 1))
        slice = [result.column
                 for result in fixture_thrift_client.get_slice('key1', column_parent, p, ConsistencyLevel.ONE)]
        assert slice == [Column(_i64(6), 'value6', 0)], slice

    def test_long_order(self, fixture_thrift_client):
        _set_keyspace(fixture_thrift_client, 'Keyspace1')

        def long_range(start, stop, step):
            i = start
            while i < stop:
                yield i
                i += step

        L = []
        for i in long_range(0, 104294967296, 429496729):
            name = _i64(i)
            fixture_thrift_client.insert('key1', ColumnParent('StandardLong1'), Column(name, 'v', 0),
                                         ConsistencyLevel.ONE)
            L.append(name)
        slice = [result.column.name for result in
                 _big_slice(fixture_thrift_client, 'key1', ColumnParent('StandardLong1'))]
        assert slice == L, slice

    def test_integer_order(self, fixture_thrift_client):
        _set_keyspace(fixture_thrift_client, 'Keyspace1')

        def long_range(start, stop, step):
            i = start
            while i >= stop:
                yield i
                i -= step

        L = []
        for i in long_range(104294967296, 0, 429496729):
            name = _i64(i)
            fixture_thrift_client.insert('key1', ColumnParent('StandardInteger1'), Column(name, 'v', 0),
                                         ConsistencyLevel.ONE)
            L.append(name)
        slice = [result.column.name for result in
                 _big_slice(fixture_thrift_client, 'key1', ColumnParent('StandardInteger1'))]
        L.sort()
        assert slice == L, slice

    @pytest.mark.skip("Super columns not implemented")
    def test_time_uuid(self, fixture_thrift_client):
        import uuid
        L = []
        _set_keyspace(fixture_thrift_client, 'Keyspace2')
        # 100 isn't enough to fail reliably if the comparator is borked
        for i in range(500):
            L.append(uuid.uuid1())
            fixture_thrift_client.insert('key1', ColumnParent('Super4', 'sc1'), Column(
                L[-1].bytes, 'value%s' % i, i), ConsistencyLevel.ONE)
        slice = _big_slice(fixture_thrift_client, 'key1', ColumnParent('Super4', 'sc1'))
        assert len(slice) == 500, len(slice)
        for i in range(500):
            u = slice[i].column
            assert u.value == 'value%s' % i
            assert u.name == L[i].bytes

        p = SlicePredicate(slice_range=SliceRange('', '', True, 1))
        column_parent = ColumnParent('Super4', 'sc1')
        slice = [result.column
                 for result in fixture_thrift_client.get_slice('key1', column_parent, p, ConsistencyLevel.ONE)]
        assert slice == [Column(L[-1].bytes, 'value499', 499)], slice

        p = SlicePredicate(slice_range=SliceRange('', L[2].bytes, False, 1000))
        column_parent = ColumnParent('Super4', 'sc1')
        slice = [result.column
                 for result in fixture_thrift_client.get_slice('key1', column_parent, p, ConsistencyLevel.ONE)]
        assert slice == [Column(L[0].bytes, 'value0', 0),
                         Column(L[1].bytes, 'value1', 1),
                         Column(L[2].bytes, 'value2', 2)], slice

        p = SlicePredicate(slice_range=SliceRange(L[2].bytes, '', True, 1000))
        column_parent = ColumnParent('Super4', 'sc1')
        slice = [result.column
                 for result in fixture_thrift_client.get_slice('key1', column_parent, p, ConsistencyLevel.ONE)]
        assert slice == [Column(L[2].bytes, 'value2', 2),
                         Column(L[1].bytes, 'value1', 1),
                         Column(L[0].bytes, 'value0', 0)], slice

        p = SlicePredicate(slice_range=SliceRange(L[2].bytes, '', False, 1))
        column_parent = ColumnParent('Super4', 'sc1')
        slice = [result.column
                 for result in fixture_thrift_client.get_slice('key1', column_parent, p, ConsistencyLevel.ONE)]
        assert slice == [Column(L[2].bytes, 'value2', 2)], slice

    def test_long_remove(self, fixture_thrift_client):
        column_parent = ColumnParent('StandardLong1')
        sp = SlicePredicate(slice_range=SliceRange('', '', False, 1))
        _set_keyspace(fixture_thrift_client, 'Keyspace1')
        for i in range(10):
            parent = ColumnParent('StandardLong1')

            fixture_thrift_client.insert('key1', parent, Column(_i64(i), 'value1', 10 * i), ConsistencyLevel.ONE)
            fixture_thrift_client.remove('key1', ColumnPath('StandardLong1'), 10 * i + 1, ConsistencyLevel.ONE)
            slice = fixture_thrift_client.get_slice('key1', column_parent, sp, ConsistencyLevel.ONE)
            assert slice == [], slice
            # resurrect
            fixture_thrift_client.insert('key1', parent, Column(_i64(i), 'value2', 10 * i + 2), ConsistencyLevel.ONE)
            slice = [result.column
                     for result in fixture_thrift_client.get_slice('key1', column_parent, sp, ConsistencyLevel.ONE)]
            assert slice == [Column(_i64(i), 'value2', 10 * i + 2)], (slice, i)

    def test_integer_remove(self, fixture_thrift_client):
        column_parent = ColumnParent('StandardInteger1')
        sp = SlicePredicate(slice_range=SliceRange('', '', False, 1))
        _set_keyspace(fixture_thrift_client, 'Keyspace1')
        for i in range(10):
            parent = ColumnParent('StandardInteger1')

            fixture_thrift_client.insert('key1', parent, Column(_i64(i), 'value1', 10 * i), ConsistencyLevel.ONE)
            fixture_thrift_client.remove('key1', ColumnPath('StandardInteger1'), 10 * i + 1, ConsistencyLevel.ONE)
            slice = fixture_thrift_client.get_slice('key1', column_parent, sp, ConsistencyLevel.ONE)
            assert slice == [], slice
            # resurrect
            fixture_thrift_client.insert('key1', parent, Column(_i64(i), 'value2', 10 * i + 2), ConsistencyLevel.ONE)
            slice = [result.column
                     for result in fixture_thrift_client.get_slice('key1', column_parent, sp, ConsistencyLevel.ONE)]
            assert slice == [Column(_i64(i), 'value2', 10 * i + 2)], (slice, i)

    def test_batch_insert(self, fixture_thrift_client):
        _set_keyspace(fixture_thrift_client, 'Keyspace1')
        _insert_batch(fixture_thrift_client, False)
        time.sleep(0.1)
        _verify_batch(fixture_thrift_client)

    def test_batch_insert_blocking(self, fixture_thrift_client):
        _set_keyspace(fixture_thrift_client, 'Keyspace1')
        _insert_batch(fixture_thrift_client, True)
        _verify_batch(fixture_thrift_client)

    def test_batch_mutate_standard_columns(self, fixture_thrift_client):
        _set_keyspace(fixture_thrift_client, 'Keyspace1')
        column_families = ['Standard1', 'Standard2']
        keys = ['key_%d' % i for i in range(27, 32)]
        mutations = [Mutation(ColumnOrSuperColumn(c)) for c in _SIMPLE_COLUMNS]
        mutation_map = dict((column_family, mutations) for column_family in column_families)
        keyed_mutations = dict((key, mutation_map) for key in keys)

        fixture_thrift_client.batch_mutate(keyed_mutations, ConsistencyLevel.ONE)

        for column_family in column_families:
            for key in keys:
                _assert_column(fixture_thrift_client, column_family, key, 'c1', 'value1')

    def test_batch_mutate_standard_columns_blocking(self, fixture_thrift_client):
        _set_keyspace(fixture_thrift_client, 'Keyspace1')

        column_families = ['Standard1', 'Standard2']
        keys = ['key_%d' % i for i in range(38, 46)]

        mutations = [Mutation(ColumnOrSuperColumn(c)) for c in _SIMPLE_COLUMNS]
        mutation_map = dict((column_family, mutations) for column_family in column_families)
        keyed_mutations = dict((key, mutation_map) for key in keys)

        fixture_thrift_client.batch_mutate(keyed_mutations, ConsistencyLevel.ONE)

        for column_family in column_families:
            for key in keys:
                _assert_column(fixture_thrift_client, column_family, key, 'c1', 'value1')

    def test_batch_mutate_remove_standard_columns(self, fixture_thrift_client):
        _set_keyspace(fixture_thrift_client, 'Keyspace1')
        column_families = ['Standard1', 'Standard2']
        keys = ['key_%d' % i for i in range(11, 21)]
        _insert_multi(fixture_thrift_client, keys)

        mutations = [Mutation(deletion=Deletion(20, predicate=SlicePredicate(column_names=[c.name])))
                     for c in _SIMPLE_COLUMNS]
        mutation_map = dict((column_family, mutations) for column_family in column_families)

        keyed_mutations = dict((key, mutation_map) for key in keys)

        fixture_thrift_client.batch_mutate(keyed_mutations, ConsistencyLevel.ONE)

        for column_family in column_families:
            for c in _SIMPLE_COLUMNS:
                for key in keys:
                    _assert_no_columnpath(fixture_thrift_client, key, ColumnPath(column_family, column=c.name))

    def test_batch_mutate_remove_standard_row(self, fixture_thrift_client):
        _set_keyspace(fixture_thrift_client, 'Keyspace1')
        column_families = ['Standard1', 'Standard2']
        keys = ['key_%d' % i for i in range(11, 21)]
        _insert_multi(fixture_thrift_client, keys)

        mutations = [Mutation(deletion=Deletion(20))]
        mutation_map = dict((column_family, mutations) for column_family in column_families)

        keyed_mutations = dict((key, mutation_map) for key in keys)

        fixture_thrift_client.batch_mutate(keyed_mutations, ConsistencyLevel.ONE)

        for column_family in column_families:
            for c in _SIMPLE_COLUMNS:
                for key in keys:
                    _assert_no_columnpath(fixture_thrift_client, key, ColumnPath(column_family, column=c.name))

    @pytest.mark.skip("Super columns not implemented")
    def test_batch_mutate_remove_super_columns_with_standard_under(self, fixture_thrift_client):
        _set_keyspace(fixture_thrift_client, 'Keyspace1')
        column_families = ['Super1', 'Super2']
        keys = ['key_%d' % i for i in range(11, 21)]
        _insert_super(fixture_thrift_client)

        mutations = []
        for sc in _SUPER_COLUMNS:
            names = []
            for c in sc.columns:
                names.append(c.name)
            mutations.append(Mutation(deletion=Deletion(20, super_column=c.name,
                                                        predicate=SlicePredicate(column_names=names))))

        mutation_map = dict((column_family, mutations) for column_family in column_families)

        keyed_mutations = dict((key, mutation_map) for key in keys)

        fixture_thrift_client.batch_mutate(keyed_mutations, ConsistencyLevel.ONE)
        for column_family in column_families:
            for sc in _SUPER_COLUMNS:
                for c in sc.columns:
                    for key in keys:
                        _assert_no_columnpath(fixture_thrift_client, key,
                                              ColumnPath(column_family, super_column=sc.name, column=c.name))

    @pytest.mark.skip("Super columns not implemented")
    def test_batch_mutate_remove_super_columns_with_none_given_underneath(self, fixture_thrift_client):
        _set_keyspace(fixture_thrift_client, 'Keyspace1')

        keys = ['key_%d' % i for i in range(17, 21)]

        for key in keys:
            _insert_super(fixture_thrift_client, key)

        mutations = []

        for sc in _SUPER_COLUMNS:
            mutations.append(Mutation(deletion=Deletion(20,
                                                        super_column=sc.name)))

        mutation_map = {'Super1': mutations}

        keyed_mutations = dict((key, mutation_map) for key in keys)

        # Sanity check
        for sc in _SUPER_COLUMNS:
            for key in keys:
                _assert_columnpath_exists(fixture_thrift_client, key, ColumnPath('Super1', super_column=sc.name))

        fixture_thrift_client.batch_mutate(keyed_mutations, ConsistencyLevel.ONE)

        for sc in _SUPER_COLUMNS:
            for c in sc.columns:
                for key in keys:
                    _assert_no_columnpath(fixture_thrift_client, key, ColumnPath('Super1', super_column=sc.name))

    @pytest.mark.skip("Super columns not implemented")
    def test_batch_mutate_remove_super_columns_entire_row(self, fixture_thrift_client):
        _set_keyspace(fixture_thrift_client, 'Keyspace1')

        keys = ['key_%d' % i for i in range(17, 21)]

        for key in keys:
            _insert_super(fixture_thrift_client, key)

        mutations = []

        mutations.append(Mutation(deletion=Deletion(20)))

        mutation_map = {'Super1': mutations}

        keyed_mutations = dict((key, mutation_map) for key in keys)

        # Sanity check
        for sc in _SUPER_COLUMNS:
            for key in keys:
                _assert_columnpath_exists(fixture_thrift_client, key, ColumnPath('Super1', super_column=sc.name))

        fixture_thrift_client.batch_mutate(keyed_mutations, ConsistencyLevel.ONE)

        for sc in _SUPER_COLUMNS:
            for key in keys:
                _assert_no_columnpath(fixture_thrift_client, key, ColumnPath('Super1', super_column=sc.name))

    def test_batch_mutate_remove_slice_standard(self, fixture_thrift_client):
        _set_keyspace(fixture_thrift_client, 'Keyspace1')

        columns = [Column('c1', 'value1', 0),
                   Column('c2', 'value2', 0),
                   Column('c3', 'value3', 0),
                   Column('c4', 'value4', 0),
                   Column('c5', 'value5', 0)]

        for column in columns:
            fixture_thrift_client.insert('key', ColumnParent('Standard1'), column, ConsistencyLevel.ONE)

        d = Deletion(1, predicate=SlicePredicate(slice_range=SliceRange(start='c2', finish='c4')))
        fixture_thrift_client.batch_mutate({'key': {'Standard1': [Mutation(deletion=d)]}}, ConsistencyLevel.ONE)

        _assert_columnpath_exists(fixture_thrift_client, 'key', ColumnPath('Standard1', column='c1'))
        _assert_no_columnpath(fixture_thrift_client, 'key', ColumnPath('Standard1', column='c2'))
        _assert_no_columnpath(fixture_thrift_client, 'key', ColumnPath('Standard1', column='c3'))
        _assert_no_columnpath(fixture_thrift_client, 'key', ColumnPath('Standard1', column='c4'))
        _assert_columnpath_exists(fixture_thrift_client, 'key', ColumnPath('Standard1', column='c5'))

    # known failure: see CASSANDRA-10046
    @pytest.mark.skip("Super columns not implemented")
    def test_batch_mutate_remove_slice_of_entire_supercolumns(self, fixture_thrift_client):
        _set_keyspace(fixture_thrift_client, 'Keyspace1')

        columns = [SuperColumn(name='sc1', columns=[Column(_i64(1), 'value1', 0)]),
                   SuperColumn(name='sc2',
                               columns=[Column(_i64(2), 'value2', 0), Column(_i64(3), 'value3', 0)]),
                   SuperColumn(name='sc3', columns=[Column(_i64(4), 'value4', 0)]),
                   SuperColumn(name='sc4',
                               columns=[Column(_i64(5), 'value5', 0), Column(_i64(6), 'value6', 0)]),
                   SuperColumn(name='sc5', columns=[Column(_i64(7), 'value7', 0)])]

        for column in columns:
            for subcolumn in column.columns:
                fixture_thrift_client.insert('key', ColumnParent('Super1', column.name), subcolumn,
                                             ConsistencyLevel.ONE)

        d = Deletion(1, predicate=SlicePredicate(slice_range=SliceRange(start='sc2', finish='sc4')))
        fixture_thrift_client.batch_mutate({'key': {'Super1': [Mutation(deletion=d)]}}, ConsistencyLevel.ONE)

        _assert_columnpath_exists(fixture_thrift_client, 'key',
                                  ColumnPath('Super1', super_column='sc1', column=_i64(1)))
        _assert_no_columnpath(fixture_thrift_client, 'key', ColumnPath('Super1', super_column='sc2', column=_i64(2)))
        _assert_no_columnpath(fixture_thrift_client, 'key', ColumnPath('Super1', super_column='sc2', column=_i64(3)))
        _assert_no_columnpath(fixture_thrift_client, 'key', ColumnPath('Super1', super_column='sc3', column=_i64(4)))
        _assert_no_columnpath(fixture_thrift_client, 'key', ColumnPath('Super1', super_column='sc4', column=_i64(5)))
        _assert_no_columnpath(fixture_thrift_client, 'key', ColumnPath('Super1', super_column='sc4', column=_i64(6)))
        _assert_columnpath_exists(fixture_thrift_client, 'key',
                                  ColumnPath('Super1', super_column='sc5', column=_i64(7)))

    @pytest.mark.skip("Super columns not implemented")
    def test_batch_mutate_remove_slice_part_of_supercolumns(self, fixture_thrift_client):
        _set_keyspace(fixture_thrift_client, 'Keyspace1')

        columns = [Column(_i64(1), 'value1', 0),
                   Column(_i64(2), 'value2', 0),
                   Column(_i64(3), 'value3', 0),
                   Column(_i64(4), 'value4', 0),
                   Column(_i64(5), 'value5', 0)]

        for column in columns:
            fixture_thrift_client.insert('key', ColumnParent('Super1', 'sc1'), column, ConsistencyLevel.ONE)

        r = SliceRange(start=_i64(2), finish=_i64(4))
        d = Deletion(1, super_column='sc1', predicate=SlicePredicate(slice_range=r))
        fixture_thrift_client.batch_mutate({'key': {'Super1': [Mutation(deletion=d)]}}, ConsistencyLevel.ONE)

        _assert_columnpath_exists(fixture_thrift_client, 'key',
                                  ColumnPath('Super1', super_column='sc1', column=_i64(1)))
        _assert_no_columnpath(fixture_thrift_client, 'key', ColumnPath('Super1', super_column='sc1', column=_i64(2)))
        _assert_no_columnpath(fixture_thrift_client, 'key', ColumnPath('Super1', super_column='sc1', column=_i64(3)))
        _assert_no_columnpath(fixture_thrift_client, 'key', ColumnPath('Super1', super_column='sc1', column=_i64(4)))
        _assert_columnpath_exists(fixture_thrift_client, 'key',
                                  ColumnPath('Super1', super_column='sc1', column=_i64(5)))

    @pytest.mark.skip("Super columns not implemented")
    def test_batch_mutate_insertions_and_deletions(self, fixture_thrift_client):
        _set_keyspace(fixture_thrift_client, 'Keyspace1')

        first_insert = SuperColumn("sc1",
                                   columns=[Column(_i64(20), 'value20', 3),
                                            Column(_i64(21), 'value21', 3)])
        second_insert = SuperColumn("sc1",
                                    columns=[Column(_i64(20), 'value20', 3),
                                             Column(_i64(21), 'value21', 3)])
        first_deletion = {'super_column': "sc1",
                          'predicate': SlicePredicate(column_names=[_i64(22), _i64(23)])}
        second_deletion = {'super_column': "sc2",
                           'predicate': SlicePredicate(column_names=[_i64(22), _i64(23)])}

        keys = ['key_30', 'key_31']
        for key in keys:
            sc = SuperColumn('sc1', [Column(_i64(22), 'value22', 0),
                                     Column(_i64(23), 'value23', 0)])
            cfmap = {'Super1': [Mutation(ColumnOrSuperColumn(super_column=sc))]}
            fixture_thrift_client.batch_mutate({key: cfmap}, ConsistencyLevel.ONE)

            sc2 = SuperColumn('sc2', [Column(_i64(22), 'value22', 0),
                                      Column(_i64(23), 'value23', 0)])
            cfmap2 = {'Super2': [Mutation(ColumnOrSuperColumn(super_column=sc2))]}
            fixture_thrift_client.batch_mutate({key: cfmap2}, ConsistencyLevel.ONE)

        cfmap3 = {
            'Super1': [Mutation(ColumnOrSuperColumn(super_column=first_insert)),
                       Mutation(deletion=Deletion(3, **first_deletion))],

            'Super2': [Mutation(deletion=Deletion(2, **second_deletion)),
                       Mutation(ColumnOrSuperColumn(super_column=second_insert))]
        }

        keyed_mutations = dict((key, cfmap3) for key in keys)
        fixture_thrift_client.batch_mutate(keyed_mutations, ConsistencyLevel.ONE)

        for key in keys:
            for c in [_i64(22), _i64(23)]:
                _assert_no_columnpath(fixture_thrift_client, key, ColumnPath('Super1', super_column='sc1', column=c))
                _assert_no_columnpath(fixture_thrift_client, key, ColumnPath('Super2', super_column='sc2', column=c))

            for c in [_i64(20), _i64(21)]:
                _assert_columnpath_exists(fixture_thrift_client, key,
                                          ColumnPath('Super1', super_column='sc1', column=c))
                _assert_columnpath_exists(fixture_thrift_client, key,
                                          ColumnPath('Super2', super_column='sc1', column=c))

    @pytest.mark.skip("Indexes not implemented")
    def test_bad_system_calls(self, fixture_thrift_client):
        def duplicate_index_names():
            _set_keyspace(fixture_thrift_client, 'Keyspace1')
            cd1 = ColumnDef('foo', 'BytesType', IndexType.KEYS, 'i')
            cd2 = ColumnDef('bar', 'BytesType', IndexType.KEYS, 'i')
            cf = CfDef('Keyspace1', 'BadCF', column_metadata=[cd1, cd2])
            fixture_thrift_client.system_add_column_family(cf)

        _expect_exception(duplicate_index_names, InvalidRequestException)

    def test_bad_batch_calls(self, fixture_thrift_client):
        # mutate_does_not_accept_cosc_and_deletion_in_same_mutation
        def too_full():
            _set_keyspace(fixture_thrift_client, 'Keyspace1')
            col = ColumnOrSuperColumn(column=Column("foo", 'bar', 0))
            dele = Deletion(2, predicate=SlicePredicate(column_names=['baz']))
            fixture_thrift_client.batch_mutate({'key_34': {'Standard1': [Mutation(col, dele)]}},
                                               ConsistencyLevel.ONE)

        # test_batch_mutate_does_not_accept_cosc_on_undefined_cf:

        def bad_cf():
            _set_keyspace(fixture_thrift_client, 'Keyspace1')
            col = ColumnOrSuperColumn(column=Column("foo", 'bar', 0))
            fixture_thrift_client.batch_mutate({'key_36': {'Undefined': [Mutation(col)]}},
                                               ConsistencyLevel.ONE)

        _expect_exception(bad_cf, InvalidRequestException)

        # test_batch_mutate_does_not_accept_deletion_on_undefined_cf
        def bad_cf_2():
            _set_keyspace(fixture_thrift_client, 'Keyspace1')
            d = Deletion(2, predicate=SlicePredicate(column_names=['baz']))
            fixture_thrift_client.batch_mutate({'key_37': {'Undefined': [Mutation(deletion=d)]}},
                                               ConsistencyLevel.ONE)

        _expect_exception(bad_cf_2, InvalidRequestException)

        # a column value that does not match the declared validator
        def send_string_instead_of_long():
            _set_keyspace(fixture_thrift_client, 'Keyspace1')
            col = ColumnOrSuperColumn(column=Column('birthdate', 'bar', 0))
            fixture_thrift_client.batch_mutate({'key_38': {'Indexed1': [Mutation(col)]}},
                                               ConsistencyLevel.ONE)

        _expect_exception(send_string_instead_of_long, InvalidRequestException)

    def test_column_name_lengths(self, fixture_thrift_client):
        _set_keyspace(fixture_thrift_client, 'Keyspace1')
        _expect_exception(lambda: fixture_thrift_client.insert('key1', ColumnParent('Standard1'), Column(
            '', 'value', 0), ConsistencyLevel.ONE), InvalidRequestException)
        fixture_thrift_client.insert('key1', ColumnParent('Standard1'), Column('x' * 1, 'value', 0),
                                     ConsistencyLevel.ONE)
        fixture_thrift_client.insert('key1', ColumnParent('Standard1'), Column('x' * 127, 'value', 0),
                                     ConsistencyLevel.ONE)
        fixture_thrift_client.insert('key1', ColumnParent('Standard1'), Column('x' * 128, 'value', 0),
                                     ConsistencyLevel.ONE)
        fixture_thrift_client.insert('key1', ColumnParent('Standard1'), Column('x' * 129, 'value', 0),
                                     ConsistencyLevel.ONE)
        fixture_thrift_client.insert('key1', ColumnParent('Standard1'), Column('x' * 255, 'value', 0),
                                     ConsistencyLevel.ONE)
        fixture_thrift_client.insert('key1', ColumnParent('Standard1'), Column('x' * 256, 'value', 0),
                                     ConsistencyLevel.ONE)
        fixture_thrift_client.insert('key1', ColumnParent('Standard1'), Column('x' * 257, 'value', 0),
                                     ConsistencyLevel.ONE)
        fixture_thrift_client.insert('key1', ColumnParent('Standard1'), Column('x' * (2 ** 16 - 3), 'value', 0),
                                     ConsistencyLevel.ONE)
        _expect_exception(lambda: fixture_thrift_client.insert('key1', ColumnParent('Standard1'), Column(
            'x' * (2 ** 16), 'value', 0), ConsistencyLevel.ONE), InvalidRequestException)

    def test_bad_calls(self, fixture_thrift_client):
        _set_keyspace(fixture_thrift_client, 'Keyspace1')
        # missing arguments
        # _expect_exception(lambda: fixture_thrift_client.insert(None, None, None, None), TApplicationException)
        # supercolumn in a non-super CF
        # _expect_exception(lambda: fixture_thrift_client.insert('key1', ColumnParent('Standard1', 'x'), Column('y', 'value', 0), ConsistencyLevel.ONE), InvalidRequestException)
        # no supercolumn in a super CF
        # _expect_exception(lambda: fixture_thrift_client.insert('key1', ColumnParent('Super1'), Column('y', 'value', 0), ConsistencyLevel.ONE), InvalidRequestException)
        # column but no supercolumn in remove
        # _expect_exception(lambda: fixture_thrift_client.remove('key1', ColumnPath('Super1', column='x'), 0, ConsistencyLevel.ONE), InvalidRequestException)
        # super column in non-super CF
        # _expect_exception(lambda: fixture_thrift_client.remove('key1', ColumnPath('Standard1', 'y', 'x'), 0, ConsistencyLevel.ONE), InvalidRequestException)
        # key too long
        _expect_exception(lambda: fixture_thrift_client.get('x' * 2 ** 16, ColumnPath('Standard1', column='c1'),
                                                            ConsistencyLevel.ONE), InvalidRequestException)
        # empty key
        _expect_exception(lambda: fixture_thrift_client.get('', ColumnPath('Standard1', column='c1'),
                                                            ConsistencyLevel.ONE), InvalidRequestException)
        cfmap = {'Super1': [Mutation(ColumnOrSuperColumn(super_column=c)) for c in _SUPER_COLUMNS],
                 'Super2': [Mutation(ColumnOrSuperColumn(super_column=c)) for c in _SUPER_COLUMNS]}
        _expect_exception(lambda: fixture_thrift_client.batch_mutate({'': cfmap}, ConsistencyLevel.ONE),
                          InvalidRequestException)
        # empty column name
        _expect_exception(lambda: fixture_thrift_client.get('key1', ColumnPath('Standard1', column=''),
                                                            ConsistencyLevel.ONE), InvalidRequestException)
        # get doesn't specify column name
        _expect_exception(lambda: fixture_thrift_client.get('key1', ColumnPath('Standard1'),
                                                            ConsistencyLevel.ONE), InvalidRequestException)
        # supercolumn in a non-super CF
        # _expect_exception(lambda: fixture_thrift_client.get('key1', ColumnPath('Standard1', 'x', 'y'), ConsistencyLevel.ONE), InvalidRequestException)
        # get doesn't specify supercolumn name
        # _expect_exception(lambda: fixture_thrift_client.get('key1', ColumnPath('Super1'), ConsistencyLevel.ONE), InvalidRequestException)
        # invalid CF
        _expect_exception(lambda: get_range_slice(fixture_thrift_client, ColumnParent('S'), SlicePredicate(
            column_names=['', '']), '', '', 5, ConsistencyLevel.ONE), InvalidRequestException)
        # 'x' is not a valid Long
        # _expect_exception(lambda: fixture_thrift_client.insert('key1', ColumnParent('Super1', 'sc1'), Column('x', 'value', 0), ConsistencyLevel.ONE), InvalidRequestException)
        # start is not a valid Long
        # p = SlicePredicate(slice_range=SliceRange('x', '', False, 1))
        # column_parent = ColumnParent('StandardLong1')
        # _expect_exception(lambda: fixture_thrift_client.get_slice('key1', column_parent, p, ConsistencyLevel.ONE),
        #                  InvalidRequestException)
        # start > finish
        p = SlicePredicate(slice_range=SliceRange(_i64(10), _i64(0), False, 1))
        column_parent = ColumnParent('StandardLong1')
        _expect_exception(lambda: fixture_thrift_client.get_slice('key1', column_parent, p, ConsistencyLevel.ONE),
                          InvalidRequestException)
        # start is not a valid Long, supercolumn version
        # p = SlicePredicate(slice_range=SliceRange('x', '', False, 1))
        # column_parent = ColumnParent('Super1', 'sc1')
        # _expect_exception(lambda: fixture_thrift_client.get_slice('key1', column_parent, p, ConsistencyLevel.ONE),
        #                  InvalidRequestException)
        # start > finish, supercolumn version
        # p = SlicePredicate(slice_range=SliceRange(_i64(10), _i64(0), False, 1))
        # column_parent = ColumnParent('Super1', 'sc1')
        # _expect_exception(lambda: fixture_thrift_client.get_slice('key1', column_parent, p, ConsistencyLevel.ONE),
        #                  InvalidRequestException)
        # start > finish, key version
        _expect_exception(lambda: get_range_slice(fixture_thrift_client, ColumnParent('Standard1'), SlicePredicate(
            column_names=['']), 'z', 'a', 1, ConsistencyLevel.ONE), InvalidRequestException)
        # ttl must be positive
        column = Column('cttl1', 'value1', 0, 0)
        _expect_exception(
            lambda: fixture_thrift_client.insert('key1', ColumnParent('Standard1'), column, ConsistencyLevel.ONE),
            InvalidRequestException)
        # don't allow super_column in Deletion for standard ColumnFamily
        # deletion = Deletion(1, 'supercolumn', None)
        # mutation = Mutation(deletion=deletion)
        # mutations = {'key': {'Standard1': [mutation]}}
        # _expect_exception(lambda: fixture_thrift_client.batch_mutate(mutations, ConsistencyLevel.QUORUM),
        #                  InvalidRequestException)
        # 'x' is not a valid long
        # deletion = Deletion(1, 'x', None)
        # mutation = Mutation(deletion=deletion)
        # mutations = {'key': {'Super5': [mutation]}}
        # _expect_exception(lambda: fixture_thrift_client.batch_mutate(mutations, ConsistencyLevel.QUORUM), InvalidRequestException)
        # counters don't support ANY
        # _expect_exception(lambda: fixture_thrift_client.add('key1', ColumnParent('Counter1', 'x'), CounterColumn('y', 1), ConsistencyLevel.ANY), InvalidRequestException)

    @pytest.mark.skip("Super columns not implemented")
    def test_batch_insert_super(self, fixture_thrift_client):
        _set_keyspace(fixture_thrift_client, 'Keyspace1')
        cfmap = {'Super1': [Mutation(ColumnOrSuperColumn(super_column=c))
                            for c in _SUPER_COLUMNS],
                 'Super2': [Mutation(ColumnOrSuperColumn(super_column=c))
                            for c in _SUPER_COLUMNS]}
        fixture_thrift_client.batch_mutate({'key1': cfmap}, ConsistencyLevel.ONE)
        _verify_super(fixture_thrift_client, 'Super1')
        _verify_super(fixture_thrift_client, 'Super2')

    @pytest.mark.skip("Super columns not implemented")
    def test_batch_insert_super_blocking(self, fixture_thrift_client):
        _set_keyspace(fixture_thrift_client, 'Keyspace1')
        cfmap = {'Super1': [Mutation(ColumnOrSuperColumn(super_column=c))
                            for c in _SUPER_COLUMNS],
                 'Super2': [Mutation(ColumnOrSuperColumn(super_column=c))
                            for c in _SUPER_COLUMNS]}
        fixture_thrift_client.batch_mutate({'key1': cfmap}, ConsistencyLevel.ONE)
        _verify_super(fixture_thrift_client, 'Super1')
        _verify_super(fixture_thrift_client, 'Super2')

    def test_cf_remove_column(self, fixture_thrift_client):
        _set_keyspace(fixture_thrift_client, 'Keyspace1')
        _insert_simple(fixture_thrift_client, )
        fixture_thrift_client.remove('key1', ColumnPath('Standard1', column='c1'), 1, ConsistencyLevel.ONE)
        _expect_missing(
            lambda: fixture_thrift_client.get('key1', ColumnPath('Standard1', column='c1'), ConsistencyLevel.ONE))
        assert fixture_thrift_client.get('key1', ColumnPath('Standard1', column='c2'), ConsistencyLevel.ONE).column \
            == Column('c2', 'value2', 0)
        assert _big_slice(fixture_thrift_client, 'key1', ColumnParent('Standard1')) \
            == [ColumnOrSuperColumn(column=Column('c2', 'value2', 0))]

        # New insert, make sure it shows up post-remove:
        fixture_thrift_client.insert('key1', ColumnParent('Standard1'), Column('c3', 'value3', 0), ConsistencyLevel.ONE)
        columns = [result.column
                   for result in _big_slice(fixture_thrift_client, 'key1', ColumnParent('Standard1'))]
        assert columns == [Column('c2', 'value2', 0), Column('c3', 'value3', 0)], columns

        # Test resurrection.  First, re-insert the value w/ older timestamp,
        # and make sure it stays removed
        fixture_thrift_client.insert('key1', ColumnParent('Standard1'), Column('c1', 'value1', 0), ConsistencyLevel.ONE)
        columns = [result.column
                   for result in _big_slice(fixture_thrift_client, 'key1', ColumnParent('Standard1'))]
        assert columns == [Column('c2', 'value2', 0), Column('c3', 'value3', 0)], columns
        # Next, w/ a newer timestamp; it should come back:
        fixture_thrift_client.insert('key1', ColumnParent('Standard1'), Column('c1', 'value1', 2), ConsistencyLevel.ONE)
        columns = [result.column
                   for result in _big_slice(fixture_thrift_client, 'key1', ColumnParent('Standard1'))]
        assert columns == [Column('c1', 'value1', 2), Column('c2', 'value2', 0), Column('c3', 'value3', 0)], columns

    def test_cf_remove(self, fixture_thrift_client):
        _set_keyspace(fixture_thrift_client, 'Keyspace1')

        _insert_simple(fixture_thrift_client, )
        # _insert_super(fixture_thrift_client)

        # Remove the key1:Standard1 cf; verify super is unaffected
        fixture_thrift_client.remove('key1', ColumnPath('Standard1'), 3, ConsistencyLevel.ONE)
        assert _big_slice(fixture_thrift_client, 'key1', ColumnParent('Standard1')) == []
        # _verify_super(fixture_thrift_client,)

        # Test resurrection.  First, re-insert a value w/ older timestamp,
        # and make sure it stays removed:
        fixture_thrift_client.insert('key1', ColumnParent('Standard1'), Column('c1', 'value1', 0), ConsistencyLevel.ONE)
        assert _big_slice(fixture_thrift_client, 'key1', ColumnParent('Standard1')) == []
        # Next, w/ a newer timestamp; it should come back:
        fixture_thrift_client.insert('key1', ColumnParent('Standard1'), Column('c1', 'value1', 4), ConsistencyLevel.ONE)
        result = _big_slice(fixture_thrift_client, 'key1', ColumnParent('Standard1'))
        assert result == [ColumnOrSuperColumn(column=Column('c1', 'value1', 4))], result

        # check removing the entire super cf, too.
        # client.remove('key1', ColumnPath('Super1'), 3, ConsistencyLevel.ONE)
        # assert _big_slice(fixture_thrift_client, 'key1', ColumnParent('Super1')) == []
        # assert _big_slice(fixture_thrift_client, 'key1', ColumnParent('Super1', 'sc1')) == []

    @pytest.mark.skip("Super columns not implemented")
    def test_super_cf_remove_and_range_slice(self, fixture_thrift_client):
        _set_keyspace(fixture_thrift_client, 'Keyspace1')

        fixture_thrift_client.insert('key3', ColumnParent('Super1', 'sc1'), Column(_i64(1), 'v1', 0),
                                     ConsistencyLevel.ONE)
        fixture_thrift_client.remove('key3', ColumnPath('Super1', 'sc1'), 5, ConsistencyLevel.ONE)

        rows = {}
        for row in get_range_slice(fixture_thrift_client, ColumnParent('Super1'),
                                   SlicePredicate(slice_range=SliceRange('', '', False, 1000)), '', '', 1000,
                                   ConsistencyLevel.ONE):
            scs = [cosc.super_column for cosc in row.columns]
            rows[row.key] = scs
        assert rows == {'key3': []}, rows

    @pytest.mark.skip("Super columns not implemented")
    def test_super_cf_remove_column(self, fixture_thrift_client):
        _set_keyspace(fixture_thrift_client, 'Keyspace1')
        _insert_simple(fixture_thrift_client, )
        _insert_super(fixture_thrift_client)

        # Make sure remove clears out what it's supposed to, and _only_ that:
        fixture_thrift_client.remove('key1', ColumnPath('Super1', 'sc2', _i64(5)), 5, ConsistencyLevel.ONE)
        _expect_missing(
            lambda: fixture_thrift_client.get('key1', ColumnPath('Super1', 'sc2', _i64(5)), ConsistencyLevel.ONE))
        super_columns = [result.super_column for result in
                         _big_slice(fixture_thrift_client, 'key1', ColumnParent('Super1'))]
        assert super_columns == [SuperColumn(name='sc1', columns=[Column(_i64(4), 'value4', 0)]),
                                 SuperColumn(name='sc2', columns=[Column(_i64(6), 'value6', 0)])]
        _verify_simple(fixture_thrift_client)

        # New insert, make sure it shows up post-remove:
        fixture_thrift_client.insert('key1', ColumnParent('Super1', 'sc2'), Column(_i64(7), 'value7', 0),
                                     ConsistencyLevel.ONE)
        super_columns_expected = [SuperColumn(name='sc1',
                                              columns=[Column(_i64(4), 'value4', 0)]),
                                  SuperColumn(name='sc2',
                                              columns=[Column(_i64(6), 'value6', 0), Column(_i64(7), 'value7', 0)])]

        super_columns = [result.super_column for result in
                         _big_slice(fixture_thrift_client, 'key1', ColumnParent('Super1'))]
        assert super_columns == super_columns_expected, super_columns

        # Test resurrection.  First, re-insert the value w/ older timestamp,
        # and make sure it stays removed:
        fixture_thrift_client.insert('key1', ColumnParent('Super1', 'sc2'), Column(_i64(5), 'value5', 0),
                                     ConsistencyLevel.ONE)

        super_columns = [result.super_column for result in
                         _big_slice(fixture_thrift_client, 'key1', ColumnParent('Super1'))]
        assert super_columns == super_columns_expected, super_columns

        # Next, w/ a newer timestamp; it should come back
        fixture_thrift_client.insert('key1', ColumnParent('Super1', 'sc2'), Column(_i64(5), 'value5', 6),
                                     ConsistencyLevel.ONE)
        super_columns = [result.super_column for result in
                         _big_slice(fixture_thrift_client, 'key1', ColumnParent('Super1'))]
        super_columns_expected = [SuperColumn(name='sc1', columns=[Column(_i64(4), 'value4', 0)]),
                                  SuperColumn(name='sc2', columns=[Column(_i64(5), 'value5', 6),
                                                                   Column(_i64(6), 'value6', 0),
                                                                   Column(_i64(7), 'value7', 0)])]
        assert super_columns == super_columns_expected, super_columns

        # shouldn't be able to specify a column w/o a super column for remove
        cp = ColumnPath(column_family='Super1', column='sc2')
        e = _expect_exception(lambda: fixture_thrift_client.remove('key1', cp, 5, ConsistencyLevel.ONE),
                              InvalidRequestException)
        assert e.why.find("column cannot be specified without") >= 0

    @pytest.mark.skip("Super columns not implemented")
    def test_super_cf_remove_supercolumn(self, fixture_thrift_client):
        _set_keyspace(fixture_thrift_client, 'Keyspace1')

        _insert_simple(fixture_thrift_client, )
        _insert_super(fixture_thrift_client)

        # Make sure remove clears out what it's supposed to, and _only_ that:
        fixture_thrift_client.remove('key1', ColumnPath('Super1', 'sc2'), 5, ConsistencyLevel.ONE)
        _expect_missing(
            lambda: fixture_thrift_client.get('key1', ColumnPath('Super1', 'sc2', _i64(5)), ConsistencyLevel.ONE))
        super_columns = _big_slice(fixture_thrift_client, 'key1', ColumnParent('Super1', 'sc2'))
        assert super_columns == [], super_columns
        super_columns_expected = [SuperColumn(name='sc1', columns=[Column(_i64(4), 'value4', 0)])]
        super_columns = [result.super_column
                         for result in _big_slice(fixture_thrift_client, 'key1', ColumnParent('Super1'))]
        assert super_columns == super_columns_expected, super_columns
        _verify_simple(fixture_thrift_client)

        # Test resurrection.  First, re-insert the value w/ older timestamp,
        # and make sure it stays removed:
        fixture_thrift_client.insert('key1', ColumnParent('Super1', 'sc2'), Column(_i64(5), 'value5', 1),
                                     ConsistencyLevel.ONE)
        super_columns = [result.super_column
                         for result in _big_slice(fixture_thrift_client, 'key1', ColumnParent('Super1'))]
        assert super_columns == super_columns_expected, super_columns

        # Next, w/ a newer timestamp; it should come back
        fixture_thrift_client.insert('key1', ColumnParent('Super1', 'sc2'), Column(_i64(5), 'value5', 6),
                                     ConsistencyLevel.ONE)
        super_columns = [result.super_column
                         for result in _big_slice(fixture_thrift_client, 'key1', ColumnParent('Super1'))]
        super_columns_expected = [SuperColumn(name='sc1', columns=[Column(_i64(4), 'value4', 0)]),
                                  SuperColumn(name='sc2', columns=[Column(_i64(5), 'value5', 6)])]
        assert super_columns == super_columns_expected, super_columns

        # check slicing at the subcolumn level too
        p = SlicePredicate(slice_range=SliceRange('', '', False, 1000))
        columns = [result.column
                   for result in
                   fixture_thrift_client.get_slice('key1', ColumnParent('Super1', 'sc2'), p, ConsistencyLevel.ONE)]
        assert columns == [Column(_i64(5), 'value5', 6)], columns

    @pytest.mark.skip("Super columns not implemented")
    def test_super_cf_resurrect_subcolumn(self, fixture_thrift_client):
        _set_keyspace(fixture_thrift_client, 'Keyspace1')
        key = 'vijay'
        fixture_thrift_client.insert(key, ColumnParent('Super1', 'sc1'), Column(_i64(4), 'value4', 0),
                                     ConsistencyLevel.ONE)

        fixture_thrift_client.remove(key, ColumnPath('Super1', 'sc1'), 1, ConsistencyLevel.ONE)

        fixture_thrift_client.insert(key, ColumnParent('Super1', 'sc1'), Column(_i64(4), 'value4', 2),
                                     ConsistencyLevel.ONE)

        result = fixture_thrift_client.get(key, ColumnPath('Super1', 'sc1'), ConsistencyLevel.ONE)
        assert result.super_column.columns is not None, result.super_column

    def test_empty_range(self, fixture_thrift_client):
        _set_keyspace(fixture_thrift_client, 'Keyspace1')
        assert get_range_slice(fixture_thrift_client, ColumnParent('Standard1'), SlicePredicate(
            column_names=['c1', 'c1']), '', '', 1000, ConsistencyLevel.ONE) == []
        # _insert_simple(fixture_thrift_client, )
        # assert get_range_slice(client, ColumnParent('Super1'), SlicePredicate(column_names=['c1', 'c1']), '', '', 1000, ConsistencyLevel.ONE) == []

    def test_range_with_remove(self, fixture_thrift_client):
        _set_keyspace(fixture_thrift_client, 'Keyspace1')
        _insert_simple(fixture_thrift_client, )
        assert get_range_slice(fixture_thrift_client, ColumnParent('Standard1'), SlicePredicate(
            column_names=['c1', 'c2']), 'key1', '', 1000, ConsistencyLevel.ONE)[0].key == b'key1'

        fixture_thrift_client.remove('key1', ColumnPath('Standard1', column='c1'), 1, ConsistencyLevel.ONE)
        fixture_thrift_client.remove('key1', ColumnPath('Standard1', column='c2'), 1, ConsistencyLevel.ONE)
        actual = get_range_slice(fixture_thrift_client, ColumnParent('Standard1'), SlicePredicate(
            column_names=['c1', 'c2']), '', '', 1000, ConsistencyLevel.ONE)
        # assert actual == [KeySlice(columns=[], key='key1')], actual
        assert actual == [], actual

    def test_range_with_remove_cf(self, fixture_thrift_client):
        _set_keyspace(fixture_thrift_client, 'Keyspace1')
        _insert_simple(fixture_thrift_client, )
        assert get_range_slice(fixture_thrift_client, ColumnParent('Standard1'), SlicePredicate(
            column_names=['c1', 'c1']), 'key1', '', 1000, ConsistencyLevel.ONE)[0].key == b'key1'

        fixture_thrift_client.remove('key1', ColumnPath('Standard1'), 1, ConsistencyLevel.ONE)
        actual = get_range_slice(fixture_thrift_client, ColumnParent('Standard1'), SlicePredicate(
            column_names=['c1', 'c1']), '', '', 1000, ConsistencyLevel.ONE)
        # assert actual == [KeySlice(columns=[], key='key1')], actual
        assert actual == [], actual

    def test_range_collation(self, fixture_thrift_client):
        _set_keyspace(fixture_thrift_client, 'Keyspace1')
        # The order of keys with Murmur3 is the following:
        # key17, key5, key20, key13, key6, key10, key14, key16, key11, key1,
        # key9, key19, key12, key18, key4, key8, key3, key7, key15, key2
        for key in ['key' + str(i + 1) for i in range(20)]:
            fixture_thrift_client.insert(key, ColumnParent('Standard1'), Column(key, 'v', 0), ConsistencyLevel.ONE)

        # slices = get_range_slice(client, ColumnParent('Standard1'), SlicePredicate(column_names=['-a', '-a']), '', '', 1000, ConsistencyLevel.ONE)
        slices = get_range_slice(fixture_thrift_client, ColumnParent('Standard1'), SlicePredicate(
            slice_range=SliceRange('', '', False, 1000)), '', '', 1000, ConsistencyLevel.ONE)
        L = ['key17', 'key5', 'key20', 'key13', 'key6', 'key10', 'key14',
             'key16', 'key11', 'key1', 'key9', 'key19', 'key12', 'key18',
             'key4', 'key8', 'key3', 'key7', 'key15', 'key2']
        assert len(slices) == len(L)
        for key, ks in zip(L, slices):
            assert key.encode('utf-8') == ks.key

    def test_range_partial(self, fixture_thrift_client):
        _set_keyspace(fixture_thrift_client, 'Keyspace1')

        # The order of keys with Murmur3 is the following:
        # key17, key5, key20, key13, key6, key10, key14, key16, key11, key1,
        # key9, key19, key12, key18, key4, key8, key3, key7, key15, key2
        for key in ['key' + str(i + 1) for i in range(20)]:
            fixture_thrift_client.insert(key, ColumnParent('Standard1'), Column(key, 'v', 0), ConsistencyLevel.ONE)

        def check_slices_against_keys(keyList, sliceList):
            assert len(keyList) == len(sliceList), "%d vs %d" % (len(keyList), len(sliceList))
            for key, ks in zip(keyList, sliceList):
                assert key.encode('utf-8') == ks.key

        # slices = get_range_slice(client, ColumnParent('Standard1'), SlicePredicate(column_names=['-a', '-a']), 'a', '', 1000, ConsistencyLevel.ONE)
        slices = get_range_slice(fixture_thrift_client, ColumnParent('Standard1'), SlicePredicate(
            slice_range=SliceRange('', '', False, 1000)), 'key15', '', 1000, ConsistencyLevel.ONE)
        check_slices_against_keys(['key15', 'key2'], slices)

        # slices = get_range_slice(client, ColumnParent('Standard1'), SlicePredicate(column_names=['-a', '-a']), '', '15', 1000, ConsistencyLevel.ONE)
        slices = get_range_slice(fixture_thrift_client, ColumnParent('Standard1'), SlicePredicate(
            slice_range=SliceRange('', '', False, 1000)), '', 'key1', 1000, ConsistencyLevel.ONE)
        check_slices_against_keys(['key17', 'key5', 'key20', 'key13', 'key6',
                                   'key10', 'key14', 'key16', 'key11', 'key1'], slices)

        # slices = get_range_slice(client, ColumnParent('Standard1'), SlicePredicate(column_names=['-a', '-a']), '50', '51', 1000, ConsistencyLevel.ONE)
        slices = get_range_slice(fixture_thrift_client, ColumnParent('Standard1'), SlicePredicate(
            slice_range=SliceRange('', '', False, 1000)), 'key1', 'key9', 1000, ConsistencyLevel.ONE)
        check_slices_against_keys(['key1', 'key9'], slices)

        # slices = get_range_slice(client, ColumnParent('Standard1'), SlicePredicate(column_names=['-a', '-a']), '1', '', 10, ConsistencyLevel.ONE)
        slices = get_range_slice(fixture_thrift_client, ColumnParent('Standard1'), SlicePredicate(
            slice_range=SliceRange('', '', False, 1000)), 'key20', '', 10, ConsistencyLevel.ONE)
        check_slices_against_keys(['key20', 'key13', 'key6', 'key10', 'key14',
                                   'key16', 'key11', 'key1', 'key9', 'key19'], slices)

    def test_get_slice_range(self, fixture_thrift_client):
        _set_keyspace(fixture_thrift_client, 'Keyspace1')
        _insert_range(fixture_thrift_client)
        _verify_range(fixture_thrift_client)

    @pytest.mark.skip("Super columns not implemented")
    def test_get_slice_super_range(self, fixture_thrift_client):
        _set_keyspace(fixture_thrift_client, 'Keyspace1')
        _insert_super_range(fixture_thrift_client)
        _verify_super_range(fixture_thrift_client)

    def test_get_range_slices_tokens(self, fixture_thrift_client):
        _set_keyspace(fixture_thrift_client, 'Keyspace2')
        for key in ['key1', 'key2', 'key3', 'key4', 'key5']:
            for cname in ['col1', 'col2', 'col3', 'col4', 'col5']:
                fixture_thrift_client.insert(key, ColumnParent('Standard1'), Column(cname, 'v-' + cname, 0),
                                             ConsistencyLevel.ONE)
        cp = ColumnParent('Standard1')
        predicate = SlicePredicate(column_names=['col1', 'col3'])
        range = KeyRange(start_token='55', end_token='55', count=100)
        result = fixture_thrift_client.get_range_slices(cp, predicate, range, ConsistencyLevel.ONE)
        assert len(result) == 5
        assert result[0].columns[0].column.name == b'col1'
        assert result[0].columns[1].column.name == b'col3'

    @pytest.mark.skip("Super columns not implemented")
    def test_get_range_slice_super(self, fixture_thrift_client):
        _set_keyspace(fixture_thrift_client, 'Keyspace2')
        for key in ['key1', 'key2', 'key3', 'key4', 'key5']:
            for cname in ['col1', 'col2', 'col3', 'col4', 'col5']:
                fixture_thrift_client.insert(key, ColumnParent('Super3', 'sc1'), Column(cname, 'v-' + cname, 0),
                                             ConsistencyLevel.ONE)

        cp = ColumnParent('Super3', 'sc1')
        result = get_range_slice(fixture_thrift_client, cp, SlicePredicate(
            column_names=['col1', 'col3']), 'key2', 'key4', 5, ConsistencyLevel.ONE)
        assert len(result) == 3
        assert result[0].columns[0].column.name == b'col1'
        assert result[0].columns[1].column.name == b'col3'

        cp = ColumnParent('Super3')
        result = get_range_slice(fixture_thrift_client, cp, SlicePredicate(
            column_names=['sc1']), 'key2', 'key4', 5, ConsistencyLevel.ONE)
        assert len(result) == 3
        assert list(set(row.columns[0].super_column.name for row in result))[0] == 'sc1'

    def test_get_slice_unsorted_column_names(self, fixture_thrift_client):
        # Reproduces https://github.com/scylladb/scylla/issues/6486
        _set_keyspace(fixture_thrift_client, 'Keyspace2')
        for key in ['key1', 'key2', 'key3', 'key4', 'key5']:
            for cname in ['col1', 'col2', 'col3', 'col4', 'col5']:
                fixture_thrift_client.insert(key, ColumnParent('Standard1'), Column(cname, 'v-' + cname, 0),
                                             ConsistencyLevel.ONE)
        cp = ColumnParent('Standard1')
        predicate = SlicePredicate(column_names=['col4', 'col3', 'col1', 'col5', 'col2'])
        range = KeyRange(start_token='55', end_token='55', count=100)
        result = fixture_thrift_client.get_range_slices(cp, predicate, range, ConsistencyLevel.ONE)
        assert len(result) == 5
        assert result[0].columns[0].column.name == b'col1'
        assert result[0].columns[1].column.name == b'col2'
        assert result[0].columns[2].column.name == b'col3'
        assert result[0].columns[3].column.name == b'col4'
        assert result[0].columns[4].column.name == b'col5'

    def test_get_range_slice(self, fixture_thrift_client):
        _set_keyspace(fixture_thrift_client, 'Keyspace1')
        # The order of keys with Murmur3 is the following:
        # key17, key5, key20, key13, key6, key10, key14, key16, key11, key1,
        # key9, key19, key12, key18, key4, key8, key3, key7, key15, key2
        #
        # and the order of only present keys is:
        # key5, key1, key4, key3, key2
        for key in ['key1', 'key2', 'key3', 'key4', 'key5']:
            for cname in ['col1', 'col2', 'col3', 'col4', 'col5']:
                fixture_thrift_client.insert(key, ColumnParent('Standard1'), Column(cname, 'v-' + cname, 0),
                                             ConsistencyLevel.ONE)
        cp = ColumnParent('Standard1')

        # test empty slice
        result = get_range_slice(fixture_thrift_client, cp, SlicePredicate(
            column_names=['col1', 'col3']), '', 'key17', 1, ConsistencyLevel.ONE)
        assert len(result) == 0

        # test empty columns
        result = get_range_slice(fixture_thrift_client, cp, SlicePredicate(column_names=['a']), 'key1', '', 1,
                                 ConsistencyLevel.ONE)
        assert len(result) == 0
        # assert len(result) == 1
        # assert len(result[0].columns) == 0

        # test column_names predicate
        result = get_range_slice(fixture_thrift_client, cp, SlicePredicate(
            column_names=['col1', 'col3']), 'key1', 'key3', 5, ConsistencyLevel.ONE)
        assert len(result) == 3, result
        assert result[0].columns[0].column.name == b'col1'
        assert result[0].columns[1].column.name == b'col3'

        # row limiting via count.
        result = get_range_slice(fixture_thrift_client, cp, SlicePredicate(
            column_names=['col1', 'col3']), 'key1', 'key3', 1, ConsistencyLevel.ONE)
        assert len(result) == 1

        # test column slice predicate
        result = get_range_slice(fixture_thrift_client, cp, SlicePredicate(slice_range=SliceRange(
            start='col2', finish='col4', reversed=False, count=5)), 'key5', 'key1', 5, ConsistencyLevel.ONE)
        assert len(result) == 2
        assert result[0].key == b'key5'
        assert result[1].key == b'key1'
        assert len(result[0].columns) == 3
        assert result[0].columns[0].column.name == b'col2'
        assert result[0].columns[2].column.name == b'col4'

        # col limiting via count
        result = get_range_slice(fixture_thrift_client, cp, SlicePredicate(slice_range=SliceRange(
            start='col2', finish='col4', reversed=False, count=2)), 'key5', 'key1', 5, ConsistencyLevel.ONE)
        assert len(result[0].columns) == 2

        # and reversed
        result = get_range_slice(fixture_thrift_client, cp, SlicePredicate(slice_range=SliceRange(
            start='col4', finish='col2', reversed=True, count=5)), 'key5', 'key1', 5, ConsistencyLevel.ONE)
        assert result[0].columns[0].column.name == b'col4'
        assert result[0].columns[2].column.name == b'col2'

        # row limiting via count
        result = get_range_slice(fixture_thrift_client, cp, SlicePredicate(slice_range=SliceRange(
            start='col2', finish='col4', reversed=False, count=5)), 'key5', 'key1', 1, ConsistencyLevel.ONE)
        assert len(result) == 1

        # removed data
        fixture_thrift_client.remove('key5', ColumnPath('Standard1', column='col1'), 1, ConsistencyLevel.ONE)
        result = get_range_slice(fixture_thrift_client, cp, SlicePredicate(slice_range=SliceRange('', '')),
                                 'key5', 'key1', 5, ConsistencyLevel.ONE)
        assert len(result) == 2, result
        assert result[0].columns[0].column.name == b'col2', result[0].columns[0].column.name
        assert result[1].columns[0].column.name == b'col1'

    def test_wrapped_range_slices(self, fixture_thrift_client):
        _set_keyspace(fixture_thrift_client, 'Keyspace1')

        def copp_token(key):
            return {'key5': '-5732932993672985672',
                    'key1': '1573573083296714675',
                    'key4': '6829471020522910836',
                    'key3': '8031872927333060586',
                    'key2': '8482869187405483569'}[key]

        for key in ['key1', 'key2', 'key3', 'key4', 'key5']:
            for cname in ['col1', 'col2', 'col3', 'col4', 'col5']:
                fixture_thrift_client.insert(key, ColumnParent('Standard1'), Column(cname, 'v-' + cname, 0),
                                             ConsistencyLevel.ONE)
        cp = ColumnParent('Standard1')

        result = fixture_thrift_client.get_range_slices(cp, SlicePredicate(column_names=['col1', 'col3']), KeyRange(
            start_token=copp_token('key2'), end_token=copp_token('key2')), ConsistencyLevel.ONE)
        assert [row.key for row in result] == [b'key5', b'key1',
                                               b'key4', b'key3', b'key2', ], [row.key for row in result]

        result = fixture_thrift_client.get_range_slices(cp, SlicePredicate(column_names=['col1', 'col3']), KeyRange(
            start_token=copp_token('key4'), end_token=copp_token('key4')), ConsistencyLevel.ONE)
        assert [row.key for row in result] == [b'key3', b'key2',
                                               b'key5', b'key1', b'key4', ], [row.key for row in result]

    def test_get_slice_by_names(self, fixture_thrift_client):
        _set_keyspace(fixture_thrift_client, 'Keyspace1')
        _insert_range(fixture_thrift_client)
        p = SlicePredicate(column_names=['c1', 'c2'])
        result = fixture_thrift_client.get_slice('key1', ColumnParent('Standard1'), p, ConsistencyLevel.ONE)
        assert len(result) == 2
        assert result[0].column.name == b'c1'
        assert result[1].column.name == b'c2'

        # _insert_super(fixture_thrift_client)
        # p = SlicePredicate(column_names=[_i64(4)])
        # result = fixture_thrift_client.get_slice('key1', ColumnParent('Super1', 'sc1'), p, ConsistencyLevel.ONE)
        # assert len(result) == 1
        # assert result[0].column.name == _i64(4)

    def test_multiget_slice_with_compact_table(self, fixture_thrift_client):
        """Insert multiple keys in a compact table and retrieve them using the multiget_slice interface"""

        _set_keyspace(fixture_thrift_client, 'Keyspace1')

        # create
        cd = ColumnDef('v', 'AsciiType', None, None)
        newcf = CfDef('Keyspace1', 'CompactColumnFamily', default_validation_class='AsciiType', column_metadata=[cd])
        fixture_thrift_client.system_add_column_family(newcf)

        CL = ConsistencyLevel.ONE
        for i in range(0, 5):
            fixture_thrift_client.insert('key' + str(i), ColumnParent('CompactColumnFamily'),
                                         Column('v', 'value' + str(i), 0), CL)
        time.sleep(0.1)

        p = SlicePredicate(column_names=['v'])
        rows = fixture_thrift_client.multiget_slice(['key' + str(i) for i in range(0, 5)],
                                                    ColumnParent('CompactColumnFamily'), p, ConsistencyLevel.ONE)

        for i in range(0, 5):
            key = b'key' + str(i).encode('utf-8')
            assert key in rows
            assert len(rows[key]) == 1
            assert rows[key][0].column.name == b'v'
            assert rows[key][0].column.value == b'value' + str(i).encode('utf-8')

    def test_multiget_slice(self, fixture_thrift_client):
        """Insert multiple keys and retrieve them using the multiget_slice interface"""

        _set_keyspace(fixture_thrift_client, 'Keyspace1')
        # Generate a list of 10 keys and insert them
        num_keys = 10
        keys = [b'key' + str(i).encode('utf-8') for i in range(1, num_keys + 1)]
        _insert_multi(fixture_thrift_client, keys)

        # Retrieve all 10 key slices
        rows = _big_multislice(fixture_thrift_client, keys, ColumnParent('Standard1'))

        columns = [ColumnOrSuperColumn(c) for c in _SIMPLE_COLUMNS]
        # Validate if the returned rows have the keys requested and if the ColumnOrSuperColumn is what was inserted
        for key in keys:
            assert key in rows
            assert columns == rows[key]

    def test_multiget_slice_with_count(self, fixture_thrift_client):
        """Insert multiple keys and retrieve them using the multiget_slice interface with a cell limit"""

        _set_keyspace(fixture_thrift_client, 'Keyspace1')
        # Generate a list of 10 keys and insert them
        num_keys = 10
        keys = [b'key' + str(i).encode('utf-8') for i in range(1, num_keys + 1)]
        _insert_multi(fixture_thrift_client, keys)

        # Retrieve all 10 key slices
        rows = _big_multislice(fixture_thrift_client, keys, ColumnParent('Standard1'), 1)
        print(rows)

        columns = [ColumnOrSuperColumn(_SIMPLE_COLUMNS[0])]
        # Validate if the returned rows have the keys requested and if the ColumnOrSuperColumn is what was inserted
        for key in keys:
            assert key in rows
            assert columns == rows[key]

    def test_multi_count(self, fixture_thrift_client):
        """Insert multiple keys and count them using the multiget interface"""
        _set_keyspace(fixture_thrift_client, 'Keyspace1')

        # Generate a list of 10 keys countaining 1 to 10 columns and insert them
        num_keys = 10
        for i in range(1, num_keys + 1):
            key = 'key' + str(i)
            for j in range(1, i + 1):
                fixture_thrift_client.insert(key, ColumnParent('Standard1'), Column(
                    'c' + str(j), 'value' + str(j), 0), ConsistencyLevel.ONE)

        # Count columns in all 10 keys
        keys = ['key' + str(i) for i in range(1, num_keys + 1)]
        p = SlicePredicate(slice_range=SliceRange('', '', False, 1000))
        counts = fixture_thrift_client.multiget_count(keys, ColumnParent('Standard1'), p, ConsistencyLevel.ONE)

        # Check the returned counts
        for i in range(1, num_keys + 1):
            key = b'key' + str(i).encode('utf-8')
            assert counts[key] == i

    @pytest.mark.skip("Super columns not implemented")
    def test_batch_mutate_super_deletion(self, fixture_thrift_client):
        _set_keyspace(fixture_thrift_client, 'Keyspace1')
        _insert_super(fixture_thrift_client, 'test')
        d = Deletion(1, predicate=SlicePredicate(column_names=['sc1']))
        cfmap = {'Super1': [Mutation(deletion=d)]}
        fixture_thrift_client.batch_mutate({'test': cfmap}, ConsistencyLevel.ONE)
        _expect_missing(lambda: fixture_thrift_client.get('key1', ColumnPath('Super1', 'sc1'), ConsistencyLevel.ONE))

    @pytest.mark.skip("Super columns not implemented")
    def test_super_reinsert(self, fixture_thrift_client):
        _set_keyspace(fixture_thrift_client, 'Keyspace1')
        for x in range(3):
            fixture_thrift_client.insert('key1', ColumnParent('Super1', 'sc2'), Column(_i64(x), 'value', 1),
                                         ConsistencyLevel.ONE)

        fixture_thrift_client.remove('key1', ColumnPath('Super1'), 2, ConsistencyLevel.ONE)

        for x in range(3):
            fixture_thrift_client.insert('key1', ColumnParent('Super1', 'sc2'), Column(_i64(x + 3), 'value', 3),
                                         ConsistencyLevel.ONE)

        for n in range(1, 4):
            p = SlicePredicate(slice_range=SliceRange('', '', False, n))
            slice = fixture_thrift_client.get_slice('key1', ColumnParent('Super1', 'sc2'), p, ConsistencyLevel.ONE)
            assert len(slice) == n, "expected %s results; found %s" % (n, slice)

    def test_describe_keyspace(self, fixture_thrift_client, fixture_cluster):
        kspaces = fixture_thrift_client.describe_keyspaces()
        ksnames = set([x.name for x in kspaces])
        # kspaces should have unique names
        assert len(kspaces) == len(ksnames), ksnames
        if fixture_cluster.dtest_config.is_enterprise:
            expected = ['Keyspace1', 'Keyspace2', 'system', 'system_auth',
                        'system_distributed', 'system_schema', 'system_traces']
            if parse_version(self.cluster.version()) >= parse_version('2022.1'):
                expected += ['audit', 'system_distributed_everywhere']
            if parse_version(self.cluster.version()) >= parse_version('2023.1'):
                expected += ['system_replicated_keys']
            assert sorted(ksnames) == sorted(expected)
        elif isinstance(self.cluster, ScyllaCluster) \
                and parse_version(self.cluster.version()) >= parse_version('4.6.dev'):
            expected = ['Keyspace2', 'Keyspace1', 'system', 'system_auth', 'system_distributed',
                        'system_distributed_everywhere', 'system_schema', 'system_traces']
            assert sorted(ksnames) == sorted(expected)
        elif parse_version(self.cluster.version()) >= parse_version('3.0'):
            # ['Keyspace2', 'Keyspace1', 'system', 'system_traces', 'system_schema', 'system_auth', 'system_distributed']
            assert len(kspaces) == 7, [x.name for x in kspaces]
        elif parse_version(self.cluster.version()) >= parse_version('2.2'):
            # Scylla does not have system_auth or system_distributed keyspaces.
            assert len(kspaces) == 5, [x.name for x in kspaces]  # ['Keyspace2', 'Keyspace1', 'system', 'system_traces']
        else:
            assert len(kspaces) == 4, [x.name for x in kspaces]  # ['Keyspace2', 'Keyspace1', 'system', 'system_traces']

        sysks = fixture_thrift_client.describe_keyspace("system")
        assert sysks in kspaces

        ks1 = fixture_thrift_client.describe_keyspace("Keyspace1")
        assert ks1.strategy_options['replication_factor'] == '1', ks1.strategy_options
        cf0 = None
        for cf in ks1.cf_defs:
            if cf.name == "Standard1":
                cf0 = cf
                break
        assert cf0.comparator_type == "org.apache.cassandra.db.marshal.BytesType"

    def test_describe(self, fixture_thrift_client):
        assert fixture_thrift_client.describe_cluster_name() == 'test'

    def test_describe_ring(self, fixture_thrift_client):
        assert list(fixture_thrift_client.describe_ring('Keyspace1'))[0].endpoints == [self.cluster.get_node_ip(1)]
        assert list(fixture_thrift_client.describe_local_ring('Keyspace1'))[0].endpoints == [
            self.cluster.get_node_ip(1)]

    def test_describe_token_map(self, fixture_thrift_client, fixture_dtest_setup):
        ring = list(fixture_thrift_client.describe_token_map().items())
        if fixture_dtest_setup.dtest_config.use_vnodes:
            if fixture_dtest_setup.dtest_config.num_tokens == -1:
                assert 256 == len(ring)
            else:
                assert fixture_dtest_setup.dtest_config.num_tokens == len(ring)
        else:
            assert len(ring) == 1
        token, node = ring[0]
        if fixture_dtest_setup.dtest_config.use_vnodes:
            t = int(token)
            assert t >= -9223372036854775808
            assert t <= 9223372036854775807
        assert node == self.cluster.get_node_ip(1)

    def test_describe_partitioner(self, fixture_thrift_client):
        # Make sure this just reads back the values from the config.
        assert fixture_thrift_client.describe_partitioner() == "org.apache.cassandra.dht.Murmur3Partitioner"

    def test_describe_snitch(self, fixture_thrift_client):
        assert fixture_thrift_client.describe_snitch() == "org.apache.cassandra.locator.SimpleSnitch"

    def test_invalid_ks_names(self, fixture_thrift_client):
        def invalid_keyspace():
            fixture_thrift_client.system_add_keyspace(
                KsDef('in-valid', 'org.apache.cassandra.locator.SimpleStrategy', {'replication_factor': '1'},
                      cf_defs=[]))

        _expect_exception(invalid_keyspace, InvalidRequestException)

    def test_invalid_strategy_class(self, fixture_thrift_client):
        def add_invalid_keyspace():
            fixture_thrift_client.system_add_keyspace(KsDef('ValidKs', 'InvalidStrategyClass', {}, cf_defs=[]))

        exc = _expect_exception(add_invalid_keyspace, InvalidRequestException)
        s = str(exc)
        assert s.find("InvalidStrategyClass") > -1, s
        assert s.find("unable to find class") > -1, s

        def update_invalid_keyspace():
            fixture_thrift_client.system_add_keyspace(
                KsDef('ValidKsForUpdate', 'org.apache.cassandra.locator.SimpleStrategy', {
                    'replication_factor': '1'}, cf_defs=[]))
            fixture_thrift_client.system_update_keyspace(
                KsDef('ValidKsForUpdate', 'InvalidStrategyClass', {}, cf_defs=[]))

        exc = _expect_exception(update_invalid_keyspace, InvalidRequestException)
        s = str(exc)
        assert s.find("InvalidStrategyClass") > -1, s
        assert s.find("unable to find class") > -1, s

    def test_invalid_cf_names(self, fixture_thrift_client):
        def invalid_cf():
            _set_keyspace(fixture_thrift_client, 'Keyspace1')
            newcf = CfDef('Keyspace1', 'in-valid')
            fixture_thrift_client.system_add_column_family(newcf)

        _expect_exception(invalid_cf, InvalidRequestException)

        def invalid_cf_inside_new_ks():
            cf = CfDef('ValidKsName_invalid_cf', 'in-valid')
            _set_keyspace(fixture_thrift_client, 'system')
            fixture_thrift_client.system_add_keyspace(
                KsDef('ValidKsName_invalid_cf', 'org.apache.cassandra.locator.SimpleStrategy', {
                    'replication_factor': '1'}, cf_defs=[cf]))

        _expect_exception(invalid_cf_inside_new_ks, InvalidRequestException)

    def test_system_cf_recreate(self, fixture_thrift_client):
        "ensures that keyspaces and column familes can be dropped and recreated in short order"
        for x in range(2):
            keyspace = 'test_cf_recreate'
            cf_name = 'recreate_cf'

            # create
            newcf = CfDef(keyspace, cf_name)
            newks = KsDef(keyspace, 'org.apache.cassandra.locator.SimpleStrategy',
                          {'replication_factor': '1'}, cf_defs=[newcf])
            fixture_thrift_client.system_add_keyspace(newks)
            _set_keyspace(fixture_thrift_client, keyspace)

            # insert
            fixture_thrift_client.insert('key0', ColumnParent(cf_name), Column('colA', 'colA-value', 0),
                                         ConsistencyLevel.ONE)
            col1 = fixture_thrift_client.get_slice('key0', ColumnParent(cf_name), SlicePredicate(
                slice_range=SliceRange('', '', False, 100)), ConsistencyLevel.ONE)[0].column
            assert col1.name == b'colA' and col1.value == b'colA-value'

            # drop
            fixture_thrift_client.system_drop_column_family(cf_name)

            # recreate
            fixture_thrift_client.system_add_column_family(newcf)

            # query
            cosc_list = fixture_thrift_client.get_slice('key0', ColumnParent(cf_name), SlicePredicate(
                slice_range=SliceRange('', '', False, 100)), ConsistencyLevel.ONE)
            # this was failing prior to CASSANDRA-1477.
            assert len(cosc_list) == 0, 'cosc length test failed'

            fixture_thrift_client.system_drop_keyspace(keyspace)

    def test_system_keyspace_operations(self, fixture_thrift_client):
        # create.  note large RF, this is OK
        keyspace = KsDef('CreateKeyspace',
                         'org.apache.cassandra.locator.SimpleStrategy',
                         {'replication_factor': '10'},
                         cf_defs=[CfDef('CreateKeyspace', 'CreateKsCf')])
        fixture_thrift_client.system_add_keyspace(keyspace)
        newks = fixture_thrift_client.describe_keyspace('CreateKeyspace')
        assert 'CreateKsCf' in [x.name for x in newks.cf_defs]

        _set_keyspace(fixture_thrift_client, 'CreateKeyspace')

        # modify valid
        modified_keyspace = KsDef('CreateKeyspace',
                                  'org.apache.cassandra.locator.NetworkTopologyStrategy',
                                  {},
                                  cf_defs=[])
        fixture_thrift_client.system_update_keyspace(modified_keyspace)
        modks = fixture_thrift_client.describe_keyspace('CreateKeyspace')
        assert modks.strategy_class == modified_keyspace.strategy_class
        assert modks.strategy_options == modified_keyspace.strategy_options

        # check strategy options are validated on modify
        def modify_invalid_ks():
            fixture_thrift_client.system_update_keyspace(KsDef('CreateKeyspace',
                                                               'org.apache.cassandra.locator.SimpleStrategy',
                                                               {},
                                                               cf_defs=[]))

        _expect_exception(modify_invalid_ks, InvalidRequestException)

        # drop
        fixture_thrift_client.system_drop_keyspace('CreateKeyspace')

        def get_second_ks():
            fixture_thrift_client.describe_keyspace('CreateKeyspace')

        _expect_exception(get_second_ks, NotFoundException)

        # check strategy options are validated on creation
        def create_invalid_ks():
            fixture_thrift_client.system_add_keyspace(KsDef('InvalidKeyspace',
                                                            'org.apache.cassandra.locator.SimpleStrategy',
                                                            {},
                                                            cf_defs=[]))

        _expect_exception(create_invalid_ks, InvalidRequestException)

    def test_create_then_drop_ks(self, fixture_thrift_client):
        keyspace = KsDef('AddThenDrop',
                         strategy_class='org.apache.cassandra.locator.SimpleStrategy',
                         strategy_options={'replication_factor': '1'},
                         cf_defs=[])

        def test_existence():
            fixture_thrift_client.describe_keyspace(keyspace.name)

        _expect_exception(test_existence, NotFoundException)
        fixture_thrift_client.set_keyspace('system')
        fixture_thrift_client.system_add_keyspace(keyspace)
        test_existence()
        fixture_thrift_client.system_drop_keyspace(keyspace.name)

    def test_column_validators(self, fixture_thrift_client):
        # columndef validation for regular CF
        ks = 'Keyspace1'
        _set_keyspace(fixture_thrift_client, ks)
        cd = ColumnDef('col', 'LongType', None, None)
        cf = CfDef('Keyspace1', 'ValidatorColumnFamily', column_metadata=[cd])
        fixture_thrift_client.system_add_column_family(cf)
        ks_def = fixture_thrift_client.describe_keyspace(ks)
        assert 'ValidatorColumnFamily' in [x.name for x in ks_def.cf_defs]

        cp = ColumnParent('ValidatorColumnFamily')
        col0 = Column('col', _i64(42), 0)
        col1 = Column('col', "ceci n'est pas 64bit", 0)
        fixture_thrift_client.insert('key0', cp, col0, ConsistencyLevel.ONE)
        e = _expect_exception(lambda: fixture_thrift_client.insert('key1', cp, col1, ConsistencyLevel.ONE),
                              InvalidRequestException)
        # assert e.why.find("failed validation") >= 0

        # columndef validation for super CF
        # scf = CfDef('Keyspace1', 'ValidatorSuperColumnFamily', column_type='Super', column_metadata=[cd])
        # fixture_thrift_client.system_add_column_family(scf)
        # ks_def = fixture_thrift_client.describe_keyspace(ks)
        # assert 'ValidatorSuperColumnFamily' in [x.name for x in ks_def.cf_defs]

        # scp = ColumnParent('ValidatorSuperColumnFamily', 'sc1')
        # client.insert('key0', scp, col0, ConsistencyLevel.ONE)
        # e = _expect_exception(lambda: fixture_thrift_client.insert('key1', scp, col1, ConsistencyLevel.ONE), InvalidRequestException)
        # assert e.why.find("failed validation") >= 0

        # columndef and cfdef default validation
        cf = CfDef('Keyspace1', 'DefaultValidatorColumnFamily',
                   column_metadata=[cd], default_validation_class='UTF8Type')
        fixture_thrift_client.system_add_column_family(cf)
        ks_def = fixture_thrift_client.describe_keyspace(ks)
        assert 'DefaultValidatorColumnFamily' in [x.name for x in ks_def.cf_defs]

        dcp = ColumnParent('DefaultValidatorColumnFamily')
        # inserting a longtype into column 'col' is valid at the columndef level
        fixture_thrift_client.insert('key0', dcp, col0, ConsistencyLevel.ONE)
        # inserting a UTF8type into column 'col' fails at the columndef level
        e = _expect_exception(lambda: fixture_thrift_client.insert('key1', dcp, col1, ConsistencyLevel.ONE),
                              InvalidRequestException)
        # assert e.why.find("failed validation") >= 0

        # FIXME: Mixed column-families are not supported
        # insert a longtype into column 'fcol' should fail at the cfdef level
        # col2 = Column('fcol', _i64(4224), 0)
        # e = _expect_exception(lambda: fixture_thrift_client.insert('key1', dcp, col2, ConsistencyLevel.ONE), InvalidRequestException)
        # assert e.why.find("failed validation") >= 0
        # insert a UTF8type into column 'fcol' is valid at the cfdef level
        # !!Disabled because of unsupported mixed CFs.
        # col3 = Column('fcol', "Stringin' it up in the Stringtel Stringifornia", 0)
        # client.insert('key0', dcp, col3, ConsistencyLevel.ONE)

    def test_system_column_family_operations(self, fixture_thrift_client):
        _set_keyspace(fixture_thrift_client, 'Keyspace1')
        # create
        cd = ColumnDef('ValidationColumn', 'BytesType', None, None)
        newcf = CfDef('Keyspace1', 'NewColumnFamily', column_metadata=[cd])
        fixture_thrift_client.system_add_column_family(newcf)
        time.sleep(5)
        ks1 = fixture_thrift_client.describe_keyspace('Keyspace1')
        assert 'NewColumnFamily' in [x.name for x in ks1.cf_defs]
        cfid = [x.id for x in ks1.cf_defs if x.name == 'NewColumnFamily'][0]

        # modify invalid
        modified_cf = CfDef('Keyspace1', 'NewColumnFamily', column_metadata=[cd])
        modified_cf.id = cfid

        def fail_invalid_field():
            modified_cf.comparator_type = 'LongType'
            fixture_thrift_client.system_update_column_family(modified_cf)

        _expect_exception(fail_invalid_field, InvalidRequestException)

        # modify valid
        modified_cf.comparator_type = 'BytesType'  # revert back to old value.
        modified_cf.gc_grace_seconds = 1
        fixture_thrift_client.system_update_column_family(modified_cf)
        ks1 = fixture_thrift_client.describe_keyspace('Keyspace1')
        server_cf = [x for x in ks1.cf_defs if x.name == 'NewColumnFamily'][0]
        assert server_cf
        assert server_cf.gc_grace_seconds == 1

        # drop
        fixture_thrift_client.system_drop_column_family('NewColumnFamily')
        ks1 = fixture_thrift_client.describe_keyspace('Keyspace1')
        assert 'NewColumnFamily' not in [x.name for x in ks1.cf_defs]
        assert 'Standard1' in [x.name for x in ks1.cf_defs]

        # FIXME: Mixed column-families are not supported
        # Make a LongType CF and add a validator
        # newcf = CfDef('Keyspace1', 'NewLongColumnFamily', comparator_type='LongType')
        # fixture_thrift_client.system_add_column_family(newcf)

        # three = _i64(3)
        # cd = ColumnDef(three, 'LongType', None, None)
        # ks1 = fixture_thrift_client.describe_keyspace('Keyspace1')
        # modified_cf = [x for x in ks1.cf_defs if x.name == 'NewLongColumnFamily'][0]
        # modified_cf.column_metadata = [cd]
        # fixture_thrift_client.system_update_column_family(modified_cf)

        # ks1 = fixture_thrift_client.describe_keyspace('Keyspace1')
        # server_cf = [x for x in ks1.cf_defs if x.name == 'NewLongColumnFamily'][0]
        # assert server_cf.column_metadata[0].name == _i64(3), server_cf.column_metadata

    @pytest.mark.skip("Indexes not implemented")
    def test_dynamic_indexes_creation_deletion(self, fixture_thrift_client):
        _set_keyspace(fixture_thrift_client, 'Keyspace1')
        cfdef = CfDef('Keyspace1', 'BlankCF')
        fixture_thrift_client.system_add_column_family(cfdef)

        ks1 = fixture_thrift_client.describe_keyspace('Keyspace1')
        cfid = [x.id for x in ks1.cf_defs if x.name == 'BlankCF'][0]
        modified_cd = ColumnDef('birthdate', 'BytesType', IndexType.KEYS, None)
        modified_cf = CfDef('Keyspace1', 'BlankCF', column_metadata=[modified_cd])
        modified_cf.id = cfid
        fixture_thrift_client.system_update_column_family(modified_cf)

        # Add a second indexed CF ...
        birthdate_coldef = ColumnDef('birthdate', 'BytesType', IndexType.KEYS, None)
        age_coldef = ColumnDef('age', 'BytesType', IndexType.KEYS, 'age_index')
        cfdef = CfDef('Keyspace1', 'BlankCF2', column_metadata=[birthdate_coldef, age_coldef])
        fixture_thrift_client.system_add_column_family(cfdef)

        # ... and update it to have a third index
        ks1 = fixture_thrift_client.describe_keyspace('Keyspace1')
        cfdef = [x for x in ks1.cf_defs if x.name == 'BlankCF2'][0]
        name_coldef = ColumnDef('name', 'BytesType', IndexType.KEYS, 'name_index')
        cfdef.column_metadata.append(name_coldef)
        fixture_thrift_client.system_update_column_family(cfdef)

        # Now drop the indexes
        ks1 = fixture_thrift_client.describe_keyspace('Keyspace1')
        cfdef = [x for x in ks1.cf_defs if x.name == 'BlankCF2'][0]
        birthdate_coldef = ColumnDef('birthdate', 'BytesType', None, None)
        age_coldef = ColumnDef('age', 'BytesType', None, None)
        name_coldef = ColumnDef('name', 'BytesType', None, None)
        cfdef.column_metadata = [birthdate_coldef, age_coldef, name_coldef]
        fixture_thrift_client.system_update_column_family(cfdef)

        ks1 = fixture_thrift_client.describe_keyspace('Keyspace1')
        cfdef = [x for x in ks1.cf_defs if x.name == 'BlankCF'][0]
        birthdate_coldef = ColumnDef('birthdate', 'BytesType', None, None)
        cfdef.column_metadata = [birthdate_coldef]
        fixture_thrift_client.system_update_column_family(cfdef)

        fixture_thrift_client.system_drop_column_family('BlankCF')
        fixture_thrift_client.system_drop_column_family('BlankCF2')

    @pytest.mark.skip("Indexes not implemented")
    def test_dynamic_indexes_with_system_update_cf(self, fixture_thrift_client):
        _set_keyspace(fixture_thrift_client, 'Keyspace1')
        cd = ColumnDef('birthdate', 'BytesType', None, None)
        newcf = CfDef('Keyspace1', 'ToBeIndexed', default_validation_class='LongType', column_metadata=[cd])
        fixture_thrift_client.system_add_column_family(newcf)

        fixture_thrift_client.insert('key1', ColumnParent('ToBeIndexed'), Column('birthdate', _i64(1), 0),
                                     ConsistencyLevel.ONE)
        fixture_thrift_client.insert('key2', ColumnParent('ToBeIndexed'), Column('birthdate', _i64(2), 0),
                                     ConsistencyLevel.ONE)
        fixture_thrift_client.insert('key2', ColumnParent('ToBeIndexed'), Column('b', _i64(2), 0), ConsistencyLevel.ONE)
        fixture_thrift_client.insert('key3', ColumnParent('ToBeIndexed'), Column('birthdate', _i64(3), 0),
                                     ConsistencyLevel.ONE)
        fixture_thrift_client.insert('key3', ColumnParent('ToBeIndexed'), Column('b', _i64(3), 0), ConsistencyLevel.ONE)

        # First without index
        cp = ColumnParent('ToBeIndexed')
        sp = SlicePredicate(slice_range=SliceRange('', ''))
        key_range = KeyRange('', '', None, None, [IndexExpression('birthdate', IndexOperator.EQ, _i64(1))], 100)
        result = fixture_thrift_client.get_range_slices(cp, sp, key_range, ConsistencyLevel.ONE)
        assert len(result) == 1, result
        assert result[0].key == 'key1'
        assert len(result[0].columns) == 1, result[0].columns

        # add an index on 'birthdate'
        ks1 = fixture_thrift_client.describe_keyspace('Keyspace1')
        cfid = [x.id for x in ks1.cf_defs if x.name == 'ToBeIndexed'][0]
        modified_cd = ColumnDef('birthdate', 'BytesType', IndexType.KEYS, 'bd_index')
        modified_cf = CfDef('Keyspace1', 'ToBeIndexed', column_metadata=[modified_cd])
        modified_cf.id = cfid
        fixture_thrift_client.system_update_column_family(modified_cf)

        ks1 = fixture_thrift_client.describe_keyspace('Keyspace1')
        server_cf = [x for x in ks1.cf_defs if x.name == 'ToBeIndexed'][0]
        assert server_cf
        assert server_cf.column_metadata[0].index_type == modified_cd.index_type
        assert server_cf.column_metadata[0].index_name == modified_cd.index_name

        # sleep a bit to give time for the index to build.
        time.sleep(0.5)

        # repeat query on one index expression
        result = fixture_thrift_client.get_range_slices(cp, sp, key_range, ConsistencyLevel.ONE)
        assert len(result) == 1, result
        assert result[0].key == 'key1'
        assert len(result[0].columns) == 1, result[0].columns

    @pytest.mark.skip("Super columns not implemented")
    def test_system_super_column_family_operations(self, fixture_thrift_client):
        _set_keyspace(fixture_thrift_client, 'Keyspace1')

        # create
        cd = ColumnDef('ValidationColumn', 'BytesType', None, None)
        newcf = CfDef('Keyspace1', 'NewSuperColumnFamily', 'Super', column_metadata=[cd])
        fixture_thrift_client.system_add_column_family(newcf)
        ks1 = fixture_thrift_client.describe_keyspace('Keyspace1')
        assert 'NewSuperColumnFamily' in [x.name for x in ks1.cf_defs]

        # drop
        fixture_thrift_client.system_drop_column_family('NewSuperColumnFamily')
        ks1 = fixture_thrift_client.describe_keyspace('Keyspace1')
        assert 'NewSuperColumnFamily' not in [x.name for x in ks1.cf_defs]
        assert 'Standard1' in [x.name for x in ks1.cf_defs]

    def test_insert_ttl(self, fixture_thrift_client):
        """ Test simple insertion of a column with ttl """
        _set_keyspace(fixture_thrift_client, 'Keyspace1')
        column = Column('cttl1', 'value1', 0, 5)
        fixture_thrift_client.insert('key1', ColumnParent('Standard1'), column, ConsistencyLevel.ONE)
        assert fixture_thrift_client.get('key1', ColumnPath('Standard1', column='cttl1'),
                                         ConsistencyLevel.ONE).column == column

    def test_simple_expiration(self, fixture_thrift_client):
        """ Test that column ttled do expires """
        _set_keyspace(fixture_thrift_client, 'Keyspace1')
        column = Column('cttl3', 'value1', 0, 2)
        fixture_thrift_client.insert('key1', ColumnParent('Standard1'), column, ConsistencyLevel.ONE)
        c = fixture_thrift_client.get('key1', ColumnPath('Standard1', column='cttl3'), ConsistencyLevel.ONE).column
        assert c == column
        time.sleep(3)
        _expect_missing(
            lambda: fixture_thrift_client.get('key1', ColumnPath('Standard1', column='cttl3'), ConsistencyLevel.ONE))

    def test_simple_expiration_batch_mutate(self, fixture_thrift_client):
        """ Test that column ttled do expires using batch_mutate """
        _set_keyspace(fixture_thrift_client, 'Keyspace1')
        column = Column('cttl4', 'value1', 0, 2)
        cfmap = {'Standard1': [Mutation(ColumnOrSuperColumn(column))]}
        fixture_thrift_client.batch_mutate({'key1': cfmap}, ConsistencyLevel.ONE)
        c = fixture_thrift_client.get('key1', ColumnPath('Standard1', column='cttl4'), ConsistencyLevel.ONE).column
        assert c == column
        time.sleep(3)
        _expect_missing(
            lambda: fixture_thrift_client.get('key1', ColumnPath('Standard1', column='cttl4'), ConsistencyLevel.ONE))

    def test_update_expiring(self, fixture_thrift_client):
        """ Test that updating a column with ttl override the ttl """
        _set_keyspace(fixture_thrift_client, 'Keyspace1')
        column1 = Column('cttl4', 'value1', 0, 1)
        fixture_thrift_client.insert('key1', ColumnParent('Standard1'), column1, ConsistencyLevel.ONE)
        column2 = Column('cttl4', 'value1', 1)
        fixture_thrift_client.insert('key1', ColumnParent('Standard1'), column2, ConsistencyLevel.ONE)
        time.sleep(1.5)
        assert fixture_thrift_client.get('key1', ColumnPath('Standard1', column='cttl4'),
                                         ConsistencyLevel.ONE).column == column2

    def test_remove_expiring(self, fixture_thrift_client):
        """ Test removing a column with ttl """
        _set_keyspace(fixture_thrift_client, 'Keyspace1')
        column = Column('cttl5', 'value1', 0, 10)
        fixture_thrift_client.insert('key1', ColumnParent('Standard1'), column, ConsistencyLevel.ONE)
        fixture_thrift_client.remove('key1', ColumnPath('Standard1', column='cttl5'), 1, ConsistencyLevel.ONE)
        _expect_missing(
            lambda: fixture_thrift_client.get('key1', ColumnPath('Standard1', column='ctt5'), ConsistencyLevel.ONE))

    def test_describe_ring_on_invalid_keyspace(self, fixture_thrift_client):
        def req():
            fixture_thrift_client.describe_ring('system')

        _expect_exception(req, InvalidRequestException)

    def test_incr_decr_standard_add(self, fixture_thrift_client):
        _set_keyspace(fixture_thrift_client, 'Keyspace1')

        d1 = 12
        d2 = -21
        d3 = 35
        # insert positive and negative values and check the counts
        fixture_thrift_client.add('key1', ColumnParent(column_family='Counter1'), CounterColumn('c1', d1),
                                  ConsistencyLevel.ONE)
        time.sleep(0.1)
        rv1 = fixture_thrift_client.get('key1', ColumnPath(column_family='Counter1', column='c1'), ConsistencyLevel.ONE)
        assert rv1.counter_column.value == d1

        fixture_thrift_client.add('key1', ColumnParent(column_family='Counter1'), CounterColumn('c1', d2),
                                  ConsistencyLevel.ONE)
        time.sleep(0.1)
        rv2 = fixture_thrift_client.get('key1', ColumnPath(column_family='Counter1', column='c1'), ConsistencyLevel.ONE)
        assert rv2.counter_column.value == (d1 + d2)

        fixture_thrift_client.add('key1', ColumnParent(column_family='Counter1'), CounterColumn('c1', d3),
                                  ConsistencyLevel.ONE)
        time.sleep(0.1)
        rv3 = fixture_thrift_client.get('key1', ColumnPath(column_family='Counter1', column='c1'), ConsistencyLevel.ONE)
        assert rv3.counter_column.value == (d1 + d2 + d3)

    @pytest.mark.skip("Super columns not implemented")
    def test_incr_decr_super_add(self, fixture_thrift_client):
        _set_keyspace(fixture_thrift_client, 'Keyspace1')

        d1 = -234
        d2 = 52345
        d3 = 3123

        fixture_thrift_client.add('key1', ColumnParent(column_family='SuperCounter1', super_column='sc1'),
                                  CounterColumn('c1', d1), ConsistencyLevel.ONE)
        fixture_thrift_client.add('key1', ColumnParent(column_family='SuperCounter1', super_column='sc1'),
                                  CounterColumn('c2', d2), ConsistencyLevel.ONE)
        time.sleep(0.1)
        rv1 = fixture_thrift_client.get('key1', ColumnPath(column_family='SuperCounter1', super_column='sc1'),
                                        ConsistencyLevel.ONE)
        assert rv1.counter_super_column.columns[0].value == d1
        assert rv1.counter_super_column.columns[1].value == d2

        fixture_thrift_client.add('key1', ColumnParent(column_family='SuperCounter1', super_column='sc1'),
                                  CounterColumn('c1', d2), ConsistencyLevel.ONE)
        time.sleep(0.1)
        rv2 = fixture_thrift_client.get('key1', ColumnPath('SuperCounter1', 'sc1', 'c1'), ConsistencyLevel.ONE)
        assert rv2.counter_column.value == (d1 + d2)

        fixture_thrift_client.add('key1', ColumnParent(column_family='SuperCounter1', super_column='sc1'),
                                  CounterColumn('c1', d3), ConsistencyLevel.ONE)
        time.sleep(0.1)
        rv3 = fixture_thrift_client.get('key1', ColumnPath(column_family='SuperCounter1',
                                                           super_column='sc1', column='c1'), ConsistencyLevel.ONE)
        assert rv3.counter_column.value == (d1 + d2 + d3)

    def test_incr_standard_remove(self, fixture_thrift_client):
        _set_keyspace(fixture_thrift_client, 'Keyspace1')

        d1 = 124

        # insert value and check it exists
        fixture_thrift_client.add('key1', ColumnParent(column_family='Counter1'), CounterColumn('c1', d1),
                                  ConsistencyLevel.ONE)
        time.sleep(5)
        rv1 = fixture_thrift_client.get('key1', ColumnPath(column_family='Counter1', column='c1'), ConsistencyLevel.ONE)
        assert rv1.counter_column.value == d1

        # remove the previous column and check that it is gone
        fixture_thrift_client.remove_counter('key1', ColumnPath(column_family='Counter1', column='c1'),
                                             ConsistencyLevel.ONE)
        time.sleep(5)
        _assert_no_columnpath(fixture_thrift_client, 'key1', ColumnPath(column_family='Counter1', column='c1'))

        # insert again and this time delete the whole row, check that it is gone
        fixture_thrift_client.add('key2', ColumnParent(column_family='Counter1'), CounterColumn('c1', d1),
                                  ConsistencyLevel.ONE)
        time.sleep(5)
        rv2 = fixture_thrift_client.get('key2', ColumnPath(column_family='Counter1', column='c1'), ConsistencyLevel.ONE)
        assert rv2.counter_column.value == d1
        fixture_thrift_client.remove_counter('key2', ColumnPath(column_family='Counter1'), ConsistencyLevel.ONE)
        time.sleep(5)
        _assert_no_columnpath(fixture_thrift_client, 'key2', ColumnPath(column_family='Counter1', column='c1'))

    @pytest.mark.skip("Super columns not implemented")
    def test_incr_super_remove(self, fixture_thrift_client):
        _set_keyspace(fixture_thrift_client, 'Keyspace1')

        d1 = 52345

        # insert value and check it exists
        fixture_thrift_client.add('key1', ColumnParent(column_family='SuperCounter1', super_column='sc1'),
                                  CounterColumn('c1', d1), ConsistencyLevel.ONE)
        time.sleep(5)
        rv1 = fixture_thrift_client.get('key1', ColumnPath(column_family='SuperCounter1',
                                                           super_column='sc1', column='c1'), ConsistencyLevel.ONE)
        assert rv1.counter_column.value == d1

        # remove the previous column and check that it is gone
        fixture_thrift_client.remove_counter('key1', ColumnPath(column_family='SuperCounter1',
                                                                super_column='sc1', column='c1'), ConsistencyLevel.ONE)
        time.sleep(5)
        _assert_no_columnpath(fixture_thrift_client, 'key1',
                              ColumnPath(column_family='SuperCounter1', super_column='sc1', column='c1'))

        # insert again and this time delete the whole row, check that it is gone
        fixture_thrift_client.add('key2', ColumnParent(column_family='SuperCounter1', super_column='sc1'),
                                  CounterColumn('c1', d1), ConsistencyLevel.ONE)
        time.sleep(5)
        rv2 = fixture_thrift_client.get('key2', ColumnPath(column_family='SuperCounter1',
                                                           super_column='sc1', column='c1'), ConsistencyLevel.ONE)
        assert rv2.counter_column.value == d1
        fixture_thrift_client.remove_counter('key2', ColumnPath(column_family='SuperCounter1',
                                                                super_column='sc1'), ConsistencyLevel.ONE)
        time.sleep(5)
        _assert_no_columnpath(fixture_thrift_client, 'key2',
                              ColumnPath(column_family='SuperCounter1', super_column='sc1', column='c1'))

    def test_incr_decr_standard_remove(self, fixture_thrift_client):
        _set_keyspace(fixture_thrift_client, 'Keyspace1')

        d1 = 124

        # insert value and check it exists
        fixture_thrift_client.add('key1', ColumnParent(column_family='Counter1'), CounterColumn('c1', d1),
                                  ConsistencyLevel.ONE)
        time.sleep(5)
        rv1 = fixture_thrift_client.get('key1', ColumnPath(column_family='Counter1', column='c1'), ConsistencyLevel.ONE)
        assert rv1.counter_column.value == d1

        # remove the previous column and check that it is gone
        fixture_thrift_client.remove_counter('key1', ColumnPath(column_family='Counter1', column='c1'),
                                             ConsistencyLevel.ONE)
        time.sleep(5)
        _assert_no_columnpath(fixture_thrift_client, 'key1', ColumnPath(column_family='Counter1', column='c1'))

        # insert again and this time delete the whole row, check that it is gone
        fixture_thrift_client.add('key2', ColumnParent(column_family='Counter1'), CounterColumn('c1', d1),
                                  ConsistencyLevel.ONE)
        time.sleep(5)
        rv2 = fixture_thrift_client.get('key2', ColumnPath(column_family='Counter1', column='c1'), ConsistencyLevel.ONE)
        assert rv2.counter_column.value == d1
        fixture_thrift_client.remove_counter('key2', ColumnPath(column_family='Counter1'), ConsistencyLevel.ONE)
        time.sleep(5)
        _assert_no_columnpath(fixture_thrift_client, 'key2', ColumnPath(column_family='Counter1', column='c1'))

    @pytest.mark.skip("Super columns not implemented")
    def test_incr_decr_super_remove(self, fixture_thrift_client):
        _set_keyspace(fixture_thrift_client, 'Keyspace1')

        d1 = 52345

        # insert value and check it exists
        fixture_thrift_client.add('key1', ColumnParent(column_family='SuperCounter1', super_column='sc1'),
                                  CounterColumn('c1', d1), ConsistencyLevel.ONE)
        time.sleep(5)
        rv1 = fixture_thrift_client.get('key1', ColumnPath(column_family='SuperCounter1',
                                                           super_column='sc1', column='c1'), ConsistencyLevel.ONE)
        assert rv1.counter_column.value == d1

        # remove the previous column and check that it is gone
        fixture_thrift_client.remove_counter('key1', ColumnPath(column_family='SuperCounter1',
                                                                super_column='sc1', column='c1'), ConsistencyLevel.ONE)
        time.sleep(5)
        _assert_no_columnpath(fixture_thrift_client, 'key1',
                              ColumnPath(column_family='SuperCounter1', super_column='sc1', column='c1'))

        # insert again and this time delete the whole row, check that it is gone
        fixture_thrift_client.add('key2', ColumnParent(column_family='SuperCounter1', super_column='sc1'),
                                  CounterColumn('c1', d1), ConsistencyLevel.ONE)
        time.sleep(5)
        rv2 = fixture_thrift_client.get('key2', ColumnPath(column_family='SuperCounter1',
                                                           super_column='sc1', column='c1'), ConsistencyLevel.ONE)
        assert rv2.counter_column.value == d1
        fixture_thrift_client.remove_counter('key2', ColumnPath(column_family='SuperCounter1',
                                                                super_column='sc1'), ConsistencyLevel.ONE)
        time.sleep(5)
        _assert_no_columnpath(fixture_thrift_client, 'key2',
                              ColumnPath(column_family='SuperCounter1', super_column='sc1', column='c1'))

    def test_incr_decr_standard_batch_add(self, fixture_thrift_client):
        _set_keyspace(fixture_thrift_client, 'Keyspace1')

        d1 = 12
        d2 = -21
        update_map = {'key1': {'Counter1': [
            Mutation(column_or_supercolumn=ColumnOrSuperColumn(counter_column=CounterColumn('c1', d1))),
            Mutation(column_or_supercolumn=ColumnOrSuperColumn(counter_column=CounterColumn('c1', d2))),
        ]}}

        # insert positive and negative values and check the counts
        fixture_thrift_client.batch_mutate(update_map, ConsistencyLevel.ONE)
        time.sleep(0.1)
        rv1 = fixture_thrift_client.get('key1', ColumnPath(column_family='Counter1', column='c1'), ConsistencyLevel.ONE)
        assert rv1.counter_column.value == d1 + d2

    def test_incr_decr_standard_batch_remove(self, fixture_thrift_client):
        _set_keyspace(fixture_thrift_client, 'Keyspace1')

        d1 = 12
        d2 = -21

        # insert positive and negative values and check the counts
        update_map = {'key1': {'Counter1': [
            Mutation(column_or_supercolumn=ColumnOrSuperColumn(counter_column=CounterColumn('c1', d1))),
            Mutation(column_or_supercolumn=ColumnOrSuperColumn(counter_column=CounterColumn('c1', d2))),
        ]}}
        fixture_thrift_client.batch_mutate(update_map, ConsistencyLevel.ONE)
        time.sleep(5)
        rv1 = fixture_thrift_client.get('key1', ColumnPath(column_family='Counter1', column='c1'), ConsistencyLevel.ONE)
        assert rv1.counter_column.value == d1 + d2

        # remove the previous column and check that it is gone
        update_map = {'key1': {'Counter1': [
            Mutation(deletion=Deletion(predicate=SlicePredicate(column_names=['c1']))),
        ]}}
        fixture_thrift_client.batch_mutate(update_map, ConsistencyLevel.ONE)
        time.sleep(5)
        _assert_no_columnpath(fixture_thrift_client, 'key1', ColumnPath(column_family='Counter1', column='c1'))

        # insert again and this time delete the whole row, check that it is gone
        update_map = {'key2': {'Counter1': [
            Mutation(column_or_supercolumn=ColumnOrSuperColumn(counter_column=CounterColumn('c1', d1))),
            Mutation(column_or_supercolumn=ColumnOrSuperColumn(counter_column=CounterColumn('c1', d2))),
        ]}}
        fixture_thrift_client.batch_mutate(update_map, ConsistencyLevel.ONE)
        time.sleep(5)
        rv2 = fixture_thrift_client.get('key2', ColumnPath(column_family='Counter1', column='c1'), ConsistencyLevel.ONE)
        assert rv2.counter_column.value == d1 + d2

        update_map = {'key2': {'Counter1': [
            Mutation(deletion=Deletion()),
        ]}}
        fixture_thrift_client.batch_mutate(update_map, ConsistencyLevel.ONE)
        time.sleep(5)
        _assert_no_columnpath(fixture_thrift_client, 'key2', ColumnPath(column_family='Counter1', column='c1'))

    # known failure: see CASSANDRA-10046
    def test_range_deletion(self, fixture_thrift_client):
        """ Tests CASSANDRA-7990 """
        _set_keyspace(fixture_thrift_client, 'Keyspace1')

        def composite(item1, item2=None, eoc=b'\x00'):
            packed = _i16(len(item1)) + item1.encode('utf-8') + eoc
            if item2 is not None:
                packed += _i16(len(item2)) + item2.encode('utf-8')
                packed += eoc
            return packed

        for i in range(10):
            column_name = composite(str(i), str(i))
            column = Column(column_name, 'value', int(time.time() * 1000))
            fixture_thrift_client.insert('key1', ColumnParent('StandardComposite'), column, ConsistencyLevel.ONE)

        delete_slice = SlicePredicate(slice_range=SliceRange(
            composite('3', eoc=b'\xff'), composite('6', '\x01'), False, 100))
        mutations = [Mutation(deletion=Deletion(int(time.time() * 1000), predicate=delete_slice))]
        keyed_mutations = {'key1': {'StandardComposite': mutations}}
        fixture_thrift_client.batch_mutate(keyed_mutations, ConsistencyLevel.ONE)

        slice_predicate = SlicePredicate(slice_range=SliceRange('', '', False, 100))
        results = fixture_thrift_client.get_slice('key1', ColumnParent('StandardComposite'), slice_predicate,
                                                  ConsistencyLevel.ONE)
        columns = [result.column.name for result in results]
        assert [composite('0', '0'), composite('1', '1'), composite('2', '2'),
                composite('6', '6'), composite('7', '7'), composite('8', '8'), composite('9', '9')] == columns

    def test_incr_decr_standard_slice(self, fixture_thrift_client):
        _set_keyspace(fixture_thrift_client, 'Keyspace1')

        d1 = 12
        d2 = -21
        fixture_thrift_client.add('key1', ColumnParent(column_family='Counter1'), CounterColumn('c1', d1),
                                  ConsistencyLevel.ONE)
        fixture_thrift_client.add('key1', ColumnParent(column_family='Counter1'), CounterColumn('c2', d1),
                                  ConsistencyLevel.ONE)
        fixture_thrift_client.add('key1', ColumnParent(column_family='Counter1'), CounterColumn('c3', d1),
                                  ConsistencyLevel.ONE)
        fixture_thrift_client.add('key1', ColumnParent(column_family='Counter1'), CounterColumn('c3', d2),
                                  ConsistencyLevel.ONE)
        fixture_thrift_client.add('key1', ColumnParent(column_family='Counter1'), CounterColumn('c4', d1),
                                  ConsistencyLevel.ONE)
        fixture_thrift_client.add('key1', ColumnParent(column_family='Counter1'), CounterColumn('c5', d1),
                                  ConsistencyLevel.ONE)

        time.sleep(0.1)
        # insert positive and negative values and check the counts
        counters = fixture_thrift_client.get_slice('key1', ColumnParent('Counter1'),
                                                   SlicePredicate(['c3', 'c4']), ConsistencyLevel.ONE)

        assert counters[0].counter_column.value == d1 + d2
        assert counters[1].counter_column.value == d1

    def test_incr_decr_standard_muliget_slice(self, fixture_thrift_client):
        _set_keyspace(fixture_thrift_client, 'Keyspace1')

        d1 = 12
        d2 = -21
        fixture_thrift_client.add('key1', ColumnParent(column_family='Counter1'), CounterColumn('c2', d1),
                                  ConsistencyLevel.ONE)
        fixture_thrift_client.add('key1', ColumnParent(column_family='Counter1'), CounterColumn('c3', d1),
                                  ConsistencyLevel.ONE)
        fixture_thrift_client.add('key1', ColumnParent(column_family='Counter1'), CounterColumn('c3', d2),
                                  ConsistencyLevel.ONE)
        fixture_thrift_client.add('key1', ColumnParent(column_family='Counter1'), CounterColumn('c4', d1),
                                  ConsistencyLevel.ONE)
        fixture_thrift_client.add('key1', ColumnParent(column_family='Counter1'), CounterColumn('c5', d1),
                                  ConsistencyLevel.ONE)

        fixture_thrift_client.add('key2', ColumnParent(column_family='Counter1'), CounterColumn('c2', d1),
                                  ConsistencyLevel.ONE)
        fixture_thrift_client.add('key2', ColumnParent(column_family='Counter1'), CounterColumn('c3', d1),
                                  ConsistencyLevel.ONE)
        fixture_thrift_client.add('key2', ColumnParent(column_family='Counter1'), CounterColumn('c3', d2),
                                  ConsistencyLevel.ONE)
        fixture_thrift_client.add('key2', ColumnParent(column_family='Counter1'), CounterColumn('c4', d1),
                                  ConsistencyLevel.ONE)
        fixture_thrift_client.add('key2', ColumnParent(column_family='Counter1'), CounterColumn('c5', d1),
                                  ConsistencyLevel.ONE)

        time.sleep(0.1)
        # insert positive and negative values and check the counts
        counters = fixture_thrift_client.multiget_slice(['key1', 'key2'], ColumnParent(
            'Counter1'), SlicePredicate(['c3', 'c4']), ConsistencyLevel.ONE)

        assert counters[b'key1'][0].counter_column.value == d1 + d2
        assert counters[b'key1'][1].counter_column.value == d1
        assert counters[b'key2'][0].counter_column.value == d1 + d2
        assert counters[b'key2'][1].counter_column.value == d1

    def test_counter_get_slice_range(self, fixture_thrift_client):
        _set_keyspace(fixture_thrift_client, 'Keyspace1')
        _insert_counter_range(fixture_thrift_client)
        _verify_counter_range(fixture_thrift_client)

    @pytest.mark.skip("Super columns not implemented")
    def test_counter_get_slice_super_range(self, fixture_thrift_client):
        _set_keyspace(fixture_thrift_client, 'Keyspace1')
        _insert_counter_super_range(fixture_thrift_client)
        _verify_counter_super_range(fixture_thrift_client)

    @pytest.mark.skip("Secondary indexes not implemented")
    def test_index_scan(self, fixture_thrift_client):
        _set_keyspace(fixture_thrift_client, 'Keyspace1')
        fixture_thrift_client.insert('key1', ColumnParent('Indexed1'), Column('birthdate', _i64(1), 0),
                                     ConsistencyLevel.ONE)
        fixture_thrift_client.insert('key2', ColumnParent('Indexed1'), Column('birthdate', _i64(2), 0),
                                     ConsistencyLevel.ONE)
        fixture_thrift_client.insert('key2', ColumnParent('Indexed1'), Column('b', _i64(2), 0), ConsistencyLevel.ONE)
        fixture_thrift_client.insert('key3', ColumnParent('Indexed1'), Column('birthdate', _i64(3), 0),
                                     ConsistencyLevel.ONE)
        fixture_thrift_client.insert('key3', ColumnParent('Indexed1'), Column('b', _i64(3), 0), ConsistencyLevel.ONE)

        # simple query on one index expression
        cp = ColumnParent('Indexed1')
        sp = SlicePredicate(slice_range=SliceRange('', ''))
        key_range = KeyRange('', '', None, None, [IndexExpression('birthdate', IndexOperator.EQ, _i64(1))], 100)
        result = fixture_thrift_client.get_range_slices(cp, sp, key_range, ConsistencyLevel.ONE)
        assert len(result) == 1, result
        assert result[0].key == 'key1'
        assert len(result[0].columns) == 1, result[0].columns

        # without index
        key_range = KeyRange('', '', None, None, [IndexExpression('b', IndexOperator.EQ, _i64(1))], 100)
        result = fixture_thrift_client.get_range_slices(cp, sp, key_range, ConsistencyLevel.ONE)
        assert len(result) == 0, result

        # but unindexed expression added to indexed one is ok
        key_range = KeyRange('', '', None, None, [IndexExpression('b', IndexOperator.EQ, _i64(
            3)), IndexExpression('birthdate', IndexOperator.EQ, _i64(3))], 100)
        result = fixture_thrift_client.get_range_slices(cp, sp, key_range, ConsistencyLevel.ONE)
        assert len(result) == 1, result
        assert result[0].key == 'key3'
        assert len(result[0].columns) == 2, result[0].columns

    @pytest.mark.skip("Secondary indexes not implemented")
    def test_index_scan_uuid_names(self, fixture_thrift_client):
        _set_keyspace(fixture_thrift_client, 'Keyspace1')
        sp = SlicePredicate(slice_range=SliceRange('', ''))
        cp = ColumnParent('Indexed3')  # timeuuid name, utf8 values
        u = uuid.UUID('00000000-0000-1000-0000-000000000000').bytes
        u2 = uuid.UUID('00000000-0000-1000-0000-000000000001').bytes
        fixture_thrift_client.insert('key1', ColumnParent('Indexed3'), Column(u, 'a', 0), ConsistencyLevel.ONE)
        fixture_thrift_client.insert('key1', ColumnParent('Indexed3'), Column(u2, 'b', 0), ConsistencyLevel.ONE)
        # name comparator + data validator of incompatible types -- see CASSANDRA-2347
        key_range = KeyRange('', '', None, None, [IndexExpression(
            u, IndexOperator.EQ, 'a'), IndexExpression(u2, IndexOperator.EQ, 'b')], 100)
        result = fixture_thrift_client.get_range_slices(cp, sp, key_range, ConsistencyLevel.ONE)
        assert len(result) == 1, result

        cp = ColumnParent('Indexed2')  # timeuuid name, long values

        # name must be valid (TimeUUID)
        key_range = KeyRange('', '', None, None, [IndexExpression(
            'foo', IndexOperator.EQ, uuid.UUID('00000000-0000-1000-0000-000000000000').bytes)], 100)
        _expect_exception(lambda: fixture_thrift_client.get_range_slices(
            cp, sp, key_range, ConsistencyLevel.ONE), InvalidRequestException)

        # value must be valid (TimeUUID)
        key_range = KeyRange('', '', None, None, [IndexExpression(
            uuid.UUID('00000000-0000-1000-0000-000000000000').bytes, IndexOperator.EQ, "foo")], 100)
        _expect_exception(lambda: fixture_thrift_client.get_range_slices(
            cp, sp, key_range, ConsistencyLevel.ONE), InvalidRequestException)

    @pytest.mark.skip("Secondary indexes not implemented")
    def test_index_scan_expiring(self, fixture_thrift_client):
        """ Test that column ttled expires from KEYS index"""
        _set_keyspace(fixture_thrift_client, 'Keyspace1')
        fixture_thrift_client.insert('key1', ColumnParent('Indexed1'), Column('birthdate', _i64(1), 0, 2),
                                     ConsistencyLevel.ONE)
        cp = ColumnParent('Indexed1')
        sp = SlicePredicate(slice_range=SliceRange('', ''))
        key_range = KeyRange('', '', None, None, [IndexExpression('birthdate', IndexOperator.EQ, _i64(1))], 100)
        # query before expiration
        result = fixture_thrift_client.get_range_slices(cp, sp, key_range, ConsistencyLevel.ONE)
        assert len(result) == 1, result
        # wait for expiration and requery
        time.sleep(3)
        result = fixture_thrift_client.get_range_slices(cp, sp, key_range, ConsistencyLevel.ONE)
        assert len(result) == 0, result

    def test_column_not_found_quorum(self, fixture_thrift_client):
        _set_keyspace(fixture_thrift_client, 'Keyspace1')
        key = 'doesntexist'
        column_path = ColumnPath(column_family="Standard1", column="idontexist")
        try:
            fixture_thrift_client.get(key, column_path, ConsistencyLevel.QUORUM)
            assert False, ('columnpath %s existed in %s when it should not' % (column_path, key))
        except NotFoundException:
            assert True, 'column did not exist'

    @pytest.mark.skip("Super columns not implemented")
    def test_get_range_slice_after_deletion(self, fixture_thrift_client):
        _set_keyspace(fixture_thrift_client, 'Keyspace2')
        key = 'key1'
        # three supercoluns, each with "col1" subcolumn
        for i in range(1, 4):
            fixture_thrift_client.insert(key, ColumnParent('Super3', 'sc%d' % i), Column('col1', 'val1', 0),
                                         ConsistencyLevel.ONE)

        cp = ColumnParent('Super3')
        predicate = SlicePredicate(slice_range=SliceRange('sc1', 'sc3', False, count=1))
        k_range = KeyRange(start_key=key, end_key=key, count=1)

        # validate count=1 restricts to 1 supercolumn
        result = fixture_thrift_client.get_range_slices(cp, predicate, k_range, ConsistencyLevel.ONE)
        assert len(result[0].columns) == 1

        # remove sc1; add back subcolumn to override tombstone
        fixture_thrift_client.remove(key, ColumnPath('Super3', 'sc1'), 1, ConsistencyLevel.ONE)
        result = fixture_thrift_client.get_range_slices(cp, predicate, k_range, ConsistencyLevel.ONE)
        assert len(result[0].columns) == 1
        fixture_thrift_client.insert(key, ColumnParent('Super3', 'sc1'), Column('col1', 'val1', 2),
                                     ConsistencyLevel.ONE)
        result = fixture_thrift_client.get_range_slices(cp, predicate, k_range, ConsistencyLevel.ONE)
        assert len(result[0].columns) == 1, result[0].columns
        assert result[0].columns[0].super_column.name == 'sc1'

    def test_multi_slice(self, fixture_thrift_client):
        _set_keyspace(fixture_thrift_client, 'Keyspace1')
        _insert_six_columns(fixture_thrift_client, 'abc')
        L = [result.column
             for result in _big_multi_slice(fixture_thrift_client, 'abc')]
        assert L == _MULTI_SLICE_COLUMNS, L


@pytest.mark.dtest_full
class TestTruncate(ThriftTester):

    def test_truncate(self, fixture_thrift_client):
        _set_keyspace(fixture_thrift_client, 'Keyspace1')

        _insert_simple(fixture_thrift_client, )
        # _insert_super(fixture_thrift_client)

        # truncate Standard1
        fixture_thrift_client.truncate('Standard1')
        assert _big_slice(fixture_thrift_client, 'key1', ColumnParent('Standard1')) == []

        # truncate Super1
        # fixture_thrift_client.truncate('Super1')
        # assert _big_slice(fixture_thrift_client, 'key1', ColumnParent('Super1')) == []
        # assert _big_slice(fixture_thrift_client, 'key1', ColumnParent('Super1', 'sc1')) == []


@pytest.mark.dtest_full
class TestCQLAccesses(ThriftTester):

    @pytest.mark.skip("Not supported yet. See #2037")
    def test_range_tombstone_and_static(self, fixture_thrift_client):
        node1, = self.cluster.nodelist()
        session = self.patient_cql_connection(node1)

        # Create a CQL table with a static column and insert a row
        session.execute('USE "Keyspace1"')
        session.execute("CREATE TABLE t (k text, s text static, t text, v text, PRIMARY KEY (k, t))")

        session.execute("INSERT INTO t (k, s, t, v) VALUES ('k', 's', 't', 'v') USING TIMESTAMP 0")
        assert_one(session, "SELECT * FROM t", [u'k', 't', 's', 'v'])

        # Now submit a range deletion that should include both the row and the static value

        _set_keyspace(fixture_thrift_client, 'Keyspace1')

        mutations = [Mutation(deletion=Deletion(1, predicate=SlicePredicate(
            slice_range=SliceRange('', '', False, 1000))))]
        mutation_map = dict((table, mutations) for table in ['t'])
        keyed_mutations = dict((key, mutation_map) for key in ['k'])
        fixture_thrift_client.batch_mutate(keyed_mutations, ConsistencyLevel.ONE)

        # And check everything is gone
        assert_none(session, "SELECT * FROM t")


@pytest.mark.dtest_full
class TestCompactStorageThriftAccesses(ThriftTester):
    """
    Test thrift access to compact storage column families.
    """

    @pytest.mark.next_gating
    @pytest.mark.dtest_debug
    def test_get(self, fixture_thrift_client):
        node1, = self.cluster.nodelist()
        session = self.patient_cql_connection(node1)

        # Create a CQL table with a static column and insert a row
        session.execute("USE \"Keyspace1\"")
        session.execute("CREATE TABLE IF NOT EXISTS cs1 (k int PRIMARY KEY,v int) WITH COMPACT STORAGE")

        _set_keyspace(fixture_thrift_client, 'Keyspace1')
        CL = ConsistencyLevel.ONE
        i = 1
        fixture_thrift_client.insert(_i32(i), ColumnParent('cs1'), Column('v', _i32(i), 0), CL)
        _assert_column(fixture_thrift_client, 'cs1', _i32(i), 'v', _i32(i), 0)


@pytest.mark.dtest_full
class TestResultKeyOrder(ThriftTester):
    def test_get_range_slices_token_order(self, fixture_thrift_client):
        _set_keyspace(fixture_thrift_client, 'Keyspace2')
        for key in ['key1', 'key2', 'key3', 'key4', 'key5']:
            for cname in ['col1', 'col2', 'col3', 'col4', 'col5']:
                fixture_thrift_client.insert(key, ColumnParent('Standard1'), Column(cname, 'v-' + cname, 0),
                                             ConsistencyLevel.ONE)
        cp = ColumnParent('Standard1')
        predicate = SlicePredicate(column_names=['col1', 'col3'])
        range = KeyRange(start_token=str(-(1 << 63)), end_token=str(-(1 << 63)), count=100)
        result = fixture_thrift_client.get_range_slices(cp, predicate, range, ConsistencyLevel.ONE)
        assert len(result) == 5
        assert result[0].key == b'key5'
        assert result[1].key == b'key1'
        assert result[2].key == b'key4'
        assert result[3].key == b'key3'
        assert result[4].key == b'key2'


@pytest.mark.dtest_full
class TestWrappingRangeQueries(ThriftTester):
    """
    Test range queries, particularly those that wrap around the token
    space (start > end, so we query (start, +inf] and (-inf, end])

    Range queries are complicated, since the coordinator has to scan
    vnodes, and within a vnode, a replica has to scan shards, and merge
    it all together.
    """

    def test_wrapping_ranges(self, fixture_thrift_client):
        # murmur3 tokens obtained using CQL TOKEN() function
        key_token = {
            'a': -8839064797231613815,
            'c': -8198557465434950441,
            'e': -4200008757497435756,
            'd': -3786697372163639434,
            'b': 8833996863197925870,
        }

        def token_for(key, delta=0):
            return str(key_token[key] + delta)

        _set_keyspace(fixture_thrift_client, 'Keyspace1')
        CL = ConsistencyLevel.ONE
        for key in ['a', 'b', 'c', 'd', 'e']:
            fixture_thrift_client.insert(
                key,
                ColumnParent('Standard1'),
                Column('c1', 'c1-' + key, 0),
                CL)
            fixture_thrift_client.insert(
                key,
                ColumnParent('Standard1'),
                Column('c2', 'c2-' + key, 0),
                CL)

        """
        Test a token range query.

        first, last: the keys we want to read, in Thrift start-exclusive
                     end-inclusive format
        first_delta, last_delta: integers to add to tokens, in order to
                     adjust inclusivity/exclusivity
        count: partition limit for query
        expected: list of keys we expect to see
        """

        def test_range(first, first_delta, last, last_delta, count, expected):
            result = fixture_thrift_client.get_range_slices(
                ColumnParent('Standard1'),
                SlicePredicate(column_names=['c1', 'c2']),
                KeyRange(start_token=token_for(first, first_delta),
                         end_token=token_for(last, last_delta),
                         count=count),
                CL)
            for a_result, a_expected in zip(result, expected):
                assert a_result is not None
                assert a_expected is not None
                assert a_result.key == a_expected

        # token order: a c e d b
        test_range('a', 0, 'a', 0, 10, [b'c', b'e', b'd', b'b', b'a'])
        test_range('a', -1, 'a', -1, 10, [b'a', b'c', b'e', b'd', b'b'])
        test_range('a', 1, 'a', 1, 10, [b'c', b'e', b'd', b'b', b'a'])
        test_range('b', -1, 'a', 0, 10, [b'b', b'a'])
        test_range('b', 0, 'a', -1, 10, [])
        test_range('b', 0, 'a', 0, 10, [b'a'])
        test_range('b', -1, 'a', 0, 10, [b'b', b'a'])
        test_range('c', 0, 'e', -1, 2, [])
        test_range('b', -1, 'a', 1, 10, [b'b', b'a'])


@pytest.mark.dtest_full
class TestServerShutdown(ThriftTester):
    """
    Test thrift server can gracefully shutdown
    """

    def test_concurrent_stop(self, fixture_thrift_client):
        node1, = self.cluster.nodelist()

        def stop():
            node1.stop()

        def do_writes():
            _set_keyspace(fixture_thrift_client, 'Keyspace1')
            num_keys = 10000
            keys = ['key' + str(i) for i in range(1, num_keys + 1)]
            try:
                _insert_multi(fixture_thrift_client, keys)
            except:
                pass

        twriter = Thread(target=do_writes)
        twriter.start()
        tstop = Thread(target=stop)
        tstop.start()
        tstop.join()
        twriter.join()

        assert not node1.grep_log('Backtrace'), 'Thrift server crashed on stop'
