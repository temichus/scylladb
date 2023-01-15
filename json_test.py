import time
import logging
from textwrap import dedent

import pytest
from ccmlib.node import Node

from dtest_class import Tester

LOGGER = logging.getLogger(__name__)


def print_cqlsh(node: Node, cmds: str):
    return "\n".join(node.run_cqlsh(cmds, show_output=True, return_output=True))


class JsonTester(Tester):

    @pytest.fixture(scope='function', autouse=True)
    def prepare(self, set_dtest_setup_on_function):
        default_ks_name = "json_test"
        self.cluster.populate(1).start()
        nodes = self.cluster.nodelist()
        with self.patient_cql_connection(nodes[0]) as conn:
            conn.execute(
                "CREATE KEYSPACE {} WITH REPLICATION = {{'class': 'SimpleStrategy', 'replication_factor': 1}};".format(default_ks_name))

    @staticmethod
    def waiting_mv_prefill_finish(node: Node, cmds: str) -> str:
        output = None

        for _ in range(20):
            output = print_cqlsh(node=node, cmds=cmds)
            if output.splitlines()[3].strip():
                break
            time.sleep(10)
        if output:
            return output


@pytest.mark.dtest_full
@pytest.mark.single_node
class TestsToJsonSelect(JsonTester):
    """
    Tests using toJson with a SELECT statement
    """

    @pytest.mark.next_gating
    @pytest.mark.dtest_debug
    def test_basic_data_types(self, doctest_namespace):
        node = self.cluster.nodelist()[0]
        LOGGER.info("Create our schema:")
        node.run_cqlsh('''
             CREATE TABLE json_test.primitive_type_test (
              key1 text PRIMARY KEY,
              col1 ascii,
              col2 blob,
              col3 inet,
              col4 text,
              col5 timestamp,
              col6 timeuuid,
              col7 uuid,
              col8 varchar,
              col9 bigint,
              col10 decimal,
              col11 double,
              col12 float,
              col13 int,
              col14 varint,
              col15 boolean)
             ''')

        LOGGER.info("Update the row to have all values defined:")

        node.run_cqlsh('''
             INSERT INTO json_test.primitive_type_test (key1, col1, col2, col3, col4, col5, col6, col7, col8, col9, col10, col11, col12, col13, col14, col15)
               VALUES ('foo', 'bar', 0x0011, '127.0.0.1', 'blarg', '2011-02-03 04:05+0000', 0ad6dfb6-7a6e-11e4-bc39-b4b6763e9d6f, bdf5e8ac-a75e-4321-9ac8-938fc9576c4a, 'bleh', -9223372036854775808, 1234.45678, 98712312.1222, 98712312.5252, -2147483648, 2147483648, true)
             ''')

        LOGGER.info("Query the values back as json:")

        output = print_cqlsh(node, '''
             SELECT toJson(col1), toJson(col2), toJson(col3), toJson(col4), toJson(col5),
                    toJson(col6), toJson(col7), toJson(col8), toJson(col9), toJson(col10),
                    toJson(col11),toJson(col12),toJson(col13),toJson(col14),toJson(col15)
              FROM json_test.primitive_type_test WHERE key1 = 'foo'
             ''')
        # supporting two representation types for double and floats
        # see: scylladb/seastar@4f4e84b and scylladb/scylladb#12509 for more details
        assert \
            output == dedent("""
             system.tojson(col1) | system.tojson(col2) | system.tojson(col3) | system.tojson(col4) | system.tojson(col5)   | system.tojson(col6)                    | system.tojson(col7)                    | system.tojson(col8) | system.tojson(col9)  | system.tojson(col10) | system.tojson(col11) | system.tojson(col12) | system.tojson(col13) | system.tojson(col14) | system.tojson(col15)
            ---------------------+---------------------+---------------------+---------------------+-----------------------+----------------------------------------+----------------------------------------+---------------------+----------------------+----------------------+----------------------+----------------------+----------------------+----------------------+----------------------
                           "bar" |            "0x0011" |         "127.0.0.1" |             "blarg" | "2011-02-03T04:05:00" | "0ad6dfb6-7a6e-11e4-bc39-b4b6763e9d6f" | "bdf5e8ac-a75e-4321-9ac8-938fc9576c4a" |              "bleh" | -9223372036854775808 |           1234.45678 |          9.87123e+07 |          9.87123e+07 |          -2147483648 |           2147483648 |                 true

            (1 rows)

            """) or \
            output == dedent("""
             system.tojson(col1) | system.tojson(col2) | system.tojson(col3) | system.tojson(col4) | system.tojson(col5)   | system.tojson(col6)                    | system.tojson(col7)                    | system.tojson(col8) | system.tojson(col9)  | system.tojson(col10) | system.tojson(col11) | system.tojson(col12) | system.tojson(col13) | system.tojson(col14) | system.tojson(col15)
            ---------------------+---------------------+---------------------+---------------------+-----------------------+----------------------------------------+----------------------------------------+---------------------+----------------------+----------------------+----------------------+----------------------+----------------------+----------------------+----------------------
                           "bar" |            "0x0011" |         "127.0.0.1" |             "blarg" | "2011-02-03T04:05:00" | "0ad6dfb6-7a6e-11e4-bc39-b4b6763e9d6f" | "bdf5e8ac-a75e-4321-9ac8-938fc9576c4a" |              "bleh" | -9223372036854775808 |           1234.45678 |        98712312.1222 |             98712310 |          -2147483648 |           2147483648 |                 true

            (1 rows)

            """)

    def test_basic_data_types_with_null(self):
        LOGGER.info("Create our schema:")
        node = self.cluster.nodelist()[0]
        node.run_cqlsh('''
             CREATE TABLE json_test.primitive_type_test (
              key1 text PRIMARY KEY,
              col1 ascii,
              col2 blob,
              col3 inet,
              col4 text,
              col5 timestamp,
              col6 timeuuid,
              col7 uuid,
              col8 varchar,
              col9 bigint,
              col10 decimal,
              col11 double,
              col12 float,
              col13 int,
              col14 varint,
              col15 boolean)
             ''')

        LOGGER.info("Insert a row with only the key defined:")

        node.run_cqlsh('''
             INSERT into json_test.primitive_type_test (key1) values ('foo')
             ''')

        LOGGER.info("Get the non-key values as json:")

        output = print_cqlsh(node, '''
             SELECT toJson(col1), toJson(col2), toJson(col3), toJson(col4), toJson(col5),
                    toJson(col6), toJson(col7), toJson(col8), toJson(col9), toJson(col10),
                    toJson(col11),toJson(col12),toJson(col13),toJson(col14),toJson(col15)
              FROM json_test.primitive_type_test WHERE key1 = 'foo'
             ''')
        assert output == dedent("""
             system.tojson(col1) | system.tojson(col2) | system.tojson(col3) | system.tojson(col4) | system.tojson(col5) | system.tojson(col6) | system.tojson(col7) | system.tojson(col8) | system.tojson(col9) | system.tojson(col10) | system.tojson(col11) | system.tojson(col12) | system.tojson(col13) | system.tojson(col14) | system.tojson(col15)
            ---------------------+---------------------+---------------------+---------------------+---------------------+---------------------+---------------------+---------------------+---------------------+----------------------+----------------------+----------------------+----------------------+----------------------+----------------------
                            null |                null |                null |                null |                null |                null |                null |                null |                null |                 null |                 null |                 null |                 null |                 null |                 null

            (1 rows)

        """)

    # yes, it's probably weird to use json for counter changes
    def test_counters(self):

        LOGGER.info("Add a table with a few counters:")

        node = self.cluster.nodelist()[0]
        node.run_cqlsh('''
             CREATE TABLE json_test.my_counters (
              key1 text PRIMARY KEY,
              col1 counter)
             ''')

        LOGGER.info("Add a row with some counter values unset, and one incremented:")

        node.run_cqlsh("UPDATE json_test.my_counters SET col1 = col1+1 WHERE key1 = 'foo'")

        LOGGER.info("Query the empty/non-empty values back as json:")

        output = print_cqlsh(node, '''
             SELECT  toJson(col1) from json_test.my_counters
             ''')
        assert output == dedent("""
             system.tojson(col1)
            ---------------------
                               1

            (1 rows)

        """)

    def test_counters_with_null(self):

        LOGGER.info("Add a table with a few counters:")

        node = self.cluster.nodelist()[0]
        node.run_cqlsh('''
             CREATE TABLE json_test.my_counters (
              key1 text PRIMARY KEY,
              col1 counter,
              col2 counter )
        ''')

        LOGGER.info("Add a row with some counter values unset, and one incremented:")

        node.run_cqlsh("UPDATE json_test.my_counters SET col1 = col1+1 WHERE key1 = 'foo'")

        LOGGER.info("Query the empty/non-empty values back as json:")

        output = print_cqlsh(node, '''
             SELECT toJson(col1), toJson(col2) from json_test.my_counters
        ''')
        assert output == dedent("""
             system.tojson(col1) | system.tojson(col2)
            ---------------------+---------------------
                               1 |                null

            (1 rows)

        """)

    def test_complex_data_types(self):
        node = self.cluster.nodelist()[0]

        LOGGER.info("Build some user types and a schema that uses them:")

        node.run_cqlsh("CREATE TYPE json_test.t_todo_item (label text, details text)")
        node.run_cqlsh("CREATE TYPE json_test.t_todo_list (name text, todo_list list<frozen<t_todo_item>>)")
        node.run_cqlsh('''
             CREATE TYPE json_test.t_kitchen_sink (
               item1 ascii,
               item2 blob,
               item3 inet,
               item4 text,
               item5 timestamp,
               item6 timeuuid,
               item7 uuid,
               item8 varchar,
               item9 bigint,
               item10 decimal,
               item11 double,
               item12 float,
               item13 int,
               item14 varint,
               item15 boolean,
               item16 list<int> )
             ''')

        node.run_cqlsh('''
             CREATE TABLE json_test.complex_types (
               key1 text PRIMARY KEY,
               mylist list<text>,
               myset set<uuid>,
               mymap map<text, int>,
               mytuple frozen<tuple<text, int, uuid, boolean>>,
               myudt frozen<t_kitchen_sink>,
               mytodolists list<frozen<t_todo_list>>,
               many_sinks list<frozen<t_kitchen_sink>>,
               named_sinks map<text, frozen<t_kitchen_sink>> )
             ''')

        LOGGER.info("Define a row with the complex data types:")

        node.run_cqlsh('''
             INSERT INTO json_test.complex_types (key1, mylist, myset, mymap, mytuple, myudt, mytodolists, many_sinks, named_sinks)
             VALUES (
               'foo',
               ['five', 'six', 'seven', 'eight'],
               {4b66458a-2a19-41d3-af25-6faef4dea9fe, 080fdd90-ae74-41d6-9883-635625d3b069, 6cd7fab5-eacc-45c3-8414-6ad0177651d6},
               {'one' : 1, 'two' : 2, 'three': 3, 'four': 4},
               ('hey', 10, 16e69fba-a656-4932-8a01-6782a34505d9, true),
               {item1: 'heyimascii', item2: 0x0011, item3: '127.0.0.1', item4: 'whatev', item5: '2011-02-03 04:05+0000', item6: 0ad6dfb6-7a6e-11e4-bc39-b4b6763e9d6f, item7: bdf5e8ac-a75e-4321-9ac8-938fc9576c4a, item8: 'bleh', item9: -9223372036854775808, item10: 1234.45678, item11: 98712312.1222, item12: 98712312.5252, item13: -2147483648, item14: 2147483647, item15: false, item16: [1,3,5,7,11,13]},
               [{name: 'stuff to do!', todo_list: [{label: 'buy groceries', details: 'bread and milk'}, {label: 'pick up car from shop', details: '$325 due'}, {label: 'call dave', details: 'for some reason'}]}, {name: 'more stuff to do!', todo_list:[{label: 'buy new car', details: 'the old one is getting expensive'}, {label: 'price insurance', details: 'current cost is $95/mo'}]}],
               [{item1: 'asdf', item2: 0x0012, item3: '127.0.0.2', item4: 'whatev1', item5: '2012-02-03 04:05+0000', item6: d05a10c8-7c12-11e4-949d-b4b6763e9d6f, item7: f90b04b1-f9ad-4ffa-b869-a7d894ce6003, item8: 'tyru', item9: -9223372036854771111, item10: 4321.45678, item11: 10012312.1222, item12: 40012312.5252, item13: -1147483648, item14: 2047483648, item15: true, item16: [1,1,2,3,5,8]}, {item1: 'fdsa', item2: 0x0013, item3: '127.0.0.3', item4: 'whatev2', item5: '2013-02-03 04:05+0000', item6: d8ac38c8-7c12-11e4-8955-b4b6763e9d6f, item7: e3e84f21-f28c-4e0f-80e0-068a640ae53a, item8: 'uytr', item9: -3333372036854775808, item10: 1234.12321, item11: 20012312.1222, item12: 50012312.5252, item13: -1547483648, item14: 1947483648, item15: false, item16: [3,6,9,12,15]},{item1: 'zxcv', item2: 0x0014, item3: '127.0.0.4', item4: 'whatev3', item5: '2014-02-03 04:05+0000', item6: de30838a-7c12-11e4-a907-b4b6763e9d6f, item7: f9381f0e-9467-4d4c-9315-eb9f0232487b, item8: 'fghj', item9: -2239372036854775808, item10: 5555.55555, item11: 30012312.1222, item12: 60012312.5252, item13: 2147483647, item14: 1347483648, item15: true, item16: [0,1,0,1,2,0]}],
               {'namedsink1':{item1: 'asdf', item2: 0x0012, item3: '127.0.0.2', item4: 'whatev1', item5: '2012-02-03 04:05+0000', item6: d05a10c8-7c12-11e4-949d-b4b6763e9d6f, item7: f90b04b1-f9ad-4ffa-b869-a7d894ce6003, item8: 'tyru', item9: -9223372036854771111, item10: 4321.45678, item11: 10012312.1222, item12: 40012312.5252, item13: -1147483648, item14: 2047483648, item15: true, item16: [1,1,2,3,5,8]},'namedsink2':{item1: 'fdsa', item2: 0x0013, item3: '127.0.0.3', item4: 'whatev2', item5: '2013-02-03 04:05+0000', item6: d8ac38c8-7c12-11e4-8955-b4b6763e9d6f, item7: e3e84f21-f28c-4e0f-80e0-068a640ae53a, item8: 'uytr', item9: -3333372036854775808, item10: 1234.12321, item11: 20012312.1222, item12: 50012312.5252, item13: -1547483648, item14: 1947483648, item15: false, item16: [3,6,9,12,15]},'namedsink3':{item1: 'zxcv', item2: 0x0014, item3: '127.0.0.4', item4: 'whatev3', item5: '2014-02-03 04:05+0000', item6: de30838a-7c12-11e4-a907-b4b6763e9d6f, item7: f9381f0e-9467-4d4c-9315-eb9f0232487b, item8: 'fghj', item9: -2239372036854775808, item10: 5555.55555, item11: 30012312.1222, item12: 60012312.5252, item13: 2147483647, item14: 1347483648, item15: true, item16: [0,1,0,1,2,0]}})
             ''')

        LOGGER.info("Query back the json (one field at a time to make it easier to read) and make sure it looks as it should")
        LOGGER.info("Check that the list is returned ok:")

        output = print_cqlsh(node, "SELECT toJson(mylist) from json_test.complex_types where key1 = 'foo'")
        assert output == dedent("""
             system.tojson(mylist)
            -----------------------------------
             ["five", "six", "seven", "eight"]

            (1 rows)

        """)
        output = print_cqlsh(node, "SELECT toJson(myset) from json_test.complex_types where key1 = 'foo'")
        assert output == dedent("""
             system.tojson(myset)
            --------------------------------------------------------------------------------------------------------------------------
             ["080fdd90-ae74-41d6-9883-635625d3b069", "4b66458a-2a19-41d3-af25-6faef4dea9fe", "6cd7fab5-eacc-45c3-8414-6ad0177651d6"]

            (1 rows)

        """)
        output = print_cqlsh(node, "SELECT toJson(mymap) from json_test.complex_types where key1 = 'foo'")
        assert output == dedent("""
             system.tojson(mymap)
            ---------------------------------------------
             {"four": 4, "one": 1, "three": 3, "two": 2}

            (1 rows)

        """)

        output = print_cqlsh(node, "SELECT toJson(mytuple) from json_test.complex_types where key1 = 'foo'")
        assert output == dedent("""
             system.tojson(mytuple)
            -----------------------------------------------------------
             ["hey", 10, "16e69fba-a656-4932-8a01-6782a34505d9", true]

            (1 rows)

        """)

        output = print_cqlsh(node, "SELECT toJson(myudt) from json_test.complex_types where key1 = 'foo'")
        # supporting two representation types for double and floats
        # see: scylladb/seastar@4f4e84b and scylladb/scylladb#12509 for more details
        assert \
            output == dedent("""
             system.tojson(myudt)
            -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
             {"item1": "heyimascii", "item2": "0x0011", "item3": "127.0.0.1", "item4": "whatev", "item5": "2011-02-03T04:05:00", "item6": "0ad6dfb6-7a6e-11e4-bc39-b4b6763e9d6f", "item7": "bdf5e8ac-a75e-4321-9ac8-938fc9576c4a", "item8": "bleh", "item9": -9223372036854775808, "item10": 1234.45678, "item11": 9.87123e+07, "item12": 9.87123e+07, "item13": -2147483648, "item14": 2147483647, "item15": false, "item16": [1, 3, 5, 7, 11, 13]}

            (1 rows)

            """) or \
            output == dedent("""
             system.tojson(myudt)
            ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
             {"item1": "heyimascii", "item2": "0x0011", "item3": "127.0.0.1", "item4": "whatev", "item5": "2011-02-03T04:05:00", "item6": "0ad6dfb6-7a6e-11e4-bc39-b4b6763e9d6f", "item7": "bdf5e8ac-a75e-4321-9ac8-938fc9576c4a", "item8": "bleh", "item9": -9223372036854775808, "item10": 1234.45678, "item11": 98712312.1222, "item12": 98712310, "item13": -2147483648, "item14": 2147483647, "item15": false, "item16": [1, 3, 5, 7, 11, 13]}

            (1 rows)

            """)

        output = print_cqlsh(node, "SELECT toJson(mytodolists) from json_test.complex_types where key1 = 'foo'")
        assert output == dedent("""
             system.tojson(mytodolists)
            ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
             [{"name": "stuff to do!", "todo_list": [{"label": "buy groceries", "details": "bread and milk"}, {"label": "pick up car from shop", "details": "$325 due"}, {"label": "call dave", "details": "for some reason"}]}, {"name": "more stuff to do!", "todo_list": [{"label": "buy new car", "details": "the old one is getting expensive"}, {"label": "price insurance", "details": "current cost is $95/mo"}]}]

            (1 rows)

        """)

        output = print_cqlsh(node, "SELECT toJson(many_sinks) from json_test.complex_types where key1 = 'foo'")
        # supporting two representation types for double and floats
        # see: scylladb/seastar@4f4e84b and scylladb/scylladb#12509 for more details
        assert \
            output == dedent("""
             system.tojson(many_sinks)
            ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
             [{"item1": "asdf", "item2": "0x0012", "item3": "127.0.0.2", "item4": "whatev1", "item5": "2012-02-03T04:05:00", "item6": "d05a10c8-7c12-11e4-949d-b4b6763e9d6f", "item7": "f90b04b1-f9ad-4ffa-b869-a7d894ce6003", "item8": "tyru", "item9": -9223372036854771111, "item10": 4321.45678, "item11": 1.00123e+07, "item12": 4.00123e+07, "item13": -1147483648, "item14": 2047483648, "item15": true, "item16": [1, 1, 2, 3, 5, 8]}, {"item1": "fdsa", "item2": "0x0013", "item3": "127.0.0.3", "item4": "whatev2", "item5": "2013-02-03T04:05:00", "item6": "d8ac38c8-7c12-11e4-8955-b4b6763e9d6f", "item7": "e3e84f21-f28c-4e0f-80e0-068a640ae53a", "item8": "uytr", "item9": -3333372036854775808, "item10": 1234.12321, "item11": 2.00123e+07, "item12": 5.00123e+07, "item13": -1547483648, "item14": 1947483648, "item15": false, "item16": [3, 6, 9, 12, 15]}, {"item1": "zxcv", "item2": "0x0014", "item3": "127.0.0.4", "item4": "whatev3", "item5": "2014-02-03T04:05:00", "item6": "de30838a-7c12-11e4-a907-b4b6763e9d6f", "item7": "f9381f0e-9467-4d4c-9315-eb9f0232487b", "item8": "fghj", "item9": -2239372036854775808, "item10": 5555.55555, "item11": 3.00123e+07, "item12": 6.00123e+07, "item13": 2147483647, "item14": 1347483648, "item15": true, "item16": [0, 1, 0, 1, 2, 0]}]

            (1 rows)

        """) or \
            output == dedent("""
             system.tojson(many_sinks)
            -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
             [{"item1": "asdf", "item2": "0x0012", "item3": "127.0.0.2", "item4": "whatev1", "item5": "2012-02-03T04:05:00", "item6": "d05a10c8-7c12-11e4-949d-b4b6763e9d6f", "item7": "f90b04b1-f9ad-4ffa-b869-a7d894ce6003", "item8": "tyru", "item9": -9223372036854771111, "item10": 4321.45678, "item11": 10012312.1222, "item12": 40012310, "item13": -1147483648, "item14": 2047483648, "item15": true, "item16": [1, 1, 2, 3, 5, 8]}, {"item1": "fdsa", "item2": "0x0013", "item3": "127.0.0.3", "item4": "whatev2", "item5": "2013-02-03T04:05:00", "item6": "d8ac38c8-7c12-11e4-8955-b4b6763e9d6f", "item7": "e3e84f21-f28c-4e0f-80e0-068a640ae53a", "item8": "uytr", "item9": -3333372036854775808, "item10": 1234.12321, "item11": 20012312.1222, "item12": 50012310, "item13": -1547483648, "item14": 1947483648, "item15": false, "item16": [3, 6, 9, 12, 15]}, {"item1": "zxcv", "item2": "0x0014", "item3": "127.0.0.4", "item4": "whatev3", "item5": "2014-02-03T04:05:00", "item6": "de30838a-7c12-11e4-a907-b4b6763e9d6f", "item7": "f9381f0e-9467-4d4c-9315-eb9f0232487b", "item8": "fghj", "item9": -2239372036854775808, "item10": 5555.55555, "item11": 30012312.1222, "item12": 60012310, "item13": 2147483647, "item14": 1347483648, "item15": true, "item16": [0, 1, 0, 1, 2, 0]}]

            (1 rows)

            """)
        output = print_cqlsh(node, "SELECT toJson(named_sinks) from json_test.complex_types where key1 = 'foo'")
        # supporting two representation types for double and floats
        # see: scylladb/seastar@4f4e84b and scylladb/scylladb#12509 for more details
        assert \
            output == dedent("""
             system.tojson(named_sinks)
            ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
             {"namedsink1": {"item1": "asdf", "item2": "0x0012", "item3": "127.0.0.2", "item4": "whatev1", "item5": "2012-02-03T04:05:00", "item6": "d05a10c8-7c12-11e4-949d-b4b6763e9d6f", "item7": "f90b04b1-f9ad-4ffa-b869-a7d894ce6003", "item8": "tyru", "item9": -9223372036854771111, "item10": 4321.45678, "item11": 1.00123e+07, "item12": 4.00123e+07, "item13": -1147483648, "item14": 2047483648, "item15": true, "item16": [1, 1, 2, 3, 5, 8]}, "namedsink2": {"item1": "fdsa", "item2": "0x0013", "item3": "127.0.0.3", "item4": "whatev2", "item5": "2013-02-03T04:05:00", "item6": "d8ac38c8-7c12-11e4-8955-b4b6763e9d6f", "item7": "e3e84f21-f28c-4e0f-80e0-068a640ae53a", "item8": "uytr", "item9": -3333372036854775808, "item10": 1234.12321, "item11": 2.00123e+07, "item12": 5.00123e+07, "item13": -1547483648, "item14": 1947483648, "item15": false, "item16": [3, 6, 9, 12, 15]}, "namedsink3": {"item1": "zxcv", "item2": "0x0014", "item3": "127.0.0.4", "item4": "whatev3", "item5": "2014-02-03T04:05:00", "item6": "de30838a-7c12-11e4-a907-b4b6763e9d6f", "item7": "f9381f0e-9467-4d4c-9315-eb9f0232487b", "item8": "fghj", "item9": -2239372036854775808, "item10": 5555.55555, "item11": 3.00123e+07, "item12": 6.00123e+07, "item13": 2147483647, "item14": 1347483648, "item15": true, "item16": [0, 1, 0, 1, 2, 0]}}

            (1 rows)

           """) or \
            output == dedent("""
             system.tojson(named_sinks)
            -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
             {"namedsink1": {"item1": "asdf", "item2": "0x0012", "item3": "127.0.0.2", "item4": "whatev1", "item5": "2012-02-03T04:05:00", "item6": "d05a10c8-7c12-11e4-949d-b4b6763e9d6f", "item7": "f90b04b1-f9ad-4ffa-b869-a7d894ce6003", "item8": "tyru", "item9": -9223372036854771111, "item10": 4321.45678, "item11": 10012312.1222, "item12": 40012310, "item13": -1147483648, "item14": 2047483648, "item15": true, "item16": [1, 1, 2, 3, 5, 8]}, "namedsink2": {"item1": "fdsa", "item2": "0x0013", "item3": "127.0.0.3", "item4": "whatev2", "item5": "2013-02-03T04:05:00", "item6": "d8ac38c8-7c12-11e4-8955-b4b6763e9d6f", "item7": "e3e84f21-f28c-4e0f-80e0-068a640ae53a", "item8": "uytr", "item9": -3333372036854775808, "item10": 1234.12321, "item11": 20012312.1222, "item12": 50012310, "item13": -1547483648, "item14": 1947483648, "item15": false, "item16": [3, 6, 9, 12, 15]}, "namedsink3": {"item1": "zxcv", "item2": "0x0014", "item3": "127.0.0.4", "item4": "whatev3", "item5": "2014-02-03T04:05:00", "item6": "de30838a-7c12-11e4-a907-b4b6763e9d6f", "item7": "f9381f0e-9467-4d4c-9315-eb9f0232487b", "item8": "fghj", "item9": -2239372036854775808, "item10": 5555.55555, "item11": 30012312.1222, "item12": 60012310, "item13": 2147483647, "item14": 1347483648, "item15": true, "item16": [0, 1, 0, 1, 2, 0]}}

            (1 rows)

           """)

    def test_complex_data_types_with_null(self):
        node = self.cluster.nodelist()[0]

        LOGGER.info("Build some user types and a schema that uses them:")

        node.run_cqlsh("CREATE TYPE json_test.t_todo_item (label text, details text)")
        node.run_cqlsh("CREATE TYPE json_test.t_todo_list (name text, todo_list list<frozen<t_todo_item>>)")
        node.run_cqlsh('''
             CREATE TYPE json_test.t_kitchen_sink (
               item1 ascii,
               item2 blob,
               item3 inet,
               item4 text,
               item5 timestamp,
               item6 timeuuid,
               item7 uuid,
               item8 varchar,
               item9 bigint,
               item10 decimal,
               item11 double,
               item12 float,
               item13 int,
               item14 varint,
               item15 boolean,
               item16 list<int> )
        ''')

        node.run_cqlsh('''
             CREATE TABLE json_test.complex_types (
               key1 text PRIMARY KEY,
               mylist list<text>,
               myset set<uuid>,
               mymap map<text, int>,
               mytuple frozen<tuple<text, int, uuid, boolean>>,
               myudt frozen<t_kitchen_sink>,
               mytodolists list<frozen<t_todo_list>>,
               many_sinks list<frozen<t_kitchen_sink>>,
               named_sinks map<text, frozen<t_kitchen_sink>> )
        ''')

        LOGGER.info("Add a row without the complex fields defined:")

        node.run_cqlsh("INSERT INTO json_test.complex_types (key1) values ('foo')")

        LOGGER.info("Call toJson on the null fields:")

        output = print_cqlsh(node, '''
             SELECT toJson(mylist), toJson(myset), toJson(mymap), toJson(mytuple), toJson(myudt), toJson(mytodolists), toJson(many_sinks), toJson(named_sinks)
               FROM json_test.complex_types where key1 = 'foo'
        ''')
        assert output == dedent("""
             system.tojson(mylist) | system.tojson(myset) | system.tojson(mymap) | system.tojson(mytuple) | system.tojson(myudt) | system.tojson(mytodolists) | system.tojson(many_sinks) | system.tojson(named_sinks)
            -----------------------+----------------------+----------------------+------------------------+----------------------+----------------------------+---------------------------+----------------------------
                              null |                 null |                 null |                   null |                 null |                       null |                      null |                       null

            (1 rows)

        """)

    def test_mv_basic_data_types_with(self):
        node = self.cluster.nodelist()[0]

        LOGGER.info("Create our schema:")

        node.run_cqlsh('''
             CREATE TABLE json_test.primitive_type_test (
              key1 text PRIMARY KEY,
              col1 ascii,
              col2 blob,
              col3 inet,
              col4 text,
              col5 timestamp,
              col6 timeuuid,
              col7 uuid,
              col8 varchar,
              col9 bigint,
              col10 decimal,
              col11 double,
              col12 float,
              col13 int,
              col14 varint,
              col15 boolean)
        ''')

        LOGGER.info("Create materialized view:")

        node.run_cqlsh('''
             CREATE MATERIALIZED VIEW json_test.mv_primitive_type_test AS SELECT * FROM json_test.primitive_type_test WHERE key1 is not null and col1 is not null PRIMARY KEY (key1, col1)
        ''')

        LOGGER.info("Update the row to have all values defined:")

        node.run_cqlsh('''
             INSERT INTO json_test.primitive_type_test (key1, col1, col2, col3, col4, col5, col6, col7, col8, col9, col10, col11, col12, col13, col14, col15)
               VALUES ('foo', 'bar', 0x0011, '127.0.0.1', 'blarg', '2011-02-03 04:05+0000', 0ad6dfb6-7a6e-11e4-bc39-b4b6763e9d6f, bdf5e8ac-a75e-4321-9ac8-938fc9576c4a, 'bleh', -9223372036854775808, 1234.45678, 98712312.1222, 98712312.5252, -2147483648, 2147483648, true)
             ''')

        LOGGER.info("Query table the values back as json:")

        output = print_cqlsh(node, '''
             SELECT toJson(col1), toJson(col2), toJson(col3), toJson(col4), toJson(col5),
                    toJson(col6), toJson(col7), toJson(col8), toJson(col9), toJson(col10),
                    toJson(col11),toJson(col12),toJson(col13),toJson(col14),toJson(col15)
              FROM json_test.primitive_type_test WHERE key1 = 'foo'
        ''')
        # supporting two representation types for double and floats
        # see: scylladb/seastar@4f4e84b and scylladb/scylladb#12509 for more details
        assert \
            output == dedent("""
             system.tojson(col1) | system.tojson(col2) | system.tojson(col3) | system.tojson(col4) | system.tojson(col5)   | system.tojson(col6)                    | system.tojson(col7)                    | system.tojson(col8) | system.tojson(col9)  | system.tojson(col10) | system.tojson(col11) | system.tojson(col12) | system.tojson(col13) | system.tojson(col14) | system.tojson(col15)
            ---------------------+---------------------+---------------------+---------------------+-----------------------+----------------------------------------+----------------------------------------+---------------------+----------------------+----------------------+----------------------+----------------------+----------------------+----------------------+----------------------
                           "bar" |            "0x0011" |         "127.0.0.1" |             "blarg" | "2011-02-03T04:05:00" | "0ad6dfb6-7a6e-11e4-bc39-b4b6763e9d6f" | "bdf5e8ac-a75e-4321-9ac8-938fc9576c4a" |              "bleh" | -9223372036854775808 |           1234.45678 |          9.87123e+07 |          9.87123e+07 |          -2147483648 |           2147483648 |                 true

            (1 rows)

            """) or \
            output == dedent("""
             system.tojson(col1) | system.tojson(col2) | system.tojson(col3) | system.tojson(col4) | system.tojson(col5)   | system.tojson(col6)                    | system.tojson(col7)                    | system.tojson(col8) | system.tojson(col9)  | system.tojson(col10) | system.tojson(col11) | system.tojson(col12) | system.tojson(col13) | system.tojson(col14) | system.tojson(col15)
            ---------------------+---------------------+---------------------+---------------------+-----------------------+----------------------------------------+----------------------------------------+---------------------+----------------------+----------------------+----------------------+----------------------+----------------------+----------------------+----------------------
                           "bar" |            "0x0011" |         "127.0.0.1" |             "blarg" | "2011-02-03T04:05:00" | "0ad6dfb6-7a6e-11e4-bc39-b4b6763e9d6f" | "bdf5e8ac-a75e-4321-9ac8-938fc9576c4a" |              "bleh" | -9223372036854775808 |           1234.45678 |        98712312.1222 |             98712310 |          -2147483648 |           2147483648 |                 true

            (1 rows)

            """)

        LOGGER.info("Query materialized view the values back as json:")

        output = self.waiting_mv_prefill_finish(node, '''
             SELECT toJson(col1), toJson(col2), toJson(col3), toJson(col4), toJson(col5),
                    toJson(col6), toJson(col7), toJson(col8), toJson(col9), toJson(col10),
                    toJson(col11),toJson(col12),toJson(col13),toJson(col14),toJson(col15)
              FROM json_test.mv_primitive_type_test WHERE key1 = 'foo' and col1 = 'bar'
        ''')
        # supporting two representation types for double and floats
        # see: scylladb/seastar@4f4e84b and scylladb/scylladb#12509 for more details
        assert \
            output == dedent("""
             system.tojson(col1) | system.tojson(col2) | system.tojson(col3) | system.tojson(col4) | system.tojson(col5)   | system.tojson(col6)                    | system.tojson(col7)                    | system.tojson(col8) | system.tojson(col9)  | system.tojson(col10) | system.tojson(col11) | system.tojson(col12) | system.tojson(col13) | system.tojson(col14) | system.tojson(col15)
            ---------------------+---------------------+---------------------+---------------------+-----------------------+----------------------------------------+----------------------------------------+---------------------+----------------------+----------------------+----------------------+----------------------+----------------------+----------------------+----------------------
                           "bar" |            "0x0011" |         "127.0.0.1" |             "blarg" | "2011-02-03T04:05:00" | "0ad6dfb6-7a6e-11e4-bc39-b4b6763e9d6f" | "bdf5e8ac-a75e-4321-9ac8-938fc9576c4a" |              "bleh" | -9223372036854775808 |           1234.45678 |          9.87123e+07 |          9.87123e+07 |          -2147483648 |           2147483648 |                 true

            (1 rows)

            """) or \
            output == dedent("""
             system.tojson(col1) | system.tojson(col2) | system.tojson(col3) | system.tojson(col4) | system.tojson(col5)   | system.tojson(col6)                    | system.tojson(col7)                    | system.tojson(col8) | system.tojson(col9)  | system.tojson(col10) | system.tojson(col11) | system.tojson(col12) | system.tojson(col13) | system.tojson(col14) | system.tojson(col15)
            ---------------------+---------------------+---------------------+---------------------+-----------------------+----------------------------------------+----------------------------------------+---------------------+----------------------+----------------------+----------------------+----------------------+----------------------+----------------------+----------------------
                           "bar" |            "0x0011" |         "127.0.0.1" |             "blarg" | "2011-02-03T04:05:00" | "0ad6dfb6-7a6e-11e4-bc39-b4b6763e9d6f" | "bdf5e8ac-a75e-4321-9ac8-938fc9576c4a" |              "bleh" | -9223372036854775808 |           1234.45678 |        98712312.1222 |             98712310 |          -2147483648 |           2147483648 |                 true

            (1 rows)

            """)


@pytest.mark.dtest_full
@pytest.mark.single_node
class TestsFromJsonUpdate(JsonTester):
    """
    Tests using fromJson within UPDATE statements.
    """

    @pytest.mark.next_gating
    @pytest.mark.dtest_debug
    def test_basic_data_types(self):
        node = self.cluster.nodelist()[0]

        LOGGER.info("Create a table with the primitive types:")

        node.run_cqlsh('''
            CREATE TABLE json_test.primitive_type_test (
               key1 text PRIMARY KEY,
               col1 ascii,
               col2 blob,
               col3 inet,
               col4 text,
               col5 timestamp,
               col6 timeuuid,
               col7 uuid,
               col8 varchar,
               col9 bigint,
               col10 decimal,
               col11 double,
               col12 float,
               col13 int,
               col14 varint,
               col15 boolean)
            ''')

        LOGGER.info("Create a basic row and update the row using fromJson:")

        node.run_cqlsh('''INSERT INTO json_test.primitive_type_test (key1, col1, col2, col3, col4, col5, col6, col7, col8, col9, col10, col11, col12, col13, col14, col15)
            VALUES ('test', 'bar', 0x0011, '127.0.0.1', 'blarg', '2011-02-03 04:05+0000', 0ad6dfb6-7a6e-11e4-bc39-b4b6763e9d6f, bdf5e8ac-a75e-4321-9ac8-938fc9576c4a, 'bleh', -9223372036854775808, 1234.45678, 98712312.1222, 98712312.5252, -2147483648, 2147483648, true)
            ''')

        node.run_cqlsh('''
            UPDATE json_test.primitive_type_test
            SET col1 = fromJson('"bar1"'),
                col2 = fromJson('"0x0012"'),
                col3 = fromJson('"127.0.0.2"'),
                col4 = fromJson('"blarg2"'),
                col5 = fromJson('"2011-02-02 21:05:00.000+0000"'),
                col6 = fromJson('"efe0922a-8638-11e4-b2ac-b4b6763e9d6f"'),
                col7 = fromJson('"05dd0249-25b4-4dec-ba27-54f8730f3c03"'),
                col8 = fromJson('"bleh2"'),
                col9 = fromJson('-8223372036854775808'),
                col10 = fromJson('2234.45678'),
                col11 = fromJson('8.87123121222E7'),
                col12 = fromJson('7.8712312E7'),
                col13 = fromJson('-1947483648'),
                col14 = fromJson('1847483648'),
                col15 = fromJson('false')
            WHERE key1 = 'test'
            ''')

        LOGGER.info("Query back the row and make sure data is represented correctly:")

        output = print_cqlsh(node, '''
            SELECT col1, col2, col3, col4, col5, col6, col7, col8, col9, col10, col11, col12, col13, col14, col15
               FROM json_test.primitive_type_test WHERE key1 = 'test'
            ''')
        assert \
            output == dedent("""
             col1 | col2   | col3      | col4   | col5                            | col6                                 | col7                                 | col8  | col9                 | col10      | col11      | col12      | col13       | col14      | col15
            ------+--------+-----------+--------+---------------------------------+--------------------------------------+--------------------------------------+-------+----------------------+------------+------------+------------+-------------+------------+-------
             bar1 | 0x0012 | 127.0.0.2 | blarg2 | 2011-02-02 21:05:00.000000+0000 | efe0922a-8638-11e4-b2ac-b4b6763e9d6f | 05dd0249-25b4-4dec-ba27-54f8730f3c03 | bleh2 | -8223372036854775808 | 2234.45678 | 8.8712e+07 | 7.8712e+07 | -1947483648 | 1847483648 | False

            (1 rows)

            """)

    def test_complex_data_types(self):
        node = self.cluster.nodelist()[0]

        LOGGER.info("UDT and schema setup:")

        node.run_cqlsh('''
             CREATE TYPE json_test.t_todo_item (
               label text,
               details text)
        ''')

        node.run_cqlsh('''
             CREATE TYPE json_test.t_todo_list (
               name text,
               todo_list list<frozen<t_todo_item>>)
        ''')

        node.run_cqlsh('''
             CREATE TYPE json_test.t_kitchen_sink (
               item1 ascii,
               item2 blob,
               item3 inet,
               item4 text,
               item5 timestamp,
               item6 timeuuid,
               item7 uuid,
               item8 varchar,
               item9 bigint,
               item10 decimal,
               item11 double,
               item12 float,
               item13 int,
               item14 varint,
               item15 boolean,
               item16 list<int> )
        ''')

        node.run_cqlsh('''
             CREATE TABLE json_test.complex_types (
               key1 text PRIMARY KEY,
               mylist list<text>,
               myset set<uuid>,
               mymap map<text, int>,
               mytuple frozen<tuple<text, int, uuid, boolean>>,
               myudt frozen<t_kitchen_sink>,
               mytodolists list<frozen<t_todo_list>>,
               many_sinks list<frozen<t_kitchen_sink>>,
               named_sinks map<text, frozen<t_kitchen_sink>> )
        ''')

        LOGGER.info("Insert a row using plain cql values, then update the complex types using fromJson:")

        node.run_cqlsh('''
             INSERT INTO json_test.complex_types (key1, mylist, myset, mymap, mytuple, myudt, mytodolists, many_sinks, named_sinks)
             VALUES (
               'row1',
               ['five', 'six', 'seven', 'eight'],
               {4b66458a-2a19-41d3-af25-6faef4dea9fe, 080fdd90-ae74-41d6-9883-635625d3b069, 6cd7fab5-eacc-45c3-8414-6ad0177651d6},
               {'one' : 1, 'two' : 2, 'three': 3, 'four': 4},
               ('hey', 10, 16e69fba-a656-4932-8a01-6782a34505d9, true),
               {item1: 'heyimascii', item2: 0x0011, item3: '127.0.0.1', item4: 'whatev', item5: '2011-02-03 04:05+0000', item6: 0ad6dfb6-7a6e-11e4-bc39-b4b6763e9d6f, item7: bdf5e8ac-a75e-4321-9ac8-938fc9576c4a, item8: 'bleh', item9: -9223372036854775808, item10: 1234.45678, item11: 98712312.1222, item12: 98712312.5252, item13: -2147483648, item14: 2147483647, item15: false, item16: [1,3,5,7,11,13]},
               [{name: 'stuff to do!', todo_list: [{label: 'buy groceries', details: 'bread and milk'}, {label: 'pick up car from shop', details: '$325 due'}, {label: 'call dave', details: 'for some reason'}]}, {name: 'more stuff to do!', todo_list:[{label: 'buy new car', details: 'the old one is getting expensive'}, {label: 'price insurance', details: 'current cost is $95/mo'}]}],
               [{item1: 'asdf', item2: 0x0012, item3: '127.0.0.2', item4: 'whatev1', item5: '2012-02-03 04:05+0000', item6: d05a10c8-7c12-11e4-949d-b4b6763e9d6f, item7: f90b04b1-f9ad-4ffa-b869-a7d894ce6003, item8: 'tyru', item9: -9223372036854771111, item10: 4321.45678, item11: 10012312.1222, item12: 40012312.5252, item13: -1147483648, item14: 2047483648, item15: true, item16: [1,1,2,3,5,8]}, {item1: 'fdsa', item2: 0x0013, item3: '127.0.0.3', item4: 'whatev2', item5: '2013-02-03 04:05+0000', item6: d8ac38c8-7c12-11e4-8955-b4b6763e9d6f, item7: e3e84f21-f28c-4e0f-80e0-068a640ae53a, item8: 'uytr', item9: -3333372036854775808, item10: 1234.12321, item11: 20012312.1222, item12: 50012312.5252, item13: -1547483648, item14: 1947483648, item15: false, item16: [3,6,9,12,15]},{item1: 'zxcv', item2: 0x0014, item3: '127.0.0.4', item4: 'whatev3', item5: '2014-02-03 04:05+0000', item6: de30838a-7c12-11e4-a907-b4b6763e9d6f, item7: f9381f0e-9467-4d4c-9315-eb9f0232487b, item8: 'fghj', item9: -2239372036854775808, item10: 5555.55555, item11: 30012312.1222, item12: 60012312.5252, item13: 2147483647, item14: 1347483648, item15: true, item16: [0,1,0,1,2,0]}],
               {'namedsink1':{item1: 'asdf', item2: 0x0012, item3: '127.0.0.2', item4: 'whatev1', item5: '2012-02-03 04:05+0000', item6: d05a10c8-7c12-11e4-949d-b4b6763e9d6f, item7: f90b04b1-f9ad-4ffa-b869-a7d894ce6003, item8: 'tyru', item9: -9223372036854771111, item10: 4321.45678, item11: 10012312.1222, item12: 40012312.5252, item13: -1147483648, item14: 2047483648, item15: true, item16: [1,1,2,3,5,8]},'namedsink2':{item1: 'fdsa', item2: 0x0013, item3: '127.0.0.3', item4: 'whatev2', item5: '2013-02-03 04:05+0000', item6: d8ac38c8-7c12-11e4-8955-b4b6763e9d6f, item7: e3e84f21-f28c-4e0f-80e0-068a640ae53a, item8: 'uytr', item9: -3333372036854775808, item10: 1234.12321, item11: 20012312.1222, item12: 50012312.5252, item13: -1547483648, item14: 1947483648, item15: false, item16: [3,6,9,12,15]},'namedsink3':{item1: 'zxcv', item2: 0x0014, item3: '127.0.0.4', item4: 'whatev3', item5: '2014-02-03 04:05+0000', item6: de30838a-7c12-11e4-a907-b4b6763e9d6f, item7: f9381f0e-9467-4d4c-9315-eb9f0232487b, item8: 'fghj', item9: -2239372036854775808, item10: 5555.55555, item11: 30012312.1222, item12: 60012312.5252, item13: 2147483647, item14: 1347483648, item15: true, item16: [0,1,0,1,2,0]}})
        ''')

        node.run_cqlsh('''
             UPDATE json_test.complex_types
             SET mylist = fromJson('["nine", "ten", "eleven"]'),
                 myset = fromJson('["74887ce9-cea2-4d63-b874-cbe0a376bd3b", "3819f267-7075-4261-a33a-72e1e5a851d9"]'),
                 mymap = fromJson('{"five" : 5, "six" : 6, "seven": 7}'),
                 mytuple = fromJson('["whah?", 437, "e2058138-9a60-4f72-94f1-f48a21d59ff2", false]'),
                 myudt = fromJson('{"item1": "imitem1", "item2": "0x0014", "item3": "127.0.1.3", "item4": "asdf", "item5": "2009-02-01 04:05+0000", "item6": "25ba53f2-8645-11e4-afbc-b4b6763e9d6f", "item7": "97e053fb-225d-400a-9d4d-6b989e7d0cd9", "item8": "fdsa", "item9": -5223372036854775808, "item10": 2134.45678, "item11": 78712312.1222, "item12": 66712312.5252, "item13": -1347483648, "item14": 1097483647, "item15": true, "item16": [13,11,7,5,3,2,1]}'),
                 mytodolists = fromJson('[{"name": "a simple todo list", "todo_list": [{"label": "go to the store", "details": "need bread and milk"}, {"label": "drop off rental car", "details": "$180 due"}, {"label": "call bob", "details": "left a message"}]}, {"name": "a second todo list", "todo_list":[{"label": "buy a race car", "details": "need to go faster"}, {"label": "go running", "details": "just because"}]}]'),
                 many_sinks = fromJson('[{"item1": "qwerty", "item2": "0x0212", "item3": "127.5.5.5", "item4": "whatever0", "item5": "1999-02-03 04:05+0000", "item6": "3137ee20-8649-11e4-853d-b4b6763e9d6f", "item7": "034ca793-7147-45f5-bc60-2d5e76cc735d", "item8": "rewq", "item9": -6623372036854771111, "item10": 321.45678, "item11": 9992312.1222, "item12": 33012312.5252, "item13": -1547483648, "item14": 1847483648, "item15": false, "item16": [0,0,5,5,10,20]}, {"item1": "ljsdf", "item2": "0x2131", "item3": "10.10.1.5", "item4": "whatever1", "item5": "1987-02-08 02:05+0000", "item6": "a7a7c3d2-8649-11e4-88ff-b4b6763e9d6f", "item7": "9dfc88cd-ab10-4cf8-8046-344c905558c4", "item8": "vbnm", "item9": -1933372036854775808, "item10": 5534.12321, "item11": 19912312.1222, "item12": 49912312.5252, "item13": -997483648, "item14": 1007483648, "item15": true, "item16": [6,9,12,15,18,21]},{"item1": "hjkl", "item2": "0x2006", "item3": "127.3.3.1", "item4": "whatever2", "item5": "1996-04-01 04:05+0000", "item6": "0ed3a30a-864a-11e4-a596-b4b6763e9d6f", "item7": "49a74392-8295-4370-ba49-2ea57aaa5107", "item8": "nanananana", "item9": -1859372036854775808, "item10": 5455.55555, "item11": 29992312.1222, "item12": 59912312.5252, "item13": 1327483647, "item14": 1017483648, "item15": false, "item16": [1,0,5,10,1000]}]'),
                 named_sinks = fromJson('{"namedsink5000":{"item1": "aaaaaa", "item2": "0x5555", "item3": "127.1.2.3", "item4": "whatev10", "item5": "1999-02-01 01:05+0000", "item6": "c2c5dc0c-864a-11e4-adc6-b4b6763e9d6f", "item7": "2d630a5d-a9e1-4e4b-b551-fb953e16c1f8", "item8": "8s8s8s8aaaa", "item9": -3323372036854771111, "item10": 1221.45678, "item11": 8812312.1222, "item12": 20112312.5252, "item13": -757483648, "item14": 1017483648, "item15": false, "item16": [6,7,8,9,8,7]},"namedsink5001":{"item1": "bbbbbb", "item2": "0x000092", "item3": "192.168.0.3", "item4": "whatev20", "item5": "2003-08-03 01:05+0000", "item6": "3e4c2a16-864b-11e4-bb20-b4b6763e9d6f", "item7": "e404dd5c-9dc7-4b1d-bce9-eb85bb72297c", "item8": "mariachi", "item9": -2211372036854775808, "item10": 34.12321, "item11": 13132312.1222, "item12": 39912312.5252, "item13": -987483648, "item14": 1047483648, "item15": true, "item16": [500]},"namedsink5002":{"item1": "ccccccccc", "item2": "0x1002", "item3": "192.168.100.5", "item4": "whatev30", "item5": "2017-012-03 04:05+0000", "item6": "65badb36-8652-11e4-a5b9-b4b6763e9d6f", "item7": "aa371f6e-8044-4d11-8aec-2e698e55bb87", "item8": "abcdef", "item9": -1144372036854775808, "item10": 4321.55555, "item11": 21212312.1222, "item12": 33912312.5252, "item13": 1147483647, "item14": 1047483648, "item15": false, "item16": [100,200,500,300]}}')
             WHERE key1 = 'row1'
        ''')

        LOGGER.info("Query back the fields one by one and make sure they match the updates:")

        output = print_cqlsh(node, "SELECT mylist from json_test.complex_types where key1 = 'row1'")
        assert output == dedent("""
             mylist
            ---------------------------
             ['nine', 'ten', 'eleven']

            (1 rows)

        """)

        output = print_cqlsh(node, "SELECT myset from json_test.complex_types where key1 = 'row1'")
        assert output == dedent("""
             myset
            ------------------------------------------------------------------------------
             {3819f267-7075-4261-a33a-72e1e5a851d9, 74887ce9-cea2-4d63-b874-cbe0a376bd3b}

            (1 rows)

        """)

        output = print_cqlsh(node, "SELECT mymap from json_test.complex_types where key1 = 'row1'")
        assert output == dedent("""
             mymap
            -----------------------------------
             {'five': 5, 'seven': 7, 'six': 6}

            (1 rows)

        """)

        output = print_cqlsh(node, "SELECT mytuple from json_test.complex_types where key1 = 'row1'")
        assert output == dedent("""
             mytuple
            -------------------------------------------------------------
             ('whah?', 437, e2058138-9a60-4f72-94f1-f48a21d59ff2, False)

            (1 rows)

        """)

        output = print_cqlsh(node, "SELECT myudt from json_test.complex_types where key1 = 'row1'")
        assert output == dedent("""
             myudt
            ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
             {item1: 'imitem1', item2: 0x0014, item3: '127.0.1.3', item4: 'asdf', item5: '2009-02-01 04:05:00.000000+0000', item6: 25ba53f2-8645-11e4-afbc-b4b6763e9d6f, item7: 97e053fb-225d-400a-9d4d-6b989e7d0cd9, item8: 'fdsa', item9: -5223372036854775808, item10: 2134.45678, item11: 7.8712e+07, item12: 6.6712e+07, item13: -1347483648, item14: 1097483647, item15: True, item16: [13, 11, 7, 5, 3, 2, 1]}

            (1 rows)

        """)

        output = print_cqlsh(node, "SELECT mytodolists from json_test.complex_types where key1 = 'row1'")
        assert output == dedent("""
             mytodolists
            ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
             [{name: 'a simple todo list', todo_list: [{label: 'go to the store', details: 'need bread and milk'}, {label: 'drop off rental car', details: '$180 due'}, {label: 'call bob', details: 'left a message'}]}, {name: 'a second todo list', todo_list: [{label: 'buy a race car', details: 'need to go faster'}, {label: 'go running', details: 'just because'}]}]

            (1 rows)

        """)

        output = print_cqlsh(node, "SELECT many_sinks from json_test.complex_types where key1 = 'row1'")
        assert output == dedent("""
             many_sinks
            --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
             [{item1: 'qwerty', item2: 0x0212, item3: '127.5.5.5', item4: 'whatever0', item5: '1999-02-03 04:05:00.000000+0000', item6: 3137ee20-8649-11e4-853d-b4b6763e9d6f, item7: 034ca793-7147-45f5-bc60-2d5e76cc735d, item8: 'rewq', item9: -6623372036854771111, item10: 321.45678, item11: 9.9923e+06, item12: 3.3012e+07, item13: -1547483648, item14: 1847483648, item15: False, item16: [0, 0, 5, 5, 10, 20]}, {item1: 'ljsdf', item2: 0x2131, item3: '10.10.1.5', item4: 'whatever1', item5: '1987-02-08 02:05:00.000000+0000', item6: a7a7c3d2-8649-11e4-88ff-b4b6763e9d6f, item7: 9dfc88cd-ab10-4cf8-8046-344c905558c4, item8: 'vbnm', item9: -1933372036854775808, item10: 5534.12321, item11: 1.9912e+07, item12: 4.9912e+07, item13: -997483648, item14: 1007483648, item15: True, item16: [6, 9, 12, 15, 18, 21]}, {item1: 'hjkl', item2: 0x2006, item3: '127.3.3.1', item4: 'whatever2', item5: '1996-04-01 04:05:00.000000+0000', item6: 0ed3a30a-864a-11e4-a596-b4b6763e9d6f, item7: 49a74392-8295-4370-ba49-2ea57aaa5107, item8: 'nanananana', item9: -1859372036854775808, item10: 5455.55555, item11: 2.9992e+07, item12: 5.9912e+07, item13: 1327483647, item14: 1017483648, item15: False, item16: [1, 0, 5, 10, 1000]}]

            (1 rows)

        """)

        output = print_cqlsh(node, "SELECT named_sinks from json_test.complex_types where key1 = 'row1'")
        assert output == dedent("""
             named_sinks
            ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
             {'namedsink5000': {item1: 'aaaaaa', item2: 0x5555, item3: '127.1.2.3', item4: 'whatev10', item5: '1999-02-01 01:05:00.000000+0000', item6: c2c5dc0c-864a-11e4-adc6-b4b6763e9d6f, item7: 2d630a5d-a9e1-4e4b-b551-fb953e16c1f8, item8: '8s8s8s8aaaa', item9: -3323372036854771111, item10: 1221.45678, item11: 8.8123e+06, item12: 2.0112e+07, item13: -757483648, item14: 1017483648, item15: False, item16: [6, 7, 8, 9, 8, 7]}, 'namedsink5001': {item1: 'bbbbbb', item2: 0x000092, item3: '192.168.0.3', item4: 'whatev20', item5: '2003-08-03 01:05:00.000000+0000', item6: 3e4c2a16-864b-11e4-bb20-b4b6763e9d6f, item7: e404dd5c-9dc7-4b1d-bce9-eb85bb72297c, item8: 'mariachi', item9: -2211372036854775808, item10: 34.12321, item11: 1.3132e+07, item12: 3.9912e+07, item13: -987483648, item14: 1047483648, item15: True, item16: [500]}, 'namedsink5002': {item1: 'ccccccccc', item2: 0x1002, item3: '192.168.100.5', item4: 'whatev30', item5: '2017-12-03 04:05:00.000000+0000', item6: 65badb36-8652-11e4-a5b9-b4b6763e9d6f, item7: aa371f6e-8044-4d11-8aec-2e698e55bb87, item8: 'abcdef', item9: -1144372036854775808, item10: 4321.55555, item11: 2.1212e+07, item12: 3.3912e+07, item13: 1147483647, item14: 1047483648, item15: False, item16: [100, 200, 500, 300]}}

            (1 rows)

        """)

    @pytest.mark.next_gating
    @pytest.mark.dtest_debug
    def test_collection_update(self):
        node = self.cluster.nodelist()[0]

        LOGGER.info("Setup schema, add a row:")

        node.run_cqlsh('''
             CREATE TABLE json_test.basic_collections (
               key1 text PRIMARY KEY,
               mylist list<text>,
               myset set<text>,
               mymap map<text, int>)
        ''')

        node.run_cqlsh("INSERT INTO json_test.basic_collections (key1) values ('row1')")

        LOGGER.info("Issue some updates:")

        node.run_cqlsh('''
             UPDATE json_test.basic_collections
             SET mylist = fromJson('["c"]'),
                 myset = fromJson('["f"]'),
                 mymap = fromJson('{"one": 1}')
             WHERE key1 = 'row1'
        ''')

        node.run_cqlsh('''
             UPDATE json_test.basic_collections
             SET mylist = fromJson('["a","b"]') + mylist,
                 myset = myset + fromJson('["d","e"]'),
                 mymap['two'] = fromJson('2')
             WHERE key1 = 'row1'
        ''')

        LOGGER.info("Query the row and make sure it's correct:")

        output = print_cqlsh(node, "SELECT * from json_test.basic_collections where key1 = 'row1'")
        assert output == dedent("""
             key1 | mylist          | mymap                | myset
            ------+-----------------+----------------------+-----------------
             row1 | ['a', 'b', 'c'] | {'one': 1, 'two': 2} | {'d', 'e', 'f'}

            (1 rows)

        """)

        LOGGER.info("Some more updates of differing types:")

        node.run_cqlsh('''
             UPDATE json_test.basic_collections
             SET mylist = mylist + fromJson('["d","e"]'),
                 myset = myset + fromJson('["g"]'),
                 mymap['three'] = fromJson('3')
             WHERE key1 = 'row1'
        ''')

        node.run_cqlsh('''
             UPDATE json_test.basic_collections
             SET mylist = mylist - fromJson('["b"]'),
                 myset = myset - fromJson('["d"]'),
                 mymap['three'] = fromJson('4')
             WHERE key1 = 'row1'
        ''')

        LOGGER.info("Query final state and check it:")

        output = print_cqlsh(node, "SELECT * from json_test.basic_collections where key1 = 'row1'")
        assert output == dedent("""
             key1 | mylist               | mymap                            | myset
            ------+----------------------+----------------------------------+-----------------
             row1 | ['a', 'c', 'd', 'e'] | {'one': 1, 'three': 4, 'two': 2} | {'e', 'f', 'g'}

            (1 rows)

        """)


@pytest.mark.dtest_full
@pytest.mark.single_node
class TestsFromJsonSelect(JsonTester):
    """
    Tests using fromJson in conjunction with a SELECT statement
    """

    def test_selecting_pkey_as_json(self):
        node = self.cluster.nodelist()[0]

        LOGGER.info("Schema setup:")

        node.run_cqlsh('''
             CREATE TYPE json_test.t_person_name (
               first text,
               middle text,
               last text)
        ''')

        node.run_cqlsh('''
             CREATE TABLE json_test.person_info (
               name frozen<t_person_name> PRIMARY KEY,
               info text )
        ''')

        LOGGER.info("Add a row:")

        node.run_cqlsh(
            "INSERT INTO json_test.person_info (name, info) VALUES ({first: 'test', middle: 'guy', last: 'jones'}, 'enjoys bacon')")

        LOGGER.info("Query the row back on the primary key with fromJson:")

        output = print_cqlsh(node, '''
             SELECT * FROM json_test.person_info WHERE name = fromJson('{"first":"test", "middle":"guy", "last":"jones"}')
        ''')
        assert output == dedent("""
             name                                          | info
            -----------------------------------------------+--------------
             {first: 'test', middle: 'guy', last: 'jones'} | enjoys bacon

            (1 rows)

        """)

    def test_select_using_secondary_index(self):
        node = self.cluster.nodelist()[0]

        LOGGER.info("Schema setup and secondary index:")

        node.run_cqlsh('''
             CREATE TYPE json_test.t_person_name (
               first text,
               middle text,
               last text )
        ''')

        node.run_cqlsh('''
             CREATE TABLE json_test.person_likes (
               id uuid PRIMARY KEY,
               name frozen<t_person_name>,
               like text )
        ''')

        node.run_cqlsh("CREATE INDEX person_likes_name ON json_test.person_likes (name)")

        LOGGER.info("Add a row:")

        node.run_cqlsh(
            "INSERT INTO json_test.person_likes (id, name, like) VALUES (99b81888-e889-44aa-a511-cbd451c8a024, {first:'test', middle: 'guy', last:'jones'}, 'art')")

        LOGGER.info("Query the row back using fromJson with the secondary index:")

        output = print_cqlsh(node, '''
             SELECT * from json_test.person_likes where name = fromJson('{"first":"test", "middle":"guy", "last":"jones"}')
        ''')
        assert output == dedent("""
             id                                   | like | name
            --------------------------------------+------+-----------------------------------------------
             99b81888-e889-44aa-a511-cbd451c8a024 |  art | {first: 'test', middle: 'guy', last: 'jones'}

            (1 rows)

        """)


@pytest.mark.dtest_full
@pytest.mark.single_node
class TestsFromJsonInsert(JsonTester):
    """
    Tests using fromJson within INSERT statements.
    """

    @pytest.mark.next_gating
    @pytest.mark.dtest_debug
    def test_basic_data_types(self):
        node = self.cluster.nodelist()[0]

        LOGGER.info("Create a table with the primitive types:")

        node.run_cqlsh('''
             CREATE TABLE json_test.primitive_type_test (
               key1 text PRIMARY KEY,
               col1 ascii,
               col2 blob,
               col3 inet,
               col4 text,
               col5 timestamp,
               col6 timeuuid,
               col7 uuid,
               col8 varchar,
               col9 bigint,
               col10 decimal,
               col11 double,
               col12 float,
               col13 int,
               col14 varint,
               col15 boolean)
        ''')

        LOGGER.info("Create a full row using fromJson for each value:")

        node.run_cqlsh('''
             INSERT INTO json_test.primitive_type_test (key1, col1, col2, col3, col4, col5, col6, col7, col8, col9, col10, col11, col12, col13, col14, col15)
               VALUES (fromJson('"test"'), fromJson('"bar"'), fromJson('"0x0011"'), fromJson('"127.0.0.1"'), fromJson('"blarg"'),
                fromJson('"2011-02-02 21:05:00.000+0200"'), fromJson('"0ad6dfb6-7a6e-11e4-bc39-b4b6763e9d6f"'), fromJson('"bdf5e8ac-a75e-4321-9ac8-938fc9576c4a"'),
                fromJson('"bleh"'), fromJson('-9223372036854775808'), fromJson('1234.45678'), fromJson('9.87123121222E7'), fromJson('9.8712312E7'),
                fromJson('-2147483648'), fromJson('2147483648'), fromJson('true'))
        ''')

        LOGGER.info("Query back the row and make sure data is represented correctly:")

        output = print_cqlsh(node, '''
             SELECT col1, col2, col3, col4, col5, col6, col7, col8, col9, col10, col11, col12, col13, col14, col15
             FROM json_test.primitive_type_test WHERE key1 = 'test'
        ''')
        assert output == dedent("""
             col1 | col2   | col3      | col4  | col5                            | col6                                 | col7                                 | col8 | col9                 | col10      | col11      | col12      | col13       | col14      | col15
            ------+--------+-----------+-------+---------------------------------+--------------------------------------+--------------------------------------+------+----------------------+------------+------------+------------+-------------+------------+-------
              bar | 0x0011 | 127.0.0.1 | blarg | 2011-02-02 19:05:00.000000+0000 | 0ad6dfb6-7a6e-11e4-bc39-b4b6763e9d6f | bdf5e8ac-a75e-4321-9ac8-938fc9576c4a | bleh | -9223372036854775808 | 1234.45678 | 9.8712e+07 | 9.8712e+07 | -2147483648 | 2147483648 |  True

            (1 rows)

        """)

        LOGGER.info(
            "Query row back as json to see if the json representation queried from the DB matches the json that was used on insert:")

        output = print_cqlsh(node, '''
             SELECT toJson(col1), toJson(col2), toJson(col3), toJson(col4), toJson(col5),
                    toJson(col6), toJson(col7), toJson(col8), toJson(col9), toJson(col10),
                    toJson(col11),toJson(col12),toJson(col13),toJson(col14),toJson(col15)
             FROM json_test.primitive_type_test WHERE key1 = 'test'
        ''')
        # supporting two representation types for double and floats
        # see: scylladb/seastar@4f4e84b and scylladb/scylladb#12509 for more details
        assert \
            output == dedent("""
             system.tojson(col1) | system.tojson(col2) | system.tojson(col3) | system.tojson(col4) | system.tojson(col5)   | system.tojson(col6)                    | system.tojson(col7)                    | system.tojson(col8) | system.tojson(col9)  | system.tojson(col10) | system.tojson(col11) | system.tojson(col12) | system.tojson(col13) | system.tojson(col14) | system.tojson(col15)
            ---------------------+---------------------+---------------------+---------------------+-----------------------+----------------------------------------+----------------------------------------+---------------------+----------------------+----------------------+----------------------+----------------------+----------------------+----------------------+----------------------
                           "bar" |            "0x0011" |         "127.0.0.1" |             "blarg" | "2011-02-02T19:05:00" | "0ad6dfb6-7a6e-11e4-bc39-b4b6763e9d6f" | "bdf5e8ac-a75e-4321-9ac8-938fc9576c4a" |              "bleh" | -9223372036854775808 |           1234.45678 |          9.87123e+07 |          9.87123e+07 |          -2147483648 |           2147483648 |                 true

            (1 rows)

            """) or \
            output == dedent("""
             system.tojson(col1) | system.tojson(col2) | system.tojson(col3) | system.tojson(col4) | system.tojson(col5)   | system.tojson(col6)                    | system.tojson(col7)                    | system.tojson(col8) | system.tojson(col9)  | system.tojson(col10) | system.tojson(col11) | system.tojson(col12) | system.tojson(col13) | system.tojson(col14) | system.tojson(col15)
            ---------------------+---------------------+---------------------+---------------------+-----------------------+----------------------------------------+----------------------------------------+---------------------+----------------------+----------------------+----------------------+----------------------+----------------------+----------------------+----------------------
                           "bar" |            "0x0011" |         "127.0.0.1" |             "blarg" | "2011-02-02T19:05:00" | "0ad6dfb6-7a6e-11e4-bc39-b4b6763e9d6f" | "bdf5e8ac-a75e-4321-9ac8-938fc9576c4a" |              "bleh" | -9223372036854775808 |           1234.45678 |        98712312.1222 |             98712310 |          -2147483648 |           2147483648 |                 true

            (1 rows)

            """)

    def test_complex_data_types(self):
        node = self.cluster.nodelist()[0]

        LOGGER.info("Build some user types and a schema that uses them:")

        node.run_cqlsh("CREATE TYPE json_test.t_todo_item (label text, details text)")
        node.run_cqlsh("CREATE TYPE json_test.t_todo_list (name text, todo_list list<frozen<t_todo_item>>)")
        node.run_cqlsh('''
             CREATE TYPE json_test.t_kitchen_sink (
               item1 ascii,
               item2 blob,
               item3 inet,
               item4 text,
               item5 timestamp,
               item6 timeuuid,
               item7 uuid,
               item8 varchar,
               item9 bigint,
               item10 decimal,
               item11 double,
               item12 float,
               item13 int,
               item14 varint,
               item15 boolean,
               item16 list<int> )
             ''')

        node.run_cqlsh('''
             CREATE TABLE json_test.complex_types (
               key1 text PRIMARY KEY,
               mylist list<text>,
               myset set<uuid>,
               mymap map<text, int>,
               mytuple frozen<tuple<text, int, uuid, boolean>>,
               myudt frozen<t_kitchen_sink>,
               mytodolists list<frozen<t_todo_list>>,
               many_sinks list<frozen<t_kitchen_sink>>,
               named_sinks map<text, frozen<t_kitchen_sink>> )
             ''')

        node.run_cqlsh('''
             INSERT INTO json_test.complex_types (key1, mylist, myset, mymap, mytuple, myudt, mytodolists, many_sinks, named_sinks)
             VALUES (
               'row1',
               ['five', 'six', 'seven', 'eight'],
               {4b66458a-2a19-41d3-af25-6faef4dea9fe, 080fdd90-ae74-41d6-9883-635625d3b069, 6cd7fab5-eacc-45c3-8414-6ad0177651d6},
               {'one' : 1, 'two' : 2, 'three': 3, 'four': 4},
               ('hey', 10, 16e69fba-a656-4932-8a01-6782a34505d9, true),
               {item1: 'heyimascii', item2: 0x0011, item3: '127.0.0.1', item4: 'whatev', item5: '2011-02-03 04:05+0000', item6: 0ad6dfb6-7a6e-11e4-bc39-b4b6763e9d6f, item7: bdf5e8ac-a75e-4321-9ac8-938fc9576c4a, item8: 'bleh', item9: -9223372036854775808, item10: 1234.45678, item11: 98712312.1222, item12: 98712312.5252, item13: -2147483648, item14: 2147483647, item15: false, item16: [1,3,5,7,11,13]},
               [{name: 'stuff to do!', todo_list: [{label: 'buy groceries', details: 'bread and milk'}, {label: 'pick up car from shop', details: '$325 due'}, {label: 'call dave', details: 'for some reason'}]}, {name: 'more stuff to do!', todo_list:[{label: 'buy new car', details: 'the old one is getting expensive'}, {label: 'price insurance', details: 'current cost is $95/mo'}]}],
               [{item1: 'asdf', item2: 0x0012, item3: '127.0.0.2', item4: 'whatev1', item5: '2012-02-03 04:05+0000', item6: d05a10c8-7c12-11e4-949d-b4b6763e9d6f, item7: f90b04b1-f9ad-4ffa-b869-a7d894ce6003, item8: 'tyru', item9: -9223372036854771111, item10: 4321.45678, item11: 10012312.1222, item12: 40012312.5252, item13: -1147483648, item14: 2047483648, item15: true, item16: [1,1,2,3,5,8]}, {item1: 'fdsa', item2: 0x0013, item3: '127.0.0.3', item4: 'whatev2', item5: '2013-02-03 04:05+0000', item6: d8ac38c8-7c12-11e4-8955-b4b6763e9d6f, item7: e3e84f21-f28c-4e0f-80e0-068a640ae53a, item8: 'uytr', item9: -3333372036854775808, item10: 1234.12321, item11: 20012312.1222, item12: 50012312.5252, item13: -1547483648, item14: 1947483648, item15: false, item16: [3,6,9,12,15]},{item1: 'zxcv', item2: 0x0014, item3: '127.0.0.4', item4: 'whatev3', item5: '2014-02-03 04:05+0000', item6: de30838a-7c12-11e4-a907-b4b6763e9d6f, item7: f9381f0e-9467-4d4c-9315-eb9f0232487b, item8: 'fghj', item9: -2239372036854775808, item10: 5555.55555, item11: 30012312.1222, item12: 60012312.5252, item13: 2147483647, item14: 1347483648, item15: true, item16: [0,1,0,1,2,0]}],
               {'namedsink1':{item1: 'asdf', item2: 0x0012, item3: '127.0.0.2', item4: 'whatev1', item5: '2012-02-03 04:05+0000', item6: d05a10c8-7c12-11e4-949d-b4b6763e9d6f, item7: f90b04b1-f9ad-4ffa-b869-a7d894ce6003, item8: 'tyru', item9: -9223372036854771111, item10: 4321.45678, item11: 10012312.1222, item12: 40012312.5252, item13: -1147483648, item14: 2047483648, item15: true, item16: [1,1,2,3,5,8]},'namedsink2':{item1: 'fdsa', item2: 0x0013, item3: '127.0.0.3', item4: 'whatev2', item5: '2013-02-03 04:05+0000', item6: d8ac38c8-7c12-11e4-8955-b4b6763e9d6f, item7: e3e84f21-f28c-4e0f-80e0-068a640ae53a, item8: 'uytr', item9: -3333372036854775808, item10: 1234.12321, item11: 20012312.1222, item12: 50012312.5252, item13: -1547483648, item14: 1947483648, item15: false, item16: [3,6,9,12,15]},'namedsink3':{item1: 'zxcv', item2: 0x0014, item3: '127.0.0.4', item4: 'whatev3', item5: '2014-02-03 04:05+0000', item6: de30838a-7c12-11e4-a907-b4b6763e9d6f, item7: f9381f0e-9467-4d4c-9315-eb9f0232487b, item8: 'fghj', item9: -2239372036854775808, item10: 5555.55555, item11: 30012312.1222, item12: 60012312.5252, item13: 2147483647, item14: 1347483648, item15: true, item16: [0,1,0,1,2,0]}})
        ''')

        LOGGER.info("Add a row:")

        node.run_cqlsh('''
             INSERT INTO json_test.complex_types (key1, mylist, myset, mymap, mytuple, myudt, mytodolists, many_sinks, named_sinks)
             VALUES (
               fromJson('"row2"'),
               fromJson('["five", "six", "seven", "eight"]'),
               fromJson('["4b66458a-2a19-41d3-af25-6faef4dea9fe", "080fdd90-ae74-41d6-9883-635625d3b069", "6cd7fab5-eacc-45c3-8414-6ad0177651d6"]'),
               fromJson('{"one" : 1, "two" : 2, "three": 3, "four": 4}'),
               fromJson('["hey", 10, "16e69fba-a656-4932-8a01-6782a34505d9", true]'),
               fromJson('{"item1": "heyimascii", "item2": "0x0011", "item3": "127.0.0.1", "item4": "whatev", "item5": "2011-02-03 04:05+0000", "item6": "0ad6dfb6-7a6e-11e4-bc39-b4b6763e9d6f", "item7": "bdf5e8ac-a75e-4321-9ac8-938fc9576c4a", "item8": "bleh", "item9": -9223372036854775808, "item10": 1234.45678, "item11": 98712312.1222, "item12": 98712312.5252, "item13": -2147483648, "item14": 2147483647, "item15": false, "item16": [1,3,5,7,11,13]}'),
               fromJson('[{"name": "stuff to do!", "todo_list": [{"label": "buy groceries", "details": "bread and milk"}, {"label": "pick up car from shop", "details": "$325 due"}, {"label": "call dave", "details": "for some reason"}]}, {"name": "more stuff to do!", "todo_list":[{"label": "buy new car", "details": "the old one is getting expensive"}, {"label": "price insurance", "details": "current cost is $95/mo"}]}]'),
               fromJson('[{"item1": "asdf", "item2": "0x0012", "item3": "127.0.0.2", "item4": "whatev1", "item5": "2012-02-03 04:05+0000", "item6": "d05a10c8-7c12-11e4-949d-b4b6763e9d6f", "item7": "f90b04b1-f9ad-4ffa-b869-a7d894ce6003", "item8": "tyru", "item9": -9223372036854771111, "item10": 4321.45678, "item11": 10012312.1222, "item12": 40012312.5252, "item13": -1147483648, "item14": 2047483648, "item15": true, "item16": [1,1,2,3,5,8]}, {"item1": "fdsa", "item2": "0x0013", "item3": "127.0.0.3", "item4": "whatev2", "item5": "2013-02-03 04:05+0000", "item6": "d8ac38c8-7c12-11e4-8955-b4b6763e9d6f", "item7": "e3e84f21-f28c-4e0f-80e0-068a640ae53a", "item8": "uytr", "item9": -3333372036854775808, "item10": 1234.12321, "item11": 20012312.1222, "item12": 50012312.5252, "item13": -1547483648, "item14": 1947483648, "item15": false, "item16": [3,6,9,12,15]},{"item1": "zxcv", "item2": "0x0014", "item3": "127.0.0.4", "item4": "whatev3", "item5": "2014-02-03 04:05+0000", "item6": "de30838a-7c12-11e4-a907-b4b6763e9d6f", "item7": "f9381f0e-9467-4d4c-9315-eb9f0232487b", "item8": "fghj", "item9": -2239372036854775808, "item10": 5555.55555, "item11": 30012312.1222, "item12": 60012312.5252, "item13": 2147483647, "item14": 1347483648, "item15": true, "item16": [0,1,0,1,2,0]}]'),
               fromJson('{"namedsink1":{"item1": "asdf", "item2": "0x0012", "item3": "127.0.0.2", "item4": "whatev1", "item5": "2012-02-03 04:05+0000", "item6": "d05a10c8-7c12-11e4-949d-b4b6763e9d6f", "item7": "f90b04b1-f9ad-4ffa-b869-a7d894ce6003", "item8": "tyru", "item9": -9223372036854771111, "item10": 4321.45678, "item11": 10012312.1222, "item12": 40012312.5252, "item13": -1147483648, "item14": 2047483648, "item15": true, "item16": [1,1,2,3,5,8]},"namedsink2":{"item1": "fdsa", "item2": "0x0013", "item3": "127.0.0.3", "item4": "whatev2", "item5": "2013-02-03 04:05+0000", "item6": "d8ac38c8-7c12-11e4-8955-b4b6763e9d6f", "item7": "e3e84f21-f28c-4e0f-80e0-068a640ae53a", "item8": "uytr", "item9": -3333372036854775808, "item10": 1234.12321, "item11": 20012312.1222, "item12": 50012312.5252, "item13": -1547483648, "item14": 1947483648, "item15": false, "item16": [3,6,9,12,15]},"namedsink3":{"item1": "zxcv", "item2": "0x0014", "item3": "127.0.0.4", "item4": "whatev3", "item5": "2014-02-03 04:05+0000", "item6": "de30838a-7c12-11e4-a907-b4b6763e9d6f", "item7": "f9381f0e-9467-4d4c-9315-eb9f0232487b", "item8": "fghj", "item9": -2239372036854775808, "item10": 5555.55555, "item11": 30012312.1222, "item12": 60012312.5252, "item13": 2147483647, "item14": 1347483648, "item15": true, "item16": [0,1,0,1,2,0]}}'))
        ''')

        LOGGER.info("Query back the the normal cql inserted row and the fromJson inserted row, and compare to make sure they match (do this one field at a time for easier reading):")

        output = print_cqlsh(node, "SELECT key1, mylist from json_test.complex_types where key1 in ('row1', 'row2')")
        assert output == dedent("""
             key1 | mylist
            ------+-----------------------------------
             row1 | ['five', 'six', 'seven', 'eight']
             row2 | ['five', 'six', 'seven', 'eight']

            (2 rows)

        """)
        output = print_cqlsh(node, "SELECT key1, myset from json_test.complex_types where key1 in ('row1', 'row2')")
        assert output == dedent("""
             key1 | myset
            ------+--------------------------------------------------------------------------------------------------------------------
             row1 | {080fdd90-ae74-41d6-9883-635625d3b069, 4b66458a-2a19-41d3-af25-6faef4dea9fe, 6cd7fab5-eacc-45c3-8414-6ad0177651d6}
             row2 | {080fdd90-ae74-41d6-9883-635625d3b069, 4b66458a-2a19-41d3-af25-6faef4dea9fe, 6cd7fab5-eacc-45c3-8414-6ad0177651d6}

            (2 rows)

        """)
        output = print_cqlsh(node, "SELECT key1, mymap from json_test.complex_types where key1 in ('row1', 'row2')")
        assert output == dedent("""
             key1 | mymap
            ------+---------------------------------------------
             row1 | {'four': 4, 'one': 1, 'three': 3, 'two': 2}
             row2 | {'four': 4, 'one': 1, 'three': 3, 'two': 2}

            (2 rows)

        """)
        output = print_cqlsh(node, "SELECT key1, mytuple from json_test.complex_types where key1 in ('row1', 'row2')")
        assert output == dedent("""
             key1 | mytuple
            ------+---------------------------------------------------------
             row1 | ('hey', 10, 16e69fba-a656-4932-8a01-6782a34505d9, True)
             row2 | ('hey', 10, 16e69fba-a656-4932-8a01-6782a34505d9, True)

            (2 rows)

        """)
        output = print_cqlsh(node, "SELECT key1, myudt from json_test.complex_types where key1 in ('row1', 'row2')")
        assert output == dedent("""
             key1 | myudt
            ------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
             row1 | {item1: 'heyimascii', item2: 0x0011, item3: '127.0.0.1', item4: 'whatev', item5: '2011-02-03 04:05:00.000000+0000', item6: 0ad6dfb6-7a6e-11e4-bc39-b4b6763e9d6f, item7: bdf5e8ac-a75e-4321-9ac8-938fc9576c4a, item8: 'bleh', item9: -9223372036854775808, item10: 1234.45678, item11: 9.8712e+07, item12: 9.8712e+07, item13: -2147483648, item14: 2147483647, item15: False, item16: [1, 3, 5, 7, 11, 13]}
             row2 | {item1: 'heyimascii', item2: 0x0011, item3: '127.0.0.1', item4: 'whatev', item5: '2011-02-03 04:05:00.000000+0000', item6: 0ad6dfb6-7a6e-11e4-bc39-b4b6763e9d6f, item7: bdf5e8ac-a75e-4321-9ac8-938fc9576c4a, item8: 'bleh', item9: -9223372036854775808, item10: 1234.45678, item11: 9.8712e+07, item12: 9.8712e+07, item13: -2147483648, item14: 2147483647, item15: False, item16: [1, 3, 5, 7, 11, 13]}

            (2 rows)

        """)
        output = print_cqlsh(
            node, "SELECT key1, mytodolists from json_test.complex_types where key1 in ('row1', 'row2')")
        assert output == dedent("""
             key1 | mytodolists
            ------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
             row1 | [{name: 'stuff to do!', todo_list: [{label: 'buy groceries', details: 'bread and milk'}, {label: 'pick up car from shop', details: '$325 due'}, {label: 'call dave', details: 'for some reason'}]}, {name: 'more stuff to do!', todo_list: [{label: 'buy new car', details: 'the old one is getting expensive'}, {label: 'price insurance', details: 'current cost is $95/mo'}]}]
             row2 | [{name: 'stuff to do!', todo_list: [{label: 'buy groceries', details: 'bread and milk'}, {label: 'pick up car from shop', details: '$325 due'}, {label: 'call dave', details: 'for some reason'}]}, {name: 'more stuff to do!', todo_list: [{label: 'buy new car', details: 'the old one is getting expensive'}, {label: 'price insurance', details: 'current cost is $95/mo'}]}]

            (2 rows)

        """)
        output = print_cqlsh(
            node, "SELECT key1, many_sinks from json_test.complex_types where key1 in ('row1', 'row2')")
        assert output == dedent("""
             key1 | many_sinks
            ------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
             row1 | [{item1: 'asdf', item2: 0x0012, item3: '127.0.0.2', item4: 'whatev1', item5: '2012-02-03 04:05:00.000000+0000', item6: d05a10c8-7c12-11e4-949d-b4b6763e9d6f, item7: f90b04b1-f9ad-4ffa-b869-a7d894ce6003, item8: 'tyru', item9: -9223372036854771111, item10: 4321.45678, item11: 1.0012e+07, item12: 4.0012e+07, item13: -1147483648, item14: 2047483648, item15: True, item16: [1, 1, 2, 3, 5, 8]}, {item1: 'fdsa', item2: 0x0013, item3: '127.0.0.3', item4: 'whatev2', item5: '2013-02-03 04:05:00.000000+0000', item6: d8ac38c8-7c12-11e4-8955-b4b6763e9d6f, item7: e3e84f21-f28c-4e0f-80e0-068a640ae53a, item8: 'uytr', item9: -3333372036854775808, item10: 1234.12321, item11: 2.0012e+07, item12: 5.0012e+07, item13: -1547483648, item14: 1947483648, item15: False, item16: [3, 6, 9, 12, 15]}, {item1: 'zxcv', item2: 0x0014, item3: '127.0.0.4', item4: 'whatev3', item5: '2014-02-03 04:05:00.000000+0000', item6: de30838a-7c12-11e4-a907-b4b6763e9d6f, item7: f9381f0e-9467-4d4c-9315-eb9f0232487b, item8: 'fghj', item9: -2239372036854775808, item10: 5555.55555, item11: 3.0012e+07, item12: 6.0012e+07, item13: 2147483647, item14: 1347483648, item15: True, item16: [0, 1, 0, 1, 2, 0]}]
             row2 | [{item1: 'asdf', item2: 0x0012, item3: '127.0.0.2', item4: 'whatev1', item5: '2012-02-03 04:05:00.000000+0000', item6: d05a10c8-7c12-11e4-949d-b4b6763e9d6f, item7: f90b04b1-f9ad-4ffa-b869-a7d894ce6003, item8: 'tyru', item9: -9223372036854771111, item10: 4321.45678, item11: 1.0012e+07, item12: 4.0012e+07, item13: -1147483648, item14: 2047483648, item15: True, item16: [1, 1, 2, 3, 5, 8]}, {item1: 'fdsa', item2: 0x0013, item3: '127.0.0.3', item4: 'whatev2', item5: '2013-02-03 04:05:00.000000+0000', item6: d8ac38c8-7c12-11e4-8955-b4b6763e9d6f, item7: e3e84f21-f28c-4e0f-80e0-068a640ae53a, item8: 'uytr', item9: -3333372036854775808, item10: 1234.12321, item11: 2.0012e+07, item12: 5.0012e+07, item13: -1547483648, item14: 1947483648, item15: False, item16: [3, 6, 9, 12, 15]}, {item1: 'zxcv', item2: 0x0014, item3: '127.0.0.4', item4: 'whatev3', item5: '2014-02-03 04:05:00.000000+0000', item6: de30838a-7c12-11e4-a907-b4b6763e9d6f, item7: f9381f0e-9467-4d4c-9315-eb9f0232487b, item8: 'fghj', item9: -2239372036854775808, item10: 5555.55555, item11: 3.0012e+07, item12: 6.0012e+07, item13: 2147483647, item14: 1347483648, item15: True, item16: [0, 1, 0, 1, 2, 0]}]

            (2 rows)

        """)
        output = print_cqlsh(
            node, "SELECT key1, named_sinks from json_test.complex_types where key1 in ('row1', 'row2')")
        assert output == dedent("""
             key1 | named_sinks
            ------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
             row1 | {'namedsink1': {item1: 'asdf', item2: 0x0012, item3: '127.0.0.2', item4: 'whatev1', item5: '2012-02-03 04:05:00.000000+0000', item6: d05a10c8-7c12-11e4-949d-b4b6763e9d6f, item7: f90b04b1-f9ad-4ffa-b869-a7d894ce6003, item8: 'tyru', item9: -9223372036854771111, item10: 4321.45678, item11: 1.0012e+07, item12: 4.0012e+07, item13: -1147483648, item14: 2047483648, item15: True, item16: [1, 1, 2, 3, 5, 8]}, 'namedsink2': {item1: 'fdsa', item2: 0x0013, item3: '127.0.0.3', item4: 'whatev2', item5: '2013-02-03 04:05:00.000000+0000', item6: d8ac38c8-7c12-11e4-8955-b4b6763e9d6f, item7: e3e84f21-f28c-4e0f-80e0-068a640ae53a, item8: 'uytr', item9: -3333372036854775808, item10: 1234.12321, item11: 2.0012e+07, item12: 5.0012e+07, item13: -1547483648, item14: 1947483648, item15: False, item16: [3, 6, 9, 12, 15]}, 'namedsink3': {item1: 'zxcv', item2: 0x0014, item3: '127.0.0.4', item4: 'whatev3', item5: '2014-02-03 04:05:00.000000+0000', item6: de30838a-7c12-11e4-a907-b4b6763e9d6f, item7: f9381f0e-9467-4d4c-9315-eb9f0232487b, item8: 'fghj', item9: -2239372036854775808, item10: 5555.55555, item11: 3.0012e+07, item12: 6.0012e+07, item13: 2147483647, item14: 1347483648, item15: True, item16: [0, 1, 0, 1, 2, 0]}}
             row2 | {'namedsink1': {item1: 'asdf', item2: 0x0012, item3: '127.0.0.2', item4: 'whatev1', item5: '2012-02-03 04:05:00.000000+0000', item6: d05a10c8-7c12-11e4-949d-b4b6763e9d6f, item7: f90b04b1-f9ad-4ffa-b869-a7d894ce6003, item8: 'tyru', item9: -9223372036854771111, item10: 4321.45678, item11: 1.0012e+07, item12: 4.0012e+07, item13: -1147483648, item14: 2047483648, item15: True, item16: [1, 1, 2, 3, 5, 8]}, 'namedsink2': {item1: 'fdsa', item2: 0x0013, item3: '127.0.0.3', item4: 'whatev2', item5: '2013-02-03 04:05:00.000000+0000', item6: d8ac38c8-7c12-11e4-8955-b4b6763e9d6f, item7: e3e84f21-f28c-4e0f-80e0-068a640ae53a, item8: 'uytr', item9: -3333372036854775808, item10: 1234.12321, item11: 2.0012e+07, item12: 5.0012e+07, item13: -1547483648, item14: 1947483648, item15: False, item16: [3, 6, 9, 12, 15]}, 'namedsink3': {item1: 'zxcv', item2: 0x0014, item3: '127.0.0.4', item4: 'whatev3', item5: '2014-02-03 04:05:00.000000+0000', item6: de30838a-7c12-11e4-a907-b4b6763e9d6f, item7: f9381f0e-9467-4d4c-9315-eb9f0232487b, item8: 'fghj', item9: -2239372036854775808, item10: 5555.55555, item11: 3.0012e+07, item12: 6.0012e+07, item13: 2147483647, item14: 1347483648, item15: True, item16: [0, 1, 0, 1, 2, 0]}}

            (2 rows)

        """)


@pytest.mark.dtest_full
@pytest.mark.single_node
class TestsFromJsonDelete(JsonTester):
    """
    Tests using fromJson within DELETE statements.
    """

    def test_delete_using_pkey_json(self):
        node = self.cluster.nodelist()[0]

        LOGGER.info("Schema setup:")

        node.run_cqlsh('''
         CREATE TYPE json_test.t_person_name (
           first text,
           middle text,
           last text)
        ''')

        node.run_cqlsh('''
             CREATE TABLE json_test.person_info (
               name frozen<t_person_name> PRIMARY KEY,
               info text)
        ''')

        LOGGER.info("Add a row:")

        node.run_cqlsh(
            "INSERT INTO json_test.person_info (name, info) VALUES ({first: 'test', middle: 'guy', last: 'jones'}, 'enjoys bacon')")

        LOGGER.info("Make sure the row is there:")

        output = print_cqlsh(node, '''
             SELECT * FROM json_test.person_info WHERE name = fromJson('{"first":"test", "middle":"guy", "last":"jones"}')
        ''')
        assert output == dedent("""
             name                                          | info
            -----------------------------------------------+--------------
             {first: 'test', middle: 'guy', last: 'jones'} | enjoys bacon

            (1 rows)

        """)

        LOGGER.info("Delete the row using a fromJson clause:")

        node.run_cqlsh('''
             DELETE FROM json_test.person_info WHERE name = fromJson('{"first":"test", "middle":"guy", "last":"jones"}')
        ''')

        LOGGER.info("Make sure the row is gone:")

        output = print_cqlsh(node, "SELECT COUNT(*) from json_test.person_info")
        assert output == dedent("""
             count
            -------
                 0

            (1 rows)

        """)


@pytest.mark.dtest_full
@pytest.mark.single_node
class TestsJsonFullRowInsertSelect(JsonTester):
    """
    Tests for creating full rows from json documents, selecting full rows back as json documents, and related functionality.
    """

    def test_simple_schema(self):
        node = self.cluster.nodelist()[0]

        LOGGER.info("Create schema:")

        node.run_cqlsh('''
             CREATE TABLE json_test.primitive_type_test (
               key1 text PRIMARY KEY,
               col1 ascii,
               col2 blob,
               col3 inet,
               col4 text,
               col5 timestamp,
               col6 timeuuid,
               col7 uuid,
               col8 varchar,
               col9 bigint,
               col10 decimal,
               col11 double,
               col12 float,
               col13 int,
               col14 varint,
               col15 boolean,
               col16 time,
               col17 date,
               col18 tinyint)
        ''')

        LOGGER.info("Use a plain insert to update one row, and a JSON insert to update the other:")

        node.run_cqlsh('''
             INSERT INTO json_test.primitive_type_test (key1, col1, col2, col3, col4, col5, col6, col7, col8, col9, col10, col11, col12, col13, col14, col15, col16, col17, col18)
               VALUES ('foo', 'bar', 0x0011, '127.0.0.1', 'blarg', '2011-02-03 04:05+0000', 0ad6dfb6-7a6e-11e4-bc39-b4b6763e9d6f, bdf5e8ac-a75e-4321-9ac8-938fc9576c4a, 'bleh', -9223372036854775808, 1234.45678, 98712312.1222, 98712312.5252, -2147483648, 2147483648, true, '13:07:45.089', '2017-11-25', 123)
        ''')

        node.run_cqlsh('''
             INSERT INTO json_test.primitive_type_test JSON '{"key1": "bar", "col1": "bar", "col2": "0x0011", "col3": "127.0.0.1", "col4": "blarg", "col5": "2011-02-02T21:05:00.000+0000", "col6": "0ad6dfb6-7a6e-11e4-bc39-b4b6763e9d6f", "col7": "bdf5e8ac-a75e-4321-9ac8-938fc9576c4a", "col8": "bleh", "col9": "-9223372036854775808", "col10": "1234.45678", "col11":"9.87123121222E7", "col12": "9.87123121222E7", "col13": "-2147483648", "col14": "2147483648", "col15": true, "col16": "13:07:45.089", "col17":"2017-11-25", "col18": "123"}'
        ''')

        LOGGER.info("Query back both rows as JSON:")

        output = print_cqlsh(node, "SELECT JSON * FROM json_test.primitive_type_test")
        # supporting two representation types for double and floats
        # see: scylladb/seastar@4f4e84b and scylladb/scylladb#12509 for more details
        assert output == dedent("""
             [json]
            ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
             {"key1": "bar", "col1": "bar", "col10": 1234.45678, "col11": 9.87123e+07, "col12": 9.87123e+07, "col13": -2147483648, "col14": 2147483648, "col15": true, "col16": 13:07:45.089000000, "col17": "2017-11-25", "col18": 123, "col2": "0x0011", "col3": "127.0.0.1", "col4": "blarg", "col5": "2011-02-02T21:05:00", "col6": "0ad6dfb6-7a6e-11e4-bc39-b4b6763e9d6f", "col7": "bdf5e8ac-a75e-4321-9ac8-938fc9576c4a", "col8": "bleh", "col9": -9223372036854775808}
             {"key1": "foo", "col1": "bar", "col10": 1234.45678, "col11": 9.87123e+07, "col12": 9.87123e+07, "col13": -2147483648, "col14": 2147483648, "col15": true, "col16": 13:07:45.089000000, "col17": "2017-11-25", "col18": 123, "col2": "0x0011", "col3": "127.0.0.1", "col4": "blarg", "col5": "2011-02-03T04:05:00", "col6": "0ad6dfb6-7a6e-11e4-bc39-b4b6763e9d6f", "col7": "bdf5e8ac-a75e-4321-9ac8-938fc9576c4a", "col8": "bleh", "col9": -9223372036854775808}

            (2 rows)

            """) or \
            output == dedent("""
             [json]
            -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
             {"key1": "bar", "col1": "bar", "col10": 1234.45678, "col11": 98712312.1222, "col12": 98712310, "col13": -2147483648, "col14": 2147483648, "col15": true, "col16": 13:07:45.089000000, "col17": "2017-11-25", "col18": 123, "col2": "0x0011", "col3": "127.0.0.1", "col4": "blarg", "col5": "2011-02-02T21:05:00", "col6": "0ad6dfb6-7a6e-11e4-bc39-b4b6763e9d6f", "col7": "bdf5e8ac-a75e-4321-9ac8-938fc9576c4a", "col8": "bleh", "col9": -9223372036854775808}
             {"key1": "foo", "col1": "bar", "col10": 1234.45678, "col11": 98712312.1222, "col12": 98712310, "col13": -2147483648, "col14": 2147483648, "col15": true, "col16": 13:07:45.089000000, "col17": "2017-11-25", "col18": 123, "col2": "0x0011", "col3": "127.0.0.1", "col4": "blarg", "col5": "2011-02-03T04:05:00", "col6": "0ad6dfb6-7a6e-11e4-bc39-b4b6763e9d6f", "col7": "bdf5e8ac-a75e-4321-9ac8-938fc9576c4a", "col8": "bleh", "col9": -9223372036854775808}

            (2 rows)

            """)

        LOGGER.info("Query back both rows, but with only some JSON fields:")

        output = print_cqlsh(
            node, "SELECT JSON col15, col1, col3, col13, col11, col2, col4 FROM json_test.primitive_type_test WHERE key1 in ('foo', 'bar')")
        # supporting two representation types for double and floats
        # see: scylladb/seastar@4f4e84b and scylladb/scylladb#12509 for more details
        assert output == dedent("""
             [json]
            ------------------------------------------------------------------------------------------------------------------------------------
             {"col15": true, "col1": "bar", "col3": "127.0.0.1", "col13": -2147483648, "col11": 9.87123e+07, "col2": "0x0011", "col4": "blarg"}
             {"col15": true, "col1": "bar", "col3": "127.0.0.1", "col13": -2147483648, "col11": 9.87123e+07, "col2": "0x0011", "col4": "blarg"}

            (2 rows)

        """) or \
            output == dedent("""
             [json]
            --------------------------------------------------------------------------------------------------------------------------------------
             {"col15": true, "col1": "bar", "col3": "127.0.0.1", "col13": -2147483648, "col11": 98712312.1222, "col2": "0x0011", "col4": "blarg"}
             {"col15": true, "col1": "bar", "col3": "127.0.0.1", "col13": -2147483648, "col11": 98712312.1222, "col2": "0x0011", "col4": "blarg"}

            (2 rows)

        """)
        LOGGER.info("Query rows normally and make sure they look ok there too:")

        output = print_cqlsh(node, "SELECT * FROM json_test.primitive_type_test")
        assert output == dedent("""
             key1 | col1 | col10      | col11      | col12      | col13       | col14      | col15 | col16              | col17      | col18 | col2   | col3      | col4  | col5                            | col6                                 | col7                                 | col8 | col9
            ------+------+------------+------------+------------+-------------+------------+-------+--------------------+------------+-------+--------+-----------+-------+---------------------------------+--------------------------------------+--------------------------------------+------+----------------------
              bar |  bar | 1234.45678 | 9.8712e+07 | 9.8712e+07 | -2147483648 | 2147483648 |  True | 13:07:45.089000000 | 2017-11-25 |   123 | 0x0011 | 127.0.0.1 | blarg | 2011-02-02 21:05:00.000000+0000 | 0ad6dfb6-7a6e-11e4-bc39-b4b6763e9d6f | bdf5e8ac-a75e-4321-9ac8-938fc9576c4a | bleh | -9223372036854775808
              foo |  bar | 1234.45678 | 9.8712e+07 | 9.8712e+07 | -2147483648 | 2147483648 |  True | 13:07:45.089000000 | 2017-11-25 |   123 | 0x0011 | 127.0.0.1 | blarg | 2011-02-03 04:05:00.000000+0000 | 0ad6dfb6-7a6e-11e4-bc39-b4b6763e9d6f | bdf5e8ac-a75e-4321-9ac8-938fc9576c4a | bleh | -9223372036854775808

            (2 rows)

        """)

    # Issue #4015: Insert using JSON: not clear message when primary key omitted from the column list and omitted from the JSON data
    def test_pkey_requirement(self):
        node = self.cluster.nodelist()[0]

        LOGGER.info("Create schema:")

        node.run_cqlsh('''
             CREATE TABLE json_test.primitive_type_test (
               key1 text PRIMARY KEY,
               col1 ascii,
               col2 blob,
               col3 inet,
               col4 text,
               col5 timestamp,
               col6 timeuuid,
               col7 uuid,
               col8 varchar,
               col9 bigint,
               col10 decimal,
               col11 double,
               col12 float,
               col13 int,
               col14 varint,
               col15 boolean)
        ''')

        LOGGER.info("Try to create a JSON row with the pkey omitted from the column list, and omitted from the JSON data:")

        output = print_cqlsh(node, '''INSERT INTO json_test.primitive_type_test JSON '{"col1": "bar"}' ''')
        assert output == dedent('''
            <stdin>:2:InvalidRequest: Error from server: code=2200 [Invalid query] message="Missing mandatory PRIMARY KEY part key1"
        ''')

    def test_null_value(self):
        node = self.cluster.nodelist()[0]

        LOGGER.info("Create schema:")

        node.run_cqlsh('''
             CREATE TABLE json_test.primitive_type_test (
               key1 text PRIMARY KEY,
               col1 ascii,
               col2 blob,
               col3 inet,
               col4 text,
               col5 timestamp,
               col6 timeuuid,
               col7 uuid,
               col8 varchar,
               col9 bigint,
               col10 decimal,
               col11 double,
               col12 float,
               col13 int,
               col14 varint,
               col15 boolean)
        ''')

        LOGGER.info(
            "Insert a row where all columns are specified in the column list, but none of the non-pkey items are provided in the JSON data:")

        node.run_cqlsh('''
             INSERT INTO json_test.primitive_type_test JSON '{"key1": "foo"}'
        ''')

        LOGGER.info("Confirm columns provided in column list but not specified are null:")

        output = print_cqlsh(node, "SELECT * FROM json_test.primitive_type_test WHERE key1 = 'foo'")
        assert output == dedent("""
             key1 | col1 | col10 | col11 | col12 | col13 | col14 | col15 | col2 | col3 | col4 | col5 | col6 | col7 | col8 | col9
            ------+------+-------+-------+-------+-------+-------+-------+------+------+------+------+------+------+------+------
              foo | null |  null |  null |  null |  null |  null |  null | null | null | null | null | null | null | null | null

            (1 rows)

        """)

    def test_complex_schema(self):
        node = self.cluster.nodelist()[0]

        LOGGER.info("Create some udt's and schema:")

        node.run_cqlsh('''
             CREATE TYPE json_test.t_todo_item (
               label text,
               details text )
        ''')

        node.run_cqlsh('''
             CREATE TYPE json_test.t_todo_list (
               name text,
               todo_list list<frozen<t_todo_item>> )
        ''')

        node.run_cqlsh('''
             CREATE TYPE json_test.t_kitchen_sink (
               item1 ascii,
               item2 blob,
               item3 inet,
               item4 text,
               item5 timestamp,
               item6 timeuuid,
               item7 uuid,
               item8 varchar,
               item9 bigint,
               item10 decimal,
               item11 double,
               item12 float,
               item13 int,
               item14 varint,
               item15 boolean,
               item16 list<int> )
        ''')

        node.run_cqlsh('''
             CREATE TABLE json_test.complex_types (
               key1 text PRIMARY KEY,
               mylist list<text>,
               myset set<uuid>,
               mymap map<text, int>,
               mytuple frozen<tuple<text, int, uuid, boolean>>,
               myudt frozen<t_kitchen_sink>,
               mytodolists list<frozen<t_todo_list>>,
               many_sinks list<frozen<t_kitchen_sink>>,
               named_sinks map<text, frozen<t_kitchen_sink>> )
        ''')

        LOGGER.info("Add two rows with all null values, create the first row using a regular INSERT statement, and the second row using JSON. Different key for each row:")

        node.run_cqlsh('''
             INSERT INTO json_test.complex_types (key1) values ('row1')
        ''')
        node.run_cqlsh('''
             INSERT INTO json_test.complex_types JSON '{"key1":"row2"}'
        ''')

        LOGGER.info("Query back both rows as JSON:")

        output = print_cqlsh(node, "SELECT JSON * FROM json_test.complex_types")
        assert output == dedent("""
             [json]
            --------------------------------------------------------------------------------------------------------------------------------------------------------------
             {"key1": "row1", "many_sinks": null, "mylist": null, "mymap": null, "myset": null, "mytodolists": null, "mytuple": null, "myudt": null, "named_sinks": null}
             {"key1": "row2", "many_sinks": null, "mylist": null, "mymap": null, "myset": null, "mytodolists": null, "mytuple": null, "myudt": null, "named_sinks": null}

            (2 rows)

        """)
        LOGGER.info("Query back both rows as non-JSON to be sure they look ok there too:")

        output = print_cqlsh(node, '''
             SELECT * FROM json_test.complex_types
        ''')
        assert output == dedent("""
             key1 | many_sinks | mylist | mymap | myset | mytodolists | mytuple | myudt | named_sinks
            ------+------------+--------+-------+-------+-------------+---------+-------+-------------
             row1 |       null |   null |  null |  null |        null |    null |  null |        null
             row2 |       null |   null |  null |  null |        null |    null |  null |        null

            (2 rows)

        """)
        LOGGER.info('Add data for "row1" using a normal insert statement to update the record:')

        node.run_cqlsh('''
             INSERT INTO json_test.complex_types (key1, mylist, myset, mymap, mytuple, myudt, mytodolists, many_sinks, named_sinks)
             VALUES (
               'row1',
               ['five', 'six', 'seven', 'eight'],
               {4b66458a-2a19-41d3-af25-6faef4dea9fe, 080fdd90-ae74-41d6-9883-635625d3b069, 6cd7fab5-eacc-45c3-8414-6ad0177651d6},
               {'one' : 1, 'two' : 2, 'three': 3, 'four': 4},
               ('hey', 10, 16e69fba-a656-4932-8a01-6782a34505d9, true),
               {item1: 'heyimascii', item2: 0x0011, item3: '127.0.0.1', item4: 'whatev', item5: '2011-02-03 04:05+0000', item6: 0ad6dfb6-7a6e-11e4-bc39-b4b6763e9d6f, item7: bdf5e8ac-a75e-4321-9ac8-938fc9576c4a, item8: 'bleh', item9: -9223372036854775808, item10: 1234.45678, item11: 98712312.1222, item12: 98712312.5252, item13: -2147483648, item14: 2147483647, item15: false, item16: [1,3,5,7,11,13]},
               [{name: 'stuff to do!', todo_list: [{label: 'buy groceries', details: 'bread and milk'}, {label: 'pick up car from shop', details: '$325 due'}, {label: 'call dave', details: 'for some reason'}]}, {name: 'more stuff to do!', todo_list:[{label: 'buy new car', details: 'the old one is getting expensive'}, {label: 'price insurance', details: 'current cost is $95/mo'}]}],
               [{item1: 'asdf', item2: 0x0012, item3: '127.0.0.2', item4: 'whatev1', item5: '2012-02-03 04:05+0000', item6: d05a10c8-7c12-11e4-949d-b4b6763e9d6f, item7: f90b04b1-f9ad-4ffa-b869-a7d894ce6003, item8: 'tyru', item9: -9223372036854771111, item10: 4321.45678, item11: 10012312.1222, item12: 40012312.5252, item13: -1147483648, item14: 2047483648, item15: true, item16: [1,1,2,3,5,8]}, {item1: 'fdsa', item2: 0x0013, item3: '127.0.0.3', item4: 'whatev2', item5: '2013-02-03 04:05+0000', item6: d8ac38c8-7c12-11e4-8955-b4b6763e9d6f, item7: e3e84f21-f28c-4e0f-80e0-068a640ae53a, item8: 'uytr', item9: -3333372036854775808, item10: 1234.12321, item11: 20012312.1222, item12: 50012312.5252, item13: -1547483648, item14: 1947483648, item15: false, item16: [3,6,9,12,15]},{item1: 'zxcv', item2: 0x0014, item3: '127.0.0.4', item4: 'whatev3', item5: '2014-02-03 04:05+0000', item6: de30838a-7c12-11e4-a907-b4b6763e9d6f, item7: f9381f0e-9467-4d4c-9315-eb9f0232487b, item8: 'fghj', item9: -2239372036854775808, item10: 5555.55555, item11: 30012312.1222, item12: 60012312.5252, item13: 2147483647, item14: 1347483648, item15: true, item16: [0,1,0,1,2,0]}],
               {'namedsink1':{item1: 'asdf', item2: 0x0012, item3: '127.0.0.2', item4: 'whatev1', item5: '2012-02-03 04:05+0000', item6: d05a10c8-7c12-11e4-949d-b4b6763e9d6f, item7: f90b04b1-f9ad-4ffa-b869-a7d894ce6003, item8: 'tyru', item9: -9223372036854771111, item10: 4321.45678, item11: 10012312.1222, item12: 40012312.5252, item13: -1147483648, item14: 2047483648, item15: true, item16: [1,1,2,3,5,8]},'namedsink2':{item1: 'fdsa', item2: 0x0013, item3: '127.0.0.3', item4: 'whatev2', item5: '2013-02-03 04:05+0000', item6: d8ac38c8-7c12-11e4-8955-b4b6763e9d6f, item7: e3e84f21-f28c-4e0f-80e0-068a640ae53a, item8: 'uytr', item9: -3333372036854775808, item10: 1234.12321, item11: 20012312.1222, item12: 50012312.5252, item13: -1547483648, item14: 1947483648, item15: false, item16: [3,6,9,12,15]},'namedsink3':{item1: 'zxcv', item2: 0x0014, item3: '127.0.0.4', item4: 'whatev3', item5: '2014-02-03 04:05+0000', item6: de30838a-7c12-11e4-a907-b4b6763e9d6f, item7: f9381f0e-9467-4d4c-9315-eb9f0232487b, item8: 'fghj', item9: -2239372036854775808, item10: 5555.55555, item11: 30012312.1222, item12: 60012312.5252, item13: 2147483647, item14: 1347483648, item15: true, item16: [0,1,0,1,2,0]}})
        ''')

        LOGGER.info('Add data for "row2" using JSON, but which should be equivalent to "row1" after insert:')

        node.run_cqlsh('''
             INSERT INTO json_test.complex_types
             JSON '{
               "key1":"row2",
               "mylist":["five", "six", "seven", "eight"],
               "myset":["4b66458a-2a19-41d3-af25-6faef4dea9fe", "080fdd90-ae74-41d6-9883-635625d3b069", "6cd7fab5-eacc-45c3-8414-6ad0177651d6"],
               "mymap":{"one" : 1, "two" : 2, "three": 3, "four": 4},
               "mytuple":["hey", 10, "16e69fba-a656-4932-8a01-6782a34505d9", true],
               "myudt":{"item1": "heyimascii", "item2": "0x0011", "item3": "127.0.0.1", "item4": "whatev", "item5": "2011-02-03 04:05+0000", "item6": "0ad6dfb6-7a6e-11e4-bc39-b4b6763e9d6f", "item7": "bdf5e8ac-a75e-4321-9ac8-938fc9576c4a", "item8": "bleh", "item9": -9223372036854775808, "item10": 1234.45678, "item11": 98712312.1222, "item12": 98712312.5252, "item13": -2147483648, "item14": 2147483647, "item15": false, "item16": [1,3,5,7,11,13]},
               "mytodolists":[{"name": "stuff to do!", "todo_list": [{"label": "buy groceries", "details": "bread and milk"}, {"label": "pick up car from shop", "details": "$325 due"}, {"label": "call dave", "details": "for some reason"}]}, {"name": "more stuff to do!", "todo_list":[{"label": "buy new car", "details": "the old one is getting expensive"}, {"label": "price insurance", "details": "current cost is $95/mo"}]}],
               "many_sinks":[{"item1": "asdf", "item2": "0x0012", "item3": "127.0.0.2", "item4": "whatev1", "item5": "2012-02-03 04:05+0000", "item6": "d05a10c8-7c12-11e4-949d-b4b6763e9d6f", "item7": "f90b04b1-f9ad-4ffa-b869-a7d894ce6003", "item8": "tyru", "item9": -9223372036854771111, "item10": 4321.45678, "item11": 10012312.1222, "item12": 40012312.5252, "item13": -1147483648, "item14": 2047483648, "item15": true, "item16": [1,1,2,3,5,8]}, {"item1": "fdsa", "item2": "0x0013", "item3": "127.0.0.3", "item4": "whatev2", "item5": "2013-02-03 04:05+0000", "item6": "d8ac38c8-7c12-11e4-8955-b4b6763e9d6f", "item7": "e3e84f21-f28c-4e0f-80e0-068a640ae53a", "item8": "uytr", "item9": -3333372036854775808, "item10": 1234.12321, "item11": 20012312.1222, "item12": 50012312.5252, "item13": -1547483648, "item14": 1947483648, "item15": false, "item16": [3,6,9,12,15]},{"item1": "zxcv", "item2": "0x0014", "item3": "127.0.0.4", "item4": "whatev3", "item5": "2014-02-03 04:05+0000", "item6": "de30838a-7c12-11e4-a907-b4b6763e9d6f", "item7": "f9381f0e-9467-4d4c-9315-eb9f0232487b", "item8": "fghj", "item9": -2239372036854775808, "item10": 5555.55555, "item11": 30012312.1222, "item12": 60012312.5252, "item13": 2147483647, "item14": 1347483648, "item15": true, "item16": [0,1,0,1,2,0]}],
               "named_sinks":{"namedsink1":{"item1": "asdf", "item2": "0x0012", "item3": "127.0.0.2", "item4": "whatev1", "item5": "2012-02-03 04:05+0000", "item6": "d05a10c8-7c12-11e4-949d-b4b6763e9d6f", "item7": "f90b04b1-f9ad-4ffa-b869-a7d894ce6003", "item8": "tyru", "item9": -9223372036854771111, "item10": 4321.45678, "item11": 10012312.1222, "item12": 40012312.5252, "item13": -1147483648, "item14": 2047483648, "item15": true, "item16": [1,1,2,3,5,8]},"namedsink2":{"item1": "fdsa", "item2": "0x0013", "item3": "127.0.0.3", "item4": "whatev2", "item5": "2013-02-03 04:05+0000", "item6": "d8ac38c8-7c12-11e4-8955-b4b6763e9d6f", "item7": "e3e84f21-f28c-4e0f-80e0-068a640ae53a", "item8": "uytr", "item9": -3333372036854775808, "item10": 1234.12321, "item11": 20012312.1222, "item12": 50012312.5252, "item13": -1547483648, "item14": 1947483648, "item15": false, "item16": [3,6,9,12,15]},"namedsink3":{"item1": "zxcv", "item2": "0x0014", "item3": "127.0.0.4", "item4": "whatev3", "item5": "2014-02-03 04:05+0000", "item6": "de30838a-7c12-11e4-a907-b4b6763e9d6f", "item7": "f9381f0e-9467-4d4c-9315-eb9f0232487b", "item8": "fghj", "item9": -2239372036854775808, "item10": 5555.55555, "item11": 30012312.1222, "item12": 60012312.5252, "item13": 2147483647, "item14": 1347483648, "item15": true, "item16": [0,1,0,1,2,0]}}
               }'
             ''')

        LOGGER.info("Query both rows back, one field at a time (for easier reading) and make sure they match:")

        output = print_cqlsh(node, "SELECT key1, mylist from json_test.complex_types")
        assert output == dedent("""
             key1 | mylist
            ------+-----------------------------------
             row1 | ['five', 'six', 'seven', 'eight']
             row2 | ['five', 'six', 'seven', 'eight']

            (2 rows)

        """)
        output = print_cqlsh(node, "SELECT key1, myset from json_test.complex_types")
        assert output == dedent("""
             key1 | myset
            ------+--------------------------------------------------------------------------------------------------------------------
             row1 | {080fdd90-ae74-41d6-9883-635625d3b069, 4b66458a-2a19-41d3-af25-6faef4dea9fe, 6cd7fab5-eacc-45c3-8414-6ad0177651d6}
             row2 | {080fdd90-ae74-41d6-9883-635625d3b069, 4b66458a-2a19-41d3-af25-6faef4dea9fe, 6cd7fab5-eacc-45c3-8414-6ad0177651d6}

            (2 rows)

        """)
        output = print_cqlsh(node, "SELECT key1, mymap from json_test.complex_types")
        assert output == dedent("""
             key1 | mymap
            ------+---------------------------------------------
             row1 | {'four': 4, 'one': 1, 'three': 3, 'two': 2}
             row2 | {'four': 4, 'one': 1, 'three': 3, 'two': 2}

            (2 rows)

        """)
        output = print_cqlsh(node, "SELECT key1, mytuple from json_test.complex_types")
        assert output == dedent("""
             key1 | mytuple
            ------+---------------------------------------------------------
             row1 | ('hey', 10, 16e69fba-a656-4932-8a01-6782a34505d9, True)
             row2 | ('hey', 10, 16e69fba-a656-4932-8a01-6782a34505d9, True)

            (2 rows)

        """)
        output = print_cqlsh(node, "SELECT key1, myudt from json_test.complex_types")
        assert output == dedent("""
             key1 | myudt
            ------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
             row1 | {item1: 'heyimascii', item2: 0x0011, item3: '127.0.0.1', item4: 'whatev', item5: '2011-02-03 04:05:00.000000+0000', item6: 0ad6dfb6-7a6e-11e4-bc39-b4b6763e9d6f, item7: bdf5e8ac-a75e-4321-9ac8-938fc9576c4a, item8: 'bleh', item9: -9223372036854775808, item10: 1234.45678, item11: 9.8712e+07, item12: 9.8712e+07, item13: -2147483648, item14: 2147483647, item15: False, item16: [1, 3, 5, 7, 11, 13]}
             row2 | {item1: 'heyimascii', item2: 0x0011, item3: '127.0.0.1', item4: 'whatev', item5: '2011-02-03 04:05:00.000000+0000', item6: 0ad6dfb6-7a6e-11e4-bc39-b4b6763e9d6f, item7: bdf5e8ac-a75e-4321-9ac8-938fc9576c4a, item8: 'bleh', item9: -9223372036854775808, item10: 1234.45678, item11: 9.8712e+07, item12: 9.8712e+07, item13: -2147483648, item14: 2147483647, item15: False, item16: [1, 3, 5, 7, 11, 13]}

            (2 rows)

        """)
        output = print_cqlsh(node, "SELECT key1, mytodolists from json_test.complex_types")
        assert output == dedent("""
             key1 | mytodolists
            ------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
             row1 | [{name: 'stuff to do!', todo_list: [{label: 'buy groceries', details: 'bread and milk'}, {label: 'pick up car from shop', details: '$325 due'}, {label: 'call dave', details: 'for some reason'}]}, {name: 'more stuff to do!', todo_list: [{label: 'buy new car', details: 'the old one is getting expensive'}, {label: 'price insurance', details: 'current cost is $95/mo'}]}]
             row2 | [{name: 'stuff to do!', todo_list: [{label: 'buy groceries', details: 'bread and milk'}, {label: 'pick up car from shop', details: '$325 due'}, {label: 'call dave', details: 'for some reason'}]}, {name: 'more stuff to do!', todo_list: [{label: 'buy new car', details: 'the old one is getting expensive'}, {label: 'price insurance', details: 'current cost is $95/mo'}]}]

            (2 rows)

        """)

        output = print_cqlsh(node, "SELECT key1, many_sinks from json_test.complex_types")
        assert output == dedent("""
             key1 | many_sinks
            ------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
             row1 | [{item1: 'asdf', item2: 0x0012, item3: '127.0.0.2', item4: 'whatev1', item5: '2012-02-03 04:05:00.000000+0000', item6: d05a10c8-7c12-11e4-949d-b4b6763e9d6f, item7: f90b04b1-f9ad-4ffa-b869-a7d894ce6003, item8: 'tyru', item9: -9223372036854771111, item10: 4321.45678, item11: 1.0012e+07, item12: 4.0012e+07, item13: -1147483648, item14: 2047483648, item15: True, item16: [1, 1, 2, 3, 5, 8]}, {item1: 'fdsa', item2: 0x0013, item3: '127.0.0.3', item4: 'whatev2', item5: '2013-02-03 04:05:00.000000+0000', item6: d8ac38c8-7c12-11e4-8955-b4b6763e9d6f, item7: e3e84f21-f28c-4e0f-80e0-068a640ae53a, item8: 'uytr', item9: -3333372036854775808, item10: 1234.12321, item11: 2.0012e+07, item12: 5.0012e+07, item13: -1547483648, item14: 1947483648, item15: False, item16: [3, 6, 9, 12, 15]}, {item1: 'zxcv', item2: 0x0014, item3: '127.0.0.4', item4: 'whatev3', item5: '2014-02-03 04:05:00.000000+0000', item6: de30838a-7c12-11e4-a907-b4b6763e9d6f, item7: f9381f0e-9467-4d4c-9315-eb9f0232487b, item8: 'fghj', item9: -2239372036854775808, item10: 5555.55555, item11: 3.0012e+07, item12: 6.0012e+07, item13: 2147483647, item14: 1347483648, item15: True, item16: [0, 1, 0, 1, 2, 0]}]
             row2 | [{item1: 'asdf', item2: 0x0012, item3: '127.0.0.2', item4: 'whatev1', item5: '2012-02-03 04:05:00.000000+0000', item6: d05a10c8-7c12-11e4-949d-b4b6763e9d6f, item7: f90b04b1-f9ad-4ffa-b869-a7d894ce6003, item8: 'tyru', item9: -9223372036854771111, item10: 4321.45678, item11: 1.0012e+07, item12: 4.0012e+07, item13: -1147483648, item14: 2047483648, item15: True, item16: [1, 1, 2, 3, 5, 8]}, {item1: 'fdsa', item2: 0x0013, item3: '127.0.0.3', item4: 'whatev2', item5: '2013-02-03 04:05:00.000000+0000', item6: d8ac38c8-7c12-11e4-8955-b4b6763e9d6f, item7: e3e84f21-f28c-4e0f-80e0-068a640ae53a, item8: 'uytr', item9: -3333372036854775808, item10: 1234.12321, item11: 2.0012e+07, item12: 5.0012e+07, item13: -1547483648, item14: 1947483648, item15: False, item16: [3, 6, 9, 12, 15]}, {item1: 'zxcv', item2: 0x0014, item3: '127.0.0.4', item4: 'whatev3', item5: '2014-02-03 04:05:00.000000+0000', item6: de30838a-7c12-11e4-a907-b4b6763e9d6f, item7: f9381f0e-9467-4d4c-9315-eb9f0232487b, item8: 'fghj', item9: -2239372036854775808, item10: 5555.55555, item11: 3.0012e+07, item12: 6.0012e+07, item13: 2147483647, item14: 1347483648, item15: True, item16: [0, 1, 0, 1, 2, 0]}]

            (2 rows)

        """)
        output = print_cqlsh(node, "SELECT key1, named_sinks from json_test.complex_types")
        assert output == dedent("""
             key1 | named_sinks
            ------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
             row1 | {'namedsink1': {item1: 'asdf', item2: 0x0012, item3: '127.0.0.2', item4: 'whatev1', item5: '2012-02-03 04:05:00.000000+0000', item6: d05a10c8-7c12-11e4-949d-b4b6763e9d6f, item7: f90b04b1-f9ad-4ffa-b869-a7d894ce6003, item8: 'tyru', item9: -9223372036854771111, item10: 4321.45678, item11: 1.0012e+07, item12: 4.0012e+07, item13: -1147483648, item14: 2047483648, item15: True, item16: [1, 1, 2, 3, 5, 8]}, 'namedsink2': {item1: 'fdsa', item2: 0x0013, item3: '127.0.0.3', item4: 'whatev2', item5: '2013-02-03 04:05:00.000000+0000', item6: d8ac38c8-7c12-11e4-8955-b4b6763e9d6f, item7: e3e84f21-f28c-4e0f-80e0-068a640ae53a, item8: 'uytr', item9: -3333372036854775808, item10: 1234.12321, item11: 2.0012e+07, item12: 5.0012e+07, item13: -1547483648, item14: 1947483648, item15: False, item16: [3, 6, 9, 12, 15]}, 'namedsink3': {item1: 'zxcv', item2: 0x0014, item3: '127.0.0.4', item4: 'whatev3', item5: '2014-02-03 04:05:00.000000+0000', item6: de30838a-7c12-11e4-a907-b4b6763e9d6f, item7: f9381f0e-9467-4d4c-9315-eb9f0232487b, item8: 'fghj', item9: -2239372036854775808, item10: 5555.55555, item11: 3.0012e+07, item12: 6.0012e+07, item13: 2147483647, item14: 1347483648, item15: True, item16: [0, 1, 0, 1, 2, 0]}}
             row2 | {'namedsink1': {item1: 'asdf', item2: 0x0012, item3: '127.0.0.2', item4: 'whatev1', item5: '2012-02-03 04:05:00.000000+0000', item6: d05a10c8-7c12-11e4-949d-b4b6763e9d6f, item7: f90b04b1-f9ad-4ffa-b869-a7d894ce6003, item8: 'tyru', item9: -9223372036854771111, item10: 4321.45678, item11: 1.0012e+07, item12: 4.0012e+07, item13: -1147483648, item14: 2047483648, item15: True, item16: [1, 1, 2, 3, 5, 8]}, 'namedsink2': {item1: 'fdsa', item2: 0x0013, item3: '127.0.0.3', item4: 'whatev2', item5: '2013-02-03 04:05:00.000000+0000', item6: d8ac38c8-7c12-11e4-8955-b4b6763e9d6f, item7: e3e84f21-f28c-4e0f-80e0-068a640ae53a, item8: 'uytr', item9: -3333372036854775808, item10: 1234.12321, item11: 2.0012e+07, item12: 5.0012e+07, item13: -1547483648, item14: 1947483648, item15: False, item16: [3, 6, 9, 12, 15]}, 'namedsink3': {item1: 'zxcv', item2: 0x0014, item3: '127.0.0.4', item4: 'whatev3', item5: '2014-02-03 04:05:00.000000+0000', item6: de30838a-7c12-11e4-a907-b4b6763e9d6f, item7: f9381f0e-9467-4d4c-9315-eb9f0232487b, item8: 'fghj', item9: -2239372036854775808, item10: 5555.55555, item11: 3.0012e+07, item12: 6.0012e+07, item13: 2147483647, item14: 1347483648, item15: True, item16: [0, 1, 0, 1, 2, 0]}}

            (2 rows)

        """)

    def test_mv_insert_json(self):
        node = self.cluster.nodelist()[0]

        LOGGER.info("Create table:")
        node.run_cqlsh('''
             CREATE TABLE json_test.complex_types (
               key1 text PRIMARY KEY,
               mvKey int,
               mytodolists list<text> )
        ''')

        LOGGER.info("Create materialized view:")

        node.run_cqlsh('''
             CREATE MATERIALIZED VIEW json_test.mv_complex_types AS SELECT * FROM json_test.complex_types WHERE key1 is not null and mvKey is not null PRIMARY KEY (key1, mvKey)
        ''')

        LOGGER.info('Add data for "row1" using a normal insert statement to update the record:')

        node.run_cqlsh('''
             INSERT INTO json_test.complex_types (key1, mvKey, mytodolists)
             VALUES (
               'row1',
               12,
               ['five', 'six', 'seven', 'eight']
             )
        ''')

        LOGGER.info('Add data for "row2" using JSON, but which should be equivalent to "row1" after insert:')

        node.run_cqlsh('''
             INSERT INTO json_test.complex_types
             JSON '{
               "key1":"row2",
               "mvKey":258,
               "mytodolists":["five", "six", "seven", "eight"]
               }'
        ''')

        LOGGER.info("Query the table and make sure it match:")

        output = print_cqlsh(node, "SELECT key1, mvKey, mytodolists from json_test.complex_types")
        assert output == dedent("""
             key1 | mvkey | mytodolists
            ------+-------+-----------------------------------
             row1 |    12 | ['five', 'six', 'seven', 'eight']
             row2 |   258 | ['five', 'six', 'seven', 'eight']

            (2 rows)

        """)

        LOGGER.info("Query the MV and make sure it match:")

        output = self.waiting_mv_prefill_finish(node, "SELECT key1, mvKey, mytodolists from json_test.mv_complex_types")
        assert output == dedent("""
             key1 | mvkey | mytodolists
            ------+-------+-----------------------------------
             row1 |    12 | ['five', 'six', 'seven', 'eight']
             row2 |   258 | ['five', 'six', 'seven', 'eight']

            (2 rows)

        """)
