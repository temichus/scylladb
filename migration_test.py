import json
import os
import random
import re
import shutil
import string
import tempfile
import time
import uuid
import subprocess
import glob
import datetime
from unittest import skip

from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement

from assertions import assert_one
from ccmlib.node import NodetoolError

from dtest import Tester, debug
from scylla_tools import CassandraCluster, drop_table, get_sstables_files
from tools import require, rows_to_list, safe_mkdtemp
from nose import tools
from nose.plugins.attrib import attr


@tools.nottest
@attr('dtest-full', 'single_node')
class MigrationTestBase(Tester):

    @attr('dtest-debug')
    def migrate_sstable_without_compression_test(self):
        # Content generated with:
        # INSERT INTO ks.cf (key, c2) VALUES ('abc', 'cde');
        self._run_basic_migration_test("without_compression", {'key': 'abc', 'c1': None, 'c2': 'cde'})

    def migrate_sstable_with_lz4_compression_test(self):
        # Content generated with:
        # INSERT INTO ks.cf (key, c1, c2) VALUES ('a', 'abc', 'cde');
        self._run_basic_migration_test('with_lz4_compression', {
                                       'key': 'a', 'c1': 'abc', 'c2': 'cde'}, compression='LZ4')

    def migrate_sstable_with_compact_storage_test(self):
        # Content generated with:
        # INSERT INTO ks.cf (key, c1, c2) VALUES ('a', 'abc', 'cde');
        self._run_basic_migration_test('with_compact_storage', {
                                       'key': 'a', 'c1': 'abc', 'c2': 'cde'}, compact_storage=True)

    def migrate_sstable_with_compact_storage_and_composite_key_test(self):
        """
        Test that we can migrate a cassandra sstable with compact storage and clustering key
        """
        query = 'CREATE COLUMNFAMILY  ks.cf (pk varchar, ck1 text, v1 text, PRIMARY KEY (pk, ck1)) WITH COMPACT STORAGE'
        self._run_basic_migration_test('with_compact_storage_and_composite_key', {'pk': 'a', 'ck1': 'b', 'v1': 'abc'},
                                       compact_storage=True, query=query)

    def migrate_sstable_with_expired_ttl_test(self):
        # Data inserted in c* with the following query: INSERT INTO ks.cf (key, c1, c2) VALUES ('a', 'abc', 'cde') USING TTL 1;
        # Expect no keys because the only one inserted is expired.
        self._run_basic_migration_test('with_expired_ttl', None, sleep=10)

    def migrate_sstable_with_cell_tombstone_test(self):
        # Content generated with:
        # INSERT INTO ks.cf (key, c1, c2) VALUES ('a', 'abc', 'cde');
        # nodetool flush
        # DELETE c2 FROM ks.cf where key = 'a';
        self._run_basic_migration_test('with_cell_tombstone', {'key': 'a', 'c1': 'abc', 'c2': None})

    def migrate_sstable_with_row_tombstone_test(self):
        # Content generated with:
        # INSERT INTO ks.cf (key, c1, c2) VALUES ('a', 'abc', 'cde');
        # nodetool flush
        # DELETE FROM ks.cf where key = 'a';
        self._run_basic_migration_test('with_row_tombstone', None)

    def migrate_sstable_with_range_boundary_tombstone_test(self):
        if self.version == '2_1_x' or self.version == '2_2_x':
            self.skipTest('Test not supported in version 2.1.x or 2.2.x')

        node1 = self.start_cluster_and_get_node1()

        query = 'CREATE COLUMNFAMILY ks.cf (pk int, ck int, PRIMARY KEY (pk, ck))'
        self.create_ks_and_cf(node1, None, None, False, query=query)

        self.load_migrated_tables(node1, 'with_range_boundary_tombstone')

        self.check_number_of_rows(node1, 1)

        result = self.get_all_rows_for_check(node1)
        # https://github.com/scylladb/scylla-tools-java/issues/205
        # Content generated with:
        # CREATE COLUMNFAMILY ks.cf (pk int, ck int, PRIMARY KEY (pk, ck));
        # INSERT INTO ks.cf (pk, ck) VALUES (1, 1);
        # INSERT INTO ks.cf (pk, ck) VALUES (1, 2);
        # INSERT INTO ks.cf (pk, ck) VALUES (1, 3);
        # INSERT INTO ks.cf (pk, ck) VALUES (1, 4);
        # INSERT INTO ks.cf (pk, ck) VALUES (1, 5);
        # nodetool flush
        # DELETE FROM ks.cf WHERE pk = 1 AND ck >= 2 AND ck < 3;
        # DELETE FROM ks.cf WHERE pk = 1 AND ck >= 3;

        self.assertEqual(result[0].pk, 1, "check partition key")
        self.assertEqual(result[0].ck, 1, "check clustering key")

    def migrate_sstable_with_range_tombstone_test(self):
        # Content generated with:
        # INSERT INTO ks.cf (key, c1, c2) VALUES ('a', 'abc', 'cde');
        # INSERT INTO ks.cf (key, c1, c2) VALUES ('b', 'abc', 'cde');
        # INSERT INTO ks.cf (key, c1, c2) VALUES ('c', 'abc', 'cde');
        # nodetool flush
        # DELETE FROM ks.cf WHERE key IN ('a', 'b');
        self._run_basic_migration_test('with_range_tombstone', {'key': 'c', 'c1': 'abc', 'c2': 'cde'})

    def migrate_sstable_with_clustering_key_range_tombstone_test(self):
        if self.version == '2_1_x' or self.version == '2_2_x':
            self.skipTest('Test not supported in version 2.1.x or 2.2.x')

        node1 = self.start_cluster_and_get_node1()

        query = 'CREATE COLUMNFAMILY ks.cf (pk int, ck int, v int, PRIMARY KEY (pk, ck))'
        self.create_ks_and_cf(node1, None, None, False, query=query)

        self.load_migrated_tables(node1, 'with_clustering_key_range_tombstone')

        self.check_number_of_rows(node1, 2)

        result = self.get_all_rows_for_check(node1)
        # https://github.com/scylladb/scylla-tools-java/issues/204
        # Content generated with:
        # CREATE COLUMNFAMILY ks.cf (pk int, ck int, v int, PRIMARY KEY (pk, ck));
        # INSERT INTO ks.cf (pk, ck, v) VALUES (1, 1, 1);
        # INSERT INTO ks.cf (pk, ck, v) VALUES (1, 2, 1);
        # INSERT INTO ks.cf (pk, ck, v) VALUES (1, 3, 1);
        # INSERT INTO ks.cf (pk, ck, v) VALUES (1, 4, 1);
        # INSERT INTO ks.cf (pk, ck, v) VALUES (1, 5, 1);
        # nodetool flush
        # DELETE FROM ks.cf WHERE pk = 1 AND ck >= 2 AND ck <= 4;

        self.assertEqual(result[0].pk, 1, "check partition key of row 1")
        self.assertEqual(result[0].ck, 1, "check clustering key of row 1")
        self.assertEqual(result[0].v, 1, "check data of row 1")

        self.assertEqual(result[1].pk, 1, "check partition key of row 2")
        self.assertEqual(result[1].ck, 5, "check clustering key of row 2")
        self.assertEqual(result[1].v, 1, "check data of row 2")

    def migrate_sstable_with_wide_row_test(self):
        node1 = self.start_cluster_and_get_node1()

        # CREATE COLUMNFAMILY ks.cf (key varchar, c varchar, v varchar, PRIMARY KEY(key, c))
        self.create_ks_and_cf(node1, None, None, False)
        self.load_migrated_tables(node1, 'with_wide_rows')

        self.check_number_of_rows(node1, 3)

        result = self.get_all_rows_for_check(node1)
        # INSERT INTO ks.cf (key, c, v) VALUES ('a', 'a', 'b');
        self.assertEqual(result[0].key, 'a', "check partition key")
        self.assertEqual(result[0].c, 'a', "check column c1")
        self.assertEqual(result[0].v, 'b', "check column c1")
        # INSERT INTO ks.cf (key, c, v) VALUES ('b', 'a', 'a');
        self.assertEqual(result[1].key, 'b', "check partition key")
        self.assertEqual(result[1].c, 'a', "check column c1")
        self.assertEqual(result[1].v, 'a', "check column c1")
        # INSERT INTO ks.cf (key, c, v) VALUES ('b', 'b', 'b');
        self.assertEqual(result[2].key, 'b', "check partition key")
        self.assertEqual(result[2].c, 'b', "check column c1")
        self.assertEqual(result[2].v, 'b', "check column c1")

    def migrate_sstable_with_collection_set_test(self):
        # CREATE COLUMNFAMILY ks.cf (key varchar PRIMARY KEY, messages set<text>);
        # INSERT INTO ks.cf (key, messages) VALUES ( 'a', {'hello world', 'scylla', 'scylladb', 'test'});
        self._run_migration_test_for_collection("with_collection_set", "set<text>", {
                                                'a': {'hello world', 'scylla', 'scylladb', 'test'}})

    def migrate_sstable_with_collection_list_test(self):
        # CREATE COLUMNFAMILY ks.cf (key varchar PRIMARY KEY, messages list<text>);
        # INSERT INTO ks.cf (key, messages) VALUES ( 'a', ['scylladb', 'scylla', 'hello world', 'test']);
        self._run_migration_test_for_collection("with_collection_list", "list<text>", {
                                                'a': ['scylladb', 'scylla', 'hello world', 'test']})

    def migrate_sstable_with_collection_map_test(self):
        # CREATE COLUMNFAMILY ks.cf (key varchar PRIMARY KEY, messages map<varchar, text>)
        # INSERT INTO ks.cf (key, messages) VALUES ( 'a', { 'a':'value1', 'b':'value2' });
        self._run_migration_test_for_collection("with_collection_map", "map<varchar, text>", {
                                                'a': {'a': 'value1', 'b': 'value2'}})

    def migrate_sstable_with_frozen_collection_map_test(self):
        # CREATE COLUMNFAMILY ks.cf (key varchar PRIMARY KEY, messages frozen<map<varchar, text>>) ...
        # C* returns [Row(key=u'a', messages=OrderedMapSerializedKey([(u'a', u'value1'), (u'b', u'value2')])),
        # Row(key=u'b', messages=OrderedMapSerializedKey([(u'a', u'value1'), (u'b', u'value2')]))] when
        # querying the whole content of sstable with frozen collection map
        self._run_migration_test_for_collection("with_frozen_collection_map", "frozen<map<varchar, text>>",
                                                {'a': {'a': 'value1', 'b': 'value2'}, 'b': {'a': 'value1', 'b': 'value2'}})

    def migrate_sstable_with_static_cell_test(self):
        node1 = self.start_cluster_and_get_node1()

        query = 'CREATE COLUMNFAMILY ks.cf (key varchar, s text STATIC, i int, PRIMARY KEY (key, i)) WITH comment=\'test cf\' AND read_repair_chance=0.000000'
        self.create_ks_and_cf(node1, None, None, False, query=query)

        self.load_migrated_tables(node1, 'with_static_cell')

        self.check_number_of_rows(node1, 2)

        result = self.get_all_rows_for_check(node1)
        # contents generated with:
        # INSERT INTO ks.cf (key, s, i) VALUES ('k', 'old', 0);
        # INSERT INTO ks.cf (key, s, i) VALUES ('k', 'new', 1);
        self.assertEqual(result[0].key, 'k', "check partition key")
        self.assertEqual(result[0].i, 0, "check clustering key")
        self.assertEqual(result[0].s, 'new', "check static cell")
        self.assertEqual(result[1].key, 'k', "check partition key")
        self.assertEqual(result[1].i, 1, "check clustering key")
        self.assertEqual(result[1].s, 'new', "check static cell")

    def migrate_sstable_with_overlapping_tombstones_test(self):
        node1 = self.start_cluster_and_get_node1()

        query = 'create COLUMNFAMILY  ks.cf (pk text, ck1 text, ck2 text, data text, primary key(pk, ck1, ck2))'
        self.create_ks_and_cf(node1, None, None, False, query=query)

        self.load_migrated_tables(node1, 'with_overlapping_tombstones')

        self.check_number_of_rows(node1, 1)

        result = self.get_all_rows_for_check(node1)
        # contents generated with:
        # insert into ks.cf (pk, ck1, ck2, data) values('pk', 'aaa', 'bbb', 'ccc');
        # insert into ks.cf (pk, ck1, ck2, data) values('pk', 'aaa', 'ccc', 'ddd');
        # insert into ks.cf (pk, ck1, ck2, data) values('pk', 'aaa', 'ddd', 'eee');
        # insert into ks.cf (pk, ck1, ck2, data) values('pk', 'bbb', 'aaa', 'fff');
        # delete from ks.cf where pk='pk' and ck1='aaa';
        # delete from ks.cf where pk='pk' and ck1='aaa' and ck2='bbb';
        #
        # ->
        # [
        #     {"key": "pk",
        #      "cells": [["aaa:_","aaa:bbb:_",1459842756489757,"t",1459842756],
        #                ["aaa:bbb:_","aaa:bbb:!",1459842776570351,"t",1459842776],
        #                ["aaa:bbb:!","aaa:!",1459842756489757,"t",1459842756],
        #                ["bbb:aaa:","",1459842718297591],
        #                ["bbb:aaa:data","fff",1459842718297591]]}
        # ]

        self.assertEqual(result[0].pk, 'pk', "check partition key")
        self.assertEqual(result[0].ck1, 'bbb', "check clustering key")
        self.assertEqual(result[0].ck2, 'aaa', "check partition key")
        self.assertEqual(result[0].data, 'fff', "check data")

    def migrate_sstable_with_user_defined_types_test(self):
        node1 = self.start_cluster_and_get_node1()

        query = [
            'create type ks.ut1 (f1 text, f2 text)',
            'create type ks.ut2 (f1 text, f2 frozen<ut1>)',
            'create table ks.cf (id uuid primary key, c frozen<ut2>)'
        ]
        self.create_ks_and_cf(node1, None, None, False, query=query)

        self.load_migrated_tables(node1, 'with_user_types')

        self.check_number_of_rows(node1, 2)

        result = self.get_all_rows_for_check(node1)

        # Content created by:
        # INSERT INTO ks.cf (id, c) VALUES (62c36092-82a1-3a00-93d1-46196ee77202, { b1: 'a', b2: { a1: 'b', a2: 'c' } });
        # INSERT INTO ks.cf (id, c) VALUES (62c36092-82a1-3a00-93d1-46196ee77242, { b1: 'x', b2: { a1: 'y', a2: 'z' } });
        self.assertEqual(result[0].id, uuid.UUID('62c36092-82a1-3a00-93d1-46196ee77202'), "check id row 0")
        self.assertEqual(result[0].c, ('a', ('b', 'c')), "check c row 0")
        self.assertEqual(result[1].id, uuid.UUID('62c36092-82a1-3a00-93d1-46196ee77242'), "check id row 1")
        self.assertEqual(result[1].c, ('x', ('y', 'z')), "check c row 1")

    # Test that scylla's issue 1212 is fixed, look: https://github.com/scylladb/scylla/issues/1212
    # Refresh procedure should ask row cache to evict some rows covered by new sstables.
    def migrate_sstable_to_check_consistency_test(self):
        node1 = self.start_cluster_and_get_node1()

        query = 'CREATE COLUMNFAMILY ks.cf (p1 text, r1 int, PRIMARY KEY (p1)) WITH read_repair_chance=0.000000'
        self.create_ks_and_cf(node1, None, None, False, query=query)

        # load row key1 with value 1
        # Content created with:
        # INSERT INTO ks.cf (p1, r1) VALUES ('key1', 1);
        self.load_migrated_tables(node1, 'to_check_consistency/1')
        self.check_number_of_rows(node1, 1)

        # read key1 content for it to be cached
        result = self.get_all_rows_for_check(node1)
        self.assertEqual(result[0].p1, 'key1', "check partition key")
        self.assertEqual(result[0].r1, 1, "check value")

        # load row key1 with value 2
        # Content created with:
        # INSERT INTO ks.cf (p1, r1) VALUES ('key1', 1);
        # nodetool flush
        # UPDATE SET ks.cf r1 = 2 WHERE p1 = 'key1';
        self.load_migrated_tables(node1, 'to_check_consistency/2')
        self.check_number_of_rows(node1, 1)

        # read key1 content and expect that it's correct because refresh invalidated cache.
        result = self.get_all_rows_for_check(node1)
        self.assertEqual(result[0].p1, 'key1', "check partition key")
        self.assertEqual(result[0].r1, 2, "check value")

    def migrate_sstable_with_large_row_number_test(self):
        """
        Create scylla cluster and run cassandra stress test to populate large number of rows.
        Migrate sstables and validate that all the rows are loaded
        """
        cluster = self.cluster
        cluster.populate(1).start()
        node1 = cluster.nodelist()[0]

        stress_count = 1000000
        if cluster.scylla_mode == 'debug':
            stress_count //= 10

        debug('Run stress test(n={}) on node1'.format(stress_count))
        profile_path = os.path.join(os.path.dirname(__file__),
                                    'test_data/c-s-profiles/cassandra-stress-custom-large-row-num-1.yaml')
        node1.stress(['user', 'profile={}'.format(profile_path), 'ops(insert=1)', 'n={}'.format(stress_count), '-rate', 'threads=4'],
                     capture_output=True)

        timeout = 300
        if cluster.scylla_mode == 'debug':
            timeout *= 3

        debug('Reading initial data')
        session = self.patient_cql_connection(node1)
        rows = rows_to_list(session.execute('SELECT count(*) FROM keyspace1.standard1;', timeout=timeout))
        row_number_src = rows[0][0]
        debug('{} rows written'.format(row_number_src))

        debug('Flush data to sstables')
        node1.flush()
        debug('Stop node1')
        node1.stop(wait_other_notice=True)

        tmpdir = safe_mkdtemp()
        dir = os.path.join(tmpdir, 'keyspace1', 'standard1')
        os.makedirs(dir)
        data_dir = self.get_cf_dir(os.path.join(node1.get_path(), 'data/keyspace1'), 'standard1')

        debug('Copy node1 sstables from {} to {}'.format(data_dir, dir))
        data_files = get_sstables_files(data_dir)
        debug('Data files: {}'.format(data_files))
        for data_file in data_files:
            shutil.copy2(os.path.join(data_dir, data_file), dir)

        debug('Remove sstables and commit log for node1')
        shutil.rmtree(os.path.join(node1.get_path(), 'commitlogs'))
        for data_file in data_files:
            os.unlink(os.path.join(data_dir, data_file))

        debug('Start node1')
        node1.start(wait_for_binary_proto=True)
        time.sleep(5)

        ip = node1.address()
        debug('Run sstableloader on node1')
        cmd = [node1.get_tool('sstableloader'), '-d', ip, dir]
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = p.communicate()
        exit_status = p.wait()

        shutil.rmtree(tmpdir)
        if exit_status != 0:
            raise Exception("sstableloader command '%s' failed; exit status: %d'; stdout: %s; stderr: %s" %
                            (" ".join(cmd), exit_status, stdout, stderr))

        time.sleep(5)
        debug('Verify number of rows on node1')
        session = self.patient_cql_connection(node1)
        rows = rows_to_list(session.execute('SELECT count(*) FROM keyspace1.standard1;', timeout=timeout))
        row_number = rows[0][0]
        debug('{} rows read'.format(row_number))
        self.assertEqual(row_number, row_number_src)

    def migrate_sstable_with_variant_data_types_test(self):
        node1 = self.start_cluster_and_get_node1()
        query = "CREATE COLUMNFAMILY ks.cf (aascii ascii,"\
            "abigint bigint,"\
            "ablob blob,"\
            "aboolean boolean,"\
            "adouble double,"\
            "adecimal decimal,"\
            "afloat float,"\
            "ainet inet,"\
            "aint int,"\
            "atext text,"\
            "atimestamp timestamp,"\
            "atimeuuid timeuuid,"\
            "auuid uuid,"\
            "avarchar varchar,"\
            "avarint varint,"\
            "alist list<int>,"\
            "amap map<int,int>,"\
            "aset set<int>,"\
            "PRIMARY KEY (aascii, abigint)) "\
            "WITH comment=\'test cf\' AND read_repair_chance=0.000000"
        self.create_ks_and_cf(node1, None, None, False, query=query)
        node1.flush()
        self.load_migrated_tables(node1, 'with_variant_data_types')
        self.check_number_of_rows(node1, 3)
        # Content created using:
        # INSERT INTO ks.cf (aascii, abigint, ablob, aboolean, adouble, adecimal, afloat, ainet, aint, atext, atimestamp, atimeuuid, auuid, avarchar, avarint, alist, amap, aset) VALUES ('tzach', 1999, 0x0000000000000003, true, 10.10, 10, 11.11, '204.202.130.223', 17, 'text', '2016-08-30 07:01:00.000Z', e23f450f-53a6-11e2-7f7f-7f7f7f7f7f7f, 123e4567-e89b-12d3-a456-426655440000, 'tzachvarchar', 17, [1, 2, 3], {1: 2}, {1, 2, 3, 4});
        # INSERT INTO ks.cf (aascii, abigint, ablob, aboolean, adouble, adecimal, afloat, ainet, aint, atext, atimestamp, atimeuuid, auuid, avarchar, avarint, alist, amap, aset) VALUES ('tzach', 2000, 0x0000000000000003, true, 10.10, 10, 11.11, '204.202.130.223', 17, 'text', '2016-08-30 07:01:00.000Z', e23f450f-53a6-11e2-7f7f-7f7f7f7f7f7f, 123e4567-e89b-12d3-a456-426655440000, 'tzachvarchar', 17, [1, 2, 3], {1: 2}, {1, 2, 3, 4});
        # INSERT INTO ks.cf (aascii, abigint, ablob, aboolean, adouble, adecimal, afloat, ainet, aint, atext, atimestamp, atimeuuid, auuid, avarchar, avarint, alist, amap, aset) VALUES ('livyatan', 2001, 0x0000000000000003, true, 10.10, 10, 11.11, '204.202.130.223', 17, 'text', '2016-08-30 07:01:00.000Z', e23f450f-53a6-11e2-7f7f-7f7f7f7f7f7f, 123e4567-e89b-12d3-a456-426655440000, 'tzachvarchar', 17, [1, 2, 3], {1: 2}, {1, 2, 3, 4});
        result = self.get_all_rows_for_check(node1)
        for i in range(0, 3):
            if i == 0 or i == 1:
                self.assertEqual(result[i].aascii,
                                 'tzach', "check ascii column")
            if i == 2:
                self.assertEqual(result[i].aascii,
                                 'livyatan', "check ascii column")
            self.assertEqual(result[i].abigint, 1999 +
                             i, "check bigint column")
            self.assertEqual(result[i].ablob.hex(), '0000000000000003', "check blob column")
            self.assertEqual(result[i].aboolean, True, "check boolean column")
            self.assertEqual(result[i].adecimal, 10, "check decimal column")
            self.assertEqual(result[i].adouble, 10.10, "check double column")
            self.assertEqual(
                round(result[i].afloat, 2), 11.11, "check afloat column")
            self.assertEqual(
                result[i].ainet, '204.202.130.223', "check ainet column")
            self.assertEqual(result[i].aint, 17, "check inet column")
            self.assertEqual(result[i].atext, "text", "check text column")
            self.assertEqual(result[i].atimestamp, datetime.datetime(
                2016, 8, 30, 7, 1), "check timestamp column")
            self.assertEqual(result[i].atimeuuid, uuid.UUID(
                'e23f450f-53a6-11e2-7f7f-7f7f7f7f7f7f'),
                "check timeuuid column")
            self.assertEqual(result[i].auuid, uuid.UUID(
                '123e4567-e89b-12d3-a456-426655440000'), "check uuid column")
            self.assertEqual(result[i].avarchar, str(
                "tzachvarchar"), "check varchar column")
            self.assertEqual(result[i].avarint, 17, "check varint column")
            self.assertEqual(result[i].alist, [1, 2, 3], "check list column")
            self.assertEqual(result[i].amap, {1: 2}, "check map column")
            self.assertEqual(result[i].aset, {1, 2, 3, 4}, "check set column")

    def migrate_sstable_with_old_format_counter_test_expect_fail(self):
        if self.version != '2_1_x':
            self.skipTest('Test only relevant to old-format counters')

        """
        create cassandra cluster version 2.0.x
        CREATE KEYSPACE ks WITH replication={'class':'SimpleStrategy', 'replication_factor':1};
        CREATE TABLE ks.cf (pk int PRIMARY KEY, cnt COUNTER);
        add 10 counters
        create cassandra cluster version 2.1.x
        create ks and cf
        copy sstables from 2.0.x
        start node and run nodetool upgradesstables
        add more 10 counters
        """
        cluster = self.cluster
        self.populate_cluster(cluster)
        node1 = self.cluster.nodelist()[0]
        node1.set_configuration_options()
        self.start_cluster(cluster)

        query = "CREATE TABLE ks.cf (pk int PRIMARY KEY, cnt COUNTER);"
        self.create_ks_and_cf(node1, None, None, False, query=query)
        expected_message = 'Direct loading non-Scylla SSTables containing counters is not supported.'
        self.load_migrated_tables_expect_fail(node1, 'with_old_format_counter', message=expected_message)

    def migrate_sstable_with_counter_test_expect_fail(self):
        """
        https://github.com/scylladb/scylla/issues/2119
        CREATE KEYSPACE ks WITH replication={'class':'SimpleStrategy', 'replication_factor':1};
        CREATE TABLE ks.cf (first_name varchar, last_name varchar, cnt counter, PRIMARY KEY(first_name, last_name));
        UPDATE ks.cf SET cnt = cnt + 1 WHERE first_name='albert' AND last_name='einstein';
        UPDATE ks.cf SET cnt = cnt + 2 WHERE first_name='thomas' AND last_name='edison';
        flush
        UPDATE ks.cf SET cnt = cnt + 10 WHERE first_name='albert' AND last_name='einstein';
        UPDATE ks.cf SET cnt = cnt + 3 WHERE first_name='marie' AND last_name='curie';
        UPDATE ks.cf SET cnt = cnt - 5 WHERE first_name='albert' AND last_name='einstein';
        flush
        """
        cluster = self.cluster
        self.populate_cluster(cluster)
        node1 = self.cluster.nodelist()[0]
        node1.set_configuration_options()
        self.start_cluster(cluster)

        query = "CREATE TABLE ks.cf " \
                "(first_name varchar, last_name varchar, cnt counter, PRIMARY KEY(first_name, last_name));"
        self.create_ks_and_cf(node1, None, None, False, query=query)
        expected_message = 'Direct loading non-Scylla SSTables containing counters is not supported.'
        self.load_migrated_tables_expect_fail(node1, 'with_counter', message=expected_message)

    def migrate_sstable_with_counter_test(self):
        """
        https://github.com/scylladb/scylla/issues/2119
        CREATE KEYSPACE ks WITH replication={'class':'SimpleStrategy', 'replication_factor':1};
        CREATE TABLE ks.cf (first_name varchar, last_name varchar, cnt counter, PRIMARY KEY(first_name, last_name));
        UPDATE ks.cf SET cnt = cnt + 1 WHERE first_name='albert' AND last_name='einstein';
        UPDATE ks.cf SETcnt = cnt + 2 WHERE first_name='thomas' AND last_name='edison';
        flush
        UPDATE ks.cf SET cnt = cnt + 10 WHERE first_name='albert' AND last_name='einstein';
        UPDATE ks.cf SET cnt = cnt + 3 WHERE first_name='marie' AND last_name='curie';
        UPDATE ks.cf SET cnt = cnt - 5 WHERE first_name='albert' AND last_name='einstein';
        flush
        """
        cluster = self.cluster
        self.populate_cluster(cluster, extra_values={'enable_dangerous_direct_import_of_cassandra_counters': True})
        node1 = self.cluster.nodelist()[0]
        node1.set_configuration_options()
        self.start_cluster(cluster)

        query = "CREATE TABLE ks.cf " \
                "(first_name varchar, last_name varchar, cnt counter, PRIMARY KEY(first_name, last_name));"
        self.create_ks_and_cf(node1, None, None, False, query=query)
        self.load_migrated_tables(node1, 'with_counter')

    def migrate_sstable_with_wrong_partitioner_test_expect_fail(self):
        """
        https://github.com/scylladb/scylla/issues/4331
        Partitioner: org.apache.cassandra.dht.RandomPartitioner
        initial_token: 1
        CREATE KEYSPACE ks
            WITH replication={
                'class':'SimpleStrategy', 'replication_factor':1
            };
        CREATE TABLE ks.cf ( pk INT, ck INT, v INT, PRIMARY KEY(pk, ck))
            WITH compression = { 'sstable_compression' : '' };
        INSERT INTO ks.cf (pk, ck, v) VALUES (1, 10, 100);
        INSERT INTO ks.cf (pk, ck, v) VALUES (2, 20, 200);
        INSERT INTO ks.cf (pk, ck, v) VALUES (3, 30, 300);
        flush
        """
        cluster = self.cluster
        self.populate_cluster(cluster)
        node1 = self.cluster.nodelist()[0]
        node1.set_configuration_options()
        self.start_cluster(cluster)

        query = "CREATE TABLE ks.cf (pk INT, ck INT, v INT, PRIMARY KEY(pk, ck))" + \
                " WITH compression = { 'sstable_compression' : '' }"
        self.create_ks_and_cf(node1, None, None, False, query=query)
        self.load_migrated_tables_expect_fail(node1,
                                              'with_wrong_partitioner',
                                              message=self.get_wrong_partitioner_error_message())

    # ######################## Helper functions ####################################

    def check_number_of_rows(self, node, expected_number_of_rows):
        debug("Checking rows on node1...")
        query = "SELECT COUNT(*) FROM cf"
        statement = SimpleStatement(query)
        s = self.patient_cql_connection(node, 'ks')
        result = list(s.execute(statement))
        self.assertEqual(result[0].count, expected_number_of_rows,
                         "Expected {} rows. Got {}".format(expected_number_of_rows, list(s.execute("SELECT * FROM ks.cf"))))

    def get_all_rows_for_check(self, node1):
        debug("Checking rows content on node1...")
        query = "SELECT * FROM ks.cf"
        statement = SimpleStatement(query)
        s = self.patient_cql_connection(node1, 'ks')
        result = list(s.execute(statement))
        debug(result)
        return result

    def _run_basic_migration_test(self, migrated_files_dir, row_content, compression=None, compact_storage=False, sleep=0, query=None):
        node1 = self.start_cluster_and_get_node1()

        self.create_ks_and_cf(node1, columns={'c1': 'text', 'c2': 'text'}, compression=compression,
                              compact_storage=compact_storage, query=query)
        self.load_migrated_tables(node1, migrated_files_dir)

        time.sleep(sleep)

        expected_keys = 1
        if row_content is None:
            expected_keys = 0
        self.check_number_of_rows(node1, expected_keys)

        if row_content is not None:
            result = self.get_all_rows_for_check(node1)
            for k, error_string in [('key', 'check partition key'), ('c1', 'check column c1'),
                                    ('c2', 'check column c2'), ('pk', 'check partition key'),
                                    ('ck', 'check clustering key'), ('v1', 'check column v1')]:
                if k in row_content:
                    self.assertEqual(getattr(result[0], k), row_content[k], error_string)

    def _run_migration_test_for_collection(self, migration_dir_name, collection_type, collection_content):
        node1 = self.start_cluster_and_get_node1()

        # CREATE COLUMNFAMILY ks.cf (key varchar PRIMARY KEY, messages collection_type<text>)
        self.create_ks_and_cf(node1, {'messages': '{}'.format(collection_type)}, None, False)
        self.load_migrated_tables(node1, migration_dir_name)

        self.check_number_of_rows(node1, len(collection_content))

        result = self.get_all_rows_for_check(node1)
        idx = 0
        for key, value in collection_content.items():
            self.assertEqual(result[idx].key, key, "check partition key")
            # INSERT INTO ks.cf (key, messages) VALUES('a', {'scylladb', 'scylla', 'hello world', 'test'});
            self.assertEqual(result[idx].messages, value, "check messages")
            idx += 1

    def create_ks_and_cf(self, node, columns, compression, compact_storage, query=None):
        debug("Creating a CQL connection...")
        session = self.patient_cql_connection(node)

        debug("Creating a keyspace 'ks'...")
        self.create_ks(session, 'ks', 1)

        debug("Creating a column family 'cf'...")
        if isinstance(query, str):
            session.execute(query)
            time.sleep(0.2)
        elif query is not None:
            for q in query:
                session.execute(q)
                time.sleep(0.2)
        else:
            self.create_cf(session, 'cf', read_repair=0.0, columns=columns,
                           compression=compression, compact_storage=compact_storage)

        debug("Flushing a keyspace...")
        node.nodetool("flush -- ks")

    def get_cassandra_sstable_dir(self, version, migrated_files_dir):
        return "{}/cassandra-sstables/migration/{}/{}".format(os.path.dirname(os.path.realpath(__file__)), version,
                                                              migrated_files_dir)

    def populate_cluster(self, cluster, extra_values=None):
        # Disable hinted handoff and set batch commit log so this doesn't
        # interfere with the test (this must be after the populate)
        values = {'hinted_handoff_enabled': False}
        if extra_values:
            values.update(extra_values)
        cluster.set_configuration_options(values, batch_commitlog=True)
        debug("Starting a cluster of one node...")
        cluster.populate(1)

    def start_cluster(self, cluster):
        cluster.start(wait_for_binary_proto=True, wait_other_notice=True)

    def get_node(self, cluster, node_idx):
        return cluster.nodelist()[node_idx]

    def start_cluster_and_get_node1(self):
        cluster = self.cluster

        self.populate_cluster(cluster)
        self.start_cluster(cluster)
        node1 = self.get_node(cluster, 0)
        return node1

    def get_cf_dir(self, ks_dir, cf_name):
        """
        Return the first CF directory for a CF with a given name
        """
        cf_pattern = re.compile("{}-".format(cf_name))
        for root, dirs, files in os.walk(ks_dir):
            for d in dirs:
                if cf_pattern.match(d):
                    return os.path.join(root, d)

    def copy_files_to(self, from_dir, to_dir):
        for f in os.listdir(from_dir):
            shutil.copy2(os.path.join(from_dir, f), os.path.join(to_dir, f))

    def recursive_copy_to(self, from_dir, to_dir):
        shutil.copytree(from_dir, to_dir)

    def get_sstable_version(self,  cf_dir, assert_only_one_version=True):
        file_list = os.listdir(cf_dir)
        debug("{}".format(file_list))
        sstable_version_regex = re.compile(r'(\w+)-\d+-(.+)\.(db|txt|sha1|crc32)')

        sstable_versions = list(
            set([sstable_version_regex.search(f).group(1) for f in file_list if sstable_version_regex.search(f)]))

        if assert_only_one_version:
            if len(sstable_versions) != 1:
                print('Expected only one version, got {}. File list: {}'.format(sstable_versions, file_list))
            self.assertEqual(len(sstable_versions), 1, sstable_versions)
        if sstable_versions:
            return sstable_versions[0]
#       else:
            return None
# Dtest created to test migration of data from C* to Scylla
#


@tools.nottest
@attr('dtest-full', 'single_node')
class TestMigration(MigrationTestBase):

    def __init__(self, *args, **kwargs):
        kwargs['cluster_options'] = {'start_rpc': 'true'}
        Tester.__init__(self, *args, **kwargs)

    def migrate_sstable_with_schema_change_test(self):
        # Content of Cassandra dir generated with following cql commands:
        # CREATE TABLE ks.cf (user_name varchar PRIMARY KEY, bio ascii);
        # INSERT INTO ks.cf (user_name, bio) VALUES ('a', 'test');
        # ALTER TABLE ks.cf ADD age int;
        # INSERT INTO ks.cf (user_name, bio, age) VALUES ('b', 'test', 0);

        cluster = self.cluster

        self.populate_cluster(cluster)
        self.copy_migrated_data_dir('with_schema_change', skip_system_traces=True)
        self.start_cluster(cluster)
        node1 = self.get_node(cluster, 0)

        self.check_number_of_rows(node1, 2)

        result = self.get_all_rows_for_check(node1)
        self.assertEqual(result[0].user_name, 'a', "check partition key")
        self.assertEqual(result[0].age, None, "check added cell")
        self.assertEqual(result[0].bio, 'test', "check static cell")
        self.assertEqual(result[1].user_name, 'b', "check partition key")
        self.assertEqual(result[1].age, 0, "check added cell")
        self.assertEqual(result[1].bio, 'test', "check static cell")

        debug("Adding a new row...")
        query = "INSERT INTO ks.cf (user_name, bio, age) VALUES ('c', 'test', 0)"
        s = self.patient_cql_connection(node1, 'ks')
        statement = SimpleStatement(query)
        s.execute(statement)
        node1.nodetool("flush -- ks")

        debug("Checking rows content after adding row...")
        self.check_number_of_rows(node1, 3)
        result = self.get_all_rows_for_check(node1)
        # new row is in index 1
        self.assertEqual(result[1].user_name, 'c', "check partition key")
        self.assertEqual(result[1].age, 0, "check added cell")
        self.assertEqual(result[1].bio, 'test', "check static cell")

    # Helpers
    def copy_migrated_data_dir(self, migrated_data_dir, skip_system_traces=False):
        cassandra_dir = "{}/data".format(self.get_cassandra_sstable_dir('2_1_x', migrated_data_dir))
        debug("cassandra data dir for counter is {}".format(cassandra_dir))

        scylla_dir = os.path.join(self.test_path, 'test', 'node1', 'data')
        debug("Node data directory is {}".format(scylla_dir))

        debug("Copying data/ks created by Cassandra...")
        self.recursive_copy_to(os.path.join(cassandra_dir, 'ks'), os.path.join(scylla_dir, 'ks'))
        debug("Copying data/system created by Cassandra...")
        self.recursive_copy_to(os.path.join(cassandra_dir, 'system'), os.path.join(scylla_dir, 'system'))
        if not skip_system_traces:
            debug("Copying data/system_traces created by Cassandra...")
            self.recursive_copy_to(os.path.join(cassandra_dir, 'system_traces'),
                                   os.path.join(scylla_dir, 'system_traces'))

    def load_migrated_tables(self, node, migrated_files_dir, ks='ks', cf='cf',
                             partitioner='org.apache.cassandra.dht.Murmur3Partitioner'):
        cassandra_sstable_dir = self.get_cassandra_sstable_dir(self.version, migrated_files_dir)
        debug("cassandra sstable dir is {}".format(cassandra_sstable_dir))

        ks_dir = os.path.join(self.test_path, 'test', 'node1', 'data', ks)
        cf_dir = self.get_cf_dir(ks_dir, cf)
        debug("Column family directory is {}".format(cf_dir))

        upload_dir = os.path.join(cf_dir, "upload")
        debug("Column family upload directory is {}".format(upload_dir))

        debug("Copying sstables created by Cassandra...")
        self.copy_files_to(cassandra_sstable_dir, upload_dir)

        debug("Running 'nodetool refresh -- {} {}' to load migrated sstables".format(ks, cf))
        node.nodetool("refresh -- {} {}".format(ks, cf))

    def load_migrated_tables_expect_fail(self, node, migrated_files_dir, message=None, ks='ks', cf='cf'):
        if message:
            self.ignore_log_patterns = [message]
        cassandra_sstable_dir = self.get_cassandra_sstable_dir(self.version, migrated_files_dir)
        debug("cassandra sstable dir is {}".format(cassandra_sstable_dir))

        ks_dir = os.path.join(self.test_path, 'test', 'node1', 'data', ks)
        cf_dir = self.get_cf_dir(ks_dir, cf)
        debug("Column family directory is {}".format(cf_dir))

        debug("Copying sstables created by Cassandra...")
        self.copy_files_to(cassandra_sstable_dir, cf_dir + "/upload")

        debug("Running 'nodetool refresh -- {} {}' to load migrated sstables".format(ks, cf))
        try:
            node.nodetool("refresh -- {} {}".format(ks, cf))
            assert False
        except NodetoolError as error:
            if message:
                assert message in str(error), error

    @attr('next-gating')
    @attr('dtest-debug')
    def migrate_sstable_with_counter_test(self):
        super(TestMigration, self).migrate_sstable_with_counter_test()

    def migrate_sstable_with_variant_data_types_test(self):
        super(TestMigration, self).migrate_sstable_with_variant_data_types_test()

    def get_wrong_partitioner_error_message(self):
        return "uses org.apache.cassandra.dht.RandomPartitioner" + \
               " partitioner which is different than" + \
               " org.apache.cassandra.dht.Murmur3Partitioner" + \
               " partitioner used by the database"


@tools.nottest
@attr('dtest-full')
class TestMigrationUpgradeSSTables(TestMigration):

    @skip('test isn\'t relevant when using nodetool upgradesstables')
    def migrate_sstable_with_row_tombstone_test(self):
        # since the row tombstone data doesn't create files on disk
        pass

    @skip('test isn\'t relevant when using nodetool upgradesstables')
    def migrate_sstable_to_check_consistency_test(self):
        # since this test load multiple versions, that conflicts with version created upgradesstables
        pass

    @skip('test isn\'t relevant when using nodetool upgradesstables')
    def migrate_sstable_with_expired_ttl_test(self):
        # since expired ttl data doens't create files on disk
        pass

    def load_migrated_tables(self, node, migrated_files_dir, ks='ks', cf='cf'):
        super(TestMigrationUpgradeSSTables, self).load_migrated_tables(node, migrated_files_dir, ks='ks', cf='cf')

        ks_dir = os.path.join(self.test_path, 'test', 'node1', 'data', ks)
        cf_dir = self.get_cf_dir(ks_dir, cf)
        debug("Column family directory is {}".format(cf_dir))

        source_dir = self.get_cassandra_sstable_dir(self.version, migrated_files_dir)
        before_sstable_version = self.get_sstable_version(source_dir, assert_only_one_version=False)

        debug("Running 'nodetool upgradesstables {} {}'".format(ks, cf))
        node.nodetool("upgradesstables {} {}".format(ks, cf))
        node.flush()

        after_sstable_version = self.get_sstable_version(cf_dir)

        # check that sstable version was upgraded, or if that version equals latest version `mc`
        self.assertTrue(after_sstable_version > before_sstable_version or (before_sstable_version == after_sstable_version and after_sstable_version in ['mc', 'md']),
                        "upgradesstable failed to upgrade sstables [before_version={} after_version={}]".format(before_sstable_version, after_sstable_version))


# @skip('not run every build')
# @attr('long','compare-cassandra')
@attr('dtest-full')
class TTLWithMigrate(Tester):
    """ Test Time To Live Feature with Migration"""

    def prepare(self, default_time_to_live=None, create_table_statement=None, nodes=1, rf=1, configuration_options=None, custom_args=None):
        if configuration_options:
            self.cluster.set_configuration_options(values=configuration_options)
        self.cluster.populate(nodes).start(jvm_args=custom_args)
        node1 = self.cluster.nodelist()[0]
        self.session1 = self.patient_cql_connection(node1)
        self.create_ks(self.session1, 'ks', rf=rf)

        drop_table(session=self.session1, table_name='ttl_table', if_exists=True)

        if create_table_statement is None:
            query = """
                CREATE TABLE ttl_table (
                    key int primary key,
                    col1 int,
                    col2 int,
                    col3 int,
                )
            """
        else:
            query = create_table_statement
        if default_time_to_live:
            query += " WITH default_time_to_live = {};".format(default_time_to_live)

        self.session1.execute(query)

    @attr('next-gating')
    def big_table_with_ttls_test(self):
        """
        Test validates migration from Scylla to Cassandra of large partition table with TTLs.
         - Create the big table with different kind of columns, create 10 partitions with 1000 rows each partition and 1 partition with 100000 rows.
         - Run updates/removes on all columns
         - Take dump
         - Migrate data to Cassandra
         - Take dump
         - Compare dumps
        """
        self.prepare(nodes=4, rf=3, custom_args=["--smp", "1", "--memory", "512M"])
        keyspace_name = 'ks'
        table_name = 'cf'
        int_columns = 99
        stmt = 'create table {} (pk int, ck int, {}, clist list<int>, cset set<text>, cmap map<int, text>, ' \
               'PRIMARY KEY(pk, ck))'.format(table_name, ', '.join('c%d int' % i for i in range(1, int_columns)))
        self.session1.execute(stmt)

        min_ttl = 120

        def create_update_command(ttl, column_expr, pk, ck, table_name=table_name):
            assert ttl > min_ttl, "TTL {} must be greater than {}".format(ttl, min_ttl)
            return 'update {table_name} USING TTL {ttl} set {column_expr} where pk={pk} and ck={ck}'.format(**locals())

        # Prefill
        partitions = 10
        rows_in_partition = 1000
        debug('Create {} partitions with {} rows'.format(partitions, rows_in_partition))
        for i in range(1, partitions+1):
            for k in range(1, rows_in_partition+1):
                s = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(10))
                stmt = 'insert into {table_name} (pk, ck, {columns}, clist, cset, cmap) values ({ilist}, {klist}, {int_values}, ' \
                       '[{ilist}, {klist}], ' \
                       '{open}{set_value}{close}, {map_value})'.format(table_name=table_name,
                                                                       columns=', '.join(
                                                                           'c%d' % l for l in range(1, int_columns)),
                                                                       int_values=', '.join('%d' % l for l in range(1, int_columns)), ilist=i, klist=k, open='{\'',
                                                                       set_value=s, close='\'}', map_value='{%d: \'%s\'}' % (k, s)
                                                                       )
                self.session1.execute(stmt)

        big_partition = partitions + 1
        big_partition_rows = 100000
        debug('Create partition where pk = {} with {} rows'.format(big_partition, big_partition_rows))
        for k in range(1, big_partition_rows+1):
            s = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(10))
            stmt = 'insert into {table_name} (pk, ck, {columns}, clist, cset, cmap) values ({ilist}, {klist}, {int_values}, ' \
                   '[{ilist}, {klist}], ' \
                   '{open}{set_value}{close}, {map_value})'.format(table_name=table_name,
                                                                   columns=', '.join('c%d' %
                                                                                     l for l in range(1, int_columns)),
                                                                   int_values=', '.join('%d' % l for l in range(1, int_columns)), ilist=big_partition, klist=k, open='{\'',
                                                                   set_value=s, close='\'}', map_value='{%d: \'%s\'}' % (k, s)
                                                                   )
            self.session1.execute(stmt)

        debug('Verifying that big_partition where pk = {} has {} rows'.format(big_partition, big_partition_rows))
        count_query = 'select count(*) from {}.{} where pk = {}'.format(keyspace_name, table_name, big_partition)
        scylla_big_partition_count = list(self.session1.execute(count_query, timeout=120))[0][0]
        self.assertTrue(scylla_big_partition_count == big_partition_rows,
                        msg='Expected {big_partition_rows} rows in the big partition before update, but received '
                            '{scylla_big_partition_count}'.format(**locals()))

        node1 = self.cluster.nodelist()[0]
        self.cluster.flush()

        ttl_boundaries = [1800, 3600]
        debug('Run updates using TTLs in the {} range'.format(ttl_boundaries))

        for _ in range(1, big_partition+1):
            # Update int columns
            stmts = [create_update_command(ttl=random.randint(ttl_boundaries[0], ttl_boundaries[1]),
                                           column_expr='c%d = %d' % (random.randint(
                                               1, int_columns-1), random.randint(0, 500000)),
                                           pk=random.randint(1, partitions), ck=random.randint(1, rows_in_partition))]
            # Update big partition
            stmts.append(create_update_command(ttl=random.randint(ttl_boundaries[0], ttl_boundaries[1]),
                                               column_expr='c%d = %d' % (random.randint(
                                                   1, int_columns-1), random.randint(0, 500000)),
                                               pk=big_partition, ck=random.randint(1, big_partition_rows)))

            # Delete int value
            stmts.append(create_update_command(ttl=random.randint(ttl_boundaries[0], ttl_boundaries[1]),
                                               column_expr='c%d = NULL' % (random.randint(1, int_columns - 1)),
                                               pk=random.randint(1, partitions), ck=random.randint(1, rows_in_partition)))
            # Delete int value in big partition
            stmts.append(create_update_command(ttl=random.randint(ttl_boundaries[0], ttl_boundaries[1]),
                                               column_expr='c%d = NULL' % (random.randint(1, int_columns-1)),
                                               pk=big_partition, ck=random.randint(1, big_partition_rows)))
            # Update collection columns
            s = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(10))
            # APPEND to set column - small partitions
            stmts.append(create_update_command(ttl=random.randint(ttl_boundaries[0], ttl_boundaries[1]),
                                               column_expr='cset = cset+{\'%s\'}' % (s),
                                               pk=random.randint(1, partitions), ck=random.randint(1, rows_in_partition)))
            # APPEND to set column - Big partition
            stmts.append(create_update_command(ttl=random.randint(ttl_boundaries[0], ttl_boundaries[1]),
                                               column_expr='cset = cset+{\'%s\'}' % (s),
                                               pk=big_partition, ck=random.randint(1, big_partition_rows)))
            # APPEND to list column - small partitions
            stmts.append(create_update_command(ttl=random.randint(ttl_boundaries[0], ttl_boundaries[1]),
                                               column_expr='clist = clist+[%d]' % (random.randint(0, 500000)),
                                               pk=random.randint(1, partitions), ck=random.randint(1, rows_in_partition)))
            # APPEND to list column - Big partition
            stmts.append(create_update_command(ttl=random.randint(ttl_boundaries[0], ttl_boundaries[1]),
                                               column_expr='clist = clist+[%d]' % (random.randint(0, 500000)),
                                               pk=big_partition, ck=random.randint(1, big_partition_rows)))
            # APPEND to map column - small partitions
            stmts.append(create_update_command(ttl=random.randint(ttl_boundaries[0], ttl_boundaries[1]),
                                               column_expr='cmap = cmap+{%d: \'%s\'}' % (random.randint(0, 500000), s),
                                               pk=random.randint(1, partitions), ck=random.randint(1, rows_in_partition)))
            # APPEND to map column - Big partition
            stmts.append(create_update_command(ttl=random.randint(ttl_boundaries[0], ttl_boundaries[1]),
                                               column_expr='cmap = cmap+{%d: \'%s\'}' % (random.randint(0, 500000), s),
                                               pk=big_partition, ck=random.randint(1, big_partition_rows)))
            # OVERWRITE set column - small partitions
            stmts.append(create_update_command(ttl=random.randint(ttl_boundaries[0], ttl_boundaries[1]),
                                               column_expr='cset = {\'%s\'}' % (s),
                                               pk=random.randint(1, partitions), ck=random.randint(1, rows_in_partition)))
            # OVERWRITE set column - Big partition
            stmts.append(create_update_command(ttl=random.randint(ttl_boundaries[0], ttl_boundaries[1]),
                                               column_expr='cset = {\'%s\'}' % (s),
                                               pk=big_partition, ck=random.randint(1, big_partition_rows)))
            # OVERWRITE list column - small partitions
            stmts.append(create_update_command(ttl=random.randint(ttl_boundaries[0], ttl_boundaries[1]),
                                               column_expr='clist = [%d]' % (random.randint(0, 500000)),
                                               pk=random.randint(1, partitions), ck=random.randint(1, rows_in_partition)))
            # OVERWRITE list column - Big partition
            stmts.append(create_update_command(ttl=random.randint(ttl_boundaries[0], ttl_boundaries[1]),
                                               column_expr='clist = [%d]' % (random.randint(0, 500000)),
                                               pk=big_partition, ck=random.randint(1, big_partition_rows)))
            # OVERWRITE map column - small partitions
            stmts.append(create_update_command(ttl=random.randint(ttl_boundaries[0], ttl_boundaries[1]),
                                               column_expr='cmap = {%d: \'%s\'}' % (random.randint(0, 500000), s),
                                               pk=random.randint(1, partitions),
                                               ck=random.randint(1, rows_in_partition)))
            # OVERWRITE map column - Big partition
            stmts.append(create_update_command(ttl=random.randint(ttl_boundaries[0], ttl_boundaries[1]),
                                               column_expr='cmap = {%d: \'%s\'}' % (random.randint(0, 500000), s),
                                               pk=big_partition, ck=random.randint(1, big_partition_rows)))

            for stmt in stmts:
                self.session1.execute(stmt)

        scylla_data_json, scylla_json_path = self._dump_data(
            cluster=self.cluster, node=node1, node_owner='Scylla', compaction=True)

        debug('Verifying that big_partition where pk = {} has {} rows'.format(big_partition, big_partition_rows))
        count_query = 'select count(*) from {}.{} where pk = {}'.format(keyspace_name, table_name, big_partition)
        scylla_big_partition_count = list(self.session1.execute(count_query, timeout=120))[0][0]
        self.assertTrue(scylla_big_partition_count == big_partition_rows,
                        msg='Expected {big_partition_rows} rows in the big partition, but received '
                            '{scylla_big_partition_count}'.format(**locals()))

        # Create Cassandra cluster, migrate the data and take the dump
        cassandra_data_json, cassandra_json_path = self.migrate_to_cassandra(keyspace_name=keyspace_name, table_name=table_name,
                                                                             scylla_big_partition_count=scylla_big_partition_count,
                                                                             count_query=count_query)

        self.assertTrue(len(scylla_data_json) == len(cassandra_data_json),
                        msg='Lengths of Scylla and Cassandra dumps are not same. '
                            'Length of Scylla dump is {}, Length of Cassandra dump is {}. '
                            'Please, check and compare {} and {} files'.format(
            len(scylla_data_json), len(cassandra_data_json),
            scylla_json_path, cassandra_json_path)
        )
        self.assertTrue(scylla_data_json == cassandra_data_json, msg='Data dumps is not same in Scylla and Cassandra. '
                                                                     'Please, check and compare {} and {} files'.format(
                                                                         scylla_json_path, cassandra_json_path)
                        )
        os.unlink(scylla_json_path)
        os.unlink(cassandra_json_path)

    def migrate_to_cassandra(self, keyspace_name, table_name, take_dump=True, scylla_big_partition_count=None, count_query=''):
        cc = None
        cassandra_data_json, cassandra_json_path = '', ''
        try:
            cc = CassandraCluster(cassandra_version='3.11.3')
            cassandra_node1 = cc.run_migration(scylla_cluster=self.cluster, scylla_test_path=self.test_path,
                                               keyspace_names_list=[keyspace_name], table_names=[table_name])
            if take_dump:
                cassandra_data_json, cassandra_json_path = self._dump_data(cluster=cc.cluster, node=cassandra_node1,
                                                                           node_owner='Cassandra')

            # We want to validate the rows amount in the large partition.
            # But the count query fails on timeout in Cassandra. Comment meanwhile
            # Error in the log: org.apache.cassandra.service.DigestMismatchException: Mismatch for key DecoratedKey
            # https://stackoverflow.com/questions/39765813/datastax-mismatch-for-key-issue
            # if scylla_big_partition_count is not None:
            #     cassandra_session = self.patient_cql_connection(cassandra_node1, keyspace=keyspace_name)
            #     assert_one(cassandra_session, count_query, [scylla_big_partition_count], cl=ConsistencyLevel.ALL, timeout=300)
        except:
            raise
        finally:
            if cc:
                cc.tearDown()

        return cassandra_data_json, cassandra_json_path

    def _dump_data(self, cluster, node, node_owner, keyspace_name='ks', table_name='cf', compaction=True):
        if compaction:
            if node.is_scylla() or node.get_cassandra_version() < '2.2':
                log_file = 'system.log'
            else:
                log_file = 'debug.log'
            mark = node.mark_log(filename=log_file)
        debug('Flush data to the disk before dump')
        cluster.flush()
        if compaction:
            debug('Compacting sstables')
            node.nodetool('compact {} {}'.format(keyspace_name, table_name))
            node.watch_log_for('Compacted', from_mark=mark, filename=log_file)
        debug('Run sstabledump')
        data_json = ''
        data_json_path = tempfile.mktemp(suffix='.schema.json', prefix=node_owner)
        with open(data_json_path, 'a') as fdw:
            node.run_sstable2json(out_file=fdw, keyspace=keyspace_name, column_families=[table_name])

        debug('{} sstabledump saved into {}'.format(node_owner, data_json_path))

        with open(data_json_path, 'r') as fdr:
            dump = fdr.readlines()
        # Why the "position" info is removing:
        # On disk the partitions are sorted in order of this hashed key.
        # The output of the position key is just referring to where in the sstable's data file its located
        # (decompressed byte offset).
        # This value is meaningless but may be different after migration in Cassandra then in Scylla
        # TODO: add mechanism to order the dump by pk and ck columns
        data_json = json.dumps(''.join([line for line in dump if '"position"' not in line]), sort_keys=True)

        # Re-write the file with data without "position"
        os.remove(data_json_path)
        with open(data_json_path, 'a') as fdw:
            for line in data_json.split('\\n'):
                fdw.write(line.replace('\\"', '"') + '\n')

        return data_json, data_json_path


versions = ['2_1_x', '2_2_x', '3_0_mc', '3_0_md']
for version in versions:
    cls_name = ('TestMigration_with_' + version)
    vars()[cls_name] = type(cls_name, (TestMigration,), {'version': version, '__test__': True})

    cls_name = ('TestMigrationUpgradeSSTables_with_' + version)
    vars()[cls_name] = type(cls_name, (TestMigrationUpgradeSSTables,), {'version': version, '__test__': True})
