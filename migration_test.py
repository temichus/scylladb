import os
import re
import shutil
import time
import uuid
import subprocess
import glob
import datetime

from cassandra.query import SimpleStatement

from dtest import Tester, debug
from tools import require, rows_to_list, safe_mkdtemp
from nose import tools


@tools.nottest
class MigrationTestBase(Tester):

    def migrate_sstable_without_compression_test(self):
        self._run_basic_migration_test("without_compression", {'key': 'abc', 'c1': None, 'c2': 'cde'})

    def migrate_sstable_with_lz4_compression_test(self):
        self._run_basic_migration_test('with_lz4_compression', {'key': 'a', 'c1': 'abc', 'c2': 'cde'}, compression='LZ4')

    def migrate_sstable_with_compact_storage_test(self):
        self._run_basic_migration_test('with_compact_storage', {'key': 'a', 'c1': 'abc', 'c2': 'cde'}, compact_storage=True)

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

    def migrate_sstable_with_range_tombstone_test(self):
        # Content generated with:
        # INSERT INTO ks.cf (key, c1, c2) VALUES ('a', 'abc', 'cde');
        # INSERT INTO ks.cf (key, c1, c2) VALUES ('b', 'abc', 'cde');
        # INSERT INTO ks.cf (key, c1, c2) VALUES ('c', 'abc', 'cde');
        # nodetool flush
        # DELETE FROM ks.cf WHERE key IN ('a', 'b');
        self._run_basic_migration_test('with_range_tombstone', {'key': 'c', 'c1': 'abc', 'c2': 'cde'})

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
        self._run_migration_test_for_collection("with_collection_set", "set<text>", {'a': {'hello world', 'scylla', 'scylladb', 'test'}})

    def migrate_sstable_with_collection_list_test(self):
        self._run_migration_test_for_collection("with_collection_list", "list<text>", {'a': ['scylladb', 'scylla', 'hello world', 'test']})

    def migrate_sstable_with_collection_map_test(self):
        # CREATE COLUMNFAMILY ks.cf (key varchar PRIMARY KEY, messages map<varchar, text>)
        # INSERT INTO ks.cf (key, messages) VALUES ( 'a', { 'a':'value1', 'b':'value2' });
        self._run_migration_test_for_collection("with_collection_map", "map<varchar, text>", {'a': {'a': 'value1', 'b': 'value2'}})

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
        self.load_migrated_tables(node1, 'to_check_consistency/1')
        self.check_number_of_rows(node1, 1)

        # read key1 content for it to be cached
        result = self.get_all_rows_for_check(node1)
        self.assertEqual(result[0].p1, 'key1', "check partition key")
        self.assertEqual(result[0].r1, 1, "check value")

        # load row key1 with value 2
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

        debug('Run stress test on node1')
        profile_path = os.path.join(os.path.dirname(__file__),
                                    'test_data/c-s-profiles/cassandra-stress-custom-large-row-num-1.yaml')
        node1.stress(['user', 'profile={}'.format(profile_path), 'ops(insert=1)', 'n=1000000', '-rate', 'threads=4'],
                     capture_output=True)

        session = self.patient_cql_connection(node1)
        rows = rows_to_list(session.execute('SELECT count(*) FROM keyspace1.standard1;', timeout=300.0))
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
        data_files = glob.glob(os.path.join(data_dir, '*.*'))
        for data_file in data_files:
            shutil.copy2(data_file, os.path.join(dir, os.path.basename(data_file)))

        debug('Remove sstables and commit log for node1')
        shutil.rmtree(os.path.join(node1.get_path(), 'commitlogs'))
        for data_file in data_files:
            os.unlink(data_file)

        debug('Start node1')
        node1.start(wait_other_notice=True)
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
        rows = rows_to_list(session.execute('SELECT count(*) FROM keyspace1.standard1;', timeout=300.0))
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
            self.assertEqual(str(result[i].ablob).encode(
                'hex'), '0000000000000003', "check blob column")
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
            self.assertEqual(result[i].avarchar, unicode(
                "tzachvarchar"), "check varchar column")
            self.assertEqual(result[i].avarint, 17, "check varint column")
            self.assertEqual(result[i].alist, [1, 2, 3], "check list column")
            self.assertEqual(result[i].amap, {1: 2}, "check map column")
            self.assertEqual(result[i].aset, {1, 2, 3, 4}, "check set column")
    # ######################## Helper functions ####################################
    def check_number_of_rows(self, node, expected_number_of_rows):
        debug("Checking rows on node1...")
        query = "SELECT COUNT(*) FROM cf"
        statement = SimpleStatement(query)
        s = self.patient_cql_connection(node, 'ks')
        result = list(s.execute(statement))
        self.assertEqual(result[0].count, expected_number_of_rows, len(result))

    def get_all_rows_for_check(self, node1):
        debug("Checking rows content on node1...")
        query = "SELECT * FROM ks.cf"
        statement = SimpleStatement(query)
        s = self.patient_cql_connection(node1, 'ks')
        return list(s.execute(statement))

    def _run_basic_migration_test(self, migrated_files_dir, row_content, compression=None, compact_storage=False, sleep=0):
        node1 = self.start_cluster_and_get_node1()
        self.create_ks_and_cf(node1, columns={'c1': 'text', 'c2': 'text'}, compression=compression, compact_storage=compact_storage)
        self.load_migrated_tables(node1, migrated_files_dir)

        time.sleep(sleep)

        expected_keys = 1
        if row_content is None:
            expected_keys = 0
        self.check_number_of_rows(node1, expected_keys)

        if row_content is not None:
            result = self.get_all_rows_for_check(node1)
            self.assertEqual(result[0].key, row_content['key'], "check partition key")
            self.assertEqual(result[0].c1, row_content['c1'], "check column c1")
            self.assertEqual(result[0].c2, row_content['c2'], "check column c2")

    def _run_migration_test_for_collection(self, migration_dir_name, collection_type, collection_content):
        node1 = self.start_cluster_and_get_node1()

        # CREATE COLUMNFAMILY ks.cf (key varchar PRIMARY KEY, messages collection_type<text>)
        self.create_ks_and_cf(node1, {'messages': '{}'.format(collection_type)}, None, False)
        self.load_migrated_tables(node1, migration_dir_name)

        self.check_number_of_rows(node1, len(collection_content))

        result = self.get_all_rows_for_check(node1)
        idx = 0
        for key, value in collection_content.iteritems():
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
        if isinstance(query, basestring):
            session.execute(query)
            time.sleep(0.2)
        elif query is not None:
            for q in query:
                session.execute(q)
                time.sleep(0.2)
        else:
            self.create_cf(session, 'cf', read_repair=0.0, columns=columns, compression=compression, compact_storage=compact_storage)

        debug("Flushing a keyspace...")
        node.nodetool("flush -- ks")

    def get_cassandra_sstable_dir(self, version, migrated_files_dir):
        return "{}/cassandra-sstables/migration/{}/{}".format(os.path.dirname(os.path.realpath(__file__)), version,
                                                              migrated_files_dir)

    def load_migrated_tables(self, node, migrated_files_dir, ks='ks', cf='cf', version='2_1_x'):
        cassandra_sstable_dir = self.get_cassandra_sstable_dir(version, migrated_files_dir)
        debug("cassandra sstable dir is {}".format(cassandra_sstable_dir))

        ks_dir = os.path.join(self.test_path, 'test', 'node1', 'data', ks)
        cf_dir = self.get_cf_dir(ks_dir, cf)
        debug("Column family directory is {}".format(cf_dir))

        debug("Copying sstables created by Cassandra...")
        self.copy_files_to(cassandra_sstable_dir, cf_dir)

        debug("Running 'nodetool refresh -- {} {}' to load migrated sstables".format(ks, cf))
        node.nodetool("refresh -- {} {}".format(ks, cf))

    def populate_cluster(self, cluster):
        # Disable hinted handoff and set batch commit log so this doesn't
        # interfere with the test (this must be after the populate)
        cluster.set_configuration_options(values={'hinted_handoff_enabled': False}, batch_commitlog=True)
        debug("Starting a cluster of one node...")
        cluster.populate(1)

    def start_cluster(self, cluster):
        cluster.start()

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

#
# Dtest created to test migration of data from C* to Scylla
#


@tools.istest
class TestMigration(MigrationTestBase):

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
        self.populate_cluster(cluster)
        node1 = self.cluster.nodelist()[0]
        node1.set_configuration_options()
        self.start_cluster(cluster)

        query = "CREATE TABLE ks.cf " \
                "(first_name varchar, last_name varchar, cnt counter, PRIMARY KEY(first_name, last_name));"
        self.create_ks_and_cf(node1, None, None, False, query=query)
        self.load_migrated_tables(node1, 'with_counter')

        debug("Checking rows content...")
        self.check_number_of_rows(node1, 3)
        rows = self.get_all_rows_for_check(node1)
        self.assertEqual(rows[0].first_name, 'albert')
        self.assertEqual(rows[0].last_name, 'einstein')
        self.assertEqual(rows[0].cnt, 6)
        self.assertEqual(rows[1].first_name, 'thomas')
        self.assertEqual(rows[1].last_name, 'edison')
        self.assertEqual(rows[1].cnt, 2)
        self.assertEqual(rows[2].first_name, 'marie')
        self.assertEqual(rows[2].last_name, 'curie')
        self.assertEqual(rows[2].cnt, 3)

        debug("Change counters...")
        conn = self.patient_cql_connection(node1, 'ks')
        st = SimpleStatement("UPDATE ks.cf "
                             "SET cnt = cnt + 5 WHERE first_name = \'albert\' and last_name = \'einstein\';")
        conn.execute(st)
        st = SimpleStatement("UPDATE ks.cf SET cnt = cnt - 1 WHERE first_name=\'thomas\' and last_name=\'edison\';")
        conn.execute(st)
        node1.nodetool("flush -- ks")

        debug("Checking rows content...")
        rows = self.get_all_rows_for_check(node1)
        self.assertEqual(rows[0].cnt, 11)
        self.assertEqual(rows[1].cnt, 1)
        self.assertEqual(rows[2].cnt, 3)

    @require('#2458')
    def migrate_sstable_with_old_format_counter_test(self):
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
        self.allow_log_errors = True
        cluster = self.cluster
        self.populate_cluster(cluster)
        node1 = self.cluster.nodelist()[0]
        node1.set_configuration_options()
        self.start_cluster(cluster)

        query = "CREATE TABLE ks.cf (pk int PRIMARY KEY, cnt COUNTER);"
        self.create_ks_and_cf(node1, None, None, False, query=query)
        self.load_migrated_tables(node1, 'with_old_format_counter')

        debug("Checking counters data...")
        rows = self.get_all_rows_for_check(node1)
        self.assertEqual(len(rows), 20)

        debug('Try to create a new counters table...')
        conn = self.patient_cql_connection(node1, 'ks')
        conn.execute(SimpleStatement("CREATE TABLE ks.cf_new (pk int PRIMARY KEY, cnt COUNTER);"))
        for i in range(1, 11):
            query = "UPDATE ks.cf_new SET cnt = cnt + {} WHERE pk={};".format(i, i)
            conn.execute(SimpleStatement(query))
        res = conn.execute(SimpleStatement("SELECT * FROM ks.cf_new"))
        rows = rows_to_list(res)
        self.assertEqual(len(rows), 10)

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
            self.recursive_copy_to(os.path.join(cassandra_dir, 'system_traces'), os.path.join(scylla_dir, 'system_traces'))
