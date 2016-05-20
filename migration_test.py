import os
import re
import shutil
import time
import uuid

from unittest import skip

from cassandra.query import SimpleStatement

from dtest import Tester, debug
from nose import tools

@tools.nottest
class MigrationTestBase(Tester):
    def migrate_sstable_without_compression_test(self):
        self._run_basic_migration_test("without_compression", {'key':'abc','c1':None,'c2':'cde'})

    def migrate_sstable_with_lz4_compression_test(self):
        self._run_basic_migration_test('with_lz4_compression', {'key':'a','c1':'abc','c2':'cde'}, compression='LZ4')

    def migrate_sstable_with_compact_storage_test(self):
        self._run_basic_migration_test('with_compact_storage', {'key':'a','c1':'abc','c2':'cde'}, compact_storage=True)

    def migrate_sstable_with_expired_ttl_test(self):
        # Data inserted in c* with the following query: INSERT INTO ks.cf (key, c1, c2) VALUES ('a', 'abc', 'cde') USING TTL 1;
        # Expect no keys because the only one inserted is expired.
        self._run_basic_migration_test('with_expired_ttl', None)

    def migrate_sstable_with_cell_tombstone_test(self):
        # Content generated with:
        # INSERT INTO ks.cf (key, c1, c2) VALUES ('a', 'abc', 'cde');
        # nodetool flush
        # DELETE c2 FROM ks.cf where key = 'a';
        self._run_basic_migration_test('with_cell_tombstone', {'key':'a','c1':'abc','c2':None})

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
        self._run_basic_migration_test('with_range_tombstone', {'key':'c','c1':'abc','c2':'cde'})

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
        self.assertEqual(result[0].ck2, 'aaa',"check partition key")
        self.assertEqual(result[0].data, 'fff', "check data") 

    def migrate_sstable_with_user_defined_types_tests(self):
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


# ######################## Helper functions ####################################
    def check_number_of_rows(self, node, expected_number_of_rows):
        debug("Checking rows on node1...")
        query="SELECT COUNT(*) FROM cf"
        statement = SimpleStatement(query)
        s = self.patient_cql_connection(node, 'ks')
        result = list(s.execute(statement))
        self.assertEqual(result[0].count, expected_number_of_rows, len(result))

    def get_all_rows_for_check(self, node1):
        debug("Checking rows content on node1...")
        query="SELECT * FROM ks.cf"
        statement = SimpleStatement(query)
        s = self.patient_cql_connection(node1, 'ks')
        return list(s.execute(statement))

    def _run_basic_migration_test(self, migrated_files_dir, row_content, compression=None, compact_storage=False):
        node1 = self.start_cluster_and_get_node1()
        self.create_ks_and_cf(node1, columns={'c1': 'text', 'c2': 'text'}, compression=compression, compact_storage=compact_storage)
        self.load_migrated_tables(node1, migrated_files_dir)

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

    def get_cassandra_sstable_dir(self, migrated_files_dir):
        return "{}/cassandra-sstables/migration/{}".format(os.path.dirname(os.path.realpath(__file__)), migrated_files_dir)

    def load_migrated_tables(self, node, migrated_files_dir):
        cassandra_sstable_dir = self.get_cassandra_sstable_dir(migrated_files_dir)
        debug("cassandra sstable dir is {}".format(cassandra_sstable_dir))

        ks_dir = os.path.join(self.test_path, 'test', 'node1', 'data', 'ks')
        cf_dir = self.get_cf_dir(ks_dir, 'cf')
        debug("Column family directory is {}".format(cf_dir))

        debug("Copying sstables created by Cassandra...")
        self.copy_files_to(cassandra_sstable_dir, cf_dir)

        debug("Running 'nodetool refresh -- ks cf' to load migrated sstables")
        node.nodetool("refresh -- ks cf")

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
    @skip('not impled')
    def migrate_sstable_with_counter_test(self):
        cluster = self.cluster

        self.populate_cluster(cluster)
        self.copy_migrated_data_dir('with_counter')
        self.start_cluster(cluster)

        # FIXME: Check row content when counter gets supported.

    @skip('failing')
    def migrate_sstable_with_schema_change_test(self):
        # Content of Cassandra dir generated with following cql commands:
        # CREATE TABLE ks.cf (user_name varchar PRIMARY KEY, bio ascii);
        # INSERT INTO ks.cf (user_name, bio) VALUES ('a', 'test');
        # ALTER TABLE ks.cf ADD age int;
        # INSERT INTO ks.cf (user_name, bio, age) VALUES ('b', 'test', 0);

        cluster = self.cluster

        self.populate_cluster(cluster)
        self.copy_migrated_data_dir('with_schema_change')
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
        query="INSERT INTO ks.cf (user_name, bio, age) VALUES ('c', 'test', 0)"
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

    ## Helpers
    def copy_migrated_data_dir(self, migrated_data_dir):
        cassandra_dir = "{}/data".format(self.get_cassandra_sstable_dir(migrated_files_dir))
        debug("cassandra data dir for counter is {}".format(cassandra_dir))

        scylla_dir = os.path.join(self.test_path, 'test', 'node1', 'data')
        debug("Node data directory is {}".format(scylla_dir))

        debug("Copying data/ks created by Cassandra...")
        self.recursive_copy_to(os.path.join(cassandra_dir, 'ks'), os.path.join(scylla_dir, 'ks'))
        debug("Copying data/system created by Cassandra...")
        self.recursive_copy_to(os.path.join(cassandra_dir, 'system'), os.path.join(scylla_dir, 'system'))
        debug("Copying data/system_traces created by Cassandra...")
        self.recursive_copy_to(os.path.join(cassandra_dir, 'system_traces'), os.path.join(scylla_dir, 'system_traces'))
