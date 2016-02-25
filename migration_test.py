import os
import re
import shutil
import time

from cassandra.query import SimpleStatement

from dtest import Tester, debug

#
# Dtest created to test migration of data from C* to Scylla
#
class TestMigration(Tester):
    def migrate_sstable_without_compression_test(self):
        self.run_basic_migration_test("without_compression", {'key':'abc','c1':None,'c2':'cde'})

    def migrate_sstable_with_lz4_compression_test(self):
        self.run_basic_migration_test('with_lz4_compression', {'key':'a','c1':'abc','c2':'cde'}, compression='LZ4')

    def migrate_sstable_with_compact_storage_test(self):
        self.run_basic_migration_test('with_compact_storage', {'key':'a','c1':'abc','c2':'cde'}, compact_storage=True)

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
        self.run_migration_test_for_collection("with_collection_set", "set", {'hello world', 'scylla', 'scylladb', 'test'})

    def migrate_sstable_with_collection_list_test(self):
        self.run_migration_test_for_collection("with_collection_list", "list", ['scylladb', 'scylla', 'hello world', 'test'])

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

    def run_basic_migration_test(self, migrated_files_dir, row_content, compression=None, compact_storage=False):
        node1 = self.start_cluster_and_get_node1()
        self.create_ks_and_cf(node1, columns={'c1': 'text', 'c2': 'text'}, compression=compression, compact_storage=compact_storage)
        self.load_migrated_tables(node1, migrated_files_dir)

        self.check_number_of_rows(node1, 1)

        result = self.get_all_rows_for_check(node1)
        self.assertEqual(result[0].key, row_content['key'], "check partition key")
        self.assertEqual(result[0].c1, row_content['c1'], "check column c1")
        self.assertEqual(result[0].c2, row_content['c2'], "check column c2")

    def run_migration_test_for_collection(self, migration_dir_name, collection_type, collection_content):
        node1 = self.start_cluster_and_get_node1()

        # CREATE COLUMNFAMILY ks.cf (key varchar PRIMARY KEY, messages collection_type<text>)
        self.create_ks_and_cf(node1, {'messages': '{}<text>'.format(collection_type)}, None, False)
        self.load_migrated_tables(node1, migration_dir_name)

        self.check_number_of_rows(node1, 1)

        result = self.get_all_rows_for_check(node1)
        self.assertEqual(result[0].key, 'a', "check partition key")
        self.assertEqual(result[0].messages, collection_content, "check messages")

    def create_ks_and_cf(self, node, columns, compression, compact_storage):
        debug("Creating a CQL connection...")
        session = self.patient_cql_connection(node)

        debug("Creating a keyspace 'ks'...")
        self.create_ks(session, 'ks', 1)

        debug("Creating a column family 'cf'...")
        self.create_cf(session, 'cf', read_repair=0.0, columns=columns, compression=compression, compact_storage=compact_storage)

        debug("Flushing a keyspace...")
        node.nodetool("flush -- ks")

    def load_migrated_tables(self, node, migrated_files_dir):
        cassandra_sstable_dir = "{}/cassandra-sstables/migration/{}".format(os.path.dirname(os.path.realpath(__file__)), migrated_files_dir)
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


