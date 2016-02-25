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

# ######################## Helper functions ####################################\
    def run_basic_migration_test(self, migrated_files_dir, row_content, compression=None):
        cluster = self.cluster

        self.populate_cluster(cluster)
        self.start_cluster(cluster)
        node1 = self.get_node(cluster, 0)

        self.create_ks_and_cf(node1, columns={'c1': 'text', 'c2': 'text'}, compression=compression)
        self.load_migrated_tables(node1, migrated_files_dir)

        debug("Checking rows on node1...")
        query="SELECT COUNT(*) FROM cf"
        statement = SimpleStatement(query)
        s = self.patient_cql_connection(node1, 'ks')
        result = list(s.execute(statement))
        self.assertEqual(result[0].count, 1, len(result))

        debug("Checking rows content on node1...")
        query="SELECT * FROM ks.cf"
        statement = SimpleStatement(query)
        s = self.patient_cql_connection(node1, 'ks')
        result = list(s.execute(statement))
        self.assertEqual(result[0].key, row_content['key'], "check partition key")
        self.assertEqual(result[0].c1, row_content['c1'], "check column c1")
        self.assertEqual(result[0].c2, row_content['c2'], "check column c2")

    def create_ks_and_cf(self, node, columns, compression):
        debug("Creating a CQL connection...")
        session = self.patient_cql_connection(node)

        debug("Creating a keyspace 'ks'...")
        self.create_ks(session, 'ks', 1)

        debug("Creating a column family 'cf'...")
        self.create_cf(session, 'cf', read_repair=0.0, columns=columns, compression=compression)

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


