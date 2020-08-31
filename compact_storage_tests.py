import os
import distutils.dir_util
import re

from dtest import Tester, debug
from nose.plugins.attrib import attr


@attr('dtest-full', 'single_node')
class TestCompactStorage(Tester):
    row_size = 1000

    def get_cf_dir(self, ks_dir, cf_name):
        """
        Return the first CF directory for a CF with a given name
        """
        cf_pattern = re.compile("{}-".format(cf_name))
        for root, dirs, files in os.walk(ks_dir):
            for d in dirs:
                if cf_pattern.match(d):
                    return os.path.join(root, d)

    def load_and_read_from_sstables(self, data_dir, lines):
        cluster = self.cluster
        cluster.populate(1).start(wait_for_binary_proto=True)
        node1 = cluster.nodelist()[0]
        session = self.patient_cql_connection(node1)
        session.execute("CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};")
        session.execute("CREATE TABLE ks.tb (key1 int, key2 int, val blob, PRIMARY KEY (key1,key2)) WITH COMPACT STORAGE;")

        node1.stop()

        dst1 = self.get_cf_dir(os.path.join(node1.get_path(), 'data', 'ks'), 'tb')
        src1 = os.path.join("test_data", data_dir)

        distutils.dir_util.copy_tree(src1, dst1)

        node1.start(wait_for_binary_proto=True)
        session = self.patient_cql_connection(node1)

        result = list(session.execute("SELECT key2 FROM ks.tb"))

        self.assertEqual(len(result), lines, len(result))

    def read_old_format_wide_row_data_test(self):
        self.load_and_read_from_sstables("scylla_compact_storage_wide_partition_old_format", self.row_size - 200)

    @attr('next-gating')
    @attr('dtest-debug')
    def read_cassandra_wide_row_data_test(self):
        self.load_and_read_from_sstables("cassandra_compact_storage_wide_partition", self.row_size - 100)

    @attr('next-gating')
    @attr('dtest-debug')
    def wide_row_test(self):
        cluster = self.cluster
        cluster.populate(1).start(wait_for_binary_proto=True)
        node1 = cluster.nodelist()[0]
        session = self.patient_cql_connection(node1)

        session.execute("CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};")
        session.execute("CREATE TABLE ks.tb (key1 int, key2 int, val blob, PRIMARY KEY (key1,key2)) WITH COMPACT STORAGE;")

        blob = ("a" * 10000)

        insert = session.prepare("INSERT INTO ks.tb (key1,key2,val) values (1,?,textAsBlob(?));")
        for key2 in range(0, self.row_size):
            session.execute(insert, (key2, blob))

        node1.flush()
        for key2 in range(0, 100):
            session.execute("delete from ks.tb where key1=1 and key2 = %d" % (key2*10))

        result = list(session.execute("SELECT * FROM ks.tb"))
        self.assertEqual(len(result), self.row_size-100, len(result))

        node1.flush()
        node1.stop()
        node1.start(wait_for_binary_proto=True)

        session = self.patient_cql_connection(node1)
        result = list(session.execute("SELECT * FROM ks.tb"))
        self.assertEqual(len(result), self.row_size-100, len(result))
