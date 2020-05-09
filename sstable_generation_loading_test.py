from distutils import dir_util
import os
import subprocess
import time

from dtest import Tester, debug
from ccmlib import common as ccmcommon
from nose.plugins.attrib import attr


@attr('dtest-full')
class TestSSTableGenerationAndLoading(Tester):

    def __init__(self, *argv, **kwargs):
        kwargs['cluster_options'] = {'start_rpc': 'true'}
        super(TestSSTableGenerationAndLoading, self).__init__(*argv, **kwargs)

    @attr('single_node')
    def promoted_index_generation_with_small_partition_followed_by_a_large_partition_test(self):
        """
        Tests for https://github.com/scylladb/scylla/issues/1567
        """
        cluster = self.cluster
        cluster.set_configuration_options(values={'enable_cache': False})
        cluster.populate(1).start()
        node1 = cluster.nodelist()[0]

        session = self.patient_cql_connection(node1)
        self.create_ks(session, 'ks', 1)

        session.execute('CREATE TABLE ks.test (pk int, ck text, s1 int static, v int, PRIMARY KEY (pk, ck))')
        session.execute('insert into ks.test (pk, s1) values (1, 7)')
        session.execute('insert into ks.test (pk, s1) values (0, 7)')

        for i in range(2000):
            session.execute("insert into ks.test  (pk, ck, v) values (0, 'ck_%d', %d)" % (i, i))

        node1.stop()
        node1.start(wait_for_binary_proto=True)

        session = self.patient_cql_connection(node1)

        rows = list(session.execute('select * from ks.test where pk = 0 and ck = \'ck_0\''))
        assert(len(rows) == 1)
        self.assertEquals([0, 'ck_0', 7, 0], list(rows[0]))

        # Fails due to https://github.com/scylladb/scylla/issues/1568
        # rows = list(session.execute('select * from ks.test where pk = 0 and ck = \'ck_45\''))
        # assert(len(rows) == 1)
        # self.assertEquals([0, 'ck_45', 7, 45], list(rows[0]))

    @attr('single_node')
    def incompressible_data_in_compressed_table_test(self):
        """
        tests for the bug that caused #3370:
        https://issues.apache.org/jira/browse/CASSANDRA-3370

        inserts random data into a compressed table. The compressed SSTable was
        compared to the uncompressed and was found to indeed be larger then
        uncompressed.
        """
        cluster = self.cluster
        cluster.populate(1).start()
        node1 = cluster.nodelist()[0]
        time.sleep(.5)

        session = self.patient_cql_connection(node1)
        self.create_ks(session, 'ks', 1)
        self.create_cf(session, 'cf', compression="Deflate")

        # make unique column names, and values that are incompressible
        for col in range(10):
            col_name = str(col)
            col_val = os.urandom(5000)
            col_val = col_val.hex()
            cql = "UPDATE cf SET v='%s' WHERE KEY='0' AND c='%s'" % (col_val, col_name)
            # print cql
            session.execute(cql)

        node1.flush()
        time.sleep(2)
        rows = list(session.execute("SELECT * FROM cf WHERE KEY = '0' AND c < '8'"))
        assert len(rows) > 0

    @attr('single_node')
    def remove_index_file_test(self):
        """
        tests for situations similar to that found in #343:
        https://issues.apache.org/jira/browse/CASSANDRA-343
        """
        cluster = self.cluster
        cluster.populate(1).start(wait_for_binary_proto=True)
        node1 = cluster.nodelist()[0]

        # Makinge sure the cluster is ready to accept the subsequent
        # stress connection. This was an issue on Windows.
        debug("Writing initial data")
        node1.stress(['write', 'n=10000', '-rate', 'threads=8'])

        # Query existing data and keep in original_rows
        session = self.patient_cql_connection(node1)
        stress_table = 'keyspace1.standard1'
        debug("Retrieving initial data")
        original_rows = list(session.execute("SELECT * FROM %s" % (stress_table,)))

        debug("Stopping node and removing summary")
        node1.flush()
        node1.compact()
        node1.stop()
        time.sleep(1)
        path = ""
        basepath = os.path.join(node1.get_path(), 'data', 'keyspace1')
        for x in os.listdir(basepath):
            if x.startswith("standard1"):
                path = os.path.join(basepath, x)

        # Verify that Summary can be regenerated
        # and that the data is still there
        os.system('rm %s/*Summary.db' % path)

        debug("Starting node")
        node1.start(wait_for_binary_proto=True)
        session = self.patient_cql_connection(node1)
        debug("Verifying data")
        new_rows = list(session.execute("SELECT * FROM %s" % (stress_table,)))
        self.assertEquals(original_rows, new_rows)

        debug("Stopping node")
        node1.stop()
        time.sleep(1)
        os.system('rm -rf %s/snapshots' % path)
        os.system('mkdir %s/snapshots' % path)

        self.ignore_log_patterns += [r"database - Exception while populating keyspace 'keyspace1' with column family 'standard1' from file '.*': "
                                      "sstables::malformed_sstable_exception \(.*: file not found\)",
                                     r"database - Exception while populating keyspace 'keyspace1' with column family 'standard1' from file '.*': "
                                      "sstables::malformed_sstable_exception \(.*: No such file or directory\)",
                                     r"database - Exception while populating keyspace 'keyspace1' with column family 'standard1' from file '.*': "
                                      "std::filesystem::__cxx11::filesystem_error \(error system:2, filesystem error: (open|stat) failed: No such file or directory \[.*\]\)",
                                     r"database - Unrecognized error while processing .*: std::filesystem::__cxx11::filesystem_error "
                                      "\(error system:2, filesystem error: (open|stat) failed: No such file or directory \[.*\]\)",
                                     r"database - malformed sstable .*: .*: file not found",
                                     r"database - malformed sstable .*: .*: No such file or directory",
                                     r"init - Startup failed: std::runtime_error"
                                    ]

        timeout = 10 if cluster.scylla_mode != 'debug' else 90

        # For each of these component files, verify that if it's removed
        # then the sstable is is detected is malformed but the data
        # file is not lost
        comps = ['Index.db', 'Filter.db', 'Statistics.db', 'Digest.*']
        for comp in comps:
            debug("Removing {comp}".format(**locals()))
            os.system("mv {path}/*{comp} {path}/snapshots/".format(**locals()))

            debug("Starting node, expected to fail")
            mark = node1.mark_log()
            node1.start(no_wait=True)
            node1.watch_log_for("malformed_sstable_exception", timeout=timeout, from_mark=mark)
            debug("Stopping node")
            node1.stop(wait=False, gently=False)
            time.sleep(1)

            data_found = 0
            for fname in os.listdir(path):
                if fname.endswith('Data.db'):
                    data_found += 1
            assert data_found > 0, "After removing %s, the data file was deleted!" % comp

            os.system("mv {path}/snapshots/*{comp} {path}/".format(**locals()))

        # Finally, verify that the data is still there after renaming
        # all components back.
        debug("Starting node")
        node1.start(wait_for_binary_proto=True)
        session = self.patient_cql_connection(node1)
        debug("Verifying data")
        new_rows = list(session.execute("SELECT * FROM %s" % (stress_table,)))
        self.assertEquals(original_rows, new_rows)

    def sstableloader_compression_none_to_none_test(self):
        self.load_sstable_with_configuration(None, None)

    def sstableloader_compression_none_to_snappy_test(self):
        self.load_sstable_with_configuration(None, 'Snappy')

    def sstableloader_compression_none_to_deflate_test(self):
        self.load_sstable_with_configuration(None, 'Deflate')

    def sstableloader_compression_snappy_to_none_test(self):
        self.load_sstable_with_configuration('Snappy', None)

    def sstableloader_compression_snappy_to_snappy_test(self):
        self.load_sstable_with_configuration('Snappy', 'Snappy')

    def sstableloader_compression_snappy_to_deflate_test(self):
        self.load_sstable_with_configuration('Snappy', 'Deflate')

    def sstableloader_compression_deflate_to_none_test(self):
        self.load_sstable_with_configuration('Deflate', None)

    def sstableloader_compression_deflate_to_snappy_test(self):
        self.load_sstable_with_configuration('Deflate', 'Snappy')

    def sstableloader_compression_deflate_to_deflate_test(self):
        self.load_sstable_with_configuration('Deflate', 'Deflate')

    def load_sstable_with_configuration(self, pre_compression=None, post_compression=None):
        """
        tests that the sstableloader works by using it to load data.
        Compression of the columnfamilies being loaded, and loaded into
        can be specified.

        pre_compression and post_compression can be these values:
        None, 'Snappy', or 'Deflate'.
        """
        NUM_KEYS = 1000

        for compression_option in (pre_compression, post_compression):
            assert compression_option in (None, 'Snappy', 'Deflate')

        debug("Testing sstableloader with pre_compression=%s and post_compression=%s" % (pre_compression, post_compression))

        cluster = self.cluster
        cluster.set_configuration_options(values={'experimental': True})
        cluster.populate(2).start(wait_for_binary_proto=True, wait_other_notice=True)
        node1, node2 = cluster.nodelist()
        time.sleep(.5)

        def create_schema(session, compression):
            self.create_ks(session, "ks", rf=2)
            self.create_cf(session, "standard1", compression=compression)
            self.create_cf(session, "counter1", compression=compression, columns={'v': 'counter'})

        debug("creating keyspace and inserting")
        session = self.cql_connection(node1)
        create_schema(session, pre_compression)

        for i in range(NUM_KEYS):
            session.execute("UPDATE standard1 SET v='%d' WHERE KEY='%d' AND c='col'" % (i, i))
            session.execute("UPDATE counter1 SET v=v+1 WHERE KEY='%d'" % i)

        node1.nodetool('drain')
        node1.stop()
        node2.nodetool('drain')
        node2.stop()

        debug("Making a copy of the sstables")
        # make a copy of the sstables
        data_dir = os.path.join(node1.get_path(), 'data')
        copy_root = os.path.join(node1.get_path(), 'data_copy')
        for ddir in os.listdir(data_dir):
            keyspace_dir = os.path.join(data_dir, ddir)
            if os.path.isdir(keyspace_dir) and ddir != 'system':
                copy_dir = os.path.join(copy_root, ddir)
                dir_util.copy_tree(keyspace_dir, copy_dir)

        debug("Wiping out the data and restarting cluster")
        # wipe out the node data.
        cluster.clear()
        cluster.start(wait_for_binary_proto=True, wait_other_notice=True)

        debug("re-creating the keyspace and column families.")
        session = self.cql_connection(node1)
        create_schema(session, post_compression)
        time.sleep(2)

        debug("Calling sstableloader")
        # call sstableloader to re-load each cf.
        host = node1.address()
        sstablecopy_dir = copy_root + '/ks'
        for cf_dir in os.listdir(sstablecopy_dir):
            full_cf_dir = os.path.join(sstablecopy_dir, cf_dir)
            if os.path.isdir(full_cf_dir):
                cmd_args = [node1.get_tool('sstableloader'), '--nodes', host, full_cf_dir]
                p = subprocess.Popen(cmd_args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                exit_status = p.wait()
                self.assertEqual(0, exit_status,
                                 "sstableloader exited with a non-zero status: %d" % exit_status)

        def read_and_validate_data(session):
            for i in range(NUM_KEYS):
                rows = list(session.execute("SELECT * FROM standard1 WHERE KEY='%d'" % i))
                self.assertEquals([str(i), 'col', str(i)], list(rows[0]))
                rows = list(session.execute("SELECT * FROM counter1 WHERE KEY='%d'" % i))
                self.assertEquals([str(i), 1], list(rows[0]))

        debug("Reading data back")
        # Now we should have sstables with the loaded data, and the existing
        # data. Lets read it all to make sure it is all there.
        read_and_validate_data(session)

        debug("compacting, and repairing")
        # do some operations and try reading the data again.
        node1.nodetool('compact')
        node1.nodetool('repair')

        debug("Reading data back one more time")
        read_and_validate_data(session)
