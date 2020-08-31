import binascii
import glob
import os
import stat
import struct
import subprocess
import time

from unittest import skip

from cassandra import WriteTimeout
from cassandra.cluster import NoHostAvailable, OperationTimedOut

from ccmlib.common import is_win
from ccmlib.node import Node, TimeoutError
from assertions import assert_almost_equal, assert_none, assert_one, assert_row_count_in_select, \
    assert_row_count_in_select_less, assert_row_count, assert_all
from dtest import Tester, debug
from tools import since, rows_to_list
from nose.plugins.attrib import attr


@attr('dtest-full', 'single_node')
class TestCommitLog(Tester):
    """ CommitLog Tests """

    def __init__(self, *argv, **kwargs):
        kwargs['cluster_options'] = {'start_rpc': 'true'}
        super(TestCommitLog, self).__init__(*argv, **kwargs)

    def setUp(self):
        super(TestCommitLog, self).setUp()
        self.cluster.populate(1)
        [self.node1] = self.cluster.nodelist()

    def tearDown(self):
        self._change_commitlog_perms(stat.S_IWRITE | stat.S_IREAD | stat.S_IEXEC)
        super(TestCommitLog, self).tearDown()

    def prepare(self, configuration={}, create_test_keyspace=True, **kwargs):
        conf = {'commitlog_sync_period_in_ms': 1000}

        conf.update(configuration)
        self.cluster.set_configuration_options(values=conf, **kwargs)
        self.cluster.start()
        unknown_options = self.cluster.nodelist()[0].grep_log("config - Unknown option")
        if unknown_options:
            self.fail("Unknown option found! Please check the test! %s" % unknown_options)
        self.session1 = self.patient_cql_connection(self.node1)
        if create_test_keyspace:
            self.session1.execute("DROP KEYSPACE IF EXISTS ks;")
            self.create_ks(self.session1, 'ks', 1)
            self.session1.execute("DROP TABLE IF EXISTS test;")
            query = """
              CREATE TABLE test (
                key int primary key,
                col1 int
              )
            """
            self.session1.execute(query)

    def _change_commitlog_perms(self, mod):
        path = self._get_commitlog_path()
        os.chmod(path, mod)
        commitlogs = glob.glob(path + '/*')
        for commitlog in commitlogs:
            os.chmod(commitlog, mod)

    def _get_commitlog_path(self):
        """ Returns the commitlog path """

        return os.path.join(self.node1.get_path(), 'commitlogs')

    def _get_commitlog_files(self):
        """ Returns the number of commitlog files in the directory """

        path = self._get_commitlog_path()
        return [os.path.join(path, p) for p in os.listdir(path)]

    def _get_commitlog_size(self):
        """ Returns the commitlog directory size in MB """

        path = self._get_commitlog_path()
        cmd_args = ['du', '-m', path]
        p = subprocess.Popen(cmd_args, stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE)
        stdout, stderr = p.communicate()
        exit_status = p.returncode
        self.assertEqual(0, exit_status,
                         "du exited with a non-zero status: %d" % exit_status)
        size = int(stdout.split('\t')[0])
        return size

    def _segment_size_test(self, segment_size_in_mb, compressed=False):
        """ Execute a basic commitlog test and validate the commitlog files """

        conf = {'commitlog_segment_size_in_mb': segment_size_in_mb}
        if compressed:
            conf['commitlog_compression'] = [{'class_name': 'LZ4Compressor'}]
        conf['memtable_heap_space_in_mb'] = 512
        self.prepare(configuration=conf, create_test_keyspace=False)

        segment_size = segment_size_in_mb * 1024 * 1024
        self.node1.stress(['write', 'n=150000', '-rate', 'threads=25'])
        time.sleep(1)

        commitlogs = self._get_commitlog_files()
        self.assertTrue(len(commitlogs) > 0, "No commit log files were created")

        # the most recently-written segment of the commitlog may be smaller
        # than the expected size, so we allow exactly one segment to be smaller
        smaller_found = False
        for i, f in enumerate(commitlogs):
            size = os.path.getsize(f)
            size_in_mb = int(size / 1024 / 1024)
            debug('segment file {} {}; smaller already found: {}'.format(f, size_in_mb, smaller_found))
            if size_in_mb < 1 or size < (segment_size * 0.1):
                continue  # commitlog not yet used

            try:
                if compressed:
                    # if compression is used, we assume there will be at most a 50% compression ratio
                    self.assertLess(size, segment_size)
                    self.assertGreater(size, segment_size / 2)
                else:
                    # if no compression is used, the size will be close to what we expect
                    assert_almost_equal(size, segment_size, error=0.05)
            except AssertionError as e:
                #  the last segment may be smaller
                if not smaller_found:
                    self.assertLessEqual(size, segment_size)
                    smaller_found = True
                else:
                    raise e

    def _provoke_commitlog_failure(self):
        """ Provoke the commitlog failure """

        # Test things are ok at this point
        self.session1.execute("""
            INSERT INTO test (key, col1) VALUES (1, 1);
        """)
        assert_one(
            self.session1,
            "SELECT * FROM test where key=1;",
            [1, 1]
        )

        self._change_commitlog_perms(0)

        try:
            self.node1.stress(['write', 'n=10K', '-col', 'size=FIXED(1000)', '-rate', 'threads=25'])
        except:
            debug("Stress failed as expected")

    @attr('next-gating')
    @attr('dtest-debug')
    def test_commitlog_replay_on_startup(self):
        """ Test commit log replay """
        node1 = self.node1
        node1.set_configuration_options(batch_commitlog=True)
        node1.start(wait_for_binary_proto=True)

        debug("Insert data")
        session = self.patient_cql_connection(node1)
        self.create_ks(session, 'Test', 1)
        session.execute("""
            CREATE TABLE users (
                user_name varchar PRIMARY KEY,
                password varchar,
                gender varchar,
                state varchar,
                birth_year bigint
            );
        """)
        session.execute("INSERT INTO Test. users (user_name, password, gender, state, birth_year) "
                        "VALUES('gandalf', 'p@$$', 'male', 'WA', 1955);")

        debug("Verify data is present")
        session = self.patient_cql_connection(node1)
        res = session.execute("SELECT * FROM Test. users")
        self.assertCountEqual(rows_to_list(res),
                              [[u'gandalf', 1955, u'male', u'p@$$', u'WA']])

        debug("Stop node abruptly")
        node1.stop(gently=False)

        debug("Verify commitlog was written before abrupt stop")
        commitlog_dir = os.path.join(node1.get_path(), 'commitlogs')
        commitlog_files = os.listdir(commitlog_dir)
        self.assertTrue(len(commitlog_files) > 0)

        debug("Verify no SSTables were flushed before abrupt stop")
        data_dir = os.path.join(node1.get_path(), 'data')
        cf_id = [s for s in os.listdir(os.path.join(data_dir, "test")) if s.startswith("users")][0]
        cf_data_dir = glob.glob("{data_dir}/test/{cf_id}".format(**locals()))[0]
        cf_data_dir_files = os.listdir(cf_data_dir)
        for special_dir in ["backups", "upload", "staging"]:
            if special_dir in cf_data_dir_files:
                cf_data_dir_files.remove(special_dir)
        self.assertEqual(0, len(cf_data_dir_files))

        debug("Verify commit log was replayed on startup")
        node1.start(wait_for_binary_proto=False)
        self.assertTrue(node1.is_running(), "node is not running")
        node1.watch_log_for("Log replay complete")
        # Here we verify there was more than 0 replayed mutations
        zero_replays = node1.grep_log(" 0 replayed mutations", filter_expr='DEBUG')
        self.assertEqual(0, len(zero_replays))

        debug("Make query and ensure data is present")
        session = self.patient_cql_connection(node1)
        res = session.execute("SELECT * FROM Test. users")
        self.assertCountEqual(rows_to_list(res),
                              [[u'gandalf', 1955, u'male', u'p@$$', u'WA']])

    @attr('next-gating')
    @attr('dtest-debug')
    def test_commitlog_replay_with_alter_table(self):
        """
        Test commit log replay with alter table
        The goal of the test is to verify that commitlog replay works correctly even if the commitlog contains
        mutations written using old versions of the schema.
        Based on test_commitlog_replay_on_startup
        """

        node1 = self.node1
        node1.set_configuration_options(batch_commitlog=True)
        node1.start(wait_for_binary_proto=True)

        debug("Create table")
        session = self.patient_cql_connection(node1)
        self.create_ks(session, 'Test', 1)
        session.execute("""
            CREATE TABLE cf (
                pk1 int,
                ck1 int,
                r2 int,
                r3 text,
                r5 set<int>,
                PRIMARY KEY(pk1, ck1)
            );
        """)

        debug("Insert some data")
        n_partitions = 5
        for key in range(n_partitions):
            session.execute("INSERT INTO Test.cf (pk1, ck1, r2, r3, r5) VALUES(%d, 9, 8, 'seven', {6, 5});" % (key))
            session.execute("INSERT INTO Test.cf (pk1, ck1, r3) VALUES(%d, 8, 'eight');" % (key))

        debug("Flush")
        self.cluster.flush()

        debug("Insert more data and alter table")
        for key in range(n_partitions):
            session.execute("INSERT INTO Test.cf (pk1, ck1, r2, r3, r5) VALUES(%d, 0, 1, 'two', {3, 4});" % (key))
        session.execute("ALTER TABLE Test.cf ADD r1 int;")
        for key in range(n_partitions):
            session.execute("INSERT INTO Test.cf (pk1, ck1, r1, r2, r3, r5) VALUES(%d, 1, 2, 3, 'four', {5, 6, 7});" % (key))
        session.execute("ALTER TABLE Test.cf DROP r2;")
        for key in range(n_partitions):
            session.execute("INSERT INTO Test.cf (pk1, ck1, r1, r3) VALUES(%d, 2, 3, 'four');" % (key))
        session.execute("ALTER TABLE Test.cf DROP r5;")
        session.execute("ALTER TABLE Test.cf ADD r2 varint;")
        session.execute("ALTER TABLE Test.cf ADD r4 int;")
        for key in range(n_partitions):
            session.execute("INSERT INTO Test.cf (pk1, ck1, r2, r4) VALUES(%d, 0, 99, 999);" % (key))

        debug("Verify data is present")
        session = self.patient_cql_connection(node1)
        for key in range(n_partitions):
            res = session.execute("SELECT * FROM Test.cf where pk1 = %d" % (key))
            self.assertCountEqual(rows_to_list(res),
                                  [
                [key, 0, None, 99, u'two', 999],
                [key, 1, 2, None, u'four', None],
                [key, 2, 3, None, u'four', None],
                [key, 8, None, None, u'eight', None],
                [key, 9, None, None, u'seven', None],
            ])

        debug("Stop node abruptly")
        node1.stop(gently=False)

        debug("Verify commitlog was written before abrupt stop")
        commitlog_dir = os.path.join(node1.get_path(), 'commitlogs')
        commitlog_files = os.listdir(commitlog_dir)
        self.assertTrue(len(commitlog_files) > 0)

        debug("Verify commitlog was replayed on startup")
        node1.start(wait_for_binary_proto=False)
        node1.watch_log_for("Log replay complete")
        replays = node1.grep_log(" (\d+) replayed mutations", filter_expr='DEBUG')
        self.assertGreater(len(replays), 0)
        replayed_mutations = 0
        for line, m in replays:
            replayed_mutations += int(m.group(1))
        self.assertGreaterEqual(replayed_mutations, 4)

        debug("Make query and ensure data is present")
        session = self.patient_cql_connection(node1)
        for key in range(n_partitions):
            res = session.execute("SELECT * FROM Test.cf where pk1 = %d" % (key))
            self.assertCountEqual(rows_to_list(res),
                                  [
                [key, 0, None, 99, u'two', 999],
                [key, 1, 2, None, u'four', None],
                [key, 2, 3, None, u'four', None],
                [key, 8, None, None, u'eight', None],
                [key, 9, None, None, u'seven', None],
            ])

    def default_segment_size_test(self):
        """ Test default commitlog_segment_size_in_mb (32MB) """

        self._segment_size_test(32)

    def small_segment_size_test(self):
        """ Test a small commitlog_segment_size_in_mb (5MB) """

        self._segment_size_test(5)

    @since('2.2')
    @skip('fails with wrong commit log size - probably because we use max commit log - and not amend to it')
    def default_compressed_segment_size_test(self):
        """ Test default compressed commitlog_segment_size_in_mb (32MB) """
        # Scylla: Unknown option commitlog_compression
        self._segment_size_test(32, compressed=True)

    @since('2.2')
    @skip('fails with wrong commit log size - probably because we use max commit log - and not amend to it')
    def small_compressed_segment_size_test(self):
        """ Test a small compressed commitlog_segment_size_in_mb (5MB) """
        # Scylla: Unknown option commitlog_compression
        self._segment_size_test(5, compressed=True)

    expected_log_message = 'commitlog - Exception in segment reservation: storage_io_error \(Storage I/O error: 13: filesystem error: open failed'

    @attr('next-gating')
    @attr('dtest-debug')
    def stop_failure_policy_test(self):
        """ Test the stop commitlog failure policy (default one) """
        self.prepare()

        self._provoke_commitlog_failure()
        failure = self.node1.grep_log(self.expected_log_message)
        debug(failure)
        self.assertTrue(failure, "Cannot find the commitlog failure message in logs")
        self.assertTrue(self.node1.is_running(), "Node1 should still be running")

        # Cannot write anymore after the failure
        with self.assertRaises(NoHostAvailable):
            self.session1.execute("""
              INSERT INTO test (key, col1) VALUES (2, 2);
            """)

        # Should not be able to read neither
        with self.assertRaises(NoHostAvailable):
            self.session1.execute("""
              "SELECT * FROM test;"
            """)

    @skip('unsupported since scylladb/scylla#2246')
    def stop_commit_failure_policy_test(self):
        """ Test the stop_commit commitlog failure policy """
        self.prepare(configuration={
            'commit_failure_policy': 'stop_commit'
        })

        self.session1.execute("""
            INSERT INTO test (key, col1) VALUES (2, 2);
        """)

        self._provoke_commitlog_failure()
        failure = self.node1.grep_log(self.expected_log_message)
        debug(failure)
        self.assertTrue(failure, "Cannot find the commitlog failure message in logs")
        self.assertTrue(self.node1.is_running(), "Node1 should still be running")

        # Cannot write anymore after the failure
        with self.assertRaises((OperationTimedOut, WriteTimeout)):
            self.session1.execute("""
              INSERT INTO test (key, col1) VALUES (2, 2);
            """)

        # Should be able to read
        assert_one(
            self.session1,
            "SELECT * FROM test where key=2;",
            [2, 2]
        )

    @skip('unsupported since scylladb/scylla#2246')
    def die_failure_policy_test(self):
        """ Test the die commitlog failure policy """
        self.prepare(configuration={
            'commit_failure_policy': 'die'
        })

        self._provoke_commitlog_failure()
        failure = self.node1.grep_log(self.expected_log_message)
        debug(failure)
        self.assertTrue(failure, "Cannot find the commitlog failure message in logs")
        self.assertFalse(self.node1.is_running(), "Node1 should not be running")

    @skip('unsupported since scylladb/scylla#2246')
    def ignore_failure_policy_test(self):
        """ Test the ignore commitlog failure policy """
        self.prepare(configuration={
            'commit_failure_policy': 'ignore'
        })

        self._provoke_commitlog_failure()
        failure = self.node1.grep_log(self.expected_log_message)
        self.assertTrue(failure, "Cannot find the commitlog failure message in logs")
        self.assertTrue(self.node1.is_running(), "Node1 should still be running")

        # on Windows, we can't delete the segments if they're chmod to 0 so they'll still be available for use by CLSM,
        # and we can still create new segments since os.chmod is limited to stat.S_IWRITE and stat.S_IREAD to set files
        # as read-only. New mutations will still be allocated and WriteTimeouts will not be raised. It's sufficient that
        # we confirm that a) the node isn't dead (stop) and b) the node doesn't terminate the thread (stop_commit)
        query = "INSERT INTO test (key, col1) VALUES (2, 2);"
        if is_win():
            # We expect this to succeed
            self.session1.execute(query)
            self.assertFalse(self.node1.grep_log("terminating thread"), "thread was terminated but CL error should have been ignored.")
            self.assertTrue(self.node1.is_running(), "Node1 should still be running after an ignore error on CL")
        else:
            with self.assertRaises((OperationTimedOut, WriteTimeout)):
                self.session1.execute(query)

            # Should not exist
            assert_none(self.session1, "SELECT * FROM test where key=2;")

        # bring back the node commitlogs
        self._change_commitlog_perms(stat.S_IWRITE | stat.S_IREAD | stat.S_IEXEC)

        self.session1.execute("""
          INSERT INTO test (key, col1) VALUES (3, 3);
        """)
        assert_one(
            self.session1,
            "SELECT * FROM test where key=3;",
            [3, 3]
        )

        time.sleep(2)
        assert_one(
            self.session1,
            "SELECT * FROM test where key=2;",
            [2, 2]
        )

    @skip("line \"version = struct.unpack('>i', f.read(4))[0]\" fails when read(4) returns an empty string")
    def test_bad_crc(self):
        """
        if the commit log header crc (checksum) doesn't match the actual crc of the header data,
        and the commit_failure_policy is stop, C* shouldn't startup
        @jira_ticket CASSANDRA-9749
        """
        expected_error = "Exiting due to error while processing commit log during initialization."
        self.ignore_log_patterns.append(expected_error)
        node = self.node1
        assert isinstance(node, Node)
        node.set_configuration_options({'commit_failure_policy': 'stop', 'commitlog_sync_period_in_ms': 1000})
        self.cluster.start()

        cursor = self.patient_cql_connection(self.cluster.nodelist()[0])
        self.create_ks(cursor, 'ks', 1)
        cursor.execute("CREATE TABLE ks.tbl (k INT PRIMARY KEY, v INT)")

        for i in range(10):
            cursor.execute("INSERT INTO ks.tbl (k, v) VALUES ({0}, {0})".format(i))

        results = list(cursor.execute("SELECT * FROM ks.tbl"))
        self.assertEqual(len(results), 10)

        # with the commitlog_sync_period_in_ms set to 1000,
        # this sleep guarantees that the commitlog data is
        # actually flushed to disk before we kill -9 it
        time.sleep(1)

        node.stop(gently=False)

        # check that ks.tbl hasn't been flushed
        path = node.get_path()
        ks_dir = os.path.join(path, 'data', 'ks')
        db_dir = os.listdir(ks_dir)[0]
        sstables = len([f for f in os.listdir(os.path.join(ks_dir, db_dir)) if f.endswith('.db')])
        self.assertEqual(sstables, 0)

        # modify the commit log crc values
        cl_dir = os.path.join(path, 'commitlogs')
        self.assertTrue(len(os.listdir(cl_dir)) > 0)
        for cl in os.listdir(cl_dir):
            # locate the CRC location
            with open(os.path.join(cl_dir, cl), 'r') as f:
                f.seek(0)
                version = struct.unpack('>i', f.read(4))[0]
                crc_pos = 12
                if version >= 5:
                    f.seek(crc_pos)
                    psize = struct.unpack('>h', f.read(2))[0] & 0xFFFF
                    crc_pos += 2 + psize

            # rewrite it with crap
            with open(os.path.join(cl_dir, cl), 'w') as f:
                f.seek(crc_pos)
                f.write(struct.pack('>i', 123456))

            # verify said crap
            with open(os.path.join(cl_dir, cl), 'r') as f:
                f.seek(crc_pos)
                crc = struct.unpack('>i', f.read(4))[0]
                self.assertEqual(crc, 123456)

        mark = node.mark_log()
        node.start()
        node.watch_log_for(expected_error, from_mark=mark)
        with self.assertRaises(TimeoutError):
            node.wait_for_binary_interface(from_mark=mark, timeout=20)
        self.assertFalse(node.is_running())

    @skip('failure in this line "self.assertEqual(get_header_crc(header_bytes), crc)"')
    def test_compression_error(self):
        """
        if the commit log header refers to an unknown compression class, and the commit_failure_policy is stop, C* shouldn't startup
        """
        expected_error = 'Could not create Compression for type org.apache.cassandra.io.compress.LZ5Compressor'
        self.ignore_log_patterns.append(expected_error)
        node = self.node1
        assert isinstance(node, Node)
        node.set_configuration_options({'commit_failure_policy': 'stop',
                                        'commitlog_compression': [{'class_name': 'LZ4Compressor'}],
                                        'commitlog_sync_period_in_ms': 1000})
        self.cluster.start()

        cursor = self.patient_cql_connection(self.cluster.nodelist()[0])
        self.create_ks(cursor, 'ks1', 1)
        cursor.execute("CREATE TABLE ks1.tbl (k INT PRIMARY KEY, v INT)")

        for i in range(10):
            cursor.execute("INSERT INTO ks1.tbl (k, v) VALUES ({0}, {0})".format(i))

        results = list(cursor.execute("SELECT * FROM ks1.tbl"))
        self.assertEqual(len(results), 10)

        # with the commitlog_sync_period_in_ms set to 1000,
        # this sleep guarantees that the commitlog data is
        # actually flushed to disk before we kill -9 it
        time.sleep(1)

        node.stop(gently=False)

        # check that ks1.tbl hasn't been flushed
        path = node.get_path()
        ks_dir = os.path.join(path, 'data', 'ks1')
        db_dir = os.listdir(ks_dir)[0]
        sstables = len([f for f in os.listdir(os.path.join(ks_dir, db_dir)) if f.endswith('.db')])
        self.assertEqual(sstables, 0)

        def get_header_crc(header):
            """
            When calculating the header crc, C* splits up the 8b id, first adding the 4 least significant
            bytes to the crc, then the 5 most significant bytes, so this splits them and calculates the same way
            """
            new_header = header[:4]
            # C* evaluates most and least significant 4 bytes out of order
            new_header += header[8:12]
            new_header += header[4:8]
            # C* evaluates the short parameter length as an int
            new_header += '\x00\x00' + header[12:14]  # the
            new_header += header[14:]
            return binascii.crc32(new_header)

        # modify the compression parameters to look for a compressor that isn't there
        # while this scenario is pretty unlikely, if a jar or lib got moved or something,
        # you'd have a similar situation, which would be fixable by the user
        cl_dir = os.path.join(path, 'commitlogs')
        self.assertTrue(len(os.listdir(cl_dir)) > 0)
        for cl in os.listdir(cl_dir):
            # read the header and find the crc location
            with open(os.path.join(cl_dir, cl), 'r') as f:
                f.seek(0)
                crc_pos = 12
                f.seek(crc_pos)
                psize = struct.unpack('>h', f.read(2))[0] & 0xFFFF
                crc_pos += 2 + psize

                header_length = crc_pos
                f.seek(crc_pos)
                crc = struct.unpack('>i', f.read(4))[0]

                # check that we're going this right
                f.seek(0)
                header_bytes = f.read(header_length)
                self.assertEqual(get_header_crc(header_bytes), crc)

            # rewrite it with imaginary compressor
            self.assertIn('LZ4Compressor', header_bytes)
            header_bytes = header_bytes.replace('LZ4Compressor', 'LZ5Compressor')
            self.assertNotIn('LZ4Compressor', header_bytes)
            self.assertIn('LZ5Compressor', header_bytes)
            with open(os.path.join(cl_dir, cl), 'w') as f:
                f.seek(0)
                f.write(header_bytes)
                f.seek(crc_pos)
                f.write(struct.pack('>i', get_header_crc(header_bytes)))

            # verify we wrote everything correctly
            with open(os.path.join(cl_dir, cl), 'r') as f:
                f.seek(0)
                self.assertEqual(f.read(header_length), header_bytes)
                f.seek(crc_pos)
                crc = struct.unpack('>i', f.read(4))[0]
                self.assertEqual(crc, get_header_crc(header_bytes))

        mark = node.mark_log()
        node.start()
        node.watch_log_for(expected_error, from_mark=mark)
        with self.assertRaises(TimeoutError):
            node.wait_for_binary_interface(from_mark=mark, timeout=20)

    def test_commitlog_replay_with_counters(self):
        """
        Test commit log replay with counters
        The goal of the test is to verify that commit log replay works correctly -
        we save the end result in the commit log, not delta.
        """
        node1 = self.node1
        node1.set_configuration_options(values={'commitlog_sync_period_in_ms': 200})
        self.cluster.start()

        debug("Create table")
        session = self.patient_cql_connection(node1)
        self.create_ks(session, 'Test', 1)
        session.execute("""
                    CREATE TABLE cf (
                        pk1 INT,
                        ck1 INT,
                        cnt COUNTER,
                        PRIMARY KEY(pk1, ck1)
                    );
                """)

        debug("Increment counter")
        for i in range(1, 10):
            session.execute("UPDATE Test.cf SET cnt = cnt + {} WHERE pk1 = 5 AND ck1 = 6;".format(i))

        res = session.execute("SELECT cnt FROM Test.cf WHERE pk1 = 5 AND ck1 = 6;")
        rows = rows_to_list(res)
        self.assertEquals(rows[0][0], 45)

        debug("Decrement counter")
        session.execute("UPDATE Test.cf SET cnt = cnt - 1 WHERE pk1 = 5 AND ck1 = 6;")
        debug("Add one more counter")
        session.execute("UPDATE Test.cf SET cnt = cnt + 10 WHERE pk1 = 7 AND ck1 = 8;")

        res = session.execute("SELECT cnt FROM Test.cf;")
        rows = rows_to_list(res)
        self.assertEquals(rows[0][0], 44)
        self.assertEquals(rows[1][0], 10)

        # wait for commit log sync
        time.sleep(2)

        debug("Stop node abruptly")
        node1.stop(gently=False)

        debug("Verify commitlog was written before abrupt stop")
        commitlog_dir = os.path.join(node1.get_path(), 'commitlogs')
        commitlog_files = glob.glob(os.path.join(commitlog_dir, '*.log'))
        self.assertTrue(len(commitlog_files) > 0)

        debug("Verify commit log was replayed on startup")
        node1.start()
        node1.watch_log_for("Log replay complete")
        # Here we verify there was more than 0 replayed mutations
        zero_replays = node1.grep_log(" 0 replayed mutations", filter_expr='DEBUG')
        self.assertEqual(0, len(zero_replays))

        debug("Make query and ensure data is present as expected")
        session = self.patient_cql_connection(node1)
        res = session.execute("SELECT cnt FROM Test.cf;")
        rows = rows_to_list(res)
        self.assertEquals(rows[0][0], 44)
        self.assertEquals(rows[1][0], 10)

    def prepare_cluster_with_ks_cf(self, jvm_args=None):
        node1 = self.node1
        jvm_args = jvm_args or []
        self.cluster.start(jvm_args=jvm_args)

        debug("Create table")
        session = self.patient_cql_connection(node1)
        self.create_ks(session, 'Test', 1)
        session.execute("""
                    CREATE TABLE cf (
                        pk1 INT,
                        ck1 INT,
                        v1 int,
                        PRIMARY KEY(pk1, ck1)
                    );
                """)
        return session, node1

    def test_periodic_commitlog(self):
        """
        Test periodic mode of commitlog flushing
        'periodic' mode is where all commitlog writes are ready the moment they are stored in
        a memory buffer and the memory buffer is flushed to a storage periodically.
        """

        session, node1 = self.prepare_cluster_with_ks_cf()
        debug("Insert 100 rows")
        for i in range(0, 100):
            session.execute("INSERT INTO Test.cf (pk1, ck1, v1) VALUES ({i}, {i}, {i})".format(i=i))

        assert_row_count(session=session, table_name='Test.cf', expected=100)

        debug("Stop node abruptly")
        node1.stop(gently=False)

        debug("Start node")
        node1.start(verbose=True, wait_other_notice=True, wait_for_binary_proto=True)

        debug("Make query and ensure data is present as expected")
        session = self.patient_cql_connection(node1)

        assert_row_count_in_select_less(session=session, query='select * from Test.cf',
                                        max_rows_expected=100)

    def test_batch_commitlog(self):
        """
        Test batch mode of commitlog flushing
        'batch' mode where each write is flushed as soon as possible (after previous flush completed)
        and writes are only ready after they are flushed.
        This mode is used for LWT
        """
        session, node1 = self.prepare_cluster_with_ks_cf()

        debug("Insert 100 rows")
        for i in range(0, 100):
            session.execute("UPDATE Test.cf SET v1={i} WHERE pk1 = {i} and ck1={i} IF v1 = NULL".format(i=i))

        assert_row_count(session=session, table_name='Test.cf', expected=100)

        debug("Stop node abruptly")
        node1.stop(gently=False)

        debug("Start node")
        node1.start()

        debug("Make query and ensure data is present as expected")
        session = self.patient_cql_connection(node1)
        assert_row_count(session=session, table_name='Test.cf', expected=100)

    def test_mixed_mode_commitlog_2_partitions_smp_1(self):
        """
        Test 'batch' and 'periodic' mode of commitlog flushing

        - 'periodic' mode is where all commitlog writes are ready the moment they are stored in
          a memory buffer and the memory buffer is flushed to a storage periodically.

        - 'batch' mode where each write is flushed as soon as possible (after previous flush completed)
          and writes are only ready after they are flushed.
          This mode is used for LWT
        """
        session, node1 = self.prepare_cluster_with_ks_cf(jvm_args=['--smp', '1'])

        expected_result = []
        debug("Insert 200 rows")
        for i in range(0, 100):
            # Row - candidate for 'batch' mode
            session.execute("INSERT INTO Test.cf (pk1, ck1, v1) VALUES ({pk}, {i},{i}) IF NOT EXISTS".format(i=i, pk=1))
            # Row - candidate for 'periodic' mode
            session.execute("INSERT INTO Test.cf (pk1, ck1, v1) VALUES ({pk}, {i}, {i})".format(i=i, pk=2))
            expected_result.append([2, i, i])
            expected_result.append([1, i, i])

        debug("Insert more 100 non-LWT rows and 1 LWT row in the middle of queue")
        for i in range(100, 200):
            if i == 150:
                session.execute(
                    "INSERT INTO Test.cf (pk1, ck1, v1) VALUES ({pk}, {i},{i}) IF NOT EXISTS".format(i=i, pk=1))
                expected_result.append([1, i, i])
            session.execute("INSERT INTO Test.cf (pk1, ck1, v1) VALUES ({pk}, {i}, {i})".format(i=i, pk=2))
            if i < 150:
                expected_result.append([2, i, i])

        assert_row_count_in_select(session=session, query='select * from Test.cf where pk1=1',
                                   num_rows_expected=101)
        assert_row_count_in_select(session=session, query='select * from Test.cf where pk1=2',
                                   num_rows_expected=200)

        debug("Stop node abruptly")
        node1.stop(gently=False)

        debug("Start node")
        node1.start()

        debug("Make query and ensure data is present as expected")
        session = self.patient_cql_connection(node1)

        # LWT rows - expected all rows were flushed immediately
        assert_row_count_in_select(session=session, query='select * from Test.cf where pk1=1',
                                   num_rows_expected=101)
        assert_row_count_in_select(session=session, query='select * from Test.cf where pk1=2',
                                   num_rows_expected=150)
        assert_all(session=session, query='select * from Test.cf', expected=expected_result, ignore_order=True)

    def test_mixed_mode_commitlog_2_partitions_smp_2(self):
        """
        Test 'batch' and 'periodic' mode of commitlog flushing

        - 'periodic' mode is where all commitlog writes are ready the moment they are stored in
          a memory buffer and the memory buffer is flushed to a storage periodically.

        - 'batch' mode where each write is flushed as soon as possible (after previous flush completed)
          and writes are only ready after they are flushed.
          This mode is used for LWT

        By Glebs explanation:
            When LWT and non-LWT data is written into different partitions and smp > 1
            non-LWT rows may be flushed for many reasons, or may be not.
            Any number between 0 and total number of written non LWT rows are expected. So there is no expected
            result for non-LWT rows
        """
        session, node1 = self.prepare_cluster_with_ks_cf(jvm_args=['--smp', '2'])

        expected_result = []
        debug("Insert 200 rows")
        for i in range(0, 100):
            # Row - candidate for 'batch' mode
            session.execute("INSERT INTO Test.cf (pk1, ck1, v1) VALUES ({pk}, {i},{i}) IF NOT EXISTS".format(i=i, pk=1))
            # Row - candidate for 'periodic' mode
            session.execute("INSERT INTO Test.cf (pk1, ck1, v1) VALUES ({pk}, {i}, {i})".format(i=i, pk=2))
            expected_result.append([1, i, i])

        debug("Insert more 100 non-LWT rows and 1 LWT row in the middle of queue")
        for i in range(100, 200):
            if i == 150:
                session.execute(
                    "INSERT INTO Test.cf (pk1, ck1, v1) VALUES ({pk}, {i},{i}) IF NOT EXISTS".format(i=i, pk=1))
                expected_result.append([1, i, i])
            session.execute("INSERT INTO Test.cf (pk1, ck1, v1) VALUES ({pk}, {i}, {i})".format(i=i, pk=2))

        assert_row_count_in_select(session=session, query='select * from Test.cf where pk1=1',
                                   num_rows_expected=101)
        assert_row_count_in_select(session=session, query='select * from Test.cf where pk1=2',
                                   num_rows_expected=200)

        debug("Stop node abruptly")
        node1.stop(gently=False)

        debug("Start node")
        node1.start()

        debug("Make query and ensure data is present as expected")
        session = self.patient_cql_connection(node1)

        # LWT rows - expected all rows were flushed immediately
        assert_row_count_in_select(session=session, query='select * from Test.cf where pk1=1',
                                   num_rows_expected=101)
        assert_all(session=session, query='select * from Test.cf where pk1=1',
                   expected=expected_result, ignore_order=True)

    def test_mixed_mode_commitlog_same_partition_smp_1(self):
        self._mixed_mode_commitlog_same_partition(smp='1')

    def test_mixed_mode_commitlog_same_partition_smp_2(self):
        self._mixed_mode_commitlog_same_partition(smp='2')

    def _mixed_mode_commitlog_same_partition(self, smp):
        """
        Test 'batch' and 'periodic' mode of commitlog flushing

        - 'periodic' mode is where all commitlog writes are ready the moment they are stored in
          a memory buffer and the memory buffer is flushed to a storage periodically.

        - 'batch' mode where each write is flushed as soon as possible (after previous flush completed)
          and writes are only ready after they are flushed.
          This mode is used for LWT

          When LWT and non-LWT data is written into the same partition it isn't matter how many smp -
          all non-LWT rows that were arrived before last LWT row should be flushed
        """
        session, node1 = self.prepare_cluster_with_ks_cf(jvm_args=['--smp', smp, '--default-log-level', 'trace'])

        expected_result = []
        debug("Insert 200 rows")
        for i in range(0, 100):
            # Row - candidate for 'batch' mode
            session.execute("INSERT INTO Test.cf (pk1, ck1, v1) VALUES ({pk}, {i},{i}) IF NOT EXISTS".format(i=i, pk=1))
            expected_result.append([1, i, i])
        for i in range(100, 200):
            # Row - candidate for 'periodic' mode
            session.execute("INSERT INTO Test.cf (pk1, ck1, v1) VALUES ({pk}, {i}, {i})".format(i=i, pk=1))
            expected_result.append([1, i, i])

        debug("Insert more 100 non-LWT rows and 1 LWT row in the middle of queue")
        for i in range(200, 300):
            if i == 250:
                session.execute(
                    "INSERT INTO Test.cf (pk1, ck1, v1) VALUES ({pk}, {i},{i}) IF NOT EXISTS".format(i=i, pk=1))
            else:
                session.execute("INSERT INTO Test.cf (pk1, ck1, v1) VALUES ({pk}, {i}, {i})".format(i=i, pk=1))

            if i <= 250:
                expected_result.append([1, i, i])
        assert_row_count_in_select(session=session, query='select * from Test.cf where pk1=1',
                                   num_rows_expected=300)

        debug("Stop node abruptly")
        node1.stop(gently=False)

        debug("Start node")
        node1.start()

        debug("Make query and ensure data is present as expected")
        session = self.patient_cql_connection(node1)

        # LWT rows - expected all rows were flushed immediately
        assert_row_count_in_select(session=session, query='select * from Test.cf',
                                   num_rows_expected=251)
        assert_all(session=session, query='select * from Test.cf', expected=expected_result, ignore_order=True)

    def test_mixed_mode_with_delete_commitlog(self):
        """
        Test 'batch' and 'periodic' mode of commitlog flushing

        - 'periodic' mode is where all commitlog writes are ready the moment they are stored in
          a memory buffer and the memory buffer is flushed to a storage periodically.

        - 'batch' mode where each write is flushed as soon as possible (after previous flush completed)
          and writes are only ready after they are flushed.
          This mode is used for LWT
        """
        session, node1 = self.prepare_cluster_with_ks_cf()

        expected_result = []
        debug("Insert 100 non-LWT rows")
        for i in range(0, 100):
            # Row - candidate for 'periodic' mode
            session.execute("INSERT INTO Test.cf (pk1, ck1, v1) VALUES ({pk}, {i}, {i})".format(i=i, pk=1))
            expected_result.append([1, i, i])

        debug("Insert 100 LWT rows")
        for i in range(100, 200):
            # Row - candidate for 'batch' mode
            session.execute("INSERT INTO Test.cf (pk1, ck1, v1) VALUES ({pk}, {i},{i}) IF NOT EXISTS".format(i=i, pk=1))
            if i != 150:
                expected_result.append([1, i, i])

        debug("Insert more 100 non-LWT rows and 1 LWT row in the middle of queue")
        for i in range(200, 300):
            if i == 250:
                session.execute(
                    "DELETE FROM Test.cf WHERE pk1 = 1 and ck1 = 150 IF EXISTS")
            session.execute("INSERT INTO Test.cf (pk1, ck1, v1) VALUES ({pk}, {i}, {i})".format(i=i, pk=1))
            if i < 250:
                expected_result.append([1, i, i])

        assert_row_count_in_select(session=session, query='select * from Test.cf where pk1=1',
                                   num_rows_expected=299)

        debug("Stop node abruptly")
        node1.stop(gently=False)

        debug("Start node")
        node1.start()

        debug("Make query and ensure data is present as expected")
        session = self.patient_cql_connection(node1)
        # LWT rows - expected all rows were flushed immediately
        assert_row_count_in_select(session=session, query='select * from Test.cf',
                                   num_rows_expected=249)
        assert_all(session=session, query='select * from Test.cf', expected=expected_result, ignore_order=True)
