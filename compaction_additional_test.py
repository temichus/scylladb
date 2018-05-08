import tempfile
import time
import os
import shutil
import glob
from re import findall
from dtest import Tester, debug
from scylla_tools import get_sstables_files, insert_c1c2, get_cf_dir
from cassandra import ConsistencyLevel
from assertions import assert_none

from datetime import datetime as dt


class CompactionAdditionalTest(Tester):

    def compaction_delete_with_smp_change_test(self):
        """
        Test that data is not resurected when shared sstables
        are used
        1. smp=1 create sstable A with 100 keys
        2. shutdown
        3. boot with smp=2 (forcing step 1 sstables to be shared) delete all keys
        4. wait past gc_preiod
        5. insert a key forcing flush multiple times till a compaction is triggered
        6. stop and start the node
        7. check that no data was resurected and that some of the deletion markers still exist
        8. insert additional 100 keys forcing a flush multiple times till multiple compactions are trigerred
        9. check that no deletion marker is left and files have been removed
        """
        cluster = self.cluster
        cluster.populate(1)
        [node1] = cluster.nodelist()
        node1.start(wait_for_binary_proto=True, jvm_args=['--smp', '1'])

        session = self.patient_cql_connection(node1)
        self.create_ks(session, 'ks', 1)

        session.execute("create table ks.cf (key int PRIMARY KEY, val int) "
                        "with compaction = {'class':'SizeTieredCompactionStrategy'} and gc_grace_seconds = 30;")

        for x in range(0, 100):
            session.execute('insert into cf (key, val) values (' + str(x) + ',1)')

        node1.flush()
        node1.compact()
        node1.stop()
        node1.start(wait_for_binary_proto=True, jvm_args=['--smp', '2'])

        session = self.patient_cql_connection(node1, 'ks')
        for x in range(0, 100):
            session.execute('delete from cf where key = ' + str(x))
        node1.flush()

        time.sleep(31)

        # we passed gc_period and force an update so that compaction will
        # be triggered on a single shard (removing data and tombstone)
        rows = session.execute("select count(*) from system.compaction_history")
        compactions_1 = rows[0][0]
        compactions_2 = compactions_1

        while compactions_1 == compactions_2:
            session.execute('insert into ks.cf (key, val) values (199,1);')
            node1.flush()
            rows = session.execute("select count(*) from system.compaction_history")
            compactions_2 = rows[0][0]
        node1.wait_for_compactions()

        # reboot and verify that data  is not resurected
        node1.stop()
        node1.start(wait_for_binary_proto=True, jvm_args=['--smp', '2'])

        session = self.patient_cql_connection(node1, 'ks')
        for x in range(0, 100):
            assert_none(session, 'select * from cf where key = ' + str(x))

        # verify that only some deletion markers will be kept since we reshard the files
        # and gc_preiod passed so some tombstones have been removed by compaction
        json_path = tempfile.mkstemp(suffix='.json')
        jname = json_path[1]
        with open(jname, 'w') as f:
            node1.run_sstable2json(f, keyspace='ks')

        with open(jname, 'r') as g:
            jsoninfo = g.read()

        numfound = jsoninfo.count("marked_deleted")

        self.assertLess(numfound, 100)
        self.assertGreater(numfound, 0)

        # trigger compaction on both shards
        node1.wait_for_compactions()
        rows = session.execute("select count(*) from system.compaction_history")
        compactions_1 = rows[0][0]
        compactions_2 = compactions_1

        while compactions_1 + 2 > compactions_2:
            for x in range(200, 300):
                session.execute('insert into ks.cf (key, val) values (' + str(x) + ',1);')
            node1.flush()
            rows = session.execute("select count(*) from system.compaction_history")
            compactions_2 = rows[0][0]
        node1.wait_for_compactions()

        # validate that all deletion markers have been removed
        json_path = tempfile.mkstemp(suffix='.json')
        jname = json_path[1]
        with open(jname, 'w') as f:
            node1.run_sstable2json(f, keyspace='ks')

        with open(jname, 'r') as g:
            jsoninfo = g.read()

        numfound = jsoninfo.count("marked_deleted")

        self.assertEqual(numfound, 0)

    def compaction_removes_ttld_data_by_time_windows_test(self):
        """
        Test that TWCS compaction removes TTLd data after gc_period by time windows
        2. Create a table with a DEFAULT TTL=70 and gc_period=10.
        3. Insert data into the table.
        4. Wait past ttl and gc_period
        5. write some data and force compaction
        6. check that ttl'd data was removed
        """

        debug("Starting a cluster of one node...")
        cluster = self.cluster
        cluster.populate(1).start(wait_for_binary_proto=True)
        nodes = cluster.nodelist()
        node1 = nodes[0]

        TIME_TO_SLEEP_BETWEEN_FILES = 5
        NUMBER_OF_FILES = 13
        NUMBER_OF_KEYS = 100
        TTL = 70
        GC_GRACE=10

        session = self.patient_cql_connection(node1)
        debug("Creating keyspace 'ks'...")
        self.create_ks(session, 'ks', 1)

        # DEFAULT TTL set to 70, gc_grace set to 10 and expiry check set to 60.
        # It means that every 60 seconds it should purge all sstabls that are older than 180+30
        debug("Creating a column family 'cf' with TWCS and DEFAULT TTL of {}".format(TTL))
        self.create_cf(session, 'cf', gc_grace=GC_GRACE, columns={'c1': 'text', 'c2': 'text'}, default_ttl=TTL,
                       compaction={'compaction_window_size': '1', 'compaction_window_unit': 'MINUTES',
                                   'class': 'TimeWindowCompactionStrategy',
                                   'expired_sstable_check_frequency_seconds': '60'})

        for t in range(0, NUMBER_OF_FILES):
            debug("Inserting concurrently {} keys...".format(NUMBER_OF_KEYS))
            insert_c1c2(session, n=NUMBER_OF_KEYS, consistency=ConsistencyLevel.ONE)
            node1.flush()
            time.sleep(TIME_TO_SLEEP_BETWEEN_FILES)

        node1.flush()

        ks_dir = os.path.join(self.test_path, 'test', 'node1', 'data', 'ks')
        cf_dir = get_cf_dir(ks_dir, 'cf')
        debug("'cf' directory is {}".format(cf_dir))

        # Save the names of the current sstable files
        sstables_files1 = get_sstables_files(cf_dir, 'ks', 'cf', f_type='Data')
        debug("sstables BEFORE SLEEP: {}".format(sstables_files1))

        debug("Sleep for {} seconds (TTL + GC) to let the files to completly TTL'ed".format(TTL+GC_GRACE))
        time.sleep(TTL+GC_GRACE)

        # Save the names of the current sstable files
        sstables_files2 = get_sstables_files(cf_dir, 'ks', 'cf', f_type='Data')
        debug("sstables AFTER SLEEP: {}".format(sstables_files2))

        # Even after the TTL+GC time has passed, the sstables remains till new data is inserted.
        # This assert just verifies that the files are still there.
        assert set(sstables_files1) == set(sstables_files2), \
            "Some or ALL of the files MISSING: {}".format(set(sstables_files1) - set(sstables_files2))

        debug("Orig files {} havn't been purged yet(expected)".format(sstables_files1.intersection(sstables_files2)))

        mark = node1.mark_log()
        # Insert one key to trigger a sstable expiration check (expired_sstable_check_frequency_seconds': '60').
        insert_c1c2(session, n=1, consistency=ConsistencyLevel.ONE)
        node1.flush()
        # Non mandatory Sleep, just to let any unfinished compaction to finish.
        time.sleep(5)
        # CHECK log: should have something like:
        # "Compacted 2 sstables to []. 36623 bytes to 0 (~0% of original) in 2ms = 0.00MB/s.
        #  ~512 total partitions merged to 0."
        found = node1.watch_log_for("Compacted [0-9]+ sstables to \[\]. [0-9]+ bytes to 0 \(\~0\% of original\) ",
                                    timeout=5, from_mark=mark)
        debug(found)
        # Save the names of the current sstable files
        sstables_files2 = get_sstables_files(cf_dir, 'ks', 'cf', f_type='Data')
        debug("sstables AFTER INSERT more data and EXPIRATION OF older sstables: {}".format(sstables_files2))

        unpurged_files = set(sstables_files1).intersection(sstables_files2)
        self.assertFalse(unpurged_files, "PROBLEM Some of original files are still there and were NOT PURGED: {}".format(unpurged_files))

        debug("Purge SUCCEEDED, original files are not there {}".format(sstables_files2))

    def compact_data_by_time_window_test(self):
        """
        1. Create TABLE with compaction_window_size of 1 MINUTES
        2. Insert data for x minutes while flushing to disk.
        3. Sleep for one window size to make sure all files and compactions are flushed.
        4. Insert more data while flushing to disk
        5. Verify that the previous files created and compacted still exist.
        (Otherwise it means they were compacted wrongly).
        """
        debug("Starting a cluster of one node...")
        cluster = self.cluster
        cluster.populate(1).start(wait_for_binary_proto=True)
        nodes = cluster.nodelist()
        node1 = nodes[0]

        WINDOW_SIZE_MINS=1
        TIME_TO_SLEEP_BETWEEN_FILES = 15
        NUMBER_OF_FILES = 13
        NUMBER_OF_ITERATIONS = 2
        NUMBER_OF_KEYS = 100

        session = self.patient_cql_connection(node1)
        debug("Creating keyspace 'ks'...")
        self.create_ks(session, 'ks', 1)

        debug("Creating a column family 'cf' with TWCS")
        self.create_cf(session, 'cf', columns={'c1': 'text', 'c2': 'text'},
                       compaction={'compaction_window_size': WINDOW_SIZE_MINS, 'compaction_window_unit': 'MINUTES',
                                   'class': 'TimeWindowCompactionStrategy'})

        # Always start to write files when a new minutes start to get consistent results
        while dt.now().second > 5:
            time.sleep(1)
            debug(dt.now().second)

        for t in range(0, NUMBER_OF_FILES):
            debug("Inserting concurrently 100 keys...")
            insert_c1c2(session, n=NUMBER_OF_KEYS, consistency=ConsistencyLevel.ONE)
            node1.flush()
            time.sleep(TIME_TO_SLEEP_BETWEEN_FILES)

        # Probably this sleep not really needed
        # Todo: test without it several times and remove it
        debug("Sleep the compaction window size to make sure all files were compacted to their windows")
        time.sleep(WINDOW_SIZE_MINS * 60)

        ks_dir = os.path.join(self.test_path, 'test', 'node1', 'data', 'ks')
        cf_dir = get_cf_dir(ks_dir, 'cf')
        debug("'cf' directory is {}".format(cf_dir))

        # Save the names of the current sstable files
        sstables_files1 = get_sstables_files(cf_dir, 'ks', 'cf', f_type='Data')
        debug("sstables BEFORE Inserting more data: {}".format(sstables_files1))

        for i in range(0, NUMBER_OF_ITERATIONS):
            for t in range(0, NUMBER_OF_FILES):
                debug("Inserting concurrently 100 keys...")
                insert_c1c2(session, n=NUMBER_OF_KEYS, consistency=ConsistencyLevel.ONE)
                node1.flush()
                time.sleep(TIME_TO_SLEEP_BETWEEN_FILES)

            # Save the names of the current sstable files
            sstables_files2 = get_sstables_files(cf_dir, 'ks', 'cf', f_type='Data')
            debug("sstables AFTER Inserting more data: {}".format(sstables_files2))

            assert sstables_files1.issubset(sstables_files2), "some sstables are missing. Possibly due to wrong " \
                                                              "compaction or wrong deletion. " \
                                                              "Expecting {} but Found {}".format(sstables_files1,
                                                                                                 sstables_files2)


class CompactionAdditionalStrategyTests(Tester):
    __test__ = False

    def __init__(self, *args, **kwargs):
        kwargs['cluster_options'] = {'start_rpc': 'true'}
        Tester.__init__(self, *args, **kwargs)

    def compaction_is_started_on_boot_test(self):
        cluster = self.cluster
        cluster.populate(1).start()
        [node1] = cluster.nodelist()

        session = self.patient_cql_connection(node1)
        self.create_ks(session, 'ks', 1)
        session.execute("create table ks.cf (key int PRIMARY KEY, val int) "
                        "with compaction = {'class':'" + self.strategy + "'};")

        for x in range(0, 100):
            session.execute('insert into cf (key, val) values (' + str(x) + ',1)')

        node1.flush()
        node1.compact()
        node1.stop()
        files = glob.glob(os.path.join(node1.get_path(), 'commitlogs', '*'))
        for f in files:
            os.remove(f)

        keyspace_dir = os.path.join(node1.get_path(), 'data', 'ks')
        sstablefiles = glob.glob(glob.glob(os.path.join(keyspace_dir,
                                                        'cf' + '-*', '*-TOC.txt'))[0].replace('TOC.txt', '') + '*')
        for f in sstablefiles:
            for generation_suffix in xrange(10, 40):
                self._copy_sstable_file(f, "9999%d" % generation_suffix)

        before_start_count = len(glob.glob(os.path.join(keyspace_dir, 'cf' + '-*', '*-Data.db')))

        node1.start()
        time.sleep(30)

        after_start_count = len(glob.glob(os.path.join(keyspace_dir, 'cf' + '-*', '*-Data.db')))

        self.assertNotEqual(before_start_count, after_start_count)

    def _copy_sstable_file(self, file, generation):
        sstable_split_parts = os.path.basename(file).split('-')
        if (len(sstable_split_parts) == 5):
            # <= ka format
            sstable_split_parts[-2] = generation
        elif (len(sstable_split_parts) == 4):
            # >= la format
            sstable_split_parts[1] = generation
        else:
            raise RuntimeError("Unexpected format of file name: '%s'" % file)
        shutil.copy(file, os.path.join(os.path.dirname(file), '-'.join(sstable_split_parts)))

    def compaction_removes_ttld_data_after_gc_period_test(self):
        """
        Test that compaction removes TTLd data after gc_period
        1. start cluster
        2. create a table with a small gc_period
        3. write data into the table with a small ttl
        4. wait past ttl and gc_period
        5. write some data and force compaction
        6. check that ttl'd data was removed
        Please note that we do not test that ttl data exists - we have other tests for this
        """
        cluster = self.cluster
        cluster.populate(1).start(wait_for_binary_proto=True)
        nodes = cluster.nodelist()
        node1 = nodes[0]

        session = self.patient_cql_connection(node1)
        self.create_ks(session, 'ks', 1)

        session.execute("create table ks.cf (key int PRIMARY KEY, val int) "
                        "with compaction = {'class':'" + self.strategy + "'} and gc_grace_seconds = 1;")

        for x in range(0, 100):
            session.execute('insert into cf (key, val) values (' + str(x) + ',1) USING TTL 29')

        time.sleep(31)

        # check that after gc_period compaction removes ttl'd data
        # force an update so that compact will have something to do
        session.execute('insert into ks.cf (key, val) values (99,1);')
        node1.flush()
        node1.compact()

        json_path = tempfile.mkstemp(suffix='.json')
        jname = json_path[1]
        with open(jname, 'w') as f:
            node1.run_sstable2json(f, keyspace='ks')

        with open(jname, 'r') as g:
            jsoninfo = g.read()

        numfound = jsoninfo.count("partition")

        self.assertEqual(numfound, 1)


strategies = ['LeveledCompactionStrategy', 'SizeTieredCompactionStrategy', 'DateTieredCompactionStrategy',
              'TimeWindowCompactionStrategy']
for strategy in strategies:
    cls_name = ('CompactionAdditionalStrategyTests_with_' + strategy)
    vars()[cls_name] = type(cls_name, (CompactionAdditionalStrategyTests,), {'strategy': strategy, '__test__': True})
