import tempfile
import time
import os
import shutil
import glob
import tools
import random

from threading import Thread

from dtest import Tester, debug, run_with_params
from scylla_tools import get_sstables_files, insert_c1c2, get_cf_dir
from cassandra import ConsistencyLevel, concurrent
from assertions import assert_none

from datetime import datetime as dt
from nose.plugins.attrib import attr
import sstable_tools.statistics

from ccmlib.node import NodetoolError
from random import randint


@attr('dtest-full', 'single_node')
class CompactionAdditionalTest(Tester):

    @attr('next-gating')
    @attr('dtest-debug')
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
        debug("Starting node1 with 1 cpu")
        node1.start(wait_for_binary_proto=True, jvm_args=['--smp', '1'])

        session = self.patient_cql_connection(node1)
        self.create_ks(session, 'ks', 1)

        gc_grace_seconds = 5
        keys = 100
        debug("Inserting {} keys with gc_grace_seconds={}".format(keys, gc_grace_seconds))
        session.execute("create table ks.cf (key int PRIMARY KEY, val int) "
                        "with compaction = {{'class':'SizeTieredCompactionStrategy'}} and gc_grace_seconds = {};".format(gc_grace_seconds))

        for x in range(0, keys):
            session.execute('insert into cf (key, val) values (' + str(x) + ',1)')

        node1.flush()
        node1.compact()
        debug("Restarting node1 with 2 cpus")
        node1.stop()
        node1.start(wait_for_binary_proto=True, jvm_args=['--smp', '2'])

        session = self.patient_cql_connection(node1, 'ks')
        debug("Deleting {} keys".format(keys))
        for x in range(0, keys):
            session.execute('delete from cf where key = ' + str(x))
        node1.flush()

        debug("Waiting gc_grace_seconds={} to pass".format(gc_grace_seconds))
        time.sleep(gc_grace_seconds + 1)

        # we passed gc_period and force an update so that compaction will
        # be triggered on a single shard (removing data and tombstone)
        def compactions_count():
            rows = session.execute("select count(*) from system.compaction_history "
                                   "where keyspace_name='ks' and columnfamily_name='cf' "
                                   "allow filtering")
            return rows[0][0]

        compactions_1 = compactions_count()
        compactions_2 = compactions_1

        debug("Inserting data and waiting for new compaction")
        while compactions_1 == compactions_2:
            session.execute('insert into ks.cf (key, val) values ({},1);'.format(keys + 1))
            node1.flush()
            compactions_2 = compactions_count()
        node1.wait_for_compactions()

        compactions_2 = compactions_count()
        num_compactions = compactions_2 - compactions_1
        debug("{} compaction(s) completed".format(num_compactions))

        # reboot and verify that data  is not resurected
        debug("Stopping node1")
        node1.stop(gently=False)

        # verify that only some deletion markers will be kept since we reshard the files
        # and gc_period passed so some tombstones have been removed by compaction
        json_path = tempfile.mkstemp(suffix='.json')
        jname = json_path[1]
        with open(jname, 'w') as f:
            node1.run_sstable2json(f, keyspace='ks')

        with open(jname, 'r') as g:
            jsoninfo = g.read()

        numfound = jsoninfo.count("marked_deleted")
        debug("{} keys are now marked_deleted (0 {} expected < {})".format(numfound, "<" if num_compactions < 2 else "<=", keys))
        self.assertLess(numfound, keys)
        if num_compactions < 2:
            self.assertGreater(numfound, 0)

        debug("Restarting node1")
        node1.start(wait_for_binary_proto=True, jvm_args=['--smp', '2'])
        session = self.patient_cql_connection(node1, 'ks')
        debug("Verify that no data was resurrected")
        for x in range(0, keys):
            assert_none(session, 'select * from cf where key = ' + str(x))

        # trigger compaction on both shards
        debug("Waiting for compaction")
        node1.wait_for_compactions()
        compactions_1 = compactions_count()
        compactions_2 = compactions_1

        debug("Inserting data and waiting for new compaction")
        while compactions_1 + 2 > compactions_2:
            for x in range(keys*2, keys*3):
                session.execute('insert into ks.cf (key, val) values (' + str(x) + ',1);')
            node1.flush()
            compactions_2 = compactions_count()
        node1.wait_for_compactions()

        # validate that all deletion markers have been removed
        json_path = tempfile.mkstemp(suffix='.json')
        jname = json_path[1]
        with open(jname, 'w') as f:
            node1.run_sstable2json(f, keyspace='ks')

        with open(jname, 'r') as g:
            jsoninfo = g.read()

        numfound = jsoninfo.count("marked_deleted")
        debug("{} keys are now marked_deleted (Excpecting 0)".format(numfound))
        self.assertEqual(numfound, 0)

    def wait_for_new_minute(self):
        while dt.now().second > 5:
            time.sleep(1)

    def write_n_data_files(self, node, session, key_space, num_of_files, num_of_keys, consistency=ConsistencyLevel.ONE):
        for t in range(0, num_of_files):
            debug("Inserting concurrently {} keys...".format(num_of_keys))
            insert_c1c2(session, n=num_of_keys, consistency=consistency, ks=key_space)
            node.flush()

    @run_with_params(timestamp_resolution=["MILLISECONDS"])  # Commenting out "MICROSECONDS" options for now.
    def compact_data_by_time_window_test(self, timestamp_resolution):
        """
        1. Create TABLE with compaction_window_size of 1 MINUTES
        2. Insert data for 4 minutes while flushing to disk.
        3. Insert more data for 2 mins while flushing to disk
        4. Verify that the previous files created and compacted still exist.
        (Otherwise it means they were compacted wrongly).
        """
        debug("Starting a cluster of one node...")
        cluster = self.cluster
        if not cluster.nodelist():
            cluster.populate(1)
            [node1] = cluster.nodelist()
            node1.start(wait_for_binary_proto=True)
        nodes = cluster.nodelist()
        node1 = nodes[0]

        window_size_mins = 1

        session = self.patient_cql_connection(node1)
        key_space_name = 'ks_' + timestamp_resolution.lower()
        debug("Creating keyspace '%s'..." % key_space_name)
        self.create_ks(session, key_space_name, 1)

        debug("Creating a column family 'cf' with TWCS")
        self.create_cf(session, 'cf', columns={'c1': 'text', 'c2': 'text'},
                       compaction={'compaction_window_size': window_size_mins, 'compaction_window_unit': "MINUTES",
                                   'timestamp_resolution': timestamp_resolution,
                                   'class': 'TimeWindowCompactionStrategy'})

        # Wait for new minute to start before inserting data - keep the test consistent
        self.wait_for_new_minute()

        # Write data for x4 time than the window_size (i.e. 4 mins) - to have 4 different windows.
        for minute in range(0, window_size_mins * 4):
            # Assuming writing the files take LESS than a MINUTE
            self.write_n_data_files(node=node1, session=session, key_space=key_space_name, num_of_files=7, num_of_keys=10)
            self.wait_for_new_minute()

        # Get list of sstables names
        cf_dir = get_cf_dir(os.path.join(self.test_path, 'test', 'node1', 'data', key_space_name), 'cf')
        sstables_files1 = get_sstables_files(cf_dir, f_type='Data')
        debug("Files BEFORE: {}".format(sstables_files1))
        assert len(sstables_files1) > 0, "No SSTable files found in %s!" % cf_dir
        # Write additional data for 2 times the window-size (i.e. 2 mins)
        # (to verify that the original files remain the same and aren't compacted).
        for minute in range(0, window_size_mins * 2):
            # Assuming writing the files take LESS than a MINUTE
            self.write_n_data_files(node=node1, session=session,  key_space=key_space_name,num_of_files=7, num_of_keys=10)
            self.wait_for_new_minute()

        # Get list of sstables names
        cf_dir = get_cf_dir(os.path.join(self.test_path, 'test', 'node1', 'data', key_space_name), 'cf')
        sstables_files2 = get_sstables_files(cf_dir, f_type='Data')
        debug("Files AFTER adding data: {}".format(sstables_files2))

        assert sstables_files1.issubset(sstables_files2), "some of the original sstables are missing. " \
                                                          "Possibly due to wrong compaction" \
                                                          "Expecting {} but Found {}".format(sstables_files1,
                                                                                             sstables_files2)

    def major_compaction_with_several_timewindows_test(self):
        """
            Test major compaction will not bundle sstables from different time windows
            Test each time window (after major compaction) has only one table
        """
        debug("Starting a cluster of one node...")
        cluster = self.cluster
        cluster.populate(1)
        [node1] = cluster.nodelist()
        node1.start(wait_for_binary_proto=True)
        session = self.patient_cql_connection(node1)

        debug("Creating keyspace 'ks'...")
        min_threshold = 7
        self.create_ks(session, 'ks', 1)
        self.create_cf(session, 'cf', columns={'c1': 'text', 'c2': 'text'},
                       compaction={'compaction_window_size': '1', 'compaction_window_unit': 'MINUTES',
                                   'class': 'TimeWindowCompactionStrategy',
                                   'expired_sstable_check_frequency_seconds': '60',
                                   'min_threshold': min_threshold})

        # Write data in different time windows
        num_of_keys = 1000 * random.randint(1, 10)
        first_key = 0
        start = time.time()
        duration = random.randint(1, 120)
        debug("Will load data for {} seconds...".format(duration))
        while time.time() - start < duration:
            debug("Inserting keys {}..{}".format(first_key, first_key + num_of_keys - 1))
            insert_c1c2(session, keys=list(range(first_key, first_key + num_of_keys)), ks='ks')
            node1.flush()
            first_key += num_of_keys // 2

        def _get_time_window(timestamp):
            return int(timestamp / 60)

        number_of_time_windows = _get_time_window(time.time() - start)

        node1.stop();

        def _get_sstables_per_timewindow_dict(cf_dir):
            # get sstables for each time window dictionary
            time_window_dict = {}
            statistics_files = get_sstables_files(cf_dir, f_type='Statistics')
            ts = TestTimeWindowDataSegregation()
            for sf in statistics_files:
                stats = ts._get_stats(os.path.join(cf_dir, sf))
                min_time_window = _get_time_window(micros_to_seconds(stats['min_timestamp']))
                max_time_window = _get_time_window(micros_to_seconds(stats['max_timestamp']))
                debug("sf={} min_timestamp={} max_timestamp={} min_time_window={} max_time_window={}".format(sf,
                      stats['min_timestamp'], stats['max_timestamp'], min_time_window, max_time_window))
                for time_window in range(min_time_window, max_time_window + 1):
                    if time_window not in time_window_dict:
                        time_window_dict[time_window] = [sf]
                    else:
                        time_window_dict[time_window].append(sf)
            return time_window_dict

        # save sstable data (before major compaction
        ks_dir = os.path.join(node1.get_path(), 'data', 'ks')
        cf_dir = get_cf_dir(ks_dir, 'cf')
        time_window_dict_before_major_compaction = _get_sstables_per_timewindow_dict(cf_dir)
        debug("time_window_dict_before_major_compaction={}".format(time_window_dict_before_major_compaction))

        # another time window may sneak in if we cross the 1-minute window in one of the sstables
        self.assertGreaterEqual(len(time_window_dict_before_major_compaction.keys()), number_of_time_windows)
        self.assertLessEqual(len(time_window_dict_before_major_compaction.keys()), number_of_time_windows + 1)

        # Run major compaction
        node1.start();
        node1.compact()
        node1.wait_for_compactions()

        sstables_files_after_major_compaction = get_sstables_files(cf_dir, f_type='Data')
        time_window_dict_after_major_compaction = _get_sstables_per_timewindow_dict(cf_dir)
        debug("time_window_dict_after_major_compaction={}".format(time_window_dict_after_major_compaction))

        # no new data and consequently, time windows, are expected
        # verify that major compaction didn't mess any time windows
        self.assertEqual(len(time_window_dict_before_major_compaction.keys()), len(time_window_dict_after_major_compaction.keys()))

        # major compaction didn't bundle all sstables together
        # number off sstables after the major compaction equals number of time windows before major compaction
        self.assertEqual(len(time_window_dict_before_major_compaction.keys()),
                         len(sstables_files_after_major_compaction))
        # each time window (after major compaction) has only one table
        for sstables in time_window_dict_after_major_compaction.values():
            self.assertEqual(len(sstables), 1)

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
        cluster.populate(1)
        [node1] = cluster.nodelist()
        node1.start(wait_for_binary_proto=True)

        TIME_TO_SLEEP_BETWEEN_FILES = 15
        NUMBER_OF_FILES = 11
        NUMBER_OF_KEYS = 10
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

        # Always start the test at th beginning of the minute for consistent results
        self.wait_for_new_minute()

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
        sstables_files1 = get_sstables_files(cf_dir, f_type='Data')
        debug("sstables BEFORE SLEEP: {}".format(sstables_files1))

        debug("Sleep for {} seconds (TTL + GC) to let the files to completly TTL'ed".format(TTL+GC_GRACE))
        time.sleep(TTL+GC_GRACE)

        # Save the names of the current sstable files
        sstables_files2 = get_sstables_files(cf_dir, f_type='Data')
        debug("sstables AFTER SLEEP: {}".format(sstables_files2))

        # Even after the TTL+GC time has passed, the sstables remains till new data is inserted.
        # This assert just verifies that the files are still there.
        assert set(sstables_files1) == set(sstables_files2), \
            "Some or ALL of the files MISSING: {}".format(set(sstables_files1) - set(sstables_files2))

        debug("Orig files {} havn't been purged yet(expected)".format(sstables_files1.intersection(sstables_files2)))

        mark = node1.mark_log()
        # Insert one key to trigger a sstable expiration check (expired_sstable_check_frequency_seconds': '60').
        insert_c1c2(session, n=10, consistency=ConsistencyLevel.ONE)
        node1.flush()
        # Non mandatory Sleep, just to let any unfinished compaction to finish.
        time.sleep(5)
        # CHECK log: should have something like:
        # "Compacted 2 sstables to []. 36623 bytes to 0 (~0% of original) in 2ms = 0.00MB/s.
        #  ~512 total partitions merged to 0."
        found = node1.watch_log_for("Compacted [0-9]+ sstables to \[\]",
                                    timeout=5, from_mark=mark)
        debug(found)
        # Save the names of the current sstable files
        sstables_files2 = get_sstables_files(cf_dir, f_type='Data')
        debug("sstables AFTER INSERT more data and EXPIRATION OF older sstables: {}".format(sstables_files2))

        unpurged_files = set(sstables_files1).intersection(sstables_files2)
        self.assertFalse(unpurged_files, "PROBLEM Some of original files are still there and were NOT PURGED: {}".format(unpurged_files))

        debug("Purge SUCCEEDED, original files are not there {}".format(sstables_files2))


@attr('dtest-full', 'single_node')
class CompactionAdditionalStrategyTests(Tester):
    __test__ = False

    def __init__(self, *args, **kwargs):
        kwargs['cluster_options'] = {'start_rpc': 'true'}
        Tester.__init__(self, *args, **kwargs)

    def compaction_is_started_on_boot_test(self):
        cluster = self.cluster
        cluster.populate(1)
        [node1] = cluster.nodelist()
        node1.start(wait_for_binary_proto=True)

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
            for generation_suffix in range(10, 40):
                self._copy_sstable_file(f, "9999%d" % generation_suffix)

        before_start_sstables = sorted(glob.glob(os.path.join(keyspace_dir, 'cf' + '-*', '*-Data.db')))

        from_mark = node1.mark_log()
        node1.start()
        node1.watch_log_for(r'compaction -.*(Compacted|Resharded|Reshaped) [0-9]+ sstables to \[.+/data/ks/cf-.+\]', from_mark=from_mark)

        after_start_sstables = sorted(glob.glob(os.path.join(keyspace_dir, 'cf' + '-*', '*-Data.db')))

        self.assertNotEqual(before_start_sstables, after_start_sstables,
                            "No compaction detected after restarting {}. SSTables in ks/cf: {}".format(node1.name, after_start_sstables))

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

    @attr('next-gating')
    @attr('dtest-debug')
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
        cluster.populate(1)
        [node1] = cluster.nodelist()
        node1.start(wait_for_binary_proto=True)

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

        self.assertEqual(numfound, 1, "Error: expected 1 partition but found {}:\n{}".format(numfound, jsoninfo))


strategies = ['LeveledCompactionStrategy', 'SizeTieredCompactionStrategy', 'DateTieredCompactionStrategy',
              'TimeWindowCompactionStrategy']
for strategy in strategies:
    cls_name = ('CompactionAdditionalStrategyTests_with_' + strategy)
    vars()[cls_name] = type(cls_name, (CompactionAdditionalStrategyTests,), {'strategy': strategy, '__test__': True})


def micros_to_seconds(micros):
    return int(micros / (1000 * 1000))


def seconds_to_micros(seconds):
    return seconds * 1000 * 1000


@attr('dtest-full')
class TestTimeWindowDataSegregation(Tester):
    keyspace_name = "ks"
    table_name = "test"
    window_size = 1
    window_unit = "MINUTES"

    def _get_stats(self, statistics_file):
        with open(statistics_file, 'rb') as f:
            data = f.read()

        metadata = sstable_tools.statistics.parse(data, 'mc')
        return metadata['Stats']

    def _get_time_window_in_seconds(self, statistics_file, stats=None):
        if not stats:
            stats = self._get_stats(statistics_file)
        min_timestamp = stats['min_timestamp']
        max_timestamp = stats['max_timestamp']
        return micros_to_seconds(max_timestamp - min_timestamp)

    def _get_list_of_sstables(self, node):
        ks_path = os.path.join(node.get_path(), 'data', self.keyspace_name)
        statistics_files = []
        for dirpath, dirnames, filenames in os.walk(ks_path):
            elems = os.path.split(dirpath)
            # We are in the table's dir
            if elems[-1].startswith(self.table_name):
                statistics_files += [os.path.join(dirpath, f) for f in filenames if f.endswith('-Statistics.db')]
                continue

            # prune all dirs that are not a table dir
            for d in dirnames:
                if not d.startswith(self.table_name):
                    dirnames.remove(d)

        return statistics_files

    def _check_sstable_timestamps(self, node):
        statistics_files = self._get_list_of_sstables(node)
        self.assertTrue(len(statistics_files) > 0)
        for sf in statistics_files:
            stats = self._get_stats(sf)
            tw = self._get_time_window_in_seconds(sf, stats)

            # Allow an error margin of a half-window.
            margin = 1.5 * self.window_size * 60
            self.assertTrue(tw <= margin, msg="time window of {} seconds is greater than {} seconds margin: "
                            "sstable={} min_timestamp={} max_timestamp={}".format(tw, margin, sf, stats['min_timestamp'], stats['max_timestamp']))

    def _create_ks_cl_with_twcs(self, session, rf=1):

        session.execute("CREATE KEYSPACE {} WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': {}}}".format(self.keyspace_name, rf))
        session.execute(
            "CREATE TABLE {0.keyspace_name}.{0.table_name} (pk int, ck int, v int, PRIMARY KEY(pk, ck))"
            "WITH compaction = {{"
            "'class': 'TimeWindowCompactionStrategy',"
            "'compaction_window_unit': '{0.window_unit}',"
            "'compaction_window_size': {0.window_size} }}".format(self))

    def _simulate_write_process_in_minutes(self, session, duration_minutes=20, start_from_minute=0, flush_period_seconds=30, flushing_exclude_nodes=None, num_pks=10):
        """Simulate a write process across duration minutes.

        We use `USING TIMESTAMP` to distribute the writes evenly
        across the entire range, simulating a write every second (to
        several partitions).
        flush_period_seconds allow to control how many time windows could be
        in sstable

        Arguments:
            session {Session} -- opened session to node

        Keyword Arguments:
            duration_minutes {number} -- how many minutes to sumilate (default: {20})
            flush_period_seconds {number} -- in how many seconds flush memtable (default: {30})
            start_from_minute {number} -- start minute to write data
            flushing_nodes {list} -- list of nodes, which should be flushed.

        """
        exclude_nodes = flushing_exclude_nodes if flushing_exclude_nodes else []
        insert_statement = session.prepare("INSERT INTO {}.{} (pk, ck, v) VALUES (?, ?, ?)"
                                           "USING TIMESTAMP ?".format(self.keyspace_name, self.table_name))
        rand_pks = set()
        while len(rand_pks) < num_pks:
            rand_pks.add(random.randint(-2147483647, 2147483647))
        flushing_nodes = [node for node in self.cluster.nodelist() if node not in exclude_nodes]
        for t in range(start_from_minute * 60, duration_minutes * 60):
            concurrent.execute_concurrent_with_args(
                session,
                insert_statement,
                [(pk, t, 0, seconds_to_micros(t)) for pk in rand_pks])

            # Flush every flush period in seconds on each node
            if t % flush_period_seconds == 0:
                for node in flushing_nodes:
                    node.flush()

    def _list_sstable_timestamps(self, node):
        statistics_files = self._get_list_of_sstables(node)
        list_sstables_timewindows = []
        for sf in statistics_files:
            time_window = self._get_time_window_in_seconds(sf)
            list_sstables_timewindows.append((sf, time_window))

        return list_sstables_timewindows

    def test_streaming_during_adding_node_with_boostrap(self):
        self.cluster.populate(1).start(wait_for_binary_proto=True)

        node1 = self.cluster.nodelist()[0]
        session = self.patient_cql_connection(node1)
        self._create_ks_cl_with_twcs(session, rf=1)

        self._simulate_write_process_in_minutes(session, duration_minutes=20)

        # Not really relevant to the test, just for sanity.
        self._check_sstable_timestamps(node1)

        # node added with bootstrap enabled
        node2 = tools.new_node(self.cluster)
        node2.start(wait_for_binary_proto=True)
        # After streaming the new node should also have at max one
        # window per sstable.
        self._check_sstable_timestamps(node2)

    def test_streaming_decommission(self):
        self.cluster.populate(2).start(wait_for_binary_proto=True)

        node1, node2 = self.cluster.nodelist()  # type: ScyllaNode
        session = self.patient_cql_connection(node1)
        self._create_ks_cl_with_twcs(session, rf=1)

        self._simulate_write_process_in_minutes(session, duration_minutes=20)
        # Not really relevant to the test, just for sanity.
        self._check_sstable_timestamps(node1)
        self._check_sstable_timestamps(node2)

        # run decommossion for node1
        node1.decommission()
        # After streaming the left node should also have at max one
        # window per sstable.
        self._check_sstable_timestamps(node2)

    def test_streaming_on_repair(self):
        self.cluster.populate(2).start(wait_for_binary_proto=True)

        node1, node2 = self.cluster.nodelist()  # type: ScyllaNode
        session = self.patient_cql_connection(node1)
        self._create_ks_cl_with_twcs(session, rf=2)

        self._simulate_write_process_in_minutes(session, duration_minutes=10)
        self._check_sstable_timestamps(node1)
        self._check_sstable_timestamps(node2)
        node2.stop()
        self._simulate_write_process_in_minutes(session, duration_minutes=20, start_from_minute=10,
                                                flushing_exclude_nodes=[node2])
        self._check_sstable_timestamps(node1)
        node2.start(wait_for_binary_proto=True)
        node2.repair(['-seq', self.keyspace_name])

        self._check_sstable_timestamps(node1)
        self._check_sstable_timestamps(node2)

    @attr('next-gating')
    def test_streaming_on_rebuild_multidc(self):

        def _add_node(i, dc):
            return self.cluster.new_node(i, debug=True, data_center=dc)

        self.cluster.set_configuration_options(values={'endpoint_snitch': 'GossipingPropertyFileSnitch'})
        node1 = _add_node(1, 'dc1')  # type: ScyllaNode

        # start node in dc1
        node1.start(wait_for_binary_proto=True)

        # populate data in dc1
        session = self.patient_exclusive_cql_connection(node1)
        session.execute("CREATE KEYSPACE {} "
                        " WITH replication = {{"
                        "'class': 'NetworkTopologyStrategy', 'dc1':1}}".format(self.keyspace_name))
        session.execute("CREATE TABLE {}.{} (pk int, ck int, v int, PRIMARY KEY(pk, ck))"
                        " WITH compaction = {{"
                        "'class': 'TimeWindowCompactionStrategy',"
                        "'compaction_window_unit': 'MINUTES',"
                        "'compaction_window_size': {}}}".format(self.keyspace_name, self.table_name, self.window_size))
        session = self.patient_cql_connection(node1)
        self._simulate_write_process_in_minutes(session, duration_minutes=10)
        self._check_sstable_timestamps(node1)
        # Bootstraping a new node in dc2 with auto_bootstrap: false
        node2 = _add_node(2, 'dc2')  # type=ScyllaNode
        node2.start(wait_other_notice=True, wait_for_binary_proto=True)

        # wait for snitch to reload
        node2.watch_log_for("init - Scylla.*initialization completed")
        # alter keyspace to replicate to dc2
        session = self.patient_exclusive_cql_connection(node2)
        session.execute("ALTER KEYSPACE {} WITH replication = {{'class':'NetworkTopologyStrategy', 'dc1':1, 'dc2':1}};".format(self.keyspace_name))

        self.rebuild_errors = 0
        self.unexpected_errors = 0
        mark = node2.mark_log()

        # rebuild dc2 from dc1
        def rebuild():
            try:
                node2.nodetool('rebuild dc1')
            except NodetoolError as e:
                if 'rebuild is in progress' in str(e):
                    self.rebuild_errors += 1
                else:
                    debug('Unexpected rebuild failure {}'.format(str(e)))
                    self.unexpected_errors += 1

        cmd1 = Thread(target=rebuild)
        cmd1.start()
        cmd1.join()

        self.assertEqual(self.unexpected_errors, 0,
                         msg='unexpected rebuild errors encountered.')

        node2.watch_log_for("Streaming for rebuild successful|"
                            "rebuild_with_repair: finished with keyspace=ks", from_mark=mark)
        node2.wait_for_compactions()
        self._check_sstable_timestamps(node2)

    def test_streaming_sstables_with_several_timewindows(self):
        self.cluster.populate(2).start(wait_for_binary_proto=True)

        node1 = self.cluster.nodelist()[0]
        session = self.patient_cql_connection(node1)
        self._create_ks_cl_with_twcs(session, rf=2)

        self._simulate_write_process_in_minutes(session, duration_minutes=10, flush_period_seconds=120, num_pks=100)

        sstable_timewindows_list = self._list_sstable_timestamps(node1)
        for sstable, timewindow in sstable_timewindows_list:
            self.assertTrue(timewindow <= 1.5 * 2 * 60)

        new_node = tools.new_node(self.cluster)
        new_node.start(wait_for_binary_proto=True)

        self._check_sstable_timestamps(new_node)

    def test_rebuild_node_streaming(self):
        self.cluster.populate(3).start(wait_for_binary_proto=True)
        node1 = self.cluster.nodelist()[0]  # type: ScyllaNode
        session = self.patient_cql_connection(node1)
        self._create_ks_cl_with_twcs(session, rf=3)

        self._simulate_write_process_in_minutes(session, duration_minutes=10, flush_period_seconds=20)
        for node in self.cluster.nodelist():
            self._check_sstable_timestamps(node)
        # stop node and remove all data
        node3 = self.cluster.nodelist()[2]  # type: ScyllaNode
        node3.stop()
        data_dir = os.path.join(node3.get_path(), 'data', self.keyspace_name)
        shutil.rmtree(data_dir, ignore_errors=True)
        # start node and rebuild
        node3.start(wait_other_notice=True, wait_for_binary_proto=True)
        mark = node3.mark_log()
        node3.nodetool('rebuild')
        node3.watch_log_for("Streaming for rebuild successful|"
                            "rebuild_with_repair: finished with keyspace=ks", from_mark=mark)
        node3.wait_for_compactions()
        self._check_sstable_timestamps(node3)
