import datetime
import glob
import itertools
import logging
import os
import random
import re
import shutil
import tempfile
import time
from collections import namedtuple
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime as dt
from pathlib import Path
from threading import Thread
from typing import Optional, List

import pytest
import sstable_tools.statistics
from cassandra import ConsistencyLevel, concurrent
from ccmlib.node import NodetoolError, TimeoutError, Node

from dtest_class import Tester, create_ks, create_cf
from dtest_setup_overrides import DTestSetupOverrides
from tools.assertions import assert_none, assert_all, assert_row_count
from tools.cluster import new_node
from tools.data import insert_c1c2, delete_c1c2, run_in_parallel, create_c1c2_table
from tools.files import copy_files_to, get_node_cf_dir, get_sstables_files, get_list_of_sstables, \
    check_file_lists_are_equal
from tools.marks import enterprise_only_param
from tools.misc import ImmutableMapping
from tools.rest_clients import StorageServiceClient
from tools.stress import fill_data_by_cs

logger = logging.getLogger(__name__)


def generate_ids(val):
    if hasattr(val, 'marks'):  # it's a pytest ParameterSet
        return f"{val.values[0]['class']}"
    return f"{val['class']}"


SpanningSStable = namedtuple("SpanningSStable", ["is_spanning_one_window",
                                                 "min_timestamp_seconds", "max_timestamp_seconds"])


class CompactionAdditionalTester(Tester):

    def prepare(self, nodes, wait_for_binary_proto=True, jvm_args=None, configuration_options={}):
        configuration_options.update({'enable_sstable_key_validation': True})
        self.cluster.set_configuration_options(values=configuration_options)
        self.cluster.populate(nodes).start(wait_for_binary_proto=wait_for_binary_proto, jvm_args=jvm_args)
        node1 = self.cluster.nodelist()[0]
        return self.cluster.nodelist(), self.patient_cql_connection(node1)

    @staticmethod
    def get_stats(statistics_file):
        with open(statistics_file, 'rb') as f:
            data = f.read()

        metadata = sstable_tools.statistics.parse(data, 'mc')
        return metadata['Stats']

    @staticmethod
    def micros_to_seconds(micros):
        return micros // (1000 * 1000)

    @staticmethod
    def seconds_to_micros(seconds):
        return seconds * 1000 * 1000


def get_strategies_upgrade_options():
    _strategies = [
        # Expect sstables are more than min_threshold in level 0
        {'class': 'LeveledCompactionStrategy', 'sstable_size_in_mb': 1, 'min_threshold': 2},
        # Expect sstables are generated in multiple minutes for TimeWindowCompactionStrategy
        {'class': 'TimeWindowCompactionStrategy', 'split_during_flush': False, 'compaction_window_size': 1,
         'compaction_window_unit': 'MINUTES', 'min_threshold': 2},
        # Expect there are more sstables than min_threshold in same bucket
        {'class': 'SizeTieredCompactionStrategy', 'bucket_high': 1.5, 'bucket_low': 0.5,
         'min_sstable_size': 1, 'min_threshold': 2},
        # DateTieredCompactionStrategy is deprecated
        # {'class': 'DateTieredCompactionStrategy'},
        # disabling for now, until we can figure when reshaping is expected in this case
        # scylladb/scylla#9944 would help with that
        # {'class': 'IncrementalCompactionStrategy'}
    ]

    output = []
    for pair in itertools.product(_strategies, _strategies):
        if pair[0]['class'] == 'IncrementalCompactionStrategy' or \
           pair[1]['class'] == 'IncrementalCompactionStrategy':
            output.append(enterprise_only_param(*pair))
        else:
            output.append(pair)
    return output


@pytest.mark.dtest_full
@pytest.mark.single_node
class TestCompactionAdditional(CompactionAdditionalTester):

    @pytest.mark.next_gating
    @pytest.mark.dtest_debug
    @pytest.mark.single_node
    def test_compaction_delete_with_smp_change(self):
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
        logger.debug("Starting node1 with 1 cpu")
        [node1], session = self.prepare(1)
        create_ks(session, 'ks', 1)

        gc_grace_seconds = 5
        keys = 100
        logger.debug(f"Inserting {keys} keys with gc_grace_seconds={gc_grace_seconds}")
        session.execute(
            f"create table ks.cf (key int PRIMARY KEY, val int) with compaction = {{'class':'SizeTieredCompactionStrategy'}} and gc_grace_seconds = {gc_grace_seconds};")

        for x in range(0, keys):
            session.execute(f'insert into cf (key, val) values ({x},1)')

        node1.flush()
        node1.compact()
        logger.debug("Restarting node1 with 2 cpus")
        node1.stop()
        node1.start(wait_for_binary_proto=True, jvm_args=['--smp', '2'])

        session = self.patient_cql_connection(node1, 'ks')
        logger.debug(f"Deleting {keys} keys")
        for x in range(0, keys):
            session.execute(f'delete from cf where key = {x}')
        node1.flush()

        def compactions_count():
            rows = session.execute("select count(*) from system.compaction_history "
                                   "where keyspace_name='ks' and columnfamily_name='cf' "
                                   "allow filtering")
            return rows[0][0]

        compactions_1 = compactions_count()
        compactions_2 = compactions_1

        logger.debug(f"Waiting gc_grace_seconds={gc_grace_seconds} to pass")
        time.sleep(gc_grace_seconds + 1)

        # we passed gc_period and force an update so that compaction will
        # be triggered on a single shard (removing data and tombstone)
        logger.debug("Inserting data and waiting for new compaction")
        while compactions_1 == compactions_2:
            session.execute(f'insert into ks.cf (key, val) values ({keys + 1},1);')
            node1.flush()
            compactions_2 = compactions_count()
        node1.wait_for_compactions()

        compactions_2 = compactions_count()
        num_compactions = compactions_2 - compactions_1
        logger.debug(f"{num_compactions} compaction(s) completed")

        # reboot and verify that data  is not resurected
        logger.debug("Stopping node1")
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
        logger.debug("{} keys are now marked_deleted (0 {} expected < {})".format(
            numfound, "<" if num_compactions < 2 else "<=", keys))
        assert numfound < keys, f"Number of found tombstones {numfound} greater than number of keys {keys}"
        if num_compactions < 2:
            assert numfound > 0, f"Number of found tombstones {numfound} != 0"

        logger.debug("Restarting node1")
        node1.start(wait_for_binary_proto=True, jvm_args=['--smp', '2'])
        session = self.patient_cql_connection(node1, 'ks')
        logger.debug("Verify that no data was resurrected")
        for x in range(0, keys):
            assert_none(session, f'select * from cf where key = {x}')

        # trigger compaction on both shards
        logger.debug("Waiting for compaction")
        node1.wait_for_compactions()
        compactions_1 = compactions_count()
        compactions_2 = compactions_1

        logger.debug("Inserting data and waiting for new compaction")
        while compactions_1 + 2 > compactions_2:
            for x in range(keys * 2, keys * 3):
                session.execute(f'insert into ks.cf (key, val) values ({x},1);')
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
        logger.debug(f"{numfound} keys are now marked_deleted (Excpecting 0)")
        assert numfound == 0, "Not all tombstones were removed during compactions"

    @pytest.mark.single_node
    @pytest.mark.parametrize("timestamp_resolution", ["MILLISECONDS"])
    def test_compact_data_by_time_window(self, timestamp_resolution):
        """
        1. Create TABLE with compaction_window_size of 1 MINUTES
        2. Insert data for 4 minutes while flushing to disk.
        3. Insert more data for 2 mins while flushing to disk
        4. Verify that the previous files created and compacted still exist.
        (Otherwise it means they were compacted wrongly).
        """
        logger.debug("Starting a cluster of one node...")
        [node1], session = self.prepare(1)

        window_size_mins = 1

        session = self.patient_cql_connection(node1)
        key_space_name = 'ks_' + timestamp_resolution.lower()
        logger.debug("Creating keyspace '%s'..." % key_space_name)
        create_ks(session, key_space_name, 1)

        logger.debug("Creating a column family 'cf' with TWCS")
        create_cf(session, 'cf', columns={'c1': 'text', 'c2': 'text'},
                  compaction={'compaction_window_size': window_size_mins, 'compaction_window_unit': "MINUTES",
                              'timestamp_resolution': timestamp_resolution,
                              'class': 'TimeWindowCompactionStrategy'})

        # Wait for new minute to start before inserting data - keep the test consistent
        self.wait_for_new_minute()

        # Write data for x4 time than the window_size (i.e. 4 mins) - to have 4 different windows.
        for minute in range(0, window_size_mins * 4):
            # Assuming writing the files take LESS than a MINUTE
            self.write_n_data_files(node=node1, session=session, key_space=key_space_name,
                                    num_of_files=7, num_of_keys=10)
            self.wait_for_new_minute()

        # Get list of sstables names
        cf_dir = get_node_cf_dir(node1, key_space_name, 'cf')
        sstables_files1 = get_sstables_files(cf_dir, f_type='Data')
        logger.debug("Files BEFORE: {}".format(sstables_files1))
        assert len(sstables_files1) > 0, "No SSTable files found in %s!" % cf_dir
        # Write additional data for 2 times the window-size (i.e. 2 mins)
        # (to verify that the original files remain the same and aren't compacted).
        for minute in range(0, window_size_mins * 2):
            # Assuming writing the files take LESS than a MINUTE
            self.write_n_data_files(node=node1, session=session, key_space=key_space_name,
                                    num_of_files=7, num_of_keys=10)
            self.wait_for_new_minute()

        # Get list of sstables names
        sstables_files2 = get_sstables_files(cf_dir, f_type='Data')
        logger.debug("Files AFTER adding data: {}".format(sstables_files2))

        assert sstables_files1.issubset(sstables_files2), "some of the original sstables are missing. " \
                                                          "Possibly due to wrong compaction" \
                                                          "Expecting {} but Found {}".format(sstables_files1,
                                                                                             sstables_files2)

    @pytest.mark.single_node
    def test_major_compaction_with_several_timewindows(self):
        """
            Test major compaction will not bundle sstables from different time windows
            Test each time window (after major compaction) has only one table
        """
        logger.debug("Starting a cluster of one node...")
        [node1], session = self.prepare(1)

        logger.debug("Creating keyspace 'ks'...")
        min_threshold = 7
        create_ks(session, 'ks', 1)
        create_cf(session, 'cf', columns={'c1': 'text', 'c2': 'text'},
                  compaction={'compaction_window_size': '1', 'compaction_window_unit': 'MINUTES',
                              'class': 'TimeWindowCompactionStrategy',
                              'expired_sstable_check_frequency_seconds': '60',
                              'min_threshold': min_threshold})

        # Write data in different time windows
        num_of_keys = 1000 * random.randint(1, 10)
        first_key = 0
        start = time.time()
        duration = random.randint(1, 120)
        logger.debug("Will load data for {} seconds...".format(duration))
        while time.time() - start < duration:
            logger.debug(f"Inserting keys {first_key}..{first_key + num_of_keys -1}")
            insert_c1c2(session, keys=list(range(first_key, first_key + num_of_keys)), ks='ks')
            node1.flush()
            first_key += num_of_keys // 2

        def _get_time_window(timestamp):
            return int(timestamp / 60)

        number_of_time_windows = _get_time_window(time.time()) - _get_time_window(start) + 1

        node1.stop()

        def _get_sstables_per_timewindow_dict(cf_dir):
            # get sstables for each time window dictionary
            time_window_dict = {}
            statistics_files = get_sstables_files(cf_dir, f_type='Statistics')

            for sf in statistics_files:
                stats = self.get_stats(os.path.join(cf_dir, sf))
                min_time_window = _get_time_window(self.micros_to_seconds(stats['min_timestamp']))
                max_time_window = _get_time_window(self.micros_to_seconds(stats['max_timestamp']))
                logger.debug("sf={} min_timestamp={} max_timestamp={} min_time_window={} max_time_window={}".format(
                    sf,
                    stats['min_timestamp'], stats['max_timestamp'], min_time_window, max_time_window))
                for time_window in range(min_time_window, max_time_window + 1):
                    if time_window not in time_window_dict:
                        time_window_dict[time_window] = [sf]
                    else:
                        time_window_dict[time_window].append(sf)
            return time_window_dict

        # save sstable data (before major compaction
        cf_dir = get_node_cf_dir(node1, 'ks', 'cf')
        time_window_dict_before_major_compaction = _get_sstables_per_timewindow_dict(cf_dir)
        logger.debug(f"time_window_dict_before_major_compaction={time_window_dict_before_major_compaction}")

        # another time window may sneak in if we cross the 1-minute window in one of the sstables
        time_windows_before_major_compaction = len(time_window_dict_before_major_compaction.keys())
        assert time_windows_before_major_compaction >= number_of_time_windows - 1, \
            f"Time window {time_windows_before_major_compaction} less than number of time windows {number_of_time_windows - 1}"
        assert time_windows_before_major_compaction <= number_of_time_windows + 1, \
            f"Time window {time_windows_before_major_compaction} greater than number of time windows {number_of_time_windows - 1}"

        # Run major compaction
        node1.start()
        node1.compact()
        node1.wait_for_compactions()

        sstables_files_after_major_compaction = get_sstables_files(cf_dir, f_type='Data')
        time_window_dict_after_major_compaction = _get_sstables_per_timewindow_dict(cf_dir)
        logger.debug(f"time_window_dict_after_major_compaction={time_window_dict_after_major_compaction}")

        # no new data and consequently, time windows, are expected
        # verify that major compaction didn't mess any time windows
        time_windows_after_major_compaction = len(time_window_dict_after_major_compaction.keys())
        assert time_windows_before_major_compaction == time_windows_after_major_compaction

        # major compaction didn't bundle all sstables together
        # number off sstables after the major compaction equals number of time windows before major compaction
        number_of_shards = getattr(node1, '_smp', 1)
        expected_number_of_sstables = time_windows_after_major_compaction * number_of_shards
        assert expected_number_of_sstables == len(sstables_files_after_major_compaction)
        # each time window (after major compaction) has only one table
        for sstables in time_window_dict_after_major_compaction.values():
            assert len(sstables) == number_of_shards

    @pytest.mark.single_node
    def test_compaction_removes_ttld_data_by_time_windows(self):
        """
        Test that TWCS compaction removes TTLd data after gc_period by time windows
        2. Create a table with a DEFAULT TTL=70 and gc_period=10.
        3. Insert data into the table.
        4. Wait past ttl and gc_period
        5. write some data and force compaction
        6. check that ttl'd data was removed
        """

        logger.debug("Starting a cluster of one node...")
        [node1], session = self.prepare(1)

        TIME_TO_SLEEP_BETWEEN_FILES = 15
        NUMBER_OF_FILES = 11
        NUMBER_OF_KEYS = 10
        TTL = 70
        GC_GRACE = 10

        logger.debug("Creating keyspace 'ks'...")
        create_ks(session, 'ks', 1)

        # DEFAULT TTL set to 70, gc_grace set to 10 and expiry check set to 60.
        # It means that every 60 seconds it should purge all sstabls that are older than 180+30
        logger.debug(f"Creating a column family 'cf' with TWCS and DEFAULT TTL of {TTL}")
        create_cf(session, 'cf', gc_grace=GC_GRACE, columns={'c1': 'text', 'c2': 'text'}, default_ttl=TTL,
                  compaction={'compaction_window_size': '1', 'compaction_window_unit': 'MINUTES',
                              'class': 'TimeWindowCompactionStrategy',
                              'expired_sstable_check_frequency_seconds': '60'})

        # Always start the test at th beginning of the minute for consistent results
        self.wait_for_new_minute()

        for t in range(0, NUMBER_OF_FILES):
            logger.debug(f"Inserting concurrently {NUMBER_OF_KEYS} keys...")
            insert_c1c2(session, n=NUMBER_OF_KEYS, consistency=ConsistencyLevel.ONE)
            node1.flush()
            time.sleep(TIME_TO_SLEEP_BETWEEN_FILES)

        node1.flush()
        cf_dir = get_node_cf_dir(node1, 'ks', 'cf')
        logger.debug(f"'cf' directory is {cf_dir}")

        # Save the names of the current sstable files
        sstables_files1 = get_sstables_files(cf_dir, f_type='Data')
        logger.debug(f"sstables BEFORE SLEEP: {sstables_files1}")

        logger.debug(f"Sleep for {TTL + GC_GRACE} seconds (TTL + GC) to let the files to completly TTL'ed")
        time.sleep(TTL + GC_GRACE)

        # Save the names of the current sstable files
        sstables_files2 = get_sstables_files(cf_dir, f_type='Data')
        logger.debug(f"sstables AFTER SLEEP: {sstables_files2}")

        # Even after the TTL+GC time has passed, the sstables remains till new data is inserted.
        # This assert just verifies that the files are still there.
        assert set(sstables_files1) == set(sstables_files2), \
            f"Some or ALL of the files MISSING: {set(sstables_files1) - set(sstables_files2)}"

        logger.debug(f"Orig files {sstables_files1.intersection(sstables_files2)} havn't been purged yet(expected)")

        mark = node1.mark_log()
        # Insert one key to trigger a sstable expiration check (expired_sstable_check_frequency_seconds': '60').
        insert_c1c2(session, n=10, consistency=ConsistencyLevel.ONE)
        node1.flush()
        node1.wait_for_compactions()
        # CHECK log: should have something like:
        # "Compacted 2 sstables to []. 36623 bytes to 0 (~0% of original) in 2ms = 0.00MB/s.
        #  ~512 total partitions merged to 0."
        found = node1.watch_log_for(r"Compact ks.cf .* Compacted [0-9]+ sstables to \[\]",
                                    timeout=5, from_mark=mark)
        logger.debug(found)
        # Save the names of the current sstable files
        sstables_files2 = get_sstables_files(cf_dir, f_type='Data')
        logger.debug(f"sstables AFTER INSERT more data and EXPIRATION OF older sstables: {sstables_files2}")

        unpurged_files = set(sstables_files1).intersection(sstables_files2)
        assert not unpurged_files, f"PROBLEM Some of original files are still there and were NOT PURGED: {unpurged_files}"

        logger.debug(f"Purge SUCCEEDED, original files are not there {sstables_files2}")

    @pytest.mark.single_node
    @pytest.mark.parametrize("strategy1,strategy2", get_strategies_upgrade_options(), ids=generate_ids)
    def test_refresh_and_restart_after_compaction_strategy_change(self, strategy1, strategy2):
        """
        This test tries to loade backup sstable by refresh and restart after changing the compaction strange.
        refreshing loads sstable from upload directory, and sstable in staging or main sstable directory will
        be loaded in cf populating during restart.

        Reshaping will only be triggered conditionally if current compaction strategy isn't satisfied.

        LeveledCompactionStrategy:
        - level 0 has more sstables than min_threshold (strict mode) or max_compaction_threshold (relax_mode) (covered in the test)
        - have 10% overlapping sstables on same level
        - sstable level out of MAX level (9)

        SizeTieredCompactionStrategy:
        - have more than min_threshold similar-sized SSTables

        TimeWindowCompactionStrategy:
        - have sstables that span more than 1 window
        - a given window has more than min_threshold SSTables.
        - Time-Window Compaction Strategy compacts SSTables within each time window
          using Size-tiered Compaction Strategy (STCS)

        DateTieredCompactionStrategy:
        - doesn't support reshaping
        """
        cluster = self.cluster
        cluster.populate(1)
        node1 = cluster.nodelist()[0]
        node1.start(wait_for_binary_proto=True)

        node1 = self.cluster.nodelist()[0]
        session = self.patient_cql_connection(node1)
        session.execute("DROP KEYSPACE IF EXISTS keyspace1")

        logger.debug(f"Create test table with {strategy1}")
        node1.stress(['write', 'n=0', 'no-warmup', '-schema', 'replication(factor=1)', '-rate', 'threads=1'])

        session.execute(f"ALTER TABLE keyspace1.standard1 WITH compaction={strategy1}")

        logger.debug("Insert test data by cassandra-stress and compact")
        # Use multiple workload to generate multiple sstables, then it's easy to reach the threshold for reshaping

        fill_data_by_cs(node1, n_range=[500, 550, 600, 650])
        # Compact initiatively, make sure there are some compacted sstables before disable autocompaction
        node1.compact()

        # Here we disable autocompaction for leaving all sstables in level 0, then
        # it's easy to trigger reshape with small dataset during restart (strict mode).
        # Actually it's not always necessary.
        #
        # Refreshing from upload will use strict mode reshape, restart population
        # from staging or main sstable directory will use relax mode reshape.
        # Only in relaxed mode, all sstables will be mutated to level to 0, reshaping
        # will be trigger very easily.

        logger.debug('disable autocompaction to leave all sstables to level 0')
        node1.nodetool('disableautocompaction keyspace1 standard1')

        logger.debug("Insert test data by cassandra-stress without compacting, leave it for next strategy")

        if (strategy2['class'] == 'TimeWindowCompactionStrategy'):
            # Prepare a sstable spans two 1 window, (window unit is 60 seconds)
            fill_data_by_cs(node1, n_range=[], duration_range=[70],
                            other_opt=['-rate', 'threads=1', '-col', 'size=FIXED(1024)'])
        elif (strategy2['class'] == 'LeveledCompactionStrategy'):
            # Need more than 10% overlapping sstables on same level
            fill_data_by_cs(node1, n_range=[500, 550, 600, 650, 1000], start=5000)
        elif (strategy2['class'] == 'SizeTieredCompactionStrategy'):
            fill_data_by_cs(node1, n_range=[500, 550, 600, 650, 2000, 5000] * 2, start=10000)
        else:
            fill_data_by_cs(node1, n_range=[500, 550, 600, 650], start=5000)

        cf_dir = get_node_cf_dir(node1, 'keyspace1', 'standard1', latest=True)
        logger.debug(cf_dir)

        # Prepare for cf population during restart # subtest1
        copy_files_to(cf_dir, os.path.join(cf_dir, './staging/'), files_only=True)
        # Prepare for refresh  # subtest2
        copy_files_to(cf_dir, os.path.join(cf_dir, './upload/'), files_only=True)

        # For troubleshot
        copy_files_to(cf_dir, os.path.join(cf_dir, f'./backup.{time.time()}/'),
                      files_only=True, create_to_dir=True)

        logger.info(f"Change table compaction strategy to {strategy2}")
        session.execute(f"ALTER TABLE keyspace1.standard1 WITH compaction={strategy2}")

        def assert_reshape_and_verify_data(srcdir, log_mark, verify_reshape=True):
            """
            Check Reshaping really happens and verify the loaded data by cs read
            """
            try:
                res = node1.watch_log_for("Reshape keyspace1.standard1", timeout=5, from_mark=log_mark)
                logger.debug(res)
            except TimeoutError:
                res = None
                msg = f"Reshape didn't occur after loading sstables from {srcdir} directory"
                if (strategy2['class'] not in ['DateTieredCompactionStrategy', 'SizeTieredCompactionStrategy'] and verify_reshape):
                    if strategy2['class'] != strategy1['class']:
                        assert res is not None, f"Reshape didn't occurred in loading sstables from {srcdir} directory"
                    else:
                        logger.debug(msg + ", as expected.")

            logger.info(f'Verify data is loaded from {srcdir} directory')
            node1.stress(['read', 'n=100', 'no-warmup', '-rate', 'threads=10', '-col', 'size=FIXED(1024)'])

        logger.debug("Clean test data & sstables before subtest by TRUNCATE")
        session.execute("TRUNCATE keyspace1.standard1")

        logger.debug("Re-enable autocompaction, otherwise compaction & reshape wont' work in restart and refresh")
        node1.nodetool('enableautocompaction keyspace1 standard1')

        logger.info('Load data from upload directory by refresh')
        mark = node1.mark_log()
        logger.info('Refresh keyspace1.standard1 .....')
        node1.nodetool("refresh -- keyspace1 standard1")
        assert_reshape_and_verify_data(srcdir='upload/', log_mark=mark)

        logger.debug("Clean test data & sstables before subtest by TRUNCATE")
        session.execute("TRUNCATE keyspace1.standard1")
        logger.info('Restart to load sstables from staging directory')
        mark = node1.mark_log()
        logger.info("Restart the node .....")
        node1.stop(gently=True)
        node1.start(wait_for_binary_proto=True)
        session = self.patient_cql_connection(node1)
        verify_reshape = not (
            strategy2['class'] == 'LeveledCompactionStrategy' and strategy1['class'] != 'LeveledCompactionStrategy')
        assert_reshape_and_verify_data(srcdir='staging/', log_mark=mark, verify_reshape=verify_reshape)

        shutil.rmtree(os.path.join(node1.get_path(), 'data', 'keyspace1'))

    @pytest.mark.parametrize("cf_sizes", [(100, 10_000, 100_000),  # cf_3 > cf_2 > cf_1
                                          (10_000, 100, 100_000),  # cf_2 > cf 1 > cf_3
                                          (10_000, 100_000, 100),  # cf_3 > cf_1 > cf_2
                                          (100, 100_000, 10_000),  # cf_2 > cf_3 > cf_1
                                          (100_000, 10_000, 100),  # cf_1 > cf_2 > cf_3
                                          (100_000, 100, 10_00)],  # cf_1 > cf_3 > cf_2
                             ids=["100-10_000-100_000",
                                  "10_000-100-100_000",
                                  "10_000-100_000-100",
                                  "100-100_000-10_000",
                                  "100_000-10_000-100",
                                  "100_000-100-10_00"])
    @pytest.mark.single_node
    @pytest.mark.dtest_full
    def test_major_compaction_processes_tables_in_order_by_size(self, cf_sizes: tuple):
        """
        Major compaction should process tables in a sorted order,
        from smallest to largest. The test checks if the ordering is correct.

        Steps:
        1. Create 3 tables of different sizes.
        2. Trigger a major compaction.
        3. Query the system.compaction_history table to get the history of
        compactions.
        4. Sort the query results by time and by table size.
        5. Assert that the 2 sorts give identical results.
        """
        @dataclass
        class CfSizeTime:
            name: str
            size: Optional[int]
            compaction_time: Optional[datetime.datetime]

        def _prepare_tables_with_data(cf_sizes: tuple):
            cf_size_time = []

            for size in cf_sizes:
                cf_name = cf_names[cf_sizes.index(size)]
                create_c1c2_table(session=session, cf=cf_name)
                insert_c1c2(session=session, ks=ks_name, cf=cf_name, n=size, consistency=ConsistencyLevel.ONE)
                cf_size_time.append(CfSizeTime(name=cf_name, size=size, compaction_time=None))

            return cf_size_time

        def _perform_major_compaction():
            node1.flush()
            node1.compact()
            node1.wait_for_compactions()

        def _get_compaction_history() -> list:
            compaction_history_query = f"SELECT columnfamily_name, compacted_at, keyspace_name " \
                                       f"FROM system.compaction_history"
            compaction_history_result = session.execute(compaction_history_query).all()
            return [row for row in compaction_history_result if row.keyspace_name == ks_name]

        def _sort_compaction_history(compaction_history: list) -> tuple:
            for item in cf_size_time:
                for row in compaction_history:
                    if item.name == row.columnfamily_name:
                        item.compaction_time = row.compacted_at
            by_time = sorted(cf_size_time, key=lambda x: x.compaction_time)
            by_size = sorted(cf_size_time, key=lambda x: x.size)
            return by_time, by_size

        nodelist, session = self.prepare(nodes=1)
        node1 = nodelist[0]
        ks_name = "ks"
        cf_names = ["cf_1", "cf_2", "cf_3"]
        create_ks(session=session, name=ks_name, rf=1)
        cf_size_time = _prepare_tables_with_data(cf_sizes=cf_sizes)

        _perform_major_compaction()
        sorted_by_time, sorted_by_size = _sort_compaction_history(_get_compaction_history())

        assert sorted_by_time == sorted_by_size, "The list of rows sorted by size is not identical" \
                                                 " to the list of rows sorted by compaction time"

    def wait_for_new_minute(self):
        while dt.now().second > 5:
            time.sleep(1)

    @staticmethod
    def get_compacted_sstable_numbers(node, exprs, from_mark):
        regular_compact_sstables = []
        special_compact_sstables = []
        for expr in exprs:
            regular_one = []
            special_one = []
            matches = node.grep_log(expr=expr, from_mark=from_mark)
            assert matches, f"Compactions were not started. Expression: {expr}"

            # Find sstable number from the line.
            # Example of line:
            #   compaction - [Compact ks.cf2 29176150-1957-11ec-8aaf-6d8518342838] Compacting
            #   [dtest-e7ugljs7/test/node1/data/ks/cf2-cb269cf0195611ec8aaf6d8518342838/md-5-big-Data.db:level=0:
            #   origin=memtable,
            #   .dtest/dtest-e7ugljs7/test/node1/data/ks/cf2-cb269cf0195611ec8aaf6d8518342838/md-1-big-Data.db:level=0:
            #   origin=memtable]
            split_pattern = re.compile(r"(?:mc|md)-(\d+)-")
            for one_match in matches:
                line_groups = one_match[1].groups()
                if not line_groups:
                    continue

                compaction_type = line_groups[0]
                sstable_numbers = re.findall(split_pattern, line_groups[1])
                if compaction_type == 'Compacting':
                    regular_one.extend(sstable_numbers)
                else:
                    special_one.extend(sstable_numbers)
            regular_compact_sstables.append(regular_one)
            special_compact_sstables.append(special_one)

        return regular_compact_sstables, special_compact_sstables

    @staticmethod
    def search_and_assert_for_double_compactions(regular_compact, cleanup_compact, tables):
        logger.debug("Search for double compactions on the same sstable")
        for i, one_expression_result in enumerate(regular_compact):
            logger.debug(f"Sstable files of table '{tables[i]}' were compacted by regular compactions: "
                         f"{one_expression_result}")
            logger.debug(f"Sstable files of table '{tables[i]}' were compacted by cleanup compactions: "
                         "{cleanup_compact[i]}")
            double_compacted_sstables = list(set(one_expression_result).intersection(cleanup_compact[i]))
            assert not double_compacted_sstables, \
                f"Found sstables that were compacted by both regular compactions and cleanup " \
                f"(table '{tables[i]}'): {double_compacted_sstables}"

    def test_double_compaction_by_cleanup_and_major_compactions(self):
        """
        Cover the issue https://github.com/scylladb/scylla/issues/8155
        Test that cleanup is not started on the sstable that regular compaction runs on it

        1. Create a cluster with a single node with rf=1, create a table, insert data and flush
        2. Run in parallel cleanup and major compaction after delete/insert rows
        3. Check in the log that same sstable was not compacted twice by cleanup and major compaction
        """
        # Set compaction_static_shares to 10 to  make compaction slower, so increasing the chances of reproducing
        # the issue
        node_list, session_node1 = self.prepare(nodes=1, configuration_options={'compaction_static_shares': 10})
        node1 = node_list[0]
        rows = 200000

        create_ks(session_node1, 'ks', 1)
        create_cf(session_node1, 'cf', gc_grace=5, read_repair=0.0, columns={'c1': 'text', 'c2': 'text'},
                  compaction={'class': 'SizeTieredCompactionStrategy'})
        logger.debug(f"Insert {rows} rows to the table")
        insert_c1c2(session_node1, keys=range(rows), consistency=ConsistencyLevel.ONE)
        self.cluster.flush()

        mark = node1.mark_log()

        proc_functions = [{'func': node1.nodetool, 'args': ('compact ks',)},
                          {'func': node1.nodetool, 'args': ('cleanup ks',)}]

        for _ in range(3):
            # Insert or delete data will change data files
            insert_c1c2(session_node1, keys=range(20000, 70000), consistency=ConsistencyLevel.ONE)
            node1.flush()
            run_in_parallel(proc_functions)

            delete_c1c2(session_node1, keys=list(range(20000, 70000)))
            node1.flush()
            run_in_parallel(proc_functions)

        # Find lines in the log about compacting and cleaning.
        # Examples:
        #   compaction - [Cleanup ks.cf2 661f8180-1958-11ec-a8dd-f95d7ef91c29] Cleaning
        #   [.dtest/dtest-2p6gfyf9/test/node1/data/ks/cf-0a06d880195811eca8ddf95d7ef91c29/md-5-big-Data.db:level=0:
        #   origin=memtable]
        #
        #   compaction - [Compact ks.cf 29176150-1957-11ec-8aaf-6d8518342838] Compacting
        #   [dtest-e7ugljs7/test/node1/data/ks/cf-cb269cf0195611ec8aaf6d8518342838/md-5-big-Data.db:level=0:
        #   origin=memtable,
        #   .dtest/dtest-e7ugljs7/test/node1/data/ks/cf-cb269cf0195611ec8aaf6d8518342838/md-1-big-Data.db:level=0:
        #   origin=memtable]
        regular_compact, cleanup_compact = self.get_compacted_sstable_numbers(
            node=node1,
            exprs=[r"(Compacting|Cleaning) (.*\/ks\/cf.*\/(?:mc|md)-.*)"],
            from_mark=mark
        )

        self.search_and_assert_for_double_compactions(regular_compact, cleanup_compact, tables=["cf"])

        errors = node1.grep_log_for_errors()
        assert not errors, f"Failed with error: {errors}"

    def test_double_compaction_by_cleanup_and_ongoing_compaction(self):
        """
        Cover the issue https://github.com/scylladb/scylla/issues/8155
        Test that cleanup is not started on the sstable that regular compaction runs on it

        1. Create a cluster with a single node with rf=1
        2. Create 3 tables with NullCompactionStrategy, insert data and flush
        3. Run in parallel cleanup and alter tables, change tables compaction to SizeTieredCompactionStrategy. It cause
           to start ongoing compaction
        4. Check in the log that same sstable was not compacted twice by cleanup and ongoing compaction
        """
        # Set compaction_static_shares to 10 to  make compaction slower, so increasing the chances of reproducing
        # the issue
        node_list, session_node1 = self.prepare(nodes=1, configuration_options={'compaction_static_shares': 10})
        node1 = node_list[0]

        create_ks(session_node1, 'ks', 1)

        tables = ['cf', 'cf1', 'cf2']
        insert_proc_functions = []
        compact_proc_functions = []
        rows = 200000
        for table in tables:
            create_cf(session_node1, table, gc_grace=5, read_repair=0.0, columns={'c1': 'text', 'c2': 'text'},
                      compaction={'class': 'NullCompactionStrategy'})
            insert_proc_functions.append({'func': insert_c1c2, 'kwargs': {"session": session_node1, "keys": range(rows),
                                                                          "consistency": ConsistencyLevel.ONE,
                                                                          "cf": table}})
            compact_proc_functions.append({'func': session_node1.execute,
                                           'args': ("alter table %s with compaction = "
                                                    "{'class':'SizeTieredCompactionStrategy'}" % table,)})

        logger.debug(f"Insert {rows} rows to the tables")
        run_in_parallel(insert_proc_functions)
        self.cluster.flush()

        compact_proc_functions.append({'func': node1.nodetool, 'args': ("cleanup ks",)})
        mark = node1.mark_log()
        logger.debug("Run in parallel cleanup on the keyspace and alter tables")
        run_in_parallel(compact_proc_functions)

        # Find lines in the log about compacting and cleaning.
        # Examples:
        #   compaction - [Cleanup ks.cf2 661f8180-1958-11ec-a8dd-f95d7ef91c29] Cleaning
        #   [.dtest/dtest-2p6gfyf9/test/node1/data/ks/cf2-0a06d880195811eca8ddf95d7ef91c29/md-5-big-Data.db:level=0:
        #   origin=memtable]
        #
        #   compaction - [Compact ks.cf2 29176150-1957-11ec-8aaf-6d8518342838] Compacting
        #   [dtest-e7ugljs7/test/node1/data/ks/cf2-cb269cf0195611ec8aaf6d8518342838/md-5-big-Data.db:level=0:
        #   origin=memtable,
        #   .dtest/dtest-e7ugljs7/test/node1/data/ks/cf2-cb269cf0195611ec8aaf6d8518342838/md-1-big-Data.db:level=0:
        #   origin=memtable]
        reg_expr_template = r"(Compacting|Cleaning) (.*\/ks\/{}-.*\/(?:mc|md)-.*)"
        regular_compact, cleanup_compact = self.get_compacted_sstable_numbers(
            node=node1,
            exprs=[reg_expr_template.format(table) for table in tables],
            from_mark=mark
        )

        self.search_and_assert_for_double_compactions(regular_compact, cleanup_compact, tables=tables)

        logger.debug("Validate expected rows")
        for table in tables:
            assert_row_count(session=session_node1, table_name=table, expected=rows)

        errors = node1.grep_log_for_errors()
        assert not errors, f"Failed with error: {errors}"

    def write_n_data_files(self, node, session, key_space, num_of_files, num_of_keys, consistency=ConsistencyLevel.ONE):
        for t in range(0, num_of_files):
            logger.debug("Inserting concurrently {} keys...".format(num_of_keys))
            insert_c1c2(session, n=num_of_keys, consistency=consistency, ks=key_space)
            node.flush()


@pytest.mark.dtest_full
@pytest.mark.single_node
class TestCompactionAdditionalStrategy(CompactionAdditionalTester):
    strategy = None

    @pytest.fixture(scope='function', autouse=True)
    def fixture_dtest_setup_overrides(self, dtest_config):
        dtest_setup_overrides = DTestSetupOverrides()
        dtest_setup_overrides.cluster_options = ImmutableMapping({'start_rpc': 'true'})
        return dtest_setup_overrides

    @pytest.fixture(params=['LeveledCompactionStrategy',
                            'SizeTieredCompactionStrategy',
                            'DateTieredCompactionStrategy',
                            'TimeWindowCompactionStrategy',
                            enterprise_only_param('IncrementalCompactionStrategy')], autouse=True)
    def fixture_set_cs(self, request):
        self.strategy = request.param

    def test_compaction_is_started_on_boot(self):
        [node1], session = self.prepare(1)
        create_ks(session, 'ks', 1)
        session.execute(
            f"create table ks.cf (key int PRIMARY KEY, val int) with compaction = {{'class':'{self.strategy}'}};")

        for x in range(0, 100):
            session.execute(f'insert into cf (key, val) values ({x},1)')

        node1.flush()
        node1.compact()
        node1.stop()
        files = glob.glob(os.path.join(node1.get_path(), 'commitlogs', '*'))
        for f in files:
            os.remove(f)

        cf_dir = get_node_cf_dir(node1, 'ks', 'cf')
        sstablefiles = get_sstables_files(cf_dir)
        # prepare a mapping between each sstable generation
        # to new, unique generations it will be copied to
        gmap = dict()
        rmap = dict()
        generations = set([self._get_sstable_generation(f) for f in sstablefiles])
        for gen in generations:
            mapped = []
            for i in range(1, 5):
                while True:
                    n = random.randint(10000, 100000)
                    if n not in rmap:
                        rmap[n] = gen
                        mapped.append(n)
                        break
            gmap[gen] = mapped
            logger.debug(f"Will copy SSTable with generation {gen} to generations {mapped}")

        for f in sstablefiles:
            gen = self._get_sstable_generation(f)
            for i in gmap[gen]:
                self._copy_sstable_file(os.path.join(cf_dir, f), str(i))

        before_start_sstables = get_sstables_files(cf_dir, 'Data')

        from_mark = node1.mark_log()
        node1.start()
        node1.watch_log_for(
            r'compaction -.*(Compacted|Resharded|Reshaped) [0-9]+ sstables to \[.+/data/ks/cf-.+\]', from_mark=from_mark)

        after_start_sstables = get_sstables_files(cf_dir, 'Data')

        assert before_start_sstables != after_start_sstables, \
            f"No compaction detected after restarting {node1.name}. SSTables in ks/cf: {after_start_sstables}"

    @pytest.mark.next_gating
    @pytest.mark.dtest_debug
    def test_compaction_removes_ttld_data_after_gc_period(self):
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
        [node1], session = self.prepare(1)
        create_ks(session, 'ks', 1)

        session.execute(
            f"create table ks.cf (key int PRIMARY KEY, val int) with compaction = {{'class':'{self.strategy}'}} and gc_grace_seconds = 1;")

        for x in range(0, 100):
            session.execute(f'insert into cf (key, val) values ({x},1) USING TTL 29')

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
        assert numfound == 1, f"Error: expected 1 partition but found {numfound}:\n{jsoninfo}"

    def _get_sstable_generation(self, file):
        sstable_split_parts = os.path.basename(file).split('-')
        if (len(sstable_split_parts) == 5):
            # <= ka format
            return int(sstable_split_parts[-2])
        elif (len(sstable_split_parts) == 4):
            # >= la format
            return int(sstable_split_parts[1])
        else:
            raise RuntimeError("Unexpected format of file name: '%s'" % file)

    def _copy_sstable_file(self, file, generation):
        # filter out scylla component for IncrementalCompactionStrategy
        # to force a new run-identifier
        if self.strategy == 'IncrementalCompactionStrategy' and 'Scylla.db' in file:
            return

        sstable_split_parts = os.path.basename(file).split('-')
        if (len(sstable_split_parts) == 5):
            # <= ka format
            sstable_split_parts[-2] = generation
        elif (len(sstable_split_parts) == 4):
            # >= la format
            sstable_split_parts[1] = generation
        else:
            raise RuntimeError("Unexpected format of file name: '%s'" % file)
        dest = os.path.join(os.path.dirname(file), '-'.join(sstable_split_parts))
        if self.strategy != 'IncrementalCompactionStrategy' or not 'TOC.txt' in file:
            shutil.copy(file, dest)
        else:
            w = open(dest, 'w+')
            r = open(file, 'r')
            line = r.readline()
            while line:
                if not 'Scylla.db' in line:
                    w.writelines(line)
                line = r.readline()
            r.close()
            w.close()


@pytest.mark.dtest_full
class TestTimeWindowDataSegregation(CompactionAdditionalTester):
    keyspace_name = "ks"
    table_name = "test"
    window_size = 1
    window_unit = "MINUTES"

    def _get_stats(self, statistics_file):
        with open(statistics_file, 'rb') as f:
            data = f.read()

        metadata = sstable_tools.statistics.parse(data, 'mc')
        return metadata['Stats']

    def _get_info_on_sstable_spanning_one_window(self, statistics_file, window_size_in_seconds) -> SpanningSStable:
        def is_sstable_one_time_window(min_timestamp_in_seconds, max_timestamp_in_seconds):
            def get_window_lower_bound(timestamp_in_seconds):
                return timestamp_in_seconds - (timestamp_in_seconds % window_size_in_seconds)

            return get_window_lower_bound(min_timestamp_in_seconds) == get_window_lower_bound(max_timestamp_in_seconds)
        stats = self._get_stats(statistics_file)
        min_timestamp_seconds = self.micros_to_seconds(stats['min_timestamp'])
        max_timestamp_seconds = self.micros_to_seconds(stats['max_timestamp'])
        is_spanning_one_window = is_sstable_one_time_window(min_timestamp_seconds, max_timestamp_seconds)
        res = SpanningSStable(
            is_spanning_one_window=is_spanning_one_window,
            min_timestamp_seconds=min_timestamp_seconds,
            max_timestamp_seconds=max_timestamp_seconds
        )
        return res

    def _get_time_window_in_seconds(self, statistics_file, stats=None):
        if not stats:
            stats = self.get_stats(statistics_file)
        min_timestamp = stats['min_timestamp']
        max_timestamp = stats['max_timestamp']
        return self.micros_to_seconds(max_timestamp - min_timestamp)

    def _check_sstable_timestamps(self, node):
        statistics_files = self._get_list_of_sstables(node)
        assert len(statistics_files) > 0, "No statisitcs files"
        for sf in statistics_files:
            stats = self.get_stats(sf)
            tw = self._get_time_window_in_seconds(sf, stats)

            # Allow an error margin of a half-window.
            margin = 1.5 * self.window_size * 60
            assert tw <= margin, f"time window of {tw} seconds is greater than {margin} \
                                   seconds margin: sstable={sf} \
                                   min_timestamp={stats['min_timestamp']} max_timestamp={stats['max_timestamp']}"

    def _create_ks_cl_with_twcs(self, session, rf=1):

        session.execute("CREATE KEYSPACE {} WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': {}}}".format(
            self.keyspace_name, rf))
        session.execute(
            "CREATE TABLE {0.keyspace_name}.{0.table_name} (pk int, ck int, v int, PRIMARY KEY(pk, ck))"
            "WITH compaction = {{"
            "'class': 'TimeWindowCompactionStrategy',"
            "'compaction_window_unit': '{0.window_unit}',"
            "'compaction_window_size': {0.window_size} }}".format(self))

    def _simulate_write_process_in_minutes(self, session, duration_minutes=20, start_from_minute=0,
                                           flush_period_seconds=30, flushing_exclude_nodes=None, num_pks=10):
        """Simulate a write process across duration minutes.

        We use `USING TIMESTAMP` to distribute the writes evenly
        across the entire range, simulating a write every second (to
        several partitions).
        flush_period_seconds allow to control how many time windows could be
        in sstable

        Arguments:
            session {Session} -- opened session to node

        Keyword Arguments:
            duration_minutes {number} -- how many minutes to simulate (default: {20})
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
                [(pk, t, 0, self.seconds_to_micros(t)) for pk in rand_pks])

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

    @pytest.mark.single_node
    def test_streaming_during_adding_node_with_boostrap(self):
        [node1], session = self.prepare(1)
        self._create_ks_cl_with_twcs(session, rf=1)

        self._simulate_write_process_in_minutes(session, duration_minutes=20)

        # Not really relevant to the test, just for sanity.
        self._check_sstable_timestamps(node1)

        # node added with bootstrap enabled
        node2 = new_node(self.cluster)
        node2.start(wait_for_binary_proto=True)
        # After streaming the new node should also have at max one
        # window per sstable.
        self._check_sstable_timestamps(node2)

    def test_streaming_decommission(self):
        [node1, node2], session = self.prepare(2)
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
        [node1, node2], session = self.prepare(2)
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

    def run_prepared_statement(self, node, session, insert_statement, rand_pks, start, end):
        for t in range(start, end * 60):
            concurrent.execute_concurrent_with_args(
                session,
                insert_statement,
                [(pk, t, 0, self.seconds_to_micros(t * 60)) for pk in rand_pks])
            if t % 60 == 0:
                node.flush()

    def test_twcs_multiple_sstables_during_bootstrap(self):
        synthetic_minutes = 20
        [node1, _], session = self.prepare(2)
        self._create_ks_cl_with_twcs(session, rf=3)

        insert_statement = session.prepare("INSERT INTO {}.{} (pk, ck, v) VALUES (?, ?, ?)"
                                           "USING TIMESTAMP ?".format(self.keyspace_name, self.table_name))
        rand_pks = set()
        while len(rand_pks) < 10:
            rand_pks.add(random.randint(-2147483647, 2147483647))

        node3 = new_node(self.cluster)
        node3.start(wait_for_binary_proto=False)

        self.run_prepared_statement(node1, session, insert_statement, rand_pks, 0, synthetic_minutes)
        num_of_sstables = len(self._get_list_of_sstables(node1))
        assert num_of_sstables >= synthetic_minutes, f"Expected {synthetic_minutes} sstables but got " \
                                                     f"{num_of_sstables} on {node1.name}"
        node3.watch_log_for("init - Scylla.*initialization completed")

        num_of_sstables = len(self._get_list_of_sstables(node3))
        assert num_of_sstables >= synthetic_minutes, f"Expected {synthetic_minutes} sstables" \
                                                     f" but got {num_of_sstables} on {node3.name}"

    def test_twcs_multiple_sstables_during_compaction(self):
        synthetic_minutes = 20
        [node1, node2], session = self.prepare(2)
        self._create_ks_cl_with_twcs(session, rf=2)

        insert_statement = session.prepare("INSERT INTO {}.{} (pk, ck, v) VALUES (?, ?, ?)"
                                           "USING TIMESTAMP ?".format(self.keyspace_name, self.table_name))
        rand_pks = set()
        while len(rand_pks) < 10:
            rand_pks.add(random.randint(-2147483647, 2147483647))

        def do_run_compaction():
            try:
                node2.nodetool('compact')
            except NodetoolError:
                pass

        executor = ThreadPoolExecutor(max_workers=1)
        thread1 = executor.submit(do_run_compaction)

        self.run_prepared_statement(node1, session, insert_statement, rand_pks, 0, synthetic_minutes)
        num_of_sstables = len(self._get_list_of_sstables(node1))
        assert num_of_sstables >= synthetic_minutes, f"Expected {synthetic_minutes} sstables but got {num_of_sstables}"

        while not thread1.done():
            time.sleep(1)
        thread1.result()

    def test_twcs_multiple_sstables_during_decommission(self):
        synthetic_minutes = 20
        [node1, node2], session = self.prepare(2)
        self._create_ks_cl_with_twcs(session, rf=2)

        insert_statement = session.prepare("INSERT INTO {}.{} (pk, ck, v) VALUES (?, ?, ?)"
                                           "USING TIMESTAMP ?".format(self.keyspace_name, self.table_name))
        rand_pks = set()
        while len(rand_pks) < 100:
            rand_pks.add(random.randint(-2147483647, 2147483647))

        self.run_prepared_statement(node1, session, insert_statement, rand_pks, 0, synthetic_minutes)
        num_of_sstables = len(self._get_list_of_sstables(node1))
        assert num_of_sstables >= synthetic_minutes, f"Expected {synthetic_minutes} sstables but got {num_of_sstables}"

        min_compile = re.compile('Minimum timestamp: (.*)')
        max_compile = re.compile('Maximum timestamp: (.*)')

        def do_run_nodetool_decommission():
            try:
                node2.nodetool('decommission')
            except NodetoolError:
                pass

        executor = ThreadPoolExecutor(max_workers=1)
        thread1 = executor.submit(do_run_nodetool_decommission)

        time_now = time.time()
        res = node1.run_sstablemetadata(keyspace=self.keyspace_name)
        min_timestamp = min([int(min_compile.search(r[0]).group(1)) for r in res])
        max_timestamp = max([int(max_compile.search(r[0]).group(1)) for r in res])

        assert min_timestamp < int(time_now) < max_timestamp,\
            f'New sstables timestamp must be between {min_timestamp} and {max_timestamp}, but it was {int(time_now)}'

        while not thread1.done():
            time.sleep(1)
        thread1.result()

    @pytest.mark.next_gating
    @pytest.mark.single_node
    def test_streaming_on_rebuild_multidc(self):

        def _add_node(i, dc):
            return self.cluster.new_node(i, debug=True, data_center=dc)

        self.cluster.set_configuration_options(values={'endpoint_snitch': 'GossipingPropertyFileSnitch',
                                                       'enable_sstable_key_validation': True})
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
        session.execute("ALTER KEYSPACE {} WITH replication = {{'class':'NetworkTopologyStrategy', 'dc1':1, 'dc2':1}};".format(
            self.keyspace_name))

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
                    logger.debug('Unexpected rebuild failure {}'.format(str(e)))
                    self.unexpected_errors += 1

        cmd1 = Thread(target=rebuild)
        cmd1.start()
        cmd1.join()

        assert self.unexpected_errors == 0, 'unexpected rebuild errors encountered.'

        node2.watch_log_for("Streaming for rebuild successful|"
                            "rebuild_with_repair: finished with keyspace=ks", from_mark=mark)
        node2.wait_for_compactions()
        self._check_sstable_timestamps(node2)

    def test_streaming_sstables_with_several_timewindows(self):
        [node1, _], session = self.prepare(2)
        self._create_ks_cl_with_twcs(session, rf=2)

        self._simulate_write_process_in_minutes(session, duration_minutes=10, flush_period_seconds=120, num_pks=100)

        sstable_timewindows_list = self._list_sstable_timestamps(node1)
        max_timewindow = 1.5 * 2 * 60
        for sstable, timewindow in sstable_timewindows_list:
            assert timewindow <= max_timewindow, f"timewindow{timewindow} is greater than {max_timewindow}"

        _new_node = new_node(self.cluster)
        _new_node.start(wait_for_binary_proto=True)

        self._check_sstable_timestamps(_new_node)

    def test_rebuild_node_streaming(self):
        [node1, _, node3], session = self.prepare(3)
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

    def _get_list_of_sstables(self, node):

        return get_list_of_sstables(node, self.keyspace_name, self.table_name)

    def test_memtable_flush(self):
        '''
        Verify scylla does memtable flush into separate sstables when TWCS is used.
        Also verify each sstable span one window.
        '''
        [node1, node2, node3], session = self.prepare(3)
        self._create_ks_cl_with_twcs(session, rf=3)

        # Simulating 10 minutes of writes without flush
        self._simulate_write_process_in_minutes(session, duration_minutes=10, flush_period_seconds=999999999999)
        list_of_sstables_pre_flush = []
        list_of_sstables_post_flush = []
        for node in self.cluster.nodelist():
            list_of_sstables_pre_flush = self._get_list_of_sstables(node)
            node.flush()
            list_of_sstables_post_flush = self._get_list_of_sstables(node)
            post = len(list_of_sstables_post_flush)
            pre = len(list_of_sstables_pre_flush)
            assert post - pre > 1, f"Expected more than one sstable after flush, lengths are {post} {pre}, {node}"
            for sf in list_of_sstables_post_flush:
                span_info = self._get_info_on_sstable_spanning_one_window(sf, self.window_size * 60)
                msg = f"Failure, sstable {sf} spans over more than one window, " + \
                      f"min_timestamp_seconds={span_info.min_timestamp_seconds} " + \
                      f"max_timestamp_seconds = {span_info.max_timestamp_seconds} " + \
                      f"window_size_in_seconds = {self.window_size * 60}"
                assert span_info.is_spanning_one_window, msg


class TestGarabageCollected(CompactionAdditionalTester):

    def test_garbage_collected_sstable(self):
        """
        Test garbage collected SSTables
        Related issue: https://github.com/scylladb/scylla/issues/6275
        """
        [node1, node2, node3], session = self.prepare(3)
        create_ks(session, 'ks', 3)

        # Use IncrementalCompactionStrategy for Enterprise
        session.execute(
            "CREATE TABLE ks.cf (key varchar PRIMARY KEY, c1 text, c2 text) "
            "WITH compaction = {'class': 'LeveledCompactionStrategy'}")

        insert_c1c2(session, n=3)
        node1.flush()

        from_mark1 = node1.mark_log()
        from_mark2 = node2.mark_log()
        from_mark3 = node3.mark_log()

        # Set a short gc_grace_seconds, and delete one key
        gc_grace_seconds = 10
        session.execute(f"ALTER TABLE ks.cf WITH gc_grace_seconds = {gc_grace_seconds}")
        session.execute("DELETE from ks.cf WHERE key = 'k2'")

        # Sleep until the garbage collected SSTables are expired
        time.sleep(gc_grace_seconds + 1)
        self.cluster.compact()

        # Verify the data by queries
        assert_none(session, "SELECT * FROM ks.cf WHERE key = 'k2'")
        assert_all(session, "SELECT * FROM ks.cf", [['k1', 'value1', 'value2'], ['k0', 'value1', 'value2']])

        for node, from_mark in [[node1, from_mark1], [node2, from_mark2], [node3, from_mark3]]:
            try:
                res = node.watch_log_for(exprs="sstable - Unable to delete", from_mark=from_mark, timeout=10)
            except TimeoutError:
                res = None

            assert not res, "Don't expect the 'Unable to delete' error"


@pytest.mark.dtest_full
@pytest.mark.single_node
class TestValidationCompaction(CompactionAdditionalTester):
    KS = "ks"
    CF = "cf"
    CF_2 = "cf2"
    RF = 1
    CORRUPT_DATA_FILE_NAME = "mc-1-big-Data.db"
    CORRUPT_DATA_FILE_DIR = Path("test-sstables/sstable_with_invalid_fragment/ks/cf-test")
    CORRUPT_DATA_FILE_PATH = CORRUPT_DATA_FILE_DIR / CORRUPT_DATA_FILE_NAME
    DATA_FILE_NAME = "md-1-big-Data.db"
    REGEX_PATTERNS = {
        "validation_start": r"compaction - Scrubbing in validate mode",
        "invalid_partition": r"Invalid partition \x19\x00\x00\x00 \(\{key: pk\{000419000000}, "
                             r"token:-5674409923619649499}\), partition is out-of-order compared to previous "
                             r"partition \x06\x00\x00\x00 \(\{key: pk\{000406000000}, "
                             r"token:-5566252076597558760}\)",
        "invalid_clustering_row": r"Invalid clustering row fragment with key 3 \(\{position: clustered,"
                                  r"ckp\{000400000003},0}\) in partition .* \(\{key: pk\{000406000000}, "
                                  r"token:-5566252076597558760}\), fragment is out-of-order compared to "
                                  r"previous clustered fragment with key 5 \(\{position: clustered,"
                                  r"ckp\{000400000005},0}\)",
        "validation_finish_invalid": r"Finished scrubbing in validate mode.*sstable\(s\) are invalid",
        "validation_finish_valid": r"Finished scrubbing in validate mode.*sstable\(s\) are valid"
    }

    def test_validation_compaction_detects_sstable_corruption(self):
        """
        The test checks whether running a validation compaction identifies
        corrupted fragments in a corrupted sstable, without modifying the
        sstable.

        Test steps:
        1. Create test keyspace and column families.
        2. Load a corrupted sstable for a column family ("cf").
        3. Trigger the validation compaction using the API.
        4. Assert that following the compaction:
        - the corrupted sstable files were moved to the quarantine dir
        - the number of sstable files before and after the compaction is the
        same
        5. Assert that the corrupted fragments were reported
        in the logs.
        """
        self.ignore_log_patterns += [
            '[Ii]nvalid clustering row fragment',
            '[Ii]nvalid partition',
            '(Sscrub) compaction ks.cf.*'
        ]

        node, session, storage_service_client = self._prepare()
        create_ks(session=session, name=self.KS, rf=self.RF)
        create_cf(session=session, name=self.CF,
                  columns={"ck": "int", "s": "int", "v": "int"},
                  key_name="pk",
                  key_type="text",
                  primary_key="pk, ck",
                  debug_query=True)
        node.flush()
        cf_dir = Path(get_node_cf_dir(node=node, ks_name=self.KS, cf_name=self.CF))
        upload_dir = cf_dir / "upload"
        quarantined_sstables_dir = cf_dir / "quarantine"
        logger.debug("Copying the sstables with invalid fragment from source directory:"
                     " %s to upload directory: %s...", self.CORRUPT_DATA_FILE_DIR, upload_dir)
        pre_scrub_file_list = [item for item in self.CORRUPT_DATA_FILE_DIR.glob("*") if item.is_file()]
        copy_files_to(self.CORRUPT_DATA_FILE_DIR, upload_dir)
        node.nodetool(f"refresh -- {self.KS} {self.CF}")
        storage_service_client.scrub_ks_cf(keyspace=self.KS, cf=self.CF, scrub_mode="VALIDATE")
        quarantined_file_list = [item for item in quarantined_sstables_dir.glob("*") if item.is_file()]

        assert check_file_lists_are_equal(file_list_a=pre_scrub_file_list, file_list_b=quarantined_file_list), \
            "Pre scrub file list was expected to be the same as quarantined file list, but was not"
        assert all(self._grep_log_patterns(
            node=node,
            patterns=[
                self.REGEX_PATTERNS["validation_start"],
                self.REGEX_PATTERNS["invalid_partition"],
                self.REGEX_PATTERNS["invalid_clustering_row"],
                self.REGEX_PATTERNS["validation_finish_invalid"]
            ])), "Some regex patterns were not found in the logs."

    def test_validation_compaction_with_valid_sstable(self):
        """
        The test verifies whether running a validation compaction on
        a valid sstable does correctly informs of the sstable's validity
        and avoids modifying the sstable.

        Test steps:
        1. Create test keyspace and column families.
        2. Populate a column family with test data.
        3. Trigger the validation compaction using the API.
        4. Assert that following the compaction the sstable
        for the populated column family was not modified, i.e.
        the same sstable files are present in the table dir.
        5. Assert that no corrupted fragments were reported
        in the logs and the sstable was marked as valid.
        """
        node, session, storage_service_client = self._prepare()
        create_ks(session=session, name=self.KS, rf=self.RF)
        create_c1c2_table(session)
        insert_c1c2(session, n=10_000)
        node.flush()
        cf_dir = Path(get_node_cf_dir(node=node, ks_name=self.KS, cf_name=self.CF))
        pre_compaction_sstable_file_list = [item for item in cf_dir.glob("*") if item.is_file()]
        storage_service_client.scrub_ks_cf(keyspace=self.KS, cf=self.CF, scrub_mode="VALIDATE")
        post_compaction_sstable_file_list = [item for item in cf_dir.glob("*") if item.is_file()]

        assert check_file_lists_are_equal(file_list_a=pre_compaction_sstable_file_list,
                                          file_list_b=post_compaction_sstable_file_list), \
            "Pre-scrub file list was expected to be the same as post-scrub file list, but was not"
        assert all(self._grep_log_patterns(
            node=node,
            patterns=[
                self.REGEX_PATTERNS["validation_start"],
                self.REGEX_PATTERNS["validation_finish_valid"]
            ])), "Some regex patterns were not found in the logs."

    def _prepare(self):
        [node], session = self.prepare(1)
        storage_service_client = StorageServiceClient(node=node)
        return node, session, storage_service_client

    @staticmethod
    def _grep_log_patterns(node: Node, patterns: List[str]):
        return [node.grep_log(pattern) for pattern in patterns]
