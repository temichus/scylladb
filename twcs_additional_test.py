import logging
import os.path
import random
import subprocess
from time import sleep, time

import psutil
import pytest
from cassandra import concurrent
from ccmlib import common

from dtest_class import Tester, create_ks
from scylla_tools import get_sstables_files, get_node_cf_dir


logger = logging.getLogger(__name__)


class TestTimeWindowCompactionStrategyAdditional(Tester):

    @pytest.mark.dtest_full
    @pytest.mark.single_node
    def test_expired_sstables_are_compacted_separately(self):
        """
        verify if expired sstables are compacted (removed) separately even during high load.
        Tests #9533 fixed in 4.6.rc1.
        """
        cluster = self.cluster
        cluster.populate(1).start(wait_for_binary_proto=True, jvm_args=['--smp', '1'])
        ttl = 30
        test_max_duration_minutes = 4
        session = self.patient_cql_connection(self.cluster.nodelist()[0])

        self._prepare_twcs_table(ttl=ttl, session=session)
        sstables = self.create_sstables_with_short_ttl(session, ttl=30)
        p = self._start_high_load_on_cluster(duration_minutes=test_max_duration_minutes)
        sleep(ttl)  # wait for sstables to be expired
        mark = self.cluster.nodelist()[0].mark_log()
        timeout = test_max_duration_minutes * 60 - ttl
        sstable_exists = self.wait_until_sstables_are_evicted(sstables, timeout)
        self.stop_high_load_on_cluster(p)

        assert not sstable_exists, "Expired sstables should be removed soon after expiration time (upon compaction)"
        self.expired_sstables_should_not_be_compacted_along_with_unexpired(sstables, mark)

    @staticmethod
    def _prepare_twcs_table(ttl, session):
        create_ks(session=session, name="keyspace1", rf=1)
        cf = f"""CREATE TABLE keyspace1.standard1
                ( key blob PRIMARY KEY, "C0" blob, "C1" blob, "C2" blob, "C3" blob, "C4" blob )
                WITH bloom_filter_fp_chance = 0.01 AND caching = {{'keys': 'ALL', 'rows_per_partition': 'ALL'}}
                AND compaction = {{'class': 'TimeWindowCompactionStrategy', 'compaction_window_size': '1',
                'compaction_window_unit': 'MINUTES', 'expired_sstable_check_frequency_seconds': '30'}}
                AND crc_check_chance = 1.0
                AND dclocal_read_repair_chance = 0.0
                AND default_time_to_live = {ttl}
                AND gc_grace_seconds = 0 AND max_index_interval = 2048
                AND memtable_flush_period_in_ms = 0 AND min_index_interval = 128
                AND read_repair_chance = 0.0
                AND speculative_retry = '99.0PERCENTILE';"""
        session.execute(cf)
        logger.info("table prepared")

    def _start_high_load_on_cluster(self, duration_minutes):
        logger.info("Starting high load on cluster")
        node = self.cluster.nodelist()[0]
        stress_options = ['write', f'duration={duration_minutes}m', 'no-warmup', '-rate', 'threads=300', '-mode', 'native', 'cql3',
                          '-pop', 'seq=1..1000000000']
        stress = common.get_stress_bin(node.get_install_dir())
        stress_options.append('-node')
        stress_options.append(node.address())
        stress_options.extend(['-port', 'jmx=' + node.jmx_port])
        cmd = [stress] + stress_options
        logger.info(cmd)
        proc = subprocess.Popen([stress] + stress_options, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        logger.info("started stress")
        return proc

    def create_sstables_with_short_ttl(self, session, duration_minutes=3, flush_period_seconds=30, ttl=30):
        """Simulate a write process across duration minutes. When using TWCS, should create ~duration_minutes number of
        sstables (when time window is 1 minute).
        Returns created sstables names.

        We use `USING TIMESTAMP` to distribute the writes evenly
        across the entire range, simulating a write every second (to
        several partitions).

        Arguments:
            session {Session} -- opened session to node

        Keyword Arguments:
            duration_minutes {number} -- how many minutes to simulate (default: {20})
            flush_period_seconds {number} -- simulates period when data is flushed in seconds (default: {30})

        """
        node = self.cluster.nodelist()[0]
        ks = "keyspace1"
        cf = "standard1"
        insert_statement = session.prepare(
            f'INSERT INTO {ks}.{cf} (key, "C0", "C1", "C2", "C3", "C4") VALUES (?, ?, ?, ?, ?, ?)'
            f' USING TIMESTAMP ? AND TTL {ttl}')
        rand_pks = set()

        logger.info(f"creating sstables with ttl={ttl}")
        while len(rand_pks) < 10:
            rand_pks.add(random.randbytes(10))

        for t in range(duration_minutes * 60, 0, -1):
            timestamp = int(time() - t)
            concurrent.execute_concurrent_with_args(
                session,
                insert_statement,
                [(pk, b"test", b"test", b"test", b"test", b"tt", timestamp * 1000 * 1000) for pk in rand_pks])
            # Flush every flush period in seconds on each node
            if t % flush_period_seconds == 0:
                node.flush()
        node.flush()
        logger.info("sstables created")
        cf_dir = get_node_cf_dir(node, ks, cf)
        sstables_file_names = get_sstables_files(cf_dir, f_type='Data')
        logger.debug(f"created sstables: {sstables_file_names}")
        return sstables_file_names

    def wait_until_sstables_are_evicted(self, sstables, timeout):
        """Waits until sstables are removed from disk. Returns list of not removed sstables."""
        logger.info("Waiting for sstables to be removed (due to expiration)")
        node = self.cluster.nodelist()[0]
        cf_dir = get_node_cf_dir(node, "keyspace1", 'standard1')
        start_time = time()
        while time() - start_time < timeout:
            sstables = [table for table in sstables if os.path.exists(cf_dir + '/' + table)]
            if sstables:
                logger.debug(f"still not removed: {sstables}")
                sleep(2)
                continue
            break
        logger.info("sstables has been removed")
        return sstables

    def expired_sstables_should_not_be_compacted_along_with_unexpired(self, expired_sstables, from_mark):
        node = self.cluster.nodelist()[0]
        log_file = os.path.join(node.get_path(), 'logs', 'system.log')
        with open(log_file, "r") as system_log:
            system_log.seek(from_mark)
            ks_compaction_lines = [line for line in system_log.readlines() if
                                   "compaction - [Compact keyspace1.standard1" in line]
            start_compaction_lines = [line for line in ks_compaction_lines if "Compacting [" in line]
            end_compaction_lines = [line for line in ks_compaction_lines if "] Compacted" in line]

            compaction_ids = set()
            for line in start_compaction_lines:
                for file in expired_sstables:
                    if file in line:
                        compaction_id = line.split("compaction - [Compact keyspace1.standard1 ")[1].split("]")[0]
                        compaction_ids.add(compaction_id)
                        break
            assert compaction_ids, "no compaction of keyspace1.standard1 found in logs"
            for compaction_id in compaction_ids:
                for line in end_compaction_lines:
                    if compaction_id in line:
                        assert "sstables to []" in line, \
                            f"expired sstable was compacted along with unexpired ones. id: {compaction_id}. Issue #9533"

    @staticmethod
    def stop_high_load_on_cluster(proc):
        logger.info("killing stress process")
        process = psutil.Process(proc.pid)
        for child_process in process.children(recursive=True):
            child_process.kill()
        process.kill()
        proc.communicate()
