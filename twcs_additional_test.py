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
        ttl = 60
        test_max_duration_minutes = 4
        session = self.patient_cql_connection(self.cluster.nodelist()[0])

        self._prepare_twcs_table(ttl=ttl, session=session)
        sstables = self.create_expired_sstables(session)
        p = self._start_high_load_on_cluster(duration_minutes=test_max_duration_minutes)

        sstable_exists = self.wait_until_expired_sstables_are_evicted(sstables, test_max_duration_minutes * 60)
        self.stop_high_load_on_cluster(p)

        assert not sstable_exists, "Expired sstables should be removed soon after expiration time (upon compaction)"

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

    def create_expired_sstables(self, session, duration_minutes=3, flush_period_seconds=30,):
        """Simulate a write process across duration minutes.
        Returns created sstables

        We use `USING TIMESTAMP` to distribute the writes evenly
        across the entire range, simulating a write every second (to
        several partitions).

        Arguments:
            session {Session} -- opened session to node

        Keyword Arguments:
            duration_minutes {number} -- how many minutes to simulate (default: {20})
            flush_period_seconds {number} -- in how many seconds flush memtable (default: {30})

        """
        node = self.cluster.nodelist()[0]
        ks = "keyspace1"
        cf = "standard1"
        insert_statement = session.prepare(
            f'INSERT INTO {ks}.{cf} (key, "C0", "C1", "C2", "C3", "C4") VALUES (?, ?, ?, ?, ?, ?)'
            f' USING TIMESTAMP ? AND TTL 60')
        rand_pks = set()

        logger.info("creating expired sstables")
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
        logger.info("expired sstables created")
        cf_dir = get_node_cf_dir(node, ks, cf)
        return get_sstables_files(cf_dir, f_type='Data')

    def wait_until_expired_sstables_are_evicted(self, expired_sstables, timeout):
        """Waits until sstables are removed from disk. Returns list of not removed sstables."""
        logger.info("Waiting for expired sstables to be removed")
        node = self.cluster.nodelist()[0]
        cf_dir = get_node_cf_dir(node, "keyspace1", 'standard1')
        start_time = time()
        while time() - start_time < timeout:
            expired_sstables = [table for table in expired_sstables if os.path.exists(cf_dir + '/' + table)]
            if expired_sstables:
                logger.debug(f"still not removed: {expired_sstables}")
                sleep(2)
                continue
            break
        return expired_sstables

    @staticmethod
    def stop_high_load_on_cluster(proc):
        logger.info("killing stress process")
        process = psutil.Process(proc.pid)
        for child_process in process.children(recursive=True):
            child_process.kill()
        process.kill()
        proc.communicate()
