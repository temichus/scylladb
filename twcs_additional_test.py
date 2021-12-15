import logging
import threading
from datetime import datetime
from os.path import getctime
from time import sleep, ctime

import pytest

from dtest_class import Tester, create_ks
from scylla_tools import get_sstables_files, get_node_cf_dir


logger = logging.getLogger(__name__)


class TestTimeWindowCompactionStrategyAdditional(Tester):

    @pytest.mark.dtest_full
    @pytest.mark.single_node
    def test_expired_sstables_are_compacted_separately(self):
        """
        verify if expired sstables are compacted (removed) separately even during high load.
        Tests #9533 fixed in 4.6.rc0.
        """
        cluster = self.cluster
        cluster.populate(1).start(wait_for_binary_proto=True)
        ttl = 60
        node = self.cluster.nodelist()[0]
        session = self.patient_cql_connection(node)

        self._prepare_twcs_table(ttl=ttl, session=session)

        stress_thread = self._start_high_load_on_cluster()
        sleep(179)  # waiting for sstables expiration, before load ends

        expired_sstables = self._get_expired_sstables(ttl=ttl)
        stress_thread.join(timeout=300)
        assert not expired_sstables, "Expired sstables should be removed soon after expiration time (upon compaction)"

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

    def _start_high_load_on_cluster(self):
        node = self.cluster.nodelist()[0]
        args = ['write', 'duration=3m', 'no-warmup', '-rate', 'threads=300', '-mode', 'native', 'cql3',
                '-pop', 'seq=1..1000000000']
        stress_thread = threading.Thread(target=node.stress, args=(args,))
        stress_thread.start()
        logger.info(f"Started high load on cluster at {ctime(datetime.now().timestamp())}")
        return stress_thread

    def _get_expired_sstables(self, ttl):
        logger.info("Getting expired sstables")
        node = self.cluster.nodelist()[0]
        cf_dir = get_node_cf_dir(node, "keyspace1", 'standard1')
        expiration_timestamp = datetime.now().timestamp() - (ttl + 90)
        logger.info(f"expiration_timestamp={ctime(expiration_timestamp)}",)
        expired_sstables = [(sstable, ctime(c_time)) for sstable in get_sstables_files(cf_dir, f_type='Data') if
                            expiration_timestamp >= (c_time := getctime(cf_dir + "/" + sstable))]
        logger.info(expired_sstables)
        return expired_sstables
