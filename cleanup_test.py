import os
import logging
import time
from concurrent.futures import ThreadPoolExecutor

import pytest
from cassandra import ConsistencyLevel, Unavailable
from cassandra.query import SimpleStatement

from dtest_class import Tester, create_ks, create_cf
from tools.data import insert_c1c2, delete_c1c2, create_c1c2_table
from tools.files import get_list_of_sstables
from tools.snapshots import make_snapshot, restore_snapshot_with_refresh
from ccmlib.scylla_cluster import ScyllaCluster


logger = logging.getLogger(__name__)


@pytest.mark.dtest_full
class TestCleanup(Tester):
    def prepare(self, nodes, num_keys, timeout=None, consistency=ConsistencyLevel.ALL, amount_of_tables=1, rf=None):
        cluster = self.cluster
        if timeout:
            values = {
                "range_request_timeout_in_ms": timeout * 1000,
            }
            logger.info("Setting cluster configuration options: %s", values)
            cluster.set_configuration_options(values=values)
        cluster.populate(nodes).start()
        node1 = self.cluster.nodelist()[0]
        with self.patient_cql_cluster_session(node1) as session:
            if not rf:
                rf = nodes
            create_ks(session=session, name="ks", rf=rf)
            for i in range(amount_of_tables):
                create_cf(session=session, name=f"cf{i}", columns={"c1": "text", "c2": "text"})
            if num_keys:
                logger.info("Inserting %s keys", num_keys)
                for i in range(amount_of_tables):
                    insert_c1c2(session=session, keys=range(num_keys), consistency=consistency, cf=f"cf{i}")

    @pytest.mark.next_gating
    @pytest.mark.single_node
    def test_cleanup(self):
        num_keys = 100000 if isinstance(self.cluster, ScyllaCluster) and self.cluster.scylla_mode != "debug" else 10000
        timeout = self.cql_timeout(300)
        self.prepare(1, num_keys, timeout)

        logger.info("Restarting node")
        node1 = self.cluster.nodelist()[0]
        node1.stop()
        node1.start(wait_for_binary_proto=True)

        logger.info("Running cleanup")
        node1.cleanup()

        logger.info("Verifying number of rows")
        session = self.patient_cql_connection(node1)
        rows = session.execute("select count(*) from ks.cf0;", timeout=timeout)

        assert rows.one()[0] == num_keys

    @pytest.mark.next_gating
    def test_cluster_cleanup(self):
        num_keys = 100000 if isinstance(self.cluster, ScyllaCluster) and self.cluster.scylla_mode != "debug" else 10000
        timeout = self.cql_timeout(300)
        self.prepare(3, num_keys, timeout)

        cluster = self.cluster
        node1 = cluster.nodelist()[0]

        logger.info("Adding a new node")
        node4 = cluster.new_node(4)
        node4.start(wait_for_binary_proto=True, wait_other_notice=True)

        logger.info("Running cleanup")
        cluster.cleanup()

        logger.info("Verifying number of rows")
        session = self.patient_cql_connection(node1)
        rows = session.execute("select count(*) from ks.cf0;", timeout=timeout)
        assert rows.one()[0] == num_keys

    def test_cleanup_space_amplification(self):
        num_keys = 100000 if isinstance(self.cluster, ScyllaCluster) and self.cluster.scylla_mode != "debug" else 10000
        timeout = self.cql_timeout(300)
        self.prepare(1, num_keys, timeout)

        def _get_list_of_sstables(node):
            full_size = 0
            sstables = get_list_of_sstables(node, "ks", "cf")
            for file in sstables:
                try:
                    full_size += os.stat(file).st_size
                except FileNotFoundError as ex:
                    logger.info("File %s was not found: %s", file, ex)
            return sstables, full_size

        cluster = self.cluster
        node1 = cluster.nodelist()[0]

        logger.info("Adding a new node")
        node2 = cluster.new_node(2)
        node2.start(wait_for_binary_proto=True, wait_other_notice=True)
        cluster.flush()
        cluster.stop()
        cluster.start(wait_for_binary_proto=True, wait_other_notice=True)
        sstables_before, size_before = _get_list_of_sstables(node1)

        def do_run_cleanup(node):
            node.cleanup()

        logger.info("Running cleanup")
        executor = ThreadPoolExecutor(max_workers=1)
        thread1 = executor.submit(do_run_cleanup, node1)

        while not thread1.done():
            sstables_during, size_during = _get_list_of_sstables(node1)
            assert size_during <= size_before * 2, f"Temporary space amplification must be less than 2x during cleanup, " \
                f"before=({sstables_before}, {size_before}) and during=({sstables_during}, {size_during})"
        thread1.result()
        sstables_after, size_after = _get_list_of_sstables(node1)
        assert size_before > size_after, f"Cleanup is supposed to decrease disk utilisation, " \
            f"before=({sstables_before}, {size_before}) and after=({sstables_after}, {size_after})"

    # Reproducer for https://github.com/scylladb/scylladb/issues/1239
    @pytest.mark.next_gating
    def test_cluster_cleanup_no_resurrection(self):
        """
        - Write data to 2-node cluster
        - Add node
        - Run cleanup
        - Delete data
        - Wait for tombstones to expire
        - Run major compaction (this will get rid of both data and tombstones)
        - Remove the added node
        - Verify there are no readable keys.
        - Any original data that wasn't cleaned up properly would get resurrected at this stage.
        """
        num_keys = 1000
        self.prepare(nodes=2, num_keys=num_keys)
        cluster = self.cluster
        node1, node2 = cluster.nodelist()
        session = self.patient_cql_connection(node1)
        gc_grace_seconds = 0
        session.execute(f"ALTER TABLE ks.cf0 WITH gc_grace_seconds={gc_grace_seconds}")

        logger.info("Adding a new node")
        new_node = cluster.new_node(len(cluster.nodelist()) + 1)
        new_node.start(wait_for_binary_proto=True, wait_other_notice=True)

        logger.info("Running cleanup")
        cluster.nodetool('cleanup ks')

        logger.info("Deleting data")
        delete_c1c2(session, n=num_keys, cf='cf0')

        logger.info("Verifying data")
        query = SimpleStatement("SELECT count(*) FROM ks.cf0", consistency_level=ConsistencyLevel.QUORUM)
        rows = session.execute(query)
        assert rows.one()[0] == 0

        logger.info(f"Sleeping until gc_grace_seconds={gc_grace_seconds} pass")
        time.sleep(gc_grace_seconds + 1)
        logger.info("Running compaction")
        cluster.compact()
        cluster.wait_for_compactions()

        new_node_hostid = new_node.hostid()
        logger.debug(f"Remove node {new_node.name} (host id {new_node_hostid})")
        new_node.stop(wait_other_notice=True)
        node1.removenode(new_node_hostid)

        node1.nodetool('snapshot ks -t after_removenode')

        logger.info("Reverifying data")
        rows = session.execute(query)
        assert rows.one()[0] == 0

    @pytest.mark.require('scylladb/scylla#11933')
    def test_cluster_restore_no_resurrection(self):
        """
        Reproducer for https://github.com/scylladb/scylladb/issues/11933:
        - Write data to 2-node cluster
        - Make a snapshot
        - Add node
        - Run cleanup
        - Delete data
        - Wait for tombstones to expire
        - Run major compaction (this will get rid of both data and tombstones)
        - Restore sstables from snapshot
        - Verify there are no readable keys.
        - Any stale data that is restored from snapshots would get resurrected at this stage.
        """
        num_keys = 1000
        self.prepare(nodes=2, num_keys=num_keys, rf=1)
        cluster = self.cluster
        node1, node2 = cluster.nodelist()
        session = self.patient_cql_connection(node1)
        gc_grace_seconds = 0
        session.execute(f"ALTER TABLE ks.cf0 WITH gc_grace_seconds={gc_grace_seconds}")

        def existing_keys(session):
            res = []
            key_exists_query = session.prepare("SELECT key from ks.cf0 WHERE key = ?")
            key_exists_query.consistency_level = ConsistencyLevel.ONE
            for i in range(num_keys):
                try:
                    if session.execute(key_exists_query, [f"k{i}"]):
                        res.append(i)
                except Unavailable:
                    pass
            return res

        logger.info("Verifying initial dataset")
        assert existing_keys(session) == list(range(num_keys))

        snapshot_name = 'pre_bootstrap'
        snapshot_dirs = {}
        for node in cluster.nodelist():
            snapshot_dirs[node.name] = make_snapshot(node, ks="ks", name=snapshot_name)

        logger.info("Adding a new node")
        new_node = cluster.new_node(len(cluster.nodelist()) + 1)
        new_node.start(wait_for_binary_proto=True, wait_other_notice=True)

        logger.info("Running cleanup")
        cluster.nodetool('cleanup ks')

        logger.info(f"Stopping {new_node.name}")
        new_node.stop()

        logger.info(f"Get existing keys")
        existing = existing_keys(session)

        logger.info(f"Starting {new_node.name}")
        new_node.start()

        logger.info("Deleting data")
        delete_c1c2(session, n=num_keys, cf='cf0')

        logger.info("Verifying data")
        query = SimpleStatement("SELECT count(*) FROM ks.cf0", consistency_level=ConsistencyLevel.QUORUM)
        rows = session.execute(query)
        assert rows.one()[0] == 0

        logger.info(f"Sleeping until gc_grace_seconds={gc_grace_seconds} pass")
        time.sleep(gc_grace_seconds + 1)
        logger.info("Running compaction")
        cluster.compact()
        cluster.wait_for_compactions()

        logger.info("Restoring from snapshot")
        for node in [node1, node2]:
            restore_snapshot_with_refresh(
                snapshot_dir=snapshot_dirs[node.name], node=node, keyspace='ks', table='cf0', name=snapshot_name)

        logger.info(f"Removing {new_node.name}")
        new_node_hostid = new_node.hostid()
        new_node.stop()
        node1.removenode(new_node_hostid)

        logger.info("Verifying that no data was resurrected")
        restored = existing_keys(session)

        assert restored == existing

    @pytest.mark.single_node
    @pytest.mark.next_gating
    def test_drop_table_during_cleanup(self):
        """
        Reproducer for https://github.com/scylladb/scylladb/issues/12007

        Populate a number of tables (with enough keys to make their cleanup time substantial).
        Drop all tables concurrently during cleanup by dropping the keyspace.
        Expect cleanup to succeed.

        Dropping the keyspace tests 2 cases in parallel, in parctice.
        One is dropping a table that is currently undergoing cleanup,
        where the compaction layer needs to handle that gracefully;
        and the other case drops a table that is pending cleanup but for which
        cleanup hasn't started yet, and the api should handle this case gracefully as well.
        """
        nodes = 1
        num_tables = 3
        # cleanup is performed in each table, sorted by their data size
        # so populate more keys as we go
        factor = 10000 if isinstance(self.cluster, ScyllaCluster) and self.cluster.scylla_mode != "debug" else 1000
        table_keys = [factor * i for i in [3, 4, 5]]

        cluster = self.cluster
        cluster.set_configuration_options({'auto_snapshot': 'false'})
        cluster.populate(nodes).start()
        node1 = self.cluster.nodelist()[0]
        session = self.patient_cql_connection(node1)
        ks = 'ks'
        create_ks(session, ks, rf=nodes)
        for i in range(num_tables):
            cf = f"cf{i}"
            num_keys = table_keys[i]
            flush_every = num_keys // 10
            compaction_options = "{'class': 'SizeTieredCompactionStrategy', 'max_threshold': 1}"
            create_c1c2_table(session, cf=cf, compaction=compaction_options)
            cluster.nodetool(f'disableautocompaction {ks} {cf}')

            logger.info(f"Inserting {num_keys} keys to ks.{cf}, flushing every {flush_every} keys")
            start_key = 0
            end_key = num_keys
            while start_key < end_key:
                batch_end = end_key if not flush_every else min(start_key + flush_every, end_key)
                insert_c1c2(session=session, keys=range(start_key, batch_end), cf=cf)
                start_key = batch_end
                for node in cluster.nodelist():
                    node.flush('ks', cf)

        def drop_keyspace(session, node, log_msg, from_mark):
            node.watch_log_for(log_msg, from_mark=from_mark)
            q = f"DROP KEYSPACE {ks}"
            logger.info(q)
            session.execute(q)

        executor = ThreadPoolExecutor(max_workers=1)

        log_msg = f"Cleanup {ks}"
        thread = executor.submit(drop_keyspace, session, node1, log_msg, node1.mark_log())

        logger.info("Running cleanup")
        node1.nodetool(f"cleanup {ks}")

        thread.result()
