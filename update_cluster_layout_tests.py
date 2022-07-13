import time
import os
import re
import logging
import collections
import random
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from pkg_resources import parse_version

import requests
import pytest
from cassandra import Unavailable, ConsistencyLevel, WriteTimeout, OperationTimedOut
from cassandra.policies import FallthroughRetryPolicy
from cassandra.query import SimpleStatement
from cassandra.cluster import NoHostAvailable
from ccmlib.node import NodetoolError
from ccmlib.scylla_cluster import ScyllaCluster
from ccmlib.scylla_node import ScyllaNode

from tools.assertions import assert_invalid

from dtest_class import Tester, create_ks, create_cf
from tools.data import create_c1c2_table, insert_c1c2, query_c1c2, query_c1c2_concurrent, insert_c1cn
from tools.cluster import new_node


logger = logging.getLogger(__name__)


@pytest.mark.dtest_full
class TestUpdateClusterLayout(Tester):

    @staticmethod
    def default_config_options(hinted_handoff_enabled=False, enable_sstable_key_validation=True):
        values = {}
        if hinted_handoff_enabled is not None:
            values.update({'hinted_handoff_enabled': hinted_handoff_enabled})
        if enable_sstable_key_validation is not None:
            values.update(
                {'enable_sstable_key_validation': enable_sstable_key_validation})
        return values

    def check_rows_on_node(self, node_to_check, rows, found=None, missings=None, restart=True, ks='ks', cf='cf',
                           counter_column=None, timeout=None):
        if found is None:
            found = []
        if missings is None:
            missings = []
        stopped_nodes = []

        for node in self.cluster.nodes.values():
            if node.is_running() and node is not node_to_check:
                stopped_nodes.append(node)
                node.stop(wait_other_notice=True)

        if not timeout:
            timeout = self.cql_timeout(300)

        session = self.patient_cql_connection(node_to_check, ks)
        if rows > 1000 and counter_column:
            query = SimpleStatement(f"SELECT count({counter_column}) FROM {ks}.{cf} LIMIT {rows * 2}",
                                    consistency_level=ConsistencyLevel.ONE)
            result = list(session.execute(query, timeout=timeout))
            count = result[0][0]
            assert count == rows
        else:
            query = SimpleStatement(f"SELECT * FROM {ks}.{cf} LIMIT {rows * 2}", consistency_level=ConsistencyLevel.ONE)
            result = list(session.execute(query, timeout=timeout))
            assert len(result) == rows

        for k in found:
            query_c1c2(session, k, ConsistencyLevel.ONE)

        for k in missings:
            query_c1c2(session, k, ConsistencyLevel.ONE, must_be_missing=True)

        if restart:
            self.cluster.start_nodes()

    def test_simple_add_node_1(self):
        """
        Test bootstrapped node streams all data
        1. Create a cluster with a single node with rf=2, insert data
        2. Add a new node
        3. Check that each node has all the data
        """
        cluster = self.cluster

        # Disable hinted handoff and set batch commit log so this doesn't
        # interfer with the test (this must be after the populate)
        cluster.set_configuration_options(
            values=self.default_config_options(), batch_commitlog=True)
        cluster.populate(1).start()
        node1 = cluster.nodelist()[0]

        session = self.patient_cql_connection(node1)
        create_ks(session, 'ks', 2)
        create_cf(session, 'cf', read_repair=0.0, columns={'c1': 'text', 'c2': 'text'})

        insert_c1c2(session, keys=range(1000), consistency=ConsistencyLevel.ONE)

        node2 = new_node(cluster)
        node2.start(wait_for_binary_proto=True)

        session = self.patient_exclusive_cql_connection(node2)
        session.execute("use ks;")
        node1.watch_log_for_alive(node2)
        node2.watch_log_for_alive(node1)

        insert_c1c2(session, keys=range(1000, 2000), consistency=ConsistencyLevel.TWO)

        self.check_rows_on_node(node2, 2000)
        self.check_rows_on_node(node1, 2000)

    def _iterative_add_decommission(self, iterations=2, node_count=2, rf=1):
        """
        Test growing and shrinking a cluster
        1. Create a cluster with a single node with rf=2, insert data
        2. In a loop add new nodes
        3. Check that all data exists
        4. In a loop remove all nodes but the last added one
        5. Check that all data exists
        """
        cluster = self.cluster

        # Disable hinted handoff and set batch commit log so this doesn't
        # interfer with the test (this must be after the populate)
        cluster.set_configuration_options(
            values=self.default_config_options(), batch_commitlog=True)
        cluster.populate(1).start()
        node1 = cluster.nodelist()[0]

        session = self.patient_cql_connection(node1)
        create_ks(session, 'ks', rf)
        create_cf(session, 'cf', read_repair=0.0, columns={'c1': 'text', 'c2': 'text'})

        consistency = ConsistencyLevel.ALL
        insert_c1c2(session, keys=range(1000), consistency=ConsistencyLevel.ONE)

        query = SimpleStatement("SELECT key FROM ks.cf limit 3000", consistency_level=consistency)
        for iteration in range(1, iterations + 1):
            for i in range(1, node_count + 1):
                node_i = new_node(cluster)
                node_i.start(wait_for_binary_proto=True, wait_other_notice=True)
                session_i = self.patient_exclusive_cql_connection(node_i)
                session_i.execute("use ks;")
                insert_c1c2(session_i, keys=range(iteration * 100000 + i * 2000, iteration * 100000 + i * 2000 + 100),
                            consistency=consistency)
                logger.debug("added %s" % node_i.name)

            result = list(session.execute(query))
            assert len(result) == iteration * node_count * 100 + 1000, \
                "data loss after increasing size to %d expecting %d rows %d" % \
                (len(cluster.nodelist()), iteration * node_count * 100 + 1000, len(result))

            for node_i in cluster.nodelist()[0:-1]:
                if node1.name != node_i.name and node_i.is_live():
                    node_i.decommission()
                    node_i.stop()
                    logger.debug("decommissioned %s" % node_i.name)

            last_node = cluster.nodelist()[-1]
            session = self.patient_cql_connection(last_node)
            result = list(session.execute("SELECT key FROM ks.cf limit 3000"))
            assert len(result) == iteration * node_count * 100 + 1000, \
                "data loss after shrinking to 2 node execpeting %d rows %d" % \
                (iteration * node_count * 100 + 1000, len(result))

        node1.decommission()
        node1.stop()
        logger.debug("decommissioned %s" % node1.name)
        last_node = cluster.nodelist()[-1]
        session = self.patient_cql_connection(last_node)
        result = list(session.execute("SELECT key FROM ks.cf limit 3000"))
        assert len(result) == iterations * node_count * 100 + 1000, \
            "data loss after shrinking to 1 node %s expecting %d rows %d" % \
            (last_node.name, iterations * node_count * 100 + 1000, len(result))

    def test_iterative_add_1_node_decommission_1_node_rf_1(self):
        """
        Test gorwing and shrinking a cluster 1 node in each iteration with rf=1
        """
        self._iterative_add_decommission(node_count=1, iterations=3, rf=1)

    def test_iterative_add_3_node_decommission_3_node_rf_1(self):
        """
        Test gorwing and shrinking a cluster 3 node in each iteration with rf=1
        """
        self._iterative_add_decommission(node_count=3, iterations=2, rf=1)

    def test_iterative_add_1_node_decommission_1_node_rf_2(self):
        """
        Test gorwing and shrinking a cluster 1 node in each iteration with rf=2
        """
        self._iterative_add_decommission(node_count=1, iterations=3, rf=2)

    def test_iterative_add_3_node_decommission_3_node_rf_2(self):
        """
        Test gorwing and shrinking a cluster 3 node in each iteration with rf=2
        """
        self._iterative_add_decommission(node_count=3, iterations=2, rf=2)

    def test_simple_add_two_nodes_in_parallel(self):
        """
        Test bootstrapped node streams all data
        1. Create a cluster with a single node with rf=3, insert data
        2. Add two nodes
        3. Check that first added node succeeds to join the cluster and completes bootstrap
        4. Check that the second fails with correct cause
        """
        cluster = self.cluster

        # Disable hinted handoff and set batch commit log so this doesn't
        # interfer with the test (this must be after the populate)
        cluster.set_configuration_options(
            values=self.default_config_options(), batch_commitlog=True)
        cluster.populate(1).start()
        node1 = cluster.nodelist()[0]

        session = self.patient_cql_connection(node1)
        create_ks(session, 'ks', 3)
        create_cf(session, 'cf', read_repair=0.0, columns={'c1': 'text', 'c2': 'text'})

        num_keys = 100000 if isinstance(cluster, ScyllaCluster) and cluster.scylla_mode != "debug" else 10000
        logger.debug("Inserting {} keys".format(num_keys))
        insert_c1c2(session, keys=range(num_keys), consistency=ConsistencyLevel.ONE)

        node2 = new_node(cluster)
        # creating an additional node without actually adding it to the cluster
        i = len(cluster.nodes) + 1
        node3 = cluster.new_node(i, auto_bootstrap=True, add_node=False)

        logger.debug("Starting node2")
        node2.start(no_wait=True)
        node2.watch_log_for("Starting to bootstrap")

        expected_error = "Other bootstrapping/leaving/moving nodes detected, cannot bootstrap while consistent_rangemovement is true"
        self.ignore_log_patterns += [expected_error]

        failed_to_detect = False
        try:
            logger.debug("Starting node3")
            cluster.add(node3, is_seed=False)
            node3.start(wait_other_notice=True, wait_for_binary_proto=True)
            # lets check that it detected there was another bootstrapping in progress
            try:
                logger.debug("Waiting until node3 notices other node was booting")
                node3.watch_log_for(
                    "Checking bootstrapping/leaving/moving nodes: node={}.* sleep 1 second and check again".format(node2.address()), timeout=5)
                logger.debug('Node3 noticed other node was booting')
            except:
                logger.debug('Node3 did not notice other node was booting')
                failed_to_detect = True
        except:
            # if the node was not allowed to boot check reason
            node3.watch_log_for(expected_error, timeout=5)
            logger.debug("Node 3 detected other node was booting and gave up booting")
        if failed_to_detect:
            pytest.fail("Node3 did not notice other node was booting")

        logger.debug("Check node2 started successfully")
        node2.watch_log_for("Starting listening for CQL clients")
        session = self.patient_exclusive_cql_connection(node2)
        session.execute("use ks;")
        node1.watch_log_for_alive(node2)
        node2.watch_log_for_alive(node1)

        logger.debug("Inserting more data")
        insert_c1c2(session, keys=range(num_keys, 2 * num_keys), consistency=ConsistencyLevel.TWO)

        logger.debug("Verifying...")
        self.check_rows_on_node(node2, 2 * num_keys)
        self.check_rows_on_node(node1, 2 * num_keys)

    @pytest.mark.next_gating
    def test_simple_kill_streaming_node_while_bootstrapping(self):
        """
        Test bootstrapped node streams all data
        1. Create a cluster with a three nodes with rf=3, insert data
        2. Add node, wait for node to start bootstrappig
        3. Kill original cluster node while it is streaming info to the new node
        4. Check that the new node has all data
        """
        cluster = self.cluster

        # Disable hinted handoff and set batch commit log so this doesn't
        # interfer with the test (this must be after the populate)
        cluster.set_configuration_options(
            values=self.default_config_options(), batch_commitlog=True)
        cluster.populate(3).start()
        node1, node2, node3 = cluster.nodelist()

        session = self.patient_cql_connection(node1)
        create_ks(session, 'ks', 4)
        create_cf(session, 'cf', read_repair=0.0, columns={'c1': 'text', 'c2': 'text'})

        insert_c1c2(session, keys=range(1000), consistency=ConsistencyLevel.THREE)

        logger.debug("Inserting more data to make streaming process longer...")
        node1.stress(['write', 'n=20000', 'no-warmup', '-schema', 'replication(factor=3) keyspace=ks1'])
        node2.stress(['write', 'n=20000', 'no-warmup', '-schema', 'replication(factor=3) keyspace=ks2'])
        node3.stress(['write', 'n=20000', 'no-warmup', '-schema', 'replication(factor=3) keyspace=ks3'])

        logger.debug("Flush cluster...")
        self.cluster.flush()

        logger.debug("Start node 4...")
        node4 = new_node(cluster)
        node4.start(jvm_args=['--logger-log-level', 'stream_session=debug'], no_wait=True)
        node4.watch_log_for("Starting to bootstrap")
        log_timeout = 600
        if isinstance(self.cluster, ScyllaCluster) and self.cluster.scylla_mode == 'debug':
            log_timeout *= 3
        node4.watch_log_for(
            "Beginning stream session|sync data for keyspace=ks[1-3]?, status=started", timeout=log_timeout)
        logger.debug("Bootstrap started streaming/repair")

        self.ignore_log_patterns += [
            r'[Rr]epair.*mandatory neighbor={} is not alive'.format(node2.address()),
            r'Startup failed:.*Failed to repair for keyspace=ks[1-3]?',
            r'Startup failed: std::runtime_error .* \(repair .* failed',
            r'Startup failed: std::runtime_error .*rpc::closed_error',
            r'Startup failed: seastar::sleep_aborted',
            r'stream_session .* Failed to handle STREAM_MUTATION_FRAGMENTS .* peer={}'.format(node2.address()),
            r'storage_service .* fail to update tokens for .*: exceptions::mutation_write_failure_exception',
            r'Abort bootstrap operation',
            r'Startup failed: seastar::rpc::closed_error',
            r'Startup failed: streaming::stream_exception \(Stream failed\)',
        ]

        sleep_time = random.random() * 0.25
        if isinstance(self.cluster, ScyllaCluster) and self.cluster.scylla_mode == 'debug':
            sleep_time *= 4
        time.sleep(sleep_time)
        logger.debug("Stop node 2...")
        node2.stop(gently=False)

        logger.debug("Look for Stream/Startup failed in node 4...")
        # The keep alive timer expires in 10 minutes.
        # Wait 5 minutes more in the test to wait for the stream to fail
        node4.watch_log_for("Stream failed|Startup failed|sync data for keyspace=ks[1-3]?, status=failed", timeout=900)

    def test_simple_kill_new_node_while_bootstrapping(self):
        """
        Test bootstrapped node streams all data
        1. Create a cluster with a three nodes with rf=1, insert data
        2. Add node, wait for each to start bootstrapping and kill it
        3. Add node, wait for each to start bootstrapping and kill it
        4. Check that the cluster returns all
        """
        cluster = self.cluster

        # Disable hinted handoff and set batch commit log so this doesn't
        # interfer with the test (this must be after the populate)
        cluster.set_configuration_options(
            values=self.default_config_options(), batch_commitlog=True)
        cluster.populate(3).start()
        node1 = cluster.nodelist()[0]
        node2 = cluster.nodelist()[1]
        node3 = cluster.nodelist()[2]

        session = self.patient_cql_connection(node1)
        create_ks(session, 'ks', 1)
        create_cf(session, 'cf', read_repair=0.0, columns={'c1': 'text', 'c2': 'text'})

        insert_c1c2(session, keys=range(1000), consistency=ConsistencyLevel.ONE)

        logger.debug("Inserting more data to make streaming process longer...")
        node1.stress(['write', 'n=5000', 'no-warmup', '-schema', 'replication(factor=3) keyspace=ks1'])
        node2.stress(['write', 'n=5000', 'no-warmup', '-schema', 'replication(factor=3) keyspace=ks2'])
        node3.stress(['write', 'n=5000', 'no-warmup', '-schema', 'replication(factor=3) keyspace=ks3'])

        for i in range(4, 5):
            # creating an additional node without actually adding it to the cluster
            new_node = cluster.new_node(i, auto_bootstrap=True, add_node=False)
            logger.debug("Start Node %d" % i)
            new_node.start(jvm_args=['--logger-log-level', 'stream_session=debug'], no_wait=True)
            new_node.watch_log_for("Starting to bootstrap")
            new_node.watch_log_for("Beginning stream session|sync data for keyspace=ks, status=started")
            logger.debug("Stop Node %d" % i)
            new_node.stop(gently=False)

            # Sleep 1 second to make sure other nodes knows this node is joining through gossip
            time.sleep(1)

            # Check the status:
            # We expect the new node will not be added to the cluster
            # status looks like below, new_node should not be in UN state but in UJ state.
            # UN  127.0.0.1  99823      256     ?       a7498138-1878-421d-8f11-cc98b204090a  rack1
            # UN  127.0.0.2  37278      256     ?       f118383c-c569-49d1-9aa6-223d3b224caa  rack1
            # UN  127.0.0.3  24834      256     ?       78b7e6ba-3039-4fc6-a875-a71661f8cd04  rack1
            # UJ  127.0.0.4  ?          256     ?       637edd3f-8888-48ab-b0ea-3ea81f8e9865  rack1
            self.wait_for_nodes_status(node1, ['UN', 'UN', 'UN', 'UJ'])

            # Sleep 30 seconds to make sure other nodes removed the new node
            time.sleep(30)
            node1.watch_log_for("FatClient .* has been silent for .*ms, removing from gossip")
            node2.watch_log_for("FatClient .* has been silent for .*ms, removing from gossip")
            node3.watch_log_for("FatClient .* has been silent for .*ms, removing from gossip")

            # Check status again:
            # status looks like below, new_node should not be in UN state
            # UN  127.0.0.1  99823      256     ?       a7498138-1878-421d-8f11-cc98b204090a  rack1
            # UN  127.0.0.2  37278      256     ?       f118383c-c569-49d1-9aa6-223d3b224caa  rack1
            # UN  127.0.0.3  24834      256     ?       78b7e6ba-3039-4fc6-a875-a71661f8cd04  rack1
            self.wait_for_nodes_status(node1, ['UN', 'UN', 'UN'])

        result = list(session.execute("SELECT * FROM cf"))
        assert len(result) == 1000

    def test_simple_kill_new_node_while_bootstrapping_with_parallel_writes(self):
        """
        Test bootstrapped node streams all data
        1. Create a cluster with a three nodes with rf=1, insert data
        2. Add node, wait for each to start bootstrapping and write additional data
        3. kill it while writing data
        4. Check that the operation exists with an expected exception
        """

        cluster = self.cluster

        # Disable hinted handoff and set batch commit log so this doesn't
        # interfer with the test (this must be after the populate)
        cluster.set_configuration_options(
            values=self.default_config_options(), batch_commitlog=True)
        cluster.populate(3).start(wait_for_binary_proto=True, wait_other_notice=True)
        node1 = cluster.nodelist()[0]
        node2 = cluster.nodelist()[1]
        node3 = cluster.nodelist()[2]

        session = self.cql_connection(node1)
        # use rf=2 so that the quorum will temporarily increase to 3
        # while adding the new node
        create_ks(session, 'ks', 2)
        create_c1c2_table(session)

        keys = 1000
        insert_c1c2(session, keys=range(keys), consistency=ConsistencyLevel.ALL)

        logger.debug("Inserting more data to make streaming process longer...")
        node1.stress(['write', 'n=5000', 'no-warmup', '-schema', 'replication(factor=3) keyspace=ks1'])
        node2.stress(['write', 'n=5000', 'no-warmup', '-schema', 'replication(factor=3) keyspace=ks2'])
        node3.stress(['write', 'n=5000', 'no-warmup', '-schema', 'replication(factor=3) keyspace=ks3'])

        for i in range(4, 5):
            # creating an additional node without actually adding it to the cluster
            new_node = cluster.new_node(i, auto_bootstrap=True, add_node=False)
            failed = None
            stop_writing = False

            def run():
                nonlocal keys, failed, stop_writing
                logger.debug("start write")
                while not stop_writing:
                    # working around the default retry_policy that attempts 5 times
                    statement = SimpleStatement("INSERT INTO cf (key, c1, c2) VALUES ('k%d', 'value1', 'value2')" %
                                                keys, consistency_level=ConsistencyLevel.QUORUM,
                                                retry_policy=FallthroughRetryPolicy())
                    tbefore = str(datetime.now())
                    try:
                        session.execute(statement)
                        keys += 1
                    except (Unavailable) as e:
                        tfailed = str(datetime.now())
                        logger.debug("exception thrown Unavailable %s" % e)
                        pass
                    except (WriteTimeout) as e:
                        tfailed = str(datetime.now())
                        logger.debug("exception thrown WriteTimeout %s" % e)
                        pass
                    except (OperationTimedOut) as e:
                        tfailed = str(datetime.now())
                        failed = "Server side exception not thrown  driver side exception thrown OperationTimeout %s %s %s"\
                            % (e, tbefore, tfailed)
                        logger.debug(failed)
                logger.debug("end write")

            executor = ThreadPoolExecutor(max_workers=1)
            t = executor.submit(run)
            logger.debug("Start Node %d" % i)
            new_node.start(jvm_args=['--logger-log-level', 'stream_session=debug'], no_wait=True)
            new_node.watch_log_for([
                "Starting to bootstrap",
                "Beginning stream session|sync data for keyspace=ks, status=started",
                "Streaming plan for Bootstrap-ks-index-2 succeeded|Repair 100 out of.*keyspace=ks,",
            ])
            logger.debug("Stop Node %d" % i)
            new_node.stop(gently=False, wait_other_notice=True)
            for node in [node1, node2, node3]:
                self.wait_for_nodes_status(node, [['UN', 'UN', 'UN'], ['UN', 'UN', 'UN', 'DN']])
            stop_writing = True
            t.result()
            assert failed is None

            for node in [node1, node2, node3]:
                status = self.nodetool_status(node, 'ks')
                logger.debug("nodetool status from {}: {}".format(node.name, status))

            logger.debug("Query Again")
            query = SimpleStatement("SELECT * FROM cf", consistency_level=ConsistencyLevel.QUORUM)
            rows = list(session.execute(query))
            assert len(rows) >= keys and len(rows) <= keys + \
                1, "Expected between {} and {} rows, but got {}".format(keys, keys+1, len(rows))

    def test_simple_kill_new_node_while_bootstrapping_with_parallel_writes_in_multidc(self):
        """
        Test bootstrapped node streams all data
        1. Create a cluster with two nodes in multidc with rf=1, insert data
        2. Add node, wait for each to start bootstrapping and write additional data
        3. kill it
        4. Check that the cluster returns all
        """
        cluster = self.cluster

        # Disable hinted handoff and set batch commit log so this doesn't
        # interfer with the test (this must be after the populate)
        cluster.set_configuration_options(
            values=self.default_config_options(), batch_commitlog=True)
        cluster.populate([1, 1]).start(wait_for_binary_proto=True, wait_other_notice=True)
        node1 = cluster.nodelist()[0]
        node2 = cluster.nodelist()[1]

        session = self.cql_connection(node1)
        create_ks(session, 'ks', {'dc1': 1, 'dc2': 1})
        create_c1c2_table(session)

        keys = 1000
        insert_c1c2(session, keys=range(keys), consistency=ConsistencyLevel.EACH_QUORUM)

        logger.debug("Inserting more data to make streaming process longer...")
        node1.stress(['write', 'n=5000', 'no-warmup', '-schema', 'replication(factor=3) keyspace=ks1'])
        node2.stress(['write', 'n=5000', 'no-warmup', '-schema', 'replication(factor=3) keyspace=ks2'])

        # create a new node and adding it - we cannot do this more then once
        a_new_node = new_node(cluster, data_center='dc1')
        failed = None
        stop_writing = False

        def run():
            nonlocal keys, failed, stop_writing

            logger.debug("start write")
            while not stop_writing:
                # working around the default retry_policy that attempts 5 times
                statement = SimpleStatement("INSERT INTO cf (key, c1, c2) VALUES ('k%d', 'value1', 'value2')" %
                                            keys, consistency_level=ConsistencyLevel.EACH_QUORUM,
                                            retry_policy=FallthroughRetryPolicy())
                tbefore = str(datetime.now())
                try:
                    session.execute(statement)
                    keys += 1
                except (Unavailable) as e:
                    tfailed = str(datetime.now())
                    logger.debug("exception thrown Unavailable %s" % e)
                    pass
                except (WriteTimeout) as e:
                    tfailed = str(datetime.now())
                    logger.debug("exception thrown WriteTimeout %s" % e)
                    pass
                except (OperationTimedOut) as e:
                    tfailed = str(datetime.now())
                    failed = "Server side exception not thrown driver side exception thrown OperationTimeout %s %s %s" %\
                        (e, tbefore, tfailed)
                    logger.debug(failed)
            logger.debug("end write")

        executor = ThreadPoolExecutor(max_workers=1)
        t = executor.submit(run)
        logger.debug("Start Node")
        a_new_node.start(jvm_args=['--logger-log-level', 'stream_session=debug'], no_wait=True)
        a_new_node.watch_log_for([
            "Starting to bootstrap",
            "Beginning stream session|sync data for keyspace=ks, status=started",
            "Streaming plan for Bootstrap-ks-index-2 succeeded|Repair 100 out of.*keyspace=ks,",
        ])
        logger.debug("Stop Node")
        a_new_node.stop(gently=False, wait_other_notice=True)
        self.wait_for_nodes_status(node1, [['UN', 'UN'], ['UN', 'DN', 'UN']])
        stop_writing = True
        t.result()
        assert failed is None

        for node in [node1, node2]:
            status = self.nodetool_status(node, 'ks')
            logger.debug("nodetool status from {}: {}".format(node.name, status))

        logger.debug("Query Again")
        query = SimpleStatement("SELECT * FROM cf", consistency_level=ConsistencyLevel.QUORUM)
        rows = list(session.execute(query))
        assert len(rows) >= keys and len(rows) <= keys + \
            1, "Expected between {} and {} rows, but got {}".format(keys, keys+1, len(rows))

    def _simple_add_new_node_while_adding_info(self, rf):
        """
        Test bootstrapped node streams all data

        1. Create a cluster with a three nodes with rf, insert data
        3. Add node, while node is bootstrapping insert data
        4. Check that the cluster returns all
        """
        cluster = self.cluster
        consistency = {1: ConsistencyLevel.ONE, 2: ConsistencyLevel.TWO}[rf]

        # Disable hinted handoff and set batch commit log so this doesn't
        # interfer with the test (this must be after the populate)
        cluster.set_configuration_options(
            values=self.default_config_options(), batch_commitlog=True)
        cluster.populate(3).start()
        node1 = cluster.nodelist()[0]

        session = self.patient_cql_connection(node1)
        create_ks(session, 'ks', rf)
        create_cf(session, 'cf', read_repair=0.0, columns={'c1': 'text', 'c2': 'text'})

        insert_c1c2(session, keys=range(2000), consistency=consistency)

        node4 = new_node(cluster)
        node4.start(jvm_args=['--logger-log-level', 'stream_session=debug'], no_wait=True)
        node4.watch_log_for("Starting to bootstrap")
        node4.watch_log_for("Beginning stream session|sync data for keyspace=ks, status=started")
        insert_c1c2(session, keys=range(2000, 4000), consistency=consistency)

        query = SimpleStatement("SELECT * FROM cf", consistency_level=consistency)
        result = list(session.execute(query))
        assert len(result) == 4000

        for k in range(0, 4000):
            query_c1c2(session, k, consistency)

    def test_simple_add_new_node_while_adding_info_1(self):
        self._simple_add_new_node_while_adding_info(1)

    def test_simple_add_new_node_while_adding_info_2(self):
        self._simple_add_new_node_while_adding_info(2)

    def repair_based_node_ops_config_options(self, enable_repair_based_node_ops: bool, ops=["bootstrap", "replace", "removenode", "decommission", "rebuild"]):
        config_options = self.default_config_options()
        config_options.update({'enable_repair_based_node_ops': enable_repair_based_node_ops})
        # Configuring allowed_repair_based_node_ops is required
        # since scylladb/scylla@97bb2e47ff004b32b2d72f1b1f085710a14cb4e2
        if enable_repair_based_node_ops and \
           parse_version(self.cluster.version()) >= parse_version('4.6.dev'):
            config_options.update({'allowed_repair_based_node_ops': ','.join(ops)})
        return config_options

    def _simple_add_new_node_while_schema_changes(self, enable_repair_based_node_ops=True):
        """
        Test bootstrapped node sync all data

        1. Create a cluster with a three nodes with rf=1, insert data
        2. Add node, while node is bootstrapping remove keyspace
        3. Still while bootstrapping add a keyspace and insert data
        4. Check that node was connected and the cluster returns all
        """
        cluster = self.cluster
        rf = 1
        consistency = {1: ConsistencyLevel.ONE, 2: ConsistencyLevel.TWO}[rf]

        # Disable hinted handoff and set batch commit log so this doesn't
        # interfer with the test (this must be after the populate)
        config_options = self.repair_based_node_ops_config_options(enable_repair_based_node_ops)
        cluster.set_configuration_options(values=config_options,
                                          batch_commitlog=True)
        cluster.populate(3).start()
        node1 = cluster.nodelist()[0]

        session = self.patient_cql_connection(node1)
        create_ks(session, 'ks', rf)
        create_cf(session, 'cf', read_repair=0.0, columns={'c1': 'text', 'c2': 'text'})

        insert_c1c2(session, keys=range(4000), consistency=consistency)

        def run():
            query = SimpleStatement("DROP KEYSPACE ks")
            result = list(session.execute(query))

            create_ks(session, 'ks1', rf)
            create_cf(session, 'cf1', read_repair=0.0, columns={'c1': 'text', 'c2': 'text'})
            for i in range(0, 100):
                insert = SimpleStatement("insert into ks1.cf1 (key,c1,c2) values ('%d','%d','%d')" %
                                         (i, i, i), consistency_level=consistency)
                session.execute(insert)

        executor = ThreadPoolExecutor(max_workers=1)

        node4 = new_node(cluster)
        node4.start(jvm_args=['--logger-log-level', 'stream_session=debug'], no_wait=True)
        node4.watch_log_for("Starting to bootstrap")
        node4.watch_log_for("Beginning stream session|sync data for keyspace=ks, status=started")

        self.ignore_log_patterns += [
            rf"ks=ks, cf=cf, .*no_such_column_family",
        ]

        t = executor.submit(run)

        if enable_repair_based_node_ops:
            node4.watch_log_for("completed successfully, keyspace=ks")

        node4.watch_log_for("Starting listening for CQL clients")
        session = self.patient_cql_connection(node4)

        t.result()
        # Verify keyspace ks is deleted
        assert_invalid(session, "SELECT * FROM ks.cf", "Keyspace ks does not exist")
        query = SimpleStatement("SELECT * FROM ks1.cf1", consistency_level=consistency)
        result = list(session.execute(query))
        assert len(result) == 100

    def test_simple_add_new_node_while_schema_changes_with_repair(self):
        """
        Test bootstrapped node sync all data by repair, schema will be
        changed in the same time.
        """
        self._simple_add_new_node_while_schema_changes(enable_repair_based_node_ops=True)

    def test_simple_add_new_node_while_schema_changes_with_stream(self):
        """
        Test bootstrapped node sync all data by streaming, schema will
        be changed in the same time.
        """
        self._simple_add_new_node_while_schema_changes(enable_repair_based_node_ops=False)

    def _simple_add_new_node_while_query_info(self, rf):
        """
        Test bootstrapped node streams all data
        1. Create a cluster with a three nodes with rf, insert data
        3. Add node, while node is bootstrapping query data
        4. Check that the cluster returns all
        """
        cluster = self.cluster
        consistency = {1: ConsistencyLevel.ONE, 2: ConsistencyLevel.TWO}[rf]

        # Disable hinted handoff and set batch commit log so this doesn't
        # interfer with the test (this must be after the populate)
        cluster.set_configuration_options(
            values=self.default_config_options(), batch_commitlog=True)
        cluster.populate(3).start()
        node1 = cluster.nodelist()[0]

        session = self.patient_cql_connection(node1)
        create_ks(session, 'ks', rf)
        create_cf(session, 'cf', read_repair=0.0, columns={'c1': 'text', 'c2': 'text'})

        insert_c1c2(session, keys=range(2000), consistency=consistency)

        def run():
            for i in range(1, 100):
                query = SimpleStatement("SELECT * FROM cf", consistency_level=consistency)
                result = list(session.execute(query))
                assert len(result) == 2000
                time.sleep(0.01)

        executor = ThreadPoolExecutor(max_workers=1)

        node4 = new_node(cluster)
        node4.start(jvm_args=['--logger-log-level', 'stream_session=debug'], no_wait=True)
        node4.watch_log_for("Starting to bootstrap")
        node4.watch_log_for("Beginning stream session|sync data for keyspace=ks, status=started")
        t = executor.submit(run)

        node4.watch_log_for("Starting listening for CQL clients")

        query = SimpleStatement("SELECT * FROM cf", consistency_level=consistency)

        result = list(session.execute(query))
        assert len(result) == 2000
        for k in range(0, 2000):
            query_c1c2(session, k, consistency)

        t.result()

    def test_simple_add_new_node_while_query_info_1(self):
        self._simple_add_new_node_while_query_info(1)

    def test_simple_add_new_node_while_query_info_2(self):
        self._simple_add_new_node_while_query_info(2)

    def test_simple_decommission_node_1(self):
        """
        Test decommissioned node streams all data
        1. Create a cluster with a single node with rf=1, insert data
        2. Decommission one node
        3. Check that the last node has all the data
        """
        cluster = self.cluster

        # Disable hinted handoff and set batch commit log so this doesn't
        # interfer with the test (this must be after the populate)
        cluster.set_configuration_options(
            values=self.default_config_options(), batch_commitlog=True)
        cluster.populate(2).start(wait_for_binary_proto=True, wait_other_notice=True)
        node1, node2 = cluster.nodelist()

        session = self.patient_cql_connection(node1)
        create_ks(session, 'ks', 1)
        create_cf(session, 'cf', read_repair=0.0, columns={'c1': 'text', 'c2': 'text'})

        insert_c1c2(session, keys=range(1000), consistency=ConsistencyLevel.ONE)

        node2.decommission()
        # lets verify new connection can not be openned to a decomissioned node
        with pytest.raises(NoHostAvailable):
            self.patient_cql_connection(node2)
        node2.stop()

        self.check_rows_on_node(node1, 1000, restart=False)

    def test_simple_decommission_node_2(self):
        """
        Test that on decommission row cache entries of non owned transfered range are invalidated

        1. Create a cluster with a single node with rf=1,insert data
        2. Check the row cahe can be used as an estimator
        3. Add a new node
        4. Cleanup data on original node (cache not cleared)
        5. Check that all data can be read
        6. Delete all the data
        7. Compact data on new node
        8. Restart new node (it should not have any data including tombstones)
        9. Decommission the new node
        10. Test if any data exists in the cluster
        """

        cluster = self.cluster

        # Disable hinted handoff and set batch commit log so this doesn't
        # interfer with the test (this must be after the populate)
        cluster.set_configuration_options(
            values=self.default_config_options(), batch_commitlog=True)
        cluster.populate(1).start()
        node1 = cluster.nodelist()[0]

        session_node1 = self.patient_cql_connection(node1)
        create_ks(session_node1, 'ks', 1)
        create_cf(session_node1, 'cf', gc_grace=0, read_repair=0.0, columns={'c1': 'text', 'c2': 'text'})

        node1.flush()

        insert_c1c2(session_node1, keys=range(1000), consistency=ConsistencyLevel.ONE)

        node1.flush()

        # We booted the new node and it got part of the items
        node2 = new_node(cluster)
        node2.start(wait_for_binary_proto=True, wait_other_notice=True)
        node1.cleanup()

        session_node2 = self.patient_exclusive_cql_connection(node2)
        session_node2.execute("use ks;")
        node1.watch_log_for_alive(node2)
        node2.watch_log_for_alive(node1)

        result = list(session_node1.execute("SELECT * FROM ks.cf limit 2000;"))
        assert len(result) == 1000, "expected 1000 lines got %d" % len(result)

        for i in range(0, 1000, 100):
            session_node1.execute("DELETE from ks.cf where key in (\'k%s\');" %
                                  "\',\'k".join(str(x) for x in range(i, i+100)))

        result = list(session_node1.execute("SELECT * FROM ks.cf limit 2000;"))
        assert len(result) == 0, "expected 0 lines got %d %s" % (len(result), result)

        cluster.flush()
        # lets make sure all the data in ssstables is removed
        node2.compact()
        # restart the node to make sure no data is left
        node2.stop()
        node2.start(wait_for_binary_proto=True, wait_other_notice=True)

        node2.decommission()
        node1.flush()

        result = list(session_node1.execute("SELECT * FROM ks.cf"))
        assert len(result) == 0, "expected 0 lines got %d" % len(result)

    @pytest.mark.next_gating
    def test_simple_kill_node_while_decommissioning(self):
        """
        Test a decommissioning node killed is able to rejoin the cluster with data
        1. Create a cluster with a three nodes with rf=1, insert data
        2. Decommission a node
        3. While node is decommissioning kill it
        4. Boot the node back up
        5. Check that the node rejoins the cluster and works correctly
        """
        cluster = self.cluster

        # Disable hinted handoff and set batch commit log so this doesn't
        # interfer with the test (this must be after the populate)
        cluster.set_configuration_options(
            values=self.default_config_options(), batch_commitlog=True)
        cluster.populate(3).start(wait_for_binary_proto=True, wait_other_notice=True,
                                  jvm_args=['--logger-log-level', 'stream_session=debug'])
        node1, node2, node3 = cluster.nodelist()

        session = self.patient_cql_connection(node1)
        create_ks(session, 'ks', 1)
        create_cf(session, 'cf', read_repair=0.0, columns={'c1': 'text', 'c2': 'text'})

        insert_c1c2(session, keys=range(10000), consistency=ConsistencyLevel.ONE)

        def run():
            try:
                node2.decommission()
            except Exception:
                pass

        executor = ThreadPoolExecutor(max_workers=1)
        executor.submit(run)

        # check node2 has started decommission
        node2.watch_log_for("DECOMMISSIONING: unbootstrap starts")
        node2.watch_log_for("Beginning stream session|sync data for keyspace=ks, status=started")

        self.ignore_log_patterns += ["Failed to handle STREAM_MUTATION_FRAGMENTS.*peer={}".format(node2.address())]

        logger.debug("Stop node2 ")
        node2.stop(gently=False)

        # starting node2 - it should reconnect and run as is
        logger.debug("Start node2 ")
        node2.start(wait_other_notice=False, wait_for_binary_proto=True)
        session2 = self.patient_cql_connection(node2)
        result = list(session2.execute("SELECT * FROM ks.cf"))
        assert len(result) == 10000

        self.verify_nodes_status(node1, ['UN', 'UN', 'UN'])

    def test_simple_kill_remained_node_while_decommissioning(self):
        """
        Test a decommissioning node killed is able to rejoin the cluster with data
        1. Create a cluster with a three nodes with rf=1, insert data
        2. Decommission a node
        3. While node is decommissioning kill another one
        4. Boot the node back up
        5. Check that the node rejoins the cluster and works correctly
        """
        cluster = self.cluster

        # Disable hinted handoff and set batch commit log so this doesn't
        # interfer with the test (this must be after the populate)
        cluster.set_configuration_options(
            values=self.default_config_options(), batch_commitlog=True)
        cluster.populate(3).start(wait_for_binary_proto=True, wait_other_notice=True,
                                  jvm_args=['--logger-log-level', 'stream_session=debug'])
        node1, node2, node3 = cluster.nodelist()

        session = self.patient_cql_connection(node1)
        create_ks(session, 'ks', 1)
        create_cf(session, 'cf', read_repair=0.0, columns={'c1': 'text', 'c2': 'text'})
        insert_c1c2(session, keys=range(1000), consistency=ConsistencyLevel.ONE)

        def run():
            try:
                node2.decommission()
            except Exception:
                pass

        executor = ThreadPoolExecutor(max_workers=1)
        executor.submit(run)

        # check node2 has started decommission
        node2.watch_log_for("DECOMMISSIONING: unbootstrap starts")
        node2.watch_log_for("Beginning stream session|sync data for keyspace=ks, status=started")

        node1.stop(gently=False)

        # starting node1 - it should reconnect and run as is
        node1.start(wait_other_notice=False, wait_for_binary_proto=True)
        result = list(session.execute("SELECT * FROM cf"))
        assert len(result) == 1000, "Should have 1000 items in table"

        # When node1 stops, the decommission of node2 will fail. We should not
        # test node2 is still in UL here. Test node2 is in either UL or UN.
        self.wait_for_nodes_status(node3, [['UN', 'UL', 'UN'], ['UN', 'UN', 'UN']])

        node2.stop()
        node2.start(no_wait=True)

        self.wait_for_nodes_status(node3, ['UN', 'UN', 'UN'])

    def verify_nodes_status(self, node, exp_statuses_list, keyspace=""):
        if exp_statuses_list and not isinstance(exp_statuses_list[0], list):
            exp_statuses_list = [exp_statuses_list]
        status = self.nodetool_status(node, keyspace)
        statuses = [s['status'] for s in status['nodes']]
        find_expected_status = False
        for exp_statuses in exp_statuses_list:
            if exp_statuses == statuses:
                find_expected_status = True
        assert find_expected_status, "found statuses: %s" % statuses

    def wait_for_nodes_status(self, node, exp_statuses, keyspace="", timeout=30):
        if isinstance(self.cluster, ScyllaCluster) and self.cluster.scylla_mode == 'debug':
            timeout *= 3
        timeout = time.time() + timeout
        while True:
            try:
                self.verify_nodes_status(node, exp_statuses, keyspace=keyspace)
                break
            except AssertionError:
                time.sleep(1)
                if time.time() > timeout:
                    self.verify_nodes_status(node, exp_statuses, keyspace=keyspace)

    def nodetool_status(self, node, keyspace=""):
        res = {}
        out = node.nodetool("status " + keyspace, True)[0]
        m = re.findall(r'Datacenter: ([^\s]+)', out, re.MULTILINE)
        if m:
            res['Datacenter'] = m[0]
        m = re.findall(
            r'^([UDNLJM]+)\s+([\d\.]+)\s+([^\s]+\s+[^\s]+)\s+([^\s]+)\s+([^\s]+)(?:\s[^\s]{2})?\s+([^\s]+)\s+([^\s]+)\s*', out, re.MULTILINE)
        res["nodes"] = [self._list2status(s) for s in m]
        return res

    @staticmethod
    def _list2status(lst):
        heads = ["status", "address", "load", "tokens", "owns", "host id", "rack"]
        res = {}
        for i in range(len(heads)):
            res[heads[i]] = lst[i]
        return res

    def _simple_decommission_node_while_adding_info(self, rf):
        """
        Test bootstrapped node streams all data
        1. Create a cluster with a three nodes with rf, insert data
        2. Decommission node, while node is decommissioning insert data
        3. Check that the cluster returns all
        """
        cluster = self.cluster
        consistency = {1: ConsistencyLevel.ONE, 2: ConsistencyLevel.TWO}[rf]

        # Disable hinted handoff and set batch commit log so this doesn't
        # interfer with the test (this must be after the populate)
        cluster.set_configuration_options(
            values=self.default_config_options(), batch_commitlog=True)
        cluster.populate(3).start()
        node1, node2, node3 = cluster.nodelist()

        session = self.patient_cql_connection(node1)
        create_ks(session, 'ks', rf)
        create_cf(session, 'cf', read_repair=0.0, columns={'c1': 'text', 'c2': 'text'})

        insert_c1c2(session, keys=range(2000), consistency=consistency)

        def run():
            insert_c1c2(session, keys=range(2000, 4000), consistency=consistency)

            query = SimpleStatement("SELECT * FROM cf", consistency_level=consistency)
            result = list(session.execute(query))
            assert len(result) == 4000, "should have 4000 items in table"

        executor = ThreadPoolExecutor(max_workers=1)
        t = executor.submit(run)

        node2.decommission()

        t.result()
        node2.stop()
        query = SimpleStatement("SELECT * FROM cf", consistency_level=consistency)
        result = list(session.execute(query))
        assert len(result) == 4000, "should have 4000 items in table"
        for k in range(0, 4000):
            query_c1c2(session, k, consistency)

    def test_simple_decommission_node_while_adding_info_1(self):
        self._simple_decommission_node_while_adding_info(1)

    def test_simple_decommission_node_while_adding_info_2(self):
        self._simple_decommission_node_while_adding_info(2)

    def _simple_decommission_node_while_query_info(self, rf):
        """
        Test decommissioning node streams all data
        1. Create a cluster with a three nodes with rf, insert data
        2. Decommission node, while node is decommissioning query data
        3. Check that the cluster returns all
        """
        cluster = self.cluster
        consistency = {1: ConsistencyLevel.ONE, 2: ConsistencyLevel.TWO}[rf]

        # Disable hinted handoff and set batch commit log so this doesn't
        # interfer with the test (this must be after the populate)
        cluster.set_configuration_options(
            values=self.default_config_options(), batch_commitlog=True)
        cluster.populate(3).start()
        node1, node2, node3 = cluster.nodelist()

        session = self.patient_cql_connection(node1)
        create_ks(session, 'ks', rf)
        create_cf(session, 'cf', read_repair=0.0, columns={'c1': 'text', 'c2': 'text'})

        insert_c1c2(session, keys=range(2000), consistency=consistency)

        def run():
            for i in range(1, 100):
                query = SimpleStatement("SELECT * FROM cf", consistency_level=consistency)
                result = list(session.execute(query))
                assert len(result) == 2000
                time.sleep(0.01)

        executor = ThreadPoolExecutor(max_workers=1)
        t = executor.submit(run)

        node2.decommission()

        query = SimpleStatement("SELECT * FROM cf", consistency_level=consistency)
        result = list(session.execute(query))
        assert len(result) == 2000

        node2.stop()

        query = SimpleStatement("SELECT * FROM cf", consistency_level=consistency)
        result = list(session.execute(query))
        assert len(result) == 2000
        for k in range(0, 2000):
            query_c1c2(session, k, consistency)

        t.result()

    def test_simple_decommission_node_while_query_info_1(self):
        self._simple_decommission_node_while_query_info(1)

    def test_simple_decommission_node_while_query_info_2(self):
        self._simple_decommission_node_while_query_info(2)

    @pytest.mark.next_gating
    def test_simple_removenode_1(self):
        """
        Test removenode with rf>1 (no data should be lost)
        1. Create a cluster with a two node with rf=2, insert data
        2. stop and remove a node
        3. Check that the data is accesible
        """
        cluster = self.cluster

        # Disable hinted handoff and set batch commit log so this doesn't
        # interfer with the test (this must be after the populate)
        cluster.set_configuration_options(
            values=self.default_config_options(), batch_commitlog=True)
        cluster.populate(3).start()
        node1, node2, node3 = cluster.nodelist()

        session = self.patient_cql_connection(node1)
        create_ks(session, 'ks', 2)
        create_cf(session, 'cf', read_repair=0.0, columns={'c1': 'text', 'c2': 'text'})

        insert_c1c2(session, keys=range(100), consistency=ConsistencyLevel.ALL)

        node2_hostid = node2.hostid()
        node2.stop(wait_other_notice=True)
        query = SimpleStatement("SELECT * FROM cf", consistency_level=ConsistencyLevel.ONE)
        result = list(session.execute(query))
        assert len(result) == 100

        node1.nodetool("removenode %s" % node2_hostid)
        time.sleep(2)
        query = SimpleStatement("SELECT * FROM cf", consistency_level=ConsistencyLevel.TWO)
        result = list(session.execute(query))
        assert len(result) == 100
        insert_c1c2(session, keys=range(120), consistency=ConsistencyLevel.TWO)

    def test_simple_removenode_2(self):
        """
        Test removenode when rf=1 (data will be lost)
        1. Create a cluster with a two node with rf=1, insert data
        2. stop and remove a node
        3. Check that the data is accesible
        """
        cluster = self.cluster

        # Disable hinted handoff and set batch commit log so this doesn't
        # interfer with the test (this must be after the populate)
        cluster.set_configuration_options(
            values=self.default_config_options(), batch_commitlog=True)
        cluster.populate(2).start()
        node1, node2 = cluster.nodelist()

        session = self.patient_cql_connection(node1)
        create_ks(session, 'ks', 1)
        create_cf(session, 'cf', read_repair=0.0, columns={'c1': 'text', 'c2': 'text'})

        insert_c1c2(session, keys=range(10), consistency=ConsistencyLevel.ALL)

        node2_hostid = node2.hostid()
        node2.stop(wait_other_notice=True)
        try:
            query = SimpleStatement("SELECT * FROM cf", consistency_level=ConsistencyLevel.ONE)
            result = list(session.execute(query))
        except Unavailable:
            pass

        node1.nodetool("removenode %s" % node2_hostid)
        insert_c1c2(session, keys=range(10), consistency=ConsistencyLevel.ALL)
        query = SimpleStatement("SELECT * FROM cf", consistency_level=ConsistencyLevel.ONE)
        result = list(session.execute(query))
        assert len(result) == 10

    @staticmethod
    def _run_removenode_api(run_on_node: ScyllaNode, remove_node_hostid: str, ignore_nodes: list = None):
        api_cmd = f"http://{run_on_node.address()}:10000/storage_service/remove_node/?host_id={remove_node_hostid}"
        if ignore_nodes:
            ignore_nodes_ips = ','.join(node.address() for node in ignore_nodes)
            api_cmd += f"&ignore_nodes={ignore_nodes_ips}"

        logger.debug("Send restful api: " + api_cmd)
        r = requests.post(api_cmd)
        logger.debug(r.text)
        if not ignore_nodes:
            assert r.status_code != requests.codes.ok
        else:
            r.raise_for_status()
            logger.debug("Node2 is removed from the cluster")

    @staticmethod
    def _run_removenode_nodetool(run_on_node: ScyllaNode, remove_node_hostid: str, ignore_nodes: list = None):
        cmd = f"removenode %s {remove_node_hostid}"
        if ignore_nodes:
            ignore_nodes_ips = ','.join(node.address() for node in ignore_nodes)
            cmd = cmd % f"--ignore-dead-nodes {ignore_nodes_ips}"

        try:
            run_on_node.nodetool(cmd)
            logger.debug("Node2 is removed from the cluster")
        except NodetoolError as exc:
            if not ignore_nodes:
                if 'needed for removenode operation are down. It is highly recommended ' \
                   'to fix the down nodes and try again' not in exc.stdout:
                    raise
                logger.debug("Nodes={127.0.28.5} needed for removenode operation are down. "
                             "It is highly recommended to fix the down nodes and try again. "
                             "Run with best-effort mode (which might cause data inconsistency), "
                             "run nodetool removenode --ignore-dead-nodes <list_of_dead_nodes> <host_id>.")
            else:
                raise

    @pytest.mark.parametrize('removenode_method, ignore_two_nodes', [
                             ('api', False),
                             ('api', True),
                             ('nodetool', False),
                             ('nodetool', True),
                             ])
    def test_simple_removenode_3(self, removenode_method: str, ignore_two_nodes: bool):
        """
        :param removenode_method: "api" or "nodetool"
        """
        cluster = self.cluster

        # Disable hinted handoff and set batch commit log so this doesn't
        # interfer with the test (this must be after the populate)
        cluster.set_configuration_options(values={'hinted_handoff_enabled': False}, batch_commitlog=True)
        cluster.populate(5).start()
        node1, node2, node3, node4, node5 = cluster.nodelist()

        session = self.patient_cql_connection(node1)
        create_ks(session, 'ks', 3)
        create_cf(session, 'cf', read_repair=0.0, columns={'c1': 'text', 'c2': 'text'})

        insert_c1c2(session, keys=range(1000), consistency=ConsistencyLevel.ALL)

        node2_hostid = node2.hostid()
        node2.stop(wait_other_notice=True)
        query = SimpleStatement("SELECT * FROM cf", consistency_level=ConsistencyLevel.TWO)
        result = list(session.execute(query))
        assert len(result) == 1000, "should have 1000 items in table"

        node5.stop(wait_other_notice=True)
        ignore_nodes = [node5]
        if ignore_two_nodes:
            node4.stop(wait_other_notice=True)
            ignore_nodes.append(node4)

        # removenode should fail since node2 is down
        if removenode_method == 'api':
            self._run_removenode_api(run_on_node=node1, remove_node_hostid=node2_hostid)
        elif removenode_method == "nodetool":
            self._run_removenode_nodetool(run_on_node=node1, remove_node_hostid=node2_hostid)

        # removenode should succeed since we ignore the down node node2
        if removenode_method == 'api':
            self._run_removenode_api(run_on_node=node1, remove_node_hostid=node2_hostid, ignore_nodes=ignore_nodes)
        elif removenode_method == "nodetool":
            self._run_removenode_nodetool(run_on_node=node1, remove_node_hostid=node2_hostid, ignore_nodes=ignore_nodes)

    def _do_simple_removenode(self, kill_coordinator):
        """
        Test removenode while kill coordinator or peer node
        1. Create a cluster with 5 nodes with rf=3
        2. Stop node2 and removenode node2
        3. Kill node1 or node5 in the middle
        4. Check node2 is added as pending when removenode starts
        5. Check node2 is removed as pending when removenode aborts
        """
        cluster = self.cluster
        if kill_coordinator:
            logger.debug('Test kill removenode coordinator node in the middle')
        else:
            logger.debug('Test kill removenode peer node in the middle')

        # Disable hinted handoff and set batch commit log so this doesn't
        # interfer with the test (this must be after the populate)
        cluster.set_configuration_options(values={'hinted_handoff_enabled': False}, batch_commitlog=True)
        cluster.populate(5).start()
        node1, node2, node3, node4, node5 = cluster.nodelist()

        session = self.patient_cql_connection(node1)
        create_ks(session, 'ks', 3)
        create_cf(session, 'cf', read_repair=0.0, columns={'c1': 'text', 'c2': 'text'})

        insert_c1c2(session, keys=range(10000), consistency=ConsistencyLevel.ALL)

        node2_hostid = node2.hostid()
        node2.stop(wait_other_notice=True)

        def kill_node_thread(kill_coordinator, node1, node2, node3, node4, node5):
            logger.debug('kill node thread')

            node3.watch_log_for(f"Added node={node2.address()} as leaving node, coordinator={node1.address()}")
            node4.watch_log_for(f"Added node={node2.address()} as leaving node, coordinator={node1.address()}")
            node5.watch_log_for(f"Added node={node2.address()} as leaving node, coordinator={node1.address()}")

            logger.debug('Wait for node 5 to start to sync data')
            node5.watch_log_for(f"Started to sync data for removing node")

            if kill_coordinator:
                logger.debug('Stop node1 gently=False')
                node1.stop(gently=False)
            else:
                logger.debug('Stop node5 gently=False')
                node5.stop(gently=False)

        executor = ThreadPoolExecutor(max_workers=1)
        t = executor.submit(kill_node_thread, kill_coordinator, node1, node2, node3, node4, node5)

        api_cmd = f"http://{node1.address()}:10000/storage_service/remove_node/?host_id={node2_hostid}"
        logger.debug("Send restful api: " + api_cmd)
        try:
            r = requests.post(api_cmd)
            logger.debug(r.text)
            r.raise_for_status()
        except Exception as e:
            logger.debug(f"It is except to see the restful api to node1 to fail because node1 is killed: {e}")

        node3.watch_log_for(f"Removed node={node2.address()} as leaving node, coordinator={node1.address()}")
        node4.watch_log_for(f"Removed node={node2.address()} as leaving node, coordinator={node1.address()}")
        if kill_coordinator:
            node5.watch_log_for(f"Removed node={node2.address()} as leaving node, coordinator={node1.address()}")
        else:
            node1.watch_log_for(f"Removed node={node2.address()} as leaving node, coordinator={node1.address()}")

        t.result()

    def test_simple_removenode_4(self):
        self._do_simple_removenode(kill_coordinator=True)

    def test_simple_removenode_5(self):
        self._do_simple_removenode(kill_coordinator=False)

    def _add_new_node_while_add_new_table(self, when):
        """
        Test bootstrapped node get data in the new table
        1. Create a cluster with a three nodes with rf=1, insert data
        2. Add node, while node is bootstrapping add new table and insert data
        4. Check that node was connected and the cluster returns all data inserted
        """
        cluster = self.cluster
        rf = 1
        consistency = {1: ConsistencyLevel.ONE, 2: ConsistencyLevel.TWO}[rf]

        # Disable hinted handoff and set batch commit log so this doesn't
        # interfer with the test (this must be after the populate)
        cluster.set_configuration_options(
            values=self.default_config_options(), batch_commitlog=True)
        cluster.populate(3).start()
        node1 = cluster.nodelist()[0]

        session = self.patient_cql_connection(node1)
        create_ks(session, 'ks', rf)
        create_cf(session, 'cf', read_repair=0.0, columns={'c1': 'text', 'c2': 'text'})

        insert_c1c2(session, keys=range(4000), consistency=consistency)

        def run():
            create_ks(session, 'ks1', rf)
            create_cf(session, 'cf1', read_repair=0.0, columns={'c1': 'text', 'c2': 'text'})
            for i in range(0, 1000):
                insert = SimpleStatement("insert into ks1.cf1 (key,c1,c2) values ('%d','%d','%d')" % (i, i, i),
                                         consistency_level=consistency)
                session.execute(insert)

        executor = ThreadPoolExecutor(max_workers=1)

        # Create table and insert data before bootstrapping of the new node
        if when == "before":
            t = executor.submit(run)

        node4 = new_node(cluster)
        node4.start(jvm_args=['--logger-log-level', 'stream_session=debug'], no_wait=True)
        node4.watch_log_for("Starting to bootstrap")
        node4.watch_log_for("Beginning stream session|sync data for keyspace=ks, status=started")
        # Create table and insert data during bootstrapping of the new node
        if when == "during":
            t = executor.submit(run)

        node4.watch_log_for("Starting listening for CQL clients")
        session = self.patient_cql_connection(node4)

        # Create table and insert data after bootstrapping of the new node
        if when == "after":
            t = executor.submit(run)

        t.result()
        query = SimpleStatement("SELECT * FROM ks1.cf1", consistency_level=consistency)
        result = list(session.execute(query))
        assert len(result) == 1000, "should have 1000 items in table"

    def test_add_new_node_while_add_new_table_before_bootstrapping(self):
        self._add_new_node_while_add_new_table("before")

    @pytest.mark.next_gating
    def test_add_new_node_while_add_new_table_during_bootstrapping(self):
        self._add_new_node_while_add_new_table("during")

    def test_add_new_node_while_add_new_table_after_bootstrapping(self):
        self._add_new_node_while_add_new_table("after")

    def _get_gossipinfo(self, output):
        """
        Parse gossipinfo output and put it into a python dict.

        Trailing slash on node ips is removed.

        :param output: 'nodetool gossip' stdout
        :returns: Dict with nodetool info. Example follows.
        {'127.0.0.1': {'DC': 'datacenter1',
                       'HOST_ID': 'bb821819-9049-4929-b7cc-7b2edf1eec10',
                       'LOAD': '128982',
                       'NET_VERSION': '0',
                       'RACK': 'rack1',
                       'RELEASE_VERSION': '2.1.8',
                       'RPC_ADDRESS': '127.0.0.1',
                       'SCHEMA': '2576e940-0936-3ff6-a12c-9c4ed9571175',
                       'STATUS': 'NORMAL,996695790724469087',
                       'generation': '1457611493',
                       'heartbeat': '118'}}
        """
        gossipinfo = {}
        current_node = None
        for line in output.splitlines():
            line = line.strip()
            try:
                if current_node and current_node not in gossipinfo:
                    gossipinfo[current_node] = {}
                key, value = line.split(':')
                gossipinfo[current_node].update({key: value})
            except:
                current_node = line[1:]
        return gossipinfo

    def test_remove_node_from_gossip(self):
        """
        Test a node can be removed from gossip
        1. Create a cluster with 3 nodes
        2. Add node, wait for node to start bootstrappig
        3. Kill the new node
        4. Check that the new node will be removed from
        4. Check gossip on_remove callback in storage_service will not cause deadlock
        """
        cluster = self.cluster

        # Disable hinted handoff and set batch commit log so this doesn't
        # interfere with the test (this must be after the populate)
        cluster.set_configuration_options(
            values=self.default_config_options(), batch_commitlog=True)
        cluster.populate(3).start()
        node1, node2, node3 = cluster.nodelist()

        session = self.patient_cql_connection(node1)
        create_ks(session, 'ks', 2)
        create_cf(session, 'cf', read_repair=0.0, columns={'c1': 'text', 'c2': 'text'})

        insert_c1c2(session, keys=range(2000), consistency=ConsistencyLevel.TWO)

        logger.debug("Start node 4...")
        self.ignore_log_patterns += ['Startup failed']
        node4 = new_node(cluster)
        node4.start(jvm_args=['--logger-log-level', 'stream_session=debug'], no_wait=True)
        node4.watch_log_for("Starting to bootstrap")
        node4.watch_log_for("Beginning stream session|sync data for keyspace=ks, status=started")

        logger.debug("Hard-stop node 4 ...")
        node4.stop(gently=False)

        logger.debug("Check node 1 removed node4  ...")
        node1.watch_log_for("FatClient {} has been silent for .*ms, removing from gossip".format(node4.address()))

        logger.debug("Check the hearbeat of node 1 ...")
        status1, err1 = node1.nodetool('gossipinfo')
        gossipinfo_1 = self._get_gossipinfo(status1)
        heartbeat_1 = int(gossipinfo_1[cluster.get_node_ip(1)]['heartbeat'])

        time.sleep(3)

        logger.debug("Check the hearbeat of node 1 updated ...")
        status2, err2 = node1.nodetool('gossipinfo')
        gossipinfo_2 = self._get_gossipinfo(status2)
        heartbeat_2 = int(gossipinfo_2[cluster.get_node_ip(1)]['heartbeat'])
        e_msg = ("Heartbeat for status 2 '%s' is not greater than for status 1 '%s', something is wrong" %
                 (heartbeat_2, heartbeat_1))
        logger.debug("heartbeat_2 = %d, heartbeat_1 = %d" % (heartbeat_2, heartbeat_1))
        assert heartbeat_2 > heartbeat_1, e_msg

    def test_add_node_when_cluster_is_filled(self):
        cluster = self.cluster
        config_options = self.default_config_options(
            hinted_handoff_enabled=None)
        cluster.set_configuration_options(values=config_options)
        cluster.populate(3).start(wait_for_binary_proto=True)
        node1 = cluster.nodelist()[0]
        logger.debug("Cluster is up, start stressing...")

        num_keys = 1000000 if not hasattr(
            cluster, 'scylla_mode') or cluster.scylla_mode != 'debug' else 10000
        num_threads = 700 if not hasattr(
            cluster, 'scylla_mode') or cluster.scylla_mode != 'debug' else 70
        node1.stress(
            ['write', 'cl=QUORUM', f"n={num_keys}", 'no-warmup', f"-rate threads={num_threads}"])

        logger.debug("Adding new node...")
        node4 = new_node(cluster)
        node4.start(wait_for_binary_proto=True)
        logger.debug("New node added...")

    def test_add_node_with_large_partition1(self):
        """
        Test bootstrapped node streams all data
        1. Create a cluster with a single node with rf=2, insert data with large partition
        2. Add a new node
        3. Check that each node has all the data
        """
        cluster = self.cluster

        # Disable hinted handoff and set batch commit log so this doesn't
        # interfer with the test (this must be after the populate)
        cluster.set_configuration_options(
            values=self.default_config_options(), batch_commitlog=True)
        cluster.populate(1).start()
        node1 = cluster.nodelist()[0]

        logger.debug("Node 1 started")
        session = self.patient_cql_connection(node1)
        create_ks(session, 'ks', 2)
        create_cf(session, 'cf', read_repair=0.0, columns={'c1': 'text', 'c2': 'text'})

        nr_rows = 100
        c1 = 'a' * 1024 * 100  # 100KB
        c2 = 'b' * 1024 * 300  # 300KB
        c1s = [c1] * nr_rows
        c2s = [c2] * nr_rows
        logger.debug("Insert data")
        insert_c1c2(session, keys=range(nr_rows), consistency=ConsistencyLevel.ONE, c1_values=c1s,
                    c2_values=c2s)

        node2 = new_node(cluster)
        node2.start(wait_for_binary_proto=True)
        logger.debug("Node 2 started")

        logger.debug("Check rows on node2")
        self.check_rows_on_node(node2, nr_rows)
        logger.debug("Check rows on node1")
        self.check_rows_on_node(node1, nr_rows)

    def test_add_node_with_large_partition2(self):
        """
        Test bootstrapped node streams all data
        1. Create a cluster with a single node with rf=2, insert data with large partition
        2. Add a new node
        3. Check that each node has all the data
        """
        nr_columns = 250
        nr_rows = 100
        column_size = 1 * 1024  # 1KB

        cluster = self.cluster

        # Disable hinted handoff and set batch commit log so this doesn't
        # interfer with the test (this must be after the populate)
        cluster.set_configuration_options(
            values=self.default_config_options(), batch_commitlog=True)
        cluster.populate(1).start()
        node1 = cluster.nodelist()[0]

        logger.debug("Node 1 started")
        session = self.patient_cql_connection(node1)

        # create ks
        create_ks(session, 'ks', 2)

        # Create cf
        columns = collections.OrderedDict()
        for i in range(1, nr_columns + 1):
            columns['c%s' % i] = 'text'
        create_cf(session, 'cf', read_repair=0.0, columns=columns)

        # Insert data
        logger.debug("Insert data")
        insert_c1cn(session, keys=range(nr_rows), consistency=ConsistencyLevel.ONE, nr_columns=nr_columns,
                    column_size=column_size)

        node2 = new_node(cluster)
        node2.start(wait_for_binary_proto=True)
        logger.debug("Node 2 started")

        logger.debug("Check rows on node2")
        self.check_rows_on_node(node2, nr_rows)
        logger.debug("Check rows on node1")
        self.check_rows_on_node(node1, nr_rows)

    def test_add_node_with_large_partition3(self):
        """
        Test bootstrapped node streams all data
        1. Create a cluster with a single node with rf=2, insert data with mixed large partition and small partion
        2. Add a new node
        3. Check that each node has all the data
        """
        cluster = self.cluster

        # Disable hinted handoff and set batch commit log so this doesn't
        # interfer with the test (this must be after the populate)
        cluster.set_configuration_options(
            values=self.default_config_options(), batch_commitlog=True)
        cluster.populate(1).start()
        node1 = cluster.nodelist()[0]

        logger.debug("Node 1 started")
        session = self.patient_cql_connection(node1)
        create_ks(session, 'ks', 2)
        create_cf(session, 'cf', read_repair=0.0, columns={'c1': 'text', 'c2': 'text'})

        nr_rows = 100
        v1 = 'a' * 1024 * 10  # 10KB
        v2 = 'b' * 1024 * 300  # 300KB
        v3 = 'c' * 1024 * 1   # 1KB
        v4 = 'd' * 1024 * 3   # 3KB
        c1s = []
        c2s = []
        for n in range(nr_rows):
            if n % 2 == 0:
                c1s.append(v1)
                c2s.append(v2)
            else:
                c1s.append(v3)
                c2s.append(v4)
        logger.debug("Insert data")
        insert_c1c2(session, keys=range(nr_rows), consistency=ConsistencyLevel.ONE, c1_values=c1s,
                    c2_values=c2s)

        node2 = new_node(cluster)
        node2.start(wait_for_binary_proto=True)
        logger.debug("Node 2 started")

        logger.debug("Check rows on node2")
        self.check_rows_on_node(node2, nr_rows)
        logger.debug("Check rows on node1")
        self.check_rows_on_node(node1, nr_rows)

    def test_add_node_with_large_partition4(self):
        """
        Test bootstrapped node streams all data
        1. Create a cluster with a single node with rf=2, insert data with large partitions
        2. Add a new node
        3. Check that each node has all the data
        """
        timeout = self.cql_timeout(300)
        values = {
            'range_request_timeout_in_ms': timeout * 1000,
        }
        logger.debug(f"Setting cluster configuration options: {values}")
        cluster = self.cluster
        cluster.set_configuration_options(values=values)

        nr_partitions = 100  # 100 fails 10 works
        if hasattr(self.cluster, 'scylla_mode') and self.cluster.scylla_mode == 'debug':
            nr_partitions //= 10
        # In cassandra-stress-custom-large-partition-1.yaml
        # name: key2
        # cluster: uniform(3000..3000)
        # each partition has 3000 cql rows, so there will be nr_partitions * 3000 cql rows
        nr_rows = nr_partitions * 3000

        # Disable hinted handoff and set batch commit log so this doesn't
        # interfer with the test (this must be after the populate)
        cluster.set_configuration_options(
            values=self.default_config_options(), batch_commitlog=True)
        cluster.populate(1).start()
        node1 = cluster.nodelist()[0]

        logger.debug("Node 1 started")
        c_s_profile = os.path.join("test_data", "c-s-profiles", "cassandra-stress-custom-large-partition-1.yaml")
        c_s_profile = os.path.abspath(c_s_profile)
        logger.debug("Inject data with cassandra-stress starts")
        logger.debug(c_s_profile)
        node1.stress(['user', 'n=%s' % nr_partitions, 'cl=ONE', 'profile=%s' % c_s_profile, 'ops(insert=1)',
                      '-rate threads=1'])
        logger.debug("Inject data with cassandra-stress completes")

        node2 = new_node(cluster)
        node2.start(wait_for_binary_proto=True)
        logger.debug("Node 2 started")

        logger.debug("Check rows on node2")
        self.check_rows_on_node(node2, nr_rows, ks='keyspace1', cf='standard1', counter_column='cn', timeout=timeout)
        logger.debug("Check rows on node1")
        self.check_rows_on_node(node1, nr_rows, ks='keyspace1', cf='standard1', counter_column='cn', timeout=timeout)

    def test_increment_decrement_counters_in_threads_nodes_restarted(self):
        """
        increment/decrement 2 counters(2 inc vs 1 dec) * 1000 times * 120 threads
        1. Create a cluster with 3 nodes with rf=3
        2. Start increment/decrement counters CL=QUORUM
        3. Stop one node and wait 10 seconds
        4. Start the node and wait 10 seconds
        5. Stop another 2 nodes
        6. Wait when all counter ops complete
        7. Start 2 nodes
        8. Verify counters consistency
        """
        cluster = self.cluster

        config_options = self.default_config_options(hinted_handoff_enabled=None)
        cluster.set_configuration_options(values=config_options)
        cluster.populate(3).start()
        nodes = cluster.nodelist()

        session = self.patient_cql_connection(nodes[0])
        create_ks(session, 'ks', 3)
        create_cf(session, 'cf', validation="CounterColumnType", columns={'c': 'counter'})

        nb_increment = 500
        nb_counter = 2

        def run(connection, decrement):
            _return = dict.fromkeys([i for i in range(nb_counter)], 0)
            for i in range(0, nb_increment):
                for c in range(0, nb_counter):
                    if decrement:
                        query = SimpleStatement("UPDATE cf SET c = c - 1 WHERE key = 'counter%i'" % c,
                                                consistency_level=ConsistencyLevel.ONE)
                    else:
                        query = SimpleStatement("UPDATE cf SET c = c + 1 WHERE key = 'counter%i'" % c,
                                                consistency_level=ConsistencyLevel.ONE)
                    connection.execute(query)
                    if decrement:
                        _return[c] -= 1
                    else:
                        _return[c] += 1
                    time.sleep(0.01)
            return _return

        result = dict.fromkeys([i for i in range(nb_counter)], 0)

        num_threads = 120

        executor = ThreadPoolExecutor(max_workers=num_threads)

        # stop and restart one node for a while
        nodes[2].stop()

        threads = []
        for x in range(num_threads):
            conn = self.patient_cql_connection(nodes[x % (len(nodes) - 1)], 'ks')
            decrement = (x % len(nodes)) == 0
            threads.append(executor.submit(run, conn, decrement))

        for t in threads:
            t_result = t.result()
            result = {k: result.get(k, 0) + t_result.get(k, 0) for k in set(result)}

        nodes[2].start(wait_other_notice=True, wait_for_binary_proto=True)

        nodes[0].stop()
        nodes[1].stop()

        threads = []
        for x in range(num_threads):
            conn = self.patient_cql_connection(nodes[2], 'ks')
            decrement = (x % len(nodes)) == 0
            threads.append(executor.submit(run, conn, decrement))

        for t in threads:
            t_result = t.result()
            result = {k: result.get(k, 0) + t_result.get(k, 0) for k in set(result)}

        nodes[0].start(wait_other_notice=True, wait_for_binary_proto=True)
        nodes[1].start(wait_other_notice=True, wait_for_binary_proto=True)

        sessions = [self.patient_cql_connection(node, 'ks') for node in nodes]

        keys = ",".join(["'counter%i'" % c for c in range(0, nb_counter)])
        query = SimpleStatement("SELECT key, c FROM cf WHERE key IN (%s)" % keys,
                                consistency_level=ConsistencyLevel.ALL)
        res = list(sessions[0].execute(query))
        assert res == list(sessions[1].execute(query)),\
            "different counter values in node0 and node1"
        assert res == list(sessions[2].execute(query)),\
            "different counter values in node0 and node2"

        for c in range(0, nb_counter):
            assert result[c] == res[c][1], "Expecting counter%i = %i, got %i" % (
                c, result[c], res[c][1])

    def test_verify_latest_copy_add_node(self):
        """
        Test bootstrapped node streams latest copy
        1. Create a cluster with a single node with rf=3
        2. Add a new node
        3. Check that new node has all the latest data
        """
        cluster = self.cluster

        config_options = self.default_config_options()
        config_options.update(self.repair_based_node_ops_config_options(True))
        cluster.set_configuration_options(
            values=config_options, batch_commitlog=True)
        cluster.populate(2).start()
        node1, node2 = cluster.nodelist()

        # Insert on node1 and node2
        session = self.patient_cql_connection(node1)
        create_ks(session, 'ks', 3)
        create_cf(session, 'cf', read_repair=0.0, columns={'c1': 'text', 'c2': 'text'})
        cs = ['0'] * 1000
        logger.debug("Insert data on node 1 and node 2")
        insert_c1c2(session, keys=range(
            0, 1000), consistency=ConsistencyLevel.TWO, c1_values=cs, c2_values=cs)

        # Insert on node1 only
        node2.stop()
        session = self.patient_cql_connection(node1)
        cs = ['1'] * 500
        logger.debug("Insert data on node 1")
        insert_c1c2(session, keys=range(
            0, 500), consistency=ConsistencyLevel.ONE, c1_values=cs, c2_values=cs)

        # Insert on node2 only
        node2.start(wait_for_binary_proto=True)
        node1.stop()
        session = self.patient_cql_connection(node2)
        cs = ['2'] * 500
        logger.debug("Insert data on node 2")
        insert_c1c2(session, keys=range(500, 1000),
                    consistency=ConsistencyLevel.ONE, c1_values=cs, c2_values=cs)
        node1.start(wait_for_binary_proto=True)

        # Bootstrap a new node
        node3 = new_node(cluster)
        node3.start(wait_for_binary_proto=True)
        session = self.patient_cql_connection(node3)
        session.execute("use ks;")
        logger.debug("Node 3 started")

        # Shtudown node1 and node2
        node1.stop()
        node2.stop()

        logger.debug("Check rows on node 3 have latest copy")
        cs = ['1'] * 500
        query_c1c2_concurrent(session, keys=range(
            0, 500), consistency=ConsistencyLevel.ONE, c1_values=cs, c2_values=cs)
        cs = ['2'] * 500
        query_c1c2_concurrent(session, keys=range(
            500, 1000), consistency=ConsistencyLevel.ONE, c1_values=cs, c2_values=cs)

    def test_verify_latest_copy_replace_node(self):
        cluster = self.cluster
        config_options = self.repair_based_node_ops_config_options(True)
        cluster.set_configuration_options(
            values=config_options, batch_commitlog=True)
        logger.debug("Starting cluster with 3 nodes.")
        cluster.populate(3).start(wait_for_binary_proto=True)
        node1, node2, node3 = cluster.nodelist()

        # Insert on node1, node2 and node3
        session = self.patient_cql_connection(node1)
        create_ks(session, 'ks', 3)
        create_cf(session, 'cf', read_repair=0.0, columns={'c1': 'text', 'c2': 'text'})
        cs = ['0'] * 1000
        logger.debug("Insert data on node 1 and node 2")
        insert_c1c2(session, keys=range(
            0, 1000), consistency=ConsistencyLevel.THREE, c1_values=cs, c2_values=cs)

        # Stop node 3
        node3.stop()

        # Insert on node1 only
        node2.stop()
        session = self.patient_cql_connection(node1)
        cs = ['1'] * 500
        logger.debug("Insert data on node 1")
        insert_c1c2(session, keys=range(
            0, 500), consistency=ConsistencyLevel.ONE, c1_values=cs, c2_values=cs)

        # Insert on node2 only
        node2.start(wait_for_binary_proto=True)
        node1.stop()
        session = self.patient_cql_connection(node2)
        cs = ['2'] * 500
        logger.debug("Insert data on node 2")
        insert_c1c2(session, keys=range(500, 1000),
                    consistency=ConsistencyLevel.ONE, c1_values=cs, c2_values=cs)
        node1.start(wait_for_binary_proto=True)

        # Replacing node3 with node4
        logger.debug("Starting node 4 to replace node 3")
        node4 = new_node(cluster, bootstrap=True, token=None, remote_debug_port='0', data_center=None)
        node4.start(wait_for_binary_proto=True, replace_address=self.cluster.get_node_ip(3))
        session = self.patient_cql_connection(node4)
        session.execute("use ks;")
        logger.debug("Node 4 finished replacing node 3")

        # Shtudown node1 and node2
        node1.stop()
        node2.stop()

        logger.debug("Check rows on node 4 have latest copy")
        cs = ['1'] * 500
        query_c1c2_concurrent(session, keys=range(
            0, 500), consistency=ConsistencyLevel.ONE, c1_values=cs, c2_values=cs)
        cs = ['2'] * 500
        query_c1c2_concurrent(session, keys=range(
            500, 1000), consistency=ConsistencyLevel.ONE, c1_values=cs, c2_values=cs)

    def test_verify_latest_copy_rebuild_node(self):
        cluster = self.cluster
        config_options = self.repair_based_node_ops_config_options(True)
        cluster.set_configuration_options(
            values=config_options, batch_commitlog=True)
        logger.debug("Starting cluster with 3 nodes.")
        cluster.populate(3).start(wait_for_binary_proto=True)
        node1, node2, node3 = cluster.nodelist()

        # Insert on node1, node2 and node3
        session = self.patient_cql_connection(node1)
        create_ks(session, 'ks', 3)
        create_cf(session, 'cf', read_repair=0.0, columns={'c1': 'text', 'c2': 'text'})
        cs = ['0'] * 1000
        logger.debug("Insert data on node 1 and node 2")
        insert_c1c2(session, keys=range(
            0, 1000), consistency=ConsistencyLevel.THREE, c1_values=cs, c2_values=cs)

        # Stop node 3
        node3.stop()

        # Insert on node1 only
        node2.stop()
        session = self.patient_cql_connection(node1)
        cs = ['1'] * 500
        logger.debug("Insert data on node 1")
        insert_c1c2(session, keys=range(
            0, 500), consistency=ConsistencyLevel.ONE, c1_values=cs, c2_values=cs)

        # Insert on node2 only
        node2.start(wait_for_binary_proto=True)
        node1.stop()
        session = self.patient_cql_connection(node2)
        cs = ['2'] * 500
        logger.debug("Insert data on node 2")
        insert_c1c2(session, keys=range(500, 1000),
                    consistency=ConsistencyLevel.ONE, c1_values=cs, c2_values=cs)
        node1.start(wait_for_binary_proto=True)

        # Replacing node3 with node4
        logger.debug("Starting node 3")
        node3.start(wait_for_binary_proto=True)
        node3.nodetool('rebuild')
        logger.debug("Node 3 finished rebuild")
        session = self.patient_cql_connection(node3)
        session.execute("use ks;")

        # Shtudown node1 and node2
        node1.stop()
        node2.stop()

        logger.debug("Check rows on node 3 have latest copy")
        cs = ['1'] * 500
        query_c1c2_concurrent(session, keys=range(
            0, 500), consistency=ConsistencyLevel.ONE, c1_values=cs, c2_values=cs)
        cs = ['2'] * 500
        query_c1c2_concurrent(session, keys=range(
            500, 1000), consistency=ConsistencyLevel.ONE, c1_values=cs, c2_values=cs)

    def test_verify_latest_copy_decommission_node(self):
        cluster = self.cluster
        config_options = self.repair_based_node_ops_config_options(True)
        cluster.set_configuration_options(
            values=config_options, batch_commitlog=True)
        logger.debug("Starting cluster with 3 nodes.")
        cluster.populate(3).start(wait_for_binary_proto=True)
        node1, node2, node3 = cluster.nodelist()

        # Insert on node1, node2 and node3
        session = self.patient_cql_connection(node1)
        create_ks(session, 'ks', 3)
        create_cf(session, 'cf', read_repair=0.0, columns={'c1': 'text', 'c2': 'text'})
        cs = ['0'] * 1000
        logger.debug("Insert data on node 1 and node 2")
        insert_c1c2(session, keys=range(
            0, 1000), consistency=ConsistencyLevel.THREE, c1_values=cs, c2_values=cs)

        # Stop node 3
        node3.stop()

        # Insert on node1 only
        node2.stop()
        session = self.patient_cql_connection(node1)
        cs = ['1'] * 500
        logger.debug("Insert data on node 1")
        insert_c1c2(session, keys=range(
            0, 500), consistency=ConsistencyLevel.ONE, c1_values=cs, c2_values=cs)

        # Insert on node2 only
        node2.start(wait_for_binary_proto=True)
        node1.stop()
        session = self.patient_cql_connection(node2)
        cs = ['2'] * 500
        logger.debug("Insert data on node 2")
        insert_c1c2(session, keys=range(500, 1000),
                    consistency=ConsistencyLevel.ONE, c1_values=cs, c2_values=cs)
        node1.start(wait_for_binary_proto=True)

        # Decommission node 1
        logger.debug("Starting node 3")
        node3.start(wait_for_binary_proto=True)
        node1.nodetool('decommission')
        logger.debug("Node 1 finished decommission")
        session = self.patient_cql_connection(node3)
        session.execute("use ks;")

        logger.debug("Check rows on node 2 and 3 have latest copy")
        cs = ['1'] * 500
        query_c1c2_concurrent(session, keys=range(
            0, 500), consistency=ConsistencyLevel.TWO, c1_values=cs, c2_values=cs)
        cs = ['2'] * 500
        query_c1c2_concurrent(session, keys=range(
            500, 1000), consistency=ConsistencyLevel.TWO, c1_values=cs, c2_values=cs)

    def test_verify_latest_copy_removenode_node(self):
        cluster = self.cluster
        config_options = self.repair_based_node_ops_config_options(True)
        cluster.set_configuration_options(
            values=config_options, batch_commitlog=True)
        logger.debug("Starting cluster with 4 nodes.")
        cluster.populate(4).start(wait_for_binary_proto=True)
        node1, node2, node3, node4 = cluster.nodelist()

        # Insert on node1, node2, node3, node4
        session = self.patient_cql_connection(node1)
        create_ks(session, 'ks', 3)
        create_cf(session, 'cf', read_repair=0.0, columns={'c1': 'text', 'c2': 'text'})
        cs = ['0'] * 1000
        logger.debug("Insert data on node 1, node 2, node 3 and node 4")
        insert_c1c2(session, keys=range(
            0, 1000), consistency=ConsistencyLevel.THREE, c1_values=cs, c2_values=cs)

        # Insert on node1, node2 and node3
        node4.stop()
        session = self.patient_cql_connection(node1)
        cs = ['1'] * 250
        logger.debug("Insert data on node 1, node 2 and node 3")
        insert_c1c2(session, keys=range(
            0, 250), consistency=ConsistencyLevel.TWO, c1_values=cs, c2_values=cs)

        # Insert on node1, node2 and node4
        node4.start(wait_for_binary_proto=True)
        node3.stop()
        session = self.patient_cql_connection(node4)
        cs = ['2'] * 250
        logger.debug("Insert data on node 1, node 2 and node 4")
        insert_c1c2(session, keys=range(
            250, 500), consistency=ConsistencyLevel.TWO, c1_values=cs, c2_values=cs)

        # Insert on node1, node3 and node4
        node3.start(wait_for_binary_proto=True)
        node2.stop()
        session = self.patient_cql_connection(node3)
        cs = ['3'] * 250
        logger.debug("Insert data on node 1, node 3 and node 4")
        insert_c1c2(session, keys=range(
            500, 750), consistency=ConsistencyLevel.TWO, c1_values=cs, c2_values=cs)

        # Insert on node2, node3 and node4
        node2.start(wait_for_binary_proto=True)
        node1.stop()
        session = self.patient_cql_connection(node2)
        cs = ['4'] * 250
        logger.debug("Insert data on node 2, node 3 and node 4")
        insert_c1c2(session, keys=range(750, 1000),
                    consistency=ConsistencyLevel.TWO, c1_values=cs, c2_values=cs)

        hostid = node2.hostid()
        node2.stop()
        node1.start(wait_for_binary_proto=True)
        node1.nodetool("removenode %s" % hostid)
        logger.debug("Node 1 finished removenode node 2")
        session = self.patient_cql_connection(node1)
        session.execute("use ks;")

        # Shtudown node4
        node4.stop()

        logger.debug("Check rows on node 1 and node 3 have latest copy")
        cs = ['1'] * 250
        query_c1c2_concurrent(session, keys=range(
            0, 250), consistency=ConsistencyLevel.TWO, c1_values=cs, c2_values=cs)
        cs = ['2'] * 250
        query_c1c2_concurrent(session, keys=range(
            250, 500), consistency=ConsistencyLevel.TWO, c1_values=cs, c2_values=cs)
        cs = ['3'] * 250
        query_c1c2_concurrent(session, keys=range(
            500, 750), consistency=ConsistencyLevel.TWO, c1_values=cs, c2_values=cs)
        cs = ['4'] * 250
        query_c1c2_concurrent(session, keys=range(
            750, 1000), consistency=ConsistencyLevel.TWO, c1_values=cs, c2_values=cs)


@pytest.mark.dtest_full
class TestStopNodeEarly(Tester):

    def _test_stop_node_while_restarting(self, gently, wait_other_notice, num_nodes=2):
        """
        Test that other nodes handle node stopping early during initialization
        """
        cluster = self.cluster
        cluster.populate(num_nodes).start()
        node1 = cluster.nodelist()[0]

        session = self.patient_cql_connection(cluster.nodelist()[1])
        create_ks(session, 'ks', rf=min(num_nodes, 3))
        create_cf(session, 'cf', read_repair=0.0, columns={'c1': 'text', 'c2': 'text'})

        insert_c1c2(session, keys=range(1000), consistency=ConsistencyLevel.ALL)

        logger.debug(f"Restarting node1")
        node1.stop(wait_other_notice=True)
        mark1 = node1.mark_log()
        other_marks = []
        for node in cluster.nodelist()[1:]:
            other_marks.append((node, node.mark_log()))
        node1.start(no_wait=True)
        if wait_other_notice:
            # we need other nodes to first notice the stopping node to be UP
            # before we actually stop the node, if we want other nodes to notice
            # the node to be DOWN during stop procedure.
            msg = f"InetAddress {node1.address()} is now UP"
            logger.debug(f"Waiting for '{msg}'")
            for node, mark in other_marks:
                node.watch_log_for(msg, from_mark=mark)
        else:
            # view update starting is an arbitrary point
            # in the startup sequence known to be early enough,
            # before gossipping starts and others notice the
            # starting node as UP.
            msg = "starting view update generator"
            logger.debug(f"Waiting for '{msg}'")
            node1.watch_log_for(msg, from_mark=mark1)
        logger.debug(f"Stopping node1 early: gently={gently} wait_other_notice={wait_other_notice}")
        node1.stop(gently=gently, wait_other_notice=wait_other_notice)

        logger.debug(f"Verifying data")
        result = list(session.execute("SELECT * FROM cf"))
        assert len(result) == 1000

    def test_stop_node_while_restarting(self):
        self._test_stop_node_while_restarting(gently=True, wait_other_notice=False)

    def test_stop_node_while_restarting_wait_other_notice(self):
        self._test_stop_node_while_restarting(gently=True, wait_other_notice=True)

    def test_kill_node_while_restarting(self):
        self._test_stop_node_while_restarting(gently=False, wait_other_notice=False)

    def test_kill_node_while_restarting_wait_other_notice(self):
        self._test_stop_node_while_restarting(gently=False, wait_other_notice=True)


@pytest.mark.dtest_full
@pytest.mark.dtest_long
@pytest.mark.dtest_heavy
class TestLargeScaleCluster(Tester):

    def add_multi_nodes(self, starting_size=3, node_count=10, rf=1, timeout=120):
        """
        1. Create a cluster with 3 nodes and rf=1, insert data
        2. In a loop add new nodes
        3. Check that all data exists
        """
        cluster = self.cluster

        node1 = cluster.nodelist()[0]

        session = self.patient_cql_connection(node1)
        create_ks(session, 'ks', rf)
        create_cf(session, 'cf', read_repair=0.0, columns={'c1': 'text', 'c2': 'text'})

        consistency = ConsistencyLevel.ALL
        logger.debug("just before first insert")
        insert_c1c2(session, keys=range(starting_size * 100 + 1000), consistency=ConsistencyLevel.ONE)

        query = SimpleStatement("SELECT key FROM ks.cf", fetch_size=100, consistency_level=consistency)
        for i in range(starting_size + 1, node_count + 1):
            node_i = new_node(cluster)
            node_i.start(wait_for_binary_proto=True, wait_other_notice=True)
            session_i = self.patient_exclusive_cql_connection(node_i)
            session_i.execute("use ks;")
            insert_c1c2(session_i, keys=range(100000 + i * 2000, 100000 + i * 2000 + 100), consistency=consistency)
            logger.debug("added %s" % node_i.name)

            result = list(session.execute(query, timeout=timeout))
            assert len(result) == i * 100 + 1000, "data loss after increasing size to %d expecting %d rows %d" % \
                (len(cluster.nodelist()), i * 100 + 1000, len(result))

    @pytest.mark.timeout(4200)
    def test_add_50_nodes(self):
        """
        Test large scale cluster (50 nodes cluster).
        Cluster starts with a starting_size=3 and grow to node_count=50 during a c-s write in the background (low load)
        and c-s read after adding all nodes to make sure all data was written successfully.
        In addition, while adding each node inserting 100 keys and verifying that all keys were written.
        E.Result: All nodes (50) were added and c-s read successfully read all keys (n=300,000).
        """
        starting_size = 3
        cluster = self.cluster

        timeout = self.cql_timeout(120)

        # Disable hinted handoff and set batch commit log so this doesn't
        # interfere with the test (this must be after the populate)
        config_options = {
            'hinted_handoff_enabled': False,
            'enable_sstable_key_validation': True,
            'range_request_timeout_in_ms': timeout * 1000,
        }
        cluster.set_configuration_options(
            values=config_options, batch_commitlog=True)
        cluster.populate(starting_size).start()
        node2 = cluster.nodelist()[1]

        n = '300000' if not hasattr(
            cluster, 'scylla_mode') or cluster.scylla_mode != 'debug' else 30000

        def run():
            node2.stress(['write', 'cl=QUORUM',  'n=%s' % n, 'no-warmup',
                          '-pop seq=1..%s' % n, '-rate threads=2 limit=100/s'])

        executor = ThreadPoolExecutor(max_workers=1)
        t = executor.submit(run)

        self.add_multi_nodes(starting_size, node_count=50, rf=1, timeout=timeout)
        t.result()

        node2.stress(['read', 'cl=ONE', 'n=%s' % n, 'no-warmup', '-pop seq=1..%s' % n, '-rate threads=20'])
