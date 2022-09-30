"""Tests for limiting streaming/repair/compaction and other throughput limits"""
import signal

import logging
from time import time

import pytest
from cassandra.protocol import ConfigurationException
from ccmlib.scylla_node import ScyllaNode

from dtest_class import Tester
from tools.cluster import new_node
from tools.metrics import get_node_metrics

logger = logging.getLogger(__name__)


class ThroughputLimitTester(Tester):
    def prepare(self, nodes, wait_for_binary_proto=True,
                jvm_args=None, configuration_options=None) -> ScyllaNode:
        self.cluster.set_configuration_options(values=configuration_options)
        self.cluster.populate(nodes).start(wait_for_binary_proto=wait_for_binary_proto, jvm_args=jvm_args)
        return self.cluster.nodelist()[0]


@pytest.mark.dtest_full
class TestStreamingLimitThroughput(ThroughputLimitTester):
    """Testing https://github.com/scylladb/scylladb/commit/1f21c1ecc8e90d2bced38c019b1de0b3f651ddf1"""

    def test_limit_streaming_throughput(self):
        """Verifies streaming throughput can be limited using configuration option."""
        # Create cluster and populate data for streaming (with setting stream throughput limit)
        node1 = self.prepare(1, configuration_options={'stream_io_throughput_mb_per_sec': 1})
        node1.stress(['write', 'n=3000', 'no-warmup', '-schema', 'replication(factor=1)', '-rate', 'threads=1',
                      "-col", "n=FIXED(1)", "size=FIXED(16384)", "-errors", "ignore"])

        # add node to start streaming
        node2 = new_node(self.cluster)
        node2.start(wait_other_notice=True, wait_for_binary_proto=True)
        node2.watch_log_for("range_streamer .+ for keyspace=keyspace1 succeeded")
        assert node2.grep_log(
            "stream_session - Set streaming bandwidth to 1MB/s"), "Failed to set up streaming bandwidth limit"

        # assert throughput is near the limit (based on data in logs)
        streaming_succeeded_lines = node2.grep_log(r"Streaming plan for Bootstrap-keyspace1.+rx=.+, ([\d.]+)")
        assert streaming_succeeded_lines, "Failed to stream keyspace1"
        for line, match in streaming_succeeded_lines[-3:]:
            # verify only last 3/10 streaming indexes as first ones are not yet limited properly - possibly due too little data.
            # also asserting 2.0MB instead 1MB due limited precision of throughput limiter
            assert float(match.group(1)) < 2000, "Failed to limit bandwidth for streaming keyspace1"

    def test_limit_rbno_bootstrap_throughput(self):
        """Verifies streaming throughput can be limited using configuration option for RBNO."""
        # Create cluster and populate data for streaming (with setting stream throughput limit)
        node1 = self.prepare(1, configuration_options={'stream_io_throughput_mb_per_sec': 1, 'enable_repair_based_node_ops': "true",
                                                       "allowed_repair_based_node_ops": "bootstrap"})
        node1.stress(['write', 'n=3000', 'no-warmup', '-schema', 'replication(factor=1)', '-rate', 'threads=1',
                      "-col", "n=FIXED(1)", "size=FIXED(16384)", "-errors", "ignore"])

        # add node to start streaming
        node2 = new_node(self.cluster)
        node2.start(wait_other_notice=True, wait_for_binary_proto=True)
        node2.watch_log_for("repair - bootstrap_with_repair: finished with keyspace=keyspace1")
        assert node2.grep_log(
            "stream_session - Set streaming bandwidth to 1MB/s"), "Failed to set up streaming bandwidth limit"

        # assert throughput is near the limit
        rbno_succeeded_lines = node2.grep_log(
            r"repair - repair.* repair_reason=bootstrap, keyspace=keyspace1.+ row_from_disk_bytes_per_sec=.+, ([\d.]+)},.+MiB"
        )
        assert rbno_succeeded_lines, "Failed to repair based bootstrap keyspace1"
        for line, match in rbno_succeeded_lines:
            # asserting 2MB instead 1MB due limited precision of throughput limiter - usually around 1.5MB/s
            assert float(match.group(1)) < 2, "Failed to limit bandwidth for repair based bootstrap keyspace1"


@pytest.mark.dtest_full
class TestCompactionLimitThroughput(ThroughputLimitTester):
    """Verifies compaction throughput limit for automatic compactions.

    Testing https://github.com/scylladb/scylladb/commit/bfc521ee9c2ba16fc4a15e68c21f68300658f530
    """
    @pytest.mark.single_node
    def test_can_limit_compaction_throughput(self):
        node1 = self.prepare(1)
        # set compaction_throughput_mb_per_sec setting in runtime
        node1.set_configuration_options(values={'compaction_throughput_mb_per_sec': 5})
        # send SIGHUP to reread configuration
        node1.kill(signal.SIGHUP)
        node1.watch_log_for(r"Set compaction bandwidth to 5MB/s", timeout=180)

        # populate data to have something to compact
        mark = node1.mark_log()
        node1.stress(['write', 'n=200000', 'no-warmup', '-schema', 'replication(factor=1)', '-rate', 'threads=3',
                      '-col', 'size=FIXED(64)'])
        node1.flush()

        # wait for compaction to finish and validate throughput
        node1.watch_log_for(r"Compact keyspace1\.standard1 .+ Compacted", from_mark=mark, timeout=240)
        compaction_lines = node1.grep_log(
            r"Compact keyspace1\.standard1 .+ Compacted [\d]+ sstables to .+ in .+= ([\d]+)MB"
        )
        assert compaction_lines, "Failed to find compaction finished lines in logs"
        for line, match in compaction_lines[-1:]:
            # asserting 4MB instead 3MB due limited precision of throughput limiter
            assert float(match.group(1)) <= 6, \
                f"Failed to limit compaction bandwidth ({match.group(1)}MB/s<=6)"


class TestPerPartitionRateLimiter(Tester):
    """Tests for per-partition rate limiter that limits read/write ops/s for given partition.

    Feature introduced in Scylla 5.1:
    https://github.com/scylladb/scylla/commit/dab56b82fae5e36f7aa2ca6700d8cfa5baa2b515"""

    @pytest.mark.dtest_full
    def test_per_partition_rate_limit(self):
        # Create 2 node cluster to verify that it works also with non-shard aware driver
        # when half of requests go to coordinator node instead of replica node.
        self.cluster.populate(2).start()
        node_1, node_2 = self.cluster.nodelist()

        # Create kesypace/table with feature enabled
        KEYSPACE = "test_ks"
        max_reads_per_second = 10
        max_writes_per_second = 10

        session = self.patient_cql_connection(node_1)
        session.execute(
            """
            CREATE KEYSPACE IF NOT EXISTS %s
            WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '1' }
            """
            % KEYSPACE
        )
        session.execute(
            f"""CREATE TABLE IF NOT EXISTS {KEYSPACE}.standard1 (a int PRIMARY KEY, b int)
             WITH per_partition_rate_limit = {{'max_reads_per_second': {max_reads_per_second},
             'max_writes_per_second': {max_writes_per_second}}}"""
        )

        # Run queries for given duration as fast as possible
        duration = 20
        end_time = time() + duration
        queries_count = 0
        queries_passed = 0
        while time() < end_time:
            try:
                queries_count += 1
                session.execute(f"insert into {KEYSPACE}.standard1 (a, b) values (1, 1)")
                queries_passed += 1
            except ConfigurationException:
                # For drivers that don't recognize rate limit error, ConfigurationException is raised
                pass
        metrics_node_1 = get_node_metrics(
            node_ip=node_1.address(),
            metrics=[
                "total_writes_rate_limited",
                "scylla_storage_proxy_coordinator_write_rate_limited",
            ],
        )
        metrics_node_2 = get_node_metrics(
            node_ip=node_2.address(),
            metrics=[
                "total_writes_rate_limited",
                "scylla_storage_proxy_coordinator_write_rate_limited",
            ],
        )
        logger.debug(metrics_node_1)
        logger.debug(metrics_node_2)
        # there should be rejections by replica present when non shard-aware driver queries node without given token
        # one metric is 0 (non replica), but we don't know which one
        rejected_by_replica = metrics_node_1["total_writes_rate_limited"] or metrics_node_2["total_writes_rate_limited"]
        assert rejected_by_replica, "Missing value for total_writes_rate_limited metric"

        # validate rejected queries metric
        rejected_queries_metric = (
            metrics_node_1["scylla_storage_proxy_coordinator_write_rate_limited"]
            + metrics_node_2["scylla_storage_proxy_coordinator_write_rate_limited"]
        )
        rejected_queries_actual = queries_count - queries_passed
        assert (
            rejected_queries_actual == rejected_queries_metric
        ), f"writes limited metric shows wrong number: {rejected_queries_metric} != {rejected_queries_actual}"

        # validate the rate (due limited precision and use of non-shard aware driver verify in (0.9x, 2x) range
        assert 0.9 * max_writes_per_second < queries_passed / duration < 2 * max_writes_per_second,\
            "Actual rate is different specified write rate limit"

        # verification for read rate limit
        queries_count = 0
        queries_passed = 0
        end_time = time() + duration
        while time() < end_time:
            try:
                queries_count += 1
                session.execute("select * from test_ks.standard1 where a = 1")
                queries_passed += 1
            except ConfigurationException:
                pass
        metrics_node_1 = get_node_metrics(
            node_ip=node_1.address(),
            metrics=[
                "total_reads_rate_limited",
                "scylla_storage_proxy_coordinator_read_rate_limited",
            ],
        )
        metrics_node_2 = get_node_metrics(
            node_ip=node_2.address(),
            metrics=[
                "total_reads_rate_limited",
                "scylla_storage_proxy_coordinator_read_rate_limited",
            ],
        )
        logger.debug(metrics_node_1)
        logger.debug(metrics_node_2)
        # there should be rejections by replica present when non shard-aware driver queries node without given token
        # one metric is 0 (non replica), but we don't know which one
        rejected_by_replica = metrics_node_1["total_reads_rate_limited"] or metrics_node_2["total_reads_rate_limited"]
        assert rejected_by_replica, "Missing value for total_read_rate_limited metric"

        # validate the rate
        assert 0.9 * max_reads_per_second < queries_passed / duration < 2 * max_reads_per_second,\
            "Actual rate is different specified read rate limit"

        # Commented out until https://github.com/scylladb/scylladb/issues/11651 is fixed
        # validate rejected queries metric
        # rejected_queries_metric = (
        #     metrics_node_1["scylla_storage_proxy_coordinator_read_rate_limited"]
        #     + metrics_node_2["scylla_storage_proxy_coordinator_read_rate_limited"]
        # )
        # rejected_queries_actual = queries_count - queries_passed
        #
        # assert (
        #     rejected_queries_actual == rejected_queries_metric
        # ), f"reads limited metric shows wrong number: {rejected_queries_metric} != {rejected_queries_actual}"
