"""Tests for limiting streaming/repair/compaction and other throughput limits"""
import signal

import pytest
from ccmlib.scylla_node import ScyllaNode

from dtest_class import Tester
from tools.cluster import new_node


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
