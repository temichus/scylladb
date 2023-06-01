# TODO: https://github.com/apache/cassandra-dtest/blob/trunk/jmx_test.py has more tests (3 tests under TestJMX class
#       and complete new TestJMXSSL class), on other hand our file contains 1 test which not exists in cassandra-dtest.
#       Probably, we need to sync with upstream project.
#
# It doesn't look like this test is pretty useful for Scylla:
#
#  * All Jolokia part is not applicable to Scylla.
#  * test_cfhistograms test removed in cassandra-dtest because "this test took 44m minutes to run, typically failed,
#    and provided questionable value."
#  * the only test we can keep is test_netstats (but need to update regexp) and it's trivial.
#

import re
import sys
import time
import logging

import pytest

import ccmlib.common
from tools.jmxutils import JolokiaAgent, make_mbean, remove_perf_disable_shared_mem
from dtest_class import Tester
from ccmlib.node import NodetoolError


PROGRESS_RE = re.compile(r"standard1, (\d+)/")

logger = logging.getLogger(__name__)


class TestJMX(Tester):

    @pytest.mark.skipif(sys.platform == "win32", reason="Skip long tests on Windows")
    def test_cfhistograms(self):
        """Test cfhistograms on large and small datasets.

        @jira_ticket CASSANDRA-8028
        """
        cluster = self.cluster
        cluster.populate(3).start(wait_for_binary_proto=True)
        node1, node2, node3 = cluster.nodelist()

        # Issue large stress write to load data into cluster.
        node1.stress(["write", "n=15M", "-schema", "replication(factor=3)", "-rate", "threads=50", ])
        node1.flush()

        try:
            # TODO: the keyspace and table name are capitalized in 2.0
            histogram = node1.nodetool("cfhistograms keyspace1 standard1", capture_output=True)
            logger.info(histogram)
        except Exception as exc:
            pytest.fail(f"Cfhistograms command failed: {exc}")
        else:
            assert "Unable to compute when histogram overflowed" not in histogram
            assert "NaN" not in histogram

        session = self.patient_cql_connection(node1)

        session.execute(
            "CREATE KEYSPACE test WITH REPLICATION = {'class':'NetworkTopologyStrategy', 'replication_factor':3}")
        session.execute("CREATE TABLE test.tab(key int primary key, val int);")

        try:
            finalhistogram = node1.nodetool("cfhistograms test tab", capture_output=True)
            logger.info(finalhistogram)
        except Exception as exc:
            pytest.fail(f"Cfhistograms command failed: {exc}")
        else:
            assert "Unable to compute when histogram overflowed" not in finalhistogram
            assert "No SSTables exists, unable to calculate 'Partition Size' and 'Cell Count' percentiles" \
                   in finalhistogram[1]

    def test_netstats(self):
        """Check functioning of nodetool netstats, especially with restarts.

        @jira_ticket CASSANDRA-8122
        @jira_ticket CASSANDRA-6577
        """
        cluster = self.cluster
        cluster.populate(3).start(wait_for_binary_proto=True)
        node1, node2, node3 = cluster.nodelist()

        node1.stress(["write", "n=500K", "-schema", "replication(factor=3)", ])
        node1.flush()
        node1.stop(gently=False)

        with pytest.raises(NodetoolError, match=r"ConnectException: 'Connection refused'\."):
            node1.nodetool("netstats")

        # Don't wait; we're testing for when nodetool is called on a node mid-startup.
        node1.start(wait_for_binary_proto=False)

        # Until the binary interface is available, try `nodetool netstats`.
        binary_interface = node1.network_interfaces["binary"]
        time_out_at = time.perf_counter() + 30

        while time.perf_counter() <= time_out_at:
            try:
                node1.nodetool("netstats")
            except NodetoolError as exc:
                assert "ConnectException: 'Connection refused'." in str(exc)
            except Exception as exc:
                assert "java.lang.reflect.UndeclaredThrowableException" not in str(exc), \
                    "Netstats failed with UndeclaredThrowableException (CASSANDRA-8122)"
            if ccmlib.common.check_socket_listening(binary_interface, timeout=0.5):
                break
        else:
            pytest.fail("node1 never started")

    def test_table_metric_mbeans(self):
        """Test some basic table metric mbeans with simple writes."""

        cluster = self.cluster
        cluster.populate(3)
        node1, node2, node3 = cluster.nodelist()
        remove_perf_disable_shared_mem(node1)
        cluster.start(wait_for_binary_proto=True)

        version = cluster.version()
        node1.stress(["write", "n=10K", "-schema", "replication(factor=3)", ])

        type_name = "ColumnFamily" if version <= "2.2.X" else "Table"
        logger.info("Version %s typeName %s", version, type_name)

        # TODO: the keyspace and table name are capitalized in 2.0
        memtable_size = make_mbean(
            package="metrics",
            type=type_name,
            keyspace="keyspace1",
            scope="standard1",
            name="AllMemtablesHeapSize",
        )
        disk_size = make_mbean(
            package="metrics",
            type=type_name,
            keyspace="keyspace1",
            scope="standard1",
            name="LiveDiskSpaceUsed",
        )
        sstable_count = make_mbean(
            package="metrics",
            type=type_name,
            keyspace="keyspace1",
            scope="standard1",
            name="LiveSSTableCount",
        )

        with JolokiaAgent(node1) as jmx:
            assert int(jmx.read_attribute(memtable_size, "Value")) > 10000
            assert int(jmx.read_attribute(disk_size, "Count")) == 0

            node1.flush()

            assert int(jmx.read_attribute(disk_size, "Count")) > 10000
            assert int(jmx.read_attribute(sstable_count, "Value")) >= 1

    def test_compactionstats(self):
        """Test that jmx MBean used by nodetool compactionstats properly updates the progress of a compaction.

        @jira_ticket CASSANDRA-10504
        @jira_ticket CASSANDRA-10427
        """
        cluster = self.cluster
        cluster.populate(1)
        node = cluster.nodelist()[0]
        cluster.set_configuration_options({
            "concurrent_compactors": 1,
            "memtable_cleanup_threshold": 0.01,
        })
        remove_perf_disable_shared_mem(node)
        cluster.start(wait_for_binary_proto=True)

        # Run a quick stress command to create the keyspace and table.
        node.stress(["write", "n=1", ])

        # Disable compaction on the table.
        node.nodetool("disableautocompaction keyspace1 standard1")
        node.stress(["write", "n=750K", ])

        # Run a major compaction.  This will be the compaction whose progress we track.
        node.nodetool("compact", capture_output=False, wait=False)

        # We need to sleep here to give compaction time to start.
        # Q: Why not to do something smarter?
        # A: Because if the bug regresses, we can't rely on jmx to tell us that compaction started.
        time.sleep(5)

        compaction_manager = make_mbean(package="db", type="CompactionManager")

        with JolokiaAgent(node) as jmx:
            progress_string = jmx.read_attribute(compaction_manager, "CompactionSummary")[0]
            logger.info(progress_string)
            progress = int(PROGRESS_RE.search(progress_string).group(1))

            # Pause in between reads to allow compaction to move forward.
            time.sleep(2)

            updated_progress_string = jmx.read_attribute(compaction_manager, "CompactionSummary")[0]
            logger.info(updated_progress_string)
            updated_progress = int(PROGRESS_RE.search(updated_progress_string).group(1))

            # We want to make sure that the progress is increasing, and that values other than zero are displayed.
            assert updated_progress > progress > 0

            # Block until the major compaction is complete otherwise nodetool will throw an exception.
            # Give a timeout, in case compaction is broken and never ends.
            time_out_at = time.perf_counter() + 600

            logger.info("Waiting for compaction to finish:")
            while jmx.read_attribute(compaction_manager, "CompactionSummary") and time.perf_counter() < time_out_at:
                logger.info(jmx.read_attribute(compaction_manager, "CompactionSummary"))
                time.sleep(2)
