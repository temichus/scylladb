import os
import random
import re
import subprocess
import pytest
import logging

from ccmlib import common
from dtest_class import Tester, create_ks


logger = logging.getLogger(__name__)


@pytest.mark.dtest_full
class TestOfflineTools(Tester):
    """
    Test cassandra offline tools.
    """

    # In 2.0, we will get this error log message due to jamm not being
    # in the classpath
    ignore_log_patterns = ["Unable to initialize MemoryMeter",
                           "Max sstable size of",
                           "Picked up JAVA_TOOL_OPTIONS"]

    def _nodetool_stderr_has_error(self, stderr):
        """
        Verify if stderr contains an actual error message.

        Check contents and ignore certain patterns.

        :param stderr: Standard error contents.
        :return: Whether stderr contains an actual error message.
        """
        if not stderr:
            return False
        error_lines = stderr.splitlines()
        results = dict()
        for line in error_lines:
            line_ok = False
            for ignore_pattern in self.ignore_log_patterns:
                if line.startswith(ignore_pattern):
                    line_ok = True
            results[line] = line_ok
        return not all(results.values())

    def verify_nodetool_stderr(self, error):
        assert not self._nodetool_stderr_has_error(error), f"Unexpected nodetool stderr: {error}"

    @pytest.mark.single_node
    def test_sstablelevelreset(self):
        """
        Insert data and call sstablelevelreset on a series of
        tables. Confirm level is reset to 0 using its output.
        Test a variety of possible errors and ensure response is resonable.
        @since 2.1.5
        @jira_ticket CASSANDRA-7614
        """
        cluster = self.cluster
        cluster.populate(1).start(wait_for_binary_proto=True)
        node1 = cluster.nodelist()[0]

        # test by trying to run on nonexistent keyspace
        cluster.stop(gently=False)
        (output, error, rc) = node1.run_sstablelevelreset("keyspace1", "standard1", output=True)
        assert "ColumnFamily not found: keyspace1/standard1" in error, "Message was not found in stderr"
        # this should return exit code 1
        assert rc == 1, f"Invalid exit code: {str(rc)}"

        # now test by generating keyspace but not flushing sstables
        cluster.start(wait_for_binary_proto=True)
        node1.stress(['write', 'n=100', '-schema', 'replication(factor=1)'])
        cluster.stop(gently=False)

        (output, error, rc) = node1.run_sstablelevelreset("keyspace1", "standard1", output=True)
        self.verify_nodetool_stderr(error)
        assert "Found no sstables, did you give the correct keyspace" in output, "Message was not found in stdout"
        assert rc == 0, f"Invalid exit code: {str(rc)}"

        # test by writing small amount of data and flushing (all sstables should be level 0)
        cluster.start(wait_for_binary_proto=True)
        session = self.patient_cql_connection(node1)
        session.execute(
            "ALTER TABLE keyspace1.standard1 with compaction={'class': 'LeveledCompactionStrategy', 'sstable_size_in_mb':1};")
        node1.stress(['write', 'n=1K', '-schema', 'replication(factor=1)'])
        node1.flush()
        cluster.stop(gently=False)

        (output, error, rc) = node1.run_sstablelevelreset("keyspace1", "standard1", output=True)
        self.verify_nodetool_stderr(error)
        assert "since it is already on level 0" in output, "Message was not found in stdout"
        assert rc == 0, f"Invalid exit code: {str(rc)}"

        # test by loading large amount data so we have multiple levels and checking all levels are 0 at end
        cluster.start(wait_for_binary_proto=True)
        node1.stress(['write', 'n=50K', '-rate', 'threads=20', '-schema', 'replication(factor=1)'])
        cluster.flush()
        self.wait_for_compactions(node1)
        cluster.stop()

        initial_levels = self.get_levels(node1.run_sstablemetadata(keyspace="keyspace1", column_families=["standard1"]))
        (output, error, rc) = node1.run_sstablelevelreset("keyspace1", "standard1", output=True)
        final_levels = self.get_levels(node1.run_sstablemetadata(keyspace="keyspace1", column_families=["standard1"]))
        self.verify_nodetool_stderr(error)
        assert rc == 0, f"Invalid exit code: {str(rc)}"

        logger.debug(initial_levels)
        logger.debug(final_levels)

        # let's make sure there was at least L1 beforing resetting levels
        assert max(initial_levels) > 0, "Missing required level before reseting"

        # let's check all sstables are on L0 after sstablelevelreset
        assert max(final_levels) == 0, f"Wronng level {max(final_levels)}"

    def get_levels(self, data):
        levels = []
        for sstable in data:
            (metadata, error, rc) = sstable
            try:
                level = int(re.findall("SSTable Level: [0-9]", metadata)[0][-1])
                levels.append(level)
            except (IndexError, ValueError):
                pytest.fail(
                    f'\nsstablemetadata failed with the following:\n\nstderr:\n{error}\nstdout:\n{metadata}\nreturn code: {rc}')
        return levels

    def wait_for_compactions(self, node):
        pattern = re.compile("pending tasks: 0")
        while True:
            output, err = node.nodetool("compactionstats", capture_output=True)
            if pattern.search(output):
                break

    @pytest.mark.skip("sstableofflinerelevel is not supported by scylla: scylladb/scylla#1151, scylladb/scylla-ccm#87")
    @pytest.mark.single_node
    def test_sstableofflinerelevel(self):
        """
        Generate sstables of varying levels.
        Reset sstables to L0 with sstablelevelreset
        Run sstableofflinerelevel and ensure tables are promoted correctly
        Also test a variety of bad inputs including nonexistent keyspace and sstables
        @since 2.1.5
        @jira_ticket CASSANRDA-8031
        """
        cluster = self.cluster
        cluster.populate(1).start(wait_for_binary_proto=True)
        node1 = cluster.nodelist()[0]

        # NOTE - As of now this does not return when it encounters Exception and causes test to hang, temporarily commented out
        # test by trying to run on nonexistent keyspace
        # cluster.stop(gently=False)
        # (output, error, rc) = node1.run_sstableofflinerelevel("keyspace1", "standard1", output=True)
        # self.assertTrue("java.lang.IllegalArgumentException: Unknown keyspace/columnFamily keyspace1.standard1" in error)
        # # this should return exit code 1
        # assert rc == 1, f"Invalid exit code: {str(rc)}"
        # cluster.start()

        # now test by generating keyspace but not flushing sstables

        node1.stress(['write', 'n=1', '-schema', 'replication(factor=1)'])
        cluster.stop(gently=False)

        (output, error, rc) = node1.run_sstableofflinerelevel("keyspace1", "standard1", output=True)

        assert "No sstables to relevel for keyspace1.standard1" in output, "Message was not found in stdout"
        assert rc == 1, f"Invalid exit code: {str(rc)}"

        # test by flushing (sstable should be level 0)
        cluster.start(wait_for_binary_proto=True)
        session = self.patient_cql_connection(node1)
        session.execute(
            "ALTER TABLE keyspace1.standard1 with compaction={'class': 'LeveledCompactionStrategy', 'sstable_size_in_mb':1};")

        node1.stress(['write', 'n=1K', '-schema', 'replication(factor=1)'])

        node1.flush()
        cluster.stop()

        (output, error, rc) = node1.run_sstableofflinerelevel("keyspace1", "standard1", output=True)
        assert "L0=1" in output
        assert rc == 1, f"Invalid exit code: {str(rc)}"

        # test by loading large amount data so we have multiple sstables
        cluster.start(wait_for_binary_proto=True)
        node1.stress(['write', 'n=100K', '-schema', 'replication(factor=1)'])
        node1.flush()
        self.wait_for_compactions(node1)
        cluster.stop()

        # Let's reset all sstables to L0
        initial_levels = self.get_levels(node1.run_sstablemetadata(keyspace="keyspace1", column_families=["standard1"]))
        (output, error, rc) = node1.run_sstablelevelreset("keyspace1", "standard1", output=True)
        final_levels = self.get_levels(node1.run_sstablemetadata(keyspace="keyspace1", column_families=["standard1"]))

        # let's make sure there was at least 3 levels (L0, L1 and L2)
        assert max(initial_levels) > 1, "Required levels are not reached"
        # let's check all sstables are on L0 after sstablelevelreset
        assert max(final_levels) == 0, "Level was not reseted to level 0"

        # time to relevel sstables
        initial_levels = self.get_levels(node1.run_sstablemetadata(keyspace="keyspace1", column_families=["standard1"]))
        (output, error, rc) = node1.run_sstableofflinerelevel("keyspace1", "standard1", output=True)
        final_levels = self.get_levels(node1.run_sstablemetadata(keyspace="keyspace1", column_families=["standard1"]))

        logger.debug(initial_levels)
        logger.debug(final_levels)

        # let's check sstables were promoted after releveling
        assert max(final_levels) > 1, "Level was not reached"

    @pytest.mark.single_node
    def test_sstableverify(self):
        """
        Generate sstables and test offline verification works correctly
        Test on bad input: nonexistent keyspace and sstables
        Test on potential situations: deleted sstables, corrupted sstables
        """

        cluster = self.cluster
        cluster.populate(1).start(wait_for_binary_proto=True)
        node1 = cluster.nodelist()[0]

        # test on nonexistent keyspace
        (out, err, rc) = node1.run_sstableverify("keyspace1", "standard1", output=True)
        assert "Unknown keyspace/table keyspace1.standard1" in err, f"Message was not found in stderr:\nstdout:\n{out}\nstderr:\n{err}"
        assert rc == 1, f"Invalid exit code: {str(rc)}"

        # test on nonexistent sstables:
        node1.stress(['write', 'n=100', '-schema', 'replication(factor=1)'])
        (out, err, rc) = node1.run_sstableverify("keyspace1", "standard1", output=True)
        assert rc == 0, f"Invalid exit code: {str(rc)}"

        # Generate multiple sstables and test works properly in the simple case
        node1.stress(['write', 'n=10K', '-schema', 'replication(factor=1)'])
        node1.flush()
        node1.stress(['write', 'n=10K', '-schema', 'replication(factor=1)'])
        node1.flush()
        # wait if any compaction are running
        node1.wait_for_compactions()
        cluster.stop()

        (out, error, rc) = node1.run_sstableverify("keyspace1", "standard1", output=True)
        logger.info(out)
        assert rc == 0, f"Invalid exit code: {str(rc)}"

        # STDOUT of the sstableverify command consists of multiple lines which may contain
        # Java-normalized paths. To later compare these with Python-normalized paths, we
        # map over each line of out and replace Java-normalized paths with Python equivalents.
        outlines = list(map(lambda line: re.sub("(?<=path=').*(?=')",
                                                lambda match: os.path.normcase(match.group(0)),
                                                line),
                            out.splitlines()))

        # check output is correct for each sstable
        sstables = self._get_final_sstables(node1, "keyspace1", "standard1")

        for sstable in sstables:
            logger.debug(sstable)
            verified = False
            hashcomputed = False
            for line in outlines:
                if sstable in line:
                    if "Verifying BigTableReader" in line:
                        verified = True
                    elif "Checking computed hash of BigTableReader" in line:
                        hashcomputed = True
                    else:
                        logger.debug(line)

            logger.debug(verified)
            logger.debug(hashcomputed)
            logger.debug(sstable)
            assert verified and hashcomputed, \
                f"Verifying {verified} or hashcomputed{hashcomputed} not found in {outlines} for sstable {sstable}"

        # now try intentionally corrupting an sstable to see if hash computed is different and error recognized
        sstable1 = random.choice(sstables)
        with open(sstable1, 'rb') as f:
            sstabledata = bytearray(f.read())
        with open(sstable1, 'wb') as out:
            position = random.randrange(0, len(sstabledata))
            sstabledata[position] = (sstabledata[position] + 1) % 256
            out.write(sstabledata)

        # use verbose to get some coverage on it
        (out, error, rc) = node1.run_sstableverify("keyspace1", "standard1", options=['-v'], output=True)

        # Process sstableverify output to normalize paths in string to Python casing as above
        regex = rf"Corrupted( SSTable\s*)?:\s+{sstable1}"

        assert re.search(regex, out) or re.search(
            regex, error), f"'{regex}' was not found in sstableverify standard output or error:\nstdout:\n{out}\nstderr:\n{error}"
        assert rc == 1, f"Invalid exit code: {str(rc)}"

    @pytest.mark.skip("Skip test due to issue: scylladb/scylla-tools-java#154")
    @pytest.mark.single_node
    def test_sstableexpiredblockers(self):
        cluster = self.cluster
        cluster.populate(1).start(wait_for_binary_proto=True)
        [node1] = cluster.nodelist()
        session = self.patient_cql_connection(node1)
        create_ks(session, 'ks', 1)
        session.execute("create table ks.cf (key int PRIMARY KEY, val int) with gc_grace_seconds=0")
        # create a blocker:
        session.execute("insert into ks.cf (key, val) values (1,1)")
        node1.flush()
        session.execute("delete from ks.cf where key = 2")
        node1.flush()
        session.execute("delete from ks.cf where key = 3")
        node1.flush()
        [(out, error, rc)] = node1.run_sstableexpiredblockers(keyspace="ks", column_family="cf")
        assert "blocks 2 expired sstables from getting dropped" in out, "Message was not found in stdout"

    def _get_final_sstables(self, node, ks, table):
        """
        Return the node final sstable data files, excluding the temporary tables.
        If sstableutil exists (>= 3.0) then we rely on this tool since the table
        file names no longer contain tmp in their names (CASSANDRA-7066).
        """
        # Get all sstable data files
        allsstables = map(os.path.normcase, node.get_sstables(ks, table))

        # Remove any temporary files
        tool_bin = node.get_tool('sstableutil')
        if os.path.isfile(tool_bin):
            args = [tool_bin, '--type', 'tmp', ks, table]
            env = common.make_cassandra_env(node.get_install_cassandra_root(), node.get_node_cassandra_root())
            p = subprocess.Popen(args, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            (stdout, stderr) = p.communicate()
            tmpsstables = map(os.path.normcase, stdout.splitlines())

            ret = list(set(allsstables) - set(tmpsstables))
        else:
            ret = [sstable for sstable in allsstables if "tmp" not in sstable[50:]]

        return ret
