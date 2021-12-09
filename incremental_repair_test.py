from __future__ import print_function
import os
import time
from re import findall
import logging
import pytest

from cassandra import ConsistencyLevel
from tools.assertions import assert_almost_equal, assert_one
from ccmlib.node import Node
from ccmlib.common import is_win
from dtest_class import Tester, create_ks, create_cf
from dtest_setup import DTestSetup
from dtest_setup_overrides import DTestSetupOverrides
from tools.misc import ImmutableMapping, require
from tools.data import insert_c1c2
from pkg_resources import parse_version


logger = logging.getLogger(__name__)


class TestIncRepair(Tester):
    @pytest.fixture(scope='function', autouse=True)
    def fixture_dtest_setup_overrides(self, dtest_config):
        dtest_setup_overrides = DTestSetupOverrides()
        dtest_setup_overrides.cluster_options = ImmutableMapping({'start_rpc': 'true'})
        return dtest_setup_overrides

    @pytest.fixture(autouse=True)
    def fixture_add_additional_log_patterns(self, fixture_dtest_setup: DTestSetup):
        fixture_dtest_setup.ignore_log_patterns += [
            # This one occurs when trying to send the migration to a
            # node that hasn't started yet, and when it does, it gets
            # replayed and everything is fine.
            r'Can\'t send migration request: node.*is down',
        ]

    def test_sstable_marking(self):
        cluster = self.cluster
        # hinted handoff can create SSTable that we don't need after node3 restarted
        cluster.set_configuration_options(values={'hinted_handoff_enabled': False})
        cluster.populate(3).start()
        node1, node2, node3 = cluster.nodelist()

        node3.stop(gently=True)

        node1.stress(['write', 'n=10000', '-schema', 'replication(factor=3)'])
        node1.flush()
        node2.flush()

        node3.start(wait_other_notice=True)
        time.sleep(3)

        if parse_version(cluster.version()) >= parse_version("2.2"):
            node3.repair()
        else:
            node3.nodetool("repair -par -inc")

        for out in (node.run_sstablemetadata(keyspace='keyspace1') for node in self.cluster.nodelist()):
            assert 'Repaired at: 0' not in out

    def test_multiple_repair(self):
        cluster = self.cluster
        cluster.populate(3).start()
        node1, node2, node3 = cluster.nodelist()

        session = self.patient_cql_connection(node1)
        create_ks(session, 'ks', 3)
        create_cf(session, 'cf', read_repair=0.0, columns={'c1': 'text', 'c2': 'text'})

        logger.debug("insert data")

        insert_c1c2(session, keys=range(1, 50), consistency=ConsistencyLevel.ALL)
        node1.flush()

        logger.debug("bringing down node 3")
        node3.flush()
        node3.stop(wait_other_notice=True, gently=False)

        logger.debug("inserting additional data into node 1 and 2")
        insert_c1c2(session, keys=range(50, 100), consistency=ConsistencyLevel.TWO)
        node1.flush()
        node2.flush()

        logger.debug("restarting and repairing node 3")
        node3.start(wait_for_binary_proto=True)

        if parse_version(cluster.version()) >= parse_version("2.2"):
            node3.repair()
        else:
            node3.nodetool("repair -par -inc")

        # wait stream handlers to be closed on windows
        # after session is finished (See CASSANDRA-10644)
        if is_win():
            time.sleep(2)

        logger.debug("stopping node 2")
        node2.stop(wait_other_notice=True, gently=False)

        logger.debug("inserting data in nodes 1 and 3")
        insert_c1c2(session, keys=range(100, 150), consistency=ConsistencyLevel.TWO)
        node1.flush()
        node3.flush()

        logger.debug("start and repair node 2")
        node2.start(wait_for_binary_proto=True)

        if parse_version(cluster.version()) >= parse_version("2.2"):
            node2.repair()
        else:
            node2.nodetool("repair -par -inc")

        logger.debug("replace node and check data integrity")
        node3.stop(wait_other_notice=True, gently=False)
        ip_network = node1.network_interfaces['thrift'][0].rsplit('.', 1)[0]
        node5 = cluster.new_node(5, auto_bootstrap=True, add_node=False)
        cluster.add(node5, is_seed=False)
        node5.start(replace_address=f'{ip_network}.3', wait_other_notice=True)

        assert_one(session, "SELECT COUNT(*) FROM ks.cf LIMIT 200", [149])

    @pytest.mark.skip('Test is now failing this assert - "assert len(uniquematches) >= 2"')
    def test_sstable_repairedset(self):
        cluster = self.cluster
        cluster.populate(2).start()
        node1, node2 = cluster.nodelist()
        node1.stress(['write', 'n=10000', '-schema', 'replication(factor=2)'])

        node1.flush()
        node2.flush()

        node2.stop(wait_other_notice=True, gently=False)

        node2.run_sstablerepairedset(keyspace='keyspace1')
        node2.start(wait_for_binary_proto=True)

        with open('initial.txt', 'w') as f:
            node2.run_sstablemetadata(output_file=f, keyspace='keyspace1')
            node1.run_sstablemetadata(output_file=f, keyspace='keyspace1')

        node1.stop()
        node2.stress(['write', 'n=15000', '-schema', 'replication(factor=2)'])
        node2.flush()
        node1.start(wait_for_binary_proto=True)

        if parse_version(cluster.version()) >= parse_version("2.2"):
            node1.repair()
        else:
            node1.nodetool("repair -par -inc")

        with open('final.txt', 'w') as h:
            node1.run_sstablemetadata(output_file=h, keyspace='keyspace1')
            node2.run_sstablemetadata(output_file=h, keyspace='keyspace1')

        with open('final.txt', 'r') as r:
            finaloutput = r.read()

        matches = findall('(?<=Repaired at:).*', finaloutput)

        logger.debug(matches)

        uniquematches = []
        matchcount = []
        for value in matches:
            if value not in uniquematches:
                uniquematches.append(value)
                matchcount.append(1)
            else:
                index = uniquematches.index(value)
                matchcount[index] = matchcount[index] + 1

        assert len(uniquematches) >= 2

        assert max(matchcount) >= 2

        assert 'repairedAt: 0' not in finaloutput

        os.remove('initial.txt')
        os.remove('final.txt')

    def test_compaction(self):
        cluster = self.cluster
        cluster.populate(3).start()
        node1, node2, node3 = cluster.nodelist()

        session = self.patient_cql_connection(node1)
        create_ks(session, 'ks', 3)
        session.execute("create table tab(key int PRIMARY KEY, val int);")

        node3.stop()

        for x in range(0, 100):
            session.execute("insert into tab(key,val) values(" + str(x) + ",0)")
        node1.flush()

        node3.start(wait_for_binary_proto=True)

        if parse_version(cluster.version()) >= parse_version("2.2"):
            node3.repair()
        else:
            node3.nodetool("repair -par -inc")
        for x in range(0, 150):
            session.execute("insert into tab(key,val) values(" + str(x) + ",1)")
        node1.flush()
        node2.flush()
        node3.flush()

        node3.nodetool('compact')

        for x in range(0, 150):
            assert_one(session, "select val from tab where key =" + str(x), [1])

    @pytest.mark.dtest_long
    @pytest.mark.skip('hangs CI')
    def test_multiple_subsequent_repair(self):
        """
        Covers CASSANDRA-8366

        There is an issue with subsequent inc repairs.
        """
        cluster = self.cluster
        cluster.populate(3).start()
        [node1, node2, node3] = cluster.nodelist()

        logger.debug("Inserting data with stress")
        node1.stress(['write', 'n=5M', '-rate', 'threads=10', '-schema', 'replication(factor=3)'])

        logger.debug("Flushing nodes")
        cluster.flush()

        logger.debug("Waiting compactions to finish")
        cluster.wait_for_compactions()

        if parse_version(self.cluster.version()) >= parse_version('2.2'):
            logger.debug("Repairing node1")
            node1.nodetool("repair")
            logger.debug("Repairing node2")
            node2.nodetool("repair")
            logger.debug("Repairing node3")
            node3.nodetool("repair")
        else:
            logger.debug("Repairing node1")
            node1.nodetool("repair -par -inc")
            logger.debug("Repairing node2")
            node2.nodetool("repair -par -inc")
            logger.debug("Repairing node3")
            node3.nodetool("repair -par -inc")

        # Using "print" instead of debug() here is on purpose.  The compactions
        # take a long time and don't print anything by default, which can result
        # in the test being timed out after 20 minutes.  These print statements
        # prevent it from being timed out.
        print("compacting node1")
        node1.compact()
        print("compacting node2")
        node2.compact()
        print("compacting node3")
        node3.compact()

        # wait some time to be sure the load size is propagated between nodes
        logger.debug("Waiting for load size info to be propagated between nodes")
        time.sleep(45)

        load_size_in_kb = float(sum(map(lambda n: n.data_size(), [node1, node2, node3])))
        load_size = load_size_in_kb / 1024 / 1024
        logger.debug("Total Load size: {}GB".format(load_size))

        # There is still some overhead, but it's lot better. We tolerate 25%.
        expected_load_size = 4.5  # In GB
        assert_almost_equal(load_size, expected_load_size, error=0.25)

    def test_sstable_marking_not_intersecting_all_ranges(self):
        """
        @jira_ticket CASSANDRA-10299
        """
        cluster = self.cluster
        cluster.populate(4, use_vnodes=True).start()
        [node1, node2, node3, node4] = cluster.nodelist()

        logger.debug("Inserting data with stress")
        node1.stress(['write', 'n=3', '-rate', 'threads=1', '-schema', 'replication(factor=3)'])

        logger.debug("Flushing nodes")
        cluster.flush()

        if parse_version(self.cluster.version()) >= parse_version('2.2'):
            logger.debug("Repairing node 1")
            node1.nodetool("repair")
            logger.debug("Repairing node 2")
            node2.nodetool("repair")
            logger.debug("Repairing node 3")
            node3.nodetool("repair")
            logger.debug("Repairing node 4")
            node4.nodetool("repair")

        else:
            logger.debug("Repairing node 1")
            node1.nodetool("repair -inc -par")
            logger.debug("Repairing node 2")
            node2.nodetool("repair -inc -par")
            logger.debug("Repairing node 3")
            node3.nodetool("repair -inc -par")
            logger.debug("Repairing node 4")
            node4.nodetool("repair -inc -par")

        for out in (node.run_sstablemetadata(keyspace='keyspace1') for node in cluster.nodelist() if
                    len(node.get_sstables('keyspace1', 'standard1')) > 0):
            assert 'Repaired at: 0' not in out
