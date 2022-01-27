import os
import re
import time
import logging
from collections import defaultdict, OrderedDict
from typing import Tuple
import pytest
from pkg_resources import parse_version

from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement

from ccmlib.node import Node
from dtest_class import Tester, create_ks
from tools.data import rows_to_list


logger = logging.getLogger(__name__)

TRACE_DETERMINE_REPLICAS = re.compile('Determining replicas for mutation')
TRACE_SEND_MESSAGE = re.compile('Sending message to /([0-9]+\\.[0-9]+\\.[0-9]+\\.[0-9]+)')
TRACE_RESPOND_MESSAGE = re.compile('Message received from /([0-9]+\\.[0-9]+\\.[0-9]+\\.[0-9]+)')
TRACE_COMMIT_LOG = re.compile('Appending to commitlog')
TRACE_FORWARD_WRITE = re.compile(r'Enqueuing forwarded write to /([0-9]+\.[0-9]+\.[0-9]+\.[0-9]+)')

# Some pre-computed murmur 3 hashes; there are no good python murmur3
# hashing libraries :(
murmur3_hashes = {
    5: -7509452495886106294,
    10: -6715243485458697746,
    16: -5477287129830487822,
    13: -5034495173465742853,
    11: -4156302194539278891,
    1: -4069959284402364209,
    19: -3974532302236993209,
    8: -3799847372828181882,
    2: -3248873570005575792,
    4: -2729420104000364805,
    18: -2695747960476065067,
    15: -1191135763843456182,
    20: 1388667306199997068,
    7: 1634052884888577606,
    6: 2705480034054113608,
    9: 3728482343045213994,
    14: 4279681877540623768,
    17: 5467144456125416399,
    12: 8582886034424406875,
    3: 9010454139840013625
}


@pytest.mark.no_vnodes
@pytest.mark.dtest_full
class TestReplication(Tester):
    """This test suite looks at how data is replicated across a cluster
    and who the coordinator, replicas and forwarders involved are.
    """

    def get_replicas_from_trace(self, trace):
        """Look at trace and return a list of the replicas contacted"""
        coordinator = None
        nodes_sent_write = set([])  # Nodes sent a write request
        nodes_responded_write = set([])  # Nodes that acknowledges a write
        replicas_written = set([])  # Nodes that wrote to their commitlog
        forwarders = set([])  # Nodes that forwarded a write to another node
        nodes_contacted = defaultdict(set)  # node -> list of nodes that were contacted

        for trace_event in trace.events:
            # Step 1, find coordinator node:
            activity = trace_event.description
            source = trace_event.source
            if activity.startswith('Determining replicas for mutation'):
                if not coordinator:
                    coordinator = source
                    break
            if not coordinator:
                continue

        for trace_event in trace.events:
            activity = trace_event.description
            source = trace_event.source

            # Step 2, find all the nodes that each node talked to:
            send_match = TRACE_SEND_MESSAGE.search(activity)
            recv_match = TRACE_RESPOND_MESSAGE.search(activity)
            if send_match:
                node_contacted = send_match.groups()[0]
                if source == coordinator:
                    nodes_sent_write.add(node_contacted)
                nodes_contacted[source].add(node_contacted)
            elif recv_match:
                node_contacted = recv_match.groups()[0]
                if source == coordinator:
                    nodes_responded_write.add(recv_match.groups()[0])

            # Step 3, find nodes that forwarded to other nodes:
            # (Happens in multi-datacenter clusters)
            if source != coordinator:
                forward_match = TRACE_FORWARD_WRITE.search(activity)
                if forward_match:
                    forwarding_node = forward_match.groups()[0]
                    nodes_sent_write.add(forwarding_node)
                    forwarders.add(forwarding_node)

            # Step 4, find nodes who actually wrote data:
            if TRACE_COMMIT_LOG.search(activity):
                replicas_written.add(source)

        return {"coordinator": coordinator,
                "forwarders": forwarders,
                "replicas": replicas_written,
                "nodes_sent_write": nodes_sent_write,
                "nodes_responded_write": nodes_responded_write,
                "nodes_contacted": nodes_contacted
                }

    def get_replicas_for_token(self, token, replication_factor,
                               strategy='SimpleStrategy', nodes=None):
        """Figure out which node(s) should receive data for a given token and
        replication factor"""
        if not nodes:
            nodes = self.cluster.nodelist()
        token_ranges = sorted(zip([n.initial_token for n in nodes], nodes))
        replicas = []

        # Find first replica:
        for i, (r, node) in enumerate(token_ranges):
            if token <= r:
                replicas.append(node.address())
                first_ring_position = i
                break
        else:
            replicas.append(token_ranges[0][1].address())
            first_ring_position = 0

        # Find other replicas:
        if strategy == 'SimpleStrategy':
            for node in nodes[first_ring_position + 1:]:
                replicas.append(node.address())
                if len(replicas) == replication_factor:
                    break
            if len(replicas) != replication_factor:
                # Replication token range looped:
                for node in nodes:
                    replicas.append(node.address())
                    if len(replicas) == replication_factor:
                        break
        elif strategy == 'NetworkTopologyStrategy':
            # NetworkTopologyStrategy can be broken down into multiple
            # SimpleStrategies, just once per datacenter:
            for dc, rf in replication_factor.items():
                dc_nodes = [n for n in nodes if n.data_center == dc]
                replicas.extend(self.get_replicas_for_token(
                    token, rf, nodes=dc_nodes))
        else:
            raise NotImplementedError(f'replication strategy not implemented: {strategy}')

        return replicas

    @staticmethod
    def pprint_trace(trace):
        """Pretty print a trace"""
        print("-" * 40)
        for t in trace.events:
            print("%s\t%s\t%s\t%s" % (t.source, t.source_elapsed, t.description, t.thread_name))
        print("-" * 40)

    def test_simple(self):
        """Test the SimpleStrategy on a 3 node cluster"""
        self.cluster.populate(3).start()
        time.sleep(5)
        node1 = self.cluster.nodelist()[0]
        self.conn = self.patient_exclusive_cql_connection(node1)

        # Install a tracing session so we can get info about who the
        # coordinator is contacting:
        session = self.conn
        session.max_trace_wait = 120

        replication_factor = 3
        create_ks(session, 'test', replication_factor)
        session.execute('CREATE TABLE test.test (id int PRIMARY KEY, value text)', trace=False)
        # Wait for table creation, otherwise trace times out -
        # CASSANDRA-5658
        time.sleep(5)

        for key, token in murmur3_hashes.items():
            query = SimpleStatement("INSERT INTO test (id, value) VALUES (%s, 'asdf')" %
                                    key, consistency_level=ConsistencyLevel.ALL)
            future = session.execute_async(query, trace=True)
            future.result()
            trace = future.get_query_trace(max_wait=120)
            self.pprint_trace(trace)
            stats = self.get_replicas_from_trace(trace)
            replicas_should_be = set(self.get_replicas_for_token(
                token, replication_factor))
            logger.debug('\nreplicas should be: %s' % replicas_should_be)
            logger.debug('replicas were: %s' % stats['replicas'])

            # Make sure the correct nodes are replicas:
            assert stats['replicas'] == replicas_should_be
            # Make sure that each replica node was contacted and
            # acknowledged the write:
            assert stats['nodes_sent_write'] == stats['nodes_responded_write']

    def test_network_topology(self):
        """Test the NetworkTopologyStrategy on a 2DC 3:3 node cluster"""
        self.cluster.populate([3, 3]).start()
        time.sleep(5)
        node1 = self.cluster.nodelist()[0]
        ip_nodes = dict((node.address(), node) for node in self.cluster.nodelist())
        self.conn = self.patient_exclusive_cql_connection(node1)

        # Install a tracing session so we can get info about who the
        # coordinator is contacting:
        session = self.conn

        replication_factor = {'dc1': 2, 'dc2': 2}
        create_ks(session, 'test', replication_factor)
        session.execute('CREATE TABLE test.test (id int PRIMARY KEY, value text)', trace=False)
        # Wait for table creation, otherwise trace times out -
        # CASSANDRA-5658
        time.sleep(5)

        forwarders_used = set()

        for key, token in murmur3_hashes.items():
            query = SimpleStatement("INSERT INTO test (id, value) VALUES (%s, 'asdf')" %
                                    key, consistency_level=ConsistencyLevel.ALL)
            future = session.execute_async(query, trace=True)
            future.result()
            trace = future.get_query_trace(max_wait=120)
            self.pprint_trace(trace)
            stats = self.get_replicas_from_trace(trace)
            replicas_should_be = set(self.get_replicas_for_token(
                token, replication_factor, strategy='NetworkTopologyStrategy'))
            logger.debug('Current token is %s' % token)
            logger.debug('\nreplicas should be: %s' % replicas_should_be)
            logger.debug('replicas were: %s' % stats['replicas'])

            # Make sure the coordinator only talked to a single node in
            # the second datacenter - CASSANDRA-5632:
            num_in_other_dcs_contacted = 0
            for node_contacted in stats['nodes_contacted'][node1.address()]:
                if ip_nodes[node_contacted].data_center != node1.data_center:
                    num_in_other_dcs_contacted += 1
            assert num_in_other_dcs_contacted == 1

            # Record the forwarder used for each INSERT:
            forwarders_used = forwarders_used.union(stats['forwarders'])

            try:
                # Make sure the correct nodes are replicas:
                assert stats['replicas'] == replicas_should_be
                # Make sure that each replica node was contacted and
                # acknowledged the write:
                assert stats['nodes_sent_write'] == stats['nodes_responded_write']
            except AssertionError as e:
                logger.debug("Failed on key %s and token %s." % (key, token))
                raise e

        # Given a diverse enough keyset, each node in the second
        # datacenter should get a chance to be a forwarder:
        assert len(forwarders_used) == 3


@pytest.mark.dtest_full
class TestSnitchConfigurationUpdate(Tester):
    """
    Test to reproduce CASSANDRA-10238, wherein changing snitch properties to change racks without a restart
    could violate RF contract.

    Since CASSANDRA-10243 it is no longer possible to change rack or dc for live nodes so we must specify
    which nodes should be shutdown in order to have the rack changed.
    """

    ignore_log_patterns = ["Fatal exception during initialization",
                           "Cannot start node if snitch's rack(.*) differs from previous rack(.*)",
                           "Cannot update data center or rack"]

    def check_endpoint_count(self, ks, table, nodes, rf):
        """
        Check a dummy key expecting it to have replication factor as the sum of rf on all dcs.
        """
        expected_count = sum([int(r) for d, r in rf.items() if d != 'class'])
        for node in nodes:
            cmd = "getendpoints {} {} dummy".format(ks, table)
            out, err = node.nodetool(cmd)

            if len(err.strip()) > 0:
                logger.debug("Error running 'nodetool {}': {}".format(cmd, err))

            logger.debug("Endpoints for node {}, expected count is {}".format(node.address(), expected_count))
            logger.debug(out)
            ips_found = re.findall(r'(\d+\.\d+\.\d+\.\d+)', out)

            assert len(ips_found) == expected_count,\
                f"wrong number of endpoints found ({len(ips_found)}), should be: {expected_count}"

    def wait_for_nodes_on_racks(self, nodes, expected_racks):
        """
        Waits for nodes to match the expected racks.
        """
        regex = re.compile(r"^UN(?:\s*)127\.0\.0(?:.*)\s(.*)$", re.IGNORECASE)
        for i, node in enumerate(nodes):
            wait_expire = time.time() + 120
            while time.time() < wait_expire:
                out, err = node.nodetool("status")

                logger.debug(out)
                if len(err.strip()) > 0:
                    logger.debug("Error trying to run nodetool status: {}".format(err))

                racks = []
                for line in out.split(os.linesep):
                    m = regex.match(line)
                    if m:
                        racks.append(m.group(1))

                if racks == expected_racks:
                    # great, the topology change is propagated
                    logger.debug("Topology change detected on node {}".format(i))
                    break
                else:
                    logger.debug("Waiting for topology change on node {}".format(i))
                    time.sleep(5)
            else:
                raise RuntimeError("Ran out of time waiting for topology to change on node {}".format(i))

    @pytest.mark.skip("unrecognised option '-Dcassandra.ignore_rack=true'")
    def test_rf_collapse_gossiping_property_file_snitch(self):
        """
        @jira_ticket CASSANDRA-10238
        @jira_ticket CASSANDRA-10242
        @jira_ticket CASSANDRA-10243

        Confirm that when racks are collapsed using a gossiping property file snitch the RF is not impacted.
        """
        self._test_rf_on_snitch_update(nodes=[3], rf={'class': '\'NetworkTopologyStrategy\'', 'dc1': 3},
                                       snitch_class_name='GossipingPropertyFileSnitch',
                                       snitch_config_file='cassandra-rackdc.properties',
                                       snitch_lines_before=lambda i, node: ["dc=dc1", "rack=rack{}".format(i)],
                                       snitch_lines_after=lambda i, node: ["dc=dc1", "rack=rack1"],
                                       final_racks=["rack1", "rack1", "rack1"],
                                       nodes_to_shutdown=[0, 2])

    @pytest.mark.skip("unrecognised option '-Dcassandra.ignore_rack=true'")
    def test_rf_expand_gossiping_property_file_snitch(self):
        """
        @jira_ticket CASSANDRA-10238
        @jira_ticket CASSANDRA-10242
        @jira_ticket CASSANDRA-10243

        Confirm that when racks are expanded using a gossiping property file snitch the RF is not impacted.
        """
        self._test_rf_on_snitch_update(nodes=[3], rf={'class': '\'NetworkTopologyStrategy\'', 'dc1': 3},
                                       snitch_class_name='GossipingPropertyFileSnitch',
                                       snitch_config_file='cassandra-rackdc.properties',
                                       snitch_lines_before=lambda i, node: ["dc=dc1", "rack=rack1"],
                                       snitch_lines_after=lambda i, node: ["dc=dc1", "rack=rack{}".format(i)],
                                       final_racks=["rack0", "rack1", "rack2"],
                                       nodes_to_shutdown=[0, 2])

    @pytest.mark.skip("unrecognised option '-Dcassandra.ignore_rack=true'")
    def test_rf_collapse_gossiping_property_file_snitch_multi_dc(self):
        """
        @jira_ticket CASSANDRA-10238
        @jira_ticket CASSANDRA-10242
        @jira_ticket CASSANDRA-10243

        Confirm that when racks are collapsed using a gossiping property file snitch the RF is not impacted, in a multi-dc environment.
        """
        self._test_rf_on_snitch_update(nodes=[3, 3], rf={'class': '\'NetworkTopologyStrategy\'', 'dc1': 3, 'dc2': 3},
                                       snitch_class_name='GossipingPropertyFileSnitch',
                                       snitch_config_file='cassandra-rackdc.properties',
                                       snitch_lines_before=lambda i, node: ["dc={}".format(
                                           node.data_center), "rack=rack{}".format(i % 3)],
                                       snitch_lines_after=lambda i, node: [
                                           "dc={}".format(node.data_center), "rack=rack1"],
                                       final_racks=["rack1", "rack1", "rack1", "rack1", "rack1", "rack1"],
                                       nodes_to_shutdown=[0, 2, 3, 5])

    @pytest.mark.skip("unrecognised option '-Dcassandra.ignore_rack=true'")
    def test_rf_expand_gossiping_property_file_snitch_multi_dc(self):
        """
        @jira_ticket CASSANDRA-10238
        @jira_ticket CASSANDRA-10242
        @jira_ticket CASSANDRA-10243

        Confirm that when racks are expanded using a gossiping property file snitch the RF is not impacted, in a multi-dc environment.
        """
        self._test_rf_on_snitch_update(nodes=[3, 3], rf={'class': '\'NetworkTopologyStrategy\'', 'dc1': 3, 'dc2': 3},
                                       snitch_class_name='GossipingPropertyFileSnitch',
                                       snitch_config_file='cassandra-rackdc.properties',
                                       snitch_lines_before=lambda i, node: [
                                           "dc={}".format(node.data_center), "rack=rack1"],
                                       snitch_lines_after=lambda i, node: ["dc={}".format(
                                           node.data_center), "rack=rack{}".format(i % 3)],
                                       final_racks=["rack0", "rack1", "rack2", "rack0", "rack1", "rack2"],
                                       nodes_to_shutdown=[0, 2, 3, 5])

    @pytest.mark.skip("unrecognised option '-Dcassandra.ignore_rack=true'")
    def test_rf_collapse_property_file_snitch(self):
        """
        @jira_ticket CASSANDRA-10238
        @jira_ticket CASSANDRA-10242
        @jira_ticket CASSANDRA-10243

        Confirm that when racks are collapsed using a property file snitch the RF is not impacted.
        """
        self._test_rf_on_snitch_update(nodes=[3], rf={'class': '\'NetworkTopologyStrategy\'', 'dc1': 3},
                                       snitch_class_name='PropertyFileSnitch',
                                       snitch_config_file='cassandra-topology.properties',
                                       snitch_lines_before=lambda i, node: [
                                           "127.0.0.1=dc1:rack0", "127.0.0.2=dc1:rack1", "127.0.0.3=dc1:rack2"],
                                       snitch_lines_after=lambda i, node: ["default=dc1:rack0"],
                                       final_racks=["rack0", "rack0", "rack0"],
                                       nodes_to_shutdown=[1, 2])

    @pytest.mark.skip("unrecognised option '-Dcassandra.ignore_rack=true'")
    def test_rf_expand_property_file_snitch(self):
        """
        @jira_ticket CASSANDRA-10238
        @jira_ticket CASSANDRA-10242
        @jira_ticket CASSANDRA-10243

        Confirm that when racks are expanded using a property file snitch the RF is not impacted.
        """
        self._test_rf_on_snitch_update(nodes=[3], rf={'class': '\'NetworkTopologyStrategy\'', 'dc1': 3},
                                       snitch_class_name='PropertyFileSnitch',
                                       snitch_config_file='cassandra-topology.properties',
                                       snitch_lines_before=lambda i, node: ["default=dc1:rack0"],
                                       snitch_lines_after=lambda i, node: [
                                           "127.0.0.1=dc1:rack0", "127.0.0.2=dc1:rack1", "127.0.0.3=dc1:rack2"],
                                       final_racks=["rack0", "rack1", "rack2"],
                                       nodes_to_shutdown=[1, 2])

    @pytest.mark.skip("unrecognised option '-Dcassandra.ignore_rack=true'")
    def test_rf_collapse_yaml_file_snitch(self):
        """
        @jira_ticket CASSANDRA-10238
        @jira_ticket CASSANDRA-10242
        @jira_ticket CASSANDRA-10243

        Confirm that when racks are collapsed using a yaml file snitch the RF is not impacted.
        """
        self._test_rf_on_snitch_update(nodes=[3], rf={'class': '\'NetworkTopologyStrategy\'', 'dc1': 3},
                                       snitch_class_name='YamlFileNetworkTopologySnitch',
                                       snitch_config_file='cassandra-topology.yaml',
                                       snitch_lines_before=lambda i, node: ["topology:",
                                                                            "  - dc_name: dc1",
                                                                            "    racks:",
                                                                            "    - rack_name: rack0",
                                                                            "      nodes:",
                                                                            "      - broadcast_address: 127.0.0.1",
                                                                            "    - rack_name: rack1",
                                                                            "      nodes:",
                                                                            "      - broadcast_address: 127.0.0.2",
                                                                            "    - rack_name: rack2",
                                                                            "      nodes:",
                                                                            "      - broadcast_address: 127.0.0.3"],
                                       snitch_lines_after=lambda i, node: ["topology:",
                                                                           "  - dc_name: dc1",
                                                                           "    racks:",
                                                                           "    - rack_name: rack0",
                                                                           "      nodes:",
                                                                           "      - broadcast_address: 127.0.0.1",
                                                                           "      - broadcast_address: 127.0.0.2",
                                                                           "      - broadcast_address: 127.0.0.3"],
                                       final_racks=["rack0", "rack0", "rack0"],
                                       nodes_to_shutdown=[1, 2])

    @pytest.mark.skip("unrecognised option '-Dcassandra.ignore_rack=true'")
    def test_rf_expand_yaml_file_snitch(self):
        """
        @jira_ticket CASSANDRA-10238
        @jira_ticket CASSANDRA-10242
        @jira_ticket CASSANDRA-10243

        Confirm that when racks are expanded using a yaml file snitch the RF is not impacted.
        """
        self._test_rf_on_snitch_update(nodes=[3], rf={'class': '\'NetworkTopologyStrategy\'', 'dc1': 3},
                                       snitch_class_name='YamlFileNetworkTopologySnitch',
                                       snitch_config_file='cassandra-topology.yaml',
                                       snitch_lines_before=lambda i, node: ["topology:",
                                                                            "  - dc_name: dc1",
                                                                            "    racks:",
                                                                            "    - rack_name: rack0",
                                                                            "      nodes:",
                                                                            "      - broadcast_address: 127.0.0.1",
                                                                            "      - broadcast_address: 127.0.0.2",
                                                                            "      - broadcast_address: 127.0.0.3"],
                                       snitch_lines_after=lambda i, node: ["topology:",
                                                                           "  - dc_name: dc1",
                                                                           "    racks:",
                                                                           "    - rack_name: rack0",
                                                                           "      nodes:",
                                                                           "      - broadcast_address: 127.0.0.1",
                                                                           "    - rack_name: rack1",
                                                                           "      nodes:",
                                                                           "      - broadcast_address: 127.0.0.2",
                                                                           "    - rack_name: rack2",
                                                                           "      nodes:",
                                                                           "      - broadcast_address: 127.0.0.3"],
                                       final_racks=["rack0", "rack1", "rack2"],
                                       nodes_to_shutdown=[1, 2])

    def _test_rf_on_snitch_update(self, nodes, rf, snitch_class_name, snitch_config_file,
                                  snitch_lines_before, snitch_lines_after, final_racks, nodes_to_shutdown):
        cluster = self.cluster
        cluster.populate(nodes)
        cluster.set_configuration_options(
            values={'endpoint_snitch': 'org.apache.cassandra.locator.{}'.format(snitch_class_name)})

        # start with separate racks
        for i, node in enumerate(cluster.nodelist()):
            with open(os.path.join(node.get_conf_dir(), snitch_config_file), 'w') as topo_file:
                for line in snitch_lines_before(i, node):
                    topo_file.write(line + os.linesep)

        cluster.start(wait_for_binary_proto=True)

        session = self.patient_cql_connection(cluster.nodelist()[0])

        options = (', ').join(['\'{}\': {}'.format(d, r) for d, r in rf.items()])
        session.execute("CREATE KEYSPACE testing WITH replication = {{{}}}".format(options))
        session.execute("CREATE TABLE testing.rf_test (key text PRIMARY KEY, value text)")

        # make sure endpoint count is correct before continuing with the rest of the test
        self.check_endpoint_count('testing', 'rf_test', cluster.nodelist(), rf)

        for i in nodes_to_shutdown:
            node = cluster.nodelist()[i]
            logger.debug("Shutting down node {}".format(node.address()))
            node.stop(wait_other_notice=True)

        logger.debug("Updating snitch file")
        for i, node in enumerate(cluster.nodelist()):
            with open(os.path.join(node.get_conf_dir(), snitch_config_file), 'w') as topo_file:
                for line in snitch_lines_after(i, node):
                    topo_file.write(line + os.linesep)

        # wait until the config is reloaded before we restart the nodes, the default check period is
        # 5 seconds so we wait for 10 seconds to be sure
        logger.debug("Waiting 10 seconds to make sure snitch file is reloaded...")
        time.sleep(10)

        for i in nodes_to_shutdown:
            node = cluster.nodelist()[i]
            logger.debug("Restarting node {}".format(node.address()))
            # Since CASSANDRA-10242 it is no longer
            # possible to start a node with a different rack unless we specify -Dcassandra.ignore_rack and since
            # CASSANDRA-9474 it is no longer possible to start a node with a different dc unless we specify
            # -Dcassandra.ignore_dc.
            node.start(jvm_args=['-Dcassandra.ignore_rack=true', '-Dcassandra.ignore_dc=true'],
                       wait_for_binary_proto=True)

        self.wait_for_nodes_on_racks(cluster.nodelist(), final_racks)

        # nodes have joined racks, check endpoint counts again
        self.check_endpoint_count('testing', 'rf_test', cluster.nodelist(), rf)

    @pytest.mark.require("2387")
    def test_cannot_restart_with_different_rack(self):
        """
        @jira_ticket CASSANDRA-10242

        Test that we cannot restart with a different rack if '-Dcassandra.ignore_rack=true' is not specified.
        """
        cluster = self.cluster
        cluster.populate(1)
        cluster.set_configuration_options(values={'endpoint_snitch': 'org.apache.cassandra.locator.{}'
                                                  .format('GossipingPropertyFileSnitch')})

        node1 = cluster.nodelist()[0]

        with open(os.path.join(node1.get_conf_dir(), 'cassandra-rackdc.properties'), 'w') as topo_file:
            for line in ["dc={}".format(node1.data_center), "rack=rack1"]:
                topo_file.write(line + os.linesep)

        logger.debug("Starting node {} with rack1".format(node1.address()))
        node1.start(wait_for_binary_proto=True)

        logger.debug("Shutting down node {}".format(node1.address()))
        node1.stop(wait_other_notice=True)

        logger.debug("Updating snitch file with rack2")
        for node in cluster.nodelist():
            with open(os.path.join(node.get_conf_dir(), 'cassandra-rackdc.properties'), 'w') as topo_file:
                for line in ["dc={}".format(node.data_center), "rack=rack2"]:
                    topo_file.write(line + os.linesep)

        logger.debug("Restarting node {} with rack2".format(node1.address()))
        mark = node1.mark_log()
        node1.start()

        # check node not running
        logger.debug("Waiting for error message in log file")

        if parse_version(cluster.version()) >= parse_version('2.2'):
            node1.watch_log_for("Cannot start node if snitch's rack(.*) differs from previous rack(.*)",
                                from_mark=mark)
        else:
            node1.watch_log_for("Fatal exception during initialization", from_mark=mark)

    @pytest.mark.require('2390')
    def test_failed_snitch_update_gossiping_property_file_snitch(self):
        """
        @jira_ticket CASSANDRA-10243

        Test that we cannot change the rack of a live node with GossipingPropertyFileSnitch.
        """
        self._test_failed_snitch_update(nodes=[3],
                                        snitch_class_name='GossipingPropertyFileSnitch',
                                        snitch_config_file='cassandra-rackdc.properties',
                                        snitch_lines_before=["dc=dc1", "rack=rack1"],
                                        snitch_lines_after=["dc=dc1", "rack=rack2"],
                                        racks=["rack1", "rack1", "rack1"],
                                        error='')

    @pytest.mark.skip("unable to find class 'org.apache.cassandra.locator.PropertyFileSnitch'")
    def test_failed_snitch_update_property_file_snitch(self):
        """
        @jira_ticket CASSANDRA-10243

        Test that we cannot change the rack of a live node with PropertyFileSnitch.
        """
        self._test_failed_snitch_update(nodes=[3],
                                        snitch_class_name='PropertyFileSnitch',
                                        snitch_config_file='cassandra-topology.properties',
                                        snitch_lines_before=["default=dc1:rack1"],
                                        snitch_lines_after=["default=dc1:rack2"],
                                        racks=["rack1", "rack1", "rack1"],
                                        error='Cannot update data center or rack')

    @pytest.mark.skip('obsolete from 2.2')
    def test_failed_snitch_update_yaml_file_snitch(self):
        """
        @jira_ticket CASSANDRA-10243

        Test that we cannot change the rack of a live node with YamlFileNetworkTopologySnitch.
        """
        self._test_failed_snitch_update(nodes=[3],
                                        snitch_class_name='YamlFileNetworkTopologySnitch',
                                        snitch_config_file='cassandra-topology.yaml',
                                        snitch_lines_before=["topology:",
                                                             "  - dc_name: dc1",
                                                             "    racks:",
                                                             "    - rack_name: rack1",
                                                             "      nodes:",
                                                             "      - broadcast_address: 127.0.0.1",
                                                             "      - broadcast_address: 127.0.0.2",
                                                             "      - broadcast_address: 127.0.0.3"],
                                        snitch_lines_after=["topology:",
                                                            "  - dc_name: dc1",
                                                            "    racks:",
                                                            "    - rack_name: rack2",
                                                            "      nodes:",
                                                            "      - broadcast_address: 127.0.0.1",
                                                            "      - broadcast_address: 127.0.0.2",
                                                            "      - broadcast_address: 127.0.0.3"],
                                        racks=["rack1", "rack1", "rack1"],
                                        error='Cannot update data center or rack')

    def _test_failed_snitch_update(self, nodes, snitch_class_name, snitch_config_file,
                                   snitch_lines_before, snitch_lines_after, racks, error):
        cluster = self.cluster
        cluster.populate(nodes)
        cluster.set_configuration_options(values={'endpoint_snitch': 'org.apache.cassandra.locator.{}'
                                                  .format(snitch_class_name)})

        # start with initial snitch lines
        for node in cluster.nodelist():
            with open(os.path.join(node.get_conf_dir(), snitch_config_file), 'w') as topo_file:
                for line in snitch_lines_before:
                    topo_file.write(line + os.linesep)

        cluster.start(wait_for_binary_proto=True)

        # check racks are as specified
        self.wait_for_nodes_on_racks(cluster.nodelist(), racks)

        marks = [node.mark_log() for node in cluster.nodelist()]

        logger.debug("Updating snitch file")
        for node in cluster.nodelist():
            with open(os.path.join(node.get_conf_dir(), snitch_config_file), 'w') as topo_file:
                for line in snitch_lines_after:
                    topo_file.write(line + os.linesep)

        # wait until the config is reloaded, the default check period is
        # 5 seconds so we wait for 10 seconds to be sure
        logger.debug("Waiting 10 seconds to make sure snitch file is reloaded...")
        time.sleep(10)

        # check racks have not changed
        self.wait_for_nodes_on_racks(cluster.nodelist(), racks)

        # check error in log files if applicable
        if error:
            for node, mark in zip(cluster.nodelist(), marks):
                node.watch_log_for(error, from_mark=mark)

    @pytest.mark.require('2387')
    def test_switch_data_center_startup_fails(self):
        """
        @jira_ticket CASSANDRA-9474

        Confirm that switching data centers fails to bring up the node.
        """
        expected_error = [r"Cannot start node if snitch's data center (.*) differs from previous data center (.*)\. "
                          "Please fix the snitch configuration, decommission and rebootstrap this node or use the flag -Dcassandra.ignore_dc=true."]
        self.ignore_log_patterns += expected_error

        cluster = self.cluster
        cluster.populate(1)
        cluster.set_configuration_options(
            values={'endpoint_snitch': 'org.apache.cassandra.locator.GossipingPropertyFileSnitch'})

        node = cluster.nodelist()[0]
        with open(os.path.join(node.get_conf_dir(), 'cassandra-rackdc.properties'), 'w') as topo_file:
            topo_file.write("dc=dc9" + os.linesep)
            topo_file.write("rack=rack1" + os.linesep)

        cluster.start(wait_for_binary_proto=True)

        node.stop()

        with open(os.path.join(node.get_conf_dir(), 'cassandra-rackdc.properties'), 'w') as topo_file:
            topo_file.write("dc=dc0" + os.linesep)
            topo_file.write("rack=rack1" + os.linesep)

        mark = node.mark_log()
        node.start()
        node.watch_log_for(expected_error, from_mark=mark, timeout=10)


@pytest.mark.dtest_full
class TestRFAutoExpand(Tester):
    """
    Test for #4210 (or CASSANDRA-14303).

    This tests the UX feature of expanding replication factor when using
    NetworkTopologyStrategy, e.g. when
    {'class': 'NetworkTopologyStrategy', 'replication_factor': 3}
    is used for the replication option, it will be expanded to
    {'class': 'NetworkTopologyStrategy', dc1: 3, dc2: 3, ... }
    where dc1, dc2, ... are the datacenters known to the node
    at the moment of creating the keyspace.
    """

    def test_rf_expand(self):
        self.cluster.populate([1, 1, 1]).start(
            wait_for_binary_proto=True,
            wait_other_notice=True)
        session = self.patient_cql_connection(self.cluster.nodelist()[0])

        create_ks(session, 'test_simple', {'replication_factor': 1})

        # simple expansion to all dcs
        res = session.execute(
            "SELECT replication FROM system_schema.keyspaces "
            "WHERE keyspace_name = 'test_simple'")
        assert rows_to_list(res) == [[mk_replication({'dc1': 1, 'dc2': 1, 'dc3': 1})]]

        create_ks(session, 'test_manual', {'replication_factor': 1, 'dc3': 3})

        # expand, but respect factors specified manually
        res = session.execute(
            "SELECT replication FROM system_schema.keyspaces "
            "WHERE keyspace_name = 'test_manual'")
        assert rows_to_list(res) == [[mk_replication({'dc1': 1, 'dc2': 1, 'dc3': 3})]]

        # expansion doesn't change existing replication factors
        session.execute(
            "ALTER KEYSPACE test_manual WITH replication = "
            "{'class': 'NetworkTopologyStrategy', 'replication_factor': 2}")
        res = session.execute(
            "SELECT replication FROM system_schema.keyspaces "
            "WHERE keyspace_name = 'test_manual'")
        assert rows_to_list(res) == [[mk_replication({'dc1': 1, 'dc2': 1, 'dc3': 3})]]

        create_ks(session, 'test_switch', 3)

        # expand when directly switching from SimpleStrategy
        # to NetworkTopologyStrategy
        session.execute(
            "ALTER KEYSPACE test_switch WITH replication = "
            "{'class': 'NetworkTopologyStrategy'}")
        res = session.execute(
            "SELECT replication FROM system_schema.keyspaces WHERE "
            "keyspace_name = 'test_switch'")
        assert rows_to_list(res) == [[mk_replication({'dc1': 3, 'dc2': 3, 'dc3': 3})]]

        create_ks(session, 'test_switch_2', 3)

        # don't expand when switching from SimpleStrategy
        # to NTS with manually specified DCs
        session.execute(
            "ALTER KEYSPACE test_switch_2 WITH replication = "
            "{'class': 'NetworkTopologyStrategy', 'dc1': 2}")
        res = session.execute(
            "SELECT replication FROM system_schema.keyspaces WHERE "
            "keyspace_name = 'test_switch_2'")
        assert rows_to_list(res) == [[mk_replication({'dc1': 2})]]

        # expand non-specified factors
        session.execute(
            "ALTER KEYSPACE test_switch_2 WITH replication = "
            "{'class': 'NetworkTopologyStrategy', 'replication_factor': 3}")
        res = session.execute(
            "SELECT replication FROM system_schema.keyspaces WHERE "
            "keyspace_name = 'test_switch_2'")
        assert rows_to_list(res) == [[mk_replication({'dc1': 2, 'dc2': 3, 'dc3': 3})]]


def mk_replication(dcs):
    return OrderedDict(
        [(u'class', u'org.apache.cassandra.locator.NetworkTopologyStrategy')] +
        [(str(k), str(v)) for k, v in dcs.items()])


@pytest.mark.dtest_full
class TestRestrictionReplicationSimpleStrategy(Tester):
    test_keyspace_ss = "test_ks_ss"
    test_keyspace_nts = "test_ks_nts"
    simple_strategy = "SimpleStrategy"
    network_topology_strategy = "NetworkTopologyStrategy"

    def prepare_one_node_cluster(self, option_value: str) -> Node:
        logger.debug("Preparing the cluster...")
        cluster = self.cluster
        cluster.set_configuration_options(values={'restrict_replication_simplestrategy': option_value})
        cluster.populate(1).start()
        logger.debug("Cluster has been prepared...")
        return cluster.nodelist()[0]

    @staticmethod
    def run_cqlsh_on_node(node_to_run_query: Node, query: str) -> Tuple[str, str]:
        logger.debug("Running query \"%s\" on the node %s...",
                     query, node_to_run_query.address())
        return node_to_run_query.run_cqlsh(query, return_output=True)

    def create_test_keyspace(self, node: Node, keyspace: str, replication_strategy: str) -> Tuple[str, str]:
        logger.debug("Creating a new keyspace '%s' with replication strategy '%s'...",
                     keyspace, replication_strategy)
        query = f"create keyspace {keyspace} with replication = " \
                f"{{'class': '{replication_strategy}', 'replication_factor' : 1}};"
        return self.run_cqlsh_on_node(node_to_run_query=node, query=query)

    def alter_test_keyspace(self, node: Node, keyspace: str, replication_strategy: str) -> Tuple[str, str]:
        logger.debug("Altering the new keyspace '%s' to use replication strategy '%s'...",
                     keyspace, replication_strategy)
        query = f"alter keyspace {keyspace} with replication = " \
                f"{{'class': '{replication_strategy}', 'replication_factor' : 1}};"
        return self.run_cqlsh_on_node(node_to_run_query=node, query=query)

    def describe_keyspace(self, node: Node, keyspace: str) -> Tuple[str, str]:
        query = f"describe keyspace {keyspace};"
        return self.run_cqlsh_on_node(node_to_run_query=node, query=query)

    def run_parametrized_test(self, mode: str):
        """
        This function runs the following generic test scenario
        1. Create one node with 'restrict_replication_simplestrategy' is equal to one of the values:
            'true', 'false' or 'warn'.
        2. Try to create KS with SimpleStrategy.
        3. Check the messages that were returned during this operation
        4. Check if the KS was really created or not
        5. Create KS with NetworkTopologyStrategy and try alter it to use SimpleStrategy
        6. Check the messages that were returned during this operation
        7. Check if the KS was really altered or not
        """
        node = self.prepare_one_node_cluster(option_value=mode)

        out, err = self.create_test_keyspace(node=node, keyspace=self.test_keyspace_ss,
                                             replication_strategy=self.simple_strategy)
        logger.info("Looking for the error/warning messages after the creation of keyspace '%s'...",
                    self.test_keyspace_ss)
        if mode == "true":
            assert "ConfigurationException" in err, "Expected to get 'ConfigurationException', but did not get it!"
        if mode == "false":
            assert not out and not err, "Expected not to get any errors/warnings, but got something!"
        if mode == "warn":
            assert "Warnings" in out, "Expected to get the warning message in the output, did not get it!"

        logger.info("Checking if the new keyspace %s with replication strategy %s was actually created...",
                    self.test_keyspace_ss, self.simple_strategy)
        out, err = self.describe_keyspace(node=node, keyspace=self.test_keyspace_ss)
        if mode == "true":
            assert "not found" in err, "Expected to get 'not found' error, but did not get it!"
        if mode in ["false", "warn"]:
            assert f"CREATE KEYSPACE {self.test_keyspace_ss}" in out, f"Expected to get the DDL for keyspace " \
                                                                      f"'{self.test_keyspace_ss}', but didn't find it!"

        self.create_test_keyspace(node=node, keyspace=self.test_keyspace_nts,
                                  replication_strategy=self.network_topology_strategy)

        out, err = self.alter_test_keyspace(node=node, keyspace=self.test_keyspace_nts,
                                            replication_strategy=self.simple_strategy)

        logger.info("Looking for the error/warning messages after the altering of keyspace '%s'...",
                    self.test_keyspace_nts)
        if mode == "true":
            assert "ConfigurationException" in err, "Expected to get 'ConfigurationException', but did not get it!"
        if mode == "false":
            assert not out and not err, "Expected not to get any errors/warnings, but got something!"
        if mode == "warn":
            assert "Warnings" in out, "Expected to get the warning message in the output, did not get it!"

        logger.info("Checking if the new keyspace %s was actually altered to use replication strategy %s...",
                    self.test_keyspace_nts, self.simple_strategy)
        out, err = self.describe_keyspace(node=node, keyspace=self.test_keyspace_nts)
        if mode == "true":
            assert self.network_topology_strategy in out, f"Expected to get '{self.network_topology_strategy}' "\
                                                          f"in the command output, but did not get it!"
        if mode in ["false", "warn"]:
            assert self.simple_strategy in out, f"Expected to get '{self.simple_strategy}' in the command output, " \
                                                f"but did not get it!"

    @pytest.mark.single_node
    def test_create_and_alter_keyspace_when_value_true(self):
        """
        Run the generic test scenario for 'restrict_replication_simplestrategy' = 'true'
        """
        self.run_parametrized_test(mode="true")

    @pytest.mark.single_node
    def test_create_and_alter_keyspace_when_value_false(self):
        """
        Run the generic test scenario for 'restrict_replication_simplestrategy' = 'false'
        """
        self.run_parametrized_test(mode="false")

    @pytest.mark.single_node
    def test_create_and_alter_keyspace_when_value_warn(self):
        """
        Run the generic test scenario for 'restrict_replication_simplestrategy' = 'warn'
        """
        self.run_parametrized_test(mode="warn")
