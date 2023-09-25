import subprocess
import time
import re
import getpass

import pytest
from cassandra import ConsistencyLevel
from cassandra.cluster import DCAwareRoundRobinPolicy
from cassandra.query import SimpleStatement

from dtest_class import Tester, create_ks, get_ip_from_node, logger


class PacketAnalyzer:
    """Class for analyzing packets between two nodes using tcpdump utility"""
    packet_lenght_regexp = re.compile(r'length (\d+)')

    def __init__(self, source, destination):
        self.source = source
        self.destination = destination
        self._tcpdump_process = None
        self._captured_packets = []

    def start(self):
        cmd = (f"sudo tcpdump -Z {getpass.getuser()} -i lo -n "
               f"src host {get_ip_from_node(self.source)} and dst host {get_ip_from_node(self.destination)} and port 7000")
        logger.debug(f"Starting tcpdump with command: {cmd}")
        self._tcpdump_process = subprocess.Popen(
            cmd.split(), stderr=subprocess.PIPE, stdout=subprocess.PIPE, universal_newlines=True)
        self._tcpdump_process.stdout.readline()  # Wait for tcpdump to start
        logger.debug("Tcpdump started")

    def stop(self):
        logger.debug("Stopping tcpdump")
        subprocess.Popen(f"sudo pkill -P {self._tcpdump_process.pid}", shell=True).wait()
        self._tcpdump_process.wait()
        logger.debug("Tcpdump stopped")
        self._captured_packets = self._tcpdump_process.stdout.readlines()
        logger.debug(f"stderr: {self._tcpdump_process.stderr.read()}")
        logger.debug("Captured %s packets", len(self._captured_packets))

    def get_max_packet_length(self):
        return max([int(packet_lenght.group(1)) for packet in self._captured_packets
                    if (packet_lenght := self.packet_lenght_regexp.search(packet))])

    def get_packets_with_length(self, length):
        return [packet for packet in self._captured_packets
                if (packet_lenght := self.packet_lenght_regexp.search(packet)) and int(packet_lenght.group(1)) == length]


@pytest.mark.dtest_full
class TestInternodeCompression(Tester):

    def test_internode_compression_compress_packets_between_nodes(self):
        """
        Verify that internode compression compress internode packets.
        """
        cluster = self.cluster
        cluster.set_configuration_options(values={"internode_compression": "all"})
        cluster.populate([2]).start()
        node1, node2 = cluster.nodelist()

        session = self.patient_cql_connection(node1)
        create_ks(session, 'ks', 2)
        session.execute("CREATE TABLE ks.cf (key int PRIMARY KEY, val TEXT)")

        packet_analyzer_1 = PacketAnalyzer(node1, node2)
        packet_analyzer_2 = PacketAnalyzer(node2, node1)  # other direction

        packet_analyzer_1.start()
        packet_analyzer_2.start()

        # insert >8kb size (compressible) row and wait for tcpdump to analyze packets for several seconds
        session.execute(
            SimpleStatement(f"insert into ks.cf (key, val) values (1, '{'1' * 8192}')",
                            consistency_level=ConsistencyLevel.ALL))
        session.shutdown()
        time.sleep(1)  # wait for tcpdump to print packets
        packet_analyzer_1.stop()
        packet_analyzer_2.stop()

        biggest_packet_length_1 = packet_analyzer_1.get_max_packet_length()
        biggest_packet_length_2 = packet_analyzer_2.get_max_packet_length()
        if biggest_packet_length_1 > biggest_packet_length_2:
            biggest_packet = packet_analyzer_1.get_packets_with_length(biggest_packet_length_1)
        else:
            biggest_packet = packet_analyzer_2.get_packets_with_length(biggest_packet_length_2)
        assert max(biggest_packet_length_1, biggest_packet_length_2) < 8000, \
            f"max packet length is bigger than 8192 bytes - compression is not working properly. Packet data: {biggest_packet}"

    def test_internode_compression_between_datacenters(self):
        """
        Verify that compression between datacenters is compressed if internode_compression is set to dc and
        not compressed for intra-dc communication.
        """
        cluster = self.cluster
        cluster.set_configuration_options(values={"internode_compression": "dc"})
        cluster.populate([2, 1]).start()

        node1, node2 = cluster.nodelist()[:2]
        node3 = cluster.nodelist()[2]
        session = self.patient_cql_connection(node1, load_balancing_policy=DCAwareRoundRobinPolicy(local_dc='dc1'))
        create_ks(session, 'ks', {'dc1': 2, 'dc2': 1})
        session.execute("CREATE TABLE ks.cf (key int PRIMARY KEY, val TEXT)")

        # start tcpdump sniffing on traffic between node1 and node2 on port 7000
        intra_dc_packet_analyzer_1 = PacketAnalyzer(node1, node2)
        intra_dc_packet_analyzer_2 = PacketAnalyzer(node2, node1)

        dc_packet_analyzer_1 = PacketAnalyzer(node1, node3)
        dc_packet_analyzer_2 = PacketAnalyzer(node2, node3)
        intra_dc_packet_analyzer_1.start()
        intra_dc_packet_analyzer_2.start()
        dc_packet_analyzer_1.start()
        dc_packet_analyzer_2.start()
        # insert >1kb size (compressible) row and wait for tcpdump to analyze packets for several seconds
        session.execute(
            SimpleStatement(f"insert into ks.cf (key, val) values (1, '{'1' * 8192}')",
                            consistency_level=ConsistencyLevel.ALL))
        session.shutdown()
        time.sleep(1)  # wait for tcpdump to print packets
        intra_dc_packet_analyzer_1.stop()
        intra_dc_packet_analyzer_2.stop()
        dc_packet_analyzer_1.stop()
        dc_packet_analyzer_2.stop()
        biggest_packet_length_1 = dc_packet_analyzer_1.get_max_packet_length()
        biggest_packet_length_2 = dc_packet_analyzer_2.get_max_packet_length()
        if biggest_packet_length_1 > biggest_packet_length_2:
            biggest_packet = dc_packet_analyzer_1.get_packets_with_length(biggest_packet_length_1)
        else:
            biggest_packet = dc_packet_analyzer_2.get_packets_with_length(biggest_packet_length_2)
        assert max(intra_dc_packet_analyzer_1.get_max_packet_length(), intra_dc_packet_analyzer_2.get_max_packet_length()) > 8192, \
            "intra-datacenter was compressed but shouldn't be"
        assert max(biggest_packet_length_1, biggest_packet_length_2) < 8000, \
            f"between datacenters communication should be compressed but it wasn't. Packet data: {biggest_packet}"
