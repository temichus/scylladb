import time

import pytest
from cassandra import ConsistencyLevel
from cassandra.cluster import DCAwareRoundRobinPolicy
from cassandra.query import SimpleStatement

from dtest_class import Tester, create_ks
from tools.packet_analyzer import PacketAnalyzer


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
