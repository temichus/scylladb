import logging
import time

import pytest
from cassandra import ConsistencyLevel
from cassandra.policies import WhiteListRoundRobinPolicy
from cassandra.query import SimpleStatement

from dtest_class import Tester, create_ks
from tools.packet_analyzer import PacketAnalyzer

logger = logging.getLogger(__name__)


@pytest.mark.dtest_full
class TestInternodeCompression(Tester):
    def test_internode_compression_compress_packets_between_nodes(self) -> None:
        """
        Verify that internode compression compress internode packets.
        """
        cluster = self.cluster
        cluster.set_configuration_options(values={"internode_compression": "all"})
        cluster.populate([2]).start()
        node1, node2 = cluster.nodelist()

        session = self.patient_cql_connection(node1)
        create_ks(session, "ks", 2)
        session.execute("CREATE TABLE ks.cf (key int PRIMARY KEY, val TEXT)")

        msg_size = 8192
        insert_stmt = session.prepare("insert into ks.cf (key, val) values (?, ?)")
        insert_stmt.consistency_level = ConsistencyLevel.ALL

        def insert_once():
            session.execute(insert_stmt, [1, "1" * msg_size])

        while True:
            packet_analyzer_1 = PacketAnalyzer(node1, node2)
            packet_analyzer_2 = PacketAnalyzer(node2, node1)  # other direction

            packet_analyzer_1.start()
            packet_analyzer_2.start()
            time.sleep(1)  # Let tcpdump start.

            # insert a compressible row of size ~8 kiB
            insert_once()

            time.sleep(1)  # Let tcpdump print everything it's got.
            packet_analyzer_1.stop()
            packet_analyzer_2.stop()

            biggest_packet_length_1 = packet_analyzer_1.get_max_packet_length()
            biggest_packet_length_2 = packet_analyzer_2.get_max_packet_length()

            actual = max(biggest_packet_length_1, biggest_packet_length_2)
            expected = msg_size / 2
            if actual > expected:
                logger.info(
                    f"RPC frame wasn't compressed. Max expected size: {expected}, actual size: {actual}. Retrying.")
                continue
            break

    def test_internode_compression_between_datacenters(self) -> None:
        """
        Verify that compression between datacenters is compressed if internode_compression is set to dc and
        not compressed for intra-dc communication.
        """
        cluster = self.cluster
        cluster.set_configuration_options(values={"internode_compression": "dc"})
        cluster.populate([2, 1]).start()

        node1, node2 = cluster.nodelist()[:2]
        node3 = cluster.nodelist()[2]
        session = self.patient_cql_connection(node1, load_balancing_policy=WhiteListRoundRobinPolicy([node1.address()]))
        create_ks(session, "ks", {"dc1": 2, "dc2": 1})
        session.execute("CREATE TABLE ks.cf (key int PRIMARY KEY, val TEXT)")

        msg_size = 8192
        insert_stmt = session.prepare("insert into ks.cf (key, val) values (?, ?)")
        insert_stmt.consistency_level = ConsistencyLevel.ALL

        def insert_once():
            session.execute(insert_stmt, [1, "1" * msg_size])

        while True:
            # start tcpdump sniffing on traffic between node1 and node2 on port 7000
            intra_dc_packet_analyzer_1 = PacketAnalyzer(node1, node2)
            intra_dc_packet_analyzer_2 = PacketAnalyzer(node2, node1)

            dc_packet_analyzer_1 = PacketAnalyzer(node1, node3)
            dc_packet_analyzer_2 = PacketAnalyzer(node2, node3)

            intra_dc_packet_analyzer_1.start()
            intra_dc_packet_analyzer_2.start()

            dc_packet_analyzer_1.start()
            dc_packet_analyzer_2.start()
            time.sleep(1)  # Let tcpdump start.

            # insert a compressible row of size ~8 kiB
            insert_once()

            time.sleep(1)  # Let tcpdump print everything it's got.
            intra_dc_packet_analyzer_1.stop()
            intra_dc_packet_analyzer_2.stop()
            dc_packet_analyzer_1.stop()
            dc_packet_analyzer_2.stop()

            biggest_packet_length_1 = dc_packet_analyzer_1.get_max_packet_length()
            biggest_packet_length_2 = dc_packet_analyzer_2.get_max_packet_length()
            max_intra_packet_length = max(intra_dc_packet_analyzer_1.get_max_packet_length(),
                                          intra_dc_packet_analyzer_2.get_max_packet_length())

            expected = msg_size / 2
            actual = max(biggest_packet_length_1, biggest_packet_length_2)
            if actual > expected:
                logger.info(
                    f"An inter-DC RPC message wasn't compressed. Max expected size: {expected}, actual size: {actual}. Retrying.")
                continue
            assert max_intra_packet_length > msg_size, f"An intra-DC RPC message was compressed unexpectedly. Min expected size: {msg_size}, actual size: {max_intra_packet_length}"
            break
