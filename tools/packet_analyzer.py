import re
import getpass
import subprocess
import logging
from pathlib import Path
from contextlib import contextmanager

from ccmlib.node import Node

from dtest_class import get_ip_from_node


logger = logging.getLogger(__name__)


class PacketAnalyzer:
    """Class for analyzing packets between two nodes using tcpdump utility"""
    packet_length_regexp = re.compile(r'length (\d+)')

    def __init__(self, source: Node = None, destination: Node = None, port: int = 7000, output_dir: str | Path = None):
        self.source = source
        self.destination = destination
        self.port = port
        self.output_path = Path(
            output_dir) / f"tcpdump_{source.address() if source else ''}_{destination.address() if destination else ''}_{port}.pcap" if output_dir else output_dir
        self._tcpdump_process = None
        self._captured_packets = []

    def start(self):
        cmd = f"sudo tcpdump -Z {getpass.getuser()} -i lo -n "
        filter = []
        if self.source:
            filter += [f"src host {get_ip_from_node(self.source)}"]
        if self.destination:
            filter += [f"dst host {get_ip_from_node(self.destination)}"]
        if self.port:
            filter += [f"port {self.port}"]

        cmd += " and ".join(filter)

        if self.output_path:
            cmd += f" -w {self.output_path}"
        logger.debug(f"Starting tcpdump with command: {cmd}")
        self._tcpdump_process = subprocess.Popen(
            cmd.split(), stderr=subprocess.PIPE, stdout=subprocess.PIPE, universal_newlines=True)
        self._tcpdump_process.stderr.readline()  # Wait for tcpdump to start
        logger.debug("Tcpdump started")

    def stop(self):
        logger.debug("Stopping tcpdump")
        subprocess.Popen(f"sudo pkill -P {self._tcpdump_process.pid}", shell=True).wait()
        self._tcpdump_process.wait()
        logger.debug("Tcpdump stopped")
        self._captured_packets = self._tcpdump_process.stdout.readlines()
        logger.debug(f"stderr: {self._tcpdump_process.stderr.read()}")

    def get_max_packet_length(self):
        return max([int(packet_length.group(1)) for packet in self._captured_packets
                    if (packet_length := self.packet_length_regexp.search(packet))])

    def get_packets_with_length(self, length):
        return [packet for packet in self._captured_packets
                if (packet_length := self.packet_length_regexp.search(packet)) and int(packet_length.group(1)) == length]


@contextmanager
def capture_cql_ports(nodes: list[Node], output_dir: str | Path, ports=[9042, 19042]):
    """
    context manager to capture cql communication

    Usage:

        with capture_cql_ports(nodes=self.cluster.nodelist(), output_dir=self.cluster.get_path()):
            # test code part we want cql to be captured

    """
    packet_analyzers = []
    for node in nodes:
        for port in ports:
            packet_analyzers += [PacketAnalyzer(destination=node,
                                                port=port, output_dir=output_dir)]

    for p in packet_analyzers:
        p.start()

    yield

    for p in packet_analyzers:
        p.stop()
