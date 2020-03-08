from datetime import datetime
from time import sleep
import requests

from nose.plugins.attrib import attr
from dtest import Tester, debug


@attr('dtest-full')
class TestScyllaARestApi(Tester):
    def config_and_create_cluster(self, nodes):
        self.cluster.populate(nodes).start(wait_for_binary_proto=True, wait_other_notice=True)
        return self.cluster.nodelist()

    @staticmethod
    def request_uptime(node):
        url = f"http://{node.address()}:10000/system/uptime_ms"
        node_uptime = int(requests.get(url=url).text)
        return node_uptime

    def test_basic_rest_uptime(self):
        node1 = self.config_and_create_cluster(1)[0]
        previous_node1_uptime = self.request_uptime(node1)
        sleep(10)
        current_node1_uptime = self.request_uptime(node1)
        debug(current_node1_uptime - previous_node1_uptime)
        assert 10000 < current_node1_uptime - previous_node1_uptime < 11000, \
            "The uptime received from scylla does not match the expected uptime"

    def test_rest_uptime_after_restart(self):
        test_start_time = datetime.now()
        node1 = self.config_and_create_cluster(1)[0]
        sleep(10)
        node1.stop(wait_other_notice=False)
        node1.start(wait_other_notice=False, wait_for_binary_proto=True)

        renewed_node1_uptime = self.request_uptime(node1)
        now = datetime.now()
        assert (now - test_start_time).total_seconds() - 10 > renewed_node1_uptime/1000, \
            f"The uptime received from scylla does not match the expected uptime\nreceived uptime:{renewed_node1_uptime}"

    def test_compare_node_uptime(self):
        node1, node2 = self.config_and_create_cluster(2)
        sleep(10)
        node1.stop(wait_other_notice=False)
        node1.start(wait_other_notice=False, wait_for_binary_proto=True)

        node1_uptime = self.request_uptime(node1)
        node2_uptime = self.request_uptime(node2)
        assert node2_uptime - node1_uptime > 10000, \
            f"The difference between the nodes' uptime did not match expectations\nnode1: {node1_uptime}\nnode2: {node2_uptime}"
