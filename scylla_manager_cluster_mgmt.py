import socket

from dtest_scylla_manager import ScyllaManagerTool
from dtest import Tester, debug


class ScyllaManagerClusterMgmt(Tester):

    def add_more_than_one_scylla_cluster(self):
        self.cluster.populate(3).start(wait_for_binary_proto=False, wait_other_notice=False)
        node1, node2, node3 = self.cluster.nodelist()

        manager_tool = ScyllaManagerTool(scylla_manager=self.cluster._scylla_manager)

        cluster_name1 = "cluster1"
        cluster_name2 = "cluster2"
        debug("Add a cluster to scylla-manager, named: {}".format(cluster_name1))
        manager_tool.add_cluster(node=node1, name=cluster_name1)

        debug("Add a cluster to scylla-manager, named: {}".format(cluster_name2))
        manager_tool.add_cluster(node=node2, name=cluster_name2)

        # TODO: add a comparison with expected result
        debug(manager_tool.cluster_list)

    def add_scylla_cluster_using_dns_name(self):
        self.cluster.populate(3).start(wait_for_binary_proto=False, wait_other_notice=False)
        node1, node2, node3 = self.cluster.nodelist()

        manager_tool = ScyllaManagerTool(scylla_manager=self.cluster._scylla_manager)

        cluster_name1 = "cluster1"
        debug("Add a cluster to scylla-manager, named: {}".format(cluster_name1))
        node1.hostname = socket.gethostname()
        manager_tool.add_cluster(node=node1, name=cluster_name1, by_name=True)

        # TODO: add a comparison with expected result
        debug(manager_tool.cluster_list)
