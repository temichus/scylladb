from dtest_scylla_manager import ScyllaManagerTool, ScyllaManagerError
from dtest import Tester, debug


class TestScyllaManagerClusterMgmt(Tester):

    def adding_cluster_while_its_down_test(self):
        self.cluster.populate(3).start()
        node1, node2, node3 = self.cluster.nodelist()

        debug("Create Manager Tool instance to run scylla-manager operations")
        manager_tool = ScyllaManagerTool(scylla_manager=self.cluster._scylla_manager)
        cluster_name = "cluster1"
        debug("trying to add an offline cluster to scylla-manager, named: {}".format(cluster_name))
        self.cluster.stop()

        try:
            manager_tool.add_cluster(node=node1, name=cluster_name)
        except ScyllaManagerError as err:
            if "connection refused" not in err.args[0].lower():
                assert False, "Received an irrelevant ScyllaManagerError when trying to add an offline cluster"
            return
        assert False, "Expected to fail when adding an offline cluster to the manager, but didn't"
