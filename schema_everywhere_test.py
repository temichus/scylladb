import re
import pytest

from cassandra.cluster import Session, ConsistencyLevel
from cassandra.query import SimpleStatement
from ccmlib.scylla_cluster import ScyllaCluster
from ccmlib.scylla_node import ScyllaNode


from dtest_class import Tester


@pytest.mark.dtest_full
class TestSchemaReplicationEverywhereStrategy(Tester):

    SYSTEM_DISTRIBUTED_EVERYWHERE = "system_distributed_everywhere"
    REPLICATION_EVERYWHERE_PATTERN = r"replication\s=\s{'class':\s'EverywhereStrategy'}"

    @pytest.mark.single_node
    def test_replication_strategy_name_in_description(self):
        cluster: ScyllaCluster = self.cluster
        cluster.populate(nodes=1).start()
        node: ScyllaNode = cluster.nodelist()[0]
        stdout, stderr = node.run_cqlsh(cmds=f"DESC KEYSPACE {self.SYSTEM_DISTRIBUTED_EVERYWHERE}", return_output=True)

        assert re.search(self.REPLICATION_EVERYWHERE_PATTERN, str(stdout), re.MULTILINE), \
            f"{self.SYSTEM_DISTRIBUTED_EVERYWHERE} doesn't have EverywhereStrategy replication.\n{stdout}, {stderr}"

        session: Session = self.patient_cql_connection(node)
        ks = session.cluster.metadata.keyspaces[self.SYSTEM_DISTRIBUTED_EVERYWHERE]

        assert ks.replication_strategy.name == "EverywhereStrategy", \
            f"{self.SYSTEM_DISTRIBUTED_EVERYWHERE} doesn't have EverywhereStrategy replication"

    def test_creating_new_table(self):
        """
        Create new table in system_distributed_everywhere
        keyspace and verify that data replicated to each node
        """
        cluster: ScyllaCluster = self.cluster
        cluster.populate(nodes=2).start(wait_for_binary_proto=True)
        node1: ScyllaNode = cluster.nodelist()[0]
        node2: ScyllaNode = cluster.nodelist()[1]

        self.create_and_fill_user_table(node1)
        node1.stop(wait_other_notice=True)
        self.verify_data_in_user_table(node2)

        node1.start(wait_for_binary_proto=True)
        node2.stop()
        self.verify_data_in_user_table(node1)

    def test_replicating_user_table_to_new_node(self):
        """
        Verify that user table in system_distributed_everywhere ks
        replicated to new nodes
        """
        cluster: ScyllaCluster = self.cluster
        cluster.populate(nodes=2).start(wait_for_binary_proto=True)
        node1: ScyllaNode = cluster.nodelist()[0]
        node2: ScyllaNode = cluster.nodelist()[1]
        self.create_and_fill_user_table(node1)
        node3 = cluster.new_node(3, auto_bootstrap=True)
        node3.start(wait_for_binary_proto=True)
        node1.stop(wait_other_notice=True)
        node2.stop(wait_other_notice=True)
        self.verify_data_in_user_table(node3)
        node2.start()
        node4 = cluster.new_node(4, auto_bootstrap=True, initial_token=None, is_seed=False)
        node4.start(wait_for_binary_proto=True, replace_address=node1.address())
        node2.stop()
        node3.stop()
        self.verify_data_in_user_table(node4)

    def test_replicating_user_table_in_mutlidc(self):
        """
        Verify that user table in system_distributed_everywhere ks
        replication in multi dc
        """
        cluster: ScyllaCluster = self.cluster
        cluster.populate([2, 2]).start(wait_for_binary_proto=True, wait_other_notice=True)
        node1: ScyllaNode = cluster.nodelist()[0]
        self.create_and_fill_user_table(node1)
        cluster.stop_nodes()
        for running_node in cluster.nodelist():
            running_node.start()
            self.verify_data_in_user_table(running_node)
            running_node.stop()

    def create_and_fill_user_table(self, node):
        session: Session = self.patient_exclusive_cql_connection(node)
        session.execute(
            f"CREATE TABLE {self.SYSTEM_DISTRIBUTED_EVERYWHERE}.test1 (key varchar PRIMARY KEY, c1 varchar, c2 varchar)")
        write_query = SimpleStatement(
            f"INSERT INTO {self.SYSTEM_DISTRIBUTED_EVERYWHERE}.test1 (key, c1, c2) VALUES ('key1', 'txt1', 'txt2')")
        write_query.consistency_level = ConsistencyLevel.ALL
        session.execute(write_query)

    def verify_data_in_user_table(self, node):
        session: Session = self.patient_exclusive_cql_connection(node)
        read_query = SimpleStatement(f"Select * from {self.SYSTEM_DISTRIBUTED_EVERYWHERE}.test1;")
        read_query.consistency_level = ConsistencyLevel.ONE
        res = list(session.execute(read_query))

        assert len(res) == 1, f"Wrong number of rows: {res}"
        assert res[0].c1 == "txt1" and res[0].c2 == "txt2", f"Row {res[0]} has wrong column values"
