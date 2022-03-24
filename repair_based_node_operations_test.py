import logging
from dataclasses import dataclass
from typing import Callable, Optional

import pytest as pytest
from cassandra.cluster import ConsistencyLevel

from ccmlib.scylla_node import ScyllaNode
from dtest_class import Tester, create_ks, create_cf
from dtest_setup import DTestSetup
from tools.data import insert_c1c2
from tools.retrying import retrying

logger = logging.getLogger(__name__)


@dataclass
class RBNOperation:
    operation: Callable[[], ScyllaNode]
    operation_name: str
    repair_on_tested_node: bool
    repair_on_all_nodes: bool


class RepairBasedNodeOperationsScenarios:
    # Cover for feature: https://github.com/scylladb/scylla/commit/97bb2e47ff004b32b2d72f1b1f085710a14cb4e2

    @pytest.fixture(autouse=True)
    def fixture_add_additional_log_patterns(self, fixture_dtest_setup: DTestSetup):
        fixture_dtest_setup.ignore_log_patterns += [r'.*Could not retrieve CDC streams']

    def __init__(self, tester: Tester):
        # RBNO supports 5 operations: bootstrap, replace, removenode, decommission and rebuild
        # Define default behaviour for RBNO for all supported operations

        # replace:
        # It is used to replace a dead node. The token ring does not change. Replacing node pulls data from only
        # one of the replicas.
        self.replace_scenario = RBNOperation(operation=self.replace_node,
                                             operation_name="replace",  # used for validation in the log
                                             repair_on_tested_node=True,
                                             repair_on_all_nodes=False)

        # replace:
        # replace with allow ignoring dead nodes:
        # Three nodes are dead. Replacing node is allowed when there are two dead nodes.
        self.replace_with_dead_nodes_scenario = RBNOperation(operation=self.replace_node_when_two_nodes_dead,
                                                             operation_name="replace",  # used for validation in the log
                                                             repair_on_tested_node=True,
                                                             repair_on_all_nodes=False)
        # bootstrap:
        # It is used to add a new node into the cluster. The token ring changes.
        # New node pulls data from existing nodes that are losing the token ranges.
        self.bootstrap_scenario = RBNOperation(operation=self.bootstrap,
                                               operation_name="bootstrap",
                                               repair_on_tested_node=True,
                                               repair_on_all_nodes=False)
        # removenode:
        # It is used to remove a dead node out of the cluster. Existing nodes pull data from other existing nodes
        # for the new ranges it owns. It pulls from one of the replicas which might not be the latest copy.
        self.removenode_scenario = RBNOperation(operation=self.removenode,
                                                operation_name="removenode",
                                                repair_on_tested_node=False,
                                                repair_on_all_nodes=True)
        # decommission:
        # It is used to remove a live node from the cluster. Token ring changes. It does not suffer from the
        # “latest replica” issue. The leaving node pushes data to existing nodes.
        self.decommission_scenario = RBNOperation(operation=self.decommission,
                                                  operation_name="decommission",
                                                  repair_on_tested_node=True,
                                                  repair_on_all_nodes=False)
        # rebuild:
        # It is used to get all the data this node owns from other existing nodes.
        # It pulls data from only one of the replicas which might not be the latest copy.
        self.rebuild_scenario = RBNOperation(operation=self.rebuild,
                                             operation_name="rebuild",
                                             repair_on_tested_node=True,
                                             repair_on_all_nodes=False)

        self.tester = tester
        self.operations_flow = [self.rebuild_scenario, self.removenode_scenario,
                                self.bootstrap_scenario, self.replace_scenario, self.decommission_scenario]

    def add_node(self, dc: int = 0, replace_address: str = None, is_seed: bool = False,
                 ignore_dead_node_ip: str = '') -> ScyllaNode:
        new_node_index = int(self.tester.cluster.nodelist()[-1].name.replace("node", "")) + 1
        new_node = self.tester.cluster.new_node(i=new_node_index, data_center=dc, is_seed=is_seed)

        jvm_args = self.tester.jvm_args
        if ignore_dead_node_ip:
            jvm_args.extend(['--ignore-dead-nodes-for-replace', ignore_dead_node_ip])

        new_node.start(wait_for_binary_proto=True, wait_other_notice=True, jvm_args=jvm_args,
                       replace_address=replace_address)
        return new_node

    def replace_node(self, dc: int = 0) -> ScyllaNode:
        replaced_node = self.tester.cluster.nodelist()[-1]
        replaced_node_address = replaced_node.address()
        logger.debug(f"Remove {replaced_node.name} ({replaced_node_address})")
        replaced_node.stop(wait_other_notice=True)

        logger.debug("Add new node")
        new_node = self.add_node(replace_address=replaced_node_address, dc=dc)
        logger.debug(f"Added new node {new_node.name} ({new_node.address()})")

        self.tester.cluster.remove(node=replaced_node, wait_other_notice=True, remove_node_dir=False)

        return new_node

    def replace_node_when_two_nodes_dead(self, dc: int = 0) -> ScyllaNode:
        replaced_node, dead_node1, dead_node2 = self.tester.cluster.nodelist()[-3:]
        replaced_node_address = replaced_node.address()
        dead_node1_address = dead_node1.address()
        dead_node2_address = dead_node2.address()
        logger.debug(f"Stop 3 nodes: {replaced_node_address}, {dead_node1_address}, {dead_node2_address}")
        for node in [replaced_node, dead_node1, dead_node2]:
            node.stop(wait_other_notice=True)

        logger.debug(f"Add a new node that replaces {replaced_node_address}")
        new_node = self.add_node(replace_address=replaced_node_address, dc=dc,
                                 ignore_dead_node_ip=f"{dead_node1_address},{dead_node2_address}")
        logger.debug(f"Added the new node {new_node.name} ({new_node.address()})")

        self.tester.cluster.remove(node=replaced_node, wait_other_notice=True, remove_node_dir=False)

        logger.debug(f"Start {dead_node1_address} and {dead_node2_address} nodes")
        for node in [dead_node1, dead_node2]:
            node.start(wait_other_notice=True, wait_for_binary_proto=True)

        return new_node

    def bootstrap(self, dc: int = 0) -> ScyllaNode:
        logger.debug("Add new node")
        new_node = self.add_node(dc=dc)
        logger.debug(f"Added new node {new_node.name} ({new_node.address()})")
        return new_node

    def removenode(self) -> Optional[ScyllaNode]:
        remove_node = self.tester.cluster.nodelist()[-1]
        remove_node_host_id = remove_node.hostid()
        logger.debug(f"Remove node {remove_node.name} (host id {remove_node_host_id})")
        self.tester.cluster.nodelist()[0].removenode(remove_node_host_id)
        logger.debug(f"Node {remove_node.name} (host id {remove_node_host_id}) removed")

        return remove_node

    def decommission(self) -> Optional[ScyllaNode]:
        decommission_node = self.tester.cluster.nodelist()[-1]
        logger.debug(f"Decommission {decommission_node.name}")
        decommission_node.decommission()
        decommission_node.stop()
        logger.debug(f"Decommissioned {decommission_node.name}")

        return decommission_node

    def rebuild(self) -> ScyllaNode:
        rebuild_node = self.tester.cluster.nodelist()[0]
        logger.debug(f'Running nodetool rebuild on {rebuild_node.name}')
        rebuild_node.nodetool('rebuild')
        logger.debug('Rebuild completed successfully')

        return rebuild_node

    @staticmethod
    def validate_repair_by_scenario(node: ScyllaNode, repair_expected: bool, search_string: str,
                                    mark_log: int = 0):
        node_name = getattr(node, 'name') or str(node)
        logger.debug(f"Validate that repair based node operation on the node {node_name}")
        found_expr = node.grep_log(expr=search_string, from_mark=mark_log)
        if repair_expected:
            assert found_expr, f"Repair based node ops was not started on the node {node_name} as expected"
        else:
            assert not found_expr, f"Repair based node ops was started on the node {node_name} unexpectedly"

    def run_scenarios(self, rbno_enabled: bool, lcs: bool = False, scenarios: list = None, ):
        scenarios = scenarios or self.operations_flow

        for scenario in scenarios:
            search_string = rf"repair_reason={scenario.operation_name},"
            mark_all_logs = [{"name": node.name, "mark": node.mark_log()} for node in self.tester.cluster.nodelist()]

            logger.debug(f"Start {scenario.operation_name} scenario")
            tested_node = scenario.operation()
            logger.debug(f"Scenario {scenario.operation_name} completed. Start validation")

            for node in self.tester.cluster.nodelist():
                if node == tested_node:
                    # If test runs with --enable-repair-based-node-ops is false, default behaviour will be changed and
                    # repair by node won't be started for any operation even for default "replace" operation
                    repair_expected = rbno_enabled if not rbno_enabled else scenario.repair_on_tested_node
                    self.validate_repair_by_scenario(node=tested_node, repair_expected=repair_expected,
                                                     search_string=search_string)
                else:
                    repair_expected = rbno_enabled if not rbno_enabled else scenario.repair_on_all_nodes
                    mark_log = [mark["mark"] for mark in mark_all_logs if mark["name"] == node.name][0]
                    self.validate_repair_by_scenario(node=node, repair_expected=repair_expected,
                                                     search_string=search_string, mark_log=mark_log)
            if lcs and scenario.operation_name in ["bootstrap", "replace"]:
                logger.debug("Validate LCS reshaping efficiency")
                assert tested_node.grep_log(
                    r"LeveledManifest - Reshaping \d+ disjoint sstables in level 0 into level \d+"
                ), "Reshaping was ran in inefficient way"

            if scenario.operation_name in ["decommission"]:
                self.tester.cluster.remove(node=tested_node, wait_other_notice=True, remove_node_dir=False)


@pytest.mark.dtest_full
class TestRepairBasedNodeOperations(Tester):
    jvm_args = None

    def prepare_cluster(self, nodes: int, enable_repair_based_node_ops: bool = None,
                        allowed_repair_based_node_ops: str = None):
        # TODO: remove when https://github.com/scylladb/scylla/issues/10138 will be solved
        self.ignore_log_patterns += ["Could not find CDC generation"]
        logger.debug("Starting cluster...")

        jvm_args = []
        if enable_repair_based_node_ops is not None:
            jvm_args.extend([f'--enable-repair-based-node-ops', str(enable_repair_based_node_ops).lower()])

        if allowed_repair_based_node_ops:
            jvm_args.extend([f'--allowed-repair-based-node-ops', allowed_repair_based_node_ops])

        self.jvm_args = jvm_args

        self.cluster.populate(nodes).start(wait_for_binary_proto=True, wait_other_notice=True, jvm_args=jvm_args)

    def prepare_schema(self, node: ScyllaNode, rows: int = 1000, compaction_strategy='SizeTieredCompactionStrategy'):
        with self.patient_cql_connection(node) as session:
            create_ks(session, 'ks', 3)
            create_cf(
                session=session,
                name='cf',
                read_repair=0.0,
                columns={'c1': 'text', 'c2': 'text'},
                compaction_strategy=compaction_strategy,
            )

        logger.debug(f"Insert {rows} rows ...")
        with self.patient_exclusive_cql_connection(node, 'ks') as session1:
            insert_c1c2(session1, keys=range(rows), consistency=ConsistencyLevel.ONE)

        for current_node in self.cluster.nodelist():
            current_node.flush()

    def test_disable_rbno(self):
        """
        This test checks that if "--enable-repair-based-node-ops" is False, repair won't be started for all
        operations, supported by RBNO: bootstrap, replace, removenode, decommission and rebuild
        """
        enable_repair_based_node_ops = False
        self.prepare_cluster(nodes=3, enable_repair_based_node_ops=enable_repair_based_node_ops)
        self.prepare_schema(node=self.cluster.nodelist()[0])

        rbnos = RepairBasedNodeOperationsScenarios(tester=self)
        rbnos.run_scenarios(rbno_enabled=enable_repair_based_node_ops)

    def test_enable_rbno_for_default_operation(self):
        """
        By default, --allowed-repair-based-node-ops is set to contain "replace"

        This test checks that if "--enable-repair-based-node-ops" is True and "--allowed-repair-based-node-ops" is not
        set, repair will run in case of "replace" operation only
        """
        enable_repair_based_node_ops = True
        self.prepare_cluster(nodes=3, enable_repair_based_node_ops=enable_repair_based_node_ops)
        self.prepare_schema(node=self.cluster.nodelist()[0])

        rbnos = RepairBasedNodeOperationsScenarios(tester=self)
        # Change default expected behaviour according to enable-repair-based-node-ops and -allowed-repair-based-node-ops
        for scenario in [rbnos.rebuild_scenario, rbnos.removenode_scenario,
                         rbnos.bootstrap_scenario, rbnos.decommission_scenario]:
            scenario.repair_on_tested_node, scenario.repair_on_all_nodes = False, False

        rbnos.run_scenarios(rbno_enabled=enable_repair_based_node_ops, scenarios=rbnos.operations_flow)

    def test_enable_rbno_for_bootstrap(self):
        """
        By default, --allowed-repair-based-node-ops is set to contain "replace"

        This test checks that if "--enable-repair-based-node-ops" is True and "--allowed-repair-based-node-ops" is set
        to "bootstrap", repair will run in case of "bootstrap" operation only
        """
        enable_repair_based_node_ops = True
        self.prepare_cluster(nodes=3, enable_repair_based_node_ops=enable_repair_based_node_ops,
                             allowed_repair_based_node_ops="bootstrap")
        self.prepare_schema(node=self.cluster.nodelist()[0])

        rbnos = RepairBasedNodeOperationsScenarios(tester=self)
        # Change default expected behaviour according to enable-repair-based-node-ops and -allowed-repair-based-node-ops
        for scenario in [rbnos.rebuild_scenario, rbnos.removenode_scenario,
                         rbnos.replace_scenario, rbnos.decommission_scenario]:
            scenario.repair_on_tested_node, scenario.repair_on_all_nodes = False, False

        rbnos.run_scenarios(rbno_enabled=enable_repair_based_node_ops, scenarios=rbnos.operations_flow)

    def test_enable_rbno_for_all_operations(self):
        """
        By default, --allowed-repair-based-node-ops is set to contain "replace"

        This test checks that if "--enable-repair-based-node-ops" is True and "--allowed-repair-based-node-ops" is set
         to all supported operations, repair will run during all operations
        """
        enable_repair_based_node_ops = True
        self.prepare_cluster(nodes=3, enable_repair_based_node_ops=enable_repair_based_node_ops,
                             allowed_repair_based_node_ops="bootstrap,replace,removenode,decommission,rebuild")
        self.prepare_schema(node=self.cluster.nodelist()[0])

        rbnos = RepairBasedNodeOperationsScenarios(tester=self)
        rbnos.run_scenarios(rbno_enabled=enable_repair_based_node_ops)

    def test_lcs_reshape_efficiency(self):
        """
        For repair-based bootstrap/replace, the input disjoint run is now efficiently reshaped into an ideal level L,
        so there's no compaction backlog once reshape completes.

        This behavior will manifest in the log as this:

            LeveledManifest - Reshaping 256 disjoint sstables in level 0 into level 2

        """
        enable_repair_based_node_ops = True
        self.prepare_cluster(nodes=3,
                             enable_repair_based_node_ops=enable_repair_based_node_ops,
                             allowed_repair_based_node_ops="bootstrap,replace")
        self.prepare_schema(node=self.cluster.nodelist()[0], compaction_strategy='LeveledCompactionStrategy')

        rbnos = RepairBasedNodeOperationsScenarios(tester=self)
        rbnos.run_scenarios(rbno_enabled=enable_repair_based_node_ops,
                            lcs=True,
                            scenarios=[rbnos.bootstrap_scenario, rbnos.replace_scenario])

    def test_ignore_dead_nodes_for_replace_option(self):
        """
        --ignore-dead-nodes-for-replace was added by commit
        https://github.com/scylladb/scylla/commit/eba4a4fba4e6f203742307c16448034f40713711

        This option allows ignoring dead nodes for replace operation.
        If this option is not set, replace operation will fail because one node is down.
        """
        enable_repair_based_node_ops = True
        self.prepare_cluster(nodes=5, enable_repair_based_node_ops=enable_repair_based_node_ops)
        self.prepare_schema(node=self.cluster.nodelist()[0])

        rbnos = RepairBasedNodeOperationsScenarios(tester=self)
        rbnos.run_scenarios(rbno_enabled=enable_repair_based_node_ops,
                            scenarios=[rbnos.replace_with_dead_nodes_scenario])
