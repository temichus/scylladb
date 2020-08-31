# coding: utf-8

import time

from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement
from nose.plugins.attrib import attr
from unittest import skip

from dtest_scylla_manager import HostStatus, HostRestStatus, ScyllaManagerTool, ScyllaManagerError, ScyllaManagerMixin
from dtest import debug, WaitTimeoutExpired
from dtest_scylla_manager import TaskStatus
from scylla_tools import insert_c1c2
from assertions import assert_row_count, assert_all


from repair_additional_test import RepairAdditionalBase


class TestScyllaMgmtRepair(RepairAdditionalBase, ScyllaManagerMixin):
    __test__ = True
    KEYSPACE_NAME = 'ks'

    @attr('scylla-manager')
    def test_manager_sanity(self):
        self._initiate_cluster_with_data()
        node1, node2, node3 = self.cluster.nodelist()

        debug("Create Manager Tool instance to run scylla-manager operations")
        manager_tool = ScyllaManagerTool(scylla_manager=self.cluster._scylla_manager)
        cluster_name =  "cluster1"
        debug("Add a cluster to scylla-manager, named: {}".format(cluster_name))
        mgr_cluster = manager_tool.add_cluster(node=node1, name=cluster_name)

        debug("Test rename cluster from: {} to: {}".format(cluster_name, cluster_name+"_renamed"))
        cluster_orig_name = mgr_cluster.name
        mgr_cluster.update(name="{}_renamed".format(cluster_orig_name))
        assert mgr_cluster.name == cluster_orig_name+"_renamed", "Cluster name wasn't changed after update command"

        debug("Test cluster Repair task")
        mgr_task = mgr_cluster.create_repair_task()
        task_final_status = mgr_task.wait_and_get_final_status()
        assert task_final_status == TaskStatus.DONE, 'Task: {} final status is: {}.'.format(mgr_task.id, str(mgr_task.status))

        debug("Test cluster Health-Check task")
        healthcheck_task = mgr_cluster.get_healthcheck_task()
        debug("Health-check task history is: {}".format(healthcheck_task.history))
        dict_host_health = mgr_cluster.get_hosts_health()
        for host_health in dict_host_health.values():
            assert host_health.status == HostStatus.UP, "Not all hosts status is 'UP'"
            assert host_health.rest_status == HostRestStatus.UP, "Not all hosts REST status is 'UP'"

        # Check for sctool status change after scylla node down
        sleep = 20
        node2.stop(wait_other_notice=True)
        debug("Health-check next run is: {}".format(healthcheck_task.next_run))
        debug('Sleep {} seconds, waiting for health-check task to run after node down'.format(sleep))
        time.sleep(sleep)

        dict_host_health = mgr_cluster.get_hosts_health()
        assert dict_host_health[node2.address()].status == HostStatus.DOWN, "Host: {} status is not 'DOWN'".format(node2.address())
        assert dict_host_health[node2.address()].rest_status == HostRestStatus.DOWN, "Host: {} REST status is not 'DOWN'".format(node2.address())

        node2.start()

    # TODO: adjust (to dtest_scylla_manager.py) or delete all RepairAdditionalBase related tests
    @attr('scylla-manager')
    def repair_disjoint_data_test(self, more_options=[]):
        return RepairAdditionalBase._repair_disjoint_data_test(self,more_options)

    @attr('scylla-manager')
    def repair_schema_test(self):
        return RepairAdditionalBase._repair_schema_test(self)

    @attr('scylla-manager')
    def repair_schema_2_test(self):
        return RepairAdditionalBase._repair_schema_2_test(self)

    @attr('scylla-manager')
    def repair_cell_update_test(self):
       return RepairAdditionalBase._repair_cell_update_test(self)

    @attr('scylla-manager')
    def repair_cell_delete_test(self):
       return RepairAdditionalBase._repair_cell_delete_test(self)

    @attr('scylla-manager')
    def repair_row_delete_test(self):
       return RepairAdditionalBase._repair_row_delete_test(self)

    @attr('scylla-manager')
    def repair_partition_delete_test(self):
       return RepairAdditionalBase._repair_partition_delete_test(self)

    def _initiate_cluster_with_data(self):
        debug("Starting cluster and inserting data...")
        node1, node2, node3 = self.config_and_create_cluster(nodes=3)
        # Start a cluster of three nodes, and create a keyspace with RF=3, and

        session = self.patient_cql_connection(node1)
        self.create_ks(session, 'ks', 3)

        session.execute(
            "CREATE TABLE cf (name text, pet text, age int, PRIMARY KEY ((name), pet)) WITH compression = {} AND read_repair_chance = 0.0;")

        query = SimpleStatement("INSERT INTO cf (name, pet, age) VALUES ('nadav', 'kitty', 5)",
                                consistency_level=ConsistencyLevel.ALL)
        session.execute(query)
        query = SimpleStatement("INSERT INTO cf (name, pet, age) VALUES ('nadav', 'adamdami', 1)",
                                consistency_level=ConsistencyLevel.ALL)
        session.execute(query)

    def _insert_data_range_to_specific_node(self, node_to_insert, nodes_to_shut_down, keyspace_name, data_ranges):
        debug("Stopping the following node(s): {}".format([node.name for node in nodes_to_shut_down]))
        [node.stop(wait_other_notice=True) for node in nodes_to_shut_down]

        session = self.patient_cql_connection(node_to_insert, keyspace_name)
        for cf_name, cf_range in data_ranges.items():
            if keyspace_name is None:
                ks, cf = cf_name.split('.')
            else:
                ks, cf = keyspace_name, cf_name
            insert_c1c2(session=session, keys=cf_range, consistency=ConsistencyLevel.ONE,
                        c1_values=['value1']*len(cf_range), c2_values=['value2']*len(cf_range),
                        ks=ks, cf=cf)
        node_to_insert.flush()

        debug("Starting the following node(s) (again) : {}".format([node.name for node in nodes_to_shut_down]))
        [node.start(wait_other_notice=True, wait_for_binary_proto=True) for node in nodes_to_shut_down]

    def _assert_multiple_row_ranges_from_specific_node(self, node_to_query, nodes_to_shut_down, keyspace_name,
                                                       tables_and_row_count_dict):
        debug("Stopping the following nodes: {}".format([node.name for node in nodes_to_shut_down]))
        [node.stop(wait_other_notice=True) for node in nodes_to_shut_down]

        session = self.patient_cql_connection(node_to_query, keyspace=keyspace_name)
        for table_name, expected_row_range in tables_and_row_count_dict.items():
            expected_row_list = [['k{}'.format(key), 'value1', 'value2'] for key in expected_row_range]
            assert_all(session=session, query='select key, c1, c2 from {};'.format(table_name),
                       expected=expected_row_list, ignore_order=True)

        debug("Starting the following nodes (again) : {}".format([node.name for node in nodes_to_shut_down]))
        [node.start(wait_other_notice=True, wait_for_binary_proto=True) for node in nodes_to_shut_down]

    def _manager_repair_token_ranges_template(self, token_range):
        """
        Test the "partitioner range" (--token-ranges <token_range>) option. We start two nodes and a
        keyspace with RF=2, and put <num_of_keys> different rows on each of the nodes
        (as in repair_disjoint_data_set). Each node has in "partitioner ranges"
        only half the key space.
        So that starting a repair with "--token-ranges <token_range>" when token_range is either 'pr' or 'npr' on one
        node will bring in around <num_of_keys>/2 missing partitions, but the other <num_of_keys>/2
        will continue to be missing until we start a repair with "--token-ranges <token_range>" on the
        second node as well. After both repairs, each nodes should contain <num_of_keys>*2 rows, which means that both
        of them should contain all of the rows in the table.

        On the other hand, starting a repair with "--token-ranges <token_range>" when token_range is 'all' on one
        node should repair all of the rows on both nodes, so they both will contain all of the rows after the repair.
        """
        # Start a cluster of two nodes, and create a keyspace ks with RF=2, and a table cf.
        # Hinted handoff and read repair are disabled so they don't fix the problems which repair is suppose to fix.
        node1, node2 = self.config_and_create_cluster(nodes=2)
        session = self.patient_cql_connection(node1)
        self.create_ks(session, self.KEYSPACE_NAME, 2)
        self.create_cf(session, 'cf', read_repair=0.0, columns={'c1': 'text', 'c2': 'text'})

        num_of_keys = 1000
        delta = int(num_of_keys * 20 / 100)
        first_range_start = num_of_keys
        second_range_start = num_of_keys * 2
        second_range_end = second_range_start + num_of_keys
        # Insert 1000 keys *only* on node 1, another 1000 keys *only* on node 2:
        debug("Adding data only on node 1...")
        self._insert_data_range_to_specific_node(node_to_insert=node1, nodes_to_shut_down=[node2],
                                                 keyspace_name=self.KEYSPACE_NAME,
                                                 data_ranges={'cf': range(first_range_start, second_range_start)})
        debug("Adding data only on node 2...")
        self._insert_data_range_to_specific_node(node_to_insert=node2, nodes_to_shut_down=[node1],
                                                 keyspace_name=self.KEYSPACE_NAME,
                                                 data_ranges={'cf': range(second_range_start, second_range_end)})

        manager_tool = ScyllaManagerTool(scylla_manager=self.cluster._scylla_manager)
        cluster_name = "cluster1"
        debug("Add a cluster to scylla-manager, named: {}".format(cluster_name))
        mgr_cluster = manager_tool.add_cluster(node=node1, name=cluster_name)

        debug("Run partitioner-range repair on node 1")
        mgr_task = mgr_cluster.create_repair_task(node=node1, token_ranges=token_range, keyspace=self.KEYSPACE_NAME)

        task_final_status = mgr_task.wait_and_get_final_status()
        assert task_final_status == TaskStatus.DONE, 'Task: {} final status is: {}.'.format(mgr_task.id, str(mgr_task.status))

        if token_range != 'all':
            # We expect "--token_ranges <token_range>" repair to have repaired only half of the ranges
            # (those for which node 1 is their primary replica), so both nodes
            # should now have around 1.5 * keys-number partitions. We don't know the exact
            # number, but given the assumed random distribution of tokens and keys,
            # it is unlikely to be far from %20 delta  - let's assert it is between a delta around
            node1.flush()
            node1.stop(wait_other_notice=True)
            session = self.patient_cql_connection(node2, self.KEYSPACE_NAME)
            count = len(list(session.execute("SELECT * FROM cf LIMIT 2000")))
            self.assertTrue((first_range_start * 1.5 - delta) < count < (first_range_start * 1.5 + delta),
                            "expected {} repair to repair part, but not everything ({} keys exist)".format(
                                token_range, count))
            node1.start(wait_other_notice=True, wait_for_binary_proto=True)
            node2.flush()
            node2.stop(wait_other_notice=True)
            session = self.patient_cql_connection(node1, self.KEYSPACE_NAME)
            count = len(list(session.execute("SELECT * FROM cf LIMIT 2000")))
            self.assertTrue((first_range_start * 1.5 - delta) < count < (first_range_start * 1.5 + delta),
                            "expected {} repair to repair part, but not everything ({} keys exist)".format(
                                token_range, count))
            node2.start(wait_other_notice=True, wait_for_binary_proto=True)

            # Run a second "--token_ranges <token_range>" repair, this time on node 2. This should repair
            # all the ranges not previously repaired (i.e., this times the ranges
            # whose primary/ non-primary is node 2), and at the end, all data should be on both nodes.
            debug("Run partioner-range repair on node 2")
            mgr_task2 = mgr_cluster.create_repair_task(node=node2, token_ranges=token_range,
                                                       keyspace=self.KEYSPACE_NAME)

            task_final_status = mgr_task2.wait_and_get_final_status()
            assert task_final_status == TaskStatus.DONE, 'Task: {} final status is: {}.'.format(mgr_task2.id,
                                                                                                str(mgr_task.status))
        # Whether we ran 2 repairs with "--token_ranges pr/npr" on each node
        # or only one repair with "--token_ranges all", the final result should be the same: both of the nodes should
        # should contain all of the rows
        debug("Check for the eventual total number of keys to be: {}".format(num_of_keys*2))
        self.check_rows_on_node(node1, num_of_keys*2)
        self.check_rows_on_node(node2, num_of_keys*2)

    @attr('scylla-manager')
    def test_manager_repair_token_ranges_pr(self):
        self._manager_repair_token_ranges_template('pr')

    @attr('scylla-manager')
    def test_manager_repair_token_ranges_npr(self):
        self._manager_repair_token_ranges_template('npr')

    @attr('scylla-manager')
    def test_manager_repair_token_ranges_all(self):
        self._manager_repair_token_ranges_template('all')

    @attr('scylla-manager')
    def test_manager_repair_multi_cfs(self):
        """
        Repairing all of the cfs in a keyspace (The entire token range)
        """
        # Start a cluster of two nodes, and create a keyspace ks with RF=2, and a table cf.
        node1, node2 = self.config_and_create_cluster(nodes=2)
        session = self.patient_cql_connection(node1)
        self.create_ks(session, self.KEYSPACE_NAME, 2)

        # Create 4 tables
        for i in range(1, 5):
            self.create_cf(session, 'cf%d' % i, read_repair=0.0, columns={'c1': 'text', 'c2': 'text'},
                           dclocal_read_repair_chance=0.0, speculative_retry='NONE')

        num_of_keys = 10
        # Data for all tables. Will be used to validate the repaired data
        data_range = {'cf{}'.format(i): range(i*i, i*i+num_of_keys) for i in range(1, 5)}

        self._insert_data_range_to_specific_node(node_to_insert=node1, nodes_to_shut_down=[node2],
                                                 keyspace_name=self.KEYSPACE_NAME, data_ranges=data_range)

        # Checking that all of tables that are stored on node2 are empty
        self._assert_multiple_row_ranges_from_specific_node(node_to_query=node2, nodes_to_shut_down=[node1],
                                                            keyspace_name=self.KEYSPACE_NAME,
                                                            tables_and_row_count_dict=
                                                            {table_name: [] for table_name, key_range in data_range.items()})

        manager_tool = ScyllaManagerTool(scylla_manager=self.cluster._scylla_manager)
        cluster_name = "cluster1"
        debug("Add a cluster to scylla-manager, named: {}".format(cluster_name))
        mgr_cluster = manager_tool.add_cluster(node=node1, name=cluster_name)

        debug("Run repair on node 2")
        repair_task = mgr_cluster.create_repair_task(node=node2, keyspace=self.KEYSPACE_NAME, token_ranges='all')

        repair_task.wait_for_status(list_status=[TaskStatus.DONE], timeout=300, step=10)

        self._assert_multiple_row_ranges_from_specific_node(node_to_query=node2, nodes_to_shut_down=[node1],
                                                            keyspace_name=self.KEYSPACE_NAME,
                                                            tables_and_row_count_dict=data_range)

    @attr('scylla-manager')
    def test_manager_repair_two_keyspaces(self):
        """
        Test parameter "-K"  - the repair all tables in the keyspace. We start two nodes and a
        keyspace with RF=2.
        """
        # Start a cluster of two nodes, and create a keyspace ks with RF=2,
        # and a table cf. Hinted handoff and read repair are disabled so
        # they don't fix the problems which repair is supposed to fix.
        node1, node2 = self.config_and_create_cluster(nodes=2)
        session = self.patient_cql_connection(node1)

        # Create 2 keyspaces, with 1 table in each
        for i in range(1, 3):
            self.create_ks(session, 'ks%d' % i, 2)
            self.create_cf(session, 'ks%d.cf' % i, read_repair=0.0, columns={'c1': 'text', 'c2': 'text'},
                           dclocal_read_repair_chance=0.0, speculative_retry='NONE')

        num_of_keys = 10
        # Data for all tables. Will be used to validate the repaired data
        data_range = {}
        for i in range(1, 3):
            data_range['ks%d.cf' % i] = range(i*i, i*i+num_of_keys)

        self._insert_data_range_to_specific_node(node_to_insert=node1, nodes_to_shut_down=[node2], keyspace_name=None,
                                                 data_ranges=data_range)

        self._assert_multiple_row_ranges_from_specific_node(node_to_query=node2, nodes_to_shut_down=[node1],
                                                            keyspace_name=None, tables_and_row_count_dict=
                                                            {table_name: [] for table_name, key_range in data_range.items()})

        manager_tool = ScyllaManagerTool(scylla_manager=self.cluster._scylla_manager)
        cluster_name = "cluster1"
        debug("Add a cluster to scylla-manager, named: {}".format(cluster_name))
        mgr_cluster = manager_tool.add_cluster(node=node1, name=cluster_name)

        debug("Run repair on node 2")
        repair_task = mgr_cluster.create_repair_task(node=node2, token_ranges='all')

        repair_task.wait_for_status(list_status=[TaskStatus.DONE], timeout=300, step=10)

        self._assert_multiple_row_ranges_from_specific_node(node_to_query=node2, nodes_to_shut_down=[node1],
                                                            keyspace_name=None, tables_and_row_count_dict=data_range)

    @attr('scylla-manager')
    def test_repairing_host_with_specific_host(self):
        """
        Test the parameter --with_hosts with a specific node, to see that when it's being used,
        the repair will only use the specified node and not others.
        """
        node1, node2, node3 = self.config_and_create_cluster(nodes=3)

        session = self.patient_cql_connection(node1)
        self.create_ks(session, self.KEYSPACE_NAME, 3)

        # Create 2 tables
        self.create_cf(session, 'cf_to_be_repaired', read_repair=0.0, columns={'c1': 'text', 'c2': 'text'},
                       dclocal_read_repair_chance=0.0, speculative_retry='NONE')
        self.create_cf(session, 'other_cf', read_repair=0.0, columns={'c1': 'text', 'c2': 'text'},
                       dclocal_read_repair_chance=0.0, speculative_retry='NONE')

        # Data for each table. Will be used to validate the repaired data
        range_to_repair = {"cf_to_be_repaired": range(1, 11)}
        other_range = {"other_cf": range(11, 21)}

        self.cluster.flush()

        self._insert_data_range_to_specific_node(node_to_insert=node1, nodes_to_shut_down=[node2, node3],
                                                 keyspace_name=self.KEYSPACE_NAME, data_ranges=range_to_repair)
        self._insert_data_range_to_specific_node(node_to_insert=node2, nodes_to_shut_down=[node1, node3],
                                                 keyspace_name=self.KEYSPACE_NAME, data_ranges=other_range)

        manager_tool = ScyllaManagerTool(scylla_manager=self.cluster._scylla_manager)
        cluster_name = "cluster1"
        debug("Add a cluster to scylla-manager, named: {}".format(cluster_name))
        mgr_cluster = manager_tool.add_cluster(node=node1, name=cluster_name)

        repair_task = mgr_cluster.create_repair_task(node=node3, token_ranges='all', keyspace=self.KEYSPACE_NAME,
                                                     with_hosts=[node1])
        repair_task.wait_for_status(list_status=[TaskStatus.DONE], timeout=300, step=10)

        self._assert_multiple_row_ranges_from_specific_node(node_to_query=node3, nodes_to_shut_down=[node1, node2],
                                                            keyspace_name=self.KEYSPACE_NAME, tables_and_row_count_dict=
                                                            dict(range_to_repair, **{"other_cf": []}))

    @attr('scylla-manager')
    def test_repairing_host_with_several_hosts(self):
        """
        Test the parameter --with_hosts with several nodes, to see that when it's being used,
        the repair will only use the specified nodes, and not others.
        """
        node1, node2, node3, node4 = self.config_and_create_cluster(nodes=4)

        session = self.patient_cql_connection(node1)
        self.create_ks(session, self.KEYSPACE_NAME, 4)

        # Create 3 tables
        self.create_cf(session, 'first_cf_to_repair', read_repair=0.0, columns={'c1': 'text', 'c2': 'text'},
                       dclocal_read_repair_chance=0.0, speculative_retry='NONE')
        self.create_cf(session, 'second_cf_to_repair', read_repair=0.0, columns={'c1': 'text', 'c2': 'text'},
                       dclocal_read_repair_chance=0.0, speculative_retry='NONE')
        self.create_cf(session, 'cf_to_not_be_repaired', read_repair=0.0, columns={'c1': 'text', 'c2': 'text'},
                       dclocal_read_repair_chance=0.0, speculative_retry='NONE')

        # Data for each table. Will be used to validate the repaired data
        first_range_to_repair = {"first_cf_to_repair": range(1, 11)}
        second_range_to_repair = {"second_cf_to_repair": range(11, 21)}
        range_to_not_be_repaired = {"cf_to_not_be_repaired": range(21, 31)}

        self.cluster.flush()

        self._insert_data_range_to_specific_node(node_to_insert=node1, nodes_to_shut_down=[node2, node3, node4],
                                                 keyspace_name=self.KEYSPACE_NAME, data_ranges=first_range_to_repair)
        self._insert_data_range_to_specific_node(node_to_insert=node2, nodes_to_shut_down=[node1, node3, node4],
                                                 keyspace_name=self.KEYSPACE_NAME, data_ranges=second_range_to_repair)
        self._insert_data_range_to_specific_node(node_to_insert=node3, nodes_to_shut_down=[node1, node2, node4],
                                                 keyspace_name=self.KEYSPACE_NAME, data_ranges=range_to_not_be_repaired)

        manager_tool = ScyllaManagerTool(scylla_manager=self.cluster._scylla_manager)
        cluster_name = "cluster1"
        debug("Add a cluster to scylla-manager, named: {}".format(cluster_name))
        mgr_cluster = manager_tool.add_cluster(node=node1, name=cluster_name)

        repair_task = mgr_cluster.create_repair_task(node=node4, token_ranges='all', keyspace=self.KEYSPACE_NAME,
                                                     with_hosts=[node1, node2])
        repair_task.wait_for_status(list_status=[TaskStatus.DONE], timeout=300, step=10)

        # Since only node1 and node2 were used to repair node4, the range that node3 contains (for the table
        # cf_to_not_be_repaired should not appear in node4, unlike the others
        self._assert_multiple_row_ranges_from_specific_node(node_to_query=node4, nodes_to_shut_down=[node1, node2, node3],
                                                            keyspace_name=self.KEYSPACE_NAME, tables_and_row_count_dict=
                                                            dict(list(first_range_to_repair.items()) +
                                                                 list(second_range_to_repair.items()) +
                                                                 [("cf_to_not_be_repaired", [])]))

    @attr('scylla-manager')
    def test_repair_with_empty_host_list(self):
        """
        Test the parameter --with_hosts with no nodes specified, to see that when it's being used,
        the repair will fail.
        """
        node1, node2 = self.config_and_create_cluster(nodes=2)

        session = self.patient_cql_connection(node1)
        self.create_ks(session, self.KEYSPACE_NAME, 2)

        manager_tool = ScyllaManagerTool(scylla_manager=self.cluster._scylla_manager)
        cluster_name = "cluster1"
        debug("Add a cluster to scylla-manager, named: {}".format(cluster_name))
        mgr_cluster = manager_tool.add_cluster(node=node1, name=cluster_name)

        try:
            mgr_cluster.create_repair_task(node=node1, token_ranges='all', keyspace=self.KEYSPACE_NAME,
                                           with_hosts=[])
        except ScyllaManagerError as err:
            if "flag needs an argument: --with-hosts" not in err.args[0]:
                raise
            return
        assert False, 'A repair command with an empty "with-hosts" list did not raise an exception'

    @attr('scylla-manager')
    def test_repair_with_unavailable_host(self):
        """
        Executing a repair while the repairing node is dow, expecting a the repair to reach an 'ERROR' status
        """
        node1, node2, node3 = self.config_and_create_cluster(nodes=3)

        session = self.patient_cql_connection(node1)
        self.create_ks(session, self.KEYSPACE_NAME, 3)

        manager_tool = ScyllaManagerTool(scylla_manager=self.cluster._scylla_manager)
        cluster_name = "cluster1"
        debug("Add a cluster to scylla-manager, named: {}".format(cluster_name))
        mgr_cluster = manager_tool.add_cluster(node=node1, name=cluster_name)

        # Create 2 tables
        for i in range(1, 3):
            self.create_cf(session, 'cf%d' % i, read_repair=0.0, columns={'c1': 'text', 'c2': 'text'},
                           dclocal_read_repair_chance=0.0, speculative_retry='NONE')

        data_range = {'cf{}'.format(i): range(i*i, i*i+10) for i in range(1, 3)}
        self._insert_data_range_to_specific_node(node_to_insert=node1, nodes_to_shut_down=[node3],
                                                 keyspace_name=self.KEYSPACE_NAME, data_ranges=data_range)
        node1.stop(wait_other_notice=True)
        repair_task = mgr_cluster.create_repair_task(node=node3, token_ranges='all', keyspace=self.KEYSPACE_NAME,
                                                     with_hosts=[node1], fail_fast=True)
        repair_task.wait_for_status(list_status=[TaskStatus.ERROR], timeout=300, step=10)

    @attr('scylla-manager')
    def test_repairing_an_unavailable_host(self):
        """
        Test that when the repair is executed on a stopped node, the task will fail
        """
        node1, node2, node3 = self.config_and_create_cluster(nodes=3)

        session = self.patient_cql_connection(node1)
        self.create_ks(session, self.KEYSPACE_NAME, 3)

        manager_tool = ScyllaManagerTool(scylla_manager=self.cluster._scylla_manager)
        cluster_name = "cluster1"
        debug("Add a cluster to scylla-manager, named: {}".format(cluster_name))
        mgr_cluster = manager_tool.add_cluster(node=node1, name=cluster_name)

        # Create 2 tables
        for i in range(1, 3):
            self.create_cf(session, 'cf%d' % i, read_repair=0.0, columns={'c1': 'text', 'c2': 'text'},
                           dclocal_read_repair_chance=0.0, speculative_retry='NONE')

        node3.stop(wait_other_notice=True)
        repair_task = mgr_cluster.create_repair_task(node=node3, token_ranges='all', keyspace=self.KEYSPACE_NAME,
                                                     with_hosts=[node1, node2])
        assert repair_task.wait_for_status(list_status=[TaskStatus.ERROR], timeout=300, step=5), \
            "Repairing an unavailable node did not fail as expected"

    @attr('scylla-manager')
    def test_repairing_only_specific_node(self):
        """
        Test that when executing a repair on a specific node, other nodes in the cluster won't be repaired
        """
        node1, node2, node3 = self.config_and_create_cluster(nodes=3)

        session = self.patient_cql_connection(node1)
        self.create_ks(session, self.KEYSPACE_NAME, 3)

        self.create_cf(session, 'cf1', read_repair=0.0, columns={'c1': 'text', 'c2': 'text'},
                       dclocal_read_repair_chance=0.0, speculative_retry='NONE')

        # Data for each table. Will be used to validate the repaired data
        number_of_keys = 10
        data_range = {"cf1": range(1, number_of_keys+1)}

        self._insert_data_range_to_specific_node(node_to_insert=node1, nodes_to_shut_down=[node2, node3],
                                                 keyspace_name=self.KEYSPACE_NAME, data_ranges=data_range)

        manager_tool = ScyllaManagerTool(scylla_manager=self.cluster._scylla_manager)
        cluster_name = "cluster1"
        debug("Add a cluster to scylla-manager, named: {}".format(cluster_name))
        mgr_cluster = manager_tool.add_cluster(node=node1, name=cluster_name)

        repair_task = mgr_cluster.create_repair_task(node=node2, token_ranges='all', keyspace=self.KEYSPACE_NAME,
                                                     with_hosts=[node1])
        repair_task.wait_for_status(list_status=[TaskStatus.DONE], timeout=300, step=10)

        self._assert_multiple_row_ranges_from_specific_node(node_to_query=node2, nodes_to_shut_down=[node1, node3],
                                                            keyspace_name=self.KEYSPACE_NAME,
                                                            tables_and_row_count_dict=data_range)
        self._assert_multiple_row_ranges_from_specific_node(node_to_query=node3, nodes_to_shut_down=[node1, node2],
                                                            keyspace_name=self.KEYSPACE_NAME,
                                                            tables_and_row_count_dict={'cf1': []})

    @attr('scylla-manager')
    def test_repair_token_ranges_flag_without_host(self):
        """
        Executing a repair with the flag '--token-ranges' should only be possible if alongside either
        '--host' or '--with-hosts'. This test tries to execute a repair with the '--token-ranges' flag, but without
        '--host' or '--with-hosts', and expects a failure right on the command execution
        """
        node1, node2, node3 = self.config_and_create_cluster(nodes=3)

        session = self.patient_cql_connection(node1)
        self.create_ks(session, self.KEYSPACE_NAME, 3)

        manager_tool = ScyllaManagerTool(scylla_manager=self.cluster._scylla_manager)
        cluster_name = "cluster1"
        debug("Add a cluster to scylla-manager, named: {}".format(cluster_name))
        mgr_cluster = manager_tool.add_cluster(node=node1, name=cluster_name)

        try:
            mgr_cluster.create_repair_task(token_ranges='all', keyspace=self.KEYSPACE_NAME)
        except ScyllaManagerError as err:
            if 'token-ranges is only available with "host" and "with-hosts" flags' not in err.args[0]:
                raise
            return
        assert False, 'A repair command with the flag "token-ranges" alone did not raise an exception'

    @attr('scylla-manager')
    def test_repair_dc_by_name(self):
        """
        Test the repair command with the '--dc-list' flag, expecting that the repair will be executed on the
        specified dc and it alone.
        """
        number_of_dcs = 3
        amount_of_nodes_in_dc = 2
        dc1_node1, dc1_node2, dc2_node1, dc2_node2, dc3_node1, dc3_node2 = self.config_and_create_cluster(
            nodes=[amount_of_nodes_in_dc]*number_of_dcs)

        session = self.patient_cql_connection(dc1_node1)
        dc_replication_dict = {"dc{}".format(i): amount_of_nodes_in_dc for i in range(1, number_of_dcs + 1)}
        self.create_ks(session, self.KEYSPACE_NAME, dc_replication_dict)
        self.create_cf(session, 'cf1', read_repair=0.0, columns={'c1': 'text', 'c2': 'text'},
                       dclocal_read_repair_chance=0.0, speculative_retry='NONE')
        data_range = {"cf1": range(1, 11)}

        self._insert_data_range_to_specific_node(node_to_insert=dc1_node1,
                                                 nodes_to_shut_down=[dc2_node1, dc2_node2, dc3_node1, dc3_node2],
                                                 keyspace_name=self.KEYSPACE_NAME, data_ranges=data_range)

        manager_tool = ScyllaManagerTool(scylla_manager=self.cluster._scylla_manager)
        cluster_name = "cluster1"
        debug("Add a cluster to scylla-manager, named: {}".format(cluster_name))
        mgr_cluster = manager_tool.add_cluster(node=dc1_node1, name=cluster_name)

        repair_task = mgr_cluster.create_repair_task(dc_list=['dc1', 'dc2'], keyspace=self.KEYSPACE_NAME)
        repair_task.wait_for_status(list_status=[TaskStatus.DONE])
        self._assert_multiple_row_ranges_from_specific_node(node_to_query=dc2_node1,
                                                            nodes_to_shut_down=
                                                            [dc1_node1, dc1_node2, dc3_node1, dc3_node2],
                                                            keyspace_name=self.KEYSPACE_NAME,
                                                            tables_and_row_count_dict=data_range)
        self._assert_multiple_row_ranges_from_specific_node(node_to_query=dc3_node1,
                                                            nodes_to_shut_down=
                                                            [dc1_node1, dc1_node2, dc2_node1, dc2_node2],
                                                            keyspace_name=self.KEYSPACE_NAME,
                                                            tables_and_row_count_dict={"cf1": []})

    @attr('scylla-manager')
    def test_repair_multiple_dc_by_name(self):
        """
        Test the repair command with the '--dc-list' flag, expecting that the repair will be executed on the
        specified dc's and them alone.
        """
        number_of_dcs = 4
        amount_of_nodes_in_dc = 1
        dc1_node, dc2_node, dc3_node, dc4_node = self.config_and_create_cluster(
            nodes=[amount_of_nodes_in_dc]*number_of_dcs)

        session = self.patient_cql_connection(dc1_node)
        dc_replication_dict = {"dc{}".format(i): amount_of_nodes_in_dc for i in range(1, number_of_dcs + 1)}
        self.create_ks(session, self.KEYSPACE_NAME, dc_replication_dict)
        self.create_cf(session, 'cf1', read_repair=0.0, columns={'c1': 'text', 'c2': 'text'},
                       dclocal_read_repair_chance=0.0, speculative_retry='NONE')
        data_range = {"cf1": range(1, 11)}

        self._insert_data_range_to_specific_node(node_to_insert=dc1_node,
                                                 nodes_to_shut_down=[dc2_node, dc3_node, dc4_node],
                                                 keyspace_name=self.KEYSPACE_NAME, data_ranges=data_range)

        manager_tool = ScyllaManagerTool(scylla_manager=self.cluster._scylla_manager)
        cluster_name = "cluster1"
        debug("Add a cluster to scylla-manager, named: {}".format(cluster_name))
        mgr_cluster = manager_tool.add_cluster(node=dc1_node, name=cluster_name)

        repair_task = mgr_cluster.create_repair_task(dc_list=['dc1', 'dc2', 'dc3'], keyspace=self.KEYSPACE_NAME)
        repair_task.wait_for_status(list_status=[TaskStatus.DONE])
        self._assert_multiple_row_ranges_from_specific_node(node_to_query=dc2_node,
                                                            nodes_to_shut_down=[dc1_node, dc3_node, dc4_node],
                                                            keyspace_name=self.KEYSPACE_NAME,
                                                            tables_and_row_count_dict=data_range)
        self._assert_multiple_row_ranges_from_specific_node(node_to_query=dc3_node,
                                                            nodes_to_shut_down=[dc1_node, dc2_node, dc4_node],
                                                            keyspace_name=self.KEYSPACE_NAME,
                                                            tables_and_row_count_dict=data_range)
        self._assert_multiple_row_ranges_from_specific_node(node_to_query=dc4_node,
                                                            nodes_to_shut_down=[dc1_node, dc2_node, dc3_node],
                                                            keyspace_name=self.KEYSPACE_NAME,
                                                            tables_and_row_count_dict={"cf1": []})

    @attr('scylla-manager')
    def test_cross_dc_repair(self):
        """
        Test that executes a repair on a node by using a node from a different dc, expecting success.
        """
        number_of_dcs = 2
        amount_of_nodes_in_dc = 1
        dc1_node, dc2_node = self.config_and_create_cluster(nodes=[amount_of_nodes_in_dc]*number_of_dcs)

        session = self.patient_cql_connection(dc1_node)
        dc_replication_dict = {"dc{}".format(i): amount_of_nodes_in_dc for i in range(1, number_of_dcs + 1)}
        self.create_ks(session, self.KEYSPACE_NAME, dc_replication_dict)
        self.create_cf(session, 'cf1', read_repair=0.0, columns={'c1': 'text', 'c2': 'text'},
                       dclocal_read_repair_chance=0.0, speculative_retry='NONE')
        data_range = {"cf1": range(1, 11)}

        self._insert_data_range_to_specific_node(node_to_insert=dc1_node, nodes_to_shut_down=[dc2_node],
                                                 keyspace_name=self.KEYSPACE_NAME, data_ranges=data_range)

        manager_tool = ScyllaManagerTool(scylla_manager=self.cluster._scylla_manager)
        cluster_name = "cluster1"
        debug("Add a cluster to scylla-manager, named: {}".format(cluster_name))
        mgr_cluster = manager_tool.add_cluster(node=dc1_node, name=cluster_name)

        repair_task = mgr_cluster.create_repair_task(node=dc2_node, keyspace=self.KEYSPACE_NAME, with_hosts=[dc1_node])
        repair_task.wait_for_status(list_status=[TaskStatus.DONE])
        self._assert_multiple_row_ranges_from_specific_node(node_to_query=dc2_node, nodes_to_shut_down=[dc1_node],
                                                            keyspace_name=self.KEYSPACE_NAME,
                                                            tables_and_row_count_dict=data_range)

    @attr('scylla-manager')
    def test_repairing_from_local_dc_only(self):
        """
        When executing a repair with only one dc in the '--dc-list' flag, the repair should only be executed among
        the nodes in the specified dc, and it should not involve any other node. The following test makes sure of that.
        """
        number_of_dcs = 2
        amount_of_nodes_in_dc = 2
        dc1_node1, dc1_node2, dc2_node1, dc2_node2 = self.config_and_create_cluster(
            nodes=[amount_of_nodes_in_dc]*number_of_dcs)

        session = self.patient_cql_connection(dc1_node1)
        dc_replication_dict = {"dc{}".format(i): amount_of_nodes_in_dc for i in range(1, number_of_dcs + 1)}
        self.create_ks(session, self.KEYSPACE_NAME, dc_replication_dict)
        self.create_cf(session, 'cf_dc1', read_repair=0.0, columns={'c1': 'text', 'c2': 'text'},
                       dclocal_read_repair_chance=0.0, speculative_retry='NONE')
        self.create_cf(session, 'cf_dc2', read_repair=0.0, columns={'c1': 'text', 'c2': 'text'},
                       dclocal_read_repair_chance=0.0, speculative_retry='NONE')
        first_data_range = {"cf_dc1": range(1, 11)}
        second_data_range = {"cf_dc2": range(11, 21)}

        self._insert_data_range_to_specific_node(node_to_insert=dc1_node1,
                                                 nodes_to_shut_down=[dc1_node2, dc2_node1, dc2_node2],
                                                 keyspace_name=self.KEYSPACE_NAME, data_ranges=first_data_range)
        self._insert_data_range_to_specific_node(node_to_insert=dc2_node1,
                                                 nodes_to_shut_down=[dc1_node1, dc1_node2, dc2_node2],
                                                 keyspace_name=self.KEYSPACE_NAME, data_ranges=second_data_range)

        manager_tool = ScyllaManagerTool(scylla_manager=self.cluster._scylla_manager)
        cluster_name = "cluster1"
        debug("Add a cluster to scylla-manager, named: {}".format(cluster_name))
        mgr_cluster = manager_tool.add_cluster(node=dc1_node1, name=cluster_name)

        repair_task = mgr_cluster.create_repair_task(dc_list=['dc2'], keyspace=self.KEYSPACE_NAME)
        repair_task.wait_for_status(list_status=[TaskStatus.DONE])
        self._assert_multiple_row_ranges_from_specific_node(node_to_query=dc2_node2,
                                                            nodes_to_shut_down=[dc1_node1, dc1_node2, dc2_node1],
                                                            keyspace_name=self.KEYSPACE_NAME,
                                                            tables_and_row_count_dict=
                                                            dict(second_data_range, **{"cf_dc1": []}))

    @skip("Times out in the jenkins job")
    @attr('scylla-manager')
    def test_fail_fast(self):
        """
        When the '--fail-fast' flag is used on a repair command, the task should immediately fail upon error,
        and if it's not used, the task will not immediately fail. The following test checks both cases.
        """
        node1, node2, node3 = self.config_and_create_cluster(nodes=3)

        session = self.patient_cql_connection(node1)
        self.create_ks(session, self.KEYSPACE_NAME, 3)
        self.create_cf(session, 'cf1', read_repair=0.0, columns={'c1': 'text', 'c2': 'text'},
                       dclocal_read_repair_chance=0.0, speculative_retry='NONE')
        data_range = {"cf1": range(1, 1111)}
        self._insert_data_range_to_specific_node(node_to_insert=node1, nodes_to_shut_down=[node2, node3],
                                                 keyspace_name=self.KEYSPACE_NAME, data_ranges=data_range)

        manager_tool = ScyllaManagerTool(scylla_manager=self.cluster._scylla_manager)
        cluster_name = "cluster1"
        debug("Add a cluster to scylla-manager, named: {}".format(cluster_name))
        mgr_cluster = manager_tool.add_cluster(node=node1, name=cluster_name)

        repair_task_fail_fast = mgr_cluster.create_repair_task(node=node3, token_ranges='all',
                                                               keyspace=self.KEYSPACE_NAME, with_hosts=[node1],
                                                               fail_fast=True)
        debug("Stopping the node used for the repair, "
              "expecting the repair task (that uses fail-fast) to reach the status of 'ERROR' soon after")
        repair_task_fail_fast.wait_for_status(list_status=[TaskStatus.RUNNING], timeout=300, step=10)
        debug("task {} has reached the status of RUNNING,"
              " shutting down the host with which the task is using to repair".format(repair_task_fail_fast.id))
        node1.stop(wait_other_notice=True)
        repair_task_fail_fast.wait_for_status(list_status=[TaskStatus.ERROR], timeout=120, step=3)

        node1.start(wait_for_binary_proto=False, wait_other_notice=False)

        repair_task = mgr_cluster.create_repair_task(node=node2, token_ranges='all', keyspace=self.KEYSPACE_NAME,
                                                     with_hosts=[node1])
        debug("Stopping the node used for the repair. Since The repair does not use the 'fail-fast' flag,"
              " the new task, {}, is not expected to reach the 'ERROR' status soon".format(repair_task.id))
        repair_task.wait_for_status(list_status=[TaskStatus.RUNNING], timeout=300, step=10)
        node1.stop(wait_other_notice=True)
        try:
            repair_task.wait_for_status(list_status=[TaskStatus.ERROR], timeout=240, step=10)
        except WaitTimeoutExpired:
            pass
        else:
            assert True, "Even without --fail-fast flag, the repair task failed in a short time"
