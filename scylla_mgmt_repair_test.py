# coding: utf-8
# import smgr
from dtest_scylla_manager import HostStatus, HostRestStatus, ScyllaManagerTool, TaskStatus
from dtest import Tester, debug
from unittest import skip

from tools import insert_c1c2, query_c1c2
from assertions import assert_row_count
from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement
from cassandra.concurrent import execute_concurrent_with_args
from ccmlib.node import NodetoolError
import time
import tempfile
import os
import threading
import random
import uuid

from repair_additional_test import RepairAdditionalBase


class ScyllaMgmtRepairTest(RepairAdditionalBase):
    __test__ = True

    # TODO: adjust (to dtest_scylla_manager.py) or delete all RepairAdditionalBase related tests
    def repair_disjoint_data_test(self, more_options=[]):
        return RepairAdditionalBase._repair_disjoint_data_test(self,more_options)

    def repair_schema_test(self):
        return RepairAdditionalBase._repair_schema_test(self)

    def repair_schema_2_test(self):
        return RepairAdditionalBase._repair_schema_2_test(self)

    def repair_cell_update_test(self):
       return RepairAdditionalBase._repair_cell_update_test(self)

    def repair_cell_delete_test(self):
       return RepairAdditionalBase._repair_cell_delete_test(self)

    def repair_row_delete_test(self):
       return RepairAdditionalBase._repair_row_delete_test(self)

    def repair_partition_delete_test(self):
       return RepairAdditionalBase._repair_partition_delete_test(self)

    def config_and_create_cluster(self, nodes):
        self.cluster.set_configuration_options(values={'hinted_handoff_enabled': False})
        self.cluster.populate(nodes).start(wait_for_binary_proto=False, wait_other_notice=False)

    def _initiate_cluster_with_data(self):
        debug("Starting cluster and inserting data...")
        self.config_and_create_cluster(nodes=3)
        # Start a cluster of three nodes, and create a keyspace with RF=3, and
        node1, node2, node3 = self.cluster.nodelist()

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

    def test_manager_repair_token_ranges_pr(self):
        """
        Test the "partitioner range" (-pr) option. We start two nodes and a
        keyspace with RF=2, and put <num_of_keys> different rows on each of the nodes
        (as in repair_disjoint_data_set). Each node has in "partioner ranges"
        only half the key space, so that starting a repair with "-pr" on one
        node will bring in around <num_of_keys/2> missing partitions, but the other <num_of_keys/2>
        will continue to be missing until we start a repair with "-pr" on the
        second node as well.
        """
        # Start a cluster of two nodes, and create a keyspace ks with RF=2,
        # and a table cf. Hinted handoff and read repair are disabled so
        # they don't fix the problems which repair is supposed to fix.
        self.cluster.set_configuration_options(values={'hinted_handoff_enabled': False})
        self.cluster.populate(2).start(wait_for_binary_proto=True, wait_other_notice=True)
        node1, node2 = self.cluster.nodelist()
        session = self.patient_cql_connection(node1)
        self.create_ks(session, 'ks', 2)
        self.create_cf(session, 'cf', read_repair=0.0, columns={'c1': 'text', 'c2': 'text'})

        num_of_keys = 1000
        delta = int(num_of_keys * 20 / 100)
        first_range_start = num_of_keys
        second_range_start = num_of_keys * 2
        second_range_end = second_range_start + num_of_keys
        eventual_total_num_of_keys = num_of_keys * 2
        # Insert 1000 keys *only* on node 1, another 1000 keys *only* on node 2:
        debug("Adding data only on node 1...")
        node2.flush()
        node2.stop(wait_other_notice=True)
        session = self.patient_cql_connection(node1, 'ks')
        insert_c1c2(session, keys=range(first_range_start, second_range_start), consistency=ConsistencyLevel.ONE)
        self.cluster.flush()
        debug("Adding data only on node 2...")
        node2.start(wait_other_notice=True, wait_for_binary_proto=True)
        node1.flush()
        node1.stop(wait_other_notice=True)
        session = self.patient_cql_connection(node2, 'ks')
        insert_c1c2(session, keys=range(second_range_start, second_range_end), consistency=ConsistencyLevel.ONE)

        # Bring up both nodes, each should have different data
        node1.start(wait_other_notice=True, wait_for_binary_proto=True)
        manager_tool = ScyllaManagerTool(scylla_manager=self.cluster._scylla_manager)
        cluster_name = "cluster1"
        debug("Add a cluster to scylla-manager, named: {}".format(cluster_name))
        mgr_cluster = manager_tool.add_cluster(node=node1, name=cluster_name)

        debug("Run partitioner-range repair on node 1")
        mgr_task = mgr_cluster.create_repair_task(node=node1, token_ranges='pr', keyspace='ks')

        task_final_status = mgr_task.wait_and_get_final_status()
        assert task_final_status == TaskStatus.DONE, 'Task: {} final status is: {}.'.format(mgr_task.id, str(mgr_task.status))

        # We expect "-pr" repair to have repaired only half of the ranges
        # (those for which node 1 is their primary replica), so both nodes
        # should now have around 1.5 * keys-number partitions. We don't know the exact
        # number, but given the assumed random distribution of tokens and keys,
        # it is unlikely to be far from %20 delta  - let's assert it is between a delta around
        node1.flush()
        node1.stop(wait_other_notice=True)
        session = self.patient_cql_connection(node2, 'ks')
        count = len(list(session.execute("SELECT * FROM cf LIMIT 2000")))
        self.assertTrue(count > (first_range_start * 1.5 - delta) and count < (first_range_start * 1.5 + delta), "expected pr repair to repair part, but not everything ({} keys exist)".format(count))
        node1.start(wait_other_notice=True, wait_for_binary_proto=True)
        node2.flush()
        node2.stop(wait_other_notice=True)
        session = self.patient_cql_connection(node1, 'ks')
        count = len(list(session.execute("SELECT * FROM cf LIMIT 2000")))
        self.assertTrue(count > (first_range_start * 1.5 - delta) and count < (first_range_start * 1.5 + delta), "expected pr repair to repair part, but not everything ({} keys exist)".format(count))
        node2.start(wait_other_notice=True, wait_for_binary_proto=True)

        # Run a second "-pr" repair, this time on node 2. This should repair
        # all the ranges not previously repared (i.e., this times the ranges
        # whose primary is node 2), and at the end, all data,
        # should be on both nodes.
        debug("Run partioner-range repair on node 2")
        mgr_task2 = mgr_cluster.create_repair_task(node=node2, token_ranges='pr', keyspace='ks')

        task_final_status = mgr_task2.wait_and_get_final_status()
        assert task_final_status == TaskStatus.DONE, 'Task: {} final status is: {}.'.format(mgr_task2.id,
                                                                                            str(mgr_task.status))
        debug("Check for the eventual total number of keys to be: {}".format(eventual_total_num_of_keys))
        self.check_rows_on_node(node1, eventual_total_num_of_keys)
        self.check_rows_on_node(node2, eventual_total_num_of_keys)

    def test_manager_repair_multi_cfs(self):
        """
        Test parameter "-K"  - the repair all tables in the keyspace. We start two nodes and a
        keyspace with RF=2.
        """
        # Start a cluster of two nodes, and create a keyspace ks with RF=2,
        # and a table cf. Hinted handoff and read repair are disabled so
        # they don't fix the problems which repair is supposed to fix.
        self.config_and_create_cluster(nodes=2)
        node1, node2 = self.cluster.nodelist()
        session = self.patient_cql_connection(node1)
        self.create_ks(session, 'ks', 2)

        # Create 4 tables
        for i in xrange(1, 5):
            self.create_cf(session, 'cf%d' % i, read_repair=0.0, columns={'c1': 'text', 'c2': 'text'},
                           dclocal_read_repair_chance=0.0, speculative_retry='NONE')

        num_of_keys = 10
        # Data for all tables. Will be used to validate the repaired data
        data_range = {}
        for i in xrange(1, 5):
            data_range['cf%d' % i] = range(i*i, i*i+num_of_keys)

        node2.flush()
        node2.stop(wait_other_notice=True)
        session = self.patient_cql_connection(node1, 'ks')

        debug("Adding data only on node 1...")
        for cf_name, cf_range in data_range.items():
            statement = session.prepare("INSERT INTO %s (key, c1, c2) VALUES (?, 'value1', 'value2')" % cf_name)
            statement.consistency_level = ConsistencyLevel.ONE
            execute_concurrent_with_args(session, statement, [['k{}'.format(k)] for k in cf_range])

        self.cluster.flush()

        debug("Start node 2...")
        node2.start(wait_other_notice=True, wait_for_binary_proto=True)

        session_node2 = self.exclusive_cql_connection(node2, 'ks')

        debug('Stop node1')
        node1.stop(wait_other_notice=True)

        # Validate the node2 has no data
        for cf_name, cf_range in data_range.items():
            assert_row_count(session=session_node2, table_name=cf_name, expected=0)

        debug("Start node1...")
        node1.start(wait_other_notice=True, wait_for_binary_proto=True)

        manager_tool = ScyllaManagerTool(scylla_manager=self.cluster._scylla_manager)
        cluster_name = "cluster1"
        debug("Add a cluster to scylla-manager, named: {}".format(cluster_name))
        mgr_cluster = manager_tool.add_cluster(node=node1, name=cluster_name)

        debug("Run repair on node 2")
        mgr_task = mgr_cluster.create_repair_task(node=node2, keyspace='ks')

        sleep = 600
        debug('Sleep {} seconds, waiting for repair task to run.'.format(sleep))
        time.sleep(sleep)
        debug("repair task status is: {}".format(mgr_task.status))

        debug('Stop node1')
        node1.stop(wait_other_notice=True)

        for cf_name, cf_range in data_range.items():
            assert_row_count(session=session_node2, table_name=cf_name, expected=num_of_keys)

    def test_manager_repair_two_keyspaces(self):
        """
        Test parameter "-K"  - the repair all tables in the keyspace. We start two nodes and a
        keyspace with RF=2.
        """
        # Start a cluster of two nodes, and create a keyspace ks with RF=2,
        # and a table cf. Hinted handoff and read repair are disabled so
        # they don't fix the problems which repair is supposed to fix.
        self.config_and_create_cluster(nodes=2)
        node1, node2 = self.cluster.nodelist()
        session = self.patient_cql_connection(node1)

        # Create 4 tables
        for i in xrange(1, 3):
            self.create_ks(session, 'ks%d' % i, 2)
            self.create_cf(session, 'cf', read_repair=0.0, columns={'c1': 'text', 'c2': 'text'},
                           dclocal_read_repair_chance=0.0, speculative_retry='NONE')

        num_of_keys = 10
        # Data for all tables. Will be used to validate the repaired data
        data_range = {}
        for i in xrange(1, 3):
            data_range['ks%d.cf' % i] = range(i*i, i*i+num_of_keys)

        node2.flush()
        node2.stop(wait_other_notice=True)
        session = self.patient_cql_connection(node1)

        debug("Adding data only on node 1...")
        for cf_name, cf_range in data_range.items():
            statement = session.prepare("INSERT INTO %s (key, c1, c2) VALUES (?, 'value1', 'value2')" % cf_name)
            statement.consistency_level = ConsistencyLevel.ONE
            execute_concurrent_with_args(session, statement, [['k{}'.format(k)] for k in cf_range])

        self.cluster.flush()

        debug("Start node 2...")
        node2.start(wait_other_notice=True, wait_for_binary_proto=True)

        session_node2 = self.exclusive_cql_connection(node2)

        debug('Stop node1')
        node1.stop(wait_other_notice=True)

        # Validate the node2 has no data
        for cf_name, cf_range in data_range.items():
            assert_row_count(session=session_node2, table_name=cf_name, expected=0)

        debug("Start node1...")
        node1.start(wait_other_notice=True, wait_for_binary_proto=True)

        manager_tool = ScyllaManagerTool(scylla_manager=self.cluster._scylla_manager)
        cluster_name = "cluster1"
        debug("Add a cluster to scylla-manager, named: {}".format(cluster_name))
        mgr_cluster = manager_tool.add_cluster(node=node1, name=cluster_name)

        debug("Run repair on node 2")
        mgr_task = mgr_cluster.create_repair_task(node=node2)

        sleep = 600
        debug('Sleep {} seconds, waiting for repair task to run.'.format(sleep))
        time.sleep(sleep)
        debug("repair task status is: {}".format(mgr_task.status))

        debug('Stop node1')
        node1.stop(wait_other_notice=True)

        for cf_name, cf_range in data_range.items():
            assert_row_count(session=session_node2, table_name=cf_name, expected=num_of_keys)
