import logging
import re

import pytest

from dtest_scylla_manager import ScyllaManagerError, TaskStatus, ScyllaManagerMixin, C1_PREFIX, C2_PREFIX
from dtest_class import Tester
from manager_backup_tests import ManagerBackupMixin, minio_docker


CLUSTER_NAME = 'cluster1'
DESTINATION_BUCKET = 'backup-bucket'
DEFAULT_KEYSPACE_TABLE_AND_KEY_RANGE = {"ks": {"cf1": (1, 21)}}


logger = logging.getLogger(__name__)


@pytest.mark.scylla_manager
class TestScyllaMgmtRestore(Tester, ManagerBackupMixin, ScyllaManagerMixin):
    def verify_c1c2(self, node, keyspace_table_and_key_range=None):
        if keyspace_table_and_key_range is None:
            keyspace_table_and_key_range = DEFAULT_KEYSPACE_TABLE_AND_KEY_RANGE
        super().verify_c1c2(keyspace_table_and_key_range=keyspace_table_and_key_range, node=node)

    def _backup_and_cleanup(self, healthy_node, mgr_cluster, keyspace_table_and_key_range):
        backup_task = mgr_cluster.run_backup_command(
            location_list=["s3:{}".format(DESTINATION_BUCKET)],
            keyspace_list=list(keyspace_table_and_key_range.keys()))
        backup_task.wait_for_status(list_status=[TaskStatus.DONE], step=5)
        self.clean_up_tables(node=healthy_node, keyspace_and_tables_dict=keyspace_table_and_key_range)
        return backup_task

    def insert_data_backup_and_cleanup(self, healthy_node, mgr_cluster, keyspace_table_and_key_range=None, rf=2):
        if keyspace_table_and_key_range is None:
            keyspace_table_and_key_range = DEFAULT_KEYSPACE_TABLE_AND_KEY_RANGE
        self.insert_data_from_ranges(healthy_node=healthy_node,
                                     keyspace_table_and_key_range=keyspace_table_and_key_range, rf=rf)
        backup_task = self._backup_and_cleanup(healthy_node=healthy_node, mgr_cluster=mgr_cluster,
                                               keyspace_table_and_key_range=keyspace_table_and_key_range)
        return backup_task

    def restore_and_verify(self, mgr_cluster, backup_task, healthy_node):
        restore_task = mgr_cluster.run_restore_command(location_list=["s3:{}".format(DESTINATION_BUCKET)],
                                                       restore_data=True,
                                                       snapshot_tag=backup_task.get_snapshot_tag())
        final_status = restore_task.wait_and_get_final_status(step=5)
        assert final_status == TaskStatus.DONE, f"Restore task failed: {restore_task.full_progress_string()}"
        for node in self.cluster.nodelist():
            if self._is_node_at_status(node.address(), functioning_node=healthy_node, desirable_status="UN",
                                       tolerate_missing=True):
                node.nodetool("repair")
        self.verify_c1c2(node=healthy_node)

    def restore_and_verify_using_stress(self, mgr_cluster, backup_task, healthy_node, number_of_rows, threads=5,
                                        batch_size=None):
        restore_task = mgr_cluster.run_restore_command(location_list=["s3:{}".format(DESTINATION_BUCKET)],
                                                       restore_data=True,
                                                       snapshot_tag=backup_task.get_snapshot_tag(),
                                                       batch_size=batch_size)
        final_status = restore_task.wait_and_get_final_status(step=5)
        assert final_status == TaskStatus.DONE, f"Restore task failed: {restore_task.full_progress_string()}"
        for node in self.cluster.nodelist():
            if self._is_node_at_status(node.address(), functioning_node=healthy_node, desirable_status="UN",
                                       tolerate_missing=True):
                node.nodetool("repair")
        self.cluster.stress(['read', f'n={number_of_rows}', '-rate', f'threads={threads}'])

    def test_basic_restore(self):
        node1, node2 = self.config_and_create_cluster(nodes=2)
        mgr_cluster = self._create_mgr_cluster(node=node1, name=CLUSTER_NAME)
        backup_task = self.insert_data_backup_and_cleanup(node1, mgr_cluster)
        self.restore_and_verify(mgr_cluster, backup_task, node1)

    def test_restore_removed_table(self):
        node1, node2 = self.config_and_create_cluster(nodes=2)
        mgr_cluster = self._create_mgr_cluster(node=node1, name=CLUSTER_NAME)
        backup_task = self.insert_data_backup_and_cleanup(healthy_node=node1, mgr_cluster=mgr_cluster)
        self._drop_table_and_delete_table_dir("ks", "cf1", node1)
        try:
            mgr_cluster.run_restore_command(location_list=["s3:{}".format(DESTINATION_BUCKET)],
                                            restore_data=True,
                                            snapshot_tag=backup_task.get_snapshot_tag())
        except ScyllaManagerError as err:
            assert "table" in err.args[0].lower() and "not found" in err.args[0].lower(), \
                f"Trying to restore a dropped table failed with an improper error message: {err.args[0]}"

    def _compare_single_column(self, node, column_name, prefix):
        keyspace_name = list(DEFAULT_KEYSPACE_TABLE_AND_KEY_RANGE.keys())[0]
        table_name = list(DEFAULT_KEYSPACE_TABLE_AND_KEY_RANGE[keyspace_name].keys())[0]
        key_range = DEFAULT_KEYSPACE_TABLE_AND_KEY_RANGE[keyspace_name][table_name]

        expected_values = sorted([prefix % i for i in range(*key_range)])
        session = self.patient_cql_connection(node)
        query_result = session.execute(f"select {column_name} from {keyspace_name}.{table_name}")
        value_list = []
        for row in query_result:
            value_list.append(getattr(row, column_name))
        value_list.sort()
        assert value_list == expected_values

    def _validate_all_column_values_none(self, node, column_name):
        keyspace_name = list(DEFAULT_KEYSPACE_TABLE_AND_KEY_RANGE.keys())[0]
        table_name = list(DEFAULT_KEYSPACE_TABLE_AND_KEY_RANGE[keyspace_name].keys())[0]
        session = self.patient_cql_connection(node)
        query_result = session.execute(f"select {column_name} from {keyspace_name}.{table_name}")
        value_list = []
        for row in query_result:
            value_list.append(getattr(row, column_name))
        is_all_values_none = map(lambda x: x is None, value_list)
        assert all(is_all_values_none), f"Some of the values of the column {column_name} are not None: {value_list}"

    # Apparently load and stream is really error resistant and changes in the schema would not cause it to fail
    # Hence, the restore will not fail when the schema is altered.
    def test_restore_after_adding_column(self):
        node1, node2 = self.config_and_create_cluster(nodes=2)
        mgr_cluster = self._create_mgr_cluster(node=node1, name=CLUSTER_NAME)
        backup_task = self.insert_data_backup_and_cleanup(healthy_node=node1, mgr_cluster=mgr_cluster)
        session = self.patient_cql_connection(node1)
        session.execute("ALTER TABLE ks.cf1 ADD c3 int")
        restore_task = mgr_cluster.run_restore_command(location_list=["s3:{}".format(DESTINATION_BUCKET)],
                                                       restore_data=True,
                                                       snapshot_tag=backup_task.get_snapshot_tag())
        final_status = restore_task.wait_and_get_final_status(step=10)
        assert final_status == TaskStatus.DONE, \
            f"The restore task should not fail when the schema has been altered, but the restore task has reached " \
            f"the status of {final_status} after a column was added to the target table"
        node1.nodetool("repair")
        node2.nodetool("repair")
        self._compare_single_column(node=node1, column_name="key", prefix="k%d")
        self._compare_single_column(node=node1, column_name="c1", prefix=C1_PREFIX)
        self._compare_single_column(node=node1, column_name="c2", prefix=C2_PREFIX)
        self._validate_all_column_values_none(node=node1, column_name="c3")

    def test_restore_after_removing_column(self):
        node1, node2 = self.config_and_create_cluster(nodes=2)
        mgr_cluster = self._create_mgr_cluster(node=node1, name=CLUSTER_NAME)
        backup_task = self.insert_data_backup_and_cleanup(healthy_node=node1, mgr_cluster=mgr_cluster)
        session = self.patient_cql_connection(node1)
        session.execute("ALTER TABLE ks.cf1 DROP c2")
        restore_task = mgr_cluster.run_restore_command(location_list=["s3:{}".format(DESTINATION_BUCKET)],
                                                       restore_data=True,
                                                       snapshot_tag=backup_task.get_snapshot_tag())
        final_status = restore_task.wait_and_get_final_status(step=10)
        # Apparently load and stream is really error resistant and changes in the schema would not cause it to fail
        assert final_status == TaskStatus.DONE, \
            f"The restore task should not fail when the schema has been altered, but the restore task has reached " \
            f"the status of {final_status} after a column from the target table was removed"
        node1.nodetool("repair")
        node2.nodetool("repair")
        self._compare_single_column(node=node1, column_name="key", prefix="k%d")
        self._compare_single_column(node=node1, column_name="c1", prefix=C1_PREFIX)

    def test_restore_after_replacing_column(self):
        node1, node2 = self.config_and_create_cluster(nodes=2)
        mgr_cluster = self._create_mgr_cluster(node=node1, name=CLUSTER_NAME)
        backup_task = self.insert_data_backup_and_cleanup(healthy_node=node1, mgr_cluster=mgr_cluster)
        session = self.patient_cql_connection(node1)
        session.execute("ALTER TABLE ks.cf1 DROP c2")
        session.execute("ALTER TABLE ks.cf1 ADD c2 int")
        restore_task = mgr_cluster.run_restore_command(location_list=["s3:{}".format(DESTINATION_BUCKET)],
                                                       restore_data=True,
                                                       snapshot_tag=backup_task.get_snapshot_tag())
        final_status = restore_task.wait_and_get_final_status(step=10)
        # Apparently load and stream is really error resistant and changes in the schema would not cause it to fail
        assert final_status == TaskStatus.DONE, \
            f"The restore task should not fail when the schema has been altered, but the restore task has reached " \
            f"the status of {final_status} after a column from the target table was removed"
        node1.nodetool("repair")
        node2.nodetool("repair")
        self._compare_single_column(node=node1, column_name="key", prefix="k%d")
        self._compare_single_column(node=node1, column_name="c1", prefix=C1_PREFIX)
        self._validate_all_column_values_none(node=node1, column_name="c2")

    def test_restore_after_decommission(self):
        node1, node2, node3 = self.config_and_create_cluster(nodes=3)
        mgr_cluster = self._create_mgr_cluster(node=node1, name=CLUSTER_NAME)
        backup_task = self.insert_data_backup_and_cleanup(healthy_node=node1, mgr_cluster=mgr_cluster)
        node3.nodetool("decommission")
        self.restore_and_verify(mgr_cluster, backup_task, node1)

    def _add_new_node_and_wait_up_normal(self, healthy_node, datacenter=None, cluster=None):
        if cluster is None:
            cluster = self.cluster
        new_node = cluster.new_node(len(cluster.nodelist()) + 1,
                                    auto_bootstrap=True,
                                    add_node=True,
                                    is_seed=False,
                                    data_center=datacenter)
        new_node.start()
        self._wait_until_node_reaches_status(new_node, healthy_node, "UN", tolerate_missing=True)
        return new_node

    def test_restore_after_adding_new_node(self):
        node1, node2 = self.config_and_create_cluster(nodes=2)
        mgr_cluster = self._create_mgr_cluster(node=node1, name=CLUSTER_NAME)
        backup_task = self.insert_data_backup_and_cleanup(healthy_node=node1, mgr_cluster=mgr_cluster)
        node3 = self._add_new_node_and_wait_up_normal(healthy_node=node1)
        node3.nodetool("repair")
        self.restore_and_verify(mgr_cluster, backup_task, node1)

    def test_restore_after_adding_new_dc(self):
        node1, node2 = self.config_and_create_cluster(nodes=[2])
        mgr_cluster = self._create_mgr_cluster(node=node1, name=CLUSTER_NAME)
        backup_task = self.insert_data_backup_and_cleanup(healthy_node=node1, mgr_cluster=mgr_cluster)
        self._add_new_node_and_wait_up_normal(healthy_node=node1, datacenter="dc2")
        self.restore_and_verify(mgr_cluster, backup_task, node1)

    def test_restore_after_remove_dc(self):
        node1, node2, node3 = self.config_and_create_cluster(nodes=[2, 1])
        mgr_cluster = self._create_mgr_cluster(node=node1, name=CLUSTER_NAME)
        backup_task = self.insert_data_backup_and_cleanup(healthy_node=node1, mgr_cluster=mgr_cluster,
                                                          rf={'dc1': 2, 'dc2': 1})
        node3.nodetool("decommission")
        self.restore_and_verify(mgr_cluster, backup_task, node1)

    def test_restore_different_dc(self, secondary_cluster):
        # manager_backend_cluster
        self.config_and_create_cluster(nodes=2)
        # test cluster
        first_dc_nodes = self.config_and_create_cluster(nodes=[3], cluster=secondary_cluster)
        mgr_cluster = self._create_mgr_cluster(node=first_dc_nodes[0], name=CLUSTER_NAME)
        backup_task = self.insert_data_backup_and_cleanup(healthy_node=first_dc_nodes[0], mgr_cluster=mgr_cluster)
        second_dc_nodes = []
        for i in range(3):
            new_node = self._add_new_node_and_wait_up_normal(healthy_node=first_dc_nodes[0], datacenter="dc2",
                                                             cluster=secondary_cluster)
            second_dc_nodes.append(new_node)
            new_node.nodetool("repair")
        for node in first_dc_nodes:
            node.nodetool("drain")
        self.restore_and_verify(mgr_cluster, backup_task, second_dc_nodes[0])

    def test_restore_using_nonexistent_snapshot_tag(self):
        node1, node2 = self.config_and_create_cluster(nodes=[2])
        mgr_cluster = self._create_mgr_cluster(node=node1, name=CLUSTER_NAME)
        try:
            mgr_cluster.run_restore_command(location_list=["s3:{}".format(DESTINATION_BUCKET)],
                                            restore_data=True,
                                            snapshot_tag="sm_20190126161112UTC")
        except ScyllaManagerError as err:
            expected_error_string = "no snapshot with given tag"
            assert expected_error_string in err.args[0].lower(), \
                f"Create a restore task with a nonexistent snapshot tag failed, as expected, " \
                f"but with an improper error message: {err.args[0]}\n\nExpected: '{expected_error_string}'"
        else:
            raise ScyllaManagerError("No error occurred when creating a restore task with a nonexistent snapshot tag")

    def test_restore_only_specific_keyspace(self):
        keyspace_table_and_key_range = {
            "ks": {"cf1": (1, 21)},
            "ks_1": {"cf1": (44, 69), "cf2": (25, 35)},
            "ks_a": {"cf1": (100, 123)}}
        backed_up_keyspaces = ["ks_1", "ks_a"]
        ks_not_backed_up = ["ks"]
        node1, node2 = self.config_and_create_cluster(nodes=[2])
        mgr_cluster = self._create_mgr_cluster(node=node1, name=CLUSTER_NAME)
        backup_task = self.insert_data_backup_and_cleanup(healthy_node=node1, mgr_cluster=mgr_cluster,
                                                          keyspace_table_and_key_range=keyspace_table_and_key_range)
        restore_task = mgr_cluster.run_restore_command(location_list=["s3:{}".format(DESTINATION_BUCKET)],
                                                       restore_data=True,
                                                       snapshot_tag=backup_task.get_snapshot_tag(),
                                                       keyspace_list=["ks_*"])
        restore_task.wait_for_status(list_status=[TaskStatus.DONE], step=5)
        node1.nodetool("repair")
        node2.nodetool("repair")
        self.verify_c1c2(
            node=node1,
            keyspace_table_and_key_range={ks: keyspace_table_and_key_range[ks] for ks in backed_up_keyspaces})
        self.verify_lack_of_keys(
            keyspace_table_and_key_range={ks: keyspace_table_and_key_range[ks] for ks in ks_not_backed_up},
            node=node1)

    def test_restore_using_nonexistent_keyspace(self):
        node1, node2 = self.config_and_create_cluster(nodes=[2])
        mgr_cluster = self._create_mgr_cluster(node=node1, name=CLUSTER_NAME)
        backup_task = self.insert_data_backup_and_cleanup(healthy_node=node1, mgr_cluster=mgr_cluster)
        try:
            mgr_cluster.run_restore_command(location_list=["s3:{}".format(DESTINATION_BUCKET)],
                                            restore_data=True,
                                            snapshot_tag=backup_task.get_snapshot_tag(),
                                            keyspace_list=["ShlomoWasHere2023"])
        except ScyllaManagerError as err:
            expected_error_string = "no data in backup locations match given keyspace pattern"
            assert expected_error_string in err.args[0].lower(), \
                f"Create a restore task with a nonexistent keyspace to restore failed, as expected, " \
                f"but with an improper error message: {err.args[0]}\n\nExpected: '{expected_error_string}'"
        else:
            raise ScyllaManagerError("No error occurred when creating a restore task with a nonexistent keyspace")

    def test_verify_repair_message_after_restore(self):
        node1, node2 = self.config_and_create_cluster(nodes=2)
        mgr_cluster = self._create_mgr_cluster(node=node1, name=CLUSTER_NAME)
        backup_task = self.insert_data_backup_and_cleanup(node1, mgr_cluster)
        restore_task = mgr_cluster.run_restore_command(location_list=["s3:{}".format(DESTINATION_BUCKET)],
                                                       restore_data=True,
                                                       snapshot_tag=backup_task.get_snapshot_tag())
        restore_task.wait_for_status(list_status=[TaskStatus.DONE], step=5)
        progress_output_lines, _ = restore_task.progress_details()
        full_output_string = "\n".join([line[0] for line in progress_output_lines])  # Since it's a list of lists
        expected_message = "repair required"
        assert expected_message in full_output_string, \
            f"There was no message in the output of 'sctool progress' that indicates the user should run a repair " \
            f"after the restoration is complete:\nExpected message: '{expected_message}'\n" \
            f"Full message:\n{full_output_string}"

    def test_restore_using_different_batch_sizes(self):
        number_of_rows = "1500K"
        node1, node2 = self.config_and_create_cluster(nodes=2)
        mgr_cluster = self._create_mgr_cluster(node=node1, name=CLUSTER_NAME)
        self.cluster.stress(
            ['write', f'n={number_of_rows}', '-rate', 'threads=50', '-schema',
             'compaction(strategy=SizeTieredCompactionStrategy)'])
        backup_task = mgr_cluster.run_backup_command(
            location_list=["s3:{}".format(DESTINATION_BUCKET)],
            keyspace_list=["keyspace1"])
        backup_task.wait_for_status(list_status=[TaskStatus.DONE], step=5)
        batch_size_list = [None, 1, 3, 5]
        for batch_size in batch_size_list:
            self.clean_up_tables(node=node1, keyspace_and_tables_dict={"keyspace1": ["standard1"]})
            self.restore_and_verify_using_stress(mgr_cluster=mgr_cluster, backup_task=backup_task, healthy_node=node1,
                                                 number_of_rows=number_of_rows, threads=50, batch_size=batch_size)

    def _get_tombstone_gc_mode(self, healthy_node, keyspace, table):
        session = self.patient_cql_connection(healthy_node)
        result = session.execute(f"select extensions from system_schema.tables "
                                 f"where keyspace_name = '{keyspace}' and table_name = '{table}';")
        tombstone_gc_mode = 'N\A'
        if "tombstone_gc" in result.current_rows[0].extensions:
            tombstone_gc_raw_string = result.current_rows[0].extensions["tombstone_gc"].decode()
            tombstone_gc_mode = re.search(r"(repair|timeout|immediate|disabled)", tombstone_gc_raw_string)[0]
        return tombstone_gc_mode

    @pytest.mark.parametrize("initial_gc_mode", ["repair", "timeout", "immediate", "disabled"])
    def test_restore_check_tombstone_gc_value(self, initial_gc_mode):
        node1, node2 = self.config_and_create_cluster(nodes=[2])
        mgr_cluster = self._create_mgr_cluster(node=node1, name=CLUSTER_NAME)
        self.cluster.stress(
            ['write', 'n=2500K', '-rate', 'threads=50', '-schema',
             "replication(strategy=NetworkTopologyStrategy,dc1=2)",
             'compaction(strategy=SizeTieredCompactionStrategy)'])
        session = self.patient_cql_connection(node1)
        session.execute("ALTER TABLE keyspace1.standard1 WITH tombstone_gc = {'mode':'%s'}" % initial_gc_mode)
        backup_task = self._backup_and_cleanup(healthy_node=node1, mgr_cluster=mgr_cluster,
                                               keyspace_table_and_key_range={"keyspace1": ["standard1"]})
        restore_task = mgr_cluster.run_restore_command(location_list=["s3:{}".format(DESTINATION_BUCKET)],
                                                       restore_data=True,
                                                       snapshot_tag=backup_task.get_snapshot_tag())
        restore_task.wait_for_status(list_status=[TaskStatus.RUNNING], timeout=35, step=1)  # Letting the restore start
        current_tombstone_gc_mode = self._get_tombstone_gc_mode(node1, "keyspace1", "standard1")
        assert current_tombstone_gc_mode == "disabled", \
            f"The manager did not alter the tombstone_gc mode of the restoring table to 'disabled'," \
            f" and instead it remained at {current_tombstone_gc_mode}"

        restore_task.wait_for_status(list_status=[TaskStatus.DONE], step=5)
        current_tombstone_gc_mode = self._get_tombstone_gc_mode(node1, "keyspace1", "standard1")
        # TODO: Once https://github.com/scylladb/scylla-manager/issues/3363 is solved, expect that the gc mode
        # will revert back to its original state
        assert current_tombstone_gc_mode == "disabled", \
            f"After the restore was completed, the value tombstone_gc mode of the restored table did not " \
            f"stay at 'disabled', and instead remained at {current_tombstone_gc_mode}"

    def test_restore_alter_batch_size(self):
        node1, node2 = self.config_and_create_cluster(nodes=2)
        mgr_cluster = self._create_mgr_cluster(node=node1, name=CLUSTER_NAME)
        self.cluster.stress(
            ['write', 'n=3500K', '-rate', 'threads=50', '-schema',
             'compaction(strategy=SizeTieredCompactionStrategy)'])
        backup_task = self._backup_and_cleanup(healthy_node=node1, mgr_cluster=mgr_cluster,
                                               keyspace_table_and_key_range={"keyspace1": ["standard1"]})
        restore_task = mgr_cluster.run_restore_command(location_list=[f"s3:{DESTINATION_BUCKET}"],
                                                       restore_data=True, batch_size=3,
                                                       snapshot_tag=backup_task.get_snapshot_tag())
        restore_task.wait_for_status(list_status=[TaskStatus.RUNNING], step=2)
        restore_task.stop()
        restore_task.update(batch_size=1)
        restore_task.start(continue_task=True)
        final_status = restore_task.wait_and_get_final_status(step=5)
        assert final_status == TaskStatus.DONE, \
            f"Restore task failed after altering the batch size: {restore_task.progress_details()}"

    def test_delete_keyspace_while_restore_is_paused(self):
        node1, node2 = self.config_and_create_cluster(nodes=2)
        mgr_cluster = self._create_mgr_cluster(node=node1, name=CLUSTER_NAME)
        self.cluster.stress(
            ['write', 'n=3500K', '-rate', 'threads=50', '-schema',
             'compaction(strategy=SizeTieredCompactionStrategy)'])
        backup_task = self._backup_and_cleanup(healthy_node=node1, mgr_cluster=mgr_cluster,
                                               keyspace_table_and_key_range={"keyspace1": ["standard1"]})
        restore_task = mgr_cluster.run_restore_command(location_list=[f"s3:{DESTINATION_BUCKET}"],
                                                       restore_data=True, snapshot_tag=backup_task.get_snapshot_tag())
        restore_task.wait_for_status(list_status=[TaskStatus.RUNNING], step=2)
        restore_task.stop()
        self._drop_table_and_delete_table_dir(keyspace_name="keyspace1", table_name="standard1", up_normal_node=node1)
        restore_task.start(continue_task=True)
        final_status = restore_task.wait_and_get_final_status(step=5)
        assert final_status == TaskStatus.ERROR,\
            f"Even though the restored keyspace was dropped while the restore task was paused, the task did not fail," \
            f" but it instead reached the status of {final_status}: {restore_task.full_progress_string()}"
        assert "unconfigured table" in restore_task.full_progress_string(), \
            f'The expected message "unconfigured table" did not appear in the output of task progress: ' \
            f'{restore_task.full_progress_string()}'

    def test_restore_after_deleting_file_from_s3(self):
        node1, node2 = self.config_and_create_cluster(nodes=2)
        mgr_cluster = self._create_mgr_cluster(node=node1, name=CLUSTER_NAME)
        self.cluster.stress(
            ['write', 'n=3500K', '-rate', 'threads=50', '-schema',
             'compaction(strategy=SizeTieredCompactionStrategy)'])
        backup_task = self._backup_and_cleanup(healthy_node=node1, mgr_cluster=mgr_cluster,
                                               keyspace_table_and_key_range={"keyspace1": ["standard1"]})
        for _ in range(15):
            self._delete_file_from_bucket(mgr_cluster.id, file_type="sst")
        restore_task = mgr_cluster.run_restore_command(location_list=[f"s3:{DESTINATION_BUCKET}"],
                                                       restore_data=True,
                                                       snapshot_tag=backup_task.get_snapshot_tag())
        final_status = restore_task.wait_and_get_final_status(step=5)
        assert final_status == TaskStatus.ERROR, \
            f"After deleting several files from the s3 snapshot directory, The restore task was expected to fail. " \
            f"However, it did not fail, and instead reached the status {final_status}"
        assert "object not found" in restore_task.full_progress_string(), \
            f"The restore task has failed as expected, but printed unexpected error message:\n" \
            f"{restore_task.full_progress_string()}"

    def test_restore_data_after_purge(self):
        key_ranges = [
            (1, 21),
            (21, 101),
            (101, 251),
            (251, 388)
        ]
        complete_key_range = (key_ranges[0][0], key_ranges[-1][1])
        keyspace_name = "ks"
        table_name = "cf1"
        node1, node2 = self.config_and_create_cluster(nodes=2)
        mgr_cluster = self._create_mgr_cluster(node=node1, name=CLUSTER_NAME)
        self.insert_data_from_ranges(healthy_node=node1,
                                     keyspace_table_and_key_range={keyspace_name: {table_name: key_ranges[0]}})
        backup_task = mgr_cluster.run_backup_command(keyspace_list=[keyspace_name],
                                                     location_list=[f"s3:{DESTINATION_BUCKET}"],
                                                     retention=2)
        backup_task.wait_for_status(list_status=[TaskStatus.DONE], step=5)
        for key_range in key_ranges[1:]:
            self.insert_data_from_ranges(healthy_node=node1,
                                         keyspace_table_and_key_range={keyspace_name: {table_name: key_range}})
            backup_task.start(continue_task=False)
            backup_task.wait_for_status(list_status=[TaskStatus.DONE], step=5)
        self.clean_up_tables(node=node1, keyspace_and_tables_dict={keyspace_name: [table_name]})
        restore_task = mgr_cluster.run_restore_command(location_list=[f"s3:{DESTINATION_BUCKET}"],
                                                       restore_data=True,
                                                       snapshot_tag=backup_task.get_snapshot_tag())
        final_status = restore_task.wait_and_get_final_status(step=5)
        assert final_status == TaskStatus.DONE
        self.verify_c1c2(keyspace_table_and_key_range={keyspace_name: {table_name: complete_key_range}}, node=node1)
