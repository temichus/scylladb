# coding: utf-8

from datetime import datetime
import os
from glob import glob

from cassandra import ConsistencyLevel
from nose.plugins.attrib import attr
from boto3 import client as boto_client
from unittest import skip

from tools import require
from dtest_scylla_manager import ScyllaManagerTool, ScyllaManagerError
from dtest_scylla_manager import TaskStatus
from scylla_tools import insert_c1c2, insert_c1c2_with_clustering
from dtest import Tester, debug, wait_for


CLUSTER_NAME = 'cluster1'
DESTINATION_BUCKET = 'backup-bucket'
FALSE_BUCKET = 'nonexistent_bucket'
C1_PREFIX = "value%d"
C2_PREFIX = "other_value%d"


class TestScyllaMgmtBackup(Tester):
    __test__ = True

    @classmethod
    def setUpClass(cls):
        minio_full_address = os.getenv("AWS_S3_ENDPOINT")
        cls.boto_client = boto_client(service_name='s3',
                                      aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
                                      aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
                                      endpoint_url=minio_full_address)
        try:
            cls.boto_client.create_bucket(Bucket=DESTINATION_BUCKET)
        except cls.boto_client.exceptions.BucketAlreadyOwnedByYou:
            pass

    def config_and_create_cluster(self, nodes):
        self.cluster.populate(nodes).start(wait_for_binary_proto=True, wait_other_notice=True)
        return self.cluster.nodelist()

    def _prepare_cluster_with_data(self, keyspace_table_and_key_range, number_of_nodes=2):
        node_list = self.config_and_create_cluster(nodes=number_of_nodes)
        self.insert_data_from_ranges(healthy_node=node_list[0], keyspace_table_and_key_range=keyspace_table_and_key_range)
        return node_list

    def create_c1_c2_with_clustering_key(self, session, keyspace_name, table_name, partition_key_name="pkey",
                                         partition_key_type="int", clustering_key_name="ckey", clustering_key_type="int"):
        session.execute(f"create table {keyspace_name}.{table_name} ( {partition_key_name} {partition_key_type}, "
                        f"{clustering_key_name} {clustering_key_type}, c1 text, c2 text, "
                        f"PRIMARY KEY({partition_key_name}, {clustering_key_name}));")

    def insert_data_from_ranges(self, healthy_node, keyspace_table_and_key_range, use_clustering_key=False, partition_key_value=1):
        """

        :param healthy_node: node in UN status
        :param keyspace_table_and_key_range: a dict that contains what rows to insert, per table in each keyspace, like so:
        {
            keyspace_name:
            {
                table_name: key_range[]
            }
        }
        :param use_clustering_key:
        :param partition_key_value:
        :return:
        """
        session = self.patient_cql_connection(healthy_node)
        keyspace_list_rows = session.execute("SELECT keyspace_name FROM system_schema.keyspaces;")
        keyspace_list = [row.keyspace_name for row in keyspace_list_rows]

        for keyspace in keyspace_table_and_key_range:
            if keyspace not in keyspace_list:
                self.create_ks(session=session, name=keyspace, rf=2)
            table_list_rows = session.execute(
                f"SELECT table_name FROM system_schema.tables where keyspace_name='{keyspace}';")
            table_list = [row.table_name for row in table_list_rows]

            for table, key_range in keyspace_table_and_key_range.get(keyspace, {}).items():
                if table not in table_list:
                    if use_clustering_key:
                        self.create_c1_c2_with_clustering_key(
                            session=session, keyspace_name=keyspace, table_name=table)
                    else:
                        self.create_cf(session=session, name="{}.{}".format(keyspace, table), read_repair=0.0,
                                       columns={'c1': 'text', 'c2': 'text'},
                                       dclocal_read_repair_chance=0.0, speculative_retry='NONE')

                if use_clustering_key:
                    insert_c1c2_with_clustering(session=session, clustering_key_values=range(*key_range),
                                                ks=keyspace, cf=table, partition_key_set_value=partition_key_value)
                else:
                    insert_c1c2(session=session, keys=range(*key_range), consistency=ConsistencyLevel.ALL,
                                c1_values=[C1_PREFIX % i for i in range(*key_range)],
                                c2_values=[C2_PREFIX % i for i in range(*key_range)],
                                ks=keyspace, cf=table)

    def delete_range(self, healthy_node, keyspace, table, key_range, clustering_key_name="ckey",
                     partition_key_name="pkey", partition_key_set_value=1):
        """
        Only works on a table that contains a clustering key
        :param healthy_node: node in UN state
        :param key_range: range of the clustering key values, the rows of which will be deleted
        :param partition_key_set_value: the permanent value of the partition key
        """
        session = self.patient_cql_connection(healthy_node)
        query = f"DELETE from {keyspace}.{table} where {clustering_key_name} >= {key_range[0]} and " \
                f"{clustering_key_name} <= {key_range[1]} and {partition_key_name} = {partition_key_set_value}"
        session.execute(query)

    def _create_mgr_cluster(self, node, name):
        manager_tool = ScyllaManagerTool(scylla_manager=self.cluster._scylla_manager)
        mgr_cluster = manager_tool.add_cluster(node=node, name=name)

        return mgr_cluster

    def clean_up_tables(self, node, keyspace_and_tables_dict):
        """
        :param node:
        :param keyspace_and_tables_dict: a dict that contains a list of names of the tables to truncate in each keyspace.
        :return:
        """
        session = self.patient_cql_connection(node)

        for keyspace, table_list in keyspace_and_tables_dict.items():
            for table in table_list:
                session.execute(f"TRUNCATE {keyspace}.{table}")

    def download_files_from_s3(self, destination, file_list, bucket_name=DESTINATION_BUCKET):
        for file_path in file_list:
            path = file_path.replace(f"s3://{bucket_name}/", "")
            file_name = file_path[file_path.rfind('/') + 1:]
            self.boto_client.download_file(bucket_name, path, os.path.join(destination, file_name))

    def restore_backup(self, node_list, mgr_cluster, snapshot_tag, keyspace_and_table_list):
        """
        At the moment, the function only supports the restoration of the LATEST backup of a specific backup task
        """
        per_node_backup_file_paths = mgr_cluster.get_backup_files_dict(snapshot_tag)
        for node in node_list:
            node_data_path = os.path.join(node.get_path(), 'data')
            node_id = node.hostid()
            for keyspace, tables in keyspace_and_table_list.items():
                keyspace_path = os.path.join(node_data_path, keyspace)
                for table in tables:
                    table_upload_path = glob(os.path.join(keyspace_path, table + '-*', 'upload'))[0]
                    self.download_files_from_s3(destination=table_upload_path,
                                                file_list=per_node_backup_file_paths[node_id][keyspace][table])
                    node.nodetool(f"refresh -- {keyspace} {table}")

    def restore_backup_from_backup_task(self, node_list, mgr_cluster, backup_task, keyspace_and_table_list):
        snapshot_tag = backup_task.get_snapshot_tag()
        self.restore_backup(node_list, mgr_cluster, snapshot_tag, keyspace_and_table_list)

    def compare_c1c2_rows_to_expected_results(self, row_list, key_range, table_name):
        key_values = []
        c1_values = []
        c2_values = []
        for row in row_list:
            key_values.append(row.key)
            c1_values.append(row.c1)
            c2_values.append(row.c2)
        result_dict = {
            "key": sorted(key_values), "c1": sorted(c1_values), "c2": sorted(c2_values)}

        expected_result_dict = {
            "key": range(*key_range),
            "c1": [C1_PREFIX % i for i in range(*key_range)],
            "c2": [C2_PREFIX % i for i in range(*key_range)]
        }
        for column in expected_result_dict:
            assert expected_result_dict[column] != result_dict[column]\
                , f"""post backup table {table_name} does not match expected data:
                      mismatched_column:{column}
                      pre backup values:{expected_result_dict[column]}
                      post backup values:{result_dict[column]}"""

    def verify_c1c2(self, keyspace_table_and_key_range, node):
        session = self.patient_cql_connection(node)
        for keyspace in keyspace_table_and_key_range:
            for table_name, key_range in keyspace_table_and_key_range.get(keyspace, {}).items():
                results = session.execute(f"select * from {keyspace}.{table_name}")
                self.compare_c1c2_rows_to_expected_results(results, key_range, table_name)

    def verify_lack_of_keys(self, keyspace_table_and_key_range, node, key_name="key"):
        session = self.patient_cql_connection(node)
        for keyspace in keyspace_table_and_key_range:
            for table_name, key_range in keyspace_table_and_key_range.get(keyspace, {}).items():
                results = session.execute(f"select {key_name} from {keyspace}.{table_name}")
                existing_key_set = {str(getattr(row, key_name)).replace("k", "") for row in results}
                missing_key_set = set([str(n) for n in range(*key_range)])
                wrongfully_existing_keys = existing_key_set.intersection(missing_key_set)
                assert not wrongfully_existing_keys, \
                    f"The table {'.'.join([keyspace, table_name])} contains the keys {wrongfully_existing_keys} " \
                    f"in column {key_name}, even though they are not suppose to exist in it"

    def clean_restore_and_verify_backup(self, backup_task, node_list, mgr_cluster, healthy_node,
                                        keyspace_table_and_key_range):
        per_keyspace_table_dict = {keyspace: list(table_dict.keys()) for keyspace, table_dict in
                                   keyspace_table_and_key_range.items()}
        self.clean_up_tables(healthy_node, per_keyspace_table_dict)
        self.restore_backup_from_backup_task(node_list, mgr_cluster, backup_task, per_keyspace_table_dict)
        self.verify_c1c2(keyspace_table_and_key_range, healthy_node)

    def clean_restore_and_verify_backup_with_stress(self, backup_task, node_list, mgr_cluster, healthy_node,
                                                    number_of_rows, per_keyspace_table_dict=None, threads=5):
        if not per_keyspace_table_dict:
            per_keyspace_table_dict = {"keyspace1": ["standard1"]}
        self.clean_up_tables(healthy_node, per_keyspace_table_dict)
        self.restore_backup_from_backup_task(node_list, mgr_cluster, backup_task, per_keyspace_table_dict)
        healthy_node.stress(['read', f'n={number_of_rows}', '-rate', f'threads={threads}'])

    @attr('scylla-manager')
    def test_basic_backup(self):
        keyspace_table_and_key_range = {"ks": {"cf1": (1, 21)}}
        node1, node2 = self._prepare_cluster_with_data(keyspace_table_and_key_range=keyspace_table_and_key_range)
        mgr_cluster = self._create_mgr_cluster(node=node1, name=CLUSTER_NAME)

        debug("Attempting to create a backup task with a location value, expecting it to success")
        backup_task = mgr_cluster.run_backup_command(location_list=["s3:{}".format(DESTINATION_BUCKET)],
                                                     keyspace_list=list(keyspace_table_and_key_range.keys()))
        backup_task.wait_for_status(list_status=[TaskStatus.DONE], timeout=1000, step=5)
        self.clean_restore_and_verify_backup(backup_task, self.cluster.nodelist(), mgr_cluster, node1,
                                             keyspace_table_and_key_range)

    @attr('scylla-manager')
    def test_backup_rate_limit_invalid(self):
        node1, node2 = self.config_and_create_cluster(nodes=2)
        mgr_cluster = self._create_mgr_cluster(node=node1, name=CLUSTER_NAME)

        debug("Attempting to create a backup task with an invalid rate limit value, expecting it to fail")
        try:
            mgr_cluster.run_backup_command(location_list=["s3:{}".format(DESTINATION_BUCKET)],
                                           rate_limit_list=['a'])
        except ScyllaManagerError as err:
            assert "invalid" in err.args[0] and "limit" in err.args[0], "Unexpected error: {}".format(err.args[0])
        else:
            assert False, "No error occurred when an invalid rate-limit is used in the sctool backup command"

    @skip("will return when minio bandwidth limiting is on")
    @attr('scylla-manager')
    def test_backup_start_date(self):
        keyspace_table_and_key_range = {"ks": {"cf1": (1, 21)}}
        node1, node2 = self._prepare_cluster_with_data(keyspace_table_and_key_range=keyspace_table_and_key_range)
        mgr_cluster = self._create_mgr_cluster(node=node1, name=CLUSTER_NAME)

        command_execution_time = datetime.now()
        backup_task = mgr_cluster.run_backup_command(location_list=["s3:{}".format(DESTINATION_BUCKET)],
                                                     keyspace_list=list(keyspace_table_and_key_range.keys()),
                                                     start_date="now+40s")
        next_run_string = backup_task.next_run
        next_run_time = datetime.strptime(next_run_string, "%d %b %y %H:%M:%S %Z")
        assert abs((next_run_time - command_execution_time).seconds) < 50, "The start time is not identical"

        backup_task.wait_for_status(list_status=[TaskStatus.RUNNING], timeout=100, step=2)
        task_start_time = datetime.now()
        assert abs((task_start_time - command_execution_time).seconds) < 50, "In practice, the start time of the " \
                                                                             "backup task did not match the requested" \
                                                                             "time"
        backup_task.wait_for_status(list_status=[TaskStatus.DONE], timeout=1000, step=5)
        self.clean_restore_and_verify_backup(backup_task, self.cluster.nodelist(), mgr_cluster, node1,
                                             keyspace_table_and_key_range)

    @attr('scylla-manager')
    def test_backup_multiple_keyspaces_and_tables(self):
        keyspace_table_and_key_range = {"ks1": {"cf1": (1, 21),
                                                "cf2": (1, 21)},
                                        "ks2": {"cf1": (1, 21),
                                                "cf2": (1, 21)},
                                        "ks3": {"cf1": (1, 21),
                                                "cf2": (1, 21)}}
        node1, node2 = self._prepare_cluster_with_data(keyspace_table_and_key_range=keyspace_table_and_key_range)
        mgr_cluster = self._create_mgr_cluster(node=node1, name=CLUSTER_NAME)

        debug("Attempting to create a backup task for a cluster with several keyspaces and column families,"
              " expecting it to succeed")
        backup_task = mgr_cluster.run_backup_command(location_list=["s3:{}".format(DESTINATION_BUCKET)],
                                                     keyspace_list=list(keyspace_table_and_key_range.keys()))
        backup_task.wait_for_status(list_status=[TaskStatus.DONE], timeout=1000, step=5)
        self.clean_restore_and_verify_backup(backup_task, self.cluster.nodelist(), mgr_cluster, node1,
                                             keyspace_table_and_key_range)

    @attr('scylla-manager')
    def test_backup_a_single_keyspace_and_glob_pattern(self):
        keyspace_table_and_key_range = {"ks1": {"cf1": (1, 21),
                                                "cf2": (1, 21)},
                                        "ks2": {"cf1": (1, 21),
                                                "cf2": (1, 21)},
                                        "ks3": {"cf1": (1, 21),
                                                "cf2": (1, 21)},
                                        "keyspace_for_glob": {"cf1": (1, 21),
                                                              "cf2": (1, 21)},
                                        "other_keyspace": {"cf1": (1, 21),
                                                           "cf2": (1, 21)}}
        node1, node2 = self._prepare_cluster_with_data(keyspace_table_and_key_range=keyspace_table_and_key_range)
        mgr_cluster = self._create_mgr_cluster(node=node1, name=CLUSTER_NAME)

        debug("Attempting to create a backup task for a cluster with several keyspaces and column families,"
              " expecting it to succeed")
        backup_task = mgr_cluster.run_backup_command(location_list=["s3:{}".format(DESTINATION_BUCKET)],
                                                     keyspace_list=['ks1', "*_for_glob"])
        backup_task.wait_for_status(list_status=[TaskStatus.DONE], timeout=1000, step=5)
        keyspaces_ranges_to_verify = {
            "ks1": keyspace_table_and_key_range["ks1"],
            "keyspace_for_glob": keyspace_table_and_key_range["keyspace_for_glob"]
        }
        self.clean_restore_and_verify_backup(backup_task, self.cluster.nodelist(), mgr_cluster, node1,
                                             keyspaces_ranges_to_verify)

    @attr('scylla-manager')
    def test_backup_nonexistent_bucket(self):
        node1, node2 = self._prepare_cluster_with_data(keyspace_table_and_key_range={"ks": {"cf1": (1, 21)}})

        mgr_cluster = self._create_mgr_cluster(node=node1, name=CLUSTER_NAME)

        debug("Attempting to create a backup task with a nonexistent bucket in the location value, expecting it to fail")
        try:
            mgr_cluster.run_backup_command(location_list=["s3:{}".format(FALSE_BUCKET)])
        except ScyllaManagerError as err:
            assert "invalid location" in err.args[0], "Unexpected error: {}".format(err.args[0])
        else:
            raise ScyllaManagerError("No error occurred when a nonexistent bucket was used as a location"
                                     " in a manager backup command")

    def _backup_nonexistent_keyspace_template(self, keyspace_filter_string):
        node1, node2 = self._prepare_cluster_with_data(keyspace_table_and_key_range={"ks": {"cf1": (1, 21)}})

        mgr_cluster = self._create_mgr_cluster(node=node1, name=CLUSTER_NAME)

        debug("Attempting to create a backup task with a nonexistent keyspace in the keyspace value,"
              " expecting it to fail")
        try:
            mgr_cluster.run_backup_command(location_list=["s3:{}".format(DESTINATION_BUCKET)],
                                           keyspace_list=[keyspace_filter_string])
        except ScyllaManagerError as err:
            assert "no matching keyspaces"\
                   in err.args[0], "The manager justifiably failed to backup a nonexistent keyspace, but the error" \
                                   " message does not describe the error properly"
        else:
            raise ScyllaManagerError("No error occurred when a non existent keyspace was used in a keyspace flag"
                                     " in a manager backup command")

    @attr('scylla-manager')
    def test_backup_nonexistent_keyspace(self):
        self._backup_nonexistent_keyspace_template("Nonexistent_keyspace")

    @attr('scylla-manager')
    def test_backup_nonexistent_keyspace_glob(self):
        self._backup_nonexistent_keyspace_template("Nonexistent*")

    def _backup_nonexistent_datacenter_template(self, dc_filter_string):
        node1, node2 = self._prepare_cluster_with_data(keyspace_table_and_key_range={"ks": {"cf1": (1, 21)}},
                                                       number_of_nodes=[1, 1])

        mgr_cluster = self._create_mgr_cluster(node=node1, name=CLUSTER_NAME)

        debug("Attempting to create a backup task with a nonexistent keyspace in the keyspace value,"
              " expecting it to fail")
        try:
            mgr_cluster.run_backup_command(location_list=["s3:{}".format(DESTINATION_BUCKET)],
                                           dc_list=[dc_filter_string])
        except ScyllaManagerError as err:
            assert "no matching DCs" in err.args[0]
        else:
            raise ScyllaManagerError("No error occurred when a non existent database was used in a database flag"
                                     " in a manager backup command")

    @attr('scylla-manager')
    def test_backup_nonexistent_datacenter(self):
        self._backup_nonexistent_datacenter_template("nonexistent")

    @attr('scylla-manager')
    def test_backup_nonexistent_datacenter_glob(self):
        self._backup_nonexistent_datacenter_template("nonexistent*")

    @attr('scylla-manager')
    def test_backup_task_progress(self):
        node1, node2 = self.config_and_create_cluster(nodes=2)

        mgr_cluster = self._create_mgr_cluster(node=node1, name=CLUSTER_NAME)

        self._create_stress_compatible_table(node=node1)
        self.cluster.stress(['write', 'n=5000K', '-rate', 'threads=50'])

        backup_task = mgr_cluster.run_backup_command(location_list=["s3:{}".format(DESTINATION_BUCKET)])

        backup_task.wait_for_status(list_status=[TaskStatus.RUNNING], timeout=100, step=1)
        progress_percentage = backup_task.progress
        assert progress_percentage != "N/A", "couldn't read the progress of the backup test"

        backup_task.wait_for_status(list_status=[TaskStatus.DONE], timeout=1000, step=5)
        progress_percentage = backup_task.progress
        assert progress_percentage == '100%', "The percentage of the backup task at its end was not 100%"
        self.clean_restore_and_verify_backup_with_stress(backup_task=backup_task, node_list=self.cluster.nodelist(),
                                                         mgr_cluster=mgr_cluster, healthy_node=node1,
                                                         number_of_rows="5000K", threads=50)

    @staticmethod
    def extract_all_snapshot_names(output, ignore_manager_snapshots=True):
        values_only_lines = output.split('\n')[2:-4]
        # Remove unnecessary lines from:

        # Snapshot Details:
        # Snapshot name                Keyspace name      Column family name              True size Size on disk
        # 1577100708489-scheduler_task scylla_manager     scheduler_task                  0 bytes   0 bytes
        # sm_manual_snapshot           system             local                           17.66 KB  17.66 KB
        # ...
        #
        # Total TrueDiskSpaceUsed: 34.38 K
        snapshot_names = [line[:line.find(' ')] for line in values_only_lines]
        if ignore_manager_snapshots:
            snapshot_names = [name for name in snapshot_names if "scheduler_task" not in name]
        return snapshot_names

    @attr('scylla-manager')
    def test_backup_nodetool_snapshots_before_backup(self):
        keyspace_table_and_key_range = {"ks": {"cf1": (1, 21)}}
        node1, node2 = self._prepare_cluster_with_data(keyspace_table_and_key_range=keyspace_table_and_key_range)
        node_list = node1, node2

        mgr_cluster = self._create_mgr_cluster(node=node1, name=CLUSTER_NAME)

        manual_snapshot_name = "sm_manual_snapshot"
        self.cluster.nodetool("snapshot -t {}".format(manual_snapshot_name))
        per_node_pre_backup_snapshot_lists = {node.name: sorted(self.extract_all_snapshot_names(
            node.nodetool("listsnapshots", capture_output=True)[0])) for node in node_list}

        backup_task = mgr_cluster.run_backup_command(location_list=["s3:{}".format(DESTINATION_BUCKET)],
                                                     keyspace_list=list(keyspace_table_and_key_range.keys()))
        backup_task.wait_for_status(list_status=[TaskStatus.DONE], timeout=1000, step=5)
        per_node_post_backup_snapshot_lists = {node.name: sorted(self.extract_all_snapshot_names(
            node.nodetool("listsnapshots", capture_output=True)[0])) for node in node_list}

        for node in node_list:
            assert per_node_post_backup_snapshot_lists == per_node_pre_backup_snapshot_lists, \
                "The list of snapshots in {} changed after the manager backup:" \
                "\nPre backup snapshot list:{}\nPost backup_snapshot list:{}".format(
                    node.name,
                    per_node_pre_backup_snapshot_lists[node.name],
                    per_node_post_backup_snapshot_lists[node.name]
                )
        self.clean_restore_and_verify_backup(backup_task, self.cluster.nodelist(), mgr_cluster, node1,
                                             keyspace_table_and_key_range)

    @skip("will return when minio bandwidth limiting is on")
    @attr('scylla-manager')
    def test_multiple_backups_task_then_restore(self):
        first_keyspace_table_and_key_range = {"ks": {"cf1": (1, 21)}}
        second_keyspace_table_and_key_range = {"ks": {"cf1": (31, 56)}}
        third_keyspace_table_and_key_range = {"ks": {"cf1": (101, 131)}}
        node1, node2 = self._prepare_cluster_with_data(keyspace_table_and_key_range=first_keyspace_table_and_key_range)
        mgr_cluster = self._create_mgr_cluster(node=node1, name=CLUSTER_NAME)

        debug("Attempting to create a backup task with a location value, expecting it to success")
        backup_task = mgr_cluster.run_backup_command(location_list=["s3:{}".format(DESTINATION_BUCKET)],
                                                     keyspace_list=['ks'])
        backup_task.wait_for_status(list_status=[TaskStatus.DONE], timeout=1000, step=5)

        self.insert_data_from_ranges(node1, second_keyspace_table_and_key_range)
        backup_task.start(continue_attr="false")
        backup_task.wait_for_status(list_status=[TaskStatus.DONE], timeout=1000, step=5)
        second_run_snapshot_tag = backup_task.get_snapshot_tag()

        self.insert_data_from_ranges(node1, third_keyspace_table_and_key_range)
        backup_task.start(continue_attr="false")
        backup_task.wait_for_status(list_status=[TaskStatus.DONE], timeout=1000, step=5)

        self.clean_up_tables(node1, {"ks": ["cf1"]})
        self.restore_backup(node_list=self.cluster.nodelist(), mgr_cluster=mgr_cluster,
                            snapshot_tag=second_run_snapshot_tag, keyspace_and_table_list={"ks": ["cf1"]})
        self.verify_c1c2(first_keyspace_table_and_key_range, node1)
        self.verify_c1c2(second_keyspace_table_and_key_range, node1)
        self.verify_lack_of_keys(third_keyspace_table_and_key_range, node1)

    @require("#1551")
    @attr('scylla-manager')
    def test_shutting_down_nodes_during_backup(self):
        keyspace_table_and_key_range = {"ks": {"cf1": (1, 21)}}
        node1, node2, node3, node4, node5 = self._prepare_cluster_with_data(
            keyspace_table_and_key_range=keyspace_table_and_key_range, number_of_nodes=5)

        mgr_cluster = self._create_mgr_cluster(node=node1, name=CLUSTER_NAME)

        backup_task = mgr_cluster.run_backup_command(location_list=["s3:{}".format(DESTINATION_BUCKET)])
        backup_task.wait_for_status(list_status=[TaskStatus.RUNNING], timeout=600, step=0.1)

        node4.stop(wait_other_notice=True)
        node5.stop(wait_other_notice=True)
        backup_task.wait_for_status(list_status=[TaskStatus.ERROR])
        node4.start(wait_other_notice=True, wait_for_binary_proto=True)
        node5.start(wait_other_notice=True, wait_for_binary_proto=True)

        backup_task.wait_for_status(list_status=[TaskStatus.ERROR])
        backup_task.start(continue_attr="true")

        backup_task.wait_for_status(list_status=[TaskStatus.DONE])
        self.clean_restore_and_verify_backup(backup_task, self.cluster.nodelist(), mgr_cluster, node1,
                                             keyspace_table_and_key_range)

    @require("#1551")
    @attr('scylla-manager')
    def test_shutting_down_node_before_backup(self):
        keyspace_table_and_key_range = {"ks": {"cf1": (1, 21)}}
        node1, node2, node3 = self._prepare_cluster_with_data(keyspace_table_and_key_range=keyspace_table_and_key_range,
                                                              number_of_nodes=3)

        mgr_cluster = self._create_mgr_cluster(node=node1, name=CLUSTER_NAME)
        node3.stop(wait_other_notice=True)

        backup_task = mgr_cluster.run_backup_command(location_list=["s3:{}".format(DESTINATION_BUCKET)])
        backup_task.wait_for_status(list_status=[TaskStatus.RUNNING], timeout=600, step=0.1)

        backup_task.wait_for_status(list_status=[TaskStatus.DONE])
        self.clean_restore_and_verify_backup(backup_task, self.cluster.nodelist(), mgr_cluster, node1, keyspace_table_and_key_range)

    @staticmethod
    def _get_node_status(node_address, functioning_node, tolerate_missing):
        output_string, err = functioning_node.nodetool("status", capture_output=True)
        assert not err, "nodetool status execution failed"
        print(output_string)
        if node_address not in output_string:
            if tolerate_missing:
                debug("node {} was not found in nodetool status, retrying")
                return "Nonexistent"
            assert False, "Could not find requested node ({}) in nodetool status".format(node_address)
        output_lines = output_string.split("\n")
        relevant_line = [line for line in output_lines if node_address in line][0]
        return relevant_line[:2]

    def _is_node_at_status(self, node_address, functioning_node, desirable_status, tolerate_missing):
        return self._get_node_status(node_address, functioning_node, tolerate_missing) == desirable_status

    def _wait_until_node_reaches_status(self, node, functioning_node, desirable_status, tolerate_missing,
                                        timeout=100, step=1):
        text = "Waiting until node {} reaches status of: {}".format(node.name, desirable_status)
        is_status_reached = wait_for(func=self._is_node_at_status, step=step, text=text, timeout=timeout, throw_exc=True,
                                     node_address=node.address(), functioning_node=functioning_node,
                                     desirable_status=desirable_status, tolerate_missing=tolerate_missing)
        return is_status_reached

    def _create_stress_compatible_table(self, node):
        session = self.patient_cql_connection(node)
        session.execute("""CREATE KEYSPACE "keyspace1" WITH replication = {
        'class': 'SimpleStrategy',
        'replication_factor': '1'};""")
        session.execute("""USE "keyspace1";""")
        session.execute("""CREATE TABLE "standard1" (
        key blob,
        "C0" blob,
        "C1" blob,
        "C2" blob,
        "C3" blob,
        "C4" blob,
        PRIMARY KEY (key)
        ) WITH
        bloom_filter_fp_chance=0.010000 AND
        caching='KEYS_ONLY' AND
        comment='' AND
        dclocal_read_repair_chance=0.000000 AND
        gc_grace_seconds=864000 AND
        index_interval=128 AND
        read_repair_chance=0.000000 AND
        replicate_on_write='true' AND
        default_time_to_live=0 AND
        speculative_retry='99.0PERCENTILE' AND
        memtable_flush_period_in_ms=0 AND
        compaction={'class': 'SizeTieredCompactionStrategy'};""")

    @skip("will return when minio bandwidth limiting is on")
    @attr('scylla-manager')
    def test_backup_while_adding_node_to_cluster(self):
        node1, node2, node3 = self.config_and_create_cluster(nodes=3)

        mgr_cluster = self._create_mgr_cluster(node=node1, name=CLUSTER_NAME)

        self._create_stress_compatible_table(node=node1)

        # C-S for two minutes
        self.cluster.stress(['write', 'n=10000K', '-rate', 'threads=50'])

        node4 = self.cluster.new_node(4, auto_bootstrap=True, add_node=True, is_seed=False)
        node4.start()
        self._wait_until_node_reaches_status(node4, node1, "UJ", tolerate_missing=True)

        backup_task = mgr_cluster.run_backup_command(location_list=["s3:{}".format(DESTINATION_BUCKET)])
        backup_task.wait_for_status(list_status=[TaskStatus.DONE], timeout=600, step=5)

        self.clean_restore_and_verify_backup_with_stress(backup_task=backup_task, node_list=self.cluster.nodelist(),
                                                         mgr_cluster=mgr_cluster, healthy_node=node1,
                                                         number_of_rows="10000K", threads=50)

    @attr('scylla-manager')
    def test_backup_while_node_is_drained(self):
        keyspace_table_and_key_range = {"ks": {"cf1": (1, 21)}}
        node1, node2, node3 = self._prepare_cluster_with_data(
            keyspace_table_and_key_range=keyspace_table_and_key_range, number_of_nodes=3)

        mgr_cluster = self._create_mgr_cluster(node=node1, name=CLUSTER_NAME)

        node3.nodetool("drain")

        self._wait_until_node_reaches_status(node=node3, functioning_node=node1, desirable_status="DN",
                                             tolerate_missing=False)
        backup_task = mgr_cluster.run_backup_command(location_list=["s3:{}".format(DESTINATION_BUCKET)],
                                                     keyspace_list=['ks'])
        backup_task.wait_for_status(list_status=[TaskStatus.DONE], timeout=600, step=5)
        node3.stop(wait_other_notice=False)
        node3.start(wait_other_notice=True, wait_for_binary_proto=True)
        self.clean_restore_and_verify_backup(backup_task, self.cluster.nodelist(), mgr_cluster, node1,
                                             keyspace_table_and_key_range)

    @skip("will return when minio bandwidth limiting is on")
    @attr('scylla-manager')
    def test_restart_node_during_backup(self):
        node1, node2, node3 = self._prepare_cluster_with_data(
            keyspace_table_and_key_range={"ks": {"cf1": (1, 21)}}, number_of_nodes=3)

        mgr_cluster = self._create_mgr_cluster(node=node1, name=CLUSTER_NAME)

        backup_task = mgr_cluster.run_backup_command(location_list=["s3:{}".format(DESTINATION_BUCKET)],
                                                     keyspace_list=['ks'])
        backup_task.wait_for_status(list_status=[TaskStatus.RUNNING], timeout=100, step=1)
        node3.stop(wait_other_notice=True)
        node3.start(wait_other_notice=True, wait_for_binary_proto=True)
        backup_task.wait_for_status(list_status=[TaskStatus.ERROR], timeout=600, step=5)

    @skip("will return when minio bandwidth limiting is on")
    @attr('scylla-manager')
    def test_restart_agent_during_backup(self):
        node1, node2, node3 = self._prepare_cluster_with_data(
            keyspace_table_and_key_range={"ks": {"cf1": (1, 21)}}, number_of_nodes=3)

        mgr_cluster = self._create_mgr_cluster(node=node1, name=CLUSTER_NAME)

        backup_task = mgr_cluster.run_backup_command(location_list=["s3:{}".format(DESTINATION_BUCKET)],
                                                     keyspace_list=['ks'])
        backup_task.wait_for_status(list_status=[TaskStatus.RUNNING], timeout=100, step=1)
        node3.restart_scylla_manager_agent(gently=True)
        backup_task.wait_for_status(list_status=[TaskStatus.ERROR], timeout=600, step=5)

    @skip("will return when minio bandwidth limiting is on")
    @attr('scylla-manager')
    def test_restart_manager_server_during_backup(self):
        keyspace_table_and_key_range = {"ks": {"cf1": (1, 21)}}
        node1, node2, node3 = self._prepare_cluster_with_data(
            keyspace_table_and_key_range=keyspace_table_and_key_range, number_of_nodes=3)
        manager_tool = ScyllaManagerTool(scylla_manager=self.cluster._scylla_manager)
        mgr_cluster = manager_tool.add_cluster(node=node1, name=CLUSTER_NAME)

        backup_task = mgr_cluster.run_backup_command(location_list=["s3:{}".format(DESTINATION_BUCKET)],
                                                     keyspace_list=['ks'])
        backup_task.wait_for_status(list_status=[TaskStatus.RUNNING], timeout=100, step=1)
        manager_tool.restart_manager_server(gently=True)
        backup_task.wait_for_status(list_status=[TaskStatus.ABORTED], timeout=100, step=1)

        backup_task.start(continue_attr="true")
        backup_task.wait_for_status(list_status=[TaskStatus.DONE], timeout=600, step=5)
        self.clean_restore_and_verify_backup(backup_task, self.cluster.nodelist(), mgr_cluster, node1,
                                             keyspace_table_and_key_range)

    @skip("will return when minio bandwidth limiting is on")
    @attr('scylla-manager')
    def test_nodetool_clearsnapshot_during_backup(self):
        node1, node2, node3 = self.config_and_create_cluster(nodes=3)

        mgr_cluster = self._create_mgr_cluster(node=node1, name=CLUSTER_NAME)

        self._create_stress_compatible_table(node=node1)
        self.cluster.stress(['write', 'n=5000K', '-rate', 'threads=50'])

        backup_task = mgr_cluster.run_backup_command(location_list=["s3:{}".format(DESTINATION_BUCKET)],
                                                     keyspace_list=['keyspace1'])
        backup_task.wait_for_status(list_status=[TaskStatus.RUNNING], timeout=100, step=1)
        self.cluster.nodetool("clearsnapshot")
        backup_task.wait_for_status(list_status=[TaskStatus.ERROR], timeout=100, step=1)

    def insert_data_over_multiple_queries(self, healthy_node, keyspace_table_and_key_range, num_of_queries=10,
                                          use_clustering_key=False, partition_key_value=1):
        for keyspace in keyspace_table_and_key_range:
            for table, key_range in keyspace_table_and_key_range.get(keyspace, {}).items():
                split_key_ranges = list(range(key_range[0], key_range[1], (key_range[1] - key_range[0]) // num_of_queries))
                split_key_ranges.append(key_range[1])
                for n in range(len(split_key_ranges[:-1])):
                    self.insert_data_from_ranges(healthy_node=healthy_node, keyspace_table_and_key_range=
                                                 {keyspace: {table: (split_key_ranges[n], split_key_ranges[n + 1])}},
                                                 use_clustering_key=use_clustering_key,
                                                 partition_key_value=partition_key_value)
                    healthy_node.nodetool("flush")

    @attr('scylla-manager')
    def test_restore_after_purge(self):
        node1, node2 = self.config_and_create_cluster(nodes=2)
        mgr_cluster = self._create_mgr_cluster(node=node1, name=CLUSTER_NAME)
        snapshot_tags = []

        self.insert_data_over_multiple_queries(
            healthy_node=node1, keyspace_table_and_key_range={"ks": {"cf1": (1, 1001)}}, use_clustering_key=True)
        backup_task = mgr_cluster.run_backup_command(location_list=["s3:{}".format(DESTINATION_BUCKET)],
                                                     keyspace_list=['ks'],
                                                     interval="1h",
                                                     num_retries="0",
                                                     retention="3")
        backup_task.wait_for_status(list_status=[TaskStatus.DONE], step=5)
        snapshot_tags.append(backup_task.get_snapshot_tag())
        self.delete_range(node1, keyspace="ks", table="cf1", key_range=(1, 1000))

        for i in range(1, 4):
            self.insert_data_over_multiple_queries(
                healthy_node=node1, keyspace_table_and_key_range={"ks": {"cf1": (i*1000+1, i*1000 + 1001)}},
                use_clustering_key=True)
            backup_task.start(continue_attr=False)
            backup_task.wait_for_status(list_status=[TaskStatus.DONE], step=5)
            snapshot_tags.append(backup_task.get_snapshot_tag())

        self.clean_up_tables(node1, {"ks": ["cf1"]})
        self.restore_backup(node_list=self.cluster.nodelist(), mgr_cluster=mgr_cluster,
                            snapshot_tag=snapshot_tags[1], keyspace_and_table_list={"ks": ["cf1"]})

        self.verify_lack_of_keys(keyspace_table_and_key_range={"ks": {"cf1": (1, 1001)}}, node=node1, key_name="ckey")
