# coding: utf-8

from datetime import datetime
import os
from glob import glob

from cassandra import ConsistencyLevel
from nose.plugins.attrib import attr
from boto3 import client as boto_client

from dtest_scylla_manager import ScyllaManagerTool, ScyllaManagerError
from dtest_scylla_manager import TaskStatus
from scylla_tools import insert_c1c2
from dtest import Tester, debug


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
        self.cluster.populate(nodes).start(wait_for_binary_proto=False, wait_other_notice=False)
        return self.cluster.nodelist()

    def _prepare_cluster_with_data(self, keyspace_table_and_key_range, number_of_nodes=2):
        node_list = self.config_and_create_cluster(nodes=number_of_nodes)
        session = self.patient_cql_connection(node_list[0])
        for keyspace in keyspace_table_and_key_range:
            self.create_ks(session=session, name=keyspace, rf=2)
            for table_name, key_range in keyspace_table_and_key_range.get(keyspace, {}).items():
                self.create_cf(session=session, name="{}.{}".format(keyspace, table_name), read_repair=0.0,
                               columns={'c1': 'text', 'c2': 'text'},
                               dclocal_read_repair_chance=0.0, speculative_retry='NONE')
                insert_c1c2(session=session, keys=range(*key_range), consistency=ConsistencyLevel.ALL,
                            c1_values=[C1_PREFIX % i for i in range(*key_range)],
                            c2_values=[C2_PREFIX % i for i in range(*key_range)],
                            ks=keyspace, cf=table_name)
        return node_list

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

    def restore_backup(self, node_list, backup_task, keyspace_and_table_list):
        """
        At the moment, the function only supports the restoration of the LATEST backup of a specific backup task
        """
        snapshot_tag = backup_task.get_snapshot_tag()
        per_node_backup_file_paths = backup_task.get_backup_files_dict(snapshot_tag)
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

    def clean_restore_and_verify_backup(self, backup_task, node_list, healthy_node, keyspace_table_and_key_range):
        per_keyspace_table_dict = {keyspace: list(table_dict.keys()) for keyspace, table_dict in keyspace_table_and_key_range.items()}
        self.clean_up_tables(healthy_node, per_keyspace_table_dict)
        self.restore_backup(node_list, backup_task, per_keyspace_table_dict)
        self.verify_c1c2(keyspace_table_and_key_range, healthy_node)

    @attr('scylla-manager')
    def test_basic_backup(self):
        keyspace_table_and_key_range = {"ks": {"cf1": (1, 21)}}
        node1, node2 = self._prepare_cluster_with_data(keyspace_table_and_key_range=keyspace_table_and_key_range)
        mgr_cluster = self._create_mgr_cluster(node=node1, name=CLUSTER_NAME)

        debug("Attempting to create a backup task with a location value, expecting it to success")
        backup_task = mgr_cluster.run_backup_command({"location": ["s3:{}".format(DESTINATION_BUCKET)],
                                                      "keyspace": list(keyspace_table_and_key_range.keys())})
        backup_task.wait_for_status(list_status=[TaskStatus.DONE], timeout=1000, step=5)
        self.clean_restore_and_verify_backup(backup_task, self.cluster.nodelist(), node1, keyspace_table_and_key_range)

    @attr('scylla-manager')
    def test_backup_rate_limit_invalid(self):
        node1, node2 = self.config_and_create_cluster(nodes=2)
        mgr_cluster = self._create_mgr_cluster(node=node1, name=CLUSTER_NAME)

        debug("Attempting to create a backup task with an invalid rate limit value, expecting it to fail")
        try:
            mgr_cluster.run_backup_command({"location": ["s3:{}".format(DESTINATION_BUCKET)],
                                           "rate-limit": ['a']})
        except ScyllaManagerError as err:
            assert "invalid" in err.args[0] and "limit" in err.args[0], "Unexpected error: {}".format(err.args[0])
        else:
            assert False, "No error occurred when an invalid rate-limit is used in the sctool backup command"

    @attr('scylla-manager')
    def test_backup_start_date(self):
        keyspace_table_and_key_range = {"ks": {"cf1": (1, 21)}}
        node1, node2 = self._prepare_cluster_with_data(keyspace_table_and_key_range=keyspace_table_and_key_range)
        mgr_cluster = self._create_mgr_cluster(node=node1, name=CLUSTER_NAME)

        command_execution_time = datetime.now()
        backup_task = mgr_cluster.run_backup_command({"location": ["s3:{}".format(DESTINATION_BUCKET)],
                                                      "keyspace": list(keyspace_table_and_key_range.keys()),
                                                      "start-date": "now+40s"})
        next_run_string = backup_task.next_run
        next_run_time = datetime.strptime(next_run_string, "%d %b %y %H:%M:%S %Z")
        assert abs((next_run_time - command_execution_time).seconds) < 50, "The start time is not identical"

        backup_task.wait_for_status(list_status=[TaskStatus.RUNNING], timeout=100, step=2)
        task_start_time = datetime.now()
        assert abs((task_start_time - command_execution_time).seconds) < 50, "In practice, the start time of the " \
                                                                             "backup task did not match the requested" \
                                                                             "time"
        backup_task.wait_for_status(list_status=[TaskStatus.DONE], timeout=1000, step=5)
        self.clean_restore_and_verify_backup(backup_task, self.cluster.nodelist(), node1, keyspace_table_and_key_range)


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
        backup_task = mgr_cluster.run_backup_command({"location": ["s3:{}".format(DESTINATION_BUCKET)],
                                                      "keyspace": list(keyspace_table_and_key_range.keys())})
        backup_task.wait_for_status(list_status=[TaskStatus.DONE], timeout=1000, step=5)
        self.clean_restore_and_verify_backup(backup_task, self.cluster.nodelist(), node1, keyspace_table_and_key_range)

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
        backup_task = mgr_cluster.run_backup_command({"location": ["s3:{}".format(DESTINATION_BUCKET)],
                                                     "keyspace": ['ks1', "*_for_glob"]})
        backup_task.wait_for_status(list_status=[TaskStatus.DONE], timeout=1000, step=5)
        keyspaces_ranges_to_verify = {
            "ks1": keyspace_table_and_key_range["ks1"],
            "keyspace_for_glob": keyspace_table_and_key_range["keyspace_for_glob"]
        }
        self.clean_restore_and_verify_backup(backup_task, self.cluster.nodelist(), node1, keyspaces_ranges_to_verify)

    @attr('scylla-manager')
    def test_backup_nonexistent_bucket(self):
        node1, node2 = self._prepare_cluster_with_data(keyspace_table_and_key_range={"ks": {"cf1": (1, 21)}})

        mgr_cluster = self._create_mgr_cluster(node=node1, name=CLUSTER_NAME)

        debug("Attempting to create a backup task with a nonexistent bucket in the location value, expecting it to fail")
        try:
            mgr_cluster.run_backup_command({"location": ["s3:{}".format(FALSE_BUCKET)]})
        except ScyllaManagerError as err:
            assert "invalid location" in err.args[0], "Unexpected error: {}".format(err.args[0])
        else:
            raise ScyllaManagerError("No error occurred when a nonexistent bucket was used as a location"
                                     " in a manager backup command")

    @attr('scylla-cluster')
    def _backup_nonexistent_keyspace_template(self, keyspace_filter_string):
        node1, node2 = self._prepare_cluster_with_data(keyspace_table_and_key_range={"ks": {"cf1": (1, 21)}})

        mgr_cluster = self._create_mgr_cluster(node=node1, name=CLUSTER_NAME)

        debug("Attempting to create a backup task with a nonexistent keyspace in the keyspace value,"
              " expecting it to fail")
        try:
            mgr_cluster.run_backup_command({"location": ["s3:{}".format(DESTINATION_BUCKET)],
                                            "keyspace": keyspace_filter_string})
        except ScyllaManagerError as err:
            assert "no matching keyspaces"\
                   in err.args[0], "The manager justifiably failed to backup a nonexistent keyspace, but the error" \
                                   " message does not describe the error properly"
        else:
            raise ScyllaManagerError("No error occurred when a non existent keyspace was used in a keyspace flag"
                                     " in a manager backup command")

    @attr('scylla-cluster')
    def test_backup_nonexistent_keyspace(self):
        self._backup_nonexistent_keyspace_template("Nonexistent_keyspace")

    @attr('scylla-cluster')
    def test_backup_nonexistent_keyspace_glob(self):
        self._backup_nonexistent_keyspace_template("Nonexistent*")

    def _backup_nonexistent_datacenter_template(self, dc_filter_string):
        node1, node2 = self._prepare_cluster_with_data(keyspace_table_and_key_range={"ks": {"cf1": (1, 21)}},
                                                       number_of_nodes=[1, 1])

        mgr_cluster = self._create_mgr_cluster(node=node1, name=CLUSTER_NAME)

        debug("Attempting to create a backup task with a nonexistent keyspace in the keyspace value,"
              " expecting it to fail")
        try:
            mgr_cluster.run_backup_command({"location": ["s3:{}".format(DESTINATION_BUCKET)],
                                            "dc": dc_filter_string})
        except ScyllaManagerError as err:
            assert "no matching DCs" in err.args[0]
        else:
            raise ScyllaManagerError("No error occurred when a non existent database was used in a database flag"
                                     " in a manager backup command")

    @attr('scylla-cluster')
    def test_backup_nonexistent_datacenter(self):
        self._backup_nonexistent_datacenter_template("nonexistent")

    @attr('scylla-cluster')
    def test_backup_nonexistent_datacenter_glob(self):
        self._backup_nonexistent_datacenter_template("nonexistent*")

    @attr('scylla-manager')
    def test_backup_task_progress(self):
        keyspace_table_and_key_range = {"ks": {"cf1": (1, 21)}}
        node1, node2 = self._prepare_cluster_with_data(keyspace_table_and_key_range=keyspace_table_and_key_range)

        mgr_cluster = self._create_mgr_cluster(node=node1, name=CLUSTER_NAME)

        backup_task = mgr_cluster.run_backup_command({"location": ["s3:{}".format(DESTINATION_BUCKET)],
                                                      "keyspace": list(keyspace_table_and_key_range.keys())})

        backup_task.wait_for_status(list_status=[TaskStatus.RUNNING], timeout=100, step=3)
        progress_percentage = backup_task.progress
        assert progress_percentage != "N/A", "couldn't read the progress of the backup test"

        backup_task.wait_for_status(list_status=[TaskStatus.DONE], timeout=1000, step=5)
        progress_percentage = backup_task.progress
        assert progress_percentage == '100%', "The percentage of the backup task at its end was not 100%"
        self.clean_restore_and_verify_backup(backup_task, self.cluster.nodelist(), node1, keyspace_table_and_key_range)

    @staticmethod
    def extract_all_snapshot_names(output):
        values_only_lines = output.split('\n')[2:-2]
        # Remove unnecessary lines from:

        # Snapshot Details:
        # Snapshot name                Keyspace name      Column family name              True size Size on disk
        # 1577100708489-scheduler_task scylla_manager     scheduler_task                  0 bytes   0 bytes
        # sm_manual_snapshot           system             local                           17.66 KB  17.66 KB
        # ...
        #
        # Total TrueDiskSpaceUsed: 34.38 K
        snapshot_names = [line[:line.find(' ')] for line in values_only_lines]
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

        backup_task = mgr_cluster.run_backup_command({"location": ["s3:{}".format(DESTINATION_BUCKET)],
                                                      "keyspace": list(keyspace_table_and_key_range.keys())})
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
        self.clean_restore_and_verify_backup(backup_task, self.cluster.nodelist(), node1, keyspace_table_and_key_range)
