# coding: utf-8

from datetime import datetime
import os

from cassandra import ConsistencyLevel
from nose.plugins.attrib import attr
from minio import Minio

from dtest_scylla_manager import ScyllaManagerTool, ScyllaManagerError
from dtest_scylla_manager import TaskStatus
from scylla_tools import insert_c1c2
from dtest import Tester, debug, warning


class TestScyllaMgmtBackup(Tester):
    # TODO add restore and verify functions
    __test__ = True
    CLUSTER_NAME = 'cluster1'
    DESTINATION_BUCKET = 'backup-bucket'
    FALSE_BUCKET = 'nonexistent_bucket'

    @classmethod
    def setUpClass(cls):
        minio_address = os.getenv("AWS_S3_ENDPOINT").replace("http://", '').strip()
        minio_client = Minio(endpoint=minio_address,
                             access_key=os.getenv("AWS_ACCESS_KEY_ID"),
                             secret_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
                             secure=False,
                             region='us-east-1')
        if not minio_client.bucket_exists(cls.DESTINATION_BUCKET):
            minio_client.make_bucket(bucket_name=cls.DESTINATION_BUCKET, location='us-east-1')

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
                            c1_values=["value%d" % i for i in range(*key_range)],
                            c2_values=["other_value%d" % i for i in range(*key_range)],
                            ks=keyspace, cf=table_name)
        return node_list

    def _create_mgr_cluster(self, node, name):
        manager_tool = ScyllaManagerTool(scylla_manager=self.cluster._scylla_manager)
        mgr_cluster = manager_tool.add_cluster(node=node, name=name)

        return mgr_cluster

    @attr('scylla-manager')
    def test_basic_backup(self):
        node1, node2 = self._prepare_cluster_with_data(keyspace_table_and_key_range={"ks": {"cf1": (1, 21)}})
        mgr_cluster = self._create_mgr_cluster(node=node1, name=self.CLUSTER_NAME)

        debug("Attempting to create a backup task with a location value, expecting it to success")
        backup_task = mgr_cluster.run_backup_command({"location": ["s3:{}".format(self.DESTINATION_BUCKET)]})
        backup_task.wait_for_status(list_status=[TaskStatus.DONE], timeout=1000, step=20)

    @attr('scylla-manager')
    def test_backup_rate_limit_invalid(self):
        node1, node2 = self.config_and_create_cluster(nodes=2)
        mgr_cluster = self._create_mgr_cluster(node=node1, name=self.CLUSTER_NAME)

        debug("Attempting to create a backup task with an invalid rate limit value, expecting it to fail")
        try:
            mgr_cluster.run_backup_command({"location": ["s3:{}".format(self.DESTINATION_BUCKET)],
                                           "rate-limit": ['a']})
        except ScyllaManagerError as err:
            assert "invalid" in err.args[0] and "limit" in err.args[0], "Unexpected error: {}".format(err.args[0])
        else:
            assert False, "No error occurred when an invalid rate-limit is used in the sctool backup command"

    @attr('scylla-manager')
    def test_backup_start_date(self):
        node1, node2 = self._prepare_cluster_with_data(keyspace_table_and_key_range={"ks": {"cf1": (1, 21)}})
        mgr_cluster = self._create_mgr_cluster(node=node1, name=self.CLUSTER_NAME)

        command_execution_time = datetime.now()
        backup_task = mgr_cluster.run_backup_command({"location": ["s3:{}".format(self.DESTINATION_BUCKET)],
                                                     "start-date": "now+40s"})
        next_run_string = backup_task.next_run
        next_run_time = datetime.strptime(next_run_string, "%d %b %y %H:%M:%S %Z")
        assert abs((next_run_time - command_execution_time).seconds) < 50, "The start time is not identical"

        backup_task.wait_for_status(list_status=[TaskStatus.RUNNING], timeout=100, step=2)
        task_start_time = datetime.now()
        assert abs((task_start_time - command_execution_time).seconds) < 50, "In practice, the start time of the " \
                                                                             "backup task did not match the requested" \
                                                                             "time"

    @attr('scylla-manager')
    def test_backup_multiple_keyspaces_and_tables(self):
        node1, node2 = self._prepare_cluster_with_data(
            keyspace_table_and_key_range={"ks1": {"cf1": (1, 21),
                                                  "cf2": (1, 21)},
                                          "ks2": {"cf1": (1, 21),
                                                  "cf2": (1, 21)},
                                          "ks3": {"cf1": (1, 21),
                                                  "cf2": (1, 21)}, })
        mgr_cluster = self._create_mgr_cluster(node=node1, name=self.CLUSTER_NAME)

        debug("Attempting to create a backup task for a cluster with several keyspaces and column families,"
              " expecting it to succeed")
        backup_task = mgr_cluster.run_backup_command({"location": ["s3:{}".format(self.DESTINATION_BUCKET)]})
        backup_task.wait_for_status(list_status=[TaskStatus.DONE], timeout=1000, step=20)

    @attr('scylla-manager')
    def test_backup_a_single_keyspace_and_glob_pattern(self):
        node1, node2 = self._prepare_cluster_with_data(
            keyspace_table_and_key_range={"ks1": {"cf1": (1, 21),
                                                  "cf2": (1, 21)},
                                          "ks2": {"cf1": (1, 21),
                                                  "cf2": (1, 21)},
                                          "ks3": {"cf1": (1, 21),
                                                  "cf2": (1, 21)},
                                          "keyspace_for_glob": {"cf1": (1, 21),
                                                                "cf2": (1, 21)},
                                          "other_keyspace": {"cf1": (1, 21),
                                                             "cf2": (1, 21)}})
        mgr_cluster = self._create_mgr_cluster(node=node1, name=self.CLUSTER_NAME)

        debug("Attempting to create a backup task for a cluster with several keyspaces and column families,"
              " expecting it to succeed")
        backup_task = mgr_cluster.run_backup_command({"location": ["s3:{}".format(self.DESTINATION_BUCKET)],
                                                     "keyspace": ['ks1', "*_for_glob"]})
        backup_task.wait_for_status(list_status=[TaskStatus.DONE], timeout=1000, step=20)

    @attr('scylla-manager')
    def test_backup_nonexistent_bucket(self):
        node1, node2 = self._prepare_cluster_with_data(keyspace_table_and_key_range={"ks": {"cf1": (1, 21)}})

        mgr_cluster = self._create_mgr_cluster(node=node1, name=self.CLUSTER_NAME)

        debug("Attempting to create a backup task with a nonexistent bucket in the location value, expecting it to fail")
        try:
            mgr_cluster.run_backup_command({"location": ["s3:{}".format(self.FALSE_BUCKET)]})
        except ScyllaManagerError as err:
            assert "invalid location" in err.args[0], "Unexpected error: {}".format(err.args[0])
        else:
            raise ScyllaManagerError("No error occurred when a nonexistent bucket was used as a location"
                                     " in a manager backup command")

    @attr('scylla-cluster')
    def _backup_nonexistent_keyspace_template(self, keyspace_filter_string):
        node1, node2 = self._prepare_cluster_with_data(keyspace_table_and_key_range={"ks": {"cf1": (1, 21)}})

        mgr_cluster = self._create_mgr_cluster(node=node1, name=self.CLUSTER_NAME)

        debug("Attempting to create a backup task with a nonexistent keyspace in the keyspace value,"
              " expecting it to fail")
        try:
            mgr_cluster.run_backup_command({"location": ["s3:{}".format(self.DESTINATION_BUCKET)],
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

        mgr_cluster = self._create_mgr_cluster(node=node1, name=self.CLUSTER_NAME)

        debug("Attempting to create a backup task with a nonexistent keyspace in the keyspace value,"
              " expecting it to fail")
        try:
            mgr_cluster.run_backup_command({"location": ["s3:{}".format(self.DESTINATION_BUCKET)],
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
        node1, node2 = self._prepare_cluster_with_data(keyspace_table_and_key_range={"ks": {"cf1": (1, 21)}})

        mgr_cluster = self._create_mgr_cluster(node=node1, name=self.CLUSTER_NAME)

        backup_task = mgr_cluster.run_backup_command({"location": ["s3:{}".format(self.DESTINATION_BUCKET)]})

        backup_task.wait_for_status(list_status=[TaskStatus.RUNNING], timeout=100, step=3)
        progress_percentage = backup_task.progress
        assert progress_percentage != "N/A", "couldn't read the progress of the backup test"

        backup_task.wait_for_status(list_status=[TaskStatus.DONE], timeout=600, step=10)
        progress_percentage = backup_task.progress
        assert progress_percentage == '100%', "The percentage of the backup task at its end was not 100%"

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
        node1, node2 = self._prepare_cluster_with_data(keyspace_table_and_key_range={"ks": {"cf1": (1, 21)}})
        node_list = node1, node2

        mgr_cluster = self._create_mgr_cluster(node=node1, name=self.CLUSTER_NAME)

        manual_snapshot_name = "sm_manual_snapshot"
        self.cluster.nodetool("snapshot -t {}".format(manual_snapshot_name))
        per_node_pre_backup_snapshot_lists = {node.name: sorted(self.extract_all_snapshot_names(
            node.nodetool("listsnapshots", capture_output=True)[0])) for node in node_list}

        backup_task = mgr_cluster.run_backup_command({"location": ["s3:{}".format(self.DESTINATION_BUCKET)]})
        backup_task.wait_for_status(list_status=[TaskStatus.DONE], timeout=600, step=10)
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
