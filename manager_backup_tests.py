# coding: utf-8

from datetime import datetime
import os

from cassandra import ConsistencyLevel
from nose.plugins.attrib import attr
from minio import Minio

from dtest_scylla_manager import ScyllaManagerTool, ScyllaManagerError
from dtest_scylla_manager import TaskStatus
from scylla_tools import insert_c1c2
from dtest import Tester, debug


class TestScyllaMgmtBackup(Tester):
    # TODO add restore and verify functions
    __test__ = True
    KEYSPACE_NAME = 'ks'
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

    @attr('scylla-manager')
    def test_basic_backup(self):
        self.config_and_create_cluster(nodes=2)
        node1, node2 = self.cluster.nodelist()

        manager_tool = ScyllaManagerTool(scylla_manager=self.cluster._scylla_manager)
        cluster_name = "cluster1"
        debug("Add a cluster to scylla-manager, named: {}".format(cluster_name))
        mgr_cluster = manager_tool.add_cluster(node=node1, name=cluster_name)

        session = self.patient_cql_connection(node1)
        self.create_ks(session=session, name='ks', rf=2)
        self.create_cf(session=session, name='cf1', read_repair=0.0, columns={'c1': 'text', 'c2': 'text'},
                       dclocal_read_repair_chance=0.0, speculative_retry='NONE')

        insert_c1c2(session=session, keys=range(1, 21), consistency=ConsistencyLevel.ALL,
                    c1_values=["value%d" % i for i in range(1, 21)],
                    c2_values=["other_value%d" % i for i in range(1, 21)], ks='ks', cf="cf1")

        debug("Attempting to create a backup task with a location value, expecting it to success")
        backup_task = mgr_cluster.run_backup_command({"location": ["s3:{}".format(self.DESTINATION_BUCKET)]})
        backup_task.wait_for_status(list_status=[TaskStatus.DONE], timeout=1000, step=20)

    @attr('scylla-manager')
    def test_backup_rate_limit_invalid(self):
        self.config_and_create_cluster(nodes=2)
        node1, node2 = self.cluster.nodelist()

        manager_tool = ScyllaManagerTool(scylla_manager=self.cluster._scylla_manager)
        cluster_name = "cluster1"
        debug("Add a cluster to scylla-manager, named: {}".format(cluster_name))
        mgr_cluster = manager_tool.add_cluster(node=node1, name=cluster_name)

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
        self.config_and_create_cluster(nodes=2)
        node1, node2 = self.cluster.nodelist()

        manager_tool = ScyllaManagerTool(scylla_manager=self.cluster._scylla_manager)
        cluster_name = "cluster1"
        debug("Add a cluster to scylla-manager, named: {}".format(cluster_name))
        mgr_cluster = manager_tool.add_cluster(node=node1, name=cluster_name)

        session = self.patient_cql_connection(node1)
        self.create_ks(session=session, name='ks', rf=2)
        self.create_cf(session=session, name='cf1', read_repair=0.0, columns={'c1': 'text', 'c2': 'text'},
                       dclocal_read_repair_chance=0.0, speculative_retry='NONE')

        insert_c1c2(session=session, keys=range(1, 21), consistency=ConsistencyLevel.ALL,
                    c1_values=["value%d" % i for i in range(1, 21)],
                    c2_values=["other_value%d" % i for i in range(1, 21)], ks='ks', cf="cf1")

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
