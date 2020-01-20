import os
import random
import shutil
import tempfile
import time
import uuid
from functools import wraps
from ccmlib.node import NodeError
from tools import new_node
from cassandra import ConsistencyLevel
from dtest import Tester, debug, run_with_params
from scylla_tools import insert_c1c2, query_c1c2_concurrent, get_cf_dir


class expected_failure(object):
    """decorator that verifies expected failure in system.log and exception/s that should happen"""
    def __init__(self, err_log_msg, exception):
        self.err_log_msg = err_log_msg
        self.exception = exception

    def __call__(self, func):
        @wraps(func)
        def inner(*args, **kwargs):
            tester_obj = args[0]
            tester_obj.allow_log_errors = True
            try:
                func(*args, **kwargs)
            except self.exception:
                log_errors = tester_obj.cluster.nodelist()[0].grep_log_for_errors(search_str=self.err_log_msg)
                assert len(log_errors) > 0, "In memory errors not found: %s" % "\n".join(log_errors)
            else:
                tester_obj.fail("'nodetool flush' shouldn't succeed when there is "
                                "not enough in-memory storage available")
        return inner


class InMemoryTest(Tester):

    def setUp(self):
        self.in_memory_compaction_strategy = "%s" % {"class": "InMemoryCompactionStrategy"}
        self.table_name = "cf"
        self.columns_definitions = {'c1': 'text', 'c2': 'text'}
        self.in_memory_amount = 8  # Mb
        self.in_memory_amount_kb = 8 * 2**20  # Mb
        # this value was empiricaly determined and is based on the assumpion
        # that the ammount of memory used is roughly linearly dependant in the
        # number of keys.
        self.memory_usage_per_key_factor = 30
        super(InMemoryTest, self).setUp()

    def in_memory_scylla_start_args(self, smp=1):
        return ["--in-memory-storage-size-mb", str(self.in_memory_amount),
                "--memory", "{}M".format(smp * 512),
                # "--default-log-level", "debug",
                "--smp", str(smp)]

    def get_metric_value(self, node, metric):
        res = self.get_node_metrics(node_ip=self.get_ip_from_node(node),
                                    metrics=[metric])
        val = res.get(metric)
        if val:
            return val
        else:
            debug("WARN: no results from '%s' metric. Probably the metric is dynamic, assuming 0." % metric)
            return 0

    def get_io_queue_total_bytes(self, node):
        return self.get_metric_value(node, "scylla_io_queue_query_total_bytes")

    def get_in_memory_stats(self, node, write_to_log=True):
        total_mem = int(self.get_metric_value(node, "in_memory_store_total_memory"))
        used_mem = int(self.get_metric_value(node, "in_memory_store_used_memory"))
        if write_to_log:
            debug("In-memory store on '%s': used - %s Kb, total - %s Kb" % (node.name, used_mem, total_mem))
        return used_mem, total_mem

    def create_cluster_nodes(self, num_nodes, scylla_startup_args):
        cluster = self.cluster
        if not cluster.nodelist():
            debug("Starting a cluster of %s node/s..." % num_nodes)
            cluster.populate(num_nodes).start(wait_for_binary_proto=True, wait_other_notice=True,
                                              jvm_args=scylla_startup_args)
        return cluster.nodelist()

    def prepare_cluster_and_keyspace(self, keyspace_name, num_nodes=1, key_space_rf=1, scylla_startup_args=()):
        nodes = self.create_cluster_nodes(num_nodes=num_nodes, scylla_startup_args=scylla_startup_args)
        node1 = nodes[0]
        session = self.patient_exclusive_cql_connection(node1)
        debug("Creating keyspace '%s'..." % keyspace_name)
        self.create_ks(session, name=keyspace_name, rf=key_space_rf)
        return node1, session

    @staticmethod
    def restart_node(node, scylla_startup_args=()):
        debug("Stopping node %s..." % node.name)
        node.stop()
        debug("Starting node '%s' with %s startup arguments" % (node.name, scylla_startup_args))
        node.start(wait_for_binary_proto=True, wait_other_notice=True, jvm_args=scylla_startup_args)

    def stop_all_nodes_except(self, node):
        debug("Stopping all nodes, except: %s" % node.name)
        for node_to_stop in self.cluster.nodelist():
            if node_to_stop.name != node.name:
                node_to_stop.stop()
                debug("%s stopped." % node_to_stop.name)

    def populate_and_read(self, restart_node, num_threads_after_restart=2, num_keys=1000):
        debug("====== Starting a test with %s ======" % ["%s: %s" % (k, v) for k, v in locals().items() if k != "self"])
        key_space_name = 'ks_%s' % (str(uuid.uuid1()).replace("-", "_"))
        node1, session = self.prepare_cluster_and_keyspace(keyspace_name=key_space_name,
                                                           scylla_startup_args=self.in_memory_scylla_start_args(smp=2))
        debug("Creating in memory table")
        self.get_in_memory_stats(node1)
        self.create_cf(session, 'cf', columns=self.columns_definitions,
                       compaction=self.in_memory_compaction_strategy, in_memory=True)
        debug("Inserting some data")
        insert_c1c2(session, n=num_keys, ks=key_space_name)
        debug("Flushing to disk")
        node1.nodetool(cmd="flush", capture_output=False, wait=False)
        node1._wait_no_pending_flushes(wait_timeout=30)
        self.get_in_memory_stats(node1)
        if restart_node:
            self.restart_node(node=node1,
                              scylla_startup_args=self.in_memory_scylla_start_args(smp=num_threads_after_restart))
            session = self.patient_cql_connection(node1, keyspace=key_space_name)
        self.validate_in_memory_data(node1, session, num_keys, validate_in_memory_used=True)

    def validate_in_memory_data(self, node, session, num_keys, consistency_level=ConsistencyLevel.QUORUM,
                                validate_in_memory_used=False, validate_func=None):
        time.sleep(2)  # Waiting for internal IO queue to calm down
        io_queue_size_before_read = self.get_io_queue_total_bytes(node)
        used_mem, total_mem = self.get_in_memory_stats(node)
        if validate_func:
            validate_func()
        else:
            debug("Checking all the data (%s keys) exists and valid..." % num_keys)
            query_c1c2_concurrent(session, consistency=consistency_level, keys=range(num_keys))
            debug("Data validated.")
        self.get_in_memory_stats(node)
        debug("Checking that data was read from in-memory store...")
        io_queue_size_after_read = self.get_io_queue_total_bytes(node)
        debug("Comparing IO queue - [before check: %s, after check: %s]" % (io_queue_size_before_read,
                                                                            io_queue_size_after_read))
        self.assertAlmostEqual(io_queue_size_before_read, io_queue_size_after_read, delta=50000,  # 50kb
                               msg="Data was not read from RAM but from disk!!!")
        debug("Data was read from in-memory store.")
        if validate_in_memory_used:
            debug("Validating used memory amounts...")
            expected_memory_usage = num_keys * self.memory_usage_per_key_factor
            self.assertGreaterEqual(used_mem, expected_memory_usage, msg="Used memory ({}) is less than expected ({})".format(used_mem, expected_memory_usage))
            debug("Validating total memory...")
            delta = 2  # when smp is odd we get 2 bytes less
            self.assertAlmostEqual(total_mem, self.in_memory_amount_kb, delta=delta,
                                    msg="Total memory is incorrect, expected {}+/-{}  but got {}"
                                   .format( self.in_memory_amount_kb, delta, total_mem))
            debug("Used memory and total memory amounts are correct.")

    @run_with_params(restart_node=[True, False])
    def populate_and_read_test(self, restart_node):
        """
            Test basic functionality:
            1. Create in-memory table
            2. Populate with some data
            3. Flush to disk
                4. Bring down the node
                5. Bring up the node
            6. Read the data and check that
                6.1 data is consistent
                6.2 was read from RAM and not from disk
        """
        self.populate_and_read(restart_node)

    @run_with_params(num_threads_after_restart=[1, 3])
    def populate_and_reshard_test(self, num_threads_after_restart):
        """
            1. Create in-memory table
            2. Populate with data
            3. Stop node
            4. Start with less/more shards
            4. Read and verify data

        """
        self.populate_and_read(restart_node=True, num_threads_after_restart=num_threads_after_restart)

    def upload_sstables_test(self):
        """
            1. Create keyspace and table with data range 1001 - 1984
            2. Copy files aside
            3. Delete keyspace
            4. Create keyspace and in memory table with range 1 - 1000
            5. Copy sstable files to upload dir
            6. Run nodetool refresh
            7. Read and verify data (range 1984)
            8. Restart the node
            9. Read and verify the data. Make sure it was read from RAM (range 1984)
        """
        key_space_name = "ks"
        num_keys = 1984
        node1, session = self.prepare_cluster_and_keyspace(keyspace_name=key_space_name)
        debug("Creating table...")
        self.create_cf(session, self.table_name, columns=self.columns_definitions)
        keys_range = (1001, num_keys)
        debug("Inserting data: keys range %s" % str(keys_range))
        insert_c1c2(session, keys=range(*keys_range), ks=key_space_name)
        debug("Flushing to disk")
        node1.flush()
        debug("Stopping node")
        node1.stop()
        keyspace_dir = os.path.join(node1.get_path(), 'data', key_space_name)
        cf_dir = get_cf_dir(keyspace_dir, self.table_name)
        temp_dir = os.path.join(tempfile.mkdtemp(), key_space_name)
        debug("Copying sstable files from '%s' to temporary dir '%s'" % (cf_dir, temp_dir))
        shutil.copytree(cf_dir, temp_dir)
        shutil.rmtree(os.path.join(temp_dir, "upload"))  # remove upload dir from temporary sstable files
        debug("Starting node")
        node1.start(wait_for_binary_proto=True, wait_other_notice=True,
                    jvm_args=self.in_memory_scylla_start_args())
        session = self.patient_cql_connection(node1, keyspace=key_space_name)
        debug("Dropping table %s" % self.table_name)
        session.execute("drop table %s" % self.table_name)
        debug("Flushing to disk")
        node1.flush()
        debug("Removing %s" % cf_dir)
        shutil.rmtree(cf_dir)
        debug("Creating new in-memory table '%s'" % self.table_name)
        self.create_cf(session, 'cf', columns=self.columns_definitions,
                       compaction=self.in_memory_compaction_strategy, in_memory=True)
        debug("Inserting data: keys 1 - %s" % (keys_range[0] - 1))
        insert_c1c2(session, n=keys_range[0], ks=key_space_name)
        keyspace_dir = os.path.join(node1.get_path(), 'data', key_space_name)
        cf_dir = get_cf_dir(keyspace_dir, self.table_name)
        upload_dir = os.path.join(cf_dir, "upload")
        shutil.rmtree(upload_dir)  # clean upload dir
        debug("Copying sstable files from '%s' to upload dir '%s'" % (temp_dir, upload_dir))
        shutil.copytree(temp_dir, upload_dir)
        debug("Uploading sstable files using 'nodetool refresh'...")
        node1.nodetool("refresh -- %s %s" % (key_space_name, self.table_name))
        self.validate_in_memory_data(node1, session, num_keys=num_keys, validate_in_memory_used=True)

    @expected_failure(err_log_msg="In-Memory disk is out of space", exception=(NodeError,))
    def populate_with_more_data_than_mem_available_test(self):
        """
            1. Create in-memory table
            2. Populate with data more than available memory
            3. Scylla should stop with IO error
        """
        num_keys = int((self.in_memory_amount_kb * 0.57) / self.memory_usage_per_key_factor)
        self.populate_and_read(restart_node=True, num_keys = num_keys)

    def alter_table_to_in_memory(self, session, key_space_name, table_name):
        debug("Altering table to in-memory...")
        session.execute("ALTER table {key_space_name}.{table_name} WITH in_memory=true".format(**locals()))
        compaction_strategy = self.in_memory_compaction_strategy
        session.execute(
            "ALTER table {key_space_name}.{table_name} WITH compaction={compaction_strategy}".format(**locals()))

    def alter_table_to_in_memory_test(self, num_additional_keys=1986):
        """
            1. Create regular table
            2. Populate with some data
            3. Alter table to be in-memory
            4. Flush
            5. Restart node
            6. Validate data and make sure it is read from RAM
        """
        key_space_name = "ks"
        table_name = self.table_name
        num_keys = 1984
        node1, session = self.prepare_cluster_and_keyspace(keyspace_name=key_space_name,
                                                           scylla_startup_args=self.in_memory_scylla_start_args())
        debug("Creating table...")
        self.create_cf(session, table_name, columns=self.columns_definitions)
        debug("Inserting data...")
        insert_c1c2(session, n=num_keys, ks=key_space_name)
        debug("Flushing to disk")
        node1.flush()
        self.alter_table_to_in_memory(session=session, key_space_name=key_space_name, table_name=table_name)
        debug("Inserting additional data")
        insert_c1c2(session, keys=list(range(num_keys, num_keys + num_additional_keys)), ks=key_space_name)
        debug("Flushing to disk")
        node1.nodetool(cmd="flush", capture_output=False, wait=False)
        node1._wait_no_pending_flushes(wait_timeout=30)
        self.restart_node(node=node1, scylla_startup_args=self.in_memory_scylla_start_args())
        session = self.patient_cql_connection(node1, keyspace=key_space_name)
        self.validate_in_memory_data(node1, session, num_keys=num_keys + num_additional_keys)

    @expected_failure(err_log_msg="In-Memory disk is out of space", exception=(NodeError,))
    def alter_table_to_in_memory_more_data_then_available_test(self):
        num_keys = int((self.in_memory_amount_kb * 0.57) // self.memory_usage_per_key_factor)
        self.alter_table_to_in_memory_test(num_additional_keys=num_keys)

    def streaming_test(self):
        """
            1. Create a cluster with 3 nodes, all started in-memory
            2. Create keyspace with rf=3
            3. Populate with data
            4. Flush
            5. Validate
            6. Restart
            5. Validate
            7. Add one more node
            8. Validate

        """
        key_space_name = "ks"
        num_keys = 1986
        node1, session = self.prepare_cluster_and_keyspace(keyspace_name=key_space_name,
                                                           num_nodes=3, key_space_rf=3,
                                                           scylla_startup_args=self.in_memory_scylla_start_args())
        debug("Creating table...")
        self.create_cf(session, self.table_name, columns=self.columns_definitions,
                       compaction=self.in_memory_compaction_strategy, in_memory=True)
        debug("Inserting data: keys %s" % num_keys)
        insert_c1c2(session, keys=range(num_keys), ks=key_space_name)
        for node in self.cluster.nodelist():
            debug("Validating via '%s'" % node.name)
            self.validate_in_memory_data(node, session, num_keys=num_keys, consistency_level=ConsistencyLevel.ALL)
            debug("Flushing to disk %s" % node.name)
            node.flush()
            self.restart_node(node=node, scylla_startup_args=self.in_memory_scylla_start_args())
            session = self.patient_exclusive_cql_connection(node, keyspace=key_space_name)
            self.validate_in_memory_data(node, session, num_keys=num_keys, consistency_level=ConsistencyLevel.ALL,
                                         validate_in_memory_used=True)

        debug("Adding another node...")
        node4 = new_node(self.cluster)
        node4.start(wait_for_binary_proto=True, wait_other_notice=True,
                    jvm_args=self.in_memory_scylla_start_args())
        session = self.patient_exclusive_cql_connection(node4, keyspace=key_space_name)
        self.validate_in_memory_data(node4, session, num_keys=num_keys, consistency_level=ConsistencyLevel.ALL)
        debug("Flushing to disk on node4")
        node4.flush()
        self.restart_node(node=node4, scylla_startup_args=self.in_memory_scylla_start_args())
        session = self.patient_exclusive_cql_connection(node4, keyspace=key_space_name)
        self.validate_in_memory_data(node4, session, num_keys=num_keys, consistency_level=ConsistencyLevel.ALL,
                                     validate_in_memory_used=True)

    def stopped_node_test(self):
        """
            Validate data that on node that was stopped during in-memory table creation, then repaired and
            check that it is served from the memory (inspired by JuliaY)
            - Create cluster with 3 nodes and in-memory enabled
            - Create keyspace with RF=3
            - Stop one of the nodes
            - Create in-memory table and fill with data
            - Start the stopped node
            - Repair stopped node
            - Validate data on the started node (with CL=One from this node)
            - Make sure that it is served from in-memory
        """
        key_space_name = "ks"
        num_keys = 2005
        node1, _ = self.prepare_cluster_and_keyspace(keyspace_name=key_space_name,
                                                     num_nodes=3, key_space_rf=3,
                                                     scylla_startup_args=self.in_memory_scylla_start_args())
        debug("Stopping node1")
        node1.stop()
        random_running_node = random.choice([node for node in self.cluster.nodelist() if node.is_live()])
        session = self.patient_exclusive_cql_connection(random_running_node, keyspace=key_space_name)
        debug("Creating table via %s..." % random_running_node.name)
        self.create_cf(session, self.table_name, columns=self.columns_definitions,
                       compaction=self.in_memory_compaction_strategy, in_memory=True)
        debug("Inserting data: keys %s" % num_keys)
        insert_c1c2(session, keys=range(num_keys), ks=key_space_name)
        debug("Starting node1")
        node1.start(wait_for_binary_proto=True, wait_other_notice=True,
                    jvm_args=self.in_memory_scylla_start_args())
        debug("Running repair on node1")
        node1.repair()
        debug("Repair completed.")
        self.stop_all_nodes_except(node1)
        self.restart_node(node=node1, scylla_startup_args=self.in_memory_scylla_start_args())
        session = self.patient_exclusive_cql_connection(node1, keyspace=key_space_name)
        self.validate_in_memory_data(node1, session, num_keys=num_keys, consistency_level=ConsistencyLevel.ONE,
                                     validate_in_memory_used=True)

    def simple_stress_check_test(self):
        num_keys = 2017
        key_space_name = "keyspace1"
        nodes = self.create_cluster_nodes(num_nodes=3, scylla_startup_args=self.in_memory_scylla_start_args())
        node1 = nodes[0]
        debug("Running stress on node1")
        node1.stress(stress_options=['write', 'n=%s' % num_keys, 'cl=ALL', '-schema', 'replication(factor=3)'])
        session = self.patient_exclusive_cql_connection(node1, keyspace=key_space_name)
        self.alter_table_to_in_memory(session=session, key_space_name=key_space_name, table_name="standard1")
        self.restart_node(node=node1, scylla_startup_args=self.in_memory_scylla_start_args())
        session = self.patient_exclusive_cql_connection(node1, keyspace=key_space_name)

        def stress_read():
            node1.stress(stress_options=['read', 'n=%s' % num_keys, 'cl=ONE', '-schema', 'replication(factor=3)'])
        self.stop_all_nodes_except(node1)
        self.validate_in_memory_data(node1, session, num_keys=num_keys, consistency_level=ConsistencyLevel.ONE,
                                     validate_func=stress_read)
