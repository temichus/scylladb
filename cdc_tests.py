import bisect
import os
import time
import itertools

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from enum import IntEnum
from threading import Event

from cassandra import ConsistencyLevel, InvalidRequest
from cassandra.connection import ConnectionException
from cassandra.metadata import Murmur3Token
from cassandra.query import SimpleStatement
from cassandra.util import datetime_from_uuid1
from cassandra.policies import FallthroughRetryPolicy
from ccmlib.scylla_cluster import ScyllaCluster
from dtest import Tester, debug, wait_for
from nose.plugins.attrib import attr
from tools import new_node


TOKENS_PER_NODE = 256

CDC_GENERATIONS_TABLE = 'system_distributed.cdc_generation_descriptions'
CDC_STREAMS_TABLE = 'system_distributed.cdc_streams_descriptions'


class CdcLogOperations(IntEnum):
    PREIMAGE = 0
    UPDATE = 1
    INSERT = 2
    ROW_DELETE = 3
    PARTITION_DELETE = 4
    RANGE_DELETE_START_INCLUSIVE = 5
    RANGE_DELETE_START_EXCLUSIVE = 6
    RANGE_DELETE_END_INCLUSIVE = 7
    RANGE_DELETE_END_EXCLUSIVE = 8
    POSTIMAGE = 9


class CDCInitializeHelper:

    def populate_sequentially(self, n):
        cluster = self.cluster
        debug('Starting node 1')
        # We need to use populate() for the first node, because it writes
        # a configuration file that specifies the first node as a seed.
        # Unless we do that, the first node will try to communicate
        # with 127.0.0.1 - which is configured to be the default seed - and
        # might fail, because in some environments the first node might listen
        # for gossip on a different address.
        cluster.populate(1).start(wait_for_binary_proto=True)
        for i in range(2, n + 1):
            debug('Starting node {}'.format(i))
            node = new_node(self.cluster, bootstrap=True)
            node.start(wait_for_binary_proto=True)

    def wait_for_last_generation_to_be_active(self, session):
        cdc_descriptions = list(self.get_cdc_description_rows(session))
        self.assertGreater(len(cdc_descriptions), 0, "No CDC generations")
        last_timestamp = max(desc.time for desc in cdc_descriptions)

        # Add one second to account for clock differences
        self.sleep_until(last_timestamp + timedelta(seconds=1))
        debug('Current generation timestamp: {}'.format(last_timestamp))
        return last_timestamp

    def get_cdc_description_rows(self, session):
        query = SimpleStatement(f"SELECT * FROM {CDC_STREAMS_TABLE}",
                                consistency_level=ConsistencyLevel.ONE)
        return session.execute(query)

    def wait_for_metadata_update(self, session, cluster_size):
        # Cluster metadata is updated asynchronously, so we need to wait
        def check_metadata():
            ring = self.get_vnode_ring(session)
            debug('Token ring length: {}'.format(len(ring)))
            return len(ring) == cluster_size * 256
        wait_for(check_metadata, timeout=60, text='Waiting until metadata is updated')

    def sleep_until(self, timestamp):
        secs = (timestamp - datetime.utcnow()).total_seconds()
        if secs > 0:
            debug('Sleeping for {} seconds'.format(secs))
            time.sleep(secs)

    def get_vnode_ring(self, session):
        return list(session.cluster.metadata.token_map.ring)


@attr('scylla-cdc')
class TestCdc(Tester, CDCInitializeHelper):
    def __init__(self, *args, **kwargs):
        ring_delay_sec = 5
        kwargs['cluster_options'] = {'experimental_features': ['cdc'],
                                     'ring_delay_ms': ring_delay_sec * 1000,
                                     'num_tokens': TOKENS_PER_NODE,
                                     'hinted_handoff_enabled': False}
        Tester.__init__(self, *args, **kwargs)
        self.ring_delay_sec = ring_delay_sec

    def setUp(self):
        Tester.setUp(self)
        assert type(self.cluster) is ScyllaCluster, \
            "CDC tests are intended for Scylla only"

    def simple_cdc_template(self, with_preimage):
        debug('Setup a cluster')
        cluster = self.cluster
        self.populate_sequentially(n=3)
        node1 = cluster.nodes['node1']
        session = self.patient_cql_connection(node1)

        debug('Wait for the last generation to become active')
        gen_timestamp = self.wait_for_last_generation_to_be_active(session)
        self.wait_for_metadata_update(session, cluster_size=3)
        ring = self.get_vnode_ring(session)

        self.generation_quality_check(session, gen_timestamp, ring)

        debug('Create a table with CDC enabled, and start writing to it')
        finish_writing = self.run_writes_with_counting(node1, with_preimage=with_preimage)

        debug('Write for 15 more seconds, and stop writing')
        time.sleep(15)
        write_count = finish_writing()

        base_rows, log_rows = self.get_base_and_log_rows(session, "ks.cf")
        update_rows = self.get_sorted_update_rows(session, log_rows)

        debug('Check invariants on written data')
        self.check_common_invariants(session, base_rows, log_rows, update_rows, write_count, with_preimage)
        self.check_that_log_entries_and_their_streams_are_in_the_same_vnode(session, base_rows, update_rows, ring)

        debug('Test finished')

    def simple_cdc_test(self):
        self.simple_cdc_template(with_preimage=False)

    @attr('next-gating')
    def simple_cdc_with_preimage_test(self):
        self.simple_cdc_template(with_preimage=True)

    def cluster_expansion_with_cdc_template(self, with_preimage):
        debug('Setup a cluster')
        cluster = self.cluster
        self.populate_sequentially(n=3)
        node1 = cluster.nodes['node1']
        session = self.patient_cql_connection(node1)

        debug('Wait for the last generation to become active')
        gen_timestamp = self.wait_for_last_generation_to_be_active(session)
        self.wait_for_metadata_update(session, cluster_size=3)
        ring_before_expansion = self.get_vnode_ring(session)

        self.generation_quality_check(session, gen_timestamp, ring_before_expansion)

        debug('Create a table with CDC enabled, and start writing to it')
        finish_writing = self.run_writes_with_counting(node1, with_preimage=with_preimage)

        debug('Add new node to the cluster')
        expansion_start_time = datetime.utcnow()
        node4 = new_node(self.cluster, bootstrap=True)
        node4.start(wait_for_binary_proto=True)

        debug('Wait until new generation starts')
        gen_timestamp = self.wait_for_last_generation_to_be_active(session)

        debug('Write for 15 more seconds, and stop writing')
        time.sleep(15)
        write_count = finish_writing()

        self.wait_for_metadata_update(session, cluster_size=4)
        ring_after_expansion = self.get_vnode_ring(session)
        self.assertNotEqual(ring_before_expansion, ring_after_expansion)

        self.generation_quality_check(session, gen_timestamp, ring_after_expansion)

        base_rows, log_rows = self.get_base_and_log_rows(session, "ks.cf")
        update_rows = self.get_sorted_update_rows(session, log_rows)

        debug('Check invariants on written data')
        self.check_common_invariants(session, base_rows, log_rows, update_rows, write_count, with_preimage)

        # Check before expansion
        self.check_that_log_entries_and_their_streams_are_in_the_same_vnode(session, base_rows, update_rows,
                                                                            ring=ring_before_expansion,
                                                                            time_range_end=expansion_start_time)

        # Check after expansion
        after_expansion_timestamp = self.get_timestamp_of_first_generation_after(
            session, expansion_start_time)
        self.check_that_log_entries_and_their_streams_are_in_the_same_vnode(session, base_rows, update_rows,
                                                                            ring=ring_after_expansion,
                                                                            time_range_begin=after_expansion_timestamp)

        debug('Test finished')

    @attr('next-gating')
    def cluster_expansion_with_cdc_test(self):
        self.cluster_expansion_with_cdc_template(with_preimage=False)

    def cluster_expansion_with_cdc_and_preimage_test(self):
        self.cluster_expansion_with_cdc_template(with_preimage=True)

    def cluster_reduction_with_cdc_template(self, with_preimage):
        debug('Setup a cluster')
        cluster = self.cluster
        self.populate_sequentially(n=4)
        node1 = cluster.nodes['node1']
        node3 = cluster.nodes['node3']
        session = self.patient_cql_connection(node1)

        debug('Wait for the last generation to become active')
        gen_timestamp = self.wait_for_last_generation_to_be_active(session)
        self.wait_for_metadata_update(session, cluster_size=4)
        ring = self.get_vnode_ring(session)

        self.generation_quality_check(session, gen_timestamp, ring)

        debug('Create a table with CDC enabled, and start writing to it')
        finish_writing = self.run_writes_with_counting(node1, with_preimage=with_preimage)
        time.sleep(15)

        debug('Downsize the cluster by one node')
        reduction_start_time = datetime.utcnow()
        node3.decommission()
        reduction_end_time = datetime.utcnow()

        debug('Write for 15 more seconds, and stop writing')
        time.sleep(15)
        write_count = finish_writing()

        base_rows, log_rows = self.get_base_and_log_rows(session, "ks.cf")
        update_rows = self.get_sorted_update_rows(session, log_rows)

        debug('Check invariants on written data')
        self.check_common_invariants(session, base_rows, log_rows, update_rows, write_count, with_preimage)
        self.check_that_log_entries_and_their_streams_are_in_the_same_vnode(session, base_rows, update_rows, ring)

        debug('Test finished')

    @attr('next-gating')
    def cluster_reduction_with_cdc_test(self):
        self.cluster_reduction_with_cdc_template(with_preimage=False)

    def cluster_reduction_with_cdc_and_preimage_test(self):
        self.cluster_reduction_with_cdc_template(with_preimage=True)

    def schema_change_template(self, alter_query, with_preimage=False, additional_fields=[]):
        debug('Setup a cluster')
        cluster = self.cluster
        self.populate_sequentially(n=3)
        node1 = cluster.nodes['node1']
        session = self.patient_cql_connection(node1)

        debug('Wait for the last generation to become active')
        gen_timestamp = self.wait_for_last_generation_to_be_active(session)
        self.wait_for_metadata_update(session, cluster_size=3)
        ring = self.get_vnode_ring(session)

        self.generation_quality_check(session, gen_timestamp, ring)

        debug('Create a table with CDC enabled, and start writing to it')
        finish_writing = self.run_writes_with_counting(node1,
                                                       with_preimage=with_preimage,
                                                       additional_fields=additional_fields)
        time.sleep(15)

        debug('Alter schema: {}'.format(alter_query))
        session.execute(alter_query)

        debug('Write for 15 more seconds, and stop writing')
        time.sleep(15)
        write_count = finish_writing()

        base_rows, log_rows = self.get_base_and_log_rows(session, "ks.cf")
        update_rows = self.get_sorted_update_rows(session, log_rows)

        debug('Check invariants on written data')
        self.check_common_invariants(session, base_rows, log_rows, update_rows, write_count, with_preimage)
        self.check_that_log_entries_and_their_streams_are_in_the_same_vnode(session, base_rows, update_rows, ring)

        debug('Test finished')

    @attr('next-gating')
    def change_field_type_with_cdc_test(self):
        self.schema_change_template("ALTER TABLE ks.cf ALTER b TYPE blob")

    def change_field_type_with_cdc_and_preimage_test(self):
        self.schema_change_template("ALTER TABLE ks.cf ALTER b TYPE blob", with_preimage=True)

    @attr('next-gating')
    def add_field_with_cdc_test(self):
        self.schema_change_template("ALTER TABLE ks.cf ADD c int")

    def add_field_with_cdc_and_preimage_test(self):
        self.schema_change_template("ALTER TABLE ks.cf ADD c int", with_preimage=True)

    @attr('next-gating')
    def remove_field_with_cdc_test(self):
        self.schema_change_template("ALTER TABLE ks.cf DROP c", additional_fields=["c int"])

    def remove_field_with_cdc_and_preimage_test(self):
        self.schema_change_template("ALTER TABLE ks.cf DROP c", additional_fields=["c int"], with_preimage=True)

    def run_writes_with_counting(self, node, with_preimage=False, additional_fields=[]):
        cdc_options = "'enabled': true"
        if with_preimage:
            cdc_options += ", 'preimage': true"
            workers_count = 1
        else:
            workers_count = 10

        session = self.patient_cql_connection(node)
        session.execute("CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}")
        fields = ["a int", "b text"] + additional_fields
        session.execute("CREATE TABLE ks.cf ({fields}, PRIMARY KEY(a)) WITH cdc = {{{cdc_options}}}".format(
            fields=", ".join(fields), cdc_options=cdc_options))

        worker_executor = ThreadPoolExecutor(max_workers=workers_count)
        stop_event = Event()
        self.addCleanup(lambda: stop_event.set())
        start_time = time.time()

        def run_writes(worker_id):
            confirmed_writes = 0
            unconfirmed_writes = 0
            i = 0
            stmt = session.prepare("INSERT INTO ks.cf (a, b) VALUES (?, ?)")
            stmt.consistency_level = ConsistencyLevel.QUORUM
            # We don't want the driver to retry writes, because this would cause us to count writes incorrectly.
            stmt.retry_policy = FallthroughRetryPolicy()
            while not stop_event.is_set():
                try:
                    session.execute(stmt, (i, str(worker_id)))
                    confirmed_writes += 1
                except ConnectionException as e:
                    debug('Got ConnectionException, probably because the cluster is being downsized, retrying; ' +
                            'exception was {}'.format(e))
                    # We cannot determine if the write was successful.
                    unconfirmed_writes += 1
                except InvalidRequest as e:
                    if "cdc: attempted to get a stream from an earlier generation than the currently used" in str(e):
                        debug('Attempted to write with a timestamp older than the current generation. ' +
                              'This is a non critical error, continuing; exception was: {}'.format(e))
                    else:
                        raise e
                i += 1
            return confirmed_writes, unconfirmed_writes

        futs = [worker_executor.submit(run_writes, i) for i in range(workers_count)]

        def finisher():
            stop_event.set()
            total_confirmed = 0
            total_unconfirmed = 0
            for i, fut in enumerate(futs):
                (confirmed, unconfirmed) = fut.result()
                debug("Worker #{} did {} successful writes, and {} unconfirmed writes".format(i, confirmed, unconfirmed))
                total_confirmed += confirmed
                total_unconfirmed += unconfirmed
            worker_executor.shutdown(wait=True)
            duration = time.time() - start_time
            rows_per_second = float(total_confirmed + total_unconfirmed) / duration
            debug("Made ({} confirmed, {} unconfirmed) inserts in total over {} seconds: {} rows per second".format(
                total_confirmed, total_unconfirmed, duration, rows_per_second))
            return total_confirmed, total_unconfirmed

        return finisher

    def check_common_invariants(self, session, base_rows, log_rows, update_rows, write_count, with_preimage):
        if with_preimage:
            self.check_preimage(log_rows)
        self.check_that_all_writes_were_recorded(session, write_count, update_rows)
        self.check_that_log_corresponds_to_current_state(session, base_rows, update_rows)
        self.check_that_every_log_entry_of_one_partition_is_in_one_stream(session, update_rows)
        self.check_that_log_entries_are_not_earlier_than_their_stream(session, update_rows)

    def check_preimage(self, log_rows):
        debug('Check that preimage reflects the previous state of the row')

        latest_rows = {}
        for _, write in itertools.groupby(log_rows, key=lambda r: r.cdc_time):
            writes = list(write)
            if len(writes) > 1:
                # The row was updated
                preimage_row, update_row = writes
                self.assertEqual(preimage_row.cdc_operation, CdcLogOperations.PREIMAGE)
                self.assertEqual(update_row.cdc_operation, CdcLogOperations.INSERT)
                self.assertEqual(preimage_row.a, update_row.a)
                self.assertIsNotNone(preimage_row.b)
                old_row = latest_rows[update_row.a]
                self.assertEqual(preimage_row.b, old_row.b, "Preimage did not contain previous state of the row")
            else:
                # This is a new row - no preimage
                update_row = writes[0]
                self.assertEqual(update_row.cdc_operation, CdcLogOperations.INSERT)

            latest_rows[update_row.a] = update_row

    def check_that_all_writes_were_recorded(self, session, write_count, update_rows):
        debug('Check that there are as many "insert" operation rows in log table as there were writes')
        confirmed_count, unconfirmed_count = write_count
        self.assertGreaterEqual(len(update_rows), confirmed_count)
        self.assertLessEqual(len(update_rows), confirmed_count + unconfirmed_count)

    def check_that_log_corresponds_to_current_state(self, session, base_rows, update_rows):
        debug('Check that most recent records in log table correspond to current state of base table')

        latest_rows = {}
        for row in update_rows:
            if row.a in latest_rows:
                self.assertGreaterEqual(datetime_from_uuid1(row.cdc_time), datetime_from_uuid1(latest_rows[row.a].cdc_time),
                                        'Update rows are not sorted, pk: {}'.format(row.a))
            latest_rows[row.a] = row

        self.assertEquals(len(latest_rows), len(base_rows))
        for row in base_rows:
            self.assertIn(row.a, latest_rows)
            latest = latest_rows[row.a]
            self.assertEquals(row.a, latest.a)
            self.assertEquals(row.b, latest.b)

    def check_that_every_log_entry_of_one_partition_is_in_one_stream(self, session, update_rows):
        debug('Check that, within a generation, a particular partition key ' +
              'may be written to one stream only')

        cdc_desciption_rows = sorted(desc.time for desc in self.get_cdc_description_rows(session))

        stream_for_partition = {}
        for row in update_rows:
            generation_number = bisect.bisect(cdc_desciption_rows, datetime_from_uuid1(row.cdc_time))
            idx = (generation_number, row.a)
            if idx not in stream_for_partition:
                stream_for_partition[idx] = row.cdc_stream_id
            else:
                self.assertEquals(stream_for_partition[idx], row.cdc_stream_id, "A partition was written " +
                                  "to more than one stream within one generation")

    def check_that_log_entries_are_not_earlier_than_their_stream(self, session, update_rows):
        debug('Check that log entries do not have earlier timestamp than their stream')
        stream_to_timestamp = self.get_stream_id_to_timestamp_assignment(session)

        for row in update_rows:
            self.assertIn(row.cdc_stream_id, stream_to_timestamp)
            timestamp = stream_to_timestamp[row.cdc_stream_id]
            self.assertLessEqual(timestamp, datetime_from_uuid1(row.cdc_time))

    def check_that_log_entries_and_their_streams_are_in_the_same_vnode(self, session, base_rows, update_rows, ring,
                                                                       time_range_begin=None, time_range_end=None):
        debug('Check that log entries and corresponding streams are from the same vnode')

        pk_to_vnode = {r.a: self.get_vnode_for_partition_token(ring, Murmur3Token(r.tok)) for r in base_rows}
        log_vnodes = list(self.get_vnode_for_partition_token(ring, Murmur3Token(r.tok)) for r in update_rows)
        good_vnodes = set(log_vnodes)

        # Check that vnode for base row and vnode for log row match
        for row, log_row_vnode in zip(update_rows, log_vnodes):
            time = datetime_from_uuid1(row.cdc_time)
            if time_range_begin is not None and time < time_range_begin:
                continue
            if time_range_end is not None and time_range_end <= time:
                continue

            base_row_vnode = pk_to_vnode[row.a]
            if base_row_vnode not in good_vnodes:
                continue

            if base_row_vnode != log_row_vnode:
                debug('Timestamp of the offending log write: {}'.format(time))
            self.assertEquals(base_row_vnode, log_row_vnode)

    def generation_quality_check(self, session, gen_timestamp, ring):
        debug('Checking invariants on generation')
        debug('Checking if generation token ranges refine vnodes')
        gen_description = self.get_cdc_topology_description_for_timestamp(session, gen_timestamp)
        token_ranges = set(entry[0] for entry in gen_description)
        ring_tokens = set(token.value for token in ring)
        self.assertEqual(ring_tokens, token_ranges, 'Generation token ranges should cover all vnodes')

        debug('Checking that all vnodes have a stream')
        prev_token = gen_description[-1][0]
        for entry in gen_description:
            vnode_size = 0;
            if entry[0] > prev_token:
                vnode_size = entry[0] - prev_token
            else:
                vnode_size = 2**63 - 1 - prev_token + entry[0]
            if vnode_size > 1:
                self.assertTrue(any(int.from_bytes(stream[0:8], byteorder='big', signed=True) != entry[0] for stream in entry[1]))
            prev_token = entry[0]

    def get_sorted_update_rows(self, session, log_rows):
        update_rows = [r for r in log_rows if r.cdc_operation == CdcLogOperations.INSERT]
        assignment = self.get_stream_id_to_timestamp_assignment(session)
        return sorted(update_rows, key=lambda r: assignment[r.cdc_stream_id])

    def get_stream_id_to_timestamp_assignment(self, session):
        cdc_descriptions = self.get_cdc_description_rows(session)

        assignment = {}
        for desc in cdc_descriptions:
            for stream_id in desc.streams:
                self.assertNotIn(stream_id, assignment)
                assignment[stream_id] = desc.time

        return assignment

    def get_timestamp_of_first_generation_after(self, session, timestamp):
        cdc_descriptions = list(self.get_cdc_description_rows(session))
        return min(desc.time for desc in cdc_descriptions if desc.time > timestamp)

    def get_cdc_topology_description_for_timestamp(self, session, timestamp):
        query = session.prepare(f"SELECT description FROM {CDC_GENERATIONS_TABLE} WHERE time = ?")
        query.consistency_level = ConsistencyLevel.ALL
        rows = session.execute(query, (timestamp,))
        return list(rows)[0].description

    def get_base_and_log_rows(self, session, base_table_name):
        debug('Fetch table data')
        base_rows = list(self.get_base_table_rows(session, base_table_name))

        debug('Fetch cdc log table data')
        log_rows = list(self.get_log_table_rows(session, self.log_table_name(base_table_name)))

        debug('There are {} base table rows and {} cdc log rows'.format(len(base_rows), len(log_rows)))
        return base_rows, log_rows

    def get_base_table_rows(self, session, base_table_name):
        query = "SELECT a, b, token(a) AS tok FROM {}".format(base_table_name)
        return session.execute(SimpleStatement(query, consistency_level=ConsistencyLevel.ALL))

    def get_log_table_rows(self, session, log_table_name):
        query = ("SELECT \"cdc$stream_id\", " +
                 "\"cdc$time\", \"cdc$batch_seq_no\", a, b, \"cdc$operation\", \"cdc$ttl\", " +
                 "token(\"cdc$stream_id\") AS tok FROM {}").format(log_table_name)
        return session.execute(SimpleStatement(query, consistency_level=ConsistencyLevel.ALL))

    def get_vnode_for_partition_token(self, ring, token):
        idx = bisect.bisect_left(ring, token)
        if idx == 0 or idx == len(ring):
            return (ring[-1], ring[0])
        return (ring[idx - 1], ring[idx])

    def log_table_name(self, base_table_name):
        return base_table_name + "_scylla_cdc_log"
