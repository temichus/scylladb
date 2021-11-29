import bisect
import time
import itertools
import logging
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from enum import IntEnum
from threading import Event
import multiprocessing
from dataclasses import dataclass
from uuid import UUID

import pytest
from cassandra import ConsistencyLevel, InvalidRequest
from cassandra.connection import ConnectionException  # pylint: disable=no-name-in-module
from cassandra.metadata import Murmur3Token  # pylint: disable=no-name-in-module
from cassandra.query import SimpleStatement
from cassandra.util import datetime_from_uuid1
from cassandra.policies import FallthroughRetryPolicy

from dtest_class import Tester, wait_for
from dtest_setup import DTestSetup
from dtest_setup_overrides import DTestSetupOverrides
from tools.misc import ImmutableMapping

TOKENS_PER_NODE = 256

CDC_GENERATIONS_TABLE = 'system_distributed_everywhere.cdc_generation_descriptions_v2'
CDC_STREAMS_TABLE = 'system_distributed.cdc_streams_descriptions_v2'
CDC_TIMESTAMPS_TABLE = 'system_distributed.cdc_generation_timestamps'

logger = logging.getLogger(__name__)


@dataclass
class GenerationId:
    time: datetime
    uuid: UUID


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

    def populate_sequentially(self, n, wait_other_notice=False):
        cluster = self.cluster  # pylint: disable=no-member
        logger.debug('Starting node 1')
        # We need to use populate() for the first node, because it writes
        # a configuration file that specifies the first node as a seed.
        # Unless we do that, the first node will try to communicate
        # with 127.0.0.1 - which is configured to be the default seed - and
        # might fail, because in some environments the first node might listen
        # for gossip on a different address.
        cluster.populate(1).start(wait_for_binary_proto=True, wait_other_notice=wait_other_notice)
        for i in range(2, n + 1):
            logger.debug('Starting node {}'.format(i))
            node = self.cluster.new_node(i, auto_bootstrap=True)  # pylint: disable=no-member
            node.start(wait_for_binary_proto=True, wait_other_notice=wait_other_notice)

    # Retrieve the ID of the last known generation from the local tables of the node `session` is connected to.
    # The ID is a (timestamp, uuid) pair.
    def get_local_generation_id(self, session) -> GenerationId:
        rs = list(session.execute("SELECT streams_timestamp, uuid FROM system.cdc_local WHERE key = 'cdc_local'"))
        assert len(rs) == 1
        return GenerationId(time=rs[0].streams_timestamp, uuid=rs[0].uuid)

    def get_cdc_description_rows(self, session):
        query = SimpleStatement(f"SELECT * FROM {CDC_STREAMS_TABLE}", consistency_level=ConsistencyLevel.ONE)
        return session.execute(query)

    def get_last_generation_timestamp(self, session):
        timestamps = list(self.get_cdc_generation_timestamps(session))
        assert len(timestamps) > 0, "No CDC generations"
        return max(row.time for row in timestamps)

    def wait_for_last_generation_to_be_active(self, session):
        last_timestamp = self.get_last_generation_timestamp(session)
        # Add one second to account for clock differences
        self.sleep_until(last_timestamp + timedelta(seconds=1))
        logger.debug('Current generation timestamp: {}'.format(last_timestamp))
        return last_timestamp

    def get_all_cdc_description_rows(self, session):
        query = SimpleStatement(f"SELECT * FROM {CDC_STREAMS_TABLE}",
                                consistency_level=ConsistencyLevel.ONE)
        return session.execute(query)

    def get_single_cdc_description_rows(self, session, gen_ts):
        query = session.prepare(f"SELECT * FROM {CDC_STREAMS_TABLE} WHERE time = ?")
        query.consistency_level = ConsistencyLevel.ONE
        return session.execute(query, (gen_ts,))

    def get_cdc_generation_timestamps(self, session):
        query = SimpleStatement(f"SELECT time FROM {CDC_TIMESTAMPS_TABLE} WHERE key = 'timestamps'",
                                consistency_level=ConsistencyLevel.ONE)
        return session.execute(query)

    def wait_for_metadata_update(self, session, cluster_size):
        # Cluster metadata is updated asynchronously, so we need to wait
        def check_metadata():
            ring = self.get_vnode_ring(session)
            logger.debug('Token ring length: {}'.format(len(ring)))
            return len(ring) == cluster_size * 256
        wait_for(check_metadata, timeout=60, text='Waiting until metadata is updated')

    def sleep_until(self, timestamp):
        secs = (timestamp - datetime.utcnow()).total_seconds()
        if secs > 0:
            logger.debug('Sleeping for {} seconds'.format(secs))
            time.sleep(secs)

    def get_vnode_ring(self, session):
        return list(session.cluster.metadata.token_map.ring)


@pytest.mark.scylla_cdc
@pytest.mark.dtest_full
class TestCdc(Tester, CDCInitializeHelper):
    @pytest.fixture(scope='function', autouse=True)
    def fixture_dtest_setup_overrides(self, dtest_config):
        assert dtest_config.scylla_version is not None, 'CDC tests are intended for Scylla only'

        ring_delay_sec = 5
        dtest_setup_overrides = DTestSetupOverrides()
        dtest_setup_overrides.cluster_options = ImmutableMapping({
            'experimental_features': ['cdc'],
            'ring_delay_ms': ring_delay_sec * 1000,
            'num_tokens': TOKENS_PER_NODE,
            'hinted_handoff_enabled': False
        })
        return dtest_setup_overrides

    def simple_cdc_template(self, request, with_preimage):
        logger.debug('Setup a cluster')
        cluster = self.cluster
        self.populate_sequentially(n=3)
        node1 = cluster.nodes['node1']
        session = self.patient_cql_connection(node1)

        logger.debug('Wait for the last generation to become active')
        gen_timestamp = self.wait_for_last_generation_to_be_active(session)
        self.wait_for_metadata_update(session, cluster_size=3)
        ring = self.get_vnode_ring(session)

        self.generation_quality_check(session, gen_timestamp, ring)

        logger.debug('Create a table with CDC enabled, and start writing to it')
        finish_writing = self.run_writes_with_counting(request, node1, with_preimage=with_preimage)

        logger.debug('Write for 15 more seconds, and stop writing')
        time.sleep(15)
        write_count = finish_writing()

        base_rows, log_rows = self.get_base_and_log_rows(session, "ks.cf")
        update_rows = self.get_sorted_update_rows(session, log_rows)

        logger.debug('Check invariants on written data')
        self.check_common_invariants(session, base_rows, log_rows, update_rows, write_count, with_preimage)
        self.check_that_log_entries_and_their_streams_are_in_the_same_vnode(session, base_rows, update_rows, ring)

        logger.debug('Test finished')

    def test_simple_cdc(self, request):
        self.simple_cdc_template(request=request, with_preimage=False)

    @pytest.mark.next_gating
    def test_simple_cdc_with_preimage(self, request):
        self.simple_cdc_template(request=request, with_preimage=True)

    def cluster_expansion_with_cdc_template(self, request, with_preimage):
        logger.debug('Setup a cluster')
        cluster = self.cluster
        self.populate_sequentially(n=3)
        node1 = cluster.nodes['node1']
        session = self.patient_cql_connection(node1)

        logger.debug('Wait for the last generation to become active')
        gen_timestamp = self.wait_for_last_generation_to_be_active(session)
        self.wait_for_metadata_update(session, cluster_size=3)
        ring_before_expansion = self.get_vnode_ring(session)

        self.generation_quality_check(session, gen_timestamp, ring_before_expansion)

        logger.debug('Create a table with CDC enabled, and start writing to it')
        finish_writing = self.run_writes_with_counting(request, node1, with_preimage=with_preimage)

        logger.debug('Add new node to the cluster')
        expansion_start_time = datetime.utcnow()
        node4 = self.cluster.new_node(4, auto_bootstrap=True)
        node4.start(wait_for_binary_proto=True)

        logger.debug('Wait until new generation starts')
        gen_timestamp = self.wait_for_last_generation_to_be_active(session)

        logger.debug('Write for 15 more seconds, and stop writing')
        time.sleep(15)
        write_count = finish_writing()

        self.wait_for_metadata_update(session, cluster_size=4)
        ring_after_expansion = self.get_vnode_ring(session)
        assert ring_before_expansion != ring_after_expansion

        self.generation_quality_check(session, gen_timestamp, ring_after_expansion)

        base_rows, log_rows = self.get_base_and_log_rows(session, "ks.cf")
        update_rows = self.get_sorted_update_rows(session, log_rows)

        logger.debug('Check invariants on written data')
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

        logger.debug('Test finished')

    @pytest.mark.next_gating
    def test_cluster_expansion_with_cdc(self, request):
        self.cluster_expansion_with_cdc_template(request, with_preimage=False)

    def test_cluster_expansion_with_cdc_and_preimage(self, request):
        self.cluster_expansion_with_cdc_template(request=request, with_preimage=True)

    def cluster_reduction_with_cdc_template(self, request, with_preimage):
        logger.debug('Setup a cluster')
        cluster = self.cluster
        self.populate_sequentially(n=4)
        node1 = cluster.nodes['node1']
        node3 = cluster.nodes['node3']
        session = self.patient_cql_connection(node1)

        logger.debug('Wait for the last generation to become active')
        gen_timestamp = self.wait_for_last_generation_to_be_active(session)
        self.wait_for_metadata_update(session, cluster_size=4)
        ring = self.get_vnode_ring(session)

        self.generation_quality_check(session, gen_timestamp, ring)

        logger.debug('Create a table with CDC enabled, and start writing to it')
        finish_writing = self.run_writes_with_counting(request, node1, with_preimage=with_preimage)
        time.sleep(15)

        logger.debug('Downsize the cluster by one node')
        reduction_start_time = datetime.utcnow()
        node3.decommission()
        reduction_end_time = datetime.utcnow()

        logger.debug('Write for 15 more seconds, and stop writing')
        time.sleep(15)
        write_count = finish_writing()

        base_rows, log_rows = self.get_base_and_log_rows(session, "ks.cf")
        update_rows = self.get_sorted_update_rows(session, log_rows)

        logger.debug('Check invariants on written data')
        self.check_common_invariants(session, base_rows, log_rows, update_rows, write_count, with_preimage)
        self.check_that_log_entries_and_their_streams_are_in_the_same_vnode(session, base_rows, update_rows, ring)

        logger.debug('Test finished')

    @pytest.mark.next_gating
    def test_cluster_reduction_with_cdc(self, request):
        self.cluster_reduction_with_cdc_template(request=request, with_preimage=False)

    def test_cluster_reduction_with_cdc_and_preimage(self, request):
        self.cluster_reduction_with_cdc_template(request=request, with_preimage=True)

    def schema_change_template(self, request, alter_query, with_preimage=False, additional_fields=[]):
        logger.debug('Setup a cluster')
        cluster = self.cluster
        self.populate_sequentially(n=3)
        node1 = cluster.nodes['node1']
        session = self.patient_cql_connection(node1)

        logger.debug('Wait for the last generation to become active')
        gen_timestamp = self.wait_for_last_generation_to_be_active(session)
        self.wait_for_metadata_update(session, cluster_size=3)
        ring = self.get_vnode_ring(session)

        self.generation_quality_check(session, gen_timestamp, ring)

        logger.debug('Create a table with CDC enabled, and start writing to it')
        finish_writing = self.run_writes_with_counting(request=request, node=node1,
                                                       with_preimage=with_preimage,
                                                       additional_fields=additional_fields)
        time.sleep(15)

        logger.debug('Alter schema: {}'.format(alter_query))
        session.execute(alter_query)

        logger.debug('Write for 15 more seconds, and stop writing')
        time.sleep(15)
        write_count = finish_writing()

        base_rows, log_rows = self.get_base_and_log_rows(session, "ks.cf")
        update_rows = self.get_sorted_update_rows(session, log_rows)

        logger.debug('Check invariants on written data')
        self.check_common_invariants(session, base_rows, log_rows, update_rows, write_count, with_preimage)
        self.check_that_log_entries_and_their_streams_are_in_the_same_vnode(session, base_rows, update_rows, ring)

        logger.debug('Test finished')

    @pytest.mark.next_gating
    def test_change_field_type_with_cdc(self, request):
        self.schema_change_template(request, "ALTER TABLE ks.cf ALTER b TYPE blob")

    def test_change_field_type_with_cdc_and_preimage(self, request):
        self.schema_change_template(request, "ALTER TABLE ks.cf ALTER b TYPE blob", with_preimage=True)

    @pytest.mark.next_gating
    def test_add_field_with_cdc(self, request):
        self.schema_change_template(request, "ALTER TABLE ks.cf ADD c int")

    def test_add_field_with_cdc_and_preimage(self, request):
        self.schema_change_template(request, "ALTER TABLE ks.cf ADD c int", with_preimage=True)

    @pytest.mark.next_gating
    def test_remove_field_with_cdc(self, request):
        self.schema_change_template(request, "ALTER TABLE ks.cf DROP c", additional_fields=["c int"])

    def test_remove_field_with_cdc_and_preimage(self, request):
        self.schema_change_template(request, "ALTER TABLE ks.cf DROP c",
                                    additional_fields=["c int"], with_preimage=True)

    # Regression test for Scylla issue #7127
    def test_check_and_repair_cdc_streams_liveness(self, fixture_dtest_setup: DTestSetup):
        # During the test, error "Could not find CDC generation" appears as part of the test logic.
        # The teardown fails because it expects a cluster doesn't contain errors if the test is passed.
        fixture_dtest_setup.ignore_log_patterns = (
            "Could not find CDC generation with timestamp .*in distributed system tables.*even though some node"
            " gossiped about it.",
        )

        logger.debug('Setup a single node cluster')
        self.populate_sequentially(n=1)
        node = self.cluster.nodes['node1']
        session = self.patient_cql_connection(node)

        logger.debug('Wait for the last generation to become active')
        gen_timestamp = self.wait_for_last_generation_to_be_active(session)

        # Get the UUID of this generation, which is used as the partition key in the GENERATIONS table
        gen_id = self.get_local_generation_id(session)

        # Sanity check: the timestamp stored by the node in system.cdc_local
        # is the timestamp of the last generation, i.e. gen_timestamp
        assert gen_timestamp, gen_id.time

        logger.debug(f'Deleting generation ({gen_timestamp}, {gen_id.uuid})')
        query = session.prepare(f"DELETE FROM {CDC_GENERATIONS_TABLE} WHERE id = ?")
        session.execute(query, (gen_id.uuid,))

        self.ignore_log_patterns = ['Could not find CDC generation']

        def check_and_repair():
            node.nodetool('checkAndRepairCdcStreams')
        logger.debug(f'Running checkAndRepairCdcStreams...')
        p = multiprocessing.Process(target=check_and_repair)
        p.start()

        # The command should terminate immediately; we give it 60 seconds
        # to account for scheduling delays etc.
        p.join(60)

        if p.is_alive():
            # Still running -- we have a liveness problem.
            p.terminate()
            p.join()
            assert False, "checkAndRepairCdcStreams did not terminate in time"

        # Ok, let's also check if the command actually created a generation just in case
        # and perform some sanity checks for the generation's consistency
        logger.debug("Retrieving generation data")
        rows = list(session.execute(f"SELECT id, num_ranges FROM {CDC_GENERATIONS_TABLE}"))
        assert len(rows) > 0, "No CDC generations"

        uuid = rows[0].id
        assert all(r.id == uuid for r in rows), \
            "More than one generation IDs detected, but there should be exactly one"

        num_ranges = rows[0].num_ranges
        assert len(rows) == num_ranges, f"Expected {num_ranges} number of rows, got {len(rows)}"

        logger.debug("Waiting for the generation to appear in the client table...")
        # It should appear pretty much instantaneously, but with those Jenkins machines nobody knows...
        old_gen_timestamp = gen_timestamp

        def new_gen_appeared():
            gen_timestamp = self.get_last_generation_timestamp(session)
            assert gen_timestamp > old_gen_timestamp
            return gen_timestamp > old_gen_timestamp
        wait_for(new_gen_appeared, 1, "waiting for new generation to appear in client table", 60)

        gen_timestamp = self.get_last_generation_timestamp(session)
        assert gen_timestamp > old_gen_timestamp

        logger.debug(f"New generation timestamp: {gen_timestamp}")
        ring = self.get_vnode_ring(session)
        self.generation_quality_check(session, gen_timestamp, ring)

        logger.debug('Test finished')

    def run_writes_with_counting(self, request, node, with_preimage=False, additional_fields=[]):
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
        request.addfinalizer(lambda: stop_event.set())
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
                    logger.debug('Got ConnectionException, probably because the cluster is being downsized, retrying; ' +
                                 'exception was {}'.format(e))
                    # We cannot determine if the write was successful.
                    unconfirmed_writes += 1
                except InvalidRequest as e:
                    if "cdc: attempted to get a stream from an earlier generation than the currently used" in str(e):
                        logger.debug('Attempted to write with a timestamp older than the current generation. ' +
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
                logger.debug("Worker #{} did {} successful writes, and {} unconfirmed writes".format(
                    i, confirmed, unconfirmed))
                total_confirmed += confirmed
                total_unconfirmed += unconfirmed
            worker_executor.shutdown(wait=True)
            duration = time.time() - start_time
            rows_per_second = float(total_confirmed + total_unconfirmed) / duration
            logger.debug("Made ({} confirmed, {} unconfirmed) inserts in total over {} seconds: {} rows per second".format(
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
        logger.debug('Check that preimage reflects the previous state of the row')

        latest_rows = {}
        for _, write in itertools.groupby(log_rows, key=lambda r: r.cdc_time):
            writes = list(write)
            if len(writes) > 1:
                # The row was updated
                preimage_row, update_row = writes
                assert preimage_row.cdc_operation == CdcLogOperations.PREIMAGE
                assert update_row.cdc_operation == CdcLogOperations.INSERT
                assert preimage_row.a == update_row.a
                assert preimage_row.b is not None
                old_row = latest_rows[update_row.a]
                assert preimage_row.b == old_row.b, "Preimage did not contain previous state of the row"
            else:
                # This is a new row - no preimage
                update_row = writes[0]
                assert update_row.cdc_operation == CdcLogOperations.INSERT

            latest_rows[update_row.a] = update_row

    def check_that_all_writes_were_recorded(self, session, write_count, update_rows):
        logger.debug('Check that there are as many "insert" operation rows in log table as there were writes')
        confirmed_count, unconfirmed_count = write_count
        assert len(update_rows) >= confirmed_count
        assert len(update_rows) <= confirmed_count + unconfirmed_count

    def check_that_log_corresponds_to_current_state(self, session, base_rows, update_rows):
        logger.debug('Check that most recent records in log table correspond to current state of base table')

        latest_rows = {}
        for row in update_rows:
            if row.a in latest_rows:
                assert datetime_from_uuid1(row.cdc_time) >= datetime_from_uuid1(
                    latest_rows[row.a].cdc_time), 'Update rows are not sorted, pk: {}'.format(row.a)
            latest_rows[row.a] = row

        assert len(latest_rows) == len(base_rows)
        for row in base_rows:
            assert row.a in latest_rows
            latest = latest_rows[row.a]
            assert row.a == latest.a
            assert row.b == latest.b

    def check_that_every_log_entry_of_one_partition_is_in_one_stream(self, session, update_rows):
        logger.debug('Check that, within a generation, a particular partition key ' +
                     'may be written to one stream only')

        cdc_desciption_rows = sorted(desc.time for desc in self.get_cdc_description_rows(session))

        stream_for_partition = {}
        for row in update_rows:
            generation_number = bisect.bisect(cdc_desciption_rows, datetime_from_uuid1(row.cdc_time))
            idx = (generation_number, row.a)
            if idx not in stream_for_partition:
                stream_for_partition[idx] = row.cdc_stream_id
            else:
                assert stream_for_partition[idx] == row.cdc_stream_id, "A partition was written " \
                                                                       "to more than one stream within one generation"

    def check_that_log_entries_are_not_earlier_than_their_stream(self, session, update_rows):
        logger.debug('Check that log entries do not have earlier timestamp than their stream')
        stream_to_timestamp = self.get_stream_id_to_timestamp_assignment(session)

        for row in update_rows:
            assert row.cdc_stream_id in stream_to_timestamp
            timestamp = stream_to_timestamp[row.cdc_stream_id]
            assert timestamp <= datetime_from_uuid1(row.cdc_time)

    def check_that_log_entries_and_their_streams_are_in_the_same_vnode(self, session, base_rows, update_rows, ring,
                                                                       time_range_begin=None, time_range_end=None):
        logger.debug('Check that log entries and corresponding streams are from the same vnode')

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
                logger.debug('Timestamp of the offending log write: {}'.format(time))
            assert base_row_vnode == log_row_vnode

    def generation_quality_check(self, session, gen_timestamp, ring):
        logger.debug('Checking invariants on generation')
        logger.debug('Checking if generation token ranges refine vnodes')
        gen_description = list(self.get_single_cdc_description_rows(session, gen_timestamp))
        gen_tokens = set(entry.range_end for entry in gen_description)
        ring_tokens = set(token.value for token in ring)
        assert ring_tokens <= gen_tokens, 'Vnodes should contain generation token ranges'

        logger.debug('Checking that all vnodes have a stream')
        prev_range_end = gen_description[-1].range_end
        for entry in gen_description:
            range_end = entry.range_end
            vnode_size = 0
            if range_end > prev_range_end:
                vnode_size = range_end - prev_range_end
            else:
                vnode_size = 2**63 - 1 - prev_range_end + range_end
            if vnode_size > 1:
                assert any(int.from_bytes(stream[0:8], byteorder='big',
                                          signed=True) != range_end for stream in entry.streams)
            prev_range_end = range_end

    def get_sorted_update_rows(self, session, log_rows):
        update_rows = [r for r in log_rows if r.cdc_operation == CdcLogOperations.INSERT]
        assignment = self.get_stream_id_to_timestamp_assignment(session)
        return sorted(update_rows, key=lambda r: assignment[r.cdc_stream_id])

    def get_stream_id_to_timestamp_assignment(self, session):
        cdc_descriptions = self.get_all_cdc_description_rows(session)

        assignment = {}
        for desc in cdc_descriptions:
            for stream_id in desc.streams:
                assert stream_id not in assignment
                assignment[stream_id] = desc.time

        return assignment

    def get_timestamp_of_first_generation_after(self, session, timestamp):
        cdc_descriptions = list(self.get_cdc_description_rows(session))
        return min(desc.time for desc in cdc_descriptions if desc.time > timestamp)

    def get_base_and_log_rows(self, session, base_table_name):
        logger.debug('Fetch table data')
        base_rows = list(self.get_base_table_rows(session, base_table_name))

        logger.debug('Fetch cdc log table data')
        log_rows = list(self.get_log_table_rows(session, self.log_table_name(base_table_name)))

        logger.debug('There are {} base table rows and {} cdc log rows'.format(len(base_rows), len(log_rows)))
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
