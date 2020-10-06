import os
import random
import shutil
import string

from nose.plugins.attrib import attr

from assertions import assert_row_count
from dtest import Tester, debug, wait_for
from scylla_defines import TABLE_NAME, KEYSPACE_NAME, CompactionStrategy, FULL_TABLE_NAME
from scylla_tools import get_sstables_files, get_cf_dir
from tools import make_snapshot, restore_snapshot_with_refresh, restore_snapshot_with_sstableloader

NUM_OF_NODES = 2
RF = NUM_OF_NODES
KB = 1024
MB = 1024 * 1024
WRITE_SIZE_UNIT_IN_MB = 1
COLUMN_DEFAULT_SIZE = MB
NUM_OF_COLUMNS = 50
PARTITIONS = 100
ROWS_IN_PARTITION = 20
BIG_PARTITION_ROWS = 10000
NUM_OF_GENERATED_SSTABLES = 4
NUM_WRITES_PER_SSTABLE = 1
START_INDEX = 1


@attr('dtest-full')
class IcsCompactionTest(Tester):

    #######################   Helper Functions Start  ###########################################################################
    def alter_table_compaction(self, compaction_strategy=None, table_name=TABLE_NAME, keyspace_name=KEYSPACE_NAME,
                               sstable_size_in_mb=None, additional_compaction_params=None,
                               assert_altered_compaction=False):
        """
         1. Alters table compaction like: ALTER TABLE mykeyspace.mytable WITH compaction = {'class' : 'IncrementalCompactionStrategy'}
         2. Can verify the new strategy is successfully applied to table.
        """

        base_query = "ALTER TABLE {}.{} WITH compaction = ".format(keyspace_name, table_name)
        dict_requested_compaction = {}
        if compaction_strategy:
            dict_requested_compaction['class'] = compaction_strategy._value_

        if sstable_size_in_mb:
            dict_requested_compaction['sstable_size_in_mb'] = sstable_size_in_mb

        if additional_compaction_params:
            for param in additional_compaction_params:
                dict_requested_compaction.update(param)

        full_alter_query = base_query + str(dict_requested_compaction)
        debug("query is: {}".format(full_alter_query))
        self.execute_session_cql_query(query=full_alter_query)

        if assert_altered_compaction:
            self.assertTrue(self._get_table_compaction_strategy(keyspace_name=keyspace_name,
                                                                table_name=table_name) == compaction_strategy)

    def _get_table_compaction_strategy(self, table_name=TABLE_NAME, keyspace_name=KEYSPACE_NAME, ):
        verify_query = "SELECT keyspace_name, table_name, compaction FROM system_schema.tables WHERE keyspace_name = '{}' AND table_name = '{}'".format(
            keyspace_name, table_name)
        result_matrix = list(self.execute_session_cql_query(query=verify_query))
        debug(result_matrix)
        retrieved_compaction = "Unknown"
        if result_matrix[0].keyspace_name == keyspace_name and result_matrix[0].table_name == table_name:
            retrieved_compaction = CompactionStrategy.from_str(result_matrix[0].compaction['class'])
            debug("Retrieved compaction strategy for {}.{} is: {}".format(keyspace_name, table_name,
                                                                          retrieved_compaction))
        return retrieved_compaction

    def create_cluster(self, num_of_nodes, configuration_options=None, jvm_args=None):
        if configuration_options:
            self.cluster.set_configuration_options(values=configuration_options)
        self.cluster.populate(num_of_nodes).start(jvm_args=jvm_args, wait_for_binary_proto=True)
        node1 = self.cluster.nodelist()[0]
        session = self.patient_cql_connection(node1)
        return session

    def create_table(self, session, compaction_strategy=CompactionStrategy.SIZE_TIERED,
                     table_name=TABLE_NAME, keyspace_name=KEYSPACE_NAME, sstable_size_in_mb=10,
                     is_large_partitions=False):
        session.execute("USE {}".format(keyspace_name))
        if not is_large_partitions:
            query = """
                CREATE TABLE {} (
                    key blob PRIMARY KEY,
                    "C0" blob
                )
            """.format(table_name)
        else:
            query = 'create table {} (pk int, ck int, {}, clist list<int>, cset set<text>, cmap map<int, text>, ' \
                    'PRIMARY KEY(pk, ck))'.format(table_name,
                                                  ', '.join('c%d int' % i for i in range(1, NUM_OF_COLUMNS)))
        query += " WITH compaction = { 'class' : '" + compaction_strategy._value_ + "', 'sstable_size_in_mb' : '" + str(
            sstable_size_in_mb) + "' }"
        debug("query is:{}".format(query))

        session.execute(query)

    def prepare(self, num_of_nodes=NUM_OF_NODES, r_factor=RF,
                compaction_strategy=CompactionStrategy.SIZE_TIERED,
                table_name=TABLE_NAME, keyspace_name=KEYSPACE_NAME, sstable_size_in_mb=10,
                is_large_partitions=False):
        jvm_args = ["--compaction-enforce-min-threshold", "true"]
        session = self.create_cluster(num_of_nodes=num_of_nodes, jvm_args=jvm_args)
        self.create_ks(session=session, name=keyspace_name, rf=r_factor)
        self.create_table(session=session, compaction_strategy=compaction_strategy,
                          keyspace_name=keyspace_name, table_name=table_name,
                          sstable_size_in_mb=sstable_size_in_mb, is_large_partitions=is_large_partitions)
        return session

    def _count_table_entries(self, keyspace=KEYSPACE_NAME, table=TABLE_NAME):
        session = self.patient_cql_connection(self.cluster.nodelist()[0])
        res = session.execute(
            "SELECT COUNT(*) FROM {}.{}".format(keyspace, table))
        count = res.current_rows[0].count
        debug("Current count of DB entries: {}".format(count))
        return count

    @staticmethod
    def _get_sstables_file_size_in_mb(list_sstable_files, cf_dir):
        debug("Get sstables file size for: {}".format(list_sstable_files))
        list_files_size = []
        for file in list_sstable_files:
            # A sample returned value of os.path.getsize is: '3165790' - which is converted to 3MB
            file_size_in_mb = os.path.getsize(os.path.join(cf_dir, file)) >> 20
            list_files_size.append(file_size_in_mb)
        return list_files_size

    def _check_sstable_file_size_limit(self, list_sstable_files, sstable_size_in_mb):
        debug("Validating maximum sstable file size of {} for: {}".format(sstable_size_in_mb, list_sstable_files))
        cf_dir = get_cf_dir(os.path.join(self.test_path, 'test', 'node1', 'data', KEYSPACE_NAME), TABLE_NAME)
        for file in list_sstable_files:
            file_size_in_mb = os.path.getsize(os.path.join(cf_dir, file)) >> 20
            assert file_size_in_mb <= sstable_size_in_mb + 1, "File size is bigger than: {}".format(sstable_size_in_mb)

    def _create_table_and_alter_compaction(self, original_compaction, new_compaction):
        self.prepare(num_of_nodes=1, r_factor=1,
                     compaction_strategy=original_compaction)

        list_additional_params = [{'bucket_high': 1.2}, {'bucket_low': 0.8}, {'min_sstable_size': 10},
                                  {'min_threshold': 2}, {'max_threshold': 80}]
        node1 = self.cluster.nodelist()[0]
        self._write_and_flush_sstables(num_of_generated_sstables=5, start_index=1, increasing_write_size=True,
                                       num_writes_per_sstable=8)
        if new_compaction in [CompactionStrategy.INCREMENTAL, CompactionStrategy.SIZE_TIERED]:
            additional_compaction_params = list_additional_params
        else:
            additional_compaction_params = None
        self.alter_table_compaction(compaction_strategy=new_compaction,
                                    additional_compaction_params=additional_compaction_params,
                                    assert_altered_compaction=True)
        node1.wait_for_compactions()
        node1.compact()
        node1.wait_for_compactions()

    def _get_sstable_files_and_sizes(self):
        """
        Returns sstable files and their sizes for a table.
        :return: sstables_files, files_size
        """
        cf_dir = get_cf_dir(os.path.join(self.test_path, 'test', 'node1', 'data', KEYSPACE_NAME), TABLE_NAME)
        sstables_files = get_sstables_files(cf_dir, f_type='Data')
        files_size = self._get_sstables_file_size_in_mb(list_sstable_files=sstables_files, cf_dir=cf_dir)
        debug("Files sizes are: {}".format(files_size))
        return sstables_files, files_size

    def _write_and_flush_sstables(self, start_index=START_INDEX, num_of_generated_sstables=NUM_OF_GENERATED_SSTABLES,
                                  write_range=1, write_size_unit_in_mb=WRITE_SIZE_UNIT_IN_MB,
                                  increasing_write_size=False, num_writes_per_sstable=NUM_WRITES_PER_SSTABLE):
        self._read_or_write_and_flush_sstables(num_of_generated_sstables=num_of_generated_sstables,
                                               start_index=start_index, write_range=write_range,
                                               write_size_unit_in_mb=write_size_unit_in_mb,
                                               increasing_write_size=increasing_write_size,
                                               num_writes_per_sstable=num_writes_per_sstable, read_only=False)

    def _read_generated_sstables_data(self, start_index=START_INDEX,
                                      num_of_generated_sstables=NUM_OF_GENERATED_SSTABLES, write_range=1,
                                      write_size_unit_in_mb=WRITE_SIZE_UNIT_IN_MB,
                                      increasing_write_size=False, num_writes_per_sstable=NUM_WRITES_PER_SSTABLE):
        self._read_or_write_and_flush_sstables(num_of_generated_sstables=num_of_generated_sstables,
                                               start_index=start_index, write_range=write_range,
                                               write_size_unit_in_mb=write_size_unit_in_mb,
                                               increasing_write_size=increasing_write_size,
                                               num_writes_per_sstable=num_writes_per_sstable, read_only=True)

    def _read_or_write_and_flush_sstables(self, num_of_generated_sstables, start_index, write_range=1,
                                          write_size_unit_in_mb=WRITE_SIZE_UNIT_IN_MB,
                                          increasing_write_size=False, num_writes_per_sstable=NUM_WRITES_PER_SSTABLE,
                                          read_only=False):
        """
        Generates sstables of sizes: write_size_unit or write_size_unit...write_size_unit * num_of_generated_sstables
        """
        node1 = self.cluster.nodelist()[0]
        op_mode = 'write' if not read_only else 'read'
        for idx in range(1, num_of_generated_sstables + 1):
            write_size_unit_in_bytes = write_size_unit_in_mb * MB
            write_size = write_size_unit_in_bytes * idx if increasing_write_size else write_size_unit_in_bytes
            debug("stress node1 #{idx} (file-size: {write_size})".format(**locals()))
            results, errors = node1.stress(
                [op_mode, "no-warmup", 'n={}'.format(num_writes_per_sstable), '-pop',
                 'seq={}..{}'.format(start_index, start_index + write_range),
                 "-col",
                 "n=fixed(1)",
                 "size=fixed({})".format(write_size),
                 "-rate", "threads=1"], capture_output=True)
            debug('Stress results:\n' + ''.join(results + errors))
            self.assertFalse(errors, "Some errors during stress %s" % errors)
            if not read_only:
                debug("flush #{}".format(idx))
                node1.flush()
                self._count_table_entries()
            start_index += write_range

    def _generate_cluster_with_table_data_snapshot(self, compaction_strategy, sstable_size_in_mb=2):
        """
        Base testing method:

        1. Create a keyspace and a table with chosen compaction strategy
        2. Generate sstables with rows
        4. Take a snapshot
        5. return the snapshot.

        """
        if compaction_strategy is not CompactionStrategy.TIME_WINDOW:
            session = self.prepare(num_of_nodes=1, r_factor=1,
                                   compaction_strategy=compaction_strategy,
                                   keyspace_name=KEYSPACE_NAME,
                                   table_name=TABLE_NAME,
                                   sstable_size_in_mb=sstable_size_in_mb)
        else:
            session = self.prepare(num_of_nodes=1, r_factor=1,
                                   compaction_strategy=compaction_strategy,
                                   keyspace_name=KEYSPACE_NAME,
                                   table_name=TABLE_NAME)
        node1 = self.cluster.nodelist()[0]
        self._write_and_flush_sstables(num_of_generated_sstables=NUM_OF_GENERATED_SSTABLES, start_index=1,
                                       increasing_write_size=True)
        node1.wait_for_compactions()
        snapshot_dir = make_snapshot(node1, KEYSPACE_NAME, TABLE_NAME, 'basic')
        return snapshot_dir, session, node1

    def basic_snapshot_and_restore(self, use_sstableloader):
        """
        Base testing method:

        1. Create a keyspace and a table with chosen compaction strategy
        2. Generate sstables with rows
        4. Take a snapshot
        5. Generate more sstables with rows after the snapshot
        6. Drop the keyspace, assure we have no data after the deletion
        7. Restore the snapshot
        8. Verify we have the same num of rows and sstables generated prior to the snapshot.

        :param use_sstableloader: Whether to use sstable loader to
            restore the snapshot or nodetool refresh.
        """
        sstable_size_in_mb = 2
        snapshot_dir, session, node1 = self._generate_cluster_with_table_data_snapshot(
            compaction_strategy=CompactionStrategy.INCREMENTAL, sstable_size_in_mb=sstable_size_in_mb)

        # Write more data after the snapshot, this will get thrown away when we restore:
        self._write_and_flush_sstables(num_of_generated_sstables=2, start_index=5)
        assert_row_count(session=session, table_name=FULL_TABLE_NAME, expected=6)

        # Drop the keyspace, make sure we have no data:
        session.execute('DROP KEYSPACE {}'.format(KEYSPACE_NAME))
        shutil.rmtree(os.path.join(node1.get_path(), 'data', KEYSPACE_NAME))

        self.create_ks(session, name=KEYSPACE_NAME, rf=RF)
        self.create_table(session=session, compaction_strategy=CompactionStrategy.INCREMENTAL,
                          keyspace_name=KEYSPACE_NAME, table_name=TABLE_NAME,
                          sstable_size_in_mb=sstable_size_in_mb)

        assert_row_count(session=session, table_name=FULL_TABLE_NAME, expected=0)

        # Restore data from snapshot:
        if use_sstableloader:
            restore_snapshot_with_sstableloader(snapshot_dir=snapshot_dir, node=node1, keyspace=KEYSPACE_NAME,
                                                table=TABLE_NAME)
        else:
            restore_snapshot_with_refresh(snapshot_dir, node1, KEYSPACE_NAME, TABLE_NAME)
        node1.nodetool('refresh {} {}'.format(KEYSPACE_NAME, TABLE_NAME))
        # Check that the number of table entries on snapshot is restored.
        assert_row_count(session=session, table_name=FULL_TABLE_NAME, expected=4)
        sstables_files1, files_size = self._get_sstable_files_and_sizes()
        # Check that the number and sizes of ssables on snapshot is restored.
        assert set(files_size) == {2, 3, 5}, "Found sstable files with wrong sizes. files_size: {}".format(files_size)

        # clean up
        debug("removing snapshot_dir: " + snapshot_dir)
        shutil.rmtree(snapshot_dir)

    @staticmethod
    def insert_large_partitions_table_data(session, partition_range_end, rows_in_partition,
                                           partition_range_start=1,
                                           table_name=TABLE_NAME, num_of_columns=NUM_OF_COLUMNS):

        debug('Create {} partitions of {} columns with {} rows'.format(partition_range_end, num_of_columns,
                                                                       rows_in_partition))
        for i in range(partition_range_start, partition_range_end + 1):
            for k in range(1, rows_in_partition + 1):
                str = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(10))
                stmt = 'insert into {table_name} (pk, ck, {columns}, clist, cset, cmap) values ({ilist}, ' \
                       '{klist}, {int_values}, [{ilist}, {klist}], ' \
                       '{open}{set_value}{close}, {map_value})'.format(table_name=table_name,
                                                                       columns=', '.join(
                                                                           'c%d' % l for l in
                                                                           range(1, num_of_columns)),
                                                                       int_values=', '.join(
                                                                           '%d' % l for l in range(1, num_of_columns)),
                                                                       ilist=i, klist=k, open='{\'',
                                                                       set_value=str, close='\'}',
                                                                       map_value='{%d: \'%s\'}' % (k, str)
                                                                       )
                session.execute(stmt)

    #######################   Helper Functions End  ###########################################################################

    def check_default_compaction_strategy_test(self):
        session = self.create_cluster(num_of_nodes=1)
        self.create_ks(session=session, name=KEYSPACE_NAME, rf=1)
        self.create_cf(session=session, name=TABLE_NAME, columns={'c1': 'text', 'c2': 'text'})
        compaction = self._get_table_compaction_strategy()
        assert compaction == CompactionStrategy.INCREMENTAL, "Default compaction is: {}".format(compaction)

    def alter_table_stcs_to_lcs_to_ics_test(self):
        self._create_table_and_alter_compaction(new_compaction=CompactionStrategy.LEVELED,
                                                original_compaction=CompactionStrategy.SIZE_TIERED)
        self.alter_table_compaction(compaction_strategy=CompactionStrategy.INCREMENTAL, assert_altered_compaction=True)
        node1 = self.cluster.nodelist()[0]
        node1.compact()
        node1.wait_for_compactions()

    def alter_table_stcs_to_ics_to_stcs_test(self):
        self._create_table_and_alter_compaction(new_compaction=CompactionStrategy.INCREMENTAL,
                                                original_compaction=CompactionStrategy.SIZE_TIERED)
        self.alter_table_compaction(compaction_strategy=CompactionStrategy.SIZE_TIERED, assert_altered_compaction=True)
        node1 = self.cluster.nodelist()[0]
        node1.compact()
        node1.wait_for_compactions()

    def alter_table_ics_to_stcs_test(self):
        self._create_table_and_alter_compaction(original_compaction=CompactionStrategy.INCREMENTAL,
                                                new_compaction=CompactionStrategy.SIZE_TIERED)

    def alter_table_ics_to_lcs_test(self):
        self._create_table_and_alter_compaction(original_compaction=CompactionStrategy.INCREMENTAL,
                                                new_compaction=CompactionStrategy.LEVELED)

    def alter_table_ics_to_time_window_test(self):
        self._create_table_and_alter_compaction(original_compaction=CompactionStrategy.INCREMENTAL,
                                                new_compaction=CompactionStrategy.TIME_WINDOW)

    def alter_table_stcs_to_ics_test(self):
        self._create_table_and_alter_compaction(new_compaction=CompactionStrategy.INCREMENTAL,
                                                original_compaction=CompactionStrategy.SIZE_TIERED)

    def alter_table_lcs_to_ics_test(self):
        self._create_table_and_alter_compaction(new_compaction=CompactionStrategy.INCREMENTAL,
                                                original_compaction=CompactionStrategy.LEVELED)

    def alter_table_time_window_to_ics_test(self):
        self._create_table_and_alter_compaction(new_compaction=CompactionStrategy.INCREMENTAL,
                                                original_compaction=CompactionStrategy.TIME_WINDOW)

    def ics_snapshot_and_restore_with_sstableloader_test(self):
        """
        Test snapshot and restore with ICS and sstable-loader.
        """
        self.basic_snapshot_and_restore(use_sstableloader=True)

    def ics_snapshot_and_restore_with_refresh_test(self):
        """
        Test snapshot and restore with ICS and nodetool refresh.
        """
        self.basic_snapshot_and_restore(use_sstableloader=False)

    def time_window_to_ics_snapshot_refresh_test(self):
        """
        Test snapshot restore with ICS and nodetool refresh of source Time-Window sstables.
        """
        sstable_size_in_mb = 2
        snapshot_dir, session, node1 = self._generate_cluster_with_table_data_snapshot(
            compaction_strategy=CompactionStrategy.TIME_WINDOW)
        self.alter_table_compaction(compaction_strategy=CompactionStrategy.INCREMENTAL,
                                    sstable_size_in_mb=sstable_size_in_mb,
                                    assert_altered_compaction=True)
        restore_snapshot_with_refresh(snapshot_dir=snapshot_dir, node=node1, keyspace=KEYSPACE_NAME,
                                      table=TABLE_NAME)
        self._read_generated_sstables_data(increasing_write_size=True)

    def lcs_to_ics_snapshot_refresh_test(self):
        """
        Test snapshot restore with ICS and nodetool refresh of source LCS sstables.
        """
        sstable_size_in_mb = 2
        snapshot_dir, session, node1 = self._generate_cluster_with_table_data_snapshot(
            compaction_strategy=CompactionStrategy.LEVELED, sstable_size_in_mb=sstable_size_in_mb)
        self.alter_table_compaction(compaction_strategy=CompactionStrategy.INCREMENTAL,
                                    sstable_size_in_mb=sstable_size_in_mb,
                                    assert_altered_compaction=True)
        restore_snapshot_with_refresh(snapshot_dir=snapshot_dir, node=node1, keyspace=KEYSPACE_NAME,
                                      table=TABLE_NAME)
        self._read_generated_sstables_data(increasing_write_size=True)

    def stcs_to_ics_snapshot_refresh_test(self):
        """
        Test snapshot restore with ICS and nodetool refresh of source STCS sstables.
        """
        sstable_size_in_mb = 2
        snapshot_dir, session, node1 = self._generate_cluster_with_table_data_snapshot(
            compaction_strategy=CompactionStrategy.SIZE_TIERED, sstable_size_in_mb=sstable_size_in_mb)
        self.alter_table_compaction(compaction_strategy=CompactionStrategy.INCREMENTAL,
                                    sstable_size_in_mb=sstable_size_in_mb,
                                    assert_altered_compaction=True)
        restore_snapshot_with_refresh(snapshot_dir=snapshot_dir, node=node1, keyspace=KEYSPACE_NAME,
                                      table=TABLE_NAME)
        self._read_generated_sstables_data(increasing_write_size=True)

    def ics_refresh_with_big_sstable_files_test(self):
        """

        1. Create a keyspace and a table with STCS
        2. Generate sstables with rows
        3. Take a snapshot
        4. Delete the STCS table.
        5. Recreate the table with ICS.
        6. Restore the snapshot on the newly created ICS table.
        7. Run major compaction
        8. Verify we have the correct num of rows and file sizes after the major compaction.
        """

        sstable_size_in_mb = 2
        # create an STCS table with data
        session = self.prepare(num_of_nodes=1, r_factor=1,
                               compaction_strategy=CompactionStrategy.SIZE_TIERED,
                               keyspace_name=KEYSPACE_NAME,
                               table_name=TABLE_NAME,
                               sstable_size_in_mb=sstable_size_in_mb)

        node1 = self.cluster.nodelist()[0]
        num_rows_per_sstable = 50
        num_of_generated_sstables = 2
        max_generated_sstable_size = WRITE_SIZE_UNIT_IN_MB * num_of_generated_sstables
        max_compacted_sstable_size = sstable_size_in_mb + max_generated_sstable_size - 1

        self._write_and_flush_sstables(num_of_generated_sstables=num_of_generated_sstables, start_index=1,
                                       increasing_write_size=True, num_writes_per_sstable=num_rows_per_sstable,
                                       write_range=num_rows_per_sstable)

        node1.wait_for_compactions()
        # Create a snapshot
        snapshot_dir = make_snapshot(node1, KEYSPACE_NAME, TABLE_NAME, 'basic')

        # Drop the keyspace, make sure we have no data.
        session.execute('DROP KEYSPACE {}'.format(KEYSPACE_NAME))
        shutil.rmtree(os.path.join(node1.get_path(), 'data', KEYSPACE_NAME))

        # Re-create a clean table as ICS
        self.create_ks(session, name=KEYSPACE_NAME, rf=1)
        self.create_table(session=session, compaction_strategy=CompactionStrategy.INCREMENTAL,
                          keyspace_name=KEYSPACE_NAME, table_name=TABLE_NAME,
                          sstable_size_in_mb=sstable_size_in_mb)

        # Restore data from snapshot of big file:
        restore_snapshot_with_refresh(snapshot_dir, node1, KEYSPACE_NAME, TABLE_NAME)
        # Run major compaction
        node1.compact()
        sstables_files1, files_size = self._get_sstable_files_and_sizes()
        # Check that sstable file sizes maximum limit
        assert all([size <= max_compacted_sstable_size for size in
                    files_size]), "Found larger sstable file size than expected"
        # Check that the number of table rows after refresh is correct.
        assert_row_count(session=session, table_name=FULL_TABLE_NAME,
                         expected=num_rows_per_sstable * num_of_generated_sstables)

        # clean up
        debug("removing snapshot_dir: " + snapshot_dir)
        shutil.rmtree(snapshot_dir)

    def ics_sstables_refresh_with_collisions_test(self):
        """

        1. Create a keyspace and a table with chosen compaction strategy
        2. Generate sstables with rows
        4. Take a snapshot
        5. Generate more sstables with rows after the snapshot
        6. Restore the snapshot
        7. Verify we have the correct num of rows after the refresh.
        """

        sstable_size_in_mb = 2
        snapshot_dir, session, node1 = self._generate_cluster_with_table_data_snapshot(
            compaction_strategy=CompactionStrategy.INCREMENTAL, sstable_size_in_mb=sstable_size_in_mb)

        # Write more data after the snapshot, this will add some more rows not found on the snapshot of duplicated sstables:
        self._write_and_flush_sstables(num_of_generated_sstables=4, start_index=5)

        # Restore data from snapshot:
        restore_snapshot_with_refresh(snapshot_dir, node1, KEYSPACE_NAME, TABLE_NAME)
        # Check that the number of table entries after refresh is correct.
        assert_row_count(session=session, table_name=FULL_TABLE_NAME, expected=8)

        # clean up
        debug("removing snapshot_dir: " + snapshot_dir)
        shutil.rmtree(snapshot_dir)

    def ics_with_partitions_larger_than_sstable_size_test(self):
        """
        Check ics with variable size partitions, smaller and larger than sstable size on flush and on compaction.
        """
        # TODO: Add the below steps comments as pytest steps after moving to pytest
        # (1) Test sstables number and sizes after flushes
        sstable_size_in_mb = 2
        compaction_strategy = CompactionStrategy.INCREMENTAL
        self.prepare(num_of_nodes=NUM_OF_NODES, r_factor=RF,
                     compaction_strategy=compaction_strategy,
                     sstable_size_in_mb=sstable_size_in_mb)
        num_of_generated_sstables = 4
        # The maximum expected compacted sstable size is the addition of the 2 largets generated sstables.
        max_expected_file_size = WRITE_SIZE_UNIT_IN_MB * (2 * num_of_generated_sstables - 1)
        start_index = 1
        self._write_and_flush_sstables(num_of_generated_sstables=num_of_generated_sstables, start_index=start_index,
                                       increasing_write_size=True)

        def is_compaction_executed():
            sstables_files1, _ = self._get_sstable_files_and_sizes()
            debug("Found {} sstables, out of {} originally created".format(
                len(sstables_files1), num_of_generated_sstables))
            return len(sstables_files1) < num_of_generated_sstables

        wait_for(func=is_compaction_executed, text=str(is_compaction_executed),
                 timeout=100)
        sstables_files1, files_size = self._get_sstable_files_and_sizes()
        max_found_file_size = max(files_size)
        debug("Number of files after {} flushes is: {} , {}".format(num_of_generated_sstables, len(sstables_files1),
                                                                    sstables_files1))
        self.assertGreater(a=len(sstables_files1), b=1, msg="More than 1 sstable is expected")
        self.assertLessEqual(a=max_found_file_size, b=max_expected_file_size,
                             msg="Maximum file size exceeds expected limit of {}: {}".format(max_expected_file_size,
                                                                                             max_found_file_size))

    def lcs_major_compaction_then_ics_major_compaction_test(self):
        """
        Check number and size of incremental compaction strategy sstables after generating load and running a major compaction via nodetool.
        """
        sstable_size_in_mb = 2
        self.prepare(num_of_nodes=1, r_factor=1,
                     compaction_strategy=CompactionStrategy.LEVELED,
                     sstable_size_in_mb=sstable_size_in_mb)
        node1 = self.cluster.nodelist()[0]
        self._write_and_flush_sstables(num_of_generated_sstables=3, start_index=1,
                                       write_range=3, num_writes_per_sstable=3)
        node1.compact()
        self.alter_table_compaction(compaction_strategy=CompactionStrategy.INCREMENTAL, assert_altered_compaction=True)
        node1.compact()

    def sstable_files_validations_with_ics_compaction_test(self):
        """
        Check number and size of incremental compaction strategy sstables after generating load and running a major compaction via nodetool.
        """
        sstable_size_in_mb = 10
        self.prepare(num_of_nodes=NUM_OF_NODES, r_factor=RF,
                     compaction_strategy=CompactionStrategy.INCREMENTAL,
                     sstable_size_in_mb=sstable_size_in_mb)
        node1 = self.cluster.nodelist()[0]
        num_of_generated_sstables = 3
        self._write_and_flush_sstables(num_of_generated_sstables=num_of_generated_sstables, start_index=1,
                                       write_range=10, num_writes_per_sstable=10)
        sstables_files1, files_size = self._get_sstable_files_and_sizes()
        self._check_sstable_file_size_limit(list_sstable_files=sstables_files1, sstable_size_in_mb=sstable_size_in_mb)
        node1.compact()
        sstables_files2, files_size2 = self._get_sstable_files_and_sizes()
        table = ".".join([KEYSPACE_NAME, TABLE_NAME])
        assert len(
            sstables_files2) >= num_of_generated_sstables, "Less than {num_of_generated_sstables} SSTable files found for {table} after ICS compaction!".format(
            **locals())
        self._check_sstable_file_size_limit(list_sstable_files=sstables_files2, sstable_size_in_mb=sstable_size_in_mb)

    def ics_sstables_basic_large_partitions_test(self):
        """
                Add new keys on large-partitions-table for cluster nodes.
        """
        test_session = self.prepare(is_large_partitions=True, compaction_strategy=CompactionStrategy.INCREMENTAL)

        # Prefill
        self.insert_large_partitions_table_data(session=test_session, partition_range_end=PARTITIONS,
                                                rows_in_partition=ROWS_IN_PARTITION)

        big_partition = PARTITIONS + 1
        debug('Create partition where pk = {} with {} rows'.format(big_partition, BIG_PARTITION_ROWS))
        self.insert_large_partitions_table_data(session=test_session, partition_range_start=big_partition,
                                                partition_range_end=big_partition,
                                                rows_in_partition=BIG_PARTITION_ROWS)

        total_rows = PARTITIONS * ROWS_IN_PARTITION + BIG_PARTITION_ROWS

        # Test adding new rows

        node1 = self.cluster.nodelist()[0]
        self.cluster.flush()

        debug("Inserting new data to nodes...")
        session = self.patient_cql_connection(node1)
        session.set_keyspace(KEYSPACE_NAME)
        num_of_new_rows = 50
        num_of_flushes = 5
        num_of_new_rows_per_flush = num_of_new_rows // num_of_flushes
        current_row_index = 1
        stmts = []
        debug("Going to generate {} CQL inserts, for table {}".format(
            num_of_new_rows, TABLE_NAME))
        for flush in range(num_of_flushes):
            for i in range(current_row_index, current_row_index + num_of_new_rows_per_flush):
                debug("#{} cmd - ".format(i))
                stmt = 'insert into {table_name} (pk, ck) values ({pk}, {ck})'.format(table_name=TABLE_NAME,
                                                                                      pk=big_partition + i,
                                                                                      ck=random.randint(1,
                                                                                                        ROWS_IN_PARTITION))
                stmts.append(stmt)

            for stmt in stmts:
                session.execute(stmt)
            self.cluster.flush()
            current_row_index += num_of_new_rows_per_flush

        total_rows += num_of_new_rows
        self.cluster.flush()
        assert_row_count(session=session, table_name=FULL_TABLE_NAME, expected=total_rows)
