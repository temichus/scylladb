from collections import defaultdict
from assertions import assert_equal_more_with_deviation
from dtest import Tester, debug
import datetime
import random
import os

status_messages = (
    "I''m going to the Cassandra Summit in June!",
    "C* is awesome!",
    "All your sstables are belong to us.",
    "Just turned on another 50 C* nodes at <insert tech startup here>, scales beautifully.",
    "Oh, look! Cats, on reddit!",
    "Netflix recommendations are really good, wonder why?",
    "Spotify playlists are always giving me good tunes, wonder why?"
)

clients = (
    "Android",
    "iThing",
    "Chromium",
    "Mozilla",
    "Emacs"
)


class TestWideRows(Tester):
    _multiprocess_can_split_ = False
    BLOB_SIZE_10k = 1024 * 10

    def __init__(self, *args, **kwargs):
        Tester.__init__(self, *args, **kwargs)


    def prepare_cluster(self, nodes=1, version=None, keyspace_name='wide_rows', rf=1, options_dict=None):
        debug('Start cluster with %d nodes' % nodes)
        cluster = self.cluster
        if version:
            self.cluster.set_install_dir(version=version)
        if options_dict:
            cluster.set_configuration_options(values=options_dict)
        cluster.populate(nodes).start()
        node1 = cluster.nodelist()[0]

        session = self.patient_cql_connection(node1)
        debug('Create %s keyspace' % keyspace_name)
        self.create_ks(session=session, name=keyspace_name, rf=rf)
        return session

    def test_wide_rows(self):
        self.write_wide_rows()

    def write_wide_rows(self, version=None):
        session = self.prepare_cluster(version=version,
                                       options_dict={'compaction_large_partition_warning_threshold_mb': 1, 'compaction_large_row_warning_threshold_mb': 1})
        # Simple timeline:  user -> {date: value, ...}
        debug('Create Table....')
        session.execute('CREATE TABLE user_events (userid text, event timestamp, value text, '
                        'PRIMARY KEY (userid, event));')
        date = datetime.datetime.now()
        # Create a large timeline for each of a group of users:
        for user in ('ryan', 'cathy', 'mallen', 'joaquin', 'erin', 'ham'):
            debug("Writing values for: %s" % user)
            for day in xrange(5000):
                date_str = (date + datetime.timedelta(day)).strftime("%Y-%m-%d")
                client = random.choice(clients)
                msg = random.choice(status_messages)
                query = "UPDATE user_events SET value = '{msg:%s, client:%s}' WHERE userid='%s' and event='%s';" \
                        % (msg, client, user, date_str)
                # debug(query)
                session.execute(query)

        # debug('Duration of test: %s' % (datetime.datetime.now() - start_time))

        # Pick out an update for a specific date:
        query = "SELECT value FROM user_events WHERE userid='ryan' and event='%s'" % \
                (date + datetime.timedelta(10)).strftime("%Y-%m-%d")
        rows = session.execute(query)
        for value in rows:
            debug(value)
            assert len(value[0]) > 0

    def test_column_index_stress(self):
        """Write a large number of columns to a single row and set
        'column_index_size_in_kb' to a sufficiently low value to force
        the creation of a column index. The test will then randomly
        read columns from that row and ensure that all data is
        returned. See CASSANDRA-5225.
        """
        session = self.prepare_cluster(options_dict={'column_index_size_in_kb': 1}) # reduce column_index_size_in_kb
                                                                                # value to force column index creation
        create_table_query = 'CREATE TABLE test_table (row varchar, name varchar, value int, PRIMARY KEY (row, name));'
        session.execute(create_table_query)

        # Now insert 100,000 columns to row 'row0'
        insert_column_query = "UPDATE test_table SET value = {value} WHERE row = '{row}' AND name = '{name}';"
        for i in range(100000):
            row = 'row0'
            name = 'val' + str(i)
            session.execute(insert_column_query.format(value=i, row=row, name=name))

        # now randomly fetch columns: 1 to 3 at a time
        for i in range(10000):
            select_column_query = "SELECT value FROM test_table WHERE row='row0' AND name in ('{name1}', '{name2}', " \
                                  "'{name3}');"
            values2fetch = [str(random.randint(0, 99999)) for i in range(3)]
            # values2fetch is a list of random values.  Because they are random, they will not be unique necessarily.
            # To simplify the template logic in the select_column_query I will not expect the query to
            # necessarily return 3 values.  Hence I am computing the number of unique values in values2fetch
            # and using that in the assert at the end.
            expected_rows = len(set(values2fetch))
            rows = list(session.execute(select_column_query.format(name1="val" + values2fetch[0],
                                                                   name2="val" + values2fetch[1],
                                                                   name3="val" + values2fetch[2])))
            assert len(rows) == expected_rows

    def test_large_row_detector_with_node_stop(self):
        """
        Create table with one large row when one node is stopped and validate that row is reported in the
        system.large_rows table and there are warning in the log
        """
        keyspace_name = 'wide_row'
        table_name = 'user_events'
        nodes = 4
        rf = 3
        rows_number = 70
        session = self.prepare_cluster(nodes=nodes, rf=rf,
                                       options_dict={'compaction_large_row_warning_threshold_mb': 1},
                                       keyspace_name=keyspace_name)
        node2 = self.cluster.nodelist()[1]
        debug('Stop {}'.format(node2.name))
        node2.stop(wait_other_notice=True)

        expected_rows_data_size = self.create_and_prefill_large_rows(session=session,
                                                                     table_name=table_name,
                                                                     columns_num=200,
                                                                     rows_num=rows_number,
                                                                     one_blob_size=self.BLOB_SIZE_10k,
                                                                     start_row_index=0)
        self._detector_after_node_stop(node=node2, keyspace_name=keyspace_name, table_name=table_name, rf=rf,
                                       entity_type='row', expected_entity_num=rows_number,
                                       expected_entity_data_size = expected_rows_data_size)

    def test_large_partition_detector_with_node_stop(self):
        """
        Create table with one large row when one node is stopped and validate that row is reported in the
        system.large_rows table and there are warning in the log
        """
        keyspace_name = 'wide_row'
        table_name = 'user_events'
        nodes = 4
        rf = 3
        session = self.prepare_cluster(nodes=nodes, rf=rf,
                                       options_dict={'compaction_large_partition_warning_threshold_mb': 1},
                                       keyspace_name=keyspace_name)
        node2 = self.cluster.nodelist()[1]
        debug('Stop {}'.format(node2.name))
        node2.stop(wait_other_notice=True)

        expected_partition_data_size = self.create_and_prefill_large_partitions(session=session,
                                                                                table_name=table_name,
                                                                                partition_rows=60000,
                                                                                partitions_num=1,
                                                                                start_partition_index=0)
        self._detector_after_node_stop(node=node2, keyspace_name=keyspace_name, table_name=table_name, rf=rf,
                                       expected_entity_num=1, entity_type='partition',
                                       expected_entity_data_size=expected_partition_data_size)

    def _detector_after_node_stop(self, node, keyspace_name, table_name, rf, entity_type, expected_entity_num,
                                  expected_entity_data_size):
        self.cluster.flush()

        self.validate_data_size(entity_type=entity_type, rf=rf, keyspace_name=keyspace_name, table_name=table_name,
                        expected_entity_data_size=expected_entity_data_size, expected_entity_num=expected_entity_num)

        debug('Start {}'.format(node.name))
        node.start(wait_other_notice=True, wait_for_binary_proto=True)
        self.cluster.flush()

        self.validate_data_size(entity_type=entity_type, rf=rf, keyspace_name=keyspace_name, table_name=table_name,
                        expected_entity_data_size=expected_entity_data_size, expected_entity_num=expected_entity_num)

    def test_large_partition_detector_multipartition(self):
        self._large_partition_detector(nodes=4, rf=3, partition_rows=60000, partition_num=10,
                                       compaction_large_partition_warning_threshold_mb=4,
                                       add_small_partitions=0)

    def test_large_partition_detector(self):
        self._large_partition_detector(nodes=4, rf=3, partition_rows=60000, partition_num=1,
                                       compaction_large_partition_warning_threshold_mb=4,
                                       add_small_partitions=10)

    def _large_partition_detector(self, nodes, rf, partition_rows, partition_num,
                                  compaction_large_partition_warning_threshold_mb,
                                  add_small_partitions):
        """
        Create table with one large partition and validate that partition is reported in the system.large_partitions
        table and there are warning in the log
        """
        keyspace_name = 'wide_row'
        table_name = 'user_events'

        session = self.prepare_cluster(nodes=nodes, rf=rf,
                                       options_dict={'compaction_large_partition_warning_threshold_mb':
                                                         compaction_large_partition_warning_threshold_mb},
                                       keyspace_name=keyspace_name)
        pk_max_index = None
        expected_partition_data_size = self.run_func_with_flush(lambda: self.create_and_prefill_large_partitions
                                                                        (session=session, table_name=table_name,
                                                                         partition_rows=partition_rows,
                                                                         partitions_num=partition_num,
                                                                         start_partition_index=0))
        if add_small_partitions:
            pk_max_index = partition_num - 1
            self.run_func_with_flush(lambda: self.create_and_prefill_large_partitions
                                    (session=session, table_name=table_name,
                                     partition_rows=4000,
                                     partitions_num=add_small_partitions,
                                     start_partition_index=partition_num+1))

        self.validate_data_size(entity_type='partition', rf=rf, keyspace_name=keyspace_name, table_name=table_name,
                                expected_entity_num=partition_num,
                                expected_entity_data_size=expected_partition_data_size,
                                pk_max_index=pk_max_index)

    def test_large_row_detector(self):
        self._large_row_detector(nodes=3, rf=3, rows_num=65, columns_num=200,
                                 compaction_large_row_warning_threshold_mb=1,
                                 add_small_rows=0)

    def test_large_row_detector_with_small(self):
        self._large_row_detector(nodes=3, rf=3, rows_num=1, columns_num=200,
                                 compaction_large_row_warning_threshold_mb=1,
                                 add_small_rows=10)

    def _large_row_detector(self, nodes, rf, columns_num, rows_num, compaction_large_row_warning_threshold_mb,
                            add_small_rows):
        """
        Create table with one large row and validate that row is reported in the system.large_rows
        table and there are warning in the log
        """
        keyspace_name = 'wide_row'
        table_name = 'user_events'

        session = self.prepare_cluster(nodes=nodes, rf=rf,
                                       options_dict={'compaction_large_row_warning_threshold_mb':
                                                         compaction_large_row_warning_threshold_mb},
                                       keyspace_name=keyspace_name)
        maximum_primary_key_value = None
        expected_rows_data_size = self.run_func_with_flush(lambda: self.create_and_prefill_large_rows(session=session,
                                                                                       table_name=table_name,
                                                                                       columns_num=columns_num,
                                                                                       rows_num=rows_num,
                                                                                       one_blob_size=self.BLOB_SIZE_10k,
                                                                                       start_row_index=0))
        if add_small_rows:
            maximum_primary_key_value = rows_num - 1
            self.run_func_with_flush(lambda: self.create_and_prefill_large_rows
                                                            (session=session,
                                                            table_name=table_name,
                                                            one_blob_size=10,
                                                            columns_num=columns_num,
                                                            rows_num=add_small_rows,
                                                            start_row_index=rows_num))

        self.validate_data_size(entity_type='row', rf=rf, keyspace_name=keyspace_name, table_name=table_name,
                                expected_entity_data_size=expected_rows_data_size, expected_entity_num=rows_num,
                                pk_max_index=maximum_primary_key_value)

    def test_large_partition_detector_small_partition(self):
        """
        Create table with one small partition and validate that partition isn't reported in the system.large_partitions
        table and there are no warning in the log
        """
        keyspace_name = 'wide_row'
        table_name = 'user_events'

        session = self.prepare_cluster(keyspace_name=keyspace_name)
        self.run_func_with_flush(lambda: self.create_and_prefill_large_partitions(session=session, table_name=table_name,
                                                                                  partition_rows=600,
                                                                                  partitions_num=1,
                                                                                  start_partition_index=0))

        self.validation_small_entity(type='partition', keyspace_name=keyspace_name, table_name=table_name)

    def test_large_row_detector_small_row(self):
        """
        Create table with one small row and validate that row isn't reported in the system.large_rows
        table and there are no warning in the log
        """
        keyspace_name = 'wide_row'
        table_name = 'user_events'
        session = self.prepare_cluster(keyspace_name=keyspace_name)
        self.run_func_with_flush(lambda: self.create_and_prefill_large_rows(session=session,
                                                                            table_name=table_name,
                                                                            columns_num=10,
                                                                            rows_num=1,
                                                                            one_blob_size=self.BLOB_SIZE_10k,
                                                                            start_row_index=0
                                                                            ))

        self.validation_small_entity(type='row', keyspace_name=keyspace_name, table_name=table_name)

    def validation_small_entity(self, type, keyspace_name, table_name):
        """
        :param type: expected "partition" or "row"
        """
        for node in self.cluster.nodelist():
            session = self.patient_exclusive_cql_connection(node=node, keyspace=keyspace_name)
            system_data_size_dict = self.get_large_entity_info(session=session, keyspace_name=keyspace_name,
                                                               table_name=table_name, entity_type=type,
                                                               expect_system_report=False)

            # Search warning in the log
            self.search_warning(node=node,
                                warning_text='Writing large {type} {keyspace_name}/{table_name}'.format(**locals()),
                                expect_warning=False)

    def run_func_with_flush(self, func):
        res = func()
        self.cluster.flush()
        return res

    def create_and_prefill_large_partitions(self, session, table_name, partition_rows, partitions_num,
                                            start_partition_index):
        debug('Create table {} with large partition'.format(table_name))
        one_blob_size = 1024  # 1K value in the blob column
        create_table_query = 'CREATE TABLE IF NOT EXISTS %s (userid text, event timestamp, value blob, ' \
                             'PRIMARY KEY (userid, event)) with compression = { }' % table_name
        session.execute(create_table_query)

        date = datetime.datetime.now()
        debug('Prefill table {} with {} partitions'.format(table_name, partitions_num))
        for k in xrange(start_partition_index, start_partition_index+partitions_num):
            user = 'user%d' % k
            for i in range(partition_rows):
                date_str = (date + datetime.timedelta(i)).strftime("%Y-%m-%d")
                value = 'a' * one_blob_size  # 1K value in the blob column
                session.execute("UPDATE %s SET value = textAsBlob('%s') WHERE userid='%s' and event='%s'" \
                                % (table_name, value, user, date_str))
        return (one_blob_size+8+8) * partition_rows # aproximately partition size

    def create_and_prefill_large_rows(self, session, table_name, rows_num, columns_num,
                                      one_blob_size, start_row_index):
        debug('Create table {} with large rows'.format(table_name))
        # one_blob_size = 1024 * 10 # 10K value in the one column
        long_text_columns = ', '.join(['value%d blob' % i for i in xrange(columns_num)])
        create_table_query = 'CREATE TABLE IF NOT EXISTS %s (userid text, event timestamp, %s, ' \
                             'PRIMARY KEY (userid, event)) with compression = { }' % (table_name, long_text_columns)
        session.execute(create_table_query)

        date = datetime.datetime.now()
        debug('Prefill table {} with {} rows'.format(table_name, rows_num))
        for k in xrange(start_row_index, start_row_index+rows_num):
            user = 'user%d' % k
            value = 'a' * one_blob_size # 10K value in the one column
            event = (date + datetime.timedelta(k)).strftime("%Y-%m-%d")
            for i in xrange(columns_num):
                out = session.execute(
                    "UPDATE {table_name} SET value{i} = textAsBlob('{value}') WHERE userid='{user}' and event='{event}'".format(
                        **locals()))

        return columns_num*one_blob_size # aproximately row size

    def search_warning(self, node, warning_text, expect_warning=True):
        res = node.grep_log(expr=warning_text)
        if expect_warning:
            self.assertTrue(res, msg='Expected warning {} is not found in the log'.format(warning_text))
        else:
            self.assertFalse(res, msg='Non expect warning {} was found in the log'.format(warning_text))

    def validate_data_size(self, entity_type, rf, keyspace_name, table_name, expected_entity_num,
                           expected_entity_data_size, pk_max_index=None, expect_warning=True):
        """
        :param entity_type: expected "partition" or "row"
        """
        data_replica_count = 0
        # In case one of the nodes is down, it's expected that the data will be found on (rf-1) nodes
        alternative_rf = rf
        debug('Run full compaction')
        self.cluster.nodetool('compact {} {}'.format(keyspace_name, table_name))

        actual_entities = set()
        for node in self.cluster.nodelist():
            if node.status == 'DOWN':
                alternative_rf -= 1
                continue
            session = self.patient_exclusive_cql_connection(node=node, keyspace=keyspace_name)
            data_size_dict = self.get_data_size(node=node, keyspace_name=keyspace_name, table_name=table_name)

            system_data_size_dict, actual_sstables = self.get_large_entity_info(session=session,
                                                                             keyspace_name=keyspace_name,
                                                                             table_name=table_name,
                                                                             entity_type=entity_type)
            if not system_data_size_dict:
                self.search_warning(node=node,
                            warning_text='Writing large {entity_type} {keyspace_name}/{table_name}'.format(**locals()),
                            expect_warning=False)
                continue

            if pk_max_index is not None:
                if entity_type == 'partition':
                    wrong_large_entity_in_system = [key for key in system_data_size_dict.keys()
                                                if int(key.replace('user', '')) > pk_max_index]
                else:
                    wrong_large_entity_in_system = [key for key in system_data_size_dict.keys()
                                                    if int(key.split('.')[0].replace('user', '')) > pk_max_index]
                self.assertFalse(wrong_large_entity_in_system,
                                 msg='Small {entity_type}s were detected as large: {wrong_large_entity}'
                                     .format(entity_type=entity_type,
                                             wrong_large_entity='/n'.join(e for e in wrong_large_entity_in_system)))
            try:
                self.validate_data_size_per_entity(system_data_size_dict, expected_entity_data_size)
            except AssertionError as ae:
                expected_sstables = sorted(data_size_dict.keys())
                node_name = node.name
                self.assertEqual(expected_sstables, actual_sstables,
                                 msg='Expected sstables on the node {node_name}: {expected_sstables}; '
                                     'Actual sstables: {actual_sstables}'.format(**locals())
                                )
                assert False, ae

            actual_entities.update(pk_name for pk_name in system_data_size_dict.keys())

            # Search warning in the log
            self.search_warning(node=node,
                                warning_text='Writing large {entity_type} {keyspace_name}/{table_name}'.format(**locals()),
                                expect_warning=expect_warning if not expect_warning else bool(data_size_dict))

            data_replica_count += int(len(data_size_dict) > 0) # if there is data on the node - increase the counter

        actual_entity_number = len(actual_entities)
        self.assertEqual(actual_entity_number, expected_entity_num,
                         msg='Expected find {expected_entity_num} large {entity_type}s, reported in the '
                             'system.large_{entity_type}s, but there are {actual_entity_number}'.format(**locals()))

        self.assertTrue((data_replica_count>=rf or data_replica_count==alternative_rf),
                         msg='The data found on %d nodes, when it\'s expected on %d '
                             '(according to replication factor %d)' % (data_replica_count, rf, rf))

    def validate_data_size_per_entity(self, system_data_size_dict, expected_row_data_size):
        # size_threshold (in percent) is allowable deviation for row/partition size, reported by
        # system.large_row_size/system.large_partition_size
        row_size_threshold = 3
        for size in system_data_size_dict.values():
            assert_equal_more_with_deviation(size, expected_row_data_size, row_size_threshold)

    def get_large_entity_info(self, session, keyspace_name, table_name, entity_type, expect_system_report=True):
        """
        :param entity_type: expected "partition" or "row"
        """
        clustering_key = 'clustering_key, ' if entity_type == 'row' else ''
        query = 'select sstable_name, partition_key, {clustering_key}{entity_type}_size from system.large_{entity_type}s ' \
                'where keyspace_name=\'{keyspace_name}\' and table_name=\'{table_name}\''.format(**locals())
        result = list(session.execute(query))
        if not expect_system_report:
            self.assertFalse(result, 'Not expected large {entity_type} info in the system.large_{entity_type}s, '
                                     'but it found'.format(entity_type=entity_type))
            return None

        entity_info = defaultdict(int)
        sstables_set = set()
        for row in result:
            key = row[1] if len(row) == 3 else '{}.{}'.format(row[1], row[2])
            entity_info[key] += row[-1]
            sstables_set.add(row[0])
        return entity_info, sorted(list(sstables_set))

    def get_data_size(self, node, keyspace_name, table_name):
        files = node.get_sstables(keyspace_name, table_name)
        self.assertIsNotNone(files, "Data file has not found")
        data_size = {}
        for file in files:
            data_size[file] = os.path.getsize(file)
        return data_size
