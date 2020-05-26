import datetime
import time
import random
from collections import defaultdict

from nose.plugins.attrib import attr
from nose.tools import nottest

from assertions import assert_equal_more_with_deviation, assert_less_equal_lists
from dtest import Tester, debug
from scylla_tools import wait_for_view

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


@nottest
@attr('dtest-full')
class TestWideRows(Tester):
    BLOB_SIZE_10k = 1024 * 10
    BLOB_SIZE_1MB = 1024 * 1024
    KEYSPACE_NAME = 'wide_row'
    TABLE_NAME = 'user_events'

    def __init__(self, *args, **kwargs):
        Tester.__init__(self, *args, **kwargs)
        self.compaction_option = "compaction = {'class': '%s'}" % self.compaction_strategy

    def prepare_cluster(self, nodes=1, version=None, keyspace_name=KEYSPACE_NAME, rf=1, options_dict=None):
        debug('Run test with %s compaction strategy' % self.compaction_strategy)
        debug('Start cluster with %d nodes' % nodes)
        cluster = self.cluster
        if version:
            self.cluster.set_install_dir(version=version)
        if options_dict:
            cluster.set_configuration_options(values=options_dict)
        cluster.populate(nodes).start(wait_for_binary_proto=True)
        node1 = cluster.nodelist()[0]

        session = self.patient_cql_connection(node1)
        debug('Create %s keyspace' % keyspace_name)
        self.create_ks(session=session, name=keyspace_name, rf=rf)
        return session

    def validation_small_entity(self, entity_type, keyspace_name, table_name):
        """
        :param entity_type: expected "partition" or "row"
        """
        for node in self.cluster.nodelist():
            session = self.patient_exclusive_cql_connection(node=node, keyspace=keyspace_name)
            system_data_size_dict = self.get_large_entity_info(session=session, keyspace_name=keyspace_name,
                                                               table_name=table_name, entity_type=entity_type,
                                                               expect_system_report=False)

            self.assertFalse(system_data_size_dict, msg='Not expected large %s found' % entity_type)

            # Search warning in the log
            self.search_warning(node=node,
                                warning_text='Writing large {entity_type} {keyspace_name}/{table_name}'.format(**locals()),
                                marked_logs_dict={},
                                expect_warning=False)

    def create_large_partition_table(self, session, table_name):
        debug('Create table {} with large partition'.format(table_name))
        create_table_query = 'CREATE TABLE IF NOT EXISTS %s (userid text, event text, value blob, ' \
                             'PRIMARY KEY (userid, event)) with compression = { } and %s' % (table_name,
                                                                                             self.compaction_option)
        session.execute(create_table_query)

    def create_large_partition_data(self, session, table_name, partition_rows, partitions_num, one_blob_size,
                                    start_partition_index):
        expected_row_size = (one_blob_size + 8 + 8) * partition_rows  # aproximately partition size
        expected_rows = {}

        date = datetime.datetime.now()
        debug('Prefill table {} with {} partition(s), {} row(s) each'.format(table_name, partitions_num, partition_rows))
        for k in range(start_partition_index, start_partition_index+partitions_num):
            user = 'user%d' % k
            for i in range(partition_rows):
                date_str = (date + datetime.timedelta(i)).strftime("%Y-%m-%d")
                value = 'a' * one_blob_size  # 1K value in the blob column
                session.execute("UPDATE %s SET value = textAsBlob('%s') WHERE userid='%s' and event='%s'"
                                % (table_name, value, user, date_str))
                expected_rows[user] = expected_row_size
        return expected_rows

    def create_large_row_table(self, session, table_name, columns_num, entity_type='row'):
        debug('Create table {} with large {}s'.format(table_name, entity_type))
        long_text_columns = ', '.join(['value%d blob' % i for i in range(columns_num)])
        create_table_query = 'CREATE TABLE IF NOT EXISTS %s (userid text, event text, %s, ' \
                             'PRIMARY KEY (userid, event)) with compression = { } and %s' % (table_name,
                                                                                             long_text_columns,
                                                                                             self.compaction_option)
        session.execute(create_table_query)

    def create_large_row_data(self, session, table_name, rows_num, columns_num, one_blob_size, start_row_index):
        expected_rows = {}
        expected_row_size = columns_num * one_blob_size  # approximately row size

        date = datetime.datetime.now()
        debug('Prefill table {} with {} rows'.format(table_name, rows_num))
        for k in range(start_row_index, start_row_index + rows_num):
            user = 'user%d' % k
            value = 'a' * int(one_blob_size)
            event = (date + datetime.timedelta(k)).strftime("%Y-%m-%d")
            for i in range(columns_num):
                out = session.execute(
                    "UPDATE {table_name} SET value{i} = textAsBlob('{value}') WHERE userid='{user}' and event='{event}'"
                    .format(**locals()))
            expected_rows['{}.{}'.format(user, event)] = expected_row_size

        return expected_rows

    def search_warning(self, node, warning_text, marked_logs_dict, expect_warning=True):
        from_mark = marked_logs_dict.get(node.name) or 0

        try:
            res = node.watch_log_for(exprs=warning_text, from_mark=from_mark, timeout=10)
        except Exception:
            res = None

        if expect_warning:
            self.assertTrue(res, 'Expected warning {} is not found in the log of node {}'
                            .format(warning_text, node.name))
        else:
            self.assertFalse(res, 'Non expect warning {} was found in the log of node {}'
                             .format(warning_text, node.name))

    def get_cluster_system_state(self, entity_type, keyspace_name, table_name):
        cluster_state = {}
        for node in self.cluster.nodelist():
            entity_info = defaultdict(int)
            entities = set()
            sstables_set = set()
            sstables_on_disk = set()
            key_appearance = defaultdict(int)
            if node.status == 'UP':
                session = self.patient_exclusive_cql_connection(node=node, keyspace=keyspace_name)
                # Get large partition/row details from system.large_partitions/large_rows tables
                clustering_key = 'clustering_key, ' if entity_type in ('row', 'cell') else ''
                query = 'select sstable_name, partition_key, {clustering_key}{entity_type}_size ' \
                        'from system.large_{entity_type}s ' \
                        'where keyspace_name=\'{keyspace_name}\' and table_name=\'{table_name}\''.format(**locals())
                result = list(session.execute(query))

                for row in result:
                    key = row[1] if len(row) == 3 else '{}.{}'.format(row[1], row[2])
                    if entity_type == 'cell':
                        key_appearance[key] += 1
                    entities.add(key)
                    entity_info[key] += row[-1]
                    sstables_set.add(row[0])

                # Get DB files for the keyspace_name and table_nam
                files = node.get_sstables(keyspace_name, table_name)
                self.assertIsNotNone(files, "Data file has not found")

                for file in files:
                    sstables_on_disk.add(file)

            cluster_state[node.name] = {'info_from_system_table': {'partition_keys': entities,
                                                                   'partition_size': entity_info,
                                                                   'sstables': sstables_set,
                                                                   'key_appearance': key_appearance},
                                        'sstables_from_disk': sstables_on_disk,
                                        'node_status': node.status
                                        }

        return cluster_state

    def validate_entities_recognized_as_large(self, entity_type, cluster_state, expected_entity_number):
        large_primary_keys = set()
        key_appearance = 0
        for node_info in cluster_state.values():
            large_primary_keys.update(node_info['info_from_system_table']['partition_keys'])
            key_appearance = sum(node_info['info_from_system_table']['key_appearance'].values())

        if entity_type == 'cell':
            actual_large_entities = key_appearance
        else:
            actual_large_entities = len(large_primary_keys)
        self.assertEqual(actual_large_entities, expected_entity_number,
                         msg='Expected find {expected_entity_number} large {entity_type}s, reported in the '
                             'system.large_{entity_type}s, but there are {actual_large_entities}'.format(**locals()))

    def validate_entities_not_recognized_as_large(self, entity_type, cluster_state, pk_max_index):
        large_primary_keys = set()
        for node_info in cluster_state.values():
            large_primary_keys.update(node_info['info_from_system_table']['partition_keys'])

        if entity_type == 'partition':
            wrong_large_entity_in_system = [key for key in large_primary_keys
                                            if int(key.replace('user', '')) > pk_max_index]
        else:
            wrong_large_entity_in_system = [key for key in large_primary_keys
                                            if int(key.split('.')[0].replace('user', '')) > pk_max_index]
        self.assertFalse(wrong_large_entity_in_system,
                         msg='Small {entity_type}s were detected as large: {wrong_large_entity}'
                         .format(entity_type=entity_type,
                                 wrong_large_entity='/n'.join(e for e in wrong_large_entity_in_system)))

    def validate_entity_size(self, cluster_state, expected_entity_data_size, entity_type):
        # size_threshold (in percent) is allowable deviation for row/partition size, reported by
        # system.large_row_size/system.large_partition_size
        row_size_threshold = 3
        for node_info in cluster_state.values():
            for pk, size in node_info['info_from_system_table']['partition_size'].items():
                expected_size = expected_entity_data_size.get(pk)
                self.assertIsNotNone(expected_size,
                                     msg='The {entity_type} with primary key "{pk}" is not reported '
                                         'as large {entity_type}'.format(entity_type=entity_type, pk=pk))
                assert_equal_more_with_deviation(size, expected_size, row_size_threshold)

    def validate_sstables_on_disk(self, cluster_state):
        for node_name, node_info in cluster_state.items():
            if not node_info['info_from_system_table']['partition_keys'] or node_info['node_status'] != 'UP':
                continue

            sstables_from_system = sorted(list(node_info['info_from_system_table']['sstables']))
            sstables_from_disk = sorted(list(node_info['sstables_from_disk']))

            assert_less_equal_lists(sstables_from_system, sstables_from_disk,
                                    msg='Expected sstables on the node {node_name}: {sstables_from_disk}; '
                                        'Actual sstables: {sstables_from_system}'.format(**locals()))

    def validate_system_table(self, entity_type, keyspace_name, table_name, expected_entity_number,
                                    expected_entity_data_size, pk_max_index=None):
        cluster_state = self.get_cluster_system_state(entity_type=entity_type,
                                                      keyspace_name=keyspace_name, table_name=table_name)
        self.validate_entities_recognized_as_large(entity_type=entity_type, cluster_state=cluster_state,
                                                   expected_entity_number=expected_entity_number)
        # In case there are small partitions/rows - verify the they didn't recognized as large
        if pk_max_index is not None:
            self.validate_entities_not_recognized_as_large(entity_type=entity_type, cluster_state=cluster_state,
                                                           pk_max_index=pk_max_index)

        self.validate_entity_size(cluster_state=cluster_state, expected_entity_data_size=expected_entity_data_size,
                                  entity_type=entity_type)
        self.validate_sstables_on_disk(cluster_state=cluster_state)
        return cluster_state

    def validate_log_warnings(self, cluster_state, entity_type, keyspace_name, table_name, marked_logs_dict=None,
                              expect_warning=True):
        for node in self.cluster.nodelist():
            # If large partition/row wasn't found - expect don't find the warnings
            current_node_info = cluster_state[node.name]
            if not current_node_info['info_from_system_table']['partition_keys']:
                self.search_warning(node=node,
                                    warning_text='Writing large {entity_type} {keyspace_name}/{table_name}'.format(
                                        **locals()),
                                    marked_logs_dict=marked_logs_dict or {},
                                    expect_warning=False)
                continue

            # Search warning in the log
            expect_warning = expect_warning if not expect_warning else bool(current_node_info['sstables_from_disk'])
            self.search_warning(node=node,
                                warning_text='Writing large {entity_type} {keyspace_name}/{table_name}'.format(
                                    **locals()),
                                marked_logs_dict=marked_logs_dict or {},
                                expect_warning=expect_warning)

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

    def set_ttl_on_few_rows_in_partition(self, session, keyspace_name, table_name, partition_num,
                                         expected_partitions, ttl_rows_amount):
        userid = 'user%d' % random.randint(0, partition_num-1)
        cks_for_ttl = list(session.execute("SELECT event FROM {table_name} WHERE userid='{userid}'"
                                           .format(**locals())))

        one_blob_size = 1024
        value = 'b' * one_blob_size
        ttl = 60
        # TTL part of rows in partition or full partition
        debug('Update %d rows of partition where PK "%s" with TTL %d' % (ttl_rows_amount, userid, ttl))
        for i, event_row in enumerate(cks_for_ttl):
            if i < ttl_rows_amount:
                event = event_row[0]
                session.execute("UPDATE {table_name} USING TTL {ttl} SET value = textAsBlob('{value}') "
                                "WHERE userid='{userid}' and event='{event}'".format(**locals()))

        self.cluster.flush()
        debug('Wait %d sec while the TTLed rows expiration' % ttl)
        time.sleep(ttl + 5)
        self.cluster.nodetool('compact {} {}'.format(self.KEYSPACE_NAME, self.TABLE_NAME))
        expected_partitions.pop(userid)

        return expected_partitions

    def set_ttl_on_few_large_rows(self, session, keyspace_name, table_name, rows_num, columns_num,
                                  expected_rows):
        userid = 'user%d' % random.randint(0, rows_num - 1)
        ck_for_ttl = list(session.execute("SELECT event FROM {table_name} WHERE userid='{userid}' LIMIT 1"
                                          .format(**locals())))

        value = 'b' * self.BLOB_SIZE_10k
        ttl = 60
        # TTL one row
        event = ck_for_ttl[0][0]
        columns = ', '.join(["value%d = textAsBlob('%s')" % (i, value) for i in range(columns_num)])

        debug('Update row where USERID="%s" and EVENT="%s" with TTL %d' % (userid, event, ttl))

        session.execute("UPDATE {table_name} USING TTL {ttl} SET {columns} "
                        "WHERE userid='{userid}' and event='{event}'".format(**locals()))

        self.cluster.flush()
        debug('Wait %d sec while the TTLed rows expiration' % ttl)
        time.sleep(ttl + 5)
        self.cluster.nodetool('compact {} {}'.format(self.KEYSPACE_NAME, self.TABLE_NAME))
        expected_rows.pop('{}.{}'.format(userid, event))
        return expected_rows

    def mark_log_on_all_nodes(self):
        mark_log_by_node = {}
        for node in self.cluster.nodelist():
            mark_log_by_node[node.name] = node.mark_log()
        return mark_log_by_node

    def trigger_compaction_by_data_write_and_flush(self, session, entity_type, index):
        row_number = 2
        size = 1

        if entity_type == 'partition':
            func = self.create_large_partition_data
        else:
            func = self.create_large_row_data

        for i in range(5):
            func(session, self.TABLE_NAME, row_number, 1, size, index)
            self.cluster.flush()
            time.sleep(0.5)
        self.cluster.compact();
        return row_number

    @attr('next-gating', 'dtest-debug', 'single_node')
    def test_wide_rows(self):
        self.write_wide_rows()

    def write_wide_rows(self, version=None):
        session = self.prepare_cluster(version=version,
                                       options_dict={'compaction_large_partition_warning_threshold_mb': 1,
                                                     'compaction_large_row_warning_threshold_mb': 1})
        # Simple timeline:  user -> {date: value, ...}
        debug('Create Table....')
        session.execute('CREATE TABLE user_events (userid text, event timestamp, value text, '
                        'PRIMARY KEY (userid, event)) WITH %s' % self.compaction_option)
        date = datetime.datetime.now()
        # Create a large timeline for each of a group of users:
        for user in ('ryan', 'cathy', 'mallen', 'joaquin', 'erin', 'ham'):
            debug("Writing values for: %s" % user)
            for day in range(5000):
                date_str = (date + datetime.timedelta(day)).strftime("%Y-%m-%d")
                client = random.choice(clients)
                msg = random.choice(status_messages)
                query = "UPDATE user_events SET value = '{msg:%s, client:%s}' WHERE userid='%s' and event='%s';" \
                        % (msg, client, user, date_str)
                # debug(query)
                session.execute(query)

        # Pick out an update for a specific date:
        query = "SELECT value FROM user_events WHERE userid='ryan' and event='%s'" % \
                (date + datetime.timedelta(10)).strftime("%Y-%m-%d")
        rows = session.execute(query)
        for value in rows:
            debug(value)
            assert len(value[0]) > 0

    @attr('single_node')
    def test_column_index_stress(self):
        """Write a large number of columns to a single row and set
        'column_index_size_in_kb' to a sufficiently low value to force
        the creation of a column index. The test will then randomly
        read columns from that row and ensure that all data is
        returned. See CASSANDRA-5225.
        """
        session = self.prepare_cluster(options_dict={'column_index_size_in_kb': 1})  # reduce column_index_size_in_kb
        # value to force column index creation
        create_table_query = 'CREATE TABLE test_table (row varchar, name varchar, value int, PRIMARY KEY (row, name)) '\
                             'WITH %s' % self.compaction_option
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

    def test_large_partition_detector_with_node_stop(self):
        """
        Create table with one large row when one node is stopped and validate that row is reported in the
        system.large_rows table and there are warning in the log
        """
        entity_type = 'partition'
        partition_num = 1
        extra_partitions = 0

        session = self.prepare_cluster(nodes=4, rf=3,
                                       options_dict={'compaction_large_partition_warning_threshold_mb': 1})

        node2 = self.cluster.nodelist()[1]
        debug('Stop {}'.format(node2.name))
        node2.stop(wait_other_notice=True)

        self.create_large_partition_table(session=session, table_name=self.TABLE_NAME)
        expected_partition_data_size = self.create_large_partition_data(session=session,
                                                                        table_name=self.TABLE_NAME,
                                                                        partition_rows=60000,
                                                                        partitions_num=partition_num,
                                                                        one_blob_size=1024,
                                                                        start_partition_index=0)

        self.cluster.flush()

        extra_partitions += self.trigger_compaction_by_data_write_and_flush(session, entity_type, partition_num)

        cluster_state = self.validate_system_table(entity_type=entity_type, keyspace_name=self.KEYSPACE_NAME,
                                                   table_name=self.TABLE_NAME,
                                                   expected_entity_number=partition_num,
                                                   expected_entity_data_size=expected_partition_data_size)
        self.validate_log_warnings(cluster_state=cluster_state, entity_type=entity_type,
                                   keyspace_name=self.KEYSPACE_NAME,
                                   table_name=self.TABLE_NAME)

        debug('Start {}'.format(node2.name))
        node2.start(wait_other_notice=True, wait_for_binary_proto=True)

        self.cluster.flush()

        extra_partitions += self.trigger_compaction_by_data_write_and_flush(session, entity_type, partition_num +
                                                                            extra_partitions)

        cluster_state = self.validate_system_table(entity_type=entity_type, keyspace_name=self.KEYSPACE_NAME,
                                                   table_name=self.TABLE_NAME,
                                                   expected_entity_number=partition_num,
                                                   expected_entity_data_size=expected_partition_data_size)
        self.validate_log_warnings(cluster_state=cluster_state, entity_type=entity_type,
                                   keyspace_name=self.KEYSPACE_NAME,
                                   table_name=self.TABLE_NAME)

    def test_large_partition_detector_multipartition(self):
        """
        Create table with 10 large partition and validate that partition is reported in the system.large_partitions
        table and there are warning in the log
        """
        partition_rows = 15000
        partition_num = 5
        entity_type = 'partition'

        session = self.prepare_cluster(nodes=4, rf=3,
                                       options_dict={'compaction_large_partition_warning_threshold_mb': 1})

        pk_max_index = None
        self.create_large_partition_table(session=session, table_name=self.TABLE_NAME)
        expected_partition_data_size = self.create_large_partition_data(session=session,
                                                                        table_name=self.TABLE_NAME,
                                                                        partition_rows=partition_rows,
                                                                        partitions_num=partition_num,
                                                                        one_blob_size=1024,
                                                                        start_partition_index=0)
        self.cluster.flush()

        self.trigger_compaction_by_data_write_and_flush(session, entity_type, partition_num)

        cluster_state = self.validate_system_table(entity_type=entity_type, keyspace_name=self.KEYSPACE_NAME,
                                                   table_name=self.TABLE_NAME,
                                                   expected_entity_number=partition_num,
                                                   expected_entity_data_size=expected_partition_data_size,
                                                   pk_max_index=pk_max_index)
        self.validate_log_warnings(cluster_state=cluster_state, entity_type=entity_type,
                                   keyspace_name=self.KEYSPACE_NAME,
                                   table_name=self.TABLE_NAME)

    def test_large_partition_detector_with_small_partitions(self):
        """
        Create table with one large partition and one small partition and validate that partition is reported in
        the system.large_partitions table and there are warning in the log
        """
        partition_rows = 60000
        partition_num = 1
        small_partition_num = 10
        entity_type = 'partition'

        session = self.prepare_cluster(nodes=4, rf=3,
                                       options_dict={'compaction_large_partition_warning_threshold_mb': 4})

        self.create_large_partition_table(session=session, table_name=self.TABLE_NAME)
        expected_partition_data_size = self.create_large_partition_data(session=session,
                                                                        table_name=self.TABLE_NAME,
                                                                        partition_rows=partition_rows,
                                                                        partitions_num=partition_num,
                                                                        one_blob_size=1024,
                                                                        start_partition_index=0)

        pk_max_index = partition_num - 1
        self.create_large_partition_data(session=session,
                                         table_name=self.TABLE_NAME,
                                         partition_rows=3000,
                                         partitions_num=small_partition_num,
                                         one_blob_size=1024,
                                         start_partition_index=partition_num+1)
        self.cluster.flush()

        self.trigger_compaction_by_data_write_and_flush(session, entity_type, partition_num + small_partition_num)

        cluster_state = self.validate_system_table(entity_type=entity_type, keyspace_name=self.KEYSPACE_NAME,
                                                   table_name=self.TABLE_NAME,
                                                   expected_entity_number=partition_num,
                                                   expected_entity_data_size=expected_partition_data_size,
                                                   pk_max_index=pk_max_index)
        self.validate_log_warnings(cluster_state=cluster_state, entity_type=entity_type,
                                   keyspace_name=self.KEYSPACE_NAME,
                                   table_name=self.TABLE_NAME)

    def test_large_partition_detector_with_ttl_on_few_row_in_partition(self):
        """
        Validate that when most of rows in the large partition are expired, this partition is not reported in
        the system.large_partitions table and there are no warning in the log
        """
        partition_rows = 60000
        partition_num = 1
        extra_partitions = 0
        entity_type = 'partition'

        session = self.prepare_cluster(nodes=4, rf=3,
                                       options_dict={'compaction_large_partition_warning_threshold_mb': 4})

        self.create_large_partition_table(session=session, table_name=self.TABLE_NAME)
        expected_partition_data_size = self.create_large_partition_data(session=session,
                                                                        table_name=self.TABLE_NAME,
                                                                        partition_rows=partition_rows,
                                                                        partitions_num=partition_num,
                                                                        one_blob_size=1024,
                                                                        start_partition_index=0)

        self.cluster.flush()

        extra_partitions += self.trigger_compaction_by_data_write_and_flush(session, entity_type, partition_num)

        cluster_state = self.validate_system_table(entity_type=entity_type, keyspace_name=self.KEYSPACE_NAME,
                                                   table_name=self.TABLE_NAME,
                                                   expected_entity_number=partition_num,
                                                   expected_entity_data_size=expected_partition_data_size)
        self.validate_log_warnings(cluster_state=cluster_state, entity_type=entity_type,
                                   keyspace_name=self.KEYSPACE_NAME,
                                   table_name=self.TABLE_NAME)

        expected_partitions = self.set_ttl_on_few_rows_in_partition(session=session,
                                                                    keyspace_name=self.KEYSPACE_NAME,
                                                                    table_name=self.TABLE_NAME,
                                                                    partition_num=partition_num,
                                                                    expected_partitions=expected_partition_data_size,
                                                                    ttl_rows_amount=partition_rows-1000)
        extra_partitions += self.trigger_compaction_by_data_write_and_flush(session, entity_type, partition_num + 
                                                                           extra_partitions)

        mark_logs = self.mark_log_on_all_nodes()

        cluster_state = self.validate_system_table(entity_type=entity_type, keyspace_name=self.KEYSPACE_NAME,
                                                   table_name=self.TABLE_NAME,
                                                   expected_entity_number=partition_num-1,
                                                   expected_entity_data_size=expected_partitions)
        self.validate_log_warnings(cluster_state=cluster_state, entity_type=entity_type,
                                   keyspace_name=self.KEYSPACE_NAME,
                                   table_name=self.TABLE_NAME,
                                   marked_logs_dict=mark_logs)

    def test_large_partition_detector_with_ttl_on_partition(self):
        """
        Validate that when all rows in the large partition are expired, this partition is not reported in
        the system.large_partitions table and there are no warning in the log
        """
        partition_rows = 60000
        partition_num = 1
        extra_partitions = 0
        entity_type = 'partition'

        session = self.prepare_cluster(nodes=4, rf=3,
                                       options_dict={'compaction_large_partition_warning_threshold_mb': 4})

        self.create_large_partition_table(session=session, table_name=self.TABLE_NAME)
        expected_partition_data_size = self.create_large_partition_data(session=session,
                                                                        table_name=self.TABLE_NAME,
                                                                        partition_rows=partition_rows,
                                                                        partitions_num=partition_num,
                                                                        one_blob_size=1024,
                                                                        start_partition_index=0)

        self.cluster.flush()

        extra_partitions += self.trigger_compaction_by_data_write_and_flush(session, entity_type, partition_num)

        cluster_state = self.validate_system_table(entity_type=entity_type, keyspace_name=self.KEYSPACE_NAME,
                                                   table_name=self.TABLE_NAME,
                                                   expected_entity_number=partition_num,
                                                   expected_entity_data_size=expected_partition_data_size)
        self.validate_log_warnings(cluster_state=cluster_state, entity_type=entity_type,
                                   keyspace_name=self.KEYSPACE_NAME,
                                   table_name=self.TABLE_NAME)

        expected_partitions = self.set_ttl_on_few_rows_in_partition(session=session,
                                                                    keyspace_name=self.KEYSPACE_NAME,
                                                                    table_name=self.TABLE_NAME,
                                                                    partition_num=partition_num,
                                                                    expected_partitions=expected_partition_data_size,
                                                                    ttl_rows_amount=partition_rows)
        extra_partitions += self.trigger_compaction_by_data_write_and_flush(session, entity_type, partition_num + 
                                                                            extra_partitions)

        mark_logs = self.mark_log_on_all_nodes()

        cluster_state = self.validate_system_table(entity_type=entity_type, keyspace_name=self.KEYSPACE_NAME,
                                                   table_name=self.TABLE_NAME,
                                                   expected_entity_number=partition_num-1,
                                                   expected_entity_data_size=expected_partitions)
        self.validate_log_warnings(cluster_state=cluster_state, entity_type=entity_type,
                                   keyspace_name=self.KEYSPACE_NAME,
                                   table_name=self.TABLE_NAME,
                                   marked_logs_dict=mark_logs)

    def test_large_row_detector_with_node_stop(self):
        """
        Create table with one large row when one node is stopped and validate that row is reported in the
        system.large_rows table and there are warning in the log
        """
        rows_number = 5
        entity_type = 'row'
        columns_num = 15
        extra_rows = 0

        session = self.prepare_cluster(nodes=2, rf=2,
                                       options_dict={'compaction_large_row_warning_threshold_mb': 1})

        node2 = self.cluster.nodelist()[1]
        debug('Stop {}'.format(node2.name))
        node2.stop(wait_other_notice=True)

        self.create_large_row_table(session=session, table_name=self.TABLE_NAME, columns_num=columns_num)
        expected_rows_data_size = self.create_large_row_data(session=session,
                                                             table_name=self.TABLE_NAME,
                                                             columns_num=columns_num,
                                                             rows_num=rows_number,
                                                             one_blob_size=self.BLOB_SIZE_10k * 10,
                                                             start_row_index=0)
        self.cluster.flush()
        extra_rows += self.trigger_compaction_by_data_write_and_flush(session, entity_type, rows_number)

        cluster_state = self.validate_system_table(entity_type=entity_type, keyspace_name=self.KEYSPACE_NAME,
                                                   table_name=self.TABLE_NAME,
                                                   expected_entity_number=rows_number,
                                                   expected_entity_data_size=expected_rows_data_size)
        self.validate_log_warnings(cluster_state=cluster_state, entity_type=entity_type,
                                   keyspace_name=self.KEYSPACE_NAME,
                                   table_name=self.TABLE_NAME)

        debug('Start {}'.format(node2.name))
        node2.start(wait_other_notice=True, wait_for_binary_proto=True)
        # self.cluster.flush()
        # extra_rows += self.trigger_compaction_by_data_write_and_flush(session, entity_type, rows_number + extra_rows)

        cluster_state = self.validate_system_table(entity_type=entity_type, keyspace_name=self.KEYSPACE_NAME,
                                                   table_name=self.TABLE_NAME,
                                                   expected_entity_number=rows_number,
                                                   expected_entity_data_size=expected_rows_data_size)
        self.validate_log_warnings(cluster_state=cluster_state, entity_type=entity_type,
                                   keyspace_name=self.KEYSPACE_NAME,
                                   table_name=self.TABLE_NAME)

    def test_simple_large_row_detector(self):
        """
        Create table with large rows. Validate that it's reported in the
        system.large_rows table and there are warning in the log
        """
        columns_num = 14
        rows_number = 5
        entity_type = 'row'

        session = self.prepare_cluster(nodes=3, rf=3,
                                       options_dict={'compaction_large_row_warning_threshold_mb': 1})

        self.create_large_row_table(session=session, table_name=self.TABLE_NAME, columns_num=columns_num)
        expected_rows_data_size = self.create_large_row_data(session=session,
                                                             table_name=self.TABLE_NAME,
                                                             columns_num=columns_num,
                                                             rows_num=rows_number,
                                                             one_blob_size=self.BLOB_SIZE_10k * 10,
                                                             start_row_index=0)

        self.cluster.flush()

        self.trigger_compaction_by_data_write_and_flush(session, entity_type, rows_number)

        cluster_state = self.validate_system_table(entity_type=entity_type, keyspace_name=self.KEYSPACE_NAME,
                                                   table_name=self.TABLE_NAME,
                                                   expected_entity_number=rows_number,
                                                   expected_entity_data_size=expected_rows_data_size)
        self.validate_log_warnings(cluster_state=cluster_state, entity_type=entity_type,
                                   keyspace_name=self.KEYSPACE_NAME,
                                   table_name=self.TABLE_NAME)

    def test_large_row_detector_with_ttl_on_row(self):
        """
        Create table with large rows. Validate that it's reported in the
        system.large_rows table and there are warning in the log
        """
        rows_number = 2
        columns_num = 20
        extra_rows = 0
        entity_type = 'row'

        session = self.prepare_cluster(nodes=3, rf=3,
                                       options_dict={'compaction_large_row_warning_threshold_mb': 1})

        self.create_large_row_table(session=session, table_name=self.TABLE_NAME, columns_num=columns_num)
        expected_rows_data_size = self.create_large_row_data(session=session,
                                                             table_name=self.TABLE_NAME,
                                                             columns_num=columns_num,
                                                             rows_num=rows_number,
                                                             one_blob_size=self.BLOB_SIZE_10k * 10,
                                                             start_row_index=0)
        self.cluster.flush()

        extra_rows += self.trigger_compaction_by_data_write_and_flush(session, entity_type, rows_number)

        cluster_state = self.validate_system_table(entity_type=entity_type, keyspace_name=self.KEYSPACE_NAME,
                                                   table_name=self.TABLE_NAME,
                                                   expected_entity_number=rows_number,
                                                   expected_entity_data_size=expected_rows_data_size)
        self.validate_log_warnings(cluster_state=cluster_state, entity_type=entity_type,
                                   keyspace_name=self.KEYSPACE_NAME,
                                   table_name=self.TABLE_NAME)

        expected_rows_data_size = self.set_ttl_on_few_large_rows(session=session,
                                                                 keyspace_name=self.KEYSPACE_NAME,
                                                                 table_name=self.TABLE_NAME,
                                                                 rows_num=rows_number,
                                                                 columns_num=columns_num,
                                                                 expected_rows=expected_rows_data_size)

        extra_rows += self.trigger_compaction_by_data_write_and_flush(session, entity_type, rows_number + extra_rows)

        mark_logs = self.mark_log_on_all_nodes()

        cluster_state = self.validate_system_table(entity_type=entity_type, keyspace_name=self.KEYSPACE_NAME,
                                                   table_name=self.TABLE_NAME,
                                                   expected_entity_number=rows_number - 1,
                                                   expected_entity_data_size=expected_rows_data_size)
        self.validate_log_warnings(cluster_state=cluster_state, entity_type=entity_type,
                                   keyspace_name=self.KEYSPACE_NAME,
                                   table_name=self.TABLE_NAME,
                                   marked_logs_dict=mark_logs,
                                   expect_warning=False)

    def test_large_row_detector_with_small_rows(self):
        """
        Create table with one large row and small rows. Validate that just large row is reported in the
        system.large_rows table and there are warning in the log
        """
        small_rows_number = 10
        rows_number = 1
        columns_num = 12
        entity_type = 'row'

        session = self.prepare_cluster(nodes=3, rf=3,
                                       options_dict={'compaction_large_row_warning_threshold_mb': 1})

        self.create_large_row_table(session=session, table_name=self.TABLE_NAME, columns_num=columns_num)
        # Insert large row
        expected_rows_data_size = self.create_large_row_data(session=session,
                                                             table_name=self.TABLE_NAME,
                                                             columns_num=columns_num,
                                                             rows_num=rows_number,
                                                             one_blob_size=self.BLOB_SIZE_10k * 10,
                                                             start_row_index=0)

        # Insert small rows
        maximum_primary_key_value = rows_number - 1
        self.create_large_row_data(session=session,
                                   table_name=self.TABLE_NAME,
                                   one_blob_size=10,
                                   columns_num=columns_num,
                                   rows_num=small_rows_number,
                                   start_row_index=rows_number)

        self.cluster.flush()

        self.trigger_compaction_by_data_write_and_flush(session, entity_type, rows_number + small_rows_number)

        cluster_state = self.validate_system_table(entity_type=entity_type, keyspace_name=self.KEYSPACE_NAME,
                                                   table_name=self.TABLE_NAME,
                                                   expected_entity_number=rows_number,
                                                   expected_entity_data_size=expected_rows_data_size,
                                                   pk_max_index=maximum_primary_key_value)
        self.validate_log_warnings(cluster_state=cluster_state, entity_type=entity_type,
                                   keyspace_name=self.KEYSPACE_NAME,
                                   table_name=self.TABLE_NAME)

    @attr('single_node')
    def test_large_partition_detector_small_partition(self):
        """
        Create table with one small partition and validate that partition isn't reported in the system.large_partitions
        table and there are no warning in the log
        """
        entity_type = 'partition'
        session = self.prepare_cluster()
        self.create_large_partition_table(session=session, table_name=self.TABLE_NAME)
        self.create_large_partition_data(session=session, table_name=self.TABLE_NAME,
                                         partition_rows=600,
                                         partitions_num=1,
                                         one_blob_size=1024,
                                         start_partition_index=0)
        self.cluster.flush()

        cluster_state = self.validate_system_table(entity_type=entity_type, keyspace_name=self.KEYSPACE_NAME,
                                                   table_name=self.TABLE_NAME,
                                                   expected_entity_number=0,
                                                   expected_entity_data_size=None)
        self.validate_log_warnings(cluster_state=cluster_state, entity_type=entity_type,
                                   keyspace_name=self.KEYSPACE_NAME,
                                   table_name=self.TABLE_NAME,
                                   expect_warning=False)

    @attr('single_node')
    def test_large_row_detector_small_row(self):
        """
        Create table with one small row and validate that row isn't reported in the system.large_rows
        table and there are no warning in the log
        """
        entity_type = 'row'
        columns_num = 10
        session = self.prepare_cluster()
        self.create_large_row_table(session=session, table_name=self.TABLE_NAME, columns_num=columns_num)
        self.create_large_row_data(session=session,
                                   table_name=self.TABLE_NAME,
                                   columns_num=columns_num,
                                   rows_num=1,
                                   one_blob_size=self.BLOB_SIZE_10k,
                                   start_row_index=0)
        self.cluster.flush()

        cluster_state = self.validate_system_table(entity_type=entity_type, keyspace_name=self.KEYSPACE_NAME,
                                                   table_name=self.TABLE_NAME,
                                                   expected_entity_number=0,
                                                   expected_entity_data_size=None)
        self.validate_log_warnings(cluster_state=cluster_state, entity_type=entity_type,
                                   keyspace_name=self.KEYSPACE_NAME,
                                   table_name=self.TABLE_NAME,
                                   expect_warning=False)

    def test_large_row_in_materialized_view(self):
        """
         Create table with one large row. Create materialized view on the table.
         Validate that just large row is reported in the system.large_rows table and there are warning in the log for
         both base table and materialized view
        """
        rows_number = 1
        columns_num = 200
        entity_type = 'row'
        view_name = '%s_view' % self.TABLE_NAME

        session = self.prepare_cluster(nodes=3, rf=3,
                                       options_dict={'compaction_large_row_warning_threshold_mb': 1})

        self.create_large_row_table(session=session, table_name=self.TABLE_NAME, columns_num=columns_num)
        # Insert large row
        expected_rows_data_size = self.create_large_row_data(session=session,
                                                             table_name=self.TABLE_NAME,
                                                             columns_num=columns_num,
                                                             rows_num=rows_number,
                                                             one_blob_size=self.BLOB_SIZE_10k,
                                                             start_row_index=0)

        session.execute('create materialized view %s as select * from %s '
                        'where userid is not null and event is not null primary key (userid, event)' %
                        (view_name, self.TABLE_NAME))
        wait_for_view(cluster=self.cluster, session=session, ks=self.KEYSPACE_NAME, view=view_name)
        self.cluster.flush()

        self.trigger_compaction_by_data_write_and_flush(session, entity_type, rows_number)

        # Validate base table
        cluster_state = self.validate_system_table(entity_type=entity_type, keyspace_name=self.KEYSPACE_NAME,
                                                   table_name=self.TABLE_NAME,
                                                   expected_entity_number=rows_number,
                                                   expected_entity_data_size=expected_rows_data_size)
        self.validate_log_warnings(cluster_state=cluster_state, entity_type=entity_type,
                                   keyspace_name=self.KEYSPACE_NAME,
                                   table_name=self.TABLE_NAME)

        # Validate view
        cluster_state = self.validate_system_table(entity_type=entity_type, keyspace_name=self.KEYSPACE_NAME,
                                                   table_name=view_name,
                                                   expected_entity_number=rows_number,
                                                   expected_entity_data_size=expected_rows_data_size)
        self.validate_log_warnings(cluster_state=cluster_state, entity_type=entity_type,
                                   keyspace_name=self.KEYSPACE_NAME,
                                   table_name=view_name)

    def test_large_partition_in_materialized_view(self):
        """
         Create table with one large partition. Create materialized view on the table.
         Validate that just large row is reported in the system.large_rows table and there are warning in the log for
         both base table and materialized view
        """
        partition_rows = 15000
        partition_num = 1
        entity_type = 'partition'
        view_name = '%s_view' % self.TABLE_NAME

        session = self.prepare_cluster(nodes=4, rf=3,
                                       options_dict={'compaction_large_partition_warning_threshold_mb': 1})

        self.create_large_partition_table(session=session, table_name=self.TABLE_NAME)
        expected_partition_data_size = self.create_large_partition_data(session=session,
                                                                        table_name=self.TABLE_NAME,
                                                                        partition_rows=partition_rows,
                                                                        partitions_num=partition_num,
                                                                        one_blob_size=1024,
                                                                        start_partition_index=0)

        session.execute('create materialized view %s as select * from %s '
                        'where userid is not null and event is not null primary key (userid, event)' %
                        (view_name, self.TABLE_NAME))
        wait_for_view(cluster=self.cluster, session=session, ks=self.KEYSPACE_NAME, view=view_name)

        self.cluster.flush()
        self.cluster.nodetool('compact {} {}'.format(self.KEYSPACE_NAME, self.TABLE_NAME))
        # self.trigger_compaction_by_data_write_and_flush(session, entity_type, partition_num)

        # Validate base table
        cluster_state = self.validate_system_table(entity_type=entity_type, keyspace_name=self.KEYSPACE_NAME,
                                                   table_name=self.TABLE_NAME,
                                                   expected_entity_number=partition_num,
                                                   expected_entity_data_size=expected_partition_data_size)
        self.validate_log_warnings(cluster_state=cluster_state, entity_type=entity_type,
                                   keyspace_name=self.KEYSPACE_NAME,
                                   table_name=self.TABLE_NAME)

        # Validate view
        cluster_state = self.validate_system_table(entity_type=entity_type, keyspace_name=self.KEYSPACE_NAME,
                                                   table_name=view_name,
                                                   expected_entity_number=partition_num,
                                                   expected_entity_data_size=expected_partition_data_size)
        self.validate_log_warnings(cluster_state=cluster_state, entity_type=entity_type,
                                   keyspace_name=self.KEYSPACE_NAME,
                                   table_name=view_name)

    def test_large_cell_detector_with_small_cells(self):
        """
        Create table with one large cell and few small cells. Validate that just large cell is reported in the
        system.large_cells table and there are warning in the log
        """
        rows_number = 10
        columns_num = 3
        entity_type = 'cell'
        session = self.prepare_cluster(nodes=3, rf=3)

        self.create_large_row_table(session=session, table_name=self.TABLE_NAME, columns_num=columns_num,
                                    entity_type=entity_type)
        # Insert large row
        expected_cells_data_size = self.create_large_row_data(session=session,
                                                              table_name=self.TABLE_NAME,
                                                              columns_num=columns_num,
                                                              rows_num=rows_number,
                                                              one_blob_size=self.BLOB_SIZE_1MB,
                                                              start_row_index=0)

        # Insert small cells
        maximum_primary_key_value = rows_number - 1
        self.create_large_row_data(session=session,
                                   table_name=self.TABLE_NAME,
                                   one_blob_size=self.BLOB_SIZE_10k,
                                   columns_num=columns_num,
                                   rows_num=rows_number,
                                   start_row_index=rows_number)

        self.cluster.flush()

        self.trigger_compaction_by_data_write_and_flush(session, entity_type, rows_number + rows_number)

        cluster_state = self.validate_system_table(entity_type=entity_type, keyspace_name=self.KEYSPACE_NAME,
                                                   table_name=self.TABLE_NAME,
                                                   expected_entity_number=columns_num * rows_number,
                                                   expected_entity_data_size=expected_cells_data_size,
                                                   pk_max_index=maximum_primary_key_value)
        self.validate_log_warnings(cluster_state=cluster_state, entity_type=entity_type,
                                   keyspace_name=self.KEYSPACE_NAME,
                                   table_name=self.TABLE_NAME)


    def test_multiple_rows_with_large_cells_detector(self):
        """
        Create table with 10 large cells. Validate that it's reported in the
        system.large_cells table and there are warning in the log
        """
        columns_num = 3
        rows_number = 10
        entity_type = 'cell'

        session = self.prepare_cluster(nodes=3, rf=3)

        self.create_large_row_table(session=session, table_name=self.TABLE_NAME, columns_num=columns_num,
                                    entity_type=entity_type)
        expected_rows_data_size = self.create_large_row_data(session=session,
                                                             table_name=self.TABLE_NAME,
                                                             rows_num=rows_number,
                                                             columns_num=columns_num,
                                                             one_blob_size=self.BLOB_SIZE_1MB,
                                                             start_row_index=0)
        self.cluster.flush()

        self.trigger_compaction_by_data_write_and_flush(session, entity_type, rows_number)

        cluster_state = self.validate_system_table(entity_type=entity_type, keyspace_name=self.KEYSPACE_NAME,
                                                   table_name=self.TABLE_NAME,
                                                   expected_entity_number=columns_num * rows_number,
                                                   expected_entity_data_size=expected_rows_data_size)
        self.validate_log_warnings(cluster_state=cluster_state, entity_type=entity_type,
                                   keyspace_name=self.KEYSPACE_NAME,
                                   table_name=self.TABLE_NAME)

    def test_multiple_columns_with_large_cells_detector(self):
        """
        Create table with 10 large cells and 5 rows (total of 50 large cell warnings). Validate that they are
        reported in the system.large_cells table and there are warning in the log
        """
        columns_num = 10
        rows_number = 5
        entity_type = 'cell'

        session = self.prepare_cluster(nodes=3, rf=3)

        self.create_large_row_table(session=session, table_name=self.TABLE_NAME, columns_num=columns_num,
                                    entity_type=entity_type)
        expected_rows_data_size = self.create_large_row_data(session=session,
                                                             table_name=self.TABLE_NAME,
                                                             rows_num=rows_number,
                                                             columns_num=columns_num,
                                                             one_blob_size=self.BLOB_SIZE_1MB,
                                                             start_row_index=0)
        self.cluster.flush()

        self.trigger_compaction_by_data_write_and_flush(session, entity_type, rows_number)

        cluster_state = self.validate_system_table(entity_type=entity_type, keyspace_name=self.KEYSPACE_NAME,
                                                   table_name=self.TABLE_NAME,
                                                   expected_entity_number=columns_num * rows_number,
                                                   expected_entity_data_size=expected_rows_data_size)
        self.validate_log_warnings(cluster_state=cluster_state, entity_type=entity_type,
                                   keyspace_name=self.KEYSPACE_NAME,
                                   table_name=self.TABLE_NAME)

    def test_multiple_rows_and_columns_with_large_cells_detector(self):
        """
        Create table with 5 large cells on 10 rows. Validate that it's reported in the
        system.large_cells table and there are warning in the log
        """
        columns_num = 5
        rows_number = 10
        entity_type = 'cell'

        session = self.prepare_cluster(nodes=3, rf=3)

        self.create_large_row_table(session=session, table_name=self.TABLE_NAME, columns_num=columns_num,
                                    entity_type=entity_type)
        expected_rows_data_size = self.create_large_row_data(session=session,
                                                             table_name=self.TABLE_NAME,
                                                             rows_num=rows_number,
                                                             columns_num=columns_num,
                                                             one_blob_size=self.BLOB_SIZE_1MB,
                                                             start_row_index=0)
        self.cluster.flush()

        self.trigger_compaction_by_data_write_and_flush(session, entity_type, rows_number)

        cluster_state = self.validate_system_table(entity_type=entity_type, keyspace_name=self.KEYSPACE_NAME,
                                                   table_name=self.TABLE_NAME,
                                                   expected_entity_number=columns_num * rows_number,
                                                   expected_entity_data_size=expected_rows_data_size)
        self.validate_log_warnings(cluster_state=cluster_state, entity_type=entity_type,
                                   keyspace_name=self.KEYSPACE_NAME,
                                   table_name=self.TABLE_NAME)

    def test_large_cell_in_materialized_view(self):
        """
         Create table with one large cell. Create materialized view on the table.
         Validate that just large cell is reported in the system.large_cells table and there are warning in the
         log for both base table and materialized view
        """
        rows_number = 10
        columns_num = 10
        entity_type = 'cell'
        view_name = '%s_view' % self.TABLE_NAME

        session = self.prepare_cluster(nodes=3, rf=3)

        self.create_large_row_table(session=session, table_name=self.TABLE_NAME, columns_num=columns_num,
                                    entity_type=entity_type)
        # Insert large row
        expected_cells_data_size = self.create_large_row_data(session=session,
                                                              table_name=self.TABLE_NAME,
                                                              columns_num=columns_num,
                                                              rows_num=rows_number,
                                                              one_blob_size=self.BLOB_SIZE_1MB,
                                                              start_row_index=0)

        session.execute('create materialized view %s as select * from %s '
                        'where userid is not null and event is not null primary key (userid, event)' %
                        (view_name, self.TABLE_NAME))
        wait_for_view(cluster=self.cluster, session=session, ks=self.KEYSPACE_NAME, view=view_name)
        self.cluster.flush()

        self.trigger_compaction_by_data_write_and_flush(session, entity_type, rows_number)

        # Validate base table
        cluster_state = self.validate_system_table(entity_type=entity_type, keyspace_name=self.KEYSPACE_NAME,
                                                   table_name=self.TABLE_NAME,
                                                   expected_entity_number=columns_num * rows_number,
                                                   expected_entity_data_size=expected_cells_data_size)
        self.validate_log_warnings(cluster_state=cluster_state, entity_type=entity_type,
                                   keyspace_name=self.KEYSPACE_NAME,
                                   table_name=self.TABLE_NAME)

        # Validate view
        cluster_state = self.validate_system_table(entity_type=entity_type, keyspace_name=self.KEYSPACE_NAME,
                                                   table_name=view_name,
                                                   expected_entity_number=columns_num * rows_number,
                                                   expected_entity_data_size=expected_cells_data_size)
        self.validate_log_warnings(cluster_state=cluster_state, entity_type=entity_type,
                                   keyspace_name=self.KEYSPACE_NAME,
                                   table_name=view_name)

    def test_large_cell_detector_with_ttl_on_row(self):
        """
        Create table with large cells. Validate that it's reported in the
        system.large_cells table and there are warning in the log.
        After that, we create more data, setting TTL to some of them and wait for the TTL timeout and then
        verify they are not reported anymore.
        """
        rows_number = 10
        columns_num = 6
        extra_rows = 0
        entity_type = 'cell'

        session = self.prepare_cluster(nodes=3, rf=3)

        self.create_large_row_table(session=session, table_name=self.TABLE_NAME, columns_num=columns_num,
                                    entity_type=entity_type)
        expected_cells_data_size = self.create_large_row_data(session=session,
                                                              table_name=self.TABLE_NAME,
                                                              columns_num=columns_num,
                                                              rows_num=rows_number,
                                                              one_blob_size=self.BLOB_SIZE_1MB,
                                                              start_row_index=0)
        self.cluster.flush()

        extra_rows += self.trigger_compaction_by_data_write_and_flush(session, entity_type, rows_number)

        cluster_state = self.validate_system_table(entity_type=entity_type, keyspace_name=self.KEYSPACE_NAME,
                                                   table_name=self.TABLE_NAME,
                                                   expected_entity_number=columns_num * rows_number,
                                                   expected_entity_data_size=expected_cells_data_size)
        self.validate_log_warnings(cluster_state=cluster_state, entity_type=entity_type,
                                   keyspace_name=self.KEYSPACE_NAME,
                                   table_name=self.TABLE_NAME)

        expected_cells_data_size = self.set_ttl_on_few_large_rows(session=session,
                                                                  keyspace_name=self.KEYSPACE_NAME,
                                                                  table_name=self.TABLE_NAME,
                                                                  rows_num=rows_number,
                                                                  columns_num=columns_num,
                                                                  expected_rows=expected_cells_data_size)

        extra_rows += self.trigger_compaction_by_data_write_and_flush(session, entity_type, rows_number + extra_rows)

        mark_logs = self.mark_log_on_all_nodes()

        cluster_state = self.validate_system_table(entity_type=entity_type, keyspace_name=self.KEYSPACE_NAME,
                                                   table_name=self.TABLE_NAME,
                                                   expected_entity_number=columns_num * rows_number - columns_num,
                                                   expected_entity_data_size=expected_cells_data_size)
        self.validate_log_warnings(cluster_state=cluster_state, entity_type=entity_type,
                                   keyspace_name=self.KEYSPACE_NAME,
                                   table_name=self.TABLE_NAME,
                                   marked_logs_dict=mark_logs,
                                   expect_warning=False)

    def test_large_cell_after_threshold_change(self):
        """
        Create table with ten large cells and few smaller cells after threshold changing. Validate that just cell
        bigger than the new threshold is reported in the system.large_cells table and there are warning in the log
        """
        small_rows = 20
        rows_number = 10
        columns_num = 3
        extra_rows = 0
        entity_type = 'cell'
        session = self.prepare_cluster(nodes=3, rf=3, options_dict={'compaction_large_cell_warning_threshold_mb': 2})

        self.create_large_row_table(session=session, table_name=self.TABLE_NAME, columns_num=columns_num,
                                    entity_type=entity_type)
        # Insert small cells
        maximum_primary_key_value = rows_number - 1
        self.create_large_row_data(session=session,
                                   table_name=self.TABLE_NAME,
                                   one_blob_size=self.BLOB_SIZE_1MB,
                                   columns_num=columns_num,
                                   rows_num=small_rows,
                                   start_row_index=0)

        self.cluster.flush()

        extra_rows += self.trigger_compaction_by_data_write_and_flush(session, entity_type, small_rows)

        # this will validate that no alerts were triggered so far
        self.validate_system_table(entity_type=entity_type, keyspace_name=self.KEYSPACE_NAME,
                                   table_name=self.TABLE_NAME,
                                   expected_entity_number=0,
                                   expected_entity_data_size=0,
                                   pk_max_index=0)

        # Insert large row
        expected_cells_data_size = self.create_large_row_data(session=session,
                                                              table_name=self.TABLE_NAME,
                                                              columns_num=columns_num,
                                                              rows_num=rows_number,
                                                              one_blob_size=self.BLOB_SIZE_1MB * 2,
                                                              start_row_index=small_rows + extra_rows)

        # Insert small cells
        maximum_primary_key_value = small_rows + rows_number + extra_rows - 1
        self.create_large_row_data(session=session,
                                   table_name=self.TABLE_NAME,
                                   one_blob_size=self.BLOB_SIZE_1MB,
                                   columns_num=columns_num,
                                   rows_num=rows_number,
                                   start_row_index=rows_number + small_rows + extra_rows)

        self.cluster.flush()

        extra_rows += self.trigger_compaction_by_data_write_and_flush(session, entity_type, rows_number + rows_number +
                                                                      small_rows + extra_rows)

        cluster_state = self.validate_system_table(entity_type=entity_type, keyspace_name=self.KEYSPACE_NAME,
                                                   table_name=self.TABLE_NAME,
                                                   expected_entity_number=columns_num * rows_number,
                                                   expected_entity_data_size=expected_cells_data_size,
                                                   pk_max_index=maximum_primary_key_value)
        self.validate_log_warnings(cluster_state=cluster_state, entity_type=entity_type,
                                   keyspace_name=self.KEYSPACE_NAME,
                                   table_name=self.TABLE_NAME)

    def test_large_cell_and_then_increase_threshold(self):
        """
        Create table with one large cell and few smaller cells after threshold changing. Validate that just cell
        bigger than the new threshold is reported in the system.large_cells table and there are warning in the log
        """
        small_rows = 10
        rows_number = 5
        columns_num = 3
        extra_rows = 0
        entity_type = 'cell'
        session = self.prepare_cluster(nodes=3, rf=3)

        self.create_large_row_table(session=session, table_name=self.TABLE_NAME, columns_num=columns_num,
                                    entity_type=entity_type)
        # Insert large cells that after increasing the threshold should disappear
        maximum_primary_key_value = rows_number - 1
        expected_cells_data_size = self.create_large_row_data(session=session,
                                                              table_name=self.TABLE_NAME,
                                                              one_blob_size=self.BLOB_SIZE_1MB,
                                                              columns_num=columns_num,
                                                              rows_num=rows_number,
                                                              start_row_index=0)

        self.cluster.flush()
        extra_rows += self.trigger_compaction_by_data_write_and_flush(session, entity_type, rows_number)

        # this will validate that no alerts were triggered so far
        cluster_state = self.validate_system_table(entity_type=entity_type, keyspace_name=self.KEYSPACE_NAME,
                                                   table_name=self.TABLE_NAME,
                                                   expected_entity_number=columns_num * rows_number,
                                                   expected_entity_data_size=expected_cells_data_size,
                                                   pk_max_index=maximum_primary_key_value)
        self.validate_log_warnings(cluster_state=cluster_state, entity_type=entity_type,
                                   keyspace_name=self.KEYSPACE_NAME,
                                   table_name=self.TABLE_NAME)

        # Increase the threshold and see the large cell warnings disappear
        debug('increasing warning threshold to 2mb')
        self.cluster.set_configuration_options({'compaction_large_cell_warning_threshold_mb': 2})

        # To apply threshold change, nodes must be restarted
        for node in self.cluster.nodelist():
            debug('restarting node {} to have threshold change to take effect'.format(node.name))
            node.stop()
            node.start(wait_other_notice=True, wait_for_binary_proto=True)

        self.cluster.flush()
        extra_rows += self.trigger_compaction_by_data_write_and_flush(session, entity_type, rows_number + extra_rows)

        # Insert what small cells (or what is not enough for triggering large cell warnings
        self.create_large_row_data(session=session,
                                   table_name=self.TABLE_NAME,
                                   one_blob_size=self.BLOB_SIZE_1MB,
                                   columns_num=columns_num,
                                   rows_num=small_rows,
                                   start_row_index=rows_number + extra_rows)

        self.cluster.flush()
        extra_rows += self.trigger_compaction_by_data_write_and_flush(session, entity_type, rows_number + small_rows +
                                                                      extra_rows)

        # Now we expect that the previous warnings will be removed
        self.validate_system_table(entity_type=entity_type, keyspace_name=self.KEYSPACE_NAME,
                                   table_name=self.TABLE_NAME,
                                   expected_entity_number=0,
                                   expected_entity_data_size=0,
                                   pk_max_index=0)

    def test_large_cell_detector_with_full_compaction(self):
        """
        Create table with one large cell and few small cells. Validate that just large cell is reported in the
        system.large_cells table and there are warning in the log with running full compaction
        """
        rows_num = 10
        columns_num = 10
        entity_type = 'cell'
        session = self.prepare_cluster(nodes=3, rf=3)

        self.create_large_row_table(session=session, table_name=self.TABLE_NAME, columns_num=columns_num,
                                    entity_type=entity_type)
        # Insert large row
        expected_cells_data_size = self.create_large_row_data(session=session,
                                                              table_name=self.TABLE_NAME,
                                                              columns_num=columns_num,
                                                              rows_num=rows_num,
                                                              one_blob_size=self.BLOB_SIZE_1MB,
                                                              start_row_index=0)

        # Insert small cells
        maximum_primary_key_value = rows_num - 1
        self.create_large_row_data(session=session,
                                   table_name=self.TABLE_NAME,
                                   one_blob_size=self.BLOB_SIZE_1MB / 2,
                                   columns_num=columns_num,
                                   rows_num=rows_num,
                                   start_row_index=rows_num)

        self.cluster.flush()
        self.cluster.nodetool('compact {} {}'.format(self.KEYSPACE_NAME, self.TABLE_NAME))

        cluster_state = self.validate_system_table(entity_type=entity_type, keyspace_name=self.KEYSPACE_NAME,
                                                   table_name=self.TABLE_NAME,
                                                   expected_entity_number=columns_num * rows_num,
                                                   expected_entity_data_size=expected_cells_data_size,
                                                   pk_max_index=maximum_primary_key_value)
        self.validate_log_warnings(cluster_state=cluster_state, entity_type=entity_type,
                                   keyspace_name=self.KEYSPACE_NAME,
                                   table_name=self.TABLE_NAME)

    def test_large_row_detector_with_full_compaction(self):
        """
        Create table with one large row and small rows. Validate that just large row is reported in the
        system.large_rows table and there are warning in the log with running full compaction
        """
        rows_num = 1
        columns_num = 15
        small_row_num = 10
        entity_type = 'row'

        session = self.prepare_cluster(nodes=3, rf=3,
                                       options_dict={'compaction_large_row_warning_threshold_mb': 1})

        self.create_large_row_table(session=session, table_name=self.TABLE_NAME, columns_num=columns_num)
        # Insert large row
        expected_rows_data_size = self.create_large_row_data(session=session,
                                                             table_name=self.TABLE_NAME,
                                                             columns_num=columns_num,
                                                             rows_num=rows_num,
                                                             one_blob_size=self.BLOB_SIZE_10k * 10,
                                                             start_row_index=0)

        # Insert small rows
        maximum_primary_key_value = rows_num - 1
        self.create_large_row_data(session=session,
                                   table_name=self.TABLE_NAME,
                                   one_blob_size=10,
                                   columns_num=columns_num,
                                   rows_num=small_row_num,
                                   start_row_index=rows_num)

        self.cluster.flush()
        self.cluster.nodetool('compact {} {}'.format(self.KEYSPACE_NAME, self.TABLE_NAME))

        cluster_state = self.validate_system_table(entity_type=entity_type, keyspace_name=self.KEYSPACE_NAME,
                                                   table_name=self.TABLE_NAME,
                                                   expected_entity_number=rows_num,
                                                   expected_entity_data_size=expected_rows_data_size,
                                                   pk_max_index=maximum_primary_key_value)
        self.validate_log_warnings(cluster_state=cluster_state, entity_type=entity_type,
                                   keyspace_name=self.KEYSPACE_NAME,
                                   table_name=self.TABLE_NAME)

    def test_large_partition_detector_with_full_compaction(self):
        """
        Create table with one large partition and one small partition and validate that partition is reported in
        the system.large_partitions table and there are warning in the log with running full compaction
        """
        partition_rows = 60000
        partition_num = 1
        entity_type = 'partition'

        session = self.prepare_cluster(nodes=4, rf=3,
                                       options_dict={'compaction_large_partition_warning_threshold_mb': 4})

        self.create_large_partition_table(session=session, table_name=self.TABLE_NAME)
        expected_partition_data_size = self.create_large_partition_data(session=session,
                                                                        table_name=self.TABLE_NAME,
                                                                        partition_rows=partition_rows,
                                                                        partitions_num=partition_num,
                                                                        one_blob_size=1024,
                                                                        start_partition_index=0)

        pk_max_index = partition_num - 1
        self.create_large_partition_data(session=session,
                                         table_name=self.TABLE_NAME,
                                         partition_rows=3000,
                                         partitions_num=10,
                                         one_blob_size=1024,
                                         start_partition_index=partition_num+1)
        self.cluster.flush()
        self.cluster.nodetool('compact {} {}'.format(self.KEYSPACE_NAME, self.TABLE_NAME))

        # adding more data and running flush again to give time to the compaction of the large partition to finish
        self.create_large_partition_data(session=session,
                                         table_name=self.TABLE_NAME,
                                         partition_rows=3000,
                                         partitions_num=10,
                                         one_blob_size=1024,
                                         start_partition_index=partition_num + partition_num + 1)
        self.cluster.flush()
        self.cluster.nodetool('compact {} {}'.format(self.KEYSPACE_NAME, self.TABLE_NAME))

        cluster_state = self.validate_system_table(entity_type=entity_type, keyspace_name=self.KEYSPACE_NAME,
                                                   table_name=self.TABLE_NAME,
                                                   expected_entity_number=partition_num,
                                                   expected_entity_data_size=expected_partition_data_size,
                                                   pk_max_index=pk_max_index)
        self.validate_log_warnings(cluster_state=cluster_state, entity_type=entity_type,
                                   keyspace_name=self.KEYSPACE_NAME,
                                   table_name=self.TABLE_NAME)


# removed DateTieredCompactionStrategy because it is not widely used and will shorten runtime by 25%
strategies = ['SizeTieredCompactionStrategy', 'TimeWindowCompactionStrategy', 'LeveledCompactionStrategy']

for strategy in strategies:
    cls_name = ('TestWideRows' + '_with_' + strategy)
    vars()[cls_name] = type(cls_name, (TestWideRows,), {'compaction_strategy': strategy, '__test__': True})
