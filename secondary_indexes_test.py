import os
import random
import re
import time
import uuid
import logging
from collections import defaultdict
from typing import Callable

import pytest
from ccmlib.scylla_cluster import ScyllaCluster

from dtest_class import Tester, create_ks, create_cf
from dtest_setup import DTestSetup
from tools.assertions import PytestRegex as regexp_matches
from tools.assertions import assert_all, assert_invalid, assert_one, assert_row_count, assert_none, \
    assert_expected_error, assert_row_count_in_select
from tools.data import get_entity_id, get_truncated_time_from_system_local, get_truncated_time_from_system_truncated, \
    get_view_id, wait_for_schema_agreement, create_index, create_local_index, rows_to_list
from tools.tables_view_manager import wait_for_view_build_start, view_built_status_query, index_is_built, \
    get_index_view_name
from tools.misc import generate_random_text, remove_node

from cassandra import ConsistencyLevel, InvalidRequest, WriteFailure, ConfigurationException
from cassandra.concurrent import (execute_concurrent, execute_concurrent_with_args)
from cassandra.query import BatchStatement, SimpleStatement

logger = logging.getLogger(__name__)

LONG_TEXT_LENGTH = 8193
OVERSIZE_LENGTH = 66536


class SecondaryIndexesHelpers:
    compaction_strategy = None
    INDEX_TYPE: str
    cluster: ScyllaCluster
    patient_exclusive_cql_connection: Callable
    patient_cql_connection: Callable
    patient_cql_cluster_session: Callable

    @pytest.fixture(autouse=True)
    def random_compaction_strategy(self, dtest_config):
        if not SecondaryIndexesHelpers.compaction_strategy:
            # NOTE: DateTieredCompactionStrategy is forbidden since https://github.com/scylladb/scylladb/pull/11458, so removed from list below
            strategies = ['LeveledCompactionStrategy', 'SizeTieredCompactionStrategy',
                          'TimeWindowCompactionStrategy']

            if dtest_config.is_enterprise:
                strategies += ['IncrementalCompactionStrategy']

            SecondaryIndexesHelpers.compaction_strategy = strategies[random.randint(0, len(strategies) - 1)]
            logger.debug('Randomly selected %s as compaction strategy for base table',
                         SecondaryIndexesHelpers.compaction_strategy)

    def assert_bootstrap_state(self, node, expected_bootstrap_state):
        """
        Assert that a node is on a given bootstrap state
        @param tester The dtest.Tester object to fetch the exclusive connection to the node
        @param node The node to check bootstrap state
        @param expected_bootstrap_state Bootstrap state to expect
        Examples:
        assert_bootstrap_state(node3, 'COMPLETED')
        """
        session = self.patient_exclusive_cql_connection(node)
        assert_all(session, "SELECT bootstrapped FROM system.local WHERE key='local'", [expected_bootstrap_state])

    @staticmethod
    def create_and_build_index(create_index_func, cluster, session, ks_name, table_name, index_column, index_name,
                               pk_name=None, compaction=None):
        if not pk_name:
            create_index_func(session=session, table_name=table_name, index_column=index_column,
                              index_name=index_name, compaction=compaction)
        else:
            create_index_func(session=session, table_name=table_name, index_column=index_column,
                              index_name=index_name, compaction=compaction, pk_name=pk_name)

        return index_is_built(cluster, session, ks_name, table_name, index_name)

    def prepare(self, user_table=False, rf=3, options={}, keyspace_name='ks', nodes=3, use_vnodes=False,
                fetch_size=None, jvm_args=[], session_node=1, consistency_level=None, **kwargs):
        """
        Prepare environment for test
        """
        cluster = self.cluster
        populate = nodes if isinstance(nodes, list) else [nodes, 0]
        cluster.populate(populate, use_vnodes=True)
        if options:
            cluster.set_configuration_options(values=options)
        if not jvm_args:
            jvm_args = ['--smp', '2', '--memory', '1G']
        cluster.start(jvm_args=jvm_args)
        if '--smp' in jvm_args:
            logger.debug('The cluster has been started with SMP {}'.format(jvm_args[jvm_args.index('--smp') + 1]))
        node1 = cluster.nodelist()[session_node - 1]

        if consistency_level is None:
            consistency_level = ConsistencyLevel.QUORUM if nodes > 1 else ConsistencyLevel.ONE

        session = self.patient_cql_connection(node1, consistency_level=consistency_level, **kwargs)

        if fetch_size:
            session.default_fetch_size = fetch_size
        create_ks(session, keyspace_name, rf)

        if user_table:
            columns = {"password": "varchar", "gender": "varchar", "session_token": "varchar", "state": "varchar",
                       "birth_year": "bigint"}
            create_cf(session, 'users', columns=columns, compaction={'class': self.compaction_strategy})

        return session

    def insert_row_with_long_value(self, create_table_cql, create_index_cql, insert_cql, session, column_name,
                                   value_length, expect_message):
        """ Validate two variations of the supplied insert statement, first
        as it is and then again transformed into a conditional statement
        """
        table_name = "table_" + str(int(round(time.time() * 1000)))
        session.execute(create_table_cql % (table_name, {'class': self.compaction_strategy}))
        session.execute(create_index_cql % table_name)
        value = "X" * value_length
        self.assert_request(session, insert_cql % table_name, value, table_name, column_name, value_length,
                            expect_message)

    def assert_request(self, session, insert_cql, value, table_name, column_name, value_length, expect_message):
        """ Perform two executions of the supplied statement, as a
        single statement and again as part of a batch
        """
        prepared = session.prepare(insert_cql)
        self.execute_and_assert(lambda: session.execute(prepared, [value]), insert_cql, table_name, column_name,
                                session, value_length, expect_message)
        batch = BatchStatement()
        batch.add(prepared, [value])
        self.execute_and_assert(lambda: session.execute(batch), insert_cql, table_name, column_name, session,
                                value_length, expect_message)

    def execute_and_assert(self, operation, cql_string, table_name, column_name, session, value_length, expect_message):
        try:
            operation()
            res_length = 0
            if self.INDEX_TYPE == 'local':
                stmt = 'select {0} from {1} where a=0 and {0}=\'{2}\''.format(column_name, table_name,
                                                                              "X" * value_length)
            elif self.INDEX_TYPE == 'global':
                stmt = 'select {0} from {1}'.format(column_name, table_name)
            else:
                raise ValueError("Unknown index type: %s", self.INDEX_TYPE)

            result = list(session.execute(stmt))
            if result:
                res_length = len(list(result[0])[0])
            if value_length == OVERSIZE_LENGTH:
                assert_success = False
                assert_fail = "Expecting query %s to be invalid" % cql_string
            else:
                assert_success = (value_length == res_length)
                assert_fail = "Expecting value length is {0}, received {1}. Result: {2}".format(
                    value_length, res_length, result)
            assert assert_success, assert_fail
        except AssertionError as e:
            raise e
        except WriteFailure:
            if not expect_message:
                raise

            error_found = False
            for node in self.cluster.nodelist():
                if node.grep_log(expr=expect_message):
                    error_found = True
                    break

            assert error_found, \
                f'Expected that failure reason is "{expect_message}", but the message wasn\'t found in the log'

        except Exception as e:
            if (expect_message and expect_message not in str(e)) or not expect_message:
                raise e

    def drain_and_restart_node(self, node, keyspace_name):
        node.nodetool('drain')
        node.stop()
        self.cluster.start()
        return self.patient_cql_connection(node, keyspace=keyspace_name)

    def node_action_with_delay(self, action, node=None, delay=0, wait=True, wait_other_notice=False, gently=True):
        """
        :param action: expected values: stop, remove
        :param action: str
        """
        if action not in ['stop', 'remove', 'decommission', 'add']:
            assert False, 'Unsupported node action'

        if delay:
            logger.debug('Sleep for {} seconds'.format(delay))
            time.sleep(delay)

        logger.debug('START: {0} node {1}'.format(action, node.name))
        if action == 'stop':
            node.stop(wait=wait, wait_other_notice=wait_other_notice, gently=gently)
        elif action == 'remove':
            remove_node(cluster=self.cluster, node=node)
        elif action == 'add':
            self.add_new_node()
        else:
            node.nodetool(action)
            if action == 'decommission':
                node.stop(wait=wait, wait_other_notice=wait_other_notice, gently=gently)
        logger.debug('FINISH: {0} node {1}'.format(action, node.name))

    def add_new_node(self, data_center='dc1', wait_for_binary_proto=True, jvm_args=None,
                     configuration_options=None, queue=None, delay=0, node_index=None):
        time.sleep(delay)
        i = len(self.cluster.nodes) + 1 if not node_index else node_index
        node = self.cluster.new_node(i=i, data_center=data_center)
        if configuration_options:
            node.set_configuration_options(values=configuration_options)  # CASSANDRA-11670
        logger.debug("Start join at {}".format(time.strftime("%H:%M:%S")))
        node.start(wait_for_binary_proto=wait_for_binary_proto, jvm_args=jvm_args)
        session = self.patient_exclusive_cql_connection(node)
        logger.debug("Finish join at {}".format(time.strftime("%H:%M:%S")))
        if queue:
            queue.put_nowait((session))
        return session

    def validate_index_data(self, session, cl, num_rows, table_name, index_column):
        if self.INDEX_TYPE == 'local':
            stmt = 'select key from {} where key = {} and  {} = {}'
        elif self.INDEX_TYPE == 'global':
            stmt = 'select key from {} where {} = {}'
        else:
            raise ValueError("Unknown index type: %s", self.INDEX_TYPE)

        logger.debug('Verify data with {} consistency level'.format(ConsistencyLevel.value_to_name[cl]))
        for _ in range(60):
            try:
                for i in range(num_rows):
                    assert_all(session, stmt.format(table_name, index_column, i, i + num_rows),
                               expected=[[i]], cl=cl)
                return
            except:
                time.sleep(1)


@pytest.mark.dtest_full
class TestSecondaryIndexes(Tester, SecondaryIndexesHelpers):
    INDEX_TYPE = 'global'

    @staticmethod
    def _index_sstables_files(node, keyspace, table, index):
        files = []
        for data_dir in node.data_directories():
            data_dir = os.path.join(data_dir, keyspace)
            base_tbl_dir = os.path.join(data_dir, [s for s in os.listdir(data_dir) if s.startswith(table)][0])
            index_sstables_dir = os.path.join(base_tbl_dir, '.' + index)
            files.extend(os.listdir(index_sstables_dir))
        return set(files)

    def config_keyspace(self, session, ks_name, table_name, index, ks_create=True):
        if ks_create:
            create_ks(session, ks_name, 1)
        create_cf(session, '{0}.{1}'.format(ks_name, table_name), key_type='text', columns={'col1': 'text'},
                  compaction={'class': self.compaction_strategy})

        assert self.create_and_build_index(create_index, self.cluster, session, ks_name, table_name,
                                           index['index_column'], index['index_name'],
                                           compaction=self.compaction_strategy), \
            'Index %s is not built' % index['index_name']

    def test_agg_query_by_pk(self):
        """
        Filter data by first primary key column that also an index
        """
        session = self.prepare(user_table=True, nodes=4, rf=3)

        session.execute('CREATE TABLE ks.t3(pk1 int, pk2 int, ck int, PRIMARY KEY((pk1, pk2), ck))')
        session.execute('INSERT INTO ks.t3(pk1, pk2, ck) VALUES (1, 1, 1)')

        assert_all(session, "SELECT COUNT(*) FROM ks.t3 WHERE pk1 = 1 ALLOW FILTERING", expected=[[1]],
                   cl=ConsistencyLevel.QUORUM)
        assert_all(session, "SELECT SUM(pk2) FROM ks.t3 WHERE pk1 = 1 ALLOW FILTERING", expected=[[1]],
                   cl=ConsistencyLevel.QUORUM)
        assert_all(session, "SELECT MIN(ck) FROM ks.t3 WHERE pk1 = 1 ALLOW FILTERING", expected=[[1]],
                   cl=ConsistencyLevel.QUORUM)

        # create index
        assert self.create_and_build_index(create_index, self.cluster, session, ks_name='ks',
                                           table_name='t3', index_column='pk1', index_name='ks_t3',
                                           compaction=self.compaction_strategy), \
            'Index ks_t3 is not built'

        assert_all(session, "SELECT COUNT(*) FROM ks.t3 WHERE pk1 = 1", expected=[[1]],
                   cl=ConsistencyLevel.QUORUM)

        session.execute("INSERT INTO ks.t3(pk1, pk2, ck) VALUES (1, 1, 2)")
        session.execute("INSERT INTO ks.t3(pk1, pk2, ck) VALUES (1, 1, 3)")

        assert_all(session, "SELECT COUNT(*) FROM ks.t3 WHERE pk1 = 1", expected=[[3]],
                   cl=ConsistencyLevel.QUORUM)
        assert_all(session, "SELECT SUM(pk2) FROM ks.t3 WHERE pk1 = 1", expected=[[3]],
                   cl=ConsistencyLevel.QUORUM)
        assert_all(session, "SELECT MAX(ck) FROM ks.t3 WHERE pk1 = 1", expected=[[3]],
                   cl=ConsistencyLevel.QUORUM)

        session.shutdown()

    def test_agg_query_by_second_pk(self):
        """
        Filter data by second primary key column that also an index
        """
        session = self.prepare(user_table=True, nodes=4, rf=3)

        session.execute('CREATE TABLE ks.t3(pk1 int, pk2 int, ck int, PRIMARY KEY((pk1, pk2), ck))')
        session.execute('INSERT INTO ks.t3(pk1, pk2, ck) VALUES (1, 1, 1)')
        session.execute('INSERT INTO ks.t3(pk1, pk2, ck) VALUES (1, 1, 4)')

        assert_all(session, "SELECT COUNT(*) FROM ks.t3 WHERE pk2 = 1 ALLOW FILTERING", expected=[[2]],
                   cl=ConsistencyLevel.QUORUM)
        assert_all(session, "SELECT SUM(pk1) FROM ks.t3 WHERE pk2 = 1 ALLOW FILTERING", expected=[[2]],
                   cl=ConsistencyLevel.QUORUM)
        assert_all(session, "SELECT MIN(ck) FROM ks.t3 WHERE pk2 = 1 ALLOW FILTERING", expected=[[1]],
                   cl=ConsistencyLevel.QUORUM)

        # create index
        assert self.create_and_build_index(create_index, self.cluster, session, ks_name='ks',
                                           table_name='t3', index_column='pk2', index_name='ks_t3',
                                           compaction=self.compaction_strategy), \
            'Index ks_t3 is not built'

        assert_all(session, "SELECT COUNT(*) FROM ks.t3 WHERE pk2 = 1", expected=[[2]],
                   cl=ConsistencyLevel.QUORUM)

        session.execute("INSERT INTO ks.t3(pk1, pk2, ck) VALUES (1, 1, 2)")
        session.execute("INSERT INTO ks.t3(pk1, pk2, ck) VALUES (1, 1, 3)")

        assert_all(session, "SELECT COUNT(*) FROM ks.t3 WHERE pk2 = 1", expected=[[4]],
                   cl=ConsistencyLevel.QUORUM)
        assert_all(session, "SELECT SUM(pk1) FROM ks.t3 WHERE pk2 = 1", expected=[[4]],
                   cl=ConsistencyLevel.QUORUM)
        assert_all(session, "SELECT MAX(ck) FROM ks.t3 WHERE pk2 = 1", expected=[[4]],
                   cl=ConsistencyLevel.QUORUM)

        session.shutdown()

    def test_agg_query_by_ck(self):
        """
        Filter data by clustering key column that also an index
        """
        session = self.prepare(user_table=True, nodes=4, rf=3)

        session.execute('CREATE TABLE ks.t3(pk1 int, ck int, v int, PRIMARY KEY(pk1, ck))')
        session.execute('INSERT INTO ks.t3(pk1, ck, v) VALUES (1, 1, 1)')
        session.execute('INSERT INTO ks.t3(pk1, ck, v) VALUES (1, 4, 1)')

        assert_all(session, "SELECT COUNT(*) FROM ks.t3 WHERE ck = 1 ALLOW FILTERING", expected=[[1]],
                   cl=ConsistencyLevel.QUORUM)
        assert_all(session, "SELECT SUM(pk1) FROM ks.t3 WHERE ck = 1 ALLOW FILTERING", expected=[[1]],
                   cl=ConsistencyLevel.QUORUM)
        assert_all(session, "SELECT MIN(v) FROM ks.t3 WHERE ck = 1 ALLOW FILTERING", expected=[[1]],
                   cl=ConsistencyLevel.QUORUM)

        # create index
        assert self.create_and_build_index(create_index, self.cluster, session, ks_name='ks',
                                           table_name='t3', index_column='ck', index_name='ks_t3',
                                           compaction=self.compaction_strategy), \
            'Index ks_t3 is not built'

        assert_all(session, "SELECT COUNT(*) FROM ks.t3 WHERE ck = 1", expected=[[1]],
                   cl=ConsistencyLevel.QUORUM)

        session.execute("INSERT INTO ks.t3(pk1, ck, v) VALUES (1, 2, 2)")
        session.execute("INSERT INTO ks.t3(pk1, ck, v) VALUES (1, 3, 3)")

        assert_all(session, "SELECT COUNT(*) FROM ks.t3 WHERE ck = 1", expected=[[1]],
                   cl=ConsistencyLevel.QUORUM)
        assert_all(session, "SELECT SUM(pk1) FROM ks.t3 WHERE ck = 1", expected=[[1]],
                   cl=ConsistencyLevel.QUORUM)
        assert_all(session, "SELECT MAX(v) FROM ks.t3 WHERE ck = 3", expected=[[3]],
                   cl=ConsistencyLevel.QUORUM)

        session.shutdown()

    def test_agg_query_by_second_ck(self):
        """
        Filter data by second clustering key column that also an index
        """
        session = self.prepare(user_table=True, nodes=4, rf=3)

        session.execute('CREATE TABLE ks.t3(pk1 int, ck1 int, ck2 int, PRIMARY KEY(pk1, ck1, ck2))')
        session.execute('INSERT INTO ks.t3(pk1, ck1, ck2) VALUES (1, 1, 1)')
        session.execute('INSERT INTO ks.t3(pk1, ck1, ck2) VALUES (1, 1, 3)')

        assert_all(session, "SELECT COUNT(*) FROM ks.t3 WHERE ck2 = 1 ALLOW FILTERING", expected=[[1]],
                   cl=ConsistencyLevel.QUORUM)
        assert_all(session, "SELECT SUM(pk1) FROM ks.t3 WHERE ck2 = 1 ALLOW FILTERING", expected=[[1]],
                   cl=ConsistencyLevel.QUORUM)
        assert_all(session, "SELECT MIN(ck1) FROM ks.t3 WHERE ck2 = 1 ALLOW FILTERING", expected=[[1]],
                   cl=ConsistencyLevel.QUORUM)

        # create index
        assert self.create_and_build_index(create_index, self.cluster, session, ks_name='ks',
                                           table_name='t3', index_column='ck2', index_name='ks_t3',
                                           compaction=self.compaction_strategy), \
            'Index ks_t3 is not built'

        assert_all(session, "SELECT COUNT(*) FROM ks.t3 WHERE ck2 = 1", expected=[[1]],
                   cl=ConsistencyLevel.QUORUM)

        session.execute("INSERT INTO ks.t3(pk1, ck1, ck2) VALUES (1, 1, 2)")
        session.execute("INSERT INTO ks.t3(pk1, ck1, ck2) VALUES (1, 2, 1)")

        assert_all(session, "SELECT COUNT(*) FROM ks.t3 WHERE ck2 = 1", expected=[[2]],
                   cl=ConsistencyLevel.QUORUM)
        assert_all(session, "SELECT SUM(pk1) FROM ks.t3 WHERE ck2 = 1", expected=[[2]],
                   cl=ConsistencyLevel.QUORUM)
        assert_all(session, "SELECT MAX(ck1) FROM ks.t3 WHERE ck2 = 1", expected=[[2]],
                   cl=ConsistencyLevel.QUORUM)

        session.shutdown()

    def test_agg_query_by_two_ck(self):
        """
        Filter data by two secondary keys columns that also an index
        """
        session = self.prepare(user_table=True, nodes=4, rf=3)
        session.default_fetch_size = 1

        session.execute('CREATE TABLE ks.t3(pk1 int, pk2 int, ck1 int, ck2 int, PRIMARY KEY((pk1, pk2), ck1, ck2))')
        session.execute('INSERT INTO ks.t3(pk1, pk2, ck1, ck2) VALUES (1, 1, 1, 1)')
        session.execute('INSERT INTO ks.t3(pk1, pk2, ck1, ck2) VALUES (1, 1, 1, 2)')
        session.execute('INSERT INTO ks.t3(pk1, pk2, ck1, ck2) VALUES (1, 2, 1, 3)')

        assert_all(session, "SELECT COUNT(*) FROM ks.t3 WHERE ck1 = 1 and ck2 in (1, 3) ALLOW FILTERING",
                   expected=[[2]], cl=ConsistencyLevel.QUORUM)
        assert_all(session, "SELECT AVG(pk2) FROM ks.t3 WHERE ck1 = 1 and ck2 in (1, 3) ALLOW FILTERING",
                   expected=[[1]], cl=ConsistencyLevel.QUORUM)
        assert_all(session, "SELECT MAX(ck2) FROM ks.t3 WHERE ck1 = 1 and ck2 in (1, 3) ALLOW FILTERING",
                   expected=[[3]], cl=ConsistencyLevel.QUORUM)

        # create index
        assert self.create_and_build_index(create_index, self.cluster, session, ks_name='ks',
                                           table_name='t3', index_column='ck1', index_name='ck1_index',
                                           compaction=self.compaction_strategy), \
            'Index on "ck1" column is not built'
        assert self.create_and_build_index(create_index, self.cluster, session, ks_name='ks',
                                           table_name='t3', index_column='ck2', index_name='ks_t3',
                                           compaction=self.compaction_strategy), \
            'Index "ck2" column is not built'

        assert_all(session, "SELECT COUNT(*) FROM ks.t3 WHERE ck1 = 1 and ck2 in (1, 3) ALLOW FILTERING",
                   expected=[[2]], cl=ConsistencyLevel.QUORUM)

        assert_all(session, "SELECT COUNT(*) FROM ks.t3 WHERE ck1 = 1 and ck2 >= 1 ALLOW FILTERING",
                   expected=[[3]], cl=ConsistencyLevel.QUORUM)

        session.execute("INSERT INTO ks.t3(pk1, pk2, ck1, ck2) VALUES (2, 1, 2, 1)")
        session.execute("INSERT INTO ks.t3(pk1, pk2, ck1, ck2) VALUES (2, 2, 1, 4)")

        assert_all(session, "SELECT COUNT(*) FROM ks.t3 WHERE ck1 = 2 and ck2 in (1, 3) ALLOW FILTERING",
                   expected=[[1]], cl=ConsistencyLevel.QUORUM)
        assert_all(session, "SELECT SUM(pk2) FROM ks.t3 WHERE ck1 = 1 and ck2 > 1 ALLOW FILTERING",
                   expected=[[5]], cl=ConsistencyLevel.QUORUM)
        assert_all(session, "SELECT AVG(pk1) FROM ks.t3 WHERE ck1 = 2 and ck2 >= 1 ALLOW FILTERING",
                   expected=[[2]], cl=ConsistencyLevel.QUORUM)

        session.shutdown()

    def test_group_by_pk_filter_by_index(self):
        """
        Create the index on the populated table and read the data that was inserted before index
        """
        session = self.prepare(user_table=False, nodes=4, rf=3)

        session.execute('CREATE TABLE ks.t(pk int, ck int, v int, PRIMARY KEY(pk, ck))')
        session.execute('INSERT INTO ks.t(pk, ck, v) VALUES (1, 2, 3)')
        session.execute('INSERT INTO ks.t(pk, ck, v) VALUES (1, 4, 3)')
        session.execute('INSERT INTO ks.t(pk, ck, v) VALUES (2, 4, 3)')
        assert_all(session, "SELECT pk FROM ks.t WHERE v=3 GROUP BY pk ALLOW FILTERING",
                   expected=[[1], [2]], cl=ConsistencyLevel.QUORUM)

        assert self.create_and_build_index(create_index, self.cluster, session, ks_name='ks',
                                           table_name='t', index_column='v', index_name='v_key',
                                           compaction=self.compaction_strategy), \
            'Index state_key is not built'

        assert_all(session, "SELECT pk FROM ks.t WHERE v=3 GROUP BY pk",
                   expected=[[1], [2]], cl=ConsistencyLevel.QUORUM)
        assert_all(session, "SELECT pk, count(pk) FROM ks.t WHERE v=3 GROUP BY pk",
                   expected=[[1, 2], [2, 1]], cl=ConsistencyLevel.QUORUM)

        session.shutdown()

    @pytest.mark.require("#7432")
    def test_filter_by_index_with_paging(self):
        """
        Create the index on the populated table and read the data that was inserted before index
        Read with pagination
        """
        session = self.prepare(user_table=False, nodes=4, rf=3)
        session.default_fetch_size = 1

        session.execute('CREATE TABLE ks.t(pk int, ck int, v int, PRIMARY KEY(pk, ck))')
        session.execute('INSERT INTO ks.t(pk, ck, v) VALUES (1, 2, 3)')
        session.execute('INSERT INTO ks.t(pk, ck, v) VALUES (1, 4, 3)')
        session.execute('INSERT INTO ks.t(pk, ck, v) VALUES (2, 4, 3)')
        assert_all(session, "SELECT pk FROM ks.t WHERE v=3 GROUP BY pk ALLOW FILTERING",
                   expected=[[1]], cl=ConsistencyLevel.QUORUM)

        assert self.create_and_build_index(create_index, self.cluster, session, ks_name='ks',
                                           table_name='t', index_column='v', index_name='v_key',
                                           compaction=self.compaction_strategy), \
            'Index state_key is not built'

        session.default_fetch_size = 3

        assert_all(session, "SELECT pk FROM ks.t WHERE v=3 GROUP BY pk",
                   expected=[[1], [2]], cl=ConsistencyLevel.QUORUM)

        session.default_fetch_size = 1
        assert_all(session, "SELECT pk FROM ks.t WHERE v=3 GROUP BY pk",
                   expected=[[1]], cl=ConsistencyLevel.QUORUM)

        session.shutdown()

    def test_query_data_created_before_index(self):
        """
        Create the index on the populated table and read the data that was inserted before index
        """
        session = self.prepare(user_table=True, nodes=4, rf=3)

        # insert data
        session.execute("INSERT INTO users (KEY, password, gender, state, birth_year) "
                        "VALUES ('user1', 'ch@ngem3a', 'f', 'TX', 1968);")
        session.execute("INSERT INTO users (KEY, password, gender, state, birth_year) "
                        "VALUES ('user2', 'ch@ngem3b', 'm', 'CA', 1971);")

        # create index
        assert self.create_and_build_index(create_index, self.cluster, session, ks_name='ks',
                                           table_name='users', index_column='gender', index_name='gender_key',
                                           compaction=self.compaction_strategy), \
            'Index %s is not built' % 'gender_key'

        assert self.create_and_build_index(create_index, self.cluster, session, ks_name='ks',
                                           table_name='users', index_column='state', index_name='state_key',
                                           compaction=self.compaction_strategy), \
            'Index %s is not built' % 'state_key'

        assert self.create_and_build_index(create_index, self.cluster, session, ks_name='ks',
                                           table_name='users', index_column='birth_year',
                                           index_name='birth_year_key', compaction=self.compaction_strategy), \
            'Index %s is not built' % 'birth_year_key'

        # insert data
        session.execute("INSERT INTO users (KEY, password, gender, state, birth_year) "
                        "VALUES ('user3', 'ch@ngem3c', 'f', 'FL', 1978);")
        session.execute("INSERT INTO users (KEY, password, gender, state, birth_year) "
                        "VALUES ('user4', 'ch@ngem3d', 'm', 'TX', 1974);")

        assert_all(session, "select count(*) from users", expected=[[4]], cl=ConsistencyLevel.QUORUM)
        assert_all(session, "select count(*) from users where state='TX'", expected=[[2]], cl=ConsistencyLevel.QUORUM)
        assert_all(session, "select count(*) from users where state='CA'", expected=[[1]], cl=ConsistencyLevel.QUORUM)

        session.shutdown()

    def test_query_data_by_pk_and_index(self):
        """
        Filter data by primary key and secondary index
        """
        session = self.prepare(user_table=True, nodes=4, rf=3)

        # insert data
        session.execute("INSERT INTO users (KEY, password, gender, state, birth_year) "
                        "VALUES ('user1', 'ch@ngem3a', 'f', 'TX', 1968);")
        session.execute("INSERT INTO users (KEY, password, gender, state, birth_year) "
                        "VALUES ('user2', 'ch@ngem3b', 'm', 'CA', 1971);")
        session.execute("INSERT INTO users (KEY, password, gender, state, birth_year) "
                        "VALUES ('user3', 'ch@ngem3c', 'f', 'FL', 1978);")
        session.execute("INSERT INTO users (KEY, password, gender, state, birth_year) "
                        "VALUES ('user4', 'ch@ngem3d', 'm', 'TX', 1974);")

        # create index
        assert self.create_and_build_index(create_index, self.cluster, session, ks_name='ks',
                                           table_name='users', index_column='gender', index_name='gender_key',
                                           compaction=self.compaction_strategy), \
            'Index %s is not built' % 'gender_key'

        assert_all(session, "select count(*) from users", expected=[[4]], cl=ConsistencyLevel.QUORUM)
        assert_all(session, "select count(*) from users where gender='f'", expected=[[2]], cl=ConsistencyLevel.QUORUM)
        assert_all(session, "select KEY, password, gender, state, birth_year from users where KEY='user2' "
                            "and gender='m'", expected=[['user2', 'ch@ngem3b', 'm', 'CA', 1971]],
                   cl=ConsistencyLevel.ALL)
        assert_all(session, "select count(*) from users where KEY='user1' and gender='m'", expected=[[0]],
                   cl=ConsistencyLevel.QUORUM)

        session.shutdown()

    def test_query_data_by_ck_and_index(self):
        """
        Filter data by primary and clustering keys and secondary index
        """
        ks_name = 'ks'
        table_name = 'cf'
        index_column = 'v'
        index_name = 'v_inx'

        session = self.prepare(nodes=4, rf=3)
        create_cf(session, '{0}.{1}'.format(ks_name, table_name), key_type='text',
                  compaction={'class': self.compaction_strategy})

        # insert data
        session.execute("INSERT INTO {} (key, c, v) VALUES ('user1', 'ch@ngem3a', 'f')".format(table_name))
        session.execute("INSERT INTO {} (key, c, v) VALUES ('user2', 'ch@ngem3b', 'm')".format(table_name))
        session.execute("INSERT INTO {} (key, c, v) VALUES ('user3', 'ch@ngem3c', 'f')".format(table_name))
        session.execute("INSERT INTO {} (key, c, v) VALUES ('user4', 'ch@ngem3d', 'm')".format(table_name))

        # create index
        assert self.create_and_build_index(create_index, self.cluster, session, ks_name=ks_name,
                                           table_name=table_name, index_column=index_column,
                                           index_name=index_name, compaction=self.compaction_strategy), \
            'Index %s is not built' % index_name

        assert_all(session, "select count(*) from {}".format(table_name), expected=[[4]], cl=ConsistencyLevel.QUORUM)
        assert_all(session, "select count(*) from {} where v='f'".format(table_name), expected=[[2]],
                   cl=ConsistencyLevel.QUORUM)
        assert_all(session, "select count(*) from {} where key='user2' and c='ch@ngem3b' and v='m'".format(table_name),
                   expected=[[1]], cl=ConsistencyLevel.ALL)

        assert_all(session, "select count(*) from {} where KEY='user1' and c='ch@ngem3a' and v='m'".format(table_name),
                   expected=[[0]], cl=ConsistencyLevel.QUORUM)

        session.shutdown()

    def test_low_cardinality_indexes(self):
        """
        Checks that low-cardinality secondary index subqueries are executed concurrently
        """
        session = self.prepare(nodes=4, rf=3)

        ks_name = 'ks'
        table_name = 'cf'
        index = {'index_name': 'col1_index', 'index_column': 'col1'}

        self.config_keyspace(session, ks_name, table_name, index, ks_create=False)

        num_rows = 100
        for i in range(num_rows):
            indexed_value = i % (num_rows // 3)
            # use the same indexed value three times
            session.execute("INSERT INTO {0}.{1} (key, col1) VALUES ('{2}', '{3}');".format(ks_name, table_name, i,
                                                                                            indexed_value))

        self.cluster.flush()

        assert_all(session, "SELECT count(*) FROM {0}.{1} WHERE {2}='1'".format(ks_name, table_name,
                                                                                index['index_column']),
                   expected=[[3]], cl=ConsistencyLevel.QUORUM, num_attempts=20)
        assert_all(session, "SELECT count(*) FROM {0}.{1} WHERE {2}='1' LIMIT 100".format(ks_name, table_name,
                                                                                          index['index_column']),
                   expected=[[3]], cl=ConsistencyLevel.QUORUM, num_attempts=20)
        assert_all(session, "SELECT count(*) FROM {0}.{1} WHERE {2}='1' LIMIT 3".format(ks_name, table_name,
                                                                                        index['index_column']),
                   expected=[[3]], cl=ConsistencyLevel.QUORUM, num_attempts=20)

        for limit in (1, 2):
            assert_row_count_in_select(session,
                                       query="select * from {0}.{1} WHERE {2}='1' LIMIT {3}".format(ks_name,
                                                                                                    table_name,
                                                                                                    index[
                                                                                                        'index_column'],
                                                                                                    limit),
                                       num_rows_expected=limit, consistency_level=ConsistencyLevel.QUORUM,
                                       num_attempts=20)

    def test_insert_data_after_recreating_ks(self):
        """
        Data inserted immediately after dropping and recreating a keyspace with an indexed column family is not included
        in the index.
        """
        session = self.prepare(nodes=4, rf=3)

        ks_name = 'ks'
        table_name = 'cf'
        index = {'index_name': 'col1_key', 'index_column': 'col1'}
        self.config_keyspace(session, ks_name, table_name, index, ks_create=False)

        for i in range(10):
            logger.debug(f"round {i}")
            try:
                session.execute("DROP KEYSPACE {}".format(ks_name))
            except ConfigurationException:
                pass

            self.config_keyspace(session, ks_name, table_name, index)

            for r in range(10):
                session.execute("INSERT INTO {0}.{1} (key, col1) VALUES ('{2}','asdf');".format(ks_name, table_name, r))

            wait_for_schema_agreement(session)
            time.sleep(30)
            assert_all(session, "select count(*) from {0}.{1} WHERE col1='asdf'".format(ks_name, table_name),
                       expected=[[10]], cl=ConsistencyLevel.QUORUM)

    def test_insert_data_after_recreating_cf(self):
        """
        Data inserted immediately after dropping and recreating an indexed column family is not included in the index.
        """
        session = self.prepare(nodes=4, rf=3)

        ks_name = 'ks'
        table_name = 'cf'
        index = {'index_name': 'col1_key', 'index_column': 'col1'}
        self.config_keyspace(session, ks_name, table_name, index, ks_create=False)
        for r in range(10):
            session.execute("INSERT INTO {0}.{1} (key, col1) VALUES ('{2}','asdf');".format(ks_name, table_name, r))

        for i in range(10):
            logger.debug("round %s" % i)
            drop_stmt = "DROP COLUMNFAMILY {0}.{1}".format(ks_name, table_name)

            logger.debug(drop_stmt)
            try:
                session.execute(drop_stmt)
            except InvalidRequest:
                pass

            self.config_keyspace(session, ks_name, table_name, index, ks_create=False)

            for r in range(10):
                session.execute("INSERT INTO {0}.{1} (key, {3}) VALUES ('{2}','asdf');".format(ks_name, table_name, r,
                                                                                               index['index_column']))

            wait_for_schema_agreement(session)
            time.sleep(30)
            assert_all(session, "select count(*) from {0}.{1} WHERE {2}='asdf'".format(ks_name, table_name,
                                                                                       index['index_column']),
                       expected=[[10]], cl=ConsistencyLevel.QUORUM)

    def test_oversize_indexed_values(self):
        """
        Reject inserts & updates where values of any indexed column is > 64k
        """
        expect_message = 'Key size too large'
        self._validate_long_indexed_values(OVERSIZE_LENGTH, expect_message)

    def test_long_indexed_values(self):
        """
        Correct inserts & updates where values of any indexed column is long and up to 64k
        """
        self._validate_long_indexed_values(LONG_TEXT_LENGTH, expect_message=None)

    def _validate_long_indexed_values(self, value_length, expect_message):
        session = self.prepare(nodes=4, rf=3)
        test = 'oversize' if value_length == OVERSIZE_LENGTH else 'long'

        self.ignore_log_patterns += [expect_message]

        logger.debug('Insert {} value into non-PK column'.format(test))
        self.insert_row_with_long_value("CREATE TABLE %s(a int, b int, c varchar, PRIMARY KEY (a)) "
                                        "WITH compaction = %s",
                                        "CREATE INDEX ON %s(c)",
                                        "INSERT INTO %s (a, b, c) VALUES (0, 0, ?)",
                                        session, column_name='c', value_length=value_length,
                                        expect_message=expect_message)

        logger.debug('Insert {} value into clustering key column'.format(test))
        self.insert_row_with_long_value("CREATE TABLE %s(a int, b text, c int, PRIMARY KEY (a, b)) "
                                        "WITH compaction = %s",
                                        "CREATE INDEX ON %s(b)",
                                        "INSERT INTO %s (a, b, c) VALUES (0, ?, 0)",
                                        session, column_name='b', value_length=value_length,
                                        expect_message=expect_message)

        logger.debug('Insert {} value into partition key column'.format(test))
        self.insert_row_with_long_value("CREATE TABLE %s(a text, b int, c int, PRIMARY KEY ((a, b))) "
                                        "WITH compaction = %s",
                                        "CREATE INDEX ON %s(a)",
                                        "INSERT INTO %s (a, b, c) VALUES (?, 0, 0)",
                                        session, column_name='a', value_length=value_length,
                                        expect_message=expect_message)

        logger.debug('Table with compact storage. Insert {} value into non-PK column'.format(test))
        self.insert_row_with_long_value("CREATE TABLE %s(a int, b text, PRIMARY KEY (a)) WITH COMPACT STORAGE and "
                                        "compaction = %s",
                                        "CREATE INDEX ON %s(b)",
                                        "INSERT INTO %s (a, b) VALUES (0, ?)",
                                        session, column_name='b', value_length=value_length,
                                        expect_message=expect_message)

    @pytest.mark.skip('Not relevant for Scylla - manual index rebuild is not supported')
    def test_manual_rebuild_index(self):
        """
        asserts that new sstables are written when rebuild_index is called from nodetool
        """
        cluster = self.cluster
        cluster.populate(1).start()
        node1, = cluster.nodelist()
        session = self.patient_cql_connection(node1)

        node1.stress(['write', 'n=50K', 'no-warmup'])
        session.execute("use keyspace1;")
        lookup_value = session.execute('select "C0" from standard1 limit 1')[0].C0
        session.execute('CREATE INDEX ix_c0 ON standard1("C0");')

        start = time.time()
        while time.time() < start + 30:
            logger.debug("waiting for index to build")
            time.sleep(1)
            if index_is_built(node1, session, 'keyspace1', 'standard1', 'ix_c0'):
                break
        else:
            raise DtestTimeoutError()

        stmt = session.prepare('select * from standard1 where "C0" = ?')
        assert 1 == len(list(session.execute(stmt, [lookup_value])))
        before_files = self._index_sstables_files(node1, 'keyspace1', 'standard1', 'ix_c0')

        node1.nodetool("rebuild_index keyspace1 standard1 ix_c0")
        start = time.time()
        while time.time() < start + 30:
            logger.debug("waiting for index to rebuild")
            time.sleep(1)
            if index_is_built(node1, session, 'keyspace1', 'standard1', 'ix_c0'):
                break
        else:
            raise DtestTimeoutError()

        after_files = self._index_sstables_files(node1, 'keyspace1', 'standard1', 'ix_c0')
        assert before_files != after_files
        assert 1 == len(list(session.execute(stmt, [lookup_value])))

        # verify that only the expected row is present in the build indexes table
        assert 1 == len(list(session.execute("""SELECT * FROM system."IndexInfo";""")))

    @pytest.mark.skip('Not relevant for Scylla - manual index rebuild is not supported')
    def test_failing_manual_rebuild_index(self):
        """
        @jira_ticket CASSANDRA-10130
        Tests the management of index status during manual index rebuilding failures.
        """

        cluster = self.cluster
        cluster.populate(1, install_byteman=True).start(wait_for_binary_proto=True)
        node = cluster.nodelist()[0]

        session = self.patient_cql_connection(node)
        create_ks(session, 'k', 1)
        session.execute("CREATE TABLE k.t (k int PRIMARY KEY, v int)")
        session.execute("CREATE INDEX idx ON k.t(v)")
        session.execute("INSERT INTO k.t(k, v) VALUES (0, 1)")
        session.execute("INSERT INTO k.t(k, v) VALUES (2, 3)")

        # Verify that the index is marked as built and it can answer queries
        assert_one(session, """SELECT * FROM system."IndexInfo" WHERE table_name='k'""", ['k', 'idx'])
        assert_one(session, "SELECT * FROM k.t WHERE v = 1", [0, 1])

        # Simulate a failing index rebuild
        before_files = self._index_sstables_files(node, 'k', 't', 'idx')
        node.byteman_submit(['./byteman/index_build_failure.btm'])
        with pytest.raises(Exception):
            node.nodetool("rebuild_index k t idx")
        after_files = self._index_sstables_files(node, 'k', 't', 'idx')

        # Verify that the index is not rebuilt, not marked as built, and it still can answer queries
        assert before_files == after_files
        assert_none(session, """SELECT * FROM system."IndexInfo" WHERE table_name='k'""")
        assert_one(session, "SELECT * FROM k.t WHERE v = 1", [0, 1])

        # Restart the node to trigger the scheduled index rebuild
        before_files = after_files
        node.nodetool('drain')
        node.stop()
        cluster.start()
        session = self.patient_cql_connection(node)
        session.execute("USE k")
        after_files = self._index_sstables_files(node, 'k', 't', 'idx')

        # Verify that, the index is rebuilt, marked as built, and it can answer queries
        assert before_files != after_files
        assert_one(session, """SELECT * FROM system."IndexInfo" WHERE table_name='k'""", ['k', 'idx'])
        assert_one(session, "SELECT * FROM k.t WHERE v = 1", [0, 1])

        # Simulate another failing index rebuild
        before_files = self._index_sstables_files(node, 'k', 't', 'idx')
        node.byteman_submit(['./byteman/index_build_failure.btm'])
        with pytest.raises(Exception):
            node.nodetool("rebuild_index k t idx")
        after_files = self._index_sstables_files(node, 'k', 't', 'idx')

        # Verify that the index is not rebuilt, not marked as built, and it still can answer queries
        assert before_files == after_files
        assert_none(session, """SELECT * FROM system."IndexInfo" WHERE table_name='k'""")
        assert_one(session, "SELECT * FROM k.t WHERE v = 1", [0, 1])

        # Successfully rebuild the index
        before_files = after_files
        node.nodetool("rebuild_index k t idx")
        cluster.wait_for_compactions()
        after_files = self._index_sstables_files(node, 'k', 't', 'idx')

        # Verify that the index is rebuilt, marked as built, and it can answer queries
        assert before_files != after_files
        assert_one(session, """SELECT * FROM system."IndexInfo" WHERE table_name='k'""", ['k', 'idx'])
        assert_one(session, "SELECT * FROM k.t WHERE v = 1", [0, 1])

    def test_drop_index_while_building(self):
        """
        Asserts that indexes deleted before they have been completely build are invalidated and not built after restart
        """
        keyspace_name = 'keyspace1'
        table_name = 'standard1'
        index_name = 'idx'
        index_column = '"C0"'

        session = self.prepare(nodes=4, rf=3, keyspace_name=keyspace_name)
        node = self.cluster.nodelist()[0]

        # Create some thousands of rows to guarantee a long index building
        node.stress(['write', 'n=50K', 'no-warmup', '-schema', 'replication(factor=3)',
                     'compaction(strategy={})'.format(self.compaction_strategy)])

        # Create an index and immediately drop it, without waiting for index building
        create_index(session, table_name, index_column, index_name, compaction=self.compaction_strategy)

        # Get view ID
        index_view_name = get_index_view_name(index_name)
        view_id = get_view_id(session=session, keyspace_name=keyspace_name, view_name=index_view_name)
        logger.debug('View ID: {}'.format(view_id))

        self.ignore_log_patterns += ['Can\'t find a column family with UUID {}'.format(view_id),
                                     'mutation_write_failure_exception']

        session.execute('DROP INDEX {}'.format(index_name))

        self.cluster.wait_for_compactions()

        # Check that the index is not marked as built nor queryable
        assert_none(session, view_built_status_query(ks=keyspace_name, view=index_view_name))
        assert_invalid(session, 'SELECT * FROM {0} WHERE {1} = 0x00'.format(table_name, index_column),
                       matching='use ALLOW FILTERING', expected=Exception)

        # Restart the node to trigger any eventual unexpected index rebuild
        session = self.drain_and_restart_node(node, keyspace_name)

        # The index should remain not built nor queryable after restart
        assert_none(session, view_built_status_query(ks=keyspace_name, view=index_view_name))
        assert_invalid(session, 'SELECT * FROM {0} WHERE {1} = 0x00'.format(table_name, index_column),
                       matching='use ALLOW FILTERING', expected=Exception)

    def test_multi_index_filtering_query(self):
        """
        asserts that having multiple indexes that cover all predicates still requires ALLOW FILTERING to also be present
        """
        keyspace_name = 'ks'
        table_name = 'tbl'
        index_names = {'ix_tbl_c0': 'c0', 'ix_tbl_c1': 'c1'}

        session = self.prepare(nodes=4, rf=3, keyspace_name=keyspace_name)

        create_cf(session, table_name, key_type='uuid', columns={'c0': 'text', 'c1': 'text', 'c2': 'text'},
                  compaction={'class': self.compaction_strategy})

        for name, column in index_names.items():
            assert self.create_and_build_index(create_index, self.cluster, session, keyspace_name,
                                               table_name, column, name,
                                               compaction=self.compaction_strategy), f'Index {name} is not built'

        smt = "INSERT INTO {0} (key, c0, c1, c2) values (uuid(), '{1}', '{2}', '{3}')"
        session.execute(smt.format(table_name, 'a', 'b', 'c'))
        session.execute(smt.format(table_name, 'a', 'b', 'c'))
        session.execute(smt.format(table_name, 'q', 'b', 'c'))
        session.execute(smt.format(table_name, 'a', 'e', 'f'))
        session.execute(smt.format(table_name, 'a', 'e', 'f'))

        assert_all(session, "SELECT count(*) FROM {0} WHERE {1} = 'a';".format(table_name, index_names['ix_tbl_c0']),
                   expected=[[4]], cl=ConsistencyLevel.QUORUM)

        # Filter query by multi index without using ALLOW FILTERING option expected fail
        smt = "SELECT count(*) FROM {0} WHERE {1} = 'a' AND {2} = 'b'".format(table_name, index_names['ix_tbl_c0'],
                                                                              index_names['ix_tbl_c1'])
        assert_invalid(session, smt, matching='use ALLOW FILTERING')

        assert_all(session, '{} ALLOW FILTERING'.format(smt), expected=[[2]], cl=ConsistencyLevel.QUORUM)

    @pytest.mark.require('#7772')
    def test_index_same_key_twice(self):
        """SELECT by indexed key with WHERE like "key = X AND key = Y".

        See scylladb/scylla#7772 for details.
        """
        keyspace_name = 'ks'
        table_name = 'tbl'

        session = self.prepare(nodes=1, rf=1, keyspace_name=keyspace_name)

        create_cf(session, table_name, key_type='uuid', columns={'c0': 'text', 'c1': 'text', 'c2': 'text'},
                  compaction={'class': self.compaction_strategy})

        assert self.create_and_build_index(create_index, self.cluster, session, keyspace_name,
                                           table_name, 'c0', 'ix_tbl_c0', compaction=self.compaction_strategy), \
            'Index ix_tbl_c0 is not built'

        smt = "INSERT INTO {0} (key, c0, c1, c2) values (uuid(), '{1}', '{2}', '{3}')"
        session.execute(smt.format(table_name, 'a', 'b', 'c'))
        session.execute(smt.format(table_name, 'a', 'b', 'c'))
        session.execute(smt.format(table_name, 'q', 'b', 'c'))
        session.execute(smt.format(table_name, 'a', 'e', 'f'))
        session.execute(smt.format(table_name, 'a', 'e', 'f'))

        smt = "SELECT count(*) FROM {0} WHERE {1} = 'a' AND {1} = 'b'".format(table_name, 'c0')
        assert_all(session, smt, expected=[[0]], cl=ConsistencyLevel.QUORUM)

    def test_truncate_base(self):
        """
        asserts that truncating base table will result in truncating secondary index as well
        """

        def create_data():
            smt = "INSERT INTO {0} (key, c0, c1) values (uuid(), '{1}', '{2}')"
            session.execute(smt.format(table_name, 'a', 'b'))
            session.execute(smt.format(table_name, 'a', 'b'))
            session.execute(smt.format(table_name, 'q', 'b'))
            session.execute(smt.format(table_name, 'a', 'e'))
            session.execute(smt.format(table_name, 'a', 'e'))

        def validate_truncated_entries_for_table_and_views():
            for node in self.cluster.nodelist():
                node_session = self.patient_exclusive_cql_connection(node=node)
                for name in [table_name] + [index_name + '_index' for index_name in index_names.keys()]:
                    table_or_view = 'table' if name == table_name else 'view'
                    id = get_entity_id(session=node_session, table_or_view=table_or_view, keyspace_name=keyspace_name,
                                       entity_name=name)

                    # validate truncation entries in the system.truncated table - expected entry
                    truncated_time = get_truncated_time_from_system_truncated(session=node_session, table_id=id)

                    assert truncated_time, \
                        'Expected truncated entry in the system.truncated table, but it\'s not found'

                    # validate truncation entries in the system.local table - not expected entry
                    truncated_time = get_truncated_time_from_system_local(session=node_session)
                    assert truncated_time == [[None]], \
                        'Not expected truncated entry in the system.local table, but it\'s found'

        keyspace_name = 'ks'
        table_name = 'tbl'
        index_names = {'ix_tbl_c0': 'c0', 'ix_tbl_c1': 'c1'}

        session = self.prepare(nodes=4, rf=3, keyspace_name=keyspace_name)

        create_cf(session, table_name, key_type='uuid', columns={'c0': 'text', 'c1': 'text', 'c2': 'text'},
                  compaction={'class': self.compaction_strategy})

        for name, column in index_names.items():
            assert self.create_and_build_index(create_index, self.cluster, session, keyspace_name,
                                               table_name, column, name, compaction=self.compaction_strategy), \
                'Index %s is not built' % name

        create_data()

        # ensure sstables are created and will be dropped
        self.cluster.flush()

        smt = "SELECT count(*) FROM {0} WHERE {1} = '{2}'"

        # ensure data is loaded into cache and the cache will be cleared
        assert_all(session, smt.format(table_name, index_names['ix_tbl_c0'], 'a'), expected=[[4]],
                   cl=ConsistencyLevel.QUORUM)

        assert_row_count(session, "tbl", 5)

        session.execute("TRUNCATE table tbl")
        assert_row_count(session, "tbl", 0)

        # check that index queries are also truncated
        assert_all(session, smt.format(table_name, index_names['ix_tbl_c0'], 'a'), expected=[[0]],
                   cl=ConsistencyLevel.QUORUM)

        assert_all(session, smt.format(table_name, index_names['ix_tbl_c1'], 'b'), expected=[[0]],
                   cl=ConsistencyLevel.QUORUM)

        validate_truncated_entries_for_table_and_views()

        logger.debug('Insert data after truncate')
        create_data()
        validate_truncated_entries_for_table_and_views()

    @pytest.mark.skip('Not relevant. No index information in the query trace')
    def test_only_coordinator_chooses_index_for_query(self):
        """
        Checks that the index to use is selected (once) on the coordinator and
        included in the serialized command sent to the replicas.
        @jira_ticket CASSANDRA-10215
        """
        cluster = self.cluster
        cluster.populate(3).start()
        node1, node2, node3 = cluster.nodelist()
        session = self.patient_exclusive_cql_connection(node3)
        session.max_trace_wait = 120
        session.execute("CREATE KEYSPACE ks WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'};")
        session.execute("CREATE TABLE ks.cf (a text PRIMARY KEY, b text);")
        session.execute("CREATE INDEX b_index ON ks.cf (b);")
        num_rows = 100
        for i in range(num_rows):
            indexed_value = i % (num_rows / 3)
            # use the same indexed value three times
            session.execute("INSERT INTO ks.cf (a, b) VALUES ('{a}', '{b}');"
                            .format(a=i, b=indexed_value))

        cluster.flush()

        def check_trace_events(trace, regex, expected_matches, on_failure):
            """
            Check for the presence of certain trace events. expected_matches should be a list of
            tuple(source, min_count, max_count) indicating that of all the trace events for the
            the source, the supplied regex should match at least min_count trace messages & at
            most max_count messages. E.g. [(127.0.0.1, 1, 10), (127.0.0.2, 0, 0)]
            indicates that the regex should match at least 1, but no more than 10 events emitted
            by node1, and that no messages emitted by node2 should match.
            """
            match_counts = {}
            for event_source, min_matches, max_matches in expected_matches:
                match_counts[event_source] = 0

            for event in trace.events:
                desc = event.description
                match = re.match(regex, desc)
                if match:
                    if event.source in match_counts:
                        match_counts[event.source] += 1
            for event_source, min_matches, max_matches in expected_matches:
                if match_counts[event_source] < min_matches or match_counts[event_source] > max_matches:
                    on_failure(trace, regex, expected_matches, match_counts, event_source, min_matches, max_matches)

        def halt_on_failure(trace, regex, expected_matches, match_counts, event_source, min_expected, max_expected):
            self.fail("Expected to find between {min} and {max} trace events matching {pattern} from {source}, "
                      "but actually found {actual}. (Full counts: {all})"
                      .format(min=min_expected, max=max_expected, pattern=regex, source=event_source,
                              actual=match_counts[event_source], all=match_counts))

        def retry_on_failure(trace, regex, expected_matches, match_counts, event_source, min_expected, max_expected):
            logger.debug("Trace event inspection did not match expected, sleeping before re-fetching trace events. "
                         "Expected: {expected} Actual: {actual}".format(expected=expected_matches, actual=match_counts))
            time.sleep(2)
            trace.populate(max_wait=2.0)
            check_trace_events(trace, regex, expected_matches, halt_on_failure)

        query = SimpleStatement("SELECT * FROM ks.cf WHERE b='1';")
        result = session.execute(query, trace=True)
        assert 3 == len(list(result))

        trace = result.get_query_trace()

        # we have forced node3 to act as the coordinator for
        # all requests by using an exclusive connection, so
        # only node3 should select the index to use
        check_trace_events(trace,
                           "Index mean cardinalities are b_index:[0-9]*. Scanning with b_index.",
                           [("127.0.0.1", 0, 0), ("127.0.0.2", 0, 0), ("127.0.0.3", 1, 1)],
                           retry_on_failure)
        # check that the index is used on each node, really we only care that the matching
        # message appears on every node, so the max count is not important
        check_trace_events(trace,
                           "Executing read on ks.cf using index b_index",
                           [("127.0.0.1", 1, 200), ("127.0.0.2", 1, 200), ("127.0.0.3", 1, 200)],
                           retry_on_failure)

    @pytest.mark.single_node
    def test_query_indexes_with_vnodes(self):
        """
        Verifies correct query behaviour in the presence of vnodes
        @jira_ticket CASSANDRA-11104
        """
        keyspace_name = 'ks'
        # True/False: create table with/without compact storage
        tables = {'compact_table': True, 'regular_table': False}
        index_column = 'b'

        session = self.prepare(nodes=1, rf=1, keyspace_name=keyspace_name, use_vnodes=True)

        for table_name, compact_storage in tables.items():
            create_cf(session, table_name, key_type='int', columns={'b': 'int'}, compact_storage=compact_storage,
                      compaction={'class': self.compaction_strategy})
            assert self.create_and_build_index(create_index, self.cluster, session, keyspace_name,
                                               table_name, index_column, get_index_view_name(table_name),
                                               compaction=self.compaction_strategy), \
                'Index %s is not built' % get_index_view_name(table_name)

        insert_args = [(i, i % 2) for i in range(100)]
        for table in tables:
            logger.debug('Perform the test for {} table'.format(table))
            execute_concurrent_with_args(session,
                                         session.prepare("INSERT INTO {}.{} (key, {}) VALUES (?, ?)".
                                                         format(keyspace_name, table, index_column)),
                                         insert_args)
            res = session.execute("SELECT * FROM {}.{} WHERE {} = 0".format(keyspace_name, table, index_column))
            assert len(rows_to_list(res)) == 50, f'Expected: {50}, got {len(rows_to_list(res))}'

    @pytest.mark.single_node
    def test_multi_column_index(self):
        """
        Test that impossible to create secondary index on the few columns and valid error message is received
        """
        keyspace_name = 'ks'
        table_name = 'cf'
        index_columns = {'b': 'int', 'c': 'int'}

        session = self.prepare(nodes=1, rf=1, keyspace_name=keyspace_name)

        # try to create index on 2 columns
        create_cf(session, table_name, key_type='int', columns=index_columns,
                  compaction={'class': self.compaction_strategy})
        assert_expected_error(func=create_index, expected_error='Only CUSTOM indexes support multiple columns',
                              args=(session, table_name, index_columns),
                              kwargs={'index_name': 'two_columns_index', 'compaction': self.compaction_strategy})

        # try to create index on 6 columns
        table_name = 'cf_6columns'
        index_columns = {'b': 'int', 'c': 'int', 'd': 'int', 'e': 'int', 'f': 'int', 'g': 'int'}
        create_cf(session, table_name, key_type='int', columns=index_columns,
                  compaction={'class': self.compaction_strategy})
        assert_expected_error(func=create_index, expected_error='Only CUSTOM indexes support multiple columns',
                              args=(session, table_name, index_columns),
                              kwargs={'index_name': 'six_columns_index', 'compaction': self.compaction_strategy})

    def _prepare_for_ttl(self):
        keyspace_name = 'ks'
        table_name = 'cf'
        index_column = 'b'
        index_name = '{}_inx'.format(index_column)
        select_query = 'select * from {} where {} = {}'
        mv_query = 'select key, {} from {}'.format(index_column, get_index_view_name(index_name))

        session = self.prepare(nodes=1, rf=1, keyspace_name=keyspace_name)

        create_cf(session, table_name, key_type='int', columns={'b': 'int', 'c': 'int'},
                  compaction={'class': self.compaction_strategy})
        session.execute("INSERT INTO {} (key, b, c) VALUES (0, 1, 2)".format(table_name))
        assert_all(session, select_query.format(table_name, 'key', 0), [[0, 1, 2]], cl=ConsistencyLevel.ALL)

        assert self.create_and_build_index(create_index, self.cluster, session, keyspace_name, table_name,
                                           index_column, index_name, compaction=self.compaction_strategy), \
            'Index %s is not built' % index_name
        assert_all(session, select_query.format(table_name, index_column, 1), [[0, 1, 2]], cl=ConsistencyLevel.ALL)
        return session, keyspace_name, table_name, index_column, index_name, select_query, mv_query

    @pytest.mark.single_node
    def test_ttl_index_column(self):
        """
        Verify SI with default_time_to_live can be deleted properly using expired livenessInfo
        """
        session, keyspace_name, table_name, index_column, index_name, select_query, mv_query = self._prepare_for_ttl()

        ttl = 60
        logger.debug('Update index column with TTL {}'.format(ttl))
        session.execute("UPDATE {} USING TTL {} SET {}=3 WHERE key=0".format(table_name, ttl, index_column))
        assert_all(session, select_query.format(table_name, 'key', 0), [[0, 3, 2]], cl=ConsistencyLevel.ALL)
        assert_all(session, select_query.format(table_name, index_column, 3), [[0, 3, 2]], cl=ConsistencyLevel.ALL)
        assert_none(session, select_query.format(table_name, index_column, 1), cl=ConsistencyLevel.ALL)
        assert_all(session, mv_query, [[0, 3]], cl=ConsistencyLevel.ALL)

        time.sleep(ttl + 5)
        # Validate that no record is returned when filtered by index
        assert_all(session, select_query.format(table_name, 'key', 0), [[0, None, 2]], cl=ConsistencyLevel.ALL)
        assert_none(session, select_query.format(table_name, index_column, 3), cl=ConsistencyLevel.ALL)
        assert_none(session, select_query.format(table_name, index_column, 1), cl=ConsistencyLevel.ALL)
        assert_none(session, mv_query, cl=ConsistencyLevel.ALL)

    @pytest.mark.single_node
    def test_ttl_non_index_column(self):
        """
        Verify SI is not impact from TTL on non-imdex column
        """
        session, keyspace_name, table_name, index_column, index_name, select_query, mv_query = self._prepare_for_ttl()

        ttl = 60
        logger.debug('Update non-index column with TTL {}'.format(ttl))
        session.execute("UPDATE {} USING TTL {} SET {}=3 WHERE key=0".format(table_name, ttl, 'c'))
        assert_all(session, select_query.format(table_name, 'key', 0), [[0, 1, 3]], cl=ConsistencyLevel.ALL)
        assert_all(session, select_query.format(table_name, index_column, 1), [[0, 1, 3]], cl=ConsistencyLevel.ALL)
        assert_all(session, mv_query, [[0, 1]], cl=ConsistencyLevel.ALL)

        time.sleep(ttl + 5)
        # Validate that record is returned when filtered by index
        assert_all(session, select_query.format(table_name, 'key', 0), [[0, 1, None]], cl=ConsistencyLevel.ALL)
        assert_all(session, select_query.format(table_name, index_column, 1), [[0, 1, None]], cl=ConsistencyLevel.ALL)
        assert_all(session, mv_query, [[0, 1]], cl=ConsistencyLevel.ALL)

    def test_delete_indexed_rows(self):
        """
        Delete rows from indexed table and read data by index
        """
        keyspace_name = 'ks'
        table_name = 'cf'
        index_name = 'b_index'
        index_column = 'b'

        session = self.prepare(nodes=4, rf=3, keyspace_name=keyspace_name, session_node=3)

        create_cf(session, table_name, key_type='int', columns={'b': 'int'},
                  compaction={'class': self.compaction_strategy})
        assert self.create_and_build_index(create_index, self.cluster, session, ks_name=keyspace_name,
                                           table_name=table_name, index_column=index_column, index_name=index_name,
                                           compaction=self.compaction_strategy), \
            'Index %s is not built' % index_name

        num_rows = 100
        for i in range(num_rows):
            indexed_value = i + 100
            session.execute(
                "INSERT INTO {}.{} (key, b) VALUES ({}, {})".format(keyspace_name, table_name, i, indexed_value))

        self.cluster.flush()

        # Delete 10 rows by index
        logger.debug('Delete 10 rows by index')
        start_key, delete_num = 30, 10
        rows_for_delete = list(range(start_key, start_key + delete_num))
        for i in rows_for_delete:
            session.execute("DELETE FROM {} WHERE key = {}".format(table_name, i))
        time.sleep(30)

        # Valudate the data in not in table
        assert_row_count(session, table_name=table_name, expected=num_rows - delete_num,
                         consistency_level=ConsistencyLevel.ALL)
        query = 'select key, b from {} where {}={}'
        for i in list(range(num_rows)):
            if i in rows_for_delete:
                assert_none(session, query=query.format(table_name, 'key', i), cl=ConsistencyLevel.ALL)
                assert_none(session, query=query.format(table_name, index_column, i + 100), cl=ConsistencyLevel.ALL)
            else:
                res = [[i, i + 100]]
                assert_all(session, query=query.format(table_name, 'key', i), expected=res, cl=ConsistencyLevel.ALL)
                assert_all(session, query=query.format(table_name, index_column, i + 100), expected=res,
                           cl=ConsistencyLevel.ALL)

        # Valudate the data in not in table and SI materialized view
        assert_row_count(session, table_name=get_index_view_name(index_name), expected=num_rows - delete_num,
                         consistency_level=ConsistencyLevel.ALL)

    # @pytest.mark.next_gating - https://github.com/scylladb/scylla/issues/4724
    # @pytest.mark.dtest_debug - https://github.com/scylladb/scylla/issues/4384
    def test_stop_node_during_index_build(self):
        """
        Stop one node during index building and read data by index
        """
        self._node_action_during_index_build(node_action='stop', nodes=4, rf=3, num_rows=100000)

    @pytest.mark.dtest_heavy
    def test_remove_node_during_index_build(self):
        """
        Remove one node during index building and read data by index
        """
        self._node_action_during_index_build(node_action='remove', nodes=4, rf=3, num_rows=100000)

    def test_decommission_node_during_index_build(self):
        """
        Decommission one node during index building and read data by index
        """
        self._node_action_during_index_build(node_action='decommission', nodes=4, rf=3, num_rows=100000)

    def test_add_node_during_index_build(self):
        """
        Decommission one node during index building and read data by index
        """
        self._node_action_during_index_build(node_action='add', nodes=3, rf=3, num_rows=100000)

    def _node_action_during_index_build(self, node_action, nodes, rf, num_rows):
        keyspace_name = 'ks'
        table_name = 'cf'
        index_name = 'b_index'
        index_column = 'b'
        view_name = get_index_view_name(index_name)

        session = self.prepare(nodes=nodes, rf=rf, keyspace_name=keyspace_name, session_node=3)
        node2 = self.cluster.nodelist()[1]
        node2_ip = list(node2.network_interfaces['binary'])[0].replace('.', r'\.')

        create_cf(session, table_name, key_type='int', columns={'b': 'int'},
                  compaction={'class': self.compaction_strategy})

        statement = session.prepare("INSERT INTO {}.{} (key, b) VALUES (?, ?)".format(keyspace_name, table_name))
        statement.consistency_level = ConsistencyLevel.QUORUM

        execute_concurrent_with_args(session, statement,
                                     map(lambda k: [k] + [k + num_rows], [k for k in range(0, num_rows)]))
        self.cluster.flush()

        # Create index and wait while the build is starting
        create_index(session, table_name, index_column, index_name, compaction=self.compaction_strategy)
        wait_for_view_build_start(session, ks=keyspace_name, view=view_name)

        exclude_errors = [rf'Can\'t send migration request: node {node2_ip} is down',
                          rf'Error applying view update to {node2_ip}.*: exceptions::unavailable_exception \(Cannot achieve consistency level for cl ONE. Requires 1, alive 0\)',
                          rf'Error applying view update to {node2_ip}.*: exceptions::mutation_write_timeout_exception \(Operation timed out for {keyspace_name}.{index_name}_index - received only 0 responses from 1 CL=ONE.\)',
                          rf'Error applying view update to .*: exceptions::mutation_write_failure_exception \(Operation failed for {keyspace_name}.{index_name}_index - received 0 responses and 1 failures from 1 CL=ONE.\)',
                          ]
        self.ignore_log_patterns += exclude_errors

        # Perform action on second node
        self.node_action_with_delay(node_action, node2)

        # Index will not finish building, because view building underneath is paused until updates can be sent.
        if node_action == 'add':
            assert True

        if node_action in ['remove', 'decommission']:
            logger.debug('Add new node')
            session = self.add_new_node(node_index=nodes + 1)
            session.execute('USE {}'.format(keyspace_name))
        elif node_action == 'stop':
            logger.debug('Start node {}'.format(node2.name))
            node2.start(wait_for_binary_proto=True)

        index_is_built(self.cluster, session, ks_name=keyspace_name, table_name=table_name, index_name=index_name)

        # Validate the data using filtering by index with cl=ONE
        self.validate_index_data(session, cl=ConsistencyLevel.ONE, num_rows=num_rows, table_name=table_name,
                                 index_column=index_column)

        # Validate view rows
        assert_row_count_in_select(session=session, query="SELECT * FROM {}".format(view_name),
                                   num_rows_expected=num_rows, consistency_level=ConsistencyLevel.QUORUM)

        self.check_errors(self.cluster.nodelist()[0], exclude_errors=exclude_errors, regex=True)

    def test_stop_node_after_index_build(self):
        """
        Stop one node after index building and read data by index
        """
        self._node_action_after_index_build(node_action='stop', nodes=4, rf=3, num_rows=1000)

    def test_remove_node_after_index_build(self):
        """
        Remove one node after index building and read data by index
        """
        self._node_action_after_index_build(node_action='remove', nodes=4, rf=3, num_rows=1000)

    def test_decommission_node_after_index_build(self):
        """
        Decommission one node after index building and read data by index
        """
        self._node_action_after_index_build(node_action='decommission', nodes=4, rf=3, num_rows=1000)

    def test_add_node_after_index_build(self):
        """
        Decommission one node after index building and read data by index
        """
        self._node_action_after_index_build(node_action='add', nodes=3, rf=3, num_rows=1000)

    def _node_action_after_index_build(self, node_action, nodes, rf, num_rows):
        keyspace_name = 'ks'
        table_name = 'cf'
        index_name = 'b_index'
        index_column = 'b'
        view_name = get_index_view_name(index_name)

        session = self.prepare(nodes=nodes, rf=rf, keyspace_name=keyspace_name, session_node=3)
        node2 = self.cluster.nodelist()[1]
        node2_ip = list(node2.network_interfaces['binary'])[0]

        create_cf(session, table_name, key_type='int', columns={'b': 'int'},
                  compaction={'class': self.compaction_strategy})

        statement = session.prepare("INSERT INTO {}.{} (key, b) VALUES (?, ?)".format(keyspace_name, table_name))
        statement.consistency_level = ConsistencyLevel.QUORUM

        execute_concurrent_with_args(session, statement,
                                     map(lambda k: [k] + [k + num_rows], [k for k in range(0, num_rows)]))
        self.cluster.flush()

        # Create index and wait while the index is built
        assert self.create_and_build_index(create_index, self.cluster, session, ks_name=keyspace_name,
                                           table_name=table_name, index_name=index_name, index_column=index_column,
                                           compaction=self.compaction_strategy), \
            'Index %s is not built' % index_name

        exclude_errors = [f'Can\'t send migration request: node {node2_ip} is down',
                          f'Error applying view update to .*: exceptions::mutation_write_failure_exception (Operation '
                          rf'failed for {keyspace_name}\.{index_name}_index - received 0 responses and 1 failures from '
                          f'1 CL=ONE)',
                          ]
        self.ignore_log_patterns += exclude_errors

        # Perform action on second node
        self.node_action_with_delay(node_action, node=node2)

        # Validate the data using filtering by index
        self.validate_index_data(session, cl=ConsistencyLevel.ONE, num_rows=num_rows, table_name=table_name,
                                 index_column=index_column)

        # Validate view rows
        assert_row_count_in_select(session=session, query="SELECT * FROM {}".format(view_name),
                                   num_rows_expected=num_rows, consistency_level=ConsistencyLevel.QUORUM)

        self.check_errors(self.cluster.nodelist()[0], exclude_errors)


@pytest.mark.dtest_full
@pytest.mark.single_node
class TestSecondaryIndexesOnCollections(Tester, SecondaryIndexesHelpers):
    INDEX_TYPE = 'global'

    def test_tuple_indexes(self):
        """
        Checks that secondary indexes on tuples work for querying
        """
        keyspace_name = 'tuple_index_test'
        table_name = 'simple_with_tuple'
        index_columns = {'single_tuple': '({0})', 'double_tuple': '({0},{0})', 'triple_tuple': '({0},{0},{0})',
                         'nested_one': '({0},({0},{0}))'}
        session = self.prepare(nodes=1, rf=1, keyspace_name=keyspace_name)

        create_cf(session, table_name, key_type='uuid', columns={'normal_col': 'int', 'single_tuple': 'tuple<int>',
                                                                 'double_tuple': 'tuple<int, int>',
                                                                 'triple_tuple': 'tuple<int, int, int>',
                                                                 'nested_one': 'tuple<int, tuple<int, int>>'},
                  compaction={'class': self.compaction_strategy})

        cmds = [("""insert into {1}
                        (key, normal_col, single_tuple, double_tuple, triple_tuple, nested_one)
                    values
                        (uuid(), {0}, ({0}), ({0},{0}), ({0},{0},{0}), ({0},({0},{0})))""".format(n, table_name), ())
                for n in range(50)]

        results = execute_concurrent(session, cmds * 5, raise_on_first_error=True, concurrency=200)

        for (success, result) in results:
            assert success, "didn't get success on insert: {0}".format(result)

        # no index present yet, make sure there's an error trying to query column
        stmt = ("SELECT * from {} where single_tuple = (1)".format(table_name))

        assert_invalid(session, stmt, matching='use ALLOW FILTERING', expected=Exception)

        for index_column in index_columns.keys():
            assert self.create_and_build_index(create_index, self.cluster, session, keyspace_name, table_name,
                                               index_column, 'idx_' + index_column,
                                               compaction=self.compaction_strategy), \
                f'Index idx_{index_column} is not built'

        select_cmd = "select * from {} where {} = {}"
        # check if indexes work on existing data
        for n in range(50):
            for index_column, template in index_columns.items():
                assert 5 == len(list(session.execute(select_cmd.format(table_name, index_column, template.format(n)))))

                assert 0 == len(list(session.execute(select_cmd.format(table_name, index_column, template.format(-1)))))

        # check if indexes work on new data inserted after index creation
        results = execute_concurrent(session, cmds * 3, raise_on_first_error=True, concurrency=200)
        for (success, result) in results:
            assert success, "didn't get success on insert: {0}".format(result)
        time.sleep(5)

        def _validate_data(expected_rows, format_value):
            for index_column, template in index_columns.items():
                assert expected_rows == len(list(session.execute(select_cmd.format(table_name, index_column,
                                                                                   template.format(format_value)))))

        for n in range(50):
            _validate_data(expected_rows=8, format_value=n)

        # check if indexes work on mutated data
        for n in range(5):
            for index_column, template in index_columns.items():
                rows = session.execute(select_cmd.format(table_name, index_column, template.format(n)))
                for row in rows:
                    session.execute("update {} set {} = {} where key = {}".format(table_name, index_column,
                                                                                  template.format(-999), row.key))

        for n in range(5):
            _validate_data(expected_rows=0, format_value=n)

        for n in range(50):
            _validate_data(expected_rows=40, format_value=-999)

    @pytest.mark.require('#2962')
    def test_list_indexes(self):
        self.collection_indexes_run(type='list')

    @pytest.mark.require('#2962')
    def test_set_indexes(self):
        self.collection_indexes_run(type='set')

    @pytest.mark.require('#2962')
    def test_map_indexes(self):
        self.collection_indexes_run(type='map')

    def collection_indexes_run(self, type):
        """
        Checks that secondary indexes on lists work for querying.
        """
        keyspace_name = 'index_search'
        table_name = 'users'
        index_name = 'user_uuids'
        index_column = 'uuids'
        index_column_type = {'list': 'list<uuid>', 'map': 'map<uuid, uuid>', 'set': 'set<uuid>'}
        session = self.prepare(nodes=1, rf=1, keyspace_name=keyspace_name)

        create_cf(session, table_name, key_type='uuid', columns={
            'email': 'text', 'uuids': index_column_type[type]}, compaction={'class': self.compaction_strategy})

        select_cmd = "SELECT * from {} where {} contains {}"

        # no index present yet, make sure there's an error trying to query column
        assert_invalid(session, select_cmd.format(table_name, index_column, uuid.uuid4()),
                       matching='use ALLOW FILTERING', expected=Exception)

        # add index and query again (even though there are no rows in the table yet)
        assert self.create_and_build_index(create_index, self.cluster, session, keyspace_name, table_name,
                                           index_column, index_name, compaction=self.compaction_strategy), \
            f'Index {index_name} is not built'

        assert 0 == len(list(session.execute(select_cmd.format(table_name, index_column, uuid.uuid4()))))

        # add a row which doesn't specify data for the indexed column, and query again
        user1_uuid = uuid.uuid4()
        session.execute("INSERT INTO {} (key, email) values ({}, 'test@example.com')".format(table_name, user1_uuid))

        assert 0 == len(list(session.execute(select_cmd.format(table_name, index_column, uuid.uuid4()))))

        # alter the row to add a single item to the indexed list
        _id = uuid.uuid4()
        index_value = {'set': '{{{id}}}', 'list': '[{id}]', 'map': '{{{id}:{user_id}}}'}
        session.execute("UPDATE {} set {} = {} where key = {}".format(table_name, index_column,
                                                                      index_value[type].format(id=_id,
                                                                                               user_id=user1_uuid),
                                                                      user1_uuid))
        time.sleep(5)

        assert 1 == len(list(session.execute(select_cmd.format(table_name, index_column, _id))))

        # add a bunch of user records and query them back
        shared_uuid = uuid.uuid4()  # this uuid will be on all records

        log = []
        index_value = {'set': '{{{s_uuid}, {u_uuid1}}}', 'list': '[{s_uuid}, {u_uuid1}]',
                       'map': '{{{u_uuid1}:{u_uuid2}, {s_uuid}:{s_uuid}}}'}
        for i in range(50000):
            user_uuid = uuid.uuid4()
            unshared_uuid1 = uuid.uuid4()
            unshared_uuid2 = uuid.uuid4()

            # give each record a unique email address using the int index
            session.execute("INSERT INTO {table_name} (key, email, uuids) values ({key}, '{prefix}@example.com',"
                            " {index_value})".format(table_name=table_name, key=user_uuid, prefix=i,
                                                     index_value=index_value[type].format(s_uuid=shared_uuid,
                                                                                          u_uuid1=unshared_uuid1,
                                                                                          u_uuid2=unshared_uuid2)))

            log.append({'user_id': user_uuid, 'email': str(i) + '@example.com', 'unshared_uuid1': unshared_uuid1})
            if type == 'map':
                log[-1].update({'unshared_uuid2': unshared_uuid2})

        # confirm there is now 50k rows with the 'shared' uuid above in the secondary index
        assert 50000 == len(list(session.execute(select_cmd.format(table_name, index_column, shared_uuid))))

        # shuffle the log in-place, and double-check a slice of records by querying the secondary index
        random.shuffle(log)

        for log_entry in log[:1000]:
            rows = list(session.execute("SELECT key, email, uuids FROM {} where {} contains {}"
                                        .format(table_name, index_column, log_entry['unshared_uuid1'])))
            assert 1 == len(rows)

            db_user_id, db_email, db_uuids = rows[0]

            assert db_user_id == log_entry['key']
            assert db_email == log_entry['email']
            assert str(db_uuids[0]) == str(shared_uuid)
            assert str(db_uuids[1]) == str(log_entry['unshared_uuid1'])

        if type == 'map':
            # attempt to add an index on map values as well (should fail pre 3.0)
            index_name_new = 'user_uuids_values'

            assert_expected_error(func=create_index,
                                  expected_error='Index {} is a duplicate of existing index {}'.format(index_name_new,
                                                                                                       index_name),
                                  args=(session, table_name, index_column), kwargs={'index_name': 'index_name_new'})

            logger.debug('Drop index {}'.format(index_name))
            session.execute("DROP INDEX {}".format(index_name))

            # add index on values (will index rows added prior)
            assert self.create_and_build_index(create_index, self.cluster, session, keyspace_name,
                                               table_name, index_column, index_name_new), \
                'Index %s is not built' % index_name_new

            # shuffle the log in-place, and double-check a slice of records by querying the secondary index
            random.shuffle(log)

            # since we already inserted unique ids for values as well, check that appropriate records are found
            for log_entry in log[:1000]:
                rows = list(session.execute(select_cmd.format(table_name, index_column, log_entry['unshared_uuid2'])))
                assert 1 == len(rows)

                db_user_id, db_email, db_uuids = rows[0]
                assert db_user_id == log_entry['key'], f"Expected: '{log_entry['key']}', got: '{db_user_id}'"
                assert db_email == log_entry['email'], f"Expected: '{log_entry['email']}', got: '{db_email}'"

                assert shared_uuid in db_uuids, f"Not found '{shared_uuid}' in '{db_uuids}'"
                assert log_entry['unshared_uuid2'] in db_uuids.values(), \
                    f"Not found '{log_entry['unshared_uuid2']}' in '{db_uuids.values()}'"

    def test_frozen_list_indexes(self):
        """
        Checks that secondary indexes can't be created on frozen list column
        """
        self.frozen_collection_indexes_run(type='frozen list')

    def test_frozen_set_indexes(self):
        """
        Checks that secondary indexes can't be created on frozen set column
        """
        self.frozen_collection_indexes_run(type='frozen set')

    def test_frozen_map_indexes(self):
        """
        Checks that secondary indexes can't be created on frozen map column
        """
        self.frozen_collection_indexes_run(type='frozen map')

    def frozen_collection_indexes_run(self, type):
        keyspace_name = 'index_search'
        table_name = 'users'
        index_name = 'user_uuids'
        index_column = 'uuids'
        index_column_type = {'frozen list': 'frozen<list<uuid>>', 'frozen map': 'frozen<map<uuid, uuid>>',
                             'frozen set': 'frozen<set<uuid>>'}
        session = self.prepare(nodes=1, rf=1, keyspace_name=keyspace_name)

        create_cf(session, table_name, key_type='uuid', columns={
            'email': 'text', 'uuids': index_column_type[type]}, compaction={'class': self.compaction_strategy})

        # try to create global index
        with pytest.raises(expected_exception=InvalidRequest) as err:
            create_index(session, table_name, index_column, index_name, compaction=self.compaction_strategy)

        assert str(err) == regexp_matches(r'.*Cannot create index on index_values of frozen.*'), 'Not expected error'

        # try to create local index
        with pytest.raises(expected_exception=InvalidRequest) as err:
            create_local_index(session, table_name, 'key', index_column, index_name,
                               compaction=self.compaction_strategy)

        assert str(err) == regexp_matches(r'.*Cannot create index on index_values of frozen.*'), 'Not expected error'


@pytest.mark.skip('Not relevant for Scylla')
@pytest.mark.dtest_full
class TestUpgradeSecondaryIndexes(Tester):

    def test_read_old_sstables_after_upgrade(self):
        """ from 2.1 the location of sstables changed (CASSANDRA-5202), but existing sstables continue
        to be read from the old location. Verify that this works for index sstables as well as regular
        data column families (CASSANDRA-9116)
        """
        cluster = self.cluster

        # Forcing cluster version on purpose
        cluster.set_install_dir(version="2.0.12")
        if "memtable_allocation_type" in cluster._config_options:
            cluster._config_options.__delitem__("memtable_allocation_type")
        cluster.populate(1).start()

        [node1] = cluster.nodelist()
        session = self.patient_cql_connection(node1)
        create_ks(session, 'index_upgrade', 1)
        session.execute("CREATE TABLE index_upgrade.table1 (k int PRIMARY KEY, v int)")
        session.execute("CREATE INDEX ON index_upgrade.table1(v)")
        session.execute("INSERT INTO index_upgrade.table1 (k,v) VALUES (0,0)")

        query = "SELECT * FROM index_upgrade.table1 WHERE v=0"
        assert_one(session, query, [0, 0])

        # Upgrade to the 2.1.x version
        node1.drain()
        node1.watch_log_for("DRAINED")
        node1.stop(wait_other_notice=False)
        logger.debug("Upgrading to current version")
        self.set_node_to_current_version(node1)
        node1.start(wait_other_notice=True)

        [node1] = cluster.nodelist()
        session = self.patient_cql_connection(node1)
        logger.debug(cluster.cassandra_version())
        assert_one(session, query, [0, 0])

    def upgrade_to_version(self, tag, nodes=None):
        logger.debug('Upgrading to ' + tag)
        if nodes is None:
            nodes = self.cluster.nodelist()

        for node in nodes:
            logger.debug('Shutting down node: ' + node.name)
            node.drain()
            node.watch_log_for("DRAINED")
            node.stop(wait_other_notice=False)

        # Update Cassandra Directory
        for node in nodes:
            node.set_install_dir(version=tag)
            logger.debug("Set new cassandra dir for %s: %s" % (node.name, node.get_install_dir()))
        self.cluster.set_install_dir(version=tag)

        # Restart nodes on new version
        for node in nodes:
            logger.debug('Starting %s on new version (%s)' % (node.name, tag))
            # Setup log4j / logback again (necessary moving from 2.0 -> 2.1):
            node.set_log_level("INFO")
            node.start(wait_other_notice=True)
            # node.nodetool('upgradesstables -a')


@pytest.mark.skip('Not relevant for Scylla')
@pytest.mark.dtest_full
class TestPreJoinCallback(Tester, SecondaryIndexesHelpers):

    @pytest.fixture(autouse=True)
    def fixture_add_additional_log_patterns(self, fixture_dtest_setup: DTestSetup):
        self.fixture_dtest_setup.ignore_log_patterns += [
            # ignore all streaming errors during bootstrap
            r'Exception encountered during startup',
            r'Streaming error occurred',
            r'\[Stream.*\] Streaming error occurred',
            r'\[Stream.*\] Remote peer 127.0.0.\d failed stream session',
            r'Error while waiting on bootstrap to complete. Bootstrap will have to be restarted.'
        ]

    def _base_test(self, joinFn):
        cluster = self.cluster
        tokens = cluster.balanced_tokens(2)
        cluster.set_configuration_options(values={'num_tokens': 1})

        # Create a single node cluster
        cluster.populate(1)
        node1 = cluster.nodelist()[0]
        node1.set_configuration_options(values={'initial_token': tokens[0]})
        cluster.start(wait_other_notice=True)

        # Create a table with 2i
        session = self.patient_cql_connection(node1)
        create_ks(session, 'ks', 1)
        create_cf(session, 'cf', columns={'c1': 'text', 'c2': 'text'})
        session.execute("CREATE INDEX c2_idx ON cf (c2);")

        keys = 10000
        insert_statement = session.prepare("INSERT INTO ks.cf (key, c1, c2) VALUES (?, 'value1', 'value2')")
        execute_concurrent_with_args(session, insert_statement, [['k%d' % k] for k in range(keys)])

        # Run the join function to test
        joinFn(cluster, tokens[1])

    def test_bootstrap(self):
        def bootstrap(cluster, token):
            node2 = self.cluster.new_node(i=2)
            node2.set_configuration_options(values={'initial_token': token})
            node2.start(wait_for_binary_proto=True)
            assert node2.grep_log('Executing pre-join post-bootstrap tasks')

        self._base_test(bootstrap)

    def test_resume(self):
        def resume(cluster, token):
            node1 = cluster.nodes['node1']
            # set up byteman on node1 to inject a failure when streaming to node2
            node1.stop(wait=True)
            node1.byteman_port = '8100'
            node1.import_config_files()
            node1.start(wait_for_binary_proto=True)
            node1.byteman_submit(['./byteman/inject_failure_streaming_to_node2.btm'])

            node2 = self.cluster.new_node(i=2)
            node2.set_configuration_options(values={'initial_token': token, 'streaming_socket_timeout_in_ms': 1000})
            node2.start(wait_other_notice=False, wait_for_binary_proto=True)
            self.assert_bootstrap_state(node2, 'IN_PROGRESS')

            node2.nodetool("bootstrap resume")
            self.assert_bootstrap_state(node2, 'COMPLETED')
            assert node2.grep_log('Executing pre-join post-bootstrap tasks')

        self._base_test(resume)

    def test_manual_join(self):
        def manual_join(cluster, token):
            node2 = self.cluster.new_node(i=2)
            node2.set_configuration_options(values={'initial_token': token})
            node2.start(join_ring=False, wait_for_binary_proto=True, wait_other_notice=240)
            assert node2.grep_log('Not joining ring as requested')
            assert not node2.grep_log('Executing pre-join')

            node2.nodetool("join")
            assert node2.grep_log('Executing pre-join post-bootstrap tasks')

        self._base_test(manual_join)

    def test_write_survey(self):
        def write_survey_and_join(cluster, token):
            node2 = self.cluster.new_node(i=2)
            node2.set_configuration_options(values={'initial_token': token})
            node2.start(jvm_args=["-Dcassandra.write_survey=true"], wait_for_binary_proto=True)
            assert node2.grep_log('Startup complete, but write survey mode is active, '
                                  'not becoming an active ring member.')
            assert not node2.grep_log('Executing pre-join')

            node2.nodetool("join")
            assert node2.grep_log('Leaving write survey mode and joining ring at operator request')
            assert node2.grep_log('Executing pre-join post-bootstrap tasks')

        self._base_test(write_survey_and_join)


@pytest.mark.dtest_full
class TestLocalIndexes(Tester, SecondaryIndexesHelpers):
    INDEX_TYPE = 'local'

    def config_keyspace(self, session, ks_name, table_name, index, columns=None, ks_create=True,
                        global_index_name=None):
        if ks_create:
            create_ks(session, ks_name, 1)
        session.execute('USE {}'.format(ks_name))
        create_cf(session, '{0}.{1}'.format(ks_name, table_name), key_type='text', columns=columns)
        assert self.create_and_build_index(create_index_func=create_local_index, cluster=self.cluster,
                                           session=session, ks_name=ks_name, table_name=table_name,
                                           index_column=index['index_column'], index_name=index['index_name'],
                                           pk_name=index['pk_name']), \
            'Index %s is not built' % index['index_name']

        if global_index_name:
            assert self.create_and_build_index(create_index_func=create_index, cluster=self.cluster,
                                               session=session, ks_name=ks_name, table_name=table_name,
                                               index_column=index['index_column'], index_name=global_index_name), \
                'Index %s is not built' % global_index_name

    def test_simple_local_index(self):
        """
        - Create table with 3 columns
        - Create local index on "v" column and partition key "key"
        - Filter data by key and local index and validate result
        """
        session = self.prepare(nodes=4, rf=3)

        ks_name = 'ks'
        table_name = 'cf'
        index = {'index_name': 'v_local_key', 'index_column': 'v', 'pk_name': 'key'}

        self.config_keyspace(session, ks_name, table_name, index, ks_create=False)

        data = {
            'Tel Aviv': [{'c': generate_random_text(), 'v': 'Dizzengof'},
                         {'c': generate_random_text(), 'v': 'Arlozorov'}],
            'Washington': [{'c': generate_random_text(), 'v': 'Southgate'},
                           {'c': generate_random_text(), 'v': '11th'},
                           {'c': generate_random_text(), 'v': '10th'}],
            'London': [{'c': generate_random_text(), 'v': 'Geneva'},
                       {'c': generate_random_text(), 'v': 'Moorland'}],
            'Vancouver': [{'c': generate_random_text(), 'v': '12th Ave'},
                          {'c': generate_random_text(), 'v': '16th Ave'}]
        }

        for key, row_columns in data.items():
            for columns_data in row_columns:
                query = "INSERT INTO {table_name} (key, c, v) VALUES ('{key}', '{c}', '{v}')". \
                    format(table_name=table_name, key=key, c=columns_data['c'], v=columns_data['v'])
                session.execute(query)

        for key, indexes in data.items():
            for columns_data in indexes:
                ck = columns_data['c']
                index_value = columns_data['v']
                query = "SELECT key, c, v FROM {table_name} WHERE key='{key}' AND v='{index_value}'".format(**locals())
                assert_all(session=session, query=query, expected=[[key, ck, index_value]], cl=ConsistencyLevel.QUORUM,
                           num_attempts=30)

    def test_global_local_index_on_same_column(self):
        """
        - Create table with 3 columns
        - Create local index on "v" column and partition key "key"
        - Create global index on "v" column
        - Filter data by key and local index and validate result
        - Filter data by global index and validate result
        """
        session = self.prepare(nodes=4, rf=3)

        ks_name = 'ks'
        table_name = 'cf'
        index = {'index_name': 'v_local_key', 'index_column': 'v', 'pk_name': 'key'}

        self.config_keyspace(session, ks_name, table_name, index, ks_create=False, global_index_name='v_global_key')

        local_data = {'Tel Aviv': [{'c': generate_random_text(), 'v': 'Dizzengof'},
                                   {'c': generate_random_text(), 'v': 'Geneva'}],
                      'Washington': [{'c': generate_random_text(), 'v': 'Southgate'},
                                     {'c': generate_random_text(), 'v': 'Dizzengof'},
                                     {'c': generate_random_text(), 'v': '10th'}],
                      'London': [{'c': generate_random_text(), 'v': 'Geneva'},
                                 {'c': generate_random_text(), 'v': 'Moorland'}],
                      'Vancouver': [{'c': generate_random_text(), 'v': '12th Ave'},
                                    {'c': generate_random_text(), 'v': 'Geneva'}]
                      }

        # Dictionary for filter by global index
        global_data = defaultdict(list)
        for key, indexes in local_data.items():
            for columns_data in indexes:
                global_data[columns_data['v']].append({'pk': key, 'c': columns_data['c']})

        # Insert data
        for key, columns in local_data.items():
            for columns_data in columns:
                query = "INSERT INTO {table_name} (key, c, v) VALUES ('{key}', '{c}', '{v}')". \
                    format(table_name=table_name, key=key, c=columns_data['c'], v=columns_data['v'])
                session.execute(query)

        # Filter by local index
        for key, columns in local_data.items():
            for columns_data in columns:
                ck = columns_data['c']
                index_value = columns_data['v']
                query = "SELECT key, c, v FROM {table_name} WHERE key='{key}' AND v='{index_value}'".format(**locals())
                assert_all(session=session, query=query, expected=[[key, ck, index_value]], cl=ConsistencyLevel.QUORUM,
                           ignore_order=True, num_attempts=30)

        # Filter by global index
        for index_value, columns in global_data.items():
            expected_result = [[row['pk'], row['c']] for row in columns]
            query = "SELECT key, c FROM {table_name} WHERE v='{index_value}'".format(**locals())
            assert_all(session=session, query=query, expected=expected_result, cl=ConsistencyLevel.QUORUM,
                       ignore_order=True, num_attempts=30)

    def test_query_data_created_before_local_index(self):
        """
        Create the index on the populated table and read the data that was inserted before index
        """
        session = self.prepare(user_table=True, nodes=4, rf=3)

        # insert data
        session.execute("INSERT INTO users (KEY, password, gender, state, birth_year) "
                        "VALUES ('user1', 'ch@ngem3a', 'f', 'TX', 1968);")
        session.execute("INSERT INTO users (KEY, password, gender, state, birth_year) "
                        "VALUES ('user2', 'ch@ngem3b', 'm', 'CA', 1971);")

        # create index
        assert self.create_and_build_index(create_local_index, self.cluster, session, ks_name='ks',
                                           table_name='users',
                                           index_column='gender', index_name='gender_key', pk_name='key',
                                           compaction=self.compaction_strategy), \
            'Index "gender_key" is not built'
        assert self.create_and_build_index(create_local_index, self.cluster, session, ks_name='ks',
                                           table_name='users',
                                           index_column='state', index_name='state_key', pk_name='key',
                                           compaction=self.compaction_strategy), \
            'Index "state_key" is not built'
        assert self.create_and_build_index(create_local_index, self.cluster, session, ks_name='ks',
                                           table_name='users',
                                           index_column='birth_year', index_name='birth_year_key', pk_name='key',
                                           compaction=self.compaction_strategy), \
            'Index "birth_year_key" is not built'

        # insert data
        session.execute("INSERT INTO users (KEY, password, gender, state, birth_year) "
                        "VALUES ('user3', 'ch@ngem3c', 'f', 'FL', 1978);")
        session.execute("INSERT INTO users (KEY, password, gender, state, birth_year) "
                        "VALUES ('user4', 'ch@ngem3d', 'm', 'TX', 1974);")

        assert_all(session, "select count(*) from users", expected=[[4]], cl=ConsistencyLevel.QUORUM)
        assert_all(session, "select count(*) from users where key='user4' and state='TX'", expected=[[1]],
                   cl=ConsistencyLevel.QUORUM)
        assert_all(session, "select count(*) from users where key='user2' and state='CA'", expected=[[1]],
                   cl=ConsistencyLevel.QUORUM)

    def test_query_data_by_ck_and_local_index(self):
        """
        Filter data by primary and clustering keys and secondary index
        """
        ks_name = 'ks'
        table_name = 'cf'
        index_column = 'v'
        index_name = 'v_inx'

        session = self.prepare(nodes=4, rf=3)
        create_cf(session, '{0}.{1}'.format(ks_name, table_name), key_type='text',
                  compaction={'class': self.compaction_strategy})

        # insert data
        session.execute("INSERT INTO {} (key, c, v) VALUES ('user1', 'ch@ngem3a', 'f')".format(table_name))
        session.execute("INSERT INTO {} (key, c, v) VALUES ('user2', 'ch@ngem3b', 'm')".format(table_name))
        session.execute("INSERT INTO {} (key, c, v) VALUES ('user3', 'ch@ngem3c', 'f')".format(table_name))
        session.execute("INSERT INTO {} (key, c, v) VALUES ('user4', 'ch@ngem3d', 'm')".format(table_name))

        # create index
        assert self.create_and_build_index(create_local_index, self.cluster, session, ks_name=ks_name,
                                           table_name=table_name, index_column=index_column,
                                           index_name=index_name, pk_name='key',
                                           compaction=self.compaction_strategy), \
            f'Index {index_name} is not built'

        assert_all(session, "select count(*) from {}".format(table_name), expected=[[4]], cl=ConsistencyLevel.QUORUM)
        assert_all(session, "select count(*) from {} where key='user2' and c='ch@ngem3b' and v='m'".format(table_name),
                   expected=[[1]], cl=ConsistencyLevel.ALL)
        assert_all(session, "select count(*) from {} where KEY='user1' and c='ch@ngem3a' and v='m'".format(table_name),
                   expected=[[0]], cl=ConsistencyLevel.QUORUM)

    def test_insert_data_after_recreating_ks_with_local_index(self):
        """
        Data inserted immediately after dropping and recreating a keyspace with an indexed column familiy is not
        included in the index.
        """
        session = self.prepare(nodes=4, rf=3)

        ks_name = 'ks'
        table_name = 'cf'
        index = {'index_name': 'v_ind', 'index_column': 'v', 'pk_name': 'key'}
        self.config_keyspace(session, ks_name, table_name, index, ks_create=False)

        for i in range(10):
            logger.debug(f"round {i}")
            try:
                session.execute("DROP KEYSPACE {}".format(ks_name))
            except ConfigurationException:
                pass

            self.config_keyspace(session, ks_name, table_name, index)

            for r in range(10):
                session.execute(f"INSERT INTO {ks_name}.{table_name} (key, c, v) VALUES ('{r}', "
                                f"'{generate_random_text()}','asdf')")

            wait_for_schema_agreement(session)
            time.sleep(30)
            for r in range(10):
                assert_all(session,
                           f"select count(*) from {ks_name}.{table_name} WHERE key='{r}' and v='asdf'",
                           expected=[[1]], cl=ConsistencyLevel.QUORUM)

    @pytest.mark.parametrize("value_length,expect_message", [
        (OVERSIZE_LENGTH, "Key size too large"),
        (LONG_TEXT_LENGTH, None),
    ], ids=["oversize", "long"])
    def test_local_indexed_values(self, value_length, expect_message):
        """
        First test:
         Reject inserts & updates where values of any indexed column is > 64k

        Second test:
          Correct inserts & updates where values of any indexed column is long and up to 64k
        """
        # This test is negative, and there are errors in the cluster' logs.
        # The teardown fails because it expects a cluster doesn't contain errors if the test is passed.
        if OVERSIZE_LENGTH == value_length:
            self.ignore_log_patterns += [
                r".*std::runtime_error \(Key size too large: .*? > 65535\).*",
            ]
        self.ignore_log_patterns += [expect_message]

        session = self.prepare(nodes=4, rf=3)
        test = 'oversize' if value_length == OVERSIZE_LENGTH else 'long'

        logger.debug('Insert {} value into non-PK column'.format(test))
        self.insert_row_with_long_value(
            "CREATE TABLE %s(a int, b int, c varchar, PRIMARY KEY (a)) "
            "WITH compaction = %s",
            "CREATE INDEX ON %s ((a), c)",
            "INSERT INTO %s (a, b, c) VALUES (0, 0, ?)",
            session, column_name='c', value_length=value_length,
            expect_message=expect_message)

        logger.debug('Insert {} value into clustering key column'.format(test))
        self.insert_row_with_long_value(
            "CREATE TABLE %s(a int, b text, c int, PRIMARY KEY (a, b)) "
            "WITH compaction = %s",
            "CREATE INDEX ON %s ((a), b)",
            "INSERT INTO %s (a, b, c) VALUES (0, ?, 0)",
            session, column_name='b', value_length=value_length,
            expect_message=expect_message)

        logger.debug('Table with compact storage. Insert {} value into non-PK column'.format(test))
        self.insert_row_with_long_value(
            "CREATE TABLE %s(a int, b text, PRIMARY KEY (a)) "
            "WITH COMPACT STORAGE and compaction = %s",
            "CREATE INDEX ON %s ((a), b)",
            "INSERT INTO %s (a, b) VALUES (0, ?)",
            session, column_name='b', value_length=value_length,
            expect_message=expect_message)

    def test_drop_local_index_while_building(self):
        """
        Asserts that indexes deleted before they have been completely build are invalidated and not built after restart
        """
        keyspace_name = 'keyspace1'
        table_name = 'standard1'
        index_name = 'idx'
        index_column = '"C0"'

        session = self.prepare(nodes=4, rf=3, keyspace_name=keyspace_name)
        node = self.cluster.nodelist()[0]

        # Create some thousands of rows to guarantee a long index building
        node.stress(['write', 'n=50K', 'no-warmup', '-schema', 'replication(factor=3)',
                     'compaction(strategy={})'.format(self.compaction_strategy)])

        # Create a local index and immediately drop it, without waiting for index building
        create_local_index(session=session, table_name=table_name, index_column=index_column,
                           index_name=index_name, pk_name='key', compaction=self.compaction_strategy)

        # Get view ID
        index_view_name = get_index_view_name(index_name)
        view_id = get_view_id(session=session, keyspace_name=keyspace_name, view_name=index_view_name)
        logger.debug('View ID: {}'.format(view_id))

        session.execute('DROP INDEX {}'.format(index_name))

        self.cluster.wait_for_compactions()

        # Check that the index is not marked as built nor queryable
        assert_none(session, view_built_status_query(ks=keyspace_name, view=index_view_name))
        assert_invalid(session, 'SELECT * FROM {0} WHERE {1} = 0x00'.format(table_name, index_column),
                       matching='use ALLOW FILTERING', expected=Exception)

        exclude_errors = ['Can\'t find a column family with UUID {}'.format(view_id),
                          'mutation_write_failure_exception']
        self.ignore_log_patterns += exclude_errors

        # Restart the node to trigger any eventual unexpected index rebuild
        session = self.drain_and_restart_node(node, keyspace_name)

        # The index should remain not built nor queryable after restart
        assert_none(session, view_built_status_query(ks=keyspace_name, view=index_view_name))
        assert_invalid(session, 'SELECT * FROM {0} WHERE {1} = 0x00'.format(table_name, index_column),
                       matching='use ALLOW FILTERING', expected=Exception)

        self.check_errors(node, exclude_errors=exclude_errors)

    def test_truncate_base_with_local_index(self):
        """
        asserts that truncating base table will result in truncating secondary index as well
        """

        def select_by_index(expected_count):
            smt = "SELECT count(*) FROM {0} WHERE {1} = '{2}' and key = {3}"
            for data in data_set:
                assert_all(session, smt.format(table_name, index_column, data[1], data[0]), expected=[[expected_count]],
                           cl=ConsistencyLevel.QUORUM)

        keyspace_name = 'ks'
        table_name = 'tbl'
        index_name = 'ix_tbl_c0'
        index_column = 'c0'
        pk_name = 'key'

        session = self.prepare(nodes=4, rf=3, keyspace_name=keyspace_name)

        create_cf(session, table_name, key_type='int', columns={'c0': 'text', 'c1': 'text', 'c2': 'text'},
                  compaction={'class': self.compaction_strategy})

        assert self.create_and_build_index(create_local_index, self.cluster, session, keyspace_name,
                                           table_name, index_column, index_name, pk_name=pk_name,
                                           compaction=self.compaction_strategy), \
            'Index %s is not built' % index_name

        data_set = [[0, 'a', 'b'], [1, 'a', 'b'], [2, 'q', 'b'], [3, 'a', 'e'], [4, 'a', 'e']]
        smt = "INSERT INTO {table_name} (key, c0, c1) values ({pk}, '{c0}', '{c1}')"
        for data in data_set:
            session.execute(smt.format(table_name=table_name, pk=data[0], c0=data[1], c1=data[2]))

        # ensure sstables are created and will be dropped
        self.cluster.flush()

        # ensure data is loaded into cache and the cache will be cleared
        select_by_index(1)
        assert_row_count(session, "tbl", 5)

        session.execute("TRUNCATE table tbl")
        assert_row_count(session, "tbl", 0)

        # check that index queries are also truncated
        select_by_index(0)

    @pytest.mark.single_node
    def test_multi_column_local_index(self):
        """
        Test that impossible to create secondary index on the few columns and valid error message is received
        """
        keyspace_name = 'ks'
        table_name = 'cf'
        index_columns = {'b': 'int', 'c': 'int'}

        session = self.prepare(nodes=1, rf=1, keyspace_name=keyspace_name)

        # try to create index on 2 columns
        create_cf(session, table_name, key_type='int', columns=index_columns,
                  compaction={'class': self.compaction_strategy})
        assert_expected_error(func=create_local_index,
                              expected_error='Only CUSTOM indexes support multiple columns',
                              args=(session, table_name, 'key', index_columns.keys()),
                              kwargs={'index_name': 'two_columns_index', 'compaction': self.compaction_strategy})

        # try to create index on 6 columns
        table_name = 'cf_6columns'
        index_columns = {'b': 'int', 'c': 'int', 'd': 'int', 'e': 'int', 'f': 'int', 'g': 'int'}
        create_cf(session, table_name, key_type='int', columns=index_columns,
                  compaction={'class': self.compaction_strategy})
        assert_expected_error(func=create_local_index, expected_error='Only CUSTOM indexes support multiple columns',
                              args=(session, table_name, 'key', index_columns),
                              kwargs={'index_name': 'six_columns_index', 'compaction': self.compaction_strategy})

    def _prepare_for_ttl(self):
        keyspace_name = 'ks'
        table_name = 'cf'
        index_column = 'b'
        index_name = '{}_inx'.format(index_column)
        select_query = 'select * from {} where {} key = 0'
        mv_query = 'select key, {} from {}'.format(index_column, get_index_view_name(index_name))

        session = self.prepare(nodes=1, rf=1, keyspace_name=keyspace_name)

        create_cf(session, table_name, key_type='int', columns={'b': 'int', 'c': 'int'},
                  compaction={'class': self.compaction_strategy})
        session.execute("INSERT INTO {} (key, b, c) VALUES (0, 1, 2)".format(table_name))
        assert_all(session, select_query.format(table_name, ''), [[0, 1, 2]], cl=ConsistencyLevel.ALL)

        assert self.create_and_build_index(create_local_index, self.cluster, session, keyspace_name,
                                           table_name, index_column, index_name, pk_name='key',
                                           compaction=self.compaction_strategy), \
            'Index %s is not built' % index_name
        assert_all(session, select_query.format(table_name, '%s = %d and' %
                                                (index_column, 1)), [[0, 1, 2]], cl=ConsistencyLevel.ALL)
        return session, keyspace_name, table_name, index_column, index_name, select_query, mv_query

    @pytest.mark.single_node
    def test_ttl_local_index_column(self):
        """
        Verify SI with default_time_to_live can be deleted properly using expired livenessInfo
        """
        session, keyspace_name, table_name, index_column, index_name, select_query, mv_query = self._prepare_for_ttl()

        ttl = 60
        logger.debug('Update index column with TTL {}'.format(ttl))
        session.execute("UPDATE {} USING TTL {} SET {}=3 WHERE key=0".format(table_name, ttl, index_column))
        assert_all(session, select_query.format(table_name, '%s = %d and' % (index_column, 3)), [[0, 3, 2]],
                   cl=ConsistencyLevel.ALL)
        assert_none(session, select_query.format(table_name, '%s = %d and' % (index_column, 1)),
                    cl=ConsistencyLevel.ALL)
        assert_all(session, mv_query, [[0, 3]], cl=ConsistencyLevel.ALL)

        time.sleep(ttl + 5)
        # Validate that no record is returned when filtered by index
        assert_all(session, select_query.format(table_name, ''), [[0, None, 2]], cl=ConsistencyLevel.ALL)
        assert_none(session, select_query.format(table_name, '%s = %d and' % (index_column, 3)),
                    cl=ConsistencyLevel.ALL)
        assert_none(session, select_query.format(table_name, '%s = %d and' % (index_column, 1)),
                    cl=ConsistencyLevel.ALL)
        assert_none(session, mv_query, cl=ConsistencyLevel.ALL)

    def test_delete_local_indexed_rows(self):
        """
        Delete rows from indexed table and read data by index
        """
        keyspace_name = 'ks'
        table_name = 'cf'
        index_name = 'b_index'
        index_column = 'b'

        session = self.prepare(nodes=4, rf=3, keyspace_name=keyspace_name, session_node=3)

        create_cf(session, table_name, key_type='int', columns={'b': 'int'},
                  compaction={'class': self.compaction_strategy})
        assert self.create_and_build_index(create_local_index, self.cluster, session,
                                           ks_name=keyspace_name, table_name=table_name,
                                           index_column=index_column, index_name=index_name, pk_name='key',
                                           compaction=self.compaction_strategy), \
            'Index %s is not built' % index_name

        num_rows = 100
        for i in range(num_rows):
            indexed_value = i + 100
            session.execute(
                "INSERT INTO {}.{} (key, b) VALUES ({}, {})".format(keyspace_name, table_name, i, indexed_value))

        self.cluster.flush()

        # Delete 10 rows by index
        logger.debug('Delete 10 rows by index')
        start_key, delete_num = 30, 10
        rows_for_delete = list(range(start_key, start_key + delete_num))
        for i in rows_for_delete:
            session.execute("DELETE FROM {} WHERE key = {}".format(table_name, i))
        time.sleep(30)

        # Valudate the data in not in table
        assert_row_count(session, table_name=table_name, expected=num_rows - delete_num,
                         consistency_level=ConsistencyLevel.ALL)
        query = 'select key, b from {} where key={} and {}={}'
        for i in list(range(num_rows)):
            if i in rows_for_delete:
                assert_none(session, query=query.format(table_name, i, index_column, i + 100),
                            cl=ConsistencyLevel.ALL)
            else:
                res = [[i, i + 100]]
                assert_all(session, query=query.format(table_name, i, index_column, i + 100), expected=res,
                           cl=ConsistencyLevel.ALL)

        # Valudate the data in not in table and SI materialized view
        assert_row_count(session, table_name=get_index_view_name(index_name), expected=num_rows - delete_num,
                         consistency_level=ConsistencyLevel.ALL)

    @pytest.mark.next_gating
    # @pytest.mark.dtest_debug - https://github.com/scylladb/scylla/issues/4384
    def test_stop_node_during_local_index_build(self):
        """
        Stop one node during index building and read data by index
        """
        self._node_action_during_index_build(node_action='stop', nodes=4, rf=3, num_rows=100000)

    def test_remove_node_during_local_index_build(self):
        """
        Remove one node during index building and read data by index
        """
        self._node_action_during_index_build(node_action='remove', nodes=4, rf=3, num_rows=100000)

    def test_decommission_node_during_local_index_build(self):
        """
        Decommission one node during index building and read data by index
        """
        self._node_action_during_index_build(node_action='decommission', nodes=4, rf=3, num_rows=100000)

    def test_add_node_during_local_index_build(self):
        """
        Add one node during index building and read data by index
        """
        self._node_action_during_index_build(node_action='add', nodes=3, rf=3, num_rows=100000)

    def _node_action_during_index_build(self, node_action, nodes, rf, num_rows):
        keyspace_name = 'ks'
        table_name = 'cf'
        index_name = 'b_index'
        index_column = 'b'
        view_name = get_index_view_name(index_name)

        session = self.prepare(nodes=nodes, rf=rf, keyspace_name=keyspace_name, session_node=3)
        node2 = self.cluster.nodelist()[1]
        node2_ip = list(node2.network_interfaces['binary'])[0].replace('.', r'\.')

        create_cf(session, table_name, key_type='int',
                  columns={'b': 'int'}, compaction={'class': self.compaction_strategy})

        statement = session.prepare("INSERT INTO {}.{} (key, b) VALUES (?, ?)".format(keyspace_name, table_name))
        statement.consistency_level = ConsistencyLevel.QUORUM

        execute_concurrent_with_args(session, statement,
                                     map(lambda k: [k] + [k + num_rows], [k for k in range(0, num_rows)]))
        self.cluster.flush()

        # Create index and wait while the build is starting
        create_local_index(session, table_name, 'key', index_column, index_name,
                           compaction=self.compaction_strategy)
        wait_for_view_build_start(session, ks=keyspace_name, view=view_name)

        exclude_errors = [f'Can\'t send migration request: node {node2_ip} is down',
                          rf'Error applying view update to {node2_ip}.*: exceptions::unavailable_exception \(Cannot achieve consistency level for cl ONE. Requires 1, alive 0\)'
                          r'\(Cannot achieve consistency level for cl ONE. Requires 1, alive 0\)',
                          r'Operation timed out for ks\.b_index_index - received only 0 responses from 1 CL=ONE']
        self.ignore_log_patterns += exclude_errors

        # Perform action on second node
        self.node_action_with_delay(node_action, node2)

        # Index will not finish building, because view building underneath is paused until updates can be sent.
        if node_action == 'add':
            assert True

        if node_action in ['remove', 'decommission']:
            logger.debug('Add new node')
            session = self.add_new_node(node_index=nodes + 1)
            session.execute('USE {}'.format(keyspace_name))
        elif node_action == 'stop':
            logger.debug('Start node {}'.format(node2.name))
            node2.start(wait_for_binary_proto=True)

        # Validate the data using filtering by index with cl=ONE
        self.validate_index_data(session, cl=ConsistencyLevel.ONE, num_rows=num_rows, table_name=table_name,
                                 index_column=index_column)

        # Validate view rows
        assert_row_count_in_select(session=session, query="SELECT * FROM {}".format(view_name),
                                   num_rows_expected=num_rows, consistency_level=ConsistencyLevel.QUORUM)

        self.check_errors(self.cluster.nodelist()[0], exclude_errors=exclude_errors, regex=True)

    def test_stop_node_after_local_index_build(self):
        """
        Stop one node after index building and read data by index
        """
        self._node_action_after_index_build(node_action='stop', nodes=4, rf=3, num_rows=1000)

    def test_remove_node_after_local_index_build(self):
        """
        Remove one node after index building and read data by index
        """
        self._node_action_after_index_build(node_action='remove', nodes=4, rf=3, num_rows=1000)

    def test_decommission_node_after_local_index_build(self):
        """
        Decommission one node after index building and read data by index
        """
        self._node_action_after_index_build(node_action='decommission', nodes=4, rf=3, num_rows=1000)

    def test_add_node_after_local_index_build(self):
        """
        Decommission one node after index building and read data by index
        """
        self._node_action_after_index_build(node_action='add', nodes=3, rf=3, num_rows=1000)

    def _node_action_after_index_build(self, node_action, nodes, rf, num_rows):
        keyspace_name = 'ks'
        table_name = 'cf'
        index_name = 'b_index'
        index_column = 'b'
        view_name = get_index_view_name(index_name)

        session = self.prepare(nodes=nodes, rf=rf, keyspace_name=keyspace_name, session_node=3)
        node2 = self.cluster.nodelist()[1]
        node2_ip = list(node2.network_interfaces['binary'])[0]

        create_cf(session, table_name, key_type='int',
                  columns={'b': 'int'}, compaction={'class': self.compaction_strategy})

        statement = session.prepare("INSERT INTO {}.{} (key, b) VALUES (?, ?)".format(keyspace_name, table_name))
        statement.consistency_level = ConsistencyLevel.QUORUM

        execute_concurrent_with_args(session, statement,
                                     map(lambda k: [k] + [k + num_rows], [k for k in range(0, num_rows)]))
        self.cluster.flush()

        # Create index and wait while the index is built
        assert self.create_and_build_index(create_local_index, self.cluster, session,
                                           ks_name=keyspace_name, table_name=table_name, index_name=index_name,
                                           index_column=index_column, pk_name='key',
                                           compaction=self.compaction_strategy), \
            'Index %s is not built' % index_name

        exclude_errors = ['Can\'t send migration request: node {} is down'.format(node2_ip)]
        self.ignore_log_patterns += exclude_errors

        # Perform action on second node
        self.node_action_with_delay(node_action, node=node2)

        # Validate the data using filtering by index
        self.validate_index_data(session, cl=ConsistencyLevel.ONE, num_rows=num_rows, table_name=table_name,
                                 index_column=index_column)

        # Validate view rows
        assert_row_count_in_select(session=session, query="SELECT * FROM {}".format(view_name),
                                   num_rows_expected=num_rows, consistency_level=ConsistencyLevel.QUORUM)

        self.check_errors(self.cluster.nodelist()[0], exclude_errors)


@pytest.mark.dtest_full
class TestMultipleSecondaryIndexes(Tester, SecondaryIndexesHelpers):
    def _prepare_for_multi_index_test(self):
        session = self.prepare(user_table=False, nodes=4, rf=3, keyspace_name='ks')
        session.consistency_level = 'ONE'
        session.execute("CREATE TABLE test_table (row varchar PRIMARY KEY, name varchar, value int);")
        assert self.create_and_build_index(create_index, self.cluster, session, 'ks', 'test_table',
                                           'name', 'name_idx'), 'Index name_idx is not built'
        assert self.create_and_build_index(create_index, self.cluster, session, 'ks', 'test_table',
                                           'value', 'value_idx'), 'Index value_idx is not built'
        stmt_insert = session.prepare("INSERT INTO test_table (row, name, value) VALUES (?, ?, ?)")
        for rec in [
            ['AAA1', 'AAAA', 0],
            ['AAA2', 'AAAA', 100],
            ['AAA3', 'XXXX', 0],
            ['AAA4', 'XXXX', 100],
            ['AAA5', 'AAAA', 0],
            ['AAA6', 'AAAA', 100],
            ['AAA7', 'XXXX', 0],
            ['AAA8', 'XXXX', 100],
        ]:
            session.execute(stmt_insert, rec)
        self._use_filtering_error_message = \
            "Cannot execute this query as it might involve data filtering and thus may have unpredictable " \
            "performance. If you want to execute this query despite the performance unpredictability, " \
            "use ALLOW FILTERING"
        return session

    def test_multy_secondary_query_with_no_pk(self):
        """
        Test against table with multiple secondary indexes, queries have no primary index field in WHERE clause
        """
        session = self._prepare_for_multi_index_test()
        assert_all(
            session,
            "SELECT * FROM test_table WHERE name='AAAA'",
            expected=[['AAA2', 'AAAA', 100], ['AAA1', 'AAAA', 0], ['AAA6', 'AAAA', 100], ['AAA5', 'AAAA', 0]],
            ignore_order=True)
        assert_all(
            session,
            "SELECT * FROM test_table WHERE name='XXXX'",
            expected=[['AAA7', 'XXXX', 0], ['AAA8', 'XXXX', 100], ['AAA4', 'XXXX', 100], ['AAA3', 'XXXX', 0]],
            ignore_order=True)
        assert_all(
            session,
            "SELECT * FROM test_table WHERE value=0",
            expected=[['AAA7', 'XXXX', 0], ['AAA1', 'AAAA', 0], ['AAA3', 'XXXX', 0], ['AAA5', 'AAAA', 0]],
            ignore_order=True)
        assert_all(
            session,
            "SELECT * FROM test_table WHERE value=100",
            expected=[['AAA2', 'AAAA', 100], ['AAA8', 'XXXX', 100], ['AAA4', 'XXXX', 100], ['AAA6', 'AAAA', 100]],
            ignore_order=True)
        assert_invalid(session,
                       "SELECT * FROM test_table WHERE name='AAAA' and value=0",
                       self._use_filtering_error_message
                       )
        assert_all(
            session,
            "SELECT * FROM test_table WHERE name='AAAA' and value=0 ALLOW FILTERING",
            expected=[['AAA1', 'AAAA', 0], [u'AAA5', 'AAAA', 0]],
            ignore_order=True)
        assert_invalid(session,
                       "SELECT * FROM test_table WHERE name='AAAA' and value=100",
                       self._use_filtering_error_message
                       )
        assert_all(
            session,
            "SELECT * FROM test_table WHERE name='AAAA' and value=100 ALLOW FILTERING",
            expected=[['AAA2', 'AAAA', 100], ['AAA6', 'AAAA', 100]],
            ignore_order=True)
        assert_invalid(session,
                       "SELECT * FROM test_table WHERE name='XXXX' and value=0",
                       self._use_filtering_error_message
                       )
        assert_all(
            session,
            "SELECT * FROM test_table WHERE name='XXXX' and value=0 ALLOW FILTERING",
            expected=[['AAA7', 'XXXX', 0], ['AAA3', 'XXXX', 0]],
            ignore_order=True)
        assert_invalid(session,
                       "SELECT * FROM test_table WHERE name='XXXX' and value=100",
                       self._use_filtering_error_message
                       )
        assert_all(
            session,
            "SELECT * FROM test_table WHERE name='XXXX' and value=100 ALLOW FILTERING",
            expected=[['AAA8', 'XXXX', 100], ['AAA4', 'XXXX', 100]],
            ignore_order=True)

    def test_multy_secondary_query_with_pk(self):
        """
        Test against table with multiple secondary indexes, queries have primary index field in WHERE clause
        """
        session = self._prepare_for_multi_index_test()
        assert_invalid(session,
                       "SELECT * FROM test_table WHERE row='AAA1' and name='AAAA' and value=0",
                       self._use_filtering_error_message
                       )
        assert_all(
            session,
            "SELECT * FROM test_table WHERE row='AAA1' and name='AAAA' and value=0 ALLOW FILTERING",
            expected=[['AAA1', 'AAAA', 0]],
            ignore_order=True)
        assert_all(
            session,
            "SELECT * FROM test_table WHERE row='AAA1' and name='AAAA'",
            expected=[['AAA1', 'AAAA', 0]],
            ignore_order=True)
        assert_all(
            session,
            "SELECT * FROM test_table WHERE row='AAA1' and value=0",
            expected=[['AAA1', 'AAAA', 0]],
            ignore_order=True)
        assert_all(
            session,
            "SELECT * FROM test_table WHERE row='AAA1'",
            expected=[['AAA1', 'AAAA', 0]],
            ignore_order=True)


class DtestTimeoutError(Exception):
    pass
