import collections

import os
import sys
import time
import traceback
import random
from functools import partial
from multiprocessing import Process, Queue, cpu_count, Lock

import pytest
from pkg_resources import parse_version

from concurrent.futures import ThreadPoolExecutor
from cassandra import ConsistencyLevel, WriteFailure, consistency_value_to_name
from cassandra.cluster import Cluster, Session
from cassandra.query import SimpleStatement
from enum import Enum  # Remove when switching to py3

from tools.assertions import assert_all, assert_one, assert_invalid, assert_unavailable, assert_none, \
    assert_all_or_none, assert_crc_check_chance_equal, assert_row_count, assert_two_queries_equal, \
    assert_two_queries_equal_ignore_order, assert_row_count_in_select

from dtest_class import Tester, wait_for, create_ks, create_cf
from tools.retrying import retrying
from tools.data import run_in_parallel, rows_to_list, run_query_with_data_processing
from tools.misc import flush_by_node, remove_node
from tools.tables_view_manager import wait_for_view_build_start, wait_for_view, TableManager, MaterializedViewManager
from cassandra.cluster import NoHostAvailable
from ccmlib.scylla_cluster import ScyllaCluster
from ccmlib.node import NodetoolError
from tools.stress import format_cs_output, assert_cs_success

import logging

logger = logging.getLogger(__name__)

# CASSANDRA-10978. Migration wait (in seconds) to use in bootstrapping tests. Needed to handle
# pathological case of flushing schema keyspace for multiple data directories. See CASSANDRA-6696
# for multiple data directory changes and CASSANDRA-10421 for compaction logging that must be
# written.
MIGRATION_WAIT = 5


class CommonUtils(Tester):
    def setup(self):
        if not 'debug_mode' in self.__dict__.keys():
            self.debug_mode = isinstance(self.cluster, ScyllaCluster) and self.cluster.scylla_mode == "debug"
            self.session_timeout = 120
            if self.debug_mode:
                self.session_timeout *= 3

    @staticmethod
    def eventually(fun, trials=64):
        """
        Runs a function until it succeeds or the trial limit is reached
        """
        assert trials > 0
        for i in range(trials - 1):
            try:
                return fun()
            except Exception as e:
                logger.debug("{} [{}/{}]: {}: will retry in 1 second".format(fun.__name__, i + 1, trials, e))
                time.sleep(1)
        return fun()

    def eventually_assert_one(self, *args):
        """
        Shortcut for eventually(lambda: assert_one(args))
        """
        return self.eventually(lambda: assert_one(*args))  # pylint: disable=no-value-for-parameter

    def eventually_assert_none(self, *args):
        """
        Shortcut for eventually(lambda: assert_one(args))
        """
        return self.eventually(lambda: assert_none(*args))  # pylint: disable=no-value-for-parameter

    def stop_cluster(self):
        # Currently, (See issues #4019 and #3966), shutdown may hang for up
        # 5 minutes (the timeout set in storage_proxy::send_to_endpoint())
        # while a view build step is stuck trying to communicate with another
        # node we previously killed. So we need to increase stop()'s timeout to
        # be more than 5 minutes (=300 seconds).
        self.cluster.stop(wait_seconds=360)

    def prepare(self, user_table: bool = False, rf: str = 1, options: dict = None, nodes: int = 3,
                fetch_size: int = None, jvm_args: list = None, **kwargs):
        self.setup()
        cluster = self.cluster
        populate = nodes if isinstance(nodes, list) else [nodes, 0]
        cluster.populate(populate)
        self.rf = sum([v for v in rf.values()]) if isinstance(rf, dict) else rf
        if options:
            logger.debug(f"Setting cluster configuration options: {options}")
            cluster.set_configuration_options(values=options)
        cluster.start(jvm_args=jvm_args, wait_other_notice=True, wait_for_binary_proto=True)
        node1 = cluster.nodelist()[0]

        session = self.patient_cql_connection(node1, **kwargs)
        if fetch_size:
            session.default_fetch_size = fetch_size
        create_ks(session, 'ks', rf)

        if user_table:
            session.execute(
                ("CREATE TABLE users (username varchar, password varchar, gender varchar, "
                 "session_token varchar, state varchar, birth_year bigint, "
                 "PRIMARY KEY (username));")
            )

            # create a materialized view
            session.execute(("CREATE MATERIALIZED VIEW users_by_state AS "
                             "SELECT * FROM users WHERE STATE IS NOT NULL AND username IS NOT NULL "
                             "PRIMARY KEY (state, username)"))

        return session

    def update_view(self, session, query, flush, compact=False):
        session.execute(query)
        # Scylla doesn't rely on the batchlog
        # self._replay_batchlogs()
        if flush:
            self.cluster.flush()
        if compact:
            self.cluster.compact()

    @staticmethod
    def _insert_data(session):
        # insert data
        insert_stmt = "INSERT INTO users (username, password, gender, state, birth_year) VALUES "
        session.execute(insert_stmt + "('user1', 'ch@ngem3a', 'f', 'TX', 1968);")
        session.execute(insert_stmt + "('user2', 'ch@ngem3b', 'm', 'CA', 1971);")
        session.execute(insert_stmt + "('user3', 'ch@ngem3c', 'f', 'FL', 1978);")
        session.execute(insert_stmt + "('user4', 'ch@ngem3d', 'm', 'TX', 1974);")

    def _replay_batchlogs(self):
        logger.debug("Replaying batchlog on all nodes")
        for node in self.cluster.nodelist():
            if node.is_running():
                node.nodetool("replaybatchlog")

    def _ensure_view_building_did_not_finish(self, all_started_view_build_processes):
        have_finished = 0
        for node in self.cluster.nodelist():
            finished = node.grep_log("Finished building view")
            have_finished += len(finished)
        if have_finished >= all_started_view_build_processes:
            # TODO(sarna): Once it's possible to actually ensure view building haven't finished,
            # e.g. by injecting waiting for it in Scylla, this function should start asserting
            # instead of just warning.
            logger.debug("View building finished too soon! nodes finished = {}, all build processes = {}".format(
                have_finished, all_started_view_build_processes))


@pytest.mark.dtest_full
class TestMaterializedViews(CommonUtils):
    """
    Test materialized views implementation.
    @jira_ticket CASSANDRA-6477
    """

    def test_stop_node_during_mv_insert_4_nodes(self):
        """ Test stopping node during MV inserts
            Test starts with a starting size 4 and stops one node during inserts into base table that cause to update materialized view as well
            (using cs_mv_profile.yaml profile).
            Validate the log has no errors.
            Issue #2783: there are mutation_write_timeout_exception in case starting size 4 and more
        """
        self._run_node_failure_during_mv_stress_insert(rf=3, nodes=4, node_action='stop', exclude_errors=[
                                                       'mutation_write_timeout_exception'])

    def test_stop_node_during_mv_insert_3_nodes(self):
        """ Test stopping node during MV inserts
            Test starts with a starting size 3 and stops one node during inserts into base table that cause to update materialized view as well
            (using cs_mv_profile.yaml profile).
            Validate the log has no errors
        """
        self._run_node_failure_during_mv_stress_insert(rf=3, nodes=3, node_action='stop', exclude_errors=[
            'mutation_write_timeout_exception'])

    def test_remove_node_during_mv_insert_4_nodes(self):
        """ Test removing node during MV inserts
            Test starts with a starting size 4 and removes one node during inserts into base table that cause to update materialized view as well
            (using cs_mv_profile.yaml profile).
            Validate the log has no errors.
            Issue #2783: there are mutation_write_timeout_exception in case starting size 4 and more
        """
        self._run_node_failure_during_mv_stress_insert(rf=3, nodes=4, node_action='remove', exclude_errors=[
                                                       'mutation_write_timeout_exception'])

    def test_decommission_node_during_mv_insert_4_nodes(self):
        """ Test removing node during MV inserts
            Test starts with a starting size 4 and removes one node during inserts into base table that cause to update materialized view as well
            (using cs_mv_profile.yaml profile).
            Validate the log has no errors.
            Issue #2783: there are mutation_write_timeout_exception in case starting size 4 and more
        """
        self._run_node_failure_during_mv_stress_insert(rf=3, nodes=4, node_action='decommission', exclude_errors=[
                                                       'mutation_write_timeout_exception'])

    @pytest.mark.next_gating
    def test_remove_node_during_mv_insert_3_nodes(self):
        """ Test removing node during MV inserts
            Test starts with a starting size 3 and removes one node during inserts into base table that cause to update materialized view as well
            (using cs_mv_profile.yaml profile).
            Validate the log has no errors.
        """
        self._run_node_failure_during_mv_stress_insert(rf=3, nodes=3, node_action='remove', exclude_errors=[
                                                       'mutation_write_timeout_exception'])

    def test_double_node_failure_during_mv_insert_4_nodes(self):
        """ Test stopping 2 nodes during MV inserts
            Test starts with a starting size 4 and stops 2 nodes during inserts into base table that cause to update materialized view as well
            (using cs_mv_profile.yaml profile).
            Validate the log has no errors.
            Issue #2783: there are mutation_write_timeout_exception in case starting size 4 and more
        """
        self._run_node_failure_during_mv_stress_insert(rf=3, nodes=4, node_action='stop', duration='2m',
                                                       double_failure=True,
                                                       exclude_errors=['mutation_write_timeout_exception'])

    def test_double_node_failure_during_mv_insert_3_nodes(self):
        """ Test stopping 2 nodes during MV inserts
            Test starts with a starting size 4 and stops 2 nodes during inserts into base table that cause to update materialized view as well
            (using cs_mv_profile.yaml profile).
            Validate the log has no errors.
            Issue #2783: there are mutation_write_timeout_exception in case starting size 4 and more
        """
        self._run_node_failure_during_mv_stress_insert(rf=3, nodes=3, node_action='stop', duration='2m',
                                                       double_failure=True,
                                                       exclude_errors=['mutation_write_timeout_exception'])

    def _run_node_failure_during_mv_stress_insert(self, rf, nodes, node_action, delay=30, duration='1m',
                                                  double_failure=False, exclude_errors=None):
        session = self.prepare(rf=rf, nodes=nodes)
        mv_profile = os.path.abspath(os.path.join("test_data", 'cassandra-mv-profile', 'cs_mv_profile.yaml'))

        node1 = self.cluster.nodelist()[0]
        n = 10000
        results = node1.stress(stress_options=['write', 'cl=QUORUM', 'n={}'.format(n),
                                               "-schema replication(factor=3)", "-mode cql3 native",
                                               "-rate threads=10", "-pop seq=1..{}".format(n)],
                               capture_output=True)
        logger.debug(format_cs_output(results))
        assert_cs_success(results)

        self.fixture_dtest_setup.ignore_log_patterns += [
            r'view - Error applying view update to .*: seastar::broken_promise']

        other_nodes = self.cluster.nodelist()
        nodes_to_start = [other_nodes.pop(1)]
        if double_failure and len(self.cluster.nodelist()) > 2:
            nodes_to_start.append(other_nodes.pop(1))

        proc_functions = [
            {'func': node1.stress,
             'args': [['user', 'profile={}'.format(mv_profile), 'cl=ONE', 'duration={}'.format(duration),
                       'ops(insert=1,read1=1,read2=1,read3=1)', '-mode cql3  native', '-rate threads=10'], True]},
            {'func': node1.stress,
             'args': [['mixed', "cl=ONE", "duration={}".format(duration), "-schema replication(factor=3)",
                       "-mode cql3 native", "-rate threads=10", "-pop seq=1..{}".format(n), "-log interval=5"], True]},
            {'func': self._node_action_with_delay, 'args': (node_action, nodes_to_start[0]), 'kwargs': {
                'delay': delay, 'other_nodes': other_nodes}}
        ]
        if double_failure and len(self.cluster.nodelist()) > 2:
            proc_functions.append({'func': self._node_action_with_delay, 'args': (node_action, nodes_to_start[1]),
                                   'kwargs': {'delay': delay + 10, 'other_nodes': other_nodes}})
        run_in_parallel(proc_functions)

        # Index will not finish building, because view building underneath is paused until updates can be sent.
        if node_action == 'stop':
            self._start_nodes(nodes_to_start)

        wait_for_view(cluster=self.cluster, session=session, ks='mview', view='users_by_first_name')
        wait_for_view(cluster=self.cluster, session=session, ks='mview', view='users_by_last_name')

        self.eventually(lambda: self._validate_cs_results(node1, exclude_errors, node_action, double_failure))

    def test_multidc_dc_failure_during_mv_insert(self):
        """ Test stopping all DC nodes during MV inserts
            Test starts with a starting size: two DCs with 2 nodes each, and stops 2 nodes of second DC during inserts
            into base
            table that cause to update materialized view as well (using cs_mv_profile.yaml profile).
            Validate the log has no errors.
            Issue #2783: there are mutation_write_timeout_exception in case starting size 4 and more
        """
        session = self.prepare(rf={'dc1': 2, 'dc2': 1}, nodes=[3, 3])
        mv_profile = os.path.abspath(os.path.join("test_data", 'cassandra-mv-profile', 'cs_mv_multidc_profile.yaml'))

        node1_dc1 = [node for node in self.cluster.nodelist() if node.data_center == 'dc1'][0]
        proc_functions = [{'func': node1_dc1.stress, 'args': [['user', 'profile={}'.format(mv_profile), 'cl=QUORUM',
                                                               'duration=2m', 'ops(insert=3,read1=1,read2=1,read3=1)',
                                                               '-mode cql3  native', '-rate threads=10'
                                                               ], True]},
                          {'func': self._stop_few_nodes, 'kwargs': {'delay': 30, 'by_dc_name': 'dc2'}}]
        run_in_parallel(proc_functions)

        # Index will not finish building, because view building underneath is paused until updates can be sent.
        for node in self.cluster.nodelist():
            if node.data_center == 'dc2':
                logger.debug('Start node {}'.format(node.name))
                node.start(wait_for_binary_proto=True)

        wait_for_view(cluster=self.cluster, session=session, ks='mview', view='users_by_first_name')
        wait_for_view(cluster=self.cluster, session=session, ks='mview', view='users_by_last_name')

        self.eventually(
            lambda: self._validate_cs_results(node1_dc1, exclude_errors=['mutation_write_timeout_exception'],
                                              node_action='', double_failure=True))

    def _truncate_base_during_mv_insert(self, auto_snapshot: bool):
        """ Test truncating the base table during MV inserts
            Validate the log has no errors and
            that materialized views building completes.
            We can't validate the result data as we don't
            know exactly what was truncated.
        """
        session = self.prepare(nodes=3, rf=3, options={'auto_snapshot': auto_snapshot})
        mv_profile = os.path.abspath(os.path.join("test_data", 'cassandra-mv-profile', 'cs_mv_profile.yaml'))

        node1 = self.cluster.nodelist()[0]
        proc_functions = [{'func': node1.stress, 'args': [['user', 'profile={}'.format(mv_profile), 'cl=QUORUM',
                                                           'duration=1m', 'ops(insert=3,read1=1,read2=1,read3=1)',
                                                           '-mode cql3  native', '-rate threads=10'
                                                           ], True]},
                          {'func': self._truncate_table, 'kwargs': {'ks': 'mview', 'table': 'users', 'delay': 30}}]
        run_in_parallel(proc_functions)

        wait_for_view(cluster=self.cluster, session=session, ks='mview', view='users_by_first_name')
        wait_for_view(cluster=self.cluster, session=session, ks='mview', view='users_by_last_name')

    def test_truncate_base_during_mv_insert_test_with_auto_snapshot(self):
        self._truncate_base_during_mv_insert(auto_snapshot=True)

    @pytest.mark.dtest_debug
    def test_truncate_base_during_mv_insert_test_without_auto_snapshot(self):
        self._truncate_base_during_mv_insert(auto_snapshot=False)

    def _node_action_with_delay(self, action, node, delay=0, wait=True, wait_other_notice=True, other_nodes=None, gently=True):
        """
        :param action: expected values: stop, remove
        :param action: str
        """
        if action not in ['stop', 'remove', 'decommission', 'restart']:
            assert False, 'Unsupported node action'

        if delay:
            logger.debug('Sleep for {} seconds'.format(delay))
            time.sleep(delay)

        logger.debug('START: {0} node {1}'.format(action, node.name))
        if action == 'stop':
            node.stop(wait=wait, wait_other_notice=wait_other_notice, other_nodes=other_nodes, gently=gently)
        elif action == 'restart':
            node.stop(wait=wait, wait_other_notice=wait_other_notice, other_nodes=other_nodes, gently=gently)
            time.sleep(delay if delay else 1)
            node.start(wait_other_notice=wait_other_notice)
        elif action == 'remove':
            remove_node(self.cluster, node, wait_other_notice=wait_other_notice, other_nodes=other_nodes)
        else:
            new_node_index = len(self.cluster.nodelist()) + 1
            node.nodetool(action)
            if action == 'decommission':
                logger.debug('START add new node')
                self._add_new_node(new_node_index=new_node_index)
                logger.debug('FINISH add new node')

        logger.debug('FINISH: {0} node {1}'.format(action, node.name))

    def _stop_few_nodes(self, by_dc_name='', by_node_names=[], delay=0, wait=True, wait_other_notice=False,
                        gently=True):
        if delay:
            logger.debug('Sleep for {} seconds'.format(delay))
            time.sleep(delay)

        other_nodes = self.cluster.nodelist()
        stop_nodes = []
        for node in self.cluster.nodelist():
            if (by_dc_name and node.data_center == by_dc_name) or (by_node_names and node.name in by_node_names):
                stop_nodes.append(node)
                other_nodes.remove(node)

        for node in stop_nodes:
            self._node_action_with_delay('stop', node, wait=wait,
                                         wait_other_notice=wait_other_notice, other_nodes=other_nodes, gently=gently)

    def _truncate_table(self, ks='mview', table='users', delay=0):
        if delay:
            logger.debug('Sleep for {} seconds'.format(delay))
            time.sleep(delay)

        node = self.cluster.nodelist()[0]
        session = self.patient_cql_connection(node)
        logger.debug(f"Truncating table '{ks}.{table}' ...")
        start = time.time()
        session.execute(f"TRUNCATE table {ks}.{table}")
        delta = time.time() - start
        logger.debug(f"Truncating table '{ks}.{table}' done in {delta:.1f} seconds")

    @pytest.mark.require('#5459')
    @pytest.mark.timeout(3500)
    def test_add_dc_during_mv_insert(self):
        """ Test expand cluster - add new DC during MV inserts
            Test starts with a starting size: one DCs with 4 nodes, and add new 2 nodes of second DC during inserts into base
            table that cause to update materialized view as well.
            Verify that MV records are as it exists in the base table
            Validate the log has no errors.
        """
        self._add_dc_during_mv_change('insert', 3, 4, start_prefill=1000, more_inserts=300000)

    def _validate_cs_results(self, node, exclude_errors, node_action, double_failure, cl=None, num_attempts=1):
        self.check_errors(node, exclude_errors)
        session = self.patient_exclusive_cql_connection(node)
        session.execute('USE mview')
        cl = self.set_consistency_level(node_action=node_action, double_failure=double_failure, cl=cl)
        logger.debug(f"Validate data using CL={consistency_value_to_name(cl)}")
        exp_res = run_query_with_data_processing(session, 'select count(*) from mview.users', consistency_level=cl)
        try:
            exp_res = int(exp_res[0].count)
        except TypeError:
            logger.debug('Try to select rows count from mview.users table. Expected integer vale, received: {}'.format(
                exp_res[0].count))
            raise
        except Exception as e:
            logger.debug('Try to select rows count from mview.users table. Failed with error: {}'.format(e))
            raise

        assert_row_count(session, 'users_by_first_name', exp_res, consistency_level=cl, num_attempts=num_attempts)
        assert_row_count(session, 'users_by_last_name', exp_res, consistency_level=cl, num_attempts=num_attempts)

    def test_add_dc_during_mv_update(self):
        """ Test expand cluster - add new DC during MV inserts
            Test starts with a starting size: one DCs with 4 nodes, and add new 2 nodes of second DC during update
            existent records of base
            table that cause to update materialized view as well.
            Verify that MV records are according to the base table
        """
        self._add_dc_during_mv_change('update', 3, 4, start_prefill=4000, more_inserts=300000)

    def _add_dc_during_mv_change(self, change, rf, nodes, start_prefill, more_inserts):
        session = self.prepare(rf=rf, nodes=nodes, fetch_size=start_prefill + more_inserts * 2)
        node1 = self.cluster.nodelist()[0]
        tm = TableManager(session, self.cluster,
                          columns={'int': {'amount': 2, 'frozen': False, 'value length': {'min': 1, 'max': 100}}
                                   }, cl_columns={})
        tm.create_table()

        mv = MaterializedViewManager(tm)
        mv_restrict_value = 53
        mv.create_materialized_view(mv_columns={'int': {'names': [tm.column_names_list[-1]]}},
                                    mv_pk_column={'type': 'int'},
                                    mv_where_restriction={
                                        'position': {-2: {'operator': '=', 'value': mv_restrict_value}}})

        tm.prefill_table(start_prefill, data={'int': [2, 5, 12, 45, mv_restrict_value, 78, 36, 85, 98, 100]})

        query = 'select id, clmn_int0, %s from {tbl}{where}{f}' % mv.mv_columns_list[0]

        exp_query = 'select id, {clmn1}, {clmn2} from {tbl}{where}{f}'.format(clmn1=tm.column_names_list[-1],
                                                                              clmn2=list(
                                                                                  mv.mv_where_restriction.keys())[0],
                                                                              tbl=tm.table_name, where='', f='')
        act_query = query.format(tbl=mv.mv_name, where='', f='')
        assert_two_queries_equal(session, exp_query, session, act_query, consistency_level=ConsistencyLevel.QUORUM,
                                 session_timeout=self.session_timeout,
                                 group=True, groupby_column1=tm.column_names_list[-1],
                                 groupby_column2=tm.column_names_list[-1],
                                 restrict_column1=list(mv.mv_where_restriction.keys())[0],
                                 restrict_value1=mv_restrict_value)

        proc_functions = [{'func': self._add_few_nodes, 'args': (2, 'dc2')},
                          {'func': self.add_mv_records if change == 'insert' else self._multiple_int_updates,
                           'args': (tm, mv_restrict_value) if change == 'insert' else
                           (session, tm, tm.column_names_list[-1],
                            list(mv.mv_where_restriction.keys())[0], mv_restrict_value, [100, 200]),
                           'kwargs': {'delay': 5, 'inserts': more_inserts} if change == 'insert' else {'delay': 5,
                                                                                                       'updates': 200}}]

        run_in_parallel(proc_functions)
        self.cluster.flush()

        # Validate data
        for node in self.cluster.nodelist():
            if node.data_center == 'dc2':
                session = self.patient_exclusive_cql_connection(node, keyspace=tm.keyspace)
                assert_two_queries_equal(session, exp_query, session, act_query, consistency_level=ConsistencyLevel.ALL,
                                         session_timeout=self.session_timeout,
                                         group=True, groupby_column1=tm.column_names_list[-1],
                                         groupby_column2=tm.column_names_list[-1],
                                         restrict_column1=list(mv.mv_where_restriction.keys())[0],
                                         restrict_value1=mv_restrict_value)

    def add_mv_records(self, tm, mv_restrict_value=None, inserts=10, delay=0):
        if delay:
            time.sleep(delay)

        id = tm.get_max_id()
        id = id if not id else id + 1
        data = {'int': [mv_restrict_value]} if mv_restrict_value else None
        tm.prefill_table(inserts, data=data, start_id_from=id, flush=False)

    def _multiple_int_updates(self, session, tm, updated_column, filter_column, filter_value, update_to_boundaries,
                              updates=10, delay=0):
        if delay:
            time.sleep(delay)

        logger.debug('Start updates')
        res = session.execute('select * from {}'.format(tm.table_name)).current_rows
        updated_column_index = [i for i, clmn in enumerate(res[0]._fields) if clmn == updated_column][0]
        for _ in range(updates):
            time.sleep(1)
            while True:
                i = random.randint(0, len(res) - 1)
                if res[i][updated_column_index] == filter_value:
                    id = res[i].id
                    break

            tm.update_table(set_clause={
                'by name': {updated_column: random.randint(update_to_boundaries[0], update_to_boundaries[1])}},
                where_filter={'by name': {filter_column: {'operator': '=', 'value': filter_value},
                                          'id': {'operator': '=', 'value': id}}})
        logger.debug('Updates were finished')

    def _add_few_nodes(self, nodes, data_center, delay=0):
        if delay:
            logger.debug('Sleep for {} seconds'.format(delay))
            time.sleep(delay)

        for i in range(0, nodes):
            logger.debug('Bootstrapping {0} node in {1}'.format(i + 1, data_center))
            self._add_new_node(data_center=data_center)

    @pytest.mark.dtest_heavy
    def test_hundred_mv_concurrent(self):
        """
        Performance and functional test.
        - Create 100 materialized views on the same base table.
        - Pre-fill the table with 5000 records. Expected same records amount in the all views
        - Validate the records count in the base table and all MVs
        - In the parallel threads run: insert 5000 new records / updates / reads
        - Validate the records count in the base table and all MVs
        - If previous validation passed - validated the data in the MVs is as in the base table
        """
        self._parallel_updates_inserts(records=2000, nodes=3, rf=3, mvs_amount=100)

    @pytest.mark.timeout(4500)
    def test_small_concurrent(self):
        """
        This test is same as "hundreds_mvs_on_table_test" test, just small.
        It was added for easy testing concurrent view updates
        """
        self._parallel_updates_inserts(records=2000, nodes=3, rf=3, mvs_amount=10)

    # TODO: update non-key column
    def _parallel_updates_inserts(self, records, nodes, rf, mvs_amount):
        def _assert_rows_count(expected_rows=None):
            names_list = [tm.table_name] + list(tm.materialized_views.keys()
                                                if expected_rows else tm.materialized_views.keys())
            for name in names_list:
                if expected_rows:
                    assert_row_count_in_select(session=session, query="SELECT * FROM {}".format(name),
                                               num_rows_expected=expected_rows,
                                               consistency_level=ConsistencyLevel.QUORUM)
                else:
                    assert_two_queries_equal(session, 'select count(*) from {}'.format(tm.table_name),
                                             session, 'select count(*) from {}'.format(name))

        session = self.prepare(rf=rf, nodes=nodes, fetch_size=records * 3)
        tm = TableManager(session, self.cluster,
                          columns={
                              'int': {'amount': mvs_amount, 'frozen': False, 'value length': {'min': 1, 'max': 100}}
                          }, pk_columns={}, cl_columns={})
        tm.create_table()

        for i in range(1, len(tm.column_names_list)):
            ctype = tm.columns_list[i].split(' ')[1]
            mv = MaterializedViewManager(tm)
            mv.create_materialized_view(mv_columns={ctype: {'names': [tm.column_names_list[i]]}},
                                        mv_pk_column={'names': [tm.column_names_list[i]]},
                                        )

        start_data = [2, 5, 12, 45, 63, 78, 36, 85, 98, 100]
        tm.prefill_table(records, data={'int': start_data})
        _assert_rows_count(records)

        new_data = [-2, -5, -12, -45, -63, -78, -36, -85, -98, -100]
        proc_functions = [
            {'func': tm.prefill_table, 'args': (records,),
             'kwargs': {'data': {'int': new_data}, 'start_id_from': records + 1}},
            {'func': tm.multiple_int_updates_by_id, 'args': ([200, 300],),
             'kwargs': {'filter_values': start_data + new_data, 'updates': 1000, 'same_id': False}},
            {'func': tm.multiple_int_updates_by_id, 'args': ([300, 400],),
             'kwargs': {'filter_values': start_data, 'updates': 1000}},
            {'func': tm.select_all_mvs, 'kwargs': {'reads': 2000, 'by_id': True}}
        ]

        run_in_parallel(proc_functions)
        flush_by_node(self.cluster)
        time.sleep(180)

        # Validate count on every node
        self.eventually(lambda: _assert_rows_count(records * 2))

        # Validate data
        query_template = 'select {clmn} from {tbl}'
        for mv_name, mv in tm.materialized_views.items():
            exp_query = query_template.format(clmn=mv.mv_columns_list[-1], tbl=tm.table_name)
            act_query = query_template.format(clmn=mv.mv_columns_list[-1], tbl=mv_name)
            logger.debug('Compare: {0} AND {1}'.format(exp_query, act_query))
            self.eventually(lambda: assert_two_queries_equal(session, exp_query, session, act_query,
                                                             consistency_level=ConsistencyLevel.QUORUM,
                                                             session_timeout=self.session_timeout,
                                                             group=True, groupby_column1=mv.mv_columns_list[-1],
                                                             groupby_column2=mv.mv_columns_list[-1]))

    @pytest.mark.skip('under investigation')
    def test_mv_on_index_column(self):
        session = self.prepare()
        session.execute(
            'CREATE TABLE ToDo (id uuid PRIMARY KEY, ToDo_Complete boolean, ToDo_Text text,ToDo_User_id uuid, ToDo_site_id uuid)')
        session.execute('CREATE INDEX ToDo_ToDo_User_id_idx ON ToDo (ToDo_User_id)')
        session.execute(
            'CREATE MATERIALIZED VIEW ToDo_ToDo_User_id_idx_index AS SELECT ToDo_User_id, id FROM ToDo WHERE ToDo_User_id IS NOT NULL PRIMARY KEY (ToDo_User_id, id)')
        result = session.execute('select * from ToDo where ToDo_User_id = 00112233-4455-6677-8899-aabbccddeeff')
        print(result)

    @staticmethod
    def _create_mvs_by_one_column(tm, mvs_amount, wait_for_view_built=False):
        for i in range(1, mvs_amount + 1):
            mv = MaterializedViewManager(tm)
            mv.create_materialized_view(
                mv_columns={tm.columns_list[i].split(' ')[1]: {'names': [tm.column_names_list[i]]}},
                mv_pk_column={'names': [tm.column_names_list[i]]}, wait_for_view_built=wait_for_view_built)

    def test_mv_populating_from_existing_data(self):
        """ Create one materialized view on the populated base table """
        self._mv_populating_from_existing_data(nodes=4, rf=3, mvs=1, prefill=100)

    def test_mvs_populating_from_existing_data(self):
        """ Create 10 materialized view on the populated base table """
        self._mv_populating_from_existing_data(nodes=4, rf=3, mvs=10, prefill=1000)

    def _mv_populating_from_existing_data(self, nodes, rf, mvs, prefill):
        session = self.prepare(rf=rf, nodes=nodes)
        tm = TableManager(session, self.cluster,
                          columns={'int': {'amount': mvs, 'frozen': False,
                                           'value length': {'min': 1, 'max': 100}}
                                   }, pk_columns={}, cl_columns={})
        tm.create_table()
        tm.prefill_table(prefill)

        self._create_mvs_by_one_column(tm, mvs, wait_for_view_built=True)
        self.cluster.flush()

        self._validate_data_in_mvs(tm=tm, session=session, table_expected_rows=prefill, mv_expected_rows=prefill,
                                   consistency_level=ConsistencyLevel.ALL)
        self.fixture_dtest_setup.ignore_log_patterns += [
            r'Error applying view update to .*: data_dictionary::no_such_column_family']

    def test_mv_populating_from_existing_data_with_restriction(self):
        session = self.prepare(rf=3, nodes=4)
        mvs = 10
        tm = TableManager(session, self.cluster,
                          columns={'int': {'amount': mvs + 1, 'frozen': False,
                                           'value length': {'min': 1, 'max': 100}}
                                   }, cl_columns={})
        tm.create_table()
        data = [2, 5, 12, 45, 53, 78, 36, 85, 98, 100]
        tm.prefill_table(10000, data={'int': data})

        self.fixture_dtest_setup.ignore_log_patterns += [
            r'view - Error applying view update to .*: exceptions::mutation_write_failure_exception '
            r'\(Operation failed for ks.tm_table_mv_\d+ - received 0 responses and 1 failures from 1 CL=ONE\.\)']

        for i in range(2, mvs + 1):
            mv = MaterializedViewManager(tm)
            mv.create_materialized_view(mv_columns={'int': {'names': [tm.column_names_list[i]]}},
                                        mv_pk_column={'names': [tm.column_names_list[i]]},
                                        mv_where_restriction={
                                            'names': {tm.pk_list[1]: {'operator': '=', 'value': data[i - 1]}}})

        query = 'select id, clmn_int0, {clmn} from {tbl}'
        for mv_name, mv in tm.materialized_views.items():
            act_query = query.format(clmn=mv.mv_columns_list[0], tbl=mv.mv_name)
            exp_query = query.format(clmn=mv.mv_columns_list[0], tbl=tm.table_name)
            self.eventually(lambda: assert_two_queries_equal(session, exp_query, session, act_query,
                                                             consistency_level=ConsistencyLevel.QUORUM,
                                                             session_timeout=self.session_timeout,
                                                             group=True, groupby_column1=mv.mv_columns_list[0],
                                                             groupby_column2=mv.mv_columns_list[0],
                                                             restrict_column1=list(mv.mv_where_restriction.keys())[0],
                                                             restrict_value1=mv.mv_where_restriction[
                                                                 list(mv.mv_where_restriction.keys())[0]]['value']))

    def test_mv_populating_from_existing_data_during_inserts(self):
        """ Create 10 materialized views in parallel with base table prefill """
        self._mv_populating_from_existing_data_during_changes_test('insert')

    def test_mv_populating_from_existing_data_during_updates(self):
        """ Create 10 materialized views in parallel with base table updates """
        self._mv_populating_from_existing_data_during_changes_test('update')

    def test_mv_populating_from_existing_data_during_deletes(self):
        """ Create 10 materialized views in parallel with base table deletes """
        self._mv_populating_from_existing_data_during_changes_test('delete')

    def test_mv_populating_from_existing_data_during_extend(self):
        """ Create 10 materialized views in parallel with adding a node """
        self._mv_populating_from_existing_data_during_changes_test('add node')

    def test_mv_populating_from_existing_data_during_node_remove(self):
        """ Create 10 materialized views in parallel with removing a node """
        self._mv_populating_from_existing_data_during_changes_test('remove node')

    @pytest.mark.dtest_heavy
    def test_mv_populating_from_existing_data_during_node_stop(self):
        """ Create 10 materialized views in parallel with stopping a node """
        self._mv_populating_from_existing_data_during_changes_test('stop node')

    def test_mv_populating_from_existing_data_during_node_decommission(self):
        """ Create 10 materialized views in parallel with a node decommission """
        self._mv_populating_from_existing_data_during_changes_test('decommission')

    @pytest.mark.dtest_heavy
    def test_mv_populating_from_existing_data_during_node_restart(self):
        """ Create 10 materialized views in parallel with a node restart """
        self._mv_populating_from_existing_data_during_changes_test('restart node')

    def _mv_populating_from_existing_data_during_changes_test(self, change_type, nodes=4, rf=3, mvs=None, prefill=None):
        session = self.prepare(rf=rf, nodes=nodes, options={'prometheus_port': 0})

        node_action = change_type.split(' ')[0]
        if node_action in ['decommission', 'restart', 'remove', 'stop']:
            session.cluster.shutdown()
            cs = self.patient_cql_cluster_session(self.cluster.nodelist(
            )[0], 'ks', exclusive=True, consistency_level=ConsistencyLevel.QUORUM)
            session = cs.session

        cluster = self.cluster
        if prefill is None:
            prefill = 40000
            max_delete = 6000
            mvs = 10
            if hasattr(cluster, 'scylla_mode') and cluster.scylla_mode == 'debug':
                prefill = 10000
                max_delete = 2000
                mvs = 2

        tm = TableManager(session, self.cluster,
                          columns={'int': {'amount': mvs, 'frozen': False,
                                           'value length': {'min': 1, 'max': 100}}
                                   }, pk_columns={}, cl_columns={})

        rows_after_test = prefill

        if change_type == 'insert':
            change_func = {'func': tm.prefill_table, 'args': (
                prefill // 2,), 'kwargs': {'start_id_from': prefill + 1, 'delay': 1}}
            rows_after_test = prefill * 1.5
        elif change_type == 'update':
            change_func = {'func': tm.multiple_int_updates_by_id, 'args': ([-100, -1],),
                           'kwargs': {'same_id': False, 'delay': 1}}
        elif change_type == 'delete':
            change_func = {'func': tm.multiple_deletes, 'args': (
                {'id': [i for i in range(1000, max_delete)]},), 'kwargs': {'delay': 1}}
            rows_after_test = max(0, prefill - (max_delete - 1000))
        elif change_type == 'add node':
            change_func = {'func': self._add_new_node, 'kwargs': {'delay': 1}}
        elif change_type == 'decommission':
            change_func = {'func': self._node_action_with_delay, 'args': ('decommission', self.cluster.nodes['node2']),
                           'kwargs': {'delay': 2}}
        elif change_type == 'restart node':
            change_func = {'func': self._node_action_with_delay, 'args': (
                node_action, self.cluster.nodelist()[1]), 'kwargs': {'delay': 1}}
        elif change_type in ['remove node', 'stop node']:
            change_func = {'func': self._node_action_with_delay, 'args': (node_action, self.cluster.nodelist()[1]),
                           'kwargs': {'delay': 1}}
        else:
            assert False, 'Unexpected parameter "change_type": {}. ' \
                          'Expected values: insert / update / delete / add node / remove node / stop node / decommission' \
                          'restart node'.format(change_type)

        tm.create_table()
        tm.prefill_table(prefill)
        self.cluster.flush()

        logger.debug('Disabling schema agreement')
        session.cluster.max_schema_agreement_wait = 0

        proc_functions = [change_func, {'func': self._create_mvs_by_one_column, 'args': (tm, mvs)}]
        run_in_parallel(proc_functions)

        if node_action == 'stop':
            self.cluster.nodelist()[1].start()

        for mv_name in tm.materialized_views.keys():
            wait_for_view(cluster=self.cluster, session=session, ks=tm.keyspace, view=mv_name)

        self._validate_data_in_mvs(tm=tm, session=session, table_expected_rows=rows_after_test,
                                   mv_expected_rows=rows_after_test,
                                   node_action=node_action)

        exclude_errors = ['migration_task - Can''t send migration request',
                          'mutation_write_timeout_exception',
                          'Error applying view update to',
                          'view - Failed to update materialized view bookkeeping.*seastar::no_sharded_instance_exception.*continuing anyway']
        self.check_errors_all_nodes(exclude_errors=exclude_errors, regex=True)

    def _restart_node(self, node, delay=0):
        time.sleep(delay)
        logger.debug('Start {} restart'.format(node.name))
        node.stop()
        time.sleep(5)
        node.start()
        logger.debug('Finish node {} restart'.format(node.name))

    def set_consistency_level(self, node_action, double_failure=None, cl=None):
        # Set CL as:
        #      - for double_failure - ALL
        #      - for stop/restart/decommission node action - QUORUM
        #      - if RF more then active nodes amount - QUORUM
        #      - for remove node action - ALL
        cl = cl or \
            (ConsistencyLevel.ALL if double_failure else
             ConsistencyLevel.QUORUM if node_action in ['stop', 'restart', 'decommission'] or
             self.rf > len(self.cluster.nodelist())
             else ConsistencyLevel.ALL)
        logger.debug('Query will run with consistency level {}'.format(cl))
        return cl

    def _validate_data_in_mvs(self, tm, session, table_expected_rows, mv_expected_rows, node_action=None,
                              grouby_column_index=-1,
                              consistency_level=None):
        query = 'select * from {}'
        consistency_level = self.set_consistency_level(node_action=node_action, cl=consistency_level)
        for mv_name, mv in tm.materialized_views.items():
            self._assert_count_table_mv(session, tm.table_name, table_expected_rows,
                                        mv_name, mv_expected_rows, cl=consistency_level)

            self.eventually(lambda: assert_two_queries_equal(session, query.format(tm.table_name),
                                                             session, query.format(mv_name),
                                                             consistency_level=consistency_level,
                                                             session_timeout=self.session_timeout, group=True,
                                                             groupby_column1=mv.mv_columns_list[grouby_column_index],
                                                             groupby_column2=mv.mv_columns_list[grouby_column_index]))

    def test_concurrent_updates_deletes(self):
        prefill = 2
        session = self.prepare(rf=3, nodes=4, fetch_size=prefill * 2)
        mvs = 1
        tm = TableManager(session, self.cluster,
                          columns={'int': {'amount': mvs, 'frozen': False,
                                           'value length': {'min': 1, 'max': 1000}}
                                   }, pk_columns={}, cl_columns={})

        tm.create_table()
        self._create_mvs_by_one_column(tm, mvs, wait_for_view_built=True)

        start_data = [2, 5, 12, 45, 63, 78, 36, 85, 98, 100]
        tm.prefill_table(prefill, data={'int': start_data})
        self.cluster.flush()

        proc_functions = [
            {'func': tm.multiple_int_updates_by_id, 'args': ([-100, -1],),
             'kwargs': {'same_id': False, 'ids': [0 for _ in range(0, 1001)],
                        'updates': 1000}},
            {'func': tm.multiple_deletes, 'args': ({'id': [0 for _ in range(0, 1001)]},)}]
        run_in_parallel(proc_functions)

        self.cluster.flush()

        mv_name, mv = next(iter(tm.materialized_views.items()))
        self.eventually(lambda: assert_two_queries_equal(session, 'select * from {}'.format(tm.table_name),
                                                         session, 'select * from {}'.format(mv_name),
                                                         consistency_level=ConsistencyLevel.QUORUM,
                                                         session_timeout=self.session_timeout, group=True,
                                                         groupby_column1=mv.mv_columns_list[-1],
                                                         groupby_column2=mv.mv_columns_list[-1]))

    def test_multi_mvs_on_different_base_tables(self):
        """ Few keyspaces and every keyspace has a few tables and every table has a few MVs.
            MVs are created on the empty base tables
        """
        self._multi_mvs_on_different_base_tables_multi_ks(rf=3, tables=3, mvs=4, prefill_start=10000,
                                                          increase_rows=10, populated_table=False)

    def test_multi_mvs_on_different_populated_base_tables(self):
        """ Few keyspaces and every keyspace has a few tables and every table has a few MVs.
            MVs are created on the populated base tables
        """
        """ Test when keyspace has a few tables and every table has a few MVs. MVs are created on the populated base tables """
        self._multi_mvs_on_different_base_tables_multi_ks(rf=3, tables=3, mvs=4, prefill_start=10000,
                                                          increase_rows=10, populated_table=True)

    def _multi_mvs_on_different_base_tables_multi_ks(self, rf, tables, mvs, prefill_start, increase_rows,
                                                     populated_table):

        session = self.prepare(rf=rf, nodes=4, options={'hinted_handoff_enabled': False, 'read_repair_chance': 0.0})
        session2, session3 = map(self.patient_cql_connection, [self.cluster.nodelist()[1], self.cluster.nodelist()[2]])
        list(map(create_ks, [session2, session3], ['multi1', 'multi2'], [rf, rf]))
        proc_functions = []
        for s in [session, session2, session3]:
            proc_functions.append({'func': self._multi_mvs_on_different_base_tables, 'args': (s,),
                                   'kwargs': {'tables': tables, 'mvs': mvs, 'prefill_start': prefill_start,
                                              'increase_rows': increase_rows, 'populated_table': populated_table}})
        run_in_parallel(proc_functions)

    def _multi_mvs_on_different_base_tables(self, session, tables, mvs, prefill_start, increase_rows, populated_table):
        def _prefill_base_tables():
            prefill = prefill_start
            for base_table in base_tables:
                base_table.prefill_table(prefill)
                prefill = prefill + increase_rows

        def _create_mvs():
            for base_table in base_tables:
                self._create_mvs_by_one_column(base_table, mvs_amount=mvs, wait_for_view_built=True)

        base_tables = []
        for i in range(tables):
            tm = TableManager(session, self.cluster, table_name='tm_table{}'.format(i),
                              columns={'int': {'amount': mvs // 2, 'frozen': False,
                                               'value length': {'min': 1, 'max': 100}},
                                       'text': {'amount': mvs // 2, 'frozen': False,
                                                'value length': {'min': 1, 'max': 10}}
                                       }, pk_columns={}, cl_columns={}, keyspace=session.keyspace)
            tm.create_table()
            base_tables.append(tm)

        order = [_prefill_base_tables, _create_mvs] if populated_table else [_create_mvs, _prefill_base_tables]
        for func in order:
            func()

        prefill = prefill_start
        for base_table in base_tables:
            self._validate_data_in_mvs(tm=base_table, session=session, table_expected_rows=prefill,
                                       mv_expected_rows=prefill,
                                       consistency_level=ConsistencyLevel.ALL)
            prefill = prefill + increase_rows

    def _prepare_cluster_for_drop(self, columns: dict) -> Session:
        rf = 3
        session = self.prepare(rf=rf, nodes=3)

        logger.debug("Create table cf")
        create_cf(session=session, name='cf', key_type='int', columns=columns)

        logger.debug("Create materialized view mv_v1_view")
        session.execute("CREATE MATERIALIZED VIEW mv_v1_view AS SELECT v1, key FROM cf WHERE v1 IS NOT NULL and "
                        "key IS NOT NULL PRIMARY KEY (v1, key)")
        wait_for_view(cluster=self.cluster, session=session, ks='ks', view='mv_v1_view')
        session.execute("INSERT INTO cf (key, v0, v1) VALUES(0, 0, 0)")
        return session

    def _search_for_apply_mutation_error(self, mark_logs: dict = {}, update_mark_logs: bool = True):
        for node in self.cluster.nodelist():
            try:
                wait_for(func=node.grep_log, step=1, timeout=5, throw_exc=True,
                         expr='Failed to apply mutation', from_mark=mark_logs[node.name])
                assert False, f"'Failed to apply mutation' error found in the {node.name} log unexpectedly"
            except:
                pass

            if update_mark_logs:
                mark_logs[node.name] = node.mark_log()

    def run_insert(self, session: Session, columns: str, range_start: int, range_end: int, repeat: int):

        logger.debug("Insert some data")
        for _ in range(repeat):
            try:
                for i in range(range_start, range_end):
                    cmd = f"INSERT INTO cf ({columns}) VALUES({','.join([str(i) for _ in columns.split(',')])})"
                    logger.debug(f"Run {cmd}")
                    session.execute(cmd)
            except WriteFailure as wf:
                assert False, f"Insert request failed unexpectedly with exception {wf}"

    @staticmethod
    @retrying(num_attempts=3)
    def rows_validation(session, rows):
        logger.debug("Validation")
        assert_row_count(session, 'cf', rows, consistency_level=ConsistencyLevel.QUORUM)
        assert_row_count(session, 'mv_v1_view', rows, consistency_level=ConsistencyLevel.QUORUM)

    def test_add_drop_column(self):
        """
        Cover https://github.com/scylladb/scylla-enterprise/issues/1467 and
        https://github.com/scylladb/scylla/issues/7061
        Drop the column that was added after MV creation
        - Create base table and materialized view
        - Add 2 new columns
        - Drop one added column
        - Insert data
        All data is inserted, no failures
        """
        session = self._prepare_cluster_for_drop(columns={'v0': 'int', 'v1': 'int'})

        mark_logs = {}
        for node in self.cluster.nodelist():
            mark_logs[node.name] = node.mark_log()

        with ThreadPoolExecutor(max_workers=1) as tp:
            thread = tp.submit(self.run_insert, session=session, columns='key, v0, v1',
                               range_start=0, range_end=20, repeat=20)

            for i in range(2, 7):
                logger.debug(f"Add regular column v{i}")
                session.execute(f"ALTER TABLE cf ADD v{i} int")

            thread.result(timeout=60)

        logger.debug("Search for mutation error in nodes' logs")
        self._search_for_apply_mutation_error(mark_logs)

        self.rows_validation(session=session, rows=20)

        with ThreadPoolExecutor(max_workers=1) as tp:
            thread = tp.submit(self.run_insert, session=session, columns='key, v0, v1',
                               range_start=20, range_end=40, repeat=20)

            for i in range(2, 7):
                logger.debug(f"Drop regular column v{i}")
                session.execute(f"ALTER TABLE cf DROP v{i}")

            thread.result(timeout=60)

        logger.debug("Search for mutation error in nodes' logs")
        self._search_for_apply_mutation_error(mark_logs, update_mark_logs=False)

        self.rows_validation(session=session, rows=40)

    def test_drop_existing_column(self):
        """
        Cover https://github.com/scylladb/scylla-enterprise/issues/1467 and
        https://github.com/scylladb/scylla/issues/7061
        Drop the regular column that was added before MV creation.
        - Create base table and materialized view
        - Drop regular column
        - Insert data
        All data is inserted, no failures
        """
        session = self._prepare_cluster_for_drop(columns={'v0': 'int', 'v1': 'int'})

        mark_logs = {}
        for node in self.cluster.nodelist():
            mark_logs[node.name] = node.mark_log()

        with ThreadPoolExecutor(max_workers=1) as tp:
            thread = tp.submit(self.run_insert, session=session, columns='key,v1',
                               range_start=0, range_end=20, repeat=20)

            logger.debug(f"Drop regular column v0")
            session.execute(f"ALTER TABLE cf DROP v0")

            thread.result(timeout=60)

        logger.debug("Search for mutation error in nodes' logs")
        self._search_for_apply_mutation_error(mark_logs, update_mark_logs=False)

        self.rows_validation(session=session, rows=20)

    @pytest.mark.dtest_heavy
    def test_drop_mv_during_base_table_writes(self):
        """ Test drop a view during base table writes: the view is created on empty base table and dropped during table prefill
            Test scenario:
            - Create base table
            - Create materialized view
            - Start table prefill with 1000000 records
            - After 40 seconds (before the table prefill is finished) drop the MV
            - Test that the view does not exist in the system schema and base table has 1000000 rows
        """
        prefill = 1000000
        if hasattr(self.cluster, 'scylla_mode') and self.cluster.scylla_mode == 'debug':
            prefill //= 10

        def _create_mvs(delay=0):
            time.sleep(delay)
            mv = MaterializedViewManager(tm)
            mv.create_materialized_view(
                mv_columns={tm.columns_list[1].split(' ')[1]: {'names': [tm.column_names_list[1]]}},
                mv_pk_column={'names': [tm.column_names_list[1]]})

        def _drop_mv(delay=0):
            time.sleep(delay)
            next(iter(tm.materialized_views.values())).drop_mv()

        timeout = self.cql_timeout(300)
        session = self.prepare(rf=3, nodes=4, options={
            'range_request_timeout_in_ms': timeout * 1000,
        })
        tm = TableManager(session, self.cluster,
                          columns={'int': {'amount': 1, 'frozen': False,
                                           'value length': {'min': 1, 'max': 100}}
                                   }, pk_columns={}, cl_columns={})
        tm.create_table()

        _create_mvs()
        proc_functions = [{'func': self.add_mv_records, 'args': (tm,), 'kwargs': {'inserts': prefill, 'delay': 0}},
                          {'func': _drop_mv, 'kwargs': {'delay': 40}}
                          ]
        run_in_parallel(proc_functions)
        self.cluster.flush()

        logger.debug("Verifying that system_schema.views is empty")
        assert_none(session, 'select * from system_schema.views', cl=ConsistencyLevel.ALL)
        logger.debug(f"Verifying that {tm.table_name} has {prefill} rows")
        assert_row_count(session, tm.table_name, prefill, consistency_level=ConsistencyLevel.QUORUM, timeout=timeout)

        self.check_errors(node=self.cluster.nodelist()[0],
                          exclude_errors=['mutation_write_timeout_exception', 'no_such_column_family'])

    def test_fetch_mv_after_recreate(self):
        """ Validate it's allowed to fetch from MV after it is dropped and recreated
        """
        session = self.prepare(rf=3, nodes=3)
        tm = TableManager(session, self.cluster,
                          columns={'int': {'amount': 2, 'frozen': False,
                                           'value length': {'min': 1, 'max': 100}}
                                   }, cl_columns={})
        tm.create_table()

        mv = MaterializedViewManager(tm)
        mv.create_materialized_view(mv_pk_column={'type': 'int'})

        tm.prefill_table(100)

        query = 'select * from {}'
        assert_two_queries_equal_ignore_order(session, query.format(tm.table_name),
                                              session, query.format(mv.mv_name),
                                              consistency_level=ConsistencyLevel.ALL, session_timeout=self.session_timeout)
        logger.debug("Drop materialized view and create another with the same name")
        mv.drop_mv()
        mv = MaterializedViewManager(tm)
        mv.create_materialized_view(mv_pk_column={'type': 'int'})

        assert_two_queries_equal_ignore_order(session, query.format(tm.table_name),
                                              session, query.format(mv.mv_name),
                                              consistency_level=ConsistencyLevel.ALL, session_timeout=self.session_timeout)

    @pytest.mark.next_gating
    @pytest.mark.dtest_debug
    def test_create(self):
        """Test the materialized view creation"""

        session = self.prepare(user_table=True)

        result = list(session.execute(("SELECT * FROM system_schema.views "
                                       "WHERE keyspace_name='ks' ALLOW FILTERING")))
        assert len(result) == 1, "Expecting 1 materialized view, got" + str(result)

    def test_gcgs_validation(self):
        """Verify that it's not possible to create or set a too low gc_grace_seconds on MVs"""
        session = self.prepare(user_table=True)

        # Shouldn't be able to alter the gc_grace_seconds of the base table to 0
        assert_invalid(session,
                       "ALTER TABLE users WITH gc_grace_seconds = 0",
                       "Cannot alter gc_grace_seconds of the base table of a materialized view "
                       "to 0, since this value is used to TTL undelivered updates. Setting "
                       "gc_grace_seconds too low might cause undelivered updates to expire "
                       "before being replayed.")

        # But can alter the gc_grace_seconds of the bease table to a value != 0
        session.execute("ALTER TABLE users WITH gc_grace_seconds = 10")

        # Shouldn't be able to alter the gc_grace_seconds of the MV to 0
        assert_invalid(session,
                       "ALTER MATERIALIZED VIEW users_by_state WITH gc_grace_seconds = 0",
                       "Cannot alter gc_grace_seconds of a materialized view to 0, since "
                       "this value is used to TTL undelivered updates. Setting gc_grace_seconds "
                       "too low might cause undelivered updates to expire before being replayed.")

        # Now let's drop MV
        session.execute("DROP MATERIALIZED VIEW ks.users_by_state;")

        # Now we should be able to set the gc_grace_seconds of the base table to 0
        session.execute("ALTER TABLE users WITH gc_grace_seconds = 0")

        # Now we shouldn't be able to create a new MV on this table
        assert_invalid(session,
                       "CREATE MATERIALIZED VIEW users_by_state AS "
                       "SELECT * FROM users WHERE STATE IS NOT NULL AND username IS NOT NULL "
                       "PRIMARY KEY (state, username)",
                       "Cannot create materialized view 'users_by_state' for base table 'users' "
                       "with gc_grace_seconds of 0, since this value is used to TTL undelivered "
                       "updates. Setting gc_grace_seconds too low might cause undelivered updates"
                       " to expire before being replayed.")

    def test_insert(self):
        """Test basic insertions"""

        session = self.prepare(user_table=True)

        self._insert_data(session)

        result = list(session.execute("SELECT * FROM users;"))
        assert len(result) == 4, "Expecting {} users, got {}".format(4, len(result))

        result = list(session.execute("SELECT * FROM users_by_state;"))
        assert len(result) == 4, "Expecting {} users, got {}".format(4, len(result))

        result = list(session.execute("SELECT * FROM users_by_state WHERE state='TX';"))
        assert len(result) == 2, "Expecting {} users, got {}".format(2, len(result))

        result = list(session.execute("SELECT * FROM users_by_state WHERE state='CA';"))
        assert len(result) == 1, "Expecting {} users, got {}".format(1, len(result))

        result = list(session.execute("SELECT * FROM users_by_state WHERE state='MA';"))
        assert len(result) == 0, "Expecting {} users, got {}".format(0, len(result))

    def test_populate_mv_after_insert(self):
        """Test that a view is OK when created with existing data"""

        session = self.prepare()

        session.execute("CREATE TABLE t (id int PRIMARY KEY, v int)")

        for i in range(1000):
            session.execute("INSERT INTO t (id, v) VALUES ({v}, {v})".format(v=i))

        session.execute(("CREATE MATERIALIZED VIEW t_by_v AS SELECT * FROM t WHERE v IS NOT NULL "
                         "AND id IS NOT NULL PRIMARY KEY (v, id)"))

        wait_for_view(cluster=self.cluster, session=session, ks='ks', view='t_by_v')

        for i in range(1000):
            assert_one(session, "SELECT * FROM t_by_v WHERE v = {}".format(i), [i, i])

    def test_populate_mv_after_insert_wide_rows(self):
        """Test that a view is OK when created with existing data with wide rows"""

        session = self.prepare()

        session.execute("CREATE TABLE t (id int, v int, PRIMARY KEY (id, v))")

        for i in range(5):
            for j in range(10000):
                session.execute("INSERT INTO t (id, v) VALUES ({}, {})".format(i, j))

        session.execute(("CREATE MATERIALIZED VIEW t_by_v AS SELECT * FROM t WHERE v IS NOT NULL "
                         "AND id IS NOT NULL PRIMARY KEY (v, id)"))

        wait_for_view(cluster=self.cluster, session=session, ks='ks', view='t_by_v')

        for i in range(5):
            for j in range(10000):
                assert_one(session, "SELECT * FROM t_by_v WHERE id = {} AND v = {}".format(i, j), [j, i])

    @pytest.mark.require('2431')
    def test_crc_check_chance(self):
        """Test that crc_check_chance parameter is properly populated after mv creation and update"""

        session = self.prepare()

        session.execute("CREATE TABLE t (id int PRIMARY KEY, v int)")
        session.execute(("CREATE MATERIALIZED VIEW t_by_v AS SELECT * FROM t WHERE v IS NOT NULL "
                         "AND id IS NOT NULL PRIMARY KEY (v, id) WITH crc_check_chance = 0.5"))

        assert_crc_check_chance_equal(session, "t_by_v", 0.5, view=True)

        session.execute("ALTER MATERIALIZED VIEW t_by_v WITH crc_check_chance = 0.3")

        assert_crc_check_chance_equal(session, "t_by_v", 0.3, view=True)

    def test_prepared_statement(self):
        """Test basic insertions with prepared statement"""

        session = self.prepare(user_table=True)

        insertPrepared = session.prepare(
            "INSERT INTO users (username, password, gender, state, birth_year) VALUES (?, ?, ?, ?, ?);"
        )
        selectPrepared = session.prepare(
            "SELECT state, password, session_token FROM users_by_state WHERE state=?;"
        )

        # insert data
        session.execute(insertPrepared.bind(('user1', 'ch@ngem3a', 'f', 'TX', 1968)))
        session.execute(insertPrepared.bind(('user2', 'ch@ngem3b', 'm', 'CA', 1971)))
        session.execute(insertPrepared.bind(('user3', 'ch@ngem3c', 'f', 'FL', 1978)))
        session.execute(insertPrepared.bind(('user4', 'ch@ngem3d', 'm', 'TX', 1974)))

        result = list(session.execute("SELECT * FROM users;"))
        assert len(result) == 4, "Expecting {} users, got {}".format(4, len(result))

        result = list(session.execute(selectPrepared.bind(['TX'])))
        assert len(result) == 2, "Expecting {} users, got {}".format(2, len(result))

        result = list(session.execute(selectPrepared.bind(['CA'])))
        assert len(result) == 1, "Expecting {} users, got {}".format(1, len(result))

        result = list(session.execute(selectPrepared.bind(['MA'])))
        assert len(result) == 0, "Expecting {} users, got {}".format(0, len(result))

    def test_immutable(self):
        """Test that a materialized view is immutable"""

        session = self.prepare(user_table=True)

        # cannot insert into view
        assert_invalid(session, "INSERT INTO users_by_state (state, username) VALUES ('TX', 'user1');",
                       "Cannot directly modify a materialized view")

        # cannot update view
        assert_invalid(session,
                       "UPDATE users_by_state SET session_token='XYZ' WHERE username='user1' AND state = 'TX';",
                       "Cannot directly modify a materialized view")

        # cannot delete a row in the view
        assert_invalid(session, "DELETE from users_by_state where state='TX';",
                       "Cannot directly modify a materialized view")

        # cannot delete a cell in the view
        assert_invalid(session, "DELETE session_token from users_by_state where state='TX' and username='user1';",
                       "Cannot directly modify a materialized view")

        # cannot alter a view
        assert_invalid(session, "ALTER TABLE users_by_state ADD first_name varchar",
                       "Cannot use ALTER TABLE on Materialized View")

    def test_immutable_truncate_mv(self):
        """Test that a materialized view is immutable"""
        session = self.prepare(user_table=True)
        session.execute("INSERT INTO users (state, username) VALUES ('TX', 'user1')")

        # cannot truncate a view
        assert_invalid(session, "TRUNCATE table users_by_state",
                       "Cannot TRUNCATE materialized view directly")

    def test_truncate_base(self):
        """Test truncate base table and as result - materialized view"""
        session = self.prepare(user_table=True)
        session.execute("INSERT INTO users (state, username) VALUES ('TX', 'user1')")

        # ensure sstables are created and will be dropped
        self.cluster.flush()

        # ensure data is loaded into cache and the cache will be cleared
        assert_one(session, "SELECT * FROM users_by_state", ['TX', 'user1', None, None, None, None])

        session.execute("TRUNCATE table users")

        self._assert_count_table_mv(session, 'users', 0, 'users_by_state', 0)

    def test_drop_mv(self):
        """Test that we can drop a view properly"""

        session = self.prepare(user_table=True)

        # create another materialized view
        session.execute(("CREATE MATERIALIZED VIEW users_by_birth_year AS "
                         "SELECT * FROM users WHERE birth_year IS NOT NULL AND "
                         "username IS NOT NULL PRIMARY KEY (birth_year, username)"))

        result = list(session.execute(("SELECT * FROM system_schema.views "
                                       "WHERE keyspace_name='ks'")))
        assert len(result) == 2, "Expecting {} materialized view, got {}".format(2, len(result))

        session.execute("DROP MATERIALIZED VIEW ks.users_by_state;")

        result = list(session.execute(("SELECT * FROM system_schema.views "
                                       "WHERE keyspace_name='ks'")))
        assert len(result) == 1, "Expecting {} materialized view, got {}".format(1, len(result))

    def test_drop_column(self):
        """Test that we cannot drop a column if it is used by a MV"""

        session = self.prepare(user_table=True)

        result = list(session.execute(("SELECT * FROM system_schema.views "
                                       "WHERE keyspace_name='ks'")))
        assert len(result) == 1, "Expecting {} materialized view, got {}".format(1, len(result))

        assert_invalid(
            session,
            "ALTER TABLE ks.users DROP state;",
            "Cannot drop column state from base table ks.users: materialized view users_by_state needs this column"
        )

    def test_drop_table(self):
        """Test that we cannot drop a table without deleting its MVs first"""

        session = self.prepare(user_table=True)

        result = list(session.execute(("SELECT * FROM system_schema.views "
                                       "WHERE keyspace_name='ks'")))
        assert len(result) == 1, f"Expecting 1 materialized view, got {len(result)}"

        assert_invalid(
            session,
            "DROP TABLE ks.users;",
            "Cannot drop table when materialized views still depend on it"
        )

        result = list(session.execute(("SELECT * FROM system_schema.views "
                                       "WHERE keyspace_name='ks'")))
        assert len(result) == 1, "Expecting {} materialized view, got {}".format(1, len(result))

        session.execute("DROP MATERIALIZED VIEW ks.users_by_state;")
        session.execute("DROP TABLE ks.users;")

        result = list(session.execute(("SELECT * FROM system_schema.views "
                                       "WHERE keyspace_name='ks'")))
        assert len(result) == 0, "Expecting {} materialized view, got {}".format(1, len(result))

    def test_clustering_column(self):
        """Test that we can use clustering columns as primary key for a materialized view"""

        session = self.prepare()

        session.execute(("CREATE TABLE users (username varchar, password varchar, gender varchar, "
                         "session_token varchar, state varchar, birth_year bigint, "
                         "PRIMARY KEY (username, state, birth_year));"))

        # create a materialized view that use a compound key
        session.execute(("CREATE MATERIALIZED VIEW users_by_state_birth_year "
                         "AS SELECT * FROM users WHERE state IS NOT NULL AND birth_year IS NOT NULL "
                         "AND username IS NOT NULL PRIMARY KEY (state, birth_year, username)"))

        session.cluster.control_connection.wait_for_schema_agreement()

        self._insert_data(session)

        result = list(session.execute("SELECT * FROM ks.users_by_state_birth_year WHERE state='TX'"))
        assert len(result) == 2, "Expecting {} users, got {}".format(2, len(result))

        result = list(
            session.execute("SELECT * FROM ks.users_by_state_birth_year WHERE state='TX' AND birth_year=1968"))
        assert len(result) == 1, "Expecting {} users, got {}".format(1, len(result))

    def _add_dc_after_mv_test(self, rf):
        """
        @jira_ticket CASSANDRA-10978
        Add datacenter with configurable replication.
        """

        session = self.prepare(rf=rf)

        logger.debug("Creating schema")
        session.execute("CREATE TABLE t (id int PRIMARY KEY, v int)")
        session.execute(("CREATE MATERIALIZED VIEW t_by_v AS SELECT * FROM t "
                         "WHERE v IS NOT NULL AND id IS NOT NULL PRIMARY KEY (v, id)"))

        logger.debug("Writing 1k to base")
        for i in range(1000):
            session.execute("INSERT INTO t (id, v) VALUES ({id}, {v})".format(id=i, v=-i))

        logger.debug("Reading 1k from view")
        for i in range(1000):
            self.eventually_assert_one(session, "SELECT * FROM t_by_v WHERE v = {}".format(-i), [-i, i])

        logger.debug("Reading 1k from base")
        for i in range(1000):
            assert_one(session, "SELECT * FROM t WHERE id = {}".format(i), [i, -i])

        logger.debug("Bootstrapping new node in another dc")
        # We are adding a new dc, to follow the add dc procedure, we should
        # bootstrap the node, then modify the rf to use the new dc, then rebuild
        # https://docs.scylladb.com/operating-scylla/procedures/cluster-management/add_dc_to_exist_dc/
        node4 = self.cluster.new_node(4, data_center='dc2')
        node4.start(wait_other_notice=True, wait_for_binary_proto=True)
        session.execute("ALTER KEYSPACE ks WITH REPLICATION = {'class':'NetworkTopologyStrategy', 'dc1':1, 'dc2':1};")
        node4.nodetool('rebuild -- dc1')

        logger.debug("Bootstrapping new node in another dc")
        node5 = self.cluster.new_node(5, data_center='dc2')
        node5.start()

        session2 = self.patient_exclusive_cql_connection(node4)

        logger.debug("Verifying data from new node in view")
        for i in range(1000):
            self.eventually_assert_one(session2, "SELECT * FROM ks.t_by_v WHERE v = {}".format(-i), [-i, i])

        logger.debug("Inserting 100 into base")
        for i in range(1000, 1100):
            session.execute("INSERT INTO t (id, v) VALUES ({id}, {v})".format(id=i, v=-i))

        logger.debug("Verify 100 in view")
        for i in range(1000, 1100):
            self.eventually_assert_one(session, "SELECT * FROM t_by_v WHERE v = {}".format(-i), [-i, i])

        self.check_errors(node=self.cluster.nodelist()[0],
                          exclude_errors='migration_task - Can''t send migration request')

    def test_add_node_during_base_table_update(self):
        """ Test expand cluster - add one node during MV updates
            Test starts with a starting size: one DCs with 4 nodes, and add new node to the same DC during update existent records of base
            table that cause to update materialized view as well.
            Verify that MV records are according to the base table
        """
        session = self.prepare()
        # Create table
        tm = TableManager(session, self.cluster,
                          columns={'int': {'amount': 1, 'frozen': False, 'value length': {'min': 1, 'max': 10000}},
                                   'text': {'amount': 1, 'frozen': False, 'value length': {'min': 1, 'max': 100}}
                                   }, pk_columns={}, cl_columns={}
                          )
        tm.create_table()

        # Create materialized view
        mv = MaterializedViewManager(tm)
        mv.create_materialized_view(mv_columns=None, mv_pk_column={'type': 'int'})

        # Pre-fill table
        prefill = 5000
        tm.prefill_table(prefill)

        # Check if all records were saved
        self._assert_count_table_mv(session, tm.table_name, prefill, mv.mv_name, prefill)

        # Run update base table and add new node in the parallel
        update_to = 5000
        proc_functions = ([{'func': self._add_new_node, 'args': (), 'kwargs': {}}]
                          + [{'func': tm.update_table, 'args': ({'by type': {'int': update_to}},
                                                                {'by name': {'id': {'operator': 'in',
                                                                                    'value': [i for i in range(start,
                                                                                                               start + 100)]}}}),
                              'kwargs': {'delay': 5}}
                             for start in range(100, 4900, 100)])
        results = run_in_parallel(proc_functions)

        # Receive the results
        new_node_session = set_clause = None
        for result in results:
            if isinstance(result, tuple):
                set_clause, _ = result
            else:
                new_node_session = result

        # Validate the results
        assert new_node_session and set_clause, \
            'Can\'t run the test. Reason: {}'.format('; '.join(msg[1]
                                                               for msg in
                                                               [[new_node_session, 'new session has been not created'],
                                                                [set_clause, 'SET clause is empty']] if not msg[0]))

        select = '{}'.format(', '.join(clmn for clmn in set_clause))
        statement_template = 'select %s from %s.{0}' % (select, tm.keyspace)
        assert_two_queries_equal(session, statement_template.format(tm.table_name), new_node_session,
                                 statement_template.format(mv.mv_name), consistency_level=ConsistencyLevel.QUORUM,
                                 group=True, groupby_column1=select, groupby_column2=select)

    def _add_new_node(self, data_center='dc1', wait_for_binary_proto=True, wait_other_notice=False, jvm_args=None,
                      configuration_options=None, delay=0, new_node_index=None):
        time.sleep(delay)
        i = len(self.cluster.nodes) + 1 if not new_node_index else new_node_index
        node = self.cluster.new_node(i=i, data_center=data_center)
        if configuration_options:
            node.set_configuration_options(values=configuration_options)  # CASSANDRA-11670
        logger.debug("Start join at {}".format(time.strftime("%H:%M:%S")))
        node.start(wait_for_binary_proto=wait_for_binary_proto, wait_other_notice=wait_other_notice, jvm_args=jvm_args)
        session = self.patient_exclusive_cql_connection(node)
        logger.debug("Finish join at {}".format(time.strftime("%H:%M:%S")))
        return session

    def test_add_dc_after_mv_simple_replication(self):
        """
        @jira_ticket CASSANDRA-10634

        Test that materialized views work as expected when adding a datacenter with SimpleStrategy.
        """

        self._add_dc_after_mv_test(1)

    def test_add_dc_after_mv_network_replication(self):
        """
        @jira_ticket CASSANDRA-10634

        Test that materialized views work as expected when adding a datacenter with NetworkTopologyStrategy.
        """

        self._add_dc_after_mv_test({'dc1': 1})

    def test_add_node_after_mv(self):
        """
        @jira_ticket CASSANDRA-10978
        Test that materialized views work as expected when adding a node.
        """

        session = self.prepare()

        session.execute("CREATE TABLE t (id int PRIMARY KEY, v int)")
        session.execute(("CREATE MATERIALIZED VIEW t_by_v AS SELECT * FROM t "
                         "WHERE v IS NOT NULL AND id IS NOT NULL PRIMARY KEY (v, id)"))

        for i in range(1000):
            session.execute("INSERT INTO t (id, v) VALUES ({id}, {v})".format(id=i, v=-i))

        for i in range(1000):
            assert_one(session, "SELECT * FROM t_by_v WHERE v = {}".format(-i), [-i, i])

        session2 = self._add_new_node(data_center='dc1')

        for i in range(1000):
            assert_one(session2, "SELECT * FROM ks.t_by_v WHERE v = {}".format(-i), [-i, i])

        for i in range(1000, 1100):
            session.execute("INSERT INTO t (id, v) VALUES ({id}, {v})".format(id=i, v=-i))

        for i in range(1000, 1100):
            assert_one(session, "SELECT * FROM t_by_v WHERE v = {}".format(-i), [-i, i])

    @pytest.mark.resource_intensive
    def test_add_node_after_wide_mv_with_range_deletions(self):
        """
        Taken from Cassandra
        @jira_ticket CASSANDRA-11670
        Test that materialized views work with wide materialized views as expected when adding a node.
        commitlog_segment_size_in_mb=32 and no defined max_mutation_size_in_kb
        """

        session = self.prepare()

        session.execute("CREATE TABLE t (id int, v int, PRIMARY KEY (id, v)) WITH compaction = { "
                        "'class': 'SizeTieredCompactionStrategy', 'enabled': 'false' }")
        session.execute(("CREATE MATERIALIZED VIEW t_by_v AS SELECT * FROM t "
                         "WHERE v IS NOT NULL AND id IS NOT NULL PRIMARY KEY (v, id)"))

        for i in range(10):
            for j in range(100):
                session.execute("INSERT INTO t (id, v) VALUES ({id}, {v})".format(id=i, v=j))

        self.cluster.flush()

        for i in range(10):
            for j in range(100):
                assert_one(session, "SELECT * FROM t WHERE id = {} and v = {}".format(i, j), [i, j])
                assert_one(session, "SELECT * FROM t_by_v WHERE id = {} and v = {}".format(i, j), [j, i])

        for i in range(10):
            for j in range(100):
                if j % 10 == 0:
                    session.execute("DELETE FROM t WHERE id = {} AND v >= {} and v < {}".format(i, j, j + 2))

        self.cluster.flush()

        for i in range(10):
            for j in range(100):
                if j % 10 == 0 or (j - 1) % 10 == 0:
                    assert_none(session, "SELECT * FROM t WHERE id = {} and v = {}".format(i, j))
                    assert_none(session, "SELECT * FROM t_by_v WHERE id = {} and v = {}".format(i, j))
                else:
                    assert_one(session, "SELECT * FROM t WHERE id = {} and v = {}".format(i, j), [i, j])
                    assert_one(session, "SELECT * FROM t_by_v WHERE id = {} and v = {}".format(i, j), [j, i])

        # Scylla does not support migration_task_wait_in_seconds and max_mutation_size_in_kb parameters
        session2 = self._add_new_node(wait_for_binary_proto=True
                                      # , jvm_args=["-Dcassandra.migration_task_wait_in_seconds={}".format(MIGRATION_WAIT)],
                                      # configuration_options={'max_mutation_size_in_kb': 20}
                                      )
        for i in range(10):
            for j in range(100):
                if j % 10 == 0 or (j - 1) % 10 == 0:
                    assert_none(session2, "SELECT * FROM ks.t WHERE id = {} and v = {}".format(i, j))
                    assert_none(session2, "SELECT * FROM ks.t_by_v WHERE id = {} and v = {}".format(i, j))
                else:
                    assert_one(session2, "SELECT * FROM ks.t WHERE id = {} and v = {}".format(i, j), [i, j])
                    assert_one(session2, "SELECT * FROM ks.t_by_v WHERE id = {} and v = {}".format(i, j), [j, i])

        for i in range(10):
            for j in range(100, 110):
                session.execute("INSERT INTO t (id, v) VALUES ({id}, {v})".format(id=i, v=j))

        for i in range(10):
            for j in range(110):
                if j < 100 and (j % 10 == 0 or (j - 1) % 10 == 0):
                    assert_none(session2, "SELECT * FROM ks.t WHERE id = {} and v = {}".format(i, j))
                    assert_none(session2, "SELECT * FROM ks.t_by_v WHERE id = {} and v = {}".format(i, j))
                else:
                    assert_one(session2, "SELECT * FROM ks.t WHERE id = {} and v = {}".format(i, j), [i, j])
                    assert_one(session2, "SELECT * FROM ks.t_by_v WHERE id = {} and v = {}".format(i, j), [j, i])

    # @pytest.mark.skip('unrecognised option \'-Dcassandra.migration_task_wait_in_second\'')
    @pytest.mark.resource_intensive
    def test_add_node_after_very_wide_mv(self):
        """
        Taken from Cassandra
        @jira_ticket CASSANDRA-11670
        Test that materialized views work with very wide materialized views as expected when adding a node.
        commitlog_segment_size_in_mb=32 and no defined max_mutation_size_in_kb
        """

        session = self.prepare()

        session.execute("CREATE TABLE t (id int, v int, PRIMARY KEY (id, v))")
        session.execute(("CREATE MATERIALIZED VIEW t_by_v AS SELECT * FROM t "
                         "WHERE v IS NOT NULL AND id IS NOT NULL PRIMARY KEY (v, id)"))

        for i in range(5):
            for j in range(5000):
                session.execute("INSERT INTO t (id, v) VALUES ({id}, {v})".format(id=i, v=j))

        self.cluster.flush()

        for i in range(5):
            for j in range(5000):
                assert_one(session, "SELECT * FROM t_by_v WHERE id = {} and v = {}".format(i, j), [j, i])

        # Scylla does not support migration_task_wait_in_seconds and max_mutation_size_in_kb parameters
        session2 = self._add_new_node(wait_for_binary_proto=True
                                      # , jvm_args=["-Dcassandra.migration_task_wait_in_seconds={}".format(MIGRATION_WAIT)],
                                      # configuration_options={'max_mutation_size_in_kb': 20}
                                      )
        for i in range(5):
            for j in range(5000):
                assert_one(session2, "SELECT * FROM ks.t_by_v WHERE id = {} and v = {}".format(i, j), [j, i])

        for i in range(5):
            for j in range(5100):
                session.execute("INSERT INTO t (id, v) VALUES ({id}, {v})".format(id=i, v=j))

        for i in range(5):
            for j in range(5100):
                assert_one(session, "SELECT * FROM t_by_v WHERE id = {} and v = {}".format(i, j), [j, i])

    @pytest.mark.skip("unrecognised option '-Dcassandra.write_survey=true'")
    def test_add_write_survey_node_after_mv(self):
        """
        @jira_ticket CASSANDRA-10621

        Test that materialized views work as expected when adding a node in write survey mode.
        """

        session = self.prepare()

        session.execute("CREATE TABLE t (id int PRIMARY KEY, v int)")
        session.execute(("CREATE MATERIALIZED VIEW t_by_v AS SELECT * FROM t "
                         "WHERE v IS NOT NULL AND id IS NOT NULL PRIMARY KEY (v, id)"))

        for i in range(1000):
            session.execute("INSERT INTO t (id, v) VALUES ({id}, {v})".format(id=i, v=-i))

        for i in range(1000):
            assert_one(session, "SELECT * FROM t_by_v WHERE v = {}".format(-i), [-i, i])

        node4 = self.cluster.new_node(i=4, data_center='dc1')
        node4.start(wait_for_binary_proto=True, jvm_args=["-Dcassandra.write_survey=true"])

        for i in range(1000, 1100):
            session.execute("INSERT INTO t (id, v) VALUES ({id}, {v})".format(id=i, v=-i))

        for i in range(1100):
            assert_one(session, "SELECT * FROM t_by_v WHERE v = {}".format(-i), [-i, i])

    def test_allow_filtering(self):
        """Test that allow filtering works as usual for a materialized view"""

        session = self.prepare()

        session.execute("CREATE TABLE t (id int PRIMARY KEY, v int, v2 text, v3 decimal)")
        session.execute(("CREATE MATERIALIZED VIEW t_by_v AS SELECT * FROM t "
                         "WHERE v IS NOT NULL AND id IS NOT NULL PRIMARY KEY (v, id)"))
        session.execute(("CREATE MATERIALIZED VIEW t_by_v2 AS SELECT * FROM t "
                         "WHERE v2 IS NOT NULL AND id IS NOT NULL PRIMARY KEY (v2, id)"))

        for i in range(1000):
            session.execute("INSERT INTO t (id, v, v2, v3) VALUES ({v}, {v}, 'a', 3.0)".format(v=i))

        for i in range(1000):
            assert_one(session, "SELECT * FROM t_by_v WHERE v = {v}".format(v=i), [i, i, 'a', 3.0])

        rows = list(session.execute("SELECT * FROM t_by_v2 WHERE v2 = 'a'"))
        assert len(rows) == 1000, "Expected 1000 rows but got {}".format(len(rows))

        assert_invalid(session, "SELECT * FROM t_by_v WHERE v = 1 AND v2 = 'a'", expected=Exception)
        assert_invalid(session, "SELECT * FROM t_by_v2 WHERE v2 = 'a' AND v = 1", expected=Exception)

        for i in range(1000):
            assert_one(
                session,
                "SELECT * FROM t_by_v WHERE v = {} AND v3 = 3.0 ALLOW FILTERING".format(i),
                [i, i, 'a', 3.0]
            )
            assert_one(
                session,
                "SELECT * FROM t_by_v2 WHERE v2 = 'a' AND v = {} ALLOW FILTERING".format(i),
                ['a', i, i, 3.0]
            )

    def test_secondary_index(self):
        """Test that secondary indexes cannot be created on a materialized view"""

        session = self.prepare()

        session.execute("CREATE TABLE t (id int PRIMARY KEY, v int, v2 text, v3 decimal)")
        session.execute(("CREATE MATERIALIZED VIEW t_by_v AS SELECT * FROM t "
                         "WHERE v IS NOT NULL AND id IS NOT NULL PRIMARY KEY (v, id)"))
        assert_invalid(session, "CREATE INDEX ON t_by_v (v2)",
                       "Secondary indexes are not supported on materialized views")

    # Restriction validation is allowed in S* but fails. Should be disable to be compatible with C*
    def test_restriction_on_non_mv_pk(self):
        """
        Test that clear message is sent when WHERE filtering criteria is applayed on columns that are not part of the base table's
        primary key
        """
        session = self.prepare()
        tm = TableManager(session, self.cluster,
                          columns={'int': {'amount': 2, 'frozen': False, 'value length': {'min': 1, 'max': 10000}}
                                   }, pk_columns={}, cl_columns={}
                          )
        tm.create_table()

        # Create materialized view
        mv = MaterializedViewManager(tm)
        pks = mv.create_mv_pk_list({'type': 'int'})
        try:
            mv.create_materialized_view(mv_columns={'int': {'amount': 1}}, mv_pk_column={'names': pks},
                                        mv_where_restriction={'names': {pks[-1]: {'operator': '>', 'value': 1}}},
                                        wait_for_view_built=False)
        except Exception as e:
            expected_error = 'Non-primary key columns cannot be restricted in the SELECT statement used for materialized ' \
                             'view creation (got restrictions on: {})'.format(next(mv.mv_where_restriction.keys()))
            assert str(e) == expected_error, '\nExpected error: {0}.\n Received error: {1}'.format(
                expected_error, str(e))

    def test_ttl_remove_with_non_mv_column(self):
        """
            Pre-condition:
             - base table with materialized view
             - one column from base table is not included into materialized view
             - record is inserted into base table with TTL = 60 ( the record will be expired after 60 seconds)
            Test case: non-view column is updated with TTL = 0 ( the record won't be expired)
            Expected result: the record exists in both base table and materialized view
        """
        session = self.prepare()
        session.execute('USE ks')
        table_name = 'base'
        mv_name = 'mv'
        # Create base table
        query = 'CREATE TABLE %s (p int, c int, v int, PRIMARY KEY (p, c))' % table_name
        logger.debug(query)
        session.execute(query)

        # Create materialized view
        query = 'CREATE MATERIALIZED VIEW %s AS SELECT p, c FROM base WHERE p IS NOT NULL ' \
                'AND c IS NOT NULL PRIMARY KEY (c, p)' % mv_name
        logger.debug(query)
        session.execute(query)
        wait_for_view(cluster=self.cluster, session=session, ks='ks', view=mv_name)

        # Pre-fill table
        prefill = 1
        ttl = 60
        query = 'INSERT INTO %s (p, c) VALUES (0, 0) USING TTL %d' % (table_name, ttl)
        logger.debug(query)
        session.execute(query)

        # Check if a record was saved in both table and materialized view
        self._assert_count_table_mv(session, table_name, prefill, mv_name, prefill)

        # Run update base table - remove TTL from whole record by removing TTL from non-MV column
        query = 'UPDATE %s USING TTL 0 SET v = 0 WHERE p = 0 and c = 0' % table_name
        logger.debug(query)
        session.execute(query)

        self._assert_count_table_mv(session, table_name, prefill, mv_name, prefill)

        # Wait for more than TTL time
        time.sleep(ttl + 5)

        # Check if the record still exists in the both table and materialized view
        self._assert_count_table_mv(session, table_name, prefill, mv_name, prefill)

    def test_ttl_set_with_non_mv_column_updated(self):
        """
            Pre-condition:
             - base table with materialized view
             - one column from base table is not included into materialized view
             - record is inserted into base table with no TTL ( the record won't be expired)
            Test case: non-view column is updated with TTL = 20 ( the column value will be expired after 20 seconds)
            Expected result: the record exists in both base table and materialized view
        """
        session = self.prepare()
        tm = TableManager(session, self.cluster,
                          columns={'int': {'amount': 2, 'frozen': False, 'value length': {'min': 1, 'max': 10000}}
                                   }, pk_columns={}, cl_columns={}
                          )
        tm.create_table()

        # Create materialized view
        mv = MaterializedViewManager(tm)
        mv.create_materialized_view(mv_columns={'int': {'amount': 1}}, mv_pk_column={'type': 'int'})

        # Pre-fill table
        prefill = 1
        tm.prefill_table(prefill)

        # Check if a record was saved in both table and materialized view
        self._assert_count_table_mv(session, tm.table_name, prefill, mv.mv_name, prefill)

        # Run update base table with TTL
        ttl = 20
        tm.update_table({'by type': {'int': 0}}, {'by name': tm.get_value_for_filter()}, using_clause={'ttl': ttl},
                        update_columns_exclude=mv.mv_columns_list)
        self._assert_count_table_mv(session, tm.table_name, prefill, mv.mv_name, prefill)

        # Wait for more than TTL time
        time.sleep(ttl + 5)

        # Check if the record still exists in the both table and materialized view
        self._assert_count_table_mv(session, tm.table_name, prefill, mv.mv_name, prefill)

    def test_ttl_set_with_mv_pk_column_updated(self):
        """
            Pre-condition:
             - base table with materialized view
             - one column from base table is not included into materialized view
             - record is inserted into base table with no TTL ( the record won't be expired)
            Test case: MV's PK column is updated with TTL = 5 ( the column value will be expired after 5 seconds)
            Expected result: the record exists in both base table and removed from materialized view
        """
        session = self.prepare()
        tm = TableManager(session, self.cluster,
                          columns={'int': {'amount': 2, 'frozen': False, 'value length': {'min': 1, 'max': 10000}}
                                   }, pk_columns={}, cl_columns={}
                          )
        tm.create_table()

        # Create materialized view
        mv = MaterializedViewManager(tm)
        mv.create_materialized_view(mv_columns={'int': {'amount': 1}}, mv_pk_column={'type': 'int'})

        # Pre-fill table
        prefill = 1
        tm.prefill_table(prefill)

        # Check if a record was saved in both table and materialized view
        self._assert_count_table_mv(session, tm.table_name, prefill, mv.mv_name, prefill)

        # Run update base table of MV's PK column with TTL
        ttl = 5
        upd_column = list(set(mv.mv_pk_list + mv.mv_cl_list) - set(tm.pk_list + tm.cl_list))[0]
        tm.update_table({'by name': {upd_column: 1}}, {'by name': tm.get_value_for_filter()}, using_clause={'ttl': ttl})
        self._assert_count_table_mv(session, tm.table_name, prefill, mv.mv_name, prefill)

        # Wait for more than TTL time
        time.sleep(ttl + 5)

        # Check if the record still exists in the both table and materialized view
        self._assert_count_table_mv(session, tm.table_name, prefill, mv.mv_name, prefill - 1)

    @staticmethod
    def _assert_count_table_mv(session, table_name, table_expected_count, mv_name, mv_expected_count,
                               cl=ConsistencyLevel.QUORUM):
        assert_row_count(session, table_name, table_expected_count, consistency_level=cl)
        assert_row_count(session, mv_name, mv_expected_count, consistency_level=cl)

    def test_ttl(self):
        """
        Test that TTL works as expected for a materialized view
        @expected_result The TTL is propagated properly between tables.
        """

        session = self.prepare()
        session.execute("CREATE TABLE t (id int PRIMARY KEY, v int, v2 int, v3 int)")
        session.execute(("CREATE MATERIALIZED VIEW t_by_v2 AS SELECT * FROM t "
                         "WHERE v2 IS NOT NULL AND id IS NOT NULL PRIMARY KEY (v2, id)"))

        for i in range(100):
            session.execute("INSERT INTO t (id, v, v2, v3) VALUES ({v}, {v}, {v}, {v}) USING TTL 10".format(v=i))

        for i in range(100):
            assert_one(session, "SELECT * FROM t_by_v2 WHERE v2 = {}".format(i), [i, i, i, i])

        time.sleep(20)

        rows = list(session.execute("SELECT * FROM t_by_v2"))
        assert len(rows) == 0, "Expected 0 rows but got {}".format(len(rows))

    def test_query_all_new_column(self):
        """
        Test that a materialized view created with a 'SELECT *' works as expected when adding a new column
        @expected_result The new column is present in the view.
        """

        session = self.prepare(user_table=True)

        self._insert_data(session)

        assert_one(
            session,
            "SELECT * FROM users_by_state WHERE state = 'TX' AND username = 'user1'",
            ['TX', 'user1', 1968, 'f', 'ch@ngem3a', None]
        )

        session.execute("ALTER TABLE users ADD first_name varchar;")

        results = list(session.execute("SELECT * FROM users_by_state WHERE state = 'TX' AND username = 'user1'"))
        assert len(results) == 1
        assert hasattr(results[0], 'first_name'), 'Column "first_name" not found'
        assert_one(
            session,
            "SELECT * FROM users_by_state WHERE state = 'TX' AND username = 'user1'",
            ['TX', 'user1', 1968, None, 'f', 'ch@ngem3a', None]
        )

    def test_query_new_column(self):
        """
        Test that a materialized view created with 'SELECT <col1, ...>' works as expected when adding a new column
        @expected_result The new column is not present in the view.
        """

        session = self.prepare(user_table=True)

        session.execute(("CREATE MATERIALIZED VIEW users_by_state2 AS SELECT username FROM users "
                         "WHERE STATE IS NOT NULL AND USERNAME IS NOT NULL PRIMARY KEY (state, username)"))

        self._insert_data(session)

        assert_one(
            session,
            "SELECT * FROM users_by_state2 WHERE state = 'TX' AND username = 'user1'",
            ['TX', 'user1']
        )

        session.execute("ALTER TABLE users ADD first_name varchar;")

        results = list(session.execute("SELECT * FROM users_by_state2 WHERE state = 'TX' AND username = 'user1'"))
        assert len(results) == 1
        assert not hasattr(results[0], 'first_name'), 'Column "first_name" found in view'
        assert_one(
            session,
            "SELECT * FROM users_by_state2 WHERE state = 'TX' AND username = 'user1'",
            ['TX', 'user1']
        )

    def test_rename_column(self):
        """
        Test that a materialized view created with a 'SELECT *' works as expected when renaming a column
        @expected_result The column is also renamed in the view.
        """

        session = self.prepare(user_table=True)

        self._insert_data(session)

        assert_one(
            session,
            "SELECT * FROM users_by_state WHERE state = 'TX' AND username = 'user1'",
            ['TX', 'user1', 1968, 'f', 'ch@ngem3a', None]
        )

        self.fixture_dtest_setup.ignore_log_patterns += [r'Column username in view .* was not found in the base table']
        session.execute("ALTER TABLE users RENAME username TO user")

        results = list(session.execute("SELECT * FROM users_by_state WHERE state = 'TX' AND user = 'user1'"))
        assert len(results) == 1
        assert hasattr(results[0], 'user'), 'Column "user" not found'
        assert_one(
            session,
            "SELECT state, user, birth_year, gender FROM users_by_state WHERE state = 'TX' AND user = 'user1'",
            ['TX', 'user1', 1968, 'f']
        )

    @pytest.mark.skip("Not relevant for Scylla - requires byteman error injection")
    def test_rename_column_atomicity(self):
        """
        Test that column renaming is atomically done between a table and its materialized views
        @jira_ticket CASSANDRA-12952
        """

        session = self.prepare(nodes=1, user_table=True, install_byteman=True)
        node = self.cluster.nodelist()[0]

        self._insert_data(session)

        assert_one(
            session,
            "SELECT * FROM users_by_state WHERE state = 'TX' AND username = 'user1'",
            ['TX', 'user1', 1968, 'f', 'ch@ngem3a', None]
        )

        # Rename a column with an injected byteman rule to kill the node after the first schema update
        script_version = '4x' if parse_version(self.cluster.version()) >= parse_version('4.0') else '3x'
        node.byteman_submit(['./byteman/merge_schema_failure_{}.btm'.format(script_version)])
        with pytest.raises(NoHostAvailable):
            session.execute("ALTER TABLE users RENAME username TO user")

        logger.debug('Restarting node')
        node.stop()
        node.start(wait_for_binary_proto=True)
        session = self.patient_cql_connection(node, consistency_level=ConsistencyLevel.ONE)

        # Both the table and its view should have the new schema after restart
        assert_one(
            session,
            "SELECT * FROM ks.users WHERE state = 'TX' AND user = 'user1' ALLOW FILTERING",
            ['user1', 1968, 'f', 'ch@ngem3a', None, 'TX']
        )
        assert_one(
            session,
            "SELECT * FROM ks.users_by_state WHERE state = 'TX' AND user = 'user1'",
            ['TX', 'user1', 1968, 'f', 'ch@ngem3a', None]
        )

    def test_lwt(self):
        """Test that lightweight transaction behave properly with a materialized view"""

        session = self.prepare()

        session.execute("CREATE TABLE t (id int PRIMARY KEY, v int, v2 text, v3 decimal)")
        session.execute(("CREATE MATERIALIZED VIEW t_by_v AS SELECT * FROM t "
                         "WHERE v IS NOT NULL AND id IS NOT NULL PRIMARY KEY (v, id)"))

        logger.debug("Inserting initial data using IF NOT EXISTS")
        for i in range(1000):
            session.execute(
                "INSERT INTO t (id, v, v2, v3) VALUES ({v}, {v}, 'a', 3.0) IF NOT EXISTS".format(v=i)
            )
        # Scylla doesn't leverage the batchlog for MVs
        # self._replay_batchlogs()

        logger.debug("All rows should have been inserted")
        for i in range(1000):
            assert_one(
                session,
                "SELECT * FROM t_by_v WHERE v = {}".format(i),
                [i, i, 'a', 3.0]
            )

        logger.debug("Tyring to UpInsert data with a different value using IF NOT EXISTS")
        for i in range(1000):
            v = i * 2
            session.execute(
                "INSERT INTO t (id, v, v2, v3) VALUES ({id}, {v}, 'a', 3.0) IF NOT EXISTS".format(id=i, v=v)
            )
        # self._replay_batchlogs()

        logger.debug("No rows should have changed")
        for i in range(1000):
            assert_one(
                session,
                "SELECT * FROM t_by_v WHERE v = {}".format(i),
                [i, i, 'a', 3.0]
            )

        logger.debug("Update the 10 first rows with a different value")
        for i in range(1000):
            v = i + 2000
            session.execute(
                "UPDATE t SET v={v} WHERE id = {id} IF v < 10".format(id=i, v=v)
            )
        # self._replay_batchlogs()

        logger.debug("Verify that only the 10 first rows changed.")
        results = list(session.execute("SELECT * FROM t_by_v;"))
        assert len(results) == 1000
        for i in range(1000):
            v = i + 2000 if i < 10 else i
            assert_one(
                session,
                "SELECT * FROM t_by_v WHERE v = {}".format(v),
                [v, i, 'a', 3.0]
            )

        logger.debug("Deleting the first 10 rows")
        for i in range(1000):
            v = i + 2000
            session.execute(
                "DELETE FROM t WHERE id = {id} IF v = {v} ".format(id=i, v=v)
            )
        # self._replay_batchlogs()

        logger.debug("Verify that only the 10 first rows have been deleted.")
        results = list(session.execute("SELECT * FROM t_by_v;"))
        assert len(results) == 990
        for i in range(10, 1000):
            assert_one(
                session,
                "SELECT * FROM t_by_v WHERE v = {}".format(i),
                [i, i, 'a', 3.0]
            )

    def test_do_not_finish_view_building_with_hints(self):
        """Test that in presence of view update hints, view building will not be marked as finished"""

        session = self.prepare(options={'hinted_handoff_enabled': False, 'shadow_round_ms': 1000})
        node1, node2, node3 = self.cluster.nodelist()

        session.execute("CREATE TABLE t (id int PRIMARY KEY, v int, v2 text, v3 decimal)")

        rows = 200000
        if hasattr(self.cluster, 'scylla_mode') and self.cluster.scylla_mode == 'debug':
            rows = 10000
        logger.debug("Inserting initial data")
        insert_stmt = session.prepare("INSERT INTO t (id, v, v2, v3) VALUES (?, ?, ?, ?)")
        for i in range(rows):
            session.execute(insert_stmt, (i, i, 'a', 3.0))

        logger.debug("Create a MV")
        # Don't wait for schema agreement, or we risk view building concluding too soon
        session.cluster.max_schema_agreement_wait = 0
        session.execute(("CREATE MATERIALIZED VIEW t_by_v AS SELECT * FROM t "
                         "WHERE v IS NOT NULL AND id IS NOT NULL PRIMARY KEY (v, id)"))

        self.cluster.stop_nodes([node2, node3])

        wait_for_view_build_start(session, "ks", "t_by_v")

        logger.debug("Ensure view building didn't finish.")
        for _ in range(10):
            self._ensure_view_building_did_not_finish(1)
            time.sleep(1)

        logger.debug("Restart the cluster")
        self.cluster.start_nodes([node2, node3], wait_other_notice=True, wait_for_binary_proto=True)

        logger.debug("Wait and ensure the MV build resumed.")
        wait_for_view(cluster=self.cluster, session=session, ks="ks", view="t_by_v")

        logger.debug("Verify all data")
        assert_row_count(session, 't_by_v', rows, consistency_level=ConsistencyLevel.ALL)

    def test_drop_while_building(self):
        """Test that a MV build is interrupted when the view is removed"""
        # Expected error due MV dropping during the building
        self.fixture_dtest_setup.ignore_log_patterns += [r'data_dictionary::no_such_column_family']

        session = self.prepare(options={'hinted_handoff_enabled': False})

        session.execute("CREATE TABLE t (id int PRIMARY KEY, v int, v2 text, v3 decimal)")

        rows = 200000
        if hasattr(self.cluster, 'scylla_mode') and self.cluster.scylla_mode == 'debug':
            rows = 10000
        logger.debug("Inserting initial data")
        insert_stmt = session.prepare("INSERT INTO t (id, v, v2, v3) VALUES (?, ?, ?, ?)")
        for i in range(rows):
            session.execute(insert_stmt, (i, i, 'a', 3.0))

        logger.debug("Create a MV")
        session.cluster.max_schema_agreement_wait = 1
        session.execute_async("CREATE MATERIALIZED VIEW t_by_v AS SELECT * FROM t "
                              "WHERE v IS NOT NULL AND id IS NOT NULL PRIMARY KEY (v, id)")

        self.ignore_log_patterns += [
            r'view - Error applying view update.*no_such_column_family',
        ]

        logger.debug("Waiting for view building to start.")
        for node in self.cluster.nodelist():
            node.watch_log_for("Building view ks.t_by_v")

        logger.debug("Drop the MV while it is still building")
        session.execute("DROP MATERIALIZED VIEW t_by_v")

        logger.debug("Verify view building never finished.")
        have_finished = 0
        for node in self.cluster.nodelist():
            finished = node.grep_log("Finished building view")
            have_finished += len(finished)
        logger.debug(f"{have_finished} / {len(self.cluster.nodelist())} views finished")
        assert have_finished < len(self.cluster.nodelist())

        assert_invalid(session, "SELECT COUNT(*) FROM t_by_v")
        if isinstance(self.cluster, ScyllaCluster):
            self.eventually_assert_none(session,
                                        "SELECT * FROM system.scylla_views_builds_in_progress")
        self.eventually_assert_none(session,
                                    "SELECT * FROM system.built_views")
        self.eventually_assert_none(session,
                                    "SELECT * FROM system.views_builds_in_progress")
        self.eventually_assert_none(session,
                                    "SELECT * FROM system_distributed.view_build_status")

    def test_mv_with_default_ttl_with_flush(self):
        self._test_mv_with_default_ttl(True)

    def test_mv_with_default_ttl_without_flush(self):
        self._test_mv_with_default_ttl(False)

    def _test_mv_with_default_ttl(self, flush):
        """
        Verify mv with default_time_to_live can be deleted properly using expired livenessInfo
        @jira_ticket CASSANDRA-14071
        """
        session = self.prepare(rf=3, nodes=3,
                               options={'hinted_handoff_enabled': False},
                               consistency_level=ConsistencyLevel.QUORUM)
        session.execute('USE ks')

        logger.debug("MV with same key and unselected columns")
        session.execute("CREATE TABLE t2 (k int, a int, b int, c int, primary key(k, a)) with default_time_to_live=600")
        session.execute(("CREATE MATERIALIZED VIEW mv2 AS SELECT k,a,b FROM t2 "
                         "WHERE k IS NOT NULL AND a IS NOT NULL PRIMARY KEY (a, k)"))
        session.cluster.control_connection.wait_for_schema_agreement()

        self.update_view(session, "UPDATE t2 SET c=1 WHERE k=1 AND a=1;", flush)
        self.eventually_assert_one(session, "SELECT k,a,b,c FROM t2", [1, 1, None, 1])
        self.eventually_assert_one(session, "SELECT k,a,b FROM mv2", [1, 1, None])

        self.update_view(session, "UPDATE t2 SET c=null WHERE k=1 AND a=1;", flush)
        self.eventually_assert_none(session, "SELECT k,a,b,c FROM t2")
        self.eventually_assert_none(session, "SELECT k,a,b FROM mv2")

        self.update_view(session, "UPDATE t2 SET c=2 WHERE k=1 AND a=1;", flush)
        self.eventually_assert_one(session, "SELECT k,a,b,c FROM t2", [1, 1, None, 2])
        self.eventually_assert_one(session, "SELECT k,a,b FROM mv2", [1, 1, None])

        self.update_view(session, "DELETE c FROM t2 WHERE k=1 AND a=1;", flush)
        self.eventually_assert_none(session, "SELECT k,a,b,c FROM t2")
        self.eventually_assert_none(session, "SELECT k,a,b FROM mv2")

        if flush:
            self.cluster.compact()
            assert_none(session, "SELECT * FROM t2")
            assert_none(session, "SELECT * FROM mv2")

        # test with user-provided ttl
        self.update_view(session, "INSERT INTO t2(k,a,b,c) VALUES(2,2,2,2) USING TTL 5", flush)
        self.update_view(session, "UPDATE t2 USING TTL 100 SET c=1 WHERE k=2 AND a=2;", flush)
        self.update_view(session, "UPDATE t2 USING TTL 50 SET c=2 WHERE k=2 AND a=2;", flush)
        self.update_view(session, "DELETE c FROM t2 WHERE k=2 AND a=2;", flush)

        time.sleep(6)

        self.eventually_assert_none(session, "SELECT k,a,b,c FROM t2")
        self.eventually_assert_none(session, "SELECT k,a,b FROM mv2")

        if flush:
            self.cluster.compact()
            assert_none(session, "SELECT * FROM t2")
            assert_none(session, "SELECT * FROM mv2")

        logger.debug("MV with extra key")
        session.execute("CREATE TABLE t (k int PRIMARY KEY, a int, b int) with default_time_to_live=600")
        session.execute(("CREATE MATERIALIZED VIEW mv AS SELECT * FROM t "
                         "WHERE k IS NOT NULL AND a IS NOT NULL PRIMARY KEY (k, a)"))
        session.cluster.control_connection.wait_for_schema_agreement()

        self.update_view(session, "INSERT INTO t (k, a, b) VALUES (1, 1, 1);", flush)
        self.eventually_assert_one(session, "SELECT * FROM t", [1, 1, 1])
        self.eventually_assert_one(session, "SELECT * FROM mv", [1, 1, 1])

        self.update_view(session, "INSERT INTO t (k, a, b) VALUES (1, 2, 1);", flush)
        self.eventually_assert_one(session, "SELECT * FROM t", [1, 2, 1])
        self.eventually_assert_one(session, "SELECT * FROM mv", [1, 2, 1])

        self.update_view(session, "INSERT INTO t (k, a, b) VALUES (1, 3, 1);", flush)
        self.eventually_assert_one(session, "SELECT * FROM t", [1, 3, 1])
        self.eventually_assert_one(session, "SELECT * FROM mv", [1, 3, 1])

        if flush:
            self.cluster.compact()
            assert_one(session, "SELECT * FROM t", [1, 3, 1])
            assert_one(session, "SELECT * FROM mv", [1, 3, 1])

        # user provided ttl
        self.update_view(session, "UPDATE t USING TTL 30 SET a = 4 WHERE k = 1", flush)
        self.eventually_assert_one(session, "SELECT * FROM t", [1, 4, 1])
        self.eventually_assert_one(session, "SELECT * FROM mv", [1, 4, 1])

        self.update_view(session, "UPDATE t USING TTL 20 SET a = 5 WHERE k = 1", flush)
        self.eventually_assert_one(session, "SELECT * FROM t", [1, 5, 1])
        self.eventually_assert_one(session, "SELECT * FROM mv", [1, 5, 1])

        last_update = time.time()
        self.update_view(session, "UPDATE t USING TTL 10 SET a = 6 WHERE k = 1", flush)
        self.eventually_assert_one(session, "SELECT * FROM t", [1, 6, 1])
        self.eventually_assert_one(session, "SELECT * FROM mv", [1, 6, 1])

        if flush:
            self.cluster.compact()
            now = time.time()
            if now - last_update < 10:
                time.sleep(10 - (now - last_update))
            self.eventually_assert_one(session, "SELECT * FROM t", [1, None, 1])
            self.eventually_assert_none(session, "SELECT * FROM mv")

    def test_no_base_column_in_view_pk_complex_timestamp_with_flush(self):
        self._test_no_base_column_in_view_pk_complex_timestamp(flush=True)

    def test_no_base_column_in_view_pk_complex_timestamp_without_flush(self):
        self._test_no_base_column_in_view_pk_complex_timestamp(flush=False)

    def _test_no_base_column_in_view_pk_complex_timestamp(self, flush):
        """
        Able to shadow old view row if all columns in base are removed including unselected
        Able to recreate view row if at least one selected column alive

        @jira_ticket CASSANDRA-11500
        """
        session = self.prepare(rf=3, nodes=3,
                               options={'hinted_handoff_enabled': False},
                               consistency_level=ConsistencyLevel.QUORUM)

        session.execute('USE ks')
        session.execute("CREATE TABLE t (k int, c int, a int, b int, e int, f int, primary key(k, c))")
        session.execute(("CREATE MATERIALIZED VIEW mv AS SELECT k,c,a,b FROM t "
                         "WHERE k IS NOT NULL AND c IS NOT NULL PRIMARY KEY (c, k)"))
        session.cluster.control_connection.wait_for_schema_agreement()

        # update unselected, view row should be alive
        self.update_view(session, "UPDATE t USING TIMESTAMP 1 SET e=1 WHERE k=1 AND c=1;", flush)
        self.eventually_assert_one(session, "SELECT * FROM t", [1, 1, None, None, 1, None])
        self.eventually_assert_one(session, "SELECT * FROM mv", [1, 1, None, None])

        # remove unselected, add selected column, view row should be alive
        self.update_view(session, "UPDATE t USING TIMESTAMP 2 SET e=null, b=1 WHERE k=1 AND c=1;", flush)
        self.eventually_assert_one(session, "SELECT * FROM t", [1, 1, None, 1, None, None])
        self.eventually_assert_one(session, "SELECT * FROM mv", [1, 1, None, 1])

        # remove selected column, view row is removed
        self.update_view(session, "UPDATE t USING TIMESTAMP 2 SET e=null, b=null WHERE k=1 AND c=1;", flush)
        self.eventually_assert_none(session, "SELECT * FROM t")
        self.eventually_assert_none(session, "SELECT * FROM mv")

        # update unselected with ts=3, view row should be alive
        self.update_view(session, "UPDATE t USING TIMESTAMP 3 SET f=1 WHERE k=1 AND c=1;", flush)
        self.eventually_assert_one(session, "SELECT * FROM t", [1, 1, None, None, None, 1])
        self.eventually_assert_one(session, "SELECT * FROM mv", [1, 1, None, None])

        # insert livenesssInfo, view row should be alive
        self.update_view(session, "INSERT INTO t(k,c) VALUES(1,1) USING TIMESTAMP 3", flush)
        self.eventually_assert_one(session, "SELECT * FROM t", [1, 1, None, None, None, 1])
        self.eventually_assert_one(session, "SELECT * FROM mv", [1, 1, None, None])

        # remove unselected, view row should be alive because of base livenessInfo alive
        self.update_view(session, "UPDATE t USING TIMESTAMP 3 SET f=null WHERE k=1 AND c=1;", flush)
        self.eventually_assert_one(session, "SELECT * FROM t", [1, 1, None, None, None, None])
        self.eventually_assert_one(session, "SELECT * FROM mv", [1, 1, None, None])

        # add selected column, view row should be alive
        self.update_view(session, "UPDATE t USING TIMESTAMP 3 SET a=1 WHERE k=1 AND c=1;", flush)
        self.eventually_assert_one(session, "SELECT * FROM t", [1, 1, 1, None, None, None])
        self.eventually_assert_one(session, "SELECT * FROM mv", [1, 1, 1, None])

        # update unselected, view row should be alive
        self.update_view(session, "UPDATE t USING TIMESTAMP 4 SET f=1 WHERE k=1 AND c=1;", flush)
        self.eventually_assert_one(session, "SELECT * FROM t", [1, 1, 1, None, None, 1])
        self.eventually_assert_one(session, "SELECT * FROM mv", [1, 1, 1, None])

        # delete with ts=3, view row should be alive due to unselected@ts4
        self.update_view(session, "DELETE FROM t USING TIMESTAMP 3 WHERE k=1 AND c=1;", flush)
        self.eventually_assert_one(session, "SELECT * FROM t", [1, 1, None, None, None, 1])
        self.eventually_assert_one(session, "SELECT * FROM mv", [1, 1, None, None])

        # remove unselected, view row should be removed
        self.update_view(session, "UPDATE t USING TIMESTAMP 4 SET f=null WHERE k=1 AND c=1;", flush)
        self.eventually_assert_none(session, "SELECT * FROM t")
        self.eventually_assert_none(session, "SELECT * FROM mv")

        # add selected with ts=7, view row is alive
        self.update_view(session, "UPDATE t USING TIMESTAMP 7 SET b=1 WHERE k=1 AND c=1;", flush)
        self.eventually_assert_one(session, "SELECT * FROM t", [1, 1, None, 1, None, None])
        self.eventually_assert_one(session, "SELECT * FROM mv", [1, 1, None, 1])

        # remove selected with ts=7, view row is dead
        self.update_view(session, "UPDATE t USING TIMESTAMP 7 SET b=null WHERE k=1 AND c=1;", flush)
        assert_none(session, "SELECT * FROM t")
        assert_none(session, "SELECT * FROM mv")

        # add selected with ts=5, view row is alive (selected column should not affects each other)
        self.update_view(session, "UPDATE t USING TIMESTAMP 5 SET a=1 WHERE k=1 AND c=1;", flush)
        self.eventually_assert_one(session, "SELECT * FROM t", [1, 1, 1, None, None, None])
        self.eventually_assert_one(session, "SELECT * FROM mv", [1, 1, 1, None])

        # add selected with ttl=10
        self.update_view(session, "UPDATE t USING TTL 10 SET a=1 WHERE k=1 AND c=1;", flush)
        self.eventually_assert_one(session, "SELECT * FROM t", [1, 1, 1, None, None, None])
        self.eventually_assert_one(session, "SELECT * FROM mv", [1, 1, 1, None])

        time.sleep(10)

        self.eventually_assert_none(session, "SELECT * FROM mv")

        # update unselected with ttl=10, view row should be alive
        self.update_view(session, "UPDATE t USING TTL 10 SET f=1 WHERE k=1 AND c=1;", flush)
        self.eventually_assert_one(session, "SELECT * FROM t", [1, 1, None, None, None, 1])
        self.eventually_assert_one(session, "SELECT * FROM mv", [1, 1, None, None])

        time.sleep(11)

        self.eventually_assert_none(session, "SELECT * FROM t")
        self.eventually_assert_none(session, "SELECT * FROM mv")

    @pytest.mark.parametrize("flush", [True, False], ids=["with_flush", "without_flush"])
    def test_base_column_in_view_pk_complex_timestamp(self, flush):
        """
        Able to shadow old view row with column ts greater than pk's ts and re-insert the view row
        Able to shadow old view row with column ts smaller than pk's ts and re-insert the view row

        @jira_ticket CASSANDRA-11500
        """
        session = self.prepare(rf=3, nodes=3,
                               options={'hinted_handoff_enabled': False},
                               consistency_level=ConsistencyLevel.QUORUM)
        node1, node2, node3 = self.cluster.nodelist()

        session.execute('USE ks')
        session.execute("CREATE TABLE t (k int PRIMARY KEY, a int, b int)")
        session.execute(("CREATE MATERIALIZED VIEW mv AS SELECT * FROM t "
                         "WHERE k IS NOT NULL AND a IS NOT NULL PRIMARY KEY (k, a)"))
        session.cluster.control_connection.wait_for_schema_agreement()

        # Set initial values TS=1
        self.update_view(session, "INSERT INTO t (k, a, b) VALUES (1, 1, 1) USING TIMESTAMP 1;", flush)
        self.eventually_assert_one(session, "SELECT * FROM t", [1, 1, 1])
        self.eventually_assert_one(session, "SELECT * FROM mv", [1, 1, 1])

        # increase b ts to 10
        self.update_view(session, "UPDATE t USING TIMESTAMP 10 SET b = 2 WHERE k = 1;", flush)
        self.eventually_assert_one(session, "SELECT k,a,b,writetime(b) FROM t", [1, 1, 2, 10])
        self.eventually_assert_one(session, "SELECT k,a,b,writetime(b) FROM mv", [1, 1, 2, 10])

        # switch entries. shadow a = 1, insert a = 2
        self.update_view(session, "UPDATE t USING TIMESTAMP 2 SET a = 2 WHERE k = 1;", flush)
        self.eventually_assert_one(session, "SELECT k,a,b,writetime(b) FROM t", [1, 2, 2, 10])
        self.eventually_assert_one(session, "SELECT k,a,b,writetime(b) FROM mv", [1, 2, 2, 10])

        # switch entries. shadow a = 2, insert a = 1
        self.update_view(session, "UPDATE t USING TIMESTAMP 3 SET a = 1 WHERE k = 1;", flush)
        self.eventually_assert_one(session, "SELECT k,a,b,writetime(b) FROM t", [1, 1, 2, 10])
        self.eventually_assert_one(session, "SELECT k,a,b,writetime(b) FROM mv", [1, 1, 2, 10])

        # switch entries. shadow a = 1, insert a = 2
        self.update_view(session, "UPDATE t USING TIMESTAMP 4 SET a = 2 WHERE k = 1;", flush, compact=True)
        self.eventually_assert_one(session, "SELECT k,a,b,writetime(b) FROM t", [1, 2, 2, 10])
        self.eventually_assert_one(session, "SELECT k,a,b,writetime(b) FROM mv", [1, 2, 2, 10])

        # able to shadow view row even if base-column in view pk's ts is smaller than row timestamp
        # set row TS = 20, a@6, b@20
        self.update_view(session, "DELETE FROM t USING TIMESTAMP 5 where k = 1;", flush)
        self.eventually_assert_one(session, "SELECT k,a,b,writetime(b) FROM t", [1, None, 2, 10])
        self.eventually_assert_none(session, "SELECT k,a,b,writetime(b) FROM mv")
        self.update_view(session, "INSERT INTO t (k, a, b) VALUES (1, 1, 1) USING TIMESTAMP 6;", flush)
        self.eventually_assert_one(session, "SELECT k,a,b,writetime(b) FROM t", [1, 1, 2, 10])
        self.eventually_assert_one(session, "SELECT k,a,b,writetime(b) FROM mv", [1, 1, 2, 10])
        self.update_view(session, "INSERT INTO t (k, b) VALUES (1, 1) USING TIMESTAMP 20;", flush)
        self.eventually_assert_one(session, "SELECT k,a,b,writetime(b) FROM t", [1, 1, 1, 20])
        self.eventually_assert_one(session, "SELECT k,a,b,writetime(b) FROM mv", [1, 1, 1, 20])

        # switch entries. shadow a = 1, insert a = 2
        self.update_view(session, "UPDATE t USING TIMESTAMP 7 SET a = 2 WHERE k = 1;", flush)
        self.eventually_assert_one(session, "SELECT k,a,b,writetime(a),writetime(b) FROM t", [1, 2, 1, 7, 20])
        self.eventually_assert_one(session, "SELECT k,a,b,writetime(b) FROM mv", [1, 2, 1, 20])

        # switch entries. shadow a = 2, insert a = 1
        self.update_view(session, "UPDATE t USING TIMESTAMP 8 SET a = 1 WHERE k = 1;", flush)
        self.eventually_assert_one(session, "SELECT k,a,b,writetime(a),writetime(b) FROM t", [1, 1, 1, 8, 20])
        self.eventually_assert_one(session, "SELECT k,a,b,writetime(b) FROM mv", [1, 1, 1, 20])

        # create another view row
        self.update_view(session, "INSERT INTO t (k, a, b) VALUES (2, 2, 2);", flush)
        self.eventually_assert_one(session, "SELECT k,a,b FROM t WHERE k = 2", [2, 2, 2])
        self.eventually_assert_one(session, "SELECT k,a,b FROM mv WHERE k = 2", [2, 2, 2])

        # stop node2, node3
        logger.debug('Shutdown [node2, node3]')
        self.cluster.stop_nodes([node2, node3], wait_other_notice=True)
        # shadow a = 1, create a = 2
        query = SimpleStatement("UPDATE t USING TIMESTAMP 9 SET a = 2 WHERE k = 1",
                                consistency_level=ConsistencyLevel.ONE)
        self.update_view(session, query, flush)
        # shadow (a=2, k=2) after 3 second
        query = SimpleStatement("UPDATE t USING TTL 3 SET a = 2 WHERE k = 2", consistency_level=ConsistencyLevel.ONE)
        self.update_view(session, query, flush)

        logger.debug('Starting [node2, node3]')
        self.cluster.start_nodes([node2, node3], wait_other_notice=True, wait_for_binary_proto=True)

        # For k = 1 & a = 1, We should get a digest mismatch of tombstones and repaired
        # We don't have check_trace_events
        query = SimpleStatement("SELECT * FROM mv WHERE k = 1 AND a = 1", consistency_level=ConsistencyLevel.ALL)
        # result = session.execute(query, trace=True)
        # self.check_trace_events(result.get_query_trace(), True)
        # assert 0 == len(result.current_rows)

        # For k = 1 & a = 1, second time no digest mismatch
        # self.check_trace_events(result.get_query_trace(), False)
        # assert_none(session, "SELECT * FROM mv WHERE k = 1 AND a = 1")

        def check_query_rows_size(_query, expected_rows_size):
            assert len(session.execute(_query, trace=True).current_rows) == expected_rows_size

        self.eventually(lambda: check_query_rows_size(query, 0))
        # For k = 1 & a = 2, We should get a digest mismatch of data and repaired for a = 2
        query = SimpleStatement("SELECT * FROM mv WHERE k = 1 AND a = 2", consistency_level=ConsistencyLevel.ALL)
        # result = session.execute(query, trace=True)
        # self.check_trace_events(result.get_query_trace(), True)
        # assert 1 == len(result.current_rows)

        # For k = 1 & a = 2, second time no digest mismatch
        # self.check_trace_events(result.get_query_trace(), False)
        self.eventually(lambda: check_query_rows_size(query, 1))
        self.eventually_assert_one(session, "SELECT k,a,b,writetime(b) FROM mv WHERE k = 1", [1, 2, 1, 20])

        time.sleep(3)
        # For k = 2 & a = 2, We should get a digest mismatch of expired and repaired
        query = SimpleStatement("SELECT * FROM mv WHERE k = 2 AND a = 2", consistency_level=ConsistencyLevel.ALL)
        # self.check_trace_events(result.get_query_trace(), True)
        # logger.debug(result.current_rows)
        # assert 0 == len(result.current_rows)

        # For k = 2 & a = 2, second time no digest mismatch
        # result = session.execute(query, trace=True)
        # self.check_trace_events(result.get_query_trace(), False)
        self.eventually(lambda: check_query_rows_size(query, 0))

    @pytest.mark.single_node
    def test_expired_liveness_with_limit_rf1_nodes1(self):
        self._test_expired_liveness_with_limit(rf=1, nodes=1)

    def test_expired_liveness_with_limit_rf1_nodes3(self):
        self._test_expired_liveness_with_limit(rf=1, nodes=3)

    def test_expired_liveness_with_limit_rf3(self):
        self._test_expired_liveness_with_limit(rf=3, nodes=3)

    def _test_expired_liveness_with_limit(self, rf, nodes):
        """
        Test MV with expired liveness limit is properly handled

        @jira_ticket CASSANDRA-13883
        """
        session = self.prepare(rf=rf, nodes=nodes,
                               options={'hinted_handoff_enabled': False},
                               consistency_level=ConsistencyLevel.QUORUM)

        session.execute('USE ks')
        session.execute("CREATE TABLE t (k int PRIMARY KEY, a int, b int)")
        session.execute(("CREATE MATERIALIZED VIEW mv AS SELECT * FROM t "
                         "WHERE k IS NOT NULL AND a IS NOT NULL PRIMARY KEY (k, a)"))
        session.cluster.control_connection.wait_for_schema_agreement()

        for k in range(100):
            session.execute("INSERT INTO t (k, a, b) VALUES ({}, {}, {})".format(k, k, k))

        # generate view row with expired liveness except for row 50 and 99
        for k in range(100):
            if k == 50 or k == 99:
                continue
            session.execute("DELETE a FROM t where k = {};".format(k))

        # there should be 2 live data
        assert_one(session, "SELECT k,a,b FROM mv limit 1", [50, 50, 50])
        assert_all(session, "SELECT k,a,b FROM mv limit 2", [[50, 50, 50], [99, 99, 99]])
        assert_all(session, "SELECT k,a,b FROM mv", [[50, 50, 50], [99, 99, 99]])

        # verify IN
        keys = range(100)
        assert_one(session, "SELECT k,a,b FROM mv WHERE k in ({}) limit 1".format(', '.join(str(x) for x in keys)),
                   [50, 50, 50])
        assert_all(session, "SELECT k,a,b FROM mv WHERE k in ({}) limit 2".format(', '.join(str(x) for x in keys)),
                   [[50, 50, 50], [99, 99, 99]])
        assert_all(session, "SELECT k,a,b FROM mv WHERE k in ({})".format(', '.join(str(x) for x in keys)),
                   [[50, 50, 50], [99, 99, 99]])

        # verify fetch size
        session.default_fetch_size = 1
        assert_one(session, "SELECT k,a,b FROM mv limit 1", [50, 50, 50])
        assert_all(session, "SELECT k,a,b FROM mv limit 2", [[50, 50, 50], [99, 99, 99]])
        assert_all(session, "SELECT k,a,b FROM mv", [[50, 50, 50], [99, 99, 99]])

    def test_base_column_in_view_pk_commutative_tombstone_with_flush(self):
        self._test_base_column_in_view_pk_commutative_tombstone_(flush=True)

    def test_base_column_in_view_pk_commutative_tombstone_without_flush(self):
        self._test_base_column_in_view_pk_commutative_tombstone_(flush=False)

    def _test_base_column_in_view_pk_commutative_tombstone_(self, flush):
        """
        view row deletion should be commutative with newer view livenessInfo, otherwise deleted columns may be resurrected.
        @jira_ticket CASSANDRA-13409
        """
        session = self.prepare(rf=3, nodes=3,
                               options={'hinted_handoff_enabled': False},
                               consistency_level=ConsistencyLevel.QUORUM)

        session.execute('USE ks')
        session.execute("CREATE TABLE t (id int PRIMARY KEY, v int, v2 text, v3 decimal)")
        session.execute(("CREATE MATERIALIZED VIEW t_by_v AS SELECT * FROM t "
                         "WHERE v IS NOT NULL AND id IS NOT NULL PRIMARY KEY (v,id)"))
        session.cluster.control_connection.wait_for_schema_agreement()

        # sstable 1, Set initial values TS=1
        self.update_view(session, "INSERT INTO t (id, v, v2, v3) VALUES (1, 1, 'a', 3.0) USING TIMESTAMP 1", flush)
        self.eventually_assert_one(session, "SELECT * FROM t_by_v", [1, 1, 'a', 3.0])

        # sstable 2, change v's value and TS=2, tombstones v=1 and adds v=0 record
        self.update_view(session, "DELETE FROM t USING TIMESTAMP 2 WHERE id = 1;", flush)
        self.eventually_assert_none(session, "SELECT * FROM t_by_v")
        self.eventually_assert_none(session, "SELECT * FROM t")

        # sstable 3, tombstones of mv created by base deletion should remain.
        self.update_view(session, "INSERT INTO t (id, v) VALUES (1, 1) USING TIMESTAMP 3", flush)
        self.eventually_assert_one(session, "SELECT * FROM t_by_v", [1, 1, None, None])
        self.eventually_assert_one(session, "SELECT * FROM t", [1, 1, None, None])

        # sstable 4, shadow view row (id=1, v=1), insert (id=1, v=2, ts=4)
        self.update_view(session, "UPDATE t USING TIMESTAMP 4 set v = 2 WHERE id = 1;", flush)
        self.eventually_assert_one(session, "SELECT * FROM t_by_v", [2, 1, None, None])
        self.eventually_assert_one(session, "SELECT * FROM t", [1, 2, None, None])

        # sstable 5, shadow view row (id=1, v=2), insert (id=1, v=1 ts=5)
        self.update_view(session, "UPDATE t USING TIMESTAMP 5 set v = 1 WHERE id = 1;", flush)
        self.eventually_assert_one(session, "SELECT * FROM t_by_v", [1, 1, None, None])
        # data deleted by row-tombstone@2 should not resurrect
        self.eventually_assert_one(session, "SELECT * FROM t", [1, 1, None, None])

        if flush:
            self.cluster.compact()
            self.eventually_assert_one(session, "SELECT * FROM t_by_v", [1, 1, None, None])
            # data deleted by row-tombstone@2 should not resurrect
            self.eventually_assert_one(session, "SELECT * FROM t", [1, 1, None, None])

        # shadow view row (id=1, v=1)
        self.update_view(session, "UPDATE t USING TIMESTAMP 5 set v = null WHERE id = 1;", flush)
        self.eventually_assert_none(session, "SELECT * FROM t_by_v")
        self.eventually_assert_one(session, "SELECT * FROM t", [1, None, None, None])

    def test_view_tombstone(self):
        """
        Test that a materialized views properly tombstone
        @jira_ticket CASSANDRA-10261
        @jira_ticket CASSANDRA-10910
        """

        self.prepare(rf=3, options={'hinted_handoff_enabled': False, 'cache_hit_rate_read_balancing': False})
        node1, node2, node3 = self.cluster.nodelist()

        session = self.patient_exclusive_cql_connection(node1)
        session.execute('USE ks')

        session.execute("CREATE TABLE t (id int PRIMARY KEY, v int, v2 text, v3 decimal)")
        session.execute("CREATE MATERIALIZED VIEW t_by_v AS SELECT * FROM t "
                        "WHERE v IS NOT NULL AND id IS NOT NULL PRIMARY KEY (v,id) "
                        "WITH read_repair_chance = 0.0 AND dclocal_read_repair_chance = 0.0 AND speculative_retry = 'none'")

        session.cluster.control_connection.wait_for_schema_agreement()

        # Set initial values TS=0, verify
        session.execute(SimpleStatement("INSERT INTO t (id, v, v2, v3) VALUES (1, 1, 'a', 3.0) USING TIMESTAMP 0",
                                        consistency_level=ConsistencyLevel.ALL))

        assert_one(
            session,
            "SELECT * FROM t_by_v WHERE v = 1",
            [1, 1, 'a', 3.0]
        )
        session.execute(SimpleStatement("INSERT INTO t (id, v2) VALUES (1, 'b') USING TIMESTAMP 1",
                                        consistency_level=ConsistencyLevel.ALL))

        assert_one(
            session,
            "SELECT * FROM t_by_v WHERE v = 1",
            [1, 1, 'b', 3.0]
        )

        # change v's value and TS=3, tombstones v=1 and adds v=0 record
        session.execute(SimpleStatement("UPDATE t USING TIMESTAMP 3 SET v = 0 WHERE id = 1",
                                        consistency_level=ConsistencyLevel.ALL))
        assert_none(session, "SELECT * FROM t_by_v WHERE v = 1")

        logger.debug('Shutdown nodes 2 and 3')
        self.cluster.stop_nodes([node2, node3], wait_other_notice=True)

        session.execute(SimpleStatement("UPDATE t USING TIMESTAMP 4 SET v = 1 WHERE id = 1",
                                        consistency_level=ConsistencyLevel.ONE))
        assert_one(
            session,
            "SELECT * FROM t_by_v WHERE v = 1",
            [1, 1, 'b', 3.0]
        )

        logger.debug('Starting nodes 2 and 3')
        self.cluster.start_nodes([node2, node3], wait_other_notice=True, wait_for_binary_proto=True)

        session2 = self.patient_exclusive_cql_connection(node2)
        session2.execute('USE ks')

        # We retry sending failed view updates, but just to view replica
        # paired with node1. This means one of node 2 and 3 will always
        # have the old, stale data
        try:
            assert_none(session2, "SELECT * FROM t_by_v WHERE v = 1")
        except AssertionError:
            session3 = self.patient_exclusive_cql_connection(node2)
            session3.execute('USE ks')
            assert_none(session3, "SELECT * FROM t_by_v WHERE v = 1")

        # We should get a digest mismatch, and data should be repaired
        assert_one(
            session,
            "SELECT * FROM t_by_v WHERE v = 1",
            [1, 1, 'b', 3.0],
            cl=ConsistencyLevel.ALL
        )

        assert_one(
            session2,
            "SELECT * FROM t_by_v WHERE v = 1",
            [1, 1, 'b', 3.0],
            cl=ConsistencyLevel.ONE
        )

    def _setup_for_viewbuildstatus(self, num_of_rows=None):
        """ this function creates a materialized view for viewbildstatus nodetool command tests
            Returns a list of [TableManager, MaterializedViewManager] objects
        """
        self.setup()
        if not num_of_rows:
            num_of_rows = 10000 if self.debug_mode else 100000
        session = self.prepare(rf=3, nodes=3, fetch_size=num_of_rows * 2)
        table_manager = TableManager(session, self.cluster,
                                     columns={
                                         'int': {'amount': 20, 'frozen': False, 'value length': {'min': 1, 'max': 100}}
                                     }, cl_columns={}, pk_columns={})
        table_manager.create_table()
        table_manager.prefill_table(num_of_rows)

        mv_pk_column = table_manager.column_names_list[1]
        mv = MaterializedViewManager(table_manager)
        mv.create_materialized_view(mv_pk_column={'names': [mv_pk_column]}, wait_for_view_built=False)
        return [table_manager, mv]

    def test_viewbuildstatus_progress_success_flow(self):
        """" test viewbuildstatus nodetool command output correctness during the creation of a materialized view"""

        table_manager, mv = self._setup_for_viewbuildstatus()
        number_of_nodes = len(self.cluster.nodelist())

        in_progress_str = output = "has not finished building; node status is below."
        success_str = "has finished building"
        max_retries = 60 if self.debug_mode else 20
        current_retry = 0

        """
        viewbuildstatus command output for example:

           keyspace1.m_view has not finished building; node status is below.

           Host      Info
           127.0.0.2 STARTED
           127.0.0.3 STARTED
           127.0.0.1 SUCCESS

        """

        while current_retry < max_retries and in_progress_str in output:
            current_retry += 1
            node_to_run = random.choice(self.cluster.nodelist())
            logger.debug(f'Waiting for viewbuildstatus command to finish. Current retry = {current_retry},'
                         f'command is running from {node_to_run.name}\n output = {output}')
            try:
                output = node_to_run.nodetool(f'viewbuildstatus {table_manager.keyspace} {mv.mv_name}')
                assert (success_str in output[0]), "viewbuildstatus command finished with unexpected output: {}, " \
                                                   "Terminating test".format(output)
                logger.debug('viewbuildstatus command finished successfully')
            except NodetoolError as e:
                # viewbuildstatus command returns exit(1) during the materialized view build process duration
                # TODO remove the try-except when https://github.com/scylladb/scylla-tools-java/issues/289 is resolved
                output = e.stdout
                logger.debug(f'e.stdout = {e.stdout}')
                cluster_info = output.splitlines()[3:]
                assert(len(cluster_info) == number_of_nodes), f'Wrong output of viewbuildstatus command:' \
                    f'number of lines is wrong'
                for line in cluster_info:
                    host_ip, host_status = line.split()
                    assert (host_status == "SUCCESS" or host_status == "STARTED"), \
                        f'Wrong output of viewbuildstatus command: host {host_ip} state is not "STARTED" or ' \
                        f'"SUCCESS" '
            time.sleep(1)

        assert (success_str in output[0]), \
            f'viewbuildstatus command exceeded {max_retries} retries without receiving {success_str} string in output'

    def test_viewbuildstatus_progress_unknown_flow(self):
        """" test viewbuildstatus nodetool command output correctness,
             testing UNKNOWN host state by giving a wrong materialized view parameter to viewbuildstatus command
        """
        """
           viewbildstatus command output for example:

               ks.tm_table has not finished building; node status is below.

               Host       Info
               127.0.41.3 UNKNOWN
               127.0.41.1 UNKNOWN
               127.0.41.2 UNKNOWN
        """
        table_manager, mv = self._setup_for_viewbuildstatus(num_of_rows=10)
        number_of_nodes = len(self.cluster.nodelist())
        try:
            node_to_run = random.choice(self.cluster.nodelist())
            logger.debug(f'Testing viewbuilstatus nodetool command with wrong materialized view name.\n '
                         f'command is running from {node_to_run.name}')
            node_to_run.nodetool(f'viewbuildstatus {table_manager.keyspace} {table_manager.table_name}')
        except NodetoolError as e:
            logger.debug(f'e.stdout = {e.stdout}')
            assert (e.stdout.count("UNKNOWN") == number_of_nodes), \
                "wrong number of UNKNOWN host states in viewbuildstatus command output"

    def test_repair_mv(self):
        """ Test repair of materialized view """
        session = self.prepare(rf=3, nodes=3, options={'hinted_handoff_enabled': False}, fetch_size=100)
        node1, node2, node3 = self.cluster.nodelist()
        tm = TableManager(session, self.cluster,
                          columns={'int': {'amount': 2, 'frozen': False, 'value length': {'min': 1, 'max': 100}}
                                   }, cl_columns={}, pk_columns={})
        tm.create_table()

        mv_pk_column = tm.column_names_list[-2]
        mv = MaterializedViewManager(tm)
        mv.create_materialized_view(mv_pk_column={'names': [mv_pk_column]})
        prefill = 100
        tm.prefill_table(prefill, data={'int': [2]})
        self.cluster.flush()

        table_statement = 'select * from {}'.format(tm.table_name)
        mv_statement = 'select * from {}'.format(mv.mv_name)
        assert_two_queries_equal(session, table_statement, session, mv_statement,
                                 session_timeout=self.session_timeout)

        node2.stop(wait_other_notice=True)

        for i in range(prefill // 2):
            tm.update_table(set_clause={'by name': {mv_pk_column: 3}},
                            where_filter={'by name': {'id': {'value': i, 'operator': '='}}},
                            consistency_level=ConsistencyLevel.ONE)

        assert_two_queries_equal(session, table_statement,
                                 session, mv_statement, session_timeout=self.session_timeout)

        node2.start(wait_other_notice=True, wait_for_binary_proto=True)

        logger.debug('Repair the mv replica')
        node1.nodetool("repair {ks} {mv_name}".format(ks=tm.keyspace, mv_name=mv.mv_name))

        logger.debug('Stop [node1, node3]')
        self.cluster.stop_nodes([node1, node3], wait_other_notice=True)

        # Validate data
        logger.debug('Verify the MV data for updated rows in the MV with CL=ONE')
        assert_one(
            session, 'select count(*) from {1} where {0}=2 ALLOW FILTERING'.format(mv_pk_column, mv.mv_name), [50])

        logger.debug('Verify the MV data for not updated rows in the MV with CL=ONE')
        assert_one(
            session, 'select count(*) from {1} where {0}=3 ALLOW FILTERING'.format(mv_pk_column, mv.mv_name), [50])

        logger.debug('Verify the base table data with CL=ONE - all rows shouldn\'t be updated')
        for i in range(prefill):
            assert_one(session, 'select {0} from {1} where id={2}'.format(mv_pk_column, tm.table_name, i), [2])

    def test_simple_repair(self):
        """
        Test that a materialized view are consistent after a simple repair.
        """

        session = self.prepare(rf=3, options={'hinted_handoff_enabled': False})
        node1, node2, node3 = self.cluster.nodelist()

        session.execute("CREATE TABLE t (id int PRIMARY KEY, v int, v2 text, v3 decimal) WITH read_repair_chance = 0.0")
        session.execute(("CREATE MATERIALIZED VIEW t_by_v AS SELECT * FROM t "
                         "WHERE v IS NOT NULL AND id IS NOT NULL PRIMARY KEY (v, id) WITH read_repair_chance = 0.0"))

        session.cluster.control_connection.wait_for_schema_agreement()

        logger.debug('Shutdown node2')
        node2.stop(wait_other_notice=True)

        for i in range(1000):
            session.execute("INSERT INTO t (id, v, v2, v3) VALUES ({v}, {v}, 'a', 3.0)".format(v=i))

        # Scylla doesn't leverage the batchlog for MVs
        # self._replay_batchlogs()

        logger.debug('Verify the data in the MV with CL=ONE')
        for i in range(1000):
            assert_one(
                session,
                "SELECT * FROM t_by_v WHERE v = {}".format(i),
                [i, i, 'a', 3.0]
            )

        logger.debug('Verify the data in the MV with CL=ALL. All should be unavailable.')
        for i in range(1000):
            statement = SimpleStatement(
                "SELECT * FROM t_by_v WHERE v = {}".format(i),
                consistency_level=ConsistencyLevel.ALL
            )

            assert_unavailable(
                session.execute,
                statement
            )

        logger.debug('Start node2, and repair')
        node2.start(wait_other_notice=True, wait_for_binary_proto=True)
        node1.repair()

        logger.debug('Verify the data in the MV with CL=ONE. All should be available now.')
        for i in range(1000):
            assert_one(
                session,
                "SELECT * FROM t_by_v WHERE v = {}".format(i),
                [i, i, 'a', 3.0],
                cl=ConsistencyLevel.ONE
            )

    @pytest.mark.next_gating
    def test_base_replica_repair(self):
        self._base_replica_repair_test()

    # This test is taken from Cassandra. Not relevant for us
    # def test_base_replica_repair_with_contention(self):
    #     """
    #     Test repair does not fail when there is MV lock contention
    #     @jira_ticket CASSANDRA-12905
    #     """

    #     self._base_replica_repair_test(fail_mv_lock=True)

    def _base_replica_repair_test(self, fail_mv_lock=False):
        """
        Test that a materialized view are consistent after the repair of the base replica.
        """

        session = self.prepare(rf=3)
        node1, node2, node3 = self.cluster.nodelist()
        session.execute("CREATE TABLE t (id int PRIMARY KEY, v int, v2 text, v3 decimal)")
        session.execute(("CREATE MATERIALIZED VIEW t_by_v AS SELECT * FROM t "
                         "WHERE v IS NOT NULL AND id IS NOT NULL PRIMARY KEY (v, id)"))
        wait_for_view(cluster=self.cluster, session=session, ks="ks", view="t_by_v")
        session.cluster.control_connection.wait_for_schema_agreement()

        logger.debug('Write initial data')
        for i in range(1000):
            session.execute("INSERT INTO t (id, v, v2, v3) VALUES ({v}, {v}, 'a', 3.0)".format(v=i))

        # Scylla doesn't leverage the batchlog for MVs
        # self._replay_batchlogs()

        logger.debug('Verify the data in the MV with CL=ALL')
        for i in range(1000):
            assert_one(
                session,
                "SELECT * FROM t_by_v WHERE v = {}".format(i),
                [i, i, 'a', 3.0],
                cl=ConsistencyLevel.ALL
            )

        logger.debug('Shutdown node1')
        node1.stop(wait_other_notice=True)
        logger.debug('Delete node1 data')
        node1.clear(clear_all=True)

        # This code is taken from Cassandra. Not relevant for us
        # jvm_args = []
        # if fail_mv_lock:
        #     if self.cluster.version() >= LooseVersion('3.10'):  # CASSANDRA-10134
        #         jvm_args = ['-Dcassandra.allow_unsafe_replace=true', '-Dcassandra.replace_address={}'.format(node1.address())]
        #     jvm_args.append("-Dcassandra.test.fail_mv_locks_count=1000")
        #     # this should not make Keyspace.apply throw WTE on failure to acquire lock
        #     node1.set_configuration_options(values={'write_request_timeout_in_ms': 100})
        # logger.debug('Restarting node1 with jvm_args={}'.format(jvm_args))
        # node1.start(wait_other_notice=True, wait_for_binary_proto=True, jvm_args=jvm_args)

        node1.start(wait_other_notice=True, wait_for_binary_proto=True)
        logger.debug('Shutdown node2 and node3')
        self.cluster.stop_nodes([node2, node3], wait_other_notice=True)

        session = self.patient_exclusive_cql_connection(node1)
        session.execute('USE ks')

        logger.debug('Verify that there is no data on node1')
        for i in range(1000):
            assert_none(
                session,
                "SELECT * FROM t_by_v WHERE v = {}".format(i)
            )

        logger.debug('Restarting node2 and node3')
        self.cluster.start_nodes([node2, node3], wait_other_notice=True, wait_for_binary_proto=True)

        # Just repair the base replica
        logger.debug('Starting repair on node1')
        node1.nodetool("repair ks t")

        logger.debug('Verify base table data with cl=ONE')
        for i in range(1000):
            assert_one(
                session,
                "SELECT * FROM t WHERE id = {}".format(i),
                [i, i, 'a', 3.0]
            )

        logger.debug('Verify materialize view data with cl=ONE')
        for i in range(1000):
            assert_one(
                session,
                "SELECT * FROM t_by_v WHERE v = {}".format(i),
                [i, i, 'a', 3.0]
            )

    def _stop_nodes(self, nodes):
        logger.debug('Stopping {}'.format([node.name for node in nodes]))
        self.cluster.stop_nodes(nodes, wait_other_notice=True)

    def _start_nodes(self, nodes):
        logger.debug('Starting {}'.format([node.name for node in nodes]))
        self.cluster.start_nodes(nodes, wait_other_notice=True, wait_for_binary_proto=True)

    def test_complex_repair(self):
        """
        Test that a materialized view are consistent after a more complex repair.
        """
        def _verify_data_by_one(session, rows, cl, multiply, debug_message, none_data=False):
            logger.debug(debug_message)
            statement_template = "SELECT * FROM ks.t_by_v WHERE v = {}"
            for i in range(rows):
                v = i * 2 if multiply else i
                statement = statement_template.format(v)
                if not none_data:
                    expected_row = [v, v, 'a', 6.0] if multiply else [v, v, 'a', 3.0]
                    assert_one(session, statement, expected_row, cl=cl
                               )
                else:
                    assert_none(session2, statement, cl=cl)

        session = self.prepare(rf=5, options={'hinted_handoff_enabled': False, 'read_repair_chance': 0.0}, nodes=5)
        node1, node2, node3, node4, node5 = self.cluster.nodelist()

        # we create the base table with gc_grace_seconds=5 so batchlog will expire after 5 seconds
        session.execute("CREATE TABLE ks.t (id int PRIMARY KEY, v int, v2 text, v3 decimal)"
                        "WITH gc_grace_seconds = 5")
        session.execute(("CREATE MATERIALIZED VIEW ks.t_by_v AS SELECT * FROM t "
                         "WHERE v IS NOT NULL AND id IS NOT NULL PRIMARY KEY (v, id)"))
        wait_for_view(cluster=self.cluster, session=session, ks="ks", view="t_by_v")
        session.cluster.control_connection.wait_for_schema_agreement()

        self._stop_nodes([node2, node3])
        rows = 1000

        logger.debug('Write initial data to node1 (will be replicated to node4 and node5)')
        for i in range(rows):
            session.execute("INSERT INTO ks.t (id, v, v2, v3) VALUES ({v}, {v}, 'a', 3.0)".format(v=i))

        _verify_data_by_one(session, rows, ConsistencyLevel.ONE, False,
                            'Verify the data in the MV on node1 with CL=ONE')

        self._stop_nodes([node1, node4, node5])

        self._start_nodes([node2, node3])

        session2 = self.patient_cql_connection(node2)

        _verify_data_by_one(session2, rows, ConsistencyLevel.ONE, False,
                            'Verify the data in the MV on node2 with CL=ONE. No rows should be found.', none_data=True)

        logger.debug('Write new data in node2 and node3 that overlap those in node1, node4 and node5')
        for i in range(rows):
            # we write i*2 as value, instead of i
            session2.execute("INSERT INTO ks.t (id, v, v2, v3) VALUES ({v}, {v}, 'a', 6.0)".format(v=i * 2))

        _verify_data_by_one(session2, rows, ConsistencyLevel.ONE, True,
                            'Verify the new data in the MV on node2 with CL=ONE')

        # Scylla doesn't leverage the batchlog for MVs
        # logger.debug('Wait for batchlogs to expire from node2 and node3')
        # time.sleep(5)

        self._start_nodes([node1, node4, node5])
        self._stop_nodes([node2, node3])
        session = self.patient_cql_connection(node1)

        _verify_data_by_one(session, rows, ConsistencyLevel.QUORUM, False,
                            'Verify the new data in the MV on node2 with CL=ONE')

        self._start_nodes([node2, node3])

        logger.debug('Run global repair on node1')
        node1.repair()

        self._stop_nodes([node2, node3])

        table_statement = 'SELECT * FROM ks.t'
        mv_statement = 'SELECT * FROM ks.t_by_v'
        logger.debug('Read data from MV at quorum (new data should be returned after repair)')
        assert_two_queries_equal(session, table_statement, session, mv_statement,
                                 consistency_level=ConsistencyLevel.QUORUM, session_timeout=self.session_timeout)

        self._start_nodes([node2, node3])
        self._stop_nodes([node1, node4, node5])

        logger.debug('Read data from MV at quorum (new data should be returned after repair)')
        assert_two_queries_equal(session2, table_statement, session2, mv_statement,
                                 consistency_level=ConsistencyLevel.ONE, session_timeout=self.session_timeout)

    def test_really_complex_repair(self):
        """
        Test that a materialized view are consistent after a more complex repair.
        """

        session = self.prepare(rf=5, options={'hinted_handoff_enabled': False}, nodes=5)
        node1, node2, node3, node4, node5 = self.cluster.nodelist()

        # we create the base table with gc_grace_seconds=5 so batchlog will expire after 5 seconds
        session.execute("CREATE TABLE ks.t (id int, v int, v2 text, v3 decimal, PRIMARY KEY(id, v, v2))"
                        "WITH gc_grace_seconds = 1")
        session.execute(("CREATE MATERIALIZED VIEW ks.t_by_v AS SELECT * FROM t "
                         "WHERE v IS NOT NULL AND id IS NOT NULL AND v IS NOT NULL AND "
                         "v2 IS NOT NULL PRIMARY KEY (v2, v, id)"))
        wait_for_view(cluster=self.cluster, session=session, ks="ks", view="t_by_v")
        session.cluster.control_connection.wait_for_schema_agreement()

        self._stop_nodes([node2, node3])

        session.execute("INSERT INTO ks.t (id, v, v2, v3) VALUES (1, 1, 'a', 3.0)")
        session.execute("INSERT INTO ks.t (id, v, v2, v3) VALUES (2, 2, 'a', 3.0)")
        # Scylla doesn't leverage the batchlog for MVs
        # self._replay_batchlogs()
        logger.debug('Verify the data in the MV on node1 with CL=ONE')
        assert_all(session, "SELECT * FROM ks.t_by_v WHERE v2 = 'a'", [['a', 1, 1, 3.0], ['a', 2, 2, 3.0]])

        session.execute("INSERT INTO ks.t (id, v, v2, v3) VALUES (1, 1, 'b', 3.0)")
        session.execute("INSERT INTO ks.t (id, v, v2, v3) VALUES (2, 2, 'b', 3.0)")
        # Scylla doesn't leverage the batchlog for MVs
        # self._replay_batchlogs()
        logger.debug('Verify the data in the MV on node1 with CL=ONE')
        assert_all(session, "SELECT * FROM ks.t_by_v WHERE v2 = 'b'", [['b', 1, 1, 3.0], ['b', 2, 2, 3.0]])

        session.shutdown()

        self._stop_nodes([node1, node4, node5])
        self._start_nodes([node2, node3])

        session2 = self.patient_cql_connection(node2)
        session2.execute('USE ks')

        logger.debug('Verify the data in the MV on node2 with CL=ONE. No rows should be found.')
        assert_none(session2, "SELECT * FROM ks.t_by_v WHERE v2 = 'a'")

        logger.debug('Write new data in node2 that overlap those in node1')
        session2.execute("INSERT INTO ks.t (id, v, v2, v3) VALUES (1, 1, 'c', 3.0)")
        session2.execute("INSERT INTO ks.t (id, v, v2, v3) VALUES (2, 2, 'c', 3.0)")
        # Scylla doesn't leverage the batchlog for MVs
        # self._replay_batchlogs()
        assert_all(session2, "SELECT * FROM ks.t_by_v WHERE v2 = 'c'", [['c', 1, 1, 3.0], ['c', 2, 2, 3.0]])

        session2.execute("INSERT INTO ks.t (id, v, v2, v3) VALUES (1, 1, 'd', 3.0)")
        session2.execute("INSERT INTO ks.t (id, v, v2, v3) VALUES (2, 2, 'd', 3.0)")
        # Scylla doesn't leverage the batchlog for MVs
        # self._replay_batchlogs()
        assert_all(session2, "SELECT * FROM ks.t_by_v WHERE v2 = 'd'", [['d', 1, 1, 3.0], ['d', 2, 2, 3.0]])

        logger.debug("Composite delete of everything")
        session2.execute("DELETE FROM ks.t WHERE id = 1 and v = 1")
        session2.execute("DELETE FROM ks.t WHERE id = 2 and v = 2")
        # Scylla doesn't leverage the batchlog for MVs
        # self._replay_batchlogs()
        assert_none(session2, "SELECT * FROM ks.t_by_v WHERE v2 = 'c'")
        assert_none(session2, "SELECT * FROM ks.t_by_v WHERE v2 = 'd'")

        # Scylla doesn't leverage the batchlog for MVs
        # logger.debug('Wait for batchlogs to expire from node2 and node3')
        # time.sleep(5)

        logger.debug('Start remaining nodes')
        self._start_nodes([node1, node4, node5])

        # at this point the data may not be repaired yet so we may have an inconsistency.
        # this value should return either the expected data or None
        assert_all_or_none(
            session2,
            "SELECT * FROM ks.t_by_v WHERE v2 = 'a'", [['a', 1, 1, 3.0], ['a', 2, 2, 3.0]],
            cl=ConsistencyLevel.QUORUM
        )

        logger.debug('Run global repair on node1')
        node1.repair()

        assert_none(session2, "SELECT * FROM ks.t_by_v WHERE v2 = 'a'", cl=ConsistencyLevel.QUORUM)

    def test_complex_mv_select_statements(self):
        """
        Test complex MV select statements
        @jira_ticket CASSANDRA-9664
        """

        self.prepare(rf=3)
        node1, _, _ = self.cluster.nodelist()
        session = self.patient_cql_connection(node1)

        logger.debug("Creating keyspace")
        session.execute("CREATE KEYSPACE mvtest WITH replication = "
                        "{'class': 'SimpleStrategy', 'replication_factor': '3'}")
        session.execute('USE mvtest')

        mv_primary_keys = ["((a, b), c)",
                           "((b, a), c)",
                           "(a, b, c)",
                           "(c, b, a)",
                           "((c, a), b)"]

        for mv_primary_key in mv_primary_keys:

            session.execute("CREATE TABLE test (a int, b int, c int, d int, PRIMARY KEY (a, b, c))")

            insert_stmt = session.prepare("INSERT INTO test (a, b, c, d) VALUES (?, ?, ?, ?)")
            update_stmt = session.prepare("UPDATE test SET d = ? WHERE a = ? AND b = ? AND c = ?")
            delete_stmt1 = session.prepare("DELETE FROM test WHERE a = ? AND b = ? AND c = ?")
            delete_stmt2 = session.prepare("DELETE FROM test WHERE a = ?")

            session.cluster.control_connection.wait_for_schema_agreement()

            rows = [(0, 0, 0, 0),
                    (0, 0, 1, 0),
                    (0, 1, 0, 0),
                    (0, 1, 1, 0),
                    (1, 0, 0, 0),
                    (1, 0, 1, 0),
                    (1, 1, -1, 0),
                    (1, 1, 0, 0),
                    (1, 1, 1, 0)]

            for row in rows:
                session.execute(insert_stmt, row)

            logger.debug("Testing MV primary key: {}".format(mv_primary_key))

            session.execute("CREATE MATERIALIZED VIEW mv AS SELECT * FROM test WHERE "
                            "a = 1 AND b IS NOT NULL AND c = 1 PRIMARY KEY {}".format(mv_primary_key))

            wait_for_view(cluster=self.cluster, session=session, ks="mvtest", view="mv")

            assert_all(
                session, "SELECT a, b, c, d FROM mv",
                [[1, 0, 1, 0], [1, 1, 1, 0]],
                ignore_order=True,
                cl=ConsistencyLevel.QUORUM
            )

            # insert new rows that does not match the filter
            session.execute(insert_stmt, (0, 0, 1, 0))
            session.execute(insert_stmt, (1, 1, 0, 0))
            assert_all(
                session, "SELECT a, b, c, d FROM mv",
                [[1, 0, 1, 0], [1, 1, 1, 0]],
                ignore_order=True,
                cl=ConsistencyLevel.QUORUM, num_attempts=20
            )

            # insert new row that does match the filter
            session.execute(insert_stmt, (1, 2, 1, 0))
            assert_all(
                session, "SELECT a, b, c, d FROM mv",
                [[1, 0, 1, 0], [1, 1, 1, 0], [1, 2, 1, 0]],
                ignore_order=True,
                cl=ConsistencyLevel.QUORUM, num_attempts=20
            )

            # update rows that does not match the filter
            session.execute(update_stmt, (1, 1, -1, 0))
            session.execute(update_stmt, (0, 1, 1, 0))
            assert_all(
                session, "SELECT a, b, c, d FROM mv",
                [[1, 0, 1, 0], [1, 1, 1, 0], [1, 2, 1, 0]],
                ignore_order=True,
                cl=ConsistencyLevel.QUORUM, num_attempts=20
            )

            # update a row that does match the filter
            session.execute(update_stmt, (2, 1, 1, 1))
            assert_all(
                session, "SELECT a, b, c, d FROM mv",
                [[1, 0, 1, 0], [1, 1, 1, 2], [1, 2, 1, 0]],
                ignore_order=True,
                cl=ConsistencyLevel.QUORUM, num_attempts=20
            )

            # delete rows that does not match the filter
            session.execute(delete_stmt1, (1, 1, -1))
            session.execute(delete_stmt1, (2, 0, 1))
            session.execute(delete_stmt2, (0,))
            assert_all(
                session, "SELECT a, b, c, d FROM mv",
                [[1, 0, 1, 0], [1, 1, 1, 2], [1, 2, 1, 0]],
                ignore_order=True,
                cl=ConsistencyLevel.QUORUM, num_attempts=20
            )

            # delete a row that does match the filter
            session.execute(delete_stmt1, (1, 1, 1))
            assert_all(
                session, "SELECT a, b, c, d FROM mv",
                [[1, 0, 1, 0], [1, 2, 1, 0]],
                ignore_order=True,
                cl=ConsistencyLevel.QUORUM, num_attempts=20
            )

            # delete a partition that matches the filter
            session.execute(delete_stmt2, (1,))
            assert_all(session, "SELECT a, b, c, d FROM mv", [], cl=ConsistencyLevel.QUORUM, num_attempts=20)

            # Cleanup
            session.execute("DROP MATERIALIZED VIEW mv")
            session.execute("DROP TABLE test")

    def test_propagate_view_creation_over_non_existing_table(self):
        """
        The internal addition of a view over a non existing table should be ignored
        @jira_ticket CASSANDRA-13737
        """

        self.prepare(rf=3, options={'shadow_round_ms': 1000})
        node1, node2, node3 = self.cluster.nodelist()
        session = self.patient_cql_connection(node1, consistency_level=ConsistencyLevel.QUORUM)
        session.execute('USE ks')
        session.execute('CREATE TABLE users (username varchar PRIMARY KEY, state varchar)')

        # create a materialized view only in nodes 1 and 2
        logger.debug("Stopping node3")
        node3.stop(wait_other_notice=True)
        logger.debug("Creating view")
        session.cluster.max_schema_agreement_wait = 0
        session.execute(('CREATE MATERIALIZED VIEW users_by_state AS '
                         'SELECT * FROM users WHERE state IS NOT NULL AND username IS NOT NULL '
                         'PRIMARY KEY (state, username)'))

        logger.debug("Stopping other nodes")
        self.cluster.stop_nodes([node1, node2], wait_other_notice=True)
        logger.debug("Restarting node3")
        node3.start(wait_for_binary_proto=True)
        session = self.patient_cql_connection(node3, consistency_level=ConsistencyLevel.QUORUM)
        logger.debug("Dropping table")
        session.execute('DROP TABLE ks.users')

        logger.debug("Restarting cluster")
        self.stop_cluster()
        self.cluster.start()

    @pytest.mark.skip("Not relevant for Scylla - requires byteman error injection")
    def test_base_view_consistency_on_failure_after_mv_apply(self):
        self._test_base_view_consistency_on_crash("after")

    @pytest.mark.skip("Not relevant for Scylla - requires byteman error injection")
    def test_base_view_consistency_on_failure_before_mv_apply(self):
        self._test_base_view_consistency_on_crash("before")

    def _test_base_view_consistency_on_crash(self, fail_phase):
        """
         * Fails base table write before or after applying views
         * Restart node and replay commit and batchlog
         * Check that base and views are present

         @jira_ticket CASSANDRA-13069
        """

        self.cluster.set_batch_commitlog(enabled=True)
        self.fixture_dtest_setup.ignore_log_patterns += [r'Dummy failure', r"Failed to force-recycle all segments"]
        self.prepare(rf=1, install_byteman=True)
        node1, node2, node3 = self.cluster.nodelist()
        session = self.patient_exclusive_cql_connection(node1)
        session.execute('USE ks')

        session.execute("CREATE TABLE t (id int PRIMARY KEY, v int, v2 text, v3 decimal)")
        session.execute(("CREATE MATERIALIZED VIEW t_by_v AS SELECT * FROM t "
                         "WHERE v IS NOT NULL AND id IS NOT NULL PRIMARY KEY (v, id)"))

        session.cluster.control_connection.wait_for_schema_agreement()

        logger.debug('Make node1 fail {} view writes'.format(fail_phase))
        node1.byteman_submit(['./byteman/fail_{}_view_write.btm'.format(fail_phase)])

        logger.debug('Write 1000 rows - all node1 writes should fail')

        failed = False
        for i in range(1, 1000):
            try:
                session.execute(
                    "INSERT INTO t (id, v, v2, v3) VALUES ({v}, {v}, 'a', 3.0) USING TIMESTAMP {v}".format(v=i))
            except WriteFailure:
                failed = True

        assert failed, "Should fail at least once."
        assert node1.grep_log("Dummy failure"), "Should throw Dummy failure"

        missing_entries = 0
        session = self.patient_exclusive_cql_connection(node1)
        session.execute('USE ks')
        for i in range(1, 1000):
            view_entry = rows_to_list(
                session.execute(SimpleStatement("SELECT * FROM t_by_v WHERE id = {} AND v = {}".format(i, i),
                                                consistency_level=ConsistencyLevel.ONE)))
            base_entry = rows_to_list(session.execute(SimpleStatement("SELECT * FROM t WHERE id = {}".format(i),
                                                                      consistency_level=ConsistencyLevel.ONE)))

            if not base_entry:
                missing_entries += 1
            if not view_entry:
                missing_entries += 1

        logger.debug("Missing entries {}".format(missing_entries))
        assert missing_entries > 0

        logger.debug('Restarting node1 to ensure commit log is replayed')
        node1.stop(wait_other_notice=True)
        # Set batchlog.replay_timeout_seconds=1 so we can ensure batchlog will be replayed below
        node1.start(jvm_args=["-Dcassandra.batchlog.replay_timeout_in_ms=1"], no_wait=True)

        logger.debug('Replay batchlogs')
        time.sleep(0.001)  # Wait batchlog.replay_timeout_in_ms=1 (ms)
        self._replay_batchlogs()

        logger.debug('Verify that both the base table entry and view are present after commit and batchlog replay')
        session = self.patient_exclusive_cql_connection(node1)
        session.execute('USE ks')
        for i in range(1, 1000):
            view_entry = rows_to_list(
                session.execute(SimpleStatement("SELECT * FROM t_by_v WHERE id = {} AND v = {}".format(i, i),
                                                consistency_level=ConsistencyLevel.ONE)))
            base_entry = rows_to_list(session.execute(SimpleStatement("SELECT * FROM t WHERE id = {}".format(i),
                                                                      consistency_level=ConsistencyLevel.ONE)))

            assert base_entry, "Both base {} and view entry {} should exist.".format(base_entry, view_entry)
            assert view_entry, "Both base {} and view entry {} should exist.".format(base_entry, view_entry)

    def _write_to_hinted_handoff_for_views(self, double_failure):
        """
        Test that view updates are stored as hints in data/view_pending_updates directory
        and that reading data from a view is consistent after updates stored as hints.
        """
        session = self.prepare(user_table=True, rf=3, nodes=3, options={'hinted_handoff_enabled': True})
        node1, node2, node3 = self.cluster.nodelist()
        ks = 'ks'
        session.execute('USE {}'.format(ks))

        for i in range(500):
            session.execute(SimpleStatement("INSERT INTO users (username, password, gender, birth_year) VALUES"
                                            "('Jane{}', 'Doe', 'F', 1980)".format(i),
                                            consistency_level=ConsistencyLevel.ALL))
        self.cluster.flush()

        stopped = [node2]
        if double_failure:
            stopped.append(node3)
        self.cluster.stop_nodes(stopped, wait_other_notice=True)

        num_updates = 500
        for i in range(num_updates):
            session.execute(
                SimpleStatement("UPDATE users SET state = 'CA{}' WHERE username = 'Jane{}'".format(i, 1500 - 2 * i),
                                consistency_level=ConsistencyLevel.ANY))
        self.cluster.start_nodes(stopped, wait_for_binary_proto=True, wait_other_notice=True)
        view = 'users_by_state'
        # Wait until the view is built.
        # Note that it won't wait until all the data is propagated from hinted handoff
        wait_for_view(cluster=self.cluster, session=session, ks=ks, view=view)
        total_wait = 60
        sleep_time = 5
        returned_rows = []
        for _ in range(total_wait // sleep_time):
            returned_rows = [row for row in session.execute(SimpleStatement(
                "SELECT * FROM {}".format(view), consistency_level=ConsistencyLevel.ALL))]
            if len(returned_rows) == num_updates:
                break
            else:
                logger.debug("Expected {} rows, got {}. Will retry in {} second(s)".format(
                    num_updates, len(returned_rows), sleep_time))
                time.sleep(sleep_time)

        assert len(returned_rows) == num_updates
        for row in returned_rows:
            assert int(row.username[4:]) == 1500 - 2 * int(row.state[2:])

    def test_write_to_hinted_handoff_for_views(self):
        self._write_to_hinted_handoff_for_views(double_failure=False)

    def test_write_to_hinted_handoff_for_views_double_failure(self):
        self._write_to_hinted_handoff_for_views(double_failure=True)

    def test_virtual_columns_schema(self):
        """
        Test that virtual columns in materialized views are correctly
        propagated between nodes as part of the schema. Reproduces issue #4339.
        """
        # Create a cluster of three nodes.
        cluster = self.cluster
        cluster.populate([3, 0])
        cluster.start(wait_other_notice=True, wait_for_binary_proto=True)
        [node1, node2, node3] = self.cluster.nodelist()
        # Create a keyspace and base table, while the three nodes are alive
        session = self.patient_cql_connection(node1)
        self.rf = 3
        create_ks(session, 'ks', self.rf)
        session.execute(
            ("CREATE TABLE tab (a INT, b INT, c INT,"
             "PRIMARY KEY (a));")
        )
        # Wait for all nodes to know about the base table
        session.cluster.control_connection.wait_for_schema_agreement()
        # stop the second node, and create a materialized view which only
        # the first and third node will know about:
        node2.stop(wait_other_notice=True)
        session.execute(
            ("CREATE MATERIALIZED VIEW mv AS "
             "SELECT a,b FROM tab WHERE a IS NOT NULL AND b IS NOT NULL "
             "PRIMARY KEY (a)"))
        # Because the above materialized views has the same key columns
        # as the base table and an unselected column (c), it will have c
        # as a "virtual column", and should be listed in the
        # "view_virtual_columns" system table.
        result1 = list(session.execute(
            ("SELECT * FROM system_schema.view_virtual_columns "
             "WHERE keyspace_name='ks' ALLOW FILTERING")))
        logger.debug(result1)
        assert len(result1) == 1, "expecting one virtual column"
        # Start the dead node. It should copy the missing schema tables
        # from the live node, including the view_virtual_columns table.
        node2.start(wait_other_notice=True, wait_for_binary_proto=True)
        session2 = self.patient_exclusive_cql_connection(node2)
        result2 = list(session2.execute(
            ("SELECT * FROM system_schema.view_virtual_columns "
             "WHERE keyspace_name='ks' ALLOW FILTERING")))
        logger.debug(result2)
        assert len(result2) == 1, "expecting one virtual column"
        assert result1 == result2, "expecting same results on both nodes"

    @pytest.mark.dtest_debug
    @pytest.mark.scylla_mode('!release')
    def test_injected_noncritical_errors(self):
        self.fixture_dtest_setup.ignore_log_patterns += [r'.*std::runtime_error.*view.*']
        cluster = self.cluster
        cluster.populate([2, 0])
        cluster.start(wait_other_notice=True, wait_for_binary_proto=True)
        nodes = self.cluster.nodelist()
        [node1, node2] = nodes
        session = self.patient_cql_connection(node1)
        session2 = self.patient_cql_connection(node2)
        self.rf = 2
        create_ks(session, 'ks', self.rf)
        session.execute(
            ("CREATE TABLE tab (a INT, b INT, c INT,"
             "PRIMARY KEY (a));")
        )
        # Wait for both nodes to know about the base table
        session.cluster.control_connection.wait_for_schema_agreement()
        for node in nodes:
            self.disable_errors(node)
        # Arm the injection points
        injection_points = [
            "table_push_view_replica_updates_stale_time_point",
            "table_push_view_replica_updates_timeout",
            "view_builder_load_views",
            "view_builder_check_for_built_views",
            "view_builder_consume_new_partition",
            "view_builder_consume_tombstone",
            "view_builder_consume_static_row",
            "view_builder_consume_clustering_row",
            "view_builder_consume_range_tombstone",
            "view_builder_flush_fragments",
            "view_builder_consume_end_of_partition",
            "view_builder_consume_end_of_stream",
            "view_builder_mark_view_as_built",
            "view_update_generator_consume_staging_sstable",
            "view_update_generator_collect_consumed_sstables",
            "view_update_generator_move_staging_sstable",
            "view_update_generator_registering_staging_sstable",
        ]
        for i, injection_point in enumerate(injection_points):
            self.enable_error(injection_point, i % 2, one_shot=True)

        for i in range(10):
            session.execute(SimpleStatement("INSERT INTO tab (a, b, c) VALUES"
                                            f"({i}, {2 * i}, {-i})", consistency_level=ConsistencyLevel.ALL))
        self.cluster.flush()

        # Create a view and wait until it's built
        session.execute(
            ("CREATE MATERIALIZED VIEW mv AS "
             "SELECT a,b FROM tab WHERE a IS NOT NULL AND b IS NOT NULL "
             "PRIMARY KEY (b,a)"))

        for i in range(5, 20):
            session2.execute(SimpleStatement("INSERT INTO ks.tab (a, b, c) VALUES"
                                             f"({i}, {2 * i}, {-i})", consistency_level=ConsistencyLevel.ALL))
        self.cluster.flush()

        get_all = "SELECT * FROM ks.mv"

        self.eventually(lambda: assert_row_count_in_select(session, get_all, 20, ConsistencyLevel.ALL))
        self.eventually(lambda: assert_row_count_in_select(session2, get_all, 20, ConsistencyLevel.ALL))


# For read verification


class MutationPresence(Enum):
    __order__ = 'match extra missing excluded unknown'
    match = 1
    extra = 2
    missing = 3
    excluded = 4
    unknown = 5


class MM(object):
    mp = None

    def out(self):
        pass


class Match(MM):

    def __init__(self):
        self.mp = MutationPresence.match

    def out(self):
        return None


class Extra(MM):
    expecting = None
    value = None
    row = None

    def __init__(self, expecting, value, row):
        self.mp = MutationPresence.extra
        self.expecting = expecting
        self.value = value
        self.row = row

    def out(self):
        return "Extra. Expected {} instead of {}; row: {}".format(self.expecting, self.value, self.row)


class Missing(MM):
    value = None
    row = None

    def __init__(self, value, row):
        self.mp = MutationPresence.missing
        self.value = value
        self.row = row

    def out(self):
        return "Missing. At {}".format(self.row)


class Excluded(MM):

    def __init__(self):
        self.mp = MutationPresence.excluded

    def out(self):
        return None


class Unknown(MM):

    def __init__(self):
        self.mp = MutationPresence.unknown

    def out(self):
        return None


readConsistency = ConsistencyLevel.QUORUM
writeConsistency = ConsistencyLevel.QUORUM
SimpleRow = collections.namedtuple('SimpleRow', 'a b c d')


def row_generate(i, num_partitions):
    return SimpleRow(a=i % num_partitions, b=(i % 400) / num_partitions, c=i, d=i)


# Create a threaded session and execute queries from a Queue
def thread_session(ip, queue, start, end, rows, num_partitions):

    def execute_query(session, select_gi, i):
        row = row_generate(i, num_partitions)
        if (row.a, row.b) in rows:
            base = rows[(row.a, row.b)]
        else:
            base = -1
        gi = list(session.execute(select_gi, [row.c, row.a]))
        if base == i and len(gi) == 1:
            return Match()
        elif base != i and len(gi) == 1:
            return Extra(base, i, (gi[0][0], gi[0][1], gi[0][2], gi[0][3]))
        elif base == i and len(gi) == 0:
            return Missing(base, i)
        elif base != i and len(gi) == 0:
            return Excluded()
        else:
            return Unknown()

    try:
        cluster = Cluster([ip])
        session = cluster.connect()
        select_gi = session.prepare("SELECT * FROM mvtest.mv1 WHERE c = ? AND a = ?")
        select_gi.consistency_level = readConsistency

        for i in range(start, end):
            ret = execute_query(session, select_gi, i)
            queue.put_nowait(ret)
    except Exception as e:
        print(str(e))
        queue.close()


@pytest.mark.skipif(sys.platform == 'win32', reason='Bug in python on Windows: https://bugs.python.org/issue10128')
@pytest.mark.dtest_full
class TestMaterializedViewsConsistency(Tester):

    def prepare(self, user_table=False, options={}):
        cluster = self.cluster
        if options:
            logger.debug(f"Setting cluster configuration options: {options}")
            cluster.set_configuration_options(values=options)
        cluster.populate(3).start()
        node2 = cluster.nodelist()[1]

        # Keep the status of async requests
        self.exception_type = collections.Counter()
        self.num_request_done = 0
        self.counts = {}
        for mp in MutationPresence:
            self.counts[mp] = 0
        self.rows = {}
        self.update_stats_every = 100

        logger.debug("Set to talk to node 2")
        self.session = self.patient_cql_connection(node2)

        return self.session

    def _print_write_status(self, row):
        output = "\r{}".format(row)
        for key in self.exception_type.keys():
            output = "{} ({}: {})".format(output, key, self.exception_type[key])
        sys.stdout.write(output)
        sys.stdout.flush()

    def _print_read_status(self, row):
        if self.counts[MutationPresence.unknown] == 0:
            sys.stdout.write(
                "\rOn {}; match: {}; extra: {}; missing: {}".format(
                    row,
                    self.counts[MutationPresence.match],
                    self.counts[MutationPresence.extra],
                    self.counts[MutationPresence.missing])
            )
        else:
            sys.stdout.write(
                "\rOn {}; match: {}; extra: {}; missing: {}; WTF: {}".format(
                    row,
                    self.counts[MutationPresence.match],
                    self.counts[MutationPresence.extra],
                    self.counts[MutationPresence.missing],
                    self.counts[MutationPresence.unknown])
            )
        sys.stdout.flush()

    def _do_row(self, insert_stmt, i, num_partitions):

        # Error callback for async requests
        def handle_errors(row, exc):
            self.num_request_done += 1
            try:
                name = type(exc).__name__
                self.exception_type[name] += 1
            except Exception as e:
                print(traceback.format_exception_only(type(e), e))

        # Success callback for async requests
        def success_callback(row):
            self.num_request_done += 1

        if i % self.update_stats_every == 0:
            self._print_write_status(i)

        row = row_generate(i, num_partitions)
        async_exec = self.session.execute_async(insert_stmt, row)
        errors = partial(handle_errors, row)
        async_exec.add_callbacks(success_callback, errors)

    def _populate_rows(self):
        statement = SimpleStatement(
            "SELECT a, b, c FROM mvtest.test1",
            consistency_level=readConsistency
        )
        data = self.session.execute(statement)
        for row in data:
            self.rows[(row.a, row.b)] = row.c

    @pytest.mark.require('2210')
    def test_single_partition_consistent_reads_after_write(self):
        """
        Tests consistency of multiple writes to a single partition
        @jira_ticket CASSANDRA-10981
        """
        self._consistent_reads_after_write_test(1)

    # nodetool: Found unexpected parameters: [replaybatchlog]
    @pytest.mark.require('2210')
    def test_multi_partition_consistent_reads_after_write(self):
        """
        Taken from Cassandra
        Tests consistency of multiple writes to a multiple partitions
        @jira_ticket CASSANDRA-10981
        """
        self._consistent_reads_after_write_test(20)

    # Scylla doesn't rely on the batchlog, but running this
    # test in debug mode fails because some writes timeout.
    # To enable this, we would need to store failed updates;
    # we plan to leverage hinted handoff for this.

    def _consistent_reads_after_write_test(self, num_partitions):

        session = self.prepare()
        [node1, node2, node3] = self.cluster.nodelist()

        # Test config
        lower = 0
        upper = 100000
        processes = 4
        queues = [None] * processes
        eachProcess = (upper - lower) / processes

        logger.debug("Creating schema")
        session.execute(
            ("CREATE KEYSPACE IF NOT EXISTS mvtest WITH replication = "
             "{'class': 'SimpleStrategy', 'replication_factor': '3'}")
        )
        session.execute(
            "CREATE TABLE mvtest.test1 (a int, b int, c int, d int, PRIMARY KEY (a,b))"
        )
        session.cluster.control_connection.wait_for_schema_agreement()

        insert1 = session.prepare("INSERT INTO mvtest.test1 (a,b,c,d) VALUES (?,?,?,?)")
        insert1.consistency_level = writeConsistency

        logger.debug("Writing data to base table")
        for i in range(upper // 10):
            self._do_row(insert1, i, num_partitions)

        logger.debug("Creating materialized view")
        session.execute(
            ('CREATE MATERIALIZED VIEW mvtest.mv1 AS '
             'SELECT a,b,c,d FROM mvtest.test1 WHERE a IS NOT NULL AND b IS NOT NULL AND '
             'c IS NOT NULL PRIMARY KEY (c,a,b)')
        )
        session.cluster.control_connection.wait_for_schema_agreement()

        logger.debug("Writing more data to base table")
        for i in range(upper // 10, upper):
            self._do_row(insert1, i, num_partitions)

        # Wait that all requests are done
        while self.num_request_done < upper:
            time.sleep(1)

        logger.debug("Making sure all batchlogs are replayed on node1")
        node1.nodetool("replaybatchlog")
        logger.debug("Making sure all batchlogs are replayed on node2")
        node2.nodetool("replaybatchlog")
        logger.debug("Making sure all batchlogs are replayed on node3")
        node3.nodetool("replaybatchlog")

        logger.debug("Finished writes, now verifying reads")
        self._populate_rows()

        for i in range(processes):
            start = lower + (eachProcess * i)
            if i == processes - 1:
                end = upper
            else:
                end = lower + (eachProcess * (i + 1))
            q = Queue()
            node_ip = self.get_ip_from_node(node2)
            p = Process(target=thread_session, args=(node_ip, q, start, end, self.rows,
                                                     num_partitions))
            p.start()
            queues[i] = q

        for i in range(lower, upper):
            if i % 100 == 0:
                self._print_read_status(i)
            mm = queues[i % processes].get()
            if not mm.out() is None:
                sys.stdout.write("\r{}\n".format(mm.out()))
            self.counts[mm.mp] += 1

        self._print_read_status(upper)
        sys.stdout.write("\n")
        sys.stdout.flush()


@pytest.mark.dtest_full
class TestInterruptBuildProcess(CommonUtils):
    # running multiple test cases in parallel with max/half number of shards
    # might exhaust aio-max-nr
    lock = Lock()

    @staticmethod
    def oddity(n):
        return n % 2

    def _max_shards(self):
        if cpu_count() < 2:
            pytest.skip("This test requires a minimum of 2 cpus")
        elif cpu_count() <= 4:
            return cpu_count()
        else:
            # highest even number of cpus
            return cpu_count() - self.oddity(cpu_count())

    def _half_shards(self):
        if cpu_count() < 2:
            pytest.skip("This test requires a minimum of 2 cpus")
        elif cpu_count() <= 4:
            return cpu_count() - 1
        else:
            # half number of cpus, made odd
            half = cpu_count() // 2
            return half + (1 - self.oddity(half))

    def _low_shards(self):
        if cpu_count() < 3:
            pytest.skip("This test requires a minimum of 3 cpus")
        elif cpu_count() < 4:
            return 1
        else:
            return 2

    @pytest.mark.dtest_heavy
    def test_interrupt_build_process_test(self):
        logger.debug("Acquiring lock")
        with TestInterruptBuildProcess.lock:
            logger.debug("Running test")
            self._interrupt_build_process_test()

    def _interrupt_build_process_test(self):
        """Test that an interrupted MV build process is resumed as it should"""
        session = self.prepare(options={'hinted_handoff_enabled': False, 'shadow_round_ms': 1000})
        node1, node2, node3 = self.cluster.nodelist()

        session.execute("CREATE TABLE t (id int PRIMARY KEY, v int, v2 text, v3 decimal)")

        rows = 200000
        if hasattr(self.cluster, 'scylla_mode') and self.cluster.scylla_mode == 'debug':
            rows = 10000
        logger.debug("Inserting initial data")
        insert_stmt = session.prepare("INSERT INTO t (id, v, v2, v3) VALUES (?, ?, ?, ?)")
        for i in range(rows):
            session.execute(insert_stmt, (i, i, 'a', 3.0))

        logger.debug("Create a MV")
        # Don't wait for schema agreement, or we risk view building concluding too soon
        session.cluster.max_schema_agreement_wait = 0
        session.execute(("CREATE MATERIALIZED VIEW t_by_v AS SELECT * FROM t "
                         "WHERE v IS NOT NULL AND id IS NOT NULL PRIMARY KEY (v, id)"))

        wait_for_view_build_start(session, "ks", "t_by_v")

        logger.debug("Stop the cluster. Interrupt the MV build process.")
        self.stop_cluster()

        logger.debug("Ensure view building didn't finish.")
        self._ensure_view_building_did_not_finish(len(self.cluster.nodelist()))

        logger.debug("Restart the cluster")
        self.cluster.start(wait_for_binary_proto=True)
        session = self.patient_cql_connection(node1)
        session.execute("USE ks")

        logger.debug("Wait and ensure the MV build resumed.")
        wait_for_view(cluster=self.cluster, session=session, ks="ks", view="t_by_v")

        logger.debug("Verify all data")
        assert_row_count(session, 't_by_v', rows, consistency_level=ConsistencyLevel.ALL)

        logger.debug("Stopping cluster")
        self.stop_cluster()

    def test_interrupt_build_process_with_resharding_low_to_half_test(self):
        """Test that an interrupted MV build process is resumed, with resharding 1 -> cpu_count() // 2"""
        self._do_resharding_test(self._low_shards(), self._half_shards())

    @pytest.mark.next_gating
    def test_interrupt_build_process_with_resharding_half_to_max_test(self):
        """Test that an interrupted MV build process is resumed, with resharding cpu_count() // 2 -> cpu_count()"""
        # For some reason, Scylla's hwloc only sees cpu_count() - 1 cpus
        self._do_resharding_test(self._half_shards(), self._max_shards())

    def test_interrupt_build_process_with_resharding_max_to_half_test(self):
        """Test that an interrupted MV build process is resumed, with resharding cpu_count() -> cpu_count() // 2"""
        # For some reason, Scylla's hwloc only sees cpu_count() - 1 cpus
        self._do_resharding_test(self._max_shards(), self._half_shards())

    def test_interrupt_build_process_with_resharding_half_to_low_test(self):
        """Test that an interrupted MV build process is resumed, with resharding cpu_count() // 2 -> 1"""
        self._do_resharding_test(self._half_shards(), self._low_shards())

    def test_interrupt_build_process_and_resharding_low_to_half_test(self):
        """Test that an interrupted MV build is resumed after interrupted resharding,
        with resharding 1 -> cpu_count() // 2"""
        self._do_resharding_test(self._low_shards(), self._half_shards(),
                                 interrupt_resharding=True)

    def test_interrupt_build_process_and_resharding_half_to_max_test(self):
        """Test that an interrupted MV build process is resumed after interrupted resharding,
        with resharding cpu_count() // 2 -> cpu_count()"""
        # For some reason, Scylla's hwloc only sees cpu_count() - 1 cpus
        self._do_resharding_test(self._half_shards(), self._max_shards(),
                                 interrupt_resharding=True)

    def test_interrupt_build_process_and_resharding_max_to_half_test(self):
        """Test that an interrupted MV build process is resumed after interrupted resharding,
        with resharding cpu_count() -> cpu_count() // 2"""
        # For some reason, Scylla's hwloc only sees cpu_count() - 1 cpus
        self._do_resharding_test(self._max_shards(), self._half_shards(),
                                 interrupt_resharding=True)

    def test_interrupt_build_process_and_resharding_half_to_low_test(self):
        """Test that an interrupted MV build process is resumed after interrupted resharding,
        with resharding cpu_count() // 2 -> 2"""
        # when changing the number of shards from N to 1
        # no resharding takes place as all sstables will naturally belong to
        # that single shard.
        if self._low_shards() == 1:
            pytest.skip("This test requires a minimum of 4 cpus")
        self._do_resharding_test(self._half_shards(), self._low_shards(),
                                 interrupt_resharding=True)

    @staticmethod
    def set_memory_param(smp):
        return '{}M'.format(512 * int(smp))

    def _do_resharding_test(self, smp_before, smp_after, compression='LZ4Compressor', interrupt_resharding=False):
        logger.debug("Acquiring lock")
        with TestInterruptBuildProcess.lock:
            self.__do_resharding_test(smp_before, smp_after, compression, interrupt_resharding)

    def __do_resharding_test(self, smp_before, smp_after, compression, interrupt_resharding):
        logger.debug(
            f"Running resharding test from {smp_before} to {smp_after} shards: interrupt_resharding={interrupt_resharding}")
        self.ignore_log_patterns += [
            r'view - Error applying view update to .*: exceptions::unavailable_exception',
            r'view - Error applying view update to .*: exceptions::mutation_write_timeout_exception',
            r'view - Error applying view update to .*: exceptions::mutation_write_failure_exception',
            r'view - Error applying view update to .*: data_dictionary::no_such_column_family',
        ]
        if interrupt_resharding:
            self.ignore_log_patterns += [
                r"Exception while populating .*: sstables::compaction_stopped_exception",
                r'Startup failed: seastar::sleep_aborted',
            ]
        session = self.prepare(options={'hinted_handoff_enabled': False, 'shadow_round_ms': 1000, 'prometheus_port': 0, 'read_request_timeout_in_ms': 100000, 'range_request_timeout_in_ms': 100000},
                               jvm_args=['--smp', str(smp_before), '--memory', self.set_memory_param(smp_before)])
        node1, node2, node3 = self.cluster.nodelist()

        query = "CREATE TABLE t (id int PRIMARY KEY, v int, v2 text, v3 decimal)"
        if compression:
            query += f" WITH compression = {{'sstable_compression': '{compression}'}}"
        session.execute(query)

        rows = 200000
        if hasattr(self.cluster, 'scylla_mode') and self.cluster.scylla_mode == 'debug':
            rows = 10000
        logger.debug("Inserting initial data; smp = {}".format(smp_before))
        insert_stmt = session.prepare("INSERT INTO t (id, v, v2, v3) VALUES (?, ?, ?, ?)")
        for i in range(rows):
            session.execute(insert_stmt, (i, i, str(i), 3.0))

        logger.debug("Create a couple of MVs")
        # Don't wait for schema agreement, or we risk view building concluding too soon
        session.cluster.max_schema_agreement_wait = 0
        session.execute(("CREATE MATERIALIZED VIEW t_by_v AS SELECT * FROM t "
                         "WHERE v IS NOT NULL AND id IS NOT NULL PRIMARY KEY (v, id)"))
        session.execute(("CREATE MATERIALIZED VIEW t_by_v2 AS SELECT * FROM t "
                         "WHERE v2 IS NOT NULL AND id IS NOT NULL PRIMARY KEY (v2, id)"))

        wait_for_view_build_start(session, "ks", "t_by_v")

        logger.debug("Stop the cluster. Interrupt the MV build process.")
        # Our views build quickly, so instead of having to insert lots of data and
        # risk the test taking too long, just force the cluster down
        self.stop_cluster()

        logger.debug("Ensure view building didn't finish.")
        have_finished = 0
        for node in self.cluster.nodelist():
            finished = node.grep_log("Finished building view")
            have_finished += len(finished)
        assert have_finished < 2 * len(self.cluster.nodelist())

        logger.debug("Restart the cluster with shards {}".format(smp_after))
        for node in self.cluster.nodelist():
            logger.debug("Starting node " + node.name)
            jvm_args = ['--smp', str(smp_after),
                        '--memory', self.set_memory_param(smp_after)]
            if interrupt_resharding:
                mark = node.mark_log()
                node.start(jvm_args=jvm_args, no_wait=True)
                node.watch_log_for(r"Reshard.*ks", from_mark=mark)
                logger.debug(f"Stopping node {node.name} during resharding")
                node.stop(gently=False, wait_other_notice=False)
                time.sleep(5)
                logger.debug(f"Restarting node {node.name}")
            node.start(jvm_args=jvm_args)

        session = self.patient_cql_connection(node1)
        session.execute("USE ks")

        logger.debug("Wait and ensure the MV build resumed.")
        wait_for_view(cluster=self.cluster, session=session, ks='ks', view="t_by_v")
        wait_for_view(cluster=self.cluster, session=session, ks='ks', view="t_by_v2")

        logger.debug("Verify all data")
        self.eventually(lambda: assert_row_count(session, 't_by_v', rows, consistency_level=ConsistencyLevel.ALL))
        self.eventually(lambda: assert_row_count(session, 't_by_v2', rows, consistency_level=ConsistencyLevel.ALL))

        logger.debug("Stopping cluster")
        self.stop_cluster()
