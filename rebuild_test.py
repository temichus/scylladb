import logging
import time
from threading import Thread

import pytest

from cassandra import ConsistencyLevel
from ccmlib.node import NodetoolError

from dtest_class import Tester, create_ks, create_cf
from dtest_setup import DTestSetup
from dtest_setup_overrides import DTestSetupOverrides
from tools.data import insert_c1c2, query_c1c2, create_c1c2_table
from tools.misc import ImmutableMapping

logger = logging.getLogger(__name__)


@pytest.mark.dtest_full
class TestRebuild(Tester):
    @pytest.fixture(scope='function', autouse=True)
    def fixture_dtest_setup_overrides(self, dtest_config):
        dtest_setup_overrides = DTestSetupOverrides()
        dtest_setup_overrides.cluster_options = ImmutableMapping({'start_rpc': 'true'})
        return dtest_setup_overrides

    @pytest.fixture(autouse=True)
    def fixture_add_additional_log_patterns(self, fixture_dtest_setup: DTestSetup):
        fixture_dtest_setup.allow_log_errors = True
        fixture_dtest_setup.ignore_log_patterns = (
            # This one occurs when trying to send the migration to a
            # node that hasn't started yet, and when it does, it gets
            # replayed and everything is fine.
            r'Can\'t send migration request: node.*is down',
            # ignore streaming error during bootstrap
            r'Exception encountered during startup',
            r'Streaming error occurred'
        )

    def add_node(self, i, dc='dc1'):
        return self.cluster.new_node(i, debug=True, data_center=dc)

    @pytest.mark.next_gating
    @pytest.mark.dtest_debug
    def test_simple_rebuild(self):
        """
        @jira_ticket CASSANDRA-9119

        Test rebuild from other dc works as expected.
        """

        keys = 10000

        cluster = self.cluster
        cluster.set_configuration_options(values={'endpoint_snitch': 'GossipingPropertyFileSnitch'})
        node1 = self.add_node(1, 'dc1')

        # start node in dc1
        node1.start(wait_for_binary_proto=True)

        # populate data in dc1
        session = self.patient_exclusive_cql_connection(node1)
        create_ks(session, 'ks', {'dc1': 1})
        create_cf(session, 'cf', columns={'c1': 'text', 'c2': 'text'})
        insert_c1c2(session, n=keys, consistency=ConsistencyLevel.ALL)

        # check data
        for i in range(0, keys):
            query_c1c2(session, i, ConsistencyLevel.ALL)
        session.shutdown()

        # Bootstraping a new node in dc2 with auto_bootstrap: false
        node2 = self.add_node(2, 'dc2')
        node2.start(wait_other_notice=True, wait_for_binary_proto=True)

        # wait for snitch to reload
        time.sleep(60)
        # alter keyspace to replicate to dc2
        session = self.patient_exclusive_cql_connection(node2)
        session.execute("ALTER KEYSPACE ks WITH REPLICATION = {'class':'NetworkTopologyStrategy', 'dc1':1, 'dc2':1};")
        session.execute('USE ks')

        self.rebuild_errors = 0
        self.unexpected_errors = 0

        # rebuild dc2 from dc1
        def rebuild():
            try:
                node2.nodetool('rebuild dc1')
            except NodetoolError as e:
                if 'rebuild is in progress' in str(e):
                    self.rebuild_errors += 1
                else:
                    logger.debug('Unexpected rebuild failure {}'.format(str(e)))
                    self.unexpected_errors += 1

        cmd1 = Thread(target=rebuild)
        cmd1.start()

        # concurrent rebuild should not be allowed (CASSANDRA-9119)
        # (following sleep is needed to avoid conflict in 'nodetool()' method setting up env.)
        time.sleep(.1)
        rebuild()

        cmd1.join()

        # exactly 1 of the two nodetool calls should fail
        # usually it will be the one in the main thread,
        # but occasionally it wins the race with the one in the secondary thread,
        # so we check that one succeeded and the other failed
        assert self.unexpected_errors == 0, 'Unexpected rebuild errors encountered.'
        assert self.rebuild_errors == 1, \
            'Concurrent rebuild should not be allowed, but one rebuild command should have succeeded.'

        # check data
        for i in range(0, keys):
            query_c1c2(session, i, ConsistencyLevel.ALL)

    def _check_data(self, session, keyspaces, tables, keys, cl=ConsistencyLevel.ALL):
        logger.debug("Checking data")
        total = 0
        errors = 0
        for ks in keyspaces:
            for cf in tables:
                cf_name = '{}.{}'.format(ks, cf)
                for i in keys:
                    total += 1
                    try:
                        query_c1c2(session, i, ks=ks, cf=cf, consistency=cl)
                    except AssertionError:
                        errors += 1
        assert errors == 0, "Found {} errors out of {} keys".format(errors, total)

    def test_rebuild_many_tables(self):
        """
        Test rebuilding many tables in same dc works as expected.
        """

        num_keys = 100
        num_tables = 100

        cluster = self.cluster
        cluster.set_configuration_options(values={'endpoint_snitch': 'GossipingPropertyFileSnitch'})
        node1 = self.add_node(1)

        # start node in dc1
        node1.start(wait_for_binary_proto=True)

        # populate data in dc1
        session = self.patient_exclusive_cql_connection(node1)
        ks = 'ks'
        dc = 'dc1'
        create_ks(session, ks, {dc: 1})

        logger.debug(f"Creating {num_tables} tables")
        tables = ['cf_{:04d}'.format(i) for i in range(0, num_tables)]
        for cf in tables:
            create_c1c2_table(session, cf=cf, debug_query=False)
            insert_c1c2(session, n=num_keys, ks=ks, cf=cf, consistency=ConsistencyLevel.ALL)

        keys = [i for i in range(0, num_keys)]
        self._check_data(session, [ks], tables, keys)
        session.shutdown()

        logger.debug("Bootstrapping node2 with {auto_bootstrap: false}")
        node2 = self.add_node(2)
        node2.start(wait_other_notice=True, wait_for_binary_proto=True)

        logger.debug("Adjusting replication")
        session = self.patient_exclusive_cql_connection(node2)
        session.execute(
            "ALTER KEYSPACE {} WITH REPLICATION = {{ 'class':'NetworkTopologyStrategy', '{}':2 }};".format(ks, dc))

        logger.debug("Rebuilding node2")
        node2.nodetool('rebuild')

        logger.debug("Killing node1")
        node1.stop(gently=False)

        self._check_data(session, [ks], tables, keys, cl=ConsistencyLevel.ONE)

    def test_rebuild_many_keyspaces(self):
        """
        Test rebuilding many keyspaces in same dc works as expected.
        """

        num_keys = 100
        num_keyspaces = 50
        num_tables = 2

        cluster = self.cluster
        cluster.set_configuration_options(values={'endpoint_snitch': 'GossipingPropertyFileSnitch'})
        node1 = self.add_node(1)

        # start node in dc1
        node1.start(wait_for_binary_proto=True)

        # populate data in dc1
        session = self.patient_exclusive_cql_connection(node1)
        dc = 'dc1'
        logger.debug(f"Creating {num_keyspaces} keyspaces")
        keyspaces = ['ks_{:04d}'.format(i) for i in range(0, num_keyspaces)]
        for ks in keyspaces:
            create_ks(session, ks, {dc: 1})

        logger.debug(f"Creating {num_tables} table(s) in each ks")
        tables = ['cf_{:04d}'.format(i) for i in range(0, num_tables)]
        for ks in keyspaces:
            for cf in tables:
                create_c1c2_table(session, cf=f'{ks}.{cf}', debug_query=False)
                insert_c1c2(session, n=num_keys, ks=ks, cf=cf, consistency=ConsistencyLevel.ALL)

        keys = [i for i in range(0, num_keys)]
        self._check_data(session, keyspaces, tables, keys)
        session.shutdown()

        logger.debug("Bootstrapping node2 with {auto_bootstrap: false}")
        node2 = self.add_node(2)
        node2.start(wait_other_notice=True, wait_for_binary_proto=True)

        logger.debug("Adjusting replication")
        session = self.patient_exclusive_cql_connection(node2)
        for ks in keyspaces:
            session.execute(
                "ALTER KEYSPACE {} WITH REPLICATION = {{ 'class':'NetworkTopologyStrategy', '{}':2 }};".format(ks, dc))

        logger.debug("Rebuilding node2")
        node2.nodetool('rebuild')

        logger.debug("Killing node1")
        node1.stop(gently=False)

        self._check_data(session, keyspaces, tables, keys, cl=ConsistencyLevel.ONE)

    def test_rebuild_keyspace_with_rf_1(self):
        self.cluster.populate(3)
        self.cluster.set_configuration_options(values={"enable_repair_based_node_ops": "false"})
        self.cluster.start(wait_for_binary_proto=True, wait_other_notice=True)
        logger.debug("Create ks with rf 1 and insert data")
        node1 = self.cluster.nodelist()[0]
        session = self.patient_exclusive_cql_connection(node1)
        create_ks(session, 'ks', rf=1)
        create_c1c2_table(session)
        insert_c1c2(session, n=1000, consistency=ConsistencyLevel.ONE)

        logger.debug("Check keys")
        for i in range(0, 1000):
            query_c1c2(session, i, ConsistencyLevel.ONE)

        logger.debug("Stopping node 3.")
        node3 = self.cluster.nodelist()[2]
        node3.stop(gently=True, wait_other_notice=True)

        logger.debug("Add node4 without bootstrap")
        node4 = self.cluster.new_node(4, auto_bootstrap=False, is_seed=False)
        node4.start(replace_address=self.cluster.get_node_ip(3), wait_for_binary_proto=True)

        # validate that warning mesasages appeared. Not error messages
        node4.watch_log_for(
            exprs=[r"WARN .* Unable to find sufficient sources to stream range .* for keyspace .* with RF = 1 for replace operation"])

        logger.debug("Run rebuild on node 4")
        node4.nodetool("rebuild")
        logger.debug("Rebuild done, Validate that data could lost due to rf=1")

        with pytest.raises(expected_exception=(AssertionError,),
                           match='Found .* errors out of 1000 keys'):
            self._check_data(session, keyspaces=["ks"], tables=["cf"], keys=[i for i in range(1000)])
