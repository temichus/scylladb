import time
from threading import Thread
from unittest import skip
from nose.plugins.attrib import attr

from cassandra import ConsistencyLevel
from ccmlib.node import NodetoolError

from dtest import Tester, debug
from tools import create_c1c2_table, insert_c1c2, query_c1c2


@attr('dtest-full')
class TestRebuild(Tester):

    def __init__(self, *args, **kwargs):
        kwargs['cluster_options'] = {'start_rpc': 'true'}
        # Ignore these log patterns:
        self.ignore_log_patterns = [
            # This one occurs when trying to send the migration to a
            # node that hasn't started yet, and when it does, it gets
            # replayed and everything is fine.
            r'Can\'t send migration request: node.*is down',
            # ignore streaming error during bootstrap
            r'Exception encountered during startup',
            r'Streaming error occurred'
        ]
        Tester.__init__(self, *args, **kwargs)

    def add_node(self, i, dc='dc1'):
        return self.cluster.new_node(i, debug=True, data_center=dc)

    @attr('next-gating')
    @attr('dtest-debug')
    def simple_rebuild_test(self):
        """
        @jira_ticket CASSANDRA-9119

        Test rebuild from other dc works as expected.
        """

        keys = 1000

        cluster = self.cluster
        cluster.set_configuration_options(values={'endpoint_snitch': 'GossipingPropertyFileSnitch'})
        node1 = self.add_node(1, 'dc1')

        # start node in dc1
        node1.start(wait_for_binary_proto=True)

        # populate data in dc1
        session = self.patient_exclusive_cql_connection(node1)
        self.create_ks(session, 'ks', {'dc1': 1})
        self.create_cf(session, 'cf', columns={'c1': 'text', 'c2': 'text'})
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
                    debug('Unexpected rebuild failure {}'.format(str(e)))
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
        self.assertEqual(self.unexpected_errors, 0,
                         msg='unexpected rebuild errors encountered.')
        self.assertEqual(self.rebuild_errors, 1,
                         msg='concurrent rebuild should not be allowed, but one rebuild command should have succeeded.')

        # check data
        for i in range(0, keys):
            query_c1c2(session, i, ConsistencyLevel.ALL)

    def _check_data(self, session, keyspaces, tables, keys, cl=ConsistencyLevel.ALL):
        debug("Checking data")
        total = 0
        errors = 0
        for ks in keyspaces:
            for cf in tables:
                cf_name = '{}.{}'.format(ks, cf)
                for i in keys:
                    total += 1
                    try:
                        query_c1c2(session, i, cf=cf_name, consistency=cl)
                    except AssertionError:
                        errors += 1
        assert errors == 0, "Found {} errors out of {} keys".format(errors, total)

    def rebuild_many_tables_test(self):
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
        self.create_ks(session, ks, {dc: 1})

        debug("Creating {} tables".format(num_tables))
        tables = ['cf_{:04d}'.format(i) for i in range(0, num_tables)]
        for cf in tables:
            create_c1c2_table(self, session, cf=cf, debug_query=False)
            insert_c1c2(session, n=num_keys, cf=cf, consistency=ConsistencyLevel.ALL)

        keys = [i for i in range(0, num_keys)]
        self._check_data(session, [ks], tables, keys)
        session.shutdown()

        debug("Bootstrapping node2 with {auto_bootstrap: false}")
        node2 = self.add_node(2)
        node2.start(wait_other_notice=True, wait_for_binary_proto=True)

        debug("Adjusting replication")
        session = self.patient_exclusive_cql_connection(node2)
        session.execute(
            "ALTER KEYSPACE {} WITH REPLICATION = {{ 'class':'NetworkTopologyStrategy', '{}':2 }};".format(ks, dc))

        debug("Rebuilding node2")
        node2.nodetool('rebuild')

        debug("Killing node1")
        node1.stop(gently=False)

        self._check_data(session, [ks], tables, keys, cl=ConsistencyLevel.ONE)

    def rebuild_many_keyspaces_test(self):
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
        debug("Creating {} keyspaces".format(num_keyspaces))
        keyspaces = ['ks_{:04d}'.format(i) for i in range(0, num_keyspaces)]
        for ks in keyspaces:
            self.create_ks(session, ks, {dc: 1})

        debug("Creating {} table(s) in each ks".format(num_tables))
        tables = ['cf_{:04d}'.format(i) for i in range(0, num_tables)]
        for ks in keyspaces:
            for cf in tables:
                cf_name = '{}.{}'.format(ks, cf)
                create_c1c2_table(self, session, cf=cf_name, debug_query=False)
                insert_c1c2(session, n=num_keys, cf=cf_name, consistency=ConsistencyLevel.ALL)

        keys = [i for i in range(0, num_keys)]
        self._check_data(session, keyspaces, tables, keys)
        session.shutdown()

        debug("Bootstrapping node2 with {auto_bootstrap: false}")
        node2 = self.add_node(2)
        node2.start(wait_other_notice=True, wait_for_binary_proto=True)

        debug("Adjusting replication")
        session = self.patient_exclusive_cql_connection(node2)
        for ks in keyspaces:
            session.execute(
                "ALTER KEYSPACE {} WITH REPLICATION = {{ 'class':'NetworkTopologyStrategy', '{}':2 }};".format(ks, dc))

        debug("Rebuilding node2")
        node2.nodetool('rebuild')

        debug("Killing node1")
        node1.stop(gently=False)

        self._check_data(session, keyspaces, tables, keys, cl=ConsistencyLevel.ONE)
