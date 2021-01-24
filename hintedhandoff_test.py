import os
import time

import pytest
from cassandra import ConsistencyLevel

from dtest_class import Tester, create_ks
from tools.data import create_c1c2_table, insert_c1c2, query_c1c2


class TestHintedHandoffConfig(Tester):
    """
    Tests the hinted handoff configuration options introduced in
    CASSANDRA-9035.

    @jira_ticket CASSANDRA-9035
    """
    DISABLE_VNODES = None

    @classmethod
    @pytest.fixture(scope='class', autouse=True)
    def pre_setup(cls, dtest_config):
        TestHintedHandoffConfig.DISABLE_VNODES = not dtest_config.use_vnodes

    def _start_two_node_cluster(self, config_options=None):
        """
        Start a cluster with two nodes and return them
        """
        cluster = self.cluster

        if config_options:
            cluster.set_configuration_options(values=config_options)

        if self.DISABLE_VNODES:
            cluster.populate([2]).start()
        else:
            tokens = cluster.balanced_tokens(2)
            cluster.populate([2], tokens=tokens).start()

        return cluster.nodelist()

    @staticmethod
    def _launch_nodetool_cmd(node, cmd):
        """
        Launch a nodetool command and check there is no error, return the result
        """
        out, err = node.nodetool(cmd, capture_output=True)
        assert err == ''
        return out

    def _do_hinted_handoff(self, nodes, enabled):
        """
        Test that if we stop one node the other one
        will store hints only when hinted handoff is enabled
        """
        node1, node2 = nodes
        session = self.patient_exclusive_cql_connection(node1)
        create_ks(session=session, name='ks', rf=2)
        create_c1c2_table(self, session=session)

        node2.stop(wait_other_notice=True)

        insert_c1c2(session=session, n=100, consistency=ConsistencyLevel.ONE)

        log_mark = node1.mark_log()
        node2.start(wait_other_notice=True)

        if enabled:
            node1.watch_log_for(["Finished hinted"], from_mark=log_mark, timeout=120)

        node1.stop(wait_other_notice=True)

        # Check node2 for all the keys that should have been delivered via HH if enabled or not if not enabled
        session = self.patient_exclusive_cql_connection(node2, keyspace='ks')
        for key in range(0, 100):
            if enabled:
                query_c1c2(session=session, key=key, consistency=ConsistencyLevel.ONE)
            else:
                query_c1c2(session=session, key=key, consistency=ConsistencyLevel.ONE, tolerate_missing=True,
                           must_be_missing=True)

    def test_nodetool(self):
        """
        Test various nodetool commands
        """
        nodes = self._start_two_node_cluster()

        for node in nodes:
            res = self._launch_nodetool_cmd(node, 'statushandoff')
            assert 'Hinted handoff is running' == res.rstrip()

            self._launch_nodetool_cmd(node, 'disablehandoff')
            res = self._launch_nodetool_cmd(node, 'statushandoff')
            assert 'Hinted handoff is not running' == res.rstrip()

            self._launch_nodetool_cmd(node, 'enablehandoff')
            res = self._launch_nodetool_cmd(node, 'statushandoff')
            assert 'Hinted handoff is running' == res.rstrip()

            self._launch_nodetool_cmd(node, 'disablehintsfordc dc1')
            res = self._launch_nodetool_cmd(node, 'statushandoff')
            assert f'Hinted handoff is running{os.linesep}Data center dc1 is disabled' == res.rstrip()

            self._launch_nodetool_cmd(node, 'enablehintsfordc dc1')
            res = self._launch_nodetool_cmd(node, 'statushandoff')
            assert 'Hinted handoff is running' == res.rstrip()

    def test_hintedhandoff_disabled(self):
        """
        Test gloabl hinted handoff disabled
        """
        nodes = self._start_two_node_cluster({'hinted_handoff_enabled': False})

        for node in nodes:
            res = self._launch_nodetool_cmd(node, 'statushandoff')
            assert 'Hinted handoff is not running' == res.rstrip()

        self._do_hinted_handoff(nodes, False)

    def test_hintedhandoff_enabled_test(self):
        """
        Test global hinted handoff enabled
        """
        nodes = self._start_two_node_cluster()

        for node in nodes:
            res = self._launch_nodetool_cmd(node, 'statushandoff')
            assert 'Hinted handoff is running' == res.rstrip()

        self._do_hinted_handoff(nodes, True)

    def test_hintedhandoff_dc_disabled(self):
        """
        Test global hinted handoff enabled with the dc disabled
        """
        nodes = self._start_two_node_cluster({'hinted_handoff_disabled_datacenters': ['dc1']})

        for node in nodes:
            res = self._launch_nodetool_cmd(node, 'statushandoff')
            assert f'Hinted handoff is running{os.linesep}Data center dc1 is disabled' == res.rstrip()

        self._do_hinted_handoff(nodes, False)

    def test_hintedhandoff_dc_reenabled(self):
        """
        Test global hinted handoff enabled with the dc disabled first and then re-enabled
        """
        nodes = self._start_two_node_cluster({'hinted_handoff_disabled_datacenters': ['dc1']})

        for node in nodes:
            res = self._launch_nodetool_cmd(node, 'statushandoff')
            assert f'Hinted handoff is running{os.linesep}Data center dc1 is disabled', res.rstrip()

        for node in nodes:
            self._launch_nodetool_cmd(node, 'enablehintsfordc dc1')
            res = self._launch_nodetool_cmd(node, 'statushandoff')
            assert 'Hinted handoff is running' == res.rstrip()

        self._do_hinted_handoff(nodes, True)


class TestHintedHandoff(Tester):  # pylint:disable=too-few-public-methods

    @pytest.mark.no_vnodes
    def test_hintedhandoff_decom(self):
        self.cluster.populate(4).start(wait_for_binary_proto=True)
        node1, node2, node3, node4 = self.cluster.nodelist()
        session = self.patient_cql_connection(node1)
        create_ks(session=session, name='ks', rf=2)
        create_c1c2_table(self, session=session)
        node4.stop(wait_other_notice=True)
        insert_c1c2(session=session, n=100, consistency=ConsistencyLevel.ONE)
        node1.decommission()
        node4.start(wait_for_binary_proto=True)
        node2.decommission()
        node3.decommission()
        time.sleep(5)
        for key in range(0, 100):
            query_c1c2(session=session, key=key, consistency=ConsistencyLevel.ONE)
