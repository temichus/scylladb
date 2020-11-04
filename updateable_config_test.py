"""
Dtest for configuration runtime

Reference:
- https://github.com/scylladb/scylla/wiki/Updateable-Configuration
- https://github.com/scylladb/scylla/issues/2517
  Scylla should be able to re-read the configuration file without restart). #2517
- https://github.com/scylladb/scylla/commit/2abe015150431c61ab5e0d1bd60be9c7b3517cd1
  database: allow live update of the compaction_enforce_min_threshold config item
- https://github.com/scylladb/scylla/commit/eb496b5eaee29748f810dde00ef1d7c673b73a43
  Merge "Allow changing configuration at runtime" from Avi
"""

import os
import signal
import requests

from nose.plugins.attrib import attr

from tools import require, insert_c1c2

from dtest import Tester, debug


@attr('dtest-full', 'single_node')
class TestUpdateableConfig(Tester):
    """
    Scylla supported to change some of configuration in runtime, this
    test tested both supported and unsupported parameters.
    """

    @staticmethod
    def trigger_reload_config(node):
        """
        Signalling the scylla process with SIGHUP to trigger the configuration change effective
        """
        node.kill(signal.SIGHUP)

    def change_and_verify_config(self, node, param, value, verify_response):
        """
        Change configuration in scylla.yaml and make it effective. The updated value is verified by
        API.
        """
        mark = node.mark_log()
        debug('Change configuration `%s`' % param)
        response = requests.get('http://%s:10000/v2/config/%s' % (self.get_ip_from_node(node),
                                                                  param))
        debug('Original value before change: %s' % response.text)
        node.set_configuration_options({param: value})
        self.trigger_reload_config(node)
        node.watch_log_for('completed re-reading configuration file', from_mark=mark)

        debug('Using the API to validate the configuration change, expected: %s' % verify_response)
        response = requests.get('http://%s:10000/v2/config/%s' % (self.get_ip_from_node(node),
                                                                  param))
        self.assertEquals(response.text, verify_response, 'response: %s, expected: %s' %
                          (response.text, verify_response))

    @attr('next-gating')
    @attr('dtest-debug')
    def test_compaction_enforce_min_threshold(self):
        self.cluster.populate(1).start(wait_other_notice=True, wait_for_binary_proto=True)
        node1 = self.cluster.nodelist()[0]

        node1.stress(['write', 'n=10000', '-rate', 'threads=8'])
        session = self.patient_cql_connection(node1)
        session.execute("""
            ALTER TABLE keyspace1.standard1 WITH compaction = {
                'class' : 'SizeTieredCompactionStrategy', 'min_threshold' : 7 }
        """)

        self.change_and_verify_config(node1, 'compaction_enforce_min_threshold', True, 'true')
        node1.stress(['mixed', 'n=10000', '-rate', 'threads=8'])

        self.change_and_verify_config(node1, 'compaction_enforce_min_threshold', False, 'false')
        node1.stress(['mixed', 'n=10000', '-rate', 'threads=8'])

    def test_verify_min_threshold(self):
        self.cluster.populate(1).start(wait_other_notice=True, wait_for_binary_proto=True)
        node1 = self.cluster.nodelist()[0]
        session = self.patient_cql_connection(node1)

        min_threshold = 5
        insert_keys_num = 1

        self.create_ks(session, 'ks', 1)
        self.create_cf(session, 'cf', columns={'c1': 'text', 'c2': 'text'})
        insert_c1c2(session, keys=range(100))
        session.execute("""
            ALTER TABLE ks.cf WITH compaction = {
                'class' : 'SizeTieredCompactionStrategy', 'min_threshold' : %d }
        """ % min_threshold)

        self.change_and_verify_config(node1, 'compaction_enforce_min_threshold', True, 'true')
        mark = node1.mark_log()
        compact_log = "compaction -.*Compacting \[%s" % os.path.join(node1.get_path(), "data/ks/cf")

        for i in range(min_threshold - 1):
            insert_c1c2(session, n=insert_keys_num)
            node1.flush()
        try:
            node1.watch_log_for(compact_log, from_mark=mark, timeout=10)
        except Exception as ex:
            debug(ex)
            assert "Missing: ['compaction -.*Compacting" in str(ex)

        insert_c1c2(session, n=insert_keys_num)
        node1.flush()
        debug('Reach to min threshold, expect compact to be triggered')
        node1.watch_log_for(compact_log, from_mark=mark, timeout=10)

        mark = node1.mark_log()
        debug('Execute compact to clean the threshold counting')
        node1.compact()
        node1.watch_log_for(compact_log, from_mark=mark, timeout=10)

        self.change_and_verify_config(node1, 'compaction_enforce_min_threshold', False, 'false')
        mark = node1.mark_log()
        insert_c1c2(session, n=insert_keys_num)
        node1.flush()
        debug('compaction_enforce_min_threshold is disabled, expect compact to be triggered by one insert')
        node1.watch_log_for(compact_log, from_mark=mark, timeout=10)

    @require('#5382')
    def test_auto_adjust_flush_quota(self):
        """
        auto_adjust_flush_quota isn't a supported updateable parameter.
        """
        self.cluster.populate(1).start(wait_other_notice=True, wait_for_binary_proto=True)
        node1 = self.cluster.nodelist()[0]

        debug('default auto_adjust_flush_quota is `false`, try to set it to false first')
        self.change_and_verify_config(node1, 'auto_adjust_flush_quota', True, 'false')
        self.change_and_verify_config(node1, 'auto_adjust_flush_quota', False, 'false')

    @require('#5382')
    def test_auto_bootstrap(self):
        """
        auto_adjust_flush_quota isn't a supported updateable parameter.
        """
        self.cluster.populate(1).start(wait_other_notice=True, wait_for_binary_proto=True)
        node1 = self.cluster.nodelist()[0]

        debug('default auto_bootstrap is `true`, try to set it to false first')
        self.change_and_verify_config(node1, 'auto_bootstrap', False, 'true')
        self.change_and_verify_config(node1, 'auto_bootstrap', True, 'true')

    @require('#5384')
    def test_sighup_flood(self):
        self.cluster.populate(1).start(wait_other_notice=True, wait_for_binary_proto=True)
        node1 = self.cluster.nodelist()[0]
        node1.stress(['write', 'n=10000', '-rate', 'threads=8'])
        sighup_num = 10000

        debug('Sending %s SIGHUP signal to scylla process ...' % sighup_num)
        for i in range(sighup_num):
            self.trigger_reload_config(node1)
        self.change_and_verify_config(node1, 'compaction_enforce_min_threshold', True, 'true')
        self.change_and_verify_config(node1, 'compaction_enforce_min_threshold', False, 'false')

    def test_without_config_file(self):
        """
        Test updateable config without config file.
        """
        self.cluster.populate(1).start(wait_other_notice=True, wait_for_binary_proto=True)
        node1 = self.cluster.nodelist()[0]

        config_file_path = os.path.join(node1.get_path(), 'conf/scylla.yaml')
        debug('Rename config file to test updateable config without config file')
        os.rename(config_file_path, '%s.backup' % config_file_path)

        mark = node1.mark_log()
        self.trigger_reload_config(node1)
        err1 = 'Could not read configuration file'
        err2 = 'failed to re-read configuration file: std::invalid_argument'
        node1.watch_log_for(err1, from_mark=mark)
        node1.watch_log_for(err2, from_mark=mark)

        self.check_errors(node1, [err1, err2])

        debug('Recover the config file')
        os.rename('%s.backup' % config_file_path, config_file_path)
        self.change_and_verify_config(node1, 'compaction_enforce_min_threshold', True, 'true')
        self.change_and_verify_config(node1, 'compaction_enforce_min_threshold', False, 'false')
