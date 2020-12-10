import os
import time

from dtest import Tester, debug, wait_for
from tools import generate_ssl_stores, putget, require
from scylla_tools import is_port_used
from nose.plugins.attrib import attr
from native_transport_ssl_test import wait_for_cert_reload


@attr('next-gating')
@attr('dtest-debug')
@attr('dtest-full')
class TestInternodeSSL(Tester):

    def __init__(self, *args, **kwargs):
        Tester.__init__(self, *args, **kwargs)

    def putget_with_internode_ssl_test(self):
        """
        Simple putget test with internode ssl enabled
        with default 'all' internode compression
        @jira_ticket CASSANDRA-9884
        """
        self.__putget_with_internode_ssl_test('all', internode_encryption='all')

    def putget_with_internode_rack_ssl_test(self):
        """
        Simple putget test with internode ssl enabled
        with default 'all' internode compression and 'rack' internode encryption.
        """
        self.__putget_with_internode_ssl_test('all', internode_encryption='rack')

    def putget_with_internode_ssl_without_compression_test(self):
        """
        Simple putget test with internode ssl enabled
        without internode compression
        @jira_ticket CASSANDRA-9884
        """
        self.__putget_with_internode_ssl_test('none', internode_encryption='none')

    def putget_with_internode_ssl_with_dc_compression_test(self):
        """
        Simple putget test with internode ssl enabled
        with 'dc' internode compression and 'dc' internode encryption.
        """
        self.__putget_with_internode_ssl_test('dc', internode_encryption='dc', dcs=2)

    def putget_with_internode_rack_ssl_with_dc_compression_test(self):
        """
        Simple putget test with internode ssl enabled
        with 'dc' internode compression and 'rack' internode encryption.
        """
        self.__putget_with_internode_ssl_test('dc', internode_encryption='rack', dcs=2)

    def putget_with_reloaded_certificates_test(self):
        self.__putget_with_internode_ssl_test('all', internode_encryption='all', reload_certs=True)

    def __putget_with_internode_ssl_test(self, internode_compression, internode_encryption='all', dcs=1, reload_certs=False):
        cluster = self.cluster

        debug("***using internode ssl***")
        generate_ssl_stores(self.test_path)
        cluster.set_configuration_options({'internode_compression': internode_compression})
        cluster.enable_internode_ssl(self.test_path, internode_encryption=internode_encryption)

        if dcs == 1:
            cluster.populate(3).start(wait_for_binary_proto=True, wait_other_notice=True)
        elif dcs > 1:
            cluster.set_configuration_options(values={'endpoint_snitch':
                                                      'org.apache.cassandra.locator.GossipingPropertyFileSnitch'})
            cluster.populate([3 for i in range(dcs)]).start(
                no_wait=False, wait_for_binary_proto=True, wait_other_notice=True)
        else:
            raise Exception('Invalid parameter dcs: {}. Must be greater than or equal to 1'.format(dcs))

        self.ignore_log_patterns += [
            'connection dropped: The TLS connection was non-properly terminated',
            'connection dropped: The certificate is NOT trusted',
            'connection dropped: sendmsg: Broken pipe',
            'connection dropped: The specified session has been invalidated for some reason',
            'storage_service -.*fail to update tokens for',
        ]

        if reload_certs:
            debug("rewriting certs")

            node_marks = {node: node.mark_log() for node in cluster.nodelist()}

            os.remove(os.path.join(self.test_path, 'keystore.jks'))
            os.remove(os.path.join(self.test_path, 'truststore.jks'))
            mtime = os.path.getmtime(os.path.join(self.test_path, 'ccm_node.key'))
            # overwrite old certs
            generate_ssl_stores(self.test_path)

            mtime2 = os.path.getmtime(os.path.join(self.test_path, 'ccm_node.key'))
            self.assertGreater(mtime2, mtime, "Cert regen failed?")

            cluster.enable_internode_ssl(self.test_path, internode_encryption=internode_encryption)

            for node, mark in node_marks.items():
                debug("waiting for {} to reload certs".format(node.get_path()))
                wait_for_cert_reload(node, "messaging_service", [
                                     "internode-ccm_node.pem", "internode-ccm_node.key"], from_mark=mark)
                debug("done")

        session = self.patient_cql_connection(cluster.nodelist()[0])
        self.create_ks(session, 'ks', 3)
        self.create_cf(session, 'cf', compression=None)
        putget(cluster, session)

    @attr('single_node')
    @require('#7783')
    def listen_ports_conf_test(self, DISABLE_VALUE=0):
        """
        Test storage ports configuration, and verify the listening storage ports after start
        """
        generate_ssl_stores(self.test_path)
        cluster = self.cluster
        cluster.populate(3)
        cluster.set_configuration_options({'internode_compression': 'all'})
        cluster.enable_internode_ssl(self.test_path, internode_encryption='all')

        storage_port = 7000
        ssl_storage_port = 7001

        def restart_and_verify_listen_ports(expected_ports=[storage_port, ssl_storage_port]):
            """
            Start the node and verify the expected ports are listened,  the node will be stop in the end
            """
            debug(f'Expected listen ports: {expected_ports}')
            node1 = self.cluster.nodelist()[0]
            mark = node1.mark_log()
            node1.start(wait_for_binary_proto=True)
            pattern = '|'.join([str(port) for port in expected_ports])
            res = node1.grep_log(f'Starting.*Messaging Service on.*port ({pattern})', from_mark=mark)
            debug(res)
            self.assertEqual(len(res), len(expected_ports),
                             f'The listened ports are not same as expected! '
                             f'Expected ports: {expected_ports}\nReal listened ports: {res}')

            for port in expected_ports:
                # Retry to check if the port can be used in 5 seconds
                wait_for(is_port_used, text=f'Waiting port {port} is used', step=0.5, timeout=2,
                         throw_exc=True, port=port, service_name='Storage Service')

            # Wait a while and check if Aborting/Segfault occurred
            time.sleep(2)
            res = node1.grep_log(f'Aborting on shard |Segmentation fault on shard ', from_mark=mark)
            self.assertEqual(0, len(res), str(res))
            node1.stop(gently=False)

        # Test default configuration, ccm only sets storage_port: 7000
        restart_and_verify_listen_ports(expected_ports=[storage_port])

        # Enable both option
        cluster.set_configuration_options({'storage_port': storage_port,
                                           'ssl_storage_port': ssl_storage_port})
        restart_and_verify_listen_ports(expected_ports=[storage_port, ssl_storage_port])

        # Disable storage_port by setting it to `DISABLE_VALUE'
        cluster.set_configuration_options({'storage_port': DISABLE_VALUE,
                                           'ssl_storage_port': ssl_storage_port})
        restart_and_verify_listen_ports(expected_ports=[ssl_storage_port])

        # Disable ssl_storage_port by setting it to `DISABLE_VALUE'
        cluster.set_configuration_options({'storage_port': storage_port,
                                           'ssl_storage_port': DISABLE_VALUE})
        restart_and_verify_listen_ports(expected_ports=[storage_port])

        # Disable both ports by setting it to `DISABLE_VALUE'
        cluster.set_configuration_options({'storage_port': DISABLE_VALUE,
                                           'ssl_storage_port': DISABLE_VALUE})
        restart_and_verify_listen_ports(expected_ports=[])

    @attr('single_node')
    @require('#7500')
    def listen_ports_conf_by_none_test(self):
        """
        Test storage ports configuration, and verify the listening storage ports after start,
        Try to disable the option by setting the option to None, ccm will remove this option from scylla.yaml
        """
        self.listen_ports_conf_test(DISABLE_VALUE=None)
