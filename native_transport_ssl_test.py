import os
import distutils.dir_util
import shutil
import ssl
import time

from cassandra import ConsistencyLevel
from cassandra.cluster import NoHostAvailable, Cluster
from cassandra.auth import PlainTextAuthProvider

from dtest import Tester, debug, wait_for
from tools import generate_ssl_stores, putget, safe_mkdtemp, require
from scylla_tools import is_port_used
from unittest import skip
from nose.plugins.attrib import attr
from ccmlib import common


def wait_for_cert_reload(node, module, files, from_mark=None):
    for f in files:
        node.watch_log_for("^.*{}.*Reloaded.*{}\.*".format(module, f.replace('.', '\.')), from_mark=from_mark)


@attr('dtest-full', 'single_node')
class NativeTransportSSL(Tester):
    """
    Native transport integration tests, specifically for ssl and port configurations.
    """

    def _create_cluster_session(self, node_to_connect, port=9042, use_ssl=False, ca_certs=None):
        ssl_context, ssl_options, ssl_options = None, None, {}
        if use_ssl or ca_certs:
            ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
        if use_ssl:
            ssl_context.load_cert_chain(certfile=os.path.join(self.test_path, 'ccm_node.pem'),
                                        keyfile=os.path.join(self.test_path, 'ccm_node.key'))
            ssl_options['server_hostname'] = self.get_ip_from_node(node_to_connect)
        if ca_certs:
            ssl_context.verify_mode = ssl.CERT_REQUIRED
            ssl_context.load_verify_locations(cafile=os.path.join(self.test_path, 'ccm_node.cer'))
        cluster_connection = Cluster(
            [self.get_ip_from_node(node_to_connect)],
            port=port,
            connect_timeout=90,
            control_connection_timeout=60,
            protocol_version=4,
            ssl_context=ssl_context,
            ssl_options=ssl_options)
        return cluster_connection.connect()

    @attr('next-gating')
    @attr('dtest-debug')
    def connect_to_ssl_test(self):
        """
        Connecting to SSL enabled native transport port should only be possible using SSL enabled client
        """
        cluster = self._populateCluster(enableSSL=True)
        node1 = cluster.nodelist()[0]

        cluster.start(jvm_args=['--logger-log-level', 'cql_server=debug'])

        try:  # hack around assertRaise's lack of msg parameter
            # try to connect without ssl options
            self._create_cluster_session(node1, use_ssl=False)
            self.fail('Should not be able to connect to SSL socket without SSL enabled client')
        except NoHostAvailable:
            pass

        assert len(node1.grep_log("(^io.netty.handler.ssl.NotSslRecordException.*|^.*An unexpected TLS packet was received.*|^.*The specified session has been invalidated for some reason.*)")) > 0, \
            "Missing SSL handshake exception while connecting with non-SSL enabled client"

        # enabled ssl on the client and try again (this should work)
        session = self._create_cluster_session(node1, use_ssl=True)
        self._putget(cluster, session)

    def connect_to_ssl_test_client_auth(self):
        """
        Connecting to SSL enabled native transport port should only be possible using SSL enabled client
        """

        cluster = self._populateCluster(enableSSL=True, requireAuth=True)
        node1 = cluster.nodelist()[0]

        cluster.start(jvm_args=['--logger-log-level', 'cql_server=debug'])

        try:  # hack around assertRaise's lack of msg parameter
            # try to connect without ssl options
            self._create_cluster_session(node1, use_ssl=False)
            self.fail('Should not be able to connect to SSL socket without SSL enabled client')
        except NoHostAvailable:
            pass

        assert len(node1.grep_log("(^io.netty.handler.ssl.NotSslRecordException.*|^.*An unexpected TLS packet was received.*|^.*The specified session has been invalidated for some reason.*)")) > 0, \
            "Missing SSL handshake exception while connecting with non-SSL enabled client"

        try:
            # try to connect without auth cert
            self._create_cluster_session(node1, use_ssl=False, ca_certs=True)
            self.fail('Should not be able to connect to SSL socket without SSL enabled client')
        except NoHostAvailable:
            pass
        session = self._create_cluster_session(node1, use_ssl=True, ca_certs=True)
        self._putget(cluster, session)

    @skip('optional_ssl')
    def connect_to_ssl_optional_test(self):
        """
        Connecting to SSL optional native transport port must be possible with SSL and non-SSL native clients
        @jira_ticket CASSANDRA-10559
        """
        cluster = self._populateCluster(enableSSL=True, sslOptional=True)
        node1 = cluster.nodelist()[0]

        # try to connect without ssl options
        cluster.start()
        session = self.patient_cql_connection(node1)
        self._putget(cluster, session)

        # enabled ssl on the client and try again (this should work)
        session = self.patient_cql_connection(
            node1, ssl_opts={'ca_certs': os.path.join(self.test_path, 'ccm_node.cer')})
        self._putget(cluster, session, ks='ks2')

    def use_custom_port_test(self):
        """
        Connect to non-default native transport port
        """

        cluster = self._populateCluster(nativePort=9567)
        node1 = cluster.nodelist()[0]

        cluster.start()
        try:  # hack around assertRaise's lack of msg parameter
            self._create_cluster_session(node1, use_ssl=False)
            self.fail('Should not be able to connect to non-default port')
        except NoHostAvailable:
            pass

        session = self._create_cluster_session(node1, port=9567, use_ssl=False)
        self._putget(cluster, session)

    def use_custom_ssl_port_test(self):
        """
        Connect to additional ssl enabled native transport port
        @jira_ticket CASSANDRA-9590
        """

        cluster = self._populateCluster(enableSSL=True, nativePortSSL=9666)
        node1 = cluster.nodelist()[0]
        cluster.start()

        # we should be able to connect to default non-ssl port
        session = self._create_cluster_session(node1, use_ssl=False)
        self._putget(cluster, session)

        # connect to additional dedicated ssl port
        session = self._create_cluster_session(node1, use_ssl=True, port=9666)
        self._putget(cluster, session, ks='ks2')

    @attr('dtest-debug')
    def reload_certificates_test(self):
        """
        Verify certificate reloading on modified file(s)
        """
        cluster = self._populateCluster(enableSSL=True)
        node1 = cluster.nodelist()[0]

        cluster.start(jvm_args=['--logger-log-level', 'cql_server=debug'])

        tmpdir = safe_mkdtemp()
        try:
            # create new certs
            generate_ssl_stores(tmpdir)

            try:  # hack around assertRaise's lack of msg parameter
                # try to connect without new, mismatched cert truststore (and required verification). Should fail
                self.patient_cql_connection(node1, ssl_opts={'ca_certs': os.path.join(
                    tmpdir, 'ccm_node.cer'), "cert_reqs": ssl.CERT_REQUIRED})
                self.fail('Should not be able to connect to SSL socket with mismatched trust store')
            except NoHostAvailable:
                pass

            mark = node1.mark_log()

            # copy new certs to old path
            distutils.dir_util.copy_tree(tmpdir, self.test_path)

            # now we play the waiting game...
            wait_for_cert_reload(node1, "cql_server", ["ccm_node.pem", "ccm_node.key"], from_mark=mark)

            # now we should match
            session = self._create_cluster_session(node1, use_ssl=True)
            self._putget(cluster, session)
        finally:
            shutil.rmtree(tmpdir)

    def _populateCluster(self, enableSSL=False, nativePort=None, nativePortSSL=None, sslOptional=False,
                         requireAuth=False, nodes_num=1):
        cluster = self.cluster

        if enableSSL:
            generate_ssl_stores(self.test_path)
            is_scylla = common.isScylla(cluster.get_install_dir())
            # C* versions before 3.0 (CASSANDRA-10559) do not know about
            # 'client_encryption_options.optional' - so we must not add that parameter
            # Note: does of course not work with scylla, we dont support "optional" (3.x feature)
            options = {'enabled': True}
            if sslOptional:
                options['optional'] = sslOptional
            if is_scylla:
                options.update({
                    'certificate': os.path.join(self.test_path, 'ccm_node.pem'),
                    'keyfile': os.path.join(self.test_path, 'ccm_node.key')
                })
                if requireAuth:
                    options.update({
                        'truststore': os.path.join(self.test_path, 'ccm_node.cer'),
                        'require_client_auth': True
                    })
            else:
                options.update({
                    'keystore': os.path.join(self.test_path, 'keystore.jks'),
                    'keystore_password': 'cassandra',
                })
                if requireAuth:
                    options.update({
                        'truststore': os.path.join(self.test_path, 'truststore.jks'),
                        'truststore_password': 'cassandra',
                        'require_client_auth': True
                    })

            cluster.set_configuration_options({'client_encryption_options': options})

        if nativePort is not None:
            cluster.set_configuration_options({
                'native_transport_port': nativePort
            })

        if nativePortSSL is not None:
            cluster.set_configuration_options({
                'native_transport_port_ssl': nativePortSSL
            })

        cluster.populate(nodes_num)
        return cluster

    def _putget(self, cluster, session, ks='ks', cf='cf'):
        self.create_ks(session, ks, 1)
        self.create_cf(session, cf, compression=None)
        putget(cluster, session, cl=ConsistencyLevel.ONE)

    def disable_regular_port_while_encryption_enabled_test(self):
        """
        This test activates a cluster with encryption turned on, but instead of using the usual native_transport_port
        (9042) the test configures native_transport_port_ssl instead, and disables native_transport_port by configuring
        it to 0. The test makes sure that the cluster responds to session that came through native_transport_port_ssl
        and not native_transport_port.
        """
        cluster = self._populateCluster(enableSSL=True, nativePortSSL=9142, nativePort=0)
        cluster.start()
        node1 = cluster.nodelist()[0]
        session = self._create_cluster_session(node1, use_ssl=True, port=9142)
        self.create_ks(session, "ks", 1)
        is_port_listening = common.check_socket_listening(cluster.get_binary_interface(1), timeout=20)
        assert not is_port_listening, \
            "Even after disabling the default cql port, the cluster continues to listen to it"

    @attr('single_node')
    @require('#7500, #7783')
    def listen_ports_conf_test(self, disable_value=None):
        """
        Test native transport ports configuration, and verify the listening native transport ports after start.
        try to disable the option by setting the option to None, ccm will remove the options from scylla.yaml
        """
        native_port = 9042
        native_port_ssl = 9142
        native_shard_aware_port = 19042
        native_shard_aware_port_ssl = 19142

        # Native_transport_port can only be disabled by `0'
        # Other 3 options can be disabled by removing the option from scylla.yaml, or set it to ~ ,
        # or null in scylla.yaml, ccm only supports to set the option to None, it will remove the
        # option from scylla.yaml
        disable_values = {'native_transport_port': 0,
                          'native_transport_port_ssl': disable_value,
                          'native_shard_aware_transport_port': disable_value,
                          'native_shard_aware_transport_port_ssl': disable_value}

        default_ports_conf = {'native_transport_port': native_port,
                              'native_transport_port_ssl': native_port_ssl,
                              'native_shard_aware_transport_port': native_shard_aware_port,
                              'native_shard_aware_transport_port_ssl': native_shard_aware_port_ssl}

        def restart_and_verify_listen_ports(expected_ports=[native_port, native_shard_aware_port_ssl]):
            """
            Start the node and verify the expected ports are listened, the node will be stop in the end
            """
            debug(f'Expected listen ports: {expected_ports}')
            node1 = cluster.nodelist()[0]
            mark = node1.mark_log()
            node1.start(wait_for_binary_proto=True)

            pattern = '|'.join([str(port) for port in expected_ports])
            res = node1.grep_log(f'Starting listening for CQL clients on.*:({pattern})', from_mark=mark)
            debug(res)
            self.assertEqual(len(res), len(expected_ports),
                             f'The listened ports are not same as expected! '
                             f'Expected ports: {expected_ports}\nReal listened ports: {res}')

            for port in expected_ports:
                # Retry to check if the port can be used in 5 seconds
                wait_for(is_port_used, text=f'Waiting port {port} is used', step=0.5, timeout=2,
                         throw_exc=True, port=port, service_name='Native Transport')

            # Wait a while and check if Aborting/Segfault occurred
            time.sleep(2)
            res = node1.grep_log(f'Aborting on shard |Segmentation fault on shard ', from_mark=mark)
            self.assertEqual(0, len(res), str(res))
            node1.stop(gently=False)

        debug('Only enabled explicitly native SSL port in init cluster')
        cluster = self._populateCluster(enableSSL=True, nativePortSSL=native_port_ssl,
                                        nativePort=native_port, nodes_num=3)
        restart_and_verify_listen_ports(expected_ports=[native_port, native_port_ssl,
                                                        native_shard_aware_port])

        debug(sorted(default_ports_conf.keys()))
        for num in range(2 ** len(default_ports_conf)):
            # Try to cover all cases
            ports_conf = default_ports_conf.copy()
            for idx, key in enumerate(sorted(default_ports_conf.keys())):
                if num & (2 ** idx):  # check if the bit is set
                    ports_conf[key] = disable_values[key]
            debug(f"Test case {num} ({('%4s' % bin(num)[2:]).replace(' ', '0')}):\n"
                  f" {sorted(ports_conf.items(), key=lambda d: d[0])}")
            # cases (9, 10, 11) will fail if disable value is 0
            # cases (13, 15) will fail for if disable_value is None
            cluster.set_configuration_options(ports_conf)
            restart_and_verify_listen_ports(expected_ports=[v for k, v in ports_conf.items() if v not in [0, None]])

    @attr('single_node')
    def listen_ports_conf_by_zero_test(self, disable_value=None):
        """
        Test native transport ports configuration, and verify the listening native transport ports after start.
        Disable 3 options by setting it to `0'
        """
        self.listen_ports_conf_test(disable_value=0)
