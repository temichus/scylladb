import os
import distutils.dir_util
import shutil
import ssl

from cassandra import ConsistencyLevel
from cassandra.cluster import NoHostAvailable

from dtest import Tester
from tools import generate_ssl_stores, putget, since, safe_mkdtemp
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
            self.patient_cql_connection(node1)
            self.fail('Should not be able to connect to SSL socket without SSL enabled client')
        except NoHostAvailable:
            pass

        assert len(node1.grep_log("(^io.netty.handler.ssl.NotSslRecordException.*|^.*An unexpected TLS packet was received.*|^.*The specified session has been invalidated for some reason.*)")) > 0, \
            "Missing SSL handshake exception while connecting with non-SSL enabled client"

        # enabled ssl on the client and try again (this should work)
        session = self.patient_cql_connection(
            node1, ssl_opts={'ca_certs': os.path.join(self.test_path, 'ccm_node.cer')})
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
            self.patient_cql_connection(node1)
            self.fail('Should not be able to connect to SSL socket without SSL enabled client')
        except NoHostAvailable:
            pass

        assert len(node1.grep_log("(^io.netty.handler.ssl.NotSslRecordException.*|^.*An unexpected TLS packet was received.*|^.*The specified session has been invalidated for some reason.*)")) > 0, \
            "Missing SSL handshake exception while connecting with non-SSL enabled client"

        try:
            # try to connect without auth cert
            self.patient_cql_connection(node1, ssl_opts={'ca_certs': os.path.join(self.test_path, 'ccm_node.cer')})
            self.fail('Should not be able to connect to SSL socket without SSL enabled client')
        except NoHostAvailable:
            pass

        # enabled ssl + auth on the client and try again (this should work)
        session = self.patient_cql_connection(node1, ssl_opts={
            'ca_certs': os.path.join(self.test_path, 'ccm_node.cer'),
            'keyfile': os.path.join(self.test_path, 'ccm_node.key'),
            'certfile': os.path.join(self.test_path, 'ccm_node.pem')
        })
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
            self.patient_cql_connection(node1)
            self.fail('Should not be able to connect to non-default port')
        except NoHostAvailable:
            pass

        session = self.patient_cql_connection(node1, port=9567)
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
        session = self.patient_cql_connection(node1)
        self._putget(cluster, session)

        # connect to additional dedicated ssl port
        session = self.patient_cql_connection(node1, port=9666, ssl_opts={
                                              'ca_certs': os.path.join(self.test_path, 'ccm_node.cer')})
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
            session = self.patient_cql_connection(node1, ssl_opts={'ca_certs': os.path.join(
                self.test_path, 'ccm_node.cer'), "cert_reqs": ssl.CERT_REQUIRED})
            self._putget(cluster, session)
        finally:
            shutil.rmtree(tmpdir)

    def _populateCluster(self, enableSSL=False, nativePort=None, nativePortSSL=None, sslOptional=False, requireAuth=False):
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

        if nativePort:
            cluster.set_configuration_options({
                'native_transport_port': nativePort
            })

        if nativePortSSL:
            cluster.set_configuration_options({
                'native_transport_port_ssl': nativePortSSL
            })

        cluster.populate(1)
        return cluster

    def _putget(self, cluster, session, ks='ks', cf='cf'):
        self.create_ks(session, ks, 1)
        self.create_cf(session, cf, compression=None)
        putget(cluster, session, cl=ConsistencyLevel.ONE)
