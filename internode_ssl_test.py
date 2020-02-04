from dtest import Tester, debug
from tools import generate_ssl_stores, putget
from nose.plugins.attrib import attr

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

    def __putget_with_internode_ssl_test(self, internode_compression, internode_encryption='all', dcs=1):
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
            cluster.populate([3 for i in range(dcs)]).start(no_wait=False, wait_for_binary_proto=True, wait_other_notice=True)
        else:
            raise Exception('Invalid parameter dc: %s' % dc)

        session = self.patient_cql_connection(cluster.nodelist()[0])
        self.create_ks(session, 'ks', 3)
        self.create_cf(session, 'cf', compression=None)
        putget(cluster, session)
