import logging
from pathlib import Path

import pytest
from cassandra.cluster import Cluster
from ccmlib.utils.sni_proxy import get_cluster_info, refresh_certs, create_cloud_config, start_sni_proxy
from ccmlib.utils.ssl_utils import generate_ssl_stores

from dtest_class import Tester
from tools.marks import required_driver
from tools.stress import assert_cs_success

logger = logging.getLogger(__name__)


@pytest.mark.single_node
@pytest.mark.dtest_full
class TestScyllaCloudBundle(Tester):

    @pytest.fixture(scope='function', autouse=True)
    def fixture_set_cluster_settings(self, fixture_dtest_setup):
        fixture_dtest_setup.cluster.populate(1)
        self.config_data_yaml, self.config_path_yaml = self.start_cluster_with_proxy(fixture_dtest_setup.cluster)

    @staticmethod
    def start_cluster_with_proxy(ccm_cluster):
        cluster_path = Path(ccm_cluster.get_path())
        generate_ssl_stores(cluster_path)
        ssl_port = 9142
        sni_port = 443
        ccm_cluster.set_configuration_options(dict(
            client_encryption_options=dict(require_client_auth=True,
                                           truststore=str(cluster_path / 'ccm_node.cer'),
                                           certificate=str(cluster_path / 'ccm_node.pem'),
                                           keyfile=str(cluster_path / 'ccm_node.key'),
                                           enabled=True),
            native_transport_port_ssl=ssl_port))

        ccm_cluster._update_config()

        ccm_cluster.start(wait_for_binary_proto=True)

        nodes_info = get_cluster_info(ccm_cluster, port=ssl_port)
        refresh_certs(ccm_cluster, nodes_info)

        docker_id, listen_address, listen_port = \
            start_sni_proxy(ccm_cluster.get_path(), nodes_info=nodes_info, listen_port=sni_port)
        ccm_cluster.sni_proxy_docker_ids = [docker_id]
        ccm_cluster.sni_proxy_listen_port = listen_port
        ccm_cluster._update_config()

        config_data_yaml, config_path_yaml = create_cloud_config(ccm_cluster.get_path(),
                                                                 port=listen_port, address=listen_address,
                                                                 nodes_info=nodes_info)
        return config_data_yaml, config_path_yaml

    @required_driver('scylla-driver>=3.24.5')
    def test_connectivity_with_scylla_driver(self):

        cluster = Cluster(scylla_cloud=self.config_data_yaml)
        try:
            with cluster.connect() as session:
                res = session.execute("SELECT * FROM system.local")
                assert res.all()
        finally:
            cluster.shutdown()

    def test_connectivity_with_cqlsh(self):
        node1, *_ = self.cluster.nodelist()
        res = node1.run_cqlsh(cmds='SELECT * FROM system.peers ;',
                              cqlsh_options=['--cloudconf', self.config_data_yaml],
                              return_output=True, show_output=True)

        assert not res[1], f"cqlsh command failed:\n\n{res[1]}"

    def test_connectivity_with_cassandra_stress(self):
        node1, *_ = self.cluster.nodelist()
        output = node1.stress(['write', 'duration=5s', "no-warmup", '-rate',
                              'threads=2', '-cloudconf', f'file={self.config_data_yaml}'])
        logger.debug(output.stdout)
        logger.debug(output.stderr)
        assert_cs_success(output)
