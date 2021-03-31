import re
import pytest
import logging

from tools.metrics import prometheus_get
from dtest_class import Tester, get_ip_from_node, create_ks
from tools.data import create_c1c2_table, insert_c1c2


logger = logging.getLogger(__name__)


@pytest.mark.dtest_full
@pytest.mark.single_node
class TestDatabaseMetrics(Tester):
    """
        Collection of tests related to database metrics stored on Prometheus.
    """

    @staticmethod
    def get_metrics(node_ip, metrics, port='9180', metric_class=None):
        metrics_res = {}
        metric_pattern = re.compile('.*{')
        prometheus_results = prometheus_get(node_ip, port).splitlines()
        for metric in prometheus_results:
            for metric_name in metrics:
                if metric_pattern.match(metric) and re.search(f'{metric_name}{{', metric):
                    if metric_class and not re.search(f'class="{metric_class}"', metric):
                        continue
                    name, val = metric.split()
                    metrics_res[name] = val
        return metrics_res

    def test_total_reads_user(self):
        """
        Following scylladb/scylla:0c6bbc8 queries are now classified by its initiator, so here is a small test that aims
        to ensure that when a user runs queries, they will be marked as user initiated (here we are checking
        `scylla_database_total_reads` metric (under class="user")
        """
        self.cluster.populate(1).start(wait_for_binary_proto=True)
        node = self.cluster.nodelist()[0]
        session = self.patient_cql_connection(node)

        metrics = ['scylla_database_total_reads']
        metric_class = 'user'
        keyspace_name = 'database_metrics'

        initial_reads = self.get_metrics(get_ip_from_node(node), metrics=metrics, metric_class=metric_class)

        create_ks(session=session, name=keyspace_name, rf=1)
        create_c1c2_table(session=session)
        insert_c1c2(session=session, ks=keyspace_name, n=100)
        res = session.execute('SELECT * FROM cf LIMIT 20')
        logger.debug(res)

        final_reads = self.get_metrics(get_ip_from_node(node), metrics=metrics, metric_class=metric_class)

        for metric_name in list(initial_reads.keys()):
            assert final_reads[metric_name] > initial_reads[metric_name],\
                f'{metrics[0]} did not increase as expected. initial={initial_reads}; final={final_reads}'
