# coding: utf-8

import re
import tools

from cql_tests import CQLTester
from dtest import debug

from nose.plugins.attrib import attr


@attr('dtest-full', 'single_node')
class DatabaseMetricsTester(CQLTester):
    """
        Collection of tests related to database metrics stored on Prometheus.
        """

    def get_metrics(self, node_ip, metrics, port='9180', metric_class=None):
        metrics_res = {}
        metric_pattern = re.compile('.*{')
        prometheus_results = self._prometheus_get(node_ip, port).splitlines()
        for metric in prometheus_results:
            for metric_name in metrics:
                if metric_pattern.match(metric) and re.search(f'{metric_name}{{', metric):
                    if metric_class and not re.search(f'class="{metric_class}"', metric):
                        continue
                    name, val = metric.split()
                    metrics_res[name] = val
        return metrics_res

    def total_reads_user_test(self):
        """
        Following scylladb/scylla:0c6bbc8 queries are now classified by its initiator, so here is a small test that aims
        to ensure that when a user runs queries, they will be marked as user initiated (here we are checking
        `scylla_database_total_reads` metric (under class="user")
        """
        session = self.prepare(create_keyspace=False)
        node = self.cluster.nodelist()[0]
        metrics = ['scylla_database_total_reads']
        metric_class = 'user'
        keyspace_name = 'database_metrics'

        initial_reads = self.get_metrics(self.get_ip_from_node(node), metrics=metrics, metric_class=metric_class)

        self.create_ks(session=session, name=keyspace_name, rf=1)
        tools.create_c1c2_table(self, session)
        tools.insert_c1c2(session, n=100)
        res = session.execute('SELECT * FROM cf LIMIT 20')
        debug(res)

        final_reads = self.get_metrics(self.get_ip_from_node(node), metrics=metrics, metric_class=metric_class)

        for metric_name in list(initial_reads.keys()):
            assert final_reads[metric_name] > initial_reads[metric_name],\
                f'{metrics[0]} did not increase as expected. initial={initial_reads}; final={final_reads}'
