import re
import pytest
import logging
import math
import cassandra.concurrent

from tools.metrics import prometheus_get
from dtest_class import Tester, get_ip_from_node, create_ks
from tools.data import create_c1c2_table, insert_c1c2


logger = logging.getLogger(__name__)


@pytest.mark.dtest_full
@pytest.mark.single_node
@pytest.mark.next_gating
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

    def _do_run(self, node, read_func):
        metrics = ['scylla_database_total_reads']
        metric_class = 'user'

        initial_reads = self.get_metrics(get_ip_from_node(node), metrics=metrics, metric_class=metric_class)

        total_count = read_func()

        final_reads = self.get_metrics(get_ip_from_node(node), metrics=metrics, metric_class=metric_class)

        total = 0
        for metric_name in list(initial_reads.keys()):
            added = int(final_reads[metric_name])-int(initial_reads[metric_name])
            total += added
            logger.debug(f"final_reads[{metric_name}]={final_reads[metric_name]} " +
                         f"initial_reads[{metric_name}]={initial_reads[metric_name]} " +
                         f"({added} added)")
        min_count = total_count
        max_count = total_count * 1.1
        assert min_count <= total <= max_count, \
            f"Expected additional reads to be in the [{min_count}, " \
            f"{max_count}] range, but metrics show {total} reads"

    def test_total_reads_user(self):
        """
        Following scylladb/scylla:0c6bbc8 queries are now classified by its initiator, so here is a small test that aims
        to ensure that when a user runs queries, they will be marked as user initiated (here we are checking
        `scylla_database_total_reads` metric (under class="user")
        """
        self.cluster.populate(1).start(wait_for_binary_proto=True)
        node = self.cluster.nodelist()[0]
        session = self.patient_cql_connection(node)

        keyspace_name = 'database_metrics'
        create_ks(session=session, name=keyspace_name, rf=1)
        create_c1c2_table(session=session)

        count = 100
        keys = range(count)
        insert_c1c2(session=session, ks=keyspace_name, keys=keys)

        select_stmt = session.prepare("SELECT * FROM cf WHERE key=?")

        reads_per_key = 16
        params = []
        for k in keys:
            params += [(str(k),) for _ in range(reads_per_key)]

        def read_func():
            cassandra.concurrent.execute_concurrent_with_args(session, select_stmt, params, concurrency=32)
            return count * reads_per_key

        self._do_run(node, read_func)

    def test_total_reads_system(self):
        """
        Same principle as test_total_reads_user, but read a system table instead,
        and check that the reads are still classified as "user" reads.
        """
        self.cluster.populate(1).start(wait_for_binary_proto=True)
        node = self.cluster.nodelist()[0]
        session = self.patient_cql_connection(node)

        all_rows = list(session.execute("SELECT keyspace_name, table_name, column_name FROM system_schema.columns"))

        params = []
        for _ in range(math.ceil(1000/len(all_rows))):
            for r in all_rows:
                params.append((r.keyspace_name, r.table_name, r.column_name))

        select_stmt = session.prepare(
            "SELECT * FROM system_schema.columns WHERE keyspace_name=? AND table_name=? AND column_name=?")

        def read_func():
            cassandra.concurrent.execute_concurrent_with_args(session, select_stmt, params, concurrency=32)
            return len(params)

        self._do_run(node, read_func)
