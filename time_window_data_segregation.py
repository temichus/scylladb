import cassandra
import cassandra.concurrent
import dtest
import os
import sstable_tools.statistics
import time
import tools


def to_seconds(micros):
    return int(micros/(1000 * 1000))


class TestTimeWindowDataSegregation(dtest.Tester):
    keyspace_name = "ks"
    table_name = "test"

    def _get_time_window_in_seconds(self, statistics_file):
        with open(statistics_file, 'rb') as f:
            data = f.read()

        metadata = sstable_tools.statistics.parse(data, 'mc')
        min_timestamp = metadata['Stats']['min_timestamp']
        max_timestamp = metadata['Stats']['max_timestamp']

        return to_seconds(max_timestamp - min_timestamp)

    def _check_sstable_timestamps(self, node):
        ks_path = os.path.join(node.get_path(), 'data', self.keyspace_name)
        statistics_files = []
        for dirpath, dirnames, filenames in os.walk(ks_path):
            elems = os.path.split(dirpath)
            # We are in the table's dir
            if elems[-1].startswith(self.table_name):
                statistics_files += [os.path.join(dirpath, f) for f in filenames if f.endswith('-Statistics.db')]
                continue

            # prune all dirs that are not a table dir
            for d in dirnames:
                if not d.startswith(self.table_name):
                    dirnames.remove(d)

        self.assertTrue(len(statistics_files) > 0)

        for sf in statistics_files:
            tw = self._get_time_window_in_seconds(sf)
            print("{} => {}".format(sf, tw))
            #self.assertTrue(tw <= 60)


    def test_streaming(self):
        cluster = self.cluster
        cluster.populate(1)
        cluster.start(wait_for_binary_proto=True)
        cluster.set_configuration_options(values={'logger-log-level': 'stream_session=trace'})

        node1 = cluster.nodelist()[0]

        session = self.patient_cql_connection(node1)

        session.execute("CREATE KEYSPACE {} WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}".format(self.keyspace_name))
        session.execute("CREATE TABLE {}.{} (pk int, ck int, v int, PRIMARY KEY(pk, ck))"
                "WITH compaction = {{"
                    "'class': 'TimeWindowCompactionStrategy',"
                    "'compaction_window_unit': 'MINUTES',"
                    "'compaction_window_size': 1}}".format(self.keyspace_name, self.table_name))

        insert_statement = session.prepare("INSERT INTO {}.{} (pk, ck, v) VALUES (?, ?, ?)".format(self.keyspace_name, self.table_name))

        for i in range(2 * 60):
            start = time.time()
            cassandra.concurrent.execute_concurrent_with_args(session, insert_statement, [
                (0, i, 0),
                (1, i, 0),
                (2, i, 0),
                (3, i, 0),
                (4, i, 0),
                (5, i, 0),
                (6, i, 0),
                (7, i, 0),
                (8, i, 0),
                (9, i, 0),
            ])

            if i % 10 == 0:
                node1.flush()

            end = time.time()
            diff = end - start
            if diff < 1.0:
                time.sleep(1.0 - diff)

        self._check_sstable_timestamps(node1)

        node2 = tools.new_node(cluster)
        node2.start(wait_for_binary_proto=True)

        self._check_sstable_timestamps(node2)
