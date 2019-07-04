from nose.tools import assert_equal
from dtest import Tester, debug
import datetime
import time
import random
import subprocess


class TestBypassCache(Tester):
    '''
    Test that will verify if the select statement will skip cache during its read
    Introduced by commit 2a371c2689d327a10f5888f39414ec66efedb093
    '''
    def prepare(self, nodes=1, keyspace_name='bypass_cache', rf=1, options_dict=None, table_name='user_events'):
        self.keyspace_name = keyspace_name
        self.table_name = table_name
        cluster = self.cluster
        if options_dict:
            cluster.set_configuration_options(values=options_dict)
        cluster.populate(nodes).start()
        node1 = cluster.nodelist()[0]
        session = self.patient_cql_connection(node1)
        self.create_ks(session=session, name=keyspace_name, rf=rf)

        self.create_table(session=session)
        self.populate_table(session=session)

        return session

    def create_table(self, session):
        session.execute('CREATE TABLE user_events (userid text, event timestamp, value text, '
                        'PRIMARY KEY (userid, event))')

    def populate_table(self, session):
        partitions = random.randint(1, 50)
        lines = random.randint(1, 1000)
        debug('Adding {} lines with {} partitions'.format(lines, partitions))

        for partition in xrange(0, partitions):
            for line in xrange(0, lines):
                session.execute("INSERT INTO user_events (userid, event, value)"
                                "VALUES ('{}', '{}', '{}')".format('user{}'.format(partition),
                                                                   str(datetime.datetime.now())[:-7],
                                                                   random.random()))

    def count_disk_ios(self):
        cmd = 'cat /sys/block/md0/stat'
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = process.communicate()

        return stdout

    def restart_scylla(self, node):
        node.stop()
        time.sleep(1)
        node.start()

    def get_scylla_cache_reads_metrics(self, node):
        return self.get_node_metrics(self.get_ip_from_node(node), metrics=['scylla_cache_reads'])

    def verify_read_was_from_disk(self, node, query, session):
        cache_read_before_bypass_read = self.get_scylla_cache_reads_metrics(node=node)
        session.execute(query)
        cache_read_after_bypass_read = self.get_scylla_cache_reads_metrics(node=node)

        assert_equal(cache_read_before_bypass_read, cache_read_after_bypass_read)

    def test_simple_bypass_cache(self):
        session = self.prepare()
        node = self.cluster.nodelist()[0]
        query = 'SELECT * FROM user_events BYPASS CACHE'

        self.verify_read_was_from_disk(node=node, query=query, session=session)
