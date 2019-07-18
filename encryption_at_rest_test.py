import os
from dtest import Tester, debug

class InMemoryTest(Tester):
    """
    Test in memory sstable when Encryption at-rest is enabled.
    a reproducer for scylla-enterprise/issues/925
    """
    def restart_query_test(self):
        self.cluster.set_configuration_options(values={'in_memory_storage_size_mb': 100})
        self.cluster.populate(1).start(wait_for_binary_proto=True, wait_other_notice=True)
        node1 = self.cluster.nodelist()[0]
        session = self.patient_cql_connection(node1)
        session.execute("CREATE KEYSPACE rest WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}")
        session.execute("""CREATE TABLE rest.table1(key text PRIMARY KEY, name text) WITH scylla_encryption_options =
            {'key_provider': 'LocalFileSystemKeyProviderFactory', 'secret_key_file': '/tmp/secret_key'} AND
            in_memory=true AND compaction={'class': 'InMemoryCompactionStrategy'}""")

        session.execute("insert into rest.table1 (key, name) values ('key1', 'name1')")
        node1.flush()
        result = session.execute("select * from rest.table1")
        debug(list(result))

        for node in self.cluster.nodelist():
            node.stop()
            node.start(wait_for_binary_proto=True, wait_other_notice=True)
        session = self.patient_cql_connection(node1)
        result = session.execute("select * from rest.table1")
        debug(list(result))

    def workload_without_restart_test(self):
        self.cluster.set_configuration_options(values={'in_memory_storage_size_mb': 100})
        self.cluster.populate(1).start(wait_for_binary_proto=True, wait_other_notice=True)
        node1 = self.cluster.nodelist()[0]
        session = self.patient_cql_connection(node1)
        session.execute("CREATE KEYSPACE keyspace1 WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}")
        session.execute("""CREATE TABLE keyspace1.standard1 (key blob PRIMARY KEY,"C0" blob,"C1" blob,"C2" blob,"C3" blob,"C4" blob)
            WITH scylla_encryption_options = {'key_provider': 'LocalFileSystemKeyProviderFactory', 'secret_key_file': '/tmp/secret_key'} AND
            in_memory=true AND compaction={'class': 'InMemoryCompactionStrategy'} """)
        node1.stress(['write', 'n=100000', 'cl=QUORUM', '-rate', 'threads=8'])
        debug('flushing ...')
        self.cluster.flush()
        node1.stress(['read', 'n=100000', 'cl=QUORUM', '-rate', 'threads=8'])
