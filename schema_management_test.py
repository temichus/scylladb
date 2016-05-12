# coding: utf-8

from dtest import Tester
from unittest import skip
from tools import debug
from cassandra import ConsistencyLevel

class SchemaManagementTest(Tester):
    def test_prepared_statements_work_after_node_restart_after_altering_schema_without_changing_columns(self):
        ring_delay_sec = 5
        self.cluster.set_configuration_options(values={
            'ring_delay_ms': ring_delay_sec * 1000,
            'experimental': True})
        self.cluster.populate(3)
        self.cluster.start(wait_other_notice=True)

        [node1, node2, node3] = self.cluster.nodelist()

        session = self.patient_cql_connection(node1)

        debug('Creating schema...')
        self.create_ks(session, 'ks', 3)
        session.execute("""
            CREATE TABLE users (
                id int,
                firstname text,
                lastname text,
                PRIMARY KEY (id)
             );
         """)

        insert_statement = session.prepare("INSERT INTO users (id, firstname, lastname) VALUES (?, 'A', 'B')")
        insert_statement.consistency_level = ConsistencyLevel.ALL
        session.execute(insert_statement, [0])

        debug("Altering schema")
        session.execute("ALTER TABLE users WITH comment = 'updated'")

        debug("Restarting node2")
        node2.stop(gently=True)
        node2.start()

        debug("Restarting node3")
        node3.stop(gently=True)
        node3.start()

        n_partitions = 20
        for i in range(n_partitions):
            session.execute(insert_statement, [i], timeout=1)

        rows = session.execute("SELECT * FROM users")
        res = sorted(rows)
        assert len(res) == n_partitions
        for i in range(n_partitions):
            expected = [i, 'A', 'B']
            assert list(res[i]) == expected, "Expected %s, got %s" % (expected, res[i])

    @skip ('unimplemented')
    def multiple_create_table_in_parallel(self):
        """ 
        Run multiple create table statements via different nodes
        1. Create a cluster of 3 nodes
        2. Run create table with different table names in parallel - check all complete
        3. Run create table with the same table name in parallel - check if they complete
        """
        fail

    @skip ('unimplemented')
    def multiple_alter_table_in_parallel(self):
        """ 
        Run multiple alter table statements via different nodes
        1. Create a cluster of 3 nodes
        2. Run alter table with different table names in parallel - check all complete
        3. Run alter table with the same table name in parallel - check if they complete
        """
        fail

    @skip ('unimplemented')
    def alter_and_drop_table_in_parallel(self):
        """ 
        Run alter and drop table statements via different nodes
        1. Create a cluster of 3 nodes
        2. Run alter and drop table with different table names in parallel - check all complete
        3. Run alter and drop table with the same table name in parallel - check if they complete
        """
        fail

    @skip ('unimplemented')
    def create_table_after_drop_table(self):
        """ 
        Run create table after drop table statements via different nodes
        1. Create a cluster of 3 nodes
        2. Run drop table
        3. Create a table using the same table name
        """
        fail

    @skip ('unimplemented')
    def alter_table_in_parallel_to_write(self):
        """ 
        Create a table and write into while altering the table
        1. Create a cluster of 3 nodes
        2. Run insert statements in a loop
        3. Alter table while inserts are running
        """
        fail

    @skip ('unimplemented')
    def alter_table_in_parallel_to_read(self):
        """ 
        Create a table and populate it and read from it while altering the table
        1. Create a cluster of 3 nodes and populate a table
        2. Run query statements in a loop
        3. Alter table while query are running
        """
        fail

    @skip ('unimplemented')
    def alter_table_in_parallel_to_read_and_write(self):
        """ 
        Create a table and populate it and read from it while altering the table
        1. Create a cluster of 3 nodes and populate a table
        2. Run query statements in a loop
        3. Run insert statements in a loop
        4. Alter table while query and insert are running
        """
        fail

    @skip ('unimplemented')
    def commitlog_replays_after_schema_change(self):
        """
        Commitlog can be replayed even though schema has been changed
        1. Create a table and insert data
        2. Alter table
        3. Kill node
        4. Boot node and verify that commitlog have been replayed and that all data is restored
        """
        fail

    @skip ('unimplemented')
    def create_table_while_node_is_killed(self):
        """ 
        Check that a node that is killed durring a table creation is able to rejoin and to synch on schema
        """
        fail

    @skip ('unimplemented')
    def alter_table_while_node_is_killed(self):
        """ 
        Check that a node that is killed durring a table alter is able to rejoin and to synch on schema
        """
        fail

    @skip ('unimplemented')
    def drop_table_while_node_is_killed(self):
        """ 
        Check that a node that is killed durring a table drop is able to rejoin and to synch on schema
        """
        fail

    @skip ('unimplemented')
    def nodes_rejoining_a_cluster_synch_on_schema(self):
        """
        Nodes rejoining the cluster synch on schema changes
        1. Create a cluster and insert data
        2. Stop a node
        3. Alter table
        4. Insert additional data
        5. Start the stopped node
        6. Verify the stopped node synchs on the updated schema 
        """
        fail

