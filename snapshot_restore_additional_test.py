# coding: utf-8

from dtest import Tester
from unittest import skip


class SnapshotRestoreAdditionalTest(Tester):

    @skip ('unimplemented')
    def restore_snapshot_using_old_schema(self):
        """
        Check that we can restore snapshot files that use old schema
        1. Use a single node and create a keyspace + table
        2. Insert data
        3. Create snapshot and save files
        4. Drop keyspace
        5. Create keyspace + table
        6. Alter table
        7. Restore data
        8. Check that all data exists
        """
        fail
   
    @skip ('unimplemented')
    def restore_snapshot_using_old_token_ownership(self):
        """
        Check that we can restore snapshot files that use a non updated token ownership
        1. Use a single node and create a keyspace + table
        2. Insert data
        3. Create snapshot and save files
        4. Add an additional node
        5. Drop keyspace
        6. Create keyspace + table
        7. Restore data
        8. Check that all data exists
        """
        fail

    @skip ('unimplemented')
    def restore_snapshot_using_different_smp_setting(self):
        """
        Check that we can restore snapshot files that used a different smp setting
        1. Use a single node with smp=1 and create a keyspace + table
        2. Insert data
        3. Create snapshot and save files
        4. Drop keyspace
        5. Stop node, start it with smp=2
        6. Create keyspace + table
        7. Restore data
        8. Check that all data exists
        """
        fail

    @skip ('unimplemented')
    def restore_snapshot_from_cassandra(self):
        """
        Check that we can restore snapshot files that have been created by cassandra
        1. Use a single node and create a keyspace + table
        2. Restore data from a cassandra snapshot
        3. Check that all data exists
        """
        fail

    @skip ('unimplemented')
    def failure_durring_snapshot_no_corrupt_data(self):
        """
        Check that we can restore snapshot files that use old schema
        1. Use a single node and create a keyspace + table
        2. Insert data
        3. Start create snapshot
        4. Kill node
        5. Start node 
        6. Check that all data exists
        """
        fail

    @skip ('unimplemented')
    def failure_durring_restore_no_corrupt_data(self):
        """
        Check that we can restore snapshot files that use old schema
        1. Use a single node and create a keyspace + table
        2. Insert data
        3. Create snapshot and save files
        4. Drop keyspace
        5. Create keyspace + table + populate different data
        6. Start restore data
        7. Kill node
        8. Start node 
        9. Check if non restored data exists
        10. Check if any restored data exists
        11. Restore data
        12. Check that all data exists
        """
        fail

    @skip ('unimplemented')
    def replay_restore_no_additional_data(self):
        """
        Check that we can restore snapshot files that use old schema
        1. Use a single node and create a keyspace + table
        2. Insert data
        3. Create snapshot and save files
        4. Drop keyspace
        5. Create keyspace + table
        6. Restore data
        7. Check that all data exists
        8. Restore data
        9. Check that all data exists
        """
