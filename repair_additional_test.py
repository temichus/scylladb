# coding: utf-8

from dtest import Tester
from unittest import skip


class RepairAdditionalTest(Tester):

    @skip ('unimplemented')
    def full_repair_of_node(self):
        """ 
        Check that repair transfers all the data in case non exists
        1. Create a cluster of 2 nodes with rf=2, disable read_repair, hintted_handoff
        2. Shutdown node 2
        3. Insert data
        4. Start node 2
        5. Run repair on node 2
        6. Shutdown node 1 - check that all data exists
        """
        fail

    @skip ('unimplemented') 
    def repair_fixes_updates_to_cells(self):
        """ 
        Check that repair fixes a few update to cells contents
        1. Create a cluster of 2 nodes with rf=2, disable read_repair, hinttef_handoff
        2. Insert data
        3. Shutdown node 2
        4. Update some cells
        5. Start node 2
        6. Run repair on node 2
        7. Shutdown node 1 - check that all data exists
        """
        fail

    @skip ('unimplemented') 
    def repair_fixes_remove_of_keys(self):
        """ 
        Check that repair fixes a few removed keys
        1. Create a cluster of 2 nodes with rf=2, disable read_repair, hinttef_handoff
        2. Insert data
        3. Shutdown node 2
        4. Remove some keys
        5. Start node 2
        6. Run repair on node 2
        7. Shutdown node 1 - check that all data exists
        """
        fail

    @skip ('unimplemented') 
    def repair_fixes_remove_of_range_of_keys(self):
        """ 
        Check that repair fixes a delete of a range key
        1. Create a cluster of 2 nodes with rf=2, disable read_repair, hinttef_handoff
        2. Insert data
        3. Shutdown node 2
        4. Remove a range of some keys
        5. Start node 2
        6. Run repair on node 2
        7. Shutdown node 1 - check that all data exists
        """
        fail

    @skip ('unimplemented') 
    def repair_fixes_deletion_of_cells(self):
        """ 
        Check that repair fixes a deleteion of cells 
        1. Create a cluster of 2 nodes with rf=2, disable read_repair, hinttef_handoff
        2. Insert data
        3. Shutdown node 2
        4. Delete some cells
        5. Start node 2
        6. Run repair on node 2
        7. Shutdown node 1 - check that all data exists
        """
        fail

    @skip ('unimplemented') 
    def repair_fixes_deletion_of_range_of_cells(self):
        """ 
        Check that repair fixes a deletion of cell range
        1. Create a cluster of 2 nodes with rf=2, disable read_repair, hinttef_handoff
        2. Insert data
        3. Shutdown node 2
        4. Delete a range of some cells
        5. Start node 2
        6. Run repair on node 2
        7. Shutdown node 1 - check that all data exists
        """
        fail

    @skip ('unimplemented') 
    def repair_fixes_update_of_ttl(self):
        """ 
        Check that repair fixes updates to ttl
        1. Create a cluster of 2 nodes with rf=2, disable read_repair, hinttef_handoff
        2. Insert data
        3. Shutdown node 2
        4. Update ttl of some cells
        5. Start node 2
        6. Run repair on node 2
        7. Shutdown node 1 - check that all data exists
        """
        fail

    @skip ('unimplemented')
    def fail_node_initiating_repair(self):
        """ 
        Check that killing a repaired node does not cause additional failures
        1. Create a cluster of 2 nodes with rf=2
        2. Stop node 2
        3. Insert data 
        4. Start node 2 
        5. Start repair
        6. Kill node 2
        7. Check that cluster is avilable (read/writes)
        """
        fail

    @skip ('unimplemented')
    def fail_node_responding_to_repair(self):
        """ 
        Check that killing a repairing node does not cause additional failures
        1. Create a cluster of 2 nodes with rf=2
        2. Stop node 2
        3. Insert data 
        4. Start node 2 
        5. Start repair
        6. Kill node 1
        7. Check that cluster is avilable (read/writes)
        """
        fail

    @skip ('unimplemented')
    def repair_while_data_is_updated(self):
        """ 
        Check that killing a repaired node does not cause additional failures
        1. Create a cluster of 2 nodes with rf=2
        2. Stop node 2
        3. Insert data 
        4. Start node 2 
        5. In a loop update part of data with CL=2 
        6. Start repair
        7. Stop node 1
        8. Check that all the data is p to date
        """
        fail

    @skip ('unimplemented')
    def repair_while_nodes_are_down_1(self):
        """ 
        Check that repair is not able to complete if no replicas for data exist
        1. Create a cluster of 3 nodes with rf=2
        2. Stop node 2
        3. Insert data 
        4. Stop node 3
        5. Start node 2 
        6. Start repair on node 2
        7. Check if repair is succesfull
        """
        fail

    @skip ('unimplemented')
    def repair_while_nodes_are_down_2(self):
        """ 
        Check that repair is able to complete if one replica of data exists
        1. Create a cluster of 4 nodes with rf=3
        2. Stop node 2
        3. Insert data 
        4. Stop node 3
        5. Start node 2 
        6. Start repair on node 2
        7. Check if repair is succesfull
        """
        fail

    @skip ('unimplemented')
    def repair_while_new_node_is_added(self):
        """ 
        Check that repair is accompileshed while new node is added 
        1. Create a cluster of 2 nodes with rf=2
        2. Stop node 2
        3. Insert data 
        4. Start node 2 
        6. Start repair
        7. Create a new node and start it
        """
        fail

    @skip ('unimplemented')
    def repair_while_node_is_decomissioned(self):
        """ 
        Check that repair is accompileshed while node is removed 
        1. Create a cluster of 3 nodes with rf=2
        2. Stop node 2
        3. Insert data 
        4. Start node 2 
        6. Start repair
        7. Decomission node 3
        8. Stop node 1 - does node 2 hold all the data 
        """
        fail

    @skip ('unimplemented')
    def test_multiple_repair(self):
        """
        Check that repair is accompileshed while node is removed 
        1. Create a cluster of 3 nodes with rf=3
        1. Insert data
        2. Stop node 2
        3. Insert data 
        2. Stop node 3
        3. Insert data 
        4. Start node 2, Start node 3 
        6. Start repair on node 2, node 3
        8. Stop node 1,node 3 - does node 2 hold all the data 
        8. Stop node 1,node 2 - does node 3 hold all the data 
        """
        fail
