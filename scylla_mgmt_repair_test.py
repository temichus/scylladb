# coding: utf-8

from dtest import Tester, debug
from unittest import skip

from tools import insert_c1c2, query_c1c2
from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement
from ccmlib.node import NodetoolError
import time
import tempfile
import os
import threading
import random
import uuid

from repair_additional_test import RepairAdditionalBase

class ScyllaMgmtRepairTest(RepairAdditionalBase):
    __test__ = True

    def _repair(self, node, options=[]):
        if len(options) > 1:
           raise Exception(options)

        keyspace = options[0]
        # hack for now till its supported
        cluster =  "test1" #str(uuid.uuid4()).replace("-","") 
        unit = "repair1" # str(uuid.uuid4()).replace("-","") 
        node.cluster.sctool(["cluster","add","--hosts",node.address(),"--name",cluster,"--shard-count","1"])
        node.cluster.sctool(["repair","unit","add","--keyspace",keyspace,"--cluster",cluster,"--name",unit])
        out,err = node.cluster.sctool(["repair","schedule",unit,"--cluster",cluster,'--interval','0','--start-date', 'now'])
        pos = out.find("\n")
        repair_id = out[:pos]
        # currently there is a delay between the schedule and getting the task running so we have this delay
        time.sleep(60)
        out, err = node.cluster.sctool(["repair","progress",repair_id,"--unit",unit,"--cluster",cluster])
        while out.startswith("Status: done") == False:
           time.sleep(1)
           out, err = node.cluster.sctool(["repair","progress",repair_id,"--unit","repair1","--cluster","test1"])
        return out,err

    def repair_disjoint_data_test(self, more_options=[]):
        return RepairAdditionalBase._repair_disjoint_data_test(self,more_options)

    def repair_schema_test(self):
        return RepairAdditionalBase._repair_schema_test(self)

    def repair_schema_2_test(self):
        return RepairAdditionalBase._repair_schema_2_test(self)

    def repair_cell_update_test(self):
       return RepairAdditionalBase._repair_cell_update_test(self)

    def repair_cell_delete_test(self):
       return RepairAdditionalBase._repair_cell_delete_test(self)

    def repair_row_delete_test(self):
       return RepairAdditionalBase._repair_row_delete_test(self)

    def repair_partition_delete_test(self):
       return RepairAdditionalBase._repair_partition_delete_test(self)
