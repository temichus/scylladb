import time
import re
from datetime import datetime, timedelta, date
from typing import Tuple
from enum import IntEnum
from decimal import Decimal

from nose.plugins.attrib import attr

from cassandra.cluster import Session, SimpleStatement
from uuid import UUID, uuid1
from cassandra import ConsistencyLevel
from cassandra.util import uuid_from_time, datetime_from_uuid1, Time, OrderedMapSerializedKey
from dtest import Tester, debug, wait_for
from ccmlib.scylla_cluster import ScyllaNode
from tools import require

from cdc_tests import CdcLogOperations, CDCInitializeHelper


class CdcTools(Tester, CDCInitializeHelper):
    keyspace = "ks"
    table = "cf"
    table_cdc_log = f"{table}_scylla_cdc_log"

    def prepare_cluster_and_schema(self, num_nodes=1, rf=1,
                                   preimage_enable=False,
                                   postimage_enable=False,
                                   primary_key_type=None):
        self.cluster.populate(num_nodes)
        self.cluster.set_configuration_options(values={"experimental_features": ["cdc"]})
        self.cluster.start(wait_for_binary_proto=True, wait_other_notice=True)
        node = self.cluster.nodelist()[0]
        session = self.patient_cql_connection(node)
        self.create_schema_with_cdc(session, rf=rf,
                                    preimage_enable=preimage_enable,
                                    postimage_enable=postimage_enable,
                                    primary_key_type=primary_key_type)

        self.wait_for_last_generation_to_be_active(session)
        self.wait_for_metadata_update(session, cluster_size=num_nodes)
        return (node, session)

    def create_schema_with_cdc(self, session, rf=1, preimage_enable=False, postimage_enable=False, primary_key_type=None):
        key_type = primary_key_type if primary_key_type else self.columns_data['cl_type']
        statement = f"CREATE TABLE {self.keyspace}.{self.table} \
                    (pkey {key_type}, \
                     ckey {key_type}, \
                     value {self.columns_data['cl_type']}, \
                     PRIMARY KEY (pkey, ckey)\
                    )"
        statement += " WITH cdc={'enabled': true"
        if preimage_enable:
            statement += ", 'preimage': true"
        if postimage_enable:
            statement += ", 'postimage': true"
        statement += "}"
        session.execute("ALTER keyspace system_distributed with replication={'class': 'SimpleStrategy', 'replication_factor': '1'}")
        self.create_ks(session, self.keyspace, rf=rf)
        session.execute(statement)

    def insert_one(self, session, data):
        timestamp = int(time.time() * 1000000)
        stm = SimpleStatement(f"INSERT INTO {self.keyspace}.{self.table} (pkey, ckey, value) VALUES (%(pkey)s, %(ckey)s, %(value)s) USING TIMESTAMP {timestamp}")
        session.execute(stm, data)
        return timestamp

    def update_one(self, session, data):
        timestamp = int(time.time() * 1000000)
        stm = SimpleStatement(f"UPDATE {self.keyspace}.{self.table} USING TIMESTAMP {timestamp} SET value = %(value)s WHERE pkey=%(pkey)s and ckey=%(ckey)s")
        session.execute(stm, data)
        return timestamp

    def update_collection_with_element(self, session, data, add=True):
        timestamp = int(time.time() * 1000000)
        if not add:
            stm = SimpleStatement(f"UPDATE {self.keyspace}.{self.table} USING TIMESTAMP {timestamp} SET value = value - %(value)s WHERE pkey=%(pkey)s and ckey=%(ckey)s")
        else:
            stm = SimpleStatement(f"UPDATE {self.keyspace}.{self.table} USING TIMESTAMP {timestamp} SET value = value + %(value)s WHERE pkey=%(pkey)s and ckey=%(ckey)s")

        session.execute(stm, data)
        return timestamp

    def delete_one(self, session, data):
        timestamp = int(time.time() * 1000000)
        stm = SimpleStatement(f"DELETE value FROM {self.keyspace}.{self.table} USING TIMESTAMP {timestamp} WHERE pkey=%(pkey)s and ckey=%(ckey)s")
        session.execute(stm, data)
        return timestamp

    def get_cdc_log_records_by_timestamp(self, session, timestamp):

        res_cdc_log = list(session.execute(f"SELECT * FROM {self.keyspace}.{self.table_cdc_log} \
                                           WHERE \"cdc$time\" >= minTimeuuid({int(timestamp/1000)}) \
                                             AND \"cdc$time\" <= maxTimeuuid({int(timestamp/1000)}) ALLOW FILTERING"))
        return res_cdc_log

    def get_all_cdc_log_records(self, session):
        res_cdc_log = list(session.execute(f"SELECT * FROM {self.keyspace}.{self.table_cdc_log}"))
        return res_cdc_log

    def get_all_base_records(self, session):
        res_cdc_log = list(session.execute(f"SELECT * FROM {self.keyspace}.{self.table}"))
        return res_cdc_log

    def check_cdc_base_field_values(self, row, expected_field_values):
        for key in expected_field_values:
            cdc_field_value = getattr(row, key)
            self.assertEqual(cdc_field_value, expected_field_values[key])

    def check_cdc_base_collection_values(self, row, expected_field_values):
        cf_value = expected_field_values.pop("value")
        for key in expected_field_values:
            cdc_field_value = getattr(row, key)
            self.assertEqual(cdc_field_value, expected_field_values[key])

        if not cf_value:
            self.assertFalse(row.value)
        elif isinstance(row.value, OrderedMapSerializedKey):
            if isinstance(cf_value, list):
                for _, value in row.value.items():
                    self.assertIn(value, cf_value)
            if isinstance(cf_value, dict):
                for key, value in row.value.items():
                    self.assertIn(key, cf_value.keys())
                    self.assertIn(value, cf_value.values())
        else:
            self.assertSetEqual(row.value, cf_value)

    def check_cdc_rec_timestamp(self, rows, timestamp):
        pass

    def check_cdc_log_num_row(self, cdc_log_results, expected_num_rows):
        self.assertEqual(len(cdc_log_results), expected_num_rows)

    def check_cdc_log_row(self, row, operation, batch_seq, expected_data, deleted_col=None):
        self.assertEqual(row.cdc_operation, operation)
        self.assertEqual(row.cdc_batch_seq_no, batch_seq)
        if deleted_col:
            self.check_cdc_deleted_columns(row, deleted_col)
        self.check_cdc_base_field_values(row, expected_data)

    def check_cdc_log_row_collection(self, row, operation, batch_seq, expected_data, deleted_col=None, deleted_keys=None):
        self.assertEqual(row.cdc_operation, operation)
        self.assertEqual(row.cdc_batch_seq_no, batch_seq)
        if deleted_col:
            self.check_cdc_deleted_columns(row, deleted_col)
        if deleted_keys:
            self.check_cdc_deleted_elements(row, deleted_keys)
        self.check_cdc_base_collection_values(row, expected_data)

    def check_cdc_deleted_columns(self, row, deleted_columns):
        for col in deleted_columns:
            self.assertTrue(getattr(row, f"cdc_deleted_{col}"))

    def check_cdc_deleted_elements(self, row, deleted_elements):
        if isinstance(deleted_elements, list):
            self.assertTrue(row.cdc_deleted_elements_value)
        else:
            for key in deleted_elements:
                self.assertIn(key, row.cdc_deleted_elements_value)


@attr('single_node', 'scylla-cdc')
class CDCNativeTypeTmpl(CdcTools):

    columns_data = None
    __test__ = False

    @property
    def inserted_dataset(self):
        return {"pkey": self.columns_data["ins_dataset"],
                "ckey": self.columns_data["ins_dataset"],
                "value": self.columns_data["ins_dataset"]}

    @property
    def updated_dataset(self):
        return {"pkey": self.columns_data["ins_dataset"],
                "ckey": self.columns_data["ins_dataset"],
                "value": self.columns_data["upd_dataset"]}

    @property
    def updated_with_null_dataset(self):
        return {"pkey": self.columns_data["ins_dataset"],
                "ckey": self.columns_data["ins_dataset"],
                "value": None}

    @property
    def deleted_dataset(self):
        return {"pkey": self.columns_data["ins_dataset"],
                "ckey": self.columns_data["ins_dataset"]}

    def test_native_type_insert(self):
        self.insert_operation_tmpl()

    def test_native_type_insert_with_preimage(self):
        self.insert_operation_tmpl(preimage_enable=True)

    def test_native_type_insert_with_postimage(self):
        self.insert_operation_tmpl(postimage_enable=True)

    def test_native_type_insert_with_preimage_postimage(self):
        self.insert_operation_tmpl(preimage_enable=True, postimage_enable=True)

    def test_natitve_type_update(self):
        self.update_operation_tmpl()

    def test_natitve_type_update_with_preimage(self):
        self.update_operation_tmpl(preimage_enable=True)

    def test_natitve_type_update_with_postimage(self):
        self.update_operation_tmpl(postimage_enable=True)

    def test_natitve_type_update_with_preimage_postimage(self):
        self.update_operation_tmpl(preimage_enable=True, postimage_enable=True)

    def test_update_with_null(self):
        self.update_with_null()

    def test_update_with_null_with_preimage(self):
        self.update_with_null(preimage_enable=True)

    def test_update_with_null_with_postimage(self):
        self.update_with_null(postimage_enable=True)

    def test_update_with_null_with_preimage_postimage(self):
        self.update_with_null(preimage_enable=True, postimage_enable=True)

    def test_native_type_delete(self):
        self.delete_operation_tmpl()

    def test_native_type_delete_with_preimage(self):
        self.delete_operation_tmpl(preimage_enable=True)

    def test_native_type_delete_with_postimage(self):
        self.delete_operation_tmpl(postimage_enable=True)

    def test_native_type_delete_with_preimage_postimage(self):
        self.delete_operation_tmpl(preimage_enable=True, postimage_enable=True)

    def test_all_operation(self):
        self.all_operation_tmpl()

    def test_all_operation_with_preimage(self):
        self.all_operation_tmpl(preimage_enable=True)

    def test_all_operation_with_postimage(self):

        self.all_operation_tmpl(postimage_enable=True)

    def test_all_operation_with_preimage_postimage(self):
        self.all_operation_tmpl(preimage_enable=True, postimage_enable=True)

    def insert_operation_tmpl(self, preimage_enable=False, postimage_enable=False):
        node, session = self.prepare_cluster_and_schema(preimage_enable=preimage_enable, postimage_enable=postimage_enable)
        # insert first record
        timestamp = self.insert_one(session, self.inserted_dataset)
        cdc_log_data = self.get_cdc_log_records_by_timestamp(session, timestamp)
        self.verify_cdc_log_rows_after_first_operation(cdc_log_data, CdcLogOperations.INSERT, postimage_enable)

        timestamp = self.insert_one(session, self.inserted_dataset)
        cdc_log_data = self.get_cdc_log_records_by_timestamp(session, timestamp)
        self.verify_cdc_log_rows_after_operation(cdc_log_data,
                                                 operation=CdcLogOperations.INSERT,
                                                 preimage_enable=preimage_enable,
                                                 postimage_enable=postimage_enable,
                                                 expected_dataset=self.inserted_dataset)

    def update_operation_tmpl(self, preimage_enable=False, postimage_enable=False):
        node, session = self.prepare_cluster_and_schema(preimage_enable=preimage_enable, postimage_enable=postimage_enable)
        # insert first record
        timestamp = self.update_one(session, self.inserted_dataset)
        cdc_log_data = self.get_cdc_log_records_by_timestamp(session, timestamp)
        self.verify_cdc_log_rows_after_first_operation(cdc_log_data, CdcLogOperations.UPDATE, postimage_enable)
        # will contain 2 records
        timestamp = self.update_one(session, self.updated_dataset)
        cdc_log_data = self.get_cdc_log_records_by_timestamp(session, timestamp)
        # first record is the same
        self.verify_cdc_log_rows_after_operation(cdc_log_data,
                                                 operation=CdcLogOperations.UPDATE,
                                                 preimage_enable=preimage_enable,
                                                 postimage_enable=postimage_enable,
                                                 expected_dataset=self.updated_dataset)

    def update_with_null(self, preimage_enable=False, postimage_enable=False):
        node, session = self.prepare_cluster_and_schema(preimage_enable=preimage_enable, postimage_enable=postimage_enable)
        # insert first record
        self.update_one(session, self.inserted_dataset)
        # sleep for second to generate new timestamp
        time.sleep(1)
        timestamp = self.update_one(session, self.updated_with_null_dataset)
        cdc_log_data = self.get_cdc_log_records_by_timestamp(session, timestamp)
        self.verify_cdc_log_rows_after_update_with_null(cdc_log_data, preimage_enable, postimage_enable)

    def delete_operation_tmpl(self, preimage_enable=False, postimage_enable=False):
        node, session = self.prepare_cluster_and_schema(preimage_enable=preimage_enable, postimage_enable=postimage_enable)
        # insert first record
        self.insert_one(session, self.inserted_dataset)
        # sleep for second to generate new timestamp
        time.sleep(1)
        timestamp = self.delete_one(session, self.deleted_dataset)
        cdc_log_data = self.get_cdc_log_records_by_timestamp(session, timestamp)
        self.verify_cdc_log_rows_after_delete_operation(cdc_log_data, preimage_enable, postimage_enable)

    def all_operation_tmpl(self, preimage_enable=False, postimage_enable=False):
        node, session = self.prepare_cluster_and_schema(preimage_enable=preimage_enable, postimage_enable=postimage_enable)
        # insert first record
        self.insert_one(session, self.inserted_dataset)
        self.update_one(session, self.updated_dataset)
        self.delete_one(session, self.deleted_dataset)

        cdc_log_data = self.get_all_cdc_log_records(session)
        self.verify_cdc_log_rows_after_several_operations(cdc_log_data, preimage_enable, postimage_enable)

    def verify_cdc_log_rows_after_first_operation(self, cdc_log_data, operation, postimage_enable):
        if postimage_enable:
            self.check_cdc_log_num_row(cdc_log_data, 2)
            self.check_cdc_log_row(cdc_log_data[0], operation=operation, batch_seq=0, expected_data=self.inserted_dataset)
            self.check_cdc_log_row(cdc_log_data[1], operation=CdcLogOperations.POSTIMAGE, batch_seq=1, expected_data=self.inserted_dataset)
        else:
            self.check_cdc_log_num_row(cdc_log_data, 1)
            self.check_cdc_log_row(cdc_log_data[0], operation=operation, batch_seq=0, expected_data=self.inserted_dataset)

    def verify_cdc_log_rows_after_operation(self, cdc_log_data, operation, preimage_enable, postimage_enable, expected_dataset):
        if preimage_enable and postimage_enable:
            self.check_cdc_log_num_row(cdc_log_data, 3)
            self.check_cdc_log_row(cdc_log_data[0], operation=CdcLogOperations.PREIMAGE, batch_seq=0, expected_data=self.inserted_dataset)
            self.check_cdc_log_row(cdc_log_data[1], operation=operation, batch_seq=1, expected_data=expected_dataset)
            self.check_cdc_log_row(cdc_log_data[2], operation=CdcLogOperations.POSTIMAGE, batch_seq=2, expected_data=expected_dataset)
        elif postimage_enable:
            self.check_cdc_log_num_row(cdc_log_data, 2)
            self.check_cdc_log_row(cdc_log_data[0], operation=operation, batch_seq=0, expected_data=expected_dataset)
            self.check_cdc_log_row(cdc_log_data[1], operation=CdcLogOperations.POSTIMAGE, batch_seq=1, expected_data=expected_dataset)
        elif preimage_enable:
            self.check_cdc_log_num_row(cdc_log_data, 2)
            self.check_cdc_log_row(cdc_log_data[0], operation=CdcLogOperations.PREIMAGE, batch_seq=0, expected_data=self.inserted_dataset)
            self.check_cdc_log_row(cdc_log_data[1], operation=operation, batch_seq=1, expected_data=expected_dataset)
        else:
            self.check_cdc_log_num_row(cdc_log_data, 1)
            self.check_cdc_log_row(cdc_log_data[0], operation=operation, batch_seq=0, expected_data=expected_dataset)

    def verify_cdc_log_rows_after_update_with_null(self, cdc_log_data, preimage_enable, postimage_enable):
        if preimage_enable and postimage_enable:
            self.check_cdc_log_num_row(cdc_log_data, 3)
            self.check_cdc_log_row(cdc_log_data[0],
                                   operation=CdcLogOperations.PREIMAGE,
                                   batch_seq=0,
                                   expected_data=self.inserted_dataset)
            self.check_cdc_log_row(cdc_log_data[1],
                                   operation=CdcLogOperations.UPDATE,
                                   batch_seq=1,
                                   expected_data=self.updated_with_null_dataset)
            self.check_cdc_log_row(cdc_log_data[2],
                                   operation=CdcLogOperations.POSTIMAGE,
                                   batch_seq=2,
                                   expected_data=self.updated_with_null_dataset)
        elif postimage_enable:
            self.check_cdc_log_num_row(cdc_log_data, 2)
            self.check_cdc_log_row(cdc_log_data[0],
                                   operation=CdcLogOperations.UPDATE,
                                   batch_seq=0,
                                   expected_data=self.updated_with_null_dataset)
            self.check_cdc_log_row(cdc_log_data[1],
                                   operation=CdcLogOperations.POSTIMAGE,
                                   batch_seq=1,
                                   expected_data=self.updated_with_null_dataset)
        elif preimage_enable:
            self.check_cdc_log_num_row(cdc_log_data, 2)
            self.check_cdc_log_row(cdc_log_data[0],
                                   operation=CdcLogOperations.PREIMAGE,
                                   batch_seq=0,
                                   expected_data=self.inserted_dataset)
            self.check_cdc_log_row(cdc_log_data[1],
                                   operation=CdcLogOperations.UPDATE,
                                   batch_seq=1,
                                   expected_data=self.updated_with_null_dataset)
        # check insert record
        else:
            self.check_cdc_log_num_row(cdc_log_data, 1)
            self.check_cdc_log_row(cdc_log_data[0],
                                   operation=CdcLogOperations.UPDATE,
                                   batch_seq=0,
                                   expected_data=self.updated_with_null_dataset)

    def verify_cdc_log_rows_after_delete_operation(self, cdc_log_data, preimage_enable, postimage_enable):
        if preimage_enable and postimage_enable:
            self.check_cdc_log_num_row(cdc_log_data, 3)
            self.check_cdc_log_row(cdc_log_data[0],
                                   operation=CdcLogOperations.PREIMAGE,
                                   batch_seq=0,
                                   expected_data=self.inserted_dataset)
            self.check_cdc_log_row(cdc_log_data[1],
                                   operation=CdcLogOperations.UPDATE,
                                   batch_seq=1,
                                   expected_data=self.deleted_dataset,
                                   deleted_col=["value"])
            self.check_cdc_log_row(cdc_log_data[2],
                                   operation=CdcLogOperations.POSTIMAGE,
                                   batch_seq=2,
                                   expected_data=self.updated_with_null_dataset)
        elif postimage_enable:
            self.check_cdc_log_num_row(cdc_log_data, 2)
            self.check_cdc_log_row(cdc_log_data[0],
                                   operation=CdcLogOperations.UPDATE,
                                   batch_seq=0,
                                   expected_data=self.deleted_dataset,
                                   deleted_col=["value"])
            self.check_cdc_log_row(cdc_log_data[1],
                                   operation=CdcLogOperations.POSTIMAGE,
                                   batch_seq=1,
                                   expected_data=self.updated_with_null_dataset)
        elif preimage_enable:
            self.check_cdc_log_num_row(cdc_log_data, 2)
        # check preimage record
            self.check_cdc_log_row(cdc_log_data[0],
                                   operation=CdcLogOperations.PREIMAGE,
                                   batch_seq=0,
                                   expected_data=self.inserted_dataset)
            self.check_cdc_log_row(cdc_log_data[1],
                                   operation=CdcLogOperations.UPDATE,
                                   batch_seq=1,
                                   expected_data=self.deleted_dataset,
                                   deleted_col=["value"])

        # check insert record
        else:
            self.check_cdc_log_num_row(cdc_log_data, 1)
            self.check_cdc_log_row(cdc_log_data[0],
                                   operation=CdcLogOperations.UPDATE,
                                   batch_seq=0,
                                   expected_data=self.deleted_dataset,
                                   deleted_col=["value"])

    def verify_cdc_log_rows_after_several_operations(self, cdc_log_data, preimage_enable, postimage_enable):
        if preimage_enable and postimage_enable:
            self.check_cdc_log_num_row(cdc_log_data, 8)
            self.check_cdc_log_row(cdc_log_data[0],
                                   operation=CdcLogOperations.INSERT,
                                   batch_seq=0,
                                   expected_data=self.inserted_dataset)
            self.check_cdc_log_row(cdc_log_data[1],
                                   operation=CdcLogOperations.POSTIMAGE,
                                   batch_seq=1,
                                   expected_data=self.inserted_dataset)
            self.check_cdc_log_row(cdc_log_data[2],
                                   operation=CdcLogOperations.PREIMAGE,
                                   batch_seq=0,
                                   expected_data=self.inserted_dataset)
            self.check_cdc_log_row(cdc_log_data[3],
                                   operation=CdcLogOperations.UPDATE,
                                   batch_seq=1,
                                   expected_data=self.updated_dataset)
            self.check_cdc_log_row(cdc_log_data[4],
                                   operation=CdcLogOperations.POSTIMAGE,
                                   batch_seq=2,
                                   expected_data=self.updated_dataset)
            self.check_cdc_log_row(cdc_log_data[5],
                                   operation=CdcLogOperations.PREIMAGE,
                                   batch_seq=0,
                                   expected_data=self.updated_dataset)
            self.check_cdc_log_row(cdc_log_data[6],
                                   operation=CdcLogOperations.UPDATE,
                                   batch_seq=1,
                                   expected_data=self.deleted_dataset)
            self.check_cdc_log_row(cdc_log_data[7],
                                   operation=CdcLogOperations.POSTIMAGE,
                                   batch_seq=2,
                                   expected_data=self.deleted_dataset)
        elif postimage_enable:
            self.check_cdc_log_num_row(cdc_log_data, 6)
            self.check_cdc_log_row(cdc_log_data[0],
                                   operation=CdcLogOperations.INSERT,
                                   batch_seq=0,
                                   expected_data=self.inserted_dataset)
            self.check_cdc_log_row(cdc_log_data[1],
                                   operation=CdcLogOperations.POSTIMAGE,
                                   batch_seq=1,
                                   expected_data=self.inserted_dataset)
            self.check_cdc_log_row(cdc_log_data[2],
                                   operation=CdcLogOperations.UPDATE,
                                   batch_seq=0,
                                   expected_data=self.updated_dataset)
            self.check_cdc_log_row(cdc_log_data[3],
                                   operation=CdcLogOperations.POSTIMAGE,
                                   batch_seq=1,
                                   expected_data=self.updated_dataset)
            self.check_cdc_log_row(cdc_log_data[4],
                                   operation=CdcLogOperations.UPDATE,
                                   batch_seq=0,
                                   expected_data=self.deleted_dataset)
            self.check_cdc_log_row(cdc_log_data[5],
                                   operation=CdcLogOperations.POSTIMAGE,
                                   batch_seq=1,
                                   expected_data=self.deleted_dataset)

        elif preimage_enable:
            self.check_cdc_log_num_row(cdc_log_data, 5)

            self.check_cdc_log_row(cdc_log_data[0],
                                   operation=CdcLogOperations.INSERT,
                                   batch_seq=0,
                                   expected_data=self.inserted_dataset)
            self.check_cdc_log_row(cdc_log_data[1],
                                   operation=CdcLogOperations.PREIMAGE,
                                   batch_seq=0,
                                   expected_data=self.inserted_dataset)
            self.check_cdc_log_row(cdc_log_data[2],
                                   operation=CdcLogOperations.UPDATE,
                                   batch_seq=1,
                                   expected_data=self.updated_dataset)
            self.check_cdc_log_row(cdc_log_data[3],
                                   operation=CdcLogOperations.PREIMAGE,
                                   batch_seq=0,
                                   expected_data=self.updated_dataset)
            self.check_cdc_log_row(cdc_log_data[4],
                                   operation=CdcLogOperations.UPDATE,
                                   batch_seq=1,
                                   expected_data=self.deleted_dataset)
        # check insert record
        else:
            self.check_cdc_log_num_row(cdc_log_data, 3)
            self.check_cdc_log_row(cdc_log_data[0],
                                   operation=CdcLogOperations.INSERT,
                                   batch_seq=0,
                                   expected_data=self.inserted_dataset)
            self.check_cdc_log_row(cdc_log_data[1],
                                   operation=CdcLogOperations.UPDATE,
                                   batch_seq=0,
                                   expected_data=self.updated_dataset)
            self.check_cdc_log_row(cdc_log_data[2],
                                   operation=CdcLogOperations.UPDATE,
                                   batch_seq=0,
                                   expected_data=self.deleted_dataset)


@attr('single_node')
class CDCCollectionsTmpl(CdcTools):
    columns_data = None
    __test__ = False

    timeuuid = uuid_from_time(time.time())

    @property
    def inserted_dataset(self):
        return {"pkey": self.timeuuid,
                "ckey": self.timeuuid,
                "value": self.columns_data["ins_dataset"]}

    @property
    def null_value_dataset(self):
        return {"pkey": self.timeuuid,
                "ckey": self.timeuuid,
                "value": None}

    @property
    def updated_dataset(self):
        return {"pkey": self.timeuuid,
                "ckey": self.timeuuid,
                "value": self.columns_data["upd_dataset"]}

    @property
    def added_element_dataset(self):
        return {"pkey": self.timeuuid,
                "ckey": self.timeuuid,
                "value": self.columns_data["add_el_dataset"]}

    @property
    def deleted_element_dataset(self):
        return {"pkey": self.timeuuid,
                "ckey": self.timeuuid,
                "value": self.columns_data["del_el_dataset"]}

    @property
    def result_dataset_after_delete_element(self):
        return {"pkey": self.timeuuid,
                "ckey": self.timeuuid,
                "value": self.columns_data["result_delete_element_dataset"]}

    @property
    def result_dataset_after_add_element(self):
        return {"pkey": self.timeuuid,
                "ckey": self.timeuuid,
                "value": self.columns_data["result_add_element_dataset"]}

    @property
    def deleted_dataset(self):
        return {"pkey": self.timeuuid,
                "ckey": self.timeuuid}

    def test_collection_insert(self):
        self.insert_operation_tmpl()

    def test_collection_insert_with_preimage(self):
        self.insert_operation_tmpl(preimage_enable=True)

    def test_collection_insert_with_postimage(self):
        self.insert_operation_tmpl(postimage_enable=True)

    def test_collection_insert_with_preimage_postimage(self):
        self.insert_operation_tmpl(preimage_enable=True, postimage_enable=True)

    def test_update_collection_with_add_element(self):
        self.collection_update_tmpl(add_element=True)

    def test_update_collection_with_add_element_with_preimage(self):
        self.collection_update_tmpl(add_element=True, preimage_enable=True)

    def test_update_collection_with_add_element_with_postimage(self):
        self.collection_update_tmpl(add_element=True, postimage_enable=True)

    def test_update_collection_with_add_element_with_preimage_postimage(self):
        self.collection_update_tmpl(add_element=True, preimage_enable=True, postimage_enable=True)

    def test_update_collection_with_delete_element(self):
        self.collection_update_tmpl(remove_element=True)

    def test_update_collection_with_delete_element_with_preimage(self):
        self.collection_update_tmpl(remove_element=True, preimage_enable=True)

    def test_update_collection_with_delete_element_with_postimage(self):
        self.collection_update_tmpl(remove_element=True, postimage_enable=True)

    def test_update_collection_with_delete_element_with_preimage_postimage(self):
        self.collection_update_tmpl(remove_element=True, preimage_enable=True, postimage_enable=True)

    def test_update_collection(self):
        self.collection_update_tmpl()

    def test_update_collection_with_preimage(self):
        self.collection_update_tmpl(preimage_enable=True)

    def test_update_collection_with_postimage(self):
        self.collection_update_tmpl(postimage_enable=True)

    def test_update_collection_with_preimage_postimage(self):
        self.collection_update_tmpl(preimage_enable=True, postimage_enable=True)

    def test_collection_delete(self):
        self.collection_delete_tmpl()

    def test_collection_delete_with_preimage(self):
        self.collection_delete_tmpl(preimage_enable=True)

    def test_collection_delete_with_postimage(self):
        self.collection_delete_tmpl(postimage_enable=True)

    def test_collection_delete_with_preimage_postimage(self):
        self.collection_delete_tmpl(preimage_enable=True, postimage_enable=True)

    def insert_operation_tmpl(self, preimage_enable=False, postimage_enable=False):
        node, session = self.prepare_cluster_and_schema(preimage_enable=preimage_enable,
                                                        postimage_enable=postimage_enable,
                                                        primary_key_type="timeuuid")
        # insert first record to partitions
        timestamp = self.insert_one(session, self.inserted_dataset)
        cdc_log_data = self.get_cdc_log_records_by_timestamp(session, timestamp)
        self.verify_cdc_log_rows_after_first_insert_to_base_table(cdc_log_data, postimage_enable)
        # insert record to not empty parition
        timestamp = self.insert_one(session, self.inserted_dataset)
        cdc_log_data = self.get_cdc_log_records_by_timestamp(session, timestamp)
        self.verify_cdc_log_rows_after_insert_to_base_table(cdc_log_data, preimage_enable, postimage_enable)

    def collection_update_tmpl(self, add_element=False, remove_element=False, preimage_enable=False, postimage_enable=False):
        node, session = self.prepare_cluster_and_schema(preimage_enable=preimage_enable,
                                                        postimage_enable=postimage_enable,
                                                        primary_key_type="timeuuid")
        # insert first record
        timestamp = self.update_one(session, self.inserted_dataset)
        cdc_log_data = self.get_cdc_log_records_by_timestamp(session, timestamp)
        self.verify_cdc_log_rows_after_first_update_to_base_table(cdc_log_data, postimage_enable)

        if add_element:
            timestamp = self.update_collection_with_element(session, self.added_element_dataset, add=True)
        elif remove_element:
            timestamp = self.update_collection_with_element(session, self.deleted_element_dataset, add=False)
        else:
            timestamp = self.update_one(session, self.updated_dataset)
        cdc_log_data = self.get_cdc_log_records_by_timestamp(session, timestamp)
        self.verify_cdc_log_rows_after_update_to_base_table(cdc_log_data, preimage_enable, postimage_enable, add_element, remove_element)

    def collection_delete_tmpl(self, preimage_enable=False, postimage_enable=False):
        node, session = self.prepare_cluster_and_schema(preimage_enable=preimage_enable,
                                                        postimage_enable=postimage_enable,
                                                        primary_key_type="timeuuid")
        # insert first record
        self.insert_one(session, self.inserted_dataset)
        # sleep for second to generate new timestamp
        time.sleep(1)

        timestamp = self.delete_one(session, self.deleted_dataset)
        cdc_log_data = self.get_cdc_log_records_by_timestamp(session, timestamp)
        self.verify_cdc_log_rows_after_delete_value(cdc_log_data, preimage_enable, postimage_enable)

    def verify_cdc_log_rows_after_first_insert_to_base_table(self, cdc_log_data, postimage_enable):
        if postimage_enable:
            self.check_cdc_log_num_row(cdc_log_data, 2)
            self.check_cdc_log_row_collection(cdc_log_data[0], operation=CdcLogOperations.INSERT, batch_seq=0,
                                              deleted_col=['value'], expected_data=self.inserted_dataset)
            self.check_cdc_log_row_collection(cdc_log_data[1], operation=CdcLogOperations.POSTIMAGE, batch_seq=1,
                                              expected_data=self.inserted_dataset)
        else:
            self.check_cdc_log_num_row(cdc_log_data, 1)
            self.check_cdc_log_row_collection(cdc_log_data[0], operation=CdcLogOperations.INSERT, batch_seq=0,
                                              expected_data=self.inserted_dataset)

    def verify_cdc_log_rows_after_insert_to_base_table(self, cdc_log_data, preimage_enable, postimage_enable):
        if preimage_enable and postimage_enable:
            self.check_cdc_log_num_row(cdc_log_data, 3)
            self.check_cdc_log_row_collection(cdc_log_data[0], operation=CdcLogOperations.PREIMAGE, batch_seq=0,
                                              expected_data=self.inserted_dataset)
            self.check_cdc_log_row_collection(cdc_log_data[1], operation=CdcLogOperations.INSERT, batch_seq=1,
                                              deleted_col=['value'], expected_data=self.inserted_dataset)
            self.check_cdc_log_row_collection(cdc_log_data[2], operation=CdcLogOperations.POSTIMAGE, batch_seq=2,
                                              expected_data=self.inserted_dataset)

        elif postimage_enable:
            self.check_cdc_log_num_row(cdc_log_data, 2)
            self.check_cdc_log_row_collection(cdc_log_data[0], operation=CdcLogOperations.INSERT, batch_seq=0,
                                              deleted_col=['value'], expected_data=self.inserted_dataset)
            self.check_cdc_log_row_collection(cdc_log_data[1], operation=CdcLogOperations.POSTIMAGE, batch_seq=1,
                                              expected_data=self.inserted_dataset)

        elif preimage_enable:
            self.check_cdc_log_num_row(cdc_log_data, 2)
            self.check_cdc_log_row_collection(cdc_log_data[0], operation=CdcLogOperations.PREIMAGE, batch_seq=0,
                                              expected_data=self.inserted_dataset)
            self.check_cdc_log_row_collection(cdc_log_data[1], operation=CdcLogOperations.INSERT, batch_seq=1,
                                              deleted_col=['value'], expected_data=self.inserted_dataset)

        else:
            self.check_cdc_log_num_row(cdc_log_data, 1)
            self.check_cdc_log_row_collection(cdc_log_data[0], operation=CdcLogOperations.INSERT, batch_seq=0,
                                              deleted_col=['value'], expected_data=self.inserted_dataset)

    def verify_cdc_log_rows_after_first_update_to_base_table(self, cdc_log_data, postimage_enable):
        if postimage_enable:
            self.check_cdc_log_num_row(cdc_log_data, 2)
            self.check_cdc_log_row_collection(cdc_log_data[0], operation=CdcLogOperations.UPDATE, batch_seq=0,
                                              deleted_col=['value'], expected_data=self.inserted_dataset)
            self.check_cdc_log_row_collection(cdc_log_data[1], operation=CdcLogOperations.POSTIMAGE, batch_seq=1,
                                              expected_data=self.inserted_dataset)
        else:
            self.check_cdc_log_num_row(cdc_log_data, 1)
            self.check_cdc_log_row_collection(cdc_log_data[0], operation=CdcLogOperations.UPDATE, batch_seq=0,
                                              deleted_col=['value'], expected_data=self.inserted_dataset)

    def verify_cdc_log_rows_after_update_to_base_table(self, cdc_log_data, preimage_enable, postimage_enable, add_element, remove_element):

        if add_element or remove_element:
            deleted_element = self.deleted_element_dataset["value"] if remove_element else None
            updating_dataset = self.added_element_dataset if add_element else self.null_value_dataset
            postimage_data_set = self.result_dataset_after_add_element if add_element else self.result_dataset_after_delete_element
            deleted_col = None

        else:
            deleted_element = None
            updating_dataset = self.updated_dataset
            postimage_data_set = self.updated_dataset
            deleted_col = ['value']

        if preimage_enable and postimage_enable:

            self.check_cdc_log_num_row(cdc_log_data, 3)
            self.check_cdc_log_row_collection(cdc_log_data[0], operation=CdcLogOperations.PREIMAGE, batch_seq=0,
                                              expected_data=self.inserted_dataset)
            self.check_cdc_log_row_collection(cdc_log_data[1], operation=CdcLogOperations.UPDATE, batch_seq=1,
                                              expected_data=updating_dataset, deleted_keys=deleted_element,
                                              deleted_col=deleted_col)
            self.check_cdc_log_row_collection(cdc_log_data[2], operation=CdcLogOperations.POSTIMAGE, batch_seq=2,
                                              expected_data=postimage_data_set)

        elif postimage_enable:
            self.check_cdc_log_num_row(cdc_log_data, 2)
            self.check_cdc_log_row_collection(cdc_log_data[0], operation=CdcLogOperations.UPDATE, batch_seq=0,
                                              deleted_col=deleted_col, expected_data=updating_dataset)
            self.check_cdc_log_row_collection(cdc_log_data[1], operation=CdcLogOperations.POSTIMAGE, batch_seq=1,
                                              expected_data=postimage_data_set)
        elif preimage_enable:
            self.check_cdc_log_num_row(cdc_log_data, 2)
            self.check_cdc_log_row_collection(cdc_log_data[0], operation=CdcLogOperations.PREIMAGE, batch_seq=0,
                                              expected_data=self.inserted_dataset)

            self.check_cdc_log_row_collection(cdc_log_data[1], operation=CdcLogOperations.UPDATE, batch_seq=1,
                                              expected_data=updating_dataset, deleted_keys=deleted_element,
                                              deleted_col=deleted_col)
        else:
            self.check_cdc_log_num_row(cdc_log_data, 1)
            self.check_cdc_log_row_collection(cdc_log_data[0], operation=CdcLogOperations.UPDATE, batch_seq=0,
                                              expected_data=updating_dataset, deleted_keys=deleted_element,
                                              deleted_col=deleted_col)

    def verify_cdc_log_rows_after_delete_value(self, cdc_log_data, preimage_enable, postimage_enable):
        if preimage_enable and postimage_enable:
            self.check_cdc_log_num_row(cdc_log_data, 3)
            self.check_cdc_log_row_collection(cdc_log_data[0], operation=CdcLogOperations.PREIMAGE, batch_seq=0,
                                              expected_data=self.inserted_dataset)
            self.check_cdc_log_row(cdc_log_data[1], operation=CdcLogOperations.UPDATE, batch_seq=1,
                                   expected_data=self.null_value_dataset, deleted_col=['value'])
            self.check_cdc_log_row_collection(cdc_log_data[2], operation=CdcLogOperations.POSTIMAGE, batch_seq=2,
                                              expected_data=self.null_value_dataset)
        elif postimage_enable:
            self.check_cdc_log_num_row(cdc_log_data, 2)
            self.check_cdc_log_row(cdc_log_data[0], operation=CdcLogOperations.UPDATE, batch_seq=0,
                                   expected_data=self.null_value_dataset, deleted_col=['value'])
            self.check_cdc_log_row_collection(cdc_log_data[1], operation=CdcLogOperations.POSTIMAGE, batch_seq=1,
                                              expected_data=self.null_value_dataset)
        elif preimage_enable:
            self.check_cdc_log_num_row(cdc_log_data, 2)
            self.check_cdc_log_row_collection(cdc_log_data[0], operation=CdcLogOperations.PREIMAGE, batch_seq=0,
                                              expected_data=self.inserted_dataset)
            self.check_cdc_log_row(cdc_log_data[1], operation=CdcLogOperations.UPDATE, batch_seq=1,
                                   expected_data=self.null_value_dataset, deleted_col=['value'])
        else:
            self.check_cdc_log_num_row(cdc_log_data, 1)
            self.check_cdc_log_row_collection(cdc_log_data[0], operation=CdcLogOperations.UPDATE, batch_seq=0,
                                              expected_data=self.null_value_dataset, deleted_col=['value'])


native_types_values = [
    {"cl_type": "bigint", "ins_dataset": 1, "upd_dataset": 2},
    {"cl_type": "int", "ins_dataset": 3, "upd_dataset": 4},
    {"cl_type": "smallint", "ins_dataset": 5, "upd_dataset": 6},
    {"cl_type": "tinyint", "ins_dataset": 8, "upd_dataset": 7},
    {"cl_type": "varint", "ins_dataset": 1, "upd_dataset": 4},
    {"cl_type": "boolean", "ins_dataset": True, "upd_dataset": False},
    {"cl_type": "blob", "ins_dataset": b'1234567890qwertyuiop', "upd_dataset": b'a'},
    {"cl_type": "date", "ins_dataset": date(2020, 2, 2), "upd_dataset": date(2020, 12, 12)},
    {"cl_type": "decimal", "ins_dataset": Decimal("10.1"), "upd_dataset": Decimal("12.2")},
    {"cl_type": "double", "ins_dataset": 10.1000001, "upd_dataset": 22.22222},
    {"cl_type": "float", "ins_dataset": 33.33000183105469, "upd_dataset": 44.44000244140625},
    {"cl_type": "inet", "ins_dataset": "1.1.1.1", "upd_dataset": "2.2.2.2"},
    {"cl_type": "time", "ins_dataset": Time('02:02:02.222'), "upd_dataset": Time('12:12:12.121')},
    {"cl_type": "timestamp", "ins_dataset": datetime(2020, 2, 2, 2, 2, 2), "upd_dataset": datetime(2020, 3, 3, 3, 3, 3)},
    {"cl_type": "timeuuid", "ins_dataset": UUID('b478b7c2-5d3c-11ea-84b5-5aa95d83d60f'), "upd_dataset": UUID('c2ecebac-5d3c-11ea-9fd2-3cd5439c36c3')},
    {"cl_type": "uuid", "ins_dataset": uuid1(), "upd_dataset": uuid1()},
    {"cl_type": "varint", "ins_dataset": 1, "upd_dataset": 4},
    {"cl_type": "text", "ins_dataset": "aaaaaaa", "upd_dataset": "bbbbbbb"},
    {"cl_type": "varchar", "ins_dataset": "cccccccc", "upd_dataset": "ddddddddd"},
    {"cl_type": "ascii", "ins_dataset": "0123456789abcdef", "upd_dataset": "abcdef0123456789"},
]

collections_types = [
    {
        "cl_type": "map<text, text>",
        "ins_dataset": {'key1': "value1", "key2": "value2"},
        "upd_dataset": {'key3': "value3", "key4": "value4"},
        "add_el_dataset": {"key5": "value5"},
        "del_el_dataset": {"key1"},
        "result_add_element_dataset": {"key1": "value1", "key2": "value2", "key5": "value5"},
        "result_delete_element_dataset": {"key2": "value2"}
    },
    {
        "cl_type": "map<bigint, text>",
        "ins_dataset": {1: "value1", 10000: "value2"},
        "upd_dataset": {2000: "value3", 3: "value4"},
        "add_el_dataset": {5000: "value5"},
        "del_el_dataset": {1},
        "result_add_element_dataset": {1: "value1", 10000: "value2", 5000: "value5"},
        "result_delete_element_dataset": {10000: "value2"}
    },
    {
        "cl_type": "set<text>",
        "ins_dataset": {"value1", "value2"},
        "upd_dataset": {"value3", "value4"},
        "add_el_dataset": {"value5"},
        "del_el_dataset": {"value1"},
        "result_add_element_dataset": {"value1", "value2", "value5"},
        "result_delete_element_dataset": {"value2"}
    },
    {
        "cl_type": "list<int>",
        "ins_dataset": [1, 2],
        "upd_dataset": [3, 4],
        "add_el_dataset": [5],
        "del_el_dataset": [1],
        "result_add_element_dataset": [1, 2, 5],
        "result_delete_element_dataset": [2]
    }
]

frozen_collections = [
    {"cl_type": "frozen<map<text, text>>", "ins_dataset": {'key1': "value1", "key2": "value2"}, "upd_dataset": {'key3': "value3", "key4": "value4"}},
    {"cl_type": "frozen<set<text>>", "ins_dataset": {"value1", "value2"}, "upd_dataset": {"value3", "value4"}},
    {"cl_type": "frozen<list<text>>", "ins_dataset": ["value1", "value2"], "upd_dataset": ["value3", "value4"]},
    {"cl_type": "frozen<list<int>>", "ins_dataset": [1, 12], "upd_dataset": [3, 13]},
]

def mkident(s):
    s = re.sub('\s+', '', s)
    s = re.sub('[<>,]', '_', s)
    return re.sub('_+$', '', s)

for native_type in native_types_values:
    cls_name = ('TestCDCNativeType_with_{}'.format(native_type["cl_type"]))
    vars()[cls_name] = type(cls_name, (CDCNativeTypeTmpl,), {'columns_data': native_type, '__test__': True})

for frozen_collection_type in frozen_collections:
    cls_name = ('TestCDCFrozenCollection_with_{}'.format(mkident(frozen_collection_type["cl_type"])))
    vars()[cls_name] = type(cls_name, (CDCNativeTypeTmpl, ), {'columns_data': frozen_collection_type, '__test__': True})

for collection_type in collections_types:
    cls_name = ('TestCDCCollectionType_with_{}'.format(mkident(collection_type["cl_type"])))
    vars()[cls_name] = type(cls_name, (CDCCollectionsTmpl,), {'columns_data': collection_type, '__test__': True})
