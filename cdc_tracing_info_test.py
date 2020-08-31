import pprint
import re

from typing import List
from itertools import zip_longest
from uuid import uuid4

from nose.plugins.attrib import attr

from cassandra.cluster import Session, SimpleStatement
from cassandra import ConsistencyLevel
from cassandra.util import uuid_from_time, datetime_from_uuid1, Time, OrderedMapSerializedKey
from dtest import Tester, debug, wait_for
from ccmlib.scylla_cluster import ScyllaNode
from tools import require, new_node

from cdc_tests import CdcLogOperations, CDCInitializeHelper

PP = pprint.PrettyPrinter(indent=4)


class CDCTraceInfoMatcher:
    start_line = "CDC: Started generating mutations for log rows.*$"
    end_line = "CDC: Finished generating all log mutations.*$"

    def __init__(self, tokens, preimage=False, postimage=False, splitting=False):
        self.tokens = tokens
        self.preimage = preimage
        self.postimage = postimage
        self.splitting = splitting

    @property
    def preimage_pattern(self):
        if self.preimage or self.postimage:
            return "CDC: Selecting preimage for {{key: pk{{.*?}}, token:{token_id}}}.*$"
        else:
            return "CDC: Preimage not enabled for the table, not querying current value of {{key: pk{{.*?}}, token:{token_id}}}.*$"

    @property
    def generate_log_mutation_pattern(self):
        return "CDC: Generating log mutations for {{key: pk{{.*?}}, token:{token_id}}}.*$"

    @property
    def splitting_pattern(self):
        if self.splitting:
            return "CDC: Splitting {{key: pk{{.*?}}, token:{token_id}}}.*$"
        else:
            return "CDC: No need to split {{key: pk{{.*?}}, token:{token_id}}}.*$"

    @property
    def number_log_mutation_pattern(self):
        return "CDC: Generated [\d]+ log mutations from {{key: pk{{.*?}}, token:{token_id}}}.*$"

    def get_raw_cdc_lines(self, output: str) -> List[str]:
        cdc_lines = [line.strip() for line in output.splitlines() if "CDC:" in line]
        return cdc_lines

    def verify_cdc_trace_info(self, output: str) -> None:
        cdc_trace_info_lines = self.get_raw_cdc_lines(output)
        assert re.match(self.start_line, cdc_trace_info_lines.pop(0)), "Start line for CDC tracing was not found"
        assert re.match(self.end_line, cdc_trace_info_lines.pop(-1)), "End line for CDC tracing was not found"

        # verify cdc trace info per token
        for token in self.tokens:
            cdc_lines_for_token = [line for line in cdc_trace_info_lines if str(token) in line]
            self._verify_trace_info_per_token(token, cdc_lines_for_token)
            # clean matched lines from cdc tracing info lines
            for line in cdc_lines_for_token:
                cdc_trace_info_lines.remove(line)

        # verify that cdc trace info doesn't contain unmatched lines
        assert len(cdc_trace_info_lines) == 0, f"Next strings were not matched {cdc_trace_info_lines}"

    def _verify_trace_info_per_token(self, token: int, lines: List[str]) -> None:
        patterns = [
            self.preimage_pattern,
            self.generate_log_mutation_pattern,
            self.splitting_pattern,
            self.number_log_mutation_pattern
        ]

        for pattern, line in zip_longest(patterns, lines):
            # if new unexpected line will appeared in tracing, pattern will be none
            assert pattern, f"{line} is not matched any pattern"
            # if expected line will be missing in output,  pattern will not match it
            assert line, f"{pattern} doesn't match any line"
            # assert cdc line order and correctnes
            assert re.match(pattern.format(token_id=token),
                            line), f"{pattern.format(token_id=token)} not matched {line}"


class CDCTraceInfoTest(Tester, CDCInitializeHelper):
    keyspace = "ks"
    table = "cf"
    table_cdc_log = f"{table}_scylla_cdc_log"

    def prepare_cluster_and_schema(self, num_nodes=1, rf=1,
                                   value_type='text',
                                   preimage_enable=False,
                                   postimage_enable=False,
                                   primary_key_type=None):
        self.cluster.populate(1)
        self.cluster.set_configuration_options(values={"experimental_features": ["cdc"]})
        self.cluster.start(wait_for_binary_proto=True, wait_other_notice=True)
        for i in range(1, num_nodes):
            node = new_node(self.cluster, bootstrap=True)
            node.start(wait_for_binary_proto=True)
        node = self.cluster.nodelist()[0]
        session = self.patient_cql_connection(node)
        self.create_schema_with_cdc(session, rf=rf,
                                    value_type=value_type,
                                    preimage_enable=preimage_enable,
                                    postimage_enable=postimage_enable)

        self.wait_for_last_generation_to_be_active(session)
        self.wait_for_metadata_update(session, cluster_size=num_nodes)
        return (node, session)

    def create_schema_with_cdc(self, session, rf=1, value_type='text',
                               preimage_enable=False, postimage_enable=False):
        statement = f"CREATE TABLE {self.keyspace}.{self.table} \
                    (pkey int, \
                     ckey int, \
                     value {value_type}, \
                     PRIMARY KEY (pkey, ckey)\
                    )"
        statement += " WITH cdc={'enabled': true"
        if preimage_enable:
            statement += ", 'preimage': true"
        if postimage_enable:
            statement += ", 'postimage': true"
        statement += "}"
        session.execute(
            f"ALTER keyspace system_distributed with replication={{'class': 'SimpleStrategy', 'replication_factor': {rf}}}")
        self.create_ks(session, self.keyspace, rf=rf)
        session.execute(statement)

    def test_tracing_insert_native_type(self):
        self.check_tracing_info_for_operation()

    def test_tracing_insert_native_type_preimage(self):
        self.check_tracing_info_for_operation(preimage_enable=True)

    def test_tracing_insert_native_type_postimage(self):
        self.check_tracing_info_for_operation(postimage_enable=True)

    def test_tracing_insert_native_type_preimage_postimage(self):
        self.check_tracing_info_for_operation(preimage_enable=True, postimage_enable=True)

    def test_tracing_update_native_type(self):
        self.check_tracing_info_for_operation(operation=CdcLogOperations.UPDATE)

    def test_tracing_update_native_type_preimage_postimage(self):
        self.check_tracing_info_for_operation(operation=CdcLogOperations.UPDATE,
                                              preimage_enable=True, postimage_enable=True)

    def test_tracing_delete_partition_native_type(self):
        self.check_tracing_info_for_operation(operation=CdcLogOperations.PARTITION_DELETE, expect_splitting=True)

    def test_tracing_delete_partition_native_type_preimage_postimage(self):
        self.check_tracing_info_for_operation(operation=CdcLogOperations.PARTITION_DELETE,
                                              preimage_enable=True, postimage_enable=True, expect_splitting=True)

    def test_tracing_delete_row_native_type(self):
        self.check_tracing_info_for_operation(operation=CdcLogOperations.ROW_DELETE)

    def test_tracing_insert_collection(self):
        self.check_tracing_info_for_operation(operation=CdcLogOperations.INSERT,
                                              value_type='list<text>', expect_splitting=True)

    def test_tracing_insert_collection_preimage_postimage(self):
        self.check_tracing_info_for_operation(operation=CdcLogOperations.INSERT, value_type='list<text>',
                                              preimage_enable=True, postimage_enable=True, expect_splitting=True)

    def test_tracing_update_collection(self):
        self.check_tracing_info_for_operation(operation=CdcLogOperations.UPDATE, value_type='list<text>')

    def test_tracing_update_collection_preimage_postimage(self):
        self.check_tracing_info_for_operation(operation=CdcLogOperations.UPDATE, value_type='list<text>',
                                              preimage_enable=True, postimage_enable=True)

    def test_tracing_delete_partition_collection(self):
        self.check_tracing_info_for_operation(operation=CdcLogOperations.PARTITION_DELETE,
                                              value_type='list<text>', expect_splitting=True)

    def test_tracing_delete_partition_collection_preimage_postimage(self):
        self.check_tracing_info_for_operation(operation=CdcLogOperations.PARTITION_DELETE, value_type='list<text>',
                                              preimage_enable=True, postimage_enable=True, expect_splitting=True)

    def test_tracing_delete_row_collection(self):
        self.check_tracing_info_for_operation(operation=CdcLogOperations.ROW_DELETE, value_type='list<text>')

    def test_tracing_delete_row_collection_preimage_postimage(self):
        self.check_tracing_info_for_operation(operation=CdcLogOperations.ROW_DELETE, value_type='list<text>',
                                              preimage_enable=True, postimage_enable=True)

    def test_tracing_delete_row_range_collection(self):
        self.check_tracing_info_for_operation(operation=CdcLogOperations.RANGE_DELETE_END_EXCLUSIVE,
                                              value_type='list<text>')

    def test_tracing_delete_row_range_collection_preimage_postimage(self):
        self.check_tracing_info_for_operation(operation=CdcLogOperations.RANGE_DELETE_END_EXCLUSIVE, value_type='list<text>',
                                              preimage_enable=True, postimage_enable=True)

    def test_tracing_info_for_batch_insert_native_type(self):
        self.check_tracing_info_for_operation(operation=CdcLogOperations.INSERT, value_type='text', use_batch=True)

    def test_tracing_info_for_batch_insert_native_type_preimage_postimage(self):
        self.check_tracing_info_for_operation(operation=CdcLogOperations.INSERT, value_type='text',
                                              preimage_enable=True, postimage_enable=True, use_batch=True)

    def test_tracing_info_for_batch_insert_collection(self):
        self.check_tracing_info_for_operation(operation=CdcLogOperations.INSERT, value_type='list<text>',
                                              use_batch=True, expect_splitting=True)

    def test_tracing_info_for_batch_insert_collection_preimage_postimage(self):
        self.check_tracing_info_for_operation(operation=CdcLogOperations.INSERT, value_type='list<text>',
                                              preimage_enable=True, postimage_enable=True, use_batch=True, expect_splitting=True)

    def test_tracing_info_for_native_type_batch_update_preimage_postimage(self):
        self.check_tracing_info_for_operation(operation=CdcLogOperations.UPDATE, value_type='text',
                                              preimage_enable=True, postimage_enable=True, use_batch=True)

    def test_tracing_info_for_batch_update_collection_preimage_postimage(self):
        self.check_tracing_info_for_operation(operation=CdcLogOperations.UPDATE, value_type='list<text>',
                                              preimage_enable=True, postimage_enable=True, use_batch=True)

    def test_tracing_info_for_native_type_batch_delete_partition_preimage_postimage(self):
        self.check_tracing_info_for_operation(operation=CdcLogOperations.PARTITION_DELETE, value_type='text',
                                              preimage_enable=True, postimage_enable=True, use_batch=True, expect_splitting=True)

    def test_tracing_info_for_batch_delete_partition_collection_preimage_postimage(self):
        self.check_tracing_info_for_operation(operation=CdcLogOperations.PARTITION_DELETE, value_type='list<text>',
                                              preimage_enable=True, postimage_enable=True, use_batch=True, expect_splitting=True)

    def test_tracing_info_for_batch_delete_row_native_type_preimage_postimage(self):
        self.check_tracing_info_for_operation(operation=CdcLogOperations.ROW_DELETE, value_type='text',
                                              preimage_enable=True, postimage_enable=True, use_batch=True)

    def test_tracing_info_for_batch_delete_row_collection_preimage_postimage(self):
        self.check_tracing_info_for_operation(operation=CdcLogOperations.ROW_DELETE, value_type='list<text>',
                                              preimage_enable=True, postimage_enable=True, use_batch=True)

    def test_tracing_info_for_native_type_batch_delete_row_range_bound_exclusive_preimage_postimage(self):
        self.check_tracing_info_for_operation(operation=CdcLogOperations.RANGE_DELETE_END_EXCLUSIVE, value_type='text',
                                              preimage_enable=True, postimage_enable=True, use_batch=True)

    def test_tracing_info_for_native_type_batch_delete_row_range_bound_exclusive_collection_preimage_postimage(self):
        self.check_tracing_info_for_operation(operation=CdcLogOperations.RANGE_DELETE_END_EXCLUSIVE, value_type='list<text>',
                                              preimage_enable=True, postimage_enable=True, use_batch=True)

    def test_tracing_info_for_native_type_batch_delete_row_range_bound_inclusive_preimage_postimage(self):
        self.check_tracing_info_for_operation(operation=CdcLogOperations.RANGE_DELETE_END_INCLUSIVE, value_type='text',
                                              preimage_enable=True, postimage_enable=True, use_batch=True)

    def test_tracing_info_for_native_type_batch_delete_row_range_bound_inclusive_collection_preimage_postimage(self):
        self.check_tracing_info_for_operation(operation=CdcLogOperations.RANGE_DELETE_END_INCLUSIVE, value_type='list<text>',
                                              preimage_enable=True, postimage_enable=True, use_batch=True)

    def check_tracing_info_for_operation(self, operation=CdcLogOperations.INSERT, value_type='text',
                                         preimage_enable=False, postimage_enable=False,
                                         use_batch=False, expect_splitting=False):
        self.prepare_cluster_and_schema(
            value_type=value_type, preimage_enable=preimage_enable, postimage_enable=postimage_enable)

        node = self.cluster.nodelist()[0]
        session = self.patient_cql_connection(node)

        cql_command = self.prepare_cql_with_tracing(session, operation, value_type, use_batch)
        # get tokens of partitions before delete operation
        token_ids = self.get_tokens(session)

        result = node.run_cqlsh(cmds=cql_command, return_output=True)

        # if operation insert/update, table was empty before executing cql_command
        if not token_ids:
            token_ids = self.get_tokens(session)

        trace_matcher = CDCTraceInfoMatcher(tokens=token_ids, splitting=expect_splitting,
                                            preimage=preimage_enable, postimage=postimage_enable)
        trace_matcher.verify_cdc_trace_info(output=result[0])

    def prepare_cql_with_tracing(self, session, operation, value_type='text', use_batch=False):
        num_of_partitions = 2 if use_batch else 1
        if operation == CdcLogOperations.INSERT:
            cql_command = self.get_insert_query(value_type, use_batch)
        elif operation == CdcLogOperations.UPDATE:
            cql_command = self.get_update_query(value_type, use_batch)
        elif operation == CdcLogOperations.PARTITION_DELETE:
            self.generate_partitions_with_5_rows(session, value_type, num_of_partitions)
            cql_command = self.get_delete_partition_query(use_batch)
        elif operation == CdcLogOperations.ROW_DELETE:
            self.generate_partitions_with_5_rows(session, value_type, num_of_partitions)
            cql_command = self.get_delete_row_query(use_batch)
        elif operation == CdcLogOperations.RANGE_DELETE_END_EXCLUSIVE:
            self.generate_partitions_with_5_rows(session, value_type, num_of_partitions)
            cql_command = self.get_delete_row_range_bound_exclusive_query(use_batch)
        elif operation == CdcLogOperations.RANGE_DELETE_END_INCLUSIVE:
            self.generate_partitions_with_5_rows(session, value_type, num_of_partitions)
            cql_command = self.get_delete_row_range_bound_inclusive_query(use_batch)

        return cql_command

    def get_insert_query(self, value_type, use_batch=False):
        value = self.generate_value(value_type)
        if use_batch:
            ops = ""
            for i in range(5):
                ops += f"INSERT INTO {self.keyspace}.{self.table} (pkey, ckey, value) VALUES ({i}, 1, {value});"

            cql_command = f"""TRACING ON; \
                              BEGIN BATCH
                                {ops}
                              APPLY BATCH;"""
        else:
            cql_command = f"""TRACING ON; \
                                INSERT INTO {self.keyspace}.{self.table} (pkey, ckey, value) \
                                  VALUES (1, 1, {value});
                               """
        return cql_command

    def get_update_query(self, value_type, use_batch=False):
        value = self.generate_value(value_type)
        if use_batch:
            ops = ""
            for i in range(5):
                ops += f"UPDATE {self.keyspace}.{self.table} SET value = {value} WHERE pkey = {i} and ckey = 1;"
            cql_command = f"""TRACING ON;
                              BEGIN BATCH
                                    {ops}
                              APPLY BATCH;"""
        else:
            cql_command = f"""TRACING ON;
                              UPDATE {self.keyspace}.{self.table} SET value = {value} \
                                    WHERE pkey = 1 and ckey = 1;
                """
        return cql_command

    def get_delete_partition_query(self, use_batch=False):

        if use_batch:
            ops = ""
            for i in range(2):
                ops += f"DELETE FROM {self.keyspace}.{self.table} WHERE pkey = {i};"

            cql_command = f"""TRACING ON;
                              BEGIN BATCH
                                {ops}
                              APPLY BATCH;"""
        else:

            cql_command = f"""TRACING ON;
                              DELETE FROM {self.keyspace}.{self.table} WHERE pkey = 0;"""
        return cql_command

    def get_delete_row_query(self, use_batch=False):

        if use_batch:
            ops = ""
            for i in range(10):
                ops += f"DELETE FROM {self.keyspace}.{self.table} WHERE pkey = {i % 2} and ckey = {i % 5};"

            cql_command = f"""TRACING ON;
                              BEGIN BATCH
                                {ops}
                              APPLY BATCH;
                          """
        else:
            cql_command = f"""TRACING ON;
                          DELETE FROM {self.keyspace}.{self.table} WHERE pkey = 0 and ckey = 1;"""
        return cql_command

    def get_delete_row_range_bound_exclusive_query(self, use_batch):

        if use_batch:
            cql_command = f"""TRACING ON;
                              BEGIN BATCH
                                DELETE FROM {self.keyspace}.{self.table} WHERE pkey = 1 and ckey > 1 and ckey < 4;
                                DELETE FROM {self.keyspace}.{self.table} WHERE pkey = 0 and ckey > 1 and ckey < 4;
                              APPLY BATCH;
                            """
        else:
            cql_command = f"""TRACING ON;
                          DELETE FROM {self.keyspace}.{self.table} WHERE pkey = 0 and ckey >1 and ckey< 4;"""

        return cql_command

    def get_delete_row_range_bound_inclusive_query(self, use_batch):
        if use_batch:
            cql_command = f"""TRACING ON;
                              BEGIN BATCH
                                DELETE FROM {self.keyspace}.{self.table} WHERE pkey = 1 and ckey >= 1 and ckey <= 4;
                                DELETE FROM {self.keyspace}.{self.table} WHERE pkey = 0 and ckey >= 1 and ckey <= 4;
                              APPLY BATCH;
                            """
        else:

            cql_command = f"""TRACING ON;
                          DELETE FROM {self.keyspace}.{self.table} WHERE pkey = 0 and ckey >=1 and ckey <= 4;"""

        return cql_command

    def generate_partitions_with_5_rows(self, session, value_type='text', p_num=2):
        value = self.generate_value(value_type)
        for i in range(p_num * 5):
            session.execute(
                f"INSERT INTO {self.keyspace}.{self.table} (pkey, ckey, value) VALUES ({i % p_num}, {i % 5}, {value});")

    def get_tokens(self, session):
        base_rows = list(session.execute(f"SELECT token(pkey) as tkn from {self.keyspace}.{self.table}"))
        token_ids = {row.tkn for row in base_rows}
        return token_ids

    def generate_value(self, value_type='text'):
        if value_type in ['text', 'varchar', 'ascii']:
            return f"'{uuid4()}'"
        elif "list" in value_type:
            return [str(uuid4()), str(uuid4())]
        else:
            return None
