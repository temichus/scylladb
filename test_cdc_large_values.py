import time
import logging

from typing import Tuple, Union, List, Dict, Optional, Any
from concurrent.futures import ThreadPoolExecutor

import pytest

from cassandra.cluster import Session, SimpleStatement
from ccmlib.scylla_node import ScyllaNode

from cdc_test import CDCInitializeHelper
from dtest_class import Tester, create_ks
from tools.misc import ImmutableMapping
from dtest_setup_overrides import DTestSetupOverrides

MB = 1024 * 1024
LOGGER = logging.getLogger(__name__)


@pytest.mark.full_dtest
@pytest.mark.single_node
@pytest.mark.scylla_cdc
class TestLargeColumnsWithCDC(Tester, CDCInitializeHelper):

    @pytest.fixture(scope='function', autouse=True)
    def fixture_dtest_setup_overrides(self, dtest_config):
        dtest_setup_overrides = DTestSetupOverrides()
        dtest_setup_overrides.cluster_options = ImmutableMapping({'max_memory_for_unlimited_query_soft_limit': 20 * MB,
                                                                  'max-memory-for-unlimited-query': 20 * MB,
                                                                  'compaction_large_row_warning_threshold_mb': 20 * MB,
                                                                  'compaction_large_cell_warning_threshold_mb': 20 * MB,
                                                                  'write-request-timeout-in-ms': 1000})
        return dtest_setup_overrides

    def prepare_cluster(self, n: int) -> Tuple[ScyllaNode, Session]:
        self.populate_sequentially(n)
        node: ScyllaNode = self.cluster.nodelist()[0]
        session: Session = self.patient_cql_connection(node, request_timeout=120)
        create_ks(session, "ks", n)
        return (node, session)

    def test_single_column_blob_max_size_with_cdc_preimage_full_postimage(self):
        """Test blob column with max size

        Run mutations with blob size close to limit and validate
        that there is no reactor stalls found
        """
        node, session = self.prepare_cluster(1)
        create_table_stmt = "CREATE TABLE ks.cf (pk bigint, ck bigint, v blob, PRIMARY KEY (pk, ck)) \
                             WITH cdc={'enabled': true, 'preimage': 'full', 'postimage': true}"
        session.execute(create_table_stmt)

        insert_value = bytes("1".encode()) * 7 * MB
        insert_statement = SimpleStatement("INSERT INTO ks.cf (pk, ck, v) VALUES (%(pk)s, %(ck)s, %(v)s)")
        insert_parameters = [{"pk": i, "ck": j, "v": insert_value} for i in range(10) for j in range(5)]

        update_value = bytes("2".encode()) * 4 * MB
        update_statement = SimpleStatement("UPDATE ks.cf set v = %(v)s where pk = %(pk)s and ck=%(ck)s")
        update_parameters = [{"pk": i, "ck": j, "v": update_value} for i in range(10) for j in range(5)]

        self.execute_case(node, session,
                          insert_data={
                              "insert_statement": insert_statement,
                              "insert_parameters": insert_parameters
                          },
                          update_data={
                              "update_statement": update_statement,
                              "update_parameters": update_parameters
                          })

    def test_row_with_several_columns_of_blobs_with_cdc_preimage_full_postimage(self):
        """test row with several columns of blob type

        Construct row with several columns of blob type and populate
        each column with large blob size, so the total size of mutation
        was close to limit 16MB

        Because cdc feature is enabled the base row size should be ~8MB

        """
        NUM_CELLS = 5
        VALUE = bytes("1".encode()) * 1 * MB
        node, session = self.prepare_cluster(1)

        cells = ", ".join([f"v_{i} blob" for i in range(NUM_CELLS)])
        session.execute(
            f"CREATE TABLE IF NOT EXISTS ks.cf (pk bigint, ck bigint, {cells}, PRIMARY KEY (pk, ck)) \
                WITH cdc={{'enabled': true, 'preimage': 'full', 'postimage': true}}")

        insert_cells_names = ", ".join([f"v_{i}" for i in range(NUM_CELLS)])
        insert_cells_values = ", ".join([f"%(v_{i})s" for i in range(NUM_CELLS)])
        insert_statement = SimpleStatement(f"INSERT INTO ks.cf (pk, ck, {insert_cells_names}) \
                                             VALUES (%(pk)s, %(ck)s, {insert_cells_values})")
        blob_columns = {f"v_{i}": VALUE for i in range(NUM_CELLS)}
        insert_parameters = [{**{"pk": i, "ck": j}, **blob_columns} for i in range(10) for j in range(10)]

        update_cells = ", ".join([f"v_{i} = %(v_{i})s" for i in range(NUM_CELLS)])
        update_statement = SimpleStatement(f"UPDATE ks.cf SET {update_cells} WHERE pk = %(pk)s and ck = %(ck)s")
        update_parameters = [{**{"pk": i, "ck": j}, **blob_columns} for i in range(10) for j in range(10)]

        self.execute_case(node, session,
                          insert_data={
                              "insert_statement": insert_statement,
                              "insert_parameters": insert_parameters
                          },
                          update_data={
                              "update_statement": update_statement,
                              "update_parameters": update_parameters
                          })

    def test_large_blob_in_map_delta_preimage_full(self):
        """test map type with large blob

        Test column with map type where one of the field
        is blob. Populate row with blob size close to mutation
        limit 16MB.
        """
        node, session = self.prepare_cluster(1)
        session.execute(
            "CREATE TABLE IF NOT EXISTS ks.cf (pk bigint, ck bigint, v map<text,blob>, PRIMARY KEY (pk, ck)) \
                WITH cdc={'enabled': true, 'preimage': 'full', 'postimage': true}")

        insert_value = bytes("1".encode()) * 1 * MB
        insert_statement = SimpleStatement("INSERT INTO ks.cf (pk, ck, v) VALUES (%(pk)s, %(ck)s, %(v)s)")
        insert_parameters = [{"pk": i, "ck": j, "v": {"key": insert_value}} for i in range(10) for j in range(5)]

        update_value = bytes("2".encode()) * 1 * MB
        update_statement = SimpleStatement("UPDATE ks.cf SET v = v + %(v)s WHERE pk=%(pk)s and ck=%(ck)s")
        update_parameters = [{"pk": i, "ck": j, "v": {f"key{k}": update_value}}
                             for i in range(10) for j in range(5) for k in range(5)]

        self.execute_case(node, session,
                          insert_data={
                              "insert_statement": insert_statement,
                              "insert_parameters": insert_parameters
                          },
                          update_data={
                              "update_statement": update_statement,
                              "update_parameters": update_parameters
                          })

    def execute_case(self, node: ScyllaNode, session: Session,
                     insert_data: Dict[str, Any], update_data: Dict[str, Any]):

        select_statement = SimpleStatement("SELECT * FROM ks.cf WHERE pk = %(pk)s and ck = %(ck)s")
        select_parameters = [{"pk": i, "ck": j} for i in range(10) for j in range(5)]

        cdc_select_statement = SimpleStatement("SELECT * FROM ks.cf_scylla_cdc_log LIMIT 10")

        mark = node.mark_log()
        self.execute_query(session, insert_data["insert_statement"], insert_data["insert_parameters"])
        found = node.grep_log("Reactor stall", from_mark=mark)
        assert not found, f"{found}"

        mark = node.mark_log()

        futures = []
        with ThreadPoolExecutor(max_workers=3) as executor:
            future = executor.submit(self.execute_query_during_timeout, session, update_data["update_statement"],
                                     update_data["update_parameters"], 60, 0.5)
            futures.append(future)
            future = executor.submit(self.execute_query_during_timeout, session,
                                     select_statement, select_parameters, 60, 0.5)
            futures.append(future)
            future = executor.submit(self.execute_query_during_timeout, session=session, statement=cdc_select_statement,
                                     duration=60, delay=0.5)
            futures.append(future)

            for future in futures:
                exc = future.exception()
                if exc:
                    print(str(exc))
                    raise exc

        found = node.grep_log("Reactor stall", from_mark=mark)
        assert not found, f"Next Reactor stalls were found: {found}"

        found = node.grep_log("oversized allocation", from_mark=mark)
        assert not found, f"Next oversized allocation were found: {found}"

        found = node.grep_log_for_errors()
        assert not found, f"Next errors were found: {found}"

    @staticmethod
    def execute_query(session: Session, statement: SimpleStatement, parameters: List[Any],
                      delay: Union[int, float] = 0.0, raise_exception: bool = False):
        for param in parameters:
            try:
                session.execute(statement, param)
            except Exception as details:
                LOGGER.error("Error: %s", details)
                if raise_exception:
                    raise details

            time.sleep(delay)

    @staticmethod
    def execute_query_during_timeout(session: Session, statement: SimpleStatement, parameters: Optional[List[Any]] = None,
                                     duration: Optional[float] = None, delay: Union[int, float] = 0.0, raise_exception: bool = False):
        start_timestampt = current_time = time.time()
        if not parameters:
            parameters = [None]
        while current_time < start_timestampt + duration:
            for param in parameters:
                try:
                    session.execute(statement, param)
                except Exception as details:
                    LOGGER.error("Error: %s", details)
                    if raise_exception:
                        raise details
                time.sleep(delay)
            current_time = time.time()
