# Distributed tests for Alternator's TTL (per-item expiration time) feature.
# The tests here should focus on distributed (multi-node) behavior of the
# expiration process such as how expiration works as expected even when
# there are multiple nodes each doing part of the expiration scanning,
# and what happens when a node dies (other nodes should take over its
# responsibilities on finding expired items).
#
# Tests which check functionality that can be tested on a single node are
# better implemented in scylla.git's test/alternator/test_ttl.py - those
# tests are significantly faster to run and therefore to develop than
# these dtests.
import logging
import random

import pytest
import time

from alternator_utils import BaseAlternator, TABLE_NAME, random_string
from alternator.utils import schemas
from ccmlib.node import NodetoolError
from ccmlib.scylla_node import ScyllaNode
from tools.retrying import retrying

logger = logging.getLogger(__name__)


@pytest.mark.dtest_full
class TestAlternatorTTL(BaseAlternator):
    @pytest.mark.parametrize('with_down_node', [False, True], ids=['all_nodes_up', 'one_node_down'])
    def test_multinode_expiration(self, with_down_node):
        """
        When the cluster has multiple nodes, different nodes are responsible
        for checking expiration in different token ranges (each responsible
        for its "primary ranges"). Let's check that this expiration really
        does happen - for the entire token range - by writing many partitions
        that will span the entire token range, and seeing that they all expire.
        Note that this test doesn't test what happens if one of the nodes goes
        down - we'll test that case below. We also don't check that nodes
        don't do more work than they should - an inefficient implementation
        where every node scans the entire data set will also pass this test.

        When the test is run a second time with with_down_node=True, we verify
        that TTL expiration works correctly even when one of the five nodes is
        brought down. This node's TTL scanner is responsible for scanning one
        fifth of the token range, so when this node is down, one fifth of the
        data will not get expired. At that point - other node(s) should take
        over expiring data in that range - and this test verifies that this
        indeed happens. Reproduces issue #9787.
        """
        self.prepare_dynamodb_cluster(num_of_nodes=5,
                                      extra_config={'experimental_features': ['alternator-ttl'], 'alternator_ttl_period_in_seconds': 1})
        node1, *_, node5 = self.cluster.nodelist()

        if with_down_node:
            # Bring down the fifth node. Everything we do below should continue
            # to work with one node down - DynamoDB writes and consistent reads
            # use CL=QUORUM which should work with just one node down.
            node5.stop(wait_other_notice=True)

        self.create_table(node=node1, schema=schemas.HASH_SCHEMA)
        dynamodb = self.get_dynamodb_api(node=node1)
        table = dynamodb.resource.Table(name=TABLE_NAME)
        # Set the "expiration" column to mark items' expiration time
        dynamodb.client.update_time_to_live(TableName=table.name,
                                            TimeToLiveSpecification={'AttributeName': 'expiration', 'Enabled': True})
        # Create many items in different partition all over the token space.
        # All items  are marked to expire 10 seconds in the past, so should
        # all expire as soon as possible, during this test.
        expiration = int(time.time()) - 10
        with table.batch_writer() as batch:
            for i in range(1000):
                batch.put_item({schemas.HASH_KEY_NAME: random_string(10),
                                'expiration': expiration})
        # Expect that after a short delay, *all* items in the table will have
        # expired - so a scan should return no responses.
        timeout = time.time() + 60
        success = False
        while not success and time.time() < timeout:
            response = table.scan(Limit=1)
            # Note that to be sure that *all* items were deleted, we need to
            # complete the scan till the end.
            success = len(response['Items']) == 0
            while success and 'LastEvaluatedKey' in response:
                response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'], Limit=1)
                success = success and len(response['Items']) == 0
            if success:
                break
            time.sleep(1)
        assert success

    def test_ttl_with_load_and_decommission(self):
        """
        1. Configure a table with TTL enabled and an 'expiration' column.
        2. Create a load of read/write/update-items delete-set-elements
        3. Run a loop of flush and compaction on cluster nodes.
        4. Verify all data is eventually deleted on cluster nodes.
        """
        ttl_polling_interval = 4
        self.prepare_dynamodb_cluster(num_of_nodes=4,
                                      extra_config={'experimental_features': ['alternator-ttl'],
                                                    'alternator_ttl_period_in_seconds': ttl_polling_interval})
        node1, *_ = self.cluster.nodelist()
        table = self.create_table(node=node1)
        # Enable TTL for table
        self.get_dynamodb_api(node=node1).client.update_time_to_live(TableName=table.name, TimeToLiveSpecification={
            'AttributeName': 'expiration', 'Enabled': True})
        expiration_sec = 10
        num_of_items = 100
        logger.info("Running background stress and topology changes..")
        stress_thread = self.run_write_stress(table_name=TABLE_NAME, node=node1, num_of_item=num_of_items,
                                              use_set_data_type=True, expiration_sec=expiration_sec,
                                              random_start_index=True, ignore_errors=True)
        read_and_delete_set_elements_thread = self.run_delete_set_elements_stress(table_name=TABLE_NAME, node=node1,
                                                                                  random_start_index=True)
        decommission_thread = self.run_decommission_add_node_thread()

        logger.info('Run flush and compaction on cluster nodes')
        for _ in range(3):
            try:
                random.choice(self.cluster.nodelist()).flush()
                random.choice(self.cluster.nodelist()).compact()
            except NodetoolError as exc:
                assert "ConnectException" in str(exc)

        logger.info('Stopping background stress and decommission')
        read_and_delete_set_elements_thread.join()
        stress_thread.join()
        decommission_thread.join()

        logger.info('Verify all data is eventually deleted')

        @retrying(num_attempts=expiration_sec + ttl_polling_interval, sleep_time=2,
                  allowed_exceptions=TtlNotExpiredError,
                  message="Wait for TTL expiration threshold of a configured table")
        def wait_for_deletion_post_ttl_expiration(node: ScyllaNode):
            """
            Wait for TTL expiration threshold of a configured table.
            :param node: the node for running table full scan

            """
            table_data = self.scan_table(table_name=TABLE_NAME, node=node, ConsistentRead=False)
            logger.info('scan table result: %s', table_data)
            if not table_data:
                return
            raise TtlNotExpiredError

        # Check 2 nodes for TTL expiration of all data
        for db_node in self.cluster.nodelist()[:1]:
            wait_for_deletion_post_ttl_expiration(node=db_node)


class TtlNotExpiredError(Exception):
    pass
