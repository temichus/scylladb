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

import pytest
import time

from alternator_utils import BaseAlternator, TABLE_NAME, random_string
from alternator.utils import schemas


@pytest.mark.dtest_full
class TestAlternatorTTL(BaseAlternator):
    def test_multinode_expiration(self):
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
        """
        self.prepare_dynamodb_cluster(num_of_nodes=5,
                                      extra_config={'experimental_features': ['alternator-ttl']})
        node1 = self.cluster.nodelist()[0]
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
            response = table.scan(Limit=1, ConsistentRead=True)
            # Note that to be sure that *all* items were deleted, we need to
            # complete the scan till the end.
            success = len(response['Items']) == 0
            while success and 'LastEvaluatedKey' in response:
                response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'], Limit=1, ConsistentRead=True)
                success = success and len(response['Items']) == 0
            if success:
                break
            time.sleep(1)
        assert success

    def test_expiration_with_down_node(self):
        """
        Verify that TTL expiration works correctly even when a node is down.
        Just like test_multinode_expiration above, also here we have data on
        five nodes which is all set to expire. However, in this test we bring
        one of the five nodes down. This node's TTL scanner is responsible for
        scanning one fifth of the token range, so when this node is down,
        one fifth of the data will not get expired. At that point - other
        node(s) should take over expiring data in that range - and this test
        verifies that this indeed happens.
        Reproduces issue #9787.
        """
        self.prepare_dynamodb_cluster(num_of_nodes=5,
                                      extra_config={'experimental_features': ['alternator-ttl']})

        # Bring down the fifth node. Everything we do below should continue
        # to work with one node down - DynamoDB writes and consistent reads
        # use CL=QUORUM which should work with just one node down.
        node5 = self.cluster.nodelist()[4]
        node5.stop(wait_other_notice=True)

        node1 = self.cluster.nodelist()[0]
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
