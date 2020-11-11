from pprint import pformat

from alternator.utils import enums
from alternator_utils import TesterAlternatorStream, NUM_OF_ITEMS


class AlternatorStreamsTest(TesterAlternatorStream):
    def test_verify_all_nodes_have_same_stream(self):
        num_of_items = NUM_OF_ITEMS
        self.prepare_dynamodb_cluster(num_of_nodes=3)
        node1 = self.cluster.nodelist()[0]
        stream_arn = self.prefill_dynamodb_table(
            node=node1, stream_specification=enums.StreamSpecification.KEYS_ONLY.value, num_of_items=num_of_items)[0]
        expected_items = [{self._table_primary_key: item[self._table_primary_key]} for item in self.create_items()]

        for node in self.cluster.nodelist():
            records = self.get_records(node=node, stream_arn=stream_arn, num_of_requests=2 * num_of_items)
            diff = self.compare_table_data(expected_table_data=expected_items, table_data=records)
            assert not diff, f"The following keys are missing '{pformat(diff)}'"
