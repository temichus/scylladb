import pprint
import logging
import concurrent.futures

import requests


LOGGER = logging.getLogger(__name__)


class ElkTestHistory:  # pylint: disable=too-many-instance-attributes
    def __init__(self, es_address, es_username, es_password, es_index_name, es_timeout=10):
        self.es_address = es_address
        self.es_username = es_username
        self.es_password = es_password
        self.es_index_name = es_index_name
        self.es_timeout = es_timeout

    @property
    def es_auth(self):
        return self.es_username, self.es_password

    @property
    def es_url(self):
        if self.es_address.startswith("http"):
            return "{0.es_address}".format(self)
        return "http://{0.es_address}".format(self)

    def fetch_test_outcomes(
            self, collected_test_list, max_workers=20, time_range='now-30d/d'
    ):
        """
        fetch test 95 percentile duration of a list of tests

        :param collected_test_list: the names of the test to lookup
        :param max_workers: number of threads to use for concurrency

        :returns: map from test_id to 95 percentile duration
        """

        test_outcomes = []
        session = requests.Session()

        def get_test_stats(test_id):
            url = "{0.es_url}/{0.es_index_name}/_search?size=0".format(self)
            body = {
                "query": {
                    "bool": {
                        "must": [
                            {
                                "query_string": {"query": f'(name:"{test_id}") AND '
                                                          f'(build_tag.keyword:jenkins-scylla-master-dtest-daily-* OR '
                                                          f'build_tag.keyword:jenkins-scylla-enterprise-dtest-daily-*) AND '
                                                          f'NOT build_tag.keyword:*daily-debug-*'},
                            }
                        ],
                        "filter": [
                            {
                                "range": {
                                    "timestamp": {
                                        "gte": time_range
                                    }
                                }
                            }],
                    }
                },

                "aggs": {
                    "test_outcomes": {
                        "terms": {
                            "field": "outcome.keyword"
                        }
                    }
                }
            }
            try:
                res = session.post(
                    url, json=body, auth=self.es_auth, timeout=self.es_timeout
                )
                res.raise_for_status()
                return dict(
                    test_name=test_id,
                    buckets=res.json()["aggregations"]["test_outcomes"][
                        "buckets"
                    ],
                )
            except (requests.exceptions.ReadTimeout, requests.exceptions.HTTPError) as exc:
                print(exc)
                return dict(test_name=test_id, buckets=None)

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_test_id = {
                executor.submit(get_test_stats, test_id): test_id
                for test_id in collected_test_list
            }
            for future in concurrent.futures.as_completed(future_to_test_id):
                test_id = future_to_test_id[future]
                try:
                    test_outcomes.append(future.result())
                except Exception:  # pylint: disable=broad-except
                    LOGGER.exception("'%s' generated an exception", test_id)

        LOGGER.debug(pprint.pformat(test_outcomes))

        return test_outcomes


if __name__ == "__main__":
    from tools.keystore import KeyStore
    es_credentials = KeyStore().get_elasticsearch_credentials()
    history = ElkTestHistory(es_address=es_credentials['es_url'],
                             es_username=es_credentials['es_user'],
                             es_password=es_credentials['es_password'],
                             es_index_name='dtest_test_data')
    t = history.fetch_test_outcomes(
        ['alternator_stream_tests.py::TestAlternatorStreams::test_sequence_numbers_during_add_decommission_node'])
    print(t)
