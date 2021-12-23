import pytest
from cassandra import ConsistencyLevel
from cassandra.cluster import Session

from dtest_class import Tester, create_ks, create_cf
from tools.assertions import assert_one, assert_none


@pytest.mark.dtest_full
class TestMultiDCAdditionalTests(Tester):

    def test_query_dc_with_rf_0_does_not_crash_db(self):
        """Test querying dc with CL=LOCAL_QUORUM when RF=0 for this dc, does not crash the node and returns None
        Covers https://github.com/scylladb/scylla/issues/8354"""
        cluster = self.cluster
        cluster.populate([1, 1]).start()
        node1 = cluster.nodelist()[0]

        session_dc1: Session = self.patient_cql_connection(cluster.nodelist()[0])
        session_dc2: Session = self.patient_cql_connection(cluster.nodelist()[1])
        create_ks(session=session_dc1, name='ks', rf={'dc1': 1, 'dc2': 0})
        create_cf(session_dc1, 'cf', columns={'c1': 'text'})

        session_dc1.execute(
            "INSERT INTO  ks.cf (key, c1) VALUES ('k1', 'value1');")
        node1.flush()
        assert_one(session_dc1, "SELECT * from ks.cf;", ['k1', 'value1'], cl=ConsistencyLevel.ONE)
        assert_none(session_dc2, "SELECT * from ks.cf;", cl=ConsistencyLevel.LOCAL_QUORUM)  # crashes node for v4.3.2
