import pytest

from dtest_class import Tester, create_ks, create_cf
from tools.data import putget


@pytest.mark.dtest_full
class TestMultiDCPutGet(Tester):

    @pytest.mark.next_gating
    @pytest.mark.dtest_debug
    def test_putget_2dc_rf1(self):
        """ Simple put-get test for 2 DC with one node each (RF=1) [catches #3539] """
        cluster = self.cluster
        cluster.populate([1, 1]).start()

        session = self.patient_cql_connection(cluster.nodelist()[0])
        create_ks(session=session, name='ks', rf={'dc1': 1, 'dc2': 1})
        create_cf(session=session, name='cf')

        putget(cluster, session)

    def test_putget_2dc_rf2(self):
        """ Simple put-get test for 2 DC with 2 node each (RF=2) -- tests cross-DC efficient writes """
        cluster = self.cluster
        cluster.populate([2, 2]).start()

        session = self.patient_cql_connection(cluster.nodelist()[0])
        create_ks(session=session, name='ks', rf={'dc1': 2, 'dc2': 2})
        create_cf(session=session, name='cf')

        putget(cluster, session)
