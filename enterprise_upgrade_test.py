import logging

import pytest

from rolling_upgrade_test import RollingUpgradeBase
from upgrade_test import upgrade_matrix_from_last_enterprise_release_version, BaseTests, \
    upgrade_matrix_enterprise_full_path

logger = logging.getLogger(__name__)


@pytest.mark.dtest_full
@pytest.mark.dtest_enterprise
class TestEnterpriseRollingUpgrade(RollingUpgradeBase):
    __test__ = True
    _multiprocess_can_split_ = False
    upgrade_path = upgrade_matrix_from_last_enterprise_release_version
    init_version = upgrade_path[0]


@pytest.mark.dtest_full
@pytest.mark.dtest_enterprise
class TestEnterpriseUpgradeBetweenReleases(BaseTests):
    __test__ = True
    _multiprocess_can_split_ = False
    upgrade_path = upgrade_matrix_enterprise_full_path
    init_version = upgrade_path[0]

    @pytest.mark.skip("skip the test for this matrix")
    def test_one_node_upgrade(self):
        pass
