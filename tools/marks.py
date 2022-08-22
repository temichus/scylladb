import pkg_resources
from pkg_resources import parse_version, DistributionNotFound, VersionConflict

import pytest
from cassandra.connection import DRIVER_NAME, DRIVER_VERSION

from dtest_config import DTestConfig


def scylla_mode(cassandra_dir, scylla_version):
    dtest_config = DTestConfig()
    dtest_config.cassandra_dir = cassandra_dir
    dtest_config.scylla_version = scylla_version
    return dtest_config.get_scylla_mode()


def get_version(cassandra_dir, scylla_version):
    dtest_config = DTestConfig()
    dtest_config.cassandra_dir = cassandra_dir
    dtest_config.scylla_version = scylla_version
    return parse_version(dtest_config.get_version_from_build())


def is_enterprise(cassandra_dir, scylla_version):
    dtest_config = DTestConfig()
    dtest_config.cassandra_dir = cassandra_dir
    dtest_config.scylla_version = scylla_version
    return dtest_config.is_enterprise


def enterprise_only_param(*param):
    return pytest.param(*param, marks=pytest.mark.skipif("get_version(config.getvalue('--cassandra-dir'), "
                                                         "config.getvalue('--scylla-version')) < parse_version('2018.1')",
                                                         reason=f"'{param}' is supported only in enterprise version"))


def required_driver(*driver_requirements):
    """
    marker for setting required version of the driver for specific test

    @required_driver('scylla-driver>=3.24.5')
    def test_01():
        pass

    # pass a list, when test depends on different version on different drivers
    # keep in mind this list is used with OR only
    @required_driver('scylla-driver>=3.25.0', 'cassandra-driver==3.25.0')
    def test_01():
        pass

    """
    for req in driver_requirements:
        assert 'scylla-driver' in req or 'cassandra-driver' in req, \
            "required_driver() is for cql drivers only"

    def check_requirement(requirement):
        # since both versions can be installed at the same time, we
        # first cross-check in the actual code the name of the driver
        is_scylla_driver = 'scylla' in DRIVER_NAME.lower()
        if 'scylla' in requirement and not is_scylla_driver:
            return False
        if 'scylla' not in requirement and is_scylla_driver:
            return False
        try:
            pkg_resources.require(requirement)
            return True
        except (DistributionNotFound, VersionConflict):
            return False

    outcome = check_requirement(driver_requirements[0])
    for req in driver_requirements[1:]:
        outcome |= check_requirement(req)

    return pytest.mark.skipif(not outcome, reason=f"test expected: {driver_requirements}\n"
                                                  f"installed: {DRIVER_VERSION} - {DRIVER_NAME}")
