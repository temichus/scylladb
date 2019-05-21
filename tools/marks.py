from pkg_resources import parse_version

import pytest

from dtest_config import DTestConfig


def get_version(cassandra_dir, scylla_version):
    dtest_config = DTestConfig()
    dtest_config.cassandra_dir = cassandra_dir
    dtest_config.scylla_version = scylla_version
    return parse_version(dtest_config.get_version_from_build())


def enterprise_only_param(*param):
    return pytest.param(*param, marks=pytest.mark.skipif("get_version(config.getvalue('--cassandra-dir'), "
                                                         "config.getvalue('--scylla-version')) < parse_version('2018.1')",
                                                         reason=f"'{param}' is supported only in enterprise version"))
