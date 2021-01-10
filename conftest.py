import pytest
import logging
import os
import re
import platform
import copy
import inspect
from itertools import zip_longest
from datetime import datetime
from distutils.version import LooseVersion

from psutil import virtual_memory
from botocore.exceptions import ClientError as AwsClientError
import netifaces as ni
from netifaces import AF_INET  # pylint: disable=no-name-in-module

import ccmlib.repository
from ccmlib.common import validate_install_dir, get_version_from_build

from dtest_class import running_in_docker, cleanup_docker_environment_before_test_execution
from dtest_config import DTestConfig
from dtest_setup import DTestSetup, copy_logs
from dtest_setup_overrides import DTestSetupOverrides
from tools.keystore import KeyStore
from tools.log_utils import log_per_process_data, TestNameFilter

logger = logging.getLogger(__name__)


def check_required_loopback_interfaces_available():
    """
    We need at least 3 loopback interfaces configured to run almost all dtests. On Linux, loopback
    interfaces are automatically created as they are used, but on Mac they need to be explicitly
    created. Check if we're running on Mac (Darwin), and if so check we have at least 3 loopback
    interfaces available, otherwise bail out so we don't run the tests in a known bad config and
    give the user some helpful advice on how to get their machine into a good known config
    """
    if platform.system() == "Darwin":
        if len(ni.ifaddresses('lo0')[AF_INET]) < 9:
            pytest.exit("At least 9 loopback interfaces are required to run dtests. "
                        "On Mac you can create the required loopback interfaces by running "
                        "'for i in {1..9}; do sudo ifconfig lo0 alias 127.0.0.$i up; done;'")


def pytest_addoption(parser):
    parser.addoption("--use-vnodes", action="store_true", default=True,
                     help="Determines wither or not to setup clusters using vnodes for tests")
    parser.addoption("--use-off-heap-memtables", action="store_true", default=False,
                     help="Enable Off Heap Memtables when creating test clusters for tests")
    parser.addoption("--num-tokens", action="store", default=256,
                     help="Number of tokens to set num_tokens yaml setting to when creating instances "
                          "with vnodes enabled")
    parser.addoption("--data-dir-count-per-instance", action="store", default=1,
                     help="Control the number of data directories to create per instance")
    parser.addoption("--force-resource-intensive-tests", action="store_true", default=False,
                     help="Forces the execution of tests marked as resource_intensive")
    parser.addoption("--skip-resource-intensive-tests", action="store_true", default=False,
                     help="Skip all tests marked as resource_intensive")
    parser.addoption("--cassandra-dir", action="store", default=None,
                     help="The directory containing the built C* artifacts to run the tests against. "
                          "(e.g. the path to the root of a cloned C* git directory. Before executing dtests using "
                          "this directory you must build C* with 'ant clean jar'). If you're doing C* development and "
                          "want to run the tests this is almost always going to be the correct option.")
    parser.addoption("--cassandra-version", action="store", default=None,
                     help="A specific C* version to run the dtests against. The dtest framework will "
                          "pull the required artifacts for this version.")
    parser.addoption("--delete-logs", action="store_true", default=False,
                     help="Delete all generated logs created by a test after the completion of a test.")
    parser.addoption("--execute-upgrade-tests", action="store_true", default=False,
                     help="Execute Cassandra Upgrade Tests (e.g. tests annotated with the upgrade_test mark)")
    parser.addoption("--disable-active-log-watching", action="store_true", default=False,
                     help="Disable ccm active log watching, which will cause dtests to check for errors in the "
                          "logs in a single operation instead of semi-realtime processing by consuming "
                          "ccm _log_error_handler callbacks")
    parser.addoption("--keep-test-dir", action="store_true", default=False,
                     help="Do not remove/cleanup the test ccm cluster directory and it's artifacts "
                          "after the test completes")
    parser.addoption("--enable-jacoco-code-coverage", action="store_true", default=False,
                     help="Enable JaCoCo Code Coverage Support")
    parser.addoption("--upgrade-version-selection", action="store", default="indev",
                     help="Specify whether to run indev, releases, or both")
    parser.addoption("--scylla-version", action="store", default=None,
                     help="Scylla relocatable version ex: unstable/master:239")


def pytest_configure(config):
    # putting those here since we use this in CCM before even starting any tests,
    # so it's not enough to put it in a function fixture, since some code would be used even before that
    logging.getLogger("boto3").setLevel(logging.INFO)
    logging.getLogger("botocore").setLevel(logging.INFO)


def sufficient_system_resources_for_resource_intensive_tests():
    mem = virtual_memory()
    total_mem_gb = mem.total/1024/1024/1024
    logger.info("total available system memory is %dGB" % total_mem_gb)
    # todo kjkj: do not hard code our bound.. for now just do 9 instances at 3gb a piece
    return total_mem_gb >= 9*3


@pytest.fixture(scope='function', autouse=True)
def fixture_dtest_setup_overrides(dtest_config):
    """
    no-op default implementation of fixture_dtest_setup_overrides.
    we run this when a test class hasn't implemented their own
    fixture_dtest_setup_overrides
    """
    return DTestSetupOverrides()


@pytest.fixture(scope='function')
def fixture_dtest_cluster_name():
    """
    :return: The name to use for the running test's cluster
    """
    return "test"


r"""
Not exactly sure why :\ but, this fixture needs to be scoped to function level and not
session or class. If you invoke pytest with tests across multiple test classes, when scopped
at session, the root logger appears to get reset between each test class invocation.
this means that the first test to run not from the first test class (and all subsequent
tests), will have the root logger reset and see a level of NOTSET. Scoping it at the
class level seems to work, and I guess it's not that much extra overhead to setup the
logger once per test class vs. once per session in the grand scheme of things.
"""


@pytest.fixture(scope="function", autouse=True)
def fixture_logging_setup(request):
    logging_plugin = request.config.pluginmanager.get_plugin("logging-plugin")

    # adding name of the test to the print of logs
    name_filer = TestNameFilter()
    logging_plugin.log_file_handler.addFilter(name_filer)

    # configure the error logger to go only to log file
    if 'error_logger' not in log_per_process_data:
        error_logger = logging.getLogger("errors")
        for handler in error_logger.handlers:
            error_logger.removeHandler(handler)
        error_logger.addHandler(logging_plugin.log_file_handler)
        log_per_process_data['error_logger'] = error_logger

    logging.getLogger("cassandra").setLevel(logging.INFO)
    logging.getLogger("boto3").setLevel(logging.INFO)
    logging.getLogger("botocore").setLevel(logging.INFO)

    yield

    logging_plugin.log_file_handler.removeFilter(name_filer)


def pytest_runtest_logreport(report):
    """
    print pytest backtraces of failures to the logs
    """
    def get_message():
        if hasattr(report, "longreprtext"):
            message = report.longreprtext
        elif hasattr(report.longrepr, "reprcrash"):
            message = report.longrepr.reprcrash.message
        elif isinstance(report.longrepr, str):
            message = report.longrepr
        else:
            message = str(report.longrepr)
        return message

    if report.failed:
        if 'error_logger' in log_per_process_data:
            log_per_process_data['error_logger'].error(f"test failed: \n{get_message()}")


@pytest.fixture(scope="session")
def log_global_env_facts(request, fixture_dtest_config, fixture_logging_setup):
    if request.config.pluginmanager.hasplugin('junitxml'):
        my_junit = getattr(request.config, '_xml', None)
        my_junit.add_global_property('USE_VNODES', fixture_dtest_config.use_vnodes)


@pytest.fixture(scope='function', autouse=True)
def fixture_maybe_skip_tests_requiring_novnodes(request):
    """
    Fixture run before the start of every test function that checks if the test is marked with
    the no_vnodes annotation but the tests were started with a configuration that
    has vnodes enabled. This should always be a no-op as we explicitly deselect tests
    in pytest_collection_modifyitems that match this configuration -- but this is explicit :)
    """
    if request.node.get_closest_marker('no_vnodes'):
        if request.config.getoption("--use-vnodes"):
            pytest.skip("Skipping test marked with no_vnodes as tests executed with vnodes enabled via the "
                        "--use-vnodes command line argument")


@pytest.fixture(scope='function', autouse=True)
def fixture_log_test_name_and_date(request, fixture_logging_setup):
    logger.info("Starting execution of %s at %s" % (request.node.name, str(datetime.now())))


def _filter_errors(dtest_setup, errors):
    """Filter errors, removing those that match ignore_log_patterns in the current DTestSetup"""
    for e in errors:
        for pattern in dtest_setup.ignore_log_patterns:
            if re.search(pattern, repr(e)):
                break
        else:
            yield e


def check_logs_for_errors(dtest_setup):
    errors = []
    for node in dtest_setup.cluster.nodelist():
        errors = list(_filter_errors(dtest_setup, ['\n'.join(msg) for msg in node.grep_log_for_errors()]))
        if len(errors) is not 0:
            for error in errors:
                if isinstance(error, (bytes, bytearray)):
                    error_str = error.decode("utf-8").strip()
                else:
                    error_str = error.strip()

                if error_str:
                    logger.error("Unexpected error in {node_name} log, error: \n{error}"
                                 .format(node_name=node.name, error=error_str))
                    errors.append(error_str)
                    break
    return errors


def reset_environment_vars(initial_environment):
    pytest_current_test = os.environ.get('PYTEST_CURRENT_TEST')
    os.environ.clear()
    os.environ.update(initial_environment)
    os.environ['PYTEST_CURRENT_TEST'] = pytest_current_test


@pytest.fixture(scope='function')
def fixture_dtest_create_cluster_func():
    """
    :return: A function whose sole argument is a DTestSetup instance and returns an
             object that operates with the same interface as ccmlib.Cluster.
    """
    return DTestSetup.create_ccm_cluster


@pytest.fixture(scope='function', autouse=False)
def fixture_dtest_setup(request,
                        dtest_config,
                        fixture_dtest_setup_overrides,
                        fixture_logging_setup,
                        fixture_dtest_cluster_name,
                        fixture_dtest_create_cluster_func):
    if running_in_docker():
        cleanup_docker_environment_before_test_execution()

    # do all of our setup operations to get the enviornment ready for the actual test
    # to run (e.g. bring up a cluster with the necessary config, populate variables, etc)
    initial_environment = copy.deepcopy(os.environ)
    dtest_setup = DTestSetup(dtest_config=dtest_config,
                             setup_overrides=fixture_dtest_setup_overrides,
                             cluster_name=fixture_dtest_cluster_name)
    dtest_setup.initialize_cluster(fixture_dtest_create_cluster_func)

    if request.node.get_closest_marker('single_node'):
        dtest_setup.cluster.set_configuration_options(values={'skip_wait_for_gossip_to_settle': 0})

    # Reduce waiting time for the nodes to hear from others before joining the ring.
    # Since all test cases run on localhost and there are no large test clusters
    # it's safe to reduce the value to save a lot of time while testing.
    # (Default value for the option is 30s)
    dtest_setup.cluster.set_configuration_options(values={'ring_delay_ms': 10000})

    # scylla-ccm doesn't support this
    # if not dtest_config.disable_active_log_watching:
    #    dtest_setup.begin_active_log_watch()

    # at this point we're done with our setup operations in this fixture
    # yield to allow the actual test to run
    yield dtest_setup

    # phew! we're back after executing the test, now we need to do
    # all of our teardown and cleanup operations

    reset_environment_vars(initial_environment)
    dtest_setup.jvm_args = []

    for con in dtest_setup.connections:
        con.cluster.shutdown()
    dtest_setup.connections = []

    failed = False
    try:
        if not dtest_setup.allow_log_errors:
            errors = check_logs_for_errors(dtest_setup)
            if len(errors) > 0:
                failed = True
                pytest.fail(msg='Unexpected error found in node logs (see stdout for full details). Errors: [{errors}]'
                            .format(errors=str.join(", ", errors)), pytrace=False)
    finally:
        try:
            # save the logs for inspection
            if failed or not dtest_config.delete_logs:
                copy_logs(request, dtest_setup)
        except Exception as e:
            logger.error("Error saving log: %s", str(e))
        finally:
            dtest_setup.cleanup_cluster()


# Based on https://bugs.python.org/file25808/14894.patch
def loose_version_compare(a, b):
    for i, j in zip_longest(a.version, b.version, fillvalue=''):
        if type(i) != type(j):
            i = str(i)
            j = str(j)
        if i == j:
            continue
        elif i < j:
            return -1
        else:  # i > j
            return 1

    # Longer version strings with equal prefixes are equal, but if one version string is longer than it is greater
    aLen = len(a.version)
    bLen = len(b.version)
    if aLen == bLen:
        return 0
    elif aLen < bLen:
        return -1
    else:
        return 1


def _skip_msg(current_running_version, since_version, max_version):
    if loose_version_compare(current_running_version, since_version) < 0:
        return "%s < %s" % (current_running_version, since_version)
    if max_version and loose_version_compare(current_running_version, max_version) > 0:
        return "%s > %s" % (current_running_version, max_version)


@pytest.fixture(autouse=True)
def fixture_since(request, fixture_dtest_setup):
    if request.node.get_closest_marker('since'):
        max_version_str = request.node.get_closest_marker('since').kwargs.get('max_version', None)
        max_version = None
        if max_version_str:
            max_version = LooseVersion(max_version_str)

        since_str = request.node.get_closest_marker('since').args[0]
        since = LooseVersion(since_str)
        # For upgrade tests don't run the test if any of the involved versions
        # are excluded by the annotation
        if hasattr(request.cls, "UPGRADE_PATH"):
            upgrade_path = request.cls.UPGRADE_PATH
            ccm_repo_cache_dir, _ = ccmlib.repository.setup(upgrade_path.starting_meta.version)
            starting_version = get_version_from_build(ccm_repo_cache_dir)
            skip_msg = _skip_msg(starting_version, since, max_version)
            if skip_msg:
                pytest.skip(skip_msg)
            ccm_repo_cache_dir, _ = ccmlib.repository.setup(upgrade_path.upgrade_meta.version)
            ending_version = get_version_from_build(ccm_repo_cache_dir)
            skip_msg = _skip_msg(ending_version, since, max_version)
            if skip_msg:
                pytest.skip(skip_msg)
        else:
            # For regular tests the value in the current cluster actually means something so we should
            # use that to check.
            # Use cassandra_version_from_build as it's guaranteed to be a LooseVersion
            # whereas cassandra_version may be a string if set in the cli options
            current_running_version = LooseVersion(str(fixture_dtest_setup.dtest_config.cassandra_version_from_build))
            skip_msg = _skip_msg(current_running_version, since, max_version)
            if skip_msg:
                pytest.skip(skip_msg)


@pytest.fixture(autouse=True)
def fixture_skip_version(request, fixture_dtest_setup):
    marker = request.node.get_closest_marker('skip_version')
    if marker is not None:
        version_to_skip = LooseVersion(marker.args[0])
        if version_to_skip == fixture_dtest_setup.dtest_config.cassandra_version_from_build:
            pytest.skip("Test marked not to run on version %s" % version_to_skip)


@pytest.fixture(scope='session', autouse=True)
def install_debugging_signal_handler():
    import faulthandler
    faulthandler.enable()


@pytest.fixture(scope='session')
def dtest_config(request):
    dtest_config = DTestConfig()
    dtest_config.setup(request)

    # if we're on mac, check that we have the required loopback interfaces before doing anything!
    check_required_loopback_interfaces_available()

    try:
        if dtest_config.cassandra_dir is not None:
            validate_install_dir(dtest_config.cassandra_dir)
    except Exception as e:
        pytest.exit("{}. Did you remember to build C*? ('ant clean jar')".format(e))

    yield dtest_config


def pytest_collection_modifyitems(items, config):
    """
    This function is called upon during the pytest test collection phase and allows for modification
    of the test items within the list
    """
    collect_only = config.getoption("--collect-only")
    cassandra_dir = config.getoption("--cassandra-dir")
    cassandra_version = config.getoption("--cassandra-version")
    scylla_version = config.getoption('--scylla-version')

    if not scylla_version:
        if not collect_only and cassandra_dir is None:
            if cassandra_version is None:
                raise Exception("Required dtest arguments were missing! You must provide either --cassandra-dir "
                                "or --cassandra-version. Refer to the documentation or invoke the help with --help.")

        # Either cassandra_version or cassandra_dir is defined, so figure out the version
        CASSANDRA_VERSION = cassandra_version or get_version_from_build(cassandra_dir)

        # Check that use_off_heap_memtables is supported in this c* version
        if config.getoption("--use-off-heap-memtables") and ("3.0" <= CASSANDRA_VERSION < "3.4"):
            raise Exception("The selected Cassandra version %s doesn't support the provided option "
                            "--use-off-heap-memtables, see https://issues.apache.org/jira/browse/CASSANDRA-9472 "
                            "for details" % CASSANDRA_VERSION)

    selected_items = []
    deselected_items = []

    sufficient_system_resources_resource_intensive = sufficient_system_resources_for_resource_intensive_tests()
    logger.debug("has sufficient resources? %s" % sufficient_system_resources_resource_intensive)

    for item in items:
        deselect_test = False

        if item.get_closest_marker("resource_intensive") and not collect_only:
            force_resource_intensive = config.getoption("--force-resource-intensive-tests")
            skip_resource_intensive = config.getoption("--skip-resource-intensive-tests")
            if not force_resource_intensive:
                if skip_resource_intensive:
                    deselect_test = True
                    logger.info("SKIP: Deselecting test %s as test marked resource_intensive. To force execution of "
                                "this test re-run with the --force-resource-intensive-tests command line argument" % item.name)
                if not sufficient_system_resources_resource_intensive:
                    deselect_test = True
                    logger.info("SKIP: Deselecting resource_intensive test %s due to insufficient system resources" % item.name)

        if item.get_closest_marker("no_vnodes"):
            if config.getoption("--use-vnodes"):
                deselect_test = True
                logger.info("SKIP: Deselecting test %s as the test requires vnodes to be disabled. To run this test, "
                            "re-run without the --use-vnodes command line argument" % item.name)

        if item.get_closest_marker("vnodes"):
            if not config.getoption("--use-vnodes"):
                deselect_test = True
                logger.info("SKIP: Deselecting test %s as the test requires vnodes to be enabled. To run this test, "
                            "re-run with the --use-vnodes command line argument" % item.name)

        for test_item_class in inspect.getmembers(item.module, inspect.isclass):
            if not hasattr(test_item_class[1], "pytestmark"):
                continue

            for module_pytest_mark in test_item_class[1].pytestmark:
                if module_pytest_mark.name == "upgrade_test":
                    if not config.getoption("--execute-upgrade-tests"):
                        deselect_test = True

        if item.get_closest_marker("upgrade_test"):
            if not config.getoption("--execute-upgrade-tests"):
                deselect_test = True

        if item.get_closest_marker("no_offheap_memtables"):
            if config.getoption("use_off_heap_memtables"):
                deselect_test = True

        # temporarily deselect tests in cqlsh_copy_tests that depend on cqlshlib,
        # until cqlshlib is Python 3 compatibile
        if item.get_closest_marker("depends_cqlshlib"):
            deselect_test = True

        if deselect_test:
            deselected_items.append(item)
        else:
            selected_items.append(item)

    config.hook.pytest_deselected(items=deselected_items)
    items[:] = selected_items


@pytest.fixture(scope='session', autouse=True)
def configure_es(elk_reporter, dtest_config):
    extra_data = {
        "SCYLLA_FULL_VERSION": dtest_config.scylla_full_version,
        "SCYLLA_BRANCH_VERSION":  dtest_config.cassandra_version_from_build,
    }
    elk_reporter.session_data.update(**extra_data)

    # if we don't have the credentials just skip this part
    try:
        es_credentials = KeyStore().get_elasticsearch_credentials()
    except AwsClientError as ex:
        logger.warning("couldn't configure configure_es, results won't be sent out:")
        logger.warning("%s", str(ex))
        return

    elk_reporter.es_address = es_credentials['es_url']
    elk_reporter.es_username = es_credentials['es_user']
    elk_reporter.es_password = es_credentials['es_password']
    elk_reporter.es_index_name = 'dtest_test_data'
