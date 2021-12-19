import os
import re
import time
import glob
import logging
import threading
import subprocess
import requests

import pytest
import cassandra
from flaky import flaky
from cassandra import ConsistencyLevel, OperationTimedOut
from cassandra.auth import PlainTextAuthProvider
from cassandra.policies import RetryPolicy
from cassandra.cluster import ExecutionProfile

from ccmlib.node import NodetoolError, TimeoutError


logger = logging.getLogger(__name__)
logger.debug("Python driver version in use: {}".format(cassandra.__version__))


class FlakyRetryPolicy(RetryPolicy):
    """
    A retry policy that retries 5 times
    """
    max_retries: int = 5

    def on_read_timeout(self, *args, **kwargs):
        if kwargs['retry_num'] < 5:
            logger.debug("Retrying read after timeout. Attempt #" + str(kwargs['retry_num']))
            return (self.RETRY, None)
        else:
            return (self.RETHROW, None)

    def on_write_timeout(self, *args, **kwargs):
        if kwargs['retry_num'] < 5:
            logger.debug("Retrying write after timeout. Attempt #" + str(kwargs['retry_num']))
            return (self.RETRY, None)
        else:
            return (self.RETHROW, None)

    def on_unavailable(self, *args, **kwargs):
        if kwargs['retry_num'] < 5:
            logger.debug("Retrying request after UE. Attempt #" + str(kwargs['retry_num']))
            return (self.RETRY, None)
        else:
            return (self.RETHROW, None)


class Runner(threading.Thread):

    def __init__(self, func, sleep=1.0):
        super().__init__()
        self.__func = func
        self.__error = None
        self.__stopped = False
        self.__sleep = sleep
        self.daemon = True

    def run(self):
        i = 0
        logging.debug("Runner: running {}".format(self.__func))
        while True:
            if self.__stopped:
                logging.debug("Runner: stopped {}".format(self.__func))
                return
            try:
                self.__func(i)
            except Exception as e:
                self.__error = e
                return
            i = i + 1
            if self.__sleep:
                time.sleep(self.__sleep)

    def stop(self, timeout=None):
        logging.debug("Runner: stopping {} (timeout={})".format(self.__func, timeout))
        self.__stopped = True
        try:
            self.join(timeout)
        except Exception as e:
            self.__error = e
        if self.__error is not None:
            raise self.__error

    def check(self):
        if self.__error is not None:
            raise self.__error


def make_execution_profile(retry_policy=FlakyRetryPolicy(), consistency_level=ConsistencyLevel.ONE, **kwargs):
    return ExecutionProfile(retry_policy=retry_policy,
                            consistency_level=consistency_level,
                            **kwargs)


def retry_till_success(fun, *args, **kwargs):
    timeout = kwargs.pop('timeout', 60)
    bypassed_exception = kwargs.pop('bypassed_exception', Exception)

    deadline = time.time() + timeout
    while True:
        try:
            return fun(*args, **kwargs)
        except bypassed_exception:
            if time.time() > deadline:
                raise
            else:
                # brief pause before next attempt
                time.sleep(0.25)


class WaitTimeoutExpired(Exception):
    pass


def forever_wait_for(func, step=1, text=None, **kwargs):
    """
    Wait indefinitely until func evaluates to True.

    This is similar to avocado.utils.wait.wait(), but there's no
    timeout, we'll just keep waiting for it.

    :param func: Function to evaluate.
    :param step: Amount of time to sleep before another try.
    :param text: Text to log, for debugging purposes.
    :param kwargs: Keyword arguments to func
    :return: Return value of func.
    """
    ok = False
    start_time = time.time()
    while not ok:
        ok = func(**kwargs)
        time.sleep(step)
        time_elapsed = time.time() - start_time
        if text is not None:
            logger.debug('{} ({} s)'.format(text, time_elapsed))
    return ok


def wait_for(func, step=1, text=None, timeout=None, throw_exc=True, **kwargs):
    """
    Wrapper function to wait with timeout option.
    If timeout received, avocado 'wait_for' method will be used.
    Otherwise the below function will be called.

    :param func: Function to evaluate.
    :param step: Time to sleep between attempts in seconds
    :param text: Text to print while waiting, for debug purposes
    :param timeout: Timeout in seconds
    :param throw_exc: Raise exception if timeout expired, but func result is not True
    :param kwargs: Keyword arguments to func
    :return: Return value of func.
    """
    if not timeout:
        return forever_wait_for(func, step, text, **kwargs)
    ok = False
    start_time = time.time()
    while not ok:
        time.sleep(step)
        ok = func(**kwargs)
        time_elapsed = time.time() - start_time
        if text is not None:
            logger.debug('({} ({} s)'.format(text, time_elapsed))
        if time_elapsed > timeout:
            err = 'Wait for: {}: timeout - {} seconds - expired'.format(text, timeout)
            logger.debug(err)
            if throw_exc:
                raise WaitTimeoutExpired(err)
    return ok


class DtestTimeoutError(Exception):
    pass


def create_ks(session, name, rf):
    query = 'CREATE KEYSPACE %s WITH replication={%s}'
    if isinstance(rf, int):
        # we assume simpleStrategy
        query = query % (name, "'class':'SimpleStrategy', 'replication_factor':%d" % rf)
    else:
        assert len(rf) >= 0, "At least one datacenter/rf pair is needed"
        # we assume networkTopologyStrategy
        options = ', '.join(['\'%s\':%d' % (dc_value, rf_value) for dc_value, rf_value in rf.items()])
        query = query % (name, "'class':'NetworkTopologyStrategy', %s" % options)

    try:
        retry_till_success(session.execute, query=query, timeout=120, bypassed_exception=cassandra.OperationTimedOut)
    except cassandra.AlreadyExists:
        logger.warning('AlreadyExists executing create ks query \'%s\'' % query)

    session.cluster.control_connection.wait_for_schema_agreement(wait_time=120)
    # Also validates it was indeed created even though we ignored OperationTimedOut
    # Might happen some of the time because CircleCI disk IO is unreliable and hangs randomly
    session.execute('USE {}'.format(name))


def get_auth_provider(user, password):
    return PlainTextAuthProvider(username=user, password=password)


def make_auth(user, password):
    def private_auth(node_ip):
        return {'username': user, 'password': password}

    return private_auth


def data_size(node, ks, cf):
    """
    Return the size in bytes for given table in a node.
    This gets the size from nodetool cfstats output.
    This might brake if the format of nodetool cfstats change
    as it is looking for specific text "Space used (total)" in output.
    @param node: Node in which table size to be checked for
    @param ks: Keyspace name for the table
    @param cf: table name
    @return: data size in bytes
    """
    cfstats = node.nodetool("cfstats {}.{}".format(ks, cf))[0]
    regex = re.compile(r'[\t]')
    stats_lines = [regex.sub("", s) for s in cfstats.split('\n')
                   if regex.sub("", s).startswith('Space used (total)')]
    if not len(stats_lines) == 1:
        msg = ('Expected output from `nodetool cfstats` to contain exactly 1 '
               'line starting with "Space used (total)". Found:\n') + cfstats
        raise RuntimeError(msg)
    space_used_line = stats_lines[0].split()

    if len(space_used_line) == 4:
        return float(space_used_line[3])
    else:
        msg = ('Expected format for `Space used (total)` in nodetool cfstats is `Space used (total): <number>`.'
               'Found:\n') + stats_lines[0]
        raise RuntimeError(msg)


def get_port_from_node(node):
    """
    Return the port that this node is listening on.
    We only use this to connect the native driver,
    so we only care about the binary port.
    """
    try:
        return node.network_interfaces['binary'][1]
    except Exception:
        raise RuntimeError("No network interface defined on this node object. {}".format(node.network_interfaces))


def get_ip_from_node(node):
    if node.network_interfaces['binary']:
        node_ip = node.network_interfaces['binary'][0]
    else:
        node_ip = node.network_interfaces['thrift'][0]
    return node_ip


def is_autocompaction_enabled(node, ks_name, table_name):
    """
    Return if autocompaction is enabled or not
    :param node: node to execute the API request
    :param ks_name: Keyspace name to verify if autocompaction is enabled
    :param table_name: table name to verify if autocompaction is enabled
    :return: True|False
    """
    node_ip = get_ip_from_node(node=node)
    response = requests.get(f'http://{node_ip}:10000/column_family/autocompaction/{ks_name}:{table_name}')
    response.raise_for_status()
    return response.json()


def test_failure_due_to_timeout(err, *args):
    """
    check if we should rerun a test with the flaky plugin or not.
    for now, only run if we failed the test for one of the following
    three exceptions: cassandra.OperationTimedOut, ccm.node.ToolError,
    and ccm.node.TimeoutError.

    - cassandra.OperationTimedOut will be thrown when a cql query made thru
    the python-driver times out.
    - ccm.node.ToolError will be thrown when an invocation of a "tool"
    (in the case of dtests this will almost always invoking stress).
    - ccm.node.TimeoutError will be thrown when a blocking ccm operation
    on a individual node times out. In most cases this tends to be something
    like watch_log_for hitting the timeout before the desired pattern is seen
    in the node's logs.
    """
    if issubclass(err[0], OperationTimedOut) or issubclass(err[0], NodetoolError) or issubclass(err[0], TimeoutError):
        return True
    else:
        return False


@flaky(rerun_filter=test_failure_due_to_timeout)
class Tester:
    def __getattribute__(self, name):
        try:
            return object.__getattribute__(self, name)
        except AttributeError:
            fixture_dtest_setup = object.__getattribute__(self, 'fixture_dtest_setup')
            return object.__getattribute__(fixture_dtest_setup, name)

    @pytest.fixture(scope='function', autouse=True)
    def set_dtest_setup_on_function(self, fixture_dtest_setup):
        self.fixture_dtest_setup = fixture_dtest_setup
        self.dtest_config = fixture_dtest_setup.dtest_config
        return None

    def set_node_to_current_version(self, node):
        version = os.environ.get('CASSANDRA_VERSION')

        if version:
            node.set_install_dir(version=version)
        else:
            node.set_install_dir(install_dir=self.dtest_config.cassandra_dir)
            os.environ['CASSANDRA_DIR'] = self.dtest_config.cassandra_dir

    def go(self, func):
        runner = Runner(func)
        self.runners.append(runner)
        runner.start()
        return runner

    def assert_log_had_msg(self, node, msg, timeout=600, **kwargs):
        """
        Wrapper for ccmlib.node.Node#watch_log_for to cause an assertion failure when a log message isn't found
        within the timeout.
        :param node: Node which logs we should watch
        :param msg: String message we expect to see in the logs.
        :param timeout: Seconds to wait for msg to appear
        """
        try:
            node.watch_log_for(msg, timeout=timeout, **kwargs)
        except TimeoutError:
            pytest.fail("Log message was not seen within timeout:\n{0}".format(msg))


def get_eager_protocol_version(cassandra_version):
    """
    Returns the highest protocol version accepted
    by the given C* version
    """
    if cassandra_version >= '2.2':
        protocol_version = 4
    elif cassandra_version >= '2.1':
        protocol_version = 3
    elif cassandra_version >= '2.0':
        protocol_version = 2
    else:
        protocol_version = 1
    return protocol_version


# We default to UTF8Type because it's simpler to use in tests
def create_cf(session, name, key_type="varchar", speculative_retry=None, read_repair=None, compression=None,
              gc_grace=None, columns=None, validation="UTF8Type", compact_storage=False,
              compaction_strategy='SizeTieredCompactionStrategy', primary_key=None, clustering=None, default_ttl=None,
              compaction=None, debug_query=False, caching=True, paxos_grace_seconds=None,
              dclocal_read_repair_chance=None, in_memory=None):

    compaction_fragment = "compaction = {'class': '%s', 'enabled': 'true'}"
    if compaction_strategy == '':
        compaction_fragment = compaction_fragment % 'SizeTieredCompactionStrategy'
    else:
        compaction_fragment = compaction_fragment % compaction_strategy

    additional_columns = ""
    if columns is not None:
        for k, v in list(columns.items()):
            additional_columns = "{}, {} {}".format(additional_columns, k, v)

    if additional_columns == "":
        query = 'CREATE COLUMNFAMILY %s (key %s, c varchar, v varchar, PRIMARY KEY(key, c)) WITH comment=\'test cf\'' % (
            name, key_type)
    else:
        if primary_key:
            query = 'CREATE COLUMNFAMILY %s (key %s%s, PRIMARY KEY(%s)) WITH comment=\'test cf\'' % (
                name, key_type, additional_columns, primary_key)
        else:
            query = 'CREATE COLUMNFAMILY %s (key %s PRIMARY KEY%s) WITH comment=\'test cf\'' % (
                name, key_type, additional_columns)

    if compaction is not None:
        query = '%s AND compaction=%s' % (query, compaction)
    else:
        if compaction_fragment is not None:
            query = '%s AND %s' % (query, compaction_fragment)

    if clustering:
        query = '%s AND CLUSTERING ORDER BY (%s)' % (query, clustering)

    if compression is not None:
        query = '%s AND compression = { \'sstable_compression\': \'%sCompressor\' }' % (query, compression)
    else:
        # if a compression option is omitted, C* will default to lz4 compression
        query += ' AND compression = {}'

    if read_repair is not None:
        query = '%s AND read_repair_chance=%f' % (query, read_repair)
    if dclocal_read_repair_chance is not None:
        query = '%s AND dclocal_read_repair_chance=\'%s\'' % (query, dclocal_read_repair_chance)
    if gc_grace is not None:
        query = '%s AND gc_grace_seconds=%d' % (query, gc_grace)
    if default_ttl is not None:
        query = '%s AND default_time_to_live=%d' % (query, default_ttl)
    if speculative_retry is not None:
        query = '%s AND speculative_retry=\'%s\'' % (query, speculative_retry)
    if in_memory:
        query += ' AND in_memory=true'
    if compact_storage:
        query += ' AND COMPACT STORAGE'

    if not caching:
        query += ' AND caching = {\'enabled\':false}'

    if debug_query:
        logger.debug(query)
    if paxos_grace_seconds is not None:
        query = '%s AND paxos_grace_seconds=%d' % (query, paxos_grace_seconds)

    try:
        retry_till_success(session.execute, query=query, timeout=120, bypassed_exception=cassandra.OperationTimedOut)
    except cassandra.AlreadyExists:
        logger.warn('AlreadyExists executing create cf query \'%s\'' % query)
    session.cluster.control_connection.wait_for_schema_agreement(wait_time=120)
    # Going to ignore OperationTimedOut from create CF, so need to validate it was indeed created
    session.execute('SELECT * FROM %s LIMIT 1' % name)
