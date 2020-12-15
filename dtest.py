import copy
import errno
import logging
import os
import re
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import traceback
import itertools
import warnings

import requests
import datetime
import inspect
from unittest import TestCase
import random
import glob
from pkg_resources import parse_version

import psutil
from cassandra import ConsistencyLevel
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster as PyCluster
from cassandra.cluster import NoHostAvailable
from cassandra.cluster import ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra.cluster import default_lbp_factory
from cassandra.policies import RetryPolicy
from cassandra.policies import WhiteListRoundRobinPolicy
from ccmlib.cluster import Cluster
from ccmlib.cluster_factory import ClusterFactory
from ccmlib.common import isScylla
from ccmlib.common import is_win
from ccmlib.node import TimeoutError
from ccmlib.scylla_cluster import ScyllaCluster
from OpenSSL import crypto
from socket import gethostname
from nose.exc import SkipTest
from nose.plugins.attrib import attr

from multiprocessing import Queue, Lock
from functools import wraps

os.environ['LOCALE'] = 'C'

LOG_SAVED_DIR = os.environ.get('LOG_SAVED_DIR', "logs")
try:
    os.mkdir(LOG_SAVED_DIR)
except OSError:
    pass

LAST_LOG = "last"

LAST_TEST_DIR = 'last_test_dir'

DEFAULT_DIR = './'
CASSANDRA_DIR = os.environ.get('CASSANDRA_DIR', None)

NO_SKIP = os.environ.get('SKIP', '').lower() in ('no', 'false')
DEBUG = os.environ.get('DEBUG', '').lower() in ('yes', 'true')
TRACE = os.environ.get('TRACE', '').lower() in ('yes', 'true')
KEEP_LOGS = os.environ.get('KEEP_LOGS', '').lower() in ('yes', 'true')
KEEP_TEST_DIR = os.environ.get('KEEP_TEST_DIR', '').lower() in ('yes', 'true')
PRINT_DEBUG = os.environ.get('PRINT_DEBUG', '').lower() in ('yes', 'true')
DISABLE_VNODES = os.environ.get('DISABLE_VNODES', '').lower() in ('yes', 'true')
OFFHEAP_MEMTABLES = os.environ.get('OFFHEAP_MEMTABLES', '').lower() in ('yes', 'true')
NUM_TOKENS = os.environ.get('NUM_TOKENS', '256')
RECORD_COVERAGE = os.environ.get('RECORD_COVERAGE', '').lower() in ('yes', 'true')
REUSE_CLUSTER = os.environ.get('REUSE_CLUSTER', '').lower() in ('yes', 'true')
SILENCE_DRIVER_ON_SHUTDOWN = os.environ.get('SILENCE_DRIVER_ON_SHUTDOWN', 'true').lower() in ('yes', 'true')
IGNORE_REQUIRE = os.environ.get('IGNORE_REQUIRE', '').lower() in ('yes', 'true')
NOSE_PROCESSES = int(os.environ.get('NOSE_PROCESSES', 0))
CLUSTER_ID_ALLOCATOR = os.environ.get('CLUSTER_ID_ALLOCATOR', 'random')
KEEP_CORES = os.environ.get('KEEP_CORES', 'true').lower() in ('yes', 'true')
DTEST_CORE_COMPRESS_TOOL = os.environ.get('DTEST_CORE_COMPRESS_TOOL', 'gzip')
DTEST_CORE_COMPRESS_EXT = os.environ.get('DTEST_CORE_COMPRESS_EXT', 'gz')
DRY_RUN = os.environ.get('DRY_RUN', '').lower() in ('yes', 'true')

CURRENT_TEST = ""
CURRENT_TEST_NESTING = 0

logging.basicConfig(filename=os.path.join(LOG_SAVED_DIR, "dtest.log"),
                    filemode='w',
                    format='%(asctime)s,%(msecs)03d %(process)-7d %(name)-30s %(levelname)-8s | %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    level=logging.DEBUG)

LOG = logging.getLogger('dtest')
logging.getLogger('ccm').setLevel(logging.DEBUG)
# set python-driver log level to WARN by default for dtest
logging.getLogger('cassandra').setLevel(logging.WARNING)
logging.getLogger('urllib3').setLevel(logging.WARNING)
logging.getLogger('boto3').setLevel(logging.WARNING)
logging.getLogger('botocore').setLevel(logging.WARNING)

# copy the initial environment variables so we can reset them later:
initial_environment = copy.deepcopy(os.environ)


def reset_environment_vars():
    os.environ.clear()
    os.environ.update(initial_environment)


def log_message(msg):
    if CURRENT_TEST != "":
        msg = CURRENT_TEST + ' - ' + str(msg)
    return msg


def warning(msg, add_timestamp=True):
    msg = log_message(msg)
    LOG.warning(msg)
    if PRINT_DEBUG:
        msg = '{0}{1}'.format('{} '.format(datetime.datetime.now()) if add_timestamp else '', msg)
        print("WARN: " + msg)


def debug(msg, add_timestamp=True):
    msg = log_message(msg)
    LOG.debug(msg)
    msg = '{0}{1}'.format('{} '.format(datetime.datetime.now()) if add_timestamp else '', msg)
    if PRINT_DEBUG:
        print(msg)


def info(msg, add_timestamp=True):
    msg = log_message(msg)
    LOG.info(msg)
    msg = '{0}{1}'.format('{} '.format(datetime.datetime.now()) if add_timestamp else '', msg)
    print(msg)


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
            debug('%s (%s s)'.format(text, time_elapsed))
    return ok


class WaitTimeoutExpired(Exception):
    pass


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
            debug('({} ({} s)'.format(text, time_elapsed))
        if time_elapsed > timeout:
            err = 'Wait for: {}: timeout - {} seconds - expired'.format(text, timeout)
            debug(err)
            if throw_exc:
                raise WaitTimeoutExpired(err)
    return ok


class FlakyRetryPolicy(RetryPolicy):
    """
    A retry policy that retries 5 times
    """

    def on_read_timeout(self, *args, **kwargs):
        if kwargs['retry_num'] < 5:
            debug("Retrying read after timeout. Attempt #" + str(kwargs['retry_num']))
            return (self.RETRY, None)
        else:
            return (self.RETHROW, None)

    def on_write_timeout(self, *args, **kwargs):
        if kwargs['retry_num'] < 5:
            debug("Retrying write after timeout. Attempt #" + str(kwargs['retry_num']))
            return (self.RETRY, None)
        else:
            return (self.RETHROW, None)

    def on_unavailable(self, *args, **kwargs):
        if kwargs['retry_num'] < 5:
            debug("Retrying request after UE. Attempt #" + str(kwargs['retry_num']))
            return (self.RETRY, None)
        else:
            return (self.RETHROW, None)


class Runner(threading.Thread):

    def __init__(self, func, sleep=1.0):
        threading.Thread.__init__(self)
        self.__func = func
        self.__error = None
        self.__stopped = False
        self.__sleep = sleep
        self.daemon = True

    def run(self):
        i = 0
        debug("Runner: running {}".format(self.__func))
        while True:
            if self.__stopped:
                debug("Runner: stopped {}".format(self.__func))
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
        debug("Runner: stopping {} (timeout={})".format(self.__func, timeout))
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


class ClusterIdAllocator:
    def alloc(self, cluster_dir):
        raise NotImplementedError

    def free(self, id):
        raise NotImplementedError


class SingleClusterIdAllocator(ClusterIdAllocator):
    _allocated = False

    def alloc(self, cluster_dir):
        if not self._allocated:
            self._allocated = True
            return 0
        raise Exception("No Available Cluster")

    def free(self, id):
        if self._allocated:
            self._allocated = False
            return
        raise Exception("Cluster was not allocated")


class MultiProcessClusterIdAllocator(ClusterIdAllocator):
    _multiprocess_shared_ = True

    def __init__(self):
        self._id = Queue()
        for id in range(1, 100):
            self._id.put(id)
        self._lock = Lock()

    def alloc(self, cluster_dir):
        with self._lock:
            id = self._id.get()
            return id

    def free(self, id):
        with self._lock:
            self._id.put(id)


class RandomClusterIdAllocator(ClusterIdAllocator):
    def __init__(self):
        self._range = list(range(1, 100))
        self._retries = 10
        self._links = {}

    def alloc(self, cluster_dir):
        dirname = os.path.dirname(cluster_dir)
        basename = os.path.basename(cluster_dir)
        for id in random.sample(self._range, self._retries):
            try:
                link = os.path.join(dirname, str(id))
                os.symlink(basename, link)
                self._links[id] = link
                debug("Allocated cluster ID {}: {}".format(id, cluster_dir))
                return id
            except OSError as e:
                if e.errno == errno.EEXIST:
                    try:
                        os.stat(link)
                    except OSError as e2:
                        if e2.errno == errno.ENOENT:
                            # If the link target does not exist, recycle the ID
                            # Note: may race with other instances doing the same
                            tmp_link = "link.{}".format(os.getpid())
                            try:
                                os.rename(link, tmp_link)
                            except OSError as e3:
                                if e2.errno != errno.ENOENT:
                                    debug("Could not rename {}: {}".format(link, e3))
                            else:
                                os.remove(tmp_link)
                    continue
                raise Exception("Exception while allocating cluster ID {}: {}".format(id, e))
        raise Exception("Could not allocate cluster ID after {} retries".format(self._retries))

    def free(self, id):
        link = self._links.pop(id, None)
        debug("Freeing cluster ID {}: link {}".format(id, link))
        if not link:
            raise AssertionError("No link found for ID {}".format(id))
        try:
            os.remove(link)
        except OSError as e:
            warning("Could not remove link {}: {}".format(link, e))


def parallel_tests():
    return NOSE_PROCESSES > 0


debug("going to run tests {}".format("in parallel" if parallel_tests() else "sequentially"))

if CLUSTER_ID_ALLOCATOR == 'random':
    debug("using the RandomClusterIdAllocator")
    cluster_id_allocator = RandomClusterIdAllocator()
elif CLUSTER_ID_ALLOCATOR == 'multiprocess':
    debug("using the MultiProcessClusterIdAllocator")
    cluster_id_allocator = MultiProcessClusterIdAllocator()
elif CLUSTER_ID_ALLOCATOR == 'single':
    debug("using the SingleClusterIdAllocator")
    cluster_id_allocator = SingleClusterIdAllocator()
else:
    raise AssertionError("Unsupported CLUSTER_ID_ALLOCATOR={}".format(CLUSTER_ID_ALLOCATOR))


def make_execution_profile(retry_policy=FlakyRetryPolicy(), consistency_level=ConsistencyLevel.ONE, **kwargs):
    return ExecutionProfile(retry_policy=retry_policy,
                            consistency_level=consistency_level,
                            **kwargs)


PRESERVED_CLUSTER = None


class Tester(TestCase):
    _multiprocess_can_split_ = True

    def __init__(self, *argv, **kwargs):
        # if False, then scan the log of each node for errors after every test.
        if not hasattr(self, '_preserve_cluster'):
            self._preserve_cluster = False
        if not hasattr(self, 'ignore_log_patterns'):
            self.ignore_log_patterns = []
        if not hasattr(self, 'ignore_cores_log_patterns'):
            self.ignore_cores_log_patterns = []
        # nodes will be added to ignore_cores if errors matching ignore_cores_log_patterns
        # are found in their log
        self.ignore_cores = []
        self.cluster_id_allocator = cluster_id_allocator
        self.cluster_options = kwargs.pop('cluster_options', None)
        self.cassandra_version = kwargs.pop('cassandra_version', None)
        self._handling_timeout = False
        self.connections = []
        self.runners = []
        self.base_cql_timeout = 10  # seconds
        super(Tester, self).__init__(*argv, **kwargs)

    def _reuse_preserved_cluster(self):
        global PRESERVED_CLUSTER
        if self._preserve_cluster and PRESERVED_CLUSTER is not None:
            self.cluster = PRESERVED_CLUSTER
            self.test_path = os.path.join(PRESERVED_CLUSTER.get_path(), "..")
            PRESERVED_CLUSTER = None
            return True
        return False

    def _get_cluster(self, name='test', version=None):
        if self._preserve_cluster and hasattr(self, 'cluster'):
            return self.cluster

        # we can not work /tmp
        dtest_root = os.path.join(os.path.expanduser("~"), '.dtest')
        if not os.path.exists(dtest_root):
            os.makedirs(dtest_root)
        self.test_path = tempfile.mkdtemp(dir=dtest_root, prefix='dtest-')

        # ccm on cygwin needs absolute path to directory - it crosses from cygwin space into
        # regular Windows space on wmic calls which will otherwise break pathing
        if sys.platform == "cygwin":
            self.test_path = subprocess.Popen(["cygpath", "-m", self.test_path],
                                              stdout=subprocess.PIPE, stderr=subprocess.STDOUT).communicate()[0].rstrip()
        debug("cluster ccm directory: " + self.test_path)
        if not version:
            version = os.environ.get('CASSANDRA_VERSION')
        cdir = CASSANDRA_DIR

        scylla_version = os.environ.get('SCYLLA_VERSION', None)

        if version:
            debug("Starting Cassandra cluster version {}".format(version))
            cluster = Cluster(self.test_path, name, cassandra_version=version)
        elif scylla_version:
            debug("Starting Scylla cluster version {}".format(scylla_version))
            cluster = ScyllaCluster(self.test_path, name, cassandra_version=scylla_version,
                                    force_wait_for_cluster_start=True)
        else:
            if isScylla(cdir):
                debug("Starting Scylla cluster from directory {}".format(cdir))
                cluster = ScyllaCluster(self.test_path, name, cassandra_dir=cdir, install_dir=cdir,
                                        force_wait_for_cluster_start=True)
            else:
                debug("Starting Cassandra cluster from directory {}".format(cdir))
                cluster = Cluster(self.test_path, name, cassandra_dir=cdir)

        if DISABLE_VNODES:
            cluster.set_configuration_options(values={'num_tokens': None})
        else:
            cluster.set_configuration_options(values={'initial_token': None, 'num_tokens': NUM_TOKENS})

        if OFFHEAP_MEMTABLES:
            cluster.set_configuration_options(values={'memtable_allocation_type': 'offheap_objects'})

        id = self.cluster_id_allocator.alloc(self.test_path)
        cluster.set_id(id)
        cluster.set_ipprefix("127.0.%d." % id)

        return cluster

    def var_debug(self, cluster):
        if os.environ.get('DEBUG', 'no').lower() not in ('no', 'false', 'yes', 'true'):
            classes_to_debug = os.environ.get('DEBUG').split(":")
            cluster.set_log_level('DEBUG', None if len(classes_to_debug) == 0 else classes_to_debug)

    def var_trace(self, cluster):
        if os.environ.get('TRACE', 'no').lower() not in ('no', 'false', 'yes', 'true'):
            classes_to_trace = os.environ.get('TRACE').split(":")
            cluster.set_log_level('TRACE', None if len(classes_to_trace) == 0 else classes_to_trace)

    def modify_log(self, cluster):
        if DEBUG:
            cluster.set_log_level("DEBUG")
        if TRACE:
            cluster.set_log_level("TRACE")
        self.var_debug(cluster)
        self.var_trace(cluster)

    def _cleanup_cluster(self, remove=True):
        Tester._cls_cleanup_cluster(self.cluster, self.test_path, self._preserve_cluster,
                                    self.cluster_id_allocator, remove)

    def _cls_cleanup_cluster(cluster, test_path, preserve_cluster, cluster_id_allocator, remove=True):
        if SILENCE_DRIVER_ON_SHUTDOWN:
            # driver logging is very verbose when nodes start going down -- bump up the level
            logging.getLogger('cassandra').setLevel(logging.CRITICAL)

        if KEEP_TEST_DIR:
            debug("{}stopping ccm cluster {} at: {}".format("gently " if RECORD_COVERAGE else "", cluster.name, test_path))
            cluster.stop(gently=RECORD_COVERAGE)
        else:
            # when recording coverage the jvm has to exit normally
            # or the coverage information is not written by the jacoco agent
            # otherwise we can just kill the process
            if RECORD_COVERAGE:
                cluster.stop(gently=True)

            if remove:
                # Cleanup everything:
                debug("removing ccm cluster " + cluster.name + " at: " + test_path)
                cluster.remove()

                # debug("clearing ssl stores from [{0}] directory".format(test_path))
                for filename in ('keystore.jks', 'truststore.jks', 'ccm_node.cer', 'ccm_node.pem', 'ccm_node.key', 'trust.pem'):
                    try:
                        os.remove(os.path.join(test_path, filename))
                    except OSError as e:
                        # once we port to py3, which has better reporting for exceptions raised while
                        # handling other excpetions, we should just assert e.errno == errno.ENOENT
                        if e.errno != errno.ENOENT:  # ENOENT = no such file or directory
                            raise

                if os.path.exists(test_path):
                    os.rmdir(test_path)
        if os.path.exists(LAST_TEST_DIR):
            os.remove(LAST_TEST_DIR)

        if not preserve_cluster:
            Tester._force_clean(cluster)

        # cluster.id may be equal to 0
        # so test it is not None
        if cluster.id is not None:
            cluster_id_allocator.free(cluster.id)
            cluster.id = None

    def set_node_to_current_version(self, node):
        version = os.environ.get('CASSANDRA_VERSION')
        cdir = CASSANDRA_DIR

        if version:
            node.set_install_dir(version=version)
        else:
            node.set_install_dir(install_dir=cdir)

    @staticmethod
    def _force_clean(cluster):
        cdir = CASSANDRA_DIR

        if isScylla(cdir):
            for proc in psutil.process_iter():
                try:
                    if ('scylla' in proc.name() or 'scylla' in proc.cmdline()[0]) and any(cluster.ipprefix in cmd for cmd in proc.cmdline()):
                        debug("proc %s killed - cluster %s" % (proc.pid, cluster.ipprefix))
                        try:
                            proc.kill()
                        except Exception:
                            pass
                except Exception:
                    pass

    def setUp(self):
        if DRY_RUN:
            raise SkipTest("Dry run")

        global CURRENT_TEST, CURRENT_TEST_NESTING
        cls = self.__class__
        qualname = cls.__qualname__
        module = cls.__module__
        if os.path.exists(module + '.py'):
            module += '.py'
        elif '.' in module:
            m = "{}.py".format(module.replace('.', '/'))
            if os.path.exists(m):
                module = m
        CURRENT_TEST_NESTING += 1
        if CURRENT_TEST_NESTING == 1:
            if CURRENT_TEST != "":
                debug("CURRENT_TEST is not empty on setUp")
            CURRENT_TEST = "{}:{}.{}".format(module, qualname, self._testMethodName)
        elif CURRENT_TEST == "":
            CURRENT_TEST = "{}:{}.{}".format(module, qualname, self._testMethodName)
            debug("CURRENT_TEST was empty on nested setUp. CURRENT_TEST_NESTING={}".format(CURRENT_TEST_NESTING))
        self.addCleanup(self.cleanUpTest)

        self._preserve_cluster = False
        if (getattr(getattr(self,  self._testMethodName), 'reuse-cluster', False) or getattr(self, 'reuse-cluster', False)):
            self._preserve_cluster = REUSE_CLUSTER

        # On Windows, forcefully terminate any leftover previously running cassandra processes. This is a temporary
        # workaround until we can determine the cause of intermittent hung-open tests and file-handles.
        if is_win():
            try:
                import psutil
                for proc in psutil.process_iter():
                    try:
                        pinfo = proc.as_dict(attrs=['pid', 'name', 'cmdline'])
                    except psutil.NoSuchProcess:
                        pass
                    else:
                        if (pinfo['name'] == 'java.exe' and '-Dcassandra' in pinfo['cmdline']):
                            print('Found running cassandra process with pid: ' + str(pinfo['pid']) + '. Killing.')
                            psutil.Process(pinfo['pid']).kill()
            except ImportError:
                debug("WARN: psutil not installed. Cannot detect and kill running cassandra processes - you may see cascading dtest failures.")

        # cleaning up if a previous execution didn't trigger tearDown (which
        # can happen if it is interrupted by KeyboardInterrupt)
        # TODO: move that part to a generic fixture
        if os.path.exists(LAST_TEST_DIR):
            with open(LAST_TEST_DIR) as f:
                self.test_path = f.readline().strip('\n')
                name = f.readline()
            try:
                self.cluster = ClusterFactory.load(self.test_path, name)
                # Avoid waiting too long for node to be marked down
                if not self._preserve_cluster:
                    self._cleanup_cluster()
            except IOError:
                # after a restart, /tmp will be emptied so we'll get an IOError when loading the old cluster here
                pass

        new_cluster = False
        if not hasattr(self, 'cluster') or self.cluster is None:
            if not self._reuse_preserved_cluster():
                new_cluster = True
                self.cluster = self._get_cluster(version=self.cassandra_version)
        self.addCleanup(self.cleanUpCluster)

        annotate = os.path.join(self.cluster.get_path(), 'current_test')
        with open(annotate, 'a') as f:
            f.write(self.id() + '\n')

        if new_cluster:
            self._force_clean(self.cluster)
        else:
            return

        if RECORD_COVERAGE:
            self.__setup_jacoco()
        # the failure detector can be quite slow in such tests with quick start/stop
        self.cluster.set_configuration_options(values={'phi_convict_threshold': 5})

        timeout = self.cql_timeout() * 1000
        range_timeout = 3 * timeout
        self.cql_request_timeout = 3 * self.cql_timeout()
        if isinstance(self.cluster, ScyllaCluster):
            debug("Scylla mode is '{}'".format(self.cluster.scylla_mode))
        debug("Cluster *_request_timeout_in_ms={}, range_request_timeout_in_ms={}, cql request_timeout={}".format(
            timeout, range_timeout, self.cql_request_timeout))
        self.cluster.set_configuration_options(values={
            'read_request_timeout_in_ms': timeout,
            'range_request_timeout_in_ms': range_timeout,
            'write_request_timeout_in_ms': timeout,
            'counter_write_request_timeout_in_ms': range_timeout,
            'truncate_request_timeout_in_ms': timeout,
            'request_timeout_in_ms': timeout
        })
        if self.cluster_options is not None:
            self.cluster.set_configuration_options(values=self.cluster_options)

        # if tests are running in parallel do not use last test info
        if not (parallel_tests() or CLUSTER_ID_ALLOCATOR == 'random'):
            with open(LAST_TEST_DIR, 'w') as f:
                f.write(self.test_path + '\n')
                f.write(self.cluster.name)

        self.modify_log(self.cluster)

        # Disable gossip for single-node cluster setups.
        #
        # Waiting for gossip to settle is absolutely redundant when there
        # is only one node in the testing cluster, but consumes a large
        # amount of time when starting cluster.
        if isinstance(self.cluster, ScyllaCluster):
            if getattr(getattr(self,  self._testMethodName), 'single_node', False) or \
               getattr(self, 'single_node', False):
                debug("configuring skip_wait_for_gossip_to_settle=0 for single_node test")
                self.cluster.set_configuration_options(values={'skip_wait_for_gossip_to_settle': 0})

            # Reduce waiting time for the nodes to hear from others before joining the ring.
            # Since all test cases run on localhost and there are no large test clusters
            # it's safe to reduce the value to save a lot of time while testing.
            # (Default value for the option is 30s)
            self.cluster.set_configuration_options(values={'ring_delay_ms': 10000})

    def find_cores(self):
        cores = []
        ignored_cores = []
        nodes = []
        for node in self.cluster.nodelist():
            try:
                pids = node.all_pids
                if not pids:
                    pids = [node.pid]
            except AttributeError:
                pids = [node.pid]
            nodes += [(node, pids)]
        for f in os.listdir('.'):
            if not f.endswith('.core'):
                continue
            for node, pids in nodes:
                """Look for this cluster's coredumps"""
                for p in pids:
                    if f.find(".{}.".format(p)) >= 0:
                        path = os.path.join(os.getcwd(), f)
                        if not node in self.ignore_cores:
                            cores += [(node.name, path)]
                        else:
                            debug("Ignoring core file {} belonging to {} due to ignore_cores_log_patterns".format(path, node.name))
                            ignored_cores += [(node.name, path)]
        # returns empty list if no core files found
        return cores, ignored_cores

    def copy_logs(self, directory=LOG_SAVED_DIR, name=LAST_LOG, cores=None):
        """Copy the current cluster's log files somewhere, by default to LOG_SAVED_DIR with a name of 'last'"""
        name = os.path.join(directory, name)
        if not os.path.exists(directory):
            os.mkdir(directory)
        basedir = str(int(time.time() * 1000)) + '_' + self.id()
        logdir = os.path.join(directory, basedir)
        os.mkdir(logdir)

        cluster_path = self.cluster.get_path()
        for log in glob.glob(os.path.join(cluster_path, '**/logs/*'), recursive=True):
            n = re.search('node\d+', log).group(0)
            logname = os.path.basename(log)
            # for backward compatibility, rename the logs:
            #   nodeX/logs/system.log to nodeX.log
            #   nodeX/logs/debug.log to nodeX_debug.log
            if logname == 'system.log':
                dest = n + '.log'
            else:
                dest = "{}_{}".format(n, logname)
            shutil.copyfile(log, os.path.join(logdir, dest))

        if hasattr(self.cluster, '_scylla_manager') and self.cluster._scylla_manager:
            log = os.path.join(self.cluster._scylla_manager._get_path(), 'scylla-manager.log')
            if os.path.exists(log):
                shutil.copyfile(log, os.path.join(logdir, 'scylla-manager.log'))

            logs = [(node.name, node.logfilename() + ".manager_agent") for node in self.cluster.nodes.values()]
            if len(logs):
                for name, agent_log in logs:
                    if os.path.exists(agent_log):
                        shutil.copyfile(agent_log, os.path.join(logdir, name + ".log.manager_agent"))

        if KEEP_CORES:
            if cores is None:
                cores, ignored_cores = self.find_cores()
                cores += ignored_cores
            if cores:
                for n, src in cores:
                    dst = os.path.join(logdir, "{}-{}".format(n, os.path.basename(src)))
                    print("Moving core file {} to {}".format(src, dst))
                    try:
                        if DTEST_CORE_COMPRESS_TOOL == '':
                            cmd = "mv {} {}".format(src, dst)
                            shutil.move(src, dst)
                        else:
                            cmd = "{} < {} > {}.{} && rm {}".format(
                                DTEST_CORE_COMPRESS_TOOL, src, dst, DTEST_CORE_COMPRESS_EXT, src)
                            subprocess.check_call(cmd, shell=True)
                    except Exception as e:
                        print("`{}` failed: {}. Keeping directory.".format(cmd, e))

        if os.path.exists(logdir):
            if os.path.exists(name):
                os.unlink(name)
            if not is_win():
                os.symlink(basedir, name)

    def get_eager_protocol_version(self, cassandra_version):
        """
        Returns the highest protocol version accepted
        by the given C* version
        """
        v = parse_version(cassandra_version)
        if v >= parse_version('2.2'):
            protocol_version = 4
        elif v >= parse_version('2.1'):
            protocol_version = 3
        elif v >= parse_version('2.0'):
            protocol_version = 2
        else:
            protocol_version = 1
        return protocol_version

    def cql_connection(self, node, keyspace=None, user=None,
                       password=None, compression=True, protocol_version=None, port=None, ssl_opts=None,
                       topology_event_refresh_window=10, request_timeout=None, **kwargs):

        return self._create_session(node, keyspace, user, password, compression,
                                    protocol_version, port=port, ssl_opts=ssl_opts,
                                    topology_event_refresh_window=topology_event_refresh_window,
                                    load_balancing_policy=default_lbp_factory(),
                                    request_timeout=request_timeout,
                                    **kwargs)

    def exclusive_cql_connection(self, node, keyspace=None, user=None,
                                 password=None, compression=True, protocol_version=None, port=None, ssl_opts=None,
                                 request_timeout=None, **kwargs):

        node_ip = self.get_ip_from_node(node)
        wlrr = WhiteListRoundRobinPolicy([node_ip])

        return self._create_session(node, keyspace, user, password, compression, protocol_version,
                                    port=port, ssl_opts=ssl_opts,
                                    topology_event_refresh_window=-1,
                                    load_balancing_policy=wlrr,
                                    request_timeout=request_timeout,
                                    **kwargs)

    def _create_session(self, node, keyspace, user, password, compression, protocol_version,
                        port=None, ssl_opts=None, execution_profiles=None,
                        topology_event_refresh_window=10,
                        request_timeout=None,
                        keep_session=True,
                        **kwargs):
        node_ip = self.get_ip_from_node(node)
        if not port:
            port = self.get_port_from_node(node)

        if protocol_version is None:
            protocol_version = self.get_eager_protocol_version(self.cluster.version())

        if user is not None:
            auth_provider = self.get_auth_provider(user=user, password=password)
        else:
            auth_provider = None

        if request_timeout is None:
            request_timeout = self.cql_request_timeout

        profiles = {EXEC_PROFILE_DEFAULT: make_execution_profile(request_timeout=request_timeout, **kwargs)
                    } if not execution_profiles else execution_profiles

        cluster = PyCluster([node_ip],
                            auth_provider=auth_provider,
                            compression=compression,
                            protocol_version=protocol_version,
                            port=port,
                            ssl_options=ssl_opts,
                            connect_timeout=5,
                            max_schema_agreement_wait=60,
                            control_connection_timeout=6.0,
                            topology_event_refresh_window=topology_event_refresh_window,
                            execution_profiles=profiles)
        session = cluster.connect()

        # temporarily increase client-side timeout to 1m to determine
        # if the cluster is simply responding slowly to requests
        # session.default_timeout = 60.0

        if keyspace is not None:
            session.set_keyspace(keyspace)

        # override driver default consistency level of LOCAL_QUORUM
        # session.default_consistency_level = ConsistencyLevel.ONE

        if keep_session:
            self.connections.append(session)

        return session

    def patient_cql_connection(self, node, keyspace=None, user=None, password=None,
                               request_timeout=None, compression=True, timeout=60,
                               protocol_version=None, port=None, ssl_opts=None,
                               topology_event_refresh_window=10, **kwargs):
        """
        Returns a connection after it stops throwing NoHostAvailables due to not being ready.

        If the timeout is exceeded, the exception is raised.
        """
        if is_win():
            timeout *= 2

        return retry_till_success(
            self.cql_connection,
            node,
            keyspace=keyspace,
            user=user,
            password=password,
            timeout=timeout,
            request_timeout=request_timeout,
            compression=compression,
            protocol_version=protocol_version,
            port=port,
            ssl_opts=ssl_opts,
            topology_event_refresh_window=topology_event_refresh_window,
            bypassed_exception=NoHostAvailable,
            **kwargs
        )

    def patient_exclusive_cql_connection(self, node, keyspace=None, user=None, password=None,
                                         timeout=60, request_timeout=None, compression=True,
                                         protocol_version=None, port=None, ssl_opts=None,  **kwargs):
        """
        Returns a connection after it stops throwing NoHostAvailables due to not being ready.

        If the timeout is exceeded, the exception is raised.
        """
        if is_win():
            timeout *= 2

        return retry_till_success(
            self.exclusive_cql_connection,
            node,
            keyspace=keyspace,
            user=user,
            password=password,
            timeout=timeout,
            request_timeout=request_timeout,
            compression=compression,
            protocol_version=protocol_version,
            port=port,
            ssl_opts=ssl_opts,
            bypassed_exception=NoHostAvailable,
            **kwargs
        )

    def cql_cluster_session(self, node, keyspace=None, user=None,
                            password=None, compression=True, protocol_version=None, port=None, ssl_opts=None,
                            topology_event_refresh_window=10, request_timeout=None, exclusive=False, **kwargs):

        if exclusive:
            node_ip = self.get_ip_from_node(node)
            topology_event_refresh_window = -1
            load_balancing_policy = WhiteListRoundRobinPolicy([node_ip])
        else:
            load_balancing_policy = default_lbp_factory()

        session = self._create_session(node, keyspace, user, password, compression, protocol_version,
                                       port=port, ssl_opts=ssl_opts,
                                       topology_event_refresh_window=topology_event_refresh_window,
                                       load_balancing_policy=load_balancing_policy,
                                       request_timeout=request_timeout,
                                       keep_session=False,
                                       **kwargs)

        class ClusterSession:
            def __init__(self, session):
                self.session = session

            def __del__(self):
                self.__cleanup()

            def __enter__(self):
                return self.session

            def __exit__(self, type, value, traceback):
                self.__cleanup()

            def __cleanup(self):
                if self.session:
                    self.session.cluster.shutdown()
                    self.session = None

        return ClusterSession(session)

    def patient_cql_cluster_session(self, node, keyspace=None, user=None, password=None,
                                    request_timeout=None, compression=True, timeout=60,
                                    protocol_version=None, port=None, ssl_opts=None,
                                    topology_event_refresh_window=10, exclusive=False, **kwargs):
        """
        Returns a connection after it stops throwing NoHostAvailables due to not being ready.

        If the timeout is exceeded, the exception is raised.
        """
        if is_win():
            timeout *= 2

        return retry_till_success(
            self.cql_cluster_session,
            node,
            keyspace=keyspace,
            user=user,
            password=password,
            timeout=timeout,
            request_timeout=request_timeout,
            compression=compression,
            protocol_version=protocol_version,
            port=port,
            ssl_opts=ssl_opts,
            topology_event_refresh_window=topology_event_refresh_window,
            exclusive=exclusive,
            bypassed_exception=NoHostAvailable,
            **kwargs
        )

    def create_ks(self, session, name, rf):
        query = 'CREATE KEYSPACE %s WITH replication={%s}'
        if isinstance(rf, int):
            # we assume simpleStrategy
            session.execute(query % (name, "'class':'SimpleStrategy', 'replication_factor':%d" % rf))
        else:
            assert len(rf) != 0, "At least one datacenter/rf pair is needed"
            # we assume networkTopologyStrategy
            options = (', ').join(['\'%s\':%d' % (d, r) for d, r in rf.items()])
            session.execute(query % (name, "'class':'NetworkTopologyStrategy', %s" % options))
        session.execute('USE %s' % name)

    # We default to UTF8Type because it's simpler to use in tests
    def create_cf(self, session, name, key_type="varchar", speculative_retry=None, read_repair=None, compression=None,
                  gc_grace=None, columns=None, validation="UTF8Type", compaction=None, compact_storage=False,
                  default_ttl=None, dclocal_read_repair_chance=None, debug_query=True, caching=True):

        additional_columns = ""
        if columns is not None:
            for k, v in columns.items():
                additional_columns = "%s, %s %s" % (additional_columns, k, v)

        if additional_columns == "":
            query = 'CREATE COLUMNFAMILY %s (key %s, c varchar, v varchar, PRIMARY KEY(key, c)) WITH comment=\'test cf\'' % (
                name, key_type)
        else:
            query = 'CREATE COLUMNFAMILY %s (key %s PRIMARY KEY%s) WITH comment=\'test cf\'' % (
                name, key_type, additional_columns)

        if compression is not None:
            query = '%s AND compression = { \'sstable_compression\': \'%sCompressor\' }' % (query, compression)
        else:
            # if a compression option is omitted, C* will default to lz4 compression
            query += ' AND compression = {}'

        if read_repair is not None:
            query = '%s AND read_repair_chance=%f' % (query, read_repair)
        if gc_grace is not None:
            query = '%s AND gc_grace_seconds=%d' % (query, gc_grace)
        if default_ttl is not None:
            query = '%s AND default_time_to_live=%d' % (query, default_ttl)
        if speculative_retry is not None:
            query = '%s AND speculative_retry=\'%s\'' % (query, speculative_retry)
        if dclocal_read_repair_chance is not None:
            query = '%s AND dclocal_read_repair_chance=\'%s\'' % (query, dclocal_read_repair_chance)
        if compaction is not None:
            query = '%s AND compaction=%s' % (query, compaction)
        if compact_storage:
            query += ' AND COMPACT STORAGE'
        if not caching:
            query = '%s AND caching={\'enabled\':false}' % query

        if debug_query:
            debug(query)

        session.execute(query)
        time.sleep(0.2)

    def create_index(self, session, table_name, index_column, index_name=None, compaction=None):
        query = "CREATE INDEX {index_name} ON {table_name} ({index_column})"
        self._index_creation(session=session, query=query, table_name=table_name, index_column=index_column,
                             index_name=index_name, compaction=compaction)

    def create_local_index(self, session, table_name, pk_name, index_column, index_name=None, compaction=None):
        query = "CREATE INDEX {index_name} ON {table_name} ((%s), {index_column})" % pk_name
        self._index_creation(session=session, query=query, table_name=table_name, index_column=index_column,
                             index_name=index_name, compaction=compaction)

    def _index_creation(self, session, query, table_name, index_column, index_name=None, compaction=None):
        index_column = [index_column] if isinstance(index_column, str) else index_column
        index_column = ', '.join([i for i in index_column])
        query = query.format(**locals())
        debug('Create index: {}'.format(query))
        session.execute(query)
        if compaction:
            # Update appropriate to index materialized view with compaction storage
            session.execute(
                'ALTER MATERIALIZED VIEW {}_index WITH compaction={}'.format(index_name, {'class': compaction}))
        debug('Index {} has been created'.format(index_name))

    @classmethod
    def tearDownClass(cls):
        global PRESERVED_CLUSTER
        if PRESERVED_CLUSTER is not None:
            cls._cls_cleanup_cluster(PRESERVED_CLUSTER, PRESERVED_CLUSTER.get_path(), False, cluster_id_allocator)
            PRESERVED_CLUSTER = None

        reset_environment_vars()
        if os.path.exists(LAST_TEST_DIR):
            with open(LAST_TEST_DIR) as f:
                test_path = f.readline().strip('\n')
                name = f.readline()
                try:
                    cluster = ClusterFactory.load(test_path, name)
                    # Avoid waiting too long for node to be marked down
                    if KEEP_TEST_DIR:
                        cluster.stop(gently=RECORD_COVERAGE)
                    else:
                        cluster.remove()
                        os.rmdir(test_path)
                except IOError:
                    # after a restart, /tmp will be emptied so we'll get an IOError when loading the old cluster here
                    pass
            try:
                os.remove(LAST_TEST_DIR)
            except IOError:
                # Ignore - see comment above
                pass

    def tearDown(self):
        reset_environment_vars()

        for runner in self.runners:
            try:
                runner.stop(timeout=300)
            except:
                pass

        for con in self.connections:
            con.cluster.shutdown()

        self.cleanUpCluster()

    def cleanUpCluster(self):
        if not hasattr(self, 'cluster') or not self.cluster:
            return
        failed = sys.exc_info() != (None, None, None)
        if failed:
            exc_type, exc_value = sys.exc_info()[:2]
            debug("Test failed with exception: {}: {}".format(exc_type, exc_value))
        if hasattr(self, '_outcome') and self._outcome is not None:
            if not self._outcome.success:
                failed = True
                debug("Test failed with unsuccessful outcome")
            if self._outcome.errors:
                failed = True
                debug("Test failed with errors: {}".format(self._outcome.errors))
        if hasattr(self, 'allow_log_errors'):
            warning('allow_log_errors is deprecated. Use ignore_log_patterns instead! {}')
        if not self._preserve_cluster:
            debug("Stopping cluster")
            # we may stop nodes that have not finished starting yet
            self.ignore_log_patterns += [
                r'(Startup|start) failed: seastar::sleep_aborted',
                r'Timer callback failed: seastar::gate_closed_exception',
            ]
            self.cluster.stop()
        found_cores = []
        ignored_cores = []
        try:
            critical_errors = []
            found_errors = []
            for node in self.cluster.nodelist():
                try:
                    critical_errors_pattern = r'Assertion.*failed|AddressSanitizer'
                    if self.ignore_cores_log_patterns:
                        expr = '|'.join(["({})".format(p) for p in set(self.ignore_cores_log_patterns)])
                        matches = node.grep_log(expr)
                        if matches:
                            debug("Will ignore cores on {}. Found the following log messages: {}".format(node.name, matches))
                            self.ignore_cores.append(node)
                    if not node in self.ignore_cores:
                        critical_errors_pattern += "|Aborting"
                    matches = node.grep_log(critical_errors_pattern)
                    if matches:
                        critical_errors.append((node.name, [m[0].strip() for m in matches]))
                except FileNotFoundError:
                    pass
                errors = list(self.__filter_errors(node.grep_log_for_errors(distinct_errors=True)))
                if len(errors):
                    found_errors.append((node.name, errors))
            if critical_errors:
                raise AssertionError('Critical errors found: {}\nOther errors: {}'.format(
                    critical_errors, found_errors))
            if found_errors:
                raise AssertionError('Unexpected errors found: {}'.format(found_errors))
            found_cores, ignored_cores = self.find_cores()
            if found_cores:
                raise AssertionError("Core file(s) found.{}".format("" if failed else " Marking test as failed."))
        except:
            failed = True
            raise
        finally:
            try:
                if failed or KEEP_LOGS or found_cores or ignored_cores:
                    self.copy_logs(cores=list(found_cores + ignored_cores))
            except Exception as e:
                print("Error saving log:", str(e))
            finally:
                if failed or not self._preserve_cluster:
                    self._cleanup_cluster()
                    self.cluster = None
                else:
                    # test passed and preserving is set
                    # removing the LAST_TEST_DIR as the test ended
                    if os.path.exists(LAST_TEST_DIR):
                        os.remove(LAST_TEST_DIR)
                    # preserving cluster
                    global PRESERVED_CLUSTER
                    PRESERVED_CLUSTER = self.cluster
                    self.cluster = None

    def cleanUpTest(self):
        global CURRENT_TEST, CURRENT_TEST_NESTING
        if CURRENT_TEST == "":
            debug("CURRENT_TEST is empty on cleanUpTest. CURRENT_TEST_NESTING={}".format(CURRENT_TEST_NESTING))
        CURRENT_TEST_NESTING -= 1
        if not CURRENT_TEST_NESTING:
            CURRENT_TEST = ""

    def go(self, func):
        runner = Runner(func)
        self.runners.append(runner)
        runner.start()
        return runner

    def skip(self, msg):
        if not NO_SKIP:
            raise SkipTest(msg)

    def __setup_jacoco(self, cluster_name='test'):
        """Setup JaCoCo code coverage support"""
        # use explicit agent and execfile locations
        # or look for a cassandra build if they are not specified
        cdir = CASSANDRA_DIR

        agent_location = os.environ.get('JACOCO_AGENT_JAR', os.path.join(cdir, 'build/lib/jars/jacocoagent.jar'))
        jacoco_execfile = os.environ.get('JACOCO_EXECFILE', os.path.join(cdir, 'build/jacoco/jacoco.exec'))

        if os.path.isfile(agent_location):
            debug("Jacoco agent found at {}".format(agent_location))
            with open(os.path.join(
                    self.test_path, cluster_name, 'cassandra.in.sh'), 'w') as f:

                f.write('JVM_OPTS="$JVM_OPTS -javaagent:{jar_path}=destfile={exec_file}"'
                        .format(jar_path=agent_location, exec_file=jacoco_execfile))

                if os.path.isfile(jacoco_execfile):
                    debug("Jacoco execfile found at {}, execution data will be appended".format(jacoco_execfile))
                else:
                    debug("Jacoco execfile will be created at {}".format(jacoco_execfile))
        else:
            debug("Jacoco agent not found or is not file. Execution will not be recorded.")

    def __filter_errors(self, errors, patterns=None):
        """Filter errors, removing those that match patterns"""
        if not patterns:
            patterns = []
        patterns += self.ignore_log_patterns
        patterns += self.ignore_cores_log_patterns
        patterns += [
            r'Compaction for .* deliberately stopped',
            r'update compaction history failed:.*ignored',
        ]
        # ignore expected rpc errors when nodes are stopped.
        expected_rpc_errors = [
            'connection dropped: connection is closed',
            'connection dropped: .*Connection reset by peer',
            'connection dropped: Semaphore broken',
            'fail to connect: Connection refused',
            'fail to connect: Connection reset by peer',
            'server stream connection dropped: invalid type specifier',
            'server stream connection dropped: Unknown parent connection',
        ]
        patterns += ["rpc - client .*({})".format('|'.join(expected_rpc_errors))]
        pattern = re.compile('|'.join(["({})".format(p) for p in set(patterns)]))
        for e in errors:
            if not pattern.search(e):
                yield e

    def check_errors(self, node, exclude_errors=None, search_str=None, from_mark=None, regex=False):
        if from_mark != None:
            node.error_mark = from_mark
        errors = node.grep_log_for_errors(distinct_errors=True, search_str=search_str)

        if exclude_errors:
            if not isinstance(exclude_errors, list):
                exclude_errors = [exclude_errors]
            if not regex:
                exclude_errors = [re.escape(ee) for ee in list(exclude_errors)]
        errors = list(self.__filter_errors(errors, exclude_errors))

        if errors:
            assert False, '\n'.join(list(errors))

        if exclude_errors:
            self.ignore_log_patterns = list(set(self.ignore_log_patterns + exclude_errors))

    def check_errors_all_nodes(self, nodes=None, exclude_errors=None, search_str=None, regex=False):
        if nodes is None:
            nodes = self.cluster.nodelist()
        for node in nodes:
            self.check_errors(node=node, exclude_errors=exclude_errors, search_str=search_str, regex=regex)

    def get_ip_from_node(self, node):
        if node.network_interfaces['binary']:
            node_ip = node.network_interfaces['binary'][0]
        else:
            node_ip = node.network_interfaces['thrift'][0]
        return node_ip

    def get_port_from_node(self, node):
        """
        Return the port that this node is listening on.
        We only use this to connect the native driver,
        so we only care about the binary port.
        """
        try:
            return node.network_interfaces['binary'][1]
        except Exception:
            raise RuntimeError("No network interface defined on this node object. {}".format(node.network_interfaces))

    def get_auth_provider(self, user, password):
        return PlainTextAuthProvider(username=user, password=password)

    def make_auth(self, user, password):
        def private_auth(node_ip):
            return {'username': user, 'password': password}
        return private_auth

    # Disable docstrings printing in nosetest output
    def shortDescription(self):
        return None

    def wait_for_any_log(self, nodes, patterns, timeout, dispersed=False):
        """
        Look for a pattern in the system.log of any in a given list
        of nodes.
        :param nodes: The list of nodes whose logs to scan
        :param patterns: The target pattern (a string, or a list of strings)
        :param timeout: How long to wait for the pattern. Note that
                        strictly speaking, timeout is not really a timeout,
                        but a maximum number of attempts. This implies that
                        the all the grepping takes no time at all, so it is
                        somewhat inaccurate, but probably close enough.
        :return: The first node in whose log the pattern was found, if not dispersed.
                 Otherwise, if dispersed=True, return a list of all nodes with the any of the patterns.
        """

        if dispersed:
            remaining = patterns
            ret = []
            for _ in range(timeout):
                for node in nodes:
                    for p in remaining:
                        try:
                            if node.watch_log_for(p, timeout=0):
                                remaining.remove(p)
                                if node not in ret:
                                    ret.append(node)
                        except TimeoutError:
                            pass
                if not remaining:
                    return ret
                time.sleep(1)
        else:
            for _ in range(timeout):
                for node in nodes:
                    try:
                        found = node.watch_log_for(patterns, timeout=0)
                        if found:
                            return node
                    except TimeoutError:
                        pass
                time.sleep(1)

        raise TimeoutError(time.strftime("%d %b %Y %H:%M:%S", time.gmtime()) +
                           (" Unable to find :%s in any node log within " % patterns) + str(timeout) + "s")

    def _prometheus_get(self, ip, port='9180'):
        prometheus_url = 'http://{}:{}/metrics'.format(ip, port)
        resp = requests.get(prometheus_url)
        if resp.status_code not in [200, 201, 202]:
            raise 'Failed getting metrics from server! error: {}'.format(resp.text)
        return resp.text

    def get_node_metrics(self, node_ip, port='9180', metrics=None):
        metrics = metrics or []
        metrics_res = {}
        if metrics:
            for metric in self._prometheus_get(node_ip, port).splitlines():
                for metric_name in metrics:
                    if not metric.startswith('#') and re.search(metric_name, metric):
                        val = metric.split()[-1]
                        try:
                            val = int(val)
                        except ValueError:
                            val = float(val)
                        metrics_res[metric_name] = val if metric_name not in metrics_res\
                            else metrics_res[metric_name] + val
        return metrics_res

    def enable_error(self, name, node, one_shot=False):
        """Enable error injection

        Args:
            name (str): name of error injection to be enabled.
            node (ScyllaNode|int): either instance of scylla node or node number.
            one_shot (bool): indicates whether the injection is one-shot
                             (resets enabled state after triggering the injection).

        """
        if isinstance(node, int):
            node = self.cluster.nodelist()[node]
        node_ip = self.get_ip_from_node(node)
        debug(f'Enabling error injection "{name}" on node {node_ip}')
        response = requests.post(f"http://{node_ip}:10000/v2/error_injection/injection/{name}",
                                 params={"one_shot": one_shot})
        response.raise_for_status()

    def disable_error(self, name, node):
        """Disable error injection

        Args:
            name (str): name of error injection to be disabled.
            node (ScyllaNode|int): either instance of scylla node or node number.

        """
        if isinstance(node, int):
            node = self.cluster.nodelist()[node]
        node_ip = self.get_ip_from_node(node)
        debug(f'Disabling error injection "{name}" on node {node_ip}')
        response = requests.delete(f"http://{node_ip}:10000/v2/error_injection/injection/{name}")
        response.raise_for_status()

    def check_error(self, name, node):
        """Get status of error injection

        Args:
            name (str): name of error injection.
            node (ScyllaNode|int): either instance of scylla node or node number.

        """
        if isinstance(node, int):
            node = self.cluster.nodelist()[node]
        node_ip = self.get_ip_from_node(node)
        response = requests.get(f"http://{node_ip}:10000/v2/error_injection/injection/{name}")
        response.raise_for_status()

    def list_errors(self, node):
        """List enabled error injections

        Args:
            node (ScyllaNode|int): either instance of scylla node or node number.

        """
        if isinstance(node, int):
            node = self.cluster.nodelist()[node]
        node_ip = self.get_ip_from_node(node)
        response = requests.get(f"http://{node_ip}:10000/v2/error_injection/injection")
        response.raise_for_status()
        return response.json()

    def is_autocompaction_enabled(self, node, ks_name, table_name):
        """
        Return if autocompaction is enabled or not
        :param node: node to execute the API request
        :param ks_name: Keyspace name to verify if autocompaction is enabled
        :param table_name: table name to verify if autocompaction is enabled
        :return: True|False
        """
        node_ip = self.get_ip_from_node(node=node)
        response = requests.get(f'http://{node_ip}:10000/column_family/autocompaction/{ks_name}:{table_name}')
        response.raise_for_status()
        return response.json()

    def disable_errors(self, node):
        """Disable all error injections

        Args:
            node (ScyllaNode|int): either instance of scylla node or node number.

        """
        if isinstance(node, int):
            node = self.cluster.nodelist()[node]
        node_ip = self.get_ip_from_node(node)
        response = requests.delete(f"http://{node_ip}:10000/v2/error_injection/injection")
        response.raise_for_status()

    def create_self_signed_x509_certificate(self, cert_file='scylla.crt', key_file='scylla.key'):
        cert_file = os.path.join(self.test_path, 'test', cert_file)
        key_file = os.path.join(self.test_path, 'test', key_file)

        # Create private RSA key
        rsa_key = crypto.PKey()
        rsa_key.generate_key(crypto.TYPE_RSA, 2048)

        # create a self-signed cert
        cert = crypto.X509()
        cert.get_subject().C = "IL"
        cert.get_subject().ST = "None"
        cert.get_subject().L = "None"
        cert.get_subject().O = "None"
        cert.get_subject().OU = "None"
        cert.get_subject().CN = gethostname()
        cert.set_serial_number(1000)
        cert.gmtime_adj_notBefore(0)
        cert.gmtime_adj_notAfter(24 * 60 * 60)
        cert.set_issuer(cert.get_subject())
        cert.set_pubkey(rsa_key)
        cert.sign(rsa_key, 'sha512')

        with open(file=cert_file, mode='w') as file:
            file.write(crypto.dump_certificate(crypto.FILETYPE_PEM, cert).decode())
        with open(file=key_file, mode='w') as file:
            file.write(crypto.dump_privatekey(crypto.FILETYPE_PEM, rsa_key).decode())

        # When tests are run with HTTPS, the server often won't have its SSL
        # certificate signed by a known authority. So we will disable certificate
        # verification with the "verify=False" request option. However, once we do
        # that, we start getting scary-looking warning messages, saying that this
        # makes HTTPS insecure. The following silences those warnings:
        warnings.filterwarnings('ignore', message='Unverified HTTPS request')
        debug(f'Created certificate file in "{cert_file}" path, and private key in "{key_file}" path')
        return cert_file, key_file

    def cql_timeout(self, seconds=None):
        if not seconds:
            seconds = self.base_cql_timeout
        factor = 1
        if isinstance(self.cluster, ScyllaCluster):
            if self.cluster.scylla_mode == 'debug':
                factor = 3
            elif self.cluster.scylla_mode != 'release':
                factor = 2
        return seconds * factor


@attr('reuse-cluster')
class TesterReuseCluster(Tester):
    _multiprocess_can_split_ = not REUSE_CLUSTER


class MultiError(Exception):
    """
    Extends Exception to provide reporting multiple exceptions at once.
    """

    def __init__(self, exceptions, tracebacks):
        # an exception and the corresponding traceback should be found at the same
        # position in their respective lists, otherwise __str__ will be incorrect
        self.exceptions = exceptions
        self.tracebacks = tracebacks

    def __str__(self):
        output = "\n****************************** BEGIN MultiError ******************************\n"

        for (exc, tb) in zip(self.exceptions, self.tracebacks):
            output += str(exc)
            output += tb + "\n"

        output += "****************************** END MultiError ******************************"

        return output


def run_scenarios(scenarios, handler, deferred_exceptions=tuple()):
    """
    Runs multiple scenarios from within a single test method.

    "Scenarios" are mini-tests where a common procedure can be reused with several different configurations.
    They are intended for situations where complex/expensive setup isn't required and some shared state is acceptable (or trivial to reset).

    Arguments: scenarios should be an iterable, handler should be a callable, and deferred_exceptions should be a tuple of exceptions which
    are safe to delay until the scenarios are all run. For each item in scenarios, handler(item) will be called in turn.

    Exceptions which occur will be bundled up and raised as a single MultiError exception, either when: a) all scenarios have run,
    or b) on the first exception encountered which is not whitelisted in deferred_exceptions.
    """
    errors = []
    tracebacks = []

    for i, scenario in enumerate(scenarios, 1):
        debug("running scenario {}/{}: {}".format(i, len(scenarios), scenario))

        try:
            handler(scenario)
        except deferred_exceptions as e:
            tracebacks.append(traceback.format_exc(sys.exc_info()))
            errors.append(type(e)('encountered {} {} running scenario:\n  {}\n'.format(
                e.__class__.__name__, str(e), scenario)))
            debug("scenario {}/{} encountered a deferrable exception, continuing".format(i, len(scenarios)))
        except Exception as e:
            # catch-all for any exceptions not intended to be deferred
            tracebacks.append(traceback.format_exc(sys.exc_info()))
            errors.append(type(e)('encountered {} {} running scenario:\n  {}\n'.format(
                e.__class__.__name__, str(e), scenario)))
            debug("scenario {}/{} encountered a non-deferrable exception, aborting".format(i, len(scenarios)))
            raise MultiError(errors, tracebacks)

    if errors:
        raise MultiError(errors, tracebacks)


class retrying(object):
    """
        Used as a decorator to retry function run that can possibly fail with allowed exceptions list
    """

    def __init__(self, num_attempts=3, sleep_time=1, allowed_exceptions=(Exception,), message="", tear_down_on_failure=False):
        self.num_attempts = num_attempts  # number of times to retry
        self.sleep_time = sleep_time  # number seconds to sleep between retries
        self.allowed_exceptions = allowed_exceptions  # if Exception is not allowed will raise
        self.message = message  # string that will be printed between retries
        self.tear_down_on_failure = tear_down_on_failure

    def __call__(self, func):
        def inner(*args, **kwargs):
            func_args = inspect.getargspec(func)
            num_attempts = self.num_attempts
            if 'num_attempts' in func_args.args:
                num_attempts = kwargs.get('num_attempts')
                if not num_attempts:
                    default_args = func_args.args[-len(func_args.defaults):]
                    num_attempts_position = default_args.index('num_attempts')
                    num_attempts = func_args.defaults[num_attempts_position]

            for i in range(num_attempts - 1):
                try:
                    if self.message:
                        debug("trying {} [{}/{}] ({})".format(func.__name__, i+1, num_attempts, self.message))
                    return func(*args, **kwargs)
                except self.allowed_exceptions as e:
                    if self.tear_down_on_failure:
                        tmp_ignore_log_patterns = args[0].ignore_log_patterns
                        args[0].ignore_log_patterns = [r'.*']
                        args[0].tearDown()
                        args[0].setUp()
                        args[0].ignore_log_patterns = tmp_ignore_log_patterns
                    debug("{} [{}/{}]: {}: will retry in {} second(s)".format(func.__name__,
                                                                              i+1, num_attempts, e, self.sleep_time))
                    time.sleep(self.sleep_time)
            if self.message:
                debug("trying {} [last try] ({})".format(func.__name__, self.message))
            return func(*args, **kwargs)

        return inner


# Ignore decorator's num_attempts. Take num_attempts from function arguments
retry_with_func_attempts = retrying(num_attempts=1, sleep_time=10)
flaky = retrying(num_attempts=5, sleep_time=3, message="Flaky test")
flaky_with_tear_down = retrying(num_attempts=5, sleep_time=3, message="Flaky test", tear_down_on_failure=True)


class run_with_params(object):
    """
       Will run function with different arguments provided as arguments for the decorator.
       Example:
            In [1]: @run_with_params(num=[1,2,3],what=["pryanik", "pastila"], persons=["Bentsi", "Nastya"])
                ...: def give(num, what, persons):
                ...:     print "Giving {num} {what} to {persons}".format(**locals())

            In [2]: give()
            Giving 1 pryanik to Bentsi
            Giving 2 pryanik to Bentsi
            Giving 3 pryanik to Bentsi
            Giving 1 pastila to Bentsi
            Giving 2 pastila to Bentsi
            Giving 3 pastila to Bentsi
            Giving 1 pryanik to Nastya
            Giving 2 pryanik to Nastya
            Giving 3 pryanik to Nastya
            Giving 1 pastila to Nastya
            Giving 2 pastila to Nastya
            Giving 3 pastila to Nastya
        Decorator is inspired by Pytest's parameterize
    """

    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs

    def __call__(self, func):
        @wraps(func)
        def inner(*args, **kwargs):
            l = []
            for key, values in self.kwargs.items():
                vals_product = list(itertools.product(*[[key], values]))
                l.append(vals_product)
            cartesian_product = list(itertools.product(*l))
            for arg in cartesian_product:
                kwargs.update({k: v for k, v in arg})
                func(*(args + self.args), **kwargs)
        return inner
