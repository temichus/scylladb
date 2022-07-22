import pytest
import glob
import os
import shutil
import time
import logging
import re
import tempfile
import subprocess
import sys
import errno
import pprint
import random
from collections import OrderedDict
from functools import partial, partialmethod

import requests
from cassandra.cluster import Cluster as PyCluster, default_lbp_factory
from cassandra.cluster import NoHostAvailable
from cassandra.cluster import EXEC_PROFILE_DEFAULT
from cassandra.policies import WhiteListRoundRobinPolicy
from ccmlib.common import is_win
from ccmlib.cluster import Cluster
from ccmlib.scylla_cluster import ScyllaCluster

from dtest_class import (get_ip_from_node, make_execution_profile, get_auth_provider, get_port_from_node,
                         get_eager_protocol_version)
from packaging.version import Version

from dtest_config import DTestConfig
from tools.context import log_filter
from tools.funcutils import merge_dicts
from tools.log_utils import remove_control_chars
from tools.log_utils import DisableLogger

logger = logging.getLogger(__name__)

# Add custom TRACE level, for development print we don't want on debug level
logging.TRACE = 5
logging.addLevelName(logging.TRACE, 'TRACE')
logging.Logger.trace = partialmethod(logging.Logger.log, logging.TRACE)
logging.trace = partial(logging.log, logging.TRACE)


class RandomClusterIdAllocator(object):
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
                logger.debug("Allocated cluster ID {}: {}".format(id, cluster_dir))
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
                                    logger.debug("Could not rename {}: {}".format(link, e3))
                            else:
                                os.remove(tmp_link)
                    continue
                raise Exception("Exception while allocating cluster ID {}: {}".format(id, e))
        raise Exception("Could not allocate cluster ID after {} retries".format(self._retries))

    def free(self, id):
        link = self._links.pop(id, None)
        logger.debug("Freeing cluster ID {}: link {}".format(id, link))
        if not link:
            raise AssertionError("No link found for ID {}".format(id))
        try:
            os.remove(link)
        except OSError as e:
            logger.warning("Could not remove link {}: {}".format(link, e))


cluster_id_allocator = RandomClusterIdAllocator()

KEEP_CORES = os.environ.get('KEEP_CORES', 'true').lower() in ('yes', 'true')
DTEST_CORE_COMPRESS_TOOL = os.environ.get('DTEST_CORE_COMPRESS_TOOL', 'gzip')
DTEST_CORE_COMPRESS_EXT = os.environ.get('DTEST_CORE_COMPRESS_EXT', 'gz')


def copy_logs(request, dtest_config, directory=None, name=None, cores=None):
    """Copy the current cluster's log files somewhere, by default to LOG_SAVED_DIR with a name of 'last'"""
    log_saved_dir = os.environ.get('LOG_SAVED_DIR', "logs")
    try:
        os.mkdir(log_saved_dir)
    except OSError:
        pass

    if directory is None:
        directory = log_saved_dir
    if name is None:
        name = os.path.join(log_saved_dir, "last")
    else:
        name = os.path.join(directory, name)
    if not os.path.exists(directory):
        os.mkdir(directory)

    basedir = str(int(time.time() * 1000)) + '_' + request.node.nodeid.replace('/', '_')
    # figure out the system max filename length (since test full name might be bigger than it)
    # if it's bigger we just truncate to the system max
    name_max = os.pathconf(directory, "PC_NAME_MAX")
    logdir = os.path.join(directory, basedir[:name_max])
    os.mkdir(logdir)

    cluster_path = dtest_config.cluster.get_path()

    for log in glob.glob(os.path.join(cluster_path, '**/logs/*'), recursive=True):
        n = re.search(r'node\d+', log).group(0)
        logname = os.path.basename(log)
        # for backward compatibility, rename the logs:
        #   nodeX/logs/system.log to nodeX.log
        #   nodeX/logs/debug.log to nodeX_debug.log
        if logname == 'system.log':
            dest = n + '.log'
        else:
            dest = "{}_{}".format(n, logname)
        shutil.copyfile(log, os.path.join(logdir, dest))

    if hasattr(dtest_config.cluster, '_scylla_manager') and dtest_config.cluster._scylla_manager:
        log = os.path.join(dtest_config.cluster._scylla_manager._get_path(), 'scylla-manager.log')
        if os.path.exists(log):
            shutil.copyfile(log, os.path.join(logdir, 'scylla-manager.log'))

        logs = [(node.name, node.logfilename() + ".manager_agent") for node in dtest_config.cluster.nodes.values()]
        if len(logs):
            for name, agent_log in logs:
                if os.path.exists(agent_log):
                    shutil.copyfile(agent_log, os.path.join(logdir, name + ".log.manager_agent"))

    if KEEP_CORES:
        if cores is None:
            cores, ignored_cores = dtest_config.find_cores()
            cores += ignored_cores
        if cores:
            for n, src in cores:
                dst = os.path.join(logdir, "{}-{}".format(n, os.path.basename(src)))
                logger.warning("Moving core file {} to {}".format(src, dst))
                try:
                    if DTEST_CORE_COMPRESS_TOOL == '':
                        cmd = "mv {} {}".format(src, dst)
                        shutil.move(src, dst)
                    else:
                        cmd = "{} < {} > {}.{} && rm {}".format(
                            DTEST_CORE_COMPRESS_TOOL, src, dst, DTEST_CORE_COMPRESS_EXT, src)
                        subprocess.check_call(cmd, shell=True)
                except Exception as e:
                    logger.warning("`{}` failed: {}. Keeping directory.".format(cmd, e))

    if os.path.exists(logdir):
        if os.path.exists(name):
            os.unlink(name)
        if not is_win():
            os.symlink(basedir, name)


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


class DTestSetup:
    def __init__(self, dtest_config: DTestConfig = None, setup_overrides=None, cluster_name="test", prefix='dtest-'):
        self.dtest_config = dtest_config
        self.setup_overrides = setup_overrides
        self.cluster_name = cluster_name
        self.ignore_log_patterns = []
        self.ignore_cores_log_patterns = []
        self.ignore_cores = []
        self.cluster = None
        self.cluster_options = []
        self.replacement_node = None
        self.allow_log_errors = False
        self.connections = []

        self.log_saved_dir = "logs"
        try:
            os.mkdir(self.log_saved_dir)
        except OSError:
            pass

        self.last_log = os.path.join(self.log_saved_dir, "last")
        self.test_path = self.get_test_path(prefix=prefix)
        self.enable_for_jolokia = False
        self.subprocs = []
        self.last_test_dir = "last_test_dir"
        self.jvm_args = []
        self.create_cluster_func = None
        self.iterations = 0
        self.runners = []
        self.base_cql_timeout = 10  # seconds
        self.cql_request_timeout = None

    def get_test_path(self, prefix='dtest-'):
        # we can not work /tmp
        dtest_root = os.path.join(os.path.expanduser("~"), '.dtest')
        if not os.path.exists(dtest_root):
            os.makedirs(dtest_root)
        test_path = tempfile.mkdtemp(prefix=prefix, dir=dtest_root)

        # ccm on cygwin needs absolute path to directory - it crosses from cygwin space into
        # regular Windows space on wmic calls which will otherwise break pathing
        if sys.platform == "cygwin":
            process = subprocess.Popen(["cygpath", "-m", test_path], stdout=subprocess.PIPE,
                                       stderr=subprocess.STDOUT)
            test_path = process.communicate()[0].rstrip()

        return test_path

    def glob_data_dirs(self, path, ks="ks"):
        result = []
        for node in self.cluster.nodelist():
            for data_dir in [os.path.join(node.get_path(), 'data')]:
                ks_dir = os.path.join(data_dir, ks, path)
                result.extend(glob.glob(ks_dir))
        return result

    def copy_logs(self, request, directory=None, name=None, cores=None):
        """Copy the current cluster's log files somewhere, by default to LOG_SAVED_DIR with a name of 'last'"""
        copy_logs(request, self, directory=directory, name=name, cores=cores)

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
                            logger.debug(
                                "Ignoring core file {} belonging to {} due to ignore_cores_log_patterns".format(path, node.name))
                            ignored_cores += [(node.name, path)]
        # returns empty list if no core files found
        return cores, ignored_cores

    def cql_connection(self, node, keyspace=None, user=None,
                       password=None, compression=True, protocol_version=None, port=None, ssl_opts=None, **kwargs):

        return self._create_session(node, keyspace, user, password, compression,
                                    protocol_version, port=port, ssl_opts=ssl_opts, **kwargs)

    def cql_cluster_session(self, node, keyspace=None, user=None,
                            password=None, compression=True, protocol_version=None, port=None, ssl_opts=None,
                            topology_event_refresh_window=10, request_timeout=None, exclusive=False, **kwargs):

        if exclusive:
            node_ip = get_ip_from_node(node)
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

    def exclusive_cql_connection(self, node, keyspace=None, user=None,
                                 password=None, compression=True, protocol_version=None, port=None, ssl_opts=None,
                                 **kwargs):

        node_ip = get_ip_from_node(node)
        wlrr = WhiteListRoundRobinPolicy([node_ip])

        return self._create_session(node, keyspace, user, password, compression,
                                    protocol_version, port=port, ssl_opts=ssl_opts, load_balancing_policy=wlrr,
                                    **kwargs)

    def _create_session(self, node, keyspace, user, password, compression, protocol_version,
                        port=None, ssl_opts=None, execution_profiles=None, topology_event_refresh_window=10,
                        request_timeout=None, keep_session=True, ssl_context=None, **kwargs):
        node_ip = get_ip_from_node(node)
        if not port:
            port = get_port_from_node(node)

        if protocol_version is None:
            protocol_version = get_eager_protocol_version(node.cluster.version())

        if user is not None:
            auth_provider = get_auth_provider(user=user, password=password)
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
                            allow_beta_protocol_version=True,
                            topology_event_refresh_window=topology_event_refresh_window,
                            execution_profiles=profiles,
                            ssl_context=ssl_context)
        session = cluster.connect(wait_for_all_pools=True)

        if keyspace is not None:
            session.set_keyspace(keyspace)

        if keep_session:
            self.connections.append(session)

        return session

    def patient_cql_connection(self, node, keyspace=None,
                               user=None, password=None, timeout=30, compression=True,
                               protocol_version=None, port=None, ssl_opts=None, **kwargs):
        """
        Returns a connection after it stops throwing NoHostAvailables due to not being ready.

        If the timeout is exceeded, the exception is raised.
        """
        if is_win():
            timeout *= 2

        expected_log_lines = ('Control connection failed to connect, shutting down Cluster:',
                              '[control connection] Error connecting to ')
        with log_filter('cassandra.cluster', expected_log_lines):
            session = retry_till_success(
                self.cql_connection,
                node,
                keyspace=keyspace,
                user=user,
                password=password,
                timeout=timeout,
                compression=compression,
                protocol_version=protocol_version,
                port=port,
                ssl_opts=ssl_opts,
                bypassed_exception=NoHostAvailable,
                **kwargs
            )

        return session

    def patient_exclusive_cql_connection(self, node, keyspace=None,
                                         user=None, password=None, timeout=30, compression=True,
                                         protocol_version=None, port=None, ssl_opts=None, **kwargs):
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
            compression=compression,
            protocol_version=protocol_version,
            port=port,
            ssl_opts=ssl_opts,
            bypassed_exception=NoHostAvailable,
            **kwargs
        )

    def check_errors(self, node, exclude_errors=None, search_str=None, from_mark=None, regex=False, return_errors=False):
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
            if not return_errors:
                assert False, '\n'.join(list(errors))

        if return_errors:
            return list(errors)

        if exclude_errors:
            self.ignore_log_patterns += exclude_errors

    def check_errors_all_nodes(self, nodes=None, exclude_errors=None, search_str=None, regex=False):
        if nodes is None:
            nodes = self.cluster.nodelist()

        critical_errors = []
        found_errors = []
        for node in nodes:
            try:
                critical_errors_pattern = r'Assertion.*failed|AddressSanitizer'
                if self.ignore_cores_log_patterns:
                    expr = '|'.join(["({})".format(p) for p in set(self.ignore_cores_log_patterns)])
                    matches = node.grep_log(expr)
                    if matches:
                        logger.debug("Will ignore cores on {}. Found the following log messages: {}".format(
                            node.name, matches))
                        self.ignore_cores.append(node)
                if node not in self.ignore_cores:
                    critical_errors_pattern += "|Aborting"
                matches = node.grep_log(critical_errors_pattern)
                if matches:
                    critical_errors.append((node.name, [m[0].strip() for m in matches]))
            except FileNotFoundError:
                pass
            errors = self.check_errors(node=node, exclude_errors=exclude_errors, search_str=search_str, regex=regex,
                                       return_errors=True)
            if len(errors):
                found_errors.append((node.name, errors))

        if critical_errors:
            raise AssertionError('Critical errors found: {}\nOther errors: {}'.format(
                critical_errors, found_errors))
        if found_errors:
            raise AssertionError('Unexpected errors found: {}'.format(found_errors))
        found_cores, ignored_cores = self.find_cores()
        if found_cores:
            raise AssertionError("Core file(s) found. Marking test as failed.")

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
        # we may stop nodes that have not finished starting yet
        patterns += [r'(Startup|start) failed: seastar::sleep_aborted',
                     r'Timer callback failed: seastar::gate_closed_exception',
                     ]
        patterns += ["rpc - client .*({})".format('|'.join(expected_rpc_errors))]
        patterns += [" raft_rpc - Failed to send "]
        # We see benign rpc errors when nodes start/stop.
        # If they cause system malfunction, it should be detected using higher-level tests.
        patterns += [r'rpc::unknown_verb_error']
        pattern = re.compile('|'.join(["({})".format(p) for p in set(patterns)]))
        for e in errors:
            if not pattern.search(e):
                yield remove_control_chars(e)

    def get_jfr_jvm_args(self):
        """
        @return The JVM arguments required for attaching flight recorder to a Java process.
        """
        return ["-XX:+UnlockCommercialFeatures", "-XX:+FlightRecorder"]

    def start_jfr_recording(self, nodes):
        """
        Start Java flight recorder provided the cluster was started with the correct jvm arguments.
        """
        for node in nodes:
            p = subprocess.Popen(['jcmd', str(node.pid), 'JFR.start'],
                                 stdout=subprocess.PIPE,
                                 stderr=subprocess.PIPE)
            stdout, stderr = p.communicate()
            logger.debug(stdout)
            logger.debug(stderr)

    def dump_jfr_recording(self, nodes):
        """
        Save Java flight recorder results to file for analyzing with mission control.
        """
        for node in nodes:
            p = subprocess.Popen(['jcmd', str(node.pid), 'JFR.dump',
                                  'recording=1', 'filename=recording_{}.jfr'.format(node.address())],
                                 stdout=subprocess.PIPE,
                                 stderr=subprocess.PIPE)
            stdout, stderr = p.communicate()
            logger.debug(stdout)
            logger.debug(stderr)

    def supports_v5_protocol(self, cluster_version):
        return cluster_version >= Version('4.0')

    def cleanup_last_test_dir(self):
        if os.path.exists(self.last_test_dir):
            os.remove(self.last_test_dir)

    def cleanup_cluster(self):
        with log_filter('cassandra'):  # quiet noise from driver when nodes start going down
            if self.dtest_config.keep_test_dir:
                self.cluster.stop(gently=self.dtest_config.enable_jacoco_code_coverage)
            else:
                # when recording coverage the jvm has to exit normally
                # or the coverage information is not written by the jacoco agent
                # otherwise we can just kill the process
                if self.dtest_config.enable_jacoco_code_coverage:
                    self.cluster.stop(gently=True)

                # Cleanup everything:
                logger.debug("removing ccm cluster {name} at: {path}".format(name=self.cluster.name,
                                                                             path=self.test_path))
                self.cluster.remove()

                logger.debug("clearing ssl stores from [{0}] directory".format(self.test_path))
                for filename in ('keystore.jks', 'truststore.jks', 'ccm_node.cer'):
                    try:
                        os.remove(os.path.join(self.test_path, filename))
                    except OSError as e:
                        # ENOENT = no such file or directory
                        assert e.errno == errno.ENOENT

                # since some leftovers, like ssl keys, etc. can stay in the directory, it's safer to use
                # shutil.rmtree over os.rmdir (or OSError: [Errno 39] Directory not empty might occur)
                shutil.rmtree(self.test_path)
                self.cleanup_last_test_dir()
                cluster_id_allocator.free(self.cluster.id)

    def cleanup_and_replace_cluster(self):
        for con in self.connections:
            con.cluster.shutdown()
        self.connections = []

        self.cleanup_cluster()
        self.test_path = self.get_test_path()
        self.initialize_cluster(self.create_cluster_func)

    def init_default_config(self):
        # the failure detector can be quite slow in such tests with quick start/stop
        phi_values = {'phi_convict_threshold': 5}

        cassandra_v4_cluster = not isinstance(self.cluster, ScyllaCluster) and self.cluster.version() >= '4'

        # enable read time tracking of repaired data between replicas by default
        if cassandra_v4_cluster:
            repaired_data_tracking_values = {'repaired_data_tracking_for_partition_reads_enabled': 'true',
                                             'repaired_data_tracking_for_range_reads_enabled': 'true',
                                             'report_unconfirmed_repaired_data_mismatches': 'true'}
        else:
            repaired_data_tracking_values = {}

        timeout = self.cql_timeout() * 1000
        range_timeout = 3 * timeout
        self.cql_request_timeout = 3 * self.cql_timeout()

        if isinstance(self.cluster, ScyllaCluster):
            logger.debug("Scylla mode is '{}'".format(self.cluster.scylla_mode))
        logger.debug(
            "Cluster *_request_timeout_in_ms={}, range_request_timeout_in_ms={}, cql request_timeout={}".format(
                timeout, range_timeout, self.cql_request_timeout))

        if self.cluster_options is not None and len(self.cluster_options) > 0:
            values = merge_dicts(self.cluster_options, phi_values, repaired_data_tracking_values)
        else:
            values = merge_dicts(phi_values, repaired_data_tracking_values, {
                'read_request_timeout_in_ms': timeout,
                'range_request_timeout_in_ms': range_timeout,
                'write_request_timeout_in_ms': timeout,
                'truncate_request_timeout_in_ms': timeout,
                'request_timeout_in_ms': timeout
            })

        if self.setup_overrides is not None and len(self.setup_overrides.cluster_options) > 0:
            values = merge_dicts(values, self.setup_overrides.cluster_options)

        # No more thrift in 4.0, and start_rpc doesn't exists anymore
        if cassandra_v4_cluster:
            if 'start_rpc' in values:
                del values['start_rpc']
            values['corrupted_tombstone_strategy'] = 'exception'

        if self.dtest_config.use_vnodes:
            self.cluster.set_configuration_options(
                values={'initial_token': None, 'num_tokens': self.dtest_config.num_tokens})
        else:
            self.cluster.set_configuration_options(values={'num_tokens': None})

        if self.dtest_config.use_off_heap_memtables:
            self.cluster.set_configuration_options(values={'memtable_allocation_type': 'offheap_objects'})

        self.cluster.set_configuration_options(values)
        logger.debug("Done setting configuration options:\n" + pprint.pformat(self.cluster._config_options, indent=4))

    def maybe_setup_jacoco(self, cluster_name='test'):
        """Setup JaCoCo code coverage support"""

        if not self.dtest_config.enable_jacoco_code_coverage:
            return

        # use explicit agent and execfile locations
        # or look for a cassandra build if they are not specified
        agent_location = os.environ.get('JACOCO_AGENT_JAR',
                                        os.path.join(self.dtest_config.cassandra_dir, 'build/lib/jars/jacocoagent.jar'))
        jacoco_execfile = os.environ.get('JACOCO_EXECFILE',
                                         os.path.join(self.dtest_config.cassandra_dir, 'build/jacoco/jacoco.exec'))

        if os.path.isfile(agent_location):
            logger.debug("Jacoco agent found at {}".format(agent_location))
            with open(os.path.join(
                    self.test_path, cluster_name, 'cassandra.in.sh'), 'w') as f:

                f.write('JVM_OPTS="$JVM_OPTS -javaagent:{jar_path}=destfile={exec_file}"'
                        .format(jar_path=agent_location, exec_file=jacoco_execfile))

                if os.path.isfile(jacoco_execfile):
                    logger.debug("Jacoco execfile found at {}, execution data will be appended".format(jacoco_execfile))
                else:
                    logger.debug("Jacoco execfile will be created at {}".format(jacoco_execfile))
        else:
            logger.debug("Jacoco agent not found or is not file. Execution will not be recorded.")

    @staticmethod
    def create_ccm_cluster(dtest_setup, skip_manager_server=False):
        logger.info("cluster ccm directory: " + dtest_setup.test_path)
        version = dtest_setup.dtest_config.cassandra_version

        # === scylla change
        scylla_version = dtest_setup.dtest_config.scylla_version
        # TODO: think if we want this
        from ccmlib.scylla_cluster import ScyllaCluster
        from ccmlib.common import isScylla

        if version:
            cluster = Cluster(dtest_setup.test_path, dtest_setup.cluster_name, cassandra_version=version)
        elif scylla_version:
            cluster = ScyllaCluster(dtest_setup.test_path, dtest_setup.cluster_name,
                                    cassandra_version=scylla_version, force_wait_for_cluster_start=True,
                                    skip_manager_server=skip_manager_server)
        else:
            if isScylla(dtest_setup.dtest_config.cassandra_dir):
                cluster = ScyllaCluster(dtest_setup.test_path, dtest_setup.cluster_name,
                                        force_wait_for_cluster_start=True,
                                        install_dir=dtest_setup.dtest_config.cassandra_dir)
            else:
                cluster = Cluster(dtest_setup.test_path, dtest_setup.cluster_name,
                                  cassandra_dir=dtest_setup.dtest_config.cassandra_dir)
        # === scylla change

        # scylla-ccm doesn't support those
        # cluster.set_datadir_count(dtest_setup.dtest_config.data_dir_count)
        # cluster.set_environment_variable('CASSANDRA_LIBJEMALLOC', dtest_setup.dtest_config.jemalloc_path)

        return cluster

    def set_cluster_log_levels(self):
        """
        The root logger gets configured in the fixture named fixture_logging_setup.
        Based on the logging configuration options the user invoked pytest with,
        that fixture sets the root logger to that configuration. We then ensure all
        Cluster objects we work with "inherit" these logging settings (which we can
        lookup off the root logger)
        """
        if logging.root.level != 'NOTSET':
            log_level = logging.getLevelName(logging.INFO)
        else:
            log_level = logging.root.level
        self.cluster.set_log_level(log_level)

    def initialize_cluster(self, create_cluster_func, **kwargs):
        """
        This method is responsible for initializing and configuring a ccm
        cluster for the next set of tests.  This can be called for two
        different reasons:
         * A class of tests is starting
         * A test method failed/errored, so the cluster has been wiped

        Subclasses that require custom initialization should generally
        do so by overriding post_initialize_cluster().
        """
        # connections = []
        # cluster_options = []
        self.iterations += 1
        self.create_cluster_func = create_cluster_func
        self.cluster = self.create_cluster_func(self, **kwargs)
        self.init_default_config()
        self.maybe_setup_jacoco()
        self.set_cluster_log_levels()

        id = cluster_id_allocator.alloc(self.test_path)
        self.cluster.set_id(id)
        self.cluster.set_ipprefix("127.0.%d." % id)

        # cls.init_config()
        # write_last_test_file(cls.test_path, cls.cluster)

        # cls.post_initialize_cluster()

    def reinitialize_cluster_for_different_version(self):
        """
        This method is used by upgrade tests to re-init the cluster to work with a specific
        version that may not be compatible with the existing configuration options
        """
        self.init_default_config()

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

    def disable_error(self, name, node):
        """Disable error injection
        Args:
            name (str): name of error injection to be disabled.
            node (ScyllaNode|int): either instance of scylla node or node number.
        """
        with DisableLogger("urllib3.connectionpool"):
            if isinstance(node, int):
                node = self.cluster.nodelist()[node]
            node_ip = get_ip_from_node(node)
            logger.trace(f'Disabling error injection "{name}" on node {node_ip}')

            response = requests.delete(f"http://{node_ip}:10000/v2/error_injection/injection/{name}")
            response.raise_for_status()

    def check_error(self, name, node):
        """Get status of error injection

        Args:
            name (str): name of error injection.
            node (ScyllaNode|int): either instance of scylla node or node number.

        """
        with DisableLogger("urllib3.connectionpool"):
            if isinstance(node, int):
                node = self.cluster.nodelist()[node]
            node_ip = get_ip_from_node(node)
            response = requests.get(f"http://{node_ip}:10000/v2/error_injection/injection/{name}")
            response.raise_for_status()

    def list_errors(self, node):
        """List enabled error injections

        Args:
            node (ScyllaNode|int): either instance of scylla node or node number.

        """
        with DisableLogger("urllib3.connectionpool"):
            if isinstance(node, int):
                node = self.cluster.nodelist()[node]
            node_ip = get_ip_from_node(node)
            response = requests.get(f"http://{node_ip}:10000/v2/error_injection/injection")
            response.raise_for_status()
            return response.json()

    def disable_errors(self, node):
        """Disable all error injections

        Args:
            node (ScyllaNode|int): either instance of scylla node or node number.

        """
        with DisableLogger("urllib3.connectionpool"):
            if isinstance(node, int):
                node = self.cluster.nodelist()[node]
            node_ip = get_ip_from_node(node)
            logger.trace(f'Disable all error injections on node {node_ip}')
            response = requests.delete(f"http://{node_ip}:10000/v2/error_injection/injection")
            response.raise_for_status()

    def enable_error(self, name, node, one_shot=False):
        """Enable error injection

        Args:
            name (str): name of error injection to be enabled.
            node (ScyllaNode|int): either instance of scylla node or node number.
            one_shot (bool): indicates whether the injection is one-shot
                             (resets enabled state after triggering the injection).

        """
        with DisableLogger("urllib3.connectionpool"):
            if isinstance(node, int):
                node = self.cluster.nodelist()[node]
            node_ip = get_ip_from_node(node)
            logger.trace(f'Enabling error injection "{name}" on node {node_ip}')
            response = requests.post(f"http://{node_ip}:10000/v2/error_injection/injection/{name}",
                                     params={"one_shot": one_shot})
            response.raise_for_status()
