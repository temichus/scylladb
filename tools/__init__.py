# our old tools.py file, would need to remove or use the new ones

import fileinput
import functools
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
import random
import string
from itertools import groupby
from pkg_resources import parse_version
from threading import Thread
from uuid import uuid1, uuid4
import errno
import logging
import glob
import distutils.dir_util

from cassandra import ConsistencyLevel
from cassandra.concurrent import execute_concurrent_with_args
from cassandra.query import SimpleStatement

from ccmlib.scylla_node import ScyllaNode

logger = logging.getLogger(__name__)


def rows_to_list(rows):
    new_list = [list(row) for row in rows]
    return new_list


def chunks_list(lst, num_chunks):
    for i in range(0, len(lst), num_chunks):
        yield lst[i:i + num_chunks]


def create_c1c2_table(tester, session, cf="cf", read_repair=None, debug_query=True, compaction=None, caching=True):
    tester.create_cf(session, cf, columns={'c1': 'text', 'c2': 'text'}, read_repair=read_repair,
                     debug_query=debug_query, compaction=compaction, caching=caching)


def insert_c1c2(session, keys=None, n=None, consistency=ConsistencyLevel.QUORUM, cf="cf"):
    if (keys is None and n is None) or (keys is not None and n is not None):
        raise ValueError("Expected exactly one of 'keys' or 'n' arguments to not be None; "
                         "got keys={keys}, n={n}".format(keys=keys, n=n))
    if n:
        keys = list(range(n))

    statement = session.prepare("INSERT INTO {} (key, c1, c2) VALUES (?, 'value1', 'value2')".format(cf))
    statement.consistency_level = consistency

    execute_concurrent_with_args(session, statement, [['k{}'.format(k)] for k in keys])


def delete_c1c2(session, keys=None, n=None, consistency=ConsistencyLevel.QUORUM, cf="cf"):
    if (keys is None and n is None) or (keys is not None and n is not None):
        raise ValueError("Expected exactly one of 'keys' or 'n' arguments to not be None; "
                         "got keys={keys}, n={n}".format(keys=keys, n=n))
    if n:
        keys = list(range(n))

    statement = session.prepare("DELETE FROM {} WHERE key=?".format(cf))
    statement.consistency_level = consistency

    execute_concurrent_with_args(session, statement, [['k{}'.format(k)] for k in keys])


def query_c1c2(session, key, consistency=ConsistencyLevel.QUORUM, tolerate_missing=False, must_be_missing=False, cf="cf"):
    query = SimpleStatement('SELECT c1, c2 FROM {} WHERE key=\'k{:d}\''.format(cf, key), consistency_level=consistency)
    rows = list(session.execute(query))
    if not tolerate_missing and not must_be_missing:
        assert len(rows) == 1
        res = rows[0]
        assert len(res) == 2 and res[0] == 'value1' and res[1] == 'value2', res
    if must_be_missing:
        assert len(rows) == 0


# work for cluster started by populate
def new_node(cluster, bootstrap=True, token=None, remote_debug_port='0', data_center=None, new_node_index=None):
    i = len(cluster.nodes) + 1 if not new_node_index else new_node_index
    # Changed from from creating ccmlib.Node to using the cluster create_node method to support creation of node based on cluster type
    node = cluster.create_node('node%s' % i,
                               bootstrap,
                               (cluster.get_node_ip(i), 9160),
                               (cluster.get_node_ip(i), 7000),
                               str(cluster.get_node_jmx_port(i)),
                               remote_debug_port,
                               token,
                               binary_interface=(cluster.get_node_ip(i), 9042))
    cluster.add(node, not bootstrap, data_center=data_center)
    return node


def insert_columns(tester, session, key, columns_count, consistency=ConsistencyLevel.QUORUM, offset=0):
    upds = ["UPDATE cf SET v=\'value%d\' WHERE key=\'k%s\' AND c=\'c%06d\'" %
            (i, key, i) for i in range(offset * columns_count, columns_count * (offset + 1))]
    query = 'BEGIN BATCH %s; APPLY BATCH' % '; '.join(upds)
    simple_query = SimpleStatement(query, consistency_level=consistency)
    session.execute(simple_query)


def query_columns(tester, session, key, columns_count, consistency=ConsistencyLevel.QUORUM, offset=0):
    query = SimpleStatement('SELECT c, v FROM cf WHERE key=\'k%s\' AND c >= \'c%06d\' AND c <= \'c%06d\'' % (
        key, offset, columns_count + offset - 1), consistency_level=consistency)
    res = list(session.execute(query))
    assert len(res) == columns_count, "%s != %s (%s-%s)" % (len(res), columns_count, offset, columns_count + offset - 1)
    for i in range(0, columns_count):
        assert res[i][1] == 'value%d' % (i + offset)


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


# Simple puts and get (on one row), testing both reads by names and by slice,
# with overwrites and flushes between inserts to make sure we hit multiple
# sstables on reads
def putget(cluster, session, cl=ConsistencyLevel.QUORUM):

    _put_with_overwrite(cluster, session, 1, cl)

    # reads by name
    ks = ["\'c%02d\'" % i for i in range(0, 100)]
    # We do not support proper IN queries yet
    # if cluster.version() >= "1.2":
    #    session.execute('SELECT * FROM cf USING CONSISTENCY %s WHERE key=\'k0\' AND c IN (%s)' % (cl, ','.join(ks)))
    # else:
    #    session.execute('SELECT %s FROM cf USING CONSISTENCY %s WHERE key=\'k0\'' % (','.join(ks), cl))
    # _validate_row(cluster, session)
    # slice reads
    query = SimpleStatement('SELECT * FROM cf WHERE key=\'k0\'', consistency_level=cl)
    rows = list(session.execute(query))
    _validate_row(cluster, rows)


def _put_with_overwrite(cluster, session, nb_keys, cl=ConsistencyLevel.QUORUM):
    for k in range(0, nb_keys):
        kvs = ["UPDATE cf SET v=\'value%d\' WHERE key=\'k%s\' AND c=\'c%02d\'" % (i, k, i) for i in range(0, 100)]
        query = SimpleStatement('BEGIN BATCH %s APPLY BATCH' % '; '.join(kvs), consistency_level=cl)
        session.execute(query)
        time.sleep(.01)
    cluster.flush()
    for k in range(0, nb_keys):
        kvs = ["UPDATE cf SET v=\'value%d\' WHERE key=\'k%s\' AND c=\'c%02d\'" %
               (i * 4, k, i * 2) for i in range(0, 50)]
        query = SimpleStatement('BEGIN BATCH %s APPLY BATCH' % '; '.join(kvs), consistency_level=cl)
        session.execute(query)
        time.sleep(.01)
    cluster.flush()
    for k in range(0, nb_keys):
        kvs = ["UPDATE cf SET v=\'value%d\' WHERE key=\'k%s\' AND c=\'c%02d\'" %
               (i * 20, k, i * 5) for i in range(0, 20)]
        query = SimpleStatement('BEGIN BATCH %s APPLY BATCH' % '; '.join(kvs), consistency_level=cl)
        session.execute(query)
        time.sleep(.01)
    cluster.flush()


def _validate_row(cluster, res):
    assert len(res) == 100, len(res)
    for i in range(0, 100):
        if i % 5 == 0:
            assert res[i][2] == 'value%d' % (i * 4), 'for %d, expecting value%d, got %s' % (i, i * 4, res[i][2])
        elif i % 2 == 0:
            assert res[i][2] == 'value%d' % (i * 2), 'for %d, expecting value%d, got %s' % (i, i * 2, res[i][2])
        else:
            assert res[i][2] == 'value%d' % i, 'for %d, expecting value%d, got %s' % (i, i, res[i][2])


# Simple puts and range gets, with overwrites and flushes between inserts to
# make sure we hit multiple sstables on reads
def range_putget(cluster, session, cl=ConsistencyLevel.QUORUM):
    keys = 100

    _put_with_overwrite(cluster, session, keys, cl)

    paged_results = session.execute('SELECT * FROM cf LIMIT 10000000')
    rows = [result for result in paged_results]

    assert len(rows) == keys * 100, len(rows)
    for k in range(0, keys):
        res = rows[:100]
        del rows[:100]
        _validate_row(cluster, res)


def replace_in_file(filepath, search_replacements):
    """In-place file search and replace.

    filepath - The path of the file to edit
    search_replacements - a list of tuples (regex, replacement) that
    represent however many search and replace operations you wish to
    perform.

    Note: This does not work with multi-line regexes.
    """
    for line in fileinput.input(filepath, inplace=True):
        for regex, replacement in search_replacements:
            line = re.sub(regex, replacement, line)
        sys.stdout.write(line)


def generate_ssl_stores(base_dir, passphrase='cassandra'):
    """
    Util for generating ssl stores using java keytool -- nondestructive method if stores already exist this method is
    a no-op.

    @param base_dir (str) directory where keystore.jks, truststore.jks and ccm_node.cer will be placed
    @param passphrase (Optional[str]) currently ccm expects a passphrase of 'cassandra' so it's the default but it can be
            overridden for failure testing
    @return None
    @throws CalledProcessError If the keytool fails during any step
    """

    if os.path.exists(os.path.join(base_dir, 'keystore.jks')):
        logger.debug("keystores already exists - skipping generation of ssl keystores")
        return

    logger.debug("generating keystore.jks in [{0}]".format(base_dir))
    subprocess.check_call(['keytool', '-genkeypair', '-alias', 'ccm_node', '-keyalg', 'RSA', '-validity', '365',
                           '-keystore', os.path.join(base_dir, 'keystore.jks'), '-storepass', passphrase,
                           '-dname', 'cn=Cassandra Node,ou=CCMnode,o=DataStax,c=US', '-keypass', passphrase])
    logger.debug("exporting cert from keystore.jks in [{0}]".format(base_dir))
    subprocess.check_call(['keytool', '-export', '-rfc', '-alias', 'ccm_node',
                           '-keystore', os.path.join(base_dir, 'keystore.jks'),
                           '-file', os.path.join(base_dir, 'ccm_node.cer'), '-storepass', passphrase])
    logger.debug("importing cert into truststore.jks in [{0}]".format(base_dir))
    subprocess.check_call(['keytool', '-import', '-file', os.path.join(base_dir, 'ccm_node.cer'),
                           '-alias', 'ccm_node', '-keystore', os.path.join(base_dir, 'truststore.jks'),
                           '-storepass', passphrase, '-noprompt'])
    # Added for scylla: Generate pem format cert/key
    logger.debug("exporting cert to pks12 from keystore.jks in [{0}]".format(base_dir))
    subprocess.check_call(['keytool', '-importkeystore', '-srckeystore', os.path.join(base_dir, 'keystore.jks'),
                           '-srcstorepass', passphrase, '-srckeypass', passphrase, '-destkeystore',
                           os.path.join(base_dir, 'ccm_node.p12'), '-deststoretype', 'PKCS12',
                           '-srcalias', 'ccm_node', '-deststorepass', passphrase, '-destkeypass', passphrase])
    logger.debug("Using openssl to split pks12 in [{0}] to pem format".format(base_dir))
    subprocess.check_call(['openssl', 'pkcs12', '-in', os.path.join(base_dir, 'ccm_node.p12'),
                           '-passin', 'pass:{0}'.format(passphrase), '-nokeys',
                           '-out', os.path.join(base_dir, 'ccm_node.pem')])
    # Key with password. We want without...
    subprocess.check_call(['openssl', 'pkcs12', '-in', os.path.join(base_dir, 'ccm_node.p12'),
                           '-passin', 'pass:{0}'.format(passphrase),
                           '-passout', 'pass:{0}'.format(passphrase), '-nocerts',
                           '-out', os.path.join(base_dir, 'ccm_node.tmp')])
    subprocess.check_call(['openssl', 'rsa', '-in', os.path.join(base_dir, 'ccm_node.tmp'),
                           '-passin', 'pass:{0}'.format(passphrase),
                           '-out', os.path.join(base_dir, 'ccm_node.key')])
    # And create the trust chain
    logger.debug("exporting cert to pks12 from truststore.jks in [{0}]".format(base_dir))
    subprocess.check_call(['keytool', '-importkeystore', '-srckeystore', os.path.join(base_dir, 'truststore.jks'),
                           '-srcstorepass', passphrase, '-destkeystore', os.path.join(base_dir, 'trust.p12'),
                           '-deststoretype', 'PKCS12', '-srcalias', 'ccm_node', '-deststorepass', passphrase])
    subprocess.check_call(['openssl', 'pkcs12', '-in', os.path.join(base_dir, 'trust.p12'),
                           '-passin', 'pass:{0}'.format(passphrase),
                           '-out', os.path.join(base_dir, 'trust.pem')])
    logger.debug("removing temporary certificates in [{0}]".format(base_dir))
    for filename in ('ccm_node.p12', 'ccm_node.tmp', 'trust.p12'):
        try:
            os.remove(os.path.join(base_dir, filename))
        except OSError as e:
            if e.errno != errno.ENOENT:  # ENOENT = no such file or directory
                raise


def create_stress_compatible_table(self, node, rf=1, dclocal_read_repair_chance=0.1, gc_grace_seconds=864000,
                                   read_repair_chance=0.0, default_time_to_live=0, speculative_retry="'99.0PERCENTILE'",
                                   compaction="'class': 'SizeTieredCompactionStrategy', 'sstable_size_in_mb': '100'"):
    session = self.patient_cql_connection(node)
    session.execute(f"""CREATE KEYSPACE keyspace1 WITH replication = {{
    'class': 'SimpleStrategy',
    'replication_factor': {rf} }};""")

    session.execute(f"""CREATE TABLE keyspace1.standard1(
    key blob PRIMARY KEY,
    "C0" blob,
    "C1" blob,
    "C2" blob,
    "C3" blob,
    "C4" blob,
    ) WITH bloom_filter_fp_chance = 0.01
    AND caching = {{'keys': 'ALL', 'rows_per_partition': 'ALL'}}
    AND comment = ''
    AND compaction = {{{compaction}}}
    AND compression = {{}}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = {dclocal_read_repair_chance}
    AND default_time_to_live = {default_time_to_live}
    AND gc_grace_seconds = {gc_grace_seconds}
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = {read_repair_chance}
    AND speculative_retry = {speculative_retry};""")


class since(object):

    def __init__(self, cass_version, max_version=None):
        self.cass_version = cass_version
        self.max_version = None
        if max_version is not None:
            v = max_version
            if v.endswith('.x') or v.endswith('.X'):
                v = v[0:-2]
            self.max_version = v

    def _skip_msg(self, version):
        if parse_version(version) < parse_version(self.cass_version):
            return "%s < %s" % (version, self.cass_version)
        if self.max_version and parse_version(version) > parse_version(self.max_version):
            return "%s > %s" % (version, self.max_version)

    def _maybe_skip(self, obj, version):
        msg = self._skip_msg(version)
        if msg:
            logger.debug("Marked for skipping: {}. Ignored.".format(msg))

    def _wrap_setUp(self, cls):
        orig_setUp = cls.setUp

        @functools.wraps(cls.setUp)
        def wrapped_setUp(obj, *args, **kwargs):
            orig_setUp(obj, *args, **kwargs)
            self._maybe_skip(obj, obj.cluster.version())

        cls.setUp = wrapped_setUp
        return cls

    def _wrap_function(self, f):
        @functools.wraps(f)
        def wrapped(obj):
            self._maybe_skip(obj, obj.cluster.version())
            f(obj)
        return wrapped

    def __call__(self, skippable):
        if isinstance(skippable, type):
            return self._wrap_setUp(skippable)
        return self._wrap_function(skippable)


def require(require_pattern):
    import pytest
    return pytest.mark.skip('requires ' + str(require_pattern))


def run_query_with_data_processing(session, query, consistency_level=ConsistencyLevel.ONE, session_timeout=None,
                                   group=False, groupby_column=None, restrict_column=None, restrict_value=None):
    if not session_timeout:
        session_timeout = 120
    result = list(session.execute(SimpleStatement(query, consistency_level=consistency_level), timeout=session_timeout))
    if result:
        if restrict_column:
            restrict_column_index = [i for i, clmn in enumerate(result[0]._fields) if clmn == restrict_column][0]
            restrict_value = [restrict_value] if not isinstance(restrict_value, list) else restrict_value

        if group:
            groupby_column_index = [i for i, clmn in enumerate(result[0]._fields) if clmn == groupby_column][0]
            result = [item[groupby_column_index] for item in result if item[restrict_column_index] in restrict_value] \
                if restrict_value and restrict_column \
                else [item[groupby_column_index] for item in result]
            result = [[key, len(list(group))] for key, group in groupby(sorted(result))]
        elif restrict_value and restrict_column:
            result = [item for item in result if item[restrict_column_index] in restrict_value]
    return result


def safe_mkdtemp():
    tmpdir = tempfile.mkdtemp()
    # \ on Windows is interpreted as an escape character and doesn't do anyone any favors
    return tmpdir.replace('\\', '/')


class InterruptBootstrap(Thread):

    def __init__(self, node):
        Thread.__init__(self)
        self.node = node

    def run(self):
        self.node.watch_log_for("Prepare completed")
        self.node.stop(gently=False)


class InterruptCompaction(Thread):
    """
    Interrupt compaction by killing a node as soon as
    the "Compacting" string is found in the log file
    for the table specified. This requires debug level
    logging in 2.1+ and expects debug information to be
    available in a file called "debug.log" unless a
    different name is passed in as a paramter.
    """

    def __init__(self, node, tablename, filename='debug.log'):
        Thread.__init__(self)
        self.node = node
        self.tablename = tablename
        self.filename = filename
        self.mark = node.mark_log(filename=self.filename)

    def run(self):
        self.node.watch_log_for("Compacting(.*)%s" % (self.tablename,), from_mark=self.mark, filename=self.filename)
        self.node.stop(gently=False)


class KillOnBootstrap(Thread):

    def __init__(self, node):
        Thread.__init__(self)
        self.node = node

    def run(self):
        self.node.watch_log_for("JOINING: Starting to bootstrap")
        self.node.stop(gently=False)


class ColumnType:
    def __init__(self, type, limits=None):
        self.type = type
        self.limits = limits
        self.value = self.generate_value(self.type)

    def get_value(self):
        return self.value

    def gen_random_string(self, length=1, source=string.printable):
        return ''.join(random.choice(source) for _ in range(length))

    def gen_random_number(self, length):
        return int(self.gen_random_string(length=length, source=string.digits), 10) if length > 0 else 0

    def gen_random_decimal_number(self, length):
        int_idx = random.randint(0, length - 1)
        dec_idx = length - int_idx
        int_num = self.gen_random_number(int_idx)
        dec_num = self.gen_random_number(dec_idx)

        return float('.'.join([str(int_num), str(dec_num)]) if dec_idx > 0 else int_num)

    def generate_value(self, data_type):
        '''
            types are:
            [ascii, bigint, blob, boolean, date, decimal, double, float, inet, int, list, map, smallint, set, text, time,
            timestamp, timeuuid, tinyint, tuple, UDT, uuid, varchar, varint]
            :return: a random value by its type definition
        '''
        if data_type.lower() == 'uuid':
            value = uuid4()
        elif data_type.lower() in ['ascii', 'text', 'varchar']:
            value = '\'{}\''.format(self.gen_random_string(length=10).replace('\'', ' '))
        elif data_type.lower() == 'bigint':
            value = random.randint(-9223372036854775808, 9223372036854775807)
        elif data_type.lower() == 'blob':
            value = hex(random.randint(0, 4294967295))
            if len(value) % 2 != 0:
                value = value[:-1]
        elif data_type.lower() == 'boolean':
            value = random.choice([True, False])
        elif data_type.lower() == 'date':
            value = '\'{}-{}-{}\''.format(random.randint(0, 2999), random.randint(1, 12), random.randint(1, 31))
        elif data_type.lower() == 'time':
            value = '\'{}:{}:{}\''.format(random.randint(0, 23), random.randint(0, 59), random.randint(0, 59))
        elif data_type.lower() == 'timestamp':
            value = int(time.time())
        elif data_type.lower() == 'timeuuid':
            value = uuid1(int(time.time()))
        elif data_type.lower() in ['decimal', 'double', 'float']:
            value = self.gen_random_decimal_number(length=random.randint(2, 15))
        elif data_type.lower() == 'inet':
            value = '\'{}.{}.{}.{}\''.format(random.randint(1, 255), random.randint(1, 255), random.randint(1, 255),
                                             random.randint(1, 255))
        elif data_type.lower() in ['int', 'varint']:
            value = random.randint(-2147483648, 2147483647)
        elif data_type.lower() == 'smallint':
            value = random.randint(-32768, 32767)
        elif data_type.lower() == 'tinyint':
            value = random.randint(-128, 127)
        elif data_type.lower() == 'list':
            value = [self.generate_value('int') for _ in range(3)]
        elif data_type.lower() == 'map':
            value = ''.join(['{\'', self.gen_random_string(5, source=string.ascii_letters), '\': ',
                             str(self.generate_value('int')) + '}'])
        elif data_type.lower() == 'udt':
            value = '{a: \'' + self.gen_random_string(5, source=string.ascii_letters) + '\', b: \'' + \
                    self.gen_random_string(5, source=string.ascii_letters) + '\'}'
        elif data_type.lower() == 'set':
            value = '{ ' + str(self.generate_value('int')) + ', ' + str(self.generate_value('int')) + ' }'
        elif data_type.lower() == 'tuple':
            value = '({})'.format(self.generate_value('int'))
        else:
            value = None
        return value


def make_snapshot(node: ScyllaNode, ks: str = None, cf: str = None, cf_param_name: str = '-cf', name: str = None) -> str:
    """Create snapshot for all keyspaces or for specified ks, ks.cf, with name

    Create snapshot for:
    - if ks is none, for all keyspaces
    - if ks is provided, create snapshot for all tables in keyspace
    - if ks and cf provided, create snapshot for ks.cf table only
    - if name is set, create snapshot with tag name, datetime otherwise

    and then copy created snapshots to temp directory

    :param node: Scylla Node instance where create snapshot
    :type node: ScyllaNode
    :param ks: keyspace name, defaults to None
    :type ks: str, optional
    :param cf: column factory name, defaults to None
    :type cf: str, optional
    :param name: tag name of snapshot, defaults to None
    :type name: str, optional
    :returns: path where all snapshots stored, temp directory
    :rtype: {str}
    """
    logger.debug("Making snapshot....")
    node.flush()
    snapshot_cmd = 'snapshot '
    if ks:
        snapshot_cmd += f"{ks} "
        if cf:
            snapshot_cmd += f"{cf_param_name} {cf} "
        if name:
            snapshot_cmd += f"-t {name}"

    logger.debug("Running snapshot cmd: {snapshot_cmd}".format(snapshot_cmd=snapshot_cmd))
    node.nodetool(snapshot_cmd)
    tmpdir = safe_mkdtemp()
    node_dir = node.get_path()

    # # Find the snapshot dir, it's different in various C* versions:
    snapshot_dirs = []
    tables = [f"{t}-*/" for t in cf.split(',')] if cf else ['*/']
    for table in tables:
        snapshot_dir_pattern = f"{node_dir}/data/"
        if ks:
            snapshot_dir_pattern += f"{ks}/"
            snapshot_dir_pattern += f"{table}"
            if name:
                snapshot_dir_pattern += f"snapshots/{name}"
            else:
                snapshot_dir_pattern += f"snapshots/*"
        else:
            snapshot_dir_pattern += f"/*/*/snapshots/*"
        snapshot_dir = glob.glob(snapshot_dir_pattern)
        if snapshot_dir:
            snapshot_dirs.extend(snapshot_dir)
        else:
            snapshot_dirs.append('')

    logger.debug(f"snapshot_dir is : {snapshot_dirs}")
    logger.debug(f"snapshot copy is : {tmpdir}")

    # # Copy files from the snapshot dir to existing temp dir
    for snapshot_dir in snapshot_dirs:
        save_dir = snapshot_dir.replace('/snapshots', '').replace(os.path.join(node_dir, "data/"), '')
        os.makedirs(os.path.join(tmpdir, save_dir), exist_ok=False)
        distutils.dir_util.copy_tree(str(snapshot_dir), os.path.join(tmpdir, save_dir))

    return tmpdir


def get_cf_snapshot_saved_dir(base_snapshot_dir: str, keyspace: str, table: str, name: str = None) -> str:
    """Get path to specified snapshot of ks.cf by name or first one

    return path to directory with sstables from snapshot store in
    base_snapshot_dir. base_snapshot_dir is a path to temp folder returned by
    make_snapshot method or any folder where all snapshots located
        - <base_snapshot_dir>/ks/cf-*/[name|any]/

    :param base_snapshot_dir: path to folder with snapshots
    :type base_snapshot_dir: str
    :param keyspace: keyspace name
    :type keyspace: str
    :param table: column family name
    :type table: str
    :param name: name of snapshot, defaults to None
    :type name: str, optional
    :returns: path to first matched snapshot dir for ks.cf by [name| of first one]
    :rtype: {str}
    """
    path_pattern = f"{base_snapshot_dir}/{keyspace}/{table}-*"
    if name:
        path_pattern += f"/{name}"
    else:
        path_pattern += f"/*/"
    return glob.glob(path_pattern)[0]


def restore_snapshot_with_refresh(snapshot_dir, node, keyspace, table, name=None):
    logger.debug("Restoring snapshot....")
    node_dir = node.get_path()
    restore_dir = glob.glob("{node_dir}/data/{keyspace}/{table}-*/upload/".format(**locals()))[0]
    snapshot_dir = get_cf_snapshot_saved_dir(base_snapshot_dir=snapshot_dir, keyspace=keyspace, table=table, name=name)
    logger.debug("Copying from %s to %s" % (str(snapshot_dir), str(restore_dir)))
    distutils.dir_util.copy_tree(snapshot_dir, restore_dir)
    node.nodetool("refresh %s %s" % (keyspace, table))


def restore_snapshot_with_sstableloader(snapshot_dir, node, keyspace, table, name=None):
    logger.debug("Restoring snapshot....")
    snapshot_dir = get_cf_snapshot_saved_dir(snapshot_dir, keyspace, table, name)
    ip = node.address()
    # copy sstables to ks.cf folder to properly load with sstableloader
    tmpdir = safe_mkdtemp()
    os.makedirs(os.path.join(tmpdir, keyspace, table), exist_ok=True)
    distutils.dir_util.copy_tree(snapshot_dir, os.path.join(tmpdir, keyspace, table))

    args = [node.get_tool('sstableloader'), '-d', ip, os.path.join(tmpdir, keyspace, table)]
    p = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = p.communicate()
    exit_status = p.wait()

    if exit_status != 0 or 'exception' in str(stderr):
        raise Exception("sstableloader command '%s' failed; exit status: %d'; stdout: %s; stderr: %s" %
                        (" ".join(args), exit_status, stdout, stderr))
    shutil.rmtree(tmpdir, ignore_errors=True)
