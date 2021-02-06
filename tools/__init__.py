# our old tools.py file, would need to remove or use the new ones

import fileinput
import functools
import os
import re
import subprocess
import sys
import time
import random
import string
from itertools import groupby
from pkg_resources import parse_version
from threading import Thread
from uuid import uuid1, uuid4
import errno
import logging

from cassandra import ConsistencyLevel
from cassandra.concurrent import execute_concurrent_with_args
from cassandra.query import SimpleStatement

from dtest_class import create_cf

logger = logging.getLogger(__name__)


def rows_to_list(rows):
    new_list = [list(row) for row in rows]
    return new_list


def chunks_list(lst, num_chunks):
    for i in range(0, len(lst), num_chunks):
        yield lst[i:i + num_chunks]


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
