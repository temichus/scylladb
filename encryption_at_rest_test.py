from hashlib import md5
import time
import subprocess
import os
import shutil
import re

from enum import Enum
from cassandra import ReadTimeout, ReadFailure, ConsistencyLevel

from dtest import Tester, debug, warning
from tools import rows_to_list, get_table_description, require
from scylla_tools import flush_by_node, insert_c1c2, query_c1c2
from assertions import assert_one


class InMemoryTest(Tester):
    """
    Test in memory sstable when Encryption at-rest is enabled.
    a reproducer for scylla-enterprise/issues/925
    """

    def restart_query_test(self):
        self.cluster.set_configuration_options(values={'in_memory_storage_size_mb': 100})
        self.cluster.populate(1).start(wait_for_binary_proto=True, wait_other_notice=True)
        node1 = self.cluster.nodelist()[0]
        session = self.patient_cql_connection(node1)
        session.execute(
            "CREATE KEYSPACE rest WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}")
        session.execute("""CREATE TABLE rest.table1(key text PRIMARY KEY, name text) WITH scylla_encryption_options =
            {'key_provider': 'LocalFileSystemKeyProviderFactory', 'secret_key_file': '/tmp/secret_key'} AND
            in_memory=true AND compaction={'class': 'InMemoryCompactionStrategy'}""")

        session.execute("insert into rest.table1 (key, name) values ('key1', 'name1')")
        node1.flush()
        result = session.execute("select * from rest.table1")
        debug(list(result))

        for node in self.cluster.nodelist():
            node.stop()
            node.start(wait_for_binary_proto=True, wait_other_notice=True)
        session = self.patient_cql_connection(node1)
        result = session.execute("select * from rest.table1")
        debug(list(result))

    def workload_without_restart_test(self):
        self.cluster.set_configuration_options(values={'in_memory_storage_size_mb': 100})
        self.cluster.populate(1).start(wait_for_binary_proto=True, wait_other_notice=True)
        node1 = self.cluster.nodelist()[0]
        session = self.patient_cql_connection(node1)
        session.execute(
            "CREATE KEYSPACE keyspace1 WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}")
        session.execute("""CREATE TABLE keyspace1.standard1 (key blob PRIMARY KEY,"C0" blob,"C1" blob,"C2" blob,"C3" blob,"C4" blob)
            WITH scylla_encryption_options = {'key_provider': 'LocalFileSystemKeyProviderFactory', 'secret_key_file': '/tmp/secret_key'} AND
            in_memory=true AND compaction={'class': 'InMemoryCompactionStrategy'} """)
        node1.stress(['write', 'n=100000', 'cl=QUORUM', '-rate', 'threads=8'])
        debug('flushing ...')
        self.cluster.flush()
        node1.stress(['read', 'n=100000', 'cl=QUORUM', '-rate', 'threads=8'])


class KeyProviderEnum(Enum):
    default = ''
    local = 'LocalFileSystemKeyProviderFactory'
    replicated = 'ReplicatedKeyProviderFactory'
    kmip = 'KmipKeyProviderFactory'


# default: 'AES/CBC/PKCS5Padding', length 128
supported_cipher_algorithms = {'': [],
                               'AES/CBC/PKCS5Padding': [128, 192, 256],  # 192 has problem
                               'AES/ECB/PKCS5Padding': [128, 192, 256],
                               'DES/CBC/PKCS5Padding': [56],
                               # 'DESede/CBC/PKCS5Padding': [112, 168],    # not support by Scylla, supported by DSE
                               # 'Blowfish/CBC/PKCS5Padding': [32, 448],   # not support by Scylla, supported by DSE
                               'RC2/CBC/PKCS5Padding':  [80, 128]  # [40, 80, 128]  # 40 to 128
                               }


class BaseKeyProviderFactory(Tester):
    def __init__(self, key_provider, Tester):
        self.key_provider = key_provider
        self.system_keyfile = None
        self.kmip_host = None
        self.Tester = Tester
        self.cluster = Tester.cluster

    def prepare_conf(self):
        pass

    def break_key(self, filename):
        debug('Break key : %s' % filename)
        debug(subprocess.getoutput('head %s*' % filename))
        os.rename(filename, filename + '.break_backup')

    def restore_key(self, filename):
        debug('Restore key : %s' % filename)
        debug(subprocess.getoutput('head %s*' % filename))
        os.rename(filename + '.break_backup', filename)

    def prepare(self, node_num=2):
        self.cluster.populate(node_num).start(wait_for_binary_proto=True, wait_other_notice=True)

    def prepare_system_key(self, dirname='./resources/system_keys/', keyfile='system_key', cipher_algorithm='AES/CBC/PKCS5Padding', secret_key_strength=128, reuse_key=True):
        if not os.path.exists(dirname):
            os.mkdir(dirname)
        dirname = os.path.realpath(dirname)
        dest = os.path.join(dirname, keyfile)
        if reuse_key:
            # use saved key in dtest repo, generate it in future
            src = './resources/system_keys/system_key'  # AES/ECB/PKCS5Padding:128
            if not os.path.exists(dest) or not os.path.samefile(src, dest):
                shutil.copy(src, dest)
        else:
            src = '/etc/dse/conf/system_key_tmp'
            subprocess.getoutput('sudo rm -f %s' % src)
            subprocess.getoutput("sudo /home/amos/.ccm/repository/5.1.5/bin/dsetool createsystemkey '%s' %d system_key_tmp" %
                                 (cipher_algorithm, secret_key_strength))
            subprocess.getoutput('sudo cp %s %s' % (src, dest))
            subprocess.getoutput('sudo chown $USER:$USER %s' % dest)

        self.cluster.set_configuration_options({'system_key_directory': dirname})
        self.system_keyfile = dest
        debug('set system_key_directory to %s' % dirname)

    def create_encrypted_cf(self, session, name='cf', columns={'c1': 'text', 'c2': 'text'},
                            cipher_algorithm=None, secret_key_strength=None,
                            compression=None, system_key_file=None, secret_key_file=None,
                            kmip_host=None):
        options = {}
        if self.key_provider:
            options.update({'key_provider': self.key_provider.value})
        if cipher_algorithm:
            options.update({'cipher_algorithm': cipher_algorithm})
        if secret_key_strength:
            options.update({'secret_key_strength': secret_key_strength})
        if kmip_host or self.kmip_host:
            options.update({'kmip_host': kmip_host if kmip_host else self.kmip_host})
        if system_key_file:
            options.update({'system_key_file': system_key_file})
            self.prepare_system_key(dirname='./resources/system_keys', keyfile=system_key_file)
        if secret_key_file:
            options.update({'secret_key_file': secret_key_file})
        self.create_cf(session, name, columns=columns, scylla_encryption_options=options, compression=compression)
        return options

    def read_verify_workload(self, session, ks='ks', cf='cf'):
        debug('Verify data by read stress: %s.%s' % (ks, cf))
        for i in range(100):
            query_c1c2(session, i, ConsistencyLevel.QUORUM, ks=ks, cf=cf)

    def prepare_write_workload(self, session, ks='ks', cf='cf', flush=True):
        debug('Insert data to encrypted table: %s.%s' % (ks, cf))
        insert_c1c2(session, keys=list(range(100)), consistency=ConsistencyLevel.ALL, ks=ks, cf=cf)
        if flush:
            debug('flush cluster')
            self.cluster.flush()

    def verify_no_secret_key(self):
        debug('Verify that system key is not generated automatically')
        keyfile = os.path.join(self.Tester.test_path, 'test/node1/conf/data_encryption_keys')
        self.assertFalse(os.path.exists(keyfile), 'Default system_key is generated unexpectedly')

    def verify_secret_key(self, cipher_algorithm=None, secret_key_strength=None):
        debug('Verify that system key is generated automatically')
        keyfile = os.path.join(self.Tester.test_path, 'test/node1/conf/data_encryption_keys')
        self.assertTrue(os.path.exists(keyfile), 'Default system_key is not generated')

        if cipher_algorithm is None:
            cipher_algorithm = 'AES/CBC/PKCS5Padding'
        if secret_key_strength is None:
            secret_key_strength = 128
        found = False
        with open(keyfile) as f:
            for line in f.readlines():
                if line.startswith('%s:%d:' % (cipher_algorithm, secret_key_strength)):
                    debug('Found system key: %s' % line)
                    found = True
        self.assertTrue(found, 'Did not found specific system key in %s' % keyfile)

    def _grep_database_files(self, pattern, path, expect=None, skip=False, debug_detail=False):
        """
        skip: skip check in topdir and result assert for avoiding dead loop
        """
        grep_commitlog_cmd = "grep -r '%s' %s" % (pattern, os.path.join(self.Tester.test_path, 'test/node*/', path))
        output = subprocess.getoutput(grep_commitlog_cmd)
        debug('\tExpect: %s, Result: %s' % (expect, len(output) > 0))
        if debug_detail:
            debug('\tCMD: %s' % grep_commitlog_cmd)
            debug(output)
        if skip:
            return len(output) != 0
        if expect is not False and (len(output) == 0):
            # try to search pattern in top directory (contains both data & commitlogs) for trouble shooting.
            # such as, data isn't flush from commitlogs to disk,
            warning('%s does not exist in %s!!' % (pattern, path))
            self._grep_database_files(pattern, '', skip=True)
        if expect is not None:
            self.assertTrue(expect ^ (len(output) == 0), "Grep result isn't expected")
        return len(output) != 0

    def _generate_rand_unique_str(self, prefix=''):
        time.sleep(0.1)
        return prefix + md5.new(str(time.time())).hexdigest()


class DefaultKeyProviderFactory(BaseKeyProviderFactory):
    def __init__(self, Tester):
        BaseKeyProviderFactory.__init__(self, None, Tester)


class LocalFileSystemKeyProviderFactory(BaseKeyProviderFactory):
    def __init__(self, Tester):
        self.secret_file = os.path.join(Tester.test_path, 'test/node1/conf/data_encryption_keys')
        BaseKeyProviderFactory.__init__(self, KeyProviderEnum.local, Tester)


class ReplicatedKeyProviderFactory(BaseKeyProviderFactory):
    def __init__(self, Tester):
        self.system_keyfile = os.path.realpath('./resources/system_keys/system_key')
        self.system_table_keyfile = os.path.realpath('./resources/system_keys/system/system_table_systab')
        BaseKeyProviderFactory.__init__(self, KeyProviderEnum.replicated, Tester)

    def prepare_conf(self):
        # prepare_secret_key(self)
        pass


class KmipKeyProviderFactory(BaseKeyProviderFactory):
    def __init__(self, Tester):
        BaseKeyProviderFactory.__init__(self, KeyProviderEnum.kmip, Tester)

    def prepare_conf(self, use_scylla_kmip_server=True):
        # restart is request to make change effective
        if use_scylla_kmip_server:
            options = {'hosts': '52.21.171.245',
                       'certificate': os.path.realpath('./resources/scylla.pem'),
                       'keyfile': os.path.realpath('./resources/scylla.pem'),
                       'truststore': os.path.realpath('./resources/cacert.pem'),
                       'priority_string': 'SECURE128:+RSA:-VERS-TLS1.0:-ECDHE-ECDSA',
                       }
        else:
            options = {'hosts': 'kmip-interop1.cryptsoft.com',
                       'certificate': '/etc/scylla/conf/SCYLLADB.pem',
                       'keyfile': '/etc/scylla/conf/SCYLLADB.pem',
                       'truststore': '/etc/scylla/conf/CA.pem',
                       'priority_string': 'SECURE128:+RSA:-VERS-TLS1.0:-ECDHE-ECDSA',
                       }
        self.cluster.set_configuration_options({'kmip_hosts': {'kmip_test': options}})
        self.kmip_host = 'kmip_test'


class EncryptionAtRestBase(Tester):
    __test__ = False
    multiple_num = 3
    default_node_num = 2

    def get_session(self, node_idx=0, user=None, password=None):
        node = self.cluster.nodelist()[node_idx]
        conn = self.patient_cql_connection(node, user=user, password=password)
        return conn

    def prepare(self, n=default_node_num, kss=['ks'], restart=False):
        if not self.cluster.nodelist():
            self.cluster.populate(n).start(wait_for_binary_proto=True, wait_other_notice=True)
        elif restart:
            self.rolling_restart()
        session = self.get_session()
        for ks in kss:
            session.execute(
                "CREATE KEYSPACE IF NOT EXISTS %s WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : %d }" % (ks, n))
        return session

    def cleanup(self, kss=['ks']):
        session = self.get_session()
        for ks in kss:
            session.execute('DROP KEYSPACE IF EXISTS %s' % ks)

    def rolling_restart(self, user=None, password=None, allow_start_failure=False):
        debug(f'Restart nodes one by one ...{" (start failures allowed)" if allow_start_failure else ""}')
        errors = []
        for node in self.cluster.nodelist():
            node.stop(wait_other_notice=True)
            try:
                node.start(wait_other_notice=True, wait_for_binary_proto=True)
            except RuntimeError as e:
                if allow_start_failure:
                    errors.append(e)
                    pass
        if not errors:
            return self.get_session(user=user, password=password)

    def cluster_restart(self, user=None, password=None):
        debug('Restart cluster ...')
        self.cluster.stop(wait_other_notice=True)
        self.cluster.start(wait_for_binary_proto=True, wait_other_notice=True)
        return self.get_session(user=user, password=password)

    def get_key_provider(self, key_provider=None):
        if key_provider == KeyProviderEnum.local:
            ret = LocalFileSystemKeyProviderFactory(self)
        elif key_provider == KeyProviderEnum.replicated:
            ret = ReplicatedKeyProviderFactory(self)
        elif key_provider == KeyProviderEnum.kmip:
            ret = KmipKeyProviderFactory(self)
        elif key_provider == KeyProviderEnum.default or key_provider is None:
            ret = DefaultKeyProviderFactory(self)
        else:
            raise Exception('Unknown key_provider: %s' % key_provider)
        return ret

    def _smoke_test(self, key_provider=KeyProviderEnum.local, cipher_algorithm=None, secret_key_strength=None, compression=None):
        kp = self.get_key_provider(key_provider)
        kp.prepare_conf()
        session = self.prepare(restart=key_provider == KeyProviderEnum.kmip)
        if cipher_algorithm and secret_key_strength:
            kp.create_encrypted_cf(session, name='ks.cf', cipher_algorithm=cipher_algorithm,
                                   secret_key_strength=secret_key_strength, compression=compression)
        else:
            kp.create_encrypted_cf(session, name='ks.cf', compression=compression)
        kp.prepare_write_workload(session)
        if key_provider == KeyProviderEnum.local:
            kp.verify_secret_key(cipher_algorithm, secret_key_strength)
        session = self.rolling_restart()
        kp.read_verify_workload(session)

    def _upgrade_sstables(self):
        for node in self.cluster.nodelist():
            out, err = node.nodetool('upgradesstables')

    def _alter_test(self, key_provider=KeyProviderEnum.local):
        kp = self.get_key_provider(key_provider)
        kp.prepare_conf()
        session = self.prepare(restart=key_provider == KeyProviderEnum.kmip)
        node1 = self.cluster.nodelist()[0]
        options = kp.create_encrypted_cf(session, name='ks.cf')
        query = "ALTER TABLE ks.cf with scylla_encryption_options=%s"

        kp.prepare_write_workload(session)
        debug('disable encryption at-rest')
        session.execute(query % "{'key_provider': 'none'}")
        table_desc = get_table_description(node1, "ks", "cf")
        assert "key_provider" not in table_desc, f"key_provider isn't disabled, schema:\n {table_desc}"
        self._upgrade_sstables()
        session = self.rolling_restart()
        kp.read_verify_workload(session)

        debug('re-enable encryption at-rest: %s' % options)
        session.execute(query % options)
        table_desc = get_table_description(node1, "ks", "cf")
        if key_provider == KeyProviderEnum.default:
            assert "key_provider" not in table_desc, f"key_provider isn't disabled, schema:\n {table_desc}"
        else:
            err_msg = f"key_provider isn't changed to {key_provider.value}, schema: \n {table_desc}"
            assert f"'key_provider': '{key_provider.value}'" in table_desc, err_msg
        self._upgrade_sstables()
        session = self.rolling_restart()
        kp.read_verify_workload(session)

    def _key_break_test(self, key_provider=KeyProviderEnum.local, cipher_algorithm=None, secret_key_strength=None, compression=None):
        kp = self.get_key_provider(key_provider)
        kp.prepare_conf()
        session = self.prepare(1)
        if cipher_algorithm and secret_key_strength:
            kp.create_encrypted_cf(session, name='ks.cf', cipher_algorithm=cipher_algorithm,
                                   secret_key_strength=secret_key_strength, compression=compression)
        else:
            kp.create_encrypted_cf(session, name='ks.cf', compression=compression)
        kp.prepare_write_workload(session)
        if key_provider == KeyProviderEnum.local:
            kp.verify_secret_key(cipher_algorithm, secret_key_strength)
        session = self.rolling_restart()
        kp.read_verify_workload(session)
        if KeyProviderEnum.local:
            key_file = os.path.join(self.test_path, 'test/node1/conf/data_encryption_keys')
        else:
            key_file = './resources/system_keys/system_key'
        node1 = self.cluster.nodelist()[0]
        mark = node1.mark_log()
        kp.break_key(key_file)
        kp.read_verify_workload(session)
        kp.prepare_write_workload(session)
        try:
            scylla_ext_opt = os.environ['SCYLLA_EXT_OPTS']
            new_scylla_ext_opt = re.sub(r'--abort-on-seastar-bad-alloc', '', scylla_ext_opt)
            os.environ['SCYLLA_EXT_OPTS'] = new_scylla_ext_opt
        except:
            pass
        session = self.rolling_restart(allow_start_failure=True)
        if session:
            kp.prepare_write_workload(session)
            try:
                kp.read_verify_workload(session)
            except ReadFailure as e:
                debug(str(e))
        errors = [
            'SSTable reader found an exception when reading sstable',
            'Exception while populating keyspace',
            'malformed_sstable_exception'
        ]
        errors_pat = '|'.join(errors)
        node1.watch_log_for(errors_pat, from_mark=mark)
        self.check_errors(node1, errors, search_str='ERROR')
        kp.restore_key(key_file)

        if scylla_ext_opt:
            os.environ['SCYLLA_EXT_OPTS'] = scylla_ext_opt
        debug('Restart to trigger the read error')
        # https://github.com/scylladb/scylla-enterprise/issues/755
        # secret_key_file missing can only be identified by read workload after restart #755
        session = self.cluster_restart()

        kp.prepare_write_workload(session)
        try:
            kp.read_verify_workload(session)
        except ReadFailure as e:
            debug('Encryption key has been re-generated, expect to fail. %s' % str(e))

    def _multiple_ks_test(self, key_provider=KeyProviderEnum.local):
        kss = ['mks_%s' % i for i in range(self.multiple_num)]
        kp = self.get_key_provider(key_provider)
        kp.prepare_conf()
        session = self.prepare(kss=kss, restart=key_provider == KeyProviderEnum.kmip)
        secret_key_file = None
        system_key_file = None
        for ks in kss:
            if key_provider == KeyProviderEnum.local:
                secret_key_file = './resources/secret_key_file_' + ks
            elif key_provider == KeyProviderEnum.replicated:
                system_key_file = 'system_key_' + ks

            kp.create_encrypted_cf(session, name=ks + '.cf', system_key_file=system_key_file,
                                   secret_key_file=secret_key_file)
            kp.prepare_write_workload(session, ks=ks)
        session = self.rolling_restart()
        for ks in kss:
            kp.read_verify_workload(session, ks=ks)
        return kss

    def _multiple_cf_test(self, key_provider=KeyProviderEnum.local):
        cfs = ['cf_%d' % i for i in range(self.multiple_num)]
        kp = self.get_key_provider(key_provider)
        kp.prepare_conf()
        session = self.prepare(restart=key_provider == KeyProviderEnum.kmip)
        secret_key_file = None
        system_key_file = None
        for cf in cfs:
            if key_provider == KeyProviderEnum.local:
                secret_key_file = './resources/secret_key_file_' + cf
            elif key_provider == KeyProviderEnum.replicated:
                system_key_file = 'system_key_' + cf
            kp.create_encrypted_cf(session, name='ks.' + cf, system_key_file=system_key_file,
                                   secret_key_file=secret_key_file)
            kp.prepare_write_workload(session, cf=cf)
        session = self.rolling_restart()
        for cf in cfs:
            kp.read_verify_workload(session, cf=cf)

    def _reboot_test(self, key_provider=KeyProviderEnum.local):
        kp = self.get_key_provider(key_provider)
        kp.prepare_conf()
        self.prepare(n=3, restart=key_provider == KeyProviderEnum.kmip)

        session = self.get_session()
        kp.create_encrypted_cf(session, name='ks.cf')
        kp.prepare_write_workload(session, flush=False)

        for node in self.cluster.nodelist()[1:]:
            for i in range(3):
                debug('Kill node {}, and restart'.format(node.name))
                node.stop(gently=False)
                node.start(wait_for_binary_proto=True, wait_other_notice=False)
            kp.read_verify_workload(self.get_session())


class EncryptionAtRestTest(EncryptionAtRestBase):
    __test__ = True

    def encryption_table_compression_test(self):
        for i in [None, 'LZ4', 'Snappy', 'Deflate']:
            debug('---- Test with compression: %s -----' % i)
            EncryptionAtRestBase._smoke_test(self, key_provider=KeyProviderEnum.local, compression=i)
            EncryptionAtRestBase.cleanup(self)

    def supported_cipher_algorithms_test(self):
        for k, v in supported_cipher_algorithms.items():
            for i in v:
                debug('---- Test with %s , length %s ----' % (k, i))
                for name, value in KeyProviderEnum.__members__.items():
                    try:
                        EncryptionAtRestBase._smoke_test(self, key_provider=value,
                                                         cipher_algorithm=k, secret_key_strength=i)
                    except Exception as e:
                        debug(str(e))
                    finally:
                        EncryptionAtRestBase.cleanup(self)

    def abbreviated_supported_cipher_algorithms_test(self):
        tested = set()
        for k, v in supported_cipher_algorithms.items():
            if not v:
                continue
            k = k.split('/')[0]
            if k in tested or not k:
                continue
            tested.add(k)
            i = v[0]
            debug('---- Test with %s , length %s ----' % (k, i))
            for name, value in KeyProviderEnum.__members__.items():
                try:
                    EncryptionAtRestBase._smoke_test(self, key_provider=value,
                                                        cipher_algorithm=k, secret_key_strength=i)
                except Exception as e:
                    debug(str(e))
                finally:
                    EncryptionAtRestBase.cleanup(self)

    def multiple_ks_test(self):
        for name, value in KeyProviderEnum.__members__.items():
            kss = EncryptionAtRestBase._multiple_ks_test(self, key_provider=value)
            EncryptionAtRestBase.cleanup(self, kss=kss)

    def multiple_cf_test(self):
        for name, value in KeyProviderEnum.__members__.items():
            EncryptionAtRestBase._multiple_cf_test(self, key_provider=value)
            EncryptionAtRestBase.cleanup(self)

    def reboot_test(self):
        for name, value in KeyProviderEnum.__members__.items():
            EncryptionAtRestBase._reboot_test(self, key_provider=value)
            EncryptionAtRestBase.cleanup(self)

    @require('scylladb/scylla-enterprise#1787')
    def alter_test(self):
        for name, value in KeyProviderEnum.__members__.items():
            EncryptionAtRestBase._alter_test(self, key_provider=value)
            EncryptionAtRestBase.cleanup(self)

    def key_break_test(self):
        EncryptionAtRestBase._key_break_test(self, key_provider=KeyProviderEnum.local)


class SystemInfoEncryptionTest(EncryptionAtRestBase):

    def verify_system_info(self, session, key_provider, ks_suffix='', expect=True):
        table_num = 10
        user_num = 5
        rand_user_prefix = key_provider._generate_rand_unique_str('user_')
        rand_password = key_provider._generate_rand_unique_str('pwd_')
        rand_comment = key_provider._generate_rand_unique_str('comment_')
        flush_by_node(self.cluster)
        debug('Add %d users for updating system_auth.roles' % user_num)
        for i in range(user_num):
            rand_user = '%s_%d' % (rand_user_prefix, i)
            session.execute("CREATE USER %s WITH PASSWORD '%s' NOSUPERUSER" % (rand_user, rand_password))
        rand_user = '%s_%d' % (rand_user_prefix, 0)
        debug('First user: %s, Password: %s' % (rand_user, rand_password))
        assert_one(session, "LIST ROLES of %s" % rand_user, [rand_user, False, True, {}])

        debug('Verify PART 1: check commitlogs -------------')
        time.sleep(10)  # sleep to wait data to be wrote into commitlogs
        debug('GREP_DB_FILES: Check original password in commitlogs .... Original password should never be saved')
        key_provider._grep_database_files(rand_password, 'commitlogs/', expect=False)
        res = session.execute("SELECT salted_hash FROM system_auth.roles WHERE role='%s'" % rand_user)
        salted_hash = rows_to_list(res)[0][0]
        debug('Original salted_hash in system_auth.roles:\n%s' % salted_hash)
        # ignore short prefix and suffix in searching binary to avoid error
        # skip fixed prefix `$6$`, one more byte, and 2 chars suffix
        salted_hash = salted_hash[4:-2].replace('/', r'\/')
        debug('GREP_DB_FILES: Check PM key user in commitlogs ....')
        key_provider._grep_database_files(rand_user, 'commitlogs/', expect=expect)
        debug('GREP_DB_FILES: Check salted_hash of password in commitlogs ....')
        key_provider._grep_database_files(salted_hash, 'commitlogs/', expect=expect)

        ks_name = 'ks_' + ks_suffix
        self.create_ks(session, ks_name, 3)
        for i in range(table_num):
            table_name = '%s.table_%d' % (ks_name, i)
            self.create_cf(session, table_name)
            session.execute("ALTER TABLE %s WITH comment = '%s'" % (table_name, rand_comment))
        debug('GREP_DB_FILES: Check table comment in commitlogs ....')
        key_provider._grep_database_files(rand_comment, 'commitlogs/', expect=expect)

        debug('Flushing cluster ......')
        self.cluster.flush()

        debug("Verify PART 2: check sstable files -------------\n`system_info_encryption` won't encrypt sstable files on disk")
        debug('GREP_DB_FILES: Check PM key user in sstable file ....')
        key_provider._grep_database_files(rand_user, 'data/system_auth/', expect=True)
        debug('GREP_DB_FILES: Check original password in commitlogs .... Original password should never be saved')
        key_provider._grep_database_files(rand_password, 'data/system_auth/', expect=False)
        debug("GREP_DB_FILES: Check salted_hash of password in sstable file ....")
        key_provider._grep_database_files(salted_hash, 'data/system_auth/', expect=True)
        debug('GREP_DB_FILES: Check table comment in sstable file ....')
        key_provider._grep_database_files(rand_comment.replace('comment_', ''), 'data/system_schema/', expect=True)

    def system_auth_encryption_test(self, key_provider=KeyProviderEnum.local):
        options = {'authenticator': 'org.apache.cassandra.auth.PasswordAuthenticator',
                   'authorizer': 'org.apache.cassandra.auth.CassandraAuthorizer'
                   }
        self.cluster.set_configuration_options(options)
        self.cluster.populate(3).start(wait_for_binary_proto=True, wait_other_notice=True)
        self.wait_for_any_log(self.cluster.nodelist(), 'Created default superuser', 10)
        node1 = self.cluster.nodelist()[0]
        session = self.patient_cql_connection(node1, user='cassandra', password='cassandra')
        debug('Set RF of system_auth to 3')
        session.execute("ALTER KEYSPACE system_auth "
                        "WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};")
        self.cluster.repair()
        kp = self.get_key_provider(key_provider)
        self.verify_system_info(session, kp, ks_suffix='orig', expect=True)

        options = {'system_info_encryption': {'enabled': True, 'key_provider': 'LocalFileSystemKeyProviderFactory'}}
        self.cluster.set_configuration_options(options)
        debug("\n\nRestarting nodes one by one ...... Make sure encryption change is persistent\n")
        session = self.rolling_restart(user='cassandra', password='cassandra')
        debug("Re-verify system info after system_info_encryption is enabled")
        self.verify_system_info(session, kp, ks_suffix='encrypt', expect=False)

    def reboot_test(self):
        """
        The test is used to reproduce a scylla crash, enable commitlog encryption and reboot.
        https://github.com/scylladb/scylla-enterprise/issues/1332
        """
        kp = self.get_key_provider(key_provider=None)
        kp.prepare_conf()

        self.prepare(n=3, restart=False)
        options = {'system_info_encryption': {'enabled': True, 'key_provider': 'LocalFileSystemKeyProviderFactory'}}
        self.cluster.set_configuration_options(options)
        debug("\n\nRestarting nodes one by one ...... Make sure encryption change is persistent\n")
        session = self.rolling_restart()

        kp.create_encrypted_cf(session, name='ks.cf')
        kp.prepare_write_workload(session, flush=False)

        for node in self.cluster.nodelist()[1:]:
            for i in range(3):
                debug('Kill node {}, and restart'.format(node.name))
                node.stop(gently=False)
                node.start(wait_for_binary_proto=True)
            kp.read_verify_workload(self.get_session())
