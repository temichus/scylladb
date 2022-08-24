from hashlib import md5
import time
import subprocess
import os
import shutil
import re
from enum import Enum
import logging

import pytest
from cassandra import ReadFailure, ConsistencyLevel
from cassandra.cluster import NoHostAvailable

from dtest_class import Tester, create_ks, create_cf
from tools.data import insert_c1c2, query_c1c2, rows_to_list
from tools.snapshots import get_table_description
from tools.misc import flush_by_node
from tools.assertions import assert_one
from tools.log_utils import wait_for_any_log

logger = logging.getLogger(__name__)


@pytest.mark.single_node
@pytest.mark.dtest_enterprise
class TestInMemory(Tester):
    """
    Test in memory sstable when Encryption at-rest is enabled.
    a reproducer for scylla-enterprise/issues/925
    """

    def test_restart_query(self):
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
        logger.debug(list(result))

        for node in self.cluster.nodelist():
            node.stop()
            node.start(wait_for_binary_proto=True, wait_other_notice=True)
        session = self.patient_cql_connection(node1)
        result = session.execute("select * from rest.table1")
        logger.debug(list(result))

    def test_workload_without_restart(self):
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
        logger.debug('flushing ...')
        self.cluster.flush()
        node1.stress(['read', 'n=100000', 'cl=QUORUM', '-rate', 'threads=8'])

# remove "default", as this is same as None, and gives us a replicated provider


class KeyProviderEnum(Enum):
    local = 'LocalFileSystemKeyProviderFactory'
    replicated = 'ReplicatedKeyProviderFactory'
    kmip = 'KmipKeyProviderFactory'


# default: 'AES/CBC/PKCS5Padding', length 128
supported_cipher_algorithms = {'': [],
                               'AES/CBC/PKCS5Padding': [128, 192, 256],  # 192 has problem
                               'AES/CBC': [128, 192, 256],  # 192 has problem
                               'AES': [128, 192, 256],  # 192 has problem
                               'AES/ECB/PKCS5Padding': [128, 192, 256],
                               'AES/ECB': [128, 192, 256],
                               'DES/CBC/PKCS5Padding': [56],
                               'DES/CBC': [56],
                               'DES': [56],
                               # 'DESede/CBC/PKCS5Padding': [112, 168],    # not support by Scylla, supported by DSE
                               # 'Blowfish/CBC/PKCS5Padding': [32, 448],   # not support by Scylla, supported by DSE
                               'RC2/CBC/PKCS5Padding':  [80, 128],  # [40, 80, 128]  # 40 to 128
                               'RC2/CBC':  [80, 128],  # [40, 80, 128]  # 40 to 128
                               'RC2':  [80, 128]  # [40, 80, 128]  # 40 to 128
                               }


class BaseKeyProviderFactory:
    def __init__(self, key_provider, tester):
        self.key_provider = key_provider
        self.system_keyfile = None
        self.kmip_host = None
        self.tester = tester
        self.cluster = tester.cluster

    def __enter__(self):
        self.prepare_conf()
        return self

    def __exit__(self, exception_type, exception_value, exception_traceback):
        pass

    def supported_cipher(self, cipher_algorithm, secret_key_strength):
        return True

    def require_restart(self):
        return False

    def prepare_conf(self):
        pass

    def prepare_system_key(self, dirname='./resources/system_keys/', keyfile='system_key', cipher_algorithm='AES/CBC/PKCS5Padding', secret_key_strength=128):
        if not os.path.exists(dirname):
            os.mkdir(dirname)
        dirname = os.path.realpath(dirname)
        dest = os.path.join(dirname, keyfile)
        # use saved key in dtest repo, generate it in future
        # the key can also be created by `dsetool createsystemkey $cipher_algorithm $strength`
        src = './resources/system_keys/system_key'  # AES/ECB/PKCS5Padding:128
        if not os.path.exists(dest) or not os.path.samefile(src, dest):
            shutil.copy(src, dest)

        self.cluster.set_configuration_options({'system_key_directory': dirname})
        self.system_keyfile = dest
        logger.debug('set system_key_directory to %s' % dirname)

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
        create_cf(session, name, columns=columns, scylla_encryption_options=options, compression=compression)
        return options

    def verify_no_secret_key(self):
        logger.debug('Verify that system key is not generated automatically')
        keyfile = os.path.join(self.tester.test_path, 'test/node1/conf/data_encryption_keys')
        assert not os.path.exists(keyfile), 'Default system_key is generated unexpectedly'

    def verify_secret_key(self, cipher_algorithm=None, secret_key_strength=None):
        logger.debug('Verify that local key is generated automatically')
        logger.debug('Verify that system key is generated automatically')
        keyfile = os.path.join(self.tester.test_path, 'test/node1/conf/data_encryption_keys')
        assert os.path.exists(keyfile), 'Default system_key is not generated'

        if cipher_algorithm is None:
            cipher_algorithm = 'AES/CBC/PKCS5Padding'
        if secret_key_strength is None:
            secret_key_strength = 128
        found = False
        with open(keyfile) as f:
            for line in f.readlines():
                if line.startswith('%s:%d:' % (cipher_algorithm, secret_key_strength)):
                    logger.debug('Found system key: %s' % line)
                    found = True
        assert found, 'Did not find specific local key in %s' % keyfile


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

    def require_restart(self):
        return True

    def supported_cipher(self, cipher_algorithm, secret_key_strength):
        # Our KMIP server is not configured to support this configuration.
        # Test fails with error: Invalid key data length 80 for RC2/CBC and kmip.
        # Decided (Roy) don't test it
        return not ('RC2' in cipher_algorithm and secret_key_strength == 80)


class EncryptionAtRestBase(Tester):
    multiple_num = 3
    default_node_num = 2
    system_key_dir = './resources/system_keys/'

    def get_session(self, node_idx=0, user=None, password=None):
        node = self.cluster.nodelist()[node_idx]
        conn = self.patient_cql_connection(node, user=user, password=password)
        return conn

    def create_ks(self, kss=['ks'], n=default_node_num):
        session = self.get_session()
        for ks in kss:
            session.execute(
                f"CREATE KEYSPACE IF NOT EXISTS {ks} WITH REPLICATION = {{'class' : 'SimpleStrategy', "
                f"'replication_factor' : {n} }}")

    def prepare(self, n=default_node_num, kss=['ks'], restart=False):
        self.cluster.set_configuration_options({'system_key_directory': EncryptionAtRestBase.system_key_dir})
        logger.debug('set system_key_directory to %s', EncryptionAtRestBase.system_key_dir)
        if not self.cluster.nodelist():
            self.cluster.populate(n).start(wait_for_binary_proto=True, wait_other_notice=True)
        elif restart:
            self.rolling_restart()
        session = self.get_session()
        self.create_ks(kss=kss, n=n)
        return session

    def drop_keyspace(self, kss=['ks']):
        session = self.get_session()
        for ks in kss:
            session.execute('DROP KEYSPACE IF EXISTS %s' % ks)

    def drop_cf(self, name='ks.cf'):
        session = self.get_session()
        session.execute('DROP TABLE IF EXISTS %s' % name)

    def cleanup(self, kss=['ks']):
        self.drop_keyspace(kss=kss)

    def read_verify_workload(self, session, ks='ks', cf='cf'):
        logger.debug('Verify data by read stress: %s.%s', ks, cf)
        for i in range(100):
            query_c1c2(session, i, ConsistencyLevel.QUORUM, ks=ks, cf=cf)

    def prepare_write_workload(self, session, ks='ks', cf='cf', flush=True):
        logger.debug('Insert data to encrypted table: %s.%s', ks, cf)
        insert_c1c2(session, keys=list(range(100)), consistency=ConsistencyLevel.ALL, ks=ks, cf=cf)
        if flush:
            logger.debug('flush cluster')
            self.cluster.flush()

    def rolling_restart(self, user=None, password=None, allow_start_failure=False):
        logger.debug(f'Restart nodes one by one ...{" (start failures allowed)" if allow_start_failure else ""}')
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
        logger.debug('Restart cluster ...')
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
        elif key_provider is None:
            ret = DefaultKeyProviderFactory(self)
        else:
            raise Exception('Unknown key_provider: %s' % key_provider)
        return ret

    def _filter_cipher(self, kp, cipher_algorithm, secret_key_strength):
        if not kp.supported_cipher(cipher_algorithm, secret_key_strength):
            logger.debug("%s does not support configuration %s.%d. The test will not be run with this configuration".format(
                kp, cipher_algorithm, secret_key_strength))
            return False
        return True

    def filter_ciphers(self, kp, ciphers=None):
        if not ciphers:
            return [(None, None)]
        return [(cipher, len) for cipher in ciphers for len in ciphers[cipher] if self._filter_cipher(kp, cipher, len)]

    def _smoke_test(self, key_provider=KeyProviderEnum.local, cipher_algorithm=None, secret_key_strength=None,
                    compression=None):
        # Our KMIP server is not configured to support this configuration.
        # Test fails with error: Invalid key data length 80 for RC2/CBC and kmip.
        # Decided (Roy) don't test it
        if key_provider == KeyProviderEnum.kmip and 'RC2' in cipher_algorithm and secret_key_strength == 80:
            logger.debug("Our KMIP server is not configured to support this configuration. "
                         "The test will not be run with this configuration")
            return

        kp = self.get_key_provider(key_provider)
        kp.prepare_conf()
        session = self.prepare(restart=kp.require_restart(), n=self.default_node_num)
        if cipher_algorithm and secret_key_strength:
            kp.create_encrypted_cf(session, name='ks.cf', cipher_algorithm=cipher_algorithm,
                                   secret_key_strength=secret_key_strength, compression=compression)
        else:
            kp.create_encrypted_cf(session, name='ks.cf', compression=compression)
        self.prepare_write_workload(session)
        if key_provider == KeyProviderEnum.local:
            kp.verify_secret_key(cipher_algorithm, secret_key_strength)
        # restart the cluster
        session = self.rolling_restart()
        self.read_verify_workload(session)

    def _upgrade_sstables(self):
        for node in self.cluster.nodelist():
            out, err = node.nodetool('upgradesstables')

    def _alter_test(self, key_provider=KeyProviderEnum.local):
        kp = self.get_key_provider(key_provider)
        kp.prepare_conf()
        session = self.prepare(restart=kp.require_restart())
        node1 = self.cluster.nodelist()[0]
        options = kp.create_encrypted_cf(session, name='ks.cf')
        query = "ALTER TABLE ks.cf with scylla_encryption_options=%s"

        self.prepare_write_workload(session)
        logger.debug('disable encryption at-rest')
        session.execute(query % "{'key_provider': 'none'}")
        table_desc = get_table_description(node1, "ks", "cf")
        assert "key_provider" not in table_desc, f"key_provider isn't disabled, schema:\n {table_desc}"
        self._upgrade_sstables()
        session = self.rolling_restart()
        self.read_verify_workload(session)

        logger.debug('re-enable encryption at-rest: %s' % options)
        session.execute(query % options)
        table_desc = get_table_description(node1, "ks", "cf")
        if key_provider == None:
            assert "key_provider" not in table_desc, f"key_provider isn't unspecified, schema:\n {table_desc}"
        else:
            err_msg = f"key_provider isn't changed to {key_provider.value}, schema: \n {table_desc}"
            assert f"'key_provider': '{key_provider.value}'" in table_desc, err_msg
        self._upgrade_sstables()
        session = self.rolling_restart()
        self.read_verify_workload(session)

    def _multiple_ks_test(self, key_provider=KeyProviderEnum.local):
        kss = ['mks_%s' % i for i in range(self.multiple_num)]
        kp = self.get_key_provider(key_provider)
        kp.prepare_conf()
        session = self.prepare(kss=kss, restart=kp.require_restart())
        secret_key_file = None
        system_key_file = None
        for ks in kss:
            if key_provider == KeyProviderEnum.local:
                secret_key_file = './resources/secret_key_file_' + ks
            elif key_provider == KeyProviderEnum.replicated:
                system_key_file = 'system_key_' + ks

            kp.create_encrypted_cf(session, name=ks + '.cf', system_key_file=system_key_file,
                                   secret_key_file=secret_key_file)
            self.prepare_write_workload(session, ks=ks)
        session = self.rolling_restart()
        for ks in kss:
            self.read_verify_workload(session, ks=ks)
        return kss

    def _multiple_cf_test(self, key_provider=KeyProviderEnum.local):
        cfs = ['cf_%d' % i for i in range(self.multiple_num)]
        kp = self.get_key_provider(key_provider)
        kp.prepare_conf()
        session = self.prepare(restart=kp.require_restart())
        secret_key_file = None
        system_key_file = None
        for cf in cfs:
            if key_provider == KeyProviderEnum.local:
                secret_key_file = './resources/secret_key_file_' + cf
            elif key_provider == KeyProviderEnum.replicated:
                system_key_file = 'system_key_' + cf
            kp.create_encrypted_cf(session, name='ks.' + cf, system_key_file=system_key_file,
                                   secret_key_file=secret_key_file)
            self.prepare_write_workload(session, cf=cf)
        session = self.rolling_restart()
        for cf in cfs:
            self.read_verify_workload(session, cf=cf)

    def _reboot_test(self, key_provider=KeyProviderEnum.local):
        kp = self.get_key_provider(key_provider)
        kp.prepare_conf()
        self.prepare(n=3, restart=kp.require_restart())

        session = self.get_session()
        kp.create_encrypted_cf(session, name='ks.cf')
        self.prepare_write_workload(session, flush=False)

        for node in self.cluster.nodelist()[1:]:
            for i in range(3):
                logger.debug('Kill node {}, and restart'.format(node.name))
                node.stop(gently=False)
                node.start(wait_for_binary_proto=True, wait_other_notice=False)
            self.read_verify_workload(self.get_session())


@pytest.mark.dtest_enterprise
class TestEncryptionAtRest(EncryptionAtRestBase):
    default_node_num = 1

    def _test_one_cipher_mode(self, tested_cipher_key_string: str, key_size: int, value: KeyProviderEnum) -> str:
        logger.debug(f'---- Test with {tested_cipher_key_string} , length {key_size}, key provider {value} ----')
        try:
            self._smoke_test(key_provider=value,
                             cipher_algorithm=tested_cipher_key_string,
                             secret_key_strength=key_size)
            # Our KMIP server is not configured to support this configuration.
            # Test fails with error: Invalid key data length 80 for RC2/CBC and kmip.
            # Decided (Roy) don't test it
            # TODO: In case of wrong block mode Scylla silently falls back to no block mode if openssl does
            # TODO: not like the input. Next validation should be uncomment when issue
            #  https://github.com/scylladb/scylla-enterprise/issues/1973 will be resolve
            # if not (value == KeyProviderEnum.kmip and 'RC2' in tested_cipher_key_string and key_size == 80):
            #   unexpected_success.append(f"Encryption option: 'key_provider': '{value}', "
            #                               f"'cipher_algorithm': '{tested_cipher_key_string}', "
            #                               f"'secret_key_strength': {key_size}")

        except NoHostAvailable as exc_details:
            error_message_to_str = str(exc_details)
            logger.debug(error_message_to_str)
            assert (f"Invalid algorithm string: {tested_cipher_key_string}" in error_message_to_str
                    or (f"Invalid algorithm" in error_message_to_str and
                        tested_cipher_key_string in error_message_to_str)
                    or 'Could not write key file' in error_message_to_str
                    or ('[Server error] message=' in error_message_to_str and 'abc' in error_message_to_str)
                    or 'non-supported padding option' in error_message_to_str
                    # TODO: There are a few cases when we have nested exceptions that "hide" the original message
                    # TODO: once it reaches cql layer. So the error message is returned empty
                    # TODO: Issue: https://github.com/scylladb/scylla/issues/9497
                    #  TODO: Remove next condition when the issue will be resolved
                    or error_message_to_str == "('Unable to complete the operation against any hosts', {})"
                    ), error_message_to_str

        except Exception as exc:
            return (f"Unexpected exception: {exc}. "
                    f"Encryption option: 'key_provider': '{value}', "
                    f"'cipher_algorithm': '{tested_cipher_key_string}', "
                    f"'secret_key_strength': {key_size}")

        self.cleanup()

        return ''

    def test_encryption_table_compression(self):
        for i in [None, 'LZ4', 'Snappy', 'Deflate']:
            logger.debug('---- Test with compression: %s -----' % i)
            self._smoke_test(key_provider=KeyProviderEnum.local, compression=i)
            self.cleanup()

    @pytest.mark.timeout(4700)
    def test_wrong_cipher_algorithm(self):
        errors = []
        # TODO: Uncomment next line when issue https://github.com/scylladb/scylla-enterprise/issues/1973 will be resolve
        # unexpected_success = []
        for cipher_key_string, key_sizes in supported_cipher_algorithms.items():
            for key_size in key_sizes:
                for value in KeyProviderEnum:
                    for additional_str in ['Abc/', '/Abc', 'Abc']:
                        tested_cipher_key_string = f'{cipher_key_string}{additional_str}'  # suffix
                        error = self._test_one_cipher_mode(tested_cipher_key_string, key_size, value)
                        if error:
                            errors.append(error)

                        tested_cipher_key_string = f'{additional_str}{cipher_key_string}'  # prefix
                        error = self._test_one_cipher_mode(tested_cipher_key_string, key_size, value)
                        if error:
                            errors.append(error)

        # TODO: Uncomment next line when issue https://github.com/scylladb/scylla-enterprise/issues/1973 will be resolve
        # assert not unexpected_success, "Negative tests succeeded unexpectedly: %s" % '\n'.join(unexpected_success)

        assert not errors, errors

    @pytest.mark.timeout(4000)
    def test_supported_cipher_algorithms(self):
        errors = []
        for cipher_key_string, key_sizes in supported_cipher_algorithms.items():
            for key_size in key_sizes:
                for value in KeyProviderEnum:
                    logger.debug(f'---- Test with {cipher_key_string} , length {key_size}, key provider {value} ----')
                    try:
                        self._smoke_test(key_provider=value,
                                         cipher_algorithm=cipher_key_string,
                                         secret_key_strength=key_size)
                    except Exception as e:
                        logger.debug(str(e))
                        errors.append(f"Test with configuration '{cipher_key_string}, length {key_size}, "
                                      f"key provider {value}' failed. Error {e}")

                    self.cleanup()

        assert len(errors) == 0, errors

    def test_abbreviated_supported_cipher_algorithms(self):
        tested = set()
        for k, v in supported_cipher_algorithms.items():
            if not v:
                continue
            k = k.split('/')[0]
            if k in tested or not k:
                continue
            tested.add(k)
            i = v[0]
            logger.debug('---- Test with %s , length %s ----' % (k, i))
            for value in KeyProviderEnum:
                try:
                    self._smoke_test(key_provider=value,
                                     cipher_algorithm=k, secret_key_strength=i)
                except Exception as e:
                    logger.debug(str(e))
                finally:
                    self.cleanup()

    def test_multiple_ks(self):
        for value in KeyProviderEnum:
            kss = self._multiple_ks_test(key_provider=value)
            self.cleanup(kss=kss)

    def test_multiple_cf(self):
        for value in KeyProviderEnum:
            self._multiple_cf_test(key_provider=value)
            self.cleanup()

    def test_reboot(self):
        for value in KeyProviderEnum:
            self._reboot_test(key_provider=value)
            self.cleanup()

    @pytest.mark.require('scylladb/scylla-enterprise#1787')
    def test_alter(self):
        for value in KeyProviderEnum:
            self._alter_test(key_provider=value)
            self.cleanup()


@pytest.mark.dtest_enterprise
class TestSystemInfoEncryption(EncryptionAtRestBase):
    def _grep_database_files(self, pattern, path, expect=None, skip=False, debug_detail=True):
        """
        skip: skip check in topdir and result assert for avoiding dead loop
        """
        grep_commitlog_cmd = "grep -r '%s' %s" % (pattern, os.path.join(self.test_path, 'test/node*/', path))
        output = subprocess.getoutput(grep_commitlog_cmd)
        logger.debug('\tExpect: %s, Result: %s' % (expect, len(output) > 0))
        if debug_detail:
            logger.debug('\tCMD: %s' % grep_commitlog_cmd)
            logger.debug(output)
        if skip:
            return len(output) != 0
        if expect is not False and (len(output) == 0):
            # try to search pattern in top directory (contains both data & commitlogs) for trouble shooting.
            # such as, data isn't flush from commitlogs to disk,
            logger.warning('%s does not exist in %s!!' % (pattern, path))
            self._grep_database_files(pattern, '', skip=True)
        if expect is not None:
            assert expect ^ (len(output) == 0), "Grep result isn't expected"
        return len(output) != 0

    def _generate_rand_unique_str(self, prefix=''):
        time.sleep(0.1)
        return prefix + md5(str(time.time()).encode('utf-8')).hexdigest()

    def verify_system_info(self, session, key_provider, ks_suffix='', expect=True):
        table_num = 10
        user_num = 5
        rand_user_prefix = self._generate_rand_unique_str('user_')
        rand_password = self._generate_rand_unique_str('pwd_')
        rand_comment = self._generate_rand_unique_str('comment_')
        flush_by_node(self.cluster)
        logger.debug('Add %d users for updating system_auth.roles' % user_num)
        for i in range(user_num):
            rand_user = '%s_%d' % (rand_user_prefix, i)
            session.execute("CREATE USER %s WITH PASSWORD '%s' NOSUPERUSER" % (rand_user, rand_password))
        rand_user = '%s_%d' % (rand_user_prefix, 0)
        logger.debug('First user: %s, Password: %s' % (rand_user, rand_password))
        assert_one(session, "LIST ROLES of %s" % rand_user, [rand_user, False, True, {}])

        logger.debug('Verify PART 1: check commitlogs -------------')
        time.sleep(10)  # sleep to wait data to be wrote into commitlogs
        logger.debug('GREP_DB_FILES: Check original password in commitlogs .... Original password should never be saved')
        self._grep_database_files(rand_password, 'commitlogs/', expect=False)
        res = session.execute("SELECT salted_hash FROM system_auth.roles WHERE role='%s'" % rand_user)
        salted_hash = rows_to_list(res)[0][0]
        logger.debug('Original salted_hash in system_auth.roles:\n%s' % salted_hash)
        # ignore short prefix and suffix in searching binary to avoid error
        # skip fixed prefix `$6$`, one more byte, and 2 chars suffix
        salted_hash = salted_hash[4:-2].replace('/', r'\/')
        logger.debug('GREP_DB_FILES: Check PM key user in commitlogs ....')
        self._grep_database_files(rand_user, 'commitlogs/', expect=expect)
        logger.debug('GREP_DB_FILES: Check salted_hash of password in commitlogs ....')
        self._grep_database_files(salted_hash, 'commitlogs/', expect=expect)

        ks_name = 'ks_' + ks_suffix
        create_ks(session, ks_name, 3)
        for i in range(table_num):
            table_name = '%s.table_%d' % (ks_name, i)
            create_cf(session, table_name)
            session.execute("ALTER TABLE %s WITH comment = '%s'" % (table_name, rand_comment))
        logger.debug('GREP_DB_FILES: Check table comment in commitlogs ....')
        self._grep_database_files(rand_comment, 'commitlogs/', expect=expect)

        logger.debug('Flushing cluster ......')
        self.cluster.flush()

        logger.debug(
            "Verify PART 2: check sstable files -------------\n`system_info_encryption` won't encrypt sstable files on disk")
        logger.debug('GREP_DB_FILES: Check PM key user in sstable file ....')
        self._grep_database_files(rand_user, 'data/system_auth/', expect=True)
        logger.debug('GREP_DB_FILES: Check original password in commitlogs .... Original password should never be saved')
        self._grep_database_files(rand_password, 'data/system_auth/', expect=False)
        logger.debug("GREP_DB_FILES: Check salted_hash of password in sstable file ....")
        self._grep_database_files(salted_hash, 'data/system_auth/', expect=True)
        logger.debug('GREP_DB_FILES: Check table comment in sstable file ....')
        self._grep_database_files(rand_comment.replace('comment_', ''), 'data/system_schema/', expect=True)

    def test_system_auth_encryption(self, key_provider=KeyProviderEnum.local):
        options = {'authenticator': 'org.apache.cassandra.auth.PasswordAuthenticator',
                   'authorizer': 'org.apache.cassandra.auth.CassandraAuthorizer'
                   }
        self.cluster.set_configuration_options(options)
        self.cluster.populate(3).start(wait_for_binary_proto=True, wait_other_notice=True)
        wait_for_any_log(self.cluster.nodelist(), 'Created default superuser', 10)
        node1 = self.cluster.nodelist()[0]
        session = self.patient_cql_connection(node1, user='cassandra', password='cassandra')
        logger.debug('Set RF of system_auth to 3')
        session.execute("ALTER KEYSPACE system_auth "
                        "WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};")
        self.cluster.repair()
        kp = self.get_key_provider(key_provider)
        self.verify_system_info(session, kp, ks_suffix='orig', expect=True)

        options = {'system_info_encryption': {'enabled': True, 'key_provider': 'LocalFileSystemKeyProviderFactory'}}
        self.cluster.set_configuration_options(options)
        logger.debug("\n\nRestarting nodes one by one ...... Make sure encryption change is persistent\n")
        session = self.rolling_restart(user='cassandra', password='cassandra')
        logger.debug("Re-verify system info after system_info_encryption is enabled")
        self.verify_system_info(session, kp, ks_suffix='encrypt', expect=False)

    def test_reboot(self):
        """
        The test is used to reproduce a scylla crash, enable commitlog encryption and reboot.
        https://github.com/scylladb/scylla-enterprise/issues/1332
        """
        kp = self.get_key_provider(key_provider=None)
        kp.prepare_conf()

        self.prepare(n=3, restart=False)
        options = {'system_info_encryption': {'enabled': True, 'key_provider': 'LocalFileSystemKeyProviderFactory'}}
        self.cluster.set_configuration_options(options)
        logger.debug("\n\nRestarting nodes one by one ...... Make sure encryption change is persistent\n")
        session = self.rolling_restart()

        kp.create_encrypted_cf(session, name='ks.cf')
        self.prepare_write_workload(session, flush=False)

        for node in self.cluster.nodelist()[1:]:
            for i in range(3):
                logger.debug('Kill node {}, and restart'.format(node.name))
                node.stop(gently=False)
                node.start(wait_for_binary_proto=True)
            self.read_verify_workload(self.get_session())
