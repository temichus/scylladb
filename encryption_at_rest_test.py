import ctypes
import logging
import re
import socket
import ssl
import threading
from hashlib import md5
import time
import subprocess
import os
import shutil
import json
from enum import Enum
import logging
import tempfile
from pathlib import Path

import docker
import pytest
import boto3
from cassandra import ConsistencyLevel
from cassandra.cluster import NoHostAvailable
from cassandra.protocol import ConfigurationException
from kmip.services import auth
from kmip.services.server.server import KmipServer
from packaging.version import Version
from ccmlib.node import ToolError
from dtest_class import Tester, create_ks, create_cf
from tools.data import insert_c1c2, query_c1c2, rows_to_list
from tools.misc import flush_by_node, generate_ssl_stores
from tools.snapshots import get_table_description
from tools.assertions import assert_one
from tools.log_utils import wait_for_any_log
from tools.ldap_docker import running_in_docker
from tools.marks import unmark
from tools.files import get_list_of_sstables
from tools.context import disable_autocompaction

logger = logging.getLogger(__name__)


# remove "default", as this is same as None, and gives us a replicated provider


class KeyProviderEnum(Enum):
    local = 'LocalFileSystemKeyProviderFactory'
    replicated = 'ReplicatedKeyProviderFactory'
    kmip = 'KmipKeyProviderFactory'
    kms = 'KmsKeyProviderFactory'
    kms_real = 'KmsRealKeyProviderFactory'


# default: 'AES/CBC/PKCS5Padding', length 128
supported_cipher_algorithms = {
    "": [],
    "AES/CBC/PKCS5Padding": [128, 192, 256],  # 192 has problem
    "AES/CBC": [128, 192, 256],  # 192 has problem
    "AES": [128, 192, 256],  # 192 has problem
    "AES/ECB/PKCS5Padding": [128, 192, 256],
    "AES/ECB": [128, 192, 256],
    # legacy algorithms, not supported in openssl 3.x
    # "DES/CBC/PKCS5Padding": [56],
    # "DES/CBC": [56],
    # "DES": [56],
    # 'DESede/CBC/PKCS5Padding': [112, 168],    # not support by Scylla, supported by DSE
    # 'Blowfish/CBC/PKCS5Padding': [32, 448],   # not support by Scylla, supported by DSE
    # "RC2/CBC/PKCS5Padding": [80, 128],  # [40, 80, 128]  # 40 to 128
    # "RC2/CBC": [80, 128],  # [40, 80, 128]  # 40 to 128
    # "RC2": [80, 128],  # [40, 80, 128]  # 40 to 128
}


class BaseKeyProviderFactory:
    def __init__(self, key_provider, tester):
        self.key_provider = key_provider
        self.system_keyfile = None
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

    def additional_cf_options(self, ks=None):
        if self.key_provider:
            return {'key_provider': self.key_provider.value}
        return {}

    def verify_secret_key(self, cipher_algorithm=None, secret_key_strength=None):
        pass


class DefaultKeyProviderFactory(BaseKeyProviderFactory):
    def __init__(self, tester):
        BaseKeyProviderFactory.__init__(self, None, tester)


class LocalFileSystemKeyProviderFactory(BaseKeyProviderFactory):
    def __init__(self, tester):
        self.secret_file = os.path.join(tester.test_path, 'test/node1/conf/data_encryption_keys')
        BaseKeyProviderFactory.__init__(self, KeyProviderEnum.local, tester)

    def additional_cf_options(self, ks=None):
        return super().additional_cf_options() | {'secret_key_file': os.path.join(self.tester.test_path, 'test/node1/conf/secret_key_file_' + ks) if ks else self.secret_file}

    def verify_secret_key(self, cipher_algorithm=None, secret_key_strength=None):
        logger.debug('Verify that local key is generated automatically')
        keyfile = os.path.join(self.tester.test_path, 'test/node1/conf/data_encryption_keys')
        assert os.path.exists(keyfile), 'Default local key is not generated'

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


class ReplicatedKeyProviderFactory(BaseKeyProviderFactory):
    def __init__(self, tester):
        BaseKeyProviderFactory.__init__(self, KeyProviderEnum.replicated, tester)

    def prepare_conf(self):
        # prepare_secret_key(self)
        pass

    def additional_cf_options(self, ks=None):
        return super().additional_cf_options(ks) | {'system_key': 'system_key_' + ks if ks else 'system_key'}


class KmipKeyProviderFactory(BaseKeyProviderFactory):
    class TLS13AuthenticationSuite(auth.TLS12AuthenticationSuite):
        """
        An authentication suite used to establish secure network connections.
        Supports TLS 1.3. More importantly, works with gnutls-<recent>
        """

        def __init__(self, cipher_suites=None):
            """
            Create a TLS12AuthenticationSuite object.
            Args:
                cipher_suites (list): A list of strings representing the names of
                    cipher suites to use. Overrides the default set of cipher
                    suites. Optional, defaults to None.
            """
            super().__init__(cipher_suites)
            self._protocol = ssl.PROTOCOL_TLS_SERVER

    @staticmethod
    def fake_wrap_ssl(sock, keyfile=None, certfile=None, server_side=False, cert_reqs=ssl.CERT_NONE, ssl_version=ssl.PROTOCOL_TLS, ca_certs=None, do_handshake_on_connect=True, suppress_ragged_eofs=True, ciphers=None):  # noqa: PLR0913
        ctxt = ssl.SSLContext(protocol=ssl_version)
        ctxt.load_cert_chain(certfile=certfile, keyfile=keyfile)
        ctxt.verify_mode = cert_reqs
        ctxt.load_verify_locations(cafile=ca_certs)
        ctxt.set_ciphers(ciphers)
        return ctxt.wrap_socket(sock, server_side=server_side, do_handshake_on_connect=do_handshake_on_connect, suppress_ragged_eofs=suppress_ragged_eofs)

    def __init__(self, tester):
        self.kmip_host = "kmip_test"
        self.kmip_port = 0
        self.kmip_server = None
        self.kmip_thread = None
        self.tempdir = None
        self.certs = None
        ssl.wrap_socket = self.fake_wrap_ssl
        BaseKeyProviderFactory.__init__(self, KeyProviderEnum.kmip, tester)

    def prepare_conf(self):
        # restart is request to make change effective
        options = {
            "hosts": "127.0.0.1:" + str(self.kmip_port),
            "certificate": self.certs["certfile"],
            "keyfile": self.certs["keyfile"],
            "truststore": self.certs["truststore"],
            "priority_string": "SECURE128:+RSA:-VERS-TLS1.0:-ECDHE-ECDSA",
        }
        self.cluster.set_configuration_options({"kmip_hosts": {self.kmip_host: options}})

    def kmip_serve(self):
        s = self.kmip_server
        assert s is not None

        s._socket.listen(5)
        s._logger.info("Starting connection service...")

        try:
            while s._is_serving:
                try:
                    connection, address = s._socket.accept()
                except TimeoutError:
                    # Setting the default socket timeout to break hung connections
                    # will cause accept to periodically raise socket.timeout. This
                    # is expected behavior, so ignore it and retry accept.
                    pass
                except OSError as e:
                    s._logger.warning("Error detected while establishing new connection.")
                    s._logger.exception(e)
                except KeyboardInterrupt:
                    s._is_serving = False
                    break
                except Exception as e:
                    s._logger.warning("Error detected while establishing new connection.")
                    s._logger.exception(e)
                else:
                    s._setup_connection_handler(connection, address)
        except KeyboardInterrupt:
            pass

        s._logger.info("Stopping connection service.")

    def __enter__(self):
        self.tempdir = tempfile.TemporaryDirectory()

        base_dir = self.tempdir.name
        generate_ssl_stores(base_dir)
        self.certs = {"certfile": os.path.join(base_dir, "ccm_node.pem"), "keyfile": os.path.join(
            base_dir, "ccm_node.key"), "truststore": os.path.join(base_dir, "trust.pem")}
        assert os.path.exists(self.certs["certfile"])
        assert os.path.exists(self.certs["keyfile"])
        assert os.path.exists(self.certs["truststore"])
        kmiplog = logging.getLogger("kmip.server")
        kmiplog.handlers.clear()  # make pykmip shut up a bit. log will written to log file (setup in init below)
        self.kmip_server = KmipServer(
            hostname="127.0.0.1",
            config_path=None,
            certificate_path=self.certs["certfile"],
            key_path=self.certs["keyfile"],
            ca_path=self.certs["truststore"],
            auth_suite="TLS1.2",
            database_path=os.path.join(self.tempdir.name, "pykmip.db"),
            log_path=os.path.join(self.tempdir.name, "pykmip.log"),
            enable_tls_client_auth=False,
        )
        assert len(kmiplog.handlers) == 1
        logger.info(kmiplog.handlers)
        self.kmip_server.auth_suite = self.TLS13AuthenticationSuite(self.kmip_server.auth_suite.ciphers)
        # force port to zero -> select dynamically
        self.kmip_server.config.settings["port"] = 0
        self.kmip_server.start()
        self.kmip_port = self.kmip_server._socket.getsockname()[1]
        self.kmip_thread = threading.Thread(name="kmip server", target=self.kmip_serve)
        self.kmip_thread.start()
        self.prepare_conf()
        return self

    def __exit__(self, exception_type, exception_value, exception_traceback):
        if self.kmip_thread is not None:
            logger.info(self.kmip_thread)
            ctypes.pythonapi.PyThreadState_SetAsyncExc(ctypes.c_long(
                self.kmip_thread.ident), ctypes.py_object(KeyboardInterrupt))
            self.kmip_thread.join()
            self.kmip_thread = None
        if self.kmip_server is not None:
            # self.kmip_server.stop()
            try:
                self.kmip_server._socket.shutdown(socket.SHUT_RDWR)
                self.kmip_server._socket.close()
            except:
                pass
            self.kmip_server = None
        if self.tempdir is not None:
            self.tempdir.cleanup()
            self.tempdir = None
        self.certs = None

    def additional_cf_options(self, ks=None):
        return super().additional_cf_options(ks) | {'kmip_host': self.kmip_host}

    def require_restart(self):
        return True

    def supported_cipher(self, cipher_algorithm, secret_key_strength):
        # Our KMIP server is not configured to support this configuration.
        # Test fails with error: Invalid key data length 80 for RC2/CBC and kmip.
        # Decided (Roy) don't test it
        return not ('RC2' in cipher_algorithm and secret_key_strength == 80)


class KMSKeyProviderFactory(BaseKeyProviderFactory):
    def __init__(self, tester):
        BaseKeyProviderFactory.__init__(self, KeyProviderEnum.kms, tester)
        self.container = None
        self.master_key = "alias/Scylla-test"
        self.kms_host = 'kms_test'
        self.endpoint_url = None
        self.client = docker.from_env()

    def connection(self):
        return boto3.client("kms", endpoint_url=self.endpoint_url, region_name='None')

    def prepare_conf(self):
        local_kms_image = "nsmithuk/local-kms:3"

        self.container = self.client.containers.run(local_kms_image, detach=True, ports={8080: None})
        self.container.reload()
        if running_in_docker():
            self.endpoint_url = f'http://{self.container.attrs["NetworkSettings"]["IPAddress"]}:8080'
        else:
            ports = self.container.attrs['NetworkSettings']['Ports']
            port = ports['8080/tcp'][0]['HostPort']
            self.endpoint_url = 'http://localhost:' + port

        try:
            # create master key
            kms_client = self.connection()
            response = kms_client.create_key(Description='dtest',
                                             Tags=[{
                                                 'TagKey': 'Name',
                                                 'TagValue': 'dtest'
                                             }])
            key_id = response['KeyMetadata']['KeyId']
            kms_client.create_alias(AliasName=self.master_key, TargetKeyId=key_id)

            options = {'endpoint': self.endpoint_url,
                       'master_key': self.master_key
                       }
            self.cluster.set_configuration_options({'kms_hosts': {self.kms_host: options}})
        except:
            self.container.stop()
            raise

    def __enter__(self):
        self.prepare_conf()
        return self

    def __exit__(self, exception_type, exception_value, exception_traceback):
        self.container.stop()
        self.container.remove()

    def additional_cf_options(self, ks=None):
        self.container.reload()
        return super().additional_cf_options(ks) | {'kms_host': self.kms_host}

    def supported_cipher(self, cipher_algorithm, secret_key_strength):
        return secret_key_strength >= 128

    def require_restart(self):
        return True


class KMSRealKeyProviderFactory(BaseKeyProviderFactory):
    def __init__(self, tester):
        BaseKeyProviderFactory.__init__(self, KeyProviderEnum.kms, tester)
        self.master_key = "alias/kms_encryption_test"
        self.kms_host = 'kms_test'

    def prepare_conf(self):
        options = {'master_key': self.master_key,
                   'aws_region': 'us-east-1'}
        self.cluster.set_configuration_options({'kms_hosts': {self.kms_host: options}})

    def __enter__(self):
        self.prepare_conf()
        return self

    def __exit__(self, exception_type, exception_value, exception_traceback):
        pass

    def additional_cf_options(self, ks=None):
        return super().additional_cf_options(ks) | {'kms_host': self.kms_host}

    def supported_cipher(self, cipher_algorithm, secret_key_strength):
        return secret_key_strength >= 128

    def require_restart(self):
        return True


def validate_sstables_encrypted(node, keyspace='ks', column_family='cf'):

    with disable_autocompaction(node, keyspace_name=keyspace, table_name=column_family):
        for sstable in get_list_of_sstables(node=node, keyspace_name=keyspace,
                                            table_name=column_family, suffix='-Scylla.db'):
            assert b'scylla_encryption_options' in Path(sstable).read_bytes()

        if Version(node.cluster.version()) >= Version('2023.2'):
            try:
                scylla_metadata = node.dump_sstable_scylla_metadata(
                    keyspace=keyspace,
                    column_family=column_family)

                assert all('scylla_encryption_options' in metadata.get('extension_attributes', {})
                           for table, metadata in scylla_metadata.items())
            except subprocess.CalledProcessError as exc:
                raise Exception(f"failed with : {exc.stderr}")

        else:
            json_path = tempfile.mktemp(suffix='.schema.json')
            try:
                with pytest.raises(ToolError, match='NullPointerException|ArrayIndexOutOfBoundsException'):
                    with open(json_path, 'w') as fdw:
                        node.run_sstable2json(out_file=fdw, keyspace='ks')
            finally:
                os.unlink(json_path)


def validate_sstables_clear(node, keyspace='ks', column_family='cf'):
    node.compact()
    with disable_autocompaction(node, keyspace_name=keyspace, table_name=column_family):
        for sstable in get_list_of_sstables(node=node, keyspace_name=keyspace,
                                            table_name=column_family, suffix='-Scylla.db'):
            assert b'scylla_encryption_options' not in Path(sstable).read_bytes()

        if Version(node.cluster.version()) >= Version('2023.2'):
            try:
                scylla_metadata = node.dump_sstable_scylla_metadata(
                    keyspace=keyspace,
                    column_family=column_family)
                assert all('scylla_encryption_options' not in metadata.get('extension_attributes', {}) for table, metadata in
                           scylla_metadata.items())
            except subprocess.CalledProcessError as exc:
                raise Exception("sstable couldn't be read, and it was expected to be clear") from exc
        else:
            json_path = tempfile.mktemp(suffix='.schema.json')
            try:
                with open(json_path, 'w') as fdw:
                    node.run_sstable2json(out_file=fdw, keyspace='ks')
                with open(json_path, 'r') as fdr:
                    data = fdr.read()
                if 'as the config file' in data:
                    # need to skip first line cause: https://github.com/scylladb/scylla-tools-java/issues/213
                    data = '\n'.join(data.split('\n')[1:])
                data_json = json.loads(data.replace("][", ","))

                assert data_json
            except ToolError as exc:
                raise Exception("sstable could be read, and it was expected to be clear") from exc
            finally:
                os.unlink(json_path)


class EncryptionAtRestBase(Tester):
    multiple_num = 3
    default_node_num = 2
    system_key_dir = './resources/system_keys/'

    def get_session(self, node_idx=0, user=None, password=None):
        node = self.cluster.nodelist()[node_idx]
        conn = self.patient_cql_connection(node, user=user, password=password)
        return conn

    def create_ks(self, kss=['ks'], n=None):
        n = n if n else self.default_node_num
        session = self.get_session()
        for ks in kss:
            session.execute(
                f"CREATE KEYSPACE IF NOT EXISTS {ks} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', "
                f"'replication_factor' : {n} }}")

    def prepare(self, n=None, kss=['ks'], restart=False):
        n = n if n else self.default_node_num
        self.cluster.set_configuration_options({'system_key_directory': EncryptionAtRestBase.system_key_dir})
        logger.debug('set system_key_directory to %s', EncryptionAtRestBase.system_key_dir)
        if not self.cluster.nodelist():
            self.cluster.populate(n).start(wait_for_binary_proto=True, wait_other_notice=True,
                                           jvm_args=['--logger-log-level', 'kms=trace'])
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

    def prepare_system_key(self, keyfile='system_key', cipher_algorithm='AES/CBC/PKCS5Padding', secret_key_strength=128):
        dest = os.path.join(EncryptionAtRestBase.system_key_dir, keyfile)
        # use saved key in dtest repo, generate it in future
        # the key can also be created by `dsetool createsystemkey $cipher_algorithm $strength`
        src = os.path.join(EncryptionAtRestBase.system_key_dir, 'system_key')  # AES/ECB/PKCS5Padding:128
        if not os.path.exists(dest) or not os.path.samefile(src, dest):
            shutil.copy(src, dest)

    def create_encrypted_cf(self, session, name='ks.cf', columns={'c1': 'text', 'c2': 'text'},
                            cipher_algorithm=None, secret_key_strength=None,
                            compression=None, additional_options={}):
        options = {}
        if additional_options:
            options.update(additional_options)
        if cipher_algorithm:
            options.update({'cipher_algorithm': cipher_algorithm})
        if secret_key_strength:
            options.update({'secret_key_strength': secret_key_strength})
        if 'system_key_file' in options:
            self.prepare_system_key(keyfile=options['system_key_file'])
        logger.debug("Create encrypted cf: %s (%s)", name, options)
        create_cf(session, name, columns=columns, scylla_encryption_options=options, compression=compression)
        return options

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
                node.start(wait_other_notice=True, wait_for_binary_proto=True,
                           jvm_args=['--logger-log-level', 'kms=trace'])
            except RuntimeError as e:
                if allow_start_failure:
                    errors.append(e)
                    pass
        if not errors:
            return self.get_session(user=user, password=password)

    def cluster_restart(self, user=None, password=None):
        logger.debug('Restart cluster ...')
        self.cluster.stop(wait_other_notice=True)
        self.cluster.start(wait_for_binary_proto=True, wait_other_notice=True,
                           jvm_args=['--logger-log-level', 'kms=trace'])
        return self.get_session(user=user, password=password)

    def get_key_provider(self, key_provider=None):
        if key_provider == KeyProviderEnum.local:
            ret = LocalFileSystemKeyProviderFactory(self)
        elif key_provider == KeyProviderEnum.replicated:
            ret = ReplicatedKeyProviderFactory(self)
        elif key_provider == KeyProviderEnum.kmip:
            ret = KmipKeyProviderFactory(self)
        elif key_provider == KeyProviderEnum.kms:
            ret = KMSKeyProviderFactory(self)
        elif key_provider == KeyProviderEnum.kms_real:
            ret = KMSRealKeyProviderFactory(self)
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

    def _smoke_test(self, key_provider=KeyProviderEnum.local, ciphers=None,
                    compression=None, exception_handler=None):
        with self.get_key_provider(key_provider) as kp:
            session = self.prepare(restart=kp.require_restart(), kss=[], n=self.default_node_num)
            cfs = []
            cfnum = 0
            try:
                self.create_ks(n=self.default_node_num)
                # to reduce test time, create one cf for every alg/len combo we test.
                # avoids rebooting cluster for every check.
                for cipher_algorithm, secret_key_strength in self.filter_ciphers(kp, ciphers):
                    try:
                        cf = 'cf' + str(cfnum)
                        cfnum += 1
                        self.create_encrypted_cf(session, name='ks.' + cf, cipher_algorithm=cipher_algorithm,
                                                 secret_key_strength=secret_key_strength, compression=compression,
                                                 additional_options=kp.additional_cf_options())
                        self.prepare_write_workload(session, cf=cf)
                        kp.verify_secret_key(cipher_algorithm, secret_key_strength)
                        cfs.append(cf)
                    except Exception as e:
                        if exception_handler:
                            exception_handler(e, cipher_algorithm, secret_key_strength)
                            continue
                        raise e

                # restart the cluster
                session = self.rolling_restart()
                for cf in cfs:
                    self.read_verify_workload(session, cf=cf)
                    self.drop_cf(name='ks.' + cf)
            finally:
                self.cleanup()

    def _upgrade_sstables(self):
        for node in self.cluster.nodelist():
            out, err = node.nodetool('upgradesstables -a')

    def _alter_test(self, key_provider=KeyProviderEnum.local):
        with self.get_key_provider(key_provider) as kp:
            session = self.prepare(restart=kp.require_restart())
            try:
                node1 = self.cluster.nodelist()[0]
                options = self.create_encrypted_cf(session, name='ks.cf', additional_options=kp.additional_cf_options())
                query = "ALTER TABLE ks.cf with scylla_encryption_options=%s"

                self.prepare_write_workload(session)
                if key_provider not in (KeyProviderEnum.replicated,):
                    validate_sstables_encrypted(node1)

                logger.debug('disable encryption at-rest')
                session.execute(query % "{'key_provider': 'none'}")
                table_desc = get_table_description(node1, "ks", "cf")
                assert "key_provider" not in table_desc, f"key_provider isn't disabled, schema:\n {table_desc}"
                self._upgrade_sstables()
                if key_provider not in (KeyProviderEnum.replicated,):
                    validate_sstables_clear(node1)
                session = self.rolling_restart()
                if key_provider not in (KeyProviderEnum.replicated,):
                    validate_sstables_clear(node1)
                self.read_verify_workload(session)

                logger.debug('re-enable encryption at-rest: %s' % options)
                session.execute(query % options)
                table_desc = get_table_description(node1, "ks", "cf")
                if key_provider == None:
                    assert "key_provider" not in table_desc, f"key_provider isn't unspecified, schema:\n {table_desc}"
                elif key_provider == KeyProviderEnum.kms_real:
                    err_msg = f"key_provider isn't changed to KmsKeyProviderFactory, schema: \n {table_desc}"
                    assert "'key_provider': 'KmsKeyProviderFactory'" in table_desc, err_msg
                else:
                    err_msg = f"key_provider isn't changed to {key_provider.value}, schema: \n {table_desc}"
                    assert f"'key_provider': '{key_provider.value}'" in table_desc, err_msg
                self._upgrade_sstables()
                if key_provider not in (KeyProviderEnum.replicated,):
                    validate_sstables_encrypted(node1)
                session = self.rolling_restart()
                if key_provider not in (KeyProviderEnum.replicated,):
                    validate_sstables_encrypted(node1)
                self.read_verify_workload(session)
            finally:
                self.cleanup()

    def _multiple_ks_test(self, key_provider=KeyProviderEnum.local):
        kss = ['mks_%s' % i for i in range(self.multiple_num)]
        with self.get_key_provider(key_provider) as kp:
            session = self.prepare(kss=kss, restart=kp.require_restart())
            try:
                for ks in kss:
                    self.create_encrypted_cf(session, name=ks + '.cf', additional_options=kp.additional_cf_options(ks))
                    self.prepare_write_workload(session, ks=ks)
                session = self.rolling_restart()
                for ks in kss:
                    self.read_verify_workload(session, ks=ks)
                return kss
            finally:
                self.cleanup(kss=kss)

    def _multiple_cf_test(self, key_provider=KeyProviderEnum.local):
        cfs = ['cf_%d' % i for i in range(self.multiple_num)]
        with self.get_key_provider(key_provider) as kp:
            session = self.prepare(restart=kp.require_restart())
            try:
                for cf in cfs:
                    self.create_encrypted_cf(session, name='ks.' + cf, additional_options=kp.additional_cf_options())
                    self.prepare_write_workload(session, cf=cf)
                session = self.rolling_restart()
                for cf in cfs:
                    self.read_verify_workload(session, cf=cf)
            finally:
                self.cleanup()

    def _reboot_test(self, key_provider=KeyProviderEnum.local):
        self.cluster.set_configuration_options({"commitlog_sync": "batch"})
        with self.get_key_provider(key_provider) as kp:
            self.prepare(n=3, restart=kp.require_restart())
            try:
                session = self.get_session()
                self.create_encrypted_cf(session, name='ks.cf', additional_options=kp.additional_cf_options())
                self.prepare_write_workload(session, flush=False)

                for node in self.cluster.nodelist()[1:]:
                    for i in range(3):
                        logger.debug('Kill node {}, and restart'.format(node.name))
                        node.stop(gently=False)
                        node.start(wait_for_binary_proto=True, wait_other_notice=False,
                                   jvm_args=['--logger-log-level', 'kms=trace'])
                    self.read_verify_workload(self.get_session())
            finally:
                self.cleanup()


def all_providers():
    return [pytest.param(p, marks=[unmark.next_gating] if p == KeyProviderEnum.kmip else []) for p in KeyProviderEnum]


@pytest.mark.dtest_full
@pytest.mark.dtest_enterprise
@pytest.mark.next_gating
class TestEncryptionAtRest(EncryptionAtRestBase):

    """
    # running specific case with parameter
    > pytest encryption_at_rest_test.py::TestEncryptionAtRest::test_multiple_ks[kmip]
    > pytest encryption_at_rest_test.py::TestEncryptionAtRest::test_encryption_table_compression[LZ4]
    # or all tests with kmip parameter
    > pytest encryption_at_rest_test.py::TestEncryptionAtRest -k kmip
    """
    default_node_num = 1

    @pytest.mark.parametrize(argnames='compression', argvalues=(None, 'LZ4', 'Snappy', 'Deflate'))
    def test_encryption_table_compression(self, compression):
        logger.debug('---- Test with compression: %s -----' % compression)
        self._smoke_test(key_provider=KeyProviderEnum.local, ciphers={
                         'AES/CBC/PKCS5Padding': [128]}, compression=compression)

    @pytest.mark.timeout(4700)
    @pytest.mark.single_node
    @pytest.mark.parametrize(argnames="key_provider", argvalues=all_providers(), ids=lambda x: x.name)
    def test_wrong_cipher_algorithm(self, key_provider):
        errors = []
        # TODO: Uncomment next line when issue https://github.com/scylladb/scylla-enterprise/issues/1973 will be resolve
        # unexpected_success = []

        broken_ciphers = {c: l for oc in supported_cipher_algorithms if oc
                          for l in [supported_cipher_algorithms[oc][:1]]
                          for a in ['Abc/', '/Abc', 'Abc']
                          for c in [oc + a, a + oc]
                          }

        def handler(e, cipher, length):
            try:
                raise e
            except (NoHostAvailable, ConfigurationException) as exc_details:
                error_message_to_str = str(exc_details)
                logger.debug(error_message_to_str)
                assert (f"Invalid algorithm string: {cipher}" in error_message_to_str
                        or (f"Invalid algorithm" in error_message_to_str and
                            cipher in error_message_to_str)
                        or 'Could not write key file' in error_message_to_str
                        or ('[Server error] message=' in error_message_to_str and 'abc' in error_message_to_str)
                        or 'non-supported padding option' in error_message_to_str
                        or 'routines::unsupported' in error_message_to_str), error_message_to_str
            except Exception as exc:
                errors.append((f"Unexpected exception: {exc}. "
                               f"Encryption option: 'key_provider': '{key_provider}', "
                               f"'cipher_algorithm': '{cipher}', "
                               f"'secret_key_strength': {length}"))
                logger.debug(errors[-1])

        self._smoke_test(key_provider=key_provider, ciphers=broken_ciphers, exception_handler=handler)

        # TODO: Uncomment next line when issue https://github.com/scylladb/scylla-enterprise/issues/1973 will be resolve
        # assert not unexpected_success, "Negative tests succeeded unexpectedly: %s" % '\n'.join(unexpected_success)

        assert not errors, errors

    @pytest.mark.timeout(4000)
    @pytest.mark.single_node
    @pytest.mark.parametrize(argnames="key_provider", argvalues=all_providers(), ids=lambda x: x.name)
    def test_supported_cipher_algorithms(self, key_provider):
        errors = []

        def handler(e, cipher, length):
            logger.debug(str(e))
            errors.append(f"Test with configuration '{cipher}', length {length}, "
                          f"key provider {key_provider}' failed. Error {e}")

        self._smoke_test(key_provider=key_provider, ciphers=supported_cipher_algorithms, exception_handler=handler)

        assert len(errors) == 0, errors

    @pytest.mark.single_node
    @pytest.mark.parametrize(argnames="key_provider", argvalues=all_providers(), ids=lambda x: x.name)
    def test_abbreviated_supported_cipher_algorithms(self, key_provider):
        errors = []
        abbreviated = {c: l for c in supported_cipher_algorithms if c
                       for l in [supported_cipher_algorithms[c][:1]]
                       }

        def handler(e, cipher, length):
            logger.debug(str(e))
            errors.append(f"Test with configuration '{cipher}', length {length}, "
                          f"key provider {key_provider}' failed. Error {e}")

        self._smoke_test(key_provider=key_provider, ciphers=abbreviated, exception_handler=handler)

        assert len(errors) == 0, errors

    @pytest.mark.single_node
    @pytest.mark.parametrize(argnames="key_provider", argvalues=all_providers(), ids=lambda x: x.name)
    def test_multiple_ks(self, key_provider):
        self._multiple_ks_test(key_provider=key_provider)

    @pytest.mark.single_node
    @pytest.mark.parametrize(argnames="key_provider", argvalues=all_providers(), ids=lambda x: x.name)
    def test_multiple_cf(self, key_provider):
        self._multiple_cf_test(key_provider=key_provider)

    @pytest.mark.parametrize(argnames="key_provider", argvalues=[pytest.param(p, marks=[pytest.mark.require("scylladb/scylla-enterprise#4067")] if p.name in ("replicated", "kmip") else []) for p in KeyProviderEnum], ids=lambda x: x.name)
    @pytest.mark.no_boot_speedups
    def test_reboot(self, key_provider):
        self._reboot_test(key_provider=key_provider)

    @pytest.mark.single_node
    @unmark.next_gating
    @pytest.mark.parametrize(argnames="key_provider", argvalues=all_providers(), ids=lambda x: x.name)
    def test_alter(self, key_provider):
        self._alter_test(key_provider=key_provider)


@pytest.mark.dtest_full
@pytest.mark.dtest_enterprise
@pytest.mark.next_gating
class TestKMSEncryption(EncryptionAtRestBase):
    def test_per_table_master_key(self):
        with KMSKeyProviderFactory(self) as kp:
            session = self.prepare()
            kms_client = kp.connection()
            try:
                self.create_ks()
                cfs = []
                for ki in [2, 3]:
                    response = kms_client.create_key(Description='dtest-' + str(ki),
                                                     Tags=[{
                                                         'TagKey': 'Name',
                                                         'TagValue': 'dtest-' + str(ki)
                                                     }])
                    key_id = response['KeyMetadata']['KeyId']
                    alias = "alias/test_per_table_master_key-" + str(ki)
                    kms_client.create_alias(AliasName=alias, TargetKeyId=key_id)
                    cf = 'cf' + str(ki)
                    opts = kp.additional_cf_options() | {'master_key': alias}
                    self.create_encrypted_cf(
                        session, name='ks.' + cf, cipher_algorithm='AES/CBC/PKCS5Padding', secret_key_strength=128, additional_options=opts)
                    self.prepare_write_workload(session, cf=cf)
                    cfs.append(cf)

                    # restart the cluster

                session = self.rolling_restart()

                # todo: how can we verify data is actually encrypted with specific key? Aws
                # does not allow key deletion, and our key ids do not reference aliases once
                # the are used.
                for cf in cfs:
                    self.read_verify_workload(session, cf=cf)
                    self.drop_cf(name='ks.' + cf)

            finally:
                self.cleanup()

    def test_per_non_existing_table_master_key(self):
        with KMSKeyProviderFactory(self) as kp:
            session = self.prepare()
            kms_client = kp.connection()
            try:
                self.create_ks()
                alias = "alias/does-not-exist"
                opts = kp.additional_cf_options() | {'master_key': alias}
                with pytest.raises(Exception):
                    self.create_encrypted_cf(
                        session, name='ks.failme', cipher_algorithm='AES/CBC/PKCS5Padding', secret_key_strength=128, additional_options=opts)
            finally:
                self.cleanup()


@pytest.mark.dtest_full
@pytest.mark.dtest_enterprise
@pytest.mark.next_gating
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
        self.cluster.stop()

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

    @unmark.next_gating
    def test_system_auth_encryption(self, key_provider=KeyProviderEnum.local):
        options = {'authenticator': 'org.apache.cassandra.auth.PasswordAuthenticator',
                   'authorizer': 'org.apache.cassandra.auth.CassandraAuthorizer'
                   }
        self.cluster.set_configuration_options(options)
        self.cluster.populate(3).start(wait_for_binary_proto=True, wait_other_notice=True,
                                       jvm_args=['--logger-log-level', 'kms=trace'])
        wait_for_any_log(self.cluster.nodelist(), 'Created default superuser', 10)
        node1 = self.cluster.nodelist()[0]
        session = self.patient_cql_connection(node1, user='cassandra', password='cassandra')
        logger.debug('Set RF of system_auth to 3')
        session.execute("ALTER KEYSPACE system_auth "
                        "WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};")
        self.cluster.repair()

        self.verify_system_info(session, None, ks_suffix="orig", expect=True)

        options = {"system_info_encryption": {"enabled": True, "key_provider": "LocalFileSystemKeyProviderFactory"},
                   "system_key_directory": EncryptionAtRestBase.system_key_dir}
        self.cluster.set_configuration_options(options)
        logger.debug("\n\nRestarting nodes one by one ...... Make sure encryption change is persistent\n")
        session = self.rolling_restart(user="cassandra", password="cassandra")
        logger.debug("Re-verify system info after system_info_encryption is enabled")
        self.verify_system_info(session, None, ks_suffix="encrypt", expect=False)

    @unmark.next_gating
    @pytest.mark.no_boot_speedups
    def test_reboot(self):
        """
        The test reproduces a scylla crash, with enabled commitlog encryption, and reboot.
        https://github.com/scylladb/scylla-enterprise/issues/1332
        """

        self.prepare(n=3, restart=False)
        options = {"system_info_encryption": {"enabled": True,
                                              "key_provider": "LocalFileSystemKeyProviderFactory"}, "commitlog_sync": "batch"}
        self.cluster.set_configuration_options(options)
        logger.debug("\n\nRestarting nodes one by one ...... Make sure encryption change is persistent\n")
        session = self.rolling_restart()

        self.create_encrypted_cf(session, name="ks.cf")
        self.prepare_write_workload(session, flush=False)

        # restarting nodes once is "enough". Since commit log is replayed
        # on first kill+start, unless we add more data, subsequent restarts
        # would not add anything
        for node in self.cluster.nodelist()[1:]:
            logger.debug(f"Kill node {node.name}, and restart")
            node.stop(gently=False, wait_other_notice=False)
            node.start(wait_for_binary_proto=True, wait_other_notice=True, jvm_args=["--logger-log-level", "kms=trace"])
            self.read_verify_workload(self.get_session())
