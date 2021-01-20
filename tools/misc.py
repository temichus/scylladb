import os
import subprocess
import time
import hashlib
import logging
import pytest
import errno

from collections.abc import Mapping


logger = logging.getLogger(__name__)


# work for cluster started by populate
def new_node(cluster, bootstrap=True, token=None, remote_debug_port='0', data_center=None, ipformat=None, use_single_interface=False):
    i = len(cluster.nodes) + 1

    if cluster.ipprefix:
        ipprefix = cluster.ipprefix

    if not ipformat:
        ipformat = ipprefix + "%d"

    if cluster.cassandra_version() >= '1.2':
        if use_single_interface:
            # Always leave 9042 and 9043 clear, in case someone defaults to adding
            # a node with those ports
            binary = (ipformat % 1, 9042 + 2 + (i * 2))
        else:
            binary = (ipformat % i, 9042)

    thrift = None
    if cluster.cassandra_version() < '4':
        thrift = (ipformat % i, 9160)

    storage_interface = ((ipformat % i), 7000)

    node = cluster.create_node(name='node%s' % i,
                               auto_bootstrap=bootstrap,
                               thrift_interface=thrift,
                               storage_interface=storage_interface,
                               jmx_port=str(7000 + i * 100 + cluster.id),
                               remote_debug_port=remote_debug_port,
                               initial_token=token,
                               binary_interface=binary,
                               )
    cluster.add(node, not bootstrap, data_center=data_center)
    return node


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


def is_port_used(port: int, service_name: str) -> bool:
    """
        Path to `ss' is /usr/sbin/ss for RHEL-like distros and /bin/ss for Debian-based.  Unfortunately,
        /usr/sbin is not always in $PATH, so need to set it explicitly.

        Output of `ss -ln' command in case of used port:
          $ ss -ln '( sport = :8000 )'
          Netid State      Recv-Q Send-Q     Local Address:Port                    Peer Address:Port
          tcp   LISTEN     0      5                      *:8000                               *:*

        And if there are no processes listening on the port:
          $ ss -ln '( sport = :8001 )'
          Netid State      Recv-Q Send-Q     Local Address:Port                    Peer Address:Port

        Can't avoid the header by using `-H' option because of ss' core on Ubuntu 18.04.
    """
    try:
        cmd = f"PATH=/bin:/usr/sbin ss -ln '( sport = :{port} )'"
        return len(subprocess.run(cmd, shell=True, capture_output=True, text=True).stdout.splitlines()) > 1
    except Exception as details:  # pylint: disable=broad-except
        logger.debug(f"Error checking for '{service_name}' on port {port}: {details}")
        return False


def list_to_hashed_dict(list):
    """
    takes a list and hashes the contents and puts them into a dict so the contents can be compared
    without order. unfortunately, we need to do a little massaging of our input; the result from
    the driver can return a OrderedMapSerializedKey (e.g. [0, 9, OrderedMapSerializedKey([(10, 11)])])
    but our "expected" list is simply a list of elements (or list of list). this means if we
    hash the values as is we'll get different results. to avoid this, when we see a dict,
    convert the raw values (key, value) into a list and insert that list into a new list
    :param list the list to convert into a dict
    :return: a dict containing the contents fo the list with the hashed contents
    """
    hashed_dict = dict()
    for item_lst in list:
        normalized_list = []
        for item in item_lst:
            if hasattr(item, "items"):
                tmp_list = []
                for a, b in item.items():
                    tmp_list.append(a)
                    tmp_list.append(b)
                normalized_list.append(tmp_list)
            else:
                normalized_list.append(item)
        list_str = str(normalized_list)
        utf8 = list_str.encode('utf-8', 'ignore')
        list_digest = hashlib.sha256(utf8).hexdigest()
        hashed_dict[list_digest] = normalized_list
    return hashed_dict


def get_current_test_name():
    """
    See https://docs.pytest.org/en/latest/example/simple.html#pytest-current-test-environment-variable
    :return: returns just the name of the current running test name
    """
    pytest_current_test = os.environ.get('PYTEST_CURRENT_TEST')
    test_splits = pytest_current_test.split("::")
    current_test_name = test_splits[len(test_splits) - 1]
    current_test_name = current_test_name.replace(" (call)", "")
    current_test_name = current_test_name.replace(" (setup)", "")
    current_test_name = current_test_name.replace(" (teardown)", "")
    return current_test_name


class ImmutableMapping(Mapping):
    """
    Convenience class for when you want an immutable-ish map.

    Useful at class level to prevent mutability problems (such as a method altering the class level mutable)
    """

    def __init__(self, init_dict):
        self._data = init_dict.copy()

    def __getitem__(self, key):
        return self._data[key]

    def __iter__(self):
        return iter(self._data)

    def __len__(self):
        return len(self._data)

    def __repr__(self):
        return '{cls}({data})'.format(cls=self.__class__.__name__, data=self._data)


def wait_for_agreement(thrift, timeout=10):
    def check_agreement():
        schemas = thrift.describe_schema_versions()
        if len([ss for ss in list(schemas.keys()) if ss != 'UNREACHABLE']) > 1:
            raise Exception("schema agreement not reached")
    retry_till_success(check_agreement, timeout=timeout)


def add_skip(cls, reason=""):
    if hasattr(cls, "pytestmark"):
        cls.pytestmark = cls.pytestmark.copy()
        cls.pytestmark.append(pytest.mark.skip(reason))
    else:
        cls.pytestmark = [pytest.mark.skip(reason)]
    return cls
