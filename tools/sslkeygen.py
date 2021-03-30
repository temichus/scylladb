import logging
import os
import os.path
import tempfile
import subprocess
import warnings
from socket import gethostname

from OpenSSL import crypto

logger = logging.getLogger(__name__)


def generate_credentials(ip, cakeystore=None, cacert=None):

    tmpdir = tempfile.mkdtemp()

    if not cakeystore:
        cakeystore = generate_cakeypair(tmpdir, 'ca')
    if not cacert:
        cacert = generate_cert(tmpdir, "ca", cakeystore)

    # create keystore with new private key
    name = "ip" + ip
    jkeystore = generate_ipkeypair(tmpdir, name, ip)

    # create signed cert
    csr = generate_sign_request(tmpdir, name, jkeystore, ['-ext', 'san=ip:' + ip])
    cert = sign_request(tmpdir, "ca", cakeystore, csr, ['-ext', 'san=ip:' + ip])

    # import cert chain into keystore
    import_cert(tmpdir, "ca", cacert, jkeystore)
    import_cert(tmpdir, name, cert, jkeystore)

    return SecurityCredentials(jkeystore, cert, cakeystore, cacert)


def generate_cakeypair(dir, name):
    return generate_keypair(dir, name, name, ['-ext', 'bc:c'])


def generate_ipkeypair(dir, name, ip):
    return generate_keypair(dir, name, ip, ['-ext', 'san=ip:' + ip])


def generate_dnskeypair(dir, name, hostname):
    return generate_keypair(dir, name, hostname, ['-ext', 'san=dns:' + hostname])


def generate_keypair(dir, name, cn, opts):
    kspath = os.path.join(dir, name + '.keystore')
    return _exec_keytool(dir, kspath, ['-alias', name, '-genkeypair', '-keyalg', 'RSA', '-dname',
                                       "cn={}, ou=cassandra, o=apache.org, c=US".format(cn), '-keypass', 'cassandra'] + opts)


def generate_cert(dir, name, keystore, opts=[]):
    fn = os.path.join(dir, name + '.pem')
    _exec_keytool(dir, keystore, ['-alias', name, '-exportcert', '-rfc', '-file', fn] + opts)
    return fn


def generate_sign_request(dir, name, keystore, opts=[]):
    fn = os.path.join(dir, name + '.csr')
    _exec_keytool(dir, keystore, ['-alias', name, '-keypass', 'cassandra', '-certreq', '-file', fn] + opts)
    return fn


def sign_request(dir, name, keystore, csr, opts=[]):
    fnout = os.path.splitext(csr)[0] + '.pem'
    _exec_keytool(dir, keystore, ['-alias', name, '-keypass', 'cassandra', '-gencert',
                                  '-rfc', '-infile', csr, '-outfile', fnout] + opts)
    return fnout


def import_cert(dir, name, cert, keystore, opts=[]):
    _exec_keytool(dir, keystore, ['-alias', name, '-keypass', 'cassandra',
                                  '-importcert', '-noprompt', '-file', cert] + opts)
    return cert


def _exec_keytool(dir, keystore, opts):
    args = ['keytool', '-keystore', keystore, '-storepass', 'cassandra', '-deststoretype', 'pkcs12'] + opts
    subprocess.check_call(args)
    return keystore


def wait_for_cert_reload(node, module, files, from_mark=None):
    for f in files:
        node.watch_log_for("^.*{}.*Reloaded.*{}\.*".format(module, f.replace('.', '\.')), from_mark=from_mark)


class SecurityCredentials():

    def __init__(self, keystore, cert, cakeystore, cacert):
        self.keystore = keystore
        self.cert = cert
        self.cakeystore = cakeystore
        self.cacert = cacert
        self.basedir = os.path.dirname(self.keystore)

    def __str__(self):
        return "keystore: {}, cert: {}, cakeystore: {}, cacert: {}".format(
               self.keystore, self.cert, self.cakeystore, self.cacert)


def create_self_signed_x509_certificate(test_path, cert_file='scylla.crt', key_file='scylla.key'):
    cert_file = os.path.join(test_path, cert_file)
    key_file = os.path.join(test_path, key_file)

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
    logger.debug(f'Created certificate file in "{cert_file}" path, and private key in "{key_file}" path')
    return cert_file, key_file
