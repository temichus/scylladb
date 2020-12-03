import os
import time
import docker
from ldap3 import Server, Connection, ALL, ALL_ATTRIBUTES
from dtest import debug
from dtest import retrying
from ldap3.core.exceptions import LDAPSessionTerminatedByServerError
from ldap3.core.exceptions import LDAPSocketOpenError


def running_in_docker():
    path = '/proc/self/cgroup'
    with open(path) as cgroup:
        return (
            os.path.exists('/.dockerenv') or
            os.path.isfile(path) and any('docker' in line for line in cgroup)
        )


class ContainerAlreadyStarted(Exception):
    pass


class ContainerDoesNotExist(Exception):
    pass


class LdapConnectionAlreadyStarted(Exception):
    pass


class LdapConnectionDoesNotExist(Exception):
    pass

class LdapServerNotReady(Exception):
    pass

# If the server has terminated the connection lets try to rebuild it
# once.
def try_and_recreate_connetion(func):
    def inner(*args, **kwargs):
        try:
            try:
                return func(*args, **kwargs)
            except LDAPSessionTerminatedByServerError:
                args[0].create_ldap_connection()
                return func(*args, **kwargs)
        except:
            args[0].container.reload()
            debug("LDAP Container ({}) status:{}".format(args[0].container.name, args[0].container.status))
            debug(f"LDAP SERVER LOG DUMP: {args[0].container.logs().decode('utf-8')}")
            raise
    return inner


class LdapDocker(object):
    def __init__(self):
        self.docker = docker.from_env()
        self.name = None
        self.ldap_port = None
        self.ldap_ssl_port = None
        self.container = None
        self.conn = None
        self.ldap_server = None
        self.ldap_base_object = None
        self.ldap_address = None

    def create_ldap_container(self, name, ldap_port=0, ldap_ssl_port=0, image='osixia/openldap:1.4.0',
                              organisation='ScyllaDB', domain='scylladb.com', password='scylla'):
        if self.container:
            raise ContainerAlreadyStarted('LDAP docker already exists for this instance')
        self.name = name
        self.docker.containers.run(ports={'389/tcp': ldap_port, '636/tcp': ldap_ssl_port},
                                   name=name,
                                   environment=[f'LDAP_ORGANISATION={organisation}', f'LDAP_DOMAIN={domain}',
                                                f'LDAP_ADMIN_PASSWORD={password}'],
                                   image=image,
                                   command="--loglevel trace",
                                   detach=True,
                                   labels=['dtest'])
        for container in self.docker.containers.list():
            if self.name in container.name:
                self.container = container
                if running_in_docker():
                    self.ldap_port = '389'
                    self.ldap_ssl_port = '636'
                    self.ldap_address = container.attrs['NetworkSettings']['IPAddress']
                else:
                    self.ldap_port = container.ports['389/tcp'][0]['HostPort']
                    self.ldap_ssl_port = container.ports['636/tcp'][0]['HostPort']
                    self.ldap_address = 'localhost'
        if self.container:
            # We try to wait here for the startup, if the server haven't finished
            # after 30s we will continue with the wishfull thinking that by the time
            # scylla will try to connect to it, it will be up and running.
            # if it will not happen, the test will fail and we will need to increase
            # the timeout. But it is better than failing early.
            # for creating connections to the server we are covered since the connection
            # creation function also waits for the server to be up.
            try:
                self.wait_for_ldap_server_startup()
            except:
                pass

    def is_container_running(self):
        if not self.container:
            raise ContainerDoesNotExist('LDAP docker does not exists for this instance')
        return 'running' in self.container.status

    def remove_container(self, force=True):
        if not self.container:
            raise ContainerDoesNotExist('LDAP docker does not exists for this instance')
        if self.conn:
            self.disconnect_ldap()
        self.container.remove(force=force)
        self.container = None

    def wait_for_ldap_server_startup(self, timeout=30):
        if self.container.exec_run(f"timeout {timeout}s container/tool/wait-process")[0] != 0:
            raise LdapServerNotReady("LDAP server didn't finish its startup yet...")

    @retrying(num_attempts=5, sleep_time=2, allowed_exceptions=(LDAPSocketOpenError, LdapServerNotReady),
              message='Trying to create LDAP connection')
    def create_ldap_connection(self, user='cn=admin,dc=scylladb,dc=com', password='scylla'):
        self.wait_for_ldap_server_startup(5)
        if not self.ldap_server:
            self.ldap_server = Server(host=f'ldap://{self.ldap_address}:{self.ldap_port}', get_info=ALL)
        if not self.conn:
            self.conn = Connection(server=self.ldap_server, user=user, password=password)
        self.conn.bind()
        self.ldap_base_object = self.ldap_server.info.naming_contexts[0]

    def is_ldap_connection_bound(self):
        return self.conn.bound

    def disconnect_ldap(self):
        if not self.conn:
            raise LdapConnectionDoesNotExist('LDAP connection does not exist for this instance')
        self.conn.unbind()
        self.conn = None

    @try_and_recreate_connetion
    def add_ldap_object(self, *args, **kwargs):
        self.conn.add(*args, **kwargs)
        return self.conn.result

    @try_and_recreate_connetion
    def search_ldap_object(self, search_base, search_filter):
        self.conn.search(search_base=search_base, search_filter=search_filter, attributes=ALL_ATTRIBUTES)
        return self.conn.entries

    @try_and_recreate_connetion
    def modify_ldap_object(self, *args, **kwargs):
        return self.conn.modify(*args, **kwargs)
