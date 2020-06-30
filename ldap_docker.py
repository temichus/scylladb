import time
import docker
from ldap3 import Server, Connection, ALL, ALL_ATTRIBUTES


class ContainerAlreadyStarted(Exception):
    pass


class ContainerDoesNotExist(Exception):
    pass


class LdapConnectionAlreadyStarted(Exception):
    pass


class LdapConnectionDoesNotExist(Exception):
    pass


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
                                   detach=True,
                                   labels=['dtest'])
        for container in self.docker.containers.list():
            if self.name in container.name:
                self.container = container
                self.ldap_port = container.ports['389/tcp'][0]['HostPort']
                self.ldap_ssl_port = container.ports['636/tcp'][0]['HostPort']

    def is_container_running(self):
        if not self.container:
            raise ContainerDoesNotExist('LDAP docker does not exists for this instance')
        return 'running' in self.container.status

    def remove_container(self, force=True):
        if not self.container:
            raise ContainerDoesNotExist('LDAP docker does not exists for this instance')
        self.container.remove(force=force)
        self.container = None
        if self.conn:
            self.disconnect_ldap()

    def create_ldap_connection(self, user='cn=admin,dc=scylladb,dc=com', password='scylla', ip='localhost'):
        self.ldap_server = Server(host=f'ldap://{ip}:{self.ldap_port}', get_info=ALL)
        self.conn = Connection(server=self.ldap_server, user=user, password=password)
        time.sleep(3)
        self.conn.open()
        self.conn.bind()
        self.ldap_base_object = self.ldap_server.info.naming_contexts[0]

    def is_ldap_connection_bound(self):
        return self.conn.bound

    def disconnect_ldap(self):
        if not self.conn:
            raise LdapConnectionDoesNotExist('LDAP connection does not exist for this instance')
        self.conn.unbind()
        self.conn = None

    def add_ldap_object(self, *args, **kwargs):
        self.conn.add(*args, **kwargs)
        return self.conn.result

    def search_ldap_object(self, search_base, search_filter):
        self.conn.search(search_base=search_base, search_filter=search_filter, attributes=ALL_ATTRIBUTES)
        return self.conn.entries

    def modify_ldap_object(self, *args, **kwargs):
        return self.conn.modify(*args, **kwargs)
