import logging
from dataclasses import dataclass, field, fields

logger = logging.getLogger(__name__)


DEFAULT_SERVICE_LEVEL_SHARES = 1000


def sla_to_dict(sla_result):
    # Result example: <type 'list'>: [Row(service_level=u'sla1', shares=1)]
    sla_list = []
    for row in sla_result:
        sla_list.append([row.service_level, row.shares])
    return sla_list


def role_to_dict(sla_result):
    # Result example: <type 'list'>: [Row(role=u'role1', service_level=u'sla1')]
    sla_list = []
    for row in sla_result:
        sla_list.append([row.role, row.service_level])
    return sla_list


@dataclass
class ServiceLevelAttributes:
    shares: int = None
    timeout: str = None
    workload_type: str = None
    query_string: str = field(init=False, repr=False)

    def __setattr__(self, key, value):
        super().__setattr__(key, value)
        if key != "query_string":
            self._generate_query_string()

    def __post_init__(self):
        self._generate_query_string()

    def _generate_query_string(self):
        attr_strings = []

        for item in fields(self):
            value = getattr(self, item.name) if item.repr else None
            if value is not None:
                if item.type is str:
                    attr_strings.append(f" AND {item.name} = '{value}'")
                else:
                    attr_strings.append(f" AND {item.name} = {value}")
        if attr_strings:
            attr_strings[0] = attr_strings[0].replace(" AND", " WITH")
        else:
            self.query_string = ""
            return

        if len(attr_strings) > 1:
            self.query_string = "".join(attr_strings)
        else:
            self.query_string = attr_strings[0]


class ServiceLevel(object):
    # The class provide interface to manage SERVICE LEVEL
    def __init__(self, session,
                 name: str,
                 service_shares: int = 1000,
                 timeout: str = None,
                 workload_type: str = None, verbose=True):
        self.session = session
        self._name = name
        self.verbose = verbose
        self._created = False
        self._sl_attributes = ServiceLevelAttributes(
            shares=service_shares,
            timeout=timeout,
            workload_type=workload_type
        )

    @property
    def name(self) -> str:
        return self._name

    @name.setter
    def name(self, name):
        self._name = name

    @property
    def service_shares(self) -> int:
        return self._sl_attributes.shares

    @service_shares.setter
    def service_shares(self, service_level_shares):
        self._sl_attributes.shares = service_level_shares

    @property
    def created(self) -> bool:
        return self.created

    @created.setter
    def created(self, created: bool):
        self._created = created

    @property
    def timeout(self) -> str:
        return self._sl_attributes.timeout

    @timeout.setter
    def timeout(self, timeout: int):
        self._sl_attributes.timeout = timeout

    @property
    def workload_type(self) -> str:
        return self._sl_attributes.workload_type

    @workload_type.setter
    def workload_type(self, workload_type: str):
        self._sl_attributes.workload_type = workload_type

    def create(self, if_not_exists=True):
        query = 'CREATE SERVICE_LEVEL{if_not_exists} {service_level_name}{query_attributes}'\
                .format(if_not_exists=' IF NOT EXISTS' if if_not_exists else '',
                        service_level_name=self.name,
                        query_attributes=self._sl_attributes.query_string)
        if self.verbose:
            logger.debug('Create service level query: {}'.format(query))
        self.session.execute(query)
        logger.debug('Service level "{}" has been created'.format(self.name))
        self.created = True

    def alter(self, new_shares: int = None, new_timeout: str = None, new_workload_type: str = None):
        sla = ServiceLevelAttributes(shares=new_shares, timeout=new_timeout, workload_type=new_workload_type)
        query = 'ALTER SERVICE_LEVEL {service_level_name} {query_string}'\
                .format(service_level_name=self.name,
                        query_string=sla.query_string)
        if self.verbose:
            logger.debug('Change service level query: {}'.format(query))
        self.session.execute(query)
        logger.debug('Service level "{}" has been altered'.format(self.name))
        self.service_shares = new_shares

    def drop(self, if_exists=True):
        query = 'DROP SERVICE_LEVEL{if_exists} {service_level_name}'\
                .format(service_level_name=self.name,
                        if_exists=' IF EXISTS' if if_exists else '')
        if self.verbose:
            logger.debug('Drop service level query: {}'.format(query))
        self.session.execute(query)
        logger.debug('Service level "{}" has been dropped'.format(self.name))
        self.created = False

    def list_service_level(self):
        query = 'LIST SERVICE_LEVEL {}'.format(self.name)
        if self.verbose:
            logger.debug('List service level query: {}'.format(query))
        res = list(self.session.execute(query))
        return sla_to_dict(res)

    def list_all_service_levels(self):
        query = 'LIST ALL SERVICE_LEVELS'
        if self.verbose:
            logger.debug('List all service levels query: {}'.format(query))
        res = list(self.session.execute(query))
        return sla_to_dict(res)


class UserRoleBase(object):
    # Base class for ROLES and USERS
    AUTHENTICATION_ENTITY = ''

    def __init__(self, session, name, password=None, superuser=None, verbose=False, **kwargs):
        self._name = name
        self.password = password
        self.session = session
        self.superuser = superuser
        self.verbose = verbose
        self._attached_service_level_name = ''
        self._attached_service_level_shares = None

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, name):
        self._name = name

    @property
    def attached_service_level_name(self):
        return self._attached_service_level_name

    @attached_service_level_name.setter
    def attached_service_level_name(self, service_level_name):
        self._attached_service_level_name = service_level_name

    @property
    def attached_service_level_shares(self):
        return self._attached_service_level_shares

    @attached_service_level_shares.setter
    def attached_service_level_shares(self, service_level_shares):
        self._attached_service_level_shares = service_level_shares

    def attach_service_level(self, service_level):
        """
        :param auth_name: it may be role name or user name
        """
        query = 'ATTACH SERVICE_LEVEL {service_level_name} TO {role_name}'\
                .format(service_level_name=service_level.name,
                        role_name=self.name)
        if self.verbose:
            logger.debug('Attach service level query: {}'.format(query))
        self.session.execute(query)
        logger.debug('Service level "{}" has been attached to {} role'.format(service_level.name, self.name))
        self.attached_service_level_name = service_level.name
        self.attached_service_level_shares = service_level.service_shares \
            if service_level.service_shares \
            else DEFAULT_SERVICE_LEVEL_SHARES

    def detach_service_level(self):
        """
        :param auth_name: it may be role name or user name
        """
        query = 'DETACH SERVICE_LEVEL FROM {role_name}'\
                .format(role_name=self.name)
        if self.verbose:
            logger.debug('Detach service level query: {}'.format(query))
        self.session.execute(query)
        logger.debug('The service level has been detached from {} role'.format(self.name))

        self.attached_service_level_name = ''
        self.attached_service_level_shares = None

    def grant_me_to(self, grant_to):
        role_be_granted = self.name
        grant_to = grant_to.name
        query = 'GRANT {role_be_granted} to {grant_to}'.format(**locals())
        if self.verbose:
            logger.debug('GRANT role query: {}'.format(query))
        self.session.execute(query)
        logger.debug('Role "{role_be_granted}" has been granted to {grant_to}'.format(**locals()))

    def revoke_me_from(self, revoke_from):
        role_be_revoked = self.name
        role_revokes_from = revoke_from.name
        query = 'REVOKE ROLE {role_be_revoked} FROM {role_revokes_from}'.format(**locals())
        if self.verbose:
            logger.debug('REVOKE role query: {}'.format(query))
        self.session.execute(query)
        logger.debug('Role "{role_be_revoked}" has been revocked from {role_revokes_from}'.format(**locals()))

    def attach_another_sla_to_role(self, service_level):
        self.detach_service_level()
        self.attach_service_level(service_level=service_level)

    def list_user_role_attached_service_levels(self):
        query = 'LIST ATTACHED SERVICE_LEVEL OF {}'.format(self.name)
        if self.verbose:
            logger.debug('List attached service level(s) query: {}'.format(query))
        res = list(self.session.execute(query))
        return role_to_dict(res)

    def list_all_attached_service_levels(self):
        query = 'LIST ATTACHED ALL SERVICE_LEVELS'
        if self.verbose:
            logger.debug('List attached service level(s) query: {}'.format(query))
        res = list(self.session.execute(query))
        return sla_to_dict(res)

    def list_effective_service_levels(self, auth_obj):
        query = 'LIST SERVICE_LEVELS OF {}'.format(auth_obj.name)
        if self.verbose:
            logger.debug('List effective service levels query: {}'.format(query))
        res = list(self.session.execute(query))
        return sla_to_dict(res)

    def drop(self, if_exists=True):
        query = 'DROP {entity}{if_exists} {name}'.format(entity=self.AUTHENTICATION_ENTITY,
                                                         name=self.name,
                                                         if_exists=' IF EXISTS' if if_exists else '')
        if self.verbose:
            logger.debug('Drop {entity} query: {query}'.format(entity=self.AUTHENTICATION_ENTITY, query=query))

        self.session.execute(query)
        logger.debug('{entity} "{name}" has been dropped'.format(entity=self.AUTHENTICATION_ENTITY, name=self.name))
        self.role_name = ''


class Role(UserRoleBase):
    # The class provide interface to manage ROLES
    AUTHENTICATION_ENTITY = 'ROLE'

    def __init__(self, session, name, password=None, login=False, superuser=False, options_dict=None, verbose=True):
        super(Role, self).__init__(session, name, password, superuser, verbose)
        self.login = login
        self.options_dict = options_dict

    def create(self):
        # Example: CREATE ROLE bob WITH PASSWORD = 'password_b'AND LOGIN = true AND SUPERUSER = true;
        # Example: CREATE ROLE carlos WITH OPTIONS = {'custom_option1': 'option1_value', 'custom_option2': 99};
        role_options = {}
        for opt in ['password', 'login', 'superuser', 'options_dict']:
            if hasattr(self, opt):
                value = getattr(self, opt)
                if value:
                    role_options[opt.replace('_dict', '')] = '\'{}\''.format(value) \
                        if opt == 'password' else value
        role_options_str = ' AND '.join(['{} = {}'.format(opt, val) for opt, val in role_options.items()])
        if role_options_str:
            role_options_str = ' WITH {}'.format(role_options_str)

        query = 'CREATE ROLE {name}{role_options_str}'.format(name=self.name, role_options_str=role_options_str)
        if self.verbose:
            logger.debug('CREATE role query: {}'.format(query))
        self.session.execute(query)
        logger.debug('Role "{}" has been created'.format(self.name))


class User(UserRoleBase):
    # The class provide interface to manage USERS
    AUTHENTICATION_ENTITY = 'USER'

    def __init__(self, session, name, password=None, superuser=None, verbose=True):
        super(User, self).__init__(session, name, password, superuser, verbose)

    def create(self):
        user_options_str = '{password}{superuser}'.format(password=' PASSWORD \'{}\''
                                                          .format(self.password) if self.password else '',
                                                          superuser='' if self.superuser is None else ' SUPERUSER'
                                                          if self.superuser else ' NOSUPERUSER')
        if user_options_str:
            user_options_str = ' WITH {}'.format(user_options_str)

        query = 'CREATE USER {name}{user_options_str}'.format(name=self.name, user_options_str=user_options_str)
        if self.verbose:
            logger.debug('Create user query: {}'.format(query))

        self.session.execute(query)
        logger.debug('User "{}" has been created'.format(self.name))
