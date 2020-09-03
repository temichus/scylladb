# coding: utf-8
# ccm clusters
import os

import time
import yaml
from enum import Enum
from re import findall

from ccmlib import common
from scrub_test import TestHelper
from dtest import warning, debug, wait_for, WaitTimeoutExpired
from distutils.version import LooseVersion


class ScyllaManagerError(Exception):
    """
    A custom exception for Manager related errors
    """
    pass


class HostSsl(Enum):
    ON = "ON"
    OFF = "OFF"

    @classmethod
    def from_str(cls, output_str):
        if "SSL" in output_str:
            return HostSsl.ON
        return HostSsl.OFF


class HostStatus(Enum):
    UP = "UP"
    DOWN = "DOWN"
    TIMEOUT = "TIMEOUT"

    @classmethod
    def from_str(cls, output_str):
        try:
            output_str = output_str.upper()
            if output_str == "-":
                return cls.DOWN
            return getattr(cls, output_str)
        except AttributeError:
            raise ScyllaManagerError("Could not recognize returned host status: {}".format(output_str))


class HostRestStatus(Enum):
    UP = "UP"
    DOWN = "DOWN"
    TIMEOUT = "TIMEOUT"
    UNAUTHORIZED = "UNAUTHORIZED"
    HTTP = "HTTP"

    @classmethod
    def from_str(cls, output_str):
        try:
            output_str = output_str.upper()
            if output_str == "-":
                return cls.DOWN
            return getattr(cls, output_str)
        except AttributeError:
            raise ScyllaManagerError("Could not recognize returned host rest status: {}".format(output_str))


class TaskStatus(Enum):
    NEW = "NEW"
    RUNNING = "RUNNING"
    DONE = "DONE"
    UNKNOWN = "UNKNOWN"
    ERROR = "ERROR"
    STOPPED = "STOPPED"
    STARTING = "STARTING"
    ABORTED = "ABORTED"

    @classmethod
    def from_str(cls, output_str):
        try:
            output_str = output_str.upper()
            output_str = output_str if len(output_str) == 1 else output_str.split()[0]
            return getattr(cls, output_str)
        except AttributeError:
            raise ScyllaManagerError("Could not recognize returned task status: {}".format(output_str))

    @classmethod
    def all_members(cls):
        return cls._member_map_.values()


class MgrUtils(object):

    @staticmethod
    def verify_errorless_result(cmd, stdout, stderr):
        if stderr:
            debug("Encountered an error on '{}' command response: {}".format(cmd, str(stdout)))
            raise ScyllaManagerError("Encountered an error on '{}' command response: {}".format(cmd, stderr))


class ScyllaManagerBase(object):

    def __init__(self, id, scylla_manager):
        self.id = id
        self.sctool = SCTool(scylla_manager=scylla_manager)
        self.scylla_manager = scylla_manager

    def get_property(self, parsed_table, column_name, is_search_substring=False, identifier=None):
        identifier = identifier or self.id
        return self.sctool.get_table_value(parsed_table=parsed_table, column_name=column_name, identifier=identifier,
                                           is_search_substring=is_search_substring)


class ScyllaManagerTool(ScyllaManagerBase):
    """
    Provides communication with scylla-manager, operating sctool commands and ssh-scripts.
    """

    def __init__(self, scylla_manager):
        ScyllaManagerBase.__init__(self, id="MANAGER", scylla_manager=scylla_manager)
        sleep = 5
        debug('Sleep {} seconds, waiting for manager service ready to respond'.format(sleep))
        time.sleep(sleep)
        debug("Initiating Scylla-Manager, version: {}".format(self.version))
        self.DEFAULT_USER = "centos"

    def restart_manager_server(self, gently):
        self.scylla_manager.stop(gently)
        self.scylla_manager.start()

    @property
    def version(self):
        cmd = "version"
        return self.sctool.run(cmd=cmd, is_verify_errorless_result=True)

    @property
    def cluster_list(self):
        """
        Gets the Manager's Cluster list
        """
        cmd = "cluster list"
        return self.sctool.run(cmd=cmd, is_verify_errorless_result=True)

    @property
    def parsed_cluster_list(self):
        """
        Gets the Manager's Cluster list
        """
        stdout, stderr = self.cluster_list
        return self.sctool.get_table_complete_column(parsed_table=stdout, column_name="name")

    def get_cluster(self, cluster_name):
        """
        Returns Manager Cluster object by a given name if exist, else returns none.
        """
        # ╭──────────────────────────────────────┬──────────╮
        # │ ID                                   │ name     │
        # ├──────────────────────────────────────┼──────────┤
        # │ 1de39a6b-ce64-41be-a671-a7c621035c0f │ Dev_Test │
        # │ bf6571ef-21d9-4cf1-9f67-9d05bc07b32e │ Prod     │
        # ╰──────────────────────────────────────┴──────────╯
        try:
            cluster_id = self.sctool.get_table_value(parsed_table=self.cluster_list, column_name="ID",
                                                     identifier=cluster_name)
        except ScyllaManagerError as e:
            debug("Cluster name not found in Scylla-Manager: {}".format(e))
            return None

        return ManagerCluster(scylla_manager=self.scylla_manager, cluster_id=cluster_id)

    def _get_cluster_hosts_ip(self, db_cluster):
        return [node_data[1] for node_data in self._get_cluster_hosts_with_ips(db_cluster=db_cluster)]

    def _get_cluster_hosts_with_ips(self, db_cluster):
        ip_addr_attr = 'public_ip_address'
        return [[n, getattr(n, ip_addr_attr)] for n in db_cluster.nodes]

    def add_cluster(self, name, node=None, db_cluster=None, client_encrypt=None, user=None, create_user=None,
                    single_node=False, by_name=False):
        """
        :param name: cluster name
        :param node: cluster node IP
        :param db_cluster: scylla cluster
        :param client_encrypt: is TSL client encryption enable/disable
        :return: ManagerCluster

        Add a cluster to manager

        Usage:
          sctool cluster add [flags]

        Flags:
          -h, --help                      help for add
              --host string               hostname or IP of one of the cluster nodes
          -n, --name alias                alias you can give to your cluster
              --ssh-identity-file path    path to identity file containing SSH private key
              --ssh-user name             SSH user name used to connect to the cluster nodes
              --ssl-user-cert-file path   path to client certificate when using client/server encryption with require_client_auth enabled
              --ssl-user-key-file path    path to key associated with ssl-user-cert-file

        Global Flags:
              --api-url URL    URL of Scylla Manager server (default "https://127.0.0.1:56443/api/v1")
          -c, --cluster name   target cluster name or ID

        Scylla Docs:
          https://docs.scylladb.com/operating-scylla/manager/1.4/add-a-cluster/
          https://docs.scylladb.com/operating-scylla/manager/1.4/sctool/#cluster-add


        """
        if not any([node, db_cluster]):
            raise ScyllaManagerError("Neither host or db_cluster parameter were given to Manager add_cluster")
        debug("Adding a cluster to scylla-manager, named: {}".format(name))
        node = node or self._get_cluster_hosts_ip(db_cluster=db_cluster)[0] #TODO: adjust  _get_cluster_hosts_ip()
        user = user or self.DEFAULT_USER
        ssh_user = create_user or 'scylla-manager'
        host = node.address()
        cluster_add_cmd = "cluster add --host {host} --name {name}".format(**locals())
        versions, _ = self.version
        client_version = versions[0][0].split()[2]
        if LooseVersion(client_version) >= LooseVersion('2.0'):
            cluster_add_cmd += " --auth-token {}".format(node.scylla_manager.auth_token)
        res_cluster_add, stderr = self.sctool.run(cmd=cluster_add_cmd)
        if not res_cluster_add or 'Cluster added' not in stderr:
            raise ScyllaManagerError("Encountered an error on 'sctool cluster add' command response: {}".format(res_cluster_add))
        # cluster_id = res_cluster_add.stdout.split('\n')[0]  # return ManagerCluster instance with the manager's new cluster-id
        cluster_id = res_cluster_add[0][0]
        return ManagerCluster(scylla_manager=self.scylla_manager, cluster_id=cluster_id, client_encrypt=client_encrypt)

    def upgrade(self, scylla_mgmt_upgrade_to_repo):
        raise ScyllaManagerError("Not converted from SCT to Dtest code")
        # manager_from_version = self.version
        # debug('Running Manager upgrade from: {} to version in repo: {}'.format(
        #     manager_from_version, scylla_mgmt_upgrade_to_repo))
        # self.manager_node.upgrade_mgmt(scylla_mgmt_repo=scylla_mgmt_upgrade_to_repo)
        # new_manager_version = self.version
        # debug('The Manager version after upgrade is: {}'.format(new_manager_version))
        # return new_manager_version

    def rollback_upgrade(self, manager_node):
        raise NotImplementedError

    def config_high_perf(self, dir=None, segment_tokens_max=100, poll_interval=50):
        poll_interval = str(poll_interval)+'ms'
        segments_per_repair = '100'
        conf_file = os.path.join(self.scylla_manager._get_path(), common.SCYLLAMANAGER_CONF)
        with open(conf_file, 'r') as f:
            data = yaml.load(f)
        if 'segment_tokens_max' in data:
            del data['segment_tokens_max']
        data['segment_tokens_max'] = segment_tokens_max
        if 'poll_interval' in data:
            del data['poll_interval']
        data['poll_interval'] = poll_interval
        if 'repair' in data and 'segments_per_repair' in data['repair']:
            del data['repair']['segments_per_repair']
            data['repair']['segments_per_repair'] = segments_per_repair
        with open(conf_file, 'w') as f:
            yaml.safe_dump(data, f, default_flow_style=False)
        with open(conf_file, 'r') as f:
            debug(msg="scylla-manager updated yaml is: {}".format(f.read()))
        self.scylla_manager.stop(gently=True)
        self.scylla_manager.start()
        time.sleep(2)


class SCTool(object):

    def __init__(self, scylla_manager):
        self.scylla_manager = scylla_manager

    def run(self, cmd, is_verify_errorless_result=False, parse_table_res=True, is_multiple_tables=False):
        list_cmd = cmd.split()
        debug("Issuing: 'sctool {}'".format(list_cmd))
        try:
            stdout, stderr = self.scylla_manager.sctool(cmd=list_cmd)

        except Exception as e:
            raise ScyllaManagerError("Encountered an error on sctool command: {}: {}".format(list_cmd, e))

        debug("sctool command result:")
        debug(msg=stdout)
        if is_verify_errorless_result:
            MgrUtils.verify_errorless_result(cmd=list_cmd, stdout=stdout, stderr=stderr)
        if parse_table_res:
            parsed_table = self.parse_result_table(stdout=stdout)
            if is_multiple_tables:
                dict_parsed_tables = self.parse_result_multiple_tables(parsed_table=parsed_table)
                return dict_parsed_tables, stderr
            return parsed_table, stderr
        return stdout, stderr

    def parse_result_table(self, stdout):
        parsed_table = []
        lines = stdout.splitlines()
        filtered_lines = [line for line in lines if line]
        if filtered_lines:
            if '╭' in stdout:
                filtered_lines = [line.replace('│', "|") for line in filtered_lines if
                                  not (line.startswith('╭') or line.startswith('├') or line.startswith(
                                      '╰'))]  # filter out the dashes lines
            else:
                filtered_lines = [line for line in filtered_lines if
                                  not line.startswith('+')]  # filter out the dashes lines
        for line in filtered_lines:
            list_line = [s if s else 'EMPTY' for s in line.split("|")]  # filter out spaces and "|" column seperators
            list_line_no_spaces = [s.split() for s in list_line if s != 'EMPTY']
            list_line_with_multiple_words_join = []
            for words in list_line_no_spaces:
                list_line_with_multiple_words_join.append(" ".join(words))
            if list_line_with_multiple_words_join:
                parsed_table.append(list_line_with_multiple_words_join)
        return parsed_table

    def parse_result_multiple_tables(self, parsed_table):
        """

        # Datacenter: us-eastscylla_node_east
        # ╭──────────┬────────────────╮
        # │ CQL      │ Host           │
        # ├──────────┼────────────────┤
        # │ UP (1ms) │ 18.233.164.181 │
        # ╰──────────┴────────────────╯
        # Datacenter: us-west-2scylla_node_west
        # ╭────────────┬───────────────╮
        # │ CQL        │ Host          │
        # ├────────────┼───────────────┤
        # │ UP (180ms) │ 54.245.183.30 │
        # ╰────────────┴───────────────╯
        # the above output example was translated to a single table that includes both 2 DC's values:
        # [['Datacenter: us-eastscylla_node_east'],
        #  ['CQL', 'Host'],
        #  ['UP (1ms)', '18.233.164.181'],
        #  ['Datacenter: us-west-2scylla_node_west'],
        #  ['CQL', 'Host'],
        #  ['UP (180ms)', '54.245.183.30']]
        :param parsed_table:
        :return:
        """
        if not any(len(line) == 1 for line in parsed_table):  # "1" means a table title like DC-name is found.
            return {"single_table": parsed_table}

        dict_res_tables = {}
        cur_table = None
        for line in parsed_table:
            if len(line) == 1:  # "1" means it is the table title like DC name.
                cur_table = line[0]
                dict_res_tables[cur_table] = []
            else:
                dict_res_tables[cur_table].append(line)
        return dict_res_tables

    def get_table_value(self, parsed_table, identifier, column_name=None, is_search_substring=False):
        """

        :param parsed_table:
        :param column_name:
        :param identifier:
        :param is_search_substring:
        :return:
        """

        # example expected parsed_table input is:
        # [['Host', 'Status', 'RTT'],
        #  ['18.234.77.216', 'UP', '0.92761'],
        #  ['54.203.234.42', 'DOWN', '0']]
        # usage flow example: mgr_cluster1.host -> mgr_cluster1.get_property -> get_table_value

        if not parsed_table or not self._is_found_in_table(parsed_table=parsed_table, identifier=identifier,
                                                           is_search_substring=is_search_substring):
            raise ScyllaManagerError(
                "Encountered an error retrieving sctool table value: {} not found in: {}".format(identifier,
                                                                                                 str(parsed_table)))
        column_titles = [title.upper() for title in
                         parsed_table[0]]  # get all table column titles capital (for comparison)
        if column_name and column_name.upper() not in column_titles:
            raise ScyllaManagerError("Column name: {} not found in table: {}".format(column_name, parsed_table))
        column_name_index = column_titles.index(
            column_name.upper()) if column_name else 1  # "1" is used in a case like "task progress" where no column names exist.
        ret_val = 'N/A'
        for row in parsed_table:
            if is_search_substring:
                if any(identifier in cur_str for cur_str in row):
                    ret_val = row[column_name_index]
                    break
            elif identifier in row:
                ret_val = row[column_name_index]
                break
        debug("{} {} value is:{}".format(identifier, column_name, ret_val))
        return ret_val

    def get_table_complete_column(self, parsed_table, column_name):

        column_titles = [title.upper() for title in
                         parsed_table[0]]  # get all table column titles capital (for comparison)
        if column_name and column_name.upper() not in column_titles:
            raise ScyllaManagerError("Column name: {} not found in table: {}".format(column_name, parsed_table))

        column_name_index = column_titles.index(column_name.upper())
        column_values = [row[column_name_index] for row in parsed_table[1:]]
        return column_values

    def _is_found_in_table(self, parsed_table, identifier, is_search_substring=False):
        full_rows_list = []
        for row in parsed_table:
            full_rows_list += row
        if is_search_substring:
            return any(identifier in cur_str for cur_str in full_rows_list)

        return identifier in full_rows_list


class ManagerTask(ScyllaManagerBase):

    def __init__(self, task_id, cluster_id, scylla_manager):
        ScyllaManagerBase.__init__(self, id=task_id, scylla_manager=scylla_manager)
        self.cluster_id = cluster_id

    def stop(self):
        cmd = "task stop {} -c {}".format(self.id, self.cluster_id)
        res = self.sctool.run(cmd=cmd, is_verify_errorless_result=True)
        return self.wait_and_get_final_status(timeout=30, step=3)

    def start(self, continue_task=True):
        cmd = "task start {} -c {}".format(self.id, self.cluster_id)
        if not continue_task:
            cmd += " --no-continue"
        self.sctool.run(cmd=cmd, is_verify_errorless_result=True)
        list_expected_task_status = [status for status in TaskStatus.all_members() if status != TaskStatus.STOPPED]
        return self.wait_for_status(list_status=list_expected_task_status, timeout=30, step=3)

    def _add_kwargs_to_cmd(self, cmd, **kwargs):
        for k, v in kwargs.items():
            cmd += ' --{}={}'.format(k, v)
        return cmd

    def update(self, **kwargs):
        """
          -e, --enabled string      enabled (default "true")
          -h, --help                help for update
          -i, --interval string     task schedule interval e.g. 3d2h10m, valid units are d, h, m, s (default "0")
          -r, --num-retries int     task schedule number of retries (default 3)
          -s, --start-date string   task start date in RFC3339 form or now[+duration], e.g. now+3d2h10m, valid units are d, h, m, s (default "now")

        :param kwargs:
        :return:
        """

        cmd_mapping = {'enabled': '--enabled',
                       'interval': '--interval',
                       'num_retries': '--num-retries',
                       'start_time': '--start-date'}
        cmd_arguments = []
        for k, v in kwargs.items():
            cmd_arguments.append("{0}={1}".format(cmd_mapping[k], v))

        cmd = "task update {0.id} -c {0.cluster_id} {update_arguments}".format(self, update_arguments=" ".join(cmd_arguments))
        stdout, _ = self.sctool.run(cmd=cmd, is_verify_errorless_result=True)
        return stdout

    @property
    def history(self):
        """
        Gets the task's history table
        """
        # ╭──────────────────────────────────────┬────────────────────────┬────────────────────────┬──────────┬───────╮
        # │ id                                   │ start time             │ end time               │ duration │ status│
        # ├──────────────────────────────────────┼────────────────────────┼────────────────────────┼──────────┼───────┤
        # │ e4f70414-ebe7-11e8-82c4-12c0dad619c2 │ 19 Nov 18 10:43:04 UTC │ 19 Nov 18 10:43:04 UTC │ 0s       │ NEW   │
        # │ 7f564891-ebe6-11e8-82c3-12c0dad619c2 │ 19 Nov 18 10:33:04 UTC │ 19 Nov 18 10:33:04 UTC │ 0s       │ NEW   │
        # │ 19b58cb3-ebe5-11e8-82c2-12c0dad619c2 │ 19 Nov 18 10:23:04 UTC │ 19 Nov 18 10:23:04 UTC │ 0s       │ NEW   │
        # │ b414cde5-ebe3-11e8-82c1-12c0dad619c2 │ 19 Nov 18 10:13:04 UTC │ 19 Nov 18 10:13:04 UTC │ 0s       │ NEW   │
        # │ 4e741c3d-ebe2-11e8-82c0-12c0dad619c2 │ 19 Nov 18 10:03:04 UTC │ 19 Nov 18 10:03:04 UTC │ 0s       │ NEW   │
        # ╰──────────────────────────────────────┴────────────────────────┴────────────────────────┴──────────┴───────╯
        cmd = "task history {} -c {}".format(self.id, self.cluster_id)
        stdout, stderr = self.sctool.run(cmd=cmd, is_verify_errorless_result=True)
        return stdout  # or can be specified like: self.get_property(parsed_table=res, column_name='status')

    @property
    def next_run(self):
        """
        Gets the task's next run value
        """
        # ╭──────────────────────────────────────────────────┬───────────────────────────────┬──────┬────────────┬────────│
        # │ task                                             │ next run                      │ ret. │ properties │ status │
        # ├──────────────────────────────────────────────────┼───────────────────────────────┼──────┼────────────┼────────│
        # │ healthcheck/7fb6f1a7-aafc-4950-90eb-dc64729e8ecb │ 18 Nov 18 20:32:08 UTC (+15s) │ 0    │            │ NEW    │
        # │ repair/22b68423-4332-443d-b8b4-713005ea6049      │ 19 Nov 18 00:00:00 UTC (+7d)  │ 3    │            │ NEW    │
        # ╰──────────────────────────────────────────────────┴───────────────────────────────┴──────┴────────────┴────────╯
        cmd = "task list -c {}".format(self.cluster_id)
        stdout, stderr = self.sctool.run(cmd=cmd, is_verify_errorless_result=True)
        return self.get_property(parsed_table=stdout, column_name='next run')

    @property
    def status(self):
        """
        Gets the task's status
        """
        cmd = "task list -a -c {}".format(self.cluster_id)
        stdout, stderr = self.sctool.run(cmd=cmd, is_verify_errorless_result=True)
        str_status = self.get_property(parsed_table=stdout, column_name='status', is_search_substring=True)
        return TaskStatus.from_str(str_status)

        # expecting output of:
        # ╭─────────────────────────────────────────────┬───────────────────────────────┬──────┬────────────┬────────╮
        # │ task                                        │ next run                      │ ret. │ properties │ status │
        # ├─────────────────────────────────────────────┼───────────────────────────────┼──────┼────────────┼────────┤
        # │ repair/2a4125d6-5d5a-45b9-9d8d-dec038b3732d │ 05 Nov 18 00:00 UTC (+7 days) │ 3    │            │ DONE   │
        # │ repair/dd98f6ae-bcf4-4c98-8949-573d533bb789 │                               │ 3    │            │ DONE   │
        # ╰─────────────────────────────────────────────┴───────────────────────────────┴──────┴────────────┴────────╯

    @property
    def progress(self):
        """
        Gets the repair task's progress
        """
        if self.status in [TaskStatus.NEW, TaskStatus.STARTING]:
            return " 0%"
        cmd = "task progress {} -c {}".format(self.id, self.cluster_id)
        stdout, stderr = self.sctool.run(cmd=cmd)
        # expecting output of:
        #  Status:           RUNNING
        #  Start time:       26 Mar 19 19:40:21 UTC
        #  Duration: 6s
        #  Progress: 0.12%
        #  Datacenters:
        #    - us-eastscylla_node_east
        #  ╭────────────────────┬───────╮
        #  │ system_auth        │ 0.47% │
        #  │ system_distributed │ 0.00% │
        #  │ system_traces      │ 0.00% │
        #  │ keyspace1          │ 0.00% │
        #  ╰────────────────────┴───────╯
        # [['Status: RUNNING'], ['Start time: 26 Mar 19 19:40:21 UTC'], ['Duration: 6s'], ['Progress: 0.12%'], ... ]
        progress = "N/A"
        for task_property in stdout:
            if task_property[0].startswith("Progress"):
                progress = task_property[0].split()[1]
                break
        return progress

    def has_percentage_reached_minimum(self, min_percentage):
        current_percentage = self.progress.strip()
        current_percentage_num = float(current_percentage[:-1])
        return current_percentage_num >= min_percentage

    def wait_for_minimal_progress_percentage(self, minimal_percentage, timeout=600, step=20):
        try:
            wait_for(func=self.has_percentage_reached_minimum, step=step, timeout=timeout,
                     min_percentage=minimal_percentage)
        except WaitTimeoutExpired:
            warning(f"Task {self.id} failed to reach a progress of {minimal_percentage} in {timeout} seconds")
            raise

    def full_progress_string(self):
        if self.status in [TaskStatus.NEW, TaskStatus.STARTING]:
            return " 0%"
        cmd = "task progress {} -c {}".format(self.id, self.cluster_id)
        stdout_list, stderr = self.sctool.run(cmd=cmd, is_verify_errorless_result=True)
        # sctool.run returns stdout_list as a list of lists, each of them containing a row of the output
        stdout_list = [line[0] for line in stdout_list]
        full_stdout_string = '\n'.join(stdout_list)
        return full_stdout_string

    def is_status_in_list(self, list_status, check_task_progress=False):
        """
        Check if the status of a given task is in list
        :param list_status:
        :return:
        """
        status = self.status
        if check_task_progress and status not in [TaskStatus.NEW,
                                                  TaskStatus.STARTING]:  # check progress for all statuses except 'NEW' / 'STARTING'
            ###
            # The reasons for the below (un-used) assignment are:
            # * check that progress command works on varios task statuses (that was how manager bug #856 found).
            # * print the progress to log in cases needed for failures/performance analysis.
            ###
            progress = self.progress
            debug("Task {} progress is: {}".format(self.id, progress))
        return status in list_status

    def wait_for_status(self, list_status, check_task_progress=True, timeout=600, step=20,
                        log_progress_on_failure=True):
        text = "Waiting until task: {} reaches status of: {}".format(self.id, list_status)
        try:
            is_status_reached = wait_for(func=self.is_status_in_list, step=step, text=text, list_status=list_status,
                                         check_task_progress=check_task_progress, timeout=timeout)
        except WaitTimeoutExpired:
            if log_progress_on_failure:
                warning(f"Task {self.id} failed to reach a status from {list_status}\n"
                        f"Task Progress:\n{self.full_progress_string()}\n")
            raise
        return is_status_reached

    def wait_and_get_final_status(self, timeout=600, step=20):
        """
        1) Wait for task to reach a 'final' status. meaning one of: done/error/stopped
        2) return the final status.
        :return:
        """
        list_final_status = [TaskStatus.ERROR, TaskStatus.STOPPED, TaskStatus.DONE, TaskStatus.ABORTED]
        debug("Waiting for task: {} getting to a final status ({})..".format(self.id, [str(s) for s in
                                                                                              list_final_status]))
        res = self.wait_for_status(list_status=list_final_status, timeout=timeout, step=step)
        if not res:
            raise ScyllaManagerError("Unexpected result on waiting for task {} status".format(self.id))
        return self.status

    def is_task_disabled(self):
        try:
            cmd = "task list -a -c {}".format(self.cluster_id)
            stdout, stderr = self.sctool.run(cmd=cmd, is_verify_errorless_result=True)
            self.get_property(
                parsed_table=stdout, column_name='status', is_search_substring=False, identifier="*" + self.id)
            return True
        except ScyllaManagerError as err:
            if "Encountered an error retrieving sctool table value" in err.args[0]:
                return False
            raise err


class RepairTask(ManagerTask):
    def __init__(self, task_id, cluster_id, scylla_manager):
        ManagerTask.__init__(self, task_id=task_id, cluster_id=cluster_id, scylla_manager=scylla_manager)


class HealthcheckTask(ManagerTask):
    def __init__(self, task_id, cluster_id, scylla_manager):
        ManagerTask.__init__(self, task_id=task_id, cluster_id=cluster_id, scylla_manager=scylla_manager)


class BackupTask(ManagerTask):
    def __init__(self, task_id, cluster_id, scylla_manager):
        ManagerTask.__init__(self, task_id=task_id, cluster_id=cluster_id, scylla_manager=scylla_manager)

    def get_snapshot_tag(self):
        # TODO: Add an option to choose from one of the tags to restore from, using backup list
        command = f" -c {self.cluster_id} task progress {self.id}"
        stdout, stderr = self.sctool.run(command, parse_table_res=False)
        if stderr:
            raise ScyllaManagerError(f"Failure for sctool '{command}' command:\n{stderr}")
        snapshot_line = [line for line in stdout.splitlines() if "snapshot tag" in line.lower()]
        # Returns the following:
        # Snapshot Tag:	sm_20200106093455UTC
        # (when executed manually, the title and value is separated by \t instead
        snapshot_tag = snapshot_line[0].split(":")[1].strip()
        return snapshot_tag


class RestTask(ManagerTask):
    def __init__(self, task_id, cluster_id, scylla_manager):
        ManagerTask.__init__(self, task_id=task_id, cluster_id=cluster_id, scylla_manager=scylla_manager)


class ManagerCluster(ScyllaManagerBase):

    def __init__(self, scylla_manager, cluster_id, client_encrypt=False):
        if not scylla_manager:
            raise ScyllaManagerError("Cannot create a Manager Cluster where no 'scylla-manager' parameter is given")
        ScyllaManagerBase.__init__(self, id=cluster_id, scylla_manager=scylla_manager)
        self.client_encrypt = client_encrypt

    def run_backup_command(self, dc_list=None,  # pylint: disable=too-many-arguments,too-many-locals,too-many-branches
                           dry_run=None, force=None, interval=None, keyspace_list=None,
                           location_list=None, num_retries=None, rate_limit_list=None, retention=None, show_tables=None,
                           snapshot_parallel_list=None, start_date=None, upload_parallel_list=None):
        cmd = "backup -c {}".format(self.id)

        if dc_list is not None:
            dc_names = ','.join(dc_list)
            cmd += " --dc {} ".format(dc_names)
        if dry_run is not None:
            cmd += " --dry-run"
        if force is not None:
            cmd += " --force"
        if interval is not None:
            cmd += " --interval {}".format(interval)
        if keyspace_list is not None:
            keyspaces_names = ','.join(keyspace_list)
            cmd += " --keyspace {} ".format(keyspaces_names)
        if location_list is not None:
            locations_names = ','.join(location_list)
            cmd += " --location {} ".format(locations_names)
        if num_retries is not None:
            cmd += " --num-retries {}".format(num_retries)
        if rate_limit_list is not None:
            rate_limit_string = ','.join(rate_limit_list)
            cmd += " --rate-limit {} ".format(rate_limit_string)
        if retention is not None:
            cmd += " --retention {} ".format(retention)
        if show_tables is not None:
            cmd += " --show-tables {} ".format(show_tables)
        if snapshot_parallel_list is not None:
            snapshot_parallel_string = ','.join(snapshot_parallel_list)
            cmd += " --snapshot-parallel {} ".format(snapshot_parallel_string)
        if start_date is not None:
            cmd += " --start-date {} ".format(start_date)
        if upload_parallel_list is not None:
            upload_parallel_string = ','.join(upload_parallel_list)
            cmd += " --upload-parallel {} ".format(upload_parallel_string)

        stdout, stderr = self.sctool.run(cmd=cmd, parse_table_res=False)
        if not stdout:
            raise ScyllaManagerError("Unknown failure for sctool '{}' command".format(cmd))

        if stderr:
            debug("Encountered an error on '{}' command response".format(cmd))
            raise ScyllaManagerError(stderr)

        task_id = stdout.strip()
        debug("Created task id is: {}".format(task_id))
        return BackupTask(task_id=task_id, cluster_id=self.id, scylla_manager=self.scylla_manager)

    def get_backup_files_dict(self, snapshot_tag):
        command = f" -c {self.id} backup files --snapshot-tag {snapshot_tag}"
        # The sctool backup files command prints the s3 paths of all of the files that are required to restore the
        # cluster from the backup
        snapshot_files, stderr = self.sctool.run(command)
        snapshot_file_list = [file_path_list[0] for file_path_list in snapshot_files]
        # sctool.run returns a list of lists, each of them is a 1 length list that contains the row.
        # This list comprehension turns the list into a list of strings (rows) instead
        return self.snapshot_files_to_dict(snapshot_file_list)

    def snapshot_files_to_dict(self, snapshot_file_lines):
        per_node_keyspaces_and_tables_backup_files = {}
        for line in snapshot_file_lines:
            s3_file_path, keyspace_and_table = [string.strip() for string in line.split(' ')]
            node_id = s3_file_path[s3_file_path.find("/node/") + len("/node/"):s3_file_path.find("/keyspace")]
            keyspace, table = keyspace_and_table.split('/')
            if node_id not in per_node_keyspaces_and_tables_backup_files:
                per_node_keyspaces_and_tables_backup_files[node_id] = {}
            if keyspace not in per_node_keyspaces_and_tables_backup_files[node_id]:
                per_node_keyspaces_and_tables_backup_files[node_id][keyspace] = {}
            if table not in per_node_keyspaces_and_tables_backup_files[node_id][keyspace]:
                per_node_keyspaces_and_tables_backup_files[node_id][keyspace][table] = []
            per_node_keyspaces_and_tables_backup_files[node_id][keyspace][table].append(s3_file_path)
        return per_node_keyspaces_and_tables_backup_files

    def delete_backup(self, snapshot_tag):
        self.sctool.run(f"-c {self.id} backup delete --snapshot-tag={snapshot_tag}")

    def create_repair_task(self, dc_list=None, keyspace=None, interval=None, num_retries=None, fail_fast=None,
                           intensity=None):
        # the interval string:
        # Amount of time after which a successfully completed task would be run again. Supported time units include:
        #
        # d - days,
        # h - hours,
        # m - minutes,
        # s - seconds.
        cmd = "repair -c {}".format(self.id)
        if dc_list is not None:
            dc_names = ','.join([dc_name for dc_name in dc_list])
            cmd += " --dc {} ".format(dc_names)
        if keyspace is not None:
            cmd += " --keyspace {} ".format(keyspace)
        if interval is not None:
            cmd += " --interval {}".format(interval)
        if num_retries is not None:
            cmd += " --num-retries {}".format(num_retries)
        if fail_fast is not None:
            cmd += " --fail-fast"
        if intensity is not None:
            cmd += f" --intensity {intensity}"

        debug("Repair command to execute is: {}".format(cmd))
        stdout, stderr = self.sctool.run(cmd=cmd, parse_table_res=False)
        if not stdout:
            raise ScyllaManagerError("Unknown failure for sctool '{}' command".format(cmd))

        if "no matching units found" in stderr:
            raise ScyllaManagerError("Manager cannot run repair where no keyspace exists.")

        # expected result output is to have a format of: "repair/2a4125d6-5d5a-45b9-9d8d-dec038b3732d"
        if 'repair' not in stdout:
            debug("Encountered an error on '{}' command response".format(cmd))
            raise ScyllaManagerError(stderr)

        task_id = stdout.strip()
        debug("Created task id is: {}".format(task_id))

        return RepairTask(task_id=task_id, cluster_id=self.id, scylla_manager=self.scylla_manager)  # return the manager's object with new repair-task-id

    def delete(self):
        """
        $ sctool cluster delete
        """

        cmd = "cluster delete -c {}".format(self.id)
        stdout, stderr = self.sctool.run(cmd=cmd, is_verify_errorless_result=True)
        return stdout

    def update(self, name=None, host=None, ssh_identity_file=None, ssh_user=None, client_encrypt=None):
        """
        $ sctool cluster update --help
        Modify a cluster

        Usage:
          sctool cluster update [flags]

        Flags:
          -h, --help                     help for update
              --host string              hostname or IP of one of the cluster nodes
          -n, --name alias               alias you can give to your cluster
              --ssh-identity-file path   path to identity file containing SSH private key
              --ssh-user name            SSH user name used to connect to the cluster nodes
        """
        cmd = "cluster update -c {}".format(self.id)
        if name:
            cmd += " --name {}".format(name)
        if host:
            cmd += " --host {}".format(host)
        if ssh_identity_file:
            cmd += " --ssh-identity-file {}".format(ssh_identity_file)
        if ssh_user:
            cmd += " --ssh-user {}".format(ssh_user)
        stdout, stderr = self.sctool.run(cmd=cmd, is_verify_errorless_result=True)
        return stdout

    @property
    def _cluster_list(self):
        """
        Gets the Manager's Cluster list
        """
        cmd = "cluster list"
        stdout, stderr = self.sctool.run(cmd=cmd, is_verify_errorless_result=True)
        return stdout

    @property
    def name(self):
        """
        Gets the Cluster name as represented in Manager
        """
        # expecting output of:
        # ╭──────────────────────────────────────┬──────┬─────────────┬────────────────╮
        # │ cluster id                           │ name │ host        │ ssh user       │
        # ├──────────────────────────────────────┼──────┼─────────────┼────────────────┤
        # │ 1de39a6b-ce64-41be-a671-a7c621035c0f │ sce2 │ 10.142.0.25 │ scylla-manager │
        # ╰──────────────────────────────────────┴──────┴─────────────┴────────────────╯
        return self.get_property(parsed_table=self._cluster_list, column_name='name')

    @property
    def ssh_user(self):
        """
        Gets the Cluster ssh_user as represented in Manager
        """
        # expecting output of:
        # ╭──────────────────────────────────────┬──────┬─────────────┬────────────────╮
        # │ cluster id                           │ name │ host        │ ssh user       │
        # ├──────────────────────────────────────┼──────┼─────────────┼────────────────┤
        # │ 1de39a6b-ce64-41be-a671-a7c621035c0f │ sce2 │ 10.142.0.25 │ scylla-manager │
        # ╰──────────────────────────────────────┴──────┴─────────────┴────────────────╯
        return self.get_property(parsed_table=self._cluster_list, column_name='ssh user')

    def _get_task_list(self):
        cmd = "task list -c {}".format(self.id)
        stdout, stderr = self.sctool.run(cmd=cmd, is_verify_errorless_result=True)
        return stdout

    @property
    def repair_task_list(self):
        """
        Gets the Cluster's  Task list
        """
        # ╭─────────────────────────────────────────────┬───────────────────────────────┬──────┬────────────┬────────╮
        # │ task                                        │ next run                      │ ret. │ properties │ status │
        # ├─────────────────────────────────────────────┼───────────────────────────────┼──────┼────────────┼────────┤
        # │ repair/2a4125d6-5d5a-45b9-9d8d-dec038b3732d │ 26 Nov 18 00:00 UTC (+7 days) │ 3    │            │ DONE   │
        # │ repair/dd98f6ae-bcf4-4c98-8949-573d533bb789 │                               │ 3    │            │ DONE   │
        # ╰─────────────────────────────────────────────┴───────────────────────────────┴──────┴────────────┴────────╯
        repair_task_list = []
        table_res = self._get_task_list()
        if len(table_res) > 1:  # if there are any tasks in list - add them as RepairTask generated objects.
            repair_task_rows_list = [row for row in table_res[1:] if row[0].startswith("repair/")]
            for row in repair_task_rows_list:
                repair_task_list.append(RepairTask(task_id=row[0], cluster_id=self.id, scylla_manager=self.scylla_manager))
        return repair_task_list

    def get_healthcheck_task(self):
        healthcheck_id = self.sctool.get_table_value(parsed_table=self._get_task_list(), column_name="task",
                                                     identifier="healthcheck/", is_search_substring=True)
        return HealthcheckTask(task_id=healthcheck_id, cluster_id=self.id, scylla_manager=self.scylla_manager)  # return the manager's health-check-task object with the found id

    def get_rest_task(self):
        rest_id = self.sctool.get_table_value(parsed_table=self._get_task_list(), column_name="task",
                                                     identifier="healthcheck_rest/", is_search_substring=True)
        return RestTask(task_id=rest_id, cluster_id=self.id, scylla_manager=self.scylla_manager)  # return the manager's rest-task object with the found id

    def get_hosts_health(self, translate_minus_to_down=True):
        """
        Gets the Manager's Cluster Nodes status
        """
        # $ sctool status -c bla
        # Datacenter: dc1
        # ╭────┬─────────────────────────┬───────────┬────────────────┬──────────────────────────────────────╮
        # │    │ CQL                     │ REST      │ Host           │ Host ID                              │
        # ├────┼─────────────────────────┼───────────┼────────────────┼──────────────────────────────────────┤
        # │ UN │ UP SSL (58ms)           │ UP (2ms)  │ 192.168.100.11 │ a2b4200a-4157-4b47-9c10-b102246fe7ff │
        # │ UN │ UP SSL (60ms)           │ UP (3ms)  │ 192.168.100.12 │ aa1d8329-a66e-4500-bb38-ed9c4f236a0d │
        # │ UN │ DOWN SSL (40ms)         │ UP (11ms) │ 192.168.100.13 │ b583255c-4029-4207-8237-e40996985f29 │
        # ╰────┴─────────────────────────┴───────────┴────────────────┴──────────────────────────────────────╯
        # Datacenter: dc2
        # ╭────┬─────────────────────────┬───────────────────┬────────────────┬──────────────────────────────────────╮
        # │    │ CQL                     │ REST              │ Host           │ Host ID                              │
        # ├────┼─────────────────────────┼───────────────────┼────────────────┼──────────────────────────────────────┤
        # │ UN │ TIMEOUT SSL             │ UP (4ms)          │ 192.168.100.21 │ 9b91b800-f74d-47ed-973c-7a8ef8088c77 │
        # │ UN │ TIMEOUT SSL             │ TIMEOUT           │ 192.168.100.22 │ 56d2f4c0-9327-487e-b115-c96d3e5c014b │
        # │ UN │ UP SSL (40ms)           │ HTTP (503) (7ms)  │ 192.168.100.23 │ 08152d3d-ed30-469e-bc19-5ab9f4248e9a │
        # ╰────┴─────────────────────────┴───────────────────┴────────────────┴──────────────────────────────────────╯
        cmd = "status -c {}".format(self.id)
        dict_status_tables, stderr = self.sctool.run(cmd=cmd, is_verify_errorless_result=True, is_multiple_tables=True)

        dict_hosts_health = {}
        for dc_name, hosts_table in dict_status_tables.items():
            if len(hosts_table) < 2:
                debug("Cluster: {} - {} has no hosts health report".format(self.id, dc_name))
            else:
                list_titles_row = hosts_table[0]
                host_col_idx = list_titles_row.index("Address")
                cql_status_col_idx = list_titles_row.index("CQL")
                rest_col_idx = list_titles_row.index("REST")

                for line in hosts_table[1:]:
                    host = line[host_col_idx]
                    list_cql = line[cql_status_col_idx].split()
                    status = list_cql[0]
                    rtt = self._extract_value_with_regex(string=list_cql[-1], regex_pattern=r"\(([^)]+ms)")
                    rest_value = line[rest_col_idx]
                    if rest_value == '-':
                        rest_status = rest_value
                    else:
                        rest_status = rest_value[:rest_value.find("(")].strip()
                    rest_rtt = self._extract_value_with_regex(string=rest_value, regex_pattern=r"\(([^)]+ms)")
                    rest_http_status_code = self._extract_value_with_regex(string=rest_value,
                                                                           regex_pattern=r"\(([0-9]*?)\)")
                    ssl = line[cql_status_col_idx]
                    # Whether or not SSL is on is now described in the cql column
                    # If SSL is on the column value will include "SSL" in it, and if not it will not.
                    if translate_minus_to_down:
                        dict_hosts_health[host] = self._HostHealth(status=HostStatus.from_str(status), rtt=rtt,
                                                                   rest_status=HostRestStatus.from_str(rest_status),
                                                                   rest_rtt=rest_rtt, ssl=HostSsl.from_str(ssl),
                                                                   rest_http_status_code=rest_http_status_code)
                    else:
                        dict_hosts_health[host] = self._HostHealth(status=status, rtt=rtt,
                                                                   rest_status=rest_status,
                                                                   rest_rtt=rest_rtt, ssl=HostSsl.from_str(ssl),
                                                                   rest_http_status_code=rest_http_status_code)
            debug("Cluster {} Hosts Health is:".format(self.id))
            for ip, health in dict_hosts_health.items():
                debug("{}: {},{},{},{},{}".format(ip, health.status, health.rtt, health.rest_status, health.rest_rtt, health.ssl))
        return dict_hosts_health

    class _HostHealth():
        def __init__(self, status, rtt, ssl, rest_status, rest_rtt, rest_http_status_code=None):
            self.status = status
            self.rtt = rtt
            self.rest_status = rest_status
            self.rest_rtt = rest_rtt
            self.ssl = ssl
            self.rest_http_status_code = rest_http_status_code

    @staticmethod
    def _extract_value_with_regex(string, regex_pattern, default_value="N/A"):
        value_list = findall(pattern=regex_pattern, string=string)
        if len(value_list) == 1:
            return value_list[0]
        return default_value


class ScyllaManagerMixin:
    def config_and_create_cluster(self, nodes):
        self.cluster.set_configuration_options(values={'hinted_handoff_enabled': False})
        self.cluster.populate(nodes).start(wait_for_binary_proto=True, wait_other_notice=True)
        return self.cluster.nodelist()

    def _create_mgr_cluster(self, node, name):
        manager_tool = ScyllaManagerTool(scylla_manager=self.cluster._scylla_manager)
        mgr_cluster = manager_tool.add_cluster(node=node, name=name)

        return mgr_cluster
