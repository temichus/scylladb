# coding: utf-8
# ccm clusters
import time
from enum import Enum

from dtest import debug, wait_for


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
        try:
            output_str = output_str.upper()
            return getattr(cls, output_str)
        except AttributeError:
            raise ScyllaManagerError("Could not recognize returned task status: {}".format(output_str))

class HostStatus(Enum):
    UP = "UP"
    DOWN = "DOWN"

    @classmethod
    def from_str(cls, output_str):
        try:
            output_str = output_str.upper()
            return getattr(cls, output_str)
        except AttributeError:
            raise ScyllaManagerError("Could not recognize returned task status: {}".format(output_str))


class HostRestStatus(Enum):
    UP = "UP"
    DOWN = "DOWN"

    @classmethod
    def from_str(cls, output_str):
        try:
            output_str = output_str.upper()
            return getattr(cls, output_str)
        except AttributeError:
            raise ScyllaManagerError("Could not recognize returned task status: {}".format(output_str))


class TaskStatus(Enum):
    NEW = "NEW"
    RUNNING = "RUNNING"
    DONE = "DONE"
    UNKNOWN = "UNKNOWN"
    ERROR = "ERROR"
    STOPPED = "STOPPED"
    STARTING = "STARTING"

    @classmethod
    def from_str(cls, output_str):
        try:
            output_str = output_str.upper()
            return getattr(cls, output_str)
        except AttributeError:
            raise ScyllaManagerError("Could not recognize returned task status: {}".format(output_str))


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
        self.scylla_manager=scylla_manager

    def get_property(self, parsed_table, column_name):
        return self.sctool.get_table_value(parsed_table=parsed_table, column_name=column_name, identifier=self.id)


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

    @property
    def version(self):
        cmd = "version"
        return self.sctool.run(list_cmd=[cmd], is_verify_errorless_result=True)

    @property
    def cluster_list(self):
        """
        Gets the Manager's Cluster list
        """
        cmd = "cluster list"
        return self.sctool.run(list_cmd=cmd.split(), is_verify_errorless_result=True)

    def get_cluster(self, cluster_name):
        """
        Returns Manager Cluster object by a given name if exist, else returns none.
        """
        # ╭──────────────────────────────────────┬──────────┬─────────────┬────────────────╮
        # │ cluster id                           │ name     │ host        │ ssh user       │
        # ├──────────────────────────────────────┼──────────┼─────────────┼────────────────┤
        # │ 1de39a6b-ce64-41be-a671-a7c621035c0f │ Dev_Test │ 10.142.0.25 │ scylla-manager │
        # │ bf6571ef-21d9-4cf1-9f67-9d05bc07b32e │ Prod     │ 10.142.0.26 │ scylla-manager │
        # ╰──────────────────────────────────────┴──────────┴─────────────┴────────────────╯
        try:
            cluster_id = self.sctool.get_table_value(parsed_table=self.cluster_list, column_name="cluster id",
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

    def add_cluster(self, name, node=None, db_cluster=None, client_encrypt=None, user=None, create_user=None, single_node=False):
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
        node = node or self._get_cluster_hosts_ip(db_cluster=db_cluster)[0] #TODO: adjust  _get_cluster_hosts_ip()
        user = user or self.DEFAULT_USER
        ssh_user = create_user or 'scylla-manager'

        # stderr, stdout = node.cluster.sctool(["version"])
        # debug("Scylla-manager version is:".format(stdout))

        list_cluster_add_cmd = ["cluster", "add", "--host", node.address(), "--name", name]
        # node.cluster.sctool(cluster_add_cmd)
        # cluster_add_cmd = "cluster add --host {} --name {}".format(node.address(), name)
        # cluster_add_cmd = "version"
        res_cluster_add, stderr = self.sctool.run(list_cmd=list_cluster_add_cmd)
        if not res_cluster_add or 'Cluster added' not in stderr:
            raise ScyllaManagerError("Encountered an error on 'sctool cluster add' command response: {}".format(res_cluster_add))
        # cluster_id = res_cluster_add.stdout.split('\n')[0]  # return ManagerCluster instance with the manager's new cluster-id
        cluster_id = res_cluster_add[0][0]
        return ManagerCluster(scylla_manager=self.scylla_manager, cluster_id=cluster_id, client_encrypt=client_encrypt)

    def upgrade(self, scylla_mgmt_upgrade_to_repo):
        raise ScyllaManagerError("Not converted from SCT to Dtest code")
        # manager_from_version = self.version
        # debug('Running Manager upgrade from: {} to version in repo: {}'.format(manager_from_version, scylla_mgmt_upgrade_to_repo))
        # self.manager_node.upgrade_mgmt(scylla_mgmt_repo=scylla_mgmt_upgrade_to_repo)
        # new_manager_version = self.version
        # debug('The Manager version after upgrade is: {}'.format(new_manager_version))
        # return new_manager_version

    def rollback_upgrade(self, manager_node):
        raise NotImplementedError


class SCTool(object):

    def __init__(self, scylla_manager):
        self.scylla_manager=scylla_manager


    def run(self, list_cmd, is_verify_errorless_result=False, parse_table_res=True, is_multiple_tables=False):
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
        lines = stdout.split('\n')
        filtered_lines = [line for line in lines if
                          not (line.startswith('╭') or line.startswith('├') or line.startswith(
                              '╰'))]  # filter out the dashes lines
        filtered_lines = [line for line in filtered_lines if line]
        for line in filtered_lines:
            list_line = [s if s else 'EMPTY' for s in line.split("│")]  # filter out spaces and "|" column seperators
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
        res = self.sctool.run(list_cmd=cmd.split(), is_verify_errorless_result=True)
        return self.wait_and_get_final_status(timeout=30, step=3)

    def start(self, cmd=None):
        cmd = cmd or "task start {} -c {}".format(self.id, self.cluster_id)
        res = self.sctool.run(list_cmd=cmd.split(), is_verify_errorless_result=True)
        list_all_task_status = [s for s in TaskStatus.__dict__ if not s.startswith("__")]
        list_expected_task_status = [status for status in list_all_task_status if status != TaskStatus.STOPPED]
        return self.wait_for_status(list_status=list_expected_task_status, timeout=30, step=3)

    def _add_kwargs_to_cmd(self, cmd, **kwargs):
        for k, v in kwargs.items():
            cmd += ' --{}={}'.format(k, v)
        return cmd

    @property
    def history(self):
        """
        Gets the task's history table
        """
        # ╭──────────────────────────────────────┬────────────────────────┬────────────────────────┬──────────┬───────╮
        # │ id                                   │ start time             │ end time               │ duration │ status│                                                                                                                                                                  │
        # ├──────────────────────────────────────┼────────────────────────┼────────────────────────┼──────────┼───────┤
        # │ e4f70414-ebe7-11e8-82c4-12c0dad619c2 │ 19 Nov 18 10:43:04 UTC │ 19 Nov 18 10:43:04 UTC │ 0s       │ NEW   │
        # │ 7f564891-ebe6-11e8-82c3-12c0dad619c2 │ 19 Nov 18 10:33:04 UTC │ 19 Nov 18 10:33:04 UTC │ 0s       │ NEW   │
        # │ 19b58cb3-ebe5-11e8-82c2-12c0dad619c2 │ 19 Nov 18 10:23:04 UTC │ 19 Nov 18 10:23:04 UTC │ 0s       │ NEW   │
        # │ b414cde5-ebe3-11e8-82c1-12c0dad619c2 │ 19 Nov 18 10:13:04 UTC │ 19 Nov 18 10:13:04 UTC │ 0s       │ NEW   │
        # │ 4e741c3d-ebe2-11e8-82c0-12c0dad619c2 │ 19 Nov 18 10:03:04 UTC │ 19 Nov 18 10:03:04 UTC │ 0s       │ NEW   │
        # ╰──────────────────────────────────────┴────────────────────────┴────────────────────────┴──────────┴───────╯
        cmd = "task history {} -c {}".format(self.id, self.cluster_id)
        stdout, stderr = self.sctool.run(list_cmd=cmd.split(), is_verify_errorless_result=True)
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
        stdout, stderr = self.sctool.run(list_cmd=cmd.split(), is_verify_errorless_result=True)
        return self.get_property(parsed_table=stdout, column_name='next run')

    @property
    def status(self):
        """
        Gets the task's status
        """
        cmd = "task list -c {}".format(self.cluster_id)
        stdout, stderr = self.sctool.run(list_cmd=cmd.split(), is_verify_errorless_result=True)
        return self.get_property(parsed_table=stdout, column_name='status')

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
        stdout, stderr = self.sctool.run(list_cmd=cmd.split())
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
                progress = task_property[0].split(':')[1]
                break
        return progress

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
        return self.status in list_status

    def wait_for_status(self, list_status, check_task_progress=True, timeout=3600, step=120):
        text = "Waiting until task: {} reaches status of: {}".format(self.id, list_status)
        is_status_reached = wait_for(func=self.is_status_in_list, step=step,
                                          text=text, list_status=list_status, check_task_progress=check_task_progress,
                                          timeout=timeout)
        return is_status_reached

    def wait_and_get_final_status(self, timeout=3600, step=120):
        """
        1) Wait for task to reach a 'final' status. meaning one of: done/error/stopped
        2) return the final status.
        :return:
        """
        list_final_status = [TaskStatus.ERROR, TaskStatus.STOPPED, TaskStatus.DONE]
        debug("Waiting for task: {} getting to a final status ({})..".format(self.id, [str(s) for s in
                                                                                              list_final_status]))
        res = self.wait_for_status(list_status=list_final_status, timeout=timeout, step=step)
        if not res:
            raise ScyllaManagerError("Unexpected result on waiting for task {} status".format(self.id))
        return self.status

class RepairTask(ManagerTask):
    def __init__(self, task_id, cluster_id, scylla_manager):
        ManagerTask.__init__(self, task_id=task_id, cluster_id=cluster_id, scylla_manager=scylla_manager)

    def start(self, use_continue=False, **kwargs):
        str_continue = '--continue=true' if use_continue else '--continue=false'
        cmd = "task start {} -c {} {}".format(self.id, self.cluster_id, str_continue)
        ManagerTask.start(self, cmd=cmd)

    def continue_repair(self):
        self.start(use_continue=True)

class HealthcheckTask(ManagerTask):
    def __init__(self, task_id, cluster_id, scylla_manager):
        ManagerTask.__init__(self, task_id=task_id, cluster_id=cluster_id, scylla_manager=scylla_manager)

class RestTask(ManagerTask):
    def __init__(self, task_id, cluster_id, scylla_manager):
        ManagerTask.__init__(self, task_id=task_id, cluster_id=cluster_id, scylla_manager=scylla_manager)

class ManagerCluster(ScyllaManagerBase):

    def __init__(self, scylla_manager, cluster_id, client_encrypt=False):
        if not scylla_manager:
            raise ScyllaManagerError("Cannot create a Manager Cluster where no 'scylla-manager' parameter is given")
        ScyllaManagerBase.__init__(self, id=cluster_id, scylla_manager=scylla_manager)
        self.client_encrypt = client_encrypt

    def create_repair_task(self, node=None, token_ranges=None, keyspace=None, with_hosts=None):
        cmd = "repair -c {}".format(self.id)
        if node:
            cmd += " --host {} ".format(node.address())
        if token_ranges:
            cmd += " --token-ranges {} ".format(token_ranges)
        if keyspace:
            cmd += " --keyspace {} ".format(keyspace)
        if with_hosts:
            cmd += " --with-hosts {} ".format(with_hosts.address())

        debug("Repair command to execute is: {}".format(cmd))
        stdout, stderr = self.sctool.run(list_cmd=cmd.split(), parse_table_res=False)
        if not stdout:
            raise ScyllaManagerError("Unknown failure for sctool {} command".format(cmd))

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
        stdout, stderr = self.sctool.run(list_cmd=cmd.split(), is_verify_errorless_result=True)
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
        stdout, stderr = self.sctool.run(list_cmd=cmd.split(), is_verify_errorless_result=True)
        return stdout

    @property
    def _cluster_list(self):
        """
        Gets the Manager's Cluster list
        """
        cmd = "cluster list"
        stdout, stderr = self.sctool.run(list_cmd=cmd.split(), is_verify_errorless_result=True)
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
        stdout, stderr = self.sctool.run(list_cmd=cmd.split(), is_verify_errorless_result=True)
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

    def get_hosts_health(self):
        """
        Gets the Manager's Cluster Nodes status
        """
        # $ sctool status -c bla
        # 19:43:56 [107.23.100.82] [stdout] Datacenter: us-eastscylla_node_east
        # 19:43:56 [107.23.100.82] [stdout] ╭──────────┬─────┬──────────┬────────────────╮
        # 19:43:56 [107.23.100.82] [stdout] │ CQL      │ SSL │ REST     │ Host           │
        # 19:43:56 [107.23.100.82] [stdout] ├──────────┼─────┼──────────┼────────────────┤
        # 19:43:56 [107.23.100.82] [stdout] │ UP (0ms) │ OFF │ UP (0ms) │ 34.205.64.58   │
        # 19:43:56 [107.23.100.82] [stdout] │ UP (0ms) │ OFF │ UP (0ms) │ 54.159.184.253 │
        # 19:43:56 [107.23.100.82] [stdout] ╰──────────┴─────┴──────────┴────────────────╯
        # 19:43:56 [107.23.100.82] [stdout] Datacenter: us-west-2scylla_node_west
        # 19:43:56 [107.23.100.82] [stdout] ╭────────────┬─────┬───────────┬──────────────╮
        # 19:43:56 [107.23.100.82] [stdout] │ CQL        │ SSL │ REST      │ Host         │
        # 19:43:56 [107.23.100.82] [stdout] ├────────────┼─────┼───────────┼──────────────┤
        # 19:43:56 [107.23.100.82] [stdout] │ UP (151ms) │ OFF │ UP (80ms) │ 34.219.6.187 │
        # 19:43:56 [107.23.100.82] [stdout] ╰────────────┴─────┴───────────┴──────────────╯
        cmd = "status -c {}".format(self.id)
        dict_status_tables, stderr = self.sctool.run(list_cmd=cmd.split(), is_verify_errorless_result=True, is_multiple_tables=True)

        dict_hosts_health = {}
        for dc_name, hosts_table in dict_status_tables.items():
            if len(hosts_table) < 2:
                debug("Cluster: {} - {} has no hosts health report".format(self.id, dc_name))
            else:
                list_titles_row = hosts_table[0]
                host_col_idx = list_titles_row.index("Host")
                cql_status_col_idx = list_titles_row.index("CQL")
                ssl_col_idx = list_titles_row.index("SSL")
                rest_col_idx = list_titles_row.index("REST")

                for line in hosts_table[1:]:
                    host = line[host_col_idx]
                    list_cql = line[cql_status_col_idx].split()
                    status = list_cql[0]
                    rtt = list_cql[1].strip("()") if len(list_cql) == 2 else "N/A"

                    list_rest = line[rest_col_idx].split()
                    rest_status = list_rest[0]
                    rest_rtt = list_rest[1].strip("()") if len(list_rest) == 2 else "N/A"

                    ssl = line[ssl_col_idx]
                    dict_hosts_health[host] = self._HostHealth(status=HostStatus.from_str(status), rtt=rtt, rest_status=HostRestStatus.from_str(rest_status), rest_rtt=rest_rtt, ssl=HostSsl.from_str(ssl))
            debug("Cluster {} Hosts Health is:".format(self.id))
            for ip, health in dict_hosts_health.items():
                debug("{}: {},{},{}".format(ip, health.status, health.rtt, health.rest_status, health.rest_rtt, health.ssl))
        return dict_hosts_health

    class _HostHealth():
        def __init__(self, status, rtt, ssl, rest_status, rest_rtt):
            self.status = status
            self.rtt = rtt
            self.rest_status = rest_status
            self.rest_rtt = rest_rtt
            self.ssl = ssl

