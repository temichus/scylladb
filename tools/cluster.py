import logging
import re
import requests
from concurrent.futures.thread import ThreadPoolExecutor
from typing import Any, TypedDict

from ccmlib.cluster import Cluster
from ccmlib.dse_cluster import DseCluster
from ccmlib.scylla_node import ScyllaNode
from cassandra.cluster import Session

logger = logging.getLogger(__name__)


def new_node(cluster, bootstrap=True, token=None, remote_debug_port='0', data_center=None, ipformat=None, use_single_interface=False
             ) -> ScyllaNode:
    i = len(cluster.nodes) + 1

    ipprefix = cluster.ipprefix or ''

    if not ipformat:
        ipformat = ipprefix + "%d"

    binary = None
    if cluster.cassandra_version() >= '1.2':
        if use_single_interface:
            # Always leave 9042 and 9043 clear, in case someone defaults to adding
            # a node with those ports
            binary = (ipformat % 1, 9042 + 2 + (i * 2))
        else:
            binary = (ipformat % i, 9042)

    thrift = None
    if cluster.__class__ in (Cluster, DseCluster):
        if cluster.cassandra_version() < '4':
            thrift = (ipformat % i, 9160)
    else:
        thrift = (ipformat % i, 9160)

    storage_interface = ((ipformat % i), 7000)

    node = cluster.create_node(name='node%s' % i,
                               auto_bootstrap=bootstrap,
                               thrift_interface=thrift,
                               storage_interface=storage_interface,
                               jmx_port=str(cluster.get_node_jmx_port(i)),
                               remote_debug_port=remote_debug_port,
                               initial_token=token,
                               binary_interface=binary,
                               )
    cluster.add(node, not bootstrap, data_center=data_center)
    return node


def run_rest_api(run_on_node: ScyllaNode, cmd, api_method: str = 'post'):
    """
    :param api_method: post/get
    :param run_on_node: node to send the REST API command.
    :param cmd: api command to execute.
    :return: api-command-request result
    """
    cmd_prefix = f"http://{run_on_node.address()}:10000"
    full_cmd = cmd_prefix + cmd
    api_method = api_method.lower()
    logger.debug(f"Send restful api: {full_cmd}: api_method={api_method}")
    if api_method == 'post':
        result = requests.post(full_cmd)
    elif api_method == 'get':
        result = requests.get(full_cmd)
    elif api_method == 'delete':
        result = requests.delete(full_cmd)
    else:
        raise Exception(f"Unknown request API method: {api_method}")
    result.raise_for_status()
    result_json = result.json() if result.text else '{}'
    logger.debug(f"API result: {result_json}")
    return result


def wait_for_compactions(node) -> None:
    pattern = re.compile("pending tasks: 0")
    while True:
        output, err = node.nodetool("compactionstats", capture_output=True)
        if pattern.search(output):
            break


def parallel_nodetool(nodes, cmd, capture_output=True, wait=True, timeout=300):
    if not isinstance(nodes, list):
        nodes = [nodes]
    if isinstance(nodes[0], ScyllaNode) and nodes[0].scylla_mode() == 'debug':
        timeout *= 3
    with ThreadPoolExecutor(max_workers=len(nodes)) as pool:
        threads = []
        for node in nodes:
            threads.append(pool.submit(node.nodetool, cmd=cmd, capture_output=capture_output, wait=wait))
        results = {}
        for i in range(len(threads)):
            results[nodes[i].name] = threads[i].result(timeout=timeout)
        return results


class Group0Member(TypedDict):
    host_id: str
    is_voter: bool


class TokenRingMember(TypedDict):
    host_ip: str
    host_id: str


def get_token_ring_members(node: ScyllaNode) -> list[TokenRingMember]:
    token_ring_members = []
    result = run_rest_api(run_on_node=node, cmd="/storage_service/host_id", api_method="get")
    if not result.text:
        return []

    for member in result.json():
        token_ring_members.append({"host_ip": member.get("key"), "host_id": member.get("value")})

    return token_ring_members


def get_group0_members(node: ScyllaNode) -> list[Group0Member]:
    def _parse_cqlsh_output(output: tuple[str, str]) -> list[str]:
        result = []
        stdout, stderr = output
        if stderr:
            return []

        for line in stdout.strip().split("\n"):
            if not line.strip():
                break
            result.append(line.strip())

        if not result and len(result) < 2:
            return []
        return result[2:]

    group0_members = []
    output = node.run_cqlsh("select value from system.scylla_local where key = 'raft_group0_id'",
                            return_output=True)
    result = _parse_cqlsh_output(output)
    if not result:
        return []
    raft_group0_id = result[0]

    output = node.run_cqlsh(f"select server_id, can_vote from system.raft_state where group_id = {raft_group0_id} and disposition = 'CURRENT'",
                            return_output=True)
    result = _parse_cqlsh_output(output)
    if not result:
        return []
    for line in result:
        server_id, can_vote = line.split("|")
        can_vote = True if can_vote.strip() == "True" else False
        group0_members.append({"host_id": server_id.strip(), "is_voter": can_vote})

    return group0_members
