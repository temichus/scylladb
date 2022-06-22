import logging
import re

import requests
from ccmlib.cluster import Cluster
from ccmlib.dse_cluster import DseCluster
from ccmlib.scylla_node import ScyllaNode

logger = logging.getLogger(__name__)


def new_node(cluster, bootstrap=True, token=None, remote_debug_port='0', data_center=None, ipformat=None, use_single_interface=False):
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
    logger.debug(f"Send restful api: {full_cmd}")
    if api_method == 'post':
        result = requests.post(full_cmd)
    elif api_method == 'get':
        result = requests.get(full_cmd)
    else:
        raise Exception(f"Unknown request API method: {api_method}")
    result.raise_for_status()
    logger.debug(f"API result: {result.json()}")
    return result


def wait_for_compactions(node) -> None:
    pattern = re.compile("pending tasks: 0")
    while True:
        output, err = node.nodetool("compactionstats", capture_output=True)
        if pattern.search(output):
            break
