
import logging
import time
import re

logger = logging.getLogger(__name__)


def verify_nodes_status(node, exp_statuses_list, keyspace=""):
    if exp_statuses_list and not isinstance(exp_statuses_list[0], list):
        exp_statuses_list = [exp_statuses_list]
    status = nodetool_status(node, keyspace)
    statuses = [s['status'] for s in status['nodes']]
    find_expected_status = False
    for exp_statuses in exp_statuses_list:
        if exp_statuses == statuses:
            find_expected_status = True
    assert find_expected_status, "found statuses: %s" % statuses


def wait_for_nodes_status(node, exp_statuses, keyspace="", timeout=90):
    timeout = time.time() + timeout
    while True:
        try:
            verify_nodes_status(node, exp_statuses, keyspace=keyspace)
            break
        except AssertionError:
            time.sleep(1)
            if time.time() > timeout:
                verify_nodes_status(node, exp_statuses, keyspace=keyspace)


def nodetool_status(node, keyspace=""):
    res = {}
    out = node.nodetool("status " + keyspace, True)[0]
    m = re.findall(r'Datacenter: ([^\s]+)', out, re.MULTILINE)
    if m:
        res['Datacenter'] = m[0]

    # example of nodetool output to parse:
    # DN  127.0.0.2  ?          256          ?       7c073ba1-ceac-447a-a098-c89c1dd379de  rack1
    # UN  127.0.0.1  1.08 MB    256          ?       7e496720-2bf4-4ece-89ad-af7dba3d7d6b  rack1

    m = re.finditer(
        r'^(?P<status>[UDNLJM]+)\s+'
        r'(?P<address>[\d\.]+)\s+'
        r'(?P<load>\?|[^\s]+\s+[^\s]+)\s+'
        r'(?P<tokens>[^\s]+)\s+'
        r'(?P<owns>[^\s]+)'
        r'(?:\s[^\s]{2})?\s+'
        r'(?P<host_id>[^\s]+)\s+'
        r'(?P<rack>[^\s]+)\s*',
        out, re.MULTILINE)

    # replace host_id with 'host id' so user of this function doesn't need to change
    res["nodes"] = [{k.replace('_', ' '): v for k, v in s.groupdict().items()} for s in m]
    return res


def nodetool_gossipinfo(node):
    """
    Parse gossipinfo output and put it into a python dict.

    Trailing slash on node ips is removed.

    :param output: 'nodetool gossip' stdout
    :returns: Dict with nodetool info. Example follows.
    {'127.0.0.1': {'DC': 'datacenter1',
                    'HOST_ID': 'bb821819-9049-4929-b7cc-7b2edf1eec10',
                    'LOAD': '128982',
                    'NET_VERSION': '0',
                    'RACK': 'rack1',
                    'RELEASE_VERSION': '2.1.8',
                    'RPC_ADDRESS': '127.0.0.1',
                    'SCHEMA': '2576e940-0936-3ff6-a12c-9c4ed9571175',
                    'STATUS': 'NORMAL,996695790724469087',
                    'generation': '1457611493',
                    'heartbeat': '118',
                    'X1': 'RANGE_TOMBSTONES,LARGE_PARTITIONS,COUNTERS',
                    'X2': 'system_traces.sessions_time_idx:0.000000;system_trac..'}}
    """
    output, error = node.nodetool('gossipinfo', capture_output=True)
    assert not error
    gossipinfo = {}
    current_node = None
    for line in output.splitlines():
        line = line.strip()
        try:
            if current_node and current_node not in gossipinfo:
                gossipinfo[current_node] = {}
            key, value = line.split(':', 1)
            gossipinfo[current_node].update({key: value})
        except:
            current_node = line[1:]
    return gossipinfo
