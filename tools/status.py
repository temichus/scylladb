
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
