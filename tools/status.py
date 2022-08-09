
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
    m = re.findall(
        r'^([UDNLJM]+)\s+([\d\.]+)\s+([^\s]+\s+[^\s]+)\s+([^\s]+)\s+([^\s]+)(?:\s[^\s]{2})?\s+([^\s]+)\s+([^\s]+)\s*', out, re.MULTILINE)

    def _list2status(lst):
        heads = ["status", "address", "load", "tokens", "owns", "host id", "rack"]
        res = {}
        for i in range(len(heads)):
            res[heads[i]] = lst[i]
        return res

    res["nodes"] = [_list2status(s) for s in m]
    return res
