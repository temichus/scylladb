from dtest import Tester
import re
import os
from tools import no_vnodes
from tools import new_node, debug
import yaml
import time
from unittest import skip
from threading import Thread


def wait(delay=2):
    time.sleep(delay)


class TestNodetool(Tester):
    def __init__(self, *args, **kwargs):
        kwargs['cluster_options'] = {'start_rpc': 'true'}
        super(TestNodetool, self).__init__(*args, **kwargs)

    @staticmethod
    def _to_cfstats(out):
        p = re.compile('^\s*([^:]+)\s*:\s*(\S.*)\s*$')
        res = {}
        ks = None
        obj = {}
        for l in [s.strip() for s in out.splitlines()]:
            m = p.match(l)
            if m:
                if m.group(1) == 'Table':
                    if ks is None:
                        ks = obj
                        ks["tables"] = {}
                    else:
                        if "Table" in obj:
                            ks["tables"][obj["Table"]] = obj
                    obj = {}
                obj[m.group(1).strip()] = m.group(2).strip()
            else:
                if l.find("----------------") >= 0:
                    if obj != {}:
                        if "Table" in obj:
                            ks["tables"][obj["Table"]] = obj
                            obj = {}
                    res[ks["Keyspace"]] = ks
                    ks = None
        return res

    def assertEqual(self, expect, second, msg=None):
        if msg is None:
            super(TestNodetool, self).assertEqual(expect, second, "Expecting " + str(expect) + " got " + str(second))
        else:
            super(TestNodetool, self).assertEqual(expect, second, msg + " (Expecting " + str(expect) + " got " + str(second) + ")")

    def assertMapGreatEqual(self, container, key, val, msg=None):
        if msg is None:
            m = ""
        else:
            m = msg + " "
        m = m + key + " is " + container[key] + " not >=" + str(val)
        self.assertIn(key, container, m)
        self.assertGreaterEqual(float(container[key]), val, m)

    def assertMapLess(self, container, key, val, msg=None):
        if msg is None:
            m = ""
        else:
            m = msg + " "
        m = m + key + " is " + str(container[key]) + " not <" + str(val)
        self.assertIn(key, container, m)
        self.assertLess(float(container[key]), val, m)

    def assertMapLessEqual(self, container, key, val, msg=None):
        if msg is None:
            m = ""
        else:
            m = msg + " "
        m = m + key + " is " + container[key] + " not >=" + str(val)
        self.assertIn(key, container, m)
        self.assertLessEqual(float(container[key]), val, m)

    def assertMapEqual(self, container, key, val, msg=None):
        if msg is None:
            m = key
        else:
            m = msg + " " + key
        self.assertIn(key, container, m)
        try:
            self.assertEqual(val, float(container[key]), m)
        except (TypeError, ValueError):
            self.assertEqual(val, container[key], m)

    def assertIP(self, addr, msg=None):
        if msg is None:
            msg = ""
        self.assertRegexpMatches(addr, "\d+\.\d+\.\d+\.\d+", msg + ": bad ip format")

    def assertMapBetween(self, container, key, a, b, msg=None):
        if msg is None:
            m = key + "=" + str(container[key]) + " not between " + str(a) + " and " + str(b)
        else:
            m = msg + " " + key + "=" + str(container[key]) + " not between " + str(a) + " and " + str(b)
        self.assertIn(key, container, m)
        v = float(container[key])
        self.assertLessEqual(a, v, m)
        self.assertLessEqual(v, b, m)

    @staticmethod
    def _parse_time(out):
        p = re.compile('^\s*([\d\.]+)\s*(\S+)\s*$')
        m = p.match(out)
        if m:
            if m.group(1) == 'NaN':
                return -1
            v = float(m.group(1))
            if m.group(2) == "s":
                v = v * 1000
            return v
        return float(out)

    @staticmethod
    def _list2status(lst):
        heads = ["status", "address", "load", "tokens", "owns", "host id", "rack"]
        res = {}
        for i in range(len(heads)):
            res[heads[i]] = lst[i]
        return res

    @staticmethod
    def _tonum(val):
        """ translate a string to a num if possible
        """
        try:
            return float(val)
        except:
            return val

    def nodetool_status(self, node, keyspace=""):
        res = {}
        out = node.nodetool("status " + keyspace, True)[0]
        m = re.findall('Datacenter: ([^\s]+)', out, re.MULTILINE)
        if m:
            res['Datacenter'] = m[0]
        m = re.findall('^([UDNLJM]+)\s+([\d\.]+)\s+([^\s]+\s+[^\s]+)\s+([^\s]+)\s+([^\s]+)(?:\s[^\s]{2})?\s+([^\s]+)\s+([^\s]+)\s*$', out, re.MULTILINE)
        res["nodes"] = [self._list2status(s) for s in m]
        return res

    def nodetool_info(self, node):
        res = {}
        out = node.nodetool("info", True)[0]
        m = re.findall('^\s*([^\s][^:]*[^:\s])\s*:\s+(.*)\s*$', out,
                       re.MULTILINE)
        for k in m:
            sp = k[1].split(',')
            if len(sp) == 1:
                res[k[0]] = k[1]
            else:
                res[k[0]] = {}
                for v in sp:
                    mt = re.match("^\s*([\d\.]+)\s+(.*)\s*$", v)
                    if mt:
                        res[k[0]][mt.group(2).strip()] = float(
                            mt.group(1).strip())
                    else:
                        mt = re.match("^\s*([^\d]+)\s+([\d\.]+)\s*$", v)
                        if mt:
                            res[k[0]][mt.group(1).strip()] = float(
                                mt.group(2).strip())
        return res

    def decommission_test(self):
        """Ensure that nodetool decomission works
        starting two node cluster
        verify that nodetool status return two nodes
        run nodetool decomission and verify that that only
        one node remains
        """
        cluster = self.cluster
        cluster.populate(2).start(wait_for_binary_proto=True)
        [node1, node2] = cluster.nodelist()
        status = self.nodetool_status(node1)
        self.assertEqual(2, len(status["nodes"]), "wrong number of nodes")
        node2.nodetool("decommission")
        status = self.nodetool_status(node1)
        self.assertEqual(1, len(status["nodes"]), "wrong number of nodes")

    def cfstats_test(self):
        """Ensure that cfstats action works successfully.
        it runs a load with write, check some of the parameters
        and then runs a load with mixed and check again
        """
        cluster = self.cluster
        cluster.populate(1).start(wait_for_binary_proto=True)
        [node1] = cluster.nodelist()
        cursor = self.patient_cql_connection(node1)

        strs = self.stress_write(node1, times=1000)
        node1.flush()

        table_name = 'standard1'
        o = node1.nodetool('cfstats', True)[0]
        output = TestNodetool._to_cfstats(o)
        self.assertLessEqual(2, len(output), "wrong number of keyspaces found " + str(output.keys()))
        self.assertIn("system", output, "System keyspace is missing")
        self.assertIn("keyspace1", output, "keyspace1 keyspace is missing")
        self.assertIn(table_name, output["keyspace1"]["tables"], table_name + "table is missing")

        output = TestNodetool._to_cfstats(node1.nodetool('cfstats keyspace1', True)[0])
        self.assertEqual(1, len(output), "wrong number of keyspaces found " + str(output.keys()))
        ks = output["keyspace1"]
        self.assertMapEqual(ks, "Write Count", 1000)
        self.assertMapEqual(ks, "Read Count", 0)
        self.assertEqual(1000, int(ks["Write Count"]) + int(ks["Read Count"]))
        table = ks["tables"][table_name]
        self.assertMapEqual(table, "SSTable count", 1)
        self.assertMapEqual(table, "Number of keys (estimate)", 1000)
        self.assertMapGreatEqual(table, "Memtable cell count", 0)

        strs = self.stress_mixed(node1, times=1000)
        output = self._to_cfstats(node1.nodetool('cfstats keyspace1', True)[0])
        ks = output["keyspace1"]
        table = ks["tables"][table_name]

        self.assertMapEqual(table, "Space used by snapshots (total)", 0)
        self.assertMapGreatEqual(table, "Off heap memory used (total)", float(table["Bloom filter off heap memory used"]) + float(table["Index summary off heap memory used"]))
        self.assertMapEqual(table, "SSTable Compression Ratio", 0)
        self.assertGreaterEqual(strs["latency mean:read"], TestNodetool._parse_time(ks["Read Latency"]))
        self.assertGreaterEqual(strs["latency mean:write"], TestNodetool._parse_time(ks["Write Latency"]))
        self.assertMapEqual(table, "Memtable switch count", 1)
        self.assertMapEqual(table, "Local read count", int(ks["Read Count"]))
        self.assertGreaterEqual(TestNodetool._parse_time(ks["Read Latency"]), TestNodetool._parse_time(table["Local read latency"]))
        self.assertMapEqual(table, "Local write count", int(ks["Write Count"]))
        self.assertGreaterEqual(TestNodetool._parse_time(ks["Write Latency"]), TestNodetool._parse_time(table["Local write latency"]))
        self.assertMapEqual(table, "Pending flushes", 0)
        self.assertMapGreatEqual(table, "Bloom filter false positives", 0)
        self.assertMapGreatEqual(table, "Bloom filter false ratio", 0)
        self.assertMapGreatEqual(table, "Bloom filter space used", 0)
        self.assertMapGreatEqual(table, "Bloom filter off heap memory used", 100000)
        self.assertMapGreatEqual(table, "Index summary off heap memory used", 500)
        self.assertMapEqual(table, "Compression metadata off heap memory used", 0)
        self.assertMapEqual(table, "Compacted partition minimum bytes", 259)
        self.assertMapEqual(table, "Compacted partition maximum bytes", 310)
        self.assertMapEqual(table, "Compacted partition mean bytes", 310)
        self.assertMapEqual(table, "Average live cells per slice (last five minutes)", 0)
        self.assertMapEqual(table, "Maximum live cells per slice (last five minutes)", 0)
        self.assertMapEqual(table, "Average tombstones per slice (last five minutes)", 0)
        self.assertMapEqual(table, "Maximum tombstones per slice (last five minutes)", 0)
        self.assertMapLessEqual(table, "Memtable data size", float(table["Memtable off heap memory used"]))

    def verify_snapshot(self, node1, ks, snapshot, exists=True):
        out = node1.nodetool("listsnapshots", True)[0]
        m = re.findall(snapshot + "\s+" + ks, out, re.MULTILINE)
        if exists:
            self.assertTrue(m, "snapshot " + snapshot + " is missing in keyspace " + ks)
        else:
            self.assertFalse(m, "unexpected snapshot " + snapshot + " found in keyspace " + ks)

    def global_snapshot_test(self):
        """ Test a global snapshot, by loading a system
        creating a snapshot, checking that it exists
        remove it and checking that it does not exists
        """
        [node1] = self.run_cluster(nodes=1)
        self.stress_write(node1, 1000)
        self.assertEqual(0, len(self.listsnapshots(node1)), "unexpected snapshot found")
        out = node1.nodetool("snapshot", True)[0]
        m = re.findall(r"Snapshot directory:\s+(\d+)", out, re.MULTILINE)
        self.assertTrue(m, "No directory found in node snapshot command: '" + out + "'")
        snapshot = m[0]
        data_dir = os.path.join(node1.get_path(), "data")
        keyspaces = [f for f in os.listdir(data_dir) if os.path.isdir(os.path.join(data_dir, f))]
        self.assertEqual(2, len(keyspaces), "wrong number of directories in the data dir")
        for ks in keyspaces:
            keyspace_dir = os.path.join(data_dir, ks)
            column_families = [os.path.join(keyspace_dir, f) for f in os.listdir(keyspace_dir) if os.path.isdir(os.path.join(keyspace_dir, f))]
            for cf in column_families:
                self.assertTrue(os.path.isdir(os.path.join(cf, "snapshots", snapshot)), "Missing snapshot dir under ks=" + ks + " cf " + cf)
                self.assertIn("manifest.json", os.listdir(os.path.join(cf, "snapshots", snapshot)), "Missing manifest.json in " + os.path.join(cf, "snapshots", snapshot))
        self.verify_snapshot(node1, "keyspace1", snapshot)
        self.verify_snapshot(node1, "system", snapshot)
        node1.nodetool("clearsnapshot")
        self.verify_snapshot(node1, "keyspace1", snapshot, exists=False)

    def test_snapshot(self, tag, keyspace=None, kc=None, column_family=None):
        """ Test a global snapshot, by loading a system
        creating a snapshot, checking that it exists
        remove it and checking that it does not exists
        """
        [node1] = self.run_cluster(nodes=1)
        self.stress_write(node1, 1000)
        self.assertEqual(0, len(self.listsnapshots(node1)), "unexpected snapshot found")
        cmd = "snapshot -t " + tag
        if kc:
            cmd = cmd + " -kc " + kc
        if column_family:
            cmd = cmd + " -cf " + column_family
        if keyspace:
            cmd = cmd + " " + keyspace
        out = node1.nodetool(cmd, True)[0]
        m = re.findall(r"Snapshot directory:\s+([^\s]+)", out, re.MULTILINE)
        self.assertTrue(m, "No directory found in node snapshot command: '" + out + "'")
        snapshot = m[0]
        self.assertEqual(tag, snapshot, "wrong directory found in node snapshot command: '" + out + "'")
        data_dir = os.path.join(node1.get_path(), "data")
        keyspaces = [f for f in os.listdir(data_dir) if os.path.isdir(os.path.join(data_dir, f))]
        self.assertEqual(2, len(keyspaces), "wrong number of directories in the data dir")
        if kc:
            brk = kc.split('.')
            keyspace = brk[0]
            column_family = brk[1]
        for ks in keyspaces:
            keyspace_dir = os.path.join(data_dir, ks)
            column_families = [f for f in os.listdir(keyspace_dir) if os.path.isdir(os.path.join(keyspace_dir, f))]
            for c in column_families:
                cf = os.path.join(keyspace_dir, c)
                if not keyspace or (keyspace == ks and (not column_family or c.startswith(column_family))):
                    self.assertTrue(os.path.isdir(os.path.join(cf, "snapshots", snapshot)), "Missing snapshot dir under ks=" + ks + " cf " + cf)
                    self.assertIn("manifest.json", os.listdir(os.path.join(cf, "snapshots", snapshot)), "Missing manifest.json in " + os.path.join(cf, "snapshots", snapshot))
                else:
                    self.assertFalse(os.path.isdir(os.path.join(cf, "snapshots", snapshot)), "Snapshot dir found under wrong ks=" + ks + " cf " + cf)

        if keyspace:
            self.verify_snapshot(node1, keyspace, snapshot)
        else:
            self.verify_snapshot(node1, "keyspace1", snapshot)
            self.verify_snapshot(node1, "system", snapshot)
        node1.nodetool("clearsnapshot -t" + tag)
        self.verify_snapshot(node1, "keyspace1", snapshot, exists=False)

    def snapshot_tag_test(self):
        self.test_snapshot("snaptag")

    def snapshot_tag_keyspace_test(self):
        self.test_snapshot("snaptag", keyspace="keyspace1")

    @skip("#1133")
    def snapshot_tag_keyspace_cf_test(self):
        self.test_snapshot("snaptag", keyspace="system", column_family="schema_columnfamilies")

    def snapshot_tag_kc_test(self):
        self.test_snapshot("snaptag", kc="system.schema_columnfamilies")

    @staticmethod
    def _list2dic(lst, heads):
        res = {}
        for index, attribute in enumerate(heads):
            res[attribute] = lst[index].strip()
        return res

    @staticmethod
    def _list2ring(lst):
        return TestNodetool._list2dic(lst, ["address", "rack", "status", "state", "load", "unit", "owns", "token"])

    def get_ring(self, node):
        out = node.nodetool("ring", True)[0]
        m = re.findall("^\s*([\d\.]+)\s+([^\s]+)\s+([^\s]+)\s+([^\s]+)\s+([\d\.]+)\s+([^\s]+)\s+([^\s]+)\s+([^\s].*)\s*$", out, re.MULTILINE)
        return [self._list2ring(r) for r in m]

    def global_create_after_clean(self):
        """ Test that after a clean
        it is possible to create an additional snapshot
        """
        cluster = self.cluster
        cluster.populate(1).start(wait_for_binary_proto=True)
        [node1] = cluster.nodelist()
        cursor = self.patient_cql_connection(node1)
        strs = self.stress_write(node1, 1000)
        out = node1.nodetool("snapshot", True)[0]
        m = re.findall(r"Snapshot directory:\s+(\d+)", out, re.MULTILINE)
        snapshot = m[0]
        self.assertTrue(m, "No directory found in node snapshot command: '" + out + "'")
        self.verify_snapshot(node1, "keyspace1", snapshot)
        node1.nodetool("clearsnapshot")
        self.verify_snapshot(node1, "keyspace1", snapshot, exists=False)
        out = node1.nodetool("snapshot", True)[0]
        m = re.findall(r"Snapshot directory:\s+(\d+)", out, re.MULTILINE)
        snapshot = m[0]
        self.verify_snapshot(node1, "keyspace1", snapshot)

    def _compactionhistory_entry(self, lst):
        res = TestNodetool._list2dic(lst, ["id", "keyspace_name", "columnfamily_name", "compacted_at", "bytes_in", "bytes_out", "rows_merged"])
        self._verify_compaction_history(res)
        return res;

    def compactionhistory(self, node):
        out = node.nodetool('compactionhistory', True)[0]
        merged = re.findall("^\s*([\d\-abcdef]+)\s+([^\s]+)\s+([^\s]+)\s+(\d+)\s+(\d+)\s+(\d+)\s+([^\s]+)?\s*$", out, re.MULTILINE)
        res = {}
        res["merged"] = [self._compactionhistory_entry(m) for m in merged]

    def _verify_compaction_history(self, cpc):
        self.assertRegexpMatches(cpc["id"], "[\d\-abcdef]+")
        self.assertRegexpMatches(cpc["keyspace_name"], "[^\s]+")
        self.assertRegexpMatches(cpc["columnfamily_name"], "[^\s]+")
        self.assertRegexpMatches(cpc["compacted_at"], "\d+")
        self.assertRegexpMatches(cpc["bytes_in"], "\d+")
        self.assertRegexpMatches(cpc["bytes_out"], "\d+")
# Fail testing awaits #1097
#        self.assertNotEqual(cpc["rows_merged"], "", "row merged information is missing")
#        self.assertRegexpMatches(cpc["rows_merged"], "\{\d+,\d+\}")

    def _compact(self, keyspace):
        cluster = self.cluster
        cluster.populate(1).start(wait_for_binary_proto=True)
        [node1] = cluster.nodelist()
        cursor = self.patient_cql_connection(node1)

        self.stress_write(node1, times=1000, opt=["-pop seq=1..1000"])
        node1.nodetool('flush')
        self.stress_write(node1, times=1000, opt=["-pop seq=1..1000"])
        node1.nodetool('flush')
        output = self._to_cfstats(node1.nodetool('cfstats keyspace1.standard1', True)[0])
        self.assertIn("keyspace1", output, "Keyspace is missing")
        self.assertIn("tables", output["keyspace1"], "Keyspace has no tables")
        self.assertIn("standard1", output["keyspace1"]["tables"], "Column family standard1 is missing")
        table = output["keyspace1"]["tables"]["standard1"]
        self.assertMapGreatEqual(table, "SSTable count", 2)
        sstable = int(table["SSTable count"])
        self.compactionhistory(node1)
        node1.nodetool("compact" + keyspace)
        output = self._to_cfstats(node1.nodetool('cfstats keyspace1.standard1', True)[0])
        table = output["keyspace1"]["tables"]["standard1"]
        self.assertMapLess(table, "SSTable count", sstable)

    def general_compact_test(self):
        """ Test that the nodetool compact works by:
        starting a cluster.
        running a load.
        check the number of sstable
        run compact
        check that the number of sstable decrease
        """
        self._compact("")

    def specific_compact_test(self):
        """ Test that the nodetool compact for
        a keyspace works, by:
        starting a cluster.
        running a load.
        check the number of sstable
        run compact
        check that the number of sstable decrease
        """
        self._compact(" keyspace1 standard1")

    def _get_current_token(self, node):
        r = self.get_ring(node)
        return next(obj for obj in r if obj['address'] == node.network_interfaces['binary'][0])['token']

    @no_vnodes()
    def move_test(self):
        """ Test that nodetool move works by:
        start a cluster with a single token per node
        check the token of one node
        call move with a new token
        check that the token was changed to the new value
        """
        cluster = self.cluster
        cluster.populate(2).start(wait_for_binary_proto=True)
        node1 = cluster.nodelist()[0]
        token = self._get_current_token(node1)
        node1.nodetool("move 1000000", True)
        token1 = self._get_current_token(node1)
        self.assertNotEqual(token, token1, "nodetool move 1000000 did not change the token")
        self.assertEqual("1000000", token1, "nodetool move 1000000 change token to wrong value")

    def statusgossip(self, node=None):
        if node is None:
            node = self.cluster.nodelist()[0]
        return node.nodetool("statusgossip", True)[0]

    def _get_ring_entry(self, lst):
        heads = ["Address", "Rack", "Status", "State", "Load", "Owns", "Token"]
        res = {}
        for i in range(len(heads)):
            res[heads[i]] = lst[i]
        return res

    def nodetool_ring(self, node=None, keyspace=""):
        if node is None:
            node = self.cluster.nodelist()[0]
        out = node.nodetool("ring " + keyspace, True)[0]
        res = {}
        dc = re.findall("^\s*Datacenter: ([^\s]+)\s*$", out, re.MULTILINE)
        self.assertEqual(1, len(dc), "Failed searching for datacenter")
        res["datacenter"] = dc[0]
        tokens = re.findall("^\s*([\d\.]+)\s+([^\s]+)\s+([^\s]+)\s+([^\s]+)\s+([^\s]+)(?:\s[^\s]{2})?\s+([^\s]+)(?:\s[^\s]{2})?\s+(\-?[\d]+)\s*$", out, re.MULTILINE)
        res["tokens"] = [self._get_ring_entry(m) for m in tokens]
        return res

    def gossip_control_test(self):
        """
        Test the `nodetool disablegossip` and `nodetool enablegossip`.

        1) Start a cluster and check the gossip via nodetool info
        2) Disable gossip
        3) Check with nodetool info
        4) Enable gossip
        5) Check with nodetool info
        """
        cluster = self.cluster
        cluster.populate(1).start(wait_for_binary_proto=True)
        [node1] = cluster.nodelist()
        gossip = self.nodetool_info(node1)['Gossip active']
        self.assertEqual("true", gossip, "Gossip is not active")
        self.assertRegexpMatches(self.statusgossip(node1), "\s*running\s*", "wrong gossip status")
        node1.nodetool("disablegossip")
        gossip = self.nodetool_info(node1)['Gossip active']
        self.assertEqual("false", gossip, "Failed to disable gossip")
        self.assertRegexpMatches(self.statusgossip(node1), "\s*not running\s*", "wrong gossip status")
        node1.nodetool("enablegossip")
        gossip = self.nodetool_info(node1)['Gossip active']
        self.assertEqual("true", gossip, "Failed to re-enable gossip")
        self.assertRegexpMatches(self.statusgossip(node1), "\s*running\s*", "wrong gossip status")

    def _flush(self, flush_cmd):
        cluster = self.cluster
        cluster.populate(1).start(wait_for_binary_proto=True)
        [node1] = cluster.nodelist()
        self.stress_write(node1, 1)
        output = self._to_cfstats(node1.nodetool('cfstats keyspace1.standard1',
                                                 True)[0])
        ks = output["keyspace1"]
        table = ks["tables"]["standard1"]
        self.assertEqual("0", table["SSTable count"],
                         "SStable count should be 0")
        node1.nodetool("flush" + flush_cmd)
        table = self._to_cfstats(node1.nodetool('cfstats keyspace1.standard1',
                                                True)[0])["keyspace1"]["tables"]["standard1"]
        self.assertEqual("1", table["SSTable count"],
                         "SStable count should be 1")

    def _describering_val(self, v):
        vals = re.findall('^\s*start_token:(-?\d+), end_token:(-?\d+), endpoints:\[([\d\.,]+)\], rpc_endpoints:\[([\d\.,]+)\], endpoint_details:\[(.*)\]\s*$', v, re.MULTILINE)
        heads = ['start_token', 'end_token', 'endpoints', 'rpc_endpoints']
        res = {}
        self.assertTrue(vals, "wrong format of token range: " + v)
        for index, attribute in enumerate(heads):
            res[attribute] = vals[0][index].strip()
            res["details"] = [self._list2dic(d, ['host', 'datacenter', 'rack']) for d in
                              re.findall('EndpointDetails\(host:([\d\.,]+), datacenter:([^,]+), rack:([^\)]+)\),?', vals[0][4])]
        return res

    def describering(self, node, ks):
        out = node.nodetool('describering ' + ks, True)[0]
        m = re.findall('^\s*TokenRange\((.*)\)\s*$', out, re.MULTILINE)
        self.assertTrue(m, "no TokenRange() found in describering")
        return [self._describering_val(v) for v in m]

    def describering_test(self):
        """
        Test the `nodetool describering` command
        Starts a cluster run a load
        Check that the correct parameters in the keyspace
        """
        cluster = self.cluster
        cluster.populate(3, use_vnodes=True).start(wait_for_binary_proto=True)
        node = cluster.nodelist()[0]
        self.stress_write(node, 1000)
        res = self.describering(node, 'keyspace1')
        self.assertGreater(len(res), 100, "no describe ring data found")

    def _verify_ring_token(self, entry, msg):
        self.assertIP(entry["Address"], msg)
        self.assertRegexpMatches(entry["Rack"], "[a-z0-9]+", msg)
        self.assertIn(entry["Status"], ["Up", "Down"], msg)
        self.assertEqual("Normal", entry["State"], msg)
        if entry["Owns"] != "?":
            self.assertRegexpMatches(entry["Owns"], "[0-9\.]+", msg)
        self.assertRegexpMatches(entry["Token"], "\-?[0-9]+", msg)

    def check_ring(self, keyspace=""):
        self.run_cluster()
        node = self.cluster.nodelist()[0]
        self.stress_write(node, times=1000)
        ring = self.nodetool_ring(node, keyspace)
        self.assertMapEqual(ring, "datacenter", "datacenter1", "Wrong datacenter")
        self.assertEqual(512, len(ring["tokens"]), "wrong number of tokens found")
        for idx, val in enumerate(ring["tokens"]):
            self._verify_ring_token(val, "Formatting error in entry " + str(idx))
            if keyspace == "":
                self.assertEqual("?", val["Owns"], "unexpected own found")
            else:
                self.assertNotEqual("?", val["Owns"], "missing own information")

    def general_ring_test(self):
        self.check_ring()

    @skip ('#1057')
    def keyspace_ring_test(self):
        self.check_ring("keyspace1")

    def general_flush_test(self):
        """
        Test the `nodetool flush` command.

        1) Start a cluster, enter a single entry
        2) check the number of sstables is 0
        3) run flush
        4) check the number of sstable is 1
        """
        self._flush("")

    def keyspace_flush_test(self):
        """
        Test the `nodetool flush` command to flush keyspace1.

        1) Start a cluster, enter a single entry
        2) check the number of sstables is 0
        3) run flush
        4) check the number of sstable is 1
        """
        self._flush(" keyspace1")

    def keyspace_column_family_flush_test(self):
        """
        Test the `nodetool flush` command to flush keyspace1.standard1.

        1) Start a cluster, enter a single entry
        2) check the number of sstables is 0
        3) run flush
        4) check the number of sstable is 1
        """
        self._flush(" keyspace1 standard1")

    def _get_cfhistogram(self, node, ks, cf):
        out = node.nodetool("cfhistograms " + ks + " " + cf, True)[0]
        m = re.findall(r"^([^\/]+)\/(.*)\s+histograms\s*$", out, re.MULTILINE)
        res = {}
        if m:
            res["ks"] = m[0][0]
            res["cf"] = m[0][1]
        m = re.findall(r"^([^\s]+)\s+([\d\.]+)\s+([\d\.]+)\s+([\d\.]+)\s+([\d\.]+)\s+([\d\.]+)\s*$", out, re.MULTILINE)
        heads = ['Percentile', 'SSTables', 'Write Latency', 'Read Latency', 'Partition Size', 'Cell Count']
        res["vals"] = {}
        for val in m:
            if val:
                res["vals"][val[0]] = {}
                for index, attribute in enumerate(heads):
                    try:
                        res["vals"][val[0]][attribute] = float(val[index])
                    except:
                        res["vals"][val[0]][attribute] = val[index]
        return res

    def cfhistograms_test(self):
        """Test the nodetool cfhistograms
        run a write load
        test that the write values make sense and that
        the read value are zero.
        write mix load check that the read value make sense
        """
        cluster = self.cluster
        cluster.populate(2).start(wait_for_binary_proto=True)
        node = cluster.nodelist()[0]
        strs = self.stress_write(node, 10000)
        res = self._get_cfhistogram(node, "keyspace1", "standard1")
        self.assertMapEqual(res, "ks", "keyspace1", "wrong keysyapce")
        self.assertMapEqual(res, "cf", "standard1", "wrong column family")
        ltnc = strs['latency 99.9th percentile:write']
        for v in res["vals"]:
            self.assertMapEqual(res["vals"][v], "Read Latency", 0, "unexpected read latency")
            if float(ltnc) != 0.0:
                self.assertMapLess(res["vals"][v], "Write Latency", ltnc * 1000, "unexpected write latency")
        res = self._get_cfhistogram(node, "keyspace1", "standard1")
        for v in res["vals"]:
            self.assertMapEqual(res["vals"][v], "Read Latency", 0, "unexpected read latency")
            self.assertMapEqual(res["vals"][v], "Write Latency", 0, "unexpected write latency")
        strs = self.stress_mixed(node, 10000)
        res = self._get_cfhistogram(node, "keyspace1", "standard1")
        ltnc = strs['latency 99.9th percentile:read']
        if float(ltnc) == 0.0:
            return
        for v in res["vals"]:
            self.assertMapLess(res["vals"][v], "Read Latency", ltnc * 1000, "unexpected read latency")

    def describecluster(self, node):
        out = node.nodetool('describecluster', True)[0]
        return yaml.load(out.replace('\t', "  "))

    def describecluster_test(self):
        """Test the nodetool describecluster command
        """
        cluster = self.cluster
        cluster.populate(3).start(wait_for_binary_proto=True)
        node = cluster.nodelist()[0]
        res = self.describecluster(node)
        self.assertIn("Cluster Information", res)
        cluster = res["Cluster Information"]
        self.assertMapEqual(cluster, "Name", "test")
        self.assertMapEqual(cluster, "Partitioner", "org.apache.cassandra.dht.Murmur3Partitioner")
        self.assertIn("Snitch", cluster)
        self.assertTrue(cluster["Snitch"].startswith("org.apache.cassandra.locator."), "invalid snitch name:" + cluster["Snitch"])
        self.assertIn("Schema versions", cluster)
        schema = cluster["Schema versions"]
        for k in schema:
            self.assertEqual(3, len(schema[k]), "wrong schema version for " + k + " " + str(schema[k]))
        self.assertMapEqual(cluster, "Name", "test")

    def create_table(self, session, obj):
        """A helper function that creates a keyspace and tables
        """
        for ks in obj:
            cls = obj[ks]["class"] if "class" in obj[ks] else 'SimpleStrategy'
            rf = obj[ks]["rf"] if "rf" in obj[ks] else 1
            session.execute("CREATE KEYSPACE " + ks + " WITH replication = { 'class':'" + cls + "', 'replication_factor':" + str(rf) + "}")
            session.execute("USE " + ks)
            for table in obj[ks]["tables"]:
                t = obj[ks]["tables"][table]
                keys = reduce(lambda a, b: a + "," + b, [k + " " + t[k] for k in t.keys() if k != "key"])
                pk = t["key"]
                create_table = "CREATE TABLE " + table + " (" + keys + " ,PRIMARY KEY (" + pk + "))"
                session.execute(create_table)

    @staticmethod
    def _sql_val(val):
        try:
            return "'" + val + "'"
        except:
            return str(val)

    def populate_data(self, session, obj):
        """A helper function that populate data
        To an existing table
        """
        for ks in obj:
            session.execute("USE " + ks)
            for table in obj[ks]:
                t = obj[ks][table]
                for val in t:
                    ins = "INSERT INTO " + table + " ("
                    ins = ins + reduce(lambda a, b: a + "," + b, val.keys()) + ") VALUES ("
                    ins = ins + reduce(lambda a, b: a + "," + b, [self._sql_val(val[a]) for a in val.keys()]) + ")"
                    session.execute(ins)

    def getendpoints(self, node, ks, cf, value):
        return node.nodetool('getendpoints ' + ks + ' ' + cf + ' value', True)[0]

    def getendpoints_test(self):
        """Test the nodetool getendpoints command
        start a cluster
        Create a table with a value
        Use the nodetool to find the endpoint
        """
        cluster = self.cluster
        cluster.populate(3).start(wait_for_binary_proto=True)
        node = cluster.nodelist()[0]
        session = self.patient_cql_connection(node)
        self.create_table(session, {"ks1" : {"tables": {"tbl1" : {"col1": "int", "col2": "text", "key": "col1"}}
                                          }})
        self.populate_data(session, {"ks1": {"tbl1" : [{"col1":4, "col2": "abc"}
                                                       ]}})
        endpoint = self.getendpoints(node, "ks1", "tbl1", "4")
        self.assertTrue(endpoint.startswith("127.0.0"), "Invalid endpoint returned '" + endpoint + "'")

    def gossipinfo(self, node):
        """A helper function that return the
        gossipinfo as an object
        """
        out = node.nodetool('gossipinfo', True)[0]
        yml = re.sub(':([^\s])', r': \1', re.sub('  ', '    ', re.sub(r'/([\d\.]+)', r'\1:', out)))
        return yaml.load(yml)

    def gossipinfo_test(self):
        cluster = self.cluster
        cluster.populate(2).start(wait_for_binary_proto=True)
        node = cluster.nodelist()[0]
        gi = self.gossipinfo(node)
        self.assertEqual(2, len(gi), "wrong number of nodes")
        for k in gi:
            info = gi[k]
            self.assertIn("generation", info)
            self.assertIn("heartbeat", info)
            self.assertIn("STATUS", info)
            self.assertIn("HOST_ID", info)
            self.assertIn("RELEASE_VERSION", info)
            self.assertIn("SCHEMA", info)
            self.assertIn("NET_VERSION", info)
            self.assertIn("LOAD", info)
            self.assertIn("RACK", info)
            self.assertIn("RPC_ADDRESS", info)
            self.assertIn("DC", info)
#            self.assertIn("SEVERITY", info)

    def verify_info(self, node=None):
        if not node:
            node = self.cluster.nodelist()[0]
        ni = self.nodetool_info(node)
        self.assertIn("ID", ni, "ID is missing")
        self.assertMapEqual(ni, "Gossip active", "true")
        self.assertIn("Thrift active", ni)
        self.assertIn("Native Transport active", ni)
        uptime = int(ni["Uptime (seconds)"])
        self.assertIn("Load", ni, "Load is missing")
        self.assertIn("Generation No", ni, "Generation No is missing")
        self.assertIn("Heap Memory (MB)", ni, "Heap Memory")
        self.assertIn("Off Heap Memory (MB)", ni, "Off Heap Memory is missing")
        self.assertMapEqual(ni, "Data Center", "datacenter1")
        self.assertMapEqual(ni, "Rack", "rack1")
        self.assertMapEqual(ni, "Exceptions", 0)
        self.assertIn("Key Cache", ni)
        self.assertIn("Row Cache", ni)
        self.assertIn("Counter Cache", ni)
        self.assertIn("Token", ni)
        time.sleep(10)
        ni = self.nodetool_info(node)
        self.assertMapBetween(ni, "Uptime (seconds)", uptime + 10, uptime + 30)

    def verify_status(self, node=None):
        if node is None:
            node = self.cluster.nodelist()[0]
        self.nodetool_status(node)

    def _verify_status_node(self, n):
        for h in ["status", "address", "load", "tokens", "owns", "host id", "rack"]:
            self.assertIn(h, n, "node status missing " + h)
        self.assertRegexpMatches(n["status"], "[UD][NLJM]?", "Node status has wrong format")
        self.assertIP(n["address"], "Node ip address")
        self.assertRegexpMatches(n["load"], "\d+\.?\d*\s+[KM]B", "Node load has wrong format")
        if n["owns"] != "?":
            self.assertRegexpMatches(n["owns"], "\d+\.?\d*\s+[KM]B", "Node owns has wrong format")
        self.assertRegexpMatches(n["tokens"], "\d+", "Node token has wrong format")
        self.assertRegexpMatches(n["host id"], "[0-9abcdef\-]+", "Node host id has wrong tokens format")
        self.assertRegexpMatches(n["rack"], "[a-z0-9]+", "Node rack has wrong tokens format")

    @skip ('#1057')
    def status_test(self):
        """ Test the nodetool status command
        Starts two node cluster
        Run a small load
        Run nodetool status without parameters check the result
        Run nodetool status with keyspace and verify that result
        """
        self.run_cluster()
        node = self.cluster.nodelist()[0]
        self.stress_write(node)
        status = self.nodetool_status(node)
        self.assertEqual(2, len(status["nodes"]), "expecting 2 nodes got " + str(len(status["nodes"])))
        self.assertMapEqual(status, "Datacenter", "datacenter1")
        for n in status["nodes"]:
            self._verify_status_node(n)
            self.assertMapEqual(n, "owns", "?")
        status = self.nodetool_status(node, "keyspace1")
        self.assertEqual(2, len(status["nodes"]), "expecting 2 nodes got " + str(len(status["nodes"])))
        self.assertMapEqual(status, "Datacenter", "datacenter1")
        for n in status["nodes"]:
            self._verify_status_node(n)
            self.assertNotEqual(n['owns'], '?', 'owns size is missing')

    def verify_netstats(self, node=None):
        if node is None:
            node = self.cluster.nodelist()[0]
        self.netstats(node)

    def info_test(self):
        """Test the `nodetool info` command
        Starts a cluster and call nodetool info
        verify that the output is as expected
        it sleeps for 10 seconds and test again
        to see that the the uptime is correct
        """
        cluster = self.cluster
        cluster.populate(2).start(wait_for_binary_proto=True)
        node = cluster.nodelist()[0]
        self.verify_info(node)

    def netstats(self, node):
        out = node.nodetool('netstats', True)[0]
        lines = out.splitlines()
        res = {}
        m = re.match("Mode:\s+(.*)$", lines.pop(0))
        self.assertTrue(m, "Mode is missing in netstats")
        res["mode"] = m.group(1)
        bootstrap = lines.pop(0)
        if bootstrap != "Not sending any streams.":
            res["streams"] = []
        read_repair = False
        stream = None
        for l in lines:
            ip = re.match("^\s+/([\d\.]+)\s*$", l)
            strm = re.match("^\s+(\S+) (\d+) files, (\d+) bytes total. Already \S+ (\d+) files, (\d+) bytes total", l)
            command = re.match("Commands\s+([^\s]+)\s+(\d+)\s+(\d+)", l)
            responses = re.match("Responses\s+([^\s]+)\s+(\d+)\s+(\d+)", l)
            rxfile = re.match("\s+(\S+)\s+(\d+)/(\d+) bytes\((\d+)%\)\s+\S+\s+\S+\s+idx:0/([\d\.]+)", l)
            if l == "Read Repair Statistics:":
                read_repair = True
                if stream is not None:
                    res["streams"].append(stream)
                    stream = None
            elif l.startswith("Pool Name"):
                read_repair = False
            elif ip:
                if stream is not None:
                    res["streams"].append(stream)
                stream = {}
                stream["ip"] = ip.group(1)
            elif strm:
                stream["direction"] = strm.group(1)
                stream["files"] = strm.group(2)
                stream["total_bytes"] = strm.group(3)
                stream["progres"] = strm.group(4)
                stream["progres_bytes"] = strm.group(5)
            elif rxfile:
                file_info = {}
                file_info["name"] = rxfile.group(1)
                file_info["rx_file"] = rxfile.group(2)
                file_info["rx_out_of"] = rxfile.group(3)
                file_info["rx_percent"] = rxfile.group(4)
                file_info["rx_ip"] = rxfile.group(5)
                if "rx_files" not in stream:
                    stream["rx_files"] = {}
                stream["rx_files"][file_info["name"]] = file_info
            elif command:
                res["commands"] = {}
                res["commands"]["Active"] = self._tonum(command.group(1))
                res["commands"]["Pending"] = self._tonum(command.group(2))
                res["commands"]["Completed"] = self._tonum(command.group(3))
            elif responses:
                res["responses"] = {}
                res["responses"]["Active"] = self._tonum(responses.group(1))
                res["responses"]["Pending"] = self._tonum(responses.group(2))
                res["responses"]["Completed"] = self._tonum(responses.group(3))
            elif read_repair:
                rr = re.match("^(.*):\s*(\d+)\s*$", l)
                self.assertTrue(rr, "unexpected line in read repair")
                res[rr.group(1)] = self._tonum(rr.group(2))
            else:
                self.assertTrue(False, "unknown line in netstats" + l+ "\n" + out)
        if stream is not None:
            res["streams"].append(stream)
        return res

    def netstats_test(self):
        """Testwing the `nodetool netstats` command
        It starts a 2 node cluster load it.
        add a node and check the results.
        """
        cluster = self.cluster
        cluster.populate(2).start(wait_for_binary_proto=True)
        node = cluster.nodelist()[0]
        self.stress_write(node, times=1000000,  pop='seq=1..3000000000', opt=["-rate threads=10"])
        node2 = new_node(cluster)
        node2.start(wait_for_binary_proto=False)
        node2.watch_log_for('streaming')
        stats = self.netstats(node2)
        self.assertEquals(len(stats["streams"]), 2)

    def nodetool_version(self, node=None):
        if node is None:
            node = self.cluster.nodelist()[0]
        return node.nodetool('version', True)[0]

    def version_test(self):
        self.run_cluster(nodes=1)
        self.assertRegexpMatches(self.nodetool_version(), "ReleaseVersion: 2\.\d+\.\d+", "Wrong version")

    def run_cluster(self, nodes=2):
        cluster = self.cluster
        cluster.populate(nodes).start(wait_for_binary_proto=True)
        return cluster.nodelist()

    def add_node(self):
        cluster = self.cluster
        node2 = new_node(cluster)
        node2.start(wait_for_binary_proto=True)

    def time_func(self, func_info, paralel=True):
        """takes a function and a time limit
        it runs the function, verify when it's done
        that it didn't took too long
        if paralel is true will run on a thread
        func_info can include a delay, in that case it would wait before
        calling the function
        """
        if paralel:
            if "delay" in func_info:
                wait(func_info["delay"])
            else:
                wait(0.2)
            tr = Thread(target=self.time_func, args=[func_info, False])
            tr.start()
            return tr
        else:
            before = int(time.time())
            debug("starting " + func_info["func"].__name__)
            func_info["func"]()
            if "time" in func_info:
                self.assertLessEqual(int(time.time()) - before, func_info["time"])
            debug(func_info["func"].__name__ + " completed in " + str(int(time.time()) - before) + " seconds")
            return None

    def concurent_stress(self, node=None):
        if node is None:
            node = self.cluster.nodelist()[0]
        self.stress_write(node, times=1000000,  pop='seq=1..3000000000', opt=["-rate threads=10"])

    def repair(self, node=None):
        if node is None:
            node = self.cluster.nodelist()[0]
        node.nodetool('repair')

    def do_recurent(self, start, waits):
        if "recurent" not in start:
            return
        for rec in start["recurent"]:
            r = self.time_func(rec)
        if "block" in rec:
            r.join()
        else:
            waits.append(r)

    def concurnet_part(self, start):
        if "operations" not in start:
            return

        waits = []
        operations = []
        for ops in start["operations"]:
            tr = self.time_func(ops)
            if "block" in ops:
                if "recurent" in start:
                    while tr.is_alive():
                        self.do_recurent(start, waits)
                else:
                    tr.join()
            else:
                operations.append(tr)
        while len(filter(lambda a: a.is_alive(), operations)) > 0:
            self.do_recurent(start, waits)
            wait(20)
        for w in waits:
            if w is not None:
                w.join()

    def general_concurent(self, tst):
        """tst is an object of the form
        tst = [{
            "operations":[{"func": self.run_cluster}, {"func": self.concurent_stress, "delay":5}, {"func": self.repair, "time":300, "delay":1}],
            "recurent":[{"func": self.verify_info, "time":20, "delay":5} ]
        },
        {
            "operations":[{"func": self.concurent_stress, "delay":5}]
        },
        {
            "operations": [{"func": self.add_node, "time":300}, {"func": self.repair, "time":300}],
            "recurent":[ {"func": self.verify_info, "time":40, "delay": 5}, {"func": self.verify_status, "time":25}, {"func": self.verify_netstats, "time":26} ],
        }
        ]

        operation and recurent are list of objects
        {"func" the function name, "time": when present check the operation time,
        "delay": add a delay before running, "block" when present the operation block}

        each test can have multiple sections.
        Each section would start after all the operations in the previous section completed.
        """
        for op in tst:
            self.concurnet_part(op)

    def concurent_repair_test(self):
        tst = [{
            "operations":[{"func": self.run_cluster}, {"func": self.concurent_stress, "delay":5}, {"func": self.repair, "time":300, "delay":1}],
            "recurent":[{"func": self.verify_info, "time":20, "delay":5} ]
        },
        {
            "operations":[{"func": self.concurent_stress, "delay":5}]
        },
        {
            "operations": [{"func": self.add_node, "time":300}, {"func": self.repair, "time":300}],
            "recurent":[ {"func": self.verify_info, "time":40}, {"func": self.verify_status, "time":25}, {"func": self.verify_netstats, "time":26} ],
        }
        ]
        self.general_concurent(tst)

    def stress(self, node, opr, times=10000, duration=None, col=None, pop=None, opt=[]):
        cmd = [opr, 'cl=ALL']
        if duration:
            cmd += ['duration=' + duration]
        else:
            cmd += ['n=' + str(times)]
        if col:
            cmd += ["-col", "'" + col + "'"]
        if pop:
            cmd += ["-pop", pop]
        if opt:
            cmd += opt
        return node.stress_object(cmd)

    def stress_write(self, node, times=10000, duration=None, col=None, pop=None, opt=[]):
        return self.stress(node, 'write', times=times, duration=duration, col=col, pop=pop, opt=opt)

    def stress_mixed(self, node, times=10000, duration=None, col=None, pop=None, opt=[]):
        return self.stress(node, 'mixed', times=times, duration=duration, col=col, pop=pop, opt=opt)
