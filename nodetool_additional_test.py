import os
import re
import stat
import sys
import time
import shutil
from threading import Thread
from unittest import skip
from binascii import hexlify
from subprocess import run
import functools
import random
import io
from psutil import Process
from concurrent.futures import ThreadPoolExecutor

import yaml
from nose.plugins.attrib import attr
from ccmlib.node import NodetoolError

from dtest import Tester
from tools import debug
from tools import new_node
from tools import insert_c1c2
from tools import no_vnodes, rows_to_list, require


def randbytes(n):
    for _ in range(n):
        yield random.getrandbits(8)


@attr('dtest-full')
class TestNodetool(Tester):

    def __init__(self, *args, **kwargs):
        kwargs['cluster_options'] = {'start_rpc': 'true'}
        super(TestNodetool, self).__init__(*args, **kwargs)
        self.width = 160
        self.multi_dc_queries_method_list = [{"func": self.verify_info, "time": 60, "args": [None, 'dc1', 'RAC1']},
                                             {"func": self.verify_status, "time": 25}, {
                                                 "func": self.verify_netstats, "time": 40},
                                             {"func": self.verify_cfhistograms, "time": 25}, {
                                                 "func": self.verify_cfstats, "time": 25, "args": [None, "keyspace1"]},
                                             {"func": self.verify_describering, "time": 25}, {"func": self.verify_decribecluster, "time": 25}]
        self.queries_method_list = [{"func": self.verify_info, "time": 60},
                                    {"func": self.verify_status, "time": 25}, {
                                        "func": self.verify_netstats, "time": 40},
                                    {"func": self.verify_cfhistograms, "time": 25}, {
                                        "func": self.verify_cfstats, "time": 25, "args": [None, "keyspace1"]},
                                    {"func": self.verify_describering, "time": 25}, {"func": self.verify_decribecluster, "time": 25}]
        self.reserved_names = ['view_pending_updates']
        self.cluster_started = False

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
                k = m.group(1).strip()
                v = m.group(2).strip()
                obj[k] = v
                # origin 3.11 changes this metric name.
                # fix by double-map value
                if k == "Number of partitions (estimate)":
                    obj["Number of keys (estimate)"] = v
            else:
                if l.find("----------------") >= 0 and ks != None:
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
            super(TestNodetool, self).assertEqual(expect, second, msg +
                                                  " (Expecting " + str(expect) + " got " + str(second) + ")")

    def assertMapGreatEqual(self, container, key, val, msg=None):
        if msg is None:
            m = ""
        else:
            m = msg + " "
        m = m + key + " is " + str(container[key]) + " not >=" + str(val)
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
        # Remove time units from output, ex '1.1242845461978741E-4 ms'
        out = out.split()[0]
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
        debug(out)
        m = re.findall('Datacenter: ([^\s]+)', out, re.MULTILINE)
        if m:
            res['Datacenter'] = m[0]
        m = re.findall(
            '^([UDNLJM]+)\s+([\d\.]+)\s+([^\s]+\s+[^\s]+)\s+([^\s]+)\s+([^\s]+)(?:\s[^\s]{2})?\s+([^\s]+)\s+([^\s]+)\s*$', out, re.MULTILINE)
        res["nodes"] = sorted([self._list2status(s) for s in m], key=lambda s: s["address"])
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
        cluster.populate(2).start(wait_for_binary_proto=True, wait_other_notice=True)
        [node1, node2] = cluster.nodelist()
        status = self.nodetool_status(node1)
        self.assertEqual(2, len(status["nodes"]), "wrong number of nodes")
        node2.nodetool("decommission")
        status = self.nodetool_status(node1)
        self.assertEqual(1, len(status["nodes"]), "wrong number of nodes")

    def cfstats(self, node=None, ks=""):
        node = self.get_node(node)
        o = node.nodetool('cfstats ' + ks, True)[0]
        return TestNodetool._to_cfstats(o)

    def _verify_cfstats_cf(self, cf):
        self.assertIn("Table", cf, "Table is missing in column family")
        self.assertIn("SSTable count", cf, "SSTable count is missing in column family")
        self.assertIn("Space used (live)", cf, "Space used (live) is missing in column family")
        self.assertIn("Space used (total)", cf, "Space used (total) is missing in column family")
        self.assertIn("Space used by snapshots (total)", cf,
                      "Space used by snapshots (total) is missing in column family")
        self.assertIn("Off heap memory used (total)", cf, "Off heap memory used (total) is missing in column family")
        self.assertIn("SSTable Compression Ratio", cf, "SSTable Compression Ratio is missing in column family")
        self.assertIn("Number of keys (estimate)", cf, "Number of keys (estimate) is missing in column family")
        self.assertIn("Memtable cell count", cf, "Memtable cell count is missing in column family")
        self.assertIn("Memtable data size", cf, "Memtable data size is missing in column family")
        self.assertIn("Memtable off heap memory used", cf, "Memtable off heap memory used is missing in column family")
        self.assertIn("Memtable switch count", cf, "Memtable switch count is missing in column family")
        self.assertIn("Local read count", cf, "Local read count is missing in column family")
        self.assertIn("Local read latency", cf, "Local read latency is missing in column family")
        self.assertIn("Local write count", cf, "Local write count is missing in column family")
        self.assertIn("Local write latency", cf, "Local write latency is missing in column family")
        self.assertIn("Pending flushes", cf, "Pending flushes is missing in column family")
        self.assertIn("Bloom filter false positives", cf, "Bloom filter false positives is missing in column family")
        self.assertIn("Bloom filter false ratio", cf, "Bloom filter false ratio is missing in column family")
        self.assertIn("Bloom filter space used", cf, "Bloom filter space used is missing in column family")
        self.assertIn("Bloom filter off heap memory used", cf,
                      "Bloom filter off heap memory used is missing in column family")
        self.assertIn("Index summary off heap memory used", cf,
                      "Index summary off heap memory used is missing in column family")
        self.assertIn("Compression metadata off heap memory used", cf,
                      "Compression metadata off heap memory used is missing in column family")
        self.assertIn("Compacted partition minimum bytes", cf,
                      "Compacted partition minimum bytes is missing in column family")
        self.assertIn("Compacted partition maximum bytes", cf,
                      "Compacted partition maximum bytes is missing in column family")
        self.assertIn("Compacted partition mean bytes", cf,
                      "Compacted partition mean bytes is missing in column family")
        self.assertIn("Average live cells per slice (last five minutes)", cf,
                      "Average live cells per slice (last five minutes) is missing in column family")
        self.assertIn("Maximum live cells per slice (last five minutes)", cf,
                      "Maximum live cells per slice (last five minutes) is missing in column family")
        self.assertIn("Average tombstones per slice (last five minutes)", cf,
                      "Average tombstones per slice (last five minutes) is missing in column family")
        self.assertIn("Maximum tombstones per slice (last five minutes)", cf,
                      "Maximum tombstones per slice (last five minutes) is missing in column family")

    def verify_cfstats(self, node=None, ks=""):
        res = self.cfstats(node, ks)
        if ks == "":
            for k in res:
                for cf in res[k]["tables"]:
                    self._verify_cfstats_cf(res[k]["tables"][cf])
        else:
            self.assertEqual(1, len(res), "wrong number of keyspaces found " + str(res.keys()))
            self.assertIn(ks, res, "keyspace " + ks + " not found")
            for cf in res[ks]["tables"]:
                self._verify_cfstats_cf(res[ks]["tables"][cf])

    @attr('single_node')
    def cfstats_test(self):
        """Ensure that cfstats action works successfully.
        it runs a load with write, check some of the parameters
        and then runs a load with mixed and check again
        """
        cluster = self.cluster
        cluster.populate(1).start(wait_for_binary_proto=True)
        [node1] = cluster.nodelist()
        cursor = self.patient_cql_connection(node1)

        debug('Run stress write test')
        strs = self.stress_write(node1, times=1000, opt=['no-warmup'])
        node1.flush()

        debug('Run and verify cfstats')
        table_name = 'standard1'
        o = node1.nodetool('cfstats', True)[0]
        output = TestNodetool._to_cfstats(o)
        self.assertLessEqual(2, len(output), "wrong number of keyspaces found " + str(output.keys()))
        self.assertIn("system", output, "System keyspace is missing")
        self.assertIn("keyspace1", output, "keyspace1 keyspace is missing")
        self.assertIn(table_name, output["keyspace1"]["tables"], table_name + "table is missing")
        self.verify_cfstats()
        self.verify_cfstats(ks="keyspace1")
        output = TestNodetool._to_cfstats(node1.nodetool('cfstats keyspace1', True)[0])
        self.assertEqual(1, len(output), "wrong number of keyspaces found " + str(output.keys()))
        ks = output["keyspace1"]
        self.assertMapEqual(ks, "Write Count", 1000)
        self.assertMapEqual(ks, "Read Count", 0)
        self.assertEqual(1000, int(ks["Write Count"]) + int(ks["Read Count"]))
        table = ks["tables"][table_name]
        self.assertMapGreatEqual(table, "SSTable count", 1)
        self.assertMapEqual(table, "Number of keys (estimate)", 1000)
        self.assertMapGreatEqual(table, "Memtable cell count", 0)

        debug('Run stress mixed test')
        strs = self.stress_mixed(node1, times=1000)
        debug('Run and verify cfstats')
        output = self._to_cfstats(node1.nodetool('cfstats keyspace1', True)[0])
        ks = output["keyspace1"]
        table = ks["tables"][table_name]

        self.assertMapEqual(table, "Space used by snapshots (total)", 0)
        self.assertMapGreatEqual(table, "Off heap memory used (total)", float(
            table["Bloom filter off heap memory used"]) + float(table["Index summary off heap memory used"]))
        self.assertMapEqual(table, "SSTable Compression Ratio", 0)
        #self.assertGreaterEqual(strs["latency mean:read"], TestNodetool._parse_time(ks["Read Latency"]))
        #self.assertGreaterEqual(strs["latency mean:write"], TestNodetool._parse_time(ks["Write Latency"]))
        self.assertMapGreatEqual(table, "Memtable switch count", 1)
        self.assertMapEqual(table, "Local read count", int(ks["Read Count"]))
        #self.assertGreaterEqual(TestNodetool._parse_time(ks["Read Latency"]), TestNodetool._parse_time(table["Local read latency"]) - 0.1)
        self.assertMapEqual(table, "Local write count", int(ks["Write Count"]))
        #self.assertGreaterEqual(TestNodetool._parse_time(ks["Write Latency"]), TestNodetool._parse_time(table["Local write latency"]) - 0.1)
        self.assertMapEqual(table, "Pending flushes", 0)
        self.assertMapGreatEqual(table, "Bloom filter false positives", 0)
        self.assertMapGreatEqual(table, "Bloom filter false ratio", 0)
        self.assertMapGreatEqual(table, "Bloom filter space used", 0)
        # bloom filter will allocate something between 512 and 128kB, depending on the keys we insert and on
        # the internals of the bitmap. With 128kB memory we can hold at most 16k elements.
        self.assertMapGreatEqual(table, "Bloom filter off heap memory used", 512)
        self.assertMapLessEqual(table, "Bloom filter off heap memory used", 128 * 1024)
        self.assertMapGreatEqual(table, "Index summary off heap memory used", 500)
        self.assertMapEqual(table, "Compression metadata off heap memory used", 0)
        self.assertMapLessEqual(table, "Compacted partition minimum bytes", 259)
        self.assertMapLessEqual(table, "Compacted partition maximum bytes", 310)
        self.assertMapLessEqual(table, "Compacted partition mean bytes", 310)
        self.assertMapEqual(table, "Average live cells per slice (last five minutes)", 0)
        self.assertMapEqual(table, "Maximum live cells per slice (last five minutes)", 0)
        self.assertMapEqual(table, "Average tombstones per slice (last five minutes)", 0)
        self.assertMapEqual(table, "Maximum tombstones per slice (last five minutes)", 0)
        self.assertMapLessEqual(table, "Memtable data size", float(table["Memtable off heap memory used"]))

    def _snapshot_entry(self, lst):
        return self._list2dic(lst, ["name", "keyspace", "Column family", "True size", "Size on disk"])

    def listsnapshots(self, node):
        out = node.nodetool("listsnapshots", True)[0]
        m = re.findall("^\s*([^\s]+)\s+([^\s]+)\s+([^\s]+)\s+([\d]+\s[^\s]+)\s+([\d]+\s[^\s]+)\s*$", out, re.MULTILINE)
        return [self._snapshot_entry(lst) for lst in m]

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
        keyspaces = [f for f in os.listdir(data_dir) if os.path.isdir(
            os.path.join(data_dir, f)) and f not in self.reserved_names]
        self.assertEqual(6, len(keyspaces), "wrong number of directories in the data dir")
        for ks in keyspaces:
            keyspace_dir = os.path.join(data_dir, ks)
            column_families = [os.path.join(keyspace_dir, f) for f in os.listdir(
                keyspace_dir) if os.path.isdir(os.path.join(keyspace_dir, f))]
            for cf in column_families:
                if ks == "system" and "schema" in cf:
                    continue
                self.assertTrue(os.path.isdir(os.path.join(cf, "snapshots", snapshot)),
                                "Missing snapshot dir under ks=" + ks + " cf " + cf)
                self.assertIn("manifest.json", os.listdir(os.path.join(cf, "snapshots", snapshot)),
                              "Missing manifest.json in " + os.path.join(cf, "snapshots", snapshot))
        self.verify_snapshot(node1, "keyspace1", snapshot)
        self.verify_snapshot(node1, "system", snapshot)
        node1.nodetool("clearsnapshot")
        self.verify_snapshot(node1, "keyspace1", snapshot, exists=False)

    def tst_snapshot(self, tag, keyspace=None, kc=None, column_family=None):
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
        keyspaces = [f for f in os.listdir(data_dir) if os.path.isdir(
            os.path.join(data_dir, f)) and f not in self.reserved_names]
        self.assertEqual(6, len(keyspaces), "wrong number of directories in the data dir")
        if kc:
            brk = kc.split('.')
            keyspace = brk[0]
            column_family = brk[1]
        for ks in keyspaces:
            keyspace_dir = os.path.join(data_dir, ks)
            column_families = [f for f in os.listdir(keyspace_dir) if os.path.isdir(os.path.join(keyspace_dir, f))]
            for c in column_families:
                cf = os.path.join(keyspace_dir, c)
                if ks == "system" and "schema" in cf:
                    continue
                if not keyspace or (keyspace == ks and (not column_family or c.startswith(column_family))):
                    self.assertTrue(os.path.isdir(os.path.join(cf, "snapshots", snapshot)),
                                    "Missing snapshot dir under ks=" + ks + " cf " + cf)
                    self.assertIn("manifest.json", os.listdir(os.path.join(cf, "snapshots", snapshot)),
                                  "Missing manifest.json in " + os.path.join(cf, "snapshots", snapshot))
                else:
                    self.assertFalse(os.path.isdir(os.path.join(cf, "snapshots", snapshot)),
                                     "Snapshot dir found under wrong ks=" + ks + " cf " + cf)

        if keyspace:
            self.verify_snapshot(node1, keyspace, snapshot)
        else:
            self.verify_snapshot(node1, "keyspace1", snapshot)
            self.verify_snapshot(node1, "system", snapshot)
        node1.nodetool("clearsnapshot -t" + tag)
        self.verify_snapshot(node1, "keyspace1", snapshot, exists=False)

    def snapshot_tag_test(self):
        self.tst_snapshot("snaptag")

    def snapshot_tag_keyspace_test(self):
        self.tst_snapshot("snaptag", keyspace="keyspace1")

    def snapshot_tag_keyspace_cf_test(self):
        self.tst_snapshot("snaptag", keyspace="system_schema", column_family="tables")

    @skip("#1133")
    def snapshot_tag_kc_test(self):
        self.tst_snapshot("snaptag", kc="system_schema.tables")

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
        m = re.findall(
            "^\s*([\d\.]+)\s+([^\s]+)\s+([^\s]+)\s+([^\s]+)\s+([\d\.]+)\s+([^\s]+)\s+([^\s]+)\s+([^\s].*)\s*$", out, re.MULTILINE)
        return [self._list2ring(r) for r in m]

    @attr('single_node')
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
        res = TestNodetool._list2dic(lst, ["id", "keyspace_name", "columnfamily_name",
                                           "compacted_at", "bytes_in", "bytes_out", "rows_merged"])
        self._verify_compaction_history(res)
        return res

    def compactionhistory(self, node):
        out = node.nodetool('compactionhistory', True)[0]
        merged = re.findall(
            "^\s*([\d\-abcdef]+)\s+([^\s]+)\s+([^\s]+)\s+(\d+)\s+(\d+)\s+(\d+)\s+([^\s]+)?\s*$", out, re.MULTILINE)
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
        # self.assertNotEqual(cpc["rows_merged"], "", "row merged information is missing")
        # self.assertRegexpMatches(cpc["rows_merged"], "\{\d+,\d+\}")

    def _compact(self, keyspace):
        cluster = self.cluster
        cluster.populate(1).start(jvm_args=["--compaction-enforce-min-threshold", "true"], wait_for_binary_proto=True)
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

    @attr('single_node')
    def general_compact_test(self):
        """ Test that the nodetool compact works by:
        starting a cluster.
        running a load.
        check the number of sstable
        run compact
        check that the number of sstable decrease
        """
        self._compact("")

    @attr('single_node')
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

    @staticmethod
    def _get_ring_entry(lst):
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
        tokens = re.findall(
            "^\s*([\d\.]+)\s+([^\s]+)\s+([^\s]+)\s+([^\s]+)\s+([^\s]+)(?:\s[^\s]{2})?\s+([^\s]+)(?:\s[^\s]{2})?\s+(\-?[\d]+)\s*$", out, re.MULTILINE)
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

    def isrunning(self, cmd, node=None):
        if not node:
            node = self.cluster.nodelist()[0]
        out = node.nodetool(cmd, True)[0]
        if re.search("^\s*running\s*$", out):
            return True
        if re.search("^\s*not running\s*$", out):
            return False
        self.assertTrue(False, cmd + " return wrong value: " + out)

    def tst_mgmt(self, cmd, mode=True):
        [node] = self.run_cluster(nodes=1)
        if mode:
            self.assertTrue(self.isrunning("status" + cmd, node), cmd + " is not working")
            node.nodetool("disable" + cmd)
            self.assertFalse(self.isrunning("status" + cmd, node), "Fail to disable " + cmd)
            node.nodetool("enable" + cmd)
            self.assertTrue(self.isrunning("status" + cmd, node), "Fail to enable " + cmd)
        else:
            self.assertFalse(self.isrunning("status" + cmd, node), cmd + " is working")
            node.nodetool("enable" + cmd)
            self.assertTrue(self.isrunning("status" + cmd, node), "Fail to enable " + cmd)
            node.nodetool("disable" + cmd)
            self.assertFalse(self.isrunning("status" + cmd, node), "Fail to disable " + cmd)

    def binary_test(self):
        """
        Test the nodetool binary commands
        it check that binary is enable
        disable
        check
        enable and check
        """
        self.tst_mgmt("binary")

    def backup_test(self):
        """
        Test the nodetool backup commands
        it check that backup is disable
        enable
        check
        disable and check
        """
        self.tst_mgmt("backup", mode=False)

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
        vals = re.findall(
            '^\s*start_token:(-?\d+), end_token:(-?\d+), endpoints:\[([\d\., ]+)\], rpc_endpoints:\[([\d\., ]+)\], endpoint_details:\[(.*)\]\s*$', v, re.MULTILINE)
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

    def verify_describering(self, node=None, ks='keyspace1'):
        node = self.get_node(node)
        res = self.describering(node, ks)

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
        self.verify_describering()

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

    @skip('#1057')
    def keyspace_ring_test(self):
        self.check_ring("keyspace1")

    @attr('single_node')
    def general_flush_test(self):
        """
        Test the `nodetool flush` command.

        1) Start a cluster, enter a single entry
        2) check the number of sstables is 0
        3) run flush
        4) check the number of sstable is 1
        """
        self._flush("")

    @attr('single_node')
    def keyspace_flush_test(self):
        """
        Test the `nodetool flush` command to flush keyspace1.

        1) Start a cluster, enter a single entry
        2) check the number of sstables is 0
        3) run flush
        4) check the number of sstable is 1
        """
        self._flush(" keyspace1")

    @attr('single_node')
    def keyspace_column_family_flush_test(self):
        """
        Test the `nodetool flush` command to flush keyspace1.standard1.

        1) Start a cluster, enter a single entry
        2) check the number of sstables is 0
        3) run flush
        4) check the number of sstable is 1
        """
        self._flush(" keyspace1 standard1")

    @staticmethod
    def _get_cfhistogram(node, ks, cf):
        out = node.nodetool("cfhistograms " + ks + " " + cf, True)[0]
        debug(out)
        m = re.findall(r"^([^\/]+)\/(.*)\s+histograms\s*$", out, re.MULTILINE)
        res = {}
        if m:
            res["ks"] = m[0][0]
            res["cf"] = m[0][1]
        m = re.findall(r"^([^\s]+)\s+([\d\.]+)\s+([\d\.]+)\s+([\d\.]+)\s+([\d\.]+)\s+([\d\.]+)\s*$", out, re.MULTILINE)
        heads = ['Percentile', 'SSTables', 'Write Latency', 'Read Latency', 'Partition Size', 'Cell Count']
        res["vals"] = {}
        res["out"] = out
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
        strs = self.stress_write(node, 10000, duration='10s', pop='seq=1..10000', opt=["-rate threads=10"])
        res = self._get_cfhistogram(node, "keyspace1", "standard1")

        self.assertMapEqual(res, "ks", "keyspace1", "wrong keysyapce")
        self.assertMapEqual(res, "cf", "standard1", "wrong column family")
        self.verify_cfhistograms(res=res)
        ltnc = strs['latency max:write']
        for v in res["vals"]:
            self.assertMapEqual(res["vals"][v], "Read Latency", 0, "unexpected read latency")
            if float(ltnc) != 0.0:
                if v != "Max":
                    self.assertMapLess(res["vals"][v], "Write Latency", ltnc * 1000, "unexpected write latency")

        strs = self.stress_mixed(node, 10000, duration='10s', pop='seq=1..10000', opt=["-rate threads=10"])
        res = self._get_cfhistogram(node, "keyspace1", "standard1")
        self.verify_cfhistograms(res=res, ltype='mixed')
        if 'latency max:read' in strs:
            ltnc = strs['latency max:read']
            if float(ltnc) != 0.0:
                for v in res["vals"]:
                    if v != "Max":
                        self.assertMapLess(res["vals"][v], "Read Latency", ltnc * 1000, "unexpected read latency")

    def verify_cfhistograms(self, node=None, ks="keyspace1", cf="standard1", res=None, ltype='write'):
        if not res:
            node = self.get_node(node)
            res = self._get_cfhistogram(node, ks, cf)
        latency_types = ['Write Latency']
        if ltype == 'mixed':
            latency_types.append('Read Latency')
        for latency_type in latency_types:
            cur = res["vals"]["Min"][latency_type]
            for v in ["50%", "75%", "95%", "98%", "99%", "Max"]:
                latency_val = res["vals"][v][latency_type]
                self.assertNotEqual(float(latency_val), 0.0, "unexpected {} 0 for {} load".format(latency_type, v))
                self.assertGreaterEqual(latency_val, cur,
                                        "{} is not monotonic: {}({} load), was {}\n{}".format(latency_type, latency_val,
                                                                                              v, cur, res["out"]))
                cur = latency_val

    @staticmethod
    def describecluster(node):
        out = node.nodetool('describecluster', True)[0]
        return yaml.safe_load(out.replace('\t', "  "))

    def verify_decribecluster(self, node=None):
        node = self.get_node(node)
        self.describecluster(node)

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
        self.assertTrue(cluster["Snitch"].startswith("org.apache.cassandra.locator."),
                        "invalid snitch name:" + cluster["Snitch"])
        self.assertIn("Schema versions", cluster)
        schema = cluster["Schema versions"]
        for k in schema:
            self.assertEqual(3, len(schema[k]), "wrong schema version for " + k + " " + str(schema[k]))
        self.assertMapEqual(cluster, "Name", "test")

    @staticmethod
    def create_table(session, obj):
        """A helper function that creates a keyspace and tables
        """
        for ks in obj:
            cls = obj[ks]["class"] if "class" in obj[ks] else 'SimpleStrategy'
            rf = obj[ks]["rf"] if "rf" in obj[ks] else 1
            session.execute("CREATE KEYSPACE " + ks +
                            " WITH replication = { 'class':'" + cls + "', 'replication_factor':" + str(rf) + "}")
            session.execute("USE " + ks)
            for table in obj[ks]["tables"]:
                t = obj[ks]["tables"][table]
                keys = functools.reduce(lambda a, b: a + "," + b, [k + " " + t[k] for k in t.keys() if k != "key"])
                pk = t["key"]
                create_table = "CREATE TABLE " + table + " (" + keys + " ,PRIMARY KEY (" + pk + "))"
                session.execute(create_table)

    @staticmethod
    def _sql_val(val):
        try:
            if val.startswith('0x'):
                return val
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
                    ins = ins + functools.reduce(lambda a, b: a + "," + b, val.keys()) + ") VALUES ("
                    ins = ins + functools.reduce(lambda a, b: a + "," + b,
                                                 [self._sql_val(val[a]) for a in val.keys()]) + ")"
                    session.execute(ins)

    @staticmethod
    def getendpoints(node, ks, cf, value):
        return node.nodetool('getendpoints ' + ks + ' ' + cf + ' ' + value, True)[0]

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
        self.create_table(session, {"ks1": {"tables": {"tbl1": {"col1": "int", "col2": "text", "key": "col1"}}}})
        self.populate_data(session, {"ks1": {"tbl1": [{"col1": 4, "col2": "abc"}]}})
        endpoint = self.getendpoints(node, "ks1", "tbl1", "4")
        self.assertTrue(endpoint.startswith("127.0."), "Invalid endpoint returned '" + endpoint + "'")

    def _get_gossipinfo(self, output):
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

    def gossipinfo_test(self):
        cluster = self.cluster
        cluster.populate(2).start(wait_for_binary_proto=True)
        node = cluster.nodelist()[0]
        gi_output = node.nodetool('gossipinfo', True)[0]
        gi = self._get_gossipinfo(gi_output)

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
            # self.assertIn("SEVERITY", info)

    def verify_info(self, node=None, dc="datacenter1", rac="rack1"):
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
        self.assertMapEqual(ni, "Data Center", dc)
        self.assertMapEqual(ni, "Rack", rac)
        self.assertMapEqual(ni, "Exceptions", 0)
        self.assertIn("Key Cache", ni)
        self.assertIn("Row Cache", ni)
        self.assertIn("Counter Cache", ni)
        self.assertIn("Token", ni)
        time.sleep(10)
        ni = self.nodetool_info(node)
        self.assertMapBetween(ni, "Uptime (seconds)", uptime + 10, uptime + 40)

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

    @skip('#1057')
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
        for line in lines:
            ip = re.match("^\s+/([\d\.]+)\s*$", line)
            strm = re.match("^\s+(\S+) (\d+) files, (\d+) bytes total. Already \S+ (\d+) files, (\d+) bytes total", line)
            command = re.match("Commands\s+([^\s]+)\s+(\d+)\s+(\d+)", line)
            responses = re.match("Responses\s+([^\s]+)\s+(\d+)\s+(\d+)", line)
            messages = re.match("(Large|Small|Gossip) messages\s+([^\s]+)\s+(\d+)\s+(\d+)\s+(\d)", line)

            rxfile = re.match("\s+(\S+)\s+(\d+)/(\d+) bytes\((\d+)%\)\s+\S+\s+\S+\s+idx:0/([\d\.]+)", line)
            if line == "Read Repair Statistics:":
                read_repair = True
                if stream is not None:
                    res["streams"].append(stream)
                    stream = None
            elif line.startswith("Pool Name"):
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
            elif messages:
                type = messages.group(1)
                res[type] = {}
                res[type]["Active"] = self._tonum(messages.group(1))
                res[type]["Pending"] = self._tonum(messages.group(2))
                res[type]["Completed"] = self._tonum(messages.group(3))
                res[type]["Dropped"] = self._tonum(messages.group(4))
            elif read_repair:
                rr = re.match("^(.*):\s*(\d+)\s*$", line)
                self.assertTrue(rr, "unexpected line in read repair")
                res[rr.group(1)] = self._tonum(rr.group(2))
            else:
                self.assertTrue(False, "unknown line in netstats" + line + "\n" + out)
        if stream is not None:
            res["streams"].append(stream)
        return res

    @require('bootstrap using streaming')
    def netstats_test(self):
        """Testwing the `nodetool netstats` command
        It starts a 2 node cluster load it.
        add a node and check the results.
        """
        cluster = self.cluster
        cluster.populate(2).start(wait_for_binary_proto=True)
        node = cluster.nodelist()[0]
        debug('Run stress write test')
        self.stress_write(node, times=1000000, pop='seq=1..3000000000', opt=["-rate threads=10"])
        debug('Add new node')
        node2 = new_node(cluster)
        node2.start(wait_for_binary_proto=False)
        node2.watch_log_for('Executing streaming plan')
        debug('Run and check netstats')
        stats = self.netstats(node)
        debug(stats)
        self.assertEquals(len(stats["streams"]), 1)

    def _change_data_perms(self, node, folder, mod):
        path = os.path.join(node.get_path(), folder)
        os.chmod(path, mod)

    def nodetool_refresh_with_data_perms_test(self):
        """ Test that nodetool refresh return Permission denied
        when data folder is not writable
        When prevent write to data folder verify that:
        enablegossip and enablebinary pass
        refresh failed with permission denied
        """
        error_to_track = re.compile("Storage I/O error: 13|Permission denied")
        self.run_cluster()
        node = self.cluster.nodelist()[0]
        self.stress_write(node, times=10000)
        node.flush()
        node.compact()
        node.nodetool("refresh keyspace1 standard1")
        try:
            self._change_data_perms(node, 'data', 644)
            output = node.nodetool("enablebinary", True)
            self.assertEqual(('', ''), output, 'enablebinary not set')
            errors = node.grep_log(error_to_track)
            self.assertFalse(len(errors), 'Permission denied errors found')

            output = node.nodetool("enablegossip", True)
            self.assertEqual(('', ''), output, 'enablebinary not set')
            errors = node.grep_log(error_to_track)
            self.assertFalse(len(errors), 'Permission denied errors found')

            try:
                node.nodetool("refresh keyspace1 standard1")
                self.fail("refresh should be with Permission denied")
            except NodetoolError as e:
                self.assertTrue(error_to_track.search(str(e)),
                                'expected error not found in nodetool error message: {}'.format(str(e)))
        finally:
            self._change_data_perms(node, 'data', stat.S_IWRITE | stat.S_IREAD | stat.S_IEXEC)
            node.mark_log_for_errors()

    def _nodetool_refresh_expect_fail(self, node, ks='keyspace1', cf='standard1', expected_error=None, debug_message='', ignore_log_error=True):
        if expected_error and ignore_log_error:
            self.ignore_log_patterns.append(expected_error)

        cmd = "refresh -- {} {}".format(ks, cf)
        msg = "Running 'nodetool {}' to load migrated sstables".format(cmd)
        if debug_message:
            msg = "{} - {}".format(msg, debug_message)
        debug(msg)
        try:
            node.nodetool(cmd)
            assert False, 'nodetool succeeded unexpectedly!'
        except NodetoolError as error:
            debug("As expected, {}".format(error))
            if expected_error:
                assert re.search(expected_error, str(error)), "/{}/ not found in '{}'".format(expected_error, error)

    @attr('single_node')
    def nodetool_refresh_with_wrong_upload_modes_test(self):
        """
        Test that nodetool refresh with different wrong modes:
            when upload folder is not writable
            when upload sstables are not readable
            when upload directory has symlink
        """
        self.run_cluster(nodes=1)
        node = self.cluster.nodelist()[0]
        self.stress_write(node, times=10000)
        node.flush()
        ks = 'keyspace1'
        cf = 'standard1'
        ks_dir = os.path.join(node.get_path(), 'data', ks)
        cf_dir = None
        for f in os.listdir(ks_dir):
            if f.startswith(cf):
                cf_dir = os.path.join(ks_dir, f)
                break
        assert cf_dir, "column family '{}' not found".format(cf)
        upload_dir = os.path.join(cf_dir, 'upload')
        for f in os.listdir(cf_dir):
            pathname = os.path.join(cf_dir, f)
            mode = os.lstat(pathname).st_mode
            if stat.S_ISREG(mode):
                shutil.copy2(pathname, os.path.join(upload_dir, f))

        debug("Testing loading sstables in a dir with no write permission. nodetool expected to fail...")
        os.chmod(upload_dir, 0o555)
        self._nodetool_refresh_expect_fail(node,
                                           expected_error=r'Directory cannot be accessed .* write',
                                           debug_message='dir with no write permission')
        os.chmod(upload_dir, 0o755)

        debug("Testing loading sstables with no read permission. nodetool expected to fail...")
        for f in os.listdir(upload_dir):
            os.chmod(os.path.join(upload_dir, f), 0o044)
        self._nodetool_refresh_expect_fail(node,
                                           expected_error=r'File cannot be accessed for read|open failed: Permission denied',
                                           debug_message='files with no read permission')
        for f in os.listdir(upload_dir):
            os.chmod(os.path.join(upload_dir, f), 0o644)

        debug("Testing loading sstables in a dir with symlink. nodetool expected to fail...")
        symlink_path = os.path.join(upload_dir, 'test_symlink')
        os.symlink('broken', symlink_path)
        self._nodetool_refresh_expect_fail(node,
                                           expected_error=r'Must be either a regular file or a directory',
                                           debug_message='with symlink')
        os.remove(symlink_path)

    def proxyhistograms(self, node=None):
        if node is None:
            node = self.cluster.nodelist()[0]
        out = node.nodetool("proxyhistograms", True)[0]
        histogram = re.findall(
            "^\s*([^\s]+)\s+(\d+\.\d+)\s+(\d+\.\d+)\s+([^\s]+)\s+([^\s]+)\s+([^\s]+)\s+([^\s]+)\s*$", out, re.MULTILINE)
        return {m[0]: self._list2dic(m[1:], ["Read Latency", "Write Latency", "Range Latency", "CAS Read", "CAS Write", "View Write"]) for m in histogram}

    def _verify_proxyhistogram(self, res):
        for latency_type in ("Read Latency", "Write Latency", "Range Latency"):
            cur = float(res["Min"][latency_type])
            for v in ["50%", "75%", "95%", "98%", "99%", "Max"]:
                latency_val = float(res[v][latency_type])
                self.assertNotEqual(latency_val, 0.0, "unexpected {} 0 for {} load".format(latency_type, v))
                self.assertGreaterEqual(latency_val, cur,
                                        "{} is not monotonic: {}({} load), was {}".format(latency_type, latency_val,
                                                                                          v, cur))
                cur = latency_val

    def proxyhistograms_test(self):
        """
        This test the `nodetool proxyhistograms` command
        it starts a cluster,
        runs a load
        call proxyhistograms and validate its output
        """
        node = self.run_cluster()[0]
        debug('Run stress write and mixed')
        node.stress_object(['write', 'n=10000', '-rate', 'threads=4'])
        node.stress_object(['mixed', 'n=10000', '-rate', 'threads=4'])
        session = self.patient_cql_connection(node)
        rows = session.execute("Select * from keyspace1.standard1 limit 100")
        keys = ['0x' + hexlify(r[0]).decode('utf-8') for r in rows_to_list(rows)]
        debug('Run range queries')
        q_slice = 10
        start = 0
        end = q_slice - 1
        for i in range(0, q_slice):
            query = "SELECT * FROM keyspace1.standard1 WHERE "\
                    "token(key) >= token({}) and token(key) <= token({})".format(keys[start], keys[end])
            rows = session.execute(query)
            self.assertEquals(len(rows_to_list(rows)), q_slice)
            start += q_slice
            end += q_slice
        debug('Run and check proxyhistograms')
        res = self.proxyhistograms(node)
        self._verify_proxyhistogram(res)

    def nodetool_version(self, node=None):
        if node is None:
            node = self.cluster.nodelist()[0]
        return node.nodetool('version', True)[0]

    @attr('single_node')
    def version_test(self):
        self.run_cluster(nodes=1)
        self.assertRegexpMatches(self.nodetool_version(), "ReleaseVersion: 3\.\d+\.\d+", "Wrong version")

    def run_cluster(self, nodes=2, configuration=None):
        self.cluster_started = False
        cluster = self.cluster
        if configuration is not None:
            cluster.set_configuration_options(values=configuration)
        cluster.populate(nodes).start(wait_other_notice=True, wait_for_binary_proto=True)
        self.cluster_started = True
        return cluster.nodelist()

    def create_datacenter(self, nodes=None, run_dc1=True, run_dc2=False):
        if nodes is None:
            nodes = [2, 2]
        cluster = self.cluster
        cluster.populate(nodes)
        nl = cluster.nodelist()
        if run_dc1:
            for i in range(0, nodes[0]):
                nl[i].start(wait_for_binary_proto=True)
        return nl

    def add_node(self):
        cluster = self.cluster
        node2 = new_node(cluster)
        # This is a workaround to support copying of the
        # executable between file systems (as oppose to static link)
        # it solve an issue of doing the copy in the context of a thread
        time.sleep(3)
        node2.start(wait_for_binary_proto=True)

    def stop(self, nodes, args=None):
        if args is None:
            args = []
        if isinstance(nodes, int):
            self.cluster.nodelist()[nodes].stop(*args)
        else:
            for i in nodes:
                self.cluster.nodelist()[i].stop(*args)

    def start(self, nodes, args=None):
        if args is None:
            args = {}
        if isinstance(nodes, int):
            self.cluster.nodelist()[nodes].start(**args)
        else:
            for i in nodes:
                self.cluster.nodelist()[i].start(**args)

    concurrent_test_fail = False

    def time_func(self, func_info, ops, paralel=True, delay=0):
        """takes a function and a time limit
        it runs the function, verify when it's done
        that it didn't took too long
        if paralel is true will run on a thread
        func_info can include a delay, in that case it would wait before
        calling the function
        """
        if paralel:
            delay = func_info["delay"] if "delay" in func_info else 0
            tr = Thread(target=self.time_func, args=[func_info, ops, False, delay])
            tr.start()
            return tr
        else:
            if delay:
                time.sleep(delay)
            before = time.time()
            ops["start"] = before
            debug("starting " + func_info["func"].__name__)
            try:
                if "args" in func_info:
                    func_info["func"](*func_info["args"])
                else:
                    func_info["func"]()
            except:
                print("Failed " + func_info["func"].__name__, " with ", sys.exc_info()[1])
                ops["exception"] = str(sys.exc_info()[1])
                self.concurrent_test_fail = True
                # raise
            duration = int(time.time() - before + 0.5)
            msg = func_info["func"].__name__ + " completed in " + str(duration) + " seconds"
            if "time" in func_info:
                # self.assertLessEqual(int(time.time()) - before, func_info["time"], msg)
                if duration > func_info["time"] and not self.concurrent_test_fail:
                    ops["exception"] = "Timeout:" + msg
                    self.concurrent_test_fail = True
            debug(msg)
            ops["end"] = time.time()
            return None

    @staticmethod
    def print_fun_name(name, ln):
        if ln <= 2:
            return "", name
        if len(name) > ln - 2:
            return name[:ln-2], name[ln-2:]
        return name.ljust(ln - 2), ""

    def print_ops(self, ops, start, ratio):
        ln = int((ops["end"] - ops["start"] + 0.5) / ratio) if "end" in ops else 0
        strt = int((ops["start"] - start) / ratio)
        end = "]" if "end" in ops else ""
        if "exception" in ops:
            end = "E" + end
        name, name_extra = self.print_fun_name(ops["name"], ln)
        res = "".rjust(strt) + "[" + name + end + name_extra
        if "exception" in ops:
            res += "\n" + ops["exception"].rjust(strt + 2 + len(name))
        return res

    def print_time(self, lst, start=None):
        lst.sort(key=lambda a: a["start"])
        if not start:
            start = lst[0]["start"]
        start = int(start)
        end = int(max(map(lambda a: a["end"] if "end" in a else a["start"], lst)) + 0.5)
        strts = list(set(map(lambda a: int(a["start"]), lst)))
        strts.sort()
        ratio = 1.0 * (end - start) / self.width
        res = str(end - start)
        left = ""
        for s in strts:
            st = s - start
            n = str(st)
            padding = int(st / ratio) - len(left)
            if padding < 0:
                padding = 0
            left = left + "|".rjust(padding) + n
        res = left + res.rjust(self.width - 1 - len(left)) + "|\n" + "".ljust(self.width, '-') + "\n"
        for element in lst:
            res = res + self.print_ops(element, start, ratio) + "\n"
        return res

    def concurrent_stress(self, node=None, arg=None):
        if arg is None:
            arg = {}
        if node is None:
            node = self.cluster.nodelist()[0]
        if "times" not in arg and "duration" not in arg:
            arg["duration"] = "30s"
        if "pop" not in arg:
            arg["pop"] = 'seq=1..3000000000'
        if "opt" not in arg:
            arg["opt"] = ["-rate threads=10"]
        self.stress_write(node, **arg)

    def repair(self, node=None):
        if node is None:
            node = self.cluster.nodelist()[0]
        node.nodetool('repair')

    def do_recurrent(self, start, waits, res):
        if "recurrent" not in start:
            return
        for rec in start["recurrent"]:
            operation = self.create_op(rec)
            res.append(operation)
            r = self.time_func(rec, operation)
            if "block" in rec:
                r.join()
            else:
                waits.append(r)

    @staticmethod
    def create_op(ops):
        res = {}
        res["start"] = time.time()
        res["name"] = ops["func"].__name__
        return res

    def concurrent_part(self, start):
        if "operations" not in start:
            return
        res = []
        waits = []
        operations = []
        for ops in start["operations"]:
            operation = self.create_op(ops)
            res.append(operation)
            tr = self.time_func(ops, operation)
            if "block" in ops:
                if "recurrent" in start:
                    while tr.is_alive():
                        if self.concurrent_test_fail:
                            break
                        self.do_recurrent(start, waits, res)
                else:
                    tr.join()
            else:
                operations.append(tr)
            if self.concurrent_test_fail:
                break
        while len(list(filter(lambda a: a.is_alive(), operations))) > 0:
            if not self.concurrent_test_fail:
                self.do_recurrent(start, waits, res)
            time.sleep(20)
        for w in waits:
            if w is not None:
                w.join()
        self.assertFalse(self.concurrent_test_fail, "Concurrent test failed\n" + self.print_time(res))
        debug("done concurrent_part")
        return res

    def general_concurrent(self, tst):
        """
        tst is an object of the form

        tst = [{"operations": [{"func": self.run_cluster}, {"func": self.concurrent_stress, "delay": 5}, {"func": self.repair, "time": 300, "delay": 10}],
                "recurrent": [{"func": self.verify_info, "time": 60, "delay": 10}]},
               {"operations": [{"func": self.concurrent_stress, "delay": 5}]},
               {"operations": [{"func": self.add_node, "time": 300}, {"func": self.repair, "time": 300}],
                "recurrent": [{"func": self.verify_info, "time": 90}, {"func": self.verify_status, "time": 25}, {"func": self.verify_netstats, "time": 26}]}]

        operation and recurrent are list of objects
        {"func" the function name, "time": when present check the operation time,
        "delay": add a delay before running, "block" when present the operation block}

        each test can have multiple sections.
        Each section would start after all the operations in the previous section completed.
        """
        self.concurrent_test_fail = False
        for op in tst:
            if not self.concurrent_test_fail:
                debug("Starting concurrent test: {}".format(op))
                start = time.time()
                times = self.concurrent_part(op)
                debug("Test call flow:\n" + self.print_time(times, start))

    def concurrent_repair_test(self):
        tst = [{"operations": [{"func": self.run_cluster, "block": True}, {"func": self.concurrent_stress, "delay": 5}, {"func": self.repair, "time": 300, "delay": 10}],
                "recurrent": [{"func": self.verify_all_api, "block": True}, {"func": self.verify_info, "time": 60, "delay": 10}]},
               {"operations": [{"func": self.concurrent_stress, "delay": 5}]},
               {"operations": [{"func": self.add_node, "time": 300}, {"func": self.repair, "time": 300}],
                "recurrent": [{"func": self.verify_info, "time": 90}, {"func": self.verify_status, "time": 25}, {"func": self.verify_netstats, "time": 26}]}]
        self.general_concurrent(tst)

    def rebuild(self, node=None, dc=""):
        node = self.get_node(node)
        node.nodetool('rebuild ' + dc)

    def verify_all_api(self, giveup=120):
        """ Check that the cluster is available
        """
        while giveup > 0:
            if self.cluster_started:
                return
            time.sleep(1)
            giveup -= 1
        raise Exception("Cluster did not start")

    def get_node(self, node):
        if node is None:
            return self.cluster.nodelist()[0]
        if isinstance(node, int):
            return self.cluster.nodelist()[node]
        return node

    def concurrent_rebuild_test(self):
        """
        Start a cluster with 2 dc
        stop 2 nodes
        load data
        start 2 nodes
        call rebuild
        """
        expected_errors = ["No schema agreement from live replicas after"]
        tst = [{"operations": [{"func": self.run_cluster, "args": [[2, 2], {'hinted_handoff_enabled': False, 'compaction_enforce_min_threshold': True}], "block": True}, {"func": self.stop, "delay": 5, "args": [[2, 3]]}],
                "recurrent":[{"func": self.verify_all_api, "block": True}, {"func": self.verify_info, "time": 60, "delay": 10, "args": [None, 'dc1', 'RAC1']}]},
               {"operations": [{"func": self.concurrent_stress, "delay": 15,
                                "args": [None, {"cl": "ONE", "duration": "1m",
                                                "opt": ["-schema", "replication(strategy=NetworkTopologyStrategy, dc1=1,dc2=1)", "-rate", "threads=10"],
                                                "expected_errors": expected_errors}]}],
                "recurrent": [{"func": self.verify_info, "time": 60, "delay": 10, "args": [None, 'dc1', 'RAC1']}]},
               {"operations": [{"func": self.start, "delay": 5, "args": [[2, 3], {"wait_for_binary_proto": True}]}],
                "recurrent": [{"func": self.verify_info, "time": 60, "delay": 10, "args": [None, 'dc1', 'RAC1']}]},
               {"operations": [{"func": self.rebuild, "time": 300, "args": [2, "dc1"]}],
                "recurrent":self.multi_dc_queries_method_list}]
        self.general_concurrent(tst)

    def stress_node_down_expected_errors(self, node):
        node_address = re.escape("{}{}".format(self.cluster.get_ipprefix(), node))
        addr_msg = "({})?/{}:9042".format(node_address, node_address)
        return [
            "\[{}\] Connection has been closed".format(addr_msg),
            "Error creating netty channel to {}".format(addr_msg),
            "Connection refused: {}".format(addr_msg),
            "Caused by: java.net.ConnectException: Connection refused",
            "Cassandra timeout during SIMPLE write query",
            "Unexpected error while executing task",
            "Unexpected error while querying {}".format(node_address),
            "java.lang.NullPointerException: null",
            "Error creating pool to {}".format(addr_msg),
            "\[{}\] Cannot connect".format(addr_msg),
        ]

    def drain(self, node_to_drain):
        # get_node uses 0-based index into cluster.nodelist()
        node = self.get_node(node_to_drain-1)
        node.nodetool("drain")

    def concurrent_drain_test(self):
        """
        Start a cluster with 2 nodes
        run load
        call drain
        """
        node_to_drain = 2
        expected_errors = self.stress_node_down_expected_errors(node_to_drain)
        tst = [{"operations": [{"func": self.run_cluster}],
                "recurrent": [{"func": self.verify_all_api, "block": True}, {"func": self.verify_info, "time": 60, "delay": 10}]},
               {"operations": [{"func": self.concurrent_stress, "delay": 5, "args": [None, {"duration": "1m", "opt": ["-schema", "replication(strategy=SimpleStrategy, replication_factor=2)", "-rate", "threads=10"]}]}],
                "recurrent": [{"func": self.verify_info, "time": 60, "delay": 10}]},
               {"operations": [{"func": self.concurrent_stress, "delay": 5, "args": [None, {"cl": "ONE", "duration": "2m", "expected_errors": expected_errors}]},
                               {"func": self.drain, "delay": 90, "args": [node_to_drain]}],
                "recurrent": self. queries_method_list}]
        self.general_concurrent(tst)

    def restart(self, node_to_restart, gently=False, wait_for_binary_proto=True, wait_other_notice=True):
        # get_node uses 0-based index into cluster.nodelist()
        node = self.get_node(node_to_restart-1)
        node.stop(gently=gently, wait_other_notice=wait_other_notice)
        time.sleep(1)
        node.start(wait_for_binary_proto=wait_for_binary_proto, wait_other_notice=wait_other_notice)

    def concurrent_restart_test(self):
        """
        Start a cluster with 2 nodes
        run load
        call drain
        """
        node_to_drain = 2
        expected_errors = self.stress_node_down_expected_errors(node_to_drain)
        tst = [{"operations": [{"func": self.run_cluster}],
                "recurrent": [{"func": self.verify_all_api, "block": True}, {"func": self.verify_info, "time": 60, "delay": 10}]},
               {"operations": [{"func": self.concurrent_stress, "delay": 5, "args": [None, {"duration": "1m", "opt": ["-schema", "replication(strategy=SimpleStrategy, replication_factor=2)", "-rate", "threads=10"]}]}],
                "recurrent": [{"func": self.verify_info, "time": 60, "delay": 10}]},
               {"operations": [{"func": self.concurrent_stress, "delay": 5, "args": [None, {"cl": "ONE", "duration": "2m", "expected_errors": expected_errors}]},
                               {"func": self.restart, "delay": 10, "args": [node_to_drain]}],
                "recurrent": self. queries_method_list}]
        self.general_concurrent(tst)

    def stress(self, node, opr, times=10000, duration=None, col=None, pop=None, opt=None, cl=None, expected_errors=None):
        cmd = [opr]
        if cl is None:
            cl = 'ALL'
        cmd += ['cl=' + cl]
        if opt is None:
            opt = []
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
        ret = node.stress_object(cmd)
        if type(ret) == type(str()):
            for line in ret.splitlines():
                # Ignore Java stacktrace lines
                if (re.search('^\s+(at|\.\.\.)', line)):
                    continue
                error = True
                for p in expected_errors:
                    if re.search(p, line):
                        error = False
                if error:
                    debug("Unexpected output line: {}".format(line))
                    raise Exception('Error running cassandra-stress: {}'.format(ret))
        return ret

    def stress_write(self, node, times=10000, duration=None, col=None, pop=None, opt=None, cl=None, expected_errors=None):
        if opt is None:
            opt = []
        return self.stress(node, 'write', times=times, duration=duration, col=col, pop=pop, opt=opt, cl=cl, expected_errors=expected_errors)

    def stress_mixed(self, node, times=10000, duration=None, col=None, pop=None, opt=None, expected_errors=None):
        if opt is None:
            opt = []
        return self.stress(node, 'mixed', times=times, duration=duration, col=col, pop=pop, opt=opt, expected_errors=expected_errors)

    @attr('single_node')
    def get_sstable_test(self):
        """
        get sstables get a keyspace, table and a key and return the sstables that contain that key

        Start a cluster
        Add create a keyspace/table
        insert a value
        do nodetool flush
        get nodetool sstables and validate that we get
        an sstable, return with a different value and validate that
        we get no sstables.
        Testing perform on int, blob and text keys
        """

        cluster = self.run_cluster(nodes=1)
        node = cluster[0]
        session = self.patient_cql_connection(node)
        self.create_table(session, {"ks1": {"tables": {"tbl1": {"col1": "int", "col2": "text", "key": "col1"}}}})
        self.populate_data(session, {"ks1": {"tbl1": [{"col1": 4, "col2": "abc"}]}})
        self.create_table(session, {"ks2": {"tables": {"tbl2": {"col1": "blob", "col2": "text", "key": "col1"}}}})
        self.populate_data(session, {"ks2": {"tbl2": [{"col1": "0x39303138374b4d343830", "col2": "abc"}]}})
        self.create_table(session, {"ks3": {"tables": {"tbl3": {"col1": "text", "col2": "text", "key": "col1"}}}})
        self.populate_data(session, {"ks3": {"tbl3": [{"col1": "keytest", "col2": "abc"}]}})
        node.nodetool("flush")
        out = node.nodetool("getsstables ks1 tbl1 4", True)[0]
        self.assertTrue("ks1/tbl1" in out, "key was not found in the sstable")
        out = node.nodetool("getsstables ks1 tbl1 5", True)[0]
        self.assertEqual("", out, "unexpected sstable return for the key")
        out = node.nodetool("getsstables ks2 tbl2 39303138374b4d343830", True)[0]
        self.assertTrue("ks2/tbl2" in out, "key was not found in the sstable")
        out = node.nodetool("getsstables ks2 tbl2 39303138374b4d343831", True)[0]
        self.assertEqual("", out, "unexpected sstable return for the key")
        out = node.nodetool("getsstables ks3 tbl3 keytest", True)[0]
        self.assertTrue("ks3/tbl3" in out, "key was not found in the sstable")
        out = node.nodetool("getsstables ks3 tbl3 keytest1", True)[0]
        self.assertEqual("", out, "unexpected sstable return for the key")

    @attr('single_node')
    def scrub_with_one_node_expect_data_loss_test(self):
        cluster = self.run_cluster(nodes=1)
        node = cluster[0]
        session = self.patient_cql_connection(node)
        self.create_ks(session, 'ks', 1)
        self.create_cf(session, 'cf', columns={'c1': 'text', 'c2': 'text'})
        insert_c1c2(session, keys=range(100))
        node.nodetool("flush")
        out = node.nodetool("getsstables ks cf k1", True)[0].strip()
        debug("Will corrupt sstable {}".format(out))
        node.stop()

        seed = int(time.time())
        debug("Random seed: {}".format(seed))
        random.seed(seed)
        size = os.stat(out).st_size
        offset = random.randint(0, size)
        length = random.randint(1, 102400)
        debug("writing random contents at offset={} length={}".format(offset, length))
        with io.open(out, 'rb+', buffering=0) as f:
            f.seek(offset)
            f.write(bytearray(randbytes(length)))

        self.ignore_log_patterns += [
            'malformed_sstable_exception',
            'SSTables with Cassandra-style shadowable deletion cannot be read by Scylla',
            'Adding missing partition-end to the end of the stream',
            'Skipping invalid clustering row',
        ]

        self.ignore_cores_log_patterns += [
            'Failed to allocate',
        ]

        node.start(wait_for_binary_proto=True, wait_other_notice=True)

        session = self.patient_cql_connection(node)
        try:
            list(session.execute('SELECT * FROM ks.cf'))
        except:
            pass

        debug('Rebuild sstables by storage_service/keyspace_scrub API')
        # Currently, scrub may fail with random corruption, e.g. on OOM
        p = run(args=['curl', 'http://{}:10000/storage_service/keyspace_scrub/ks?skip_corrupted=true'.format(self.get_ip_from_node(node))],
                capture_output=True, check=False, timeout=60)
        debug("Scrub output: {}".format(p.stdout))

        try:
            list(session.execute('SELECT * FROM ks.cf'))
        except:
            pass

    def scrub_with_multi_nodes_expect_data_rebuild_test(self):
        cluster = self.run_cluster(nodes=3)
        node = cluster[0]
        session = self.patient_cql_connection(node)
        self.create_ks(session, 'ks', 3)
        self.create_cf(session, 'cf', columns={'c1': 'text', 'c2': 'text'})
        insert_c1c2(session, keys=range(100))
        node.nodetool("flush")
        out = node.nodetool("getsstables ks cf k1", True)[0].strip()
        debug("Will corrupt sstable {}".format(out))
        node.stop()

        seed = int(time.time())
        debug("Random seed: {}".format(seed))
        random.seed(seed)
        size = os.stat(out).st_size
        offset = random.randint(0, size)
        length = random.randint(1, 102400)
        debug("writing random contents at offset={} length={}".format(offset, length))
        with io.open(out, 'rb+', buffering=0) as f:
            f.seek(offset)
            f.write(bytearray(randbytes(length)))

        self.ignore_log_patterns += [
            'malformed_sstable_exception',
            'SSTables with Cassandra-style shadowable deletion cannot be read by Scylla',
            'Adding missing partition-end to the end of the stream',
            'Skipping invalid clustering row',
        ]

        self.ignore_cores_log_patterns += [
            'Failed to allocate',
        ]

        node.start(wait_for_binary_proto=True, wait_other_notice=True)

        session = self.patient_cql_connection(node)
        debug('Rebuild sstables by storage_service/keyspace_scrub API')
        # Currently, scrub may fail with random corruption, e.g. on OOM
        p = run(args=['curl', 'http://{}:10000/storage_service/keyspace_scrub/ks?skip_corrupted=true'.format(self.get_ip_from_node(node))],
                capture_output=True, check=False, timeout=60)
        debug("Scrub output: {}".format(p.stdout))

        rows = list(session.execute('SELECT * FROM ks.cf'))
        assert len(rows) == 100

    def node_graceful_stop_during_stress_and_decommission_test(self, starting_size=4, node_count=10, rf=1):
        """
        reference:https://github.com/scylladb/scylla/issues/4491
        1. Create a cluster with 4 nodes and rf=3, insert data
        2. Run stress (write) on node 2
        3. Decommission node 4
        4. Stop node 3 (immediately  after Decommission)
        5. Check if node3 process exited successfully

        shell:
        ccm create scylla-repository5 --scylla --vnodes -n 4 --version unstable/master:2020-05-11T12:14:24Z
        ccm create scylla-repository5 --scylla -n 4 --version unstable/master:2020-05-11T12:14:24Z
        ccm start --jvm_arg="--memory" --jvm_arg="1G" --jvm_arg="--collectd-address" --jvm_arg="127.0.0.1:25826" --jvm_arg="--hinted-handoff-enabled" --jvm_arg="false" --jvm_arg="--collectd" --jvm_arg="1" --jvm_arg="--logger-log-level" --jvm_arg="stream_session=debug"
        ccm node1 stress write cl=QUORUM duration=15h no-warmup -rate threads=300 -mode native cql3 -schema "replication(factor=3)" -pop dist=gaussian\(0..100000000,500000,100000\)
        ccm node4 decommission
        ccm node3 stop
        ccm node2 nodetool status
        ps -elf | grep "bin/scylla" | grep node3
        """
        starting_size = 4
        # Create/Start cluster
        cluster = self.cluster
        cluster.set_configuration_options(values={'hinted_handoff_enabled': True}, batch_commitlog=True)
        cluster.populate(starting_size).start(wait_for_binary_proto=True, wait_other_notice=True,
                                              jvm_args=['--logger-log-level', 'stream_session=debug'])
        _, node2, node3, node4 = cluster.nodelist()

        # save node 3 process details
        node3_pid = node3.all_pids[0]
        node3_process = Process(node3_pid)
        executor = ThreadPoolExecutor(max_workers=3)

        def run_stress_write():
            debug('Run stress write on node 2')
            node2.stress(
                ['write', 'cl=QUORUM', 'no-warmup', 'duration=15m', '-mode', 'cql3', 'native', '-rate', 'threads=300',
                 '-pop', 'seq=1..100000000', '-log', 'interval=5'],
                capture_output=True)

        def run_decommission():
            try:
                debug('Decommission node 4')
                node4.decommission()
            except Exception:
                pass

        stress_thread = executor.submit(run_stress_write)
        decommission_thread = executor.submit(run_decommission)

        first_iteration = True
        while not decommission_thread.done():
            debug("Waiting until decommission_thread terminates...")
            if first_iteration:
                debug('Check logs and stop node 3')
                row = node4.watch_log_for("DECOMMISSIONING: unbootstrap starts")
                debug("Found the proper message in logs: {}".format(row))
                time.sleep(2)
                debug('Stop node 3')
                node3.stop()
                first_iteration = False
            time.sleep(10)

        debug('Get node 3 status')
        test_node_tool = TestNodetool()
        status = test_node_tool.nodetool_status(node2)
        node_3_status = None
        for s in status["nodes"]:
            if s['address'] == node3.address():
                node_3_status = s['status']
                break

        assert node_3_status is not None, "{} not found in {}".format(node3.address(), status["nodes"])

        if not stress_thread.done():
            debug('Cancel stress write')
            stress_thread.cancel()

        debug("Verifying node 3 status is DN")
        self.assertEqual("DN", node_3_status, "Node 3 status is incorrect (should be DN) Instead we got {}".format(
            node_3_status))
        debug("Verifying node 3 process is not running")
        self.assertEqual(False, node3_process.is_running(), "Node 3 process didn't stop/exit correctly")
