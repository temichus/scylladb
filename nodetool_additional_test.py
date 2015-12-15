from dtest import Tester
import re
import os
from tools import no_vnodes


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
        m = m + key + " is " + container[key] + " not >=" + str(val)
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
        except TypeError:
            self.assertEqual(val, container[key], m)

    def assertMapBetween(self, container, key, a, b, msg=None):
        if msg is None:
            m = key
        else:
            m = msg + " " + key
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

    def nodetool_status(self, node):
        res = {}
        out = node.nodetool("status", True)[0]
        m = re.findall('Datacenter: ([^\s+])', out, re.MULTILINE)
        if m:
            res['Datacenter'] = m[0]
        m = re.findall('^([UDNLJM]+)\s+([\d\.]+)\s+([^\s]+)\s+([^\s]+)\s+([^\s]+)\s+([^\s]+)\s+([^\s]+)\s*$', out, re.MULTILINE)
        res["nodes"] = [self._list2status(s) for s in m]
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

        strs = self.stress_write(node1)
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
        self.assertMapEqual(ks, "Write Count", 150000)
        self.assertMapEqual(ks, "Read Count", 0)
        self.assertEqual(150000, int(ks["Write Count"]) + int(ks["Read Count"]))
        table = ks["tables"][table_name]
        self.assertMapBetween(table, "SSTable count", 3, 8)
        self.assertMapEqual(table, "Number of keys (estimate)", 100000)
        self.assertMapGreatEqual(table, "Memtable cell count", 0)

        strs = self.stress_mixed(node1)
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
        self.assertMapGreatEqual(table, "Index summary off heap memory used", 1000)
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
        cluster = self.cluster
        cluster.populate(1).start(wait_for_binary_proto=True)
        [node1] = cluster.nodelist()
        cursor = self.patient_cql_connection(node1)
        strs = self.stress_write(node1, 1000)
        out = node1.nodetool("snapshot", True)[0]
        m = re.findall(r"Snapshot directory:\s+(\d+)", out, re.MULTILINE)
        snapshot = m[0]
        self.assertTrue(m, "No directory found in node snapshot command: '" + out + "'")

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

    @staticmethod
    def _list2ring(lst):
        heads = ["address", "rack", "status", "state", "load", "unit", "owns", "token"]
        res = {}
        for index, attribute in enumerate(heads):
            res[attribute] = lst[index].strip()
        return res

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

    def _compact(self, keyspace):
        cluster = self.cluster
        cluster.populate(1).start(wait_for_binary_proto=True)
        [node1] = cluster.nodelist()
        cursor = self.patient_cql_connection(node1)

        self.stress_write(node1)
        output = self._to_cfstats(node1.nodetool('cfstats keyspace1.standard1', True)[0])
        self.assertIn("keyspace1", output, "Keyspace is missing")
        self.assertIn("tables", output["keyspace1"], "Keyspace has no tables")
        self.assertIn("standard1", output["keyspace1"]["tables"], "Column family standard1 is missing")
        table = output["keyspace1"]["tables"]["standard1"]
        self.assertMapGreatEqual(table, "SSTable count", 1)
        sstable = int(table["SSTable count"])
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

    def stress_write(self, node, times=100000):
        return node.stress_object(['write', 'n=' + str(times)])

    def stress_mixed(self, node):
        return node.stress_object(['mixed', 'n=100000'])
