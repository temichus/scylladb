from dtest import Tester
import re


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

    def stress_write(self, node):
        return node.stress_object(['write', 'n=100000'])

    def stress_mixed(self, node):
        return node.stress_object(['mixed', 'n=100000'])
