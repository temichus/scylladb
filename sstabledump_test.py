import os
import tempfile
import json
import math
import uuid
from datetime import timedelta
from dateutil.parser import parse
from decimal import Decimal
import pprint
from dtest import Tester, debug
from nose.plugins.attrib import attr
from tools import rows_to_list
from cqlsh_tests.cqlsh_copy_tests import CqlshPrepare


class SSTableDump(Tester):

    def _fetch_data_from_json(self, data):
        res = list()
        for section in data:
            name = section['partition']['key'][0]
            values = [int(v['value']) for v in section['rows'][0]['cells']]
            values.insert(0, name)
            res.append(values)
        res = [tuple(item) for item in res]
        return res

    def _dump_data(self):
        debug('Run sstabledump')
        self.node.flush()
        json_path = tempfile.mktemp(suffix='.schema.json')
        with open(json_path, 'w') as fdw:
            self.node.run_sstable2json(out_file=fdw, keyspace='ks')

        with open(json_path, 'r') as fdr:
            data = fdr.read()
        data_json = json.loads(data.replace("][", ","))

        os.unlink(json_path)
        return data_json

    def _compare_data(self, src, dst, debug_print=True):
        debug('Compare data')
        if debug_print:
            debug('----------src----------')
            debug(src)
            debug('----------dst----------')
            debug(dst)
        symmetric_diff = set(src) ^ set(dst)
        self.assertEquals(len(symmetric_diff), 0)


@attr('dtest-full', 'single_node')
class SSTableDumpTests(SSTableDump):

    @attr('next-gating')
    @attr('dtest-debug')
    def sstabledump_basic_test(self):
        """
        Populate data, run sstabledump, extract data from json
        and compare it with a source
        """
        cluster = self.cluster
        cluster.populate(1).start()
        self.node = cluster.nodelist()[0]

        session = self.patient_cql_connection(self.node)
        session.execute("""CREATE KEYSPACE ks
            WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };
        """)
        session.execute("""CREATE TABLE ks.cf (
            name text PRIMARY KEY,
            value_one int,
            value_two int
            );
        """)

        debug('Insert data')
        values_list = [('mary', 1, 12), ('sara', 2, 24), ('mike', 3, 36), ('ted', 4, 48)]
        for values in values_list:
            session.execute("INSERT INTO ks.cf (name, value_one, value_two) VALUES {};".format(values))

        data_json = self._dump_data()
        json_values = self._fetch_data_from_json(data_json)
        self._compare_data(values_list, json_values)

    @attr('next-gating')
    @attr('dtest-debug')
    def sstabledump_counter_basic_test(self):
        """
        Populate counter data, run sstabledump, extract data from json
        and compare it with a source
        """
        cluster = self.cluster
        cluster.populate(1).start()
        self.node = cluster.nodelist()[0]

        session = self.patient_cql_connection(self.node)
        session.execute("""CREATE KEYSPACE ks
            WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };
        """)
        session.execute("""CREATE TABLE ks.cf (
            pk INT PRIMARY KEY,
            cnt COUNTER
            );
        """)

        for i in range(1, 10):
            session.execute("UPDATE ks.cf SET cnt = cnt + {} WHERE pk = {};".format(i, i))
            val = int(math.pow(10, 18))
            session.execute("UPDATE ks.cf SET cnt = cnt + {} WHERE pk = {};".format(val, i))

        rows = rows_to_list(session.execute("SELECT * FROM ks.cf;"))
        values_list = [tuple(item) for item in rows]

        data_json = self._dump_data()
        json_values = self._fetch_counter_data_from_json(data_json)
        self._compare_data(values_list, json_values)

    def _fetch_counter_data_from_json(self, data):
        res = list()
        for section in data:
            key = section['partition']['key'][0]
            values = [int(v['value'][56:], 16) for v in section['rows'][0]['cells']]
            values.insert(0, int(key))
            res.append(values)
        res = [tuple(item) for item in res]
        return res


@attr('dtest-full', 'single_node')
class SSTableDumpAllDatatypes(CqlshPrepare, SSTableDump):

    @attr('next-gating')
    @attr('dtest-debug')
    def sstabledump_all_datatypes_test(self):
        cluster = self.cluster
        cluster.populate(1).start()
        self.all_datatypes_prepare(nodes=1)

        # TODO: apply all the data on scylla-tools-java #24 fix
        data = self.data[:19] + self.data[21:]
        self.session.execute('ALTER TABLE ks.testdatatype DROP t')
        self.session.execute('ALTER TABLE ks.testdatatype DROP u')
        insert_statement = self.session.prepare(
            # """INSERT INTO testdatatype (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v, w)
            # VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""")
            """INSERT INTO testdatatype (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, v, w)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""")
        self.session.execute(insert_statement, data)

        exp_results = rows_to_list(self.session.execute("SELECT * FROM testdatatype"))
        debug(exp_results)

        self.node = self.node1
        data_json = self._dump_data()
        pp = pprint.PrettyPrinter(indent=2)
        debug(pp.pformat(data_json))

        json_values = self._fetch_data_from_json(data_json)
        self._compare_data(data, json_values)

    def _fetch_data_from_json(self, data):
        res = list()
        p = list()
        q = list()
        r = dict()
        format_val = {'b': lambda v: int(v),
                      'c': lambda v: bytearray.fromhex(v[2:]),
                      'd': lambda v: json.loads(v) if type(v) == type(str()) else v,
                      'e': lambda v: Decimal(v),
                      'f': lambda v: float(v),
                      'g': lambda v: float(v),
                      'i': lambda v: int(v),
                      'j': lambda v: v.encode('utf-8'),
                      'k': lambda v: (parse(v)),
                      'l': lambda v: uuid.UUID(v),
                      'm': lambda v: uuid.UUID(v),
                      'o': lambda v: int(v),
                      'p': lambda v: p.append((int(v))),
                      'q': lambda v: q.extend(v),
                      'r': lambda x, y: r.update(
                          {(parse(x)): str(y)}),
                      's': lambda v: tuple([json.loads(val) if i != 1 else val for i, val in enumerate(v.split(':'))]) if type(v) == type(str()) else tuple(v),
                      }

        for item in data[0]['rows'][0]['cells']:
            if 'value' in item:
                name = item['name']
                value = item['value']
                if name in format_val:
                    if name == 'q':
                        value = format_val[name](item['path'])
                    elif name == 'r':
                        value = format_val[name](item['path'][0], value)
                    else:
                        value = format_val[name](value)
                if name not in ('p', 'q', 'r'):
                    res.append(value)
        res.insert(14, p)
        res.insert(15, set(q))
        res.insert(16, r)
        res.insert(0, data[0]['partition']['key'][0])
        res = tuple(res)
        return res

    def _compare_data(self, src, dst, debug_print=True):
        debug('Compare data')
        if debug_print:
            debug('----------src----------')
            debug(src)
            debug('----------dst----------')
            debug(dst)

        # TODO: compare all when it will be readable
        for i in range(0, len(src) - 2):
            if isinstance(dst[i], dict):
                self.assertEquals(dict(src[i]), dst[i])
            if isinstance(src[i], str) and isinstance(dst[i], bytes):
                self.assertEquals(src[i], dst[i].decode('utf-8'))
            else:
                self.assertEquals(src[i], dst[i])
