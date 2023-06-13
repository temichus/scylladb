import logging
import os
import tempfile
import json
import math
import uuid

import pytest
from dateutil.parser import parse
from decimal import Decimal
import pprint

from dtest_class import Tester
from tools.data import rows_to_list
from cqlsh_tests.cqlsh_copy_tests import CqlshPrepare

logger = logging.getLogger(__name__)


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
        logger.debug('Run sstabledump')
        self.node.flush()
        json_path = tempfile.mktemp(suffix='.schema.json')
        with open(json_path, 'w') as fdw:
            self.node.run_sstable2json(out_file=fdw, keyspace='ks')

        with open(json_path, 'r') as fdr:
            data = fdr.read()
        if 'as the config file' in data:
            # need to skip first line cause: https://github.com/scylladb/scylla-tools-java/issues/213
            data = '\n'.join(data.split('\n')[1:])
        data_json = json.loads(data.replace("][", ","))

        os.unlink(json_path)
        return data_json

    def _compare_data(self, src, dst, debug_print=True):
        logger.debug('Compare data')
        if debug_print:
            logger.debug('----------src----------')
            logger.debug(src)
            logger.debug('----------dst----------')
            logger.debug(dst)
        symmetric_diff = set(src) ^ set(dst)
        assert len(symmetric_diff) == 0, f"Destination data set is not same as source. " \
                                         f"Found difference:\n{symmetric_diff} "


@pytest.mark.dtest_full
@pytest.mark.single_node
# disable "uuid_sstable_identifier_enabled", as node.run_sstable2json() uses
# sstabledump under the hood, but sstabledump is not able to parse the sstable
# component's file name if the sstable uses uuid-based identifier instead of
# the integer-based generation.
@pytest.mark.cluster_options(uuid_sstable_identifiers_enabled=False)
class TestSSTableDump(SSTableDump):

    @pytest.mark.next_gating
    @pytest.mark.dtest_debug
    def test_sstabledump_basic(self):
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

        logger.debug('Insert data')
        values_list = [('mary', 1, 12), ('sara', 2, 24), ('mike', 3, 36), ('ted', 4, 48)]
        for values in values_list:
            session.execute("INSERT INTO ks.cf (name, value_one, value_two) VALUES {};".format(values))

        data_json = self._dump_data()
        json_values = self._fetch_data_from_json(data_json)
        self._compare_data(values_list, json_values)

    @pytest.mark.next_gating
    @pytest.mark.dtest_debug
    def test_sstabledump_counter_basic(self):
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

    @staticmethod
    def _fetch_counter_data_from_json(data):
        res = list()
        for section in data:
            key = section['partition']['key'][0]
            values = [int(v['value'][56:], 16) for v in section['rows'][0]['cells']]
            values.insert(0, int(key))
            res.append(values)
        res = [tuple(item) for item in res]
        return res


@pytest.mark.dtest_full
@pytest.mark.single_node
# disable "uuid_sstable_identifier_enabled", as node.run_sstable2json() uses
# sstabledump under the hood, but sstabledump is not able to parse the sstable
# component's file name if the sstable uses uuid-based identifier instead of
# the integer-based generation.
@pytest.mark.cluster_options(uuid_sstable_identifiers_enabled=False)
class TestSSTableDumpAllDatatypes(CqlshPrepare, SSTableDump):

    @pytest.mark.next_gating
    @pytest.mark.dtest_debug
    def test_sstabledump_all_datatypes(self):
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
        logger.debug(exp_results)

        self.node = self.node1
        data_json = self._dump_data()
        pp = pprint.PrettyPrinter(indent=2)
        logger.debug(pp.pformat(data_json))

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
        logger.debug('Compare data')
        if debug_print:
            logger.debug('----------src----------')
            logger.debug(src)
            logger.debug('----------dst----------')
            logger.debug(dst)

        # TODO: compare all when it will be readable
        for i in range(0, len(src) - 2):
            if isinstance(dst[i], dict):
                assert dict(src[i]) == dst[i], f"Destination data: {dst[i]} is not same as source: {dict(src[i])}"
            if isinstance(src[i], str) and isinstance(dst[i], bytes):
                assert src[i] == dst[i].decode('utf-8'), \
                    f"Destination data: {dst[i].decode('utf-8')} is not same as source: {src[i]}"
            else:
                assert src[i] == dst[i], f"Destination data: {dst[i]} is not same as source: {src[i]}"
