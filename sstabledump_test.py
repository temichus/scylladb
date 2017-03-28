import os
import tempfile
import json
import math
from dtest import Tester, debug
from tools import rows_to_list


class SSTableDumpTests(Tester):

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
        data_json = json.loads(data)

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

    def sstabledump_counter_basic_test(self):
        """
        Populate counter data, run sstabledump, extract data from json
        and compare it with a source
        """
        cluster = self.cluster
        cluster.set_configuration_options(values={'experimental': True})
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
