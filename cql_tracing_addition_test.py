from random import randint
from uuid import uuid4

from dtest import Tester, debug
from tools import rows_to_list
from cassandra.concurrent import execute_concurrent_with_args

# for type hints
from ccmlib.scylla_node import ScyllaNode
from ccmlib.scylla_cluster import ScyllaCluster
from cassandra.cluster import Session


class TracingReadAccess(Tester):

    def prepare_cluster(self, nodes=1, disable_cache=True):
        """Create, start cluster, create ks, cf

        create cf with column name:type:
            key (pk): uuid
            name : text
            rate : int

        :param nodes: [description], defaults to 1
        :type nodes: number, optional
        :param disable_cache: run cluster without cache
        :type disable_cache: boolean
        :returns: opened session
        :rtype: {Session}
        """
        if disable_cache:
            self.cluster.set_configuration_options(values={"enable_cache": "false"})
        self.cluster.populate(nodes).start(wait_for_binary_proto=True)
        node = self.cluster.nodelist()[0]  # type: ScyllaNode
        session = self.patient_cql_connection(node)  # type: Session
        self.create_ks(session, "ks", rf=nodes)
        self.create_cf(session, "cf", key_type="text", columns={"name": "text", "rate": "int"})

        return session

    def insertinto_table(self, session, rows=1):
        """Fill table with data

        use cf with columns created in self.prepare_cluster
        :param session: Cql session
        :type session: Session
        :param rows: number of rows, defaults to 1
        :type rows: number, optional
        """
        statement = session.prepare("INSERT INTO ks.cf (key, name, rate)"
                                    "VALUES (?, ?, ?)")
        execute_concurrent_with_args(session, statement,
                                     map(lambda x, y, z: [x, y, z],
                                         ["{}".format(uuid4()) for _ in range(rows)],
                                         ["lastname_{}".format(randint(1, 1000)) for _ in range(rows)],
                                         [randint(1, 100) for _ in range(rows)]))

    def update_table(self, session, rows=1):
        """Fill table with data

        use cf with columns created in self.prepare_cluster
        :param session: Cql session
        :type session: Session
        :param rows: number of rows, defaults to 1
        :type rows: number, optional
        """

        result = session.execute("SELECT key FROM cf")
        keys = rows_to_list(result)

        statement = session.prepare("INSERT INTO ks.cf (key, name, rate)"
                                    "VALUES (?, ?, ?)")
        execute_concurrent_with_args(session, statement,
                                     map(lambda x, y, z: [x, y, z],
                                         [key[0] for key in keys[:rows]],
                                         ["lastname_{}".format(randint(1, 1000)) for _ in range(rows)],
                                         [randint(1, 100) for _ in range(rows)]))

    def restart_node(self, node):
        """Restart node

        Restart node for reading data from sstable
        :param node: node to restart
        :type node: ScyllaNode
        """
        node.stop()
        node.start(wait_for_binary_proto=True)

    def restart_cluster(self):
        self.cluster.stop()
        self.cluster.start(wait_for_binary_proto=True)

    def assert_sstable_read_access(self, output, node):
        """verify tracing info in output

        Verify that result contains tracing info
        for I/O read sstables
        :param output: result of query with sstable
        :type output: str
        :param node: Node where operations run
        :type node: ScyllaNode
        """
        self.assertRegexpMatches(
            output,
            r"Reading partition range .*data/ks/cf-.*/mc-[\d]*-big-Data\.db.*{}".format(node.address()))
        self.assertRegexpMatches(
            output,
            r"data/ks/cf-.*/mc-[\d]*-big-Index.db: scheduling bulk DMA read of size [\d]* at offset [\d]*.*{}".format(node.address()))
        self.assertRegexpMatches(
            output,
            r"data/ks/cf-.*/mc-[\d]*-big-Index.db: finished bulk DMA read of size [\d]* at offset [\d]*, successfully read [\d]* bytes.*{}".format(node.address()))

    def test_tracing_one_node_one_sstable(self):
        """test tracing read access of sstable

        Testing that read from 1 sstable displayed
        correctly
        """
        session = self.prepare_cluster(nodes=1)  # type: Session
        node = self.cluster.nodelist()[0]  # type: ScyllaNode
        node.nodetool('settraceprobability 1.0')
        self.insertinto_table(session, rows=1)

        # flush memtable to sstable
        node.flush()

        # Read all data with tracing on
        out, err = node.run_cqlsh('TRACING ON; '
                                  'SELECT * '
                                  'FROM ks.cf',
                                  return_output=True, cqlsh_options=['--no-color'])

        debug(out)
        # Assert Reading partitions from sstable
        self.assertFalse(err)
        self.assert_sstable_read_access(out, node)

    def test_tracing_one_node_several_sstables(self):
        """test tracing I/O reads for several sstables

        """
        session = self.prepare_cluster(nodes=1)
        node = self.cluster.nodelist()[0]  # type: ScyllaNode
        node.nodetool('settraceprobability 1.0')
        self.insertinto_table(session, rows=4)
        node.flush()
        self.update_table(session, rows=2)
        node.flush()

        # Read all data with tracing on
        out, err = node.run_cqlsh('TRACING ON; '
                                  'SELECT * '
                                  'FROM ks.cf',
                                  return_output=True, cqlsh_options=['--no-color'])
        debug(out)
        print out
        # Assert Reading partitions from sstable
        self.assertFalse(err)
        self.assert_sstable_read_access(out, node)

    def test_tracing_for_all_nodes_sstables(self):
        session = self.prepare_cluster(nodes=3)
        self.insertinto_table(session, rows=50)
        for node in self.cluster.nodelist():
            node.nodetool('settraceprobability 1.0')
            node.flush()

        node = self.cluster.nodelist()[0]
        out, err = node.run_cqlsh('TRACING ON; '
                                  'SELECT * '
                                  'FROM ks.cf',
                                  return_output=True, cqlsh_options=['--no-color'])

        # Assert Reading partitions from sstable
        self.assertFalse(err)
        self.assert_sstable_read_access(out, node)
