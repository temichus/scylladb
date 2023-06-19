import logging
import os
import subprocess
import time
from distutils import dir_util
from io import StringIO
import re

import pytest

from dtest_class import Tester, create_ks, create_cf
from dtest_setup_overrides import DTestSetupOverrides
from tools.misc import ImmutableMapping

logger = logging.getLogger(__name__)


@pytest.mark.dtest_full
class TestSSTableGenerationAndLoading(Tester):

    @pytest.fixture(scope='class', autouse=True)
    def fixture_dtest_setup_overrides(self, dtest_config):  # pylist:disable=unused-argument
        dtest_setup_overrides = DTestSetupOverrides()
        dtest_setup_overrides.cluster_options = ImmutableMapping({'start_rpc': 'true'})
        return dtest_setup_overrides

    @pytest.mark.single_node
    def test_promoted_index_generation_with_small_partition_followed_by_a_large_partition(self):
        """
        Tests for https://github.com/scylladb/scylla/issues/1567
        """
        cluster = self.cluster
        cluster.set_configuration_options(values={'enable_cache': False})
        cluster.populate(1).start()
        node1 = cluster.nodelist()[0]

        with self.patient_cql_connection(node1) as session:
            create_ks(session=session, name='ks', rf=1)
            session.execute('CREATE TABLE ks.test (pk int, ck text, s1 int static, v int, PRIMARY KEY (pk, ck));')
            session.execute('insert into ks.test (pk, s1) values (1, 7);')
            session.execute('insert into ks.test (pk, s1) values (0, 7);')

            for idx in range(2000):
                session.execute("insert into ks.test  (pk, ck, v) values (0, 'ck_%d', %d);" % (idx, idx))

        node1.stop()
        node1.start(wait_for_binary_proto=True)

        with self.patient_cql_connection(node1) as session:
            rows = list(session.execute('select * from ks.test where pk = 0 and ck = \'ck_0\';'))
        assert len(rows) == 1
        assert [0, 'ck_0', 7, 0], list(rows[0])

        # Fails due to https://github.com/scylladb/scylla/issues/1568
        # rows = list(session.execute('select * from ks.test where pk = 0 and ck = \'ck_45\''))
        # assert(len(rows) == 1)
        # assert [0, 'ck_45', 7, 45] == list(rows[0])

    @pytest.mark.single_node
    def test_incompressible_data_in_compressed_table(self):
        """
        tests for the bug that caused #3370:
        https://issues.apache.org/jira/browse/CASSANDRA-3370

        inserts random data into a compressed table. The compressed SSTable was
        compared to the uncompressed and was found to indeed be larger then
        uncompressed.
        """
        cluster = self.cluster
        cluster.populate(1).start()
        node1 = cluster.nodelist()[0]
        time.sleep(.5)

        with self.patient_cql_connection(node1) as session:
            create_ks(session=session, name='ks', rf=1)
            create_cf(session=session, name='cf', compression="Deflate")

            # make unique column names, and values that are incompressible
            for col in range(10):
                col_name = str(col)
                col_val = os.urandom(5000)
                col_val = col_val.hex()
                cql = "UPDATE cf SET v='%s' WHERE KEY='0' AND c='%s';" % (col_val, col_name)
                # print cql
                session.execute(cql)

            node1.flush()
            time.sleep(2)
            rows = list(session.execute("SELECT * FROM cf WHERE KEY = '0' AND c < '8';"))
            assert rows

    @pytest.mark.single_node
    def test_remove_index_file(self, fixture_dtest_setup):  # pylint:disable=too-many-statements, too-many-locals
        """
        tests for situations similar to that found in #343:
        https://issues.apache.org/jira/browse/CASSANDRA-343
        """
        cluster = self.cluster
        cluster.populate(1).start(wait_for_binary_proto=True)
        node1 = cluster.nodelist()[0]

        # Makinge sure the cluster is ready to accept the subsequent
        # stress connection. This was an issue on Windows.
        logger.info("Writing initial data")
        node1.stress(['write', 'n=10000', '-rate', 'threads=8'])

        # Query existing data and keep in original_rows
        with self.patient_cql_connection(node1) as session:
            stress_table = 'keyspace1.standard1'
            logger.info("Retrieving initial data")
            original_rows = list(session.execute("SELECT * FROM %s" % (stress_table,)))

        logger.info("Stopping node and removing summary")
        node1.flush()
        node1.compact()
        node1.stop()
        time.sleep(1)
        path = ""
        basepath = os.path.join(node1.get_path(), 'data', 'keyspace1')
        for dir_name in os.listdir(basepath):
            if dir_name.startswith("standard1"):
                path = os.path.join(basepath, dir_name)

        # Verify that Summary can be regenerated
        # and that the data is still there
        os.system('rm %s/*Summary.db' % path)

        logger.info("Starting node")
        node1.start(wait_for_binary_proto=True)
        with self.patient_cql_connection(node1) as session:
            logger.info("Verifying data")
            new_rows = list(session.execute("SELECT * FROM %s" % (stress_table,)))
            assert original_rows == new_rows

        logger.info("Stopping node")
        node1.stop()
        time.sleep(1)
        os.system('rm -rf %s/snapshots' % path)
        os.system('mkdir %s/snapshots' % path)

        fixture_dtest_setup.allow_log_errors = True
        fixture_dtest_setup.ignore_log_patterns += [
            r"database - Exception while populating keyspace 'keyspace1' with column family 'standard1' from file "
            r"'.*': sstables::malformed_sstable_exception \(.*: file not found\)",
            r"database - Exception while populating keyspace 'keyspace1' with column family 'standard1' from file "
            r"'.*': sstables::malformed_sstable_exception \(.*: No such file or directory\)",
            r"database - Exception while populating keyspace 'keyspace1' with column family 'standard1' from file "
            r"'.*': std::filesystem::__cxx11::filesystem_error \(error system:2, filesystem error: (open|stat) "
            r"failed: No such file or directory \[.*\]\)",
            r"database - Unrecognized error while processing .*: std::filesystem::__cxx11::filesystem_error "
            r"\(error system:2, filesystem error: (open|stat) failed: No such file or directory \[.*\]\)",
            r"database - malformed sstable .*: .*: file not found",
            r"database - malformed sstable .*: .*: No such file or directory",
            r"init - Startup failed: std::runtime_error"
        ]

        timeout = 10 if cluster.scylla_mode != 'debug' else 90

        # For each of these component files, verify that if it's removed
        # then the sstable is is detected is malformed but the data
        # file is not lost
        comps = ['Index.db', 'Filter.db', 'Statistics.db', 'Digest.*']
        for comp in comps:
            logger.info("Removing {comp}".format(**locals()))
            os.system("mv {path}/*{comp} {path}/snapshots/".format(**locals()))

            logger.info("Starting node, expected to fail")
            mark = node1.mark_log()
            node1.start(no_wait=True)
            node1.watch_log_for("malformed_sstable_exception", timeout=timeout, from_mark=mark)
            logger.info("Stopping node")
            node1.stop(wait=False, gently=False)
            time.sleep(1)

            data_found = 0
            for fname in os.listdir(path):
                if fname.endswith('Data.db'):
                    data_found += 1
            assert data_found > 0, "After removing %s, the data file was deleted!" % comp

            os.system("mv {path}/snapshots/*{comp} {path}/".format(**locals()))

        # Finally, verify that the data is still there after renaming
        # all components back.
        logger.info("Starting node")
        node1.start(wait_for_binary_proto=True)
        with self.patient_cql_connection(node1) as session:
            logger.info("Verifying data")
            new_rows = list(session.execute("SELECT * FROM %s" % (stress_table,)))
        assert original_rows == new_rows

    def load_sstable_with_configuration(self, pre_compression=None, post_compression=None, table_details=None):
        """
        tests that the sstableloader works by using it to load data.
        Compression of the columnfamilies being loaded, and loaded into
        can be specified.

        pre_compression and post_compression can be these values:
        None, 'Snappy', or 'Deflate'.
        """

        # pylint: disable=too-many-statements,too-many-locals
        num_keys = 1000
        table_details = table_details or dict()

        for compression_option in (pre_compression, post_compression):
            assert compression_option in (None, 'Snappy', 'Deflate')

        logger.info("Testing sstableloader with pre_compression=%s and post_compression=%s" %
                    (pre_compression, post_compression))

        cluster = self.cluster

        cluster.populate(2).start(wait_for_binary_proto=True, wait_other_notice=True)
        node1, node2 = cluster.nodelist()
        time.sleep(.5)

        def create_schema(_session, compression):
            create_ks(session=_session, name="ks", rf=2)
            create_cf(session=_session, name="standard1", compression=compression)
            create_cf(session=_session, name="counter1", compression=compression, columns={'v': 'counter'})
            for _table_name, _details in table_details.items():
                create_cf(session, _table_name, compression=compression, columns=_details['column'])

        logger.info("creating keyspace and inserting")
        with self.patient_cql_connection(node1) as session:
            create_schema(session, pre_compression)

            for idx in range(num_keys):
                session.execute("UPDATE standard1 SET v='%d' WHERE KEY='%d' AND c='col'" % (idx, idx))
                session.execute("UPDATE counter1 SET v=v+1 WHERE KEY='%d'" % idx)
            for table_name, details in table_details.items():
                column_name = list(details['column'].keys())[0]
                for old_value, new_value in zip(details['values'], details['values'][::-1]):
                    session.execute(f"UPDATE {table_name} SET {column_name}={old_value} WHERE KEY='{new_value}'")

        node1.nodetool('drain')
        node1.stop()
        node2.nodetool('drain')
        node2.stop()

        logger.info("Making a copy of the sstables")
        # make a copy of the sstables
        data_dir = os.path.join(node1.get_path(), 'data')
        copy_root = os.path.join(node1.get_path(), 'data_copy')
        for ddir in os.listdir(data_dir):
            keyspace_dir = os.path.join(data_dir, ddir)
            if os.path.isdir(keyspace_dir) and ddir != 'system':
                copy_dir = os.path.join(copy_root, ddir)
                dir_util.copy_tree(keyspace_dir, copy_dir)

        logger.info("Wiping out the data and restarting cluster")
        # wipe out the node data.
        cluster.clear()
        cluster.start(wait_for_binary_proto=True, wait_other_notice=True)

        logger.info("re-creating the keyspace and column families.")
        with self.patient_cql_connection(node1) as session:
            create_schema(session, post_compression)
        time.sleep(2)

        logger.info("Calling sstableloader")
        # call sstableloader to re-load each cf.
        host = node1.address()
        sstablecopy_dir = copy_root + '/ks'
        for cf_dir in os.listdir(sstablecopy_dir):
            full_cf_dir = os.path.join(sstablecopy_dir, cf_dir)
            if os.path.isdir(full_cf_dir):
                cmd_args = [node1.get_tool('sstableloader'), '--nodes', host, full_cf_dir]
                p_open = subprocess.Popen(cmd_args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                stdout, stderr = p_open.communicate()
                exit_status = p_open.returncode
                stderr = stderr.decode()
                stdout = stdout.decode()
                if stderr:
                    buf = StringIO(stderr)
                    has_errors = False
                    for line in buf:
                        if re.search('WARN .* Ignoring codec', line):
                            logger.debug(f"Ignoring sstableloader warning: {line.strip()}")
                        else:
                            has_errors = True
                    assert not has_errors, f"The stderr of sstableloader has errors: {stderr}"
                assert 'Error' not in stdout, f'The stdout contains error message: {stdout}'
                assert 'exception' not in stdout, f'The stdout contains exception message: {stdout}'
                assert exit_status == 0, f'sstableloader exited with a non-zero status: {exit_status}'

        def read_and_validate_data(_session):
            for _idx in range(num_keys):
                rows = list(_session.execute("SELECT * FROM standard1 WHERE KEY='%d'" % _idx))
                assert [str(_idx), 'col', str(_idx)] == list(rows[0])
                rows = list(_session.execute("SELECT * FROM counter1 WHERE KEY='%d'" % _idx))
                assert [str(_idx), 1] == list(rows[0])

        with self.patient_cql_connection(node1) as session:
            session.execute('USE ks;')
            logger.info("Reading data back")
            # Now we should have sstables with the loaded data, and the existing
            # data. Lets read it all to make sure it is all there.
            read_and_validate_data(session)

            logger.info("compacting, and repairing")
            # do some operations and try reading the data again.
            node1.nodetool('compact')
            node1.nodetool('repair')

            logger.info("Reading data back one more time")
            read_and_validate_data(session)

    @pytest.mark.parametrize("pre_compression,post_compression",
                             [(None, None), (None, 'Snappy'), (None, 'Deflate'), ('Snappy', None), ('Snappy', 'Snappy'),
                              ('Snappy', 'Deflate'), ('Deflate', None), ('Deflate', 'Snappy'), ('Deflate', 'Deflate')])
    @pytest.mark.cluster_options(uuid_sstable_identifiers_enabled=False)
    def test_sstableloader_compression(self, pre_compression, post_compression):
        self.load_sstable_with_configuration(pre_compression=pre_compression, post_compression=post_compression)

    @pytest.mark.cluster_options(uuid_sstable_identifiers_enabled=False)
    def test_sstableloader_case_sensitive_with_quotas(self):
        """
        The test checks that can load data from column with quotes without errors
        https://github.com/scylladb/scylla-enterprise-tools-java/issues/26
        """
        table_details = {'case_sensitive_quotes_table': {
            'column': {'"ColumnUpperCaseWithQuotas"': 'int'},
            'values': list(range(1000)),
        }}
        self.load_sstable_with_configuration(pre_compression='Deflate', post_compression='Deflate',
                                             table_details=table_details)
