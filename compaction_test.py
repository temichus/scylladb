import os
import re
import tempfile
import time
import random

from assertions import assert_none, assert_one
from dtest import Tester, debug
from tools import since, create_c1c2_table, insert_c1c2, chunks_list
from nose.plugins.attrib import attr
from unittest import skip


@attr('dtest-full', 'single_node')
class TestCompaction(Tester):

    __test__ = False

    def __init__(self, *args, **kwargs):
        kwargs['cluster_options'] = {'start_rpc': 'true'}
        Tester.__init__(self, *args, **kwargs)

    def setUp(self):
        Tester.setUp(self)

    def _compaction_delete_test(self):
        cluster = self.cluster
        cluster.populate(1).start(wait_for_binary_proto=True)
        [node1] = cluster.nodelist()

        session = self.patient_cql_connection(node1)
        self.create_ks(session, 'ks', 1)

        gc_grace_seconds = 60
        session.execute("create table ks.cf (key int PRIMARY KEY, val int) "
                        "with compaction = {{'class':'{}'}} and gc_grace_seconds = {};".format(self.strategy, gc_grace_seconds))

        for x in range(0, 100):
            session.execute('insert into cf (key, val) values (' + str(x) + ',1)')

        node1.flush()
        self.tombstone_expiry_time = time.time() + gc_grace_seconds
        for x in range(0, 10):
            session.execute('delete from cf where key = ' + str(x))

        node1.flush()
        for x in range(0, 10):
            assert_none(session, 'select * from cf where key = ' + str(x))

        json_path = tempfile.mkstemp(suffix='.json')
        jname = json_path[1]
        with open(jname, 'w') as f:
            node1.run_sstable2json(f)

        with open(jname, 'r') as g:
            jsoninfo = g.read()

        numfound = jsoninfo.count("marked_deleted")

        self.assertEqual(numfound, 10, "Error: expected {} deleted partitions but found {}:\n{}".format(10, numfound, jsoninfo))


    @since('2.2.X')
    def compaction_delete_test(self):
        """
        Test that executing a delete properly tombstones a row.
        Insert data, delete a partition of data and check that the requisite rows are tombstoned.
        """
        self._compaction_delete_test()


    @since('2.2.X')
    def compaction_delete_2_test(self):
        """
        Test that executing a delete properly tombstones a row.
        Insert data, delete a partition of data, compact and test
        Wait past gc_period, compact and test.
        """

        self._compaction_delete_test()
        [node1] = self.cluster.nodelist()
        session = self.patient_cql_connection(node1)

        # check that after compaction the tombstones remain
        # force an update so that compact will have something to do
        session.execute('insert into ks.cf (key, val) values (99,1);')
        node1.flush()
        node1.compact()

        json_path = tempfile.mkstemp(suffix='.json')
        jname = json_path[1]
        with open(jname, 'w') as f:
            node1.run_sstable2json(f)

        with open(jname, 'r') as g:
            jsoninfo = g.read()

        numfound = jsoninfo.count("marked_deleted")

        time_to_expire = self.tombstone_expiry_time - time.time()
        debug("Time left to expire: {}".format(time_to_expire))
        self.assertTrue(time_to_expire > 0, "Error: missed tombstone expiration time: {} >= {}".format(time.time(), self.tombstone_expiry_time))

        self.assertEqual(numfound, 10, "Error: expected {} deleted partitions but found {}:\n{}".format(10, numfound, jsoninfo))

        time.sleep(time_to_expire + 1)

        # check that after gc_period compaction removes tombstones
        # force an update so that compact will have something to do
        session.execute('insert into ks.cf (key, val) values (99,1);')
        node1.flush()
        node1.compact()

        json_path = tempfile.mkstemp(suffix='.json')
        jname = json_path[1]
        with open(jname, 'w') as f:
            node1.run_sstable2json(f)

        with open(jname, 'r') as g:
            jsoninfo = g.read()

        numfound = jsoninfo.count("marked_deleted")

        self.assertEqual(numfound, 0, "Error: expected {} deleted partitions but found {}:\n{}".format(0, numfound, jsoninfo))

    def data_size_test(self):
        """
        Ensure that data size does not have unwarranted increases after compaction.
        Insert data and check data size before and after a compaction.
        """
        cluster = self.cluster
        cluster.populate(1).start(wait_for_binary_proto=True)
        [node1] = cluster.nodelist()

        stress_write(node1)

        node1.flush()

        table_name = 'standard1'
        output = node1.nodetool('cfstats', True)[0]
        if output.find(table_name) != -1:
            output = output[output.find(table_name):]
            output = output[output.find("Space used (live)"):]
            initial_value = int(output[output.find(":") + 1:output.find("\n")].strip())
        else:
            debug("datasize not found")
            debug(output)

        block_on_compaction_log(node1)

        output = node1.nodetool('cfstats', True)[0]
        if output.find(table_name) != -1:
            output = output[output.find(table_name):]
            output = output[output.find("Space used (live)"):]
            final_value = int(output[output.find(":") + 1:output.find("\n")].strip())
        else:
            debug("datasize not found")

        self.assertLessEqual(final_value, initial_value)

    @attr('next-gating')
    @attr('dtest-debug')
    def sstable_deletion_test(self):
        """
        Test that sstables are deleted properly when able after compaction.
        Insert data setting gc_grace_seconds to 0, and determine sstable
        is deleted upon data deletion.
        """
        self.skip_if_no_major_compaction()
        cluster = self.cluster
        cluster.populate(1).start(wait_for_binary_proto=True)
        [node1] = cluster.nodelist()
        session = self.patient_cql_connection(node1)
        self.create_ks(session, 'ks', 1)
        session.execute("create table cf (key int PRIMARY KEY, val int) with gc_grace_seconds = 0 and "
                        "compaction= {'class':'" + self.strategy + "'}")

        for x in range(0, 100):
            session.execute('insert into cf (key, val) values (' + str(x) + ',1)')
        node1.flush()
        for x in range(0, 100):
            session.execute('delete from cf where key = ' + str(x))

        block_on_compaction_log(node1, ks='ks', table='cf')
        time.sleep(1)

        try:
            cfs = os.listdir(node1.get_path() + "/data/ks")
            ssdir = os.listdir(node1.get_path() + "/data/ks/" + cfs[0])
            for afile in ssdir:
                self.assertFalse("Data" in afile)

        except OSError:
            self.fail("Path to sstables not valid.")

    @skip('sstable is already removed when expecting it to be marked as expired only')
    def dtcs_deletion_test(self):
        """
        Test that sstables are deleted properly when able after compaction with
        DateTieredCompactionStrategy.
        Insert data setting max_sstable_age_days low, and determine sstable
        is deleted upon data deletion past max_sstable_age_days.
        """
        if not hasattr(self, 'strategy'):
            self.strategy = 'DateTieredCompactionStrategy'
        elif self.strategy != 'DateTieredCompactionStrategy':
            self.skipTest('Not implemented unless DateTieredCompactionStrategy is used')

        cluster = self.cluster
        cluster.populate(1).start(wait_for_binary_proto=True)
        [node1] = cluster.nodelist()
        session = self.patient_cql_connection(node1)
        self.create_ks(session, 'ks', 1)
        # max sstable age is 0.5 minute:
        session.execute("create table cf (key int PRIMARY KEY, val int) with gc_grace_seconds = 0 and "
                        "compaction= {'class':'DateTieredCompactionStrategy', 'max_sstable_age_days':0.00035, "
                        "'min_threshold':2}")

        # insert data
        for x in range(0, 300):
            session.execute('insert into cf (key, val) values (' + str(x) + ',1) USING TTL 35')
        node1.flush()
        time.sleep(40)
        expired_sstables = node1.get_sstables('ks', 'cf')
        self.assertEqual(len(expired_sstables), 1)
        expired_sstable = expired_sstables[0]
        # write a new sstable to make DTCS check for expired sstables:
        for x in range(0, 100):
            session.execute('insert into cf (key, val) values (%d, %d)' % (x, x))
        node1.flush()
        time.sleep(5)
        # we only check every 10 minutes - sstable should still be there:
        assert expired_sstable in node1.get_sstables('ks', 'cf')

        session.execute("alter table cf with "
                        "compaction =  {'class':'DateTieredCompactionStrategy', 'max_sstable_age_days':0.00035, "
                        "'min_threshold':2, 'expired_sstable_check_frequency_seconds':0}")
        time.sleep(1)
        for x in range(0, 100):
            session.execute('insert into cf (key, val) values (%d, %d)' % (x, x))
        node1.flush()
        time.sleep(5)
        assert expired_sstable not in node1.get_sstables('ks', 'cf')

    @skip('nodetool command setcompactionthroughput not supported in scylla')
    def compaction_throughput_test(self):
        """
        Test setting compaction throughput.
        Set throughput, insert data and ensure compaction performance corresponds.
        """
        cluster = self.cluster
        cluster.populate(1).start(wait_for_binary_proto=True)
        [node1] = cluster.nodelist()

        # disableautocompaction only disables compaction for existing tables,
        # so initialize stress tables with stress first
        stress_write(node1, keycount=1)
        node1.nodetool('disableautocompaction')

        stress_write(node1, keycount=200000)

        threshold = "5"
        node1.nodetool('setcompactionthroughput -- ' + threshold)

        matches = block_on_compaction_log(node1)
        stringline = matches[0]
        throughput_pattern = re.compile('''.*          # it doesn't matter what the line starts with
                                           =           # wait for an equals sign
                                           ([\s\d\.]*) # capture a decimal number, possibly surrounded by whitespace
                                           MB/s.*      # followed by 'MB/s'
                                        ''', re.X)

        avgthroughput = re.match(throughput_pattern, stringline).group(1).strip()
        debug(avgthroughput)

        self.assertGreaterEqual(float(threshold), float(avgthroughput))

    def compaction_strategy_switching_test(self):
        """Ensure that switching strategies does not result in problems.
        Insert data, switch strategies, then check against data loss.
        """
        strategies = ['LeveledCompactionStrategy', 'SizeTieredCompactionStrategy', 'DateTieredCompactionStrategy',
                      'TimeWindowCompactionStrategy']

        if self.strategy in strategies:
            strategies.remove(self.strategy)
            cluster = self.cluster
            cluster.populate(1).start(wait_for_binary_proto=True)
            [node1] = cluster.nodelist()

            for strat in strategies:
                session = self.patient_cql_connection(node1)
                self.create_ks(session, 'ks', 1)

                session.execute("create table ks.cf (key int PRIMARY KEY, val int) with gc_grace_seconds = 0 "
                                "and compaction= {'class':'" + self.strategy + "'};")

                for x in range(0, 100):
                    session.execute('insert into ks.cf (key, val) values (' + str(x) + ',1)')

                node1.flush()

                for x in range(0, 10):
                    session.execute('delete from cf where key = ' + str(x))

                session.execute("alter table ks.cf with compaction = {'class':'" + strat + "'};")

                for x in range(11, 100):
                    assert_one(session, "select * from ks.cf where key =" + str(x), [x, 1])

                for x in range(0, 10):
                    assert_none(session, 'select * from cf where key = ' + str(x))

                node1.flush()
                cluster.clear()
                time.sleep(5)
                cluster.start(wait_for_binary_proto=True)

    @skip('large row is most likely not being produce, hence test is failing')
    def large_compaction_warning_test(self):
        """
        @jira_ticket CASSANDRA-9643
        Check that we log a warning when the partition size is bigger than
        compaction_large_partition_warning_threshold_mb
        """
        cluster = self.cluster
        cluster.set_configuration_options({'compaction_large_partition_warning_threshold_mb': 1})
        cluster.populate(1).start(wait_for_binary_proto=True)
        [node] = cluster.nodelist()

        session = self.patient_cql_connection(node)
        self.create_ks(session, 'ks', 1)

        mark = node.mark_log()
        strlen = (1024 * 1024) / 100
        session.execute("CREATE TABLE large(userid text PRIMARY KEY, properties map<int, text>) with compression = {}")
        for i in range(200):  # ensures partition size larger than compaction_large_partition_warning_threshold_mb
            session.execute("UPDATE ks.large SET properties[%i] = '%s' "
                            "WHERE userid = 'user'" % (i, get_random_word(strlen)))

        ret = list(session.execute("SELECT properties from ks.large where userid = 'user'"))
        assert len(ret) == 1
        self.assertEqual(200, len(ret[0][0].keys()))

        node.flush()

        node.nodetool('compact ks large')
        node.watch_log_for('Writing large row ks/large:.* \(\d+ bytes\)', from_mark=mark, timeout=180)

        ret = list(session.execute("SELECT properties from ks.large where userid = 'user'"))

        assert len(ret) == 1
        self.assertEqual(200, len(ret[0][0].keys()))

        # Check that system.large_partitions contains this large entry
        large_partition_ret = list(session.execute("SELECT * from system.large_partitions"))
        self.assertEqual(len(large_partition_ret), 1)
        row = large_partition_ret[0]
        self.assertEqual(row.partition_size, 2104020)
        self.assertEqual(row.partition_key, 'user')

    def disable_autocompaction_nodetool_test(self):
        """
        Make sure we can enable/disable compaction using nodetool
        """
        node = self.prepate_testbed()
        disable_mark = self.disable_autocompaction(node=node, ks=self.primary_ks, table=self.primary_table)
        self.fill_table_with_data(node=node, ks=self.primary_ks, table=self.primary_table, keys=1000)
        self.fill_table_with_data(node=node, ks=self.secondary_ks, table=self.secondary_table, keys=1000)
        self.assertTrue(len(node.grep_log(f'Compacting.+{self.primary_table}', from_mark=disable_mark)) == 0,
                        f'Found compaction log items for {self.strategy}')
        self.assertTrue(len(node.grep_log(f'Compacting.+{self.secondary_table}', from_mark=disable_mark)) > 0,
                        f'{self.secondary_ks}.{self.secondary_table} should have continued with regular compactions')
        enable_mark = self.enable_autocompaction(node=node, ks=self.primary_ks, table=self.primary_table)
        self.fill_table_with_data(node=node, ks=self.primary_ks, table=self.primary_table, keys=1000)
        self.fill_table_with_data(node=node, ks=self.secondary_ks, table=self.secondary_table, keys=1000)
        self.assertTrue(len(node.grep_log(f'Compacting.+{self.primary_table}', from_mark=enable_mark)) > 0,
                        f'Found no log items for {self.strategy}')
        self.assertTrue(len(node.grep_log(f'Compacting.+{self.secondary_table}', from_mark=enable_mark)) > 0,
                        f'{self.secondary_ks}.{self.secondary_table} should have continued with regular compactions')

    @skip('nodetool enableautocompaction does not work in Scylla if the schema has it disabled')
    def disable_autocompaction_schema_test(self):
        """
        Make sure we can disable compaction via the schema compaction parameter 'enabled' = false
        """
        node = self.prepate_testbed(with_compaction=f'WITH compaction = {{\'class\':\'{self.strategy}\', '
                                                    f'\'enabled\':\'false\'}}')
        disable_mark = self.disable_autocompaction(node=node, ks=self.primary_ks, table=self.primary_table)
        self.fill_table_with_data(node=node, ks=self.primary_ks, table=self.primary_table, keys=1000)
        self.fill_table_with_data(node=node, ks=self.secondary_ks, table=self.secondary_table, keys=1000)
        self.assertTrue(len(node.grep_log(f'Compacting.+{self.primary_table}', from_mark=disable_mark)) == 0,
                        f'Found compaction log items for {self.strategy}')
        self.assertTrue(len(node.grep_log(f'Compacting.+{self.secondary_table}', from_mark=disable_mark)) > 0,
                        f'{self.secondary_ks}.{self.secondary_table} should have continued with regular compactions')
        # should still be disabled after restart:
        node.stop()
        node.start(wait_for_binary_proto=True)
        session = self.patient_cql_connection(node)
        session.execute('use ks')
        self.assertTrue(len(node.grep_log(f'Compacting.+{self.primary_table}', from_mark=disable_mark)) == 0,
                        f'Found compaction log items for {self.strategy}')
        self.assertTrue(len(node.grep_log(f'Compacting.+{self.secondary_table}', from_mark=disable_mark)) > 0,
                        f'{self.secondary_ks}.{self.secondary_table} should have continued with regular compactions')
        # TODO: in Scylla it doesn't work, so i shall run here alter table and update with the `enabled: true`
        enable_mark = self.enable_autocompaction(node=node, ks=self.primary_ks, table=self.primary_table)
        self.fill_table_with_data(node=node, ks=self.primary_ks, table=self.primary_table, keys=1000)
        self.fill_table_with_data(node=node, ks=self.secondary_ks, table=self.secondary_table, keys=1000)
        self.assertTrue(len(node.grep_log(f'Compacting.+{self.primary_table}', from_mark=enable_mark)) > 0,
                        f'Found no log items for {self.strategy}')
        self.assertTrue(len(node.grep_log(f'Compacting.+{self.secondary_table}', from_mark=enable_mark)) > 0,
                        f'{self.secondary_ks}.{self.secondary_table} should have continued with regular compactions')

    def disable_autocompaction_alter_test(self):
        """
        Make sure we can enable compaction using an alter-statement
        """
        node = self.prepate_testbed()
        session = self.patient_cql_connection(node)
        session.execute('use ks')
        session.execute('ALTER TABLE to_disable '
                        f'WITH compaction = {{\'class\':\'{self.strategy}\', \'enabled\':\'false\'}}')
        disable_mark = node.mark_log()
        # the API used on is_autocompaction_enabled doesn't return correct values if they are set in the table schema
        self.fill_table_with_data(node=node, ks=self.primary_ks, table=self.primary_table, keys=1000)
        self.fill_table_with_data(node=node, ks=self.secondary_ks, table=self.secondary_table, keys=1000)
        self.assertTrue(len(node.grep_log(f'Compacting.+{self.primary_table}', from_mark=disable_mark)) == 0,
                        f'Found compaction log items for {self.strategy}')
        self.assertTrue(len(node.grep_log(f'Compacting.+{self.secondary_table}', from_mark=disable_mark)) > 0,
                        f'{self.secondary_ks}.{self.secondary_table} should have continued with regular compactions')
        enable_mark = node.mark_log()
        session.execute('ALTER TABLE to_disable '
                        f'WITH compaction = {{\'class\':\'{self.strategy}\', \'enabled\':\'true\'}}')
        # the API used on is_autocompaction_enabled doesn't return correct values if they are set in the table schema
        self.fill_table_with_data(node=node, ks=self.primary_ks, table=self.primary_table, keys=1000)
        self.fill_table_with_data(node=node, ks=self.secondary_ks, table=self.secondary_table, keys=1000)
        self.assertTrue(len(node.grep_log(f'Compacting.+{self.primary_table}', from_mark=enable_mark)) > 0,
                        f'Found no log items for {self.strategy}')
        self.assertTrue(len(node.grep_log(f'Compacting.+{self.secondary_table}', from_mark=enable_mark)) > 0,
                        f'{self.secondary_ks}.{self.secondary_table} should have continued with regular compactions')

    def disable_autocompaction_alter_and_nodetool_test(self):
        """
        Make sure compaction stays disabled after an alter statement where we have disabled using nodetool first
        """
        node = self.prepate_testbed()
        session = self.patient_cql_connection(node)
        session.execute('use ks')
        disable_mark = self.disable_autocompaction(node=node, ks=self.primary_ks, table=self.primary_table)
        self.fill_table_with_data(node=node, ks=self.primary_ks, table=self.primary_table, keys=1000)
        self.fill_table_with_data(node=node, ks=self.secondary_ks, table=self.secondary_table, keys=1000)
        self.assertTrue(len(node.grep_log(f'Compacting.+{self.primary_table}', from_mark=disable_mark)) == 0,
                        f'Found compaction log items for {self.strategy}')
        self.assertTrue(len(node.grep_log(f'Compacting.+{self.secondary_table}', from_mark=disable_mark)) > 0,
                        f'{self.secondary_ks}.{self.secondary_table} should have continued with regular compactions')
        session.execute('ALTER TABLE to_disable '
                        f'WITH compaction = {{\'class\':\'{self.strategy}\', \'tombstone_threshold\':0.9}}')
        session.execute('insert into to_disable (key, c1, c2) values (\'99\', \'hello\', \'hello\')')
        new_disable_mark = node.mark_log()
        self.fill_table_with_data(node=node, ks=self.primary_ks, table=self.primary_table, keys=1000)
        self.fill_table_with_data(node=node, ks=self.secondary_ks, table=self.secondary_table, keys=1000)
        self.assertTrue(len(node.grep_log(f'Compacting.+{self.primary_table}', from_mark=disable_mark)) == 0,
                        f'Found compaction log items for {self.strategy}')
        self.assertTrue(len(node.grep_log(f'Compacting.+{self.secondary_table}', from_mark=new_disable_mark)) > 0,
                        f'{self.secondary_ks}.{self.secondary_table} should have continued with regular compactions')
        enable_mark = self.enable_autocompaction(node=node, ks=self.primary_ks, table=self.primary_table)
        self.fill_table_with_data(node=node, ks=self.primary_ks, table=self.primary_table, keys=1000)
        self.fill_table_with_data(node=node, ks=self.secondary_ks, table=self.secondary_table, keys=1000)
        self.assertTrue(len(node.grep_log(f'Compacting.+{self.primary_table}', from_mark=enable_mark)) > 0,
                        f'Found no log items for {self.strategy}')
        self.assertTrue(len(node.grep_log(f'Compacting.+{self.secondary_table}', from_mark=enable_mark)) > 0,
                        f'{self.secondary_ks}.{self.secondary_table} should have continued with regular compactions')

    def disable_autocompaction_without_params_test(self):
        """
        Make sure compaction is disabled even if no ks and table name params are passed to the nodetool command
        """
        node = self.prepate_testbed()
        node.nodetool('disableautocompaction')
        disable_mark = node.mark_log()
        self.assertFalse(self.is_autocompaction_enabled(node, self.primary_ks, self.primary_table),
                         'Expected to have autocompaction disabled but got it is enabled')
        self.assertFalse(self.is_autocompaction_enabled(node, self.secondary_ks, self.secondary_table),
                         'All keyspaces and tables are expected to be affected by disableautocompaction')
        self.fill_table_with_data(node=node, ks=self.primary_ks, table=self.primary_table, keys=1000)
        self.fill_table_with_data(node=node, ks=self.secondary_ks, table=self.secondary_table, keys=1000)
        self.assertTrue(len(node.grep_log(f'Compacting.+{self.primary_table}', from_mark=disable_mark)) == 0,
                        f'Found compaction log items for {self.strategy}')
        self.assertTrue(len(node.grep_log(f'Compacting.+{self.secondary_table}', from_mark=disable_mark)) == 0,
                        f'{self.secondary_ks}.{self.secondary_table} should not have stopped with regular compactions')
        node.nodetool('enableautocompaction')
        enable_mark = node.mark_log()
        self.assertTrue(self.is_autocompaction_enabled(node, self.primary_ks, self.primary_table),
                        'Expected to have autocompaction enabled but got it is disabled')
        self.assertTrue(self.is_autocompaction_enabled(node, self.secondary_ks, self.secondary_table),
                        'All keyspaces and tables are expected to be affected by enableautocompaction')
        self.fill_table_with_data(node=node, ks=self.primary_ks, table=self.primary_table, keys=1000)
        self.fill_table_with_data(node=node, ks=self.secondary_ks, table=self.secondary_table, keys=1000)
        self.assertTrue(len(node.grep_log(f'Compacting.+{self.primary_table}', from_mark=enable_mark)) > 0,
                        f'Found no log items for {self.strategy}')
        self.assertTrue(len(node.grep_log(f'Compacting.+{self.secondary_table}', from_mark=enable_mark)) > 0,
                        f'{self.secondary_ks}.{self.secondary_table} should have continued with regular compactions')

    def disable_autocompaction_twice_test(self):
        """
        Make sure disabling compaction command executed twice in a row doesn't fail
        """
        node = self.prepate_testbed()
        disable_mark = self.disable_autocompaction(node=node, ks=self.primary_ks, table=self.primary_table)
        self.fill_table_with_data(node=node, ks=self.primary_ks, table=self.primary_table, keys=1000)
        self.fill_table_with_data(node=node, ks=self.secondary_ks, table=self.secondary_table, keys=1000)
        self.assertTrue(len(node.grep_log(f'Compacting.+{self.primary_table}', from_mark=disable_mark)) == 0,
                        f'Found compaction log items for {self.strategy}')
        self.assertTrue(len(node.grep_log(f'Compacting.+{self.secondary_table}', from_mark=disable_mark)) > 0,
                        f'{self.secondary_ks}.{self.secondary_table} should have continued with regular compactions')
        self.disable_autocompaction(node=node, ks=self.primary_ks, table=self.primary_table)
        self.disable_autocompaction(node=node, ks=self.primary_ks, table=self.primary_table)
        self.fill_table_with_data(node=node, ks=self.primary_ks, table=self.primary_table, keys=1000)
        self.fill_table_with_data(node=node, ks=self.secondary_ks, table=self.secondary_table, keys=1000)
        self.assertTrue(len(node.grep_log(f'Compacting.+{self.primary_table}', from_mark=disable_mark)) == 0,
                        f'Found compaction log items for {self.strategy}')
        self.assertTrue(len(node.grep_log(f'Compacting.+{self.secondary_table}', from_mark=disable_mark)) > 0,
                        f'{self.secondary_ks}.{self.secondary_table} should have continued with regular compactions')

    primary_ks = 'ks'
    primary_table = 'to_disable'
    secondary_ks = 'ks2'
    secondary_table = 'std1'

    def disable_autocompaction(self, node, ks, table, verify=True):
        node.nodetool(f'disableautocompaction {ks} {table}')
        mark = node.mark_log()
        if verify:
            self.assertFalse(self.is_autocompaction_enabled(node, ks, table),
                             'Expected to have autocompaction disabled but got it is enabled')
            self.assertTrue(self.is_autocompaction_enabled(node, self.secondary_ks, self.secondary_table),
                            'Other keyspaces and tables are not supposed to be affected by disableautocompaction')
        return mark

    def enable_autocompaction(self, node, ks, table, verify=True):
        node.nodetool(f'enableautocompaction {ks} {table}')
        mark = node.mark_log()
        if verify:
            self.assertTrue(self.is_autocompaction_enabled(node, ks, table),
                            'Expected to have autocompaction enabled but got it is disabled')
            self.assertTrue(self.is_autocompaction_enabled(node, self.secondary_ks, self.secondary_table),
                            'Expected to have autocompaction enabled but got it is disabled')
        return mark

    def create_ks_and_table(self, node, ks, table, with_compaction=None):
        session = self.patient_cql_connection(node)
        self.create_ks(session, ks, 1)
        if not with_compaction:
            with_compaction = f'{{\'class\':\'{self.strategy}\'}}'
        create_c1c2_table(tester=self, session=session, cf=table, compaction=with_compaction)

    def fill_table_with_data(self, node, ks, table, keys, flush=True):
        session = self.patient_cql_connection(node)
        session.execute(f'use {ks}')
        for chunk in chunks_list(list(range(keys)), 100):
            insert_c1c2(session=session, keys=chunk, consistency=0, cf=table)
            if flush:
                node.flush()

    def prepate_testbed(self, with_compaction=None):
        cluster = self.cluster
        cluster.populate(1).start(wait_for_binary_proto=True)
        [node] = cluster.nodelist()
        self.create_ks_and_table(node=node, ks=self.primary_ks, table=self.primary_table,
                                 with_compaction=with_compaction)
        self.create_ks_and_table(node=node, ks=self.secondary_ks, table=self.secondary_table)
        return node

    def skip_if_no_major_compaction(self):
        if self.cluster.version() < '2.2' and self.strategy == 'LeveledCompactionStrategy':
            self.skipTest('major compaction not implemented for LCS in this version of Cassandra')


def get_random_word(word_len):
    word = ''
    for i in range(word_len):
        word += random.choice('ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789')
    return word


def block_on_compaction_log(node, ks=None, table=None):
    """
    @param node the node on which to trigger and block on compaction
    @param ks the keyspace to compact
    @param table the table to compact

    Helper method for testing compaction. This triggers compactions by
    calling flush and compact on node. In situations where major
    compaction won't apply to a table, such as in pre-2.2 LCS tables, the
    flush will trigger minor compactions.

    This method uses log-watching to block until compaction is completed.

    By default, this method uses the keyspace and table names generated by
    cassandra-stress. These will not be used if ks and table names parameters
    are passed in.

    Calling flush before calling this method may cause it to hang; if
    compaction completes before the method starts, it may not occur again
    during this method.
    """
    if node.is_scylla() or node.get_cassandra_version() < '2.2':
        log_file = 'system.log'
    else:
        log_file = 'debug.log'
    mark = node.mark_log(filename=log_file)
    node.flush()

    # on newer C* versions, default stress names are titlecased
    stress_keyspace, stress_table = ('keyspace1', 'standard1')

    ks = ks or stress_keyspace
    table = table or stress_table

    node.nodetool('compact {ks} {table}'.format(ks=ks, table=table))

    return node.watch_log_for('Compacted', from_mark=mark, filename=log_file)


def stress_write(node, keycount=100000):
    node.stress(['write', f'n={keycount}'])


strategies = ['LeveledCompactionStrategy', 'SizeTieredCompactionStrategy', 'DateTieredCompactionStrategy',
              'TimeWindowCompactionStrategy']
for strategy in strategies:
    cls_name = ('TestCompaction_with_' + strategy)
    vars()[cls_name] = type(cls_name, (TestCompaction,), {'strategy': strategy, '__test__': True})
