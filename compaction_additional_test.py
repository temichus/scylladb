import tempfile
import time
import os
import shutil
import glob

from assertions import assert_none
from dtest import Tester


class CompactionAdditionalTest(Tester):

    def compaction_delete_with_smp_change_test(self):
        """
        Test that data is not resurected when shared sstables
        are used
        1. smp=1 create sstable A with 100 keys
        2. shutdown
        3. boot with smp=2 (forcing step 1 sstables to be shared) delete all keys
        4. wait past gc_preiod
        5. insert a key forcing flush multiple times till a compaction is triggered
        6. stop and start the node
        7. check that no data was resurected and that some of the deletion markers still exist
        8. insert additional 100 keys forcing a flush multiple times till multiple compactions are trigerred
        9. check that no deletion marker is left and files have been removed
        """
        cluster = self.cluster
        cluster.populate(1)
        [node1] = cluster.nodelist()
        node1.start(wait_for_binary_proto=True, jvm_args=['--smp', '1'])

        session = self.patient_cql_connection(node1)
        self.create_ks(session, 'ks', 1)

        session.execute("create table ks.cf (key int PRIMARY KEY, val int) with compaction = {'class':'SizeTieredCompactionStrategy'} and gc_grace_seconds = 30;")

        for x in range(0, 100):
            session.execute('insert into cf (key, val) values (' + str(x) + ',1)')

        node1.flush()
        node1.compact()
        node1.stop()
        node1.start(wait_for_binary_proto=True, jvm_args=['--smp', '2'])

        session = self.patient_cql_connection(node1, 'ks')
        for x in range(0, 100):
            session.execute('delete from cf where key = ' + str(x))
        node1.flush()

        time.sleep(31)

        # we passed gc_period and force an update so that compaction will
        # be triggered on a single shard (removing data and tombstone)
        rows = session.execute("select count(*) from system.compaction_history")
        compactions_1 = rows[0][0]
        compactions_2 = compactions_1

        while compactions_1 == compactions_2:
            session.execute('insert into ks.cf (key, val) values (199,1);')
            node1.flush()
            rows = session.execute("select count(*) from system.compaction_history")
            compactions_2 = rows[0][0]
        node1.wait_for_compactions()

        # reboot and verify that data  is not resurected
        node1.stop()
        node1.start(wait_for_binary_proto=True, jvm_args=['--smp', '2'])

        session = self.patient_cql_connection(node1, 'ks')
        for x in range(0, 100):
            assert_none(session, 'select * from cf where key = ' + str(x))

        # verify that only some deletion markers will be kept since we reshard the files
        # and gc_preiod passed so some tombstones have been removed by compaction
        json_path = tempfile.mkstemp(suffix='.json')
        jname = json_path[1]
        with open(jname, 'w') as f:
            node1.run_sstable2json(f)

        with open(jname, 'r') as g:
            jsoninfo = g.read()

        numfound = jsoninfo.count("markedForDeleteAt")

        self.assertLess(numfound, 100)
        self.assertGreater(numfound, 0)

        # trigger compaction on both shards
        node1.wait_for_compactions()
        rows = session.execute("select count(*) from system.compaction_history")
        compactions_1 = rows[0][0]
        compactions_2 = compactions_1

        while compactions_1 + 2 > compactions_2:
            for x in range(200, 300):
                session.execute('insert into ks.cf (key, val) values (' + str(x) + ',1);')
            node1.flush()
            rows = session.execute("select count(*) from system.compaction_history")
            compactions_2 = rows[0][0]
        node1.wait_for_compactions()

        # validate that all deletion markers have been removed
        json_path = tempfile.mkstemp(suffix='.json')
        jname = json_path[1]
        with open(jname, 'w') as f:
            node1.run_sstable2json(f)

        with open(jname, 'r') as g:
            jsoninfo = g.read()

        numfound = jsoninfo.count("markedForDeleteAt")

        self.assertEqual(numfound, 0)


class CompactionAdditionalStrategyTests(Tester):
    __test__ = False

    def __init__(self, *args, **kwargs):
        kwargs['cluster_options'] = {'start_rpc': 'true'}
        Tester.__init__(self, *args, **kwargs)

    def compaction_is_started_on_boot_test(self):
        cluster = self.cluster
        cluster.populate(1).start()
        [node1] = cluster.nodelist()

        session = self.patient_cql_connection(node1)
        self.create_ks(session, 'ks', 1)
        session.execute("create table ks.cf (key int PRIMARY KEY, val int) with compaction = {'class':'" + self.strategy + "'};")

        for x in range(0, 100):
            session.execute('insert into cf (key, val) values (' + str(x) + ',1)')

        node1.flush()
        node1.compact()
        node1.stop()
        files = glob.glob(os.path.join(node1.get_path(), 'commitlogs', '*'))
        for f in files:
            os.remove(f)

        keyspace_dir = os.path.join(node1.get_path(), 'data', 'ks')
        sstablefiles = glob.glob(glob.glob(os.path.join(keyspace_dir, 'cf' + '-*', '*-TOC.txt'))[0].replace('TOC.txt', '') + '*')
        for f in sstablefiles:
            for generation_suffix in xrange(10, 40):
                self._copy_sstable_file(f, "9999%d" % generation_suffix)

        before_start_count = len(glob.glob(os.path.join(keyspace_dir, 'cf' + '-*', '*-Data.db')))

        node1.start()
        time.sleep(30)

        after_start_count = len(glob.glob(os.path.join(keyspace_dir, 'cf' + '-*', '*-Data.db')))

        self.assertNotEqual(before_start_count, after_start_count)

    def _copy_sstable_file(self, file, generation):
        sstable_split_parts = os.path.basename(file).split('-')
        sstable_split_parts[-2] = generation
        shutil.copy(file, os.path.join(os.path.dirname(file), '-'.join(sstable_split_parts)))


strategies = ['LeveledCompactionStrategy', 'SizeTieredCompactionStrategy', 'DateTieredCompactionStrategy']
for strategy in strategies:
    cls_name = ('CompactionAdditionalStrategyTests_with_' + strategy)
    vars()[cls_name] = type(cls_name, (CompactionAdditionalStrategyTests,), {'strategy': strategy, '__test__': True})
