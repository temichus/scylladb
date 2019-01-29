import distutils.dir_util
import glob
import os
import shutil
import subprocess
import time
import uuid

from cassandra.concurrent import execute_concurrent_with_args
from threading import Thread

from dtest import Tester, debug
from tools import safe_mkdtemp, replace_in_file, require


class SnapshotTester(Tester):

    """
    Object with utility functions to perform snapshot operations.
    """

    def __init__(self, *args, **kwargs):
        Tester.__init__(self, *args, **kwargs)

    def insert_rows(self, session, start, end):
        insert_statement = session.prepare("INSERT INTO ks.cf (key, val) VALUES (?, 'asdf')")
        args = [(r,) for r in range(start, end)]
        execute_concurrent_with_args(session, insert_statement, args, concurrency=20)

    def make_snapshot(self, node, ks, cf, name):
        debug("Making snapshot....")
        node.flush()
        snapshot_cmd = 'snapshot {ks} -cf {cf} -t {name}'.format(**locals())
        debug("Running snapshot cmd: {snapshot_cmd}".format(snapshot_cmd=snapshot_cmd))
        node.nodetool(snapshot_cmd)
        tmpdir = safe_mkdtemp()
        os.mkdir(os.path.join(tmpdir, ks))
        os.mkdir(os.path.join(tmpdir, ks, cf))
        node_dir = node.get_path()

        # Find the snapshot dir, it's different in various C* versions:
        snapshot_dir = "{node_dir}/data/{ks}/{cf}/snapshots/{name}".format(**locals())
        if not os.path.isdir(snapshot_dir):
            snapshot_dir = glob.glob("{node_dir}/data/{ks}/{cf}-*/snapshots/{name}".format(**locals()))[0]
        debug("snapshot_dir is : " + snapshot_dir)
        debug("snapshot copy is : " + tmpdir)

        # Copy files from the snapshot dir to existing temp dir
        distutils.dir_util.copy_tree(str(snapshot_dir), os.path.join(tmpdir, ks, cf))

        return tmpdir

    def restore_snapshot_with_sstableloader(self, snapshot_dir, node, ks, cf):
        debug("Restoring snapshot....")
        snapshot_dir = os.path.join(snapshot_dir, ks, cf)
        ip = node.address()

        args = [node.get_tool('sstableloader'), '-d', ip, snapshot_dir]
        p = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = p.communicate()
        exit_status = p.wait()

        if exit_status != 0:
            raise Exception("sstableloader command '%s' failed; exit status: %d'; stdout: %s; stderr: %s" %
                            (" ".join(args), exit_status, stdout, stderr))

    def restore_snapshot_with_refresh(self, snapshot_dir, node, ks, cf):
        debug("Restoring snapshot....")
        node_dir = node.get_path()
        restore_dir = "{node_dir}/data/{ks}/{cf}/".format(**locals())
        if not os.path.isdir(restore_dir):
            restore_dir = glob.glob("{node_dir}/data/{ks}/{cf}-*/".format(**locals()))[0]
        snapshot_dir = os.path.join(snapshot_dir, ks, cf)
        debug("Copying from %s to %s" % (str(snapshot_dir), str(restore_dir)))
        distutils.dir_util.copy_tree(snapshot_dir, restore_dir)
        node.nodetool("refresh %s %s" % (ks, cf))


class TestSnapshot(SnapshotTester):

    """
    Test snapshot operations.
    """

    def __init__(self, *args, **kwargs):
        SnapshotTester.__init__(self, *args, **kwargs)

    def test_basic_snapshot_and_restore_with_sstableloader(self):
        """
        Test basic snapshot and restore using an sstable loader.
        """
        self.basic_snapshot_and_restore(use_sstableloader=True)

    def test_basic_snapshot_and_restore_with_refresh(self):
        """
        Test basic snapshot and restore without an sstable loader.
        """
        self.basic_snapshot_and_restore(use_sstableloader=False)

    def basic_snapshot_and_restore(self, use_sstableloader):
        """
        Base testing method:

        1. Create a keyspace
        2. Create a column family
        3. Insert 100 rows into the column family
        4. Take a snapshot
        5. Insert more rows after the snapshot
        6. Drop the keyspace, assure we have no data after the deletion
        7. Restore the snapshot
        8. Verify we have the same num of rows inserted prior to the snapshot.

        @param use_sstableloader: Whether to use sstable loader to
            restore the snapshot.
        """
        cluster = self.cluster
        cluster.populate(1).start()
        (node1,) = cluster.nodelist()
        session = self.patient_cql_connection(node1)
        self.create_ks(session, 'ks', 1)
        session.execute('CREATE TABLE ks.cf ( key int PRIMARY KEY, val text);')

        self.insert_rows(session, 0, 100)
        snapshot_dir = self.make_snapshot(node1, 'ks', 'cf', 'basic')

        # Write more data after the snapshot, this will get thrown
        # away when we restore:
        self.insert_rows(session, 100, 200)
        rows = session.execute('SELECT count(*) from ks.cf')
        self.assertEqual(rows[0][0], 200)

        # Drop the keyspace, make sure we have no data:
        session.execute('DROP KEYSPACE ks')
        shutil.rmtree(os.path.join(node1.get_path(), 'data', 'ks'))
        self.create_ks(session, 'ks', 1)
        session.execute('CREATE TABLE ks.cf ( key int PRIMARY KEY, val text);')
        rows = session.execute('SELECT count(*) from ks.cf')
        self.assertEqual(rows[0][0], 0)

        # Restore data from snapshot:
        if use_sstableloader:
            self.restore_snapshot_with_sstableloader(snapshot_dir, node1, 'ks', 'cf')
        else:
            self.restore_snapshot_with_refresh(snapshot_dir, node1, 'ks', 'cf')
        node1.nodetool('refresh ks cf')
        rows = session.execute('SELECT count(*) from ks.cf')

        # clean up
        debug("removing snapshot_dir: " + snapshot_dir)
        shutil.rmtree(snapshot_dir)

        self.assertEqual(rows[0][0], 100)

    def restore_snapshot_with_alter_table(self, drop=False):
        """
        1. create table (A INT,B INT ,C INT)
        2. insert and flush data
        3. alter table: rename C to D / drop B
        4. insert and flush data
        5. create snapshot
        6. stop node and clear data
        7. try to load snapshot using sstableloader
        """
        cluster = self.cluster
        cluster.populate(1).start()
        (node1,) = cluster.nodelist()
        session = self.patient_cql_connection(node1)
        self.create_ks(session, 'ks', 1)
        session.execute('CREATE TABLE ks.cf (key varchar, A int, B int, C int, PRIMARY KEY(key, c));')

        query = "INSERT INTO ks.cf (key, A, B, C) VALUES ('{}', 11, 22, 33);".format(str(uuid.uuid1()))
        session.execute(query)
        node1.nodetool("flush -- ks")

        debug('Alter table...')
        session.execute("ALTER TABLE ks.cf RENAME C TO D;")
        if drop:
            session.execute("ALTER TABLE ks.cf DROP B;")
        query = "INSERT INTO ks.cf (key, A, D) VALUES ('{}', 44, 55);".format(str(uuid.uuid1()))
        session.execute(query)
        node1.nodetool("flush -- ks")

        snapshot_dir = self.make_snapshot(node1, 'ks', 'cf', 'basic')

        # clear data
        cluster.stop(gently=True)
        debug('Clear data...')
        data_path = '{}/data/ks/cf-*'.format(node1.get_path())
        data_files = glob.glob(os.path.join(data_path, '*'))
        for f in data_files:
            if os.path.isfile(f):
                os.unlink(f)

        cluster.start()
        self.restore_snapshot_with_sstableloader(snapshot_dir, node1, 'ks', 'cf')

    def restore_snapshot_with_alter_table_test(self):
        self.restore_snapshot_with_alter_table()

    @require('#1470')
    def restore_snapshot_with_alter_table_drop_column_test(self):
        self.restore_snapshot_with_alter_table(drop=True)

    def test_nodetool_snapshot_race_condition_with_compaction_under_stress(self):
        # Cover Issue #4051 https://github.com/scylladb/scylla/issues/4051
        def run_stress(node):
            debug('Start stress command')
            results, errors = node.stress(['write', 'duration=5m', '-mode', 'cql3', 'native', '-rate', 'threads=100', '-pop', 'seq=1..100000000', '-log', 'interval=5'],
                                          capture_output=True)
            debug('Stress results:\n' + ''.join(results + errors))
            self.assertFalse(errors, "Some errors during stress %s" % errors)

        cluster = self.cluster
        cluster.populate(1).start()
        node1 = cluster.nodelist()[0]

        # Start stress in thread
        stress_run_th = Thread(target=run_stress, args=(node1, ))
        stress_run_th.start()

        # wait for 2,5min (150s) while db populated with data
        time.sleep(150)

        # run several snapshot commands
        for i in range(2):
            time.sleep(60)
            results, errors = node1.nodetool('snapshot')
            debug(results + errors)
            self.assertNotIn(
                'failed: filesystem error: link failed: No such file or directory',
                ' '.join(results + errors)
            )
            # Check that no other errors occured during snapshot command
            self.assertFalse(errors, "Some errors in creating snapshot: %s" % errors)

        # wait stress command completion.
        stress_run_th.join()

    def test_nodetool_snapshot_race_condition_with_compaction_after_node_start(self):

        cluster = self.cluster
        cluster.populate(1).start()
        node1 = cluster.nodelist()[0]

        debug('Run stress command')
        results, errors = node1.stress(['write', 'n=1000000', '-rate', 'threads=10'],
                                       capture_output=True)
        debug('Stress results:\n' + ''.join(results + errors))
        self.assertFalse(errors, "Some errors during stress %s" % errors)

        debug('Stoping node..')
        node1.stop()
        debug('Node has been stopped')

        debug('Starting node...')
        node1.start()
        debug('Node has been started')

        debug('Create snapshot right after start')
        result, errors = node1.nodetool('snapshot')
        debug(result + errors)
        self.assertNotIn(
            'failed: filesystem error: link failed: No such file or directory',
            ' '.join(results + errors)
        )
        # Check that no other errors occured during snapshot command
        self.assertFalse(errors, "Some errors in creating snapshot: %s" % errors)

    def test_nodetool_snapshot_race_condition_with_compaction_during_node_start(self):

        def run_node_start_in_thread(node):
            debug('Starting node...')
            node.start()
            debug('Node has been started')

        cluster = self.cluster
        cluster.populate(1).start()
        node1 = cluster.nodelist()[0]

        debug('Run stress command')
        results, errors = node1.stress(['write', 'n=1000000', '-rate', 'threads=10'],
                                       capture_output=True)
        debug('Stress results:\n' + ''.join(results + errors))
        self.assertFalse(errors, "Some errors during stress %s" % errors)

        debug('Stoping node..')
        node1.stop()
        debug('Node has been stopped')

        debug('Starting node in separate thread')
        node_start_thread = Thread(target=run_node_start_in_thread, args=(node1, ))
        node_start_thread.start()
        debug('Thread is starting...')
        time.sleep(2)

        debug('Create snapshot during node start')
        result, errors = node1.nodetool('snapshot')
        debug(result + errors)
        self.assertNotIn(
            'failed: filesystem error: link failed: No such file or directory',
            ' '.join(results + errors)
        )
        # Check that no other errors occured during snapshot command
        self.assertFalse(errors, "Some errors in creating snapshot: %s" % errors)

        node_start_thread.join()

    def test_nodetool_snapshot_during_major_compaction(self):

        def run_compaction(node):
            debug('Start compaction by command')
            node.compact()
            debug('Compaction done')

        cluster = self.cluster
        cluster.populate(1).start()
        node1 = cluster.nodelist()[0]

        debug('Run stress command')
        results, errors = node1.stress(['write', 'n=1000000', '-rate', 'threads=10'],
                                       capture_output=True)
        debug('Stress results:\n' + ''.join(results + errors))
        self.assertFalse(errors, "Some errors during stress %s" % errors)
        self.assertTrue(node1.is_live())

        compaction_thread = Thread(target=run_compaction, args=(node1, ))
        compaction_thread.start()
        time.sleep(0.5)
        debug('Create snapshot right after start')
        result, errors = node1.nodetool('snapshot')
        debug(result + errors)
        self.assertNotIn(
            'failed: filesystem error: link failed: No such file or directory',
            ' '.join(results + errors)
        )
        # Check that no other errors occured during snapshot command
        self.assertFalse(errors, "Some errors in creating snapshot: %s" % errors)

        compaction_thread.join()


class TestArchiveCommitlog(SnapshotTester):

    """
    Test operations with the archive commit log.
    """

    def __init__(self, *args, **kwargs):
        kwargs['cluster_options'] = {'commitlog_segment_size_in_mb': 1}
        SnapshotTester.__init__(self, *args, **kwargs)

    def make_snapshot(self, node, ks, cf, name):
        debug("Making snapshot....")
        node.flush()
        snapshot_cmd = 'snapshot {ks} -cf {cf} -t {name}'.format(**locals())
        debug("Running snapshot cmd: {snapshot_cmd}".format(snapshot_cmd=snapshot_cmd))
        node.nodetool(snapshot_cmd)
        tmpdir = safe_mkdtemp()
        node_dir = node.get_path()

        # Copy files from the snapshot dir to existing temp dir
        distutils.dir_util.copy_tree(os.path.join(node.get_path(), 'data', ks), tmpdir)

        return tmpdir

    def restore_snapshot(self, snapshot_dir, node, ks, cf, name):
        debug("Restoring snapshot for cf ....")
        data_dir = os.path.join(node.get_path(), 'data')
        cf_id = [s for s in os.listdir(snapshot_dir) if s.startswith(cf + "-")][0]
        snapshot_dir = glob.glob("{snapshot_dir}/{cf_id}/snapshots/{name}".format(**locals()))[0]
        if not os.path.exists(os.path.join(data_dir, ks)):
            os.mkdir(os.path.join(data_dir, ks))
        os.mkdir(os.path.join(data_dir, ks, cf_id))

        debug("snapshot_dir is : " + snapshot_dir)
        distutils.dir_util.copy_tree(snapshot_dir, os.path.join(data_dir, ks, cf_id))

    def test_archive_commitlog(self):
        self.run_archive_commitlog(restore_point_in_time=False)

    def test_archive_commitlog_with_active_commitlog(self):
        """
        Copy the active commitlogs to the archive directory before restoration
        """
        self.run_archive_commitlog(restore_point_in_time=False, archive_active_commitlogs=True)

    def dont_test_archive_commitlog(self):
        """
        Run the archive commitlog test, but forget to add the restore commands
        """
        self.run_archive_commitlog(restore_point_in_time=False, restore_archived_commitlog=False)

    def test_archive_commitlog_point_in_time(self):
        """
        Test archive commit log with restore_point_in_time setting
        """
        self.run_archive_commitlog(restore_point_in_time=True)

    def test_archive_commitlog_point_in_time_with_active_commitlog(self):
        """
        Test archive commit log with restore_point_in_time setting
        """
        self.run_archive_commitlog(restore_point_in_time=True, archive_active_commitlogs=True)

    def run_archive_commitlog(self, restore_point_in_time=False, restore_archived_commitlog=True, archive_active_commitlogs=False):
        """
        Run archive commit log restoration test
        """

        cluster = self.cluster
        cluster.populate(1)
        (node1,) = cluster.nodelist()

        # Create a temp directory for storing commitlog archives:
        tmp_commitlog = safe_mkdtemp()
        debug("tmp_commitlog: " + tmp_commitlog)

        # Edit commitlog_archiving.properties and set an archive
        # command:
        replace_in_file(os.path.join(node1.get_path(), 'conf', 'commitlog_archiving.properties'),
                        [(r'^archive_command=.*$', 'archive_command=cp %path {tmp_commitlog}/%name'.format(
                            tmp_commitlog=tmp_commitlog))])

        cluster.start()

        session = self.patient_cql_connection(node1)
        self.create_ks(session, 'ks', 1)
        session.execute('CREATE TABLE ks.cf ( key bigint PRIMARY KEY, val text);')
        debug("Writing first 30,000 rows...")
        self.insert_rows(session, 0, 30000)
        # Record when this first set of inserts finished:
        insert_cutoff_times = [time.gmtime()]

        # Delete all commitlog backups so far:
        for f in glob.glob(tmp_commitlog + "/*"):
            os.remove(f)

        snapshot_dir = self.make_snapshot(node1, 'ks', 'cf', 'basic')

        if self.cluster.version() >= '3.0':
            system_ks_snapshot_dir = self.make_snapshot(node1, 'system_schema', 'keyspaces', 'keyspaces')
        else:
            system_ks_snapshot_dir = self.make_snapshot(node1, 'system', 'schema_keyspaces', 'keyspaces')

        if self.cluster.version() >= '3.0':
            system_col_snapshot_dir = self.make_snapshot(node1, 'system_schema', 'columns', 'columns')
        else:
            system_col_snapshot_dir = self.make_snapshot(node1, 'system', 'schema_columns', 'columns')

        if self.cluster.version() >= '3.0':
            system_ut_snapshot_dir = self.make_snapshot(node1, 'system_schema', 'types', 'usertypes')
        else:
            system_ut_snapshot_dir = self.make_snapshot(node1, 'system', 'schema_usertypes', 'usertypes')

        if self.cluster.version() >= '3.0':
            system_cfs_snapshot_dir = self.make_snapshot(node1, 'system_schema', 'tables', 'cfs')
        else:
            system_cfs_snapshot_dir = self.make_snapshot(node1, 'system', 'schema_columnfamilies', 'cfs')

        try:
            # Write more data:
            debug("Writing second 30,000 rows...")
            self.insert_rows(session, 30000, 60000)
            node1.flush()
            time.sleep(10)
            # Record when this second set of inserts finished:
            insert_cutoff_times.append(time.gmtime())

            debug("Writing final 5,000 rows...")
            self.insert_rows(session, 60000, 65000)
            # Record when the third set of inserts finished:
            insert_cutoff_times.append(time.gmtime())

            rows = session.execute('SELECT count(*) from ks.cf')
            # Make sure we have the same amount of rows as when we snapshotted:
            self.assertEqual(rows[0][0], 65000)

            # Check that there are at least one commit log backed up that
            # is not one of the active commit logs:
            commitlog_dir = os.path.join(node1.get_path(), 'commitlogs')
            debug("node1 commitlog dir: " + commitlog_dir)

            self.assertTrue(len(set(os.listdir(tmp_commitlog)) - set(os.listdir(commitlog_dir))) > 0)

            cluster.flush()
            cluster.compact()
            node1.drain()
            if archive_active_commitlogs:
                # restart the node which causes the active commitlogs to be archived
                node1.stop()
                node1.start(wait_for_binary_proto=True)

            # Destroy the cluster
            cluster.stop()
            self.copy_logs(name=self.id().split(".")[0] + "_pre-restore")
            self._cleanup_cluster()
            cluster = self.cluster = self._get_cluster()
            cluster.populate(1)
            node1, = cluster.nodelist()

            # Restore schema from snapshots:
            if self.cluster.version() >= '3.0':
                self.restore_snapshot(system_ks_snapshot_dir, node1, 'system_schema', 'keyspaces', 'keyspaces')
            else:
                self.restore_snapshot(system_ks_snapshot_dir, node1, 'system', 'schema_keyspaces', 'keyspaces')

            if self.cluster.version() >= '3.0':
                self.restore_snapshot(system_col_snapshot_dir, node1, 'system_schema', 'columns', 'columns')
            else:
                self.restore_snapshot(system_col_snapshot_dir, node1, 'system', 'schema_columns', 'columns')

            if self.cluster.version() >= '3.0':
                self.restore_snapshot(system_ut_snapshot_dir, node1, 'system_schema', 'types', 'usertypes')
            else:
                self.restore_snapshot(system_ut_snapshot_dir, node1, 'system', 'schema_usertypes', 'usertypes')

            if self.cluster.version() >= '3.0':
                self.restore_snapshot(system_cfs_snapshot_dir, node1, 'system_schema', 'tables', 'cfs')
            else:
                self.restore_snapshot(system_cfs_snapshot_dir, node1, 'system', 'schema_columnfamilies', 'cfs')

            self.restore_snapshot(snapshot_dir, node1, 'ks', 'cf', 'basic')

            cluster.start(wait_for_binary_proto=True)

            session = self.patient_cql_connection(node1)
            node1.nodetool('refresh ks cf')

            rows = session.execute('SELECT count(*) from ks.cf')
            # Make sure we have the same amount of rows as when we snapshotted:
            self.assertEqual(rows[0][0], 30000)

            # Edit commitlog_archiving.properties. Remove the archive
            # command  and set a restore command and restore_directories:
            if restore_archived_commitlog:
                replace_in_file(os.path.join(node1.get_path(), 'conf', 'commitlog_archiving.properties'),
                                [(r'^archive_command=.*$', 'archive_command='),
                                 (r'^restore_command=.*$', 'restore_command=cp -f %from %to'),
                                 (r'^restore_directories=.*$', 'restore_directories={tmp_commitlog}'.format(
                                     tmp_commitlog=tmp_commitlog))])

                if restore_point_in_time:
                    restore_time = time.strftime("%Y:%m:%d %H:%M:%S", insert_cutoff_times[1])
                    replace_in_file(os.path.join(node1.get_path(), 'conf', 'commitlog_archiving.properties'),
                                    [(r'^restore_point_in_time=.*$', 'restore_point_in_time={restore_time}'.format(**locals()))])

            debug("Restarting node1..")
            node1.stop()
            node1.start(wait_for_binary_proto=True)

            node1.nodetool('flush')
            node1.nodetool('compact')

            session = self.patient_cql_connection(node1)
            rows = session.execute('SELECT count(*) from ks.cf')
            # Now we should have 30000 rows from the snapshot + 30000 rows
            # from the commitlog backups:
            if not restore_archived_commitlog:
                self.assertEqual(rows[0][0], 30000)
            elif restore_point_in_time:
                self.assertEqual(rows[0][0], 60000)
            else:
                self.assertEqual(rows[0][0], 65000)

        finally:
            # clean up
            debug("removing snapshot_dir: " + snapshot_dir)
            shutil.rmtree(snapshot_dir)
            debug("removing snapshot_dir: " + system_ks_snapshot_dir)
            shutil.rmtree(system_ks_snapshot_dir)
            debug("removing snapshot_dir: " + system_cfs_snapshot_dir)
            shutil.rmtree(system_cfs_snapshot_dir)
            debug("removing snapshot_dir: " + system_ut_snapshot_dir)
            shutil.rmtree(system_ut_snapshot_dir)
            debug("removing snapshot_dir: " + system_col_snapshot_dir)
            shutil.rmtree(system_col_snapshot_dir)
            debug("removing tmp_commitlog: " + tmp_commitlog)
            shutil.rmtree(tmp_commitlog)
