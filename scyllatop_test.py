"""
All dtest functional test for scyllatop utils.
"""

import subprocess
import time
import signal
import os
import tempfile

from dtest import Tester, debug
from nose.plugins.attrib import attr


@attr('dtest-full')
class TestScyllaTop(Tester):

    def get_cli(self):
        node = self.cluster.nodelist()[0]
        cli = os.path.join(node.get_install_dir(), 'tools/scyllatop/scyllatop.py')

        # in relocatable packages the path is a bit different
        if not os.path.exists(cli):
            cli = os.path.join(node.get_install_dir(), 'scylla/bin/scyllatop')
        if not os.path.exists(cli):
            cli = os.path.join(node.get_install_dir(), 'scylla/opt/scylladb/scyllatop/scyllatop.py')

        t = tempfile.mkstemp(prefix='scyllatop.log.')
        os.close(t[0])
        logfile = t[1]
        cli += ' -L {} -p http://{}:9180/metrics -v DEBUG'.format(logfile, node.address())
        return (cli, logfile)

    def interactive_start(self, wait=True, sleep_time=10):
        """
        Common usage, start scyllatop without options
        """
        (cmd, logfile) = self.get_cli()
        debug(cmd)
        p = subprocess.Popen(cmd.split(), stdout=subprocess.PIPE, universal_newlines=True)
        if not wait:
            return (p, logfile)
        time.sleep(sleep_time)
        p.send_signal(signal.SIGINT)
        out, err = p.communicate()
        debug(out[0:40] + '...')
        debug('Length of output is %s' % len(out.split()))
        assert p.returncode == 0, err
        assert len(out) > 0, 'Output should not be empty'
        os.remove(logfile)

    def batch_mode_start(self, wait=True, n=1):
        """
        Start scyllatop in batch mode
        """
        (cmd, logfile) = self.get_cli()
        cmd = "%s -b -n %s" % (cmd, n)
        debug(cmd)
        p = subprocess.Popen(cmd.split(), stdout=subprocess.PIPE, universal_newlines=True)
        if not wait:
            return (p, logfile)
        out, err = p.communicate()
        debug(out[0:40] + '...')
        debug('Length of output is %s' % len(out.split()))
        assert p.returncode == 0, err
        assert len(out) > 0, 'Output should not be empty'
        os.remove(logfile)

    @attr('single_node')
    def help_test(self):
        """
        Test help message of scyllatop tool
        """
        self.cluster.populate(1).start(wait_for_binary_proto=True)
        debug("1 nodes started")

        (cmd, logfile) = self.get_cli()
        cmd = '%s --help' % cmd
        debug(cmd)
        p = subprocess.Popen(cmd.split(), stdout=subprocess.PIPE, universal_newlines=True)
        out, err = p.communicate()
        debug(out[0:40] + '...')
        assert p.returncode == 0, err
        assert len(out) > 0, 'Output should not be empty'
        os.remove(logfile)

    @attr('single_node')
    def list_test(self):
        """
        Test list message of scyllatop tool
        """
        self.cluster.populate(1).start(wait_for_binary_proto=True)
        debug("1 nodes started")

        (cmd, logfile) = self.get_cli()
        cmd = '%s --list' % cmd
        debug(cmd)
        p = subprocess.Popen(cmd.split(), stdout=subprocess.PIPE, universal_newlines=True)
        out, err = p.communicate()
        debug(out[0:40] + '...')
        assert p.returncode == 0, err
        assert len(out) > 0, 'Output should not be empty'
        os.remove(logfile)

    @attr('next-gating')
    @attr('dtest-debug')
    def default_start_test(self):
        """
        Common usage, start scyllatop without options
        """
        self.cluster.populate(3).start(wait_for_binary_proto=True)
        debug("3 nodes started")

        self.interactive_start()

        (p, logfile) = self.interactive_start(wait=False)
        node = self.cluster.nodelist()[0]
        node.stress(['write', 'duration=10s', "no-warmup", '-rate', 'threads=2'])
        debug('Write stress completed')

        p.send_signal(signal.SIGINT)
        out, err = p.communicate()
        debug(out[0:40] + '...')
        debug('Length of output is %s' % len(out.split()))
        assert p.returncode == 0, err
        os.remove(logfile)

    def batch_mode_start_test(self):
        """
        Start scyllatop in batch mode, we can verify the content
        """
        self.cluster.populate(3).start(wait_for_binary_proto=True)
        debug("3 nodes started")

        self.batch_mode_start()

        (p, logfile) = self.batch_mode_start(wait=False, n=20)
        node = self.cluster.nodelist()[0]
        node.stress(['write', 'duration=10s', "no-warmup", '-rate', 'threads=2'])
        debug('Write stress completed')
        out, err = p.communicate()
        debug(out[0:40] + '...')
        debug('Length of output is %s' % len(out.split()))
        assert p.returncode == 0, err
        os.remove(logfile)
