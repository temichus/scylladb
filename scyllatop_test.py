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
        t = tempfile.mkstemp(prefix='scyllatop.log.')
        os.close(t[0])
        cli += ' -L {} -p http://{}:9180/metrics'.format(t[1], node.address())
        return cli

    def interactive_start(self, wait=True, sleep_time=60):
        """
        Common usage, start scyllatop without options
        """
        cmd = self.get_cli()
        debug(cmd)
        p = subprocess.Popen(cmd.split(), stdout=subprocess.PIPE)
        if not wait:
            return p
        time.sleep(sleep_time)
        p.send_signal(signal.SIGINT)
        out, err = p.communicate()
        debug(out)
        debug('Length of output is %s' % len(out.split()))
        assert p.returncode == 0, err
        assert len(out) > 0, 'Output should not be empty'

    def batch_mode_start(self, wait=True, n=1):
        """
        Start scyllatop in batch mode
        """
        cmd = "%s -v DEBUG -b -n %s" % (self.get_cli(), n)
        debug(cmd)
        p = subprocess.Popen(cmd.split(), stdout=subprocess.PIPE)
        if not wait:
            return p
        out, err = p.communicate()
        debug(out)
        debug('Length of output is %s' % len(out.split()))
        assert p.returncode == 0, err
        assert len(out) > 0, 'Output should not be empty'

    def help_test(self):
        """
        Test help message of scyllatop tool
        """
        self.cluster.populate(3).start(wait_for_binary_proto=True)
        debug("3 nodes started")

        cmd = '%s --help' % self.get_cli()
        debug(cmd)
        p = subprocess.Popen(cmd.split(), stdout=subprocess.PIPE)
        out, err = p.communicate()
        debug(out)
        assert p.returncode == 0, err
        assert len(out) > 0, 'Output should not be empty'

    @attr('next-gating')
    @attr('dtest-debug')
    def default_start_test(self):
        """
        Common usage, start scyllatop without options
        """
        self.cluster.populate(3).start(wait_for_binary_proto=True)
        debug("3 nodes started")

        self.interactive_start()

        p = self.interactive_start(wait=False)
        node = self.cluster.nodelist()[0]
        node.stress(['write', 'duration=60s', "no-warmup", '-rate', 'threads=2'])
        debug('Write stress completed')

        p.send_signal(signal.SIGINT)
        out, err = p.communicate()
        debug(out)
        debug('Length of output is %s' % len(out.split()))
        assert p.returncode == 0, err

    def batch_mode_start_test(self):
        """
        Start scyllatop in batch mode, we can verify the content
        """
        self.cluster.populate(3).start(wait_for_binary_proto=True)
        debug("3 nodes started")

        self.batch_mode_start()

        p = self.batch_mode_start(wait=False, n=20)
        node = self.cluster.nodelist()[0]
        node.stress(['write', 'duration=60s', "no-warmup", '-rate', 'threads=2'])
        debug('Write stress completed')
        out, err = p.communicate()
        debug(out)
        debug('Length of output is %s' % len(out.split()))
        assert p.returncode == 0, err
