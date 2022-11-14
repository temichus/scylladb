"""
All dtest functional test for scyllatop utils.
"""

import subprocess
import time
import signal
import os
import tempfile
import logging
import pytest
from pathlib import Path

from dtest_class import Tester

logger = logging.getLogger(__name__)


@pytest.mark.dtest_full
class TestScyllaTop(Tester):

    def get_cli(self):
        node = self.cluster.nodelist()[0]
        candidates_clis = [
            'tools/scyllatop/scyllatop.py',
            'scylla/bin/scyllatop',
            'scylla/opt/scylladb/scyllatop/scyllatop.py',
            'scyllatop/scyllatop.py',
        ]
        for candidate_cli in candidates_clis:
            cli = Path(node.get_install_dir()) / candidate_cli
            if cli.exists():
                break
        else:
            raise EnvironmentError("Didn't found scyllatop cli ")

        t = tempfile.mkstemp(prefix='scyllatop.log.')
        os.close(t[0])
        logfile = t[1]
        cli = f'{cli} -L {logfile} -p http://{node.address()}:9180/metrics -v DEBUG'
        return cli, logfile

    def interactive_start(self, wait=True, sleep_time=10):
        """
        Common usage, start scyllatop without options
        """
        (cmd, logfile) = self.get_cli()
        logger.debug(cmd)
        p = subprocess.Popen(cmd.split(), stdout=subprocess.PIPE, universal_newlines=True)
        if not wait:
            return (p, logfile)
        time.sleep(sleep_time)
        p.send_signal(signal.SIGINT)
        out, err = p.communicate()
        logger.debug(out[0:40] + '...')
        logger.debug('Length of output is %s' % len(out.split()))
        assert p.returncode == 0, err
        assert len(out) > 0, 'Output should not be empty'
        os.remove(logfile)

    def batch_mode_start(self, wait=True, n=1):
        """
        Start scyllatop in batch mode
        """
        (cmd, logfile) = self.get_cli()
        cmd = "%s -b -n %s" % (cmd, n)
        logger.debug(cmd)
        p = subprocess.Popen(cmd.split(), stdout=subprocess.PIPE, universal_newlines=True)
        if not wait:
            return (p, logfile)
        out, err = p.communicate()
        logger.debug(out[0:40] + '...')
        logger.debug('Length of output is %s' % len(out.split()))
        assert p.returncode == 0, err
        assert len(out) > 0, 'Output should not be empty'
        os.remove(logfile)

    @pytest.mark.single_node
    def test_help(self):
        """
        Test help message of scyllatop tool
        """
        self.cluster.populate(1).start(wait_for_binary_proto=True)
        logger.debug("1 nodes started")

        (cmd, logfile) = self.get_cli()
        cmd = '%s --help' % cmd
        logger.debug(cmd)
        p = subprocess.Popen(cmd.split(), stdout=subprocess.PIPE, universal_newlines=True)
        out, err = p.communicate()
        logger.debug(out[0:40] + '...')
        assert p.returncode == 0, err
        assert len(out) > 0, 'Output should not be empty'
        os.remove(logfile)

    @pytest.mark.single_node
    def test_list(self):
        """
        Test list message of scyllatop tool
        """
        self.cluster.populate(1).start(wait_for_binary_proto=True)
        logger.debug("1 nodes started")

        (cmd, logfile) = self.get_cli()
        cmd = '%s --list' % cmd
        logger.debug(cmd)
        p = subprocess.Popen(cmd.split(), stdout=subprocess.PIPE, universal_newlines=True)
        out, err = p.communicate()
        logger.debug(out[0:40] + '...')
        assert p.returncode == 0, err
        assert len(out) > 0, 'Output should not be empty'
        os.remove(logfile)

    @pytest.mark.next_gating
    @pytest.mark.dtest_debug
    def test_default_start(self):
        """
        Common usage, start scyllatop without options
        """
        self.cluster.populate(3).start(wait_for_binary_proto=True)
        logger.debug("3 nodes started")

        self.interactive_start()

        (p, logfile) = self.interactive_start(wait=False)
        node = self.cluster.nodelist()[0]
        node.stress(['write', 'duration=10s', "no-warmup", '-rate', 'threads=2'])
        logger.debug('Write stress completed')

        p.send_signal(signal.SIGINT)
        out, err = p.communicate()
        logger.debug(out[0:40] + '...')
        logger.debug('Length of output is %s' % len(out.split()))
        assert p.returncode == 0, err
        os.remove(logfile)

    def test_batch_mode_start(self):
        """
        Start scyllatop in batch mode, we can verify the content
        """
        self.cluster.populate(3).start(wait_for_binary_proto=True)
        logger.debug("3 nodes started")

        self.batch_mode_start()

        (p, logfile) = self.batch_mode_start(wait=False, n=20)
        node = self.cluster.nodelist()[0]
        node.stress(['write', 'duration=10s', "no-warmup", '-rate', 'threads=2'])
        logger.debug('Write stress completed')
        out, err = p.communicate()
        logger.debug(out[0:40] + '...')
        logger.debug('Length of output is %s' % len(out.split()))
        assert p.returncode == 0, err
        os.remove(logfile)
