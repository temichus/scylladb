import subprocess
import tempfile

from ccmlib.urchin_cluster import UrchinCluster

from dtest import Tester, debug


class TestSimple(Tester):

    __test__ = False
    __urchin_args__ = []

    def __init__(self, *args, **kwargs):
        Tester.__init__(self, *args, **kwargs)

    def prepare(self):
        """
        Sets up cluster to test against. Currently 3 CCM Nodes
        """
        cluster = self.cluster
        return cluster

    def stress_write(self, node):
        """
        Writes data via stress. Should write exact data expected by stress_read()
        """
        node.stress(['write', 'n=100000', '-mode', 'cql3', 'simplenative', '-rate', 'threads=1', '-pop', 'seq=1..100000'])

    def stress_read(self, node):
        """
        Reads previously written data via stress. Should check for exact data written
        by stress_write().
        """
        # Verify the data
        tmpfile = tempfile.mktemp()
        with open(tmpfile, 'w+') as tmp:
            node.stress(['read', 'n=100000', '-mode', 'cql3', 'simplenative', '-rate', 'threads=1', '-pop', 'seq=1..100000'],
                        stdout=tmp, stderr=subprocess.STDOUT)
        return tmpfile

    def validate_stress_output(self, outfile, expect_failure=False, expect_errors=False):
        """
        Validates if data was lost, or determines if stress encountered errors.
        Should be updated once stress has more sophisticated validation.

        outfile - an output file with stress output.
        expect_failure - if data loss should be expected
        expect_errors - if exceptions should be expected
        """
        with open(outfile, 'r') as tmp:
            output = tmp.read()

            debug(output)
            failure = output.find("Data returned was not validated")
            if expect_failure:
                assert failure >= 0, "No missing data detected, despite data loss"
            else:
                self.assertEqual(failure, -1, "Stress failed to validate all data")

            failure = output.find("Exception")
            if expect_errors:
                assert failure >= 0, "No errors detected, despite invalid cluster state"
            else:
                self.assertEqual(failure, -1, "Error while reading data")

    def simple_single_node_write_read_test(self):
        """
        A basic test that writes data at CL=ONE, RF=1
        Tests to ensure no data is lost or errors thrown.
        """
        cluster = self.prepare()
        jvm_args = []
        if type(cluster) is UrchinCluster:
            jvm_args = self.__urchin_args__
        cluster.populate(1).start(jvm_args=jvm_args)
        node1 = cluster.nodelist()[0]
        self.stress_write(node1)
        out = self.stress_read(node1)
        self.validate_stress_output(out)


options = {'Single': ['--smp', '1'], 'SMP': ['--smp', '2']}

for option in options.keys():
    cls_name = ('SimpleNoDriverTest_with_' + option)
    vars()[cls_name] = type(cls_name, (TestSimple,), {'__urchin_args__': options[option], '__test__': True})
