import logging
import os
import subprocess
import pprint
import pytest
import re
import time

from dtest_class import Tester
from tools.marks import enterprise_only_param

logger = logging.getLogger(__file__)
ALLOW_BALANCE_DIFF = 0.2
PP = pprint.PrettyPrinter(indent=2)


@pytest.mark.dtest_full
class TestDataDistribution(Tester):
    def prepare(self, nodes_num=4):
        self.cluster.populate(nodes=nodes_num)
        self.cluster.start(wait_for_binary_proto=True, wait_other_notice=True)
        self.ks = "keyspace1"
        self.cf = "standard1"

    @pytest.mark.parametrize('strategy', [
        'LeveledCompactionStrategy',
        'SizeTieredCompactionStrategy',
        'TimeWindowCompactionStrategy',
        enterprise_only_param('IncrementalCompactionStrategy')
    ])
    def test_data_distribution_balance(self, strategy):
        """Check data distribution between nodes

        Based on issue #6193, verify data distribution
        between nodes by asserting size of dataset with
        nodetool status and filesizes on fs
        """
        self.prepare()

        logger.info("Writing data...")
        stress_cmd = f"write cl=QUORUM n=210000 -schema replication(factor=3) compaction(strategy={strategy}) \
                    -port jmx=6868 -mode cql3 native -rate threads=50 \
                    -col size=fixed(200) n=FIXED(5) -pop seq=1..210000"

        self.cluster.stress(stress_cmd.split(" "))
        self.cluster.flush()
        logger.info("Compacting data...")
        self.cluster.nodetool(f'disableautocompaction {self.ks}')
        self.cluster.compact()
        logger.info("Waiting for compaction...")
        self.cluster.wait_for_compactions()

        # nodetool status load is updated every 60 seconds
        status_ready_at = time.time() + 60 + 1

        for node in self.cluster.nodelist():
            cf_stats = node.nodetool("cfstats keyspace1", capture_output=True, wait=True)
            logger.info(PP.pformat(cf_stats))

        self.verify_datasize_by_check_filesize()

        now = time.time()
        if now < status_ready_at:
            logger.info("sleep for {} seconds until status.load is ready".format(int(status_ready_at - now + 0.5)))
            time.sleep(status_ready_at - now)
        self.verify_datasize_with_nodetool_status()

    def verify_datasize_with_nodetool_status(self):
        node = self.cluster.nodelist()[0]  # type: ScyllaNode
        result = node.nodetool("status", capture_output=True, wait=True)
        logger.info(result)
        status_result = self.parse_nodetool_status(result[0].splitlines())
        logger.info(PP.pformat(status_result))
        avg_size_dataset = self.get_avg_size(status_result)

        size_dimensions = {res["dimension"] for res in status_result}
        assert 1 == len(size_dimensions), "Dimension is different {}".format(size_dimensions)

        min_size = min([res["size"] for res in status_result])
        assert min_size >= avg_size_dataset * (1 - ALLOW_BALANCE_DIFF)
        max_size = max([res["size"] for res in status_result])
        assert max_size <= avg_size_dataset * (1 + ALLOW_BALANCE_DIFF)

    def verify_datasize_by_check_filesize(self):
        fs_sizes = self.parse_fs_size()
        avg_size_dataset = self.get_avg_size(fs_sizes)

        size_dimensions = {res["dimension"] for res in fs_sizes}
        assert 1 == len(size_dimensions), "Dimension is different {}".format(size_dimensions)

        min_size = min([res["size"] for res in fs_sizes])
        assert min_size >= avg_size_dataset * (1 - ALLOW_BALANCE_DIFF)
        max_size = max([res["size"] for res in fs_sizes])
        assert max_size <= avg_size_dataset * (1 + ALLOW_BALANCE_DIFF)

    def parse_nodetool_status(self, lines):
        """parse output of nodetool status

        Nodetool status output:
        Datacenter: eu-west
        ===================
        Status=Up/Down
        |/ State=Normal/Leaving/Joining/Moving
        --  Address      Load       Tokens       Owns    Host ID                               Rack
        UN  10.0.15.114  36.53 GB   256          ?       98429fc3-1e89-4029-ac1c-325179752142  1a
        UN  10.0.126.57  88.17 GB   256          ?       aea7e0f2-c2c3-4dc6-8ffd-8eda27f4ab8e  1a
        UN  10.0.74.155  90.62 GB   256          ?       f2df2267-b8d1-4a1b-a5d8-a6c57a289f44  1a
        UN  10.0.65.254  101.39 GB  256          ?       20eca592-3eda-478b-b9c8-03266879b8ba  1a

        Parsed result:
        [
            {"status": "UN", "address": "10.0.15.114", size: "36.53", dimension: KB|MB|GB},
            {"status": "UN", "address": "10.0.126.57", size: "88.17", dimension: KB|MB|GB},
            {"status": "UN", "address": "10.0.74.155", size: "90.62", dimension: KB|MB|GB},
            {"status": "UN", "address": "10.0.65.254", size: "101.39", dimension: KB|MB|GB}

        ]
        """
        keys = ["status", "address", "size", "dimension"]
        nodes_statuses = []
        line_re = re.compile(
            r"(?P<status>[UND]{2}?)\s+(?P<address>[\d]{1,3}\.[\d]{1,3}\.[\d]{1,3}\.[\d]{1,3}?)\s+(?P<size>[\d]+\.[\d]+?)\s(?P<dimension>[KMGT]B)")
        for line in lines:
            node_status = {}
            res = line_re.search(line)
            if res:
                for key in keys:
                    if key == "size":
                        node_status[key] = float(res[key])
                        continue
                    node_status[key] = res[key]
                nodes_statuses.append(node_status)
        return nodes_statuses

    def parse_fs_size(self):
        """Get size of files for ks/table on fs for each node

        output per node:
        4.0K\t/home/abykov/.dtest/dtest-j8phachp/test/node1/data/keyspace1/standard1-636995d07f1b11eabc68000000000000/staging
        4.0K\t/home/abykov/.dtest/dtest-j8phachp/test/node1/data/keyspace1/standard1-636995d07f1b11eabc68000000000000/0000000000000007.sstable
        4.0K\t/home/abykov/.dtest/dtest-j8phachp/test/node1/data/keyspace1/standard1-636995d07f1b11eabc68000000000000/upload
        4.0K\t/home/abykov/.dtest/dtest-j8phachp/test/node1/data/keyspace1/standard1-636995d07f1b11eabc68000000000000/0000000000000005.sstable
        412M\t/home/abykov/.dtest/dtest-j8phachp/test/node1/data/keyspace1/standard1-636995d07f1b11eabc68000000000000
        412M\t/home/abykov/.dtest/dtest-j8phachp/test/node1/data/keyspace1

        match the line 412M\t/home/abykov/.dtest/dtest-j8phachp/test/node1/data/keyspace1/standard1-636995d07f1b11eabc68000000000000

        result for all nodes:
        return:
        [
            {"address": "127.0.0.1", size: "412", dimension: KB|MB|GB},
            {"address": "127.0.0.2", size: "412", dimension: KB|MB|GB},
        ]


        """
        size_re = re.compile(
            r"^(?P<size>[\d]+?)(?P<dimension>[KMGT]?)\s.*\/{ks}\/{cf}-[0-9a-f]*$".format(ks=self.ks, cf=self.cf))
        node_size = []
        for node in self.cluster.nodelist():  # type: ScyllaNode
            data_dir = os.path.join(node.get_path(), "data", self.ks)
            res = subprocess.run(["du", "-h", data_dir], capture_output=True)
            if res.stdout and res.returncode == 0:
                for line in res.stdout.split(b"\n"):
                    size_res = size_re.search(line.decode(encoding="utf-8"))
                    if size_res:
                        node_size.append({"address": node.address(),
                                          "size": float(size_res["size"]),
                                          "dimension": size_res["dimension"]})
        logger.info(node_size)
        return node_size

    def get_avg_size(self, sizes_data):
        sizes = [float(p['size']) for p in sizes_data]
        return sum(sizes) / len(sizes)
