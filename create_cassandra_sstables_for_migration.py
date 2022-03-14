"""
The utility is intended to create different format Cassandra sstables (depend on the version) for migration test.

Utility uses the test data file with definition of tests and statements, that have to be performed.
It's test_data/create_sstables_for_migration/cassandra_sstables_tests_for_migration_v3.0.yaml (I checked it on the
3.11.3 version and didn't check on other versions). This is default folder.

Created sstables will be saved under folder, that passed in the -s (--sstables-folder) parameter. The folder should be
located under cassandra-sstables/migration/ and new folder name should be according to the version/type of sstables

Usage: python create_cassandra_sstables_for_migration.py -v <cassandra version, like: 3.11.3>
                                                         -s <path to cassandra sstables folder for dtest>
                                                         -d <path to the test data file>
Example:
python ./create_cassandra_sstables_for_migration.py -v 2.1.20 -s ./cassandra-sstables/migration/2_1_x -d test_data/create_sstables_for_migration/cassandra_sstables_tests_for_migration_v3.0.yaml
python ./create_cassandra_sstables_for_migration.py -v 2.2.13 -s ./cassandra-sstables/migration/2_2_x -d test_data/create_sstables_for_migration/cassandra_sstables_tests_for_migration_v3.0.yaml
python ./create_cassandra_sstables_for_migration.py -v 3.11.3 -s ./cassandra-sstables/migration/3_0_mc -d test_data/create_sstables_for_migration/cassandra_sstables_tests_for_migration_v3.0.yaml
CQLSH_NO_BUNDLED=TRUE python ./create_cassandra_sstables_for_migration.py -v 3.0.6 -s ./cassandra-sstables/migration/3_0_x -d test_data/create_sstables_for_migration/cassandra_sstables_tests_for_migration_v3.0.yaml


Output is Cassandra sstables, that will be saved under folder, that passed in the second parameter.
"""
import argparse
import os
import shutil
from collections import namedtuple
import time
import yaml

from tools.cassandra_helpers import CassandraCluster
from tools.files import copy_files_to, get_cf_dir


def create_folder_if_not_exists(folder_name):
    if not os.path.exists(folder_name):
        os.makedirs(folder_name)


def parse_args():
    parser = argparse.ArgumentParser(description='Create Cassandra sstables for migration test')
    parser.add_argument('-v', '--cassandra-version', type=str, dest='cassandra_version',
                        required=True, help='Cassandra version')
    parser.add_argument('-s', '--sstables-folder', type=str, dest='cassandra_sstables_folder', required=True,
                        help='Path to folder where the created Cassandra sstables will be saved')
    parser.add_argument('-d', '--tests-data-file-path', type=str, dest='tests_def',
                        default='test_data/create_sstables_for_migration/cassandra_sstables_tests_for_migration_v3.0.yaml',
                        help='Path to test data file with definition of tests and statements, that have to be performed')
    args = vars(parser.parse_args())

    return args


def main(args):
    cassandra_version = args['cassandra_version']
    cassandra_sstables_folder = args['cassandra_sstables_folder']
    tests_def = args['tests_def']

    create_folder_if_not_exists(cassandra_sstables_folder)

    cc = None
    try:
        request = namedtuple("request", 'node config')
        request.node = namedtuple('node', 'nodeid')
        request.config = namedtuple('config', 'getoption')
        request.config.getoption = lambda x: None
        request.node.nodeid = 'create_cassandra_sstables_for_migration'
        cc = CassandraCluster(cassandra_version=cassandra_version, request=request)
        node1 = cc.create_and_start_cluster(nodes=1)

        data_folder = os.path.join(node1.get_node_cassandra_root(), 'data')
        print('Test data folder: {}'.format(data_folder))

        with open(tests_def, 'r') as stream:
            tests = yaml.safe_load(stream)

        for test, desc in tests.items():
            print(test)
            test_data_folder = os.path.join(data_folder, desc['keyspace_name'])
            create_folder_if_not_exists(test_data_folder)

            test_folder = os.path.join(cassandra_sstables_folder, test)
            create_folder_if_not_exists(test_folder)
            for step in desc['cmds']:
                tool = step[0]
                cmd = step[1].strip() if len(step) == 2 else None
                print(cmd)
                if tool == 'cqlsh':
                    stdout, stderr = node1.run_cqlsh(cmds=cmd, return_output=True)
                    if stderr:
                        raise Exception(stderr)
                elif tool == 'nodetool':
                    node1.nodetool(cmd)
                elif tool == 'subfolder':
                    sstables_folder = test_folder
                    if cmd:
                        sstables_folder = os.path.join(sstables_folder, cmd)
                        create_folder_if_not_exists(sstables_folder)
                    copy_files_to(from_dir=get_cf_dir(ks_dir=test_data_folder, cf_name=desc['table_name']),
                                  to_dir=sstables_folder,
                                  files_only=True)
                elif tool == 'delete folder':
                    shutil.rmtree(test_data_folder)
                elif tool == 'wait':
                    time.sleep(float(cmd))

    except Exception as e:
        print('Error: {}'.format(e))
    finally:
        if cc:
            cc.tear_down()


if __name__ == "__main__":
    args = parse_args()
    main(args)
