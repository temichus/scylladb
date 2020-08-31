## Setup dtest for manager testing
# download manager version
mkdir /home/`whoami`/manager
cd /home/`whoami`/manager
curl -o scylla-manager-client.rpm -L http://downloads.scylladb.com/manager/rpm/unstable/centos/DESIRED_BRANCH/DESIRED_BUILD/scylla-manager/7/x86_64/scylla-manager-client-....x86_64.rpm
rpm2cpio scylla-manager-client.rpm | cpio -idm
cp ./usr/bin/sctool .

sudo curl -o scylla-manager-server.rpm -L http://downloads.scylladb.com/manager/rpm/unstable/centos/DESIRED_BRANCH/DESIRED_BUILD/scylla-manager/7/x86_64/scylla-manager-server-....x86_64.rpm
rpm2cpio scylla-manager-server.rpm | cpio -idmv
cp ./usr/bin/scylla-manager .

# run the test
export KEEP_TEST_DIR=true
export DEBUG=true
export PRINT_DEBUG=true
export SCYLLA_VERSION=unstable/master:239
(or any other scylla build)
export SCYLLA_EXT_OPTS=--scylla-manager\=/home/`whoami`/manager

See that manager sanity test runs OK by executing:
nosetests -s -v scylla_mgmt_repair_test.py:ScyllaMgmtRepairTest.test_manager_sanity
