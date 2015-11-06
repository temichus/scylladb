CCM_DIR=../scylla-ccm;
export PATH=$CCM_DIR:$PATH
export PERL5LIB=$CCM_DIR
export PYTHONPATH=$CCM_DIR
export CASSANDRA_DIR=`pwd`/../scylla-tools-java
export CASSANDRA_DIR=`pwd`/../scylla
echo "Examples:"
echo "=== To run a simple boot and shutdown test:"
echo 'nosetests -v -s simple_boot_shutdown.py'
echo
echo "=== To run a test and keep the cluster data"
echo 'KEEP_TEST_DIR=true REUSE_CLUSTER=false nosetests -v -s simple_boot_shutdown.py'
echo
echo "=== To remove dtest produced clsuter data"
echo "ls -l ~/.dtest; rm -rf ~/.dtest"

