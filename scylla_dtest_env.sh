# Configure locale:
export LC_ALL=en_US.UTF-8
export LANG=en_US.UTF-8
export LANGUAGE=en_US.UTF-8

CCM_DIR=`pwd`/../scylla-ccm
export PATH=$CCM_DIR:$PATH
export PERL5LIB=$CCM_DIR
export PYTHONPATH=$CCM_DIR
export CASSANDRA_DIR=`pwd`/../scylla
echo "Scylla dir set to ${CASSANDRA_DIR}"
echo "Using release build. To use debug (or dev) build set with:"
echo "export CASSANDRA_DIR=\"${CASSANDRA_DIR}/build/debug\""
echo
echo "Examples:"
echo "=== To run a simple boot and shutdown test:"
echo 'nosetests -v -s simple_boot_shutdown.py'
echo
echo "=== To run a test and keep the cluster data"
echo 'KEEP_TEST_DIR=true REUSE_CLUSTER=false nosetests -v -s simple_boot_shutdown.py'
echo
echo "=== To remove dtest produced clsuter data"
echo "ls -l ~/.dtest; rm -rf ~/.dtest"
