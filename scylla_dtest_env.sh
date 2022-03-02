# Configure locale:
export LC_ALL=en_US.UTF-8
export LANG=en_US.UTF-8
export LANGUAGE=en_US.UTF-8

CCM_DIR=`pwd`/../scylla-ccm
export PYTHONPATH=$CCM_DIR
echo
echo "Examples:"
echo "=== To run a simple boot and shutdown test:"
echo 'pytest simple_boot_shutdown_test.py --cassandra-dir=`pwd`/../scylla/build/debug '
echo
echo "=== To run a test and keep the cluster data"
echo 'pytest simple_boot_shutdown_test.py --cassandra-dir=`pwd`/../scylla/build/debug --keep-test-dir'
echo
echo "=== To remove dtest produced clsuter data"
echo "ls -l ~/.dtest; rm -rf ~/.dtest"
