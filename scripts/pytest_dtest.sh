#!/bin/bash

# Script being used by Jeknins pipeline to run dtest
set -e

PROGRAM=$(basename $0)
DIR=$(dirname $(readlink -f $0))
dry_run=false
repeat_tests=1
source $DIR/sh-utils.sh

function usage {
  echo "Usage: $PROGRAM --mode=<mode> --smp=<smp> [--home=<path>] [--include=<tests>] [--exclude=<tests>] [--random=<n>|all [--random_seed=<seed>]] [--scylla_ext_opts=<flags>] [--scylla_ext_env=<vars>] [--debug] [--repeat=<num>] [--repeat=<num>] [--dry_run]"
  echo ""
  echo "    --home=<path>                  Path that overrides the HOME environment variable"
  echo "    --mode=<mode>                  Build mode (supported values: release, debug, dev)"
  echo "    --smp=<smp>                    Number of processors to use when launching Scylla"
  echo "    --include=<tests>              Comma separated list of tests attributes and/or space separated list of tests to include in the dtest run"
  echo "    --exclude=<tests>              Comma separated list of tests attributes and/or space separated list of tests to exclude in the dtest run"
  echo "    --random=<n>|all               Number of random tests to run. \"all\" for shuffling all tests"
  echo "    --random_seed=<seed>           Optional random seed"
  echo "    --scylla_ext_opts=<flags>      Space separated list of command line options and values. Example: \"--abort-on-seastar-bad-alloc --abort-on-lsa-bad-alloc=1\""
  echo "    --scylla_ext_env=<vars>        Semicolon separated list of env vars and values. Example: \"ASAN_OPTIONS=disable_coredump=0,abort_on_error=1;UBSAN_OPTIONS=halt_on_error=1:abort_on_error=1;BOOST_TEST_CATCH_SYSTEM_ERRORS=no\""
  echo "    --debug                        Enable dtest debugging"
  echo "    --keep_logs                    Keep logs"
  echo "    --repeat=<num>                 How many times to repeat the tests. Default is 1"
  echo "    --dry_run                      Print commands instead of running them"
  echo "    --manager-package=<url>        Url to the scylla-manager relocatable package"
  echo "    --driver-version=<ver>         driver version to install before tests start, ex. scylla-driver==3.25.4"
  echo "    --pytest-ext-opts=<options>    space separated list of cli options and values for pytest.ex: \"--option1=1 --option2=2\""

  exit 1
}


function cleanup_workspace {
  set +e
  # Usually we clean the work space, so these files should not exist.
  # but we support keeping the work space for debug, so we still need these.
  sudo rm -Rf $HOME/.dtest
  sudo rm -Rf $HOME/.ccm
  sudo rm -Rf logs
  sudo rm -Rf logs-$dtest_type.$mode.$NODE_INDEX
  mkdir logs-$dtest_type.$mode.$NODE_INDEX
  rm -Rf $WORKSPACE/scylla-dtest.$dtest_type.$mode.$NODE_INDEX.xml
  set -e
}

function setup_environment_vars {
  # The env script assums we run under scylla-dtest, and that ccm dir is next to it (sister)
  echo "Path before setting: |$PATH|"
  source scylla_dtest_env.sh
  echo "Path after setting: |$PATH|"
  echo "Locale settings:"
  echo "LC_ALL=\"$LC_ALL\""
  echo "LANG=\"$LANG\""
  echo "LANGUAGE=\"$LANGUAGE\""
  export LOG_SAVED_DIR=`pwd`/logs-$dtest_type.$mode.$NODE_INDEX
  echo "Env settings done"
}

function min() {
    local ret="$1"; shift
    while (( $# )); do
	curr="$1"
	ret=$((curr < ret ? curr : ret)); shift
    done
    echo "$ret"
}

function xdist_processes() {
    local smp="$1"
    local nodes_per_cluster="$2"
    local mb_per_cpu="$3"

    local nproc="$(nproc)"
    local cpu_per_dtest=$(($smp * $nodes_per_cluster))
    local cpu_limit=$((nproc / cpu_per_dtest))

    local total_mem=$(($(getconf _PHYS_PAGES) * $(getconf PAGESIZE)))
    local mem_per_dtest="$((cpu_per_dtest * mb_per_cpu * 1024 * 1024))"
    local mem_limit=$((total_mem / mem_per_dtest))

    local mode_limit=10
    if [[ "$mode" == "debug" ]]; then
        mode_limit=3
    fi
    local limit=$(min $mode_limit $cpu_limit $mem_limit)
    limit=$((limit < 1 ? 1 : limit))
    echo "$limit"
}

function copy_orphaned_logs() {
	local src
	local src_node
	for src in $HOME/.dtest/dtest-*; do
		local t=""
		if [ -f "$src/test/current_test" ]; then
			local name="$src/test/current_test"
			ts=$({ echo "import os"; echo "print(int(os.path.getctime('$name')))"; } | python)
			t=${ts}_$(cat "$name")
		fi
		if [ -z "$t" ]; then
			t=$(basename "$src")
		fi
		run_cmd mkdir -p "$LOG_SAVED_DIR/orphaned/$t"
		echo Copying logs from orphaned dtest $src to $LOG_SAVED_DIR/orphaned/$t
		for src_node in "$src"/test/node*; do
			local n=$(basename "$src_node")
			run_cmd cp "$src_node/logs/system.log" "$LOG_SAVED_DIR/orphaned/$t/$n.log"
		done
		run_cmd rm -rf "$src"
	done
}

for i in "$@"
do
case $i in
    --home*)
    home_dir="${i#*=}"
    shift
    ;;
    --mode*)
    mode="${i#*=}"
    shift
    ;;
    --smp*)
    smp="${i#*=}"
    shift
    ;;
    --include*)
    tests="${i#*=}"
    shift
    ;;
    --exclude*)
    excluded_tests="${i#*=}"
    shift
    ;;
    --random=*)
    random="${i#*=}"
    shift
    ;;
    --random_seed*)
    random_seed="${i#*=}"
    shift
    ;;
    --debug*)
    set_dtest_debug=true
    shift
    ;;
    --scylla_ext_opts*)
    scylla_ext_opts_param="${i#*=}"
    shift
    ;;
    --scylla_ext_env*)
    scylla_ext_env_param="${i#*=}"
    shift
    ;;
    --repeat*)
    repeat_tests="${i#*=}"
    shift
    ;;
    --dry_run)
    dry_run=true
    shift
    ;;
    --dtest-type*)
    dtest_type="${i#*=}"
    shift
    ;;
    --manager-package*)
    manager_package="${i#*=}"
    shift
    ;;
    --driver-version*)
    driver_version="${i#*=}"
    shift
    ;;
    --pytest-ext-opts*)
    pytest_ext_opts="${i#*=}"
    shift
    ;;
    *)
    echo "Error: unknown command line option: |$i|"
    usage
    exit 1
    ;;
esac
done

fail_if_param_missing "$mode" '--mode'
fail_if_param_missing "$smp" '--smp'

echo "$PROGRAM got these Parameters:"
echo "   --home                  = \"$home_dir\""
echo "   --mode                  = \"$mode\""
echo "   --smp                   = \"$smp\""
echo "   --dry_run               = \"$dry_run\""
echo "   --debug                 = \"$set_dtest_debug\""
echo "   --exclude               = \"$excluded_tests\""
echo "   --include               = \"$tests\""
echo "   --random                = \"$random\""
echo "   --random_seed           = \"$random_seed\""
echo "   --keep_logs             = \"$keep_logs\""
echo "   --scylla_ext_opts       = \"$scylla_ext_opts_param\""
echo "   --scylla_ext_env        = \"$scylla_ext_env_param\""
echo "   --dtest_type            = \"$dtest_type\""
echo "   --manager-package       = \"$manager_package\""
echo "   --driver-version        = \"$driver_version\""
echo "   --pytest-ext-opts       = \"$pytest_ext_opts\""
echo "=================="

# Script is called on with workspace as the current directory, which contains the scylla, scylla-ccm, scylla-dtest, and other directories.
if [ -h "scylla/resources/cassandra" ]; then
	rm scylla/resources/cassandra
fi
mkdir -p scylla/resources
scylla_tools_java_dir="`pwd`/scylla/tools/java"
ln -s $scylla_tools_java_dir scylla/resources/cassandra

cd scylla-dtest

cleanup_workspace

setup_environment_vars

if [ -z $SCYLLA_VERSION ] ; then
  echo "SCYLLA_VERSION is not defined. Setting ccm create only for cas-tmp"
  export INSTALL_CASSANDRA='ccm create cas-tmp --vnodes -n 1 --version=3.11.3'
else
  echo "SCYLLA_VERSION is defined: |${SCYLLA_VERSION}| Running ccm create scylla-tmp and cas-tmp"
  export INSTALL_CASSANDRA='ccm create cas-tmp --vnodes -n 1 --version=3.11.3; ccm create scylla-tmp --scylla -n 1 --version=${SCYLLA_VERSION}'
fi

if [ ! -z $manager_package ]; then
  export INSTALL_CASSANDRA="$INSTALL_CASSANDRA --scylla-manager-package=$manager_package"
fi

if [ ! -z $driver_version ]; then
  export INSTALL_CASSANDRA="$INSTALL_CASSANDRA ; pip3 install $driver_version"
fi

RUN_TEST_CMD="./scripts/run_test.sh"
export TOOLS_JAVA_DIR="$scylla_tools_java_dir"

if [ "x$tests" = "xgating" ]; then
	echo "Going to run next-gating dtest"
	export tests="-a next-gating"
	export all_modules_flag=true
elif [[ "$tests" == -a* ]]; then
	export all_modules_flag=true
elif [[ "$tests" == @* ]]; then
	export tests=$(cat ${tests:1})
else
	export all_modules_flag=false
	if [ "x$tests" == "x" ]; then
		echo "No specific tests were given."
		if [ "$mode" == "debug" -a -z "$random" ]; then
			export tests="-a dtest-debug"
		else
			export tests="`cat scylla_tests`"
		fi
	fi
	echo "Dtest tests before exclude:"
	echo $tests
	echo "====== End of Tests list before exclude ============="

	if [ "x$excluded_tests" != "x" ]; then
		echo "Excluding tests"
		export tests="$tests --exclude `echo $excluded_tests | sed s/' '/'\|'/g`"
	fi
fi
PYTEST_FLAGS="-v --junit-xml=$WORKSPACE/scylla-dtest.$dtest_type.$mode.$NODE_INDEX.xml  --delete-logs=passed --log-file=${LOG_SAVED_DIR}/dtest.log"

if [[ -n "$random" ]]; then
    echo "Selecting $random random tests"
    if [[ ! -z "$random_seed" ]]; then
        PYTEST_FLAGS="${PYTEST_FLAGS} --randomly-seed=${random_seed}"
    fi
else
    PYTEST_FLAGS="${PYTEST_FLAGS} -p no:randomly"
fi

if [ $repeat_tests -gt 1 ]; then
    PYTEST_FLAGS="${PYTEST_FLAGS} --count ${repeat_tests}"
fi


echo "Going to run dtest tests:"
echo $tests
echo "====== End of Tests list ============="

if [ "x$set_dtest_debug" = "x" ]; then
	set_dtest_debug=false
fi


if [ "x$keep_logs" = "x" ]; then
	keep_logs=false
fi

echo "Setting env for dtest"
export HOME=$home_dir

export mb_per_cpu=512
export nodes_per_cluster=3

export XDIST_PROCESSES=$(xdist_processes "$smp" "$nodes_per_cluster" "$mb_per_cpu")
PYTEST_FLAGS="${PYTEST_FLAGS} -n ${XDIST_PROCESSES}"

if [ -z "$SCYLLA_VERSION" ]; then
    PYTEST_FLAGS="$PYTEST_FLAGS --cassandra-dir=${CASSANDRA_DIR}"
else
    PYTEST_FLAGS="$PYTEST_FLAGS --scylla-version=${SCYLLA_VERSION}"
fi

export SCYLLA_EXT_OPTS="--smp $smp --memory $(($smp * $mb_per_cpu))M $scylla_ext_opts_param"
if [[ "$SCYLLA_EXT_OPTS" != *--abort-on-internal-error* ]]; then
    export SCYLLA_EXT_OPTS="$SCYLLA_EXT_OPTS --abort-on-internal-error 1"
fi

export SCYLLA_EXT_ENV="$scylla_ext_env_param"
if [ "$mode" == "debug" -a -z "$SCYLLA_EXT_ENV" ]; then
    export SCYLLA_EXT_ENV="ASAN_OPTIONS=disable_coredump=0:abort_on_error=1;UBSAN_OPTIONS=halt_on_error=1:abort_on_error=1;BOOST_TEST_CATCH_SYSTEM_ERRORS=no"
fi

# Following 2 env vars are for debug. https://github.com/scylladb/scylla/wiki/Scylla-DTEST#additional-good-to-know
if $set_dtest_debug ; then
	PYTEST_FLAGS="$PYTEST_FLAGS --log-cli-level=debug"
fi

export KEEP_CORES=true

if $dry_run ; then
    PYTEST_FLAGS="$PYTEST_FLAGS --collect-only"
fi

if [ ! -z $pytest_ext_opts ]; then
    PYTEST_FLAGS="$PYTEST_FLAGS $pytest_ext_opts"
fi

if [ ! -z $manager_package ] ; then
  export PYTEST_FLAGS="$PYTEST_FLAGS --scylla-manager-package=$manager_package"
fi

echo "PYTEST_FLAGS=\"$PYTEST_FLAGS\""
echo "XDIST_PROCESSES=\"$XDIST_PROCESSES\""
echo "SCYLLA_EXT_OPTS=\"$SCYLLA_EXT_OPTS\""
echo "SCYLLA_EXT_ENV=\"$SCYLLA_EXT_ENV\""
echo "============= Environment ============="
env
echo "========= End of Environment ============="
echo "Dont fail because pytest returned an error, collect the logs in any case"
set +e

core_pattern="%e.%p.%t.core"
last_core_pattern=$(sudo /sbin/sysctl -n kernel.core_pattern)
if [ "$last_core_pattern" != "$core_pattern" ]; then
	echo "Setting kernel.core_pattern"
	sudo /sbin/sysctl kernel.core_pattern="$core_pattern"
fi

# Running this without the run_cmd function, as on dry-run it uses the DRY_RUN env var.
echo "Running dtest: |$RUN_TEST_CMD $tests $PYTEST_FLAGS|"
$RUN_TEST_CMD $tests $PYTEST_FLAGS
exitStatus=$?
echo "pytest ended with status |$exitStatus|"
if $dry_run ; then
	echo "Setting status to 0 if this is dry_run. Workaround till https://github.com/scylladb/scylla-dtest/issues/1137 is resolved"
	exitStatus=0
fi

if [ "$last_core_pattern" != "$core_pattern" ]; then
	echo "Restoring kernel.core_pattern"
	sudo /sbin/sysctl kernel.core_pattern="$last_core_pattern"
fi

# Fix results file - hack for now https://issues.jenkins-ci.org/browse/JENKINS-51914
run_cmd sed -i s/skip=/skipped=/g ../scylla-dtest.$dtest_type.$mode.$NODE_INDEX.xml

# When all dtest pass, no logs are available. Don't fail build in this nice case.

# Copy any orphaned logs - workaround for https://github.com/scylladb/scylla-pkg/issues/198
if [ $(find $HOME/.dtest -maxdepth 1 -name 'dtest-*' -type d | wc -l) -ne 0 ]; then
	copy_orphaned_logs
fi

ls -la logs-$dtest_type.$mode.$NODE_INDEX

exit $exitStatus
