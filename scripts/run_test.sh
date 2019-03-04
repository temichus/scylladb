#!/usr/bin/env bash

set -e

help_text="
Script to run dtest from within docker

    Optional values can be set via environment variables
    SCYLLA_DIR, TOOLS_JAVA_DIR, JMX_DIR, DTEST_DIR, CCM_DIR, SCYLLA_DBUILD_SO_DIR, SCYLLA_EXT_OPTS, NOSE_PROCESSES

    cd ~/scylla-dtest/

    # run specific tests
    ./scripts/run_test.sh read_amplification_test.py:ReadAmplificationTest.no_amplification_on_read_20kb_test

    # run all tests
    ./scripts/run_test.sh
"

# once we upload to dockerhub we won't need to build it ontop each worker
if result=$(docker pull docker.io/scylladb/scylla-dtest) ; then
     echo "using image downloaded from docker.io"
else
    if result=$(docker build . -t docker.io/scylladb/scylla-dtest); then
        echo "Docker image is already successfully built..."
    else
        rc=$?
        echo ${result}
        exit ${rc}
    fi
fi

export SCYLLA_DIR=${SCYLLA_DIR:-`pwd`/../scylla}
export CASSANDRA_DIR=${CASSANDRA_DIR:-${SCYLLA_DIR}}
export TOOLS_JAVA_DIR=${TOOLS_JAVA_DIR:-`pwd`/../scylla-tools-java}
export JMX_DIR=${JMX_DIR:-`pwd`/../scylla-jmx}
export DTEST_DIR=${DTEST_DIR:-`pwd`}
export CCM_DIR=${CCM_DIR:-`pwd`/../scylla-ccm}
export SCYLLA_DBUILD_SO_DIR=${SCYLLA_DBUILD_SO_DIR:-${SCYLLA_DIR}/dynamic_libs}
export SCYLLA_EXT_OPTS=${SCYLLA_EXT_OPTS:-"--smp 1 --memory 512M"}

export DEBUG=${DEBUG:-true}
export KEEP_TEST_DIR=${KEEP_TEST_DIR:-true}
export PRINT_DEBUG=${PRINT_DEBUG:-true}

if [[ ! -d ${SCYLLA_DIR} ]]; then
    echo -e "\e[31m\$SCYLLA_DIR = $SCYLLA_DIR doesn't exist\e[0m"
    echo "${help_text}"
    exit 1
fi

if [[ ! -d ${TOOLS_JAVA_DIR} ]]; then
    echo -e "\e[31m\$TOOLS_JAVA_DIR = $TOOLS_JAVA_DIR doesn't exist\e[0m"
    echo "${help_text}"
    exit 1
fi
if [[ ! -d ${JMX_DIR} ]]; then
    echo -e "\e[31m\$JMX_DIR = $JMX_DIR doesn't exist\e[0m"
    echo "${help_text}"
    exit 1
fi
if [[ ! -d ${DTEST_DIR} ]]; then
    echo -e "\e[31m\$DTEST_DIR = $DTEST_DIR doesn't exist\e[0m"
    echo "${help_text}"
    exit 1
fi
if [[ ! -d ${CCM_DIR} ]]; then
    echo -e "\e[31m\$CCM_DIR = $CCM_DIR doesn't exist\e[0m"
    echo "${help_text}"
    exit 1
fi

if [[ ! -d ${SCYLLA_DBUILD_SO_DIR} ]]; then
    echo "scylla was built with dbuild, and SCYLLA_DBUILD_SO_DIR wasn't supplied or exists"
    cd ${SCYLLA_DIR}
    ./dbuild
    ./tools/toolchain/dbuild -it -v ${DTEST_DIR}/scripts/dbuild_collect_so.sh:/bin/dbuild_collect_so.sh -- dbuild_collect_so.sh build/release/scylla dynamic_libs/
    cd -
fi

docker run --rm=true \
    -v ${DTEST_DIR}:${DTEST_DIR} \
    -v ${SCYLLA_DIR}:${SCYLLA_DIR} \
    -v ${TOOLS_JAVA_DIR}:${TOOLS_JAVA_DIR} \
    -v ${JMX_DIR}:${JMX_DIR} \
    -v ${CCM_DIR}:${CCM_DIR} \
    -e CASSANDRA_DIR \
    -e SCYLLA_DBUILD_SO_DIR \
    -e KEEP_TEST_DIR \
    -e DEBUG \
    -e SCYLLA_EXT_OPTS \
    -e NOSE_PROCESSES \
    -w ${DTEST_DIR} \
    -v /etc/passwd:/etc/passwd:ro \
    -v /etc/group:/etc/group:ro \
    -u $(id -u ${USER}):$(id -g ${USER}) \
    --tmpfs ${HOME}/.cache \
    --tmpfs ${HOME}/.local \
    -v ${HOME}/.dtest:${HOME}/.dtest \
    -v ${HOME}/.ccm:${HOME}/.ccm \
    docker.io/scylladb/scylla-dtest:latest bash -c "pip install --user -e  ${CCM_DIR} ; nosetests -v -s $*"
