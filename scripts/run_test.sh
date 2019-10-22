#!/usr/bin/env bash

set -e

help_text="
Script to run dtest from within docker

    Optional values can be set via environment variables

    Running dtest from scylla source code :

        CASSANDRA_DIR
            directory of the scylla source code or specific build output variant
            '../scylla/build/release' or '../scylla/build/debug' default to '../scylla'
        SCYLLA_DBUILD_SO_DIR
            directory of dynamic .so files to be collected. defaults to '\$CASSANDRA_DIR/dynamic_libs'
        TOOLS_JAVA_DIR
            directory of scylla java tools, should be already compiled. defaults to '../scylla-tools-java'
        JMX_DIR
            directory of scylla jmx, should be already compiled. defaults to '../scylla-jmx'

    Running from scylla relocatable packages:

        SCYLLA_VERSION
            a version from scylla downloads: http://downloads.scylladb.com/relocatable/unstable/master/
            for example: 'unstable/master:380'
        SCYLLA_CORE_PACKAGE
            local path or url for taking the relocatable core package
        SCYLLA_JAVA_TOOLS_PACKAGE
            local path or url for taking the relocatable java tools package
        SCYLLA_JMX_PACKAGE
            local path or url for taking the relocatable jmx package

    Other options:
        CCM_DIR
            directory of scylla ccm, should be already compiled. defaults to '../scylla-ccm'

    for the rest of the options see:
    https://github.com/scylladb/scylla-dtest#common-optional-environment-variables

        SCYLLA_EXT_OPTS
        LOG_SAVED_DIR
        PRINT_DEBUG
        DEBUG
        TRACE
        KEEP_LOGS
        KEEP_TEST_DIR
        KEEP_CORES
        CLUSTER_ID_ALLOCATOR
        NOSE_PROCESSES

    Examples:
    cd ~/scylla-dtest/

    # run specific tests
    ./scripts/run_test.sh read_amplification_test.py:ReadAmplificationTest.no_amplification_on_read_20kb_test

    # run all tests
    ./scripts/run_test.sh
"

here="$(realpath $(dirname "$0"))"
DOCKER_IMAGE="$(<"$here/image")"

export CASSANDRA_DIR=${CASSANDRA_DIR:-`pwd`/../scylla}
SCYLLA_ROOT_DIR=$(echo $CASSANDRA_DIR | sed 's|/build/.*||')

# if CASSANDRA_DIR didn't point to specific variant default to release
if [[ ${CASSANDRA_DIR} == ${SCYLLA_ROOT_DIR} ]]; then
    export CASSANDRA_DIR=${CASSANDRA_DIR}/build/release
fi

export TOOLS_JAVA_DIR=${TOOLS_JAVA_DIR:-`pwd`/../scylla-tools-java}
export JMX_DIR=${JMX_DIR:-`pwd`/../scylla-jmx}
export DTEST_DIR=${DTEST_DIR:-`pwd`}
export CCM_DIR=${CCM_DIR:-`pwd`/../scylla-ccm}
export SCYLLA_DBUILD_SO_DIR=${SCYLLA_DBUILD_SO_DIR:-${CASSANDRA_DIR}/dynamic_libs}
export SCYLLA_EXT_OPTS=${SCYLLA_EXT_OPTS:-"--smp 1 --memory 512M"}

mkdir -p ${HOME}/.dtest
mkdir -p ${HOME}/.ccm
mkdir -p ${HOME}/.certs
mkdir -p ${HOME}/.config
mkdir -p ${HOME}/.local/lib

function check_directory_exists()
{
    if [[ ! -d ${!1} ]]; then
        echo -e "\e[31m\$$1 = ${!1} directory not found\e[0m"
        echo "${help_text}"
        exit 1
    fi
}

check_directory_exists DTEST_DIR
check_directory_exists CCM_DIR

if [[ -z ${SCYLLA_VERSION} ]]; then
    check_directory_exists CASSANDRA_DIR
    check_directory_exists TOOLS_JAVA_DIR
    check_directory_exists JMX_DIR

    if [[ ! -d ${SCYLLA_DBUILD_SO_DIR} ]]; then
        echo "scylla was built with dbuild, and SCYLLA_DBUILD_SO_DIR wasn't supplied or exists"
        set +e
        ${SCYLLA_ROOT_DIR}/tools/toolchain/dbuild -v ${CASSANDRA_DIR}:${CASSANDRA_DIR} -v ${DTEST_DIR}/scripts/dbuild_collect_so.sh:/bin/dbuild_collect_so.sh -- dbuild_collect_so.sh ${CASSANDRA_DIR}/scylla ${SCYLLA_DBUILD_SO_DIR}
        set -e
    fi

    DOCKER_COMMAND_PARAMS="
    -v ${SCYLLA_ROOT_DIR}:${SCYLLA_ROOT_DIR} \
    -v ${CASSANDRA_DIR}:${CASSANDRA_DIR} \
    -v ${TOOLS_JAVA_DIR}:${TOOLS_JAVA_DIR} \
    -v ${JMX_DIR}:${JMX_DIR} \
    -e SCYLLA_JMX_DIR=${JMX_DIR} \
    -e SCYLLA_DBUILD_SO_DIR \
    -e CASSANDRA_DIR \
    "
else
    DOCKER_COMMAND_PARAMS="
    -e SCYLLA_VERSION \
    -e SCYLLA_CORE_PACKAGE \
    -e SCYLLA_JAVA_TOOLS_PACKAGE \
    -e SCYLLA_JMX_PACKAGE
    "
fi

# if in jenkins also mount the workspace into docker
if [[ -d ${WORKSPACE} ]]; then
WORKSPACE_MNT="-v ${WORKSPACE}:${WORKSPACE}"
else
WORKSPACE_MNT=""
fi

docker_cmd="docker run --detach=true \
    ${WORKSPACE_MNT} \
    ${DOCKER_COMMAND_PARAMS} \
    -v ${DTEST_DIR}:${DTEST_DIR} \
    -v ${CCM_DIR}:${CCM_DIR} \
    -e LOG_SAVED_DIR \
    -e HOME \
    -e SCYLLA_EXT_OPTS \
    -e LC_ALL=en_US.UTF-8 \
    -e PRINT_DEBUG \
    -e DEBUG \
    -e TRACE \
    -e KEEP_LOGS \
    -e KEEP_TEST_DIR \
    -e KEEP_CORES \
    -e NOSE_PROCESSES \
    -e CLUSTER_ID_ALLOCATOR \
    -e NODE_TOTAL \
    -e NODE_INDEX \
    -e SCYLLA_MANAGER_PACKAGE \
    -w ${DTEST_DIR} \
    -v /etc/passwd:/etc/passwd:ro \
    -v /etc/group:/etc/group:ro \
    -u $(id -u ${USER}):$(id -g ${USER}) \
    --tmpfs ${HOME}/.cache \
    -v ${HOME}/.local:${HOME}/.local \
    -v ${HOME}/.dtest:${HOME}/.dtest \
    -v ${HOME}/.ccm:${HOME}/.ccm \
    -v ${HOME}/.certs:${HOME}/.certs \
    -v ${HOME}/.config:${HOME}/.config \
    --network=bridge --privileged \
    ${DOCKER_IMAGE} bash -c 'pip install --user -e ${CCM_DIR} ; export PATH=\$PATH:\${HOME}/.local/bin ; bash -c \"${INSTALL_CASSANDRA}\"; nosetests --nologcapture -v -s $*'"
echo "Running Docker: $docker_cmd"
container=$(eval $docker_cmd)


kill_it() {
    if [[ -n "$container" ]]; then
        docker rm -f "$container" > /dev/null
        container=
    fi
}

trap kill_it SIGTERM SIGINT SIGHUP EXIT

docker logs "$container" -f

if [[ -n "$container" ]]; then
    exitcode="$(docker wait "$container")"
else
    exitcode=99
fi

echo "Docker exitcode: $exitcode"

kill_it

trap - SIGTERM SIGINT SIGHUP EXIT

# after "docker kill", docker wait will not print anything
[[ -z "$exitcode" ]] && exitcode=1

exit "$exitcode"
