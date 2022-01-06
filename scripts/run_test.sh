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
            directory of scylla java tools, should be already compiled. defaults to '\$SCYLLA_ROOT_DIR/tools/java',
            if it exists, or to '../scylla-tools-java', otherwise.
        JMX_DIR
            directory of scylla jmx, should be already compiled. defaults to '\$SCYLLA_ROOT_DIR/tools/jmx',
            if it exists, or to '../scylla-jmx', otherwise.

    Running from scylla relocatable packages:

        --scylla-version
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
        SCYLLA_EXT_ENV
        LOG_SAVED_DIR
        KEEP_CORES
        GITHUB_TOKEN - github api token to get issue states
        DTEST_REQUIRE - auto : default value, check issue state in @pytest.mark.require marker and run(state=closed) or skip(state=open) test
                      - enabled : skip tests marked with @pytest.mark.require
                      - disabled : disable @pytest.mark.require decorator and run test (mostly for manual tests)

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

# turn CASSANDRA_DIR into an absolute path if needed
if [[ ${CASSANDRA_DIR} != /* ]]; then
    export CASSANDRA_DIR="`pwd`/${CASSANDRA_DIR}"
fi

SCYLLA_ROOT_DIR=$(echo $CASSANDRA_DIR | sed 's|/build/.*||')

# if CASSANDRA_DIR didn't point to specific variant default to release
if [[ ${CASSANDRA_DIR} == ${SCYLLA_ROOT_DIR} ]]; then
    export CASSANDRA_DIR=${CASSANDRA_DIR}/build/release
fi

if [[ ${CASSANDRA_DIR} == */build/* ]]; then
    mode=$(echo $CASSANDRA_DIR | sed 's|.*/build/||')
fi

SCYLLA_PRODUCT=$(basename ${SCYLLA_ROOT_DIR})
export TOOLS_JAVA_DIR=$(
    { [ -n "${TOOLS_JAVA_DIR}" ] && echo "${TOOLS_JAVA_DIR}"; } ||
    { [ -d "${SCYLLA_ROOT_DIR}/tools/java" ] && echo "${SCYLLA_ROOT_DIR}/tools/java"; } ||
    { echo "$(pwd)/../${SCYLLA_PRODUCT}-tools-java"; }
)
export JMX_DIR=$(
    { [ -n "${JMX_DIR}" ] && echo "${JMX_DIR}"; } ||
    { [ -d "${SCYLLA_ROOT_DIR}/tools/jmx" ] && echo "${SCYLLA_ROOT_DIR}/tools/jmx"; } ||
    { echo "${SCYLLA_ROOT_DIR}/${SCYLLA_PRODUCT}-jmx"; }
)
export DTEST_DIR=${DTEST_DIR:-`pwd`}
export CCM_DIR=${CCM_DIR:-$(echo ${DTEST_DIR} | sed 's/-dtest$/-ccm/')}
export SCYLLA_DBUILD_SO_DIR=$( realpath ${SCYLLA_DBUILD_SO_DIR:-${CASSANDRA_DIR}/dynamic_libs} )
export SCYLLA_EXT_OPTS=${SCYLLA_EXT_OPTS:-"--smp 2 --memory 1024M"}
if [[ "$mode" == debug ]]; then
    export DEF_SCYLLA_EXT_ENV="ASAN_OPTIONS=disable_coredump=0:abort_on_error=1:detect_stack_use_after_return=1;UBSAN_OPTIONS=halt_on_error=1:abort_on_error=1;BOOST_TEST_CATCH_SYSTEM_ERRORS=no"
fi
export SCYLLA_EXT_ENV=${SCYLLA_EXT_ENV:-"$DEF_SCYLLA_EXT_ENV"}

mkdir -p ${HOME}/.dtest
mkdir -p ${HOME}/.ccm
mkdir -p ${HOME}/.certs
chmod 0700 ${HOME}/.certs
mkdir -p ${HOME}/.config
mkdir -p ${HOME}/.local/lib
mkdir -p ${HOME}/.cassandra
mkdir -p ${HOME}/.cache/pre-commit

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

if [[ "$*" == *'--cassandra-dir'* ]]; then
    check_directory_exists CASSANDRA_DIR
    check_directory_exists TOOLS_JAVA_DIR
    check_directory_exists JMX_DIR

    cas_symlink=${SCYLLA_ROOT_DIR}/resources/cassandra
    if [ ! -h ${cas_symlink} ]; then
        echo -e "\e[31mwarning: ${cas_symlink}: symbolic link not found\e[0m"
    else
        cas_inode_num=$(ls -Lid ${cas_symlink} | awk '{print $1}')
        tools_java_inode_num=$(ls -Lid ${TOOLS_JAVA_DIR} | awk '{print $1}')
        if [ ${cas_inode_num} != ${tools_java_inode_num} ]; then
            echo -e "\e[31mwarning: ${cas_symlink} mismatches \$TOOLS_JAVA_DIR"
            echo -e "         $(ls -ld ${cas_symlink})"
            echo -e "         $(ls -ld ${TOOLS_JAVA_DIR})\e[0m"
        fi
    fi

    if [[ ! -d ${SCYLLA_DBUILD_SO_DIR} ]] || diff -q ${SCYLLA_ROOT_DIR}/tools/toolchain/image ${SCYLLA_DBUILD_SO_DIR}/image; then
        echo "scylla was built with dbuild, and SCYLLA_DBUILD_SO_DIR wasn't supplied, does not exist, or is outdated"
        ${SCYLLA_ROOT_DIR}/tools/toolchain/dbuild -v ${CASSANDRA_DIR}:${CASSANDRA_DIR} -v ${DTEST_DIR}/scripts/dbuild_collect_so.sh:/bin/dbuild_collect_so.sh -- dbuild_collect_so.sh ${CASSANDRA_DIR}/scylla ${SCYLLA_DBUILD_SO_DIR}
        cp ${SCYLLA_ROOT_DIR}/tools/toolchain/image ${SCYLLA_DBUILD_SO_DIR}
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
elif [[ "$*" == *'--scylla-version'*  ]]; then
    DOCKER_COMMAND_PARAMS="
    -e SCYLLA_VERSION \
    -e SCYLLA_CORE_PACKAGE \
    -e SCYLLA_JAVA_TOOLS_PACKAGE \
    -e SCYLLA_JMX_PACKAGE
    "
fi

# A link between the dtest docker to the minio docker
if [[ -z ${MINIO_DOCKER_ID} ]]; then
    export MINIO_DOCKER_LINK_PARAM=""
    export DOCKER_NETWORK_PARAM="--network=bridge"  # TODO: Enhancement: replace the bridge with a network
    # removed the --network=bridge from the docker run command specifically for manager testing,
    # since docker sometimes does not allow to create a link when this attribute is set (even though it's the default)
else
    export MINIO_DOCKER_LINK_PARAM="--link ${MINIO_DOCKER_ID}:MinioServer"
    export DOCKER_NETWORK_PARAM=""
    export AWS_S3_ENDPOINT="http://MinioServer:9000"
fi

echo
env | grep -E '^((DTEST|CCM|SCYLLA_ROOT|CASSANDRA|TOOLS_JAVA|JMX|SCYLLA_DBUILD_SO|LOG_SAVED)_DIR|HOME|SCYLLA_.*|CLUSTER_.*|DRY_.*|NODE_.*|AWS_.*)='
echo

# if in jenkins also mount the workspace into docker
if [[ -d ${WORKSPACE} ]]; then
WORKSPACE_MNT="-v ${WORKSPACE}:${WORKSPACE}"
else
WORKSPACE_MNT=""
fi

# export all BUILD_* env vars into the docker run
BUILD_OPTIONS=$(env | grep BUILD_ | cut -d "=" -f 1 | xargs -i echo "--env {}")

# export all AWS_* env vars into the docker run
AWS_OPTIONS=$(env | grep AWS_ | cut -d "=" -f 1 | xargs -i echo "--env {}")

# export all JENKINS_* env vars into the docker run
JENKINS_OPTIONS=$(env | grep JENKINS_ | cut -d "=" -f 1 | xargs -i echo "--env {}")

group_args=()
for gid in $(id -G); do
    group_args+=(--group-add "$gid")
done

subcommand=""

for i in "$@"
do
subcommand+=$(printf "%q " "$i")
done

if [[ ${subcommand} == *'bash'* ]] || [[ ${subcommand} == *'python'* ]]; then
    CMD=${subcommand}
else
    CMD="bash -c $'sudo rsyslogd; pip3 install --user ${CCM_DIR} ; export PATH=\$PATH:\${HOME}/.local/bin ; cp -a /.ccm/repo* \${HOME}/.ccm/ ; bash -c \"${INSTALL_CASSANDRA}\"; python3 -m pytest -v -s ${subcommand}'"
fi

docker_cmd="docker run --init --detach=true \
    ${WORKSPACE_MNT} \
    ${DOCKER_COMMAND_PARAMS} \
    ${MINIO_DOCKER_LINK_PARAM} \
    -v ${DTEST_DIR}:${DTEST_DIR} \
    -v ${CCM_DIR}:${CCM_DIR} \
    -e LOG_SAVED_DIR \
    -e HOME \
    -e USER \
    -e SCYLLA_EXT_OPTS \
    -e SCYLLA_EXT_ENV \
    -e LC_ALL=en_US.UTF-8 \
    -e KEEP_CORES \
    -e NODE_TOTAL \
    -e NODE_INDEX \
    -e SCYLLA_MANAGER_PACKAGE \
    -e AWS_S3_ENDPOINT \
    -e AWS_ACCESS_KEY_ID \
    -e AWS_SECRET_ACCESS_KEY \
    -e PYTHONUNBUFFERED=1 \
    -e GITHUB_TOKEN \
    -e DTEST_REQUIRE \
    -w ${DTEST_DIR} \
    -v /var/run/docker.sock:/var/run/docker.sock \
    -v /sys/fs/cgroup:/sys/fs/cgroup:ro \
    -v /etc/passwd:/etc/passwd:ro \
    -v /etc/group:/etc/group:ro \
    -v /dev:/dev \
    -u $(id -u ${USER}):$(id -g ${USER}) \
    ${group_args[@]} \
    --tmpfs ${HOME}/.cache \
    -v ${HOME}/.cache/pre-commit:${HOME}/.cache/pre-commit \
    -v ${HOME}/.local:${HOME}/.local \
    -v ${HOME}/.dtest:${HOME}/.dtest \
    -v ${HOME}/.ccm:${HOME}/.ccm \
    -v ${HOME}/.certs:${HOME}/.certs \
    -v ${HOME}/.config:${HOME}/.config \
    -v ${HOME}/.cassandra:${HOME}/.cassandra \
    -v ${HOME}/.aws:${HOME}/.aws \
    ${DOCKER_NETWORK_PARAM} \
    ${BUILD_OPTIONS} \
    ${AWS_OPTIONS} \
    ${JENKINS_OPTIONS} \
    --privileged \
    --ulimit nofile=40000:40000 \
    ${DOCKER_IMAGE} $CMD"
echo "Running Docker: $docker_cmd"
container=$(eval $docker_cmd)


kill_it() {
    if [[ -n "$container" ]]; then
        docker rm -f "$container" > /dev/null
        container=
    fi
}

trap kill_it SIGTERM SIGINT SIGHUP EXIT

docker logs -f "$container"

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
