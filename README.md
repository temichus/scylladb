Scylla Distributed Tests - Introduction
=======================================

Tests for [Scylla](http://www.scylladb.com/) clusters.

Basic overview
--------------

### High level component diagram for basic DTest understanding
The purpose of this diagram is to demonstrate the main test manipulators used by Dtest

![DTest HL component](docs/DTest_HL_component_diagram.jpg?raw=true "DTest HL component diagramm")

1.  Dtest uses [Pytest](https://docs.pytest.org/en/7.2.x/) as test runner
2.  almost all test cases are located under ./ folder in files ending with `_test.py`
3.  [CCM](https://github.com/scylladb/scylla-ccm). Modified for Scylla support 3rd party Java solution that spawns many DB instances(processes)
and provides the interface to easily manage them. DTest uses python SDK but CCM also has CLI interface.
Dtest uses CCM for deployment, run Cassandra-stress and in cqlsh tests.
4. Cassandra driver is used to talk to ScyllaDB instance directly and to perform CRUD (Create/Read/Update/Delete) operations
5. `tools` contain rest of the manipulators that are mainly used for particular test or feature
### known issues and limitations
1. The primary purpose of DTest is to run *Functional* tests not performance because Dtest works locally.
For performance test please refer to [scylla-cluster-tests](https://github.com/scylladb/scylla-cluster-tests)

Contribution (pre-commit setup)
-------------------------------
Since we are trying to keep the code neat, please install this git precommit hooks,
that would fix the code style, and run more checks:

```bash
pre-commit install
```

If you want to remove the hook
```bash
pre-commit uninstall
```


few helpers to know
```bash
# running all checks on all files
pre-commit run -a

# running specific check on all files
pre-commit run -a autopep8

# Doing a commit without the hook checks
git commit ... -n

# running pre-commit from inside docker (if you don't have virtualenv for dtests)
./scripts/run_test.sh 'bash -c "pre-commit run -a"'
```

Running using docker
--------------------
For running dtests in `scylla-dtest` docker container,
[docker](https://docs.docker.com/install/linux/docker-ce/fedora/) is required.


### Quick start:
```bash
mkdir docker_dtest
cd docker_dtest
git clone git@github.com:scylladb/scylla-ccm.git
git clone git@github.com:scylladb/scylla-dtest.git # or from your fork
cd scylla-dtest
./scripts/run_test.sh --scylla-version='unstable/master:latest' <file>::<class>::<test>
```


Use `scripts/run_test.sh` to run the distributed tests in the `scylla-dtest` docker container.

Optional values can be set via environment variables:
    `CASSANDRA_DIR`, `TOOLS_JAVA_DIR`, `JMX_DIR`, `DTEST_DIR`, `CCM_DIR`, `SCYLLA_DBUILD_SO_DIR`, `SCYLLA_EXT_OPTS`, `CLUSTER_ID_ALLOCATOR`

The script pulls the latest `docker.io/scylladb/scylla-dtest` image (and if that fails, it builds it)
and the it runs pytest in a docker container based on this image.

This method requires _no_ setup of a virtualenv.

For example:

    ./scripts/run_test.sh -scylla-version='unstable/master:latest' <file>:<class>.<test>

For more parameters please see [Common Optional Environment Variables and parameters](#common-optional-environment-variables-and-parameters) section

Running Local
----------------------
for running [ccm](https://github.com/scylladb/scylla-ccm) local we need to installed git, Java and pyenv with their dependencies

### Ubuntu requirements:
```bash
apt-get update -y
apt-get install curl git openssh-client -y
apt-get install make build-essential libssl-dev zlib1g-dev \
libbz2-dev libreadline-dev libsqlite3-dev wget curl llvm \
libncursesw5-dev xz-utils tk-dev libxml2-dev libxmlsec1-dev libffi-dev liblzma-dev -y
apt-get install openjdk-8-jdk -y
```

### Fedora requirements:
```bash
dnf install git -y
dnf groupinstall "Development Tools" -y
dnf install zlib-devel bzip2 bzip2-devel readline-devel sqlite sqlite-devel openssl-devel xz xz-devel libffi-devel findutils -y
dnf install java-1.8.0-openjdk-devel -y
```

make ssh key using `ssh-keygen` and [add a new SSH key to your GitHub account](https://docs.github.com/en/authentication/connecting-to-github-with-ssh/adding-a-new-ssh-key-to-your-github-account)


Using `pyenv virtualenv` is recommended in order to not pollute your global python3 installation with the dtest requirements. It also makes it very easy to switch between the different ccm versions (or other package versions) when changing release branches.
To setup a `pyenv virtualenv` follow the below instructions:

### Quick start:
```bash
mkdir local_dtest
cd local_dtest
git clone git@github.com:scylladb/scylla-ccm.git
git clone git@github.com:scylladb/scylla-dtest.git # or from your fork
cd scylla-dtest
## install  3.9.10 via pyenv same as used in docker installation
## ❯ docker run -it `cat ./scripts/image` python --version
##      Python 3.9.10
curl https://pyenv.run | bash
# go to: https://github.com/pyenv/pyenv/wiki/Common-build-problems#prerequisites
# and follow the instructions for your distribution, to install the prerequisites
#then run exec $SHELL
# or for run the following commands to add 'pyenv' in path for current terminal session
export PYENV_ROOT="$HOME/.pyenv"
command -v pyenv >/dev/null || export PATH="$PYENV_ROOT/bin:$PATH"
eval "$(pyenv init -)"
# compiling python from source
pyenv install 3.9.10
# create a virtualenv for Dtest
pyenv virtualenv 3.9.10 dtest-3.9.10
# cd to projecct tocectory and run
pyenv local dtest-3.9.10
# Some external tools (e.g. jedi) might require you to activate the virtualenv and conda environments.
# If eval "$(pyenv virtualenv-init -)" is configured in your shell, pyenv-virtualenv will automatically
# activate/deactivate virtualenvs on entering/leaving directories which contain a .python-version
# General dependencies.
pip install -r ./requirements.txt
# Install Scylla CCM, using pip ensures it will be installed *into* the
# virtualenv (setup.py does a global install by default).
pip install ../scylla-ccm/.
pytest --scylla-version='unstable/master:latest'  <file>::<class>::<test>
```

* To get rid of the virtual environment run `pyenv virtualenv-delete dtest-3.9.10`
* To deactivate the virtual environment `cd` to another folder
* To start using the virtual environment again just `cd` to repo folder
* To remove automatic virtual environment enabling on `cd` just delete file `.python-version` in repo directory


Usage
-----
### Running with relocatable packages

The only thing needed is the location of the (compiled) sources for Scylla. This is done by pointing
the `SCYLLA_VERSION` to shortname representing the directory in our `s3://downloads.scylladb.com`:

```bash
pytest --scylla-version=unstable/master:latest [other pytest parameters]
```

Getting the listings or the latest version can be done like that:

### From web:

go to https://downloads.scylladb.com/unstable/


### From AWS cli
```bash
LATEST_MASTER_JOB_ID=`aws s3 ls downloads.scylladb.com/relocatable/unstable/master/ | tr -s ' ' | cut -d ' ' -f 3 | tr -d '\/'  | sort -g | tail -n 1`
LATEST_SCYLA_VERSION=master:${LATEST_MASTER_JOB_ID}
```

### Running with relocatable packages from compiled tarballs

Taking a relocatable using you own scylla core compiled package
a tarball that was built by scylla [scripts/create-relocatable-package.py](https://github.com/scylladb/scylla/blob/master/scripts/create-relocatable-package.py)

Commands to build relocatable package:
```bash
cd /path/to/scylla
git submodule update --init --force --recursive
./tools/toolchain/dbuild ./configure.py
./tools/toolchain/dbuild ninja -j2 dist-unified-release
```
at the end of building procedure you will see the message like
```commandline
[555/555] unified/build_unified.sh --mode release --unified-pkg 'build/release/dist/tar/scylla-unified-5.3.0~dev-0.20230206.53366db6c667.x86_64.tar.gz'
```
use

**NOTE:** `~/.ccm/scylla-repository/local_tarball` should be deleted, otherwise it would use it as is.
ccm currently isn't very smart on the way it's caching the versions.

Command to run tests wia pytest:
```bash
SCYLLA_UNIFIED_PACKAGE=absolute/path/to/scylla-package.tar.gz pytest --scylla-version=local_tarball  <file>::<class>::<test>
```
Command to run tests using docker:
```bash
WORKSPACE='absolute/path/to/scylla' SCYLLA_UNIFIED_PACKAGE='absolute/path/to/scylla-package.tar.gz' ./scripts/run_test.sh --scylla-version='local_tarball'  <file>::<class>::<test>
```
### Running different architecture

Use `SCYLLA_ARCH` environment variable, so ccm and run_test.sh could know which architecture to use.
You should first install https://github.com/multiarch/qemu-user-static for it to work on x64 machine.

The following command would pull the arm64 docker image, and run with it.

```commandline
export INSTALL_CASSANDRA='ccm create scylla-tmp --scylla -n 1 --version=unstable/master:2021-12-28T04:03:46Z'
SCYLLA_ARCH=aarch64 ./scripts/run_test.sh cql_additional_tests.py::TestCQL::test_mc_sstables_case_sensitive_insert --scylla-version=unstable/master:2021-12-28T04:03:46Z
```

### Running from the compiled source
requirements:

1) Java Version:


    scylla-jmx requires a JDK8 launcher (JDK11 or higher will not work).
    To control which java executable is used to run scylla-jmx you can
    set the JAVA_HOME variable to point to an appropriate jre/jdk.

2) directory of dynamic .so files. Please see https://github.com/scylladb/scylla-dtest/blob/next/docs/working-with-dbuild.md for more info
```bash
export SCYLLA_DIR=/absolute/path/to/scylla-next
export DTEST_DIR=/absolute/path/to/scylla-dtest
cd ${SCYLLA_DIR}
./tools/toolchain/dbuild -it -v ${DTEST_DIR}/scripts/dbuild_collect_so.sh:/bin/dbuild_collect_so.sh -- bash
mkdir dynamic_libs_for_dtest
dbuild_collect_so.sh build/release/scylla dynamic_libs_for_dtest/
export SCYLLA_DBUILD_SO_DIR=${SCYLLA_DIR}/dynamic_libs_for_dtest
```
run tests via pytest:
```bash
SCYLLA_DBUILD_SO_DIR='absolute/path/to/dynamic_libs_for_dtest' pytest --cassandra-dir='absolute/path/to/scylla'  <file>::<class>::<test>
```

run tests using docker:
```bash
WORKSPACE='absolute/path/to/scylla' SCYLLA_DBUILD_SO_DIR='absolute/path/to/dynamic_libs_for_dtest' ./scripts/run_test.sh --cassandra-dir='absolute/path/to/scylla'  <file>::<class>::<test>
```

To target a Scylla executable compiled in a specific mode include the full path
to the build dir:
```commandline
  --cassandra-dir=~/path/to/scylla/build/debug

```

Note: To run the upgrade tests, you have must both JDK7 and JDK8 installed. Paths
to these installations should be defined in the environment variables
JAVA7_HOME and JAVA8_HOME, respectively.

### Changing the Cluster ID Allocator

The default allocator was changed to the RandomClusterIdAllocator that draws
a random number in the range [1, 99] and allocates it by creating a symbolic link
in the ~/.dtest directory by that name, pointing at the cluster directory.
This way conflicts are resolved with no need for shared memory-based coordination
between pytest processes.  pytest, if pytest is aborted before the symbolic
link has been removed, there may be stale symlinks that prevent re-allocating
those cluster IDs.  These should be cleaned up by hand.

Common Optional Environment Variables and Parameters
-------------------------------------
**NOTE:**  all mentioned parameters are applied for pytest and run_test.sh , as call the same pytest command inside Docker environment

To run a specific test in a test file concatenate class and test:

    pytest <file>.py::<class>::<test>
See more information about dtest here: [Scylla-DTEST](https://github.com/scylladb/scylla/wiki/Scylla-DTEST)

To run the same tests that are used to validate changes into master,

    -m next_gating

To set the maximum number of tests to run concurrently (1 by default), use, for example:

    pytest -n 3

To print additional test debug messages, use:

    pytest --log-cli-level=debug

To (re)use a directory for saving the system-under-test nodes' logs, use:

    LOG_SAVED_DIR=<logs_dir>

To keep logs of just failed tests in `$LOG_SAVED_DIR`, rather than all tests, use:

    pytest --delete-logs=pass

To keep all test cluster directories (under `$HOME/.dtest/`), use:

    pytest --keep-test-dir

> See also "Test Directories" below.

To skip all tests and just check that all modules are found:

     pytest --collect-only

To change Scylla CPU and memory configuration:

    SCYLLA_EXT_OPTS="--smp 2 --memory 1G"

To pass environment variables for running scylla:

    SCYLLA_EXT_ENV="ASAN_OPTIONS=disable_coredump=0:abort_on_error=1:detect_stack_use_after_return=1;UBSAN_OPTIONS=halt_on_error=1:abort_on_error=1;BOOST_TEST_CATCH_SYSTEM_ERRORS=no"

To tune the behavior of the @pytest.mark.require marker:

    The @pytest.mark.require skips tests based on the github status of the respective issue it refers to.
    The test is skipped while the issue is open. Once the issue is closed on github, the test will start running again.
    However for that to work, the test environemnt needs to be set up properly to be able to use the github REST API.
    To disable the @pytest.mark.require marker and allow running test marked with `require` locally (e.g. for working on provisional fixes for them),
    the DTEST_REQUIRE environment variable may be set to `disabled`, as follows:

    export DTEST_REQUIRE=disabled

    Here are the supported values for `DTEST_REQUIRE`:
    - auto : default value, check issue state in @pytest.mark.require marker and run(state=closed) or skip(state=open) test
    - enabled : skip tests marked with @pytest.mark.require
    - disabled : disable @pytest.mark.require decorator and run test (mostly for manual tests)

Test Directories
----------------
Each test directory is given a temporary name, e.g. `dtest-IouAlot`,
under which the test cluster is created as `test`.

The test directory holds:
* `cluster.conf`: The cluster ccm configuration file.
* `current_test`: A file holding the name of the current test.
* `node<n>/`: Cluster node directories, each containing a complete node hierarchy, including:
    * `node.conf`: The node ccm configuration file.
    * `cassandra.pid`: Containing the process ID of the running scylla process.
    * `scylla-jmx.pid`: Containing the process ID of the running scylla-jmx java-management interface process.
    * `bin/`: A directory containing the scylal and scylla-jmx binaries as well as other scripts.
    * `conf/`: Containing the node configuration files, `scylla.yaml` in particular.
    * `commitlogs/`, `data/`, `hints/`, `view_hints/`: The database (meta)data directories.
    * `logs/`: Containing the node logs: `system.log` and `system.log.jmx`.

scylla_tests
------------

The file `scylla_tests` in the root of this repository holds the list of stable
tests that are run regularly on scylla master and release branches.

The file lists either complete test files (e.g. `auth_test.py`),
in which case, pytest runs all test cases in the file (unless skipped
with the `@pytest.mark.skip()` directive), or individual test cases, using the
`<file>:<class>.<test>` notation.


Writing Tests
-------------

Existing tests are probably the best place to start to look at how to write
tests.

Each test spawns a new fresh cluster and tears it down after the test, unless
`REUSE_CLUSTER` is set to true. Then some tests will share cassandra instances. If a
test fails, the logs for the node are saved in a `logs/<timestamp>` directory
for analysis (it's not perfect but has been good enough so far, I'm open to
better suggestions).

- By default, when the cluster is started, `wait_for_binary_proto=True` and `wait_other_notice=True` are set by default
so the call blocks until all nodes start and the cluster is ready to accept CQL connections.
You may want to explicitly set these to `False` if you want to execute some stimulus while the cluster is starting.
Another example is setting `wait_for_binary_proto=False` and/or `wait_other_notice=False` when starting a single node
that is not expected to be able to join the cluster or if other nodes in the cluster are considered alive but
cannot detect that the node started.

If you'd like to know what to expect during a code review, please see the included [CONTRIBUTING file](CONTRIBUTING.md).

Saving Coredumps
----------------

By default, dtest.py looks for coredump files when the test finishes,
and if found, they are copied to the test logs directory under `logs/<timestamp>_test_name`.

Note that modern linux systems use `coredumpctl` and therefore will not dump core files.
To use this feature, run the following command to instruct the kernel to dump core files in the current working directory:

    sudo /sbin/sysctl kernel.core_pattern="%e.%p.%t.core"

The coredump file are compressed by default.
To change the compression tool and/or compressed core file extension, use:

    DTEST_CORE_COMPRESS_TOOL=lz4
    DTEST_CORE_COMPRESS_EXT=lz4

To disable coredump compression, set:

    DTEST_CORE_COMPRESS_TOOL=""

To disable coredump collection altogether, set:

    KEEP_CORES=false

Uploading docker images
-----------------------

when doing changes to requirements.txt, or any other change to docker image, it can be uploaded like this:

**Via a jenkins job:**

https://jenkins.scylladb.com/job/scylla-staging/job/dtest-build-docker-image/

when it ends, copy the docker image pushed to dockerhub, into `scripts/image`

**Manually:**
```bash
export DTEST_DOCKER_IMAGE=scylladb/scylla-dtest:fedora-34-pytest-$(date +'%Y%m%d')
docker buildx build --platform linux/arm64,linux/amd64 -t  ${DTEST_DOCKER_IMAGE} . --push
echo "${DTEST_DOCKER_IMAGE}" > scripts/image
```
**Note:** you'll need permissions on the scylladb dockerhub organization for uploading images
