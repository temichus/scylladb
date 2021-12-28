Scylla Distributed Tests - Introduction
=======================================

Tests for [Scylla](http://www.scylladb.com/) clusters.

Prerequisites
------------

See [requirements.txt](./requirements.txt). In addition to the packages listed
therein, the following extra packages are required (not published to PIP):
 * [ccm](https://github.com/scylladb/scylla-ccm)

For running dtests in `scylla-dtest` docker container,
[docker](https://docs.docker.com/install/linux/docker-ce/fedora/) is required.
Note that when using docker, the other prerequisites are _not_ needed on the
host.

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

Use `scripts/run_test.sh` to run the distributed tests in the `scylla-dtest` docker container.

Optional values can be set via environment variables:
    `SCYLLA_DIR`, `TOOLS_JAVA_DIR`, `JMX_DIR`, `DTEST_DIR`, `CCM_DIR`, `SCYLLA_DBUILD_SO_DIR`, `SCYLLA_EXT_OPTS`, `CLUSTER_ID_ALLOCATOR`

The script pulls the latest `docker.io/scylladb/scylla-dtest` image (and if that fails, it builds it)
and the it runs pytest in a docker container based on this image.

This method requires _no_ setup of a virtualenv.

For example:

    CASSANDRA_DIR=../scylla ./scripts/run_test.sh <file>:<class>.<test>

Setup using virtualenv
----------------------

Using `virtualenv` is recommended in order to not pollute your global python3 installation with the dtest requirements. It also makes it very easy to switch between the different ccm versions (or other package versions) when changing release branches.
To setup a `virtualenv` follow the below instructions:

```bash
# fedora
sudo dnf install python3 python3-devel python3-pip python3-virtualenv

# centos/redhat
sudo yum install https://centos7.iuscommunity.org/ius-release.rpm
sudo yum install python36u python36u-libs python36u-devel python36u-pip python36u-virtualenv

# ubuntu/debian
sudo apt install python3 python3-dev python3-pip python3-virtualenv

# Create the virtualenv (dtests require python3)
python3 -m virtualenv env

# Start using the virtualenv, you should now see `(env)` in you bash prompt.
source ./env/bin/activate

# General dependencies.
pip3 install -r ./requirements.txt

# Install Scylla CCM, using pip ensures it will be installed *into* the
# virtualenv (setup.py does a global install by default).
cd /path/to/scylla-ccm
pip3 install .
```

To get rid of the virtual environment just delete the directory (`env` in the above example) it was created in.

To deactivate the virtual environment close the terminal and start a new one (there is no `deactivate` script unfortunately).

To start using the virtual environment in a new terminal just source the `bin/activate` script, like above.


Usage
-----
### Running with relocatable packages

The only thing needed is the location of the (compiled) sources for Scylla. This is done by pointing
the `SCYLLA_VERSION` to shortname representing the directory in our `s3://downloads.scylladb.com`:

```bash
pytest --scylla-version=unstable/master:201910240141 [other pytest parameters]
```

Getting the listings or the latest version can be done like that:
```bash
LATEST_MASTER_JOB_ID=`aws s3 ls downloads.scylladb.com/relocatable/unstable/master/ | tr -s ' ' | cut -d ' ' -f 3 | tr -d '\/'  | sort -g | tail -n 1`
LATEST_SCYLA_VERSION=master:${LATEST_MASTER_JOB_ID}
```

### Running with relocatable packages from compiled tarballs

Taking a relocatable using you own scylla core compiled package
a tarball that was built by scylla [scripts/create-relocatable-package.py](https://github.com/scylladb/scylla/blob/master/scripts/create-relocatable-package.py)
see scylla [docs/building-packages.md#scylla-server](https://github.com/scylladb/scylla/blob/master/docs/building-packages.md#scylla-server)

**NOTE:** `~/.ccm/scylla-repository/unstable/master/201910240141` should be deleted, otherwise it would use it as is.
ccm currently isn't very smart on the way it's caching the versions.

```bash
SCYLLA_CORE_PACKAGE=../scylla/build/dev/scylla-package.tar.gz
pytest --scylla-version=unstable/master:201910240141  [more pytest parameters]
```

Also `SCYLLA_JAVA_TOOLS_PACKAGE` or `SCYLLA_JMX_PACKAGE` can be used for replacing other relocatable packages relevant.

All the `*_PACKAGE` environment variables can also point to public available files on http

### Running different architecture

Use `SCYLLA_ARCH` environment variable, so ccm and run_test.sh could know which architecture to use.
You should first install https://github.com/multiarch/qemu-user-static for it to work on x64 machine.

The following command would pull the arm64 docker image, and run with it.

```commandline
export INSTALL_CASSANDRA='ccm create scylla-tmp --scylla -n 1 --version=unstable/master:2021-12-28T04:03:46Z'
SCYLLA_ARCH=aarch64 ./scripts/run_test.sh cql_additional_tests.py::TestCQL::test_mc_sstables_case_sensitive_insert --scylla-version=unstable/master:2021-12-28T04:03:46Z
```

### Running from the compiled source

The only thing needed is the location of the (compiled) sources for Scylla. This is done by pointing
the `--cassandra_dir` to the path of the Scylla repository:

    pytest --cassandra-dir=~/path/to/scylla

To target a Scylla executable compiled in a specific mode include the full path
to the build dir:

    pytest --cassandra-dir=~/path/to/scylla/build/debug

The shell script `scylla_dtest_env.sh` will set this automatically for you to a
value that works in most deployments, it assumes the Scylla sources are next to
the `scylla-dtest` repository, in a directory called `scylla`. To use this
script just source it:

    source ./scylla_dtest_env.sh

Note that for the dtests to work the Scylla repository has to contain a
directory called `resources` that contains a symlink to a local clone of the
[scylla-tools-java](https://github.com/scylladb/scylla-tools-java) repository,
that is now a submodule of the `scylla` repository.
The name of the symlink has to be `cassandra`. Create it like this:

    cd ~/path/to/scylla
    mkdir resources
    ln -s ../tools/java resources/cassandra

A convenient option if tests are regularly run against the same existing
directory is to set a `default_dir` in `~/.cassandra-dtest`. Create the file and
set it to something like:

    [main]
    default_dir=~/path/to/scylla

The tests will use this directory by default, avoiding the need for any
environment variable (that still will have precedence if given though).

To run a specific test in a test file concatenate class and test:

    pytest <file>.py::<class>::<test>

To run the same tests that are used to validate changes into master,
use `-m next_gating`.

Note: To run the upgrade tests, you have must both JDK7 and JDK8 installed. Paths
to these installations should be defined in the environment variables
JAVA7_HOME and JAVA8_HOME, respectively.

See more information about dtest here: [Scylla-DTEST](https://github.com/scylladb/scylla/wiki/Scylla-DTEST)

### Changing the Cluster ID Allocator

The default allocator was changed to the RandomClusterIdAllocator that draws
a random number in the range [1, 99] and allocates it by creating a symbolic link
in the ~/.dtest directory by that name, pointing at the cluster directory.
This way conflicts are resolved with no need for shared memory-based coordination
between pytest processes.  pytest, if pytest is aborted before the symbolic
link has been removed, there may be stale symlinks that prevent re-allocating
those cluster IDs.  These should be cleaned up by hand.

Common Optional Environment Variables
-------------------------------------

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

Installation Instructions
-------------------------

See more detailed instructions in the included [INSTALL file](./INSTALL.md).

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

- If you're using JMX via [the `jmxutils` module](jmxutils.py), make sure to call `remove_perf_disable_shared_mem` on the node or nodes you want to query with JMX _before starting the nodes_. `remove_perf_disable_shared_mem` disables a JVM option that's incompatible with JMX (see [this JMX ticket](https://github.com/rhuss/jolokia/issues/198)). It works by performing a string replacement in the node's Cassandra startup script, so changes will only propagate to the node at startup time.

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
