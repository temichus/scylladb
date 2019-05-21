Scylla Distributed Tests - Introduction
=======================================

Tests for [Scylla](http://www.scylladb.com/) clusters.

Prerequisites
------------

An up to date copy of ccm should be installed for starting and stopping Cassandra.
The tests are run using nosetests.
These tests require the datastax python driver.
A few tests still require the deprecated python CQL over thrift driver.

Installing docker is required for running tests in the scylla-dtest docker container.

 * [ccm](https://github.com/pcmanus/ccm)
 * [nosetests](http://readthedocs.org/docs/nose/en/latest/)
 * [Python Driver](http://datastax.github.io/python-driver/installation.html)
 * [CQL over Thrift Driver](http://code.google.com/a/apache-extras.org/p/cassandra-dbapi2/)
 * [docker](https://docs.docker.com/install/linux/docker-ce/fedora/)

Running using docker
--------------------

Use `scripts/run_test.sh` to run the distributed tests in the `scylla-dtest` docker container.

Optional values can be set via environment variables:
    SCYLLA_DIR, TOOLS_JAVA_DIR, JMX_DIR, DTEST_DIR, CCM_DIR, SCYLLA_DBUILD_SO_DIR, SCYLLA_EXT_OPTS, NOSE_PROCESSES

The script pulls the latest `docker.io/scylladb/scylla-dtest` image (and if that fails, it builds it)
and the it runs nosetests in a docker container based on this image.

This method requires _no_ setup of a virtualenv.

For example:

    CASSANDRA_DIR=../scylla ./scripts/run_test.sh <file>:<class>.<test>

Setup using virtualenv
----------------------

Using `virtualenv` is recommended in order to not pollute your global python installation with the dtest requirements. It also makes it very easy to switch between the different ccm versions (or other package versions) when changing release branches.
To setup a `virtualenv` follow the below instructions:

```bash
# Create the virtualenv.
virtualenv env

# Start using the virtualenv, you should now see `(env)` in you bash prompt.
source ./env/bin/activate

# General dependencies.
pip install -r ./requirements.txt

# Install Scylla CCM, using pip ensures it will be installed *into* the
# virtualenv (setup.py does a global install by default).
cd /path/to/scylla-ccm
pip install .
```

To get rid of the virtual environment just delete the directory (`env` in the above example) it was created in.

To deactivate the virtual environment close the terminal and start a new one (there is no `deactivate` script unfortunately).

To start using the virtual environment in a new terminal just source the `bin/activate` script, like above.


Usage
-----

The tests are run by nosetests. There are a few settings that are
required to run the tests reliably:

The environment variable NOSE_PROCESSES must be set to 1, or maybe
greater (not tested). With the default of 0 tests are unreliable and
after awhile start failing with "Cluster was not allocated".

Fixing the need for NOSE_PROCESSES=1 is tracked by
https://github.com/scylladb/scylla-dtest/issues/942.

The corresponding --process=1 option doesn't seem to work. Fixing that
is tracked by https://github.com/scylladb/scylla-dtest/issues/943.

The command line --process-timeout must be set to a value much higher
than the default of 10 or many tests fail with TimedOutException. A
value of 7200 seems to work for all next-gating tests.

The only thing the framework needs to know is
the location of the (compiled) sources for Scylla. This is done by pointing
the `CASSANDRA_DIR` to the path of the Scylla repository:

    CASSANDRA_DIR=~/path/to/scylla nosetests --process-timeout=7200

To target a Scylla executable compiled in a specific mode include the full path
to the build dir:

    CASSANDRA_DIR=~/path/to/scylla/build/debug nosetests --process-timeout=7200

The shell script `scylla_dtest-env.sh` will set this automatically for you to a
value that works in most deployments, it assumes the Scylla sources are next to
the `scylla-dtest` repository, in a directory called `scylla`. To use this
script just source it:

    source ./scylla_dtest-env.sh

Note that for the dtests to work the Scylla repository has to contain a
directory called `resources` that contains a symlink to a local clone of the
[scylla-tools-java](https://github.com/scylladb/scylla-tools-java) repository.
The name of the symlink has to be `cassandra`. Create it like this:

    cd ~/path/to/scylla
    mkdir resources
    cd resources
    ln -s ~/path/to/scylla-tools-java cassandra

A convenient option if tests are regularly run against the same existing
directory is to set a `default_dir` in `~/.cassandra-dtest`. Create the file and
set it to something like:

    [main]
    default_dir=~/path/to/scylla

The tests will use this directory by default, avoiding the need for any
environment variable (that still will have precedence if given though).

To run the same tests that are used to validate changes into master,
use `-a next-gating`.

Note: To run the upgrade tests, you have must both JDK7 and JDK8 installed. Paths
to these installations should be defined in the environment variables
JAVA7_HOME and JAVA8_HOME, respectively.

Installation Instructions
-------------------------

See more detailed instructions in the included [INSTALL file](https://github.com/riptano/cassandra-dtest/blob/master/INSTALL.md).

Writing Tests
-------------

Existing tests are probably the best place to start to look at how to write
tests.

Each test spawns a new fresh cluster and tears it down after the test, unless
`REUSE_CLUSTER` is set to true. Then some tests will share cassandra instances. If a
test fails, the logs for the node are saved in a `logs/<timestamp>` directory
for analysis (it's not perfect but has been good enough so far, I'm open to
better suggestions).

- Most of the time when you start a cluster with `cluster.start()`, you'll want to pass in `wait_for_binary_proto=True` so the call blocks until the cluster is ready to accept CQL connections. We tried setting this to `True` by default once, but the problems caused there (e.g. when it waited the full timeout time on a node that was deliberately down) were more unpleasant and more difficult to debug than the problems caused by having it `False` by default.
- If you're using JMX via [the `jmxutils` module](jmxutils.py), make sure to call `remove_perf_disable_shared_mem` on the node or nodes you want to query with JMX _before starting the nodes_. `remove_perf_disable_shared_mem` disables a JVM option that's incompatible with JMX (see [this JMX ticket](https://github.com/rhuss/jolokia/issues/198)). It works by performing a string replacement in the node's Cassandra startup script, so changes will only propagate to the node at startup time.

If you'd like to know what to expect during a code review, please see the included [CONTRIBUTING file](CONTRIBUTING.md).
