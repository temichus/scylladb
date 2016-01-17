Scylla Test Plan {#mainpage}
============================

Test Plan Document {#scylla_test_plan}
======================================

[TOC]

This is the Scylla Test Plan document. It covers the tests executed to ensure
the quality of the Database Services and associated tools, in terms of
functionality, administration tasks, distributed, performance,
scalability and behavior under stress.

Many of the tests are implmented as a Scylla Distributed tests (that's why
the test plan is located in the dtests repository), but there are some tests
that are executed in other test suites. When that is the case, the test will
be appropriately linked here.

Functional {#label_functional} 
------------------------------

Ensure that the Scylla database and associated tools perform their functions
adequately.

### CQL {#label_cql} ###

Scylla implements the Cassandra query language interface (CQL) [1], a
language used to retrieve data from the database through SQL [2] style
queries. The CQL tests aim to verify that CQL statements return appropriate
values given a set of inputs, and that the Scylla clusters behave well while
serving the CQL requests.

CQL tests are implemented as dtests, and java unittests. See:

* cql_tests.AbortedQueriesTester
* cql_tests.CQLTester
* cql_tests.MiscellaneousCQLTester
* cql_tests.StorageProxyCQLTester
* cql_prepared_test.TestCQL
* cql_additional_tests.CQLAdditionalTests
* cql_additional_tests.TestCQL
* scylla_unsupported_test.ScyllaUnsupportedTest

  [1]: https://cassandra.apache.org/doc/cql3/CQL.html
  [2]: https://en.wikipedia.org/wiki/SQL

### SSTables {#label_sstables} ###

SSTables are the files that Scylla uses to actually store data in the disk.
There are developer level tests to verify integrity and behavior of such
files.

### Companion Tools {#label_companion_tools} ###

Tests the scylla companion tools.

#### nodetool {#label_nodetool} ####

nodetool should accept only supported commands and supported params
nodetool should provide help only to supported commands and supported params

Nodetool is a tool to control and manage scylla nodes. The subcommands to test
are:

##### move #####

Test nodetool command `move`.

##### cfhistograms #####

Test nodetool command `cfhistograms`.

##### cfstats #####

Test nodetool command `cfstats`.

##### clearsnapshot #####

Test nodetool command `clearsnapshot`.

##### compact  #####

Test nodetool command `compact`.

##### decommission #####

Test nodetool command `decommission`.

##### describecluster #####

Test nodetool command `describecluster`.

##### describering #####

Test nodetool command `describering`.

##### disablegossip #####

Test nodetool command `disablegossip`.

##### enablegossip #####

Test nodetool command `enablegossip`.

##### flush #####

Test nodetool command `flush`.

##### getendpoints #####

Test nodetool command `getendpoints`.

##### gossipinfo #####

Test nodetool command `gossipinfo`.

##### info #####

Test nodetool command `info`.

##### netstats #####

Test nodetool command `netstats`.

##### refresh #####

Test nodetool command `refresh`.

##### repair #####

Test nodetool command `repair`.

##### ring #####

Test nodetool command `ring`.

##### status #####

Test nodetool command `status`.

##### tpstats #####

Test nodetool command `tpstats`.

##### cleanup #####

Test nodetool command `cleanup`.

##### compactionhistory #####

Test nodetool command `compactionhistory`.

##### compactionstats #####

Test nodetool command `compactionstats`.

##### drain #####

Test nodetool command `drain`.

##### rebuild #####

Test nodetool command `rebuild`.

##### removenode #####

Test nodetool command `removenode`.

#### cassandra-stress {#label_cassandra_stress} ####

The tool `cassandra-stress` executes read and write operations against a
scylla cluster. It is used to verify scylla node operation throughput.

#### cqlsh {#label_cqlsh} ####

The tool `cqlsh` is an interface to perform CQL queries against a scylla
database. We aim to verify if the tool is working according to the given
user scenarios.

### Installable packages and images {#label_installable_pkgs_imgs} ###

Installable package testing works by:

  1. Installing the given packages
  2. Checking that the service is running
  3. Run nodetool status
  4. Run cassandra-stress
  5. Restart services, repeat 2-4
  6. Stop and start services, repeat 2-4

Implemented in:

https://github.com/lmr/scylla-artifact-tests

#### Fedora RPMs {#label_fedora_rpms} ####

Test the produced Fedora RPMs.

#### CentOS RPMs {#label_centos_rpms} ####

Test the produced CentOS RPMs.

#### Ubuntu DEBs {#label_ubuntu_debs} ####

Test the produced Ubuntu DEBs.

#### AMI {#label_ami} ####

Test the produced AMI image.

#### Docker {#label_docker} ####

Test the produced Docker image.

## Administration {#label_administration} ###

Tests operations that Database Administrators would perform on a scylla DB.

### Adding nodes to an existing cluster {#label_add_nodes_cluster} ###

Test that adding nodes to an existing cluster works.

### Adding a data center to a cluster {#label_add_data_center_cluster} ###

Test that adding a data center to a cluster works.

### Replacing a dead node {#label_replace_dead_node} ###

Test that it's possible to replace a dead node.

### Replacing a dead seed node {#label_replace_dead_seed_node} ###

Test that it's possible to replace a dead seed node.

### Replacing a running node {#label_replace_running_node} ###

Test that it's possible to replace a running node.

### Decommissioning a data center {#label_decommission_data_center} ###

Test that it's possible to decommission a data center.

### Removing a node {#label_removing_node} ###

Test removing a node from a cluster.

### Reduce the size of a data center {#label_reduce_size_dc} ###

Test reducing the size of a data center.

### Switching snitches {#label_switching_snitches} ###

Test switching the snitches.

### Transitioning or migrating a cluster {#label_migrating_cluster} ###

Test cluster migration/transitioning.

### Snapshot (backup) / Restore {#label_snapshot_backup_restore} ###

Test cases

  * Base test cases ./snapshot_test.py
  * Advanced tests cases (covering schema updates, failures etc.) snapshot_restore_additional_test.SnapshotRestoreAdditionalTest
  * Testing of nodetool snapshot / clearsnapshot commands nodetool_additional_test.TestNodetool.global_create_after_clean nodetool_additional_test.TestNodetool.global_snapshot_test

### Repair {#label_other_repair_functions} ###

Test cases
  * Base test cases repair_tesy.py
  * Advanced test cases : repair_additional_test.RepairAdditionalTest
  * Test nodetool options : FIXME

### Compaction strategy {#label_compaction_strategy} ###

Test the available, implemented scylla compaction strategy.

#### SizedTiered {#label_sized_tiered} ####

Test size tiered compaction.

#### LeveledTiered {#label_leveled_tiered} ####

Test leveled tiered compaction.

#### DateTiered {#label_date_tiered} ####

Test date tiered compaction.

### Rolling upgrades {#label_rolling_upgrades} ###

Test rolling upgrades.

### JMX proxy failures {#label_jmx_proxy_failures} ###

Verify how a scylla cluster behaves when only the JMX service fails.
What happens with the pending operations?

### Booting with different number of shards {#label_booting_df_nm_shards} ###

Boot a scylla cluster, then stop it and re-start, now with a different number
of shards.

### Recovery tests {#label_recovery_tests} ###

Test recovery functions.

#### Kill scylla-server process and restore {#label_kill_server_restore} ####

Kill scylla-server processes and restore the service afterwards.

#### Poweroff machine {#label_poweroff_machine} ####

Power off the bare metal machine where the scylla services lie and restore.

### Migration from Cassandra {#label_migration_from_cassandra} ###

Test migration from a Cassandra DB.

#### Boot from existing Cassandra sstable directory {#label_b_sstable_dir} ####

Populate a cassandra database, shut it down, then start scylla, using the
same sstable directory. Verify results.

#### Restore from a Cassandra snapshot {#label_restore_from_cassandra_snap} ####

Populate a Cassandra database, take a snapshot, then restore it using scylla.

Tests that need to be moved to test files

- Check migration of all format of sstables (compressed and non compressed with and without compact storage)
- Check migration of all format of compaction strategies - do we adhere to the strategy after migration (e.g. Leveled will we be able to use the level info from cassandra, date tiered can we use the files from origin)
- Check migration of wide row tables
- Check migration of all data types (collections, frozen, static etc).
- Check migration of all data metadata (ttl, cell tombstone, row tombstone, range_tombstone)
- Clock skew at migration (migration of data with future timestamps)
- Check migration of schemas with items we do not support (e.g. counters, secondary indexes)

- Do we want to check backport from Scylla to Cassandra as well ?

Distributed {#label_distributed}
------------------------------------

Scylla is a distributed database system [3], and so, it's important that we
verify its reliability under a number of adverse circumstances.

  [3]: https://en.wikipedia.org/wiki/Distributed_computing

### Schema Management {#label_schema_management} ###

Distributed schema management 

  * Server to client side notification tests - pushed_notifications_test.TestPushedNotifications
  * Schema management under cluster topology changes and failure tests - schema_management_test.SchemaManagementTest

### Snitches {#label_snitches} ###

Test all snitch possibilities.

### Gossip Protocol {#label_gossip_protocol} ###

Gossip is a peer-to-peer communication protocol in which nodes periodically
exchange state information about themselves and about other nodes they know
about [6]. This item relates to how to test this protocol in real, living
scylla cluster nodes.

  [6]: https://docs.datastax.com/en/cassandra/2.0/cassandra/architecture/architectureGossipAbout_c.html

### Consistency tests ###

Test how scylla behaves when adverse conditions disturb nodes in terms of data
consistency.

* consistency_test - functional tests for distributed related features (concurrency, consistency level, read_repair, etc.)

#### Read Repair ####


### Upgrade ###

Upgrade of scylla between minor / major versions in case of different cases

- Upgrade without a change in protocols / data serialization (bug fix)
- Upgrade with a change (mutations, query_result, schema, gossip info, streaming, repair protocol, sstable format, commitlog format, conf file, sharding, cql binary, cql protocol, messaging service additional method, rpc protocol)
- Rollback for any change case

Stability {#label_stability}
----------------------------

Abillity to handle different error cases and failures and continue to function

### Single node tests {#label_single_node_tests} ###

Tests that need to be moved to test files

- Operation Errors (Network Errors, Disk Errors)
   - Disk error are critical, we need to validate there is no data corruptions for the following operations:
     - write commit log (we already seen that)
     - sstable flush
     - repair
     - compaction
     - restore/backup
     - write hinted hand off (once we have it)
     - log file
   - Disk error can be:
     - out of space
     - out of bandwidth
     - disk IO report (write fail)
     - gracfull shutdown
     - process kill from exception
     - process kill (with kill -9)
     - hard shutdown (power off)

- Disk Bandwidth (with compression/without compression)- Write workload (Write+Read)
   - Large batch statement processing
   - Under changed disk performance (e.g. when the disk is stressed by other users irregulalrly)
- Disk Bandwidth (with compression/without compression)- Read workload
   - working set not in memory
   - working set partially in memory
   - items that have been updated in multiple sstables files
   - Under changed disk performance (e.g. when the disk is stressed by other users irregulalrly)
- Wide rows handling (common for many users / time series etc).
- Very large results set performance impact (very large wide row / multiple rows)
- Compaction Bandwidth (large disk size, is compaction limitted), compaction falling behind for multiple cases
- Multiple compactions from nodetool
- Burst testing on different load setting (including idle)
- Repair bandwidth check while nodes/cluster is stressed
- New node streaming while cluster is stressed
- Alter Table while cluster is stressed
- Memory pressure (huge, large and small objects)

### Cluster tests {#label_cluster_tests} ###

Tests that need to moved to test files

- Cluster bandwidth - Write workload
  - Under failed node when CL can be met and cannot be met - are we able to reach normal performance under error case
  - Large batch statement processing
- Cluster bandwidth - Read workload
  - Under failed node when CL can be met and cannot be met - are we able to reach normal peroformance under error case
  - Very large results set performance impact (very large wide row / multiple rows) - case of pulling a lot of info for additional processing
- Burst testing on different load setting (including idle) / admin operations (add / decomission / repair etc.)
- Non stable network (disconnects)
- Slow Node
- Clock Skew, multi-dc cross timezone
- handling of daylight saving time update
- Network bandwidth issues , cross dc network bandiwdth/latency issues (regular workload, management operations)
- Resource leak issues on errors (network error - failed/killed remote node, failed/killed client)


Performance {#label_performance}
------------------------------------

Scylla aims to be a high perorming database system, so it's imperative that
it can serve requests at certain levels of throughput.

### Read throughput {#label_read_througput} ###

Test the performance of reading from a scylla node.

### Write throughput {#label_write_througput} ###

Test the performance of reading to a scylla node.

### Latency {#label_latency} ###

Test the latency (round trip time required to complete a request) for a scylla
node.

SSL Performance {#label_ssl_performance}
------------------------------------
Running with client node and node to node encryption (SSL) will affect throughput and performance. Each of the performance tests should be repeated with SSL to test its effect.

### Read throughput with client to node SSL {#label_c2n_ssl_read_througput} ###

Test the performance of reading from a scylla node with client to node encryption

### Write throughput {#label_c2n_ssl_write_througput} ###

Test the performance of reading to a scylla node with client to node encryption

### Latency {#label_c2n_ssl_latency} ###

Test the latency (round trip time required to complete a request) for a scylla
node with client to node encryption

### Read throughput with client to node SSL {#label_n2n_ssl_read_througput} ###

Test the performance of reading from a scylla node with node to node encryption

### Write throughput {#label_n2n_ssl_write_througput} ###

Test the performance of reading to a scylla node with node to node encryption

### Latency {#label_n2n_ssl_latency} ###

Test the latency (round trip time required to complete a request) for a scylla
node with node to node encryption

Scalability tests {#label_scalability}
--------------------------------------

### Maximum number of partitions {#label_max_part} ###

Test the maximum number of partitions a scylla cluster can support.

### Maximum number of rows {#label_max_rows} ###

Test the maximum number of rows a scylla table can hold.

### Maximum number of columns {#label_max_columns} ###

Test the maximum number of columns a scylla table can hold.
In Cassandra, maximum number of cells (rows x columns) in a single partition is (2 billion)[https://wiki.apache.org/cassandra/CassandraLimitations]
A 10K is a huge number in practice.

### Maximum number of  fields in a tuple {#label_max_fields_in_tuple} ###
Test fields in a tuple can get to [32768](http://docs.datastax.com/en/cql/3.1/cql/cql_reference/refLimits.html)

### Maximum key length {#label_max_key_length} ###
Test key length can reach 65535

### Maximum Query parameters in a query  {#label_query_parameters_in_query} ###
Test query parameters can reach 65535

### Maximum statements in a batch  {#label_statements_in_batch} ###
Test statements in a batch can reach 65535

Single column, value of: 2GB, xMB are recommended

### Maximum blob size {#label_max_blob_size} ###

Test max size of a blob (in MB) which allow scylla to run smoothly

### Maximum number of nodes {#label_max_nodes} ###

Test the maximum number of nodes a scylla cluster can support.

### Maximum number of connections {#label_max_connections} ###

Test the maximum number of concurrent connections
[#674](https://github.com/scylladb/scylla/issues/674) is an issue this test should expose.

### Largest data volume {#label_max_data_volume} ###

Test the largest data volume a scylla DB can hold, in term of TB per node.
10TB per node should normal, 100TB per node possible.
Testing should cover compaction, repair and backups, all are affected by volume.

Stress tests {#label_stress}
----------------------------

### Memory {#label_stress_memory} ###

Test scylla behavior under different system memory conditions.

#### Memory pressure {#label_stress_memory_pressure} ####

Replicate a low menory condition, nearing the OOM killer, and see how scylla
behaves.

#### Mix of small and huge objects {#label_mem_small_and_huge_objs} ####

Create small objects and large blobs, and perform operations in it and see how
scylla behaves.

### Disks {#label_stress_disk} ###

Test a high amount of reads workload, from cache or out of cache.

#### Random access vs local access {#label_stress_random_access} ####

Test disk random access and local access.

#### Scan resistance {#label_stress_scan_resistance} ####

Scan resistence is a cache property of caches to not drop frequently accessed
objects in face of large sequential data reads. This will be tested when scylla
implements a scan resistant cache.

Longevity tests {#label_Longevity}
----------------------------------

Ensure that the Scylla database and associated tools can function over long period of times.

### Scylla Longevity {#label_scylla_longevity} ###
Run scylla for 24h / 7d / 1 month

* on EC2 /  bare metal
* With EC2 zones / regions
* With chaos monkey: kill a server every 30 min*
* With chaos kong: kill DC every  hour *
* With hourly repairs
* With the following operations been done in the background some in parallel depdening on RF/Cluster size:
    * A server is killed and started
    * A server is drained stopped and started
    * A server is added - wait till it finished
    * A server is added and is being killed in the process
    * A server is being decomissioned - wait till it finished
    * A server is being decomissioned and is being killed in the process, restart node and repair
    * A server is killed - part of the data is removed and started (repair is run - wait till it ends)
    * A server is killed - part of the data is removed and started - rebuilt is run
cassandra-stress read / cassandra-stress write is run in parallel with CL=Quorum
A single thread in a loop that writes and reads known data allways growing is executed with CL=Quorum
 With RF=5 2 operations in parallel
 With RF=3 1 operation in parallel
(need to think on how we handle cassandra-stress, write/read issues that can happen because of a failing node processing request).

### scylla-jmx longevity {#label_scylla-jmx-longevity} ###

Longevity test to scylla-jmx validating it does not have memory leak
Test should include stressing the JMX.
