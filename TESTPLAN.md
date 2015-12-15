Scylla Test Plan {#mainpage}
============================

[TOC]

This is the Scylla Test Plan document. It covers the tests executed to ensure
the quality of the Database Services and associated tools, in terms of
functionality, administration tasks, distributed, performance,
scalability and behavior under stress.

Many of the tests are implmented as a Scylla Distributed tests (that's why
the test plan is located in the dtests repository), but there are some tests
that are executed in other test suites. When that is the case, the test will
be appropriately linked here.

Functional
----------
Functional {#label_functional}
------------------------------

Ensure that the Scylla database and associated tools perform their functions
adequately.

### CQL ###
### CQL {#label_cql} ###

Scylla implements the Cassandra query language interface (CQL) [1], a
language used to retrieve data from the database through SQL [2] style
queries. The CQL tests aim to verify that CQL statements return appropriate
values given a set of inputs, and that the Scylla clusters behave well while
serving the CQL requests.

CQL tests are implemented as dtests, and java unittests. See:

* cql_tests.py
* cql_prepared_test.py
* cql_additional_tests.py
* scylla_unsupported_test.py

  [1]: https://cassandra.apache.org/doc/cql3/CQL.html
  [2]: https://en.wikipedia.org/wiki/SQL

### SSTables ###
### SSTables {#label_sstables} ###

SSTables are the files that Scylla uses to actually store data in the disk.
There are developer level tests to verify integrity and behavior of such
files.

### Companion Tools ###
### Companion Tools {#label_companion_tools} ###

Tests the scylla companion tools.

#### nodetool ####
#### nodetool {#label_nodetool} ####

nodetool should accept only supported commands and supported params
nodetool should provide help only to supported commands and supported params

Nodetool is a tool to control and manage scylla nodes. The subcommands to test
are:

##### move {#label_nodetool_move} #####

Test nodetool command `move`.

##### cfhistograms {#label_nodetool_cfhistograms} #####

Test nodetool command `cfhistograms`.

##### cfstats {#label_nodetool_cfhistograms} #####

Test nodetool command `cfstats`.

##### clearsnapshot {#label_nodetool_cfhistograms} #####

Test nodetool command `clearsnapshot`.

##### compact {#label_nodetool_compact} #####

Test nodetool command `compact`.

##### decommission {#label_nodetool_decommission} #####

Test nodetool command `decommission`.

##### describecluster {#label_nodetool_describecluster} #####

Test nodetool command `describecluster`.

##### describering {#label_nodetool_describering} #####

Test nodetool command `describering`.

##### disablegossip {#label_nodetool_disablegossip} #####

Test nodetool command `disablegossip`.

##### enablegossip {#label_nodetool_enablegossip} #####

Test nodetool command `enablegossip`.

##### flush {#label_nodetool_flush} #####

Test nodetool command `flush`.

##### getendpoints {#label_nodetool_getendpoints} #####

Test nodetool command `getendpoints`.

##### gossipinfo {#label_nodetool_gossipinfo} #####

Test nodetool command `gossipinfo`.

##### info {#label_nodetool_info} #####

Test nodetool command `info`.

##### netstats {#label_nodetool_netstats} #####

Test nodetool command `netstats`.

##### refresh {#label_nodetool_refresh} #####

Test nodetool command `refresh`.

##### repair {#label_nodetool_repair} #####

Test nodetool command `repair`.

##### ring {#label_nodetool_ring} #####

Test nodetool command `ring`.

##### status {#label_nodetool_status} #####

Test nodetool command `status`.

##### tpstats {#label_nodetool_tpstats} #####

Test nodetool command `tpstats`.

##### cleanup {#label_nodetool_cleanup} #####

Test nodetool command `cleanup`.

##### compactionhistory {#label_nodetool_compactionhistory} #####

Test nodetool command `compactionhistory`.

##### compactionstats {#label_nodetool_compactionstats} #####

Test nodetool command `compactionstats`.

##### drain {#label_nodetool_drain} #####

Test nodetool command `drain`.

##### rebuild {#label_nodetool_rebuild} #####

Test nodetool command `rebuild`.

##### removenode {#label_nodetool_removenode} #####

Test nodetool command `removenode`.

#### cassandra-stress ####
#### cassandra-stress {#label_cassandra_stress} ####

The tool `cassandra-stress` executes read and write operations against a
scylla cluster. It is used to verify scylla node operation throughput.

#### cqlsh ####
#### cqlsh {#label_cqlsh} ####

The tool `cqlsh` is an interface to perform CQL queries against a scylla
database. We aim to verify if the tool is working according to the given
user scenarios.

### Installable packages and images ###
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

#### Fedora RPMs ####
#### Fedora RPMs {#label_fedora_rpms} ####

Test the produced Fedora RPMs.

#### CentOS RPMs ####
#### CentOS RPMs {#label_centos_rpms} ####

Test the produced CentOS RPMs.

#### Ubuntu DEBs ####
#### Ubuntu DEBs {#label_ubuntu_debs} ####

Test the produced Ubuntu DEBs.

#### AMI ####
#### AMI {#label_ami} ####

Test the produced AMI image.

#### Docker ####
#### Docker {#label_docker} ####

Test the produced Docker image.

## Administration ###
## Administration {#label_administration} ###

Tests operations that Database Administrators would perform on a scylla DB.

### Adding nodes to an existing cluster ###
### Adding nodes to an existing cluster {#label_add_nodes_cluster} ###

Test that adding nodes to an existing cluster works.

### Adding a data center to a cluster ###
### Adding a data center to a cluster {#label_add_data_center_cluster} ###

Test that adding a data center to a cluster works.

### Replacing a dead node ###
### Replacing a dead node {#label_replace_dead_node} ###

Test that it's possible to replace a dead node.

### Replacing a dead seed node ###
### Replacing a dead seed node {#label_replace_dead_seed_node} ###

Test that it's possible to replace a dead seed node.

### Replacing a running node ###
### Replacing a running node {#label_replace_running_node} ###

Test that it's possible to replace a running node.

### Decommissioning a data center ###
### Decommissioning a data center {#label_decommission_data_center} ###

Test that it's possible to decommission a data center.

### Removing a node ###
### Removing a node {#label_removing_node} ###

Test removing a node from a cluster.

### Reduce the size of a data center ###
### Reduce the size of a data center {#label_reduce_size_dc} ###

Test reducing the size of a data center.

### Switching snitches ###
### Switching snitches {#label_switching_snitches} ###

Test switching the snitches.

### Transitioning or migrating a cluster ###
### Transitioning or migrating a cluster {#label_migrating_cluster} ###

Test cluster migration/transitioning.

### Snapshot (backup) ###
### Snapshot (backup) {#label_snapshot_backup} ###

Test taking a cluster snapshot or backup.

### Incremental Backups ###
### Incremental Backups {#label_incremental_backups} ###

Test taking incremental backups from a cluster.

### Restore ###
### Restore {#label_restore} ###

Test restoring a backup from a cluster.

### Deleting snapshot files ###
### Deleting snapshot files {#delete_snapshot} ###

Test deleting snapshot files.

### Repair ###
### Repair {#label_other_repair_functions} ###

Test other repair functions.

#### Full repair (new node) ####
#### Full repair (new node) {#label_full_repair_new_node} ####

Test a full repair on a new node.

#### Repair on 2 relatively similar nodes ####
#### Repair on 2 relatively similar nodes {#label_full_repair_similar_nodes} ####

Test repair on 2 nodes that are relatively similar (Merkle tree).

### Compaction strategy ###
### Compaction strategy {#label_compaction_strategy} ###

Test the available, implemented scylla compaction strategy.

#### SizedTiered ####
#### SizedTiered {#label_sized_tiered} ####

Test size tiered compaction.

#### LeveledTiered ####
#### LeveledTiered {#label_leveled_tiered} ####

Test leveled tiered compaction.

#### DateTiered ####
#### DateTiered {#label_date_tiered} ####

Test date tiered compaction.

### Rolling upgrades ###
### Rolling upgrades {#label_rolling_upgrades} ###

Test rolling upgrades.

### JMX proxy failures ###
### JMX proxy failures {#label_jmx_proxy_failures} ###

Verify how a scylla cluster behaves when only the JMX service fails.
What happens with the pending operations?

### Booting with different number of shards ###
### Booting with different number of shards {#label_booting_df_nm_shards} ###

Boot a scylla cluster, then stop it and re-start, now with a different number
of shards.

### Recovery tests ###
### Recovery tests {#label_recovery_tests} ###

Test recovery functions.

#### Kill scylla-server process and restore ####
#### Kill scylla-server process and restore {#label_kill_server_restore} ####

Kill scylla-server processes and restore the service afterwards.

#### Poweroff machine ####
#### Poweroff machine {#label_poweroff_machine} ####

Power off the bare metal machine where the scylla services lie and restore.

### Migration from Cassandra ###
### Migration from Cassandra {#label_migration_from_cassandra} ###

Test migration from a Cassandra DB.

#### Boot from existing Cassandra sstable directory ####
#### Boot from existing Cassandra sstable directory {#label_b_sstable_dir} ####

Populate a cassandra database, shut it down, then start scylla, using the
same sstable directory. Verify results.

#### Restore from a Cassandra snapshot ####
#### Restore from a Cassandra snapshot {#label_restore_from_cassandra_snap} ####

Populate a Cassandra database, take a snapshot, then restore it using scylla.

Distributed
-----------
Distributed {#label_distributed}
------------------------------------

Scylla is a distributed database system [3], and so, it's important that we
verify its reliability under a number of adverse circumstances.

  [3]: https://en.wikipedia.org/wiki/Distributed_computing

#### Schema Management ####

Distributed schema management 

  * pushed_notifications_test - tests for server to client side notifications
  * schema_management_test - tests for schema management under cluster topology 
changes and failures

### Snitches ###
### Snitches {#label_snitches} ###

Test all snitch possibilities.

### Gossip Protocol ###
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

Performance
-----------
Performance {#label_performance}
------------------------------------

Scylla aims to be a high perorming database system, so it's imperative that
it can serve requests at certain levels of throughput.

### Read throughput ###
### Read throughput {#label_read_througput} ###

Test the performance of reading from a scylla node.

### Write throughput ###
### Write throughput {#label_write_througput} ###

Test the performance of reading to a scylla node.

### Latency ###
### Latency {#label_latency} ###

Test the latency (round trip time required to complete a request) for a scylla
node.

Scalability tests
-----------------
Scalability tests {#label_scalability}
--------------------------------------

### Maximum number of partitions ###
### Maximum number of partitions {#label_max_part} ###

Test the maximum number of partitions a scylla cluster can support.

### Maximum number of rows ###
### Maximum number of rows {#label_max_rows} ###

Test the maximum number of rows a scylla table can hold.

### Maximum number of nodes ###
### Maximum number of nodes {#label_max_nodes} ###

Test the maximum number of nodes a scylla cluster can support.

### Largest data volume ###
### Largest data volume {#label_max_data_volume} ###

Test the largest data volume a scylla DB can hold.

Stress tests
------------
Stress tests {#label_stress}
----------------------------

### Memory ###
### Memory {#label_stress_memory} ###

Test scylla behavior under different system memory conditions.

#### Memory pressure ####
#### Memory pressure {#label_stress_memory_pressure} ####

Replicate a low menory condition, nearing the OOM killer, and see how scylla
behaves.

#### Mix of small and huge objects ####
#### Mix of small and huge objects {#label_mem_small_and_huge_objs} ####

Create small objects and large blobs, and perform operations in it and see how
scylla behaves.

### Disks ###
### Disks {#label_stress_disk} ###

Test a high amount of reads workload, from cache or out of cache.

#### Random access vs local access ####
#### Random access vs local access {#label_stress_random_access} ####

Test disk random access and local access.

#### Scan resistance ####
#### Scan resistance {#label_stress_scan_resistance} ####

Scan resistence is a cache property of caches to not drop frequently accessed
objects in face of large sequential data reads. This will be tested when scylla
implements a scan resistant cache.
