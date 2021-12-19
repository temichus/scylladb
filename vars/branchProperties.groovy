#!groovy
import groovy.transform.Field

@Field String dtestStableBranchName = 'master'
@Field String ccmStableBranchName = 'master'
@Field String productName = 'scylla'

@Field String ccmDefaultRepo = 'git@github.com:scylladb/scylla-ccm.git'
@Field String dtestDefaultRepo = 'git@github.com:scylladb/scylla-dtest.git'
