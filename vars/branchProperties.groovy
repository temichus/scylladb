#!groovy
import groovy.transform.Field

@Field String dtestStableBranchName = 'pytest'
@Field String ccmStableBranchName = 'master'

@Field String ccmDefaultRepo = 'git@github.com:scylladb/scylla-ccm.git'
@Field String dtestDefaultRepo = 'git@github.com:scylladb/scylla-dtest.git'
