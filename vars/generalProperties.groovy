#!groovy
import groovy.transform.Field

@Field String buildMetadataFile = '00-Build.txt'
@Field String targetDtestBuilder = 'dtest-fleet'
@Field String targetDtestStrongBuilder = 'ec2-fleet-next'

@Field String smpNumber = "2"
