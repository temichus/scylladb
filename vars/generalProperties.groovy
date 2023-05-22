#!groovy
import groovy.transform.Field

@Field String buildMetadataFile = '00-Build.txt'
@Field String targetDtestBuilder = 'ec2-fleet-4cpu-dtest-asg-spot-testing'
@Field String targetDtestStrongBuilder = 'ec2-asg-strong-dtest-spot-testing'

@Field String smpNumber = "2"

@Field String x86MetadataFile= "metadata_x86_64.txt"
@Field String armMetadataFile= "metadata_aarch64.txt"
@Field String x86ArchName= "x86_64"
@Field String armArchName= "aarch64"

@Field String jobSummaryFile = "job-summary-results.properties"
