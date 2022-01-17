#!groovy
import groovy.transform.Field

@Field String buildMetadataFile = '00-Build.txt'
@Field String targetDtestBuilder = 'ec2-fleet-4cpu-dtest'
@Field String targetDtestStrongBuilder = 'ec2-fleet-strong-dtest'

@Field String smpNumber = "2"

@Field String x86MetadataFile= "metadata_x86_64.txt"
@Field String armMetadataFile= "metadata_aarch64.txt"
@Field String x86ArchName= "x86_64"
@Field String armArchName= "aarch64"

@Field String jobSummaryFile = "job-summary-results.properties"
