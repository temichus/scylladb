#!groovy
import groovy.transform.Field

@Field String buildMetadataFile = '00-Build.txt'
@Field String targetDtestBuilder = 'ec2-4cpu-dtest-asg-spot-releng'
@Field String targetDtestStrongBuilder = 'ec2-strong-dtest-asg-spot-releng'

@Field String armTargetDtestBuilder = 'ec2-4cpu-dtest-asg-arm-spot-releng'
@Field String armTargetDtestStrongBuilder = 'ec2-strong-dtest-asg-arm-spot-releng'

@Field String smpNumber = "2"

@Field String x86MetadataFile= "metadata_x86_64.txt"
@Field String armMetadataFile= "metadata_aarch64.txt"
@Field String x86ArchName= "x86_64"
@Field String armArchName= "aarch64"

@Field String jobSummaryFile = "job-summary-results.properties"
