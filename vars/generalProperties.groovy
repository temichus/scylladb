#!groovy
import groovy.transform.Field

@Field String buildMetadataFile = '00-Build.txt'
@Field String targetDtestBuilder = 'ec2-fleet-Sdtest2'
@Field String targetDtestStrongBuilder = 'ec2-fleet-Sdtest2'

@Field String smpNumber = "2"

@Field String x86MetadataFile= "metadata_x86_64.txt"
@Field String armMetadataFile= "metadata_aarch64.txt"
@Field String x86ArchName= "x86_64"
@Field String armArchName= "aarch64"
