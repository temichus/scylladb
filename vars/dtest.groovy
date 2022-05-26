#!groovy

def createEmptyDir(String path) {
	sh "rm -rf $path && mkdir -p $path"
}

String setDtestParams (Map args) {
	// Parameters:
	// boolean (default false): dryRun - Run builds on dry run (that will show commands instead of really run them).
	// boolean (default false): dtestDebugInfoFlag - Set to run with debug info
	// boolean (default false): dtestKeepLogsFlag - Set to keep logs
	// String (default null) excludeTests: List of tests to exclude.
	// String (default null) includeTests: List of tests to include.
	// String (default null) extOpts: Any set of external dtest options.
	// String (default null) extEnv: Any set of env settings
	// String (default null) randomDtests: Number of tested modes.
	// String (default null) randomDtestsSeed:
	// String (default null) dtestRepeats: How many times to repeat the dtests.
	// String (mandatory) dtestMode release|debug

	jenkins.traceFunctionParams ("test.setDtestParams", args)

	boolean dryRun = args.dryRun ?: false
	boolean dtestDebugInfoFlag = args.dtestDebugInfoFlag ?: false
	boolean dtestKeepLogsFlag = args.dtestKeepLogsFlag ?: false
	String excludeTests = args.excludeTests ?: ""
	String includeTests = args.includeTests ?: ""
	String extOpts = args.extOpts ?: ""
	String extEnv = args.extEnv ?: ""
	String randomDtests = args.randomDtests ?: ""
	String randomDtestsSeed = args.randomDtestsSeed ?: ""
	String dtestRepeats = args.dtestRepeats ?: "1"
    String dtestType = args.dtestType ?: "full"
    String managerPackage = args.managerPackage ?: ""

	String dtestParameters = "--home=${WORKSPACE}/${params.PRODUCT_NAME}"
	dtestParameters += " --mode=$args.dtestMode"
	dtestParameters += " --smp=$generalProperties.smpNumber"
	dtestParameters += " --exclude=\"$excludeTests\""
	dtestParameters += " --include=\"$includeTests\""
    dtestParameters += " --dtest-type=\"$dtestType\""

	if (dryRun) {
		dtestParameters = dtestParameters + " --dry_run"
	}
	if (dtestDebugInfoFlag) {
		dtestParameters = dtestParameters + " --debug"
	}

	if (dtestKeepLogsFlag) {
		dtestParameters = dtestParameters + " --keep_logs"
	}
	if (extOpts != "") {
		dtestParameters = dtestParameters + " --scylla_ext_opts=\"$extOpts\""
	}
	if (extEnv != "") {
		dtestParameters = dtestParameters + " --scylla_ext_env=\"$extEnv\""
	}

	if (randomDtests != "") {
		dtestParameters = dtestParameters + " --random=\"$randomDtests\""
		if (randomDtestsSeed != "") {
			dtestParameters = dtestParameters + " --random_seed=\"$randomDtestsSeed\""
		}
	}

	if (dtestRepeats != "1") {
		dtestParameters = dtestParameters + " --repeat=\"$dtestRepeats\""
	}

    if (managerPackage) {
        dtestParameters = dtestParameters + " --manager-package=\"$managerPackage\""
    }

	setupTestEnv(args.dtestMode)

	echo "dtestParameters: |$dtestParameters|"
	return "$dtestParameters"
}

def artifactScyllaVersion() {
	def versionFile = generalProperties.buildMetadataFile
	def scyllaSha = ""
	boolean versionFileExists = fileExists "${versionFile}"
	if (versionFileExists) {
		scyllaSha = sh(script: "awk '/scylladb\\/scylla(-enterprise)?\\.git/ { print \$NF }' ${generalProperties.buildMetadataFile}", returnStdout: true).trim()
	}
	echo "Version is: |$scyllaSha|"
	return scyllaSha
}

def setupTestEnv(String buildMode, String architecture="", boolean dryRun=false) {
	// This override of HOME as an empty dir is needed by ccm
	echo "Setting test environment, mode: |$buildMode|"
	// First look for local built package
	String scyllaPackageName = artifact.relocPackageName (
		dryRun: dryRun,
		checkLocal: true,
		mustExist: false,
		urlOrPath: "$WORKSPACE/${params.PRODUCT_NAME}/build/$buildMode/dist/tar",
		packagePrefix: params.PRODUCT_NAME,
		buildMode: buildMode,
		architecture: architecture,
	)
	// If not found - look where artifacts are downloaded
	if (! scyllaPackageName) {
		scyllaPackageName = artifact.relocPackageName (
			dryRun: dryRun,
			checkLocal: true,
			mustExist: true,
			urlOrPath: WORKSPACE,
			packagePrefix: params.PRODUCT_NAME,
			buildMode: buildMode,
			architecture: architecture,
		)
	}
    String scyllaRelocPkgFile = "$WORKSPACE/${params.PRODUCT_NAME}/build/$buildMode/dist/tar/${scyllaPackageName}"

	boolean pkgFileExists = fileExists scyllaRelocPkgFile
	if (pkgFileExists) {
		echo "Reloc pkg file exists. Setting testing env vars."
		env.SCYLLA_VERSION = artifactScyllaVersion()
		env.SCYLLA_CORE_PACKAGE_NAME = scyllaPackageName
		env.SCYLLA_CORE_PACKAGE = scyllaRelocPkgFile
		env.SCYLLA_JAVA_TOOLS_PACKAGE = "$WORKSPACE/${params.PRODUCT_NAME}/build/$buildMode/dist/tar/${params.PRODUCT_NAME}-tools-package.tar.gz"
		env.SCYLLA_JMX_PACKAGE = "$WORKSPACE/${params.PRODUCT_NAME}/build/$buildMode/dist/tar/${params.PRODUCT_NAME}-jmx-package.tar.gz"
		env.CASSANDRA_DIR = "$WORKSPACE/${params.PRODUCT_NAME}/build/$buildMode"
	} else {
		echo "Reloc pkg file does not exist. Skipping set testing env vars."
	}
}

def prepareDtestLocalTree (Map args) {
	// Checkout and Get artifacts needed to run dtest
	//
	// boolean (default false): preserveWorkspace -
	// String (default: branchProperties.stableBranchName): scyllaBranch - scylla branch
	// String (default: branchProperties.stableBranchName): dtestBranch - dtest branch
	// String (default: branchProperties.stableBranchName): ccmBranch - ccm branch
	//
	// string (default null): relocWebUrl - From where to get the artifact (when you run on a cloud machine)
	// String (default none): baseRelocJob - Name of job to get artifacts from, on local machine
	// String (default null): relocBuildID - From where to get the artifact (when you run on a local machine)
	// String (default: release) buildMode - release|debug

	jenkins.traceFunctionParams ("dtest.prepareDtestLocalTree", args)

	boolean preserveWorkspace = args.preserveWorkspace ?: false
	String dtestBranch = args.dtestBranch ?: "${GIT_BRANCH}".split('/')[1]
	String dtestRepo = args.dtestRepo ?: "${GIT_URL}"
	String ccmBranch = args.ccmBranch ?: branchProperties.ccmStableBranchName
	String ccmRepo = args.ccmRepo ?: branchProperties.ccmDefaultRepo
	String relocWebUrl = args.relocWebUrl ?: "latest"
	String buildMode = args.buildMode ?: "release"

	jenkins.cleanWorkSpaceUponRequest(preserveWorkspace)
    dir('scylla-dtest') {
        timeout(time: 5, unit: 'MINUTES') {
             git(url: dtestRepo,
                credentialsId: 'github-promoter',
                branch: dtestBranch)
        }
    }
    dir("scylla-ccm") {
        timeout(time: 5, unit: 'MINUTES') {
            git(url: ccmRepo,
                credentialsId: 'github-promoter',
                branch: ccmBranch)
        }
    }
	artifact.getRelocArtifacts(relocWebUrl, buildMode)

	echo "dtest will run based on relocatable package. Info: ============="
	sh "cat ${generalProperties.buildMetadataFile}"
	echo "============================"

	setupTestEnv(buildMode)
}

def splitAndCopyDtestJobs (Map args) {
	// Parameters:
	// Integer (defaults: 45): splitTimeTarget -Time period (minutes) for a test group to run. Used to calculate the needed number of spot machines
	// Integer (defaults: 55): splitMaxNodes - Maximum number of nodes we can use for the job in parallel
	// Integer (defaults: 120): defaultTestTimeSec - the default time test would be consider if not found history for (only supported in pytest)
	// String (default: release): buildMode release|debug
    // String (default: None): includeTests
    // String (default: None): excludeTests

	jenkins.traceFunctionParams ("test.splitAndCopyDtestJobs(Split test based on jenkins job history)", args)

	int splitTimeTarget = args.splitTimeTarget as Integer ?: 45
	int splitMaxNodes = args.splitMaxNodes as Integer ?: 55
	int defaultTestTimeSec = args.defaultTestTimeSec as Integer ?: 120
	String buildMode = args.buildMode ?: "release"
	String includeTests = args.includeTests
	String excludeTests = args.excludeTests
    String dtestType = args.dtestType ?: "full"

    dir ("$WORKSPACE/scylla-dtest") {
        sh "env"
        def status = sh(script: "./scripts/run_test.sh ${includeTests} ${excludeTests} --scylla-version=${env.SCYLLA_VERSION} --collect-only -q --es-slices --es-max-splice-time=${splitTimeTarget} --es-default-test-time=${defaultTestTimeSec}", returnStatus: true)
        // TODO: stop ignoring status once all test are collectable
        echo "$status, ignoring it for now"
    }
    int numOfSplitFiles = sh(returnStdout: true, script: "ls $WORKSPACE/scylla-dtest/include_* -1 | wc -l") as Integer

    echo "Number Of Split Files:$numOfSplitFiles"
    stash(name: "${dtestType}-dtest-split-files", includes:"scylla-dtest/include_*", useDefaultExcludes: false)
    if (splitMaxNodes < numOfSplitFiles) {
        error("Build failed because splitMaxNodes(${splitMaxNodes}) < numOfSplitFiles(${numOfSplitFiles})")
    }
    return numOfSplitFiles
}

boolean publishTestResults (String testsWildcardFiles, String baseDir) {
	boolean dirExists = fileExists "$baseDir"
	if (!dirExists) {
		echo "Error: No xunit / junit to publish (no dir |$baseDir|)"
		return true
	}
	boolean status = false
	dir(baseDir) {
        echo "Going to publish junit files: $testsWildcardFiles"
        try {
            junit testsWildcardFiles
        } catch (error) {
            echo "Error: Could not publish junit files: |$testsWildcardFiles|. Error: |$error|"
            status = true
        }
	}
	return status
}

def doParallelDtest (Map args) {

	// Parameters:
	// boolean (default false): dryRun - Run builds on dry run (that will show commands instead of really run them).
	// boolean (default false): dtestDebugInfoFlag - Set to run with debug info
	// boolean (default false): dtestKeepLogsFlag - Set to keep logs
	// String (default null) extOpts: Any set of external dtest options.
	// String (default null) extEnv: Any set of env settings
	// String (mandatory) dtestMode release|debug
	// String (default latest): cloudUrl - From where to download the artifacts.
	// String (mandatory): runningUserID - show which user triggered the job
	// string (default git@github.com:scylladb/scylla-pkg.git): relengRepo
	// String (default stableBranch): relengBranch
	// String (default stableBranch): ccmBranch
	// String (default stableBranch): dtestBranch

	boolean dryRun = args.dryRun ?: false
	boolean dtestDebugInfoFlag = args.dtestDebugInfoFlag ?: false
	boolean dtestKeepLogsFlag = args.dtestKeepLogsFlag ?: false
	String excludeTests = args.excludeTests ?: ""
	String extOpts = args.extOpts ?: ""
	String extEnv = args.extEnv ?: ""
	String cloudUrl = args.cloudUrl ?: "latest"
    String dtestType = args.dtestType ?: "full"
    String managerPackage = args.managerPackage ?: ""

	def branches = [:]
	def runnersLabel =  args.splitFleetLabal ?: generalProperties.targetDtestStrongBuilder
	boolean dtestFailed = false
	boolean publishFailed = false
	int numOfSplitFiles = args.numOfSplitFiles

    for (int i = 0; i < numOfSplitFiles; i++) {

        String nodeIndex = i
        nodeIndex = nodeIndex.padLeft(3, '0')
        branches["${dtestType}-split${nodeIndex}"] = {
            node(runnersLabel) {
                jenkins.checkAndTagAwsInstance(args.runningUserID)
                withEnv(["NODE_TOTAL=${numOfSplitFiles}", "NODE_INDEX=${nodeIndex}"]) {
                    prepareDtestLocalTree (
                        preserveWorkspace: false,
                        dtestRepo: args.dtestRepo,
                        ccmRepo: args.ccmRepo,
                        dtestBranch: args.dtestBranch,
                        ccmBranch: args.ccmBranch,
                        relocWebUrl: cloudUrl,
                        buildMode: args.dtestMode,
                    )

                    def currentWorkSpace = sh(returnStdout: true, script: 'echo $WORKSPACE').trim()
                    def instanceType = sh(returnStdout: true, script: "curl http://169.254.169.254/latest/meta-data/instance-type").trim()
                    echo "instanceType: ${instanceType}"

                    // HACK: avoid getting Argument list too long
                    sh 'ulimit -s 65536'

                    unstash(name: "${dtestType}-dtest-split-files")
                    setupTestEnv(args.dtestMode)
                    String splitFileName = "$WORKSPACE/scylla-dtest/include_${NODE_INDEX}.txt"
                    if (! fileExists(splitFileName)) {
                        error("split file missing - ${splitFileName}")
                    }
                    String localIncludeTests = sh(returnStdout: true, script: " cat ${splitFileName} | tr '\n' ' '").trim()
                    String dtestRunTestSh = "$WORKSPACE/scylla-dtest/scripts/run_test.sh"
                    String dtestParameters = setDtestParams (
                        dryRun: dryRun,
                        dtestMode: args.dtestMode,
                        dtestDebugInfoFlag: dtestDebugInfoFlag,
                        dtestKeepLogsFlag: dtestKeepLogsFlag,
                        includeTests: localIncludeTests,
                        extOpts: extOpts,
                        extEnv: extEnv,
                        dtestType: dtestType,
                        managerPackage: managerPackage)

                    String dtestScript = "$WORKSPACE/scylla-dtest/scripts/pytest_dtest.sh"
                    echo "dtestParameters: |${dtestParameters}|"
                    try {
                        sh "set -o pipefail; ${dtestScript} ${dtestParameters} 2>&1 | tee output_${dtestType}_dtest_${NODE_INDEX}.txt"
                    } catch (org.jenkinsci.plugins.workflow.steps.FlowInterruptedException interruptEx) {
                        currentBuild.result = 'ABORTED'
                        error("Interrupt exception (abort) while dtest phase, error: |$interruptEx|")
                    } catch (Exception error) {
                        if (currentBuild.currentResult == 'ABORTED') {
                            error("Interrupt exception (abort) while dtest phase, error: |$error|")
                        }
                        else {
                            echo "Error: dtest phase failed, going ahead to check tests and dtest status, error: |$error|"
                            currentBuild.result = 'FAILURE'
                        }
                    }
                    finally {
                        if (!dryRun) {
                            publishFailed |= artifact.publishArtifactsStatus("scylla-dtest.${dtestType}.${args.dtestMode}.${NODE_INDEX}*.xml", WORKSPACE)
                            publishFailed |= artifact.publishArtifactsStatus("**/logs-${dtestType}.${args.dtestMode}.${NODE_INDEX}/**", 'scylla-dtest')
                            publishFailed |= publishTestResults("scylla-dtest.${dtestType}.${args.dtestMode}.${NODE_INDEX}*.xml", WORKSPACE)
                        }
                    }
                }
            }
        }
    }
    parallel branches

    Boolean failedStatus = currentBuild.result == 'FAILURE' || currentBuild.result == 'ABORTED'
    jenkins.raiseErrorOnFailureStatus (failedStatus, "dtest phase failed")
}


def doDtest (Map args) {
	// Parameters:
	// boolean (default false): dryRun - Run builds on dry run (that will show commands instead of really run them).
	// boolean (default false): dtestDebugInfoFlag - Set to run with debug info
	// boolean (default false): dtestKeepLogsFlag - Set to keep logs
	// String (default null) excludeTests: List of tests to exclude.
	// String (default null) includeTests: List of tests to include.
	// String (default null) extOpts: Any set of external dtest options.
	// String (default null) extEnv: Any set of env settings
	// String (default null) randomDtests: Number of tested modes.
	// String (default null) randomDtestsSeed:
	// String (default null) dtestRepeats: How many times to repeat the dtests.
	// String (mandatory) dtestMode release|debug
	jenkins.traceFunctionParams ("test.doDtest", args)

	boolean dryRun = args.dryRun ?: false
	boolean dtestDebugInfoFlag = args.dtestDebugInfoFlag ?: false
	boolean dtestKeepLogsFlag = args.dtestKeepLogsFlag ?: false
	String excludeTests = args.excludeTests ?: ""
	String includeTests = args.includeTests ?: ""
	String extOpts = args.extOpts ?: ""
	String extEnv = args.extEnv ?: ""
	String randomDtests = args.randomDtests ?: ""
	String randomDtestsSeed = args.randomDtestsSeed ?: ""
	String dtestRepeats = args.dtestRepeats ?: "1"
	String testRunner = args.testRunner ?: ""
	String architecture = args.architecture ?: ""
    String dtestType = args.dtestType ?: "full"
    String managerPackage = args.managerPackage ?: ""

	echo "Calling dtest in docker toolchain"
	String dtestScript = "$WORKSPACE/scylla-dtest/scripts/pytest_dtest.sh"
	String dtestParameters = setDtestParams (
			dryRun: dryRun,
			dtestMode: args.dtestMode,
			dtestDebugInfoFlag: dtestDebugInfoFlag,
			dtestKeepLogsFlag: dtestKeepLogsFlag,
			excludeTests: excludeTests,
			includeTests: includeTests,
			extOpts: extOpts,
			extEnv: extEnv,
			randomDtests: randomDtests,
			randomDtestsSeed: randomDtestsSeed,
			dtestRepeats: dtestRepeats,
			dtestType: args.dtestType,
			managerPackage: managerPackage
		)

	boolean dtestFailed = false
	boolean publishFailed = false
	boolean needToPublish = ( ! dryRun)

	env.NODE_INDEX = generalProperties.smpNumber

	try {
        sh "set -o pipefail; $dtestScript $dtestParameters 2>&1 | tee output_${dtestType}_dtest.txt"
	} catch (org.jenkinsci.plugins.workflow.steps.FlowInterruptedException interruptEx) {
		currentBuild.result = 'ABORTED'
		error("Interrupt exception (abort) while dtest phase, error: |$interruptEx|")
	} catch (error) {
		if (currentBuild.currentResult == 'ABORTED') {
			error("Interrupt exception (abort) while dtest phase, error: |$error|")
		} else {
			dtestFailed = true
			echo "Error: dtest phase failed, going ahead to check tests and dtest status, error: |$error|"
			currentBuild.result = 'FAILURE'
		}
	} finally {
		if (needToPublish) {
			publishFailed |= artifact.publishArtifactsStatus("scylla-dtest.${dtestType}.${args.dtestMode}.${NODE_INDEX}*.xml", WORKSPACE)
			publishFailed |= artifact.publishArtifactsStatus("**/logs-${dtestType}.${args.dtestMode}.${NODE_INDEX}/**/*", 'scylla-dtest')
			publishFailed |= publishTestResults("scylla-dtest.${dtestType}.${args.dtestMode}.${NODE_INDEX}*.xml", WORKSPACE)
		}
	}

	String errorDescription = "dtest phase"
	if (publishFailed) {
		errorDescription = "Failed to publish some dtest artifact(s)"
	}
	if (dtestFailed) {
		errorDescription = "dtest failed"
	}
	boolean failedStatus = dtestFailed || publishFailed

	jenkins.raiseErrorOnFailureStatus (failedStatus, errorDescription)
}
