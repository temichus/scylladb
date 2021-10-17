#!groovy

def call(Map pipelineParams) {
    pipeline {
        parameters {
            booleanParam(name: 'SKIP_DTEST_HEAVY', defaultValue: false, description: 'Check this to skip DtestHeavy, when running in parallel mode only!.')
            booleanParam(name: 'SKIP_DTEST_LONG', defaultValue: false, description: 'Check this to skip DtestLong, when running in parallel mode only!.')
            string(name: 'NODE_PARAM', defaultValue: 'scylla-dtest', description: 'Machine to run on. Useful options for parallel dtest: packager, or for local runs: monster, godzilla.')
            string(name: 'SPLIT_FLEET_LABEL', defaultValue: '', description: 'On which spot instance fleet to run the parallel jobs. default: ec2-fleet-Sdtest2')
            string(name: 'SPLIT_TIME_TARGET', defaultValue: '240', description: 'Time period (minutes) for a test group to run. Used to calculate the needed number of spot machines')
            string(name: 'SPLIT_MAX_NODES', defaultValue: '100', description: 'Maximum number of nodes to run tests on parallel.')
            string(name: 'BRANCH', defaultValue: "${pipelineParams.get('BRANCH', 'master')}", description: 'Choose: master|branch-4.4')
            string(name: 'PRODUCT_NAME', defaultValue: "${pipelineParams.get('PRODUCT_NAME', 'scylla')}", description: 'Choose: scylla|scylla-enterprise')

            //Not mandatory
            string(name: 'TIMEOUT_PARAM', defaultValue: '4', description: 'hours. This time includes the time needed to wait for local machines. Could be much less for cloud machines.')
            string(name: 'BUILD_MODE', defaultValue: "${pipelineParams.get('BUILD_MODE', 'release')}", description: 'Choose: dev|release|debug, If empty, default to release')
            string(name: 'RELOC_WEB_URL', defaultValue: 'latest', description: 'URL to take reloc items from. Use when reloc is not available on jenkins, or when running on AWS, which will download faster from S3.')
            booleanParam(name: 'DTEST_DEBUG_INFO', defaultValue: false, description: 'Check this to set env PRINT_DEBUG=true and DEBUG=true when running dtest')
            booleanParam(name: 'DTEST_KEEP_LOGS', defaultValue: false, description: 'Check this, to keep dtest logs (set KEEP_LOGS=true)')
            string(name: 'INCLUDE_DTESTS', defaultValue: '' , description: """Specify which dtests to run. default for release:
                                                                 -m '"dtest_full and not dtest_heavy and not dtest_long"',
                                                                 for debug: -m dtest_debug """)
            string(name: 'SCYLLA_EXT_OPTS_EXTRA_SETTINGS', defaultValue: '--abort-on-seastar-bad-alloc --abort-on-lsa-bad-alloc=1', description: 'Anything you put here will be added to any default settings of SCYLLA_EXT_OPTS env var sent to nose.')
            string(name: 'SCYLLA_EXT_ENV_EXTRA_SETTINGS', defaultValue: 'ASAN_OPTIONS=disable_coredump=0:abort_on_error=1;UBSAN_OPTIONS=halt_on_error=1:abort_on_error=1;BOOST_TEST_CATCH_SYSTEM_ERRORS=no', description: 'Anything you put here will be added to any default settings of SCYLLA_EXT_ENV env var sent to nose.')
            string(name: 'SCYLLA_DTEST_REPO', defaultValue: '', description: '')
            string(name: 'SCYLLA_DTEST_BRANCH', defaultValue: '', description: '')
            string(name: 'SCYLLA_CCM_REPO', defaultValue: '', description: '')
            string(name: 'SCYLLA_CCM_BRANCH', defaultValue: '', description: '')
            booleanParam(name: 'PRESERVE_WORKSPACE', defaultValue: false, description: 'Check this if you need the workspace to remain (for debug)')
            booleanParam(name: 'DRY_RUN', defaultValue: false, description: 'Check this to check pipeline syntax. will not perform anything.')
        }

        agent {
            label "aws-sct-builders-us-east-1"
        }

        options {
            disableConcurrentBuilds()
            timeout(time: params.TIMEOUT_PARAM, unit: 'HOURS')
            buildDiscarder(logRotator(numToKeepStr: '200'))
        }

        stages {
            stage ('Prepare') {
                steps {
                    script {
                        baseRelocJob = params.RELOC_JOB_NAME ?: "next"
                        buildMode = params.BUILD_MODE
                        excludeTests = params.EXCLUDE_DTESTS ?: ""
                        includeDtests = params.INCLUDE_DTESTS ?: ""

                        nodeParam = "packager"
                        splitMaxNodesForHeavyAndLong = "1"

                        echo "Build mode upon parameter |${params.BUILD_MODE}| or upon job name |${JOB_NAME}|: |${buildMode}|"
                    }
                }
            }

            stage('Run Dtest Parallel Cloud Machines') {
                environment {
                    AWS_ACCESS_KEY_ID     = credentials('jenkins2-aws-secret-key-id')
                    AWS_SECRET_ACCESS_KEY = credentials('jenkins2-aws-secret-access-key')
                }
                steps {
                    parallel (
                        FullDtest: {
                            script {
                                node(nodeParam) {
                                    runDtest (params.SPLIT_MAX_NODES, includeDtests)
                                }
                            }
                        },
                        HeavyDtest: {
                            script {
                                if (!params.SKIP_DTEST_HEAVY) {
                                    node(generalProperties.targetDtestStrongBuilder) {
                                        runDtest (splitMaxNodesForHeavyAndLong, "-a dtest-heavy")
                                    }
                                }
                            }
                        },
                        LongDtest: {
                            script {
                                if (!params.SKIP_DTEST_LONG) {
                                    node(generalProperties.targetDtestStrongBuilder) {
                                        runDtest (splitMaxNodesForHeavyAndLong, "-a dtest-long")
                                    }
                                }
                            }
                        }
                    )
                }
            }
        }

        post {
            //Order is: always, changed, fixed, regression, aborted, failure, success, unstable, and cleanup.
            always {
                script {
                    jenkins.cleanWorkSpaceUponRequest(params.PRESERVE_WORKSPACE)
                }
            }
        }
    }
}

def runDtest(String splitMaxNodes, String includeDtestsTag) {
	echo "runDtest"
	dtest.prepareDtestLocalTree (
		preserveWorkspace: params.PRESERVE_WORKSPACE,
		dtestBranch: params.SCYLLA_DTEST_BRANCH,
		dtestRepo: params.SCYLLA_DTEST_REPO,
		ccmBranch: params.SCYLLA_CCM_BRANCH,
		ccmRepo: params.SCYLLA_CCM_REPO,
		relocWebUrl: params.RELOC_WEB_URL,
		baseRelocJob: baseRelocJob,
		relocBuildID: params.RELOC_BUILD_ID,
		buildMode: buildMode,
	)
	numOfSplitFiles = dtest.splitAndCopyDtestJobs (
		splitTimeTarget: params.SPLIT_TIME_TARGET,
		splitMaxNodes: splitMaxNodes,
		buildMode: buildMode,
		includeTests: includeDtestsTag,
		excludeTests: excludeTests
	)
	dtest.doParallelDtest(
		dryRun: params.DRY_RUN,
		dtestMode: buildMode,
		downloadfromCloud: true,
		cloudUrl: params.RELOC_WEB_URL,
		dtestDebugInfoFlag: params.DTEST_DEBUG_INFO,
		dtestKeepLogsFlag: params.DTEST_KEEP_LOGS,
		extOpts: params.SCYLLA_EXT_OPTS_EXTRA_SETTINGS,
		extEnv: params.SCYLLA_EXT_ENV_EXTRA_SETTINGS,
		numOfSplitFiles: numOfSplitFiles,
		runningUserID: jenkins.getRunningUserInfo().userId,
		dtestRepo: params.SCYLLA_DTEST_REPO,
		dtestBranch: params.SCYLLA_DTEST_BRANCH,
		ccmBranch: params.SCYLLA_CCM_BRANCH,
		ccmRepo: params.SCYLLA_CCM_REPO,
		splitFleetLabal: params.SPLIT_FLEET_LABEL,
	)
}
