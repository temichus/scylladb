#!groovy
def lib = library identifier: 'dtest@snapshot', retriever: legacySCM(scm)

def pullRequestSetResult(String status, String context, String description){
    if (env.CHANGE_ID) {
        pullRequest.createStatus(status: status,
            context: context,
            description: description,
            targetUrl: "${env.JOB_URL}/workflow-stage")
    }
}

def pullRequestContainsLabels(String labels){
    result = false
    if (env.CHANGE_ID){
        def labels_to_look_for = labels.split(',')

        pullRequest.labels.each {
            if (labels_to_look_for.contains(it)){
                result = true
            }
        }
    }
    return result
}

pipeline {
    agent {
            label "sct-builders"
    }
    environment {
        AWS_ACCESS_KEY_ID         = credentials('qa-aws-secret-key-id')
        AWS_SECRET_ACCESS_KEY     = credentials('qa-aws-secret-access-key')
        GITHUB_TOKEN = credentials('github-api-access-token')
    }
    options {
        timestamps()
        buildDiscarder(logRotator(numToKeepStr: '10'))
    }
    parameters {
        string(name: 'PRODUCT_NAME', defaultValue: getProduct(), description: 'Choose: scylla|scylla-enterprise')
        string(name: 'BRANCH', defaultValue: getBranch(), description: 'Choose: master|branch-4.4')
        booleanParam(name: 'DRY_RUN', defaultValue: false, description: 'Check this to check pipeline syntax. will not perform anything.')
        booleanParam(name: 'PRESERVE_WORKSPACE', defaultValue: false, description: 'Check this if you need the workspace to remain (for debug)')
        string(name: 'SPLIT_TIME_TARGET', defaultValue: '240', description: 'Time period (minutes) for a test group to run. Used to calculate the needed number of spot machines')
    }
    stages {
        stage("precommit") {
            options {
                timeout(time: 30, unit: 'MINUTES')
            }
            steps {
                catchError(buildResult: 'SUCCESS', stageResult: 'FAILURE') {
                    script {
                        lastStage = env.STAGE_NAME

                        sh '''
                        export INSTALL_CASSANDRA="pip3 install --user https://github.com/scylladb/scylla-ccm/archive/next.zip"
                        ./scripts/run_test.sh bash -c "pre-commit run -a --show-diff-on-failure"
                        '''
                    }
                }
            }
            post {
                success {
                    script {
                        pullRequestSetResult('success', 'jenkins/precommit', 'Precommit passed')
                    }
                }
                failure {
                    script {
                        pullRequestSetResult('failure', 'jenkins/precommit', 'Precommit failed')
                    }
                }
            }
        }
        stage("test-collection") {
            options {
                timeout(time: 5, unit: 'MINUTES')
            }
            steps {
                catchError(buildResult: 'SUCCESS', stageResult: 'FAILURE') {
                    script {
                        lastStage = env.STAGE_NAME
                        sh '''
                        export INSTALL_CASSANDRA="pip3 install --user https://github.com/scylladb/scylla-ccm/archive/next.zip"
                         ./scripts/run_test.sh --collect-only -qqq
                        '''
                    }
                }
            }
            post {
                success {
                    script {
                        pullRequestSetResult('success', 'jenkins/collection', 'test collection passed')
                    }
                }
                failure {
                    script {
                        pullRequestSetResult('failure', 'jenkins/collection', 'test collection failed')
                    }
                }
            }
        }
        stage("test-pipelines") {
            options {
                timeout(time: 30, unit: 'MINUTES')
            }
            steps {
                catchError(buildResult: 'SUCCESS', stageResult: 'FAILURE') {
                    script {
                        lastStage = env.STAGE_NAME

                        def changedFiles = jenkins.getChangedFilesList()
                        def groovyFiles = changedFiles.findAll({it =~ /.*\.groovy/})
                        def jenkinsFiles = changedFiles.findAll({it =~ /.*\.jenkinsfile/})
                        if ((!groovyFiles.isEmpty() | !jenkinsFiles.isEmpty()) | (env.CHANGE_ID && pullRequestContainsLabels("test/pipelines"))) {
                            sh '''
                            chmod 777 -R pipelines
                            docker run -u gradle -v `pwd`:/dtest -w /dtest/pipelines gradle:7.3.3-jdk11-alpine gradle clean test -i
                            '''
                        }
                    }
                }
            }
            post {
                success {
                    script {
                        pullRequestSetResult('success', 'jenkins/test-pipelines', 'Test pipelines passed')
                    }
                }
                failure {
                    script {
                        pullRequestSetResult('failure', 'jenkins/test-pipelines', 'Test pipelines failed')
                    }
                }
            }
        }
        stage("test") {
            when {
                expression {
                    return env.CHANGE_ID && ! pullRequestContainsLabels("skip/test/PR")
                }
            }
            options {
                timeout(time: 2, unit: 'HOURS')
            }
            steps {
                catchError(buildResult: 'SUCCESS', stageResult: 'FAILURE') {
                    script {
                        lastStage = env.STAGE_NAME

                        needTestRun = false
                        def changedFiles = jenkins.getChangedFilesList()
                        def pythonChangedFiles = changedFiles.findAll({it =~ /.*\.py/})

                        // try running tests only if python files changed
                        if (!pythonChangedFiles.isEmpty()) {
                            needTestRun = true
                            String gitSelection = ""
                            if (env.CHANGE_TARGET) {
                                gitSelection = "origin/$CHANGE_TARGET"
                            } else {
                                gitSelection = "HEAD^"
                            }
                            def testFiles = ""
                            if (pullRequestContainsLabels("test/PR/by_files")) {
                                // run all the changed test files (regardless of the affect of files not in this PR)
                                testFiles = changedFiles.findAll({it =~ /.*_test.*py/}).join(' ')
                            } else {
                                // try finding which test is affected by changes so we'll only run it
                                sh(script: "./scripts/run_test.sh bash -c 'python scripts/selector.py $gitSelection > test_list.txt'")
                                testFiles = sh(returnStdout: true, script: " cat test_list.txt | tr '\n' ' '").trim()
                            }
                            echo "$testFiles"
                            RELOC_JOB_NAME = params.RELOC_JOB_NAME ?: "next"
                            BUILD_MODE = params.BUILD_MODE ?: "release"
                            SCYLLA_DTEST_REPO = params.SCYLLA_DTEST_REPO ?: "git@github.com:${env.CHANGE_FORK}/scylla-dtest.git"
                            SCYLLA_DTEST_BRANCH = params.SCYLLA_DTEST_BRANCH ?: env.CHANGE_BRANCH

                            String managerPackage = ""
                            if (testFiles.contains(' manager_')) {
                                managerPackage = artifact.getManagerRelocUrl("master")
                            }
                            if (testFiles) {
                                runParallelDtest("20", testFiles, "PR", managerPackage)
                            } else {
                                // if no affected tests, run only smoke tests (without spinning up more workers)
                                dtest.prepareDtestLocalTree (
                                    preserveWorkspace: false,
                                    dtestBranch: SCYLLA_DTEST_BRANCH,
                                    dtestRepo: SCYLLA_DTEST_REPO,
                                    ccmBranch: params.SCYLLA_CCM_BRANCH,
                                    ccmRepo: params.SCYLLA_CCM_REPO,
                                    relocWebUrl: params.RELOC_WEB_URL,
                                    baseRelocJob: RELOC_JOB_NAME,
                                    relocBuildID: params.RELOC_BUILD_ID,
                                    buildMode: BUILD_MODE,
                                )
                                dtest.doDtest(dryRun: params.DRY_RUN, dtestMode: BUILD_MODE, includeTests: "-m dtest_smoke bootstrap_test.py", dtestType: "PR")
                            }
                        }
                    }
                }
            }
            post {
                success {
                    script {
                        if (needTestRun) {
                            pullRequestSetResult('success', 'jenkins/test/PR', 'test passed')
                        }
                    }
                }
                failure {
                    script {
                        pullRequestSetResult('failure', 'jenkins/test/PR', 'test failed')
                    }
                }
            }
        }
    }

    post {
        //Order is: always, changed, fixed, regression, aborted, failure, success, unstable, and cleanup.
        always {
            script {
                jenkins.isSpotTermination(lastStage)
                jenkins.cleanWorkSpaceUponRequest(params.PRESERVE_WORKSPACE)
            }
        }
    }
}

def runParallelDtest(String splitMaxNodes, String includeDtestsTag, String dtestType, String managerPackage) {
    echo "runParallelDtest"
    dtest.prepareDtestLocalTree (
        preserveWorkspace: false,
        dtestBranch: SCYLLA_DTEST_BRANCH,
        dtestRepo: SCYLLA_DTEST_REPO,
        ccmBranch: params.SCYLLA_CCM_BRANCH,
        ccmRepo: params.SCYLLA_CCM_REPO,
        relocWebUrl: params.RELOC_WEB_URL,
        baseRelocJob: RELOC_JOB_NAME,
        relocBuildID: params.RELOC_BUILD_ID,
        buildMode: BUILD_MODE,
    )
    numOfSplitFiles = dtest.splitAndCopyDtestJobs (
        splitTimeTarget: params.SPLIT_TIME_TARGET,
        splitMaxNodes: splitMaxNodes,
        buildMode: BUILD_MODE,
        includeTests: includeDtestsTag,
        excludeTests: '',
        dtestType: dtestType,
    )
    dtest.doParallelDtest(
        dryRun: params.DRY_RUN,
        dtestMode: BUILD_MODE,
        downloadfromCloud: true,
        cloudUrl: params.RELOC_WEB_URL,
        dtestDebugInfoFlag: false,
        dtestKeepLogsFlag: false,
        extOpts: params.SCYLLA_EXT_OPTS_EXTRA_SETTINGS,
        extEnv: params.SCYLLA_EXT_ENV_EXTRA_SETTINGS,
        numOfSplitFiles: numOfSplitFiles,
        runningUserID: jenkins.getRunningUserInfo().userId,
        dtestRepo: SCYLLA_DTEST_REPO,
        dtestBranch: SCYLLA_DTEST_BRANCH,
        ccmBranch: params.SCYLLA_CCM_BRANCH,
        ccmRepo: params.SCYLLA_CCM_REPO,
        splitFleetLabal: params.SPLIT_FLEET_LABEL,
        dtestType: dtestType,
        managerPackage: managerPackage,
    )
}

def getProduct() {
    if (env.CHANGE_ID && pullRequestContainsLabels("enterprise")) {
        return "scylla-enterprise"
    }
    else {
        return "scylla"
    }
}

def getBranch() {
    if (env.CHANGE_ID && pullRequestContainsLabels("enterprise")) {
        return "enterprise"
    }
    else {
        return "master"
    }
}
