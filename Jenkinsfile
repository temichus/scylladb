#!groovy
def lib = library identifier: 'dtest@snapshot', retriever: legacySCM(scm)

def pullRequestSetResult(String status, String context, String description){
    if (env.CHANGE_ID) {
        pullRequest.createStatus(status: status,
            context: context,
            description: description,
            targetUrl: "${env.JOB_URL}/workflow-stage")
    }
    if (status == 'failure') {
        currentBuild.result = 'FAILURE'
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
        string(name: 'PRODUCT_NAME', defaultValue: "scylla", description: 'Choose: scylla|scylla-enterprise')
        string(name: 'BRANCH', defaultValue: "master", description: 'Choose: master|branch-4.4')
        booleanParam(name: 'DRY_RUN', defaultValue: false, description: 'Check this to check pipeline syntax. will not perform anything.')
        booleanParam(name: 'PRESERVE_WORKSPACE', defaultValue: false, description: 'Check this if you need the workspace to remain (for debug)')
    }
    stages {
        stage("precommit") {
            options {
                timeout(time: 30, unit: 'MINUTES')
            }
            steps {
                script {
                    lastStage = env.STAGE_NAME
                    try {
                        sh '''
                        rm -rf ./temp_home
                        mkdir -p ./temp_home
                        export HOME=`pwd`/temp_home
                        export SCYLLA_VERSION=dummy
                        ./scripts/run_test.sh bash -c "pip3 install --user https://github.com/scylladb/scylla-ccm/archive/next.zip; pre-commit run -a --show-diff-on-failure"
                        '''
                        pullRequestSetResult('success', 'jenkins/precommit', 'Precommit passed')
                    } catch(Exception ex) {
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
                script {
                    lastStage = env.STAGE_NAME
                    try {
                        sh '''
                        export INSTALL_CASSANDRA="pip3 install --user https://github.com/scylladb/scylla-ccm/archive/next.zip"
                         ./scripts/run_test.sh --collect-only -qqq
                        '''
                        pullRequestSetResult('success', 'jenkins/collection', 'test collection passed')
                    } catch(Exception ex) {
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
                script {
                    lastStage = env.STAGE_NAME

                    def changedFiles = jenkins.getChangedFilesList()
                    def groovyFiles = changedFiles.findAll({it =~ /.*\.groovy/})
                    def jenkinsFiles = changedFiles.findAll({it =~ /.*\.jenkinsfile/})
                    if ((!groovyFiles.isEmpty() | !jenkinsFiles.isEmpty()) | (env.CHANGE_ID && pullRequestContainsLabels("test/pipelines"))) {
                        try {
                            sh '''
                            chmod 777 -R pipelines
                            docker run -u gradle -v `pwd`:/dtest -w /dtest/pipelines gradle:7.3.3-jdk11-alpine gradle clean test -i
                            '''
                            pullRequestSetResult('success', 'jenkins/test-pipelines', 'Test pipelines passed')
                        } catch(Exception ex) {
                            pullRequestSetResult('failure', 'jenkins/test-pipelines', 'Test pipelines failed')
                        }
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
                timeout(time: 30, unit: 'MINUTES')
            }
            steps {
                script {
                    lastStage = env.STAGE_NAME
                    try {
                        def changedFiles = jenkins.getChangedFilesList()
                        def testFiles = changedFiles.findAll({it =~ /.*_test.*py/})

                        RELOC_JOB_NAME = params.RELOC_JOB_NAME ?: "next"
                        BUILD_MODE = params.BUILD_MODE ?: "release"
                        SCYLLA_DTEST_REPO = params.SCYLLA_DTEST_REPO ?: "git@github.com:${env.CHANGE_FORK}/scylla-dtest.git"
                        SCYLLA_DTEST_BRANCH = params.SCYLLA_DTEST_BRANCH ?: env.CHANGE_BRANCH

                        if (testFiles) {
                            runParallelDtest("20", testFiles.join(' '), "PR")
                        } else {
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
                        pullRequestSetResult('success', 'jenkins/test/PR', 'test passed')
                    } catch(Exception ex) {
                        echo ex
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

def runParallelDtest(String splitMaxNodes, String includeDtestsTag, String dtestType) {
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
    )
}
