#!groovy

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
	if (!changeRequest() || !env.CHANGE_ID){
		return false
	}
	def labels_to_look_for = labels.split(',')
	def result = false
	pullRequest.labels.each {
		if (labels_to_look_for.contains(it)){
			result = true
		}
	}
	return result
}

pipeline {
    agent {
        label {
            label "sct-builders"
        }
    }
    environment {
        AWS_ACCESS_KEY_ID         = credentials('qa-aws-secret-key-id')
        AWS_SECRET_ACCESS_KEY     = credentials('qa-aws-secret-access-key')
    }
    options {
        timestamps()
        buildDiscarder(logRotator(numToKeepStr: '10'))
    }
    stages {
        stage("precommit") {
            when {
                expression {
                    return changeRequest() || env.CHANGE_ID
                }
            }
            options {
                timeout(time: 30, unit: 'MINUTES')
            }
            steps {
                script {
                    try {
                        sh '''
                        rm -rf ./temp_home
                        mkdir -p ./temp_home
                        export HOME=`pwd`/temp_home
                        export SCYLLA_VERSION=dummy
                        ./scripts/run_test.sh 'bash -c "pre-commit run -a --show-diff-on-failure"'
                        '''
                        pullRequestSetResult('success', 'jenkins/precommit', 'Precommit passed')
                    } catch(Exception ex) {
                        pullRequestSetResult('failure', 'jenkins/precommit', 'Precommit failed')
                    }
                }
            }
        }
    }
}
