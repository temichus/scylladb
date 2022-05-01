#!groovy

import static groovy.json.JsonOutput.*

def getRunningUserInfo () {
	def buildCause = currentBuild.getBuildCauses()
	String strBuildCause = buildCause.toString()
	String runningUserID
	String runningUserEmail
	echo "Build cause: |$strBuildCause|"

	if (strBuildCause.contains("Started by user")) {
		echo "This is a user requested build. get username"
		// The wrap fails in case the build was triggered by an scm change / timer.
		wrap([$class: 'BuildUser']) {
			 // https://wiki.jenkins-ci.org/display/JENKINS/Build+User+Vars+Plugin variables available inside this block
			 runningUserID = "${BUILD_USER_ID}"
			 runningUserEmail = BUILD_USER_EMAIL
		}
	} else {
		runningUserID = "jenkins"
		runningUserEmail = "jenkins"
	}
	return [ userId: runningUserID, email: runningUserEmail ]
}

def traceFunctionParams (String functionName, args) {
	echo "$functionName parameters: ${prettyPrint(toJson(args))}"
}

def cleanWorkSpaceUponRequest(boolean preserveWorkSpace = false, boolean cleanRootFiles = true) {
	echo "Cleaning workspace |$WORKSPACE|, Node: |$NODE_NAME|"
	if (preserveWorkSpace) {
		echo "Keeping workspace due to user request"
	} else {
		// In order to clean also root owned files, first change ownership
		// https://issues.jenkins-ci.org/browse/JENKINS-24440?focusedCommentId=357010&page=com.atlassian.jira.plugin.system.issuetabpanels%3Acomment-tabpanel#comment-357010
		if (cleanRootFiles) {
			sh "sudo chmod -R 777 ."
		}
		cleanWs() /* clean up our workspace */
		// Clean docker images older then 2 weeks to save disk space. The which docker is to prevent error when no docker installed.
	}
}

def checkAndTagAwsInstance (String runningUserID) {
	// TAG the spot instance with more tags
		wrap([$class: 'BuildUser']) {
			withCredentials([string(credentialsId: 'jenkins2-aws-secret-key-id', variable: 'AWS_ACCESS_KEY_ID'),
			string(credentialsId: 'jenkins2-aws-secret-access-key', variable: 'AWS_SECRET_ACCESS_KEY')]) {
            sh("""
                INSTANCE_ID=`curl -s http://169.254.169.254/latest/meta-data/instance-id`
                REGION_NAME=`curl -s http://169.254.169.254/latest/meta-data/placement/region`

                aws ec2 --region \${REGION_NAME} create-tags \
                        --resources \${INSTANCE_ID} \
                        --tag Key=RunByUser,Value=${runningUserID} Key=JenkinsJobTag,Value=${BUILD_TAG} Key=NodeType,Value=compile-spotfleet Key=keep,Value=${params.TIMEOUT_PARAM} Key=keep_action,Value=terminate

                echo `curl -s http://169.254.169.254/latest/meta-data/instance-type`
            """)
		}
	}
}

def raiseErrorOnFailureStatus (boolean status, String description) {
	if (status) {
		echo "Error: $description"
		if (currentBuild.currentResult != "ABORTED") {
			currentBuild.result = 'FAILURE'
			error("$description")
		}
	}
}

def getChangedFilesList() {
    // Get list of files changed,
    // if in PR the diff is against the target branch, if not in PR the diff is only the last commit
    String diffScript = ""
    if (env.CHANGE_TARGET) {
        diffScript = "git diff --name-only origin/$CHANGE_TARGET"
    } else {
        diffScript = "git diff --name-only HEAD^"
    }
    String files = sh(script: diffScript, returnStdout: true)
    return files.split()
}

def isSpotTermination (String lastStage = env.STAGE_NAME) {
	try {
		echo "Last stage: |$lastStage|"
		sh """
			echo 'lastStage=$lastStage' >> $WORKSPACE/$generalProperties.jobSummaryFile
			echo 'result=$currentBuild.currentResult' >> $WORKSPACE/$generalProperties.jobSummaryFile
		"""
		artifact.publishArtifactsStatus(generalProperties.jobSummaryFile, WORKSPACE)
	} catch (java.io.IOException e) {
		echo "Spot termination. Writing job description"
		currentBuild.description = "spot termination"
		error("Spot termination")
	} catch (error) {
		echo "Other error: |$error|"
	}
}
