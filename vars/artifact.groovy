#!groovy

boolean publishArtifactsStatus (String wildcardFiles, String baseDir, String excludeWildcardFiles = "") {
	// This function gets baseDir as full path or as relative path based on the default dir when you call it.
	// It does cd on the baseDir.
	// The wildcardFiles should be a path, relative to the baseDir (or a file). This path (file) is the path to publihs.
	// So if you give a as baseDir and b/c.txt as File, you will see b/c.txt as the artifact.
	boolean status = false
	echo "Going to publish artifacts: |$wildcardFiles| from dir |$baseDir|, excluding |$excludeWildcardFiles|"
	boolean dirExists = fileExists "$baseDir"
	if (dirExists) {
		dir(baseDir) {
            try {
                archiveArtifacts artifacts: "$wildcardFiles", excludes: "$excludeWildcardFiles"
            } catch (error) {
                echo "Error: Could not publish |$wildcardFiles|. Error: |$error|"
                status = true
            }
		}
	} else {
		echo "Nothing to publish (no baseDir |$baseDir|)"
		status = true
	}
	return status
}

def getRelocatableLink(String cloudUrl) {
    if ("latest".equals(cloudUrl)) {
        return "s3://downloads.scylladb.com/unstable/${params.PRODUCT_NAME}/${params.BRANCH}/relocatable/latest"
    }
    return cloudUrl
}
def downloadArtifactFromS3(Map args) {
    String artifact = args.artifact
	String targetPath = args.targetPath
	String sourceUrl = args.sourceUrl

    withCredentials([string(credentialsId: 'jenkins2-aws-secret-key-id', variable: 'AWS_ACCESS_KEY_ID'),
		string(credentialsId: 'jenkins2-aws-secret-access-key', variable: 'AWS_SECRET_ACCESS_KEY')]) {

        echo "downloading:  ${sourceUrl}/${artifact}"
        sh "aws s3 cp --only-show-errors ${sourceUrl}/${artifact} $targetPath/$artifact"
    }
}
def getRelocArtifacts (String cloudUrl, String buildMode) {
	// get Test artifacts from jenkins or cloud
	//
	// Parameters:
	def artifactsTargets = [:]
    String url = getRelocatableLink(cloudUrl)
    artifactsTargets.scyllaReloc = [artifact: "${params.PRODUCT_NAME}-package.tar.gz",
                                    target: "$WORKSPACE/scylla/build/${buildMode}/dist/tar"]
    artifactsTargets.jmxReloc = [artifact: "${params.PRODUCT_NAME}-jmx-package.tar.gz",
                                 target: "$WORKSPACE/scylla/build/${buildMode}/dist/tar"]
    artifactsTargets.toolsJavaReloc = [artifact: "${params.PRODUCT_NAME}-tools-package.tar.gz",
                                       target: "$WORKSPACE/scylla/build/${buildMode}/dist/tar"]
	artifactsTargets.metadataFile = [artifact: generalProperties.buildMetadataFile, target: WORKSPACE]

	artifactsTargets.each { key, val ->
		downloadArtifactFromS3(artifact: val.artifact,
			targetPath: val.target,
			sourceUrl: url)
	}
}
