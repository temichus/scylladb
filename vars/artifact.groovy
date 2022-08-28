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

def getManagerRelocUrl(String url, String architecture='x86_64') {
    if (url.startsWith('http')) {
        return url
    }

    if (url.isEmpty()) {
        url = "master"
    }

    url = "s3://downloads.scylladb.com/manager/relocatable/unstable/$url/"
    String directory = sh(script: "aws s3 ls $url | sort | tail -n1 | awk '{print \$2}'", returnStdout:true).trim()
    String filename = sh(script: "aws s3 ls $url$directory | grep $architecture | awk '{print \$4}'", returnStdout:true).trim()

    url = url.replace('s3:', 'http:')

    return "$url$directory$filename"
}

def getRelocArtifacts (String cloudUrl, String buildMode) {
	// get Test artifacts from jenkins or cloud
	//
	// Parameters:
	def artifactsTargets = [:]
    String url = getRelocatableLink(cloudUrl)
    String architecture = generalProperties.x86ArchName

    // normalize the url to make sure it's a vaild s3 url
    url = url.replaceFirst("http://", "")
    if (! url.contains("s3://")) {
		url = "s3://$url"
    }
    downloadArtifactFromS3(artifact: generalProperties.buildMetadataFile, targetPath: WORKSPACE, sourceUrl: url)

    releaseFromMetadata = fetchMetadataValue (
        downloadFromCloud: true,
        fieldName: "scylla-release:",
    )

    versionFromMetadata = fetchMetadataValue (
        downloadFromCloud: true,
        fieldName: "scylla-version:",
    )

    scyllaPackageName = relocPackageName (
		checkLocal: false,
		mustExist: true,
		urlOrPath: url,
		packagePrefix: params.PRODUCT_NAME,
		buildMode: buildMode,
		architecture: architecture,
		version: versionFromMetadata,
		release: releaseFromMetadata,
	)
	jmxPackageName = "${params.PRODUCT_NAME}-jmx-${versionFromMetadata}-${releaseFromMetadata}.noarch.tar.gz"
	toolsPackageName = "${params.PRODUCT_NAME}-tools-${versionFromMetadata}-${releaseFromMetadata}.noarch.tar.gz"
	target = "$WORKSPACE/${params.PRODUCT_NAME}/build/${buildMode}/dist/tar"
    artifactsTargets.scyllaReloc = [artifact: scyllaPackageName, target: target]
    artifactsTargets.jmxReloc = [artifact: jmxPackageName, target: target]
    artifactsTargets.toolsJavaReloc = [artifact: toolsPackageName, target: target]

	artifactsTargets.each { key, val ->
		downloadArtifactFromS3(artifact: val.artifact,
			targetPath: val.target,
			sourceUrl: url)
	}
	return [scyllaPackageName, jmxPackageName, toolsPackageName]
}

boolean fileExistsOnPath(String file, String path=WORKSPACE) {
    boolean exists = fileExists "$path/$file"
    if (exists) {
        echo "File |$file| exists on path |$path|"
        return true
    } else {
        echo "File |$file| does not exist on path |$path|"
        return false
    }
}

String relocPackageName (Map args) {
	// returns Scylla package name on cloud
	// Assuming a package naming convention: "${package name}-${build mode not on release}-${optional architecture}-package.tar.gz"
	// Parameters:
	// boolean (default false): dryRun - Run builds on dry run (that will show commands instead of really run them).
	// boolean (default false) checkLocal - true to check on local disk, false to check on cloud
	// boolean (default true) mustExist - If must exist - error if not found, else, return ""
	// String (mandatory): packagePrefix - The name of the package with no mode, architecture, package and tar.
	// String (default local): urlOrPath - From where to download the artifacts - local path or a URL.
	// String (default: none): buildMode (release | debug | dev)
	// String (default null): architecture Which architecture to publish x86_64|aarch64 Default null is for backwards competability
	// String (mandatory): version: reloc package version
	// String (mandatory): release: reloc package release

	jenkins.traceFunctionParams("artifact.relocPackageName", args)

	boolean checkLocal = args.checkLocal ?: false
	boolean dryRun = args.dryRun ?: false
	boolean mustExist = args.mustExist != null ? args.mustExist : true
	if (dryRun) {
		mustExist = false
	}
	String buildMode = args.buildMode ?: ""
	String architecture = args.architecture ?: generalProperties.x86ArchName

	String packageName = "${args.packagePrefix}-${args.version}-${args.release}.${architecture}.tar.gz"
	String lsOutput = ""
	if (buildMode.contains("debug") && args.packagePrefix == params.PRODUCT_NAME) {
		packageName = "${args.packagePrefix}-${buildMode}-${args.version}-${args.release}.${architecture}.tar.gz"
	}

	if ((checkLocal && fileExistsOnPath(packageName, args.urlOrPath)) ||
			(! checkLocal && fileExistsOnCloud("${args.urlOrPath}/${packageName}"))) {
		echo "Found package $packageName"
		return packageName
	}

	if (mustExist) {
		error ("No package found")
	} else {
		echo "Didn't find any package"
		return ""
	}
}

def runOrDryRunShOutput (boolean dryRun = false, String cmd, String description) {
    echo "$description"
    def cmdOutput = ""
    if (dryRun) {
        echo "Dry-run: |$cmd|"
        cmdOutput = "Dry-run"
    } else {
        echo "Running sh cmd: |$cmd|"
        cmdOutput = sh(script: "$cmd", returnStdout:true).trim()
    }
    echo "Command output: |$cmdOutput|"
    return cmdOutput
}

boolean fileExistsOnCloud(String url) {
    try {
        def lsOutput = runOrDryRunShOutput (false, "aws s3 ls $url", "Check if $url exists")
        echo "aws s3 ls output: |$lsOutput|"
        if (lsOutput) {
            echo "URL: |$url| exists."
            return true
        } else {
            echo "URL: |$url| does not exist."
            return false
        }
    } catch (error) {
        echo "URL: |$url| does not exist."
        return false
    }
}

String fetchMetadataValue (Map args) {
    // get a value from metadata file as artifact from jenkins or cloud
    // Download the file if does not exist locally
    //
    // Parameters:
    // boolean (default false): downloadFromCloud - Whether to publish to cloud storage (S3) or not.
    // String (mandatory if downloadFromCloud is false): artifactSourceJob - From where to download the artifacts.
    // String (default: last success run): artifactSourceJobNum (number) to download from
    // String (mandatory if downloadFromCloud): cloudUrl - From where to download the artifacts.
    // String (mandatory): fieldName - Name of field to take value from.

    boolean downloadFromCloud = args.downloadFromCloud ?: false
    String cloudUrl = args.cloudUrl ?: ""
    boolean local = args.local ?: false

    // FixMe: We should improve this in the future to return a Metadata object that the callers can query.
    String metaDataFileName=generalProperties.buildMetadataFile

    String metaDataFilePath="$WORKSPACE/${metaDataFileName}"

    fieldValue = sh(script: "grep '$args.fieldName' $metaDataFilePath | awk '{print \$2}'", returnStdout: true).trim()
    echo "Value of field |$args.fieldName| from metadatafile: |$fieldValue|"
    if (! fieldValue) {
        error ("Could not get $args.fieldName from relocatable metadata file")
    }
    return fieldValue
}
