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
    String architecture = ""

    // normalize the url to make sure it's a vaild s3 url
    url = url.replaceFirst("http://", "")
    if (! url.contains("s3://")) {
		url = "s3://$url"
    }

    String packageName = relocPackageName (
		checkLocal: false,
		mustExist: true,
		urlOrPath: url,
		packagePrefix: params.PRODUCT_NAME,
		buildMode: buildMode,
		architecture: architecture,
	)
    artifactsTargets.scyllaReloc = [artifact: packageName,
                                    target: "$WORKSPACE/${params.PRODUCT_NAME}/build/${buildMode}/dist/tar"]
    artifactsTargets.jmxReloc = [artifact: "${params.PRODUCT_NAME}-jmx-package.tar.gz",
                                 target: "$WORKSPACE/${params.PRODUCT_NAME}/build/${buildMode}/dist/tar"]
    artifactsTargets.toolsJavaReloc = [artifact: "${params.PRODUCT_NAME}-tools-package.tar.gz",
                                       target: "$WORKSPACE/${params.PRODUCT_NAME}/build/${buildMode}/dist/tar"]
	artifactsTargets.metadataFile = [artifact: generalProperties.buildMetadataFile, target: WORKSPACE]

	artifactsTargets.each { key, val ->
		downloadArtifactFromS3(artifact: val.artifact,
			targetPath: val.target,
			sourceUrl: url)
	}
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

	jenkins.traceFunctionParams("artifact.relocPackageName", args)

	boolean checkLocal = args.checkLocal ?: false
	boolean dryRun = args.dryRun ?: false
	boolean mustExist = args.mustExist != null ? args.mustExist : true
	if (dryRun) {
		mustExist = false
	}
	String buildMode = args.buildMode ?: ""
	String architecture = args.architecture ?: ""
	if (architecture){
		architecture += "-"
	}
	String packageName = "${args.packagePrefix}-${architecture}package.tar.gz"
	String packageNameNoArch = "${args.packagePrefix}-package.tar.gz"
	String packageNameX86 = "${args.packagePrefix}-${generalProperties.x86ArchName}-package.tar.gz"
	String lsOutput = ""
	if (buildMode.contains("debug") && args.packagePrefix == params.PRODUCT_NAME) {
		packageName = "${args.packagePrefix}-${buildMode}-${architecture}package.tar.gz"
		packageNameNoArch = "${args.packagePrefix}-${buildMode}-package.tar.gz"
		packageNameX86 = "${args.packagePrefix}-${buildMode}-${generalProperties.x86ArchName}-package.tar.gz"
	}

	if ((checkLocal && fileExistsOnPath(packageName, args.urlOrPath)) ||
			(! checkLocal && fileExistsOnCloud("${args.urlOrPath}/${packageName}"))) {
		echo "Found package $packageName with architecture if given, or with no architecture if not given"
		return packageName
	}
	echo "Could not find package with architecture if given, or no arch if not given. Trying another way."

	String packageNameToReturn = ""
	if (architecture) {
		packageNameToReturn = packageNameNoArch
	} else {
		packageNameToReturn = packageNameX86
	}
	if ((checkLocal && fileExistsOnPath(packageNameToReturn, args.urlOrPath)) ||
			(! checkLocal && fileExistsOnCloud("${args.urlOrPath}/${packageNameToReturn}"))) {
		echo "Found package $packageNameToReturn with architecture"
		return packageNameToReturn
	}
	if (mustExist) {
		error ("No package (with or without architecture) found")
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
