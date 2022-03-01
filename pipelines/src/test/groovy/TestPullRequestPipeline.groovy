#!groovy

import static com.lesfurets.jenkins.unit.global.lib.ProjectSource.projectSource
import static com.lesfurets.jenkins.unit.global.lib.LibraryConfiguration.library

import groovy.transform.CompileStatic
import org.junit.Test
import org.junit.Before
import com.lesfurets.jenkins.unit.declarative.*
import com.lesfurets.jenkins.unit.LibClassLoader

class AffectedFile {
    private path = "none.py"
    def getPath() { this.path }
}

class Entry {
    def getAffectedFiles() {
        return [new AffectedFile(), new AffectedFile()]
    }
}

class ChangeSet {
    def getItems() {
         return [new Entry(), new Entry()]
    }
}

/** Test the Pipeline used in PRs
* @auther fruch
* @since 1.0
*/
class TestPullRequestPipeline extends DeclarativePipelineTest {

    String sharedLibs = "../"

    @Before
    @Override
    void setUp() throws Exception {
        super.setUp()
        helper.cloneArgsOnMethodCallRegistration = false

        helper.registerSharedLibrary(library().name('dtest')
                .defaultVersion('snapshot')
                .targetPath(sharedLibs)
                .retriever(projectSource(sharedLibs))
                .implicit(true)
                .allowOverride(false)
                .build()
        )
        helper.registerAllowedMethod('changeRequest', []) { true }

        binding.setVariable('WORKSPACE', '/workspace/')
        binding.setVariable('NODE_NAME', 'Node1')
        binding.setVariable('JOB_NAME', 'job1')
        binding.setVariable('BUILD_TAG', 'build_tag')
        binding.setVariable('NODE_INDEX', '001')
        binding.setVariable('BUILD_USER_ID', 'fruch')
        binding.setVariable('BUILD_USER_EMAIL', 'fruch@scylladb.com')
        binding.setVariable('CHANGE_TARGET', 'next')
        binding.setVariable('scm', 'string')
        helper.registerAllowedMethod('legacySCM', [String])
        helper.registerAllowedMethod('library', [Map], {Map m ->
            helper.getLibLoader().loadImplicitLibraries()
            helper.getLibLoader().loadLibrary(m.identifier)
            helper.setGlobalVars(binding)
            return new LibClassLoader(helper, null)
        })
        binding.getVariable('currentBuild').changeSets = [ new ChangeSet(), new ChangeSet()]
        binding.setVariable('pullRequest', [labels: []])
        helper.registerAllowedMethod('git', [Map], { Map c ->
            println "Git cloning: ${c}"
        })

        helper.registerAllowedMethod('fileExists', [String], {String s ->
            println "fileExists: $s"
            return true
        })

        helper.registerAllowedMethod("sh", [Map.class], {c ->
            println "exec sh: $c"
            if (c.script.contains('ls /workspace//scylla-dtest/include')) {
                return "1"
            }
            return "bcc19744"
        })

        helper.registerAllowedMethod('wrap', [Map, Closure], { Map args, Closure c ->
            c.delegate = delegate
            helper.callClosure(c)
        })
        helper.registerAllowedMethod('catchError', [Map, Closure], { Map args, Closure c ->
            c.delegate = delegate
            helper.callClosure(c)
        })
        binding.getVariable('currentBuild').getBuildCauses = { "Started by user" }
    }

    @Test void jenkinsfile_success() throws Exception {
        runScript('../Jenkinsfile')

        printCallStack()
        assertCallStackContains('credentials(qa-aws-secret-key-id)')
        assertCallStackContains('credentials(qa-aws-secret-access-key)')
        assertCallStackContains('stage(precommit, groovy.lang.Closure)')
        assertJobStatusSuccess()
    }

    @Test void jenkinsfile_PR_tagged_success() throws Exception {

        binding.setVariable('pullRequest', [labels: ["test/PR", ]])

        runScript('../Jenkinsfile')

        printCallStack()
        assertCallStackContains('credentials(qa-aws-secret-key-id)')
        assertCallStackContains('credentials(qa-aws-secret-access-key)')
        assertCallStackContains('stage(precommit, groovy.lang.Closure)')
        assertJobStatusSuccess()
    }


}
