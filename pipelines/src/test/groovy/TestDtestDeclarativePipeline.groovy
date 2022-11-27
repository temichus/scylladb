#!groovy

import static com.lesfurets.jenkins.unit.MethodSignature.method
import static com.lesfurets.jenkins.unit.global.lib.ProjectSource.projectSource
import static com.lesfurets.jenkins.unit.global.lib.LibraryConfiguration.library
import static com.lesfurets.jenkins.unit.MethodCall.callArgsToString

import java.util.LinkedHashMap

import java.nio.file.Paths
import org.junit.Test
import org.junit.Before
import static org.junit.Assert.assertTrue
import com.lesfurets.jenkins.unit.declarative.*
import com.lesfurets.jenkins.unit.LibClassLoader
import org.codehaus.groovy.runtime.ComposedClosure

class TestDtestDeclarativePipeline extends DeclarativePipelineTest {

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
        helper.registerSharedLibrary(library().name('camunda-community')
                .defaultVersion('snapshot')
                .targetPath(sharedLibs)
                .retriever(projectSource(sharedLibs))
                .implicit(true)
                .allowOverride(false)
                .build()
        )
        helper.registerSharedLibrary(library().name('pipeline-logparser')
                .defaultVersion('3.2')
                .targetPath(sharedLibs)
                .retriever(projectSource(sharedLibs))
                .implicit(true)
                .allowOverride(false)
                .build()
        )
        helper.registerAllowedMethod('changeRequest', []) { true }
        binding.setVariable('scm', 'string')
        binding.setVariable('WORKSPACE', '/workspace/')
        binding.setVariable('NODE_NAME', 'Node1')
        binding.setVariable('JOB_NAME', 'job1')
        binding.setVariable('BUILD_TAG', 'build_tag')
        binding.setVariable('NODE_INDEX', '001')
        binding.setVariable('BUILD_USER_ID', 'fruch')
        binding.setVariable('BUILD_USER_EMAIL', 'fruch@scylladb.com')
        binding.setVariable('GIT_BRANCH', 'origin/master')
        binding.setVariable('GIT_URL', 'some_url')

        helper.registerAllowedMethod('legacySCM', [String])
        helper.registerAllowedMethod('library', [Map], {Map m ->
            helper.getLibLoader().loadImplicitLibraries()
            helper.getLibLoader().loadLibrary(m.identifier)
            helper.setGlobalVars(binding)
            return new LibClassLoader(helper, null)
        })

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

        binding.getVariable('currentBuild').getBuildCauses = { "Started by user" }

        helper.registerAllowedMethod('conditionalRetry', [Map], { Map c ->
            println "conditionalRetry: ${c}"
            c.runSteps.delegate = delegate
            helper.callClosure(c.runSteps)
        })
    }

    @Test
    void jenkinsfile_release_success() throws Exception {
        try {
            runScript('../jenkins_pipelines/master-dtest-release.jenkinsfile')
            assertJobStatusSuccess()
            assertTrue(helper.callStack.findAll { call ->
                call.methodName == "sh"
            }.any { call ->
                callArgsToString(call).contains("--mode=release")
            })
        } finally {
            printCallStack()
            println binding.getVariable('currentBuild')
        }
    }

    @Test
    void jenkinsfile_debug_success() throws Exception {
        try {
            runScript('../jenkins_pipelines/master-dtest-debug.jenkinsfile')
            assertJobStatusSuccess()
            assertTrue(helper.callStack.findAll { call ->
                call.methodName == "sh"
            }.any { call ->
                callArgsToString(call).contains("--mode=debug")
            })
        } finally {
            printCallStack()
            println binding.getVariable('currentBuild')
        }
    }
}
