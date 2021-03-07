#!groovy

import groovy.transform.CompileStatic
import org.junit.Test
import org.junit.Before
import com.lesfurets.jenkins.unit.declarative.*

/** Test the Pipeline used in PRs
* @auther fruch
* @since 1.0
*/
class TestPullRequestPipeline extends DeclarativePipelineTest {

    @Before
    @Override
    void setUp() throws Exception {
        // scriptRoots += ''
        scriptExtension = ''
        super.setUp()
        helper.registerAllowedMethod('changeRequest', []) { true }
    }

    @Test void jenkinsfile_success() throws Exception {
        runScript('../Jenkinsfile')

        printCallStack()
        assertCallStackContains('credentials(qa-aws-secret-key-id)')
        assertCallStackContains('credentials(qa-aws-secret-access-key)')
        assertCallStackContains('stage(precommit, groovy.lang.Closure)')

        assertJobStatusSuccess()
    }

}
