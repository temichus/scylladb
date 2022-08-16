# Dtest CI

Dtest CI is defined in `Jenkinsfile`, and triggered for each PR that gets opened

It has the following stages:

* `jenkins/precommit` - runs the pre-commit hooks, like pylint and autopep8, fail if any of them fails.
* `jenkins/collection` - do the pytest collections, to make sure all the tests can be imported and collect
                         (can find tests with problematic names, that can break out pipelines)
* `jenkins/test-pipelines` - run the pipeline unittests if some pipeline code was changed
* `jenkins/test/PR` - find the affected tests, and run them on the parallel pipeline, if it can't identify which test changed, runs the smoke tests

### Github labels
* `skip/test/PR` - Skip the PR test stage, for cases we know it would fail, or we are sure it's not relevant, and don't want to spend resources on it.
* `enterprise` - use this label when the changes are in test need to be run ontop of enterprise version
* `test/PR/by_files` - use this flag when the logic of finding the affected tests is broken, it would select all the test from any changed test file.
* `test/pipelines` - force running the pipeline unittests even if then weren't changed
* `test/Jenkinsfile` - should be used if we're changing the Jenkinsfile and want to test it out.
   only whom have write permissions would be able to use it like that. all others would pick the latest version of the pipeline code straight out next branch
