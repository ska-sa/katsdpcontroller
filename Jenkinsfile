#!groovy

@Library('katsdpjenkins') _
katsdp.killOldJobs()
katsdp.setDependencies([
    'ska-sa/katsdpdockerbase/master',
    'ska-sa/katsdpmodels/master',
    'ska-sa/katsdpservices/master',
    'ska-sa/katsdptelstate/master'])
katsdp.standardBuild(docker_venv: true, push_external: true)
katsdp.mail('sdpdev+katsdpcontroller@ska.ac.za')
