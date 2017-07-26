#!groovy

@Library('katsdpjenkins') _
katsdp.setDependencies([
    'ska-sa/katsdpdockerbase/master',
    'ska-sa/katsdpservices/master',
    'ska-sa/katsdptelstate/master'])
katsdp.standardBuild(docker_venv: true)
katsdp.mail('simonr@ska.ac.za bmerry@ska.ac.za')
