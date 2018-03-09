#!groovy

@Library('katsdpjenkins') _
katsdp.setDependencies([
    'ska-sa/katsdpdockerbase/master',
    'ska-sa/katsdpservices/master',
    'ska-sa/katsdptelstate/master'])
katsdp.standardBuild(docker_venv: true, python3: true, python2: false)
katsdp.mail('simonr@ska.ac.za bmerry@ska.ac.za')
