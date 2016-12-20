#!groovy

@Library('katsdpjenkins') _
katsdp.setDependencies(['ska-sa/katsdpdockerbase/master'])
katsdp.standardBuild(docker_venv: true)
katsdp.mail('simonr@ska.ac.za')
