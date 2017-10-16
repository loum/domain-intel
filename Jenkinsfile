@Library('jenkins-ipe-lib')
import mergeToMaster
import publishToDevpi
import globals

node('master') {
    cleanWs()
    stage ('Check out') {
        checkout scm
        gitCommit = sh(returnStdout: true, script: 'git rev-parse --abbrev-ref HEAD').trim()
        sh "echo ${gitCommit}"
        env.LOCAL_GIT_COMMIT = "${gitCommit}"
    }

    stage ('Setup') {
        globals()
        env.PYENV_HOME = "${env.WORKSPACE}/3env/"
    }

    stage ('Setup python2/3 virtualenvs') {
		sh 'make init PYVERSION=2 APP_ENV=dev'
		sh 'make init PYVERSION=3 APP_ENV=dev'
    }

    stage ('Test domain-intel python2 ...') {
        try {
            sh '. 2env/bin/activate; make test; deactivate'
        }
        catch (e) {
/*            slackSend (
                color: '#F00',
                message: "python2 tests failed for " +
                         "${env.JOB_NAME} #${env.BUILD_NUMBER}: ${env.BUILD_URL}"
            )*/
            throw e
        }
/*        slackSend (
            color: '#0F0',
            message: "python2 tests successful for " +
                     "${env.JOB_NAME} #${env.BUILD_NUMBER}: ${env.BUILD_URL}"
        )*/
    }

    stage ('Test domain-intel python3 ...') {
        try {
          sh '. 3env/bin/activate; make test; deactivate'
          step([$class: 'JUnitResultArchiver', testResults: "junit.xml"])
        }
        catch (e) {
/*            slackSend (
                color: '#F00',
                message: "python3 tests failed for " +
                         "${env.JOB_NAME} #${env.BUILD_NUMBER}: ${env.BUILD_URL}"
            )*/
            throw e
        }
/*        slackSend (
            color: '#0F0',
            message: "python3 tests successful for " +
                     "${env.JOB_NAME} #${env.BUILD_NUMBER}: ${env.BUILD_URL}"
        )*/
    }

    stage ('Build the domain-intel docs ...') {
        sh '. 3env/bin/activate; make docs; deactivate'
        publishHTML (
            target: [
                allowMissing: false,
                alwaysLinkToLastBuild: false,
                keepAll: false,
                reportDir: 'doc/out',
                reportFiles: 'index.html',
                reportName: 'Domain Intel Docs'
            ]
        )
    }

    stage ('Publish to devpi - master branch only') {
        if ("${env.LOCAL_GIT_COMMIT}" == 'master') {
            publishToDevpi()
        }
    }
}
