@Library('jenkins-joylib@v1.0.2') _

pipeline {

    agent {
        // Temporarily use new jenkins-agent 2.2.0 from MANTA-4728.
        label joyCommonLabels(image_ver: '19.1.0') + ' && jenkins_agent:2.2.0'
    }

    options {
        buildDiscarder(logRotator(numToKeepStr: '90'))
        timestamps()
    }

    stages {
        stage('check') {
            steps{
                sh('make check')
            }
        }
        stage('test') {
            steps{
                sh('make test-unit')
            }
        }
        stage('build image and upload') {
            steps {
                joyBuildImageAndUpload()
            }
        }
    }

    post {
        always {
            joyMattermostNotification()
        }
    }
}
