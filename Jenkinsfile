@Library('jenkins-joylib@v1.0.2') _

pipeline {

    agent {
        label joyCommonLabels(image_ver: '19.1.0')
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
