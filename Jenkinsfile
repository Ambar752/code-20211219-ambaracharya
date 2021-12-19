pipeline {
    agent any

    stages {
        stage('checkout') {
            steps {
                checkout([$class: 'GitSCM', branches: [[name: '*/master']], extensions: [], userRemoteConfigs: [[credentialsId: '6b13915c-d7fe-4b60-a429-b2e1c8be951e', url: 'https://github.com/Ambar752/code-20211219-ambaracharya.git']]])
            }
        }
        stage('build') {
            steps {
                git credentialsId: '6b13915c-d7fe-4b60-a429-b2e1c8be951e', url: 'https://github.com/Ambar752/code-20211219-ambaracharya.git'
                sh '/opt/spark/bin/spark-submit --master local[*] /home/ambar/IdeaProjects/bmiCalculator/calcBmicountObese.py'
            }
        }
        }
}