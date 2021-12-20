pipeline {
    agent any

    stages {
        stage('checkout') {
            steps {
                checkout([$class: 'GitSCM', branches: [[name: '*/master']], extensions: [], userRemoteConfigs: [[credentialsId: '6b13915c-d7fe-4b60-a429-b2e1c8be951e', url: 'https://github.com/Ambar752/code-20211219-ambaracharya.git']]])
            }
        }
        stage('test') {
            steps {
                git credentialsId: '6b13915c-d7fe-4b60-a429-b2e1c8be951e', url: 'https://github.com/Ambar752/code-20211219-ambaracharya.git'
                sh '/opt/spark/bin/spark-submit --master local[*] /home/ambar/IdeaProjects/bmiCalculator/bmiutest.py /home/ambar/IdeaProjects/bmiCalculator/test/data/patientSourceData.json /home/ambar/IdeaProjects/bmiCalculator/test/out'
            }
        }
        stage('build') {
            steps {
                git credentialsId: '6b13915c-d7fe-4b60-a429-b2e1c8be951e', url: 'https://github.com/Ambar752/code-20211219-ambaracharya.git'
                sh '/opt/spark/bin/spark-submit --master local[*] /home/ambar/IdeaProjects/bmiCalculator/calcBmicountObeseCount.py /home/ambar/IdeaProjects/bmiCalculator/data/patientSourceData.json /home/ambar/IdeaProjects/bmiCalculator/out'
            }
        }
        }
}