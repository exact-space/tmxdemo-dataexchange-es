pipeline {
    agent any  
    environment {
        VERSION = "${env.BUILD_ID}"
        registry = "dev.exactspace.co"
        APP_NAME = "tmxdemo-dataexchange-es"
    }
    stages {
        stage("get scm") {
            steps {
                checkout([$class: 'GitSCM',
                    branches: [[name: 'master']],
                    userRemoteConfigs: [[url: 'git@github.com:exact-space/tmxdemo-dataexchange-es.git']]
                ])
            }
        }
        stage("cython compilation") {
            steps {
                sh "cython index.py --embed"
            }
        }
        stage("building images") {
            steps {
                sh "sudo docker build --rm --no-cache -t $APP_NAME:r1 ."
            }
        }
        stage("tagging images-r1") {
            steps {
                sh "sudo docker tag $APP_NAME:r1 $registry/$APP_NAME:r1"
            }
        }
        stage("remove old docker image-r1") {
            steps {
                sh "sudo docker image rm $APP_NAME:r1"
            }
        }
        stage("image push-r1") {
            steps {
                sh "sudo docker push $registry/$APP_NAME:r1"
            }
        }    
       /* stage('deploying to prod') {
            steps {
                catchError(buildResult: 'SUCCESS', stageResult: 'FAILURE') {
                timeout(time: 15, unit: 'MINUTES') {
                    httpRequest url: "https://data.exactspace.co/deployservice/cicd/mqtt/$APP_NAME", timeout: 900000
                }
            }
        }
        }
        stage('deploying to UTCL') {
            steps {
                catchError(buildResult: 'SUCCESS', stageResult: 'FAILURE') {
                timeout(time: 15, unit: 'MINUTES') {
                    httpRequest url: "https://cpp.utclconnect.com/deployservice/cicd/mqtt/$APP_NAME", timeout: 900000
                }
            }
        }
        }
        stage('deploying to HRD') {
            steps {
                catchError(buildResult: 'SUCCESS', stageResult: 'FAILURE') {
                timeout(time: 15, unit: 'MINUTES') {
                    httpRequest url: "https://hrd-dcs/deployservice/cicd/api/$APP_NAME", timeout: 900000
                }
            }
        }
        }
        stage('deploying to LPG') {
            steps {
                catchError(buildResult: 'SUCCESS', stageResult: 'FAILURE') {
                timeout(time: 15, unit: 'MINUTES') {
                    httpRequest url: "https://lpg-dcs/deployservice/cicd/api/$APP_NAME", timeout: 900000
                }
            }
        }
        }
        stage('deploying to BHEL') {
            steps {
                catchError(buildResult: 'SUCCESS', stageResult: 'FAILURE') {
                timeout(time: 15, unit: 'MINUTES') {
                    httpRequest url: "https://rmds.bhel.in/deployservice/cicd/mqtt/$APP_NAME", timeout: 900000
                }
            }
        }
        } */
    }
    post {
        failure {
            script {
                def mailAddresses = readFile("${env.WORKSPACE}/devops-mail.txt").trim().split("\\s*,\\s*")
                if (mailAddresses) {
                    emailext body: '${BUILD_LOG, maxLines=1000, escapeHtml=false}', 
                        subject: "deployment failed for ${currentBuild.fullDisplayName}", 
                        to: mailAddresses.join(',')
                }
            }
        }
    } 
}  
