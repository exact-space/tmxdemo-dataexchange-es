pipeline {
    agent any  
      environment{
        registry= "dev.exactspace.co"
      }
    stages{
        stage("get scm"){
            steps{
                checkout scmGit(branches: [[name: '*/master']], extensions: [], userRemoteConfigs: [[url: 'git@github.com:exact-space/TmxDemo-dataExchange.git']])
            }
        }
        stage("building images"){
            steps{
                sh "sudo docker build --rm --no-cache -t tmxdemo-dataexchange-cron-es:r1 ." 
                }
            }   
        stage("tagging imges"){       //25.00 time stamp
            steps{
                sh  "sudo docker tag tmxdemo-dataexchange-cron-es:r1 $registry/tmxdemo-dataexchange-cron-es:r1 "
                }
            }
        stage("remove old docker image"){
            steps{
                sh  "sudo docker image remove tmxdemo-dataexchange-cron-es:r1"
                }
            }
        stage("image push"){
            steps{                    // account name/image name: version of the tah     
                sh "sudo docker push $registry/tmxdemo-dataexchange-cron-es:r1"  
                }
            }  
        stage("deploying images in DEV ENV"){
            steps{                    // account name/image name: version of the tah     
                sh "sudo docker rm -f tmxdemo-dataexchange-cron-es"
                sh "sudo docker run -d  --name tmxdemo-dataexchange-cron-es $registry/tmxdemo-dataexchange-cron-es:r1"  
            }
        }
    }   
}
