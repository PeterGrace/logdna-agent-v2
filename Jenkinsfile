library 'magic-butler-catalogue'
def PROJECT_NAME = 'logdna-agent-v2'
def TRIGGER_PATTERN = '.*@logdnabot.*'
def publishGCRImage = false
def publishDockerhubICRImages = false

pipeline {
    agent any
    options {
        timeout time: 1, unit: 'HOURS'
        timestamps()
        ansiColor 'xterm'
    }
    triggers {
        issueCommentTrigger(TRIGGER_PATTERN)
        cron(env.BRANCH_NAME ==~ /\d\.\d/ ? 'H 8 * * 1' : '')
    }
    environment {
        RUST_IMAGE_REPO = 'us.gcr.io/logdna-k8s/rust'
        RUST_IMAGE_TAG = 'buster-1-stable'
        SCCACHE_BUCKET = 'logdna-sccache-us-west-2'
        SCCACHE_REGION = 'us-west-2'
        CARGO_INCREMENTAL = 'false'
    }
    stages {
        stage('Validate PR Source') {
          when {
            expression { env.CHANGE_FORK }
            not {
                triggeredBy 'issueCommentCause'
            }
          }
          steps {
            error("A maintainer needs to approve this PR for CI by commenting")
          }
        }
        stage('Pull Build Image') {
            steps {
                sh "docker pull ${RUST_IMAGE_REPO}:${RUST_IMAGE_TAG}"
            }
        }
        stage('Lint and Test') {
            environment {
                CREDS_FILE = credentials('pipeline-e2e-creds')
                LOGDNA_HOST = "logs.use.stage.logdna.net"
            }
            parallel {
                stage('Lint, Unit and Integration Tests'){
                    steps {
                        script {
                            def creds = readJSON file: CREDS_FILE
                            // Assumes the pipeline-e2e-creds format remains the same. Chase
                            // refer to the e2e tests's README's authorization docs for the
                            // current structure
                            LOGDNA_INGESTION_KEY = creds["packet-stage"]["account"]["ingestionkey"]
                        }
                        withCredentials([[
                                                 $class: 'AmazonWebServicesCredentialsBinding',
                                                 credentialsId: 'aws',
                                                 accessKeyVariable: 'AWS_ACCESS_KEY_ID',
                                                 secretKeyVariable: 'AWS_SECRET_ACCESS_KEY'
                                         ]]){
                            sh """
                        make lint
                        make test
                        make integration-test LOGDNA_INGESTION_KEY=${LOGDNA_INGESTION_KEY}
                    """
                        }
                    }
                    post {
                        success {
                            sh "make clean"
                        }
                    }
                }
                stage('Run K8s Integration Tests') {
                    steps {
                        catchError(buildResult: 'SUCCESS', stageResult: 'FAILURE') {
                            withCredentials([[
                                              $class: 'AmazonWebServicesCredentialsBinding',
                                              credentialsId: 'aws',
                                              accessKeyVariable: 'AWS_ACCESS_KEY_ID',
                                              secretKeyVariable: 'AWS_SECRET_ACCESS_KEY'
                                             ]]) {
                                sh '''
                                    make k8s-test
                                '''
                            }
                        }
                    }
                }
            }
        }
        stage('Build Release Binaries') {
            environment {
                CREDS_FILE = credentials('pipeline-e2e-creds')
                LOGDNA_HOST = "logs.use.stage.logdna.net"
            }
            parallel {
                stage('Build Release Image') {
                    steps {
                        withCredentials([[
                            $class: 'AmazonWebServicesCredentialsBinding',
                            credentialsId: 'aws',
                            accessKeyVariable: 'AWS_ACCESS_KEY_ID',
                            secretKeyVariable: 'AWS_SECRET_ACCESS_KEY'
                        ]]){
                            sh """
                                echo "[default]" > ${PWD}/.aws_creds
                                echo "AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID}" >> ${PWD}/.aws_creds
                                echo "AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY}" >> ${PWD}/.aws_creds
                                make build-image AWS_SHARED_CREDENTIALS_FILE=${PWD}/.aws_creds
                            """
                        }
                    }
                    post {
                        always {
                            sh "rm ${PWD}/.aws_creds"
                        }
                    }
                }
                stage('Build static release binary') {
                    steps {
                        withCredentials([[
                            $class: 'AmazonWebServicesCredentialsBinding',
                            credentialsId: 'aws',
                            accessKeyVariable: 'AWS_ACCESS_KEY_ID',
                            secretKeyVariable: 'AWS_SECRET_ACCESS_KEY'
                        ]]) {
                            sh '''
                                echo "[default]" > ${PWD}/.aws_creds_static
                                echo "AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID}" >> ${PWD}/.aws_creds_static
                                echo "AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY}" >> ${PWD}/.aws_creds_static
                                STATIC=1 FEATURES= make build-release AWS_SHARED_CREDENTIALS_FILE=${PWD}/.aws_creds_static
                                ARCH=aarch64 STATIC=1 FEATURES= make build-release AWS_SHARED_CREDENTIALS_FILE=${PWD}/.aws_creds_static
                                rm ${PWD}/.aws_creds_static
                            '''
                        }
                    }
                }
            }
        }
        stage('Check Publish Images') {
            when {
                branch pattern: "\\d\\.\\d.*", comparator: "REGEXP"
            }
            stages {
                stage('Scanning Images') {
                    steps {
                        sh 'make sysdig_secure_images'
                        sysdig engineCredentialsId: 'sysdig-secure-api-credentials', name: 'sysdig_secure_images', inlineScanning: true
                    }
                }
                stage('Publish static binary') {
                    steps {
                        withCredentials([[
                            $class: 'AmazonWebServicesCredentialsBinding',
                            credentialsId: 'aws',
                            accessKeyVariable: 'AWS_ACCESS_KEY_ID',
                            secretKeyVariable: 'AWS_SECRET_ACCESS_KEY'
                        ]]) {
                            sh '''
                                echo "[default]" > ${PWD}/.aws_creds_static
                                echo "AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID}" >> ${PWD}/.aws_creds_static
                                echo "AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY}" >> ${PWD}/.aws_creds_static
                                STATIC=1 make publish-s3-binary
                                ARCH=aarch64 STATIC=1 make publish-s3-binary
                                rm ${PWD}/.aws_creds_static
                            '''
                        }
                    }
                }
                stage('Check Publish GCR Image or Timeout') {
                    steps {
                        script {
                            publishGCRImage = true
                            if (currentBuild.getBuildCauses('hudson.triggers.TimerTrigger$TimerTriggerCause')) {
                                echo "started by timer, publishing"
                            } else {
                                echo "not started by timer"
                                try {
                                    timeout(time: 5, unit: 'MINUTES') {
                                        input(message: 'Should we publish the versioned image?')
                                    }
                                } catch (err) {
                                    publishGCRImage = false
                                }
                            }
                        }
                    }
                }
                stage('Publish GCR images') {
                    when {
                        expression { return publishGCRImage == true }
                    }
                    steps {
                        // Publish to gcr, jenkins is logged into gcr globally
                        sh 'make publish-image-gcr'
                    }
                }
                stage('Check Publish Dockerhub and ICR Image or Timeout') {
                    steps {
                        script {
                            publishDockerhubICRImages = true
                            if (currentBuild.getBuildCauses('hudson.triggers.TimerTrigger$TimerTriggerCause')) {
                                echo "started by timer, publishing"
                            } else {
                                echo "not started by timer"
                                try {
                                    timeout(time: 5, unit: 'MINUTES') {
                                        input(message: 'Should we publish the versioned images to dockerhub/icr?')
                                    }
                                } catch (err) {
                                    publishDockerhubICRImages = false
                                }
                            }
                        }
                    }
                }
                stage('Publish Dockerhub and ICR images') {
                    when {
                        expression { return publishDockerhubICRImages == true }
                    }
                    steps {
                        script {
                            // Login and publish to dockerhub
                            docker.withRegistry(
                                'https://index.docker.io/v1/',
                                'dockerhub-username-password'
                            ) {
                                sh 'make publish-image-docker'
                            }
                            // Login and publish to ibm
                            docker.withRegistry(
                                'https://icr.io',
                                'icr-iam-username-password'
                            ) {
                                sh 'make publish-image-ibm'
                            }
                        }
                    }
                }
            }
            post {
                always {
                    sh 'make clean-all'
                }
            }
        }
    }
}
