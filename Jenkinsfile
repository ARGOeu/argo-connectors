pipeline {
    agent any
    options {
        checkoutToSubdirectory('argo-connectors')
    }
    environment {
        PROJECT_DIR="argo-connectors"
        GIT_COMMIT=sh(script: "cd ${WORKSPACE}/$PROJECT_DIR && git log -1 --format=\"%H\"",returnStdout: true).trim()
        GIT_COMMIT_HASH=sh(script: "cd ${WORKSPACE}/$PROJECT_DIR && git log -1 --format=\"%H\" | cut -c1-7",returnStdout: true).trim()
        GIT_COMMIT_DATE=sh(script: "date -d \"\$(cd ${WORKSPACE}/$PROJECT_DIR && git show -s --format=%ci ${GIT_COMMIT_HASH})\" \"+%Y%m%d%H%M%S\"",returnStdout: true).trim()
    }
    stages {
        stage ('Execute tests') {
            agent {
                docker {
                    image 'argo.registry:5000/epel-9-ams'
                    alwaysPull true
                    args '-u jenkins:jenkins -v /dev/log:/dev/log'
                }
            }
            steps {
                sh '''
                    cd $WORKSPACE/$PROJECT_DIR/
                    make wheel-devel
                    make clean
                    poetry run pip install *.whl
                    poetry run coverage run -m xmlrunner discover --output-file junit.xml -v tests/
                    poetry run coverage xml
                '''
                cobertura coberturaReportFile: '**/coverage.xml'
                junit '**/junit.xml'
            }
        }
    }
    post {
        always {
            cleanWs()
        }
        success {
            script{
                if ( env.BRANCH_NAME == 'master' || env.BRANCH_NAME == 'devel' ) {
                    slackSend( message: ":rocket: New version for <$BUILD_URL|$PROJECT_DIR>:$BRANCH_NAME Job: $JOB_NAME !")
                }
            }
        }
        failure {
            script{
                if ( env.BRANCH_NAME == 'master' || env.BRANCH_NAME == 'devel' ) {
                    slackSend( message: ":rain_cloud: Build Failed for <$BUILD_URL|$PROJECT_DIR>:$BRANCH_NAME Job: $JOB_NAME")
                }
            }
        }
    }
}
