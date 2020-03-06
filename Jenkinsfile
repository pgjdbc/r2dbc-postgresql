pipeline {
	agent none

	triggers {
		pollSCM 'H/10 * * * *'
		upstream(upstreamProjects: "r2dbc-spi/master", threshold: hudson.model.Result.SUCCESS)
	}

	options {
		disableConcurrentBuilds()
		buildDiscarder(logRotator(numToKeepStr: '14'))
	}

	stages {
		stage("test: baseline (jdk8)") {
			agent {
				docker {
					image 'adoptopenjdk/openjdk8:latest'
                    args '-u root -v /var/run/docker.sock:/var/run/docker.sock' // root but with no maven caching
				}
			}
			options { timeout(time: 30, unit: 'MINUTES') }
			steps {
				sh 'PROFILE=none ci/test.sh'
                sh "chown -R 1001:1001 target"
			}
		}

		stage('Deploy') {
			when {
				anyOf {
					branch 'master'
					branch 'release'
				}
			}
			agent {
				docker {
					image 'springci/r2dbc-openjdk8-with-gpg:latest'
					args '-v $HOME/.m2:/tmp/jenkins-home/.m2'
				}
			}
			options { timeout(time: 20, unit: 'MINUTES') }

			environment {
				ARTIFACTORY = credentials('02bd1690-b54f-4c9f-819d-a77cb7a9822c')
				SONATYPE = credentials('oss-token')
				KEYRING = credentials('spring-signing-secring.gpg')
				PASSPHRASE = credentials('spring-gpg-passphrase')
			}

			steps {
				script {
					// Warm up this plugin quietly before using it.
					sh 'MAVEN_OPTS="-Duser.name=jenkins -Duser.home=/tmp/jenkins-home" ./mvnw -q org.apache.maven.plugins:maven-help-plugin:2.1.1:evaluate -Dexpression=project.version'

					// Extract project's version number
					PROJECT_VERSION = sh(
							script: 'MAVEN_OPTS="-Duser.name=jenkins -Duser.home=/tmp/jenkins-home" ./mvnw org.apache.maven.plugins:maven-help-plugin:2.1.1:evaluate -Dexpression=project.version -o | grep -v INFO',
							returnStdout: true
					).trim()

					RELEASE_TYPE = 'milestone' // .RC? or .M?

					if (PROJECT_VERSION.endsWith('SNAPSHOT')) { // .SNAPSHOT
						RELEASE_TYPE = 'snapshot'
					} else if (PROJECT_VERSION.endsWith('RELEASE') || PROJECT_VERSION ==~ /.*SR[0-9]+/) { // .RELEASE or .SR?
						RELEASE_TYPE = 'release'
					}

					if (RELEASE_TYPE == 'release') {
						sh "PROFILE=central ci/build-and-deploy-to-maven-central.sh"
						script {
							slackSend(
									color: 'warning',
									channel: '#r2dbc-dev',
									message: "WORKING ON: ${currentBuild.fullDisplayName} - `${currentBuild.currentResult}`\n${env.BUILD_URL} staged on Maven Central, awaiting final release.")
						}
					} else {
						sh "PROFILE=${RELEASE_TYPE} ci/build-and-deploy-to-artifactory.sh"
					}
				}
			}
		}
	}

	post {
		changed {
			script {
				slackSend(
						color: (currentBuild.currentResult == 'SUCCESS') ? 'good' : 'danger',
						channel: '#r2dbc-dev',
						message: "${currentBuild.fullDisplayName} - `${currentBuild.currentResult}`\n${env.BUILD_URL}")
				emailext(
						subject: "[${currentBuild.fullDisplayName}] ${currentBuild.currentResult}",
						mimeType: 'text/html',
						recipientProviders: [[$class: 'CulpritsRecipientProvider'], [$class: 'RequesterRecipientProvider']],
						body: "<a href=\"${env.BUILD_URL}\">${currentBuild.fullDisplayName} is reported as ${currentBuild.currentResult}</a>")
			}
		}
	}
}
