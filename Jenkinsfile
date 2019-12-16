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

		stage('Deploy to Artifactory') {
			when {
				anyOf {
					branch 'master'
					branch 'release'
				}
			}
			agent {
				docker {
					image 'adoptopenjdk/openjdk8:latest'
					args '-v $HOME/.m2:/tmp/jenkins-home/.m2'
				}
			}
			options { timeout(time: 20, unit: 'MINUTES') }

			environment {
				ARTIFACTORY = credentials('02bd1690-b54f-4c9f-819d-a77cb7a9822c')
			}

			steps {
				script {
					sh 'rm -rf ?'

					// Warm up this plugin quietly before using it.
					sh 'MAVEN_OPTS="-Duser.name=jenkins -Duser.home=/tmp/jenkins-home" ./mvnw -q org.apache.maven.plugins:maven-help-plugin:2.1.1:evaluate -Dexpression=project.version'

					// Extract project's version number
					PROJECT_VERSION = sh(
							script: 'MAVEN_OPTS="-Duser.name=jenkins -Duser.home=/tmp/jenkins-home" ./mvnw org.apache.maven.plugins:maven-help-plugin:2.1.1:evaluate -Dexpression=project.version -o | grep -v INFO',
							returnStdout: true
					).trim()

					RELEASE_TYPE = 'milestone' // .RC? or .M?

					if (PROJECT_VERSION.endsWith('SNAPSHOT')) {
						RELEASE_TYPE = 'snapshot'
					} else if (PROJECT_VERSION.endsWith('RELEASE')) {
						RELEASE_TYPE = 'release'
					}

					// Capture build output...
					OUTPUT = sh(
							script: "PROFILE=ci,${RELEASE_TYPE} ci/build.sh",
							returnStdout: true
					).trim()

					echo "$OUTPUT"

					// ...to extract artifactory build info
					build_info_path = OUTPUT.split('\n')
							.find { it.contains('Artifactory Build Info Recorder') }
							.split('Saving Build Info to ')[1]
							.trim()[1..-2]

					// Stash the JSON build info to support promotion to bintray
					dir(build_info_path + '/..') {
						stash name: 'build_info', includes: "*.json"
					}
				}
			}
		}

		stage('Promote to Bintray') {
			when {
				branch 'release'
			}
			agent {
				docker {
					image 'adoptopenjdk/openjdk8:latest'
					args '-v $HOME/.m2:/tmp/jenkins-home/.m2'
				}
			}
			options { timeout(time: 20, unit: 'MINUTES') }

			environment {
				ARTIFACTORY = credentials('02bd1690-b54f-4c9f-819d-a77cb7a9822c')
			}

			steps {
				script {
					sh 'rm -rf ?'

					// Warm up this plugin quietly before using it.
					sh 'MAVEN_OPTS="-Duser.name=jenkins -Duser.home=/tmp/jenkins-home" ./mvnw -q org.apache.maven.plugins:maven-help-plugin:2.1.1:evaluate -Dexpression=project.version'

					PROJECT_VERSION = sh(
							script: 'MAVEN_OPTS="-Duser.name=jenkins -Duser.home=/tmp/jenkins-home" ./mvnw org.apache.maven.plugins:maven-help-plugin:2.1.1:evaluate -Dexpression=project.version -o | grep -v INFO',
							returnStdout: true
					).trim()

					if (PROJECT_VERSION.endsWith('RELEASE')) {
						unstash name: 'build_info'
						sh "ci/promote-to-bintray.sh"
					} else {
						echo "${PROJECT_VERSION} is not a candidate for promotion to Bintray."
					}
				}
			}
		}

		stage('Sync to Maven Central') {
			when {
				branch 'release'
			}
			agent {
				docker {
					image 'adoptopenjdk/openjdk8:latest'
					args '-v $HOME/.m2:/tmp/jenkins-home/.m2'
				}
			}
			options { timeout(time: 20, unit: 'MINUTES') }

			environment {
				BINTRAY = credentials('Bintray-spring-operator')
				SONATYPE = credentials('oss-token')
			}

			steps {
				script {
					sh 'rm -rf ?'

					// Warm up this plugin quietly before using it.
					sh 'MAVEN_OPTS="-Duser.name=jenkins -Duser.home=/tmp/jenkins-home" ./mvnw -q org.apache.maven.plugins:maven-help-plugin:2.1.1:evaluate -Dexpression=project.version'

					PROJECT_VERSION = sh(
							script: 'MAVEN_OPTS="-Duser.name=jenkins -Duser.home=/tmp/jenkins-home" ./mvnw org.apache.maven.plugins:maven-help-plugin:2.1.1:evaluate -Dexpression=project.version -o | grep -v INFO',
							returnStdout: true
					).trim()

					if (PROJECT_VERSION.endsWith('RELEASE')) {
						unstash name: 'build_info'
						sh "ci/sync-to-maven-central.sh"
					} else {
						echo "${PROJECT_VERSION} is not a candidate for syncing to Maven Central."
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
