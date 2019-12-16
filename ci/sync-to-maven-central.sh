#!/bin/bash

set -e -u

buildName=`jq -r '.name' build-info.json`
buildNumber=`jq -r '.number' build-info.json`
groupId=`jq -r '.modules[0].id' build-info.json | sed 's/\(.*\):.*:.*/\1/'`
version=`jq -r '.modules[0].id' build-info.json | sed 's/.*:.*:\(.*\)/\1/'`

echo "Syncing ${buildName}/${buildNumber}/${groupId}/${version} to Maven Central..."

curl \
		-s \
		--connect-timeout 240 \
		--max-time 2700 \
		-u ${BINTRAY_USR}:${BINTRAY_PSW} \
		-H 'Content-Type: application/json' \
		-d "{ \"username\": \"${SONATYPE_USR}\", \"password\": \"${SONATYPE_PSW}\"}" \
		-f \
		-X \
		POST "https://api.bintray.com/maven_central_sync/spring/jars/${groupId}/versions/${version}" > /dev/null || { echo "Failed to sync" >&2; exit 1; }

echo "Sync complete"
