#!/bin/bash

set -e -u

buildName=`jq -r '.name' build-info.json`
buildNumber=`jq -r '.number' build-info.json`
groupId=`jq -r '.modules[0].id' build-info.json | sed 's/\(.*\):.*:.*/\1/'`
version=`jq -r '.modules[0].id' build-info.json | sed 's/.*:.*:\(.*\)/\1/'`

echo "Promoting ${buildName}/${buildNumber}/${groupId}/${version} to libs-release-local"

curl \
	-s \
	--connect-timeout 240 \
	--max-time 2700 \
	-u ${ARTIFACTORY_USR}:${ARTIFACTORY_PSW} \
	-H 'Content-type:application/json' \
	-d '{"sourceRepos": ["libs-release-local"], "targetRepo" : "spring-distributions", "async":"true"}' \
	-f \
	-X \
	POST "https://repo.spring.io/api/build/distribute/${buildName}/${buildNumber}" > /dev/null || { echo "Failed to distribute" >&2; exit 1; }

echo "Waiting for artifacts to be published"

ARTIFACTS_PUBLISHED=false
WAIT_TIME=10
COUNTER=0

while [ $ARTIFACTS_PUBLISHED == "false" ] && [ $COUNTER -lt 120 ]; do

	result=$( curl -s https://api.bintray.com/packages/spring/jars/"${groupId}" )
	versions=$( echo "$result" | jq -r '.versions' )
	exists=$( echo "$versions" | grep "$version" -o || true )

	if [ "$exists" = "$version" ]; then
		ARTIFACTS_PUBLISHED=true
	fi
	
	COUNTER=$(( COUNTER + 1 ))
	sleep $WAIT_TIME

done
