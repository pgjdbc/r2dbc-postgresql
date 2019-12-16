#!/bin/bash

set -euo pipefail

MAVEN_OPTS="-Duser.name=jenkins -Duser.home=/tmp/r2dbc-maven-repository" ./mvnw -P${PROFILE} clean dependency:list test -Dsort -B
