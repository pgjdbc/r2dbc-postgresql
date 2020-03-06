#!/bin/bash -x

set -euo pipefail

#
# Stage on Maven Central
#
echo 'Staging on Maven Central...'

GNUPGHOME=/tmp/gpghome
export GNUPGHOME

mkdir $GNUPGHOME
cp $KEYRING $GNUPGHOME

MAVEN_OPTS="-Duser.name=jenkins -Duser.home=/tmp/jenkins-home" ./mvnw \
    -s settings.xml \
    -P${PROFILE} \
    -Dmaven.test.skip=true \
    -Dgpg.passphrase=${PASSPHRASE} \
    -Dgpg.secretKeyring=${GNUPGHOME}/secring.gpg \
    clean deploy -B
