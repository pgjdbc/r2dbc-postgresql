#!/bin/bash

set -euo pipefail

VERSION=$(./mvnw org.apache.maven.plugins:maven-help-plugin:2.1.1:evaluate -Dexpression=project.version -o | grep -v INFO)

if [[ $VERSION =~ [^.*-SNAPSHOT$] ]] ; then

  echo "Cannot deploy a snapshot: $VERSION"
  exit 1
fi

if [[ $VERSION =~ [^(\d+\.)+(RC(\d+)|M(\d+)|RELEASE)$] ]] ; then

  #
  # Prepare GPG Key is expected to be in base64
  # Exported with gpg -a --export-secret-keys "your@email" | base64 > gpg.base64
  #
  printf "$GPG_KEY_BASE64" | base64 --decode > gpg.asc
  echo ${GPG_PASSPHRASE} | gpg --batch --yes --passphrase-fd 0 --import gpg.asc
  gpg -k

  #
  # Stage on Maven Central
  #
  echo "Staging $VERSION to Maven Central"

  ./mvnw \
      -s settings.xml \
      -Pcentral \
      -Dmaven.test.skip=true \
      -Dgpg.passphrase=${GPG_PASSPHRASE} \
      clean deploy -B -D skipITs
else

  echo "Not a release: $VERSION"
  exit 1
fi

