#!/usr/bin/env bash

set -e -u

[[ -d $PWD/maven && ! -d $HOME/.m2 ]] && ln -s $PWD/maven $HOME/.m2

r2dbc_postgresql_artifactory=$(pwd)/r2dbc-postgresql-artifactory

rm -rf $HOME/.m2/repository/io/r2dbc 2> /dev/null || :

cd r2dbc-postgresql
./mvnw deploy \
    -DaltDeploymentRepository=distribution::default::file://${r2dbc_postgresql_artifactory}
