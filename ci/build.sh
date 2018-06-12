#!/usr/bin/env bash


[[ -d $PWD/maven && ! -d $HOME/.m2 ]] && ln -s $PWD/maven $HOME/.m2
[[ -d $PWD/postgresql && ! -d $HOME/.embedpostgresql ]] && ln -s $PWD/postgresql $HOME/.embedpostgresql

repository=$(pwd)/r2dbc-postgresql-artifactory

chown -R test:test /tmp/build

cd r2dbc-postgresql
exec sudo -u test -E ./mvnw deploy -DaltDeploymentRepository=distribution::default::file://${repository}
