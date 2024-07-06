#!/usr/bin/env bash

# https://github.com/docker-library/postgres/blob/master/Dockerfile-alpine.template
chmod 0600 "$(pwd)/src/test/resources/server.key"

docker run --rm -it \
  --user 70:70 \
  -e "POSTGRES_USER=test" \
  -e "POSTGRES_PASSWORD=test" \
  -e "POSTGRES_DB=test" \
  --mount type=bind,source="$(pwd)/src/test/resources/server.crt",target=/var/server.crt,readonly \
  --mount type=bind,source="$(pwd)/src/test/resources/server.key",target=/var/server.key \
  --mount type=bind,source="$(pwd)/src/test/resources/client.crt",target=/var/client.crt,readonly \
  --mount type=bind,source="$(pwd)/src/test/resources/pg_hba.conf",target=/var/pg_hba.conf,readonly \
  --mount type=bind,source="$(pwd)/src/test/resources/setup.sh",target=/var/setup.sh,readonly \
  --mount type=bind,source="$(pwd)/src/test/resources/test-db-init-script.sql",target=/docker-entrypoint-initdb.d/test-db-init-script.sql,readonly \
  postgres:16-alpine \
  -c 'ssl=on' \
  -c 'ssl_key_file=/var/server.key' \
  -c 'ssl_cert_file=/var/server.crt' \
  -c 'ssl_ca_file=/var/client.crt' \
  -c 'hba_file=/var/pg_hba.conf'
