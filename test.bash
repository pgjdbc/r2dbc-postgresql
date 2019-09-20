#!/usr/bin/env bash

docker run -it \
  -e "POSTGRES_USER=test" \
  -e "POSTGRES_PASSWORD=test" \
  -e "POSTGRES_DB=test" \
  --mount type=bind,source="$(pwd)/src/test/resources/server.crt",target=/var/server.crt \
  --mount type=bind,source="$(pwd)/src/test/resources/server.key",target=/var/server.key \
  --mount type=bind,source="$(pwd)/src/test/resources/client.crt",target=/var/client.crt \
  --mount type=bind,source="$(pwd)/src/test/resources/pg_hba.conf",target=/var/pg_hba.conf \
  --mount type=bind,source="$(pwd)/src/test/resources/setup.sh",target=/var/setup.sh \
  --mount type=bind,source="$(pwd)/src/test/resources/test-db-init-script.sql",target=/docker-entrypoint-initdb.d/test-db-init-script.sql \
  postgres:11.1 \
  -c 'ssl=on' \
  -c 'ssl_key_file=/var/server.key' \
  -c 'ssl_cert_file=/var/server.crt' \
  -c 'ssl_ca_file=/var/client.crt' \
  -c 'hba_file=/var/pg_hba.conf'
