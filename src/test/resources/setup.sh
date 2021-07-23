#!/bin/sh

whoami

mkdir -p /var/runtime

echo "Copying certificates and pg_hba..."
cp /var/server.key /var/runtime
cp /var/server.crt /var/runtime
cp /var/client.crt /var/runtime
cp /var/pg_hba.conf /var/runtime

chown postgres:postgres /var/runtime/*
ls -l /var/runtime

/usr/local/bin/docker-entrypoint.sh postgres \
  -c 'wal_level=logical' \
  -c 'wal_keep_size=4' \
  -c 'max_replication_slots=4' \
  -c 'ssl=on' \
  -c 'ssl_key_file=/var/runtime/server.key' \
  -c 'ssl_cert_file=/var/runtime/server.crt' \
  -c 'ssl_ca_file=/var/runtime/client.crt' \
  -c 'hba_file=/var/runtime/pg_hba.conf'
