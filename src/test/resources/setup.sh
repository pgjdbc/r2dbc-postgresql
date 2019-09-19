#!/bin/sh

echo "Copying server.crt..."

chmod 0600 /var/server.crt
chown postgres:postgres /var/server.crt

echo "Copying server.key..."

chmod 0600 /var/server.key
chown postgres:postgres /var/server.key

echo "Copying client.crt..."

chmod 0600 /var/client.crt
chown postgres:postgres /var/client.crt

ls -l /tmp

postgres \
  -c 'ssl=on' \
  -c 'ssl_key_file=/var/server.key' \
  -c 'ssl_cert_file=/var/server.crt' \
  -c 'ssl_ca_file=/var/client.crt' \
  -c 'hba_file=/var/pg_hba.conf' \

