#!/bin/sh

if [ ! -s "$PGDATA/PG_VERSION" ]; then
  echo "*:*:*:$PG_REP_USER:$PG_REP_PASSWORD" > ~/.pgpass
  chmod 0600 ~/.pgpass
  until pg_basebackup -h "${PG_MASTER_HOST}" -p "${PG_MASTER_PORT}" -D "${PGDATA}" -U "${PG_REP_USER}" -vP -W
  do
    echo "Waiting for primary server to connect..."
    sleep 1s
  done
  echo "host replication all 0.0.0.0/0 md5" >> "$PGDATA/pg_hba.conf"
  set -e
  cat > "${PGDATA}"/standby.signal <<EOF
primary_conninfo = 'host=$PG_MASTER_HOST port=$PG_MASTER_PORT user=$PG_REP_USER password=$PG_REP_PASSWORD'
promote_trigger_file = '/tmp/promote_trigger_file'
EOF
  chown postgres. "${PGDATA}" -R
  chmod 700 "${PGDATA}" -R
fi
sed -i 's/wal_level = hot_standby/wal_level = replica/g' "${PGDATA}"/postgresql.conf
echo "ready to run"
# gosu isn't installed in alpine: https://github.com/docker-library/postgres/blob/master/Dockerfile-alpine.template
if [ -x "$(type gosu >/dev/null 2>&1)" ]; then
  exec gosu postgres postgres
else
  exec su-exec postgres postgres
fi
