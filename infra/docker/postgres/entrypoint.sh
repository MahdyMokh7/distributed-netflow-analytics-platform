#!/bin/sh
set -eu

DATA_PARENT="$(dirname "$PGDATA")"

mkdir -p "$DATA_PARENT"
mkdir -p /var/log/postgresql

chown -R postgres:postgres "$DATA_PARENT"
chown -R postgres:postgres /var/log/postgresql

exec su-exec postgres patroni /etc/patroni/patroni.yml