#!/bin/sh
set -eu

mkdir -p /tmp/bors-diagnostics
printf 'Bors startup diagnosis in progress\n' > /tmp/bors-diagnostics/index.html
busybox httpd -f -p 4000 -h /tmp/bors-diagnostics &

if [ "${DATABASE_AUTO_MIGRATE:-true}" = true ]; then
  if ! /app/bin/bors eval 'BorsNG.Database.Migrate.up()' > /tmp/bors-startup.log 2>&1; then
    echo "Bors migration failed; keeping diagnostic container running" >&2
    sleep 900
    exit 1
  fi
fi

if ! PORT=4001 /app/bin/bors start >> /tmp/bors-startup.log 2>&1; then
  echo "Bors application failed; keeping diagnostic container running" >&2
  sleep 900
  exit 1
fi
