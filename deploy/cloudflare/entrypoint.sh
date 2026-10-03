#!/bin/sh
set -eu

if [ "${DATABASE_AUTO_MIGRATE:-true}" = true ]; then
  if ! /app/bin/bors eval 'BorsNG.Database.Migrate.up()' > /tmp/bors-startup.log 2>&1; then
    echo "Bors migration failed; keeping diagnostic container running" >&2
    sleep 900
    exit 1
  fi
fi

if ! /app/bin/bors start >> /tmp/bors-startup.log 2>&1; then
  echo "Bors application failed; keeping diagnostic container running" >&2
  sleep 900
  exit 1
fi
