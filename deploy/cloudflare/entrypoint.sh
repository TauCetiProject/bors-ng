#!/bin/sh
set -eu

if [ "${DATABASE_AUTO_MIGRATE:-true}" = true ]; then
  /app/bin/bors eval 'BorsNG.Database.Migrate.up()'
fi

exec /app/bin/bors start
