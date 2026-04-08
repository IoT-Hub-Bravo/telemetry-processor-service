#!/usr/bin/env bash
set -e

: "${DB_HOST:?DB_HOST is not set}"
: "${DB_PORT:?DB_PORT is not set}"

MAX_RETRIES=10
RETRY_DELAY=3

echo "Waiting for DNS..."

until getent hosts "$DB_HOST" > /dev/null 2>&1; do
  echo "DNS not ready for $DB_HOST..."
  sleep 2
done

echo "Waiting for database at $DB_HOST:$DB_PORT..."

for i in $(seq 1 $MAX_RETRIES); do
  if nc -z "$DB_HOST" "$DB_PORT"; then
    echo "Database is up"
    break
  fi

  if [ "$i" -eq "$MAX_RETRIES" ]; then
    echo "Database is still unavailable after $MAX_RETRIES attempts"
    exit 1
  fi

  echo "Attempt $i failed, retrying in $RETRY_DELAY seconds..."
  sleep $RETRY_DELAY
done

echo "Starting application..."
exec "$@"