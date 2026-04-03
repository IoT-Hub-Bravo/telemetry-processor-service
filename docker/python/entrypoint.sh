#!/usr/bin/env bash
set -e

MAX_RETRIES=10
RETRY_DELAY=3

echo "Waiting for database..."

for i in $(seq 1 $MAX_RETRIES); do
  if nc -z "$DB_HOST" "$DB_PORT"; then
    echo "Database is up"
    break
  fi
  echo "Attempt $i failed, retrying in $RETRY_DELAY seconds..."
  sleep $RETRY_DELAY
done

echo "Running migrations..."
alembic upgrade head || {
  echo "Migration failed"
  exit 1
}

echo "Starting application..."
exec "$@"