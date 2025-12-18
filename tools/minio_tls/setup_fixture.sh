#!/bin/bash
set -euo pipefail

# Sets up a bucket/object in the local MinIO TLS environment for zpq tests.
#
# Assumes MinIO is running via tools/minio_tls/docker-compose.yml
# and that mc container has /fixtures mounted read-only.

BUCKET="zpq-ci"
OBJECT="range_payload.bin"

cd "$(dirname "$0")"

mkdir -p ./data
mkdir -p ./certs

# Ensure TLS certs exist; without these MinIO will start as plain HTTP.
if [[ ! -f ./certs/public.crt || ! -f ./certs/private.key ]]; then
  echo "Generating TLS certs..."
  ./gen_certs.sh
fi

echo "Ensuring MinIO is up..."
# Force recreate so MinIO picks up new/changed certs.
docker-compose up -d --force-recreate

echo "Waiting for MinIO health..."
for i in $(seq 1 60); do
  if curl -k -fsS https://localhost:9000/minio/health/live >/dev/null; then
    echo "MinIO HTTPS healthy."
    break
  fi
  echo "  still starting... (${i}/60)"
  sleep 1
done

echo "Configuring mc alias..."
# Don't rely on container entrypoint timing; set it explicitly each run.
docker-compose exec -T mc mc alias set local https://minio:9000 minioadmin minioadmin --insecure

echo "Creating bucket if needed..."
docker-compose exec -T mc mc mb --ignore-existing local/${BUCKET} --insecure

echo "Setting anonymous download policy..."
docker-compose exec -T mc mc anonymous set download local/${BUCKET} --insecure || true

echo "Uploading fixture..."
docker-compose exec -T mc mc cp /fixtures/${OBJECT} local/${BUCKET}/${OBJECT} --insecure

echo "Done. Object available at: https://localhost:9000/${BUCKET}/${OBJECT}"


