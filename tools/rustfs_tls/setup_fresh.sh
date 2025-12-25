#!/bin/bash
set -e

# Always anchor to the script's directory
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd "$DIR"

echo "[1/5] Cleaning existing RustFS environment..."
docker compose down -v || true
rm -rf data certs
mkdir -p certs data

echo "[2/5] Generating fresh TLS certificates..."
# Official RustFS requirements: rustfs_cert.pem and rustfs_key.pem
openssl req -x509 -newkey rsa:4096 -keyout certs/rustfs_key.pem -out certs/rustfs_cert.pem -days 365 -nodes -subj "/CN=localhost"
# Ensure permissions for the 'rustfs' user inside the container
chmod 644 certs/rustfs_cert.pem
chmod 600 certs/rustfs_key.pem

echo "[3/5] Starting RustFS..."
docker compose up -d

echo "[4/5] Waiting for HTTPS health (https://localhost:9999)..."
MAX_RETRIES=30
COUNT=0
until curl -k -s https://localhost:9999 > /dev/null; do
    echo -n "."
    sleep 1
    COUNT=$((COUNT+1))
    if [ $COUNT -ge $MAX_RETRIES ]; then
        echo -e "\nERROR: RustFS failed to start or TLS is not responding."
        docker compose logs
        exit 1
    fi
done
echo " Online!"

echo "[5/5] Uploading fixtures via PyArrow/Boto3..."
export AWS_ACCESS_KEY_ID=rustfsadmin
export AWS_SECRET_ACCESS_KEY=rustfsadmin
export AWS_DEFAULT_REGION=us-east-1

python3 <<EOF
import boto3
import os
from botocore.client import Config

s3 = boto3.client(
    's3',
    endpoint_url='https://localhost:9999',  # Use HTTPS
    aws_access_key_id='rustfsadmin',
    aws_secret_access_key='rustfsadmin',
    config=Config(signature_version='s3v4'),
    verify=False
)

bucket = 'zpq-ci'
try:
    s3.create_bucket(Bucket=bucket)
    print(f"Created bucket: {bucket}")
except Exception as e:
    print(f"Bucket might already exist: {e}")

fixture_path = os.path.abspath(os.path.join('$DIR', '../../ci/fixtures/parquet/simple.parquet'))
s3.upload_file(fixture_path, bucket, 'simple.parquet')
print(f"Uploaded simple.parquet to {bucket}")
EOF

echo "SUCCESS: RustFS is ready for ZPQ benchmarking."

