#!/bin/bash
set -e
mkdir -p tools/rustfs_tls/certs
cd tools/rustfs_tls/certs

if [ ! -f rustfs_cert.pem ]; then
    echo "Generating self-signed certs for RustFS..."
    openssl req -x509 -newkey rsa:4096 -keyout rustfs_key.pem -out rustfs_cert.pem -days 365 -nodes -subj "/CN=localhost"
    cp rustfs_cert.pem rustfs.crt
    cp rustfs_key.pem rustfs.key
fi

