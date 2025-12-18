#!/bin/bash
set -e

# Directory for certs
CERT_DIR="./certs"
mkdir -p "$CERT_DIR"

# Generate a self-signed CA and Server Cert
# We'll use a simple approach with OpenSSL
echo "Generating Self-Signed Certificate for localhost..."

# 1. Private Key
openssl genrsa -out "$CERT_DIR/private.key" 2048

# 2. Public Cert (valid for localhost and 127.0.0.1)
openssl req -new -x509 -nodes -sha256 -days 365 \
  -key "$CERT_DIR/private.key" \
  -out "$CERT_DIR/public.crt" \
  -subj "/C=US/ST=State/L=City/O=ZPQ/CN=localhost" \
  -addext "subjectAltName = DNS:localhost,IP:127.0.0.1"

chmod 644 "$CERT_DIR/private.key"
chmod 644 "$CERT_DIR/public.crt"

echo "Certificates generated in $CERT_DIR"

