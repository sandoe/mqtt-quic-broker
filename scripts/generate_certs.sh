#!/usr/bin/env bash
# generate_certs.sh — Generates a self-signed CA, server, and optional client certificates
# for the MQTT over QUIC broker (TLS 1.3, suitable for IIoT mTLS).
set -euo pipefail

CERTS_DIR="${1:-certs}"
mkdir -p "$CERTS_DIR"

echo "Generating certificates in $CERTS_DIR ..."

# --- CA certificate ---
openssl req -x509 -newkey ec \
  -pkeyopt ec_paramgen_curve:P-256 \
  -keyout "$CERTS_DIR/ca.key" \
  -out "$CERTS_DIR/ca.crt" \
  -days 3650 \
  -nodes \
  -subj "/CN=MQTT-QUIC-CA/O=IIoT Broker/C=DK" \
  -addext "basicConstraints=critical,CA:true"

echo "CA certificate: $CERTS_DIR/ca.crt"

# --- Server certificate ---
openssl req -newkey ec \
  -pkeyopt ec_paramgen_curve:P-256 \
  -keyout "$CERTS_DIR/server.key" \
  -out "$CERTS_DIR/server.csr" \
  -nodes \
  -subj "/CN=localhost/O=IIoT Broker/C=DK"

openssl x509 -req \
  -in "$CERTS_DIR/server.csr" \
  -CA "$CERTS_DIR/ca.crt" \
  -CAkey "$CERTS_DIR/ca.key" \
  -CAcreateserial \
  -out "$CERTS_DIR/server.crt" \
  -days 365 \
  -extfile <(printf "subjectAltName=DNS:localhost,IP:127.0.0.1\nextendedKeyUsage=serverAuth")

echo "Server certificate: $CERTS_DIR/server.crt"

# --- Client certificate (for mTLS) ---
openssl req -newkey ec \
  -pkeyopt ec_paramgen_curve:P-256 \
  -keyout "$CERTS_DIR/client.key" \
  -out "$CERTS_DIR/client.csr" \
  -nodes \
  -subj "/CN=mqtt-client/O=IIoT Device/C=DK"

openssl x509 -req \
  -in "$CERTS_DIR/client.csr" \
  -CA "$CERTS_DIR/ca.crt" \
  -CAkey "$CERTS_DIR/ca.key" \
  -CAcreateserial \
  -out "$CERTS_DIR/client.crt" \
  -days 365 \
  -extfile <(printf "extendedKeyUsage=clientAuth")

echo "Client certificate: $CERTS_DIR/client.crt"

# Clean up CSRs
rm -f "$CERTS_DIR"/*.csr

echo ""
echo "Done! Generated files:"
ls -la "$CERTS_DIR"
echo ""
echo "Quick start:"
echo "  ./mqtt-quic-broker --config configs/broker.yaml"
