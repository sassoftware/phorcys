#!/usr/bin/env bash

# Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

# Generates a throwaway CA and a server certificate for the TLS integration
# environment. Output goes to ./certs (git-ignored). Test use only.

set -euo pipefail

cd "$(dirname "$0")"
out=certs
mkdir -p "$out"

openssl genrsa -out "$out/ca.key" 2048
openssl req -x509 -new -key "$out/ca.key" -sha256 -days 30 \
	-subj "/CN=phorcys-test-ca" -out "$out/ca.pem"

openssl genrsa -out "$out/server.key" 2048
openssl req -new -key "$out/server.key" -subj "/CN=rabbitmq" -out "$out/server.csr"

cat >"$out/server.ext" <<EOF
subjectAltName = DNS:localhost, DNS:rabbitmq1, DNS:rabbitmq2, DNS:rabbitmq3, IP:127.0.0.1
extendedKeyUsage = serverAuth
EOF

openssl x509 -req -in "$out/server.csr" -CA "$out/ca.pem" -CAkey "$out/ca.key" \
	-CAcreateserial -days 30 -sha256 -extfile "$out/server.ext" -out "$out/server.pem"

# The rabbitmq user inside the container must be able to read the bind-mounted key.
chmod 644 "$out"/*.pem "$out"/*.key
rm -f "$out/server.csr" "$out/server.ext" "$out/ca.srl"

echo "Wrote certificates to $(pwd)/$out"
