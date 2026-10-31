#!/usr/bin/env bash
# Creates a private certificate authority and a server certificate for HTTPS, in ./certs (git-ignored).
#   tools/make-cert.sh 10.42.75.146 [extra-host-name ...]
# Browsers warn about this certificate until the CA (certs/ca.crt, also served at /guides-upload-ca.crt)
# is trusted on the computer, or until it is replaced by a certificate from Adobe IT (same file names).
set -euo pipefail
cd "$(dirname "$0")/.."
[ $# -ge 1 ] || { echo "usage: $0 <ip-or-host> [more names...]" >&2; exit 2; }
mkdir -p certs && chmod 700 certs
cd certs

if [ ! -f ca.key ]; then
    openssl genrsa -out ca.key 4096 2>/dev/null
    openssl req -x509 -new -key ca.key -sha256 -days 3650 -subj "/CN=Guides Upload Utility CA" -out ca.crt
fi

san=""
for name in "$@"; do
    if [[ "$name" =~ ^[0-9.]+$ ]]; then san="${san:+$san,}IP:$name"; else san="${san:+$san,}DNS:$name"; fi
done
openssl genrsa -out server.key 2048 2>/dev/null
openssl req -new -key server.key -subj "/CN=$1" -out server.csr
printf 'basicConstraints=CA:FALSE\nkeyUsage=digitalSignature,keyEncipherment\nextendedKeyUsage=serverAuth\nsubjectAltName=%s\n' "$san" > server.ext
openssl x509 -req -in server.csr -CA ca.crt -CAkey ca.key -CAcreateserial -days 825 -sha256 -extfile server.ext -out server.crt 2>/dev/null
cat server.crt ca.crt > server-chain.crt
rm -f server.csr server.ext
chmod 600 ca.key server.key
echo "certs/server-chain.crt + certs/server.key for: $san (valid 825 days)"
