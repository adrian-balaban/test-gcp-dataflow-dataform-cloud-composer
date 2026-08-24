#!/usr/bin/env bash
# Generate the throwaway OpenPGP keypair the extraction lane encrypts with and
# the file_processor decrypts with. This is NOT a production key — it exists so
# the prototype can prove the archive → gzip → PGP round-trip end-to-end locally.
#
# The keyring lives at $PGP_HOME (default local/keys) and is shared verbatim by:
#   - the Java Extractor App  (apps/common Pgp.java:  gpg --homedir $PGP_HOME)
#   - the Python file_processor (python-gnupg:        gnupg.GPG(gnupghome=$PGP_HOME))
#
# On real GCP the private key never lands on a worker — see docs/runbook-gcp.md.
set -euo pipefail

KEY_DIR="${PGP_HOME:-local/keys}"
RECIPIENT="${PGP_RECIPIENT:-mig-prototype@example.invalid}"

mkdir -p "$KEY_DIR"
chmod 700 "$KEY_DIR"
export GNUPGHOME="$KEY_DIR"

# Idempotent: if a usable key for this recipient already exists, leave it.
if gpg --batch --list-keys "$RECIPIENT" >/dev/null 2>&1; then
  echo "pgp key for $RECIPIENT already present in $KEY_DIR — nothing to do"
  exit 0
fi

batch="$(mktemp)"
trap 'rm -f "$batch"' EXIT
cat > "$batch" <<EOF
%no-protection
Key-Type: RSA
Key-Length: 2048
Subkey-Type: RSA
Subkey-Length: 2048
Name-Real: MIG 000001-1 Prototype
Name-Email: $RECIPIENT
Expire-Date: 0
%commit
EOF

gpg --batch --generate-key "$batch"

gpg --batch --yes --export --armor "$RECIPIENT"       > "$KEY_DIR/pubkey.asc"
gpg --batch --yes --export-secret-keys --armor "$RECIPIENT" > "$KEY_DIR/seckey.asc"
chmod 600 "$KEY_DIR/seckey.asc"

echo "pgp keypair generated for $RECIPIENT in $KEY_DIR (pubkey.asc, seckey.asc)"