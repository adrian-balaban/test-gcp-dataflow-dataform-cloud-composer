#!/usr/bin/env bash
# Push the real secret payloads into the Secret Manager entries Terraform declared.
#
# This is the `make seed-secrets` step. Terraform creates the secret *containers*
# (terraform/modules/secrets) with the right accessors but no versions — putting a
# private key in a .tfvars file would move the sensitive-data problem into state, so
# the values are pushed separately here. Re-running adds a new version; the latest
# version is what the pipelines read.
#
# Secrets and their sources:
#   pgp-private-key        local/keys/seckey.asc          (the throwaway prototype key)
#   pgp-passphrase          empty (the prototype key is generated with %no-protection)
#   target-system-credentials  $TARGET_SYSTEM_CREDENTIALS env   (JSON blob for the loader)
#   dataform-git-token      $DATAFORM_GIT_TOKEN env       (PAT, only if a git remote is used)
#
# Authentication: GOOGLE_APPLICATION_CREDENTIALS (the SA key) must be exported; the SA
# needs roles/secretmanager.admin (or the owner/editor it inherits) to add versions.
set -euo pipefail

# Temp files written for secret payloads are removed on exit. Appended to as each
# secret is seeded, so adding a secret is a one-line change — not a re-listing of every
# prior file in a fresh `trap` (which replaces, not appends to, the previous one).
TMP_FILES=()
trap 'rm -f "${TMP_FILES[@]}" 2>/dev/null || true' EXIT

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
cd "$ROOT"

# Project id, billing account and key file come from the environment — see _env.sh.
# shellcheck source=local/scripts/gcp/_env.sh
source local/scripts/gcp/_env.sh
require_gcp_env
PROJECT="${TF_VAR_project_id}"

# add a version to a secret. If the secret container is missing (terraform not applied
# for secrets), create it first so this script is usable standalone too.
seed() {
  local name="$1" data_file="$2" desc="$3"
  if ! gcloud secrets describe "$name" --project="$PROJECT" >/dev/null 2>&1; then
    echo "▶ secret ${name} missing — creating container"
    gcloud secrets create "$name" --project="$PROJECT" \
      --replication-policy="automatic" --description="$desc"
  fi
  gcloud secrets versions add "$name" --data-file="$data_file" --project="$PROJECT"
  echo "✓ ${name}: new version added"
}

# pgp-private-key — the ASCII-armored private key from the local keyring.
if [[ ! -f local/keys/seckey.asc ]]; then
  echo "error: local/keys/seckey.asc not found. Run local/scripts/gen_pgp_key.sh first." >&2
  exit 1
fi
seed "pgp-private-key" "local/keys/seckey.asc" "OpenPGP private key for extraction bundles"

# pgp-passphrase — the prototype key has no passphrase (%no-protection). Secret Manager
# rejects an empty payload (INVALID_ARGUMENT), so push a single newline; readers strip
# it and a passphrase is ignored for a %no-protection key anyway.
printf '\n' > /tmp/mig-empty-passphrase
TMP_FILES+=(/tmp/mig-empty-passphrase)
seed "pgp-passphrase" "/tmp/mig-empty-passphrase" "Passphrase for the PGP private key (empty: %no-protection)"

# target-system-credentials — provided by the operator as an env var (JSON blob the loader
# posts to Target System). Never committed.
if [[ -z "${TARGET_SYSTEM_CREDENTIALS:-}" ]]; then
  echo "⚠ TARGET_SYSTEM_CREDENTIALS not set — skipping target-system-credentials" >&2
  echo "  set it (the JSON the loader app posts to Target System) and re-run to seed." >&2
else
  printf '%s' "$TARGET_SYSTEM_CREDENTIALS" > /tmp/mig-target-system-credentials
  TMP_FILES+=(/tmp/mig-target-system-credentials)
  seed "target-system-credentials" "/tmp/mig-target-system-credentials" "Target System loader client credentials"
fi

# dataform-git-token — only needed when the Dataform repo is linked to a git remote.
if [[ -z "${DATAFORM_GIT_TOKEN:-}" ]]; then
  echo "⚠ DATAFORM_GIT_TOKEN not set — skipping dataform-git-token" >&2
  echo "  set it (a PAT with read access to the Dataform models repo) and re-run if needed." >&2
else
  printf '%s' "$DATAFORM_GIT_TOKEN" > /tmp/mig-dataform-git-token
  TMP_FILES+=(/tmp/mig-dataform-git-token)
  seed "dataform-git-token" "/tmp/mig-dataform-git-token" "PAT for the Dataform git remote"
fi

echo
echo "✓ secrets seeded for project ${PROJECT}."