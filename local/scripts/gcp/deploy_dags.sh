#!/usr/bin/env bash
# Sync composer/dags/ into the Cloud Composer DAG bucket.
#
# This is the `make deploy-dags` step. Composer polls its DAG bucket, so a plain
# `gcloud storage rsync` is enough to make the DAG appear — no Composer API call
# or restart is required for new/changed DAG files.
#
# Requires Composer to be enabled (terraform apply -var=enable_composer=true). The
# DAG bucket name comes from the `composer_dag_bucket` terraform output; if that is
# empty, Composer is not provisioned and this script refuses to run.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
cd "$ROOT"

# Project id, billing account and key file come from the environment — see _env.sh.
# shellcheck source=local/scripts/gcp/_env.sh
source local/scripts/gcp/_env.sh
require_gcp_env TF_VAR_billing_account

DAG_BUCKET="$(terraform -chdir="$TF_DIR" output -raw composer_dag_bucket 2>/dev/null || true)"
if [[ -z "$DAG_BUCKET" ]]; then
  echo "error: composer_dag_bucket terraform output is empty." >&2
  echo "       Composer is not provisioned. Run:" >&2
  echo "         terraform -chdir=$TF_DIR apply -var=enable_composer=true -var=create_project=false" >&2
  echo "       first, then re-run make deploy-dags." >&2
  exit 1
fi

# The output is the gs:// URL prefix the DAGs live under (…/dags). rsync the local
# composer/dags dir into it. --delete keeps the bucket a true mirror; skip __pycache__.
echo "▶ syncing composer/dags/ → ${DAG_BUCKET}"
gcloud storage rsync --delete-unmatched-destination-objects --recursive --exclude=".*__pycache__.*" \
  composer/dags/ "${DAG_BUCKET}"

echo "✓ DAGs deployed. Composer will pick up changes within its poll interval."