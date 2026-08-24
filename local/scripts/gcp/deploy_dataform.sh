#!/usr/bin/env bash
# Publish the Dataform SQLX models to the Dataform repository Terraform created.
#
# This is the `make deploy-dataform` step. Two paths, depending on whether a git
# remote was wired into the Dataform repo at apply time (root var dataform_git_remote):
#
#   * git remote set  → push dataform/ to that remote; Dataform's scheduled
#     compilation (terraform/modules/dataform, cron 0 3 * * * Europe/Bucharest) picks
#     the change up, or we trigger a one-off compilation here.
#
#   * no git remote  → the Dataform repo is "unlinked". We compile it directly with the
#     `dataform` CLI against the repo id, which is enough for a prototype. The scheduled
#     release config is skipped because it requires a git remote.
#
# The Dataform repository id comes from the `dataform_repository` terraform output
# (terraform/modules/dataform, repo "mig-000001-1").
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
cd "$ROOT"

# Project id, billing account and key file come from the environment — see _env.sh.
# shellcheck source=local/scripts/gcp/_env.sh
source local/scripts/gcp/_env.sh
require_gcp_env TF_VAR_billing_account
PROJECT="${TF_VAR_project_id}"

REPO="$(terraform -chdir="$TF_DIR" output -raw dataform_repository)"
echo "▶ Dataform repository: projects/${PROJECT}/locations/${TF_VAR_region}/repositories/${REPO}"

# Root var dataform_git_remote (default "") is not re-exported as an output, so read
# it from terraform.tfvars if present. Absent == no remote == unlinked-repo path.
GIT_REMOTE=""
if [[ -f "$TF_DIR/terraform.tfvars" ]]; then
  GIT_REMOTE="$(grep -E '^\s*dataform_git_remote\s*=' "$TF_DIR/terraform.tfvars" \
    | sed -E 's/.*=\s*"([^"]*)".*/\1/; s/.*=\s*(\S+).*/\1/' || true)"
fi

if [[ -n "$GIT_REMOTE" ]]; then
  # Linked-repo path: push to git, Dataform compiles from the configured branch.
  echo "▶ git remote configured: ${GIT_REMOTE}"
  if [[ ! -d dataform/.git ]]; then
    git -C dataform init -q
    git -C dataform remote add origin "${GIT_REMOTE}" 2>/dev/null || \
      git -C dataform remote set-url origin "${GIT_REMOTE}"
  fi
  git -C dataform add -A
  git -C dataform commit -q -m "Dataform models for ${REPO}" || true
  git -C dataform push -u origin HEAD:main
  echo "✓ pushed to git; Dataform will compile per its release config."
else
  # Unlinked-repo path: compile directly with the dataform CLI.
  if ! command -v dataform >/dev/null 2>&1; then
    echo "error: dataform CLI not installed and no git remote is configured." >&2
    echo "       Either set dataform_git_remote and re-apply, or install the CLI:" >&2
    echo "         npm install -g @dataform/cli" >&2
    exit 1
  fi
  echo "▶ no git remote — validating dataform/ with a local compile"
  # `dataform compile` is purely local in CLI 3.x (no repository/project flags); it
  # proves the SQLX is valid. Execution against BigQuery goes through
  # local/scripts/run_dataform.py (BQ_TARGET=real), which runs the compiled SQL in
  # dependency order — the Dataform *service* only executes workflows for git-linked
  # repos, which this prototype deliberately does not require.
  (cd dataform && dataform compile --json > /dev/null)
  echo "✓ dataform/ compiles clean. Execution happens via run_dataform.py (BQ_TARGET=real)."
fi