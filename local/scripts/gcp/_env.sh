# Shared environment for the local/scripts/gcp/* scripts. Sourced, not executed.
#
# Deliberately carries no account identifiers: this repo is public, so the project id,
# billing account and service-account key file are supplied by the operator, never
# committed. Export them once per shell (see docs/runbook-gcp.md):
#
#   export TF_VAR_project_id=…      # e.g. mig-000001-1-dev
#   export TF_VAR_billing_account=… # an OPEN billing account id
#   export TF_VAR_region=…          # optional, defaults to europe-west1
#   export GOOGLE_APPLICATION_CREDENTIALS=/path/to/sa-key.json
#
# GOOGLE_APPLICATION_CREDENTIALS is inferred when exactly one key file sits in
# terraform/envs/dev/sa-json-keys/ (that directory is gitignored), so the common case
# still needs no extra export.

TF_DIR="terraform/envs/dev"

# The project is created manually (the service account cannot create one under
# "No organization"), so terraform must adopt it rather than recreate it. Pass these
# to apply/plan only — `terraform output` rejects -var and errors out.
TF_ARGS=(-var="create_project=false")

export TF_VAR_region="${TF_VAR_region:-europe-west1}"

if [[ -z "${GOOGLE_APPLICATION_CREDENTIALS:-}" ]]; then
  # shellcheck disable=SC2206
  _keys=($TF_DIR/sa-json-keys/*.json)
  # Absolute: `terraform -chdir=…` resolves credentials against its own directory,
  # so a repo-relative path would not be found.
  if [[ ${#_keys[@]} -eq 1 && -f "${_keys[0]}" ]]; then
    export GOOGLE_APPLICATION_CREDENTIALS="$PWD/${_keys[0]}"
  fi
  unset _keys
fi

# Fail with a usable message instead of a terraform "No value for required variable".
require_gcp_env() {
  local missing=()
  [[ -n "${TF_VAR_project_id:-}" ]] || missing+=("TF_VAR_project_id")
  for var in "$@"; do
    [[ -n "${!var:-}" ]] || missing+=("$var")
  done
  if [[ -n "${GOOGLE_APPLICATION_CREDENTIALS:-}" && ! -f "$GOOGLE_APPLICATION_CREDENTIALS" ]]; then
    echo "error: GOOGLE_APPLICATION_CREDENTIALS points at a missing file: $GOOGLE_APPLICATION_CREDENTIALS" >&2
    exit 1
  fi
  [[ -n "${GOOGLE_APPLICATION_CREDENTIALS:-}" ]] || missing+=("GOOGLE_APPLICATION_CREDENTIALS")
  if [[ ${#missing[@]} -gt 0 ]]; then
    echo "error: set these first — ${missing[*]}" >&2
    echo "       see docs/runbook-gcp.md; nothing account-specific is committed." >&2
    exit 1
  fi
  export TF_VAR_project_id
  [[ -n "${TF_VAR_billing_account:-}" ]] && export TF_VAR_billing_account
  return 0
}

# ── build helpers (sourced by build_*.sh) ─────────────────────────────────────
# Centralised so the two build scripts derive their tag by one rule rather than two
# copies of it. The 2026-08-19 retest failed because build_java_images.sh pushed
# :review-fixes while build_templates.sh stayed on the git SHA, so the DAG — one
# MIG_JAVA_IMAGE_TAG for both — asked for an image that existed under one name and not
# the other.
#
# One rule is not one value: each script calls derive_image_version in its own process at
# its own time, so running them across an edit still yields <sha> and <sha>-dirty. Only a
# single caller that derives the tag once and exports it closes that window — which is
# what `make build-images` does. Running the scripts individually is still supported;
# then pinning the tag is the operator's job (MIG_JAVA_IMAGE_TAG).

# The container CLI the build scripts use. podman and docker are interchangeable here.
detect_container_cli() {
  if command -v podman >/dev/null 2>&1; then
    echo podman
  elif command -v docker >/dev/null 2>&1; then
    echo docker
  else
    echo "error: neither podman nor docker found on PATH" >&2
    return 1
  fi
}

# The immutable image tag for this build. MIG_JAVA_IMAGE_TAG overrides the derived tag;
# without it the tag is the short SHA, suffixed -dirty on an unclean tree so a local
# experiment can never be mistaken for a committed build.
derive_image_version() {
  if [ -n "${MIG_JAVA_IMAGE_TAG:-}" ]; then
    printf '%s' "$MIG_JAVA_IMAGE_TAG"
  else
    local v
    v="$(git rev-parse --short HEAD 2>/dev/null || echo nogit)"
    if ! git diff --quiet 2>/dev/null || ! git diff --cached --quiet 2>/dev/null; then
      v="${v}-dirty"
    fi
    printf '%s' "$v"
  fi
}
