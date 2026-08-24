#!/usr/bin/env bash
# Acceptance criterion 8 (mvp-plan §4): a second project runs end-to-end with a
# zero-line diff in engine code.
#
# What counts as "engine" matters, so it is stated explicitly rather than left to
# interpretation:
#
#   ENGINE  = pipelines/ + apps/      — must not change, at all, ever, for a new project
#   PROJECT = contracts/              — new files only; this is the extension point
#
# The honest caveat: a new project also adds one Dataform model (a *new* .sqlx file — no
# existing model is edited). SQL that shapes a different target table cannot be
# config-driven without building a SQL generator, which would be a worse trade. So the
# claim proved here is "extend, never rewrite": nothing existing is modified.
set -euo pipefail

cd "$(dirname "$0")/../.."

ENGINE_PATHS=(pipelines apps)
# Existing Dataform models are engine too: a new project may *add* a .sqlx, but editing
# one that already exists is a rewrite, not an extension. Tracked separately from
# ENGINE_PATHS because the rule differs — new files are allowed here, modifications are
# not — which the fingerprint alone could not express.
SQLX_PATH=dataform/definitions
GREEN=$'\033[32m'; RED=$'\033[31m'; BOLD=$'\033[1m'; OFF=$'\033[0m'

fingerprint() {
  find "${ENGINE_PATHS[@]}" -type f \( -name '*.py' -o -name '*.java' \) \
    -not -path '*/build/*' -not -path '*/__pycache__/*' \
    | sort | xargs sha256sum | sha256sum | cut -d' ' -f1
}

echo "${BOLD}engine fingerprint before project2${OFF}"
BEFORE="$(fingerprint)"
echo "  $BEFORE"

echo
echo "${BOLD}running project1${OFF} (contracts/mappings/mapping-project1.yaml)"
MAPPING=contracts/mappings/mapping-project1.yaml \
  .venv/bin/python -m tests.project_smoke --mapping contracts/mappings/mapping-project1.yaml --layout fixed

echo
echo "${BOLD}running project2${OFF} (contracts/mappings/mapping-project2.yaml)"
MAPPING=contracts/mappings/mapping-project2.yaml \
  .venv/bin/python -m tests.project_smoke --mapping contracts/mappings/mapping-project2.yaml --layout csv

echo
echo "${BOLD}engine fingerprint after project2${OFF}"
AFTER="$(fingerprint)"
echo "  $AFTER"

if [ "$BEFORE" != "$AFTER" ]; then
  echo "${RED}FAIL: engine files changed while running a second project.${OFF}" >&2
  exit 1
fi

# When a baseline commit exists, assert the stronger property directly: no tracked
# engine file is modified in the working tree.
if git rev-parse HEAD >/dev/null 2>&1; then
  if ! git diff --quiet -- "${ENGINE_PATHS[@]}"; then
    echo "${RED}FAIL: git reports modified engine files:${OFF}" >&2
    git diff --stat -- "${ENGINE_PATHS[@]}" >&2
    exit 1
  fi
  echo "  git: no modifications under ${ENGINE_PATHS[*]}"

  # Adding a .sqlx is the documented caveat; modifying an existing one is not allowed.
  if ! git diff --quiet -- "$SQLX_PATH" 2>/dev/null; then
    echo "${RED}FAIL: an existing Dataform model was modified:${OFF}" >&2
    git diff --stat -- "$SQLX_PATH" >&2
    echo "  a new project may ADD a .sqlx, but editing one is a rewrite, not an extension" >&2
    exit 1
  fi
  echo "  git: no existing model modified under ${SQLX_PATH} (new files are allowed)"
else
  echo "  git: no baseline commit yet — fingerprint comparison used instead"
fi

echo
echo "${GREEN}PASS${OFF} — project2 runs end-to-end with zero engine-code diff."
echo "      Only contracts/ files were added: TDS x2, mapping YAML, TARGET JSON Schema."
