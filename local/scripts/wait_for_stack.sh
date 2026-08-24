#!/usr/bin/env bash
# Wait until every default service in the local stack is actually serving, or
# bail out. Compose's own healthchecks cover fake-gcs / redpanda / target-system-mock
# inside the containers; this polls the *published* ports from the host so the
# caller knows the stack is reachable from the pipelines and Java apps too.
#
# bigquery has no in-container probe (the goccy image has no shell tooling), so
# the host-side HTTP poll here is its readiness gate.
#
# Airflow is optional (separate compose file). It is waited on only if it is running.
set -euo pipefail

TIMEOUT="${STACK_WAIT_TIMEOUT:-240}"   # seconds, total budget
INTERVAL=3

started="$(date +%s)"

port_open() { # host port  → 0 if something answers
  local host="$1" port="$2"
  timeout 2 bash -c ": > /dev/tcp/${host}/${port}" 2>/dev/null
}

http_ok() { # url [expected_http_code_prefix]  → 0 if it answers
  local url="$1" expect="${2:-}"
  local code
  code="$(curl -s -o /dev/null -w '%{http_code}' --connect-timeout 2 "$url" 2>/dev/null || echo 000)"
  [ "$code" = "000" ] && return 1
  [ -z "$expect" ] && return 0
  [[ "$code" == "$expect"* ]]
}

wait_for() { # name  predicate...
  local name="$1"; shift
  local predicate="$1"
  local url="${2:-}"
  local label="$3"
  while true; do
    if [ "$predicate" = port ]; then
      local port="${url##*:}"
      local host="${url%%:*}"
      port_open "$host" "$port" && { echo "  ✓ $name ready ($label)"; return 0; }
    else
      http_ok "$url" "" && { echo "  ✓ $name ready ($label)"; return 0; }
    fi
    if [ $(( $(date +%s) - started )) -ge "$TIMEOUT" ]; then
      echo "  ✗ $name NOT ready after ${TIMEOUT}s ($label)" >&2
      echo "    last probe: ${url}${label:+ }" >&2
      return 1
    fi
    sleep "$INTERVAL"
  done
}

echo "waiting for the local stack (timeout ${TIMEOUT}s)…"

wait_for fake-gcs      http   "http://localhost:4443/storage/v1/b?project=mig-local" "GCS REST on :4443"
wait_for bigquery      http   "http://localhost:9050/bigquery/v2/projects/mig-local/datasets" "BigQuery REST on :9050"
wait_for redpanda      port   "127.0.0.1:19092" "Kafka broker on :19092"
wait_for target-system-mock http "http://localhost:8080/__admin/health" "Target System mock on :8080"

# Airflow is brought up separately with `make up-airflow`; only wait if it is running.
if podman ps --format '{{.Names}}' 2>/dev/null | grep -qi airflow; then
  wait_for airflow http "http://localhost:8081/health" "Airflow webserver on :8081"
else
  echo "  • airflow not running (make up-airflow) — local/scripts/run_pipeline.py orchestrates instead"
fi

echo "local stack is up and reachable."