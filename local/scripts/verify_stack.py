#!/usr/bin/env python3
"""Healthcheck every default service in the local stack by exercising it — not
just by opening a port. This is the `make verify-stack` gate.

For each service it performs the cheapest operation the pipeline will actually
do, so a green `verify-stack` means the stack is *usable*, not merely *up*:
  fake-gcs      list buckets (GET /storage/v1/b)
  bigquery      list datasets (GET /bigquery/v2/projects/{p}/datasets)
                 + a `SELECT 1` via the jobs.query API (reported, not fatal)
  redpanda      list topics (confluent-kafka AdminClient)
  target-system-mock  GET /__admin/stats
  airflow       GET /health (only if the profile is running)
"""
from __future__ import annotations

import os
import sys
import time
from pathlib import Path

import requests
from confluent_kafka import KafkaException
from confluent_kafka.admin import AdminClient


def load_dotenv(path: Path) -> None:
    if not path.is_file():
        return
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, val = line.partition("=")
        os.environ.setdefault(key.strip(), val.strip().strip('"').strip("'"))


load_dotenv(Path(__file__).resolve().parents[2] / ".env")

GCP_PROJECT = os.environ.get("GCP_PROJECT", "mig-local")
GCS_HOST = os.environ.get("STORAGE_EMULATOR_HOST", "http://localhost:4443")
BQ_HOST = os.environ.get("BIGQUERY_EMULATOR_HOST", "http://localhost:9050").rstrip("/")
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:19092")
TARGET_SYSTEM_URL = os.environ.get("TARGET_SYSTEM_URL", "http://localhost:8080")

failures: list[str] = []


def check(name: str, fn) -> None:
    try:
        detail = fn()
        print(f"  \033[32m✓\033[0m {name}: {detail}")
    except Exception as e:  # noqa: BLE001 — a failed check is the whole point
        print(f"  \033[31m✗\033[0m {name}: {e}")
        failures.append(name)


def check_gcs() -> str:
    r = requests.get(f"{GCS_HOST}/storage/v1/b?project={GCP_PROJECT}", timeout=5)
    r.raise_for_status()
    names = [b["name"] for b in r.json().get("items", [])]
    return f"{len(names)} buckets: {', '.join(names) or '(none)'}"


def check_bigquery() -> str:
    r = requests.get(f"{BQ_HOST}/bigquery/v2/projects/{GCP_PROJECT}/datasets", timeout=10)
    r.raise_for_status()
    ds = [d["datasetReference"]["datasetId"] for d in r.json().get("datasets", [])]
    # Bonus probe: a synchronous query via jobs.query. Some emulator builds
    # implement listing but not query; report it but don't fail the gate.
    try:
        q = requests.post(
            f"{BQ_HOST}/bigquery/v2/projects/{GCP_PROJECT}/queries",
            json={"query": "SELECT 1 AS one", "useLegacySql": False},
            timeout=10,
        )
        q_detail = "SELECT 1 ok" if q.ok else f"SELECT 1 {q.status_code}"
    except Exception:  # noqa: BLE001
        q_detail = "SELECT 1 unreachable"
    return f"{len(ds)} datasets ({', '.join(ds) or 'none'}); {q_detail}"


def check_kafka() -> str:
    admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP})
    topics = admin.list_topics(timeout=5).topics
    if topics is None:
        raise KafkaException("list_topics returned None")
    return f"{len([t for t in topics if t and not t.startswith('_')])} topics"


def check_target_system() -> str:
    r = requests.get(f"{TARGET_SYSTEM_URL}/__admin/stats", timeout=5)
    r.raise_for_status()
    stats = r.json()
    return f"received={stats.get('received', 0)} accepted={stats.get('accepted', 0)}"


def check_airflow() -> str | None:
    try:
        r = requests.get("http://localhost:8081/health", timeout=5)
        r.raise_for_status()
        return f"healthy ({r.json().get('status', 'ok')})"
    except Exception:  # noqa: BLE001
        return None  # profile not active — not a failure


def main() -> int:
    print("verify-stack — exercising every local service")
    check("fake-gcs", check_gcs)
    check("bigquery", check_bigquery)
    check("redpanda", check_kafka)
    check("target-system-mock", check_target_system)

    airflow = check_airflow()
    if airflow is not None:
        print(f"  \033[32m✓\033[0m airflow: {airflow}")
    else:
        print("  • airflow: skipped (profile not active)")

    if failures:
        print(f"\n\033[31mverify-stack FAILED: {', '.join(failures)}\033[0m", file=sys.stderr)
        return 1
    print("\n\033[32mverify-stack OK — stack is up and usable.\033[0m")
    return 0


if __name__ == "__main__":
    sys.exit(main())