#!/usr/bin/env python3
"""Create the GCS buckets, BigQuery datasets/tables and Kafka topic the pipeline
expects, against the local emulator stack.

This is the `make init-infra` step. It is idempotent: existing buckets, datasets,
tables and topics are left in place. Everything here is created against the
local emulators by default; the same objects would be provisioned by Terraform on
real GCP (see terraform/modules/storage, bigquery, kafka).

Environment (from .env, exported by the Makefile):
  STORAGE_EMULATOR_HOST  fake-gcs endpoint        (default http://localhost:4443)
  GCP_PROJECT             emulator project id      (default mig-local)
  GCS_LANDING_BUCKET / GCS_JSON_BUCKET / GCS_RECON_BUCKET
  BIGQUERY_EMULATOR_HOST BQ emulator endpoint     (default http://localhost:9050)
  BQ_DATASET_EXTRACTION / BQ_DATASET_TRANSFORMATION / BQ_DATASET_RECON
  KAFKA_BOOTSTRAP        redpanda bootstrap        (default localhost:19092)
  KAFKA_TOPIC            target topic             (default target-system-target)
"""
from __future__ import annotations

import os
import sys
import time
from pathlib import Path

import requests
from confluent_kafka import KafkaException
from confluent_kafka.admin import AdminClient, NewTopic


# ── tiny .env loader (the Makefile exports .env, but be nice when run directly) ──
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
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "target-system-target")
# The confirmation stream topic (docs/PLAN-CHANGES-22082026.md) — the mock publishes one
# event per accepted write here; recon consumes it. Created alongside the target topic
# on the same redpanda cluster.
CONFIRMATION_TOPIC = os.environ.get(
    "TARGET_SYSTEM_CONFIRMATION_TOPIC", "target-system-confirmations"
)
# The rejection stream topic (docs/PLAN-CHANGES-02092026-kafka-loader.md) — the other half
# of the mock's write-back. Once the Load edge is Kafka the loader has no HTTP status to
# classify, so a refused document arrives here with a reason string and becomes an .ERR
# row. Absent topic = every published document reports as `unsettled`.
REJECTION_TOPIC = os.environ.get(
    "TARGET_SYSTEM_REJECTION_TOPIC", "target-system-rejections"
)

LANDING = os.environ.get("GCS_LANDING_BUCKET", "mig-landing")
JSON_BUCKET = os.environ.get("GCS_JSON_BUCKET", "mig-json-out")
RECON_BUCKET = os.environ.get("GCS_RECON_BUCKET", "mig-recon")
DS_EXTRACT = os.environ.get("BQ_DATASET_EXTRACTION", "bq_extraction")
DS_TRANS = os.environ.get("BQ_DATASET_TRANSFORMATION", "bq_transformation")
DS_RECON = os.environ.get("BQ_DATASET_RECON", "bq_recon")

RETRIES = 30
RETRY_DELAY = 2


def ok(msg: str) -> None:
    print(f"  \033[32m✓\033[0m {msg}")


# ── GCS via fake-gcs REST ──────────────────────────────────────────────────────
def create_buckets() -> None:
    print("GCS (fake-gcs-server):")
    for bucket in (LANDING, JSON_BUCKET, RECON_BUCKET):
        url = f"{GCS_HOST}/storage/v1/b?project={GCP_PROJECT}"
        body = {"name": bucket, "location": "EU"}
        created = False
        for attempt in range(RETRIES):
            try:
                r = requests.post(url, json=body, timeout=5)
                if r.status_code in (200, 409):
                    ok(f"bucket {bucket}")
                    created = True
                    break
                if r.status_code == 404:
                    raise RuntimeError(f"fake-gcs 404 on bucket create — is the server up? {r.text}")
            except requests.exceptions.ConnectionError:
                pass
            if attempt == RETRIES - 1:
                raise RuntimeError(f"could not create bucket {bucket} at {GCS_HOST}")
            time.sleep(RETRY_DELAY)
        if not created:
            raise RuntimeError(f"could not create bucket {bucket} at {GCS_HOST}")
    r = requests.get(f"{GCS_HOST}/storage/v1/b?project={GCP_PROJECT}", timeout=5)
    r.raise_for_status()
    names = {b["name"] for b in r.json().get("items", [])}
    missing = {LANDING, JSON_BUCKET, RECON_BUCKET} - names
    if missing:
        raise RuntimeError(f"buckets missing after create: {missing}")


# ── BigQuery via the goccy emulator REST ──────────────────────────────────────
def bq(method: str, path: str, **kwargs) -> requests.Response:
    url = f"{BQ_HOST}/bigquery/v2{path}"
    for attempt in range(RETRIES):
        try:
            r = requests.request(method, url, timeout=10, **kwargs)
            if r.status_code not in (502, 503):
                return r
        except requests.exceptions.ConnectionError:
            pass
        time.sleep(RETRY_DELAY)
    raise RuntimeError(f"BQ endpoint {url} unreachable after {RETRIES} attempts")


def _already_exists(r: requests.Response) -> bool:
    # goccy emulator returns 409 for some duplicates but 500 + "already created"
    # for datasets/tables — treat both as idempotent success.
    if r.status_code == 409:
        return True
    if r.status_code == 500 and "already" in r.text.lower():
        return True
    return False


def create_dataset(dataset: str) -> None:
    path = f"/projects/{GCP_PROJECT}/datasets"
    body = {"datasetReference": {"projectId": GCP_PROJECT, "datasetId": dataset}}
    r = bq("POST", path, json=body)
    if _already_exists(r):
        ok(f"dataset {dataset} (exists)")
        return
    if r.status_code not in (200, 201):
        raise RuntimeError(f"create dataset {dataset} failed: {r.status_code} {r.text}")
    ok(f"dataset {dataset}")


def create_table(dataset: str, table: str, schema: list[dict]) -> None:
    path = f"/projects/{GCP_PROJECT}/datasets/{dataset}/tables"
    body = {
        "tableReference": {"projectId": GCP_PROJECT, "datasetId": dataset, "tableId": table},
        "schema": {"fields": schema},
    }
    r = bq("POST", path, json=body)
    if _already_exists(r):
        ok(f"table {dataset}.{table} (exists)")
        return
    if r.status_code not in (200, 201):
        raise RuntimeError(f"create table {dataset}.{table} failed: {r.status_code} {r.text}")
    ok(f"table {dataset}.{table}")


# run_ledger persists the per-run counters, so recon reads them rather than
# re-deriving. Column names predate the two-door framing and are deliberately
# unchanged — see pipelines/common/doors.py.
RUN_LEDGER_SCHEMA = [
    {"name": "run_id", "type": "STRING", "mode": "REQUIRED"},
    {"name": "src_read", "type": "INTEGER"},
    {"name": "extraction_written", "type": "INTEGER"},
    {"name": "rejected", "type": "INTEGER"},
    {"name": "balanced", "type": "BOOLEAN"},
    {"name": "created_at", "type": "TIMESTAMP"},
]


# record_lineage names every record that did not migrate — the per-record evidence
# behind run_ledger's counts. Mirrors LINEAGE_SCHEMA in
# pipelines/file_processor/pipeline.py.
RECORD_LINEAGE_SCHEMA = [
    {"name": "run_id", "type": "STRING", "mode": "REQUIRED"},
    {"name": "source_key", "type": "STRING"},
    {"name": "account_key", "type": "STRING"},
    {"name": "door", "type": "STRING"},
    {"name": "stage", "type": "STRING"},
    {"name": "reason", "type": "STRING"},
    {"name": "detail", "type": "STRING"},
    {"name": "source_file", "type": "STRING"},
    {"name": "created_at", "type": "TIMESTAMP"},
]


def create_bigquery() -> None:
    print("BigQuery (goccy emulator):")
    create_dataset(DS_EXTRACT)
    create_dataset(DS_TRANS)
    create_dataset(DS_RECON)
    create_table(DS_RECON, "run_ledger", RUN_LEDGER_SCHEMA)
    create_table(DS_RECON, "record_lineage", RECORD_LINEAGE_SCHEMA)


# ── Kafka via redpanda (confluent-kafka AdminClient) ───────────────────────────
def create_kafka_topic() -> None:
    print(f"Kafka (redpanda @ {KAFKA_BOOTSTRAP}):")
    admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP})
    existing: set[str] = set()
    for attempt in range(RETRIES):
        try:
            existing = set(admin.list_topics(timeout=5).topics.keys())
            break
        except KafkaException:
            if attempt == RETRIES - 1:
                raise
            time.sleep(RETRY_DELAY)
    # The target topic (the loader's outbound stream) and both return topics — the
    # mock's confirmations (docs/PLAN-CHANGES-22082026.md) and rejections
    # (docs/PLAN-CHANGES-02092026-kafka-loader.md) — live on the same cluster.
    for topic in (KAFKA_TOPIC, CONFIRMATION_TOPIC, REJECTION_TOPIC):
        if topic in existing:
            ok(f"topic {topic} (exists)")
            continue
        fs = admin.create_topics(
            [NewTopic(topic, num_partitions=1, replication_factor=1)]
        )
        for _, f in fs.items():
            f.result()  # raises on error
        ok(f"topic {topic}")


def main() -> int:
    print("init-infra — creating buckets, BigQuery datasets/tables, Kafka topic")
    create_buckets()
    create_bigquery()
    create_kafka_topic()
    print("init-infra complete.")
    return 0


if __name__ == "__main__":
    sys.exit(main())