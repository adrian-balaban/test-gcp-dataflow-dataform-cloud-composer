"""Dataflow Data Enrichment.

architecture diagram places this between the Cloud Composer orchestrator and the BigQuery
Transformation dataset: "Performs Data Enrichment". It reads the curated table Dataform
produced, joins reference data that does not exist in the source extract, and writes
`account_enriched` — the table the JSON Data Producer maps to Target System documents.

Reference data is a Beam **side input**: small, broadcast to every worker, and re-read
per run so a reference change takes effect without a redeploy. At real volume a large
reference set would move to a BigQuery join in Dataform instead; the seam is here.

No doors are accounted at this stage — enrichment adds columns, it never drops or
rejects a row, and the run-level assertion below enforces that.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Iterable

import apache_beam as beam
from apache_beam.metrics import Metrics
from google.cloud import bigquery as bq_types

from pipelines.common.config import ROOT, Config, require_identifier
from pipelines.common.runner import pipeline_options
from pipelines.common.storage import BigQuery

CURATED_TABLE = "account_curated"
ENRICHED_TABLE = "account_enriched"
REFERENCE_FILE = ROOT / "contracts/reference/branches.json"

ENRICHED_SCHEMA = [
    bq_types.SchemaField("account_id", "STRING", mode="REQUIRED"),
    bq_types.SchemaField("customer_id", "STRING"),
    bq_types.SchemaField("product_code", "STRING"),
    bq_types.SchemaField("currency", "STRING"),
    bq_types.SchemaField("opened_on", "DATE"),
    bq_types.SchemaField("status_code", "STRING"),
    bq_types.SchemaField("balance_minor", "INT64"),
    bq_types.SchemaField("balance_amount", "STRING"),
    bq_types.SchemaField("segment", "STRING"),
    bq_types.SchemaField("branch", "STRING"),
    bq_types.SchemaField("risk_rating", "INT64"),
    bq_types.SchemaField("balance_band", "STRING"),
    bq_types.SchemaField("account_age_years", "INT64"),
    # ── contributed by this stage ──
    bq_types.SchemaField("branch_name", "STRING"),
    bq_types.SchemaField("branch_city", "STRING"),
    bq_types.SchemaField("branch_region", "STRING"),
    bq_types.SchemaField("run_id", "STRING", mode="REQUIRED"),
    bq_types.SchemaField("account_key", "STRING"),
]


def load_reference() -> dict[str, dict[str, str]]:
    return json.loads(REFERENCE_FILE.read_text(encoding="utf-8"))["branches"]


class EnrichFn(beam.DoFn):
    """Join each account to its branch reference record."""

    def __init__(self) -> None:
        self.c_enriched = Metrics.counter("data_enrichment", "enriched")
        self.c_unmatched = Metrics.counter("data_enrichment", "reference_unmatched")

    def process(self, row: dict[str, Any], branches: dict[str, dict[str, str]]) -> Iterable[dict]:
        out = dict(row)
        branch = branches.get((row.get("branch") or "").strip())
        if branch is None:
            # An unresolved reference is a data-quality signal, not a reject: the
            # account is still migratable, it just carries no branch detail. It is
            # counted so the migrability report can show it.
            self.c_unmatched.inc()
            out["branch_name"] = None
            out["branch_city"] = None
            out["branch_region"] = None
        else:
            out["branch_name"] = branch["name"]
            out["branch_city"] = branch["city"]
            out["branch_region"] = branch["region"]

        if out.get("opened_on") is not None and not isinstance(out["opened_on"], str):
            out["opened_on"] = out["opened_on"].isoformat()

        self.c_enriched.inc()
        yield out


class WriteRowsFn(beam.DoFn):
    def __init__(self, cfg: Config, dataset: str, table: str) -> None:
        self.cfg = cfg
        self.dataset = dataset
        self.table = table

    def setup(self) -> None:
        self.bq = BigQuery(self.cfg)

    def process(self, batch: list[dict]) -> Iterable[Any]:
        if batch:
            self.bq.insert(self.dataset, self.table, batch)
        return []


def run_pipeline(cfg: Config, run_id: str) -> dict[str, int]:
    bq = BigQuery(cfg)
    bq.ensure_dataset(cfg.ds_transformation)
    bq.ensure_table(cfg.ds_transformation, ENRICHED_TABLE, ENRICHED_SCHEMA)

    # Re-running a run id must be idempotent, so clear this run's slice first.
    bq.client.query(
        f"DELETE FROM `{cfg.project}.{cfg.ds_transformation}.{ENRICHED_TABLE}` "
        f"WHERE run_id = '{run_id}'"
    ).result()

    source_rows = bq.query(
        f"SELECT * FROM `{cfg.project}.{cfg.ds_transformation}.{CURATED_TABLE}` "
        f"WHERE run_id = '{run_id}'"
    )
    branches = load_reference()

    options = pipeline_options(cfg)
    pipeline = beam.Pipeline(options=options)

    reference = pipeline | "Reference" >> beam.Create([branches])

    (
        pipeline
        | "ReadCurated" >> beam.Create(source_rows)
        | "Enrich" >> beam.ParDo(EnrichFn(), branches=beam.pvalue.AsSingleton(reference))
        | "Batch" >> beam.BatchElements(min_batch_size=100, max_batch_size=500)
        | "WriteEnriched" >> beam.ParDo(WriteRowsFn(cfg, cfg.ds_transformation, ENRICHED_TABLE))
    )

    result = pipeline.run()
    result.wait_until_finish()

    def metric(name: str) -> int:
        found = result.metrics().query(
            beam.metrics.MetricsFilter().with_namespace("data_enrichment").with_name(name)
        )
        return sum(c.result for c in found["counters"]) if found["counters"] else 0

    stats = {
        "read": len(source_rows),
        "enriched": metric("enriched"),
        "reference_unmatched": metric("reference_unmatched"),
    }
    # Enrichment must never lose a row — it adds columns, nothing else.
    if stats["read"] != stats["enriched"]:
        raise RuntimeError(
            f"data_enrichment lost rows: read {stats['read']} but wrote {stats['enriched']}"
        )
    return stats


def main() -> None:
    ap = argparse.ArgumentParser(description="Dataflow Data Enrichment")
    # Both dash and underscore forms: the local orchestrator calls `--run-id` while a
    # Dataflow Flex Template launcher passes parameters by their underscore key.
    ap.add_argument("--run-id", "--run_id", dest="run_id", required=True)
    args = ap.parse_args()
    require_identifier("--run-id", args.run_id)

    stats = run_pipeline(Config.from_env(), args.run_id)
    print("data_enrichment:", json.dumps(stats, sort_keys=True))


if __name__ == "__main__":
    main()
