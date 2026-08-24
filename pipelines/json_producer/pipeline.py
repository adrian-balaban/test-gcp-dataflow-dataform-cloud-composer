"""Dataflow JSON Data Producer.

architecture diagram: "JSON extraction" out of the BigQuery Transformation dataset, producing the
JSON files the Load lane consumes. This is where a record's disposition is finally
settled — the **map** and **schema** stages — so a mapping bug becomes a reject rather
than bad data in Target System. A row that survives here is migrated; anything else leaves
through the not-migrated door with a reason attached.

Two stages, deliberately:

1. **Map + validate** (Beam, parallel). Each enriched row is re-keyed into SRC-TDS field
   names via the mapping's `curated.aliases`, mapped through the YAML rules, and validated
   against the TARGET JSON Schema. Survivors are materialised; failures go to the reject log.

2. **Batch + emit** (sequential). Survivors are numbered with `ROW_NUMBER()` in SQL and
   grouped into batches of exactly 200 — the Loader contract. Numbering *after* the reject
   stage is what makes the batch count exactly `ceil(written / 200)` rather than merely
   approximately so. Emission is sequential because ordering within a topic partition is
   part of the contract we are honouring.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from typing import Any, Iterable

import apache_beam as beam
from apache_beam.metrics import Metrics
from google.cloud import bigquery as bq_types

from pipelines.common.config import Config, require_identifier
from pipelines.common.doors import Door, Lineage, RecordError
from pipelines.common.mapping import TransformEngine, load_mapping
from pipelines.common.runner import pipeline_options
from pipelines.common.sinks import bigquery_writer, target_writer
from pipelines.common.storage import BigQuery

ENRICHED_TABLE = "account_enriched"
TARGET_TABLE = "account_target"
REJECT_TABLE = "reject_log"
LINEAGE_TABLE = "record_lineage"

TARGET_SCHEMA = [
    bq_types.SchemaField("run_id", "STRING", mode="REQUIRED"),
    bq_types.SchemaField("account_id", "STRING", mode="REQUIRED"),
    bq_types.SchemaField("account_key", "STRING", mode="REQUIRED"),
    bq_types.SchemaField("doc_json", "STRING", mode="REQUIRED"),
]


class MapAndValidateFn(beam.DoFn):
    """The map and schema stages.

    Emits rejects twice, deliberately and in two shapes: a `reject_log` row (which keeps
    the raw payload, for debugging) and a `record_lineage` row (which keeps the key and
    the door, for the audit register). The lane's two stages both settle dispositions, so
    both must contribute to the register — otherwise it names only the records the file
    processor happened to stop.
    """

    REJECTED = "rejected"
    LINEAGE = "lineage"

    def __init__(self, cfg: Config, run_id: str, ingested_at: str) -> None:
        self.cfg = cfg
        self.run_id = run_id
        self.ingested_at = ingested_at
        self.c_written = Metrics.counter("json_producer", "written")
        self.c_rejected = Metrics.counter("json_producer", "rejected")

    def setup(self) -> None:
        self.mapping = load_mapping(self.cfg.mapping_path)
        self.engine = TransformEngine(self.mapping)

    def process(self, row: dict[str, Any]) -> Iterable[Any]:
        fields = self.mapping.curated_row_to_fields(row)
        try:
            doc = self.engine.map_record(fields, self.run_id)
            self.engine.validate(doc)
        except RecordError as exc:
            self.c_rejected.inc()
            yield beam.pvalue.TaggedOutput(
                self.REJECTED,
                {
                    "run_id": self.run_id,
                    "batch_id": 0,
                    "source_key": str(row.get("account_id") or ""),
                    "stage": exc.stage.value,
                    "reason": exc.reason.value,
                    "detail": exc.detail,
                    "raw_record": json.dumps(row, sort_keys=True, default=str),
                },
            )
            yield beam.pvalue.TaggedOutput(
                self.LINEAGE,
                Lineage(
                    run_id=self.run_id,
                    source_key=str(row.get("account_id") or ""),
                    account_key=str(row.get("account_key") or "") or None,
                    door=Door.REJECTED.value,
                    stage=exc.stage.value,
                    reason=exc.reason.value,
                    detail=exc.detail,
                    source_file=f"{self.cfg.ds_transformation}.{row.get('_source_table', '')}".rstrip("."),
                    created_at=self.ingested_at,
                ).to_dict(),
            )
            return

        self.c_written.inc()
        yield {
            "run_id": self.run_id,
            "account_id": doc["accountId"],
            "account_key": doc["migration"]["dedupKey"],
            "doc_json": json.dumps(doc, sort_keys=True),
        }


class WriteRowsFn(beam.DoFn):
    def __init__(self, cfg: Config, dataset: str, table: str) -> None:
        self.cfg = cfg
        self.dataset = dataset
        self.table = table

    def setup(self) -> None:
        self.writer = bigquery_writer(self.cfg)

    def process(self, batch: list[dict]) -> Iterable[Any]:
        if batch:
            self.writer.write(self.dataset, self.table, batch)
        return []


def run_pipeline(cfg: Config, run_id: str, sinks: str = "both") -> dict[str, Any]:
    mapping = load_mapping(cfg.mapping_path)
    ingested_at = datetime.now(timezone.utc).isoformat()
    bq = BigQuery(cfg)
    bq.ensure_dataset(cfg.ds_transformation)
    bq.ensure_table(cfg.ds_transformation, TARGET_TABLE, TARGET_SCHEMA)

    # Idempotent per run id.
    bq.client.query(
        f"DELETE FROM `{cfg.project}.{cfg.ds_transformation}.{TARGET_TABLE}` "
        f"WHERE run_id = '{run_id}'"
    ).result()

    # This stage appends to reject_log and record_lineage, so it must purge what it
    # appends — the same rule file_processor follows. Without it a standalone re-run
    # (clearing only this task in Airflow) double-counts its own map/schema rejects, and
    # the whole-lane equation stops closing: rejected inflates while src_read does not.
    # It does not fail quietly — recon exits non-zero — but it fails for a reason that
    # looks like data corruption rather than a re-run.
    for table in (REJECT_TABLE, LINEAGE_TABLE):
        bq.client.query(
            f"DELETE FROM `{cfg.project}.{cfg.ds_recon}.{table}` "
            f"WHERE run_id = '{run_id}' AND stage IN ('map', 'schema')"
        ).result()

    source_table = mapping.curated_table or ENRICHED_TABLE
    rows = bq.query(
        f"SELECT * FROM `{cfg.project}.{cfg.ds_transformation}.{source_table}` "
        f"WHERE run_id = '{run_id}'"
    )

    # ── stage 1: map + validate ────────────────────────────────────────────────
    options = pipeline_options(cfg)
    pipeline = beam.Pipeline(options=options)

    mapped = (
        pipeline
        | "ReadEnriched" >> beam.Create(rows)
        | "MapAndValidate" >> beam.ParDo(
            MapAndValidateFn(cfg, run_id, ingested_at)
        ).with_outputs(
            MapAndValidateFn.REJECTED, MapAndValidateFn.LINEAGE, main="documents"
        )
    )

    (
        mapped["documents"]
        | "BatchDocs" >> beam.BatchElements(min_batch_size=100, max_batch_size=500)
        | "WriteDocs" >> beam.ParDo(WriteRowsFn(cfg, cfg.ds_transformation, TARGET_TABLE))
    )

    (
        mapped[MapAndValidateFn.REJECTED]
        | "BatchRejects" >> beam.BatchElements(min_batch_size=1, max_batch_size=500)
        | "WriteRejects" >> beam.ParDo(WriteRowsFn(cfg, cfg.ds_recon, REJECT_TABLE))
    )

    # The file processor created and purged this table for the run; this stage appends
    # the map/schema dispositions it settles, exactly as it appends to reject_log.
    (
        mapped[MapAndValidateFn.LINEAGE]
        | "BatchLineage" >> beam.BatchElements(min_batch_size=1, max_batch_size=500)
        | "WriteLineage" >> beam.ParDo(WriteRowsFn(cfg, cfg.ds_recon, LINEAGE_TABLE))
    )

    result = pipeline.run()
    result.wait_until_finish()

    def metric(name: str) -> int:
        found = result.metrics().query(
            beam.metrics.MetricsFilter().with_namespace("json_producer").with_name(name)
        )
        return sum(c.result for c in found["counters"]) if found["counters"] else 0

    written, rejected = metric("written"), metric("rejected")

    # ── stage 2: batch at exactly 200 and emit to both sinks ───────────────────
    batch_size = mapping.batch_size
    numbered = bq.query(
        f"""
        SELECT doc_json,
               DIV(ROW_NUMBER() OVER (ORDER BY account_key) - 1, {batch_size}) AS batch_id
        FROM `{cfg.project}.{cfg.ds_transformation}.{TARGET_TABLE}`
        WHERE run_id = '{run_id}'
        ORDER BY batch_id, account_key
        """
    )

    writer = target_writer(cfg, run_id, sinks=sinks)
    current_batch: list[dict] = []
    current_id = 0
    emitted_batches = 0
    # Per-batch tallies, so the balancing equation is enforced at batch granularity on
    # the pipeline the DAG actually runs — not only in the in-memory `run_batches` path.
    # Here the batch id is authoritative (assigned by ROW_NUMBER above), which is exactly
    # why the check belongs at this stage rather than at intake.
    batch_counters: dict[int, int] = {}

    def close_batch(batch_id: int, docs: list[dict]) -> None:
        nonlocal emitted_batches
        writer.write_batch(batch_id, docs)
        emitted_batches += 1
        # No require_balance here: the per-batch tally is a single count, so a balance
        # check has no second side to compare against and could never fail — it would
        # read as a guard while guarding nothing. The three checks after the loop are the
        # ones with teeth: batch sizes, batch count, and the per-batch tallies summing to
        # the run total.
        batch_counters[batch_id] = len(docs)

    for row in numbered:
        if row["batch_id"] != current_id and current_batch:
            close_batch(current_id, current_batch)
            current_batch, current_id = [], row["batch_id"]
        current_id = row["batch_id"]
        current_batch.append(json.loads(row["doc_json"]))
    if current_batch:
        close_batch(current_id, current_batch)

    sink_stats = writer.close()

    # Every batch but the last must be exactly batch_size. This is the check the vacuous
    # per-batch balance was standing in for: it catches a batch-boundary bug — a document
    # numbered into the wrong batch, or a short batch emitted early — which the counters
    # cannot see because they only ever compare a batch with itself.
    if batch_counters:
        *full_batches, _last = [batch_counters[b] for b in sorted(batch_counters)]
        short = [n for n in full_batches if n != batch_size]
        if short:
            raise RuntimeError(
                f"non-final batches must hold exactly {batch_size} documents, got {short} "
                f"— a document was numbered into the wrong batch"
            )

    expected_batches = -(-written // batch_size)  # ceil
    if emitted_batches != expected_batches:
        raise RuntimeError(
            f"batch count {emitted_batches} != ceil({written}/{batch_size}) = {expected_batches}"
        )

    # The per-batch tallies must also sum back to the run total: per-batch balance alone
    # would still permit a whole batch going missing between numbering and emission.
    batched_docs = sum(batch_counters.values())
    if batched_docs != written:
        raise RuntimeError(
            f"batched {batched_docs} documents but {written} were written to "
            f"{TARGET_TABLE} — a batch was lost between numbering and emission"
        )

    return {
        "written": written,
        "rejected": rejected,
        "batches": emitted_batches,
        "batch_size": batch_size,
        "sinks": sink_stats,
    }


def main() -> None:
    ap = argparse.ArgumentParser(description="Dataflow JSON Data Producer")
    # Both dash and underscore forms: the local orchestrator calls `--run-id` while a
    # Dataflow Flex Template launcher passes parameters by their underscore key.
    ap.add_argument("--run-id", "--run_id", dest="run_id", required=True)
    ap.add_argument("--sinks", default="both", choices=("kafka", "gcs", "both"))
    args = ap.parse_args()
    require_identifier("--run-id", args.run_id)

    stats = run_pipeline(Config.from_env(), args.run_id, sinks=args.sinks)
    print("json_producer:", json.dumps(stats, sort_keys=True))


if __name__ == "__main__":
    main()
