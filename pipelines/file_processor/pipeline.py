"""Dataflow File Processor / Data Loader.

architecture diagram: "Process source files (decompress/decrypt) + load data in BigQuery + File
Transport". Waits on the `.FLG` semaphore, pulls the encrypted bundle, decrypts and
unpacks it, verifies the `.CHS` checksums, parses every record through the SRC-TDS and
loads the survivors into the BigQuery Extraction dataset.

Parse is settled here:

    raw line ──parse──▶ bq_extraction.account_src
                 │
                 └─ .ERR ──▶ bq_recon.record_lineage (+ counted)

Every record that does *not* reach `account_src` is named in `record_lineage` with its
door, stage and enumerated reason — the counters in `run_ledger` are the claim, that
table is the evidence.

Map and schema happen downstream, in the JSON Data Producer, after Dataform has done
the SQL transformation. The balancing equation therefore closes across the lane, not
inside one process — which is how it will have to work at real volume anyway. A record
still counted as written here may yet end up not migrated downstream.

Parse is not re-implemented here: `ParseFn` delegates to
`pipelines.common.engine.route_intake`, the same function `RecordRouter` uses, so the
two paths cannot drift. This layer supplies parallelism and nothing else.

Every run is one full snapshot of the source, scoped only by `run_id` — no run kind, no
window (docs/PLAN-CHANGES-21082026.md D5). Idempotency is count-then-DELETE per
`run_id`, below.

Runs on DirectRunner locally and on DataflowRunner unchanged — see docs/runbook-gcp.md
for what packaging that requires.
"""

from __future__ import annotations

import argparse
import io
import json
import tarfile
from datetime import datetime, timezone
from typing import Any, Iterable

import apache_beam as beam
from apache_beam.metrics import Metrics
from google.cloud import bigquery as bq_types

from pipelines.common.artefacts import artefact_names
from pipelines.common.config import Config, require_identifier
from pipelines.common.doors import (
    Counters,
    Door,
    Lineage,
    Reject,
    content_name,
    require_balance,
)
from pipelines.common.engine import route_intake
from pipelines.common.mapping import TransformEngine, load_mapping
from pipelines.common.pgp import resolve_gpg
from pipelines.common.runner import pipeline_options
from pipelines.common.schema import bq_schema_from_tds, row_from_fields
from pipelines.common.sinks import bigquery_writer
from pipelines.common.storage import BigQuery, Gcs

SRC_TABLE = "account_src"
REJECT_TABLE = "reject_log"
LEDGER_TABLE = "run_ledger"
LINEAGE_TABLE = "record_lineage"

# Mirrors the table local/scripts/init_infra.py provisions.
LEDGER_SCHEMA = [
    bq_types.SchemaField("run_id", "STRING", mode="REQUIRED"),
    bq_types.SchemaField("src_read", "INT64"),
    bq_types.SchemaField("extraction_written", "INT64"),
    bq_types.SchemaField("rejected", "INT64"),
    bq_types.SchemaField("balanced", "BOOL"),
    bq_types.SchemaField("created_at", "TIMESTAMP"),
]

# The per-record register behind the aggregate ledger: one row per not-migrated record.
# `run_ledger` says how many left through each door; this says which — see doors.Lineage
# for why migrated records are not restated here.
LINEAGE_SCHEMA = [
    bq_types.SchemaField("run_id", "STRING", mode="REQUIRED"),
    bq_types.SchemaField("source_key", "STRING"),
    bq_types.SchemaField("account_key", "STRING"),
    bq_types.SchemaField("door", "STRING"),
    bq_types.SchemaField("stage", "STRING"),
    bq_types.SchemaField("reason", "STRING"),
    bq_types.SchemaField("detail", "STRING"),
    bq_types.SchemaField("source_file", "STRING"),
    bq_types.SchemaField("created_at", "TIMESTAMP"),
]

REJECT_SCHEMA = [
    bq_types.SchemaField("run_id", "STRING", mode="REQUIRED"),
    bq_types.SchemaField("batch_id", "INT64"),
    bq_types.SchemaField("source_key", "STRING"),
    bq_types.SchemaField("stage", "STRING"),
    bq_types.SchemaField("reason", "STRING"),
    bq_types.SchemaField("detail", "STRING"),
    bq_types.SchemaField("raw_record", "STRING"),
]


# ────────────────────────────────────────────────────────────────────────── stages


def _parse_chs(text: str) -> dict[str, tuple[str, int]]:
    out: dict[str, tuple[str, int]] = {}
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.split()
        if len(parts) >= 3:
            out[parts[2]] = (parts[0], int(parts[1]))
    return out


def _sha256(data: bytes) -> str:
    import hashlib

    return hashlib.sha256(data).hexdigest()


def _count_records(data: bytes) -> int:
    if not data:
        return 0
    n = data.count(b"\n")
    return n if data.endswith(b"\n") else n + 1


class ReadBundleFn(beam.DoFn):
    """Emits `(source_file, raw_line)` for every record in the encrypted bundle."""

    def __init__(self, cfg: Config, run_id: str, record_name: str) -> None:
        self.cfg = cfg
        self.run_id = run_id
        self.record_name = record_name
        self.records_read = Metrics.counter("file_processor", "src_read")

    def setup(self) -> None:
        self.gcs = Gcs(self.cfg)
        # Locally the keyring on disk; on Dataflow the key is materialised from Secret
        # Manager — see pipelines/common/pgp.py. One call site, two worlds.
        self.gpg = resolve_gpg(self.cfg)
        # Needed only to recognise the CSV header line (contracts/README.md) before it
        # reaches `src_read` — ParseFn already skips it via the same check, but by then
        # the header has already been counted here, one line too early.
        self.engine = TransformEngine(load_mapping(self.cfg.mapping_path))

    def process(self, _element: Any) -> Iterable[tuple[str, str]]:
        names = artefact_names(self.record_name)
        prefix = f"extraction/{self.run_id}/"

        # The semaphore is the contract: nothing is read until the extractor says done.
        if not self.gcs.exists(self.cfg.landing_bucket, prefix + names["flg"]):
            raise RuntimeError(
                f"no .FLG semaphore at gs://{self.cfg.landing_bucket}/{prefix}{names['flg']} "
                "— the extract is absent or incomplete"
            )

        encrypted = self.gcs.get(self.cfg.landing_bucket, prefix + names["bundle"])
        decrypted = self.gpg.decrypt(
            encrypted, passphrase=getattr(self.gpg, "mig_passphrase", None)
        )
        if not decrypted.ok:
            raise RuntimeError(f"PGP decrypt failed: {decrypted.status}")

        files: dict[str, bytes] = {}
        with tarfile.open(fileobj=io.BytesIO(decrypted.data), mode="r:gz") as tar:
            for member in tar.getmembers():
                if member.isfile():
                    extracted = tar.extractfile(member)
                    if extracted is not None:
                        files[member.name] = extracted.read()

        # Verify every .DAT against the .CHS manifest before a single record is trusted.
        manifest = _parse_chs(files[names["chs"]].decode("utf-8"))
        problems: list[str] = []
        for file_name, (expected_sha, expected_records) in manifest.items():
            payload = files.get(file_name)
            if payload is None:
                problems.append(f"{file_name}: listed in .CHS but absent from the bundle")
                continue
            if _sha256(payload) != expected_sha:
                problems.append(f"{file_name}: checksum mismatch")
            if _count_records(payload) != expected_records:
                problems.append(
                    f"{file_name}: record count {_count_records(payload)} != {expected_records}"
                )
        if problems:
            raise RuntimeError("checksum verification failed: " + "; ".join(problems))

        # "File Transport" (architecture diagram): republish the non-payload artefacts to the recon
        # bucket in the clear. Reconciliation needs the Extractor's .RPT to know what the
        # upstream side *claims* it produced, and it must not need the PGP private key to
        # get it — the key stays with this component, which already had to decrypt.
        for artefact in (names["rpt"], names["chs"], names["err"]):
            if artefact in files:
                self.gcs.put(
                    self.cfg.recon_bucket,
                    f"extraction/{self.run_id}/{artefact}",
                    files[artefact],
                )

        for file_name in sorted(f for f in files if f.endswith(".DAT")):
            lines = files[file_name].decode("utf-8").splitlines()
            for line in lines:
                if line.strip() and not self.engine.is_header(line):
                    self.records_read.inc()
                    yield file_name, line


class ParseFn(beam.DoFn):
    """Parse — a thin wrapper over the shared engine.

    The routing decision itself lives in `pipelines.common.engine.route_intake`, which
    is the single implementation shared with `RecordRouter`. This class only supplies
    parallelism and the Beam-specific shape: a tagged output and metric counters.

    Emits `(account_key, payload)` on the main output; parse failures go to `rejected`.
    """

    REJECTED = "rejected"

    def __init__(self, cfg: Config, run_id: str, ingested_at: str) -> None:
        self.cfg = cfg
        self.run_id = run_id
        self.ingested_at = ingested_at
        self.c_rejected = Metrics.counter("file_processor", "rejected")
        self.c_written = Metrics.counter("file_processor", "written_to_extraction")

    def setup(self) -> None:
        self.engine = TransformEngine(load_mapping(self.cfg.mapping_path))

    def process(self, element: tuple[str, str]) -> Iterable[Any]:
        source_file, raw = element
        if self.engine.is_header(raw):
            # The pipe-delimited extract carries a header row (contracts/README.md);
            # it is not a record and is silently skipped, not counted or rejected.
            return
        outcome = route_intake(self.engine, raw)

        if outcome.door is Door.REJECTED:
            self.c_rejected.inc()
            reject = outcome.reject
            assert reject is not None
            # The run id is stamped here rather than inside the shared door: the engine
            # is run-agnostic so the same code can serve recon and the smoke tests.
            # batch_id stays 0 — batches are 200 *written* documents, and nothing is
            # written yet; the authoritative numbering is assigned in json_producer.
            yield beam.pvalue.TaggedOutput(
                self.REJECTED,
                Reject(
                    run_id=self.run_id,
                    batch_id=0,
                    source_key=reject.source_key,
                    stage=reject.stage,
                    reason=reject.reason,
                    detail=reject.detail,
                    raw_record=reject.raw_record,
                ).to_dict(),
            )
            return

        fields = outcome.doc
        assert fields is not None
        self.c_written.inc()
        row = dict(row_from_fields(fields, {}))
        row.update(
            {
                "_source_file": source_file,
                "_source_key": outcome.source_key,
                "_account_key": outcome.account_key,
            }
        )
        yield row


class WriteRowsFn(beam.DoFn):
    """Batched writes into BigQuery, through whichever writer the backend selects.

    This class does not choose the write path: `sinks.bigquery_writer(cfg)` does, in
    `setup`. The emulator implements no load jobs, so locally it is `insertAll`; on real
    BigQuery it is `FileLoadsBigQueryWriter`, which stages newline-delimited JSON in GCS
    and issues a load job, because `insertAll` is quota-limited and priced per row at the
    volumes this exists for. Writes below its threshold still stream, load jobs being
    capped per table per day.
    """

    def __init__(self, cfg: Config, dataset: str, table: str) -> None:
        self.cfg = cfg
        self.dataset = dataset
        self.table = table

    def setup(self) -> None:
        # The writer is chosen by the backend, not by this class: insertAll against the
        # emulator, GCS-staged load jobs against real BigQuery. Resolving it in `setup`
        # rather than `__init__` keeps the DoFn picklable and gives each worker its own
        # client.
        self.writer = bigquery_writer(self.cfg)

    def process(self, batch: list[dict]) -> Iterable[Any]:
        if batch:
            self.writer.write(self.dataset, self.table, batch)
        return []


# ──────────────────────────────────────────────────────────────────── the pipeline


def run_pipeline(cfg: Config, run_id: str, record_name: str = "ACCOUNT") -> Counters:
    """Build and execute the file processor, returning the door tallies."""
    mapping = load_mapping(cfg.mapping_path)
    ingested_at = datetime.now(timezone.utc).isoformat()

    # Tables follow the contract layer, so a TDS change is a schema change.
    bq = BigQuery(cfg)
    bq.ensure_dataset(cfg.ds_extraction)
    bq.ensure_table(cfg.ds_extraction, SRC_TABLE, bq_schema_from_tds(mapping.src_record))
    bq.ensure_dataset(cfg.ds_recon)
    bq.ensure_table(cfg.ds_recon, REJECT_TABLE, REJECT_SCHEMA)
    bq.ensure_table(cfg.ds_recon, LINEAGE_TABLE, LINEAGE_SCHEMA)

    # Re-running a run id must be idempotent across every table it touches. This is now
    # the *only* idempotency mechanism (docs/PLAN-CHANGES-21082026.md D3) — there is no
    # dedup-key collapsing to fall back on.
    #
    # The DELETE is issued only when the run id actually has rows. That is not an
    # optimisation: these tables are written with the streaming API, and BigQuery rejects
    # DML against a table with a non-empty streaming buffer —
    #
    #   UPDATE or DELETE statement over table … would affect rows in the streaming
    #   buffer, which is not supported
    #
    # — for up to ~90 minutes after the last insert. The tables are neither partitioned
    # nor clustered, so BigQuery cannot prove the predicate misses the buffer and refuses
    # the statement whatever the WHERE clause says. An unconditional DELETE therefore made
    # a *first* run of a fresh run id fail whenever any other run had streamed recently —
    # which is exactly how the 2026-08-20 DAG run failed, minutes after a `make smoke-gcp`
    # had streamed into the same table under a different run id. Counting first costs one
    # cheap query and removes DML entirely from the case that never needed it.
    #
    # Re-running a run id whose own rows are still buffered still fails, and still should:
    # there is no way to delete those rows, so silently appending would double-count them.
    for dataset, table, column in (
        (cfg.ds_extraction, SRC_TABLE, "_run_id"),
        (cfg.ds_recon, REJECT_TABLE, "run_id"),
        (cfg.ds_recon, LINEAGE_TABLE, "run_id"),
    ):
        fq = f"`{cfg.project}.{dataset}.{table}`"
        existing = next(
            iter(
                bq.client.query(
                    f"SELECT COUNT(*) AS n FROM {fq} WHERE {column} = '{run_id}'"
                ).result()
            )
        ).n
        if existing:
            bq.client.query(f"DELETE FROM {fq} WHERE {column} = '{run_id}'").result()

    def to_src_row(row: dict) -> dict:
        out = dict(row)
        out.update(
            {
                "_run_id": run_id,
                "_batch_id": 0,
                "_ingested_at": ingested_at,
            }
        )
        return out

    options = pipeline_options(cfg)
    pipeline = beam.Pipeline(options=options)

    records = (
        pipeline
        | "Trigger" >> beam.Create([run_id])
        | "ReadBundle" >> beam.ParDo(ReadBundleFn(cfg, run_id, record_name))
    )

    parsed = records | "Parse" >> beam.ParDo(ParseFn(cfg, run_id, ingested_at)).with_outputs(
        ParseFn.REJECTED, main="written"
    )

    (
        parsed["written"]
        | "ToSrcRow" >> beam.Map(to_src_row)
        | "BatchSrc" >> beam.BatchElements(min_batch_size=100, max_batch_size=500)
        | "WriteSrc" >> beam.ParDo(WriteRowsFn(cfg, cfg.ds_extraction, SRC_TABLE))
    )

    (
        parsed[ParseFn.REJECTED]
        | "BatchRejects" >> beam.BatchElements(min_batch_size=1, max_batch_size=500)
        | "WriteRejects" >> beam.ParDo(WriteRowsFn(cfg, cfg.ds_recon, REJECT_TABLE))
    )

    # Every not-migrated record, named. Rejects arrive as a reject row, so they are
    # projected onto the `Lineage` shape here rather than emitted twice from the DoFn.
    def reject_to_lineage(row: dict) -> dict:
        return Lineage(
            run_id=row["run_id"],
            # A parse reject has no source key — it is named by its own content.
            source_key=row["source_key"] or content_name(row["raw_record"]),
            # A record that failed to parse never reached an account key.
            account_key=None,
            door=Door.REJECTED.value,
            stage=row["stage"],
            reason=row["reason"],
            detail=row["detail"],
            source_file="",
            created_at=ingested_at,
        ).to_dict()

    (
        parsed[ParseFn.REJECTED]
        | "RejectToLineage" >> beam.Map(reject_to_lineage)
        | "BatchLineage" >> beam.BatchElements(min_batch_size=1, max_batch_size=500)
        | "WriteLineage" >> beam.ParDo(WriteRowsFn(cfg, cfg.ds_recon, LINEAGE_TABLE))
    )

    result = pipeline.run()
    result.wait_until_finish()

    counters = _read_counters(result)
    # Parse must account for every record this stage read.
    require_balance(f"file_processor run {run_id}", counters)

    # Persist the tallies so reconciliation reads what this stage observed rather than
    # re-deriving it from the tables. The tallies are the claim; `record_lineage` above
    # is the per-record evidence behind them, so a rejected record is recoverable by
    # key and not only by count.
    bq.ensure_table(cfg.ds_recon, LEDGER_TABLE, LEDGER_SCHEMA)
    bq.client.query(
        f"DELETE FROM `{cfg.project}.{cfg.ds_recon}.{LEDGER_TABLE}` WHERE run_id = '{run_id}'"
    ).result()
    bq.insert(
        cfg.ds_recon,
        LEDGER_TABLE,
        [
            {
                "run_id": run_id,
                "src_read": counters.src_read,
                # Named for what it counts. It was "target_written", which invited the
                # reading "42 records reached Target System" when the number is rows into
                # account_src — map and schema settle the remaining disposition
                # downstream, so the real TARGET count for the same run can be lower.
                # Both balance, at different stages; only the column name suggested
                # otherwise.
                "extraction_written": counters.written,
                "rejected": counters.rejected,
                "balanced": counters.balances,
                "created_at": ingested_at,
            }
        ],
    )
    return counters


def _read_counters(result: Any) -> Counters:
    def value(name: str) -> int:
        filtered = result.metrics().query(
            beam.metrics.MetricsFilter().with_namespace("file_processor").with_name(name)
        )
        return sum(c.result for c in filtered["counters"]) if filtered["counters"] else 0

    counters = Counters()
    counters.src_read = value("src_read")
    counters.written = value("written_to_extraction")
    counters.rejected = value("rejected")
    return counters


def main() -> None:
    ap = argparse.ArgumentParser(description="Dataflow File Processor / Data Loader")
    # Both dash and underscore forms: the local orchestrator calls `--run-id` while a
    # Dataflow Flex Template launcher passes parameters by their underscore key.
    ap.add_argument("--run-id", "--run_id", dest="run_id", required=True)
    ap.add_argument("--record", default="ACCOUNT")
    args = ap.parse_args()
    require_identifier("--run-id", args.run_id)

    cfg = Config.from_env()
    counters = run_pipeline(cfg, args.run_id, args.record)
    print("file_processor:", json.dumps(counters.to_dict(), sort_keys=True))


if __name__ == "__main__":
    main()
