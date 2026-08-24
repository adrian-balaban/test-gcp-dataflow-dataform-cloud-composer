"""Output adapters — the seam that keeps "where things are written" out of the pipelines.

Two ports, each with a local and a real-GCP implementation:

* **BigQuery** — `insertAll` on the emulator, which implements no load jobs; NDJSON
  staged in GCS and loaded with a load job on real BigQuery, which is what the row
  target requires. Small writes take the `insertAll` path in both worlds.
* **TargetWriter** — TARGET documents go to *both* sinks in architecture diagram's Load lane:
  Kafka in 200-element batches and JSON files in GCS (what the diagram draws). Choosing
  between them, or running both, is configuration.

The Kafka contract is ambiguous in the one detail that matters: "batches of 200" could
mean 200 messages per produce request or one message holding an array of 200. Until that
is answered this implements *one message per record, 200 per produce request*, isolated
in a single class.
"""

from __future__ import annotations

import base64
import json
import os
import time
import uuid
from typing import Any, Iterable, Protocol

from google.cloud import bigquery

from .config import Config
from .storage import BigQuery, Gcs


# ──────────────────────────────────────────────────────────────── BigQuery writing


class BigQueryWriter(Protocol):
    def write(self, dataset: str, table: str, rows: Iterable[dict[str, Any]]) -> int: ...


class InsertAllBigQueryWriter:
    """`tabledata.insertAll` — the only write path the emulator implements.

    The goccy emulator has no load-job support at all, so locally this is not a choice.
    It is also correct for small real writes (the run ledger is one row), which is why
    the GCP writer below delegates to it rather than staging a file for nine rows.
    """

    def __init__(self, cfg: Config) -> None:
        self.bq = BigQuery(cfg)

    def write(self, dataset: str, table: str, rows: Iterable[dict[str, Any]]) -> int:
        rows = list(rows)
        self.bq.insert(dataset, table, rows)
        return len(rows)


class FileLoadsBigQueryWriter:
    """Real-GCP path: newline-delimited JSON staged in GCS, then one load job.

    This is the change that matters at the 20B-row target. `insertAll` is quota-limited
    (rows/second per table), priced per row, and streams into a buffer that then has to
    be merged; a load job is free, bulk, and atomic per job. Before this class existed
    the "two writers" in this module had identical bodies and neither was called by
    anything — the seam looked real in the type system and did nothing on the data path.

    Small writes still go through `insertAll`: below `min_load_rows` the staging round
    trip costs more than it saves, and BigQuery caps load jobs at 1,500 per table per
    day, which a per-batch load job would burn through at volume. The threshold is what
    keeps both paths honest — bulk where bulk pays, streaming where it does not.
    """

    #: Below this, a load job is more overhead than it is worth.
    min_load_rows = 500

    def __init__(self, cfg: Config, staging_bucket: str | None = None) -> None:
        self.cfg = cfg
        self.bq = BigQuery(cfg)
        self.gcs = Gcs(cfg)
        # Dataflow's own temp bucket: it already exists, is regional with the job, and
        # has a lifecycle rule, so staged load files do not accumulate.
        self.staging_bucket = staging_bucket or f"{cfg.project}-dataflow-temp"
        self.fallback = InsertAllBigQueryWriter(cfg)

    def write(self, dataset: str, table: str, rows: Iterable[dict[str, Any]]) -> int:
        rows = list(rows)
        if not rows:
            return 0
        if len(rows) < self.min_load_rows:
            return self.fallback.write(dataset, table, rows)

        name = f"bqload/{dataset}/{table}/{uuid.uuid4().hex}.jsonl"
        payload = "\n".join(json.dumps(row, default=str) for row in rows).encode("utf-8")
        self.gcs.put(self.staging_bucket, name, payload)

        try:
            job = self.bq.client.load_table_from_uri(
                f"gs://{self.staging_bucket}/{name}",
                f"{self.cfg.project}.{dataset}.{table}",
                job_config=bigquery.LoadJobConfig(
                    source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                    # The table is created by `ensure_table` from the TDS contract, so the
                    # load must never invent a schema of its own — a load job that guesses
                    # is how a column silently changes type between runs.
                    autodetect=False,
                    write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                ),
            )
            job.result()
            if job.errors:
                raise RuntimeError(f"BigQuery load job failed: {job.errors}")
        finally:
            # The staged file has served its purpose either way; leaving it behind on
            # failure would just make the next debugging session read stale data.
            self.gcs.delete(self.staging_bucket, name)
        return len(rows)


def bigquery_writer(cfg: Config) -> BigQueryWriter:
    """The write path for this backend — insertAll locally, load jobs on real BigQuery."""
    return InsertAllBigQueryWriter(cfg) if cfg.bq_is_emulator else FileLoadsBigQueryWriter(cfg)


# ─────────────────────────────────────────────────────────── TARGET document sinks


class TargetWriter(Protocol):
    """Where transformed TARGET documents leave the Transformation lane."""

    def write_batch(self, batch_id: int, documents: list[dict[str, Any]]) -> None: ...

    def close(self) -> dict[str, Any]: ...


def _oauth_token_cb(_config: str = "") -> tuple[str, float]:
    """confluent_kafka OAUTHBEARER callback: return (token, expiry_epoch_seconds).

    Reuses the same cloud-platform access token the Java side feeds to
    GcpTokenOauthCallbackHandler. run_pipeline.py gcp_endpoints() sets MIG_KAFKA_TOKEN
    alongside MIG_GCS_TOKEN from the same google.auth.default() call; when that env var
    is absent (e.g. the producer is constructed in a context that did not run the local
    orchestrator's gcp_endpoints), mint one from ADC on demand. The 1h expiry matches a
    typical access-token TTL and what the Java callback claims.

    The `_config` parameter is not optional decoration: librdkafka invokes the callback
    with the `sasl.oauthbearer.config` string, so a zero-arg callable raises TypeError
    inside the client's service thread, the token is never set, and the handshake dies
    with "OAuth token not set within 10 seconds timeout" — an error that names the
    symptom and not the signature (2026-08-23, json_producer on the DAG).
    """

    token = os.environ.get("MIG_KAFKA_TOKEN")
    principal = os.environ.get("GOOGLE_MANAGED_KAFKA_AUTH_PRINCIPAL", "")
    if not token or not principal:
        import google.auth
        import google.auth.transport.requests

        creds, _ = google.auth.default(
            scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        creds.refresh(google.auth.transport.requests.Request())
        token = token or creds.token
        principal = principal or _principal(creds)
    expiry = time.time() + 3600
    return _kafka_token(token, principal, expiry), expiry


def _principal(creds: Any) -> str:
    """The service-account email the Kafka token claims as `sub`.

    google.auth exposes it under different names per credential type — `service_account_email`
    on Compute/Impersonated credentials, none at all on user ADC — so fall back to the
    metadata server, which is what a Dataflow worker or Composer pod resolves to anyway.
    """
    email = getattr(creds, "service_account_email", None)
    if email and email != "default":
        return email
    import urllib.request

    request = urllib.request.Request(
        "http://metadata.google.internal/computeMetadata/v1/instance/"
        "service-accounts/default/email",
        headers={"Metadata-Flavor": "Google"},
    )
    with urllib.request.urlopen(request, timeout=5) as response:  # noqa: S310
        return response.read().decode("utf-8").strip()


def _kafka_token(access_token: str, principal: str, expiry: float) -> str:
    """Wrap the access token the way Google Managed Kafka validates it.

    The broker does not accept a bare access token. It expects a dot-joined, base64url,
    JWT-shaped value — `b64(header).b64(claims).b64(accessToken)` with
    `alg=GOOG_OAUTH2_TOKEN`, `scope=kafka` and `sub` naming the service account. Sending
    the raw token authenticates nothing and fails with "invalid credentials with SASL
    mechanism OAUTHBEARER", which names the mechanism rather than the encoding
    (2026-08-23, json_producer on the DAG). Same construction as the Java side's
    ro.mig.common.GcpTokenOauthCallbackHandler, and as Google's own GcpLoginCallbackHandler.
    """
    header = {"typ": "JWT", "alg": "GOOG_OAUTH2_TOKEN"}
    claims = {
        "exp": int(expiry),
        "iat": int(time.time()),
        "scope": "kafka",
        "sub": principal,
    }
    return ".".join(
        _b64(json.dumps(part, separators=(",", ":")))
        for part in (header, claims)
    ) + "." + _b64(access_token)


def _b64(data: str) -> str:
    return base64.urlsafe_b64encode(data.encode("utf-8")).decode("utf-8").rstrip("=")


class KafkaTargetWriter:
    """Emits TARGET records to Kafka in batches of 200 — the Loader contract.

    Idempotence is configured on the producer and every message carries the engine's
    deterministic dedup key, so a replayed batch is safe downstream.
    """

    def __init__(self, cfg: Config) -> None:
        from confluent_kafka import Producer

        conf: dict[str, Any] = {
            "bootstrap.servers": cfg.kafka_bootstrap,
            "security.protocol": cfg.kafka_security_protocol,
            "enable.idempotence": True,
            "acks": "all",
            "retries": 5,
            "linger.ms": 20,
        }
        # Real GCP Managed Kafka authenticates the service account via SASL/OAUTHBEARER;
        # locally redpanda is PLAINTEXT. See docs/runbook-gcp.md.
        if cfg.kafka_security_protocol != "PLAINTEXT":
            conf["sasl.mechanisms"] = "OAUTHBEARER"
            # confluent_kafka needs an oauth_cb returning (token, expiry_epoch_seconds)
            # alongside the mechanism — the mechanism alone is a 4-line config that
            # builds a client which then fails the handshake with no callback. The token
            # is the same cloud-platform access token the Java side gets via
            # GcpTokenOauthCallbackHandler (run_pipeline.py gcp_endpoints() sets
            # MIG_KAFKA_TOKEN alongside MIG_GCS_TOKEN). When unset we mint one from ADC,
            # mirroring the MIG_GCS_TOKEN seam so a caller that set neither still works.
            def _cb(config: str = "") -> tuple[str, float]:
                token_pair = _oauth_token_cb(config)
                self._token_set = True
                return token_pair

            conf["oauth_cb"] = _cb

        self._token_set = False
        self.producer = Producer(conf)
        # Prime the OAUTHBEARER handshake. librdkafka only invokes `oauth_cb` from the
        # client's service queue, and nothing services that queue until the first
        # produce/poll — so a producer built and then immediately handed a batch fails
        # the handshake with
        #   KafkaError{code=SASL_AUTHENTICATION_FAILED,str="OAuth token not set within
        #   10 seconds timeout"}
        # before a single message is sent (2026-08-23, json_producer on the DAG). One
        # poll() per iteration runs the callback; the loop stops as soon as the token is
        # set (the client reports no more auth errors) and gives up after ~10s rather
        # than hanging, leaving the real failure to surface on flush() as before.
        if cfg.kafka_security_protocol != "PLAINTEXT":
            deadline = time.time() + 10
            while time.time() < deadline:
                self.producer.poll(0.5)
                if self._token_set:
                    break
        self.bootstrap = cfg.kafka_bootstrap
        self.topic = cfg.kafka_topic
        self.batch_size = cfg.kafka_batch_size
        self.batches = 0
        self.messages = 0
        self._errors: list[str] = []

    def _on_delivery(self, err, _msg) -> None:
        if err is not None:
            self._errors.append(str(err))

    def write_batch(self, batch_id: int, documents: list[dict[str, Any]]) -> None:
        for doc in documents:
            self.producer.produce(
                topic=self.topic,
                key=doc["migration"]["dedupKey"].encode("utf-8"),
                value=json.dumps(doc, sort_keys=True).encode("utf-8"),
                headers={
                    "batch-id": str(batch_id),
                    "run-id": doc["migration"]["runId"],
                    "idempotency-key": doc["migration"]["dedupKey"],
                },
                on_delivery=self._on_delivery,
            )
            self.messages += 1
        # One flush per batch: the produce request boundary IS the batch boundary.
        # flush() returns the number of messages STILL QUEUED when it gives up. Ignoring
        # that return value is how a dead broker passes for success: with nothing
        # listening, every message sits in the local queue until the timeout, the
        # delivery callback is never invoked at all, and `_errors` stays empty. The
        # first Composer run lost 400 records this way — two green 30s batches, zero
        # messages in Kafka, nobody told. Undelivered is a failure, not a slow success.
        self._check_delivered(self.producer.flush(30))
        self.batches += 1

    def _check_delivered(self, still_queued: int) -> None:
        if still_queued:
            raise RuntimeError(
                f"Kafka flush timed out with {still_queued} message(s) undelivered to "
                f"{self.topic!r} at {self.bootstrap!r} — the broker never acknowledged them"
            )
        if self._errors:
            raise RuntimeError(f"Kafka delivery failed for {len(self._errors)} messages: {self._errors[:3]}")

    def close(self) -> dict[str, Any]:
        self._check_delivered(self.producer.flush(30))
        return {"sink": "kafka", "topic": self.topic, "batches": self.batches, "messages": self.messages}


class GcsJsonTargetWriter:
    """Writes TARGET documents as JSON files for the Loader App to download.

    This is the path architecture diagram draws: Dataflow JSON Data Producer -> File Storage ->
    Loader App -> Target System. One file per batch keeps the 200-record grouping visible
    on the object store, which the load-side reconciliation then counts.
    """

    def __init__(self, cfg: Config, run_id: str) -> None:
        self.gcs = Gcs(cfg)
        self.bucket = cfg.json_bucket
        self.prefix = f"json/{run_id}/"
        self.batches = 0
        self.documents = 0
        self.files: list[str] = []

    def write_batch(self, batch_id: int, documents: list[dict[str, Any]]) -> None:
        name = f"{self.prefix}ACCOUNT.{batch_id:05d}.json"
        # JSON Lines: one document per line, so the Loader can stream a batch.
        body = "\n".join(json.dumps(d, sort_keys=True) for d in documents) + "\n"
        self.gcs.put_text(self.bucket, name, body, content_type="application/x-ndjson")
        self.files.append(name)
        self.batches += 1
        self.documents += len(documents)

    def close(self) -> dict[str, Any]:
        return {
            "sink": "gcs-json",
            "bucket": self.bucket,
            "prefix": self.prefix,
            "batches": self.batches,
            "documents": self.documents,
            "files": self.files,
        }


class FanOutTargetWriter:
    """Writes every batch to several sinks — the default, so both paths stay exercised."""

    def __init__(self, writers: list[TargetWriter]) -> None:
        self.writers = writers

    def write_batch(self, batch_id: int, documents: list[dict[str, Any]]) -> None:
        for writer in self.writers:
            writer.write_batch(batch_id, documents)

    def close(self) -> dict[str, Any]:
        return {"sinks": [w.close() for w in self.writers]}


def target_writer(cfg: Config, run_id: str, sinks: str = "both") -> TargetWriter:
    """`sinks` is one of `kafka`, `gcs`, `both`.

    `both` means "every sink that is configured". Kafka is optional infrastructure
    (`enable_kafka=false` by default, and the cluster is torn down between runs), so an
    empty bootstrap address under `both` drops the Kafka writer rather than pointing it
    at the localhost default — inside a Dataflow worker that address is nothing at all.
    Asking for `kafka` explicitly with no address is still an error: that is a caller
    who wants Kafka and would otherwise be silently given nothing.
    """
    writers: list[TargetWriter] = []
    if sinks == "kafka" and not cfg.kafka_bootstrap:
        raise ValueError("--sinks kafka requires KAFKA_BOOTSTRAP to be set")
    if sinks in ("kafka", "both") and cfg.kafka_bootstrap:
        writers.append(KafkaTargetWriter(cfg))
    if sinks in ("gcs", "both"):
        writers.append(GcsJsonTargetWriter(cfg, run_id))
    if not writers:
        raise ValueError(f"no sink selected: {sinks!r}")
    return writers[0] if len(writers) == 1 else FanOutTargetWriter(writers)
