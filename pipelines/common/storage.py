"""Object storage and BigQuery client adapters.

These two factories are the whole local-vs-GCP seam on the Python side. Locally they
point the standard Google clients at fake-gcs-server and the BigQuery emulator with
anonymous credentials; with `TARGET_PROFILE=real` they fall through to Application
Default Credentials and the real endpoints — same client library, same call sites.
"""

from __future__ import annotations

import json
from typing import Any, Iterable

from google.api_core.client_options import ClientOptions
from google.auth.credentials import AnonymousCredentials
from google.cloud import bigquery, storage
from google.cloud.exceptions import NotFound

from .config import Config


# ─────────────────────────────────────────────────────────────────── object storage


def storage_client(cfg: Config) -> storage.Client:
    if cfg.is_local:
        return storage.Client(
            project=cfg.project,
            credentials=AnonymousCredentials(),
            client_options=ClientOptions(api_endpoint=cfg.storage_host),
        )
    return storage.Client(project=cfg.project)


class Gcs:
    """Thin convenience wrapper so callers deal in bytes and names, not blobs."""

    def __init__(self, cfg: Config) -> None:
        self.client = storage_client(cfg)

    def ensure_bucket(self, bucket: str) -> None:
        if not self.client.lookup_bucket(bucket):
            self.client.create_bucket(bucket)

    def put(self, bucket: str, name: str, data: bytes, content_type: str = "application/octet-stream") -> None:
        self.client.bucket(bucket).blob(name).upload_from_string(data, content_type=content_type)

    def put_text(self, bucket: str, name: str, text: str, content_type: str = "text/plain") -> None:
        self.put(bucket, name, text.encode("utf-8"), content_type)

    def put_json(self, bucket: str, name: str, obj: Any) -> None:
        self.put_text(bucket, name, json.dumps(obj, indent=2, sort_keys=True), "application/json")

    def get(self, bucket: str, name: str) -> bytes:
        return self.client.bucket(bucket).blob(name).download_as_bytes()

    def get_json(self, bucket: str, name: str) -> Any:
        return json.loads(self.get(bucket, name).decode("utf-8"))

    def list(self, bucket: str, prefix: str = "") -> list[str]:
        return sorted(b.name for b in self.client.list_blobs(bucket, prefix=prefix))

    def exists(self, bucket: str, name: str) -> bool:
        return self.client.bucket(bucket).blob(name).exists()

    def delete(self, bucket: str, name: str) -> None:
        """Delete if present. Absent is success: the caller wanted it gone."""
        try:
            self.client.bucket(bucket).blob(name).delete()
        except NotFound:
            pass


# ────────────────────────────────────────────────────────────────────────── bigquery


def bigquery_client(cfg: Config) -> bigquery.Client:
    if cfg.bq_is_emulator:
        return bigquery.Client(
            project=cfg.project,
            credentials=AnonymousCredentials(),
            client_options=ClientOptions(api_endpoint=cfg.bq_host),
        )
    # BQ_TARGET=real — the free sandbox tier is enough for this prototype.
    return bigquery.Client(project=cfg.project)


class BigQuery:
    """Insert and query helpers that behave the same on the emulator and on real BQ."""

    def __init__(self, cfg: Config) -> None:
        self.cfg = cfg
        self.client = bigquery_client(cfg)

    def ensure_dataset(self, dataset: str) -> None:
        self._ensure(
            f"{self.cfg.project}.{dataset}",
            self.client.get_dataset,
            lambda ref: self.client.create_dataset(bigquery.Dataset(ref), retry=None),
        )

    def ensure_table(self, dataset: str, table: str, schema: list[bigquery.SchemaField]) -> None:
        self._ensure(
            f"{self.cfg.project}.{dataset}.{table}",
            self.client.get_table,
            lambda ref: self.client.create_table(bigquery.Table(ref, schema=schema), retry=None),
        )

    @staticmethod
    def _ensure(ref: str, get, create) -> None:
        """Idempotent create-if-absent.

        Deliberately checks first rather than relying on `exists_ok=True`. The BigQuery
        emulator answers a duplicate create with **HTTP 500 "already created"** instead
        of the 409 the client expects, so `exists_ok` does not recognise it and the
        default retry policy then hammers the endpoint for its full 600s deadline.
        Retries are disabled on the create for the same reason: a 500 here means
        "already there", not "try again".
        """
        try:
            get(ref)
            return
        except NotFound:
            pass
        try:
            create(ref)
        except Exception as exc:  # noqa: BLE001 — narrow check on the message follows
            if "already" not in str(exc).lower():
                raise

    def insert(self, dataset: str, table: str, rows: Iterable[dict[str, Any]]) -> None:
        """Batched `insertAll` — the write path in **both** worlds today.

        The BigQuery emulator implements no load jobs, so locally there is no choice.
        On real GCP this is now the *small-write* path only: `sinks.bigquery_writer`
        returns `FileLoadsBigQueryWriter` there, which stages NDJSON in GCS and issues a
        load job for anything of size, because `insertAll` is quota-limited, priced per
        row and streaming rather than bulk. Below that threshold the staging round trip
        costs more than it saves, so this method still carries the run ledger and other
        handful-of-rows writes in both worlds.
        """
        rows = list(rows)
        if not rows:
            return
        table_ref = self.client.get_table(f"{self.cfg.project}.{dataset}.{table}")
        for chunk_start in range(0, len(rows), 500):
            chunk = rows[chunk_start : chunk_start + 500]
            errors = self.client.insert_rows_json(table_ref, chunk)
            if errors:
                raise RuntimeError(f"BigQuery insert into {dataset}.{table} failed: {errors[:3]}")

    def query(self, sql: str) -> list[dict[str, Any]]:
        return [dict(row) for row in self.client.query(sql).result()]

    def truncate(self, dataset: str, table: str) -> None:
        self.client.query(f"DELETE FROM `{self.cfg.project}.{dataset}.{table}` WHERE TRUE").result()


# ─────────────────────────────────────────────────────────────────── secret manager


def secret_manager_client(cfg: Config):
    """Google Secret Manager client. Local mode is unused — secrets are a real-GCP only
    concern; locally the PGP key sits on disk and Target System credentials come from .env."""
    from google.cloud import secretmanager

    return secretmanager.SecretManagerServiceClient()


class Secrets:
    """Read-only accessor for the Secret Manager entries Terraform declared.

    The secret ids are the fixed conventions from terraform/modules/secrets/main.tf:
    `pgp-private-key`, `pgp-passphrase`, `target-system-credentials`, `dataform-git-token`.
    """

    def __init__(self, cfg: Config) -> None:
        self.cfg = cfg
        self.client = secret_manager_client(cfg)

    def version_name(self, secret_id: str, version: str = "latest") -> str:
        return f"projects/{self.cfg.project}/secrets/{secret_id}/versions/{version}"

    def get(self, secret_id: str, version: str = "latest") -> bytes:
        response = self.client.access_secret_version(
            request={"name": self.version_name(secret_id, version)}
        )
        return response.payload.data
