"""Derive BigQuery schemas from TDS definitions.

The contract layer is the single source of truth: adding a field to a TDS gives the
BigQuery table that column, with no separate DDL to keep in sync. This is what makes
"a new project is new YAML + TDS only" hold on the warehouse side too.

Decimals are carried as STRING deliberately — the same reasoning as the TARGET JSON
Schema. NUMERIC round-trips are not worth the precision risk in a banking migration,
and Dataform casts explicitly where arithmetic is actually needed.
"""

from __future__ import annotations

import json
from datetime import date
from decimal import Decimal

from google.cloud import bigquery

from .tds import Record

_BQ_TYPES = {
    "string": "STRING",
    "int": "INT64",
    "decimal": "STRING",
    "date": "DATE",
    "json": "STRING",
}

# Columns every ingested row carries, so reconciliation can scope by run — one full
# snapshot per `_run_id` — and trace a row back to the exact source line it came from.
INGEST_FIELDS = [
    bigquery.SchemaField("_run_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("_batch_id", "INT64"),
    bigquery.SchemaField("_source_file", "STRING"),
    bigquery.SchemaField("_source_key", "STRING"),
    bigquery.SchemaField("_account_key", "STRING"),
    bigquery.SchemaField("_ingested_at", "TIMESTAMP"),
]


def bq_schema_from_tds(record: Record) -> list[bigquery.SchemaField]:
    """Map a TDS record's fields onto BigQuery columns, plus the ingest metadata."""
    fields = [
        bigquery.SchemaField(
            f.name,
            _BQ_TYPES.get(f.type, "STRING"),
            mode="REQUIRED" if f.required else "NULLABLE",
        )
        for f in record.fields
    ]
    return fields + INGEST_FIELDS


def row_from_fields(fields: dict, meta: dict) -> dict:
    """Render a parsed record as a BigQuery row: JSON-safe values plus ingest metadata."""
    row: dict = {}
    for key, value in fields.items():
        if value is None:
            row[key] = None
        elif isinstance(value, Decimal):
            row[key] = str(value)
        elif isinstance(value, date):
            row[key] = value.isoformat()
        elif isinstance(value, (dict, list)):
            row[key] = json.dumps(value, sort_keys=True)
        else:
            row[key] = value
    row.update(meta)
    return row
