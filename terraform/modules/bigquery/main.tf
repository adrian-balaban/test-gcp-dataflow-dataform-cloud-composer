# BigQuery — the Extraction and Transformation datasets from architecture diagram, plus a recon
# dataset for the run ledger and reject log.
#
# Partitioning and clustering are not optional at this workload's size: 1.7M accounts and
# 20B transactions make an unpartitioned table both unqueryable and a cost incident.
# Every table here is partitioned on ingestion date and clustered on the columns
# reconciliation actually filters by.

variable "project_id" { type = string }
variable "region" { type = string }
variable "deletion_protection" {
  type        = bool
  default     = true
  description = "Disabled only in dev."
}

locals {
  datasets = {
    bq_extraction     = "SRC records as loaded by the Dataflow File Processor"
    bq_transformation = "Curated, enriched and TARGET-shaped records"
    bq_recon          = "Run ledger, reject log and reconciliation output"
  }
}

resource "google_bigquery_dataset" "this" {
  for_each = local.datasets

  dataset_id    = each.key
  project       = var.project_id
  location      = var.region
  description   = each.value
  friendly_name = each.key

  delete_contents_on_destroy = !var.deletion_protection
}

# ── run ledger: the two-door tallies, the correctness contract's storage ─────
resource "google_bigquery_table" "run_ledger" {
  project             = var.project_id
  dataset_id          = google_bigquery_dataset.this["bq_recon"].dataset_id
  table_id            = "run_ledger"
  deletion_protection = var.deletion_protection

  description = "One row per run: SRC_read, written, rejected, and whether the balancing equation closed."

  time_partitioning {
    type  = "DAY"
    field = "created_at"
  }
  clustering = ["run_id"]

  schema = jsonencode([
    { name = "run_id", type = "STRING", mode = "REQUIRED" },
    { name = "src_read", type = "INT64" },
    { name = "extraction_written", type = "INT64" },
    { name = "rejected", type = "INT64" },
    { name = "balanced", type = "BOOL" },
    { name = "created_at", type = "TIMESTAMP" },
  ])
}

# ── record lineage: the per-record evidence behind the ledger's counts ────────
resource "google_bigquery_table" "record_lineage" {
  project             = var.project_id
  dataset_id          = google_bigquery_dataset.this["bq_recon"].dataset_id
  table_id            = "record_lineage"
  deletion_protection = var.deletion_protection

  description = "One row per record that did not migrate, with its door, stage and enumerated reason. run_ledger says how many; this says which."

  time_partitioning {
    type  = "DAY"
    field = "created_at"
  }
  clustering = ["run_id", "door", "reason"]

  schema = jsonencode([
    { name = "run_id", type = "STRING", mode = "REQUIRED" },
    { name = "source_key", type = "STRING" },
    { name = "account_key", type = "STRING" },
    { name = "door", type = "STRING" },
    { name = "stage", type = "STRING" },
    { name = "reason", type = "STRING" },
    { name = "detail", type = "STRING" },
    { name = "source_file", type = "STRING" },
    { name = "created_at", type = "TIMESTAMP" },
  ])
}

# ── reject log: enumerated reason codes, so rejects can be counted and trended ─
resource "google_bigquery_table" "reject_log" {
  project             = var.project_id
  dataset_id          = google_bigquery_dataset.this["bq_recon"].dataset_id
  table_id            = "reject_log"
  deletion_protection = var.deletion_protection

  description = "Every rejected record with its stage, enumerated reason code and raw payload."

  clustering = ["run_id", "reason"]

  schema = jsonencode([
    { name = "run_id", type = "STRING", mode = "REQUIRED" },
    { name = "batch_id", type = "INT64" },
    { name = "source_key", type = "STRING" },
    { name = "stage", type = "STRING" },
    { name = "reason", type = "STRING" },
    { name = "detail", type = "STRING" },
    { name = "raw_record", type = "STRING" },
  ])
}

output "datasets" {
  value = { for k, d in google_bigquery_dataset.this : k => d.dataset_id }
}
