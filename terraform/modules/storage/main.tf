# GCS buckets — the "File Storage" boxes in architecture diagram, on both sides of the diagram.

variable "project_id" { type = string }
variable "region" { type = string }
variable "force_destroy" {
  type        = bool
  default     = false
  description = "Only ever true in dev. A migration's landing bucket is evidence."
}
variable "landing_retention_days" {
  type        = number
  default     = 90
  description = "Encrypted source extracts. Long enough to re-run a migration wave."
}

locals {
  buckets = {
    landing = {
      # Extracts land here from the mainframe side, PGP-encrypted.
      lifecycle_days = var.landing_retention_days
      versioning     = true
    }
    json-out = {
      # TARGET documents awaiting the Loader.
      lifecycle_days = 30
      versioning     = false
    }
    recon = {
      # Reconciliation reports and the republished .RPT/.CHS/.ERR artefacts.
      # Kept longest: this is the audit trail for the migration.
      lifecycle_days = 365
      versioning     = true
    }
    dataflow-temp = {
      # Dataflow staging/temp. Short-lived by definition.
      lifecycle_days = 7
      versioning     = false
    }
    dataflow-templates = {
      # Flex Template specs. Versioned so a rollback is possible.
      lifecycle_days = 0
      versioning     = true
    }
  }
}

resource "google_storage_bucket" "this" {
  for_each = local.buckets

  name     = "${var.project_id}-${each.key}"
  project  = var.project_id
  location = var.region

  force_destroy               = var.force_destroy
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"

  versioning {
    enabled = each.value.versioning
  }

  dynamic "lifecycle_rule" {
    for_each = each.value.lifecycle_days > 0 ? [each.value.lifecycle_days] : []
    content {
      condition {
        age = lifecycle_rule.value
      }
      action {
        type = "Delete"
      }
    }
  }

  # Abort stalled resumable uploads so half-written extracts do not accumulate.
  lifecycle_rule {
    condition {
      age                    = 7
      with_state             = "ARCHIVED"
      num_newer_versions     = 3
    }
    action {
      type = "Delete"
    }
  }
}

output "buckets" {
  value = { for k, b in google_storage_bucket.this : k => b.name }
}

output "landing_bucket" { value = google_storage_bucket.this["landing"].name }
output "json_bucket" { value = google_storage_bucket.this["json-out"].name }
output "recon_bucket" { value = google_storage_bucket.this["recon"].name }
output "templates_bucket" { value = google_storage_bucket.this["dataflow-templates"].name }
