# Project, billing link, APIs and the Terraform state bucket.
#
# This module is applied on its own first (`terraform apply -target=module.bootstrap`)
# with local state, because Terraform cannot store state in a bucket it has not created
# yet. docs/runbook-gcp.md walks through the sequence.

variable "project_id" { type = string }
variable "project_name" {
  type    = string
  default = "MIG 000001-1 Migration"
}
variable "billing_account" {
  type        = string
  description = "Must be an OPEN billing account. On the machine this was written, both available accounts were closed — see the runbook."
}
variable "org_id" {
  type    = string
  default = ""
}
variable "folder_id" {
  type    = string
  default = ""
}
variable "region" { type = string }
variable "create_project" {
  type        = bool
  default     = true
  description = "False when pointing at a project that already exists."
}

resource "google_project" "this" {
  count = var.create_project ? 1 : 0

  project_id      = var.project_id
  name            = var.project_name
  billing_account = var.billing_account
  org_id          = var.org_id != "" ? var.org_id : null
  folder_id       = var.folder_id != "" ? var.folder_id : null

  # Otherwise `terraform destroy` orphans the default network.
  auto_create_network = false
}

locals {
  services = [
    "cloudresourcemanager.googleapis.com",
    "compute.googleapis.com",
    "storage.googleapis.com",
    "bigquery.googleapis.com",
    "dataflow.googleapis.com",
    "dataform.googleapis.com",
    "composer.googleapis.com",
    "artifactregistry.googleapis.com",
    "secretmanager.googleapis.com",
    "managedkafka.googleapis.com",
    "cloudbuild.googleapis.com",
    # Cloud Run hosts the Target System stand-in the Load lane posts to.
    "run.googleapis.com",
    # Serverless VPC Access: the connector the Cloud Run mock egresses through to reach
    # VPC-internal Managed Kafka. Only the connector resource needs it, but the API must be
    # on before module.vpc_connector can create that resource. Gated at apply time by
    # enable_kafka (the connector is count=0 without it), so enabling the API here costs
    # nothing on a no-Kafka run — an enabled API is not a billed resource.
    "vpcaccess.googleapis.com",
    "monitoring.googleapis.com",
    "logging.googleapis.com",
    "iam.googleapis.com",
  ]
}

resource "google_project_service" "this" {
  for_each = toset(local.services)

  project = var.project_id
  service = each.value

  # Disabling APIs on destroy breaks other things in a shared project and is rarely
  # what anyone wants.
  disable_on_destroy = false

  depends_on = [google_project.this]
}

# Terraform state. Versioned, because losing migration state is not recoverable.
resource "google_storage_bucket" "tfstate" {
  name     = "${var.project_id}-tfstate"
  project  = var.project_id
  location = var.region

  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
  force_destroy               = false

  versioning {
    enabled = true
  }

  depends_on = [google_project_service.this]
}
