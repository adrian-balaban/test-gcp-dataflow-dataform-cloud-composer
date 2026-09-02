# Cloud Composer 2 — the orchestrator box in architecture diagram.
#
# COST WARNING: a Composer 2 environment runs roughly $300-400/month even when idle,
# because the GKE Autopilot cluster and the Airflow database exist regardless of whether
# a DAG is running. That is why `enable_composer` defaults to false in envs/dev: the rest
# of the stack can be applied and exercised for a few euros, and Composer is turned on
# deliberately.

variable "project_id" { type = string }
variable "region" { type = string }
variable "target_system_url" {
  type        = string
  default     = ""
  description = "Where the Loader posts. Empty means the Load lane has no target and will fail."
}
variable "java_image_tag" {
  type        = string
  default     = "latest"
  description = "Tag build_java_images.sh published — a git SHA, plus -dirty on an unclean tree."
}
variable "kafka_bootstrap" {
  type        = string
  default     = ""
  description = "Managed Kafka bootstrap address; empty drops the Kafka sink."
}
variable "kafka_topic" {
  type        = string
  default     = "target-system-target"
  description = "Topic json_producer publishes enriched TARGET documents to."
}
variable "target_system_confirmation_bootstrap" {
  type        = string
  default     = ""
  description = <<-EOT
    Managed Kafka bootstrap the target-system-mock publishes confirmation events to and
    recon-service consumes them from (docs/PLAN-CHANGES-22082026.md). Empty (the default
    when Kafka is off) means the mock skips the producer and recon skips the read, so a
    no-Kafka run stays green rather than failing on zero confirmations.
  EOT
}
variable "target_system_confirmation_topic" {
  type        = string
  default     = "target-system-confirmations"
  description = "Topic the mock writes confirmations to and recon reads them from."
}
variable "target_system_rejection_topic" {
  type        = string
  default     = "target-system-rejections"
  description = <<-EOT
    Topic the mock writes one rejection event per refused document to, and the loader
    reads to settle the run (docs/PLAN-CHANGES-02092026-kafka-loader.md). Together with
    the confirmation topic this is the Loader's verdict once the Load edge is Kafka
    rather than HTTP: confirmations become `accepted`, rejections become `.ERR` rows, and
    anything in neither is `unsettled` and fails the run.
  EOT
}
variable "loader_settle_timeout_seconds" {
  type        = number
  default     = 120
  description = <<-EOT
    How long the loader waits on the return topics before declaring the remaining
    documents unsettled. A new operational parameter with no HTTP analogue (plan Q5): too
    short and a slow consumer looks like data loss, too long and a genuinely stalled
    consumer holds the DAG open. 120s suits the prototype's ~400-document runs.
  EOT
}
variable "dataflow_subnetwork" {
  type        = string
  default     = ""
  description = "Subnetwork the Dataflow workers join. Wrong value = jobs that start and hang."
}
variable "dataflow_service_account" {
  type        = string
  default     = ""
  description = "Worker service account the Beam pods submit jobs as."
}
variable "enabled" {
  type    = bool
  default = false
}
variable "service_account" { type = string }
variable "network_id" { type = string }
variable "subnet_id" { type = string }
variable "datasets" { type = map(string) }
variable "buckets" { type = map(string) }
variable "airflow_version" {
  type    = string
  default = "composer-2.9.7-airflow-2.9.3"
}

# Composer v2 prerequisite: Google's Composer Service Agent SA must hold
# roles/composer.ServiceAgentV2Ext BEFORE the environment is created, or creation
# fails with Error 400 failedPrecondition ("missing required permissions:
# iam.serviceAccounts.getIamPolicy, iam.serviceAccounts.setIamPolicy"). The SA is
# Google-managed and named service-<project_number>@cloudcomposer-accounts.iam — we
# resolve the project number from the project id and grant the role here, so a plain
# `terraform apply` is self-sufficient instead of needing a manual gcloud step first.
# https://cloud.google.com/composer/docs/composer-2/run-apache-airflow#service-agent-ext-role
data "google_project" "this" {
  count      = var.enabled ? 1 : 0
  project_id = var.project_id
}

resource "google_project_iam_member" "composer_service_agent_ext" {
  count   = var.enabled ? 1 : 0
  project = var.project_id
  role    = "roles/composer.ServiceAgentV2Ext"
  member  = "serviceAccount:service-${data.google_project.this[0].number}@cloudcomposer-accounts.iam.gserviceaccount.com"
}

# Composer 2 runs on GKE Autopilot, so container.googleapis.com (GKE) must be on
# before the environment is created, or creation fails with Error 400
# failedPrecondition "Please enable all APIs Cloud Composer depends on".
resource "google_project_service" "gke" {
  count   = var.enabled ? 1 : 0
  project = var.project_id
  service = "container.googleapis.com"
  # Don't disable on destroy of a prototype — other things may use GKE.
  disable_on_destroy = false
}

resource "google_composer_environment" "this" {
  count      = var.enabled ? 1 : 0
  depends_on = [google_project_iam_member.composer_service_agent_ext, google_project_service.gke]

  name    = "mig-composer"
  project = var.project_id
  region  = var.region

  config {
    software_config {
      image_version = var.airflow_version

      # The DAG reads its endpoints from the environment, exactly as it does locally —
      # which is what lets one DAG file serve both worlds.
      env_variables = {
        MIG_EXECUTION_MODE = "dataflow"
        # GCP_PROJECT is intentionally omitted: Cloud Composer reserves it and sets it
        # to the environment's own project (mig-000001-1-dev) — which is exactly what
        # the DAG reads via os.environ.get("GCP_PROJECT", "mig-local").
        GCP_REGION                = var.region
        BQ_DATASET_EXTRACTION     = var.datasets["bq_extraction"]
        BQ_DATASET_TRANSFORMATION = var.datasets["bq_transformation"]
        BQ_DATASET_RECON          = var.datasets["bq_recon"]
        GCS_LANDING_BUCKET        = var.buckets["landing"]
        GCS_JSON_BUCKET           = var.buckets["json-out"]
        GCS_RECON_BUCKET          = var.buckets["recon"]

        # These must be declared here, not set afterwards with `gcloud composer
        # environments update`: Terraform owns softwareConfig.envVariables, so the next
        # apply reverts anything added out of band. That is not hypothetical — the target system
        # URL was set by hand, silently reverted by an unrelated apply, and the loader
        # then failed with "URI with undefined scheme" three DAG runs later.
        MIG_TARGET_SYSTEM_URL = var.target_system_url
        MIG_JAVA_IMAGE_TAG    = var.java_image_tag
        KAFKA_BOOTSTRAP       = var.kafka_bootstrap
        # SASL_SSL whenever a Managed Kafka cluster exists — it has no PLAINTEXT
        # listener. The DAG forwards this into the Beam and Java pods; without it the
        # recon consumer and the json_producer sink both speak PLAINTEXT to a broker
        # that only accepts SASL_SSL/OAUTHBEARER.
        KAFKA_SECURITY_PROTOCOL = var.kafka_bootstrap != "" ? "SASL_SSL" : "PLAINTEXT"
        KAFKA_TOPIC             = var.kafka_topic
        # The confirmation stream (docs/PLAN-CHANGES-22082026.md). The DAG reads these
        # and passes them as --confirmation-bootstrap / --confirmation-topic to the
        # recon pod. The mock reads its own copy from the target_system_mock module.
        TARGET_SYSTEM_CONFIRMATION_BOOTSTRAP = var.target_system_confirmation_bootstrap
        TARGET_SYSTEM_CONFIRMATION_TOPIC     = var.target_system_confirmation_topic
        # The rejection stream and the settle budget, both read by the DAG and passed to
        # the loader pod as --rejection-topic / --settle-timeout-seconds.
        TARGET_SYSTEM_REJECTION_TOPIC = var.target_system_rejection_topic
        LOADER_SETTLE_TIMEOUT_SECONDS = tostring(var.loader_settle_timeout_seconds)
        DATAFLOW_SUBNETWORK           = var.dataflow_subnetwork
        DATAFLOW_SERVICE_ACCOUNT      = var.dataflow_service_account
      }

      airflow_config_overrides = {
        # Secrets come from Secret Manager, never from Airflow Variables.
        secrets-backend = "airflow.providers.google.cloud.secrets.secret_manager.CloudSecretManagerBackend"
      }
    }

    node_config {
      service_account = var.service_account
      network         = var.network_id
      subnetwork      = var.subnet_id
    }

    # Smallest supported footprint — this orchestrates, it does not compute.
    workloads_config {
      scheduler {
        cpu        = 0.5
        memory_gb  = 2
        storage_gb = 1
        count      = 1
      }
      web_server {
        cpu        = 0.5
        memory_gb  = 2
        storage_gb = 1
      }
      worker {
        cpu        = 0.5
        memory_gb  = 2
        storage_gb = 1
        min_count  = 1
        max_count  = 3
      }
    }

    environment_size = "ENVIRONMENT_SIZE_SMALL"

    private_environment_config {
      enable_private_endpoint = false
    }
  }
}

output "gke_cluster_name" {
  description = "Bare GKE cluster name — the last path segment of the fully-qualified cluster, so the kubernetes data source can use it without splitting."
  value       = var.enabled ? element(split("/", google_composer_environment.this[0].config[0].gke_cluster), length(split("/", google_composer_environment.this[0].config[0].gke_cluster)) - 1) : ""
}

output "dag_bucket" {
  description = "Sync composer/dags/ here — see `make deploy-dags`."
  value       = var.enabled ? google_composer_environment.this[0].config[0].dag_gcs_prefix : ""
}

output "airflow_uri" {
  value = var.enabled ? google_composer_environment.this[0].config[0].airflow_uri : ""
}
