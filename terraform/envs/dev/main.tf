# MIG 000001-1 — dev environment.
#
# Apply order matters; see docs/runbook-gcp.md. Terraform cannot create its own state
# bucket, so `module.bootstrap` is applied first with local state, then state is migrated.
#
# The two expensive components — Cloud Composer (~$300-400/month idle) and Managed Kafka
# (billed per vCPU-hour) — default to OFF. Everything else in this environment costs a few
# euros a month, so the storage/BigQuery/Dataform half of the stack can be stood up and
# exercised cheaply and the expensive parts turned on deliberately.

terraform {
  required_version = ">= 1.9"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 6.0"
    }
    google-beta = {
      source  = "hashicorp/google-beta"
      version = "~> 6.0"
    }
    # Only used by module.composer_rbac, to grant Airflow workers pod permissions.
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = "~> 2.30"
    }
  }

  # Enabled only after `terraform apply -target=module.bootstrap` has created it.
  backend "gcs" {
    bucket = "mig-000001-1-dev-tfstate"
    prefix = "envs/dev"
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

provider "google-beta" {
  project = var.project_id
  region  = var.region
}

# Credentials for the Composer GKE cluster, so module.composer_rbac can bind a Role.
# Wrapped in try(): when enable_composer is false the composer module produces no
# cluster, and a provider block cannot be made conditional. Empty values are harmless
# because the module's resources are gated on the same flag and create nothing.
data "google_client_config" "this" {}

data "google_container_cluster" "composer" {
  count = var.enable_composer && var.composer_pod_namespace != "" ? 1 : 0

  name     = module.composer.gke_cluster_name
  location = var.region
  project  = var.project_id
}

provider "kubernetes" {
  host = try(
    "https://${data.google_container_cluster.composer[0].endpoint}",
    "https://localhost"
  )
  token = data.google_client_config.this.access_token
  cluster_ca_certificate = try(
    base64decode(data.google_container_cluster.composer[0].master_auth[0].cluster_ca_certificate),
    ""
  )
}

# ── variables ─────────────────────────────────────────────────────────────────

variable "project_id" {
  type    = string
  default = "mig-000001-1-dev"
}

variable "region" {
  type    = string
  default = "europe-west1"
}

variable "billing_account" {
  type        = string
  description = "An OPEN billing account id. Both accounts on the authoring machine were closed — see the runbook."
}

variable "org_id" {
  type    = string
  default = ""
}

variable "create_project" {
  type    = bool
  default = false

  description = <<-EOT
    Whether Terraform should create the GCP project itself.

    Defaults to false: the common case is an existing project (created by hand, or by an
    org's provisioning process) that this stack is deployed *into*. Creating a project
    requires billing.resourceAssociations.create on the billing account, which an
    ordinary deployer service account does not have — so a default of true made every
    apply fail on a permission error for a project that already existed.

    Set to true only when bootstrapping a genuinely new project with credentials that
    carry billing-account permissions.
  EOT
}

variable "enable_composer" {
  type        = bool
  default     = false
  description = "Cloud Composer 2 costs ~$300-400/month even idle. Opt in explicitly."
}

variable "java_image_tag" {
  type        = string
  default     = "latest"
  description = <<-EOT
    The tag local/scripts/gcp/build_java_images.sh printed — a git SHA, with -dirty
    appended when the working tree is not clean. Left at "latest" the DAG launches
    whatever was pushed last, which is rarely what you just built.
  EOT
}

variable "enable_target_system_mock" {
  type        = bool
  default     = true
  description = "Target System stand-in on Cloud Run. Scales to zero, so idle cost is nil."
}

variable "enable_kafka" {
  type        = bool
  default     = false
  description = "Managed Service for Apache Kafka, billed per vCPU-hour."
}

variable "dataform_git_remote" {
  type    = string
  default = ""
}

variable "composer_pod_namespace" {
  type        = string
  default     = ""
  description = <<-EOT
    Composer's own Kubernetes namespace, e.g. composer-2-9-7-airflow-2-9-3-ab8412f5.

    Required only when enable_composer is true and you want the KubernetesPodOperator
    tasks to work. Composer does not expose this as an attribute and it changes whenever
    the environment is recreated, so read it after the environment is up:

      gcloud container clusters get-credentials <cluster> --region <region>
      kubectl get namespaces | grep composer

    Empty leaves the RBAC unmanaged, which is the correct default when Composer is off.
  EOT
}

variable "dataform_git_token_secret_version" {
  type        = string
  default     = ""
  description = <<-EOT
    Secret Manager version holding the Dataform git token, e.g.
    projects/<num>/secrets/dataform-git-token/versions/latest.

    Empty (the default) keeps the unlinked path: models are compiled by the Dataform CLI
    in deploy_dataform.sh, which is the only path exercised so far. Set it — together
    with dataform_git_remote — to activate the linked-repository path instead. Leaving
    both unset is a supported configuration, not an omission.
  EOT
}

# ── modules ───────────────────────────────────────────────────────────────────

module "bootstrap" {
  source = "../../modules/bootstrap"

  project_id      = var.project_id
  billing_account = var.billing_account
  org_id          = var.org_id
  region          = var.region
  create_project  = var.create_project
}

module "network" {
  source = "../../modules/network"

  project_id = var.project_id
  region     = var.region

  depends_on = [module.bootstrap]
}

module "storage" {
  source = "../../modules/storage"

  project_id = var.project_id
  region     = var.region
  # dev only — a production landing bucket is migration evidence and must not be
  # destroyable by a stray `terraform destroy`.
  force_destroy = true

  depends_on = [module.bootstrap]
}

module "bigquery" {
  source = "../../modules/bigquery"

  project_id          = var.project_id
  region              = var.region
  deletion_protection = false # dev only

  depends_on = [module.bootstrap]
}

module "iam" {
  source = "../../modules/iam"

  project_id = var.project_id
  buckets    = module.storage.buckets
  # Empty when Kafka is off → no roles/managedkafka.client grants are created. Non-empty
  # only when enable_kafka=true, and then it grants the client role to the four broker
  # principals (see terraform/modules/iam).
  kafka_cluster_id = module.kafka.cluster_id
}

module "secrets" {
  source = "../../modules/secrets"

  project_id = var.project_id
  region     = var.region

  accessors = {
    pgp-private-key           = module.iam.service_accounts["dataflow-worker"]
    pgp-passphrase            = module.iam.service_accounts["dataflow-worker"]
    target-system-credentials = module.iam.service_accounts["loader-app"]
    dataform-git-token        = module.iam.service_accounts["dataform-runner"]
  }

  depends_on = [module.bootstrap]
}

module "dataflow" {
  source = "../../modules/dataflow"

  project_id = var.project_id
  region     = var.region

  depends_on = [module.bootstrap]
}

module "dataform" {
  source = "../../modules/dataform"

  project_id      = var.project_id
  region          = var.region
  service_account = module.iam.service_accounts["dataform-runner"]
  git_remote_url  = var.dataform_git_remote

  # H7: the module declares git_token_secret_version and gates its linked-repo path on
  # it, but nothing passed it — so seed_secrets.sh wrote dataform-git-token into a secret
  # the module never read, and the linked-repo path could never activate. Passing it
  # closes that loop; empty (the default) keeps the unlinked CLI path, which is what
  # deploy_dataform.sh uses and the only path exercised so far.
  git_token_secret_version = var.dataform_git_token_secret_version
}

# A linked Dataform repository without a token authenticates as nobody: every compile
# fails at run time with "the git reference could not be resolved", which reads like a
# branch problem and is not one. The two inputs are only meaningful together, so the pair
# is checked at plan time rather than discovered from a failed Composer task.
check "dataform_git_credentials_are_complete" {
  assert {
    condition = var.dataform_git_remote == "" || var.dataform_git_token_secret_version != ""
    error_message = join("", [
      "dataform_git_remote is set but dataform_git_token_secret_version is empty: the ",
      "repository would be linked with no credential. Set the secret version (see ",
      "`make seed-secrets`), or leave both empty to keep the unlinked CLI path."
    ])
  }
}

module "target_system_mock" {
  source = "../../modules/target_system_mock"

  project_id = var.project_id
  region     = var.region
  enabled    = var.enable_target_system_mock
  image      = "${var.region}-docker.pkg.dev/${var.project_id}/mig-dataflow/target-system-mock:latest"

  # Empty when Kafka is off (the default) → the mock publishes no confirmations and the
  # run behaves as before. Non-empty only when enable_kafka=true, and then only reachable
  # from Cloud Run via a Serverless VPC Access connector (module.vpc_connector below).
  confirmation_bootstrap = try(module.kafka.bootstrap_servers, "")
  # The mock's second intake and the rejection half of its verdict. Both come from the
  # kafka module's topics map rather than being hardcoded, so "what Terraform built" and
  # "what the mock reads" cannot drift. Empty when Kafka is off → no consumer thread and
  # no rejection producer, which is the unchanged HTTP-only behaviour.
  target_topic    = try(module.kafka.topics["target"], "")
  rejection_topic = try(module.kafka.topics["rejections"], "")
  # The mock runs as its own SA so it can hold roles/managedkafka.client, and egresses
  # through the connector to reach the VPC-internal broker. Both empty when Kafka is off.
  service_account  = module.iam.service_accounts["target-system-mock"]
  vpc_connector_id = module.vpc_connector.connector_id

  depends_on = [module.bootstrap]
}

module "kafka" {
  source = "../../modules/kafka"

  project_id = var.project_id
  region     = var.region
  enabled    = var.enable_kafka
  subnet_id  = module.network.subnet_id
}

# Serverless VPC Access connector so the Cloud Run mock can reach the VPC-internal
# Managed Kafka broker. Gated on the same flag as Kafka: only exists when the broker does,
# so a no-Kafka run pays nothing and a disabled apply produces no connector.
module "vpc_connector" {
  source = "../../modules/vpc_connector"

  project_id = var.project_id
  region     = var.region
  network_id = module.network.network_id
  enabled    = var.enable_kafka

  depends_on = [module.bootstrap]
}

module "composer" {
  source = "../../modules/composer"

  project_id      = var.project_id
  region          = var.region
  enabled         = var.enable_composer
  service_account = module.iam.service_accounts["composer-runner"]
  network_id      = module.network.network_id
  subnet_id       = module.network.subnet_id
  datasets        = module.bigquery.datasets
  buckets         = module.storage.buckets

  target_system_url = module.target_system_mock.url
  java_image_tag    = var.java_image_tag
  kafka_bootstrap   = try(module.kafka.bootstrap_servers, "")
  kafka_topic       = try(module.kafka.topics["target"], "target-system-target")
  # Recon reads the confirmation topic; the bootstrap is empty when Kafka is off, which
  # makes recon skip the confirmation read (docs/PLAN-CHANGES-22082026.md).
  target_system_confirmation_bootstrap = try(module.kafka.bootstrap_servers, "")
  # The loader settles against both return topics. Sourced from the kafka module's map so
  # the topic Terraform creates and the topic the loader reads cannot drift apart.
  target_system_confirmation_topic = try(module.kafka.topics["confirmations"], "target-system-confirmations")
  target_system_rejection_topic    = try(module.kafka.topics["rejections"], "target-system-rejections")
  dataflow_subnetwork              = "regions/${var.region}/subnetworks/mig-subnet"
  dataflow_service_account         = module.iam.service_accounts["dataflow-worker"]
}

# Composer does not grant its Airflow workers permission to create pods, so every
# KubernetesPodOperator task — the three Beam pipelines and the two Java apps — fails
# with a 403 until this Role/RoleBinding exists. Requires composer_pod_namespace.
module "composer_rbac" {
  source = "../../modules/composer_rbac"

  enabled   = var.enable_composer && var.composer_pod_namespace != ""
  namespace = var.composer_pod_namespace

  # Pods impersonate dataflow-worker via Workload Identity, reusing the BigQuery, GCS and
  # Secret Manager grants module.iam already gives it. Without this the pod runs as the
  # node's default SA and is denied on the first BigQuery call.
  project_id             = var.project_id
  google_service_account = module.iam.service_accounts["dataflow-worker"]

  depends_on = [module.composer]

  app_service_accounts = {
    loader = module.iam.service_accounts["loader-app"]
    recon  = module.iam.service_accounts["recon-service"]
  }
}

# ── outputs ───────────────────────────────────────────────────────────────────

output "buckets" { value = module.storage.buckets }

# A scalar alongside the `buckets` map, so shell callers can read it with `output -raw`.
# `-raw` refuses complex types, and the alternative was piping `output -json buckets`
# through a JSON parser — which made python3 a prerequisite of the GCP-only path for one
# dictionary lookup. See local/scripts/gcp/build_templates.sh.
output "templates_bucket" { value = module.storage.templates_bucket }
output "datasets" { value = module.bigquery.datasets }
output "service_accounts" { value = module.iam.service_accounts }
output "dataflow_registry" { value = module.dataflow.registry }
output "dataform_repository" { value = module.dataform.repository_id }
output "composer_dag_bucket" { value = module.composer.dag_bucket }
output "composer_airflow_uri" { value = module.composer.airflow_uri }
output "kafka_bootstrap" { value = module.kafka.bootstrap_servers }

output "env_file" {
  description = "Paste into .env to point the pipelines at this environment."
  value       = <<-EOT
    TARGET_PROFILE=real
    GCP_PROJECT=${var.project_id}
    GCS_LANDING_BUCKET=${module.storage.landing_bucket}
    GCS_JSON_BUCKET=${module.storage.json_bucket}
    GCS_RECON_BUCKET=${module.storage.recon_bucket}
    BQ_TARGET=real
    KAFKA_SECURITY_PROTOCOL=SASL_SSL
    # KAFKA_BOOTSTRAP, not KAFKA_BOOTSTRAP_SERVERS: config.py reads the former (as does
    # .env.example). Under the old name the address was emitted but never read, so the
    # pipeline silently fell back to localhost:19092 — which inside a Dataflow worker is
    # nothing at all. H8 was recorded as passing because `terraform output` produced a
    # real address; that only ever proved the value existed, not that it reached anyone.
    KAFKA_BOOTSTRAP=${try(module.kafka.bootstrap_servers, "")}
    # runner.py defaults these to conventions ("mig-subnet", "dataflow-worker@<project>")
    # when unset. The convention is right on this environment and wrong on any other, and
    # the failure mode is the expensive one: a Dataflow job that starts against a
    # nonexistent subnet and then never progresses. Emitting the real values from state
    # means a run uses what Terraform actually built.
    DATAFLOW_SUBNETWORK=${module.network.subnet_self_link}
    DATAFLOW_SERVICE_ACCOUNT=${module.iam.service_accounts["dataflow-worker"]}
    # Without this the loader falls back to http://localhost:8080, which inside a
    # Dataflow worker or the toolbox container is nothing at all — every document then
    # fails "transport failure" after exhausting its retries, which reads like a data
    # problem and is an infrastructure one.
    TARGET_SYSTEM_URL=${module.target_system_mock.url}
    # Confirmation stream (docs/PLAN-CHANGES-22082026.md). Empty when Kafka is off → the
    # mock and recon both skip the confirmation path and the run is green. Non-empty only
    # with enable_kafka=true, and the confirmation topic is the second topic on the
    # cluster (terraform/modules/kafka's topics map).
    TARGET_SYSTEM_CONFIRMATION_BOOTSTRAP=${try(module.kafka.bootstrap_servers, "")}
    TARGET_SYSTEM_CONFIRMATION_TOPIC=target-system-confirmations
  EOT
}

output "target_system_url" { value = module.target_system_mock.url }
