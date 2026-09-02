# Service accounts, one per component, least privilege.
#
# This keeps the contracts we do not own behind adapters, and does the same for authority.
# The Dataflow workers can read the landing bucket and write BigQuery, but they cannot read
# the reconciliation reports; the reconciliation service can read everything and write
# nothing but its own reports. That separation is what makes an audit tractable.

variable "project_id" { type = string }
variable "buckets" { type = map(string) }

variable "kafka_cluster_id" {
  type        = string
  default     = ""
  description = <<-EOT
    The Managed Kafka cluster id ("mig-kafka"), or empty when Kafka is off. Drives the
    roles/managedkafka.client grants below: empty (the default) creates no Kafka IAM, so a
    no-Kafka apply is unchanged. Non-empty grants the client role to every account that
    needs a broker socket — json_producer's dataflow-worker, the loader-app (which now
    produces the target topic and reads back the return streams), recon-service, and the
    target-system-mock (docs/PLAN-CHANGES-22082026.md,
    docs/PLAN-CHANGES-02092026-kafka-loader.md).
  EOT
}

locals {
  accounts = {
    dataflow-worker    = "Dataflow workers running the three pipelines"
    composer-runner    = "Cloud Composer environment service account"
    dataform-runner    = "Dataform service account for the SQL transformation"
    loader-app         = "Loader App pushing to Target System"
    recon-service      = "Reconciliation service (read-mostly)"
    target-system-mock = "Target System mock on Cloud Run (publishes confirmations)"
  }

  # role => list of accounts that need it
  project_roles = merge({
    # dataflow.worker lets a VM *execute* a job; it does not let anyone *create* one.
    # The pipeline pods submit their own Dataflow jobs, so the account they impersonate
    # needs dataflow.developer too — without it the submit fails with
    #   403 Could not create workflow; user does not have write access to project
    "roles/dataflow.worker"     = ["dataflow-worker"]
    "roles/dataflow.developer"  = ["dataflow-worker", "composer-runner"]
    "roles/bigquery.jobUser"    = ["dataflow-worker", "dataform-runner", "recon-service", "composer-runner"]
    "roles/bigquery.dataEditor" = ["dataflow-worker", "dataform-runner"]
    # composer-runner reads too, not only submits: `assert_run_balanced` is a
    # PythonOperator running inside the Airflow worker rather than a pod, so it queries
    # run_ledger as this account. bigquery.jobUser lets it *start* a job; without
    # dataViewer the job fails with "User does not have permission to query table
    # bq_recon.run_ledger", and the DAG's correctness gate is the one task that cannot be
    # allowed to fail for an infrastructure reason.
    "roles/bigquery.dataViewer"          = ["recon-service", "composer-runner"]
    "roles/composer.worker"              = ["composer-runner"]
    "roles/dataform.editor"              = ["composer-runner"]
    "roles/secretmanager.secretAccessor" = ["dataflow-worker", "loader-app"]
    "roles/artifactregistry.reader"      = ["dataflow-worker", "composer-runner"]
    }, var.kafka_cluster_id == "" ? {} : {
    # Managed Kafka has no cluster-level IAM resource (the provider exposes only cluster,
    # topic and acl). The client role is project-level: it grants the SA permission to open
    # a socket to the broker, which is all the SASL_SSL/OAUTHBEARER handshake needs. The
    # four broker principals are the dataflow-worker (json_producer on the DAG),
    # loader-app (produces target-system-target and reads back the confirmation and
    # rejection streams to settle the run), recon-service (consumes confirmations on the
    # DAG) and target-system-mock (consumes the target topic, publishes confirmations and
    # rejections from Cloud Run). Gated on kafka_cluster_id so a no-Kafka apply creates
    # no grants — the map is empty and google_project_iam_member.this binds nothing for it.
    "roles/managedkafka.client" = ["dataflow-worker", "loader-app", "recon-service", "target-system-mock"]
  })

  # bucket => { account => role }
  bucket_roles = {
    landing = {
      dataflow-worker = "roles/storage.objectViewer"
    }
    json-out = {
      dataflow-worker = "roles/storage.objectAdmin"
      loader-app      = "roles/storage.objectViewer"
    }
    recon = {
      # objectAdmin, not objectCreator: the File Processor republishes the extractor's
      # .RPT/.CHS/.ERR here, and the storage client reads object metadata before writing.
      # objectCreator permits the create but denies that read, so the republish failed
      # with 403 inside ReadBundle on a Dataflow worker.
      dataflow-worker = "roles/storage.objectAdmin"
      # objectAdmin for the loader too, and for the same class of reason. It writes its
      # own .RPT/.CHS/.ERR here, and a re-run of the same run id *overwrites* them —
      # which GCS implements as create + delete, so objectCreator alone fails with
      # "does not have storage.objects.delete access". That never showed up while every
      # pod ran as dataflow-worker; it appeared the moment the loader started using the
      # narrow identity actually written for it, which is the point of having one.
      loader-app    = "roles/storage.objectAdmin"
      recon-service = "roles/storage.objectAdmin"
    }
    dataflow-temp = {
      dataflow-worker = "roles/storage.objectAdmin"
    }
  }

  bucket_bindings = merge([
    for bucket, grants in local.bucket_roles : {
      for account, role in grants :
      "${bucket}/${account}" => { bucket = bucket, account = account, role = role }
    }
  ]...)

  role_bindings = merge([
    for role, accounts in local.project_roles : {
      for account in accounts :
      "${replace(role, "/", "_")}/${account}" => { role = role, account = account }
    }
  ]...)
}

resource "google_service_account" "this" {
  for_each = local.accounts

  account_id   = each.key
  project      = var.project_id
  display_name = each.value
}

resource "google_project_iam_member" "this" {
  for_each = local.role_bindings

  project = var.project_id
  role    = each.value.role
  member  = "serviceAccount:${google_service_account.this[each.value.account].email}"
}

resource "google_storage_bucket_iam_member" "this" {
  for_each = local.bucket_bindings

  bucket = var.buckets[each.value.bucket]
  role   = each.value.role
  member = "serviceAccount:${google_service_account.this[each.value.account].email}"
}

# ── impersonation: composer-runner must act as dataflow-worker ────────────────
#
# Project-level roles are not enough to *launch* a Dataflow job. The DAG runs as
# composer-runner and submits a Flex Template that must execute as dataflow-worker, so
# composer-runner needs token-creator + user ON THAT SERVICE ACCOUNT — a resource-level
# grant, which no amount of project_roles can express. Without it every Dataflow task
# fails at submission with "does not have permission to act as service account"
# (review finding H6, the reason the deployed DAG was never triggered).
# A Dataflow job submitted BY dataflow-worker also RUNS AS dataflow-worker, and GCP
# requires the submitter to hold actAs on the account the job will use — even when they
# are the same account. Without this the submit fails with
#   403 Could not create workflow; user does not have write access to project
resource "google_service_account_iam_member" "dataflow_worker_acts_as_itself" {
  service_account_id = google_service_account.this["dataflow-worker"].name
  role               = "roles/iam.serviceAccountUser"
  member             = "serviceAccount:${google_service_account.this["dataflow-worker"].email}"
}

resource "google_service_account_iam_member" "composer_runner_impersonates_dataflow_worker" {
  for_each = toset([
    "roles/iam.serviceAccountTokenCreator",
    "roles/iam.serviceAccountUser",
  ])

  service_account_id = google_service_account.this["dataflow-worker"].name
  role               = each.value
  member             = "serviceAccount:${google_service_account.this["composer-runner"].email}"
}

# ── Managed Kafka: roles/managedkafka.client per broker principal ───────────────
#
# Every client that opens a socket to Managed Kafka needs roles/managedkafka.client —
# without it the SASL_SSL/OAUTHBEARER handshake authenticates the SA but the broker
# refuses the connection as unauthorized. The four principals:
#   * dataflow-worker — json_producer writes target-system-target on the DAG path
#   * loader-app       — produces target-system-target, then reads target-system-confirmations
#                        and target-system-rejections to settle the run into its .RPT/.ERR
#   * recon-service    — consumes target-system-confirmations on the DAG path
#   * target-system-mock — consumes target-system-target, publishes confirmations and rejections
# Managed Kafka has no cluster-level IAM resource (the provider exposes only cluster,
# topic and acl), so the grant is project-level via the shared google_project_iam_member.this
# above, conditionally added to local.project_roles when kafka_cluster_id is non-empty
# (i.e. enable_kafka=true). A no-Kafka apply adds nothing to the map and binds nothing.

output "service_accounts" {
  value = { for k, sa in google_service_account.this : k => sa.email }
}
