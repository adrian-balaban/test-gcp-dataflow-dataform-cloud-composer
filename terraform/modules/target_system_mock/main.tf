# Target System stand-in on Cloud Run.
#
# Target System belongs to another team and does not exist for this prototype, so the Load
# lane needs something to post to. Locally that is the podman-compose `target-system-mock`;
# on GCP there was *nothing*, and the loader fell back to its localhost default, retried
# 200 times and reported 40 documents as permanently failed — a Load lane that could
# never have worked, discovered only by reading .ERR. This module gives the lane a real
# target so "full E → T+R → L on real GCP" is a statement that can be tested.
#
# Cloud Run rather than GKE: it scales to zero, so an idle environment bills nothing,
# which matters because this environment is torn down and rebuilt between demos.

variable "project_id" { type = string }
variable "region" { type = string }
variable "image" {
  type        = string
  description = "Fully qualified image, e.g. europe-west1-docker.pkg.dev/<p>/mig-dataflow/target-system-mock:latest"
}
variable "enabled" {
  type        = bool
  default     = true
  description = "Scale-to-zero, so leaving it on costs nothing while idle."
}
variable "failure_rate" {
  type        = string
  default     = "0.15"
  description = "The mock injects 429/503 at this rate on purpose, so retries are exercised."
}
variable "confirmation_bootstrap" {
  type        = string
  default     = ""
  description = <<-EOT
    Kafka bootstrap the mock publishes confirmation events to
    (docs/PLAN-CHANGES-22082026.md). Empty (the default, and the value when Managed Kafka
    is off) means the mock creates no producer and publishes nothing — the run behaves
    exactly as before. When non-empty the mock needs network reachability to the broker;
    on GCP Managed Kafka is VPC-only, so a Cloud Run mock additionally needs a Serverless
    VPC Access connector (not provisioned by this module — see docs/runbook-gcp.md).
  EOT
}
variable "confirmation_topic" {
  type        = string
  default     = "target-system-confirmations"
  description = "Topic the mock writes one confirmation event per accepted write to."
}
variable "rejection_topic" {
  type        = string
  default     = "target-system-rejections"
  description = <<-EOT
    Topic the mock writes one rejection event per refused document to, carrying its own
    reason string (docs/PLAN-CHANGES-02092026-kafka-loader.md). This is what gives the
    Loader's .ERR a source once the Load edge is Kafka: without it a bad document is
    indistinguishable from a slow one and lands in `unsettled`.
  EOT
}
variable "target_topic" {
  type        = string
  default     = ""
  description = <<-EOT
    The Loader's target topic, which the mock consumes as a second intake alongside the
    POST endpoint. Empty (the default, and the value when Kafka is off) means no consumer
    thread is started and the mock is HTTP-only, exactly as before. Non-empty only makes
    sense together with confirmation_bootstrap, since the verdict for a consumed document
    is published rather than returned.
  EOT
}

variable "vpc_connector_id" {
  type        = string
  default     = ""
  description = <<-EOT
    The Serverless VPC Access connector the mock egresses through to reach the
    VPC-internal Managed Kafka broker. Empty (the default, and when Kafka is off) means
    no VPC wiring — the mock has no broker to reach. Required only when
    confirmation_bootstrap is non-empty (see docs/runbook-gcp.md).
  EOT
}

variable "service_account" {
  type        = string
  default     = ""
  description = <<-EOT
    The dedicated target-system-mock service account, which holds
    roles/managedkafka.client so the mock's confirmation producer can authenticate to
    Managed Kafka over OAUTHBEARER. Empty falls back to the Cloud Run runtime default
    account, which has no Kafka grant and is correct only when Kafka is off.
  EOT
}

resource "google_cloud_run_v2_service" "this" {
  count = var.enabled ? 1 : 0

  name     = "target-system-mock"
  project  = var.project_id
  location = var.region

  # This is a disposable mock; protecting it from deletion would only make teardown
  # require a console visit.
  deletion_protection = false
  ingress             = "INGRESS_TRAFFIC_ALL"

  template {
    # Run as the dedicated target-system-mock SA when one is supplied, so the confirmation
    # producer has the roles/managedkafka.client grant it needs. Empty (Kafka off) leaves
    # the Cloud Run runtime default, which is fine — there is no broker to authenticate to.
    # Cloud Run v2 puts service_account inside template, not at the service top level.
    service_account = var.service_account

    scaling {
      # On the HTTP path scale-to-zero is free: the Loader's first POST is itself the
      # cold-start trigger, so the only cost is that one request waiting longer. On the
      # Kafka path there is no request to wake the instance — the consumer thread either
      # happens to be running already or the messages it should be applying just sit on
      # the topic. Measured on GCP: a cold mock left an entire 500-document run
      # `unsettled` past a 120s loader timeout, because nothing woke the consumer until
      # unrelated traffic (e.g. a stray /__admin/stats poll) happened to hit it minutes
      # later. So min_instance_count follows target_topic — pinned to 1 whenever the
      # consumer has something to listen for, back to 0 (its old, harmless default)
      # otherwise.
      min_instance_count = var.target_topic == "" ? 0 : 1
      max_instance_count = 2
    }

    # Egress through the Serverless VPC Access connector so the mock can open a socket to
    # the VPC-internal broker. Only wired when a connector is supplied; the dynamic block
    # renders nothing when vpc_connector_id is empty (Kafka off), so a no-Kafka run is
    # unchanged. Cloud Run v2 names this block vpc_access (not vpc_access_connector).
    dynamic "vpc_access" {
      for_each = var.vpc_connector_id == "" ? [] : [var.vpc_connector_id]
      content {
        connector = vpc_access.value
        # PRIVATE_RANGES_ONLY: only RFC1918 destinations (the broker) go through the
        # connector; everything else stays on the default path.
        egress = "PRIVATE_RANGES_ONLY"
      }
    }

    containers {
      image = var.image
      ports { container_port = 8080 }

      env {
        name  = "TARGET_SYSTEM_FAILURE_RATE"
        value = var.failure_rate
      }
      # Confirmation stream (docs/PLAN-CHANGES-22082026.md). Empty bootstrap = the mock
      # creates no producer; non-empty = the mock publishes on every 201. The mock reads
      # these directly (it is a Cloud Run service, not a Composer pod fed by the DAG).
      env {
        name  = "TARGET_SYSTEM_CONFIRMATION_BOOTSTRAP"
        value = var.confirmation_bootstrap
      }
      env {
        name  = "TARGET_SYSTEM_CONFIRMATION_TOPIC"
        value = var.confirmation_topic
      }
      # The other half of the verdict on the Kafka path, and the second intake that makes
      # a verdict necessary at all (docs/PLAN-CHANGES-02092026-kafka-loader.md). An empty
      # target topic leaves the consumer thread unstarted, so the mock stays HTTP-only.
      env {
        name  = "TARGET_SYSTEM_REJECTION_TOPIC"
        value = var.rejection_topic
      }
      env {
        name  = "TARGET_SYSTEM_TARGET_TOPIC"
        value = var.target_topic
      }
      # Tells the mock's Java producer to wire SASL_SSL/OAUTHBEARER instead of PLAINTEXT.
      # The mock reads it directly (Cloud Run service, not a DAG pod fed by env_file).
      # Left at SASL_SSL whenever Kafka is on; the producer falls back to PLAINTEXT when
      # the bootstrap is empty, so this only matters on the non-empty path.
      env {
        name  = "KAFKA_SECURITY_PROTOCOL"
        value = var.confirmation_bootstrap == "" ? "PLAINTEXT" : "SASL_SSL"
      }

      resources {
        limits = {
          cpu    = "1"
          memory = "512Mi"
        }
      }
    }
  }
}

# The loader authenticates to the *mock* with an idempotency key, not with a Google
# token — it is standing in for a third-party banking API, so making it require Google
# IAM would change the contract under test. The data it holds is synthetic.
resource "google_cloud_run_v2_service_iam_member" "public" {
  count = var.enabled ? 1 : 0

  project  = var.project_id
  location = var.region
  name     = google_cloud_run_v2_service.this[0].name
  role     = "roles/run.invoker"
  member   = "allUsers"
}

output "url" {
  value = var.enabled ? google_cloud_run_v2_service.this[0].uri : ""
}
