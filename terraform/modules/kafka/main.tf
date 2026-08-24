# Managed Service for Apache Kafka — the Loader team's sink.
#
# Locally this is redpanda on PLAINTEXT. Here it is a managed cluster reached over
# SASL_SSL/OAUTHBEARER using the worker service account, which is the one part of the
# Kafka switch that is a genuine client-config change rather than a URL swap.

variable "project_id" { type = string }
variable "region" { type = string }
variable "enabled" {
  type        = bool
  default     = false
  description = "Managed Kafka is billed per vCPU-hour; off unless the Kafka sink is being exercised."
}
variable "subnet_id" { type = string }

# Two topics on one cluster: the loader's target stream and the confirmation stream
# the mock writes back so recon can prove every TARGET row was actually persisted (see
# docs/PLAN-CHANGES-22082026.md). The map also drives the outputs below — a for_each over
# the same map keeps "what Terraform built" and "what the apps read" from drifting.
variable "topics" {
  type = map(object({
    topic_id   = string
    partitions = number
  }))
  default = {
    target = {
      topic_id   = "target-system-target"
      partitions = 12
    }
    confirmations = {
      # The confirmation stream is small and short-lived — one event per accepted TARGET
      # row, consumed once per run by a fresh consumer group — so a single partition is
      # both sufficient and what the bounded endOffsets read in recon-service assumes.
      topic_id   = "target-system-confirmations"
      partitions = 1
    }
  }
  description = "Topics on the cluster. Keys are local labels (target, confirmations) used to address outputs; topic_id is the real Kafka topic name."
}

resource "google_managed_kafka_cluster" "this" {
  count    = var.enabled ? 1 : 0
  provider = google-beta

  project    = var.project_id
  location   = var.region
  cluster_id = "mig-kafka"

  capacity_config {
    vcpu_count   = 3
    memory_bytes = 3221225472
  }

  gcp_config {
    access_config {
      network_configs {
        subnet = var.subnet_id
      }
    }
  }
}

resource "google_managed_kafka_topic" "this" {
  for_each = var.enabled ? var.topics : {}
  provider = google-beta

  project  = var.project_id
  location = var.region
  cluster  = google_managed_kafka_cluster.this[0].cluster_id
  topic_id = each.value.topic_id

  partition_count    = each.value.partitions
  replication_factor = 3

  configs = {
    # Long enough that a failed migration wave can be replayed from the topic.
    "retention.ms"      = "604800000" # 7 days
    "cleanup.policy"    = "delete"
    "max.message.bytes" = "1048576"
  }
}

output "bootstrap_servers" {
  value = var.enabled ? "bootstrap.${google_managed_kafka_cluster.this[0].cluster_id}.${var.region}.managedkafka.${var.project_id}.cloud.goog:9092" : ""
}
# The cluster id ("mig-kafka") is what the IAM grant resources key on. Empty when Kafka is
# off, so module.iam can pass it through unconditionally and only create the
# roles/managedkafka.client grants when a cluster actually exists.
output "cluster_id" {
  value = var.enabled ? google_managed_kafka_cluster.this[0].cluster_id : ""
}
# A map keyed by the local label, so callers read `module.kafka.topics["target"]` /
# `module.kafka.topics["confirmations"]` rather than hardcoding a topic name. The empty
# default keeps `module.kafka.topics["..."]` from blowing up when Kafka is disabled — it
# returns "" and a no-Kafka run behaves exactly as before.
output "topics" {
  value = { for k, v in var.topics : k => var.enabled ? google_managed_kafka_topic.this[k].topic_id : "" }
}
# Kept for callers that only want the target topic — `terraform output -raw kafka_topic`
# style reads. The confirmation topic has no such shorthand yet because nothing reads it
# outside the mock/recon pair, which take the value from env vars.
output "topic" { value = var.enabled ? google_managed_kafka_topic.this["target"].topic_id : "" }
