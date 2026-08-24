# GKE RBAC so Composer's Airflow workers may launch KubernetesPodOperator pods.
#
# Composer 2 runs Airflow on a GKE cluster, but it does NOT grant its workers permission
# to create arbitrary pods. Without the binding below, every KubernetesPodOperator task
# fails at submission with:
#
#   pods is forbidden: User "system:serviceaccount:composer-2-9-7-airflow-2-9-3-<id>:default"
#   cannot list resource "pods" in API group "" in the namespace "<same namespace>"
#
# That is not a namespace mistake — asking for "default" fails the same way. The Airflow
# worker genuinely has no pod RBAC anywhere, so it has to be granted explicitly.
#
# This matters for five of the nine DAG tasks: the three Beam pipelines and the two Java
# apps (loader, reconciliation) all run as pods.
#
# Kept in its own module rather than folded into `composer` because it needs the
# Kubernetes provider, which can only be configured *after* the cluster exists. Merging
# them would create a provider-configuration cycle on a clean apply.

terraform {
  required_providers {
    google = {
      source = "hashicorp/google"
    }
    kubernetes = {
      source = "hashicorp/kubernetes"
    }
  }
}

variable "enabled" {
  type    = bool
  default = false
}

variable "namespace" {
  type        = string
  description = <<-EOT
    The Composer environment's own Kubernetes namespace, e.g.
    composer-2-9-7-airflow-2-9-3-ab8412f5.

    Composer names it after the image version plus an environment-specific suffix, and
    does not expose it as an attribute — read it from the cluster:

      kubectl get namespaces | grep composer

    It changes when the environment is recreated, which is why it is a variable rather
    than something derived.
  EOT
}

variable "service_account" {
  type        = string
  default     = "default"
  description = "The Kubernetes SA the Airflow workers run as — 'default' in that namespace."
}

# Only the verbs KubernetesPodOperator actually uses: it creates a pod, polls it, streams
# its logs, and (with is_delete_operator_pod) removes it afterwards. Deliberately not
# cluster-wide and deliberately not "*" — a task that can create pods in one namespace is
# a much smaller blast radius than one that can act anywhere.
resource "kubernetes_role" "pod_launcher" {
  count = var.enabled ? 1 : 0

  metadata {
    name      = "airflow-pod-launcher"
    namespace = var.namespace
  }

  rule {
    api_groups = [""]
    resources  = ["pods"]
    verbs      = ["create", "get", "list", "watch", "delete", "patch"]
  }

  rule {
    api_groups = [""]
    resources  = ["pods/log", "pods/status"]
    verbs      = ["get", "list", "watch"]
  }

  # KubernetesPodOperator writes the task's return value into a sidecar and reads it back
  # via exec; without this, xcom_push tasks fail after the pod itself succeeds.
  rule {
    api_groups = [""]
    resources  = ["pods/exec"]
    verbs      = ["create", "get"]
  }
}

resource "kubernetes_role_binding" "pod_launcher" {
  count = var.enabled ? 1 : 0

  metadata {
    name      = "airflow-pod-launcher"
    namespace = var.namespace
  }

  role_ref {
    api_group = "rbac.authorization.k8s.io"
    kind      = "Role"
    name      = kubernetes_role.pod_launcher[0].metadata[0].name
  }

  subject {
    kind      = "ServiceAccount"
    name      = var.service_account
    namespace = var.namespace
  }
}

# ── Workload Identity: give the pods a GCP identity ───────────────────────────
#
# RBAC lets Airflow *create* the pod; it says nothing about what that pod may do in GCP.
# Without the binding below the pod runs as the GKE node's default service account and
# dies on the first API call:
#
#   403 Access Denied: Dataset <project>:bq_extraction:
#   Permission bigquery.datasets.get denied
#
# Workload Identity maps a Kubernetes SA onto a Google SA. Pointing it at dataflow-worker
# reuses the grants module.iam already gives that account (BigQuery read/write, the
# landing/json-out/recon buckets, Secret Manager) rather than inventing a second set that
# would drift.

variable "app_service_accounts" {
  type        = map(string)
  default     = {}
  description = <<-EOT
    App name → Google service account it should run as, e.g.
    { loader = "loader-app@<project>...", recon = "recon-service@<project>..." }.
    Each gets its own annotated Kubernetes SA so a pod runs under the identity whose
    roles were written for it, rather than borrowing dataflow-worker's.
  EOT
}

variable "google_service_account" {
  type        = string
  default     = ""
  description = "Google SA the pods impersonate — normally dataflow-worker@<project>."
}

variable "project_id" {
  type    = string
  default = ""
}

resource "kubernetes_service_account" "pipeline" {
  count = var.enabled && var.google_service_account != "" ? 1 : 0

  metadata {
    name      = "mig-pipeline"
    namespace = var.namespace
    annotations = {
      "iam.gke.io/gcp-service-account" = var.google_service_account
    }
  }
}

# The Java apps have their own Google service accounts with deliberately narrow roles —
# loader-app may write the JSON and recon buckets, recon-service may only *read* BigQuery
# — but until now no Kubernetes service account was annotated to them, so every pod ran as
# mig-pipeline and therefore as dataflow-worker. The tailored roles existed and nothing
# used them, which is least privilege on paper only. One KSA per identity fixes that; the
# DAG selects them per task.
resource "kubernetes_service_account" "app" {
  for_each = var.enabled ? var.app_service_accounts : {}

  metadata {
    name      = "mig-${each.key}"
    namespace = var.namespace
    annotations = {
      "iam.gke.io/gcp-service-account" = each.value
    }
  }
}

resource "google_service_account_iam_member" "app_workload_identity" {
  for_each = var.enabled ? var.app_service_accounts : {}

  service_account_id = "projects/${var.project_id}/serviceAccounts/${each.value}"
  role               = "roles/iam.workloadIdentityUser"
  member             = "serviceAccount:${var.project_id}.svc.id.goog[${var.namespace}/mig-${each.key}]"
}

output "app_service_accounts" {
  description = "task-name → Kubernetes SA, for the DAG's service_account_name."
  value       = { for k, v in kubernetes_service_account.app : k => v.metadata[0].name }
}

# The other half of the mapping: the Google SA must allow that specific KSA to act as it.
resource "google_service_account_iam_member" "workload_identity" {
  count = var.enabled && var.google_service_account != "" ? 1 : 0

  service_account_id = "projects/${var.project_id}/serviceAccounts/${var.google_service_account}"
  role               = "roles/iam.workloadIdentityUser"
  member             = "serviceAccount:${var.project_id}.svc.id.goog[${var.namespace}/mig-pipeline]"
}

output "pipeline_service_account" {
  description = "Set this as `service_account_name` on the KubernetesPodOperator tasks."
  value       = var.enabled && var.google_service_account != "" ? kubernetes_service_account.pipeline[0].metadata[0].name : ""
}

output "role_binding" {
  value = var.enabled ? kubernetes_role_binding.pod_launcher[0].metadata[0].name : ""
}
