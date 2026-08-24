# Serverless VPC Access connector — lets the Cloud Run Target System mock reach the
# VPC-internal Managed Kafka broker (docs/PLAN-CHANGES-22082026.md).
#
# Managed Kafka attaches to mig-subnet inside mig-vpc, so it has no public endpoint. A
# Cloud Run service is serverless and lives outside any VPC by default; the connector is
# the bridge — it provisions a /28 of reserved IP space in the VPC and routes the mock's
# egress through it. Without it the mock's confirmation producer cannot open a socket to
# the broker and criterion 9 is unreachable from the Cloud Run path.
#
# Gated on the same flag as Kafka: the connector only exists when the broker does, so a
# no-Kafka run pays nothing for it. It auto-creates its own /28 subnet (no separate
# google_compute_subnetwork resource needed), which must not overlap mig-subnet's
# 10.20.0.0/20 — 10.20.128.0/28 is well clear.

variable "project_id" { type = string }
variable "region" { type = string }
variable "network_id" {
  type        = string
  description = "Self link / id of the mig-vpc network the connector attaches to."
}
variable "ip_cidr_range" {
  type        = string
  default     = "10.20.128.0/28"
  description = "A /28 inside mig-vpc, non-overlapping with mig-subnet (10.20.0.0/20)."
}
variable "enabled" {
  type        = bool
  default     = false
  description = "Only needed when Managed Kafka is on; off otherwise to avoid idle cost."
}

resource "google_vpc_access_connector" "this" {
  count    = var.enabled ? 1 : 0
  provider = google-beta

  name          = "mig-vpc-connector"
  project       = var.project_id
  region        = var.region
  network       = var.network_id
  ip_cidr_range = var.ip_cidr_range

  # Throughput in Mbps. The confirmation stream is tiny (one event per accepted row),
  # so the minimum band is plenty; the range lets GCP scale within it if traffic spikes.
  min_throughput = 200
  max_throughput = 300
}

# Empty when disabled, so `module.target_system_mock` can pass it through unconditionally
# and its dynamic block simply renders nothing.
output "connector_id" {
  value = var.enabled ? google_vpc_access_connector.this[0].id : ""
}