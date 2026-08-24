# VPC for the Dataflow workers.
#
# Three things here are requirements, not preferences, for a banking workload:
#   * workers get no public IPs (`WORKER_IP_PRIVATE` in the Flex Template launch),
#   * Private Google Access so they can still reach GCS/BigQuery/Secret Manager,
#   * Cloud NAT for any remaining egress.
# Plus the inter-worker firewall rule Dataflow requires; without it, jobs hang at
# "workers started" with no useful error.

variable "project_id" { type = string }
variable "region" { type = string }
variable "subnet_cidr" {
  type    = string
  default = "10.20.0.0/20"
}

resource "google_compute_network" "this" {
  name                    = "mig-vpc"
  project                 = var.project_id
  auto_create_subnetworks = false
}

resource "google_compute_subnetwork" "this" {
  name    = "mig-subnet"
  project = var.project_id
  region  = var.region
  network = google_compute_network.this.id

  ip_cidr_range = var.subnet_cidr

  # Lets workers without external IPs reach Google APIs.
  private_ip_google_access = true

  log_config {
    aggregation_interval = "INTERVAL_10_MIN"
    flow_sampling        = 0.5
    metadata             = "INCLUDE_ALL_METADATA"
  }
}

resource "google_compute_router" "this" {
  name    = "mig-router"
  project = var.project_id
  region  = var.region
  network = google_compute_network.this.id
}

resource "google_compute_router_nat" "this" {
  name    = "mig-nat"
  project = var.project_id
  region  = var.region
  router  = google_compute_router.this.name

  nat_ip_allocate_option             = "AUTO_ONLY"
  source_subnetwork_ip_ranges_to_nat = "LIST_OF_SUBNETWORKS"

  subnetwork {
    name                    = google_compute_subnetwork.this.id
    source_ip_ranges_to_nat = ["ALL_IP_RANGES"]
  }

  log_config {
    enable = true
    filter = "ERRORS_ONLY"
  }
}

# Dataflow workers talk to each other on 12345-12346. Omitting this rule is the single
# most common cause of a job that starts and then simply never progresses.
resource "google_compute_firewall" "dataflow_internal" {
  name    = "mig-allow-dataflow-internal"
  project = var.project_id
  network = google_compute_network.this.name

  description = "Inter-worker communication for Dataflow (required)."

  allow {
    protocol = "tcp"
    ports    = ["12345-12346"]
  }

  source_tags = ["dataflow"]
  target_tags = ["dataflow"]
}

output "network_id" { value = google_compute_network.this.id }
output "subnet_id" { value = google_compute_subnetwork.this.id }
output "subnet_self_link" { value = google_compute_subnetwork.this.self_link }
