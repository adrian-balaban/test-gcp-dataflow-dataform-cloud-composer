# Artifact Registry for the Flex Template images.
#
# The templates themselves are built and pushed by `make build-templates`, not by
# Terraform: they are build artefacts that change with every code commit, and putting a
# `docker build` inside `terraform apply` couples deploys to infrastructure changes.
# Terraform owns the registry and the naming; CI owns the contents.

variable "project_id" { type = string }
variable "region" { type = string }

resource "google_artifact_registry_repository" "templates" {
  project       = var.project_id
  location      = var.region
  repository_id = "mig-dataflow"
  format        = "DOCKER"
  description   = "Dataflow Flex Template images for the three pipelines"

  cleanup_policies {
    id     = "keep-recent"
    action = "KEEP"
    most_recent_versions {
      keep_count = 10
    }
  }
}

output "registry" {
  value = "${var.region}-docker.pkg.dev/${var.project_id}/${google_artifact_registry_repository.templates.repository_id}"
}
