# Dataform repository — where the .sqlx models in dataform/definitions/ actually execute
# on GCP.
#
# The models themselves do not change between local and cloud: locally
# local/scripts/run_dataform.py runs `dataform compile` (a purely local operation) and
# executes the compiled SQL against the emulator; here the Dataform service compiles and
# runs the same files from git, invoked by Composer. That is the whole reason the local
# runner was built around the real CLI rather than around hand-written SQL.

variable "project_id" { type = string }
variable "region" { type = string }
variable "service_account" { type = string }
variable "git_remote_url" {
  type        = string
  default     = ""
  description = "HTTPS URL of the repo holding dataform/. Empty leaves the repository unlinked."
}
variable "git_branch" {
  type    = string
  default = "main"
}
variable "git_token_secret_version" {
  type        = string
  default     = ""
  description = "Secret Manager version holding the git PAT, e.g. projects/p/secrets/s/versions/1"
}

resource "google_dataform_repository" "this" {
  provider = google-beta

  name         = "mig-000001-1"
  project      = var.project_id
  region       = var.region
  service_account = var.service_account

  dynamic "git_remote_settings" {
    for_each = var.git_remote_url != "" ? [1] : []
    content {
      url                                 = var.git_remote_url
      default_branch                      = var.git_branch
      authentication_token_secret_version = var.git_token_secret_version
    }
  }

  workspace_compilation_overrides {
    default_database = var.project_id
  }
}

# A release config compiles the repository on a schedule; the workflow config runs it.
# Composer triggers these directly per run (with the run id as a compilation var), so the
# cron here is a safety net rather than the primary path.
resource "google_dataform_repository_release_config" "main" {
  provider = google-beta
  count    = var.git_remote_url != "" ? 1 : 0

  project    = var.project_id
  region     = var.region
  repository = google_dataform_repository.this.name

  name          = "main"
  git_commitish = var.git_branch
  cron_schedule = "0 3 * * *"
  time_zone     = "Europe/Bucharest"

  code_compilation_config {
    default_database = var.project_id
    default_schema   = "bq_transformation"
    default_location = var.region
    assertion_schema = "bq_recon"
  }
}

output "repository_id" { value = google_dataform_repository.this.name }
