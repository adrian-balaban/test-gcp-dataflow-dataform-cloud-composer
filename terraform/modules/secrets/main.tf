# Secret Manager entries.
#
# Locally the PGP keypair sits in local/keys/ — fine for a throwaway prototype key, and
# unacceptable for anything else. Here the secrets are *declared* by Terraform but their
# values are pushed separately by `make seed-secrets`: putting a private key in a
# .tfvars file would move the problem rather than solve it, since state itself is then
# sensitive.

variable "project_id" { type = string }
variable "region" { type = string }
variable "accessors" {
  type        = map(string)
  description = "secret name => service account email allowed to read it"
  default     = {}
}

locals {
  secrets = {
    pgp-private-key = "OpenPGP private key used to decrypt extraction bundles"
    pgp-passphrase  = "Passphrase for the PGP private key"
    target-system-credentials = "Client credentials for the Target System loader APIs"
    dataform-git-token = "PAT used by the Dataform repository to read the models from git"
  }
}

resource "google_secret_manager_secret" "this" {
  for_each = local.secrets

  project   = var.project_id
  secret_id = each.key

  replication {
    user_managed {
      replicas {
        location = var.region
      }
    }
  }

  labels = {
    system = "mig-000001-1"
  }
}

resource "google_secret_manager_secret_iam_member" "accessors" {
  for_each = var.accessors

  project   = var.project_id
  secret_id = google_secret_manager_secret.this[each.key].secret_id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${each.value}"
}
