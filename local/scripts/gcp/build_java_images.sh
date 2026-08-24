#!/usr/bin/env bash
# Build and publish the two Java app images the Composer DAG launches (L and R lanes).
#
# This is the missing half of the GCP path (review finding H5): the Dataflow and Dataform
# tasks had Composer-ready operators, but `loader_app` and `reconciliation` shelled out to
# `apps/*/build/install/...` — paths that exist only on a developer laptop and never on a
# Composer worker. Publishing them as images lets the DAG launch them with
# KubernetesPodOperator on Composer's own GKE cluster, with no extra infrastructure.
#
# Cloud Run Jobs would also work and would be a defensible choice; KubernetesPodOperator
# was picked because Composer 2 already runs on GKE, so it needs no new Terraform module,
# no new service, and no new IAM surface.
#
# Tagging follows the same rule as build_templates.sh: the git short SHA, suffixed -dirty
# on an unclean tree, so a published image always identifies the source it was built from
# (M13). The rule is shared, the moment of evaluation is not — run this and
# build_templates.sh either side of an edit and they land on different tags, while the
# DAG has one MIG_JAVA_IMAGE_TAG for both. `make build-images` runs both on one tag.
#
# Authentication: `gcloud auth` (or GOOGLE_APPLICATION_CREDENTIALS) must already be set
# up; this script does not log in. The service account needs roles/artifactregistry.writer.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
cd "$ROOT"

# shellcheck source=local/scripts/gcp/_env.sh
source local/scripts/gcp/_env.sh
require_gcp_env TF_VAR_billing_account

REGISTRY="$(terraform -chdir="$TF_DIR" output -raw dataflow_registry)"

CTX="$(detect_container_cli)" || exit 1

# MIG_JAVA_IMAGE_TAG overrides the derived tag. Without an override the tag is the git
# SHA, which is right for a clean tree and ambiguous for a dirty one: every rebuild from
# the same working tree reuses "<sha>-dirty", so the tag stops identifying the contents.
# Passing an explicit tag is what lets Terraform's java_image_tag point at exactly the
# build you just pushed.
VERSION="$(derive_image_version)"
echo "image version: ${VERSION}"

# Maven module name == image name for the two Java apps. dataform-runner is not a Maven
# module — it is the Node+Python image that runs the unlinked Dataform path — so it is
# built from its own Dockerfile but published alongside, on the same tag, so the whole
# DAG can be pinned to one version.
# target-system-mock is in this list even though the DAG never launches it: it runs as the
# Cloud Run service the Loader posts to, and terraform pins that service to
# target-system-mock:latest. Leaving it out is how the deployed mock silently stayed on a
# months-old image while every other component moved — on 2026-08-23 that meant the mock
# kept publishing confirmations with a Kafka client that cannot do OAUTHBEARER on JDK 25,
# and recon reported "0 confirmations" for a fix that had already shipped everywhere else.
# It has its own Dockerfile (multi-stage Maven, like the compose stack uses).
declare -a APPS=(loader-app recon-service dataform-runner target-system-mock)

for app in "${APPS[@]}"; do
  image="${REGISTRY}/${app}:${VERSION}"
  moving="${REGISTRY}/${app}:latest"

  if [[ "$app" == "dataform-runner" ]]; then
    echo "▶ building ${app} → ${image}"
    $CTX build -f Dockerfile.dataform -t "${image}" -t "${moving}" .
  elif [[ "$app" == "target-system-mock" ]]; then
    echo "▶ building ${app} → ${image}"
    $CTX build -f apps/target-system-mock/Dockerfile -t "${image}" -t "${moving}" .
  else
    echo "▶ building ${app} → ${image}"
    $CTX build -f Dockerfile.javaapp --build-arg MIG_APP="${app}" \
      -t "${image}" -t "${moving}" .
  fi

  echo "▶ pushing ${image} (and :latest)"
  $CTX push "${image}"
  $CTX push "${moving}"
done

echo
echo "published:"
for app in "${APPS[@]}"; do
  echo "  ${REGISTRY}/${app}:${VERSION}"
done
echo
echo "Set MIG_JAVA_IMAGE_TAG=${VERSION} on the Composer environment so the DAG launches"
echo "these exact images rather than whatever :latest happens to be."
