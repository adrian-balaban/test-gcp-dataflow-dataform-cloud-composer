#!/usr/bin/env bash
# Build and publish the three Dataflow Flex Template images, plus their spec JSONs.
#
# This is the `make build-templates` step. For each pipeline it:
#   1. builds a Docker image tagged into the Artifact Registry repo Terraform created
#      (output `dataflow_registry`, e.g. europe-west1-docker.pkg.dev/<project>/mig-dataflow);
#   2. pushes it (gcloud / docker / podman, whichever is configured);
#   3. writes a Flex Template spec JSON and uploads it to
#      gs://<project>-dataflow-templates/<template>.json — the `containerSpecGcsPath`
#      the Composer DAG points `DataflowStartFlexTemplateOperator` at.
#
# The image is one Dockerfile (Dockerfile.dataflow) parameterised by MIG_MODULE; the
# module is baked into the ENTRYPOINT, so each template resolves to its own pipeline.
#
# Authentication: `gcloud auth` (or the GOOGLE_APPLICATION_CREDENTIALS the caller exports)
# must already be set up; this script does not log in. The service account needs
# roles/artifactregistry.writer and storage.objectCreator on the templates bucket.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
cd "$ROOT"

# Project id, billing account and key file come from the environment — see _env.sh.
# shellcheck source=local/scripts/gcp/_env.sh
source local/scripts/gcp/_env.sh
require_gcp_env TF_VAR_billing_account

PROJECT="${TF_VAR_project_id}"
REGION="${TF_VAR_region}"

REGISTRY="$(terraform -chdir="$TF_DIR" output -raw dataflow_registry)"
# `templates_bucket` is a scalar output that exists precisely so this line needs no JSON
# parser — reading it out of the `buckets` map made python3 a prerequisite of the
# GCP-only path for one dictionary lookup.
TEMPLATES_BUCKET="$(terraform -chdir="$TF_DIR" output -raw templates_bucket 2>/dev/null || true)"
TEMPLATES_BUCKET="${TEMPLATES_BUCKET:-${PROJECT}-dataflow-templates}"

# Detect a working container CLI. podman and docker are interchangeable here.
CTX="$(detect_container_cli)" || exit 1

# Each template: module name (python package) -> template name (image tag / spec file).
declare -a TEMPLATES=(
  "file_processor:file-processor"
  "data_enrichment:data-enrichment"
  "json_producer:json-producer"
)

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

# Images are tagged with an immutable version, not just :latest. A Flex Template spec
# records the image it was built against, and Dataflow pulls that image at launch — so
# with a floating :latest, rebuilding silently changes what an already-published
# "stable" template runs, and a job cannot be reproduced after the fact.
#
# The version is the git SHA, suffixed -dirty when the tree has uncommitted changes so a
# local experiment can never be mistaken for a committed build. :latest is still pushed
# as a convenience pointer, but the spec JSON always references the pinned tag.
# Both build scripts derive the tag with derive_image_version (see _env.sh), but each in
# its own process — so building the Java images and these templates either side of an
# edit still lands them on <sha> and <sha>-dirty, and the DAG has only one
# MIG_JAVA_IMAGE_TAG for both. That mismatch is what sank the 2026-08-19 retest (the Java
# images were pushed as :review-fixes while these stayed on the git SHA). Use
# `make build-images` to build both on one derived tag, or set MIG_JAVA_IMAGE_TAG
# yourself and use it for both runs.
VERSION="$(derive_image_version)"
echo "image version: ${VERSION}"

for entry in "${TEMPLATES[@]}"; do
  module="${entry%%:*}"
  template="${entry##*:}"
  image="${REGISTRY}/${template}:${VERSION}"
  moving="${REGISTRY}/${template}:latest"
  spec="${TMP}/${template}.json"

  echo "▶ building ${template} (module=${module}) → ${image}"
  $CTX build -f Dockerfile.dataflow --build-arg MIG_MODULE="${module}" \
    -t "${image}" -t "${moving}" .
  echo "▶ pushing ${image} (and :latest)"
  # Requires a one-time `gcloud auth configure-docker ${REGION}-docker.pkg.dev`
  # (or the podman login equivalent). The caller's ADC is used.
  $CTX push "${image}"
  $CTX push "${moving}"

  # Flex Template spec — the DAG reads containerSpecGcsPath = gs://<bucket>/<template>.json
  #
  # Written with a heredoc rather than a JSON library: the document is static apart from
  # the image tag and one of three fixed parameter lists, and generating it in Python made
  # a Python interpreter a prerequisite of the GCP-only path. The parameters are
  # pipeline-arg specific; the launcher passes each as --<name>=<value>.
  case "${template}" in
    file-processor)
      params='
        { "name": "run_id", "label": "Run id", "help": "Migration run identifier" }'
      ;;
    json-producer)
      params='
        { "name": "run_id", "label": "Run id", "help": "Migration run identifier" },
        { "name": "sinks",  "label": "Sinks",  "help": "kafka | gcs | both" }'
      ;;
    *)  # data-enrichment
      params='
        { "name": "run_id", "label": "Run id", "help": "Migration run identifier" }'
      ;;
  esac

  cat > "${spec}" <<JSON
{
  "image": "${image}",
  "sdk_info": { "language": "PYTHON" },
  "metadata": {
    "name": "${template}",
    "description": "MIG 000001-1 Dataflow Flex Template: ${template}",
    "parameters": [${params}
    ]
  }
}
JSON

  echo "▶ uploading spec → gs://${TEMPLATES_BUCKET}/${template}.json"
  gcloud storage cp "${spec}" "gs://${TEMPLATES_BUCKET}/${template}.json"
done

echo
echo "✓ ${#TEMPLATES[@]} Flex Templates published."
echo "  registry: ${REGISTRY}"
echo "  specs:    gs://${TEMPLATES_BUCKET}/{file-processor,data-enrichment,json-producer}.json"