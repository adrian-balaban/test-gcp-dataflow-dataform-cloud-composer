# MIG 000001-1 — Mainframe → Target System prototype
# Every target is runnable locally with no GCP billing. See docs/runbook-gcp.md for the cloud path.

.DEFAULT_GOAL := help
SHELL := /bin/bash
.SHELLFLAGS := -eu -o pipefail -c

-include .env
export

VENV        := .venv
PY          := $(VENV)/bin/python
PYTEST      := $(VENV)/bin/pytest
# podman, deliberately — not docker. The `docker-compose` on this host is a real
# Docker Compose binary that talks to /var/run/docker.sock, and there is no Docker
# daemon here; podman-compose drives rootless podman directly.
PODMAN      := podman
COMPOSE     := podman-compose -f local/docker-compose.yml
GNUPGHOME   := $(CURDIR)/local/keys
# Maven is the sole Java build (see the operator note in README.md).

# ─────────────────────────────────────────────────────────────────── meta

.PHONY: help
help: ## Show this help
	@echo "MIG 000001-1 prototype — available targets:"
	@echo
	@grep -hE '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
	  | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'
	@echo
	@echo "Typical first run:  make bootstrap && make up && make init-infra && make run-initial && make verify"

# ─────────────────────────────────────────────────────── phase 0/3: environment

.PHONY: bootstrap
bootstrap: .env $(VENV) keys ## One-time setup: venv (Python 3.11), .env, throwaway PGP keypair

.env:
	@cp .env.example .env && echo "wrote .env from .env.example"

$(VENV): requirements.txt
	@echo "==> pinning Python 3.11 (host python is too new for Beam/Airflow)"
	uv venv --python 3.11 $(VENV)
	uv pip install --python $(VENV)/bin/python -r requirements.txt
	@touch $(VENV)

.PHONY: keys
keys: ## Generate the throwaway PGP keypair used by the extraction lane
	@bash local/scripts/gen_pgp_key.sh

.PHONY: up
up: ## Start the local stack (fake-gcs, bigquery-emulator, redpanda, target-system-mock)
	@bash local/scripts/check_container_runtime.sh
	$(COMPOSE) up -d
	@bash local/scripts/wait_for_stack.sh

.PHONY: up-airflow
up-airflow: ## Additionally start the Cloud Composer stand-in (heavy; http://localhost:8081)
	podman-compose -f local/docker-compose.airflow.yml up -d
	@echo "Airflow starting on http://localhost:8081 (admin/admin) — first boot takes ~60s"

.PHONY: down
down: ## Stop the local stack
	-podman-compose -f local/docker-compose.airflow.yml down -v
	$(COMPOSE) down -v

.PHONY: logs
logs: ## Tail local stack logs
	$(COMPOSE) logs -f

.PHONY: init-infra
init-infra: ## Create buckets, BigQuery datasets/tables and the Kafka topic in the local stack
	$(PY) local/scripts/init_infra.py

.PHONY: verify-stack
verify-stack: ## Healthcheck every local service
	$(PY) local/scripts/verify_stack.py

# ─────────────────────────────────────────────────────────── pipeline execution

.PHONY: generate
generate: ## Generate synthetic mainframe source data (harness)
	$(PY) -m harness.generate

.PHONY: run
run: ## Full end-to-end run — one full snapshot (E → T+R → L)
	$(PY) local/scripts/run_pipeline.py

.PHONY: run-initial
run-initial: run ## Alias for `run`, kept for READMEs written before deltas were removed

.PHONY: dataform
dataform: ## Compile and execute the Dataform SQLX models
	$(PY) local/scripts/run_dataform.py

# ───────────────────────────────────────────────────────────────── verification

.PHONY: verify
verify: ## Assert all acceptance criteria against the last run
	$(PY) -m tests.acceptance

.PHONY: verify-project2
verify-project2: ## Run mapping-project2 and prove zero engine-code diff — must-prove #3
	@bash local/scripts/verify_project2.sh

.PHONY: test
test: ## Unit tests (Python + Java)
	$(PYTEST) tests -q
	mvn -B test

# ────────────────────────────────────────────────────────────────── java builds

.PHONY: java-build
java-build: ## Build the five Java apps (self-contained jars at apps/<app>/target/<app>.jar)
	mvn -B -q package -DskipTests

.PHONY: java-clean
java-clean: ## Remove Java build output
	@rm -rf apps/*/target
	@echo "removed apps/*/target"

# ──────────────────────────────────────────────────────────────── real GCP path

.PHONY: tf-validate
tf-validate: ## terraform fmt -check && validate (works without billing)
	terraform -chdir=terraform/envs/dev fmt -check -recursive
	terraform -chdir=terraform/envs/dev init -backend=false
	terraform -chdir=terraform/envs/dev validate

.PHONY: build-templates
build-templates: ## Build + push the 3 Dataflow Flex Templates to Artifact Registry
	@bash local/scripts/gcp/build_templates.sh

# The DAG pins every image it launches with a single MIG_JAVA_IMAGE_TAG, so the Java
# images and the Flex Templates have to carry the same tag. Each build script derives its
# own tag when it runs, which is identical only while the tree does not change between
# the two runs — edit a file in between and one side gets <sha>, the other <sha>-dirty.
# This target derives the tag once and exports it, so both children build under it.
.PHONY: build-images
build-images: ## Build + push every image the DAG launches (Java apps + Flex Templates) on one tag
	@bash -c 'set -euo pipefail; \
	  source local/scripts/gcp/_env.sh; \
	  export MIG_JAVA_IMAGE_TAG="$${MIG_JAVA_IMAGE_TAG:-$$(derive_image_version)}"; \
	  echo "building every image on tag $$MIG_JAVA_IMAGE_TAG"; \
	  bash local/scripts/gcp/build_java_images.sh; \
	  bash local/scripts/gcp/build_templates.sh; \
	  echo; \
	  echo "all images published on tag $$MIG_JAVA_IMAGE_TAG"; \
	  echo "pass it to terraform as -var=java_image_tag=$$MIG_JAVA_IMAGE_TAG"'

.PHONY: deploy-dags
deploy-dags: ## Sync composer/dags to the Composer DAG bucket
	@bash local/scripts/gcp/deploy_dags.sh

.PHONY: deploy-dataform
deploy-dataform: ## Push dataform/ to the linked git remote and create a release config
	@bash local/scripts/gcp/deploy_dataform.sh

.PHONY: seed-secrets
seed-secrets: ## Push the PGP private key and Target System credentials into Secret Manager
	@bash local/scripts/gcp/seed_secrets.sh

TOOLBOX_IMAGE := mig-toolbox:local
# docker and podman are interchangeable on the GCP path — the images here are pushed to a
# registry or run once, never wired into the emulator stack that needs podman-compose.
CTR := $(shell command -v podman 2>/dev/null || command -v docker 2>/dev/null)

.PHONY: toolbox-image
toolbox-image: ## Build the container that runs smoke-gcp (no host Python needed)
	@test -n "$(CTR)" || { echo "error: neither podman nor docker on PATH" >&2; exit 1; }
	$(CTR) build -f Dockerfile.toolbox -t $(TOOLBOX_IMAGE) .

.PHONY: smoke-gcp
smoke-gcp: toolbox-image ## One tiny end-to-end run against real GCP infrastructure
	@test -f .env || { echo "error: .env missing — terraform output -raw env_file > .env" >&2; exit 1; }
	@test -n "$${GOOGLE_APPLICATION_CREDENTIALS:-}" \
	  || { echo "error: GOOGLE_APPLICATION_CREDENTIALS unset — source local/scripts/gcp/_env.sh" >&2; exit 1; }
	$(CTR) run --rm \
	  --env-file .env \
	  -e GOOGLE_APPLICATION_CREDENTIALS=/adc/key.json \
	  -v "$${GOOGLE_APPLICATION_CREDENTIALS}:/adc/key.json:ro" \
	  -v "$(CURDIR)/local/keys:/app/local/keys:ro" \
	  $(TOOLBOX_IMAGE) --profile real --accounts 50

# ─────────────────────────────────────────────────────────────────────── hygiene

.PHONY: clean
clean: ## Remove generated run artefacts (keeps .venv and keys)
	rm -rf out local/state local/data
