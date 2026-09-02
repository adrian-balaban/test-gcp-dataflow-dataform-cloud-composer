#!/usr/bin/env python3
"""Run the whole MIG 000001-1 chain locally: E → T+R → L.

This mirrors composer/dags/mig_000001_1.py task for task. Two orchestrators exist on
purpose: the DAG is the real, Composer-ready article, and this is what runs when the
(heavy, optional) Airflow container is not up. If they ever disagree, the DAG is right.

Every run is one full snapshot of the source, scoped only by `run_id`
(docs/PLAN-CHANGES-21082026.md D5) — there is no run kind and no window.

    python local/scripts/run_pipeline.py
"""
from __future__ import annotations

import argparse
import os
import subprocess
from dataclasses import replace
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from pipelines.common.config import Config, require_identifier  # noqa: E402

# This orchestrator runs the Beam pipelines as local subprocesses. Even against real GCP
# (`--profile real`, used by `make smoke-gcp`) the pipelines must run in-process on
# DirectRunner against real GCS/BQ via ADC — spinning up a Dataflow job per stage is not
# what a smoke test is for. The Composer DAG is what launches the Flex Templates on
# DataflowRunner; that path does not go through here. See pipelines/common/runner.py.
os.environ.setdefault("MIG_RUNNER", "direct")

# The three Java apps this orchestrator shells out to, as the self-contained jars
# Maven's shade plugin writes.
JAVA_MODULES = ("extractor-app", "loader-app", "recon-service")

JAVA_APPS = {
    "extractor": "apps/extractor-app/target/extractor-app.jar",
    "loader": "apps/loader-app/target/loader-app.jar",
    "recon": "apps/recon-service/target/recon-service.jar",
}


def java_cmd(app: str) -> list[str]:
    return ["java", "-jar", str(ROOT / JAVA_APPS[app])]


def banner(step: str, detail: str = "") -> None:
    print(f"\n\033[1m▶ {step}\033[0m {detail}", flush=True)


def sh(cmd: list[str], what: str) -> None:
    started = time.time()
    proc = subprocess.run(cmd, cwd=ROOT)
    if proc.returncode != 0:
        raise SystemExit(f"\n\033[31m{what} failed (exit {proc.returncode})\033[0m")
    print(f"  ({time.time() - started:.1f}s)")


def ensure_java_built() -> None:
    missing = [name for name, path in JAVA_APPS.items() if not (ROOT / path).is_file()]
    if not missing:
        return
    banner("maven", f"building {', '.join(missing)}")
    sh(["mvn", "-q", "-B", "-pl", ",".join(f"apps/{m}" for m in JAVA_MODULES), "-am",
        "package", "-DskipTests"], "maven build")


def gcp_endpoints(cfg: Config) -> tuple[str, str]:
    """The hosts the Java apps talk to: emulators locally, Google APIs on `real`.

    On the real profile the Java side authenticates with an OAuth access token minted
    from Application Default Credentials and passed via MIG_GCS_TOKEN (see
    HttpObjectStore.fromEnv / BigQueryRest.fromEnv).

    The token is minted through `google.auth`, not by shelling out to `gcloud`. The
    Python side already authenticates to GCS and BigQuery through that library, so
    asking a second mechanism for the same credential bought nothing — and it did not
    survive containerisation: `Dockerfile.toolbox` has no gcloud in it, so `make
    smoke-gcp` failed before it read a single record. This works with whatever ADC is
    present, a service-account key or a user credential.
    """
    if cfg.is_local:
        return cfg.storage_host, cfg.bq_host

    import google.auth
    import google.auth.transport.requests

    credentials, _ = google.auth.default(
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    credentials.refresh(google.auth.transport.requests.Request())
    os.environ["MIG_GCS_TOKEN"] = credentials.token
    os.environ["MIG_GCS_PROJECT"] = cfg.project
    # Same cloud-platform token authorises Managed Kafka OAUTHBEARER when the SA holds
    # roles/managedkafka.client. sinks.KafkaTargetWriter reads this via _oauth_token_cb;
    # the Java callback handler reads MIG_GCS_TOKEN. One token, two seams.
    os.environ["MIG_KAFKA_TOKEN"] = credentials.token
    return "https://storage.googleapis.com", "https://bigquery.googleapis.com"


def run_once(cfg: Config, run_id: str, accounts: int, sinks: str) -> None:
    """One complete pass through all three lanes of arch.diagram — one full snapshot."""
    py = sys.executable
    gcs_host, bq_host = gcp_endpoints(cfg)

    banner("harness", f"generating {accounts} accounts (csv)")
    sh([py, "-m", "harness.generate",
        "--accounts", str(accounts)], "harness")

    banner("E — Extractor App", "5 artefacts → archive → gzip → PGP → File Storage")
    sh([*java_cmd("extractor"),
        "--input", "local/data/mainframe/ACCOUNT.src",
        "--bucket", cfg.landing_bucket,
        "--gcs-host", gcs_host,
        "--run-id", run_id,
        "--gnupg-home", cfg.pgp_home,
        "--recipient", cfg.pgp_recipient,
        "--split", "300"], "extractor-app")

    banner("T — Dataflow File Processor",
           "decrypt, verify .CHS, TDS parse → BigQuery")
    sh([py, "-m", "pipelines.file_processor.pipeline", "--run-id", run_id], "file_processor")

    banner("T — Dataform", "SQL transformation: Extraction → Transformation dataset")
    sh([py, "local/scripts/run_dataform.py", "--run-id", run_id], "dataform")

    banner("T — Dataflow Data Enrichment", "reference-data join")
    sh([py, "-m", "pipelines.data_enrichment.pipeline", "--run-id", run_id], "data_enrichment")

    banner("T — Dataflow JSON Data Producer",
           f"map + schema, emit to {sinks}, batches of 200")
    sh([py, "-m", "pipelines.json_producer.pipeline",
        "--run-id", run_id, "--sinks", sinks], "json_producer")

    banner("L — Loader App", "download JSON → Target System APIs with retries → load artefacts")
    sh([*java_cmd("loader"),
        "--run-id", run_id,
        "--gcs-host", gcs_host,
        "--json-bucket", cfg.json_bucket,
        "--recon-bucket", cfg.recon_bucket,
        "--target-system-url", cfg.target_system_url,
        "--max-retries", str(cfg.target_system_max_retries),
        # The Load edge (docs/PLAN-CHANGES-02092026-kafka-loader.md). On `kafka` the
        # loader produces to the target topic and settles against the confirmation and
        # rejection streams; on `http` it POSTs as before and these are inert.
        "--sink", cfg.loader_sink,
        "--kafka-bootstrap", cfg.kafka_bootstrap,
        "--kafka-topic", cfg.kafka_topic,
        "--confirmation-topic", cfg.target_system_confirmation_topic,
        "--rejection-topic", cfg.target_system_rejection_topic,
        "--settle-timeout-seconds", str(cfg.loader_settle_timeout_seconds)], "loader-app")

    banner("R — Reconciliation Services", "source + transformation/load recon, migrability reports")
    sh([*java_cmd("recon"),
        "--run-id", run_id,
        "--gcs-host", gcs_host,
        "--bq-host", bq_host,
        "--project", cfg.project,
        "--landing-bucket", cfg.landing_bucket,
        "--recon-bucket", cfg.recon_bucket,
        "--ds-extraction", cfg.ds_extraction,
        "--ds-transformation", cfg.ds_transformation,
        "--ds-recon", cfg.ds_recon,
        "--confirmation-bootstrap", cfg.target_system_confirmation_bootstrap,
        "--confirmation-topic", cfg.target_system_confirmation_topic,
        # Fail closed unless the loader used HTTP, where its own status-code
        # classification is the verdict and the confirmation stream adds nothing
        # (docs/PLAN-CHANGES-02092026-kafka-loader.md §3.4). Derived from the sink so the
        # two settings cannot disagree.
        "--allow-unconfirmed-load",
        "true" if cfg.loader_sink == "http" else "false"], "recon-service")


def main() -> int:
    ap = argparse.ArgumentParser(description="Run the MIG 000001-1 chain end to end")
    ap.add_argument("--profile", choices=("local", "real"), default=None)
    ap.add_argument("--accounts", type=int, default=500)
    ap.add_argument("--sinks", default="both", choices=("kafka", "gcs", "both"))
    ap.add_argument("--run-id", default=None, help="override the generated run id")
    args = ap.parse_args()

    cfg = Config.from_env()
    if args.profile:
        # `replace(...).validate()`, not a bare rebuild. Constructing Config directly
        # skips validate(), which is the one guard that rejects TARGET_PROFILE=real with
        # BQ_TARGET=emulator — real object store, throwaway warehouse. Overriding the
        # profile here is exactly the path that can create that pairing, so it is also
        # the path that most needs checking.
        cfg = replace(cfg, profile=args.profile).validate()

    ensure_java_built()
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")

    # The run id names GCS object paths and is interpolated into every DELETE the
    # pipelines issue, so it is checked once here, at the outermost entry point.
    run_id = require_identifier("--run-id", args.run_id or f"run-{stamp}")
    run_once(cfg, run_id, args.accounts, args.sinks)

    print(f"\n\033[1mrun:\033[0m {run_id}")

    (ROOT / "local/state").mkdir(parents=True, exist_ok=True)
    (ROOT / "local/state/last_run_id").write_text(run_id, encoding="utf-8")
    # Recorded so `make verify` knows which sinks this run actually used — the Kafka
    # assertions are skipped for a gcs-only run (e.g. Managed Kafka is VPC-only and
    # unreachable from an operator laptop).
    (ROOT / "local/state/last_run_sinks").write_text(args.sinks, encoding="utf-8")
    return 0


if __name__ == "__main__":
    sys.exit(main())
