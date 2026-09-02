"""MIG 000001-1 — Cloud Composer orchestration DAG.

The "Cloud Composer Orchestrator" box in architecture diagram. One DAG run == one migration run:
parameterised on `run_id`. Every run is one full snapshot of the source
(docs/PLAN-CHANGES-21082026.md D5) — there is no run kind and no window.

**One execution path: real GCP.** Every task here runs on Google infrastructure —
KubernetesPodOperator on Composer's own GKE cluster for the three Beam pipelines (each
pod submits a Dataflow job and owns its lifecycle — see `beam_pipeline` for why a pod
rather than a Flex Template) and for the Java Loader and Reconciliation apps, plus the
Dataform operators for the SQL transformation.

This DAG previously carried a second, `local` mode selected by `MIG_EXECUTION_MODE`,
which shelled out to laptop paths. That was removed: it meant two implementations of the
same graph, and its L and R lanes pointed at `apps/*/build/install/...` binaries that
never existed on a Composer worker, so GCP mode could not actually complete (review
finding H5). The local emulator stack still exists as the **test fixture** behind
`make run-initial` / `make verify` — `local/scripts/run_pipeline.py` is the entry point
there. What is gone is the duplicate production code path, not local testing.

Trigger with a config:

    {"run_id": "run-20260803"}
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor

# ── configuration ─────────────────────────────────────────────────────────────

PROJECT = os.environ.get("GCP_PROJECT", "mig-local")
REGION = os.environ.get("GCP_REGION", "europe-west1")
# The exact Java app images to launch, published by local/scripts/gcp/build_java_images.sh.
# Defaults to :latest so a fresh environment works, but set MIG_JAVA_IMAGE_TAG to the git
# SHA the script prints: a floating tag means a rebuild silently changes what this DAG
# runs, and a past run cannot be reproduced (M13).
# Pods must land in Composer's OWN namespace, not "default". The Airflow worker runs as
# system:serviceaccount:<composer-ns>:default, and Composer's RBAC scopes that account to
# its own namespace — asking for "default" fails with
#   pods is forbidden: User "system:serviceaccount:composer-...:default"
#   cannot list resource "pods" in the namespace "default"
# The environment exposes its namespace as COMPOSER_KUBERNETES_NAMESPACE; the sentinel
# below makes KubernetesPodOperator infer the current namespace when that is absent.
def _pod_namespace() -> str:
    """The namespace to launch pods into.

    Read from the file every pod carries, rather than from an environment variable:
    Composer's `--update-env-variables` silently drops some names and replaces the whole
    set on each call, so an env var here is not reliable. This file is mounted into the
    Airflow worker by Kubernetes itself and always names the worker's own namespace,
    which is the only one Composer's RBAC can be extended to.
    """
    token = "/var/run/secrets/kubernetes.io/serviceaccount/namespace"
    try:
        with open(token, encoding="utf-8") as handle:
            value = handle.read().strip()
            if value:
                return value
    except OSError:
        pass
    # Outside a pod (local DAG parsing, tests) there is no such file.
    return os.environ.get("COMPOSER_KUBERNETES_NAMESPACE") or "default"


POD_NAMESPACE = _pod_namespace()

# Created by terraform/modules/composer_rbac and annotated onto dataflow-worker.
POD_SERVICE_ACCOUNT = os.environ.get("MIG_POD_SERVICE_ACCOUNT", "mig-pipeline")
# Per-app identities, created by terraform/modules/composer_rbac and annotated onto the
# loader-app / recon-service Google accounts. They fall back to the shared SA so a cluster
# that predates them still runs.
LOADER_SERVICE_ACCOUNT = os.environ.get("MIG_LOADER_SERVICE_ACCOUNT", "mig-loader")
RECON_SERVICE_ACCOUNT = os.environ.get("MIG_RECON_SERVICE_ACCOUNT", "mig-recon")

JAVA_IMAGE_TAG = os.environ.get("MIG_JAVA_IMAGE_TAG", "latest")
JAVA_REGISTRY = os.environ.get(
    "MIG_JAVA_REGISTRY", f"{REGION}-docker.pkg.dev/{PROJECT}/mig-dataflow"
)

# Optional. Unset, Dataflow chooses a zone in REGION and the job fails outright if that
# zone is full (ZONE_RESOURCE_POOL_EXHAUSTED). Setting it makes a capacity retry a
# deliberate move to a named zone rather than another roll of the dice.
WORKER_ZONE = os.environ.get("MIG_WORKER_ZONE", "")

DATASET_EXTRACTION = os.environ.get("BQ_DATASET_EXTRACTION", "bq_extraction")
DATASET_TRANSFORMATION = os.environ.get("BQ_DATASET_TRANSFORMATION", "bq_transformation")
DATASET_RECON = os.environ.get("BQ_DATASET_RECON", "bq_recon")
LANDING_BUCKET = os.environ.get("GCS_LANDING_BUCKET", "mig-landing")
JSON_BUCKET = os.environ.get("GCS_JSON_BUCKET", f"{PROJECT}-json-out")
RECON_BUCKET = os.environ.get("GCS_RECON_BUCKET", f"{PROJECT}-recon")

# The Java apps default every endpoint to a local emulator — gcsHost localhost:4443,
# bqHost localhost:9050, targetSystemUrl localhost:8080 — because that is where they run under
# `make run-initial`. On Composer nothing overrode them, so loader_app died with
#   ObjectStoreException: request failed: http://localhost:4443/...?project=mig-local
# local/scripts/run_pipeline.py sets all of this for the real profile; the DAG did not.
# Launching the pods (H5) was never the same as pointing them at real infrastructure.
GCS_API = "https://storage.googleapis.com"
BQ_API = "https://bigquery.googleapis.com"
# Target System is a deployed service, not a convention — no default that could ever be
# right. Empty means the L lane has nowhere to push, which is a deployment gap the
# operator has to close (see docs/runbook-gcp.md).
TARGET_SYSTEM_URL = os.environ.get("MIG_TARGET_SYSTEM_URL", "")
# The confirmation stream (docs/PLAN-CHANGES-22082026.md): the mock publishes one event
# per accepted write and recon set-differences it against account_target. Empty (the
# default when Kafka is off) means recon skips the read and reports enabled=false, so a
# no-Kafka DAG run stays green instead of failing on zero confirmations.
TARGET_SYSTEM_CONFIRMATION_BOOTSTRAP = os.environ.get(
    "TARGET_SYSTEM_CONFIRMATION_BOOTSTRAP", ""
)
TARGET_SYSTEM_CONFIRMATION_TOPIC = os.environ.get(
    "TARGET_SYSTEM_CONFIRMATION_TOPIC", "target-system-confirmations"
)
# The rejection stream (docs/PLAN-CHANGES-02092026-kafka-loader.md). Once the Load edge is
# Kafka there is no response code to classify, so the loader's `.ERR` rows come from here
# instead of from HTTP 4xx. A document that gets neither a confirmation nor a rejection is
# reported as `unsettled`, and a non-zero `unsettled` fails the loader task.
TARGET_SYSTEM_REJECTION_TOPIC = os.environ.get(
    "TARGET_SYSTEM_REJECTION_TOPIC", "target-system-rejections"
)
# How long the loader waits on the two return topics before calling the rest unsettled.
# New operational parameter with no HTTP analogue: HTTP settled every document
# synchronously, so "how long do we wait" was never a question anyone had to answer.
LOADER_SETTLE_TIMEOUT_SECONDS = os.environ.get("LOADER_SETTLE_TIMEOUT_SECONDS", "120")
# Managed Kafka speaks SASL_SSL/OAUTHBEARER; local redpanda speaks PLAINTEXT. Both the
# Python sink (pipelines/common/sinks.py) and the Java consumer (ReconService) read this
# from the environment, so it has to reach the *pods* — the Composer environment variable
# alone is invisible to a KubernetesPodOperator, which passes only what env_vars names.
# Omitting it is not a silent no-op: recon would open a PLAINTEXT connection to a
# SASL_SSL-only broker and hang until its poll deadline, reporting zero confirmations.
KAFKA_SECURITY_PROTOCOL = os.environ.get("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "target-system-target")
# Which Load edge to use. `kafka` is the target design; `http` keeps the original
# POST /v1/accounts path runnable for one release so both can be compared against the same
# acceptance suite. Defaults to http when no broker exists, so a no-Kafka environment still
# has a working Load lane rather than a loader that refuses to start on a missing
# bootstrap. Declared after KAFKA_BOOTSTRAP because it reads it.
LOADER_SINK = os.environ.get("MIG_LOADER_SINK", "kafka" if KAFKA_BOOTSTRAP else "http")

DEFAULT_ARGS = {
    "owner": "our-team",
    "depends_on_past": False,
    # Failure is routine at this scale — retry the task, and let the per-batch
    # checkpointing make the retry cheap.
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": False,
}

# Templated from the trigger config, so every task is scoped to one run.
RUN_ID = "{{ dag_run.conf.get('run_id', run_id) }}"


def beam_pipeline(task_id: str, module: str, arguments: list[str]) -> KubernetesPodOperator:
    """Run one Beam pipeline as a pod that submits to Dataflow and waits for it.

    **Why a pod rather than a Flex Template.** A Flex Template launch runs the pipeline
    module in a sandbox VM with `--template_location`, which makes Beam *stage* the job
    instead of running it. All three pipelines then call `wait_until_finish()` and read
    metrics off the result — there is no job to query, so every launch died with
    `ValueError: Can not query metrics. Job id is unknown.` The same assumption bites
    twice: `data_enrichment` and `json_producer` also call `bq.query(...)` at
    construction time and feed the rows into `beam.Create`, which under a template would
    embed every row into the job graph as a literal at *launch* time.

    Running the module in a pod restores the assumption it was written under: this
    process owns the job lifecycle, so `pipeline.run()` submits to Dataflow (MIG_RUNNER=
    dataflow), `wait_until_finish()` blocks until the job is done, and the metrics read,
    `require_balance` and the `run_ledger` write all work as written.

    The Flex Template path is deliberately kept (`Dockerfile.dataflow`,
    `build_templates.sh`, the specs in GCS) — it is the right destination once the
    pipelines are lifecycle-agnostic and read through `ReadFromBigQuery` rather than
    `beam.Create`.
    """
    return KubernetesPodOperator(
        task_id=task_id,
        name=task_id.replace("_", "-"),
        namespace=POD_NAMESPACE,
        image=f"{JAVA_REGISTRY}/{module.replace('_', '-')}:{JAVA_IMAGE_TAG}",
        # Always re-pull. MIG_JAVA_IMAGE_TAG is a git SHA, and a dirty tree appends
        # "-dirty" — a tag that is mutable by construction, because every rebuild from the
        # same working tree reuses it. Kubernetes defaults to IfNotPresent for any tag
        # other than :latest, so a node that has already pulled the tag keeps serving the
        # old layer and a rebuilt fix silently does not take effect.
        image_pull_policy="Always",
        # `cmds` becomes the pod's `command:`, which overrides the image ENTRYPOINT.
        # That override is required, not cosmetic: the template-launcher base image sets
        # ENTRYPOINT ["/opt/apache/beam/boot"], which swallows any argv it is given and
        # exits with "No id provided." Naming the interpreter explicitly bypasses it.
        cmds=["python", "-m", f"pipelines.{module}.pipeline"],
        arguments=arguments,
        get_logs=True,
        is_delete_operator_pod=True,
        # Workload Identity: this KSA is annotated onto dataflow-worker, so the pod
        # inherits that account's BigQuery/GCS/Secret Manager grants. Without it the pod
        # runs as the node's default SA and is denied on its first API call.
        service_account_name=POD_SERVICE_ACCOUNT,
        # The pod submits a Dataflow job and blocks on it, so it lives as long as the
        # job does — minutes, not seconds.
        # 900s, not 600. On a *cold* Composer environment the pod waits for Autopilot to
        # provision a node AND for a multi-GB Beam image to be pulled with
        # image_pull_policy=Always; on 2026-08-19 that combination exceeded 600s and the
        # task failed with "Pod took longer than 600 seconds to start" — an infrastructure
        # timeout that reads like a pipeline failure. Warm clusters start in well under a
        # minute, so this ceiling only ever costs time on the first run after a rebuild.
        startup_timeout_seconds=900,
        env_vars={
            "TARGET_PROFILE": "real",
            "BQ_TARGET": "real",
            "MIG_RUNNER": "dataflow",
            "GCP_PROJECT": PROJECT,
            "GCP_REGION": REGION,
            "GCS_LANDING_BUCKET": LANDING_BUCKET,
            # Dataflow workers must run the same image as this pod, or they cannot
            # import the `pipelines` package the DoFns live in.
            "MIG_SDK_CONTAINER_IMAGE": (
                f"{JAVA_REGISTRY}/{module.replace('_', '-')}:{JAVA_IMAGE_TAG}"
            ),
            # Empty means "let Dataflow choose", which is the default behaviour.
            "MIG_WORKER_ZONE": WORKER_ZONE,
            # json_producer runs with --sinks both, so the Kafka target writer is built
            # on the launcher and shipped into the Dataflow workers. Empty bootstrap is
            # the no-Kafka path: sinks.target_writer drops the Kafka writer entirely.
            "KAFKA_BOOTSTRAP": KAFKA_BOOTSTRAP,
            "KAFKA_TOPIC": KAFKA_TOPIC,
            "KAFKA_SECURITY_PROTOCOL": KAFKA_SECURITY_PROTOCOL,
        },
    )


def java_app(
    task_id: str, app: str, arguments: list[str], service_account: str | None = None
) -> KubernetesPodOperator:
    """Run one of the Java apps as a pod on Composer's GKE cluster.

    The L and R lanes are Java, not Beam, so they have no Flex Template. Launching them
    as pods keeps them on infrastructure Composer already owns — no Cloud Run service, no
    extra Terraform module, no new IAM surface.

    `is_delete_operator_pod` keeps the cluster clean; `get_logs` puts the app's stdout in
    the Airflow task log, which is where an operator will look first when a load fails.

    `service_account` selects the pod's Kubernetes SA, and through Workload Identity the
    Google identity it acts as. It defaults to the shared `mig-pipeline` (dataflow-worker)
    so a pod always has *some* identity, but the loader and recon tasks pass their own:
    module.iam writes deliberately narrow roles for loader-app and recon-service — recon
    may only read BigQuery — and before this every pod ran as dataflow-worker, so those
    roles were never the ones in force.
    """
    return KubernetesPodOperator(
        task_id=task_id,
        name=task_id.replace("_", "-"),
        namespace=POD_NAMESPACE,
        image=f"{JAVA_REGISTRY}/{app}:{JAVA_IMAGE_TAG}",
        # Always re-pull. MIG_JAVA_IMAGE_TAG is a git SHA, and a dirty tree appends
        # "-dirty" — a tag that is mutable by construction, because every rebuild from the
        # same working tree reuses it. Kubernetes defaults to IfNotPresent for any tag
        # other than :latest, so a node that has already pulled the tag keeps serving the
        # old layer and a rebuilt fix silently does not take effect.
        image_pull_policy="Always",
        arguments=arguments,
        get_logs=True,
        is_delete_operator_pod=True,
        # Same Workload Identity mapping as the Beam pods — the Java apps read GCS and
        # BigQuery too, so they need a real GCP identity rather than the node default.
        service_account_name=service_account or POD_SERVICE_ACCOUNT,
        startup_timeout_seconds=300,
        env_vars={
            # HttpObjectStore.createBucket sends ?project=, defaulting to the literal
            # "mig-local". Against real GCS that names a project this account cannot see.
            "MIG_GCS_PROJECT": PROJECT,
            # NOTE: MIG_GCS_TOKEN is *not* set here, and cannot usefully be — an access
            # token lives ~1h while a DAG is long-lived, so freezing one into the pod spec
            # would be a credential that expires mid-migration. The Java apps talk raw
            # HTTP (HttpObjectStore / BigQueryRest), so unlike the Beam pods they do not
            # pick up Workload Identity automatically — which is why `GcpToken` fetches a
            # token from the GKE metadata server and refreshes it, wired into
            # HttpObjectStore.fromEnv and BigQueryRest.fromEnv. MIG_GCS_TOKEN still wins
            # when set, which is how the local orchestrator injects one. Both tasks run
            # against real GCS and BigQuery this way (verified 2026-08-18, run 3).
            "GCP_PROJECT": PROJECT,
            "GCP_REGION": REGION,
            # recon-service's confirmation consumer authenticates to Managed Kafka with
            # the pod's own Workload Identity token (GcpTokenOauthCallbackHandler); this
            # is the switch that turns that on. PLAINTEXT on the local/no-Kafka path.
            "KAFKA_SECURITY_PROTOCOL": KAFKA_SECURITY_PROTOCOL,
        },
    )


def assert_run_balanced(**context) -> None:
    """Fail the DAG if the balancing equation does not close.

    "A run that doesn't balance is a failed run." This is the gate that makes that true
    operationally rather than aspirationally — without it, an imbalance is a line in a
    report nobody reads.
    """
    from google.cloud import bigquery

    run_id = context["dag_run"].conf.get("run_id") or context["run_id"]
    client = bigquery.Client(project=PROJECT)
    rows = list(
        client.query(
            f"SELECT src_read, extraction_written, rejected, balanced "
            f"FROM `{PROJECT}.{DATASET_RECON}.run_ledger` WHERE run_id = @run_id",
            job_config=bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("run_id", "STRING", run_id)
                ]
            ),
        ).result()
    )
    if not rows:
        raise ValueError(f"no run_ledger row for run {run_id} — the file processor did not complete")

    row = rows[0]
    if not row["balanced"]:
        not_migrated = row["rejected"]
        raise ValueError(
            f"run {run_id} does not balance: src_read={row['src_read']} != "
            f"written_at_extraction={row['extraction_written']} + not_migrated={not_migrated} "
            f"(rejected={row['rejected']})"
        )


with DAG(
    dag_id="mig_000001_1_migration",
    description="Mainframe (Source) → Target migration: extract, transform, reconcile, load",
    default_args=DEFAULT_ARGS,
    schedule=None,          # triggered per run, never on a timer
    start_date=datetime(2026, 8, 1),
    catchup=False,
    max_active_runs=1,      # one migration run at a time — runs share BigQuery tables
    tags=["mig", "migration", "target-system"],
) as dag:

    # ── E: wait for the extract ───────────────────────────────────────────────
    #
    # The .FLG semaphore is the entire contract with the other team's Extractor: it is
    # written last, and only once the encrypted bundle is durable. Nothing downstream
    # reads a byte before it appears.
    wait_for_extract = GCSObjectExistenceSensor(
        task_id="wait_for_extract_flg",
        bucket=LANDING_BUCKET,
        object=f"extraction/{RUN_ID}/ACCOUNT.FLG",
        poke_interval=60,
        timeout=60 * 60 * 12,
        mode="reschedule",   # free the worker slot while waiting hours for a mainframe
    )

    # ── T: file processing → BigQuery Extraction dataset ──────────────────────
    # NOTE: the DataflowStartFlexTemplateOperator version of these three tasks was
    # removed because a Flex Template launch stages rather than runs the job (see
    # `beam_pipeline` above). The template images and specs are still built and
    # published by local/scripts/gcp/build_templates.sh, and the operator form
    # (`DataflowStartFlexTemplateOperator`) is recorded here so it can be restored once
    # the pipelines no longer read metrics off their own result.

    common_args = [
        "--run-id", RUN_ID,
    ]
    file_processor = beam_pipeline("file_processor", "file_processor", common_args)
    data_enrichment = beam_pipeline("data_enrichment", "data_enrichment", ["--run-id", RUN_ID])
    json_producer = beam_pipeline(
        "json_producer", "json_producer", ["--run-id", RUN_ID, "--sinks", "both"]
    )

    # ── T: Dataform SQL transformation ────────────────────────────────────────
    #
    # A pod running the *unlinked* path, not DataformCreateCompilationResultOperator.
    # That operator needs `git_commitish`, which requires the Dataform repository to be
    # linked to a git remote; this one is not (H7 left that path deliberately unwired),
    # so every compile failed with
    #   400 The git reference 'main' could not be resolved.
    # `dataform compile --json` plus the executor in run_dataform.py is the path that is
    # actually deployed, and the one deploy_dataform.sh exercises.
    dataform_run = KubernetesPodOperator(
        task_id="dataform_run",
        name="dataform-run",
        namespace=POD_NAMESPACE,
        image=f"{JAVA_REGISTRY}/dataform-runner:{JAVA_IMAGE_TAG}",
        # Always re-pull. MIG_JAVA_IMAGE_TAG is a git SHA, and a dirty tree appends
        # "-dirty" — a tag that is mutable by construction, because every rebuild from the
        # same working tree reuses it. Kubernetes defaults to IfNotPresent for any tag
        # other than :latest, so a node that has already pulled the tag keeps serving the
        # old layer and a rebuilt fix silently does not take effect.
        image_pull_policy="Always",
        arguments=["--run-id", RUN_ID],
        get_logs=True,
        is_delete_operator_pod=True,
        service_account_name=POD_SERVICE_ACCOUNT,
        # 900s, not 600. On a *cold* Composer environment the pod waits for Autopilot to
        # provision a node AND for a multi-GB Beam image to be pulled with
        # image_pull_policy=Always; on 2026-08-19 that combination exceeded 600s and the
        # task failed with "Pod took longer than 600 seconds to start" — an infrastructure
        # timeout that reads like a pipeline failure. Warm clusters start in well under a
        # minute, so this ceiling only ever costs time on the first run after a rebuild.
        startup_timeout_seconds=900,
        env_vars={
            "TARGET_PROFILE": "real",
            "BQ_TARGET": "real",
            "GCP_PROJECT": PROJECT,
            "GCP_REGION": REGION,
        },
    )
    dataform_stage = dataform_run
    dataform_entry = dataform_run

    # ── L: push to Target System ─────────────────────────────────────────────────
    #
    # Java, so no Flex Template — launched as a pod on Composer's own GKE cluster from
    # the image build_java_images.sh publishes. This is the lane that review finding H5
    # identified as unrunnable on Composer; it pointed at a laptop path before.
    loader = java_app(
        "loader_app",
        "loader-app",
        [
            "--run-id", RUN_ID,
            # Without these the app keeps its local-development defaults and talks to
            # http://localhost:4443 and http://localhost:8080 inside the pod.
            "--gcs-host", GCS_API,
            "--json-bucket", JSON_BUCKET,
            "--recon-bucket", RECON_BUCKET,
            # Still passed on both paths: --sink http uses it, and --sink kafka ignores
            # it, so flipping the sink back needs no other change.
            "--target-system-url", TARGET_SYSTEM_URL,
            # The Load edge (docs/PLAN-CHANGES-02092026-kafka-loader.md). On `kafka` the
            # loader produces to KAFKA_TOPIC and then settles the run against the two
            # return topics; the confirmation topic gives it `accepted`, the rejection
            # topic gives it `.ERR`, and whatever neither mentions is `unsettled` and
            # fails the task. On `http` these are inert and the POST path runs as before.
            "--sink", LOADER_SINK,
            "--kafka-bootstrap", KAFKA_BOOTSTRAP,
            "--kafka-topic", KAFKA_TOPIC,
            "--confirmation-topic", TARGET_SYSTEM_CONFIRMATION_TOPIC,
            "--rejection-topic", TARGET_SYSTEM_REJECTION_TOPIC,
            "--settle-timeout-seconds", LOADER_SETTLE_TIMEOUT_SECONDS,
        ],
        service_account=LOADER_SERVICE_ACCOUNT,
    )

    # ── R: reconciliation, then the gate ──────────────────────────────────────
    reconciliation = java_app(
        "reconciliation",
        "recon-service",
        [
            "--run-id", RUN_ID,
            # Recon reads GCS *and* BigQuery, so it needs both hosts named; the defaults
            # are localhost:4443 and localhost:9050.
            "--gcs-host", GCS_API,
            "--bq-host", BQ_API,
            "--project", PROJECT,
            "--landing-bucket", LANDING_BUCKET,
            "--recon-bucket", RECON_BUCKET,
            "--ds-extraction", DATASET_EXTRACTION,
            "--ds-transformation", DATASET_TRANSFORMATION,
            "--ds-recon", DATASET_RECON,
            # The confirmation stream is the only evidence a Kafka load happened, so an
            # empty bootstrap now *fails* recon rather than reporting enabled=false and
            # exiting 0 (docs/PLAN-CHANGES-02092026-kafka-loader.md §3.4). The opt-out is
            # tied to the sink rather than set independently: on --sink http the loader's
            # own 201/4xx classification is the verdict and the stream adds nothing, so
            # skipping is legitimate; on --sink kafka nothing else proves the load, so the
            # two settings cannot be allowed to disagree.
            "--confirmation-bootstrap", TARGET_SYSTEM_CONFIRMATION_BOOTSTRAP,
            "--allow-unconfirmed-load", "true" if LOADER_SINK == "http" else "false",
            "--confirmation-topic", TARGET_SYSTEM_CONFIRMATION_TOPIC,
        ],
        service_account=RECON_SERVICE_ACCOUNT,
    )

    balance_gate = PythonOperator(
        task_id="assert_run_balanced",
        python_callable=assert_run_balanced,
    )

    (
        wait_for_extract
        >> file_processor
        >> dataform_entry
    )
    dataform_stage >> data_enrichment >> json_producer >> loader >> reconciliation >> balance_gate
