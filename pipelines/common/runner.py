"""Build Beam `PipelineOptions` for the local or the Dataflow runner.

The pipeline code is one shape in both worlds; only the runner and its options differ.
Locally DirectRunner; on GCP DataflowRunner with the temp/staging locations, worker
service account and subnetwork that Cloud Composer passes in (or, when run standalone
via `make smoke-gcp`, the conventions derived from the project).

On GCP the pipeline module runs inside a pod launched by
`composer/dags/mig_000001_1.py`, and that pod submits the Dataflow job itself, so the
options built here *are* the job's options. The DAG passes `MIG_RUNNER`, `GCP_PROJECT`,
`GCP_REGION` and `MIG_SDK_CONTAINER_IMAGE` through the pod env, and leaves the worker
service account and subnetwork to the conventions below — the same names Terraform
provisions, so they agree by construction. `DATAFLOW_SERVICE_ACCOUNT` and
`DATAFLOW_SUBNETWORK` override them for ad-hoc runs.
"""

from __future__ import annotations

import os
from typing import Any

from apache_beam.options.pipeline_options import PipelineOptions

from .config import Config, Runner

# Conventional resources provisioned by terraform/modules/{network,iam,storage}.
_DEFAULT_REGION = "europe-west1"
_SUBNET = f"regions/{_DEFAULT_REGION}/subnetworks/mig-subnet"


def pipeline_options(cfg: Config, argv: list[str] | None = None) -> PipelineOptions:
    """Return PipelineOptions for the configured world.

    `argv` lets a caller forward extra flags (e.g. `--experiments`, SDK overrides) without
    knowing which runner is active.

    Runner selection lives in `Config.runner`, not here — `MIG_RUNNER` forces a runner,
    otherwise the backend implies one (LOCAL → Direct, GCP → Dataflow). This module only
    turns that decision into flags. Keeping the choice next to the endpoint choice is what
    lets `Config.validate` reject "local emulators, Dataflow workers", which is
    unreachable-by-construction rather than merely unusual.

    The two combinations that remain legal are the deliberate ones: `make smoke-gcp` runs
    the pipelines in-process (Direct) against real GCS/BigQuery via ADC, and the Composer
    DAG runs them on Dataflow — same pipeline code, different runners.
    """
    flags: list[str] = list(argv or [])

    # Flags the Dataflow Flex Template launcher appended to the command line
    # (--runner, --project, --temp_location, --template_location, …). The entry-point
    # shim parks them here because the pipelines' own argparse would reject them; Beam
    # needs them, and --template_location in particular is what makes a Flex Template
    # launch *construct* the job rather than run it. See dataflow_entrypoint.py.
    launcher_flags = os.environ.get("MIG_BEAM_ARGS", "").strip()
    if launcher_flags:
        import shlex

        flags += shlex.split(launcher_flags)

    # The runner choice is resolved by Config alongside the endpoint choice, so the two
    # cannot disagree — a local backend asking for DataflowRunner is rejected there rather
    # than producing workers that cannot reach the emulators.
    if cfg.runner is Runner.DIRECT:
        flags += ["--runner=DirectRunner", "--no_save_main_session"]
        return PipelineOptions(flags)

    project = cfg.project
    region = os.environ.get("GCP_REGION", _DEFAULT_REGION)
    subnet = os.environ.get("DATAFLOW_SUBNETWORK", f"regions/{region}/subnetworks/mig-subnet")
    service_account = os.environ.get(
        "DATAFLOW_SERVICE_ACCOUNT", f"dataflow-worker@{project}.iam.gserviceaccount.com"
    )

    flags += [
        "--runner=DataflowRunner",
        f"--project={project}",
        f"--region={region}",
        f"--temp_location=gs://{project}-dataflow-temp/tmp",
        f"--staging_location=gs://{project}-dataflow-temp/staging",
        f"--service_account_email={service_account}",
        f"--subnetwork={subnet}",
        "--no_use_public_ips",
        # Pickles module-level globals the DoFns close over.
        "--save_main_session",
    ]

    # Dataflow workers run a stock Beam SDK image, which does not contain this repo —
    # so a DoFn importing `pipelines.common...` dies with ModuleNotFoundError even though
    # the launching process imported it fine. Pointing the workers at the same image the
    # pipeline is launched from makes the code present on both sides by construction,
    # rather than shipping a source distribution and hoping the two stay in step.
    #
    # MIG_SDK_CONTAINER_IMAGE is set by the DAG to the exact pinned tag it launched.
    sdk_image = os.environ.get("MIG_SDK_CONTAINER_IMAGE", "").strip()
    if sdk_image:
        flags += [f"--sdk_container_image={sdk_image}"]

    # Left unset, Dataflow picks a zone in the region and a run dies outright when that
    # zone happens to be full — `ZONE_RESOURCE_POOL_EXHAUSTED`, which is a capacity
    # shortage at Google rather than anything wrong here. Pinning the zone turns the
    # remedy into a deterministic retry against a different one instead of a reroll.
    worker_zone = os.environ.get("MIG_WORKER_ZONE", "").strip()
    if worker_zone:
        flags += [f"--worker_zone={worker_zone}"]

    return PipelineOptions(flags)


def resolve(options: PipelineOptions) -> Any:
    """Convenience: return the underlying options object `beam.Pipeline(options=...)` takes."""
    return options