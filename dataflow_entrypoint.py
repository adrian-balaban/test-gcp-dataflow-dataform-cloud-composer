"""Flex Template entry point — the file Dataflow's launcher executes.

A Flex Template launcher runs a *file* (`FLEX_TEMPLATE_PYTHON_PY_FILE`), while the three
pipelines are package modules (`pipelines.file_processor.pipeline` and friends) whose
relative imports only work when imported as part of the package. This shim bridges the
two: it stays a plain file, and dispatches to whichever module the image was built for.

`MIG_MODULE` is baked in at build time (see Dockerfile.dataflow), so one Dockerfile
produces three images that differ only in which pipeline they launch.

Arguments arrive from two directions at once, which is the subtlety this file exists to
handle:

* the Flex Template `parameters` map, as `--run_id=…` — the pipeline's own arguments,
  which its argparse understands;
* Beam/Dataflow's own flags, appended by the launcher — `--runner`, `--project`,
  `--temp_location`, `--staging_location`, `--service_account_email`, `--subnetwork`,
  `--no_use_public_ips`, and so on.

Every pipeline uses `parse_args()`, which *errors* on unknown flags, so passing the
combined list through unchanged fails the launch with "unrecognized arguments" and
exit status 2. Splitting them here keeps each pipeline's CLI honest (an unknown flag is
still an error when a human runs it by hand) while letting the launcher pass what Beam
needs — forwarded via MIG_BEAM_ARGS, which `pipelines.common.runner.pipeline_options`
picks up.
"""

from __future__ import annotations

import importlib
import os
import shlex
import sys

MODULES = {
    "file_processor": "pipelines.file_processor.pipeline",
    "data_enrichment": "pipelines.data_enrichment.pipeline",
    "json_producer": "pipelines.json_producer.pipeline",
}

# The arguments the pipelines define themselves. Anything else on the command line came
# from the Dataflow launcher and belongs to Beam.
PIPELINE_FLAGS = {
    "--run-id", "--run_id",
    "--record",
    "--sinks",
}


def split_args(argv: list[str]) -> tuple[list[str], list[str]]:
    """Partition argv into (pipeline args, beam args).

    Handles both `--flag=value` and `--flag value` forms, since the launcher uses the
    first and a human typically uses the second.
    """
    mine: list[str] = []
    beam: list[str] = []
    i = 0
    while i < len(argv):
        arg = argv[i]
        name = arg.split("=", 1)[0]
        target = mine if name in PIPELINE_FLAGS else beam
        target.append(arg)
        # A bare `--flag value` pair: the value belongs with its flag.
        if "=" not in arg and i + 1 < len(argv) and not argv[i + 1].startswith("--"):
            target.append(argv[i + 1])
            i += 1
        i += 1
    return mine, beam


def main() -> None:
    name = os.environ.get("MIG_MODULE", "").strip()
    if name not in MODULES:
        raise SystemExit(
            f"MIG_MODULE={name!r} is not one of {sorted(MODULES)} — the image was built "
            "without a valid --build-arg MIG_MODULE (see Dockerfile.dataflow)."
        )

    mine, beam = split_args(sys.argv[1:])

    # The launcher passes --project but does not set GOOGLE_CLOUD_PROJECT in the
    # container, and Config.from_env() has no other way to learn which project it is
    # running against — it would fall back to "mig-local" and call the real BigQuery
    # API for a project that does not exist. Promote the flag to the environment.
    for flag in beam:
        if flag.startswith("--project="):
            os.environ.setdefault("GOOGLE_CLOUD_PROJECT", flag.split("=", 1)[1])
            os.environ.setdefault("GCP_PROJECT", flag.split("=", 1)[1])
        elif flag.startswith("--region="):
            os.environ.setdefault("GCP_REGION", flag.split("=", 1)[1])

    if beam:
        # Appended to whatever the runner already computes, so an explicit flag from the
        # launcher (e.g. --template_location) wins over the module's own default.
        existing = os.environ.get("MIG_BEAM_ARGS", "")
        os.environ["MIG_BEAM_ARGS"] = (existing + " " + shlex.join(beam)).strip()

    sys.argv = [sys.argv[0], *mine]

    # Import rather than exec so the package's relative imports resolve normally.
    module = importlib.import_module(MODULES[name])
    print(
        f"dataflow_entrypoint: launching {MODULES[name]}\n"
        f"  pipeline args: {mine}\n"
        f"  beam args:     {beam}",
        flush=True,
    )
    module.main()


if __name__ == "__main__":
    main()
