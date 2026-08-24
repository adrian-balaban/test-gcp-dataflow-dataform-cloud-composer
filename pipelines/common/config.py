"""Runtime configuration — the single place that knows local emulators from real GCP.

`TARGET_PROFILE=local` (default) points at the emulator stack; `real` uses Application
Default Credentials against actual GCP services.

**One resolved `Backend`, not three free switches.** `TARGET_PROFILE`, `BQ_TARGET` and
`MIG_RUNNER` used to be read independently — by `Config`, by `sinks.py` and by `runner.py`,
none of which consulted the others — so `TARGET_PROFILE=real` + `BQ_TARGET=emulator` +
`MIG_RUNNER=direct` was accepted and meant "real GCS, emulator BigQuery, local runner".
Nothing failed; the run simply wrote half its output somewhere nobody was looking.

Now the backend is resolved once and the incoherent combinations are rejected at startup
(`validate`). Two variations survive because they are deliberate, not accidents:

* **LOCAL + `BQ_TARGET=real`** — the free BigQuery sandbox, which is how Dataform is
  exercised against real BQ at zero cost. Emulator object store, real warehouse: odd on
  paper, but it is a documented workflow (see docs/runbook-gcp.md).
* **GCP + `MIG_RUNNER=direct`** — `make smoke-gcp` runs the pipelines in-process against
  real GCS/BigQuery via ADC. A smoke test has no business spinning up a Dataflow job.
"""

from __future__ import annotations

import enum
import os
import re
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


#: What a run id, project or dataset name is allowed to contain. Deliberately the same
#: shape as ReconService's SAFE_IDENTIFIER on the Java side.
SAFE_IDENTIFIER = re.compile(r"^[A-Za-z0-9_.-]+$")


def require_identifier(option: str, value: str) -> str:
    """Reject a run id (or dataset, or project) that is not a plain identifier.

    Every SQL statement in the pipelines interpolates the run id rather than binding it,
    because most of the statements are DELETEs whose *table* is also interpolated and
    BigQuery cannot parameterise identifiers. The Java side closed the same hole with
    `requireIdentifier` plus `@run_id` binding; this is the Python half, applied once at
    the CLI boundary so a bad value cannot reach SQL — or a GCS object path, which the
    run id also names — from any of the four entry points.
    """
    if not SAFE_IDENTIFIER.match(value or ""):
        raise ValueError(
            f"{option} must match {SAFE_IDENTIFIER.pattern} but was: {value!r}"
        )
    return value


class Backend(enum.Enum):
    """Which world the endpoints point at. Resolved once, never re-derived."""

    LOCAL = "local"
    GCP = "gcp"


class Runner(enum.Enum):
    """Which Beam runner executes the pipelines."""

    DIRECT = "direct"
    DATAFLOW = "dataflow"


class ConfigError(RuntimeError):
    """An incoherent combination of switches — raised before anything runs."""


def _env(key: str, default: str = "") -> str:
    value = os.environ.get(key, default)
    return value.strip()


def load_dotenv(path: Path | None = None) -> None:
    """Load `.env` without adding a dependency. Existing env vars always win."""
    path = path or (ROOT / ".env")
    if not path.is_file():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


@dataclass(frozen=True)
class Config:
    profile: str

    project: str
    storage_host: str
    landing_bucket: str
    json_bucket: str
    recon_bucket: str

    bq_target: str
    bq_host: str
    ds_extraction: str
    ds_transformation: str
    ds_recon: str

    kafka_bootstrap: str
    kafka_topic: str
    kafka_batch_size: int
    kafka_security_protocol: str

    target_system_url: str
    target_system_max_retries: int

    # The confirmation stream (docs/PLAN-CHANGES-22082026.md): the mock publishes one
    # event per accepted write to `target_system_confirmation_topic`, recon consumes it
    # and joins to account_target by account_key. Empty bootstrap means "no cluster"
    # — the mock skips the producer and recon skips the read, exactly as an empty
    # `kafka_bootstrap` drops the Kafka sink. This keeps a no-Kafka run (smoke-gcp
    # --sinks gcs, or any run where Managed Kafka is VPC-only and unreachable) green
    # rather than failing on zero confirmations.
    target_system_confirmation_bootstrap: str
    target_system_confirmation_topic: str

    pgp_home: str
    pgp_recipient: str

    mapping: str

    @property
    def backend(self) -> Backend:
        return Backend.LOCAL if self.profile != "real" else Backend.GCP

    @property
    def is_local(self) -> bool:
        return self.backend is Backend.LOCAL

    @property
    def bq_is_emulator(self) -> bool:
        return self.bq_target != "real"

    @property
    def runner(self) -> Runner:
        """Direct unless explicitly asked for Dataflow, or implied by the GCP backend.

        `runner.py` asks this rather than reading `MIG_RUNNER` itself, so the runner
        choice and the endpoint choice are made in one place and can be validated
        against each other.
        """
        forced = _env("MIG_RUNNER").lower()
        if forced == "direct":
            return Runner.DIRECT
        if forced == "dataflow":
            return Runner.DATAFLOW
        return Runner.DIRECT if self.backend is Backend.LOCAL else Runner.DATAFLOW

    def validate(self) -> "Config":
        """Reject the combinations that cannot mean anything sensible.

        Called by `from_env`, so a misconfiguration is a startup error rather than a run
        that quietly writes to the wrong world.
        """
        if self.backend is Backend.GCP and self.bq_is_emulator:
            raise ConfigError(
                "TARGET_PROFILE=real with BQ_TARGET=emulator: real GCS with an emulator "
                "BigQuery. The extraction rows would land in a throwaway warehouse while "
                "the artefacts went to real buckets, and reconciliation would compare the "
                "two. Set BQ_TARGET=real, or run the whole thing locally."
            )
        if self.backend is Backend.LOCAL and self.runner is Runner.DATAFLOW:
            raise ConfigError(
                "TARGET_PROFILE=local with MIG_RUNNER=dataflow: Dataflow workers run in "
                "Google's network and cannot reach the emulators on localhost. Use "
                "MIG_RUNNER=direct locally, or TARGET_PROFILE=real to run on Dataflow."
            )
        return self

    @property
    def mapping_path(self) -> Path:
        return ROOT / self.mapping

    @classmethod
    def from_env(cls) -> "Config":
        load_dotenv()

        # On a laptop every one of these comes from .env. Inside a Dataflow Flex Template
        # container there is no .env — only TARGET_PROFILE=real baked into the image and
        # whatever the launch environment provides — so the local defaults below would
        # send the pipeline at project "mig-local" and buckets that do not exist.
        #
        # GOOGLE_CLOUD_PROJECT is set by the Dataflow launcher; deriving the project and
        # the conventional bucket names from it means a real run needs no extra wiring,
        # while a laptop with a .env is completely unaffected.
        profile = _env("TARGET_PROFILE", "local")
        project = _env("GCP_PROJECT") or _env("GOOGLE_CLOUD_PROJECT") or "mig-local"

        # Bucket names follow the project only on the real profile *and* only when the
        # project is genuinely a GCP one. Keeping the "mig-" prefix for the local
        # emulator profile means a laptop with a .env behaves exactly as before.
        real = profile != "local" and project != "mig-local"
        prefix = f"{project}-" if real else "mig-"

        return cls(
            profile=_env("TARGET_PROFILE", "local"),
            project=project,
            storage_host=_env("STORAGE_EMULATOR_HOST", "http://localhost:4443"),
            landing_bucket=_env("GCS_LANDING_BUCKET", f"{prefix}landing"),
            json_bucket=_env("GCS_JSON_BUCKET", f"{prefix}json-out"),
            recon_bucket=_env("GCS_RECON_BUCKET", f"{prefix}recon"),
            bq_target=_env("BQ_TARGET", "emulator"),
            bq_host=_env("BIGQUERY_EMULATOR_HOST", "http://localhost:9050"),
            ds_extraction=_env("BQ_DATASET_EXTRACTION", "bq_extraction"),
            ds_transformation=_env("BQ_DATASET_TRANSFORMATION", "bq_transformation"),
            ds_recon=_env("BQ_DATASET_RECON", "bq_recon"),
            # No localhost default off the local profile. Inside a Dataflow worker
            # localhost:19092 is nothing at all, and defaulting to it turned "Kafka was
            # never configured" into "Kafka is configured and unreachable" — which the
            # sink then spent 30s per batch failing to notice. Empty means "no cluster",
            # and sinks.target_writer drops the Kafka writer rather than inventing one.
            kafka_bootstrap=_env(
                "KAFKA_BOOTSTRAP", "localhost:19092" if profile == "local" else ""
            ),
            kafka_topic=_env("KAFKA_TOPIC", "target-system-target"),
            kafka_batch_size=int(_env("KAFKA_BATCH_SIZE", "200")),
            kafka_security_protocol=_env("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT"),
            target_system_url=_env("TARGET_SYSTEM_URL", "http://localhost:8080"),
            target_system_max_retries=int(_env("TARGET_SYSTEM_MAX_RETRIES", "5")),
            # Same localhost-when-local / empty-otherwise pattern as kafka_bootstrap:
            # locally redpanda is always up at 19092, on a Dataflow worker or the smoke-gcp
            # laptop localhost:19092 is nothing at all, and empty means "no confirmation
            # stream" rather than "confirmation stream configured and unreachable".
            target_system_confirmation_bootstrap=_env(
                "TARGET_SYSTEM_CONFIRMATION_BOOTSTRAP",
                "localhost:19092" if profile == "local" else "",
            ),
            target_system_confirmation_topic=_env(
                "TARGET_SYSTEM_CONFIRMATION_TOPIC", "target-system-confirmations"
            ),
            pgp_home=_env("PGP_HOME", "local/keys"),
            pgp_recipient=_env("PGP_RECIPIENT", "mig-prototype@example.invalid"),
            mapping=_env("MAPPING", "contracts/mappings/mapping-project1.yaml"),
        ).validate()
