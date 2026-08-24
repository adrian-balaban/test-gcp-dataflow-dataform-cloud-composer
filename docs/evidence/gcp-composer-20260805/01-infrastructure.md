# Real-GCP Composer run — evidence, 2026-08-05

## Infrastructure actually created
  composer: mig-composer	RUNNING
  kafka: mig-kafka	ACTIVE

## H8 — KAFKA_BOOTSTRAP_SERVERS now resolves (was empty)
bootstrap.mig-kafka.europe-west1.managedkafka.mig-000001-1-dev.cloud.goog:9092

## H6 — composer-runner may impersonate dataflow-worker
roles/iam.serviceAccountTokenCreator;roles/iam.serviceAccountUser

## M13 — images pinned to the git SHA, spec references the pinned tag
  europe-west1-docker.pkg.dev/mig-000001-1-dev/mig-dataflow/file-processor:c39570e

## DAG task states (first triggered run)
Composer 2.9.7 / Airflow 2.9.3, `mig-composer`, europe-west1.

- `wait_for_extract_flg` — **success** (GCS sensor found the `.FLG` semaphore)
- `file_processor` — **failed**, see the defect chain below
- everything downstream — never reached

## What this run proved

| Finding | Verified how |
|---|---|
| **H6** impersonation | The DAG (as `composer-runner`) *submitted* Dataflow jobs. Submission is precisely what failed before the grant. |
| **H8** Kafka bootstrap | `terraform output` emits a real address; was empty before. |
| **H5** L+R lanes | Images built and pushed; DAG resolves them as `KubernetesPodOperator`. **Not executed** — the run never got past `file_processor`. |
| **M13** image pinning | Spec JSON references `file-processor:c39570e`, not `:latest`. |
| **H7** Dataform | Unlinked CLI path deployed and compiled clean. |

## Five Flex Template defects found by running it

None of these were visible from reading the code, and none could have been caught
locally — nothing local runs the Dataflow launcher. Each was found from the launcher's
`console_logs` in GCS, not from the job-level error, which only ever said
"Timeout in polling result file".

1. **Wrong base image.** Built `FROM apache/beam_python3.11_sdk`, which has the worker
   harness but not `/opt/google/dataflow/python_template_launcher`. Every launch died
   with `exec: no such file or directory`. Fixed: `dataflow-templates-base/python311`.
2. **ENTRYPOINT is ignored by Flex Templates.** They are configured by
   `FLEX_TEMPLATE_PYTHON_*` env vars. The old ENTRYPOINT silently did nothing.
3. **Launcher flags collided with the pipelines' argparse.** The launcher appends
   `--runner`, `--project`, `--template_location`, … and all three pipelines use
   `parse_args()`, which errors on unknown flags → `exit status 2`. Fixed by splitting
   argv in `dataflow_entrypoint.py` rather than loosening the CLIs to
   `parse_known_args()`, which would let a typo'd flag pass silently.
4. **Re-installing dependencies at launch.** `FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE`
   made the sandbox VM `pip install apache-beam[gcp]` on every launch, blowing the
   polling deadline (13 minutes QUEUED, then dead). The deps are already in the image.
5. **Emulator config inside the container.** No `.env` there, so `Config.from_env()`
   resolved `TARGET_PROFILE=local`, `localhost:9050`, project `mig-local` — and called
   the *real* BigQuery API for a project that does not exist. Fixed by baking
   `TARGET_PROFILE=real` into the image and promoting the launcher's `--project`.

## The one remaining defect (not fixed)

With `--template_location`, Beam **stages** the job rather than running it, so
`pipeline.run()` returns no live job. `file_processor` then calls
`result.wait_until_finish()` and `_read_counters(result)` unconditionally
(`pipeline.py:392-395`) and dies with:

```
ValueError: Can not query metrics. Job id is unknown.
```

This is a genuine design gap, not a config error: **the pipelines assume they own the
job lifecycle**, which is true under `run_pipeline.py` and false under a Flex Template.
The metrics read, the `require_balance` call and the `run_ledger` write all have to move
out of the launch path — either into a separate DAG task that queries the finished job,
or behind a check for template-construction mode.

**Resolved 2026-08-05 by option C** — see `02-pods-run-1101.md`. Rather than redesign
three pipelines, the DAG now runs them as `KubernetesPodOperator` pods with
`MIG_RUNNER=dataflow`. The pod owns the Dataflow job lifecycle, so `wait_until_finish()`
and the metrics read work exactly as written, with **zero pipeline changes**.

That also sidesteps a second instance of the same assumption, which the metrics error was
masking: `data_enrichment` and `json_producer` call `bq.query(...)` at *construction*
time and feed the rows into `beam.Create`. Under a Flex Template the launcher would embed
every row into the job graph as a literal — survivable at 526 records, fatal at 1.7M.

The Flex Template path is kept (images and specs are still built and published). It
becomes viable once the pipelines stop reading metrics off their own result and read
through `ReadFromBigQuery` instead of `beam.Create` — a recorded future option.

## Honest status

The **data path on real GCP was already proven** by an earlier run through
`run_pipeline.py` (see `docs/evidence/gcp-run-20260804/`) — 40 documents, all 8
acceptance criteria green. What remains unproven is the **Composer orchestration path**
end to end, and the two `KubernetesPodOperator` tasks in particular have never executed.
