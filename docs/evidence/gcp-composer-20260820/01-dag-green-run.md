# Green DAG run — streaming-buffer fix confirmed on a fresh extract

The migration DAG `mig_000001_1_migration` ran green end-to-end on Cloud Composer 2
against real GCP, on a **fresh extract under a brand-new run id** — the exact case the
streaming-buffer defect used to kill (see `02-failure-streaming-buffer.md`).

## The run

| field | value |
|---|---|
| Airflow run id | `manual__2026-08-20T20:33:28+00:00` |
| Migration run id | `initial-20260821-003500` |
| State | **success** |
| Started | 2026-08-20T20:33:29Z |
| Ended | 2026-08-20T20:48:55Z (~15 min 27 s) |
| Image tag (all 6 images) | `868d958-dirty`, `image_pull_policy=Always` |
| Runner | `MIG_RUNNER=dataflow`, `TARGET_PROFILE=real`, `BQ_TARGET=real` |

Full `dags list-runs` output in `03-dag-run-state.log`. The two runs above it
(`19:41` and `19:49`) are the same day's earlier green passes; `19:41` is the run whose
`file_processor` first exposed the defect on try 1 and then succeeded on the retry after
the rebuilt image was pulled.

## What this run proves that the earlier ones did not

The `19:41` and `19:49` runs proved the fix holds when Airflow **retries** a task or
re-runs soon after a rebuild. This run proves the clean first-shot case: a brand-new run
id whose `DELETE … WHERE run_id = …` predicate matches **zero rows**, so `file_processor`
issues no DML at all and the streaming buffer cannot reject it — even though a different
run had streamed into the same tables minutes earlier. The extract for
`initial-20260821-003500` was produced separately and the `.FLG` semaphore placed before
the DAG was triggered, so the `wait_for_extract_flg` sensor passed immediately.

## The three Beam stages as real Dataflow jobs

Each Beam stage runs as a `KubernetesPodOperator` pod that submits a Flex Template to
Dataflow (`MIG_RUNNER=dataflow`). All three completed (`Done`):

| stage | Dataflow job (name encodes MMDDHHMMSS) |
|---|---|
| file_processor | `beamapp-root-0820203408-650628-4v3yz1ku` |
| data_enrichment | `beamapp-root-0820203907-374850-ykey2b1k` |
| json_producer | `beamapp-root-0820204401-202080-knflaxdl` |

Full list in `04-dataflow-jobs.log`.

## Data landed in BigQuery — `run_ledger` balanced

```
run_id                  src_read  written  excluded  rejected  duplicates  balanced
initial-20260821-003500    76       42       10        4         20        true
```

76 = 42 written + 10 excluded + 4 rejected + 20 duplicates — every source record
accounted for, `balanced = true`. This is identical to the `19:41` retry outcome recorded
in `02-failure-streaming-buffer.md`, confirming the fix is deterministic, not a fluke of
one retry.

Per-table row counts for this run id (full output in `08-bigquery-counts.log`):

| table | dataset | rows | run column |
|---|---|---|---|
| account_src | bq_extraction | 42 | `_run_id` |
| account_curated | bq_transformation | 42 | `run_id` |
| account_enriched | bq_transformation | 42 | `run_id` |
| account_target | bq_transformation | 40 | `run_id` |
| record_lineage | bq_recon | 36 | `run_id` |
| reject_log | bq_recon | 6 | `run_id` |

`record_lineage` (36) + `reject_log` (6) = 42, the curated row count — the per-record
lineage table accounts for every record that left the extraction stage, tagged with the
door it exited through.

## Supporting evidence in this directory

- `02-failure-streaming-buffer.md` — the defect, root cause, and the count-then-delete fix
- `03-dag-run-state.log` — `dags list-runs` showing this run `success`
- `04-dataflow-jobs.log` — the three Dataflow jobs, all `Done`
- `05-local-suite.log` — 34 Python + 10 Java tests green before the GCP run
- `06-images-one-tag.md` — all six DAG images on one derived tag `868d958-dirty`
- `07-smoke-gcp.log` — `make smoke-gcp` full pass (DirectRunner against real GCS/BQ)
- `08-bigquery-counts.log` — the `run_ledger` row and per-table counts above