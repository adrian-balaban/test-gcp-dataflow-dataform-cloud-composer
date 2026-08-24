# Composer full-DAG run 1 — 2026-08-05 11:03 UTC

First run of the complete DAG on real GCP. Five of eight tasks succeeded, including three
that had **never executed before**. The run stopped at `loader_app` on a real defect,
diagnosed below and fixed in run 2.

Run id `pods-20260805-103402` · DAG run `manual__2026-08-05T11:03:23+00:00` ·
images `be8d6ef-dirty` · worker zone `europe-west1-b`.

## Task outcomes

| Task | State | Duration |
|---|---|---|
| `wait_for_extract_flg` | **success** | 3s |
| `file_processor` | **success** | 4m 18s |
| `dataform_run` | **success** | 42s |
| `data_enrichment` | **success** | 4m 31s |
| `json_producer` | **success** | 5m 50s |
| `loader_app` | **failed** (2 attempts) | — |
| `reconciliation` | `upstream_failed` | — |
| `assert_run_balanced` | `upstream_failed` | — |

## What this run proved for the first time

### The zone fix works — verified against live VMs, not config

The previous run (12:12 on 2026-08-04) died with `ZONE_RESOURCE_POOL_EXHAUSTED` in
`europe-west1-d`, and with no `--worker_zone` flag the only remedy was to retry and hope.
`MIG_WORKER_ZONE` now threads Composer env → DAG → pod env → `PipelineOptions`. Both Beam
workers landed in the pinned zone:

```
beamapp-root-0805110405-2-08050404-wlzu-harness-c7rc   europe-west1-b   RUNNING
beamapp-root-0805110922-3-08050409-mif0-harness-w0lr   europe-west1-b   RUNNING
```

Two different pipelines, same pinned zone — the knob works structurally (it lives in the
shared `pipelines/common/runner.py`), not by luck on one job.

### `dataform_run` executed for the first time ever

It required three stacked fixes, each invisible until the previous was cleared:

1. the `dataform-runner` image **had never been built or pushed** — the DAG referenced a
   tag that did not exist;
2. once added to the build, it **would not build**: the root `.dockerignore` excludes
   `dataform/` and `pipelines/`, which is the entire content of that image;
3. once built, it **would have crashed on import**: `ROOT = parents[2]` encodes the repo
   layout, and at `/app/run_dataform.py` that index is past the filesystem root
   (`IndexError` before `main()`).

### All three Beam pipelines reached Dataflow and finished

```
beamapp-root-0805110405-269976-k0j3hy17   Done   (file_processor)
beamapp-root-0805110922-357957-ntbw3mz4   Done   (data_enrichment)
beamapp-root-0805111356-234607-heh9xv9a   Done   (json_producer)
```

`file_processor` moved 402 elements through `GroupByDedupKey → Dedup → ToSrcRow →
WriteSrc` in 4 batches.

### The transformation tables filled — the objective test

All three were **empty for this run id** beforehand, because no previous run got past
`file_processor`:

| Table | Before | After |
|---|---|---|
| `account_curated` | 0 | **402** |
| `account_enriched` | 0 | **402** |
| `account_target` | 0 | **400** |

### The ledger was rewritten by the pod path

```
531 read = 402 written + 100 excluded + 4 rejected + 25 duplicates → balanced = true
created_at 2026-08-05T11:03:56Z
```

The timestamp matters: the pre-existing row came from `run_pipeline.py`. `file_processor`
does DELETE-then-INSERT on `run_id`, so this row was written by *this* run, through
Composer, independently reproducing the same balancing equation.

## Why `loader_app` failed

```
ro.mig.common.HttpObjectStore$ObjectStoreException: request failed:
  http://localhost:4443/storage/v1/b?project=mig-local
Caused by: java.net.ConnectException
```

The pod was talking to the **local fake-GCS emulator**. Two independent causes:

**1. The DAG configured nothing.** `java_app()` passed no `env_vars` and only run/window
arguments, while its sibling `beam_pipeline()` passed seven env vars. Every endpoint in
`LoaderApp`/`ReconService` defaults to a local emulator (`localhost:4443`,
`localhost:9050`, `localhost:8080`) because that is where they run under
`make run-initial`. `local/scripts/run_pipeline.py` sets all of it for the real profile;
the DAG did not.

**2. The apps could not authenticate even if pointed correctly.** `HttpObjectStore` and
`BigQueryRest` speak the JSON APIs over raw HTTP, so — unlike the Beam pods, which use
Google client libraries — they inherit nothing from Workload Identity. Nothing minted a
token: `MIG_GCS_TOKEN` was read from an env var only `run_pipeline.py` ever set. Verified
by search: no `GoogleCredentials`, no metadata-server call, no `google-auth` dependency.

So the L lane died at `LoaderApp.java:59`, `store.createBucket(...)`, **before Target System
was ever contacted**. Review finding H5 made these lanes *launchable* as pods; it never
made them *usable*, and "pods start and pull images" was mistaken for the lane working.

## Also found: the Kafka sink loses data silently

`json_producer` runs `--sinks both`, and `sinks.py` builds a `KafkaTargetWriter`
unconditionally for `both`. No Kafka cluster exists (`enable_kafka=false`, destroyed
earlier), no `KAFKA_*` env reaches the pods, so the producer targets `localhost:19092`:

```
%3|FAIL|rdkafka#producer-1| localhost:19092/bootstrap: Connect to ipv4#127.0.0.1:19092
   failed: Connection refused (149 identical error(s) suppressed)
```

**The task still passed.** `produce()` queues locally and reports only through a delivery
callback that is never invoked with an error inside the flush window, so `_errors` stays
empty and `write_batch` never raises. The symptom is latency and silence, not failure:
the two GCS batch files are timestamped 11:18:19 and 11:18:49 — exactly the `flush(30)`
timeout per batch. GCS received both batches, Kafka received nothing, and nobody was
told. For a Loader contract specified to be fed *by* Kafka, silent loss is worse than a
crash. `json_producer` spent 5m50s on 400 records, almost all of it waiting on a dead
broker.

A second, independent bug in the same area: `terraform/envs/dev/main.tf` emitted
`KAFKA_BOOTSTRAP_SERVERS` while `config.py` reads `KAFKA_BOOTSTRAP`. The names never met,
so even *with* a cluster provisioned the address would never have reached the pipeline.
Recorded as H8 "the bootstrap address resolves" — true of `terraform output`, false of
anything that consumes it. Fixed.
