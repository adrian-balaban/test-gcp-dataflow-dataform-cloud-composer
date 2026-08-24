# Composer pod run — 2026-08-05 12:12 (Europe/Bucharest)

The run that got furthest. Every code and configuration defect is resolved; the run
stopped on a **GCP capacity shortage**, not on anything in this repo.

## How far it got

```
wait_for_extract_flg  success
file_processor        running  ← for ~9 minutes, then failed on zone capacity
```

The `file_processor` pod:

1. was **created** by the Airflow worker (RBAC works)
2. pulled and ran the **pinned image** `file-processor:03f2169` (M13 pinning works)
3. authenticated as **dataflow-worker** via Workload Identity
4. **read BigQuery successfully** — no more 403 on `bq_extraction`
5. **submitted a Dataflow job** — no more "Could not create workflow"
6. the job **started**, and Dataflow then could not obtain a worker VM:

```
JOB_MESSAGE_ERROR: Startup of the worker pool in europe-west1 failed to bring up
any of the desired 1 workers. ... ZONE_RESOURCE_POOL_EXHAUSTED: Instance
'beamapp-root-0805091628-...' creation failed: The zone
'projects/mig-000001-1-dev/zones/europe-west1-d' does not have enough resources
available to fulfill the request. Try a different zone, or try again later.
```

That is Google having no spare capacity in that zone at that moment. Retrying, or
pinning `--worker_zone` to a different zone in the region, is the whole remedy.

## The defect chain, in the order it was found

Each of these was invisible until the previous one was fixed. None could have been
caught locally — nothing local runs Composer, GKE RBAC, or Workload Identity.

| # | Symptom | Cause | Fix |
|---|---|---|---|
| 1 | `pods is forbidden ... cannot list resource "pods"` | Composer 2 grants its Airflow workers no pod RBAC | `Role` + `RoleBinding` (`modules/composer_rbac`) |
| 2 | Same 403, now naming `default` | The namespace came from an env var Composer silently dropped | DAG reads `/var/run/secrets/.../namespace` — the file K8s mounts into every pod |
| 3 | `403 Access Denied: Dataset bq_extraction` | Pod had RBAC but no GCP identity — ran as the node's default SA | Workload Identity: KSA `mig-pipeline` → `dataflow-worker` |
| 4 | `Could not create workflow; user does not have write access` | `dataflow.worker` executes jobs; it cannot *create* them | `roles/dataflow.developer` + `actAs` on itself |
| 5 | `ModuleNotFoundError: No module named 'pipelines'` | Workers run a stock Beam image with none of this repo in it | `--sdk_container_image` = the same pinned image the pod runs |
| 6 | `ZONE_RESOURCE_POOL_EXHAUSTED` | **Not a defect** — no GCP capacity in `europe-west1-d` | retry, or pin `--worker_zone` |

## What is now proven

- **H5 — the `KubernetesPodOperator` path works.** Pods are created, scheduled, pull the
  right image, hold a real GCP identity, and reach Dataflow. This was the finding that
  had never been executed at all.
- **H6** — `composer-runner` impersonation, exercised by every submission.
- **H8** — the Kafka bootstrap address resolves.
- **M13** — image pinning holds end to end: the tag the DAG launches is the tag the
  Dataflow *workers* run, because `--sdk_container_image` carries it through.

## What is still unproven

The **downstream tasks**: `dataform_*`, `data_enrichment`, `json_producer`, `loader_app`,
`reconciliation`, `assert_run_balanced` have not run, because `file_processor` never
completed. The remaining gap is a capacity retry, not a fix.

The data path itself remains proven end to end on real GCP via `run_pipeline.py`
(`docs/evidence/gcp-run-20260804/` — all 8 acceptance criteria green).
