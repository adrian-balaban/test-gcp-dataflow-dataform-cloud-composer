# Green DAG run after the two-door simplification (D1–D6)

`docs/PLAN-CHANGES-21082026.md` landed in this session: two dispositions
(`WRITTEN`/`REJECTED`), one full snapshot per run (no initial/delta, no window),
homogeneous TDS definitions, `account_key` renamed from `dedup_key`. Full re-provision
from a torn-down project (`terraform apply` bootstrap → phase-1 → images → Composer),
then the migration DAG run green end-to-end on the rebuilt infrastructure.

## The run

| field | value |
|---|---|
| Airflow run id | `manual__2026-08-21T19:36:24+00:00` |
| Migration run id | `mig-composer-run-20260821-203610` |
| State | **success**, all 8 tasks |
| Started | 2026-08-21T19:36:24Z |
| Ended | 2026-08-21T20:18:53Z (~42 min, mostly `file_processor` cold Dataflow-pod start) |
| Image tag (all 6 images) | `e2ebe53-dirty`, `image_pull_policy=Always` |
| Runner | `MIG_RUNNER=dataflow`, `TARGET_PROFILE=real`, `BQ_TARGET=real` |

Full task states in `01-dag-run-state.log`.

## The three Beam stages as real Dataflow jobs

| stage | Dataflow job |
|---|---|
| file_processor | `beamapp-root-0821194235-293847-aqppercf` |
| data_enrichment | `beamapp-root-0821200901-550407-onv881vz` |
| json_producer | `beamapp-root-0821201326-009307-0p0tixm6` |

All `Done`. Full list in `02-dataflow-jobs.log`.

## `run_ledger` — two columns, not five

```json
{"run_id": "mig-composer-run-20260821-203610", "src_read": 81,
 "extraction_written": 78, "rejected": 3, "balanced": true}
```

No `window_from`/`window_to`/`excluded`/`duplicates` columns — the schema itself proves
D5 and D1 landed (`03-run-ledger.json`). `extraction_written` (78) is the parse-stage
count; two more records fail map/schema downstream, so the lane-wide equation is
`81 = 76 written + 5 rejected` (3 parse + 2 map/schema), confirmed by `make verify`
below and by `04-bigquery-counts.log`.

## `make verify` — all 8 acceptance criteria, against this real run

```
✓ 1. balancing equation closes (whole lane): 81 = 76 migrated + 5 not migrated
✓ 2. seeded malformed records rejected with correct reason codes: 5 reason codes, all matching
✓ 3. every TARGET document validates against the JSON Schema: 76/76 documents valid
✓ 4. TARGET emitted in 200-element batches: 1 batches of 200 (1 GCS files; Kafka assertions
     skipped — Managed Kafka not provisioned, enable_kafka=false)
✓ 5. every key appears exactly once in TARGET: 76 keys, each present exactly once, 0 orphans
✓ 6. every not-migrated record is named in record_lineage: 5 not-migrated records named
✓ 7. all five artefact types present in both lanes
✓ 8. .CHS checksums verify on both sides

VERIFY PASSED — all 8 acceptance criteria hold.
```

8 criteria, not 10 — D7 (excluded-exact and delta-run criteria dropped).

## Local suite green before and after

32 Python tests (`pytest tests/`) and 10 Java tests (`mvn test`) green — `05-local-suite.log`.

## What this run proves

- The engine change (D1, D2, D6) round-trips through the whole lane on real GCP: parse
  → map → schema, two doors, no filter/dedup stage, homogeneous TDS (pipe-delimited
  with header, verified via the CSV smoke path locally).
- One full snapshot per run (D5): the DAG is parameterised by `run_id` alone.
- Idempotency (D3) unaffected: count-then-DELETE still gates re-running a run id.
- The extend-not-rewrite claim (project2) unaffected by the simplification — both
  project smokes pass with the new homogeneous, header-aware, two-door engine.

## Supporting evidence in this directory

- `01-dag-run-state.log` — `dags list-runs` showing this run `success`
- `02-dataflow-jobs.log` — the three Dataflow jobs, all `Done`
- `03-run-ledger.json` — the two-column ledger row (no window/excluded/duplicates)
- `04-bigquery-counts.log` — per-table row counts for this run id
- `05-local-suite.log` — 32 Python + 10 Java tests green
