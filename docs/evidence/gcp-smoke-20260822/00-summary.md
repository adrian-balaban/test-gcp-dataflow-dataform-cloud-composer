# Evidence — 2026-08-22 smoke-gcp re-verification at HEAD af96295

## Why this bundle exists

The 2026-08-20 DAG green run (`evidence/gcp-composer-20260820/`) was on the **pre-two-door**
code. Since then the engine was simplified to two dispositions (WRITTEN/REJECTED), one full
snapshot per run, and a homogeneous TDS, and "Vault Core" was renamed to "Target System"
across docs, code and evidence (commits `a517b27`, `204b671`, `1dd1cb5`, `af96295`). This
bundle records that the **current HEAD** is green end-to-end against real GCP, so the
rename + two-door change is verified, not just compiled.

## What ran

`make smoke-gcp`'s host variant — `run_pipeline.py --profile real --accounts 50 --sinks gcs`
on the operator laptop, DirectRunner in-process against real GCS + BigQuery via ADC. The DAG
path (least-priv SA pods, DataflowRunner Flex Templates) was already proven at tag `868d958`
on 2026-08-20; the two-door + rename commits touch engine logic and naming only, not
SA/pod-spec/IAM/Flex-template code, so the smoke path covers the changed code and the DAG
path needs no re-proof for a rename.

- **run id:** `run-20260822-150834`
- **accounts:** 50, layout `copybook` (from `contracts/mappings/mapping-project1.yaml`)
- **sinks:** `gcs` — Managed Kafka is VPC-only and unreachable from the laptop, so a `both`
  run would record `last_run_sinks=both` and criterion 4 would try an empty Kafka bootstrap.
  `gcs` records the truth and the Kafka assertion is skipped (see `tests/acceptance.py`).
- **HEAD:** `af96295` (typo fix, on top of the rename + two-door commits).

## Result — 8/8 acceptance criteria green

`make verify` against the live state (`local/state/last_run_id`,
`local/state/last_run_sinks=gcs`, harness manifest on host):

```
verify — run run-20260822-150834
  ✓ 1. balancing equation closes (whole lane): 55 = 50 migrated + 5 not migrated
  ✓ 2. seeded malformed records rejected with correct reason codes: 5 reason codes, all matching
  ✓ 3. every TARGET document validates against the JSON Schema: 50/50 documents valid
  ✓ 4. TARGET emitted in 200-element batches (Kafka skipped — run used sinks=gcs)
  ✓ 5. every key appears exactly once in TARGET: 50 keys, each present exactly once, 0 orphans
  ✓ 6. every not-migrated record is named in record_lineage: 5 not-migrated records named
  ✓ 7. all five artefact types present in both lanes: extraction 2 + 3, load 4; .FLG vouches for 4
  ✓ 8. .CHS checksums verify on both sides: extraction 1 entries parsed, load 1 files re-hashed
VERIFY PASSED — all 8 acceptance criteria hold for run run-20260822-150834.
```

The two-door arithmetic is visible in criterion 1: `55 = 50 migrated + 5 not migrated`
(50 WRITTEN, 5 REJECTED — the rejected records are the 5 seeded malformed ones, named in
`record_lineage` per criterion 6).

## Finding — the DAG trigger needs an explicit `--conf` run id

While re-confirming the DAG path I triggered it bare and discovered a runbook gap, now
documented in `docs/runbook-gcp.md` (*Triggering the DAG — the run id is a `--conf`
argument, not the Airflow run id*):

- The DAG is **sensor-first**: its first task is a `GCSObjectExistenceSensor` on
  `extraction/{RUN_ID}/ACCOUNT.FLG`. It does not run the harness/extractor; the extract must
  be pre-staged under the same run id.
- `RUN_ID = "{{ dag_run.conf.get('run_id', run_id) }}"` defaults to the Airflow run id.
  A bare `dags trigger` (no `--conf`) makes that `manual__2026-08-22T14:11:17+00:00`,
  which contains `:` and `+` and fails `SAFE_IDENTIFIER = ^[A-Za-z0-9_.-]+$` enforced by
  every pipeline module on `--run-id`. Every pod raises `ValueError` before reading a
  record, and `max_active_runs=1` blocks re-triggering until the sensor times out (12h).
- Fix: trigger with `--conf '{"run_id":"<safe-id>"}'` and pre-stage the extract first.

This is a documentation gap, not a code defect — `run_pipeline.py` already generates a safe
`run-{stamp}` id via `require_identifier`, which is why `make smoke-gcp` never hits it. No
code change was needed for the test loop to go green.