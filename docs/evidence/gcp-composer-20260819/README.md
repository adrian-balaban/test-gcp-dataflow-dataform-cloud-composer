# Post-review-fix DAG run on Cloud Composer — evidence, 2026-08-19

The 08-18 run proved the DAG works. This one proves it still works **after** the two review
passes, and closes the gap the 08-19 quota failure left open
([`../gcp-composer-20260818/11-retest-20260819-blocked-by-quota.md`](../gcp-composer-20260818/11-retest-20260819-blocked-by-quota.md)).

Run `manual__2026-08-19T15:13:07+00:00` — **8 of 8 tasks succeeded**.

## What this run is the first to exercise

| Change | Why the DAG is the only place it could be tested |
|---|---|
| `run_ledger.target_written` → **`extraction_written`** | `assert_run_balanced` reads that column. The rename was verified locally and by `ALTER TABLE`, but the gate itself had never run against the new name. It passes |
| **Per-app identities** (finding 5) | `loader_app` runs as `mig-loader` → `loader-app@`, `reconciliation` as `mig-recon` → `recon-service@`. Until now every pod ran as `dataflow-worker`, so the narrow roles written for these two accounts were never the ones in force |
| **`BigQueryRest` pagination** (finding 6) | recon ran green against real BigQuery with the paging loop in place |
| **One image tag for both build scripts** | All six images published as `:rev2`; the 08-19 failure was the Java and Beam scripts deriving different tags while the DAG launches both from one variable |

## The failure it found, which is the point

Switching to the narrow identities broke `loader_app` immediately:

```
loader-app@… does not have storage.objects.delete access to the Google Cloud Storage object.
put gs://…-recon/load/…/ACCOUNT.CHS -> HTTP 403
```

A re-run overwrites the loader's own artefacts, and GCS overwrite is create + delete, so
`objectCreator` is not enough. `dataflow-worker` already had `objectAdmin` — which is exactly
why nobody noticed while every pod borrowed that identity. Fixed in `terraform/modules/iam`;
the cleared tasks then passed. Detail in `02-failure-loader-objectadmin.md`.

## Files

| File | Contents |
|---|---|
| `01-dag-task-states.log` | All 8 tasks, `airflow tasks states-for-dag-run` |
| `02-failure-loader-objectadmin.md` | The 403, why the narrow role was too tight, and the fix |
| `03-ledger-and-lineage.md` | `run_ledger` showing `extraction_written`, and the lineage breakdown |
| `04-reconciliation-report.json` | The recon pod's report: equation closes, ledger agrees with the register |
| `05-local-suite.log` | 33 Python + 10 Java tests and all 10 acceptance criteria, including the new delta check |

## Numbers

```
src_read 76 = 42 written at extraction, of which 40 migrated lane-wide
              + 10 excluded + 4 rejected + 20 duplicates
ledgerAgreement.agrees: true   (10/10 excluded, 20/20 duplicates, 6/6 rejected)
```

The 42-vs-40 difference is the two map/schema dispositions `json_producer` settles after
extraction — the reason the column is no longer called `target_written`.
