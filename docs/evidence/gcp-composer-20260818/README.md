# Full DAG on Cloud Composer — evidence, 2026-08-18

**What this proves:** the migration DAG runs end to end on Cloud Composer 2, with the three
Beam pipelines executing as **real Dataflow jobs** launched from `KubernetesPodOperator`
pods, and the correctness gate passing. Previous GCP evidence
([`gcp-run-20260804/`](../gcp-run-20260804/)) exercised the *data path* against real
GCS/BigQuery but ran the pipelines in-process on DirectRunner; the execution substrate —
Dataflow, Composer, Kafka — was untested until this run.

The environment had been destroyed at the end of the 2026-08-05 session, **including the
Terraform state bucket**, so this was a bootstrap from an empty project.

## Result

Run `manual__2026-08-18T21:49:29+00:00` — **8 of 8 tasks succeeded**.

```
src_read 76 = 42 migrated + 34 not migrated (10 excluded, 4 rejected, 20 duplicates), balanced
36 record_lineage rows name every not-migrated record
```

The 36-vs-34 difference is not a discrepancy: `run_ledger.rejected` counts only the file
processor's door, while `json_producer` settles two more at the map/schema stage. The
equation closes across the lane, not inside one process — which is why reconciliation
compares the ledger against the register rather than trusting either alone
(`ledgerAgreement.agrees: true` in `05-reconciliation-report.json`).

## Files

| File | What it holds |
|---|---|
| `01-infrastructure.md` | Bootstrap + base apply, Composer/Kafka apply, the Cloud Run stand-in, the Terraform-declared environment variables |
| `02-dag-task-states.md` | Task states for all three DAG runs — the two failures are kept, because they are the finding |
| `03-dataflow-jobs.log` | The Dataflow jobs the pods submitted: proof the pipelines ran on the managed runner |
| `04-ledger-and-lineage.md` | `run_ledger` and `record_lineage` straight out of BigQuery, including named rows |
| `05-reconciliation-report.json` | The report the recon pod wrote to GCS, with the ledger-vs-register agreement block |
| `06-failure-1-buckets-create.log` | `storage.buckets.create` denied — and its fix |
| `07-failure-2-assert-run-balanced.log` | The gate could not read `run_ledger` — and its fix |
| `08-failure-3-target-system-url-reverted.log` | A hand-set env var reverted by Terraform — and its fix |
| `09-local-suite.log` | `make test` and all 9 acceptance criteria, locally, after every change |
| `10-teardown.log` | Composer and Kafka destroyed, verified empty |
| `11-retest-20260819-blocked-by-quota.md` | The post-review-fix retest, and why it could not complete |

## Why the failures are kept

All three passed the local stack *and* `make smoke-gcp`, and failed on the DAG. The reason
is identity: `smoke-gcp` authenticates as the human operator, whose rights are broad, while
the DAG's pods authenticate as the least-privilege `dataflow-worker` service account through
Workload Identity, and the gate task runs as `composer-runner`. **A green `smoke-gcp` is
necessary and not sufficient** — anything touching IAM, quotas or the distributed runner
needs a DAG run before it can be called tested.

Two of the three were also *delayed*: the reverted environment variable broke a run three
DAG runs after the apply that caused it. That is the argument for the DAG's configuration
living in Terraform rather than in a `gcloud composer environments update`.

## Reproducing

Composer's GKE control plane is private-endpoint-enforced with no authorized networks, so
the RBAC phase cannot be applied from a laptop — see
[`local/scripts/gcp/composer_rbac_via_vm.sh`](../../../local/scripts/gcp/composer_rbac_via_vm.sh)
and the runbook section it is described in. Namespace for this environment was
`composer-2-9-7-airflow-2-9-3-3bd36512`.
