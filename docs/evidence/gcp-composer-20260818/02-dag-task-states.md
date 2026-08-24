# DAG task states — three runs, captured with `airflow tasks states-for-dag-run`

All three ran the same DAG (`mig_000001_1_migration`) against the same extract
(`initial-20260818-165241`) on Cloud Composer 2. The first two are kept because the
failures are the finding: each exposed a defect that no local or `smoke-gcp` run could.

## Run 1 — `manual__2026-08-18T19:36:44+00:00`: loader_app failed

```
task_id              | state
=====================+=================
wait_for_extract_flg | success
file_processor       | success   (Dataflow job, 5m14s)
dataform_run         | success
data_enrichment      | success
json_producer        | success
loader_app           | failed    <-- storage.buckets.create denied (see 06-)
reconciliation       | upstream_failed
assert_run_balanced  | upstream_failed
```

The five pod tasks starting is itself the proof that the Composer RBAC applied: without
the Role/RoleBinding every one fails at submission with
`pods is forbidden: ... cannot list resource "pods"`.

## Run 2 — `manual__2026-08-18T20:07:03+00:00`: gate failed

```
task_id              | state
=====================+=================
wait_for_extract_flg | success
file_processor       | success
dataform_run         | success
data_enrichment      | success
json_producer        | success
loader_app           | success   <-- the bucket fix worked
reconciliation       | success
assert_run_balanced  | failed    <-- composer-runner cannot query run_ledger (see 07-)
```

## Run 3 — `manual__2026-08-18T21:49:29+00:00`: all green

```
task_id              | state   | start                | end
=====================+=========+======================+=====================
wait_for_extract_flg | success | 21:49:39             | 21:49:40
file_processor       | success | 21:49:43             | 21:54:18
dataform_run         | success | 21:54:21             | 21:54:49
data_enrichment      | success | 21:54:52             | 21:59:48
json_producer        | success | 21:59:50             | 22:04:20
loader_app           | success | 22:04:23             | 22:04:53
reconciliation       | success | 22:04:56             | 22:05:13
assert_run_balanced  | success | 22:05:16             | 22:05:21
```

Eight of eight. The three Beam tasks each launched a real Dataflow job (see
`03-dataflow-jobs.log`); the other five ran as `KubernetesPodOperator` pods on Composer's
own GKE cluster using the `mig-pipeline` Kubernetes service account.
