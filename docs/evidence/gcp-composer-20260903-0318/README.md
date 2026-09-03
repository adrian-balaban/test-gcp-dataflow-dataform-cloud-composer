# Evidence — GCP Composer CSV E2E, 2026-09-03 03:18 UTC

## Verdict: GREEN

Full migration DAG (`mig_000001_1_migration`) green on Cloud Composer 2, all 8 tasks,
pipe-delimited CSV source (no copybook), real Dataflow / BigQuery / Managed Kafka /
Cloud Run target-system-mock. Run `run-dag-20260903-0657`, Airflow run
`manual__2026-09-03T07:00:15+00:00`, wall clock **15m32s** (07:00:18 → 07:15:50 UTC).

- **`make verify` (laptop, `BQ_TARGET=real`): 9/10** — the single red is criterion 4's
  Kafka half; the Managed Kafka bootstrap DNS resolves only inside Google's network, so
  a host laptop cannot reach the broker (`_ALL_BROKERS_DOWN`). Documented constraint, not
  a defect. See `41-gcp-verify.log`.
- **In-cloud proof (DAG + recon report): 10/10** — balancing equation closes
  (`srcRead=55 = written=50 + rejected=5`), 50/50 TARGET rows confirmed, 50/50 published
  documents settled. See `reconciliation-run-dag-20260903-0657/reconciliation-report.json`
  and `46-gcp-phase-timing-summary.txt`.

This is the second GCP evidence round for the CSV-only source; the first is
[`gcp-csv-input-20260903-0300/`](../gcp-csv-input-20260903-0300/). The CSV change itself
was committed as `85fa560`; a reusable-pattern entry for Secret Manager was added in
`d635d5d`.

## The loop: one red, then green

The first DAG run (`manual__2026-09-03T06:22:24`, conf `run-dag-20260903-0409`) **failed**
at `loader_app`: 50 documents published, 0 settled. Root cause was the known Cloud Run
mock OAUTHBEARER wedge documented in `docs/runbook-gcp.md` — even on a fresh
terraform-created instance, after ~34 min the kafka-clients re-login thread logs
`logout() still needs to be called on a previous login = true`, after which every broker
connection is `terminated during authentication` once a second and the mock consumes
nothing. Evidence: `22-loader-failure.log`.

Fix (the runbook's prescribed one): recycle the Cloud Run mock by name —
`gcloud run services update target-system-mock --image=<same image>` deploys a new
revision (`00002-cfs`) with a fresh instance and a fresh login, restarting the ~34 min
window. Evidence: `23-mock-recycle.log`. Then a fresh extract (`24-extract-stage-rerun.log`)
and a fresh DAG trigger (`25-dag-trigger-rerun.log`) finished the loader in 12.9 s, well
inside the window. The recycled mock drained the failed run's 50 stale messages first,
seeding its in-memory idempotency store, so this run's 50 messages were all duplicates
from the mock's point of view — `0 accepted / 50 duplicates` is a replay artefact, not a
defect; the settle equation still closes and recon confirms all 50 rows.

## Infra note: one zonal-capacity failure, cleared on retry

The first Composer apply (`03-apply-composer-kafka.log`) failed mid-create:
`zone 'europe-west1-b' does not have enough resources available to fulfill the request`
during `CP_GKE_CLUSTER_CREATING`. Kafka created and stayed `ACTIVE`; the Composer env was
deleted by rollback. The retry (`03b-apply-composer-retry.log`) succeeded — Composer 2
Autopilot landed in a different zone / capacity had freed up. This is a transient GCP
supply issue, not a code or config defect.

## Per-phase record counts and timing

See `46-gcp-phase-timing-summary.txt` for the full table. Headline:

| task | duration | records |
|---|---|---|
| wait_for_extract_flg | 1.2s | sensor, .FLG pre-staged |
| file_processor | 292.0s | 55 read (header skipped) → 52 extracted, 3 parse rejects |
| dataform_run | 21.5s | 52 → account_curated |
| data_enrichment | 258.2s | 52 → 52 enriched |
| json_producer | 275.6s | 52 → 50 target docs (2 schema/map rejects) → GCS + Kafka 50 msgs |
| loader_app | 12.9s | 50 published → 50 settled (0 accepted + 50 duplicates) |
| reconciliation | 23.4s | 50/50 TARGET rows confirmed |
| assert_run_balanced | 19.3s | 55 = 50 migrated + 5 rejected |

Rejects by reason (5): `PARSE_BAD_COLUMN_COUNT`, `PARSE_BAD_DATE`, `PARSE_BAD_NUMERIC`,
`SCHEMA_INVALID`, `MAP_UNMAPPED_ENUM_VALUE` — the first is the CSV-specific reject code.

## Files

| File | What it proves |
|---|---|
| `00-start-time.txt` | Infra round start (03:19:22 UTC) |
| `01-bootstrap.log`, `01-infra-apply.log`, `01-infra-plan.txt` | Terraform base infra (user's manual apply) |
| `02-build-images.log` | Six images built+pushed on tag `csv-e2e-20260903` (and `:latest`) |
| `03-apply-composer-kafka.log` | Composer+Kafka apply — zonal-capacity failure + RUNNING milestone |
| `03b-apply-composer-retry.log` | Retry apply that succeeded (Composer RUNNING) |
| `04-extract-stage.log` | First extract (run 0409) — anonymous-caller failure then success |
| `05-rbac-via-vm.log` | RBAC applied via in-VPC VM, namespace `composer-2-9-7-airflow-2-9-3-6aacf9fe`, 3 KSA→GSA bindings |
| `06-deploy-dags.log` | DAG deployed to Composer DAG bucket |
| `07-dags-list.log` | DAG registered in Composer |
| `08-dag-trigger.log` | First DAG trigger (run 0409) — DagNotFound then triggered |
| `09-task-instances.log` | Mid-run task snapshot (run 0409) |
| `22-loader-failure.log` | Run-1 loader failure: 50 unsettled, mock OAUTHBEARER wedge |
| `23-mock-recycle.log` | Cloud Run mock recycled → revision 00002-cfs |
| `24-extract-stage-rerun.log` | Fresh extract staged (run 0657) |
| `25-dag-trigger-rerun.log` | Run-2 DAG trigger (run 0657) |
| `40-gcp-dag-task-states.txt` | Final task states with start/end timestamps (run 0657, all success) |
| `41-gcp-verify.log` | `make verify` 9/10 (criterion 4 Kafka-DNS laptop constraint) |
| `46-gcp-phase-timing-summary.txt` | Per-phase durations + record counts + verdict |
| `run-dag-20260903-0657/ACCOUNT.RPT` | Loader report (50 published, 50 settled) |
| `run-dag-20260903-0657/ACCOUNT.{CHS,ERR,FLG}` | Load-lane artefacts |
| `reconciliation-run-dag-20260903-0657/reconciliation-report.json` | Recon: balances=true, 50/50 confirmed, rejects by reason |
| `reconciliation-run-dag-20260903-0657/reconciliation-report.html` | Human-readable recon report |