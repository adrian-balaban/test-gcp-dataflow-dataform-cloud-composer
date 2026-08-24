# GCP run evidence — 2026-08-04

Real-infrastructure run of the full E → T+R → L chain in project `mig-000001-1-dev`
(`europe-west1`), with **Cloud Composer 2 and Managed Service for Apache Kafka both
provisioned**. Java apps built with **Maven on JDK 25**.

**Result: VERIFY PASSED — all 8 acceptance criteria hold for run `initial-20260804-163113`.**

---

## What was deployed

| Resource | Identity | State |
|---|---|---|
| Cloud Composer 2 | `mig-composer` (`composer-2.9.7-airflow-2.9.3`) | RUNNING — created in 17m51s |
| Managed Kafka cluster | `mig-kafka` | ACTIVE — created in 11m9s |
| Kafka topic | `target-system-target` | 12 partitions, RF 3, 7-day retention |
| Dataflow Flex Templates | `file-processor`, `data-enrichment`, `json-producer` | 3 images in Artifact Registry + 3 spec JSONs in GCS |
| Migration DAG | `mig_000001_1.py` | synced to `gs://europe-west1-mig-composer-8bef5f9c-bucket/dags/` |
| Secrets | `pgp-private-key`, `pgp-passphrase`, `target-system-credentials` | versions seeded |
| BigQuery datasets | `bq_extraction`, `bq_transformation`, `bq_recon` | present, populated |
| Dataform models | `account_src` (declaration), `account_curated` | compile clean, executed against real BQ |

Terraform: `Apply complete! Resources: 5 added, 0 changed, 0 destroyed.` — exactly the
Kafka + Composer resources; the pre-existing 63-resource base stack was untouched.

## The run

Command (`--sinks gcs`, see the Kafka caveat below):

```
python local/scripts/run_pipeline.py --mode initial --profile real --accounts 50 --sinks gcs
```

### Data trail, stage by stage (all on real GCP)

| Stage | Evidence | Numbers |
|---|---|---|
| Harness | seeded manifest | 76 records: 40 valid, 10 excluded, 6 malformed, 20 duplicates |
| E — Extractor | `gs://…-landing/extraction/<run>/` | `ACCOUNT.tar.gz.pgp` + `ACCOUNT.FLG` (semaphore last) |
| T — File Processor | `run_ledger`, `account_src` | 76 read → 42 written, 10 excluded, 4 rejected, 20 deduplicated, `balanced=true` |
| T — Dataform | `account_curated` | 42 rows, executed against real BigQuery |
| T — Enrichment | `account_enriched` | 42 rows (reference-data join, no rows dropped) |
| T — JSON Producer | `account_target`, `gs://…-json-out/json/<run>/` | 40 documents (2 more rejected at the map/schema door), 1 batch file, 17,485 bytes |
| L — Loader | load-lane `.RPT` | 40 read → **40 accepted, 0 errors, 3 retries** |
| R — Recon | `reconciliation-report.{json,html}` | equation closes, `imbalance=0`, migrability 60.61% |

### Every disposition, individually proven

Every enumerated reject reason fired exactly once — the balancing equation is closing over
real, distinguishable failure modes, not an aggregate that happens to add up:

```
stage   reason                    n
map     MAP_UNMAPPED_ENUM_VALUE   1
parse   PARSE_BAD_DATE            1
parse   PARSE_BAD_NUMERIC         1
parse   PARSE_INVALID_JSON        1
parse   PARSE_SHORT_RECORD        1
schema  SCHEMA_INVALID            1
```

### Balancing equation

Recorded by the pipeline in `bq_recon.run_ledger`, and independently recomputed by the Java
recon service across the whole lane:

```
76 src_read = 40 written + 10 excluded + 6 rejected + 20 deduplicated
```

In today's two-door form that reads `76 = 40 migrated + 36 not migrated`, with the same
`10 / 6 / 20` breakdown. The line above is quoted verbatim as the run recorded it — this
run predates the two-door framing, and the numbers are unchanged by it.

(The ledger's own row reads `written=42, rejected=4` because it records only parse, filter
and dedup; the remaining 2 rejects come from the map/schema stage in the JSON producer. The
lane-level equation in criterion 1 and in the recon report accounts for every disposition.)

### Idempotency and retry, exercised for real

The Target System mock was restarted to clear its idempotency map before the run, so these
numbers are this run's alone:

```json
{"received": 43, "accepted": 40, "duplicatesIgnored": 0, "injectedFailures": 3, "distinctAccounts": 40}
```

43 requests for 40 documents: 3 injected 429/503 failures were retried with backoff and
succeeded. `X-Idempotency-Key` + server-side `putIfAbsent` meant zero double-writes.

## Acceptance verify (`08-acceptance-verify.log`)

```
✓ 1. balancing equation closes (whole lane): 76 = 40 written + 10 excluded + 6 rejected + 20 deduplicated
✓ 2. excluded count matches the harness exactly: 10 == 10 seeded
✓ 3. seeded malformed records rejected with correct reason codes: 6 reason codes, all matching
✓ 4. every TARGET document validates against the JSON Schema: 40/40 documents valid
✓ 5. TARGET emitted in 200-element batches: 1 batches of 200 (1 GCS file)
✓ 6. every key appears exactly once in TARGET: 40 keys, 0 orphans
✓ 7. all five artefact types present in both lanes: extraction 2 + 3, load 4; .FLG vouches for 4
✓ 8. .CHS checksums verify on both sides: extraction 1 entries parsed, load 1 files re-hashed and matching

VERIFY PASSED — all 8 acceptance criteria hold for run initial-20260804-163113.
```

---

## Honest caveats — what this run does NOT prove

1. **Kafka was provisioned but not written to.** Managed Kafka is VPC-only: its bootstrap
   address (`bootstrap.mig-kafka.…cloud.goog:9092`) does not resolve from an operator
   laptop (`NXDOMAIN`, confirmed). The cluster and topic exist and are ACTIVE; producing to
   them needs a client inside the VPC (a Dataflow worker or GCE/GKE host). The run
   therefore used `--sinks gcs`, and criterion 5 reports the GCS batch count exactly while
   explicitly stating the Kafka assertion was skipped. Review finding **M5** (no
   OAUTHBEARER token callback) is unresolved and would bite the moment a client *is*
   in-VPC.
2. **The Beam pipelines ran on DirectRunner against real GCS/BigQuery**, not as Dataflow
   jobs. The Flex Templates are built and published (so the Composer path has its
   artefacts), but launching them requires the Composer DAG, which is blocked by review
   finding **H6** (no `serviceAccountTokenCreator` grant for `composer-runner` →
   `dataflow-worker`).
3. **The Composer DAG was deployed, not triggered.** The environment is RUNNING and the DAG
   file is in its bucket; a DAG run in `dataflow` mode would fail on H5 (loader/recon tasks
   still point at local `build/install` paths) and H6.

## Fixes made during this run (previously-unknown defects)

These were real blockers found by actually deploying, not by reading:

| Fix | File | Why |
|---|---|---|
| Secret Manager rejects empty payloads | `local/scripts/gcp/seed_secrets.sh` | `pgp-passphrase` seeded as `""` → `INVALID_ARGUMENT`; now a single newline |
| `terraform output -raw` refuses map types | `local/scripts/gcp/build_templates.sh` | `buckets` is a map; switched to `output -json` + `json.load` |
| Podman needs fully-qualified images | `Dockerfile.dataflow` | `apache/beam_python3.11_sdk` → `docker.io/apache/…` |
| Root `.dockerignore` excluded `pipelines/` | `Dockerfile.dataflow.dockerignore` (new) | that ignore file serves the Java image; the Beam image needs the opposite |
| `gcloud storage rsync` flag/regex drift | `local/scripts/gcp/deploy_dags.sh` | `--delete` → `--delete-unmatched-destination-objects`; glob → regex exclude |
| Dataform CLI 3.x dropped repo flags | `local/scripts/gcp/deploy_dataform.sh` | `compile --repository/--project/--location` no longer exist |
| Dataform models hardcoded `mig-local` | `local/scripts/run_dataform.py` | retarget compiled JSON to the real project (`400 project mig-local has not enabled BigQuery`) |
| `import_keys(passphrase="")` breaks import | `pipelines/common/pgp.py` | gpg saw "no valid OpenPGP data"; only pass a passphrase when the key is protected |
| Java apps had no real-GCS auth path | `HttpObjectStore.fromEnv`, `BigQueryRest.fromEnv`, `run_pipeline.py` | `HTTP 401 Login Required`; now an OAuth token via `MIG_GCS_TOKEN` |
| `createBucket` hardcoded `project=mig-local` | `HttpObjectStore.java` | review finding **M8**, hit for real; now `MIG_GCS_PROJECT` |
| Kafka assertion unconditional | `tests/acceptance.py`, `run_pipeline.py` | records the run's sinks so a gcs-only run reports honestly instead of failing |

## Files in this directory

| File | Contents |
|---|---|
| `01-terraform-apply.log` | full apply creating Composer + Kafka (5 resources) |
| `02-seed-secrets.log` | Secret Manager versions seeded |
| `03-deploy-dataform.log` | Dataform compile validation |
| `04-build-templates.log` | 3 Flex Template images built + pushed |
| `05-deploy-dags.log` | DAG sync to the Composer bucket |
| `07-smoke-run.log` | the full E → T+R → L run |
| `08-acceptance-verify.log` | **the 8 green acceptance criteria** |
| `09-infra-evidence.log` | Composer state, DAG bucket, Kafka cluster, AR images, template specs |
| `09b-kafka-topic.log` | `target-system-target` topic definition |
| `10-bigquery-evidence.log` | per-table row counts, run_ledger, reject reason codes |
| `11-gcs-artefacts.log` | artefacts in all three buckets |
| `12-reconciliation-report.{json,html}` | the recon service's own verdict |
| `13-target-system-load.log` | load-lane `.RPT` + `.FLG` |
| `13b-vault-stats-clean.json` | Target System mock counters for this run alone |
