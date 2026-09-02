# GCP evidence — CSV-only source input — 2026-09-03

What changed: the mainframe source lane was switched from copybook records to **pipe-delimited CSV
with a header row** (`ACCT_ID|CUST_ID|CLIENT_TYPE|PRODUCT_CODE|CURRENCY|OPEN_DATE|STATUS|BALANCE`),
per `docs/PLAN-CHANGES-02092026-kafka-loader.md`. The copybook parser and the `contracts/copybooks/`
alternative were removed. The header row is skipped in the three readers (Python `RecordParser`,
the Java extractor, and the Beam `ReadBundleFn`), and a wrong-width CSV record is rejected with the
new `PARSE_BAD_COLUMN_COUNT` reason code. Everything else — the Kafka loader, two-door engine,
balancing equation, harness — is unchanged from the run documented in
`docs/evidence/gcp-kafka-loader-20260902/`.

The GCP runs under test, both on Composer `mig-composer` with the extract pre-staged from
`local/data/mainframe/ACCOUNT.src` (56 lines: 1 CSV header + 55 records; 50 valid, 5 seeded
malformed) and image tag `acedc65-dirty`:

- `run-dag-20260903-0125` (Airflow `manual__2026-09-02T22:33:50+00:00`, triggered 01:33 EEST) —
  **failed at `loader_app`** for an infrastructure reason unrelated to the CSV change (below).
- `run-dag-20260903-0223` (Airflow `manual__2026-09-02T23:29:16+00:00`, triggered 02:29 EEST) —
  **all 8 tasks green**, after recycling the wedged mock instance. This is the run the
  artefacts 40–46 document.

## Artefacts

| file | what it proves |
|---|---|
| `10-java-tests.txt` | `mvn -B test` on the CSV-only tree: 28 tests green across `common`, `loader-app`, `recon-service` |
| `11-python-tests.txt` | `.venv/bin/pytest tests -q`: 30 passed — includes the CSV header-skip and `PARSE_BAD_COLUMN_COUNT` unit tests |
| `20-local-e2e.txt` | Local end-to-end on the emulator stack with CSV input: 505 read → 502 migrated → 500 published/confirmed via the redpanda Kafka loader; balancing equation closes |
| `21-local-verify.txt` | `make verify` locally: all 10 acceptance criteria pass, criterion 2 now lists `PARSE_BAD_COLUMN_COUNT` |
| `22-gcp-first-attempt-failure.txt` | GCP: the first DAG run **failed** at `loader_app` — 3 × "50 published but never settled"; task states + the mock's wedged-OAUTHBEARER log lines that explain it |
| `30-gcp-kafka-consumer.txt` | GCP: the recycled mock's Kafka consumer re-attached (12 partitions, generation 5, no auth errors) and drained the failed run's 200 stale messages as duplicates |
| `31-gcp-kafka-infra.txt` | GCP: Managed Kafka cluster + topics + IAM bindings as deployed |
| `40-gcp-dag-task-states.txt` | GCP: per-task states of the `run-dag-20260903-0223` DAG run — all 8 tasks green |
| `41-gcp-reconciliation-report.json` | GCP: reconciliation report from `bq_recon` (balancing equation + key-level recon + target-system confirmations) |
| `42-gcp-load-report.json` | GCP: loader report — documents read/published/confirmed/unsettled on Managed Kafka |
| `43-gcp-mock-final-stats.json` | GCP: target-system-mock final stats — accepted/duplicate/rejected counters |
| `44-gcp-phase-record-counts.txt` | GCP: `[base]` stage counters from Cloud Logging for the three Dataflow jobs (records per phase) |
| `45-gcp-verify.txt` | GCP: `make verify` against real GCP (9/10 — criterion 4's Kafka half can't be checked from the laptop; pre-existing constraint, see below) |
| `46-gcp-phase-timing-summary.txt` | GCP: per-phase durations and record counts with wall-clock stamps |

## Local proof (before GCP)

- **Unit tests.** Java `mvn -B test`: 10 (common) + 12 (loader) + 6 (recon) = **28 passed**, build
  success in 5.8 s (`10-java-tests.txt`). Python `pytest tests -q`: **30 passed** in 2.5 s
  (`11-python-tests.txt`).
- **Local end-to-end** (`20-local-e2e.txt`, run `run-20260902-223743`, 26.1 s wall clock on the
  emulator stack):
  - harness writes **CSV**: `505 records -> local/data/mainframe/ACCOUNT.src (written=500 rejected=5)`
    — 1 header + 505 record lines
  - extractor: 505 records, 2 DAT files, 0 extraction errors (1.4 s)
  - file_processor: `src_read=505, migrated=502, not_migrated=3, rejected=3, balances=true` (9.1 s)
  - dataform: `account_curated: 502 rows` (6.6 s)
  - data_enrichment: `read=502, enriched=502` (6.8 s)
  - json_producer: `written=500, batches=3` → kafka 500 messages + 3 GCS JSON files; 2 rejects here
    complete the 5 seeded rejects (8.9 s)
  - loader (Kafka): `documents=500, published=500, accepted=0, duplicates=500, unsettled=0` (10.1 s)
  - recon: balancing equation `505 = 500 + 5` closes; all 500 TARGET rows confirmed (26.1 s)
- **Local verify** (`21-local-verify.txt`): **10/10 acceptance criteria pass** for
  `run-20260902-223743`, including criterion 2's CSV-native reason code set
  (`MAP_UNMAPPED_ENUM_VALUE, PARSE_BAD_COLUMN_COUNT, PARSE_BAD_DATE, PARSE_BAD_NUMERIC, SCHEMA_INVALID`)
  and criterion 10's settle equation `500 published = 0 confirmed + 500 duplicate + 0 errors + 0 unsettled`.
- **Replay caveat, honestly stated:** the loader line shows `accepted=0 duplicates=500` because the
  laptop redpanda kept its volume across `make down/up`, so yesterday's confirmations were still on
  the topic and every key of this run had already been confirmed once. Criterion 10's equation
  (settled = accepted + duplicatesIgnored + errors + unsettled, with `unsettled == 0`) still closes —
  this is the accepted/duplicates split already documented in
  `gcp-kafka-loader-20260902/README.md`. The GCP run below, on a fresh cluster, shows the clean
  `accepted`/`duplicates` split.
- **Negative paths** (forced replay / forced rejection / fail-closed recon) were exercised and
  documented in `gcp-kafka-loader-20260902/` (artefacts 23–25) against loader/recon code that this
  change does not touch; the CSV-native reject path is exercised inside every run by the 5 seeded
  malformed records (criterion 2 above).

## The first GCP attempt failed — honestly recorded

The CSV-only change worked right up to the load edge, and what broke there was not the CSV change
at all. `run-dag-20260903-0125` (triggered 01:33 EEST) passed the sensor and all four
transform phases (`file_processor`, `dataform_run`, `data_enrichment`, `json_producer` — the CSV
header skip and `PARSE_BAD_COLUMN_COUNT` ran on real Dataflow), then **failed at `loader_app` on
all three attempts**: *"50 document(s) published but never settled — Target System neither
confirmed nor rejected them within 120s. The load cannot be declared complete."* `reconciliation`
and `assert_run_balanced` went `upstream_failed`; the run ended `failed` at 02:01 EEST
(`22-gcp-first-attempt-failure.txt`).

The fail-closed settle gate did exactly its job — but against the wrong enemy:

- **Root cause: a 27-hour-old mock with a wedged Kafka login.** The mock instance serving the run
  was still **yesterday's** Cloud Run instance: the freshly built image carried the same tag
  (`acedc65-dirty`, git SHA plus `-dirty` on an unclean tree), so `terraform apply` created **no
  new revision** and the old instance kept serving. Around hour 27 of uptime its OAUTHBEARER
  credential refresh wedged — the broker client logged *"Initiating re-login for
  target-system-mock@…, logout() still needs to be called on a previous login = true"*, then
  *"Connection to node -1 terminated during authentication"* about once a second, forever. The
  consumer thread was alive and looping but no poll ever succeeded: `/__admin/stats` was frozen at
  exactly yesterday's totals (`consumedFromTopic: 1000, confirmationsPublished: 1000`) — it
  consumed **nothing** from today's run, so the loader's 50 documents got zero verdicts and the
  task correctly refused to pass them.
- **Fix: recycle the instance, keep the code.** `gcloud run services update target-system-mock
  --image=<same image>` creates a new revision — fresh instance, fresh login. The consumer group
  (`target-system-mock`) resumes from its committed offsets, so the ~200 stale messages from the
  failed attempts (50 from `json_producer`, ~150 from the loader's three publish rounds) replay
  as duplicates and are confirmed — the duplicate-tolerant settle path already proven in
  `gcp-kafka-loader-20260902/23-negative-replay.txt`. The CSV change needed no code fix.
- **Lesson recorded in the runbook:** a `terraform apply` that changes nothing deploys nothing.
  When a Cloud Run service must be recycled, the explicit switch is
  `gcloud run services update --image=<current image>`.

## GCP run — green re-run

`run-dag-20260903-0223` (Airflow `manual__2026-09-02T23:29:16+00:00`, triggered 02:29 EEST) —
**all 8 DAG tasks green** in 16 min 51 s (`40-gcp-dag-task-states.txt`), with the mock recycle as
the only changed variable: same code, same images, same 50-account CSV extract (same seed), so
what differs from the failed attempt is a fresh Cloud Run instance with a working Kafka login.

Per phase (`46-gcp-phase-timing-summary.txt`, counts from `44-gcp-phase-record-counts.txt`):

| task | duration | records |
|---|---|---|
| wait_for_extract_flg | 1.2 s | sensor — .FLG pre-staged |
| file_processor | 342.5 s | 55 read (CSV header skipped) → 52 migrated, 3 not_migrated, 0 imbalance |
| dataform_run | 34.4 s | 52 rows → `account_curated` |
| data_enrichment | 274.4 s | 52 read → 52 enriched |
| json_producer | 263.2 s | 50 written, 1 batch → GCS 1 file (50 docs) + Kafka 50 messages |
| loader_app | 25.8 s | 50 documentsRead → 50 published → 50 settled, **0 unsettled** |
| reconciliation | 20.0 s | 50/50 TARGET rows confirmed |
| assert_run_balanced | 7.9 s | 55 = 50 migrated + 5 not migrated |

The CSV-only change, proven on real Dataflow:

- **Header skip:** the source file is 56 lines (1 CSV header + 55 records) and
  `src_read=55` — the three readers skipped the header everywhere.
- **New reason code:** `PARSE_BAD_COLUMN_COUNT` appears in the reject set next to the four
  pre-existing reasons (`41-gcp-reconciliation-report.json`, `rejectsByReason`: 1 ×
  `MAP_UNMAPPED_ENUM_VALUE`, 1 × `PARSE_BAD_COLUMN_COUNT`, 1 × `PARSE_BAD_DATE`,
  1 × `PARSE_BAD_NUMERIC`, 1 × `SCHEMA_INVALID`).
- **Balancing equation closes:** `srcRead 55 = written 50 + rejected 5`, `imbalance 0`; ledger
  agrees (reject log 5 = lineage 5); all 50 TARGET rows confirmed (`41`).
- **Load edge:** 50 published, 50 settled, 0 errors, 0 unsettled in 25.8 s (`42-gcp-load-report.json`)
  — versus three 120 s timeouts in the failed attempt.

**One honest caveat — the accept/duplicate split is a replay artefact.** The load report shows
`accepted=0, duplicatesIgnored=50`. That is the duplicate-tolerant settle path, not a defect: the
recycled mock first drained the failed run's ~200 stale messages (50 accepted then, see
`30-gcp-kafka-consumer.txt`), so its in-memory account set already held all 50 keys when this
run's messages arrived — every document was a duplicate from the mock's point of view, and the
loader's fresh consumer group settled on the confirmation events already on the topic. The settle
equation still closes exactly (`50 published = 0 accepted + 50 duplicates + 0 errors + 0
unsettled`, criterion 10), and a fresh-cluster run shows the clean split
(`gcp-kafka-loader-20260902/README.md`). Final mock stats (`43-gcp-mock-final-stats.json`):
`consumedFromTopic 300` = 200 stale-drain + 50 json_producer + 50 loader;
`confirmationsPublished 300`; `accepted 50` (all from the drain); `duplicatesIgnored 250`;
`distinctAccounts 50`.

## Verify on GCP

`make verify` (`tests/acceptance`) run from the laptop against `run-dag-20260903-0223`, querying
the real GCS and BigQuery endpoints of the run — **9 of 10 criteria pass** (`45-gcp-verify.txt`),
the same 9/10 as yesterday's Kafka run:

- **Criterion 2** now lists the CSV-native reason code set in full:
  `MAP_UNMAPPED_ENUM_VALUE, PARSE_BAD_COLUMN_COUNT, PARSE_BAD_DATE, PARSE_BAD_NUMERIC, SCHEMA_INVALID`
  — `PARSE_BAD_COLUMN_COUNT` is the new code this change adds, and it fires on-GCP.
- **Criterion 10** reads `50 published, 0 confirmed, 50 duplicate, 0 rejected, 0 unsettled` — the
  settle equation closes on the replay split explained above.
- **The only failure is criterion 4's Kafka half, and it is a laptop constraint, not a pipeline
  one:** the acceptance script tries to read the `target-system-target` topic from the host, but
  the Managed Kafka bootstrap endpoint (`bootstrap.mig-kafka….cloud.goog:9092`) resolves only
  inside Google's network, so rdkafka fails with `_ALL_BROKERS_DOWN` before any offset is read
  (the two rdkafka `FAIL` lines at the top of `45-gcp-verify.txt`). This is the pre-existing
  constraint already documented in `gcp-kafka-loader-20260902/45-gcp-verify.txt`, unchanged by
  the CSV switch. The Kafka half of criterion 4 is proven by the in-cloud evidence instead:
  `json_producer`'s stage counter (`44-gcp-phase-record-counts.txt`) shows
  `batches: 1, messages: 50` on `target-system-target` — one ≤200-element batch, as required —
  and the loader's 50 published → 50 settled (`42-gcp-load-report.json`) means the messages
  were on the topic and consumed.

(For the verify the run id was pointed at the GCP run via `local/state/last_run_id`; the file
was restored to the local run afterwards — it is gitignored scratch state.)