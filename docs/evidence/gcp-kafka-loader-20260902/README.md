# 2026-09-02 — the Load edge moved from HTTP POST to Kafka

Implements [`docs/PLAN-CHANGES-02092026-kafka-loader.md`](../../PLAN-CHANGES-02092026-kafka-loader.md)
section 3: the Loader stops calling `POST /v1/accounts` and instead produces every document
to `target-system-target`, then **settles** the run against two return topics —
`target-system-confirmations` and the new `target-system-rejections`.

The point of the change is also its main risk. On HTTP the response code *was* the verdict:
201 accepted, 200 duplicate, 4xx permanently rejected, and the `.RPT` was built out of those
three. A Kafka produce ack carries none of it — `acks=all` means the broker durably holds the
bytes, not that Target System parsed, accepted or persisted the record. So the evidence below
is mostly about one question: **after the switch, does the Loader still know what happened to
each document?**

- **Image tag** `fd92e81-dirty` (all six images).
- **Terraform** `-var=enable_kafka=true -var=enable_composer=true` — Managed Kafka with
  **three** topics now, Serverless VPC Access connector, **four** project-level
  `managedkafka.client` grants (`loader-app` is the new one).
- **Sink** `--sink kafka`. `--sink http` is retained for one release so both paths run
  against the same acceptance suite.

## Artefacts

| Artefact | What it shows |
|---|---|
| `10-java-tests.txt` | 28 Java tests green. `SettleTest` is 12 of them — the settle set-difference as pure logic, no broker. |
| `11-python-tests.txt` | 32 Python tests green. |
| `20-local-e2e.txt` | Full chain on the Kafka edge: 505 read → 500 published → **500 accepted, 0 unsettled**; recon confirms all 500 TARGET rows. |
| `21-local-verify.txt` | All **10** acceptance criteria pass, including the new criterion 10. |
| `22-load-report.json` | The load `.RPT` at `reportVersion: 2`, with the new `published` / `unsettled` / `sink` fields. |
| `23-negative-replay.txt` | **Negative path A** — re-running the loader on an already-loaded batch settles cleanly and exits 0. |
| `24-negative-rejection.txt` | **Negative path B** — a document with a blank `accountId` produces a real rejection event carrying `reason: missing_accountId`. |
| `25-negative-recon-failclosed.txt` | **Negative path C** — recon with no confirmation stream now exits 1 instead of reporting `enabled=false` and passing. |
| `30-gcp-mock-consumer.txt` | The mock's Kafka consumer attached to real Managed Kafka — all 12 partitions of `target-system-target` assigned across 3 brokers over SASL_SSL/OAUTHBEARER, from a Cloud Run service reached through the Serverless VPC Access connector. |
| `31-gcp-kafka-infra.txt` | Terraform-managed Kafka on GCP: cluster ACTIVE, all 3 topics (`target-system-rejections` is new), all 4 `roles/managedkafka.client` grants (`loader-app` is new). |
| `40-gcp-dag-task-states.txt` | The full Composer DAG run, `run-dag-20260902-1852` — **8/8 tasks `success`**. |
| `41-gcp-reconciliation-report.json` | The GCP run's reconciliation report: `targetSystemReconciliation.confirmedTargetRows: 500`, `unconfirmedTargetRows: 0`. |
| `42-gcp-load-report.json` | The GCP run's load `.RPT`: `published: 500, accepted: 500, unsettled: 0`. |
| `43-gcp-mock-final-stats.json` | Mock's `/__admin/stats` after the run — 1000 received (500 from the loader + 500 from `json_producer`'s evidence-only Kafka write), 1000 confirmations published. |
| `44-gcp-phase-record-counts.txt` | Per-Beam-stage record counts pulled from Cloud Logging (`file_processor`, `data_enrichment`, `json_producer`). |
| `45-gcp-verify.txt` | `make verify` run from the laptop against the live GCP endpoints for this run: **9/10 pass**, including criteria 9 and 10 — the only failure is criterion 4's optional Kafka-watermark half, which needs VPC-internal access the laptop does not have (documented, pre-existing constraint, unrelated to this change). |
| `46-gcp-phase-timing-summary.txt` | Wall-clock duration and record counts for every DAG phase — 17.5 minutes end to end, `loader_app` itself in 30 seconds. |

## What criterion 10 actually asserts

```
documentsRead = accepted + duplicatesIgnored + errors + unsettled
```

and `unsettled == 0`. The identity is over `documentsRead`, not `published`, because `errors`
mixes two populations: documents rejected before sending (missing `accountId`/`dedupKey`,
never published) and documents rejected by Target System (published, then refused).

`unsettled` is the number with no HTTP analogue — published, and never spoken about. It
covers a dead consumer and a poison message stalling a partition at one offset, both of which
otherwise look exactly like a successful run. A non-zero `unsettled` exits non-zero and fails
the DAG.

## GCP DAG run — full end-to-end proof

`run-dag-20260902-1852`, triggered on Cloud Composer with `-var=enable_kafka=true
-var=enable_composer=true`, image tag `fd92e81-dirty`. All 8 tasks green, 17.5 minutes wall
clock (`46-gcp-phase-timing-summary.txt`):

| Task | Duration | Result |
|---|---|---|
| `wait_for_extract_flg` | 1.2s | `.FLG` present (extract pre-staged) |
| `file_processor` | 383.2s | 505 read → 502 migrated, 3 not_migrated, balances |
| `dataform_run` | 40.9s | 502 rows → `account_curated` |
| `data_enrichment` | 262.7s | 502 read → 502 enriched |
| `json_producer` | 270.8s | 500 written, 3 batches → GCS (500 docs) + Kafka (500 msgs, evidence-only) |
| **`loader_app`** | **30.4s** | **500 documentsRead → 500 published → 500 accepted, 0 unsettled** |
| `reconciliation` | 20.3s | **500/500 TARGET rows confirmed by Target System** |
| `assert_run_balanced` | 9.2s | balancing equation closes |

Reconciliation report (`41-gcp-reconciliation-report.json`):
`targetSystemReconciliation.confirmedTargetRows: 500, unconfirmedTargetRows: 0`.
Load report (`42-gcp-load-report.json`): `reportVersion: 2, sink: "kafka", published: 500,
accepted: 500, unsettled: 0`. `make verify` run against these live GCP endpoints
(`45-gcp-verify.txt`) confirms 9 of 10 criteria, including 9 and 10 — the two criteria this
whole plan exists to prove.

## Three defects the proposal did not anticipate

All were invisible to review and only appeared once the code ran — two on the local stack,
the third only on real GCP. They are the reason this directory exists rather than a green
tick on a plan document.

### 1. The bounded end-offset read silently under-reported

The plan says to reuse the pattern in `ReconService.readConfirmations`: snapshot `endOffsets`,
poll until every partition reaches them. Correct in recon, which runs long after the load.
**Wrong in the loader**, which publishes and reads immediately: at snapshot time Target System
has consumed almost nothing, so the end offsets are near-empty, the read hits "all partitions
at end" on its first pass, and the settle timeout never fires at all.

Measured: **96 of 500 documents settled, 404 reported as `unsettled`** on a run where every
document was confirmed seconds later. A false negative — the worst kind for a migration gate,
because it fails a good run and trains people to ignore it.

Fixed by terminating on the question being answered rather than the topic's length: poll until
every published key has a verdict, stop early only on the deadline. That also promotes the
`--settle-timeout-seconds` value from an unused backstop into the real bound.

### 2. A silent duplicate failed a legitimate re-run

On HTTP a replay returned 200 — a positive verdict. The first Kafka implementation published
nothing for a replay, mirroring the mock's existing rule. Under settle-by-set-difference that
is fatal: a replayed document gets *no verdict at all*, so re-running an already-loaded batch
reports **100% `unsettled` and fails**. Since replay-after-failure is the main operational
reason to be on Kafka (the topic retains 7 days precisely so a wave can be re-consumed), the
design would have broken its own headline benefit.

Fixed with an `outcome` field on the confirmation event: `created` on first apply, `duplicate`
on a replay. Both settle; only the tally differs, so `duplicatesIgnored` keeps its HTTP-path
meaning (plan Q2). `23-negative-replay.txt` is the proof.

### 3. Cloud Run scale-to-zero starves the consumer — found only on real GCP

Local testing (redpanda + a podman-compose mock that never stops) could not surface this
one. The mock's `min_instance_count` was `0` unconditionally — fine for HTTP, where the
Loader's first POST is itself the cold-start trigger and the only cost is that one request
waiting longer. On Kafka there is no request to wake the instance: the consumer either
happens to be running already or the messages just sit on the topic.

First DAG attempt on GCP: the loader published all 500 documents successfully
(SASL_SSL/OAUTHBEARER auth against Managed Kafka worked, per `30-gcp-mock-consumer.txt`),
then reported **500 unsettled and failed** — the mock woke too slowly to confirm inside the
120s settle window. It did eventually catch up and confirm everything; by then the DAG task
had already failed.

Fixed by tying `min_instance_count` to whether the mock has a topic to consume:
`var.target_topic == "" ? 0 : 1` in `terraform/modules/target_system_mock/main.tf`. Zero
(unchanged) whenever Kafka is off; one whenever the consumer has something to listen for.
Re-run after the fix: `loader_app` completed in **30 seconds** — see the GCP DAG run table
above.

## One honest caveat

On a replay the `accepted`/`duplicates` **split is not deterministic**. The settle loop stops
as soon as every key has a verdict, so it may read a key's older `created` event or its newer
`duplicate` event depending on timing — `23-negative-replay.txt` shows `accepted=236
duplicates=264` where a strict reading would say `0/500`. The *total* is exact and `unsettled`
is 0, which is what the criterion asserts; only the split between two reporting columns
varies. Making it exact would mean draining to the true end offsets after satisfying the
count, which is a real cost for a cosmetic gain — noted rather than done.

## Still open

**Q6 — nobody watches consumer lag.** HTTP had 429 backpressure; Kafka's equivalent signal is
consumer lag, and nothing in this repo watches or alarms on it. The plan named this in section
4 and the implementation has not closed it.

**Criterion 4's Kafka half is unreachable from a laptop.** `45-gcp-verify.txt` shows this as
the sole failure of `make verify` run against the live GCP endpoints — Managed Kafka has no
public endpoint, so `_ALL_BROKERS_DOWN` is expected from outside the VPC. The GCS half of
the same criterion (batch-file count) already passed before the Kafka check ran. This is the
same documented, pre-existing constraint noted in `docs/runbook-gcp.md` §B8 since
2026-08-23 — not a regression introduced by this change.
