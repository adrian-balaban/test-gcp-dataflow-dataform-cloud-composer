# Proposal — move the Loader off HTTP POST onto Kafka

> **Status:** proposal for discussion. Not implemented, not wired into `make verify`.
> **Date:** 2026-09-02. Grounded in the working tree at that commit.
> **Audience:** the alignment on 2026-09-03, then the backend dev group.
> Companion to [`alternative-implementations.md`](alternative-implementations.md), which
> proposes alternative *engines*; this one proposes an alternative *Load edge*.

---

## 1. Which edge are we actually changing

The Loader App has two edges, and "make the loader Kafka" could mean either. They are
independent, and only the second is what this proposal is about.

```
        (input edge)                     LoaderApp                (output edge)
  GCS  json/<runId>/*.jsonl   ───────►  read, validate,  ───────►  POST /v1/accounts
  written by json_producer              push, tally               Target System (mock)
```

| Edge | Today | Kafka version | This proposal |
|---|---|---|---|
| **Output** — how documents reach Target System | `POST /v1/accounts` + `X-Idempotency-Key`, hand-rolled retry/backoff | produce to `target-system-target`, Target System consumes | **yes — section 3** |
| **Input** — where the Loader gets documents | `store.list(jsonBucket, "json/<runId>/")` | consume `target-system-target`, which `json_producer` **already publishes** (`--sinks both`) | optional, section 6 |

Note the awkward consequence if we did both: `json_producer` already writes the *same*
topic name the Loader would produce to. Doing both edges means two topics
(`…-target-staged` → Loader → `…-target`) or dropping the Loader from the middle
entirely. That is a real design fork and it belongs on tomorrow's agenda (section 7, Q1).

---

## 2. What the HTTP path actually buys us today (the thing we must not lose)

This is the crux. `LoaderApp` is not a dumb pipe — the **synchronous response code is the
verdict**, and three artefacts are built out of it:

| HTTP result | Loader treats as | Lands in |
|---|---|---|
| `201` | `CREATED` → `accepted++` | `.RPT` `accepted` |
| `200` | `DUPLICATE` (idempotent replay) → `duplicates++` | `.RPT` `duplicatesIgnored` |
| `429` / `5xx` | transient → backoff+jitter, retry, `retries++` | `.RPT` `retriesPerformed` |
| other `4xx` | **permanent** → `.ERR` row | `.ERR` + `.RPT` `errors` |
| missing `accountId` / `dedupKey` | permanent, never sent | `.ERR` |

`recon-service` then reads `documentsRead` and `errors` off that `.RPT`
(`ReconService.java:141-143`) and `tests/acceptance.py:288` reads the load `.CHS`.

**A Kafka produce ack does not mean any of this.** `acks=all` means *the broker durably
holds the bytes*. It does not mean Target System parsed, accepted, or persisted the
record. Swapping POST for produce, naively, converts a per-document verdict into a
per-document "we mailed it" — and the `.RPT` becomes a report about our own outbox.

**So the verdict has to be relocated, not deleted.** That is the whole design of this
proposal, and it is the one point worth 10 minutes of the meeting.

---

## 3. The proposed shape

The verdict moves to two return topics. One of them **already exists and already works**.

```
LoaderApp  ──produce──►  target-system-target        ──►  Target System
                                                            │
recon/loader ◄──consume── target-system-confirmations ◄─────┤  applied
             ◄──consume── target-system-rejections   ◄──────┘  refused (new topic)
```

### 3.1 Publish phase (replaces `Loader.send`)

* Producer config: `enable.idempotence=true`, `acks=all`, `retries=5`, `linger.ms=20` —
  **identical to `pipelines/common/sinks.py:KafkaTargetWriter`**, so both producers on
  this project agree.
* `key = migration.dedupKey`. `dedupKey` is `account_key` — a sha256 of the mapping's
  account-key fields (`mapping.py:313`) — so **keying by it partitions by account and
  preserves per-account ordering**. Same key `KafkaTargetWriter` already uses.
* Headers: `run-id`, `idempotency-key`, `batch-id` — same three headers, same names.
* The `accountId` / `dedupKey` presence check **stays exactly as is** and still routes to
  `.ERR` without sending. It is a batch defect, and it predates Kafka.
* `flush()`'s return value is checked. Undelivered is a failure, not a slow success —
  this project has already lost 400 records to ignoring it once (`sinks.py:296-303`).

### 3.2 Settle phase (replaces the 200/201/4xx branch)

After publishing every document, the Loader does a **bounded read of the two return
topics for this `runId`**, then writes its artefacts:

* Reuse the exact pattern in `ReconService.readConfirmations` — fresh per-run consumer
  group `loader-<runId>`, `assign` all partitions, `seekToBeginning`, poll until every
  partition reaches its snapshot `endOffsets`, 30s backstop deadline, filter on `runId`.
  It is written, reviewed, and works against Managed Kafka.
* `accepted` = confirmations matched to published keys.
* `errors` = rejection events → `.ERR` rows, carrying Target System's own reason string
  instead of an HTTP status code.
* `sent - accepted - errors` = **`unsettled`** — a new, third outcome that has no HTTP
  equivalent, and which is the honest name for "we published it and nobody told us
  anything". A non-zero `unsettled` must fail the run.

### 3.3 `.RPT` field changes

| Field | Today | After |
|---|---|---|
| `documentsRead` | unchanged | unchanged |
| `accepted` | HTTP 201 count | confirmed-by-Target-System count |
| `duplicatesIgnored` | HTTP 200 count | **gone** (see Q2) |
| `errors` | 4xx + malformed | rejection events + malformed |
| `retriesPerformed` | our backoff loop | **gone** — producer-internal |
| `published` | — | **new**: broker-acked sends |
| `unsettled` | — | **new**: published, never settled |

⚠️ `accepted` keeps its name but **changes meaning**. This project deliberately kept
`written`/`rejected` names stable "so archived evidence reports stay comparable"
(`ReconService.java`, `Balance`). We should either rename to `confirmed` or add a
`reportVersion` field. Q3 below.

### 3.4 Reconciliation flips from advisory to primary

Today criterion 9 is guarded: empty `TARGET_SYSTEM_CONFIRMATION_BOOTSTRAP` → recon skips
the confirmation read and reports `enabled=false`, "which keeps a no-Kafka run green".

Under this proposal **there is no no-Kafka run**, and the confirmation stream is the only
evidence the load happened. The `enabled` flag stops being a cost switch and becomes a
way to silently pass a run that proved nothing. It should be removed or inverted to
fail-closed. (`docs/production-readiness.md` already flags this flag as a deviation.)

---

## 4. What gets simpler, what gets harder

**Simpler**
- ~90 lines go: the retry loop, `backoff()` with jitter, the transient/permanent status
  classification, `LoadFailure.status`. Producer idempotence + `retries` replaces it.
- No Cloud Run identity-token fetch (`GcpToken.identityToken`), no VPC-reachability
  question for an HTTPS endpoint.
- Target System absorbs load at its own pace. The 429 throttle disappears as a concept.
- Replay is free: the topic holds 7 days (`terraform/modules/kafka/main.tf`), so a failed
  wave is re-consumable without re-running the pipeline.

**Harder**
- **Loss of synchronous truth** — section 2. This is the whole cost.
- **Backpressure inverts.** Today a slow Target System pushes back with 429 and the
  Loader paces itself. With Kafka we publish at full speed and the only signal is
  *consumer lag* — which nothing in this repo currently watches or alarms on.
- **A new dependency for `.ERR`.** Rejections must be a contract the Loader team commits
  to. Without a rejection topic, a bad document is indistinguishable from a slow one, and
  everything falls into `unsettled`.
- **Two more failure modes with no HTTP analogue:** a partition whose consumer is dead
  (records durably queued, never applied), and a poison message that stalls Target
  System's consumer at one offset.
- **The settle phase needs a real timeout policy.** 30s is fine for 400 documents. For
  millions it is not, and "how long do we wait for confirmations before failing the run"
  is a new operational decision that HTTP never asked us to make.

---

## 5. Concrete change list

| # | File | Change |
|---|---|---|
| 1 | `apps/loader-app/pom.xml` | add `kafka-clients` + `slf4j-simple` (copy `recon-service/pom.xml:30-36` verbatim — without the binder, a SASL failure reads only as "Topic not present in metadata after 60000 ms") |
| 2 | `apps/common/.../KafkaClients.java` *(new)* | extract the producer/consumer property builder + the `KAFKA_SECURITY_PROTOCOL` → SASL_SSL/OAUTHBEARER block, which is **currently copy-pasted in `TargetSystemMock:96-121` and `ReconService:437-462`**. Third copy is the moment to extract it. |
| 3 | `apps/loader-app/.../LoaderApp.java` | `Loader.send` → `publish` + `settle`; delete `backoff`; keep `requireField` |
| 4 | `apps/loader-app/.../LoaderApp.java` | new `--kafka-bootstrap`, `--kafka-topic`, `--rejection-topic`, `--settle-timeout-seconds`; keep `--target-system-url` behind a `--sink http\|kafka` flag for one release |
| 5 | `apps/target-system-mock/.../TargetSystemMock.java` | add a consumer loop on `target-system-target` applying the same idempotency map the POST handler uses, and publishing to the rejection topic on 400. The 429/503 injector becomes lag injection. |
| 6 | `terraform/modules/kafka/main.tf` | add `rejections` to the `topics` map (1 partition, same shape as `confirmations`) |
| 7 | `terraform/modules/iam` | `roles/managedkafka.client` for the `loader-app` SA (grant already exists for the other three principals) |
| 8 | `composer/dags/mig_000001_1.py` | loader task gains `--kafka-bootstrap`/`--kafka-topic` args. `KAFKA_SECURITY_PROTOCOL` **is already in `java_app` env_vars** — that bug is already paid for. |
| 9 | `apps/recon-service` | `enabled` flag fails closed (section 3.4) |
| 10 | `tests/` | `ReconciliationMatcherTest`-style pure test for the settle set-difference; acceptance criterion for `unsettled == 0` |
| 11 | `docs/`, `apps/README.md`, `README.md` slide 2 | the Load lane arrow stops being "REST + retries" |

Local stack needs no change: redpanda is already up, `make init-infra` already creates
topics, and the mock is already on the network.

---

## 6. Optional companion — the input edge

`json_producer --sinks both` already publishes every TARGET document to
`target-system-target`. If the Loader consumed that instead of listing GCS, the GCS JSON
sink becomes evidence-only rather than a handoff, and the `.FLG`-semaphore-on-GCS contract
for the Load lane weakens. **Recommend deferring**: it is a bigger change to the handoff
contract with the Loader team than the output edge is, and it is not what was asked.

---

## 7. Open questions for tomorrow

| Q | Question | Why it blocks |
|---|---|---|
| **Q1** | If `json_producer` already publishes to `target-system-target`, **is there still a Loader App at all**, or does Target System just consume the pipeline's topic? | This is the strategic fork. The Kafka answer may be *delete the Loader*, not *rewrite it*. Answer this first. |
| **Q2** | Does Target System dedupe on message key, or do we keep an idempotency concept in the payload? | Decides whether `duplicates` survives as a number we can report at all. |
| **Q3** | Rename `accepted` → `confirmed`, or version the report? | Archived evidence comparability is a stated project value. |
| **Q4** | Will the Loader team commit to a **rejection topic**? | Without it there is no `.ERR`, and every bad document is `unsettled`. Non-negotiable dependency. |
| **Q5** | How long does the Loader wait for confirmations before failing the run? | New operational parameter with no HTTP analogue. |
| **Q6** | Who owns consumer-lag alerting? | The replacement for 429 backpressure. Nothing watches it today. |

## 8. Recommendation

Do the output edge (section 3), keep `--sink http|kafka` for one release so both paths are
runnable side by side against the same acceptance suite, and **settle Q1 and Q4 before
writing code** — Q1 can make the whole change moot, and Q4 can make it unshippable.
