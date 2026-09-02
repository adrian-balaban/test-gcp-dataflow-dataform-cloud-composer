# Reusable patterns extracted from this repo

> **What this is:** the parts of `test-gcp-dataflow-dataform-cloud-composer` that are worth
> carrying to other projects — patterns, and hard-won bug fixes that will otherwise be
> rediscovered the expensive way. Each entry names the file, so the code is the reference.
>
> **Date:** 2026-09-02. **Audience:** backend devs and devops on the next project.
>
> Ordered by how much time it saves the next person, not by subsystem.

---

## Part 1 — For developers

### 1.1 The actual data source in this repo: synthetic, generated locally

`harness/generate.py`, `apps/extractor-app/src/main/java/ro/mig/extractor/ExtractorApp.java`

Because the real data doesn't exist yet, the repo has two generators/mocks that stand in for
that source:

`apps/extractor-app/.../ExtractorApp.java` — the javadoc is explicit: "Owned by the other team
in reality; here it is a faithful mock that honours the stated contract." It doesn't generate
the data itself — it reads an already-existing dump (`readSource(a.input)`, table2table-style)
and just packages it into the `.DAT`/`.CHS`/`.ERR`/`.RPT`/`.FLG` artefacts per the contract.

`harness/generate.py` — this is the actual generator. It produces a synthetic Db2 account
extract (`python -m harness.generate --accounts 2000 --format copybook`), plus a `Manifest`
JSON with the exact expected counts (how many valid records, how many rejected, and for which
reason). It deliberately seeds 5 kinds of malformed records (`short_record`, `bad_numeric`,
`bad_date`, `unmapped_status`, `schema_violation`) so it can later verify the pipeline
classifies them correctly — the manifest is the oracle `make verify` checks the engine's actual
output against.

The full flow, including the rest of the real pipeline (the part this team tests) and the
confirmation loop at the Load end:

```
harness/generate.py     →  synthetic dump (pipe-separated CSV) + manifest.json (the oracle)
        ↓
ExtractorApp (mock)     →  packages into .DAT/.CHS/.ERR/.RPT/.FLG, PGP-encrypts,
                            uploads the bundle to File Storage (GCS)
                            (`apps/extractor-app/src/main/java/ro/mig/extractor/ExtractorApp.java`)
        ↓
Dataflow File Processor →  reads after the .FLG semaphore (1.2), writes to BigQuery "Extraction"
                            (`pipelines/file_processor/pipeline.py`)
        ↓
Dataform                →  SQL transformation (`dataform/definitions/account_curated.sqlx`),
                            writes to BigQuery "Transformation"
        ↓
Dataflow Data Enrichment →  enriches the records (`pipelines/data_enrichment/pipeline.py`)
        ↓
Dataflow JSON Producer  →  produces the target messages, writes them to File Storage (FS2)
                            (`pipelines/json_producer/pipeline.py`)
        ↓
LoaderApp               →  reads from File Storage, sends each record with an
                            `X-Idempotency-Key` (1.8) to Target System
                            (`apps/loader-app/src/main/java/ro/mig/loader/LoaderApp.java`)
        ↓
TargetSystemMock        →  stands in for Target System: replies 201/200 on idempotency,
                            injects 429/503 (rate limited/unavailable) at a configurable rate (1.9), sends back
                            confirmations + rejections
                            (`apps/target-system-mock/src/main/java/ro/mig/vault/TargetSystemMock.java`)
        ↓
ReconService             →  reads the confirmations/rejections, computes the balancing
                            equation `src_read == migrated + not_migrated` (1.3) and
                            produces the migrability/reconciliability reports
                            (`apps/recon-service/src/main/java/ro/mig/recon/ReconService.java`)
        ↓
Cloud Composer             orchestrates all the stages above (FP, DF, EN, JP) and fails
                            the DAG if recon exits non-zero
                            (`composer/dags/mig_000001_1.py`)
```

So: the "business" source is the mainframe (unavailable right now), and the source actually
used today is `harness/generate.py`, with `ExtractorApp` as the mock layer that faithfully
imitates the format/contract the other team's app would produce, while at the other end
`TargetSystemMock` plays the same role for Target System — the whole chain, from generation to
reconciliation, runs with no dependency on either team's real systems.

This abstraction has a further payoff: because `contracts/` separates the engine from the
runtime that executes it, and the acceptance criteria check outcomes (BigQuery tables, GCS
artefacts, manifest numbers, the balance), not Beam's internal mechanics, it was possible to
generate 4 alternative runtime proposals for the same C1 (Spark on Dataproc Serverless, dbt +
BigQuery, Cloud Run Jobs + Workflows, a streaming/CDC variant — plus a Flink sub-variant of the
latter), documented in
[`docs/alternative-implementations.md`](alternative-implementations.md).

The C4 model itself — C1 System Context through C4 Code — was generated as PlantUML sources
rendered to SVG, one file per level; the file pairs are listed in the
[appendix](#appendix--the-c4-diagrams-as-files).

### 1.2 The `.FLG` semaphore handoff ⭐ *the single most portable idea here*

`apps/README.md`, `composer/dags/mig_000001_1.py` (`GCSObjectExistenceSensor`)

Producer writes `.DAT` `.CHS` `.ERR` `.RPT`, then — **last, only once everything else is
durable** — a `.FLG` file. Nothing downstream reads a byte until `.FLG` appears.

A partially-written extract can never be half-processed. No locking, no coordination, no
shared database between two teams that never call each other. Works on any object store.
**Use this for every cross-team file handoff.**

### 1.3 The balancing equation as a build gate

`ReconService.Balance`, `assert_run_balanced` in the DAG

`src_read == migrated + not_migrated`, where **each term is read from where it was
actually recorded, not re-derived**: `src_read` from the *upstream's own* `.RPT`,
`written` from the target table, `rejected` from the reject log. A discrepancy anywhere
shows up as an imbalance instead of being defined away by a shared derivation.

Then: **recon exits non-zero and fails the DAG.** "A run that doesn't balance is a failed
run" is operational, not aspirational. Without the exit code it is a line in a report
nobody reads.

### 1.4 Contracts as data, enforced at build time

`contracts/artefacts.json`, `apps/common/.../Artefacts.java`, `pipelines/common/artefacts.py`

One JSON file holds the naming convention and shared column names. Maven copies it onto
the Java classpath and a **`maven-enforcer` rule fails the build if it is missing**, so a
jar can never ship without it. Python reads the same file. `ArtefactsTest` uses the
on-disk manifest as its oracle, so hardcoding a name back into Java fails the build.

This is how you stop two languages drifting apart. (Caveat: `ARCHITECTURE.md` weakness #1
notes this covers *names*, not *types* — a real IDL would be better. Take the pattern,
know the ceiling.)

### 1.5 Never default an identity field

`LoaderApp.requireField`

Defaulting a missing idempotency key to `""` made the server accept `""` as a *valid*
key: the first key-less document was created and every later one collided with it and was
counted as a duplicate. **N-1 accounts vanished with a zero exit code and a `.FLG`
claiming success.**

Rule: a missing identity field is a defect in the input, not a value to substitute. Reject
it, route it to the error file, never send it.

### 1.6 An undelivered async write is a failure, not a slow success

`pipelines/common/sinks.py:_check_delivered`

`producer.flush(30)` returns **the number of messages still queued when it gave up**.
Ignoring that return value is how a dead broker passes for success: with nothing
listening, every message sits in the local queue until timeout, the delivery callback is
never invoked at all, and the error list stays empty.

The first Composer run lost 400 records this way — two green 30-second batches, zero
messages in Kafka, nobody told. **Check the return value of every flush/close/drain.**

### 1.7 Bounded read of an unbounded stream

`ReconService.readConfirmations`

Reading "everything currently in a topic" and terminating: fresh per-run consumer group,
`assign` all partitions, `seekToBeginning`, snapshot `endOffsets` once, poll until every
partition reaches its snapshot, with a wall-clock deadline as backstop.

Three things make it correct: the **fresh group per run** (a re-run doesn't skip what a
prior group committed past), the **end-offset snapshot** (a topic in steady state is never
empty, so the bound is what makes it terminate), and the **deadline** (a broker that stops
advancing offsets cannot hang the job).

### 1.8 Idempotency that is real, not hopeful

`LoaderApp` + `TargetSystemMock`

The client sends `X-Idempotency-Key`; the server *remembers* it and returns `200` instead
of `201` on replay. That — and only that — is what makes at-least-once delivery safe.
Exponential backoff **with jitter** so a throttled batch doesn't retry in lockstep.

### 1.9 A mock that misbehaves on purpose

`TargetSystemMock` — injects 429/503 at a configurable rate, seeded for reproducibility

The retry and backoff paths are *executed on every run* rather than merely written. A
mock that always returns 200 tests nothing you were worried about.

Plus admin endpoints for testability: `/__admin/stats`, `/__admin/reset`, and a one-shot
`/__admin/suppress-next-confirmation` that manufactures a "sent but not persisted" gap
**deterministically, never in a real path** — so the negative-path acceptance criterion is
actually provable.

### 1.10 Separate the decidable core from the I/O

`ReconciliationMatcherTest` tests the set-difference logic with no Kafka and no BigQuery.

Anything shaped "fetch from two places, compare, decide" should have the *compare and
decide* half callable with two in-memory collections.

### 1.11 Hand-rolled REST over vendor SDKs — with a seam

`apps/common/`: `HttpObjectStore`, `BigQueryRest`, `GcpToken`

~200 lines of plain HTTP instead of the Google client tree, for mock/support apps where
the dependency cost exceeds the savings. **The value is the recorded reasoning plus the
`ObjectStore` interface as the seam** — swapping in `google-cloud-storage` is a one-class
change if the trade stops making sense. Do this consciously, document the ceiling.

### 1.12 Comments that record the incident, not the code

Throughout — `sinks.py`, `LoaderApp`, the DAG

The comments in this repo say *what went wrong, when, and what the error message looked
like*: "2026-08-23, json_producer on the DAG", "an error that names the symptom and not
the signature". Someone hitting the same log line can grep for it.

This is the highest-leverage convention in the repo and costs nothing.


---

## Part 2 — For devops / platform

### 2.1 GCP Managed Kafka OAUTHBEARER — the two non-obvious parts ⭐

`GcpTokenOauthCallbackHandler.java`, `sinks.py:_kafka_token`, `_oauth_token_cb`

Two failures that each cost a day:

1. **The broker does not accept a bare access token.** It wants a dot-joined base64url
   JWT-shaped value: `b64(header).b64(claims).b64(accessToken)` with
   `alg=GOOG_OAUTH2_TOKEN`, `scope=kafka`, `sub` = the service-account email. Sending the
   raw token fails with *"invalid credentials with SASL mechanism OAUTHBEARER"* — which
   names the mechanism, not the encoding.
2. **The JAAS login module is required even with a custom callback handler**, else Kafka
   fails with *"No login module found for OAUTHBEARER"*.

And on the Python side: `librdkafka` calls `oauth_cb` with the `sasl.oauthbearer.config`
string, so a **zero-arg callable raises TypeError inside the client's service thread**,
the token is never set, and the handshake dies with *"OAuth token not set within 10
seconds timeout"*. It also only services that queue on the first produce/poll — so
**prime the handshake with a `poll()` loop** before handing the producer a batch.

### 2.2 One env var gates local-vs-cloud transport

`KAFKA_SECURITY_PROTOCOL` — `PLAINTEXT` (redpanda, default) vs `SASL_SSL` (Managed Kafka)

The same binary runs in both worlds. **Omitting it is not a silent no-op**: a PLAINTEXT
client against a SASL_SSL-only broker hangs until its poll deadline and reports zero
records. It has to reach the *pods* — a Composer environment variable is invisible to a
`KubernetesPodOperator`, which passes only what `env_vars` names.

> Counter-lesson from the same repo: `ARCHITECTURE.md` weakness #2 flags **three
> independent "which world am I in" switches that can disagree**. One switch good; three
> switches is the next bug.

### 2.3 Never deploy a floating image tag

`composer/dags/mig_000001_1.py`

Tag images with the **git SHA** (`-dirty` appended for an unclean tree), and set
`image_pull_policy="Always"`. A floating tag means a rebuild silently changes what the DAG
runs and a past run cannot be reproduced. Kubernetes defaults to `IfNotPresent` for any
tag but `:latest`, so with a mutable SHA tag a node keeps serving the **old layer** and a
rebuilt fix silently does not take effect.

### 2.4 Don't freeze a credential into a pod spec

Same file

An access token lives ~1h; a DAG is long-lived. Freezing one into the pod spec is a
credential that expires mid-migration. Apps that talk raw HTTP don't pick up Workload
Identity automatically — so **fetch from the GKE metadata server and refresh**, with an
env-var override as the seam the local orchestrator injects through.

### 2.5 Composer pods must land in Composer's own namespace

Same file, `_pod_namespace()`

The Airflow worker runs as `system:serviceaccount:<composer-ns>:default` and Composer's
RBAC scopes it to its own namespace. Asking for `default` fails with *"pods is forbidden
… cannot list resource pods in the namespace default"*.

Read the namespace **from the file every pod carries**, not from an env var — Composer's
`--update-env-variables` silently drops some names and replaces the whole set.

### 2.6 Cold-start timeouts read like application failures

`startup_timeout_seconds=900`, not 600

On a cold Composer environment the pod waits for Autopilot to provision a node *and* pulls
a multi-GB image with `image_pull_policy=Always`. That exceeded 600s and failed as *"Pod
took longer than 600 seconds to start"* — an infrastructure timeout wearing a pipeline
failure's clothes. Warm clusters start in under a minute, so the higher ceiling only ever
costs time on the first run after a rebuild.

### 2.7 Give every app its own identity

`terraform/modules/iam`, per-app `service_account_name` on each pod

`dataflow-worker`, `loader-app`, `recon-service`, `target-system-mock` are separate SAs
with deliberately narrow roles (recon may only *read* BigQuery). Writing narrow roles is
wasted if every pod still runs as the shared account — **the pod-level
`service_account_name` is what puts them in force.**

### 2.8 Cost switches as first-class Terraform variables

`enable_kafka`, `enable_composer`; `count = var.enabled ? 1 : 0`; outputs return `""` when
disabled so callers can pass them through unconditionally

Managed Kafka is billed per vCPU-hour. The whole dependent chain — cluster, topics, VPC
connector, IAM grants — hangs off one flag, and a disabled apply produces nothing and
costs nothing.

> ⚠️ **And the trap:** the same flag pattern leaked into *behaviour*. An empty Kafka
> bootstrap makes recon skip the confirmation check and report `enabled=false` — "which
> keeps a no-Kafka run green". A cost switch that also **silently downgrades a
> correctness gate** is how a run passes having proved nothing. Keep cost switches out of
> assertion paths, or make them fail closed.

### 2.9 Serverless→VPC is not free

`terraform/modules/vpc_connector`

Managed Kafka is VPC-internal. Cloud Run cannot reach it without a Serverless VPC Access
connector. This was discovered *after* the mock was deployed and blocked an acceptance
criterion. **Check reachability at design time, not at test time.**

### 2.10 Evidence as a repo directory

`docs/evidence/<scenario>-<date>/`, `docs/evidence-map.md`

Terraform apply logs, pod logs, verification output, teardown logs — committed, dated, and
indexed by which acceptance criterion each proves. When someone asks "did this ever
actually work on real infrastructure", the answer is a path, not a memory.

### 2.11 Terraform inputs that are production-critical should not be optional

`ARCHITECTURE.md` weakness #6 — an open finding, listed here as a warning

Optional variables with plausible defaults turn an omission into a **runtime** failure
instead of a **plan-time** one. If a value has no default that could ever be right, give
it none.

### 2.12 Containerize the smoke test

`Dockerfile.toolbox`, `make smoke-gcp`

One tiny end-to-end run against real GCP, from a container that pins Python, Beam and the
JRE — so "works on my laptop" is not part of the result.

---

## Part 3 — Ways of working worth stealing

| Practice | Where | Why it pays |
|---|---|---|
| **`make help`** with `##` comments on every target | `Makefile` | one discoverable entry point per task; no README drift |
| **Dated change docs** (`PLAN-CHANGES-<date>.md`) that supersede in place | `docs/` | later docs annotate earlier ones instead of silently contradicting them |
| **Report field names are frozen on purpose** | `ReconService.Balance` | archived evidence stays comparable across releases; a rename is a breaking change |
| **A ranked weakness list with status markers** | `ARCHITECTURE.md` | "known and ranked" beats "unknown"; ✅/⚠️ markers show movement |
| **Local stack mirrors prod topology, not prod scale** | `local/docker-compose.yml` | fake-gcs + BQ emulator + redpanda + misbehaving mock — the *shape* is right |
| **Say what is mocked, up front** | `docs/production-readiness.md` §0 | nobody mistakes the prototype for the product |
| **C4 diagrams with every edge labelled *what flows* and *over what technology*** | `README.md`, `ARCHITECTURE.md` | unlabelled arrows hide exactly the decisions that matter |

---

## Top 5, if there is only time for five

1. **`.FLG` semaphore last** (1.2) — portable to any team, any storage, today.
2. **Check the return value of every async flush** (1.6) — this one silently lost data.
3. **Never default an identity field** (1.5) — this one silently lost data too.
4. **The balancing equation with an exit code** (1.3) — turns a report into a gate.
5. **Git-SHA image tags + `pull_policy: Always`** (2.3) — reproducibility for free.

---

## Appendix — the C4 diagrams, as files

The C4 model — C1 System Context through C4 Code — was generated as PlantUML sources and
rendered to SVG, one `.puml` + `.svg` pair per level, committed under
[`docs/plantuml/`](plantuml) and indexed in
[`docs/plantuml/README.md`](plantuml/README.md):

| Level | PlantUML source | Rendered SVG |
|---|---|---|
| C1 System Context | [`readme-03-c1-system-context.puml`](plantuml/readme-03-c1-system-context.puml) | [`readme-03-c1-system-context.svg`](plantuml/readme-03-c1-system-context.svg) |
| C2 Containers | [`readme-04-c2-containers.puml`](plantuml/readme-04-c2-containers.puml) | [`readme-04-c2-containers.svg`](plantuml/readme-04-c2-containers.svg) |
| C3 Components — File Processor | [`architecture-01-c3-file-processor.puml`](plantuml/architecture-01-c3-file-processor.puml) | [`architecture-01-c3-file-processor.svg`](plantuml/architecture-01-c3-file-processor.svg) |
| C3 Components — Recon Service | [`architecture-02-c3-recon-service.puml`](plantuml/architecture-02-c3-recon-service.puml) | [`architecture-02-c3-recon-service.svg`](plantuml/architecture-02-c3-recon-service.svg) |
| C4 Code — the two-door engine | [`architecture-03-c4-code-two-door-engine.puml`](plantuml/architecture-03-c4-code-two-door-engine.puml) | [`architecture-03-c4-code-two-door-engine.svg`](plantuml/architecture-03-c4-code-two-door-engine.svg) |

Regenerate them all with `./render.sh` — it needs only a JRE and `plantuml.jar`; the C4
diagrams use the C4-PlantUML stdlib with `layout smetana`, so no Graphviz is required.
