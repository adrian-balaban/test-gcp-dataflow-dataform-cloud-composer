# Mainframe → Target System MVP migration prototype

A working prototype (still WIP) that runs end-to-end against **real GCP provisioned with
Terraform** (Dataflow, BigQuery, Dataform, Cloud Composer).

This README doubles as a presentation — each section is one slide. Operator detail lives in
[`docs/`](docs/); the architecture analysis lives in
[ARCHITECTURE.md](ARCHITECTURE.md).

---

## Slide 1 — The problem

Migrate **n Million accounts / Billions of transactions** from Mainframe Db into **Target System**, verifiably.

```mermaid
flowchart LR
    DB[(Mainframe)] -->|Million accounts<br/> Billion transactions| P{{Migration<br/>pipeline}} --> VC[(Target System)]
    P -.-> R[Reconciliation<br/>reports]
```

- Real data & contracts from the other team land **2 months after our MVP deadline**.
- So: prove **our half** (Transformation + Reconciliation) against synthetic data and mocked
  contracts.

---

## Slide 2 — The pipeline, end to end

```mermaid
flowchart LR
    subgraph E["Extraction (Other Team)"]
        MF[Mainframe] --> XA[Extractor App] --> FS1[(File Storage)]
    end
    subgraph TR["Transformation + Reconciliation (Our Team, GCP)"]
        FS1 --> FP[Dataflow<br/>File Processor] --> BQ1[(BigQuery<br/>Extraction)]
        BQ1 --> DF[Dataform<br/>SQL Transformation] --> BQ2[(BigQuery<br/>Transformation)]
        BQ2 --> EN[Dataflow<br/>Data Enrichment] --> JP[Dataflow<br/>JSON Producer] --> FS2[(File Storage)]
        CC[Cloud Composer] -. orchestrates .-> FP & DF & EN & JP
        RS[Reconciliation<br/>Services] --> RPT[Migrability /<br/>Reconciliability reports]
    end
    subgraph L["Load (Other Team)"]
        FS2 --> LA[Loader App] -->|"produce<br/>target-system-target"| VC[Target System]
        VC -->|"confirmations<br/>+ rejections"| LA
    end
```

Every stage emits artefacts: `.DAT` `.CHS` `.ERR` `.RPT` `.FLG` — checksums and semaphores
so reconciliation never trusts a stage's word for it.

---

## Slide 3 — C1, System Context

C4 notation from here on: `Person`, `Software System`, `Container`, `External System`, every
relationship labelled *what flows* and *over what technology*.

```mermaid
graph TB
    subgraph people[" "]
        MO["<b>Migration Operator</b><br/><i>[Person]</i><br/>Triggers and monitors<br/>migration runs"]
        AUD["<b>Auditor / Data Owner</b><br/><i>[Person]</i><br/>Needs proof that every source<br/>record was accounted for"]
        ENG["<b>Migration Engineer</b><br/><i>[Person]</i><br/>Onboards a new project by<br/>editing contracts only"]
        SRE["<b>Platform / SRE Operator</b><br/><i>[Person]</i><br/>Provisions, scales and tears<br/>down the environment"]
        LDT["<b>Loader Team</b><br/><i>[Person]</i><br/>Owns the Load lane; consumes<br/>the TARGET JSON handoff"]
    end

    SYS["<b>Mainframe → Target System<br/>Migration Platform</b><br/><i>[Software System]</i><br/>Extracts mainframe TDS files, routes every record through<br/>the two-door engine (parse→map→schema),<br/>loads survivors into Target System and BigQuery,<br/>proves src_read == migrated + not_migrated,<br/>and reconciles the load against Target System's<br/>confirmation streams [JSON/protobuf, Kafka]"]

    MF["<b>Mainframe / Legacy Core</b><br/><i>[External System]</i><br/>System of record being<br/>migrated away from"]
    VC["<b>Target System</b><br/><i>[External System]</i><br/>Target banking core<br/>(mocked locally by target-system-mock)<br/>Consumes the account topic; publishes<br/>confirmation and rejection<br/>streams [JSON or protobuf]"]
    DS["<b>Downstream Consumers</b><br/><i>[External System]</i><br/>Consume enriched account<br/>JSON events"]
    PLAT["<b>Google Cloud Platform</b><br/><i>[External System]</i><br/>GCS, BigQuery, Dataflow,<br/>Composer, Dataform, Secret Manager"]
    SCM["<b>Source Control</b><br/><i>[External System]</i><br/>Holds contracts/, pipelines,<br/>Dataform models, Terraform"]
    KMS["<b>Key &amp; Secret Management</b><br/><i>[External System]</i><br/>PGP keypairs, Target System creds,<br/>Git tokens"]
    MON["<b>Monitoring &amp; Alerting</b><br/><i>[External System]</i><br/>Receives run outcomes and<br/>balance-failure alerts"]

    MO -->|"triggers a run, reads<br/>balance report [Airflow UI / CLI]"| SYS
    ENG -->|"adds TDS layouts, mappings,<br/>JSON Schemas [contracts/, Git]"| SYS
    AUD -->|"reads run_ledger, .RPT<br/>and recon report [BigQuery / GCS]"| SYS
    SRE -->|"provisions and tears down<br/>[Terraform, CLI]"| SYS
    SYS -->|"hands off TARGET JSON<br/>+ .RPT [GCS]"| LDT

    MF -->|"TDS pipe-delimited CSV files +<br/>.FLG/.CHS artefacts [PGP over SFTP/GCS]"| SYS
    SYS -->|"publishes accounts, idempotent<br/>[Kafka, key = dedupKey]"| VC
    VC -->|"confirmation and rejection events<br/>as streams [JSON or protobuf, Kafka]"| SYS
    SYS -->|"publishes enriched<br/>account JSON [Kafka]"| DS
    SYS -->|"runs pipelines, stores state<br/>and evidence [GCP APIs]"| PLAT
    SCM -->|"contracts, pipeline code,<br/>IaC [Git]"| SYS
    KMS -->|"decryption keys,<br/>credentials [Secret Manager]"| SYS
    SYS -->|"run outcome, balance<br/>failures [logs / metrics]"| MON

    classDef person fill:#08427b,stroke:#052e56,color:#fff
    classDef system fill:#1168bd,stroke:#0b4884,color:#fff
    classDef ext fill:#999999,stroke:#6b6b6b,color:#fff
    class MO,AUD,ENG,SRE,LDT person
    class SYS system
    class MF,VC,DS,PLAT,SCM,KMS,MON ext
    style people fill:none,stroke:none
```

**Reading it.** Five actors, seven external systems. Three points prose does not make:

- **The Auditor is a first-class actor with a read-only relationship to evidence**
  (`run_ledger`, `.RPT`, recon report). That relationship is exactly what weakness #5 in
  [ARCHITECTURE.md](ARCHITECTURE.md) degrades — today the auditor can read
  *counts*, not *which records*.
- **C1 is the specification, not a picture of this repo.** The platform is GCP and stays GCP;
  what varies is the runtime *inside* the boundary. Today that is Beam on Dataflow.
- **Reconciliation with Target System is a return edge — now wired.** Target System publishes
  confirmation/audit events back to the platform as streams (JSON or protobuf, over Kafka),
  consumed to reconcile the load. The prototype wires this today: the mock publishes one
  confirmation per accepted write, recon matches it against `account_target`, and an unconfirmed
  row fails the run (Slide 7).

---

## Slide 4 — C2, Containers

Zooming inside the platform boundary. Each box is a separately deployable/runnable unit.

```mermaid
graph TB
    MO["<b>Migration Operator</b><br/><i>[Person]</i>"]
    MF["<b>Mainframe / Legacy Core</b><br/><i>[External System]</i>"]
    VC["<b>Target System</b><br/><i>[External System]</i>"]
    DS["<b>Downstream Consumers</b><br/><i>[External System]</i>"]

    subgraph BOUNDARY["Mainframe → Target System Migration Platform"]
        direction TB

        DAG["<b>Migration Orchestrator</b><br/><i>[Container: Airflow DAG on Cloud Composer]</i><br/>composer/dags/mig_000001_1.py — sequences the lanes,<br/>runs assert_run_balanced against run_ledger"]

        CONTRACTS["<b>Contract Store</b><br/><i>[Container: YAML / JSON Schema in Git]</i><br/>contracts/ — TDS layouts (pipe-delimited CSV), field mappings, JSON Schemas,<br/>reference data. The only thing edited to onboard a project"]

        EXT["<b>Extractor App</b><br/><i>[Container: Java, Maven]</i><br/>apps/extractor-app — pulls TDS + artefacts from the<br/>mainframe lane, writes .FLG/.CHS/.ERR/.RPT"]

        FP["<b>File Processor Pipeline</b><br/><i>[Container: Apache Beam / Python — Direct or Dataflow runner]</i><br/>pipelines/file_processor — the two-door engine:<br/>parse→map→schema; writes run_ledger"]

        EN["<b>Data Enrichment Pipeline</b><br/><i>[Container: Apache Beam / Python]</i><br/>pipelines/data_enrichment — joins reference data,<br/>derives curated attributes"]

        JP["<b>JSON Producer Pipeline</b><br/><i>[Container: Apache Beam / Python]</i><br/>pipelines/json_producer — emits schema-valid<br/>account JSON to GCS and Kafka"]

        LOAD["<b>Loader App</b><br/><i>[Container: Java, Maven]</i><br/>apps/loader-app — publishes survivors to Target System<br/>keyed on accountKey, settles against the<br/>confirmation + rejection streams, writes .RPT"]

        RECON["<b>Recon Service</b><br/><i>[Container: Java, Maven]</i><br/>apps/recon-service — rebuilds the balancing equation,<br/>consumes Target System confirmation streams,<br/>renders the reconciliation report"]

        DF["<b>Curation Transforms</b><br/><i>[Container: Dataform SQLX on BigQuery]</i><br/>dataform/definitions — account_src → account_curated"]

        MOCK["<b>Target System Mock</b><br/><i>[Container: Java — HTTP + Kafka consumer]</i><br/>apps/target-system-mock — stands in for Target System;<br/>consumes the target topic, publishes confirmations<br/>and rejections back"]

        LAKE[("<b>Object Store</b><br/><i>[Container: GCS — landing, json-out,<br/>recon, dataflow-temp, dataflow-templates]</i><br/>Files and artefacts on both lanes")]

        BQ[("<b>Analytics Warehouse</b><br/><i>[Container: BigQuery]</i><br/>account_src, account_curated,<br/>run_ledger — the evidence tables")]

        BUS["<b>Event Bus</b><br/><i>[Container: Kafka / Redpanda]</i><br/>Enriched account JSON (out);<br/>Target System confirmation streams (in)"]

        SEC["<b>Secret Store</b><br/><i>[Container: Secret Manager]</i><br/>PGP keys, Git tokens, endpoints"]
    end

    MO -->|"triggers / monitors [Airflow UI]"| DAG
    MF -->|"TDS + artefacts [PGP over SFTP]"| EXT

    DAG -->|"launches [KubernetesPodOperator]"| FP
    DAG -->|"launches"| EN
    DAG -->|"launches"| JP
    DAG -->|"launches"| EXT
    DAG -->|"launches"| LOAD
    DAG -->|"launches"| RECON
    DAG -->|"invokes compile+run [Dataform]"| DF
    DAG -->|"assert_run_balanced<br/>[parameterized SQL]"| BQ

    CONTRACTS -.->|"layouts, mappings, schemas<br/>[read at startup]"| FP
    CONTRACTS -.->|"reference data"| EN
    CONTRACTS -.->|"JSON Schemas"| JP
    CONTRACTS -->|"artefact naming + BQ columns<br/>[artefacts.json, read at startup]"| EXT

    EXT -->|"writes TDS + .FLG/.CHS/.ERR/.RPT"| LAKE
    LAKE -->|"reads decrypted TDS"| FP
    FP -->|"writes rows + run_ledger<br/>[load jobs / insertAll]"| BQ
    FP -->|"writes artefacts"| LAKE
    BQ -->|"reads account_src"| EN
    EN -->|"writes enriched rows"| BQ
    BQ -->|"reads curated"| JP
    JP -->|"writes account JSON"| LAKE
    JP -->|"publishes"| BUS
    BUS -->|"consumes"| DS
    BUS -->|"confirmations [JSON/protobuf]"| RECON
    LAKE -->|"reads account JSON"| LOAD
    LOAD -->|"upsert account<br/>[HTTPS/JSON]"| VC
    VC -->|"confirmation / audit events<br/>[JSON or protobuf]"| BUS
    LOAD -->|"upsert account [HTTPS/JSON — local]"| MOCK
    LOAD -->|"writes .RPT"| LAKE
    BQ -->|"run_ledger + record_lineage<br/>+ key-level joins [BigQuery REST]"| RECON
    RECON -->|"writes reconciliation report"| LAKE
    DF -->|"transforms in place [SQLX]"| BQ
    SEC -.->|"PGP keys, credentials"| EXT
    SEC -.->|"credentials"| FP
    SEC -.->|"Git token"| DF

    classDef person fill:#08427b,stroke:#052e56,color:#fff
    classDef ext fill:#999999,stroke:#6b6b6b,color:#fff
    classDef container fill:#438dd5,stroke:#2e6295,color:#fff
    classDef store fill:#438dd5,stroke:#2e6295,color:#fff
    class MO person
    class MF,VC,DS ext
    class DAG,CONTRACTS,EXT,FP,EN,JP,LOAD,RECON,DF,MOCK container
    class LAKE,BQ,BUS,SEC store
    style BOUNDARY fill:#f5f5f5,stroke:#cccccc
```

**Reading it.** Three arrows carry the honest bad news, and each is a known weakness:

- The **dotted `Contract Store → Extractor App` arrow** used to be labelled "by convention
  only" — a comment promising two languages agreed on artefact names. Both now load
  `contracts/artefacts.json`, so that promise is a file; what remains by convention is the
  reject taxonomy, which exists only in Python.
- **Two arrows into BigQuery once carried the same fact** — `File Processor → run_ledger` and
  `BigQuery → Recon Service (COUNT(*))`, the equation written once and re-derived once. Recon
  now reads the ledger and the per-record register and fails the run if they disagree, rather
  than re-deriving a second answer.
- **`Loader App` fans out to both Target System and the mock**, and the Beam pipelines run on
  Direct *or* Dataflow. Which edge is live used to be decided by three independent env
  switches; it is now one resolved `Backend`, and the combinations that cannot mean anything
  are rejected at startup.

Beyond those three weaknesses, C2 also shows the **reconciliation-with-Target-System return
path** — now wired: Target System publishes confirmation/audit events (JSON or protobuf) back
to the Event Bus, and the Recon Service consumes them to match against the load. A TARGET row
with no matching confirmation fails the run (Slide 7, [`docs/PLAN-CHANGES-22082026.md`](docs/PLAN-CHANGES-22082026.md)).

Deeper views — **C3** (components inside the File Processor and the Recon Service) and **C4** (the two-door engine's
types) — are in
[ARCHITECTURE.md](ARCHITECTURE.md#c3--components-file-processor-pipeline).

---

## Slide 5 — What we actually built (every box has a deliverable)

| Element in architecture diagram | Here | Stack |
|---|---|---|
| Mainframe | `harness/` | Python |
| Extractor App *(mock)* | `apps/extractor-app` | Java |
| File Storage | fake-gcs-server → GCS | — |
| Dataflow File Processor | `pipelines/file_processor` | Beam Python |
| BigQuery Extraction / Transformation | `bq_extraction`, `bq_transformation` | BQ |
| Dataform SQL Transformation | `dataform/definitions/*.sqlx` | SQLX |
| Dataflow Data Enrichment | `pipelines/data_enrichment` | Beam Python |
| Dataflow JSON Data Producer | `pipelines/json_producer` | Beam Python |
| Cloud Composer | `composer/dags/` (migration DAG) | Airflow |
| Reconciliation Services | `apps/recon-service` | Java |
| Loader App *(mock)* | `apps/loader-app` | Java |
| Target System *(mock)* | `apps/target-system-mock` | Java |

Mocks honour the stated contracts → the full chain runs **without waiting for real data**.

---

## Slide 6 — Two doors + the balancing equation

Every record leaves through **exactly one door**. Two dispositions, two doors, and the
equation closes per run *and* per batch:

```mermaid
flowchart LR
    SRC[SRC record] --> D{Router}
    D --> W["✅ written"]
    D --> RJ["rejected<br/>enumerated reason code"]
    W --> M(["Door 1 — migrated"])
    RJ --> NM(["Door 2 — not migrated"])
```

```
src_read     = migrated + not_migrated
migrated     = written
not_migrated = rejected
```

**A run that does not balance fails the DAG.** Proved by `make verify`; persisted per run in
`bq_recon.run_ledger`.

And the rejected door is not only counted — every record that leaves through door 2 is
**named** in `bq_recon.record_lineage` with its source key, door, stage and enumerated reason,
so "why was this account not migrated?" is answerable per record, not just per tally:

```sql
SELECT source_key, door, stage, reason, detail
FROM `<project>.bq_recon.record_lineage`
WHERE run_id = 'initial-…' AND door = 'rejected'
-- ACC000000007 | rejected | parse | PARSE_BAD_NUMERIC | non-numeric BALANCE
```

---

## Slide 7 — Reconciliation with Target System

Slide 6 closes our *internal* books — `src_read = migrated + not_migrated`. Reconciliation
**with Target System** is the layer on top: proving Target System actually accepted and persisted
what the loader handed off. That proof does not come from the loader's HTTP response — it
comes back from Target System as **streams**, which the reconciliation side consumes and matches
against the load.

1. Target System publishes its confirmation/audit events as **streams of data**, in **JSON or
   protobuf** format, on **Kafka topics** — consumed by the reconciliation side and matched
   against what the loader sent.

The prototype now wires this. On every accepted write (HTTP `201 Created`) the Target System
mock publishes a confirmation event `{runId, accountId, accountKey, confirmedAt}` to a second
Kafka topic (`target-system-confirmations`). The recon service reads that topic with a fresh
per-run consumer group (`recon-<runId>`, `auto.offset.reset=earliest`), takes the set of
confirmed `accountKey`s, and set-differences it against the TARGET rows in `account_target`. A
TARGET row whose key has no matching confirmation is **sent but not persisted** — the run fails
with `RECONCILIATION FAILED: target system reconciliation GAP — N of M TARGET rows unconfirmed`
and exits non-zero. A no-Kafka run (no confirmation bootstrap configured) skips cleanly rather
than reading "zero confirmations" as a gap, so `make smoke-gcp --sinks gcs` stays green.

This is acceptance criterion 9 — the only criterion that checks an *external* system's own
claim, not an internal invariant. The negative path is exercised through the mock's
`/__admin/suppress-next-confirmation` one-shot hook, which makes one accepted write publish no
confirmation; recon then names the unconfirmed `account_key` and fails the run. See
[`docs/PLAN-CHANGES-22082026.md`](docs/PLAN-CHANGES-22082026.md) for the build sequence.

> **Scale caveat.** Recon's confirmation-topic read is a full scan per run
> (`auto.offset.reset=earliest`, fresh group every time). Fine at prototype volume (≈500
> rows), this is the same category of "works at prototype scale, named explicitly as not yet
> real-scale-ready" as §2.1 and §2.4 of [`production-readiness.md`](docs/production-readiness.md):
> a bounded/incremental read (seek-by-timestamp to the run's start, or a compacted topic keyed
> by `account_key`) is needed before real volume.

---

## Slide 8 — TDS carries `.DAT` or JSON

Reason: to extend the actual system.

```mermaid
flowchart LR
    TDS1[/"TDS definition — pipe-delimited .DAT<br/>tds-src.def"/] --> H["header row:<br/>ACCT_ID|CUST_ID|…|BALANCE"]
    TDS1 --> F1["ACCT_ID · col=0"]
    TDS1 --> F2["CLIENT_TYPE · col=2"]
    TDS1 --> F3["BALANCE · col=7"]
    TDS2[/"TDS definition — JSON<br/>tds-src-json.def"/] --> J1["$.accountId"]
    TDS2 --> J2["$.clientType"]
    TDS2 --> J3["$.balance"]
    F1 & F2 & F3 --> P[One parser] --> OUT[Typed record]
    J1 & J2 & J3 --> P
```

- A definition is **homogeneous**: one flat `.DAT` layout (the source feed is pipe-delimited
  with a header row) *or* JSONPath for nested JSON — one format per definition, never mixed.
- `.DAT` values are **loaded raw and normalized in the transform** — trailing spaces and
  leading zeros are stripped by explicit `transform:` rules in the mapping, not at parse.
  Full physical spec: [`contracts/README.md`](contracts/README.md).
- SRC and TARGET are **separate definitions**; output validated against TARGET JSON Schema.

---

## Slide 9 — Extend, not rewrite

Reason: five more projects to come; a new project should touch **only `contracts/`** (config
files, not code):

```mermaid
flowchart LR
    subgraph contracts["contracts/ — the only thing a project edits"]
        Y1[mapping-project1.yaml]
        Y2[mapping-project2.yaml]
        T1[TDS defs]
        S1[JSON Schemas]
    end
    subgraph engine["Engine — zero code diff"]
        PIPE[Beam pipelines · Dataform · recon]
    end
    Y1 --> PIPE
    Y2 --> PIPE
    T1 --> PIPE
    S1 --> PIPE
```

Enforced mechanically: `make verify-project2` runs a second, deliberately different project
with a **zero-line engine diff**.

---

## Slide 10 — One full snapshot per run

How the data arrives from source.

```mermaid
flowchart LR
    S["Source snapshot<br/>all accounts, once"] --> R["Run<br/>run_id=R1"]
    R -. scoped recon .-> RR["Report R1"]
    RP["Replay<br/>same run_id=R1"] --> RT["Replaces R1's rows,<br/>leaves every other run untouched"]
```

- The source delivers **one full snapshot** per run — every account, every time. No
  initial/delta distinction, no from/to window.
- Replays are idempotent by `run_id`: a re-run with the same id **replaces** that run's rows
  (count-then-DELETE per `run_id`) and touches no other run. Restart costs ≤ one run.
- Proved by re-running the same `run_id` and checking the run still balances with no duplicate
  rows.

---

## Slide 11 — Real GCP architecture implemented

```mermaid
flowchart LR
    subgraph gcp["GCP — applied & run (europe-west1)"]
        GCS[(GCS<br/>landing · artefacts)]
        DFL[Dataflow<br/>file_processor · enrichment · json_producer]
        BQ[(BigQuery<br/>extraction · transformation · recon)]
        DF[Dataform<br/>SQL transformation]
        KFK[Managed Kafka]
        CMP[Cloud Composer 2<br/>migration DAG]
        VCM[Cloud Run<br/>target-system-mock]
    end
    CMP -. "KubernetesPodOperator pods" .-> DFL
    CMP -. "Dataform operators" .-> DF
    GCS -->|landing<br/>input| DFL --> BQ
    BQ --> DF --> BQ
    DFL -->|TARGET JSON<br/>artefacts| GCS
    DFL -. "optional sink" .-> KFK
    GCS -->|TARGET JSON| VCM
```

- **Applied and run** on a real project in `europe-west1`: full E → T+R → L, most recently
  run `initial-20260818-165241` — 76 records read end-to-end and **40 migrated to Target
  System**, every not-migrated record named in `record_lineage`, the loader accepting all 40
  documents against the Target System stand-in. Earlier evidence in
  [`docs/evidence/gcp-run-20260804/`](docs/evidence/gcp-run-20260804/).
- **The full DAG runs green on Cloud Composer 2.** Run `manual__2026-08-18T21:49:29+00:00`:
  all 8 tasks succeeded — the semaphore sensor, three Beam pipelines as **real Dataflow
  jobs** launched from `KubernetesPodOperator` pods, the Dataform pod, loader, recon, and
  the `assert_run_balanced` gate. `run_ledger` closed at 76 = 42 migrated + 34 not migrated,
  with 36 `record_lineage` rows naming every one (the extra 2 are json_producer's map/schema
  rejects — the equation closes across the lane, not inside one process). Captured in
  [`docs/evidence/gcp-composer-20260818/`](docs/evidence/gcp-composer-20260818/), including
  the three failures that only the DAG path could expose — and re-run green after two review
  passes in [`docs/evidence/gcp-composer-20260819/`](docs/evidence/gcp-composer-20260819/),
  where the loader and recon tasks first ran under their own narrow service accounts.
  The most recent pass,
  [`docs/evidence/gcp-composer-20260820/`](docs/evidence/gcp-composer-20260820/),
  re-ran green on a fresh extract (`initial-20260821-003500`, run
  `manual__2026-08-20T20:33:28+00:00`) and closed the BigQuery streaming-buffer defect:
  `file_processor` now counts before it deletes, so a first run of a brand-new run id no
  longer fails a DML statement that matches zero rows.
  After the two-door simplification (WRITTEN/REJECTED, one snapshot per run) and the
  "Vault Core" → "Target System" rename, the current HEAD was re-verified 2026-08-22 by
  `make smoke-gcp` 8/8 green against real GCS/BigQuery — see
  [`docs/evidence/gcp-smoke-20260822/`](docs/evidence/gcp-smoke-20260822/).
  The newest pass,
  [`docs/evidence/gcp-composer-20260823/`](docs/evidence/gcp-composer-20260823/), is the one
  that proves Slide 7's claim rather than describing it: a full DAG run with Managed Kafka
  enabled, all 8 tasks green, and **criterion 9 closing on live confirmations** — 50 of 50
  TARGET rows corroborated by Target System's own stream, consumed over SASL_SSL/OAUTHBEARER
  from inside the VPC. Getting there cost five fixes that only real GCP could surface; they
  are itemised in that bundle and in `docs/runbook-gcp.md` §B8.
  Earlier evidence in
  [`docs/evidence/gcp-composer-20260805/`](docs/evidence/gcp-composer-20260805/).
- BigQuery sandbox is free and reachable: `BQ_TARGET=real` runs Dataform against real BQ.
- The expensive resources (Composer, Kafka) are behind feature flags, default **off**, and torn
  down between runs — cost and teardown detail in
  [`docs/runbook-gcp.md`](docs/runbook-gcp.md#composer--create-cost-teardown).

---

## Slide 12 — What it proves (acceptance criteria, not demo)

| | Property | Proved by |
|---|---|---|
| 1 | Two doors + balancing equation, per run *and* per batch | `make verify` |
| 2 | TDS carries `.DAT` or JSON (homogeneous definitions) | `make verify` |
| 3 | Extend, not rewrite — zero engine diff for project2 | `make verify-project2` |
| 4 | Idempotent replays by `run_id` (re-run replaces, never duplicates) | re-run same `run_id` → `make verify` |

These are the four **must-proves**. `make verify` checks them through **10 acceptance criteria**
in `tests/acceptance.py`: the balancing equation closes, seeded malformed records carry the
right reason codes, every TARGET document validates against the JSON Schema, TARGET is emitted
in 200-element batches, every key appears exactly once, every rejected record is named in
`record_lineage` and agrees with the ledger, all five artefact types are present in both lanes,
`.CHS` checksums verify on both sides, every TARGET row is confirmed by Target System
(criterion 9 — the Target System confirmation stream [`docs/PLAN-CHANGES-22082026.md`](docs/PLAN-CHANGES-22082026.md)
adds; the only criterion that checks an external system's own claim, not an internal invariant),
and every document the Loader published came back settled — confirmed, duplicate or rejected
(criterion 10, added with the Kafka Load edge in
[`docs/PLAN-CHANGES-02092026-kafka-loader.md`](docs/PLAN-CHANGES-02092026-kafka-loader.md);
a produce ack proves the broker holds the bytes, not that Target System applied them).

---

## Running it

```bash
make bootstrap && make up && make init-infra   # local stack, no GCP account
make java-build && make run-initial
make verify                                    # all 10 criteria must pass
```

`make help` prints the same sequence as its "typical first run" line — if the two ever
disagree, the Makefile is right.

| I want to… | Read |
|---|---|
| Prove it on my laptop, no cloud account | [`docs/runbook-gcp.md` §A](docs/runbook-gcp.md#a-local-stack-no-billing-no-gcp-account) |
| Provision it on GCP with Terraform | [`docs/runbook-gcp.md` §B](docs/runbook-gcp.md#b-real-gcp-with-terraform) |
| Retarget the repo at my own account / laptop | [`docs/setup-gcp.md`](docs/setup-gcp.md) |
| Find the evidence in the GCP console | [`docs/evidence-map.md`](docs/evidence-map.md) |
| Operate Composer / control cost | [`docs/runbook-gcp.md`](docs/runbook-gcp.md#composer--create-cost-teardown) |
| Understand the architecture and its weaknesses | [ARCHITECTURE.md](ARCHITECTURE.md) |
| Know what stands between this and the real migration | [`docs/production-readiness.md`](docs/production-readiness.md) |

This repo is public, so **no account identifier is committed**: project id, region and billing
account are read from your shell, and the service-account key directory is gitignored. If you
find a hardcoded project id anywhere, that is a bug.
