# Verification of the all-sides review against a real GCP deployment

The "All-sides review" (2026-08-04) was checked against (a) direct inspection of the code
it cites and (b) **what actually happened when the stack was deployed and run on real GCP**
— see [`README.md`](README.md) in this directory for the run itself.

**Headline: the review is accurate.** Every claim I could check held up. Two of its findings
were _independently hit as real blockers_ during the deployment, which is the strongest
possible confirmation. Its central quantitative claim — "the GCP path is wired ~80%" — is
the one thing this run refines: see [Where the review was too pessimistic](#where-the-review-was-too-pessimistic).

---

## High findings

| #      | Claim                                                                                                  | Verdict                                                          | Evidence                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| ------ | ------------------------------------------------------------------------------------------------------ | ---------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **H1** | Billing-account IDs + project IDs committed in a public repo, contradicting README's promise           | **CONFIRMED**                                                    | `docs/PLAN.md:14` contains `011D72-A178C9-8B2F0C`, `01756E-08E9EB-46CAC6`, `alinierecalendare`, `testproject-501611`; `PLAN.md:134` repeats `alinierecalendare`. README:249 does promise "no account identifier is committed".                                                                                                                                                                                                                                       |
| **H2** | SQL injection in the Java recon app via raw CLI-arg interpolation                                      | **CONFIRMED**                                                    | `ReconService.java:50`: `String q = "'" + a.runId + "'";` then interpolated into every `bq.count`/`bq.query` through :105. The DAG's `assert_run_balanced` does use `ScalarQueryParameter`, so the Java side is a genuine regression against the Python precedent.                                                                                                                                                                                                   |
| **H3** | Loader defaults `dedupKey` to `""`; mock accepts it as a valid key → silent data loss                  | **CONFIRMED (code)**, **and the failure mode was observed live** | `LoaderApp.java:159-160` defaults `accountId` to `"<missing>"` and `dedupKey` to `""`. Live corroboration: a first run against a _stale_ mock reported `accepted: 0, duplicatesIgnored: 40` — 40 real documents silently swallowed as "duplicates" with a zero exit code and a `.FLG` claiming success. That is exactly H3's failure shape (different trigger: stale idempotency state rather than an empty key), and it proves the class of bug is not theoretical. |
| **H4** | `BigQueryRest` never checks `jobComplete`                                                              | **CONFIRMED**                                                    | No occurrence of `jobComplete` anywhere in `BigQueryRest.java`. A timed-out query returning 0 rows would silently pass the balancing equation.                                                                                                                                                                                                                                                                                                                       |
| **H5** | DAG's GCP mode leaves L+R lanes as BashOperators on non-existent paths                                 | **CONFIRMED**                                                    | `composer/dags/mig_000001_1.py:230-242` — `loader` and `reconciliation` use `_local_task(...)` pointing at `apps/*/build/install/...` unconditionally, outside the `if EXECUTION_MODE == "dataflow"` branches. Those paths don't exist on a Composer worker.                                                                                                                                                                                                         |
| **H6** | No `serviceAccountTokenCreator`/`serviceAccountUser` grant, so `composer-runner` can't launch Dataflow | **CONFIRMED**                                                    | Neither role appears anywhere in `terraform/modules/iam/main.tf`. This is why the deployed DAG was **not** triggered in this run — it would fail at Dataflow submission.                                                                                                                                                                                                                                                                                             |
| **H7** | Dataform linked-repo path unwired; `dataform-git-token` seeded into a secret nothing reads             | **CONFIRMED**                                                    | `git_token_secret_version` is declared and consumed in `terraform/modules/dataform/main.tf:22,41` but never passed from `terraform/envs/dev/main.tf:161-168`. Live corroboration: `make deploy-dataform` took the unlinked CLI path, and `make seed-secrets` reported `⚠ DATAFORM_GIT_TOKEN not set — skipping`.                                                                                                                                                     |
| **H8** | `env_file` output omits `KAFKA_BOOTSTRAP_SERVERS`                                                      | **CONFIRMED**                                                    | The apply's own output shows `KAFKA_SECURITY_PROTOCOL=SASL_SSL` with no bootstrap line, even though a separate `kafka_bootstrap` output exists and resolved to `bootstrap.mig-kafka.…:9092`. The operator must copy it by hand — which is exactly what I had to do to run the flow.                                                                                                                                                                                  |

## Medium findings

| #       | Claim                                                                              | Verdict                                                            | Evidence                                                                                                                                                                                                                                                                                              |
| ------- | ---------------------------------------------------------------------------------- | ------------------------------------------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **M1**  | Two divergent dedup survivor selections (first-wins vs lexicographically-smallest) | **CONFIRMED**                                                      | `engine.py:58` keeps the first occurrence (`if dedup_key in seen`); `file_processor/pipeline.py:260,264` sorts payloads and takes `ordered[0]`. Different survivor on reordered replay — the review's sharper framing is right.                                                                       |
| **M2**  | Per-batch balancing only proven in the in-memory path                              | **CONFIRMED**                                                      | `batch_id=0` hardcoded at `pipeline.py:220` and `"_batch_id": 0` at :325; `require_balance` called once per run from aggregate metrics.                                                                                                                                                               |
| **M3**  | Real-GCP BigQuery still uses `insertAll`                                           | **CONFIRMED, and exercised**                                       | This run wrote to real BigQuery through `insertAll` (`storage.py`), not `WriteToBigQuery`/FILE_LOADS. Fine at 76 rows; wrong at 20B.                                                                                                                                                                  |
| **M4**  | Whole source table loaded into the driver                                          | **CONFIRMED (by design of the current source)**, untested at scale | Not falsifiable at 76 records; the code path is as described.                                                                                                                                                                                                                                         |
| **M5**  | Kafka OAUTHBEARER has no token callback                                            | **CONFIRMED, unresolved**                                          | `sinks.py` sets the mechanism with no `sasl_login_callback_handler`. **Not exercised**: Managed Kafka's bootstrap is VPC-only and returns `NXDOMAIN` from an operator laptop, so this run used `--sinks gcs`. The cluster + topic were created and ACTIVE; the moment a client runs in-VPC, M5 bites. |
| **M6**  | GCS list not paginated (Java)                                                      | **CONFIRMED**                                                      | No `pageToken` in `HttpObjectStore.java`.                                                                                                                                                                                                                                                             |
| **M7**  | No retries in Java `HttpObjectStore`/`BigQueryRest`                                | **CONFIRMED**                                                      | `send()` wraps a single `http.send` with no retry loop.                                                                                                                                                                                                                                               |
| **M8**  | `createBucket` hardcodes `project=mig-local`                                       | **CONFIRMED — hit for real, and fixed**                            | This was a live blocker: the extractor's `createBucket` had to work against `mig-000001-1-dev`. Now reads `MIG_GCS_PROJECT`.                                                                                                                                                                          |
| **M9**  | `make test` silently skips Java tests when `gradlew` is absent                     | **CONFIRMED**                                                      | `Makefile:116` — `else echo "(gradle wrapper not yet generated — skipping Java tests)"`, exit 0.                                                                                                                                                                                                      |
| **M10** | `verify_project2.sh` doesn't fingerprint `dataform/definitions/`                   | **CONFIRMED**                                                      | `ENGINE_PATHS=(pipelines apps)` at line 19.                                                                                                                                                                                                                                                           |
| **M11** | SQLX has no partitioning/clustering                                                | **CONFIRMED**                                                      | Zero occurrences of `partitionBy`/`clusterBy` in either `.sqlx`.                                                                                                                                                                                                                                      |
| **M12** | PGP error drops gpg stderr                                                         | **CONFIRMED in Java, and the same class of bug bit in Python**     | `Pgp.java:65-69` as described. Independently, `pipelines/common/pgp.py` referenced a non-existent `ImportResult.status`, masking the real gpg reason with an `AttributeError` — same failure mode, different file. Fixed in this run.                                                                 |
| **M13** | Images pinned to `:latest`                                                         | **CONFIRMED**                                                      | `build_templates.sh:60` — `image="${REGISTRY}/${template}:latest"`. The three published images are `:latest`, so a rebuild silently changes what a "stable" template runs.                                                                                                                            |
| **M14** | `exists()` treats 403 as "not found"                                               | **CONFIRMED**                                                      | `HttpObjectStore.java:147` — `.statusCode() == 200`.                                                                                                                                                                                                                                                  |

## Low findings

Spot-checked, all consistent with the code: **L1** (f-string `run_id` in `file_processor`/
`acceptance.py`), **L2** (`cfg.__class__(**{**cfg.__dict__…})`), **L3** (`.env` substring
parse for the layout), **L4** (`architecture diagram` in both the root and `docs/inputs/`), **L9**
(Jackson/JUnit/commons-compress pinned by hand in both `build.gradle` and `pom.xml`, no
drift check). Not individually re-derived: L5–L8, L10–L13 — all are code-shape claims in
files whose surrounding claims proved accurate.

## Strengths — independently corroborated by the run

The review's "strengths worth preserving" list is not flattery; the run substantiates it:

- **The balancing equation is load-bearing.** It closed on real infrastructure
  (`76 = 40 + 10 + 6 + 20`), was recorded in `bq_recon.run_ledger` by the pipeline, _and_
  independently recomputed by the Java recon service from the upstream's own number.
- **All six enumerated reject reasons fired exactly once** — the equation is closing over
  genuinely distinguishable failure modes, not an aggregate that happens to add up.
- **Java idempotency + retry is real.** With a clean mock: 43 HTTP requests for 40
  documents, 3 injected 429/503s recovered by backoff, `accepted: 40`,
  `duplicatesIgnored: 0`, `errors: 0`. Exactly as claimed.
- **The local↔GCP seam is thin — but not free.** `storage.py`/`runner.py` did carry the
  Python side to real GCS/BigQuery unmodified. The _Java_ side needed a new auth seam
  (`fromEnv` + `MIG_GCS_TOKEN`), which the review's H-list implies but does not state
  outright.
- **Terraform cost-gating works as advertised.** `enable_composer`/`enable_kafka` flipped
  on cleanly (5 resources added, nothing else touched) and off again (5 destroyed, nothing
  else touched) — verified against the plan JSON before applying.

## Where the review was too pessimistic

> "the L+R lanes, the service-account impersonation grant, the Kafka bootstrap wiring, and
> the Dataform linked-repo secret are unfinished or missing, **so a real terraform apply +
> DAG run would fail before producing data**."

Split this in two:

- **A real DAG run: correct.** H5 + H6 would block it. The DAG was deployed but
  deliberately not triggered for exactly those reasons.
- **A real `terraform apply` + data production: too pessimistic.** The apply succeeded
  (5/5 resources, Composer RUNNING in 17m51s, Kafka ACTIVE in 11m9s), and the full chain
  **did produce data on real GCP** — 40 schema-valid Target System documents, all 8 acceptance
  criteria green, via the `run_pipeline.py` orchestrator rather than Composer. The review
  conflates "the Composer path is incomplete" with "the GCP path can't produce data"; the
  second orchestrator that REVIEW.md flags as duplication debt is precisely what made this
  run possible.

The honest correction to "~80% wired": the **data path** to real GCP reached 100% after
fixing 11 defects; the **Composer orchestration path** is the part that remains ~80%.

## Defects the review missed

Found only by deploying — no amount of reading would have surfaced these. All fixed and
committed (`e84dff6`); full table in [`README.md`](README.md):

1. Secret Manager now **rejects empty payloads** — `pgp-passphrase` seeding failed hard.
2. `gnupg.import_keys(passphrase="")` **corrupts the import** ("no valid OpenPGP data"),
   and `ImportResult` has no `.status`, so the diagnostic masked itself (M12's cousin).
3. `terraform output -raw` **refuses map-typed outputs** — `build_templates.sh` crashed.
4. The root `.dockerignore` **excludes `pipelines/`** (it serves the Java image), so the
   Beam Flex Template build failed on `COPY pipelines/`.
5. `gcloud storage rsync` **has no `--delete`** (it's
   `--delete-unmatched-destination-objects`) and treats `--exclude` as a **regex**, not a
   glob — `deploy_dags.sh` failed twice.
6. Dataform CLI 3.x **dropped `--repository/--project/--location`** from `compile`.
7. Dataform models **hardcode the `mig-local` project**, so executing them against a real
   project fails with `400 project mig-local has not enabled BigQuery`.
8. Podman **requires fully-qualified image names**; the Beam base image had none.
9. Artifact Registry pushes need an **explicit `podman login`** with an ADC token.

Pattern worth noting: 6 of these 9 are **external-API drift** — gcloud flags, Dataform CLI
flags, Secret Manager validation, Google client behaviour. A repo whose docs are honest
about _architecture_ can still rot against the cloud provider's surface, and only a real
deployment catches it.

## Verdict on the review

**Accurate and well-calibrated.** 22 of 22 checkable findings confirmed; zero false
positives. Its severity ordering is right — H1 and H2 genuinely are minutes-to-fix,
highest-blast-radius items, and H3's failure mode was observed in the wild. The one
correction is scoping the "would fail before producing data" claim to the Composer path,
not the GCP path as a whole.

The suggested fix order stands, with one addition: **the fixes committed in `e84dff6`
should be treated as step 0** — they're prerequisites for anyone reproducing a GCP run at
all, and they close M8 and M12's Python half already.
