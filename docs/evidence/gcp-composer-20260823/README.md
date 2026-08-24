# 2026-08-23 — the confirmation stream, proven on real GCP

The first DAG pass that exercises `docs/PLAN-CHANGES-22082026.md` end to end against real
infrastructure: Cloud Composer 2 → three Dataflow jobs → Cloud Run Target System mock →
**Managed Service for Apache Kafka** → recon's confirmation consumer. Acceptance criterion 9
("every TARGET row confirmed by Target System") is green here from *live* confirmations, not
from a skipped path.

- **Run id** `run-dag-20260823-0230`, Airflow run `manual__2026-08-22T22:44:15+00:00`.
- **Image tag** `32b8848-dirty` (all six images, plus the Cloud Run mock).
- **Terraform** `enable_composer=true -var=enable_kafka=true` — Composer, Managed Kafka
  (two topics), Serverless VPC Access connector, three project-level `managedkafka.client`
  grants.

| Artefact | What it shows |
|---|---|
| `01-dag-task-states.txt` | All 8 tasks `success`. |
| `02-reconciliation-report.json` | `targetSystemReconciliation`: `enabled: true`, `targetRows: 50`, `confirmations: 50`, `confirmedTargetRows: 50`, `unconfirmedTargetRows: 0`. |
| `03-verify.txt` | `tests/acceptance.py` against this run — 8 of 9 green from the host; criterion 4's *Kafka half* is unreachable from a laptop (see below), criterion 9 green. |
| `04-json-producer-sink-stats.txt` | The in-VPC counterpart to that one gap: the Kafka sink delivered 50 messages in 1 batch to `target-system-target`, acknowledged. |

## The one thing the host cannot check

`tests/acceptance.py` runs on the laptop. Managed Kafka is VPC-internal with no public
endpoint, so criterion 4's optional Kafka watermark check fails there with
`_ALL_BROKERS_DOWN` — the same host-reachability constraint `docs/runbook-gcp.md` §B8 states.
Its GCS half passes, and the delivery it would have counted is evidenced by
`04-json-producer-sink-stats.txt`, written by the pod that ran *inside* the VPC. Criterion 9
is unaffected: it reads recon's report from GCS, and recon ran in the VPC.

## Five defects this pass found and closed

Each was invisible until the code ran on real GCP; all five are fixed in the same commit.

1. **`KAFKA_SECURITY_PROTOCOL` never reached the pods.** A `KubernetesPodOperator` passes
   only what `env_vars` names, so a Composer environment variable is invisible to the pod.
   Recon would have opened a PLAINTEXT connection to a SASL_SSL-only broker.
2. **The Python `oauth_cb` could never fire.** It took no arguments (librdkafka calls it with
   the `sasl.oauthbearer.config` string) and nothing pumped the producer's service queue
   before the first `produce()` → *"OAuth token not set within 10 seconds timeout"*.
3. **Both languages sent the wrong token.** Managed Kafka does not accept a bare access
   token: it wants `b64(header).b64(claims).b64(accessToken)` with `alg=GOOG_OAUTH2_TOKEN`,
   `scope=kafka` and `sub` naming the service account — what Google's own
   `GcpLoginCallbackHandler` builds. The bare token fails as *"invalid credentials with SASL
   mechanism OAUTHBEARER"*.
4. **kafka-clients 3.7.1 cannot do OAUTHBEARER on JDK 25.** Its client callback handler calls
   `Subject.getSubject(AccessControlContext)`, which JDK 24+ answers with
   `UnsupportedOperationException` now that the Security Manager is gone. Bumped to 4.2.1.
   The symptom was only ever `TimeoutException: Timeout expired while fetching topic metadata`
   — because with no SLF4J binder on the classpath, kafka-clients' own diagnosis was
   discarded. `slf4j-simple` is now a dependency of both Kafka-using apps; it is what turned
   a 40-minute guess into a one-line answer.
5. **`make build-images` never built the Cloud Run mock.** `build_java_images.sh` listed
   loader-app, recon-service and dataform-runner only, while Terraform pins the Cloud Run
   service to `target-system-mock:latest`. The deployed mock silently stayed months behind
   every other component — so fixes 3 and 4 shipped everywhere *except* the one process that
   publishes confirmations, and recon kept reporting `0 confirmations` for code that was
   already correct. The mock is now in that list.

## And the negative path, incidentally

Before fix 5 landed, this same stack produced the failure criterion 9 exists to catch:

    RECONCILIATION FAILED: target system reconciliation GAP — 50 of 50 TARGET rows
    unconfirmed (confirmations seen: 0). Sent but not persisted.

with all 50 `account_key`s named, `recon: balancing equation closes; key-level reconciliation
clean.` printed immediately above it. The lane was internally consistent and the external
system had confirmed nothing — exactly the gap the balancing equation cannot see, caught on
real infrastructure rather than by a test hook.
