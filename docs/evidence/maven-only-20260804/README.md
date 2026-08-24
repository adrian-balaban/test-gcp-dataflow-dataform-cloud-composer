# Evidence — Maven-only build, JDK 25, full local E2E green

Captured 2026-08-04, branch `main`, immediately after removing the Gradle build
(REVIEW.md recommendation #1). Proves the collapsed single-build repo still passes
everything the dual-build repo passed.

Companion to `docs/evidence/gcp-run-20260804/`, which covers the real-GCP run
(Composer + Managed Kafka) on the same Maven jars.

## Toolchain

| | |
|---|---|
| JDK | 25.0.4 (GraalVM CE 25.2.4) |
| Maven | 3.8.6 |
| Java target | 17, via `maven.compiler.release` |

`--release 17` does the targeting, so the JDK 25 host builds JDK 17 bytecode. This is
the reason Maven was kept over Gradle: Gradle 8.10.2 cannot *run* on JDK 25
(`Unsupported class file major version 69`).

## Files

| File | What it shows |
|------|---------------|
| `01-toolchain.log` | `java -version` / `mvn -version` — JDK 25 + Maven 3.8.6 |
| `02-maven-package.log` | `mvn -B clean package` — 5 modules SUCCESS, 4 runnable jars |
| `03-make-test.log` | `make test` — 22/22 pytest + Maven reactor SUCCESS |
| `04-run-initial.log` | full E→T+R→L chain on the local stack |
| `05-verify.log` | all 8 acceptance criteria PASS |
| `06-vault-stats.json` | Target System mock counters — proves a live, non-stale mock |
| `07-verify-project2.log` | engine fingerprint identical across project1/project2 |

## Results

`make run-initial && make verify` — **VERIFY PASSED, all 8 criteria**, run
`initial-20260804-175558`:

```
526 src_read = 400 written + 100 excluded + 6 rejected + 20 deduplicated
```

All six enumerated reject reasons fired exactly once; 400/400 documents schema-valid;
400 keys each present exactly once with 0 orphans; `.CHS` checksums verify on both
lanes. Run reproduced twice (`…-175241` and `…-175558`), both 8/8.

Target System mock counters:

```json
{ "received": 949, "accepted": 400, "duplicatesIgnored": 400,
  "injectedFailures": 149, "distinctAccounts": 400 }
```

400 accepted with 400 duplicates ignored and 149 injected failures recovered by retry
— the idempotency + backoff path is genuinely exercised. The container was rebuilt
from scratch for this run, so the H3 stale-mock failure mode (a warm mock silently
counting fresh documents as duplicates) is ruled out.

`verify-project2`: the engine fingerprint is byte-identical before and after running
project2 (`9d87d40a4e985683cd6cfcee6796438f7f05b2a7b21667abb903a2bba78a3e71`), and both
contracts balance — only the mapping YAML changed. (The script's `git diff` guard
additionally requires a clean tree, so it must be run from a committed state.)

## Defect found by this work

Removing the `|| true` from the target-system-mock Dockerfile's `dependency:go-offline`
step (review finding L11) immediately exposed a real bug it had been masking: the
Dockerfile copied only 3 of the 5 module poms, while the root pom declares all 5, so
Maven could not read the reactor and the dependency-cache layer silently did nothing.
Fixed by copying all five poms. This is L11 confirmed as a live defect, not a nit.
