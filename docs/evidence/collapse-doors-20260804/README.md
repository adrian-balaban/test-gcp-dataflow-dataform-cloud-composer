# Evidence — collapsing the parse/filter doors to one implementation

_2026-08-04, branch `main`. Applies REVIEW.md simplification #2 and issues #1/#2 — but
**not** as the review literally worded them. What follows records what was checked, what
was changed, and what was deliberately not changed._

## Summary

| Review item                                                                      | Verdict                     | Action                                                                                         |
| -------------------------------------------------------------------------------- | --------------------------- | ---------------------------------------------------------------------------------------------- |
| Simplification #2 — "make the Beam DoFns thin wrappers that call `RecordRouter`" | **Partly wrong as written** | Applied the intent (one implementation of parse+filter), rejected the literal mechanism        |
| Issue #2 — two implementations of the disposition logic                          | **Real**                    | Fixed: `route_intake` is now the single parse+filter implementation, plus a drift test         |
| Issue #1 — per-batch balancing not enforced on the Beam path                     | **Real**                    | Fixed in `json_producer`, where batch ids are authoritative                                    |
| Java rewrite of the Beam pipelines + engine + harness                            | **Not applied**             | Analysis section, not a recommendation; the review's own Bottom Line prescribes only items 1–2 |
| L4 — `architecture diagram` committed twice                                      | **Real**                    | Root copy removed; `docs/inputs/architecture diagram` is canonical                             |

## Why the literal wording of simplification #2 is wrong

The review says the Beam DoFns should become thin wrappers calling `RecordRouter.route`.
Doing that would break the pipeline, for two independent reasons found by reading the code:

**1. `route()` settles every disposition; the file processor must stop after dedup.**
`RecordRouter.route` goes parse → filter → dedup → **map → schema** in one pass
(`engine.py`). The Beam file processor deliberately stops after dedup — map/schema run
much later in `json_producer`, _after_ Dataform's SQL transform and the enrichment stage.

This is not stylistic. `contracts/mappings/mapping-project1.yaml:97-99` maps `branchName`
from `BRANCH_NAME`, and line 54 of that file states plainly:

> `# Added by the Data Enrichment pipeline, not present in the source record.`

So calling `map_record` at intake time hits `mapping.py:370` →
`RecordError(Stage.MAP, Reason.MAP_MISSING_REQUIRED)` for **every record**. A literal
application of the review's advice would reject 100% of the corpus.

**2. Dedup is architecturally different on purpose.**
`route()` takes `seen: set[str]` — an in-memory set, correct only single-threaded. The
Beam path uses `GroupByKey` + `DedupFn` because, per its own docstring, "it is the only
formulation that still works when the key space does not fit on one worker". Wrapping
`route()` would either reintroduce a per-worker `seen` set — silently _wrong_, since
duplicates landing on different workers would both survive — or leave dedup outside the
wrapper regardless.

The genuinely shared surface is therefore **parse and filter only**.

## What was actually changed

- **`pipelines/common/engine.py`** — new `route_intake(engine, raw)`: the single
  implementation of parse + filter, returning REJECTED / EXCLUDED / WRITTEN (where
  WRITTEN means "survived intake" and carries the parsed SRC-TDS fields). Its docstring
  records why the two callers diverge after it, so the next reader does not "simplify" it
  back into a bug. `RecordRouter.route` now delegates its first two doors to it.
- **`pipelines/file_processor/pipeline.py`** — `ParseAndFilterFn` is now a genuine thin
  wrapper: it calls `route_intake` and adds only Beam-specific shape (tagged outputs,
  metric counters). The module docstring no longer implies the doors are re-encoded here.
- **`pipelines/json_producer/pipeline.py`** — per-batch balancing enforced via
  `require_balance` as each batch closes, plus a run-level check that the per-batch
  tallies sum back to `written` (per-batch balance alone would still permit a whole batch
  vanishing between numbering and emission).
- **`tests/test_engine.py`** — three new tests (25 total, up from 22): the two door tests
  plus the delegation guard added afterwards.

### Why per-batch balancing went into `json_producer`, not `file_processor`

Issue #1 asks for "real batch ids wired through". In `file_processor` there is no honest
batch id to wire: a batch is defined as **200 written documents**, but "written" is not
known until after the `GroupByKey` dedup, and the authoritative numbering is assigned
later by `ROW_NUMBER()` in `json_producer`. Stamping a fabricated id at intake would make
the claim _look_ satisfied while proving nothing. The `batch_id=0` on intake rejects is
now explicitly commented as such rather than left to look like an oversight.

## Verification

All commands run against the live local stack (`MIG_EXECUTION_MODE=local`).

**Unit tests — 25 passed** (`01-unit-tests.log` captured at 24; a 25th, the delegation
guard below, was added afterwards).

**The delegation guard** (`05-delegation-guard.log`) —
`test_beam_dofn_actually_delegates_to_the_shared_door` stubs `route_intake` to return a
known-wrong answer and asserts the DoFn's output changes with it, which is only possible
if the delegation is real. It therefore catches a _duplicate implementation that happens
to agree_, which the corpus-comparison test below cannot. This replaces the hand-run
mutation check that previously lived only in an evidence log and so could never fail the
build.

Worth reading that log for the trap: the first draft of this test fed the DoFn an
unparseable line, which made it **pass against the very regression it was written to
catch** (an inlined duplicate throws on junk, falls back to `route_intake`, and the stub
is reached anyway). Driving it with a valid seeded record fixes it. The mistake is
recorded rather than quietly corrected, because it is the exact "passes for the wrong
reason" failure that motivated verifying these guards against a realistic break.

**The drift guard is load-bearing** (`03-drift-guard-mutation.log`). A mutation that makes
`RecordRouter` re-implement the filter door divergently is caught by
`test_beam_intake_and_router_agree_on_every_record`, which runs the whole seeded corpus
through both callers and asserts identical classification. This is precisely the test the
review noted was missing ("A behavioural drift between them would not be caught by any
test").

**Full local E2E — `make run-initial`**, run `initial-20260804-184528`: balancing equation
closes, key-level reconciliation clean, migrability 93.9% (526 candidates → 400 migrated,
100 excluded by policy, 6 blocked by data quality) — identical to the pre-change numbers.

**All 8 acceptance criteria pass** (`02-acceptance-verify.log`):

```
✓ 1. balancing equation closes (whole lane): 526 = 400 written + 100 excluded + 6 rejected + 20 deduplicated
✓ 2. excluded count matches the harness exactly: 100 == 100 seeded
✓ 3. seeded malformed records rejected with correct reason codes: 6 reason codes, all matching
✓ 4. every TARGET document validates against the JSON Schema: 400/400 documents valid
✓ 5. TARGET emitted in 200-element batches: 2 batches of 200
✓ 6. every key appears exactly once in TARGET: 400 keys, 0 orphans
✓ 7. all five artefact types present in both lanes
✓ 8. .CHS checksums verify on both sides
```

**`make verify-project2` — PASS** (`04-verify-project2.log`, exit 0). The engine
fingerprint is unchanged across running a second project (`BEFORE == AFTER`) and both
projects balance (project1 and project2, 422 accounted each).

One note for anyone re-running this mid-change: the script _also_ asserts
`git diff --quiet` over `pipelines/` + `apps/`, which compares against the last commit,
not against the project2 run. It therefore fails while an engine change is uncommitted —
as it did here before the commit — and passes once committed, which is the log captured
above. The fingerprint half (the property that actually matters: adding a project requires
no engine change) passed throughout, including before the commit.

## Not applied: the Java rewrite

The quoted passage is from the review's _analysis_ of whether Java could be used, not its
recommendations. The review's own Bottom Line prescribes simplifications 1–2 only, and
concludes: "For the September MVP as-is I wouldn't rewrite." Against it specifically:

- It is a ~1,500-line rewrite of the best-tested code in the repo (25 unit tests + the
  seeded-oracle acceptance path).
- `verify_project2.sh` fingerprints `pipelines/` + `apps/`; a rewrite means rebuilding
  that whole "extend, not rewrite" proof from scratch.
- The review itself concedes the config-driven mapping engine "is genuinely more pleasant
  in Python (dynamic YAML→transform dispatch is verbose in Java)".
- The Composer DAG must stay Python regardless, so "all-Java" is unreachable by design.

The one durable argument for it — collapsing the dual TDS reader to "one contract, one
reader" — remains a real future win, and is unaffected by this change. It stays a
team-level call, as the review says of items 3–4.
