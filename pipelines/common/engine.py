"""Per-record routing to one of the two doors.

Every record ends up either migrated or not migrated, and a record that was not migrated
carries its disposition — the reason. See `doors.py` for the equation.

Deliberately Beam-free so the correctness contract is testable without a runner, and
so the same logic can back the Beam `DoFn`, the reconciliation service and the unit
tests. The Beam layer only supplies parallelism.

The order of the stages is fixed and load-bearing:

    parse ──▶ map ──▶ schema ──▶ migrated

A record that fails to parse never reaches map, so a malformed record is always
reported against the stage that actually stopped it. That matters because the reject
*reasons* are what the acceptance suite checks individually; collapsing the stages
would make the two-door total still balance while hiding a misclassification
underneath it.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Iterator

from .doors import Counters, Door, Reason, RecordError, Reject, Stage
from .mapping import TransformEngine


@dataclass
class Outcome:
    """Which door a single record left through, and what it produced."""

    door: Door
    raw: str
    account_key: str | None = None
    source_key: str | None = None
    doc: dict[str, Any] | None = None
    reject: Reject | None = None


def route_intake(engine: TransformEngine, raw: str) -> Outcome:
    """Intake — parse. The half of the engine both paths share.

    This is the *only* implementation of "did it parse", and it is deliberately
    smaller than `RecordRouter.route`. The two callers diverge immediately after this
    point, for reasons that are architectural rather than incidental — so collapsing
    them further would be wrong, not tidier:

    * `RecordRouter.route` (in-memory: recon, smoke tests, `run_batches`) continues
      straight through map and schema in one pass, because it has the whole record set
      on one machine.
    * The Beam file processor stops here. Its map/schema doors cannot run yet at all —
      `map_record` needs enrichment-only fields (e.g. BRANCH_NAME, see
      contracts/mappings/*.yaml) that Dataform and the enrichment pipeline have not
      contributed at intake time. Mapping here would reject every record with
      MAP_MISSING_REQUIRED.

    Returns an Outcome whose disposition is REJECTED or WRITTEN — where WRITTEN means
    only "survived intake", **not** "migrated". A record can still fail map/schema
    downstream, so intake settles what it can see and defers the rest.
    """
    try:
        fields = engine.parse(raw)
    except RecordError as exc:
        return Outcome(
            door=Door.REJECTED,
            raw=raw,
            source_key="",
            reject=Reject(
                run_id="",
                batch_id=0,
                source_key="",
                stage=exc.stage.value,
                reason=exc.reason.value,
                detail=exc.detail,
                raw_record=raw,
            ),
        )

    source_key = engine.source_key(fields)

    return Outcome(
        door=Door.WRITTEN,
        raw=raw,
        account_key=engine.account_key(fields),
        source_key=source_key,
        doc=fields,
    )


class RecordRouter:
    """Routes raw source lines to one of the two doors, for one run."""

    def __init__(self, engine: TransformEngine, run_id: str) -> None:
        self.engine = engine
        self.run_id = run_id

    def route(self, raw: str, batch_id: int) -> Outcome:
        """Route one record through every stage: parse → map → schema."""
        # Parse is shared with the Beam intake path — one implementation.
        intake = route_intake(self.engine, raw)
        if intake.door is Door.REJECTED:
            assert intake.reject is not None
            return Outcome(
                door=Door.REJECTED,
                raw=raw,
                source_key=intake.source_key,
                reject=Reject(
                    run_id=self.run_id,
                    batch_id=batch_id,
                    source_key=intake.reject.source_key,
                    stage=intake.reject.stage,
                    reason=intake.reject.reason,
                    detail=intake.reject.detail,
                    raw_record=intake.reject.raw_record,
                ),
            )

        # `route_intake` parked the parsed fields in `doc`; map and schema follow.
        fields = intake.doc
        assert fields is not None
        source_key = intake.source_key or ""
        account_key = intake.account_key or ""

        try:
            doc = self.engine.map_record(fields, self.run_id)
            self.engine.validate(doc)
        except RecordError as exc:
            return self._reject(raw, batch_id, source_key, exc)

        return Outcome(
            door=Door.WRITTEN,
            raw=raw,
            account_key=account_key,
            source_key=source_key,
            doc=doc,
        )

    def _reject(self, raw: str, batch_id: int, source_key: str, exc: RecordError) -> Outcome:
        return Outcome(
            door=Door.REJECTED,
            raw=raw,
            source_key=source_key,
            reject=Reject(
                run_id=self.run_id,
                batch_id=batch_id,
                source_key=source_key,
                stage=exc.stage.value,
                reason=exc.reason.value,
                detail=exc.detail,
                raw_record=raw,
            ),
        )


@dataclass
class BatchResult:
    """One batch of 200 (or fewer, for the tail) with its own closing equation."""

    batch_id: int
    counters: Counters
    documents: list[dict[str, Any]]
    rejects: list[Reject]


def run_batches(
    router: RecordRouter,
    lines: Iterable[str],
    batch_size: int,
) -> Iterator[BatchResult]:
    """Stream records in batches, yielding each batch's documents, rejects and tallies.

    Memory is flat across the run: only one batch is ever resident, and the router
    keeps no per-run accumulation of its own.
    """
    batch_id = 0
    counters = Counters()
    documents: list[dict[str, Any]] = []
    rejects: list[Reject] = []

    def flush() -> BatchResult:
        return BatchResult(
            batch_id=batch_id,
            counters=counters,
            documents=documents,
            rejects=rejects,
        )

    for raw in lines:
        raw = raw.rstrip("\n")
        if not raw.strip():
            continue
        if router.engine.is_header(raw):
            continue

        counters.src_read += 1
        outcome = router.route(raw, batch_id)
        counters.count(
            outcome.door,
            Reason(outcome.reject.reason) if outcome.reject else None,
        )
        if outcome.doc is not None:
            documents.append(outcome.doc)
        if outcome.reject is not None:
            rejects.append(outcome.reject)

        # A batch is 200 *written* records — the Loader contract counts documents,
        # not source lines.
        if len(documents) >= batch_size:
            yield flush()
            batch_id += 1
            counters, documents, rejects = Counters(), [], []

    if counters.src_read or documents or rejects:
        yield flush()
