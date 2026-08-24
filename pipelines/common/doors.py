"""The two doors, the reject taxonomy and the balancing equation.

Every source record leaves through exactly one of **two doors**:

    SRC_read = migrated + not_migrated

A record that was not migrated always carries *why* — its `Door` disposition. There is
exactly one such disposition, so the equation expands, without changing, to:

    SRC_read = TARGET_written + rejected

Reject reasons are an enumeration, not free text, so they can be counted and trended.
A "6 not migrated" tally is never stored without the per-reason breakdown behind it.
"""

from __future__ import annotations

import enum
import hashlib
from dataclasses import dataclass, field, asdict
from typing import Any


class Exit(str, enum.Enum):
    """The two doors. Every record leaves through exactly one."""

    MIGRATED = "migrated"
    NOT_MIGRATED = "not_migrated"


class Door(str, enum.Enum):
    """The disposition — *which* of the two doors a record left through.

    One value per door: `WRITTEN` is the migrated door, `REJECTED` the not-migrated one.
    The `Reason` carried alongside a `REJECTED` disposition is what makes "why was this
    account not migrated" answerable rather than merely countable, and it is what the
    acceptance table checks (reject reasons individually).
    """

    WRITTEN = "written"
    REJECTED = "rejected"

    @property
    def exit(self) -> Exit:
        """Which of the two doors this disposition leaves through."""
        return Exit.MIGRATED if self is Door.WRITTEN else Exit.NOT_MIGRATED


class Stage(str, enum.Enum):
    """Where in the engine a record was stopped."""

    PARSE = "parse"
    MAP = "map"
    SCHEMA = "schema"


class Reason(str, enum.Enum):
    """Enumerated reason codes for the not-migrated door — never free text.

    One closed enum across every stage that can reject is what lets `record_lineage` be
    queried with a single `GROUP BY reason`.
    """

    # parse stage
    PARSE_SHORT_RECORD = "PARSE_SHORT_RECORD"
    PARSE_MISSING_FIELD = "PARSE_MISSING_FIELD"
    PARSE_BAD_NUMERIC = "PARSE_BAD_NUMERIC"
    PARSE_BAD_DATE = "PARSE_BAD_DATE"
    PARSE_INVALID_JSON = "PARSE_INVALID_JSON"
    PARSE_MISSING_JSON_PATH = "PARSE_MISSING_JSON_PATH"
    PARSE_BAD_COLUMN_COUNT = "PARSE_BAD_COLUMN_COUNT"

    # map stage
    MAP_MISSING_REQUIRED = "MAP_MISSING_REQUIRED"
    MAP_UNKNOWN_TRANSFORM = "MAP_UNKNOWN_TRANSFORM"
    MAP_UNMAPPED_ENUM_VALUE = "MAP_UNMAPPED_ENUM_VALUE"
    MAP_TYPE_ERROR = "MAP_TYPE_ERROR"

    # schema stage
    SCHEMA_INVALID = "SCHEMA_INVALID"


class RecordError(Exception):
    """Raised by parse/map/validate to route a record through the REJECTED door.

    Carries the stage and enumerated reason so the caller never has to interpret
    a message string.
    """

    def __init__(self, stage: Stage, reason: Reason, detail: str = "") -> None:
        super().__init__(f"{stage.value}/{reason.value}: {detail}")
        self.stage = stage
        self.reason = reason
        self.detail = detail


@dataclass(frozen=True)
class Reject:
    """One row of the `.ERR` reject file."""

    run_id: str
    batch_id: int
    source_key: str
    stage: str
    reason: str
    detail: str
    raw_record: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def content_name(raw: str) -> str:
    """A stable identity for a record too malformed to yield a source key.

    A parse-stage reject never reached the field that identifies it, so the only thing
    that names it is its own bytes. Hashing them gives the lineage register a real key —
    stable across replays, joinable back to `reject_log.raw_record` — instead of a blank
    that would make the register name only the records that happened to parse.
    """
    return "raw:" + hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


@dataclass(frozen=True)
class Lineage:
    """One not-migrated record, named rather than merely counted.

    `run_ledger` says *how many* records left through each door; this says *which*. The
    aggregate is the claim, this is the evidence behind it — "why was this account not
    migrated" is answerable only from here.

    Only not-migrated records are written here. A migrated record is already named in
    `account_src` by its `_source_key` / `_account_key`, so duplicating 1.7M rows to
    restate it would cost storage and prove nothing new; the two sets together are
    exactly `src_read`, which is what the balancing equation asserts.
    """

    run_id: str
    source_key: str
    account_key: str | None
    door: str
    stage: str
    reason: str
    detail: str
    source_file: str
    created_at: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class Counters:
    """Per-run and per-batch tallies. The balancing equation is checked off these.

    One field per door. The field names match the BigQuery `run_ledger` columns (with
    the long-standing spelling difference: `written` is `extraction_written` there), so
    the storage schema is unaffected by the two-door framing.
    """

    src_read: int = 0
    written: int = 0
    rejected: int = 0
    by_reason: dict[str, int] = field(default_factory=dict)

    def count(self, door: Door, reason: Reason | None = None) -> None:
        if door is Door.WRITTEN:
            self.written += 1
        elif door is Door.REJECTED:
            self.rejected += 1
            if reason is not None:
                self.by_reason[reason.value] = self.by_reason.get(reason.value, 0) + 1

    @property
    def migrated(self) -> int:
        """Door 1 of 2."""
        return self.written

    @property
    def not_migrated(self) -> int:
        """Door 2 of 2."""
        return self.rejected

    @property
    def accounted(self) -> int:
        # Identical arithmetic either way: migrated + not_migrated. Written out in full
        # because this is the number the equation turns on.
        return self.written + self.rejected

    @property
    def balances(self) -> bool:
        """The correctness contract."""
        return self.src_read == self.accounted

    @property
    def imbalance(self) -> int:
        return self.src_read - self.accounted

    def merge(self, other: "Counters") -> None:
        self.src_read += other.src_read
        self.written += other.written
        self.rejected += other.rejected
        for reason, n in other.by_reason.items():
            self.by_reason[reason] = self.by_reason.get(reason, 0) + n

    def to_dict(self) -> dict[str, Any]:
        return {
            "src_read": self.src_read,
            # The two doors, then the disposition names behind them. This dict is
            # consumed by the orchestrator and the run summary.
            "migrated": self.migrated,
            "not_migrated": self.not_migrated,
            "written": self.written,
            "rejected": self.rejected,
            "accounted": self.accounted,
            "balances": self.balances,
            "imbalance": self.imbalance,
            "by_reason": dict(sorted(self.by_reason.items())),
        }


class BalanceError(Exception):
    """Raised when the balancing equation does not close. Fails the run and the DAG."""

    def __init__(self, scope: str, counters: Counters) -> None:
        super().__init__(
            f"balancing equation failed for {scope}: "
            f"src_read={counters.src_read} != "
            f"migrated={counters.migrated} + not_migrated={counters.not_migrated} "
            f"(rejected={counters.rejected}) "
            f"= {counters.accounted}, off by {counters.imbalance}"
        )
        self.scope = scope
        self.counters = counters


def require_balance(scope: str, counters: Counters) -> None:
    """Assert the equation closes, or fail loudly. Called per batch and per run."""
    if not counters.balances:
        raise BalanceError(scope, counters)
