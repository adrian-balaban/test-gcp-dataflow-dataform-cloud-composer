"""Generate a synthetic Db2 account extract plus a manifest of exact expected counts.

The manifest is the oracle `make verify` asserts against: if the engine's tallies
disagree with what was deliberately seeded here, the run has lost or miscategorised
records.

It seeds the reject reasons behind the not-migrated door: getting the two-door total
right is not enough — a record miscategorised under the wrong reason still balances,
and the manifest is what catches that, because it pins each reject reason to an exact
expected count.

One full snapshot per run, scoped only by `run_id` (docs/PLAN-CHANGES-21082026.md D5):
no run kind, no window. The source snapshot is assumed to contain no duplicate account
keys (D4) — this generator never emits one.

Input is pipe-delimited CSV only — the fixed-width copybook layout was removed
(docs/PLAN-CHANGES-02092026-kafka-loader.md follow-up). `contracts/tds/*.def` no longer
declares a `layout fixed` alternative.

Usage:
    python -m harness.generate --accounts 2000
"""

from __future__ import annotations

import argparse
import json
import random
from dataclasses import dataclass, asdict
from decimal import Decimal
from pathlib import Path
from typing import Any

CLIENT_TYPES = ("RETL", "SME ")

PRODUCTS = ("SAV001", "CUR001", "DEP002", "LON003")
CURRENCIES = ("EUR", "RON", "USD", "GBP")
STATUSES = ("A", "C", "D")

CSV_HEADER = (
    "ACCT_ID|CUST_ID|CLIENT_TYPE|PRODUCT_CODE|CURRENCY|OPEN_DATE|STATUS|BALANCE"
)

# The malformed variants seeded into every run, and the door/reason each must produce.
MALFORMED_KINDS: tuple[str, ...] = (
    "short_record",
    "bad_numeric",
    "bad_date",
    "unmapped_status",
    "schema_violation",
)

EXPECTED_REASON = {
    # A truncated CSV row loses columns rather than bytes.
    "short_record": "PARSE_BAD_COLUMN_COUNT",
    "bad_numeric": "PARSE_BAD_NUMERIC",
    "bad_date": "PARSE_BAD_DATE",
    "unmapped_status": "MAP_UNMAPPED_ENUM_VALUE",
    "schema_violation": "SCHEMA_INVALID",
}


@dataclass
class Manifest:
    """The oracle. Every number here is seeded, not observed."""

    layout: str
    seed: int
    total_records: int
    expected_written: int
    expected_rejected: int
    expected_by_reason: dict[str, int]
    account_ids: list[str]

    def to_json(self) -> str:
        return json.dumps(asdict(self), indent=2, sort_keys=True)


def _fmt_balance(value: Decimal) -> str:
    sign = "-" if value < 0 else "+"
    whole, _, frac = f"{abs(value):.2f}".partition(".")
    return f"{sign}{whole.rjust(11, '0')}.{frac}"


@dataclass
class Row:
    acct_id: str
    cust_id: str
    client_type: str
    product: str
    currency: str
    open_date: str
    status: str
    balance: str

    def render(self) -> str:
        return "|".join(
            [
                self.acct_id,
                self.cust_id,
                self.client_type,
                self.product,
                self.currency,
                self.open_date,
                self.status,
                self.balance,
            ]
        )


def _row(rng: random.Random, idx: int, client_type: str) -> Row:
    balance = Decimal(rng.randrange(-500_000, 50_000_000)) / 100
    return Row(
        acct_id=f"ACC{idx:09d}",
        cust_id=f"CUS{idx % 900000:07d}",
        client_type=client_type,
        product=rng.choice(PRODUCTS),
        currency=rng.choice(CURRENCIES),
        open_date=f"{rng.randint(1998, 2025)}{rng.randint(1, 12):02d}{rng.randint(1, 28):02d}",
        status=rng.choice(STATUSES),
        balance=_fmt_balance(balance),
    )


def _break(row: Row, kind: str) -> str:
    """Damage one rendered row in a specific, known way."""
    if kind == "short_record":
        return "|".join(row.render().split("|")[:4])
    if kind == "bad_numeric":
        return Row(**{**row.__dict__, "balance": "+0000000ABCD.56"}).render()
    if kind == "bad_date":
        return Row(**{**row.__dict__, "open_date": "20191345"}).render()
    if kind == "unmapped_status":
        # Parses cleanly; map_enum has no entry for 'X'.
        return Row(**{**row.__dict__, "status": "X"}).render()
    if kind == "schema_violation":
        # Parses and maps; accountId then violates the TARGET schema's ^[A-Z0-9]+$.
        return Row(**{**row.__dict__, "acct_id": "acc-bad!0001"}).render()
    raise ValueError(f"unknown malformed kind {kind!r}")


def generate(accounts: int, seed: int, header: bool = True) -> tuple[list[str], Manifest]:
    rng = random.Random(seed)
    lines: list[str] = []
    account_ids: list[str] = []

    for i in range(accounts):
        idx = i + 1
        row = _row(rng, idx, rng.choice(CLIENT_TYPES))
        lines.append(row.render())
        account_ids.append(row.acct_id)

    # One malformed record per kind — enough to prove every reject reason fires.
    for kind in MALFORMED_KINDS:
        donor = _row(rng, 900_000 + MALFORMED_KINDS.index(kind), CLIENT_TYPES[0])
        lines.append(_break(donor, kind))

    rng.shuffle(lines)

    if header:
        lines = [CSV_HEADER] + lines

    expected_by_reason: dict[str, int] = {}
    for k in MALFORMED_KINDS:
        expected_by_reason[EXPECTED_REASON[k]] = expected_by_reason.get(EXPECTED_REASON[k], 0) + 1

    manifest = Manifest(
        layout="csv",
        seed=seed,
        total_records=accounts + len(MALFORMED_KINDS),
        expected_written=accounts,
        expected_rejected=len(MALFORMED_KINDS),
        expected_by_reason=expected_by_reason,
        account_ids=sorted(account_ids),
    )
    return lines, manifest


def main() -> None:
    ap = argparse.ArgumentParser(description="Generate a synthetic Db2 account extract (pipe-delimited CSV)")
    ap.add_argument("--accounts", type=int, default=2000)
    ap.add_argument("--seed", type=int, default=20260803)
    ap.add_argument("--out", type=Path, default=Path("local/data/mainframe"))
    args = ap.parse_args()

    lines, manifest = generate(accounts=args.accounts, seed=args.seed)

    out = args.out
    out.mkdir(parents=True, exist_ok=True)
    (out / "ACCOUNT.src").write_text("\n".join(lines) + "\n", encoding="utf-8")
    (out / "manifest.json").write_text(manifest.to_json(), encoding="utf-8")

    print(
        f"{manifest.total_records} records -> {out/'ACCOUNT.src'}  "
        f"(written={manifest.expected_written} rejected={manifest.expected_rejected})"
    )


if __name__ == "__main__":
    main()
