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

Usage:
    python -m harness.generate --accounts 2000 --format copybook
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
# Keyed by layout so the CSV path's different failure mode stays explicit. `invalid_json`
# is gone — homogeneous DAT records (D6) carry no JSON side-channel to corrupt.
MALFORMED_KINDS: tuple[str, ...] = (
    "short_record",
    "bad_numeric",
    "bad_date",
    "unmapped_status",
    "schema_violation",
)

EXPECTED_REASON = {
    "fixed": {
        "short_record": "PARSE_SHORT_RECORD",
        "bad_numeric": "PARSE_BAD_NUMERIC",
        "bad_date": "PARSE_BAD_DATE",
        "unmapped_status": "MAP_UNMAPPED_ENUM_VALUE",
        "schema_violation": "SCHEMA_INVALID",
    },
    "csv": {
        # A truncated CSV row loses columns rather than bytes.
        "short_record": "PARSE_BAD_COLUMN_COUNT",
        "bad_numeric": "PARSE_BAD_NUMERIC",
        "bad_date": "PARSE_BAD_DATE",
        "unmapped_status": "MAP_UNMAPPED_ENUM_VALUE",
        "schema_violation": "SCHEMA_INVALID",
    },
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
    """15 bytes: sign + 11 integer digits + '.' + 2 decimals (PIC S9(11)V99)."""
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

    def render(self, layout: str) -> str:
        if layout == "csv":
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
        return (
            f"{self.acct_id:<12}"
            f"{self.cust_id:<10}"
            f"{self.client_type:<4}"
            f"{self.product:<6}"
            f"{self.currency:<3}"
            f"{self.open_date:<8}"
            f"{self.status:<1}"
            f"{self.balance:>15}"
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


def _break(row: Row, kind: str, layout: str) -> str:
    """Damage one rendered row in a specific, known way."""
    if kind == "short_record":
        rendered = row.render(layout)
        return rendered[:30] if layout == "fixed" else "|".join(rendered.split("|")[:4])
    if kind == "bad_numeric":
        return Row(**{**row.__dict__, "balance": "+0000000ABCD.56"}).render(layout)
    if kind == "bad_date":
        return Row(**{**row.__dict__, "open_date": "20191345"}).render(layout)
    if kind == "unmapped_status":
        # Parses cleanly; map_enum has no entry for 'X'.
        return Row(**{**row.__dict__, "status": "X"}).render(layout)
    if kind == "schema_violation":
        # Parses and maps; accountId then violates the TARGET schema's ^[A-Z0-9]+$.
        return Row(**{**row.__dict__, "acct_id": "acc-bad!0001"}).render(layout)
    raise ValueError(f"unknown malformed kind {kind!r}")


def generate(accounts: int, layout: str, seed: int, header: bool = True) -> tuple[list[str], Manifest]:
    rng = random.Random(seed)
    lines: list[str] = []
    account_ids: list[str] = []

    for i in range(accounts):
        idx = i + 1
        row = _row(rng, idx, rng.choice(CLIENT_TYPES))
        lines.append(row.render(layout))
        account_ids.append(row.acct_id)

    # One malformed record per kind — enough to prove every reject reason fires.
    for kind in MALFORMED_KINDS:
        donor = _row(rng, 900_000 + MALFORMED_KINDS.index(kind), CLIENT_TYPES[0])
        lines.append(_break(donor, kind, layout))

    rng.shuffle(lines)

    if layout == "csv" and header:
        lines = [CSV_HEADER] + lines

    reasons = EXPECTED_REASON["csv" if layout == "csv" else "fixed"]
    expected_by_reason: dict[str, int] = {}
    for k in MALFORMED_KINDS:
        expected_by_reason[reasons[k]] = expected_by_reason.get(reasons[k], 0) + 1

    manifest = Manifest(
        layout=layout,
        seed=seed,
        total_records=accounts + len(MALFORMED_KINDS),
        expected_written=accounts,
        expected_rejected=len(MALFORMED_KINDS),
        expected_by_reason=expected_by_reason,
        account_ids=sorted(account_ids),
    )
    return lines, manifest


def main() -> None:
    ap = argparse.ArgumentParser(description="Generate a synthetic Db2 account extract")
    ap.add_argument("--accounts", type=int, default=2000)
    ap.add_argument("--format", dest="layout", choices=("copybook", "csv"), default="copybook")
    ap.add_argument("--seed", type=int, default=20260803)
    ap.add_argument("--out", type=Path, default=Path("local/data/mainframe"))
    args = ap.parse_args()

    layout = "fixed" if args.layout == "copybook" else "csv"
    lines, manifest = generate(accounts=args.accounts, layout=layout, seed=args.seed)

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
