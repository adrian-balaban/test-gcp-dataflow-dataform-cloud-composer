"""Run any project's contract through the engine and assert its correctness contract.

Deliberately project-agnostic: it takes a mapping path and knows nothing else. That is
what makes it usable as the extensibility proof — the same command runs project1 and
project2, and if it needed a branch for either, the "extend, not rewrite" claim would
already be false.

    python -m tests.project_smoke --mapping contracts/mappings/mapping-project2.yaml --layout csv
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from harness.generate import generate
from pipelines.common.doors import Counters, require_balance
from pipelines.common.engine import RecordRouter, run_batches
from pipelines.common.mapping import TransformEngine, load_mapping

ROOT = Path(__file__).resolve().parents[1]


def main() -> int:
    ap = argparse.ArgumentParser(description="Smoke-test one project's contract")
    ap.add_argument("--mapping", required=True)
    ap.add_argument("--layout", choices=("fixed", "csv"), default="fixed")
    ap.add_argument("--accounts", type=int, default=400)
    ap.add_argument("--seed", type=int, default=4242)
    args = ap.parse_args()

    # The harness always generates ACCOUNT-shaped synthetic data (ACCT_ID, CUST_ID, ...);
    # project2's DEPOSIT record reads the same bytes positionally under different field
    # names, so a real header row (named for ACCOUNT's columns) would not be recognised
    # as a header by a DEPOSIT-keyed parser and would be miscounted as a data row. The
    # header spec (contracts/README.md) is specifically project1's; skip it here.
    lines, manifest = generate(
        accounts=args.accounts, layout=args.layout, seed=args.seed,
        header=(args.mapping.endswith("mapping-project1.yaml")),
    )

    mapping = load_mapping(ROOT / args.mapping, root=ROOT).with_layout(args.layout)
    engine = TransformEngine(mapping)
    router = RecordRouter(engine, run_id=f"smoke-{mapping.project}")

    total = Counters()
    batches = 0
    documents = 0
    sample: dict | None = None

    for batch in run_batches(router, lines, mapping.batch_size):
        # Per batch as well as per run — both are required.
        require_balance(f"{mapping.project} batch {batch.batch_id}", batch.counters)
        total.merge(batch.counters)
        batches += 1
        documents += len(batch.documents)
        if sample is None and batch.documents:
            sample = batch.documents[0]

    require_balance(f"{mapping.project} run", total)

    if total.src_read != manifest.total_records:
        raise SystemExit(
            f"{mapping.project}: read {total.src_read} records, harness generated "
            f"{manifest.total_records}"
        )
    if documents != total.written:
        raise SystemExit(
            f"{mapping.project}: emitted {documents} documents but counted {total.written} written"
        )
    if total.written == 0:
        raise SystemExit(f"{mapping.project}: nothing was written — the contract cannot be valid")

    print(f"  project      : {mapping.project}")
    print(f"  layout       : {mapping.source_layout}  (record {mapping.source_record})")
    print(f"  target       : {mapping.target_record}")
    print(f"  equation     : {json.dumps(total.to_dict(), sort_keys=True)}")
    print(f"  batches      : {batches} (size {mapping.batch_size})")
    print(f"  balances     : {total.balances}")
    if sample is not None:
        print(f"  sample keys  : {sorted(sample)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
