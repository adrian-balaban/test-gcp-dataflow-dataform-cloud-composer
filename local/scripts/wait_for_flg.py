#!/usr/bin/env python3
"""Local stand-in for GCSObjectExistenceSensor — wait for the extraction `.FLG` semaphore.

The semaphore is the whole contract with the other team's Extractor: it is written last,
and only once the encrypted bundle is durable. Nothing downstream may read a byte before
it appears. On Cloud Composer this is a reschedule-mode sensor; here it is a poll.
"""
from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from pipelines.common.config import Config  # noqa: E402
from pipelines.common.storage import Gcs  # noqa: E402


def main() -> int:
    ap = argparse.ArgumentParser(description="Wait for the extraction .FLG semaphore")
    ap.add_argument("--run-id", required=True)
    ap.add_argument("--record", default="ACCOUNT")
    ap.add_argument("--timeout", type=int, default=600)
    ap.add_argument("--interval", type=int, default=3)
    args = ap.parse_args()

    cfg = Config.from_env()
    gcs = Gcs(cfg)
    name = f"extraction/{args.run_id}/{args.record}.FLG"

    deadline = time.time() + args.timeout
    while time.time() < deadline:
        if gcs.exists(cfg.landing_bucket, name):
            print(f"semaphore present: gs://{cfg.landing_bucket}/{name}")
            return 0
        time.sleep(args.interval)

    print(
        f"timed out after {args.timeout}s waiting for gs://{cfg.landing_bucket}/{name}",
        file=sys.stderr,
    )
    return 1


if __name__ == "__main__":
    sys.exit(main())
