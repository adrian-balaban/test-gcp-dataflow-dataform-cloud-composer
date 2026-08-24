"""The artefact naming convention and shared BigQuery column names — loaded, not restated.

Both this module and `apps/common/.../Artefacts.java` read `contracts/artefacts.json`.
Before that file existed the convention lived twice, once per language, kept in step by a
comment in each — so a rename in Python stayed invisible to Java until a run failed at
integration. The manifest makes the agreement a file both sides load rather than a promise
both sides make.

Resolution order for the contracts directory: `MIG_CONTRACTS_DIR` if set (the Dataflow
images place it elsewhere), otherwise `contracts/` next to the repo root. The Java side does
not need a path at all — Maven copies the same file onto its classpath.
"""

from __future__ import annotations

import json
import os
from functools import lru_cache
from pathlib import Path
from typing import Any

_REPO_ROOT = Path(__file__).resolve().parents[2]
MANIFEST_NAME = "artefacts.json"


def manifest_path() -> Path:
    override = os.environ.get("MIG_CONTRACTS_DIR", "").strip()
    base = Path(override) if override else _REPO_ROOT / "contracts"
    return base / MANIFEST_NAME


@lru_cache(maxsize=1)
def manifest() -> dict[str, Any]:
    """The parsed manifest. Cached — it is read once per process and never changes."""
    path = manifest_path()
    if not path.is_file():
        # Failing loudly here beats falling back to a hardcoded convention: a silent
        # fallback is exactly the drift this file exists to prevent.
        raise FileNotFoundError(
            f"artefact manifest not found at {path} — set MIG_CONTRACTS_DIR if the "
            f"contracts directory is elsewhere"
        )
    return json.loads(path.read_text(encoding="utf-8"))


def prefix(lane: str, run_id: str) -> str:
    """e.g. `extraction/initial-20260818-081725/`."""
    lanes = manifest()["lanes"]
    if lane not in lanes:
        raise ValueError(f"unknown lane {lane!r} — the manifest declares {sorted(lanes)}")
    return manifest()["prefix"].format(lane=lanes[lane], run_id=run_id)


def dat(record: str, sequence: int) -> str:
    """Sequenced data file — a large extract can be produced in parts."""
    m = manifest()
    width = m["dat_sequence_width"]
    return m["artefacts"]["dat"].format(record=record, sequence=f"{sequence:0{width}d}")


def artefact_names(record: str) -> dict[str, str]:
    """Every non-sequenced artefact for one record, keyed by type."""
    return {
        key: template.format(record=record)
        for key, template in manifest()["artefacts"].items()
        if key != "dat"
    }


def columns(table: str) -> dict[str, str]:
    """Logical name → physical BigQuery column, for `src` or `target`."""
    cols = manifest()["columns"]
    if table not in cols:
        raise ValueError(f"unknown table {table!r} — the manifest declares {sorted(cols)}")
    return dict(cols[table])
