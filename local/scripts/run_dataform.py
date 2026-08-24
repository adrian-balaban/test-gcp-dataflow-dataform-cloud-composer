#!/usr/bin/env python3
"""Compile the Dataform project and execute the compiled SQL.

Why this exists: the Dataform CLI can *compile* anywhere — that is a purely local
operation — but it can only *execute* against real BigQuery, because `.df-credentials.json`
has no endpoint override for an emulator. So locally we take the genuine compiled output
and run it ourselves.

The important consequence is that `dataform/definitions/*.sqlx` are real, unmodified
Dataform artefacts. On GCP they are executed by the Dataform service itself, invoked from
Composer via DataformCreateCompilationResultOperator + DataformCreateWorkflowInvocationOperator —
the SQL does not change, only who runs it. See docs/runbook-gcp.md.

    python local/scripts/run_dataform.py --run-id run-e2e-001
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

# Two layouts run this file. In the repo it sits at local/scripts/, so the root is two
# levels up. In the dataform-runner pod image it is flattened to /app/run_dataform.py
# alongside dataform/ and pipelines/, where parents[2] does not exist at all — it raised
# IndexError at import, before the script could do anything. Locate the root by the
# directory that actually holds the project instead of by depth.
_here = Path(__file__).resolve()
ROOT = next(
    (p for p in (_here.parent, *_here.parents) if (p / "dataform").is_dir()),
    _here.parents[2] if len(_here.parents) > 2 else _here.parent,
)
sys.path.insert(0, str(ROOT))

from pipelines.common.config import Config  # noqa: E402
from pipelines.common.storage import BigQuery  # noqa: E402

DATAFORM_DIR = ROOT / "dataform"
# Pinned, not @latest: the compiler version decides the generated SQL, and an unpinned
# CLI means a rebuild can silently change what the models compile to — the same hazard as
# a floating image tag. Bump deliberately.
CLI = "@dataform/cli@3.0.0"


def compile_project(run_id: str, project: str) -> dict:
    """Run `dataform compile --json`, passing the run id through as a project var.

    The models are written against the local default project (`mig-local`, in
    workflow_settings.yaml and the source declaration). When executing against another
    project — BQ_TARGET=real — retarget the compiled output: `mig-local` only ever
    appears as a project id, so a compiled-JSON-level substitution is exact.
    """
    cmd = ["npx", "-y", CLI, "compile", "--json"]
    if run_id:
        cmd.append(f"--vars=runId={run_id}")

    proc = subprocess.run(
        cmd, cwd=DATAFORM_DIR, capture_output=True, text=True, timeout=600
    )
    if proc.returncode != 0:
        raise RuntimeError(f"dataform compile failed:\n{proc.stderr[-2000:]}")

    stdout = proc.stdout
    if project and project != "mig-local":
        stdout = stdout.replace("mig-local", project)

    try:
        graph = json.loads(stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"dataform compile produced non-JSON output: {exc}") from None

    errors = graph.get("graphErrors", {}).get("compilationErrors", [])
    if errors:
        detail = "\n".join(f"  {e.get('fileName')}: {e.get('message')}" for e in errors)
        raise RuntimeError(f"dataform compilation errors:\n{detail}")
    return graph


def fq(target: dict) -> str:
    return f"{target['database']}.{target['schema']}.{target['name']}"


def order_actions(tables: list[dict]) -> list[dict]:
    """Topologically sort by declared dependencies.

    Dependencies on declarations (external sources) are ignored — they are not
    actions we execute, they already exist.
    """
    by_name = {fq(t["target"]): t for t in tables}
    ordered: list[dict] = []
    visiting: set[str] = set()
    done: set[str] = set()

    def visit(name: str) -> None:
        if name in done:
            return
        if name in visiting:
            raise RuntimeError(f"dependency cycle in the Dataform graph at {name}")
        visiting.add(name)
        for dep in by_name[name].get("dependencyTargets", []) or []:
            dep_name = fq(dep)
            if dep_name in by_name:  # skip declarations
                visit(dep_name)
        visiting.discard(name)
        done.add(name)
        ordered.append(by_name[name])

    for name in by_name:
        visit(name)
    return ordered


def statement_for(action: dict) -> str:
    """Render one compiled action as executable SQL."""
    target = fq(action["target"])
    kind = (action.get("enumType") or "TABLE").upper()
    query = action["query"].strip().rstrip(";")

    if kind == "VIEW":
        return f"CREATE OR REPLACE VIEW `{target}` AS\n{query}"
    if kind in ("TABLE", "INCREMENTAL"):
        # INCREMENTAL is materialised as a full rebuild locally: the prototype's runs
        # are scoped to one run id anyway, so there is no partial-merge semantics to
        # preserve. On GCP Dataform handles incrementality properly.
        return f"CREATE OR REPLACE TABLE `{target}` AS\n{query}"
    raise RuntimeError(f"unsupported Dataform action type {kind!r} for {target}")


def main() -> int:
    ap = argparse.ArgumentParser(description="Compile and execute the Dataform models")
    ap.add_argument("--run-id", default="", help="scope the models to one run id")
    ap.add_argument("--dry-run", action="store_true", help="print the SQL, execute nothing")
    args = ap.parse_args()

    cfg = Config.from_env()

    print(f"dataform: compiling {DATAFORM_DIR.relative_to(ROOT)} (runId={args.run_id or '<all>'})")
    graph = compile_project(args.run_id, cfg.project)
    actions = order_actions(graph.get("tables", []))
    declarations = graph.get("declarations", [])
    print(f"  {len(declarations)} declaration(s), {len(actions)} action(s)")

    if args.dry_run:
        for action in actions:
            print(f"\n-- {fq(action['target'])}\n{statement_for(action)}")
        return 0

    bq = BigQuery(cfg)
    for dataset in {a["target"]["schema"] for a in actions}:
        bq.ensure_dataset(dataset)

    for action in actions:
        target = fq(action["target"])
        print(f"  → {target}")
        bq.client.query(statement_for(action)).result()

    for action in actions:
        target = fq(action["target"])
        rows = bq.query(f"SELECT COUNT(*) AS n FROM `{target}`")
        print(f"  ✓ {target}: {rows[0]['n']} rows")

    print("dataform complete.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
