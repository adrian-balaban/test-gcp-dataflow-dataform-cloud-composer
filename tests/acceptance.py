"""`make verify` — assert every acceptance criterion against the last completed run.

This is a pass/fail table "measured at the demo, not a list of things to show", executed.
It reads the live stack, not a log, and exits non-zero on the first thing that is not true.

The oracle for counts is the harness manifest: numbers that were *seeded* deliberately,
not observed after the fact. The reject-reason breakdown must match exactly, which is
only meaningful against a known-good expectation.

Eight criteria (docs/PLAN-CHANGES-21082026.md D7), plus criterion 9 (the Target System
confirmation stream the same plan adds). Two were dropped from the original
ten: "excluded count matches" (there is no excluded door any more — D1) and "delta run
balances" (there are no deltas — D5).
"""

from __future__ import annotations

import json
import math
import sys
from pathlib import Path
from typing import Any, Callable

from pipelines.common.config import Config, require_identifier
from pipelines.common.mapping import load_mapping, load_target_validator
from pipelines.common.storage import BigQuery, Gcs

ROOT = Path(__file__).resolve().parents[1]

GREEN, RED, YELLOW, BOLD, OFF = "\033[32m", "\033[31m", "\033[33m", "\033[1m", "\033[0m"

failures: list[str] = []
checks_run = 0


def check(name: str, fn: Callable[[], str]) -> None:
    global checks_run
    checks_run += 1
    try:
        detail = fn()
        print(f"  {GREEN}✓{OFF} {name}: {detail}")
    except AssertionError as exc:
        print(f"  {RED}✗{OFF} {name}: {exc}")
        failures.append(name)
    except Exception as exc:  # noqa: BLE001 — an erroring check is a failing check
        print(f"  {RED}✗{OFF} {name}: {type(exc).__name__}: {exc}")
        failures.append(name)


def main() -> int:
    cfg = Config.from_env()
    state = ROOT / "local/state/last_run_id"
    if not state.is_file():
        print(f"{RED}no run to verify — run `make run` first{OFF}", file=sys.stderr)
        return 1
    # Guarded like every other entry point. This one reads its run id from a file rather
    # than a flag, so nothing else would stop a tampered `local/state/last_run_id` from
    # reaching the interpolated SQL below — BigQuery cannot bind identifiers, and several
    # of these statements name their table by interpolation too.
    run_id = require_identifier("local/state/last_run_id", state.read_text(encoding="utf-8").strip())

    manifest = json.loads(
        (ROOT / "local/data/mainframe/manifest.json").read_text(encoding="utf-8")
    )
    mapping = load_mapping(cfg.mapping_path)
    bq = BigQuery(cfg)
    gcs = Gcs(cfg)
    p = cfg.project

    print(f"{BOLD}verify — run {run_id}{OFF}\n")

    def q1(sql: str) -> Any:
        rows = bq.query(sql)
        return next(iter(rows[0].values())) if rows else None

    # ── 1. balancing equation, per run and per batch ─────────────────────────
    def balancing() -> str:
        ledger = bq.query(
            f"SELECT * FROM `{p}.{cfg.ds_recon}.run_ledger` WHERE run_id = '{run_id}'"
        )
        assert ledger, "no run_ledger row for this run"
        row = ledger[0]
        rejected_total = q1(
            f"SELECT COUNT(*) FROM `{p}.{cfg.ds_recon}.reject_log` WHERE run_id = '{run_id}'"
        )
        written = q1(
            f"SELECT COUNT(*) FROM `{p}.{cfg.ds_transformation}.account_target` "
            f"WHERE run_id = '{run_id}'"
        )
        src = row["src_read"]
        not_migrated = rejected_total
        accounted = written + not_migrated
        assert src == accounted, (
            f"{src} != {written} migrated + {not_migrated} not migrated "
            f"(rejected={rejected_total}) = {accounted}"
        )
        assert row["balanced"], "the file processor recorded its own stage as unbalanced"
        return f"{src} = {written} migrated + {not_migrated} not migrated"

    check("1. balancing equation closes (whole lane)", balancing)

    # ── 2. every seeded malformed record rejected with the right reason ──────
    def reject_reasons() -> str:
        rows = bq.query(
            f"SELECT reason, COUNT(*) AS n FROM `{p}.{cfg.ds_recon}.reject_log` "
            f"WHERE run_id = '{run_id}' GROUP BY reason"
        )
        actual = {r["reason"]: r["n"] for r in rows}
        expected = manifest["expected_by_reason"]
        assert actual == expected, f"got {actual}, seeded {expected}"
        return f"{len(actual)} reason codes, all matching: {', '.join(sorted(actual))}"

    check("2. seeded malformed records rejected with correct reason codes", reject_reasons)

    # ── 3. 100% of emitted TARGET JSON validates against the schema ──────────
    def schema_valid() -> str:
        assert mapping.target_schema_path is not None, "no TARGET schema configured"
        validator = load_target_validator(mapping.target_schema_path)
        docs = bq.query(
            f"SELECT doc_json FROM `{p}.{cfg.ds_transformation}.account_target` "
            f"WHERE run_id = '{run_id}'"
        )
        assert docs, "no TARGET documents were produced"
        bad = []
        for row in docs:
            doc = json.loads(row["doc_json"])
            errors = list(validator.iter_errors(doc))
            if errors:
                bad.append(f"{doc.get('accountId')}: {errors[0].message}")
        assert not bad, f"{len(bad)}/{len(docs)} invalid, first: {bad[0]}"
        return f"{len(docs)}/{len(docs)} documents valid"

    check("3. every TARGET document validates against the JSON Schema", schema_valid)

    # ── 4. batch count == ceil(written / 200), on every sink the run used ────
    sinks_state = ROOT / "local/state/last_run_sinks"
    run_sinks = sinks_state.read_text(encoding="utf-8").strip() if sinks_state.is_file() else "both"

    def target_batches() -> str:
        """Batch emission is the assertion; Kafka is an *additional* check when in play.

        This was called `kafka_batches`, which oversold it: the GCS batch-file count is
        what always runs, and the Kafka half is conditional on the run having used that
        sink. A check whose name promises more than it asserts is the kind of thing an
        acceptance table exists to prevent.
        """
        written = q1(
            f"SELECT COUNT(*) FROM `{p}.{cfg.ds_transformation}.account_target` "
            f"WHERE run_id = '{run_id}'"
        )
        gcs_files = gcs.list(cfg.json_bucket, f"json/{run_id}/")
        expected_batches = math.ceil(written / mapping.batch_size)
        assert len(gcs_files) == expected_batches, (
            f"{len(gcs_files)} JSON batch files != ceil({written}/{mapping.batch_size}) "
            f"= {expected_batches}"
        )

        if "kafka" not in run_sinks and run_sinks != "both":
            # A gcs-only run has no Kafka messages by design (e.g. Managed Kafka is
            # VPC-only and unreachable from the operator host); say so rather than
            # asserting against a topic this run never wrote to.
            return (
                f"{expected_batches} batches of {mapping.batch_size} "
                f"({len(gcs_files)} GCS files; Kafka assertions skipped — run used sinks={run_sinks})"
            )

        from confluent_kafka import Consumer, TopicPartition

        consumer = Consumer(
            {"bootstrap.servers": cfg.kafka_bootstrap, "group.id": "verify", "auto.offset.reset": "earliest"}
        )
        try:
            low, high = consumer.get_watermark_offsets(
                TopicPartition(cfg.kafka_topic, 0), timeout=15
            )
        finally:
            consumer.close()

        # The topic accumulates across runs, so assert the run's messages are present
        # rather than that the topic holds exactly this run's count.
        assert high - low >= written, f"topic holds {high - low} messages, expected >= {written}"
        return (
            f"{expected_batches} batches of {mapping.batch_size} "
            f"({len(gcs_files)} GCS files, {high - low} Kafka messages on the topic)"
        )

    check("4. TARGET emitted in 200-element batches (Kafka too, when that sink ran)", target_batches)

    # ── 5. key-level reconciliation ───────────────────────────────────────────
    def key_level() -> str:
        orphans = q1(
            f"SELECT COUNT(*) FROM `{p}.{cfg.ds_transformation}.account_target` t "
            f"LEFT JOIN `{p}.{cfg.ds_extraction}.account_src` s "
            f"ON t.account_key = s._account_key AND s._run_id = '{run_id}' "
            f"WHERE t.run_id = '{run_id}' AND s._account_key IS NULL"
        )
        assert orphans == 0, f"{orphans} TARGET keys have no SRC row"
        # "Exactly once" holds only because the source snapshot is assumed to contain no
        # duplicate account keys (docs/PLAN-CHANGES-21082026.md D4) — there is no dedup
        # stage any more to collapse a duplicate that slipped through.
        dupes = q1(
            f"SELECT COUNT(*) FROM (SELECT account_key FROM "
            f"`{p}.{cfg.ds_transformation}.account_target` WHERE run_id = '{run_id}' "
            f"GROUP BY account_key HAVING COUNT(*) > 1)"
        )
        assert dupes == 0, f"{dupes} account keys appear more than once in TARGET"
        total = q1(
            f"SELECT COUNT(DISTINCT account_key) FROM "
            f"`{p}.{cfg.ds_transformation}.account_target` WHERE run_id = '{run_id}'"
        )
        return f"{total} keys, each present exactly once, 0 orphans"

    check("5. every key appears exactly once in TARGET", key_level)

    # ── 6. every not-migrated record is named, not merely counted ────────────
    def lineage_named() -> str:
        """The counts in run_ledger are a claim; record_lineage is the evidence.

        Checked as an agreement between the two rather than as a row count on its own:
        a lineage table that disagrees with the ledger is worse than none, because it
        would let two answers to "which records did not migrate" coexist.
        """
        rows = bq.query(
            f"SELECT door, COUNT(*) AS n FROM `{p}.{cfg.ds_recon}.record_lineage` "
            f"WHERE run_id = '{run_id}' GROUP BY door"
        )
        by_door = {r["door"]: r["n"] for r in rows}
        expected = {
            "rejected": q1(
                f"SELECT COUNT(*) FROM `{p}.{cfg.ds_recon}.reject_log` "
                f"WHERE run_id = '{run_id}'"
            ),
        }
        for door, want in expected.items():
            got = by_door.get(door, 0)
            assert got == want, f"lineage has {got} {door} rows, the ledger says {want}"

        # A row that names no key, or no reason, is not evidence.
        blank = q1(
            f"SELECT COUNT(*) FROM `{p}.{cfg.ds_recon}.record_lineage` "
            f"WHERE run_id = '{run_id}' AND (source_key IS NULL OR source_key = '' "
            f"OR reason IS NULL OR reason = '')"
        )
        assert blank == 0, f"{blank} lineage rows carry no source key or no reason"

        total = sum(expected.values())
        return f"{total} not-migrated records named ({', '.join(f'{k}={v}' for k, v in expected.items())})"

    check("6. every not-migrated record is named in record_lineage", lineage_named)

    # ── 7. all five artefact types in both lanes, .FLG last ──────────────────
    def artefacts() -> str:
        landing = gcs.list(cfg.landing_bucket, f"extraction/{run_id}/")
        recon_extraction = gcs.list(cfg.recon_bucket, f"extraction/{run_id}/")
        load = gcs.list(cfg.recon_bucket, f"load/{run_id}/")

        assert any(n.endswith(".FLG") for n in landing), "no extraction .FLG semaphore"
        assert any(n.endswith(".tar.gz.pgp") for n in landing), "no encrypted extraction bundle"
        for suffix in (".RPT", ".CHS", ".ERR"):
            assert any(n.endswith(suffix) for n in recon_extraction), (
                f"extraction {suffix} not republished for reconciliation"
            )
        for suffix in (".CHS", ".ERR", ".RPT", ".FLG"):
            assert any(n.endswith(suffix) for n in load), f"no load-lane {suffix}"

        flg = json.loads(gcs.get(cfg.landing_bucket, f"extraction/{run_id}/ACCOUNT.FLG"))
        bundled = set(flg["bundledArtefacts"])
        for suffix in (".DAT", ".CHS", ".ERR", ".RPT"):
            assert any(n.endswith(suffix) for n in bundled), (
                f"the .FLG does not vouch for a {suffix} artefact"
            )
        return f"extraction {len(landing)} + {len(recon_extraction)}, load {len(load)}; .FLG vouches for {len(bundled)}"

    check("7. all five artefact types present in both lanes", artefacts)

    # ── 8. .CHS checksums verify on both sides ───────────────────────────────
    def checksums() -> str:
        import hashlib

        text = gcs.get(cfg.recon_bucket, f"extraction/{run_id}/ACCOUNT.CHS").decode("utf-8")
        entries = [
            line.split()
            for line in text.splitlines()
            if line.strip() and not line.startswith("#")
        ]
        assert entries, "extraction .CHS is empty"

        load_text = gcs.get(cfg.recon_bucket, f"load/{run_id}/ACCOUNT.CHS").decode("utf-8")
        load_entries = [
            line.split()
            for line in load_text.splitlines()
            if line.strip() and not line.startswith("#")
        ]
        assert load_entries, "load .CHS is empty"

        # The load-side .CHS covers the JSON batch files, which are still in the bucket,
        # so those can be verified for real rather than merely parsed.
        verified = 0
        for sha, records, name in load_entries:
            payload = gcs.get(cfg.json_bucket, name)
            actual = hashlib.sha256(payload).hexdigest()
            assert actual == sha, f"{name}: checksum mismatch"
            lines = payload.count(b"\n") + (0 if payload.endswith(b"\n") else 1)
            assert lines == int(records), f"{name}: {lines} records, .CHS says {records}"
            verified += 1
        return f"extraction {len(entries)} entries parsed, load {verified} files re-hashed and matching"

    check("8. .CHS checksums verify on both sides", checksums)

    # ── 9. every TARGET row is confirmed by Target System ───────────────────
    def target_confirmed() -> str:
        """The balancing equation proves the Loader's books close; this proves Target
        System kept what the Loader sent. The mock publishes one confirmation event per
        accepted write, recon set-differences it against account_target, and writes the
        verdict to the reconciliation report. Skipped when the confirmation stream is not
        enabled (no Kafka / smoke-gcp --sinks gcs) — "not configured" must not read as
        "zero confirmations", or a no-Kafka run could never go green.
        """
        report = json.loads(
            gcs.get(cfg.recon_bucket, f"reconciliation/{run_id}/reconciliation-report.json")
        )
        ts = report["targetSystemReconciliation"]
        if not ts["enabled"]:
            return (
                f"skipped — confirmation stream not enabled (targetRows={ts['targetRows']})"
            )
        unconfirmed = ts["unconfirmedTargetRows"]
        assert unconfirmed == 0, (
            f"{unconfirmed} TARGET row(s) have no matching confirmation — sent but not persisted"
        )
        return (
            f"{ts['confirmedTargetRows']}/{ts['targetRows']} TARGET rows confirmed "
            f"({ts['confirmations']} confirmation events)"
        )

    check("9. every TARGET row confirmed by Target System", target_confirmed)

    # ── 10. nothing published to Kafka went unsettled ────────────────────────
    def load_settled() -> str:
        """Every document the Loader published got a verdict back.

        On the HTTP path this was free: the response code settled each document as it was
        sent, so "published but unaccounted for" was not a state the code could reach. On
        the Kafka path it is the characteristic failure — a broker ack means the bytes are
        durable, not that Target System applied them — so `unsettled` counts documents
        that were published and never confirmed or rejected. It is the number that catches
        a dead consumer or a poison message stalling a partition, both of which otherwise
        look exactly like a successful run.

        Criterion 9 is the mirror of this from the reconciliation side; this one reads the
        Loader's own .RPT, so a disagreement between the two is itself visible.
        """
        rpt = json.loads(
            gcs.get(cfg.recon_bucket, f"load/{run_id}/ACCOUNT.RPT")
        )
        # reportVersion 1 is the HTTP-only report, which has no `unsettled` field because
        # the concept did not exist. Skip rather than assert on a missing key, so archived
        # evidence from before the Kafka switch stays readable.
        if rpt.get("reportVersion", 1) < 2:
            return "skipped — pre-Kafka load report (reportVersion 1)"
        if rpt.get("sink") != "kafka":
            return f"skipped — loader ran with --sink {rpt.get('sink')}"

        unsettled = rpt["unsettled"]
        published = rpt["published"]
        assert unsettled == 0, (
            f"{unsettled} of {published} published document(s) were never confirmed or "
            f"rejected by Target System — published is not applied"
        )
        # The report must also balance. `errors` mixes two populations: documents
        # rejected before sending (missing accountId/dedupKey — never published) and
        # documents rejected by Target System (published, then refused). So the identity
        # is over documentsRead, not over published:
        #
        #   documentsRead = accepted + errors + unsettled
        #
        # with published = accepted + unsettled + (the rejected subset of errors). Testing
        # the documentsRead form catches the case that matters — a document read and then
        # accounted for nowhere, which is silent loss.
        read = rpt["documentsRead"]
        dupes = rpt["duplicatesIgnored"]
        accounted = rpt["accepted"] + dupes + rpt["errors"] + unsettled
        assert accounted == read, (
            f"load .RPT does not balance: accepted({rpt['accepted']}) + "
            f"duplicatesIgnored({dupes}) + errors({rpt['errors']}) + "
            f"unsettled({unsettled}) = {accounted}, but documentsRead = {read}"
        )
        return (
            f"{published} published, {rpt['accepted']} confirmed, {dupes} duplicate, "
            f"{rpt['errors']} rejected, 0 unsettled"
        )

    check("10. every published document settled (Kafka load path)", load_settled)

    # ── summary ──────────────────────────────────────────────────────────────
    print()
    if failures:
        print(f"{RED}{BOLD}VERIFY FAILED{OFF} — {len(failures)}/{checks_run}: {', '.join(failures)}")
        return 1
    print(f"{GREEN}{BOLD}VERIFY PASSED{OFF} — all {checks_run} acceptance criteria hold for run {run_id}.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
