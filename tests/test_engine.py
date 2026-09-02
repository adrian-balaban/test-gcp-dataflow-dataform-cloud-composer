"""Unit tests for the contract layer and the two-door engine.

These assert the correctness contract directly, without Beam or any emulator, so a
regression in the balancing equation fails in milliseconds rather than at the demo.
"""

from __future__ import annotations

import json
import re
from decimal import Decimal
from pathlib import Path

import pytest

from harness.generate import EXPECTED_REASON, MALFORMED_KINDS, generate
from pipelines.common.doors import Counters, Door, Exit, Reason, RecordError, require_balance
from pipelines.common.engine import (
    Outcome,
    RecordRouter,
    route_intake,
    run_batches,
)
from pipelines.common.mapping import TransformEngine, load_mapping
from pipelines.common.tds import RecordParser, load_tds, parse_tds, resolve_path

ROOT = Path(__file__).resolve().parents[1]
MAPPING_P1 = ROOT / "contracts/mappings/mapping-project1.yaml"


@pytest.fixture(scope="module")
def mapping():
    return load_mapping(MAPPING_P1, root=ROOT)


@pytest.fixture(scope="module")
def engine(mapping):
    return TransformEngine(mapping)


# ────────────────────────────────────────────────────────────────────── TDS parsing


def test_tds_records_are_homogeneous_dat_or_json(mapping):
    """One definition is one format (D6) — SRC is all `.DAT`, TARGET is all JSON."""
    src = mapping.src_record
    assert all(f.path is None for f in src.fields)
    assert all(f.offset is not None or f.col is not None for f in src.fields)

    target = mapping.tgt_record
    assert all(f.path is not None for f in target.fields)
    assert all(f.offset is None and f.col is None for f in target.fields)


def test_mixed_dat_and_json_fields_in_one_record_is_rejected():
    """A record mixing `.DAT` and JSON fields fails to load, not silently parses."""
    with pytest.raises(ValueError, match="one format"):
        parse_tds(
            """
            record MIXED
              layout fixed width=5
              layout json
              field CODE offset=0 len=5
              field EXTRA path=$.extra
            """
        )


def test_a_dat_only_definition_still_parses():
    tds = parse_tds(
        """
        record LEGACY
          layout fixed width=5
          field CODE offset=0 len=5
        """
    )
    assert tds.record("LEGACY").field("CODE").offset == 0


def test_src_and_target_tds_are_separate_definitions(mapping):
    assert mapping.source_tds is not mapping.target_tds
    assert "ACCOUNT" in mapping.source_tds.records
    assert "TARGET_SYSTEM_ACCOUNT" in mapping.target_tds.records


def test_resolve_path_returns_sentinel_for_missing():
    from pipelines.common.tds import MISSING

    assert resolve_path({"a": {"b": 1}}, "$.a.b") == 1
    assert resolve_path({"a": {}}, "$.a.b") is MISSING


# ─────────────────────────────────────────────────────── fixed-width vs CSV parity


FIXED_LINE = (
    "ACC000000001"      # ACCT_ID   12
    "CUS0000001"        # CUST_ID   10
    "RETL"              # CLIENT_TYPE 4
    "SAV001"            # PRODUCT    6
    "eur"               # CURRENCY   3
    "20190314"          # OPEN_DATE  8
    "A"                 # STATUS     1
    "+00000001234.56"   # BALANCE   15
)

CSV_LINE = "ACC000000001|CUS0000001|RETL|SAV001|eur|20190314|A|+00000001234.56"


def test_fixed_width_parse(mapping):
    fields = RecordParser(mapping.src_record, "fixed").parse(FIXED_LINE)
    assert fields["ACCT_ID"] == "ACC000000001"
    assert fields["BALANCE"] == Decimal("1234.56")


def test_csv_layout_yields_identical_fields(mapping):
    """The layout switch is config; the parsed record must be identical either way."""
    fixed = RecordParser(mapping.src_record, "fixed").parse(FIXED_LINE)
    csv = RecordParser(mapping.src_record, "csv").parse(CSV_LINE)
    assert fixed == csv


def test_open_date_parsed_from_yyyymmdd(mapping):
    fields = RecordParser(mapping.src_record, "fixed").parse(FIXED_LINE)
    assert fields["OPEN_DATE"].isoformat() == "2019-03-14"


# ──────────────────────────────────────────────────────────────────────── mapping


def test_map_produces_schema_valid_target_document(engine):
    fields = engine.parse(FIXED_LINE)
    doc = engine.map_record(fields, run_id="run-test")
    engine.validate(doc)

    assert doc["accountId"] == "ACC000000001"
    assert doc["currency"] == "EUR", "upper transform applied"
    assert doc["openedOn"] == "2019-03-14"
    assert doc["status"] == "ACTIVE", "map_enum applied"
    assert doc["balance"] == {"amount": "1234.56", "currency": "EUR"}
    # segment/branch/riskRating are no longer carried on the homogeneous DAT source
    # (D6) — an absent optional value is omitted, not emitted as null, so the whole
    # `attributes` object is absent until a reference-data join lands.
    assert "attributes" not in doc or doc["attributes"].get("segment") is None
    assert doc["migration"]["runId"] == "run-test"


def test_transform_is_deterministic(engine):
    """Replaying a record yields byte-identical output."""
    fields = engine.parse(FIXED_LINE)
    a = json.dumps(engine.map_record(fields, "run-1"), sort_keys=True)
    b = json.dumps(engine.map_record(fields, "run-1"), sort_keys=True)
    assert a == b


def test_account_key_is_stable_and_deterministic(engine):
    fields = engine.parse(FIXED_LINE)
    assert engine.account_key(fields) == engine.account_key(fields)
    assert len(engine.account_key(fields)) == 32


# ────────────────────────────────────────────────────────── reject reason coverage


@pytest.mark.parametrize("kind", MALFORMED_KINDS)
def test_each_malformed_kind_produces_its_enumerated_reason(engine, kind):
    """Seeded malformed records get the *correct* reason code."""
    from harness.generate import _break, _row
    import random

    donor = _row(random.Random(1), 1, "RETL")
    broken = _break(donor, kind, "fixed")

    router = RecordRouter(engine, run_id="run-test")
    outcome = router.route(broken, batch_id=0)

    assert outcome.door is Door.REJECTED, f"{kind} should be rejected"
    assert outcome.reject is not None
    assert outcome.reject.reason == EXPECTED_REASON["fixed"][kind]
    assert outcome.reject.raw_record == broken, "raw record is preserved for triage"


def test_reject_reasons_are_enumerated_not_free_text():
    """Reason codes must be countable and trendable."""
    valid = {r.value for r in Reason}
    for layout_reasons in EXPECTED_REASON.values():
        assert set(layout_reasons.values()) <= valid


# ────────────────────────────────────────────────── the balancing equation itself


def test_counters_balance_arithmetic():
    c = Counters(src_read=7, written=6, rejected=1)
    assert c.balances
    require_balance("unit", c)

    c.rejected += 1
    assert not c.balances
    assert c.imbalance == -1


def test_two_doors_partition_every_disposition():
    """The two-door claim must be exactly the two dispositions, re-layered.

    Guards the framing itself: if a third disposition is ever added, or one stops
    mapping to an exit, `migrated + not_migrated` silently stops equalling `accounted`
    and the headline equation would be provable while the breakdown underneath it lied.
    """
    # Every disposition leaves through exactly one of the two doors.
    assert {d.exit for d in Door} == {Exit.MIGRATED, Exit.NOT_MIGRATED}
    assert [d for d in Door if d.exit is Exit.MIGRATED] == [Door.WRITTEN]
    assert [d for d in Door if d.exit is Exit.NOT_MIGRATED] == [Door.REJECTED]

    c = Counters(src_read=406, written=400, rejected=6)
    assert c.migrated == 400
    assert c.not_migrated == 6
    assert c.migrated + c.not_migrated == c.accounted == c.src_read
    assert c.balances

    broken = Counters(src_read=406, written=400, rejected=7)
    assert broken.migrated + broken.not_migrated != broken.src_read
    assert not broken.balances


@pytest.mark.parametrize("layout", ["fixed", "csv"])
def test_end_to_end_counts_match_the_seeded_manifest(engine, layout, tmp_path):
    """Counts against the harness oracle, for both layouts."""
    lines, manifest = generate(accounts=500, layout=layout, seed=99)

    mapping = load_mapping(MAPPING_P1, root=ROOT).with_layout(layout)
    eng = TransformEngine(mapping)
    router = RecordRouter(eng, run_id="run-oracle")

    total = Counters()
    for batch in run_batches(router, lines, batch_size=200):
        require_balance(f"batch {batch.batch_id}", batch.counters)   # per batch
        total.merge(batch.counters)
    require_balance("run", total)                                    # and per run

    assert total.src_read == manifest.total_records
    assert total.written == manifest.expected_written
    assert total.rejected == manifest.expected_rejected
    assert total.by_reason == manifest.expected_by_reason


def test_batches_are_200_written_documents(engine):
    """Batch count = ceil(written / 200)."""
    lines, manifest = generate(accounts=1000, layout="fixed", seed=7)
    router = RecordRouter(engine, run_id="run-batch")
    batches = list(run_batches(router, lines, batch_size=200))

    full = [b for b in batches if len(b.documents) == 200]
    assert len(full) == manifest.expected_written // 200
    assert sum(len(b.documents) for b in batches) == manifest.expected_written


# ─────────────────────────────── one implementation of the parse and filter doors


def test_beam_intake_and_router_agree_on_every_record(engine):
    """The Beam path and the in-memory router must not drift on parse and filter.

    This is the test that makes the shared `route_intake` load-bearing rather than
    merely tidy: it runs the whole seeded corpus through both callers and asserts they
    classify every record identically. Before the doors were collapsed, a change to one
    implementation could silently disagree with the other.
    """
    lines, _ = generate(accounts=500, layout="fixed", seed=99)

    router = RecordRouter(engine, run_id="run-drift")
    compared = 0

    for raw in lines:
        raw = raw.rstrip("\n")
        if not raw.strip():
            continue

        intake = route_intake(engine, raw)
        routed = router.route(raw, batch_id=0)
        compared += 1

        if intake.door is Door.REJECTED:
            # A parse failure must be a parse failure on both paths, same reason code.
            assert routed.door is Door.REJECTED
            assert intake.reject is not None and routed.reject is not None
            assert intake.reject.reason == routed.reject.reason
            assert intake.reject.stage == routed.reject.stage
        else:
            # Survived intake: the router may still send it through map/schema, but
            # never back out through parse.
            assert routed.door in (Door.WRITTEN, Door.REJECTED)
            if routed.door is not Door.REJECTED:
                assert intake.account_key == routed.account_key

    assert compared > 400, "the corpus should be substantial enough to be meaningful"


def test_beam_dofn_actually_delegates_to_the_shared_door(engine, monkeypatch):
    """`ParseFn` must *call* `route_intake`, not merely agree with it.

    The sibling test above compares outputs, which would still pass if someone
    reintroduced a second, coincidentally-identical implementation in the DoFn. This one
    closes that hole by making the shared door return a known-wrong answer and asserting
    the DoFn's output changes with it — only possible if the delegation is real.

    Replaces an earlier hand-run mutation check that lived in an evidence log and so
    could not fail the build.
    """
    from pipelines.common.doors import Door as D
    from pipelines.common.doors import Reject
    from pipelines.file_processor import pipeline as fp

    fn = fp.ParseFn.__new__(fp.ParseFn)
    fn.run_id = "run-delegation"
    fn.ingested_at = "1970-01-01T00:00:00Z"
    fn.engine = engine
    # Metrics counters are Beam-side bookkeeping; the routing decision is what is under
    # test, so a no-op stand-in keeps this runner-free.
    fn.c_rejected = fn.c_written = type("NoOp", (), {"inc": lambda self: None})()

    # The record must be *valid and parseable*. A junk line would be rejected by any
    # inlined re-implementation too, and an exception fallback to `route_intake` would
    # then mask the very drift this test exists to detect — a real trap, hit while
    # writing this test.
    lines, _ = generate(accounts=5, layout="fixed", seed=11)
    raw = next(
        l.rstrip("\n") for l in lines
        if l.strip() and route_intake(engine, l.rstrip("\n")).door is D.WRITTEN
    )
    sentinel = "STUB_SAW_THIS_RECORD"

    def stub(_engine, _raw):
        return Outcome(
            door=D.REJECTED,
            raw=_raw,
            source_key="",
            reject=Reject(
                run_id="", batch_id=0, source_key=sentinel,
                stage="parse", reason="PARSE_SHORT_RECORD", detail="stub",
                raw_record=_raw,
            ),
        )

    monkeypatch.setattr(fp, "route_intake", stub)
    out = list(fn.process(("SOME.DAT", raw)))

    # Undoctored, this record routes to the main output (WRITTEN). Seeing the stub's
    # REJECTED sentinel instead is only possible if the DoFn really called route_intake.
    assert len(out) == 1, "one record in, one output out"
    assert getattr(out[0], "tag", None) == fp.ParseFn.REJECTED, (
        "ParseFn ignored the stubbed route_intake and routed the record itself — it "
        "has grown a second implementation of the parse door, which is the drift "
        "this refactor exists to prevent"
    )
    assert out[0].value["source_key"] == sentinel


def test_route_intake_does_not_run_the_map_door(engine):
    """Intake must stop before map — enrichment fields do not exist yet.

    `BRANCH_NAME` is contributed by the Data Enrichment pipeline, so mapping at intake
    time would reject every record with MAP_MISSING_REQUIRED. This test pins the split
    that makes the file processor correct, and would fail if someone "simplified"
    `ParseFn` into calling the full two-door `route()`.
    """
    lines, _ = generate(accounts=20, layout="fixed", seed=3)
    survivors = [
        o for o in (route_intake(engine, l.rstrip("\n")) for l in lines if l.strip())
        if o.door is Door.WRITTEN
    ]
    assert survivors, "expected records to survive intake"

    for outcome in survivors:
        # `doc` carries the *parsed source fields*, not a mapped TARGET document.
        assert outcome.doc is not None
        assert "ACCT_ID" in outcome.doc, "intake yields SRC-TDS fields"
        assert "accountId" not in outcome.doc, "intake must not have mapped to TARGET"
        assert "BRANCH_NAME" not in outcome.doc, "enrichment has not run yet"


# ── the cross-language contract ───────────────────────────────────────────────


def test_artefact_names_come_from_the_manifest_not_from_code():
    """#1 — the naming convention must be loaded, not restated.

    The convention used to be declared twice, once in `_artefact_names` and once in
    `Artefacts.java`, kept in step by a comment in each. This asserts the Python side
    now derives every name from `contracts/artefacts.json`, so the manifest is the only
    place a rename can happen.
    """
    from pipelines.common import artefacts

    manifest = json.loads(artefacts.manifest_path().read_text(encoding="utf-8"))
    names = artefacts.artefact_names("ACCOUNT")

    for key, template in manifest["artefacts"].items():
        if key == "dat":
            continue
        assert names[key] == template.replace("{record}", "ACCOUNT"), (
            f"{key} is not derived from the manifest template"
        )

    width = manifest["dat_sequence_width"]
    assert artefacts.dat("ACCOUNT", 7) == manifest["artefacts"]["dat"].replace(
        "{record}", "ACCOUNT"
    ).replace("{sequence}", "7".zfill(width))


def test_java_ships_the_same_manifest_python_reads():
    """#1 — one file, two languages: the jar must not carry a stale copy.

    Maven copies `contracts/artefacts.json` onto apps/common's classpath, so the Java
    side reads the same bytes Python does. If the build output has drifted from the
    source the two languages are back to agreeing by luck, which is the failure this
    manifest exists to prevent — so compare the bytes rather than trusting the build.

    Skipped when the Java module has not been built; `make test` builds it first.
    """
    from pipelines.common import artefacts

    built = ROOT / "apps/common/target/classes/artefacts.json"
    if not built.is_file():
        pytest.skip("apps/common not built — run `make java-build`")

    assert built.read_bytes() == artefacts.manifest_path().read_bytes(), (
        "the manifest on Java's classpath differs from contracts/artefacts.json — "
        "rebuild with `make java-build`"
    )


def test_column_lookups_resolve_the_names_the_sql_depends_on():
    """#1 — the manifest's column names must be the ones the physical objects carry.

    Mirrors `ArtefactsTest.columnLookupsResolveTheNamesTheSqlDependsOn` on the Java side,
    but the oracle cannot be the manifest itself: `artefacts.columns()` reads that same
    file, so comparing the two only asserts that a dict round-trips. The drift worth
    catching is between the manifest and the objects it names — the BigQuery schema this
    repo writes `account_src` with, and the SELECT list Dataform publishes
    `account_curated` with. recon-service joins those two tables on names it takes from
    the manifest, so a rename on one side that skips the manifest produces a query
    returning zero rows rather than one that fails, which is the dangerous shape.
    """
    from pipelines.common import artefacts
    from pipelines.common.schema import INGEST_FIELDS

    # src side: every name the manifest hands out must be a column account_src actually
    # has. Subset, not equality — an ingest field no cross-language reader needs may stay
    # out of the manifest; a manifest entry with no column behind it may not.
    ingested = {f.name for f in INGEST_FIELDS}
    missing = {
        logical: physical
        for logical, physical in artefacts.columns("src").items()
        if physical not in ingested
    }
    assert not missing, (
        f"manifest src columns absent from the account_src schema: {missing} — "
        f"pipelines/common/schema.py and contracts/artefacts.json have drifted"
    )

    # target side: the curated table is produced by SQL rather than a Python schema, so
    # the aliases in the model's SELECT list are its physical column names.
    curated = (ROOT / "dataform/definitions/account_curated.sqlx").read_text(encoding="utf-8")
    for logical, physical in artefacts.columns("target").items():
        assert re.search(rf"\bAS\s+{re.escape(physical)}\s*,?\s*$", curated, re.MULTILINE), (
            f"manifest target column {logical!r} resolves to {physical!r}, which "
            f"account_curated.sqlx does not produce"
        )


# ── the backend selection ─────────────────────────────────────────────────────


def _config(**overrides):
    """A Config built directly, bypassing the environment."""
    from pipelines.common.config import Config

    base = dict(
        profile="local", project="mig-local", storage_host="", landing_bucket="",
        json_bucket="", recon_bucket="", bq_target="emulator", bq_host="",
        ds_extraction="", ds_transformation="", ds_recon="", kafka_bootstrap="",
        kafka_topic="", kafka_batch_size=200, kafka_security_protocol="",
        target_system_url="", target_system_max_retries=1, pgp_home="", pgp_recipient="",
        target_system_confirmation_bootstrap="", target_system_confirmation_topic="",
        target_system_rejection_topic="", loader_sink="http",
        loader_settle_timeout_seconds=120,
        mapping="contracts/mappings/mapping-project1.yaml",
    )
    base.update(overrides)
    return Config(**base)


def test_incoherent_backend_combinations_fail_at_startup(monkeypatch):
    """#2 — a misconfiguration must be an error, not a quietly wrong run.

    `TARGET_PROFILE=real` + `BQ_TARGET=emulator` used to be accepted and meant real GCS
    with a throwaway warehouse; nothing failed, the run just wrote half its output where
    nobody was looking.
    """
    from pipelines.common.config import ConfigError, Runner

    monkeypatch.delenv("MIG_RUNNER", raising=False)

    with pytest.raises(ConfigError, match="BQ_TARGET=emulator"):
        _config(profile="real", bq_target="emulator").validate()

    monkeypatch.setenv("MIG_RUNNER", "dataflow")
    with pytest.raises(ConfigError, match="cannot reach the emulators"):
        _config(profile="local").validate()

    # …while the two deliberate variations stay legal.
    monkeypatch.setenv("MIG_RUNNER", "direct")
    assert _config(profile="real", bq_target="real").validate().runner is Runner.DIRECT
    assert _config(profile="local", bq_target="real").validate().bq_is_emulator is False


def test_runner_and_endpoints_are_decided_together(monkeypatch):
    """#2 — one resolved backend, not two components reading env independently."""
    from pipelines.common.config import Backend, Runner

    monkeypatch.delenv("MIG_RUNNER", raising=False)
    local = _config().validate()
    assert (local.backend, local.runner, local.is_local) == (Backend.LOCAL, Runner.DIRECT, True)

    gcp = _config(profile="real", bq_target="real").validate()
    assert (gcp.backend, gcp.runner, gcp.is_local) == (Backend.GCP, Runner.DATAFLOW, False)


# ── the BigQuery write path ───────────────────────────────────────────────────


def test_backend_chooses_the_write_path(monkeypatch):
    """#3 — the writer seam must be real, not two classes with one body.

    Both writers used to have identical `insertAll` bodies *and* no callers at all: the
    seam existed in the type system and nowhere on the data path. This pins the choice
    to the backend, so a GCP run cannot silently keep streaming rows.
    """
    from pipelines.common.sinks import (
        FileLoadsBigQueryWriter,
        InsertAllBigQueryWriter,
        bigquery_writer,
    )

    monkeypatch.setattr("pipelines.common.sinks.BigQuery", lambda cfg: None)
    monkeypatch.setattr("pipelines.common.sinks.Gcs", lambda cfg: None)

    assert isinstance(bigquery_writer(_config(bq_target="emulator")), InsertAllBigQueryWriter)
    gcp = bigquery_writer(_config(profile="real", bq_target="real"))
    assert isinstance(gcp, FileLoadsBigQueryWriter)
    assert gcp.staging_bucket == "mig-local-dataflow-temp"


def test_small_writes_skip_the_staging_round_trip(monkeypatch):
    """#3 — a load job per handful of rows would burn the 1,500/table/day quota."""
    from pipelines.common.sinks import FileLoadsBigQueryWriter

    monkeypatch.setattr("pipelines.common.sinks.BigQuery", lambda cfg: None)
    monkeypatch.setattr("pipelines.common.sinks.Gcs", lambda cfg: None)
    writer = FileLoadsBigQueryWriter(_config(profile="real", bq_target="real"))

    staged: list[str] = []
    writer.gcs = type("G", (), {"put": lambda self, *a: staged.append(a[1]),
                                "delete": lambda self, *a: None})()
    writer.fallback = type("F", (), {"write": lambda self, d, t, rows: len(list(rows))})()

    assert writer.write("ds", "t", [{"a": i} for i in range(10)]) == 10
    assert staged == [], "a 10-row write should not stage a file"
