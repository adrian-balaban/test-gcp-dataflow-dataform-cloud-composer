"""The config-driven transformation engine.

The SRC→TARGET mapping and the batch size are *data*, not code.
A new migration project supplies a YAML file and two TDS definitions; nothing in this
module changes. `make verify-project2` proves it.

Transform functions are pure and deterministic, which is what makes at-least-once replay
safe.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field as dc_field, replace
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Any, Callable

import yaml

from .doors import Reason, RecordError, Stage
from .tds import Record, RecordParser, Tds, load_tds

# ─────────────────────────────────────────────────────────────────────── transforms


def _t_trim(v: Any, _rule: "Rule", _fields: dict[str, Any]) -> Any:
    return v.strip() if isinstance(v, str) else v


def _t_upper(v: Any, _rule: "Rule", _fields: dict[str, Any]) -> Any:
    return v.strip().upper() if isinstance(v, str) else v


def _t_lower(v: Any, _rule: "Rule", _fields: dict[str, Any]) -> Any:
    return v.strip().lower() if isinstance(v, str) else v


def _t_to_iso_date(v: Any, _rule: "Rule", _fields: dict[str, Any]) -> Any:
    if v is None:
        return None
    if isinstance(v, date):
        return v.isoformat()
    return str(v)


def _t_to_decimal(v: Any, rule: "Rule", _fields: dict[str, Any]) -> Any:
    """Emit as a *string* so no precision is lost through JSON float handling."""
    if v is None:
        return None
    try:
        d = v if isinstance(v, Decimal) else Decimal(str(v))
    except Exception:
        raise RecordError(
            Stage.MAP, Reason.MAP_TYPE_ERROR, f"{rule.target}: not numeric: {v!r}"
        ) from None
    return str(d.quantize(Decimal(1).scaleb(-rule.scale)))


def _t_map_enum(v: Any, rule: "Rule", _fields: dict[str, Any]) -> Any:
    if v is None:
        return None
    key = str(v).strip()
    if key not in rule.values:
        raise RecordError(
            Stage.MAP,
            Reason.MAP_UNMAPPED_ENUM_VALUE,
            f"{rule.target}: {key!r} not in {sorted(rule.values)}",
        )
    return rule.values[key]


def _t_passthrough_json(v: Any, _rule: "Rule", _fields: dict[str, Any]) -> Any:
    return v


def _t_strip_trailing_spaces(v: Any, _rule: "Rule", _fields: dict[str, Any]) -> Any:
    """Pipe-delimited target source physical format (contracts/README.md).

    Parse keeps values exactly as they arrive — trailing spaces are stripped here, in
    the transform phase, not at parse, so what changed about a value is always visible
    on the mapping record rather than silently baked into the parser.
    """
    return v.rstrip(" ") if isinstance(v, str) else v


def _t_strip_leading_zeros(v: Any, _rule: "Rule", _fields: dict[str, Any]) -> Any:
    """Leading zeros on numeric fields, stripped in the transform phase (see above)."""
    if not isinstance(v, str):
        return v
    sign = ""
    digits = v
    if digits[:1] in ("+", "-"):
        sign, digits = digits[0], digits[1:]
    stripped = digits.lstrip("0") or "0"
    return sign + stripped


def _t_const(_v: Any, rule: "Rule", _fields: dict[str, Any]) -> Any:
    return rule.value


def _t_concat(_v: Any, rule: "Rule", fields: dict[str, Any]) -> Any:
    """Reads its inputs from the record, not from the rule — rules are shared across
    records and workers, so stashing per-record state on them would race under Beam."""
    return rule.sep.join(str(fields.get(name)) for name in rule.sources)


def _t_passthrough(v: Any, _rule: "Rule", _fields: dict[str, Any]) -> Any:
    """Identity — for values the SQL transformation already put in final form."""
    return v


TRANSFORMS: dict[str, Callable[[Any, "Rule", dict[str, Any]], Any]] = {
    "passthrough": _t_passthrough,
    "trim": _t_trim,
    "upper": _t_upper,
    "lower": _t_lower,
    "to_iso_date": _t_to_iso_date,
    "to_decimal": _t_to_decimal,
    "map_enum": _t_map_enum,
    "passthrough_json": _t_passthrough_json,
    "const": _t_const,
    "concat": _t_concat,
    "strip_trailing_spaces": _t_strip_trailing_spaces,
    "strip_leading_zeros": _t_strip_leading_zeros,
}

@dataclass
class Rule:
    target: str
    source: str | None = None
    transform: str = "trim"
    values: dict[str, Any] = dc_field(default_factory=dict)
    value: Any = None
    sep: str = ""
    sources: tuple[str, ...] = ()
    scale: int = 2


@dataclass(frozen=True)
class Mapping:
    project: str
    source_tds: Tds
    target_tds: Tds
    target_schema_path: Path | None
    source_record: str
    source_layout: str
    target_record: str
    account_key: tuple[str, ...]
    rules: tuple[Rule, ...]
    batch_size: int
    reject_policy: dict[str, str]
    curated_table: str
    curated_aliases: dict[str, str]

    def curated_row_to_fields(self, row: dict[str, Any]) -> dict[str, Any]:
        """Re-key a curated BigQuery row into SRC-TDS field names.

        The mapping rules address TDS field names; the curated table uses SQL-idiomatic
        column names. `curated.aliases` in the mapping YAML is the declared bridge, so
        neither side has to know about the other's naming.
        """
        return {tds_name: row.get(column) for tds_name, column in self.curated_aliases.items()}

    @property
    def src_record(self) -> Record:
        return self.source_tds.record(self.source_record)

    @property
    def tgt_record(self) -> Record:
        return self.target_tds.record(self.target_record)

    def with_layout(self, layout: str) -> "Mapping":
        """Switch between the copybook (`fixed`) and delimited (`csv`) source layouts.

        Both coordinate systems are already declared in the TDS, so this changes which
        one is read — never how records are mapped. Used by `HARNESS_FORMAT` so the
        prototype can demo either extract shape without a code change.
        """
        if layout not in self.src_record.layouts:
            raise KeyError(
                f"record {self.source_record!r} has no {layout!r} layout; "
                f"available: {sorted(self.src_record.layouts)}"
            )
        return replace(self, source_layout=layout)


def load_mapping(path: str | Path, root: str | Path | None = None) -> Mapping:
    """Load a mapping YAML and the two TDS definitions it references."""
    path = Path(path)
    base = Path(root) if root else Path.cwd()
    cfg = yaml.safe_load(path.read_text(encoding="utf-8"))

    target_tds = load_tds(base / cfg["target_tds"])
    target_record_name = cfg.get("target", {}).get("record") or next(iter(target_tds.records))

    rules: list[Rule] = []
    for raw in cfg.get("mappings", []):
        name = raw["target"]
        transform = raw.get("transform", "trim")
        if transform not in TRANSFORMS:
            raise RecordError(
                Stage.MAP,
                Reason.MAP_UNKNOWN_TRANSFORM,
                f"{name}: {transform!r} not in {sorted(TRANSFORMS)}",
            )
        # scale for to_decimal comes from the TARGET TDS, so it stays a schema property
        try:
            scale = target_tds.record(target_record_name).field(name).scale or 2
        except KeyError:
            scale = 2
        rules.append(
            Rule(
                target=name,
                source=raw.get("from"),
                transform=transform,
                values=raw.get("values", {}) or {},
                value=raw.get("value"),
                sep=raw.get("sep", ""),
                sources=tuple(raw.get("from_fields", []) or []),
                scale=scale,
            )
        )

    return Mapping(
        project=cfg["project"],
        source_tds=load_tds(base / cfg["source_tds"]),
        target_tds=target_tds,
        target_schema_path=(base / cfg["target_schema"]) if cfg.get("target_schema") else None,
        source_record=cfg["source"]["record"],
        source_layout=cfg["source"].get("layout", "fixed"),
        target_record=target_record_name,
        account_key=tuple(cfg.get("account_key", []) or []),
        rules=tuple(rules),
        batch_size=int(cfg.get("batch", {}).get("size", 200)),
        reject_policy=cfg.get("reject", {}) or {},
        curated_table=(cfg.get("curated", {}) or {}).get("table", ""),
        curated_aliases=(cfg.get("curated", {}) or {}).get("aliases", {}) or {},
    )


# ────────────────────────────────────────────────────────────────── target assembly


def set_path(doc: dict[str, Any], path: str, value: Any) -> None:
    """Write `value` into `doc` at a `$.a.b` path, creating intermediate objects."""
    parts = [p for p in path[1:].split(".") if p]
    cur = doc
    for part in parts[:-1]:
        nxt = cur.get(part)
        if not isinstance(nxt, dict):
            nxt = {}
            cur[part] = nxt
        cur = nxt
    cur[parts[-1]] = value


def _json_safe(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, dict):
        return {k: _json_safe(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_json_safe(v) for v in value]
    return value


def load_target_validator(path: Path) -> "Draft202012Validator":
    """Build a JSON-Schema validator for the TARGET record from its schema file.

    The one place the TARGET schema is read and compiled. Both the pipeline's
    TransformEngine (in-flight validation) and the acceptance harness (the "100% of
    emitted TARGET JSON validates" check) build the same validator from the same
    ``mapping.target_schema_path``, so the construction lives here rather than in
    two inline copies that can drift apart.
    """
    import json as _json

    from jsonschema import Draft202012Validator

    schema = _json.loads(path.read_text(encoding="utf-8"))
    return Draft202012Validator(schema)


class TransformEngine:
    """Applies one Mapping to parsed SRC records. Holds no per-record state."""

    def __init__(self, mapping: Mapping) -> None:
        self.mapping = mapping
        self.parser = RecordParser(mapping.src_record, mapping.source_layout)
        self._target_paths = {f.name: f.path for f in mapping.tgt_record.fields if f.path}
        self._required = {f.name for f in mapping.tgt_record.fields if f.required}
        self._validator = self._build_validator()

    def _build_validator(self):
        if not self.mapping.target_schema_path:
            return None
        return load_target_validator(self.mapping.target_schema_path)

    # ── the stages that decide a record's disposition, in order ─────────────────

    def parse(self, raw: str) -> dict[str, Any]:
        return self.parser.parse(raw)

    def is_header(self, raw: str) -> bool:
        """True when `raw` is the extract's header line, not a record to parse."""
        return self.parser.is_header(raw)

    def account_key(self, fields: dict[str, Any]) -> str:
        """Stable deterministic account key — the join key reconciliation counts on."""
        parts = [str(fields.get(k, "")) for k in self.mapping.account_key]
        joined = "\x1f".join(parts)
        return hashlib.sha256(joined.encode("utf-8")).hexdigest()[:32]

    def source_key(self, fields: dict[str, Any]) -> str:
        return "|".join(str(fields.get(k, "")) for k in self.mapping.account_key)

    def map_record(self, fields: dict[str, Any], run_id: str) -> dict[str, Any]:
        doc: dict[str, Any] = {}

        for rule in self.mapping.rules:
            if rule.transform == "concat":
                raw_value: Any = None
            elif rule.transform == "const":
                raw_value = None
            else:
                if rule.source is None:
                    raise RecordError(
                        Stage.MAP, Reason.MAP_TYPE_ERROR, f"{rule.target}: no `from` field"
                    )
                raw_value = fields.get(rule.source)

            value = TRANSFORMS[rule.transform](raw_value, rule, fields)
            path = self._target_paths.get(rule.target)
            if path is None:
                raise RecordError(
                    Stage.MAP,
                    Reason.MAP_TYPE_ERROR,
                    f"{rule.target!r} is not defined in the TARGET TDS",
                )
            # An absent optional value is omitted rather than emitted as null: a typed
            # TARGET schema rejects `null` for a string field, and "we don't have this"
            # is better expressed by the key being absent. Required fields still fail
            # loudly, in _check_required below.
            if value is None and rule.target not in self._required:
                continue
            set_path(doc, path, _json_safe(value))

        # Provenance is engine-owned, not mapping-owned: reconciliation always needs it.
        set_path(doc, self._target_paths["runId"], run_id)
        set_path(doc, self._target_paths["sourceKey"], self.source_key(fields))
        set_path(doc, self._target_paths["dedupKey"], self.account_key(fields))

        self._check_required(doc)
        return doc

    def _check_required(self, doc: dict[str, Any]) -> None:
        from .tds import MISSING, resolve_path

        for name in self._required:
            path = self._target_paths.get(name)
            if path is None:
                continue
            value = resolve_path(doc, path)
            if value is MISSING or value is None or value == "":
                raise RecordError(Stage.MAP, Reason.MAP_MISSING_REQUIRED, name)

    def validate(self, doc: dict[str, Any]) -> None:
        """A mapping bug becomes a reject, not bad data in Target System."""
        if self._validator is None:
            return
        errors = sorted(self._validator.iter_errors(doc), key=lambda e: list(e.path))
        if errors:
            first = errors[0]
            location = "/".join(str(p) for p in first.path) or "<root>"
            raise RecordError(
                Stage.SCHEMA, Reason.SCHEMA_INVALID, f"{location}: {first.message}"
            )
