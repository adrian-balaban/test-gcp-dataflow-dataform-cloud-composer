"""TDS — the layout definition format.

  * one definition is one format — `.DAT` or JSON, never mixed in the same file. The
    record's `layout` decides which: a `json` layout resolves every field by `path`, a
    `fixed` or `csv` layout resolves every field by offset or column. Mixing the two
    kinds of field in one record is rejected at load (`parse_tds`);
  * SRC and TARGET are separate definitions, referenced together only by the mapping.

Every contract in this repo now declares a `csv` layout — pipe-delimited only, the
fixed-width copybook alternative was removed
(docs/PLAN-CHANGES-02092026-kafka-loader.md follow-up). The parser still understands
`offset`/`len` addressing for a `fixed` layout, since no contract declares one that path
is unreachable rather than deleted; `col` is what every `.DAT` field in this repo actually
uses. A `csv` layout may declare `header=true`, in which case the extract's first line
names the columns and is skipped rather than parsed as a record (`RecordParser.is_header`).

Parsing is pure and deterministic: replaying a record yields identical output, which
is what makes at-least-once delivery safe.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Final

from .doors import Reason, RecordError, Stage

MISSING: Final = object()

_TRUE = {"true", "yes", "1"}


@dataclass(frozen=True)
class Field:
    name: str
    offset: int | None = None
    length: int | None = None
    col: int | None = None
    path: str | None = None
    type: str = "string"      # string | int | decimal | date | json
    scale: int = 0
    required: bool = False


@dataclass(frozen=True)
class Layout:
    kind: str                 # fixed | csv | json
    width: int | None = None
    delimiter: str | None = None
    header: bool = False      # csv only — first line names the columns


@dataclass(frozen=True)
class Record:
    name: str
    layouts: dict[str, Layout]
    fields: tuple[Field, ...]

    def field(self, name: str) -> Field:
        for f in self.fields:
            if f.name == name:
                return f
        raise KeyError(f"field {name!r} not defined in record {self.name!r}")

    @property
    def field_names(self) -> tuple[str, ...]:
        return tuple(f.name for f in self.fields)


@dataclass(frozen=True)
class Tds:
    records: dict[str, Record]

    def record(self, name: str) -> Record:
        try:
            return self.records[name]
        except KeyError:
            raise KeyError(
                f"record {name!r} not defined; known records: {sorted(self.records)}"
            ) from None


# ────────────────────────────────────────────────────────────────── definition parsing


def _kv(tokens: list[str], where: str) -> dict[str, str]:
    out: dict[str, str] = {}
    for tok in tokens:
        if "=" not in tok:
            raise ValueError(f"{where}: expected key=value, got {tok!r}")
        k, _, v = tok.partition("=")
        out[k] = v
    return out


def _require_homogeneous(
    name: str, layouts: dict[str, Layout], fields: list[Field]
) -> None:
    """One definition is one format — reject a record that mixes `.DAT` and JSON fields.

    The record's layouts decide the format: a `json` layout addresses every field by
    `path`, a `fixed`/`csv` layout addresses every field by `offset`/`len` or `col`. A
    field carrying the *other* kind's coordinates is the mix this rejects, and it is
    rejected at definition load rather than at the first record that trips over it.
    """
    if not fields:
        return
    is_json = "json" in layouts
    if is_json and len(layouts) > 1:
        raise ValueError(
            f"record {name!r} mixes a json layout with {sorted(set(layouts) - {'json'})}; "
            "one definition is one format"
        )

    for f in fields:
        has_dat = f.offset is not None or f.col is not None
        has_json = f.path is not None
        if is_json and (has_dat or not has_json):
            raise ValueError(
                f"record {name!r} has a json layout, so field {f.name!r} must be "
                "addressed by `path` and carry no offset/len/col"
            )
        if not is_json and (has_json or not has_dat):
            raise ValueError(
                f"record {name!r} has a {sorted(layouts)} layout, so field {f.name!r} "
                "must be addressed by offset/len or col and carry no `path`"
            )


def parse_tds(text: str) -> Tds:
    """Parse a TDS definition. Blank lines and `#` comments are ignored."""
    records: dict[str, Record] = {}
    name: str | None = None
    layouts: dict[str, Layout] = {}
    fields: list[Field] = []

    def flush() -> None:
        if name is None:
            return
        _require_homogeneous(name, layouts, fields)
        records[name] = Record(name=name, layouts=dict(layouts), fields=tuple(fields))

    for lineno, raw in enumerate(text.splitlines(), start=1):
        line = raw.split("#", 1)[0].strip()
        if not line:
            continue
        head, *rest = line.split()
        where = f"line {lineno}"

        if head == "record":
            flush()
            name, layouts, fields = rest[0], {}, []
        elif head == "layout":
            if name is None:
                raise ValueError(f"{where}: `layout` outside a record")
            kind, attrs = rest[0], _kv(rest[1:], where)
            layouts[kind] = Layout(
                kind=kind,
                width=int(attrs["width"]) if "width" in attrs else None,
                delimiter=attrs.get("delimiter"),
                header=attrs.get("header", "false").lower() in _TRUE,
            )
        elif head == "field":
            if name is None:
                raise ValueError(f"{where}: `field` outside a record")
            attrs = _kv(rest[1:], where)
            fields.append(
                Field(
                    name=rest[0],
                    offset=int(attrs["offset"]) if "offset" in attrs else None,
                    length=int(attrs["len"]) if "len" in attrs else None,
                    col=int(attrs["col"]) if "col" in attrs else None,
                    path=attrs.get("path"),
                    type=attrs.get("type", "string"),
                    scale=int(attrs.get("scale", 0)),
                    required=attrs.get("required", "false").lower() in _TRUE,
                )
            )
        else:
            raise ValueError(f"{where}: unknown directive {head!r}")

    flush()
    if not records:
        raise ValueError("TDS definition contains no records")
    return Tds(records=records)


def load_tds(path: str | Path) -> Tds:
    return parse_tds(Path(path).read_text(encoding="utf-8"))


# ──────────────────────────────────────────────────────────────────── value resolution


def resolve_path(doc: Any, path: str) -> Any:
    """Resolve a `$.a.b` path against a decoded JSON document.

    Deliberately a small dotted-path resolver rather than a general JSONPath engine:
    this runs once per field per record on the hot path, and the TDS only ever needs
    object traversal.
    """
    if not path.startswith("$"):
        raise ValueError(f"path must start with '$': {path!r}")
    cur = doc
    for part in path[1:].split("."):
        if not part:
            continue
        if not isinstance(cur, dict) or part not in cur:
            return MISSING
        cur = cur[part]
    return cur


def _coerce(field: Field, raw: Any) -> Any:
    """Apply the declared type. Raises RecordError with an enumerated reason."""
    if field.type == "json":
        return raw

    if isinstance(raw, str):
        raw = raw.strip()
        if raw == "":
            return None
    elif raw is None:
        return None

    if field.type == "string":
        return str(raw)

    if field.type == "int":
        try:
            return int(str(raw).lstrip("0") or "0")
        except ValueError:
            raise RecordError(
                Stage.PARSE, Reason.PARSE_BAD_NUMERIC, f"{field.name}={raw!r}"
            ) from None

    if field.type == "decimal":
        try:
            value = Decimal(str(raw))
        except (InvalidOperation, ValueError):
            raise RecordError(
                Stage.PARSE, Reason.PARSE_BAD_NUMERIC, f"{field.name}={raw!r}"
            ) from None
        return value.quantize(Decimal(1).scaleb(-field.scale)) if field.scale else value

    if field.type == "date":
        text = str(raw)
        try:
            if len(text) == 8 and text.isdigit():
                return date(int(text[:4]), int(text[4:6]), int(text[6:8]))
            return date.fromisoformat(text)
        except ValueError:
            raise RecordError(
                Stage.PARSE, Reason.PARSE_BAD_DATE, f"{field.name}={raw!r}"
            ) from None

    raise ValueError(f"unsupported TDS type {field.type!r} on field {field.name!r}")


# ───────────────────────────────────────────────────────────────────── record parsing


class RecordParser:
    """Parses one raw source line into a field dict, per a record and a layout kind."""

    def __init__(self, record: Record, layout_kind: str) -> None:
        if layout_kind not in record.layouts:
            raise KeyError(
                f"record {record.name!r} has no {layout_kind!r} layout; "
                f"available: {sorted(record.layouts)}"
            )
        self.record = record
        self.layout = record.layouts[layout_kind]

    @property
    def has_header(self) -> bool:
        return bool(self.layout.header)

    def is_header(self, raw: str) -> bool:
        """True when `raw` is the extract's header line rather than a record.

        Recognised by content, not by position: the Beam path reads a file as an
        unordered PCollection of lines, so "the first line" is not a notion it has.
        """
        if not self.layout.header or self.layout.kind != "csv":
            return False
        columns = [c.strip() for c in raw.rstrip("\n").split(self.layout.delimiter or "|")]
        return columns[: len(self.record.fields)] == list(self.record.field_names)

    def _parse_json(self, raw: str) -> dict[str, Any]:
        text = raw.strip()
        if not text:
            raise RecordError(Stage.PARSE, Reason.PARSE_INVALID_JSON, "empty document")
        try:
            doc = json.loads(text)
        except json.JSONDecodeError as exc:
            raise RecordError(
                Stage.PARSE, Reason.PARSE_INVALID_JSON, f"{exc.msg} at pos {exc.pos}"
            ) from None

        out: dict[str, Any] = {}
        for f in self.record.fields:
            assert f.path is not None  # guaranteed by _require_homogeneous
            value = resolve_path(doc, f.path)
            if value is MISSING:
                if f.required:
                    raise RecordError(
                        Stage.PARSE, Reason.PARSE_MISSING_JSON_PATH, f"{f.name} @ {f.path}"
                    )
                out[f.name] = None
                continue
            out[f.name] = _coerce(f, value)
        return out

    def _parse_dat(self, raw: str) -> dict[str, Any]:
        columns: list[str] | None = None

        if self.layout.kind == "fixed":
            width = self.layout.width or 0
            if len(raw) < width:
                raise RecordError(
                    Stage.PARSE,
                    Reason.PARSE_SHORT_RECORD,
                    f"expected >= {width} bytes, got {len(raw)}",
                )
        else:
            columns = raw.split(self.layout.delimiter or "|")

        out: dict[str, Any] = {}
        for f in self.record.fields:
            if self.layout.kind == "fixed":
                if f.offset is None or f.length is None:
                    raise ValueError(f"DAT field {f.name!r} lacks offset/len for fixed layout")
                cell: Any = raw[f.offset : f.offset + f.length]
            else:
                if f.col is None:
                    raise ValueError(f"DAT field {f.name!r} lacks col for csv layout")
                if columns is None or f.col >= len(columns):
                    raise RecordError(
                        Stage.PARSE,
                        Reason.PARSE_BAD_COLUMN_COUNT,
                        f"{f.name} needs column {f.col}, row has {len(columns or [])}",
                    )
                cell = columns[f.col]

            value = _coerce(f, cell)
            if f.required and (value is None or value == ""):
                raise RecordError(Stage.PARSE, Reason.PARSE_MISSING_FIELD, f.name)
            out[f.name] = value

        return out

    def parse(self, raw: str) -> dict[str, Any]:
        """Parse one record. The layout kind, not any per-field flag, picks the reader."""
        raw = raw.rstrip("\n")
        if self.layout.kind == "json":
            return self._parse_json(raw)
        return self._parse_dat(raw)
