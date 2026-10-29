"""Load and validate the YAML pipeline configuration.

All identifiers (dataset, column, rule ids) must be snake_case. They are
interpolated into SQL, so they are validated here rather than escaped later.
"""
from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path

import yaml

IDENT = re.compile(r"^[a-z][a-z0-9_]*$")
SQL_TYPES = {"string": "TEXT", "integer": "INTEGER", "number": "REAL",
             "date": "TEXT", "timestamp": "TEXT"}
SEVERITIES = ("error", "warn")
ROW_RULES = {"not_null", "unique", "referential", "range", "accepted_values", "custom_sql", "freshness"}
DATASET_RULES = {"freshness", "row_count", "max_reject_rate"}
PUBLISH_MODES = {"upsert", "event_history"}


class ConfigError(ValueError):
    """Raised when the pipeline configuration is invalid."""


@dataclass(frozen=True)
class Column:
    name: str
    type: str
    nullable: bool = True
    normalize: str | None = None      # "lower" | "upper" | None (strings only)
    description: str = ""


@dataclass(frozen=True)
class Derived:
    name: str
    type: str
    expr: str                          # SQL expression over the dataset's columns


@dataclass(frozen=True)
class Rule:
    id: str
    type: str
    severity: str
    params: dict

    @property
    def is_dataset_level(self) -> bool:
        if "level" in self.params:
            return self.params["level"] == "dataset"
        return self.type in {"row_count", "max_reject_rate"} or (self.type == "freshness" and self.params.get("level") != "row")


@dataclass(frozen=True)
class Dataset:
    name: str
    source_file: str
    target_table: str
    key: tuple
    mode: str                          # upsert | event_history
    history_table: str | None
    version_column: str | None
    columns: tuple
    derived: tuple
    rules: tuple
    reconcile_sum: tuple

    @property
    def col_names(self) -> list:
        return [c.name for c in self.columns]


@dataclass(frozen=True)
class PipelineConfig:
    datasets: tuple
    sha256: str
    batch_size: int = 10000

    def dataset(self, name: str) -> Dataset:
        for d in self.datasets:
            if d.name == name:
                return d
        raise ConfigError(f"unknown dataset '{name}'")


def _ident(value, what):
    if not isinstance(value, str) or not IDENT.match(value):
        raise ConfigError(f"{what} must be snake_case, got {value!r}")
    return value


def _norm(v):
    """YAML turns bare 2026-01-01 into a date; keep ISO strings instead."""
    return v.isoformat() if isinstance(v, (date, datetime)) else v


def _build_rule(raw: dict, ds_name: str, cols: dict, done: dict) -> Rule:
    raw = dict(raw)
    rid = _ident(raw.pop("id", None), f"{ds_name}: rule id")
    rtype = raw.pop("type", None)
    sev = raw.pop("severity", "error")
    where = f"{ds_name}.{rid}"
    if rtype not in ROW_RULES | DATASET_RULES:
        raise ConfigError(f"{where}: unknown rule type {rtype!r}")
    if sev not in SEVERITIES:
        raise ConfigError(f"{where}: severity must be one of {SEVERITIES}")
    p = {k: _norm(v) for k, v in raw.items()}

    if rtype in {"not_null", "range", "accepted_values", "referential", "freshness"}:
        if p.get("column") not in cols:
            raise ConfigError(f"{where}: 'column' must be a column of {ds_name}")
    if rtype == "unique":
        if not p.get("columns") or any(c not in cols for c in p["columns"]):
            raise ConfigError(f"{where}: 'columns' must list columns of {ds_name}")
        if p.get("keep", "first") not in ("first", "last"):
            raise ConfigError(f"{where}: keep must be first|last")
    elif rtype == "referential":
        ref = done.get(p.get("ref_dataset"))
        if ref is None:
            raise ConfigError(f"{where}: ref_dataset must be declared earlier")
        if p.get("ref_column") not in ref:
            raise ConfigError(f"{where}: ref_column not in {p['ref_dataset']}")
    elif rtype == "range":
        if "min" not in p and "max" not in p:
            raise ConfigError(f"{where}: range needs min and/or max")
    elif rtype == "accepted_values":
        if not isinstance(p.get("values"), list) or not p["values"]:
            raise ConfigError(f"{where}: 'values' must be a non-empty list")
    elif rtype == "custom_sql":
        expr = p.get("expression")
        if not isinstance(expr, str) or ";" in expr:
            raise ConfigError(f"{where}: 'expression' must be one SQL expression")
    elif rtype == "freshness":
        if not isinstance(p.get("max_age_hours"), (int, float)):
            raise ConfigError(f"{where}: max_age_hours required")
    elif rtype == "row_count":
        if "min_rows" not in p and "max_rows" not in p:
            raise ConfigError(f"{where}: row_count needs min_rows and/or max_rows")
    elif rtype == "max_reject_rate":
        if not isinstance(p.get("max_rate"), (int, float)) or not 0 <= p["max_rate"] <= 1:
            raise ConfigError(f"{where}: max_rate must be within [0, 1]")
    return Rule(rid, rtype, sev, p)


def parse_config(raw: dict, text: str = "") -> PipelineConfig:
    if not isinstance(raw, dict) or not raw.get("datasets"):
        raise ConfigError("config needs a non-empty 'datasets' list")
    done: dict = {}
    datasets = []
    for d in raw["datasets"]:
        name = _ident(d.get("name"), "dataset name")
        if name in done:
            raise ConfigError(f"duplicate dataset {name}")
        columns = []
        for c in d.get("columns", []):
            if c.get("type") not in SQL_TYPES:
                raise ConfigError(f"{name}.{c.get('name')}: type must be one of {sorted(SQL_TYPES)}")
            if c.get("normalize") not in (None, "lower", "upper"):
                raise ConfigError(f"{name}.{c['name']}: normalize must be lower|upper")
            columns.append(Column(_ident(c["name"], f"{name} column"), c["type"],
                                  bool(c.get("nullable", True)), c.get("normalize"),
                                  c.get("description", "")))
        if not columns:
            raise ConfigError(f"{name}: no columns")
        cols = {c.name: c for c in columns}
        key = tuple(d.get("key", []))
        if not key or any(k not in cols for k in key):
            raise ConfigError(f"{name}: key must list existing columns")
        pub = d.get("publish", {"mode": "upsert"})
        mode = pub.get("mode")
        if mode not in PUBLISH_MODES:
            raise ConfigError(f"{name}: publish.mode must be one of {sorted(PUBLISH_MODES)}")
        hist = ver = None
        if mode == "event_history":
            hist = _ident(pub.get("history_table"), f"{name} history_table")
            ver = pub.get("version_column")
            if ver not in cols:
                raise ConfigError(f"{name}: version_column must be a column")
        derived = tuple(Derived(_ident(x["name"], f"{name} derived"), x["type"], x["expr"])
                        for x in d.get("derived", []))
        rules = [_build_rule(r, name, cols, done) for r in d.get("rules", [])]
        ids = [r.id for r in rules]
        if len(ids) != len(set(ids)):
            raise ConfigError(f"{name}: duplicate rule ids")
        # nullable:false columns get an automatic not_null error rule.
        explicit = {r.params.get("column") for r in rules if r.type == "not_null"}
        auto = [Rule(f"{name}_{c.name}_required", "not_null", "error", {"column": c.name})
                for c in columns if not c.nullable and c.name not in explicit]
        for s in d.get("reconcile_sum", []):
            if s not in cols:
                raise ConfigError(f"{name}: reconcile_sum column {s} unknown")
        datasets.append(Dataset(name, d.get("source_file", f"{name}.csv"),
                                _ident(d.get("target_table"), f"{name} target_table"),
                                key, mode, hist, ver, tuple(columns), derived,
                                tuple(auto + rules), tuple(d.get("reconcile_sum", []))))
        done[name] = cols
    batch = int(raw.get("ingest", {}).get("batch_size", 10000))
    return PipelineConfig(tuple(datasets), hashlib.sha256(text.encode()).hexdigest(), batch)


def load_config(path) -> PipelineConfig:
    text = Path(path).read_text(encoding="utf-8")
    return parse_config(yaml.safe_load(text), text)
