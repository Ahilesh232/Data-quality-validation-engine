"""CSV ingestion: header check, type casting, normalisation and staging.

Rows that cannot be parsed (wrong field count, wrong type) never reach the
staging table; they are written straight to dq_failed_record with the original
text so they stay traceable.
"""
from __future__ import annotations

import csv
import hashlib
import json
import math
import os
from collections import Counter
from datetime import datetime, timezone

from .config import SQL_TYPES, Dataset

TS_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


class SchemaError(Exception):
    """File-level problem: nothing from the file can be trusted."""
    rule_id = "schema_columns"


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime(TS_FORMAT)


def parse_timestamp(s: str) -> str:
    """Parse ISO-8601 (naive treated as UTC) -> normalised UTC 'YYYY-MM-DDTHH:MM:SSZ'."""
    dt = datetime.fromisoformat(s.strip().replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).strftime(TS_FORMAT)


def to_datetime(s: str) -> datetime:
    """Parse a stored date / timestamp string back into an aware datetime."""
    if "T" in s:
        return datetime.strptime(s, TS_FORMAT).replace(tzinfo=timezone.utc)
    return datetime.strptime(s, "%Y-%m-%d").replace(tzinfo=timezone.utc)


def parse_value(raw, col):
    """Cast one raw CSV cell. Blank -> None. Raises ValueError on a bad value."""
    if raw is None or raw.strip() == "":
        return None
    s = raw.strip()
    t = col.type
    if t == "string":
        return s.lower() if col.normalize == "lower" else s.upper() if col.normalize == "upper" else s
    if t == "integer":
        return int(s)
    if t == "number":
        v = float(s)
        if math.isnan(v) or math.isinf(v):
            raise ValueError("not a finite number")
        return v
    if t == "date":
        return datetime.strptime(s, "%Y-%m-%d").date().isoformat()
    if t == "timestamp":
        return parse_timestamp(s)
    raise ValueError(f"unsupported type {t}")


def file_sha256(path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _fail(run_id, dataset, rule_id, source_file, row, key, reason, raw_json, now):
    fid = hashlib.sha1(f"{run_id}|{dataset}|{rule_id}|{source_file}|{row}".encode()).hexdigest()[:16]
    return (fid, run_id, dataset, rule_id, "error", source_file, row, key, reason, raw_json, now)


def stage_file(conn, ds: Dataset, path: str, run_id: str, batch_size: int, now: str) -> dict:
    """Create stg_<dataset> and load parseable rows. Returns counters."""
    stg = f"stg_{ds.name}"
    conn.execute(f"DROP TABLE IF EXISTS {stg}")
    coldefs = ", ".join(f"{c.name} {SQL_TYPES[c.type]}" for c in ds.columns)
    conn.execute(f"CREATE TABLE {stg} (source_row INTEGER PRIMARY KEY, source_file TEXT NOT NULL, "
                 f"run_id TEXT NOT NULL, {coldefs})")
    insert_stg = f"INSERT INTO {stg} VALUES ({', '.join('?' * (3 + len(ds.columns)))})"
    insert_fail = ("INSERT INTO dq_failed_record(failure_id, run_id, dataset, rule_id, severity, "
                   "source_file, source_row, record_key, reason, raw_record, detected_at) "
                   "VALUES (?,?,?,?,?,?,?,?,?,?,?)")
    source_file = os.path.basename(path)
    key_cols = list(ds.key) + ([ds.version_column] if ds.version_column else [])
    good, fails = [], []
    n = n_staged = unparseable = 0
    counts = Counter()

    def flush():
        if good:
            conn.executemany(insert_stg, good)
            good.clear()
        if fails:
            conn.executemany(insert_fail, fails)
            fails.clear()

    with open(path, newline="", encoding="utf-8-sig") as f:
        reader = csv.reader(f)
        try:
            header = [h.strip().lower() for h in next(reader)]
        except StopIteration:
            raise SchemaError("file is empty: no header row") from None
        if len(set(header)) != len(header):
            raise SchemaError("duplicate column names in header")
        missing = [c for c in ds.col_names if c not in header]
        if missing:
            raise SchemaError(f"missing required columns: {missing}")
        extra = [h for h in header if h not in ds.col_names]
        pos = {name: header.index(name) for name in ds.col_names}

        for raw in reader:
            if not raw or all(x.strip() == "" for x in raw):
                continue                              # blank line, not a record
            n += 1
            if len(raw) != len(header):
                counts["schema_malformed_row"] += 1
                unparseable += 1
                fails.append(_fail(run_id, ds.name, "schema_malformed_row", source_file, n, None,
                                   f"expected {len(header)} fields, found {len(raw)}",
                                   json.dumps({"_raw_fields": raw}), now))
            else:
                values, errors = [], []
                for c in ds.columns:
                    cell = raw[pos[c.name]]
                    try:
                        values.append(parse_value(cell, c))
                    except ValueError:
                        values.append(None)
                        errors.append(f"column '{c.name}': cannot cast {cell!r} to {c.type}")
                if errors:
                    counts["schema_type"] += 1
                    unparseable += 1
                    key = "|".join(raw[pos[k]].strip() for k in key_cols)
                    fails.append(_fail(run_id, ds.name, "schema_type", source_file, n, key,
                                       "; ".join(errors), json.dumps(dict(zip(header, raw))), now))
                else:
                    n_staged += 1
                    good.append((n, source_file, run_id, *values))
            if len(good) + len(fails) >= batch_size:
                flush()
        flush()

    results = [
        ("schema_columns", "schema", "warn", len(header), len(extra),
         f"unexpected columns ignored: {extra}" if extra else "header matches schema"),
        ("schema_malformed_row", "schema", "error", n, counts["schema_malformed_row"],
         "rows whose field count differs from the header"),
        ("schema_type", "schema", "error", n, counts["schema_type"],
         "rows with a value that cannot be cast to its declared type"),
    ]
    return {"rows_read": n, "rows_staged": n_staged, "rows_unparseable": unparseable,
            "source_file": source_file, "schema_results": results}
