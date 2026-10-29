"""SQLite result store, run metadata and target-table DDL."""
from __future__ import annotations

import hashlib
import sqlite3

from .config import SQL_TYPES, Dataset, PipelineConfig

DQ_DDL = """
CREATE TABLE IF NOT EXISTS dq_run (
  run_id TEXT PRIMARY KEY, status TEXT NOT NULL, started_at TEXT NOT NULL, ended_at TEXT,
  as_of TEXT NOT NULL, config_sha256 TEXT NOT NULL, forced INTEGER NOT NULL DEFAULT 0,
  duration_seconds REAL, failure_reason TEXT);

CREATE TABLE IF NOT EXISTS dq_dataset_run (
  run_id TEXT NOT NULL, dataset TEXT NOT NULL, source_file TEXT, file_sha256 TEXT,
  status TEXT NOT NULL, failure_reason TEXT, started_at TEXT, ended_at TEXT,
  rows_read INTEGER DEFAULT 0, rows_unparseable INTEGER DEFAULT 0, rows_staged INTEGER DEFAULT 0,
  rows_rejected INTEGER DEFAULT 0, rows_warned INTEGER DEFAULT 0, rows_accepted INTEGER DEFAULT 0,
  rows_inserted INTEGER DEFAULT 0, rows_updated INTEGER DEFAULT 0,
  rows_already_loaded INTEGER DEFAULT 0, late_arriving INTEGER DEFAULT 0,
  ingest_seconds REAL, validate_seconds REAL, publish_seconds REAL,
  PRIMARY KEY (run_id, dataset));

CREATE TABLE IF NOT EXISTS dq_rule_result (
  run_id TEXT NOT NULL, dataset TEXT NOT NULL, rule_id TEXT NOT NULL, rule_type TEXT NOT NULL,
  severity TEXT NOT NULL, checked_count INTEGER NOT NULL, failed_count INTEGER NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('PASS','FAIL')), detail TEXT,
  PRIMARY KEY (run_id, dataset, rule_id));

CREATE TABLE IF NOT EXISTS dq_failed_record (
  failure_id TEXT PRIMARY KEY, run_id TEXT NOT NULL, dataset TEXT NOT NULL, rule_id TEXT NOT NULL,
  severity TEXT NOT NULL, source_file TEXT NOT NULL, source_row INTEGER NOT NULL,
  record_key TEXT, reason TEXT NOT NULL, raw_record TEXT, detected_at TEXT NOT NULL);
CREATE INDEX IF NOT EXISTS idx_dq_failed_run ON dq_failed_record(run_id, dataset, severity);

CREATE TABLE IF NOT EXISTS dq_ingest_manifest (
  dataset TEXT NOT NULL, file_sha256 TEXT NOT NULL, config_sha256 TEXT NOT NULL,
  run_id TEXT NOT NULL, source_file TEXT NOT NULL, loaded_at TEXT NOT NULL,
  PRIMARY KEY (dataset, file_sha256));

CREATE TABLE IF NOT EXISTS dq_reconciliation (
  run_id TEXT NOT NULL, dataset TEXT NOT NULL, check_name TEXT NOT NULL,
  expected REAL, actual REAL, passed INTEGER NOT NULL, detail TEXT,
  PRIMARY KEY (run_id, dataset, check_name));
"""


def _sha1_16(*args):
    return hashlib.sha1("|".join("" if a is None else str(a) for a in args).encode()).hexdigest()[:16]


def connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, isolation_level=None)   # explicit BEGIN/COMMIT
    conn.create_function("sha1_16", -1, _sha1_16, deterministic=True)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def table_ddl(ds: Dataset, table: str, pk: list) -> str:
    cols = [f"{c.name} {SQL_TYPES[c.type]}{'' if c.nullable else ' NOT NULL'}" for c in ds.columns]
    cols += [f"{d.name} {SQL_TYPES[d.type]}" for d in ds.derived]
    cols += ["run_id TEXT NOT NULL", "source_file TEXT NOT NULL", "loaded_at TEXT NOT NULL"]
    return f"CREATE TABLE IF NOT EXISTS {table} ({', '.join(cols)}, PRIMARY KEY ({', '.join(pk)}))"


def init_db(conn: sqlite3.Connection, cfg: PipelineConfig) -> None:
    conn.executescript(DQ_DDL)
    for ds in cfg.datasets:
        conn.execute(table_ddl(ds, ds.target_table, list(ds.key)))
        if ds.history_table:
            conn.execute(table_ddl(ds, ds.history_table, list(ds.key) + [ds.version_column]))


def insert_row(conn, table: str, row: dict) -> None:
    cols = ", ".join(row)
    conn.execute(f"INSERT OR REPLACE INTO {table} ({cols}) VALUES ({', '.join('?' * len(row))})",
                 list(row.values()))


def add_rule_result(conn, run_id, dataset, rule_id, rule_type, severity, checked, failed, detail):
    insert_row(conn, "dq_rule_result", dict(
        run_id=run_id, dataset=dataset, rule_id=rule_id, rule_type=rule_type, severity=severity,
        checked_count=checked, failed_count=failed, status="FAIL" if failed else "PASS", detail=detail))
