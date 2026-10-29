"""Publish accepted rows (acc_<dataset>) to the target tables, idempotently."""
from __future__ import annotations

from .config import Dataset


def _all_cols(ds: Dataset) -> list:
    return ds.col_names + [d.name for d in ds.derived] + ["run_id", "source_file", "loaded_at"]


def _select_exprs(ds: Dataset) -> str:
    return ", ".join(ds.col_names + [d.expr for d in ds.derived] + ["run_id", "source_file", "?"])


def _match(keys, a, b):
    return " AND ".join(f"{a}.{k} = {b}.{k}" for k in keys)


def publish(conn, ds: Dataset, run_id: str, now: str) -> dict:
    """Returns counters: rows_inserted, rows_updated, rows_already_loaded, late_arriving."""
    acc, cols = f"acc_{ds.name}", _all_cols(ds)
    collist, sel = ", ".join(cols), _select_exprs(ds)
    out = {"rows_inserted": 0, "rows_updated": 0, "rows_already_loaded": 0, "late_arriving": 0}

    if ds.mode == "upsert":
        t = ds.target_table
        out["rows_updated"] = conn.execute(
            f"SELECT COUNT(*) FROM {acc} a WHERE EXISTS (SELECT 1 FROM {t} t WHERE {_match(ds.key, 't', 'a')})"
        ).fetchone()[0]
        total = conn.execute(f"SELECT COUNT(*) FROM {acc}").fetchone()[0]
        out["rows_inserted"] = total - out["rows_updated"]
        sets = ", ".join(f"{c}=excluded.{c}" for c in cols if c not in ds.key)
        conn.execute(f"INSERT INTO {t} ({collist}) SELECT {sel} FROM {acc} WHERE 1 ORDER BY source_row "
                     f"ON CONFLICT ({', '.join(ds.key)}) DO UPDATE SET {sets}", [now])
        return out

    # event_history: append-only history + "current" = latest version per key.
    cur, hist, ver = ds.target_table, ds.history_table, ds.version_column
    out["late_arriving"] = conn.execute(
        f"SELECT COUNT(*) FROM {acc} a WHERE EXISTS (SELECT 1 FROM {cur} c WHERE "
        f"{_match(ds.key, 'c', 'a')} AND c.{ver} > a.{ver})").fetchone()[0]
    out["rows_already_loaded"] = conn.execute(
        f"SELECT COUNT(*) FROM {acc} a WHERE EXISTS (SELECT 1 FROM {hist} h WHERE "
        f"{_match(ds.key, 'h', 'a')} AND h.{ver} = a.{ver})").fetchone()[0]
    out["rows_inserted"] = conn.execute(
        f"INSERT INTO {hist} ({collist}) SELECT {sel} FROM {acc} WHERE 1 ORDER BY source_row "
        f"ON CONFLICT DO NOTHING", [now]).rowcount
    conn.execute(
        f"INSERT OR REPLACE INTO {cur} ({collist}) SELECT {collist} FROM {hist} h "
        f"WHERE EXISTS (SELECT 1 FROM {acc} a WHERE {_match(ds.key, 'a', 'h')}) "
        f"AND h.{ver} = (SELECT MAX(h2.{ver}) FROM {hist} h2 WHERE {_match(ds.key, 'h2', 'h')})")
    return out


def source_target_sums(conn, ds: Dataset) -> list:
    """(column, accepted-sum, target-sum) for each reconcile_sum column."""
    acc = f"acc_{ds.name}"
    target = ds.history_table or ds.target_table
    on = _match(ds.key, "a", "t") + (f" AND a.{ds.version_column} = t.{ds.version_column}"
                                      if ds.version_column else "")
    rows = []
    for c in ds.reconcile_sum:
        exp = conn.execute(f"SELECT COALESCE(SUM({c}), 0) FROM {acc}").fetchone()[0]
        act = conn.execute(f"SELECT COALESCE(SUM(t.{c}), 0) FROM {acc} a JOIN {target} t ON {on}").fetchone()[0]
        rows.append((c, exp, act))
    return rows
