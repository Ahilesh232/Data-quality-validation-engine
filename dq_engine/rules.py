"""Rule evaluation. Row-level rules insert failures into dq_failed_record;
dataset-level rules return (checked, failed, detail) and can block publishing.

Row-level rules are SQL over stg_<dataset>. A rule "fails" a row when the
condition is definitely violated; NULL operands are *not applicable* (the
not_null rules own missing-value detection).
"""
from __future__ import annotations

from .config import Dataset, PipelineConfig, Rule
from .ingest import to_datetime


def key_sql(ds: Dataset, alias: str = "s") -> str:
    cols = list(ds.key) + ([ds.version_column] if ds.version_column else [])
    return " || '|' || ".join(f"COALESCE(CAST({alias}.{c} AS TEXT), '')" for c in cols)


def raw_json_sql(ds: Dataset, alias: str = "s") -> str:
    return "json_object(" + ", ".join(f"'{c.name}', {alias}.{c.name}" for c in ds.columns) + ")"


def _bound(v, as_of):
    return as_of if v == "$as_of" else v


def fail_query(ds: Dataset, rule: Rule, cfg: PipelineConfig, as_of: str):
    """Return (SQL selecting failing source_row values, bind params, reason text)."""
    t, p, col = f"stg_{ds.name}", rule.params, rule.params.get("column")
    if rule.type == "not_null":
        return f"SELECT source_row FROM {t} WHERE {col} IS NULL", [], f"column '{col}' is null"
    if rule.type == "unique":
        cols = p["columns"]
        keep = p.get("keep", "first")
        order = "ASC" if keep == "first" else "DESC"
        nn = " AND ".join(f"{c} IS NOT NULL" for c in cols)
        sql = (f"SELECT source_row FROM (SELECT source_row, ROW_NUMBER() OVER "
               f"(PARTITION BY {', '.join(cols)} ORDER BY source_row {order}) AS rn "
               f"FROM {t} WHERE {nn}) WHERE rn > 1")
        return sql, [], f"duplicate key ({', '.join(cols)}); kept the {keep} occurrence"
    if rule.type == "range":
        conds, params = [], []
        if "min" in p:
            conds.append(f"{col} < ?")
            params.append(_bound(p["min"], as_of))
        if "max" in p:
            conds.append(f"{col} > ?")
            params.append(_bound(p["max"], as_of))
        sql = f"SELECT source_row FROM {t} WHERE {col} IS NOT NULL AND ({' OR '.join(conds)})"
        return sql, params, f"column '{col}' outside [{p.get('min', '-inf')}, {p.get('max', 'inf')}]"
    if rule.type == "accepted_values":
        marks = ", ".join("?" * len(p["values"]))
        return (f"SELECT source_row FROM {t} WHERE {col} IS NOT NULL AND {col} NOT IN ({marks})",
                list(p["values"]), f"column '{col}' not in accepted values")
    if rule.type == "referential":
        ref = cfg.dataset(p["ref_dataset"])
        sql = (f"SELECT source_row FROM {t} WHERE {col} IS NOT NULL AND NOT EXISTS "
               f"(SELECT 1 FROM {ref.target_table} r WHERE r.{p['ref_column']} = {t}.{col})")
        return sql, [], f"'{col}' has no match in {ref.target_table}.{p['ref_column']}"
    if rule.type == "freshness":
        max_age_hours = p["max_age_hours"]
        sql = (f"SELECT source_row FROM {t} WHERE {col} IS NOT NULL AND "
               f"(strftime('%s', ?) - strftime('%s', {col})) / 3600.0 > ?")
        return sql, [as_of, max_age_hours], f"column '{col}' exceeds maximum age threshold of {max_age_hours}h relative to as_of"
    if rule.type == "custom_sql":
        return (f"SELECT source_row FROM {t} WHERE NOT ({p['expression']})", [],
                f"violates: {p['expression']}")
    raise ValueError(rule.type)


def run_row_rule(conn, ds: Dataset, rule: Rule, cfg, run_id: str, as_of: str, now: str) -> int:
    sql, params, reason = fail_query(ds, rule, cfg, as_of)
    t = f"stg_{ds.name}"
    cur = conn.execute(
        "INSERT INTO dq_failed_record(failure_id, run_id, dataset, rule_id, severity, source_file, "
        "source_row, record_key, reason, raw_record, detected_at) "
        f"SELECT sha1_16(?, ?, ?, s.source_file, s.source_row), ?, ?, ?, ?, s.source_file, s.source_row, "
        f"{key_sql(ds)}, ?, {raw_json_sql(ds)}, ? FROM {t} s WHERE s.source_row IN ({sql})",
        [run_id, ds.name, rule.id, run_id, ds.name, rule.id, rule.severity, reason, now, *params])
    return cur.rowcount


def run_dataset_rule(conn, ds: Dataset, rule: Rule, stats: dict, as_of: str):
    """Evaluate on accepted rows (acc_<dataset>) and run counters. -> (checked, failed, detail)."""
    p, acc = rule.params, f"acc_{ds.name}"
    if rule.type == "freshness":
        latest = conn.execute(f"SELECT MAX({p['column']}) FROM {acc}").fetchone()[0]
        if latest is None:
            return 1, 1, "no accepted rows; freshness cannot be established"
        age = (to_datetime(as_of) - to_datetime(latest)).total_seconds() / 3600
        bad = age > p["max_age_hours"]
        return 1, int(bad), (f"latest {p['column']}={latest}, as_of={as_of}, age_hours={age:.2f}, "
                             f"max_age_hours={p['max_age_hours']}")
    if rule.type == "row_count":
        n = conn.execute(f"SELECT COUNT(*) FROM {acc}").fetchone()[0]
        lo, hi = p.get("min_rows"), p.get("max_rows")
        bad = (lo is not None and n < lo) or (hi is not None and n > hi)
        return 1, int(bad), f"accepted_rows={n}, min_rows={lo}, max_rows={hi}"
    if rule.type == "max_reject_rate":
        rate = stats["rows_rejected"] / stats["rows_read"] if stats["rows_read"] else 0.0
        return 1, int(rate > p["max_rate"]), f"reject_rate={rate:.4f}, max_rate={p['max_rate']}"
    raise ValueError(rule.type)
