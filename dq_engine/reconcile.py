"""Post-run reconciliation checks, stored in dq_reconciliation."""
from __future__ import annotations

from .config import PipelineConfig
from .store import insert_row


def add_check(conn, run_id, dataset, name, expected, actual, detail=""):
    ok = abs(expected - actual) <= 1e-6 * max(1.0, abs(expected))
    insert_row(conn, "dq_reconciliation", dict(run_id=run_id, dataset=dataset, check_name=name,
                                               expected=expected, actual=actual, passed=int(ok), detail=detail))


def reconcile(conn, cfg: PipelineConfig, run_id: str) -> list:
    rows = conn.execute(
        "SELECT dataset, status, rows_read, rows_rejected, rows_accepted, rows_inserted, rows_updated, "
        "rows_already_loaded FROM dq_dataset_run WHERE run_id=? AND status IN ('PUBLISHED','BLOCKED')",
        (run_id,)).fetchall()
    for name, status, read, rejected, accepted, ins, upd, loaded in rows:
        ds = cfg.dataset(name)
        add_check(conn, run_id, name, "rows_read = accepted + rejected", read, accepted + rejected,
                  "every input record is either accepted or rejected (quarantined)")
        untraceable = conn.execute(
            "SELECT COUNT(*) FROM dq_failed_record WHERE run_id=? AND dataset=? AND "
            "(source_file IS NULL OR source_row IS NULL)", (run_id, name)).fetchone()[0]
        add_check(conn, run_id, name, "failed records traceable to source file+row", 0, untraceable)
        if status != "PUBLISHED":
            continue
        add_check(conn, run_id, name, "accepted = inserted + updated + already_loaded",
                  accepted, ins + upd + loaded,
                  "a difference means duplicate keys inside the accepted set")
        keys = ", ".join(ds.key)
        if ds.mode == "event_history":
            keys_h = keys
            distinct = conn.execute(f"SELECT COUNT(*) FROM (SELECT DISTINCT {keys_h} FROM {ds.history_table})").fetchone()[0]
            current = conn.execute(f"SELECT COUNT(*) FROM {ds.target_table}").fetchone()[0]
            add_check(conn, run_id, name, "one current row per distinct key in history", distinct, current)
        else:
            total = conn.execute(f"SELECT COUNT(*) FROM {ds.target_table}").fetchone()[0]
            distinct = conn.execute(f"SELECT COUNT(*) FROM (SELECT DISTINCT {keys} FROM {ds.target_table})").fetchone()[0]
            add_check(conn, run_id, name, "target primary key unique", total, distinct)
        for rule in ds.rules:
            if rule.type == "referential":
                ref = cfg.dataset(rule.params["ref_dataset"])
                col = rule.params["column"]
                orphans = conn.execute(
                    f"SELECT COUNT(*) FROM {ds.target_table} t WHERE t.{col} IS NOT NULL AND NOT EXISTS "
                    f"(SELECT 1 FROM {ref.target_table} r WHERE r.{rule.params['ref_column']} = t.{col})"
                ).fetchone()[0]
                add_check(conn, run_id, name, f"orphans in target ({rule.id})", 0, orphans)
    return [dict(zip(("dataset", "check", "expected", "actual", "passed", "detail"), r)) for r in conn.execute(
        "SELECT dataset, check_name, expected, actual, passed, detail FROM dq_reconciliation "
        "WHERE run_id=? ORDER BY dataset, check_name", (run_id,))]
