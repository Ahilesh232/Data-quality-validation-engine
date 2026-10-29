"""Markdown run report built purely from the result tables."""
from __future__ import annotations


def _table(headers, rows):
    out = ["| " + " | ".join(headers) + " |", "|" + "|".join("---" for _ in headers) + "|"]
    out += ["| " + " | ".join("" if v is None else str(v) for v in r) + " |" for r in rows]
    return "\n".join(out)


def build_report(conn, run_id: str, sample_per_rule: int = 3) -> str:
    run = conn.execute("SELECT status, started_at, ended_at, as_of, duration_seconds, failure_reason "
                       "FROM dq_run WHERE run_id=?", (run_id,)).fetchone()
    if not run:
        raise KeyError(f"unknown run_id {run_id}")
    parts = [f"# Data-quality report: `{run_id}`", "",
             f"- Status: **{run[0]}**  \n- Started: {run[1]}  Ended: {run[2]}  Duration: {run[4]}s  \n"
             f"- As-of (freshness / future-date reference): {run[3]}"]
    if run[5]:
        parts.append(f"- Failure reason: {run[5]}")
    parts += ["", "## Datasets", _table(
        ["dataset", "status", "read", "unparseable", "rejected", "warned", "accepted", "inserted",
         "updated", "already_loaded", "late_arriving", "ingest_s", "validate_s", "publish_s"],
        conn.execute("SELECT dataset, status, rows_read, rows_unparseable, rows_rejected, rows_warned, "
                     "rows_accepted, rows_inserted, rows_updated, rows_already_loaded, late_arriving, "
                     "ingest_seconds, validate_seconds, publish_seconds FROM dq_dataset_run "
                     "WHERE run_id=? ORDER BY dataset", (run_id,)).fetchall())]
    reasons = conn.execute("SELECT dataset, failure_reason FROM dq_dataset_run WHERE run_id=? AND "
                           "failure_reason IS NOT NULL", (run_id,)).fetchall()
    if reasons:
        parts += ["", "### Dataset notes"] + [f"- **{d}**: {r}" for d, r in reasons]
    parts += ["", "## Rule results (by dataset, rule, severity)", _table(
        ["dataset", "rule_id", "type", "severity", "checked", "failed", "status", "detail"],
        conn.execute("SELECT dataset, rule_id, rule_type, severity, checked_count, failed_count, status, detail "
                     "FROM dq_rule_result WHERE run_id=? ORDER BY dataset, status DESC, rule_id",
                     (run_id,)).fetchall())]
    parts += ["", "## Reconciliation & Row Accounting"]
    balance_rows = []
    for d, st, r, rej, acc, ins, upd, ldd, late, f_reason in conn.execute(
        "SELECT dataset, status, rows_read, rows_rejected, rows_accepted, rows_inserted, rows_updated, "
        "rows_already_loaded, late_arriving, failure_reason FROM dq_dataset_run WHERE run_id=? ORDER BY dataset", (run_id,)).fetchall():
        unexplained = r - (rej + acc)
        if st == "SKIPPED":
            explanation = "Input already published in target; skipped duplicate load (rerun with --force to reprocess)"
        elif st == "BLOCKED":
            explanation = f"Blocked by dataset quality rule: {f_reason or 'pre-publish check failed'}; nothing published"
        elif unexplained == 0:
            explanation = f"100% accounted for: {acc} accepted into target, {rej} quarantined"
        else:
            explanation = f"{unexplained} records unexplained/unprocessed: {f_reason or 'investigate source file'}"
        balance_rows.append([d, st, r, acc, rej, unexplained, explanation])

    parts += [_table(
        ["dataset", "status", "input_read", "accepted", "rejected", "unprocessed", "explanation"],
        balance_rows
    ), ""]

    parts += ["### Verification Checks", _table(
        ["dataset", "check", "expected", "actual", "passed"],
        [(d, c, e, a, "PASS" if p else "FAIL") for d, c, e, a, p in conn.execute(
            "SELECT dataset, check_name, expected, actual, passed FROM dq_reconciliation "
            "WHERE run_id=? ORDER BY dataset, check_name", (run_id,))])]
    samples = []
    for d, r, sev in conn.execute("SELECT DISTINCT dataset, rule_id, severity FROM dq_failed_record "
                                  "WHERE run_id=? ORDER BY dataset, rule_id", (run_id,)).fetchall():
        samples += conn.execute(
            "SELECT dataset, rule_id, severity, source_file, source_row, record_key, reason FROM dq_failed_record "
            "WHERE run_id=? AND dataset=? AND rule_id=? ORDER BY source_row LIMIT ?",
            (run_id, d, r, sample_per_rule)).fetchall()
    parts += ["", f"## Failed-record samples (first {sample_per_rule} per rule)", _table(
        ["dataset", "rule_id", "severity", "source_file", "source_row", "record_key", "reason"], samples)]
    return "\n".join(parts) + "\n"
