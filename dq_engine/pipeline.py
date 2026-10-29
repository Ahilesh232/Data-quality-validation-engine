"""Orchestration: stage -> validate -> (block | publish) -> record, per dataset.

Each dataset is processed in ONE transaction (staging, rule results, publish,
run metadata), so a crash never leaves a half-published dataset.
"""
from __future__ import annotations

import logging
import os
import time
import uuid
from datetime import datetime, timezone

from . import ingest, rules, store
from .config import PipelineConfig
from .publish import publish, source_target_sums
from .reconcile import add_check, reconcile

log = logging.getLogger("dq_engine")
INDEX_ACC = True      # index the accepted-rows table on the business key (publish probes it per row); benchmark toggles it


def new_run_id() -> str:
    return datetime.now(timezone.utc).strftime("run_%Y%m%dT%H%M%SZ_") + uuid.uuid4().hex[:6]


def _count(conn, sql, params=()):
    return conn.execute(sql, params).fetchone()[0]


def _process_dataset(conn, cfg, ds, path, run_id, as_of, force, batch_size) -> dict:
    started = ingest.utc_now()
    sha = ingest.file_sha256(path)                    # FileNotFoundError -> FAILED
    seen = conn.execute("SELECT run_id, config_sha256 FROM dq_ingest_manifest WHERE dataset=? AND file_sha256=?",
                        (ds.name, sha)).fetchone()
    base = dict(run_id=run_id, dataset=ds.name, source_file=os.path.basename(path), file_sha256=sha,
                started_at=started)
    if seen and seen[1] == cfg.sha256 and not force:
        row = dict(base, status="SKIPPED", ended_at=ingest.utc_now(),
                   failure_reason=f"identical input already published in {seen[0]} (use --force to reprocess)")
        store.insert_row(conn, "dq_dataset_run", row)
        return row

    conn.execute("BEGIN")
    try:
        now = ingest.utc_now()
        t0 = time.perf_counter()
        st = ingest.stage_file(conn, ds, path, run_id, batch_size, now)
        for rid, rtype, sev, checked, failed, detail in st["schema_results"]:
            store.add_rule_result(conn, run_id, ds.name, rid, rtype, sev, checked, failed, detail)
        t1 = time.perf_counter()

        for rule in (r for r in ds.rules if not r.is_dataset_level):
            failed = rules.run_row_rule(conn, ds, rule, cfg, run_id, as_of, now)
            store.add_rule_result(conn, run_id, ds.name, rule.id, rule.type, rule.severity,
                                  st["rows_staged"], failed, rule.params.get("expression") or f"{rule.type} check")
        err = ("FROM dq_failed_record WHERE run_id=? AND dataset=? AND severity='error'", (run_id, ds.name))
        conn.execute(f"DROP TABLE IF EXISTS acc_{ds.name}")
        conn.execute(f"CREATE TEMP TABLE acc_{ds.name} AS SELECT * FROM stg_{ds.name} WHERE source_row NOT IN "
                     f"(SELECT source_row {err[0]} AND source_row IS NOT NULL)", err[1])
        if INDEX_ACC:
            conn.execute(f"CREATE INDEX acc_{ds.name}_key ON acc_{ds.name} ({', '.join(ds.key)})")
        stats = dict(st)
        stats["rows_rejected"] = _count(conn, f"SELECT COUNT(DISTINCT source_row) {err[0]}", err[1])
        stats["rows_accepted"] = _count(conn, f"SELECT COUNT(*) FROM acc_{ds.name}")
        stats["rows_warned"] = _count(
            conn, "SELECT COUNT(DISTINCT source_row) FROM dq_failed_record WHERE run_id=? AND dataset=? "
                  f"AND severity='warn' AND source_row NOT IN (SELECT source_row {err[0]})", (run_id, ds.name, *err[1]))

        blockers = []
        for rule in (r for r in ds.rules if r.is_dataset_level):
            checked, failed, detail = rules.run_dataset_rule(conn, ds, rule, stats, as_of)
            store.add_rule_result(conn, run_id, ds.name, rule.id, rule.type, rule.severity, checked, failed, detail)
            if failed and rule.severity == "error":
                blockers.append(f"{rule.id}: {detail}")
        t2 = time.perf_counter()

        counters = {"rows_inserted": 0, "rows_updated": 0, "rows_already_loaded": 0, "late_arriving": 0}
        if blockers:
            status, reason = "BLOCKED", "; ".join(blockers)
        else:
            counters = publish(conn, ds, run_id, now)
            for col, exp, act in source_target_sums(conn, ds):
                add_check(conn, run_id, ds.name, f"sum({col}) accepted vs target", exp, act)
            status, reason = "PUBLISHED", None
            store.insert_row(conn, "dq_ingest_manifest", dict(
                dataset=ds.name, file_sha256=sha, config_sha256=cfg.sha256, run_id=run_id,
                source_file=base["source_file"], loaded_at=now))
        t3 = time.perf_counter()
        row = dict(base, status=status, failure_reason=reason, ended_at=ingest.utc_now(),
                   rows_read=st["rows_read"], rows_unparseable=st["rows_unparseable"],
                   rows_staged=st["rows_staged"], rows_rejected=stats["rows_rejected"],
                   rows_warned=stats["rows_warned"], rows_accepted=stats["rows_accepted"],
                   ingest_seconds=round(t1 - t0, 4), validate_seconds=round(t2 - t1, 4),
                   publish_seconds=round(t3 - t2, 4), **counters)
        store.insert_row(conn, "dq_dataset_run", row)
        conn.execute("COMMIT")
        return row
    except BaseException:
        conn.execute("ROLLBACK")
        raise


def run_pipeline(cfg: PipelineConfig, db_path: str, input_dir: str = ".", *, run_id: str | None = None,
                 as_of: str | None = None, force: bool = False, only: list | None = None,
                 inputs: dict | None = None, batch_size: int | None = None) -> dict:
    """Run every (or the selected) dataset. Returns a summary dict (also stored in dq_run)."""
    run_id = run_id or new_run_id()
    as_of = ingest.parse_timestamp(as_of) if as_of else ingest.utc_now()
    batch_size = batch_size or cfg.batch_size
    inputs = inputs or {}
    conn = store.connect(db_path)
    store.init_db(conn, cfg)
    started, t_start = ingest.utc_now(), time.perf_counter()

    conn.execute("BEGIN")
    conn.execute("UPDATE dq_run SET status='ABANDONED' WHERE status='RUNNING'")   # crashed earlier runs
    for t in ("dq_reconciliation", "dq_failed_record", "dq_rule_result", "dq_dataset_run", "dq_run"):
        conn.execute(f"DELETE FROM {t} WHERE run_id=?", (run_id,))               # same run_id = retry
    store.insert_row(conn, "dq_run", dict(run_id=run_id, status="RUNNING", started_at=started, as_of=as_of,
                                          config_sha256=cfg.sha256, forced=int(force)))
    conn.execute("COMMIT")
    log.info("run %s started (as_of=%s force=%s)", run_id, as_of, force)

    outcomes = {}
    for ds in cfg.datasets:
        if only and ds.name not in only:
            continue
        path = inputs.get(ds.name) or os.path.join(input_dir, ds.source_file)
        try:
            outcomes[ds.name] = _process_dataset(conn, cfg, ds, path, run_id, as_of, force, batch_size)
        except Exception as exc:                                                    # noqa: BLE001
            reason = f"{type(exc).__name__}: {exc}"
            log.error("dataset %s FAILED: %s", ds.name, reason)
            conn.execute("BEGIN")
            row = dict(run_id=run_id, dataset=ds.name, source_file=os.path.basename(path),
                       status="FAILED", failure_reason=reason, started_at=ingest.utc_now(),
                       ended_at=ingest.utc_now())
            store.insert_row(conn, "dq_dataset_run", row)
            if isinstance(exc, ingest.SchemaError):
                store.add_rule_result(conn, run_id, ds.name, exc.rule_id, "schema", "error", 1, 1, str(exc))
            conn.execute("COMMIT")
            outcomes[ds.name] = row
        o = outcomes[ds.name]
        log.info("dataset %s %s read=%s rejected=%s accepted=%s inserted=%s updated=%s already_loaded=%s late=%s %s",
                 ds.name, o["status"], *(o.get(k) or 0 for k in (
                     "rows_read", "rows_rejected", "rows_accepted", "rows_inserted", "rows_updated",
                     "rows_already_loaded", "late_arriving")), o.get("failure_reason") or "")

    statuses = [o["status"] for o in outcomes.values()]
    bad = [s for s in statuses if s in ("FAILED", "BLOCKED")]
    status = "SUCCEEDED" if not bad else ("FAILED" if len(bad) == len(statuses) else "PARTIAL")
    reason = "; ".join(f"{n}: {o['failure_reason']}" for n, o in outcomes.items()
                       if o["status"] in ("FAILED", "BLOCKED")) or None
    conn.execute("BEGIN")
    checks = reconcile(conn, cfg, run_id)
    conn.execute("UPDATE dq_run SET status=?, ended_at=?, duration_seconds=?, failure_reason=? WHERE run_id=?",
                 (status, ingest.utc_now(), round(time.perf_counter() - t_start, 4), reason, run_id))
    conn.execute("COMMIT")
    conn.close()
    log.info("run %s %s", run_id, status)
    return {"run_id": run_id, "status": status, "datasets": outcomes, "reconciliation": checks,
            "failure_reason": reason}
