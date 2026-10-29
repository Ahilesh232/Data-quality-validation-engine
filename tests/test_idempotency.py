"""Reruns, retries, restarts, late-arriving data, backfill."""
from tests.conftest import cust, order


def snapshot(env):
    return (env.q("SELECT * FROM dim_customers ORDER BY customer_id"),
            env.q("SELECT * FROM fact_order_events ORDER BY order_id, updated_at"),
            env.q("SELECT * FROM fact_orders_current ORDER BY order_id"))


def test_rerun_of_same_input_is_skipped_and_changes_nothing(env):
    env.files([cust("C1")], [order("O1"), order("O2")])
    env.run()
    before = snapshot(env)
    s = env.run()
    assert {o["status"] for o in s["datasets"].values()} == {"SKIPPED"}
    assert snapshot(env) == before
    assert s["status"] == "SUCCEEDED"


def test_forced_reprocess_creates_no_duplicates_and_no_change(env):
    env.files([cust("C1")], [order("O1"), order("O2")])
    env.run()
    before = [r[:-3] for r in env.q("SELECT * FROM fact_order_events")]
    s = env.run(force=True)
    assert s["datasets"]["orders"]["status"] == "PUBLISHED"
    assert s["datasets"]["orders"]["rows_inserted"] == 0
    assert s["datasets"]["orders"]["rows_already_loaded"] == 2
    assert s["datasets"]["customers"]["rows_updated"] == 1
    assert env.one("SELECT COUNT(*) FROM fact_order_events") == 2
    assert [r[:-3] for r in env.q("SELECT * FROM fact_order_events")] == before
    assert all(c["passed"] for c in s["reconciliation"])


def test_same_run_id_retry_replaces_results_instead_of_duplicating(env):
    env.files([cust("C1", email="")], [order()])
    env.run(run_id="r1")
    n = env.one("SELECT COUNT(*) FROM dq_failed_record WHERE run_id='r1'")
    env.run(run_id="r1", force=True)
    assert env.one("SELECT COUNT(*) FROM dq_failed_record WHERE run_id='r1'") == n
    assert env.one("SELECT COUNT(*) FROM dq_run WHERE run_id='r1'") == 1


def test_partial_failure_then_retry_only_does_missing_work(env):
    env.files(customers=[cust("C1")])                   # orders.csv missing -> that dataset fails
    s1 = env.run()
    assert s1["status"] == "PARTIAL"
    assert s1["datasets"]["orders"]["status"] == "FAILED"
    assert "FileNotFoundError" in s1["datasets"]["orders"]["failure_reason"]
    assert env.one("SELECT COUNT(*) FROM dim_customers") == 1
    env.files(orders=[order("O1")])                     # the late file arrives; retry
    s2 = env.run()
    assert s2["datasets"]["customers"]["status"] == "SKIPPED"
    assert s2["datasets"]["orders"]["status"] == "PUBLISHED"
    assert s2["status"] == "SUCCEEDED"
    assert env.one("SELECT COUNT(*) FROM fact_orders_current") == 1


def test_failed_dataset_publishes_nothing_atomically(env, monkeypatch):
    import dq_engine.pipeline as pl
    env.files([cust("C1")], [order("O1")])
    real = pl.publish

    def flaky(conn, ds, *a, **k):
        if ds.name == "orders":
            raise RuntimeError("disk full")
        return real(conn, ds, *a, **k)

    monkeypatch.setattr(pl, "publish", flaky)
    s = env.run()
    assert s["datasets"]["orders"]["status"] == "FAILED" and "disk full" in s["datasets"]["orders"]["failure_reason"]
    assert env.one("SELECT COUNT(*) FROM fact_orders_current") == 0
    assert env.one("SELECT COUNT(*) FROM dq_ingest_manifest WHERE dataset='orders'") == 0   # so retry reprocesses
    monkeypatch.undo()
    assert env.run()["datasets"]["orders"]["status"] == "PUBLISHED"


def test_late_arriving_older_version_goes_to_history_but_not_current(env):
    env.files([cust("C1")], [order("O1", status="shipped", updated="2026-09-30T12:00:00Z")])
    env.run()
    env.files(orders=[order("O1", status="pending", updated="2026-09-30T08:00:00Z"),
                      order("O2", updated="2026-09-30T13:00:00Z")])       # late v1 for O1 + a normal new order
    s = env.run()
    assert s["datasets"]["orders"]["late_arriving"] == 1
    assert env.one("SELECT order_status FROM fact_orders_current WHERE order_id='O1'") == "shipped"
    assert env.one("SELECT COUNT(*) FROM fact_order_events WHERE order_id='O1'") == 2   # history preserved
    assert env.one("SELECT COUNT(*) FROM fact_orders_current") == 2


def test_out_of_order_versions_within_one_file_resolve_to_latest(env):
    env.files([cust("C1")], [order("O1", status="shipped", updated="2026-09-30T12:00:00Z"),
                             order("O1", status="pending", updated="2026-09-30T08:00:00Z")])
    s = env.run()
    assert s["datasets"]["orders"]["late_arriving"] == 0          # not late relative to the target
    assert env.one("SELECT order_status FROM fact_orders_current") == "shipped"
    assert env.one("SELECT COUNT(*) FROM fact_order_events") == 2


def test_newer_version_in_later_batch_updates_current_and_keeps_history(env):
    env.files([cust("C1")], [order("O1", status="pending", updated="2026-09-30T08:00:00Z")])
    env.run()
    env.files(orders=[order("O1", status="paid", updated="2026-09-30T09:00:00Z")])
    env.run()
    assert env.one("SELECT order_status FROM fact_orders_current") == "paid"
    assert [r[0] for r in env.q("SELECT order_status FROM fact_order_events ORDER BY updated_at")] == ["pending", "paid"]


def test_backfill_with_force_picks_up_orders_rejected_for_missing_customer(env):
    env.files([cust("C1")], [order("O1", "C1"), order("O2", "C2")])
    env.run()
    assert env.one("SELECT COUNT(*) FROM fact_orders_current") == 1
    env.files(customers=[cust("C1"), cust("C2")])                 # reference data arrives late
    env.run()                                                     # customers changed -> processed
    env.run(force=True, only=["orders"])                          # backfill orders
    assert env.one("SELECT COUNT(*) FROM fact_orders_current") == 2


def test_customer_update_overwrites_snapshot_row(env):
    env.files([cust("C1", name="Old")], [order()])
    env.run()
    env.files(customers=[cust("C1", name="New")])
    s = env.run()
    assert s["datasets"]["customers"]["rows_updated"] == 1
    assert env.one("SELECT full_name FROM dim_customers") == "New"
    assert env.one("SELECT COUNT(*) FROM dim_customers") == 1


def test_stale_running_run_is_marked_abandoned(env):
    env.files([cust("C1")], [order()])
    env.run()
    import sqlite3
    c = sqlite3.connect(env.db)
    c.execute("INSERT INTO dq_run(run_id,status,started_at,as_of,config_sha256) VALUES('ghost','RUNNING','x','x','x')")
    c.commit(); c.close()
    env.run()
    assert env.one("SELECT status FROM dq_run WHERE run_id='ghost'") == "ABANDONED"
