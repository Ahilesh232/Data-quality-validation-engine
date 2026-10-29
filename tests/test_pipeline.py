"""Happy path, empty/malformed input, duplicates, nulls, schema change, validations."""
from tests.conftest import cust, order


def test_happy_path_loads_everything_and_reconciles(env):
    env.files([cust("C1"), cust("C2", email="B@Example.com")], [order("O1", "C1"), order("O2", "C2")])
    s = env.run()
    assert s["status"] == "SUCCEEDED"
    assert env.one("SELECT COUNT(*) FROM dim_customers") == 2
    assert env.one("SELECT COUNT(*) FROM fact_orders_current") == 2
    assert env.one("SELECT email FROM dim_customers WHERE customer_id='C2'") == "b@example.com"   # normalised
    assert env.one("SELECT email_domain FROM dim_customers WHERE customer_id='C2'") == "example.com"  # derived
    assert env.one("SELECT order_date FROM fact_orders_current WHERE order_id='O1'") == "2026-09-29"
    assert env.one("SELECT COUNT(*) FROM dq_failed_record") == 0
    assert all(c["passed"] for c in s["reconciliation"])
    row = env.q("SELECT rows_read, rows_rejected, rows_accepted, rows_inserted FROM dq_dataset_run "
                "WHERE dataset='orders'")[0]
    assert row == (2, 0, 2, 2)


def test_empty_customers_batch_is_a_valid_noop(env):
    env.files([], [])                                  # header only
    s = env.run(only=["customers"])
    assert s["status"] == "SUCCEEDED"
    assert env.one("SELECT rows_read FROM dq_dataset_run WHERE dataset='customers'") == 0
    assert env.one("SELECT COUNT(*) FROM dim_customers") == 0


def test_empty_orders_batch_is_blocked_by_freshness_rule(env):
    env.files([cust("C1")], [])
    s = env.run()
    assert s["datasets"]["orders"]["status"] == "BLOCKED"
    assert s["status"] == "PARTIAL"
    assert "orders_freshness" in s["failure_reason"]
    assert env.one("SELECT COUNT(*) FROM fact_orders_current") == 0


def test_file_with_no_header_fails_dataset_with_reason(env):
    (env.dir / "customers.csv").write_text("")
    env.files(orders=[order()])
    s = env.run(only=["customers"])
    assert s["status"] == "FAILED"
    assert "no header" in s["datasets"]["customers"]["failure_reason"]


def test_malformed_records_are_quarantined_with_raw_text(env):
    env.files([cust("C1"), cust("C2") + ["EXTRA"], cust("C3", credit="abc"), cust("C4", signup="yesterday")],
              [order("O1", "C1")])
    s = env.run()
    assert env.failures("t1", "customers") == {(2, "schema_malformed_row"), (3, "schema_type"), (4, "schema_type")}
    assert env.one("SELECT COUNT(*) FROM dim_customers") == 1
    raw = env.one("SELECT raw_record FROM dq_failed_record WHERE rule_id='schema_malformed_row'")
    assert "EXTRA" in raw
    assert "credit_limit" in env.one("SELECT reason FROM dq_failed_record WHERE source_row=3 AND dataset='customers'")
    assert env.one("SELECT rows_unparseable FROM dq_dataset_run WHERE dataset='customers'") == 3
    assert s["status"] == "SUCCEEDED"


def test_blank_lines_are_ignored_and_not_counted(env):
    (env.dir / "customers.csv").write_text(
        "customer_id,email,full_name,country_code,signup_ts,credit_limit\n\nC1,a@x.com,A,US,2025-01-01T00:00:00Z,1\n\n")
    env.run(only=["customers"])
    assert env.one("SELECT rows_read FROM dq_dataset_run WHERE dataset='customers'") == 1


def test_duplicate_events_and_duplicate_customers(env):
    env.files([cust("C1"), cust("C1", name="Dup"), cust("C2")],
              [order("O1", "C1"), order("O1", "C1"),
               order("O1", "C1", status="shipped", updated="2026-09-30T12:00:00Z")])   # same order, NEW version: legit
    env.run()
    assert env.failures("t1", "customers") == {(2, "customers_unique_id")}
    assert env.one("SELECT full_name FROM dim_customers WHERE customer_id='C1'") == "Ann"     # first kept
    assert env.failures("t1", "orders") == {(2, "orders_unique_event")}
    assert env.one("SELECT COUNT(*) FROM fact_order_events") == 2
    assert env.one("SELECT order_status FROM fact_orders_current WHERE order_id='O1'") == "shipped"


def test_nulls_in_required_fields(env):
    env.files([cust("C1"), cust("C2", email=""), cust("", email="z@x.com")],
              [order("O1", "C1"), order("O2", "C1", total=""), order("O3", cid="")])
    env.run()
    assert env.failures("t1", "customers") == {(2, "customers_email_required"), (3, "customers_customer_id_required")}
    assert env.failures("t1", "orders") == {(2, "orders_total_amount_required"), (3, "orders_customer_id_required")}


def test_invalid_reference_and_cascade_from_rejected_customer(env):
    env.files([cust("C1"), cust("C2", email="")], [order("O1", "C1"), order("O2", "C2"), order("O3", "NOPE")])
    env.run()
    assert env.failures("t1", "orders") == {(2, "orders_customer_exists"), (3, "orders_customer_exists")}
    assert env.q("SELECT order_id FROM fact_orders_current") == [("O1",)]


def test_domain_boundaries_are_accepted_and_just_outside_rejected(env):
    ok = [order("O1", qty="1", price="0.01", total="0.01"),
          order("O2", qty="100", price="10000.00", total="1000000.00"),
          order("O3", placed="2026-10-01T00:00:00Z", updated="2026-10-01T00:00:00Z")]    # exactly as_of
    bad = [order("O4", qty="0", price="1", total="0"), order("O5", qty="101", price="1", total="101"),
           order("O6", price="0.00", total="0"), order("O7", price="10000.01", total="20000.02"),
           order("O8", total="20.02"), order("O9", placed="2026-10-01T00:00:01Z", updated="2026-10-01T00:00:01Z"),
           order("O10", updated="2026-09-29T09:00:00Z")]
    env.files([cust("C1", credit="0"), cust("C2", credit="50000"), cust("C3", credit="50000.01")], ok + bad)
    env.run()
    assert env.failures("t1", "customers") == {(3, "customers_credit_range")}
    rej = {r for r, _ in env.failures("t1", "orders")}
    assert rej == set(range(4, 11))                   # every "bad" row, no "ok" row
    assert env.one("SELECT COUNT(*) FROM fact_orders_current") == 3


def test_total_must_match_quantity_times_price_within_a_cent(env):
    env.files([cust("C1")], [order("O1", qty="3", price="0.10", total="0.30"),
                             order("O2", qty="3", price="9.99", total="29.97"),
                             order("O3", qty="3", price="9.99", total="29.99")])
    env.run()
    assert env.failures("t1", "orders") == {(3, "orders_total_matches")}


def test_warn_severity_loads_row_but_records_failure(env):
    env.files([cust("C1", country="ZZ")], [order()])
    env.run()
    assert env.failures("t1", "customers", "warn") == {(1, "customers_country_known")}
    assert env.one("SELECT COUNT(*) FROM dim_customers") == 1
    assert env.one("SELECT rows_warned FROM dq_dataset_run WHERE dataset='customers'") == 1


def test_unexpected_extra_column_is_a_warning_not_a_failure(env):
    env.files([cust("C1") + ["x"]], [order()], cust_header=["customer_id", "email", "full_name", "country_code",
                                                            "signup_ts", "credit_limit", "surprise"])
    s = env.run()
    assert s["datasets"]["customers"]["status"] == "PUBLISHED"
    assert env.one("SELECT detail FROM dq_rule_result WHERE rule_id='schema_columns' AND dataset='customers'").endswith("['surprise']")


def test_missing_column_fails_only_that_dataset(env):
    env.files([cust("C1")], [order()], cust_header=["customer_id", "email", "full_name", "country_code", "signup_ts"])
    s = env.run()
    assert s["datasets"]["customers"]["status"] == "FAILED"
    assert "credit_limit" in s["datasets"]["customers"]["failure_reason"]
    assert env.one("SELECT COUNT(*) FROM dim_customers") == 0
    assert "orders" in s["datasets"]                  # orders was still attempted (failure is per dataset)
    assert s["status"] in ("PARTIAL", "FAILED")
    assert env.one("SELECT status FROM dq_rule_result WHERE rule_id='schema_columns' AND dataset='customers'") == "FAIL"


def test_column_order_change_is_tolerated(env):
    env.files([["a@x.com", "C1", "A", "US", "2025-01-01T00:00:00Z", "1"]], [order()],
              cust_header=["email", "customer_id", "full_name", "country_code", "signup_ts", "credit_limit"])
    env.run()
    assert env.q("SELECT customer_id, email FROM dim_customers") == [("C1", "a@x.com")]


def test_freshness_blocks_publish_when_stale(env):
    env.files([cust("C1")], [order(placed="2026-09-19T10:00:00Z", updated="2026-09-20T00:00:00Z")])
    s = env.run()
    assert s["datasets"]["orders"]["status"] == "BLOCKED"
    assert env.one("SELECT status FROM dq_rule_result WHERE rule_id='orders_freshness'") == "FAIL"
    assert env.one("SELECT COUNT(*) FROM fact_orders_current") == 0
    s2 = env.run(as_of="2026-09-21T00:00:00Z")      # same file is fresh relative to an earlier as_of
    assert s2["datasets"]["orders"]["status"] == "PUBLISHED"


def test_reject_rate_threshold_blocks_a_mostly_bad_file(env):
    from dq_engine.config import load_config
    from tests.conftest import ROOT
    env.cfg = load_config(ROOT / "config" / "pipeline.yaml")      # strict thresholds (25%)
    env.files([cust("C1"), cust("C2", email=""), cust("C3", email="")], [order()])
    s = env.run()
    assert s["datasets"]["customers"]["status"] == "BLOCKED"
    assert env.one("SELECT COUNT(*) FROM dim_customers") == 0


def test_timestamps_are_normalised_to_utc(env):
    env.files([cust("C1", signup="2025-01-01T05:30:00+05:30")], [order(updated="2026-09-30T12:00:00")])
    env.run()
    assert env.one("SELECT signup_ts FROM dim_customers") == "2025-01-01T00:00:00Z"
    assert env.one("SELECT updated_at FROM fact_orders_current") == "2026-09-30T12:00:00Z"


def test_unique_keep_last_policy_via_custom_config(env):
    from dq_engine.config import parse_config
    import yaml
    raw = yaml.safe_load(open("config/pipeline.yaml"))
    raw["datasets"][0]["rules"][0]["keep"] = "last"
    raw["datasets"][0]["rules"][-1]["max_rate"] = 1.0         # tiny fixture: allow 50% rejects
    env.cfg = parse_config(raw, "variant")
    env.files([cust("C1", name="old"), cust("C1", name="new")], [order()])
    env.run()
    assert env.one("SELECT full_name FROM dim_customers") == "new"
    assert env.failures("t1", "customers") == {(1, "customers_unique_id")}


# ---------------------------------------------------------------------------
# Dedicated tests for the 5 requested improvements
# ---------------------------------------------------------------------------

def test_referential_integrity_nonexistent_foreign_key_and_orphan_prevention(env):
    """1. Referential integrity: Test an order referencing a completely nonexistent customer record."""
    # C1 is valid; O1 references C1. O_BAD references nonexistent customer 'C_NONEXISTENT_999'
    env.files([cust("C1")], [order("O1", "C1"), order("O_BAD", "C_NONEXISTENT_999")])
    s = env.run()
    # O_BAD must fail the referential check
    assert env.failures("t1", "orders") == {(2, "orders_customer_exists")}
    # Quarantined reason must clearly explain the foreign key failure
    reason = env.one("SELECT reason FROM dq_failed_record WHERE record_key LIKE 'O_BAD%'")
    assert "has no match in dim_customers.customer_id" in reason
    # Target table must contain ONLY the valid order O1
    assert env.q("SELECT order_id FROM fact_orders_current") == [("O1",)]
    # Target table must have ZERO orphan orders
    orphans = env.one("SELECT COUNT(*) FROM fact_orders_current o WHERE NOT EXISTS "
                      "(SELECT 1 FROM dim_customers c WHERE c.customer_id = o.customer_id)")
    assert orphans == 0
    # Reconciliation check for orphans must pass
    rec = {r["check"]: r["actual"] for r in s["reconciliation"] if "orphans" in r["check"]}
    assert rec.get("orphans in target (orders_customer_exists)") == 0.0


def test_freshness_age_threshold_vs_future_date_check(env):
    """2. Freshness: Dedicated test proving a future-date check is NOT the same as a freshness check.
    - Future-date check verifies: timestamp <= as_of (no future events).
    - Freshness check verifies: (as_of - max(timestamp)) <= max_age_hours (data is not stale).
    """
    # Case A: A past order (e.g. 10 days old relative to as_of 2026-10-01)
    # This PASSES the future-date check (it is in the past), but FAILS the 48-hour freshness check!
    stale_past_order = order("O_STALE", "C1", placed="2026-09-20T00:00:00Z", updated="2026-09-20T12:00:00Z")
    env.files([cust("C1")], [stale_past_order])
    s_stale = env.run(as_of="2026-10-01T00:00:00Z")

    # Future date check PASSED (order is in the past)
    future_check = env.one("SELECT status FROM dq_rule_result WHERE rule_id='orders_placed_not_future'")
    assert future_check == "PASS"

    # Freshness check FAILED (order is 252 hours old > 48 hours max_age)
    freshness_check = env.one("SELECT status FROM dq_rule_result WHERE rule_id='orders_freshness'")
    assert freshness_check == "FAIL"
    assert "orders_freshness" in s_stale["datasets"]["orders"]["failure_reason"]
    assert s_stale["datasets"]["orders"]["status"] == "BLOCKED"

    # Case B: A future order placed after as_of (e.g. 2026-10-05 relative to as_of 2026-10-01)
    # This FAILS the future-date check!
    future_order = order("O_FUTURE", "C1", placed="2026-10-05T00:00:00Z", updated="2026-10-05T00:00:00Z")
    env.files([cust("C1")], [future_order])
    s_future = env.run(as_of="2026-10-01T00:00:00Z", force=True)
    assert env.failures(s_future["run_id"], "orders") == {(1, "orders_placed_not_future")}


def test_reconciliation_row_accounting_balance_and_unprocessed_explanations(env):
    """3. Reconciliation: Show total input rows, accepted rows, and rejected rows, with explanation for any records not processed."""
    # 2 customers: C1 valid, C2 invalid email (rejected)
    # 2 orders: O1 valid, O2 referencing C2 (rejected due to missing customer)
    env.files([cust("C1"), cust("C2", email="")], [order("O1", "C1"), order("O2", "C2")])
    s = env.run()
    assert s["status"] == "SUCCEEDED"

    # Reconciliation must verify: rows_read = accepted + rejected
    for ds_name in ("customers", "orders"):
        r = env.q(f"SELECT rows_read, rows_accepted, rows_rejected FROM dq_dataset_run WHERE dataset='{ds_name}'")[0]
        rows_read, rows_accepted, rows_rejected = r
        assert rows_read == rows_accepted + rows_rejected
        assert rows_read == 2
        assert rows_accepted == 1
        assert rows_rejected == 1

    # Check the reconciliation record
    rec_balance = [c for c in s["reconciliation"] if c["check"] == "rows_read = accepted + rejected"]
    assert len(rec_balance) == 2
    assert all(c["passed"] == 1 for c in rec_balance)


def test_export_cleanup_removes_unnamed_index_column(env):
    """4. Export cleanup: Remove unnecessary Unnamed: 0 column if it is only an exported DataFrame index."""
    import pandas as pd
    import io

    # Create a DataFrame with an extra Unnamed: 0 index column (common from df.to_csv() without index=False)
    sample_df = pd.DataFrame({
        "Unnamed: 0": [0, 1, 2],
        "customer_id": ["C1", "C2", "C3"],
        "credit_limit": [1000, 2000, 3000]
    })

    # Test the export cleaning utility
    from app import clean_df_for_export
    cleaned = clean_df_for_export(sample_df)
    assert "Unnamed: 0" not in cleaned.columns
    assert list(cleaned.columns) == ["customer_id", "credit_limit"]

    # When exported to CSV, no Unnamed: 0 column exists
    buf = io.StringIO()
    cleaned.to_csv(buf, index=False)
    assert "Unnamed: 0" not in buf.getvalue()


def test_warnings_reported_separately_from_errors_and_policy_enforcement(env):
    """5. Warnings: Confirm warnings are reported separately from errors and follow policy allowing warning rows to publish."""
    # C1: Has invalid email (SEVERITY: ERROR) -> must be REJECTED / quarantined
    # C2: Has unknown country 'ZZ' (SEVERITY: WARN) -> must be REPORTED as warning but PUBLISHED
    # C3: Completely clean row -> PUBLISHED
    env.files([cust("C1", email=""), cust("C2", country="ZZ"), cust("C3")], [order("O1", "C2"), order("O2", "C3")])
    s = env.run()
    assert s["status"] == "SUCCEEDED"

    # In dq_failed_record, warnings and errors are logged with distinct severities
    error_count = env.one("SELECT COUNT(*) FROM dq_failed_record WHERE dataset='customers' AND severity='error'")
    warn_count = env.one("SELECT COUNT(*) FROM dq_failed_record WHERE dataset='customers' AND severity='warn'")
    assert error_count == 1   # C1
    assert warn_count == 1    # C2

    # In dq_dataset_run, counts are reported separately
    stats = env.q("SELECT rows_read, rows_rejected, rows_warned, rows_accepted FROM dq_dataset_run WHERE dataset='customers'")[0]
    assert stats == (3, 1, 1, 2)  # read=3, rejected=1, warned=1, accepted=2

    # Verify warning row C2 WAS published to dim_customers, while error row C1 was NOT
    published_keys = set(r[0] for r in env.q("SELECT customer_id FROM dim_customers"))
    assert "C1" not in published_keys   # Error was quarantined
    assert "C2" in published_keys       # Warning was allowed to publish
    assert "C3" in published_keys
