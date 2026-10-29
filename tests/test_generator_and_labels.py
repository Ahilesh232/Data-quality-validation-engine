"""Generator determinism + answer-key comparison (labels live in a separate file)."""
import csv
import filecmp

from dq_engine.generate import generate
from tests.conftest import Env


def _expected(labels_dir):
    rows = list(csv.DictReader(open(labels_dir / "expected_labels.csv")))
    err = {(r["dataset"], int(r["source_row"]), r["expected_rule_id"]) for r in rows if r["expected_outcome"] == "rejected"}
    warn = {(r["dataset"], int(r["source_row"]), r["expected_rule_id"]) for r in rows if r["expected_outcome"] != "rejected"}
    return err, warn


def test_generator_is_deterministic_and_seed_sensitive(tmp_path):
    a, b, c = tmp_path / "a", tmp_path / "b", tmp_path / "c"
    generate(a, 40, 150, seed=7); generate(b, 40, 150, seed=7); generate(c, 40, 150, seed=8)
    assert filecmp.cmp(a / "orders.csv", b / "orders.csv", shallow=False)
    assert filecmp.cmp(a / "labels" / "expected_labels.csv", b / "labels" / "expected_labels.csv", shallow=False)
    assert not filecmp.cmp(a / "orders.csv", c / "orders.csv", shallow=False)


def test_labels_are_not_embedded_in_input_files(tmp_path):
    generate(tmp_path, 20, 60)
    for name in ("customers.csv", "orders.csv"):
        text = (tmp_path / name).read_text()
        assert "fault" not in text.splitlines()[0] and "expected" not in text.splitlines()[0]


def test_engine_finds_exactly_the_labelled_faults(tmp_path):
    generate(tmp_path / "in", 300, 1500, seed=123, labels_dir=tmp_path / "labels")
    (tmp_path / "w").mkdir()
    env = Env(tmp_path / "w")
    env.dir = tmp_path / "in"
    from dq_engine.config import load_config
    from tests.conftest import ROOT
    env.cfg = load_config(ROOT / "config" / "pipeline.yaml")       # strict shipped config
    s = env.run()
    assert s["status"] == "SUCCEEDED"
    exp_err, exp_warn = _expected(tmp_path / "labels")
    got_err = {(d, r, rid) for d in ("customers", "orders") for (r, rid) in env.failures("t1", d)}
    got_warn = {(d, r, rid) for d in ("customers", "orders") for (r, rid) in env.failures("t1", d, "warn")}
    assert got_err == exp_err                       # no misses, no false positives
    assert got_warn == exp_warn
    assert all(c["passed"] for c in s["reconciliation"])
    # key aggregate: published event sum equals sum of the accepted rows (checked in-pipeline too)
    assert env.one("SELECT COUNT(*) FROM fact_orders_current c WHERE NOT EXISTS "
                   "(SELECT 1 FROM dim_customers d WHERE d.customer_id=c.customer_id)") == 0
    assert env.one("SELECT COUNT(*) FROM fact_orders_current") == env.one(
        "SELECT COUNT(DISTINCT order_id) FROM fact_order_events")


def test_generated_data_includes_out_of_order_versions(tmp_path):
    generate(tmp_path, 100, 2000, seed=5)
    seen, out_of_order = {}, 0
    for r in csv.DictReader(open(tmp_path / "orders.csv")):
        if len(r) != 8:
            continue
        if r["order_id"] in seen and r["updated_at"] < seen[r["order_id"]]:
            out_of_order += 1
        seen[r["order_id"]] = max(seen.get(r["order_id"], ""), r["updated_at"])
    assert out_of_order > 0
