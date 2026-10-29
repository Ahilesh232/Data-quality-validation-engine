import csv
import sqlite3
import sys
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from dq_engine.config import load_config, parse_config          # noqa: E402
from dq_engine.generate import CUST_HEADER, ORD_HEADER   # noqa: E402
from dq_engine.pipeline import run_pipeline        # noqa: E402

AS_OF = "2026-10-01T00:00:00Z"


def cust(cid="C1", email="a@example.com", name="Ann", country="US", signup="2025-01-01T00:00:00Z", credit="100.00"):
    return [cid, email, name, country, signup, credit]


def order(oid="O1", cid="C1", status="paid", qty="2", price="10.00", total="20.00",
          placed="2026-09-29T10:00:00Z", updated="2026-09-30T10:00:00Z"):
    return [oid, cid, status, qty, price, total, placed, updated]


def write_csv(path, header, rows, raw_lines=None):
    with open(path, "w", newline="", encoding="utf-8") as f:
        if header is not None:
            csv.writer(f, lineterminator="\n").writerow(header)
        csv.writer(f, lineterminator="\n").writerows(rows)
    return path


def lenient_config():
    """Shipped config with the reject-rate guard disabled (tiny fixtures reject >25% by design)."""
    raw = yaml.safe_load(open(ROOT / "config" / "pipeline.yaml"))
    for d in raw["datasets"]:
        for r in d["rules"]:
            if r["type"] == "max_reject_rate":
                r["max_rate"] = 1.0
    return parse_config(raw, "lenient")


class Env:
    """A tmp dir + db + helpers to run the pipeline repeatedly."""

    def __init__(self, tmp_path):
        self.dir = tmp_path
        self.db = str(tmp_path / "dq.db")
        self.cfg = lenient_config()
        self.n = 0

    def files(self, customers=None, orders=None, cust_header=CUST_HEADER, ord_header=ORD_HEADER):
        if customers is not None:
            write_csv(self.dir / "customers.csv", cust_header, customers)
        if orders is not None:
            write_csv(self.dir / "orders.csv", ord_header, orders)

    def run(self, **kw):
        self.n += 1
        kw.setdefault("as_of", AS_OF)
        kw.setdefault("run_id", f"t{self.n}")
        return run_pipeline(self.cfg, self.db, str(self.dir), **kw)

    def q(self, sql, *params):
        c = sqlite3.connect(self.db)
        try:
            return c.execute(sql, params).fetchall()
        finally:
            c.close()

    def one(self, sql, *params):
        return self.q(sql, *params)[0][0]

    def failures(self, run_id, dataset, severity="error"):
        return {(r, rid) for r, rid in self.q(
            "SELECT source_row, rule_id FROM dq_failed_record WHERE run_id=? AND dataset=? AND severity=?",
            run_id, dataset, severity)}


@pytest.fixture
def env(tmp_path):
    return Env(tmp_path)
