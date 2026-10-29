"""CLI:  python -m dq_engine {run,report,generate} ..."""
from __future__ import annotations

import argparse
import logging
import sys

from . import generate as gen
from .config import ConfigError, load_config
from .pipeline import run_pipeline
from .report import build_report
from .store import connect

EXIT = {"SUCCEEDED": 0, "FAILED": 1, "PARTIAL": 2}


def main(argv=None) -> int:
    p = argparse.ArgumentParser(prog="dq_engine")
    sub = p.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("run", help="ingest, validate and publish")
    r.add_argument("--config", default="config/pipeline.yaml")
    r.add_argument("--db", default="output/dq.db")
    r.add_argument("--input-dir", default="data/sample")
    r.add_argument("--input", action="append", default=[], metavar="DATASET=PATH", help="override a file")
    r.add_argument("--only", action="append", default=[], help="restrict to a dataset (repeatable)")
    r.add_argument("--run-id")
    r.add_argument("--as-of", help="ISO-8601 reference time for freshness/future checks (default: now)")
    r.add_argument("--force", action="store_true", help="reprocess inputs already published (backfill)")
    r.add_argument("--batch-size", type=int)
    rp = sub.add_parser("report", help="print the Markdown report for a run")
    rp.add_argument("--db", default="output/dq.db")
    rp.add_argument("--run-id", help="default: latest run")
    rp.add_argument("--out")
    g = sub.add_parser("generate", help="write synthetic CSVs + separate expected labels")
    g.add_argument("--out", default="data/sample")
    g.add_argument("--labels-dir", default="data/sample_labels")
    g.add_argument("--customers", type=int, default=50)
    g.add_argument("--orders", type=int, default=200)
    g.add_argument("--seed", type=int, default=42)
    a = p.parse_args(argv)
    logging.basicConfig(level=logging.INFO, stream=sys.stderr,
                        format="%(asctime)sZ %(levelname)s %(message)s", datefmt="%Y-%m-%dT%H:%M:%S")

    if a.cmd == "generate":
        print(gen.generate(a.out, a.customers, a.orders, a.seed, labels_dir=a.labels_dir))
        return 0
    if a.cmd == "report":
        conn = connect(a.db)
        rid = a.run_id or conn.execute("SELECT run_id FROM dq_run ORDER BY started_at DESC, rowid DESC LIMIT 1").fetchone()[0]
        text = build_report(conn, rid)
        if a.out:
            open(a.out, "w", encoding="utf-8").write(text)
        print(text)
        return 0
    try:
        cfg = load_config(a.config)
    except (ConfigError, OSError) as e:
        print(f"config error: {e}", file=sys.stderr)
        return 1
    inputs = dict(i.split("=", 1) for i in a.input)
    s = run_pipeline(cfg, a.db, a.input_dir, run_id=a.run_id, as_of=a.as_of, force=a.force,
                     only=a.only or None, inputs=inputs, batch_size=a.batch_size)
    print(f"run_id={s['run_id']} status={s['status']}")
    return EXIT[s["status"]]


if __name__ == "__main__":
    raise SystemExit(main())
