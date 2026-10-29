"""Deterministic synthetic data generator (fixed seed) with SEPARATE answer labels.

Writes <out>/customers.csv, <out>/orders.csv and <labels_dir>/expected_labels.csv.
`source_row` in the labels is the 1-based record number (header excluded), the
same numbering the pipeline stores in dq_failed_record.source_row.
"""
from __future__ import annotations

import csv
import os
import random
from datetime import datetime, timedelta, timezone

AS_OF = "2026-10-01T00:00:00Z"
FMT = "%Y-%m-%dT%H:%M:%SZ"
COUNTRIES = ["US", "GB", "DE", "FR", "IN", "BR", "JP", "AU", "CA", "ZA"]
NAMES = ["Ada Lovelace", "Alan Turing", "Grace Hopper", "Linus Pike", "Mae Jemison", "Ravi Rao",
         "Chen Wei", "Ines Duarte", "Sam Okoye", "Yuki Tanaka"]
CUST_HEADER = ["customer_id", "email", "full_name", "country_code", "signup_ts", "credit_limit"]
ORD_HEADER = ["order_id", "customer_id", "order_status", "quantity", "unit_price", "total_amount",
              "order_ts", "updated_at"]
LABEL_HEADER = ["dataset", "source_row", "record_key", "fault_type", "expected_outcome", "expected_rule_id"]


def _ts(dt):
    return dt.strftime(FMT)


def _assemble(rng, base, dups):
    """base: list of [fields, label]; dups: list of (orig_idx, element). Dups land after their original."""
    keyed = [(float(i), e) for i, e in enumerate(base)]
    for orig, el in dups:
        keyed.append((rng.randint(orig + 1, len(base)) - 0.5 + rng.random() * 0.01, el))
    keyed.sort(key=lambda x: x[0])
    return [e for _, e in keyed]


def generate(out_dir, n_customers=50, n_orders=200, seed=42, as_of=AS_OF, labels_dir=None):
    rng = random.Random(seed)
    t_asof = datetime.strptime(as_of, FMT).replace(tzinfo=timezone.utc)
    labels_dir = labels_dir or os.path.join(out_dir, "labels")
    os.makedirs(out_dir, exist_ok=True)
    os.makedirs(labels_dir, exist_ok=True)

    # ---------------- customers ----------------
    rate = lambda n, r: max(1, int(n * r))        # noqa: E731
    fault_types = ["null_email", "bad_email", "future_signup", "negative_credit", "unknown_country",
                   "malformed_row", "bad_type"]
    k = rate(n_customers, 0.02)
    chosen = rng.sample(range(n_customers), min(n_customers, k * len(fault_types)))
    fault_at = {idx: fault_types[i % len(fault_types)] for i, idx in enumerate(chosen)}
    cust, clean_ids = [], []
    for i in range(n_customers):
        cid = f"C{i + 1:07d}"
        credit = 0.0 if i % 25 == 0 else 50000.0 if i % 25 == 1 else round(rng.uniform(0, 50000), 2)  # boundaries
        row = [cid, f"user{i + 1}@example.com", rng.choice(NAMES), rng.choice(COUNTRIES),
               _ts(t_asof - timedelta(days=rng.randint(30, 1000), seconds=rng.randint(0, 86399))), f"{credit:.2f}"]
        label = None
        ft = fault_at.get(i)
        if ft == "null_email":
            row[1] = ""; label = ("rejected", "customers_email_required")
        elif ft == "bad_email":
            row[1] = "not-an-email"; label = ("rejected", "customers_email_format")
        elif ft == "future_signup":
            row[4] = _ts(t_asof + timedelta(days=3)); label = ("rejected", "customers_signup_not_future")
        elif ft == "negative_credit":
            row[5] = "-1.00"; label = ("rejected", "customers_credit_range")
        elif ft == "unknown_country":
            row[3] = "ZZ"; label = ("loaded_with_warning", "customers_country_known")
        elif ft == "malformed_row":
            row = row + ["EXTRA"]; label = ("rejected", "schema_malformed_row")
        elif ft == "bad_type":
            row[5] = "abc"; label = ("rejected", "schema_type")
        if label is None:
            clean_ids.append(cid)
        cust.append([row, (ft, *label, cid) if label else None])
    rng.shuffle(cust)
    dup_candidates = [i for i, e in enumerate(cust) if e[1] is None]
    dups = []
    for orig in rng.sample(dup_candidates, min(len(dup_candidates), k)):
        r = list(cust[orig][0])
        dups.append((orig, [r, ("duplicate_key", "rejected", "customers_unique_id", r[0])]))
    cust_final = _assemble(rng, cust, dups)

    # ---------------- orders (event feed) ----------------
    o_faults = ["null_customer", "invalid_customer", "zero_quantity", "bad_status", "total_mismatch",
                "future_order", "bad_type", "null_total", "malformed_row"]
    k = rate(n_orders, 0.015)
    chosen = rng.sample(range(n_orders), min(n_orders, k * len(o_faults)))
    fault_at = {idx: o_faults[i % len(o_faults)] for i, idx in enumerate(chosen)}
    orders = []
    for i in range(n_orders):
        oid = f"O{i + 1:08d}"
        qty = 1 if i % 20 == 0 else 100 if i % 20 == 1 else rng.randint(1, 100)                # boundaries
        price = 0.01 if i % 20 == 2 else 10000.0 if i % 20 == 3 else round(rng.uniform(0.01, 10000), 2)
        total = round(qty * price, 2)
        placed = t_asof - timedelta(hours=rng.randint(60, 45 * 24), minutes=rng.randint(0, 59))
        upd = placed + timedelta(minutes=rng.randint(0, 600))
        if i == n_orders - 1:                                  # guarantees a fresh event for the freshness rule
            placed, upd = t_asof - timedelta(hours=30), t_asof - timedelta(hours=5)
        cust_id = rng.choice(clean_ids)
        row = [oid, cust_id, "pending", str(qty), f"{price:.2f}", f"{total:.2f}", _ts(placed), _ts(upd)]
        label, ft = None, fault_at.get(i)
        if ft == "null_customer":
            row[1] = ""; label = ("rejected", "orders_customer_id_required")
        elif ft == "invalid_customer":
            row[1] = "C9999999"; label = ("rejected", "orders_customer_exists")
        elif ft == "zero_quantity":
            row[3] = "0"; row[5] = "0.00"; label = ("rejected", "orders_quantity_range")
        elif ft == "bad_status":
            row[2] = "teleported"; label = ("rejected", "orders_status_valid")
        elif ft == "total_mismatch":
            row[5] = f"{total + 5:.2f}"; label = ("rejected", "orders_total_matches")
        elif ft == "future_order":
            fut = _ts(t_asof + timedelta(days=2)); row[6] = fut; row[7] = fut
            label = ("rejected", "orders_placed_not_future")
        elif ft == "bad_type":
            row[3] = "abc"; label = ("rejected", "schema_type")
        elif ft == "null_total":
            row[5] = ""; label = ("rejected", "orders_total_amount_required")
        elif ft == "malformed_row":
            row = row + ["EXTRA"]; label = ("rejected", "schema_malformed_row")
        orders.append([row, (ft, *label, f"{oid}|{row[7]}") if label else None])
        if label is None and i != n_orders - 1 and rng.random() < 0.10:     # a later version of the order
            v2 = list(row)
            v2[2] = "shipped"
            v2[7] = _ts(min(upd + timedelta(hours=rng.randint(1, 72)), t_asof - timedelta(hours=1)))
            if v2[7] > row[7]:
                orders.append([v2, None])
    rng.shuffle(orders)                                        # out-of-order versions are legitimate input
    cand = [i for i, e in enumerate(orders) if e[1] is None]
    dups = []
    for orig in rng.sample(cand, min(len(cand), k)):
        r = list(orders[orig][0])
        dups.append((orig, [r, ("duplicate_event", "rejected", "orders_unique_event", f"{r[0]}|{r[7]}")]))
    ord_final = _assemble(rng, orders, dups)

    labels = []
    for name, header, final, path in (("customers", CUST_HEADER, cust_final, "customers.csv"),
                                      ("orders", ORD_HEADER, ord_final, "orders.csv")):
        with open(os.path.join(out_dir, path), "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f, lineterminator="\n")
            w.writerow(header)
            for n, (fields, lab) in enumerate(final, start=1):
                w.writerow(fields)
                if lab:
                    ft, outcome, rule, key = lab
                    labels.append([name, n, key, ft, outcome, rule])
    with open(os.path.join(labels_dir, "expected_labels.csv"), "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, lineterminator="\n")
        w.writerow(LABEL_HEADER)
        w.writerows(labels)
    return {"customers": len(cust_final), "orders": len(ord_final), "labelled_faults": len(labels)}
