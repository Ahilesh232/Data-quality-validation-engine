# Data Quality Validation Engine

Data Quality Validation Engine is a configurable data validation and monitoring platform designed to ensure the accuracy, consistency, completeness, and reliability of datasets across multiple business domains.

The system allows users to upload datasets, validate data using configurable rules, detect duplicates and invalid records, identify data-quality issues, and quarantine rejected records. It also tracks pipeline execution, maintains validation history, supports safe reruns, and generates detailed data-quality reports with measurable metrics — all through an intuitive **Streamlit interface** powered by Python, Pandas, and SQLite.

---

## 🚀 Setup & Quick Start

```bash
# 1. Setup environment and install dependencies
python -m venv .venv
source .venv/bin/activate    # Windows: .venv\Scripts\Activate.ps1
pip install -r requirements.txt

# 2. Run automated test suite (46 tests)
python -m pytest -q

# 3. Launch interactive web dashboard
streamlit run app.py
```
*Web Dashboard live at **http://localhost:8501***.

---

## 🏛️ Architecture & Pipeline Flow

The engine enforces a transactional **CSV ➔ Validate ➔ Quarantine ➔ Publish ➔ Reconcile** flow:

```
  customers.csv ─┐                                   ┌─ dq_failed_record   (quarantine: file, row, rule, reason, raw)
  orders.csv ────┤  per dataset, ONE transaction:    ├─ dq_rule_result     (dataset, rule, run, severity, counts)
                 ▼                                   ├─ dq_dataset_run     (status, counts, timings, failure reason)
   1. Schema Check ─── missing col ⇒ FAILED          ├─ dq_run / dq_reconciliation / dq_ingest_manifest
   2. Parse & Cast ─── malformed row ⇒ quarantine    │
   3. Stage (stg_*) ── typed SQLite staging          │      ┌─ dim_customers       (upsert, SCD1)
   4. SQL Row Rules ── error ⇒ quarantine, warn ⇒ log│      ├─ fact_order_events   (append-only event history)
   5. Filter Clean ─── acc_* = staged − rejected     ├────► └─ fact_orders_current (latest state per order)
   6. Dataset Guard ── freshness & reject-rate checks│
   7. Atomic Publish ── idempotent commit + sums ────┘
  then: reconcile() ──► dq_reconciliation (accounting balance & zero orphan orders)
```

---

## 📖 Data Dictionary

### Target & Source Tables

| Table | Grain / Key | Source | Key Validation Rules & Constraints |
|---|---|---|---|
| `dim_customers` | `customer_id` | `customers.csv` | PK `C0000001`; valid email (`%_@_%.__%`); $0 \le \text{credit\_limit} \le 50,000$; $\text{signup\_ts} \le \text{as\_of}$. |
| `fact_order_events` | `(order_id, updated_at)` | `orders.csv` | Event-history feed; FK $\rightarrow$ `dim_customers`; status in allowed domain; $\text{updated\_at} \ge \text{order\_ts}$. |
| `fact_orders_current` | `order_id` | `orders.csv` | Current snapshot; latest `updated_at`; $\text{total} = \text{qty} \times \text{price}$; $1 \le \text{qty} \le 100$. |

### Operational & Audit Store (`output/dq.db`)

| Table | Purpose | Tracked Columns / Metrics |
|---|---|---|
| `dq_run` | Run metadata | `run_id`, `status` (SUCCEEDED/PARTIAL/FAILED), timestamps, `duration_seconds`. |
| `dq_dataset_run` | Dataset metrics | `rows_read`, `rows_staged`, `rows_rejected`, `rows_accepted`, `stage_timings`. |
| `dq_failed_record` | **Quarantine Registry** | `source_file`, `source_row`, `record_key`, `rule_id`, `reason`, `raw_record` (JSON). |
| `dq_rule_result` | Rule telemetry | Checked counts, failed counts, severity (`error`, `warn`), status. |
| `dq_reconciliation` | Integrity audits | Asserts `rows_read = accepted + rejected`, sum balances, zero orphan orders. |
| `dq_upload_history` | Upload audit log | Tracks user CSV uploads, file sizes, row counts, and run IDs. |
| `dq_ingest_manifest`| Idempotency cache | SHA-256 file fingerprints preventing duplicate processing. |

---

## ⚙️ Configurable Quality Rules (`config/pipeline.yaml`)

| Rule | Level | Severity | Behavior |
|---|---|:---:|---|
| `not_null` | Row | `error` | Completeness check; rejects records missing mandatory fields. |
| `unique` | Row | `error` | Duplicate check; keeps first record, quarantines duplicates. |
| `referential` | Row | `error` | Foreign key check; asserts key exists in published dimension. |
| `range` | Row | `error` | Boundary checks for numbers and timestamps ($\le \text{"\$as\_of"}$). |
| `accepted_values` | Row | `error`/`warn` | Domain whitelist validation. |
| `custom_sql` | Row | `error` | Business calculation checks (e.g., `abs(total - qty * price) <= 0.01`). |
| `freshness` | Dataset | `error` | Blocks publish if newest record age exceeds threshold (e.g., 48 h). |
| `max_reject_rate` | Dataset | `error` | Quality guard; blocks publish if quarantine rate $> 25\%$. |

---

## 🛡️ Idempotency, Performance & Reruns

- **Fingerprint Deduplication**: Inputs are fingerprinted via `SHA-256` in `dq_ingest_manifest`; identical files are safely skipped on reruns unless forced.
- **Atomic Transactions**: Ingestion, validation, publishing, and audit logging for each dataset commit inside a single SQLite transaction.
- **Late & Out-of-Order Events**: Append-only event history; current order state derived from `max(updated_at)`, making arrival order independent.
- **Performance Optimization**: Staged accepted records are indexed by business key (`INDEX_ACC`), eliminating an $O(n^2)$ lookup and delivering a **~29× publish speedup** (~130k records processed in ≈4 s).
