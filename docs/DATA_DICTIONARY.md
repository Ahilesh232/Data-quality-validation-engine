# Data dictionary

Grain, keys and rules for source and target. All timestamps ISO-8601 UTC (`...Z`). "Req" = `nullable: false`
(enforced by an auto `<dataset>_<col>_required` rule; rows violating it are quarantined, not loaded).

## Source: `customers.csv` → `dim_customers` (grain: one row per customer, latest snapshot wins)
| column | type | req | notes / rules |
|---|---|---|---|
| customer_id | string | Y | PK, e.g. `C0000001`; unique per file (`customers_unique_id`, keep first) |
| email | string | Y | trimmed + lower-cased; must match `%_@_%.__%` |
| full_name | string | N | |
| country_code | string | N | upper-cased; warn if not in US,GB,DE,FR,IN,BR,JP,AU,CA,ZA (row still loaded) |
| signup_ts | timestamp | Y | must be ≤ `--as-of` |
| credit_limit | number | N | 0 ≤ x ≤ 50000 (boundaries valid) |
| *email_domain* | string | – | derived: text after `@` |

## Source: `orders.csv` (event feed) → `fact_order_events` + `fact_orders_current`
Grain: one **event** per `(order_id, updated_at)`; `fact_orders_current` has one row per `order_id` (latest `updated_at`).
| column | type | req | notes / rules |
|---|---|---|---|
| order_id | string | Y | business key `O00000001` |
| customer_id | string | Y | FK → `dim_customers.customer_id` (`orders_customer_exists`) |
| order_status | string | Y | lower-cased; pending/paid/shipped/cancelled/refunded |
| quantity | integer | Y | 1..100 |
| unit_price | number | Y | 0.01..10000 |
| total_amount | number | Y | ≥ 0 and `|total − qty×price| ≤ 0.01` |
| order_ts | timestamp | Y | ≤ `--as-of` |
| updated_at | timestamp | Y | version/event time; ≥ `order_ts`; duplicates of `(order_id, updated_at)` rejected; dataset freshness ≤ 48 h |
| *order_date* | date | – | derived `substr(order_ts,1,10)` |
| *event_id* | string | – | derived `sha1_16(order_id, updated_at)` |

Target tables add lineage: `run_id`, `source_file`, `loaded_at`.
PKs: `dim_customers(customer_id)`, `fact_order_events(order_id, updated_at)`, `fact_orders_current(order_id)`.

## Operational tables (the "results store")
| table | grain / key | purpose |
|---|---|---|
| dq_run | run_id | status SUCCEEDED/PARTIAL/FAILED/RUNNING/ABANDONED, start/end, as_of, config hash, failure_reason |
| dq_dataset_run | (run_id, dataset) | status PUBLISHED/BLOCKED/FAILED/SKIPPED; rows_read, rows_unparseable, rows_staged, rows_rejected, rows_warned, rows_accepted, rows_inserted, rows_updated, rows_already_loaded, late_arriving, stage timings, failure_reason |
| dq_rule_result | (run_id, dataset, rule_id) | rule_type, **severity**, checked_count, failed_count, PASS/FAIL, detail |
| dq_failed_record | failure_id | quarantine: rule_id, severity, source_file, source_row, record_key, reason, raw_record (JSON) |
| dq_ingest_manifest | (dataset, file_sha256) | idempotency fingerprint of published inputs |
| dq_reconciliation | (run_id, dataset, check_name) | expected vs actual, passed flag |
| stg_<dataset> | source_row | transient typed staging for the latest run (rebuilt each run) |

Built-in pseudo-rules: `schema_columns` (warn: extra columns ignored; error: missing columns ⇒ FAILED),
`schema_malformed_row`, `schema_type`.
