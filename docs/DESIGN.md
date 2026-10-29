# Design and trade-off note

**Why SQLite + SQL rules.** Rules compile to set-based SQL over a staging table, so 100k+ rows validate in
~1 s and every rule is auditable (the generated SQL is the rule). SQLite keeps setup to `pip install`; the SQL
(window functions, `ON CONFLICT`, `EXISTS`) ports directly to Postgres. Alternatives considered: pandas (simple
but row-level traceability and idempotent upserts are clumsier) and Great Expectations/dbt (powerful, but heavy
for the brief and hides the design I'm asked to show).

**Severity model.** `error` ⇒ reject row / block dataset; `warn` ⇒ load but record. Dataset-level rules run on
*accepted* rows so one bad future-dated row can't fake freshness. `max_reject_rate` stops a systemic upstream
break from silently publishing 30% of a file.

**Quarantine, not deletion.** Every rejected row has `source_file` + `source_row` + rule + reason (+ raw JSON for
malformed/type failures). Reconciliation asserts `rows_read = accepted + rejected` per dataset.

**History.** Orders are an event feed: append-only `fact_order_events` is the source of truth; `fact_orders_current`
is derived as "max `updated_at`", which makes late and out-of-order arrival order-independent. Customers are SCD1
(overwrite) — a conscious simplification; lineage (`run_id`) shows which run last wrote a row.

**Idempotency.** Three layers: manifest skip (file+config hash) → keyed upserts / `DO NOTHING` → one transaction per
dataset. Forced reruns are therefore safe, and a mid-run crash needs no cleanup.

**Referential integrity** is checked against the *published* target table, so datasets run in dependency order and
a rejected parent rejects its children (visible, not silent). Late parents → `--force` backfill of children.

**Performance.** Measured, not assumed: the index on `acc_<dataset>` removed an O(n²) probe (≈29× on publish at 8k rows;
the full 130k-row run takes ≈4 s). Batched inserts gave ~1.01× and are kept only as a harmless tunable.

**Cloud mapping (documented, not implemented).** Land files in S3/GCS → run `python -m dq_engine run` as a container job
(ECS/Batch/Cloud Run) orchestrated by Airflow/Step Functions, passing `--input` paths and a fixed `--run-id` per
scheduled interval (so retries replace, not append, results) → store results in Postgres (swap `sha1_16` for
`md5`/`digest`, `json_object` for `json_build_object`) or Snowflake (`MERGE`) → alert on exit code `2`/`1`.

**Limitations.** See README: single writer, CSV only, REAL money, trusted `custom_sql`, no schema migrations/deletes,
raw values post-normalisation for rule failures, basename-only `source_file`.
