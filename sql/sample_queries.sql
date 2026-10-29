-- name: 1_run_summary
SELECT run_id, status, as_of, duration_seconds, failure_reason FROM dq_run WHERE run_id = :run_id;

-- name: 2_dataset_counts_and_balance  (read = accepted + rejected)
SELECT dataset, status, rows_read, rows_rejected, rows_accepted, rows_read - rows_rejected - rows_accepted AS unexplained
FROM dq_dataset_run WHERE run_id = :run_id ORDER BY dataset;

-- name: 3_results_by_dataset_rule_severity
SELECT dataset, rule_id, severity, checked_count, failed_count, status
FROM dq_rule_result WHERE run_id = :run_id AND failed_count > 0 ORDER BY dataset, failed_count DESC;

-- name: 4_reconciliation_failures  (expected: no rows)
SELECT * FROM dq_reconciliation WHERE run_id = :run_id AND passed = 0;

-- name: 5_rejected_records_traceable_to_source
SELECT dataset, rule_id, source_file, source_row, record_key, reason
FROM dq_failed_record WHERE run_id = :run_id AND severity = 'error' ORDER BY dataset, rule_id, source_row LIMIT 8;

-- name: 6_order_with_most_versions_history  (change history preserved)
SELECT order_id, updated_at, order_status, run_id, source_file FROM fact_order_events
WHERE order_id = (SELECT order_id FROM fact_order_events GROUP BY order_id ORDER BY COUNT(*) DESC, order_id LIMIT 1)
ORDER BY updated_at;

-- name: 7_orphan_orders_in_target  (expected: 0)
SELECT COUNT(*) AS orphan_orders FROM fact_orders_current o
WHERE NOT EXISTS (SELECT 1 FROM dim_customers c WHERE c.customer_id = o.customer_id);

-- name: 8_source_to_target_aggregate  (accepted-input sum vs published history sum)
SELECT 'fact_order_events' AS table_name, COUNT(*) AS n, ROUND(SUM(total_amount), 2) AS total_amount FROM fact_order_events
UNION ALL SELECT 'fact_orders_current', COUNT(*), ROUND(SUM(total_amount), 2) FROM fact_orders_current;

-- name: 9_revenue_by_status_current
SELECT order_status, COUNT(*) AS orders, ROUND(SUM(total_amount), 2) AS revenue FROM fact_orders_current
GROUP BY order_status ORDER BY revenue DESC;

-- name: 10_every_failed_record_has_a_rule_result  (expected: no rows)
SELECT DISTINCT f.dataset, f.rule_id FROM dq_failed_record f
LEFT JOIN dq_rule_result r ON r.run_id = f.run_id AND r.dataset = f.dataset AND r.rule_id = f.rule_id
WHERE f.run_id = :run_id AND r.rule_id IS NULL;
