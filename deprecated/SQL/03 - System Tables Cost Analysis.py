# Databricks notebook source
# MAGIC %md
# MAGIC # System Tables Cost Analysis

# COMMAND ----------

# DBTITLE 1,Cost by Product (Last 30 Days)
# MAGIC %sql
# MAGIC -- Cost by product (last 30 days): JOBS, SQL, ALL_PURPOSE, DLT, etc.
# MAGIC -- Joins usage with list_prices for estimated cost (usage_quantity * price).
# MAGIC WITH usage_net AS (
# MAGIC   SELECT
# MAGIC     usage_date,
# MAGIC     billing_origin_product,
# MAGIC     sku_name,
# MAGIC     usage_unit,
# MAGIC     SUM(usage_quantity) AS usage_quantity
# MAGIC   FROM system.billing.usage
# MAGIC   WHERE record_type = 'ORIGINAL'
# MAGIC     AND usage_date >= current_date() - INTERVAL 30 DAYS
# MAGIC   GROUP BY usage_date, billing_origin_product, sku_name, usage_unit
# MAGIC ),
# MAGIC prices AS (
# MAGIC   SELECT
# MAGIC     sku_name,
# MAGIC     cloud,
# MAGIC     usage_unit,
# MAGIC     CAST(pricing.effective_list.default AS DOUBLE) AS unit_price
# MAGIC   FROM system.billing.list_prices
# MAGIC   WHERE price_end_time > current_timestamp() - INTERVAL 30 DAYS
# MAGIC   QUALIFY ROW_NUMBER() OVER (PARTITION BY sku_name, cloud, usage_unit ORDER BY price_start_time DESC) = 1
# MAGIC )
# MAGIC SELECT
# MAGIC   u.usage_date,
# MAGIC   u.billing_origin_product AS product,
# MAGIC   u.sku_name,
# MAGIC   u.usage_quantity,
# MAGIC   COALESCE(p.unit_price, 0) AS unit_price,
# MAGIC   ROUND(u.usage_quantity * COALESCE(p.unit_price, 0), 2) AS estimated_cost_usd
# MAGIC FROM usage_net u
# MAGIC LEFT JOIN prices p ON u.sku_name = p.sku_name AND u.usage_unit = p.usage_unit
# MAGIC ORDER BY u.usage_date DESC, estimated_cost_usd DESC;

# COMMAND ----------

# DBTITLE 1,Cost by Workspace and SKU (Last 30 Days)
# MAGIC %sql
# MAGIC -- Cost by workspace and SKU (last 30 days)
# MAGIC WITH usage_net AS (
# MAGIC   SELECT
# MAGIC     workspace_id,
# MAGIC     sku_name,
# MAGIC     billing_origin_product,
# MAGIC     SUM(usage_quantity) AS usage_quantity
# MAGIC   FROM system.billing.usage
# MAGIC   WHERE record_type = 'ORIGINAL'
# MAGIC     AND usage_date >= current_date() - INTERVAL 30 DAYS
# MAGIC   GROUP BY workspace_id, sku_name, billing_origin_product
# MAGIC ),
# MAGIC prices AS (
# MAGIC   SELECT
# MAGIC     sku_name,
# MAGIC     CAST(pricing.effective_list.default AS DOUBLE) AS unit_price
# MAGIC   FROM system.billing.list_prices
# MAGIC   WHERE price_end_time > current_timestamp()
# MAGIC   QUALIFY ROW_NUMBER() OVER (PARTITION BY sku_name ORDER BY price_start_time DESC) = 1
# MAGIC )
# MAGIC SELECT
# MAGIC   u.workspace_id,
# MAGIC   u.sku_name,
# MAGIC   u.billing_origin_product AS product,
# MAGIC   u.usage_quantity,
# MAGIC   ROUND(u.usage_quantity * COALESCE(p.unit_price, 0), 2) AS estimated_cost_usd
# MAGIC FROM usage_net u
# MAGIC LEFT JOIN prices p ON u.sku_name = p.sku_name
# MAGIC ORDER BY estimated_cost_usd DESC;

# COMMAND ----------

# DBTITLE 1,Cost by User (Last 30 Days)
# MAGIC %sql
# MAGIC -- Cost by user (run_as) – last 30 days
# MAGIC SELECT
# MAGIC   usage_date,
# MAGIC   identity_metadata.run_as AS run_as_user,
# MAGIC   billing_origin_product AS product,
# MAGIC   sku_name,
# MAGIC   SUM(usage_quantity) AS usage_quantity
# MAGIC FROM system.billing.usage
# MAGIC WHERE record_type = 'ORIGINAL'
# MAGIC   AND usage_date >= current_date() - INTERVAL 30 DAYS
# MAGIC   AND identity_metadata.run_as IS NOT NULL
# MAGIC GROUP BY usage_date, identity_metadata.run_as, billing_origin_product, sku_name
# MAGIC ORDER BY usage_date DESC, usage_quantity DESC;

# COMMAND ----------

# DBTITLE 1,Daily Cost Trend (Last 90 Days)
# MAGIC %sql
# MAGIC -- Daily cost trend (last 90 days) – for dashboards
# MAGIC WITH usage_net AS (
# MAGIC   SELECT
# MAGIC     usage_date,
# MAGIC     billing_origin_product,
# MAGIC     sku_name,
# MAGIC     usage_unit,
# MAGIC     SUM(usage_quantity) AS usage_quantity
# MAGIC   FROM system.billing.usage
# MAGIC   WHERE record_type = 'ORIGINAL'
# MAGIC     AND usage_date >= current_date() - INTERVAL 90 DAYS
# MAGIC   GROUP BY usage_date, billing_origin_product, sku_name, usage_unit
# MAGIC ),
# MAGIC prices AS (
# MAGIC   SELECT
# MAGIC     sku_name,
# MAGIC     usage_unit,
# MAGIC     CAST(pricing.effective_list.default AS DOUBLE) AS unit_price
# MAGIC   FROM system.billing.list_prices
# MAGIC   WHERE price_end_time > current_timestamp()
# MAGIC   QUALIFY ROW_NUMBER() OVER (PARTITION BY sku_name, usage_unit ORDER BY price_start_time DESC) = 1
# MAGIC )
# MAGIC SELECT
# MAGIC   u.usage_date,
# MAGIC   SUM(u.usage_quantity ) AS estimated_daily_cost_usd
# MAGIC FROM usage_net u
# MAGIC LEFT JOIN prices p ON u.sku_name = p.sku_name AND u.usage_unit = p.usage_unit
# MAGIC GROUP BY u.usage_date
# MAGIC ORDER BY u.usage_date DESC;

# COMMAND ----------

# DBTITLE 1,Top Expensive Jobs (Last 30 Days)
# MAGIC %sql
# MAGIC -- Top expensive jobs by usage (last 30 days)
# MAGIC SELECT
# MAGIC   usage_metadata.job_id AS job_id,
# MAGIC   usage_metadata.job_name AS job_name,
# MAGIC   billing_origin_product AS product,
# MAGIC   sku_name,
# MAGIC   SUM(usage_quantity) AS total_usage_quantity
# MAGIC FROM system.billing.usage
# MAGIC WHERE record_type = 'ORIGINAL'
# MAGIC   AND usage_date >= current_date() - INTERVAL 30 DAYS
# MAGIC   AND usage_metadata.job_id IS NOT NULL
# MAGIC GROUP BY usage_metadata.job_id, usage_metadata.job_name, billing_origin_product, sku_name
# MAGIC ORDER BY total_usage_quantity DESC
# MAGIC LIMIT 50;

# COMMAND ----------

# DBTITLE 1,Failed Queries (Last 7 Days)
# MAGIC %sql
# MAGIC -- Failed queries (last 7 days) – find bugs and permission issues
# MAGIC SELECT
# MAGIC   statement_id,
# MAGIC   start_time,
# MAGIC   executed_by,
# MAGIC   execution_status,
# MAGIC   LEFT(statement_text, 200) AS statement_preview,
# MAGIC   error_message,
# MAGIC   total_duration_ms,
# MAGIC   query_source
# MAGIC FROM system.query.history
# MAGIC WHERE execution_status = 'FAILED'
# MAGIC   AND start_time >= current_timestamp() - INTERVAL 7 DAYS
# MAGIC ORDER BY start_time DESC
# MAGIC LIMIT 200;

# COMMAND ----------

# DBTITLE 1,Failures by User (Last 14 Days)
# MAGIC %sql
# MAGIC -- Failed queries grouped by user – who hits errors most (last 14 days)
# MAGIC SELECT
# MAGIC   executed_by,
# MAGIC   COUNT(*) AS failure_count,
# MAGIC   COLLECT_LIST(DISTINCT LEFT(error_message, 80)) AS sample_errors
# MAGIC FROM system.query.history
# MAGIC WHERE execution_status = 'FAILED'
# MAGIC   AND start_time >= current_timestamp() - INTERVAL 14 DAYS
# MAGIC GROUP BY executed_by
# MAGIC ORDER BY failure_count DESC;

# COMMAND ----------

# DBTITLE 1,Common Error Messages (Last 14 Days)
# MAGIC %sql
# MAGIC -- Common error messages – pattern detection for bugs (last 14 days)
# MAGIC SELECT
# MAGIC   LEFT(error_message, 120) AS error_preview,
# MAGIC   COUNT(*) AS occurrence_count,
# MAGIC   COUNT(DISTINCT executed_by) AS distinct_users
# MAGIC FROM system.query.history
# MAGIC WHERE execution_status = 'FAILED'
# MAGIC   AND error_message IS NOT NULL
# MAGIC   AND start_time >= current_timestamp() - INTERVAL 14 DAYS
# MAGIC GROUP BY LEFT(error_message, 120)
# MAGIC ORDER BY occurrence_count DESC
# MAGIC LIMIT 50;

# COMMAND ----------

# DBTITLE 1,Slow or Canceled Queries (Last 7 Days)
# MAGIC %sql
# MAGIC -- Long-running and canceled queries (last 7 days)
# MAGIC SELECT
# MAGIC   statement_id,
# MAGIC   start_time,
# MAGIC   executed_by,
# MAGIC   execution_status,
# MAGIC   total_duration_ms / 1000.0 AS duration_seconds,
# MAGIC   LEFT(statement_text, 150) AS statement_preview,
# MAGIC   error_message
# MAGIC FROM system.query.history
# MAGIC WHERE start_time >= current_timestamp() - INTERVAL 7 DAYS
# MAGIC   AND (
# MAGIC     execution_status = 'CANCELED'
# MAGIC     OR total_duration_ms > 300000
# MAGIC   )
# MAGIC ORDER BY total_duration_ms DESC
# MAGIC LIMIT 100;

# COMMAND ----------

# DBTITLE 1,Tables with No Recent Reads (Last 90 Days)
# MAGIC %sql
# MAGIC -- Tables with no READ lineage in the last 90 days
# MAGIC WITH recent_reads AS (
# MAGIC   SELECT DISTINCT source_table_full_name AS table_full_name
# MAGIC   FROM system.access.table_lineage
# MAGIC   WHERE source_table_full_name IS NOT NULL
# MAGIC     AND event_date >= current_date() - INTERVAL 90 DAYS
# MAGIC ),
# MAGIC all_seen_tables AS (
# MAGIC   SELECT DISTINCT source_table_full_name AS table_full_name
# MAGIC   FROM system.access.table_lineage
# MAGIC   WHERE source_table_full_name IS NOT NULL
# MAGIC   UNION
# MAGIC   SELECT DISTINCT target_table_full_name
# MAGIC   FROM system.access.table_lineage
# MAGIC   WHERE target_table_full_name IS NOT NULL
# MAGIC )
# MAGIC SELECT
# MAGIC   a.table_full_name,
# MAGIC   'no_reads_last_90d' AS reason
# MAGIC FROM all_seen_tables a
# MAGIC LEFT JOIN recent_reads r ON a.table_full_name = r.table_full_name
# MAGIC WHERE r.table_full_name IS NULL
# MAGIC ORDER BY a.table_full_name;

# COMMAND ----------

# DBTITLE 1,Cold Tables (Last 90 Days)
# MAGIC %sql
# MAGIC -- Cold tables: no lineage activity (read or write) in the last 90 days
# MAGIC WITH active_90d AS (
# MAGIC   SELECT DISTINCT source_table_full_name AS table_full_name
# MAGIC   FROM system.access.table_lineage
# MAGIC   WHERE event_date >= current_date() - INTERVAL 90 DAYS
# MAGIC     AND source_table_full_name IS NOT NULL
# MAGIC   UNION
# MAGIC   SELECT DISTINCT target_table_full_name
# MAGIC   FROM system.access.table_lineage
# MAGIC   WHERE event_date >= current_date() - INTERVAL 90 DAYS
# MAGIC     AND target_table_full_name IS NOT NULL
# MAGIC ),
# MAGIC all_known AS (
# MAGIC   SELECT DISTINCT source_table_full_name AS table_full_name
# MAGIC   FROM system.access.table_lineage
# MAGIC   WHERE source_table_full_name IS NOT NULL
# MAGIC   UNION
# MAGIC   SELECT DISTINCT target_table_full_name
# MAGIC   FROM system.access.table_lineage
# MAGIC   WHERE target_table_full_name IS NOT NULL
# MAGIC )
# MAGIC SELECT
# MAGIC   k.table_full_name,
# MAGIC   'no_activity_90d' AS reason
# MAGIC FROM all_known k
# MAGIC LEFT JOIN active_90d a ON k.table_full_name = a.table_full_name
# MAGIC WHERE a.table_full_name IS NULL
# MAGIC ORDER BY k.table_full_name;

# COMMAND ----------

# DBTITLE 1,Unused Tables by Catalog/Schema
# MAGIC %sql
# MAGIC -- Unused tables: list tables from a catalog's information_schema with no lineage in 90 days
# MAGIC -- Replace your_catalog with your actual catalog name (e.g. main, analytics). Run per catalog.
# MAGIC WITH lineage_tables_90d AS (
# MAGIC   SELECT DISTINCT source_table_full_name AS full_name
# MAGIC   FROM system.access.table_lineage
# MAGIC   WHERE event_date >= current_date() - INTERVAL 90 DAYS
# MAGIC   UNION
# MAGIC   SELECT DISTINCT target_table_full_name
# MAGIC   FROM system.access.table_lineage
# MAGIC   WHERE event_date >= current_date() - INTERVAL 90 DAYS
# MAGIC )
# MAGIC SELECT
# MAGIC   t.table_catalog,
# MAGIC   t.table_schema,
# MAGIC   t.table_name,
# MAGIC   CONCAT(t.table_catalog, '.', t.table_schema, '.', t.table_name) AS full_name,
# MAGIC   CASE WHEN l.full_name IS NULL THEN 'no_lineage_90d' ELSE 'used' END AS usage_status
# MAGIC FROM main.information_schema.tables t   -- change 'main' to your catalog
# MAGIC LEFT JOIN lineage_tables_90d l
# MAGIC   ON CONCAT(t.table_catalog, '.', t.table_schema, '.', t.table_name) = l.full_name
# MAGIC WHERE t.table_schema NOT IN ('information_schema', 'system')
# MAGIC   AND t.table_type IN ('MANAGED', 'EXTERNAL')
# MAGIC ORDER BY usage_status, full_name;

# COMMAND ----------

# DBTITLE 1,Query Result Cache Hit Rate (Last 7 Days)
# MAGIC %sql
# MAGIC -- Query result cache hit rate (last 7 days)
# MAGIC SELECT
# MAGIC   DATE(start_time) AS query_date,
# MAGIC   COUNT(*) AS total_queries,
# MAGIC   SUM(CASE WHEN from_result_cache = TRUE THEN 1 ELSE 0 END) AS cache_hits,
# MAGIC   ROUND(100.0 * SUM(CASE WHEN from_result_cache = TRUE THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2) AS cache_hit_rate_pct
# MAGIC FROM system.query.history
# MAGIC WHERE start_time >= current_timestamp() - INTERVAL 7 DAYS
# MAGIC   AND execution_status = 'FINISHED'
# MAGIC GROUP BY DATE(start_time)
# MAGIC ORDER BY query_date DESC;

# COMMAND ----------

# DBTITLE 1,Cost by Custom Tag (Last 30 Days)
# MAGIC %sql
# MAGIC -- Cost by custom tag (last 30 days)
# MAGIC SELECT
# MAGIC   usage_date,
# MAGIC   custom_tags.team AS tag_team,
# MAGIC   custom_tags.cost_center AS tag_cost_center,
# MAGIC   custom_tags.env AS tag_env,
# MAGIC   billing_origin_product,
# MAGIC   SUM(usage_quantity) AS usage_quantity
# MAGIC FROM system.billing.usage
# MAGIC WHERE record_type = 'ORIGINAL'
# MAGIC   AND usage_date >= current_date() - INTERVAL 30 DAYS
# MAGIC   AND (custom_tags IS NOT NULL AND size(custom_tags) > 0)
# MAGIC GROUP BY usage_date, custom_tags.team, custom_tags.cost_center, custom_tags.env, billing_origin_product
# MAGIC ORDER BY usage_date DESC, usage_quantity DESC;

# COMMAND ----------

# DBTITLE 1,Queries with High Spill or I/O (Last 7 Days)
# MAGIC %sql
# MAGIC -- Queries with high spill or read I/O (last 7 days)
# MAGIC SELECT
# MAGIC   statement_id,
# MAGIC   start_time,
# MAGIC   executed_by,
# MAGIC   total_duration_ms / 1000.0 AS duration_sec,
# MAGIC   read_bytes / 1024 / 1024 AS read_mb,
# MAGIC   spilled_local_bytes / 1024 / 1024 AS spill_mb,
# MAGIC   written_bytes / 1024 / 1024 AS written_mb,
# MAGIC   read_rows,
# MAGIC   produced_rows,
# MAGIC   LEFT(statement_text, 120) AS statement_preview
# MAGIC FROM system.query.history
# MAGIC WHERE start_time >= current_timestamp() - INTERVAL 7 DAYS
# MAGIC   AND execution_status = 'FINISHED'
# MAGIC   AND (spilled_local_bytes > 0 OR read_bytes > 1e9)
# MAGIC ORDER BY read_bytes DESC
# MAGIC LIMIT 50;

# COMMAND ----------

# DBTITLE 1,Job Run Failures (Last 14 Days)
# MAGIC %sql
# MAGIC -- Job run failures (last 14 days) – from job run timeline
# MAGIC SELECT
# MAGIC   job_id,
# MAGIC   run_id AS job_run_id,
# MAGIC   MIN(period_start_time) AS start_time,
# MAGIC   MAX(period_end_time) AS end_time,
# MAGIC   CAST(SUM(UNIX_TIMESTAMP(period_end_time) - UNIX_TIMESTAMP(period_start_time)) AS BIGINT) AS duration_sec,
# MAGIC   MAX(result_state) AS result_state
# MAGIC FROM system.lakeflow.job_run_timeline
# MAGIC WHERE period_start_time >= current_timestamp() - INTERVAL 14 DAYS
# MAGIC GROUP BY job_id, run_id
# MAGIC HAVING MAX(result_state) = 'FAILED'
# MAGIC ORDER BY start_time DESC
# MAGIC LIMIT 100;

# COMMAND ----------

# DBTITLE 1,Lineage: Downstream Consumers
# MAGIC %sql
# MAGIC -- Lineage: tables with most downstream consumers (targets)
# MAGIC SELECT
# MAGIC   source_table_full_name,
# MAGIC   COUNT(DISTINCT target_table_full_name) AS downstream_count,
# MAGIC   COLLECT_SET(DISTINCT entity_type) AS consumer_types
# MAGIC FROM system.access.table_lineage
# MAGIC WHERE source_table_full_name IS NOT NULL
# MAGIC   AND target_table_full_name IS NOT NULL
# MAGIC   AND event_date >= current_date() - INTERVAL 90 DAYS
# MAGIC GROUP BY source_table_full_name
# MAGIC ORDER BY downstream_count DESC
# MAGIC LIMIT 100;

# COMMAND ----------

# DBTITLE 1,Warehouse vs Jobs Cost Split (Last 30 Days)
# MAGIC %sql
# MAGIC -- Warehouse vs Jobs cost split (last 30 days)
# MAGIC SELECT
# MAGIC   billing_origin_product AS product,
# MAGIC   CASE
# MAGIC     WHEN billing_origin_product IN ('SQL', 'ALL_PURPOSE') THEN 'Warehouse / Interactive'
# MAGIC     WHEN billing_origin_product IN ('JOBS', 'DLT', 'INTERACTIVE') THEN 'Jobs / DLT / Serverless'
# MAGIC     ELSE 'Other'
# MAGIC   END AS bucket,
# MAGIC   SUM(usage_quantity) AS total_usage_quantity
# MAGIC FROM system.billing.usage
# MAGIC WHERE record_type = 'ORIGINAL'
# MAGIC   AND usage_date >= current_date() - INTERVAL 30 DAYS
# MAGIC GROUP BY billing_origin_product
# MAGIC ORDER BY total_usage_quantity DESC;

# COMMAND ----------

# DBTITLE 1,Alert Triggered Queries
# MAGIC %sql
# MAGIC SELECT
# MAGIC   query_source.alert_id AS alert_id,
# MAGIC   COUNT(*) AS runs,
# MAGIC   SUM(CASE WHEN execution_status = 'FAILED' THEN 1 ELSE 0 END) AS failures,
# MAGIC   MIN(start_time) AS first_run,
# MAGIC   MAX(start_time) AS last_run
# MAGIC FROM system.query.history
# MAGIC WHERE start_time >= current_timestamp() - INTERVAL 7 DAYS
# MAGIC   AND query_source.alert_id IS NOT NULL
# MAGIC GROUP BY query_source.alert_id
# MAGIC ORDER BY runs DESC;

# COMMAND ----------

# DBTITLE 1,Compilation vs Execution Time
# MAGIC %sql
# MAGIC SELECT
# MAGIC   statement_id,
# MAGIC   start_time,
# MAGIC   executed_by,
# MAGIC   compilation_duration_ms / 1000.0 AS compile_sec,
# MAGIC   execution_duration_ms / 1000.0 AS execution_sec,
# MAGIC   total_duration_ms / 1000.0 AS total_sec,
# MAGIC   ROUND(100.0 * compilation_duration_ms / NULLIF(total_duration_ms, 0), 1) AS compile_pct,
# MAGIC   LEFT(statement_text, 100) AS statement_preview
# MAGIC FROM system.query.history
# MAGIC WHERE execution_status = 'FINISHED'
# MAGIC   AND start_time >= current_timestamp() - INTERVAL 7 DAYS
# MAGIC   AND total_duration_ms > 0
# MAGIC ORDER BY compilation_duration_ms DESC
# MAGIC LIMIT 50;

# COMMAND ----------

# DBTITLE 1,Cost by Cluster (Last 30 Days)
# MAGIC %sql
# MAGIC SELECT
# MAGIC   usage_metadata.cluster_id AS cluster_id,
# MAGIC   billing_origin_product,
# MAGIC   sku_name,
# MAGIC   SUM(usage_quantity) AS usage_quantity
# MAGIC FROM system.billing.usage
# MAGIC WHERE record_type = 'ORIGINAL'
# MAGIC   AND usage_date >= current_date() - INTERVAL 30 DAYS
# MAGIC   AND usage_metadata.cluster_id IS NOT NULL
# MAGIC GROUP BY usage_metadata.cluster_id, billing_origin_product, sku_name
# MAGIC ORDER BY usage_quantity DESC
# MAGIC LIMIT 50;

# COMMAND ----------

# DBTITLE 1,Cost by Warehouse (Last 30 Days)
# MAGIC %sql
# MAGIC SELECT
# MAGIC   usage_metadata.warehouse_id AS warehouse_id,
# MAGIC   billing_origin_product,
# MAGIC   sku_name,
# MAGIC   SUM(usage_quantity) AS usage_quantity
# MAGIC FROM system.billing.usage
# MAGIC WHERE record_type = 'ORIGINAL'
# MAGIC   AND usage_date >= current_date() - INTERVAL 30 DAYS
# MAGIC   AND usage_metadata.warehouse_id IS NOT NULL
# MAGIC GROUP BY usage_metadata.warehouse_id, billing_origin_product, sku_name
# MAGIC ORDER BY usage_quantity DESC
# MAGIC LIMIT 50;

# COMMAND ----------

# DBTITLE 1,Cost Week Over Week
# MAGIC %sql
# MAGIC WITH weekly_usage AS (
# MAGIC   SELECT
# MAGIC     DATE_TRUNC('week', usage_date) AS week_start,
# MAGIC     billing_origin_product AS product,
# MAGIC     sku_name,
# MAGIC     usage_unit,
# MAGIC     SUM(usage_quantity) AS usage_quantity
# MAGIC   FROM system.billing.usage
# MAGIC   WHERE record_type = 'ORIGINAL'
# MAGIC     AND usage_date >= current_date() - INTERVAL 56 DAYS
# MAGIC   GROUP BY DATE_TRUNC('week', usage_date), billing_origin_product, sku_name, usage_unit
# MAGIC ),
# MAGIC prices AS (
# MAGIC   SELECT
# MAGIC     sku_name,
# MAGIC     usage_unit,
# MAGIC     CAST(COALESCE(pricing.effective_list.default, pricing.default) AS DOUBLE) AS unit_price
# MAGIC   FROM system.billing.list_prices
# MAGIC   WHERE price_end_time > current_timestamp()
# MAGIC   QUALIFY ROW_NUMBER() OVER (PARTITION BY sku_name, usage_unit ORDER BY price_start_time DESC) = 1
# MAGIC )
# MAGIC SELECT
# MAGIC   w.week_start,
# MAGIC   w.product,
# MAGIC   w.usage_quantity,
# MAGIC   ROUND(w.usage_quantity * COALESCE(p.unit_price, 0), 2) AS estimated_cost_usd
# MAGIC FROM weekly_usage w
# MAGIC LEFT JOIN prices p ON w.sku_name = p.sku_name AND w.usage_unit = p.usage_unit
# MAGIC ORDER BY w.week_start DESC, estimated_cost_usd DESC;

# COMMAND ----------

# DBTITLE 1,Dashboard Triggered Queries
# MAGIC %sql
# MAGIC SELECT
# MAGIC   query_source.dashboard_id AS dashboard_id,
# MAGIC   COUNT(*) AS query_count,
# MAGIC   SUM(total_duration_ms) / 1000.0 AS total_duration_sec
# MAGIC FROM system.query.history
# MAGIC WHERE start_time >= current_timestamp() - INTERVAL 7 DAYS
# MAGIC   AND query_source.dashboard_id IS NOT NULL
# MAGIC GROUP BY query_source.dashboard_id
# MAGIC ORDER BY query_count DESC
# MAGIC LIMIT 50;

# COMMAND ----------

# DBTITLE 1,Failure Rate by Client
# MAGIC %sql
# MAGIC SELECT
# MAGIC   COALESCE(client_application, 'unknown') AS client_application,
# MAGIC   COUNT(*) AS total_queries,
# MAGIC   SUM(CASE WHEN execution_status = 'FAILED' THEN 1 ELSE 0 END) AS failures,
# MAGIC   ROUND(100.0 * SUM(CASE WHEN execution_status = 'FAILED' THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2) AS failure_rate_pct
# MAGIC FROM system.query.history
# MAGIC WHERE start_time >= current_timestamp() - INTERVAL 14 DAYS
# MAGIC GROUP BY client_application
# MAGIC HAVING COUNT(*) >= 5
# MAGIC ORDER BY failure_rate_pct DESC, failures DESC;

# COMMAND ----------

# DBTITLE 1,Failure Rate by Warehouse
# MAGIC %sql
# MAGIC SELECT
# MAGIC   compute.warehouse_id AS warehouse_id,
# MAGIC   COUNT(*) AS total_queries,
# MAGIC   SUM(CASE WHEN execution_status = 'FAILED' THEN 1 ELSE 0 END) AS failures,
# MAGIC   ROUND(100.0 * SUM(CASE WHEN execution_status = 'FAILED' THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2) AS failure_rate_pct
# MAGIC FROM system.query.history
# MAGIC WHERE start_time >= current_timestamp() - INTERVAL 14 DAYS
# MAGIC   AND compute.warehouse_id IS NOT NULL
# MAGIC GROUP BY compute.warehouse_id
# MAGIC HAVING COUNT(*) >= 10
# MAGIC ORDER BY failure_rate_pct DESC, failures DESC;

# COMMAND ----------

# DBTITLE 1,I/O Cache Effectiveness
# MAGIC %sql
# MAGIC SELECT
# MAGIC   DATE(start_time) AS query_date,
# MAGIC   COUNT(*) AS queries_with_reads,
# MAGIC   AVG(read_io_cache_percent) AS avg_cache_pct,
# MAGIC   SUM(read_bytes) / 1024 / 1024 / 1024 AS total_read_gb
# MAGIC FROM system.query.history
# MAGIC WHERE execution_status = 'FINISHED'
# MAGIC   AND start_time >= current_timestamp() - INTERVAL 7 DAYS
# MAGIC   AND read_bytes > 0
# MAGIC GROUP BY DATE(start_time)
# MAGIC ORDER BY query_date DESC;

# COMMAND ----------

# DBTITLE 1,Job Frequency
# MAGIC %sql
# MAGIC WITH run_state AS (
# MAGIC   SELECT job_id, run_id, MIN(period_start_time) AS run_start, MAX(result_state) AS result_state
# MAGIC   FROM system.lakeflow.job_run_timeline
# MAGIC   WHERE period_start_time >= current_timestamp() - INTERVAL 14 DAYS
# MAGIC   GROUP BY job_id, run_id
# MAGIC )
# MAGIC SELECT
# MAGIC   job_id,
# MAGIC   COUNT(DISTINCT run_id) AS run_count,
# MAGIC   COUNT(DISTINCT run_id) / 14.0 AS runs_per_day,
# MAGIC   MIN(run_start) AS first_run,
# MAGIC   MAX(run_start) AS last_run,
# MAGIC   SUM(CASE WHEN result_state = 'SUCCESS' THEN 1 ELSE 0 END) AS success_count,
# MAGIC   SUM(CASE WHEN result_state = 'FAILED' THEN 1 ELSE 0 END) AS fail_count
# MAGIC FROM run_state
# MAGIC GROUP BY job_id
# MAGIC ORDER BY run_count DESC
# MAGIC LIMIT 50;

# COMMAND ----------

# DBTITLE 1,Job Run Duration Trend
# MAGIC %sql
# MAGIC WITH run_durations AS (
# MAGIC   SELECT
# MAGIC     job_id,
# MAGIC     run_id,
# MAGIC     MIN(period_start_time) AS run_start,
# MAGIC     CAST(SUM(UNIX_TIMESTAMP(period_end_time) - UNIX_TIMESTAMP(period_start_time)) AS BIGINT) AS duration_sec,
# MAGIC     MAX(result_state) AS result_state
# MAGIC   FROM system.lakeflow.job_run_timeline
# MAGIC   WHERE period_start_time >= current_timestamp() - INTERVAL 30 DAYS
# MAGIC   GROUP BY job_id, run_id
# MAGIC )
# MAGIC SELECT
# MAGIC   job_id,
# MAGIC   DATE(run_start) AS run_date,
# MAGIC   COUNT(*) AS run_count,
# MAGIC   AVG(duration_sec) AS avg_duration_sec,
# MAGIC   MAX(duration_sec) AS max_duration_sec
# MAGIC FROM run_durations
# MAGIC WHERE result_state = 'SUCCESS'
# MAGIC GROUP BY job_id, DATE(run_start)
# MAGIC ORDER BY run_date DESC, avg_duration_sec DESC;

# COMMAND ----------

# DBTITLE 1,Jobs Always Fail
# MAGIC %sql
# MAGIC WITH run_outcomes AS (
# MAGIC   SELECT job_id, run_id, MAX(result_state) AS result_state, MIN(period_start_time) AS run_start
# MAGIC   FROM system.lakeflow.job_run_timeline
# MAGIC   WHERE period_start_time >= current_timestamp() - INTERVAL 14 DAYS
# MAGIC   GROUP BY job_id, run_id
# MAGIC )
# MAGIC SELECT
# MAGIC   job_id,
# MAGIC   COUNT(DISTINCT run_id) AS total_runs,
# MAGIC   SUM(CASE WHEN result_state = 'FAILED' THEN 1 ELSE 0 END) AS failed_runs,
# MAGIC   MIN(run_start) AS first_failure,
# MAGIC   MAX(run_start) AS last_failure
# MAGIC FROM run_outcomes
# MAGIC GROUP BY job_id
# MAGIC HAVING SUM(CASE WHEN result_state = 'SUCCESS' THEN 1 ELSE 0 END) = 0
# MAGIC    AND COUNT(*) >= 1
# MAGIC ORDER BY total_runs DESC;

# COMMAND ----------

# DBTITLE 1,Lineage: Most Readers
# MAGIC %sql
# MAGIC SELECT
# MAGIC   source_table_full_name,
# MAGIC   COUNT(DISTINCT entity_id) AS reader_count,
# MAGIC   COUNT(DISTINCT created_by) AS distinct_users,
# MAGIC   COLLECT_SET(entity_type) AS entity_types
# MAGIC FROM system.access.table_lineage
# MAGIC WHERE source_table_full_name IS NOT NULL
# MAGIC   AND event_date >= current_date() - INTERVAL 90 DAYS
# MAGIC GROUP BY source_table_full_name
# MAGIC ORDER BY reader_count DESC
# MAGIC LIMIT 50;

# COMMAND ----------

# DBTITLE 1,Lineage: Most Writers
# MAGIC %sql
# MAGIC SELECT
# MAGIC   target_table_full_name,
# MAGIC   COUNT(DISTINCT entity_id) AS writer_count,
# MAGIC   COUNT(DISTINCT created_by) AS distinct_users,
# MAGIC   COLLECT_SET(entity_type) AS entity_types
# MAGIC FROM system.access.table_lineage
# MAGIC WHERE target_table_full_name IS NOT NULL
# MAGIC   AND event_date >= current_date() - INTERVAL 90 DAYS
# MAGIC GROUP BY target_table_full_name
# MAGIC ORDER BY writer_count DESC
# MAGIC LIMIT 50;

# COMMAND ----------

# DBTITLE 1,Most Data Scanned
# MAGIC %sql
# MAGIC SELECT
# MAGIC   statement_id,
# MAGIC   start_time,
# MAGIC   executed_by,
# MAGIC   read_bytes / 1024 / 1024 / 1024 AS read_gb,
# MAGIC   read_rows,
# MAGIC   read_files,
# MAGIC   pruned_files,
# MAGIC   total_duration_ms / 1000.0 AS duration_sec,
# MAGIC   LEFT(statement_text, 120) AS statement_preview
# MAGIC FROM system.query.history
# MAGIC WHERE execution_status = 'FINISHED'
# MAGIC   AND start_time >= current_timestamp() - INTERVAL 7 DAYS
# MAGIC   AND read_bytes > 0
# MAGIC ORDER BY read_bytes DESC
# MAGIC LIMIT 50;

# COMMAND ----------

# DBTITLE 1,Peak Concurrency
# MAGIC %sql
# MAGIC WITH buckets AS (
# MAGIC   SELECT
# MAGIC     statement_id,
# MAGIC     start_time,
# MAGIC     end_time,
# MAGIC     DATE_TRUNC('hour', start_time) AS bucket_start
# MAGIC   FROM system.query.history
# MAGIC   WHERE start_time >= current_timestamp() - INTERVAL 7 DAYS
# MAGIC     AND end_time IS NOT NULL
# MAGIC )
# MAGIC SELECT
# MAGIC   bucket_start AS hour_utc,
# MAGIC   COUNT(*) AS queries_started_in_hour
# MAGIC FROM buckets
# MAGIC GROUP BY bucket_start
# MAGIC ORDER BY queries_started_in_hour DESC
# MAGIC LIMIT 50;

# COMMAND ----------

# DBTITLE 1,Permission Errors
# MAGIC %sql
# MAGIC SELECT
# MAGIC   start_time,
# MAGIC   executed_by,
# MAGIC   executed_as,
# MAGIC   LEFT(error_message, 300) AS error_message,
# MAGIC   LEFT(statement_text, 150) AS statement_preview
# MAGIC FROM system.query.history
# MAGIC WHERE execution_status = 'FAILED'
# MAGIC   AND start_time >= current_timestamp() - INTERVAL 14 DAYS
# MAGIC   AND (
# MAGIC     LOWER(error_message) LIKE '%insufficient%privilege%'
# MAGIC     OR LOWER(error_message) LIKE '%permission%denied%'
# MAGIC     OR LOWER(error_message) LIKE '%access%denied%'
# MAGIC     OR LOWER(error_message) LIKE '%cannot resolve%'
# MAGIC   )
# MAGIC ORDER BY start_time DESC
# MAGIC LIMIT 100;

# COMMAND ----------

# DBTITLE 1,Queries by Hour
# MAGIC %sql
# MAGIC SELECT
# MAGIC   HOUR(start_time) AS hour_utc,
# MAGIC   COUNT(*) AS query_count,
# MAGIC   SUM(CASE WHEN execution_status = 'FINISHED' THEN 1 ELSE 0 END) AS finished
# MAGIC FROM system.query.history
# MAGIC WHERE start_time >= current_timestamp() - INTERVAL 14 DAYS
# MAGIC GROUP BY HOUR(start_time)
# MAGIC ORDER BY hour_utc;

# COMMAND ----------

# DBTITLE 1,Queries from Jobs
# MAGIC %sql
# MAGIC SELECT
# MAGIC   query_source.job_info.job_id AS job_id,
# MAGIC   COUNT(*) AS query_count,
# MAGIC   SUM(total_duration_ms) / 1000.0 AS total_duration_sec
# MAGIC FROM system.query.history
# MAGIC WHERE start_time >= current_timestamp() - INTERVAL 7 DAYS
# MAGIC   AND query_source.job_info.job_id IS NOT NULL
# MAGIC GROUP BY query_source.job_info.job_id
# MAGIC ORDER BY query_count DESC
# MAGIC LIMIT 50;

# COMMAND ----------

# DBTITLE 1,Queries per User
# MAGIC %sql
# MAGIC SELECT
# MAGIC   executed_by,
# MAGIC   COUNT(*) AS query_count,
# MAGIC   COUNT(DISTINCT DATE(start_time)) AS active_days,
# MAGIC   SUM(CASE WHEN execution_status = 'FINISHED' THEN 1 ELSE 0 END) AS finished,
# MAGIC   SUM(CASE WHEN execution_status = 'FAILED' THEN 1 ELSE 0 END) AS failed
# MAGIC FROM system.query.history
# MAGIC WHERE start_time >= current_timestamp() - INTERVAL 7 DAYS
# MAGIC GROUP BY executed_by
# MAGIC ORDER BY query_count DESC
# MAGIC LIMIT 50;

# COMMAND ----------

# DBTITLE 1,Queue Time
# MAGIC %sql
# MAGIC SELECT
# MAGIC   statement_id,
# MAGIC   start_time,
# MAGIC   executed_by,
# MAGIC   waiting_at_capacity_duration_ms / 1000.0 AS queue_sec,
# MAGIC   waiting_for_compute_duration_ms / 1000.0 AS provision_sec,
# MAGIC   total_duration_ms / 1000.0 AS total_sec,
# MAGIC   compute.warehouse_id,
# MAGIC   LEFT(statement_text, 100) AS statement_preview
# MAGIC FROM system.query.history
# MAGIC WHERE start_time >= current_timestamp() - INTERVAL 7 DAYS
# MAGIC   AND waiting_at_capacity_duration_ms > 0
# MAGIC ORDER BY waiting_at_capacity_duration_ms DESC
# MAGIC LIMIT 100;

# COMMAND ----------

# DBTITLE 1,Result Fetch Time
# MAGIC %sql
# MAGIC SELECT
# MAGIC   statement_id,
# MAGIC   start_time,
# MAGIC   executed_by,
# MAGIC   result_fetch_duration_ms / 1000.0 AS result_fetch_sec,
# MAGIC   total_duration_ms / 1000.0 AS total_sec,
# MAGIC   produced_rows,
# MAGIC   ROUND(100.0 * result_fetch_duration_ms / NULLIF(total_duration_ms + result_fetch_duration_ms, 0), 1) AS fetch_pct
# MAGIC FROM system.query.history
# MAGIC WHERE execution_status = 'FINISHED'
# MAGIC   AND start_time >= current_timestamp() - INTERVAL 7 DAYS
# MAGIC   AND result_fetch_duration_ms > 1000
# MAGIC ORDER BY result_fetch_duration_ms DESC
# MAGIC LIMIT 50;

# COMMAND ----------

# DBTITLE 1,Run As vs Executed By
# MAGIC %sql
# MAGIC SELECT
# MAGIC   executed_by AS user_who_triggered,
# MAGIC   executed_as AS identity_used_for_privileges,
# MAGIC   COUNT(*) AS query_count
# MAGIC FROM system.query.history
# MAGIC WHERE start_time >= current_timestamp() - INTERVAL 14 DAYS
# MAGIC   AND executed_by IS NOT NULL
# MAGIC   AND executed_as IS NOT NULL
# MAGIC   AND executed_by != executed_as
# MAGIC GROUP BY executed_by, executed_as
# MAGIC ORDER BY query_count DESC;

# COMMAND ----------

# DBTITLE 1,Slowest Queries
# MAGIC %sql
# MAGIC SELECT
# MAGIC   statement_id,
# MAGIC   start_time,
# MAGIC   executed_by,
# MAGIC   total_duration_ms / 1000.0 AS duration_sec,
# MAGIC   execution_duration_ms / 1000.0 AS execution_sec,
# MAGIC   waiting_for_compute_duration_ms / 1000.0 AS wait_compute_sec,
# MAGIC   waiting_at_capacity_duration_ms / 1000.0 AS wait_capacity_sec,
# MAGIC   read_bytes / 1024 / 1024 / 1024 AS read_gb,
# MAGIC   produced_rows,
# MAGIC   LEFT(statement_text, 150) AS statement_preview
# MAGIC FROM system.query.history
# MAGIC WHERE execution_status = 'FINISHED'
# MAGIC   AND start_time >= current_timestamp() - INTERVAL 7 DAYS
# MAGIC ORDER BY total_duration_ms DESC
# MAGIC LIMIT 50;

# COMMAND ----------

# DBTITLE 1,Stale Tables (Last Written)
# MAGIC %sql
# MAGIC WITH last_write AS (
# MAGIC   SELECT
# MAGIC     target_table_full_name,
# MAGIC     MAX(event_time) AS last_write_time
# MAGIC   FROM system.access.table_lineage
# MAGIC   WHERE target_table_full_name IS NOT NULL
# MAGIC     AND event_date >= current_date() - INTERVAL 365 DAYS
# MAGIC   GROUP BY target_table_full_name
# MAGIC )
# MAGIC SELECT
# MAGIC   target_table_full_name,
# MAGIC   last_write_time,
# MAGIC   DATEDIFF(current_timestamp(), last_write_time) AS days_since_write
# MAGIC FROM last_write
# MAGIC WHERE last_write_time < current_timestamp() - INTERVAL 90 DAYS
# MAGIC ORDER BY last_write_time ASC;

# COMMAND ----------

# DBTITLE 1,Table Not Found Errors
# MAGIC %sql
# MAGIC SELECT
# MAGIC   start_time,
# MAGIC   executed_by,
# MAGIC   LEFT(error_message, 400) AS error_message,
# MAGIC   LEFT(statement_text, 200) AS statement_preview
# MAGIC FROM system.query.history
# MAGIC WHERE execution_status = 'FAILED'
# MAGIC   AND start_time >= current_timestamp() - INTERVAL 14 DAYS
# MAGIC   AND (
# MAGIC     LOWER(error_message) LIKE '%table%not found%'
# MAGIC     OR LOWER(error_message) LIKE '%cannot find%table%'
# MAGIC     OR LOWER(error_message) LIKE '%schema%not found%'
# MAGIC     OR LOWER(error_message) LIKE '%path does not exist%'
# MAGIC   )
# MAGIC ORDER BY start_time DESC
# MAGIC LIMIT 50;

# COMMAND ----------

# DBTITLE 1,Warehouse Utilization
# MAGIC %sql
# MAGIC SELECT
# MAGIC   compute.warehouse_id AS warehouse_id,
# MAGIC   DATE_TRUNC('hour', start_time) AS hour_utc,
# MAGIC   COUNT(*) AS query_count,
# MAGIC   SUM(total_duration_ms) / 1000.0 AS total_duration_sec
# MAGIC FROM system.query.history
# MAGIC WHERE start_time >= current_timestamp() - INTERVAL 7 DAYS
# MAGIC   AND compute.warehouse_id IS NOT NULL
# MAGIC GROUP BY compute.warehouse_id, DATE_TRUNC('hour', start_time)
# MAGIC ORDER BY hour_utc DESC, query_count DESC;

# COMMAND ----------

# DBTITLE 1,MASTER Q1: Total Spend This Month
# MAGIC %sql
# MAGIC -- MASTER Q1: What is our total estimated spend this month?
# MAGIC WITH usage_net AS (
# MAGIC   SELECT
# MAGIC     billing_origin_product AS product,
# MAGIC     sku_name,
# MAGIC     usage_unit,
# MAGIC     SUM(usage_quantity) AS usage_quantity
# MAGIC   FROM system.billing.usage
# MAGIC   WHERE record_type = 'ORIGINAL'
# MAGIC     AND usage_date >= DATE_TRUNC('month', current_date())
# MAGIC   GROUP BY billing_origin_product, sku_name, usage_unit
# MAGIC ),
# MAGIC prices AS (
# MAGIC   SELECT sku_name, usage_unit,
# MAGIC     CAST(COALESCE(pricing.effective_list.default, pricing.default) AS DOUBLE) AS unit_price
# MAGIC   FROM system.billing.list_prices
# MAGIC   WHERE price_end_time > current_timestamp()
# MAGIC   QUALIFY ROW_NUMBER() OVER (PARTITION BY sku_name, usage_unit ORDER BY price_start_time DESC) = 1
# MAGIC )
# MAGIC SELECT
# MAGIC   'TOTAL' AS metric,
# MAGIC   NULL AS product,
# MAGIC   ROUND(SUM(u.usage_quantity * COALESCE(p.unit_price, 0)), 2) AS estimated_cost_usd
# MAGIC FROM usage_net u
# MAGIC LEFT JOIN prices p ON u.sku_name = p.sku_name AND u.usage_unit = p.usage_unit
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC   'BY_PRODUCT' AS metric,
# MAGIC   u.product,
# MAGIC   ROUND(SUM(u.usage_quantity * COALESCE(p.unit_price, 0)), 2) AS estimated_cost_usd
# MAGIC FROM usage_net u
# MAGIC LEFT JOIN prices p ON u.sku_name = p.sku_name AND u.usage_unit = p.usage_unit
# MAGIC GROUP BY u.product
# MAGIC ORDER BY metric, estimated_cost_usd DESC NULLS LAST;

# COMMAND ----------

# DBTITLE 1,MASTER Q2: Month Over Month Cost
# MAGIC %sql
# MAGIC -- MASTER Q2: How does this month's spend compare to last month?
# MAGIC WITH monthly_usage AS (
# MAGIC   SELECT
# MAGIC     DATE_TRUNC('month', usage_date) AS month_start,
# MAGIC     billing_origin_product AS product,
# MAGIC     sku_name,
# MAGIC     usage_unit,
# MAGIC     SUM(usage_quantity) AS usage_quantity
# MAGIC   FROM system.billing.usage
# MAGIC   WHERE record_type = 'ORIGINAL'
# MAGIC     AND usage_date >= DATE_TRUNC('month', current_date()) - INTERVAL 2 MONTHS
# MAGIC   GROUP BY DATE_TRUNC('month', usage_date), billing_origin_product, sku_name, usage_unit
# MAGIC ),
# MAGIC prices AS (
# MAGIC   SELECT sku_name, usage_unit,
# MAGIC     CAST(COALESCE(pricing.effective_list.default, pricing.default) AS DOUBLE) AS unit_price
# MAGIC   FROM system.billing.list_prices
# MAGIC   WHERE price_end_time > current_timestamp()
# MAGIC   QUALIFY ROW_NUMBER() OVER (PARTITION BY sku_name, usage_unit ORDER BY price_start_time DESC) = 1
# MAGIC )
# MAGIC SELECT
# MAGIC   m.month_start,
# MAGIC   m.product,
# MAGIC   SUM(m.usage_quantity) AS usage_quantity,
# MAGIC   ROUND(SUM(m.usage_quantity * COALESCE(p.unit_price, 0)), 2) AS estimated_cost_usd
# MAGIC FROM monthly_usage m
# MAGIC LEFT JOIN prices p ON m.sku_name = p.sku_name AND m.usage_unit = p.usage_unit
# MAGIC GROUP BY m.month_start, m.product
# MAGIC ORDER BY m.month_start DESC, estimated_cost_usd DESC;

# COMMAND ----------

# DBTITLE 1,MASTER Q3: Idle Warehouses
# MAGIC %sql
# MAGIC -- MASTER Q3: Which SQL warehouses are idle or low-utilization?
# MAGIC SELECT
# MAGIC   compute.warehouse_id AS warehouse_id,
# MAGIC   COUNT(*) AS query_count_7d,
# MAGIC   SUM(total_duration_ms) / 1000.0 AS total_duration_sec
# MAGIC FROM system.query.history
# MAGIC WHERE start_time >= current_timestamp() - INTERVAL 7 DAYS
# MAGIC   AND compute.warehouse_id IS NOT NULL
# MAGIC GROUP BY compute.warehouse_id
# MAGIC HAVING COUNT(*) < 10
# MAGIC ORDER BY query_count_7d ASC;

# COMMAND ----------

# DBTITLE 1,MASTER Q4: Queries Returning Zero Rows
# MAGIC %sql
# MAGIC -- MASTER Q4: Which queries returned zero rows? (Potential waste or misconfiguration)
# MAGIC SELECT
# MAGIC   statement_id,
# MAGIC   start_time,
# MAGIC   executed_by,
# MAGIC   total_duration_ms / 1000.0 AS duration_sec,
# MAGIC   read_bytes / 1024 / 1024 AS read_mb,
# MAGIC   LEFT(statement_text, 150) AS statement_preview
# MAGIC FROM system.query.history
# MAGIC WHERE execution_status = 'FINISHED'
# MAGIC   AND start_time >= current_timestamp() - INTERVAL 7 DAYS
# MAGIC   AND produced_rows = 0
# MAGIC   AND read_bytes > 0
# MAGIC ORDER BY read_bytes DESC
# MAGIC LIMIT 100;

# COMMAND ----------

# DBTITLE 1,MASTER Q5: Daily Failure Rate Trend
# MAGIC %sql
# MAGIC -- MASTER Q5: What is our daily query failure rate trend?
# MAGIC SELECT
# MAGIC   DATE(start_time) AS query_date,
# MAGIC   COUNT(*) AS total_queries,
# MAGIC   SUM(CASE WHEN execution_status = 'FAILED' THEN 1 ELSE 0 END) AS failures,
# MAGIC   ROUND(100.0 * SUM(CASE WHEN execution_status = 'FAILED' THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2) AS failure_rate_pct
# MAGIC FROM system.query.history
# MAGIC WHERE start_time >= current_timestamp() - INTERVAL 14 DAYS
# MAGIC GROUP BY DATE(start_time)
# MAGIC ORDER BY query_date DESC;

# COMMAND ----------

# DBTITLE 1,MASTER Q6: Jobs Not Run in 30 Days
# MAGIC %sql
# MAGIC -- MASTER Q6: Which jobs haven't run in the last 30 days?
# MAGIC WITH last_run AS (
# MAGIC   SELECT
# MAGIC     job_id,
# MAGIC     MAX(period_start_time) AS last_run_time
# MAGIC   FROM system.lakeflow.job_run_timeline
# MAGIC   WHERE period_start_time >= current_timestamp() - INTERVAL 365 DAYS
# MAGIC   GROUP BY job_id
# MAGIC )
# MAGIC SELECT
# MAGIC   job_id,
# MAGIC   last_run_time,
# MAGIC   DATEDIFF(current_timestamp(), last_run_time) AS days_since_run
# MAGIC FROM last_run
# MAGIC WHERE last_run_time < current_timestamp() - INTERVAL 30 DAYS
# MAGIC ORDER BY last_run_time ASC;

# COMMAND ----------

# DBTITLE 1,MASTER Q7: Top Users by Compute Time
# MAGIC %sql
# MAGIC -- MASTER Q7: Who consumes the most compute time (total duration)?
# MAGIC SELECT
# MAGIC   executed_by,
# MAGIC   COUNT(*) AS query_count,
# MAGIC   SUM(total_duration_ms) / 1000.0 / 3600.0 AS total_hours,
# MAGIC   SUM(execution_duration_ms) / 1000.0 / 3600.0 AS execution_hours
# MAGIC FROM system.query.history
# MAGIC WHERE execution_status = 'FINISHED'
# MAGIC   AND start_time >= current_timestamp() - INTERVAL 7 DAYS
# MAGIC GROUP BY executed_by
# MAGIC ORDER BY total_hours DESC
# MAGIC LIMIT 50;

# COMMAND ----------

# DBTITLE 1,MASTER Q8: Alert Failure Rate
# MAGIC %sql
# MAGIC -- MASTER Q8: What is the failure rate of alert-triggered queries?
# MAGIC SELECT
# MAGIC   query_source.alert_id AS alert_id,
# MAGIC   COUNT(*) AS total_runs,
# MAGIC   SUM(CASE WHEN execution_status = 'FAILED' THEN 1 ELSE 0 END) AS failures,
# MAGIC   ROUND(100.0 * SUM(CASE WHEN execution_status = 'FAILED' THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2) AS failure_rate_pct
# MAGIC FROM system.query.history
# MAGIC WHERE start_time >= current_timestamp() - INTERVAL 14 DAYS
# MAGIC   AND query_source.alert_id IS NOT NULL
# MAGIC GROUP BY query_source.alert_id
# MAGIC ORDER BY failure_rate_pct DESC, failures DESC;

# COMMAND ----------

# DBTITLE 1,MASTER Q10: Longest Wait for Compute
# MAGIC %sql
# MAGIC -- MASTER Q9: When do our dashboards generate the most load?
# MAGIC SELECT
# MAGIC   HOUR(start_time) AS hour_utc,
# MAGIC   COUNT(*) AS dashboard_query_count
# MAGIC FROM system.query.history
# MAGIC WHERE start_time >= current_timestamp() - INTERVAL 7 DAYS
# MAGIC   AND query_source.dashboard_id IS NOT NULL
# MAGIC GROUP BY HOUR(start_time)
# MAGIC ORDER BY hour_utc;

# COMMAND ----------

# DBTITLE 1,MASTER Q11: Storage Cost Breakdown
# MAGIC %sql
# MAGIC -- MASTER Q10: Where do we wait longest for compute to be provisioned?
# MAGIC SELECT
# MAGIC   statement_id,
# MAGIC   start_time,
# MAGIC   executed_by,
# MAGIC   waiting_for_compute_duration_ms / 1000.0 AS wait_provision_sec,
# MAGIC   waiting_at_capacity_duration_ms / 1000.0 AS wait_capacity_sec,
# MAGIC   total_duration_ms / 1000.0 AS total_sec,
# MAGIC   compute.warehouse_id
# MAGIC FROM system.query.history
# MAGIC WHERE start_time >= current_timestamp() - INTERVAL 7 DAYS
# MAGIC   AND waiting_for_compute_duration_ms > 0
# MAGIC ORDER BY waiting_for_compute_duration_ms DESC
# MAGIC LIMIT 50;

# COMMAND ----------

# DBTITLE 1,MASTER Q12: Model Serving and AI Cost
# MAGIC %sql
# MAGIC -- MASTER Q11: What is our storage (and default storage) cost?
# MAGIC SELECT
# MAGIC   billing_origin_product AS product,
# MAGIC   sku_name,
# MAGIC   SUM(usage_quantity) AS usage_quantity
# MAGIC FROM system.billing.usage
# MAGIC WHERE record_type = 'ORIGINAL'
# MAGIC   AND usage_date >= current_date() - INTERVAL 30 DAYS
# MAGIC   AND (billing_origin_product = 'DEFAULT_STORAGE' OR LOWER(sku_name) LIKE '%storage%')
# MAGIC GROUP BY billing_origin_product, sku_name
# MAGIC ORDER BY usage_quantity DESC;

# COMMAND ----------

# DBTITLE 1,MASTER Q13: DLT Pipeline Health
# MAGIC %sql
# MAGIC -- MASTER Q12: How much do we spend on model serving and AI Gateway?
# MAGIC SELECT
# MAGIC   billing_origin_product AS product,
# MAGIC   sku_name,
# MAGIC   SUM(usage_quantity) AS usage_quantity
# MAGIC FROM system.billing.usage
# MAGIC WHERE record_type = 'ORIGINAL'
# MAGIC   AND usage_date >= current_date() - INTERVAL 30 DAYS
# MAGIC   AND billing_origin_product IN ('MODEL_SERVING', 'AI_GATEWAY', 'AI_FUNCTIONS', 'VECTOR_SEARCH')
# MAGIC GROUP BY billing_origin_product, sku_name
# MAGIC ORDER BY usage_quantity DESC;

# COMMAND ----------

# DBTITLE 1,MASTER Q14: Query Duration Percentiles
# MAGIC %sql
# MAGIC -- MASTER Q13: What is the success/failure rate of DLT pipeline updates?
# MAGIC SELECT
# MAGIC   pipeline_id,
# MAGIC   COUNT(DISTINCT update_id) AS update_count,
# MAGIC   SUM(CASE WHEN result_state = 'SUCCESS' THEN 1 ELSE 0 END) AS success_count,
# MAGIC   SUM(CASE WHEN result_state = 'FAILED' THEN 1 ELSE 0 END) AS failed_count,
# MAGIC   ROUND(100.0 * SUM(CASE WHEN result_state = 'FAILED' THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2) AS failure_rate_pct
# MAGIC FROM system.lakeflow.pipeline_update_timeline
# MAGIC WHERE period_start_time >= current_timestamp() - INTERVAL 14 DAYS
# MAGIC   AND result_state IS NOT NULL
# MAGIC GROUP BY pipeline_id
# MAGIC ORDER BY failed_count DESC, update_count DESC
# MAGIC LIMIT 50;

# COMMAND ----------

# DBTITLE 1,MASTER Q15: Concurrency by Warehouse
# MAGIC %sql
# MAGIC -- MASTER Q14: What are our query duration percentiles (p50, p90, p99)?
# MAGIC SELECT
# MAGIC   APPROX_PERCENTILE(total_duration_ms / 1000.0, 0.5) AS p50_duration_sec,
# MAGIC   APPROX_PERCENTILE(total_duration_ms / 1000.0, 0.9) AS p90_duration_sec,
# MAGIC   APPROX_PERCENTILE(total_duration_ms / 1000.0, 0.99) AS p99_duration_sec,
# MAGIC   AVG(total_duration_ms / 1000.0) AS avg_duration_sec,
# MAGIC   COUNT(*) AS query_count
# MAGIC FROM system.query.history
# MAGIC WHERE execution_status = 'FINISHED'
# MAGIC   AND start_time >= current_timestamp() - INTERVAL 7 DAYS;

# COMMAND ----------

# DBTITLE 1,MASTER Q16: Duplicate Query Patterns
# MAGIC %sql
# MAGIC -- MASTER Q15: What is query concurrency by warehouse?
# MAGIC SELECT
# MAGIC   compute.warehouse_id AS warehouse_id,
# MAGIC   DATE_TRUNC('hour', start_time) AS hour_utc,
# MAGIC   COUNT(*) AS queries_in_hour
# MAGIC FROM system.query.history
# MAGIC WHERE start_time >= current_timestamp() - INTERVAL 7 DAYS
# MAGIC   AND compute.warehouse_id IS NOT NULL
# MAGIC GROUP BY compute.warehouse_id, DATE_TRUNC('hour', start_time)
# MAGIC ORDER BY queries_in_hour DESC
# MAGIC LIMIT 100;

# COMMAND ----------

# DBTITLE 1,MASTER Q17: Queries Using Query Tags
# MAGIC %sql
# MAGIC -- MASTER Q16: Are we running the same query repeatedly? (Duplicate patterns)
# MAGIC SELECT
# MAGIC   LEFT(statement_text, 80) AS query_fingerprint,
# MAGIC   COUNT(*) AS run_count,
# MAGIC   COUNT(DISTINCT executed_by) AS distinct_users
# MAGIC FROM system.query.history
# MAGIC WHERE execution_status = 'FINISHED'
# MAGIC   AND start_time >= current_timestamp() - INTERVAL 7 DAYS
# MAGIC   AND statement_text IS NOT NULL
# MAGIC   AND LENGTH(statement_text) > 10
# MAGIC GROUP BY LEFT(statement_text, 80)
# MAGIC HAVING COUNT(*) >= 5
# MAGIC ORDER BY run_count DESC
# MAGIC LIMIT 50;

# COMMAND ----------

# DBTITLE 1,MASTER Q18: Job Run Duration Percentiles
# MAGIC %sql
# MAGIC -- MASTER Q17: Who uses query tags (for cost attribution)?
# MAGIC SELECT
# MAGIC   executed_by,
# MAGIC   COUNT(*) AS tagged_query_count
# MAGIC FROM system.query.history
# MAGIC WHERE start_time >= current_timestamp() - INTERVAL 7 DAYS
# MAGIC   AND query_tags IS NOT NULL
# MAGIC   AND size(query_tags) > 0
# MAGIC GROUP BY executed_by
# MAGIC ORDER BY tagged_query_count DESC
# MAGIC LIMIT 30;

# COMMAND ----------

# DBTITLE 1,MASTER Q19: Tables with No Downstream
# MAGIC %sql
# MAGIC -- MASTER Q18: What are job run duration percentiles by job?
# MAGIC WITH run_durations AS (
# MAGIC   SELECT
# MAGIC     job_id,
# MAGIC     run_id,
# MAGIC     CAST(SUM(UNIX_TIMESTAMP(period_end_time) - UNIX_TIMESTAMP(period_start_time)) AS DOUBLE) AS duration_sec
# MAGIC   FROM system.lakeflow.job_run_timeline
# MAGIC   WHERE period_start_time >= current_timestamp() - INTERVAL 30 DAYS
# MAGIC   GROUP BY job_id, run_id
# MAGIC   HAVING MAX(result_state) = 'SUCCESS'
# MAGIC )
# MAGIC SELECT
# MAGIC   job_id,
# MAGIC   COUNT(*) AS run_count,
# MAGIC   ROUND(APPROX_PERCENTILE(duration_sec, 0.5), 1) AS p50_sec,
# MAGIC   ROUND(APPROX_PERCENTILE(duration_sec, 0.9), 1) AS p90_sec,
# MAGIC   ROUND(AVG(duration_sec), 1) AS avg_sec
# MAGIC FROM run_durations
# MAGIC GROUP BY job_id
# MAGIC HAVING COUNT(*) >= 5
# MAGIC ORDER BY p90_sec DESC
# MAGIC LIMIT 50;

# COMMAND ----------

# DBTITLE 1,MASTER Q20: Lineage Events by Entity Type
# MAGIC %sql
# MAGIC -- MASTER Q19: Which tables have no downstream consumers (leaf tables)?
# MAGIC WITH written AS (
# MAGIC   SELECT DISTINCT target_table_full_name AS full_name
# MAGIC   FROM system.access.table_lineage
# MAGIC   WHERE target_table_full_name IS NOT NULL
# MAGIC     AND event_date >= current_date() - INTERVAL 90 DAYS
# MAGIC ),
# MAGIC read_as_source AS (
# MAGIC   SELECT DISTINCT source_table_full_name AS full_name
# MAGIC   FROM system.access.table_lineage
# MAGIC   WHERE source_table_full_name IS NOT NULL
# MAGIC     AND event_date >= current_date() - INTERVAL 90 DAYS
# MAGIC )
# MAGIC SELECT w.full_name AS table_full_name, 'no_downstream_reads_90d' AS reason
# MAGIC FROM written w
# MAGIC LEFT JOIN read_as_source r ON w.full_name = r.full_name
# MAGIC WHERE r.full_name IS NULL
# MAGIC ORDER BY w.full_name
# MAGIC LIMIT 200;

# COMMAND ----------

# DBTITLE 1,MASTER Q21: Warehouse Events
# MAGIC %sql
# MAGIC -- MASTER Q20: What generates the most lineage (reads/writes) by entity type?
# MAGIC SELECT
# MAGIC   entity_type,
# MAGIC   COUNT(*) AS lineage_event_count
# MAGIC FROM system.access.table_lineage
# MAGIC WHERE event_date >= current_date() - INTERVAL 30 DAYS
# MAGIC GROUP BY entity_type
# MAGIC ORDER BY lineage_event_count DESC;

# COMMAND ----------

# DBTITLE 1,MASTER Q22: Most Read Bytes by Table
# MAGIC %sql
# MAGIC -- MASTER Q21: When do warehouses start and stop? (SQL warehouse events)
# MAGIC SELECT
# MAGIC   warehouse_id,
# MAGIC   event_type,
# MAGIC   event_time,
# MAGIC   cluster_count
# MAGIC FROM system.compute.warehouse_events
# MAGIC WHERE event_time >= current_timestamp() - INTERVAL 7 DAYS
# MAGIC ORDER BY event_time DESC
# MAGIC LIMIT 200;

# COMMAND ----------

# DBTITLE 1,MASTER Q23: Canceled Queries
# MAGIC %sql
# MAGIC -- MASTER Q22: Which tables are read the most (by lineage source)?
# MAGIC SELECT
# MAGIC   source_table_full_name,
# MAGIC   COUNT(*) AS read_event_count
# MAGIC FROM system.access.table_lineage
# MAGIC WHERE source_table_full_name IS NOT NULL
# MAGIC   AND event_date >= current_date() - INTERVAL 30 DAYS
# MAGIC GROUP BY source_table_full_name
# MAGIC ORDER BY read_event_count DESC
# MAGIC LIMIT 50;

# COMMAND ----------

# DBTITLE 1,MASTER Q24: Cost per Workspace
# MAGIC %sql
# MAGIC -- MASTER Q23: Who is canceling queries and why?
# MAGIC SELECT
# MAGIC   statement_id,
# MAGIC   start_time,
# MAGIC   executed_by,
# MAGIC   total_duration_ms / 1000.0 AS duration_before_cancel_sec,
# MAGIC   LEFT(statement_text, 120) AS statement_preview
# MAGIC FROM system.query.history
# MAGIC WHERE execution_status = 'CANCELED'
# MAGIC   AND start_time >= current_timestamp() - INTERVAL 7 DAYS
# MAGIC ORDER BY start_time DESC
# MAGIC LIMIT 100;

# COMMAND ----------

# DBTITLE 1,MASTER Q25: Slow Compilation Queries
# MAGIC %sql
# MAGIC -- MASTER Q24: What is cost per workspace? (Multi-workspace accounts)
# MAGIC SELECT
# MAGIC   workspace_id,
# MAGIC   billing_origin_product AS product,
# MAGIC   SUM(usage_quantity) AS usage_quantity
# MAGIC FROM system.billing.usage
# MAGIC WHERE record_type = 'ORIGINAL'
# MAGIC   AND usage_date >= current_date() - INTERVAL 30 DAYS
# MAGIC GROUP BY workspace_id, billing_origin_product
# MAGIC ORDER BY usage_quantity DESC
# MAGIC LIMIT 100;

# COMMAND ----------

# DBTITLE 1,MASTER Q26: Client Application Breakdown
# MAGIC %sql
# MAGIC -- MASTER Q25: Which queries spend the most time in compilation?
# MAGIC SELECT
# MAGIC   statement_id,
# MAGIC   start_time,
# MAGIC   executed_by,
# MAGIC   compilation_duration_ms / 1000.0 AS compile_sec,
# MAGIC   execution_duration_ms / 1000.0 AS execution_sec,
# MAGIC   ROUND(100.0 * compilation_duration_ms / NULLIF(total_duration_ms, 0), 1) AS compile_pct,
# MAGIC   LEFT(statement_text, 100) AS statement_preview
# MAGIC FROM system.query.history
# MAGIC WHERE execution_status = 'FINISHED'
# MAGIC   AND start_time >= current_timestamp() - INTERVAL 7 DAYS
# MAGIC   AND compilation_duration_ms > 5000
# MAGIC ORDER BY compilation_duration_ms DESC
# MAGIC LIMIT 50;

# COMMAND ----------

# DBTITLE 1,MASTER Q27: Retried Job Runs
# MAGIC %sql
# MAGIC -- MASTER Q26: What client applications run queries? (SQL Editor, Tableau, JDBC, etc.)
# MAGIC SELECT
# MAGIC   COALESCE(client_application, 'unknown') AS client_application,
# MAGIC   COUNT(*) AS query_count
# MAGIC FROM system.query.history
# MAGIC WHERE start_time >= current_timestamp() - INTERVAL 7 DAYS
# MAGIC GROUP BY client_application
# MAGIC ORDER BY query_count DESC
# MAGIC LIMIT 30;

# COMMAND ----------

# DBTITLE 1,MASTER Q28: Most Written Tables (Lineage)
# MAGIC %sql
# MAGIC -- MASTER Q27: Which job runs were retried?
# MAGIC WITH run_slices AS (
# MAGIC   SELECT workspace_id, job_id, run_id, result_state,
# MAGIC     COUNT(*) AS slice_count
# MAGIC   FROM system.lakeflow.job_run_timeline
# MAGIC   WHERE period_start_time >= current_timestamp() - INTERVAL 14 DAYS
# MAGIC     AND result_state IS NOT NULL
# MAGIC   GROUP BY workspace_id, job_id, run_id, result_state
# MAGIC ),
# MAGIC retried AS (
# MAGIC   SELECT job_id, run_id, SUM(CASE WHEN result_state = 'FAILED' THEN 1 ELSE 0 END) AS fail_slices
# MAGIC   FROM run_slices
# MAGIC   GROUP BY job_id, run_id
# MAGIC   HAVING fail_slices > 0
# MAGIC )
# MAGIC SELECT job_id, run_id, fail_slices AS retry_or_failure_count
# MAGIC FROM retried
# MAGIC ORDER BY fail_slices DESC
# MAGIC LIMIT 50;

# COMMAND ----------

# DBTITLE 1,MASTER Q29: Timeout or Long Running
# MAGIC %sql
# MAGIC -- MASTER Q28: Which tables are written to most often (by lineage)?
# MAGIC SELECT
# MAGIC   target_table_full_name,
# MAGIC   COUNT(*) AS write_event_count
# MAGIC FROM system.access.table_lineage
# MAGIC WHERE target_table_full_name IS NOT NULL
# MAGIC   AND event_date >= current_date() - INTERVAL 30 DAYS
# MAGIC GROUP BY target_table_full_name
# MAGIC ORDER BY write_event_count DESC
# MAGIC LIMIT 50;

# COMMAND ----------

# DBTITLE 1,MASTER Q30: Top Expensive SKUs
# MAGIC %sql
# MAGIC -- MASTER Q29: Queries that ran > 30 minutes (potential runaways)
# MAGIC SELECT
# MAGIC   statement_id,
# MAGIC   start_time,
# MAGIC   executed_by,
# MAGIC   total_duration_ms / 1000.0 / 60.0 AS duration_min,
# MAGIC   read_bytes / 1024 / 1024 / 1024 AS read_gb,
# MAGIC   LEFT(statement_text, 120) AS statement_preview
# MAGIC FROM system.query.history
# MAGIC WHERE execution_status IN ('FINISHED', 'CANCELED', 'FAILED')
# MAGIC   AND start_time >= current_timestamp() - INTERVAL 7 DAYS
# MAGIC   AND total_duration_ms > 1800000
# MAGIC ORDER BY total_duration_ms DESC
# MAGIC LIMIT 50;

# COMMAND ----------

# DBTITLE 1,MASTER Q31: Queries from Notebooks
# MAGIC %sql
# MAGIC -- MASTER Q30: Which SKUs drive the most spend? (Top by estimated cost)
# MAGIC WITH usage_net AS (
# MAGIC   SELECT sku_name, usage_unit, SUM(usage_quantity) AS usage_quantity
# MAGIC   FROM system.billing.usage
# MAGIC   WHERE record_type = 'ORIGINAL'
# MAGIC     AND usage_date >= current_date() - INTERVAL 30 DAYS
# MAGIC   GROUP BY sku_name, usage_unit
# MAGIC ),
# MAGIC prices AS (
# MAGIC   SELECT sku_name, usage_unit,
# MAGIC     CAST(COALESCE(pricing.effective_list.default, pricing.default) AS DOUBLE) AS unit_price
# MAGIC   FROM system.billing.list_prices
# MAGIC   WHERE price_end_time > current_timestamp()
# MAGIC   QUALIFY ROW_NUMBER() OVER (PARTITION BY sku_name, usage_unit ORDER BY price_start_time DESC) = 1
# MAGIC )
# MAGIC SELECT
# MAGIC   u.sku_name,
# MAGIC   u.usage_quantity,
# MAGIC   ROUND(u.usage_quantity * COALESCE(p.unit_price, 0), 2) AS estimated_cost_usd
# MAGIC FROM usage_net u
# MAGIC LEFT JOIN prices p ON u.sku_name = p.sku_name AND u.usage_unit = p.usage_unit
# MAGIC ORDER BY estimated_cost_usd DESC
# MAGIC LIMIT 30;

# COMMAND ----------

# DBTITLE 1,MASTER Q32: Read vs Written Rows
# MAGIC %sql
# MAGIC -- MASTER Q31: How many queries come from notebooks (vs SQL Editor)?
# MAGIC SELECT
# MAGIC   CASE WHEN query_source.notebook_id IS NOT NULL THEN 'notebook' ELSE 'other' END AS source_type,
# MAGIC   COUNT(*) AS query_count
# MAGIC FROM system.query.history
# MAGIC WHERE start_time >= current_timestamp() - INTERVAL 7 DAYS
# MAGIC GROUP BY CASE WHEN query_source.notebook_id IS NOT NULL THEN 'notebook' ELSE 'other' END;

# COMMAND ----------

# DBTITLE 1,MASTER Q33: Spill Volume by User
# MAGIC %sql
# MAGIC -- MASTER Q32: What is our read vs written row volume? (Data movement)
# MAGIC SELECT
# MAGIC   DATE(start_time) AS query_date,
# MAGIC   SUM(read_rows) AS total_read_rows,
# MAGIC   SUM(written_rows) AS total_written_rows,
# MAGIC   SUM(produced_rows) AS total_produced_rows
# MAGIC FROM system.query.history
# MAGIC WHERE start_time >= current_timestamp() - INTERVAL 7 DAYS
# MAGIC   AND execution_status = 'FINISHED'
# MAGIC GROUP BY DATE(start_time)
# MAGIC ORDER BY query_date DESC;

# COMMAND ----------

# DBTITLE 1,MASTER Q34: Result Cache Savings
# MAGIC %sql
# MAGIC -- MASTER Q33: Who is causing the most spill to disk?
# MAGIC SELECT
# MAGIC   executed_by,
# MAGIC   COUNT(*) AS queries_with_spill,
# MAGIC   SUM(spilled_local_bytes) / 1024 / 1024 / 1024 AS total_spill_gb
# MAGIC FROM system.query.history
# MAGIC WHERE start_time >= current_timestamp() - INTERVAL 7 DAYS
# MAGIC   AND execution_status = 'FINISHED'
# MAGIC   AND spilled_local_bytes > 0
# MAGIC GROUP BY executed_by
# MAGIC ORDER BY total_spill_gb DESC
# MAGIC LIMIT 30;

# COMMAND ----------

# DBTITLE 1,MASTER Q35: Jobs by Trigger Type
# MAGIC %sql
# MAGIC -- MASTER Q34: How much compute did we save with result cache?
# MAGIC SELECT
# MAGIC   DATE(start_time) AS query_date,
# MAGIC   COUNT(*) AS total_finished,
# MAGIC   SUM(CASE WHEN from_result_cache = TRUE THEN 1 ELSE 0 END) AS cache_hits,
# MAGIC   ROUND(100.0 * SUM(CASE WHEN from_result_cache = TRUE THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2) AS cache_hit_pct
# MAGIC FROM system.query.history
# MAGIC WHERE execution_status = 'FINISHED'
# MAGIC   AND start_time >= current_timestamp() - INTERVAL 7 DAYS
# MAGIC GROUP BY DATE(start_time)
# MAGIC ORDER BY query_date DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ========== master_31_queries_from_notebooks.sql ==========
# MAGIC -- MASTER Q31: How many queries come from notebooks (vs SQL Editor)?
# MAGIC SELECT
# MAGIC   CASE WHEN query_source.notebook_id IS NOT NULL THEN 'notebook' ELSE 'other' END AS source_type,
# MAGIC   COUNT(*) AS query_count
# MAGIC FROM system.query.history
# MAGIC WHERE start_time >= current_timestamp() - INTERVAL 7 DAYS
# MAGIC GROUP BY CASE WHEN query_source.notebook_id IS NOT NULL THEN 'notebook' ELSE 'other' END;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC - ========== master_30_top_expensive_skus.sql ==========
# MAGIC -- MASTER Q30: Which SKUs drive the most spend? (Top by estimated cost)
# MAGIC WITH usage_net AS (
# MAGIC   SELECT sku_name, usage_unit, SUM(usage_quantity) AS usage_quantity
# MAGIC   FROM system.billing.usage
# MAGIC   WHERE record_type = 'ORIGINAL'
# MAGIC     AND usage_date >= current_date() - INTERVAL 30 DAYS
# MAGIC   GROUP BY sku_name, usage_unit
# MAGIC ),
# MAGIC prices AS (
# MAGIC   SELECT sku_name, usage_unit,
# MAGIC     CAST(COALESCE(pricing.effective_list.default, pricing.default) AS DOUBLE) AS unit_price
# MAGIC   FROM system.billing.list_prices
# MAGIC   WHERE price_end_time > current_timestamp()
# MAGIC   QUALIFY ROW_NUMBER() OVER (PARTITION BY sku_name, usage_unit ORDER BY price_start_time DESC) = 1
# MAGIC )
# MAGIC SELECT
# MAGIC   u.sku_name,
# MAGIC   u.usage_quantity,
# MAGIC   ROUND(u.usage_quantity * COALESCE(p.unit_price, 0), 2) AS estimated_cost_usd
# MAGIC FROM usage_net u
# MAGIC LEFT JOIN prices p ON u.sku_name = p.sku_name AND u.usage_unit = p.usage_unit
# MAGIC ORDER BY estimated_cost_usd DESC
# MAGIC LIMIT 30;
# MAGIC
# MAGIC
# MAGIC -- ========== master_32_read_vs_written_rows.sql ==========
# MAGIC -- MASTER Q32: What is our read vs written row volume? (Data movement)
# MAGIC SELECT
# MAGIC   DATE(start_time) AS query_date,
# MAGIC   SUM(read_rows) AS total_read_rows,
# MAGIC   SUM(written_rows) AS total_written_rows,
# MAGIC   SUM(produced_rows) AS total_produced_rows
# MAGIC FROM system.query.history
# MAGIC WHERE start_time >= current_timestamp() - INTERVAL 7 DAYS
# MAGIC   AND execution_status = 'FINISHED'
# MAGIC GROUP BY DATE(start_time)
# MAGIC ORDER BY query_date DESC;
# MAGIC
# MAGIC -- ========== master_33_spill_volume_by_user.sql ==========
# MAGIC -- MASTER Q33: Who is causing the most spill to disk?
# MAGIC SELECT
# MAGIC   executed_by,
# MAGIC   COUNT(*) AS queries_with_spill,
# MAGIC   SUM(spilled_local_bytes) / 1024 / 1024 / 1024 AS total_spill_gb
# MAGIC FROM system.query.history
# MAGIC WHERE start_time >= current_timestamp() - INTERVAL 7 DAYS
# MAGIC   AND execution_status = 'FINISHED'
# MAGIC   AND spilled_local_bytes > 0
# MAGIC GROUP BY executed_by
# MAGIC ORDER BY total_spill_gb DESC
# MAGIC LIMIT 30;
# MAGIC
# MAGIC -- ========== master_34_result_cache_savings.sql ==========
# MAGIC -- MASTER Q34: How much compute did we save with result cache?
# MAGIC SELECT
# MAGIC   DATE(start_time) AS query_date,
# MAGIC   COUNT(*) AS total_finished,
# MAGIC   SUM(CASE WHEN from_result_cache = TRUE THEN 1 ELSE 0 END) AS cache_hits,
# MAGIC   ROUND(100.0 * SUM(CASE WHEN from_result_cache = TRUE THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2) AS cache_hit_pct
# MAGIC FROM system.query.history
# MAGIC WHERE execution_status = 'FINISHED'
# MAGIC   AND start_time >= current_timestamp() - INTERVAL 7 DAYS
# MAGIC GROUP BY DATE(start_time)
# MAGIC ORDER BY query_date DESC;
# MAGIC
# MAGIC -- ========== master_35_jobs_by_trigger_type.sql ==========
# MAGIC -- MASTER Q35: How are jobs triggered? (Manual, cron, pipeline, etc.)
# MAGIC SELECT
# MAGIC   trigger_type,
# MAGIC   COUNT(DISTINCT run_id) AS run_count
# MAGIC FROM system.lakeflow.job_run_timeline
# MAGIC WHERE period_start_time >= current_timestamp() - INTERVAL 14 DAYS
# MAGIC GROUP BY trigger_type
# MAGIC ORDER BY run_count DESC;
# MAGIC
# MAGIC
# MAGIC -- =============================================================================
# MAGIC -- SECTION: MONITORING EXTRA (monitoring_extra.sql)
# MAGIC -- =============================================================================
# MAGIC
# MAGIC -- ========== 1. Jobs that create unused tables ==========
# MAGIC WITH tables_written_by_jobs AS (
# MAGIC   SELECT DISTINCT
# MAGIC     entity_id AS job_entity_id,
# MAGIC     target_table_full_name AS table_full_name
# MAGIC   FROM system.access.table_lineage
# MAGIC   WHERE entity_type = 'JOB'
# MAGIC     AND target_table_full_name IS NOT NULL
# MAGIC     AND event_date >= current_date() - INTERVAL 90 DAYS
# MAGIC ),
# MAGIC tables_read_90d AS (
# MAGIC   SELECT DISTINCT source_table_full_name AS table_full_name
# MAGIC   FROM system.access.table_lineage
# MAGIC   WHERE source_table_full_name IS NOT NULL
# MAGIC     AND event_date >= current_date() - INTERVAL 90 DAYS
# MAGIC ),
# MAGIC unused_written AS (
# MAGIC   SELECT w.job_entity_id, w.table_full_name
# MAGIC   FROM tables_written_by_jobs w
# MAGIC   LEFT JOIN tables_read_90d r ON w.table_full_name = r.table_full_name
# MAGIC   WHERE r.table_full_name IS NULL
# MAGIC )
# MAGIC SELECT
# MAGIC   job_entity_id AS job_id,
# MAGIC   COUNT(*) AS unused_table_count,
# MAGIC   COLLECT_SET(table_full_name) AS unused_tables
# MAGIC FROM unused_written
# MAGIC GROUP BY job_entity_id
# MAGIC HAVING COUNT(*) > 0
# MAGIC ORDER BY unused_table_count DESC
# MAGIC LIMIT 50;
# MAGIC
# MAGIC -- ========== 2. Unused tables with last write time (prioritize cleanup) ==========
# MAGIC WITH tables_written_90d AS (
# MAGIC   SELECT DISTINCT target_table_full_name AS table_full_name
# MAGIC   FROM system.access.table_lineage
# MAGIC   WHERE target_table_full_name IS NOT NULL
# MAGIC     AND event_date >= current_date() - INTERVAL 90 DAYS
# MAGIC ),
# MAGIC tables_read_90d AS (
# MAGIC   SELECT DISTINCT source_table_full_name AS table_full_name
# MAGIC   FROM system.access.table_lineage
# MAGIC   WHERE source_table_full_name IS NOT NULL
# MAGIC     AND event_date >= current_date() - INTERVAL 90 DAYS
# MAGIC ),
# MAGIC last_write AS (
# MAGIC   SELECT
# MAGIC     target_table_full_name AS table_full_name,
# MAGIC     MAX(event_time) AS last_write_time
# MAGIC   FROM system.access.table_lineage
# MAGIC   WHERE target_table_full_name IS NOT NULL
# MAGIC     AND event_date >= current_date() - INTERVAL 365 DAYS
# MAGIC   GROUP BY target_table_full_name
# MAGIC )
# MAGIC SELECT
# MAGIC   w.table_full_name,
# MAGIC   lw.last_write_time,
# MAGIC   DATEDIFF(current_date(), DATE(lw.last_write_time)) AS days_since_write,
# MAGIC   'no_reads_90d' AS reason
# MAGIC FROM tables_written_90d w
# MAGIC LEFT JOIN tables_read_90d r ON w.table_full_name = r.table_full_name
# MAGIC INNER JOIN last_write lw ON w.table_full_name = lw.table_full_name
# MAGIC WHERE r.table_full_name IS NULL
# MAGIC ORDER BY days_since_write DESC
# MAGIC LIMIT 200;
# MAGIC
# MAGIC -- ========== 3. Tables with most access (read + write) ==========
# MAGIC
# MAGIC -- ========== 4. Clusters that need optimization (cost + cost per job) ==========
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH read_activity AS (
# MAGIC   SELECT
# MAGIC     source_table_full_name AS table_full_name,
# MAGIC     COUNT(*) AS read_events,
# MAGIC     COUNT(DISTINCT entity_id) AS reader_entities,
# MAGIC     COUNT(DISTINCT created_by) AS distinct_readers
# MAGIC   FROM system.access.table_lineage
# MAGIC   WHERE source_table_full_name IS NOT NULL
# MAGIC     AND event_date >= current_date() - INTERVAL 30 DAYS
# MAGIC   GROUP BY source_table_full_name
# MAGIC ),
# MAGIC write_activity AS (
# MAGIC   SELECT
# MAGIC     target_table_full_name AS table_full_name,
# MAGIC     COUNT(*) AS write_events,
# MAGIC     COUNT(DISTINCT entity_id) AS writer_entities,
# MAGIC     COUNT(DISTINCT created_by) AS distinct_writers
# MAGIC   FROM system.access.table_lineage
# MAGIC   WHERE target_table_full_name IS NOT NULL
# MAGIC     AND event_date >= current_date() - INTERVAL 30 DAYS
# MAGIC   GROUP BY target_table_full_name
# MAGIC )
# MAGIC SELECT
# MAGIC   COALESCE(r.table_full_name, w.table_full_name) AS table_full_name,
# MAGIC   COALESCE(r.read_events, 0) AS read_events,
# MAGIC   COALESCE(w.write_events, 0) AS write_events,
# MAGIC   COALESCE(r.read_events, 0) + COALESCE(w.write_events, 0) AS total_access_events,
# MAGIC   COALESCE(r.distinct_readers, 0) + COALESCE(w.distinct_writers, 0) AS distinct_users_approx
# MAGIC FROM read_activity r
# MAGIC FULL OUTER JOIN write_activity w ON r.table_full_name = w.table_full_name
# MAGIC ORDER BY total_access_events DESC
# MAGIC LIMIT 100;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH cluster_usage AS (
# MAGIC   SELECT
# MAGIC     usage_metadata.cluster_id AS cluster_id,
# MAGIC     usage_metadata.job_id AS job_id,
# MAGIC     billing_origin_product,
# MAGIC     sku_name,
# MAGIC     usage_unit,
# MAGIC     SUM(usage_quantity) AS usage_quantity
# MAGIC   FROM system.billing.usage
# MAGIC   WHERE record_type = 'ORIGINAL'
# MAGIC     AND usage_date >= current_date() - INTERVAL 30 DAYS
# MAGIC     AND usage_metadata.cluster_id IS NOT NULL
# MAGIC   GROUP BY usage_metadata.cluster_id, usage_metadata.job_id, billing_origin_product, sku_name, usage_unit
# MAGIC ),
# MAGIC prices AS (
# MAGIC   SELECT
# MAGIC     sku_name,
# MAGIC     usage_unit,
# MAGIC     CAST(COALESCE(pricing.effective_list.default, pricing.default) AS DOUBLE) AS unit_price
# MAGIC   FROM system.billing.list_prices
# MAGIC   WHERE price_end_time > current_timestamp()
# MAGIC   QUALIFY ROW_NUMBER() OVER (PARTITION BY sku_name, usage_unit ORDER BY price_start_time DESC) = 1
# MAGIC ),
# MAGIC cluster_cost AS (
# MAGIC   SELECT
# MAGIC     cluster_id,
# MAGIC     SUM(c.usage_quantity) AS total_usage,
# MAGIC     ROUND(SUM(c.usage_quantity * COALESCE(p.unit_price, 0)), 2) AS estimated_cost_usd,
# MAGIC     COUNT(DISTINCT job_id) AS distinct_jobs
# MAGIC   FROM cluster_usage c
# MAGIC   LEFT JOIN prices p ON c.sku_name = p.sku_name AND c.usage_unit = p.usage_unit
# MAGIC   GROUP BY cluster_id
# MAGIC )
# MAGIC SELECT
# MAGIC   cluster_id,
# MAGIC   estimated_cost_usd,
# MAGIC   total_usage,
# MAGIC   distinct_jobs,
# MAGIC   ROUND(estimated_cost_usd / NULLIF(distinct_jobs, 0), 2) AS cost_per_job_usd
# MAGIC FROM cluster_cost
# MAGIC ORDER BY estimated_cost_usd DESC
# MAGIC LIMIT 50;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH cluster_usage AS (
# MAGIC   SELECT
# MAGIC     usage_metadata.cluster_id AS cluster_id,
# MAGIC     usage_metadata.job_id AS job_id,
# MAGIC     sku_name,
# MAGIC     usage_unit,
# MAGIC     SUM(usage_quantity) AS usage_quantity
# MAGIC   FROM system.billing.usage
# MAGIC   WHERE record_type = 'ORIGINAL'
# MAGIC     AND usage_date >= current_date() - INTERVAL 30 DAYS
# MAGIC     AND usage_metadata.cluster_id IS NOT NULL
# MAGIC   GROUP BY usage_metadata.cluster_id, usage_metadata.job_id, sku_name, usage_unit
# MAGIC ),
# MAGIC prices AS (
# MAGIC   SELECT sku_name, usage_unit,
# MAGIC     CAST(COALESCE(pricing.effective_list.default, pricing.default) AS DOUBLE) AS unit_price
# MAGIC   FROM system.billing.list_prices
# MAGIC   WHERE price_end_time > current_timestamp()
# MAGIC   QUALIFY ROW_NUMBER() OVER (PARTITION BY sku_name, usage_unit ORDER BY price_start_time DESC) = 1
# MAGIC ),
# MAGIC cluster_metrics AS (
# MAGIC   SELECT
# MAGIC     cluster_id,
# MAGIC     ROUND(SUM(c.usage_quantity * COALESCE(p.unit_price, 0)), 2) AS estimated_cost_usd,
# MAGIC     COUNT(DISTINCT job_id) AS distinct_jobs
# MAGIC   FROM cluster_usage c
# MAGIC   LEFT JOIN prices p ON c.sku_name = p.sku_name AND c.usage_unit = p.usage_unit
# MAGIC   GROUP BY cluster_id
# MAGIC   HAVING SUM(c.usage_quantity * COALESCE(p.unit_price, 0)) > 10
# MAGIC )
# MAGIC SELECT
# MAGIC   cluster_id,
# MAGIC   estimated_cost_usd,
# MAGIC   distinct_jobs,
# MAGIC   ROUND(estimated_cost_usd / NULLIF(distinct_jobs, 0), 2) AS cost_per_job_usd
# MAGIC FROM cluster_metrics
# MAGIC WHERE distinct_jobs < 20
# MAGIC ORDER BY cost_per_job_usd DESC
# MAGIC LIMIT 30;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ========== 6. Tables written but never read (orphan writes) ==========
# MAGIC WITH written AS (
# MAGIC   SELECT DISTINCT target_table_full_name AS full_name
# MAGIC   FROM system.access.table_lineage
# MAGIC   WHERE target_table_full_name IS NOT NULL
# MAGIC     AND event_date >= current_date() - INTERVAL 90 DAYS
# MAGIC ),
# MAGIC read_as_source AS (
# MAGIC   SELECT DISTINCT source_table_full_name AS full_name
# MAGIC   FROM system.access.table_lineage
# MAGIC   WHERE source_table_full_name IS NOT NULL
# MAGIC     AND event_date >= current_date() - INTERVAL 90 DAYS
# MAGIC )
# MAGIC SELECT w.full_name AS table_full_name, 'written_never_read_90d' AS reason
# MAGIC FROM written w
# MAGIC LEFT JOIN read_as_source r ON w.full_name = r.full_name
# MAGIC WHERE r.full_name IS NULL
# MAGIC ORDER BY w.full_name
# MAGIC LIMIT 200;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ========== 7. Jobs with most distinct tables written (table churn) ==========
# MAGIC SELECT
# MAGIC   entity_id AS job_entity_id,
# MAGIC   COUNT(DISTINCT target_table_full_name) AS tables_written,
# MAGIC   COLLECT_SET(DISTINCT target_table_full_name) AS table_list
# MAGIC FROM system.access.table_lineage
# MAGIC WHERE entity_type = 'JOB'
# MAGIC   AND target_table_full_name IS NOT NULL
# MAGIC   AND event_date >= current_date() - INTERVAL 30 DAYS
# MAGIC GROUP BY entity_id
# MAGIC ORDER BY tables_written DESC
# MAGIC LIMIT 50;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ========== 8. Warehouses with longest avg queue time (under-provisioned) ==========
# MAGIC SELECT
# MAGIC   compute.warehouse_id AS warehouse_id,
# MAGIC   COUNT(*) AS queries_with_queue,
# MAGIC   ROUND(AVG(waiting_at_capacity_duration_ms) / 1000.0, 1) AS avg_queue_sec,
# MAGIC   ROUND(MAX(waiting_at_capacity_duration_ms) / 1000.0, 1) AS max_queue_sec
# MAGIC FROM system.query.history
# MAGIC WHERE start_time >= current_timestamp() - INTERVAL 7 DAYS
# MAGIC   AND compute.warehouse_id IS NOT NULL
# MAGIC   AND waiting_at_capacity_duration_ms > 0
# MAGIC GROUP BY compute.warehouse_id
# MAGIC HAVING COUNT(*) >= 5
# MAGIC ORDER BY avg_queue_sec DESC
# MAGIC LIMIT 20;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ========== 9. Top tables by distinct consumers (read + write) ==========
# MAGIC WITH as_source AS (
# MAGIC   SELECT
# MAGIC     source_table_full_name AS table_full_name,
# MAGIC     COUNT(DISTINCT entity_id) AS consumer_count
# MAGIC   FROM system.access.table_lineage
# MAGIC   WHERE source_table_full_name IS NOT NULL
# MAGIC     AND event_date >= current_date() - INTERVAL 30 DAYS
# MAGIC   GROUP BY source_table_full_name
# MAGIC ),
# MAGIC as_target AS (
# MAGIC   SELECT
# MAGIC     target_table_full_name AS table_full_name,
# MAGIC     COUNT(DISTINCT entity_id) AS producer_count
# MAGIC   FROM system.access.table_lineage
# MAGIC   WHERE target_table_full_name IS NOT NULL
# MAGIC     AND event_date >= current_date() - INTERVAL 30 DAYS
# MAGIC   GROUP BY target_table_full_name
# MAGIC )
# MAGIC SELECT
# MAGIC   COALESCE(s.table_full_name, t.table_full_name) AS table_full_name,
# MAGIC   COALESCE(s.consumer_count, 0) AS distinct_readers,
# MAGIC   COALESCE(t.producer_count, 0) AS distinct_writers,
# MAGIC   COALESCE(s.consumer_count, 0) + COALESCE(t.producer_count, 0) AS total_distinct_entities
# MAGIC FROM as_source s
# MAGIC FULL OUTER JOIN as_target t ON s.table_full_name = t.table_full_name
# MAGIC ORDER BY total_distinct_entities DESC
# MAGIC LIMIT 100;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ========== 10. Jobs with high failure rate and unused output tables ==========
# MAGIC WITH tables_read_90d AS (
# MAGIC   SELECT DISTINCT source_table_full_name AS table_full_name
# MAGIC   FROM system.access.table_lineage
# MAGIC   WHERE source_table_full_name IS NOT NULL
# MAGIC     AND event_date >= current_date() - INTERVAL 90 DAYS
# MAGIC ),
# MAGIC tables_written_by_job AS (
# MAGIC   SELECT DISTINCT entity_id AS job_entity_id, target_table_full_name AS table_full_name
# MAGIC   FROM system.access.table_lineage
# MAGIC   WHERE entity_type = 'JOB'
# MAGIC     AND target_table_full_name IS NOT NULL
# MAGIC     AND event_date >= current_date() - INTERVAL 90 DAYS
# MAGIC ),
# MAGIC unused_count_by_job AS (
# MAGIC   SELECT
# MAGIC     w.job_entity_id,
# MAGIC     COUNT(*) AS unused_count
# MAGIC   FROM tables_written_by_job w
# MAGIC   LEFT JOIN tables_read_90d r ON w.table_full_name = r.table_full_name
# MAGIC   WHERE r.table_full_name IS NULL
# MAGIC   GROUP BY w.job_entity_id
# MAGIC ),
# MAGIC job_fail_rate AS (
# MAGIC   SELECT
# MAGIC     job_id,
# MAGIC     COUNT(DISTINCT run_id) AS total_runs,
# MAGIC     SUM(CASE WHEN result_state = 'FAILED' THEN 1 ELSE 0 END) AS failed_runs
# MAGIC   FROM (
# MAGIC     SELECT job_id, run_id, MAX(result_state) AS result_state
# MAGIC     FROM system.lakeflow.job_run_timeline
# MAGIC     WHERE period_start_time >= current_timestamp() - INTERVAL 14 DAYS
# MAGIC     GROUP BY job_id, run_id
# MAGIC   ) x
# MAGIC   GROUP BY job_id
# MAGIC   HAVING SUM(CASE WHEN result_state = 'FAILED' THEN 1 ELSE 0 END) >= 1
# MAGIC )
# MAGIC SELECT
# MAGIC   f.job_id,
# MAGIC   f.total_runs,
# MAGIC   f.failed_runs,
# MAGIC   ROUND(100.0 * f.failed_runs / NULLIF(f.total_runs, 0), 1) AS failure_rate_pct,
# MAGIC   COALESCE(u.unused_count, 0) AS unused_tables_created_90d
# MAGIC FROM job_fail_rate f
# MAGIC LEFT JOIN unused_count_by_job u ON CAST(f.job_id AS STRING) = CAST(u.job_entity_id AS STRING)
# MAGIC ORDER BY failure_rate_pct DESC, unused_tables_created_90d DESC
# MAGIC LIMIT 30;
# MAGIC