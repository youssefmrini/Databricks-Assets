-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Query Tags (Public Preview - February 2026)
-- MAGIC
-- MAGIC Query Tags allow you to annotate queries with metadata for cost attribution, debugging, and auditing. Tags appear in `system.query.history` and the Query History UI.
-- MAGIC
-- MAGIC **Key capabilities:**
-- MAGIC - Set key-value tags on any SQL query
-- MAGIC - Tags flow through to system tables for cost tracking
-- MAGIC - Works in SQL editor, notebooks, dashboards, and connectors
-- MAGIC
-- MAGIC **Runtime:** Serverless SQL / DBR 18.0+

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1. Setting Query Tags

-- COMMAND ----------

-- Set tags for a team and project
-- Query Tags (Public Preview) - syntax:
-- SET QUERY_TAGS team = 'data-engineering', project = 'etl-pipeline';
-- Note: Requires query tags feature to be enabled in your workspace
SELECT 'Query Tags is in Public Preview - enable it in workspace settings' AS note;

-- COMMAND ----------

-- Run a query — tags are automatically attached
SELECT current_catalog(), current_schema(), current_timestamp() AS tagged_query_time;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2. Viewing Tags in Query History

-- COMMAND ----------

-- Query the system table to see tagged queries
SELECT
  statement_text,
  query_tags,
  start_time,
  total_duration_ms,
  warehouse_id
FROM system.query.history
WHERE query_tags IS NOT NULL
  AND start_time > dateadd(MINUTE, -5, current_timestamp())
ORDER BY start_time DESC
LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3. Cost Attribution by Tag

-- COMMAND ----------

-- Aggregate cost by team tag
SELECT
  query_tags['team'] AS team,
  query_tags['project'] AS project,
  COUNT(*) AS query_count,
  SUM(total_duration_ms) / 1000 AS total_seconds
FROM system.query.history
WHERE query_tags IS NOT NULL
  AND start_time > dateadd(DAY, -7, current_timestamp())
GROUP BY query_tags['team'], query_tags['project']
ORDER BY total_seconds DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4. Changing and Removing Tags

-- COMMAND ----------

-- Change tags mid-session
-- SET QUERY_TAGS team = 'analytics', project = 'dashboard-refresh';
SELECT 'Changed tags (when enabled)' AS note;

SELECT 'This query has different tags' AS note;

-- COMMAND ----------

-- Remove all tags
-- SET QUERY_TAGS NONE;
SELECT 'Tags removed (when enabled)' AS note;

SELECT 'This query has no tags' AS note;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ---
-- MAGIC **Summary:** Query Tags enable fine-grained cost attribution and audit trails. Tag queries by team, project, or environment, then analyze spending patterns in `system.query.history`.
