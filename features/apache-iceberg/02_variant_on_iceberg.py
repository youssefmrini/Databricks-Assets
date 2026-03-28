# Databricks notebook source
# MAGIC %md
# MAGIC # VARIANT Data Type on Iceberg V3
# MAGIC
# MAGIC The VARIANT data type is now supported on Iceberg V3 tables. This enables storing and querying semi-structured JSON-like data with native type support — broader than JSON with date, timestamp, decimal, and binary primitives.
# MAGIC
# MAGIC **Key points:**
# MAGIC - Only available on Iceberg V3 tables (V2 does not support VARIANT)
# MAGIC - Supports shredding for performance optimization
# MAGIC - Native path expression syntax for querying
# MAGIC
# MAGIC **Runtime:** DBR 18.0+

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS features_demo;
# MAGIC USE CATALOG features_demo;
# MAGIC CREATE SCHEMA IF NOT EXISTS iceberg_variant;
# MAGIC USE SCHEMA iceberg_variant;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Create Iceberg V3 Table with VARIANT Column

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS api_events;
# MAGIC CREATE TABLE api_events (
# MAGIC   event_id BIGINT,
# MAGIC   event_time TIMESTAMP,
# MAGIC   payload VARIANT
# MAGIC ) USING iceberg
# MAGIC TBLPROPERTIES ('format-version' = 3);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify VARIANT column in schema
# MAGIC DESCRIBE TABLE api_events;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Insert VARIANT Data

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO api_events
# MAGIC SELECT ROW_NUMBER() OVER (ORDER BY json_str) AS event_id, current_timestamp() AS event_time, PARSE_JSON(json_str) AS payload
# MAGIC FROM VALUES
# MAGIC   ('{"endpoint":"/api/users","method":"GET","status":200,"latency_ms":45,"user_agent":"Chrome/120"}'),
# MAGIC   ('{"endpoint":"/api/orders","method":"POST","status":201,"latency_ms":120,"body":{"product_id":42,"quantity":2}}'),
# MAGIC   ('{"endpoint":"/api/users/123","method":"PUT","status":200,"latency_ms":89,"changes":["email","phone"]}'),
# MAGIC   ('{"endpoint":"/api/auth","method":"POST","status":401,"latency_ms":15,"error":"Invalid credentials"}'),
# MAGIC   ('{"endpoint":"/api/orders","method":"GET","status":200,"latency_ms":67,"filters":{"date_range":"30d","status":"pending"}}'),
# MAGIC   ('{"endpoint":"/api/health","method":"GET","status":200,"latency_ms":5}')
# MAGIC AS t(json_str);

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Query VARIANT with Path Expressions

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Extract fields using path expressions
# MAGIC SELECT
# MAGIC   event_id,
# MAGIC   payload:endpoint::STRING AS endpoint,
# MAGIC   payload:method::STRING AS method,
# MAGIC   payload:status::INT AS status_code,
# MAGIC   payload:latency_ms::INT AS latency_ms
# MAGIC FROM api_events
# MAGIC ORDER BY event_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Aggregate by endpoint
# MAGIC SELECT
# MAGIC   payload:endpoint::STRING AS endpoint,
# MAGIC   payload:method::STRING AS method,
# MAGIC   COUNT(*) AS request_count,
# MAGIC   AVG(payload:latency_ms::INT) AS avg_latency_ms,
# MAGIC   MAX(payload:latency_ms::INT) AS max_latency_ms
# MAGIC FROM api_events
# MAGIC GROUP BY payload:endpoint::STRING, payload:method::STRING
# MAGIC ORDER BY avg_latency_ms DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Nested VARIANT Access

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Access nested objects
# MAGIC SELECT
# MAGIC   event_id,
# MAGIC   payload:endpoint::STRING AS endpoint,
# MAGIC   payload:body.product_id::INT AS product_id,
# MAGIC   payload:body.quantity::INT AS quantity,
# MAGIC   payload:error::STRING AS error_message
# MAGIC FROM api_events
# MAGIC WHERE payload:body IS NOT NULL OR payload:error IS NOT NULL;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Access array elements
# MAGIC SELECT
# MAGIC   event_id,
# MAGIC   payload:changes AS changes_array,
# MAGIC   payload:filters.date_range::STRING AS date_filter
# MAGIC FROM api_events
# MAGIC WHERE payload:changes IS NOT NULL OR payload:filters IS NOT NULL;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Add VARIANT Column to Existing Table

# COMMAND ----------

# MAGIC %sql
# MAGIC -- You can add VARIANT columns to existing V3 tables
# MAGIC ALTER TABLE api_events ADD COLUMN metadata VARIANT;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Update with metadata
# MAGIC UPDATE api_events
# MAGIC SET metadata = PARSE_JSON('{"source":"web","version":"2.1"}')
# MAGIC WHERE payload:endpoint::STRING = '/api/users';

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT event_id, payload:endpoint::STRING AS endpoint, metadata
# MAGIC FROM api_events ORDER BY event_id;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Enable Variant Shredding on Iceberg

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Enable shredding for columnar performance
# MAGIC ALTER TABLE api_events
# MAGIC SET TBLPROPERTIES ('iceberg.enableVariantShredding' = 'true');

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Apply shredding to existing data
# MAGIC REORG TABLE api_events APPLY (SHRED VARIANT);

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **Summary:** VARIANT on Iceberg V3 provides first-class semi-structured data support with rich type handling, nested access, and shredding for performance. It replaces the need to pre-define schemas for JSON data while maintaining query performance.
