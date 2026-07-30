# Databricks notebook source
# MAGIC %md
# MAGIC # VARIANT Shredding (DBR 17.2+)
# MAGIC
# MAGIC Variant shredding **columnarizes commonly occurring fields** in VARIANT data, providing massive read performance gains while keeping the flexibility of semi-structured data.
# MAGIC
# MAGIC **Performance:**
# MAGIC - **8x** faster reads vs regular VARIANT
# MAGIC - **30x** faster reads vs storing JSON as STRING
# MAGIC - 20-50% slower writes (acceptable tradeoff for read-heavy workloads)
# MAGIC
# MAGIC **Runtime:** DBR 17.2+

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS features_demo;
# MAGIC USE CATALOG features_demo;
# MAGIC CREATE SCHEMA IF NOT EXISTS variant_shredding;
# MAGIC USE SCHEMA variant_shredding;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Create Table with VARIANT Column

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS events_variant;
# MAGIC CREATE TABLE events_variant (
# MAGIC   event_id BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC   event_time TIMESTAMP,
# MAGIC   payload VARIANT
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Insert Semi-structured JSON Data

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Insert diverse JSON payloads
# MAGIC INSERT INTO events_variant (event_time, payload)
# MAGIC SELECT current_timestamp(), PARSE_JSON(json_str)
# MAGIC FROM VALUES
# MAGIC   ('{"type":"click","page":"/home","user_id":1001,"duration_ms":250}'),
# MAGIC   ('{"type":"purchase","product":"laptop","user_id":1002,"amount":999.99,"currency":"USD"}'),
# MAGIC   ('{"type":"click","page":"/products","user_id":1001,"duration_ms":180}'),
# MAGIC   ('{"type":"view","page":"/cart","user_id":1003,"items":["laptop","mouse"]}'),
# MAGIC   ('{"type":"purchase","product":"phone","user_id":1004,"amount":699.99,"currency":"EUR"}'),
# MAGIC   ('{"type":"click","page":"/checkout","user_id":1002,"duration_ms":320}'),
# MAGIC   ('{"type":"signup","user_id":1005,"source":"google","plan":"premium"}'),
# MAGIC   ('{"type":"purchase","product":"tablet","user_id":1001,"amount":449.99,"currency":"USD"}')
# MAGIC AS t(json_str);

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Query VARIANT Data with Path Expressions

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Extract typed values from VARIANT
# MAGIC SELECT
# MAGIC   event_id,
# MAGIC   payload:type::STRING AS event_type,
# MAGIC   payload:user_id::INT AS user_id,
# MAGIC   payload:amount::DECIMAL(10,2) AS amount,
# MAGIC   payload:page::STRING AS page
# MAGIC FROM events_variant
# MAGIC ORDER BY event_id;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Enable Variant Shredding

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Enable shredding for future writes
# MAGIC ALTER TABLE events_variant
# MAGIC SET TBLPROPERTIES ('delta.enableVariantShredding' = 'true');

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Apply shredding to existing data
# MAGIC REORG TABLE events_variant APPLY (SHRED VARIANT);

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Verify Shredding

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check table properties
# MAGIC SHOW TBLPROPERTIES events_variant;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Queries on shredded fields are now significantly faster
# MAGIC -- The engine reads columnar data directly instead of parsing VARIANT blobs
# MAGIC SELECT
# MAGIC   payload:type::STRING AS event_type,
# MAGIC   COUNT(*) AS event_count,
# MAGIC   SUM(payload:amount::DECIMAL(10,2)) AS total_amount
# MAGIC FROM events_variant
# MAGIC GROUP BY payload:type::STRING
# MAGIC ORDER BY event_count DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Compare: STRING vs VARIANT vs Shredded VARIANT

# COMMAND ----------

# MAGIC %sql
# MAGIC -- For comparison: storing as raw STRING (worst performance)
# MAGIC DROP TABLE IF EXISTS events_string;
# MAGIC CREATE TABLE events_string (
# MAGIC   event_id BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC   event_time TIMESTAMP,
# MAGIC   payload STRING
# MAGIC );
# MAGIC
# MAGIC INSERT INTO events_string (event_time, payload)
# MAGIC SELECT current_timestamp(), json_str
# MAGIC FROM VALUES
# MAGIC   ('{"type":"click","page":"/home","user_id":1001,"duration_ms":250}'),
# MAGIC   ('{"type":"purchase","product":"laptop","user_id":1002,"amount":999.99}'),
# MAGIC   ('{"type":"click","page":"/products","user_id":1001,"duration_ms":180}')
# MAGIC AS t(json_str);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- STRING query: requires parsing JSON at read time (slowest)
# MAGIC SELECT get_json_object(payload, '$.type') AS event_type, COUNT(*) AS cnt
# MAGIC FROM events_string
# MAGIC GROUP BY get_json_object(payload, '$.type');

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **Summary:** VARIANT shredding gives you the best of both worlds — the flexibility of semi-structured data with the performance of columnar storage. Enable it on read-heavy tables with semi-structured data for up to 30x speedups.
