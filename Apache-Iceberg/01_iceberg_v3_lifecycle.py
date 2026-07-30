# Databricks notebook source
# MAGIC %md
# MAGIC # Iceberg V3 Table Lifecycle (DBR 18.0+)
# MAGIC
# MAGIC Iceberg V3 is now fully supported on Databricks. This notebook demonstrates the complete lifecycle of V3 tables including:
# MAGIC
# MAGIC - **Deletion Vectors**: Compact Roaring bitmaps for efficient row-level deletes
# MAGIC - **Row Lineage**: `_row_id` and `_last_updated_sequence_number` for conflict detection
# MAGIC - **Time Travel and RESTORE**
# MAGIC - **Full DML Support**: INSERT, UPDATE, DELETE, MERGE INTO
# MAGIC
# MAGIC **Runtime:** DBR 18.0+

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS features_demo;
# MAGIC USE CATALOG features_demo;
# MAGIC CREATE SCHEMA IF NOT EXISTS iceberg_v3;
# MAGIC USE SCHEMA iceberg_v3;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Create an Iceberg V3 Table

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS customers_v3;
# MAGIC CREATE TABLE customers_v3 (
# MAGIC   customer_id BIGINT,
# MAGIC   name STRING,
# MAGIC   email STRING,
# MAGIC   city STRING,
# MAGIC   signup_date DATE
# MAGIC ) USING iceberg
# MAGIC TBLPROPERTIES ('format-version' = 3);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify format version
# MAGIC DESCRIBE DETAIL customers_v3;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Insert Data

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO customers_v3 VALUES
# MAGIC   (1, 'Alice Martin', 'alice@example.com', 'Paris', '2025-06-15'),
# MAGIC   (2, 'Bob Smith', 'bob@example.com', 'London', '2025-07-20'),
# MAGIC   (3, 'Charlie Lee', 'charlie@example.com', 'Tokyo', '2025-08-10'),
# MAGIC   (4, 'Diana Garcia', 'diana@example.com', 'Madrid', '2025-09-05'),
# MAGIC   (5, 'Eve Johnson', 'eve@example.com', 'New York', '2025-10-01');

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM customers_v3 ORDER BY customer_id;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Deletion Vectors in Action

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Delete a row — uses deletion vectors instead of rewriting files
# MAGIC DELETE FROM customers_v3 WHERE customer_id = 3;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Update a row — also leverages deletion vectors
# MAGIC UPDATE customers_v3 SET city = 'Berlin' WHERE customer_id = 2;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify changes
# MAGIC SELECT * FROM customers_v3 ORDER BY customer_id;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. MERGE INTO (Upsert)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Merge new and updated records
# MAGIC MERGE INTO customers_v3 AS target
# MAGIC USING (
# MAGIC   SELECT * FROM VALUES
# MAGIC     (3, 'Charlie Lee', 'charlie.new@example.com', 'Seoul', DATE'2025-08-10'),
# MAGIC     (6, 'Frank White', 'frank@example.com', 'Sydney', DATE'2026-01-15')
# MAGIC   AS source(customer_id, name, email, city, signup_date)
# MAGIC ) AS source
# MAGIC ON target.customer_id = source.customer_id
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM customers_v3 ORDER BY customer_id;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Time Travel

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View table history
# MAGIC DESCRIBE HISTORY customers_v3;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Read an earlier version (version 1 = after initial insert)
# MAGIC SELECT * FROM customers_v3 VERSION AS OF 1 ORDER BY customer_id;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. RESTORE Table

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Restore to a previous version
# MAGIC RESTORE TABLE customers_v3 TO VERSION AS OF 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify restore — should show original 5 rows
# MAGIC SELECT * FROM customers_v3 ORDER BY customer_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Restore back to latest state
# MAGIC DESCRIBE HISTORY customers_v3;

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **Summary:** Iceberg V3 tables on Databricks support the full spectrum of data operations with built-in deletion vectors and row lineage. Time travel and RESTORE provide robust recovery capabilities, while MERGE INTO enables efficient upsert patterns.
