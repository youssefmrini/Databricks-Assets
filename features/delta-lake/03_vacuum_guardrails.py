# Databricks notebook source
# MAGIC %md
# MAGIC # Time Travel & VACUUM New Guardrails (DBR 18.0)
# MAGIC
# MAGIC DBR 18.0 introduces stricter guardrails for time travel and VACUUM to prevent data loss:
# MAGIC
# MAGIC - Time travel beyond `deletedFileRetentionDuration` is now **blocked** for all tables
# MAGIC - `VACUUM` command **ignores** the `RETAIN XXX HOURS` argument (except 0 hours)
# MAGIC - `deletedFileRetentionDuration` cannot exceed `logRetentionDuration`
# MAGIC
# MAGIC **Runtime:** DBR 18.0+

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS features_demo;
# MAGIC USE CATALOG features_demo;
# MAGIC CREATE SCHEMA IF NOT EXISTS vacuum_guardrails;
# MAGIC USE SCHEMA vacuum_guardrails;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a demo table
# MAGIC CREATE OR REPLACE TABLE retention_demo (
# MAGIC   id INT,
# MAGIC   value STRING,
# MAGIC   updated_at TIMESTAMP
# MAGIC );
# MAGIC
# MAGIC INSERT INTO retention_demo VALUES (1, 'v1', current_timestamp());

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Default Retention Properties

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check default retention settings
# MAGIC DESCRIBE DETAIL features_demo.vacuum_guardrails.retention_demo;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View table properties
# MAGIC SHOW TBLPROPERTIES features_demo.vacuum_guardrails.retention_demo;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Guardrail: deletedFileRetentionDuration Cannot Exceed logRetentionDuration

# COMMAND ----------

# MAGIC %sql
# MAGIC -- This will fail: trying to set deletedFileRetentionDuration > logRetentionDuration
# MAGIC -- The default logRetentionDuration is 30 days
# MAGIC -- ALTER TABLE retention_demo SET TBLPROPERTIES ('delta.deletedFileRetentionDuration' = 'interval 60 days');
# MAGIC -- Uncomment above to see the error
# MAGIC
# MAGIC -- This works: set within bounds
# MAGIC ALTER TABLE retention_demo SET TBLPROPERTIES ('delta.deletedFileRetentionDuration' = 'interval 7 days');
# MAGIC SELECT 'Set deletedFileRetentionDuration to 7 days' AS status;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. VACUUM Now Uses Table Properties

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create multiple versions
# MAGIC UPDATE retention_demo SET value = 'v2' WHERE id = 1;
# MAGIC UPDATE retention_demo SET value = 'v3' WHERE id = 1;
# MAGIC UPDATE retention_demo SET value = 'v4' WHERE id = 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View version history
# MAGIC DESCRIBE HISTORY retention_demo LIMIT 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DRY RUN: VACUUM now uses table properties, RETAIN argument is informational only
# MAGIC VACUUM retention_demo DRY RUN;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Time Travel Respects Retention

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Time travel to version 0 (if within retention)
# MAGIC SELECT * FROM retention_demo VERSION AS OF 0;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Current version
# MAGIC SELECT * FROM retention_demo;

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **Summary:** DBR 18.0 adds safety guardrails to prevent accidental data loss. VACUUM respects table-level retention properties, and time travel is blocked beyond retention windows. These changes protect production data while still allowing time travel within configured bounds.
