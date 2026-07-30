# Databricks notebook source
# MAGIC %md
# MAGIC # Predictive Optimization Commands

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER CATALOG [catalog_name] { ENABLE | DISABLE | INHERIT } PREDICTIVE OPTIMIZATION;

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER { SCHEMA | DATABASE } schema_name { ENABLE | DISABLE | INHERIT } PREDICTIVE OPTIMIZATION;

# COMMAND ----------

# MAGIC %md
# MAGIC ALTER TABLE [table_name] DELETE ROWS {X} DAYS AFTER [time_column_name]
# MAGIC
# MAGIC Optimization capability that completely automates row deletion. Using this feature, you’ll be able to set a simple time-to-live policy directly on any UC managed table using a command like:
# MAGIC

# COMMAND ----------

# DBTITLE 1,To enable it for a new table
# MAGIC %sql
# MAGIC CREATE TABLE my_table (
# MAGIC   ...
# MAGIC   event_time TIMESTAMP
# MAGIC )
# MAGIC DELETE ROWS 30 DAYS AFTER event_time;

# COMMAND ----------

# DBTITLE 1,To enable it on an existing table:
# MAGIC %sql
# MAGIC ALTER TABLE my_table
# MAGIC DELETE ROWS 30 DAYS AFTER event_time;

# COMMAND ----------

# DBTITLE 1,To disable the feature
# MAGIC %sql
# MAGIC ALTER TABLE my_table DROP ROW DELETION;