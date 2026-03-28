# Databricks notebook source
# MAGIC %md
# MAGIC # Delta Vacuum

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS youssef;
# MAGIC use catalog youssef;
# MAGIC CREATE SCHEMA IF NOT EXISTS test;
# MAGIC use test;
# MAGIC -- Create the table
# MAGIC
# MAGIC
# MAGIC -- Insert random data into the table
# MAGIC INSERT INTO random_names (id, first_name, last_name) VALUES
# MAGIC (1, 'JOHN', 'Doe'),
# MAGIC (2, 'Jane', 'smith')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from random_names order by first_name 

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from random_names order by first_name collate unicode_ci_ai