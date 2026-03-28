# Databricks notebook source
# MAGIC %md
# MAGIC # ITM Demo

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC use catalog demo_itm;
# MAGIC create schema tables;
# MAGIC use schema tables;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS demo_itm.tables.customers (
# MAGIC   customer_id INT,
# MAGIC   first_name STRING,
# MAGIC   last_name STRING,
# MAGIC   email STRING,
# MAGIC   phone STRING,
# MAGIC   address STRING,
# MAGIC   city STRING,
# MAGIC   state STRING,
# MAGIC   zip_code STRING,
# MAGIC   customer_segment STRING,
# MAGIC   registration_date DATE,
# MAGIC   customer_status STRING,
# MAGIC   loyalty_tier STRING,
# MAGIC   tenure_years INT,
# MAGIC   churn_risk_score DOUBLE,
# MAGIC   customer_value_score DOUBLE
# MAGIC )
# MAGIC ;
# MAGIC
# MAGIC
# MAGIC
# MAGIC INSERT INTO main.dbdemos_ai_agent.customers VALUES
# MAGIC   (1, 'Alice', 'Smith', 'alice.smith@example.com', '555-1234', '123 Main St', 'Springfield', 'IL', '62701', 'Retail', '2022-01-15', 'Active', 'Gold', 3, 0.12, 85.5),
# MAGIC   (2, 'Bob', 'Johnson', 'bob.johnson@example.com', '555-5678', '456 Oak Ave', 'Greenville', 'TX', '75401', 'Wholesale', '2023-03-22', 'Inactive', 'Silver', 1, 0.45, 60.0)
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended demo_itm.tables.customers