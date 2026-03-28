# Databricks notebook source
# MAGIC %md
# MAGIC # Zstandard (ZSTD) Compression

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS youssef_demo;
# MAGIC use catalog youssef_demo;
# MAGIC use schema default;

# COMMAND ----------

# DBTITLE 1,Create customer table
# MAGIC %sql
# MAGIC -- Create customer table with common customer attributes
# MAGIC CREATE TABLE IF NOT EXISTS customer (
# MAGIC   customer_id INT,
# MAGIC   first_name STRING,
# MAGIC   last_name STRING,
# MAGIC   email STRING,
# MAGIC   phone STRING,
# MAGIC   address STRING,
# MAGIC   city STRING,
# MAGIC   state STRING,
# MAGIC   zip_code STRING,
# MAGIC   country STRING,
# MAGIC   registration_date DATE,
# MAGIC   customer_status STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# DBTITLE 1,Insert sample customer data


# COMMAND ----------

# DBTITLE 1,Verify customer data
# MAGIC %sql
# MAGIC -- Display all customer records
# MAGIC describe extended customer
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from youssef_demo.default.customer;