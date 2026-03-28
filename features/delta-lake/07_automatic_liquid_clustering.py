# Databricks notebook source
# MAGIC %md
# MAGIC # Automatic Liquid Clustering (GA - 2026)
# MAGIC
# MAGIC Predictive Optimization now includes **automatic liquid clustering** — Databricks analyzes your query workload and automatically selects and adapts optimal clustering keys.
# MAGIC
# MAGIC **Key capabilities:**
# MAGIC - `CLUSTER BY AUTO` lets Databricks choose the best keys
# MAGIC - Keys adapt as query patterns change
# MAGIC - Enabled by default for new UC managed tables with Predictive Optimization
# MAGIC - Up to 4 clustering keys supported
# MAGIC
# MAGIC **Runtime:** DBR 17.2+ / Serverless

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS features_demo;
# MAGIC USE CATALOG features_demo;
# MAGIC CREATE SCHEMA IF NOT EXISTS auto_clustering;
# MAGIC USE SCHEMA auto_clustering;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Create Table with Automatic Clustering

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS orders_auto;
# MAGIC CREATE TABLE orders_auto (
# MAGIC   order_id BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC   customer_id BIGINT,
# MAGIC   product_category STRING,
# MAGIC   order_date DATE,
# MAGIC   amount DECIMAL(10,2),
# MAGIC   region STRING
# MAGIC ) CLUSTER BY AUTO;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Insert Sample Data

# COMMAND ----------

# Generate sample data
from pyspark.sql.functions import expr, lit, col, rand, when, date_add
from pyspark.sql.types import *

df = (spark.range(0, 10000)
    .withColumn("customer_id", (rand() * 1000).cast("bigint"))
    .withColumn("product_category",
        when(rand() < 0.3, lit("Electronics"))
        .when(rand() < 0.5, lit("Clothing"))
        .when(rand() < 0.7, lit("Home"))
        .otherwise(lit("Books")))
    .withColumn("order_date", date_add(lit("2026-01-01"), (rand() * 90).cast("int")))
    .withColumn("amount", (rand() * 500 + 10).cast("decimal(10,2)"))
    .withColumn("region",
        when(rand() < 0.25, lit("EMEA"))
        .when(rand() < 0.5, lit("APAC"))
        .when(rand() < 0.75, lit("AMER"))
        .otherwise(lit("LATAM")))
    .drop("id")
)

df.write.mode("append").saveAsTable("features_demo.auto_clustering.orders_auto")
print(f"Inserted {df.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Check Clustering Status

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View clustering details
# MAGIC DESCRIBE DETAIL orders_auto;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Show table properties — look for clustering configuration
# MAGIC SHOW TBLPROPERTIES orders_auto;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Manual Clustering Comparison

# COMMAND ----------

# MAGIC %sql
# MAGIC -- For comparison: explicit clustering keys
# MAGIC DROP TABLE IF EXISTS orders_manual;
# MAGIC CREATE TABLE orders_manual (
# MAGIC   order_id BIGINT,
# MAGIC   customer_id BIGINT,
# MAGIC   product_category STRING,
# MAGIC   order_date DATE,
# MAGIC   amount DECIMAL(10,2),
# MAGIC   region STRING
# MAGIC ) CLUSTER BY (order_date, region);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- You can also change clustering keys at any time
# MAGIC ALTER TABLE orders_manual CLUSTER BY (product_category, order_date);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Or switch to auto
# MAGIC ALTER TABLE orders_manual CLUSTER BY AUTO;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Or disable clustering entirely
# MAGIC ALTER TABLE orders_manual CLUSTER BY NONE;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Run OPTIMIZE to Trigger Clustering

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE orders_auto;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify the optimization
# MAGIC DESCRIBE HISTORY orders_auto LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **Summary:** Automatic Liquid Clustering removes the guesswork from data layout optimization. Use `CLUSTER BY AUTO` and let Databricks continuously adapt clustering keys based on actual query patterns. This replaces traditional partitioning with a zero-maintenance approach.
