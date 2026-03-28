# Databricks notebook source
# MAGIC %md
# MAGIC # Liquid Clustering on Iceberg (Public Preview)
# MAGIC
# MAGIC Liquid clustering is now available for Iceberg tables on Databricks. This replaces traditional partitioning with an adaptive, zero-maintenance approach to data layout optimization.
# MAGIC
# MAGIC **Key capabilities:**
# MAGIC - `CLUSTER BY (col1, col2, ...)` — up to 4 explicit keys
# MAGIC - `CLUSTER BY AUTO` — Databricks selects keys based on query patterns
# MAGIC - Keys can be changed at any time without data rewrite
# MAGIC - `PARTITIONED BY` is **NOT** supported on managed Iceberg tables
# MAGIC
# MAGIC **Runtime:** DBR 16.4 LTS+

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS features_demo;
# MAGIC USE CATALOG features_demo;
# MAGIC CREATE SCHEMA IF NOT EXISTS iceberg_clustering;
# MAGIC USE SCHEMA iceberg_clustering;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Create Iceberg Table with Explicit Clustering

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS transactions;
# MAGIC CREATE TABLE transactions (
# MAGIC   txn_id BIGINT,
# MAGIC   customer_id BIGINT,
# MAGIC   merchant STRING,
# MAGIC   category STRING,
# MAGIC   amount DECIMAL(10,2),
# MAGIC   txn_date DATE,
# MAGIC   region STRING
# MAGIC ) USING iceberg
# MAGIC TBLPROPERTIES (
# MAGIC   'iceberg.enableDeletionVectors' = 'false',
# MAGIC   'iceberg.enableRowTracking' = 'false'
# MAGIC )
# MAGIC CLUSTER BY (txn_date, region);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check clustering configuration
# MAGIC DESCRIBE DETAIL transactions;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Insert Data

# COMMAND ----------

from pyspark.sql.functions import expr, lit, rand, when, date_add, col

df = (spark.range(0, 50000)
    .withColumn("customer_id", (rand() * 5000).cast("bigint"))
    .withColumn("merchant",
        when(rand() < 0.2, lit("Amazon"))
        .when(rand() < 0.4, lit("Walmart"))
        .when(rand() < 0.6, lit("Starbucks"))
        .when(rand() < 0.8, lit("Apple"))
        .otherwise(lit("Netflix")))
    .withColumn("category",
        when(rand() < 0.25, lit("Electronics"))
        .when(rand() < 0.5, lit("Food"))
        .when(rand() < 0.75, lit("Entertainment"))
        .otherwise(lit("Clothing")))
    .withColumn("amount", (rand() * 200 + 5).cast("decimal(10,2)"))
    .withColumn("txn_date", date_add(lit("2026-01-01"), (rand() * 90).cast("int")))
    .withColumn("region",
        when(rand() < 0.3, lit("EMEA"))
        .when(rand() < 0.55, lit("APAC"))
        .when(rand() < 0.8, lit("AMER"))
        .otherwise(lit("LATAM")))
    .select(
        col("id").cast("bigint").alias("txn_id"),
        col("customer_id"),
        col("merchant"),
        col("category"),
        col("amount"),
        col("txn_date"),
        col("region")
    )
)

# Use SQL INSERT for Iceberg tables
df.createOrReplaceTempView("txn_temp")
spark.sql("INSERT INTO features_demo.iceberg_clustering.transactions SELECT * FROM txn_temp")
print(f"Inserted {df.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Run OPTIMIZE to Apply Clustering

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE transactions;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query with clustering key filter — benefits from data skipping
# MAGIC SELECT
# MAGIC   region,
# MAGIC   COUNT(*) AS txn_count,
# MAGIC   SUM(amount) AS total_amount,
# MAGIC   AVG(amount) AS avg_amount
# MAGIC FROM transactions
# MAGIC WHERE txn_date BETWEEN '2026-02-01' AND '2026-02-28'
# MAGIC   AND region = 'EMEA'
# MAGIC GROUP BY region;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Change Clustering Keys (No Data Rewrite!)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Change to cluster by category and merchant instead
# MAGIC ALTER TABLE transactions CLUSTER BY (category, merchant);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Re-optimize with new keys
# MAGIC OPTIMIZE transactions;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Switch to Automatic Clustering

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Let Databricks choose the best keys
# MAGIC ALTER TABLE transactions CLUSTER BY AUTO;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL transactions;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Disable Clustering

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE transactions CLUSTER BY NONE;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Re-enable with original keys
# MAGIC ALTER TABLE transactions CLUSTER BY (txn_date, region);

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **Summary:** Liquid clustering on Iceberg provides adaptive data layout that you can change at any time — no need to rewrite data or redesign partitioning. Use `CLUSTER BY AUTO` for zero-maintenance optimization, or specify explicit keys for predictable workloads.
