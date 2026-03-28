# Databricks notebook source
# MAGIC %md
# MAGIC # Partition Column Materialization (DBR 18.0 - January 2026)
# MAGIC
# MAGIC Partitioned Delta tables now write partition column values **directly into Parquet data files**. Previously, partition values lived only in the transaction log metadata and directory paths.
# MAGIC
# MAGIC **Benefits:**
# MAGIC - External Parquet readers see partition columns without parsing directory names
# MAGIC - Improved UniForm/Iceberg compatibility
# MAGIC - Better interoperability with non-Delta engines
# MAGIC
# MAGIC **Runtime:** DBR 18.0+

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS features_demo;
# MAGIC USE CATALOG features_demo;
# MAGIC CREATE SCHEMA IF NOT EXISTS partition_demo;
# MAGIC USE SCHEMA partition_demo;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Create a Partitioned Table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE sales_partitioned (
# MAGIC   order_id BIGINT,
# MAGIC   product STRING,
# MAGIC   amount DECIMAL(10,2),
# MAGIC   region STRING,
# MAGIC   order_date DATE
# MAGIC )
# MAGIC PARTITIONED BY (region)
# MAGIC TBLPROPERTIES ('delta.enableDeletionVectors' = 'true');

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Insert sample data across multiple partitions
# MAGIC INSERT INTO sales_partitioned VALUES
# MAGIC   (1, 'Laptop', 999.99, 'EMEA', '2026-01-15'),
# MAGIC   (2, 'Phone', 699.99, 'EMEA', '2026-01-16'),
# MAGIC   (3, 'Tablet', 449.99, 'APAC', '2026-01-15'),
# MAGIC   (4, 'Monitor', 349.99, 'APAC', '2026-01-17'),
# MAGIC   (5, 'Keyboard', 79.99, 'AMER', '2026-01-15'),
# MAGIC   (6, 'Mouse', 49.99, 'AMER', '2026-01-18');

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Verify Partition Column in Parquet Files

# COMMAND ----------

# Get the table location
table_detail = spark.sql("DESCRIBE DETAIL sales_partitioned").collect()[0]
table_location = table_detail.location
print(f"Table location: {table_location}")

# COMMAND ----------

# List the Parquet files
detail = spark.sql("DESCRIBE DETAIL sales_partitioned")
display(detail)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Read the raw Parquet data — partition column 'region' is now embedded in the files
# MAGIC SELECT * FROM sales_partitioned;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Compare with Table Details

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Show partitioning info
# MAGIC DESCRIBE DETAIL sales_partitioned;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Show the file-level details with partition info
# MAGIC SELECT
# MAGIC   input_file_name() AS file_path,
# MAGIC   region,
# MAGIC   COUNT(*) AS row_count
# MAGIC FROM sales_partitioned
# MAGIC GROUP BY input_file_name(), region
# MAGIC ORDER BY region;

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **Summary:** With DBR 18.0, partition column values are materialized directly in Parquet data files. This eliminates the dependency on directory-based partition discovery and improves compatibility with external Parquet readers and Iceberg/UniForm metadata.
