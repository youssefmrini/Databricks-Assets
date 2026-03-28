# Databricks notebook source
# MAGIC %md
# MAGIC # UniForm V3 - Delta + Iceberg Interoperability (DBR 18.0+)
# MAGIC
# MAGIC UniForm enables Delta tables to be read as Iceberg tables — a single copy of Parquet data with multiple metadata formats. **IcebergCompatV3** adds support for deletion vectors, enabling full V3 compatibility.
# MAGIC
# MAGIC **Key points:**
# MAGIC - Single copy of data, multiple metadata formats (Delta + Iceberg + Hudi)
# MAGIC - Iceberg metadata generated asynchronously — no data rewrite
# MAGIC - V3 compatibility enables deletion vectors for Iceberg readers
# MAGIC - External engines (Spark, Trino, DuckDB, Snowflake) can read via Iceberg REST Catalog
# MAGIC
# MAGIC **Runtime:** DBR 18.0+ for V3 compatibility

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS features_demo;
# MAGIC USE CATALOG features_demo;
# MAGIC CREATE SCHEMA IF NOT EXISTS uniform_v3;
# MAGIC USE SCHEMA uniform_v3;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Create a Delta Table with UniForm (Iceberg reads)

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS products_uniform;
# MAGIC CREATE TABLE products_uniform (
# MAGIC   product_id BIGINT,
# MAGIC   name STRING,
# MAGIC   category STRING,
# MAGIC   price DECIMAL(10,2),
# MAGIC   stock INT,
# MAGIC   last_updated TIMESTAMP
# MAGIC )
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.universalFormat.enabledFormats' = 'iceberg',
# MAGIC   'delta.enableIcebergCompatV2' = 'true',
# MAGIC   'delta.columnMapping.mode' = 'name'
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify UniForm is enabled
# MAGIC SHOW TBLPROPERTIES products_uniform;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Insert Data

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO products_uniform VALUES
# MAGIC   (1, 'MacBook Pro 16"', 'Laptops', 2499.99, 150, current_timestamp()),
# MAGIC   (2, 'iPhone 17', 'Phones', 1199.99, 500, current_timestamp()),
# MAGIC   (3, 'AirPods Max', 'Audio', 549.99, 200, current_timestamp()),
# MAGIC   (4, 'iPad Pro', 'Tablets', 1099.99, 300, current_timestamp()),
# MAGIC   (5, 'Apple Watch Ultra', 'Wearables', 799.99, 180, current_timestamp());

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Upgrade to Iceberg V3 Compatibility

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Enable V3 compatibility for deletion vector support
# MAGIC ALTER TABLE products_uniform SET TBLPROPERTIES (
# MAGIC   'delta.enableIcebergCompatV3' = 'true',
# MAGIC   'delta.enableIcebergCompatV2' = 'false'
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DML operations now use deletion vectors readable by Iceberg V3 clients
# MAGIC UPDATE products_uniform SET price = 2299.99 WHERE product_id = 1;
# MAGIC DELETE FROM products_uniform WHERE product_id = 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM products_uniform ORDER BY product_id;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. View UniForm Metadata

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check table details including Iceberg metadata
# MAGIC DESCRIBE DETAIL products_uniform;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- History shows both Delta and Iceberg metadata are maintained
# MAGIC DESCRIBE HISTORY products_uniform LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Enable UniForm on Existing Delta Table

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a regular Delta table first
# MAGIC DROP TABLE IF EXISTS inventory;
# MAGIC CREATE TABLE inventory (
# MAGIC   sku STRING,
# MAGIC   warehouse STRING,
# MAGIC   quantity INT
# MAGIC );
# MAGIC
# MAGIC INSERT INTO inventory VALUES
# MAGIC   ('SKU-001', 'Paris', 100),
# MAGIC   ('SKU-002', 'London', 250),
# MAGIC   ('SKU-003', 'Tokyo', 180);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Enable UniForm on the existing table — no data rewrite needed
# MAGIC ALTER TABLE inventory SET TBLPROPERTIES (
# MAGIC   'delta.universalFormat.enabledFormats' = 'iceberg',
# MAGIC   'delta.enableIcebergCompatV2' = 'true',
# MAGIC   'delta.columnMapping.mode' = 'name'
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify it's enabled
# MAGIC SHOW TBLPROPERTIES inventory;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. REST Catalog Access (External Engines)
# MAGIC
# MAGIC External Iceberg-compatible engines can read these tables via Unity Catalog's Iceberg REST Catalog endpoint:
# MAGIC
# MAGIC ```python
# MAGIC # Example: Reading from external Spark
# MAGIC spark = SparkSession.builder \
# MAGIC     .config("spark.sql.catalog.unity", "org.apache.iceberg.spark.SparkCatalog") \
# MAGIC     .config("spark.sql.catalog.unity.type", "rest") \
# MAGIC     .config("spark.sql.catalog.unity.uri", "https://<workspace>/api/2.1/unity-catalog/iceberg") \
# MAGIC     .config("spark.sql.catalog.unity.token", "<pat>") \
# MAGIC     .getOrCreate()
# MAGIC
# MAGIC df = spark.read.table("unity.features_demo.uniform_v3.products_uniform")
# MAGIC ```
# MAGIC
# MAGIC Compatible engines: Spark, Trino, DuckDB, Daft, Snowflake, Dremio

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **Summary:** UniForm bridges the Delta-Iceberg divide with zero data duplication. Write once in Delta, read anywhere via Iceberg metadata. V3 compatibility ensures deletion vectors work across both formats, enabling the latest optimizations for all readers.
