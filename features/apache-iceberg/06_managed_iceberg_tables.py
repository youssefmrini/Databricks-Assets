# Databricks notebook source
# MAGIC %md
# MAGIC # Managed Iceberg Tables in Unity Catalog
# MAGIC
# MAGIC Fully governed Iceberg tables created with `USING iceberg` in Unity Catalog. Includes automatic maintenance, predictive optimization, and full DML support.
# MAGIC
# MAGIC **Key capabilities:**
# MAGIC - Predictive Optimization (auto OPTIMIZE, VACUUM, ANALYZE)
# MAGIC - Automatic liquid clustering
# MAGIC - In-memory metadata caching
# MAGIC - Full DML: INSERT, UPDATE, DELETE, MERGE INTO
# MAGIC - Time travel and RESTORE
# MAGIC
# MAGIC **Runtime:** DBR 16.4 LTS+

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS features_demo;
# MAGIC USE CATALOG features_demo;
# MAGIC CREATE SCHEMA IF NOT EXISTS managed_iceberg;
# MAGIC USE SCHEMA managed_iceberg;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Create a Managed Iceberg Table

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS web_analytics;
# MAGIC CREATE TABLE web_analytics (
# MAGIC   session_id STRING,
# MAGIC   user_id BIGINT,
# MAGIC   page_url STRING,
# MAGIC   event_type STRING,
# MAGIC   duration_sec INT,
# MAGIC   device STRING,
# MAGIC   country STRING,
# MAGIC   event_time TIMESTAMP
# MAGIC ) USING iceberg
# MAGIC CLUSTER BY (event_time, country)
# MAGIC TBLPROPERTIES ('format-version' = 3);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify it's a managed Iceberg table
# MAGIC DESCRIBE DETAIL web_analytics;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Insert Data

# COMMAND ----------

from pyspark.sql.functions import expr, lit, rand, when, concat, date_add, from_unixtime
import uuid

df = (spark.range(0, 20000)
    .withColumn("session_id", expr("uuid()"))
    .withColumn("user_id", (rand() * 10000).cast("bigint"))
    .withColumn("page_url",
        when(rand() < 0.3, lit("/home"))
        .when(rand() < 0.5, lit("/products"))
        .when(rand() < 0.7, lit("/cart"))
        .when(rand() < 0.85, lit("/checkout"))
        .otherwise(lit("/profile")))
    .withColumn("event_type",
        when(rand() < 0.5, lit("page_view"))
        .when(rand() < 0.75, lit("click"))
        .when(rand() < 0.9, lit("scroll"))
        .otherwise(lit("purchase")))
    .withColumn("duration_sec", (rand() * 300 + 1).cast("int"))
    .withColumn("device",
        when(rand() < 0.5, lit("mobile"))
        .when(rand() < 0.8, lit("desktop"))
        .otherwise(lit("tablet")))
    .withColumn("country",
        when(rand() < 0.25, lit("US"))
        .when(rand() < 0.45, lit("FR"))
        .when(rand() < 0.6, lit("UK"))
        .when(rand() < 0.75, lit("DE"))
        .when(rand() < 0.85, lit("JP"))
        .otherwise(lit("BR")))
    .withColumn("event_time",
        expr("current_timestamp() - make_interval(0, 0, 0, cast(rand() * 90 as int), 0, 0, 0)"))
    .drop("id")
)

# Write using SQL INSERT for Iceberg tables
df.createOrReplaceTempView("web_data_temp")
spark.sql("INSERT INTO features_demo.managed_iceberg.web_analytics SELECT * FROM web_data_temp")
print(f"Inserted {df.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Query and Analyze

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Page views by country
# MAGIC SELECT
# MAGIC   country,
# MAGIC   event_type,
# MAGIC   COUNT(*) AS event_count,
# MAGIC   AVG(duration_sec) AS avg_duration
# MAGIC FROM web_analytics
# MAGIC GROUP BY country, event_type
# MAGIC ORDER BY country, event_count DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Top pages by conversion (purchase events)
# MAGIC SELECT
# MAGIC   page_url,
# MAGIC   COUNT(*) AS total_events,
# MAGIC   SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) AS purchases,
# MAGIC   ROUND(SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS conversion_rate
# MAGIC FROM web_analytics
# MAGIC GROUP BY page_url
# MAGIC ORDER BY conversion_rate DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. DML Operations

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Update device labels
# MAGIC UPDATE web_analytics SET device = 'Mobile' WHERE device = 'mobile';
# MAGIC UPDATE web_analytics SET device = 'Desktop' WHERE device = 'desktop';
# MAGIC UPDATE web_analytics SET device = 'Tablet' WHERE device = 'tablet';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Delete test data
# MAGIC DELETE FROM web_analytics WHERE duration_sec < 2;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT device, COUNT(*) as cnt FROM web_analytics GROUP BY device ORDER BY cnt DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Automatic Maintenance

# COMMAND ----------

# MAGIC %sql
# MAGIC -- OPTIMIZE is automatically run by Predictive Optimization
# MAGIC -- but you can also run it manually
# MAGIC OPTIMIZE web_analytics;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View maintenance history
# MAGIC DESCRIBE HISTORY web_analytics LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Table properties show maintenance configuration
# MAGIC SHOW TBLPROPERTIES web_analytics;

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **Summary:** Managed Iceberg tables in Unity Catalog give you the full power of Iceberg with Databricks-level governance and maintenance automation. Predictive Optimization handles OPTIMIZE, VACUUM, and ANALYZE automatically, while liquid clustering adapts to your query patterns.
