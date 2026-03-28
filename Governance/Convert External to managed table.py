# Databricks notebook source
# MAGIC %md
# MAGIC # Convert External to Managed Table

# COMMAND ----------

# MAGIC %sql
# MAGIC drop catalog demo_managed cascade;

# COMMAND ----------

# The 'SET MANAGED' command in Databricks is used to specify that a table is managed by the metastore.
# Managed tables store both the metadata and the data in the metastore's storage location.
# This simplifies data management, as dropping the table also deletes the underlying data.
# Use 'SET MANAGED' when you want Databricks to handle the lifecycle of your table data automatically.

# WHY USE SET MANAGED?
# 1. Simplified Data Lifecycle Management: Databricks automatically handles storage location and cleanup
# 2. Consistent Storage Location: All managed tables are stored in the metastore's default location
# 3. Automatic Cleanup: When you DROP a managed table, both metadata and underlying data files are deleted
# 4. Reduced Storage Management Overhead: No need to manually manage file paths or storage locations
# 5. Better Integration with Unity Catalog: Managed tables work seamlessly with governance features
# 6. Simplified Backup and Recovery: Centralized storage makes backup strategies more straightforward
# 7. Cost Control: Easier to track and manage storage costs when data is centrally managed

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC create catalog demo_managed;
# MAGIC use catalog demo_managed;
# MAGIC create schema test;
# MAGIC use test;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE demo_managed.test.customers
# MAGIC (
# MAGIC   customer_id BIGINT,                -- Unique customer identifier
# MAGIC   first_name STRING,                 -- Customer's first name
# MAGIC   last_name STRING,                  -- Customer's last name
# MAGIC   email STRING,                      -- Email address
# MAGIC   phone STRING,                      -- Phone number
# MAGIC   signup_date DATE,                  -- Date of account creation
# MAGIC   loyalty_points INT,                -- Loyalty program points
# MAGIC   is_active BOOLEAN,                 -- Active status
# MAGIC   country STRING,                    -- Country of residence
# MAGIC   last_purchase_timestamp TIMESTAMP  -- Timestamp of last purchase
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://one-env-uc-external-location/abhisaxena/demoos'
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import expr, monotonically_increasing_id, col, rand, when
from pyspark.sql.types import DateType, TimestampType
import datetime

# Generate 1 million rows of fake customer data
num_rows = 1_000_000
start_date = datetime.date(2020, 1, 1)
end_date = datetime.date(2025, 1, 1)
date_range_days = (end_date - start_date).days

df = (
    spark.range(num_rows)
    .withColumn("customer_id", col("id") + 1)
    .withColumn("first_name", expr("concat('First', id % 1000)"))
    .withColumn("last_name", expr("concat('Last', id % 1000)"))
    .withColumn("email", expr("concat('user', id, '@example.com')"))
    .withColumn("phone", expr("concat('+1-555-', lpad((id % 10000), 4, '0'))"))
    .withColumn("signup_date", expr(f"date_add('{start_date}', cast(rand() * {date_range_days} as int))"))
    .withColumn("loyalty_points", expr("cast(rand() * 10000 as int)"))
    .withColumn("is_active", expr("id % 2 = 0"))
    .withColumn("country", expr("CASE WHEN id % 3 = 0 THEN 'US' WHEN id % 3 = 1 THEN 'UK' ELSE 'IN' END"))
    .withColumn("last_purchase_timestamp", expr(f"timestampadd(DAY, cast(rand() * {date_range_days} as int), '{start_date}')"))
    .drop("id")
)

df.write.format("delta").mode("append").saveAsTable("demo_managed.test.customers")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from demo_managed.test.customers limit 10

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended demo_managed.test.customers
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC UPDATE demo_managed.test.customers
# MAGIC SET loyalty_points = 9999
# MAGIC WHERE customer_id IN (1, 2)

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history demo_managed.test.customers

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE demo_managed.test.customers SET MANAGED;

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended  demo_managed.test.customers

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history  demo_managed.test.customers

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER CATALOG demo_managed enable PREDICTIVE OPTIMIZATION