# Databricks notebook source
# MAGIC %md
# MAGIC # Insert Sample Data

# COMMAND ----------

import datetime
from pyspark.sql.functions import expr, col, rand

# Generate 20,000 rows of fake customer data
num_rows = 20_000
start_date = datetime.date(2020, 1, 1)
end_date = datetime.date(2025, 1, 1)
date_range_days = (end_date - start_date).days

df_20k = (
    spark.range(num_rows)
    .withColumn("customer_id", col("id") + 1_000_001)  # Avoid overlap with existing IDs
    .withColumn("first_name", expr("concat('First', id % 1000)"))
    .withColumn("last_name", expr("concat('Last', id % 1000)"))
    .withColumn("email", expr("concat('user', id + 1_000_000, '@example.com')"))
    .withColumn("phone", expr("concat('+1-555-', lpad((id % 10000), 4, '0'))"))
    .withColumn("signup_date", expr(f"date_add('{start_date}', cast(rand() * {date_range_days} as int))"))
    .withColumn("loyalty_points", expr("cast(rand() * 10000 as int)"))
    .withColumn("is_active", expr("id % 2 = 0"))
    .withColumn("country", expr("CASE WHEN id % 3 = 0 THEN 'US' WHEN id % 3 = 1 THEN 'UK' ELSE 'IN' END"))
    .withColumn("last_purchase_timestamp", expr(f"timestampadd(DAY, cast(rand() * {date_range_days} as int), '{start_date}')"))
    .drop("id")
)

df_20k.write.format("delta").mode("append").saveAsTable("demo_managed.test.customers")