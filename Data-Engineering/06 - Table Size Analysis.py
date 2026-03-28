# Databricks notebook source
# MAGIC %md
# MAGIC # Table Size Analysis

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType
from pyspark.sql.functions import split, col

results = []
catalogs_df = spark.sql("SHOW CATALOGS")
catalogs = [row["catalog"] for row in catalogs_df.collect()]

for catalog in catalogs:
    schemas_df = spark.sql(f"SHOW SCHEMAS IN `{catalog}`")
    schemas = [
        row["databaseName"]
        for row in schemas_df.collect()
        if row["databaseName"].lower() not in ["information_schema"]
    ]
    for schema in schemas:
        tables_df = spark.sql(f"SHOW TABLES IN `{catalog}`.`{schema}`")
        for row in tables_df.collect():
            full_table_name = f"`{catalog}`.`{schema}`.`{row['tableName']}`"
            try:
                detail_df = spark.sql(
                    f"DESCRIBE DETAIL {full_table_name}"
                ).select("sizeInBytes")
                print(full_table_name)
                size = detail_df.collect()[0]["sizeInBytes"]
                results.append((full_table_name, size))
            except Exception:
                continue

if results:
    sizes_df = spark.createDataFrame(
        results,
        ["table_name", "size_in_bytes"]
    )
else:
    schema_struct = StructType([
        StructField("table_name", StringType(), True),
        StructField("size_in_bytes", LongType(), True)
    ])
    sizes_df = spark.createDataFrame([], schema_struct)

sizes_df = sizes_df.withColumn(
    "size_in_mb",
    col("size_in_bytes") / 1024
).withColumn(
    "catalog",
    split(col("table_name"), "[.]")[0]
).withColumn(
    "schema",
    split(col("table_name"), "[.]")[1]
).withColumn(
    "table",
    split(col("table_name"), "[.]")[2]
).drop("table_name", "size_in_bytes")

spark.sql(
    """
    CREATE TABLE IF NOT EXISTS demo_youssef.youssef.table_sizeszs (
        size_in_mb DOUBLE,
        catalog STRING,
        schema STRING,
        table STRING
    )
    """
)