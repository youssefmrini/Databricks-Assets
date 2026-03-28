# Databricks notebook source
# MAGIC %md
# MAGIC # S3 Autoloader Ingestion Template

# COMMAND ----------

# Initialize variables
CATALOG_NAME = "YOUR_CATALOG" # TODO
SCHEMA_NAME = "YOUT_SCHEMA" # TODO
S3_PATH = "YOUR_S3_PATH" # TODO

DATA_TYPE = "data"
ORG_ID = "a0KAU00000HtR2c2AF"

CHECKPOINT_VOLUME_NAME = "checkpoints"
CHECKPOINT_PATH = f"/Volumes/{CATALOG_NAME}/{SCHEMA_NAME}/{CHECKPOINT_VOLUME_NAME}/{DATA_TYPE}_{ORG_ID}_checkpoint"

target_table_name = f"{DATA_TYPE}_{ORG_ID}_dedup"

# COMMAND ----------

from pyspark.sql.functions import col
from delta.tables import DeltaTable

spark.sql(f"USE CATALOG {CATALOG_NAME}")
spark.sql(f"USE SCHEMA {SCHEMA_NAME}")

# Create the volume if it doesn't exist to store the checkpoint files
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG_NAME}.{SCHEMA_NAME}.{CHECKPOINT_VOLUME_NAME}")

# Using Auto Loader for robust and scalable data ingestion from S3
s3_df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("header", "true")
    .option("mergeSchema", "true")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.schemaLocation", CHECKPOINT_PATH)
    .option("delimiter", ",")
    .option("quote", '"')
    .option("escape", '"')
    .load(S3_PATH))
