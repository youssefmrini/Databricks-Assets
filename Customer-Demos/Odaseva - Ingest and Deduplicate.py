# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest and Deduplicate - Odaseva

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC --CREATE CATALOG IF NOT EXISTS odasevas;
# MAGIC USE CATALOG odasevas;
# MAGIC --CREATE SCHEMA IF NOT EXISTS test;
# MAGIC USE SCHEMA test;
# MAGIC --CREATE VOLUME IF NOT EXISTS demo;

# COMMAND ----------

# Initialize variables
CATALOG_NAME = "odasevas" # TODO
SCHEMA_NAME = "test" # TODO
S3_PATH = "/Volumes/youssef_demos/test/storages" # TODO

DATA_TYPE = "data"
ORG_ID = "a0KAU00000HtR2c2AF"

CHECKPOINT_VOLUME_NAME = "checkpoints"
CHECKPOINT_PATH = f"/Volumes/{CATALOG_NAME}/{SCHEMA_NAME}/{CHECKPOINT_VOLUME_NAME}/{DATA_TYPE}__{ORG_ID}_checkpoint"



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

# COMMAND ----------

# Temporary code to create an empty table and infer the schema
sample_df = (spark.read
                 .format("csv")
                 .option("header", "true")
                 .option("inferSchema", "true")
                 .load(S3_PATH).limit(0))
(sample_df
  .write
  .format("delta")
  .mode("overwrite")
  .option("clusterByAuto", "true") # This enables Liquid clustering
  .option("mergeSchema", "true")
  .saveAsTable(target_table_name)
)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS youssef_demos.test.customer_demo (
# MAGIC   CustomerID STRING NOT NULL PRIMARY KEY RELY,
# MAGIC   FirstName STRING,
# MAGIC   LastName STRING,
# MAGIC   Email STRING,
# MAGIC   AccountName STRING,
# MAGIC   CreatedDate STRING,
# MAGIC   LastModifiedDate STRING,
# MAGIC   Status STRING,
# MAGIC   Phone STRING,
# MAGIC   Address STRING
# MAGIC ) using delta 
# MAGIC CLUSTER BY AUTO

# COMMAND ----------

# --- Perform Merge (Upsert) operation into the Delta table ---
# This will insert new records and update existing ones based on 'recordid'
def merge_micro_batch(micro_batch_df, batch_id):
    """
    Merges a micro-batch from the stream into the target Delta table.
    This function handles the deduplication logic and initial table creation.
    """
    
    delta_target = DeltaTable.forName(spark, "youssef_demos.test.customer_demo")
    (delta_target.alias("target")
        .merge(micro_batch_df.alias("source"), "target.CustomerID = source.CustomerID")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute())
        
query = (s3_df.writeStream
    .foreachBatch(merge_micro_batch)
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(availableNow=True)
    .start())

query.awaitTermination()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from youssef_demos.test.customer_demo

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from youssef_demos.test.customer_demo version as of 3

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from youssef_demos.test.customer_demo version as of 2