# Databricks notebook source
# MAGIC %md
# MAGIC # Autoloader Ingestion and Merge

# COMMAND ----------



# COMMAND ----------

from delta.tables import DeltaTable

dbutils.widgets.text("customer_name","")
catalog = dbutils.widgets.get("customer_name")           
schema = "bronze"                   


def ingest_and_merge_customer_data(customer_name):
    """
    Incrementally ingests data from a Databricks Volume using Autoloader and merges it into a Delta table.
    """

    volume_path = f"/Volumes/{catalog}/{schema}/{catalog}"
    data_path = f"/Volumes/{catalog}/{schema}/{catalog}/data/"
    checkpoint_dir = f"{volume_path}/checkpoints/"

    print(checkpoint_dir)
    print(data_path)

    # Read the streaming data into a DataFrame
    streaming_df = (spark.readStream
                    .format("cloudFiles")
                    .option("cloudFiles.format", "csv")
                    .option("mergeSchema", "true")
                    .option("header", "true")
                    .option("cloudFiles.inferColumnTypes", "true")
                    .option("cloudFiles.schemaLocation", checkpoint_dir)
                    .load(data_path)
                   )

    # Define the target Delta table path
    delta_table_path = f"{catalog}/{schema}/{catalog}"

    # Assume the Delta table exists and perform the merge
    delta_table = DeltaTable.forPath(spark, delta_table_path)
