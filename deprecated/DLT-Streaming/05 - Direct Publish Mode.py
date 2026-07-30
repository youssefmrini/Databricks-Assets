# Databricks notebook source
# MAGIC %md
# MAGIC # Direct Publish Mode

# COMMAND ----------

from pyspark.sql.functions import col
import dlt

@dlt.table(name="users.youssef_mrini.sales")
def sales():
  return (
    spark.readStream.format("cloudFiles")
      .option("cloudFiles.format", "csv")
      .load("/Volumes/youssef_sarl/demo/data")
  )




# COMMAND ----------

@dlt.table(name="youssef_sarl.sales.motorcyles")
def func():
  df=spark.read.table("users.youssef_mrini.sales").where(col("PRODUCTLINE")=="Motorcycles")
  return df
