# Databricks notebook source
from pyspark.sql.functions import col
  
df = spark.table('deltademo.sample.delai')
part = df.filter(col('OP_UNIQUE_CARRIER') == 'MQ')
part.write.mode("overwrite").saveAsTable("deltademo.sample.delaidab")