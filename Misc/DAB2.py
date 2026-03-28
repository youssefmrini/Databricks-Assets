# Databricks notebook source
from pyspark.sql.functions import col

df = spark.table('deltademo.sample.delaidab')
df.createOrReplaceTempView("page")
part=spark.sql("select count(*), origin from page group by origin")
display(part)