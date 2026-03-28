# Databricks notebook source
username = dbutils.widgets.text("username", "")

# COMMAND ----------

#spark.sql(f"create catalog movie_{username}");
spark.sql(f"use catalog movie_{username}");
spark.sql(f"create database if not exists info; use info");

# COMMAND ----------

# MAGIC %md
# MAGIC # IMDB [Dataset](https://developer.imdb.com/non-commercial-datasets/)

# COMMAND ----------

