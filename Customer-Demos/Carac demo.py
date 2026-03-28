# Databricks notebook source
# MAGIC %md
# MAGIC # Carac Demo

# COMMAND ----------

# MAGIC %pip install dbdemos

# COMMAND ----------

import dbdemos
dbdemos.install("lakehouse-retail-c360", catalog="youssefmrini", schema="demo",overwrite=True, path='/Users/youssef.mrini@databricks.com/lakehouse-retail-c360')