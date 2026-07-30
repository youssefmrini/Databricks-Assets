# Databricks notebook source
# MAGIC %md
# MAGIC # Deploy Online Table

# COMMAND ----------

# MAGIC %pip install dbdemos
# MAGIC

# COMMAND ----------

import dbdemos
dbdemos.install('feature-store', catalog='main', schema='dbdemos_fs_travel')