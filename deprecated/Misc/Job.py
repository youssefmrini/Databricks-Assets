# Databricks notebook source
# MAGIC %md
# MAGIC # Job Task Values

# COMMAND ----------


x=dbutils.jobs.taskValues.set(key = 'name', value = 'youssef')

#dbutils.jobs.taskValues.set(key = "age", value = 30)

# COMMAND ----------

dbutils.widgets.dropdown("state", "CA", ["CA", "IL", "MI", "NY", "OR", "VA"])


# COMMAND ----------

getArgument("state")

# COMMAND ----------

dbutils.widgets.get("state")