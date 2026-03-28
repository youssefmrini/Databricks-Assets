-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Predictive Optimization Demo

-- COMMAND ----------

alter catalog english_football enable predictive optimization;

-- COMMAND ----------

describe catalog extended english_football;

-- COMMAND ----------

use catalog english_football;
alter schema uk_data disable predictive optimization;

-- COMMAND ----------

describe extended english_football.default.hello