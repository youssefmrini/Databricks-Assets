-- Databricks notebook source
-- MAGIC %md
-- MAGIC # System Tables - Query History

-- COMMAND ----------

-- Select a subset of useful columns from the system.query.history table
SELECT 
  account_id, 
  workspace_id, 
  executed_by, 
  start_time, 
  execution_status 
FROM 
  system.query.history 
WHERE 
  start_time >= date_sub(current_date(), 30) -- Filter for the last 30 days to improve performance
LIMIT 
  5 -- Limit the number of rows returned

-- COMMAND ----------

-- MAGIC %md
-- MAGIC

-- COMMAND ----------


select * from system.compute.clusters
