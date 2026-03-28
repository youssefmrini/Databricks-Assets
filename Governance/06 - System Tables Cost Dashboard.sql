-- Databricks notebook source
-- MAGIC %md
-- MAGIC # System Tables - Cost Dashboard

-- COMMAND ----------

-- MAGIC %md
-- MAGIC <h2> Cost per SKU per Month </H2>

-- COMMAND ----------

SELECT sku_name, month(usage_date) as month_usage, year(usage_date) as year_usage, bround(SUM(usage_quantity),1) as DBUs
FROM system.billing.usage
GROUP BY sku_name, month(usage_date), year(usage_date)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC <h2> Daily Consumption </H2>

-- COMMAND ----------

SELECT usage_date as `Date`, bround(SUM(usage_quantity),1) as DBUs, sku_name
  FROM system.billing.usage
GROUP BY usage_date,sku_name
ORDER BY usage_date,sku_name ASC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC <H2> Cost per tag </H2>

-- COMMAND ----------

select custom_tags.Owner , bround(SUM(usage_quantity),1) DBUs from system.billing.usage where custom_tags.Owner is not null 
group by custom_tags.Owner order by DBUs desc;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC <H2> Usage </H2>

-- COMMAND ----------

select user_identity.email,request_params.commandLanguage, request_params.executionTime, request_params.status from system.access.audit where  service_name="notebook" and action_name="runCommand" 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC <H2> Cost per tag </H2>

-- COMMAND ----------

SELECT user_identity.email, SUM(request_params.executionTime) AS duration 
FROM system.access.audit 
WHERE service_name = "notebook" AND action_name = "runCommand" 
GROUP BY user_identity.email order by duration desc limit 10

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC <H2> top 10 users who execute long running code </H2>

-- COMMAND ----------

SELECT user_identity.email, SUM(request_params.executionTime) AS duration 
FROM system.access.audit 
WHERE service_name = "notebook" AND action_name = "runCommand" 
GROUP BY user_identity.email order by duration desc limit 10

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC <H2> Job metrics </H2>

-- COMMAND ----------

select user_identity.email,request_params.CreatorUserName ,request_params.jobId,request_params.jobTriggerType from system.access.audit where service_name="jobs" and action_name="runSucceeded"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC <H2> Table Access </h2>

-- COMMAND ----------

select event_date, user_identity.email ,request_params.full_name_arg,request_params.workspace_id,action_name from system.access.audit where service_name="unityCatalog" and   action_name
    IN ('createTable','getTable','deleteTable')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC <H2> Table Info </h2>

-- COMMAND ----------

select * from    system.information_schema.tables where table_type not in ("VIEW")

-- COMMAND ----------

SELECT
   *
FROM system.information_schema.tables


-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC <h2> Top 10 Shared Catalogs<H2>

-- COMMAND ----------

select count(*) iteration, catalog_name from system.information_schema.table_share_usage group by catalog_name order by iteration desc limit 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC <h2> Add user to the account console </H2>

-- COMMAND ----------

select user_identity.email, event_time  from system.access.audit where audit_level="ACCOUNT_LEVEL" and service_name="accounts" and action_name="add" order by event_time desc limit 15;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC <H2> Login to the account console </H2>

-- COMMAND ----------

select user_identity.email, count(*) logs from system.access.audit where audit_level="ACCOUNT_LEVEL" and service_name="accounts" and action_name="login" group by user_identity.email order by logs desc limit 15;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC <H2> Interactive Cost </H2>

-- COMMAND ----------

SELECT 
  MONTH(usage_date) AS nbr_month,
  SUM(usage_quantity) AS DBUs,
  sku_name
FROM 
  system.billing.usage 
WHERE 
  sku_name IN ('ENTERPRISE_ALL_PURPOSE_COMPUTE_(PHOTON)', 'ENTERPRISE_ALL_PURPOSE_COMPUTE', 'STANDARD_ALL_PURPOSE_COMPUTE', 'STANDARD_ALL_PURPOSE_COMPUTE_(PHOTON)') 
GROUP BY 
  sku_name, nbr_month;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC <H2> Job Cost </H2>

-- COMMAND ----------


SELECT 
  MONTH(usage_date) AS nbr_month,
  SUM(usage_quantity) AS DBUs,
  sku_name
FROM 
  system.billing.usage 
WHERE 
  sku_name IN ('ENTERPRISE_JOBS_COMPUTE', 'ENTERPRISE_JOBS_COMPUTE_(PHOTON)','ENTERPRISE_JOBS_SERVERLESS_COMPUTE_US_WEST_OREGON') 
GROUP BY 
  sku_name, nbr_month;
  

-- COMMAND ----------

-- MAGIC %md
-- MAGIC <h2> Inference Cost </H2>

-- COMMAND ----------

SELECT 
  MONTH(usage_date) AS nbr_month,
  SUM(usage_quantity) AS DBUs,
  sku_name
FROM 
  system.billing.usage 
WHERE 
  sku_name IN ('ENTERPRISE_SERVERLESS_REAL_TIME_INFERENCE_LAUNCH_US_WEST_OREGON', 'ENTERPRISE_SERVERLESS_REAL_TIME_INFERENCE_US_EAST_OHIO
','ENTERPRISE_SERVERLESS_REAL_TIME_INFERENCE_US_WEST_OREGON','ENTERPRISE_SERVERLESS_SQL_COMPUTE_US_EAST_OHIO
','ENTERPRISE_SERVERLESS_REAL_TIME_INFERENCE_LAUNCH_US_EAST_OHIO
') 
GROUP BY 
  sku_name, nbr_month;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC <h2> DLT Cost </H2>

-- COMMAND ----------

SELECT 
  MONTH(usage_date) AS nbr_month,
  SUM(usage_quantity) AS DBUs,
  sku_name
FROM 
  system.billing.usage 
WHERE 
  sku_name IN ('ENTERPRISE_DLT_CORE_COMPUTE_(PHOTON)', 'ENTERPRISE_DLT_PRO_COMPUTE_(PHOTON)','ENTERPRISE_DLT_CORE_COMPUTE','ENTERPRISE_DLT_ADVANCED_COMPUTE','ENTERPRISE_DLT_PRO_COMPUTE') 
GROUP BY 
  sku_name, nbr_month;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC <h2> SQL Cost </H2>

-- COMMAND ----------

SELECT 
  MONTH(usage_date) AS nbr_month,
  SUM(usage_quantity) AS DBUs,
  sku_name
FROM 
  system.billing.usage 
WHERE 
  sku_name IN ('ENTERPRISE_SQL_PRO_COMPUTE_US_WEST_OREGON', 'ENTERPRISE_SQL_COMPUTE','ENTERPRISE_SQL_PRO_COMPUTE_US_EAST_OHIO','ENTERPRISE_SERVERLESS_SQL_COMPUTE_US_EAST_OHIO')
GROUP BY 
  sku_name, nbr_month;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC <h2> Number of unique users </H2>

-- COMMAND ----------

SELECT count(distinct user_identity.email) as unique_users
FROM system.access.audit 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC <h2> Users who creates the most notebooks </h2>

-- COMMAND ----------

SELECT user_identity.email, count(*) as Nbr
FROM system.access.audit 
WHERE service_name = "notebook"  and action_name="createNotebook" group by user_identity.email order by nbr desc limit 10
