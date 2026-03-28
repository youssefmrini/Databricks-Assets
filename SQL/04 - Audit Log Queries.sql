-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Audit Log Queries

-- COMMAND ----------

select * from system.access.audit where service_name= :service_name

-- COMMAND ----------

select user_identity.email,request_params.commandLanguage,sum(request_params.executionTime) duration,request_params.status, month(event_date) month, year(event_date) as year  from system.access.audit where action_name="runCommand" group by all

-- COMMAND ----------

select distinct service_name from system.access.audit 

-- COMMAND ----------

select * from system.access.audit where service_name="notebook" and action_name="runCommand"

-- COMMAND ----------

select user_identity.email,response.status_code, count(*) nbr_catalog, month(event_date) month, year(event_date) as year  from system.access.audit where action_name="createCatalog" group by all 

-- COMMAND ----------

select user_identity.email,response.status_code, count(*) nbr_catalog, month(event_date) month, year(event_date) as year  from system.access.audit where action_name="createTable" group by all 

-- COMMAND ----------

select user_identity.email, count(*) nbr_notebook, month(event_date) month, year(event_date) as year  from system.access.audit where action_name="createNotebook" group by all 

-- COMMAND ----------

select user_identity.email, count(*) nbr_command, month(event_date) month, year(event_date) as year  from system.access.audit where action_name="runCommand" group by all 

-- COMMAND ----------

select distinct action_name from system.access.audit where service_name="clusters"

-- COMMAND ----------

select user_identity.email, count(*) nbr_clusters, month(event_date) month, year(event_date) as year  from system.access.audit where action_name="create" group by all 

-- COMMAND ----------

select user_identity.email, count(*) as nbr_token, month(event_date) as month, year(event_date) as year from system.access.audit where service_name="accounts" and action_name="generateDbToken" group by all

-- COMMAND ----------

select distinct action_name from system.access.audit where service_name="unityCatalog" --action_name="getTable" 