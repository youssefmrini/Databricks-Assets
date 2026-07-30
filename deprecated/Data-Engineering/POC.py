# Databricks notebook source
# MAGIC %md
# MAGIC # POC Data Processing

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog demo_youssef;
# MAGIC use schema youssef;
# MAGIC --drop table demos;
# MAGIC CREATE OR REFRESH STREAMING TABLE demos CLUSTER BY (status) AS 
# MAGIC SELECT *
# MAGIC FROM STREAM READ_FILES(
# MAGIC   "/Volumes/demo_youssef/youssef/demo",
# MAGIC   FORMAT => 'csv',
# MAGIC   header => True,
# MAGIC   inferschema => 'true'
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from demos;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC     
# MAGIC --drop table dedup_empty;
# MAGIC CREATE or replace table demo_youssef.youssef.dedup_empty cluster by auto AS SELECT * FROM demo_youssef.youssef.demos WHERE 1=0;

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO demo_youssef.youssef.dedup_empty AS target
# MAGIC USING demo_youssef.youssef.demos AS source
# MAGIC ON target.CustomerID = source.CustomerID
# MAGIC WHEN MATCHED AND (
# MAGIC   target.FirstName <> source.FirstName OR
# MAGIC   target.LastName <> source.LastName OR
# MAGIC   target.Email <> source.Email OR
# MAGIC   target.AccountName <> source.AccountName OR
# MAGIC   target.CreatedDate <> source.CreatedDate OR
# MAGIC   target.LastModifiedDate <> source.LastModifiedDate OR
# MAGIC   target.Status <> source.Status OR
# MAGIC   target.Phone <> source.Phone OR
# MAGIC   target.Address <> source.Address OR
# MAGIC   target._rescued_data <> source._rescued_data 
# MAGIC )
# MAGIC THEN UPDATE SET
# MAGIC   FirstName = source.FirstName,
# MAGIC   LastName = source.LastName,
# MAGIC   Email = source.Email,
# MAGIC   AccountName = source.AccountName,
# MAGIC   CreatedDate = source.CreatedDate,
# MAGIC   LastModifiedDate = source.LastModifiedDate,
# MAGIC   Status = source.Status,
# MAGIC   Phone = source.Phone,
# MAGIC   Address = source.Address,
# MAGIC   _rescued_data = source._rescued_data
# MAGIC WHEN NOT MATCHED THEN INSERT (
# MAGIC   CustomerID,
# MAGIC   FirstName,
# MAGIC   LastName,
# MAGIC   Email,
# MAGIC   AccountName,
# MAGIC   CreatedDate,
# MAGIC   LastModifiedDate,
# MAGIC   Status,
# MAGIC   Phone,
# MAGIC   Address,
# MAGIC   _rescued_data
# MAGIC ) VALUES (
# MAGIC   source.CustomerID,
# MAGIC   source.FirstName,
# MAGIC   source.LastName,
# MAGIC   source.Email,
# MAGIC   source.AccountName,
# MAGIC   source.CreatedDate,
# MAGIC   source.LastModifiedDate,
# MAGIC   source.Status,
# MAGIC   source.Phone,
# MAGIC   source.Address,
# MAGIC   source._rescued_data
# MAGIC );

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC MERGE INTO demo_youssef.youssef.dedup_empty AS target
# MAGIC USING demo_youssef.youssef.demos AS source
# MAGIC ON target.CustomerID = source.CustomerID
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history demo_youssef.youssef.dedup_empty

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from demo_youssef.youssef.dedup_empty