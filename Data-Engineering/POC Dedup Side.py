# Databricks notebook source
# MAGIC %md
# MAGIC # POC Dedup Streaming

# COMMAND ----------

# DBTITLE 1,Create streaming table demos with inferred schema
# MAGIC %sql
# MAGIC -- Set the current catalog and schema
# MAGIC use catalog demo_youssef;
# MAGIC use schema youssef;
# MAGIC
# MAGIC -- Drop the demos table if it exists
# MAGIC --drop table demos;
# MAGIC
# MAGIC -- Create or refresh a streaming table 'demos' clustered by 'status'
# MAGIC CREATE OR REFRESH STREAMING TABLE demos CLUSTER BY (status) AS 
# MAGIC SELECT *
# MAGIC FROM STREAM READ_FILES(
# MAGIC   "/Volumes/demo_youssef/youssef/demo",
# MAGIC   FORMAT => 'csv',
# MAGIC   header => True,
# MAGIC   inferschema => 'true'
# MAGIC );

# COMMAND ----------

# DBTITLE 1,View all records from demos table
# MAGIC %sql
# MAGIC -- Select all records from the demos streaming table
# MAGIC select * from demos;

# COMMAND ----------

# DBTITLE 1,Create empty dedup table with demos schema
# MAGIC %sql
# MAGIC -- Create an empty dedup_empty table with the same schema as demos, clustered automatically
# MAGIC CREATE OR REPLACE TABLE demo_youssef.youssef.dedup CLUSTER BY AUTO AS
# MAGIC SELECT * FROM demo_youssef.youssef.demos WHERE 1=0;

# COMMAND ----------

# DBTITLE 1,Merge demos data into dedup by CustomerID
# MAGIC %sql
# MAGIC -- Merge data from demos into dedup based on CustomerID
# MAGIC MERGE INTO demo_youssef.youssef.dedup AS target
# MAGIC USING demo_youssef.youssef.demos AS source
# MAGIC ON target.CustomerID = source.CustomerID
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history demo_youssef.youssef.dedup