-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Deep Clone and Delta Operations

-- COMMAND ----------

create or replace table  deltademo.sample.raw_clustered deep clone deltademo.sample.raw_table

-- COMMAND ----------

CREATE TABLE deltademo.sample.raw_partitioned
USING DELTA
PARTITIONED BY (FL_DATE)
AS SELECT * FROM deltademo.sample.raw_clustered;

-- COMMAND ----------

alter table deltademo.sample.raw_partitioned cluster by (FL_DATE)

-- COMMAND ----------

optimize deltademo.sample.raw_clustered zorder by (FL_DATE)

-- COMMAND ----------

alter table deltademo.sample.raw_clustered cluster by (FL_DATE)

-- COMMAND ----------

select * from deltademo.sample.raw_clustered 

-- COMMAND ----------

describe extended deltademo.sample.youssef

-- COMMAND ----------

ALTER TABLE deltademo.sample.youssef DROP COLUMN `Unnamed: 27`;
select * from deltademo.sample.youssef