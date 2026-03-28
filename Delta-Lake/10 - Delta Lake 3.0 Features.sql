-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Delta Lake 3.0 Features

-- COMMAND ----------

--create catalog deltademo;
use catalog deltademo;
--create schema sample;
use schema sample;
select * from delays

-- COMMAND ----------

describe extended delays

-- COMMAND ----------

ALTER TABLE delays CLUSTER BY (FL_DATE)

-- COMMAND ----------

optimize delays

-- COMMAND ----------

select * from delays where FL_DATE="2019-01-01"

-- COMMAND ----------

-- Select all columns from the "delays" table where the FL_DATE column has a value of "2019-01-01"
select * 
from delays 
where FL_DATE="2019-01-01";


-- COMMAND ----------

select * from delai;

-- COMMAND ----------

 create or replace table delay_agg as select op_unique_carrier, dest, sum(dep_delay) as total_delay from delai group by OP_UNIQUE_CARRIER,DEST


-- COMMAND ----------

 create or replace table delay_agg as select op_unique_carrier, dest, sum(dep_delay) as total_delay from delai group by OP_UNIQUE_CARRIER
