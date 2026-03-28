-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Shallow Clone

-- COMMAND ----------

CREATE CATALOG IF NOT EXISTS clone;
use catalog clone;
CREATE SCHEMA IF NOT EXISTS test;
use test;

-- COMMAND ----------

CREATE OR REPLACE TABLE people (
  id INT,
  name STRING,
  age INT
) using delta 
location "s3://one-env-uc-external-location/aaboode/test"


-- COMMAND ----------

INSERT INTO people (id, name, age)
VALUES (1, 'John Doe', 25),
       (2, 'Jane Smith', 30),
       (3, 'Bob Johnson', 45),
       (4, 'Sara Wilson', 20);

-- COMMAND ----------

-- Shallow clone creates a managed table reference
CREATE OR REPLACE TABLE youssef_people_clone SHALLOW CLONE people

-- COMMAND ----------

select * from clone.test.youssef_people