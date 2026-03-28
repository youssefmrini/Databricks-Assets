-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Delta UniForm
-- MAGIC Universal Format support for cross-engine compatibility.

-- COMMAND ----------

CREATE CATALOG IF NOT EXISTS delta_demo;
USE CATALOG delta_demo;
CREATE SCHEMA IF NOT EXISTS uniform_demo;
USE SCHEMA uniform_demo;

-- COMMAND ----------

-- Create a table with UniForm enabled
CREATE OR REPLACE TABLE uniform_sample (
  id INT, 
  name STRING, 
  value DOUBLE,
  category STRING
)
TBLPROPERTIES (
  'delta.universalFormat.enabledFormats' = 'iceberg'
);

-- COMMAND ----------

INSERT INTO uniform_sample VALUES 
  (1, 'alpha', 10.5, 'A'),
  (2, 'beta', 20.3, 'B'),
  (3, 'gamma', 30.1, 'A'),
  (4, 'delta', 40.7, 'C');

-- COMMAND ----------

SELECT * FROM uniform_sample;

-- COMMAND ----------

-- Check UniForm properties
SHOW TBLPROPERTIES uniform_sample;

-- COMMAND ----------

DESCRIBE DETAIL uniform_sample;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ---
-- MAGIC **Summary:** UniForm enables Delta tables to be read as Iceberg (and Hudi) tables with zero data duplication. External engines access the same Parquet files through Iceberg metadata.
