-- Databricks notebook source
-- MAGIC %md
-- MAGIC # DLT Football Pipeline (SQL)

-- COMMAND ----------



CREATE or refresh STREAMING live TABLE score1990
TBLPROPERTIES ("quality" = "gold", 'delta.columnMapping.mode' = 'name',
   'delta.minReaderVersion' = '2',
   'delta.minWriterVersion' = '5')
AS SELECT *
  FROM cloud_files(
    "/FileStore/tables/england-master/1990s/",
    "csv",
    map("schema","Round int, Date String,`Team 1` String, `Team 2` string")
    
  );

  CREATE or refresh STREAMING live TABLE score2000
TBLPROPERTIES ("quality" = "gold", 'delta.columnMapping.mode' = 'name',
   'delta.minReaderVersion' = '2',
   'delta.minWriterVersion' = '5')
AS SELECT *
  FROM cloud_files(
    "/FileStore/tables/england-master/2000s/",
    "csv",
    map("schema","Round int, Date String,`Team 1` String, `Team 2` string")
  );


CREATE or refresh STREAMING live TABLE score2010
TBLPROPERTIES ("quality" = "gold", 'delta.columnMapping.mode' = 'name',
   'delta.minReaderVersion' = '2',
   'delta.minWriterVersion' = '5')

AS SELECT *
  FROM cloud_files(
    "/FileStore/tables/england-master/2010s/",
    "csv",
    map("schema","Round int, Date String,`Team 1` String, `Team 2` string")

  );





-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC