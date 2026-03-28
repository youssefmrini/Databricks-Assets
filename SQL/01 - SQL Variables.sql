-- Databricks notebook source
-- MAGIC %md
-- MAGIC # SQL Variables

-- COMMAND ----------


-- A verbose definition of a temporary variable
DECLARE OR REPLACE VARIABLE test INT DEFAULT 17;

-- A dense definition, including derivation of the type from the default expression
DECLARE addresses = named_struct('paris', 'Fauboug Saint Honore', 'number', 128);

-- Referencing a variable
SELECT test, session.addresses.number;


-- COMMAND ----------

-- Setting a single variable
SET VAR test = (SELECT max(c1) FROM VALUES (1), (2) AS t(c1));
SELECT test;

-- COMMAND ----------

SELECT mask(
  'yOuSsefmrini#',
  lowerChar => '1',
  upperChar => '2'
  ) as masked_data

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.sql(
-- MAGIC   "CREATE or replace TABLE  IDENTIFIER(:tbl)(col INT) USING delta",
-- MAGIC   args = {
-- MAGIC     "tbl": "youssefm.mrini.kg"
-- MAGIC   }
-- MAGIC
-- MAGIC
-- MAGIC spark.sql(
-- MAGIC   "SELECT IDENTIFIER(:col) FROM IDENTIFIER(:tbl) limit 1",
-- MAGIC   args = {
-- MAGIC     "col": "email",
-- MAGIC     "tbl": "youssefm.mrini.my_emails"
-- MAGIC   }
-- MAGIC ).show()

-- COMMAND ----------

WITH myvar AS (
  SELECT address.number, named_struct('street', address.street, 'number', 10) AS address
  FROM your_table
)
SELECT myvar, myvar.address
FROM myvar;