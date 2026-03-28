# Databricks notebook source
# MAGIC %md
# MAGIC # Collations

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS demo_y;
# MAGIC use catalog demo_y;
# MAGIC --create schema test;
# MAGIC CREATE SCHEMA IF NOT EXISTS test;
use schema test;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE collation_demo (
# MAGIC   id INT,
# MAGIC   name STRING COLLATE UTF8_LCASE,
# MAGIC   city STRING COLLATE UNICODE_CI_AI,
# MAGIC   notes STRING COLLATE UTF8_LCASE_RTRIM
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE collation_name_test (
# MAGIC   id INT, 
# MAGIC   first_name STRING, 
# MAGIC   last_name STRING
# MAGIC   );
# MAGIC
# MAGIC INSERT INTO collation_name_test
# MAGIC VALUES 
# MAGIC (1, 'aaron', 'SMITH'), 
# MAGIC (2, 'Aaron' , 'Michaels'),
# MAGIC (3, 'Àrne' , 'Solar'),
# MAGIC (4, 'bob' , 'mcdonald'),
# MAGIC (5, 'carson' , 'lee'),
# MAGIC (6, 'Carson' , 'Cruise'),
# MAGIC (7, 'Bobby' , 'McCloud'),
# MAGIC (8, 'C.J.' , 'Smith');

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM collation_name_test
# MAGIC ORDER BY first_name;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC ALTER TABLE collation_name_test DEFAULT COLLATION UTF8_LCASE;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC ALTER TABLE collation_name_test  ALTER COLUMN first_name TYPE STRING COLLATE UNICODE_CI_AI;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM collation_name_test
# MAGIC ORDER BY first_name;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC
# MAGIC ALTER TABLE collation_name_test 
# MAGIC ADD COLUMN new_col STRING COLLATE UTF8_LCASE_RTRIM;
# MAGIC
# MAGIC -- COMMAND ----------
# MAGIC
# MAGIC SELECT 'I see trailing spaces' = 'I see trailing spaces    ' COLLATE UNICODE_RTRIM as rtrim_compare;
# MAGIC
# MAGIC -- COMMAND ----------
# MAGIC
# MAGIC DESCRIBE collation_name_test;
# MAGIC
# MAGIC -- COMMAND ----------
# MAGIC
# MAGIC SELECT first_name 
# MAGIC FROM collation_name_test
# MAGIC ORDER BY first_name;
# MAGIC
# MAGIC -- COMMAND ----------
# MAGIC
# MAGIC CREATE OR REPLACE TABLE new_table (
# MAGIC   ID INT,
# MAGIC   name STRING
# MAGIC ) DEFAULT COLLATION UTF8_LCASE;
# MAGIC
# MAGIC -- COMMAND ----------
# MAGIC
# MAGIC INSERT INTO new_table (ID, name) VALUES
# MAGIC (1, 'Alice'),
# MAGIC (2, 'Bob'),
# MAGIC (3, 'Youssef');
# MAGIC
# MAGIC -- COMMAND ----------
# MAGIC
# MAGIC select * 
# MAGIC from new_table
# MAGIC -- where lower(name) = 'youssef'
# MAGIC where name = 'youssef'
# MAGIC
# MAGIC -- COMMAND ----------
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS kent_marten_test_catalog.colschema2 DEFAULT COLLATION UNICODE_CI_AI;
# MAGIC
# MAGIC -- COMMAND ----------
# MAGIC
# MAGIC USE SCHEMA colschema2;
# MAGIC
# MAGIC -- COMMAND ----------
# MAGIC
# MAGIC CREATE OR REPLACE TABLE collation_name_test2 (
# MAGIC   id INT, 
# MAGIC   first_name STRING, 
# MAGIC   last_name STRING
# MAGIC   );
# MAGIC
# MAGIC INSERT INTO collation_name_test2
# MAGIC VALUES 
# MAGIC (1, 'aaron', 'SMITH'), 
# MAGIC (2, 'Aaron' , 'Michaels'),
# MAGIC (3, 'Àrne' , 'Solar'),
# MAGIC (4, 'bob' , 'mcdonald'),
# MAGIC (5, 'carson' , 'lee'),
# MAGIC (6, 'Carson' , 'Cruise'),
# MAGIC (7, 'Bobby' , 'McCloud'),
# MAGIC (8, 'C.J.' , 'Smith');
# MAGIC