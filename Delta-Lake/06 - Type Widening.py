# Databricks notebook source
# MAGIC %md
# MAGIC # Type Widening

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Cleanup: drop catalog youssef_demo cascade;
# MAGIC CREATE CATALOG IF NOT EXISTS youssef_demo;
# MAGIC use catalog youssef_demo;
# MAGIC CREATE SCHEMA IF NOT EXISTS hello;
# MAGIC use schema hello;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS type_widen_demo (
# MAGIC   id INT,
# MAGIC   value INT
# MAGIC ) USING DELTA
# MAGIC TBLPROPERTIES ('delta.enableTypeWidening' = 'false');

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS type_widen_demos (
# MAGIC   id INT,
# MAGIC   value INT
# MAGIC ) USING DELTA
# MAGIC TBLPROPERTIES ('delta.enableTypeWidening' = 'true')

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO youssef_demo.hello.type_widen_demos (id, value) VALUES (1, 100), (2, 200);
# MAGIC INSERT INTO youssef_demo.hello.type_widen_demo (id, value) VALUES (1, 100), (2, 200);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- This will fail because 'value' is INT
# MAGIC INSERT INTO type_widen_demos (id, value) VALUES (3, 123.45);
# MAGIC INSERT INTO type_widen_demo (id, value) VALUES (3, 123.45);

# COMMAND ----------

# MAGIC %sql
# MAGIC --select * from type_widen_demos;
# MAGIC select * from type_widen_demo;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC ALTER TABLE type_widen_demos ALTER COLUMN id TYPE double;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC ALTER TABLE type_widen_demo ALTER COLUMN id TYPE double;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO youssef_demo.hello.type_widen_demos (id, value) VALUES (3.24, 123);

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM youssef_demo.hello.type_widen_demos;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS demo_partition (
# MAGIC   id INT,
# MAGIC   value STRING,
# MAGIC   type string
# MAGIC ) USING DELTA
# MAGIC PARTITIONED BY (type);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS demo_partition (
# MAGIC   id INT,
# MAGIC   value STRING,
# MAGIC   type string
# MAGIC ) USING DELTA
# MAGIC PARTITIONED BY (value);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS demo_liquid (
# MAGIC   id INT,
# MAGIC   value STRING,
# MAGIC   type string
# MAGIC ) USING DELTA
# MAGIC CLUSTER BY (type)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC ALTER TABLE demo_liquid CLUSTER BY (value)
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE or replace TABLE userss (user_id float) TBLPROPERTIES('delta.enableTypeWidening' = 'true');
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE userss ALTER COLUMN user_id TYPE LONG;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO userss VALUES (123.45);

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from userss

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended users