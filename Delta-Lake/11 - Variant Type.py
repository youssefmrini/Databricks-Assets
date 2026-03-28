# Databricks notebook source
# MAGIC %md
# MAGIC # Variant Type

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS youssef_demo;
# MAGIC use catalog youssef_demo;
# MAGIC --CREATE SCHEMA IF NOT EXISTS hello;
# MAGIC use schema hello;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE variant_example (
# MAGIC   id INT,
# MAGIC   data VARIANT
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO variant_example (id, data) VALUES
# MAGIC   (1, PARSE_JSON('{"name": "Alice", "age": 30, "address": {"city": "NY", "zip": "10001"}}')),
# MAGIC   (2, PARSE_JSON('{"name": "Bob", "age": 25, "address": {"city": "SF", "zip": "94105"}}')),
# MAGIC   (3, PARSE_JSON('{"name": "Charlie", "age": 35, "address": {"city": "LA", "zip": "90001"}}'));

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from   variant_example

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   id,
# MAGIC   data:name AS name,
# MAGIC   data:age AS age,
# MAGIC   data:address:city AS city,
# MAGIC   data:address:zip AS zip
# MAGIC FROM variant_example;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE json_example (
# MAGIC   id INT,
# MAGIC   data STRING
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO json_example (id, data) VALUES
# MAGIC   (1, '{"name": "Alice", "age": 30, "address": {"city": "NY", "zip": "10001"}}'),
# MAGIC   (2, '{"name": "Bob", "age": 25, "address": {"city": "SF", "zip": "94105"}}'),
# MAGIC   (3, '{"name": "Charlie", "age": 35, "address": {"city": "LA", "zip": "90001"}}');

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   id,
# MAGIC   get_json_object(data, '$.name') AS name,
# MAGIC   get_json_object(data, '$.age') AS age,
# MAGIC   get_json_object(data, '$.address.city') AS city,
# MAGIC   get_json_object(data, '$.address.zip') AS zip
# MAGIC FROM json_example;