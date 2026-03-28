# Databricks notebook source
# MAGIC %md
# MAGIC # Primary and Foreign Keys

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC
# MAGIC ALTER TABLE youssef_sarl.demo.customerss
# MAGIC SET TBLPROPERTIES (
# MAGIC     'delta.logRetentionDuration' = '30 days',
# MAGIC     'delta.checkpointPolicy' = 'classic'
# MAGIC );
# MAGIC
# MAGIC describe detail youssef_sarl.demo.customerss;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE youssef_sarl.demo.customers (
# MAGIC     customer_id INT,
# MAGIC     first_name STRING,
# MAGIC     last_name STRING,
# MAGIC     email STRING,
# MAGIC     phone_number STRING,
# MAGIC     address STRING,
# MAGIC     city STRING,
# MAGIC     state STRING,
# MAGIC     zip_code STRING,
# MAGIC     country STRING
# MAGIC );
# MAGIC ALTER TABLE youssef_sarl.demo.customers
# MAGIC SET TBLPROPERTIES (
# MAGIC     'delta.appendOnly' = 'true',
# MAGIC     'delta.logRetentionDuration' = '30 days',
# MAGIC     'delta.checkpointPolicy' = 'classic'
# MAGIC );
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC describe detail youssef_sarl.demo.customerss;

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE youssef_sarl.demo.customerss
# MAGIC UNSET TBLPROPERTIES ('delta.checkpointPolicy');
# MAGIC Describe table extended youssef_sarl.demo.customerss as json