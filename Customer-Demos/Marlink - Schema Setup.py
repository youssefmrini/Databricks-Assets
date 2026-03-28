# Databricks notebook source
# MAGIC %md
# MAGIC # Marlink - Schema Setup

# COMMAND ----------

# MAGIC %sql
# MAGIC create catalog marlink;
# MAGIC use catalog marlink;
# MAGIC create schema demo;
# MAGIC use schema demo;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE customer_data (
# MAGIC     customer_id VARCHAR(36),
# MAGIC     name VARCHAR(100),
# MAGIC     email VARCHAR(100),
# MAGIC     phone_number VARCHAR(20),
# MAGIC     address VARCHAR(255),
# MAGIC     cake_preference VARCHAR(50),
# MAGIC     purchase_date DATE
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE customer_data ADD COLUMN hello VARCHAR(255);

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history customer_data
