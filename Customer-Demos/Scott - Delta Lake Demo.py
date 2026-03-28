# Databricks notebook source
# MAGIC %md
# MAGIC # Delta Lake Demo - Scott

# COMMAND ----------

# MAGIC %sql
# MAGIC Create catalog DLSession;
# MAGIC use catalog DLSession;
# MAGIC create schema demo;
# MAGIC use schema demo;
# MAGIC
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS customers (
# MAGIC     customer_id INT,
# MAGIC     first_name STRING,
# MAGIC     last_name STRING,
# MAGIC     email STRING,
# MAGIC     created_at TIMESTAMP
# MAGIC );
# MAGIC
# MAGIC INSERT INTO customers (customer_id, first_name, last_name, email, created_at) VALUES
# MAGIC (1, 'John', 'Doe', 'john.doe@example.com', '2025-03-28 10:00:00'),
# MAGIC (2, 'Jane', 'Smith', 'jane.smith@example.com', '2025-03-28 10:05:00'),
# MAGIC (3, 'Alice', 'Johnson', 'alice.johnson@example.com', '2025-03-28 10:10:00');
# MAGIC

# COMMAND ----------

from delta.tables import DeltaTable
deltaTable = DeltaTable.forName(spark, "DLSession.demo.customers")

# Create a shallow clone of the latest version
deltaTable.clone(target="DLSession.demo.customers_cloned", isShallow=True, replace=False)

# Create a shallow clone of a specific version
deltaTable.cloneAtVersion(version=1, target="DLSession.demo.customers_cloned", isShallow=True, replace=False)

# COMMAND ----------

# MAGIC %sql
# MAGIC --# Create a shallow clone of the latest version
# MAGIC CREATE TABLE DLSession.demo.customers_clone SHALLOW CLONE DLSession.demo.customers;
# MAGIC --# Create a shallow clone of a specific version
# MAGIC CREATE TABLE DLSession.demo.customers_clonev1 SHALLOW CLONE DLSession.demo.customers version as of 1;
# MAGIC
# MAGIC

# COMMAND ----------

from delta.tables import DeltaTable
deltaTable = DeltaTable.forName(spark, "DLSession.demo.customers")

# Create a shallow clone of the latest version
deltaTable.clone(target="DLSession.demo.customers_cloned", isShallow=False, replace=False)

# Create a shallow clone of a specific version
deltaTable.cloneAtVersion(version=1, target="DLSession.demo.customers_cloned", isShallow=False, replace=False)

# COMMAND ----------

# MAGIC %sql
# MAGIC --# Create a shallow clone of the latest version
# MAGIC CREATE TABLE DLSession.demo.customers_clone deep CLONE DLSession.demo.customers;
# MAGIC --# Create a shallow clone of a specific version
# MAGIC CREATE TABLE DLSession.demo.customers_clonev1 deep CLONE DLSession.demo.customers version as of 1;
# MAGIC
# MAGIC