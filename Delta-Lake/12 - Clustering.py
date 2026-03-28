# Databricks notebook source
# MAGIC %md
# MAGIC # Clustering

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS youssef;
use catalog youssef;
# MAGIC CREATE SCHEMA IF NOT EXISTS default;
use schema default;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE or replace TABLE customer (
# MAGIC     customer_id INT PRIMARY KEY,
# MAGIC     first_name VARCHAR(50),
# MAGIC     last_name VARCHAR(50),
# MAGIC     email VARCHAR(100),
# MAGIC     created_at TIMESTAMP
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Step 1: Create the customer table and insert fake data
# MAGIC
# MAGIC INSERT INTO customer (customer_id, first_name, last_name, email)
# MAGIC VALUES
# MAGIC (1, 'John', 'Doe', 'john.doe@example.com'),
# MAGIC (2, 'Jane', 'Smith', 'jane.smith@example.com'),
# MAGIC (3, 'Alice', 'Johnson', 'alice.johnson@example.com');
# MAGIC
# MAGIC -- Step 2: Create a Delta table from the customer table
# MAGIC CREATE or replace TABLE   delta_customer
# MAGIC USING DELTA
# MAGIC AS SELECT * FROM customer;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Step 3: Create another table with updates and new data
# MAGIC CREATE or replace TABLE customer_updates (
# MAGIC     customer_id INT,
# MAGIC     first_name VARCHAR(50),
# MAGIC     last_name VARCHAR(50),
# MAGIC     email VARCHAR(100),
# MAGIC     created_at TIMESTAMP
# MAGIC );
# MAGIC
# MAGIC INSERT INTO customer_updates (customer_id, first_name, last_name, email)
# MAGIC VALUES
# MAGIC (2, 'Jane', 'Doe', 'jane.doe@example.com'),  -- Update existing customer
# MAGIC (4, 'Bob', 'Brown', 'bob.brown@example.com'); -- New customer
# MAGIC
# MAGIC -- Step 4: Perform a merge operation
# MAGIC MERGE INTO delta_customer AS target
# MAGIC USING customer_updates AS source
# MAGIC ON target.customer_id = source.customer_id
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET
# MAGIC     target.first_name = source.first_name,
# MAGIC     target.last_name = source.last_name,
# MAGIC     target.email = source.email,
# MAGIC     target.created_at = source.created_at
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (customer_id, first_name, last_name, email, created_at)
# MAGIC   VALUES (source.customer_id, source.first_name, source.last_name, source.email, source.created_at);