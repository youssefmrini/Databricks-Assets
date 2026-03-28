# Databricks notebook source
# MAGIC %md
# MAGIC # Iceberg - Odaseva

# COMMAND ----------

# MAGIC %sql
# MAGIC create catalog odaseva;
# MAGIC use catalog odaseva;
# MAGIC create schema hello;
# MAGIC use hello;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Step 1: Create the Iceberg table
# MAGIC CREATE TABLE IF NOT EXISTS customer (
# MAGIC     customer_id INT,
# MAGIC     first_name STRING,
# MAGIC     last_name STRING,
# MAGIC     email STRING,
# MAGIC     phone_number STRING,
# MAGIC     address STRING,
# MAGIC     city STRING,
# MAGIC     state STRING,
# MAGIC     zip_code STRING
# MAGIC ) USING iceberg;
# MAGIC
# MAGIC -- Step 2: Insert fake data into the Iceberg table
# MAGIC INSERT INTO customer (customer_id, first_name, last_name, email, phone_number, address, city, state, zip_code) VALUES
# MAGIC (1, 'John', 'Doe', 'john.doe@example.com', '123-456-7890', '123 Elm St', 'Springfield', 'IL', '62701'),
# MAGIC (2, 'Jane', 'Smith', 'jane.smith@example.com', '234-567-8901', '456 Oak St', 'Metropolis', 'NY', '10001'),
# MAGIC (3, 'Jim', 'Brown', 'jim.brown@example.com', '345-678-9012', '789 Pine St', 'Gotham', 'NJ', '07001'),
# MAGIC (4, 'Jill', 'Johnson', 'jill.johnson@example.com', '456-789-0123', '101 Maple St', 'Star City', 'CA', '90001'),
# MAGIC (5, 'Jack', 'Davis', 'jack.davis@example.com', '567-890-1234', '202 Birch St', 'Central City', 'TX', '73301');

# COMMAND ----------

# MAGIC %sql
# MAGIC describe detail customer

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Step 1: Create a view with new and updated customer data
# MAGIC CREATE OR REPLACE VIEW customer_view AS
# MAGIC SELECT * FROM VALUES
# MAGIC   (1, 'John', 'Doe', 'john.doe@newexample.com', '123-456-7890', '123 Elm St', 'Springfield', 'IL', '62701'), -- Updated email
# MAGIC   (6, 'Alice', 'Williams', 'alice.williams@example.com', '678-901-2345', '303 Cedar St', 'Smallville', 'KS', '66002') -- New customer
# MAGIC AS v(customer_id, first_name, last_name, email, phone_number, address, city, state, zip_code);
# MAGIC
# MAGIC -- Step 2: Perform the merge operation
# MAGIC MERGE INTO customer AS target
# MAGIC USING customer_view AS source
# MAGIC ON target.customer_id = source.customer_id
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET
# MAGIC     target.first_name = source.first_name,
# MAGIC     target.last_name = source.last_name,
# MAGIC     target.email = source.email,
# MAGIC     target.phone_number = source.phone_number,
# MAGIC     target.address = source.address,
# MAGIC     target.city = source.city,
# MAGIC     target.state = source.state,
# MAGIC     target.zip_code = source.zip_code
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (customer_id, first_name, last_name, email, phone_number, address, city, state, zip_code)
# MAGIC   VALUES (source.customer_id, source.first_name, source.last_name, source.email, source.phone_number, source.address, source.city, source.state, source.zip_code);