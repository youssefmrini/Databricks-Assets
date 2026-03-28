# Databricks notebook source
# MAGIC %md
# MAGIC # Delta Lake and Iceberg - Neoma

# COMMAND ----------

# MAGIC %sql
# MAGIC create catalog neoma;
# MAGIC use catalog neoma;
# MAGIC create schema demo;
# MAGIC use schema demo;

# COMMAND ----------

# DBTITLE 1,Create Customer Table
# MAGIC %sql
# MAGIC -- Create customer table with sample data
# MAGIC CREATE OR REPLACE TABLE customers (
# MAGIC   customer_id INT,
# MAGIC   first_name STRING,
# MAGIC   last_name STRING,
# MAGIC   email STRING,
# MAGIC   phone STRING,
# MAGIC   city STRING,
# MAGIC   state STRING,
# MAGIC   registration_date DATE,
# MAGIC   total_purchases DECIMAL(10,2),
# MAGIC   is_active BOOLEAN
# MAGIC ) using Delta;
# MAGIC
# MAGIC -- Insert sample customer data
# MAGIC INSERT INTO customers VALUES
# MAGIC   (1, 'John', 'Smith', 'john.smith@email.com', '555-0101', 'New York', 'NY', '2023-01-15', 1250.50, true),
# MAGIC   (2, 'Sarah', 'Johnson', 'sarah.j@email.com', '555-0102', 'Los Angeles', 'CA', '2023-02-20', 890.25, true),
# MAGIC   (3, 'Michael', 'Williams', 'mwilliams@email.com', '555-0103', 'Chicago', 'IL', '2023-03-10', 2100.00, true),
# MAGIC   (4, 'Emily', 'Brown', 'emily.brown@email.com', '555-0104', 'Houston', 'TX', '2023-04-05', 450.75, false),
# MAGIC   (5, 'David', 'Jones', 'djones@email.com', '555-0105', 'Phoenix', 'AZ', '2023-05-12', 1680.30, true),
# MAGIC   (6, 'Jessica', 'Garcia', 'jgarcia@email.com', '555-0106', 'Philadelphia', 'PA', '2023-06-18', 920.00, true),
# MAGIC   (7, 'Daniel', 'Martinez', 'daniel.m@email.com', '555-0107', 'San Antonio', 'TX', '2023-07-22', 1340.60, true),
# MAGIC   (8, 'Ashley', 'Rodriguez', 'ashley.r@email.com', '555-0108', 'San Diego', 'CA', '2023-08-30', 560.40, false),
# MAGIC   (9, 'Christopher', 'Davis', 'cdavis@email.com', '555-0109', 'Dallas', 'TX', '2023-09-14', 2890.75, true);

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended customers

# COMMAND ----------

print('hello')

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create customer table with sample data
# MAGIC CREATE OR REPLACE TABLE customers_iceberg (
# MAGIC   customer_id INT,
# MAGIC   first_name STRING,
# MAGIC   last_name STRING,
# MAGIC   email STRING,
# MAGIC   phone STRING,
# MAGIC   city STRING,
# MAGIC   state STRING,
# MAGIC   registration_date DATE,
# MAGIC   total_purchases DECIMAL(10,2),
# MAGIC   is_active BOOLEAN
# MAGIC ) USING iceberg
# MAGIC TBLPROPERTIES ('format-version' = 3);
# MAGIC
# MAGIC -- Insert sample customer data
# MAGIC INSERT INTO customers VALUES
# MAGIC   (1, 'John', 'Smith', 'john.smith@email.com', '555-0101', 'New York', 'NY', '2023-01-15', 1250.50, true),
# MAGIC   (2, 'Sarah', 'Johnson', 'sarah.j@email.com', '555-0102', 'Los Angeles', 'CA', '2023-02-20', 890.25, true),
# MAGIC   (3, 'Michael', 'Williams', 'mwilliams@email.com', '555-0103', 'Chicago', 'IL', '2023-03-10', 2100.00, true),
# MAGIC   (4, 'Emily', 'Brown', 'emily.brown@email.com', '555-0104', 'Houston', 'TX', '2023-04-05', 450.75, false),
# MAGIC   (5, 'David', 'Jones', 'djones@email.com', '555-0105', 'Phoenix', 'AZ', '2023-05-12', 1680.30, true),
# MAGIC   (6, 'Jessica', 'Garcia', 'jgarcia@email.com', '555-0106', 'Philadelphia', 'PA', '2023-06-18', 920.00, true),
# MAGIC   (7, 'Daniel', 'Martinez', 'daniel.m@email.com', '555-0107', 'San Antonio', 'TX', '2023-07-22', 1340.60, true),
# MAGIC   (8, 'Ashley', 'Rodriguez', 'ashley.r@email.com', '555-0108', 'San Diego', 'CA', '2023-08-30', 560.40, false),
# MAGIC   (9, 'Christopher', 'Davis', 'cdavis@email.com', '555-0109', 'Dallas', 'TX', '2023-09-14', 2890.75, true);

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended customers_iceberg

# COMMAND ----------

# MAGIC %md
# MAGIC ## MERGE Operation Examples
# MAGIC
# MAGIC The **MERGE** statement (also called UPSERT) allows you to:
# MAGIC * **UPDATE** existing records when they match
# MAGIC * **INSERT** new records when they don't match
# MAGIC
# MAGIC This is useful for keeping tables up-to-date with new data!

# COMMAND ----------

# DBTITLE 1,Create Update Data
# MAGIC %sql
# MAGIC -- Create a temporary view with updates and new customers
# MAGIC CREATE OR REPLACE TEMP VIEW customer_updates AS
# MAGIC SELECT * FROM VALUES
# MAGIC   (2, 'Sarah', 'Johnson', 'sarah.j@email.com', '555-0102', 'San Francisco', 'CA', '2023-02-20', 1500.00, true),  -- Updated city and purchases
# MAGIC   (5, 'David', 'Jones', 'djones@email.com', '555-0105', 'Phoenix', 'AZ', '2023-05-12', 2000.00, true),  -- Updated purchases
# MAGIC   (10, 'Amanda', 'Wilson', 'awilson@email.com', '555-0110', 'Seattle', 'WA', '2026-01-20', 750.00, true),  -- New customer
# MAGIC   (11, 'Robert', 'Taylor', 'rtaylor@email.com', '555-0111', 'Boston', 'MA', '2026-01-25', 320.00, true)  -- New customer
# MAGIC AS customer_updates(customer_id, first_name, last_name, email, phone, city, state, registration_date, total_purchases, is_active);

# COMMAND ----------

# DBTITLE 1,MERGE into Delta Table
# MAGIC %sql
# MAGIC -- MERGE updates into the Delta table
# MAGIC MERGE INTO customers AS target
# MAGIC USING customer_updates AS source
# MAGIC ON target.customer_id = source.customer_id
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET
# MAGIC     target.city = source.city,
# MAGIC     target.total_purchases = source.total_purchases
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT *;

# COMMAND ----------

# DBTITLE 1,Verify Delta MERGE Results
# MAGIC %sql
# MAGIC -- Check the results: notice updated cities and new customers
# MAGIC SELECT customer_id, first_name, last_name, city, total_purchases
# MAGIC FROM customers
# MAGIC WHERE customer_id IN (2, 5, 10, 11)
# MAGIC ORDER BY customer_id;

# COMMAND ----------

# DBTITLE 1,MERGE into Iceberg Table
# MAGIC %sql
# MAGIC -- Same MERGE operation works for Iceberg tables
# MAGIC MERGE INTO customers_iceberg AS target
# MAGIC USING customer_updates AS source
# MAGIC ON target.customer_id = source.customer_id
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET
# MAGIC     target.city = source.city,
# MAGIC     target.total_purchases = source.total_purchases
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT *;

# COMMAND ----------

# DBTITLE 1,Verify Iceberg MERGE Results
# MAGIC %sql
# MAGIC -- Verify Iceberg table was updated the same way
# MAGIC SELECT customer_id, first_name, last_name, city, total_purchases
# MAGIC FROM customers_iceberg
# MAGIC WHERE customer_id IN (2, 5, 10, 11)
# MAGIC ORDER BY customer_id;