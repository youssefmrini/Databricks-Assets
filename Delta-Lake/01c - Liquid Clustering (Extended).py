# Databricks notebook source
# MAGIC %md
# MAGIC # Liquid Clustering - Extended

# COMMAND ----------

# MAGIC %sql
# MAGIC drop catalog demo_youssefM cascade;
# MAGIC create catalog demo_youssefM;
# MAGIC use catalog demo_youssefM;
# MAGIC create schema delta;
# MAGIC use schema delta;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **Why use Liquid Clustering over Partitioning?**
# MAGIC
# MAGIC - **Flexibility:** Easily change clustering keys without rewriting or migrating data. Partitioning requires table recreation for key changes.
# MAGIC - **Performance:** Liquid clustering adapts to query patterns, optimizing data layout for faster queries. Partitioning is static and may become suboptimal as workloads evolve.
# MAGIC - **Simplicity:** No need to design complex partition schemes. Liquid clustering automatically manages data organization.
# MAGIC - **Compatibility:** Works with both batch and streaming tables, and supports evolving analytic needs.
# MAGIC - **No Data Rewrite:** Changing clustering keys is instant; partitioning changes require full data rewrite.
# MAGIC
# MAGIC > Liquid clustering is recommended for most Delta Lake tables to simplify management and optimize performance as your data and queries change.

# COMMAND ----------

# DBTITLE 1,Create Liquid Clustered Customers Table
# MAGIC %sql
# MAGIC -- Create liquid clustering table
# MAGIC CREATE TABLE customers_clustered (
# MAGIC     customer_id BIGINT,
# MAGIC     first_name STRING,
# MAGIC     last_name STRING,
# MAGIC     email STRING,
# MAGIC     phone STRING,
# MAGIC     registration_date DATE,
# MAGIC     total_orders INT,
# MAGIC     total_spent DECIMAL(10,2),
# MAGIC     customer_segment STRING,
# MAGIC     region STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC CLUSTER BY (customer_id)  -- Initial clustering key
# MAGIC TBLPROPERTIES (
# MAGIC     'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC     'delta.autoOptimize.autoCompact' = 'true'
# MAGIC );

# COMMAND ----------

# DBTITLE 1,Create Simple Partitioned Customers Table
# MAGIC %sql
# MAGIC -- Create traditional partitioned table with different name
# MAGIC CREATE TABLE customers_partitioned_v2 (
# MAGIC     customer_id BIGINT,
# MAGIC     first_name STRING,
# MAGIC     last_name STRING,
# MAGIC     email STRING,
# MAGIC     phone STRING,
# MAGIC     registration_date DATE,
# MAGIC     total_orders INT,
# MAGIC     total_spent DECIMAL(10,2),
# MAGIC     customer_segment STRING,
# MAGIC     region STRING  -- This will be our partition key
# MAGIC )
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (region);

# COMMAND ----------

# DBTITLE 1,Insert Sample Data into Both Tables
# MAGIC %sql
# MAGIC -- Insert sample customer data into both tables
# MAGIC INSERT INTO customers_partitioned_v2 VALUES
# MAGIC (1, 'John', 'Doe', 'john.doe@email.com', '555-0101', '2023-01-15', 5, 1250.00, 'Premium', 'North America'),
# MAGIC (2, 'Jane', 'Smith', 'jane.smith@email.com', '555-0102', '2023-02-20', 3, 750.50, 'Standard', 'Europe'),
# MAGIC (3, 'Bob', 'Johnson', 'bob.johnson@email.com', '555-0103', '2023-03-10', 8, 2100.75, 'Premium', 'Asia'),
# MAGIC (4, 'Alice', 'Brown', 'alice.brown@email.com', '555-0104', '2023-01-25', 2, 450.25, 'Basic', 'North America'),
# MAGIC (5, 'Charlie', 'Wilson', 'charlie.wilson@email.com', '555-0105', '2023-04-05', 6, 1800.00, 'Premium', 'Europe'),
# MAGIC (6, 'Diana', 'Davis', 'diana.davis@email.com', '555-0106', '2023-02-14', 4, 950.30, 'Standard', 'Asia'),
# MAGIC (7, 'Frank', 'Miller', 'frank.miller@email.com', '555-0107', '2023-05-12', 7, 1650.80, 'Premium', 'North America'),
# MAGIC (8, 'Grace', 'Taylor', 'grace.taylor@email.com', '555-0108', '2023-03-28', 1, 200.00, 'Basic', 'Europe'),
# MAGIC (9, 'Henry', 'Anderson', 'henry.anderson@email.com', '555-0109', '2023-06-18', 9, 2750.45, 'Premium', 'Asia'),
# MAGIC (10, 'Ivy', 'Thomas', 'ivy.thomas@email.com', '555-0110', '2023-04-22', 3, 680.90, 'Standard', 'North America');

# COMMAND ----------

# DBTITLE 1,Insert Same Data into Clustering Table
# MAGIC %sql
# MAGIC -- Insert the same sample data into the liquid clustering table
# MAGIC INSERT INTO customers_clustered VALUES
# MAGIC (1, 'John', 'Doe', 'john.doe@email.com', '555-0101', '2023-01-15', 5, 1250.00, 'Premium', 'North America'),
# MAGIC (2, 'Jane', 'Smith', 'jane.smith@email.com', '555-0102', '2023-02-20', 3, 750.50, 'Standard', 'Europe'),
# MAGIC (3, 'Bob', 'Johnson', 'bob.johnson@email.com', '555-0103', '2023-03-10', 8, 2100.75, 'Premium', 'Asia'),
# MAGIC (4, 'Alice', 'Brown', 'alice.brown@email.com', '555-0104', '2023-01-25', 2, 450.25, 'Basic', 'North America'),
# MAGIC (5, 'Charlie', 'Wilson', 'charlie.wilson@email.com', '555-0105', '2023-04-05', 6, 1800.00, 'Premium', 'Europe'),
# MAGIC (6, 'Diana', 'Davis', 'diana.davis@email.com', '555-0106', '2023-02-14', 4, 950.30, 'Standard', 'Asia'),
# MAGIC (7, 'Frank', 'Miller', 'frank.miller@email.com', '555-0107', '2023-05-12', 7, 1650.80, 'Premium', 'North America'),
# MAGIC (8, 'Grace', 'Taylor', 'grace.taylor@email.com', '555-0108', '2023-03-28', 1, 200.00, 'Basic', 'Europe'),
# MAGIC (9, 'Henry', 'Anderson', 'henry.anderson@email.com', '555-0109', '2023-06-18', 9, 2750.45, 'Premium', 'Asia'),
# MAGIC (10, 'Ivy', 'Thomas', 'ivy.thomas@email.com', '555-0110', '2023-04-22', 3, 680.90, 'Standard', 'North America');

# COMMAND ----------

# DBTITLE 1,Show Current Clustering Information
# MAGIC %sql
# MAGIC -- Check current clustering information for the liquid clustering table
# MAGIC DESCRIBE EXTENDED customers_clustered;

# COMMAND ----------

# DBTITLE 1,Easy Clustering Key Change - Step 1
# MAGIC %sql
# MAGIC -- LIQUID CLUSTERING: Change clustering key from customer_id to region (VERY EASY!)
# MAGIC -- This is a simple ALTER TABLE command that changes the clustering strategy
# MAGIC ALTER TABLE customers_clustered CLUSTER BY (region);

# COMMAND ----------

# DBTITLE 1,Verify New Clustering Key
# MAGIC %sql
# MAGIC -- Verify the clustering key has been changed
# MAGIC DESCRIBE EXTENDED customers_clustered;

# COMMAND ----------

# DBTITLE 1,Change to Multiple Clustering Keys
# MAGIC %sql
# MAGIC
# MAGIC ALTER TABLE customers_clustered CLUSTER BY (region, customer_segment);

# COMMAND ----------

# DBTITLE 1,Show Current Partition Information
# MAGIC %sql
# MAGIC
# MAGIC DESCRIBE extended customers_partitioned_v2;

# COMMAND ----------

# DBTITLE 1,Attempt to Change Partition (Will Fail)
# MAGIC %sql
# MAGIC -- TRADITIONAL PARTITIONING: Cannot directly change partition scheme!
# MAGIC -- There is NO ALTER TABLE command to change partitioning on existing tables
# MAGIC -- This demonstrates the inflexibility of traditional partitioning
# MAGIC
# MAGIC ALTER TABLE customers_partitioned_v2 PARTITIONED BY (customer_segment);

# COMMAND ----------

# MAGIC %md
# MAGIC **Migrating from partitioning or Z-order**
# MAGIC ![Screenshot 2025-11-05 at 17.39.20.png](./Screenshot 2025-11-05 at 17.39.20.png "Screenshot 2025-11-05 at 17.39.20.png")

# COMMAND ----------

# DBTITLE 1,Final Comparison Summary
# MAGIC %sql
# MAGIC -- SUMMARY: Liquid Clustering vs Traditional Partitioning
# MAGIC SELECT 
# MAGIC     'Liquid Clustering' as approach,
# MAGIC     'ALTER TABLE table_name CLUSTER BY (new_columns)' as change_command,
# MAGIC     'Instant' as time_required,
# MAGIC     'Zero downtime' as availability,
# MAGIC     'No data movement' as data_impact,
# MAGIC     'Very Easy' as complexity
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC     'Traditional Partitioning' as approach,
# MAGIC     'No direct command - must recreate table' as change_command,
# MAGIC     'Hours/Days for large tables' as time_required,
# MAGIC     'Downtime required' as availability,
# MAGIC     'Full data rewrite required' as data_impact,
# MAGIC     'Very Complex & Risky' as complexity;

# COMMAND ----------

# MAGIC %md
# MAGIC Operations that support clustering on write
# MAGIC Operations that cluster on write include the following:
# MAGIC
# MAGIC - INSERT INTO operations
# MAGIC - CTAS and RTAS statements
# MAGIC - COPY INTO from Parquet format
# MAGIC - spark.write.mode("append")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run Optimize

# COMMAND ----------

# MAGIC %md
# MAGIC Liquid clustering makes OPTIMIZE easier because:
# MAGIC - No need to specify partition predicates; Delta intelligently clusters data based on current query patterns.
# MAGIC - Clustering keys can be changed instantly without data rewrite, so OPTIMIZE adapts automatically.
# MAGIC - OPTIMIZE on liquid clustered tables works efficiently across all data, regardless of clustering key changes.
# MAGIC
# MAGIC Liquid clustering is incremental, meaning that OPTIMIZE only rewrites data as necessary to accommodate data that needs clustering. 
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# DBTITLE 1,OPTIMIZE Traditional Partitioned Table
# MAGIC %sql
# MAGIC -- OPTIMIZE for Traditional Partitioned Table
# MAGIC -- Must specify partition predicates for efficient optimization
# MAGIC OPTIMIZE customers_partitioned_v2 WHERE region = 'North America';
# MAGIC
# MAGIC -- To optimize all partitions (expensive for large tables)
# MAGIC -- OPTIMIZE customers_partitioned_v2;

# COMMAND ----------

# DBTITLE 1,OPTIMIZE Liquid Clustering Table
# MAGIC %sql
# MAGIC
# MAGIC OPTIMIZE customers_clustered;

# COMMAND ----------

# DBTITLE 1,Show File Structure After Optimization
# MAGIC %sql
# MAGIC
# MAGIC DESCRIBE DETAIL customers_partitioned_v2;

# COMMAND ----------

# DBTITLE 1,OPTIMIZE Comparison Summary
# MAGIC %sql
# MAGIC -- OPTIMIZE Command Comparison
# MAGIC SELECT 
# MAGIC     'Traditional Partitioned' as table_type,
# MAGIC     'OPTIMIZE table WHERE partition_col = value' as optimize_syntax,
# MAGIC     'Must specify partition predicates' as requirement,
# MAGIC     'Partition-by-partition optimization' as behavior,
# MAGIC     'Manual partition management' as complexity,
# MAGIC     'Can be expensive for many partitions' as cost_impact
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC     'Liquid Clustering' as table_type,
# MAGIC     'OPTIMIZE table' as optimize_syntax,
# MAGIC     'No predicates needed' as requirement,
# MAGIC     'Intelligent clustering-aware optimization' as behavior,
# MAGIC     'Automatic optimization' as complexity,
# MAGIC     'Efficient across all data' as cost_impact;