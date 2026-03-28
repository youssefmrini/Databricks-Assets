# Databricks notebook source
# MAGIC %md
# MAGIC # Delta Lake vs Apache Iceberg v3 Demo
# MAGIC
# MAGIC This notebook demonstrates key features and capabilities of both **Delta Lake** and **Apache Iceberg v3** table formats in Databricks.
# MAGIC
# MAGIC ## What We'll Cover:
# MAGIC * Table creation and basic operations
# MAGIC * Time travel and versioning
# MAGIC * Schema evolution
# MAGIC * ACID transactions
# MAGIC * Partition evolution
# MAGIC * Performance optimizations
# MAGIC * Metadata operations

# COMMAND ----------

# DBTITLE 1,Setup: Create catalogs and schemas
# MAGIC %sql
# MAGIC -- Create a schema for our demo tables
# MAGIC CREATE SCHEMA IF NOT EXISTS main.table_formats_demo
# MAGIC COMMENT 'Demo schema comparing Delta Lake and Iceberg v3';
# MAGIC
# MAGIC USE main.table_formats_demo;

# COMMAND ----------

# DBTITLE 1,Create sample data
# MAGIC %sql
# MAGIC -- Create a temporary view with sample e-commerce data
# MAGIC CREATE OR REPLACE TEMP VIEW sample_data AS
# MAGIC SELECT 
# MAGIC   id,
# MAGIC   customer_id,
# MAGIC   product_name,
# MAGIC   category,
# MAGIC   price,
# MAGIC   quantity,
# MAGIC   order_date,
# MAGIC   region
# MAGIC FROM (
# MAGIC   SELECT 
# MAGIC     row_number() OVER (ORDER BY rand()) as id,
# MAGIC     cast(rand() * 1000 as int) as customer_id,
# MAGIC     CASE cast(rand() * 5 as int)
# MAGIC       WHEN 0 THEN 'Laptop'
# MAGIC       WHEN 1 THEN 'Phone'
# MAGIC       WHEN 2 THEN 'Tablet'
# MAGIC       WHEN 3 THEN 'Headphones'
# MAGIC       ELSE 'Monitor'
# MAGIC     END as product_name,
# MAGIC     CASE cast(rand() * 3 as int)
# MAGIC       WHEN 0 THEN 'Electronics'
# MAGIC       WHEN 1 THEN 'Accessories'
# MAGIC       ELSE 'Computers'
# MAGIC     END as category,
# MAGIC     round(rand() * 2000 + 100, 2) as price,
# MAGIC     cast(rand() * 5 + 1 as int) as quantity,
# MAGIC     date_add(current_date(), -cast(rand() * 365 as int)) as order_date,
# MAGIC     CASE cast(rand() * 4 as int)
# MAGIC       WHEN 0 THEN 'North'
# MAGIC       WHEN 1 THEN 'South'
# MAGIC       WHEN 2 THEN 'East'
# MAGIC       ELSE 'West'
# MAGIC     END as region
# MAGIC   FROM range(10000)
# MAGIC );
# MAGIC
# MAGIC SELECT * FROM sample_data LIMIT 5;

# COMMAND ----------

# DBTITLE 1,Create Delta Lake table
# MAGIC %sql
# MAGIC -- Create a Delta Lake table (default format in Databricks)
# MAGIC CREATE OR REPLACE TABLE orders_delta
# MAGIC USING DELTA
# MAGIC CLUSTER BY (region)
# MAGIC COMMENT 'Orders table in Delta Lake format'
# MAGIC AS SELECT * FROM sample_data;
# MAGIC
# MAGIC -- Show table properties
# MAGIC DESCRIBE DETAIL orders_delta;

# COMMAND ----------

# DBTITLE 1,Create Iceberg v3 table
# MAGIC %sql
# MAGIC -- Create an Apache Iceberg table
# MAGIC CREATE OR REPLACE TABLE orders_iceberg
# MAGIC USING ICEBERG
# MAGIC CLUSTER BY (region)
# MAGIC COMMENT 'Orders table in Iceberg format'
# MAGIC AS SELECT * FROM sample_data;
# MAGIC
# MAGIC -- Show table properties
# MAGIC DESCRIBE DETAIL orders_iceberg;

# COMMAND ----------

# DBTITLE 1,Feature 1: Time Travel - Delta Lake
# MAGIC %sql
# MAGIC -- Delta Lake: Query historical versions using VERSION AS OF
# MAGIC SELECT 'Initial row count' as description, count(*) as row_count FROM orders_delta VERSION AS OF 0
# MAGIC UNION ALL
# MAGIC SELECT 'Current row count', count(*) FROM orders_delta;
# MAGIC
# MAGIC -- Delta Lake: View version history
# MAGIC DESCRIBE HISTORY orders_delta LIMIT 5;

# COMMAND ----------

# DBTITLE 1,Feature 1: Time Travel - Iceberg
# MAGIC %sql
# MAGIC -- Iceberg: Query historical versions using TIMESTAMP AS OF
# MAGIC SELECT 'Current row count' as description, count(*) as row_count FROM orders_iceberg;
# MAGIC
# MAGIC -- Iceberg: View snapshot history
# MAGIC SELECT * FROM main.table_formats_demo.orders_iceberg.history ORDER BY made_current_at DESC LIMIT 5;

# COMMAND ----------

# DBTITLE 1,Feature 2: ACID Transactions - Updates
# MAGIC %sql
# MAGIC -- Update records in both tables
# MAGIC -- Delta Lake update
# MAGIC UPDATE orders_delta 
# MAGIC SET price = price * 1.1 
# MAGIC WHERE category = 'Electronics' AND region = 'North';
# MAGIC
# MAGIC -- Iceberg update
# MAGIC UPDATE orders_iceberg 
# MAGIC SET price = price * 1.1 
# MAGIC WHERE category = 'Electronics' AND region = 'North';
# MAGIC
# MAGIC SELECT 'Delta Lake' as table_format, count(*) as updated_rows 
# MAGIC FROM orders_delta 
# MAGIC WHERE category = 'Electronics' AND region = 'North'
# MAGIC UNION ALL
# MAGIC SELECT 'Iceberg', count(*) 
# MAGIC FROM orders_iceberg 
# MAGIC WHERE category = 'Electronics' AND region = 'North';

# COMMAND ----------

# DBTITLE 1,Feature 3: Schema Evolution - Add Column
# MAGIC %sql
# MAGIC -- Add a new column to both tables
# MAGIC -- Delta Lake
# MAGIC ALTER TABLE orders_delta ADD COLUMN discount_applied BOOLEAN DEFAULT false;
# MAGIC
# MAGIC -- Iceberg
# MAGIC ALTER TABLE orders_iceberg ADD COLUMN discount_applied BOOLEAN DEFAULT false;
# MAGIC
# MAGIC -- Verify schema changes
# MAGIC SELECT 'Delta Lake' as format, * FROM orders_delta LIMIT 3
# MAGIC UNION ALL
# MAGIC SELECT 'Iceberg', * FROM orders_iceberg LIMIT 3;

# COMMAND ----------

# DBTITLE 1,Feature 4: Delete Operations
# MAGIC %sql
# MAGIC -- Delete old orders from both tables
# MAGIC -- Delta Lake delete
# MAGIC DELETE FROM orders_delta WHERE order_date < date_add(current_date(), -300);
# MAGIC
# MAGIC -- Iceberg delete
# MAGIC DELETE FROM orders_iceberg WHERE order_date < date_add(current_date(), -300);
# MAGIC
# MAGIC -- Compare row counts after deletion
# MAGIC SELECT 
# MAGIC   'Delta Lake' as table_format,
# MAGIC   count(*) as remaining_rows,
# MAGIC   min(order_date) as oldest_order,
# MAGIC   max(order_date) as newest_order
# MAGIC FROM orders_delta
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC   'Iceberg',
# MAGIC   count(*),
# MAGIC   min(order_date),
# MAGIC   max(order_date)
# MAGIC FROM orders_iceberg;

# COMMAND ----------

# DBTITLE 1,Feature 5: Merge Operations (UPSERT)
# MAGIC %sql
# MAGIC -- Create new data for merge
# MAGIC CREATE OR REPLACE TEMP VIEW new_orders AS
# MAGIC SELECT 
# MAGIC   cast(id + 10000 as bigint) as id,
# MAGIC   customer_id,
# MAGIC   product_name,
# MAGIC   category,
# MAGIC   price * 0.9 as price,
# MAGIC   quantity,
# MAGIC   current_date() as order_date,
# MAGIC   region,
# MAGIC   true as discount_applied
# MAGIC FROM sample_data
# MAGIC LIMIT 100;
# MAGIC
# MAGIC -- Delta Lake MERGE
# MAGIC MERGE INTO orders_delta t
# MAGIC USING new_orders s
# MAGIC ON t.id = s.id
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *;
# MAGIC
# MAGIC -- Iceberg MERGE
# MAGIC MERGE INTO orders_iceberg t
# MAGIC USING new_orders s
# MAGIC ON t.id = s.id
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *;
# MAGIC
# MAGIC SELECT 'Merge completed' as status;

# COMMAND ----------

# DBTITLE 1,Feature 6: Partition Evolution - Iceberg Advantage
# MAGIC %sql
# MAGIC -- Iceberg supports partition evolution without rewriting data
# MAGIC -- This is a key advantage over Delta Lake
# MAGIC
# MAGIC -- View current partitioning for Iceberg
# MAGIC SELECT * FROM main.table_formats_demo.orders_iceberg.partitions LIMIT 10;
# MAGIC
# MAGIC -- Note: Iceberg allows changing partition spec without rewriting data
# MAGIC -- Delta Lake requires recreating the table for partition changes

# COMMAND ----------

# DBTITLE 1,Feature 7: Metadata Tables - Iceberg
# MAGIC %sql
# MAGIC -- Iceberg provides rich metadata tables
# MAGIC -- View snapshots
# MAGIC SELECT 
# MAGIC   snapshot_id,
# MAGIC   parent_id,
# MAGIC   operation,
# MAGIC   summary['added-records'] as added_records,
# MAGIC   summary['deleted-records'] as deleted_records,
# MAGIC   summary['total-records'] as total_records
# MAGIC FROM main.table_formats_demo.orders_iceberg.snapshots
# MAGIC ORDER BY committed_at DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# DBTITLE 1,Feature 8: File-level Metadata - Iceberg
# MAGIC %sql
# MAGIC -- Iceberg: View data files and their statistics
# MAGIC SELECT 
# MAGIC   file_path,
# MAGIC   file_format,
# MAGIC   record_count,
# MAGIC   file_size_in_bytes / 1024 / 1024 as file_size_mb,
# MAGIC   partition
# MAGIC FROM main.table_formats_demo.orders_iceberg.files
# MAGIC LIMIT 10;

# COMMAND ----------

# DBTITLE 1,Feature 9: Optimization - Delta Lake OPTIMIZE
# MAGIC %sql
# MAGIC -- Delta Lake: Optimize and Z-ORDER for better query performance
# MAGIC OPTIMIZE orders_delta ZORDER BY (order_date, customer_id);
# MAGIC
# MAGIC -- View optimization results
# MAGIC DESCRIBE HISTORY orders_delta LIMIT 3;

# COMMAND ----------

# DBTITLE 1,Feature 10: Vacuum - Delta Lake
# MAGIC %sql
# MAGIC -- Delta Lake: Clean up old files (dry run)
# MAGIC -- Note: VACUUM removes files older than retention period
# MAGIC SELECT 'Delta Lake supports VACUUM to remove old data files' as feature;
# MAGIC
# MAGIC -- Syntax (not executed to preserve history):
# MAGIC -- VACUUM orders_delta RETAIN 168 HOURS DRY RUN;

# COMMAND ----------

# DBTITLE 1,Feature 11: Expire Snapshots - Iceberg
# MAGIC %sql
# MAGIC -- Iceberg: Expire old snapshots
# MAGIC -- This is similar to Delta's VACUUM but operates on snapshots
# MAGIC SELECT 'Iceberg supports snapshot expiration for cleanup' as feature;
# MAGIC
# MAGIC -- Syntax (not executed to preserve history):
# MAGIC -- CALL system.expire_snapshots('main.table_formats_demo.orders_iceberg', TIMESTAMP '2025-01-01 00:00:00');

# COMMAND ----------

# DBTITLE 1,Performance Comparison
# MAGIC %sql
# MAGIC -- Compare query performance on both tables
# MAGIC -- Aggregation query
# MAGIC SELECT 
# MAGIC   'Delta Lake' as format,
# MAGIC   region,
# MAGIC   category,
# MAGIC   count(*) as order_count,
# MAGIC   sum(price * quantity) as total_revenue,
# MAGIC   avg(price) as avg_price
# MAGIC FROM orders_delta
# MAGIC GROUP BY region, category
# MAGIC ORDER BY total_revenue DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# DBTITLE 1,Performance Comparison - Iceberg
# MAGIC %sql
# MAGIC -- Same query on Iceberg table
# MAGIC SELECT 
# MAGIC   'Iceberg' as format,
# MAGIC   region,
# MAGIC   category,
# MAGIC   count(*) as order_count,
# MAGIC   sum(price * quantity) as total_revenue,
# MAGIC   avg(price) as avg_price
# MAGIC FROM orders_iceberg
# MAGIC GROUP BY region, category
# MAGIC ORDER BY total_revenue DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Delta Lake vs Iceberg v3: Key Takeaways
# MAGIC
# MAGIC ### **Delta Lake Advantages:**
# MAGIC * **Native Databricks integration** - Optimized for Databricks platform
# MAGIC * **OPTIMIZE with Z-ORDER** - Advanced data skipping and clustering
# MAGIC * **Liquid Clustering** - Automatic clustering without manual maintenance
# MAGIC * **Change Data Feed (CDF)** - Built-in CDC capabilities
# MAGIC * **Photon acceleration** - Vectorized query engine optimization
# MAGIC * **Simpler time travel syntax** - VERSION AS OF is intuitive
# MAGIC
# MAGIC ### **Iceberg v3 Advantages:**
# MAGIC * **Partition evolution** - Change partitioning without rewriting data
# MAGIC * **Hidden partitioning** - Users don't need to know partition columns
# MAGIC * **Rich metadata tables** - snapshots, files, manifests, partitions
# MAGIC * **Multi-engine support** - Works with Spark, Flink, Trino, etc.
# MAGIC * **Snapshot isolation** - Better concurrent write handling
# MAGIC * **Row-level deletes** - More efficient delete operations
# MAGIC
# MAGIC ### **Common Features:**
# MAGIC * ACID transactions
# MAGIC * Time travel
# MAGIC * Schema evolution
# MAGIC * MERGE operations
# MAGIC * Partition pruning
# MAGIC * File-level statistics
# MAGIC
# MAGIC ### **When to Choose:**
# MAGIC * **Delta Lake**: Databricks-native workloads, need Photon/Liquid Clustering, CDF requirements
# MAGIC * **Iceberg**: Multi-engine environments, need partition evolution, open-source preference

# COMMAND ----------

# DBTITLE 1,Cleanup (Optional)
# MAGIC %sql
# MAGIC -- Uncomment to clean up demo tables
# MAGIC -- DROP TABLE IF EXISTS orders_delta;
# MAGIC -- DROP TABLE IF EXISTS orders_iceberg;
# MAGIC -- DROP SCHEMA IF EXISTS main.table_formats_demo CASCADE;
# MAGIC
# MAGIC SELECT 'Demo completed! Tables preserved for further exploration.' as status;

# COMMAND ----------

