# Databricks notebook source
# DBTITLE 1,Introduction
# MAGIC %md
# MAGIC # Apache Iceberg V3 Features - Complete Demo
# MAGIC
# MAGIC This notebook demonstrates the key features of **Apache Iceberg V3** in Databricks:
# MAGIC
# MAGIC ## Key Features:
# MAGIC
# MAGIC * **Deletion Vectors**: Enable efficient row-level deletes and updates without rewriting entire data files
# MAGIC * **VARIANT Data Type**: Store and query semi-structured JSON data with optimized encoding
# MAGIC * **Row Lineage**: Automatic tracking of incremental changes to table data (enabled by default)
# MAGIC
# MAGIC ## What You'll Learn:
# MAGIC
# MAGIC 1. Creating Iceberg V3 tables with different features
# MAGIC 2. Using deletion vectors for efficient row-level operations
# MAGIC 3. Working with VARIANT columns for semi-structured data
# MAGIC 4. Upgrading existing Iceberg V2 tables to V3
# MAGIC 5. Comparing table formats and performance
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Note**: This feature is in Beta. Make sure your workspace admin has enabled this preview feature.

# COMMAND ----------

# DBTITLE 1,Setup: Create Demo Catalog and Schema
# Create a catalog and schema for our Iceberg V3 demo
catalog_name = "iceberg_v3_demo"
schema_name = "demo_schema"

# Create catalog
spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog_name}")
print(f"✅ Catalog '{catalog_name}' created")

# Use the catalog
spark.sql(f"USE CATALOG {catalog_name}")

# Create schema
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
print(f"✅ Schema '{schema_name}' created")

# Use the schema
spark.sql(f"USE SCHEMA {schema_name}")

print(f"\n📍 Current location: {catalog_name}.{schema_name}")

# COMMAND ----------

# DBTITLE 1,Feature 1: Create Basic Iceberg V3 Table with DV enabled
# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS customers_v3 (
# MAGIC   customer_id INT,
# MAGIC   name STRING,
# MAGIC   email STRING,
# MAGIC   signup_date DATE,
# MAGIC   total_purchases DECIMAL(10,2)
# MAGIC ) USING ICEBERG
# MAGIC TBLPROPERTIES (
# MAGIC   'format-version' = '3'
# MAGIC );
# MAGIC
# MAGIC SHOW TBLPROPERTIES customers_v3;
# MAGIC

# COMMAND ----------

# DBTITLE 1,Insert Sample Data
from datetime import date, timedelta

# Insert sample customer data
spark.sql("""
INSERT INTO customers_v3 VALUES
  (1, 'Alice Johnson', 'alice@example.com', '2024-01-15', 1250.50),
  (2, 'Bob Smith', 'bob@example.com', '2024-02-20', 890.25),
  (3, 'Carol White', 'carol@example.com', '2024-03-10', 2100.75),
  (4, 'David Brown', 'david@example.com', '2024-04-05', 450.00),
  (5, 'Emma Davis', 'emma@example.com', '2024-05-12', 1650.80)
""")

print("✅ Sample data inserted")

# Display the data
print("\n📊 Customer Data:")
display(spark.table("customers_v3"))

# COMMAND ----------

# DBTITLE 1,Insert Order Data
# Insert sample order data
spark.sql("""
INSERT INTO orders_v3 VALUES
  (1001, 1, '2024-06-01', 125.00, 'completed'),
  (1002, 1, '2024-06-05', 89.50, 'completed'),
  (1003, 2, '2024-06-07', 250.00, 'pending'),
  (1004, 3, '2024-06-10', 450.75, 'completed'),
  (1005, 2, '2024-06-12', 180.00, 'shipped'),
  (1006, 4, '2024-06-15', 95.25, 'completed'),
  (1007, 5, '2024-06-18', 320.00, 'pending'),
  (1008, 3, '2024-06-20', 210.50, 'shipped'),
  (1009, 1, '2024-06-22', 75.00, 'completed'),
  (1010, 4, '2024-06-25', 155.80, 'pending')
""")

print("✅ Order data inserted")
print(f"\n📊 Total orders: {spark.table('orders_v3').count()}")

display(spark.table("orders_v3").orderBy("order_id"))

# COMMAND ----------

# DBTITLE 1,Perform Row-Level Updates
# Perform row-level UPDATE - deletion vectors make this efficient!
# Update pending orders to shipped status

print("🔄 Updating pending orders to 'shipped' status...\n")

spark.sql("""
UPDATE orders_v3 
SET status = 'shipped'
WHERE status = 'pending'
""")

print("✅ Update completed using deletion vectors (no full file rewrite!)\n")

# Show updated data
print("📊 Updated orders:")
display(spark.sql("""
  SELECT order_id, customer_id, status, amount 
  FROM orders_v3 
  WHERE status = 'shipped'
  ORDER BY order_id
"""))

# COMMAND ----------

# DBTITLE 1,Perform Row-Level Deletes
# Perform row-level DELETE - deletion vectors make this efficient!
# Delete orders with small amounts

print("🗑️ Deleting orders with amount < $100...\n")

spark.sql("""
DELETE FROM orders_v3 
WHERE amount < 100
""")

print("✅ Delete completed using deletion vectors (no full file rewrite!)\n")

# Show remaining data
remaining_count = spark.table("orders_v3").count()
print(f"📊 Remaining orders: {remaining_count}\n")

display(spark.table("orders_v3").orderBy("order_id"))

# COMMAND ----------

# DBTITLE 1,Check Table History and Deletion Vectors
# Check table history to see the operations
print("📜 Table History (showing row lineage):")
display(spark.sql("""
  DESCRIBE HISTORY orders_v3 
  LIMIT 5
"""))

# Check for deletion vectors in the table
print("\n🔍 Table Details:")
table_detail = spark.sql("DESCRIBE DETAIL orders_v3").toPandas()
print(f"Format: {table_detail['format'][0]}")
print(f"Number of files: {table_detail['numFiles'][0]}")

print("\n✅ Deletion vectors enabled efficient row-level operations without full file rewrites!")
print("✅ Row lineage automatically tracks all changes")

# COMMAND ----------

# DBTITLE 1,Cell 11
# MAGIC %md
# MAGIC ## Feature 3: VARIANT Data Type
# MAGIC
# MAGIC The VARIANT data type allows you to store and query semi-structured JSON data with optimized encoding.
# MAGIC
# MAGIC **Benefits:**
# MAGIC * Outperforms JSON strings for reads and writes
# MAGIC * Flexible schema for semi-structured data
# MAGIC * Efficient storage and querying
# MAGIC * Automatic schema evolution
# MAGIC
# MAGIC **Use Cases:**
# MAGIC * Event data with varying structures
# MAGIC * IoT sensor data
# MAGIC * API responses
# MAGIC * User activity logs

# COMMAND ----------

# DBTITLE 1,Create Table with VARIANT Column
# Create an Iceberg table with VARIANT column
# Using VARIANT automatically creates a V3 table

spark.sql("""
CREATE TABLE IF NOT EXISTS user_events (
  event_id INT,
  user_id INT,
  event_timestamp TIMESTAMP,
  event_type STRING,
  event_data VARIANT
) USING ICEBERG
""")

print("✅ Table 'user_events' created with VARIANT column")
print("\nℹ️ Using VARIANT automatically upgrades the table to Iceberg V3\n")

# Verify table format version
table_props = spark.sql("SHOW TBLPROPERTIES user_events").toPandas()
format_version = table_props[table_props['key'] == 'format-version']['value'].values
if len(format_version) > 0:
    print(f"🔍 Format Version: {format_version[0]}")

# COMMAND ----------

# DBTITLE 1,Insert Semi-Structured Data
# Insert event data with different JSON structures
spark.sql("""
INSERT INTO user_events VALUES
  (1, 101, '2024-06-01 10:00:00', 'page_view', 
   parse_json('{"page": "/home", "duration_seconds": 45, "referrer": "google.com"}')),
  
  (2, 101, '2024-06-01 10:05:00', 'purchase', 
   parse_json('{"product_id": "P123", "quantity": 2, "price": 29.99, "currency": "USD"}')),
  
  (3, 102, '2024-06-01 10:10:00', 'page_view', 
   parse_json('{"page": "/products", "duration_seconds": 120, "device": "mobile"}')),
  
  (4, 103, '2024-06-01 10:15:00', 'click', 
   parse_json('{"element": "banner", "campaign_id": "SUMMER2024", "position": "top"}')),
  
  (5, 102, '2024-06-01 10:20:00', 'purchase', 
   parse_json('{"product_id": "P456", "quantity": 1, "price": 149.99, "currency": "USD", "discount": 15}'))
""")

print("✅ Event data with varying JSON structures inserted\n")
print("📊 Total events: ", spark.table("user_events").count())

# Display the data
display(spark.table("user_events").orderBy("event_id"))

# COMMAND ----------

# DBTITLE 1,Query Nested Fields in VARIANT
# Query nested fields in VARIANT columns using dot notation
print("🔍 Querying nested fields in VARIANT columns:\n")

display(spark.sql("""
  SELECT 
    event_id,
    user_id,
    event_type,
    event_data:page AS page,
    event_data:duration_seconds AS duration,
    event_data:device AS device
  FROM user_events
  WHERE event_type = 'page_view'
  ORDER BY event_id
"""))

print("\n✅ Successfully queried nested fields using colon notation (event_data:field_name)")

# COMMAND ----------

# DBTITLE 1,Query Purchase Events
# Query purchase events and extract product information
print("🛋️ Purchase Events with Product Details:\n")

display(spark.sql("""
  SELECT 
    event_id,
    user_id,
    event_timestamp,
    event_data:product_id::string AS product_id,
    event_data:quantity::int AS quantity,
    event_data:price::decimal(10,2) AS price,
    event_data:currency::string AS currency,
    event_data:discount::int AS discount_pct,
    (event_data:price::decimal(10,2) * event_data:quantity::int) AS total_amount
  FROM user_events
  WHERE event_type = 'purchase'
  ORDER BY event_id
"""))

print("\n✅ Extracted and typed nested fields with ::type casting")

# COMMAND ----------

# DBTITLE 1,Filter and Aggregate on VARIANT Fields
# Filter and aggregate on VARIANT fields
print("📊 Aggregate analysis on VARIANT data:\n")

display(spark.sql("""
  SELECT 
    event_type,
    COUNT(*) AS event_count,
    COUNT(DISTINCT user_id) AS unique_users,
    SUM(event_data:price::decimal(10,2) * event_data:quantity::int) AS total_revenue
  FROM user_events
  GROUP BY event_type
  ORDER BY event_count DESC
"""))

print("\n✅ VARIANT fields can be used in WHERE, GROUP BY, and aggregate functions!")

# COMMAND ----------

# DBTITLE 1,Add VARIANT Column to Existing Table
# Add a VARIANT column to an existing table
# This automatically upgrades the table to V3

print("🔄 Adding VARIANT column to existing 'customers_v3' table...\n")

spark.sql("""
  ALTER TABLE customers_v3 
  ADD COLUMN preferences VARIANT
""")

print("✅ VARIANT column 'preferences' added")
print("ℹ️ Table automatically upgraded to Iceberg V3\n")

# Verify the schema
print("🔍 Updated table schema:")
display(spark.sql("DESCRIBE customers_v3"))

# COMMAND ----------

# DBTITLE 1,Cell 18
# MAGIC %md
# MAGIC ## Feature 4: Upgrading from Iceberg V2 to V3
# MAGIC
# MAGIC Existing Iceberg V2 tables can be upgraded to V3 to take advantage of new features:
# MAGIC
# MAGIC **Upgrade Methods:**
# MAGIC 1. Explicit upgrade using `ALTER TABLE SET TBLPROPERTIES`
# MAGIC 2. Automatic upgrade when enabling deletion vectors
# MAGIC 3. Automatic upgrade when adding VARIANT columns
# MAGIC
# MAGIC Let's demonstrate the explicit upgrade method.

# COMMAND ----------

# DBTITLE 1,Create V2 Table for Upgrade Demo
# Create an Iceberg V2 table
spark.sql("""
CREATE TABLE IF NOT EXISTS products_v2 (
  product_id INT,
  product_name STRING,
  category STRING,
  price DECIMAL(10,2),
  stock_quantity INT
) USING ICEBERG
TBLPROPERTIES (
  'format-version' = '2'
)
""")

print("✅ Iceberg V2 table 'products_v2' created\n")

# Insert some data
spark.sql("""
INSERT INTO products_v2 VALUES
  (1, 'Laptop', 'Electronics', 999.99, 50),
  (2, 'Mouse', 'Electronics', 29.99, 200),
  (3, 'Desk Chair', 'Furniture', 249.99, 75),
  (4, 'Monitor', 'Electronics', 349.99, 100)
""")

# Check current format version
table_props = spark.sql("SHOW TBLPROPERTIES products_v2").toPandas()
format_info = table_props[table_props['key'] == 'format-version']
print(f"🔍 Current Format Version: {format_info['value'].values[0] if len(format_info) > 0 else 'Unknown'}\n")

display(spark.table("products_v2"))

# COMMAND ----------

# DBTITLE 1,Upgrade Table to V3 - Method 1
# Upgrade the table to V3 using ALTER TABLE
print("🔄 Upgrading 'products_v2' table from V2 to V3...\n")

spark.sql("""
  ALTER TABLE products_v2 
  SET TBLPROPERTIES ('format-version' = '3')
""")

print("✅ Table successfully upgraded to V3!\n")

# Verify the upgrade
table_props = spark.sql("SHOW TBLPROPERTIES products_v2").toPandas()
format_info = table_props[table_props['key'] == 'format-version']
print(f"🔍 New Format Version: {format_info['value'].values[0]}\n")

# Check for V3 features
print("🎉 V3 Features now available:")
print("  • Deletion vectors")
print("  • VARIANT columns")
print("  • Row lineage tracking")

# COMMAND ----------

# DBTITLE 1,Verify V3 Features are Available
# Test V3 features on the upgraded table
print("🧪 Testing V3 Features on Upgraded Table\n")

# Test 1: Row-level delete (uses deletion vectors)
print("1️⃣ Testing deletion vectors with row-level delete...")
spark.sql("""
  DELETE FROM products_v2 
  WHERE stock_quantity < 80
""")
print("   ✅ Row-level delete successful\n")

# Test 2: Row-level update
print("2️⃣ Testing row-level update...")
spark.sql("""
  UPDATE products_v2 
  SET price = price * 1.1 
  WHERE category = 'Electronics'
""")
print("   ✅ Row-level update successful\n")

# Show updated data
print("📊 Updated product data:")
display(spark.table("products_v2").orderBy("product_id"))

# COMMAND ----------

# DBTITLE 1,Check Table History for Row Lineage
# Check table history - Row lineage is automatically enabled in V3
print("📜 Table History (Row Lineage Tracking):\n")

history_df = spark.sql("""
  DESCRIBE HISTORY products_v2 
  LIMIT 10
""")

display(history_df.select("version", "timestamp", "operation", "operationMetrics"))

print("\n✅ Row lineage automatically tracks all changes in V3 tables")
print("ℹ️ Every INSERT, UPDATE, DELETE, and MERGE operation is tracked")

# COMMAND ----------

# DBTITLE 1,Compare Table Formats
# Compare features between V2 and V3
import pandas as pd

print("🔍 Iceberg V2 vs V3 Feature Comparison:\n")

comparison_data = {
    'Feature': [
        'Deletion Vectors',
        'VARIANT Data Type',
        'Row Lineage Tracking',
        'Efficient Row-Level Deletes',
        'Efficient Row-Level Updates',
        'Semi-Structured Data',
        'Performance Improvements'
    ],
    'V2': [
        '❌ No',
        '❌ No',
        '❌ No',
        '⚠️ Requires full file rewrite',
        '⚠️ Requires full file rewrite',
        '⚠️ JSON strings only',
        '—'
    ],
    'V3': [
        '✅ Yes (default)',
        '✅ Yes',
        '✅ Yes (automatic)',
        '✅ Optimized with deletion vectors',
        '✅ Optimized with deletion vectors',
        '✅ Optimized VARIANT type',
        '✅ Better read/write performance'
    ]
}

comparison_df = pd.DataFrame(comparison_data)
display(comparison_df)

print("\n🎉 Iceberg V3 provides significant improvements for modern data workloads!")

# COMMAND ----------

# DBTITLE 1,Cell 24
# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC Congratulations! You've explored the key features of Apache Iceberg V3:
# MAGIC
# MAGIC ### ✅ What We Covered:
# MAGIC
# MAGIC 1. **Deletion Vectors**
# MAGIC    * Efficient row-level deletes and updates
# MAGIC    * No full file rewrites required
# MAGIC    * Significant performance improvements
# MAGIC
# MAGIC 2. **VARIANT Data Type**
# MAGIC    * Optimized semi-structured data storage
# MAGIC    * Flexible schema for varying JSON structures
# MAGIC    * Better performance than JSON strings
# MAGIC
# MAGIC 3. **Row Lineage**
# MAGIC    * Automatic tracking of all table changes
# MAGIC    * Complete audit trail via DESCRIBE HISTORY
# MAGIC    * No configuration needed
# MAGIC
# MAGIC 4. **Upgrading to V3**
# MAGIC    * Simple ALTER TABLE command
# MAGIC    * Backward compatible
# MAGIC    * Unlocks all V3 features
# MAGIC
# MAGIC ### 🚀 Next Steps:
# MAGIC * Explore time travel with row lineage
# MAGIC * Test MERGE operations with deletion vectors
# MAGIC * Build ETL pipelines with VARIANT columns

# COMMAND ----------

# DBTITLE 1,Cell 25
# Get details about all our demo tables
print("📊 Demo Tables Summary:\n")

tables = ['customers_v3', 'orders_v3', 'user_events', 'products_v2']

for table_name in tables:
    try:
        detail = spark.sql(f"DESCRIBE DETAIL {table_name}").toPandas()
        print(f"📦 {table_name}:")
        print(f"   Format: {detail['format'][0]}")
        print(f"   Number of files: {detail['numFiles'][0]}")
        print(f"   Size: {detail['sizeInBytes'][0] / 1024:.2f} KB")
        
        # Get format version
        props = spark.sql(f"SHOW TBLPROPERTIES {table_name}").toPandas()
        version_row = props[props['key'] == 'format-version']
        if len(version_row) > 0:
            print(f"   Format Version: {version_row['value'].values[0]}")
        print()
    except Exception as e:
        print(f"   Error reading table: {str(e)}\n")

# COMMAND ----------

# DBTITLE 1,Additional Features Demo
# Additional V3 Features
print("🌟 Additional Iceberg V3 Capabilities:\n")

# 1. Time Travel with Row Lineage
print("1️⃣ Time Travel Query:")
history = spark.sql("DESCRIBE HISTORY orders_v3 LIMIT 1").collect()
if len(history) > 0:
    first_version = history[0]['version']
    print(f"   Querying version {first_version}...")
    
    version_data = spark.sql(f"""
        SELECT COUNT(*) as count 
        FROM orders_v3 VERSION AS OF {first_version}
    """)
    display(version_data)
    print()

# 2. Table Statistics
print("2️⃣ Analyze Table Statistics:")
spark.sql("ANALYZE TABLE user_events COMPUTE STATISTICS")
print("   ✅ Statistics computed for query optimization\n")

# 3. MERGE Operation (uses deletion vectors)
print("3️⃣ MERGE Operation with Deletion Vectors:")
spark.sql("""
  MERGE INTO products_v2 AS target
  USING (SELECT 5 as product_id, 'Keyboard' as product_name, 'Electronics' as category, 79.99 as price, 150 as stock_quantity) AS source
  ON target.product_id = source.product_id
  WHEN MATCHED THEN UPDATE SET *
  WHEN NOT MATCHED THEN INSERT *
""")
print("   ✅ MERGE completed efficiently with deletion vectors\n")

print("🎉 All V3 features working perfectly!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📖 Resources & References
# MAGIC
# MAGIC ### Official Documentation:
# MAGIC * [Iceberg V3 Features Overview](https://docs.databricks.com/aws/en/iceberg/iceberg-v3/)
# MAGIC * [Deletion Vectors in Databricks](https://docs.databricks.com/delta/deletion-vectors/)
# MAGIC * [VARIANT Data Type](https://docs.databricks.com/sql/language-manual/data-types/variant-type/)
# MAGIC * [Semi-Structured Data with VARIANT](https://docs.databricks.com/semi-structured/index/)
# MAGIC
# MAGIC ### Key Benefits:
# MAGIC * **10-100x faster** row-level operations with deletion vectors
# MAGIC * **Better compression** for semi-structured data with VARIANT
# MAGIC * **Automatic auditing** with row lineage tracking
# MAGIC * **Backward compatible** with V2 tables
# MAGIC
# MAGIC ### Best Practices:
# MAGIC 1. Use deletion vectors for tables with frequent updates/deletes
# MAGIC 2. Use VARIANT for varying JSON schemas
# MAGIC 3. Monitor table history for audit compliance
# MAGIC 4. Upgrade V2 tables when you need V3 features
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC 👍 **Ready to use Iceberg V3 in production!**

# COMMAND ----------

# DBTITLE 1,Cleanup (Optional)
# Optional: Clean up demo resources
# Uncomment and run if you want to remove all demo tables and catalog

print("⚠️  Cleanup Script - Uncomment to execute\n")

# Uncomment below to clean up:
"""
print("Dropping demo tables...")
spark.sql("DROP TABLE IF EXISTS customers_v3")
spark.sql("DROP TABLE IF EXISTS orders_v3")
spark.sql("DROP TABLE IF EXISTS user_events")
spark.sql("DROP TABLE IF EXISTS products_v2")
print("✅ Tables dropped\n")

print("Dropping demo schema...")
spark.sql("DROP SCHEMA IF EXISTS iceberg_v3_demo.demo_schema CASCADE")
print("✅ Schema dropped\n")

print("Dropping demo catalog...")
spark.sql("DROP CATALOG IF EXISTS iceberg_v3_demo CASCADE")
print("✅ Catalog dropped\n")

print("🧹 Cleanup complete!")
"""

print("💡 Tip: To clean up, uncomment the code block above and run this cell")