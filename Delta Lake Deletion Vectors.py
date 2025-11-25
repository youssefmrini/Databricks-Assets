# Databricks notebook source
# MAGIC %sql
# MAGIC create  catalog youssef_demo;
# MAGIC use catalog youssef_demo;
# MAGIC create  schema test;
# MAGIC use schema test;

# COMMAND ----------

# Delta Deletion Vector Demo - Focus on MERGE Operations
import pyspark.sql.functions as F
from pyspark.sql.types import *
import time
from delta.tables import DeltaTable

# Configuration
catalog = "youssef_demo"
schema = "test"

print("🚀 Delta Deletion Vector Demo")
print("Focus: How deletion vectors improve MERGE performance")
print("\n📚 Key Points:")
print("• Deletion vectors track deleted rows without rewriting files")
print("• Especially beneficial for MERGE operations (UPDATE/DELETE)")
print("• Trade-off: Faster writes, slightly slower reads")

# COMMAND ----------

# DBTITLE 1,Generate Sample Orders Dataset
from pyspark.sql.functions import (
    col,
    rand,
    when,
    lit,
    concat,
    current_timestamp
)

print("📊 Creating sample data...")

orders_df = spark.range(20000000).select(
    col("id").alias("order_id"),
    (rand() * 10000 + 1).cast("int").alias("customer_id"),
    (rand() * 500 + 10).cast("decimal(10,2)").alias("amount"),
    (rand() * 50 + 1).cast("decimal(8,2)").alias("tax_amount"),
    (rand() * 20 + 5).cast("decimal(8,2)").alias("shipping_cost"),
    when(rand() < 0.8, "active").otherwise("cancelled").alias("status"),
    when(rand() < 0.3, "online")
    .when(rand() < 0.7, "store")
    .otherwise("mobile").alias("channel"),
    when(rand() < 0.4, "credit_card")
    .when(rand() < 0.7, "debit_card")
    .when(rand() < 0.9, "paypal")
    .otherwise("bank_transfer").alias("payment_method"),
    (rand() * 100 + 1).cast("int").alias("product_id"),
    (rand() * 10 + 1).cast("int").alias("quantity"),
    when(rand() < 0.6, "standard")
    .when(rand() < 0.9, "express")
    .otherwise("overnight").alias("shipping_method"),
    concat(lit("Order for customer "), col("id").cast("string")).alias("order_notes"),
    when(rand() < 0.8, "USD")
    .when(rand() < 0.95, "EUR")
    .otherwise("GBP").alias("currency"),
    (rand() * 5 + 1).cast("decimal(3,2)").alias("discount_rate"),
    current_timestamp().alias("created_at"),
    current_timestamp().alias("updated_at")
)

print(f"Generated {orders_df.count():,} orders with {len(orders_df.columns)} columns")
print("Sample data:")
display(orders_df.limit(3))
print(f"\nColumns: {', '.join(orders_df.columns)}")

# COMMAND ----------

# DBTITLE 1,Create Table Without Deletion Vectors
# Create two identical tables - one with DV, one without
timestamp = str(int(time.time()))
table_with_dv = f"{catalog}.{schema}.orders_with_dv_{timestamp}"
table_without_dv = f"{catalog}.{schema}.orders_without_dv_{timestamp}"

print(f"📋 Creating comparison tables...")

# Table WITHOUT deletion vectors
orders_df.write.format("delta").option("delta.enableDeletionVectors", "false").saveAsTable(table_without_dv)

# Table WITH deletion vectors (default)
orders_df.write.format("delta").option("delta.enableDeletionVectors", "true").saveAsTable(table_with_dv)

print(f"✅ Created tables:")
print(f"Without DV: {table_without_dv}")
print(f"With DV: {table_with_dv}")

# COMMAND ----------

# DBTITLE 1,Create Table With Deletion Vectors
# Create updates data for MERGE operations with more comprehensive changes
print("🔄 Preparing MERGE demo...")

# Create updates: change multiple columns for 200K orders (10% of base table)
updates_df = spark.range(200000).select(
    col("id").alias("order_id"),
    lit("updated").alias("new_status"),
    (rand() * 100 + 500).cast("decimal(10,2)").alias("new_amount"),
    (rand() * 10 + 5).cast("decimal(8,2)").alias("new_tax_amount"),
    (rand() * 15 + 10).cast("decimal(8,2)").alias("new_shipping_cost"),
    when(rand() < 0.5, "express").otherwise("overnight").alias("new_shipping_method"),
    (rand() * 3 + 2).cast("decimal(3,2)").alias("new_discount_rate"),
    concat(lit("Updated order "), col("id").cast("string")).alias("new_order_notes"),
    current_timestamp().alias("updated_at")
)

print(f"Created {updates_df.count():,} updates for MERGE")
updates_df.show(3, truncate=False)

# Create new orders to insert (100K new records) with all columns
new_orders_df = spark.range(2000000, 2100000).select(
    col("id").alias("order_id"),
    (rand() * 10000 + 1).cast("int").alias("customer_id"),
    (rand() * 500 + 10).cast("decimal(10,2)").alias("amount"),
    (rand() * 50 + 1).cast("decimal(8,2)").alias("tax_amount"),
    (rand() * 20 + 5).cast("decimal(8,2)").alias("shipping_cost"),
    lit("new").alias("status"),
    lit("online").alias("channel"),
    lit("credit_card").alias("payment_method"),
    (rand() * 100 + 1).cast("int").alias("product_id"),
    (rand() * 5 + 1).cast("int").alias("quantity"),
    lit("standard").alias("shipping_method"),
    concat(lit("New order "), col("id").cast("string")).alias("order_notes"),
    lit("USD").alias("currency"),
    (rand() * 2 + 1).cast("decimal(3,2)").alias("discount_rate"),
    current_timestamp().alias("created_at"),
    current_timestamp().alias("updated_at")
)

print(f"Created {new_orders_df.count():,} new orders for INSERT")
print(f"Total MERGE operations will be: {updates_df.count() + new_orders_df.count():,}")

# COMMAND ----------

# DBTITLE 1,Delete Operations Comparison
# MERGE performance comparison with comprehensive column updates
print("⏱️ MERGE Performance Test")
print("="*40)

# Combine updates and new records for MERGE with matching schemas
merge_data = updates_df.select(
    col("order_id"),
    lit(1000).alias("customer_id"),
    col("new_amount").alias("amount"),
    col("new_tax_amount").alias("tax_amount"),
    col("new_shipping_cost").alias("shipping_cost"),
    col("new_status").alias("status"),
    lit("online").alias("channel"),
    lit("credit_card").alias("payment_method"),
    lit(50).alias("product_id"),
    lit(2).alias("quantity"),
    col("new_shipping_method").alias("shipping_method"),
    col("new_order_notes").alias("order_notes"),
    lit("USD").alias("currency"),
    col("new_discount_rate").alias("discount_rate"),
    col("updated_at").alias("created_at"),
    col("updated_at")
).union(
    new_orders_df.select(
        col("order_id"),
        col("customer_id"),
        col("amount"),
        col("tax_amount"),
        col("shipping_cost"),
        col("status"),
        col("channel"),
        col("payment_method"),
        col("product_id"),
        col("quantity"),
        col("shipping_method"),
        col("order_notes"),
        col("currency"),
        col("discount_rate"),
        col("created_at"),
        col("updated_at")
    )
)

print(f"Total MERGE operations: {merge_data.count():,}")

# MERGE into table WITHOUT deletion vectors
print("\n🚫 MERGE without deletion vectors...")
start_time = time.time()

DeltaTable.forName(spark, table_without_dv).alias("target") \
    .merge(merge_data.alias("source"), "target.order_id = source.order_id") \
    .whenMatchedUpdate(set={
        "amount": "source.amount",
        "tax_amount": "source.tax_amount",
        "shipping_cost": "source.shipping_cost",
        "status": "source.status",
        "shipping_method": "source.shipping_method",
        "order_notes": "source.order_notes",
        "discount_rate": "source.discount_rate",
        "updated_at": "source.updated_at"
    }) \
    .whenNotMatchedInsert(values={
        "order_id": "source.order_id",
        "customer_id": "source.customer_id",
        "amount": "source.amount",
        "tax_amount": "source.tax_amount",
        "shipping_cost": "source.shipping_cost",
        "status": "source.status",
        "channel": "source.channel",
        "payment_method": "source.payment_method",
        "product_id": "source.product_id",
        "quantity": "source.quantity",
        "shipping_method": "source.shipping_method",
        "order_notes": "source.order_notes",
        "currency": "source.currency",
        "discount_rate": "source.discount_rate",
        "created_at": "source.created_at",
        "updated_at": "source.updated_at"
    }) \
    .execute()

merge_time_without_dv = time.time() - start_time
print(f"Time without DV: {merge_time_without_dv:.2f} seconds")

# COMMAND ----------

# DBTITLE 1,Performance and Storage Analysis
# MERGE into table WITH deletion vectors
print("✅ MERGE with deletion vectors...")
start_time = time.time()

DeltaTable.forName(spark, table_with_dv).alias("target") \
    .merge(merge_data.alias("source"), "target.order_id = source.order_id") \
    .whenMatchedUpdate(set={
        "amount": "source.amount",
        "tax_amount": "source.tax_amount",
        "shipping_cost": "source.shipping_cost",
        "status": "source.status",
        "shipping_method": "source.shipping_method",
        "order_notes": "source.order_notes",
        "discount_rate": "source.discount_rate",
        "updated_at": "source.updated_at"
    }) \
    .whenNotMatchedInsert(values={
        "order_id": "source.order_id",
        "customer_id": "source.customer_id",
        "amount": "source.amount",
        "tax_amount": "source.tax_amount",
        "shipping_cost": "source.shipping_cost",
        "status": "source.status",
        "channel": "source.channel",
        "payment_method": "source.payment_method",
        "product_id": "source.product_id",
        "quantity": "source.quantity",
        "shipping_method": "source.shipping_method",
        "order_notes": "source.order_notes",
        "currency": "source.currency",
        "discount_rate": "source.discount_rate",
        "created_at": "source.created_at",
        "updated_at": "source.updated_at"
    }) \
    .execute()

merge_time_with_dv = time.time() - start_time
print(f"Time with DV: {merge_time_with_dv:.2f} seconds")

# Show performance improvement
if merge_time_without_dv > merge_time_with_dv:
    speedup = merge_time_without_dv / merge_time_with_dv
    print(f"\n🚀 Deletion vectors are {speedup:.1f}x FASTER for MERGE!")
else:
    print(f"\n📈 Results may vary based on data size and cluster resources")

print(f"\n📊 Performance Summary:")
print(f"Without DV: {merge_time_without_dv:.2f}s")
print(f"With DV: {merge_time_with_dv:.2f}s")

# Show table sizes after MERGE
print(f"\n📋 Final table sizes:")
print(f"Table without DV: {spark.table(table_without_dv).count():,} records")
print(f"Table with DV: {spark.table(table_with_dv).count():,} records")

# COMMAND ----------

# DBTITLE 1,Deletion Vector Concepts and Best Practices
# Key Takeaways: When Deletion Vectors Excel
print("🏆 Key Takeaways")
print("="*30)

print("🚀 Deletion Vectors are BEST for:")
print("• MERGE operations (UPDATE + INSERT)")
print("• Frequent UPDATE/DELETE workloads")
print("• CDC (Change Data Capture) pipelines")
print("• Streaming upserts")

print("\n📈 Why MERGE benefits most:")
print("• Updates don't rewrite entire files")
print("• Only creates small deletion vector files")
print("• Massive time savings on large tables")

print("\n⚙️ Configuration:")
print("CREATE TABLE orders (...) TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')")
print("\n-- Disable for read-heavy workloads")
print("CREATE TABLE orders (...) TBLPROPERTIES ('delta.enableDeletionVectors' = 'false')")

print(f"\n📋 Demo tables created:")
print(f"With DV: {table_with_dv}")
print(f"Without DV: {table_without_dv}")