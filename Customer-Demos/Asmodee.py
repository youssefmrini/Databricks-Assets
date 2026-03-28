# Databricks notebook source
# MAGIC %md
# MAGIC # Asmodee Demo

# COMMAND ----------

# MAGIC %sql
# MAGIC create catalog youssefmm;
# MAGIC use catalog youssefmm;
# MAGIC use schema default

# COMMAND ----------

import pandas as pd
import random
from datetime import datetime, timedelta

# Define volume path - using main catalog and default schema
catalog = "youssefmm"
schema = "default"
volume_name = "sales_data_volume"
volume_path = f"/Volumes/{catalog}/{schema}/{volume_name}"

# Create volume if it doesn't exist
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.{volume_name}")

print(f"Volume created/verified at: {volume_path}")

# Generate sales data
random.seed(42)
products = ['Laptop', 'Mouse', 'Keyboard', 'Monitor', 'Headphones', 'Webcam', 'Desk', 'Chair']
regions = ['North', 'South', 'East', 'West', 'Central']

def generate_sales_data(num_rows, start_id):
    """Generate sales data with specified number of rows"""
    data = []
    start_date = datetime(2024, 1, 1)
    
    for i in range(num_rows):
        sale_id = start_id + i
        sale_date = start_date + timedelta(days=random.randint(0, 330))
        product = random.choice(products)
        quantity = random.randint(1, 10)
        price = round(random.uniform(10.0, 2000.0), 2)
        customer_id = f"CUST{random.randint(1000, 9999)}"
        region = random.choice(regions)
        
        data.append({
            'sale_id': sale_id,
            'sale_date': sale_date.strftime('%Y-%m-%d'),
            'product': product,
            'quantity': quantity,
            'price': price,
            'customer_id': customer_id,
            'region': region
        })
    
    return pd.DataFrame(data)

# Generate 2 CSV files with 1000 rows each
df1 = generate_sales_data(1000, 1)
df2 = generate_sales_data(1000, 1001)

# Save to volume using dbutils (volumes don't support /dbfs access)
csv_path_1 = f"{volume_path}/sales_data_1.csv"
csv_path_2 = f"{volume_path}/sales_data_2.csv"

dbutils.fs.put(csv_path_1, df1.to_csv(index=False), overwrite=True)
dbutils.fs.put(csv_path_2, df2.to_csv(index=False), overwrite=True)

print(f"\nGenerated 2 CSV files with 1000 rows each:")
print(f"  - {csv_path_1}")
print(f"  - {csv_path_2}")
print(f"\nSample data from first file:")
print(df1.head())

# COMMAND ----------

# DBTITLE 1,Define schema for sales data
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, DoubleType

# Define the schema matching the CSV structure
sales_schema = StructType([
    StructField("sale_id", IntegerType(), True),
    StructField("sale_date", DateType(), True),
    StructField("product", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("price", DoubleType(), True),
    StructField("customer_id", StringType(), True),
    StructField("region", StringType(), True)
])

print("Schema defined for sales data:")
print(sales_schema)

# COMMAND ----------

# DBTITLE 1,Ingest CSV files using Auto Loader
# Define paths
csv_source_path = f"{volume_path}/*.csv"
checkpoint_path = f"{volume_path}/checkpoints/sales_autoloader"
target_table = f"{catalog}.{schema}.sales_data"

# Read CSV files using Auto Loader with defined schema
df_stream = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("header", "true")
    .schema(sales_schema)  # Using the defined schema
    .load(csv_source_path)
)

print(f"Auto Loader configured to read from: {csv_source_path}")
print(f"Checkpoint location: {checkpoint_path}")
print(f"Target table: {target_table}")
print(f"\nStream schema:")
df_stream.printSchema()

# COMMAND ----------

# DBTITLE 1,Write stream to Delta table
# Write the stream to a Delta table with checkpoint in volume
query = (df_stream.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_path)
    .option("mergeSchema", "true")
    .trigger(availableNow=True)  # Process all available data and stop
    .toTable(target_table)
)

# Wait for the stream to complete
query.awaitTermination()

print(f"\nData successfully ingested into table: {target_table}")
print(f"Checkpoint saved at: {checkpoint_path}")

# COMMAND ----------

# DBTITLE 1,Verify ingested data
# Verify the data was ingested correctly
result_df = spark.table(target_table)

print(f"Total rows ingested: {result_df.count()}")
print(f"\nSample data from {target_table}:")
display(result_df.limit(10))

print(f"\nData distribution by region:")
display(result_df.groupBy("region").count().orderBy("count", ascending=False))