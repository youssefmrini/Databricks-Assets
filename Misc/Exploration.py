# Databricks notebook source
# MAGIC %md
# MAGIC ## 
# MAGIC ## In summary, while these _commit files might seem like overhead, they are essential for maintaining the robustness and reliability of data processing workflows in Spark environments. They help avoid scenarios where incomplete or duplicate data could corrupt datasets, thus ensuring that only valid and complete data is processed further.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Automatic Cleanup: Tools like Databricks use these files to manage automatic cleanup processes such as VACUUM, which removes uncommitted or orphaned files after a specified period

# COMMAND ----------

# DBTITLE 1,Disable Transaction Log Files
spark.conf.set("spark.sql.sources.commitProtocolClass", "org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol")

# COMMAND ----------

# MAGIC %md
# MAGIC ## The _SUCCESS file in Apache Spark plays a significant role in indicating the successful completion of a write operation, particularly when working with Parquet files. Here are the key aspects of its importance:

# COMMAND ----------

# DBTITLE 1,_SUCCESS
spark.conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC create catalog test_youssef;
# MAGIC use catalog test_youssef;
# MAGIC create schema test;
# MAGIC use test;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create the Delta table
# MAGIC CREATE TABLE IF NOT EXISTS customer (
# MAGIC   customer_id INT,
# MAGIC   name STRING,
# MAGIC   city STRING
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC -- Insert 15 rows into the customer table
# MAGIC INSERT INTO customer VALUES
# MAGIC   (1, 'Alice', 'New York'),
# MAGIC   (2, 'Bob', 'Los Angeles'),
# MAGIC   (3, 'Charlie', 'Chicago'),
# MAGIC   (4, 'David', 'Houston'),
# MAGIC   (5, 'Eve', 'Phoenix'),
# MAGIC   (6, 'Frank', 'Philadelphia'),
# MAGIC   (7, 'Grace', 'San Antonio'),
# MAGIC   (8, 'Hank', 'San Diego'),
# MAGIC   (9, 'Ivy', 'Dallas'),
# MAGIC   (10, 'Jack', 'San Jose'),
# MAGIC   (11, 'Kathy', 'Austin'),
# MAGIC   (12, 'Leo', 'Jacksonville'),
# MAGIC   (13, 'Mona', 'Fort Worth'),
# MAGIC   (14, 'Nina', 'Columbus'),
# MAGIC   (15, 'Oscar', 'Charlotte');

# COMMAND ----------

# MAGIC %sql
# MAGIC describe detail customer

# COMMAND ----------

# List files in the root directory of DBFS
files = dbutils.fs.ls("s3://databricks-e2demofieldengwest/b169b504-4c54-49f2-bc3a-adf4b128f36d/tables/f4a1bff3-16d3-4fd6-9772-35ea63218b61/_delta_log/_commits/")

# Display the files
display(files)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create the Delta table
# MAGIC CREATE TABLE IF NOT EXISTS customers (
# MAGIC   customer_id INT,
# MAGIC   name STRING,
# MAGIC   city STRING
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC -- Insert 15 rows into the customer table
# MAGIC INSERT INTO customers VALUES
# MAGIC   (1, 'Alice', 'New York'),
# MAGIC   (2, 'Bob', 'Los Angeles'),
# MAGIC   (3, 'Charlie', 'Chicago'),
# MAGIC   (4, 'David', 'Houston'),
# MAGIC   (5, 'Eve', 'Phoenix'),
# MAGIC   (6, 'Frank', 'Philadelphia'),
# MAGIC   (7, 'Grace', 'San Antonio'),
# MAGIC   (8, 'Hank', 'San Diego'),
# MAGIC   (9, 'Ivy', 'Dallas'),
# MAGIC   (10, 'Jack', 'San Jose'),
# MAGIC   (11, 'Kathy', 'Austin'),
# MAGIC   (12, 'Leo', 'Jacksonville'),
# MAGIC   (13, 'Mona', 'Fort Worth'),
# MAGIC   (14, 'Nina', 'Columbus'),
# MAGIC   (15, 'Oscar', 'Charlotte');

# COMMAND ----------

# MAGIC %sql
# MAGIC describe detail customers

# COMMAND ----------

# List files in the root directory of DBFS
files = dbutils.fs.ls("s3://databricks-e2demofieldengwest/b169b504-4c54-49f2-bc3a-adf4b128f36d/tables/035e18f5-20ef-4c3c-994c-29c6b8b88838/_delta_log/_commits/")

# Display the files
display(files)

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

# Create a Spark session
spark = SparkSession.builder.appName("UpdateCustomerTable").getOrCreate()

# Create a DataFrame with fake data
data = [
    (1, 'Alice', 'San Francisco'),
    (2, 'Bob', 'Seattle'),
    (3, 'Charlie', 'Boston'),
    (16, 'Diana', 'Miami'),  # New customer
    (17, 'Eve', 'Denver')    # New customer
]

columns = ["customer_id", "name", "city"]
new_data_df = spark.createDataFrame(data, columns)

# Write the new data to a temporary Delta table
new_data_df.write.format("delta").mode("overwrite").saveAsTable("temp_customer_updates")

# Perform the merge operation
spark.sql("""
MERGE INTO customers AS target
USING temp_customer_updates AS source
ON target.customer_id = source.customer_id
WHEN MATCHED THEN
  UPDATE SET target.name = source.name, target.city = source.city
WHEN NOT MATCHED THEN
  INSERT (customer_id, name, city) VALUES (source.customer_id, source.name, source.city)
""")