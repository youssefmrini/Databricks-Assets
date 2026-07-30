# Databricks notebook source
# MAGIC %md
# MAGIC # Automatic Type Widening in Streaming (DBR 18.1)
# MAGIC
# MAGIC Streaming reads on Delta tables now **automatically handle column type widening** without interrupting the stream.
# MAGIC
# MAGIC **Key behavior:**
# MAGIC - Schema tracking location is optional (was required in 18.0)
# MAGIC - Streams automatically adapt when columns are widened (e.g., INT → BIGINT)
# MAGIC - No manual acknowledgment needed by default
# MAGIC
# MAGIC **Config:** `spark.databricks.delta.typeWidening.enableStreamingSchemaTracking` controls manual ack
# MAGIC
# MAGIC **Runtime:** DBR 18.1+

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS features_demo;
# MAGIC USE CATALOG features_demo;
# MAGIC CREATE SCHEMA IF NOT EXISTS type_widening_stream;
# MAGIC USE SCHEMA type_widening_stream;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Create Source Table with Type Widening Enabled

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS sensor_readings;
# MAGIC CREATE TABLE sensor_readings (
# MAGIC   sensor_id INT,
# MAGIC   temperature INT,
# MAGIC   reading_time TIMESTAMP
# MAGIC )
# MAGIC TBLPROPERTIES ('delta.enableTypeWidening' = 'true');

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Insert initial data with INT values
# MAGIC INSERT INTO sensor_readings VALUES
# MAGIC   (1, 22, current_timestamp()),
# MAGIC   (2, 25, current_timestamp()),
# MAGIC   (3, 18, current_timestamp());

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Start a Streaming Read

# COMMAND ----------

# Read the table as a stream
stream_df = (spark.readStream
    .format("delta")
    .table("features_demo.type_widening_stream.sensor_readings"))

# Write to a display for demo purposes
display(stream_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Widen the Column Type (INT → BIGINT)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Widen the temperature column from INT to BIGINT
# MAGIC ALTER TABLE sensor_readings ALTER COLUMN temperature TYPE BIGINT;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify the schema change
# MAGIC DESCRIBE TABLE sensor_readings;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Insert data that uses the wider type
# MAGIC INSERT INTO sensor_readings VALUES
# MAGIC   (4, 2147483648, current_timestamp()),
# MAGIC   (5, 3000000000, current_timestamp());

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Verify Stream Continued Without Interruption

# COMMAND ----------

# MAGIC %sql
# MAGIC -- The stream should have picked up the new rows automatically
# MAGIC SELECT * FROM sensor_readings ORDER BY sensor_id;

# COMMAND ----------

# Stop the stream for cleanup
for s in spark.streams.active:
    s.stop()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **Summary:** Type widening in streaming eliminates the need to restart streams when column types are widened. The stream automatically adapts to schema changes, reducing operational overhead for evolving data pipelines.
