# Databricks notebook source
# MAGIC %md
# MAGIC # Liquid Clustering
# MAGIC Adaptive data layout in Delta Lake.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS airline;
use catalog airline;
# MAGIC CREATE SCHEMA IF NOT EXISTS details;
use schema details;

# COMMAND ----------

# Generate synthetic flight data (original used a Volume that no longer exists)
from pyspark.sql.functions import lit, rand, when, expr, date_add
df = (spark.range(0, 5000)
    .withColumn("FL_DATE", date_add(lit("2024-01-01"), (rand() * 365).cast("int")).cast("string"))
    .withColumn("OP_UNIQUE_CARRIER", when(rand() < 0.25, lit("AA")).when(rand() < 0.5, lit("DL")).when(rand() < 0.75, lit("UA")).otherwise(lit("WN")))
    .withColumn("OP_CARRIER_FL_NUM", (rand() * 9999).cast("int").cast("string"))
    .withColumn("ORIGIN", when(rand() < 0.2, lit("LAX")).when(rand() < 0.4, lit("JFK")).when(rand() < 0.6, lit("ORD")).when(rand() < 0.8, lit("ATL")).otherwise(lit("SFO")))
    .withColumn("DEST", when(rand() < 0.2, lit("MIA")).when(rand() < 0.4, lit("BOS")).when(rand() < 0.6, lit("DEN")).when(rand() < 0.8, lit("SEA")).otherwise(lit("DFW")))
    .withColumn("DEP_DELAY", ((rand() * 120) - 20).cast("int").cast("string"))
    .withColumn("ARR_DELAY", ((rand() * 120) - 20).cast("int").cast("string"))
    .withColumn("DISTANCE", ((rand() * 3000) + 200).cast("int").cast("string"))
    .drop("id"))
df.write.mode("overwrite").saveAsTable("airline.details.informations")
display(df)

# COMMAND ----------


# Streaming read replaced with batch (Volume no longer exists)
df = spark.table("airline.details.informations")

# Batch write instead of streaming
df.write.mode("overwrite").saveAsTable("informations")




# COMMAND ----------

display(query)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC <img src="https://cms.databricks.com/sites/default/files/inline-images/image2_3.png"/>
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC <H2> Create a Clustered Delta Table </H2>

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE or replace TABLE airline.details.delays_clustering  CLUSTER BY (ORIGIN, DEST) as select * from informations 

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC <H2> Create a partitioned Delta Table </H2>

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE or replace TABLE  airline.details.delays_partitions USING DELTA partitioned  BY (ORIGIN, DEST) as select * from informations
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC <H2> Optimize both tables</H2>
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC optimize airline.details.delays_partitions;
# MAGIC optimize airline.details.delays_clustering;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC <H2> Table partitioned </H2>
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from airline.details.delays_partitions where origin="LGA";
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC <H2> Table Clustered </H2>
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from  airline.details.delays_clustering where origin="LGA";

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC <H2> Change The clustering Column </H2>

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC ALTER TABLE delays_clustering CLUSTER BY (OP_UNIQUE_CARRIER);
# MAGIC OPTIMIZE delays_clustering;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delays_clustering where OP_UNIQUE_CARRIER="MQ"

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC describe detail delays_clustering