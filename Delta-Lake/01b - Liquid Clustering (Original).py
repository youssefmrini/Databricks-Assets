# Databricks notebook source
# MAGIC %md
# MAGIC # Liquid Clustering (Original)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG airline;

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG airline;
# MAGIC create schema details;

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog airline;
# MAGIC use schema airline.details;

# COMMAND ----------

df=spark.read.format("csv").option("header","true").schema("FL_DATE string,OP_UNIQUE_CARRIER string,OP_CARRIER_FL_NUM string,ORIGIN string,DEST string,DEP_TIME string,DEP_DELAY string,TAXI_OUT string,WHEELS_OFF string,WHEELS_ON string,TAXI_IN string,ARR_TIME string,ARR_DELAY string,AIR_TIME string,DISTANCE string,CARRIER_DELAY string,WEATHER_DELAY string,NAS_DELAY string,SECURITY_DELAY string,LATE_AIRCRAFT_DELAY string,CRS_ARR_TIME string,CANCELLED string,CANCELLATION_CODE string,DIVERTED string,CRS_ELAPSED_TIME string,ACTUAL_ELAPSED_TIME string").load("/Volumes/airline/details/data/2010.csv")
display(df)

# COMMAND ----------


df=spark.readStream.format("cloudFiles").option("cloudFiles.format", "csv").schema("FL_DATE string,OP_UNIQUE_CARRIER string,OP_CARRIER_FL_NUM string,ORIGIN string,DEST string,DEP_TIME string,DEP_DELAY string,TAXI_OUT string,WHEELS_OFF string,WHEELS_ON string,TAXI_IN string,ARR_TIME string,ARR_DELAY string,AIR_TIME string,DISTANCE string,CARRIER_DELAY string,WEATHER_DELAY string,NAS_DELAY string,SECURITY_DELAY string,LATE_AIRCRAFT_DELAY string,CRS_ARR_TIME string,CANCELLED string,CANCELLATION_CODE string,DIVERTED string,CRS_ELAPSED_TIME string,ACTUAL_ELAPSED_TIME string").option("cloudFiles.schemaLocation","/info").option("header","True").load("/Volumes/airline/details/data/")

query = df.writeStream.format("delta").option("mergeSchema", "true").option("checkpointLocation", "/information").toTable("informations")




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