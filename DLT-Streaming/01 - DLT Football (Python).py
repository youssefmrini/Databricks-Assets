# Databricks notebook source
# MAGIC %md
# MAGIC # DLT Football Pipeline (Python)

# COMMAND ----------

import dlt
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col
from pyspark.sql.functions import *

@dlt.table(
  comment="Games for the 2000s",
  table_properties={"quality" : "bronze", 'delta.columnMapping.mode' : 'name','delta.minReaderVersion' : '2','delta.minWriterVersion' : '5'}
  )
def games2000_bronze():
  return (
  spark.readStream.format("cloudFiles").schema("Round int, Date String,`Team 1` String,FT String , `Team 2` string").option("cloudFiles.format", "csv").option("header","True").load("/FileStore/tables/england-master/2000s")
  )


@dlt.table(
  comment="Games for the 2010s",
  table_properties={"quality" : "bronze", 'delta.columnMapping.mode' : 'name','delta.minReaderVersion' : '2','delta.minWriterVersion' : '5'}
  )
def games2010_bronze():
  return (
  spark.readStream.format("cloudFiles").schema("Round int, Date String, `Team 1` String, FT String ,`Team 2` string").option("cloudFiles.format", "csv").option("header","True").load("/FileStore/tables/england-master/2010s")
  )

@dlt.table(
  comment="Silver",
  table_properties={"quality" : "bronze", 'delta.columnMapping.mode' : 'name','delta.minReaderVersion' : '2','delta.minWriterVersion' : '5'}
)
def game_silver():
  df=dlt.read("games2000_bronze").union(dlt.read("games2010_bronze")).drop("_rescued_data").select("*",split(col("FT"),"-").alias("score")).drop("FT").withColumnRenamed("Team 1","Team1").withColumnRenamed("Team 2","Team2")
  tf=df.withColumn("Score_T1", df.score[0]).withColumn("Score_T2", df.score[1]).drop("score").select("*",split(col("Date")," ").alias("Date_con")).drop("date")
  return(tf.withColumn("DayOfWeek", tf.Date_con[0]).withColumn("Month", tf.Date_con[1]).withColumn("Day", tf.Date_con[2]).withColumn("Year", tf.Date_con[3]).drop("Date_con"))


@dlt.table(
  comment="Large Scores",
  table_properties={"quality" : "Gold", 'delta.columnMapping.mode' : 'name','delta.minReaderVersion' : '2','delta.minWriterVersion' : '5'}
)
def large_scores():
  return(dlt.read("game_silver").where("Score_T1>Score_T2+4").union(dlt.read("game_silver").where("Score_T1+4<Score_T2 ")))



@dlt.table(
  comment="Home Wins per year",
  table_properties={"quality" : "bronze", 'delta.columnMapping.mode' : 'name','delta.minReaderVersion' : '2','delta.minWriterVersion' : '5'}
)
def home_wins_per_year():
  return(dlt.read("game_silver").where("Score_T1>Score_T2").groupby("year","Team1").count().withColumnRenamed("count","wins"))


@dlt.table(
  comment="Away wins per year",
  table_properties={"quality" : "bronze", 'delta.columnMapping.mode' : 'name','delta.minReaderVersion' : '2','delta.minWriterVersion' : '5'}
)
def away_wins_per_year():
  return(dlt.read("game_silver").where("Score_T1<Score_T2").groupby("year","Team2").count().withColumnRenamed("count","wins"))

