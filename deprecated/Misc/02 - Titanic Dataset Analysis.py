# Databricks notebook source
# MAGIC %md
# MAGIC # 🚢 Titanic Dataset Analysis: SQL & Python KPIs
# MAGIC
# MAGIC ## The Power of Databricks Notebooks
# MAGIC
# MAGIC This notebook demonstrates the **flexibility and power** of Databricks notebooks by:
# MAGIC
# MAGIC * **Multi-language Support**: Seamlessly combining SQL and Python in a single notebook
# MAGIC * **Interactive Analysis**: Computing KPIs with both declarative SQL and programmatic PySpark
# MAGIC * **Rich Visualizations**: Displaying results with built-in `display()` function
# MAGIC * **Unified Data Access**: Querying Unity Catalog tables from any language
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📊 What We'll Explore
# MAGIC
# MAGIC We'll analyze the famous Titanic dataset to uncover survival patterns through **4 comprehensive KPIs**:
# MAGIC
# MAGIC 1. **Overall Survival Rate** - What percentage of passengers survived?
# MAGIC 2. **Survival by Passenger Class** - How did class affect survival chances?
# MAGIC 3. **Fare Analysis by Class & Gender** - Economic patterns and survival correlation
# MAGIC 4. **Age Group & Gender Analysis** - Demographic survival patterns
# MAGIC
# MAGIC Each KPI is implemented in **both SQL and Python** to showcase language interoperability!

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🎨 Visual Context: The Titanic Disaster
# MAGIC
# MAGIC <img src="https://upload.wikimedia.org/wikipedia/commons/thumb/f/fd/RMS_Titanic_3.jpg/1200px-RMS_Titanic_3.jpg" width="800" alt="RMS Titanic">
# MAGIC
# MAGIC *The RMS Titanic, photographed in 1912 before her maiden voyage*
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Historical Context
# MAGIC
# MAGIC On April 15, 1912, the RMS Titanic sank in the North Atlantic Ocean after striking an iceberg. Of the estimated 2,224 passengers and crew aboard, more than 1,500 died. This dataset contains information about 891 passengers, allowing us to analyze survival patterns based on:
# MAGIC
# MAGIC * **Passenger Class** (1st, 2nd, 3rd)
# MAGIC * **Gender** (Male, Female)
# MAGIC * **Age** (Children, Adults, Seniors)
# MAGIC * **Fare** (Ticket price)
# MAGIC * **Family Relations** (Siblings/Spouses, Parents/Children)
# MAGIC
# MAGIC Let's dive into the data! 👇

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog `youssefmm`;
# MAGIC use schema `default`;

# COMMAND ----------

# DBTITLE 1,Load Titanic Dataset
# Load the titanic dataset
df = spark.table("titanic_dataset")
display(df)

# COMMAND ----------

# DBTITLE 1,Data Overview
# MAGIC %sql
# MAGIC -- Data Overview: Row count and basic statistics
# MAGIC SELECT 
# MAGIC   COUNT(*) AS total_passengers,
# MAGIC   COUNT(DISTINCT Pclass) AS passenger_classes,
# MAGIC   COUNT(DISTINCT Sex) AS gender_categories,
# MAGIC   COUNT(DISTINCT Embarked) AS embarkation_ports,
# MAGIC   COUNT(Age) AS passengers_with_age,
# MAGIC   COUNT(Cabin) AS passengers_with_cabin,
# MAGIC   ROUND(AVG(Age), 2) AS avg_age,
# MAGIC   ROUND(AVG(Fare), 2) AS avg_fare
# MAGIC FROM `titanic_dataset`;

# COMMAND ----------

# DBTITLE 1,Data Overview - Python
# Data Overview: Row count and basic statistics
from pyspark.sql.functions import count, countDistinct, avg, round as spark_round

data_overview = df.agg(
    count("*").alias("total_passengers"),
    countDistinct("Pclass").alias("passenger_classes"),
    countDistinct("Sex").alias("gender_categories"),
    countDistinct("Embarked").alias("embarkation_ports"),
    count("Age").alias("passengers_with_age"),
    count("Cabin").alias("passengers_with_cabin"),
    spark_round(avg("Age"), 2).alias("avg_age"),
    spark_round(avg("Fare"), 2).alias("avg_fare")
)

display(data_overview)

# COMMAND ----------

# DBTITLE 1,KPI 1: Overall Survival Rate
# MAGIC %sql
# MAGIC -- KPI 1: Overall Survival Rate
# MAGIC -- Percentage of passengers who survived the Titanic disaster
# MAGIC SELECT 
# MAGIC   COUNT(*) AS total_passengers,
# MAGIC   SUM(Survived) AS survivors,
# MAGIC   COUNT(*) - SUM(Survived) AS casualties,
# MAGIC   ROUND(SUM(Survived) * 100.0 / COUNT(*), 2) AS survival_rate_pct
# MAGIC FROM `titanic_dataset`;

# COMMAND ----------

# DBTITLE 1,KPI 1: Overall Survival Rate - Python
# KPI 1: Overall Survival Rate
# Percentage of passengers who survived the Titanic disaster
from pyspark.sql.functions import sum as spark_sum, col

kpi1 = df.agg(
    count("*").alias("total_passengers"),
    spark_sum("Survived").alias("survivors"),
    (count("*") - spark_sum("Survived")).alias("casualties"),
    spark_round(spark_sum("Survived") * 100.0 / count("*"), 2).alias("survival_rate_pct")
)

display(kpi1)

# COMMAND ----------

# DBTITLE 1,KPI 2: Survival Rate by Passenger Class
# MAGIC %sql
# MAGIC -- KPI 2: Survival Rate by Passenger Class
# MAGIC -- Breakdown of survival rates across 1st, 2nd, and 3rd class
# MAGIC SELECT 
# MAGIC   Pclass AS passenger_class,
# MAGIC   COUNT(*) AS total_passengers,
# MAGIC   SUM(Survived) AS survivors,
# MAGIC   COUNT(*) - SUM(Survived) AS casualties,
# MAGIC   ROUND(SUM(Survived) * 100.0 / COUNT(*), 2) AS survival_rate_pct
# MAGIC FROM `titanic_dataset`
# MAGIC GROUP BY Pclass
# MAGIC ORDER BY Pclass;

# COMMAND ----------

# DBTITLE 1,KPI 2: Survival Rate by Passenger Class - Python
# KPI 2: Survival Rate by Passenger Class
# Breakdown of survival rates across 1st, 2nd, and 3rd class
kpi2 = df.groupBy("Pclass").agg(
    count("*").alias("total_passengers"),
    spark_sum("Survived").alias("survivors"),
    (count("*") - spark_sum("Survived")).alias("casualties"),
    spark_round(spark_sum("Survived") * 100.0 / count("*"), 2).alias("survival_rate_pct")
).orderBy("Pclass")

display(kpi2)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- KPI 3: Average Fare by Class and Gender
# MAGIC -- Shows fare distribution patterns across passenger segments
# MAGIC SELECT 
# MAGIC   Pclass AS passenger_class,
# MAGIC   Sex AS gender,
# MAGIC   COUNT(*) AS passenger_count,
# MAGIC   ROUND(AVG(Fare), 2) AS avg_fare,
# MAGIC   ROUND(MIN(Fare), 2) AS min_fare,
# MAGIC   ROUND(MAX(Fare), 2) AS max_fare,
# MAGIC   ROUND(SUM(Survived) * 100.0 / COUNT(*), 2) AS survival_rate_pct
# MAGIC FROM `titanic_dataset`
# MAGIC GROUP BY Pclass
# MAGIC ORDER BY Pclass, Sex;