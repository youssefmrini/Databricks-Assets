-- Databricks notebook source
-- MAGIC %md # Delta Live Tables quickstart (SQL)
-- MAGIC
-- MAGIC A notebook that provides an example Delta Live Tables pipeline to:
-- MAGIC
-- MAGIC - Read raw CSV data from a publicly available dataset into a table.
-- MAGIC - Read the records from the raw data table and use a Delta Live Tables query and expectations to create a new table that contains cleansed data.
-- MAGIC - Perform an analysis on the prepared data with a Delta Live Tables query.

-- COMMAND ----------

-- DBTITLE 1,Ingest the raw data into a table
CREATE OR REFRESH STREAMING TABLE baby_names_sql_raw
COMMENT "Popular baby first names in New York. This data was ingested from the New York State Departement of Health."
AS SELECT Year, `First Name` AS First_Name, County, Sex, Count FROM read_files(
  '/Volumes/main/default/my-volume/babynames.csv',
  format => 'csv',
  header => true,
  mode => 'FAILFAST')

-- COMMAND ----------

-- DBTITLE 1,Clean and prepare data
CREATE OR REFRESH LIVE TABLE baby_names_sql_prepared(
  CONSTRAINT valid_first_name EXPECT (First_Name IS NOT NULL),
  CONSTRAINT valid_count EXPECT (Count > 0) ON VIOLATION FAIL UPDATE
)
COMMENT "New York popular baby first name data cleaned and prepared for analysis."
AS SELECT
  Year AS Year_Of_Birth,
  First_Name,
  Count
FROM live.baby_names_sql_raw;

-- COMMAND ----------

-- DBTITLE 1,Top baby names 2021
CREATE OR REFRESH LIVE TABLE top_baby_names_sql_2021
COMMENT "A table summarizing counts of the top baby names for New York for 2021."
AS SELECT
  First_Name,
  SUM(Count) AS Total_Count
FROM live.baby_names_sql_prepared
WHERE Year_Of_Birth = 2021
GROUP BY First_Name
ORDER BY Total_Count DESC
LIMIT 10;