-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Query Pipeline Results

-- COMMAND ----------

CREATE catalog Football_python_DLT;


-- COMMAND ----------

SELECT * FROM event_log('e772d1c4-17dd-4ea4-998a-9b66d2c7345a')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC event_log = spark.sql("SELECT * FROM event_log('e772d1c4-17dd-4ea4-998a-9b66d2c7345a')")
-- MAGIC event_log.createOrReplaceTempView("event_log_raw")
-- MAGIC latest_update_id = spark.sql("SELECT origin.update_id FROM event_log_raw WHERE event_type = 'create_update' ORDER BY timestamp DESC LIMIT 1").collect()[0].update_id
-- MAGIC spark.conf.set('latest_update.id', latest_update_id)

-- COMMAND ----------


SELECT
  row_expectations.dataset as dataset,
  row_expectations.name as expectation,
  SUM(row_expectations.passed_records) as passing_records,
  SUM(row_expectations.failed_records) as failing_records
FROM
  (
    SELECT
      explode(
        from_json(
          details :flow_progress :data_quality :expectations,
          "array<struct<name: string, dataset: string, passed_records: int, failed_records: int>>"
        )
      ) row_expectations
    FROM
      event_log_raw
  
    
  )
GROUP BY
  row_expectations.dataset,
  row_expectations.name

-- COMMAND ----------

SELECT timestamp, details, details:user_action:user_name FROM event_log_raw WHERE event_type = 'user_action'