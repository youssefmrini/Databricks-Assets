-- Databricks notebook source
-- MAGIC %md
-- MAGIC # DLT Event Logs

-- COMMAND ----------

use catalog dlt_demo_youssef;
--create schema logs;
use logs;
SELECT * FROM event_log("b05fb7e1-de59-4167-86ee-df02c21af6d5");
CREATE VIEW event_log_raw AS SELECT * FROM event_log("b05fb7e1-de59-4167-86ee-df02c21af6d5");
CREATE OR REPLACE TEMP VIEW latest_update AS SELECT origin.update_id AS id FROM event_log_raw WHERE event_type = 'create_update' ORDER BY timestamp DESC LIMIT 1;




-- COMMAND ----------

-- DBTITLE 1,Query Lineage
SELECT
  details:flow_definition.output_dataset as output_dataset,
  details:flow_definition.input_datasets as input_dataset
FROM
  event_log_raw,
  latest_update
WHERE
  event_type = 'flow_definition'
  AND
  origin.update_id = latest_update.id


-- COMMAND ----------

-- DBTITLE 1,Data Quality
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
      event_log_raw,
      latest_update
    WHERE
      event_type = 'flow_progress'
      AND origin.update_id = latest_update.id
  )
GROUP BY
  row_expectations.dataset,
  row_expectations.name


-- COMMAND ----------

-- DBTITLE 1,Query user actions
SELECT timestamp, details:user_action:action, details:user_action:user_name FROM event_log_raw WHERE event_type = 'user_action'


-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC <h1><a href="https://docs.databricks.com/en/delta-live-tables/observability.html"> Documentation of the Observability </a></h1>