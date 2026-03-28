-- Databricks notebook source
-- MAGIC %md
-- MAGIC # View Sharing

-- COMMAND ----------

select current_metastore()

-- COMMAND ----------


CREATE  RECIPIENT  view_sharing_youssefmris USING ID 'aws:us-west-2:9317329f-e53f-4293-bdc1-b7712bffd7f8'

-- COMMAND ----------

create share youssef_sharing;

-- COMMAND ----------

CREATE VIEW view_sharing.testing.youssefsharing AS SELECT *  FROM view_sharing.testing.flights


-- COMMAND ----------

describe extended view_sharing.testing.youssefsharing

-- COMMAND ----------

ALTER SHARE youssef_sharing ADD VIEW view_sharing.testing.youssefsharing;

-- COMMAND ----------

GRANT SELECT ON SHARE youssef_sharing TO RECIPIENT view_sharing_youssefm