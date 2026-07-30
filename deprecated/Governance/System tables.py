# Databricks notebook source
# MAGIC %md
# MAGIC # System Tables (API)

# COMMAND ----------

import requests

# Retrieve token
token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()

# Use token in a request example
response = requests.get("https://e2-demo-field-eng.cloud.databricks.com/api/2.0/clusters/list", headers={"Authorization": f"Bearer {token}"})
response

# COMMAND ----------

# MAGIC %sh
# MAGIC
# MAGIC curl -v -X PUT -H "Authorization: Bearer {$token}" "https://e2-demo-field-eng.cloud.databricks.com/api/2.0/unity-catalog/metastores/b169b504-4c54-49f2-bc3a-adf4b128f36d/systemschemas/compute"
# MAGIC

# COMMAND ----------

# MAGIC %sh
# MAGIC  curl -v -X PUT -H "Authorization: Bearer {$token}" "https://e2-demo-field-eng.cloud.databricks.com/api/2.0/unity-catalog/metastores/b169b504-4c54-49f2-bc3a-adf4b128f36d/systemschemas/storage"
# MAGIC

# COMMAND ----------

# MAGIC  %sh
# MAGIC  curl -v -X PUT -H "Authorization: Bearer {$token}" "https://e2-demo-field-eng.cloud.databricks.com/api/2.0/unity-catalog/metastores/b169b504-4c54-49f2-bc3a-adf4b128f36d/systemschemas/marketplace"