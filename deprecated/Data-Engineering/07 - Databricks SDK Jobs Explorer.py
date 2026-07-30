# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks SDK - Jobs Explorer

# COMMAND ----------

# MAGIC %pip install -U databricks-sdk
# MAGIC dbutils.library.restartPython()
# MAGIC from databricks.sdk import WorkspaceClient
# MAGIC
# MAGIC # Initialize the Databricks workspace client
# MAGIC ws = WorkspaceClient()
# MAGIC
# MAGIC # List all jobs
# MAGIC jobs = ws.jobs.list()
# MAGIC
# MAGIC # Collect job name and id into a list of tuples
# MAGIC job_data = [(job.settings.name, job.job_id) for job in jobs]
# MAGIC
# MAGIC # Create a DataFrame with columns 'name' and 'id'
# MAGIC jobs_df = spark.createDataFrame(job_data, ["name", "id"])
# MAGIC
# MAGIC # Write the DataFrame to a Delta table
# MAGIC jobs_df.write.format("delta").mode("overwrite").saveAsTable("tmp_jobs_delta")

# COMMAND ----------

from databricks.sdk import WorkspaceClient

ws = WorkspaceClient()
for job in ws.jobs.list():
    settings = job.settings
    print(settings)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from system.workflow.job_run_timeline where run_name is not null
