# Databricks notebook source
# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Egress Cost Analysis Pipeline
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## How to use
# MAGIC
# MAGIC This is a [Delta Live Table ETL pipeline defined in Python](https://docs.databricks.com/en/delta-live-tables/python-programming.html#python-programming-for-delta-live-tables). The input of the pipeline is [S3 access logs](https://docs.aws.amazon.com/AmazonS3/latest/userguide/ServerLogs.html), and Databricks [Unity Catalog information_schema](https://docs.databricks.com/en/sql/language-manual/sql-ref-information-schema.html#:~:text=The%20INFORMATION_SCHEMA%20is%20a%20SQL,you%20are%20privileged%20the%20see.) tables.
# MAGIC
# MAGIC
# MAGIC ### Prerequisites
# MAGIC
# MAGIC 1. AWS S3 management permission (to set up access log).
# MAGIC 2. Databricks metastore admin / `CREATE_EXTERNAL_LOCATION` privilege, to create external locations.
# MAGIC 3. Proper data access privileges to store the tables. Databricks recommends using Unity Catalog when available.
# MAGIC 4. Cluster creation permission to create and run a DLT pipeline.
# MAGIC
# MAGIC ### Input setup
# MAGIC
# MAGIC 1. Enable S3 access log (if it is not already enabled) on the S3 storage buckets of the Delta shared tables of interest. Follow [AWS instructions](https://docs.aws.amazon.com/AmazonS3/latest/userguide/enable-server-access-logging.html)
# MAGIC
# MAGIC     The S3 buckets storing the shared tables can be found in `Data Explorer`, in the `Details` tab after selecting the table.
# MAGIC
# MAGIC 2. Add the S3 bucket that stores the access log as an [external location](https://docs.databricks.com/en/data-governance/unity-catalog/manage-external-locations-and-credentials.html) in Databricks. It is recommended to use the `AWS Quickstart (Recommended)` found in `Data Explorer - External Data` to add the external location.
# MAGIC
# MAGIC 3. IP range tables. There is another pipeline defined in the `IpRanges` notebook for that.
# MAGIC
# MAGIC
# MAGIC ### Create & Execute the Pipeline
# MAGIC
# MAGIC To create the pipeline, go to `Delta Live Tables` under `Data Engineering`, and click the `Create Pipeline` button. Then fill in the parameters in the UI. Refer to the [instructions](https://docs.databricks.com/en/delta-live-tables/settings.html#configure-pipeline-settings-for-delta-live-tables) for more information.
# MAGIC
# MAGIC * Databricks recommends serverless compute when available. 
# MAGIC * The pipeline can be triggered manually / on a schedule, based on the balance of reducing cost vs more up-to-date information.
# MAGIC * Use this notebook as the source file.
# MAGIC * Please use Unity Catalog as destination to store the output data. 
# MAGIC   * Hive metastore would not work for using external location / accessing system table.
# MAGIC   * Python UDF support with Unity Catalog is in Public Preview. Since the pipeline uses Python UDFs, it must use the preview channel. See up-to-date description about this limitation [here](https://docs.databricks.com/en/delta-live-tables/unity-catalog.html#limitations).
# MAGIC * Specify the external location name for S3 access logs, and the name of the AWS IP range table as [pipeline parameters](https://docs.databricks.com/en/delta-live-tables/settings.html#parameterize-pipelines), `egressCostAws.auditLogExternalLocation` and `egressCostAws.cloudIpTable`
# MAGIC
# MAGIC Here is an example configuration in JSON using Unity Catalog and serverless compute (All the UUIDs are auto-generated from UI):
# MAGIC
# MAGIC ```
# MAGIC {
# MAGIC     "id": "44f7d53d-ba2d-43a5-afc6-f12fede81450",
# MAGIC     "pipeline_type": "WORKSPACE",
# MAGIC     "development": true,
# MAGIC     "continuous": false,
# MAGIC     "channel": "PREVIEW",
# MAGIC     "photon": true,
# MAGIC     "libraries": [
# MAGIC         {
# MAGIC             "notebook": {
# MAGIC                 "path": "/Repos/name@example.com/delta-sharing-egress-analysis/AccessLogDlt"
# MAGIC             }
# MAGIC         }
# MAGIC     ],
# MAGIC     "name": "EgressCostAWS",
# MAGIC     "serverless": true,
# MAGIC     "catalog": "example_catalog",
# MAGIC     "configuration": {
# MAGIC         "egressCostAws.auditLogExternalLocation": "s3://examples3bucket/",
# MAGIC         "egressCostAws.cloudIpTable": "hive_metastore.egress_ip_addresses.ip_with_region_all",
# MAGIC         "egressCostAws.useWorkaround": "true"
# MAGIC     },
# MAGIC     "target": "logs",
# MAGIC     "data_sampling": false
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC
# MAGIC ### Consume the output
# MAGIC
# MAGIC The output will be in `access_log_with_table` in the schema defined when creating the pipeline. The table contains the following dimensions:
# MAGIC "", "recipient_name","","","","url_pattern","","","service","region"
# MAGIC * `table_catalog`
# MAGIC * `table_schema`
# MAGIC * `table_name`
# MAGIC * `url_pattern`
# MAGIC * `shares_list`
# MAGIC * `request_date`
# MAGIC * `recipient_id` 
# MAGIC * `region`. AWS region of the requester. It will be null if the request is not in the IP ranges table. It is derived from `remote_ip`.
# MAGIC
# MAGIC The `sum_bytes_sent` column is the sum of data transmitted for the table/recipient/date/region. 
# MAGIC
# MAGIC

# COMMAND ----------

# DBTITLE 1,Imports S3 access logs from configured external location
import dlt
from pyspark.sql.functions import *
import re
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql import functions as F

def get_boolean_config(config_name, default):
  return spark.conf.get(config_name, str(default)).lower() == 'true'

external_location = spark.conf.get("egressCostAws.auditLogExternalLocation")
cloud_ip_table = spark.conf.get("egressCostAws.cloudIpTable")
use_workaround = get_boolean_config("egressCostAws.useWorkaround", True)

# Import S3 Access Logs
@dlt.table()
def access_logs():
  return (
    spark.readStream.format("cloudFiles")
      .option("cloudFiles.format", "text")
      .option("cloudFiles.inferColumnTypes", "false")
      .option("cloudFiles.validateOptions", "true")
      .load(external_location)
  )


import re
from uuid import UUID
import base64
def base64_to_bytes(base64_string):
    # Pad the string with '=' to make its length a multiple of 4
    padding = '==' if len(base64_string) % 4 == 2 else '=' if len(base64_string) % 4 == 3 else ''
    return base64.urlsafe_b64decode(base64_string + padding)

def bytes_to_uuid(byte_value):
    return str(UUID(bytes=byte_value))

def uuid_from_sanitized_base64(base64_sanitized_value):
    base64_standard = base64_sanitized_value.replace('@', '+').replace('-', '/')
    return bytes_to_uuid(base64_to_bytes(base64_standard))
  
def extract_recipient_id_from_requester(input_string):
    match = re.search(r'r=([^,]+)', input_string)
    if match:
        sanitized_base64_value = match.group(1)
        return uuid_from_sanitized_base64(sanitized_base64_value)
    else:
        return None
extract_recipient_id_from_requester_udf = udf(extract_recipient_id_from_requester, StringType())



# Parse the logs
PATTERN = r'(\S+) (\S+) \[(\S+ \+\d{4})\] (\S+) (\S+) (\S+) (\S+) (\S+) "(.*?)" (\S+) (\S+) (\S+) (\S+) (\S+) (\S+) "(.*?)" "(.*?)" (\S+) (\S+) (\S+) (\S+) (\S+) (\S+) (\S+)'

schema = StructType([
    StructField("bucket_owner", StringType(), True),
    StructField("bucket", StringType(), True),
    StructField("request_datetime", StringType(), True),
    StructField("remote_ip", StringType(), True),
    StructField("requester", StringType(), True),
    StructField("request_id", StringType(), True),
    StructField("operation", StringType(), True),
    StructField("key", StringType(), True),
    StructField("request_uri", StringType(), True),
    StructField("http_status", StringType(), True),
    StructField("error_code", StringType(), True),
    StructField("bytes_sent", StringType(), True),
    StructField("object_size", StringType(), True),
    StructField("total_time", StringType(), True),
    StructField("turn_around_time", StringType(), True),
    StructField("referrer", StringType(), True),
    StructField("user_agent", StringType(), True),
    StructField("version_id", StringType(), True),
    StructField("host_id", StringType(), True),
    StructField("signature_version", StringType(), True),
    StructField("cipher_suite", StringType(), True),
    StructField("authentication_type", StringType(), True),
    StructField("host_header", StringType(), True),
    StructField("tls_version", StringType(), True)
])
@dlt.table()
def access_log_csv():
  df = dlt.read_stream("access_logs")
  for i, field in enumerate(schema.fields):
    field_name = field.name
    df = df.withColumn(field_name, regexp_extract(df['value'], PATTERN, i + 1))

  date_format = "dd/MMM/yyyy:HH:mm:ss Z"
  return (
    df.drop('value')
      .withColumn("bytes_sent", df["bytes_sent"].cast(IntegerType()))
      .withColumn('request_uri', split(df['request_uri'], ' ').getItem(1))
      .withColumn('request_date', to_date(df["request_datetime"], date_format))
      .withColumn('recipient_id', extract_recipient_id_from_requester_udf(df["requester"]))
  )

# COMMAND ----------

# DBTITLE 1,Filter the logs relevant for Delta Sharing egress cost attribution
@dlt.table()
def access_log_filtered():
  df = dlt.read_stream("access_log_csv")
  return (
    df.where(df.http_status == "200")
      .where((df.bytes_sent.isNotNull()) & (df.bytes_sent > 0))
      .where(df.request_uri.isNotNull())
      .where(df.recipient_id.isNotNull())
      .select("request_datetime", "request_date", "requester", "request_uri", "remote_ip", "bytes_sent", "recipient_id")
  )

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql import Row
import requests

def get_api_to_get_recipients():
    # Access token and workspace URL retrieval within Databricks
    access_token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
    databricks_instance = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().get()

    # Set up the headers for authorization
    headers = {
        'Authorization': f'Bearer {access_token}'
    }

    # API endpoint - replace with the correct API endpoint for recipients
    recipients_endpoint = f'{databricks_instance}/api/2.1/unity-catalog/recipients'

    # Make the API call
    response = requests.get(recipients_endpoint, headers=headers)
    # Check if the API call was successful
    if response.status_code == 200:
        recipients_list = response.json().get('recipients', [])     
    else:
        response.raise_for_status()
    return recipients_list

if (use_workaround):
  recipients_list = get_api_to_get_recipients()

  @dlt.table()
  def temp_recipients():
      # Create a DataFrame with recipientName and metastore_id columns
      recipients_df = spark.createDataFrame([Row(recipient_name=recipient.get('name'), recipient_id=recipient.get('id')) for recipient in recipients_list])
      return recipients_df

# COMMAND ----------

# DBTITLE 1,Construct table / share / recipient information table
@dlt.table()
def table_share_info():
  if (use_workaround): 
    recipients_table_name = "LIVE.temp_recipients"
  else:
    recipients_table_name = system.information_schema.recipients
  joined_df = spark.sql(f"""
    SELECT 
        tables.table_catalog, 
        tables.table_schema, 
        tables.table_name, 
        recipients.recipient_name,
        recipients.recipient_id,
        CONCAT("%", replace(tables.storage_sub_directory, '/', '\%2F'), "%") AS url_pattern,
        table_share_usage.share_name
    FROM {recipients_table_name} recipients
    INNER JOIN system.information_schema.share_recipient_privileges share_recipient_privileges
      ON recipients.recipient_name =  share_recipient_privileges.recipient_name
    LEFT OUTER JOIN system.information_schema.table_share_usage table_share_usage
        ON table_share_usage.share_name = share_recipient_privileges.share_name
    LEFT OUTER JOIN system.information_schema.tables tables
        ON tables.table_name = table_share_usage.table_name
         AND tables.table_schema = table_share_usage.schema_name
         AND tables.table_catalog = table_share_usage.catalog_name
  """)
  return (
    joined_df
    .groupBy("table_catalog", "table_schema", "table_name", "recipient_id", "recipient_name", "url_pattern")
    .agg(F.concat_ws(', ', F.collect_list(joined_df.share_name)).alias("shares_list"))
  )

# COMMAND ----------

# DBTITLE 1,Join access logs with table share information and aggregate daily
# Join System Tables 
@dlt.table()
def access_log_with_table():
  # join the join_table with the access_log_csv table
  return spark.sql(f"""
    SELECT /*+ BROADCAST(table_share_info) */ 
      logs.recipient_id,
      recipient_name,
      shares_list,
      request_date,
      requester,
      request_uri,
      remote_ip,
      table_catalog, 
      table_schema,
      table_name,
      url_pattern,
      region,
      service,
      bytes_sent
    FROM LIVE.access_log_filtered logs
    LEFT OUTER JOIN LIVE.table_share_info
      ON (table_share_info.url_pattern IS NOT NULL 
        AND logs.request_uri LIKE table_share_info.url_pattern)
        AND logs.recipient_id = table_share_info.recipient_id
    LEFT OUTER JOIN {cloud_ip_table} ip_region_table
      ON logs.remote_ip = ip_region_table.ip
    """)

# Aggregate for the final result
@dlt.table()
def access_log_with_table_agg():
  group_cols = ["recipient_id", "recipient_name","table_catalog","table_schema","table_name","url_pattern","shares_list","request_date","service","region"]
  df = (
    dlt.read("access_log_with_table") 
    .groupBy(group_cols) 
    .agg(sum("bytes_sent").alias("sum_bytes_sent")) 
  )
  return df