# Databricks notebook source
# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # IP Ranges Mapping Pipeline

# COMMAND ----------

# MAGIC %md
# MAGIC ## How to use
# MAGIC
# MAGIC This is a [Delta Live Table ETL pipeline defined in Python](https://docs.databricks.com/en/delta-live-tables/python-programming.html#python-programming-for-delta-live-tables). The input of the pipeline is [AWS IP ranges file](https://docs.aws.amazon.com/vpc/latest/userguide/aws-ip-ranges.html), and the output is a flatterned table, with one row per each IP with its region for all AWS IP addresses.
# MAGIC
# MAGIC ### Prerequisites
# MAGIC
# MAGIC 1. Proper data access privileges to store the table. 
# MAGIC 2. [Cluster creation permission](https://docs.databricks.com/en/clusters/clusters-manage.html#cluster-permissions) to create and run a DLT pipeline.
# MAGIC
# MAGIC ### Create & Execute the Pipeline
# MAGIC
# MAGIC To create the pipeline, go to `Delta Live Tables` under `Data Engineering`, and click the `Create Pipeline` button. Then fill in the parameters in the UI. Refer to the [instructions](https://docs.databricks.com/en/delta-live-tables/settings.html#configure-pipeline-settings-for-delta-live-tables) for more information.
# MAGIC
# MAGIC * Use this notebook as the source file.
# MAGIC * The pipeline utilizes Python UDF. Due to current [limitations](https://docs.databricks.com/en/delta-live-tables/unity-catalog.html#limitations) of Python UDF with Unity Catalog, it is recommended to use Hive_metastore as destination for this pipeline.
# MAGIC   * Please specify "Target Schema", under which the tables will be populated. The same schema will be specified as input parameter for the egress cost analysis pipelines.
# MAGIC * The pipeline should run on a schedule, commonly once per week but can be less frequent as well.
# MAGIC * The pipeline needs 3 parameters to indicate which cloud providers IPs to parse. For example, when running for AWS egress cost analysis, `ipRanges.includeAws` needs to be `true`, and `ipRanges.includeAzure` \ `ipRanges.includeGcp` can be false.
# MAGIC   * Azure IP range parsing takes longer time and is unreliable due to its size.
# MAGIC
# MAGIC Here is an example configuration in JSON (All the UUIDs are auto-generated from UI):
# MAGIC
# MAGIC ```
# MAGIC {
# MAGIC     "id": "f9f4276d-187d-4208-8b18-06f57d6aaa6f",
# MAGIC     "pipeline_type": "WORKSPACE",
# MAGIC     "clusters": [
# MAGIC         {
# MAGIC             "label": "default",
# MAGIC             "autoscale": {
# MAGIC                 "min_workers": 1,
# MAGIC                 "max_workers": 5,
# MAGIC                 "mode": "ENHANCED"
# MAGIC             }
# MAGIC         }
# MAGIC     ],
# MAGIC     "development": true,
# MAGIC     "continuous": false,
# MAGIC     "channel": "CURRENT",
# MAGIC     "photon": false,
# MAGIC     "libraries": [
# MAGIC         {
# MAGIC             "notebook": {
# MAGIC                 "path": "/Repos/name@example.com/delta-sharing-egress-analysis/IpRanges"
# MAGIC             }
# MAGIC         }
# MAGIC     ],
# MAGIC     "name": "Cloud IP Ranges",
# MAGIC     "serverless": false,
# MAGIC     "edition": "ADVANCED",
# MAGIC     "storage": "dbfs:/pipelines/f9f4276d-187d-4208-8b18-06f57d6aaa6f",
# MAGIC     "configuration": {
# MAGIC         "ipRanges.includeAws": "true",
# MAGIC         "ipRanges.includeAzure": "false",
# MAGIC         "ipRanges.includeGcp": "true"
# MAGIC     },
# MAGIC     "target": "egress_ip_addresses",
# MAGIC     "data_sampling": false
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC ### Consume the output
# MAGIC
# MAGIC The output table has 3 columns:
# MAGIC
# MAGIC * `service string` The name of the cloudservice using the IP.
# MAGIC * `region string` The name of the region within the cluster provider.
# MAGIC * `ip string` Concrete IP address.
# MAGIC

# COMMAND ----------

import dlt
import pandas as pd
import requests
import json
from pyspark.sql.functions import udf, explode, col
from pyspark.sql.types import ArrayType, StringType, StructType, StructField
import ipaddress
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, desc

def get_boolean_config(config_name, default):
  return spark.conf.get(config_name, str(default)).lower() == 'true'
include_aws = get_boolean_config("ipRanges.includeAws", True)
include_azure = get_boolean_config("ipRanges.includeAzure", False)
include_gcp = get_boolean_config("ipRanges.includeGcp", False)

schema = StructType([
  StructField("service", StringType(), True),
  StructField("region", StringType(), True),
  StructField("ip_prefix", StringType(), True)
])

def emptyDataframe():
  return spark.createDataFrame([], schema)

@dlt.table()
def ip_ranges_gcp():
  if not include_gcp:
    return emptyDataframe()
  
  url = "https://www.gstatic.com/ipranges/cloud.json"
  response = requests.get(url)
  content = json.loads(response.content)
  data = pd.json_normalize(content['prefixes'])
  df = pd.DataFrame(data)
  return (
    spark
      .createDataFrame(df)
      .filter("ipv4Prefix is not null")
      .filter("ipv4Prefix <> 'NaN'")
      .withColumnRenamed("ipv4Prefix", "ip_prefix")
      .withColumnRenamed("scope", "region")
      .drop("ipv6Prefix")
  )

@dlt.table()
def ip_ranges_azure():
  if not include_azure:
    return emptyDataframe()
  
  url = "https://www.microsoft.com/en-us/download/confirmation.aspx?id=56519"

  headers = {'User-Agent': "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36"}

  response = requests.get(url, headers=headers)
  start_link = response.text.find("https://download.microsoft.com/download/")
  end_link = response.text.find(".json", start_link) + 5
  azure_url = response.text[start_link:end_link]
  azure_data = requests.get(azure_url).json()["values"]
  result = []
  for item in  azure_data:
    for ip_prefix in item["properties"]["addressPrefixes"]:
      result.append({"service": item["name"], "region": item["properties"]["region"], "ip_prefix":ip_prefix})

  # Create a Spark DataFrame from the list of dictionaries
  return spark.createDataFrame(result)

@dlt.table()
def ip_ranges_aws():
  if not include_aws:
    return emptyDataframe()
  
  url = "https://ip-ranges.amazonaws.com/ip-ranges.json"
  response = requests.get(url)
  content = json.loads(response.content)
  data = pd.json_normalize(content['prefixes'])
  df = pd.DataFrame(data)
  return spark.createDataFrame(df).drop('network_border_group')

@udf(returnType=ArrayType(StringType()))
def cidr_to_ips(cidr):
  # Convert a CIDR range to a list of IPs.
  return [str(ip) for ip in ipaddress.ip_network(cidr, strict=False)]
  
def explode_ip_prefix(df):
  return df.withColumn("ip", explode(cidr_to_ips(col("ip_prefix")))).drop("ip_prefix")
    
@dlt.table()
def ip_with_region_aws():
  return explode_ip_prefix(dlt.read("ip_ranges_aws"))

@dlt.table()
def ip_with_region_azure():
  return explode_ip_prefix(dlt.read("ip_ranges_azure"))

@dlt.table()
def ip_with_region_gcp():
  return explode_ip_prefix(dlt.read("ip_ranges_gcp"))

# Show the results
@dlt.table()
def ip_with_region_all():
  # This is to make sure there is at most 1 record per IP, since AWS IP range records can overlap with each other.
  # For example, 
  # region	service	ip
  # us-east-2	AMAZON	18.226.121.141
  # us-east-2	EC2	18.226.121.141
  windowSpec = Window.partitionBy("ip").orderBy(desc("service"))
  df = (dlt.read("ip_with_region_aws").union(dlt.read("ip_with_region_gcp")).union(dlt.read("ip_with_region_azure"))
    .withColumn("row_number", row_number().over(windowSpec))
  )

  return df.filter(df.row_number == 1).drop("row_number")