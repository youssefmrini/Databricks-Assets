# Databricks notebook source
# MAGIC %md
# MAGIC # Online Feature Store example notebook
# MAGIC
# MAGIC This notebook illustrates the use of Databricks Feature Store to publish features to Databricks Online Tables for real-time 
# MAGIC serving and automated feature lookup. The problem is to predict the wine quality using a ML model
# MAGIC with a variety of static wine features and a realtime input.
# MAGIC
# MAGIC This notebook creates an endpoint to predict the quality of a bottle of wine, given an ID and the realtime feature alcohol by volume (ABV).
# MAGIC
# MAGIC The notebook is structured as follows:
# MAGIC  
# MAGIC 1. Prepare the feature table.
# MAGIC 2. Set up Databricks Online Table.
# MAGIC     * This notebook uses Databricks Online Tables. For a list of supported functionality, see the Databricks documentation ([AWS](https://docs.databricks.com/machine-learning/feature-store/online-tables.html)|[Azure](https://learn.microsoft.com/azure/databricks/machine-learning/feature-store/online-tables)).  
# MAGIC 3. Train and deploy the model.
# MAGIC 4. Serve realtime queries with automatic feature lookup.
# MAGIC 5. Clean up.
# MAGIC
# MAGIC ### Data Set
# MAGIC
# MAGIC This example uses the [Wine Quality Data Set](https://archive.ics.uci.edu/ml/datasets/wine+quality).
# MAGIC
# MAGIC ### Requirements
# MAGIC
# MAGIC * Databricks Runtime 14.2 for Machine Learning or above.
# MAGIC * Sign up for Gated Public Preview of Databricks Online Tables [form](https://docs.google.com/forms/d/e/1FAIpQLSdmEB1r-t3ryPTvX2fo9L87RATUPNEuGI08l0ML2kVlxdPO-w/viewform?usp=send_form).

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://docs.databricks.com/_static/images/machine-learning/feature-store/online-tables-nb-diagram.png"/>

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Prepare the feature table
# MAGIC
# MAGIC Suppose you need to build an endpoint to predict wine quality with just the `wine_id`. This requires a feature table saved in Feature Store where the endpoint can look up features of the wine by the `wine_id`. For the purpose of this demo, we need to prepare this feature table ourselves first. The steps are:
# MAGIC
# MAGIC 1. Load and clean the raw data.
# MAGIC 2. Separate features and labels.
# MAGIC 3. Save features into a feature table.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load and clean the raw data 
# MAGIC
# MAGIC The raw data contains 12 columns including 11 features and the `quality` column. The `quality` column is an integer that ranges from 3 to 8. The goal is to build a model that predicts the `quality` value.

# COMMAND ----------

raw_data_frame = spark.read.load("/databricks-datasets/wine-quality/winequality-red.csv",format="csv",sep=";",inferSchema="true",header="true" )
display(raw_data_frame.limit(10))

# COMMAND ----------

# Have a look at the size of the raw data.
raw_data_frame.toPandas().shape

# COMMAND ----------

# MAGIC %md
# MAGIC There are some problems with the raw data:
# MAGIC 1. The column names contain space (' '), which is not compatible with Feature Store. 
# MAGIC 2. We need to add ID to the raw data so they can be looked up later by Feature Store.
# MAGIC
# MAGIC The following cell addresses these issues.

# COMMAND ----------

from sklearn.preprocessing import MinMaxScaler
from pyspark.sql.functions import monotonically_increasing_id


def addIdColumn(dataframe, id_column_name):
    columns = dataframe.columns
    new_df = dataframe.withColumn(id_column_name, monotonically_increasing_id())
    return new_df[[id_column_name] + columns]

def renameColumns(df):
    renamed_df = df
    for column in df.columns:
        renamed_df = renamed_df.withColumnRenamed(column, column.replace(' ', '_'))
    return renamed_df


# Rename columns so that they are compatible with Feature Store
renamed_df = renameColumns(raw_data_frame)

# Add id column
id_and_data = addIdColumn(renamed_df, 'wine_id')

display(id_and_data)

# COMMAND ----------

# MAGIC %md
# MAGIC Let's assume that the alcohol by volume (ABV) is a variable that changes over time after the wine is opened. The value will be provided as a real-time input for online inference. 
# MAGIC
# MAGIC Now, split the data into two parts and store only the part with static features to Feature Store. 

# COMMAND ----------

# wine_id and static features
id_static_features = id_and_data.drop('alcohol', 'quality')

# wine_id, realtime feature (alcohol), label (quality)
id_rt_feature_labels = id_and_data.select('wine_id', 'alcohol', 'quality')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create a feature table
# MAGIC
# MAGIC Save the feature data `id_static_features` into a feature table.

# COMMAND ----------

# You must have `CREATE CATALOG` privileges on the catalog.
# If necessary, change the catalog and schema name here.
username = spark.sql("SELECT current_user()").first()["current_user()"]
username = username.split(".")[0]
catalog_name = username

# Fetch the username to use as the schema name.
schema_name = "online_tables"

spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog_name}")
spark.sql(f"USE CATALOG {catalog_name}")
spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog_name}.{schema_name}")
spark.sql(f"USE SCHEMA {schema_name}")

# COMMAND ----------

from databricks.feature_store.client import FeatureStoreClient

wine_table = f"{catalog_name}.{schema_name}.wine_static_features"

fs = FeatureStoreClient()
fs.create_table(
    name=wine_table,
    primary_keys=["wine_id"],
    df=id_static_features,
    description="id and features of all wine",
)

# COMMAND ----------

# MAGIC %md
# MAGIC The feature data has now been stored into the feature table. The next step is to set up a Databricks Online Table.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Set up Databricks Online Tables
# MAGIC
# MAGIC You create an online table from the Catalog Explorer. The steps are described below. For more details, see the Databricks documentation ([AWS](https://docs.databricks.com/en/machine-learning/feature-store/online-tables.html#create)|[Azure](https://learn.microsoft.com/azure/databricks/machine-learning/feature-store/online-tables#create)). For information about required permissions, see Permissions ([AWS](https://docs.databricks.com/en/machine-learning/feature-store/online-tables.html#user-permissions)|[Azure](https://learn.microsoft.com/azure/databricks/machine-learning/feature-store/online-tables#user-permissions)).
# MAGIC
# MAGIC
# MAGIC In Catalog Explorer, navigate to the source table that you want to sync to an online table. From the kebab menu, select **Create online table**.
# MAGIC
# MAGIC * Use the selectors in the dialog to configure the online table.
# MAGIC   * Name: Name to use for the online table in Unity Catalog.
# MAGIC   * Primary Key: Column(s) in the source table to use as primary key(s) in the online table.
# MAGIC   * Timeseries Key: (Optional). Column in the source table to use as timeseries key. When specified, the online table includes only the row with the latest timeseries key value for each primary key.
# MAGIC   * Sync mode: Specifies how the synchronization pipeline updates the online table. Select one of Snapshot, Triggered, or Continuous.
# MAGIC   * Policy
# MAGIC     * Snapshot - The pipeline runs once to take a snapshot of the source table and copy it to the online table. Subsequent changes to the source table are automatically reflected in the online table by taking a new snapshot of the source and creating a new copy. The content of the online table is updated atomically.
# MAGIC     * Triggered - The pipeline runs once to create an initial snapshot copy of the source table in the online table. Unlike the Snapshot sync mode, when the online table is refreshed, only changes since the last pipeline execution are retrieved and applied to the online table. The incremental refresh can be manually triggered or automatically triggered according to a schedule.
# MAGIC     * Continuous - The pipeline runs continuously. Subsequent changes to the source table are incrementally applied to the online table in real time streaming mode. No manual refresh is necessary.
# MAGIC * When you are done, click Confirm. The online table page appears.
# MAGIC
# MAGIC The new online table is created under the catalog, schema, and name specified in the creation dialog. In Catalog Explorer, the online table is indicated by online table icon.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Train and deploy the model
# MAGIC
# MAGIC Now, you will train a classifier using features in the Feature Store. You only need to specify the primary key, and Feature Store will fetch the required features.

# COMMAND ----------

from sklearn.ensemble import RandomForestClassifier

import pandas as pd
import logging
import mlflow.sklearn

from databricks.feature_store.entities.feature_lookup import FeatureLookup

# COMMAND ----------

# MAGIC %md
# MAGIC First, define a `TrainingSet`. The training set accepts a `feature_lookups` list, where each item represents some features from a feature table in the Feature Store. This example uses `wine_id` as the lookup key to fetch all the features from table `online_feature_store_example.wine_features`.

# COMMAND ----------

training_set = fs.create_training_set(
    id_rt_feature_labels,
    label='quality',
    feature_lookups=[
        FeatureLookup(
            table_name=f"{catalog_name}.{schema_name}.wine_static_features",
            lookup_key="wine_id"
        )
    ],
    exclude_columns=['wine_id'],
)

# Load the training data from Feature Store
training_df = training_set.load_df()

display(training_df)

# COMMAND ----------

# MAGIC %md
# MAGIC The next cell trains a RandomForestClassifier model.

# COMMAND ----------

X_train = training_df.drop('quality').toPandas()
y_train = training_df.select('quality').toPandas()

# Train model
model = RandomForestClassifier()
model.fit(X_train, y_train.values.ravel())

# COMMAND ----------

# MAGIC %md
# MAGIC Save the trained model using `log_model`. `log_model` also saves lineage information between the model and the features (through `training_set`). So, during serving, the model automatically knows where to fetch the features by just the lookup keys.

# COMMAND ----------

mlflow.set_registry_uri("databricks-uc")

fs.log_model(
    model,
    artifact_path="model",
    flavor=mlflow.sklearn,
    training_set=training_set,
    registered_model_name=f"{catalog_name}.{schema_name}.wine_classifier"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Serve real-time queries with automatic feature lookup
# MAGIC
# MAGIC After calling `log_model`, a new version of the model is saved. To provision a serving endpoint, follow the steps below.
# MAGIC
# MAGIC 1. In the left sidebar, click **Serving**.
# MAGIC 2. To create a new serving endpoint, click **Create serving endpoint**.   
# MAGIC   a. In the **Name** field, type a name for the endpoint.  
# MAGIC   b. Click in the **Entity** field. A dialog appears. Select **Unity catalog model**, and then select the catalog, schema, and model from the drop-down menus.  
# MAGIC   c. In the **Version** drop-down menu, select the version of the model to use.  
# MAGIC   d. Click **Confirm**.  
# MAGIC   e. In the **Compute Scale-out** drop-down, select Small, Medium, or Large. If you want to use GPU serving, select a GPU type from the **Compute type** drop-down menu.  
# MAGIC   e. Click **Create**. The endpoint page opens and the endpoint creation process starts.   
# MAGIC   
# MAGIC See the Databricks documentation for details ([AWS](https://docs.databricks.com/machine-learning/model-serving/create-manage-serving-endpoints.html#ui-workflow)|[Azure](https://learn.microsoft.com/azure/databricks/machine-learning/model-serving/create-manage-serving-endpoints#--ui-workflow)).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Send a query
# MAGIC
# MAGIC In the Serving page, there are three approaches for calling the model. You can try the "Browser" approach with a JSON format request, as shown below. But here we copy-pasted the Python approach to illustrate an programatic way.

# COMMAND ----------

# Fill in the Databricks access token value.
# Note: You can generate a new Databricks access token by going to left sidebar "Settings" > "User Settings" > "Access Tokens", or using databricks-cli.

DATABRICKS_TOKEN = "<DATABRICKS_TOKEN>"
assert DATABRICKS_TOKEN.strip() != "<DATABRICKS_TOKEN>"

# COMMAND ----------

import os
import requests
import numpy as np
import pandas as pd
import json

def create_tf_serving_json(data):
    return {'inputs': {name: data[name].tolist() for name in data.keys()} if isinstance(data, dict) else data.tolist()}

def score_model(dataset):
    url = '<Replace with the URL shown in Serving page>'
    headers = {'Authorization': f'Bearer {DATABRICKS_TOKEN}', 'Content-Type': 'application/json'}
    ds_dict = {'dataframe_split': dataset.to_dict(orient='split')} if isinstance(dataset, pd.DataFrame) else create_tf_serving_json(dataset)
    data_json = json.dumps(ds_dict, allow_nan=True)
    print(data_json)
    response = requests.request(method='POST', headers=headers, url=url, data=data_json)
    if response.status_code != 200:
        raise Exception(f'Request failed with status {response.status_code}, {response.text}')
    return response.json()

# COMMAND ----------

# MAGIC %md
# MAGIC Now, suppose you opened a bottle of wine and you have a sensor to measure the current ABV from the bottle. Using the model and automated feature lookup with realtime serving, you can predict the quality of the wine using the measured ABV value as the realtime input "alcohol".

# COMMAND ----------

new_wine_ids = pd.DataFrame([(25, 7.9), (25, 11.0), (25, 27.9)], columns=['wine_id', "alcohol"])

print(score_model(new_wine_ids))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Notes on request format and API versions
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Here is an example of the request format:
# MAGIC ```
# MAGIC {"dataframe_split": {"index": [0, 1, 2], "columns": ["wine_id", "alcohol"], "data": [[25, 7.9], [25, 11.0], [25, 27.9]]}}
# MAGIC ```
# MAGIC
# MAGIC Learn more about Databricks Model Serving ([AWS](https://docs.databricks.com/en/machine-learning/model-serving/index.html)|[Azure](https://learn.microsoft.com/en-us/azure/databricks/machine-learning/model-serving/)).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clean up
# MAGIC
# MAGIC To clean up the resources created by this notebook, follow these steps:
# MAGIC
# MAGIC 1. Delete the Databricks Online Table from Catalog Explorer.  
# MAGIC   a. In the left sidebar, click **Catalog**.   
# MAGIC   b. Navigate to the online table.  
# MAGIC   c. From the kebab menu, select **Delete**.  
# MAGIC 2. Delete the Serving Endpoint from the **Serving** tab.  
# MAGIC   a. In the left sidebar, click **Serving**.  
# MAGIC   b. Click the name of the endpoint.  
# MAGIC   c. From the kebab menu, select **Delete**.