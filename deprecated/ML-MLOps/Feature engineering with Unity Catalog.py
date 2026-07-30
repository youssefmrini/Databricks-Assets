# Databricks notebook source

#Write a code to hash an email


# COMMAND ----------

# MAGIC %md
# MAGIC #Feature Engineering in Unity Catalog
# MAGIC
# MAGIC This notebook illustrates the use of Feature Engineering in Unity Catalog to create a model that predicts NYC Yellow Taxi fares. It includes these steps:
# MAGIC
# MAGIC - Compute and write time series features.
# MAGIC - Train a model using these features to predict fares.
# MAGIC - Evaluate that model on a new batch of data using existing features, saved to Feature Store.
# MAGIC
# MAGIC ## Requirements
# MAGIC - Databricks Runtime 13.2 for Machine Learning or above
# MAGIC   - Alternatively, you may use Databricks Runtime by running `%pip install databricks-feature-store` at the start of this notebook.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://docs.databricks.com/_static/images/machine-learning/feature-store/taxi_example_flow_v3.png"/>
# MAGIC

# COMMAND ----------

# MAGIC %md ## Compute features

# COMMAND ----------

raw_data = spark.read.format("delta").load("/databricks-datasets/nyctaxi-with-zipcodes/subsampled")
display(raw_data)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC From the taxi fares transactional data, we will compute two groups of features based on trip pickup and drop off zip codes.
# MAGIC
# MAGIC #### Pickup features
# MAGIC 1. Count of trips (time window = 1 hour, sliding window = 15 minutes)
# MAGIC 1. Mean fare amount (time window = 1 hour, sliding window = 15 minutes)
# MAGIC
# MAGIC #### Drop off features
# MAGIC 1. Count of trips (time window = 30 minutes)
# MAGIC 1. Does trip end on the weekend (custom feature using python code)
# MAGIC
# MAGIC <img src="https://docs.databricks.com/_static/images/machine-learning/feature-store/taxi_example_computation_v5.png"/>
# MAGIC

# COMMAND ----------

# MAGIC %md ### Helper functions

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import FloatType, IntegerType, StringType
from pytz import timezone


@udf(returnType=IntegerType())
def is_weekend(dt):
    tz = "America/New_York"
    return int(dt.astimezone(timezone(tz)).weekday() >= 5)  # 5 = Saturday, 6 = Sunday


def filter_df_by_ts(df, ts_column, start_date, end_date):
    if ts_column and start_date:
        df = df.filter(col(ts_column) >= start_date)
    if ts_column and end_date:
        df = df.filter(col(ts_column) < end_date)
    return df

# COMMAND ----------

# MAGIC %md ###  Code to compute features

# COMMAND ----------

def pickup_features_fn(df, ts_column, start_date, end_date):
    
    df = filter_df_by_ts(df, ts_column, start_date, end_date)
    pickupzip_features = (
        df.groupBy(
            "pickup_zip", window("tpep_pickup_datetime", "1 hour", "15 minutes")
        )  
        .agg(
            mean("fare_amount").alias("mean_fare_window_1h_pickup_zip"),
            count("*").alias("count_trips_window_1h_pickup_zip"),
        )
        .select(
            col("pickup_zip").alias("zip"),
            unix_timestamp(col("window.end")).cast("timestamp").alias("ts"),
            col("mean_fare_window_1h_pickup_zip").cast(FloatType()),
            col("count_trips_window_1h_pickup_zip").cast(IntegerType()),
        )
    )
    return pickupzip_features


def dropoff_features_fn(df, ts_column, start_date, end_date):
   
    df = filter_df_by_ts(df, ts_column, start_date, end_date)
    dropoffzip_features = (
        df.groupBy("dropoff_zip", window("tpep_dropoff_datetime", "30 minute"))
        .agg(count("*").alias("count_trips_window_30m_dropoff_zip"))
        .select(
            col("dropoff_zip").alias("zip"),
            unix_timestamp(col("window.end")).cast("timestamp").alias("ts"),
            col("count_trips_window_30m_dropoff_zip").cast(IntegerType()),
            is_weekend(col("window.end")).alias("dropoff_is_weekend"),
        )
    )
    return dropoffzip_features

# COMMAND ----------

from datetime import datetime

pickup_features = pickup_features_fn(
    df=raw_data,
    ts_column="tpep_pickup_datetime",
    start_date=datetime(2016, 1, 1),
    end_date=datetime(2016, 1, 31),
)
dropoff_features = dropoff_features_fn(
    df=raw_data,
    ts_column="tpep_dropoff_datetime",
    start_date=datetime(2016, 1, 1),
    end_date=datetime(2016, 1, 31),
)

# COMMAND ----------

display(pickup_features)

# COMMAND ----------

# MAGIC %md ### Create a Catalog and a Schema

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC CREATE CATALOG ml_feature_store;
# MAGIC USE CATALOG ml_feature_store;
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS taxi_example;
# MAGIC USE SCHEMA taxi_example;

# COMMAND ----------

# MAGIC %md the Feature Store client

# COMMAND ----------

from databricks import feature_store
fs = feature_store.FeatureStoreClient()

# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", "5")

fs.create_table(
    name="ml_feature_store.taxi_example.trip_pickup_time_series_features",
    primary_keys=["zip", "ts"],
    timestamp_keys=["ts"],
    df=pickup_features,
    description="Taxi Fares. Pickup Time Series Features",
)
fs.create_table(
    name="ml_feature_store.taxi_example.trip_dropoff_time_series_features",
    primary_keys=["zip", "ts"],
    timestamp_keys=["ts"],
    df=dropoff_features,
    description="Taxi Fares. Dropoff Time Series Features",
)

# COMMAND ----------

# MAGIC %md ## Update features
# MAGIC
# MAGIC Use the `write_table` function to update the feature table values.
# MAGIC
# MAGIC <img src="https://docs.databricks.com/_static/images/machine-learning/feature-store/taxi_example_compute_and_write.png"/>

# COMMAND ----------

display(raw_data)

# COMMAND ----------

new_pickup_features = pickup_features_fn(
    df=raw_data,
    ts_column="tpep_pickup_datetime",
    start_date=datetime(2016, 2, 1),
    end_date=datetime(2016, 2, 29),
)
fs.write_table(
    name="ml_feature_store.taxi_example.trip_pickup_time_series_features",
    df=new_pickup_features,
    mode="merge",
)

new_dropoff_features = dropoff_features_fn(
    df=raw_data,
    ts_column="tpep_dropoff_datetime",
    start_date=datetime(2016, 2, 1),
    end_date=datetime(2016, 2, 29),
)
fs.write_table(
    name="ml_feature_store.taxi_example.trip_dropoff_time_series_features",
    df=new_dropoff_features,
    mode="merge",
)

# COMMAND ----------

# MAGIC %md  AP

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   SUM(count_trips_window_30m_dropoff_zip) AS num_rides,
# MAGIC   dropoff_is_weekend
# MAGIC FROM
# MAGIC   ml_feature_store.taxi_example.trip_dropoff_time_series_features
# MAGIC WHERE
# MAGIC   dropoff_is_weekend IS NOT NULL
# MAGIC GROUP BY
# MAGIC   dropoff_is_weekend;

# COMMAND ----------

# MAGIC %md ## Train a model
# MAGIC
# MAGIC This section illustrates how to create a training set with the time series pickup and dropoff feature tables using point-in-time lookup and train a model using the training set. It trains a LightGBM model to predict taxi fare.

# COMMAND ----------

# MAGIC %md ### Helper functions

# COMMAND ----------

import mlflow.pyfunc

def get_latest_model_version(model_name):
    latest_version = 1
    mlflow_client = MlflowClient()
    for mv in mlflow_client.search_model_versions(f"name='{model_name}'"):
        version_int = int(mv.version)
        if version_int > latest_version:
            latest_version = version_int
    return latest_version

# COMMAND ----------

# MAGIC %md ### Understanding how a training dataset is created
# MAGIC
# MAGIC In order to train a model, you need to create a training dataset that is used to train the model.  The training dataset is comprised of:
# MAGIC
# MAGIC 1. Raw input data
# MAGIC 1. Features from the feature store
# MAGIC
# MAGIC The raw input data is needed because it contains:
# MAGIC
# MAGIC 1. Primary keys and timestamp keys are used to join with features with point-in-time correctness ([AWS](https://docs.databricks.com/machine-learning/feature-store/time-series.html#create-a-training-set-with-a-time-series-feature-table)|[Azure](https://learn.microsoft.com/en-us/azure/databricks/machine-learning/feature-store/time-series#create-a-training-set-with-a-time-series-feature-table)|[GCP](https://docs.gcp.databricks.com/machine-learning/feature-store/train-models-with-feature-store.html#create-a-training-dataset))[](https://docs.gcp.databricks.com/machine-learning/feature-store/time-series.html#create-a-training-set-with-a-time-series-feature-table).
# MAGIC 1. Raw features like `trip_distance` that are not in the feature store.
# MAGIC 1. Prediction targets like `fare` that are required for model training.
# MAGIC
# MAGIC Here's a visual overview that shows the raw input data being combined with the features in the Feature Store to produce the training dataset:
# MAGIC
# MAGIC <img src="https://docs.databricks.com/_static/images/machine-learning/feature-store/taxi_example_feature_lookup_with_pit.png"/>
# MAGIC
# MAGIC
# MAGIC These concepts are described further in the Creating a Training Dataset documentation ([AWS](https://docs.databricks.com/machine-learning/feature-store/train-models-with-feature-store.html#create-a-training-dataset)|[Azure](https://learn.microsoft.com/en-us/azure/databricks/machine-learning/feature-store/train-models-with-feature-store#create-a-training-dataset)|[GCP](https://docs.gcp.databricks.com/machine-learning/feature-store/train-models-with-feature-store.html#create-a-training-dataset)).
# MAGIC
# MAGIC The next cell loads features from Feature Store for model training by creating a `FeatureLookup` for each needed feature.
# MAGIC
# MAGIC To perform a point-in-time lookup for feature values from a time series feature table, you must specify a `timestamp_lookup_key` in the feature’s `FeatureLookup`, which indicates the name of the DataFrame column that contains timestamps against which to lookup time series features. For each row in the DataFrame, Databricks Feature Store retrieves the latest feature values prior to the timestamps specified in the DataFrame’s `timestamp_lookup_key` column and whose primary keys match the values in the DataFrame’s `lookup_key` columns, or `null` if no such feature value exists.

# COMMAND ----------

from databricks.feature_store import FeatureLookup
import mlflow

pickup_features_table = "ml_feature_store.taxi_example.trip_pickup_time_series_features"
dropoff_features_table = "ml_feature_store.taxi_example.trip_dropoff_time_series_features"

pickup_feature_lookups = [
    FeatureLookup(
        table_name=pickup_features_table,
        feature_names=[
            "mean_fare_window_1h_pickup_zip",
            "count_trips_window_1h_pickup_zip",
        ],
        lookup_key=["pickup_zip"],
        timestamp_lookup_key="tpep_pickup_datetime",
    ),
]

dropoff_feature_lookups = [
    FeatureLookup(
        table_name=dropoff_features_table,
        feature_names=["count_trips_window_30m_dropoff_zip", "dropoff_is_weekend"],
        lookup_key=["dropoff_zip"],
        timestamp_lookup_key="tpep_dropoff_datetime",
    ),
]

# COMMAND ----------

# MAGIC %md
# MAGIC When `fs.create_training_set(..)` is invoked, the following steps take place:
# MAGIC
# MAGIC 1. A `TrainingSet` object is created, which selects specific features from Feature Store to use in training your model. Each feature is specified by the `FeatureLookup`'s created previously. 
# MAGIC
# MAGIC 1. Features are joined with the raw input data according to each `FeatureLookup`'s `lookup_key`.
# MAGIC
# MAGIC 1. Point-in-Time lookup is applied to avoid data leakage problems. Only the most recent feature values, based on `timestamp_lookup_key`, are joined.
# MAGIC
# MAGIC The `TrainingSet` is then transformed into a DataFrame for training. This DataFrame includes the columns of taxi_data, as well as the features specified in the `FeatureLookups`.

# COMMAND ----------

mlflow.end_run()

mlflow.start_run()


exclude_columns = ["tpep_pickup_datetime", "tpep_dropoff_datetime"]

training_set = fs.create_training_set(
    raw_data,
    feature_lookups=pickup_feature_lookups + dropoff_feature_lookups,
    label="fare_amount",
    exclude_columns=exclude_columns,
)

training_df = training_set.load_df()

# COMMAND ----------

# Display the training dataframe, and note that it contains both the raw input data and the features from the Feature Store, like `dropoff_is_weekend`
display(training_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Train a LightGBM model on the data returned by `TrainingSet.to_df`, then log the model with `FeatureStoreClient.log_model`. The model will be packaged with feature metadata.

# COMMAND ----------

from sklearn.model_selection import train_test_split
from mlflow.tracking import MlflowClient
import lightgbm as lgb
import mlflow.lightgbm
from mlflow.models.signature import infer_signature

features_and_label = training_df.columns

# Collect data into a Pandas array for training
data = training_df.toPandas()[features_and_label]

train, test = train_test_split(data, random_state=123)
X_train = train.drop(["fare_amount"], axis=1)
X_test = test.drop(["fare_amount"], axis=1)
y_train = train.fare_amount
y_test = test.fare_amount

mlflow.lightgbm.autolog()
train_lgb_dataset = lgb.Dataset(X_train, label=y_train.values)
test_lgb_dataset = lgb.Dataset(X_test, label=y_test.values)

param = {"num_leaves": 32, "objective": "regression", "metric": "rmse"}
num_rounds = 100

# Train a lightGBM model
model = lgb.train(param, train_lgb_dataset, num_rounds)

# COMMAND ----------

# Log the trained model with MLflow and package it with feature lookup information.
fs.log_model(
    model,
    artifact_path="model_packaged",
    flavor=mlflow.lightgbm,
    training_set=training_set,
    registered_model_name="taxi_example_fare_unity_catalog",
)

# COMMAND ----------

# MAGIC %md ## Scoring: batch inference

# COMMAND ----------

# MAGIC %md Display the data to use for inference, reordered to highlight the `fare_amount` column, which is the prediction target.

# COMMAND ----------

cols = [
    "fare_amount",
    "trip_distance",
    "pickup_zip",
    "dropoff_zip",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
]
new_taxi_data = raw_data.select(cols)
display(new_taxi_data)

# COMMAND ----------

# MAGIC %md
# MAGIC Use the `score_batch` API to evaluate the model on the batch of data, retrieving needed features from FeatureStore.
# MAGIC
# MAGIC When you score a model trained with features from time series feature tables, Databricks Feature Store retrieves the appropriate features using point-in-time lookups with metadata packaged with the model during training. The DataFrame you provide to `FeatureStoreClient.score_batch` must contain a timestamp column with the same name and DataType as the `timestamp_lookup_key` of the FeatureLookup provided to `FeatureStoreClient.create_training_set`.

# COMMAND ----------

# Get the model URI
latest_model_version = get_latest_model_version(
    "taxi_example_fare_unity_catalog"
)
model_uri = f"models:/taxi_example_fare_unity_catalog/{latest_model_version}"

# Call score_batch to get the predictions from the model
with_predictions = fs.score_batch(model_uri, new_taxi_data)

# COMMAND ----------

# MAGIC %md <img src="https://docs.databricks.com/_static/images/machine-learning/feature-store/taxi_example_score_batch_with_pit.png"/>

# COMMAND ----------

# MAGIC %md ### View the taxi fare predictions
# MAGIC
# MAGIC This code reorders the columns to show the taxi fare predictions in the first column.  Note that the `predicted_fare_amount` roughly lines up with the actual `fare_amount`, although more data and feature engineering would be required to improve the model accuracy.
# MAGIC

# COMMAND ----------

import pyspark.sql.functions as func

cols = [
    "prediction",
    "fare_amount",
    "trip_distance",
    "pickup_zip",
    "dropoff_zip",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "mean_fare_window_1h_pickup_zip",
    "count_trips_window_1h_pickup_zip",
    "count_trips_window_30m_dropoff_zip",
    "dropoff_is_weekend",
]

with_predictions_reordered = (
    with_predictions.select(
        cols,
    )
    .withColumnRenamed(
        "prediction",
        "predicted_fare_amount",
    )
    .withColumn(
        "predicted_fare_amount",
        func.round("predicted_fare_amount", 2),
    )
)

display(with_predictions_reordered)

# COMMAND ----------

