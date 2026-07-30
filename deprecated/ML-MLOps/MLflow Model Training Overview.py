# Databricks notebook source
# MAGIC %md # Databricks ML Quickstart: Model Training
# MAGIC
# MAGIC This notebook provides a quick overview of machine learning model training on Databricks. To train models, you can use libraries like scikit-learn that are preinstalled on the Databricks Runtime for Machine Learning. In addition, you can use MLflow to track the trained models, and Hyperopt with SparkTrials to scale hyperparameter tuning.
# MAGIC
# MAGIC This tutorial covers:
# MAGIC - Part 1: Training a simple classification model with MLflow tracking
# MAGIC - Part 2: Hyperparameter tuning a better performing model with Hyperopt
# MAGIC
# MAGIC For more details on productionizing machine learning on Databricks including model lifecycle management and model inference, see the ML End to End Example ([AWS](https://docs.databricks.com/applications/mlflow/end-to-end-example.html)|[Azure](https://docs.microsoft.com/azure/databricks/applications/mlflow/end-to-end-example)).
# MAGIC
# MAGIC ### Requirements
# MAGIC - Cluster running Databricks Runtime 7.5 ML or above

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Libraries
# MAGIC Import the necessary libraries. These libraries are preinstalled on Databricks Runtime for Machine Learning ([AWS](https://docs.databricks.com/runtime/mlruntime.html)|[Azure](https://docs.microsoft.com/azure/databricks/runtime/mlruntime)) clusters and are tuned for compatibility and performance.

# COMMAND ----------

import mlflow
import numpy as np
import pandas as pd
import sklearn.datasets
import sklearn.metrics
import sklearn.model_selection
import sklearn.ensemble

from hyperopt import fmin, tpe, hp, SparkTrials, Trials, STATUS_OK
from hyperopt.pyll import scope

# COMMAND ----------

import mlflow
mlflow.set_registry_uri("databricks-uc")

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Load data
# MAGIC The tutorial uses a dataset describing different wine samples. The [dataset](https://archive.ics.uci.edu/ml/datasets/Wine) is from the UCI Machine Learning Repository and is included in DBFS ([AWS](https://docs.databricks.com/data/databricks-file-system.html)|[Azure](https://docs.microsoft.com/azure/databricks/data/databricks-file-system)).
# MAGIC The goal is to classify red and white wines by their quality. 
# MAGIC
# MAGIC For more details on uploading and loading from other data sources, see the documentation on working with data ([AWS](https://docs.databricks.com/data/index.html)|[Azure](https://docs.microsoft.com/azure/databricks/data/index)).

# COMMAND ----------

# Load and preprocess data
white_wine = pd.read_csv("/dbfs/databricks-datasets/wine-quality/winequality-white.csv", sep=';')
red_wine = pd.read_csv("/dbfs/databricks-datasets/wine-quality/winequality-red.csv", sep=';')
white_wine['is_red'] = 0.0
red_wine['is_red'] = 1.0
data_df = pd.concat([white_wine, red_wine], axis=0)

# Define classification labels based on the wine quality
data_labels = data_df['quality'] >= 7
data_df = data_df.drop(['quality'], axis=1)

# Split 80/20 train-test
X_train, X_test, y_train, y_test = sklearn.model_selection.train_test_split(
  data_df,
  data_labels,
  test_size=0.2,
  random_state=1
)



# COMMAND ----------

{
"inputs":[
{
    "fixed acidity": 6.2,
    "volatile acidity": 0.2,
    "citric acid": 0.25,
    "residual sugar": 15.0,
    "chlorides": 0.055,
    "free sulfur dioxide": 8.0,
    "total sulfur dioxide": 120.0,
    "density": 0.99767,
    "pH": 3.19,
    "sulphates": 0.53,
    "alcohol": 9.6,
    "is_red": 0.0
}
]
}

# COMMAND ----------

# MAGIC %md ## Part 1. Train a classification model

# COMMAND ----------

# MAGIC %md ### MLflow Tracking
# MAGIC [MLflow tracking](https://www.mlflow.org/docs/latest/tracking.html) allows you to organize your machine learning training code, parameters, and models. 
# MAGIC
# MAGIC You can enable automatic MLflow tracking by using [*autologging*](https://www.mlflow.org/docs/latest/tracking.html#automatic-logging).

# COMMAND ----------

# Enable MLflow autologging for this notebook
mlflow.autolog()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC The following libraries support autologging:
# MAGIC <ul>
# MAGIC   <li> Scikit-learn </li>
# MAGIC   <li>TensorFlow </li>
# MAGIC   <li>Keras </li>
# MAGIC   <li>Gluon </li>
# MAGIC   <li>XGBoost</li>
# MAGIC   <li>LightGBM</li>
# MAGIC   <li>Statsmodels</li>
# MAGIC   <li>Spark </li>
# MAGIC   <li>Fastai</li>
# MAGIC   <li>Pytorch </li>
# MAGIC </ul>

# COMMAND ----------

# MAGIC %md
# MAGIC Next, train a classifier within the context of an MLflow run, which automatically logs the trained model and many associated metrics and parameters. 
# MAGIC
# MAGIC You can supplement the logging with additional metrics such as the model's AUC score on the test dataset.

# COMMAND ----------

with mlflow.start_run(run_name='gradient_boost') as run:
  model = sklearn.ensemble.GradientBoostingClassifier(random_state=0)
  
  # Models, parameters, and training metrics are tracked automatically
  model.fit(X_train, y_train)

  predicted_probs = model.predict_proba(X_test)
  roc_auc = sklearn.metrics.roc_auc_score(y_test, predicted_probs[:,1])
  
  # The AUC score on test data is not automatically logged, so log it manually
  mlflow.log_metric("test_auc", roc_auc)
  print("Test AUC of: {}".format(roc_auc))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC <b>mlflow.log_param()</b> logs a single key-value param in the currently active run.
# MAGIC
# MAGIC <b>mlflow.log_metric() </b> logs a single key-value metric. The value must always be a number.
# MAGIC
# MAGIC <b> mlflow.set_tag() </b> sets a single key-value tag in the currently active run.

# COMMAND ----------

# MAGIC %md
# MAGIC If you aren't happy with the performance of this model, train another model with different hyperparameters.

# COMMAND ----------

# Start a new run and assign a run_name for future reference
with mlflow.start_run(run_name='gradient_boost') as run:
  model_2 = sklearn.ensemble.GradientBoostingClassifier(
    random_state=0, 
    
    # Try a new parameter setting for n_estimators
    n_estimators=200,
  )
  model_2.fit(X_train, y_train)

  predicted_probs = model_2.predict_proba(X_test)
  roc_auc = sklearn.metrics.roc_auc_score(y_test, predicted_probs[:,1])
  mlflow.log_metric("test_auc", roc_auc)
  print("Test AUC of: {}".format(roc_auc))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC <h3>The MLflow Model Registry component </h3> is a centralized model store, set of APIs, and UI, to collaboratively manage the full lifecycle of an MLflow Model. It provides model lineage (which MLflow experiment and run produced the model), model versioning, stage transitions (for example from staging to production), and annotations.

# COMMAND ----------

mlflow.register_model(
    "runs:/{run_id}/model".format(run_id=run.info.run_id), 
    "demo_youssef.youssef.Wine_Quality_AB"
)

# COMMAND ----------

# Start a new run and assign a run_name for future reference


with mlflow.start_run(run_name='gradient_boost') as run:
  model_3 = sklearn.ensemble.GradientBoostingClassifier(
    random_state=0, 
    
    # Try a new parameter setting for n_estimators
    n_estimators=200,
    max_depth=5,
  )
  model_3.fit(X_train, y_train)

  predicted_probs = model_3.predict_proba(X_test)
  roc_auc = sklearn.metrics.roc_auc_score(y_test, predicted_probs[:,1])
  mlflow.log_metric("test_auc", roc_auc)
  mlflow.set_tag("information", "testing purpose")
  mlflow.log_param("delta",3)
  print("Test AUC of: {}".format(roc_auc))
 

# COMMAND ----------

mlflow.register_model(
    "runs:/{run_id}/model".format(run_id=run.info.run_id), 
    "Wine_Quality_AB"
)

# COMMAND ----------

# Start a new run and assign a run_name for future reference
with mlflow.start_run(run_name='RandomForestClassifier') as run:
  model_4 = sklearn.ensemble.RandomForestClassifier(
    random_state=0, 
    # Try a new parameter setting for n_estimators
    n_estimators=200,
    max_depth=5

  )
  model_4.fit(X_train, y_train)

  predicted_probs = model_4.predict_proba(X_test)
  roc_auc = sklearn.metrics.roc_auc_score(y_test, predicted_probs[:,1])
  mlflow.log_metric("test_auc", roc_auc)
  print("Test AUC of: {}".format(roc_auc))

# COMMAND ----------

# MAGIC %md ### View MLflow runs
# MAGIC To view the logged training runs, click the **Experiment** icon at the upper right of the notebook to display the experiment sidebar. If necessary, click the refresh icon to fetch and monitor the latest runs. 
# MAGIC
# MAGIC <img width="350" src="https://docs.databricks.com/_static/images/mlflow/quickstart/experiment-sidebar-icons.png"/>
# MAGIC
# MAGIC You can then click the experiment page icon to display the more detailed MLflow experiment page ([AWS](https://docs.databricks.com/applications/mlflow/tracking.html#notebook-experiments)|[Azure](https://docs.microsoft.com/azure/databricks/applications/mlflow/tracking#notebook-experiments)). This page allows you to compare runs and view details for specific runs.
# MAGIC
# MAGIC <img width="800" src="https://docs.databricks.com/_static/images/mlflow/quickstart/compare-runs.png"/>

# COMMAND ----------

#Search Function
df=mlflow.search_runs( order_by=["metrics.test_auc DESC"],max_results=1)
print(df["run_id"][0])
run_id=df["run_id"][0]

# COMMAND ----------

#Register the best Model
mlflow.register_model("runs:/{run_id}/model".format(run_id=run_id), "demo_youssef.youssef.Wine_Quality_AB")


# COMMAND ----------

#Transition an mlflow model's stage
from mlflow import MlflowClient
client = MlflowClient()

# create "Champion" alias for version 1 of model "prod.ml_team.iris_model"
client.set_registered_model_alias("demo_youssef.youssef.Wine_Quality_AB", "Champion", 2)


# COMMAND ----------

import mlflow.pyfunc
model_version_uri = "demo_youssef.youssef.Wine_Quality_AB@Champion"
champion_version = mlflow.pyfunc.load_model(model_version_uri)
champion_version.predict(test_x)

# COMMAND ----------

loaded_model = mlflow.pyfunc.spark_udf(spark, model_uri='runs:/c97eb85b6e38491c880d0849f132be54/model',result_type='string')
df=spark.createDataFrame(data_df)
# Predict on a Spark DataFrame.
columns = list(data_df.columns)
df.withColumn('predictions', loaded_model(*columns))
display(df.withColumn('predictions', loaded_model(*columns)))

# COMMAND ----------

import mlflow.pyfunc
# Load version 1 of the model "prod.ml_team.iris_model"
model_version_uri = "models:/demo_youssef.youssef.Wine_Quality_AB/1"
first_version = mlflow.pyfunc.load_model(model_version_uri)
first_version.predict(test_x)