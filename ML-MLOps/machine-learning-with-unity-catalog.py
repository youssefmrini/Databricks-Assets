# Databricks notebook source
# MAGIC %md # Databricks ML Tutorial: Model Training
# MAGIC
# MAGIC This notebook provides a quick overview of machine learning model training on Databricks. To train models, you can use libraries like scikit-learn that are preinstalled on the Databricks Runtime for Machine Learning. In addition, you can use MLflow to track the trained models, and Hyperopt with SparkTrials to scale hyperparameter tuning.
# MAGIC
# MAGIC This tutorial covers:
# MAGIC - Part 1: Training a simple classification model with MLflow tracking
# MAGIC - Part 2: Hyperparameter tuning a better performing model with Hyperopt
# MAGIC - Part 3: Save results and models to Unity Catalog
# MAGIC
# MAGIC For more details on productionizing machine learning on Databricks including model lifecycle management and model inference, see the ML End to End Example ([AWS](https://docs.databricks.com/mlflow/end-to-end-example.html) | [Azure](https://learn.microsoft.com/azure/databricks/mlflow/end-to-end-example) | [GCP](https://docs.gcp.databricks.com/mlflow/end-to-end-example.html)).
# MAGIC
# MAGIC The example uses a dataset from the UCI Machine Learning Repository, presented in [*Modeling wine preferences by data mining from physicochemical properties*](https://www.sciencedirect.com/science/article/pii/S0167923609001377?via%3Dihub) [Cortez et al., 2009].
# MAGIC
# MAGIC ### Requirements
# MAGIC - Cluster running Databricks Runtime 12.2 LTS ML or above

# COMMAND ----------

# MAGIC %pip install --upgrade "mlflow-skinny[databricks]"
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md ## Unity Catalog setup
# MAGIC By default, the MLflow Python client creates models in the Databricks workspace model registry. To save models in Unity Catalog, configure the MLflow client as shown in the following cell.

# COMMAND ----------

import mlflow
mlflow.set_registry_uri("databricks-uc")

# COMMAND ----------

# MAGIC %md 
# MAGIC The following cell sets the catalog and schema where the model will be registered. You must have USE CATALOG privilege on the catalog, and USE_SCHEMA, CREATE_TABLE, and CREATE_MODEL privileges on the schema. Change the catalog and schema names in the following cell if necessary.
# MAGIC
# MAGIC For more information about how to use Unity Catalog, see ([AWS](https://docs.databricks.com/data-governance/unity-catalog/index.html) | [Azure](https://learn.microsoft.com/azure/databricks/data-governance/unity-catalog/index) | [GCP](https://docs.gcp.databricks.com/data-governance/unity-catalog/index.html)).

# COMMAND ----------

# Specify the catalog and schema to use. You must have USE_CATALOG privilege on the catalog and USE_SCHEMA, CREATE_TABLE, and CREATE_MODEL privileges on the schema.
# Change the catalog and schema here if necessary.
CATALOG_NAME = "main"
SCHEMA_NAME = "default"

# COMMAND ----------

# MAGIC %md ## Write data to Unity Catalog tables
# MAGIC The dataset is available in `databricks-datasets`. In the following cell, you read the data in from `.csv` files into Spark DataFrames. You then write the DataFrames to tables in Unity Catalog. This both persists the data and lets you control how to share it with others.

# COMMAND ----------

white_wine = spark.read.csv("dbfs:/databricks-datasets/wine-quality/winequality-white.csv", sep=';', header=True)
red_wine = spark.read.csv("dbfs:/databricks-datasets/wine-quality/winequality-red.csv", sep=';', header=True)

# Remove the spaces from the column names
for c in white_wine.columns:
    white_wine = white_wine.withColumnRenamed(c, c.replace(" ", "_"))
for c in red_wine.columns:
    red_wine = red_wine.withColumnRenamed(c, c.replace(" ", "_"))

# Define table names
red_wine_table = f"{CATALOG_NAME}.{SCHEMA_NAME}.red_wine"
white_wine_table = f"{CATALOG_NAME}.{SCHEMA_NAME}.white_wine"

# Write to tables in Unity Catalog
spark.sql(f"DROP TABLE IF EXISTS {red_wine_table}")
spark.sql(f"DROP TABLE IF EXISTS {white_wine_table}")
white_wine.write.saveAsTable(f"{CATALOG_NAME}.{SCHEMA_NAME}.white_wine")
red_wine.write.saveAsTable(f"{CATALOG_NAME}.{SCHEMA_NAME}.red_wine")

# COMMAND ----------

import numpy as np
import pandas as pd
import sklearn.datasets
import sklearn.metrics
import sklearn.model_selection
import sklearn.ensemble

from hyperopt import fmin, tpe, hp, SparkTrials, Trials, STATUS_OK
from hyperopt.pyll import scope

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Load data from Unity Catalog and do preprocessing

# COMMAND ----------

# Load data from Unity Catalog as Pandas dataframes
white_wine = spark.read.table(f"{CATALOG_NAME}.{SCHEMA_NAME}.white_wine").toPandas()
red_wine = spark.read.table(f"{CATALOG_NAME}.{SCHEMA_NAME}.red_wine").toPandas()

# Add Boolean fields for red and white wine
white_wine['is_red'] = 0.0
red_wine['is_red'] = 1.0
data_df = pd.concat([white_wine, red_wine], axis=0)

# Define classification labels based on the wine quality
data_labels = data_df['quality'].astype('int') >= 7
data_df = data_df.drop(['quality'], axis=1)

# Split 80/20 train-test
X_train, X_test, y_train, y_test = sklearn.model_selection.train_test_split(
  data_df,
  data_labels,
  test_size=0.2,
  random_state=1
)

# COMMAND ----------

display(data_df)

# COMMAND ----------

# MAGIC %md ## Part 1. Train a classification model

# COMMAND ----------

# Enable MLflow autologging for this notebook
mlflow.autolog()

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

import mlflow
catalog = "main"
schema = "default"
model_name = "youssef_mrini"
mlflow.set_registry_uri("databricks-uc")
mlflow.register_model("runs:/9cc0c25537a1425ab0649302ec8bcb50/model", f"{catalog}.{schema}.{model_name}")

# COMMAND ----------

import mlflow
catalog = "main"
schema = "default"
model_name = "youssef_mrini"
mlflow.set_registry_uri("databricks-uc")
mlflow.register_model("runs:/94c566c5555d4c04ac259e9866e3ca46/model", f"{catalog}.{schema}.{model_name}")

# COMMAND ----------

import mlflow
catalog = "main"
schema = "default"

model_name = "youssefmirni2"
mlflow.set_registry_uri("databricks-uc")
mlflow.register_model("runs:/e0a15542d457421abdbd0b13f40b8abc/model", f"{catalog}.{schema}.{model_name}")

# COMMAND ----------

# MAGIC %md ### View MLflow runs
# MAGIC To view the logged training run, click the **Experiment** icon <img src="https://docs.databricks.com/_static/images/mlflow/quickstart/experiment-icon.png"/> at the upper right of the notebook to display the experiment sidebar. If necessary, click the refresh icon to fetch and monitor the latest runs. 
# MAGIC
# MAGIC <img src="https://docs.databricks.com/_static/images/mlflow/quickstart/experiment-sidebar-icons.png"/>
# MAGIC
# MAGIC To display the more detailed MLflow experiment page, click the experiment page icon. This page allows you to compare runs and view details for specific runs ([AWS](https://docs.databricks.com/applications/mlflow/tracking.html#notebook-experiments) | [Azure](https://docs.microsoft.com/azure/databricks/applications/mlflow/tracking#notebook-experiments) | [GCP](https://docs.gcp.databricks.com/applications/mlflow/tracking.html#notebook-experiments)).

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load models
# MAGIC You can also access the results for a specific run using the MLflow API. The code in the following cell illustrates how to load the model trained in a given MLflow run and use it to make predictions. You can also find code snippets for loading specific models on the MLflow run page ([AWS](https://docs.databricks.com/mlflow/runs.html#code-snippets-for-prediction) | [Azure](https://docs.microsoft.com/azure/databricks/mlflow/runs#code-snippets-for-prediction) | [GCP](https://docs.gcp.databricks.com/mlflow/runs.html#code-snippets-for-prediction)).

# COMMAND ----------

# After a model has been logged, you can load it in different notebooks or jobs
# mlflow.pyfunc.load_model makes model prediction available under a common API
model_loaded = mlflow.pyfunc.load_model(
  'runs:/{run_id}/model'.format(
    run_id=run.info.run_id
  )
)

predictions_loaded = model_loaded.predict(X_test)
predictions_original = model_2.predict(X_test)

# The loaded model should match the original
assert(np.array_equal(predictions_loaded, predictions_original))

# COMMAND ----------

# MAGIC %md ## Part 2. Hyperparameter Tuning
# MAGIC At this point, you have trained a simple model and used the MLflow tracking service to organize your work. Next, you can perform more sophisticated tuning using Hyperopt.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Parallel training with Hyperopt and SparkTrials
# MAGIC [Hyperopt](http://hyperopt.github.io/hyperopt/) is a Python library for hyperparameter tuning. For more information about using Hyperopt in Databricks, see the documentation ([AWS](https://docs.databricks.com/applications/machine-learning/automl-hyperparam-tuning/index.html#hyperparameter-tuning-with-hyperopt) | [Azure](https://docs.microsoft.com/azure/databricks/applications/machine-learning/automl-hyperparam-tuning/index#hyperparameter-tuning-with-hyperopt) | [GCP](https://docs.gcp.databricks.com/applications/machine-learning/automl-hyperparam-tuning/index.html#hyperparameter-tuning-with-hyperopt)).
# MAGIC
# MAGIC You can use Hyperopt with SparkTrials to run hyperparameter sweeps and train multiple models in parallel. This reduces the time required to optimize model performance. MLflow tracking is integrated with Hyperopt to automatically log models and parameters.

# COMMAND ----------

# Define the search space to explore
search_space = {
  'n_estimators': scope.int(hp.quniform('n_estimators', 20, 1000, 1)),
  'learning_rate': hp.loguniform('learning_rate', -3, 0),
  'max_depth': scope.int(hp.quniform('max_depth', 2, 5, 1)),
}

def train_model(params):
  # Enable autologging on each worker
  mlflow.autolog()
  with mlflow.start_run(nested=True):
    model_hp = sklearn.ensemble.GradientBoostingClassifier(
      random_state=0,
      **params
    )
    model_hp.fit(X_train, y_train)
    predicted_probs = model_hp.predict_proba(X_test)
    # Tune based on the test AUC
    # In production, you could use a separate validation set instead
    roc_auc = sklearn.metrics.roc_auc_score(y_test, predicted_probs[:,1])
    mlflow.log_metric('test_auc', roc_auc)
    
    # Set the loss to -1*auc_score so fmin maximizes the auc_score
    return {'status': STATUS_OK, 'loss': -1*roc_auc}

# SparkTrials distributes the tuning using Spark workers
# Greater parallelism speeds processing, but each hyperparameter trial has less information from other trials
# On smaller clusters try setting parallelism=2
spark_trials = SparkTrials(
  parallelism=1
)

with mlflow.start_run(run_name='gb_hyperopt') as run:
  # Use hyperopt to find the parameters yielding the highest AUC
  best_params = fmin(
    fn=train_model, 
    space=search_space, 
    algo=tpe.suggest, 
    max_evals=32,
    trials=spark_trials)

# COMMAND ----------

# MAGIC %md ### Search runs to retrieve the best model
# MAGIC Because all of the runs are tracked by MLflow, you can retrieve the metrics and parameters for the best run using the MLflow search runs API to find the tuning run with the highest test auc.
# MAGIC
# MAGIC This tuned model should perform better than the simpler models trained in Part 1. 

# COMMAND ----------

# Sort runs by their test auc. In case of ties, use the most recent run.
best_run = mlflow.search_runs(
  order_by=['metrics.test_auc DESC', 'start_time DESC'],
  max_results=10,
).iloc[0]
print('Best Run')
print('AUC: {}'.format(best_run["metrics.test_auc"]))
print('Num Estimators: {}'.format(best_run["params.n_estimators"]))
print('Max Depth: {}'.format(best_run["params.max_depth"]))
print('Learning Rate: {}'.format(best_run["params.learning_rate"]))

best_model_pyfunc = mlflow.pyfunc.load_model(
  'runs:/{run_id}/model'.format(
    run_id=best_run.run_id
  )
)

# Make a dataset with all predictions
best_model_predictions = X_test
best_model_predictions["prediction"] = best_model_pyfunc.predict(X_test)

# COMMAND ----------

# MAGIC %md ## Part 3. Save results and models to Unity Catalog

# COMMAND ----------

# DBTITLE 1,Write results back to Unity Catalog
predictions_table = f"{CATALOG_NAME}.{SCHEMA_NAME}.predictions"
spark.sql(f"DROP TABLE IF EXISTS {predictions_table}")

results = spark.createDataFrame(best_model_predictions)

#Write results back to Unity Catalog from Python
results.write.saveAsTable(f"{CATALOG_NAME}.{SCHEMA_NAME}.predictions")

# COMMAND ----------

# DBTITLE 1,Save model to Unity Catalog
model_uri = 'runs:/{run_id}/model'.format(
    run_id=best_run.run_id
  )

mlflow.register_model(model_uri, f"{CATALOG_NAME}.{SCHEMA_NAME}.wine_model")