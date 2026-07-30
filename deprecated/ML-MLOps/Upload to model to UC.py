# Databricks notebook source
# MAGIC %md
# MAGIC # Upload Model to Unity Catalog

# COMMAND ----------

# MAGIC %pip install --upgrade "mlflow-skinny[databricks]"
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import mlflow
mlflow.set_registry_uri("databricks-uc")

# COMMAND ----------

from sklearn import datasets
from sklearn.ensemble import RandomForestClassifier

with mlflow.start_run():
    # Train a sklearn model on the iris dataset
    X, y = datasets.load_iris(return_X_y=True, as_frame=True)
    clf = RandomForestClassifier(max_depth=7)
    clf.fit(X, y)
    # Take the first row of the training dataset as the model input example.
    input_example = X.iloc[[0]]
    # Log the model and register it as a new version in UC.
    mlflow.sklearn.log_model(
        sk_model=clf,
        artifact_path="model",
        # The signature is automatically inferred from the input example and its predicted output.
        input_example=input_example,
        registered_model_name="alex_farin_catalog.uniform.iris_model",
    )

# COMMAND ----------

data = spark.sql(""" select * from kiliba_db.bounceai_dataset_train """) # Définir les colonnes de caractéristiques à utiliser 
feature_columns = ['latest_date_order_days', 'domain_bounce_rate', 'domain_opening_rate', 'email_length', 'num_digits', 'num_special_chars', 'contains_keyword', 'has_alias', 'domain_length', 'email_entropy'] # Assembler les colonnes de caractéristiques en une colonne `features` 
assembler = VectorAssembler(inputCols=feature_columns, outputCol="features") 
data_with_features = assembler.transform(data) # Charger le modèle depuis MLflow 
logged_model = 'runs:/4e9a55ccc60c4c47a407e2c5a05aa0ed/rf_model' 
loaded_model = mlflow.spark.load_model(logged_model) # Effectuer l’inférence via model.transform() 
predictions = loaded_model.transform(data_with_features) predictions.select("email", "prediction", "probability").write.mode("overwrite").option("mergeSchema", "true").saveAsTable("kiliba_db.bounceai_dataset_predictions")

# COMMAND ----------

