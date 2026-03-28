# Databricks notebook source
# MAGIC %md
# MAGIC # Grant Catalog Permissions

# COMMAND ----------

catalogs = spark.sql("SHOW CATALOGS").collect()
principal = "youssef.mrini@databricks.com"
catalog_list = [row['catalog'] for row in catalogs]
for catalog_name in catalog_list:
    for privilege in ["USE CATALOG", "CREATE SCHEMA"]:
        grant_query = (
            f"GRANT `{privilege}` ON CATALOG `{catalog_name}` TO `{principal}`"
        )
        try:
            spark.sql(grant_query)
            print(f"Granted `{privilege}` on `{catalog_name}` to `{principal}`")
        except Exception as e:
            print(f"Failed to grant `{privilege}` on `{catalog_name}`: {e}")