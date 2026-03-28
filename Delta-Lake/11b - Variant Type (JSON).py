# Databricks notebook source
# MAGIC %md
# MAGIC # Variant Type - JSON Example

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from json.`/Volumes/youssef_mrini/demo/storage/example.json`

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog youssef_mrini;
# MAGIC use schema demo;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE or replace TABLE  json_table AS
# MAGIC SELECT parse_json(
# MAGIC   '{
# MAGIC     "store":{
# MAGIC         "fruit": [
# MAGIC           {"weight":8,"type":"apple"},
# MAGIC           {"weight":9,"type":"pear"}
# MAGIC         ],
# MAGIC         "basket":[
# MAGIC           [1,2,{"b":"y","a":"x"}],
# MAGIC           [3,4],
# MAGIC           [5,6]
# MAGIC         ],
# MAGIC         "book":[
# MAGIC           {
# MAGIC             "author":"Nigel Rees",
# MAGIC             "title":"Sayings of the Century",
# MAGIC             "category":"reference",
# MAGIC             "price":8.95
# MAGIC           },
# MAGIC           {
# MAGIC             "author":"Herman Melville",
# MAGIC             "title":"Moby Dick",
# MAGIC             "category":"fiction",
# MAGIC             "price":8.99,
# MAGIC             "isbn":"0-553-21311-3"
# MAGIC           },
# MAGIC           {
# MAGIC             "author":"J. R. R. Tolkien",
# MAGIC             "title":"The Lord of the Rings",
# MAGIC             "category":"fiction",
# MAGIC             "reader":[
# MAGIC               {"age":25,"name":"bob"},
# MAGIC               {"age":26,"name":"jack"}
# MAGIC             ],
# MAGIC             "price":22.99,
# MAGIC             "isbn":"0-395-19395-8"
# MAGIC           }
# MAGIC         ],
# MAGIC         "bicycle":{
# MAGIC           "price":19.95,
# MAGIC           "color":"red"
# MAGIC         }
# MAGIC       },
# MAGIC       "owner":"amy",
# MAGIC       "zip code":"94025",
# MAGIC       "fb:testid":"1234"
# MAGIC   }'
# MAGIC ) as raw

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Use dot notation
# MAGIC SELECT raw:store.bicycle FROM json_table
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * 