# Databricks notebook source
# MAGIC %md
# MAGIC # Sales Accounts Exploration

# COMMAND ----------

df=spark.table("youssef_sarl.sales.accounts")

# COMMAND ----------

df2=df.select("industry","id","name","TYPE","region_hq__c","region__c","company_size_segment__c","annualrevenue")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from youssef_sarl.sales.accounts

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT industry, 
# MAGIC        any_value(id) as id, 
# MAGIC        any_value(name) as name, 
# MAGIC        any_value(TYPE) as TYPE, 
# MAGIC        any_value(region_hq__c) as region_hq__c, 
# MAGIC        any_value(region__c) as region__c, 
# MAGIC        any_value(company_size_segment__c) as company_size_segment__c, 
# MAGIC        any_value(annualrevenue) as annualrevenue
# MAGIC FROM youssef_sarl.sales.accounts
# MAGIC GROUP BY industry
