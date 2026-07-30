# Databricks notebook source
# MAGIC %md
# MAGIC # DAIS Demo

# COMMAND ----------

# MAGIC %pip install dbdemos

# COMMAND ----------

import dbdemos
dbdemos.install('aibi-sales-pipeline-review', catalog='main', schema='dbdemos_aibi_sales_pipeline_review')

# COMMAND ----------

# MAGIC %sql
# MAGIC --create catalog dais;
# MAGIC use catalog dais;
# MAGIC --create schema demo;
# MAGIC use demo;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE
# MAGIC OR REPLACE FUNCTION dais.demo.open_opps_in_region (
# MAGIC   regions ARRAY < STRING >
# MAGIC   COMMENT 'List of regions.  Example: ["APAC", "EMEA"]' DEFAULT NULL
# MAGIC ) RETURNS TABLE
# MAGIC COMMENT 'Addresses questions about the pipeline in the specified regions by returning
# MAGIC a list of all the open opportunities. If no region is specified, returns all open opportunities.
# MAGIC Example questions: "What is the pipeline for APAC and EMEA?", "Open opportunities in APAC"'
# MAGIC RETURN
# MAGIC   SELECT
# MAGIC   o.opportunityid AS `OppId`,
# MAGIC   a.region__c AS `Region`,
# MAGIC   o.name AS `Opportunity Name`,
# MAGIC   o.forecastcategory AS `Forecast Category`,
# MAGIC   o.stagename,
# MAGIC   o.closedate AS `Close Date`,
# MAGIC   o.amount AS `Opp Amount`
# MAGIC   FROM
# MAGIC   dais.demo.opportunity o
# MAGIC   JOIN dais.demo.accounts a ON o.accountid = a.id
# MAGIC   WHERE
# MAGIC   o.forecastcategory = 'Pipeline'
# MAGIC   AND o.stagename NOT LIKE '%closed%'
# MAGIC   AND (
# MAGIC     isnull(open_opps_in_region.regions)
# MAGIC     OR array_contains(open_opps_in_region.regions, region__c)
# MAGIC   );

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   o.opportunityid AS `OppId`,
# MAGIC   a.region__c AS `Region`,
# MAGIC   o.name AS `Opportunity Name`,
# MAGIC   o.forecastcategory AS `Forecast Category`,
# MAGIC   o.stagename,
# MAGIC   o.closedate AS `Close Date`,
# MAGIC   o.amount AS `Opp Amount`
# MAGIC   FROM
# MAGIC   dais.demo.opportunity o
# MAGIC   JOIN dais.demo.accounts a ON o.accountid = a.id
# MAGIC   WHERE
# MAGIC   o.forecastcategory = 'Pipeline'
# MAGIC   AND o.stagename NOT LIKE '%closed%'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dais.demo.opportunity

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC
# MAGIC INSERT INTO dais.demo.accounts
# MAGIC SELECT * FROM main.dbdemos_aibi_sales_pipeline_review.accounts;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE or replace TABLE dais.demo.accounts(
# MAGIC   id STRING PRIMARY KEY COMMENT "Unique identifier for each account",
# MAGIC   name STRING COMMENT "Name of the account, providing a human-readable label for identification",
# MAGIC   industry STRING COMMENT "Represents the industry the account belongs to, allowing categorization and filtering" ,
# MAGIC   type string COMMENT "Represents the type of account, providing additional context for the account",
# MAGIC   region_hq__c STRING COMMENT "Represents the headquarters region of the account, allowing filtering and categorization based on geographical location",
# MAGIC   region__c STRING COMMENT "Represents the region the account is located in, allowing filtering and categorization based on geographical location",
# MAGIC   company_size_segment__c STRING COMMENT "
# MAGIC Represents the company size segment the account belongs to, allowing categorization and filtering based on company size",
# MAGIC   annualrevenue DOUBLE COMMENT "Represents the annual revenue of the account, providing a quantitative measure of the account's importance or value"
# MAGIC   -- Add all other columns from the existing accounts table
# MAGIC );
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE dais.demo.user (
# MAGIC   id STRING PRIMARY KEY COMMENT "Unique identifier for each user, allowing easy reference and tracking",
# MAGIC   name STRING COMMENT "Human-readable name for the user, providing a personalized experience",
# MAGIC   email STRING COMMENT "Contact email address for the user, facilitating communication and collaboration" ,
# MAGIC   managerid STRING COMMENT "Identifier for the user's manager, allowing tracking of hierarchical relationships",
# MAGIC   role__c STRING COMMENT "Represents the user's role within the organization, providing context for their responsibilities and tasks",
# MAGIC   segment__c STRING COMMENT "Identifies the user's assigned segment, allowing tracking of specific groups and their associated tasks",
# MAGIC   region__c STRING COMMENT "Represents the user's assigned region, allowing tracking of specific geographical areas and their associated tasks."
# MAGIC );
# MAGIC
# MAGIC -- Insert data from the old table into the new table
# MAGIC INSERT INTO dais.demo.user
# MAGIC SELECT  * FROM main.dbdemos_aibi_sales_pipeline_review.user;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE or replace TABLE dais.demo.opportunity (
# MAGIC   opportunityid STRING COMMENT "Unique identifier for the opportunity, allowing easy reference and tracking.",
# MAGIC   type STRING COMMENT "Represents the type of opportunity, such as new or existing.",
# MAGIC   name STRING COMMENT "The name of the opportunity, providing a human-readable label for identification.",
# MAGIC   description STRING COMMENT "A detailed description of the opportunity, offering context and additional information.",
# MAGIC   accountid STRING COMMENT "Identifies the account associated with the opportunity, allowing tracking of opportunities by account.",
# MAGIC   days_to_close BIGINT COMMENT "Represents the number of days remaining until the opportunity is closed.",
# MAGIC   amount DOUBLE COMMENT "The monetary value of the opportunity, providing a measure of its potential impact.",
# MAGIC   opportunity_amount DOUBLE COMMENT "",
# MAGIC   closedate DATE COMMENT "The date when the opportunity is closed, offering a timestamp for tracking and reporting.",
# MAGIC   createddate DATE COMMENT "The date when the opportunity was created, allowing tracking of its lifecycle.",
# MAGIC   leadsource STRING COMMENT "Source of the lead",
# MAGIC   ownerid STRING COMMENT "Identifies the user who owns the opportunity, allowing tracking of ownership and accountability.",
# MAGIC   probability DOUBLE COMMENT "Represents the probability of winning the opportunity, offering a measure of its potential success.",
# MAGIC   probability_per DOUBLE COMMENT "Represents the probability of winning the opportunity per unit of time, allowing tracking of opportunity progress.",
# MAGIC   stagename STRING COMMENT "Represents the stage of the sales process, providing insights into the opportunity's progress.",
# MAGIC   forecastcategory STRING COMMENT "Represents the forecast category, offering insights into the opportunity's potential success.",
# MAGIC   New_Expansion_Booking_Annual__c DOUBLE COMMENT "Represents the annual expansion booking for the opportunity, allowing tracking of revenue potential.",
# MAGIC   New_Expansion_Booking_Annual_Wtd__c DOUBLE COMMENT "Represents the annual expansion booking for the opportunity, weighted by the forecast category, allowing more accurate revenue projections.",
# MAGIC   New_Recurring_Bookings_Manual__c DOUBLE COMMENT "Represents the number of recurring bookings for the opportunity, allowing tracking of potential revenue streams.",
# MAGIC   New_Recurring_Bookings__c DOUBLE COMMENT "Represents the number of recurring bookings for the opportunity, allowing tracking of potential revenue streams.",
# MAGIC   business_type__c STRING COMMENT "Represents the type of business, allowing categorization and filtering of opportunities based on business type.",
# MAGIC   industry STRING COMMENT "Represents the industry the business is in, providing insights into the specific sector.",
# MAGIC   account_name STRING COMMENT "Unique identifier for the account, allowing easy reference and tracking of opportunities.",
# MAGIC   region_hq__c STRING COMMENT "Represents the headquarters region of the business, providing insights into the geographical location of the opportunity.",
# MAGIC   region STRING COMMENT "Represents the user's assigned region, allowing tracking of specific geographical areas and their associated tasks." ,
# MAGIC   company_size_segment__c STRING COMMENT "Represents the user's assigned region, allowing tracking of specific geographical areas and their associated tasks.",
# MAGIC   region_size STRING COMMENT "Represents the size of the region, allowing further categorization and filtering.",
# MAGIC   annualrevenue DOUBLE COMMENT "Represents the annual revenue of the business, providing insights into the financial potential of the opportunity.",
# MAGIC   PRIMARY KEY (accountid,ownerid),
# MAGIC   FOREIGN KEY (accountid) REFERENCES dais.demo.accounts(id),
# MAGIC   FOREIGN KEY (ownerid) REFERENCES dais.demo.user(id)
# MAGIC );
# MAGIC
# MAGIC -- Insert data from the old table into the new table
# MAGIC INSERT INTO dais.demo.opportunity
# MAGIC SELECT * FROM main.dbdemos_aibi_sales_pipeline_review.opportunity;
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Demo restaurant

# COMMAND ----------

