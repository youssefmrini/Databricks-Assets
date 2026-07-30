-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Genie Room Setup

-- COMMAND ----------

use catalog youssef_mrini_sales;
create schema filtered_view;
use filtered_view;

-- COMMAND ----------

create view accounts as
(
  select 
    case 
        when industry is null then array('Major Banks', 'Major Pharmaceuticals', 'Real Estate Investment Trusts', 'Business Services', 'Industrial Machinery/Components', 'Telecommunications Equipment', 'Oil & Gas Production')[floor(rand()*7)]
        else industry 
    end as industry,
    id,
    name,
    TYPE,
    region_hq__c,
    region__c,
    company_size_segment__c,
    annualrevenue
  from
    youssef_mrini_sales.sales.accounts
  )

-- COMMAND ----------

create view opportunity as
(
  select
    id,
    ownerid,
    stagename,
    accountid,
    type,
    name,
    forecastcategory,
    description,
    leadsource,
    date_add(closedate, 180) as closedate,
    date_add(createddate, 180) as createddate,
    probability,
    amount,
    New_Expansion_Booking_Annual__c,
    New_Expansion_Booking_Annual_Wtd__c,
    New_Recurring_Bookings_Manual__c,
    New_Recurring_Bookings__c,
    business_type__c
  from
    youssef_mrini_sales.sales.opportunity
)

-- COMMAND ----------

create view opportunityhistory as

(
  select
    id,
    opportunityid,
    probability,
    amount,
    createdbyid,
    stagename,
    forecastcategory,
    date_add(closedate, 180) as closedate,
    date_add(createddate, 180) as createddate
  from
    youssef_mrini_sales.sales.opportunityhist
)

-- COMMAND ----------

create view period as
(
  select
    FISCALYEARSETTINGSID,
    ISFORECASTPERIOD,
    PERIODLABEL,
    ENDDATE,
    QUARTERLABEL,
    STARTDATE,
    FULLYQUALIFIEDLABEL,
    NUMBER,
    ID,
    TYPE
  from
    youssef_mrini_sales.sales.period
)

-- COMMAND ----------

create view teams as
(
  select
    USER,
    REGION,
    SEGMENT,
    MANAGER,
    TEAM
  from
    youssef_mrini_sales.sales.teams
)

-- COMMAND ----------

create view user as
(
  select
    id,
    name,
    title,
    managerid,
    role__c,
    segment__c,
    region__c
  from
    youssef_mrini_sales.sales.user
)