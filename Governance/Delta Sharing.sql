-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Delta Sharing Setup

-- COMMAND ----------

drop catalog sales_youssef cascade;
create catalog sales_youssef;
use catalog sales_youssef;
create schema tpch;
use tpch;
CREATE TABLE IF NOT EXISTS sales_youssef.tpch.orders_bronze;

COPY INTO orders_bronze FROM 
      (SELECT 
                    _c0 O_ORDERKEY,
				_c1 O_CUSTKEY, 
				_c2 O_ORDERSTATUS, 
				_c3 O_TOTALPRICE, 
				_c4 O_ORDER_DATE, 
				_c5 O_ORDERPRIORITY,
				_c6 O_CLERK, 
				_c7 O_SHIPPRIORITY, 
				_c8 O_COMMENT  
FROM 'dbfs:/databricks-datasets/tpch/data-001/orders/')
FILEFORMAT = CSV 
FORMAT_OPTIONS('header' = 'false',
			'inferSchema' = 'true',
			'delimiter' = '|')
COPY_OPTIONS ('mergeSchema' = 'true');

CREATE TABLE IF NOT EXISTS sales_youssef.tpch.lineitem_bronze;

COPY INTO  sales_youssef.tpch.lineitem_bronze FROM 
(SELECT 
                      _c0 L_ORDERKEY,
				_c1 L_PARTKEY, 
				_c2 L_SUPPKEY, 
				_c3 L_LINENUMBER, 
				_c4 L_QUANTITY, 
				_c5 L_EXTENDEDPRICE,
				_c6 L_DISCOUNT, 
				_c7 L_TAX, 
				_c8 L_RETURNFLAG,
				_c9 L_LINESTATUS,
				_c10 L_SHIPDATE, 
				_c11 L_COMMITDATE,
				_c12 L_SHIPINSTRUCT,
				_c13 L_SHIPMODE,
				_c14 L_COMMENT
FROM 'dbfs:/databricks-datasets/tpch/data-001/lineitem/')
FILEFORMAT = CSV 
FORMAT_OPTIONS('header' = 'false',
		    'inferSchema' = 'true',
		    'delimiter' = '|')
COPY_OPTIONS ('mergeSchema' = 'true');


CREATE TABLE IF NOT EXISTS sales_youssef.tpch.customer_bronze;

COPY INTO sales_youssef.tpch.customer_bronze FROM 
(SELECT 
        _c0 C_CUSTKEY,
				_c1 C_NAME, 
				_c2 C_ADDRESS, 
				_c3 C_NATIONKEY, 
				_c4 C_PHONE, 
				_c5 C_ACCTBAL,
				_c6 C_MKTSEGMENT, 
				_c7 C_COMMENT
FROM 'dbfs:/databricks-datasets/tpch/data-001/customer/')
FILEFORMAT = CSV 
FORMAT_OPTIONS('header' = 'false',
		    'inferSchema' = 'true',
		    'delimiter' = '|')
COPY_OPTIONS ('mergeSchema' = 'true');

-- COMMAND ----------

CREATE OR REPLACE TABLE customer_silver
AS SELECT C_CUSTKEY, C_NAME, C_NATIONKEY, C_ACCTBAL
FROM sales_youssef.tpch.customer_bronze
WHERE C_CUSTKEY IS NOT NULL
AND C_NATIONKEY != 21;

SELECT COUNT(DISTINCT C_CUSTKEY) FROM customer_silver;

CREATE OR REPLACE TABLE lineitem_silver AS
SELECT 
   L_ORDERKEY, 
   L_LINENUMBER, 
   L_Quantity, 
   L_EXTENDEDPRICE, 
   L_DISCOUNT, 
   L_TAX, 
   L_RETURNFLAG, 
   L_LINESTATUS
FROM sales_youssef.tpch.lineitem_bronze
WHERE L_ORDERKEY IS NOT NULL
AND L_SHIPDATE >= date '1990-01-01';


CREATE OR REPLACE TABLE discounts AS
SELECT o.O_ORDERKEY,
			 o.O_TOTALPRICE, 
			 l.L_ORDERKEY, 
			 l.L_DISCOUNT, 
			 CASE WHEN (l.L_DISCOUNT >= 0.06) then 'discounted' ELSE 'not_discounted' END AS Discount
FROM sales_youssef.tpch.orders_silver AS o
LEFT JOIN sales_youssef.tpch.lineitem_silver as l
ON o.O_ORDERKEY = l.L_ORDERKEY;




-- COMMAND ----------

use catalog sales_youssef ;
use tpch;
create or refresh streaming table orders_st as select * from stream read_files("/Volumes/global_sales/europe/test/");

create materialized view sales_youssef.tpch.daily_sales as SELECT  DATE_FORMAT(TO_DATE(transaction_date, 'MM-dd-yyyy HH:mm:ss'), 'MM-dd-yyyy') as transaction_date, COUNT(*) as transaction_count, SUM(amount) as total_amount
FROM global_sales.europe.orders_st
GROUP BY transaction_date;



-- COMMAND ----------

create share youssefmrini_mv;

-- COMMAND ----------

alter share youssefmrini_mv add MATERIALIZED VIEW sales_youssef.tpch.daily_sales