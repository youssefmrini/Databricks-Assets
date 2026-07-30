# Databricks notebook source
# MAGIC %md
# MAGIC # SQLAlchemy Connector

# COMMAND ----------

# MAGIC %pip install databricks-sql-connector
# MAGIC %pip install sqlalchemy
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

from sqlalchemy.engine import create_engine
import pandas as pd
import numpy as np
#from databricks import sqlalchemy
from databricks import sql


token = "<YOUR_DATABRICKS_TOKEN>"
hostname = "e2-demo-field-eng.cloud.databricks.com"
port = "443"
http_path = "/sql/1.0/warehouses/db02e6988ef9c735"
database = ""

catalog = "sales_youssef"
schema_name = "tpch"
table = "orders_bronze"

new_table = pd.DataFrame(np.random.randint(20,size=(50,6)), columns=['a', 'b', 'c', 'd', 'e', 'f'])
df = pd.DataFrame({
    'id': [1, 2, 3],
    'name': ['Alice', 'Bob', 'Charlie'],
    'age': [25, 30, 35]
})

with sql.connect(
    server_hostname=hostname,
    http_path=http_path,
    access_token=token,
    catalog=catalog,
    schema=schema_name,
) as connection:
  
  with connection.cursor() as cursor:
    cursor.execute(f"CREATE TABLE IF NOT EXISTS  {catalog}.{schema_name}.squares (x int, x_squared int)")

    squares = [(i, i * i) for i in range(100)]
    values = ",".join([f"({x}, {y})" for (x, y) in squares])

    cursor.execute(f"INSERT INTO  {catalog}.{schema_name}.squares VALUES {values}")

    cursor.execute(f"SELECT * FROM   {catalog}.{schema_name}.squares LIMIT 10")

    result = cursor.fetchall()

    for row in result:
      print(row)


# COMMAND ----------

from sqlalchemy.engine import create_engine
import pandas as pd
import numpy as np
#from databricks import sqlalchemy
from databricks import sql


token = "<YOUR_DATABRICKS_TOKEN>"
hostname = "e2-demo-field-eng.cloud.databricks.com"
port = "443"
http_path = "/sql/1.0/warehouses/db02e6988ef9c735"
database = ""

catalog = "sales_youssef"
schema_name = "tpch"
table = "orders_bronze"

new_table = pd.DataFrame(np.random.randint(20,size=(50,6)), columns=['a', 'b', 'c', 'd', 'e', 'f'])



with sql.connect(
    server_hostname=hostname,
    http_path=http_path,
    access_token=token,
    catalog=catalog,
    schema=schema_name,
) as connection:
  
  with connection.cursor() as cursor:
    cursor.execute(f"CREATE TABLE IF NOT EXISTS  {catalog}.{schema_name}.squares (x int, x_squared int)")

    squares = [(i, i * i) for i in range(100)]
    values = ",".join([f"({x}, {y})" for (x, y) in squares])

    cursor.execute(f"INSERT INTO  {catalog}.{schema_name}.squares VALUES {values}")

    cursor.execute(f"SELECT * FROM   {catalog}.{schema_name}.squares LIMIT 10")

    result = cursor.fetchall()

    for row in result:
      print(row)

# COMMAND ----------

import pandas as pd
from databricks import sql

# create a sample DataFrame
df = pd.DataFrame({
    'id': [1, 2, 3],
    'name': ['Alice', 'Bob', 'Charlie'],
    'age': [25, 30, 35]
})
token = "<YOUR_DATABRICKS_TOKEN>"
hostname = "e2-demo-field-eng.cloud.databricks.com"
port = "443"
http_path = "/sql/1.0/warehouses/db02e6988ef9c735"
database = ""

catalog = "sales_youssef"
schema_name = "tpch"
table = "orders_bronze"

# connect to Databricks SQL
with sql.connect(
  server_hostname=hostname,
  http_path=http_path,
  access_token=token,
  catalog=catalog,
  schema=schema_name,

) as con:
    # create a cursor
    cursor = con.cursor()
    
    # create a table
    cursor.execute(f"CREATE TABLE {catalog}.{schema_name}.my_table (id INT, name STRING, age INT)")
    
    # iterate over DataFrame rows and insert them into the table
    for _, row in df.iterrows():
        # insert a row into the table
        cursor.execute(f"INSERT INTO my_table (id, name, age) VALUES ({row['id']}, '{row['name']}', {row['age']})")
   
    # commit the transaction
    con.commit()

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from sales_youssef.tpch.my_table

# COMMAND ----------

from sqlalchemy import *
from sqlalchemy.engine import create_engine
import pandas as pd
import numpy as np
#from databricks import sqlalchemy
from databricks import sql

df = pd.DataFrame({
    'id': [1, 2, 3],
    'name': ['Alice', 'Bob', 'Charlie'],
    'age': [25, 30, 35]
})
new_table = pd.DataFrame(np.random.randint(20,size=(50,6)), columns=['a', 'b', 'c', 'd', 'e', 'f'])
token = "<YOUR_DATABRICKS_TOKEN>"
hostname = "e2-demo-field-eng.cloud.databricks.com"
port = "443"
http_path = "/sql/1.0/warehouses/e4c6a6ade8881b10"
database = ""

catalog = "sales_youssef"
schema = "tpch"
table = "orders_bronze"
engine = create_engine(
    f"databricks://token:{token}@{hostname}?http_path={http_path}&schema={schema}&catalog={catalog}",
)

with engine.connect() as connection:    # %%
    my_df = pd.read_sql(f"SELECT * FROM {catalog}.{schema}.{table} LIMIT 10;", con=connection) 
    #display(my_df)# OK
    new_table.to_sql("testables", con=connection,  if_exists='append', chunksize=25, method='multi') # ne fonctionne pas
   

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from sales_youssef.tpch.testables

# COMMAND ----------

# MAGIC %sql 
# MAGIC optimize sales_youssef.tpch.testables

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from sales_youssef.tpch.testables