# Databricks notebook source
# MAGIC %md
# MAGIC # ML Training

# COMMAND ----------

from pyspark.sql import SparkSession 
from pyspark.sql.types import IntegerType,StringType 
from pyspark.sql.functions import udf 
  
  
cms = ["Name","RawScore"] 
data =  [("Jack", "79"), 
        ("Mira", "80"), 
        ("Carter", "90")] 
                     
df = spark.createDataFrame(data=data,schema=cms) 
  
df.show()

# COMMAND ----------

def Converter(str): 
	result = "" 
	a = str.split(" ") 
	
	for q in a: 
		if q == 'J' or 'C' or 'M': 
			result += q[1:2].upper() 
			
	return result 

NumberUDF = udf(lambda m: Converter(m))

df.withColumn("Special Names", NumberUDF("Name")).show() 



# COMMAND ----------

@udf(returnType=StringType())
def Converter(str): 
	result = "" 
	a = str.split(" ") 
	
	for q in a: 
		if q == 'J' or 'C' or 'M': 
			result += q[1:2].upper() 
			
	return result 
df.withColumn("Special Names", Converter("Name")).show()

# COMMAND ----------

import math
from pyspark.sql.functions import udf
from pyspark.sql.types import FloatType

def SQRT(x):
    return float(math.sqrt(x) + 3)

udf_mark = udf(lambda m: SQRT(x), FloatType())