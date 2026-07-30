# Databricks notebook source
# MAGIC %md
# MAGIC # Generate Customer Data

# COMMAND ----------

# DBTITLE 1,Create catalog, schema and customer table with RLS
# Create the catalog and schema
spark.sql("CREATE CATALOG IF NOT EXISTS AIBI_Bsport")
spark.sql("CREATE SCHEMA IF NOT EXISTS AIBI_Bsport.demo")

# Create sample customer data with 15 customers
from pyspark.sql import Row
from datetime import datetime

customers_data = [
    # Customers for youssef.mrini@databricks.com (IDs 1-5)
    Row(customer_id=1, name="Alice Martin", email="alice.martin@example.com", country="France", city="Paris", purchase_amount=1250.50, membership_tier="Gold", assigned_to="youssef.mrini@databricks.com"),
    Row(customer_id=2, name="Bob Dubois", email="bob.dubois@example.com", country="France", city="Lyon", purchase_amount=890.75, membership_tier="Silver", assigned_to="youssef.mrini@databricks.com"),
    Row(customer_id=3, name="Claire Petit", email="claire.petit@example.com", country="France", city="Marseille", purchase_amount=2100.00, membership_tier="Platinum", assigned_to="youssef.mrini@databricks.com"),
    Row(customer_id=4, name="David Moreau", email="david.moreau@example.com", country="France", city="Toulouse", purchase_amount=650.25, membership_tier="Bronze", assigned_to="youssef.mrini@databricks.com"),
    Row(customer_id=5, name="Emma Leroy", email="emma.leroy@example.com", country="France", city="Nice", purchase_amount=1450.80, membership_tier="Gold", assigned_to="youssef.mrini@databricks.com"),
    
    # Customers for hanane.oudnia@databricks.com (IDs 6-10)
    Row(customer_id=6, name="François Bernard", email="francois.bernard@example.com", country="Belgium", city="Brussels", purchase_amount=980.30, membership_tier="Silver", assigned_to="hanane.oudnia@databricks.com"),
    Row(customer_id=7, name="Gabrielle Roux", email="gabrielle.roux@example.com", country="Belgium", city="Antwerp", purchase_amount=1750.60, membership_tier="Gold", assigned_to="hanane.oudnia@databricks.com"),
    Row(customer_id=8, name="Henri Fournier", email="henri.fournier@example.com", country="Belgium", city="Ghent", purchase_amount=520.40, membership_tier="Bronze", assigned_to="hanane.oudnia@databricks.com"),
    Row(customer_id=9, name="Isabelle Girard", email="isabelle.girard@example.com", country="Belgium", city="Bruges", purchase_amount=2300.90, membership_tier="Platinum", assigned_to="hanane.oudnia@databricks.com"),
    Row(customer_id=10, name="Jacques Bonnet", email="jacques.bonnet@example.com", country="Belgium", city="Liège", purchase_amount=1100.50, membership_tier="Silver", assigned_to="hanane.oudnia@databricks.com"),
    
    # Customers for axel.richier@databricks.com (IDs 11-15)
    Row(customer_id=11, name="Karine Lambert", email="karine.lambert@example.com", country="Switzerland", city="Geneva", purchase_amount=3200.75, membership_tier="Platinum", assigned_to="axel.richier@databricks.com"),
    Row(customer_id=12, name="Louis Fontaine", email="louis.fontaine@example.com", country="Switzerland", city="Zurich", purchase_amount=1580.20, membership_tier="Gold", assigned_to="axel.richier@databricks.com"),
    Row(customer_id=13, name="Marie Chevalier", email="marie.chevalier@example.com", country="Switzerland", city="Lausanne", purchase_amount=890.60, membership_tier="Silver", assigned_to="axel.richier@databricks.com"),
    Row(customer_id=14, name="Nicolas Gauthier", email="nicolas.gauthier@example.com", country="Switzerland", city="Bern", purchase_amount=2750.40, membership_tier="Platinum", assigned_to="axel.richier@databricks.com"),
    Row(customer_id=15, name="Olivia Mercier", email="olivia.mercier@example.com", country="Switzerland", city="Basel", purchase_amount=1320.85, membership_tier="Gold", assigned_to="axel.richier@databricks.com")
]

# Create DataFrame
customers_df = spark.createDataFrame(customers_data)

# Write to Delta table
customers_df.write.format("delta").mode("overwrite").saveAsTable("AIBI_Bsport.demo.Customer")

print("✅ Catalog, schema, and Customer table created successfully!")
print(f"Total customers: {customers_df.count()}")

# COMMAND ----------

# DBTITLE 1,Apply Row-Level Security filter
# MAGIC %sql
# MAGIC -- Create a row filter function for RLS
# MAGIC CREATE OR REPLACE FUNCTION AIBI_Bsport.demo.customer_filter(assigned_to STRING)
# MAGIC RETURN assigned_to = current_user();

# COMMAND ----------

# DBTITLE 1,Enable RLS on Customer table
# MAGIC %sql
# MAGIC -- Apply the row filter to the Customer table
# MAGIC ALTER TABLE AIBI_Bsport.demo.Customer 
# MAGIC SET ROW FILTER AIBI_Bsport.demo.customer_filter ON (assigned_to);

# COMMAND ----------

# DBTITLE 1,Test RLS - Query customers
# MAGIC %sql
# MAGIC -- Query the Customer table - you will only see customers assigned to you
# MAGIC SELECT 
# MAGIC   customer_id,
# MAGIC   name,
# MAGIC   email,
# MAGIC   country,
# MAGIC   city,
# MAGIC   purchase_amount,
# MAGIC   membership_tier,
# MAGIC   assigned_to
# MAGIC FROM AIBI_Bsport.demo.Customer
# MAGIC ORDER BY customer_id;