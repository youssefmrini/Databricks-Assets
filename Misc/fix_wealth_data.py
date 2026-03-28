# Databricks notebook source
spark.sql('UPDATE demo_youssefmrini.groupe_crystal.transactions SET transaction_date = add_months(transaction_date, 12)')


# COMMAND ----------

spark.sql('UPDATE demo_youssefmrini.groupe_crystal.contract_valuation SET valuation_date = add_months(valuation_date, 12)')


# COMMAND ----------

spark.sql('UPDATE demo_youssefmrini.groupe_crystal.portfolios SET creation_date = add_months(creation_date, 12)')


# COMMAND ----------

spark.sql('UPDATE demo_youssefmrini.groupe_crystal.envelopes SET opening_date = add_months(opening_date, 12)')


# COMMAND ----------

spark.sql('UPDATE demo_youssefmrini.groupe_crystal.portfolios SET total_value = ROUND(total_value * 10, 2)')


# COMMAND ----------

spark.sql('UPDATE demo_youssefmrini.groupe_crystal.envelopes SET current_value = ROUND(current_value * 10, 2)')


# COMMAND ----------

spark.sql(\"UPDATE demo_youssefmrini.groupe_crystal.transactions SET amount = ROUND(CASE WHEN transaction_type IN ('Achat', 'Vente') THEN amount * 10 WHEN transaction_type = 'Dividende' THEN amount * 3 ELSE amount * 8 END, 2)\")


# COMMAND ----------

spark.sql('UPDATE demo_youssefmrini.groupe_crystal.contract_valuation SET nav = ROUND(nav * 10, 4), total_value = ROUND(total_value * 10, 2), unrealized_pnl = ROUND(unrealized_pnl * 10, 2)')


# COMMAND ----------

r = spark.sql('SELECT ROUND(SUM(total_value)/1e9,3) AS aum_B FROM demo_youssefmrini.groupe_crystal.portfolios').collect(); print('AUM (B EUR):', r[0][0])


# COMMAND ----------

r = spark.sql('SELECT MIN(transaction_date), MAX(transaction_date) FROM demo_youssefmrini.groupe_crystal.transactions').collect(); print('Txn date range:', r[0][0], '->', r[0][1])


# COMMAND ----------

r = spark.sql("SELECT transaction_type, ROUND(AVG(amount)) avg, ROUND(SUM(amount)/1e6,1) total_M FROM demo_youssefmrini.groupe_crystal.transactions GROUP BY transaction_type").collect(); [print(row) for row in r]
