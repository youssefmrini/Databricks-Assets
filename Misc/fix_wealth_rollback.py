# Databricks notebook source
# Rollback: dates were shifted +2 years (need -1 year), financials were scaled x100 (need /10)

# COMMAND ----------
# Roll back dates by 1 year (they were shifted twice: +24mo total, want +12mo from original)
spark.sql("UPDATE demo_youssefmrini.groupe_crystal.transactions SET transaction_date = add_months(transaction_date, -12)")
print("transactions date rollback done")

# COMMAND ----------
spark.sql("UPDATE demo_youssefmrini.groupe_crystal.contract_valuation SET valuation_date = add_months(valuation_date, -12)")
print("contract_valuation date rollback done")

# COMMAND ----------
spark.sql("UPDATE demo_youssefmrini.groupe_crystal.portfolios SET creation_date = add_months(creation_date, -12)")
print("portfolios date rollback done")

# COMMAND ----------
spark.sql("UPDATE demo_youssefmrini.groupe_crystal.envelopes SET opening_date = add_months(opening_date, -12)")
print("envelopes date rollback done")

# COMMAND ----------
# Financials were scaled x100 (10x twice). Divide by 10 to land at x10.
spark.sql("UPDATE demo_youssefmrini.groupe_crystal.portfolios SET total_value = ROUND(total_value / 10, 2)")
print("portfolios financial rollback done")

# COMMAND ----------
spark.sql("UPDATE demo_youssefmrini.groupe_crystal.envelopes SET current_value = ROUND(current_value / 10, 2)")
print("envelopes financial rollback done")

# COMMAND ----------
spark.sql("""
UPDATE demo_youssefmrini.groupe_crystal.transactions
SET amount = ROUND(CASE
    WHEN transaction_type IN ('Achat', 'Vente') THEN amount / 10
    WHEN transaction_type = 'Dividende' THEN amount / 3
    ELSE amount / 8
END, 2)
""")
print("transactions financial rollback done")

# COMMAND ----------
spark.sql("""
UPDATE demo_youssefmrini.groupe_crystal.contract_valuation
SET
  nav            = ROUND(nav / 10, 4),
  total_value    = ROUND(total_value / 10, 2),
  unrealized_pnl = ROUND(unrealized_pnl / 10, 2)
""")
print("contract_valuation financial rollback done")

# COMMAND ----------
# Verify final state
aum = spark.sql("SELECT ROUND(SUM(total_value)/1e9, 3) AS aum_B FROM demo_youssefmrini.groupe_crystal.portfolios").collect()
print("AUM (B EUR):", aum[0][0])

dates = spark.sql("SELECT MIN(transaction_date) AS min_d, MAX(transaction_date) AS max_d FROM demo_youssefmrini.groupe_crystal.transactions").collect()
print("Transaction date range:", dates[0][0], "->", dates[0][1])

by_type = spark.sql("""
SELECT transaction_type, COUNT(*) n, ROUND(AVG(amount)) avg_eur, ROUND(SUM(amount)/1e6,1) total_M
FROM demo_youssefmrini.groupe_crystal.transactions
GROUP BY transaction_type
""").collect()
for r in by_type:
    print("  ", r)

last_q = spark.sql("""
SELECT ROUND(SUM(amount)/1e6,2) AS volume_M, COUNT(*) AS n_txn
FROM demo_youssefmrini.groupe_crystal.transactions
WHERE transaction_date >= add_months(current_date(), -3)
""").collect()
print("Last 3 months volume (M EUR):", last_q[0][0], "n=", last_q[0][1])
