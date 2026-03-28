# Databricks notebook source

# COMMAND ----------
# Step 1: Shift all dates +1 year
spark.sql("UPDATE demo_youssefmrini.groupe_crystal.transactions SET transaction_date = add_months(transaction_date, 12)")
print("transactions dates shifted")

# COMMAND ----------
spark.sql("UPDATE demo_youssefmrini.groupe_crystal.contract_valuation SET valuation_date = add_months(valuation_date, 12)")
print("contract_valuation dates shifted")

# COMMAND ----------
spark.sql("UPDATE demo_youssefmrini.groupe_crystal.portfolios SET creation_date = add_months(creation_date, 12)")
print("portfolios dates shifted")

# COMMAND ----------
spark.sql("UPDATE demo_youssefmrini.groupe_crystal.envelopes SET opening_date = add_months(opening_date, 12)")
print("envelopes dates shifted")

# COMMAND ----------
# Step 2: Scale financial values x10 for realism (target: ~1B EUR AUM)
spark.sql("UPDATE demo_youssefmrini.groupe_crystal.portfolios SET total_value = ROUND(total_value * 10, 2)")
print("portfolios scaled x10")

# COMMAND ----------
spark.sql("UPDATE demo_youssefmrini.groupe_crystal.envelopes SET current_value = ROUND(current_value * 10, 2)")
print("envelopes scaled x10")

# COMMAND ----------
# Buy/Sell x10, Dividends x3 (dividends scale less to keep yield realistic)
spark.sql("""
UPDATE demo_youssefmrini.groupe_crystal.transactions
SET amount = ROUND(
  CASE
    WHEN transaction_type IN ('Achat', 'Vente') THEN amount * 10
    WHEN transaction_type = 'Dividende' THEN amount * 3
    ELSE amount * 8
  END, 2)
""")
print("transactions scaled")

# COMMAND ----------
spark.sql("""
UPDATE demo_youssefmrini.groupe_crystal.contract_valuation
SET
  nav          = ROUND(nav * 10, 4),
  total_value  = ROUND(total_value * 10, 2),
  unrealized_pnl = ROUND(unrealized_pnl * 10, 2)
""")
print("contract_valuation scaled x10")

# COMMAND ----------
# Verify
aum = spark.sql("SELECT ROUND(SUM(total_value)/1e9, 3) AS aum_B FROM demo_youssefmrini.groupe_crystal.portfolios").collect()
print("FINAL AUM (B EUR):", aum[0][0])

dates = spark.sql("SELECT MIN(transaction_date), MAX(transaction_date) FROM demo_youssefmrini.groupe_crystal.transactions").collect()
print("Txn date range:", dates[0][0], "->", dates[0][1])

txn_stats = spark.sql("""
  SELECT transaction_type,
         COUNT(*) n,
         ROUND(AVG(amount)) avg_eur,
         ROUND(MAX(amount)) max_eur
  FROM demo_youssefmrini.groupe_crystal.transactions
  GROUP BY transaction_type
""").collect()
for r in txn_stats:
    print(r)

portfolio_stats = spark.sql("""
  SELECT COUNT(*) n, ROUND(MIN(total_value)) min_eur, ROUND(MAX(total_value)) max_eur, ROUND(AVG(total_value)) avg_eur
  FROM demo_youssefmrini.groupe_crystal.portfolios
""").collect()
print("Portfolio range:", portfolio_stats[0])
