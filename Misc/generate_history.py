# Databricks notebook source
# MAGIC %md
# MAGIC # Generate Historical Data: Feb 2023 - Jan 2025
# MAGIC Adds 2 years of historical data to transactions and contract_valuation tables.
# MAGIC Existing data (Feb 2025 - Feb 2026) is NOT touched.
# MAGIC Schema: transaction_id=bigint, envelope_id=bigint, transaction_date=timestamp

# COMMAND ----------

import builtins
_round = builtins.round
_min = builtins.min
_max = builtins.max
_int = builtins.int

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, LongType, TimestampType, StringType, DoubleType
)
from datetime import date, timedelta, datetime
import random
import math

spark = SparkSession.builder.getOrCreate()

CATALOG = "demo_youssefmrini"
SCHEMA   = "groupe_crystal"

# COMMAND ----------
# Fetch existing envelope IDs (bigint)

envelope_ids_df = spark.sql(f"SELECT envelope_id FROM {CATALOG}.{SCHEMA}.envelopes ORDER BY envelope_id")
envelope_ids = [row["envelope_id"] for row in envelope_ids_df.collect()]  # These will be ints (bigint)
print(f"Found {len(envelope_ids)} envelopes. Types: {type(envelope_ids[0])}, examples: {envelope_ids[:5]}")

# COMMAND ----------
# Fetch max existing IDs

max_txn_row = spark.sql(f"SELECT MAX(transaction_id) AS max_id FROM {CATALOG}.{SCHEMA}.transactions").collect()
max_txn_id = _int(max_txn_row[0]["max_id"] or 0)
print(f"Max existing transaction_id: {max_txn_id}")

max_val_row = spark.sql(f"SELECT MAX(valuation_id) AS max_id FROM {CATALOG}.{SCHEMA}.contract_valuation").collect()
max_val_id = _int(max_val_row[0]["max_id"] or 0)
print(f"Max existing valuation_id: {max_val_id}")

# COMMAND ----------
# Generate historical transactions (Feb 2023 - Jan 2025)

random.seed(42)

ASSET_CLASSES = {
    "Actions": [
        "LVMH", "Airbus", "TotalEnergies", "Sanofi", "BNP Paribas", "Kering",
        "L'Oreal", "Hermes", "Safran", "Danone", "Schneider Electric", "AXA",
        "Credit Agricole", "Engie", "Renault", "Carrefour", "Scor"
    ],
    "Obligations": [
        "Amundi Euro Gov Bond", "Carmignac Patrimoine", "OAT France 2030",
        "BNP Paribas Bond 2028", "Societe Generale Bond"
    ],
    "Immobilier": [
        "Primonial REIM", "SCPI Pierre", "Corum XL", "Sofidy Immo"
    ],
    "Monetaire": [
        "Amundi Monetaire", "BNP Paribas Tresorerie", "HSBC Money Market"
    ],
}

ASSET_CLASS_WEIGHTS = [0.60, 0.20, 0.12, 0.08]
ASSET_CLASS_NAMES   = list(ASSET_CLASSES.keys())
TRANSACTION_TYPES   = ["Achat", "Vente", "Dividende"]
TXN_WEIGHTS         = [0.50, 0.30, 0.20]

start_date = date(2023, 2, 1)
end_date   = date(2025, 1, 31)

# Build list of months
months = []
d = start_date
while d <= end_date:
    months.append(date(d.year, d.month, 1))
    if d.month == 12:
        d = date(d.year + 1, 1, 1)
    else:
        d = date(d.year, d.month + 1, 1)

print(f"Months to fill: {len(months)} ({months[0]} to {months[-1]})")

transactions = []
txn_id = max_txn_id + 1

for month_start in months:
    if month_start.month == 12:
        month_end = date(month_start.year + 1, 1, 1) - timedelta(days=1)
    else:
        month_end = date(month_start.year, month_start.month + 1, 1) - timedelta(days=1)

    n_txns = random.randint(90, 110)

    for _ in range(n_txns):
        delta    = (month_end - month_start).days
        rand_day = month_start
        for attempt in range(20):
            candidate = month_start + timedelta(days=random.randint(0, delta))
            if candidate.weekday() < 5:
                rand_day = candidate
                break
        # Convert to timestamp (midnight) as required by the table schema
        txn_ts = datetime(rand_day.year, rand_day.month, rand_day.day, 0, 0, 0)

        txn_type    = random.choices(TRANSACTION_TYPES, weights=TXN_WEIGHTS)[0]
        asset_class = random.choices(ASSET_CLASS_NAMES, weights=ASSET_CLASS_WEIGHTS)[0]
        asset_name  = random.choice(ASSET_CLASSES[asset_class])

        if txn_type == "Dividende":
            amount = _round(random.uniform(20000, 120000), 2)
        else:
            amount = _round(random.uniform(150000, 1500000), 2)

        envelope_id = random.choice(envelope_ids)  # already bigint from Spark

        transactions.append((
            txn_id,            # bigint
            envelope_id,       # bigint
            txn_ts,            # timestamp
            txn_type,          # string
            amount,            # double
            asset_name,        # string
            asset_class,       # string
        ))
        txn_id += 1

print(f"Generated {len(transactions)} transactions")
dates_only = [t[2].date() for t in transactions]
print(f"Date range: {_min(dates_only)} to {_max(dates_only)}")

# COMMAND ----------
# Write transactions to Delta table

txn_schema = StructType([
    StructField("transaction_id",   LongType(),      False),
    StructField("envelope_id",      LongType(),      False),
    StructField("transaction_date", TimestampType(), False),
    StructField("transaction_type", StringType(),    False),
    StructField("amount",           DoubleType(),    False),
    StructField("asset_name",       StringType(),    False),
    StructField("asset_class",      StringType(),    False),
])

txn_df = spark.createDataFrame(transactions, schema=txn_schema)
print(f"Transaction DataFrame: {txn_df.count()} rows")
txn_df.show(5)

txn_df.write.format("delta").mode("append").saveAsTable(f"{CATALOG}.{SCHEMA}.transactions")
print("Transactions appended successfully.")

# COMMAND ----------
# Verify transaction counts

count_row = spark.sql(f"""
    SELECT COUNT(*) as cnt,
           MIN(transaction_date) as min_d,
           MAX(transaction_date) as max_d
    FROM {CATALOG}.{SCHEMA}.transactions
""").collect()[0]
print(f"Total: {count_row['cnt']} | {count_row['min_d']} - {count_row['max_d']}")

# COMMAND ----------
# Generate historical contract_valuation (Feb 2023 - Jan 2025)
# Schema: valuation_id=bigint, envelope_id=bigint, valuation_date=timestamp, nav=double, total_value=double, unrealized_pnl=double

earliest_vals_df = spark.sql(f"""
    SELECT envelope_id, nav, total_value, unrealized_pnl
    FROM {CATALOG}.{SCHEMA}.contract_valuation
    WHERE valuation_date = (
        SELECT MIN(valuation_date) FROM {CATALOG}.{SCHEMA}.contract_valuation
    )
""")
earliest_vals = {
    row["envelope_id"]: {
        "nav":            float(row["nav"]),
        "total_value":    float(row["total_value"]),
        "unrealized_pnl": float(row["unrealized_pnl"]),
    }
    for row in earliest_vals_df.collect()
}
print(f"Got anchor values for {len(earliest_vals)} envelopes")

# COMMAND ----------

val_start   = date(2023, 2, 1)
val_end     = date(2025, 1, 31)
anchor_date = date(2025, 2, 25)
days_back   = (anchor_date - val_start).days

all_dates = []
d = val_start
while d <= val_end:
    all_dates.append(d)
    d += timedelta(days=1)

print(f"Historical dates: {len(all_dates)} days from {val_start} to {val_end}")

val_schema = StructType([
    StructField("valuation_id",   LongType(),      False),
    StructField("envelope_id",    LongType(),      False),
    StructField("valuation_date", TimestampType(), False),
    StructField("nav",            DoubleType(),    False),
    StructField("total_value",    DoubleType(),    False),
    StructField("unrealized_pnl", DoubleType(),    False),
])

BATCH_ENVELOPES = 20
random.seed(123)

total_written = 0
# Compute global offset for IDs
# Each envelope has len(all_dates) rows, stagger by envelope index
global_val_id = max_val_id + 1

for batch_start in range(0, len(envelope_ids), BATCH_ENVELOPES):
    batch_env_ids = envelope_ids[batch_start:batch_start + BATCH_ENVELOPES]
    batch_rows = []

    for env_offset, env_id in enumerate(batch_env_ids):
        anchor = earliest_vals.get(env_id) or {
            "nav": 500.0, "total_value": 1000000.0, "unrealized_pnl": 5000.0
        }

        annual_growth = random.uniform(0.06, 0.14)
        daily_drift   = (1 + annual_growth) ** (1.0 / 365) - 1
        daily_vol     = random.uniform(0.008, 0.018) / math.sqrt(252)

        start_nav   = anchor["nav"]         / ((1 + annual_growth) ** (days_back / 365.0))
        start_total = anchor["total_value"] / ((1 + annual_growth) ** (days_back / 365.0))
        start_pnl   = anchor["unrealized_pnl"] * 0.2

        current_nav   = start_nav
        current_total = start_total
        current_pnl   = start_pnl

        local_id = global_val_id + (batch_start + env_offset) * len(all_dates)

        for d in all_dates:
            daily_return  = daily_drift + random.gauss(0, daily_vol)
            current_nav   = _max(current_nav   * (1 + daily_return), 10.0)
            current_total = _max(current_total * (1 + daily_return), 10000.0)
            pnl_noise     = random.gauss(0, abs(current_total) * 0.002)
            current_pnl   = current_pnl * 0.98 + pnl_noise + current_total * 0.0005

            val_ts = datetime(d.year, d.month, d.day, 0, 0, 0)

            batch_rows.append((
                local_id,                       # bigint
                env_id,                         # bigint
                val_ts,                         # timestamp
                _round(current_nav, 4),         # double
                _round(current_total, 2),       # double
                _round(current_pnl, 2),         # double
            ))
            local_id += 1

    batch_df = spark.createDataFrame(batch_rows, schema=val_schema)
    batch_df.write.format("delta").mode("append").saveAsTable(f"{CATALOG}.{SCHEMA}.contract_valuation")
    total_written += len(batch_rows)
    print(f"Written {total_written:,} rows (batch envelopes {batch_start+1}-{batch_start+len(batch_env_ids)} / {len(envelope_ids)})...")

print(f"Done! Total valuation rows written: {total_written:,}")

# COMMAND ----------
# Final verification

print("=== FINAL DATA VERIFICATION ===")

txn_stats = spark.sql(f"""
    SELECT COUNT(*) as total,
           MIN(transaction_date) as min_date,
           MAX(transaction_date) as max_date,
           COUNT(DISTINCT DATE_TRUNC('month', transaction_date)) as months
    FROM {CATALOG}.{SCHEMA}.transactions
""").collect()[0]
print(f"Transactions: {txn_stats['total']:,} rows | {txn_stats['min_date']} to {txn_stats['max_date']} | {txn_stats['months']} months")

cv_stats = spark.sql(f"""
    SELECT COUNT(*) as total,
           MIN(valuation_date) as min_date,
           MAX(valuation_date) as max_date
    FROM {CATALOG}.{SCHEMA}.contract_valuation
""").collect()[0]
print(f"Contract_valuation: {cv_stats['total']:,} rows | {cv_stats['min_date']} to {cv_stats['max_date']}")

spark.sql(f"""
    SELECT DATE_TRUNC('month', transaction_date) as month,
           transaction_type,
           COUNT(*) as cnt
    FROM {CATALOG}.{SCHEMA}.transactions
    WHERE transaction_date < '2023-08-01'
    GROUP BY 1, 2
    ORDER BY 1, 2
""").show(30)

print("Historical data generation COMPLETE!")
