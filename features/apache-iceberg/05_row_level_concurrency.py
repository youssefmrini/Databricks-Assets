# Databricks notebook source
# MAGIC %md
# MAGIC # Row-Level Concurrency on Iceberg V3
# MAGIC
# MAGIC Iceberg V3's deletion vectors and row lineage enable **automatic conflict resolution** for concurrent operations that touch different rows in the same data file.
# MAGIC
# MAGIC **Key behaviors:**
# MAGIC - Concurrent MERGE/UPDATE/DELETE on different rows succeed without conflicts
# MAGIC - OPTIMIZE and REORG never conflict with other writes
# MAGIC - Row lineage (`_row_id`, `_last_updated_sequence_number`) tracks per-row state
# MAGIC
# MAGIC **Runtime:** DBR 18.0+

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS features_demo;
# MAGIC USE CATALOG features_demo;
# MAGIC CREATE SCHEMA IF NOT EXISTS iceberg_concurrency;
# MAGIC USE SCHEMA iceberg_concurrency;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Create V3 Table with Deletion Vectors

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS account_balances;
# MAGIC CREATE TABLE account_balances (
# MAGIC   account_id BIGINT,
# MAGIC   holder_name STRING,
# MAGIC   balance DECIMAL(12,2),
# MAGIC   last_txn TIMESTAMP
# MAGIC ) USING iceberg
# MAGIC TBLPROPERTIES ('format-version' = 3);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Seed data
# MAGIC INSERT INTO account_balances VALUES
# MAGIC   (1001, 'Alice', 5000.00, current_timestamp()),
# MAGIC   (1002, 'Bob', 12000.00, current_timestamp()),
# MAGIC   (1003, 'Charlie', 8500.00, current_timestamp()),
# MAGIC   (1004, 'Diana', 3200.00, current_timestamp()),
# MAGIC   (1005, 'Eve', 15000.00, current_timestamp());

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Demonstrate Non-Conflicting Updates

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Update Alice's balance
# MAGIC UPDATE account_balances SET balance = balance + 500, last_txn = current_timestamp()
# MAGIC WHERE account_id = 1001;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Update Bob's balance (different row — no conflict with Alice's update)
# MAGIC UPDATE account_balances SET balance = balance - 200, last_txn = current_timestamp()
# MAGIC WHERE account_id = 1002;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Both updates succeeded without conflict
# MAGIC SELECT * FROM account_balances ORDER BY account_id;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. MERGE with Row-Level Concurrency

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Batch of incoming transactions
# MAGIC -- Create a temp view for the merge source
# MAGIC CREATE OR REPLACE TEMP VIEW merge_source AS
# MAGIC SELECT CAST(1003 AS BIGINT) AS account_id, 'Charlie' AS holder_name, CAST(1000.00 AS DECIMAL(12,2)) AS amount, current_timestamp() AS txn_time
# MAGIC UNION ALL
# MAGIC SELECT CAST(1006 AS BIGINT), 'Frank', CAST(7500.00 AS DECIMAL(12,2)), current_timestamp();
# MAGIC
# MAGIC MERGE INTO account_balances AS target
# MAGIC USING merge_source AS source
# MAGIC ON target.account_id = source.account_id
# MAGIC WHEN MATCHED THEN UPDATE SET
# MAGIC   balance = target.balance + source.amount,
# MAGIC   last_txn = source.txn_time
# MAGIC WHEN NOT MATCHED THEN INSERT (account_id, holder_name, balance, last_txn)
# MAGIC VALUES (source.account_id, source.holder_name, source.amount, source.txn_time);

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM account_balances ORDER BY account_id;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. DELETE with Deletion Vectors

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Delete uses deletion vectors — no file rewrite needed
# MAGIC DELETE FROM account_balances WHERE account_id = 1004;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify deletion
# MAGIC SELECT * FROM account_balances ORDER BY account_id;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. OPTIMIZE Runs Without Conflicting

# COMMAND ----------

# MAGIC %sql
# MAGIC -- OPTIMIZE compacts files and applies deletion vectors — never conflicts with other writes
# MAGIC OPTIMIZE account_balances;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY account_balances;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Simulated Concurrent Workload

# COMMAND ----------

from concurrent.futures import ThreadPoolExecutor
import time

def update_account(account_id, delta):
    spark.sql(f"""
        UPDATE features_demo.iceberg_concurrency.account_balances
        SET balance = balance + {delta}, last_txn = current_timestamp()
        WHERE account_id = {account_id}
    """)
    return f"Updated account {account_id} by {delta}"

# Run concurrent updates on different rows
with ThreadPoolExecutor(max_workers=3) as executor:
    futures = [
        executor.submit(update_account, 1001, 100),
        executor.submit(update_account, 1002, -50),
        executor.submit(update_account, 1003, 200),
    ]
    for f in futures:
        print(f.result())

# COMMAND ----------

# MAGIC %sql
# MAGIC -- All concurrent updates succeeded
# MAGIC SELECT * FROM account_balances ORDER BY account_id;

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **Summary:** Row-level concurrency on Iceberg V3 eliminates write conflicts for operations that touch different rows. This enables high-throughput concurrent pipelines without lock contention — a significant advantage for real-time workloads.
