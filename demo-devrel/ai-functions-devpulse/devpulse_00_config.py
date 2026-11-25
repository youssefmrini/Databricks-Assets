# Databricks notebook source
# DBTITLE 1,Shared configuration and warehouse helper
# =============================================================================
# DevPulse shared configuration.
# Loaded with `%run ./devpulse_00_config` by:
#   * devpulse_01_data_generator                       (creates the fake data)
#   * DevPulse - Databricks AI Functions End to End    (the AI Functions pipeline)
# This is the ONLY place to adapt to your workspace: edit the widget defaults
# below or change the values in the widget bar of the calling notebook.
# =============================================================================
import os
import re
import time

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState


# ---- 1. Widgets -------------------------------------------------------------
dbutils.widgets.text("catalog",      "youssef_demo",                        "1. Catalog (must exist)")
dbutils.widgets.text("schema",       "devpulse_ai_functions",       "2. Schema (created if missing)")
dbutils.widgets.text("warehouse_id", "",                            "3. SQL warehouse ID (empty = auto)")

CATALOG      = dbutils.widgets.get("catalog").strip()
SCHEMA       = dbutils.widgets.get("schema").strip()
WAREHOUSE_ID = dbutils.widgets.get("warehouse_id").strip()

# Catalog and schema are interpolated into SQL: only allow safe identifiers.
for name, value in [("catalog", CATALOG), ("schema", SCHEMA)]:
    if not re.fullmatch(r"[A-Za-z0-9_]+", value):
        raise ValueError(
            f"Widget '{name}' must contain only letters, digits and underscores, got: {value!r}"
        )


# ---- 2. Unity Catalog objects -----------------------------------------------
# A dedicated schema keeps the demo isolated and easy to drop afterwards.
spark.sql(
    f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA} "
    "COMMENT 'DevPulse: Databricks AI Functions end-to-end showcase'"
)
# A volume stores the unstructured inputs (PDFs and MP3s).
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.raw_files")

# Every following SQL / Python cell can use unqualified table names.
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

VOL = f"/Volumes/{CATALOG}/{SCHEMA}/raw_files"
for folder in ["docs", "calls"]:
    os.makedirs(f"{VOL}/{folder}", exist_ok=True)


# ---- 3. SQL session variables -----------------------------------------------
# %sql cells cannot read Python variables, so the few values they need are
# exposed as SQL session variables (they live for the notebook session).
spark.sql(f"DECLARE OR REPLACE VARIABLE devpulse_volume STRING DEFAULT '{VOL}'")
# Minimum ai_decide confidence to link a message to a known bug (1.7).
# Shared by the weekly brief (4.3) and the Gold board, so both count the same reports.
spark.sql("DECLARE OR REPLACE VARIABLE devpulse_issue_min_conf DOUBLE DEFAULT 0.7")
# Minimum ai_decide urgency score (0-3, from 1.7) for a message to count as urgent.
# Shared by ai_enrich (3.1) and the weekly brief (4.3).
spark.sql("DECLARE OR REPLACE VARIABLE devpulse_urgent_min DOUBLE DEFAULT 2.5")


# ---- 4. SQL warehouse for the predictive functions --------------------------
# ai_forecast, ai_detect_anomalies, ai_top_drivers and ai_predict_* run on a
# Pro or Serverless SQL warehouse. If no ID is given, pick the first usable one:
# a PRO warehouse (serverless first, running first). REALTIME warehouses are
# skipped: they do not support CREATE OR REPLACE TABLE.
w = WorkspaceClient()   # authenticates as the current notebook user

if not WAREHOUSE_ID:
    candidates = sorted(
        (wh for wh in w.warehouses.list() if str(wh.warehouse_type).endswith("PRO")),
        key=lambda wh: (not wh.enable_serverless_compute, not str(wh.state).endswith("RUNNING")),
    )
    if not candidates:
        raise RuntimeError(
            "No Pro or Serverless SQL warehouse found. "
            "Create one and set the 'warehouse_id' widget."
        )
    WAREHOUSE_ID = candidates[0].id


def run_on_warehouse(sql: str, timeout_s: int = 600):
    """Run one SQL statement on the SQL warehouse and wait for it to finish.

    Used for the predictive AI Functions, which are not available on notebook
    serverless compute. The statement runs in the same catalog and schema, so
    the result tables can be read back with spark.table / spark.sql.

    Raises RuntimeError with the warehouse error message if the statement fails,
    and cancels it if it runs longer than timeout_s seconds (default 10 minutes).
    """
    r = w.statement_execution.execute_statement(
        warehouse_id=WAREHOUSE_ID,
        statement=sql,
        catalog=CATALOG,
        schema=SCHEMA,
        wait_timeout="50s",
    )
    deadline = time.time() + timeout_s

    # Long statements return while still running: poll until they finish.
    while r.status.state in (StatementState.PENDING, StatementState.RUNNING):
        if time.time() > deadline:
            w.statement_execution.cancel_execution(r.statement_id)
            raise TimeoutError(f"Statement cancelled after {timeout_s}s: {sql[:80]}...")
        time.sleep(3)
        r = w.statement_execution.get_statement(r.statement_id)

    if r.status.state != StatementState.SUCCEEDED:
        raise RuntimeError(r.status.error.message if r.status.error else str(r.status.state))
    return r


print(
    f"Catalog.schema : {CATALOG}.{SCHEMA}\n"
    f"Volume         : {VOL}\n"
    f"Warehouse      : {WAREHOUSE_ID}"
)