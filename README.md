# Databricks-Assets

Databricks notebooks, SQL scripts, dashboards, and utilities for analytics, Delta Lake, AI functions, and system tables.

---

## AI Functions.sql

**Type:** SQL notebook  
**Link:** [AI Functions.sql](https://github.com/youssefmrini/Databricks-Assets/blob/main/AI%20Functions.sql)

SQL examples using Databricks AI/BI AI functions for text analysis directly in SQL. Includes:

| Function | Description |
|----------|-------------|
| `ai_analyze_sentiment()` | Sentiment analysis on input text |
| `ai_extract()` | Extract entities (e.g. person, nationality, company) from text |
| `ai_fix_grammar()` | Correct grammatical errors in text |
| `ai_classify()` | Classify text into given labels |
| `ai_gen()` | Generate text from a prompt |
| `ai_mask()` | Mask specified entities (e.g. city, salary) in text |
| `ai_summarize()` | Summarize long text (with optional max words) |
| `ai_translate()` | Translate text to a target language (e.g. `'es'` for Spanish) |

**Use case:** Quick AI-powered analytics in SQL for sentiment, NER, classification, summarization, and translation.

---

## Account Usage Dashboard v2.lvdash.json

**Type:** Lakeview dashboard (JSON)  
**Link:** [Account Usage Dashboard v2.lvdash.json](https://github.com/youssefmrini/Databricks-Assets/blob/main/Account%20Usage%20Dashboard%20v2.lvdash.json)

Pre-built Lakeview dashboard for **account usage** (warehouses, jobs, storage, etc.). Import into Databricks to visualize usage and costs.

**Use case:** Monitor account-level usage and costs without building queries from scratch.

---

## Delta Lake Column Mapping.ipynb

**Type:** Jupyter notebook  
**Link:** [Delta Lake Column Mapping.ipynb](https://github.com/youssefmrini/Databricks-Assets/blob/main/Delta%20Lake%20Column%20Mapping.ipynb)

Covers **Delta Lake column mapping**: renaming, dropping, or changing column types without rewriting data. Uses `delta.columnMapping.mode` and related table properties.

**Use case:** Schema evolution and safe column renames/drops on existing Delta tables.

---

## Delta Lake Deletion Vectors.py

**Type:** Python notebook  
**Link:** [Delta Lake Deletion Vectors.py](https://github.com/youssefmrini/Databricks-Assets/blob/main/Delta%20Lake%20Deletion%20Vectors.py)

Demo of **Delta Lake deletion vectors**: compares MERGE performance with and without deletion vectors. Creates sample order data (20M rows), then runs MERGE (updates + inserts) on tables with `delta.enableDeletionVectors` true vs false.

**Highlights:**
- Deletion vectors track deletes without rewriting files; good for MERGE/UPDATE/DELETE and CDC.
- Tables created under `youssef_demo.test` (configurable catalog/schema).
- Prints timing and best-practice notes.

**Use case:** Understand when to enable deletion vectors for faster MERGE/update-heavy workloads.

---

## Delta Lake Identity column.ipynb

**Type:** Jupyter notebook  
**Link:** [Delta Lake Identity column.ipynb](https://github.com/youssefmrini/Databricks-Assets/blob/main/Delta%20Lake%20Identity%20column.ipynb)

Explains **Delta Lake identity columns**: auto-generated, unique row IDs (e.g. `GENERATED ALWAYS AS IDENTITY`). Covers creation, behavior, and use cases.

**Use case:** Surrogate keys and stable row identifiers in Delta tables.

---

## Delta Lake Variant.ipynb

**Type:** Jupyter notebook  
**Link:** [Delta Lake Variant.ipynb](https://github.com/youssefmrini/Databricks-Assets/blob/main/Delta%20Lake%20Variant.ipynb)

Introduces the **Variant** type in Delta/Unity Catalog: store and query semi-structured data (JSON-like) with type preservation and efficient access.

**Use case:** Semi-structured data (logs, API payloads) without flattening into many columns.

---

## GOVERNANCE HUB SYSTEM DASHBOARD.lvdash.json

**Type:** Lakeview dashboard (JSON)  
**Link:** [GOVERNANCE HUB SYSTEM DASHBOARD.lvdash.json](https://github.com/youssefmrini/Databricks-Assets/blob/main/GOVERNANCE%20HUB%20SYSTEM%20DASHBOARD.lvdash.json)

Pre-built **Governance Hub** dashboard using **system tables** (Unity Catalog). Surfaces lineage, access, compliance, and governance metrics.

**Use case:** Central view of governance and lineage for admins and auditors.

---

## Jobs System Tables Dashboard.lvdash.json

**Type:** Lakeview dashboard (JSON)  
**Link:** [Jobs System Tables Dashboard.lvdash.json](https://github.com/youssefmrini/Databricks-Assets/blob/main/Jobs%20System%20Tables%20Dashboard.lvdash.json)

Dashboard built on **jobs-related system tables**: job runs, duration, success/failure, and resource usage.

**Use case:** Monitor job health and performance without writing custom system-table queries.

---

## Lab_SQL_Scripting.sql

**Type:** SQL notebook  
**Link:** [Lab_SQL_Scripting.sql](https://github.com/youssefmrini/Databricks-Assets/blob/main/Lab_SQL_Scripting.sql)

**SQL Scripting** lab: variables, compound statements (`BEGIN`/`END`), control flow, loops, and exception handling. Intended as a foundation for **stored procedures** and for migrating legacy SQL warehouses to the lakehouse.

**Topics include:**
- Compound statements and block labels
- Declaring and using variables (`DECLARE`, `SET`)
- Conditionals (`IF`/`ELSE`)
- Loops (`WHILE`, `FOR`)
- Cursors and result sets
- Exception handling
- Dynamic SQL

**Use case:** Learn or teach SQL scripting on Databricks and prepare for stored procedures.

**Setup:** Uses `catalog` and `schema` parameters (e.g. `:catalog`, `:schema`); run the cleanup section if you need to reset variables.

---

## enable_system_tables.py

**Type:** Python notebook  
**Link:** [enable_system_tables.py](https://github.com/youssefmrini/Databricks-Assets/blob/main/enable_system_tables.py)

**Programmatic enablement of Unity Catalog system tables** from a Databricks notebook. Uses the Unity Catalog API to list and enable system schemas for the current metastore. Requires **account admin** and a Unity Catalog–governed workspace.

**Steps:**
1. Reads current metastore ID (or use the widget).
2. Calls `GET /api/2.0/unity-catalog/metastores/{id}/systemschemas` to list schemas.
3. Enables each schema in state `available` via `PUT .../systemschemas/{schema}`.

**Use case:** Turn on system tables (for dashboards like Governance Hub and Jobs) without using the account UI. See [AWS](https://docs.databricks.com/administration-guide/system-tables/index.html) \| [Azure](https://learn.microsoft.com/en-us/azure/databricks/administration-guide/system-tables/) docs.

---

## Summary

| File | Type | Purpose |
|------|------|---------|
| AI Functions.sql | SQL | AI functions in SQL (sentiment, extract, classify, translate, etc.) |
| Account Usage Dashboard v2.lvdash.json | Dashboard | Account usage and cost monitoring |
| Delta Lake Column Mapping.ipynb | Notebook | Delta column mapping (schema evolution) |
| Delta Lake Deletion Vectors.py | Notebook | Deletion vectors and MERGE performance |
| Delta Lake Identity column.ipynb | Notebook | Identity columns in Delta |
| Delta Lake Variant.ipynb | Notebook | Variant type for semi-structured data |
| GOVERNANCE HUB SYSTEM DASHBOARD.lvdash.json | Dashboard | Governance and lineage (system tables) |
| Jobs System Tables Dashboard.lvdash.json | Dashboard | Job runs and performance (system tables) |
| Lab_SQL_Scripting.sql | SQL | SQL scripting lab (variables, loops, procedures) |
| enable_system_tables.py | Notebook | Enable system tables via API (account admin) |

---

*Repository: [youssefmrini/Databricks-Assets](https://github.com/youssefmrini/Databricks-Assets)*
