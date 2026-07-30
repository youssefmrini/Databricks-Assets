# Databricks-Assets

A collection of Databricks notebooks, SQL scripts, AI/BI dashboards, and utilities
covering Delta Lake, data engineering, ML/MLOps, governance, and SQL. Assets are
organized into topical folders — each folder has its own README describing every file.

## Structure

| Folder | What's inside |
|--------|---------------|
| [`Delta-Lake/`](Delta-Lake/) | Delta Lake storage features — liquid clustering, deletion vectors, column mapping, identity columns, variant type, type widening, cloning, UniForm, and Iceberg comparisons. Includes [`features/`](Delta-Lake/features/) deep-dives. |
| [`Apache-Iceberg/`](Apache-Iceberg/) | Apache Iceberg V3 demos — lifecycle, Variant on Iceberg, liquid clustering, UniForm V3, row-level concurrency, managed Iceberg tables. |
| [`Data-Engineering/`](Data-Engineering/) | Ingestion, Auto Loader, web scraping, the Databricks SDK, unit testing, and data-generation utilities. |
| [`DLT-Streaming/`](DLT-Streaming/) | Delta Live Tables / Lakeflow declarative pipelines in Python and SQL, plus event-log inspection. |
| [`ML-MLOps/`](ML-MLOps/) | MLflow, Unity Catalog model registry, feature engineering, AutoML, and end-to-end ML examples. |
| [`Governance/`](Governance/) | Unity Catalog governance — system tables, lineage, permissions, Delta Sharing, predictive optimization, online tables. |
| [`SQL/`](SQL/) | Databricks SQL — SQL scripting, warehouse optimization, AI functions, system-table cost/audit analysis, connectors, Genie setup. |
| [`Dashboards/`](Dashboards/) | Importable AI/BI (Lakeview) dashboards for account usage, governance, Genie cost, and jobs. |
| [`Misc/`](Misc/) | Assorted demos — release-note walkthroughs, Databricks Asset Bundles, LLM demos, and scratch notebooks. |

## Usage

- **Notebooks** (`.py`, `.sql`, `.ipynb`) are Databricks notebooks — import them into a
  workspace via *Workspace → Import*, or clone this repo through Databricks Repos.
- **Dashboards** (`.lvdash.json`) import via the AI/BI Dashboards UI. Many depend on Unity
  Catalog system tables; enable them with
  [`Governance/enable_system_tables.py`](Governance/enable_system_tables.py).
- Notebooks generally parameterize `catalog` / `schema`; update those to your own before running.

---

*Repository: [youssefmrini/Databricks-Assets](https://github.com/youssefmrini/Databricks-Assets)*
