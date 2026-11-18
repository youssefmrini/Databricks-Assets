# Databricks-Assets

A collection of Databricks notebooks, SQL scripts, and AI/BI dashboards covering
Delta Lake and Apache Iceberg. Assets are organized into topical folders — each folder
has its own README describing every file.

## Structure

| Folder | What's inside |
|--------|---------------|
| [`Delta-Lake/`](Delta-Lake/) | Delta Lake storage features — liquid clustering, deletion vectors, column mapping, identity columns, variant type, type widening, cloning, UniForm, and Iceberg comparisons. Includes [`features/`](Delta-Lake/features/) deep-dives. |
| [`Apache-Iceberg/`](Apache-Iceberg/) | Apache Iceberg V3 demos — lifecycle, Variant on Iceberg, liquid clustering, UniForm V3, row-level concurrency, managed Iceberg tables. |
| [`Dashboards/`](Dashboards/) | Importable AI/BI (Lakeview) dashboards for account usage, governance, Genie cost, and jobs. |
| [`SQL/`](SQL/) | Databricks SQL — SQL scripting, warehouse optimization, AI functions, system-table cost/audit analysis, connectors, Genie setup. |
| [`demo-devrel/`](demo-devrel/) | DevRel video demos built on fictional Nimbus payments data — [`ai-functions-devpulse/`](demo-devrel/ai-functions-devpulse/) (Databricks AI Functions end to end: config, data generator, pipeline) and [`ip-functions-netsentinel/`](demo-devrel/ip-functions-netsentinel/) (Databricks IP functions for API security: data generator and end-to-end demos). |
| [`deprecated/`](deprecated/) | Older assets kept for reference — data engineering, DLT/streaming, ML/MLOps, governance, and miscellaneous demos. |

## Usage

- **Notebooks** (`.py`, `.sql`, `.ipynb`) are Databricks notebooks — import them into a
  workspace via *Workspace → Import*, or clone this repo through Databricks Repos.
- **Dashboards** (`.lvdash.json`) import via the AI/BI Dashboards UI. Many depend on Unity
  Catalog system tables; enable them with
  [`deprecated/Governance/enable_system_tables.py`](deprecated/Governance/enable_system_tables.py).
- Notebooks generally parameterize `catalog` / `schema`; update those to your own before running.

---

*Repository: [youssefmrini/Databricks-Assets](https://github.com/youssefmrini/Databricks-Assets)*
