# Governance

Unity Catalog governance assets: system tables, lineage, permissions, Delta Sharing,
predictive optimization, online tables, and catalog maintenance.

## Numbered walkthroughs

| File | Type | Description |
|------|------|-------------|
| `01 - Catalog Cleansing.py` | Python | Clean up and reorganize catalogs/schemas. |
| `02 - View Sharing.sql` | SQL | Share views across catalogs/workspaces. |
| `03 - Primary and Foreign Keys.py` | Python | Declaring PK/FK constraints in Unity Catalog. |
| `04 - Predictive Optimization Commands.py` | Python | Enabling and managing predictive optimization. |
| `05 - Predictive Optimization Demo.sql` | SQL | Predictive optimization in action. |
| `06 - System Tables Cost Dashboard.sql` | SQL | Cost dashboard queries over system tables. |
| `07 - System Tables Query History.sql` | SQL | Query-history analysis via system tables. |

## Standalone notebooks

| File | Type | Description |
|------|------|-------------|
| `enable_system_tables.py` | Notebook | Programmatically enable Unity Catalog system schemas via the UC API (requires account admin). |
| `System tables.py` | Notebook | Working with Unity Catalog system tables. |
| `Lineage.py` | Notebook | Exploring data lineage in Unity Catalog. |
| `Grant Permissions.py` | Notebook | Granting and managing UC privileges. |
| `Delta Sharing.sql` | SQL | Delta Sharing setup and usage. |
| `Delta Sharing Youssef.py` | Notebook | Delta Sharing walkthrough (Python). |
| `Convert External to managed table.py` | Notebook | Convert external tables to managed tables. |
| `Deploy online table.py` | Notebook | Deploy an online table for low-latency serving. |
| `online-tables.py` | Notebook | Online tables usage and management. |
