# Dashboards

Pre-built Databricks AI/BI (Lakeview) dashboards. Import a `.lvdash.json` file into a
Databricks workspace to get the visualizations without rebuilding the queries. Most rely
on Unity Catalog **system tables** — see [`../deprecated/Governance/enable_system_tables.py`](../deprecated/Governance/enable_system_tables.py)
to enable them.

| File | Description |
|------|-------------|
| `Account Usage Dashboard v2.lvdash.json` | Account-level usage and cost across warehouses, jobs, and storage. |
| `GOVERNANCE HUB SYSTEM DASHBOARD.lvdash.json` | Governance hub — lineage, access, and compliance metrics from system tables. |
| `Genie Cost Monitoring.lvdash.json` | Track Genie space usage and associated costs. |
| `Jobs System Tables Dashboard.lvdash.json` | Job runs, duration, success/failure, and resource usage from jobs system tables. |
