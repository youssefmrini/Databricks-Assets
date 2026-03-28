# Databricks notebook source
# MAGIC %md
# MAGIC # Fix Groupe Crystal Dashboard v2
# MAGIC Delete broken dashboard and recreate with correct field references.

# COMMAND ----------

import requests
import json

token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
host = "https://e2-demo-field-eng.cloud.databricks.com"
headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

OLD_DASHBOARD_ID = "01f11260d14f195fa38b033056df27bb"
WAREHOUSE_ID = "4b9b953939869799"

# COMMAND ----------

# Step 1: Delete old broken dashboard
print(f"Deleting old dashboard {OLD_DASHBOARD_ID}...")
resp = requests.delete(f"{host}/api/2.0/lakeview/dashboards/{OLD_DASHBOARD_ID}", headers=headers)
print(f"Delete status: {resp.status_code} - {resp.text}")

# COMMAND ----------

# Step 2: Build the new dashboard with correct format
# Datasets as a LIST, using queryLines (list of strings)

datasets = [
    {
        "name": "f9e80b57",
        "displayName": "AUM by Advisor",
        "queryLines": [
            "SELECT a.name AS conseiller, ROUND(SUM(p.total_value)/1e6,2) AS aum_millions FROM demo_youssefmrini.groupe_crystal.portfolios p JOIN demo_youssefmrini.groupe_crystal.clients c ON p.client_id = c.client_id JOIN demo_youssefmrini.groupe_crystal.advisors a ON c.advisor_id = a.advisor_id GROUP BY a.name ORDER BY aum_millions DESC"
        ]
    },
    {
        "name": "7686b341",
        "displayName": "KPI Totals",
        "queryLines": [
            "SELECT COUNT(DISTINCT p.portfolio_id) AS total_portfolios, COUNT(DISTINCT c.client_id) AS total_clients, ROUND(SUM(p.total_value)/1e6, 2) AS total_aum_millions FROM demo_youssefmrini.groupe_crystal.portfolios p JOIN demo_youssefmrini.groupe_crystal.clients c ON p.client_id = c.client_id"
        ]
    },
    {
        "name": "0ebb0ad6",
        "displayName": "AUM by Risk Profile",
        "queryLines": [
            "SELECT c.risk_profile, ROUND(SUM(p.total_value)/1e6,2) AS aum_millions FROM demo_youssefmrini.groupe_crystal.portfolios p JOIN demo_youssefmrini.groupe_crystal.clients c ON p.client_id = c.client_id GROUP BY c.risk_profile ORDER BY aum_millions DESC"
        ]
    },
    {
        "name": "cdf57197",
        "displayName": "Transaction Volume Over Time",
        "queryLines": [
            "SELECT DATE_TRUNC('month', transaction_date) AS mois, COUNT(*) AS nb_transactions, ROUND(SUM(amount)/1e3,1) AS volume_k FROM demo_youssefmrini.groupe_crystal.transactions GROUP BY 1 ORDER BY 1"
        ]
    },
    {
        "name": "ced350e5",
        "displayName": "Transactions by Asset Class",
        "queryLines": [
            "SELECT asset_class, COUNT(*) AS nb, ROUND(SUM(amount)/1e3,1) AS volume_k FROM demo_youssefmrini.groupe_crystal.transactions GROUP BY asset_class ORDER BY volume_k DESC"
        ]
    },
    {
        "name": "fa83293c",
        "displayName": "Transactions by Type",
        "queryLines": [
            "SELECT transaction_type, COUNT(*) AS nb FROM demo_youssefmrini.groupe_crystal.transactions GROUP BY transaction_type ORDER BY nb DESC"
        ]
    },
    {
        "name": "9c8304b1",
        "displayName": "Top Assets by Volume",
        "queryLines": [
            "SELECT asset_name, COUNT(*) AS nb_transactions, ROUND(SUM(amount)/1e3,1) AS volume_k FROM demo_youssefmrini.groupe_crystal.transactions GROUP BY asset_name ORDER BY volume_k DESC LIMIT 10"
        ]
    },
    {
        "name": "2edc593e",
        "displayName": "NAV Trend Top 5 Envelopes",
        "queryLines": [
            "SELECT v.valuation_date, e.envelope_type, CAST(e.envelope_id AS STRING) AS envelope_id, ROUND(v.nav,2) AS nav FROM demo_youssefmrini.groupe_crystal.contract_valuation v JOIN demo_youssefmrini.groupe_crystal.envelopes e ON v.envelope_id = e.envelope_id WHERE e.envelope_id IN (SELECT envelope_id FROM demo_youssefmrini.groupe_crystal.envelopes ORDER BY current_value DESC LIMIT 5) ORDER BY v.valuation_date"
        ]
    },
    {
        "name": "84059692",
        "displayName": "Top Envelopes by Unrealized PnL",
        "queryLines": [
            "SELECT CAST(e.envelope_id AS STRING) AS envelope_id, e.envelope_type, CAST(p.portfolio_id AS STRING) AS portfolio_id, ROUND(MAX(v.unrealized_pnl),0) AS pnl FROM demo_youssefmrini.groupe_crystal.contract_valuation v JOIN demo_youssefmrini.groupe_crystal.envelopes e ON v.envelope_id = e.envelope_id JOIN demo_youssefmrini.groupe_crystal.portfolios p ON e.portfolio_id = p.portfolio_id GROUP BY e.envelope_id, e.envelope_type, p.portfolio_id ORDER BY pnl DESC LIMIT 10"
        ]
    },
    {
        "name": "5689b9aa",
        "displayName": "Total AUM Trend",
        "queryLines": [
            "SELECT valuation_date, ROUND(SUM(total_value)/1e6,2) AS aum_millions FROM demo_youssefmrini.groupe_crystal.contract_valuation GROUP BY valuation_date ORDER BY valuation_date"
        ]
    },
    {
        "name": "bb130264",
        "displayName": "Advisor Summary",
        "queryLines": [
            "SELECT a.name AS advisor_name, COUNT(DISTINCT c.client_id) AS nb_clients, ROUND(SUM(p.total_value)/1e6,2) AS aum_millions FROM demo_youssefmrini.groupe_crystal.clients c JOIN demo_youssefmrini.groupe_crystal.advisors a ON c.advisor_id = a.advisor_id JOIN demo_youssefmrini.groupe_crystal.portfolios p ON p.client_id = c.client_id GROUP BY a.name ORDER BY aum_millions DESC"
        ]
    },
    {
        "name": "08e9d0f9",
        "displayName": "Client Distribution by Advisor",
        "queryLines": [
            "SELECT a.name AS advisor_name, c.risk_profile, COUNT(DISTINCT c.client_id) AS client_count FROM demo_youssefmrini.groupe_crystal.clients c JOIN demo_youssefmrini.groupe_crystal.advisors a ON c.advisor_id = a.advisor_id GROUP BY a.name, c.risk_profile ORDER BY a.name"
        ]
    }
]

# Pages as a LIST with correct field references
pages = [
    {
        "name": "60f90b95",
        "displayName": "Vue Portefeuille",
        "layout": [
            {
                "widget": {
                    "name": "230d6d68",
                    "queries": [{"name": "q", "query": {"datasetName": "7686b341", "fields": [{"name": "total_aum_millions", "expression": "SUM(`total_aum_millions`)"}], "disaggregated": False}}],
                    "spec": {"version": 3, "widgetType": "counter", "encodings": {"value": {"fieldName": "total_aum_millions", "displayName": "AUM Total (M\u20ac)"}}, "frame": {"showTitle": True, "title": "AUM Total"}}
                },
                "position": {"x": 0, "y": 0, "width": 2, "height": 3}
            },
            {
                "widget": {
                    "name": "74e9a3ec",
                    "queries": [{"name": "q", "query": {"datasetName": "7686b341", "fields": [{"name": "total_portfolios", "expression": "SUM(`total_portfolios`)"}], "disaggregated": False}}],
                    "spec": {"version": 3, "widgetType": "counter", "encodings": {"value": {"fieldName": "total_portfolios", "displayName": "Portefeuilles"}}, "frame": {"showTitle": True, "title": "Nombre de Portefeuilles"}}
                },
                "position": {"x": 2, "y": 0, "width": 2, "height": 3}
            },
            {
                "widget": {
                    "name": "cb504a5b",
                    "queries": [{"name": "q", "query": {"datasetName": "7686b341", "fields": [{"name": "total_clients", "expression": "SUM(`total_clients`)"}], "disaggregated": False}}],
                    "spec": {"version": 3, "widgetType": "counter", "encodings": {"value": {"fieldName": "total_clients", "displayName": "Clients"}}, "frame": {"showTitle": True, "title": "Nombre de Clients"}}
                },
                "position": {"x": 4, "y": 0, "width": 2, "height": 3}
            },
            {
                "widget": {
                    "name": "ef82ddf0",
                    "queries": [{"name": "q", "query": {"datasetName": "f9e80b57", "fields": [{"name": "conseiller", "expression": "`conseiller`"}, {"name": "aum_millions", "expression": "SUM(`aum_millions`)"}], "disaggregated": False}}],
                    "spec": {"version": 3, "widgetType": "bar", "encodings": {"x": {"fieldName": "conseiller", "scale": {"type": "categorical", "sort": {"by": "y-reversed"}}, "displayName": "Conseiller"}, "y": {"fieldName": "aum_millions", "scale": {"type": "quantitative"}, "displayName": "AUM (M\u20ac)"}, "label": {"show": True}}, "frame": {"showTitle": True, "title": "AUM par Conseiller"}}
                },
                "position": {"x": 0, "y": 3, "width": 4, "height": 5}
            },
            {
                "widget": {
                    "name": "6de5ddc3",
                    "queries": [{"name": "q", "query": {"datasetName": "0ebb0ad6", "fields": [{"name": "risk_profile", "expression": "`risk_profile`"}, {"name": "aum_millions", "expression": "SUM(`aum_millions`)"}], "disaggregated": False}}],
                    "spec": {"version": 3, "widgetType": "pie", "encodings": {"angle": {"fieldName": "aum_millions", "scale": {"type": "quantitative"}, "displayName": "AUM (M\u20ac)"}, "color": {"fieldName": "risk_profile", "scale": {"type": "categorical"}, "displayName": "Profil de Risque"}}, "frame": {"showTitle": True, "title": "AUM par Profil de Risque"}}
                },
                "position": {"x": 4, "y": 3, "width": 2, "height": 5}
            }
        ],
        "pageType": "PAGE_TYPE_CANVAS"
    },
    {
        "name": "10a4b16a",
        "displayName": "Analyse Transactions",
        "layout": [
            {
                "widget": {
                    "name": "e3d770e3",
                    "queries": [{"name": "q", "query": {"datasetName": "cdf57197", "fields": [{"name": "mois", "expression": "`mois`"}, {"name": "volume_k", "expression": "SUM(`volume_k`)"}], "disaggregated": False}}],
                    "spec": {"version": 3, "widgetType": "area", "encodings": {"x": {"fieldName": "mois", "scale": {"type": "temporal"}, "displayName": "Mois"}, "y": {"fieldName": "volume_k", "scale": {"type": "quantitative"}, "displayName": "Volume (k\u20ac)"}}, "frame": {"showTitle": True, "title": "Volume de Transactions dans le Temps"}}
                },
                "position": {"x": 0, "y": 0, "width": 6, "height": 4}
            },
            {
                "widget": {
                    "name": "d3590c18",
                    "queries": [{"name": "q", "query": {"datasetName": "ced350e5", "fields": [{"name": "asset_class", "expression": "`asset_class`"}, {"name": "volume_k", "expression": "SUM(`volume_k`)"}], "disaggregated": False}}],
                    "spec": {"version": 3, "widgetType": "bar", "encodings": {"x": {"fieldName": "asset_class", "scale": {"type": "categorical", "sort": {"by": "y-reversed"}}, "displayName": "Classe d'Actif"}, "y": {"fieldName": "volume_k", "scale": {"type": "quantitative"}, "displayName": "Volume Total (k\u20ac)"}, "label": {"show": True}}, "frame": {"showTitle": True, "title": "Volume par Classe d'Actif"}}
                },
                "position": {"x": 0, "y": 4, "width": 3, "height": 4}
            },
            {
                "widget": {
                    "name": "69664513",
                    "queries": [{"name": "q", "query": {"datasetName": "fa83293c", "fields": [{"name": "transaction_type", "expression": "`transaction_type`"}, {"name": "nb", "expression": "SUM(`nb`)"}], "disaggregated": False}}],
                    "spec": {"version": 3, "widgetType": "pie", "encodings": {"angle": {"fieldName": "nb", "scale": {"type": "quantitative"}, "displayName": "Nombre"}, "color": {"fieldName": "transaction_type", "scale": {"type": "categorical"}, "displayName": "Type"}}, "frame": {"showTitle": True, "title": "R\u00e9partition par Type de Transaction"}}
                },
                "position": {"x": 3, "y": 4, "width": 3, "height": 4}
            },
            {
                "widget": {
                    "name": "0df81b70",
                    "queries": [{"name": "q", "query": {"datasetName": "9c8304b1", "fields": [{"name": "asset_name", "expression": "`asset_name`"}, {"name": "nb_transactions", "expression": "`nb_transactions`"}, {"name": "volume_k", "expression": "`volume_k`"}], "disaggregated": True}}],
                    "spec": {"version": 1, "widgetType": "table", "encodings": {"columns": [{"fieldName": "asset_name", "type": "string", "displayAs": "string", "title": "Actif"}, {"fieldName": "nb_transactions", "type": "integer", "displayAs": "number", "title": "Nb Transactions", "alignContent": "right"}, {"fieldName": "volume_k", "type": "float", "displayAs": "number", "numberFormat": "#,##0.0", "title": "Volume (k\u20ac)", "alignContent": "right"}]}, "frame": {"showTitle": True, "title": "Top 10 Actifs par Volume"}}
                },
                "position": {"x": 0, "y": 8, "width": 6, "height": 4}
            }
        ],
        "pageType": "PAGE_TYPE_CANVAS"
    },
    {
        "name": "4f88d821",
        "displayName": "Valorisation Contrats",
        "layout": [
            {
                "widget": {
                    "name": "4dd421be",
                    "queries": [{"name": "q", "query": {"datasetName": "5689b9aa", "fields": [{"name": "valuation_date", "expression": "`valuation_date`"}, {"name": "aum_millions", "expression": "SUM(`aum_millions`)"}], "disaggregated": False}}],
                    "spec": {"version": 3, "widgetType": "area", "encodings": {"x": {"fieldName": "valuation_date", "scale": {"type": "temporal"}, "displayName": "Date"}, "y": {"fieldName": "aum_millions", "scale": {"type": "quantitative"}, "displayName": "AUM Total (M\u20ac)"}}, "frame": {"showTitle": True, "title": "\u00c9volution de l'AUM Total (12 mois)"}}
                },
                "position": {"x": 0, "y": 0, "width": 6, "height": 4}
            },
            {
                "widget": {
                    "name": "cf90a2d5",
                    "queries": [{"name": "q", "query": {"datasetName": "2edc593e", "fields": [{"name": "valuation_date", "expression": "`valuation_date`"}, {"name": "envelope_id", "expression": "`envelope_id`"}, {"name": "nav", "expression": "AVG(`nav`)"}], "disaggregated": False}}],
                    "spec": {"version": 3, "widgetType": "line", "encodings": {"x": {"fieldName": "valuation_date", "scale": {"type": "temporal"}, "displayName": "Date"}, "y": {"fieldName": "nav", "scale": {"type": "quantitative"}, "displayName": "VL (NAV)"}, "color": {"fieldName": "envelope_id", "scale": {"type": "categorical"}, "displayName": "Enveloppe"}}, "frame": {"showTitle": True, "title": "\u00c9volution de la VL \u2014 Top 5 Enveloppes"}}
                },
                "position": {"x": 0, "y": 4, "width": 4, "height": 5}
            },
            {
                "widget": {
                    "name": "6c9952d4",
                    "queries": [{"name": "q", "query": {"datasetName": "84059692", "fields": [{"name": "envelope_id", "expression": "`envelope_id`"}, {"name": "pnl", "expression": "SUM(`pnl`)"}, {"name": "envelope_type", "expression": "`envelope_type`"}], "disaggregated": False}}],
                    "spec": {"version": 3, "widgetType": "bar", "encodings": {"x": {"fieldName": "envelope_id", "scale": {"type": "categorical", "sort": {"by": "y-reversed"}}, "displayName": "Enveloppe"}, "y": {"fieldName": "pnl", "scale": {"type": "quantitative"}, "displayName": "PnL Latent (\u20ac)"}, "color": {"fieldName": "envelope_type", "scale": {"type": "categorical"}, "displayName": "Type"}, "label": {"show": True}}, "frame": {"showTitle": True, "title": "Top 10 Enveloppes \u2014 PnL Latent"}}
                },
                "position": {"x": 4, "y": 4, "width": 2, "height": 5}
            }
        ],
        "pageType": "PAGE_TYPE_CANVAS"
    },
    {
        "name": "75d927a8",
        "displayName": "Vue Conseiller",
        "layout": [
            {
                "widget": {
                    "name": "0f443868",
                    "multilineTextboxSpec": {
                        "lines": [
                            "## Vue Conseiller\n",
                            "\n",
                            "Cette page affiche la r\u00e9partition des clients et de l'AUM par conseiller, ainsi que la distribution par profil de risque.\n"
                        ]
                    }
                },
                "position": {"x": 0, "y": 0, "width": 6, "height": 2}
            },
            {
                "widget": {
                    "name": "3e951a4c",
                    "queries": [{"name": "q", "query": {"datasetName": "bb130264", "fields": [{"name": "advisor_name", "expression": "`advisor_name`"}, {"name": "nb_clients", "expression": "`nb_clients`"}, {"name": "aum_millions", "expression": "`aum_millions`"}], "disaggregated": True}}],
                    "spec": {"version": 1, "widgetType": "table", "encodings": {"columns": [{"fieldName": "advisor_name", "type": "string", "displayAs": "string", "title": "Conseiller"}, {"fieldName": "nb_clients", "type": "integer", "displayAs": "number", "title": "Clients", "alignContent": "right"}, {"fieldName": "aum_millions", "type": "float", "displayAs": "number", "numberFormat": "#,##0.00", "title": "AUM (M\u20ac)", "alignContent": "right"}]}, "frame": {"showTitle": True, "title": "R\u00e9sum\u00e9 par Conseiller"}}
                },
                "position": {"x": 0, "y": 2, "width": 6, "height": 4}
            },
            {
                "widget": {
                    "name": "98f469a9",
                    "queries": [{"name": "q", "query": {"datasetName": "08e9d0f9", "fields": [{"name": "advisor_name", "expression": "`advisor_name`"}, {"name": "risk_profile", "expression": "`risk_profile`"}, {"name": "client_count", "expression": "SUM(`client_count`)"}], "disaggregated": False}}],
                    "spec": {"version": 3, "widgetType": "bar", "encodings": {"x": {"fieldName": "advisor_name", "scale": {"type": "categorical"}, "displayName": "Conseiller"}, "y": {"fieldName": "client_count", "scale": {"type": "quantitative"}, "displayName": "Nombre de Clients"}, "color": {"fieldName": "risk_profile", "scale": {"type": "categorical"}, "displayName": "Profil de Risque"}}, "frame": {"showTitle": True, "title": "Clients par Conseiller et Profil de Risque"}}
                },
                "position": {"x": 0, "y": 6, "width": 6, "height": 5}
            }
        ],
        "pageType": "PAGE_TYPE_CANVAS"
    }
]

serialized_dashboard = json.dumps({"datasets": datasets, "pages": pages})

print("Dashboard JSON built successfully")
print(f"Datasets: {len(datasets)}")
print(f"Pages: {len(pages)}")
print(f"JSON length: {len(serialized_dashboard)}")

# COMMAND ----------

# Step 3: Create new dashboard
create_payload = {
    "display_name": "Groupe Crystal - Vue Patrimoine",
    "warehouse_id": WAREHOUSE_ID,
    "serialized_dashboard": serialized_dashboard,
    "parent_path": "/Workspace/Users/youssef.mrini@databricks.com"
}

print("Creating new dashboard...")
resp = requests.post(
    f"{host}/api/2.0/lakeview/dashboards",
    headers=headers,
    json=create_payload
)
print(f"Create status: {resp.status_code}")

if resp.status_code == 200:
    result = resp.json()
    new_dashboard_id = result.get("dashboard_id")
    print(f"New dashboard ID: {new_dashboard_id}")
    print(f"Dashboard path: {result.get('path')}")
else:
    print(f"Error: {resp.text}")
    dbutils.notebook.exit(json.dumps({"error": resp.text, "status": resp.status_code}))

# COMMAND ----------

# Step 4: Publish the dashboard
print(f"Publishing dashboard {new_dashboard_id}...")
publish_payload = {
    "warehouse_id": WAREHOUSE_ID,
    "embed_credentials": True
}

resp = requests.post(
    f"{host}/api/2.0/lakeview/dashboards/{new_dashboard_id}/published",
    headers=headers,
    json=publish_payload
)
print(f"Publish status: {resp.status_code}")
print(f"Publish response: {resp.text}")

published_url = f"{host}/dashboardsv3/{new_dashboard_id}/published"
embed_url = f"{host}/embed/dashboardsv3/{new_dashboard_id}"

print(f"\nDashboard published!")
print(f"Published URL: {published_url}")
print(f"Embed URL: {embed_url}")

dbutils.notebook.exit(json.dumps({
    "dashboard_id": new_dashboard_id,
    "published_url": published_url,
    "embed_url": embed_url
}))
