# Databricks notebook source
# MAGIC %md
# MAGIC # Fix Groupe Crystal Dashboard
# MAGIC Delete broken dashboard and recreate with valid queries.

# COMMAND ----------

import requests
import json

# Get workspace token from inside the notebook
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

# Step 2: Build the new dashboard

datasets = {
    "f9e80b57": {
        "name": "aum_par_conseiller",
        "query": "SELECT a.name AS conseiller, ROUND(SUM(p.total_value)/1e6,2) AS aum_millions FROM demo_youssefmrini.groupe_crystal.portfolios p JOIN demo_youssefmrini.groupe_crystal.clients c ON p.client_id = c.client_id JOIN demo_youssefmrini.groupe_crystal.advisors a ON c.advisor_id = a.advisor_id GROUP BY a.name ORDER BY aum_millions DESC"
    },
    "7686b341": {
        "name": "kpi_globaux",
        "query": "SELECT COUNT(DISTINCT p.portfolio_id) AS total_portfolios, COUNT(DISTINCT c.client_id) AS total_clients, ROUND(SUM(p.total_value)/1e6, 2) AS total_aum_millions FROM demo_youssefmrini.groupe_crystal.portfolios p JOIN demo_youssefmrini.groupe_crystal.clients c ON p.client_id = c.client_id"
    },
    "0ebb0ad6": {
        "name": "aum_par_profil_risque",
        "query": "SELECT c.risk_profile, ROUND(SUM(p.total_value)/1e6,2) AS aum_millions FROM demo_youssefmrini.groupe_crystal.portfolios p JOIN demo_youssefmrini.groupe_crystal.clients c ON p.client_id = c.client_id GROUP BY c.risk_profile ORDER BY aum_millions DESC"
    },
    "cdf57197": {
        "name": "transactions_par_mois",
        "query": "SELECT DATE_TRUNC('month', transaction_date) AS mois, COUNT(*) AS nb_transactions, ROUND(SUM(amount)/1e3,1) AS volume_k FROM demo_youssefmrini.groupe_crystal.transactions GROUP BY 1 ORDER BY 1"
    },
    "ced350e5": {
        "name": "volume_par_classe_actif",
        "query": "SELECT asset_class, COUNT(*) AS nb, ROUND(SUM(amount)/1e3,1) AS volume_k FROM demo_youssefmrini.groupe_crystal.transactions GROUP BY asset_class ORDER BY volume_k DESC"
    },
    "fa83293c": {
        "name": "type_transaction",
        "query": "SELECT transaction_type, COUNT(*) AS nb FROM demo_youssefmrini.groupe_crystal.transactions GROUP BY transaction_type ORDER BY nb DESC"
    },
    "9c8304b1": {
        "name": "top10_actifs",
        "query": "SELECT asset_name, COUNT(*) AS nb_transactions, ROUND(SUM(amount)/1e3,1) AS volume_k FROM demo_youssefmrini.groupe_crystal.transactions GROUP BY asset_name ORDER BY volume_k DESC LIMIT 10"
    },
    "2edc593e": {
        "name": "nav_top5_envelopes",
        "query": "SELECT v.valuation_date, e.envelope_type, CAST(e.envelope_id AS STRING) AS envelope_id, ROUND(v.nav,2) AS nav FROM demo_youssefmrini.groupe_crystal.contract_valuation v JOIN demo_youssefmrini.groupe_crystal.envelopes e ON v.envelope_id = e.envelope_id WHERE e.envelope_id IN (SELECT envelope_id FROM demo_youssefmrini.groupe_crystal.envelopes ORDER BY current_value DESC LIMIT 5) ORDER BY v.valuation_date"
    },
    "84059692": {
        "name": "pnl_top10_envelopes",
        "query": "SELECT CAST(e.envelope_id AS STRING) AS envelope_id, e.envelope_type, CAST(p.portfolio_id AS STRING) AS portfolio_id, ROUND(MAX(v.unrealized_pnl),0) AS pnl FROM demo_youssefmrini.groupe_crystal.contract_valuation v JOIN demo_youssefmrini.groupe_crystal.envelopes e ON v.envelope_id = e.envelope_id JOIN demo_youssefmrini.groupe_crystal.portfolios p ON e.portfolio_id = p.portfolio_id GROUP BY e.envelope_id, e.envelope_type, p.portfolio_id ORDER BY pnl DESC LIMIT 10"
    },
    "5689b9aa": {
        "name": "aum_evolution",
        "query": "SELECT valuation_date, ROUND(SUM(total_value)/1e6,2) AS aum_millions FROM demo_youssefmrini.groupe_crystal.contract_valuation GROUP BY valuation_date ORDER BY valuation_date"
    },
    "bb130264": {
        "name": "advisors_overview",
        "query": "SELECT a.name AS advisor_name, COUNT(DISTINCT c.client_id) AS nb_clients, ROUND(SUM(p.total_value)/1e6,2) AS aum_millions FROM demo_youssefmrini.groupe_crystal.clients c JOIN demo_youssefmrini.groupe_crystal.advisors a ON c.advisor_id = a.advisor_id JOIN demo_youssefmrini.groupe_crystal.portfolios p ON p.client_id = c.client_id GROUP BY a.name ORDER BY aum_millions DESC"
    },
    "08e9d0f9": {
        "name": "advisors_risk_profile",
        "query": "SELECT a.name AS advisor_name, c.risk_profile, COUNT(DISTINCT c.client_id) AS client_count FROM demo_youssefmrini.groupe_crystal.clients c JOIN demo_youssefmrini.groupe_crystal.advisors a ON c.advisor_id = a.advisor_id GROUP BY a.name, c.risk_profile ORDER BY a.name"
    }
}

# Build datasets section
datasets_json = {}
for ds_id, ds_info in datasets.items():
    datasets_json[ds_id] = {
        "name": ds_info["name"],
        "query": ds_info["query"]
    }

# Build pages with widgets
pages = {
    # Page 1: Vue d'ensemble
    "p_overview": {
        "name": "Vue d'ensemble",
        "displayName": "Vue d'ensemble",
        "layout": [
            {
                "widget": {
                    "name": "w_kpi",
                    "queries": [{"name": "main_query", "query": {"datasetName": "kpi_globaux", "fields": [{"name": "total_portfolios", "expression": "`total_portfolios`"}, {"name": "total_clients", "expression": "`total_clients`"}, {"name": "total_aum_millions", "expression": "`total_aum_millions`"}], "disaggregated": False}}],
                    "spec": {
                        "version": 2,
                        "widgetType": "counter",
                        "encodings": {
                            "value": {"fieldName": "total_aum_millions", "displayName": "AUM Total (M EUR)"}
                        }
                    },
                    "overrides": {}
                },
                "position": {"x": 0, "y": 0, "width": 6, "height": 2}
            },
            {
                "widget": {
                    "name": "w_aum_conseiller",
                    "queries": [{"name": "main_query", "query": {"datasetName": "aum_par_conseiller", "fields": [{"name": "conseiller", "expression": "`conseiller`"}, {"name": "aum_millions", "expression": "`aum_millions`"}], "disaggregated": False}}],
                    "spec": {
                        "version": 2,
                        "widgetType": "bar",
                        "encodings": {
                            "x": {"fieldName": "conseiller", "scale": {"type": "categorical"}, "displayName": "Conseiller"},
                            "y": {"fieldName": "aum_millions", "scale": {"type": "quantitative"}, "displayName": "AUM (M EUR)"}
                        }
                    },
                    "overrides": {}
                },
                "position": {"x": 0, "y": 2, "width": 6, "height": 4}
            },
            {
                "widget": {
                    "name": "w_aum_risque",
                    "queries": [{"name": "main_query", "query": {"datasetName": "aum_par_profil_risque", "fields": [{"name": "risk_profile", "expression": "`risk_profile`"}, {"name": "aum_millions", "expression": "`aum_millions`"}], "disaggregated": False}}],
                    "spec": {
                        "version": 2,
                        "widgetType": "pie",
                        "encodings": {
                            "angle": {"fieldName": "aum_millions", "displayName": "AUM (M EUR)"},
                            "color": {"fieldName": "risk_profile", "scale": {"type": "categorical"}, "displayName": "Profil de risque"}
                        }
                    },
                    "overrides": {}
                },
                "position": {"x": 0, "y": 6, "width": 3, "height": 4}
            },
            {
                "widget": {
                    "name": "w_advisors_overview",
                    "queries": [{"name": "main_query", "query": {"datasetName": "advisors_overview", "fields": [{"name": "advisor_name", "expression": "`advisor_name`"}, {"name": "nb_clients", "expression": "`nb_clients`"}, {"name": "aum_millions", "expression": "`aum_millions`"}], "disaggregated": False}}],
                    "spec": {
                        "version": 2,
                        "widgetType": "table"
                    },
                    "overrides": {}
                },
                "position": {"x": 3, "y": 6, "width": 3, "height": 4}
            }
        ]
    },
    # Page 2: Transactions
    "p_transactions": {
        "name": "Transactions",
        "displayName": "Transactions",
        "layout": [
            {
                "widget": {
                    "name": "w_transactions_mois",
                    "queries": [{"name": "main_query", "query": {"datasetName": "transactions_par_mois", "fields": [{"name": "mois", "expression": "`mois`"}, {"name": "nb_transactions", "expression": "`nb_transactions`"}, {"name": "volume_k", "expression": "`volume_k`"}], "disaggregated": False}}],
                    "spec": {
                        "version": 2,
                        "widgetType": "line",
                        "encodings": {
                            "x": {"fieldName": "mois", "scale": {"type": "temporal"}, "displayName": "Mois"},
                            "y": {"fieldName": "volume_k", "scale": {"type": "quantitative"}, "displayName": "Volume (K EUR)"}
                        }
                    },
                    "overrides": {}
                },
                "position": {"x": 0, "y": 0, "width": 6, "height": 4}
            },
            {
                "widget": {
                    "name": "w_volume_classe",
                    "queries": [{"name": "main_query", "query": {"datasetName": "volume_par_classe_actif", "fields": [{"name": "asset_class", "expression": "`asset_class`"}, {"name": "volume_k", "expression": "`volume_k`"}, {"name": "nb", "expression": "`nb`"}], "disaggregated": False}}],
                    "spec": {
                        "version": 2,
                        "widgetType": "bar",
                        "encodings": {
                            "x": {"fieldName": "asset_class", "scale": {"type": "categorical"}, "displayName": "Classe d'actif"},
                            "y": {"fieldName": "volume_k", "scale": {"type": "quantitative"}, "displayName": "Volume (K EUR)"}
                        }
                    },
                    "overrides": {}
                },
                "position": {"x": 0, "y": 4, "width": 3, "height": 4}
            },
            {
                "widget": {
                    "name": "w_type_transaction",
                    "queries": [{"name": "main_query", "query": {"datasetName": "type_transaction", "fields": [{"name": "transaction_type", "expression": "`transaction_type`"}, {"name": "nb", "expression": "`nb`"}], "disaggregated": False}}],
                    "spec": {
                        "version": 2,
                        "widgetType": "pie",
                        "encodings": {
                            "angle": {"fieldName": "nb", "displayName": "Nombre"},
                            "color": {"fieldName": "transaction_type", "scale": {"type": "categorical"}, "displayName": "Type"}
                        }
                    },
                    "overrides": {}
                },
                "position": {"x": 3, "y": 4, "width": 3, "height": 4}
            },
            {
                "widget": {
                    "name": "w_top10_actifs",
                    "queries": [{"name": "main_query", "query": {"datasetName": "top10_actifs", "fields": [{"name": "asset_name", "expression": "`asset_name`"}, {"name": "nb_transactions", "expression": "`nb_transactions`"}, {"name": "volume_k", "expression": "`volume_k`"}], "disaggregated": False}}],
                    "spec": {
                        "version": 2,
                        "widgetType": "bar",
                        "encodings": {
                            "x": {"fieldName": "asset_name", "scale": {"type": "categorical"}, "displayName": "Actif"},
                            "y": {"fieldName": "volume_k", "scale": {"type": "quantitative"}, "displayName": "Volume (K EUR)"}
                        }
                    },
                    "overrides": {}
                },
                "position": {"x": 0, "y": 8, "width": 6, "height": 4}
            }
        ]
    },
    # Page 3: Envelopes & Valorisation
    "p_envelopes": {
        "name": "Envelopes & Valorisation",
        "displayName": "Envelopes & Valorisation",
        "layout": [
            {
                "widget": {
                    "name": "w_nav_evolution",
                    "queries": [{"name": "main_query", "query": {"datasetName": "nav_top5_envelopes", "fields": [{"name": "valuation_date", "expression": "`valuation_date`"}, {"name": "envelope_type", "expression": "`envelope_type`"}, {"name": "envelope_id", "expression": "`envelope_id`"}, {"name": "nav", "expression": "`nav`"}], "disaggregated": False}}],
                    "spec": {
                        "version": 2,
                        "widgetType": "line",
                        "encodings": {
                            "x": {"fieldName": "valuation_date", "scale": {"type": "temporal"}, "displayName": "Date"},
                            "y": {"fieldName": "nav", "scale": {"type": "quantitative"}, "displayName": "NAV"},
                            "color": {"fieldName": "envelope_id", "scale": {"type": "categorical"}, "displayName": "Envelope"}
                        }
                    },
                    "overrides": {}
                },
                "position": {"x": 0, "y": 0, "width": 6, "height": 5}
            },
            {
                "widget": {
                    "name": "w_pnl_envelopes",
                    "queries": [{"name": "main_query", "query": {"datasetName": "pnl_top10_envelopes", "fields": [{"name": "envelope_id", "expression": "`envelope_id`"}, {"name": "envelope_type", "expression": "`envelope_type`"}, {"name": "portfolio_id", "expression": "`portfolio_id`"}, {"name": "pnl", "expression": "`pnl`"}], "disaggregated": False}}],
                    "spec": {
                        "version": 2,
                        "widgetType": "bar",
                        "encodings": {
                            "x": {"fieldName": "envelope_id", "scale": {"type": "categorical"}, "displayName": "Envelope"},
                            "y": {"fieldName": "pnl", "scale": {"type": "quantitative"}, "displayName": "PnL (EUR)"}
                        }
                    },
                    "overrides": {}
                },
                "position": {"x": 0, "y": 5, "width": 6, "height": 5}
            },
            {
                "widget": {
                    "name": "w_aum_evolution",
                    "queries": [{"name": "main_query", "query": {"datasetName": "aum_evolution", "fields": [{"name": "valuation_date", "expression": "`valuation_date`"}, {"name": "aum_millions", "expression": "`aum_millions`"}], "disaggregated": False}}],
                    "spec": {
                        "version": 2,
                        "widgetType": "line",
                        "encodings": {
                            "x": {"fieldName": "valuation_date", "scale": {"type": "temporal"}, "displayName": "Date"},
                            "y": {"fieldName": "aum_millions", "scale": {"type": "quantitative"}, "displayName": "AUM (M EUR)"}
                        }
                    },
                    "overrides": {}
                },
                "position": {"x": 0, "y": 10, "width": 6, "height": 4}
            }
        ]
    },
    # Page 4: Conseillers
    "p_advisors": {
        "name": "Conseillers",
        "displayName": "Conseillers",
        "layout": [
            {
                "widget": {
                    "name": "w_advisors_table",
                    "queries": [{"name": "main_query", "query": {"datasetName": "advisors_overview", "fields": [{"name": "advisor_name", "expression": "`advisor_name`"}, {"name": "nb_clients", "expression": "`nb_clients`"}, {"name": "aum_millions", "expression": "`aum_millions`"}], "disaggregated": False}}],
                    "spec": {
                        "version": 2,
                        "widgetType": "table"
                    },
                    "overrides": {}
                },
                "position": {"x": 0, "y": 0, "width": 6, "height": 5}
            },
            {
                "widget": {
                    "name": "w_risk_heatmap",
                    "queries": [{"name": "main_query", "query": {"datasetName": "advisors_risk_profile", "fields": [{"name": "advisor_name", "expression": "`advisor_name`"}, {"name": "risk_profile", "expression": "`risk_profile`"}, {"name": "client_count", "expression": "`client_count`"}], "disaggregated": False}}],
                    "spec": {
                        "version": 2,
                        "widgetType": "bar",
                        "encodings": {
                            "x": {"fieldName": "advisor_name", "scale": {"type": "categorical"}, "displayName": "Conseiller"},
                            "y": {"fieldName": "client_count", "scale": {"type": "quantitative"}, "displayName": "Nombre de clients"},
                            "color": {"fieldName": "risk_profile", "scale": {"type": "categorical"}, "displayName": "Profil de risque"}
                        }
                    },
                    "overrides": {}
                },
                "position": {"x": 0, "y": 5, "width": 6, "height": 5}
            }
        ]
    }
}

# Assemble the serialized_dashboard JSON
serialized_dashboard = json.dumps({
    "pages": pages,
    "datasets": datasets_json
})

print("Dashboard JSON built successfully")
print(f"Datasets: {len(datasets_json)}")
print(f"Pages: {len(pages)}")

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
    dbutils.notebook.exit(json.dumps({"error": resp.text}))

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
