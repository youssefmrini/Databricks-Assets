# Databricks notebook source
# DBTITLE 1,About this notebook
# MAGIC %md
# MAGIC # DevPulse 01: Data generator
# MAGIC
# MAGIC Creates all the **synthetic** inputs used by the **DevPulse - Databricks AI Functions End to End** notebook, for the fictional **NextGenLakehouse** payments API platform. Run it **once** before the pipeline notebook. It takes about 1 to 2 minutes.
# MAGIC
# MAGIC | Step | Output | Used by |
# MAGIC | --- | --- | --- |
# MAGIC | 1. Feedback and bug tracker | `feedback_raw` (24 multilingual messages with leaked PII and keys), `known_issues`, `release_note_drafts` | Act 1, Act 5 |
# MAGIC | 2. Structured tables | `accounts` (400), `daily_metrics` (200 days), `api_calls` (8,000) | Acts 3 and 4 |
# MAGIC | 3. Documents | 4 PDFs in `<volume>/docs/` | Act 2 |
# MAGIC | 4. Support calls | 2 MP3s in `<volume>/calls/` | Act 2 |
# MAGIC
# MAGIC **Configuration** lives in `devpulse_00_config` (catalog, schema, warehouse, ...). Use the same widget values here and in the pipeline notebook.
# MAGIC
# MAGIC **Re-running is safe.** Tables are overwritten with the same seeded data. Files that already exist are skipped (set `FORCE_REGENERATE = True` to rebuild them).
# MAGIC
# MAGIC **Internet access.** `%pip` needs PyPI, and step 4 calls Google Text-to-Speech (gTTS). Only the fixed call scripts below are sent, never workspace data.
# MAGIC
# MAGIC All names, emails, phone numbers and keys are **fake**. Company names are **fictional** too; any resemblance to real companies is coincidental.

# COMMAND ----------

# DBTITLE 1,Install libraries for fake PDFs and audio
# Libraries used ONLY to generate the synthetic inputs:
#   reportlab -> builds the 4 PDF documents
#   gTTS      -> Google Text-to-Speech, builds the 2 MP3 support calls
%pip install -q reportlab gTTS

# Restart Python so the new packages can be imported in the next cells.
dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Load shared configuration
# MAGIC %run ./devpulse_00_config

# COMMAND ----------

# DBTITLE 1,Step 1 header
# MAGIC %md
# MAGIC ## 1. Developer feedback, bug tracker and release-note drafts

# COMMAND ----------

# DBTITLE 1,Generate developer feedback
# -----------------------------------------------------------------------------
# 24 messages: (id, timestamp, channel, account, SDK language, raw text).
# Written in 6 languages and seeded with 4 recurring themes:
#   POST timeouts (NMB-408), duplicate webhooks, 429 without Retry-After, key rotation.
# Emails use ".example" domains, phones are 555 numbers and keys are made up:
# they exist so ai_mask has something to redact.
# -----------------------------------------------------------------------------
feedback = [
    ("F001", "2026-10-21 08:14", "github_issue", "Tunewave", "python",
     "After upgrading nextgenlakehouse-python to 3.0.1 every POST /v2/payments call times out after exactly 10 seconds. Worked fine on 2.9.4. Stack trace: NextGenLakehouseTimeoutError: request exceeded 10000ms (code NMB-408). Region eu-west-1. Contact me at lena.fischer@tunewave-dev.example or +1 415 555 0142. Our key sk_live_9f8a7b6c5d4e3f2a1b is attached for repro."),
    ("F002", "2026-10-21 09:02", "forum", "StayNest", "node",
     "Depuis la mise à jour vers le SDK Node 3.0.0, nos webhooks arrivent en double. Nous recevons deux fois l'événement payment.succeeded, ce qui crée des remboursements en double pour nos clients. C'est critique pour nous ! Merci de répondre à julien.martin@staynest-eng.example"),
    ("F003", "2026-10-21 10:30", "support_ticket", "Mercado Sol", "java",
     "Hola, el SDK de Java 3.0 devuelve error 429 Too Many Requests aunque solo hacemos 40 peticiones por segundo. Nuestro plan Enterprise debería permitir 500 rps. Esto está bloqueando nuestras pruebas para Black Friday. Teléfono: +52 55 5555 0199."),
    ("F004", "2026-10-21 11:45", "app_review", "Cartorama", "ruby",
     "Honestly the new NextGenLakehouse dashboard is gorgeous and the API explorer saved me hours. Docs for idempotency keys are top notch. 5 stars."),
    ("F005", "2026-10-21 13:10", "forum", "Kronwerk", "java",
     "Die Dokumentation zur API-Key-Rotation ist verwirrend. Nach dem Rotieren bekommen wir 401 statt 403, und es ist unklar, wie lange der alte Schlüssel gültig bleibt. Bitte klarstellen."),
    ("F006", "2026-10-21 14:20", "github_issue", "PagaFacile", "python",
     "Dopo l'aggiornamento all'SDK Python 3.0 le richieste POST vanno in timeout. Prima aspettavano 30 secondi, ora falliscono dopo 10 secondi con NMB-408. Sembra che anche i retry automatici siano disattivati."),
    ("F007", "2026-10-21 15:05", "support_ticket", "Graphhound", "go",
     "Go SDK 3.0 is great, 40% lower latency on GET /v2/customers. But the changelog doesn't mention that context cancellation now closes the connection pool. Took us a full day to find. Mixed feelings."),
    ("F008", "2026-10-21 16:40", "forum", "Inkwell Docs", "python",
     "Thread: 'Timeouts everywhere after 3.0?' | @dev_marco: Since Monday our batch payouts job fails with NMB-408 on POST /v2/payouts. | @sara_k: Same here, only for POST, GETs are fine. We're in eu-west-1. | @dev_marco: Downgrading to 2.9.4 fixes it. | @nextgenlakehouse_alex (staff): Thanks all, we're investigating. Could you share SDK version and region? | @priya.n: 3.0.1, eu-west-1, Python 3.12. Setting timeout=30 in the client constructor fixes the timeout for us but retries for POST still seem disabled. | @sara_k: Confirmed, timeout=30 plus max_retries=3 restores the 2.x behavior. Can this be the default again? | @dev_marco: +1, this broke production for 6 hours and our on-call was not happy. | @kenji_w: Also seeing it on POST /v2/refunds. Please put this in the migration guide in bold."),
    ("F009", "2026-10-22 08:05", "support_ticket", "SignalPost", "node",
     "Our webhook signature verification fails intermittently with 'NMB-401 invalid signature' since Node SDK 3.0.0. We use NTP so it's not clock skew. Secret whsec_5a4b3c2d1e0f for reference. Urgent: customers can't check out."),
    ("F010", "2026-10-22 09:30", "forum", "Banco Lumen", "python",
     "O SDK Python 3.0 está excelente, a nova paginação automática é incrível. Só falta um exemplo de uso com asyncio na documentação."),
    ("F011", "2026-10-22 10:12", "app_review", "RideLoop", "java",
     "Rate limits are a nightmare. We get 429s with no Retry-After header, so our backoff is blind. Please add Retry-After. 2 stars."),
    ("F012", "2026-10-22 11:00", "github_issue", "Trellis Works", "python",
     "Feature request: support for OpenTelemetry tracing in the Python SDK. Would love auto-instrumentation of every request with span attributes for endpoint and status code."),
    ("F013", "2026-10-22 11:48", "support_ticket", "Tunewave", "python",
     "Following up on my GitHub issue: POST /v2/payments keeps timing out in eu-west-1 with nextgenlakehouse-python 3.0.1. Our payout run of 120k transactions failed last night. We need a hotfix ASAP. Lena (lena.fischer@tunewave-dev.example)"),
    ("F014", "2026-10-22 13:15", "support_ticket", "CovoitRoute", "node",
     "Bonjour, les webhooks payment.succeeded sont livrés deux fois depuis la version 3.0 du SDK Node. Est-ce un problème connu ? Nous avons dû désactiver les remboursements automatiques."),
    ("F015", "2026-10-22 14:02", "forum", "Pixelboard", "typescript",
     "Is there a way to generate TypeScript types from the OpenAPI spec? The current types are missing nullable fields on Customer, so we keep writing casts."),
    ("F016", "2026-10-22 15:30", "support_ticket", "Orbit Bank", "java",
     "We are an Enterprise customer and our SLA says 99.95% monthly uptime. On Oct 21 your API was degraded for about 3 hours in eu-west-1. How exactly do we claim service credits, and how much are we entitled to?"),
    ("F017", "2026-10-22 16:45", "app_review", "Sketchloom", "python",
     "Best developer experience of any payments API I've used. The CLI command 'nextgenlakehouse listen' for local webhooks is pure magic."),
    ("F018", "2026-10-23 08:20", "forum", "Rayo Express", "python",
     "La migración de 2.x a 3.0 rompió nuestro código: el parámetro 'timeout' ahora está en segundos y no en milisegundos, así que nuestro timeout=30000 se volvió 8 horas. La guía no lo deja claro."),
    ("F019", "2026-10-23 09:40", "github_issue", "EdgeForge", "typescript",
     "Edge runtime support: the TS SDK imports 'crypto' from node, which breaks on EdgeForge Edge Functions. Please switch to WebCrypto so we can verify webhooks at the edge."),
    ("F020", "2026-10-23 10:55", "support_ticket", "Modestoff", "python",
     "POST /v2/refunds hangs, then fails with NMB-408 after 10s in eu-west-1. Python SDK 3.0.1. Same as others are reporting? Contact: tom.becker@modestoff-tech.example, +49 30 5555 0123"),
    ("F021", "2026-10-23 12:10", "support_ticket", "Voltmark", "java",
     "Wir benötigen eine schriftliche Bestätigung, dass NextGenLakehouse DSGVO-konform ist und alle Zahlungsdaten in der EU gespeichert werden. Bitte senden Sie uns den Auftragsverarbeitungsvertrag."),
    ("F022", "2026-10-23 14:30", "forum", "Chatterbox", "go",
     "Love the generics-based client in the Go SDK, pagination iterators feel idiomatic. Small nit: error types should implement errors.Is so we can match NMB codes cleanly."),
    ("F023", "2026-10-23 15:12", "support_ticket", "Lagom Pay", "python",
     "PRODUCTION OUTAGE: all POST requests to /v2/payments have been failing with NMB-408 since 09:12 UTC in eu-west-1. Python SDK 3.0.1. This is costing us thousands of euros per minute. Call me: +46 8 5555 0177 (Priya Nair)."),
    ("F024", "2026-10-23 16:50", "forum", "Sabor Delivery", "node",
     "Os webhooks do SDK Node 3.0 estão sendo entregues em duplicidade. Estamos contornando com chaves de idempotência, mas precisamos de uma correção definitiva."),
]

# Delta table feedback_raw (created_at parsed to a real TIMESTAMP).
fb_df = (
    spark.createDataFrame(
        feedback,
        "feedback_id string, created_at string, channel string, "
        "account_name string, sdk_language string, raw_text string",
    )
    .selectExpr(
        "feedback_id",
        "to_timestamp(created_at) AS created_at",
        "channel",
        "account_name",
        "sdk_language",
        "raw_text",
    )
)
fb_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("feedback_raw")

# Preview: note the leaked emails, phones and keys.
display(spark.table("feedback_raw"))

# COMMAND ----------

# DBTITLE 1,Generate bug tracker and release-note drafts
# Engineering's open bugs. The pipeline links each message to one of them (or none) with ai_decide.
known_issues = [
    ("KI-101", "POST requests time out after 10 seconds in SDK 3.0",
     "In SDK 3.0 the default request timeout was lowered from 30s to 10s and automatic retries were disabled for non-idempotent POST requests. Long-running payment, payout and refund calls fail with NMB-408."),
    ("KI-102", "Duplicate webhook delivery with Node SDK 3.0",
     "Node SDK 3.0 webhook middleware acknowledges events only after the handler finishes. Handlers slower than 5s trigger redelivery, so payment.succeeded arrives twice."),
    ("KI-103", "429 responses missing Retry-After header",
     "Rate-limited responses on API versions before 2026-11-01 do not include the Retry-After header, so clients cannot back off precisely."),
    ("KI-104", "API key rotation returns 401 for old keys",
     "After key rotation, the previous key is valid for a 24h grace period, then returns 401 Unauthorized. 403 means the key is valid but lacks scope."),
]
(
    spark.createDataFrame(known_issues, "issue_id string, title string, description string")
    .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("known_issues")
)

# Release notes written in a hurry (grammar mistakes on purpose) for ai_fix_grammar.
drafts = [
    ("RN-1", "hotfix 3.0.2",
     "we has fixed the timeout issue that effect POST request in python sdk, default timeout are now 30 second again and retry is enable when idempotency key are provide. sorry for the trouble this have caused to you're integrations."),
    ("RN-2", "node 3.0.1",
     "webhook middleware now acknowledge event immediatly and process it async, this prevent the duplicate delivery that many of you was seeing. please upgrade asap its important."),
    ("RN-3", "api 2026-11-01",
     "all 429 response now includes a Retry-After header who tell you how many second to wait before retry. pin you're api version to 2026-11-01 for get this behavior."),
]
(
    spark.createDataFrame(drafts, "draft_id string, release string, draft_text string")
    .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("release_note_drafts")
)

print(f"known_issues {len(known_issues)} | release_note_drafts {len(drafts)}")

# COMMAND ----------

# DBTITLE 1,Step 2 header
# MAGIC %md
# MAGIC ## 2. Structured tables for the predictive functions
# MAGIC
# MAGIC Random data with **planted signal** so the functions have something real to find. A fixed seed makes every run identical.

# COMMAND ----------

# DBTITLE 1,Generate accounts
import numpy as np
import pandas as pd

rng = np.random.default_rng(42)   # fixed seed = reproducible data


def save_table(pdf: pd.DataFrame, name: str):
    """Write a pandas DataFrame to a Delta table, replacing it if it exists."""
    (
        spark.createDataFrame(pdf)
        .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(name)
    )


# ---- accounts: 400 customers for ai_predict_class (churn) and ai_predict_value (ARR) ----
n = 400
plans     = rng.choice(["Startup", "Growth", "Enterprise"], n, p=[0.5, 0.35, 0.15])
region    = rng.choice(["us-east-1", "eu-west-1", "ap-southeast-1"], n, p=[0.45, 0.4, 0.15])
sdk       = rng.choice(["python", "node", "java", "go", "ruby"], n, p=[0.4, 0.25, 0.2, 0.1, 0.05])
sdk_major = rng.choice([2, 3], n, p=[0.45, 0.55])

# Monthly API volume (millions of calls) depends on the plan.
base_calls = {"Startup": 2, "Growth": 25, "Enterprise": 180}
calls_m = np.round([rng.lognormal(np.log(base_calls[p]), 0.5) for p in plans], 2)

# Planted signal: SDK 3 users in eu-west-1 open more tickets, and more negative ones.
sdk3_eu   = (sdk_major == 3) * (region == "eu-west-1")
tickets   = rng.poisson(2 + 3 * sdk3_eu + calls_m / 60)
neg_share = np.clip(rng.normal(0.2 + 0.35 * sdk3_eu, 0.1), 0, 1).round(2)

days_login = rng.integers(0, 60, n)
docs_views = rng.poisson(40 - 0.4 * days_login + 5)

# Churn probability (logistic model): rises with negative tickets, inactivity and
# ticket volume; falls with docs usage and the Enterprise plan.
logit = (
    -2.2
    + 3.5 * neg_share
    + 0.04 * days_login
    + 0.15 * tickets
    - 0.02 * docs_views
    - 0.8 * (plans == "Enterprise")
)
churned = np.where(rng.random(n) < 1 / (1 + np.exp(-logit)), "churned", "renewed")

# ARR = volume x price per million calls (volume discount by plan) + small noise.
price_per_m = np.array([{"Startup": 900, "Growth": 700, "Enterprise": 520}[p] for p in plans])
arr = np.round(price_per_m * calls_m * 12 * rng.normal(1, 0.08, n) + 3000, -2)

accounts = pd.DataFrame(dict(
    account_id=[f"ACC-{i:04d}" for i in range(n)],
    plan=plans,
    region=region,
    primary_sdk=sdk,
    sdk_major_version=sdk_major,
    monthly_api_calls_m=calls_m,
    tickets_90d=tickets,
    negative_ticket_share=neg_share,
    days_since_last_login=days_login,
    docs_views_30d=docs_views,
    renewal_outcome=churned,
    arr_usd=arr,
))
# Leave targets empty for the rows the pipeline must predict.
accounts.loc[accounts.index[-40:], "renewal_outcome"] = None                # 40 upcoming renewals
accounts.loc[accounts.sample(30, random_state=1).index, "arr_usd"] = None   # 30 new accounts

save_table(accounts, "accounts")
print("accounts", len(accounts))

# COMMAND ----------

# DBTITLE 1,Generate daily metrics
# ---- daily_metrics: 200 days for ai_detect_anomalies and ai_forecast ----
# Upward trend + weekly seasonality (weekends at 60%) + noise, plus an incident
# spike on Oct 21-24, the days after the SDK 3.0 launch (2026-10-20).
days   = pd.date_range("2026-04-30", "2026-11-15", freq="D")
t      = np.arange(len(days))
weekly = np.where(days.dayofweek >= 5, 0.6, 1.0)

tickets_d = (60 + 0.15 * t) * weekly + rng.normal(0, 4, len(days))
err       = 0.35 + 0.03 * np.sin(t / 5) + rng.normal(0, 0.02, len(days))

# Multiply tickets and add error-rate points on the 4 incident days.
spike = (days >= "2026-10-21") & (days <= "2026-10-24")
tickets_d[spike] *= np.array([3.4, 2.9, 2.2, 1.6])
err[spike]       += np.array([2.6, 1.9, 1.1, 0.5])

calls_d = (420 + 0.8 * t) * weekly * rng.normal(1, 0.03, len(days))

metrics = pd.DataFrame(dict(
    metric_date=days.date,
    support_tickets=np.round(tickets_d).astype(int),
    api_error_rate_pct=err.round(3),
    api_calls_m=calls_d.round(1),
))
save_table(metrics, "daily_metrics")
print("daily_metrics", len(metrics))

# COMMAND ----------

# DBTITLE 1,Generate API calls
# ---- api_calls: 8,000 calls before vs after the release, for ai_top_drivers ----
# Hidden root cause: after the release, Python + POST + eu-west-1 calls fail ~43%
# of the time (baseline 1%). A smaller secondary effect hits Node on /v2/payments.
m = 8000
calls = pd.DataFrame(dict(
    is_after_release=rng.random(m) < 0.5,
    region=rng.choice(["us-east-1", "eu-west-1", "ap-southeast-1"], m, p=[0.45, 0.4, 0.15]),
    sdk_language=rng.choice(["python", "node", "java", "go"], m, p=[0.4, 0.3, 0.2, 0.1]),
    http_method=rng.choice(["GET", "POST"], m, p=[0.6, 0.4]),
    endpoint=rng.choice(
        ["/v2/payments", "/v2/refunds", "/v2/customers", "/v2/payouts"], m, p=[0.4, 0.15, 0.3, 0.15]
    ),
))
calls["sdk_version"] = np.where(calls.is_after_release, "3.0.1", "2.9.4")

after = calls.is_after_release
python_post_eu = after & (calls.sdk_language == "python") & (calls.http_method == "POST") & (calls.region == "eu-west-1")
node_payments  = after & (calls.sdk_language == "node") & (calls.endpoint == "/v2/payments")
p_err = 0.01 + np.where(python_post_eu, 0.42, 0) + np.where(node_payments, 0.04, 0)

calls["errors"] = (rng.random(m) < p_err).astype(int)
# Failed calls hit the 10 s timeout, so their latency is ~10,000 ms higher.
calls["latency_ms"] = np.round(rng.gamma(2, 60, m) + calls.errors * 10000).astype(int)

save_table(calls, "api_calls")
print("api_calls", len(calls))

# COMMAND ----------

# DBTITLE 1,Step 3 header
# MAGIC %md
# MAGIC ## 3. Documents (PDF)
# MAGIC
# MAGIC Three product docs plus a signed Enterprise SLA. The answers to most support tickets are hidden in them.

# COMMAND ----------

# DBTITLE 1,Generate PDF documents
import shutil

from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle

FORCE_REGENERATE = False   # True = rebuild files that already exist in the volume
styles = getSampleStyleSheet()


def should_skip(path: str) -> bool:
    """Skip files that already exist, unless FORCE_REGENERATE is set."""
    if os.path.exists(path) and not FORCE_REGENERATE:
        print(f"skip (exists): {path}")
        return True
    return False


def make_pdf(fname: str, title: str, blocks: list):
    """Build a simple PDF and copy it to the volume's docs/ folder.

    blocks is a list of (kind, payload):
      "h" = section header, "p" = paragraph,
      "t" = table as a list of rows (the first row is the header).
    The PDF is built in /tmp first because reportlab needs a local file.
    """
    target = f"{VOL}/docs/{fname}"
    if should_skip(target):
        return

    story = [Paragraph(title, styles["Title"]), Spacer(1, 12)]
    for kind, payload in blocks:
        if kind == "h":
            story.append(Paragraph(payload, styles["Heading2"]))
        elif kind == "p":
            story += [Paragraph(payload, styles["BodyText"]), Spacer(1, 6)]
        elif kind == "t":
            table = Table(payload)
            table.setStyle(TableStyle([
                ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
                ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
            ]))
            story += [table, Spacer(1, 10)]

    tmp = f"/tmp/{fname}"
    SimpleDocTemplate(tmp, pagesize=A4).build(story)
    shutil.copy(tmp, target)
    print(f"created: {target}")


make_pdf("nextgenlakehouse_sdk_3_migration_guide.pdf", "NextGenLakehouse SDK 3.0 Migration Guide", [
    ("p", "Version 3.0 of the NextGenLakehouse SDKs (Python, Node, Java, Go) was released on October 20, 2026. This guide lists every breaking change and how to restore 2.x behavior."),
    ("h", "1. Request timeouts"),
    ("p", "The default request timeout was lowered from 30 seconds to 10 seconds. The timeout parameter is now expressed in seconds (it was milliseconds in 2.x). A value of timeout=30000 now means 30,000 seconds. Long-running calls such as POST /v2/payments, POST /v2/payouts and POST /v2/refunds can exceed 10 seconds during peak load and fail with error NMB-408."),
    ("h", "2. Automatic retries"),
    ("p", "Automatic retries are disabled by default for non-idempotent requests (POST) unless an Idempotency-Key header is provided. To restore 2.x behavior, construct the client with NextGenLakehouseClient(timeout=30, max_retries=3) and send an Idempotency-Key on every POST."),
    ("h", "3. Webhooks (Node SDK)"),
    ("p", "Webhook handlers must return a 2xx response within 5 seconds, otherwise NextGenLakehouse redelivers the event. In Node SDK 3.0 the middleware acknowledges only after your handler resolves, so slow handlers receive duplicates of payment.succeeded. Fix: acknowledge immediately with res.sendStatus(200), process the event asynchronously, and deduplicate on event.id. Node SDK 3.0.1 acknowledges automatically."),
    ("h", "4. Webhook signatures"),
    ("p", "Signatures now use the v2 scheme (HMAC-SHA256 over timestamp.payload). Verification fails with NMB-401 if the raw request body is parsed as JSON before verification. Pass the raw Buffer to nextgenlakehouse.webhooks.verify()."),
    ("t", [
        ["Setting",            "2.x default", "3.0 default",   "Restore 2.x"],
        ["timeout",            "30000 ms",    "10 s",          "timeout=30"],
        ["max_retries (POST)", "3",           "0",             "max_retries=3 + Idempotency-Key"],
        ["webhook ack",        "immediate",   "after handler", "upgrade Node to 3.0.1"],
    ]),
])

make_pdf("nextgenlakehouse_rate_limits.pdf", "NextGenLakehouse API Rate Limits", [
    ("p", "Rate limits are enforced per account and per region using a token bucket."),
    ("t", [
        ["Plan",       "Live mode (requests/s)", "Burst"],
        ["Startup",    "100",                    "200"],
        ["Growth",     "250",                    "500"],
        ["Enterprise", "500",                    "1000"],
    ]),
    ("h", "Test mode"),
    ("p", "Test-mode keys (prefix sk_test_) are always limited to 25 requests per second regardless of plan. Load tests against test mode will see HTTP 429 above 25 rps. Contact your account team to enable a dedicated load-testing sandbox."),
    ("h", "Retry-After"),
    ("p", "Starting with API version 2026-11-01, every 429 response includes a Retry-After header in seconds. Accounts pinned to older API versions do not receive it. Upgrade the NextGenLakehouse-Version header to 2026-11-01."),
])

make_pdf("nextgenlakehouse_auth_and_keys.pdf", "Authentication and API Key Rotation", [
    ("p", "NextGenLakehouse authenticates requests with secret keys sent as a Bearer token."),
    ("h", "Rotating keys"),
    ("p", "When you roll a key, the previous key remains valid for a 24-hour grace period. After the grace period it returns 401 Unauthorized. A 403 Forbidden means the key is valid but lacks the scope for that endpoint, for example a restricted key calling POST /v2/payouts."),
    ("h", "Best practices"),
    ("p", "Never paste live keys in GitHub issues or support tickets. Store keys in a secret manager and use restricted keys with the minimum scopes."),
])

# A signed contract: the pipeline extracts its terms and credit-tier table with ai_extract.
make_pdf("nextgenlakehouse_enterprise_sla_orbitbank.pdf", "Enterprise Service Level Agreement", [
    ("p", "This Service Level Agreement (SLA) is entered into between NextGenLakehouse Payments Ltd. and Orbit Bank Ltd. (the Customer), effective January 1, 2026, for an initial term of 24 months. Governing law: Ireland. Customer data is stored exclusively in the EU (eu-west-1)."),
    ("h", "Uptime commitment"),
    ("p", "NextGenLakehouse commits to a Monthly Uptime Percentage of 99.95% for the Payments API. Degraded performance where more than 5% of requests fail counts as downtime."),
    ("t", [
        ["Monthly Uptime",         "Service Credit"],
        ["< 99.95% and >= 99.9%",  "10% of monthly fee"],
        ["< 99.9% and >= 99.0%",   "25% of monthly fee"],
        ["< 99.0%",                "50% of monthly fee"],
    ]),
    ("h", "Claiming credits"),
    ("p", "The Customer must submit a claim to sla-claims@nextgenlakehouse.example within 30 days of the incident, including the incident ID and affected request IDs. Credits are applied to the next invoice within 5 business days. The monthly fee is EUR 48,000."),
])

# COMMAND ----------

# DBTITLE 1,Step 4 header
# MAGIC %md
# MAGIC ## 4. Support call recordings (MP3)
# MAGIC
# MAGIC Two synthetic phone calls (English and Spanish) built with Google Text-to-Speech. Each speaker uses a different accent so `ai_transcribe` can tell them apart. Needs outbound internet; skipped if the files already exist.

# COMMAND ----------

# DBTITLE 1,Generate support call recordings
def make_call(fname: str, turns: list):
    """Synthesize a phone call: one MP3 clip per turn, concatenated into one file.

    turns is a list of (language, tld, text). A different Google TTS domain (tld)
    gives each speaker a different accent.
    """
    target = f"{VOL}/calls/{fname}"
    if should_skip(target):
        return

    from gtts import gTTS   # imported here so the cell only needs gTTS when a file is missing

    parts = []
    for i, (lang, tld, text) in enumerate(turns):
        clip = f"/tmp/{fname}_{i}.mp3"
        gTTS(text, lang=lang, tld=tld).save(clip)
        with open(clip, "rb") as f:
            parts.append(f.read())

    with open(f"/tmp/{fname}", "wb") as f:
        f.write(b"".join(parts))
    shutil.copy(f"/tmp/{fname}", target)
    print(f"created: {target}")


# English call: production outage, workaround, SLA credits and an incident ID.
make_call("call_lagompay_2026-10-23.mp3", [
    ("en", "com",   "Thank you for calling NextGenLakehouse developer support, this is Alex. How can I help you today?"),
    ("en", "co.uk", "Hi Alex, it's Priya from Lagom Pay. Since nine this morning all our POST payments are failing with error N M B four oh eight in E U west one. We're on the Python SDK three point oh point one and it's a full production outage."),
    ("en", "com",   "I'm sorry Priya, that matches a known issue. In version three the default timeout dropped to ten seconds and retries are off for POST requests. Can you set timeout to thirty and max retries to three in the client constructor?"),
    ("en", "co.uk", "Okay, deploying now. Yes, payments are going through again. But we lost about two hours of transactions. Are we entitled to service credits?"),
    ("en", "com",   "Absolutely. I've opened incident I N C two zero four one, and our team will apply credits under your SLA within five business days. A hotfix, version three point oh point two, ships tomorrow."),
])

# Spanish call: 429s caused by a test key, and a commitment with a deadline.
make_call("call_mercadosol_2026-10-22.mp3", [
    ("es", "com.mx", "Hola, gracias por llamar al soporte de NextGenLakehouse, habla Sofía. ¿En qué puedo ayudarle?"),
    ("es", "es",     "Hola Sofía, soy Diego de Mercado Sol. Estamos haciendo pruebas de carga para Black Friday y recibimos errores cuatrocientos veintinueve con solo cuarenta peticiones por segundo. Tenemos plan Enterprise."),
    ("es", "com.mx", "Entiendo. ¿Están usando una clave de prueba que empieza con sk test? Las claves de prueba tienen un límite de veinticinco peticiones por segundo sin importar el plan."),
    ("es", "es",     "Sí, es la clave de prueba. ¿Pueden habilitarnos un entorno de pruebas de carga dedicado antes del quince de noviembre?"),
    ("es", "com.mx", "Claro, lo solicito hoy mismo con su equipo de cuenta y le confirmo por correo en veinticuatro horas."),
])

# COMMAND ----------

# DBTITLE 1,Check generated data
# Sanity check: every table and file the pipeline notebook expects.
expected_rows = {
    "feedback_raw": 24,
    "known_issues": 4,
    "release_note_drafts": 3,
    "accounts": 400,
    "daily_metrics": 200,
    "api_calls": 8000,
}
for table, expected in expected_rows.items():
    actual = spark.table(table).count()
    status = "OK" if actual == expected else "MISMATCH"
    print(f"{status:8} {table:20} {actual:>6} rows (expected {expected})")

for folder, expected in [("docs", 4), ("calls", 2)]:
    files = sorted(os.listdir(f"{VOL}/{folder}"))
    status = "OK" if len(files) >= expected else "MISSING"
    print(f"{status:8} {folder + '/':20} {len(files):>6} files: {', '.join(files)}")

print("\nNext: open 'DevPulse - Databricks AI Functions End to End' and Run all.")