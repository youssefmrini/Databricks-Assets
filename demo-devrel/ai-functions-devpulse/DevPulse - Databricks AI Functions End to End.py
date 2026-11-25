# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# DBTITLE 1,DevPulse intro
# MAGIC %md
# MAGIC # DevPulse: Developer Experience Intelligence with Databricks AI Functions
# MAGIC
# MAGIC *NextGenLakehouse* is a (fictional) payments API platform. It just shipped **SDK 3.0** and developer feedback is pouring in from GitHub issues, forums, support tickets, app reviews and phone calls, in 5 languages and full of secrets and PII. The docs live in PDFs and the metrics live in tables.
# MAGIC
# MAGIC **The mission.** Turn that noise into an **Action Board** with nothing more than **SQL**: no model training, no prompt plumbing, no extra infrastructure.
# MAGIC
# MAGIC | Act | Question | AI Functions |
# MAGIC | --- | --- | --- |
# MAGIC | 1. Listen | What are developers telling us, safely and in one language? | `ai_mask`, `ai_translate`, `ai_analyze_sentiment`, `ai_classify`, `ai_extract`, `ai_summarize`, `ai_decide` |
# MAGIC | 2. Doc analysis | What do our docs and support calls say? | `ai_parse_document`, `ai_prep_search`, `ai_similarity` + `ai_gen`, `ai_transcribe` |
# MAGIC | 3. Diagnose | Who is affected, who might churn, what broke, why, and what comes next? | Customers: `ai_enrich`, `ai_predict_class`, `ai_predict_value`. Metrics: `ai_detect_anomalies`, `ai_top_drivers`, `ai_forecast` |
# MAGIC | 4. Act | Reply, document, report | `ai_fix_grammar`, `ai_gen`, `ai_translate`, `ai_extract` |
# MAGIC
# MAGIC All data is synthetic and created by the companion notebook `devpulse_01_data_generator`. Company names are fictional too.
# MAGIC
# MAGIC ### Project layout
# MAGIC
# MAGIC | Notebook | Role |
# MAGIC | --- | --- |
# MAGIC | `devpulse_00_config` | Widgets, Unity Catalog setup and the `run_on_warehouse` helper. Loaded with `%run` by the two others |
# MAGIC | `devpulse_01_data_generator` | Creates the fake tables, PDFs and MP3s. Run it **once** first |
# MAGIC
# MAGIC
# MAGIC ### Prerequisites
# MAGIC
# MAGIC | Requirement | Why |
# MAGIC | --- | --- |
# MAGIC | `USE CATALOG` + `CREATE SCHEMA` on the target catalog | The config notebook creates its own schema and volume |
# MAGIC | **Serverless** notebook compute | Runs the notebook and the row-level AI Functions |
# MAGIC | A **Pro or Serverless SQL warehouse** | The predictive functions (`ai_forecast`, `ai_detect_anomalies`, `ai_top_drivers`, `ai_predict_*`) run there through the Statement Execution API |
# MAGIC | Previews enabled where required | Some functions (for example `ai_transcribe`, `ai_enrich`, the predictive functions)  Ask a workspace admin to enable them on the **Previews** page |
# MAGIC | Outbound internet from serverless (data generator only) | `%pip install` (reportlab, gTTS) and Google Text-to-Speech for the 2 audio files |
# MAGIC
# MAGIC ### How to run
# MAGIC
# MAGIC 1. Keep the three notebooks in the **same folder** and attach them to **Serverless** compute.
# MAGIC 2. Open `devpulse_01_data_generator`, fill in the widgets (`catalog`, `schema`, `warehouse_id` empty = auto) and **Run all** (1 to 2 minutes).
# MAGIC 3. Open this notebook, use the **same widget values** and **Run all** (about 10 minutes; `ai_enrich` web search is the slowest step).
# MAGIC 4. When you are done, run the **Cleanup** cell to drop the demo schema.
# MAGIC
# MAGIC

# COMMAND ----------

# DBTITLE 1,Load shared configuration
# MAGIC %run ./devpulse_00_config $catalog="youssef_demo" $schema="ai_functions_devrel"

# COMMAND ----------

# DBTITLE 1,Act 1 header
# MAGIC %md
# MAGIC ## Act 1. Listen: turn developer messages into structured, safe, triaged insight
# MAGIC
# MAGIC
# MAGIC
# MAGIC Each step is **one SQL function call** and persists a table, so the cells chain into a real pipeline (`feedback_raw` -> `feedback_masked` -> `feedback_en` -> `fb_sentiment` -> `fb_topics` -> `fb_facts` -> `fb_enriched` -> `fb_triage`). 

# COMMAND ----------

# DBTITLE 1,About ai_mask
# MAGIC %md
# MAGIC ### `ai_mask`
# MAGIC Masks personal data and secrets in text, so it can be stored, shared and sent to other AI functions safely.
# MAGIC
# MAGIC ```sql
# MAGIC ai_mask(content, labels)
# MAGIC ```
# MAGIC
# MAGIC | Parameter | Type | Description |
# MAGIC | --- | --- | --- |
# MAGIC | `content` | `STRING` | The text to mask. |
# MAGIC | `labels` | `ARRAY<STRING>` | The entity types to mask, for example `array('email', 'phone number', 'person name', 'credential')`. |
# MAGIC
# MAGIC **Returns** a `STRING` in which each detected entity is replaced with `[MASKED]`. Version 2.0 (`options => map('version', '2.0')`) uses named classifiers such as `EMAIL_ADDRESS` or `SECRET` instead of free-text labels.
# MAGIC
# MAGIC [ai_mask documentation](https://docs.databricks.com/aws/en/sql/language-manual/functions/ai_mask/)

# COMMAND ----------

# DBTITLE 1,1.1 ai_mask: redact secrets and PII
# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE feedback_masked AS
# MAGIC SELECT *,
# MAGIC        ai_mask(raw_text, array('email', 'phone number', 'person name', 'credential')) AS masked_text
# MAGIC FROM feedback_raw;
# MAGIC
# MAGIC SELECT feedback_id, account_name, raw_text, masked_text
# MAGIC FROM feedback_masked
# MAGIC WHERE raw_text RLIKE '@|sk_live|whsec|\\+[0-9]'
# MAGIC ORDER BY feedback_id limit 5;

# COMMAND ----------

# DBTITLE 1,1.1 explanation
# MAGIC %md
# MAGIC **What happened.** <br> `ai_mask` read each raw message and replaced emails, phone numbers, person names and **credentials** (live API keys `sk_live_...`, webhook secrets `whsec_...`) with `[MASKED]`, in five languages, without a single regex. It keeps what engineers need: error codes such as `NMB-408`, SDK versions and endpoints survive. It leans toward over-masking: on some runs it also masks the word "API-Key" in Kronwerk's German "API-Key-Rotation". Label wording matters (`'credential'` worked better than `'api key'`), so test your labels on a sample before rolling them out.
# MAGIC
# MAGIC **Why developers care.** <br> A developer pastes a live key into a GitHub issue; support copies it into Jira, Slack and the RAG index. Every copy is a leak and a compliance finding. Masking in the first transformation means every downstream table, AI call, dashboard and Genie space only ever sees safe text.
# MAGIC
# MAGIC In production, keep the raw table in a restricted schema (Unity Catalog grants) for incident response and mask into the table everyone else reads. Keep a deterministic check for your own key prefixes (`sk_live_`, `whsec_`) next to it, because an AI-based mask is probabilistic. And a leaked key still has to be rotated: masking stops the spread, it does not undo the exposure.

# COMMAND ----------

# DBTITLE 1,About ai_translate
# MAGIC %md
# MAGIC ### `ai_translate`
# MAGIC Translates text into a target language.
# MAGIC
# MAGIC ```sql
# MAGIC ai_translate(content, to_lang)
# MAGIC ```
# MAGIC
# MAGIC | Parameter | Type | Description |
# MAGIC | --- | --- | --- |
# MAGIC | `content` | `STRING` | The text to translate. |
# MAGIC | `to_lang` | `STRING` | The target language code, for example `'en'`, `'es'` or `'de'`. |
# MAGIC
# MAGIC **Returns** the translated `STRING`. A `NULL` input returns `NULL`.
# MAGIC
# MAGIC [ai_translate documentation](https://docs.databricks.com/aws/en/sql/language-manual/functions/ai_translate/)

# COMMAND ----------

# DBTITLE 1,1.2 ai_translate: one language for the whole team
# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE feedback_en AS
# MAGIC SELECT *, ai_translate(masked_text, 'en') AS text_en
# MAGIC FROM feedback_masked;
# MAGIC
# MAGIC -- Show the non-English messages next to their translation.
# MAGIC SELECT feedback_id, account_name, masked_text AS original, text_en
# MAGIC FROM feedback_en
# MAGIC WHERE feedback_id IN ('F002', 'F003', 'F005', 'F006', 'F010', 'F018', 'F021', 'F024')
# MAGIC ORDER BY feedback_id;

# COMMAND ----------

# DBTITLE 1,1.2 explanation
# MAGIC %md
# MAGIC **What happened.** <br>`ai_translate(text, 'en')` detected French, Spanish, German, Italian and Portuguese on its own and produced English, while keeping technical tokens such as `NMB-408`, `payment.succeeded` and `timeout=30000` intact. English messages come back unchanged, so one call covers the whole table.
# MAGIC
# MAGIC **Why developers care.** <br> Without it, a Portuguese ticket waits until someone who reads Portuguese is online, or gets pasted into a consumer translator together with customer data. Translating once, after masking, gives every later step (classification, extraction, search, dashboards) one language and one column.
# MAGIC
# MAGIC Keep the original next to the translation: translations can soften the tone or lose nuance, and the original is what you quote back to the customer. In Act 4 we translate the **reply** back, so each developer gets an answer in the language they wrote in.

# COMMAND ----------

# DBTITLE 1,About ai_analyze_sentiment
# MAGIC %md
# MAGIC ### `ai_analyze_sentiment`
# MAGIC Detects the overall sentiment of a text, with no prompt or configuration.
# MAGIC
# MAGIC ```sql
# MAGIC ai_analyze_sentiment(content)
# MAGIC ```
# MAGIC
# MAGIC | Parameter | Type | Description |
# MAGIC | --- | --- | --- |
# MAGIC | `content` | `STRING` | The text to analyze. |
# MAGIC
# MAGIC **Returns** a `STRING`: `positive`, `negative`, `neutral` or `mixed`, or `NULL` if the sentiment cannot be detected.
# MAGIC
# MAGIC [ai_analyze_sentiment documentation](https://docs.databricks.com/aws/en/sql/language-manual/functions/ai_analyze_sentiment/)

# COMMAND ----------

# DBTITLE 1,1.3 ai_analyze_sentiment: how do developers feel
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE fb_sentiment AS
# MAGIC SELECT *, ai_analyze_sentiment(text_en) AS sentiment
# MAGIC FROM feedback_en;
# MAGIC
# MAGIC -- Sentiment breakdown: how many messages per label, and from which accounts.
# MAGIC SELECT sentiment,
# MAGIC        count(*) AS messages,
# MAGIC        array_join(collect_list(account_name), ', ') AS accounts
# MAGIC FROM fb_sentiment
# MAGIC GROUP BY sentiment
# MAGIC ORDER BY messages DESC;

# COMMAND ----------

# DBTITLE 1,1.3 explanation
# MAGIC %md
# MAGIC **What happened.**  <br> `ai_analyze_sentiment` labeled every message `positive`, `negative`, `neutral` or `mixed`, with no prompt and no configuration. Launch week is clearly negative: **17 of 24** messages. Graphhound's "40% lower latency, but the changelog hid a breaking change" lands in **mixed**, which a keyword-based model would likely score as positive.
# MAGIC
# MAGIC **Why developers care.** <br> A release-health signal from data you already have. Group it by SDK, channel or day and compare it with the previous release to spot a bad rollout before the ticket count shows it.
# MAGIC
# MAGIC Treat it as a trend, not a verdict on one message: four labels are coarse, so use `ai_classify` or `ai_decide` when you need to act on a single ticket. `mixed` is the most useful bucket for DevRel: developers who like the product but hit a rough edge are the easiest to win back.

# COMMAND ----------

# DBTITLE 1,About ai_classify
# MAGIC %md
# MAGIC ### `ai_classify`
# MAGIC Assigns one or more of your own labels to a text or a parsed document.
# MAGIC
# MAGIC ```sql
# MAGIC ai_classify(content, labels [, options])
# MAGIC ```
# MAGIC
# MAGIC | Parameter | Type | Description |
# MAGIC | --- | --- | --- |
# MAGIC | `content` | `STRING` or `VARIANT` | Raw text, or the output of another AI function such as `ai_parse_document`. |
# MAGIC | `labels` | `STRING` | A JSON array of label names, or a JSON object mapping each label to a description (2 to 500 labels). |
# MAGIC | `options` | `MAP<STRING, STRING>` | Optional. `version` (use `'2.1'`), `instructions` (domain context), `multilabel`, `enableConfidenceScores`, `enableRationales`. |
# MAGIC
# MAGIC **Returns** a `VARIANT`: `response` is an array of `{value, confidence_score, rationale}`, plus `metadata` and `error_message`.
# MAGIC
# MAGIC [ai_classify documentation](https://docs.databricks.com/aws/en/sql/language-manual/functions/ai_classify/)

# COMMAND ----------

# DBTITLE 1,1.4 ai_classify: multi-label topics with confidence and rationale
# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE fb_topics AS
# MAGIC SELECT *,
# MAGIC   ai_classify(
# MAGIC     text_en,
# MAGIC     -- Our taxonomy: label -> what it means.
# MAGIC     '{
# MAGIC       "timeouts_retries": "Requests timing out, NMB-408, timeout or retry configuration",
# MAGIC       "webhooks": "Webhook delivery, duplicates, signatures, verification",
# MAGIC       "rate_limits": "HTTP 429, throttling, Retry-After, requests per second",
# MAGIC       "auth_keys": "API keys, rotation, 401/403, authentication",
# MAGIC       "docs_gap": "Missing, unclear or wrong documentation, changelog or migration guide",
# MAGIC       "feature_request": "Asks for a new capability or SDK improvement",
# MAGIC       "compliance_legal": "GDPR, data residency, contracts, DPA",
# MAGIC       "sla_billing": "Uptime commitments, service credits, invoices",
# MAGIC       "praise": "Compliments about the product or developer experience"
# MAGIC     }',
# MAGIC     MAP('version', '2.1',                    -- pin the function version for reproducible output
# MAGIC         'multilabel', 'true',                -- a message can have several topics
# MAGIC         'enableConfidenceScores', 'true',    -- 0..1 confidence per label
# MAGIC         'enableRationales', 'true',          -- short justification grounded in the text
# MAGIC         'instructions', 'Inputs are developer feedback about the NextGenLakehouse payments API and SDKs, posted in the week after the SDK 3.0 release.')  -- domain context
# MAGIC   ) AS topics
# MAGIC FROM fb_sentiment;
# MAGIC
# MAGIC -- One row per (message, topic): explode the response array with variant_explode.
# MAGIC SELECT feedback_id, account_name,
# MAGIC        t.value:value::STRING AS topic,
# MAGIC        round(t.value:confidence_score::DOUBLE, 2) AS confidence,
# MAGIC        t.value:rationale::STRING AS rationale
# MAGIC FROM fb_topics, LATERAL variant_explode(topics:response) AS t
# MAGIC ORDER BY feedback_id, confidence DESC;

# COMMAND ----------

# DBTITLE 1,1.4 explanation
# MAGIC %md
# MAGIC **What happened.**   <br>`ai_classify` v2.1 applied our own nine-topic taxonomy, written as labels **with descriptions**. With `multilabel`, a message can get several topics (Kronwerk is both `auth_keys` and `docs_gap`), and each label comes with a **confidence score** and a short **rationale** taken from the text.
# MAGIC
# MAGIC **Why developers care.**  <br> The taxonomy lives in the query as JSON, so product and DevRel can change it in a pull request: add a topic, rerun, done. There is no labeled dataset to build and no model to retrain when the product changes.
# MAGIC
# MAGIC Use the confidence to split the work: route the confident labels automatically and send the low ones (Kronwerk's `docs_gap` scored 0.6) to a human queue, with the rationale so the reviewer can agree or correct in seconds. Pin `version` so a function upgrade does not silently change your dashboards.

# COMMAND ----------

# DBTITLE 1,About ai_extract
# MAGIC %md
# MAGIC ### `ai_extract`
# MAGIC Extracts the fields you define in a schema from a text or a parsed document, as typed values.
# MAGIC
# MAGIC ```sql
# MAGIC ai_extract(content, schema [, options])
# MAGIC ```
# MAGIC
# MAGIC | Parameter | Type | Description |
# MAGIC | --- | --- | --- |
# MAGIC | `content` | `STRING` or `VARIANT` | Raw text, or the output of `ai_parse_document`. |
# MAGIC | `schema` | `STRING` | A JSON array of field names, or a JSON object with types (`string`, `integer`, `number`, `boolean`, `enum`, `object`, `array`) and optional descriptions. |
# MAGIC | `options` | `MAP<STRING, STRING>` | Optional. `version` (use `'2.1'`), `instructions`, `mode` (`'precision'` for long or complex documents), `enableCitations`, `enableConfidenceScores`. |
# MAGIC
# MAGIC **Returns** a `VARIANT`: `response` holds one object per field with its `value` (and confidence or citations when enabled), plus `metadata` and `error_message`.
# MAGIC
# MAGIC [ai_extract documentation](https://docs.databricks.com/aws/en/sql/language-manual/functions/ai_extract/)

# COMMAND ----------

# DBTITLE 1,1.5 ai_extract: typed fields out of free text
# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE fb_facts AS
# MAGIC SELECT *,
# MAGIC   ai_extract(
# MAGIC     masked_text,
# MAGIC     '{
# MAGIC       "original_language": {"type": "enum", "labels": ["English", "French", "Spanish", "German", "Italian", "Portuguese"]},
# MAGIC       "sdk_version":       {"type": "string",  "description": "NextGenLakehouse SDK version mentioned, e.g. 3.0.1"},
# MAGIC       "error_code":        {"type": "string",  "description": "NextGenLakehouse error code such as NMB-408, or HTTP status like 429"},
# MAGIC       "endpoint":          {"type": "string",  "description": "API endpoint path such as /v2/payments"},
# MAGIC       "http_method":       {"type": "enum",    "labels": ["GET", "POST", "PUT", "DELETE"]},
# MAGIC       "region":            {"type": "string",  "description": "Cloud region such as eu-west-1"},
# MAGIC       "business_impact":   {"type": "enum",    "labels": ["production_outage", "revenue_loss", "blocked_testing", "developer_friction", "none"], "description": "production_outage = live customer traffic failing; blocked_testing = only pre-production or load tests blocked"},
# MAGIC       "workaround":        {"type": "string",  "description": "Workaround the developer found, if any"},
# MAGIC       "asks":              {"type": "array",   "description": "Concrete product, fix or documentation requests made to NextGenLakehouse. Exclude contact details and greetings.", "items": {"type": "string"}}
# MAGIC     }',
# MAGIC     MAP('version', '2.1',
# MAGIC         'enableConfidenceScores', 'true',   -- each field returns {value, confidence_score}
# MAGIC         'instructions', 'Developer feedback about the NextGenLakehouse payments API. Leave a field null if it is not mentioned.')  -- avoid invented values
# MAGIC   ) AS facts
# MAGIC FROM fb_topics;
# MAGIC
# MAGIC -- Flatten the VARIANT into columns: facts:response.<field>.value
# MAGIC SELECT feedback_id, account_name,
# MAGIC        facts:response.original_language.value::STRING AS language,
# MAGIC        facts:response.sdk_version.value::STRING       AS sdk_version,
# MAGIC        facts:response.error_code.value::STRING        AS error_code,
# MAGIC        facts:response.http_method.value::STRING       AS method,
# MAGIC        facts:response.endpoint.value::STRING          AS endpoint,
# MAGIC        facts:response.region.value::STRING            AS region,
# MAGIC        facts:response.business_impact.value::STRING   AS business_impact,
# MAGIC        round(facts:response.business_impact.confidence_score::DOUBLE, 2) AS impact_conf,
# MAGIC        facts:response.workaround.value::STRING        AS workaround,
# MAGIC        facts:response.asks                            AS asks
# MAGIC FROM fb_facts
# MAGIC ORDER BY feedback_id;

# COMMAND ----------

# DBTITLE 1,1.5 explanation
# MAGIC %md
# MAGIC **What happened.** <br> `ai_extract` v2.1 read the original (masked) text in any language and filled a **typed schema**: enums (`http_method`, `business_impact`), strings (`sdk_version`, `endpoint`, `region`) and an array of asks, each with a confidence score. Fields a message does not mention come back `null` instead of being made up, because the instructions ask for it.
# MAGIC
# MAGIC **Why developers care.** <br>
# MAGIC Free text becomes columns you can `GROUP BY`: how many `production_outage` reports hit `POST /v2/payments` in `eu-west-1` on SDK `3.0.1`? That is the reproduction data engineering usually asks for in three follow-up emails. Enums keep the values clean for dashboards (no "Prod outage" vs "production down").
# MAGIC
# MAGIC Use `confidence_score` to decide what to trust automatically, and treat the schema like code: it is JSON in the cell, reviewed in a pull request, and a new field is one line.

# COMMAND ----------

# DBTITLE 1,About ai_summarize
# MAGIC %md
# MAGIC ### `ai_summarize`
# MAGIC Summarizes a text in a target number of words.
# MAGIC
# MAGIC ```sql
# MAGIC ai_summarize(content [, max_words])
# MAGIC ```
# MAGIC
# MAGIC | Parameter | Type | Description |
# MAGIC | --- | --- | --- |
# MAGIC | `content` | `STRING` | The text to summarize. |
# MAGIC | `max_words` | `INT` | Optional. Best-effort target length in words. Default 50; `0` means no limit. |
# MAGIC
# MAGIC **Returns** the summary as a `STRING`. A `NULL` input returns `NULL`.
# MAGIC
# MAGIC [ai_summarize documentation](https://docs.databricks.com/aws/en/sql/language-manual/functions/ai_summarize/)

# COMMAND ----------

# DBTITLE 1,1.6 ai_summarize: TL;DR for long threads
# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE fb_enriched AS
# MAGIC SELECT *,
# MAGIC        CASE WHEN length(text_en) > 300 THEN ai_summarize(text_en, 40) ELSE text_en END AS tl_dr
# MAGIC FROM fb_facts;
# MAGIC
# MAGIC -- Compare the long originals with their summary.
# MAGIC SELECT feedback_id, account_name, length(text_en) AS chars, text_en, tl_dr
# MAGIC FROM fb_enriched
# MAGIC WHERE length(text_en) > 300;

# COMMAND ----------

# DBTITLE 1,1.6 explanation
# MAGIC %md
# MAGIC **What happened.** <br>`ai_summarize(text, 40)` turned an 8-post forum thread into one sentence: payouts fail with timeouts after 3.0, and downgrading or raising the timeout fixes it. Short messages are kept as they are (the `CASE` only summarizes text over 300 characters), which saves calls and cost.
# MAGIC
# MAGIC
# MAGIC **Why developers care.** <br> The on-call engineer reads one line instead of a long thread, and the same line feeds the triage queue, the Gold board and the weekly brief. The same function shortens call transcripts (2.6), Slack exports and PR discussions.
# MAGIC
# MAGIC Summaries drop detail, so always link back to the full text, and only summarize what is long: running it on every short message adds cost and latency for nothing.

# COMMAND ----------

# DBTITLE 1,About ai_decide
# MAGIC %md
# MAGIC ### `ai_decide`
# MAGIC Answers several questions about one input in a single call, as structured, scored answers.
# MAGIC
# MAGIC ```sql
# MAGIC ai_decide(state, questions [, options])
# MAGIC ```
# MAGIC
# MAGIC | Parameter | Type | Description |
# MAGIC | --- | --- | --- |
# MAGIC | `state` | `STRING` or `VARIANT` | The content and context to evaluate: plain text, a JSON string, or the output of another AI function. |
# MAGIC | `questions` | `STRING` | A constant JSON object: question ID mapped to `{type, instructions, criteria}`. Types: `choice` (pick one option), `noul` (probability that a statement is true), `score` (position on an ordered scale). |
# MAGIC | `options` | `MAP<STRING, STRING>` | Optional. `version` (`'1.0'`). |
# MAGIC
# MAGIC **Returns** a `VARIANT`: `response.answers.<question_id>` holds a `choice` with `confidence`, a `probability`, or a `score` with `confidence`, plus `metadata` and `error_message`.
# MAGIC
# MAGIC [ai_decide documentation](https://docs.databricks.com/aws/en/sql/language-manual/functions/ai_decide/)

# COMMAND ----------

# DBTITLE 1,1.7 ai_decide: known bug, triage, outage and urgency
# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE fb_triage AS
# MAGIC WITH decided AS (
# MAGIC   SELECT f.*,
# MAGIC     ai_decide(
# MAGIC       -- Give the model the message plus context extracted earlier, as one JSON string.
# MAGIC       to_json(named_struct(
# MAGIC         'feedback', f.text_en,
# MAGIC         'channel', f.channel,
# MAGIC         'business_impact', f.facts:response.business_impact.value::STRING)),
# MAGIC       '{
# MAGIC   "known_issue": {
# MAGIC     "type": "choice",
# MAGIC     "instructions": "Which known open bug does this message report? Pick none if it reports something else.",
# MAGIC     "criteria": {
# MAGIC       "KI-101": "POST requests time out after 10 seconds in SDK 3.0: default timeout lowered from 30s to 10s, retries disabled for POST, NMB-408 on payments, payouts and refunds",
# MAGIC       "KI-102": "Duplicate webhook delivery with Node SDK 3.0: slow handlers trigger redelivery, so payment.succeeded arrives twice",
# MAGIC       "KI-103": "429 rate-limited responses missing the Retry-After header",
# MAGIC       "KI-104": "API key rotation: old keys return 401 after the 24h grace period",
# MAGIC       "none": "Praise, feature request, legal or commercial question, or a different bug than the ones above"
# MAGIC     }
# MAGIC   },
# MAGIC   "team": {
# MAGIC     "type": "choice",
# MAGIC     "instructions": "Which NextGenLakehouse team should own this feedback?",
# MAGIC     "criteria": {
# MAGIC       "sdk_engineering": "Bugs or regressions in an SDK",
# MAGIC       "api_platform": "Server-side API behavior, rate limits, uptime",
# MAGIC       "docs": "Documentation or changelog gaps without a code bug",
# MAGIC       "security": "Authentication, keys, signatures",
# MAGIC       "legal_compliance": "GDPR, contracts, data residency",
# MAGIC       "account_management": "SLA credits, commercial questions",
# MAGIC       "devrel_community": "Praise or feature ideas to engage with"
# MAGIC     }
# MAGIC   },
# MAGIC   "outage": {
# MAGIC     "type": "noul",
# MAGIC     "instructions": "Is this customer in a live PRODUCTION OUTAGE right now: the customer explicitly says production is down or that all or most live requests are failing at this moment, with no workaround?",
# MAGIC     "criteria": {
# MAGIC       "true": "Explicit live production outage: production is down, all or most live payments failing now, customers cannot pay right now",
# MAGIC       "false": "Anything else: a bug or regression with a workaround, intermittent or partial failures, failing tests or staging, duplicates handled by idempotency, documentation questions, follow-ups, complaints, or past incidents"
# MAGIC     }
# MAGIC   },
# MAGIC   "urgency": {
# MAGIC     "type": "score",
# MAGIC     "instructions": "How urgent is a response?",
# MAGIC     "criteria": [
# MAGIC       "Can wait for the next sprint",
# MAGIC       "Reply this week",
# MAGIC       "Reply today",
# MAGIC       "Page on-call now"
# MAGIC     ]
# MAGIC   },
# MAGIC   "trust_risk": {
# MAGIC     "type": "noul",
# MAGIC     "instructions": "Is the developer losing trust in NextGenLakehouse or hinting they might switch provider?"
# MAGIC   }
# MAGIC }',
# MAGIC       map('version', '1.0')
# MAGIC     ) AS decision
# MAGIC   FROM fb_enriched f
# MAGIC )
# MAGIC
# MAGIC SELECT d.*,
# MAGIC        d.decision:response.answers.known_issue.choice      AS best_issue,
# MAGIC        CAST(k.title AS VARIANT)                            AS best_issue_title,
# MAGIC        d.decision:response.answers.known_issue.confidence  AS issue_confidence
# MAGIC FROM decided d
# MAGIC LEFT JOIN known_issues k ON k.issue_id = d.decision:response.answers.known_issue.choice::STRING;
# MAGIC
# MAGIC -- Triage queue: the key columns, sorted by urgency.
# MAGIC SELECT feedback_id, account_name, tl_dr,
# MAGIC        best_issue::STRING                                                AS best_issue,
# MAGIC        round(issue_confidence::DOUBLE, 2)                                 AS issue_confidence,
# MAGIC        decision:response.answers.team.choice::STRING                      AS owner_team,
# MAGIC        round(decision:response.answers.urgency.score::DOUBLE, 2)         AS urgency,
# MAGIC        round(decision:response.answers.outage.probability::DOUBLE, 2)    AS p_outage,
# MAGIC        round(decision:response.answers.trust_risk.probability::DOUBLE, 2) AS p_trust_risk
# MAGIC FROM fb_triage
# MAGIC ORDER BY urgency DESC;

# COMMAND ----------

# DBTITLE 1,1.7 explanation
# MAGIC %md
# MAGIC **What happened.** <br> One `ai_decide` call answered **five questions** per message: which **known bug** it reports (or `none`), which **team** owns it, whether it is a **live production outage**, whether there is a **trust risk**, and how **urgent** it is (0 to 3). Choices return a confidence, yes/no questions return a probability, and the score is probability-weighted, so 2.4 means "between reply today and page on-call".
# MAGIC
# MAGIC **Bug linking without a threshold.** `none` is a real answer, so praise and feature requests are never forced onto the closest bug. Mercado Sol's 429 came back as KI-103 with only **0.63** confidence; downstream we count a link only at `issue_confidence >= 0.7`.
# MAGIC
# MAGIC **Write precise criteria.** A vague "is this an outage?" scored most bug reports above 0.8, including Mercado Sol's failing *load tests*. With explicit criteria Mercado Sol drops to 0.26 while Lagom Pay's real outage stays at 0.99. Scores still move between runs, so calibrate paging thresholds on a labeled sample.
# MAGIC
# MAGIC **Why developers care.** <br> Triage becomes numbers you can code against: page on-call when `p_outage > 0.9`, sort the queue by urgency, alert the account owner when trust risk spikes on a strategic customer. One call replaces a routing classifier, a duplicate-bug matcher and a hand-written priority rule, and the questions are versioned in the query.
# MAGIC
# MAGIC Before it pages anyone, backtest the thresholds on a few weeks of labeled tickets and keep a human in the loop for the first releases. Keep `error_message` so a failed call is never mistaken for a low score.

# COMMAND ----------

# DBTITLE 1,Act 2 header
# MAGIC %md
# MAGIC ## Act 2. Doc analysis: PDFs, grounded answers and phone calls, all in SQL
# MAGIC
# MAGIC The answers to most tickets already exist in the docs. The richest feedback is in support calls. Let's make both queryable.

# COMMAND ----------

# DBTITLE 1,About ai_parse_document
# MAGIC %md
# MAGIC ### `ai_parse_document`
# MAGIC Parses binary documents such as PDFs into structured, typed elements.
# MAGIC
# MAGIC ```sql
# MAGIC ai_parse_document(content [, options])
# MAGIC ```
# MAGIC
# MAGIC | Parameter | Type | Description |
# MAGIC | --- | --- | --- |
# MAGIC | `content` | `BINARY` | The file bytes, for example the `content` column of `read_files(..., format => 'binaryFile')`. |
# MAGIC | `options` | `MAP<STRING, STRING>` | Optional. `version` (`'2.0'`), `pageRange` (for example `'1,3,5-10'`), `imageOutputPath` (save page images to a volume), `descriptionElementTypes` (AI descriptions of figures). |
# MAGIC
# MAGIC **Returns** a `VARIANT` with the document's elements (titles, section headers, text, tables as HTML, figures) and page metadata. Documents over 500 pages need `pageRange`.
# MAGIC
# MAGIC [ai_parse_document documentation](https://docs.databricks.com/aws/en/sql/language-manual/functions/ai_parse_document/)

# COMMAND ----------

# DBTITLE 1,2.1 ai_parse_document: PDFs to structured elements
# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE docs_parsed AS
# MAGIC SELECT _metadata.file_name AS file_name,
# MAGIC        path,
# MAGIC        ai_parse_document(content, MAP('version', '2.0')) AS parsed
# MAGIC FROM read_files(devpulse_volume || '/docs/', format => 'binaryFile');
# MAGIC
# MAGIC SELECT d.file_name,
# MAGIC        e.value:type::STRING    AS element_type,
# MAGIC        e.value:content::STRING AS content
# MAGIC FROM docs_parsed d, LATERAL variant_explode(d.parsed:document:elements) AS e
# MAGIC WHERE d.file_name LIKE '%sla%'
# MAGIC ORDER BY e.pos;

# COMMAND ----------

# DBTITLE 1,2.1 explanation
# MAGIC %md
# MAGIC **What happened.** `ai_parse_document` turned the binary PDFs in the volume into a `VARIANT` with typed **elements**: `title`, `section_header`, `text` and `table`. Tables come back as HTML, so rows and columns survive, which 2.2 relies on to read the SLA credit tiers.
# MAGIC
# MAGIC **Why developers care.** Most teams maintain a PDF library, an OCR service and layout rules that break with every new template. Here it is one function, and the parsed `VARIANT` feeds `ai_classify`, `ai_extract` and `ai_prep_search` directly.
# MAGIC
# MAGIC In production, parse once and store the result (parsing is the expensive step), process only new files with Auto Loader or a file-arrival trigger, and use `pageRange` to skip appendices in long documents.

# COMMAND ----------

# DBTITLE 1,2.2 ai_classify + ai_extract on documents: SLA terms and credits
# MAGIC %sql
# MAGIC -- 2.2a ai_classify works directly on the parsed VARIANT: label each document by type.
# MAGIC -- :response[0].value takes the top label (single-label mode).
# MAGIC CREATE OR REPLACE TABLE docs_catalog AS
# MAGIC SELECT file_name,
# MAGIC   ai_classify(parsed,
# MAGIC     '{"migration_guide": "Breaking changes and upgrade steps between SDK versions",
# MAGIC       "api_reference": "Limits, parameters and behavior of the API",
# MAGIC       "security_guide": "Authentication, keys and security practices",
# MAGIC       "legal_contract": "Signed agreement with commercial or legal terms"}',
# MAGIC     MAP('version', '2.1')):response[0].value::STRING AS doc_type,
# MAGIC   parsed
# MAGIC FROM docs_parsed;
# MAGIC
# MAGIC -- 2.2b ai_extract on the contract only: typed terms, including the credit-tier TABLE
# MAGIC -- extracted as an array of objects. Citations point back to the source text.
# MAGIC CREATE OR REPLACE TABLE sla_terms AS
# MAGIC SELECT file_name,
# MAGIC   ai_extract(parsed,
# MAGIC     '{
# MAGIC       "customer_name":          {"type": "string"},
# MAGIC       "effective_date":         {"type": "string", "description": "YYYY-MM-DD"},
# MAGIC       "term_months":            {"type": "integer"},
# MAGIC       "uptime_commitment_pct":  {"type": "number"},
# MAGIC       "monthly_fee_eur":        {"type": "number"},
# MAGIC       "claim_window_days":      {"type": "integer"},
# MAGIC       "claim_email":            {"type": "string"},
# MAGIC       "governing_law":          {"type": "string"},
# MAGIC       "credit_tiers": {"type": "array", "items": {"type": "object", "properties": {
# MAGIC           "min_uptime_pct": {"type": "number", "description": "Lower bound of the uptime band, inclusive. Use 0 for the lowest band."},
# MAGIC           "max_uptime_pct": {"type": "number", "description": "Upper bound of the uptime band, exclusive"},
# MAGIC           "credit_pct":     {"type": "number", "description": "Credit as % of monthly fee"}}}}
# MAGIC     }',
# MAGIC     MAP('version', '2.1', 'enableCitations', 'true', 'enableConfidenceScores', 'true',
# MAGIC         'instructions', 'This is a signed Enterprise SLA for the NextGenLakehouse Payments API.')) AS terms
# MAGIC FROM docs_catalog
# MAGIC WHERE doc_type = 'legal_contract';
# MAGIC
# MAGIC -- Which document is which?
# MAGIC SELECT file_name, doc_type FROM docs_catalog ORDER BY file_name;

# COMMAND ----------

# DBTITLE 1,2.2 answer Orbit Bank's SLA question with SQL
# MAGIC %sql
# MAGIC -- 2.2c Answer Orbit Bank's ticket (F016) with plain SQL on the extracted contract terms:
# MAGIC -- "degraded for about 3 hours on Oct 21 in eu-west-1, how much are we entitled to?"
# MAGIC -- The LLM extracts the facts; SQL does the arithmetic exactly.
# MAGIC
# MAGIC -- t: contract terms as typed columns.
# MAGIC WITH t AS (
# MAGIC   SELECT terms:response.customer_name.value::STRING           AS customer,
# MAGIC          terms:response.uptime_commitment_pct.value::DOUBLE   AS committed_uptime,
# MAGIC          terms:response.monthly_fee_eur.value::DOUBLE         AS monthly_fee_eur,
# MAGIC          terms:response.claim_window_days.value::INT          AS claim_window_days,
# MAGIC          terms:response.claim_email.value::STRING             AS claim_email,
# MAGIC          terms:response.credit_tiers                          AS tiers
# MAGIC   FROM sla_terms
# MAGIC ),
# MAGIC -- incident: ai_extract reads the downtime and its date from Orbit Bank's ticket;
# MAGIC -- SQL derives the hours in that month (31 * 24 for October).
# MAGIC incident AS (
# MAGIC   SELECT ext:response.downtime_hours.value::DOUBLE AS downtime_hours,
# MAGIC          day(last_day(to_date(ext:response.incident_date.value::STRING))) * 24 AS hours_in_month
# MAGIC   FROM (SELECT ai_extract(text_en,
# MAGIC           '{"downtime_hours": {"type": "number", "description": "Hours of downtime or degradation reported"},
# MAGIC             "incident_date":  {"type": "string", "description": "Incident date as YYYY-MM-DD. The year is 2026."}}',
# MAGIC           MAP('version', '2.1')) AS ext
# MAGIC         FROM fb_triage WHERE feedback_id = 'F016')
# MAGIC ),
# MAGIC -- calc: actual monthly uptime %.
# MAGIC calc AS (
# MAGIC   SELECT t.*, round(100 * (i.hours_in_month - i.downtime_hours) / i.hours_in_month, 3) AS actual_uptime_pct
# MAGIC   FROM t CROSS JOIN incident i
# MAGIC ),
# MAGIC -- tiers: one row per credit band [min, max).
# MAGIC tiers AS (
# MAGIC   SELECT tier.value:min_uptime_pct.value::DOUBLE AS min_pct,
# MAGIC          tier.value:max_uptime_pct.value::DOUBLE AS max_pct,
# MAGIC          tier.value:credit_pct.value::DOUBLE     AS credit_pct
# MAGIC   FROM t, LATERAL variant_explode(t.tiers) AS tier
# MAGIC )
# MAGIC -- Keep the credit band that contains the actual uptime. LEFT JOIN + coalesce:
# MAGIC -- a month that meets the SLA (no matching band) returns a credit of 0, not zero rows.
# MAGIC SELECT c.customer, c.committed_uptime, c.actual_uptime_pct,
# MAGIC        coalesce(b.credit_pct, 0)                         AS credit_pct,
# MAGIC        c.monthly_fee_eur * coalesce(b.credit_pct, 0) / 100 AS credit_eur,
# MAGIC        c.claim_email, c.claim_window_days
# MAGIC FROM calc c
# MAGIC LEFT JOIN tiers b
# MAGIC   ON  c.actual_uptime_pct >= coalesce(b.min_pct, 0)
# MAGIC   AND c.actual_uptime_pct <  coalesce(b.max_pct, 100.0001);

# COMMAND ----------

# DBTITLE 1,2.2 explanation
# MAGIC %md
# MAGIC **What happened.** `ai_classify` labeled each PDF by document type straight from the parsed `VARIANT`, so only the contract went to extraction. `ai_extract` then pulled typed terms from the SLA, including the **credit tier table as an array of objects**, with citations and confidence scores. A second `ai_extract` read the downtime and its date from Orbit Bank's ticket, and plain SQL computed the actual uptime (**99.597%**), matched the 25% credit tier and returned **EUR 12,000**, with the claim address and the 30-day window.
# MAGIC
# MAGIC **Why developers care.** Today, "how much do we owe Orbit Bank?" means finding the signed PDF, reading the credit table and doing the math in a spreadsheet, usually while the customer is waiting. Here the answer comes from the contract and is traceable through citations. The AI extracts the facts and SQL does the arithmetic, so the number is exact and repeatable.
# MAGIC
# MAGIC Have finance confirm the extracted terms once per contract, store them as a table, and join every incident to it: the next SLA breach is calculated the day it happens, not when the customer asks.

# COMMAND ----------

# DBTITLE 1,About ai_prep_search
# MAGIC %md
# MAGIC ### `ai_prep_search` (Beta)
# MAGIC Splits a document into retrieval-ready chunks and adds document context to each one.
# MAGIC
# MAGIC ```sql
# MAGIC ai_prep_search(parsed [, options])
# MAGIC ```
# MAGIC
# MAGIC | Parameter | Type | Description |
# MAGIC | --- | --- | --- |
# MAGIC | `parsed` | `VARIANT` or `STRING` | The output of `ai_parse_document`, or plain text or markdown. |
# MAGIC | `options` | `MAP<STRING, STRING>` | Optional. `version`, and `schema` to extract document-level metadata (same format as `ai_extract`). |
# MAGIC
# MAGIC **Returns** a `VARIANT` whose `document.contents` array holds one entry per chunk: `chunk_id`, `chunk_position`, `chunk_to_retrieve` (raw text) and `chunk_to_embed` (text with title and headers).
# MAGIC
# MAGIC [ai_prep_search documentation](https://docs.databricks.com/aws/en/sql/language-manual/functions/ai_prep_search/)

# COMMAND ----------

# DBTITLE 1,2.3 ai_prep_search: chunk docs for retrieval
# MAGIC %sql
# MAGIC -- 2.3 ai_prep_search(parsed): split a parsed document into retrieval-ready chunks.
# MAGIC -- The chunks are the knowledge base used to answer tickets in 2.4.
# MAGIC CREATE OR REPLACE TABLE docs_chunks AS
# MAGIC WITH prepped AS (
# MAGIC   SELECT file_name, ai_prep_search(parsed) AS prep FROM docs_parsed
# MAGIC )
# MAGIC -- One row per chunk: chunk_to_retrieve is shown to readers,
# MAGIC -- chunk_to_embed adds title/section context and is what we match tickets against.
# MAGIC SELECT c.value:chunk_id::STRING          AS chunk_id,
# MAGIC        file_name,
# MAGIC        c.value:chunk_position::INT       AS chunk_position,
# MAGIC        c.value:chunk_to_retrieve::STRING AS chunk_to_retrieve,
# MAGIC        c.value:chunk_to_embed::STRING    AS chunk_to_embed,
# MAGIC        coalesce(prep:document.source_uri::STRING, file_name) AS source_uri
# MAGIC FROM prepped, LATERAL variant_explode(prep:document.contents) AS c;
# MAGIC
# MAGIC -- Look at the context-enriched chunks.
# MAGIC SELECT file_name, chunk_position, chunk_to_embed FROM docs_chunks ORDER BY file_name, chunk_position;

# COMMAND ----------

# DBTITLE 1,2.3 explanation
# MAGIC %md
# MAGIC **What happened.** `ai_prep_search` split each parsed document into chunks that follow its structure. Each chunk has `chunk_to_retrieve`, the raw text you show a reader, and `chunk_to_embed`, the same text with the document title and section headers added, so matching knows where the text comes from.
# MAGIC
# MAGIC **Why developers care.** Chunking is where many RAG projects lose quality: fixed-size splits cut tables in half and drop the heading that gives a paragraph its meaning. One function replaces that custom code and keeps the context.
# MAGIC
# MAGIC The output is a plain Delta table, so it works with any retrieval method: the cross join in 2.4 for a handful of documents, or an AI Search index synced from the table when your docs site has thousands of pages.

# COMMAND ----------

# DBTITLE 1,About ai_similarity and ai_gen
# MAGIC %md
# MAGIC ### `ai_similarity` and `ai_gen`
# MAGIC `ai_similarity` scores how close two texts are in meaning. `ai_gen` generates text from a prompt.
# MAGIC
# MAGIC ```sql
# MAGIC ai_similarity(expr1, expr2)
# MAGIC ai_gen(prompt)
# MAGIC ```
# MAGIC
# MAGIC | Function | Parameter | Type | Description |
# MAGIC | --- | --- | --- | --- |
# MAGIC | `ai_similarity` | `expr1`, `expr2` | `STRING` | The two texts to compare. |
# MAGIC | `ai_gen` | `prompt` | `STRING` | The instruction and the context for the model. |
# MAGIC
# MAGIC **Returns.** `ai_similarity` returns a `FLOAT`; 1 means identical text. The score is relative, so use it to rank, not as an absolute threshold. `ai_gen` returns the generated `STRING`.
# MAGIC
# MAGIC [ai_similarity documentation](https://docs.databricks.com/aws/en/sql/language-manual/functions/ai_similarity/) | [ai_gen documentation](https://docs.databricks.com/aws/en/sql/language-manual/functions/ai_gen/)

# COMMAND ----------

# DBTITLE 1,2.4 ai_similarity + ai_gen: grounded answers for open tickets
# MAGIC %sql
# MAGIC -- 2.4 Grounded answers WITHOUT a vector index: retrieve with ai_similarity, answer with ai_gen.
# MAGIC -- Our knowledge base is 4 chunks, so we can score every (ticket, chunk) pair directly.
# MAGIC -- For thousands of documents, swap this retrieval step for ai_search over an AI Search index.
# MAGIC
# MAGIC CREATE OR REPLACE TABLE ticket_answers AS
# MAGIC -- tickets: support questions whose answer should exist in the docs.
# MAGIC WITH tickets AS (
# MAGIC   SELECT feedback_id, account_name, text_en
# MAGIC   FROM fb_triage
# MAGIC   WHERE feedback_id IN ('F003', 'F005', 'F009', 'F014', 'F016', 'F018', 'F023')
# MAGIC ),
# MAGIC -- scored: semantic similarity between each ticket and each chunk (0..1).
# MAGIC scored AS (
# MAGIC   SELECT t.*,
# MAGIC          c.file_name,
# MAGIC          c.chunk_to_retrieve,
# MAGIC          ai_similarity(t.text_en, c.chunk_to_embed) AS retrieval_score
# MAGIC   FROM tickets t
# MAGIC   CROSS JOIN docs_chunks c
# MAGIC ),
# MAGIC -- best: keep the most similar chunk per ticket (top-1 retrieval).
# MAGIC best AS (
# MAGIC   SELECT *
# MAGIC   FROM scored
# MAGIC   QUALIFY row_number() OVER (PARTITION BY feedback_id ORDER BY retrieval_score DESC) = 1
# MAGIC )
# MAGIC -- ai_gen writes the answer from that chunk only, and admits when the docs do not cover it.
# MAGIC SELECT feedback_id,
# MAGIC        account_name,
# MAGIC        text_en,
# MAGIC        file_name AS source_doc,
# MAGIC        round(retrieval_score, 3) AS retrieval_score,
# MAGIC        ai_gen(concat(
# MAGIC          'You are NextGenLakehouse developer support. Diagnose the developer message using ONLY the documentation excerpt. ',
# MAGIC          'Explain the most likely cause in one sentence, then give the exact setting, limit or step to apply. ',
# MAGIC          'Maximum 80 words, plain text. If the excerpt is unrelated to the message, reply exactly: NOT_IN_DOCS',
# MAGIC          '\n\nDeveloper message: ', text_en,
# MAGIC          '\n\nDocumentation excerpt: ', chunk_to_retrieve)) AS doc_answer
# MAGIC FROM best;
# MAGIC
# MAGIC -- Readable output: question, grounded answer and the document it came from.
# MAGIC SELECT feedback_id, account_name, text_en AS question, doc_answer, source_doc, retrieval_score
# MAGIC FROM ticket_answers
# MAGIC ORDER BY feedback_id;

# COMMAND ----------

# DBTITLE 1,2.4 explanation
# MAGIC %md
# MAGIC **What happened.** A full retrieval-augmented answer in one SQL statement. `ai_similarity` scored every ticket against every chunk from 2.3 and kept the best match, then `ai_gen` wrote the answer from that chunk **only**, or `NOT_IN_DOCS` when the chunk does not help. Mercado Sol's "429 at 40 rps on Enterprise" is answered by one sentence of the rate-limit doc: they are using a **test key**, capped at 25 rps.
# MAGIC
# MAGIC **Why developers care.** Support agents find a draft answer with its source on every open ticket before they open it, with no index, endpoint or retriever code to maintain. `source_doc` lets them check the answer in seconds, and the `NOT_IN_DOCS` tickets are a ready-made backlog for the docs team.
# MAGIC
# MAGIC Keep a human approving the drafts: top-1 retrieval can pick the wrong chunk, and `retrieval_score` is relative, so use it to rank tickets, not as a pass/fail threshold.
# MAGIC
# MAGIC **When to scale up.** A cross join is perfect for a few dozen chunks. For a real documentation site, put the same `docs_chunks` table behind an AI Search index and use `ai_search`, which adds query rewriting and reranking.
# MAGIC
# MAGIC **Exact math belongs in SQL.** For Orbit Bank, the answer points to the claim process; the exact credit (EUR 12,000) comes from the SQL in 2.2. Use the LLM for language and SQL for arithmetic.

# COMMAND ----------

# DBTITLE 1,About ai_transcribe
# MAGIC %md
# MAGIC ### `ai_transcribe`
# MAGIC Converts speech in an audio file to text, split into time-stamped segments with speaker labels.
# MAGIC
# MAGIC ```sql
# MAGIC ai_transcribe(content)
# MAGIC ```
# MAGIC
# MAGIC | Parameter | Type | Description |
# MAGIC | --- | --- | --- |
# MAGIC | `content` | `BINARY` or `FILE` | The audio file, for example the `content` column of `read_files(..., format => 'binaryFile')`. |
# MAGIC
# MAGIC **Returns** a `VARIANT`: `response.segments` is an array of `{start, end, speaker_id, text}`, and `error_message` is `NULL` on success.
# MAGIC
# MAGIC [ai_transcribe documentation](https://docs.databricks.com/aws/en/sql/language-manual/functions/ai_transcribe/)

# COMMAND ----------

# DBTITLE 1,2.5 ai_transcribe: support calls to speaker-labeled text
# MAGIC %sql
# MAGIC -- 2.5 ai_transcribe(audio_bytes): speech-to-text with timestamps and speaker labels.
# MAGIC -- Output VARIANT: t:response.segments = [{start, end, speaker_id, text}], t:error_message.
# MAGIC CREATE OR REPLACE TABLE calls_transcribed AS
# MAGIC SELECT _metadata.file_name AS file_name, ai_transcribe(content) AS t
# MAGIC FROM read_files(devpulse_volume || '/calls/', format => 'binaryFile');
# MAGIC
# MAGIC -- One row per spoken segment (skip files that failed to transcribe).
# MAGIC SELECT c.file_name,
# MAGIC        round(s.value:start::DOUBLE, 1) AS start_s,
# MAGIC        round(s.value:end::DOUBLE, 1)   AS end_s,
# MAGIC        s.value:speaker_id::STRING      AS speaker,
# MAGIC        s.value:text::STRING            AS text
# MAGIC FROM calls_transcribed c, LATERAL variant_explode(c.t:response.segments) AS s
# MAGIC WHERE is_variant_null(c.t:error_message)
# MAGIC ORDER BY c.file_name, s.pos;

# COMMAND ----------

# DBTITLE 1,2.6 Chain: transcript to translate, summarize, sentiment, extract
# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE call_insights AS
# MAGIC WITH tx AS (
# MAGIC   SELECT file_name,
# MAGIC          -- Keep the speaker on each line, so ai_extract can tell agent promises from customer statements.
# MAGIC          array_join(transform(cast(t:response.segments AS ARRAY<VARIANT>),
# MAGIC                               s -> concat(s:speaker_id::STRING, ': ', s:text::STRING)), '\n') AS transcript
# MAGIC   FROM calls_transcribed
# MAGIC   WHERE is_variant_null(t:error_message)
# MAGIC ),
# MAGIC -- en: translate the Spanish call to English (the English call passes through).
# MAGIC en AS (SELECT file_name, transcript, ai_translate(transcript, 'en') AS transcript_en FROM tx)
# MAGIC SELECT file_name, transcript, transcript_en,
# MAGIC        ai_summarize(transcript_en, 30)  AS summary,
# MAGIC        ai_analyze_sentiment(transcript_en) AS sentiment,
# MAGIC        -- Turn the conversation into trackable facts, including what support promised.
# MAGIC        ai_extract(transcript_en,
# MAGIC          '{"customer_company": {"type": "string"},
# MAGIC            "issue":            {"type": "string"},
# MAGIC            "root_cause":       {"type": "string", "description": "Cause explained by the support agent"},
# MAGIC            "resolution":       {"type": "string"},
# MAGIC            "commitment":       {"type": "string", "description": "What NextGenLakehouse promised to do next, with any deadline"},
# MAGIC            "incident_id":      {"type": "string"}}',
# MAGIC          MAP('version', '2.1')) AS facts
# MAGIC FROM en;
# MAGIC
# MAGIC -- One row per call with its key facts.
# MAGIC SELECT file_name, summary, sentiment,
# MAGIC        facts:response.customer_company.value::STRING AS company,
# MAGIC        facts:response.issue.value::STRING            AS issue,
# MAGIC        facts:response.root_cause.value::STRING       AS root_cause,
# MAGIC        facts:response.resolution.value::STRING       AS resolution,
# MAGIC        facts:response.commitment.value::STRING       AS commitment,
# MAGIC        facts:response.incident_id.value::STRING      AS incident_id
# MAGIC FROM call_insights;

# COMMAND ----------

# DBTITLE 1,2.5-2.6 explanation
# MAGIC %md
# MAGIC **What happened.** `ai_transcribe` turned the two MP3 calls, one in English and one in Spanish, into time-stamped segments with automatic **speaker labels** (agent vs customer). Because the output is a `VARIANT`, we chained it straight into `ai_translate` -> `ai_summarize` -> `ai_analyze_sentiment` -> `ai_extract` in one statement. Transcription is not perfect: it heard "LaGombe" for Lagom Pay, and that error travels downstream.
# MAGIC
# MAGIC
# MAGIC **Same answer, two channels.** On the call, the agent diagnosed Mercado Sol's test key live; in 2.4 the docs gave the same answer to their written ticket. The data agrees with itself.
# MAGIC
# MAGIC **Why developers care.** Calls are usually the least searchable channel: what is known depends on who took the notes. Here every call becomes text, and the promises the agent made ("credits within five business days", "sandbox confirmed in 24 hours") become rows you can assign, track and alert on, next to the account's written tickets.
# MAGIC
# MAGIC Check consent and retention rules before you record and transcribe calls, and correct product and customer names the model mishears (a small mapping table in SQL is enough) before the text reaches summaries and briefs.

# COMMAND ----------

# DBTITLE 1,Act 3 header
# MAGIC %md
# MAGIC ## Act 3. Diagnose: who is affected, what broke, why, and what comes next
# MAGIC
# MAGIC Two parts: **customers** (3.1 to 3.3) and **metrics** (3.4 to 3.6).
# MAGIC
# MAGIC ### Customers: enrich accounts and predict renewals
# MAGIC
# MAGIC The predictive functions (`ai_predict_class`, `ai_predict_value`) train a model **inside the query** on rows where the target is known and score the rows where it is `NULL`. They run on the SQL warehouse through `run_on_warehouse`.

# COMMAND ----------

# DBTITLE 1,About ai_enrich
# MAGIC %md
# MAGIC ### `ai_enrich`
# MAGIC Adds new fields to each row, grounded in a knowledge source such as web search, an AI Search index or an MCP service.
# MAGIC
# MAGIC ```sql
# MAGIC ai_enrich(content, schema [, knowledge_sources] [, options])
# MAGIC ```
# MAGIC
# MAGIC | Parameter | Type | Description |
# MAGIC | --- | --- | --- |
# MAGIC | `content` | `STRING` or `VARIANT` | The row to enrich, for example a company name. |
# MAGIC | `schema` | `STRING` | The fields to generate, in the same format as `ai_extract`. |
# MAGIC | `knowledge_sources` | `VARIANT` or `STRING` | Optional. A JSON array of `{type, description, config}`, for example `web_search` (with `allowed_domains`) or `mcp`. |
# MAGIC | `options` | `MAP<STRING, STRING>` | Optional. `version`, `instructions`, `enableRationale` (default `'true'`). |
# MAGIC
# MAGIC **Returns** a `VARIANT`: `response` holds `{value, rationale}` per field, `metadata` lists the sources used, and `error_message` is `NULL` on success.
# MAGIC
# MAGIC [ai_enrich documentation](https://docs.databricks.com/aws/en/sql/language-manual/functions/ai_enrich/)

# COMMAND ----------

# DBTITLE 1,3.1 ai_enrich: firmographics from the web
# MAGIC %sql
# MAGIC -- 3.1 ai_enrich(input, fields, knowledge_sources, options): add NEW columns from an
# MAGIC -- external knowledge source. Here: live web search.
# MAGIC -- Each field returns {value, rationale}; metadata.sources lists the URLs used.
# MAGIC -- Web search is the slowest call in this notebook (several seconds per row), so we
# MAGIC -- only enrich the accounts that reported urgent issues (urgency >= devpulse_urgent_min, 2.5 by default, from 1.7),
# MAGIC -- capped at 8 rows. Remove the filter to enrich every account.
# MAGIC CREATE OR REPLACE TABLE account_profiles AS
# MAGIC SELECT account_name,
# MAGIC   ai_enrich(
# MAGIC     account_name,
# MAGIC     '["industry", "headquarters_country", "approx_employees", "business_model"]',  -- fields to fill
# MAGIC     knowledge_sources => PARSE_JSON('[{"type": "web_search", "config": {}}]'),       -- or an AI Search index
# MAGIC     options => map('instructions', 'The input is a company name. Fill a field only when a source clearly describes this exact company; never use a different company with a similar name. Otherwise return null. headquarters_country is the operating headquarters. approx_employees is a rounded number.')
# MAGIC   ) AS profile
# MAGIC FROM (SELECT DISTINCT account_name FROM fb_triage
# MAGIC       WHERE decision:response.answers.urgency.score::DOUBLE >= devpulse_urgent_min
# MAGIC       ORDER BY account_name LIMIT 8);
# MAGIC
# MAGIC -- Flatten values, one rationale and the sources for spot-checking.
# MAGIC SELECT account_name,
# MAGIC        profile:response.industry.value::STRING             AS industry,
# MAGIC        profile:response.headquarters_country.value::STRING AS hq_country,
# MAGIC        profile:response.approx_employees.value::STRING     AS employees,
# MAGIC        profile:response.business_model.value::STRING       AS business_model,
# MAGIC        profile:response.industry.rationale::STRING         AS industry_rationale,
# MAGIC        profile:metadata.sources                            AS sources
# MAGIC FROM account_profiles
# MAGIC ORDER BY account_name;

# COMMAND ----------

# DBTITLE 1,3.1 explanation
# MAGIC %md
# MAGIC **What happened.** `ai_enrich` added columns the data never contained (industry, HQ country, headcount, business model) using **live web search**. Each value comes with a **rationale** and the **source URLs** it used.
# MAGIC
# MAGIC **Check the sources.** Our accounts are fictional, so the web cannot know them. Because the instructions ask for an exact match, Lagom Pay and Modestoff come back null with a rationale saying no source matches. SignalPost and Tunewave still get values, but from real, unrelated companies with the same name (in North Macedonia and India). Read `sources` before you trust a value, and for your own accounts restrict `allowed_domains` or use an internal index.
# MAGIC
# MAGIC **Keep it fast.** Web search takes several seconds per row, so the cell only enriches the accounts with urgent tickets (4 or 5 depending on the run, capped at 8). Enrich once per account and store the result; don't re-run it on every message.
# MAGIC
# MAGIC **Why developers care.** Account context changes priorities: a timeout from a bank with a strict SLA is not the same as one from a hobby project. `ai_enrich` adds that context without a firmographics contract or a scraper.
# MAGIC
# MAGIC For your own customers, point `knowledge_sources` at an AI Search index over CRM notes or account plans. Keep web search for prospects and public companies, restrict it with `allowed_domains`, and store each result with its sources so someone can audit it.

# COMMAND ----------

# DBTITLE 1,About ai_predict_class
# MAGIC %md
# MAGIC ### `ai_predict_class`
# MAGIC Table function that trains a classifier on rows with a known label and predicts the label for rows where it is `NULL`.
# MAGIC
# MAGIC ```sql
# MAGIC ai_predict_class(input => TABLE(t), target_col => 'col', feature_cols => ARRAY(...) [, seed => n])
# MAGIC ```
# MAGIC
# MAGIC | Parameter | Type | Description |
# MAGIC | --- | --- | --- |
# MAGIC | `input` | `TABLE` | Training rows (target known) and rows to score (target `NULL`). |
# MAGIC | `target_col` | `STRING` | The column to predict: numeric, boolean or string. |
# MAGIC | `feature_cols` | `ARRAY<STRING>` | The columns used as features. |
# MAGIC | `seed` | integer | Optional. Makes training reproducible. Default 0. |
# MAGIC
# MAGIC **Returns** the rows to score with `<target>_prediction` and `<target>_confidence` (0 to 1). Each call trains a new model; nothing is stored.
# MAGIC
# MAGIC [ai_predict_class documentation](https://docs.databricks.com/aws/en/sql/language-manual/functions/ai_predict_class/)

# COMMAND ----------

# DBTITLE 1,3.2 ai_predict_class: backtest then predict renewals
# -----------------------------------------------------------------------------
# 3.2 ai_predict_class(input => TABLE(...), target_col, feature_cols): trains a
# classifier INSIDE the query on rows where target_col is known, and predicts the
# rows where it is NULL. Runs on the SQL warehouse (see run_on_warehouse).
# Output: the input columns + <target>_prediction + <target>_confidence.
# -----------------------------------------------------------------------------

# Columns passed to the function, and the features the model may use
# (account_id is passed through as an identifier, not used as a feature).
FEATURE_LIST = [
    "plan", "region", "primary_sdk", "sdk_major_version", "monthly_api_calls_m",
    "tickets_90d", "negative_ticket_share", "days_since_last_login", "docs_views_30d",
]
COLS = ", ".join(["account_id"] + FEATURE_LIST)
FEATURES = "ARRAY(" + ", ".join(f"'{c}'" for c in FEATURE_LIST) + ")"

# 1) Backtest: hide the label for ~25% of the accounts whose outcome we know, then compare.
#    crc32(account_id) % 4 = 0 picks that test set deterministically (same split every run).
run_on_warehouse(f"""
    CREATE OR REPLACE TABLE renewal_backtest AS
    WITH known AS (
        SELECT {COLS}, renewal_outcome AS actual
        FROM accounts
        WHERE renewal_outcome IS NOT NULL
    ),
    input AS (
        SELECT {COLS},
               CASE WHEN crc32(account_id) % 4 = 0 THEN NULL ELSE actual END AS renewal_outcome
        FROM known
    )
    SELECT p.account_id,
           k.actual,
           p.renewal_outcome_prediction AS predicted,
           p.renewal_outcome_confidence AS confidence
    FROM ai_predict_class(
        input        => TABLE(input),
        target_col   => 'renewal_outcome',
        feature_cols => {FEATURES},
        seed         => 42
    ) p
    JOIN known k USING (account_id)
""")

# 2) Score the 40 upcoming renewals (renewal_outcome IS NULL in the accounts table).
run_on_warehouse(f"""
    CREATE OR REPLACE TABLE renewal_predictions AS
    WITH input AS (SELECT {COLS}, renewal_outcome FROM accounts)
    SELECT *
    FROM ai_predict_class(
        input        => TABLE(input),
        target_col   => 'renewal_outcome',
        feature_cols => {FEATURES},
        seed         => 42
    )
""")

# Backtest result: accuracy on the hidden labels vs the naive "always renewed" baseline.
display(spark.sql("""
    SELECT count(*)                                                       AS held_out,
           round(avg(CASE WHEN actual = predicted THEN 1 ELSE 0 END), 3)  AS accuracy,
           round(avg(CASE WHEN actual = 'renewed' THEN 1 ELSE 0 END), 3)  AS baseline_always_renewed
    FROM renewal_backtest
"""))

# Predictions for the upcoming renewals, churn risks first.
display(spark.sql("""
    SELECT account_id, plan, region, primary_sdk, sdk_major_version,
           tickets_90d, negative_ticket_share, days_since_last_login,
           renewal_outcome_prediction,
           round(renewal_outcome_confidence, 2) AS confidence
    FROM renewal_predictions
    ORDER BY renewal_outcome_prediction, confidence DESC
"""))

# COMMAND ----------

# DBTITLE 1,About ai_predict_value
# MAGIC %md
# MAGIC ### `ai_predict_value`
# MAGIC Table function: the regression version of `ai_predict_class`. Predicts a number for rows where the target is `NULL`.
# MAGIC
# MAGIC ```sql
# MAGIC ai_predict_value(input => TABLE(t), target_col => 'col', feature_cols => ARRAY(...) [, seed => n])
# MAGIC ```
# MAGIC
# MAGIC | Parameter | Type | Description |
# MAGIC | --- | --- | --- |
# MAGIC | `input` | `TABLE` | Training rows (target known) and rows to score (target `NULL`). |
# MAGIC | `target_col` | `STRING` | The numeric column to predict. |
# MAGIC | `feature_cols` | `ARRAY<STRING>` | The columns used as features. |
# MAGIC | `seed` | integer | Optional. Makes training reproducible. Default 0. |
# MAGIC
# MAGIC **Returns** the rows to score with `<target>_prediction` as a `DOUBLE`.
# MAGIC
# MAGIC [ai_predict_value documentation](https://docs.databricks.com/aws/en/sql/language-manual/functions/ai_predict_value/)

# COMMAND ----------

# DBTITLE 1,3.3 ai_predict_value: predict ARR for new accounts
# -----------------------------------------------------------------------------
# 3.3 ai_predict_value: same idea as 3.2 for a NUMERIC target (regression).
# Predicts arr_usd for the 30 accounts that have no ARR yet.
# Output column: arr_usd_prediction.
# -----------------------------------------------------------------------------
# Columns passed to the function (defined here so the cell runs on its own).
COLS = ", ".join(["account_id", "plan", "region", "primary_sdk", "monthly_api_calls_m", "tickets_90d"])
ARR_FEATURES = "ARRAY('plan', 'region', 'primary_sdk', 'monthly_api_calls_m', 'tickets_90d')"

# 1) Backtest: hide arr_usd for ~25% of the accounts with known ARR (same split as 3.2).
run_on_warehouse(f"""
    CREATE OR REPLACE TABLE arr_backtest AS
    WITH known AS (
        SELECT {COLS}, arr_usd AS actual
        FROM accounts
        WHERE arr_usd IS NOT NULL
    ),
    input AS (
        SELECT {COLS},
               CASE WHEN crc32(account_id) % 4 = 0 THEN NULL ELSE actual END AS arr_usd
        FROM known
    )
    SELECT p.account_id, p.plan, k.actual, p.arr_usd_prediction AS predicted
    FROM ai_predict_value(
        input        => TABLE(input),
        target_col   => 'arr_usd',
        feature_cols => {ARR_FEATURES},
        seed         => 42
    ) p
    JOIN known k USING (account_id)
""")

# Backtest result: model error vs a naive baseline (average ARR of the same plan,
# computed on the training rows only).
display(spark.sql("""
    WITH baseline AS (
        SELECT plan, avg(arr_usd) AS plan_avg
        FROM accounts
        WHERE arr_usd IS NOT NULL AND crc32(account_id) % 4 <> 0
        GROUP BY plan
    )
    SELECT count(*)                                                        AS held_out,
           round(avg(abs(actual - predicted)), -2)                         AS mae_model_usd,
           round(avg(abs(actual - plan_avg)), -2)                          AS mae_baseline_usd,
           round(100 * avg(abs(actual - predicted) / nullif(actual, 0)), 1) AS mape_model_pct,
           round(100 * avg(abs(actual - plan_avg)  / nullif(actual, 0)), 1) AS mape_baseline_pct
    FROM arr_backtest JOIN baseline USING (plan)
"""))

# 2) Predict the accounts with no ARR yet.

run_on_warehouse(f"""
    CREATE OR REPLACE TABLE arr_predictions AS
    WITH input AS (SELECT {COLS}, arr_usd FROM accounts)
    SELECT *
    FROM ai_predict_value(
        input        => TABLE(input),
        target_col   => 'arr_usd',
        feature_cols => {ARR_FEATURES},
        seed         => 42
    )
""")

# Predicted ARR rounded to the nearest 100 USD, biggest first.
display(spark.sql("""
    SELECT account_id, plan, monthly_api_calls_m,
           round(arr_usd_prediction, -2) AS predicted_arr_usd
    FROM arr_predictions
    ORDER BY predicted_arr_usd DESC
"""))

# COMMAND ----------

# DBTITLE 1,3.2-3.3 explanation
# MAGIC %md
# MAGIC **What happened.** `ai_predict_class` trained a classifier on the 360 accounts with a known renewal outcome and predicted `renewed` / `churned` with a confidence score for the 40 upcoming renewals. The **backtest** hides a quarter of the known labels and checks the predictions against reality, so you can see whether to trust it. In our run: **90 held-out accounts, 64.4% accuracy vs 58.9% for "always predict renewed"**. That is a real but modest lift on noisy synthetic data, enough to rank outreach and not enough to automate decisions. Always show the backtest next to the predictions. `ai_predict_value` did the same for a number: expected ARR for 30 accounts that have not been billed yet, with the same backtest. On **97 held-out accounts** its mean absolute error is **USD 52,100 vs 141,300** for "average ARR of the same plan" (MAPE 30% vs 50%).
# MAGIC
# MAGIC **Why developers care.** No feature store, training job or serving endpoint: rows `WHERE target IS NULL` *are* the scoring set, and each call trains on the latest data. Combined with Act 1, Customer Success can rank outreach by "angry about SDK 3.0 **and** likely to churn **and** high ARR" in one query.
# MAGIC
# MAGIC Show the backtest next to every prediction, use the scores to prioritize human outreach rather than to trigger automatic actions, and move to a tracked MLflow model once the decision needs monitoring and approval.

# COMMAND ----------

# DBTITLE 1,Act 3 metrics header
# MAGIC %md
# MAGIC ### Act 3, metrics (3.4 to 3.6): what broke, why, and what comes next
# MAGIC
# MAGIC Three time-series and contribution functions, all on the SQL warehouse.

# COMMAND ----------

# DBTITLE 1,About ai_detect_anomalies
# MAGIC %md
# MAGIC ### `ai_detect_anomalies` (Beta)
# MAGIC Table function that flags unusual points in a time series, using the history before each point to estimate what was expected.
# MAGIC
# MAGIC ```sql
# MAGIC ai_detect_anomalies(observed, time_col, value_col [, group_col] [, anomaly_threshold] [, covariate_col] [, holiday_region] [, only_last_n] [, smoothing])
# MAGIC ```
# MAGIC
# MAGIC | Parameter | Type | Description |
# MAGIC | --- | --- | --- |
# MAGIC | `observed` | `TABLE` | The time series data. |
# MAGIC | `time_col` | `STRING` | A `DATE` or `TIMESTAMP` column, unique within each series. |
# MAGIC | `value_col` | `STRING` | The numeric column to analyze. |
# MAGIC | `group_col` | `STRING` or `ARRAY` | Optional. Analyze each group (region, SDK...) as its own series. |
# MAGIC | `anomaly_threshold` | `DOUBLE` | Optional. Score above which a point is flagged. Default 0.95. |
# MAGIC | `holiday_region` | `STRING` | Optional. Country code for holiday effects, for example `'US'`. |
# MAGIC
# MAGIC **Returns** the input columns plus `expected`, `lower_bound`, `upper_bound`, `anomaly_score` (0 to 1) and `is_anomaly`. The first 8 rows of each series are context only.
# MAGIC
# MAGIC [ai_detect_anomalies documentation](https://docs.databricks.com/aws/en/sql/language-manual/functions/ai_detect_anomalies/)

# COMMAND ----------

# DBTITLE 1,3.4 ai_detect_anomalies: catch the incident
# -----------------------------------------------------------------------------
# 3.4 ai_detect_anomalies(TABLE(...), time_col, value_col): learns each series'
# normal level and seasonality, then flags points outside the prediction interval.
# Output: expected, lower_bound, upper_bound, anomaly_score (0..1), is_anomaly.
# Both metrics go into one long table (metric, metric_date, value); group_col
# makes ai_detect_anomalies treat each metric as its own series in ONE call.
# -----------------------------------------------------------------------------
run_on_warehouse("""
    CREATE OR REPLACE TABLE metric_anomalies AS
    WITH series AS (
        SELECT 'support_tickets' AS metric, metric_date, CAST(support_tickets AS DOUBLE) AS value FROM daily_metrics
        UNION ALL
        SELECT 'api_error_rate_pct' AS metric, metric_date, CAST(api_error_rate_pct AS DOUBLE) AS value FROM daily_metrics
    )
    SELECT *
    FROM ai_detect_anomalies(
        TABLE(series),
        time_col  => 'metric_date',
        value_col => 'value',
        group_col => 'metric'
    )
""")

# Only the anomalous days.
display(spark.sql("""
    SELECT metric, metric_date,
           round(value, 2)         AS value,
           round(expected, 2)      AS expected,
           round(upper_bound, 2)   AS upper_bound,
           round(anomaly_score, 3) AS anomaly_score
    FROM metric_anomalies
    WHERE is_anomaly
    ORDER BY metric, metric_date
"""))

# COMMAND ----------

# DBTITLE 1,3.4 explanation
# MAGIC %md
# MAGIC **What happened.** `ai_detect_anomalies` learned the normal level and weekly pattern of each series from its own history, then flagged the days that fall outside the prediction interval, with an `anomaly_score` from 0 to 1. In our run it flagged exactly **Oct 21 to 24** for both support tickets (274 vs about 85 expected on day one) and API error rate (2.97% vs 0.34%), which is the SDK 3.0 launch week, and nothing else in 200 days.
# MAGIC
# MAGIC **Why developers care.** Monitoring without hand-tuned thresholds or a separate tool. Schedule the query daily and alert on `is_anomaly` (a SQL alert is enough) to catch the next bad release on day one, not in the monthly review.
# MAGIC
# MAGIC Tune `anomaly_threshold` to trade missed incidents against noisy alerts, and use `group_col` to watch each region or SDK separately: a spike in one region can disappear in the global average.

# COMMAND ----------

# DBTITLE 1,About ai_top_drivers
# MAGIC %md
# MAGIC ### `ai_top_drivers`
# MAGIC Table function that compares a metric between a control group and a test group and ranks the segments that explain the change.
# MAGIC
# MAGIC ```sql
# MAGIC ai_top_drivers(input => TABLE(t), metric => 'col', is_test => 'col' [, dimensions => ARRAY(...)] [, aggregation => 'sum'] [, min_support => 0.05])
# MAGIC ```
# MAGIC
# MAGIC | Parameter | Type | Description |
# MAGIC | --- | --- | --- |
# MAGIC | `input` | `TABLE` | Rows with the metric, the dimensions and the test flag. |
# MAGIC | `metric` | `STRING` | The numeric column to explain. |
# MAGIC | `is_test` | `STRING` | A boolean column: `TRUE` for the test group, `FALSE` for control. |
# MAGIC | `dimensions` | `ARRAY<STRING>` | Optional. Columns to segment on. If omitted, picks the 20 most correlated. |
# MAGIC | `aggregation` | `STRING` | Optional. `sum` (default), `avg`, `count`, `min` or `max`. |
# MAGIC | `min_support` | `DOUBLE` | Optional. Minimum share of rows a segment must cover. Default 0.05. |
# MAGIC
# MAGIC **Returns** one row per segment: `contributor` (for example `["region=eu-west-1", "http_method=POST"]`), `metric_control`, `metric_test`, `change`, `relative_change` and `support`.
# MAGIC
# MAGIC [ai_top_drivers documentation](https://docs.databricks.com/aws/en/sql/language-manual/functions/ai_top_drivers/)

# COMMAND ----------

# DBTITLE 1,3.5 ai_top_drivers: root cause of the error spike
# -----------------------------------------------------------------------------
# 3.5 ai_top_drivers(input, metric, is_test, dimensions, aggregation): compares a
# metric between a control group (is_test = false) and a test group (true) and ranks
# the segments (single dimensions and pairs) that explain most of the change.
# Here: control = before the SDK 3.0 release, test = after; metric = sum(errors).
# Note: pass the table directly; a CTE as TABLE(...) input fails inside CREATE TABLE AS.
# -----------------------------------------------------------------------------
run_on_warehouse("""
    CREATE OR REPLACE TABLE error_drivers AS
    SELECT *
    FROM ai_top_drivers(
        input       => TABLE(api_calls),
        metric      => 'errors',
        is_test     => 'is_after_release',
        dimensions  => ARRAY('region', 'sdk_language', 'http_method', 'endpoint'),
        aggregation => 'sum'
    )
""")

# Top segments by absolute increase in errors. share_of_calls = support of the segment.
display(spark.sql("""
    SELECT array_join(contributor, ' AND ') AS segment,
           metric_control                   AS errors_before,
           metric_test                      AS errors_after,
           change,
           round(relative_change, 2)        AS relative_change,
           round(support, 3)                AS share_of_calls
    FROM error_drivers
    ORDER BY change DESC
    LIMIT 12
"""))

# COMMAND ----------

# DBTITLE 1,3.5 explanation
# MAGIC %md
# MAGIC **What happened.** `ai_top_drivers` compared API errors **before** (control) and **after** (test) the release and ranked the segments, single dimensions and **pairs**, that explain most of the increase.
# MAGIC
# MAGIC **Why developers care.** The slice-and-dice an SRE would spend an afternoon on, in one call, ranked by impact. Because it also tests pairs of dimensions, it finds the combination (one region with one method) that single-dimension charts spread across several bars. It matches what developers told us in Act 1 (Python, POST, eu-west-1), so the fix goes to the top of the backlog with both the telemetry and the customers behind it.
# MAGIC
# MAGIC It shows correlation, not cause: confirm with the deploy log or a trace before you roll back.

# COMMAND ----------

# DBTITLE 1,About ai_forecast
# MAGIC %md
# MAGIC ### `ai_forecast`
# MAGIC Table function that forecasts one or more time series, with prediction intervals.
# MAGIC
# MAGIC ```sql
# MAGIC ai_forecast(observed, horizon, time_col, value_col [, group_col] [, covariate_col] [, prediction_interval_width] [, frequency] [, holiday_region] [, positive_only] [, version])
# MAGIC ```
# MAGIC
# MAGIC | Parameter | Type | Description |
# MAGIC | --- | --- | --- |
# MAGIC | `observed` | `TABLE` | The history: one time column and one or more value columns. |
# MAGIC | `horizon` | date or timestamp | End of the forecast, **exclusive**. |
# MAGIC | `time_col` | `STRING` | A `DATE` or `TIMESTAMP` column. |
# MAGIC | `value_col` | `STRING` or `ARRAY` | The column(s) to forecast. |
# MAGIC | `group_col` | `STRING` or `ARRAY` | Optional. Forecast each group separately. |
# MAGIC | `holiday_region` | `STRING` | Optional. Country code for holiday effects. |
# MAGIC | `positive_only` | `BOOLEAN` | Optional. Never forecast below 0. |
# MAGIC | `version` | `STRING` | Optional. `'2'` uses a time-series foundation model (recommended). |
# MAGIC
# MAGIC **Returns** one row per future date with `<value>_forecast`, `<value>_lower` and `<value>_upper`.
# MAGIC
# MAGIC [ai_forecast documentation](https://docs.databricks.com/aws/en/sql/language-manual/functions/ai_forecast/)

# COMMAND ----------

# DBTITLE 1,3.6 ai_forecast: support load for the next 30 days
# -----------------------------------------------------------------------------
# 3.6 ai_forecast(TABLE(...), horizon, time_col, value_col, ...): forecasts a time
# series up to `horizon` with prediction intervals (version 2 = time-series
# foundation model). holiday_region adds holiday effects; positive_only clips at 0.
# Output: <value>_forecast, <value>_lower, <value>_upper per future date.
# -----------------------------------------------------------------------------
# Dates come from the data: forecast 30 days past the last actual, plot the last 90.
# horizon is exclusive, so last + 31 days yields 30 forecast days.
from datetime import timedelta
last_date = spark.sql("SELECT max(metric_date) FROM daily_metrics").first()[0]
HORIZON = last_date + timedelta(days=31)
SINCE = last_date - timedelta(days=90)

run_on_warehouse(f"""
    CREATE OR REPLACE TABLE ticket_forecast AS
    -- hist: the launch-week spike flagged in 3.4 is replaced by its expected value,
    -- so one incident does not inflate the forecast and its upper bound.
    WITH hist AS (
        SELECT d.metric_date,
               CASE WHEN a.is_anomaly THEN a.expected
                    ELSE CAST(d.support_tickets AS DOUBLE) END AS support_tickets
        FROM daily_metrics d
        LEFT JOIN metric_anomalies a
          ON a.metric = 'support_tickets' AND a.metric_date = d.metric_date
    )
    SELECT *
    FROM ai_forecast(
        TABLE(hist),
        horizon        => DATE'{HORIZON}',
        time_col       => 'metric_date',
        value_col      => 'support_tickets',
        holiday_region => 'US',
        positive_only  => true,
        version        => '2'
    )
""")

# Chart-ready output (plot as a line chart): recent actuals, one bridge row
# (last actual repeated as forecast so the two lines connect), then the forecast.
display(spark.sql(f"""
    -- Actuals for the last 90 days
    SELECT metric_date,
           CAST(support_tickets AS DOUBLE) AS actual,
           CAST(NULL AS DOUBLE)            AS forecast,
           CAST(NULL AS DOUBLE)            AS lower,
           CAST(NULL AS DOUBLE)            AS upper
    FROM daily_metrics
    WHERE metric_date >= DATE'{SINCE}'

    UNION ALL

    -- Bridge row
    SELECT metric_date, NULL, CAST(support_tickets AS DOUBLE), NULL, NULL
    FROM daily_metrics
    WHERE metric_date = (SELECT max(metric_date) FROM daily_metrics)

    UNION ALL

    -- Forecast with prediction interval
    SELECT metric_date, NULL,
           round(support_tickets_forecast, 1),
           round(support_tickets_lower, 1),
           round(support_tickets_upper, 1)
    FROM ticket_forecast

    ORDER BY metric_date
"""))

# COMMAND ----------

# DBTITLE 1,3.6 explanation
# MAGIC %md
# MAGIC **What happened.** `ai_forecast` (version 2, a time-series foundation model) projected daily support tickets for the 30 days after the last actual, with prediction intervals, US holiday awareness and a non-negative constraint. Before training, the launch-week days that 3.4 flagged are replaced by their expected values, so one incident does not distort the forecast: the point forecast follows the weekly pattern between about 53 and 97 tickets a day, and the upper bound now peaks at about 109 (it was about 225 with the spike left in). Plan staffing on the interval, not only the point forecast. The bridge row joins the actual and forecast lines when you plot the result as a line chart.
# MAGIC
# MAGIC **Why developers care.** Staff the support rotation and plan DevRel office hours from a forecast with lower and upper bounds, not last month's average. Point `value_col` at API calls and the same query plans capacity.
# MAGIC
# MAGIC Rerun it weekly as a job so the forecast follows the data, and staff for the upper bound in the weeks after a release.

# COMMAND ----------

# DBTITLE 1,Act 4 header
# MAGIC %md
# MAGIC ## Act 4. Act: reply, document and report
# MAGIC
# MAGIC Insight is only useful if it ships. These functions write the outputs DevRel actually sends.

# COMMAND ----------

# DBTITLE 1,About ai_fix_grammar
# MAGIC %md
# MAGIC ### `ai_fix_grammar`
# MAGIC Corrects grammar and spelling while keeping the meaning.
# MAGIC
# MAGIC ```sql
# MAGIC ai_fix_grammar(content)
# MAGIC ```
# MAGIC
# MAGIC | Parameter | Type | Description |
# MAGIC | --- | --- | --- |
# MAGIC | `content` | `STRING` | The text to correct. |
# MAGIC
# MAGIC **Returns** the corrected `STRING`. A `NULL` input returns `NULL`.
# MAGIC
# MAGIC [ai_fix_grammar documentation](https://docs.databricks.com/aws/en/sql/language-manual/functions/ai_fix_grammar/)

# COMMAND ----------

# DBTITLE 1,4.1 ai_fix_grammar: release notes ready to publish
# MAGIC %sql
# MAGIC -- 4.1 ai_fix_grammar(text): corrects grammar and spelling, keeps the meaning and technical tokens.
# MAGIC CREATE OR REPLACE TABLE release_notes AS
# MAGIC SELECT draft_id, release, draft_text, ai_fix_grammar(draft_text) AS published_text
# MAGIC FROM release_note_drafts;
# MAGIC
# MAGIC -- Draft vs publishable text.
# MAGIC SELECT * FROM release_notes ORDER BY draft_id;

# COMMAND ----------

# DBTITLE 1,4.1 explanation
# MAGIC %md
# MAGIC **What happened.** `ai_fix_grammar` cleaned up the drafts (agreement, tense, "you're" vs "your") without rewriting the meaning, and kept versions, headers and dates intact. It fixes grammar, not style: RN-1 still starts with a lowercase "we", so keep a human editor for tone.
# MAGIC
# MAGIC **Why developers care.** A pre-publish check for changelogs, docs PRs and status-page updates, whoever writes them and whatever their first language. It leaves versions, endpoints and dates alone, so it can run on every release note, for example as a step in the job that publishes the changelog, with the diff shown to the author.
# MAGIC
# MAGIC It does not check facts: a well-written note can still describe the wrong fix.

# COMMAND ----------

# DBTITLE 1,4.2 ai_gen + ai_translate: replies in the developer's language
# MAGIC %sql
# MAGIC -- 4.2 Close the loop: draft a reply with ai_gen, then localize it with ai_translate.
# MAGIC
# MAGIC -- base: each answered ticket + its grounded answer (2.4) + its original language (1.5).
# MAGIC -- Tickets the docs could not answer (NOT_IN_DOCS) are left for a human.
# MAGIC CREATE OR REPLACE TABLE ticket_replies AS
# MAGIC WITH base AS (
# MAGIC   SELECT a.feedback_id, a.account_name, a.text_en,
# MAGIC          a.doc_answer,
# MAGIC          t.facts:response.original_language.value::STRING AS lang
# MAGIC   FROM ticket_answers a JOIN fb_triage t USING (feedback_id)
# MAGIC   WHERE a.doc_answer NOT LIKE '%NOT_IN_DOCS%'
# MAGIC ),
# MAGIC -- drafted: ai_gen(prompt) is a general-purpose text generator. The prompt pins tone,
# MAGIC -- length and "only use the documentation answer" to keep the reply grounded.
# MAGIC drafted AS (
# MAGIC   SELECT *, ai_gen(concat(
# MAGIC     'You are a friendly NextGenLakehouse developer advocate. Write a reply of at most 120 words to the developer message below. ',
# MAGIC     'Acknowledge the impact, give the exact fix from the documentation answer, and end with one concrete next step. ',
# MAGIC     'Do not add facts that are not in the documentation answer. Plain text, no headings.',
# MAGIC     '\n\nDeveloper message: ', text_en,
# MAGIC     '\n\nDocumentation answer: ', doc_answer)) AS reply_en
# MAGIC   FROM base
# MAGIC )
# MAGIC -- Translate back into the developer's language (English replies pass through unchanged).
# MAGIC SELECT *,
# MAGIC   CASE lang WHEN 'French' THEN ai_translate(reply_en, 'fr') WHEN 'Spanish' THEN ai_translate(reply_en, 'es')
# MAGIC             WHEN 'German' THEN ai_translate(reply_en, 'de') WHEN 'Italian' THEN ai_translate(reply_en, 'it')
# MAGIC             WHEN 'Portuguese' THEN ai_translate(reply_en, 'pt') ELSE reply_en END AS reply_localized
# MAGIC FROM drafted;
# MAGIC
# MAGIC -- Replies ready for a human to approve and send.
# MAGIC SELECT feedback_id, account_name, lang, reply_en, reply_localized FROM ticket_replies ORDER BY feedback_id;

# COMMAND ----------

# DBTITLE 1,4.2 explanation
# MAGIC %md
# MAGIC **What happened.** `ai_gen` turned each grounded answer from 2.4 into a short, empathetic reply, and `ai_translate` localized it into the language the developer wrote in: Spanish for Mercado Sol and Rayo Express, German for Kronwerk, French for CovoitRoute. English replies pass through unchanged, and tickets the docs could not answer (`NOT_IN_DOCS`) are left for a human.
# MAGIC
# MAGIC **Why developers care.** The full loop: a ticket arrives in Spanish, is masked, understood, answered from the docs and drafted in Spanish, and the agent only reviews and sends. Because the input is the grounded answer and the prompt says "do not add facts", the reply is far less likely to promise something the docs do not say.
# MAGIC
# MAGIC Keep the approval step: post the draft to the support tool as an internal note, not to the customer, and track which drafts agents edit to see where the docs or the prompt need work.

# COMMAND ----------

# DBTITLE 1,4.3 ai_gen + ai_extract: the weekly DevRel brief as typed columns
# MAGIC %sql
# MAGIC -- 4.3 The weekly DevRel brief in two task-specific calls, no model name to manage:
# MAGIC --   ai_gen(prompt)            writes the brief as prose from a compact JSON of the pipeline outputs
# MAGIC --   ai_extract(text, schema)  turns that prose into typed columns (arrays included)
# MAGIC -- Pattern: per-row functions did the heavy lifting; ONE ai_gen call now reasons over the aggregate.
# MAGIC
# MAGIC -- ctx: aggregate the pipeline outputs into one JSON document (one row).
# MAGIC CREATE OR REPLACE TABLE devrel_brief AS
# MAGIC WITH ctx AS (
# MAGIC   SELECT to_json(named_struct(
# MAGIC     -- issues: sorted by number of reports, most first (collect_list alone has no order).
# MAGIC     'issues', (SELECT array_sort(collect_list(named_struct('issue', best_issue_title, 'reports', n, 'accounts', accts, 'owner_team', owner)),
# MAGIC                                  (l, r) -> CASE WHEN l.reports > r.reports THEN -1 WHEN l.reports < r.reports THEN 1 ELSE 0 END)
# MAGIC                FROM (SELECT best_issue_title::STRING AS best_issue_title, count(*) AS n, array_join(collect_set(account_name), ', ') AS accts,
# MAGIC                             max_by(decision:response.answers.team.choice::STRING, decision:response.answers.urgency.score::DOUBLE) AS owner
# MAGIC                      FROM fb_triage WHERE best_issue::STRING <> 'none' AND issue_confidence::DOUBLE >= devpulse_issue_min_conf GROUP BY best_issue_title::STRING)),
# MAGIC     'urgent', (SELECT collect_list(named_struct('account', account_name, 'tl_dr', tl_dr))
# MAGIC                FROM fb_triage WHERE decision:response.answers.urgency.score::DOUBLE >= devpulse_urgent_min),
# MAGIC     'docs_gaps', (SELECT collect_list(tl_dr) FROM fb_triage
# MAGIC                   WHERE exists(cast(topics:response AS ARRAY<VARIANT>), x -> x:value::STRING = 'docs_gap')),
# MAGIC     'praise', (SELECT collect_list(account_name) FROM fb_triage WHERE sentiment = 'positive'),
# MAGIC     'top_error_driver', (SELECT first(array_join(contributor, ' AND ')) FROM (SELECT * FROM error_drivers WHERE size(contributor) = 2 ORDER BY change DESC LIMIT 1)),
# MAGIC     'calls', (SELECT collect_list(summary) FROM call_insights)
# MAGIC   )) AS context
# MAGIC ),
# MAGIC -- written: ai_gen drafts the brief. The prompt pins audience, length, sections and "context only".
# MAGIC written AS (
# MAGIC   SELECT ai_gen(concat(
# MAGIC     'You are the head of Developer Relations at NextGenLakehouse. Using ONLY the JSON context, write the weekly brief for the VP of Engineering in under 300 words. ',
# MAGIC     'Sections: a one-line headline; the top issues in the order given (most reports first), each with evidence (account names and counts), its owner_team exactly as given in the context, and next action; docs to fix; wins; a two-sentence risk summary. ',
# MAGIC     'Do not add facts that are not in the context.\n\nContext: ', context)) AS brief_text
# MAGIC   FROM ctx
# MAGIC )
# MAGIC -- ai_extract turns the prose into typed fields, so the brief can feed a dashboard or a ticket.
# MAGIC SELECT brief_text,
# MAGIC   ai_extract(brief_text,
# MAGIC     '{
# MAGIC       "headline":     {"type": "string"},
# MAGIC       "risk_summary": {"type": "string"},
# MAGIC       "top_issues":   {"type": "array", "items": {"type": "object", "properties": {
# MAGIC           "issue":    {"type": "string"},
# MAGIC           "evidence": {"type": "string", "description": "Accounts and counts cited for this issue"},
# MAGIC           "owner":    {"type": "string", "description": "Owner team"},
# MAGIC           "action":   {"type": "string", "description": "Next action"}}}},
# MAGIC       "docs_to_fix":  {"type": "array", "items": {"type": "string"}},
# MAGIC       "wins":         {"type": "array", "items": {"type": "string"}}
# MAGIC     }',
# MAGIC     MAP('version', '2.1')) AS brief
# MAGIC FROM written;
# MAGIC
# MAGIC -- The brief as typed columns.
# MAGIC SELECT brief:response.headline.value::STRING     AS headline,
# MAGIC        brief:response.risk_summary.value::STRING AS risk_summary,
# MAGIC        brief:response.top_issues                 AS top_issues,
# MAGIC        brief:response.docs_to_fix                AS docs_to_fix,
# MAGIC        brief:response.wins                       AS wins,
# MAGIC        brief:error_message::STRING               AS error_message
# MAGIC FROM devrel_brief;

# COMMAND ----------

# DBTITLE 1,4.3 explanation
# MAGIC %md
# MAGIC **What happened.** `ai_gen` read a compact JSON of everything the pipeline learned (linked issues with their owner team, urgent tickets, docs gaps, praise, the top error driver, call summaries) and wrote the weekly brief. `ai_extract` then turned that prose into **typed** fields: a headline, a risk summary, and arrays of issues (evidence, owner, action), docs to fix and wins. No model name, endpoint or response schema to manage.
# MAGIC
# MAGIC **Review before sending.** The brief is only as good as its inputs. Without owner teams in the context, `ai_gen` invented one ("Developer Relations" for the webhook bug), so we pass them in from 1.7. The issues are passed sorted by number of reports, so the brief leads with the biggest bug (POST timeouts, 6 reports). In our run it still repeated the transcription error "LaGombe" from 2.5. Confident prose can still mix facts, so keep a human approval step.
# MAGIC
# MAGIC **Why developers care.** Row-level functions do the heavy lifting, one `ai_gen` call reasons over the aggregate, and `ai_extract` makes the result machine-readable. The typed columns drop straight into a dashboard, a Slack message or a Jira ticket, and every fact in the brief traces back to a table you can query.
# MAGIC
# MAGIC Schedule it weekly, post it as a draft for the head of DevRel to approve, and store the context JSON with the brief so you can always see what the model was given.

# COMMAND ----------

# DBTITLE 1,Gold: the DevRel Action Board
# MAGIC %sql
# MAGIC -- Gold table: one row per known issue, aggregating every AI signal from Act 1.
# MAGIC -- A message counts toward an issue only if ai_decide linked it with confidence >= devpulse_issue_min_conf (0.7, set in devpulse_00_config).
# MAGIC -- LEFT JOIN keeps issues with no matching reports.
# MAGIC CREATE OR REPLACE TABLE devrel_action_board AS
# MAGIC SELECT k.issue_id, k.title,
# MAGIC        count(t.feedback_id)                                                      AS reports,
# MAGIC        count(DISTINCT t.account_name)                                            AS accounts_affected,
# MAGIC        array_join(collect_set(t.account_name), ', ')                             AS accounts,
# MAGIC        array_join(collect_set(t.facts:response.original_language.value::STRING), ', ') AS languages,
# MAGIC        array_join(collect_set(t.channel), ', ')                                  AS channels,
# MAGIC        round(max(t.decision:response.answers.urgency.score::DOUBLE), 2)          AS max_urgency_0_to_3,
# MAGIC        round(max(t.decision:response.answers.outage.probability::DOUBLE), 2)     AS max_p_outage,
# MAGIC        max_by(t.decision:response.answers.team.choice::STRING, t.decision:response.answers.urgency.score::DOUBLE) AS owner_team
# MAGIC FROM known_issues k
# MAGIC LEFT JOIN fb_triage t ON t.best_issue::STRING = k.issue_id AND t.issue_confidence::DOUBLE >= devpulse_issue_min_conf
# MAGIC GROUP BY k.issue_id, k.title;
# MAGIC
# MAGIC -- The board, most urgent first.
# MAGIC SELECT * FROM devrel_action_board ORDER BY max_urgency_0_to_3 DESC NULLS LAST;

# COMMAND ----------

# DBTITLE 1,Wrap-up
# MAGIC %md
# MAGIC ## What we built, and why it matters
# MAGIC
# MAGIC From 24 raw messages, 4 PDFs, 2 phone calls and 3 tables to a ranked **DevRel Action Board**, grounded replies in 4 languages, a root cause, a forecast and an executive brief, written in **SQL** and governed by **Unity Catalog**.
# MAGIC
# MAGIC | Developer pain | Without AI Functions | With AI Functions |
# MAGIC | --- | --- | --- |
# MAGIC | Secrets in issues | Regex that misses half | `ai_mask` |
# MAGIC | Multilingual feedback | Manual translation | `ai_translate` |
# MAGIC | Triage and routing | Label data, train, host a model | `ai_classify`, `ai_decide` |
# MAGIC | Free text to columns | Custom NLP or prompt plumbing | `ai_extract` |
# MAGIC | Duplicate bugs | Manual linking | `ai_decide` |
# MAGIC | Docs Q&A | Build a RAG stack | `ai_parse_document`, `ai_prep_search`, `ai_similarity`, `ai_gen` (scale out with `ai_search`) |
# MAGIC | Calls | Listen and take notes | `ai_transcribe` |
# MAGIC | Account context | Buy a data feed | `ai_enrich` |
# MAGIC | Churn and ARR | Feature store, training job, endpoint | `ai_predict_class`, `ai_predict_value` |
# MAGIC | Incidents | Hand-tuned thresholds, ad hoc root cause | `ai_detect_anomalies`, `ai_top_drivers` |
# MAGIC | Capacity planning | Spreadsheet | `ai_forecast` |
# MAGIC | Replies and reports | Write by hand | `ai_gen`, `ai_fix_grammar`, `ai_extract` |
# MAGIC
# MAGIC **The board in our run:** KI-101 (POST timeouts) leads with 5 accounts (Lagom Pay, Inkwell Docs, Modestoff, PagaFacile, Tunewave), max urgency 2.98 and outage probability 0.99. Next is KI-102 (duplicate webhooks) with 3 accounts (StayNest, CovoitRoute, Sabor Delivery). SignalPost's signature failure is correctly kept off it, and Mercado Sol's low-confidence 429 link (0.63) is not counted. Every signal points the same way: the feedback (Act 1), the docs and calls (Act 2), the error data (`ai_top_drivers`: Python + POST + eu-west-1) and the forecast (Act 3) all say **fix the POST timeout default first**.
# MAGIC
# MAGIC **Productionize it.** Every cell is a plain `CREATE OR REPLACE TABLE ... AS SELECT`, so the pipeline moves as-is into a Lakeflow Spark Declarative Pipeline or a scheduled job. Point a dashboard or a Genie space at `devrel_action_board`.
# MAGIC
# MAGIC **Good practices shown here.** Mask before any other AI call. Pin function versions (`'version', '2.1'`). Store AI outputs once and derive scores from the stored values. Keep exact arithmetic in SQL. Backtest predictive outputs before trusting them.

# COMMAND ----------

# DBTITLE 1,Cost tracking for this demo
# MAGIC %md
# MAGIC ## Cost tracking
# MAGIC
# MAGIC Recommended tags: `project=devpulse_ai_functions`, `owner=<your_name>`, `team=devrel`, `env=demo`. Tag values cannot contain `-`, `,`, `=`, `/` or `:`.
# MAGIC
# MAGIC * **Serverless notebook compute:** in the notebook's **Environment** panel, pick a serverless budget policy that carries these tags.
# MAGIC * **SQL warehouse:** add the tags to the warehouse in its settings. If the warehouse is shared with other users, use a dedicated tagged warehouse for clean attribution.
# MAGIC
# MAGIC The cell below reports DBUs by product for this notebook, read from `system.billing.usage`. Statements sent with `run_on_warehouse` are billed to the warehouse.

# COMMAND ----------

# DBTITLE 1,Cost report from system.billing.usage
# -----------------------------------------------------------------------------
# Cost report: DBUs billed to this notebook's serverless compute (including the
# row-level AI Functions) PLUS the SQL warehouse used by run_on_warehouse for the
# predictive functions (3.2-3.6), read from the billing system table.
# Warehouse usage is not tagged with a notebook id, so the warehouse rows include
# any other workload that ran on the same warehouse.
# Requires SELECT on system.billing.usage (ask an admin if it fails with a
# permission error). Billing records can lag a few hours, so an empty result
# right after a run is expected.
# -----------------------------------------------------------------------------

# Look up this notebook's id at run time (nothing is hard-coded).
notebook_id = str(
    dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookId().get()
)

# Named parameters keep the query safe from injection.
display(spark.sql("""
    SELECT usage_date,
           CASE WHEN usage_metadata.warehouse_id = :wh THEN 'sql_warehouse' ELSE 'notebook' END AS source,
           billing_origin_product,
           custom_tags,
           round(sum(usage_quantity), 2) AS dbus
    FROM system.billing.usage
    WHERE usage_date >= current_date() - INTERVAL 30 DAYS
      AND (usage_metadata.notebook_id = :nb OR usage_metadata.warehouse_id = :wh)
    GROUP BY ALL
    ORDER BY usage_date, dbus DESC
""", args={"nb": notebook_id, "wh": WAREHOUSE_ID}))

# COMMAND ----------

# DBTITLE 1,Cleanup (optional)
# -----------------------------------------------------------------------------
# Cleanup. Commented out on purpose so "Run all" never deletes anything.
# Uncomment the lines you need and run this cell when you are done with the demo.
# -----------------------------------------------------------------------------

# Drop every table, the volume and its files created by this notebook.
# spark.sql(f"DROP SCHEMA IF EXISTS {CATALOG}.{SCHEMA} CASCADE")

# COMMAND ----------

# DBTITLE 1,Presenter guide: video intro and pitch steps
# MAGIC %md
# MAGIC ## Presenter guide: how to pitch this demo
# MAGIC
# MAGIC ### Video intro (about 45 seconds, read to camera)
# MAGIC
# MAGIC > You just shipped a new SDK. Within 48 hours, developer feedback arrives from GitHub, forums, app reviews, support tickets and phone calls, in six languages, with API keys and phone numbers pasted right into the issues.
# MAGIC >
# MAGIC > Most teams would need a translation service, a PII scrubber, a labeled dataset, a RAG stack, a speech-to-text API, an ML platform and a BI tool to make sense of it.
# MAGIC >
# MAGIC > Today I'll do it with **SQL**. One notebook, about twenty Databricks AI Functions, no model to train, no endpoint to deploy. By the end we'll know what broke, why, who is affected and who might churn, and we'll have replies ready in each developer's language.
# MAGIC >
# MAGIC > Meet NextGenLakehouse, a fictional payments API, on the worst launch week of its life.
# MAGIC
# MAGIC ### Before you hit record
# MAGIC
# MAGIC 1. Run `devpulse_01_data_generator` once, then **Run all** on this notebook so every cell has fresh output. Live AI calls take seconds to minutes; in a video, scroll through results rather than waiting.
# MAGIC 2. Collapse the explanation cells you won't read aloud, and widen the result tables.
# MAGIC 3. Pre-open one raw row (F001, Tunewave) to show the leaked key and email before masking.
# MAGIC
# MAGIC ### The storyline (about 10 minutes)
# MAGIC
# MAGIC | # | Beat | Show | Say |
# MAGIC | --- | --- | --- | --- |
# MAGIC | 1 | **The mess** (30 s) | `feedback_raw`, F001 | "Secrets, six languages, four channels. This is real launch-week feedback." |
# MAGIC | 2 | **Safe first** (45 s) | 1.1 `ai_mask` before vs after | "Before any AI touches it, one function removes keys, emails and phones. Everything downstream is safe." |
# MAGIC | 3 | **Understand** (90 s) | 1.2 to 1.6 | "Translate, sentiment, our own taxonomy with confidence and rationale, typed fields out of free text. Each is one function call." |
# MAGIC | 4 | **Triage** (60 s) | 1.7 `ai_decide` | "One call links every message to a known bug, picks the owner team and scores outage, urgency and trust risk. 'none' is a real answer, so SignalPost's signature failure isn't forced onto the webhook bug, and low-confidence links like Mercado Sol's stay off the board." |
# MAGIC | 5 | **The docs know the answer** (90 s) | 2.2 Orbit Bank credit, 2.4 grounded answers | "The LLM reads the contract; SQL does the math: EUR 12,000. Mercado Sol's 429 is a test key, straight from the docs, with a citation and no vector index." |
# MAGIC | 6 | **Hear the customer** (45 s) | 2.5 to 2.6 calls | "Phone calls become speaker-labeled text, then summaries and tracked commitments." |
# MAGIC | 7 | **Who is at risk** (60 s) | 3.2 backtest, then predictions | "A churn model trained inside the query. Show the backtest first: 64% vs 59% baseline, honest and useful for ranking outreach." |
# MAGIC | 8 | **The data agrees** (90 s) | 3.4 anomalies, 3.5 top drivers, 3.6 forecast | "Anomalies flag launch week only. Root cause: Python + POST + eu-west-1, exactly what developers told us in Act 1." |
# MAGIC | 9 | **Close the loop** (60 s) | 4.2 replies, 4.3 brief | "A grounded reply in Spanish for Mercado Sol, and a typed weekly brief for the VP of Engineering." |
# MAGIC | 10 | **The payoff** (30 s) | Gold action board, Wrap-up table | "From noise to a ranked action board. Fix the POST timeout default first." |
# MAGIC
# MAGIC ### Key messages to repeat
# MAGIC
# MAGIC * **It's just SQL.** Every step is `CREATE OR REPLACE TABLE ... AS SELECT`, so it moves as-is into a Lakeflow Spark Declarative Pipeline or a scheduled job.
# MAGIC * **Governed by Unity Catalog.** Data, files and results stay in one governed place; masking happens first.
# MAGIC * **Right tool per step.** Task-specific functions for row-level work, SQL for exact math, one `ai_gen` call to reason over the result and `ai_extract` to type it.
# MAGIC * **Trust, but verify.** Confidence scores, rationales, citations and backtests are part of the output. Point out the mistakes on camera; it builds credibility.
# MAGIC
# MAGIC ### Likely audience questions
# MAGIC
# MAGIC * *How much does it cost?* Pay per call; nothing keeps running when the notebook stops. Use the **Cost tracking** cells.
# MAGIC * *What about thousands of documents?* Keep `ai_prep_search` and switch the retrieval in 2.4 to `ai_search` over an AI Search index.
# MAGIC * *Can I use my own model?* This demo uses only task-specific functions, so there is no model to pick or endpoint to manage. When you need a specific model, `ai_query` calls any model in `system.ai` or your own serving endpoint.
# MAGIC * *Why are my numbers different?* LLM outputs are non-deterministic; the story holds, exact values move a little.
# MAGIC
# MAGIC ### Call to action (closing line)
# MAGIC
# MAGIC > Clone the three notebooks, point the widgets at your catalog and **Run all**. Then replace `feedback_raw` with your own tickets and see what your developers are telling you.