# Databricks notebook source
# DBTITLE 1,DevPulse intro
# MAGIC %md
# MAGIC # DevPulse: Developer Experience Intelligence with Databricks AI Functions
# MAGIC
# MAGIC **The scenario.** *Nimbus* is a (fictional) payments API platform. It just shipped **SDK 3.0** and developer feedback is pouring in from GitHub issues, forums, support tickets, app reviews and phone calls, in 5 languages and full of secrets and PII. The docs live in PDFs and the metrics live in tables.
# MAGIC
# MAGIC **The mission.** Turn that noise into a **DevRel Action Board** with nothing more than **SQL**: no model training, no prompt plumbing, no extra infrastructure.
# MAGIC
# MAGIC | Act | Question | AI Functions |
# MAGIC | --- | --- | --- |
# MAGIC | 1. Listen | What are developers telling us, safely and in one language? | `ai_mask`, `ai_translate`, `ai_analyze_sentiment`, `ai_classify`, `ai_extract`, `ai_summarize`, `ai_similarity`, `ai_decide` |
# MAGIC | 2. Read & hear | What do our docs and support calls say? | `ai_parse_document`, `ai_prep_search`, `ai_similarity` + `ai_gen` (RAG), `ai_transcribe` |
# MAGIC | 3. Know the customer | Who is affected, and who might churn? | `ai_enrich`, `ai_predict_class`, `ai_predict_value` |
# MAGIC | 4. Measure | What broke, why, and what comes next? | `ai_detect_anomalies`, `ai_top_drivers`, `ai_forecast` |
# MAGIC | 5. Act | Reply, document, report | `ai_gen`, `ai_fix_grammar`, `ai_translate`, `ai_query` |
# MAGIC
# MAGIC All data is synthetic and created by the companion notebook `devpulse_01_data_generator`. Company names are used only for a realistic demo.
# MAGIC
# MAGIC ### Project layout
# MAGIC
# MAGIC | Notebook | Role |
# MAGIC | --- | --- |
# MAGIC | `devpulse_00_config` | Widgets, Unity Catalog setup and the `run_on_warehouse` helper. Loaded with `%run` by the two others |
# MAGIC | `devpulse_01_data_generator` | Creates the fake tables, PDFs and MP3s. Run it **once** first |
# MAGIC | **This notebook** | The AI Functions pipeline (Acts 1 to 5) |
# MAGIC
# MAGIC ### Prerequisites
# MAGIC
# MAGIC | Requirement | Why |
# MAGIC | --- | --- |
# MAGIC | Databricks workspace with **Unity Catalog** | All tables and the volume live in UC |
# MAGIC | `USE CATALOG` + `CREATE SCHEMA` on the target catalog | The config notebook creates its own schema and volume |
# MAGIC | **Serverless** notebook compute | Runs the notebook and the row-level AI Functions |
# MAGIC | A **Pro or Serverless SQL warehouse** | The predictive functions (`ai_forecast`, `ai_detect_anomalies`, `ai_top_drivers`, `ai_predict_*`) run there through the Statement Execution API |
# MAGIC | Access to `system.ai` models | Used by `ai_query` |
# MAGIC | Previews enabled where required | Some functions (for example `ai_transcribe`, `ai_enrich`, the predictive functions) can be in Beta or Preview in your region. Ask a workspace admin to enable them on the **Previews** page |
# MAGIC | Outbound internet from serverless (data generator only) | `%pip install` (reportlab, gTTS) and Google Text-to-Speech for the 2 audio files |
# MAGIC
# MAGIC ### How to run
# MAGIC
# MAGIC 1. Keep the three notebooks in the **same folder** and attach them to **Serverless** compute.
# MAGIC 2. Open `devpulse_01_data_generator`, fill in the widgets (`catalog`, `schema`, `warehouse_id` empty = auto, `llm_model`) and **Run all** (1 to 2 minutes).
# MAGIC 3. Open this notebook, use the **same widget values** and **Run all** (about 10 minutes; `ai_enrich` web search is the slowest step).
# MAGIC 4. When you are done, run the **Cleanup** cell to drop the demo schema.
# MAGIC
# MAGIC No index, endpoint or model to deploy: everything below is a function call.
# MAGIC
# MAGIC **Cost.** Every AI Function call is billed as model serving or warehouse usage. The demo is small (24 messages, 4 PDFs, 2 calls, 8,000 rows). Nothing keeps running after the notebook stops: there is no endpoint or index to pay for.
# MAGIC
# MAGIC **Results vary.** LLM-backed functions are non-deterministic. Numbers quoted in the explanation cells come from one reference run, and yours will be close but not identical.

# COMMAND ----------

# DBTITLE 1,Load shared configuration
# MAGIC %run ./devpulse_00_config

# COMMAND ----------

# DBTITLE 1,Act 1 header
# MAGIC %md
# MAGIC ## Act 1. Listen: turn 24 messy messages into structured, safe, triaged insight
# MAGIC
# MAGIC Each step below is **one SQL function call** and persists a table, so the cells chain into a real pipeline (`feedback_raw` -> `feedback_masked` -> `feedback_en` -> `fb_sentiment` -> `fb_topics` -> `fb_facts` -> `fb_enriched` -> `fb_triage`). Drop the same SQL into a Lakeflow Spark Declarative Pipeline or a scheduled job and it runs on every new message.

# COMMAND ----------

# DBTITLE 1,1.1 ai_mask: redact secrets and PII
# MAGIC %sql
# MAGIC -- 1.1 ai_mask(text, labels): replace every entity matching one of the labels with [MASKED].
# MAGIC -- Run it FIRST so no later AI call (or analyst) ever sees secrets or PII.
# MAGIC -- Labels are plain English; 'credential' catches API keys and webhook secrets
# MAGIC -- without masking the words "API key" themselves.
# MAGIC CREATE OR REPLACE TABLE feedback_masked AS
# MAGIC SELECT *,
# MAGIC        ai_mask(raw_text, array('email', 'phone number', 'person name', 'credential')) AS masked_text
# MAGIC FROM feedback_raw;
# MAGIC
# MAGIC -- Show only the rows that contained something sensitive: before vs after.
# MAGIC SELECT feedback_id, account_name, raw_text, masked_text
# MAGIC FROM feedback_masked
# MAGIC WHERE raw_text RLIKE '@|sk_live|whsec|\\+[0-9]'
# MAGIC ORDER BY feedback_id;

# COMMAND ----------

# DBTITLE 1,1.1 explanation
# MAGIC %md
# MAGIC **What happened.** `ai_mask(text, labels)` replaced every email, phone number, person name and **credential** (live API key `sk_live_...`, webhook secret `whsec_...`) with `[MASKED]`, across languages, with no regex. It is context aware: error codes like `NMB-408`, versions and endpoints are kept; secret values go. It errs on the side of safety: on some runs it also masks the words "API-Key" in SAP's German "API-Key-Rotation". Label wording matters (`'credential'` works better than `'api key'`), so test labels on a sample first.
# MAGIC
# MAGIC **Why developers care.** Developers paste secrets into GitHub issues all the time. Masking **first** means every downstream table, LLM call, dashboard and Genie space only ever sees safe text. Compliance by default, in one line.

# COMMAND ----------

# DBTITLE 1,1.2 ai_translate: one language for the whole team
# MAGIC %sql
# MAGIC -- 1.2 ai_translate(text, target_language): the source language is detected automatically.
# MAGIC -- We translate the MASKED text so secrets never leave the masking step.
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
# MAGIC **What happened.** `ai_translate(text, 'en')` auto-detected French, Spanish, German, Italian and Portuguese and produced English, while keeping technical tokens such as `NMB-408`, `payment.succeeded` and `timeout=30000` intact. English rows pass through unchanged.
# MAGIC
# MAGIC **Why developers care.** The Paris and Sao Paulo forums now land in the same queue as GitHub. Later we translate the **reply** back into each developer's language (Act 5).

# COMMAND ----------

# DBTITLE 1,1.3 ai_analyze_sentiment: how do developers feel
# MAGIC %sql
# MAGIC -- 1.3 ai_analyze_sentiment(text) returns one of: positive, negative, neutral, mixed.
# MAGIC CREATE OR REPLACE TABLE fb_sentiment AS
# MAGIC SELECT *, ai_analyze_sentiment(text_en) AS sentiment
# MAGIC FROM feedback_en;
# MAGIC
# MAGIC -- Mood meter: how many messages per sentiment, and from which accounts.
# MAGIC SELECT sentiment, count(*) AS messages, array_join(collect_list(account_name), ', ') AS accounts
# MAGIC FROM fb_sentiment
# MAGIC GROUP BY sentiment
# MAGIC ORDER BY messages DESC;

# COMMAND ----------

# DBTITLE 1,1.3 explanation
# MAGIC %md
# MAGIC **What happened.** `ai_analyze_sentiment` returns `positive`, `negative`, `neutral` or `mixed` with zero configuration. Datadog's "40% faster, but the changelog hid a breaking change" lands in **mixed**, which a keyword model would get wrong.
# MAGIC
# MAGIC **Why developers care.** A launch-week mood meter that DevRel can trend daily, per SDK, per channel.

# COMMAND ----------

# DBTITLE 1,1.4 ai_classify: multi-label topics with confidence and rationale
# MAGIC %sql
# MAGIC -- 1.4 ai_classify(content, labels, options): zero-shot classification into OUR taxonomy.
# MAGIC -- Labels are a JSON object {label: description}; descriptions improve accuracy a lot.
# MAGIC -- Output is a VARIANT: topics:response is an array of {value, confidence_score, rationale}.
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
# MAGIC         'instructions', 'Inputs are developer feedback about the Nimbus payments API and SDKs, posted in the week after the SDK 3.0 release.')  -- domain context
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
# MAGIC **What happened.** `ai_classify` v2.1 took our own taxonomy (labels **with descriptions**), returned **several labels per message** (`multilabel`), a **confidence score** and a short **rationale** grounded in the text.
# MAGIC
# MAGIC **Why developers care.** No labeled training set, no model to host. Change the taxonomy by editing a JSON string. Confidence lets you auto-route high-confidence items and send the rest to a human; rationales make the routing auditable.

# COMMAND ----------

# DBTITLE 1,1.5 ai_extract: typed fields out of free text
# MAGIC %sql
# MAGIC -- 1.5 ai_extract(content, schema, options): pull typed fields out of free text.
# MAGIC -- The schema is JSON: each field has a type (string, number, integer, enum, array, object)
# MAGIC -- and an optional description that tells the model what to look for.
# MAGIC -- We read the ORIGINAL language (masked) to show that no translation is needed.
# MAGIC CREATE OR REPLACE TABLE fb_facts AS
# MAGIC SELECT *,
# MAGIC   ai_extract(
# MAGIC     masked_text,
# MAGIC     '{
# MAGIC       "original_language": {"type": "enum", "labels": ["English", "French", "Spanish", "German", "Italian", "Portuguese"]},
# MAGIC       "sdk_version":       {"type": "string",  "description": "Nimbus SDK version mentioned, e.g. 3.0.1"},
# MAGIC       "error_code":        {"type": "string",  "description": "Nimbus error code such as NMB-408, or HTTP status like 429"},
# MAGIC       "endpoint":          {"type": "string",  "description": "API endpoint path such as /v2/payments"},
# MAGIC       "http_method":       {"type": "enum",    "labels": ["GET", "POST", "PUT", "DELETE"]},
# MAGIC       "region":            {"type": "string",  "description": "Cloud region such as eu-west-1"},
# MAGIC       "business_impact":   {"type": "enum",    "labels": ["production_outage", "revenue_loss", "blocked_testing", "developer_friction", "none"], "description": "production_outage = live customer traffic failing; blocked_testing = only pre-production or load tests blocked"},
# MAGIC       "workaround":        {"type": "string",  "description": "Workaround the developer found, if any"},
# MAGIC       "asks":              {"type": "array",   "description": "Concrete product, fix or documentation requests made to Nimbus. Exclude contact details and greetings.", "items": {"type": "string"}}
# MAGIC     }',
# MAGIC     MAP('version', '2.1',
# MAGIC         'enableConfidenceScores', 'true',   -- each field returns {value, confidence_score}
# MAGIC         'instructions', 'Developer feedback about the Nimbus payments API. Leave a field null if it is not mentioned.')  -- avoid invented values
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
# MAGIC **What happened.** `ai_extract` v2.1 read the original (masked) text in any language and filled a **typed schema**: enums, strings and arrays, each with a confidence score. Missing fields are `null` instead of hallucinated.
# MAGIC
# MAGIC **Why developers care.** Free text becomes columns you can `GROUP BY`: how many `production_outage` reports hit `POST /v2/payments` in `eu-west-1` on SDK `3.0.1`? That is a bug report engineering can act on.

# COMMAND ----------

# DBTITLE 1,1.6 ai_summarize: TL;DR for long threads
# MAGIC %sql
# MAGIC -- 1.6 ai_summarize(text, max_words): abstractive summary of at most max_words words.
# MAGIC -- Only long messages are summarized; short ones are already their own TL;DR
# MAGIC -- (this also saves model calls).
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
# MAGIC **What happened.** `ai_summarize(text, 40)` compressed an 8-post forum thread into one sentence: payouts fail with timeouts after 3.0, and downgrading or raising the timeout fixes it. Note that it dropped the `max_retries=3` detail, so keep the full text next to the summary for engineers.
# MAGIC
# MAGIC **Why developers care.** Engineers on call read the TL;DR, not the thread. Same function works on Slack exports, PR discussions or call transcripts (see Act 2).

# COMMAND ----------

# DBTITLE 1,1.7 ai_similarity: match feedback to known issues
# MAGIC %sql
# MAGIC -- 1.7 ai_similarity(a, b): semantic similarity score between two texts (higher = closer).
# MAGIC -- Score every message against every known issue (24 x 4 pairs), keep the best match.
# MAGIC CREATE OR REPLACE TABLE fb_issue_match AS
# MAGIC WITH scored AS (
# MAGIC   SELECT f.feedback_id, k.issue_id, k.title,
# MAGIC          ai_similarity(f.text_en, k.title || '. ' || k.description) AS score
# MAGIC   FROM fb_enriched f CROSS JOIN known_issues k
# MAGIC )
# MAGIC -- max_by(x, score) returns x from the row with the highest score.
# MAGIC SELECT feedback_id,
# MAGIC        max_by(issue_id, score) AS best_issue,
# MAGIC        max_by(title, score)    AS best_issue_title,
# MAGIC        round(max(score), 3)    AS similarity
# MAGIC FROM scored
# MAGIC GROUP BY feedback_id;
# MAGIC
# MAGIC -- Best match per message, most confident first. Low scores mean "no real match".
# MAGIC SELECT m.feedback_id, f.account_name, f.tl_dr, m.best_issue, m.best_issue_title, m.similarity
# MAGIC FROM fb_issue_match m JOIN fb_enriched f USING (feedback_id)
# MAGIC ORDER BY m.similarity DESC;

# COMMAND ----------

# DBTITLE 1,1.7 explanation
# MAGIC %md
# MAGIC **What happened.** `ai_similarity(a, b)` returns a semantic similarity score. We scored every message against every open bug and kept the best match. The French, Portuguese and English duplicate-webhook reports score about 0.9 against **KI-102**, and the Italian timeout report maps to **KI-101** at about 0.8. Low scores (praise, feature requests, GDPR, under about 0.6) are only the "closest of a bad bunch", so the action board links a message to a bug only when `similarity >= 0.69`.
# MAGIC
# MAGIC **Honest caveat.** A threshold is a trade-off. In our run Twilio's *signature* failure scored 0.73 against the *duplicate webhook* bug (a false match), while Spotify's follow-up on the timeout bug scored 0.66 (a missed duplicate). Calibrate the threshold on a labeled sample, or pair similarity with the `ai_classify` topic.
# MAGIC
# MAGIC **Why developers care.** Instant duplicate detection: one bug, many reporters. This feeds the "affected accounts" count on the action board, and engineering sees the real blast radius of KI-101.

# COMMAND ----------

# DBTITLE 1,1.8 ai_decide: triage, outage probability and urgency
# MAGIC %sql
# MAGIC -- 1.8 ai_decide(input, questions, options): answer several structured questions per row.
# MAGIC -- Question types used here:
# MAGIC --   choice -> pick one option (with a probability per option)
# MAGIC --   noul   -> yes/no question, returns a probability
# MAGIC --   score  -> place the input on an ordered rubric, returns a 0..N score
# MAGIC CREATE OR REPLACE TABLE fb_triage AS
# MAGIC SELECT f.*, m.best_issue, m.best_issue_title, m.similarity,
# MAGIC   ai_decide(
# MAGIC     -- Give the model the message plus context extracted earlier, as one JSON string.
# MAGIC     to_json(named_struct(
# MAGIC       'feedback', f.text_en,
# MAGIC       'channel', f.channel,
# MAGIC       'business_impact', f.facts:response.business_impact.value::STRING)),
# MAGIC     '{
# MAGIC       "team": {"type": "choice", "instructions": "Which Nimbus team should own this feedback?",
# MAGIC         "criteria": {
# MAGIC           "sdk_engineering": "Bugs or regressions in an SDK",
# MAGIC           "api_platform": "Server-side API behavior, rate limits, uptime",
# MAGIC           "docs": "Documentation or changelog gaps without a code bug",
# MAGIC           "security": "Authentication, keys, signatures",
# MAGIC           "legal_compliance": "GDPR, contracts, data residency",
# MAGIC           "account_management": "SLA credits, commercial questions",
# MAGIC           "devrel_community": "Praise or feature ideas to engage with"}},
# MAGIC       "outage": {"type": "noul", "instructions": "Does this describe a live production outage happening now?"},
# MAGIC       "urgency": {"type": "score", "instructions": "How urgent is a response?",
# MAGIC         "criteria": ["Can wait for the next sprint", "Reply this week", "Reply today", "Page on-call now"]},
# MAGIC       "trust_risk": {"type": "noul", "instructions": "Is the developer losing trust in Nimbus or hinting they might switch provider?"}
# MAGIC     }',
# MAGIC     map('version', '1.0')
# MAGIC   ) AS decision
# MAGIC FROM fb_enriched f
# MAGIC LEFT JOIN fb_issue_match m USING (feedback_id);
# MAGIC
# MAGIC -- Triage queue: numeric answers you can threshold, sort and alert on.
# MAGIC SELECT feedback_id, account_name, tl_dr,
# MAGIC        decision:response.answers.team.choice::STRING                 AS owner_team,
# MAGIC        round(decision:response.answers.outage.probability::DOUBLE, 2) AS p_outage,
# MAGIC        round(decision:response.answers.urgency.score::DOUBLE, 2)      AS urgency_0_to_3,
# MAGIC        round(decision:response.answers.trust_risk.probability::DOUBLE, 2) AS p_trust_risk
# MAGIC FROM fb_triage
# MAGIC ORDER BY urgency_0_to_3 DESC;

# COMMAND ----------

# DBTITLE 1,1.8 explanation
# MAGIC %md
# MAGIC **What happened.** One `ai_decide` call answered **four questions** about each message: a **choice** (owner team, with probabilities for every option), two **yes/no probabilities** (live outage? trust risk?) and a **score** on an ordered rubric (urgency 0 to 3).
# MAGIC
# MAGIC **Why developers care.** Numbers, not prose: you can threshold them (`p_outage > 0.8` pages on-call), sort a queue by urgency, or alert when trust risk spikes. Klarna's "PRODUCTION OUTAGE" goes to the top; Figma's praise goes to DevRel for a thank-you.

# COMMAND ----------

# DBTITLE 1,Act 2 header
# MAGIC %md
# MAGIC ## Act 2. Read and hear: PDFs, grounded answers and phone calls, all in SQL
# MAGIC
# MAGIC The answers to most tickets already exist in the docs. The richest feedback is in support calls. Let's make both queryable.

# COMMAND ----------

# DBTITLE 1,2.1 ai_parse_document: PDFs to structured elements
# MAGIC %sql
# MAGIC -- 2.1 ai_parse_document(binary, options): PDF / image / Office file -> structured VARIANT.
# MAGIC -- read_files(..., format => 'binaryFile') returns one row per file with its bytes in `content`.
# MAGIC -- devpulse_volume is the SQL session variable set in the configuration cell.
# MAGIC CREATE OR REPLACE TABLE docs_parsed AS
# MAGIC SELECT _metadata.file_name AS file_name,
# MAGIC        path,
# MAGIC        ai_parse_document(content, MAP('version', '2.0')) AS parsed
# MAGIC FROM read_files(devpulse_volume || '/docs/', format => 'binaryFile');
# MAGIC
# MAGIC -- Inspect the elements of the SLA: type (title, section_header, text, table) and content.
# MAGIC SELECT d.file_name,
# MAGIC        e.value:type::STRING    AS element_type,
# MAGIC        e.value:content::STRING AS content
# MAGIC FROM docs_parsed d, LATERAL variant_explode(d.parsed:document:elements) AS e
# MAGIC WHERE d.file_name LIKE '%sla%'
# MAGIC ORDER BY e.pos;

# COMMAND ----------

# DBTITLE 1,2.1 explanation
# MAGIC %md
# MAGIC **What happened.** `ai_parse_document` turned binary PDFs into a `VARIANT` with typed **elements**: `title`, `section_header`, `text` and `table` (tables come back as HTML, so rows and columns survive).
# MAGIC
# MAGIC **Why developers care.** No PDF library, no OCR service. The parsed `VARIANT` feeds `ai_classify`, `ai_extract` and `ai_prep_search` directly, without flattening it to text first.

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
# MAGIC         'instructions', 'This is a signed Enterprise SLA for the Nimbus Payments API.')) AS terms
# MAGIC FROM docs_catalog
# MAGIC WHERE doc_type = 'legal_contract';
# MAGIC
# MAGIC -- Which document is which?
# MAGIC SELECT file_name, doc_type FROM docs_catalog ORDER BY file_name;

# COMMAND ----------

# DBTITLE 1,2.2 answer Revolut's SLA question with SQL
# MAGIC %sql
# MAGIC -- 2.2c Answer Revolut's ticket (F016) with plain SQL on the extracted contract terms:
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
# MAGIC -- incident: 3 hours of downtime in October (31 days).
# MAGIC incident AS (SELECT 3.0 AS downtime_hours, 31 * 24 AS hours_in_october),
# MAGIC -- calc: actual monthly uptime %.
# MAGIC calc AS (
# MAGIC   SELECT t.*, round(100 * (i.hours_in_october - i.downtime_hours) / i.hours_in_october, 3) AS actual_uptime_pct
# MAGIC   FROM t CROSS JOIN incident i
# MAGIC )
# MAGIC SELECT customer, committed_uptime, actual_uptime_pct,
# MAGIC        tier.value:credit_pct.value::DOUBLE AS credit_pct,
# MAGIC        monthly_fee_eur * tier.value:credit_pct.value::DOUBLE / 100 AS credit_eur,
# MAGIC        claim_email, claim_window_days
# MAGIC -- Keep the one credit tier whose [min, max) uptime band contains the actual uptime.
# MAGIC FROM calc, LATERAL variant_explode(tiers) AS tier
# MAGIC WHERE actual_uptime_pct >= coalesce(tier.value:min_uptime_pct.value::DOUBLE, 0)
# MAGIC   AND actual_uptime_pct <  tier.value:max_uptime_pct.value::DOUBLE;

# COMMAND ----------

# DBTITLE 1,2.2 explanation
# MAGIC %md
# MAGIC **What happened.** `ai_classify` labeled each PDF by document type straight from the parsed `VARIANT`. Then `ai_extract` pulled typed contract terms from the SLA, including the **credit tier table as an array of objects**, with citations and confidence scores. Plain SQL then computed Revolut's actual uptime for the 3-hour incident and looked up the matching credit tier.
# MAGIC
# MAGIC **Why developers care.** The account manager gets an exact credit amount, claim address and deadline, computed from the signed contract rather than from someone's memory. Contract terms become columns you can join to incidents.

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
# MAGIC **What happened.** `ai_prep_search` split each parsed document into semantic chunks. Each chunk has `chunk_to_retrieve` (the raw text you show to a reader) and `chunk_to_embed` (the same text with the document title and section headers prepended, so matching knows the context).
# MAGIC
# MAGIC **Why developers care.** Chunking is where most RAG projects lose quality. Here it is one function that respects the document structure, and the output is ready for any retrieval method: the cross join in 2.4 today, an AI Search index tomorrow.

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
# MAGIC          'You are Nimbus developer support. Diagnose the developer message using ONLY the documentation excerpt. ',
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
# MAGIC **What happened.** A complete retrieval-augmented answer in one SQL statement. `ai_similarity` scored every ticket against every chunk from 2.3 and kept the best match, and each ticket landed on the right document. `ai_gen` then wrote the answer from that chunk **only**. Mercado Libre's "429 at 40 rps on Enterprise" is solved by one sentence of the rate-limit doc: they are using a **test key**, capped at 25 rps.
# MAGIC
# MAGIC **Why developers care.** Batch RAG over a whole support queue with no index to build, no endpoint to pay for and no retriever code. `source_doc` is the citation, and `NOT_IN_DOCS` is a clean signal for "we have a documentation gap".
# MAGIC
# MAGIC **When to scale up.** A cross join is perfect for a few dozen chunks. For a real documentation site, put the same `docs_chunks` table behind an AI Search index and use `ai_search`, which adds query rewriting and reranking.
# MAGIC
# MAGIC **Exact math belongs in SQL.** For Revolut, the answer points to the claim process; the exact credit (EUR 12,000) comes from the SQL in 2.2. Use the LLM for language and SQL for arithmetic.

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
# MAGIC -- 2.6 Chain several AI Functions on the transcripts in ONE statement:
# MAGIC --   segments -> full transcript -> ai_translate -> ai_summarize / ai_analyze_sentiment / ai_extract
# MAGIC
# MAGIC -- tx: concatenate the segment texts into one transcript per call.
# MAGIC CREATE OR REPLACE TABLE call_insights AS
# MAGIC WITH tx AS (
# MAGIC   SELECT file_name,
# MAGIC          array_join(transform(cast(t:response.segments AS ARRAY<VARIANT>), s -> s:text::STRING), ' ') AS transcript
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
# MAGIC            "commitment":       {"type": "string", "description": "What Nimbus promised to do next, with any deadline"},
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
# MAGIC **What happened.** `ai_transcribe` turned MP3 files into time-stamped segments with automatic **speaker labels** (agent vs customer), in English and Spanish. Because the output is a `VARIANT`, we chained it straight into `ai_translate` -> `ai_summarize` -> `ai_analyze_sentiment` -> `ai_extract` in one statement.
# MAGIC
# MAGIC **Read it critically.** It is beta and our audio is synthetic text-to-speech. It heard "Klarna" as "Klana", and in the Spanish call it split a few words between speakers. Proper nouns and diarization need a human glance before you use them as keys.
# MAGIC
# MAGIC **Same answer, two channels.** On the call, the agent diagnosed Mercado Libre's test key live; in 2.4 the docs gave the same answer to their written ticket. The data agrees with itself.
# MAGIC
# MAGIC **Why developers care.** Calls stop being a black box. The commitments the agent made ("credits within five business days", "sandbox confirmed in 24 hours") become rows you can track and alert on.

# COMMAND ----------

# DBTITLE 1,Act 3 header
# MAGIC %md
# MAGIC ## Act 3. Know the customer: enrich accounts and predict renewals
# MAGIC
# MAGIC The predictive functions (`ai_predict_class`, `ai_predict_value`) train a model **inside the query** on rows where the target is known and score the rows where it is `NULL`. They run on the SQL warehouse through `run_on_warehouse`.

# COMMAND ----------

# DBTITLE 1,3.1 ai_enrich: firmographics from the web
# MAGIC %sql
# MAGIC -- 3.1 ai_enrich(input, fields, knowledge_sources, options): add NEW columns from an
# MAGIC -- external knowledge source. Here: live web search.
# MAGIC -- Each field returns {value, rationale}; metadata.sources lists the URLs used.
# MAGIC -- Web search is the slowest call in this notebook (several seconds per row), so we
# MAGIC -- only enrich the accounts that reported urgent issues (urgency >= 2.5 from 1.8),
# MAGIC -- capped at 8 rows. Remove the filter to enrich every account.
# MAGIC CREATE OR REPLACE TABLE account_profiles AS
# MAGIC SELECT account_name,
# MAGIC   ai_enrich(
# MAGIC     account_name,
# MAGIC     '["industry", "headquarters_country", "approx_employees", "business_model"]',  -- fields to fill
# MAGIC     knowledge_sources => PARSE_JSON('[{"type": "web_search", "config": {}}]'),       -- or an AI Search index
# MAGIC     options => map('instructions', 'The input is the name of a well-known company. Fill each field from public sources; use null if unknown. headquarters_country is the operating headquarters. approx_employees is a rounded number.')
# MAGIC   ) AS profile
# MAGIC FROM (SELECT DISTINCT account_name FROM fb_triage
# MAGIC       WHERE decision:response.answers.urgency.score::DOUBLE >= 2.5
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
# MAGIC **Review before you trust it.** Web-grounded values can be debatable, and they change between runs. Klarna's headquarters comes back as **Sweden** (where it operates) on some runs and **United Kingdom** (where its listed holding company is registered) on others, even with an instruction asking for the operating headquarters. Both are defensible; the `rationale` and `sources` columns tell you which one you got.
# MAGIC
# MAGIC **Keep it fast.** Web search takes several seconds per row, so the cell only enriches the accounts with urgent tickets (4 or 5 depending on the run, capped at 8). Enrich once per account and store the result; don't re-run it on every message.
# MAGIC
# MAGIC **Why developers care.** DevRel can segment feedback by industry or region without buying a firmographics feed. Point `knowledge_sources` at an AI Search index of your CRM notes and it enriches from internal data instead.

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

# DBTITLE 1,3.3 ai_predict_value: predict ARR for new accounts
# -----------------------------------------------------------------------------
# 3.3 ai_predict_value: same idea as 3.2 for a NUMERIC target (regression).
# Predicts arr_usd for the 30 accounts that have no ARR yet. Reuses COLS from 3.2.
# Output column: arr_usd_prediction.
# -----------------------------------------------------------------------------
run_on_warehouse(f"""
    CREATE OR REPLACE TABLE arr_predictions AS
    WITH input AS (SELECT {COLS}, arr_usd FROM accounts)
    SELECT *
    FROM ai_predict_value(
        input        => TABLE(input),
        target_col   => 'arr_usd',
        feature_cols => ARRAY('plan', 'region', 'primary_sdk', 'monthly_api_calls_m', 'tickets_90d'),
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
# MAGIC **What happened.** `ai_predict_class` trained a classifier on the 360 accounts with a known renewal outcome and predicted `renewed` / `churned` with a confidence score for the 40 upcoming renewals. The **backtest** hides a quarter of the known labels and checks the predictions against reality, so you can see whether to trust it. In our run: **90 held-out accounts, 64.4% accuracy vs 58.9% for "always predict renewed"**. That is a real but modest lift on noisy synthetic data, enough to rank outreach and not enough to automate decisions. Always show the backtest next to the predictions. `ai_predict_value` did the same for a number: expected ARR for 30 accounts that have not been billed yet.
# MAGIC
# MAGIC **Why developers care.** No feature store, no training job, no endpoint: `WHERE target IS NULL` *is* the scoring set. Combined with Act 1, DevRel can prioritise outreach by "angry about SDK 3.0 **and** predicted to churn **and** high ARR".

# COMMAND ----------

# DBTITLE 1,Act 4 header
# MAGIC %md
# MAGIC ## Act 4. Measure: what broke, why, and what comes next
# MAGIC
# MAGIC Three time-series and contribution functions, all on the SQL warehouse.

# COMMAND ----------

# DBTITLE 1,4.1 ai_detect_anomalies: catch the incident
# -----------------------------------------------------------------------------
# 4.1 ai_detect_anomalies(TABLE(...), time_col, value_col): learns each series'
# normal level and seasonality, then flags points outside the prediction interval.
# Output: expected, lower_bound, upper_bound, anomaly_score (0..1), is_anomaly.
# We run it on two metrics and stack the results with UNION ALL.
# -----------------------------------------------------------------------------
run_on_warehouse("""
    CREATE OR REPLACE TABLE metric_anomalies AS
    WITH tickets AS (
        SELECT metric_date, CAST(support_tickets AS DOUBLE) AS value FROM daily_metrics
    ),
    errs AS (
        SELECT metric_date, api_error_rate_pct AS value FROM daily_metrics
    )
    SELECT 'support_tickets' AS metric, *
    FROM ai_detect_anomalies(TABLE(tickets), time_col => 'metric_date', value_col => 'value')
    UNION ALL
    SELECT 'api_error_rate_pct' AS metric, *
    FROM ai_detect_anomalies(TABLE(errs), time_col => 'metric_date', value_col => 'value')
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

# DBTITLE 1,4.1 explanation
# MAGIC %md
# MAGIC **What happened.** `ai_detect_anomalies` learned the normal level and weekly pattern of each series from its own history, then flagged the days that fall outside the prediction interval, with an `anomaly_score` from 0 to 1. In our run it flagged exactly **Oct 21 to 24** for both support tickets (274 vs about 85 expected on day one) and API error rate (2.97% vs 0.34%), which is the SDK 3.0 launch week, and nothing else in 200 days.
# MAGIC
# MAGIC **Why developers care.** Monitoring without hand-tuned thresholds. Schedule this query and alert on `is_anomaly` to catch the next bad release on day one.

# COMMAND ----------

# DBTITLE 1,4.2 ai_top_drivers: root cause of the error spike
# -----------------------------------------------------------------------------
# 4.2 ai_top_drivers(input, metric, is_test, dimensions, aggregation): compares a
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

# DBTITLE 1,4.2 explanation
# MAGIC %md
# MAGIC **What happened.** `ai_top_drivers` compared API errors **before** (control) and **after** (test) the release and ranked the segments, single dimensions and **pairs**, that explain most of the increase.
# MAGIC
# MAGIC **Why developers care.** This is the root-cause query an SRE would spend an afternoon on, in one call. It matches what developers told us in Act 1: Python, POST and eu-west-1. Data and voice of the customer agree.

# COMMAND ----------

# DBTITLE 1,4.3 ai_forecast: support load for the next 30 days
# -----------------------------------------------------------------------------
# 4.3 ai_forecast(TABLE(...), horizon, time_col, value_col, ...): forecasts a time
# series up to `horizon` with prediction intervals (version 2 = time-series
# foundation model). holiday_region adds holiday effects; positive_only clips at 0.
# Output: <value>_forecast, <value>_lower, <value>_upper per future date.
# -----------------------------------------------------------------------------
run_on_warehouse("""
    CREATE OR REPLACE TABLE ticket_forecast AS
    WITH hist AS (
        SELECT metric_date, CAST(support_tickets AS DOUBLE) AS support_tickets
        FROM daily_metrics
    )
    SELECT *
    FROM ai_forecast(
        TABLE(hist),
        horizon        => DATE'2026-12-16',
        time_col       => 'metric_date',
        value_col      => 'support_tickets',
        holiday_region => 'US',
        positive_only  => true,
        version        => '2'
    )
""")

# Chart-ready output (plot as a line chart): recent actuals, one bridge row
# (last actual repeated as forecast so the two lines connect), then the forecast.
display(spark.sql("""
    -- Actuals since September
    SELECT metric_date,
           CAST(support_tickets AS DOUBLE) AS actual,
           CAST(NULL AS DOUBLE)            AS forecast,
           CAST(NULL AS DOUBLE)            AS lower,
           CAST(NULL AS DOUBLE)            AS upper
    FROM daily_metrics
    WHERE metric_date >= DATE'2026-09-01'

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

# DBTITLE 1,4.3 explanation
# MAGIC %md
# MAGIC **What happened.** `ai_forecast` (version 2, a time-series foundation model) projected daily support tickets to mid-December with prediction intervals, US holiday awareness and a non-negative constraint. It picked up the weekly pattern (about 90 tickets on weekdays, about 53 to 58 at weekends). The upper bound is wide (up to about 225), partly because the launch spike is still in the history, so plan staffing on the interval and not only the point forecast. The bridge row joins the actual and forecast lines when you plot the result as a line chart.
# MAGIC
# MAGIC **Why developers care.** Staff the support rotation and plan DevRel office hours from a forecast, not a guess. Swap `value_col` for API calls to plan capacity.

# COMMAND ----------

# DBTITLE 1,Act 5 header
# MAGIC %md
# MAGIC ## Act 5. Act: reply, document and report
# MAGIC
# MAGIC Insight is only useful if it ships. These functions write the outputs DevRel actually sends.

# COMMAND ----------

# DBTITLE 1,5.1 ai_fix_grammar: release notes ready to publish
# MAGIC %sql
# MAGIC -- 5.1 ai_fix_grammar(text): corrects grammar and spelling, keeps the meaning and technical tokens.
# MAGIC CREATE OR REPLACE TABLE release_notes AS
# MAGIC SELECT draft_id, release, draft_text, ai_fix_grammar(draft_text) AS published_text
# MAGIC FROM release_note_drafts;
# MAGIC
# MAGIC -- Draft vs publishable text.
# MAGIC SELECT * FROM release_notes ORDER BY draft_id;

# COMMAND ----------

# DBTITLE 1,5.1 explanation
# MAGIC %md
# MAGIC **What happened.** `ai_fix_grammar` cleaned up the drafts (agreement, tense, "you're" vs "your") without rewriting the meaning, and kept versions, headers and dates intact. It fixes grammar, not style: RN-1 still starts with a lowercase "we", so keep a human editor for tone.
# MAGIC
# MAGIC **Why developers care.** A pre-publish step for changelogs, docs PRs and status-page updates, whoever writes them and whatever their first language.

# COMMAND ----------

# DBTITLE 1,5.2 ai_gen + ai_translate: replies in the developer's language
# MAGIC %sql
# MAGIC -- 5.2 Close the loop: draft a reply with ai_gen, then localize it with ai_translate.
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
# MAGIC     'You are a friendly Nimbus developer advocate. Write a reply of at most 120 words to the developer message below. ',
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

# DBTITLE 1,5.2 explanation
# MAGIC %md
# MAGIC **What happened.** `ai_gen` turned each grounded answer from 2.4 into a short, empathetic reply. `ai_translate` then localized it into the language the developer wrote in (Spanish for Mercado Libre, German for SAP, French for BlaBlaCar).
# MAGIC
# MAGIC **Why developers care.** The full loop: a ticket arrives in Spanish, is masked, understood, answered from the docs and replied to in Spanish, and a human only approves. The instruction "do not add facts" plus a grounded input keeps the reply honest.

# COMMAND ----------

# DBTITLE 1,5.3 ai_query: the weekly DevRel brief as structured output
# MAGIC %sql
# MAGIC -- 5.3 ai_query(model, prompt, responseFormat): call any model with structured output.
# MAGIC -- Pattern: task-specific functions did the per-row work; ONE ai_query call now reasons
# MAGIC -- over a compact JSON summary of everything the pipeline produced.
# MAGIC
# MAGIC -- ctx: aggregate the pipeline outputs into one JSON document (one row).
# MAGIC CREATE OR REPLACE TABLE devrel_brief AS
# MAGIC WITH ctx AS (
# MAGIC   SELECT to_json(named_struct(
# MAGIC     'issues', (SELECT collect_list(named_struct('issue', best_issue_title, 'reports', n, 'accounts', accts))
# MAGIC                FROM (SELECT best_issue_title, count(*) AS n, array_join(collect_set(account_name), ', ') AS accts
# MAGIC                      FROM fb_triage WHERE similarity >= 0.69 GROUP BY best_issue_title)),
# MAGIC     'urgent', (SELECT collect_list(named_struct('account', account_name, 'tl_dr', tl_dr))
# MAGIC                FROM fb_triage WHERE decision:response.answers.urgency.score::DOUBLE >= 2.5),
# MAGIC     'docs_gaps', (SELECT collect_list(tl_dr) FROM fb_triage
# MAGIC                   WHERE exists(cast(topics:response AS ARRAY<VARIANT>), x -> x:value::STRING = 'docs_gap')),
# MAGIC     'praise', (SELECT collect_list(account_name) FROM fb_triage WHERE sentiment = 'positive'),
# MAGIC     'top_error_driver', (SELECT first(array_join(contributor, ' AND ')) FROM (SELECT * FROM error_drivers WHERE size(contributor) = 2 ORDER BY change DESC LIMIT 1)),
# MAGIC     'calls', (SELECT collect_list(summary) FROM call_insights)
# MAGIC   )) AS context
# MAGIC )
# MAGIC -- Model name comes from the llm_model widget (devpulse_llm session variable).
# MAGIC -- responseFormat must have exactly one top-level field, so the brief is wrapped in `brief`.
# MAGIC SELECT ai_query(
# MAGIC   devpulse_llm,
# MAGIC   concat('You are the head of Developer Relations at Nimbus. Using ONLY the JSON context, write the weekly brief for the VP of Engineering. ',
# MAGIC          'Be specific: cite account names, counts and segments from the context.\n\nContext: ', context),
# MAGIC   responseFormat => 'STRUCT<brief: STRUCT<headline: STRING, top_issues: ARRAY<STRUCT<issue: STRING, evidence: STRING, owner: STRING, action: STRING>>, docs_to_fix: ARRAY<STRING>, wins: ARRAY<STRING>, risk_summary: STRING>>'
# MAGIC ) AS brief
# MAGIC FROM ctx;
# MAGIC
# MAGIC SELECT b.headline, b.risk_summary, b.top_issues, b.docs_to_fix, b.wins
# MAGIC -- responseFormat needs one top-level field (brief); the returned JSON holds the inner struct.
# MAGIC FROM (SELECT from_json(brief, 'STRUCT<headline: STRING, top_issues: ARRAY<STRUCT<issue: STRING, evidence: STRING, owner: STRING, action: STRING>>, docs_to_fix: ARRAY<STRING>, wins: ARRAY<STRING>, risk_summary: STRING>') AS b FROM devrel_brief);

# COMMAND ----------

# DBTITLE 1,5.3 explanation
# MAGIC %md
# MAGIC **What happened.** `ai_query` is the general-purpose function: pick any model in `system.ai` (here Claude Sonnet) and constrain the output with `responseFormat`. We passed a compact JSON of everything the pipeline learned (issues, urgent tickets, docs gaps, root-cause segment, call summaries) and got back a **typed** brief: a headline, issues with owner and action, docs to fix, wins and risks.
# MAGIC
# MAGIC **Review before sending.** The brief is only as good as its inputs. In one run it called the timeout bug "Python & Node" (it is Python only), and it can treat Twilio's signature failure as part of the duplicate-webhook issue. That second error comes from the similarity false match flagged in 1.7. Garbage in, confident prose out, so keep a human approval step.
# MAGIC
# MAGIC **Why developers care.** Use the task-specific functions for the heavy lifting on every row, then one `ai_query` call to reason across the aggregated result. The structured output drops straight into a dashboard, Slack message or Jira ticket.

# COMMAND ----------

# DBTITLE 1,Gold: the DevRel Action Board
# MAGIC %sql
# MAGIC -- Gold table: one row per known issue, aggregating every AI signal from Act 1.
# MAGIC -- A message counts toward an issue only if ai_similarity >= 0.69 (see 1.7).
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
# MAGIC LEFT JOIN fb_triage t ON t.best_issue = k.issue_id AND t.similarity >= 0.69
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
# MAGIC | Duplicate bugs | Manual linking | `ai_similarity` |
# MAGIC | Docs Q&A | Build a RAG stack | `ai_parse_document`, `ai_prep_search`, `ai_similarity`, `ai_gen` (scale out with `ai_search`) |
# MAGIC | Calls | Listen and take notes | `ai_transcribe` |
# MAGIC | Account context | Buy a data feed | `ai_enrich` |
# MAGIC | Churn and ARR | Feature store, training job, endpoint | `ai_predict_class`, `ai_predict_value` |
# MAGIC | Incidents | Hand-tuned thresholds, ad hoc root cause | `ai_detect_anomalies`, `ai_top_drivers` |
# MAGIC | Capacity planning | Spreadsheet | `ai_forecast` |
# MAGIC | Replies and reports | Write by hand | `ai_gen`, `ai_fix_grammar`, `ai_query` |
# MAGIC
# MAGIC **The board in our run:** KI-101 (POST timeouts) leads with 5 accounts (Klarna, Notion, Zalando, Satispay, Spotify), max urgency 2.98 and outage probability 0.99. Next is KI-102 (duplicate webhooks) with 4 accounts, which includes Twilio by mistake: its issue is signatures, not duplicates (see 1.7). Every signal points the same way: the feedback (Act 1), the docs and calls (Act 2), the error data (`ai_top_drivers`: Python + POST + eu-west-1) and the forecast all say **fix the POST timeout default first**.
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
# row-level AI Functions), read from the billing system table.
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
           billing_origin_product,
           custom_tags,
           round(sum(usage_quantity), 2) AS dbus
    FROM system.billing.usage
    WHERE usage_date >= current_date() - INTERVAL 30 DAYS
      AND usage_metadata.notebook_id = :nb
    GROUP BY ALL
    ORDER BY usage_date, dbus DESC
""", args={"nb": notebook_id}))

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
# MAGIC > Meet Nimbus, a fictional payments API, on the worst launch week of its life.
# MAGIC
# MAGIC ### Before you hit record
# MAGIC
# MAGIC 1. Run `devpulse_01_data_generator` once, then **Run all** on this notebook so every cell has fresh output. Live AI calls take seconds to minutes; in a video, scroll through results rather than waiting.
# MAGIC 2. Collapse the explanation cells you won't read aloud, and widen the result tables.
# MAGIC 3. Pre-open one raw row (F001, Spotify) to show the leaked key and email before masking.
# MAGIC
# MAGIC ### The storyline (about 10 minutes)
# MAGIC
# MAGIC | # | Beat | Show | Say |
# MAGIC | --- | --- | --- | --- |
# MAGIC | 1 | **The mess** (30 s) | `feedback_raw`, F001 | "Secrets, six languages, four channels. This is real launch-week feedback." |
# MAGIC | 2 | **Safe first** (45 s) | 1.1 `ai_mask` before vs after | "Before any AI touches it, one function removes keys, emails and phones. Everything downstream is safe." |
# MAGIC | 3 | **Understand** (90 s) | 1.2 to 1.6 | "Translate, sentiment, our own taxonomy with confidence and rationale, typed fields out of free text. Each is one function call." |
# MAGIC | 4 | **Triage** (60 s) | 1.7 `ai_similarity`, 1.8 `ai_decide` | "Link every message to a known bug and score urgency. Notice the Twilio false match: AI needs review, and the notebook shows it." |
# MAGIC | 5 | **The docs know the answer** (90 s) | 2.2 Revolut credit, 2.4 grounded answers | "The LLM reads the contract; SQL does the math: EUR 12,000. Mercado Libre's 429 is a test key, straight from the docs, with a citation and no vector index." |
# MAGIC | 6 | **Hear the customer** (45 s) | 2.5 to 2.6 calls | "Phone calls become speaker-labeled text, then summaries and tracked commitments." |
# MAGIC | 7 | **Who is at risk** (60 s) | 3.2 backtest, then predictions | "A churn model trained inside the query. Show the backtest first: 64% vs 59% baseline, honest and useful for ranking outreach." |
# MAGIC | 8 | **The data agrees** (90 s) | 4.1 anomalies, 4.2 top drivers, 4.3 forecast | "Anomalies flag launch week only. Root cause: Python + POST + eu-west-1, exactly what developers told us in Act 1." |
# MAGIC | 9 | **Close the loop** (60 s) | 5.2 replies, 5.3 brief | "A grounded reply in Spanish for Mercado Libre, and a typed weekly brief for the VP of Engineering." |
# MAGIC | 10 | **The payoff** (30 s) | Gold action board, Wrap-up table | "From noise to a ranked action board. Fix the POST timeout default first." |
# MAGIC
# MAGIC ### Key messages to repeat
# MAGIC
# MAGIC * **It's just SQL.** Every step is `CREATE OR REPLACE TABLE ... AS SELECT`, so it moves as-is into a Lakeflow Spark Declarative Pipeline or a scheduled job.
# MAGIC * **Governed by Unity Catalog.** Data, files and results stay in one governed place; masking happens first.
# MAGIC * **Right tool per step.** Task-specific functions for row-level work, SQL for exact math, one `ai_query` to reason over the result.
# MAGIC * **Trust, but verify.** Confidence scores, rationales, citations and backtests are part of the output. Point out the mistakes on camera; it builds credibility.
# MAGIC
# MAGIC ### Likely audience questions
# MAGIC
# MAGIC * *How much does it cost?* Pay per call; nothing keeps running when the notebook stops. Use the **Cost tracking** cells.
# MAGIC * *What about thousands of documents?* Keep `ai_prep_search` and switch the retrieval in 2.4 to `ai_search` over an AI Search index.
# MAGIC * *Can I use my own model?* Yes: `ai_query` takes any model in `system.ai` or your own serving endpoint (`llm_model` widget).
# MAGIC * *Why are my numbers different?* LLM outputs are non-deterministic; the story holds, exact values move a little.
# MAGIC
# MAGIC ### Call to action (closing line)
# MAGIC
# MAGIC > Clone the three notebooks, point the widgets at your catalog and **Run all**. Then replace `feedback_raw` with your own tickets and see what your developers are telling you.