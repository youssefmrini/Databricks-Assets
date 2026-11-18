# Databricks notebook source
# DBTITLE 1,Intro
# MAGIC %md
# MAGIC # NetSentinel - Network Threat Hunting with Databricks IP Functions
# MAGIC
# MAGIC **Story:** Nimbus runs a payments API. Its security team gets 5M API requests a week, mixed IPv4/IPv6, from an edge proxy that logs IPs as messy strings. They need to enrich every request with GeoIP/ASN and threat intel, enforce API key IP allowlists, and catch card-testing bots - in minutes, in SQL, in the lakehouse.
# MAGIC
# MAGIC **What we use:** the native, Photon-accelerated [IP functions](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-ip-functions) (GA on DBR 18 LTS+ and serverless) - see the [launch blog](https://www.databricks.com/blog/ip-functions-are-generally-available-bringing-high-performance-network-analytics-lakehouse).
# MAGIC
# MAGIC | Step | IP functions showcased |
# MAGIC | --- | --- |
# MAGIC | 1. IP functions in 60 seconds | all 14 |
# MAGIC | 2. Clean messy logs | `try_ip_host`, `ip_version`, `ip_as_binary`, `ip_cidr` |
# MAGIC | 3. Canonicalize feeds | `try_ip_cidr`, `ip_network_first/last`, `ip_prefix_length` |
# MAGIC | 4. GeoIP / ASN range join | `ip_cidr_contains` on BINARY |
# MAGIC | 5. Threat intel join + hunting | `ip_cidr_contains` (IP in CIDR) |
# MAGIC | 6. API key allowlist | `ip_cidr_contains` anti join |
# MAGIC | 7. Card-testing detection | `ip_cidr` subnets, CIDR-in-CIDR |
# MAGIC | 8. Legacy vs native | IPv6 blind spot |
# MAGIC | 9. Gold incidents + AI brief | `ip_as_string`, `ai_gen` |
# MAGIC | 10. WAF blocklist export | `ip_network`, `ip_network_last` |
# MAGIC
# MAGIC Prereq: run `netsentinel_01_data_generator` once.

# COMMAND ----------

# DBTITLE 1,Config
dbutils.widgets.text("catalog", "youssef_demo")
dbutils.widgets.text("schema", "ip_functions_devrel")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

spark.sql(f"USE CATALOG `{catalog}`")
spark.sql(f"USE SCHEMA `{schema}`")
print(f"Using {catalog}.{schema}")

# COMMAND ----------

# DBTITLE 1,1. IP functions in 60 seconds
# MAGIC %md
# MAGIC ## 1. IP functions in 60 seconds
# MAGIC One row, every function. IPv4 and IPv6 behave the same way.

# COMMAND ----------

# DBTITLE 1,All 14 IP functions on literals
# MAGIC %sql
# MAGIC SELECT
# MAGIC   -- canonicalize
# MAGIC   ip_host('2001:0DB8:0000:0000:0000:0000:0000:0001')  AS ip_host,            -- 2001:db8::1
# MAGIC   ip_cidr('192.168.1.77/24')                          AS ip_cidr,            -- 192.168.1.0/24
# MAGIC   -- inspect a CIDR
# MAGIC   ip_network('10.20.0.0/14')                          AS ip_network,         -- 10.20.0.0
# MAGIC   ip_network_first('2001:db8::/32')                   AS ip_network_first,   -- 2001:db8::
# MAGIC   ip_network_last('192.168.1.0/24')                   AS ip_network_last,    -- 192.168.1.255
# MAGIC   ip_prefix_length('10.0.0.0/8')                      AS ip_prefix_length,   -- 8
# MAGIC   ip_version('2001:db8::/32')                         AS ip_version,         -- 6
# MAGIC   -- containment (IP in CIDR, CIDR in CIDR)
# MAGIC   ip_cidr_contains('10.0.0.0/8', '10.1.2.3')          AS ip_in_cidr,         -- true
# MAGIC   ip_cidr_contains('10.0.0.0/8', '10.9.0.0/16')       AS cidr_in_cidr,       -- true
# MAGIC   -- compact binary: 4 bytes (v4) / 16 bytes (v6)
# MAGIC   hex(ip_as_binary('192.168.1.5'))                    AS ip_as_binary,       -- C0A80105
# MAGIC   ip_as_string(ip_as_binary('2001:db8::/32'))         AS ip_as_string,       -- 2001:db8::/32
# MAGIC   -- safe variants: NULL instead of an error
# MAGIC   try_ip_host('999.1.1.1')                            AS try_ip_host,        -- NULL
# MAGIC   try_ip_cidr('10.0.0.0/33')                          AS try_ip_cidr,        -- NULL
# MAGIC   try_ip_as_binary('unknown')                         AS try_ip_as_binary,   -- NULL
# MAGIC   try_ip_as_string(X'0102')                           AS try_ip_as_string    -- NULL

# COMMAND ----------

# DBTITLE 1,2. Clean messy logs
# MAGIC %md
# MAGIC ## 2. Bronze -> Silver: clean messy API logs
# MAGIC The edge proxy logs `src_ip` as a raw string: compressed IPv6, expanded upper-case IPv6, and junk (`unknown`, `-`, `999.10.2.1`, `052.1.2.3`).

# COMMAND ----------

# DBTITLE 1,Profile raw src_ip
# MAGIC %sql
# MAGIC SELECT
# MAGIC   CASE WHEN try_ip_host(src_ip) IS NULL       THEN 'invalid'
# MAGIC        WHEN src_ip <> try_ip_host(src_ip)     THEN concat('IPv', ip_version(try_ip_host(src_ip)), ' non-canonical')
# MAGIC        ELSE concat('IPv', ip_version(src_ip), ' canonical') END AS src_ip_shape,
# MAGIC   count(*)                                                         AS requests,
# MAGIC   any_value(src_ip)                                                AS raw_example,
# MAGIC   any_value(try_ip_host(src_ip))                                   AS canonical_example
# MAGIC FROM api_logs_raw
# MAGIC GROUP BY ALL
# MAGIC ORDER BY requests DESC

# COMMAND ----------

# DBTITLE 1,Build api_logs_silver + quarantine
# MAGIC %sql
# MAGIC -- Canonical IP string, IP version, 4/16-byte binary key and the /24 (v4) or /64 (v6) subnet
# MAGIC CREATE OR REPLACE TABLE api_logs_silver
# MAGIC COMMENT 'Nimbus API logs with canonical IP (ip), version, BINARY join key (ip_bin) and subnet (/24 v4, /64 v6).'
# MAGIC AS
# MAGIC WITH parsed AS (
# MAGIC   SELECT *, try_ip_host(src_ip) AS ip
# MAGIC   FROM api_logs_raw
# MAGIC )
# MAGIC SELECT
# MAGIC   event_ts, request_id, merchant_id, api_key_id, endpoint, http_status,
# MAGIC   user_agent, card_fingerprint, latency_ms,
# MAGIC   src_ip                                                                         AS src_ip_raw,
# MAGIC   ip,
# MAGIC   ip_version(ip)                                                                 AS ip_version,
# MAGIC   ip_as_binary(ip)                                                               AS ip_bin,
# MAGIC   ip_cidr(concat(ip, CASE WHEN ip_version(ip) = 4 THEN '/24' ELSE '/64' END))   AS subnet
# MAGIC FROM parsed
# MAGIC WHERE ip IS NOT NULL;
# MAGIC
# MAGIC -- Bad rows are kept, not silently dropped
# MAGIC CREATE OR REPLACE TABLE api_logs_quarantine
# MAGIC COMMENT 'API log rows whose src_ip is not a valid IP address.'
# MAGIC AS
# MAGIC SELECT * FROM api_logs_raw WHERE try_ip_host(src_ip) IS NULL;

# COMMAND ----------

# DBTITLE 1,Silver summary: rows, versions, binary savings
# MAGIC %sql
# MAGIC SELECT
# MAGIC   (SELECT count(*) FROM api_logs_raw)                      AS raw_rows,
# MAGIC   count(*)                                                  AS silver_rows,
# MAGIC   (SELECT count(*) FROM api_logs_quarantine)               AS quarantined_rows,
# MAGIC   count_if(ip_version = 4)                                  AS ipv4_rows,
# MAGIC   count_if(ip_version = 6)                                  AS ipv6_rows,
# MAGIC   round(sum(length(src_ip_raw)) / 1024 / 1024, 1)           AS raw_string_mb,
# MAGIC   round(sum(octet_length(ip_bin)) / 1024 / 1024, 1)         AS binary_mb
# MAGIC FROM api_logs_silver

# COMMAND ----------

# DBTITLE 1,3. Canonicalize feeds
# MAGIC %md
# MAGIC ## 3. Canonicalize reference feeds
# MAGIC GeoIP, threat intel and merchant allowlists all arrive as CIDR strings - some with host bits set, some expanded IPv6, some broken. `try_ip_cidr` fixes or rejects each one.

# COMMAND ----------

# DBTITLE 1,Build geoip_blocks, threat_intel, merchant_allowlist
# MAGIC %sql
# MAGIC -- GeoIP / ASN
# MAGIC CREATE OR REPLACE TABLE geoip_blocks
# MAGIC COMMENT 'Canonical GeoIP + ASN blocks with BINARY join key and range bounds.'
# MAGIC AS
# MAGIC WITH c AS (
# MAGIC   SELECT *, try_ip_cidr(network) AS cidr FROM geoip_blocks_raw
# MAGIC )
# MAGIC SELECT
# MAGIC   cidr                     AS network,
# MAGIC   ip_as_binary(cidr)       AS network_bin,
# MAGIC   ip_network_first(cidr)   AS first_ip,
# MAGIC   ip_network_last(cidr)    AS last_ip,
# MAGIC   ip_prefix_length(cidr)   AS prefix_length,
# MAGIC   ip_version(cidr)         AS ip_version,
# MAGIC   country_code, country, city, asn, asn_org,
# MAGIC   network                  AS network_raw
# MAGIC FROM c
# MAGIC WHERE cidr IS NOT NULL;
# MAGIC
# MAGIC -- Threat intel (valid indicators)
# MAGIC CREATE OR REPLACE TABLE threat_intel
# MAGIC COMMENT 'Canonical threat intel indicators (CIDR) with BINARY join key.'
# MAGIC AS
# MAGIC WITH c AS (
# MAGIC   SELECT *, try_ip_cidr(indicator) AS cidr FROM threat_intel_raw
# MAGIC )
# MAGIC SELECT
# MAGIC   cidr                     AS indicator,
# MAGIC   ip_as_binary(cidr)       AS indicator_bin,
# MAGIC   ip_prefix_length(cidr)   AS prefix_length,
# MAGIC   ip_version(cidr)         AS ip_version,
# MAGIC   category, confidence, provider,
# MAGIC   indicator                AS indicator_raw
# MAGIC FROM c
# MAGIC WHERE cidr IS NOT NULL;
# MAGIC
# MAGIC -- Threat intel (rejected indicators)
# MAGIC CREATE OR REPLACE TABLE threat_intel_rejected
# MAGIC COMMENT 'Threat intel rows whose indicator is not a valid CIDR.'
# MAGIC AS
# MAGIC SELECT * FROM threat_intel_raw WHERE try_ip_cidr(indicator) IS NULL;
# MAGIC
# MAGIC -- Merchant server-key allowlists
# MAGIC CREATE OR REPLACE TABLE merchant_allowlist
# MAGIC COMMENT 'Canonical per-merchant allowlisted CIDRs for sk_live keys.'
# MAGIC AS
# MAGIC SELECT
# MAGIC   merchant_id, merchant_name,
# MAGIC   ip_cidr(allowed_cidr)                    AS allowed_cidr,
# MAGIC   ip_as_binary(ip_cidr(allowed_cidr))      AS allowed_cidr_bin,
# MAGIC   ip_version(allowed_cidr)                 AS ip_version,
# MAGIC   allowed_cidr                             AS allowed_cidr_raw
# MAGIC FROM merchant_allowlist_raw;

# COMMAND ----------

# DBTITLE 1,What try_ip_cidr fixed and rejected
# MAGIC %sql
# MAGIC SELECT 'geoip_blocks' AS feed, 'canonicalized' AS outcome, count(*) AS rows, any_value(network_raw) AS raw_example, any_value(network) AS canonical_example
# MAGIC FROM geoip_blocks WHERE network_raw <> network
# MAGIC UNION ALL
# MAGIC SELECT 'threat_intel', 'canonicalized', count(*), any_value(indicator_raw), any_value(indicator)
# MAGIC FROM threat_intel WHERE indicator_raw <> indicator
# MAGIC UNION ALL
# MAGIC SELECT 'merchant_allowlist', 'canonicalized', count(*), any_value(allowed_cidr_raw), any_value(allowed_cidr)
# MAGIC FROM merchant_allowlist WHERE allowed_cidr_raw <> allowed_cidr
# MAGIC UNION ALL
# MAGIC SELECT 'threat_intel', 'rejected', count(*), array_join(collect_list(concat('"', indicator, '"')), ', '), NULL
# MAGIC FROM threat_intel_rejected

# COMMAND ----------

# DBTITLE 1,4. GeoIP range join
# MAGIC %md
# MAGIC ## 4. GeoIP / ASN enrichment: the CIDR range join
# MAGIC ~4.9M requests x ~37K CIDR blocks. `ip_cidr_contains` on BINARY keys triggers the Photon-optimized IP range join - no exploding ranges, no integer math, IPv4 and IPv6 in one query.

# COMMAND ----------

# DBTITLE 1,Build api_logs_enriched
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE api_logs_enriched
# MAGIC COMMENT 'API logs enriched with GeoIP country/city and ASN via ip_cidr_contains range join.'
# MAGIC AS
# MAGIC SELECT
# MAGIC   l.*,
# MAGIC   g.network       AS geo_network,
# MAGIC   g.country_code,
# MAGIC   g.country,
# MAGIC   g.city,
# MAGIC   g.asn,
# MAGIC   g.asn_org
# MAGIC FROM api_logs_silver l
# MAGIC LEFT JOIN geoip_blocks g
# MAGIC   ON ip_cidr_contains(g.network_bin, l.ip_bin)

# COMMAND ----------

# DBTITLE 1,Enrichment coverage by IP version
# MAGIC %sql
# MAGIC SELECT
# MAGIC   ip_version,
# MAGIC   count(*)                                         AS requests,
# MAGIC   count_if(country IS NOT NULL)                    AS geo_matched,
# MAGIC   round(100 * count_if(country IS NOT NULL) / count(*), 2) AS match_pct,
# MAGIC   count(DISTINCT country)                          AS countries,
# MAGIC   count(DISTINCT asn)                              AS asns
# MAGIC FROM api_logs_enriched
# MAGIC GROUP BY ip_version
# MAGIC ORDER BY ip_version

# COMMAND ----------

# DBTITLE 1,5. Threat intel
# MAGIC %md
# MAGIC ## 5. Threat intel join + interactive hunting
# MAGIC Same pattern: every request vs every threat indicator (/32s, /24s, /16s, IPv6 /64s).

# COMMAND ----------

# DBTITLE 1,Build threat_hits
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE threat_hits
# MAGIC COMMENT 'API requests whose source IP falls inside a threat intel indicator.'
# MAGIC AS
# MAGIC SELECT
# MAGIC   e.request_id, e.event_ts, e.ip, e.ip_version, e.subnet,
# MAGIC   e.merchant_id, e.api_key_id, e.endpoint, e.http_status,
# MAGIC   e.country, e.asn_org,
# MAGIC   t.indicator, t.category, t.confidence, t.provider
# MAGIC FROM api_logs_enriched e
# MAGIC JOIN threat_intel t
# MAGIC   ON ip_cidr_contains(t.indicator_bin, e.ip_bin)

# COMMAND ----------

# DBTITLE 1,Threat hits by category
# MAGIC %sql
# MAGIC SELECT
# MAGIC   category,
# MAGIC   max(confidence)              AS max_confidence,
# MAGIC   count(*)                     AS requests,
# MAGIC   count(DISTINCT ip)           AS distinct_ips,
# MAGIC   count(DISTINCT indicator)    AS indicators_hit,
# MAGIC   collect_set(ip_version)      AS ip_versions,
# MAGIC   mode(endpoint)               AS top_endpoint,
# MAGIC   min(event_ts)                AS first_seen,
# MAGIC   max(event_ts)                AS last_seen
# MAGIC FROM threat_hits
# MAGIC GROUP BY category
# MAGIC ORDER BY max_confidence DESC

# COMMAND ----------

# DBTITLE 1,Hunt: every connection from a suspicious /16, last 24h
# MAGIC %sql
# MAGIC -- The analyst gets a tip: "look at 45.7.0.0/16". One predicate, no regex, no parsing.
# MAGIC SELECT
# MAGIC   endpoint,
# MAGIC   http_status,
# MAGIC   count(*)               AS requests,
# MAGIC   count(DISTINCT ip)     AS distinct_ips,
# MAGIC   any_value(user_agent)  AS user_agent,
# MAGIC   any_value(asn_org)     AS asn_org
# MAGIC FROM api_logs_enriched
# MAGIC WHERE ip_cidr_contains('45.7.0.0/16', ip)
# MAGIC   AND event_ts >= current_timestamp() - INTERVAL 24 HOURS
# MAGIC GROUP BY ALL
# MAGIC ORDER BY requests DESC

# COMMAND ----------

# DBTITLE 1,6. Allowlist
# MAGIC %md
# MAGIC ## 6. API key IP allowlist enforcement
# MAGIC Server-side `sk_live_` keys must only be used from the merchant's allowlisted CIDRs. Anti join on merchant + `ip_cidr_contains`.

# COMMAND ----------

# DBTITLE 1,Build allowlist_violations
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE allowlist_violations
# MAGIC COMMENT 'sk_live API key requests from a source IP outside the merchant allowlist.'
# MAGIC AS
# MAGIC SELECT e.*
# MAGIC FROM api_logs_enriched e
# MAGIC LEFT ANTI JOIN merchant_allowlist a
# MAGIC   ON a.merchant_id = e.merchant_id
# MAGIC  AND ip_cidr_contains(a.allowed_cidr_bin, e.ip_bin)
# MAGIC WHERE e.api_key_id LIKE 'sk_live_%'

# COMMAND ----------

# DBTITLE 1,Allowlist violations by key and subnet
# MAGIC %sql
# MAGIC SELECT
# MAGIC   merchant_id,
# MAGIC   api_key_id,
# MAGIC   subnet,
# MAGIC   ip_version,
# MAGIC   count(*)                             AS requests,
# MAGIC   count_if(endpoint = 'POST /v1/payouts') AS payout_calls,
# MAGIC   any_value(country)                   AS country,
# MAGIC   any_value(asn_org)                   AS asn_org,
# MAGIC   any_value(user_agent)                AS user_agent
# MAGIC FROM allowlist_violations
# MAGIC GROUP BY ALL
# MAGIC ORDER BY requests DESC
# MAGIC LIMIT 10

# COMMAND ----------

# DBTITLE 1,7. Card testing
# MAGIC %md
# MAGIC ## 7. Behavioral detection: card testing by subnet
# MAGIC Bots rotate IPs inside a subnet. Aggregate on the `/24` (`/64` for IPv6) built with `ip_cidr`, then check each suspicious subnet against the threat feed with **CIDR-in-CIDR** containment.

# COMMAND ----------

# DBTITLE 1,Build card_testing_subnets
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE card_testing_subnets
# MAGIC COMMENT 'Subnets with card-testing behaviour in the last 24h (high volume, many cards, mostly declined).'
# MAGIC AS
# MAGIC WITH s AS (
# MAGIC   SELECT
# MAGIC     subnet,
# MAGIC     ip_version,
# MAGIC     count(*)                                   AS requests,
# MAGIC     count(DISTINCT card_fingerprint)           AS distinct_cards,
# MAGIC     count(DISTINCT ip)                         AS distinct_ips,
# MAGIC     avg(CASE WHEN http_status = 402 THEN 1 ELSE 0 END) AS decline_rate,
# MAGIC     collect_set(merchant_id)                   AS merchant_ids,
# MAGIC     any_value(country)                         AS country,
# MAGIC     any_value(asn_org)                         AS asn_org,
# MAGIC     min(event_ts)                              AS first_seen,
# MAGIC     max(event_ts)                              AS last_seen
# MAGIC   FROM api_logs_enriched
# MAGIC   WHERE endpoint = 'POST /v1/payment_intents/confirm'
# MAGIC     AND event_ts >= current_timestamp() - INTERVAL 24 HOURS
# MAGIC   GROUP BY subnet, ip_version
# MAGIC   HAVING count(*) >= 500 AND avg(CASE WHEN http_status = 402 THEN 1 ELSE 0 END) >= 0.5
# MAGIC ),
# MAGIC feed AS (
# MAGIC   -- CIDR-in-CIDR: is the whole subnet covered by a known indicator?
# MAGIC   SELECT s.subnet, max_by(t.category, t.confidence) AS feed_category
# MAGIC   FROM s JOIN threat_intel t ON ip_cidr_contains(t.indicator, s.subnet)
# MAGIC   GROUP BY s.subnet
# MAGIC )
# MAGIC SELECT s.*, feed.feed_category, feed.feed_category IS NOT NULL AS in_threat_feed
# MAGIC FROM s LEFT JOIN feed USING (subnet)

# COMMAND ----------

# DBTITLE 1,Card-testing subnets
# MAGIC %sql
# MAGIC SELECT subnet, requests, distinct_cards, distinct_ips, round(decline_rate * 100, 1) AS decline_pct,
# MAGIC        in_threat_feed, feed_category, country, asn_org, merchant_ids
# MAGIC FROM card_testing_subnets
# MAGIC ORDER BY requests DESC

# COMMAND ----------

# DBTITLE 1,8. Legacy vs native
# MAGIC %md
# MAGIC ## 8. Why native? The legacy IPv4-only blind spot
# MAGIC The classic workaround - `split` the string into 4 octets and compare integers - silently drops every IPv6 request. Here is what that pipeline would never see.

# COMMAND ----------

# DBTITLE 1,What a legacy IPv4-only pipeline misses
# MAGIC %sql
# MAGIC WITH legacy AS (
# MAGIC   -- the old way: regex + split + integer math, IPv4 only
# MAGIC   SELECT
# MAGIC     request_id,
# MAGIC     CASE WHEN src_ip_raw RLIKE '^[0-9]{1,3}[.][0-9]{1,3}[.][0-9]{1,3}[.][0-9]{1,3}$'
# MAGIC          THEN cast(split(src_ip_raw, '[.]')[0] AS BIGINT) * 16777216
# MAGIC             + cast(split(src_ip_raw, '[.]')[1] AS BIGINT) * 65536
# MAGIC             + cast(split(src_ip_raw, '[.]')[2] AS BIGINT) * 256
# MAGIC             + cast(split(src_ip_raw, '[.]')[3] AS BIGINT) END AS ip_int
# MAGIC   FROM api_logs_silver
# MAGIC )
# MAGIC SELECT 'All API requests' AS dataset, count(*) AS native_rows, count(l.ip_int) AS legacy_rows
# MAGIC FROM api_logs_silver s JOIN legacy l USING (request_id)
# MAGIC UNION ALL
# MAGIC SELECT concat('Threat hits: ', category), count(*), count_if(ip_version = 4)
# MAGIC FROM threat_hits GROUP BY category
# MAGIC UNION ALL
# MAGIC SELECT 'Allowlist violations', count(*), count_if(ip_version = 4)
# MAGIC FROM allowlist_violations
# MAGIC UNION ALL
# MAGIC SELECT 'Payouts from outside allowlist', count_if(endpoint = 'POST /v1/payouts'), count_if(endpoint = 'POST /v1/payouts' AND ip_version = 4)
# MAGIC FROM allowlist_violations

# COMMAND ----------

# DBTITLE 1,9. Gold incidents
# MAGIC %md
# MAGIC ## 9. Gold: security incidents + AI-written brief
# MAGIC One row per incident, indicator stored as compact BINARY and rendered back with `ip_as_string`. `ai_gen` drafts the SOC brief.

# COMMAND ----------

# DBTITLE 1,Build security_incidents
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE security_incidents
# MAGIC COMMENT 'Gold: one row per detected network security incident.'
# MAGIC AS
# MAGIC WITH incidents AS (
# MAGIC   -- A) card testing (behavioral)
# MAGIC   SELECT 'card_testing' AS incident_type, 'critical' AS severity,
# MAGIC          ip_as_binary(subnet) AS indicator_bin, ip_version,
# MAGIC          array_join(merchant_ids, ',') AS merchant_ids, requests, distinct_ips, first_seen, last_seen, country, asn_org,
# MAGIC          concat(distinct_cards, ' distinct cards, ', round(decline_rate * 100, 1), '% declined, in threat feed: ', in_threat_feed) AS evidence
# MAGIC   FROM card_testing_subnets
# MAGIC   UNION ALL
# MAGIC   -- B) leaked server key: sustained traffic from a subnet outside the allowlist
# MAGIC   SELECT 'leaked_api_key', CASE WHEN count_if(endpoint = 'POST /v1/payouts') > 0 THEN 'critical' ELSE 'high' END,
# MAGIC          ip_as_binary(subnet), ip_version,
# MAGIC          merchant_id, count(*), count(DISTINCT ip), min(event_ts), max(event_ts), any_value(country), any_value(asn_org),
# MAGIC          concat(any_value(api_key_id), ' used outside allowlist, ', count_if(endpoint = 'POST /v1/payouts'), ' payout calls')
# MAGIC   FROM allowlist_violations
# MAGIC   GROUP BY merchant_id, subnet, ip_version
# MAGIC   HAVING count(*) >= 100
# MAGIC   UNION ALL
# MAGIC   -- C) credential stuffing from Tor exits
# MAGIC   SELECT 'credential_stuffing', 'high', NULL, ip_version,
# MAGIC          array_join(slice(array_sort(collect_set(merchant_id)), 1, 5), ',') || ',...', count(*), count(DISTINCT ip),
# MAGIC          min(event_ts), max(event_ts), mode(country), mode(asn_org),
# MAGIC          concat(count(DISTINCT ip), ' Tor exit IPs, ', round(100 * avg(CASE WHEN http_status = 401 THEN 1 ELSE 0 END), 1), '% failed logins')
# MAGIC   FROM threat_hits
# MAGIC   WHERE category = 'tor_exit' AND endpoint = 'POST /v1/auth/token'
# MAGIC   GROUP BY ip_version
# MAGIC   UNION ALL
# MAGIC   -- D) scanner
# MAGIC   SELECT 'scanner', 'medium', ip_as_binary(indicator), ip_version, NULL, count(*), count(DISTINCT ip),
# MAGIC          min(event_ts), max(event_ts), any_value(country), any_value(asn_org),
# MAGIC          concat(count(DISTINCT endpoint), ' probe paths, top: ', mode(endpoint))
# MAGIC   FROM threat_hits
# MAGIC   WHERE category = 'scanner'
# MAGIC   GROUP BY indicator, ip_version
# MAGIC )
# MAGIC SELECT
# MAGIC   concat('INC-', lpad(cast(row_number() OVER (ORDER BY CASE severity WHEN 'critical' THEN 1 WHEN 'high' THEN 2 ELSE 3 END, requests DESC) AS STRING), 4, '0')) AS incident_id,
# MAGIC   incident_type,
# MAGIC   severity,
# MAGIC   coalesce(try_ip_as_string(indicator_bin), 'feed:tor_exit') AS indicator,
# MAGIC   indicator_bin,
# MAGIC   ip_version, merchant_ids, requests, distinct_ips, first_seen, last_seen, country, asn_org, evidence
# MAGIC FROM incidents

# COMMAND ----------

# DBTITLE 1,Security incidents
# MAGIC %sql
# MAGIC SELECT incident_id, incident_type, severity, indicator, hex(indicator_bin) AS indicator_hex, ip_version,
# MAGIC        merchant_ids, requests, distinct_ips, country, asn_org, evidence, first_seen, last_seen
# MAGIC FROM security_incidents
# MAGIC ORDER BY incident_id

# COMMAND ----------

# DBTITLE 1,AI incident brief with ai_gen
# MAGIC %sql
# MAGIC SELECT
# MAGIC   incident_id,
# MAGIC   incident_type,
# MAGIC   indicator,
# MAGIC   ai_gen(concat(
# MAGIC     'You are a SOC analyst at Nimbus, a payments API. Write a 3-sentence incident brief: ',
# MAGIC     'what happened, business impact, and the immediate containment action. Be specific, no preamble. Incident: ',
# MAGIC     to_json(struct(incident_type, severity, indicator, ip_version, merchant_ids, requests, distinct_ips,
# MAGIC                    country, asn_org, evidence, first_seen, last_seen))
# MAGIC   )) AS ai_brief
# MAGIC FROM security_incidents
# MAGIC ORDER BY incident_id

# COMMAND ----------

# DBTITLE 1,10. WAF blocklist
# MAGIC %md
# MAGIC ## 10. Act: export a WAF blocklist
# MAGIC Firewalls want ranges. `ip_network` / `ip_network_last` turn each CIDR into start/end addresses; Tor exits are added as /32s.

# COMMAND ----------

# DBTITLE 1,Build waf_blocklist
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE waf_blocklist
# MAGIC COMMENT 'CIDRs to push to the edge WAF, with range bounds.'
# MAGIC AS
# MAGIC WITH cidrs AS (
# MAGIC   SELECT indicator AS cidr, incident_id AS reason FROM security_incidents WHERE indicator_bin IS NOT NULL
# MAGIC   UNION ALL
# MAGIC   SELECT DISTINCT ip_cidr(concat(ip, '/32')), 'INC tor_exit' FROM threat_hits WHERE category = 'tor_exit'
# MAGIC )
# MAGIC SELECT
# MAGIC   cidr,
# MAGIC   ip_network(cidr)        AS range_start,
# MAGIC   ip_network_last(cidr)   AS range_end,
# MAGIC   ip_prefix_length(cidr)  AS prefix_length,
# MAGIC   ip_version(cidr)        AS ip_version,
# MAGIC   reason
# MAGIC FROM cidrs;
# MAGIC
# MAGIC SELECT ip_version, prefix_length, count(*) AS rules, any_value(cidr) AS example_cidr,
# MAGIC        any_value(range_start) AS example_start, any_value(range_end) AS example_end
# MAGIC FROM waf_blocklist
# MAGIC GROUP BY ALL
# MAGIC ORDER BY ip_version, prefix_length;

# COMMAND ----------

# DBTITLE 1,Validate: every planted attack detected
inc = spark.table("security_incidents").toPandas()

expected = {
    ("card_testing", "185.3.17.0/24"),
    ("card_testing", "185.3.18.0/24"),
    ("leaked_api_key", "2001:db8:3e7:beef::/64"),
    ("credential_stuffing", "feed:tor_exit"),
    ("scanner", "45.7.0.0/16"),
}
found = set(zip(inc.incident_type, inc.indicator))

missing = expected - found
unexpected = found - expected
print(f"Incidents: {len(inc)} | missing: {missing or 'none'} | unexpected: {unexpected or 'none'}")
assert not missing, f"Missed planted attacks: {missing}"

# COMMAND ----------

# DBTITLE 1,Presenter guide
# MAGIC %md
# MAGIC ## Presenter guide (DevRel video, ~8 min)
# MAGIC
# MAGIC **Hook (0:00-0:30)**
# MAGIC > "Every packet, login and payment has an IP address. Until now, analyzing them in SQL meant regex, integer math, and pretending IPv6 doesn't exist. Today: native, Photon-accelerated IP functions, GA on Databricks. Let's catch four attackers hitting a payments API - in pure SQL."
# MAGIC
# MAGIC **Beats**
# MAGIC
# MAGIC | # | Cell | Say | Wow moment |
# MAGIC | --- | --- | --- | --- |
# MAGIC | 1 | IP functions in 60 seconds | "14 functions, one row, IPv4 and IPv6 identical." | `cidr_in_cidr`, `try_*` return NULL not errors |
# MAGIC | 2 | Profile raw src_ip / silver | "Real logs are messy." | 175K expanded IPv6 canonicalized, 14K junk rows quarantined, BINARY key ~2.3x smaller than raw strings |
# MAGIC | 3 | What try_ip_cidr fixed | "Feeds are messy too." | 1,738 GeoIP blocks with host bits fixed, 5 broken indicators rejected |
# MAGIC | 4 | GeoIP range join | "4.9M requests x 37K CIDRs, one ON clause." | 100% matched, v4 and v6; point at runtime |
# MAGIC | 5 | Threat hits + /16 hunt | "The analyst's tip: 45.7.0.0/16." | `WHERE ip_cidr_contains('45.7.0.0/16', ip)` - zgrab probing `/.env` |
# MAGIC | 6 | Allowlist violations | "Server keys only from allowlisted CIDRs." | `sk_live_mer_0117` - 8,000 calls, ~4.8K payouts, from an IPv6 VPN |
# MAGIC | 7 | Card-testing subnets | "Threat intel knew one botnet subnet. Behavior found its twin." | `185.3.18.0/24`: 30K cards, 90% declined, `in_threat_feed = false` |
# MAGIC | 8 | Legacy vs native | "What would your old pipeline miss?" | IPv4-only: 0 of 8,000 VPN hits, 60 of 4,819 payouts |
# MAGIC | 9 | Incidents + ai_gen | "From rows to a SOC brief." | 5 incidents, AI-written briefs |
# MAGIC | 10 | WAF blocklist | "Close the loop." | ranges via `ip_network` / `ip_network_last` |
# MAGIC
# MAGIC **Perf line (from the [blog](https://www.databricks.com/blog/ip-functions-are-generally-available-bringing-high-performance-network-analytics-lakehouse)):** IP CIDR joins up to 3.1x faster and up to 6.4x cheaper than another leading cloud data warehouse; scales to 10B IPs x 10M blocks.
# MAGIC
# MAGIC **Close (CTA)**
# MAGIC > "Store IPs as BINARY, join with `ip_cidr_contains`, use `try_` on raw data. Available now on DBR 18 LTS+ and serverless. Docs and this notebook in the description."
# MAGIC
# MAGIC **Before recording:** rerun `netsentinel_01_data_generator` (attack timestamps are relative to now, so the 24h windows stay populated).

# COMMAND ----------

