# Databricks notebook source
# DBTITLE 1,About
# MAGIC %md
# MAGIC # NetSentinel 01 - Synthetic Data Generator
# MAGIC
# MAGIC Generates the data for the **NetSentinel** demo: the security team of **Nimbus** (fictional payments API) hunting attacks with Databricks **IP functions** (GA, DBR 18 LTS+ / serverless).
# MAGIC
# MAGIC | Table | Rows | What it is |
# MAGIC | --- | --- | --- |
# MAGIC | `geoip_blocks_raw` | ~37K | GeoIP + ASN feed, IPv4 /16 and /24 + IPv6 /48 (some non-canonical) |
# MAGIC | `threat_intel_raw` | ~2K | Threat feed: botnet, Tor exits, scanner, VPN, spam (non-canonical + malformed rows) |
# MAGIC | `merchant_allowlist_raw` | ~240 | Server-side API key allowlisted CIDRs per merchant |
# MAGIC | `api_logs_raw` | ~4.93M | 7 days of API access logs, mixed IPv4/IPv6, messy IP strings |
# MAGIC
# MAGIC Planted attacks (what the pipeline must find):
# MAGIC * **Card testing** - botnet in `185.3.17.0/24` (in threat feed) and `185.3.18.0/24` (NOT in feed), last 6h
# MAGIC * **Credential stuffing** - 300 Tor exit IPs on `/v1/auth/token`, last 48h
# MAGIC * **Leaked API key** - `sk_live_mer_0117` calling `/v1/payouts` from IPv6 `2001:db8:3e7:beef::/64`, outside allowlist
# MAGIC * **Scanner** - `45.7.0.0/16` probing admin paths, last 20h

# COMMAND ----------

# DBTITLE 1,Config
dbutils.widgets.text("catalog", "youssef_demo")
dbutils.widgets.text("schema", "ip_functions_devrel")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")
spark.sql(f"USE CATALOG `{catalog}`")
spark.sql(f"USE SCHEMA `{schema}`")
print(f"Using {catalog}.{schema}")

# COMMAND ----------

# DBTITLE 1,Country / ASN reference
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW geo_ref AS
# MAGIC SELECT * FROM VALUES
# MAGIC   (0,  23,  'US', 'United States',  array('New York', 'Chicago', 'Dallas')),
# MAGIC   (1,  31,  'NL', 'Netherlands',    array('Amsterdam', 'Rotterdam', 'Utrecht')),
# MAGIC   (2,  41,  'ZA', 'South Africa',   array('Johannesburg', 'Cape Town', 'Durban')),
# MAGIC   (3,  45,  'BR', 'Brazil',         array('Sao Paulo', 'Rio de Janeiro', 'Curitiba')),
# MAGIC   (4,  52,  'US', 'United States',  array('Ashburn', 'Seattle', 'San Jose')),
# MAGIC   (5,  61,  'AU', 'Australia',      array('Sydney', 'Melbourne', 'Brisbane')),
# MAGIC   (6,  77,  'FR', 'France',         array('Paris', 'Lyon', 'Marseille')),
# MAGIC   (7,  81,  'GB', 'United Kingdom', array('London', 'Manchester', 'Leeds')),
# MAGIC   (8,  89,  'ES', 'Spain',          array('Madrid', 'Barcelona', 'Valencia')),
# MAGIC   (9,  95,  'SE', 'Sweden',         array('Stockholm', 'Gothenburg', 'Malmo')),
# MAGIC   (10, 102, 'IN', 'India',          array('Mumbai', 'Bengaluru', 'Delhi')),
# MAGIC   (11, 103, 'SG', 'Singapore',      array('Singapore', 'Jurong', 'Woodlands')),
# MAGIC   (12, 110, 'JP', 'Japan',          array('Tokyo', 'Osaka', 'Nagoya')),
# MAGIC   (13, 151, 'IT', 'Italy',          array('Milan', 'Rome', 'Turin')),
# MAGIC   (14, 177, 'MX', 'Mexico',         array('Mexico City', 'Monterrey', 'Guadalajara')),
# MAGIC   (15, 185, 'DE', 'Germany',        array('Frankfurt', 'Berlin', 'Munich')),
# MAGIC   (16, 196, 'NG', 'Nigeria',        array('Lagos', 'Abuja', 'Ibadan')),
# MAGIC   (17, 200, 'AR', 'Argentina',      array('Buenos Aires', 'Cordoba', 'Rosario')),
# MAGIC   (18, 203, 'KR', 'South Korea',    array('Seoul', 'Busan', 'Incheon')),
# MAGIC   (19, 213, 'PL', 'Poland',         array('Warsaw', 'Krakow', 'Gdansk'))
# MAGIC AS t(idx, o1, country_code, country, cities)

# COMMAND ----------

# DBTITLE 1,GeoIP / ASN feed
# MAGIC %sql
# MAGIC -- IPv4: o2 0-6 -> 256 x /24 blocks per /16, o2 7-9 -> one /16 block. IPv6: 1,000 x /48.
# MAGIC -- ~2% of IPv4 rows are written non-canonically (host bits set), like many real feeds.
# MAGIC CREATE OR REPLACE TABLE geoip_blocks_raw
# MAGIC COMMENT 'Raw GeoIP + ASN feed (CIDR -> country, city, ASN). Mixed IPv4/IPv6, some non-canonical CIDRs.'
# MAGIC AS
# MAGIC WITH v4_24 AS (
# MAGIC   SELECT g.*, o2, o3,
# MAGIC          CASE WHEN rand(11) < 0.02
# MAGIC               THEN concat(o1, '.', o2, '.', o3, '.', cast(1 + rand(12) * 254 AS INT), '/24')
# MAGIC               ELSE concat(o1, '.', o2, '.', o3, '.0/24') END AS network
# MAGIC   FROM geo_ref g
# MAGIC   CROSS JOIN (SELECT cast(id AS INT) AS o2 FROM range(0, 7))
# MAGIC   CROSS JOIN (SELECT cast(id AS INT) AS o3 FROM range(0, 256))
# MAGIC ),
# MAGIC v4_16 AS (
# MAGIC   SELECT g.*, o2, 0 AS o3, concat(o1, '.', o2, '.0.0/16') AS network
# MAGIC   FROM geo_ref g
# MAGIC   CROSS JOIN (SELECT cast(id AS INT) AS o2 FROM range(7, 10))
# MAGIC ),
# MAGIC v6_48 AS (
# MAGIC   SELECT g.*, cast(id % 10 AS INT) AS o2, cast(id AS INT) AS o3,
# MAGIC          concat('2001:0db8:', lpad(lower(hex(id)), 4, '0'), '::/48') AS network
# MAGIC   FROM range(0, 1000) r
# MAGIC   JOIN geo_ref g ON g.idx = r.id % 20
# MAGIC )
# MAGIC SELECT
# MAGIC   network,
# MAGIC   country_code,
# MAGIC   country,
# MAGIC   element_at(cities, cast(o3 % 3 AS INT) + 1)                                   AS city,
# MAGIC   64512 + idx * 10 + o2                                                          AS asn,
# MAGIC   concat(country_code, ' ', element_at(array('Telecom', 'Broadband', 'Mobile', 'Hosting', 'Cloud',
# MAGIC                                              'Fiber', 'Datacenter', 'Cable', 'Wireless', 'Networks'), o2 + 1)) AS asn_org,
# MAGIC   'geo_feed_2026_11'                                                             AS source
# MAGIC FROM (SELECT * FROM v4_24 UNION ALL SELECT * FROM v4_16 UNION ALL SELECT * FROM v6_48)

# COMMAND ----------

# DBTITLE 1,Threat intelligence feed
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE threat_intel_raw
# MAGIC COMMENT 'Raw third-party threat intel feed. Indicators are CIDRs or /32s, non-canonical forms and malformed rows included.'
# MAGIC AS
# MAGIC -- Card-testing botnet (only ONE of the two attacking /24s is known to the feed)
# MAGIC SELECT '185.3.17.88/24' AS indicator, 'botnet' AS category, 95 AS confidence, 'abuse.ch' AS provider
# MAGIC UNION ALL
# MAGIC -- Tor exit nodes, same deterministic formula as the attack generator
# MAGIC SELECT concat(CASE WHEN k % 2 = 0 THEN 95 ELSE 213 END, '.', k % 7, '.', (k * 37) % 256, '.', (k * 11) % 254 + 1, '/32'),
# MAGIC        'tor_exit', 90, 'torproject'
# MAGIC FROM (SELECT id AS k FROM range(0, 300))
# MAGIC UNION ALL
# MAGIC -- Mass scanner /16
# MAGIC SELECT '45.7.0.0/16', 'scanner', 60, 'greynoise'
# MAGIC UNION ALL
# MAGIC -- Anonymizing VPN range (IPv6, written expanded and upper case)
# MAGIC SELECT '2001:0DB8:03E7:BEEF:0000:0000:0000:0000/64', 'anonymizing_vpn', 70, 'spur'
# MAGIC UNION ALL
# MAGIC -- Low-confidence noise: IPv4 /24s and IPv6 /64s
# MAGIC SELECT concat(element_at(array(23, 31, 41, 61, 77, 81, 89, 102, 103, 110, 151, 177, 196, 200, 203),
# MAGIC                          cast(rand(21) * 15 AS INT) + 1), '.', cast(rand(22) * 7 AS INT), '.', cast(rand(23) * 256 AS INT), '.0/24'),
# MAGIC        'spam', cast(20 + rand(24) * 30 AS INT), 'spamhaus'
# MAGIC FROM range(0, 1500)
# MAGIC UNION ALL
# MAGIC SELECT concat('2001:db8:', lower(hex(cast(rand(25) * 999 AS INT))), ':', lower(hex(cast(rand(26) * 65535 AS INT))), '::/64'),
# MAGIC        'spam', cast(20 + rand(27) * 30 AS INT), 'spamhaus'
# MAGIC FROM range(0, 200)
# MAGIC UNION ALL
# MAGIC -- Malformed rows a real feed always has
# MAGIC SELECT * FROM VALUES
# MAGIC   ('N/A', 'unknown', 10, 'community'),
# MAGIC   ('10.0.0.0/33', 'scanner', 40, 'community'),
# MAGIC   ('300.1.1.0/24', 'botnet', 50, 'community'),
# MAGIC   ('2001:db8::zz/64', 'spam', 20, 'community'),
# MAGIC   ('', 'unknown', 10, 'community')

# COMMAND ----------

# DBTITLE 1,Merchant allowlist
# MAGIC %sql
# MAGIC -- Each merchant allowlists an IPv4 /28 for its server-side (sk_live) key; every 5th merchant also an IPv6 /56.
# MAGIC CREATE OR REPLACE TABLE merchant_allowlist_raw
# MAGIC COMMENT 'Server-side API key IP allowlists per merchant (CIDR). Some entries non-canonical.'
# MAGIC AS
# MAGIC SELECT merchant_id, merchant_name, allowed_cidr
# MAGIC FROM (
# MAGIC   SELECT concat('mer_', lpad(cast(m AS STRING), 4, '0')) AS merchant_id,
# MAGIC          concat(element_at(array('Acme', 'Globex', 'Initech', 'Umbrella', 'Stark', 'Wayne', 'Hooli', 'Vandelay', 'Soylent', 'Wonka'),
# MAGIC                            cast(m % 10 AS INT) + 1), ' ',
# MAGIC                 element_at(array('Shop', 'Pay', 'Market', 'Travel', 'Games', 'Foods', 'Health', 'Media'), cast(m % 8 AS INT) + 1),
# MAGIC                 ' #', m) AS merchant_name,
# MAGIC          -- 1 in 10 entries written with host bits set
# MAGIC          CASE WHEN m % 10 = 3 THEN concat('52.', m % 7, '.', 100 + m DIV 7, '.7/28')
# MAGIC               ELSE concat('52.', m % 7, '.', 100 + m DIV 7, '.0/28') END AS v4_cidr,
# MAGIC          CASE WHEN m % 5 = 0 THEN concat('2001:db8:', lower(hex(m)), ':ff00::/56') END AS v6_cidr
# MAGIC   FROM (SELECT id AS m FROM range(0, 200))
# MAGIC )
# MAGIC LATERAL VIEW explode(filter(array(v4_cidr, v6_cidr), x -> x IS NOT NULL)) AS allowed_cidr

# COMMAND ----------

# DBTITLE 1,API access logs (normal traffic)
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW normal_traffic AS
# MAGIC WITH base AS (
# MAGIC   SELECT id,
# MAGIC          rand(101) AS r_kind, rand(102) AS r_ep, rand(103) AS r_status, rand(104) AS r_mess,
# MAGIC          cast(rand(105) * 200 AS INT) AS m,
# MAGIC          cast(rand(106) * 20 AS INT) AS gidx,
# MAGIC          cast(rand(107) * 10 AS INT) AS o2,
# MAGIC          cast(rand(108) * 256 AS INT) AS o3,
# MAGIC          cast(1 + rand(109) * 254 AS INT) AS o4,
# MAGIC          cast(rand(110) * 999 AS INT) AS v6_net,
# MAGIC          cast(rand(111) * 65535 AS INT) AS v6_sub,
# MAGIC          cast(1 + rand(112) * 65534 AS INT) AS v6_host,
# MAGIC          cast(rand(113) * 604800 AS INT) AS age_s
# MAGIC   FROM range(0, 4800000)
# MAGIC ),
# MAGIC shaped AS (
# MAGIC   SELECT b.*, g.o1,
# MAGIC          CASE WHEN r_kind < 0.70 THEN 'server'
# MAGIC               WHEN r_kind < 0.93 THEN 'consumer_v4'
# MAGIC               WHEN r_kind < 0.9998 THEN 'consumer_v6'
# MAGIC               ELSE 'misconfigured_server' END AS kind,
# MAGIC          concat('mer_', lpad(cast(m AS STRING), 4, '0')) AS merchant_id
# MAGIC   FROM base b JOIN geo_ref g ON g.idx = b.gidx
# MAGIC ),
# MAGIC with_ip AS (
# MAGIC   SELECT *,
# MAGIC          -- consumer IPv4, keeping clear of the attack ranges
# MAGIC          concat(o1, '.', CASE WHEN o1 IN (45, 185) AND o2 IN (3, 7) THEN o2 + 1 ELSE o2 END, '.', o3, '.', o4) AS consumer_v4,
# MAGIC          -- consumer IPv6: half expanded upper case (as some proxies log it), half compressed
# MAGIC          CASE WHEN id % 2 = 0
# MAGIC               THEN upper(concat('2001:0db8:', lpad(hex(v6_net), 4, '0'), ':', lpad(hex(v6_sub), 4, '0'), ':0000:0000:0000:', lpad(hex(v6_host), 4, '0')))
# MAGIC               ELSE concat('2001:db8:', lower(hex(v6_net)), ':', lower(hex(v6_sub)), '::', lower(hex(v6_host))) END AS consumer_v6,
# MAGIC          -- server traffic from the merchant allowlist (/28 v4, /56 v6 for every 5th merchant)
# MAGIC          CASE WHEN m % 5 = 0 AND id % 2 = 0
# MAGIC               THEN concat('2001:db8:', lower(hex(m)), ':ff', lpad(lower(hex(o3)), 2, '0'), '::', lower(hex(v6_host)))
# MAGIC               ELSE concat('52.', m % 7, '.', 100 + m DIV 7, '.', 1 + o4 % 14) END AS server_ip
# MAGIC   FROM shaped
# MAGIC )
# MAGIC SELECT
# MAGIC   timestampadd(SECOND, -age_s, current_timestamp())                               AS event_ts,
# MAGIC   concat('req_', id)                                                               AS request_id,
# MAGIC   merchant_id,
# MAGIC   CASE WHEN kind IN ('server', 'misconfigured_server') THEN concat('sk_live_', merchant_id)
# MAGIC        ELSE concat('pk_live_', merchant_id) END                                    AS api_key_id,
# MAGIC   CASE WHEN kind IN ('server', 'misconfigured_server')
# MAGIC        THEN CASE WHEN r_ep < 0.45 THEN 'POST /v1/charges' WHEN r_ep < 0.70 THEN 'GET /v1/charges'
# MAGIC                  WHEN r_ep < 0.85 THEN 'POST /v1/refunds' WHEN r_ep < 0.93 THEN 'GET /v1/balance'
# MAGIC                  ELSE 'POST /v1/payouts' END
# MAGIC        ELSE CASE WHEN r_ep < 0.40 THEN 'POST /v1/tokens' WHEN r_ep < 0.75 THEN 'POST /v1/payment_intents/confirm'
# MAGIC                  WHEN r_ep < 0.90 THEN 'GET /v1/checkout/session' ELSE 'POST /v1/auth/token' END
# MAGIC   END                                                                              AS endpoint,
# MAGIC   -- dirty IP strings in ~0.3% of rows
# MAGIC   CASE WHEN r_mess < 0.003
# MAGIC        THEN element_at(array('unknown', '-', '', '999.10.2.1', '052.1.2.3', '10.0.0.256', '::ffff:zz'), cast(id % 7 AS INT) + 1)
# MAGIC        WHEN kind = 'server' THEN server_ip
# MAGIC        WHEN kind = 'consumer_v6' THEN consumer_v6
# MAGIC        ELSE consumer_v4 END                                                        AS src_ip,
# MAGIC   CASE WHEN r_status < 0.003 THEN 503
# MAGIC        WHEN r_ep >= 0.40 AND r_ep < 0.75 AND kind LIKE 'consumer%' AND r_status < 0.04 THEN 402
# MAGIC        WHEN r_ep >= 0.90 AND kind LIKE 'consumer%' AND r_status < 0.05 THEN 401
# MAGIC        ELSE 200 END                                                                AS http_status,
# MAGIC   CASE WHEN kind IN ('server', 'misconfigured_server')
# MAGIC        THEN element_at(array('nimbus-python/7.2.0', 'nimbus-node/12.4.1', 'nimbus-java/24.1.0', 'nimbus-go/78.3.0'), cast(id % 4 AS INT) + 1)
# MAGIC        ELSE element_at(array('Mozilla/5.0 (iPhone; CPU iPhone OS 18_1)', 'Mozilla/5.0 (Windows NT 10.0; Win64)',
# MAGIC                              'Mozilla/5.0 (Macintosh; Intel Mac OS X 14_6)', 'Mozilla/5.0 (Linux; Android 15)'), cast(id % 4 AS INT) + 1)
# MAGIC   END                                                                              AS user_agent,
# MAGIC   CASE WHEN kind LIKE 'consumer%' AND r_ep < 0.75
# MAGIC        THEN concat('fp_', cast(rand(114) * 2000000 AS INT)) END                    AS card_fingerprint,
# MAGIC   cast(20 + rand(115) * 180 AS INT)                                                AS latency_ms
# MAGIC FROM with_ip

# COMMAND ----------

# DBTITLE 1,API access logs (planted attacks)
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW attack_traffic AS
# MAGIC -- A) Card testing: headless bots, two /24s, last 6h, thousands of distinct cards, mostly declined
# MAGIC SELECT timestampadd(SECOND, -cast(rand(201) * 21600 AS INT), current_timestamp()) AS event_ts,
# MAGIC        concat('req_ct_', id) AS request_id, 'mer_0042' AS merchant_id, 'pk_live_mer_0042' AS api_key_id,
# MAGIC        'POST /v1/payment_intents/confirm' AS endpoint,
# MAGIC        concat('185.3.', 17 + id % 2, '.', cast(1 + rand(202) * 254 AS INT)) AS src_ip,
# MAGIC        CASE WHEN rand(203) < 0.90 THEN 402 ELSE 200 END AS http_status,
# MAGIC        'Mozilla/5.0 (X11; Linux x86_64) HeadlessChrome/131.0' AS user_agent,
# MAGIC        concat('fp_ct_', id) AS card_fingerprint, cast(15 + rand(204) * 30 AS INT) AS latency_ms
# MAGIC FROM range(0, 60000)
# MAGIC UNION ALL
# MAGIC -- B) Credential stuffing from 300 Tor exits, last 48h
# MAGIC SELECT timestampadd(SECOND, -cast(rand(211) * 172800 AS INT), current_timestamp()),
# MAGIC        concat('req_cs_', id), concat('mer_', lpad(cast(id % 200 AS STRING), 4, '0')),
# MAGIC        concat('pk_live_mer_', lpad(cast(id % 200 AS STRING), 4, '0')), 'POST /v1/auth/token',
# MAGIC        concat(CASE WHEN k % 2 = 0 THEN 95 ELSE 213 END, '.', k % 7, '.', (k * 37) % 256, '.', (k * 11) % 254 + 1),
# MAGIC        CASE WHEN rand(212) < 0.88 THEN 401 ELSE 200 END,
# MAGIC        'python-requests/2.32.3', NULL, cast(30 + rand(213) * 60 AS INT)
# MAGIC FROM (SELECT id, id % 300 AS k FROM range(0, 40000))
# MAGIC UNION ALL
# MAGIC -- C) Leaked server key used from an IPv6 VPN range, outside the merchant allowlist
# MAGIC SELECT timestampadd(SECOND, -cast(rand(221) * 259200 AS INT), current_timestamp()),
# MAGIC        concat('req_lk_', id), 'mer_0117', 'sk_live_mer_0117',
# MAGIC        CASE WHEN rand(222) < 0.60 THEN 'POST /v1/payouts' ELSE 'GET /v1/balance' END,
# MAGIC        upper(concat('2001:0db8:03e7:beef:0000:0000:', lpad(hex(cast(rand(223) * 65535 AS INT)), 4, '0'), ':',
# MAGIC                     lpad(hex(cast(1 + rand(224) * 65534 AS INT)), 4, '0'))),
# MAGIC        200, 'curl/8.9.1', NULL, cast(80 + rand(225) * 200 AS INT)
# MAGIC FROM range(0, 8000)
# MAGIC UNION ALL
# MAGIC -- D) Scanner sweeping 45.7.0.0/16 hitting admin / traversal paths, last 20h
# MAGIC SELECT timestampadd(SECOND, -cast(rand(231) * 72000 AS INT), current_timestamp()),
# MAGIC        concat('req_sc_', id), NULL, NULL,
# MAGIC        element_at(array('GET /admin', 'GET /.env', 'GET /wp-login.php', 'GET /v1/internal/debug', 'GET /../../etc/passwd'),
# MAGIC                   cast(id % 5 AS INT) + 1),
# MAGIC        concat('45.7.', cast(rand(232) * 256 AS INT), '.', cast(1 + rand(233) * 254 AS INT)),
# MAGIC        CASE WHEN id % 3 = 0 THEN 403 ELSE 404 END,
# MAGIC        'Mozilla/5.0 zgrab/0.x', NULL, cast(5 + rand(234) * 20 AS INT)
# MAGIC FROM range(0, 20000)

# COMMAND ----------

# DBTITLE 1,Write api_logs_raw
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE api_logs_raw
# MAGIC COMMENT 'Raw Nimbus API access logs (7 days). src_ip is the raw string from the edge proxy: IPv4, IPv6 (expanded or compressed) and junk.'
# MAGIC AS
# MAGIC SELECT * FROM normal_traffic
# MAGIC UNION ALL
# MAGIC SELECT * FROM attack_traffic

# COMMAND ----------

# DBTITLE 1,Check row counts
# MAGIC %sql
# MAGIC SELECT 'api_logs_raw' AS table_name, count(*) AS row_count FROM api_logs_raw
# MAGIC UNION ALL SELECT 'geoip_blocks_raw', count(*) FROM geoip_blocks_raw
# MAGIC UNION ALL SELECT 'threat_intel_raw', count(*) FROM threat_intel_raw
# MAGIC UNION ALL SELECT 'merchant_allowlist_raw', count(*) FROM merchant_allowlist_raw

# COMMAND ----------

