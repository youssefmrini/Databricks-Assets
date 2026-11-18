# Databricks notebook source
# DBTITLE 1,About this notebook
# MAGIC %md
# MAGIC # Nimbus NetSentinel: Network Threat Hunting with Databricks IP Functions
# MAGIC
# MAGIC **Scenario.** Nimbus Payments runs a card-payments API for 200 online merchants. Its Security Engineering team receives ~5M API requests a week through an edge proxy that logs client IPs as raw strings - IPv4, IPv6 (compressed and expanded), and the occasional garbage. They need to:
# MAGIC
# MAGIC 1. Normalize every IP, IPv4 and IPv6 alike
# MAGIC 2. Enrich each request with GeoIP and ASN (network operator)
# MAGIC 3. Match traffic against third-party threat intelligence
# MAGIC 4. Enforce IP allowlists on server-side secret keys
# MAGIC 5. Spot card-testing bots that rotate IPs inside a subnet
# MAGIC 6. Ship incidents and a WAF blocklist to the SOC
# MAGIC
# MAGIC **Built with** the native, Photon-accelerated [IP functions](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-ip-functions), generally available on Databricks Runtime 18 LTS+ and serverless ([launch blog](https://www.databricks.com/blog/ip-functions-are-generally-available-bringing-high-performance-network-analytics-lakehouse)).
# MAGIC
# MAGIC **Notebook layout**
# MAGIC
# MAGIC | Part | Sections |
# MAGIC | --- | --- |
# MAGIC | A. Generate data | A1 reference data, A2 merchants, A3 GeoIP/ASN feed, A4 threat intel feed, A5 API logs |
# MAGIC | B. Pipeline | B1 IP functions tour, B2 clean logs, B3 canonicalize feeds, B4 GeoIP range join, B5 threat intel + hunting, B6 allowlist, B7 card testing, B8 legacy vs native, B9 incidents + AI brief, B10 WAF blocklist, B11 validation |
# MAGIC | C. Presenter | Video intro and demo flow (last cell) |
# MAGIC
# MAGIC **Run it:** attach serverless (or DBR 18 LTS+), set the `catalog` / `schema` widgets, Run all (~3-5 min).
# MAGIC
# MAGIC > All companies, merchants, keys, networks and feeds in this notebook are fictional and generated synthetically. IP ranges are used for illustration only and do not describe real operators.

# COMMAND ----------

# DBTITLE 1,Config
dbutils.widgets.text("catalog", "youssef_demo")
dbutils.widgets.text("schema", "ip_functions_demo")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")
spark.sql(f"USE CATALOG `{catalog}`")
spark.sql(f"USE SCHEMA `{schema}`")
print(f"Using {catalog}.{schema}")

# COMMAND ----------

# DBTITLE 1,Part A
# MAGIC %md
# MAGIC # Part A - Generate realistic synthetic data
# MAGIC
# MAGIC | Table | Rows | Description |
# MAGIC | --- | --- | --- |
# MAGIC | `merchants` | 200 | Nimbus merchant accounts (`acct_...`) and their API keys |
# MAGIC | `merchant_allowlist_raw` | 240 | CIDRs allowed to use each merchant's `sk_live_` key |
# MAGIC | `geoip_blocks_raw` | ~37K | Commercial-style GeoIP/ASN feed: IPv4 /16 and /24, IPv6 /32 |
# MAGIC | `threat_intel_raw` | ~2K | Aggregated third-party threat feeds |
# MAGIC | `api_logs_raw` | ~4.93M | 7 days of edge-proxy access logs |
# MAGIC
# MAGIC **Planted attacks** (the pipeline must find all of them):
# MAGIC
# MAGIC | Attack | Source | Target |
# MAGIC | --- | --- | --- |
# MAGIC | Card testing | Headless Chrome bots in two /24s at a German hosting provider; only one /24 is in the threat feed | `POST /v1/payment_intents/confirm` for *Bramble Home EU*, last 6h |
# MAGIC | Credential stuffing | 300 Tor exit nodes | `POST /v1/auth/token` (merchant dashboard login), last 48h |
# MAGIC | Leaked secret key | `curl` from an IPv6 VPN range, outside the allowlist | *Tidewater Surf Australia* `sk_live_` key, `POST /v1/payouts`, last 3 days |
# MAGIC | Mass scanning | zgrab sweeping a Brazilian /16 | `/.env`, `/wp-login.php`, path traversal, last 20h |
# MAGIC
# MAGIC All timestamps are relative to `current_timestamp()`, so rerunning keeps the 24-hour windows populated.

# COMMAND ----------

# DBTITLE 1,A1. Reference data
# MAGIC %md
# MAGIC ## A1. Country and network-operator reference
# MAGIC Each first IPv4 octet maps to a country with three cities and a fictional operator brand. The operator's line of business (Telecom, Hosting, Datacenter...) comes from the second octet.

# COMMAND ----------

# DBTITLE 1,Country / operator reference
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW geo_ref AS
# MAGIC SELECT * FROM VALUES
# MAGIC   (0,  23,  'US', 'United States',  array('New York', 'Chicago', 'Dallas'),           'Northstar',      'Inc.'),
# MAGIC   (1,  31,  'NL', 'Netherlands',    array('Amsterdam', 'Rotterdam', 'Utrecht'),       'Polderlink',     'B.V.'),
# MAGIC   (2,  41,  'ZA', 'South Africa',   array('Johannesburg', 'Cape Town', 'Durban'),     'Kaapnet',        '(Pty) Ltd'),
# MAGIC   (3,  45,  'BR', 'Brazil',         array('Sao Paulo', 'Rio de Janeiro', 'Curitiba'), 'Atlantica',      'Ltda.'),
# MAGIC   (4,  52,  'US', 'United States',  array('Ashburn', 'Boardman', 'San Jose'),         'Cirrus',         'LLC'),
# MAGIC   (5,  61,  'AU', 'Australia',      array('Sydney', 'Melbourne', 'Brisbane'),         'Wattle',         'Pty Ltd'),
# MAGIC   (6,  77,  'FR', 'France',         array('Paris', 'Lyon', 'Marseille'),              'Lumiere',        'SAS'),
# MAGIC   (7,  81,  'GB', 'United Kingdom', array('London', 'Manchester', 'Leeds'),           'Thameside',      'Ltd'),
# MAGIC   (8,  89,  'ES', 'Spain',          array('Madrid', 'Barcelona', 'Valencia'),         'Iberlink',       'S.L.'),
# MAGIC   (9,  95,  'SE', 'Sweden',         array('Stockholm', 'Gothenburg', 'Malmo'),        'Nordlink',       'AB'),
# MAGIC   (10, 102, 'IN', 'India',          array('Mumbai', 'Bengaluru', 'Delhi'),            'Indus',          'Pvt Ltd'),
# MAGIC   (11, 103, 'SG', 'Singapore',      array('Singapore', 'Jurong', 'Woodlands'),        'Merlion',        'Pte Ltd'),
# MAGIC   (12, 110, 'JP', 'Japan',          array('Tokyo', 'Osaka', 'Nagoya'),                'Sakura',         'K.K.'),
# MAGIC   (13, 151, 'IT', 'Italy',          array('Milan', 'Rome', 'Turin'),                  'Appennino',      'S.p.A.'),
# MAGIC   (14, 177, 'MX', 'Mexico',         array('Mexico City', 'Monterrey', 'Guadalajara'), 'Sierra Madre',   'S.A. de C.V.'),
# MAGIC   (15, 185, 'DE', 'Germany',        array('Frankfurt', 'Nuremberg', 'Falkenstein'),   'Rheinwerk',      'GmbH'),
# MAGIC   (16, 196, 'NG', 'Nigeria',        array('Lagos', 'Abuja', 'Ibadan'),                'Lagoon',         'Ltd'),
# MAGIC   (17, 200, 'AR', 'Argentina',      array('Buenos Aires', 'Cordoba', 'Rosario'),      'Pampa',          'S.A.'),
# MAGIC   (18, 203, 'KR', 'South Korea',    array('Seoul', 'Busan', 'Incheon'),               'Hangang',        'Co., Ltd.'),
# MAGIC   (19, 213, 'PL', 'Poland',         array('Warsaw', 'Krakow', 'Gdansk'),              'Wisla',          'Sp. z o.o.')
# MAGIC AS t(idx, o1, country_code, country, cities, brand, legal_suffix);
# MAGIC
# MAGIC -- Line of business per second octet (0-9)
# MAGIC CREATE OR REPLACE TEMP VIEW operator_types AS
# MAGIC SELECT posexplode(array('Telecom', 'Broadband', 'Mobile', 'Hosting', 'Cloud',
# MAGIC                         'Fibre', 'Wireless', 'Datacenter', 'Cable', 'Networks')) AS (o2, line_of_business);

# COMMAND ----------

# DBTITLE 1,A2. Merchants
# MAGIC %md
# MAGIC ## A2. Merchants, API keys and allowlists
# MAGIC Each merchant gets a Stripe-style account id, a publishable key (`pk_live_`, used from browsers and apps) and a secret key (`sk_live_`, server-to-server). Secret keys are restricted to the merchant's cloud egress range: an IPv4 /28, plus an IPv6 /40 for every 5th merchant. 10% of allowlist entries are typed by hand with host bits set (`52.3.100.7/28`).

# COMMAND ----------

# DBTITLE 1,Merchants and allowlists
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE merchants
# MAGIC COMMENT 'Nimbus merchant accounts with publishable (pk_live) and secret (sk_live) API keys. Synthetic.'
# MAGIC AS
# MAGIC SELECT
# MAGIC   m,
# MAGIC   concat('acct_', substr(sha2(concat('nimbus-acct-', m), 256), 1, 16))        AS merchant_id,
# MAGIC   concat(element_at(array('Brightline Coffee', 'Volta Bikes', 'Juniper & Sage', 'Northwind Outfitters', 'Pixel Forge Games',
# MAGIC                           'Harbor Health', 'Lumen Travel', 'Fable Books', 'Kettle & Crumb', 'Orbit Fitness',
# MAGIC                           'Cobalt Audio', 'Meadow Pet Supply', 'Atlas Luggage', 'Tidewater Surf', 'Ember Candle Co',
# MAGIC                           'Quill Stationery', 'Summit Gear', 'Bramble Home', 'Nova Skincare', 'Copper Kettle Tea',
# MAGIC                           'Driftwood Furniture', 'Saffron Kitchen', 'Polar Ride', 'Wildflower Florists', 'Granite Fitness Labs'),
# MAGIC                     m % 25 + 1), ' ',
# MAGIC          element_at(array('US', 'EU', 'UK', 'Canada', 'Australia', 'Japan', 'Brazil', 'India'), cast(m DIV 25 AS INT) + 1)) AS merchant_name,
# MAGIC   element_at(array('enterprise', 'growth', 'growth', 'starter', 'starter'), m % 5 + 1) AS tier,
# MAGIC   concat('pk_live_', substr(sha2(concat('pk-', m), 256), 1, 24))              AS publishable_key,
# MAGIC   concat('sk_live_', substr(sha2(concat('sk-', m), 256), 1, 24))              AS secret_key
# MAGIC FROM (SELECT cast(id AS INT) AS m FROM range(0, 200));
# MAGIC
# MAGIC CREATE OR REPLACE TABLE merchant_allowlist_raw
# MAGIC COMMENT 'Raw CIDR allowlists for sk_live keys, as entered by merchants in the dashboard. Synthetic.'
# MAGIC AS
# MAGIC SELECT merchant_id, allowed_cidr, 'dashboard' AS entered_via
# MAGIC FROM (
# MAGIC   SELECT merchant_id,
# MAGIC          CASE WHEN m % 10 = 3 THEN concat('52.', m % 7, '.', 100 + m DIV 7, '.7/28')
# MAGIC               ELSE concat('52.', m % 7, '.', 100 + m DIV 7, '.0/28') END                    AS v4_cidr,
# MAGIC          CASE WHEN m % 5 = 0 THEN concat('2a0b:', lower(hex(19968 + m)), ':ff00::/40') END  AS v6_cidr
# MAGIC   FROM merchants
# MAGIC )
# MAGIC LATERAL VIEW explode(filter(array(v4_cidr, v6_cidr), x -> x IS NOT NULL)) AS allowed_cidr;
# MAGIC
# MAGIC SELECT * FROM merchants LIMIT 5;

# COMMAND ----------

# DBTITLE 1,A3. GeoIP feed
# MAGIC %md
# MAGIC ## A3. GeoIP / ASN feed
# MAGIC Shaped like a commercial GeoIP + ASN database: IPv4 networks of mixed size (/24 and /16) and IPv6 /32 allocations, each with country, city, ASN and operator. About 2% of IPv4 rows come with host bits set - common in feeds exported from spreadsheets.

# COMMAND ----------

# DBTITLE 1,GeoIP / ASN feed
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE geoip_blocks_raw
# MAGIC COMMENT 'Raw GeoIP + ASN feed (CIDR -> country, city, ASN, operator). IPv4 /16 + /24, IPv6 /32. Synthetic.'
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
# MAGIC v6_32 AS (
# MAGIC   SELECT g.*, cast(id % 10 AS INT) AS o2, cast(id AS INT) AS o3,
# MAGIC          concat('2a0b:', lower(hex(19968 + id)), '::/32') AS network
# MAGIC   FROM range(0, 1000) r
# MAGIC   JOIN geo_ref g ON g.idx = r.id % 20
# MAGIC )
# MAGIC SELECT
# MAGIC   b.network,
# MAGIC   b.country_code,
# MAGIC   b.country,
# MAGIC   element_at(b.cities, b.o3 % 3 + 1)                              AS city,
# MAGIC   13000 + b.idx * 2111 + b.o2 * 7                                 AS asn,
# MAGIC   concat(b.brand, ' ', t.line_of_business, ' ', b.legal_suffix)   AS asn_org,
# MAGIC   t.line_of_business                                              AS connection_type,
# MAGIC   'geofeed-2026.11'                                               AS feed_version
# MAGIC FROM (SELECT * FROM v4_24 UNION ALL SELECT * FROM v4_16 UNION ALL SELECT * FROM v6_32) b
# MAGIC JOIN operator_types t ON t.o2 = b.o2;
# MAGIC
# MAGIC -- The networks our attackers (and one merchant) live in
# MAGIC SELECT * FROM geoip_blocks_raw
# MAGIC WHERE network IN ('185.3.17.0/24', '185.3.18.0/24', '45.7.0.0/16', '2a0b:51e7::/32', '52.1.100.0/24');

# COMMAND ----------

# DBTITLE 1,A4. Threat intel feed
# MAGIC %md
# MAGIC ## A4. Threat intelligence feed
# MAGIC Aggregated from five (fictional) providers. Indicators arrive as CIDRs of every size, in whatever format the provider uses - including upper-case, fully expanded IPv6 and outright broken rows.

# COMMAND ----------

# DBTITLE 1,Threat intel feed
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE threat_intel_raw
# MAGIC COMMENT 'Raw aggregated threat intel indicators from 5 providers. Mixed formats, some malformed. Synthetic.'
# MAGIC AS
# MAGIC -- Card-testing botnet: the feed knows only ONE of the two attacking /24s
# MAGIC SELECT '185.3.17.88/24' AS indicator, 'botnet' AS category, 95 AS confidence, 'IronGate Botnet C2 Tracker' AS provider,
# MAGIC        'Card-testing botnet, headless Chrome' AS description
# MAGIC UNION ALL
# MAGIC -- Tor exit nodes (same deterministic formula as the attack traffic)
# MAGIC SELECT concat(CASE WHEN k % 2 = 0 THEN 95 ELSE 213 END, '.', k % 7, '.', (k * 37) % 256, '.', (k * 11) % 254 + 1, '/32'),
# MAGIC        'tor_exit', 90, 'Torbridge Exit List', 'Tor exit relay'
# MAGIC FROM (SELECT id AS k FROM range(0, 300))
# MAGIC UNION ALL
# MAGIC SELECT '45.7.0.0/16', 'scanner', 60, 'NoiseNet Sensor Grid', 'Internet-wide scanner, zgrab'
# MAGIC UNION ALL
# MAGIC SELECT '2A0B:51E7:9C40:17A2:0000:0000:0000:0000/64', 'anonymizing_vpn', 70, 'Veilscope VPN Intel', 'Commercial VPN egress'
# MAGIC UNION ALL
# MAGIC -- Low-confidence spam ranges, IPv4 and IPv6
# MAGIC SELECT concat(element_at(array(23, 31, 41, 61, 77, 81, 89, 102, 103, 110, 151, 177, 196, 200, 203),
# MAGIC                          cast(rand(21) * 15 AS INT) + 1), '.', cast(rand(22) * 7 AS INT), '.', cast(rand(23) * 256 AS INT), '.0/24'),
# MAGIC        'spam', cast(20 + rand(24) * 30 AS INT), 'Spamhold DROP', 'Spam source'
# MAGIC FROM range(0, 1500)
# MAGIC UNION ALL
# MAGIC SELECT concat('2a0b:', lower(hex(19968 + cast(rand(25) * 999 AS INT))), ':', lower(hex(cast(rand(26) * 65535 AS INT))),
# MAGIC               ':', lower(hex(cast(rand(27) * 65535 AS INT))), '::/64'),
# MAGIC        'spam', cast(20 + rand(28) * 30 AS INT), 'Spamhold DROP', 'Spam source'
# MAGIC FROM range(0, 200)
# MAGIC UNION ALL
# MAGIC -- Broken rows every real feed has
# MAGIC SELECT * FROM VALUES
# MAGIC   ('N/A',             'unknown', 10, 'Community submissions', 'placeholder'),
# MAGIC   ('10.0.0.0/33',     'scanner', 40, 'Community submissions', 'invalid prefix'),
# MAGIC   ('300.1.1.0/24',    'botnet',  50, 'Community submissions', 'invalid octet'),
# MAGIC   ('2a0b:4e00::zz/64','spam',    20, 'Community submissions', 'invalid hex'),
# MAGIC   ('',                'unknown', 10, 'Community submissions', 'empty');
# MAGIC
# MAGIC SELECT provider, category, count(*) AS indicators FROM threat_intel_raw GROUP BY ALL ORDER BY indicators DESC;

# COMMAND ----------

# DBTITLE 1,A5. API logs
# MAGIC %md
# MAGIC ## A5. Edge-proxy API access logs (7 days)
# MAGIC **Normal traffic (4.8M rows)**
# MAGIC * 70% server-to-server calls with `sk_live_` keys from merchant allowlisted ranges (official SDK user agents)
# MAGIC * 23% browser/app calls with `pk_live_` keys from consumer IPv4
# MAGIC * 7% browser/app calls from consumer IPv6 - half logged fully expanded in upper case by a legacy proxy tier
# MAGIC * 0.02% server calls from outside the allowlist (real-world misconfigurations, low volume)
# MAGIC * 0.3% unparseable `src_ip` values (`unknown`, `-`, `999.10.2.1`, zero-padded `052.1.2.3`...)
# MAGIC
# MAGIC **Attack traffic (128K rows)** - the four campaigns described in Part A.

# COMMAND ----------

# DBTITLE 1,Normal traffic
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW normal_traffic AS
# MAGIC WITH base AS (
# MAGIC   SELECT id,
# MAGIC          rand(101) AS r_kind, rand(102) AS r_ep, rand(103) AS r_status, rand(104) AS r_mess,
# MAGIC          cast(rand(105) * 200 AS INT)          AS m,
# MAGIC          cast(rand(106) * 20 AS INT)           AS gidx,
# MAGIC          cast(rand(107) * 10 AS INT)           AS o2,
# MAGIC          cast(rand(108) * 256 AS INT)          AS o3,
# MAGIC          cast(1 + rand(109) * 254 AS INT)      AS o4,
# MAGIC          cast(rand(110) * 999 AS INT)          AS v6_net,
# MAGIC          cast(rand(111) * 65535 AS INT)        AS v6_sub,
# MAGIC          cast(rand(112) * 65535 AS INT)        AS v6_sub2,
# MAGIC          cast(1 + rand(113) * 65534 AS INT)    AS v6_host,
# MAGIC          cast(rand(114) * 604800 AS INT)       AS age_s
# MAGIC   FROM range(0, 4800000)
# MAGIC ),
# MAGIC shaped AS (
# MAGIC   SELECT b.*, g.o1, mc.merchant_id, mc.publishable_key, mc.secret_key,
# MAGIC          CASE WHEN r_kind < 0.70   THEN 'server'
# MAGIC               WHEN r_kind < 0.93   THEN 'consumer_v4'
# MAGIC               WHEN r_kind < 0.9998 THEN 'consumer_v6'
# MAGIC               ELSE 'misconfigured_server' END AS kind
# MAGIC   FROM base b
# MAGIC   JOIN geo_ref g    ON g.idx = b.gidx
# MAGIC   JOIN merchants mc ON mc.m = b.m
# MAGIC ),
# MAGIC with_ip AS (
# MAGIC   SELECT *,
# MAGIC          -- consumer IPv4, kept clear of the attack ranges
# MAGIC          concat(o1, '.', CASE WHEN o1 IN (45, 185) AND o2 IN (3, 7) THEN o2 + 1 ELSE o2 END, '.', o3, '.', o4) AS consumer_v4,
# MAGIC          -- consumer IPv6: legacy proxy tier logs expanded upper case, new tier logs compressed
# MAGIC          CASE WHEN id % 2 = 0
# MAGIC               THEN upper(concat('2a0b:', lpad(hex(19968 + v6_net), 4, '0'), ':', lpad(hex(v6_sub), 4, '0'), ':',
# MAGIC                                 lpad(hex(v6_sub2), 4, '0'), ':0000:0000:0000:', lpad(hex(v6_host), 4, '0')))
# MAGIC               ELSE concat('2a0b:', lower(hex(19968 + v6_net)), ':', lower(hex(v6_sub)), ':', lower(hex(v6_sub2)),
# MAGIC                           '::', lower(hex(v6_host))) END AS consumer_v6,
# MAGIC          -- merchant servers: inside their /28 (or /40 for IPv6-enabled merchants)
# MAGIC          CASE WHEN m % 5 = 0 AND id % 2 = 0
# MAGIC               THEN concat('2a0b:', lower(hex(19968 + m)), ':ff', lpad(lower(hex(o3)), 2, '0'), ':', lower(hex(v6_sub)),
# MAGIC                           '::', lower(hex(v6_host)))
# MAGIC               ELSE concat('52.', m % 7, '.', 100 + m DIV 7, '.', 1 + o4 % 14) END AS server_ip
# MAGIC   FROM shaped
# MAGIC )
# MAGIC SELECT
# MAGIC   timestampadd(SECOND, -age_s, current_timestamp())                                          AS event_ts,
# MAGIC   concat('req_', substr(sha2(concat('n', id), 256), 1, 20))                                   AS request_id,
# MAGIC   merchant_id,
# MAGIC   CASE WHEN kind IN ('server', 'misconfigured_server') THEN secret_key ELSE publishable_key END AS api_key_id,
# MAGIC   CASE WHEN kind IN ('server', 'misconfigured_server')
# MAGIC        THEN CASE WHEN r_ep < 0.45 THEN 'POST /v1/charges'  WHEN r_ep < 0.70 THEN 'GET /v1/charges'
# MAGIC                  WHEN r_ep < 0.85 THEN 'POST /v1/refunds'  WHEN r_ep < 0.93 THEN 'GET /v1/balance'
# MAGIC                  ELSE 'POST /v1/payouts' END
# MAGIC        ELSE CASE WHEN r_ep < 0.40 THEN 'POST /v1/tokens'   WHEN r_ep < 0.75 THEN 'POST /v1/payment_intents/confirm'
# MAGIC                  WHEN r_ep < 0.90 THEN 'GET /v1/checkout/sessions' ELSE 'POST /v1/auth/token' END
# MAGIC   END                                                                                         AS endpoint,
# MAGIC   CASE WHEN r_mess < 0.003
# MAGIC        THEN element_at(array('unknown', '-', '', '999.10.2.1', '052.1.2.3', '10.0.0.256', '::ffff:zz'), cast(id % 7 AS INT) + 1)
# MAGIC        WHEN kind = 'server'      THEN server_ip
# MAGIC        WHEN kind = 'consumer_v6' THEN consumer_v6
# MAGIC        ELSE consumer_v4 END                                                                   AS src_ip,
# MAGIC   CASE WHEN r_status < 0.003 THEN 503
# MAGIC        WHEN kind LIKE 'consumer%' AND r_ep >= 0.40 AND r_ep < 0.75 AND r_status < 0.04 THEN 402
# MAGIC        WHEN kind LIKE 'consumer%' AND r_ep >= 0.90 AND r_status < 0.05 THEN 401
# MAGIC        ELSE 200 END                                                                           AS http_status,
# MAGIC   CASE WHEN kind IN ('server', 'misconfigured_server')
# MAGIC        THEN element_at(array('Nimbus/v1 PythonBindings/9.3.0', 'Nimbus/v1 NodeBindings/17.2.1',
# MAGIC                              'Nimbus/v1 JavaBindings/26.0.0', 'Nimbus/v1 GoBindings/81.1.0'), cast(id % 4 AS INT) + 1)
# MAGIC        ELSE element_at(array('Mozilla/5.0 (iPhone; CPU iPhone OS 18_1 like Mac OS X) AppleWebKit/605.1.15 Version/18.1 Mobile/15E148 Safari/604.1',
# MAGIC                              'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/131.0.0.0 Safari/537.36',
# MAGIC                              'Mozilla/5.0 (Macintosh; Intel Mac OS X 14_6) AppleWebKit/605.1.15 Version/18.0 Safari/605.1.15',
# MAGIC                              'Mozilla/5.0 (Linux; Android 15; Pixel 9) AppleWebKit/537.36 Chrome/131.0.0.0 Mobile Safari/537.36'),
# MAGIC                        cast(id % 4 AS INT) + 1)
# MAGIC   END                                                                                         AS user_agent,
# MAGIC   CASE WHEN kind LIKE 'consumer%' AND r_ep < 0.75
# MAGIC        THEN concat('fp_', substr(sha2(cast(cast(rand(115) * 2000000 AS INT) AS STRING), 256), 1, 16)) END AS card_fingerprint,
# MAGIC   cast(40 + rand(116) * 260 AS INT)                                                           AS latency_ms
# MAGIC FROM with_ip

# COMMAND ----------

# DBTITLE 1,Attack traffic
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW attack_traffic AS
# MAGIC -- A) Card testing against Bramble Home EU (m = 42): 2 x /24, last 6h, one fresh card per attempt, ~90% declined
# MAGIC SELECT timestampadd(SECOND, -cast(rand(201) * 21600 AS INT), current_timestamp())   AS event_ts,
# MAGIC        concat('req_', substr(sha2(concat('ct', id), 256), 1, 20))                    AS request_id,
# MAGIC        mc.merchant_id, mc.publishable_key                                            AS api_key_id,
# MAGIC        'POST /v1/payment_intents/confirm'                                            AS endpoint,
# MAGIC        concat('185.3.', 17 + id % 2, '.', cast(1 + rand(202) * 254 AS INT))          AS src_ip,
# MAGIC        CASE WHEN rand(203) < 0.90 THEN 402 ELSE 200 END                              AS http_status,
# MAGIC        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 HeadlessChrome/131.0.0.0 Safari/537.36' AS user_agent,
# MAGIC        concat('fp_', substr(sha2(concat('ct-card', id), 256), 1, 16))                AS card_fingerprint,
# MAGIC        cast(15 + rand(204) * 30 AS INT)                                              AS latency_ms
# MAGIC FROM range(0, 60000) JOIN merchants mc ON mc.m = 42
# MAGIC UNION ALL
# MAGIC -- B) Credential stuffing on dashboard login from 300 Tor exits, last 48h
# MAGIC SELECT timestampadd(SECOND, -cast(rand(211) * 172800 AS INT), current_timestamp()),
# MAGIC        concat('req_', substr(sha2(concat('cs', id), 256), 1, 20)),
# MAGIC        mc.merchant_id, mc.publishable_key, 'POST /v1/auth/token',
# MAGIC        concat(CASE WHEN k % 2 = 0 THEN 95 ELSE 213 END, '.', k % 7, '.', (k * 37) % 256, '.', (k * 11) % 254 + 1),
# MAGIC        CASE WHEN rand(212) < 0.88 THEN 401 ELSE 200 END,
# MAGIC        'python-requests/2.32.3', NULL, cast(30 + rand(213) * 60 AS INT)
# MAGIC FROM (SELECT id, id % 300 AS k, cast(id % 200 AS INT) AS m FROM range(0, 40000)) a
# MAGIC JOIN merchants mc ON mc.m = a.m
# MAGIC UNION ALL
# MAGIC -- C) Leaked secret key of Tidewater Surf Australia (m = 113) used from an IPv6 VPN /64, last 3 days
# MAGIC SELECT timestampadd(SECOND, -cast(rand(221) * 259200 AS INT), current_timestamp()),
# MAGIC        concat('req_', substr(sha2(concat('lk', id), 256), 1, 20)),
# MAGIC        mc.merchant_id, mc.secret_key,
# MAGIC        CASE WHEN rand(222) < 0.60 THEN 'POST /v1/payouts' ELSE 'GET /v1/balance' END,
# MAGIC        upper(concat('2a0b:51e7:9c40:17a2:0000:0000:', lpad(hex(cast(rand(223) * 65535 AS INT)), 4, '0'), ':',
# MAGIC                     lpad(hex(cast(1 + rand(224) * 65534 AS INT)), 4, '0'))),
# MAGIC        200, 'curl/8.9.1', NULL, cast(80 + rand(225) * 200 AS INT)
# MAGIC FROM range(0, 8000) JOIN merchants mc ON mc.m = 113
# MAGIC UNION ALL
# MAGIC -- D) Mass scanner sweeping 45.7.0.0/16, last 20h, no API key
# MAGIC SELECT timestampadd(SECOND, -cast(rand(231) * 72000 AS INT), current_timestamp()),
# MAGIC        concat('req_', substr(sha2(concat('sc', id), 256), 1, 20)),
# MAGIC        NULL, NULL,
# MAGIC        element_at(array('GET /admin', 'GET /.env', 'GET /wp-login.php', 'GET /v1/internal/debug', 'GET /../../etc/passwd'),
# MAGIC                   cast(id % 5 AS INT) + 1),
# MAGIC        concat('45.7.', cast(rand(232) * 256 AS INT), '.', cast(1 + rand(233) * 254 AS INT)),
# MAGIC        CASE WHEN id % 3 = 0 THEN 403 ELSE 404 END,
# MAGIC        'Mozilla/5.0 zgrab/0.x', NULL, cast(5 + rand(234) * 20 AS INT)
# MAGIC FROM range(0, 20000)

# COMMAND ----------

# DBTITLE 1,Write api_logs_raw and check counts
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE api_logs_raw
# MAGIC COMMENT 'Raw Nimbus edge-proxy access logs, 7 days. src_ip is the raw string from the proxy. Synthetic.'
# MAGIC AS
# MAGIC SELECT * FROM normal_traffic
# MAGIC UNION ALL
# MAGIC SELECT * FROM attack_traffic;
# MAGIC
# MAGIC SELECT 'api_logs_raw' AS table_name, count(*) AS row_count FROM api_logs_raw
# MAGIC UNION ALL SELECT 'merchants', count(*) FROM merchants
# MAGIC UNION ALL SELECT 'merchant_allowlist_raw', count(*) FROM merchant_allowlist_raw
# MAGIC UNION ALL SELECT 'geoip_blocks_raw', count(*) FROM geoip_blocks_raw
# MAGIC UNION ALL SELECT 'threat_intel_raw', count(*) FROM threat_intel_raw;

# COMMAND ----------

# DBTITLE 1,Part B
# MAGIC %md
# MAGIC # Part B - The NetSentinel pipeline
# MAGIC Bronze (`*_raw`) -> Silver (clean, canonical, BINARY keys) -> enrichment joins -> detections -> Gold (`security_incidents`, `waf_blocklist`). Everything is SQL.

# COMMAND ----------

# DBTITLE 1,B1. IP functions tour
# MAGIC %md
# MAGIC ## B1. The 14 IP functions in one query
# MAGIC
# MAGIC | Family | Functions | Use for |
# MAGIC | --- | --- | --- |
# MAGIC | Canonicalize | `ip_host`, `ip_cidr` | One standard text form per address / block |
# MAGIC | Inspect | `ip_network` (= `ip_network_first`), `ip_network_last`, `ip_prefix_length`, `ip_version` | Range bounds, block size, v4 vs v6 |
# MAGIC | Contain | `ip_cidr_contains` | IP-in-CIDR and CIDR-in-CIDR tests, optimized range joins |
# MAGIC | Convert | `ip_as_binary`, `ip_as_string` | 4-byte / 16-byte keys for storage and joins |
# MAGIC | Safe | `try_ip_host`, `try_ip_cidr`, `try_ip_as_binary`, `try_ip_as_string` | NULL instead of an error on bad input |

# COMMAND ----------

# DBTITLE 1,All 14 IP functions
# MAGIC %sql
# MAGIC SELECT
# MAGIC   ip_host('2A0B:51E7:9C40:17A2:0000:0000:0000:0001')  AS ip_host,            -- 2a0b:51e7:9c40:17a2::1
# MAGIC   ip_cidr('185.3.17.88/24')                          AS ip_cidr,            -- 185.3.17.0/24
# MAGIC   ip_network('52.1.100.0/28')                        AS ip_network,         -- 52.1.100.0
# MAGIC   ip_network_first('2a0b:51e7::/32')                 AS ip_network_first,   -- 2a0b:51e7::
# MAGIC   ip_network_last('52.1.100.0/28')                   AS ip_network_last,    -- 52.1.100.15
# MAGIC   ip_prefix_length('45.7.0.0/16')                    AS ip_prefix_length,   -- 16
# MAGIC   ip_version('2a0b:51e7::/32')                       AS ip_version,         -- 6
# MAGIC   ip_cidr_contains('45.7.0.0/16', '45.7.12.34')      AS ip_in_cidr,         -- true
# MAGIC   ip_cidr_contains('185.3.0.0/16', '185.3.18.0/24')  AS cidr_in_cidr,       -- true
# MAGIC   hex(ip_as_binary('185.3.17.42'))                   AS ip_as_binary,       -- B9031122 (4 bytes)
# MAGIC   ip_as_string(ip_as_binary('2a0b:51e7::/32'))       AS ip_as_string,       -- 2a0b:51e7::/32
# MAGIC   try_ip_host('052.1.2.3')                           AS try_ip_host,        -- NULL (zero-padded)
# MAGIC   try_ip_cidr('10.0.0.0/33')                         AS try_ip_cidr,        -- NULL (bad prefix)
# MAGIC   try_ip_as_binary('unknown')                        AS try_ip_as_binary,   -- NULL
# MAGIC   try_ip_as_string(X'0102')                          AS try_ip_as_string    -- NULL (2 bytes is not an IP)

# COMMAND ----------

# DBTITLE 1,B2. Clean logs
# MAGIC %md
# MAGIC ## B2. Bronze -> Silver: clean the access logs
# MAGIC * `try_ip_host` canonicalizes valid IPs and returns NULL for junk, so bad rows go to quarantine instead of failing the job
# MAGIC * `ip_version` tags v4/v6
# MAGIC * `ip_as_binary` creates a compact join key (4 or 16 bytes) - parse once, join many times
# MAGIC * `ip_cidr(ip || '/24')` (or `/64` for IPv6) builds the subnet used for behavioral detection

# COMMAND ----------

# DBTITLE 1,Profile raw src_ip
# MAGIC %sql
# MAGIC SELECT
# MAGIC   CASE WHEN try_ip_host(src_ip) IS NULL   THEN 'invalid'
# MAGIC        WHEN src_ip <> try_ip_host(src_ip) THEN concat('IPv', ip_version(try_ip_host(src_ip)), ' non-canonical')
# MAGIC        ELSE concat('IPv', ip_version(src_ip), ' canonical') END AS src_ip_shape,
# MAGIC   count(*)                                                       AS requests,
# MAGIC   any_value(src_ip)                                              AS raw_example,
# MAGIC   any_value(try_ip_host(src_ip))                                 AS canonical_example
# MAGIC FROM api_logs_raw
# MAGIC GROUP BY ALL
# MAGIC ORDER BY requests DESC

# COMMAND ----------

# DBTITLE 1,Build api_logs_silver and quarantine
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE api_logs_silver
# MAGIC COMMENT 'API logs with canonical IP, IP version, BINARY join key (ip_bin) and subnet (/24 v4, /64 v6).'
# MAGIC AS
# MAGIC WITH parsed AS (
# MAGIC   SELECT *, try_ip_host(src_ip) AS ip FROM api_logs_raw
# MAGIC )
# MAGIC SELECT
# MAGIC   event_ts, request_id, merchant_id, api_key_id, endpoint, http_status,
# MAGIC   user_agent, card_fingerprint, latency_ms,
# MAGIC   src_ip                                                                        AS src_ip_raw,
# MAGIC   ip,
# MAGIC   ip_version(ip)                                                                AS ip_version,
# MAGIC   ip_as_binary(ip)                                                              AS ip_bin,
# MAGIC   ip_cidr(concat(ip, CASE WHEN ip_version(ip) = 4 THEN '/24' ELSE '/64' END))  AS subnet
# MAGIC FROM parsed
# MAGIC WHERE ip IS NOT NULL;
# MAGIC
# MAGIC CREATE OR REPLACE TABLE api_logs_quarantine
# MAGIC COMMENT 'API log rows whose src_ip is not a valid IP address.'
# MAGIC AS
# MAGIC SELECT * FROM api_logs_raw WHERE try_ip_host(src_ip) IS NULL;
# MAGIC
# MAGIC SELECT
# MAGIC   (SELECT count(*) FROM api_logs_raw)               AS raw_rows,
# MAGIC   count(*)                                           AS silver_rows,
# MAGIC   (SELECT count(*) FROM api_logs_quarantine)        AS quarantined_rows,
# MAGIC   count_if(ip_version = 4)                           AS ipv4_rows,
# MAGIC   count_if(ip_version = 6)                           AS ipv6_rows,
# MAGIC   round(sum(length(src_ip_raw)) / 1024 / 1024, 1)    AS raw_string_mb,
# MAGIC   round(sum(octet_length(ip_bin)) / 1024 / 1024, 1)  AS binary_key_mb
# MAGIC FROM api_logs_silver;

# COMMAND ----------

# DBTITLE 1,B3. Canonicalize feeds
# MAGIC %md
# MAGIC ## B3. Canonicalize reference feeds
# MAGIC GeoIP, threat intel and allowlists all arrive as CIDR strings. `try_ip_cidr` clears host bits and normalizes IPv6, or returns NULL for broken rows. We also precompute range bounds (`ip_network_first` / `ip_network_last`), prefix length and the BINARY key.

# COMMAND ----------

# DBTITLE 1,Build geoip_blocks, threat_intel, merchant_allowlist
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE geoip_blocks
# MAGIC COMMENT 'Canonical GeoIP + ASN blocks with BINARY join key and range bounds.'
# MAGIC AS
# MAGIC WITH c AS (SELECT *, try_ip_cidr(network) AS cidr FROM geoip_blocks_raw)
# MAGIC SELECT
# MAGIC   cidr                    AS network,
# MAGIC   ip_as_binary(cidr)      AS network_bin,
# MAGIC   ip_network_first(cidr)  AS first_ip,
# MAGIC   ip_network_last(cidr)   AS last_ip,
# MAGIC   ip_prefix_length(cidr)  AS prefix_length,
# MAGIC   ip_version(cidr)        AS ip_version,
# MAGIC   country_code, country, city, asn, asn_org, connection_type,
# MAGIC   network                 AS network_raw
# MAGIC FROM c
# MAGIC WHERE cidr IS NOT NULL;
# MAGIC
# MAGIC CREATE OR REPLACE TABLE threat_intel
# MAGIC COMMENT 'Canonical threat intel indicators (CIDR) with BINARY join key.'
# MAGIC AS
# MAGIC WITH c AS (SELECT *, try_ip_cidr(indicator) AS cidr FROM threat_intel_raw)
# MAGIC SELECT
# MAGIC   cidr                    AS indicator,
# MAGIC   ip_as_binary(cidr)      AS indicator_bin,
# MAGIC   ip_prefix_length(cidr)  AS prefix_length,
# MAGIC   ip_version(cidr)        AS ip_version,
# MAGIC   category, confidence, provider, description,
# MAGIC   indicator               AS indicator_raw
# MAGIC FROM c
# MAGIC WHERE cidr IS NOT NULL;
# MAGIC
# MAGIC CREATE OR REPLACE TABLE threat_intel_rejected
# MAGIC COMMENT 'Threat intel rows whose indicator is not a valid CIDR.'
# MAGIC AS
# MAGIC SELECT * FROM threat_intel_raw WHERE try_ip_cidr(indicator) IS NULL;
# MAGIC
# MAGIC CREATE OR REPLACE TABLE merchant_allowlist
# MAGIC COMMENT 'Canonical per-merchant allowlisted CIDRs for sk_live keys.'
# MAGIC AS
# MAGIC SELECT
# MAGIC   merchant_id,
# MAGIC   ip_cidr(allowed_cidr)                AS allowed_cidr,
# MAGIC   ip_as_binary(ip_cidr(allowed_cidr))  AS allowed_cidr_bin,
# MAGIC   ip_version(allowed_cidr)             AS ip_version,
# MAGIC   allowed_cidr                         AS allowed_cidr_raw
# MAGIC FROM merchant_allowlist_raw;

# COMMAND ----------

# DBTITLE 1,What try_ip_cidr fixed and rejected
# MAGIC %sql
# MAGIC SELECT 'geoip_blocks' AS feed, 'canonicalized' AS outcome, count(*) AS rows, any_value(network_raw) AS raw_example, any_value(network) AS canonical_example
# MAGIC FROM geoip_blocks WHERE network_raw <> network
# MAGIC UNION ALL
# MAGIC SELECT 'threat_intel', 'canonicalized', count(*), max(indicator_raw), max(indicator)
# MAGIC FROM threat_intel WHERE indicator_raw <> indicator
# MAGIC UNION ALL
# MAGIC SELECT 'merchant_allowlist', 'canonicalized', count(*), any_value(allowed_cidr_raw), any_value(allowed_cidr)
# MAGIC FROM merchant_allowlist WHERE allowed_cidr_raw <> allowed_cidr
# MAGIC UNION ALL
# MAGIC SELECT 'threat_intel', 'rejected', count(*), array_join(collect_list(concat('"', indicator, '"')), ', '), NULL
# MAGIC FROM threat_intel_rejected

# COMMAND ----------

# DBTITLE 1,B4. GeoIP range join
# MAGIC %md
# MAGIC ## B4. GeoIP / ASN enrichment - the CIDR range join
# MAGIC ~4.9M requests x ~37K blocks of mixed sizes and both IP versions, in one `ON ip_cidr_contains(...)`. On BINARY keys this uses the Photon-optimized IP range join: no exploding CIDRs into rows, no integer math, no separate IPv6 path.

# COMMAND ----------

# DBTITLE 1,Build api_logs_enriched
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE api_logs_enriched
# MAGIC COMMENT 'API logs enriched with GeoIP country/city and ASN/operator via ip_cidr_contains range join.'
# MAGIC AS
# MAGIC SELECT
# MAGIC   l.*,
# MAGIC   g.network AS geo_network,
# MAGIC   g.country_code, g.country, g.city,
# MAGIC   g.asn, g.asn_org, g.connection_type
# MAGIC FROM api_logs_silver l
# MAGIC LEFT JOIN geoip_blocks g
# MAGIC   ON ip_cidr_contains(g.network_bin, l.ip_bin);
# MAGIC
# MAGIC SELECT
# MAGIC   ip_version,
# MAGIC   count(*)                                                  AS requests,
# MAGIC   count_if(country IS NOT NULL)                             AS geo_matched,
# MAGIC   round(100 * count_if(country IS NOT NULL) / count(*), 2)  AS match_pct,
# MAGIC   count(DISTINCT country)                                   AS countries,
# MAGIC   count(DISTINCT asn)                                       AS asns
# MAGIC FROM api_logs_enriched
# MAGIC GROUP BY ip_version
# MAGIC ORDER BY ip_version;

# COMMAND ----------

# DBTITLE 1,B5. Threat intel
# MAGIC %md
# MAGIC ## B5. Threat intel matching and interactive hunting
# MAGIC The same join pattern against every indicator - /32 Tor exits, /24 botnets, a /16 scanner, IPv6 /64 VPN ranges. Then the analyst follows a tip with a single `WHERE ip_cidr_contains(...)`.

# COMMAND ----------

# DBTITLE 1,Build threat_hits
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE threat_hits
# MAGIC COMMENT 'API requests whose source IP falls inside a threat intel indicator.'
# MAGIC AS
# MAGIC SELECT
# MAGIC   e.request_id, e.event_ts, e.ip, e.ip_version, e.subnet,
# MAGIC   e.merchant_id, e.api_key_id, e.endpoint, e.http_status, e.user_agent,
# MAGIC   e.country, e.asn_org,
# MAGIC   t.indicator, t.category, t.confidence, t.provider
# MAGIC FROM api_logs_enriched e
# MAGIC JOIN threat_intel t
# MAGIC   ON ip_cidr_contains(t.indicator_bin, e.ip_bin);
# MAGIC
# MAGIC SELECT
# MAGIC   category,
# MAGIC   any_value(provider)        AS provider,
# MAGIC   max(confidence)            AS max_confidence,
# MAGIC   count(*)                   AS requests,
# MAGIC   count(DISTINCT ip)         AS distinct_ips,
# MAGIC   count(DISTINCT indicator)  AS indicators_hit,
# MAGIC   collect_set(ip_version)    AS ip_versions,
# MAGIC   mode(endpoint)             AS top_endpoint
# MAGIC FROM threat_hits
# MAGIC GROUP BY category
# MAGIC ORDER BY max_confidence DESC;

# COMMAND ----------

# DBTITLE 1,Hunt: every request from 45.7.0.0/16 in the last 24h
# MAGIC %sql
# MAGIC -- Tip from a partner CERT: "check your logs for 45.7.0.0/16"
# MAGIC SELECT
# MAGIC   endpoint,
# MAGIC   http_status,
# MAGIC   count(*)               AS requests,
# MAGIC   count(DISTINCT ip)     AS distinct_ips,
# MAGIC   any_value(user_agent)  AS user_agent,
# MAGIC   any_value(asn_org)     AS operator
# MAGIC FROM api_logs_enriched
# MAGIC WHERE ip_cidr_contains('45.7.0.0/16', ip)
# MAGIC   AND event_ts >= current_timestamp() - INTERVAL 24 HOURS
# MAGIC GROUP BY ALL
# MAGIC ORDER BY requests DESC

# COMMAND ----------

# DBTITLE 1,B6. Allowlist
# MAGIC %md
# MAGIC ## B6. Secret-key IP allowlist enforcement
# MAGIC A `sk_live_` key may only be used from its merchant's allowlisted CIDRs. Anti join on `merchant_id` + `ip_cidr_contains` returns every call made from anywhere else.

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
# MAGIC WHERE e.api_key_id LIKE 'sk_live_%';
# MAGIC
# MAGIC SELECT
# MAGIC   m.merchant_name,
# MAGIC   v.subnet,
# MAGIC   v.ip_version,
# MAGIC   count(*)                                   AS requests,
# MAGIC   count_if(v.endpoint = 'POST /v1/payouts')  AS payout_calls,
# MAGIC   any_value(v.asn_org)                       AS operator,
# MAGIC   any_value(v.user_agent)                    AS user_agent
# MAGIC FROM allowlist_violations v
# MAGIC JOIN merchants m USING (merchant_id)
# MAGIC GROUP BY ALL
# MAGIC ORDER BY requests DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# DBTITLE 1,B7. Card testing
# MAGIC %md
# MAGIC ## B7. Behavioral detection - card testing by subnet
# MAGIC Card-testing bots rotate through every address in a subnet, so per-IP rules miss them. Aggregate on the subnet built with `ip_cidr`, flag high-volume / many-card / mostly-declined subnets, then ask the threat feed about each one with **CIDR-in-CIDR** containment.

# COMMAND ----------

# DBTITLE 1,Build card_testing_subnets
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE card_testing_subnets
# MAGIC COMMENT 'Subnets showing card-testing behaviour in the last 24h.'
# MAGIC AS
# MAGIC WITH s AS (
# MAGIC   SELECT
# MAGIC     subnet, ip_version,
# MAGIC     count(*)                                            AS requests,
# MAGIC     count(DISTINCT card_fingerprint)                    AS distinct_cards,
# MAGIC     count(DISTINCT ip)                                  AS distinct_ips,
# MAGIC     avg(CASE WHEN http_status = 402 THEN 1 ELSE 0 END)  AS decline_rate,
# MAGIC     collect_set(merchant_id)                            AS merchant_ids,
# MAGIC     any_value(country)                                  AS country,
# MAGIC     any_value(asn_org)                                  AS asn_org,
# MAGIC     min(event_ts)                                       AS first_seen,
# MAGIC     max(event_ts)                                       AS last_seen
# MAGIC   FROM api_logs_enriched
# MAGIC   WHERE endpoint = 'POST /v1/payment_intents/confirm'
# MAGIC     AND event_ts >= current_timestamp() - INTERVAL 24 HOURS
# MAGIC   GROUP BY subnet, ip_version
# MAGIC   HAVING count(*) >= 500 AND avg(CASE WHEN http_status = 402 THEN 1 ELSE 0 END) >= 0.5
# MAGIC ),
# MAGIC feed AS (
# MAGIC   SELECT s.subnet, max_by(t.category, t.confidence) AS feed_category
# MAGIC   FROM s JOIN threat_intel t ON ip_cidr_contains(t.indicator, s.subnet)
# MAGIC   GROUP BY s.subnet
# MAGIC )
# MAGIC SELECT s.*, feed.feed_category, feed.feed_category IS NOT NULL AS in_threat_feed
# MAGIC FROM s LEFT JOIN feed USING (subnet);
# MAGIC
# MAGIC SELECT subnet, requests, distinct_cards, distinct_ips, round(decline_rate * 100, 1) AS decline_pct,
# MAGIC        in_threat_feed, feed_category, country, asn_org
# MAGIC FROM card_testing_subnets
# MAGIC ORDER BY subnet;

# COMMAND ----------

# DBTITLE 1,B8. Legacy vs native
# MAGIC %md
# MAGIC ## B8. Why native functions? The legacy IPv4-only blind spot
# MAGIC The classic workaround - regex, `split` into four octets, integer math - silently ignores IPv6. This is what that pipeline would have missed.

# COMMAND ----------

# DBTITLE 1,What a legacy IPv4-only pipeline misses
# MAGIC %sql
# MAGIC WITH legacy AS (
# MAGIC   SELECT
# MAGIC     request_id,
# MAGIC     CASE WHEN src_ip_raw RLIKE '^[0-9]{1,3}[.][0-9]{1,3}[.][0-9]{1,3}[.][0-9]{1,3}$'
# MAGIC          THEN cast(split(src_ip_raw, '[.]')[0] AS BIGINT) * 16777216
# MAGIC             + cast(split(src_ip_raw, '[.]')[1] AS BIGINT) * 65536
# MAGIC             + cast(split(src_ip_raw, '[.]')[2] AS BIGINT) * 256
# MAGIC             + cast(split(src_ip_raw, '[.]')[3] AS BIGINT) END AS ip_int
# MAGIC   FROM api_logs_silver
# MAGIC )
# MAGIC SELECT 'All API requests' AS dataset, count(*) AS native_rows, count(ip_int) AS legacy_rows,
# MAGIC        count(*) - count(ip_int) AS invisible_to_legacy
# MAGIC FROM legacy
# MAGIC UNION ALL
# MAGIC SELECT concat('Threat hits: ', category), count(*), count_if(ip_version = 4), count_if(ip_version = 6)
# MAGIC FROM threat_hits GROUP BY category
# MAGIC UNION ALL
# MAGIC SELECT 'Secret-key allowlist violations', count(*), count_if(ip_version = 4), count_if(ip_version = 6)
# MAGIC FROM allowlist_violations
# MAGIC UNION ALL
# MAGIC SELECT 'Payouts from outside allowlist', count_if(endpoint = 'POST /v1/payouts'),
# MAGIC        count_if(endpoint = 'POST /v1/payouts' AND ip_version = 4), count_if(endpoint = 'POST /v1/payouts' AND ip_version = 6)
# MAGIC FROM allowlist_violations

# COMMAND ----------

# DBTITLE 1,B9. Incidents
# MAGIC %md
# MAGIC ## B9. Gold - security incidents and an AI-written SOC brief
# MAGIC One row per incident. The indicator is stored as compact BINARY (`ip_as_binary`) and rendered back with `ip_as_string`. `ai_gen` turns each incident into a short brief for the on-call analyst.

# COMMAND ----------

# DBTITLE 1,Build security_incidents
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE security_incidents
# MAGIC COMMENT 'Gold: one row per detected network security incident.'
# MAGIC AS
# MAGIC WITH incidents AS (
# MAGIC   -- Card testing (behavioral)
# MAGIC   SELECT 'card_testing' AS incident_type, 'critical' AS severity,
# MAGIC          ip_as_binary(subnet) AS indicator_bin, ip_version, merchant_ids[0] AS merchant_id,
# MAGIC          requests, distinct_ips, first_seen, last_seen, country, asn_org,
# MAGIC          concat(distinct_cards, ' distinct cards tried, ', round(decline_rate * 100, 1), '% declined; in threat feed: ',
# MAGIC                 CASE WHEN in_threat_feed THEN 'yes' ELSE 'no' END) AS evidence
# MAGIC   FROM card_testing_subnets
# MAGIC   UNION ALL
# MAGIC   -- Leaked secret key: sustained traffic from a subnet outside the allowlist
# MAGIC   SELECT 'leaked_secret_key', CASE WHEN count_if(endpoint = 'POST /v1/payouts') > 0 THEN 'critical' ELSE 'high' END,
# MAGIC          ip_as_binary(subnet), ip_version, merchant_id,
# MAGIC          count(*), count(DISTINCT ip), min(event_ts), max(event_ts), any_value(country), any_value(asn_org),
# MAGIC          concat('Secret key used from outside allowlist by ', any_value(user_agent), '; ',
# MAGIC                 count_if(endpoint = 'POST /v1/payouts'), ' payout calls')
# MAGIC   FROM allowlist_violations
# MAGIC   GROUP BY merchant_id, subnet, ip_version
# MAGIC   HAVING count(*) >= 100
# MAGIC   UNION ALL
# MAGIC   -- Credential stuffing from Tor exits
# MAGIC   SELECT 'credential_stuffing', 'high', NULL, ip_version, NULL,
# MAGIC          count(*), count(DISTINCT ip), min(event_ts), max(event_ts), 'Multiple', 'Tor network',
# MAGIC          concat(count(DISTINCT ip), ' Tor exit nodes, ', count(DISTINCT merchant_id), ' merchant dashboards targeted, ',
# MAGIC                 round(100 * avg(CASE WHEN http_status = 401 THEN 1 ELSE 0 END), 1), '% failed logins')
# MAGIC   FROM threat_hits
# MAGIC   WHERE category = 'tor_exit' AND endpoint = 'POST /v1/auth/token'
# MAGIC   GROUP BY ip_version
# MAGIC   UNION ALL
# MAGIC   -- Mass scanning
# MAGIC   SELECT 'mass_scanning', 'medium', ip_as_binary(indicator), ip_version, NULL,
# MAGIC          count(*), count(DISTINCT ip), min(event_ts), max(event_ts), any_value(country), any_value(asn_org),
# MAGIC          concat(count(DISTINCT endpoint), ' probe paths (', array_join(array_sort(collect_set(endpoint)), ', '), ')')
# MAGIC   FROM threat_hits
# MAGIC   WHERE category = 'scanner'
# MAGIC   GROUP BY indicator, ip_version
# MAGIC )
# MAGIC SELECT
# MAGIC   concat('INC-2611-', lpad(cast(row_number() OVER (ORDER BY CASE severity WHEN 'critical' THEN 1 WHEN 'high' THEN 2 ELSE 3 END,
# MAGIC                                                    requests DESC) AS STRING), 3, '0')) AS incident_id,
# MAGIC   i.incident_type,
# MAGIC   i.severity,
# MAGIC   coalesce(try_ip_as_string(i.indicator_bin), 'Tor exit list') AS indicator,
# MAGIC   i.indicator_bin,
# MAGIC   i.ip_version,
# MAGIC   coalesce(m.merchant_name, 'Multiple / none')                  AS merchant_name,
# MAGIC   i.requests, i.distinct_ips, i.first_seen, i.last_seen, i.country, i.asn_org, i.evidence
# MAGIC FROM incidents i
# MAGIC LEFT JOIN merchants m ON m.merchant_id = i.merchant_id;
# MAGIC
# MAGIC SELECT incident_id, incident_type, severity, indicator, hex(indicator_bin) AS indicator_hex, ip_version,
# MAGIC        merchant_name, requests, distinct_ips, country, asn_org, evidence
# MAGIC FROM security_incidents
# MAGIC ORDER BY incident_id;

# COMMAND ----------

# DBTITLE 1,AI incident brief with ai_gen
# MAGIC %sql
# MAGIC SELECT
# MAGIC   incident_id,
# MAGIC   incident_type,
# MAGIC   indicator,
# MAGIC   ai_gen(concat(
# MAGIC     'You are an on-call SOC analyst at Nimbus Payments, a card-payments API. ',
# MAGIC     'Write a 3-sentence incident brief: what happened, business impact, immediate containment action. ',
# MAGIC     'Be specific and factual, no preamble, no markdown. Incident: ',
# MAGIC     to_json(struct(incident_type, severity, indicator, ip_version, merchant_name, requests, distinct_ips,
# MAGIC                    country, asn_org, evidence, first_seen, last_seen))
# MAGIC   )) AS ai_brief
# MAGIC FROM security_incidents
# MAGIC ORDER BY incident_id

# COMMAND ----------

# DBTITLE 1,B10. WAF blocklist
# MAGIC %md
# MAGIC ## B10. Act - export a WAF blocklist
# MAGIC Edge firewalls take ranges. `ip_network` and `ip_network_last` turn each CIDR into start/end addresses. Tor exits are added as /32s.

# COMMAND ----------

# DBTITLE 1,Build waf_blocklist
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE waf_blocklist
# MAGIC COMMENT 'CIDRs to push to the edge WAF, with range bounds and the incident that justifies each rule.'
# MAGIC AS
# MAGIC WITH cidrs AS (
# MAGIC   SELECT indicator AS cidr, incident_id FROM security_incidents WHERE indicator_bin IS NOT NULL
# MAGIC   UNION ALL
# MAGIC   SELECT DISTINCT ip_cidr(concat(h.ip, '/32')), i.incident_id
# MAGIC   FROM threat_hits h
# MAGIC   CROSS JOIN (SELECT incident_id FROM security_incidents WHERE incident_type = 'credential_stuffing') i
# MAGIC   WHERE h.category = 'tor_exit'
# MAGIC )
# MAGIC SELECT
# MAGIC   cidr,
# MAGIC   ip_network(cidr)        AS range_start,
# MAGIC   ip_network_last(cidr)   AS range_end,
# MAGIC   ip_prefix_length(cidr)  AS prefix_length,
# MAGIC   ip_version(cidr)        AS ip_version,
# MAGIC   incident_id
# MAGIC FROM cidrs;
# MAGIC
# MAGIC SELECT ip_version, prefix_length, count(*) AS rules, min(cidr) AS example_cidr,
# MAGIC        min(range_start) AS example_start, min(range_end) AS example_end
# MAGIC FROM waf_blocklist
# MAGIC GROUP BY ALL
# MAGIC ORDER BY ip_version, prefix_length;

# COMMAND ----------

# DBTITLE 1,B11. Validation
# MAGIC %md
# MAGIC ## B11. Validation
# MAGIC Checks that each planted attack became exactly one incident with the expected indicator. Fails loudly if not.

# COMMAND ----------

# DBTITLE 1,Validate planted attacks
inc = spark.table("security_incidents").select("incident_type", "indicator", "merchant_name").toPandas()

expected = {
    ("card_testing", "185.3.17.0/24", "Bramble Home EU"),
    ("card_testing", "185.3.18.0/24", "Bramble Home EU"),
    ("leaked_secret_key", "2a0b:51e7:9c40:17a2::/64", "Tidewater Surf Australia"),
    ("credential_stuffing", "Tor exit list", "Multiple / none"),
    ("mass_scanning", "45.7.0.0/16", "Multiple / none"),
}
found = set(map(tuple, inc.values.tolist()))

missing, unexpected = expected - found, found - expected
print(f"Incidents: {len(inc)} | missing: {missing or 'none'} | unexpected: {unexpected or 'none'}")
assert not missing and not unexpected, "Detection results differ from the planted attacks"

# COMMAND ----------

# DBTITLE 1,Video intro and demo flow
# MAGIC %md
# MAGIC # Part C - Video intro and demo flow (~8 min)
# MAGIC
# MAGIC ## Intro (0:00 - 0:40)
# MAGIC > "Every login, every payment, every API call carries an IP address. And for years, analyzing them in SQL meant regex, integer math, and quietly ignoring IPv6.
# MAGIC >
# MAGIC > Today I'm the security engineer at Nimbus Payments. In the last week, five million requests hit our API - and somewhere in there are a card-testing botnet, a credential-stuffing crew on Tor, a leaked secret key draining payouts, and a scanner sweeping a whole /16.
# MAGIC >
# MAGIC > Let's find all four, in pure SQL, with the new native IP functions - now generally available on Databricks."
# MAGIC
# MAGIC ## Demo flow
# MAGIC
# MAGIC | Time | Section | What to show | Line to say |
# MAGIC | --- | --- | --- | --- |
# MAGIC | 0:40 | Part A (scroll) | Merchants, GeoIP feed, threat feed, raw logs | "Realistic data: 200 merchants, 37K GeoIP blocks, 5 threat feeds, 4.9M log lines." |
# MAGIC | 1:10 | B1 | 14 functions, one row | "Same functions for IPv4 and IPv6. `try_` versions return NULL instead of failing." |
# MAGIC | 1:50 | B2 profile | 175K expanded IPv6, 14K junk rows | "Real logs are messy. `try_ip_host` normalizes the good rows and quarantines the bad." |
# MAGIC | 2:30 | B2 silver | BINARY key 26.5 MB vs 61.7 MB of strings | "Parse once, store 4 or 16 bytes, join forever." |
# MAGIC | 3:00 | B3 | 738 GeoIP blocks fixed, 5 indicators rejected | "Feeds are messy too. `try_ip_cidr` clears host bits, normalizes IPv6, rejects garbage." |
# MAGIC | 3:30 | B4 | 100% geo match, v4 and v6 | "4.9 million requests against 37K CIDRs. One ON clause. Photon range join." |
# MAGIC | 4:10 | B5 hunt | `WHERE ip_cidr_contains('45.7.0.0/16', ip)` | "A partner CERT sends a /16. One predicate: zgrab hunting for `.env` files." |
# MAGIC | 4:50 | B6 | Tidewater Surf Australia, 8,000 calls, 4,759 payouts | "Their secret key is being used with curl, from an IPv6 VPN, to move money." |
# MAGIC | 5:40 | B7 | `185.3.18.0/24`, `in_threat_feed = false` | "The feed knew one botnet subnet. Behavior found its twin - CIDR-in-CIDR." |
# MAGIC | 6:20 | B8 | Legacy: 0 of 8,000 VPN hits, 60 of 4,819 payouts | "An IPv4-only pipeline would never have seen the leaked key." |
# MAGIC | 6:50 | B9 | 5 incidents + `ai_gen` briefs | "From raw rows to a SOC brief, still in SQL." |
# MAGIC | 7:20 | B10 + B11 | WAF ranges, validation passes | "Close the loop: ranges for the firewall, and a test that proves we caught everything." |
# MAGIC
# MAGIC ## Performance line
# MAGIC From the [launch blog](https://www.databricks.com/blog/ip-functions-are-generally-available-bringing-high-performance-network-analytics-lakehouse): IP CIDR joins up to **3.1x faster** and up to **6.4x cheaper** than another leading cloud data warehouse, tested up to 10B IPs x 10M CIDR blocks.
# MAGIC
# MAGIC ## Close and call to action (7:40 - 8:00)
# MAGIC > "Three takeaways: store IPs as BINARY, join with `ip_cidr_contains`, and use `try_` on anything raw. IP functions are GA on Databricks Runtime 18 LTS and serverless. The notebook is linked below - Run all, and go hunting."
# MAGIC
# MAGIC ## Before you record
# MAGIC * Run all once (timestamps are relative to now, so the 24h windows are always populated)
# MAGIC * Collapse Part A cells except A5 to keep the scroll short
# MAGIC * Zoom the B6, B7 and B8 result tables - those are the wow moments