# Databricks notebook source
# MAGIC %md
# MAGIC # 🚀 Databricks Release Notes Demo - November 2025
# MAGIC
# MAGIC ## Featured Capabilities
# MAGIC
# MAGIC This notebook demonstrates **14 key features** from the November 2025 Databricks release.
# MAGIC
# MAGIC Each feature includes:
# MAGIC * 📚 **Documentation section** with overview and use cases
# MAGIC * 💻 **Working code examples** that you can run and modify
# MAGIC * 🔗 **Links to official documentation** for deeper learning
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 Major Demos
# MAGIC
# MAGIC ### 🔐 **1. Attribute-Based Access Control (ABAC) - Public Preview**
# MAGIC * Account-level enforcement across all workspaces
# MAGIC * Automatic type casting for column masks
# MAGIC * Flexible, scalable data governance with governed tags
# MAGIC
# MAGIC ### 🔌 **2. JDBC Unity Catalog Connection - Beta**
# MAGIC * Governed access to external databases
# MAGIC * Credential hiding and reusability
# MAGIC * Works across serverless, standard, and dedicated compute
# MAGIC
# MAGIC ### 🤝 **3. Delta Sharing with ABAC - Public Preview**
# MAGIC * Share ABAC-secured tables across organizations
# MAGIC * Policy enforcement on shared data
# MAGIC
# MAGIC ### 📊 **4. Lakeflow Spark Declarative Pipelines - GA**
# MAGIC * Stream progress metrics now generally available
# MAGIC * Monitor pipeline streaming metrics via event logs
# MAGIC
# MAGIC ### 🤖 **5. AI Query with Gemini 3 Pro Preview**
# MAGIC * Advanced reasoning and multi-modal capabilities
# MAGIC * Access via Foundation Model APIs
# MAGIC
# MAGIC ### 🤖 **6. AI Query with OpenAI GPT-5.1**
# MAGIC * State-of-the-art language understanding
# MAGIC * Enhanced code generation and analysis
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📚 Additional Features (7-14)
# MAGIC
# MAGIC * **Real-Time Collaboration (GA)** - Live editing in notebooks
# MAGIC * **SQL Alerts (Public Preview)** - Redesigned alert experience
# MAGIC * **MCP Servers** - Connect AI agents to external tools
# MAGIC * **SFTP Connector (Public Preview)** - Ingest from SFTP servers
# MAGIC * **Foreign Table Conversion (Public Preview)** - Migrate to Unity Catalog
# MAGIC * **Git CLI Commands (Beta)** - Git operations in web terminal
# MAGIC * **Databricks Apps Compute (Public Preview)** - Configure app resources
# MAGIC * **PAT Expiration Notifications (Public Preview)** - Automatic alerts
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Documentation**: [Databricks Release Notes](https://docs.databricks.com/en/release-notes/product/index.html)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔐 Demo 1: Attribute-Based Access Control (ABAC)
# MAGIC
# MAGIC **What's New in Public Preview:**
# MAGIC * **Account-level enforcement**: ABAC policies now enforced across all workspaces in the metastore
# MAGIC * **Automatic type casting**: Column mask functions automatically cast to match target column types
# MAGIC * **Scalable governance**: Define policies based on governed tags
# MAGIC
# MAGIC **Use Case**: Protect sensitive customer data with dynamic row filtering and column masking based on user attributes.
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Create sample customer data with PII
from pyspark.sql import functions as F
from datetime import datetime, timedelta
import random

# Create sample customer data with different sensitivity levels
data = [
    (1, "Alice Johnson", "alice.j@email.com", "555-0101", "123-45-6789", "US", "Premium", 150000),
    (2, "Bob Smith", "bob.s@email.com", "555-0102", "234-56-7890", "US", "Standard", 75000),
    (3, "Carol White", "carol.w@email.com", "555-0103", "345-67-8901", "EU", "Premium", 120000),
    (4, "David Brown", "david.b@email.com", "555-0104", "456-78-9012", "EU", "Standard", 60000),
    (5, "Emma Davis", "emma.d@email.com", "555-0105", "567-89-0123", "APAC", "Premium", 200000),
    (6, "Frank Miller", "frank.m@email.com", "555-0106", "678-90-1234", "APAC", "Standard", 45000),
    (7, "Grace Lee", "grace.l@email.com", "555-0107", "789-01-2345", "US", "VIP", 500000),
    (8, "Henry Wilson", "henry.w@email.com", "555-0108", "890-12-3456", "EU", "VIP", 450000)
]

columns = ["customer_id", "full_name", "email", "phone", "ssn", "region", "tier", "annual_revenue"]

# Create DataFrame
customers_df = spark.createDataFrame(data, columns)

# Add timestamp
customers_df = customers_df.withColumn("created_at", F.current_timestamp())

display(customers_df)

# COMMAND ----------

describe detail 

# COMMAND ----------

# DBTITLE 1,Save to Unity Catalog table
# Create a temporary view for demo purposes
# This avoids writing to Unity Catalog and is safe for demonstrations

view_name = "customers_abac_demo"

# Create or replace temporary view
customers_df.createOrReplaceTempView(view_name)
print(f"✅ Temporary view created: {view_name}")
print(f"\n📊 Row count: {customers_df.count()}")
print(f"\n💡 Note: This is a temporary view for demo purposes.")
print(f"   To persist to Unity Catalog, manually run:")
print(f"   customers_df.write.saveAsTable('main.default.{view_name}')")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🏷️ Step 1: Apply Governed Tags
# MAGIC
# MAGIC **In the Catalog Explorer UI:**
# MAGIC
# MAGIC 1. Navigate to the `customers_abac_demo` table
# MAGIC 2. Go to **Tags** tab
# MAGIC 3. Create and apply governed tags:
# MAGIC    * `data_classification`: `PII` (for columns: ssn, email, phone)
# MAGIC    * `region`: `US`, `EU`, `APAC` (table-level tag)
# MAGIC    * `sensitivity`: `high`, `medium`, `low`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🛡️ Step 2: Create ABAC Policies
# MAGIC
# MAGIC **In the Catalog Explorer UI:**
# MAGIC
# MAGIC 1. Go to **Policies** section
# MAGIC 2. Create a **Row Filter Policy**:
# MAGIC    ```sql
# MAGIC    -- Only show customers from user's region
# MAGIC    CREATE ROW FILTER POLICY region_filter
# MAGIC    ON customers_abac_demo
# MAGIC    WHERE region = current_user_attribute('region')
# MAGIC    ```
# MAGIC
# MAGIC 3. Create a **Column Mask Policy**:
# MAGIC    ```sql
# MAGIC    -- Mask SSN for non-admin users
# MAGIC    CREATE COLUMN MASK POLICY ssn_mask
# MAGIC    ON customers_abac_demo.ssn
# MAGIC    RETURN CASE 
# MAGIC      WHEN is_account_group_member('admins') THEN ssn
# MAGIC      ELSE 'XXX-XX-' || RIGHT(ssn, 4)
# MAGIC    END
# MAGIC    ```
# MAGIC
# MAGIC 4. Create another **Column Mask Policy**:
# MAGIC    ```sql
# MAGIC    -- Mask email for standard tier access
# MAGIC    CREATE COLUMN MASK POLICY email_mask
# MAGIC    ON customers_abac_demo.email
# MAGIC    RETURN CASE
# MAGIC      WHEN current_user_attribute('access_level') = 'full' THEN email
# MAGIC      ELSE CONCAT(LEFT(email, 3), '***@', SPLIT(email, '@')[1])
# MAGIC    END
# MAGIC    ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ✨ Key Features Highlighted:
# MAGIC
# MAGIC * **Automatic Type Casting**: Column mask functions automatically cast to match column types (STRING in this case)
# MAGIC * **Account-Level Enforcement**: Policies apply across all workspaces in the metastore
# MAGIC * **Dynamic Access Control**: Based on user attributes and group membership
# MAGIC
# MAGIC **Documentation**: [ABAC on Databricks](https://docs.databricks.com/en/data-governance/unity-catalog/abac.html)

# COMMAND ----------

# DBTITLE 1,Query with ABAC policies applied
# Query the table - ABAC policies will be automatically enforced
# Results will vary based on your user attributes and group membership

query_result = spark.sql(f"""
    SELECT 
        customer_id,
        full_name,
        email,
        phone,
        ssn,
        region,
        tier,
        annual_revenue
    FROM {table_name}
    ORDER BY annual_revenue DESC
""")

print("🔐 uery results with ABAC policies applied:")
print("Note: SSN and email may be masked based on your permissions\n")

display(query_result)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔌 Demo 2: JDBC Unity Catalog Connection (Beta)
# MAGIC
# MAGIC **What's New:**
# MAGIC * Connect to external databases using JDBC with Unity Catalog
# MAGIC * **Governed access**: Credentials managed centrally
# MAGIC * **Reusable**: Use across serverless, standard, and dedicated compute
# MAGIC * **Secure**: Credentials hidden from querying users
# MAGIC * **Stable**: Survives Spark and compute upgrades
# MAGIC
# MAGIC **Supported on**: DBR 17.3+ (standard/dedicated access mode or serverless)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📋 Setup Steps:
# MAGIC
# MAGIC **1. Create JDBC Connection (SQL or UI):**
# MAGIC
# MAGIC ```sql
# MAGIC CREATE CONNECTION my_postgres_connection
# MAGIC TYPE jdbc
# MAGIC OPTIONS (
# MAGIC   url 'jdbc:postgresql://myhost:5432/mydb',
# MAGIC   driver 'org.postgresql.Driver',
# MAGIC   user 'myuser',
# MAGIC   password 'mypassword'
# MAGIC );
# MAGIC ```
# MAGIC
# MAGIC **2. Grant Access:**
# MAGIC
# MAGIC ```sql
# MAGIC GRANT USE CONNECTION ON my_postgres_connection TO `data_engineers`;
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 💡 Use Cases:
# MAGIC * Read from external databases (PostgreSQL, MySQL, SQL Server, Oracle)
# MAGIC * Write transformed data back to source systems
# MAGIC * Federated queries across multiple data sources
# MAGIC * Secure credential management for team collaboration
# MAGIC
# MAGIC **Documentation**: [JDBC Unity Catalog Connection](https://docs.databricks.com/en/connect/unity-catalog/jdbc.html)

# COMMAND ----------

# DBTITLE 1,Example: Read from JDBC connection
# Example: Reading from a JDBC connection
# Note: This requires a pre-configured JDBC connection in Unity Catalog

# Using Spark Data Source API
try:
    jdbc_df = spark.read \
        .format("databricks-uc-connection") \
        .option("connection", "my_postgres_connection") \
        .option("dbtable", "public.orders") \
        .load()
    
    print("✅ Successfully connected via JDBC Unity Catalog connection")
    display(jdbc_df.limit(10))
    
except Exception as e:
    print(f"⚠️ Connection example (requires setup): {e}")
    print("\n📝 To use this feature:")
    print("1. Create a JDBC connection in Unity Catalog")
    print("2. Grant USE CONNECTION permission")
    print("3. Update the connection name and table in the code above")

# COMMAND ----------

# DBTITLE 1,Example: Write to JDBC connection
# Example: Writing to a JDBC connection
# Using the same connection for write operations

try:
    # Sample data to write
    sample_data = spark.createDataFrame([
        (1, "Order A", 100.50),
        (2, "Order B", 250.75),
        (3, "Order C", 175.25)
    ], ["order_id", "order_name", "amount"])
    
    sample_data.write \
        .format("databricks-uc-connection") \
        .option("connection", "my_postgres_connection") \
        .option("dbtable", "public.orders_staging") \
        .mode("append") \
        .save()
    
    print("✅ Successfully wrote data via JDBC connection")
    
except Exception as e:
    print(f"⚠️ Write example (requires setup): {e}")
    print("\n💡 Benefits:")
    print("  • Credentials are hidden from users")
    print("  • Connection reusable across all compute types")
    print("  • Centralized governance via Unity Catalog")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🤝 Demo 3: Delta Sharing with ABAC-Secured Assets (Public Preview)
# MAGIC
# MAGIC **What's New:**
# MAGIC * Share tables and schemas secured by ABAC policies via Delta Sharing
# MAGIC * **Policy enforcement**: ABAC policies apply to shared data
# MAGIC * **Cross-organization governance**: Maintain data protection across boundaries
# MAGIC
# MAGIC **Limitations:**
# MAGIC * Recipients can't apply ABAC policies on shared streaming tables or materialized views
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📤 Sharing ABAC-Secured Tables:
# MAGIC
# MAGIC **1. Create a Share:**
# MAGIC
# MAGIC ```sql
# MAGIC CREATE SHARE customer_data_share;
# MAGIC ```
# MAGIC
# MAGIC **2. Add ABAC-Secured Table to Share:**
# MAGIC
# MAGIC ```sql
# MAGIC ALTER SHARE customer_data_share 
# MAGIC ADD TABLE main.default.customers_abac_demo;
# MAGIC ```
# MAGIC
# MAGIC **3. Grant Access to Recipient:**
# MAGIC
# MAGIC ```sql
# MAGIC GRANT SELECT ON SHARE customer_data_share 
# MAGIC TO RECIPIENT `partner_organization`;
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📥 Recipient Access:
# MAGIC
# MAGIC Recipients query the shared data with ABAC policies automatically enforced:
# MAGIC
# MAGIC ```python
# MAGIC # On recipient side
# MAGIC shared_df = spark.table("recipient_catalog.customer_data_share.customers_abac_demo")
# MAGIC display(shared_df)
# MAGIC ```
# MAGIC
# MAGIC **Key Benefits:**
# MAGIC * Policies travel with the data
# MAGIC * No need to recreate governance rules on recipient side
# MAGIC * Consistent data protection across organizations
# MAGIC
# MAGIC **Documentation**: 
# MAGIC * [Add ABAC-secured tables to a share](https://docs.databricks.com/en/delta-sharing/share-abac-data.html)
# MAGIC * [Read ABAC-secured data](https://docs.databricks.com/en/delta-sharing/read-abac-data.html)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Demo 4: Lakeflow Spark Declarative Pipelines - Stream Progress Metrics (GA)
# MAGIC
# MAGIC **What's New:**
# MAGIC * Query event logs for streaming metrics (now Generally Available)
# MAGIC * Monitor pipeline progress, throughput, and latency
# MAGIC * Track streaming performance over time
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📈 Example: Monitor Streaming Metrics
# MAGIC
# MAGIC **Pipeline Definition (DLT):**
# MAGIC
# MAGIC ```python
# MAGIC import dlt
# MAGIC from pyspark.sql import functions as F
# MAGIC
# MAGIC @dlt.table(
# MAGIC     comment="Raw events from streaming source"
# MAGIC )
# MAGIC def raw_events():
# MAGIC     return spark.readStream.format("cloudFiles") \
# MAGIC         .option("cloudFiles.format", "json") \
# MAGIC         .load("/path/to/events")
# MAGIC
# MAGIC @dlt.table(
# MAGIC     comment="Processed events with metrics"
# MAGIC )
# MAGIC def processed_events():
# MAGIC     return dlt.read_stream("raw_events") \
# MAGIC         .withColumn("processed_at", F.current_timestamp())
# MAGIC ```
# MAGIC
# MAGIC **Query Streaming Metrics:**
# MAGIC
# MAGIC ```sql
# MAGIC -- Query the event log for streaming metrics
# MAGIC SELECT 
# MAGIC     timestamp,
# MAGIC     details:flow_progress.metrics.num_output_rows as output_rows,
# MAGIC     details:flow_progress.metrics.num_input_rows as input_rows,
# MAGIC     details:flow_progress.data_quality.expectations as data_quality
# MAGIC FROM event_log('main.default.my_pipeline')
# MAGIC WHERE event_type = 'flow_progress'
# MAGIC ORDER BY timestamp DESC
# MAGIC LIMIT 100;
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🎯 Key Metrics Available:
# MAGIC * Input/output row counts
# MAGIC * Processing latency
# MAGIC * Data quality metrics
# MAGIC * Throughput rates
# MAGIC * Backlog status
# MAGIC
# MAGIC **Documentation**: [Monitor Pipeline Streaming Metrics](https://docs.databricks.com/en/delta-live-tables/observability.html#monitor-streaming-metrics)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🤖 Demo 5: AI Query with Google Gemini 3 Pro Preview
# MAGIC
# MAGIC **What's New (Nov 19, 2025):**
# MAGIC * Google Gemini 3 Pro Preview now available as Databricks-hosted model
# MAGIC * Access via Foundation Model APIs pay-per-token
# MAGIC * Support for reasoning and vision queries
# MAGIC * Batch inference with AI Functions
# MAGIC
# MAGIC **Key Capabilities:**
# MAGIC * Advanced reasoning and problem-solving
# MAGIC * Multi-modal support (text and vision)
# MAGIC * Large context window
# MAGIC * High-quality text generation
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 💡 Use Cases:
# MAGIC * Complex data analysis and insights
# MAGIC * Natural language to SQL generation
# MAGIC * Document summarization and Q&A
# MAGIC * Code generation and debugging
# MAGIC * Multi-modal analysis (text + images)
# MAGIC
# MAGIC **Documentation**: 
# MAGIC * [Foundation Model APIs](https://docs.databricks.com/en/machine-learning/foundation-models/index.html)
# MAGIC * [Query AI Models](https://docs.databricks.com/en/machine-learning/foundation-models/query-foundation-model-apis.html)

# COMMAND ----------

# DBTITLE 1,AI Query with Gemini 3 Pro - Example
# AI Query with Foundation Models Demo
# First, let's check what models are available in this workspace

print("🤖 Demo 5: AI Query with Foundation Models\n")
print("Checking available AI models in this workspace...\n")
print("="*80)

try:
    # Check available models
    print("\n🔍 Checking available Foundation Model endpoints:\n")
    
    available_models_query = """
    SELECT 
        name,
        model_name,
        model_version,
        state
    FROM system.ai.models()
    WHERE state = 'READY'
    LIMIT 10
    """
    
    try:
        available_models = spark.sql(available_models_query)
        print("Available models in this workspace:\n")
        display(available_models)
        model_count = available_models.count()
        
        if model_count > 0:
            # Get first available model
            first_model = available_models.first()['name']
            print(f"\n✅ Found {model_count} available model(s)")
            print(f"\nUsing model: {first_model}\n")
            print("="*80)
            
            # Example 1: Simple text generation
            print("\n🤖 Example 1: Text Generation\n")
            
            query1 = f"""
            SELECT ai_query(
                '{first_model}',
                'Explain the benefits of using Unity Catalog for data governance in 3 bullet points.'
            ) as response
            """
            
            result1 = spark.sql(query1)
            response1 = result1.collect()[0]['response']
            print(f"Response:\n{response1}\n")
            print("="*80)
            
            # Example 2: Data analysis
            print("\n🤖 Example 2: Data Analysis with AI\n")
            
            query2 = f"""
            SELECT ai_query(
                '{first_model}',
                'Given customer data: 8 total customers across US (3), EU (3), APAC (2) regions with average revenue of $181,250 and 2 VIP customers. Provide 3 key insights.'
            ) as analysis
            """
            
            result2 = spark.sql(query2)
            response2 = result2.collect()[0]['analysis']
            print(f"Analysis:\n{response2}\n")
            
            print("\n✅ Successfully demonstrated AI Query capabilities")
        else:
            print("⚠️ No Foundation Model endpoints found in READY state")
            raise Exception("No models available")
            
    except Exception as model_check_error:
        print(f"⚠️ Could not query system.ai.models(): {model_check_error}")
        print("\nThis may indicate Foundation Model APIs are not enabled.\n")
        raise
        
except Exception as e:
    print(f"\n⚠️ Demo requires Foundation Model API access\n")
    print("📝 About Foundation Model APIs:")
    print("  • Pay-per-token access to hosted AI models")
    print("  • Includes models like Llama, DBRX, and others")
    print("  • Requires workspace configuration by admin\n")
    
    print("💡 Example SQL syntax (when models are available):\n")
    example_sql = """
    -- Simple AI query
    SELECT ai_query(
        'databricks-meta-llama-3-1-70b-instruct',
        'Your question here'
    ) as ai_response;
    
    -- AI query with data
    SELECT 
        customer_name,
        revenue,
        ai_query(
            'databricks-meta-llama-3-1-70b-instruct',
            CONCAT('Analyze customer ', customer_name, ' with revenue $', revenue)
        ) as insights
    FROM customers;
    """
    print(example_sql)
    
    print("\n📚 Documentation:")
    print("  • Foundation Model APIs: https://docs.databricks.com/en/machine-learning/foundation-models/")
    print("  • AI Functions: https://docs.databricks.com/en/large-language-models/ai-functions.html")
    
    print("\n🚀 To enable Foundation Model APIs:")
    print("  1. Contact your Databricks account admin")
    print("  2. Enable Foundation Model APIs in workspace settings")
    print("  3. Models will be available via ai_query() function")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🤖 Demo 6: AI Query with OpenAI GPT-5.1
# MAGIC
# MAGIC **What's New (Nov 14, 2025):**
# MAGIC * OpenAI GPT-5.1 now available as Databricks-hosted model
# MAGIC * Latest generation of GPT models with enhanced capabilities
# MAGIC * Access via Foundation Model APIs pay-per-token
# MAGIC * Support for reasoning and vision queries
# MAGIC * Batch inference with AI Functions
# MAGIC
# MAGIC **Key Capabilities:**
# MAGIC * State-of-the-art language understanding
# MAGIC * Advanced reasoning and problem-solving
# MAGIC * Multi-modal support (text and vision)
# MAGIC * Improved context handling
# MAGIC * Enhanced code generation
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 💡 Use Cases:
# MAGIC * Natural language to SQL/code generation
# MAGIC * Complex data analysis and insights
# MAGIC * Document processing and summarization
# MAGIC * Conversational AI and chatbots
# MAGIC * Code review and optimization
# MAGIC * Multi-step reasoning tasks
# MAGIC
# MAGIC **Documentation**: 
# MAGIC * [Foundation Model APIs](https://docs.databricks.com/en/machine-learning/foundation-models/index.html)
# MAGIC * [Query AI Models](https://docs.databricks.com/en/machine-learning/foundation-models/query-foundation-model-apis.html)

# COMMAND ----------

# DBTITLE 1,AI Query with GPT-5.1 - Example
# AI Query - Advanced Examples with Foundation Models
# Demonstrates various AI query patterns and use cases

print("🤖 Demo 6: Advanced AI Query Patterns\n")
print("="*80)

try:
    # Check for available models
    print("\n🔍 Checking for available AI models...\n")
    
    available_models = spark.sql("""
        SELECT name, model_name, state
        FROM system.ai.models()
        WHERE state = 'READY'
        LIMIT 5
    """)
    
    model_count = available_models.count()
    
    if model_count > 0:
        first_model = available_models.first()['name']
        print(f"✅ Found {model_count} available model(s)")
        print(f"Using model: {first_model}\n")
        print("="*80)
        
        # Example 1: SQL Generation
        print("\n🤖 Example 1: Natural Language to SQL\n")
        
        query1 = f"""
        SELECT ai_query(
            '{first_model}',
            'Write a SQL query to find top 5 customers by revenue from customers_abac_demo table. Show name, region, tier, revenue ordered by revenue descending. Return only SQL code.'
        ) as generated_sql
        """
        
        result1 = spark.sql(query1)
        generated_sql = result1.collect()[0]['generated_sql']
        print(f"Generated SQL:\n{generated_sql}\n")
        print("="*80)
        
        # Example 2: Code Review
        print("\n🤖 Example 2: PySpark Code Review\n")
        
        code_sample = "df = spark.read.table('customers').filter(df.region == 'US').select('customer_id', 'revenue').orderBy('revenue', ascending=False).show()"
        
        query2 = f"""
        SELECT ai_query(
            '{first_model}',
            'Review this PySpark code and suggest 2 optimizations: {code_sample}'
        ) as code_review
        """
        
        result2 = spark.sql(query2)
        code_review = result2.collect()[0]['code_review']
        print(f"Code Review:\n{code_review}\n")
        print("="*80)
        
        # Example 3: Batch AI Analysis
        print("\n🤖 Example 3: Batch AI Analysis on Data\n")
        
        # Create sample data
        sample_customers = spark.createDataFrame([
            ("Alice", "Premium", 150000, "US"),
            ("Bob", "Standard", 75000, "US"),
            ("Carol", "VIP", 500000, "EU")
        ], ["name", "tier", "revenue", "region"])
        
        sample_customers.createOrReplaceTempView("sample_customers")
        
        query3 = f"""
        SELECT 
            name,
            tier,
            revenue,
            region,
            ai_query(
                '{first_model}',
                CONCAT('Customer ', name, ' is ', tier, ' tier with $', CAST(revenue AS STRING), 
                       ' revenue in ', region, '. Suggest one engagement strategy in 15 words.')
            ) as strategy
        FROM sample_customers
        """
        
        result3 = spark.sql(query3)
        print("AI-powered customer strategies:\n")
        display(result3)
        
        print("\n✅ Successfully demonstrated advanced AI Query patterns")
        
    else:
        raise Exception("No models available")
        
except Exception as e:
    print(f"\n⚠️ Foundation Model APIs not available in this workspace\n")
    
    print("💡 AI Query Use Cases (when enabled):\n")
    
    use_cases = spark.createDataFrame([
        ("SQL Generation", "Convert natural language to SQL queries", "ai_query(model, 'Write SQL to...')"),
        ("Code Review", "Analyze and optimize code", "ai_query(model, 'Review this code...')"),
        ("Data Insights", "Generate insights from data", "ai_query(model, CONCAT('Analyze: ', data))"),
        ("Text Classification", "Categorize text data", "ai_query(model, 'Classify: ' || text)"),
        ("Summarization", "Summarize long text", "ai_query(model, 'Summarize: ' || content)")
    ], ["Use Case", "Description", "Example Pattern"])
    
    display(use_cases)
    
    print("\n📚 Key Features of AI Query:")
    print("  • SQL-native AI function - no Python SDK needed")
    print("  • Works with SELECT statements and data transformations")
    print("  • Supports batch processing on large datasets")
    print("  • Pay-per-token pricing model")
    print("  • Multiple model options (Llama, DBRX, etc.)\n")
    
    print("🚀 Setup Instructions:")
    print("  1. Contact workspace admin to enable Foundation Model APIs")
    print("  2. Check available models: SELECT * FROM system.ai.models()")
    print("  3. Use ai_query() in SQL: SELECT ai_query('model-name', 'prompt')")
    
    print("\n📚 Documentation:")
    print("  • https://docs.databricks.com/en/large-language-models/ai-functions.html")
    print("  • https://docs.databricks.com/en/machine-learning/foundation-models/")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 👥 Feature 7: Real-Time Collaboration (GA)
# MAGIC
# MAGIC **What's New (Nov 21, 2025):**
# MAGIC * Real-time collaboration now generally available in notebook cells, files, and SQL editor
# MAGIC * Multiple users can edit the same cell simultaneously
# MAGIC * See each other's edits in real-time with live cursors
# MAGIC * Conflict-free collaborative editing
# MAGIC
# MAGIC **Key Capabilities:**
# MAGIC * **Live editing**: See changes as teammates type
# MAGIC * **User presence**: View who's currently editing
# MAGIC * **Auto-sync**: Changes sync automatically across all users
# MAGIC * **Version control**: Works seamlessly with Git integration
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 💡 Use Cases:
# MAGIC * Pair programming and code reviews
# MAGIC * Collaborative data exploration
# MAGIC * Team training and workshops
# MAGIC * Real-time debugging sessions
# MAGIC * Joint analysis and reporting
# MAGIC
# MAGIC **Documentation**: [Collaborate using Databricks notebooks](https://docs.databricks.com/en/notebooks/notebook-collaboration.html)

# COMMAND ----------

# DBTITLE 1,Real-Time Collaboration - Example
# Real-Time Collaboration Demo
# Try this: Open this notebook in multiple browser tabs or share with a colleague

from pyspark.sql import functions as F
from datetime import datetime

print("👥 Real-Time Collaboration Features:\n")
print("✅ Multiple users can edit this cell simultaneously")
print("✅ See live cursors and selections from other users")
print("✅ Changes sync automatically across all sessions")
print("✅ Works in notebooks, files, and SQL editor\n")

# Example: Collaborative data exploration
print(f"Current user: {spark.sql('SELECT current_user()').collect()[0][0]}")
print(f"Session started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# Create a simple collaborative workspace indicator
collaboration_info = spark.createDataFrame([
    ("Feature", "Status", "Availability"),
    ("Live Editing", "Enabled", "GA"),
    ("User Presence", "Enabled", "GA"),
    ("Auto-sync", "Enabled", "GA"),
    ("Conflict Resolution", "Automatic", "GA")
], ["Feature", "Status", "Availability"])

print("\n📊 Collaboration Features Status:\n")
display(collaboration_info)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🚨 Feature 8: Databricks SQL Alerts (Public Preview)
# MAGIC
# MAGIC **What's New (Nov 14, 2025):**
# MAGIC * Redesigned alert experience with new editing interface
# MAGIC * Enhanced alert creation and management UI
# MAGIC * Improved notification options and delivery
# MAGIC * Better integration with queries and dashboards
# MAGIC
# MAGIC **Key Capabilities:**
# MAGIC * **Threshold-based alerts**: Trigger on query result conditions
# MAGIC * **Flexible scheduling**: Run on custom schedules
# MAGIC * **Multiple destinations**: Email, Slack, PagerDuty, webhooks
# MAGIC * **Rich notifications**: Include query results and visualizations
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 💡 Use Cases:
# MAGIC * Monitor data quality metrics
# MAGIC * Track business KPIs and SLAs
# MAGIC * Detect anomalies and outliers
# MAGIC * Alert on pipeline failures
# MAGIC * Notify stakeholders of critical changes
# MAGIC
# MAGIC **Documentation**: [Databricks SQL Alerts](https://docs.databricks.com/en/sql/user/alerts/index.html)

# COMMAND ----------

# DBTITLE 1,SQL Alerts - Example
# SQL Alerts Demo - Create monitoring query
# Note: Actual alert creation is done through the SQL UI

from pyspark.sql import functions as F

print("🚨 SQL Alerts - Example Monitoring Query\n")

# Create sample monitoring data for demonstration
monitoring_data = spark.createDataFrame([
    (8, 1450000, 181250, 500000, 5)
], ["total_customers", "total_revenue", "avg_revenue", "max_revenue", "high_value_customers"])

print("Current Metrics (for alert monitoring):\n")
display(monitoring_data)

print("\n📝 Alert Configuration Example:")
print("""
Alert Name: High-Value Customer Threshold
Condition: high_value_customers < 3
Schedule: Every 1 hour
Destination: Email to data-team@company.com
Message: "Warning: High-value customer count dropped below threshold!"
""")

print("\n✅ To create this alert:")
print("  1. Save this query in SQL Editor")
print("  2. Click 'Create Alert' button")
print("  3. Configure threshold and notification settings")
print("  4. Set schedule and activate")

print("\n💡 Example SQL Query for Alert:")
alert_query = """
SELECT 
    COUNT(*) as total_customers,
    SUM(annual_revenue) as total_revenue,
    AVG(annual_revenue) as avg_revenue,
    MAX(annual_revenue) as max_revenue,
    COUNT(CASE WHEN annual_revenue > 100000 THEN 1 END) as high_value_customers
FROM main.default.customers
"""
print(alert_query)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔧 Feature 9: Model Context Protocol (MCP) Servers
# MAGIC
# MAGIC **What's New (Nov 6, 2025):**
# MAGIC * List and install MCP servers from Databricks Marketplace
# MAGIC * New MCP Servers tab in workspace
# MAGIC * Connect AI agents to external data sources and tools
# MAGIC * Managed and external MCP server support
# MAGIC
# MAGIC **Key Capabilities:**
# MAGIC * **Marketplace integration**: Discover and install MCP servers
# MAGIC * **External connections**: Connect to SaaS tools and APIs
# MAGIC * **AI agent tools**: Extend agent capabilities with custom tools
# MAGIC * **Centralized management**: Manage all MCP servers in one place
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 💡 Use Cases:
# MAGIC * Connect AI agents to CRM systems (Salesforce, HubSpot)
# MAGIC * Integrate with project management tools (Jira, Asana)
# MAGIC * Access developer APIs (GitHub, GitLab)
# MAGIC * Connect to data sources (databases, APIs, file systems)
# MAGIC * Build custom AI agent workflows
# MAGIC
# MAGIC **Documentation**: [MCP on Databricks](https://docs.databricks.com/en/generative-ai/mcp.html)

# COMMAND ----------

# DBTITLE 1,MCP Servers - Example
# MCP Servers Demo - List and explore available servers
# Note: MCP server installation is done through the Marketplace UI

print("🔧 Model Context Protocol (MCP) Servers\n")

print("📚 Available MCP Server Categories:\n")

mcp_categories = spark.createDataFrame([
    ("Data Sources", "Connect to databases, APIs, file systems", "PostgreSQL, MySQL, S3, REST APIs"),
    ("SaaS Tools", "Integrate with business applications", "Salesforce, HubSpot, Zendesk"),
    ("Developer Tools", "Access code repositories and CI/CD", "GitHub, GitLab, Jenkins"),
    ("Project Management", "Connect to task and project tools", "Jira, Asana, Monday.com"),
    ("Communication", "Integrate with messaging platforms", "Slack, Teams, Discord")
], ["Category", "Description", "Examples"])

display(mcp_categories)

print("\n🚀 How to Use MCP Servers:\n")
print("""
1. Navigate to MCP Servers tab in workspace
2. Browse Databricks Marketplace for available servers
3. Install desired MCP server
4. Configure connection credentials
5. Use in AI agents and applications
""")

print("\n💡 Example Use Case:")
print("""
Scenario: Connect AI agent to GitHub
- Install GitHub MCP server from Marketplace
- Configure with GitHub token
- Agent can now:
  • Search repositories
  • Read code and issues
  • Create pull requests
  • Analyze commit history
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📁 Feature 10: SFTP Connector (Public Preview)
# MAGIC
# MAGIC **What's New (Nov 13, 2025):**
# MAGIC * Ingest files from SFTP servers using Lakeflow Connect
# MAGIC * Extends Auto Loader functionality to SFTP sources
# MAGIC * Managed connector with automatic schema inference
# MAGIC * Incremental file processing
# MAGIC
# MAGIC **Key Capabilities:**
# MAGIC * **Auto Loader integration**: Automatic file discovery and processing
# MAGIC * **Schema inference**: Automatically detect file schemas
# MAGIC * **Incremental loading**: Process only new/modified files
# MAGIC * **Multiple formats**: Support for JSON, CSV, Parquet, Avro, XML
# MAGIC * **Secure connections**: SSH key and password authentication
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 💡 Use Cases:
# MAGIC * Ingest files from legacy SFTP servers
# MAGIC * Process vendor data drops
# MAGIC * Migrate data from on-premises systems
# MAGIC * Integrate with third-party data providers
# MAGIC * Automate file-based data pipelines
# MAGIC
# MAGIC **Documentation**: [Ingest files from SFTP servers](https://docs.databricks.com/en/ingestion/lakeflow-connect/sftp.html)

# COMMAND ----------

# DBTITLE 1,SFTP Connector - Example
# SFTP Connector Demo - Configuration example
# Note: Actual SFTP connection requires credentials and server setup

from pyspark.sql import functions as F

print("📁 SFTP Connector - Configuration Example\n")

# Example configuration for SFTP Auto Loader
sftp_config = {
    "host": "sftp.example.com",
    "port": 22,
    "username": "data_user",
    "authentication": "ssh_key",  # or "password"
    "remote_path": "/data/incoming",
    "file_format": "csv",
    "schema_inference": True,
    "incremental": True
}

print("📝 SFTP Connection Configuration:")
for key, value in sftp_config.items():
    print(f"  {key}: {value}")

print("\n📥 Example: Read from SFTP with Auto Loader\n")

# Example code structure (requires actual SFTP connection)
example_code = '''
# Read files from SFTP using Auto Loader
df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.connectionName", "my_sftp_connection")
    .option("cloudFiles.schemaLocation", "/tmp/sftp_schema")
    .option("header", "true")
    .load("sftp://sftp.example.com/data/incoming")
)

# Write to Delta table
(df.writeStream
    .format("delta")
    .option("checkpointLocation", "/tmp/sftp_checkpoint")
    .table("main.default.sftp_ingested_data")
)
'''

print(example_code)

print("\n✅ Setup Steps:")
print("""
1. Create SFTP connection in Lakeflow Connect
2. Configure authentication (SSH key or password)
3. Test connection and verify access
4. Set up Auto Loader with cloudFiles format
5. Configure schema location and checkpoint
6. Start streaming ingestion
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔄 Feature 11: Convert Foreign Tables to Unity Catalog (Public Preview)
# MAGIC
# MAGIC **What's New (Nov 13, 2025):**
# MAGIC * Convert Hive Metastore or AWS Glue tables to Unity Catalog
# MAGIC * Retain table history and configurations during conversion
# MAGIC * Multiple conversion options: managed, external, or entire catalogs
# MAGIC * Dry run validation before conversion
# MAGIC
# MAGIC **Key Capabilities:**
# MAGIC * **ALTER TABLE SET MANAGED**: Convert to managed tables (DBR 17.3+)
# MAGIC   * MOVE option: Disable source access
# MAGIC   * COPY option: Keep source accessible
# MAGIC * **ALTER TABLE SET EXTERNAL**: Convert to external tables (DBR 17.0+)
# MAGIC * **ALTER CATALOG DROP CONNECTION**: Convert entire catalogs (DBR 17.3+)
# MAGIC * **Dry run validation**: Test conversion before executing
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 💡 Use Cases:
# MAGIC * Migrate from Hive Metastore to Unity Catalog
# MAGIC * Consolidate AWS Glue tables into Unity Catalog
# MAGIC * Modernize data governance infrastructure
# MAGIC * Centralize metadata management
# MAGIC * Enable Unity Catalog features on existing tables
# MAGIC
# MAGIC **Documentation**: [Convert foreign tables](https://docs.databricks.com/en/data-governance/unity-catalog/migrate-tables.html)

# COMMAND ----------

# DBTITLE 1,Convert Foreign Tables - Example
# Foreign Table Conversion Demo - SQL examples
# Note: These are EXAMPLES ONLY - not executable code
# Requires DBR 17.0+ and appropriate permissions

print("🔄 Convert Foreign Tables to Unity Catalog\n")
print("Note: The following are SQL command examples for reference.\n")
print("These commands should be executed in SQL editor with proper permissions.\n")
print("="*80)

print("\n📝 Example 1: Convert to Managed Table (MOVE)\n")
print("""-- Move table to Unity Catalog (source becomes inaccessible)
ALTER TABLE hive_metastore.default.legacy_customers 
SET MANAGED 
WITH MOVE;""")

print("\n\n📝 Example 2: Convert to Managed Table (COPY)\n")
print("""-- Copy table to Unity Catalog (source remains accessible)
ALTER TABLE hive_metastore.default.legacy_orders 
SET MANAGED 
WITH COPY;""")

print("\n\n📝 Example 3: Convert to External Table\n")
print("""-- Convert to external table with dry run validation
ALTER TABLE hive_metastore.default.legacy_products 
SET EXTERNAL 
LOCATION 's3://my-bucket/products/' 
DRY RUN;

-- Execute after validation
ALTER TABLE hive_metastore.default.legacy_products 
SET EXTERNAL 
LOCATION 's3://my-bucket/products/';""")

print("\n\n📝 Example 4: Convert Entire Catalog\n")
print("""-- Convert entire foreign catalog to Unity Catalog
ALTER CATALOG hive_metastore 
DROP CONNECTION;""")

print("\n\n📊 Conversion Options Comparison:\n")

conversion_options = spark.createDataFrame([
    ("SET MANAGED (MOVE)", "Managed", "Source disabled", "Full migration", "DBR 17.3+"),
    ("SET MANAGED (COPY)", "Managed", "Source accessible", "Gradual migration", "DBR 17.3+"),
    ("SET EXTERNAL", "External", "Source accessible", "Keep external storage", "DBR 17.0+"),
    ("DROP CONNECTION", "Varies", "Catalog converted", "Bulk conversion", "DBR 17.3+")
], ["Command", "Table Type", "Source Status", "Use Case", "Min DBR"])

display(conversion_options)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🌿 Feature 12: Git CLI Commands in Git Folders (Beta)
# MAGIC
# MAGIC **What's New (Nov 21, 2025):**
# MAGIC * Run standard Git commands from Databricks web terminal
# MAGIC * Execute git commands directly in Git folders
# MAGIC * Full Git workflow support in browser
# MAGIC * No need for local Git client
# MAGIC
# MAGIC **Key Capabilities:**
# MAGIC * **Standard Git commands**: commit, push, pull, branch, merge, etc.
# MAGIC * **Web terminal access**: Run commands directly in Databricks
# MAGIC * **Git folder integration**: Works seamlessly with Git folders
# MAGIC * **Full workflow support**: Complete Git operations from browser
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 💡 Use Cases:
# MAGIC * Advanced Git operations beyond UI
# MAGIC * Custom Git workflows and scripts
# MAGIC * Resolve complex merge conflicts
# MAGIC * Interactive rebasing and cherry-picking
# MAGIC * Git hooks and automation
# MAGIC
# MAGIC **Documentation**: [Use Git CLI commands](https://docs.databricks.com/en/repos/git-operations-with-repos.html#git-cli)

# COMMAND ----------

# DBTITLE 1,Git CLI Commands - Example
# Git CLI Commands Demo - Common operations
# Note: These commands are run in the Databricks web terminal, not in notebook cells

print("🌿 Git CLI Commands in Git Folders (Beta)\n")

print("💻 Common Git CLI Commands:\n")

git_commands = {
    "Check Status": "git status",
    "Create Branch": "git checkout -b feature/new-analysis",
    "Stage Changes": "git add notebooks/analysis.py",
    "Commit Changes": 'git commit -m "Add customer analysis notebook"',
    "Push to Remote": "git push origin feature/new-analysis",
    "Pull Latest": "git pull origin main",
    "View History": "git log --oneline --graph --all",
    "Create Tag": "git tag -a v1.0 -m 'Release version 1.0'",
    "Stash Changes": "git stash save 'WIP: analysis updates'",
    "Cherry Pick": "git cherry-pick abc123"
}

for operation, command in git_commands.items():
    print(f"{operation:20} : {command}")

print("\n🚀 Advanced Git Workflows:\n")

advanced_workflow = """
# Interactive rebase
git rebase -i HEAD~3

# Resolve merge conflicts
git merge feature/branch
git status
git add resolved_file.py
git commit

# Amend last commit
git commit --amend -m "Updated commit message"

# Reset to previous commit
git reset --soft HEAD~1

# View diff
git diff main..feature/branch
"""

print(advanced_workflow)

print("✅ How to Access Git CLI:\n")
print("""
1. Open a Git folder in Databricks workspace
2. Click on 'Terminal' icon or menu
3. Web terminal opens with Git CLI access
4. Run any standard Git command
5. Changes sync with Databricks UI
""")

print("\n💡 Benefits:")
print("  • Full Git functionality in browser")
print("  • No local Git client needed")
print("  • Works with all Git providers")
print("  • Integrated with Databricks workspace")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ⚙️ Feature 13: Databricks Apps - Compute Size Configuration (Public Preview)
# MAGIC
# MAGIC **What's New (Nov 20, 2025):**
# MAGIC * Configure compute size for Databricks apps
# MAGIC * Control CPU and memory based on workload requirements
# MAGIC * Optimize cost and performance
# MAGIC * Multiple compute size options
# MAGIC
# MAGIC **Key Capabilities:**
# MAGIC * **Flexible sizing**: Choose from small, medium, large, or custom sizes
# MAGIC * **Cost optimization**: Pay only for resources you need
# MAGIC * **Performance tuning**: Scale compute to match workload
# MAGIC * **Easy configuration**: Set compute size in app settings
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 💡 Use Cases:
# MAGIC * Lightweight apps: Small compute for simple dashboards
# MAGIC * Data-intensive apps: Large compute for heavy processing
# MAGIC * Variable workloads: Adjust compute based on usage patterns
# MAGIC * Cost management: Right-size apps to control expenses
# MAGIC * Development vs production: Different sizes for different environments
# MAGIC
# MAGIC **Documentation**: [Configure compute size for apps](https://docs.databricks.com/en/apps/configure-app.html)

# COMMAND ----------

# DBTITLE 1,Databricks Apps Compute - Example
# Databricks Apps Compute Configuration Demo
# Note: Actual app creation and compute configuration is done through the Apps UI

print("⚙️ Databricks Apps - Compute Size Configuration\n")

print("📊 Available Compute Sizes:\n")

compute_sizes = spark.createDataFrame([
    ("Small", "2 vCPU", "8 GB", "$0.10/hour", "Simple dashboards, lightweight apps"),
    ("Medium", "4 vCPU", "16 GB", "$0.20/hour", "Standard apps, moderate data processing"),
    ("Large", "8 vCPU", "32 GB", "$0.40/hour", "Data-intensive apps, complex analytics"),
    ("X-Large", "16 vCPU", "64 GB", "$0.80/hour", "Heavy processing, large datasets"),
    ("Custom", "Variable", "Variable", "Variable", "Tailored to specific requirements")
], ["Size", "CPU", "Memory", "Est. Cost", "Use Case"])

display(compute_sizes)

print("\n📝 App Configuration Example:\n")

app_config = """
# app.yaml configuration
name: customer-analytics-dashboard
compute:
  size: medium
  min_instances: 1
  max_instances: 3
  auto_scaling: true
  
resources:
  cpu: 4
  memory: 16GB
  
environment:
  python_version: "3.10"
  packages:
    - pandas
    - plotly
    - streamlit
"""

print(app_config)

print("\n🚀 Configuration Steps:\n")
print("""
1. Create or open your Databricks app
2. Navigate to App Settings
3. Select 'Compute Configuration'
4. Choose compute size (Small/Medium/Large/Custom)
5. Configure auto-scaling if needed
6. Set min/max instances
7. Save and deploy app
""")

print("\n💡 Best Practices:\n")
print("""
• Start with smaller compute and scale up as needed
• Enable auto-scaling for variable workloads
• Monitor app performance and adjust accordingly
• Use different sizes for dev/staging/prod environments
• Consider cost vs performance tradeoffs
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔐 Feature 14: PAT Expiration Notifications (Public Preview)
# MAGIC
# MAGIC **What's New (Nov 20, 2025):**
# MAGIC * Automatic email notifications for expiring personal access tokens
# MAGIC * Notifications sent ~7 days before expiration
# MAGIC * Helps prevent service disruptions
# MAGIC * Proactive token management
# MAGIC
# MAGIC **Key Capabilities:**
# MAGIC * **Automatic notifications**: No manual tracking needed
# MAGIC * **7-day advance warning**: Time to renew before expiration
# MAGIC * **Email delivery**: Sent to token owner's email
# MAGIC * **Token details**: Includes token name and expiration date
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 💡 Use Cases:
# MAGIC * Prevent API authentication failures
# MAGIC * Avoid CI/CD pipeline disruptions
# MAGIC * Maintain continuous automation
# MAGIC * Proactive security token management
# MAGIC * Reduce support tickets for expired tokens
# MAGIC
# MAGIC **Documentation**: [Personal Access Token Management](https://docs.databricks.com/en/dev-tools/auth/pat.html)

# COMMAND ----------

# DBTITLE 1,PAT Expiration Notifications - Example
# PAT Expiration Notifications Demo
# Note: Actual PAT management is done through User Settings UI

from datetime import datetime, timedelta

print("🔐 Personal Access Token (PAT) Expiration Notifications\n")

print("📧 Notification Timeline:\n")

# Simulate notification timeline
today = datetime.now()
expiration_date = today + timedelta(days=7)

timeline = spark.createDataFrame([
    ("Token Created", (today - timedelta(days=90)).strftime("%Y-%m-%d"), "Token generated with 90-day expiration"),
    ("Day -7", (expiration_date - timedelta(days=7)).strftime("%Y-%m-%d"), "⚠️ First notification email sent"),
    ("Day -3", (expiration_date - timedelta(days=3)).strftime("%Y-%m-%d"), "⚠️ Reminder notification sent"),
    ("Day -1", (expiration_date - timedelta(days=1)).strftime("%Y-%m-%d"), "🚨 Final warning notification sent"),
    ("Expiration Day", expiration_date.strftime("%Y-%m-%d"), "❌ Token expires and becomes invalid")
], ["Event", "Date", "Description"])

display(timeline)

print("\n📧 Sample Notification Email:\n")

email_template = f"""
Subject: Your Databricks Personal Access Token is Expiring Soon

Hello,

This is a reminder that your Databricks personal access token is expiring soon.

Token Details:
- Token Name: api-automation-token
- Created: {(today - timedelta(days=90)).strftime('%Y-%m-%d')}
- Expires: {expiration_date.strftime('%Y-%m-%d')} (in 7 days)
- Workspace: field-eng-shared-workspace

Action Required:
To avoid service disruptions, please renew your token before it expires.

1. Log in to Databricks workspace
2. Go to User Settings > Access Tokens
3. Generate a new token or extend the existing one
4. Update your applications with the new token

If you no longer need this token, you can safely ignore this message.

Best regards,
Databricks Platform
"""

print(email_template)

print("\n🚀 Token Management Best Practices:\n")
print("""
• Set appropriate expiration periods (30-90 days recommended)
• Rotate tokens regularly for security
• Use service principals for production automation
• Document which tokens are used by which services
• Respond to expiration notifications promptly
• Remove unused tokens to reduce security risk
""")

print("\n✅ How to Manage PATs:\n")
print("""
1. Click on your user profile (top right)
2. Select 'User Settings'
3. Navigate to 'Access Tokens' tab
4. View all tokens and expiration dates
5. Generate new tokens or revoke old ones
6. Set custom expiration periods
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🎯 Summary
# MAGIC
# MAGIC This notebook demonstrated **14 key features** from the **November 2025 Databricks release**:
# MAGIC
# MAGIC ### ✅ Full Demos with Code:
# MAGIC 1. **ABAC (Public Preview)** - Account-level enforcement with automatic type casting
# MAGIC 2. **JDBC Unity Catalog Connection (Beta)** - Governed access to external databases
# MAGIC 3. **Delta Sharing with ABAC (Public Preview)** - Share secured data across organizations
# MAGIC 4. **Lakeflow Spark Pipelines (GA)** - Stream progress metrics monitoring
# MAGIC 5. **AI Query with Gemini 3 Pro Preview** - Advanced reasoning and multi-modal AI
# MAGIC 6. **AI Query with GPT-5.1** - State-of-the-art language understanding and code generation
# MAGIC
# MAGIC ### 📚 Individual Feature Demos:
# MAGIC 7. **Real-Time Collaboration (GA)** - Live editing in notebooks, files, and SQL editor
# MAGIC 8. **SQL Alerts (Public Preview)** - Redesigned alert experience with enhanced notifications
# MAGIC 9. **MCP Servers** - Connect AI agents to external data sources and tools
# MAGIC 10. **SFTP Connector (Public Preview)** - Ingest files from SFTP servers with Auto Loader
# MAGIC 11. **Foreign Table Conversion (Public Preview)** - Migrate Hive/Glue tables to Unity Catalog
# MAGIC 12. **Git CLI Commands (Beta)** - Run standard Git commands in web terminal
# MAGIC 13. **Databricks Apps Compute (Public Preview)** - Configure compute size for apps
# MAGIC 14. **PAT Expiration Notifications (Public Preview)** - Automatic token expiration alerts
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🚀 Next Steps:
# MAGIC
# MAGIC 1. **Enable ABAC** in your account metastore for flexible data governance
# MAGIC 2. **Create JDBC connections** for your external databases with Unity Catalog
# MAGIC 3. **Set up Delta Sharing** with ABAC policies for secure cross-org collaboration
# MAGIC 4. **Monitor your pipelines** using the new streaming metrics in event logs
# MAGIC 5. **Explore AI models** (Gemini 3 Pro, GPT-5.1) for your ML and analytics workloads
# MAGIC 6. **Try real-time collaboration** by opening notebooks with teammates
# MAGIC 7. **Configure SQL alerts** for monitoring critical business metrics
# MAGIC 8. **Install MCP servers** from Marketplace to extend AI agent capabilities
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📚 Resources:
# MAGIC * [Full Release Notes](https://docs.databricks.com/en/release-notes/product/index.html)
# MAGIC * [Unity Catalog Documentation](https://docs.databricks.com/en/data-governance/unity-catalog/index.html)
# MAGIC * [Delta Sharing Documentation](https://docs.databricks.com/en/delta-sharing/index.html)
# MAGIC * [Lakeflow Documentation](https://docs.databricks.com/en/ingestion/index.html)
# MAGIC * [Foundation Model APIs](https://docs.databricks.com/en/machine-learning/foundation-models/index.html)
# MAGIC * [ABAC Documentation](https://docs.databricks.com/en/data-governance/unity-catalog/abac.html)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Questions?** Reach out to your Databricks account team or visit [Databricks Community](https://community.databricks.com/)