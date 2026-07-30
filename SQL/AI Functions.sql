-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC SQL is crucial for data analysis due to its versatility, efficiency, and widespread use. Its simplicity enables swift retrieval, manipulation, and management of large datasets. Incorporating AI functions into SQL for data analysis enhances efficiency, which enables businesses to swiftly extract insights.
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC <a href="https://docs.databricks.com/en/sql/language-manual/functions/ai_analyze_sentiment.html"> Documentation of AI_ANALYZE_SENTIMENT  </a>
-- MAGIC <li> The ai_analyze_sentiment() function allows you to invoke a state-of-the-art generative AI model to perform sentiment analysis on input text using SQL </li>

-- COMMAND ----------

-- DBTITLE 1,AI analyze sentiment
SELECT ai_analyze_sentiment("I love my wife more than my job");

-- COMMAND ----------

-- MAGIC %md
-- MAGIC <a href="https://docs.databricks.com/en/sql/language-manual/functions/ai_extract.html"> Documentation of AI_Extract  </a>
-- MAGIC <li>The ai_extract() function allows you to invoke a state-of-the-art generative AI model to extract entities specified by labels from a given text using SQL </li>

-- COMMAND ----------

-- DBTITLE 1,AI Extract
SELECT ai_extract(
    'Youssef Mrini is from Morocco and is currently a Solution Architect At Databricks based in Paris',
    array('person', 'nationality', 'Company')
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC <a href="https://docs.databricks.com/en/sql/language-manual/functions/ai_fix_grammar.html"> Documentation of AI_fix_grammar  </a>
-- MAGIC <li> The ai_fix_grammar() function allows you to invoke a state-of-the-art generative AI model to correct grammatical errors in a given text using SQL </li>

-- COMMAND ----------

-- DBTITLE 1,AI fix grammar
SELECT ai_fix_grammar('my english is so god, I feel I m a native Spaker');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC <a href="https://docs.databricks.com/en/sql/language-manual/functions/ai_analyze_sentiment.html"> Documentation of AI_Classify  </a>
-- MAGIC <li>The ai_classify() function allows you to invoke a state-of-the-art generative AI model to classify input text according to labels you provide using SQL</li>

-- COMMAND ----------

-- DBTITLE 1,AI classify
SELECT ai_classify("My laptop is broken.", ARRAY("bad", "not bad"));

-- COMMAND ----------

-- MAGIC %md
-- MAGIC <a href="https://docs.databricks.com/en/sql/language-manual/functions/ai_gen.html"> Documentation of AI_Gen  </a>
-- MAGIC <li> The ai_gen() function invokes a state-of-the-art generative AI model to answer the user-provided prompt using SQL </li>

-- COMMAND ----------

-- DBTITLE 1,AI Gen
SELECT ai_gen('Generate a text praising morocco in 10 words');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC <a href="https://docs.databricks.com/en/sql/language-manual/functions/ai_mask.html"> Documentation of AI_Mask  </a>
-- MAGIC <li>The ai_mask() function allows you to invoke a state-of-the-art generative AI model to mask specified entities in a given text using SQL.</li>

-- COMMAND ----------

-- DBTITLE 1,AI Mask
SELECT ai_mask(
    'Youssef Mrini lives in Bezons. His salary is 100000000 euros per year',
    array('city', 'salary')
  );

-- COMMAND ----------

-- MAGIC %md
-- MAGIC <a href="https://docs.databricks.com/en/sql/language-manual/functions/ai_summarize.html"> Documentation of AI_Summarize  </a>
-- MAGIC <li> The ai_summarize() function allows you to invoke a state-of-the-art generative AI model to generate a summary of a given text using SQL </li>

-- COMMAND ----------

-- DBTITLE 1,AI Summarize
SELECT ai_summarize(
    'Apache Spark is a unified analytics engine for large-scale data processing. ' ||
    'It provides high-level APIs in Java, Scala, Python and R, and an optimized ' ||
    'engine that supports general execution graphs. It also supports a rich set ' ||
    'of higher-level tools including Spark SQL for SQL and structured data ' ||
    'processing, pandas API on Spark for pandas workloads, MLlib for machine ' ||
    'learning, GraphX for graph processing, and Structured Streaming for incremental ' ||
    'computation and stream processing.',
    20
  )

-- COMMAND ----------

-- MAGIC %md
-- MAGIC <a href="https://docs.databricks.com/en/sql/language-manual/functions/ai_translate.html"> Documentation of AI_Translate  </a>
-- MAGIC <li> The ai_translate() function allows you to invoke a state-of-the-art generative AI model to translate text to a specified target language using SQL </li>

-- COMMAND ----------

-- DBTITLE 1,AI Translate
SELECT ai_translate('I Speak spanish very well', 'es');
