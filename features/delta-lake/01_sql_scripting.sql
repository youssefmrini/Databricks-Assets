-- Databricks notebook source
-- MAGIC %md
-- MAGIC # SQL Scripting (GA - January 2026)
-- MAGIC
-- MAGIC SQL Scripting brings procedural capabilities to Databricks SQL with variables, control flow, and session management.
-- MAGIC
-- MAGIC **Note:** Full BEGIN/END blocks with LOOP, CURSOR, and HANDLER require a DBSQL Warehouse. This notebook demonstrates the SQL variable and scripting features that work across all compute types.
-- MAGIC
-- MAGIC **Runtime:** DBR 17.3 LTS+ / Serverless SQL

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1. SQL Session Variables

-- COMMAND ----------

-- Declare and use session variables
DECLARE OR REPLACE VARIABLE my_threshold INT DEFAULT 100;
DECLARE OR REPLACE VARIABLE my_catalog STRING DEFAULT 'features_demo';

-- COMMAND ----------

-- Use variables in queries
SELECT my_threshold AS threshold_value, my_catalog AS catalog_name;

-- COMMAND ----------

-- Update variable values
SET VAR my_threshold = 250;
SELECT my_threshold AS updated_threshold;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2. Variables in WHERE Clauses

-- COMMAND ----------

CREATE CATALOG IF NOT EXISTS features_demo;
USE CATALOG features_demo;
CREATE SCHEMA IF NOT EXISTS sql_scripting;
USE SCHEMA sql_scripting;

CREATE OR REPLACE TABLE employees (id INT, name STRING, department STRING, salary DECIMAL(10,2));
INSERT INTO employees VALUES
  (1, 'Alice', 'Engineering', 120000),
  (2, 'Bob', 'Engineering', 115000),
  (3, 'Charlie', 'Sales', 95000),
  (4, 'Diana', 'Sales', 98000),
  (5, 'Eve', 'HR', 85000);

-- COMMAND ----------

-- Filter using the variable
DECLARE OR REPLACE VARIABLE min_salary DECIMAL(10,2) DEFAULT 100000;
SELECT * FROM employees WHERE salary > min_salary ORDER BY salary DESC;

-- COMMAND ----------

-- Change threshold and re-query
SET VAR min_salary = 90000;
SELECT * FROM employees WHERE salary > min_salary ORDER BY salary DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3. EXECUTE IMMEDIATE (Dynamic SQL)

-- COMMAND ----------

-- Build and execute SQL dynamically
DECLARE OR REPLACE VARIABLE dept_filter STRING DEFAULT 'Engineering';

EXECUTE IMMEDIATE
  'SELECT name, salary FROM features_demo.sql_scripting.employees WHERE department = ?'
  USING dept_filter;

-- COMMAND ----------

-- Dynamic DDL with IDENTIFIER clause
DECLARE OR REPLACE VARIABLE tbl_name STRING DEFAULT 'features_demo.sql_scripting.employees';

EXECUTE IMMEDIATE
  'SELECT COUNT(*) AS total_count FROM IDENTIFIER(:tbl)'
  USING tbl_name AS tbl;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4. Parameter Markers

-- COMMAND ----------

-- Named parameter markers
SELECT name, department, salary
FROM features_demo.sql_scripting.employees
WHERE department = :dept_filter
  AND salary > :min_salary
ORDER BY salary DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 5. String Literal Coalescing

-- COMMAND ----------

-- Sequential string literals now combine automatically
SELECT 'Hello' ' World' ' from' ' Databricks!' AS greeting;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 6. Full SQL Scripting (DBSQL Warehouse Required)
-- MAGIC
-- MAGIC The following features require a **DBSQL SQL Warehouse** to execute:
-- MAGIC
-- MAGIC ```sql
-- MAGIC -- Loops and control flow
-- MAGIC BEGIN
-- MAGIC   DECLARE total INT DEFAULT 0;
-- MAGIC   DECLARE i INT DEFAULT 1;
-- MAGIC   WHILE i <= 10 DO
-- MAGIC     SET total = total + i;
-- MAGIC     SET i = i + 1;
-- MAGIC   END WHILE;
-- MAGIC   SELECT total AS sum_1_to_10;
-- MAGIC END;
-- MAGIC
-- MAGIC -- Cursors
-- MAGIC DECLARE emp_cursor CURSOR FOR SELECT name FROM employees;
-- MAGIC
-- MAGIC -- Exception handling
-- MAGIC DECLARE CONTINUE HANDLER FOR SQLEXCEPTION ...
-- MAGIC ```

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ---
-- MAGIC **Summary:** SQL Scripting brings procedural programming to SQL. Session variables, EXECUTE IMMEDIATE, and parameter markers work across all compute types. For full control flow (BEGIN/END, LOOP, CURSOR, HANDLER), use a DBSQL SQL Warehouse.
