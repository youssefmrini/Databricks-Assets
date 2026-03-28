-- Databricks notebook source
-- MAGIC %md
-- MAGIC # SQL Scripting (GA - January 2026)
-- MAGIC
-- MAGIC SQL Scripting is now generally available. It introduces full procedural SQL with variables, loops, conditionals, cursors, and exception handling — all natively in Databricks SQL.
-- MAGIC
-- MAGIC **Key capabilities:**
-- MAGIC - `DECLARE` / `SET` variables
-- MAGIC - `IF` / `CASE` conditionals
-- MAGIC - `LOOP` / `FOR` / `WHILE` loops with `LEAVE` and `ITERATE`
-- MAGIC - `OPEN` / `FETCH` / `CLOSE` cursors
-- MAGIC - Exception handling with condition handlers
-- MAGIC
-- MAGIC **Runtime:** DBR 17.3 LTS+ / Serverless SQL

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1. Variables and Basic Control Flow

-- COMMAND ----------

-- Simple variable declaration and arithmetic
BEGIN
  DECLARE total INT DEFAULT 0;
  DECLARE i INT DEFAULT 1;

  WHILE i <= 10 DO
    SET total = total + i;
    SET i = i + 1;
  END WHILE;

  SELECT total AS sum_1_to_10;
END;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2. IF / CASE Conditionals

-- COMMAND ----------

BEGIN
  DECLARE day_type STRING;
  DECLARE today STRING DEFAULT date_format(current_date(), 'EEEE');

  IF today IN ('Saturday', 'Sunday') THEN
    SET day_type = 'Weekend';
  ELSEIF today = 'Friday' THEN
    SET day_type = 'Almost weekend!';
  ELSE
    SET day_type = 'Weekday';
  END IF;

  SELECT today AS day_name, day_type;
END;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3. FOR Loops

-- COMMAND ----------

-- Generate a multiplication table using FOR loop
BEGIN
  DECLARE results ARRAY<STRING> DEFAULT ARRAY();

  DECLARE x INT DEFAULT 1;
  DECLARE y INT;
  WHILE x <= 5 DO
    SET y = 1;
    WHILE y <= 5 DO
      SET results = array_append(results, CONCAT(x, ' x ', y, ' = ', x * y));
      SET y = y + 1;
    END WHILE;
    SET x = x + 1;
  END WHILE;

  SELECT explode(results) AS multiplication_table;
END;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4. Cursor Processing

-- COMMAND ----------

-- Setup: create a sample table
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

-- Use cursor to process rows and build a report
BEGIN
  DECLARE emp_name STRING;
  DECLARE emp_dept STRING;
  DECLARE emp_salary DECIMAL(10,2);
  DECLARE total_processed INT DEFAULT 0;
  DECLARE done BOOLEAN DEFAULT FALSE;

  DECLARE emp_cursor CURSOR FOR
    SELECT name, department, salary FROM features_demo.sql_scripting.employees WHERE salary > 100000;

  DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

  OPEN emp_cursor;

  read_loop: LOOP
    FETCH emp_cursor INTO emp_name, emp_dept, emp_salary;
    IF done THEN
      LEAVE read_loop;
    END IF;
    SET total_processed = total_processed + 1;
  END LOOP;

  CLOSE emp_cursor;

  SELECT total_processed AS high_earners_count;
END;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 5. Exception Handling

-- COMMAND ----------

BEGIN
  DECLARE CONTINUE HANDLER FOR SQLEXCEPTION
  BEGIN
    SELECT 'An error occurred but we handled it gracefully' AS message;
  END;

  -- This will fail but be caught
  SELECT * FROM non_existent_table_xyz;
END;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 6. Practical Example: Dynamic Data Validation

-- COMMAND ----------

-- Validate data quality rules using SQL scripting
BEGIN
  DECLARE null_count INT;
  DECLARE dup_count INT;
  DECLARE issues ARRAY<STRING> DEFAULT ARRAY();

  -- Check for nulls
  SET null_count = (SELECT COUNT(*) FROM features_demo.sql_scripting.employees WHERE name IS NULL);
  IF null_count > 0 THEN
    SET issues = array_append(issues, CONCAT('Found ', null_count, ' null names'));
  END IF;

  -- Check for duplicates
  SET dup_count = (SELECT COUNT(*) - COUNT(DISTINCT id) FROM features_demo.sql_scripting.employees);
  IF dup_count > 0 THEN
    SET issues = array_append(issues, CONCAT('Found ', dup_count, ' duplicate IDs'));
  END IF;

  -- Report results
  IF size(issues) = 0 THEN
    SELECT 'All validations passed!' AS status;
  ELSE
    SELECT explode(issues) AS validation_issues;
  END IF;
END;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ---
-- MAGIC **Summary:** SQL Scripting enables complex procedural logic directly in SQL — no Python or external orchestration needed. Ideal for data validation, ETL procedures, and administrative tasks.
