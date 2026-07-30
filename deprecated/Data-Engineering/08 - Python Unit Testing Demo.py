# Databricks notebook source
# MAGIC %md
# MAGIC # Python Unit Testing in Databricks
# MAGIC
# MAGIC This demo showcases how to write and run unit tests in Databricks notebooks using **pytest**.
# MAGIC
# MAGIC ## Key Concepts Covered:
# MAGIC * Installing and using pytest in Databricks
# MAGIC * Writing unit tests for Python functions
# MAGIC * Testing Spark DataFrames and transformations
# MAGIC * Using pytest fixtures for SparkSession
# MAGIC * Parameterized tests for multiple scenarios
# MAGIC * Best practices for testing in Databricks
# MAGIC
# MAGIC ## Prerequisites:
# MAGIC * Pytest library installed on the cluster
# MAGIC * Basic understanding of Python and PySpark

# COMMAND ----------

# DBTITLE 1,Install pytest
# MAGIC %pip install pytest ipytest
# MAGIC
# MAGIC # ipytest allows us to run pytest directly in notebooks
# MAGIC import ipytest
# MAGIC ipytest.autoconfig()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sample Functions
# MAGIC
# MAGIC Let's create some sample functions that we'll write tests for. These include:
# MAGIC * Data transformation functions
# MAGIC * Data validation functions
# MAGIC * Business logic functions

# COMMAND ----------

# DBTITLE 1,Define Helper Functions
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, sum as _sum, count, avg
from typing import List

def calculate_total_amount(df: DataFrame, amount_col: str) -> float:
    """Calculate the total sum of a specified amount column."""
    result = df.select(_sum(col(amount_col)).alias("total")).collect()
    return float(result[0]["total"]) if result else 0.0

def filter_by_threshold(df: DataFrame, column_name: str, threshold: float) -> DataFrame:
    """Filter DataFrame rows where column value exceeds threshold."""
    return df.filter(col(column_name) > threshold)

def add_derived_column(df: DataFrame, col1: str, col2: str, new_col: str) -> DataFrame:
    """Add a new column that is the sum of two existing columns."""
    return df.withColumn(new_col, col(col1) + col(col2))

def validate_columns_exist(df: DataFrame, required_columns: List[str]) -> bool:
    """Check if all required columns exist in the DataFrame."""
    df_columns = set(df.columns)
    return all(col in df_columns for col in required_columns)

print("✓ Helper functions defined successfully")

# COMMAND ----------

# DBTITLE 1,Create Sample Test Data
# Create a sample DataFrame for testing
test_data = [
    (1, "Alice", 100.0, 25.0),
    (2, "Bob", 200.0, 50.0),
    (3, "Charlie", 150.0, 30.0),
    (4, "Diana", 300.0, 75.0),
    (5, "Eve", 50.0, 10.0)
]

test_df = spark.createDataFrame(test_data, ["id", "name", "sales", "commission"])

print("Sample test data:")
display(test_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Basic Unit Tests
# MAGIC
# MAGIC Let's write some basic unit tests for our helper functions.

# COMMAND ----------

# DBTITLE 1,Test: Calculate Total Amount
# MAGIC %%ipytest
# MAGIC
# MAGIC import pytest
# MAGIC from pyspark.sql import SparkSession
# MAGIC
# MAGIC @pytest.fixture
# MAGIC def spark_session():
# MAGIC     """Fixture to provide SparkSession to tests."""
# MAGIC     return SparkSession.builder.getOrCreate()
# MAGIC
# MAGIC @pytest.fixture
# MAGIC def sample_df(spark_session):
# MAGIC     """Fixture to provide sample DataFrame."""
# MAGIC     data = [
# MAGIC         (1, "Alice", 100.0, 25.0),
# MAGIC         (2, "Bob", 200.0, 50.0),
# MAGIC         (3, "Charlie", 150.0, 30.0)
# MAGIC     ]
# MAGIC     return spark_session.createDataFrame(data, ["id", "name", "sales", "commission"])
# MAGIC
# MAGIC def test_calculate_total_amount(sample_df):
# MAGIC     """Test that total amount calculation is correct."""
# MAGIC     total = calculate_total_amount(sample_df, "sales")
# MAGIC     assert total == 450.0, f"Expected 450.0, got {total}"
# MAGIC
# MAGIC def test_calculate_total_commission(sample_df):
# MAGIC     """Test total commission calculation."""
# MAGIC     total = calculate_total_amount(sample_df, "commission")
# MAGIC     assert total == 105.0, f"Expected 105.0, got {total}"

# COMMAND ----------

# DBTITLE 1,Test: Filter Functions
# MAGIC %%ipytest
# MAGIC
# MAGIC import pytest
# MAGIC
# MAGIC @pytest.fixture
# MAGIC def sample_df():
# MAGIC     """Fixture to provide sample DataFrame."""
# MAGIC     data = [
# MAGIC         (1, "Alice", 100.0),
# MAGIC         (2, "Bob", 200.0),
# MAGIC         (3, "Charlie", 150.0),
# MAGIC         (4, "Diana", 300.0),
# MAGIC         (5, "Eve", 50.0)
# MAGIC     ]
# MAGIC     return spark.createDataFrame(data, ["id", "name", "sales"])
# MAGIC
# MAGIC def test_filter_by_threshold(sample_df):
# MAGIC     """Test filtering DataFrame by threshold."""
# MAGIC     filtered_df = filter_by_threshold(sample_df, "sales", 150.0)
# MAGIC     count = filtered_df.count()
# MAGIC     assert count == 2, f"Expected 2 rows with sales > 150, got {count}"
# MAGIC     
# MAGIC     # Verify the correct rows are included
# MAGIC     names = [row.name for row in filtered_df.collect()]
# MAGIC     assert "Bob" in names
# MAGIC     assert "Diana" in names
# MAGIC
# MAGIC def test_filter_empty_result(sample_df):
# MAGIC     """Test filtering that returns no results."""
# MAGIC     filtered_df = filter_by_threshold(sample_df, "sales", 1000.0)
# MAGIC     assert filtered_df.count() == 0, "Expected empty result for threshold > max value"

# COMMAND ----------

# DBTITLE 1,Test: Column Operations
# MAGIC %%ipytest
# MAGIC
# MAGIC import pytest
# MAGIC
# MAGIC @pytest.fixture
# MAGIC def sample_df():
# MAGIC     """Fixture to provide sample DataFrame."""
# MAGIC     data = [(1, 100.0, 25.0), (2, 200.0, 50.0)]
# MAGIC     return spark.createDataFrame(data, ["id", "sales", "commission"])
# MAGIC
# MAGIC def test_add_derived_column(sample_df):
# MAGIC     """Test adding a derived column."""
# MAGIC     result_df = add_derived_column(sample_df, "sales", "commission", "total_earnings")
# MAGIC     
# MAGIC     # Check column exists
# MAGIC     assert "total_earnings" in result_df.columns
# MAGIC     
# MAGIC     # Check values are correct
# MAGIC     row = result_df.filter(col("id") == 1).collect()[0]
# MAGIC     assert row.total_earnings == 125.0, f"Expected 125.0, got {row.total_earnings}"
# MAGIC
# MAGIC def test_validate_columns_exist(sample_df):
# MAGIC     """Test column validation function."""
# MAGIC     # Test with existing columns
# MAGIC     assert validate_columns_exist(sample_df, ["id", "sales"])
# MAGIC     
# MAGIC     # Test with non-existing columns
# MAGIC     assert not validate_columns_exist(sample_df, ["id", "nonexistent_column"])
# MAGIC     
# MAGIC     # Test with empty list
# MAGIC     assert validate_columns_exist(sample_df, [])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameterized Tests
# MAGIC
# MAGIC Parameterized tests allow you to run the same test with different input values.

# COMMAND ----------

# DBTITLE 1,Test: Parameterized Examples
# MAGIC %%ipytest
# MAGIC
# MAGIC import pytest
# MAGIC
# MAGIC @pytest.fixture
# MAGIC def sample_df():
# MAGIC     data = [(1, 100.0), (2, 200.0), (3, 150.0), (4, 300.0)]
# MAGIC     return spark.createDataFrame(data, ["id", "amount"])
# MAGIC
# MAGIC @pytest.mark.parametrize("threshold,expected_count", [
# MAGIC     (50.0, 4),   # All rows above 50
# MAGIC     (150.0, 2),  # 2 rows above 150
# MAGIC     (250.0, 1),  # 1 row above 250
# MAGIC     (400.0, 0),  # No rows above 400
# MAGIC ])
# MAGIC def test_filter_with_different_thresholds(sample_df, threshold, expected_count):
# MAGIC     """Test filtering with various threshold values."""
# MAGIC     filtered_df = filter_by_threshold(sample_df, "amount", threshold)
# MAGIC     actual_count = filtered_df.count()
# MAGIC     assert actual_count == expected_count, \
# MAGIC         f"Threshold {threshold}: expected {expected_count}, got {actual_count}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Testing with Real Databricks Tables
# MAGIC
# MAGIC You can also write tests that work with actual tables in your workspace.

# COMMAND ----------

# DBTITLE 1,Test: Sample Databricks Table
# MAGIC %%ipytest
# MAGIC
# MAGIC import pytest
# MAGIC from pyspark.sql import SparkSession
# MAGIC
# MAGIC @pytest.fixture
# MAGIC def spark_session():
# MAGIC     """Fixture to provide SparkSession."""
# MAGIC     return SparkSession.builder.getOrCreate()
# MAGIC
# MAGIC def test_samples_catalog_accessible(spark_session):
# MAGIC     """Test that we can access the samples catalog."""
# MAGIC     # Try to access a sample table
# MAGIC     try:
# MAGIC         df = spark_session.sql("SELECT * FROM samples.nyctaxi.trips LIMIT 10")
# MAGIC         assert df.count() > 0, "Sample table should have data"
# MAGIC         print(f"✓ Successfully accessed samples.nyctaxi.trips, found {df.count()} rows")
# MAGIC     except Exception as e:
# MAGIC         pytest.skip(f"Sample table not available: {e}")
# MAGIC
# MAGIC def test_table_schema_validation(spark_session):
# MAGIC     """Test that a table has expected columns."""
# MAGIC     try:
# MAGIC         df = spark_session.sql("SELECT * FROM samples.nyctaxi.trips LIMIT 1")
# MAGIC         
# MAGIC         # Check for expected columns
# MAGIC         expected_columns = ["pickup_zip", "dropoff_zip", "fare_amount"]
# MAGIC         actual_columns = df.columns
# MAGIC         
# MAGIC         for col in expected_columns:
# MAGIC             assert col in actual_columns, f"Expected column '{col}' not found"
# MAGIC         
# MAGIC         print(f"✓ Schema validation passed")
# MAGIC     except Exception as e:
# MAGIC         pytest.skip(f"Sample table not available: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Best Practices for Unit Testing in Databricks
# MAGIC
# MAGIC ### 1. **Use Fixtures**
# MAGIC * Create reusable fixtures for SparkSession and test data
# MAGIC * Keep fixtures focused and single-purpose
# MAGIC
# MAGIC ### 2. **Test Isolation**
# MAGIC * Each test should be independent
# MAGIC * Clean up resources after tests
# MAGIC * Don't rely on test execution order
# MAGIC
# MAGIC ### 3. **Test Data**
# MAGIC * Use small, representative datasets
# MAGIC * Create test data programmatically
# MAGIC * Consider edge cases (empty DataFrames, null values, etc.)
# MAGIC
# MAGIC ### 4. **Assertions**
# MAGIC * Use descriptive assertion messages
# MAGIC * Test both positive and negative scenarios
# MAGIC * Verify row counts, schema, and data values
# MAGIC
# MAGIC ### 5. **Performance**
# MAGIC * Keep unit tests fast (< 5 seconds each)
# MAGIC * Use `.limit()` when testing with large tables
# MAGIC * Mock external dependencies when possible
# MAGIC
# MAGIC ### 6. **Organization**
# MAGIC * Group related tests in test classes
# MAGIC * Use clear, descriptive test names
# MAGIC * Follow naming convention: `test_<function_name>_<scenario>`

# COMMAND ----------

# DBTITLE 1,Advanced Example: Testing Data Quality
from pyspark.sql.functions import col, isnan, when, count as spark_count

def check_data_quality(df: DataFrame, column_name: str) -> dict:
    """Check data quality metrics for a column."""
    total_count = df.count()
    null_count = df.filter(col(column_name).isNull()).count()
    
    return {
        "total_rows": total_count,
        "null_count": null_count,
        "null_percentage": (null_count / total_count * 100) if total_count > 0 else 0,
        "non_null_count": total_count - null_count
    }

print("✓ Data quality function defined")

# COMMAND ----------

# DBTITLE 1,Test: Data Quality Checks
# MAGIC %%ipytest
# MAGIC
# MAGIC import pytest
# MAGIC from pyspark.sql.functions import col, lit
# MAGIC
# MAGIC @pytest.fixture
# MAGIC def df_with_nulls():
# MAGIC     """Create DataFrame with null values for testing."""
# MAGIC     data = [
# MAGIC         (1, "Alice", 100.0),
# MAGIC         (2, "Bob", None),
# MAGIC         (3, None, 150.0),
# MAGIC         (4, "Diana", None),
# MAGIC         (5, "Eve", 200.0)
# MAGIC     ]
# MAGIC     return spark.createDataFrame(data, ["id", "name", "amount"])
# MAGIC
# MAGIC def test_data_quality_with_nulls(df_with_nulls):
# MAGIC     """Test data quality check with null values."""
# MAGIC     quality_metrics = check_data_quality(df_with_nulls, "amount")
# MAGIC     
# MAGIC     assert quality_metrics["total_rows"] == 5
# MAGIC     assert quality_metrics["null_count"] == 2
# MAGIC     assert quality_metrics["non_null_count"] == 3
# MAGIC     assert quality_metrics["null_percentage"] == 40.0
# MAGIC
# MAGIC def test_data_quality_no_nulls():
# MAGIC     """Test data quality check with no null values."""
# MAGIC     data = [(1, 100.0), (2, 200.0), (3, 300.0)]
# MAGIC     df = spark.createDataFrame(data, ["id", "amount"])
# MAGIC     
# MAGIC     quality_metrics = check_data_quality(df, "amount")
# MAGIC     
# MAGIC     assert quality_metrics["null_count"] == 0
# MAGIC     assert quality_metrics["null_percentage"] == 0.0
# MAGIC     assert quality_metrics["non_null_count"] == 3

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC This demo covered:
# MAGIC
# MAGIC ✅ **Installing pytest** in Databricks notebooks using ipytest
# MAGIC
# MAGIC ✅ **Writing unit tests** for Python and PySpark functions
# MAGIC
# MAGIC ✅ **Using fixtures** to provide reusable test components
# MAGIC
# MAGIC ✅ **Testing DataFrames** with assertions on counts, schemas, and values
# MAGIC
# MAGIC ✅ **Parameterized tests** for testing multiple scenarios
# MAGIC
# MAGIC ✅ **Testing real tables** from the Databricks catalog
# MAGIC
# MAGIC ✅ **Data quality checks** and validation functions
# MAGIC
# MAGIC ✅ **Best practices** for maintainable test suites
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Next Steps:
# MAGIC
# MAGIC * **For local development**: Use Databricks Connect with pytest from your IDE
# MAGIC * **For CI/CD**: Integrate tests into your deployment pipeline
# MAGIC * **For notebooks**: Use the VS Code extension to run tests on remote clusters
# MAGIC * **For production**: Implement automated testing as part of your workflow jobs
# MAGIC
# MAGIC Refer to the [official documentation](https://docs.databricks.com/aws/en/files/python-unit-tests) for more details!

# COMMAND ----------

