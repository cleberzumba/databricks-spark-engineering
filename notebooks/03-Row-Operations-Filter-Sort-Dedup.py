# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 2: Row Operations - Filtering, Sorting, Dropping, Deduplication
# MAGIC
# MAGIC ## Key Functions:
# MAGIC - `filter()` / `where()` - Filter rows based on conditions
# MAGIC - `sort()` / `orderBy()` - Sort rows 
# MAGIC - `distinct()` - Remove duplicate rows
# MAGIC - `dropDuplicates()` / `drop_duplicates()` - Remove duplicates based on columns
# MAGIC - `limit()` - Limit number of rows
# MAGIC - `dropna()` / `fillna()` - Handle missing data

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup: Create Sample DataFrames

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, isnan, isnull, desc, asc
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# Sample employee data with duplicates and nulls
employee_data = [
    ("John", "Doe", "Engineering", 75000, "USA"),
    ("Jane", "Smith", "Marketing", 65000, "UK"),
    ("Bob", "Johnson", "Engineering", 80000, "USA"),
    ("Alice", "Williams", "Sales", 70000, None),  # NULL country
    ("Charlie", "Brown", "Marketing", 60000, "USA"),
    ("John", "Doe", "Engineering", 75000, "USA"),  # Duplicate
    ("Eve", "Davis", "Sales", None, "Canada"),  # NULL salary
    ("Jane", "Smith", "Marketing", 65000, "UK"),  # Duplicate
]

schema = StructType([
    StructField("firstName", StringType(), True),
    StructField("lastName", StringType(), True),
    StructField("department", StringType(), True),
    StructField("salary", IntegerType(), True),
    StructField("country", StringType(), True)
])

df = spark.createDataFrame(employee_data, schema)
display(df)
print(f"Total rows: {df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. filter() and where() - Filter Rows
# MAGIC
# MAGIC - `filter()` and `where()` are **IDENTICAL** (aliases)
# MAGIC - Multiple syntax options for conditions
# MAGIC - Can chain multiple filters or use AND/OR logic

# COMMAND ----------

# METHOD 1: Using string expressions (SQL-like)
df.filter("salary > 65000").show()
df.where("salary > 65000").show()  # IDENTICAL to filter()

# METHOD 2: Using Column expressions
df.filter(col("salary") > 65000).show()

# METHOD 3: Using DataFrame column reference
df.filter(df.salary > 65000).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Multiple Conditions in Filters
# MAGIC
# MAGIC - Use `&` for AND (not `and`)
# MAGIC - Use `|` for OR (not `or`)
# MAGIC - Must use parentheses around each condition!

# COMMAND ----------

# AND condition - Both conditions must be true
df.filter((col("salary") > 65000) & (col("department") == "Engineering")).show()

# OR condition - Either condition can be true
df.filter((col("salary") > 75000) | (col("department") == "Sales")).show()

# NOT condition - Negate a condition
df.filter(~(col("department") == "Marketing")).show()

# Complex combinations
df.filter(
    ((col("salary") > 65000) & (col("department") == "Engineering")) |
    (col("country") == "UK")
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Chaining Multiple Filters
# MAGIC
# MAGIC ### Best Practice:
# MAGIC - Can chain filters for readability
# MAGIC - Equivalent to using AND (&)

# COMMAND ----------

# Chained filters (easier to read)
result = (df
    .filter(col("salary") > 60000)
    .filter(col("department") == "Engineering")
)
result.show()

# Equivalent single filter with AND
result2 = df.filter((col("salary") > 60000) & (col("department") == "Engineering"))
result2.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. sort() vs orderBy()
# MAGIC
# MAGIC ### CRITICAL DIFFERENCES:
# MAGIC
# MAGIC | Feature              | sort()                          | orderBy()                  |
# MAGIC |----------------------|---------------------------------|----------------------------|
# MAGIC | **Syntax**           | Can use column names as strings | Prefers Column objects     |
# MAGIC | **Multiple columns** | `sort("col1", "col2")`          | `orderBy("col1", "col2")`  |
# MAGIC | **Descending**       | `sort(col("x").desc())`         | `orderBy(col("x").desc())` |
# MAGIC | **Ascending param**  | `sort("col", ascending=False)`  | N/A - use desc()           |
# MAGIC | **Recommendation**   | More Pythonic                   | More Java/Scala-like       |
# MAGIC
# MAGIC - Both do the SAME thing (sorting)

# COMMAND ----------

# ===== ASCENDING ORDER (Default) =====

# Using sort() - string column names
df.sort("salary").show()

# Using orderBy() - string column names
df.orderBy("salary").show()

# Using sort() - Column objects
df.sort(col("salary")).show()

# Using orderBy() - Column objects
df.orderBy(col("salary")).show()

# Explicit ascending with asc()
df.sort(col("salary").asc()).show()
df.orderBy(col("salary").asc()).show()

# COMMAND ----------

# ===== DESCENDING ORDER =====

# METHOD 1: Using desc() function (works with both)
df.sort(col("salary").desc()).show()
df.orderBy(col("salary").desc()).show()

# METHOD 2: Using ascending parameter (sort() and orderBy()!)
df.sort("salary", ascending=False).show()
df.orderBy("salary", ascending=False).show() 

# METHOD 3: Using desc() imported function
from pyspark.sql.functions import desc, asc
df.sort(desc("salary")).show()
df.orderBy(desc("salary")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Sorting by Multiple Columns
# MAGIC
# MAGIC - First sort by one column, then by another
# MAGIC - Can mix ascending and descending

# COMMAND ----------

# Sort by department (asc), then salary (desc)
df.sort(col("department").asc(), col("salary").desc()).show()

# Same with orderBy
df.orderBy(col("department").asc(), col("salary").desc()).show()

# Using sort with ascending parameter for multiple columns
df.sort("department", "salary", ascending=[True, False]).show()

# String syntax (simplest for same direction)
df.orderBy("department", "salary").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. distinct() - Remove All Duplicate Rows
# MAGIC
# MAGIC - Removes rows where ALL columns are identical

# COMMAND ----------

print(f"Before distinct: {df.count()} rows")
df.show()

# Remove complete duplicates
df_unique = df.distinct()
print(f"After distinct: {df_unique.count()} rows")
df_unique.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. dropDuplicates() - Remove Duplicates Based on Specific Columns
# MAGIC
# MAGIC - Can specify which columns to consider for duplicates
# MAGIC - More flexible than distinct()
# MAGIC - Also called `drop_duplicates()` (both work!)

# COMMAND ----------

# Drop duplicates based on firstName and lastName only
df.dropDuplicates(["firstName", "lastName"]).show()

# Alternative syntax (same result)
df.drop_duplicates(["firstName", "lastName"]).show()

# Drop duplicates based on single column
df.dropDuplicates(["department"]).show()

# Drop all duplicates (same as distinct())
df.dropDuplicates().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Handling NULL Values - dropna() and fillna()
# MAGIC
# MAGIC | Method                   | Parameters | Behavior                              |
# MAGIC |--------------------------|------------|---------------------------------------|
# MAGIC | `dropna()`               | No params  | Drops rows with ANY null              |
# MAGIC | `dropna("all")`          | how="all"  | Drops rows where ALL values are null  |
# MAGIC | `dropna("any")`          | how="any"  | Drops rows with ANY null (default)    |
# MAGIC | `dropna(subset=["col"])` | subset     | Only check specific columns for nulls |

# COMMAND ----------

# Show data with nulls
print("Original data:")
df.show()

# Drop rows with ANY null value in ANY column
df.dropna().show()  # or df.na.drop()

# Drop rows with ALL null values
df.dropna(how="all").show()  # or df.na.drop("all")

# Drop rows with null in specific columns
df.dropna(subset=["salary"]).show()

df.na.drop("all")
df.na.drop(how="all")
df.dropna(how="all")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Example
# MAGIC
# MAGIC "Which code fragment will return a new DataFrame from storesDF that excludes
# MAGIC all rows containing at least one missing value in any column?"
# MAGIC
# MAGIC **Answer: storesDF.na.drop()** or **storesDF.dropna()**

# COMMAND ----------

# Create test DataFrame with nulls
test_data = [
    ("John", 75000, "USA"),
    ("Jane", None, "UK"),     # NULL salary
    ("Bob", 80000, None),     # NULL country
    (None, 70000, "Canada"),  # NULL name
]

test_df = spark.createDataFrame(test_data, ["name", "salary", "country"])
print("Original:")
test_df.show()

# all equivalent:
print("Excluding rows with ANY null:")
test_df.na.drop().show()
test_df.dropna().show()
test_df.dropna(how="any").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. fillna() - Fill NULL Values
# MAGIC
# MAGIC - Replace nulls with default values
# MAGIC - Different defaults for different columns

# COMMAND ----------

# Fill all numeric nulls with 0
df.fillna(0).show()

# Fill string nulls with default value
df.fillna("Unknown").show()

# Fill specific columns with different values
df.fillna({"salary": 50000, "country": "Unknown"}).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. limit() - Limit Number of Rows
# MAGIC
# MAGIC - Preview data
# MAGIC - Sampling
# MAGIC - Testing with small datasets

# COMMAND ----------

# Get first 3 rows
df.limit(3).show()

# Combine with orderBy for "top N"
df.orderBy(col("salary").desc()).limit(3).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Example - Combined Operations

# COMMAND ----------

# Complex real-world pattern
result = (df
    .filter(col("salary").isNotNull())  # Remove rows with null salary
    .filter(col("salary") > 60000)      # High earners only
    .dropDuplicates(["firstName", "lastName"])  # Remove duplicate people
    .orderBy(col("salary").desc())       # Highest salary first
    .limit(5)                           # Top 5
)

print("Top 5 high earners (no duplicates, no nulls):")
result.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 13. Checking for NULL Values
# MAGIC
# MAGIC ### Important Functions:
# MAGIC - `isNull()` / `isNotNull()` - Check for NULL
# MAGIC - `isnan()` - Check for NaN (Not a Number)

# COMMAND ----------

# Filter rows where salary is NULL
df.filter(col("salary").isNull()).show()

# Filter rows where salary is NOT NULL
df.filter(col("salary").isNotNull()).show()

# Filter rows where country is NULL
df.filter(col("country").isNull()).show()

# Multiple null checks
df.filter(col("salary").isNull() | col("country").isNull()).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC 1. **filter() == where()** - Completely identical!
# MAGIC 2. **sort() vs orderBy()** - Same result, different syntax options
# MAGIC 3. **Descending order**:
# MAGIC    - `sort(col("x").desc())`
# MAGIC    - `orderBy(col("x").desc())`
# MAGIC    - `sort("x", ascending=False)`
# MAGIC 4. **distinct()** - All columns must match
# MAGIC 5. **dropDuplicates(["col1", "col2"])** - Specific columns
# MAGIC 6. **dropna()** vs **dropna("all")**:
# MAGIC    - `dropna()` = remove rows with ANY null
# MAGIC    - `dropna("all")` = remove rows where ALL are null
# MAGIC
