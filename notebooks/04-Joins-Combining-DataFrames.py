# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 4: Joins and Combining DataFrames
# MAGIC
# MAGIC - `join()` - Join DataFrames
# MAGIC - Join types: `inner`, `left`, `right`, `outer`, `cross`, `left_anti`, `left_semi`
# MAGIC - `broadcast()` 
# MAGIC - `union()` / `unionAll()` (deprecated)/ `unionByName()` - Combine rows
# MAGIC - Multiple join keys
# MAGIC - Handling duplicate column names after joins

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup: Create Sample DataFrames

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, broadcast, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Employees DataFrame
employees_data = [
    (1, "John", "Engineering", 1),
    (2, "Jane", "Marketing", 2),
    (3, "Bob", "Engineering", 1),
    (4, "Alice", "Sales", 3),
    (5, "Charlie", "HR", None),  # NULL department_id (no matching department)
]
employees_schema = ["emp_id", "name", "role", "dept_id"]
employees = spark.createDataFrame(employees_data, employees_schema)

# Departments DataFrame
departments_data = [
    (1, "Engineering", "Building A"),
    (2, "Marketing", "Building B"),
    (3, "Sales", "Building C"),
    (4, "Finance", "Building D"),  # No employees in this department
]
departments_schema = ["dept_id", "dept_name", "location"]
departments = spark.createDataFrame(departments_data, departments_schema)

print("Employees:")
employees.show()
print("Departments:")
departments.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Inner Join - Most Common Join Type
# MAGIC
# MAGIC ### Behavior:
# MAGIC - Returns rows where join key exists in BOTH DataFrames
# MAGIC - **Default join type**
# MAGIC - Most frequently used

# COMMAND ----------

# METHOD 1: Basic inner join (default)
employees.join(departments, employees.dept_id == departments.dept_id).show()

# METHOD 2: Explicit inner join
employees.join(departments, employees.dept_id == departments.dept_id, "inner").show()

# METHOD 3: Using string column name (when column names match)
employees.join(departments, "dept_id", "inner").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Left Join (Left Outer Join)
# MAGIC
# MAGIC ### Behavior:
# MAGIC - Returns ALL rows from LEFT DataFrame
# MAGIC - Matching rows from RIGHT DataFrame
# MAGIC - NULL for right side when no match

# COMMAND ----------

# Left join - keeps all employees, even those without matching department
result = employees.join(departments, employees.dept_id == departments.dept_id, "left")
result.show()

# Notice Charlie (emp_id=5) appears with NULL dept_name and location

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Right Join (Right Outer Join)
# MAGIC
# MAGIC ### Behavior:
# MAGIC - Returns ALL rows from RIGHT DataFrame
# MAGIC - Matching rows from LEFT DataFrame
# MAGIC - NULL for left side when no match

# COMMAND ----------

# Right join - keeps all departments, even those without employees
result = employees.join(departments, employees.dept_id == departments.dept_id, "right")
result.show()

# Notice Finance department (dept_id=4) appears with NULL employee info

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Outer Join (Full Outer Join)
# MAGIC
# MAGIC ### Behavior:
# MAGIC - Returns ALL rows from BOTH DataFrames
# MAGIC - NULL when no match on either side

# COMMAND ----------

# Outer join - keeps everything from both sides
result = employees.join(departments, employees.dept_id == departments.dept_id, "outer")
result.show()

# Notice both Charlie and Finance appear with NULLs on their non-matching sides

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Left Semi Join
# MAGIC
# MAGIC ### Behavior:
# MAGIC - Returns rows from LEFT where match exists in RIGHT
# MAGIC - **Only returns columns from LEFT DataFrame**
# MAGIC - Like inner join but only left columns

# COMMAND ----------

# Left semi join - employees who have a department
result = employees.join(departments, employees.dept_id == departments.dept_id, "left_semi")
result.show()

# Only employee columns, but Charlie is excluded (no matching department)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Left Anti Join
# MAGIC
# MAGIC ### Behavior:
# MAGIC - Returns rows from LEFT where NO match exists in RIGHT
# MAGIC - Opposite of left semi join
# MAGIC - Useful for finding non-matching records

# COMMAND ----------

# Left anti join - employees WITHOUT a department
result = employees.join(departments, employees.dept_id == departments.dept_id, "left_anti")
result.show()

# Only Charlie appears (has NULL dept_id)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Cross Join (Cartesian Product)
# MAGIC
# MAGIC ### Behavior:
# MAGIC - Returns EVERY combination of rows from both DataFrames
# MAGIC - If left has M rows and right has N rows, result has M × N rows
# MAGIC - **WARNING**: Can create huge datasets!

# COMMAND ----------

# Cross join - every employee paired with every department
result = employees.crossJoin(departments)
print(f"Employees: {employees.count()} rows")
print(f"Departments: {departments.count()} rows")
print(f"Cross join result: {result.count()} rows (5 × 4 = 20)")
result.show()

# Alternative syntax
result2 = employees.join(departments, how="cross")
result2.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Join on Multiple Keys
# MAGIC
# MAGIC ### When to Use:
# MAGIC - Composite keys
# MAGIC - More specific join conditions

# COMMAND ----------

# Create sample data with composite keys
sales_data = [
    ("2024-01-01", "Store_A", "Electronics", 1200),
    ("2024-01-01", "Store_B", "Clothing", 500),
    ("2024-01-02", "Store_A", "Electronics", 800),
]
sales = spark.createDataFrame(sales_data, ["date", "store", "category", "revenue"])

targets_data = [
    ("2024-01-01", "Store_A", 1000),
    ("2024-01-01", "Store_B", 600),
    ("2024-01-02", "Store_A", 900),
]
targets = spark.createDataFrame(targets_data, ["date", "store", "target"])

# Join on MULTIPLE keys (date AND store)
result = sales.join(targets, (sales.date == targets.date) & (sales.store == targets.store), "inner")
result.show()

# Alternative syntax when column names match
result2 = sales.join(targets, ["date", "store"], "inner")
result2.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Broadcast Join 
# MAGIC
# MAGIC **What is a Broadcast Join?**
# MAGIC - Sends **small** DataFrame to ALL executors
# MAGIC - Avoids expensive shuffle operation
# MAGIC - Significantly faster for small-to-large joins
# MAGIC
# MAGIC **When to Use:**
# MAGIC - One DataFrame is small (< 10MB default)
# MAGIC - One DataFrame is significantly smaller than the other
# MAGIC - Want to avoid shuffle
# MAGIC
# MAGIC **When NOT to Use:**
# MAGIC - Both DataFrames are large
# MAGIC - Small DataFrame is still too large for memory
# MAGIC
# MAGIC **How Spark Decides:**
# MAGIC - Spark auto-broadcasts if size < `spark.sql.autoBroadcastJoinThreshold` (default 10MB)
# MAGIC - You can force with `broadcast()` hint

# COMMAND ----------

# Regular join (may shuffle both sides)
regular_join = employees.join(departments, "dept_id").show()

# Broadcast join (broadcasts send small departments to all executors, avoids expensive shuffle operation)
broadcast_join = employees.join(broadcast(departments), "dept_id")

# Both produce same result, but broadcast join is faster for small departments
broadcast_join.show()

# You can also broadcast the left side if it's smaller
result = broadcast(employees).join(departments, "dept_id")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Handling Duplicate Column Names After Join
# MAGIC
# MAGIC ### Common Problem:
# MAGIC - After join, both DataFrames may have same column names
# MAGIC - Causes ambiguity

# COMMAND ----------

# Problem: duplicate dept_id columns
result = employees.join(departments, employees.dept_id == departments.dept_id)
result.printSchema()

# SOLUTION 1: Select specific columns
result.select(
    employees.emp_id,
    employees.name,
    employees.dept_id,  # From employees
    departments.dept_name,
    departments.location
).show()

# SOLUTION 2: Alias DataFrames
emp_alias = employees.alias("emp")
dept_alias = departments.alias("dept")

result = emp_alias.join(
    dept_alias,
    col("emp.dept_id") == col("dept.dept_id")
).select(
    "emp.emp_id",
    "emp.name",
    "emp.dept_id",
    "dept.dept_name",
    "dept.location"
)
result.show()

# SOLUTION 3: Drop duplicate column
result = employees.join(departments, employees.dept_id == departments.dept_id).drop(departments.dept_id)
result.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. union() and unionAll()
# MAGIC
# MAGIC ### Behavior:
# MAGIC - Combine rows from two DataFrames
# MAGIC - Columns must match by position (not name)
# MAGIC - `union()` and `unionAll()` are IDENTICAL in Spark SQL (both keep duplicates)
# MAGIC - **Different from SQL UNION** (which removes duplicates)

# COMMAND ----------

# Create two employee DataFrames
employees1 = spark.createDataFrame([
    (1, "John", "Engineering"),
    (2, "Jane", "Marketing"),
], ["id", "name", "dept"])

employees2 = spark.createDataFrame([
    (3, "Bob", "Sales"),
    (4, "Alice", "HR"),
    (1, "John", "Engineering"),  # Duplicate
], ["id", "name", "dept"])

# union - keeps duplicates
result = employees1.union(employees2)
print(f"union result: {result.count()} rows")
result.show()

# unionAll - same as union (keeps duplicates) - (deprecated)
result2 = employees1.unionAll(employees2)
print(f"unionAll result: {result2.count()} rows")
result2.show()

# Remove duplicates after union
result_distinct = employees1.union(employees2).distinct()
print(f"After distinct: {result_distinct.count()} rows")
result_distinct.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. unionByName()
# MAGIC
# MAGIC ### Difference from union():
# MAGIC - Matches columns by **NAME** (not position)
# MAGIC - More flexible for DataFrames with different column orders

# COMMAND ----------

# Different column order
df1 = spark.createDataFrame([(1, "John", "Engineering")], ["id", "name", "dept"])
df2 = spark.createDataFrame([("Sales", "Bob", 2)], ["dept", "name", "id"])  # Different order

# union() fails or produces wrong results (matches by position)
# df1.union(df2).show()  # Would create wrong alignment

# unionByName() matches by column names
result = df1.unionByName(df2)
result.show()  # Correct

# COMMAND ----------

# MAGIC %md
# MAGIC ## 13. Broadcast Variables (General Concept)
# MAGIC
# MAGIC - Read-only variables cached on each executor
# MAGIC - Avoid sending large variables with every task
# MAGIC - Different from broadcast join (but related concept)
# MAGIC
# MAGIC WARNING: It doesn't work with Databricks Free, since the cluster can't be Serverless to use this feature
# MAGIC
# MAGIC

# COMMAND ----------

# Create broadcast variable
config_dict = {"threshold": 1000, "multiplier": 1.5}
broadcast_config = spark.sparkContext.broadcast(config_dict)

# Use in UDF
def apply_discount(price):
    config = broadcast_config.value
    if price > config["threshold"]:
        return price * config["multiplier"]
    return price

from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType

discount_udf = udf(apply_discount, DoubleType())

# Apply
sample_df = spark.createDataFrame([(1, 500), (2, 1500)], ["id", "price"])
sample_df.withColumn("final_price", discount_udf(col("price"))).show()

# COMMAND ----------

# Create realistic scenario
orders = spark.createDataFrame([
    (1, 101, "2024-01-01"),
    (2, 102, "2024-01-02"),
    (3, 101, "2024-01-03"),
    (4, 103, "2024-01-04"),
], ["order_id", "customer_id", "order_date"])

customers = spark.createDataFrame([
    (101, "John", "USA"),
    (102, "Jane", "UK"),
    (103, "Bob", "Canada"),
    (104, "Alice", "USA"),  # No orders
], ["customer_id", "name", "country"])

# Get all orders with customer details, using broadcast for small customers table
result = (orders
    .join(broadcast(customers), "customer_id", "inner")
    .select(
        "order_id",
        "customer_id",
        "name",
        "country",
        "order_date"
    )
    .orderBy("order_id")
)
result.show()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Join Types:
# MAGIC
# MAGIC | Join Type     | Returns                                     | Use Case                 |
# MAGIC |---------------|---------------------------------------------|--------------------------|
# MAGIC | **inner**     | Matches from both                           | Default, most common     |
# MAGIC | **left**      | All left + matches from right               | Keep all from main table |
# MAGIC | **right**     | All right + matches from left               | Reverse of left          |
# MAGIC | **outer**     | All from both                               | Everything               |
# MAGIC | **left_semi** | Left where match exists (left columns only) | Filtering                |
# MAGIC | **left_anti** | Left where NO match exists                  | Find non-matches         |
# MAGIC | **cross**     | Cartesian product (all combinations)        | Rare, dangerous!         |
# MAGIC
# MAGIC **Why use broadcast join:**
# MAGIC - Avoids expensive shuffle operations
# MAGIC - Faster for small-to-large joins
# MAGIC - Small table replicated to all executors
# MAGIC
# MAGIC **When to use:**
# MAGIC - One table is small (< 10MB typically)
# MAGIC - Significant size difference between tables
# MAGIC
# MAGIC **Use broadcast variables only when:**
# MAGIC - You need custom logic (UDF, map, etc.)
# MAGIC - The data is small (KB / a few MB)
# MAGIC - It can't be resolved with a join
# MAGIC
# MAGIC
# MAGIC  **Broadcast variables are rarely used in modern pipelines because we avoid UDFs. I prefer broadcast joins or native Spark functions to leverage Catalyst.**
