# `option("mergeSchema", "true")` in PySpark

## Overview

The `mergeSchema` option is used when reading Parquet files in a directory where the schemas have evolved over time. It tells Spark to **merge the schemas** of all Parquet files into a unified schema, with missing columns filled as `null`.

---

## Typical Scenario: Schema Evolution

You have 3 Parquet files in a directory, created at different times:

```
/data/sales/file1.parquet  (created in 2023)
columns: [id, amount]

/data/sales/file2.parquet  (created in 2024)
columns: [id, amount, customer]      ← new column

/data/sales/file3.parquet  (created in 2025)
columns: [id, amount, customer, region]  ← another new column
```

---

## Without `mergeSchema` (Default Behavior)

```python
df = spark.read.parquet("/data/sales/")

df.printSchema()
# root
#  |-- id:     long
#  |-- amount: double
```

> ⚠️ Spark reads only the schema of the first file — `customer` and `region` columns are **ignored**.

---

## With `mergeSchema = true`

```python
df = spark.read \
    .option("mergeSchema", "true") \
    .parquet("/data/sales/")

df.printSchema()
# root
#  |-- id:       long
#  |-- amount:   double
#  |-- customer: string   ← included ✅
#  |-- region:   string   ← included ✅
```

### Result:

```
+---+-------+--------+--------+
| id| amount|customer|  region|
+---+-------+--------+--------+
|  1| 100.0 |   null |   null |  ← file1: missing both columns
|  2| 200.0 |  Alice |   null |  ← file2: missing region
|  3| 300.0 |    Bob |     BR |  ← file3: complete
+---+-------+--------+--------+
```

---

## Complete Working Example

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("MergeSchema").getOrCreate()

# File 1 — original schema
df1 = spark.createDataFrame(
    [(1, 100.0), (2, 200.0)],
    ["id", "amount"]
)
df1.write.mode("overwrite").parquet("/tmp/sales/file1")

# File 2 — added "customer" column
df2 = spark.createDataFrame(
    [(3, 300.0, "Alice"), (4, 400.0, "Bob")],
    ["id", "amount", "customer"]
)
df2.write.mode("overwrite").parquet("/tmp/sales/file2")

# File 3 — added "region" column
df3 = spark.createDataFrame(
    [(5, 500.0, "Carol", "BR")],
    ["id", "amount", "customer", "region"]
)
df3.write.mode("overwrite").parquet("/tmp/sales/file3")

# Reading WITHOUT mergeSchema
df_without = spark.read.parquet("/tmp/sales/")
df_without.printSchema()  # ← incomplete schema

# Reading WITH mergeSchema
df_with = spark.read \
    .option("mergeSchema", "true") \
    .parquet("/tmp/sales/")
df_with.printSchema()  # ← unified schema ✅
df_with.show()
```

---

## Configuration Options

```python
# Option 1 — per read operation
df = spark.read.option("mergeSchema", "true").parquet(path)

# Option 2 — global session config
spark.conf.set("spark.sql.parquet.mergeSchema", "true")
df = spark.read.parquet(path)

# Option 3 — Delta Lake (on write with append mode)
df.write \
    .option("mergeSchema", "true") \
    .mode("append") \
    .format("delta") \
    .save("/path/delta_table")
```

---

## ⚠️ Performance Considerations

`mergeSchema = true` has a **performance cost**:

- Spark reads the schema (footer) of **every file** in the directory
- In directories with thousands of files, this can be **slow** ❌

### When to use:

| Scenario | Recommendation |
|---|---|
| Schema actually evolved over time | ✅ Use it |
| Unsure if schemas are identical | ✅ Use it |
| Schemas are known to be identical | ❌ Don't use (default `false` is faster) |

---

## Key Takeaway

> `mergeSchema=true` performs a **union of schemas** across all Parquet files — missing columns in older files become `null`. It's the standard solution for **schema evolution** in data lakes.

---

## Related Concepts

- **Schema Evolution** — gradual change of data structure over time
- **Delta Lake `mergeSchema`** — equivalent option for Delta tables on write
- **`spark.sql.parquet.mergeSchema`** — equivalent global config

---

*Tags: #ApacheSpark #PySpark #Parquet #DataEngineering #SchemaEvolution*
