# Cache, Persist and Storage Levels in Apache Spark

---

## 1. Overview

Caching (or persistence) in Spark is the mechanism for storing a DataFrame/RDD in memory or disk to reuse across multiple actions, avoiding recomputation from the original source.

### When to use cache

- DataFrame is used in **multiple actions** (`count`, `show`, `write`, etc.)
- **Iterative operations** (loops, ML algorithms, multiple passes)
- Avoiding recomputation of **expensive transformations**

### When NOT to use cache

- DataFrame used only once
- Dataset too large to fit in the cluster
- Simple linear pipeline (no reuse)

---

## 2. `cache()` vs `persist()` — The Fundamental Difference

### `cache()`

- Fixed shortcut, **does not accept arguments**
- Equivalent to `persist(StorageLevel.MEMORY_AND_DISK)`
- Always uses memory + disk fallback

### `persist()`

- **Accepts a `StorageLevel`** as argument
- Allows choosing exactly where and how to store
- `persist()` without argument = same as `cache()`

### Equivalences

| Call | Equivalent to |
|---|---|
| `df.cache()` | `df.persist(StorageLevel.MEMORY_AND_DISK)` |
| `df.persist()` | `df.persist(StorageLevel.MEMORY_AND_DISK)` |
| `df.persist(StorageLevel.MEMORY_ONLY)` | **NOT the same as `df.cache()`** |

### ⚠️ Exam pitfall

```python
df.cache(StorageLevel.MEMORY_ONLY)   # ❌ ERROR! cache() takes no argument
df.persist(StorageLevel.MEMORY_ONLY)  # ✅ Correct syntax
```

---

## 3. Available Storage Levels

| StorageLevel | Memory | Disk | Serialized | Replicas |
|---|---|---|---|---|
| `MEMORY_ONLY` | ✅ | ❌ | ❌ | 1 |
| `MEMORY_ONLY_2` | ✅ | ❌ | ❌ | 2 |
| `MEMORY_ONLY_SER` | ✅ | ❌ | ✅ | 1 |
| `MEMORY_AND_DISK` *(default)* | ✅ | ✅ | ❌ | 1 |
| `MEMORY_AND_DISK_2` | ✅ | ✅ | ❌ | 2 |
| `MEMORY_AND_DISK_SER` | ✅ | ✅ | ✅ | 1 |
| `DISK_ONLY` | ❌ | ✅ | ✅ | 1 |
| `DISK_ONLY_2` | ❌ | ✅ | ✅ | 2 |
| `DISK_ONLY_3` | ❌ | ✅ | ✅ | 3 |
| `OFF_HEAP` | Off-heap | ❌ | ✅ | 1 |
| `NONE` | - | - | - | - |

### Important notes

- **Default in PySpark DataFrame API**: `MEMORY_AND_DISK`
- **`_2`, `_3`** suffix = number of replicas (fault tolerance)
- **`_SER`** suffix = serialized format (less memory, more CPU)
- **`OFF_HEAP`** requires `spark.memory.offHeap.enabled=true`

---

## 4. `MEMORY_ONLY` vs `MEMORY_AND_DISK`

### `MEMORY_ONLY`

- **Faster** (direct memory access)
- If partition doesn't fit → **dropped** and recomputed when needed
- **Risk**: expensive recomputation if dataset doesn't fit in memory

### `MEMORY_AND_DISK` (default for `cache()`)

- Tries memory first
- If it doesn't fit → **spills to disk** (doesn't drop)
- **Safer** for large datasets

---

## 5. Lazy Evaluation of Cache

`cache()` and `persist()` are **lazy** — they only mark the intention, they don't execute the cache.

The cache is only materialized when an **action** triggers execution:

```python
storesDF.persist(StorageLevel.MEMORY_ONLY)   # just marks, nothing happens
storesDF.count()                              # action: cache is materialized
storesDF.show()                               # uses existing cache
storesDF.filter(...).count()                  # uses existing cache
```

**Idiomatic pattern**: call a lightweight action (`count`) right after `persist` to force immediate materialization.

---

## 6. `unpersist()` — Release Memory

Cache is **not** released automatically. To release manually:

```python
df.unpersist()              # removes from cache
df.unpersist(blocking=True) # waits for removal to complete before continuing
```

**Best practice**: unpersist DataFrames when no longer needed, especially in long pipelines with many stages.

---

## 7. Correct Import for `StorageLevel`

```python
from pyspark.storagelevel import StorageLevel

df.persist(StorageLevel.MEMORY_ONLY)
```

> ⚠️ The import is **NOT** `from pyspark.sql` — it's `from pyspark.storagelevel`

---

## 8. Complete Syntax Examples

```python
from pyspark.storagelevel import StorageLevel

# Default cache (memory + disk)
df.cache()
df.count()  # materializes

# Custom persist (memory only)
df.persist(StorageLevel.MEMORY_ONLY)
df.count()

# Disk-only persist
df.persist(StorageLevel.DISK_ONLY)

# Persist with 2 replicas (high availability)
df.persist(StorageLevel.MEMORY_AND_DISK_2)

# Release from cache
df.unpersist()
```

---

## 9. Cache vs Checkpoint (Important Difference)

| Aspect | Cache / Persist | Checkpoint |
|---|---|---|
| Storage | Executor memory/disk | Reliable storage (HDFS, S3) |
| Lineage | Preserved (Spark can recreate if lost) | **Broken** (history discarded) |
| Speed | Faster | Slower (distributed disk write) |
| Fault tolerance | Limited | Full |
| Syntax | `df.cache()` | `sc.setCheckpointDir(path)` + `df.checkpoint()` |

---

## 10. Where to Verify the Cache

**Spark UI → Storage tab**:

- Shows cached DataFrames
- Size in memory and disk
- Cached fraction (if not everything fit)
- How many executors hold each partition

---

## 11. Common Exam Pitfalls

| Mistake | Reality |
|---|---|
| ❌ "`cache()` accepts argument" | `cache()` **never** accepts argument |
| ❌ "`cache()` is memory only" | `cache()` is **MEMORY_AND_DISK** |
| ❌ "cache happens immediately" | Lazy — needs an action |
| ❌ `from pyspark.sql import StorageLevel` | Wrong module — use `pyspark.storagelevel` |
| ❌ `persist(Nothing)` | `Nothing` is **Scala** syntax, not Python |
| ❌ Forgetting `unpersist()` | Memory keeps occupied until session ends |

### Example pitfalls in code

```python
# ❌ WRONG — cache() doesn't accept argument
df.cache(StorageLevel.MEMORY_ONLY)

# ✅ CORRECT
df.persist(StorageLevel.MEMORY_ONLY)

# ❌ WRONG — Nothing is Scala
df.persist(Nothing)

# ✅ CORRECT (in Python)
df.persist()  # uses default
```

---

## 12. Quick Reference Summary

| Concept | Rule |
|---|---|
| `cache()` | = `persist(MEMORY_AND_DISK)` |
| `cache()` argument | **Never** — always an error |
| `persist()` | Accepts `StorageLevel` |
| Default level | `MEMORY_AND_DISK` |
| `MEMORY_ONLY` | Memory only — recomputation if it doesn't fit |
| `DISK_ONLY` | Disk only — slower but safer |
| `_2` suffix | 2 replicas |
| `_SER` suffix | Serialized format |
| Lazy behavior | Cache/persist need an action |
| Release | `df.unpersist()` |

---

## Official Documentation

### PySpark API

- [`DataFrame.cache()`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.cache.html)
- [`DataFrame.persist()`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.persist.html)
- [`DataFrame.unpersist()`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.unpersist.html)
- [`StorageLevel`](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.StorageLevel.html)

### Spark Guides

- [Spark Programming Guide — RDD Persistence](https://spark.apache.org/docs/latest/rdd-programming-guide.html#rdd-persistence)
- [Spark SQL — Caching Data In Memory](https://spark.apache.org/docs/latest/sql-performance-tuning.html#caching-data-in-memory)

### Databricks

- [Databricks — Optimize performance with caching](https://docs.databricks.com/en/optimizations/disk-cache.html)
