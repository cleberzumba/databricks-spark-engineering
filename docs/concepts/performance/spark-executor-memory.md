# Spark Executor Memory

> A deep dive into how `spark.executor.memory` is actually distributed across regions, and how each Spark memory configuration affects the slicing.

---

## The Misconception

When you set:

```python
spark.executor.memory = "16g"
```

You're **not** giving the executor a single 16 GB block to use freely.

That value is just the **total JVM heap size** — the *cake*. Other configurations slice or extend it for different purposes.

Understanding this is the difference between solving an `OutOfMemoryError` correctly vs throwing more memory at the problem until something works (or doesn't).

---

## The Complete Breakdown

For an executor configured with `spark.executor.memory = 16g`:

```
┌─────────────────────────────────────────────────────────┐
│  EXECUTOR JVM HEAP — 16g                                │
├─────────────────────────────────────────────────────────┤
│  ① Reserved Memory — 300MB (always fixed)               │
│     → Spark internal, never touched                     │
├─────────────────────────────────────────────────────────┤
│  ② Spark Memory — 60% of (16g - 300MB) ≈ 9.4g           │
│     ↑ controlled by: spark.memory.fraction = 0.6        │
│                                                         │
│     ├── Storage Memory — 50% of 9.4g ≈ 4.7g             │
│     │   ↑ controlled by: storageFraction = 0.5          │
│     │   → cache(), persist(), broadcast variables       │
│     │                                                   │
│     └── Execution Memory — 50% of 9.4g ≈ 4.7g           │
│         → shuffles, sorts, aggregations, joins          │
│                                                         │
│     (storage and execution dynamically borrow           │
│      memory from each other based on demand)            │
│                                                         │
├─────────────────────────────────────────────────────────┤
│  ③ User Memory — 40% of (16g - 300MB) ≈ 6.3g            │
│     → Python workers, UDFs, RDD lineage                 │
│     → everything NOT managed by Spark                   │
└─────────────────────────────────────────────────────────┘

         ↕ OUTSIDE the JVM heap ↕

┌─────────────────────────────────────────────────────────┐
│  ④ Memory Overhead — 2g                                 │
│     ↑ controlled by: spark.executor.memoryOverhead      │
│     → network buffers, JVM internals, Python workers    │
│     → default: max(384MB, 10% of executor.memory)       │
├─────────────────────────────────────────────────────────┤
│  ⑤ Off-Heap Memory — 4g (if enabled)                    │
│     ↑ spark.memory.offHeap.enabled = "true"             │
│     ↑ spark.memory.offHeap.size = "4g"                  │
│     → outside the JVM Garbage Collector                 │
│     → used by Tungsten Engine for vectorized ops        │
│     → reduces GC pressure                               │
└─────────────────────────────────────────────────────────┘
```

**Total memory allocated by the cluster manager per executor:**

```
16g (heap) + 2g (overhead) + 4g (off-heap) = 22 GB
```

---

## Each Configuration in Detail

### 1. `spark.executor.memory`

**What it is:** the total JVM heap size of the executor.

**Why it matters:** this is the starting point — every other memory configuration slices or extends this value.

```python
spark.executor.memory = "16g"
```

> ⚠️ Setting this to 64g doesn't necessarily mean you'll have 64g of usable memory. The reserved memory, Spark Memory fraction, and User Memory split still apply.

---

### 2. `spark.memory.fraction`

**What it is:** how much of the heap (after the 300MB reserved) is allocated to Spark Memory (Storage + Execution).

**Default:** `0.6` (60% to Spark, 40% to User Memory)

```python
spark.memory.fraction = "0.6"
```

**When to adjust:**

- **Heavy Python UDFs / pandas_udf** → lower it (e.g., `0.5`)
  More room for Python workers in User Memory.

- **Pure Spark SQL workloads (no Python)** → raise it (e.g., `0.7`)
  Maximize memory for Spark's internal operations.

---

### 3. `spark.memory.storageFraction`

**What it is:** how much of Spark Memory is reserved for Storage (cache, broadcast variables) vs Execution (shuffles, sorts, joins).

**Default:** `0.5` (50% Storage, 50% Execution)

```python
spark.memory.storageFraction = "0.5"
```

**Important:** Storage and Execution share memory dynamically. If Storage isn't using its allocation, Execution can borrow from it (and vice versa). The fraction is a *baseline*, not a hard wall.

**When to adjust:**

- **Heavy caching workloads** → raise to `0.7` or `0.8`
  Prioritize keeping `cache()` data in memory.

- **Heavy shuffle/sort workloads** → lower to `0.3`
  Give Execution more guaranteed space.

---

### 4. `spark.executor.memoryOverhead`

**What it is:** additional memory allocated **outside** the JVM heap, used for JVM internals, network buffers, Python workers, and other non-heap structures.

**Default:** `max(384MB, 10% of spark.executor.memory)`

```python
spark.executor.memoryOverhead = "2g"
```

**Why it matters:** when you see a container being killed by YARN or Kubernetes (not a Java OOM), the executor probably exhausted its overhead — not the heap. Increasing `spark.executor.memory` won't help; you need more overhead.

**Common pattern for PySpark jobs:**

```python
# Default 10% is rarely enough for Python-heavy jobs
spark.executor.memory          = "16g"
spark.executor.memoryOverhead  = "4g"   # 25% — better for PySpark
```

---

### 5. `spark.memory.offHeap`

**What it is:** memory allocated outside the JVM Garbage Collector, used by the Tungsten Engine for vectorized operations.

```python
spark.memory.offHeap.enabled = "true"
spark.memory.offHeap.size    = "4g"
```

**When to use:**

- Long-running jobs experiencing GC pressure (frequent GC pauses)
- Heavy Tungsten-eligible operations (DataFrame ops, Catalyst optimizations)
- Large in-memory datasets where GC overhead becomes the bottleneck

---

## Practical Configuration Examples

### Heavy PySpark workload (lots of Python UDFs)

```python
spark.executor.memory          = "16g"
spark.executor.memoryOverhead  = "4g"   # more buffer for Python workers
spark.memory.fraction          = "0.5"  # more space for User Memory
spark.memory.offHeap.enabled   = "true"
spark.memory.offHeap.size      = "4g"
```

**Why:** Python workers consume User Memory. Reducing `memory.fraction` from 0.6 to 0.5 gives them more room. Increased overhead handles the network and Python serialization buffers.

---

### Pure Spark SQL workload (no Python)

```python
spark.executor.memory          = "16g"
spark.executor.memoryOverhead  = "2g"   # default is fine
spark.memory.fraction          = "0.7"  # 70% to Spark
spark.memory.storageFraction   = "0.3"  # prioritize Execution (shuffles)
```

**Why:** No Python = User Memory barely used. Push more memory to Spark itself, prioritizing Execution since shuffles and joins are the bottleneck.

---

### Cache-heavy ML workload

```python
spark.executor.memory          = "16g"
spark.executor.memoryOverhead  = "2g"
spark.memory.fraction          = "0.6"
spark.memory.storageFraction   = "0.7"  # prioritize Storage (cache)
```

**Why:** When you `.cache()` a DataFrame multiple times, you want it to stay in memory. Increasing `storageFraction` reduces eviction.

---

## Diagnosing Memory Issues

| Symptom | Root cause | Configuration to adjust |
|---|---|---|
| Generic OOM in executor logs | Heap too small | `spark.executor.memory`  |
| Container killed by YARN/K8s | Off-heap (overhead) too small | `spark.executor.memoryOverhead`  |
| Long GC pauses, slow tasks | GC pressure on heap | `spark.memory.offHeap` enable + size |
| `cache()` being evicted constantly | Storage region too small | `spark.memory.storageFraction`  |
| Shuffle/sort spilling to disk | Execution region too small | `spark.memory.storageFraction`  |
| OOM during Python UDFs | User Memory too small | `spark.memory.fraction`  |
| OOM in driver | Driver heap too small | `spark.driver.memory`  |

---

## Red Flags

Memory tuning isn't always about *more memory*. If you're scaling `spark.executor.memory` from 16g → 32g → 64g without seeing improvement, **the problem is architectural**, not configurational. Common culprits:

### 1. Data skew

One partition contains 80% of the data. That single executor will OOM no matter how much memory you give it.

**Solution:** salting, AQE skew join optimization, repartitioning.

### 2. Hidden `collect()` or `toPandas()`

Bringing all data to the driver bypasses distributed processing.

**Solution:** rewrite to keep computation on executors.

### 3. Broadcasting a too-large table

The broadcast threshold accidentally triggered for a multi-GB table.

**Solution:** check `spark.sql.autoBroadcastJoinThreshold`, force `MERGE` join hint when needed.

### 4. Excessive caching

Caching every intermediate DataFrame consumes Storage memory unnecessarily.

**Solution:** `unpersist()` what's no longer needed; cache strategically.

---

## Key Takeaways

1. **`spark.executor.memory` is the total heap, not the usable memory.** Subtract reserved, then split by `memory.fraction`, then by `storageFraction`.

2. **OOM in heap ≠ OOM in container.** A killed container usually means `memoryOverhead` is too small — not the heap.

3. **Python workers live in User Memory and Memory Overhead.** Heavy PySpark jobs need lower `memory.fraction` and higher `memoryOverhead`.

4. **GC pressure is solved by off-heap, not more heap.** Adding heap can make GC pauses worse.

5. **If scaling memory doesn't help, it's not a memory problem.** Look for data skew, hidden `collect()`, or accidental broadcasts.

---

## Configuration Cheat Sheet

```python
# Total heap
spark.executor.memory = "16g"

# Heap split (Spark vs User)
spark.memory.fraction = "0.6"           # 60% Spark, 40% User

# Spark Memory split (Storage vs Execution)
spark.memory.storageFraction = "0.5"    # 50% Storage, 50% Execution

# Off-heap regions
spark.executor.memoryOverhead = "2g"    # network, Python, JVM internals
spark.memory.offHeap.enabled  = "true"
spark.memory.offHeap.size     = "4g"    # Tungsten Engine

# Driver (separate)
spark.driver.memory          = "8g"
spark.driver.maxResultSize   = "4g"
```

---

## Related Concepts

- **Tungsten Engine** — uses off-heap memory for vectorized operations
- **Garbage Collection (GC)** — affects heap; off-heap bypasses it
- **Adaptive Query Execution (AQE)** — automatic tuning of partitions and joins
- **Catalyst Optimizer** — query plan optimization, runs in Driver memory

---

*Tags: #ApacheSpark #PySpark #Databricks #DataEngineering #PerformanceTuning #MemoryManagement*
